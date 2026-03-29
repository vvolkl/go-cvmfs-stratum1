// mirrorv2.go implements the packfile-based CVMFS mirror (v2).
//
// It replaces the original loose-object mirror with a 256-way parallel
// pipeline that stores small objects (<256 KB) into git-packfile-inspired
// pack files and large objects as loose files.
//
// The pipeline:
//
//  1. Fetch manifest, download history DB, discover tagged roots.
//  2. Parallel catalog traversal (one per tree, dedup via LoadOrStore).
//  3. Merger goroutine routes hashes to 256 prefix workers.
//  4. Each prefix worker maintains a min-heap for dedup, then for each
//     unique hash: check existence (loose + pack), download if missing,
//     store in pack.tmp or loose depending on size.
//  5. Finalize packs when they reach 16 MB or when the worker finishes.
package mirror

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/catalog"
	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/packfile"
	"github.com/bbockelm/cvmfs-gc-optim/gc/repolock"
	"github.com/bbockelm/cvmfs-gc-optim/gc/sweep"
)

// V2Config controls a mirror-v2 operation.
type V2Config struct {
	BaseURL      string          // Stratum-1 base URL
	LocalDir     string          // local destination directory
	Parallelism  int             // catalog traversal parallelism
	TempDir      string          // temp directory for catalog decompression
	Ctx          context.Context // cancellation context (nil = Background)
	DrainTimeout time.Duration   // grace period after ^C to keep downloading (0 = 60s default)
}

// V2Stats reports mirror-v2 statistics.
type V2Stats struct {
	CatalogsStarted   int64 // dispatched to workers
	CatalogsProcessed int64 // fully processed
	CatalogsSkipped   int64 // cross-tree dedup
	HashesDiscovered  int64
	HashesQueued      int64 // sitting in prefix dedup heaps
	HashesSkipped     int64 // already present (loose or packed)
	DownloadedLoose   int64 // >= LooseThreshold -> data/
	DownloadedPacked  int64 // < LooseThreshold -> pack
	PacksFinalized    int64
	DownloadFailed    int64
	TotalBytes        int64 // bytes downloaded
}

// RunV2 mirrors a CVMFS repository using the packfile-based pipeline.
func RunV2(cfg V2Config) (*V2Stats, error) {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}

	// Drain grace period: after ctx cancels, keep downloading for
	// up to DrainTimeout before discarding remaining queued work.
	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 60 * time.Second
	}
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()
	context.AfterFunc(cfg.Ctx, func() {
		os.Stderr.WriteString(fmt.Sprintf(
			"\n  [Mirror V2] Context cancelled — draining for up to %s\n",
			drainTimeout))
		time.AfterFunc(drainTimeout, func() {
			log.Println("  [Mirror V2] drain timeout — stopping downloads")
			drainCancel()
		})
	})

	dataDir := filepath.Join(cfg.LocalDir, "data")
	packsDir := filepath.Join(cfg.LocalDir, "packs")

	// Acquire the update lock to prevent concurrent mirrors or GC.
	lockPath := filepath.Join(cfg.LocalDir, repolock.UpdateLockName)
	lock, err := repolock.TryLock(lockPath)
	if err != nil {
		return nil, fmt.Errorf("acquiring update lock: %w", err)
	}
	defer lock.Unlock()

	// Create directories.
	for i := 0; i < 256; i++ {
		prefix := fmt.Sprintf("%02x", i)
		os.MkdirAll(filepath.Join(dataDir, prefix), 0755)
		os.MkdirAll(filepath.Join(packsDir, prefix), 0755)
	}

	// Clean up any orphaned pack.tmp files from a prior crash.
	packfile.CleanupOrphanedPacks(packsDir)

	// Set up temp dir.
	tempDir := cfg.TempDir
	if tempDir == "" {
		td, err := os.MkdirTemp("", "cvmfs-mirrorv2-*")
		if err != nil {
			return nil, fmt.Errorf("creating temp dir: %w", err)
		}
		tempDir = td
		defer os.RemoveAll(tempDir)
	}

	stats := &V2Stats{}

	// ------------------------------------------------------------------
	// Step 1: Fetch manifest and upstream metadata files
	// ------------------------------------------------------------------
	log.Println("=== Mirror V2: fetching manifest ===")
	manifestData, err := httpGet(cfg.BaseURL + "/.cvmfspublished")
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	mfPath := filepath.Join(cfg.LocalDir, ".cvmfspublished")
	if err := os.WriteFile(mfPath, manifestData, 0644); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	// Fetch .cvmfswhitelist (required for CVMFS client verification).
	if wl, err := httpGet(cfg.BaseURL + "/.cvmfswhitelist"); err != nil {
		log.Printf("  WARNING: fetching .cvmfswhitelist: %v", err)
	} else {
		os.WriteFile(filepath.Join(cfg.LocalDir, ".cvmfswhitelist"), wl, 0644)
	}
	// Fetch .cvmfswhitelist.pkcs7 (optional companion).
	if wlp, err := httpGet(cfg.BaseURL + "/.cvmfswhitelist.pkcs7"); err == nil {
		os.WriteFile(filepath.Join(cfg.LocalDir, ".cvmfswhitelist.pkcs7"), wlp, 0644)
	}

	fields := parseManifestFields(manifestData)
	rootHash := fields['C']
	if rootHash == "" {
		return nil, fmt.Errorf("no root catalog hash (C) in manifest")
	}
	historyHash := fields['H']

	log.Printf("  Root catalog: %s", rootHash)
	log.Printf("  Repo:         %s", fields['N'])

	// ------------------------------------------------------------------
	// Step 2: Download manifest-referenced objects (H, X, M) as loose.
	//
	// The root catalog (C) is NOT downloaded here — TraverseMirror
	// uses the on-disk presence of a catalog file as a signal that its
	// entire subtree was already mirrored.  Downloading C now would
	// cause the traversal to skip the root tree and discover zero
	// content hashes.  The traversal itself will download and process
	// the root catalog.
	// ------------------------------------------------------------------
	for _, pair := range []struct {
		key    byte
		suffix byte
	}{
		{'H', catalog.SuffixHistory},
		{'X', catalog.SuffixCertificate},
		{'M', catalog.SuffixMetainfo},
	} {
		if h := fields[pair.key]; h != "" {
			if err := downloadObject(cfg.BaseURL, dataDir, h, string(pair.suffix)); err != nil {
				log.Printf("  WARNING: manifest object %s: %v", h[:min(16, len(h))], err)
			}
		}
	}

	// ------------------------------------------------------------------
	// Step 3: Discover tagged roots from history DB
	// ------------------------------------------------------------------
	var taggedRoots []catalog.TaggedRoot
	if historyHash != "" {
		historyCfg := catalog.HistoryConfig{
			DataDir: dataDir,
			TempDir: tempDir,
		}
		taggedRoots, err = catalog.ReadTaggedRoots(historyCfg, historyHash)
		if err != nil {
			log.Printf("  WARNING: reading history: %v", err)
		} else if len(taggedRoots) > 0 {
			log.Printf("  Tags: %d distinct roots to preserve", len(taggedRoots))
		}
	}

	// Collect manifest-referenced extra hashes (H, X, M).
	var extraHashes []catalog.Hash
	for _, pair := range []struct {
		key    byte
		suffix byte
	}{
		{'H', catalog.SuffixHistory},
		{'X', catalog.SuffixCertificate},
		{'M', catalog.SuffixMetainfo},
	} {
		if h := fields[pair.key]; h != "" {
			extraHashes = append(extraHashes, catalog.Hash{Hex: h, Suffix: pair.suffix})
		}
	}

	// ------------------------------------------------------------------
	// Step 4: Parallel catalog traversal
	// ------------------------------------------------------------------
	// Load the set of previously-processed catalog hashes.  This is the
	// authoritative record of which catalogs have been fully processed;
	// on-disk file existence alone is not reliable because a catalog may
	// have been downloaded but not yet processed (e.g., interrupted run).
	processedPath := filepath.Join(cfg.LocalDir, ".cvmfs_processed_catalogs")
	processed, err := catalog.LoadProcessed(processedPath)
	if err != nil {
		return nil, fmt.Errorf("loading processed catalogs: %w", err)
	}

	// Deduplicate tagged roots against current root.
	var uniqueTaggedRoots []catalog.TaggedRoot
	for _, t := range taggedRoots {
		if t.Hash != rootHash {
			uniqueTaggedRoots = append(uniqueTaggedRoots, t)
		}
	}
	nTrees := 1 + len(uniqueTaggedRoots)
	log.Printf("=== Mirror V2: traversing %d catalog tree(s) ===", nTrees)

	traverseCfg := catalog.TraverseConfig{
		DataDir:     dataDir,
		Parallelism: cfg.Parallelism,
		TempDir:     tempDir,
		FetchCatalog: func(hexHash string) error {
			return downloadObject(cfg.BaseURL, dataDir, hexHash,
				string(catalog.SuffixCatalog))
		},
		Processed: processed,
	}

	var catProg catalog.Progress
	traverseErrs := make(chan error, nTrees)

	// Each traversal gets its own output channel.
	privateChs := make([]chan catalog.Hash, nTrees)
	for i := range privateChs {
		privateChs[i] = make(chan catalog.Hash, 4096)
	}

	// Launch traversals.
	// Use TraverseMirror (not TraverseFromRootHash): if a catalog's
	// data file already exists on disk, its entire subtree is pruned
	// because a previous mirror run already downloaded everything it
	// references.
	go func() {
		err := catalog.TraverseMirror(traverseCfg, rootHash, privateChs[0], &catProg)
		if err != nil {
			traverseErrs <- err
		}
	}()
	for i, t := range uniqueTaggedRoots {
		go func(idx int, tag catalog.TaggedRoot) {
			err := catalog.TraverseMirror(traverseCfg, tag.Hash, privateChs[idx+1], &catProg)
			if err != nil {
				traverseErrs <- err
			}
		}(i, t)
	}

	// ------------------------------------------------------------------
	// Step 5: 256-way prefix pipeline
	// ------------------------------------------------------------------
	log.Println("=== Mirror V2: downloading + packing objects ===")
	startTime := time.Now()

	// 256 prefix channels.
	prefixChs := make([]chan hashsort.Entry, 256)
	for i := range prefixChs {
		prefixChs[i] = make(chan hashsort.Entry, 256)
	}

	// Merger: read from all traversal channels, inject extra hashes,
	// route to prefix channels.
	go func() {
		// Inject manifest-referenced hashes first.
		for _, mh := range extraHashes {
			e := hashsort.EntryFromHexSuffix(mh.Hex, mh.Suffix)
			select {
			case prefixChs[e.Prefix()] <- e:
			case <-cfg.Ctx.Done():
				// User cancelled — stop feeding new work.  The prefix
				// workers will drain what's already in their channels
				// and heaps within the drainCtx window.
				log.Println("  [Mirror V2] interrupted during extra-hash injection")
				for i := range prefixChs {
					close(prefixChs[i])
				}
				return
			}
		}

		var mergeWg sync.WaitGroup
		cancelled := int32(0)
		for _, ch := range privateChs {
			mergeWg.Add(1)
			go func(c <-chan catalog.Hash) {
				defer mergeWg.Done()
				for {
					select {
					case h, ok := <-c:
						if !ok {
							return // traversal finished
						}
						atomic.AddInt64(&stats.HashesDiscovered, 1)
						if atomic.LoadInt32(&cancelled) != 0 {
							continue // drain without routing
						}
						e := hashsort.EntryFromHexSuffix(h.Hex, h.Suffix)
						select {
						case prefixChs[e.Prefix()] <- e:
						case <-cfg.Ctx.Done():
							// Stop routing immediately so prefix
							// workers can drain their existing heaps.
							atomic.StoreInt32(&cancelled, 1)
						}
					case <-drainCtx.Done():
						// Drain expired — stop waiting for traversal
						// to finish.  Traversal goroutines may block
						// on their output channel (goroutine leak
						// until process exit); that's acceptable.
						return
					}
				}
			}(ch)
		}
		mergeWg.Wait()
		for i := range prefixChs {
			close(prefixChs[i])
		}
	}()

	// Initialize pack store for existence checking.
	store := packfile.NewStore(packsDir)
	store.RefreshAll()

	// Print baseline summary — what's already on disk before we start.
	looseCount, looseBytes := countLooseObjects(dataDir)
	packObjs := store.TotalObjects()
	packBytes := store.TotalPackBytes()
	packFiles := store.PackFileCount()
	processedCount := processed.Count()

	log.Println("=== Existing data on disk ===")
	log.Printf("  Loose objects: %d  (%.1f MB)", looseCount, float64(looseBytes)/(1024*1024))
	log.Printf("  Pack files:    %d  (%d objects, %.1f MB)", packFiles, packObjs, float64(packBytes)/(1024*1024))
	log.Printf("  Processed catalogs: %d (from prior runs)", processedCount)

	// Merge fragmented prefixes before starting the mirror.  Interrupted
	// runs leave behind many small packs per prefix; consolidating them
	// up front speeds up existence checks during the mirror.
	const mergeThreshold = 3
	maxIdx := store.MaxPrefixIndices()
	if maxIdx > mergeThreshold {
		log.Printf("=== Pack merge: max %d packs/prefix (threshold %d) — consolidating ===", maxIdx, mergeThreshold)
		nMerged, mergeErr := sweep.MergeFragmented(packsDir, mergeThreshold, store)
		if mergeErr != nil {
			log.Printf("  WARNING: merge error: %v", mergeErr)
		}
		if nMerged > 0 {
			store.RefreshAll()
			log.Printf("  Merged %d prefixes — now %d pack files (%d objects, %.1f MB)",
				nMerged, store.PackFileCount(), store.TotalObjects(),
				float64(store.TotalPackBytes())/(1024*1024))
		}
	}

	// Launch 256 prefix workers.
	var workerWg sync.WaitGroup
	for i := 0; i < 256; i++ {
		workerWg.Add(1)
		go func(prefixByte byte) {
			defer workerWg.Done()
			prefixWorkerV2(
				drainCtx,
				prefixByte,
				prefixChs[prefixByte],
				cfg.BaseURL,
				dataDir,
				packsDir,
				store.Prefix(prefixByte),
				stats,
			)
		}(byte(i))
	}

	// Progress ticker.  Keeps running during shutdown so the user
	// sees activity while workers drain.
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	shutdownNoticed := int32(0)

	printProgress := func(tag string) {
		log.Printf("%s cat_start=%d  cat_done=%d  discovered=%d  queued=%d  skipped=%d  loose=%d  packed=%d  packs=%d  failed=%d  bytes=%.1fMB  elapsed=%s",
			tag,
			atomic.LoadInt64(&catProg.CatalogsStarted),
			atomic.LoadInt64(&catProg.CatalogsProcessed),
			atomic.LoadInt64(&stats.HashesDiscovered),
			atomic.LoadInt64(&stats.HashesQueued),
			atomic.LoadInt64(&stats.HashesSkipped),
			atomic.LoadInt64(&stats.DownloadedLoose),
			atomic.LoadInt64(&stats.DownloadedPacked),
			atomic.LoadInt64(&stats.PacksFinalized),
			atomic.LoadInt64(&stats.DownloadFailed),
			float64(atomic.LoadInt64(&stats.TotalBytes))/(1024*1024),
			time.Since(startTime).Truncate(time.Second))
	}

	// ctxDone is set to nil after the first cancellation notice so
	// we don't keep selecting on an already-closed channel.
	ctxDone := cfg.Ctx.Done()

	go func() {
		legendPrinted := false
		for {
			select {
			case <-ticker.C:
				if !legendPrinted {
					log.Println("  [Mirror V2] Progress legend:")
					log.Println("    cat_start = catalogs dispatched to workers")
					log.Println("    cat_done  = catalogs fully processed")
					log.Println("    discovered= content hashes emitted by traversal")
					log.Println("    queued    = hashes buffered in dedup heaps (not yet downloaded)")
					log.Println("    skipped   = hashes already on disk (loose file or pack)")
					log.Println("    loose     = large objects (>=256KB) downloaded as individual files")
					log.Println("    packed    = small objects (<256KB) downloaded into pack files")
					log.Println("    packs     = pack files finalized (sealed and indexed)")
					log.Println("    failed    = download or write errors")
					log.Println("    bytes     = total bytes downloaded so far")
					legendPrinted = true
				}
				tag := "  [Mirror V2]"
				if drainCtx.Err() != nil {
					tag = "  [SHUTDOWN]"
				} else if cfg.Ctx.Err() != nil {
					tag = "  [DRAINING]"
				}
				printProgress(tag)

			case <-ctxDone:
				// Fires immediately when context is cancelled.
				ctxDone = nil // don't re-enter
				if atomic.CompareAndSwapInt32(&shutdownNoticed, 0, 1) {
					log.Printf("  [Mirror V2] *** shutting down — draining for up to %s ***", drainTimeout)
					printProgress("  [DRAINING]")
				}

			case <-done:
				return
			}
		}
	}()

	workerWg.Wait()
	ticker.Stop()
	close(done)

	// The run is "interrupted" only if the drain timed out (we had
	// to discard queued work).  If ^C was hit but the drain completed
	// all work before the timeout, the run is effectively complete.
	drainTimedOut := drainCtx.Err() != nil
	userCancelled := cfg.Ctx.Err() != nil
	if drainTimedOut {
		log.Println("=== Mirror V2 interrupted (drain timed out) — flushing state to disk ===")
	} else if userCancelled {
		log.Println("=== Mirror V2 drain completed — all queued work finished ===")
	}

	// Drain traversal errors.
	close(traverseErrs)
	for err := range traverseErrs {
		log.Printf("  WARNING: traversal error: %v", err)
	}

	stats.CatalogsStarted = atomic.LoadInt64(&catProg.CatalogsStarted)
	stats.CatalogsProcessed = atomic.LoadInt64(&catProg.CatalogsProcessed)
	stats.CatalogsSkipped = atomic.LoadInt64(&catProg.CatalogsSkipped)

	// Persist the set of processed catalogs (sorted & deduped).
	// Only save on a fully clean (non-interrupted) run.  A catalog is
	// marked "processed" in memory as soon as its hashes are emitted
	// to the output channel — but on ^C the merger stops routing, so
	// some of those hashes may never reach prefix workers.  Saving the
	// set would cause the next run to skip those catalogs even though
	// their objects were never downloaded.  Re-traversal is cheap:
	// existence checks (loose + pack) skip already-downloaded objects.
	if !userCancelled {
		if err := processed.Save(); err != nil {
			log.Printf("  WARNING: saving processed catalogs: %v", err)
		}
		// Write CVMFS-compatible status files on clean completion.
		writeSnapshotMetadata(cfg.LocalDir)
	} else {
		log.Println("  Skipping processed-catalogs save (interrupted)")
	}

	// Refresh store indices after mirroring.
	store.RefreshAll()

	elapsed := time.Since(startTime)
	if drainTimedOut {
		log.Println("=== Mirror V2 interrupted (partial) ===")
	} else if userCancelled {
		log.Println("=== Mirror V2 complete (drain finished) ===")
	} else {
		log.Println("=== Mirror V2 complete ===")
	}
	log.Printf("  Catalogs:     %d started, %d completed (%d deduped)", stats.CatalogsStarted, stats.CatalogsProcessed, stats.CatalogsSkipped)
	log.Printf("  Discovered:   %d hashes", stats.HashesDiscovered)
	log.Printf("  Skipped:      %d (already present)", stats.HashesSkipped)
	log.Printf("  Downloaded:   %d loose + %d packed", stats.DownloadedLoose, stats.DownloadedPacked)
	log.Printf("  Packs:        %d finalized", stats.PacksFinalized)
	log.Printf("  Failed:       %d", stats.DownloadFailed)
	log.Printf("  Total bytes:  %.1f MB", float64(stats.TotalBytes)/(1024*1024))
	log.Printf("  Elapsed:      %s", elapsed.Truncate(time.Millisecond))
	log.Printf("  Packed objs:  %d across all packs", store.TotalObjects())

	return stats, nil
}

// prefixWorkerV2 processes all hashes for a single byte prefix.
// It maintains a min-heap for dedup, checks existence, downloads
// missing objects, and packs small ones.
func prefixWorkerV2(
	ctx context.Context,
	prefixByte byte,
	input <-chan hashsort.Entry,
	baseURL string,
	dataDir string,
	packsDir string,
	ps *packfile.PrefixState,
	stats *V2Stats,
) {
	prefix := fmt.Sprintf("%02x", prefixByte)
	packDir := filepath.Join(packsDir, prefix)

	// Min-heap for dedup (same as hashsort.PrefixWrite).
	maxEntries := 1024 * 1024 / hashsort.EntrySize // ~1 MB
	if maxEntries < 256 {
		maxEntries = 256
	}

	type entryHeap []hashsort.Entry
	heap := make(entryHeap, 0, maxEntries+1)

	heapPush := func(e hashsort.Entry) {
		heap = append(heap, e)
		i := len(heap) - 1
		for i > 0 {
			parent := (i - 1) / 2
			if hashsort.CompareEntries(heap[i], heap[parent]) >= 0 {
				break
			}
			heap[i], heap[parent] = heap[parent], heap[i]
			i = parent
		}
	}

	heapPop := func() hashsort.Entry {
		item := heap[0]
		n := len(heap) - 1
		heap[0] = heap[n]
		heap = heap[:n]
		i := 0
		for {
			left := 2*i + 1
			if left >= len(heap) {
				break
			}
			smallest := left
			right := left + 1
			if right < len(heap) && hashsort.CompareEntries(heap[right], heap[left]) < 0 {
				smallest = right
			}
			if hashsort.CompareEntries(heap[i], heap[smallest]) <= 0 {
				break
			}
			heap[i], heap[smallest] = heap[smallest], heap[i]
			i = smallest
		}
		return item
	}

	var lastWritten hashsort.Entry
	hasLast := false

	var pw *packfile.PackWriter

	processEntry := func(entry hashsort.Entry) {
		// Consecutive-duplicate check.
		if hasLast && entry == lastWritten {
			return
		}
		lastWritten = entry
		hasLast = true

		hexHash := entry.Hex()
		suffix := entry.Suffix()
		var suffixStr string
		if suffix != 0 {
			suffixStr = string(suffix)
		}

		// Check loose file existence.
		loosePath := filepath.Join(dataDir, prefix, hexHash[2:]+suffixStr)
		if _, err := os.Stat(loosePath); err == nil {
			atomic.AddInt64(&stats.HashesSkipped, 1)
			return
		}

		// Check pack indices.
		var binHash [20]byte
		hex.Decode(binHash[:], []byte(hexHash))
		if ps.Contains(binHash) {
			atomic.AddInt64(&stats.HashesSkipped, 1)
			return
		}

		// Download — stream the response, don't buffer into memory.
		reqURL := baseURL + "/data/" + prefix + "/" + hexHash[2:] + suffixStr
		resp, err := http.Get(reqURL)
		if err != nil {
			atomic.AddInt64(&stats.DownloadFailed, 1)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			atomic.AddInt64(&stats.DownloadFailed, 1)
			return
		}

		// Use Content-Length to decide loose vs pack when available.
		// If Content-Length is missing (chunked), we must stream into
		// the pack (the common case — CVMFS objects are typically
		// small) and then check the size after the fact.
		contentLen := resp.ContentLength // -1 if unknown

		if contentLen >= int64(packfile.LooseThreshold) {
			// Definitely loose — stream directly to file.
			dir := filepath.Join(dataDir, prefix)
			os.MkdirAll(dir, 0755)
			n, writeErr := streamToFile(loosePath, resp.Body)
			if writeErr != nil {
				log.Printf("    WARNING: writing loose %s: %v", hexHash[:16], writeErr)
				os.Remove(loosePath)
				atomic.AddInt64(&stats.DownloadFailed, 1)
				return
			}
			atomic.AddInt64(&stats.TotalBytes, n)
			atomic.AddInt64(&stats.DownloadedLoose, 1)
		} else if contentLen >= 0 {
			// Known size, below threshold — stream into pack.
			if pw == nil {
				var err error
				pw, err = packfile.NewPackWriter(packDir, prefixByte)
				if err != nil {
					log.Printf("    WARNING: creating pack writer for %s: %v", prefix, err)
					atomic.AddInt64(&stats.DownloadFailed, 1)
					return
				}
			}

			_, written, appendErr := pw.AppendReader(binHash, resp.Body)
			if appendErr != nil {
				log.Printf("    WARNING: appending to pack %s: %v", prefix, appendErr)
				atomic.AddInt64(&stats.DownloadFailed, 1)
				return
			}
			atomic.AddInt64(&stats.TotalBytes, written)
			atomic.AddInt64(&stats.DownloadedPacked, 1)

			if pw.Count() >= packfile.MaxPackEntries {
				if _, err := pw.Finalize(); err != nil {
					log.Printf("    WARNING: finalizing pack %s: %v", prefix, err)
				} else {
					atomic.AddInt64(&stats.PacksFinalized, 1)
					ps.Refresh()
				}
				pw = nil
			}
		} else {
			// Content-Length unknown — stream to a temp file,
			// then decide based on actual size.
			tmpFile, tmpErr := os.CreateTemp(packDir, "obj-*.tmp")
			if tmpErr != nil {
				atomic.AddInt64(&stats.DownloadFailed, 1)
				return
			}
			tmpPath := tmpFile.Name()
			n, copyErr := io.Copy(tmpFile, resp.Body)
			tmpFile.Close()
			if copyErr != nil {
				os.Remove(tmpPath)
				atomic.AddInt64(&stats.DownloadFailed, 1)
				return
			}
			atomic.AddInt64(&stats.TotalBytes, n)

			if n >= int64(packfile.LooseThreshold) {
				// Move temp file to loose location.
				dir := filepath.Join(dataDir, prefix)
				os.MkdirAll(dir, 0755)
				if renameErr := os.Rename(tmpPath, loosePath); renameErr != nil {
					os.Remove(tmpPath)
					atomic.AddInt64(&stats.DownloadFailed, 1)
					return
				}
				atomic.AddInt64(&stats.DownloadedLoose, 1)
			} else {
				// Read temp file into pack.
				tmpData, readErr := os.ReadFile(tmpPath)
				os.Remove(tmpPath)
				if readErr != nil {
					atomic.AddInt64(&stats.DownloadFailed, 1)
					return
				}
				if pw == nil {
					var err error
					pw, err = packfile.NewPackWriter(packDir, prefixByte)
					if err != nil {
						log.Printf("    WARNING: creating pack writer for %s: %v", prefix, err)
						atomic.AddInt64(&stats.DownloadFailed, 1)
						return
					}
				}
				if _, err := pw.Append(binHash, tmpData); err != nil {
					log.Printf("    WARNING: appending to pack %s: %v", prefix, err)
					atomic.AddInt64(&stats.DownloadFailed, 1)
					return
				}
				atomic.AddInt64(&stats.DownloadedPacked, 1)

				if pw.Count() >= packfile.MaxPackEntries {
					if _, err := pw.Finalize(); err != nil {
						log.Printf("    WARNING: finalizing pack %s: %v", prefix, err)
					} else {
						atomic.AddInt64(&stats.PacksFinalized, 1)
						ps.Refresh()
					}
					pw = nil
				}
			}
		}
	}

	// Read input and maintain heap.
	for entry := range input {
		if ctx.Err() != nil {
			// Context cancelled — drain input without processing.
			continue
		}
		heapPush(entry)
		atomic.AddInt64(&stats.HashesQueued, 1)
		if len(heap) > maxEntries {
			evicted := heapPop()
			atomic.AddInt64(&stats.HashesQueued, -1)
			processEntry(evicted)
		}
	}

	// Flush remaining heap entries in sorted order.
	// On cancellation, skip the flush — those hashes haven't been
	// downloaded yet and attempting millions of HTTP GETs would hang
	// the shutdown for hours.
	if ctx.Err() == nil {
		for len(heap) > 0 {
			e := heapPop()
			atomic.AddInt64(&stats.HashesQueued, -1)
			processEntry(e)
		}
	} else {
		// Discard queued entries.
		n := int64(len(heap))
		heap = heap[:0]
		atomic.AddInt64(&stats.HashesQueued, -n)
	}

	// Finalize any remaining open pack.
	if pw != nil && pw.Count() > 0 {
		if _, err := pw.Finalize(); err != nil {
			log.Printf("    WARNING: final pack finalize %s: %v", prefix, err)
		} else {
			atomic.AddInt64(&stats.PacksFinalized, 1)
			ps.Refresh()
		}
	} else if pw != nil {
		pw.Abort()
	}
}

// streamToFile streams from r into a new file at path, writing through
// a buffer.  Returns the number of bytes written.  The file is created
// with mode 0644; on error, the caller should clean up.
func streamToFile(path string, r io.Reader) (int64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	n, copyErr := io.Copy(bw, r)
	if flushErr := bw.Flush(); flushErr != nil && copyErr == nil {
		copyErr = flushErr
	}
	if closeErr := f.Close(); closeErr != nil && copyErr == nil {
		copyErr = closeErr
	}
	return n, copyErr
}

// countLooseObjects walks data/XX/ directories and counts regular files
// and their total size.  This is a quick scan (ReadDir, no stat per file
// unless needed) used for the startup summary.
func countLooseObjects(dataDir string) (count int64, bytes int64) {
	for i := 0; i < 256; i++ {
		prefix := fmt.Sprintf("%02x", i)
		dir := filepath.Join(dataDir, prefix)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			count++
			if info, err := e.Info(); err == nil {
				bytes += info.Size()
			}
		}
	}
	return
}

// writeSnapshotMetadata writes the CVMFS-compatible metadata files that
// monitoring tools and other Stratum-1s expect after a successful
// snapshot (mirror) cycle:
//
//   - .cvmfs_last_snapshot: human-readable UTC timestamp (deprecated but
//     still used by some tooling)
//   - .cvmfs_status.json:  JSON with last_snapshot timestamp, compatible
//     with the format written by cvmfs_server snapshot
func writeSnapshotMetadata(repoDir string) {
	now := time.Now().UTC()

	// .cvmfs_last_snapshot — matches `date --utc` output format
	ts := now.Format("Mon Jan  2 15:04:05 UTC 2006")
	if err := os.WriteFile(
		filepath.Join(repoDir, ".cvmfs_last_snapshot"),
		[]byte(ts+"\n"), 0644); err != nil {
		log.Printf("  WARNING: writing .cvmfs_last_snapshot: %v", err)
	}

	// .cvmfs_status.json — read existing, merge last_snapshot field
	statusPath := filepath.Join(repoDir, ".cvmfs_status.json")
	statusMap := map[string]interface{}{}
	if data, err := os.ReadFile(statusPath); err == nil {
		json.Unmarshal(data, &statusMap) // ignore parse errors, overwrite
	}
	statusMap["last_snapshot"] = ts
	if out, err := json.MarshalIndent(statusMap, "", "  "); err == nil {
		os.WriteFile(statusPath, append(out, '\n'), 0644)
	}
}
