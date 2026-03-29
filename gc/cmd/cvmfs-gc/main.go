// cvmfs-gc is an optimized garbage collector for CVMFS Stratum-1 servers.
//
// It replaces the traditional mark-and-sweep approach with a three-phase
// algorithm designed for repositories with hundreds of millions of objects
// while minimising the time the repository lock is held:
//
//  1. Catalog traversal + prefix-partitioned write + chunk sort (unlocked):
//     parallel catalog tree walk feeds hashes into 256 goroutines — one
//     per byte prefix.  Each goroutine maintains a ~1 MB min-heap of raw
//     binary entries (20-byte hash + 1-byte suffix) and writes evicted
//     entries to a per-prefix chunk file (chunk-00 through chunk-ff).
//     After all hashes are dispatched, the chunk files are sorted and
//     deduplicated in parallel (bounded to ~100 MB concurrently in memory).
//     Catalog hashes are collected in memory for Phase 2b.
//     2a. Candidate collection (unlocked): for each of the 256 byte prefixes,
//     list + sort the corresponding data directory entries, merge-join
//     against the sorted chunk file for that prefix.  Unreachable files
//     become deletion candidates.  All 256 prefixes run in parallel.
//     2b. Locked delta + deletion: acquire the repository lock, re-read the
//     manifest, re-walk only new/changed catalogs (skipping those already
//     seen in Phase 1), subtract newly-reachable hashes from the
//     candidate set, then delete what remains.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/catalog"
	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/mirror"
	"github.com/bbockelm/cvmfs-gc-optim/gc/repolock"
	"github.com/bbockelm/cvmfs-gc-optim/gc/sweep"
)

// cleanupRegistry tracks directories that should be removed on exit,
// including when the process is interrupted by SIGINT or SIGTERM.
// All methods are safe for concurrent use.
var cleanupRegistry struct {
	mu   sync.Mutex
	dirs []string
}

// repoLocks holds the CVMFS GC + update locks. Released on exit or signal.
var repoLocks repolock.Set

func registerCleanup(dir string) {
	cleanupRegistry.mu.Lock()
	cleanupRegistry.dirs = append(cleanupRegistry.dirs, dir)
	cleanupRegistry.mu.Unlock()
}

func runCleanup() {
	cleanupRegistry.mu.Lock()
	dirs := cleanupRegistry.dirs
	cleanupRegistry.dirs = nil
	cleanupRegistry.mu.Unlock()
	for _, d := range dirs {
		os.RemoveAll(d)
	}
}

func init() {
	// Catch SIGINT/SIGTERM so we can clean up temp files.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Caught %v, cleaning up...", sig)
		repoLocks.Release()
		runCleanup()
		os.Exit(1)
	}()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		repoDir      = flag.String("repo", "", "Path to the CVMFS repository (local)")
		rootHash     = flag.String("root-hash", "", "Hex hash of root catalog")
		parallelism  = flag.Int("parallelism", 8, "Catalog traversal workers")
		prefixHeapKB = flag.Int("prefix-heap-kb", 1024, "Per-prefix heap budget in KB (~1 MB default)")
		sortMB       = flag.Int("sort-mb", 100, "Max memory for parallel chunk sorting in MB")
		doDelete     = flag.Bool("delete", false, "Actually delete unreachable files (default is list only)")
		outputFile   = flag.String("output", "", "Write unreachable hashes to this file (default: <repo>-gc-unreachable.txt)")
		tempDir      = flag.String("temp-dir", "", "Temp directory")
		manifestFile = flag.String("manifest", "", "Path to .cvmfspublished")
		spoolDir     = flag.String("spool-dir", "", "Spool directory for lock files (default: /var/spool/cvmfs/<repo>)")
		noLock       = flag.Bool("no-lock", false, "Skip repository locking (not recommended)")

		// Mirror mode
		doMirror    = flag.Bool("mirror", false, "Mirror a repo from a Stratum-1 instead of running GC")
		stratum1URL = flag.String("stratum1-url", "", "Stratum-1 base URL for mirroring")
		mirrorJobs  = flag.Int("mirror-jobs", 8, "Parallel downloads for mirroring")
	)
	flag.Parse()

	// Mirror mode: download a repo snapshot for testing.
	if *doMirror {
		if *stratum1URL == "" {
			log.Fatal("ERROR: -stratum1-url is required for -mirror")
		}
		if *repoDir == "" {
			log.Fatal("ERROR: -repo is required (local destination directory)")
		}
		mirrorCfg := mirror.Config{
			BaseURL:     *stratum1URL,
			LocalDir:    *repoDir,
			Parallelism: *mirrorJobs,
		}
		if _, err := mirror.Run(mirrorCfg); err != nil {
			log.Fatalf("ERROR: mirror failed: %v", err)
		}
		return
	}

	// GC mode: requires existing local repo.

	if *repoDir == "" {
		log.Fatal("ERROR: -repo is required")
	}

	dataDir := filepath.Join(*repoDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		log.Fatalf("ERROR: data directory not found: %s", dataDir)
	}

	// ----------------------------------------------------------------
	// Repository locking — deferred until Phase 2b (see below).
	// We only record the spool directory here so we can lock later.
	// ----------------------------------------------------------------
	if !*noLock {
		if *spoolDir == "" {
			*spoolDir = filepath.Join("/var/spool/cvmfs", filepath.Base(*repoDir))
		}
	}

	// Determine root hash
	if *rootHash == "" && *manifestFile == "" {
		mfPath := filepath.Join(*repoDir, ".cvmfspublished")
		if _, err := os.Stat(mfPath); err == nil {
			*manifestFile = mfPath
		} else {
			log.Fatal("ERROR: -root-hash or -manifest required")
		}
	}

	var manifest *manifestInfo
	var manifestHashes []catalog.Hash
	var historyHash string
	if *rootHash == "" && *manifestFile != "" {
		var err error
		manifest, err = parseManifest(*manifestFile)
		if err != nil {
			log.Fatalf("ERROR: parsing manifest: %v", err)
		}
		*rootHash = manifest.RootHash
		manifestHashes = manifest.ExtraHashes
		historyHash = manifest.HistoryHash

		if !manifest.GarbageCollectable {
			log.Fatal("ERROR: repository does not allow garbage collection (G=no in manifest)")
		}

		log.Printf("Root catalog hash from manifest: %s", *rootHash)
		if manifest.RepoName != "" {
			log.Printf("Repository name: %s", manifest.RepoName)
		}
		if len(manifestHashes) > 0 {
			log.Printf("Manifest-referenced objects: %d (history, certificate, metainfo)", len(manifestHashes))
		}
	}

	// Setup temp directory (must be before history DB reading which may
	// need to decompress into a temp file).
	if *tempDir == "" {
		td, err := os.MkdirTemp("", "cvmfs-gc-*")
		if err != nil {
			log.Fatalf("ERROR: creating temp dir: %v", err)
		}
		*tempDir = td
		registerCleanup(*tempDir)
		defer runCleanup()
	} else {
		os.MkdirAll(*tempDir, 0755)
	}

	// ---------------------------------------------------------------
	// Open the history database to discover tagged root catalogs.
	// Every named snapshot must have its entire catalog tree preserved.
	// ---------------------------------------------------------------
	var taggedRoots []catalog.TaggedRoot
	if historyHash != "" {
		historyCfg := catalog.HistoryConfig{
			DataDir: dataDir,
			TempDir: *tempDir,
		}
		var err error
		taggedRoots, err = catalog.ReadTaggedRoots(historyCfg, historyHash)
		if err != nil {
			log.Printf("WARNING: reading history DB: %v (tagged snapshots will NOT be preserved)", err)
		} else if len(taggedRoots) > 0 {
			log.Printf("Named snapshots (tags): %d distinct root catalogs to preserve", len(taggedRoots))
			for i, t := range taggedRoots {
				if i < 10 {
					log.Printf("  tag %-30s  rev=%d  root=%s", t.Name, t.Revision, t.Hash[:min(16, len(t.Hash))])
				} else if i == 10 {
					log.Printf("  ... and %d more tags", len(taggedRoots)-10)
				}
			}
		} else {
			log.Printf("History DB present but no tags found")
		}
	}

	startTime := time.Now()

	// ----------------------------------------------------------------
	// Phase 1: Catalog traversal → 256-way prefix write → chunk sort
	//
	// We traverse the current root catalog tree AND every tagged root
	// catalog tree in parallel.  All traversals feed into the shared
	// entryCh so the prefix writer receives hashes from all trees.
	//
	// TraverseFromRootHash closes the channel it's given, so each
	// traversal gets its own private channel.  A merger goroutine
	// copies from all private channels into entryCh.
	// ----------------------------------------------------------------
	// Deduplicate tagged roots against the current root hash — if a
	// tag points to the same root as the manifest, skip it.
	var uniqueTaggedRoots []catalog.TaggedRoot
	for _, t := range taggedRoots {
		if t.Hash != *rootHash {
			uniqueTaggedRoots = append(uniqueTaggedRoots, t)
		}
	}
	nTrees := 1 + len(uniqueTaggedRoots)
	log.Printf("=== Phase 1: Traversing %d catalog tree(s) + prefix-partitioned write + chunk sort ===", nTrees)
	phaseStart := time.Now()

	cfg := catalog.TraverseConfig{
		DataDir:     dataDir,
		Parallelism: *parallelism,
		TempDir:     *tempDir,
	}

	var catProg catalog.Progress
	traverseErrs := make(chan error, nTrees)

	// Create a private channel per traversal.  Each TraverseFromRootHash
	// call closes its own channel when done.
	privateChs := make([]chan catalog.Hash, nTrees)
	for i := range privateChs {
		privateChs[i] = make(chan catalog.Hash, 4096)
	}

	// Launch traversal for the current root.
	go func() {
		err := catalog.TraverseFromRootHash(cfg, *rootHash, privateChs[0], &catProg)
		if err != nil {
			traverseErrs <- fmt.Errorf("current root %s: %w", *rootHash, err)
		}
	}()

	// Launch traversals for tagged roots.
	for i, t := range uniqueTaggedRoots {
		go func(idx int, tag catalog.TaggedRoot) {
			err := catalog.TraverseFromRootHash(cfg, tag.Hash, privateChs[idx+1], &catProg)
			if err != nil {
				traverseErrs <- fmt.Errorf("tag %q root %s: %w", tag.Name, tag.Hash, err)
			}
		}(i, t)
	}

	// Merger: read from all private channels into entryCh.
	// Also inject manifest-referenced hashes first.
	entryCh := make(chan hashsort.Entry, 8192)
	go func() {
		// Inject manifest-referenced hashes.
		for _, mh := range manifestHashes {
			entryCh <- hashsort.EntryFromHexSuffix(mh.Hex, mh.Suffix)
		}

		// Merge all traversal channels.  Use a WaitGroup to know
		// when every channel has been drained.
		var mergeWg sync.WaitGroup
		for _, ch := range privateChs {
			mergeWg.Add(1)
			go func(c <-chan catalog.Hash) {
				defer mergeWg.Done()
				for h := range c {
					entryCh <- hashsort.EntryFromHexSuffix(h.Hex, h.Suffix)
				}
			}(ch)
		}
		mergeWg.Wait()
		close(entryCh)
	}()

	// PrefixWrite dispatches entries to 256 goroutines, one per byte
	// prefix.  Each maintains a ~1 MB min-heap and writes evicted
	// entries to chunk-XX files.
	prefixCfg := hashsort.PrefixWriterConfig{
		HeapBytes: int64(*prefixHeapKB) * 1024,
	}
	chunkDir := filepath.Join(*tempDir, "chunks")

	type prefixResult struct {
		files []string
		err   error
	}

	var hashesConsumed int64
	var evictDeduped int64
	prefixDone := make(chan prefixResult, 1)
	go func() {
		files, err := hashsort.PrefixWrite(prefixCfg, entryCh, chunkDir, &hashesConsumed, &evictDeduped, nil)
		prefixDone <- prefixResult{files, err}
	}()

	// Progress ticker for Phase 1a (catalog traversal + prefix write).
	p1aTicker := time.NewTicker(5 * time.Second)
	p1aDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-p1aTicker.C:
				skipped := atomic.LoadInt64(&catProg.CatalogsSkipped)
				eDedup := atomic.LoadInt64(&evictDeduped)
				extraMsg := ""
				if skipped > 0 {
					extraMsg += fmt.Sprintf("  catalogs_deduped=%d", skipped)
				}
				if eDedup > 0 {
					extraMsg += fmt.Sprintf("  evict_deduped=%d", eDedup)
				}
				log.Printf("  [Phase 1a: prefix write] catalogs=%d  hashes_discovered=%d  hashes_consumed=%d%s  elapsed=%s",
					atomic.LoadInt64(&catProg.CatalogsProcessed),
					atomic.LoadInt64(&catProg.HashesEmitted),
					atomic.LoadInt64(&hashesConsumed),
					extraMsg,
					time.Since(phaseStart).Truncate(time.Second))
			case <-p1aDone:
				return
			}
		}
	}()

	// Wait for PrefixWrite to finish (implies all traversals and the
	// merger are done, since PrefixWrite reads from entryCh which is
	// fed by the merger which reads from all private traversal channels).
	pr := <-prefixDone
	chunkFiles := pr.files
	if pr.err != nil {
		log.Fatalf("ERROR: prefix write failed: %v", pr.err)
	}

	// Check for traversal errors (non-blocking drain).
	close(traverseErrs)
	for err := range traverseErrs {
		log.Fatalf("ERROR: catalog traversal failed: %v", err)
	}

	p1aTicker.Stop()
	close(p1aDone)

	prefixWriteElapsed := time.Since(phaseStart)
	skippedCatalogs := atomic.LoadInt64(&catProg.CatalogsSkipped)
	evictDedupCount := atomic.LoadInt64(&evictDeduped)
	log.Printf("Prefix write complete: %d catalogs (%d deduped), %d hashes consumed, %d evict-deduped, %d chunk files, elapsed %s",
		atomic.LoadInt64(&catProg.CatalogsProcessed),
		skippedCatalogs,
		atomic.LoadInt64(&hashesConsumed),
		evictDedupCount,
		len(chunkFiles), prefixWriteElapsed.Truncate(time.Millisecond))

	// Phase 1b: Sort and deduplicate each chunk file in parallel,
	// bounded to ~sortMB total memory.
	log.Printf("=== Phase 1b: Sorting %d chunk files (up to %d MB concurrent) ===", len(chunkFiles), *sortMB)
	sortStart := time.Now()

	sortCfg := hashsort.ChunkSortConfig{
		MaxMemoryBytes: int64(*sortMB) * 1024 * 1024,
	}
	var chunksSorted int64
	var sortDeduped int64

	// Progress ticker for Phase 1b (chunk sort).
	p1bTicker := time.NewTicker(5 * time.Second)
	p1bDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-p1bTicker.C:
				sorted := atomic.LoadInt64(&chunksSorted)
				log.Printf("  [Phase 1b: chunk sort] sorted=%d/%d  elapsed=%s",
					sorted, len(chunkFiles),
					time.Since(sortStart).Truncate(time.Second))
			case <-p1bDone:
				return
			}
		}
	}()

	if err := hashsort.SortChunks(chunkFiles, sortCfg, &chunksSorted, &sortDeduped); err != nil {
		log.Fatalf("ERROR: chunk sort failed: %v", err)
	}

	p1bTicker.Stop()
	close(p1bDone)

	sortDedupCount := atomic.LoadInt64(&sortDeduped)
	log.Printf("Chunk sort complete: %d chunks sorted (%d sort-phase dups removed) in %s",
		chunksSorted, sortDedupCount, time.Since(sortStart).Truncate(time.Millisecond))

	// Count total unique entries across all chunks.
	allChunks := hashsort.AllChunkFiles(chunkDir)
	hashCount, _ := hashsort.CountEntriesMulti(allChunks)
	phase1Elapsed := time.Since(phaseStart)
	log.Printf("Phase 1 complete: %d catalogs, %d hashes consumed, %d unique entries, %d chunks, elapsed %s",
		atomic.LoadInt64(&catProg.CatalogsProcessed),
		atomic.LoadInt64(&hashesConsumed),
		hashCount,
		len(allChunks), phase1Elapsed.Truncate(time.Millisecond))

	// Snapshot the catalog hashes seen during Phase 1.
	// Because catalogs are content-addressed, any catalog with an unchanged
	// hash has unchanged content.  Phase 2b will skip these entirely.
	seenCatalogs := catProg.CatalogHashSet()
	log.Printf("Catalog hashes collected: %d (will skip in delta re-walk)", len(seenCatalogs))

	// ----------------------------------------------------------------
	// Phase 2a: Candidate collection (NO LOCK)
	// Each of the 256 byte prefixes is processed in parallel: list the
	// data/XX directory, merge-join against the sorted chunk-XX file.
	// ----------------------------------------------------------------
	log.Println("=== Phase 2a: Collecting deletion candidates (unlocked) ===")
	phase2aStart := time.Now()

	// Determine output file path for unreachable hashes.
	if *outputFile == "" {
		base := filepath.Base(*repoDir)
		*outputFile = base + "-gc-unreachable.txt"
	}
	outF, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("ERROR: creating output file %s: %v", *outputFile, err)
	}
	outBuf := bufio.NewWriterSize(outF, 1024*1024)

	sweepCfg := sweep.Config{
		DataDir:      dataDir,
		ChunkDir:     chunkDir,
		DryRun:       !*doDelete,
		OutputWriter: outBuf,
	}

	var sweepStats sweep.Stats

	// Collect candidates via merge-join (no deletion yet).
	candidateDone := make(chan error, 1)
	var candidates map[string]sweep.Candidate
	go func() {
		var e error
		candidates, e = sweep.CollectCandidates(sweepCfg, &sweepStats)
		candidateDone <- e
	}()

	// Progress ticker for Phase 2a.
	p2aTicker := time.NewTicker(5 * time.Second)
	func() {
		for {
			select {
			case <-p2aTicker.C:
				done := atomic.LoadInt64(&sweepStats.PrefixesDone)
				elapsed := time.Since(phase2aStart)
				eta := "--"
				if done > 0 {
					remaining := time.Duration(float64(elapsed) * float64(256-done) / float64(done))
					eta = remaining.Truncate(time.Second).String()
				}
				log.Printf("  [Phase 2a progress] prefixes=%d/256  files_checked=%d  candidates=%d  retained=%d  elapsed=%s  eta=%s",
					done,
					atomic.LoadInt64(&sweepStats.FilesChecked),
					atomic.LoadInt64(&sweepStats.CandidatesFound),
					atomic.LoadInt64(&sweepStats.FilesRetained),
					elapsed.Truncate(time.Second), eta)
			case err := <-candidateDone:
				if err != nil {
					log.Fatalf("ERROR: candidate collection failed: %v", err)
				}
				return
			}
		}
	}()
	p2aTicker.Stop()

	phase2aElapsed := time.Since(phase2aStart)
	log.Printf("Phase 2a complete: %d candidates from %d files checked, elapsed %s",
		len(candidates), sweepStats.FilesChecked, phase2aElapsed.Truncate(time.Millisecond))

	// ----------------------------------------------------------------
	// Phase 2b: Locked delta re-walk + deletion
	// ----------------------------------------------------------------
	var lockDuration time.Duration

	if len(candidates) == 0 {
		log.Println("=== Phase 2b: No candidates — skipping lock + deletion ===")
	} else {
		if *doDelete {
			log.Println("=== Phase 2b: Locked delta re-walk + deletion ===")
		} else {
			log.Println("=== Phase 2b: Delta re-walk (dry-run, no lock) ===")
		}
		lockStart := time.Now()

		// Acquire repository lock — only when actually deleting.
		if *doDelete && !*noLock {
			log.Printf("Acquiring repository locks in %s ...", *spoolDir)
			if err := repoLocks.Acquire(*spoolDir); err != nil {
				log.Fatalf("ERROR: failed to lock repository: %v", err)
			}
			log.Printf("Repository locked")
		}

		// Re-read the manifest — the root hash may have changed while
		// we were scanning (a new publish may have landed).
		newRootHash := *rootHash
		var newManifestHashes []catalog.Hash
		var newHistoryHash string
		if *manifestFile != "" {
			newManifest, err := parseManifest(*manifestFile)
			if err != nil {
				log.Printf("WARNING: re-reading manifest: %v (using original root hash)", err)
				newRootHash = *rootHash
			} else {
				newRootHash = newManifest.RootHash
				newManifestHashes = newManifest.ExtraHashes
				newHistoryHash = newManifest.HistoryHash
				if newRootHash != *rootHash {
					log.Printf("Root hash changed: %s → %s", *rootHash, newRootHash)
				}
			}
		}

		// Re-read the history DB to discover any new tagged roots
		// that may have been added while we were scanning.
		var newTaggedRoots []catalog.TaggedRoot
		if newHistoryHash != "" {
			historyCfg := catalog.HistoryConfig{
				DataDir: dataDir,
				TempDir: *tempDir,
			}
			newTaggedRoots, err = catalog.ReadTaggedRoots(historyCfg, newHistoryHash)
			if err != nil {
				log.Printf("WARNING: re-reading history DB: %v", err)
			}
		}

		// Collect all root hashes that need delta traversal: the
		// current root + all tagged roots.  TraverseNewCatalogs will
		// skip any catalog tree already seen in Phase 1.
		type deltaTraversal struct {
			label string
			hash  string
		}
		var deltaRoots []deltaTraversal
		deltaRoots = append(deltaRoots, deltaTraversal{label: "current", hash: newRootHash})
		for _, t := range newTaggedRoots {
			deltaRoots = append(deltaRoots, deltaTraversal{label: "tag:" + t.Name, hash: t.Hash})
		}

		// Launch delta traversals in parallel with private channels.
		var deltaProg catalog.Progress
		deltaPrivChs := make([]chan catalog.Hash, len(deltaRoots))
		deltaTraverseErrs := make(chan error, len(deltaRoots))
		for i, dr := range deltaRoots {
			deltaPrivChs[i] = make(chan catalog.Hash, 4096)
			go func(idx int, d deltaTraversal) {
				err := catalog.TraverseNewCatalogs(cfg, d.hash, seenCatalogs, deltaPrivChs[idx], &deltaProg)
				if err != nil {
					deltaTraverseErrs <- fmt.Errorf("delta %s root %s: %w", d.label, d.hash, err)
				}
			}(i, dr)
		}

		// Merge all delta channels and collect reachable hashes.
		reachable := make(map[string]struct{})
		{
			deltaMerged := make(chan catalog.Hash, 4096)
			var deltaMergeWg sync.WaitGroup
			for _, ch := range deltaPrivChs {
				deltaMergeWg.Add(1)
				go func(c <-chan catalog.Hash) {
					defer deltaMergeWg.Done()
					for h := range c {
						deltaMerged <- h
					}
				}(ch)
			}
			go func() {
				deltaMergeWg.Wait()
				close(deltaMerged)
			}()
			for h := range deltaMerged {
				reachable[h.String()] = struct{}{}
			}
		}

		// Check for delta traversal errors.
		close(deltaTraverseErrs)
		for err := range deltaTraverseErrs {
			log.Printf("WARNING: delta catalog re-walk: %v", err)
		}

		// Also include any manifest-referenced hashes from the re-read.
		for _, mh := range newManifestHashes {
			reachable[mh.String()] = struct{}{}
		}

		newCatalogs := atomic.LoadInt64(&deltaProg.CatalogsProcessed)
		newHashes := len(reachable)

		if newCatalogs > 0 {
			log.Printf("Delta re-walk: %d new catalogs, %d new hashes discovered",
				newCatalogs, newHashes)
		} else {
			log.Printf("Delta re-walk: no new catalogs (repo unchanged)")
		}

		// Subtract newly-reachable hashes from candidates.
		sweep.SubtractReachable(candidates, reachable, &sweepStats)
		if sweepStats.CandidatesProtected > 0 {
			log.Printf("Protected %d candidates that became reachable since scan start",
				sweepStats.CandidatesProtected)
		}

		// Delete the remaining candidates.
		sweep.DeleteCandidates(candidates, sweepCfg, &sweepStats)

		// Release lock.
		if *doDelete && !*noLock {
			repoLocks.Release()
		}
		lockDuration = time.Since(lockStart)
		if *doDelete {
			log.Printf("Phase 2b complete (lock held %s)", lockDuration.Truncate(time.Millisecond))
		} else {
			log.Printf("Phase 2b complete (would have held lock %s)", lockDuration.Truncate(time.Millisecond))
		}
	}

	stats := &sweepStats

	// Flush and close the output file.
	if err := outBuf.Flush(); err != nil {
		log.Printf("WARNING: flushing output file: %v", err)
	}
	outF.Close()

	phase2Elapsed := time.Since(phase2aStart)

	// Cleanup temp chunk files.
	for _, f := range allChunks {
		os.Remove(f)
	}
	os.Remove(chunkDir)

	// ----------------------------------------------------------------
	// Summary
	// ----------------------------------------------------------------
	totalElapsed := time.Since(startTime)
	log.Println("")
	log.Println("============================================")
	log.Println("  Garbage Collection Summary")
	log.Println("============================================")
	log.Printf("  Phase 1 (traverse + sort):   %s", phase1Elapsed.Truncate(time.Millisecond))
	log.Printf("    Phase 1a (prefix write):   %s", prefixWriteElapsed.Truncate(time.Millisecond))
	log.Printf("    Phase 1b (chunk sort):     %s", time.Since(sortStart).Truncate(time.Millisecond))
	log.Printf("    Catalog trees traversed:   %d (1 current + %d tagged)", nTrees, len(uniqueTaggedRoots))
	log.Printf("    Prefix chunks (non-empty): %d / 256", len(allChunks))
	log.Printf("    Unique reachable hashes:   %d", hashCount)
	if evictDedupCount > 0 || sortDedupCount > 0 {
		log.Printf("    Duplicates removed:        %d evict + %d sort = %d total",
			evictDedupCount, sortDedupCount, evictDedupCount+sortDedupCount)
	}
	log.Printf("    Catalogs seen:             %d", len(seenCatalogs))
	if skippedCatalogs > 0 {
		log.Printf("    Catalogs deduped:          %d (shared across tag trees)", skippedCatalogs)
	}
	log.Printf("  Phase 2a (candidate scan):   %s", phase2aElapsed.Truncate(time.Millisecond))
	log.Printf("    Files checked:             %d", stats.FilesChecked)
	log.Printf("    Files retained (reachable): %d", stats.FilesRetained)
	log.Printf("    Candidates found:          %d", stats.CandidatesFound)
	log.Printf("    Candidates protected (delta): %d", stats.CandidatesProtected)
	log.Printf("  Phase 2 total (scan+delete): %s", phase2Elapsed.Truncate(time.Millisecond))
	log.Printf("    Files to delete:           %d", stats.FilesDeleted)
	if stats.FilesChecked > 0 {
		pct := 100.0 * float64(stats.FilesDeleted) / float64(stats.FilesChecked)
		log.Printf("    Delete percentage:         %.1f%%", pct)
	}
	// Per-type breakdown of deletions.
	type suffixLabel struct {
		suffix byte
		label  string
	}
	suffixes := []suffixLabel{
		{0, "data (no suffix)"},
		{catalog.SuffixPartial, "partial chunks (P)"},
		{catalog.SuffixCatalog, "catalogs (C)"},
		{catalog.SuffixMicroCatalog, "micro-catalogs (L)"},
		{catalog.SuffixHistory, "history (H)"},
		{catalog.SuffixCertificate, "certificates (X)"},
		{catalog.SuffixMetainfo, "metainfo (M)"},
	}
	for _, sl := range suffixes {
		cnt := atomic.LoadInt64(&stats.DeletedBySuffix[sl.suffix])
		if cnt > 0 {
			typePct := 100.0 * float64(cnt) / float64(stats.FilesChecked)
			log.Printf("      %-24s %d (%.1f%% of total)", sl.label, cnt, typePct)
		}
	}
	log.Printf("    Unreachable hashes file:   %s", *outputFile)
	log.Printf("    Errors:                    %d", stats.Errors)
	if *doDelete && stats.BytesFreed > 0 {
		log.Printf("    Space freed:               %s", humanBytes(stats.BytesFreed))
	}
	log.Println("  ──────────────────────────────────────────")
	log.Printf("  Total elapsed:               %s", totalElapsed.Truncate(time.Millisecond))
	if lockDuration > 0 && *doDelete {
		log.Printf("  Lock held time:              %s", lockDuration.Truncate(time.Millisecond))
	} else if lockDuration > 0 {
		log.Printf("  Lock held time (est.):       %s (dry-run, no lock taken)", lockDuration.Truncate(time.Millisecond))
	} else if *noLock {
		log.Printf("  Lock held time:              (locking disabled)")
	} else {
		log.Printf("  Lock held time:              0 (no candidates)")
	}
	if !*doDelete {
		log.Println("")
		log.Println("  ** No files were deleted (pass -delete to actually remove) **")
	}
	log.Println("============================================")
}

// manifestInfo holds parsed information from a .cvmfspublished file.
type manifestInfo struct {
	RootHash           string         // C line: root catalog hash
	HistoryHash        string         // H line: history database hash (empty if absent)
	ExtraHashes        []catalog.Hash // H, X, M objects as reachable hashes
	GarbageCollectable bool           // G line: true if GC is permitted
	RepoName           string         // N line: repository name
}

// parseManifest reads a .cvmfspublished file and extracts:
//   - the root catalog hash (C line)
//   - the history database hash (H line)
//   - additional reachable object hashes: history (H), certificate (X),
//     metainfo (M)
//   - the garbage-collectable flag (G line)
//   - the repository name (N line)
//
// Manifest line format:
//
//	C<hash>  - root catalog content hash
//	B<size>  - catalog size
//	R<md5>   - root path hash
//	H<hash>  - history database hash
//	X<hash>  - certificate hash
//	M<hash>  - metainfo hash
//	S<rev>   - revision number
//	G<bool>  - garbage-collectable flag
//	N<name>  - repository name
func parseManifest(path string) (*manifestInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	info := &manifestInfo{
		// Default to true: if G line is absent, assume GC is allowed
		// (older manifests may not have it).
		GarbageCollectable: true,
	}

	for _, line := range splitLines(string(data)) {
		if len(line) < 2 {
			continue
		}
		value := line[1:]
		switch line[0] {
		case 'C':
			info.RootHash = value
		case 'H':
			info.HistoryHash = value
			info.ExtraHashes = append(info.ExtraHashes, catalog.Hash{Hex: value, Suffix: catalog.SuffixHistory})
		case 'X':
			info.ExtraHashes = append(info.ExtraHashes, catalog.Hash{Hex: value, Suffix: catalog.SuffixCertificate})
		case 'M':
			info.ExtraHashes = append(info.ExtraHashes, catalog.Hash{Hex: value, Suffix: catalog.SuffixMetainfo})
		case 'G':
			info.GarbageCollectable = (value == "yes" || value == "true" || value == "1")
		case 'N':
			info.RepoName = value
		}
	}

	if info.RootHash == "" {
		return nil, fmt.Errorf("root catalog hash (C line) not found in manifest")
	}

	return info, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
