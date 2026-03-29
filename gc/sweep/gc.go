package sweep

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/catalog"
	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/repolock"
)

// GCConfig controls a full garbage collection cycle.
type GCConfig struct {
	RepoDir     string // local repo root (contains data/, packs/, .cvmfspublished)
	Parallelism int    // catalog traversal parallelism
	TempDir     string // temp directory for chunk files; auto-cleaned if empty
	DryRun      bool
}

// GCStats reports the result of a GC cycle.
type GCStats struct {
	CatalogsProcessed int64
	CatalogsSkipped   int64
	HashesConsumed    int64
	UniqueHashes      int64
	TaggedRoots       int
	Compact           CompactStats
	Elapsed           time.Duration
}

// RunGCCycle performs a complete garbage collection:
//  1. Parse the local manifest for root hash + history.
//  2. Discover tagged roots from the history database.
//  3. Traverse all catalog trees → hashsort → sorted chunk files.
//  4. Always merge all packs, then delete unreachable loose files.
//  5. Report reclaimable volume after merging.
func RunGCCycle(cfg GCConfig) (*GCStats, error) {
	start := time.Now()
	stats := &GCStats{}

	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}

	dataDir := filepath.Join(cfg.RepoDir, "data")
	packsDir := filepath.Join(cfg.RepoDir, "packs")

	// Acquire both locks: GC lock to prevent concurrent GC, and update
	// lock to prevent concurrent mirrors from modifying the repo.
	var locks repolock.Set
	if err := locks.Acquire(cfg.RepoDir); err != nil {
		return nil, fmt.Errorf("acquiring locks: %w", err)
	}
	defer locks.Release()

	// ---- Temp directory ----
	tempDir := cfg.TempDir
	ownTemp := false
	if tempDir == "" {
		td, err := os.MkdirTemp("", "cvmfs-gc-*")
		if err != nil {
			return nil, fmt.Errorf("creating temp dir: %w", err)
		}
		tempDir = td
		ownTemp = true
	}
	if ownTemp {
		defer os.RemoveAll(tempDir)
	}

	// ---- Parse manifest ----
	mfPath := filepath.Join(cfg.RepoDir, ".cvmfspublished")
	mf, err := parseLocalManifest(mfPath)
	if err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	log.Printf("[GC] Root catalog: %s", mf.rootHash)

	// ---- Read history → tagged roots ----
	var taggedRoots []catalog.TaggedRoot
	if mf.historyHash != "" {
		hCfg := catalog.HistoryConfig{
			DataDir: dataDir,
			TempDir: tempDir,
		}
		taggedRoots, err = catalog.ReadTaggedRoots(hCfg, mf.historyHash)
		if err != nil {
			log.Printf("[GC] WARNING: reading history: %v", err)
		} else {
			log.Printf("[GC] Tagged roots: %d", len(taggedRoots))
		}
	}
	stats.TaggedRoots = len(taggedRoots)

	// Deduplicate tagged roots against the current root hash.
	var uniqueTaggedRoots []catalog.TaggedRoot
	for _, t := range taggedRoots {
		if t.Hash != mf.rootHash {
			uniqueTaggedRoots = append(uniqueTaggedRoots, t)
		}
	}
	nTrees := 1 + len(uniqueTaggedRoots)

	// ---- Phase 1: Catalog traversal + hashsort ----
	log.Printf("[GC] Phase 1: traversing %d catalog tree(s)", nTrees)

	traverseCfg := catalog.TraverseConfig{
		DataDir:     dataDir,
		Parallelism: cfg.Parallelism,
		TempDir:     tempDir,
	}

	var catProg catalog.Progress
	traverseErrs := make(chan error, nTrees)

	// Private channel per traversal.
	privateChs := make([]chan catalog.Hash, nTrees)
	for i := range privateChs {
		privateChs[i] = make(chan catalog.Hash, 4096)
	}

	// Current root.
	go func() {
		err := catalog.TraverseFromRootHash(traverseCfg, mf.rootHash, privateChs[0], &catProg)
		if err != nil {
			traverseErrs <- fmt.Errorf("current root: %w", err)
		}
	}()

	// Tagged roots.
	for i, t := range uniqueTaggedRoots {
		go func(idx int, tag catalog.TaggedRoot) {
			err := catalog.TraverseFromRootHash(traverseCfg, tag.Hash, privateChs[idx+1], &catProg)
			if err != nil {
				traverseErrs <- fmt.Errorf("tag %q: %w", tag.Name, err)
			}
		}(i, t)
	}

	// Merge all traversal channels into a single entry channel.
	entryCh := make(chan hashsort.Entry, 8192)
	go func() {
		// Inject manifest-referenced hashes.
		for _, mh := range mf.extraHashes {
			entryCh <- hashsort.EntryFromHexSuffix(mh.Hex, mh.Suffix)
		}

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

	// Prefix write → chunk files.
	chunkDir := filepath.Join(tempDir, "chunks")
	prefixCfg := hashsort.PrefixWriterConfig{HeapBytes: 1024 * 1024}
	var hashesConsumed int64
	var evictDeduped int64

	chunkFiles, err := hashsort.PrefixWrite(prefixCfg, entryCh, chunkDir, &hashesConsumed, &evictDeduped, nil)
	if err != nil {
		return nil, fmt.Errorf("prefix write: %w", err)
	}

	// Check for traversal errors.
	close(traverseErrs)
	for err := range traverseErrs {
		return nil, fmt.Errorf("catalog traversal: %w", err)
	}

	stats.HashesConsumed = atomic.LoadInt64(&hashesConsumed)
	stats.CatalogsProcessed = atomic.LoadInt64(&catProg.CatalogsProcessed)
	stats.CatalogsSkipped = atomic.LoadInt64(&catProg.CatalogsSkipped)

	// Sort + deduplicate chunk files.
	sortCfg := hashsort.ChunkSortConfig{MaxMemoryBytes: 100 * 1024 * 1024}
	var chunksSorted, sortDeduped int64
	if err := hashsort.SortChunks(chunkFiles, sortCfg, &chunksSorted, &sortDeduped); err != nil {
		return nil, fmt.Errorf("chunk sort: %w", err)
	}

	// Count unique hashes.
	allChunks := hashsort.AllChunkFiles(chunkDir)
	uniqueCount, _ := hashsort.CountEntriesMulti(allChunks)
	stats.UniqueHashes = uniqueCount

	log.Printf("[GC] Phase 1 complete: %d catalogs, %d unique hashes",
		stats.CatalogsProcessed, stats.UniqueHashes)

	// ---- Phase 2: Merge packs + delete unreachable loose files ----
	log.Printf("[GC] Phase 2: merging packs + cleaning loose files")

	compactCfg := CompactConfig{
		DataDir:  dataDir,
		PacksDir: packsDir,
		ChunkDir: chunkDir,
		DryRun:   cfg.DryRun,
	}
	if err := CompactAll(compactCfg, &stats.Compact); err != nil {
		return nil, fmt.Errorf("compaction: %w", err)
	}

	stats.Elapsed = time.Since(start)

	log.Printf("[GC] Complete: packs_read=%d packs_written=%d "+
		"pack_kept=%d pack_removed=%d "+
		"loose_deleted=%d loose_bytes_freed=%d "+
		"pack_bytes_reclaimed=%d elapsed=%s",
		stats.Compact.PacksRead, stats.Compact.PacksWritten,
		stats.Compact.PackObjectsKept, stats.Compact.PackObjectsRemoved,
		stats.Compact.LooseFilesDeleted, stats.Compact.LooseBytesFreed,
		stats.Compact.PackBytesReclaimed, stats.Elapsed.Truncate(time.Millisecond))

	// Update .cvmfs_status.json with last_gc timestamp.
	writeGCStatus(cfg.RepoDir)

	return stats, nil
}

// localManifest holds parsed manifest fields needed for GC.
type localManifest struct {
	rootHash    string
	historyHash string
	extraHashes []catalog.Hash
}

// parseLocalManifest reads a .cvmfspublished file on disk.
func parseLocalManifest(path string) (*localManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	mf := &localManifest{}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if len(line) < 2 {
			continue
		}
		value := line[1:]
		switch line[0] {
		case 'C':
			mf.rootHash = value
		case 'H':
			mf.historyHash = value
			mf.extraHashes = append(mf.extraHashes, catalog.Hash{
				Hex: value, Suffix: catalog.SuffixHistory,
			})
		case 'X':
			mf.extraHashes = append(mf.extraHashes, catalog.Hash{
				Hex: value, Suffix: catalog.SuffixCertificate,
			})
		case 'M':
			mf.extraHashes = append(mf.extraHashes, catalog.Hash{
				Hex: value, Suffix: catalog.SuffixMetainfo,
			})
		}
	}

	if mf.rootHash == "" {
		return nil, fmt.Errorf("no root catalog hash (C line) in manifest")
	}
	return mf, nil
}

// writeGCStatus updates .cvmfs_status.json with the last_gc timestamp.
func writeGCStatus(repoDir string) {
	now := time.Now().UTC().Format("Mon Jan  2 15:04:05 UTC 2006")
	statusPath := filepath.Join(repoDir, ".cvmfs_status.json")
	statusMap := map[string]interface{}{}
	if data, err := os.ReadFile(statusPath); err == nil {
		json.Unmarshal(data, &statusMap)
	}
	statusMap["last_gc"] = now
	if out, err := json.MarshalIndent(statusMap, "", "  "); err == nil {
		os.WriteFile(statusPath, append(out, '\n'), 0644)
	}
}
