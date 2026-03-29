package sweep

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/packfile"
)

const (
	// MaxGCConcurrency limits the number of prefixes being compacted
	// in parallel.
	MaxGCConcurrency = 10
)

// CompactConfig controls pack-aware garbage collection.
type CompactConfig struct {
	DataDir  string // repo data/ directory
	PacksDir string // repo packs/ directory
	ChunkDir string // directory containing sorted chunk-XX files (reachable set)
	DryRun   bool
}

// CompactStats tracks compaction progress and results.
type CompactStats struct {
	PrefixesDone       int64
	PacksRead          int64
	PacksWritten       int64
	PackObjectsKept    int64
	PackObjectsRemoved int64
	LooseFilesKept     int64
	LooseFilesDeleted  int64
	LooseBytesFreed    int64
	PackBytesReclaimed int64 // old pack size − new pack size
}

// CompactAll runs pack merging + loose file cleanup for all 256 prefixes.
//
// Pack files are ALWAYS merged: for each prefix, all existing packs are
// consolidated into new packs containing only reachable objects.  After
// merging, unreachable loose files are deleted.  Reclaimable volume is
// computed after the merge is complete.
func CompactAll(cfg CompactConfig, stats *CompactStats) error {
	if stats == nil {
		stats = &CompactStats{}
	}

	sem := make(chan struct{}, MaxGCConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i := 0; i < 256; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(prefix byte) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := compactPrefix(cfg, prefix, stats); err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("prefix %02x: %w", prefix, err)
				})
			}
			atomic.AddInt64(&stats.PrefixesDone, 1)
		}(byte(i))
	}

	wg.Wait()
	return firstErr
}

// compactPrefix handles a single byte prefix.
func compactPrefix(cfg CompactConfig, prefix byte, stats *CompactStats) error {
	prefixHex := fmt.Sprintf("%02x", prefix)

	// Build the reachable set from the chunk file for this prefix.
	reachable, err := loadReachableSet(cfg.ChunkDir, prefix)
	if err != nil {
		return err
	}

	// Always merge packs.
	packDir := filepath.Join(cfg.PacksDir, prefixHex)
	if err := mergePacksForPrefix(packDir, prefix, reachable, cfg.DryRun, stats); err != nil {
		return fmt.Errorf("pack merge: %w", err)
	}

	// Clean up unreachable loose files.
	dataDir := filepath.Join(cfg.DataDir, prefixHex)
	if err := cleanLooseFiles(dataDir, prefixHex, reachable, cfg.DryRun, stats); err != nil {
		return fmt.Errorf("loose cleanup: %w", err)
	}

	return nil
}

// loadReachableSet reads the chunk file for a prefix and returns the set
// of reachable 20-byte hashes.  The suffix byte is ignored — a hash is
// considered reachable if any suffix variant appears in the chunk file.
//
// Returns nil (not an empty map) if no chunk file exists, meaning no
// reachable objects have this prefix.
func loadReachableSet(chunkDir string, prefix byte) (map[[20]byte]bool, error) {
	chunkPath := hashsort.ChunkFileForPrefix(chunkDir, prefix)

	reader, err := hashsort.NewChunkReader(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer reader.Close()

	reachable := make(map[[20]byte]bool)
	for reader.Next() {
		entry := reader.Value()
		var hash [20]byte
		copy(hash[:], entry[:20])
		reachable[hash] = true
	}
	return reachable, nil
}

// sourceEntry describes a reachable object found in an existing pack.
type sourceEntry struct {
	hash     [20]byte
	packPath string
	offset   int64
	length   int64
}

// mergePacksForPrefix consolidates all packs for a prefix into new packs
// containing only reachable objects.  This is always performed regardless
// of how much space is reclaimable — the reclaimed volume is computed
// after the merge completes.
func mergePacksForPrefix(packDir string, prefix byte, reachable map[[20]byte]bool, dryRun bool, stats *CompactStats) error {
	// Load all existing pack indices.
	ps := packfile.NewPrefixState(packDir)
	if err := ps.Refresh(); err != nil {
		return err
	}

	indices := ps.Indices()
	if len(indices) == 0 {
		return nil // no packs to merge
	}

	atomic.AddInt64(&stats.PacksRead, int64(len(indices)))

	// Collect old pack/idx paths and sizes.
	type oldPackInfo struct {
		packPath string
		idxPath  string
		size     int64
	}
	var oldPacks []oldPackInfo
	var oldTotalSize int64

	for _, idx := range indices {
		idxPath := strings.TrimSuffix(idx.PackPath, ".pack") + ".idx"
		var sz int64
		if fi, err := os.Stat(idx.PackPath); err == nil {
			sz = fi.Size()
		}
		oldPacks = append(oldPacks, oldPackInfo{idx.PackPath, idxPath, sz})
		oldTotalSize += sz
	}

	// Collect reachable entries, deduplicating across packs.
	var reachableEntries []sourceEntry
	seen := make(map[[20]byte]bool)

	for _, idx := range indices {
		idx.Iterate(func(e packfile.IndexEntry) bool {
			if seen[e.Hash] {
				return true // already collected from another pack
			}
			seen[e.Hash] = true

			if reachable != nil && reachable[e.Hash] {
				off, ln, ok := idx.Lookup(e.Hash)
				if ok {
					reachableEntries = append(reachableEntries, sourceEntry{
						hash:     e.Hash,
						packPath: idx.PackPath,
						offset:   off,
						length:   ln,
					})
				}
				atomic.AddInt64(&stats.PackObjectsKept, 1)
			} else {
				atomic.AddInt64(&stats.PackObjectsRemoved, 1)
			}
			return true
		})
	}

	if dryRun {
		return nil
	}

	// Write reachable entries to new pack(s).
	var newTotalSize int64

	if len(reachableEntries) > 0 {
		pw, err := packfile.NewPackWriter(packDir, prefix)
		if err != nil {
			return err
		}

		for _, se := range reachableEntries {
			or, err := packfile.OpenObject(se.packPath, se.offset, se.length)
			if err != nil {
				log.Printf("WARNING: reading pack object %x: %v", se.hash, err)
				continue
			}

			_, _, err = pw.AppendReader(se.hash, or)
			or.Close()
			if err != nil {
				log.Printf("WARNING: writing pack object %x: %v", se.hash, err)
				continue
			}

			// Rotate pack if it exceeds the index memory budget.
			if pw.Count() >= packfile.MaxPackEntries {
				name, err := pw.Finalize()
				if err != nil {
					return fmt.Errorf("finalizing pack: %w", err)
				}
				if name != "" {
					np := filepath.Join(packDir, name+".pack")
					if fi, err := os.Stat(np); err == nil {
						newTotalSize += fi.Size()
					}
					atomic.AddInt64(&stats.PacksWritten, 1)
				}
				pw, err = packfile.NewPackWriter(packDir, prefix)
				if err != nil {
					return fmt.Errorf("creating new pack: %w", err)
				}
			}
		}

		// Finalize the last pack.
		name, err := pw.Finalize()
		if err != nil {
			return fmt.Errorf("finalizing pack: %w", err)
		}
		if name != "" {
			np := filepath.Join(packDir, name+".pack")
			if fi, err := os.Stat(np); err == nil {
				newTotalSize += fi.Size()
			}
			atomic.AddInt64(&stats.PacksWritten, 1)
		}
	}

	// Delete old packs + indices (new ones are already finalized).
	for _, op := range oldPacks {
		os.Remove(op.packPath)
		os.Remove(op.idxPath)
	}

	// Report reclaimed bytes.
	if oldTotalSize > newTotalSize {
		atomic.AddInt64(&stats.PackBytesReclaimed, oldTotalSize-newTotalSize)
	}

	return nil
}

// cleanLooseFiles deletes unreachable loose files in a data/XX/ directory.
func cleanLooseFiles(dataDir, prefixHex string, reachable map[[20]byte]bool, dryRun bool, stats *CompactStats) error {
	entries, err := os.ReadDir(dataDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()

		// Parse the hash from the filename.  data/XX/<rest>[<suffix>]
		// where <rest> is 38 hex chars and suffix is an optional
		// single uppercase letter.
		hashHex := prefixHex + name
		if len(hashHex) > 40 {
			hashHex = hashHex[:40] // strip type suffix
		}
		if len(hashHex) != 40 {
			continue // not a valid hash file
		}

		var binHash [20]byte
		if _, err := hex.Decode(binHash[:], []byte(hashHex)); err != nil {
			continue
		}

		if reachable != nil && reachable[binHash] {
			atomic.AddInt64(&stats.LooseFilesKept, 1)
			continue
		}

		// Unreachable — delete.
		if dryRun {
			atomic.AddInt64(&stats.LooseFilesDeleted, 1)
			continue
		}

		filePath := filepath.Join(dataDir, name)
		var sz int64
		if fi, err := os.Lstat(filePath); err == nil {
			sz = fi.Size()
		}

		if err := os.Remove(filePath); err != nil {
			if !os.IsNotExist(err) {
				log.Printf("WARNING: deleting %s: %v", filePath, err)
			}
			continue
		}
		atomic.AddInt64(&stats.LooseFilesDeleted, 1)
		atomic.AddInt64(&stats.LooseBytesFreed, sz)
	}

	return nil
}

// -----------------------------------------------------------------------
// Merge-only consolidation (no reachability filtering)
// -----------------------------------------------------------------------

// MergeFragmented consolidates pack files for any prefix that has more
// than `threshold` pack files.  Unlike a full GC cycle, this keeps ALL
// objects — it just rewrites multiple small packs into fewer large ones.
// This is useful at mirror startup to clean up fragmentation left by
// interrupted runs.
//
// Returns the number of prefixes that were merged.
func MergeFragmented(packsDir string, threshold int, store *packfile.Store) (int, error) {
	// First pass: identify which prefixes need merging.
	type mergeJob struct {
		prefix    byte
		packCount int
		objCount  int
	}
	var jobs []mergeJob
	for i := 0; i < 256; i++ {
		ps := store.Prefix(byte(i))
		n := ps.IndexCount()
		if n <= threshold {
			continue
		}
		jobs = append(jobs, mergeJob{byte(i), n, ps.ObjectCount()})
	}

	total := len(jobs)
	if total == 0 {
		return 0, nil
	}

	log.Printf("  [Merge] %d prefixes to consolidate (concurrency=%d)", total, MaxGCConcurrency)

	start := time.Now()
	var done int64
	var merged int64
	var lastPrint int64 // unix-nano of last progress line
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	sem := make(chan struct{}, MaxGCConcurrency)

	for _, job := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j mergeJob) {
			defer wg.Done()
			defer func() { <-sem }()

			packDir := filepath.Join(packsDir, fmt.Sprintf("%02x", j.prefix))
			if err := mergePacksKeepAll(packDir, j.prefix); err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("merge prefix %02x: %w", j.prefix, err)
				})
				atomic.AddInt64(&done, 1)
				return
			}
			atomic.AddInt64(&merged, 1)
			d := atomic.AddInt64(&done, 1)

			// Print progress at most every 5 seconds, plus the final one.
			now := time.Now().UnixNano()
			last := atomic.LoadInt64(&lastPrint)
			isFinal := d >= int64(total)
			if !isFinal && now-last < int64(5*time.Second) {
				return
			}
			if !atomic.CompareAndSwapInt64(&lastPrint, last, now) && !isFinal {
				return // another goroutine just printed
			}

			elapsed := time.Since(start)
			var eta string
			if d > 0 && !isFinal {
				remaining := time.Duration(float64(elapsed) / float64(d) * float64(int64(total)-d))
				eta = fmt.Sprintf("  ETA %s", remaining.Truncate(time.Second))
			}
			log.Printf("  [Merge] %d/%d prefixes  %s%s",
				d, total, elapsed.Truncate(time.Second), eta)
		}(job)
	}

	wg.Wait()
	log.Printf("  [Merge] completed %d prefixes in %s", total, time.Since(start).Truncate(time.Second))
	return int(merged), firstErr
}

// mergePacksKeepAll consolidates all packs for a prefix into the minimum
// number of packs, keeping every object.  No reachability filtering.
func mergePacksKeepAll(packDir string, prefix byte) error {
	ps := packfile.NewPrefixState(packDir)
	if err := ps.Refresh(); err != nil {
		return err
	}

	indices := ps.Indices()
	if len(indices) <= 1 {
		return nil // nothing to merge
	}

	// Collect all entries, deduplicating across packs.
	var entries []sourceEntry
	seen := make(map[[20]byte]bool)

	for _, idx := range indices {
		idx.Iterate(func(e packfile.IndexEntry) bool {
			if seen[e.Hash] {
				return true
			}
			seen[e.Hash] = true

			off, ln, ok := idx.Lookup(e.Hash)
			if ok {
				entries = append(entries, sourceEntry{
					hash:     e.Hash,
					packPath: idx.PackPath,
					offset:   off,
					length:   ln,
				})
			}
			return true
		})
	}

	// Collect old pack/idx paths.
	type oldFile struct {
		packPath string
		idxPath  string
	}
	var oldPacks []oldFile
	for _, idx := range indices {
		idxPath := strings.TrimSuffix(idx.PackPath, ".pack") + ".idx"
		oldPacks = append(oldPacks, oldFile{idx.PackPath, idxPath})
	}

	// Write consolidated packs.
	if len(entries) > 0 {
		pw, err := packfile.NewPackWriter(packDir, prefix)
		if err != nil {
			return err
		}

		for _, se := range entries {
			or, err := packfile.OpenObject(se.packPath, se.offset, se.length)
			if err != nil {
				log.Printf("WARNING: reading pack object %x during merge: %v", se.hash, err)
				continue
			}

			_, _, err = pw.AppendReader(se.hash, or)
			or.Close()
			if err != nil {
				log.Printf("WARNING: writing pack object %x during merge: %v", se.hash, err)
				continue
			}

			if pw.Count() >= packfile.MaxPackEntries {
				if _, err := pw.Finalize(); err != nil {
					return fmt.Errorf("finalizing merged pack: %w", err)
				}
				pw, err = packfile.NewPackWriter(packDir, prefix)
				if err != nil {
					return fmt.Errorf("creating new merged pack: %w", err)
				}
			}
		}

		if _, err := pw.Finalize(); err != nil {
			return fmt.Errorf("finalizing last merged pack: %w", err)
		}
	}

	// Delete old packs + indices.
	for _, op := range oldPacks {
		os.Remove(op.packPath)
		os.Remove(op.idxPath)
	}

	return nil
}
