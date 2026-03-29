// Package sweep implements the GC sweep phase for CVMFS.
//
// The sweep uses a two-pass algorithm designed to minimise the time the
// repository lock is held:
//
// Pass 1 — Candidate Collection (no lock required):
//
//  1. For each of the 256 byte prefixes (00..ff), open the corresponding
//     sorted binary chunk file (if it exists) and the matching directory
//     in the data store.  Merge-join the sorted directory entries against
//     the chunk reader.  Any entry not matched is added to the candidate
//     set.
//
//  2. Because each chunk file maps 1:1 to a directory prefix, all 256
//     prefixes can be processed in parallel.  Parallelism is bounded by
//     the ReadAhead configuration (default 16).
//
// Pass 2 — Locked Deletion:
//
//  4. Acquire the repository lock.
//  5. Re-walk all catalogs from the (possibly updated) manifest to
//     discover hashes that became reachable while we were scanning.
//  6. Remove those hashes from the candidate set.
//  7. Delete the remaining candidates.
//  8. Release the lock.
//
// The existing Run() function preserves the original single-pass API for
// backward compatibility and testing.
package sweep

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
)

// Config holds configuration for the sweep phase.
type Config struct {
	// DataDir is the CVMFS data directory containing 00..ff subdirs.
	DataDir string
	// ChunkDir is the directory containing per-prefix chunk files
	// (chunk-00 through chunk-ff) produced by hashsort.PrefixWrite +
	// hashsort.SortChunks.
	ChunkDir string
	// DryRun logs deletions without actually removing files.
	DryRun bool
	// OutputWriter, if non-nil, receives one hash per line for every
	// unreachable object found during dry-run or deletion.  The caller
	// is responsible for flushing and closing the underlying file.
	OutputWriter *bufio.Writer
	// ReadAhead controls how many prefix directories to process in
	// parallel.  Defaults to 16 if <= 0.
	ReadAhead int
}

// Stats holds statistics about the sweep operation.
// Fields are updated atomically and can be read concurrently for progress
// reporting while the sweep is running.
type Stats struct {
	FilesChecked  int64
	FilesDeleted  int64
	FilesRetained int64
	BytesFreed    int64
	Errors        int64
	PrefixesDone  int64 // 0..256: how many of the 00..ff prefixes are complete

	// Per-suffix deletion counters.  Updated atomically.
	// Suffixes: 0=none, 'C'=catalog, 'P'=partial, 'L'=micro-catalog,
	// 'H'=history, 'X'=certificate, 'M'=metainfo.
	DeletedBySuffix [256]int64

	// Two-pass specific counters.
	CandidatesFound     int64 // after pass 1 merge-join
	CandidatesProtected int64 // removed by pass 2 delta
}

// Candidate represents a file identified as potentially unreachable.
type Candidate struct {
	// FullHash is prefix + filename (e.g. "aa" + "bcdef...").
	FullHash string
	// FilePath is the absolute path on disk.
	FilePath string
}

// suffixFromName extracts the CVMFS hash suffix from a filename.  CVMFS
// filenames are the hash hex (minus the 2-char prefix directory), optionally
// followed by a single ASCII suffix byte (C, P, L, H, X, M).
func suffixFromName(name string) byte {
	if len(name) == 0 {
		return 0
	}
	last := name[len(name)-1]
	if last >= 'A' && last <= 'Z' {
		return last
	}
	return 0
}

// ===================================================================
// Pass 1: Candidate Collection (parallel, one prefix at a time)
// ===================================================================

// CollectCandidates performs the merge-join sweep and returns a map of
// unreachable hash → Candidate.  No files are deleted.
//
// Each of the 256 prefixes is processed independently:
//   - Open the per-prefix chunk file (if it exists)
//   - List the corresponding data directory
//   - Merge-join directory entries against the sorted chunk entries
//
// This is embarrassingly parallel since each prefix has its own chunk
// file and directory.
//
// The stats argument is updated atomically for progress reporting.
func CollectCandidates(cfg Config, stats *Stats) (map[string]Candidate, error) {
	if stats == nil {
		stats = &Stats{}
	}

	readAhead := cfg.ReadAhead
	if readAhead <= 0 {
		readAhead = 16
	}

	var mu sync.Mutex
	candidates := make(map[string]Candidate)

	// Semaphore for parallelism.
	sem := make(chan struct{}, readAhead)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i := 0; i < 256; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(prefixIdx int) {
			defer wg.Done()
			defer func() { <-sem }()

			prefix := fmt.Sprintf("%02x", prefixIdx)
			local, err := collectPrefix(cfg, prefix, byte(prefixIdx), stats)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("prefix %s: %w", prefix, err)
				})
				atomic.AddInt64(&stats.PrefixesDone, 1)
				return
			}

			if len(local) > 0 {
				mu.Lock()
				for k, v := range local {
					candidates[k] = v
				}
				mu.Unlock()
			}

			atomic.AddInt64(&stats.PrefixesDone, 1)
		}(i)
	}

	wg.Wait()
	return candidates, firstErr
}

// collectPrefix processes a single byte prefix directory, merge-joining
// the directory listing against the sorted chunk file for that prefix.
func collectPrefix(cfg Config, prefix string, prefixByte byte, stats *Stats) (map[string]Candidate, error) {
	dirPath := filepath.Join(cfg.DataDir, prefix)

	// Read directory entries.
	dirEntries, err := os.ReadDir(dirPath)
	if os.IsNotExist(err) {
		// Directory doesn't exist — skip the chunk too.
		return nil, nil
	}
	if err != nil {
		log.Printf("WARNING: reading directory %s: %v", dirPath, err)
		atomic.AddInt64(&stats.Errors, 1)
		return nil, nil
	}

	// Filter to regular files and build sorted file-hash list.
	type fileEntry struct {
		fullHash string // prefix + filename
		filePath string
	}
	var files []fileEntry
	for _, e := range dirEntries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		fullHash := prefix + name
		files = append(files, fileEntry{
			fullHash: fullHash,
			filePath: filepath.Join(dirPath, name),
		})
	}

	if len(files) == 0 {
		return nil, nil
	}

	// Sort the file entries by fullHash for merge-join.
	sort.Slice(files, func(i, j int) bool {
		return files[i].fullHash < files[j].fullHash
	})

	// Open the chunk file for this prefix, if it exists.
	chunkPath := hashsort.ChunkFileForPrefix(cfg.ChunkDir, prefixByte)
	chunkExists := false
	if _, err := os.Stat(chunkPath); err == nil {
		chunkExists = true
	}

	candidates := make(map[string]Candidate)

	if !chunkExists {
		// No reachable hashes for this prefix — everything is a candidate.
		for _, f := range files {
			atomic.AddInt64(&stats.FilesChecked, 1)
			candidates[f.fullHash] = Candidate{
				FullHash: f.fullHash,
				FilePath: f.filePath,
			}
			atomic.AddInt64(&stats.CandidatesFound, 1)
		}
		return candidates, nil
	}

	// Open chunk reader.
	reader, err := hashsort.NewChunkReader(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("opening chunk reader %s: %w", chunkPath, err)
	}
	defer reader.Close()

	readerValid := reader.Next()

	for _, f := range files {
		atomic.AddInt64(&stats.FilesChecked, 1)

		// To compare, convert the file's fullHash to the Entry string
		// format.  The chunk entries are binary but we need string
		// comparison against the file's fullHash (which is hex + optional
		// suffix).
		fileHashStr := f.fullHash

		// Advance the reader past entries that sort before this file.
		for readerValid {
			readerStr := reader.ValueString()
			if readerStr >= fileHashStr {
				break
			}
			readerValid = reader.Next()
		}

		if readerValid && reader.ValueString() == fileHashStr {
			// Reachable.
			atomic.AddInt64(&stats.FilesRetained, 1)
			readerValid = reader.Next()
			continue
		}

		// Unreachable — record as candidate.
		candidates[f.fullHash] = Candidate{
			FullHash: f.fullHash,
			FilePath: f.filePath,
		}
		atomic.AddInt64(&stats.CandidatesFound, 1)
	}

	return candidates, nil
}

// ===================================================================
// Pass 2: Delta Subtraction
// ===================================================================

// SubtractReachable removes any hash present in the reachable set from the
// candidates map.  The reachable set is typically built from a catalog
// re-walk performed under the repository lock.
func SubtractReachable(candidates map[string]Candidate, reachable map[string]struct{}, stats *Stats) {
	var removed int64
	for hash := range reachable {
		if _, ok := candidates[hash]; ok {
			delete(candidates, hash)
			removed++
		}
	}
	if stats != nil {
		atomic.AddInt64(&stats.CandidatesProtected, removed)
	}
}

// ===================================================================
// Deletion
// ===================================================================

// DeleteCandidates removes (or records in dry-run mode) the remaining
// candidates.  It updates stats.FilesDeleted, BytesFreed, DeletedBySuffix,
// and Errors.
func DeleteCandidates(candidates map[string]Candidate, cfg Config, stats *Stats) {
	if stats == nil {
		stats = &Stats{}
	}

	for _, c := range candidates {
		name := c.FullHash
		suf := suffixFromName(name)
		atomic.AddInt64(&stats.DeletedBySuffix[suf], 1)

		if cfg.OutputWriter != nil {
			fmt.Fprintln(cfg.OutputWriter, c.FullHash)
		}

		if cfg.DryRun {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			continue
		}

		// Stat the file just before removal to get accurate size.
		var sz int64
		if fi, err := os.Lstat(c.FilePath); err == nil {
			sz = fi.Size()
		}

		if err := os.Remove(c.FilePath); err != nil {
			if !os.IsNotExist(err) {
				log.Printf("WARNING: failed to delete %s: %v", c.FilePath, err)
				atomic.AddInt64(&stats.Errors, 1)
			}
		} else {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			atomic.AddInt64(&stats.BytesFreed, sz)
		}
	}
}

// ===================================================================
// Single-pass Run (backward compatible)
// ===================================================================

// Run performs the sweep in a single pass (collect + delete without locking
// or delta).  This preserves the original API for simple usage and testing.
func Run(cfg Config, stats *Stats) (*Stats, error) {
	if stats == nil {
		stats = &Stats{}
	}

	candidates, err := CollectCandidates(cfg, stats)
	if err != nil {
		return stats, err
	}

	DeleteCandidates(candidates, cfg, stats)
	return stats, nil
}

// ===================================================================
// Utility: write chunk files from string hashes (for tests)
// ===================================================================

// WriteChunkFromHashes is a test helper that writes a sorted binary
// chunk file for the given prefix from a list of hex-encoded hash strings.
// Each hash string is "hex" or "hexSUFFIX" where SUFFIX is a single byte.
func WriteChunkFromHashes(chunkDir string, hashes []string) error {
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return err
	}

	// Group by prefix byte.
	byPrefix := make(map[byte][]hashsort.Entry)
	for _, h := range hashes {
		hexPart := h
		var suffix byte
		// Check if last char is a suffix (A-Z).
		if len(h) > 0 {
			last := h[len(h)-1]
			if last >= 'A' && last <= 'Z' {
				hexPart = h[:len(h)-1]
				suffix = last
			}
		}

		if len(hexPart) < 2 {
			continue
		}

		b, err := hex.DecodeString(hexPart[:2])
		if err != nil || len(b) < 1 {
			continue
		}

		e := hashsort.EntryFromHexSuffix(hexPart, suffix)
		byPrefix[b[0]] = append(byPrefix[b[0]], e)
	}

	for prefix, entries := range byPrefix {
		// Sort entries.
		sort.Slice(entries, func(i, j int) bool {
			return hashsort.CompareEntries(entries[i], entries[j]) < 0
		})

		chunkPath := hashsort.ChunkFileForPrefix(chunkDir, prefix)
		f, err := os.Create(chunkPath)
		if err != nil {
			return err
		}
		w := bufio.NewWriterSize(f, 64*1024)
		for _, e := range entries {
			if _, err := w.Write(e[:]); err != nil {
				f.Close()
				return err
			}
		}
		if err := w.Flush(); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}

	return nil
}
