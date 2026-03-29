// Package hashsort implements external sorting for content hashes using
// a byte-prefix partitioning scheme.
//
// The pipeline works as follows:
//
//  1. Dispatch: Incoming hashes (as catalog.Hash values) are routed to one
//     of 256 goroutines based on the first byte of the binary hash.  Each
//     goroutine maintains a small min-heap (~1 MB) of raw bytes (binary
//     hash + suffix byte).  When the heap is full, the minimum entry is
//     evicted and appended to the chunk file for that prefix.  When the
//     input is exhausted, remaining heap entries are flushed in sorted
//     order.  This produces one partially-sorted chunk file per byte
//     prefix: chunk-00 through chunk-ff.
//
//  2. Chunk sort: Each chunk file is read into memory, sorted, and
//     deduplicated.  Sorting is parallelised but limited so that at most
//     ~100 MB of chunk data is in memory at once.
//
// The result is 256 sorted, deduplicated chunk files — one per byte
// prefix — that can be consumed directly by the sweep phase (one
// directory per prefix).
package hashsort

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Entry: binary representation of hash + suffix
// ---------------------------------------------------------------------------

// EntrySize is the number of bytes per heap/chunk entry.
// 20 bytes for SHA-1 hash + 1 byte for the suffix character (0 if none).
const EntrySize = 21

// Entry is a fixed-size binary record: 20-byte hash + 1-byte suffix.
type Entry [EntrySize]byte

// EntryFromHexSuffix converts a hex hash string and suffix byte into an Entry.
func EntryFromHexSuffix(hexHash string, suffix byte) Entry {
	var e Entry
	hex.Decode(e[:20], []byte(hexHash))
	e[20] = suffix
	return e
}

// Hex returns the hex-encoded hash (first 20 bytes).
func (e Entry) Hex() string {
	return hex.EncodeToString(e[:20])
}

// Suffix returns the suffix byte.
func (e Entry) Suffix() byte {
	return e[20]
}

// String returns the hash with suffix, matching the old string format.
func (e Entry) String() string {
	h := e.Hex()
	if e[20] == 0 {
		return h
	}
	return h + string(rune(e[20]))
}

// Prefix returns the first byte of the hash (used for routing).
func (e Entry) Prefix() byte {
	return e[0]
}

// CompareEntries returns -1, 0, or +1 comparing two entries lexicographically.
func CompareEntries(a, b Entry) int {
	for i := 0; i < EntrySize; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Min-heap of Entry values
// ---------------------------------------------------------------------------

type entryHeap []Entry

func (h entryHeap) Len() int { return len(h) }

// heapPush adds an entry to the min-heap without interface{} boxing.
func heapPush(h *entryHeap, e Entry) {
	*h = append(*h, e)
	// Sift up.
	i := len(*h) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if CompareEntries((*h)[i], (*h)[parent]) >= 0 {
			break
		}
		(*h)[i], (*h)[parent] = (*h)[parent], (*h)[i]
		i = parent
	}
}

// heapPop removes and returns the minimum entry without interface{} boxing.
func heapPop(h *entryHeap) Entry {
	old := *h
	n := len(old)
	item := old[0]
	old[0] = old[n-1]
	*h = old[:n-1]
	// Sift down.
	i := 0
	for {
		left := 2*i + 1
		if left >= len(*h) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(*h) && CompareEntries((*h)[right], (*h)[left]) < 0 {
			smallest = right
		}
		if CompareEntries((*h)[i], (*h)[smallest]) <= 0 {
			break
		}
		(*h)[i], (*h)[smallest] = (*h)[smallest], (*h)[i]
		i = smallest
	}
	return item
}

// ---------------------------------------------------------------------------
// Prefix Writer Config
// ---------------------------------------------------------------------------

// PrefixWriterConfig controls the 256-way partitioned write phase.
type PrefixWriterConfig struct {
	// HeapBytes is the approximate memory budget per prefix heap.
	// Default: 1 MB.  Entries are 21 bytes each, so 1 MB ≈ 48k entries.
	HeapBytes int64
}

// DefaultPrefixWriterConfig returns a config with 1 MB heaps.
func DefaultPrefixWriterConfig() PrefixWriterConfig {
	return PrefixWriterConfig{
		HeapBytes: 1 * 1024 * 1024,
	}
}

// ---------------------------------------------------------------------------
// Prefix Writer: 256-way partitioned write with per-prefix min-heaps
// ---------------------------------------------------------------------------

// PrefixWrite routes incoming entries to 256 goroutines (one per byte
// prefix), each maintaining a min-heap.  When the heap is full, the
// minimum is evicted to the chunk file.  On completion, remaining entries
// are flushed sorted.
//
// Returns the list of chunk files that were actually written (prefixes
// with no data produce no file).
//
// hashesConsumed is atomically incremented for each entry processed.
// evictDeduped is atomically incremented for each duplicate skipped
// during eviction (consecutive duplicate detection in the min-heap
// output stream).
// chunksProduced is atomically incremented for each chunk file written.
func PrefixWrite(cfg PrefixWriterConfig, input <-chan Entry, outputDir string,
	hashesConsumed *int64, evictDeduped *int64, chunksProduced *int64) ([]string, error) {

	if cfg.HeapBytes <= 0 {
		cfg.HeapBytes = 1 * 1024 * 1024
	}
	maxEntries := int(cfg.HeapBytes / EntrySize)
	if maxEntries < 256 {
		maxEntries = 256
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating chunk dir: %w", err)
	}

	// 256 channels, one per byte prefix.
	chans := make([]chan Entry, 256)
	for i := range chans {
		chans[i] = make(chan Entry, 1024)
	}

	// Each prefix goroutine writes its unsorted chunk file.
	type prefixResult struct {
		path string // empty if no entries
		err  error
	}

	var wg sync.WaitGroup
	results := make([]prefixResult, 256)

	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			chunkPath := filepath.Join(outputDir, fmt.Sprintf("chunk-%02x", idx))

			h := &entryHeap{}

			// Stream evicted entries directly to disk to avoid
			// buffering them in memory.  The file and writer are
			// created lazily on the first eviction.
			//
			// lastWritten tracks the most recent entry written to
			// disk so we can skip consecutive duplicates.  Because
			// the min-heap evicts in non-decreasing order, any
			// duplicates are guaranteed to be adjacent.
			var f *os.File
			var w *bufio.Writer
			var writeErr error
			var lastWritten Entry
			wrote := false
			hasLast := false

			openWriter := func() error {
				var err error
				f, err = os.Create(chunkPath)
				if err != nil {
					return err
				}
				w = bufio.NewWriterSize(f, 256*1024)
				return nil
			}

			for entry := range chans[idx] {
				heapPush(h, entry)
				if h.Len() > maxEntries {
					evicted := heapPop(h)
					if hasLast && evicted == lastWritten {
						if evictDeduped != nil {
							atomic.AddInt64(evictDeduped, 1)
						}
						continue // duplicate of last eviction
					}
					lastWritten = evicted
					hasLast = true
					if writeErr != nil {
						continue // drain channel on prior error
					}
					if w == nil {
						if writeErr = openWriter(); writeErr != nil {
							continue
						}
					}
					if _, writeErr = w.Write(evicted[:]); writeErr != nil {
						continue
					}
					wrote = true
				}
			}

			// Flush remaining heap entries in sorted order,
			// continuing to skip consecutive duplicates.
			remaining := make([]Entry, 0, h.Len())
			for h.Len() > 0 {
				e := heapPop(h)
				if hasLast && e == lastWritten {
					if evictDeduped != nil {
						atomic.AddInt64(evictDeduped, 1)
					}
					continue
				}
				lastWritten = e
				hasLast = true
				remaining = append(remaining, e)
			}

			if len(remaining) == 0 && !wrote {
				// No entries at all for this prefix.
				return
			}

			if writeErr != nil {
				if f != nil {
					f.Close()
				}
				results[idx] = prefixResult{err: writeErr}
				return
			}

			// If we never evicted anything, open the file now for
			// the heap remainder.
			if w == nil {
				if err := openWriter(); err != nil {
					results[idx] = prefixResult{err: err}
					return
				}
			}

			for i := range remaining {
				if _, err := w.Write(remaining[i][:]); err != nil {
					f.Close()
					results[idx] = prefixResult{err: err}
					return
				}
			}

			if err := w.Flush(); err != nil {
				f.Close()
				results[idx] = prefixResult{err: err}
				return
			}
			f.Close()

			results[idx] = prefixResult{path: chunkPath}
			if chunksProduced != nil {
				atomic.AddInt64(chunksProduced, 1)
			}
		}(i)
	}

	// Dispatch loop: route each entry to the appropriate prefix channel.
	for entry := range input {
		if hashesConsumed != nil {
			atomic.AddInt64(hashesConsumed, 1)
		}
		chans[entry.Prefix()] <- entry
	}

	// Close all prefix channels so goroutines finish.
	for i := range chans {
		close(chans[i])
	}

	wg.Wait()

	// Collect results.
	var files []string
	for i := 0; i < 256; i++ {
		if results[i].err != nil {
			return nil, fmt.Errorf("prefix %02x: %w", i, results[i].err)
		}
		if results[i].path != "" {
			files = append(files, results[i].path)
		}
	}

	return files, nil
}

// ---------------------------------------------------------------------------
// Chunk sorting (parallel, memory-bounded)
// ---------------------------------------------------------------------------

// ChunkSortConfig controls the parallel chunk-sort phase.
type ChunkSortConfig struct {
	// MaxMemoryBytes is the maximum total memory to use across all
	// concurrent sort operations.  Default: 100 MB.
	MaxMemoryBytes int64
}

// DefaultChunkSortConfig returns a config with 100 MB memory budget.
func DefaultChunkSortConfig() ChunkSortConfig {
	return ChunkSortConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
	}
}

// SortChunks sorts and deduplicates each chunk file in place.
// It parallelises sorting but limits concurrency so that at most
// cfg.MaxMemoryBytes of chunk data is in memory at once.
//
// chunksSorted is atomically incremented for each chunk sorted.
// sortDeduped is atomically incremented by the number of duplicates
// removed during the sort phase (entries that survived the heap but
// appeared in the eviction stream more than once).
func SortChunks(chunkFiles []string, cfg ChunkSortConfig, chunksSorted *int64, sortDeduped *int64) error {
	if cfg.MaxMemoryBytes <= 0 {
		cfg.MaxMemoryBytes = 100 * 1024 * 1024
	}

	if len(chunkFiles) == 0 {
		return nil
	}

	// Gather file sizes to plan parallelism.
	type chunkInfo struct {
		path string
		size int64
	}
	chunks := make([]chunkInfo, 0, len(chunkFiles))
	for _, p := range chunkFiles {
		fi, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("stat %s: %w", p, err)
		}
		chunks = append(chunks, chunkInfo{path: p, size: fi.Size()})
	}

	// Use a memory-bounded approach: a mutex + condition variable to
	// track available memory.
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	var memUsed int64

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for _, ci := range chunks {
		wg.Add(1)
		go func(c chunkInfo) {
			defer wg.Done()

			// Acquire memory budget.
			mu.Lock()
			for memUsed+c.size > cfg.MaxMemoryBytes && memUsed > 0 {
				cond.Wait()
			}
			memUsed += c.size
			mu.Unlock()

			dupsRemoved, err := sortChunkFile(c.path)
			if sortDeduped != nil && dupsRemoved > 0 {
				atomic.AddInt64(sortDeduped, dupsRemoved)
			}

			// Release memory budget.
			mu.Lock()
			memUsed -= c.size
			mu.Unlock()
			cond.Broadcast()

			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("sorting %s: %w", c.path, err)
				})
				return
			}
			if chunksSorted != nil {
				atomic.AddInt64(chunksSorted, 1)
			}
		}(ci)
	}

	wg.Wait()
	return firstErr
}

// sortChunkFile reads a binary chunk file, sorts its entries, deduplicates
// them, and rewrites the file in place.  Returns the number of duplicate
// entries removed during sort-phase dedup.
func sortChunkFile(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	n := len(data) / EntrySize
	if n == 0 {
		return 0, nil
	}

	// Build a slice of entries.
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		copy(entries[i][:], data[i*EntrySize:(i+1)*EntrySize])
	}

	// Sort.
	sort.Slice(entries, func(i, j int) bool {
		return CompareEntries(entries[i], entries[j]) < 0
	})

	// Deduplicate.
	w := 0
	for i := 0; i < len(entries); i++ {
		if i == 0 || entries[i] != entries[w-1] {
			entries[w] = entries[i]
			w++
		}
	}
	dupsRemoved := int64(len(entries) - w)
	entries = entries[:w]

	// Rewrite file.
	f, err := os.Create(path)
	if err != nil {
		return dupsRemoved, err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	for i := range entries {
		if _, err := bw.Write(entries[i][:]); err != nil {
			f.Close()
			return dupsRemoved, err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return dupsRemoved, err
	}
	return dupsRemoved, f.Close()
}

// ---------------------------------------------------------------------------
// Chunk reader: read sorted entries from a prefix chunk file
// ---------------------------------------------------------------------------

// ChunkReader reads sorted entries from a single binary chunk file.
type ChunkReader struct {
	f   *os.File
	buf []byte
	pos int
	end int
	eof bool
	cur Entry
	ok  bool
}

// NewChunkReader opens a binary chunk file for sequential reading.
func NewChunkReader(path string) (*ChunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &ChunkReader{
		f:   f,
		buf: make([]byte, 256*1024),
	}, nil
}

// Next advances to the next entry. Returns false when exhausted.
func (r *ChunkReader) Next() bool {
	for {
		if r.pos+EntrySize <= r.end {
			copy(r.cur[:], r.buf[r.pos:r.pos+EntrySize])
			r.pos += EntrySize
			r.ok = true
			return true
		}
		if r.eof {
			r.ok = false
			return false
		}
		// Refill buffer, keeping any partial entry.
		remaining := r.end - r.pos
		if remaining > 0 {
			copy(r.buf[:remaining], r.buf[r.pos:r.end])
		}
		r.pos = 0
		r.end = remaining

		n, err := r.f.Read(r.buf[r.end:])
		r.end += n
		if err == io.EOF || n == 0 {
			r.eof = true
		}
		if err != nil && err != io.EOF {
			r.eof = true
			r.ok = false
			return false
		}
	}
}

// Value returns the current entry. Only valid after Next() returns true.
func (r *ChunkReader) Value() Entry {
	return r.cur
}

// ValueString returns the current entry as a string (hex + suffix).
// Convenience method for the sweep phase.
func (r *ChunkReader) ValueString() string {
	return r.cur.String()
}

// Close releases the file handle.
func (r *ChunkReader) Close() {
	if r.f != nil {
		r.f.Close()
	}
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

// ChunkFileForPrefix returns the expected chunk file path for a given
// byte prefix.
func ChunkFileForPrefix(outputDir string, prefix byte) string {
	return filepath.Join(outputDir, fmt.Sprintf("chunk-%02x", prefix))
}

// AllChunkFiles returns paths for all 256 possible chunk files, but only
// those that actually exist on disk.
func AllChunkFiles(outputDir string) []string {
	var files []string
	for i := 0; i < 256; i++ {
		p := ChunkFileForPrefix(outputDir, byte(i))
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
	}
	return files
}

// PrefixFromChunkFile extracts the byte prefix from a chunk filename
// like "chunk-ab".  Returns the prefix byte and true, or 0 and false
// if the name doesn't match.
func PrefixFromChunkFile(path string) (byte, bool) {
	base := filepath.Base(path)
	if len(base) != 8 || base[:6] != "chunk-" {
		return 0, false
	}
	b, err := hex.DecodeString(base[6:8])
	if err != nil || len(b) != 1 {
		return 0, false
	}
	return b[0], true
}

// CountEntries counts the number of entries in a binary chunk file.
func CountEntries(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size() / EntrySize, nil
}

// CountEntriesMulti counts the total entries across multiple chunk files.
func CountEntriesMulti(paths []string) (int64, error) {
	var total int64
	for _, p := range paths {
		n, err := CountEntries(p)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// ReadAllEntries reads all entries from a binary chunk file into a slice.
func ReadAllEntries(path string) ([]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	n := len(data) / EntrySize
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		copy(entries[i][:], data[i*EntrySize:(i+1)*EntrySize])
	}
	return entries, nil
}
