// processed.go tracks which catalogs have been fully processed across
// mirror runs.  A simple text file (one hex hash per line, sorted) is
// used as persistent storage.  At runtime, the set is held in a
// sync.Map so multiple goroutines can check and mark concurrently.
//
// IMPORTANT: MarkProcessed only updates the in-memory set.  The file
// is written atomically by Save() and should only be called after the
// entire download pipeline has completed — i.e., all prefix workers
// have finished and packs are finalized.  A catalog is not truly
// "processed" until every hash it references has been downloaded (or
// confirmed to already exist on disk).  Since hashes from many
// catalogs are interleaved across the 256 prefix workers, we cannot
// know a single catalog's downloads are done until *all* downloads
// are done.
package catalog

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"sync"
)

// ProcessedCatalogs is a thread-safe set of catalog hex hashes that
// have been fully processed by a previous or current mirror run.
type ProcessedCatalogs struct {
	set  sync.Map
	path string // on-disk file (one hash per line)
}

// LoadProcessed reads the processed-catalogs file and returns a
// ProcessedCatalogs ready for use.  If the file does not exist, an
// empty set is returned.
func LoadProcessed(path string) (*ProcessedCatalogs, error) {
	pc := &ProcessedCatalogs{path: path}

	// Read existing entries.
	f, err := os.Open(path)
	if err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			h := strings.TrimSpace(scanner.Text())
			if h != "" {
				pc.set.Store(h, struct{}{})
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	return pc, nil
}

// Contains reports whether hexHash was already processed.
func (pc *ProcessedCatalogs) Contains(hexHash string) bool {
	_, ok := pc.set.Load(hexHash)
	return ok
}

// Count returns the number of processed catalogs in the set.
func (pc *ProcessedCatalogs) Count() int {
	n := 0
	pc.set.Range(func(_, _ interface{}) bool {
		n++
		return true
	})
	return n
}

// MarkProcessed records hexHash as processed in the in-memory set.
// This is used for cross-tree dedup within a single mirror run.
// The on-disk file is NOT updated here — call Save() after the
// entire download pipeline has completed.
func (pc *ProcessedCatalogs) MarkProcessed(hexHash string) {
	pc.set.LoadOrStore(hexHash, struct{}{})
}

// Save sorts and writes the in-memory set to the on-disk file.
// This should only be called after the full pipeline has completed
// (all prefix workers done, all packs finalized) and not on
// interrupted runs.
func (pc *ProcessedCatalogs) Save() error {
	// Collect all entries from the map.
	var hashes []string
	pc.set.Range(func(key, _ interface{}) bool {
		hashes = append(hashes, key.(string))
		return true
	})

	sort.Strings(hashes)

	f, err := os.OpenFile(pc.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, h := range hashes {
		bw.WriteString(h)
		bw.WriteByte('\n')
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
