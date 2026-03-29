package packfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// PrefixState manages the set of pack indices for a single byte prefix
// (one packs/XX/ directory).  It supports concurrent read access with
// MRU-ordered lookups and periodic refresh from disk.
type PrefixState struct {
	mu      sync.RWMutex
	dir     string       // packs/XX directory
	indices []*PackIndex // active indices, MRU order
}

// NewPrefixState creates a PrefixState for the given packs/XX directory.
// It does NOT load indices automatically — call Refresh() first.
func NewPrefixState(dir string) *PrefixState {
	return &PrefixState{dir: dir}
}

// Refresh rescans the packs/XX/ directory and loads any new .idx files.
// Existing indices whose files are still present are retained (and keep
// their position); indices whose files have been deleted are dropped.
func (ps *PrefixState) Refresh() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	entries, err := os.ReadDir(ps.dir)
	if os.IsNotExist(err) {
		ps.indices = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading pack dir %s: %w", ps.dir, err)
	}

	// Collect .idx filenames on disk.
	onDisk := make(map[string]bool)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".idx") {
			onDisk[e.Name()] = true
		}
	}

	// Build a set of already-loaded index filenames.
	loaded := make(map[string]int) // filename → position
	for i, idx := range ps.indices {
		base := filepath.Base(idx.PackPath)
		// Pack file base without .pack → matches idx name without .idx
		idxName := strings.TrimSuffix(base, ".pack") + ".idx"
		loaded[idxName] = i
	}

	// Remove indices no longer on disk.
	var kept []*PackIndex
	for _, idx := range ps.indices {
		base := filepath.Base(idx.PackPath)
		idxName := strings.TrimSuffix(base, ".pack") + ".idx"
		if onDisk[idxName] {
			kept = append(kept, idx)
		} else {
			idx.Close() // release mmap
		}
	}

	// Build set of kept names.
	keptNames := make(map[string]bool)
	for _, idx := range kept {
		base := filepath.Base(idx.PackPath)
		idxName := strings.TrimSuffix(base, ".pack") + ".idx"
		keptNames[idxName] = true
	}

	// Load new indices.
	for name := range onDisk {
		if keptNames[name] {
			continue // already loaded
		}
		baseName := strings.TrimSuffix(name, ".idx")
		packPath := filepath.Join(ps.dir, baseName+".pack")
		idxPath := filepath.Join(ps.dir, name)

		// Determine pack file size.
		fi, err := os.Stat(packPath)
		if err != nil {
			continue // .idx without .pack — skip
		}

		idx, err := ReadIndexFile(idxPath, packPath, fi.Size())
		if err != nil {
			continue // corrupt index — skip
		}
		// Prepend new indices (MRU: newest first).
		kept = append([]*PackIndex{idx}, kept...)
	}

	ps.indices = kept
	return nil
}

// Contains returns true if the given hash exists in any pack for this
// prefix.  The matching index is moved to the MRU position.
func (ps *PrefixState) Contains(hash [20]byte) bool {
	_, _, _, found := ps.Lookup(hash)
	return found
}

// Lookup searches all loaded indices (MRU-first) for the given hash.
// On a hit the matching index is promoted to the front.
// Returns the pack file path, offset, length, and whether the hash was found.
func (ps *PrefixState) Lookup(hash [20]byte) (packPath string, offset, length int64, found bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for i, idx := range ps.indices {
		off, ln, ok := idx.Lookup(hash)
		if ok {
			// Promote to MRU position.
			if i > 0 {
				copy(ps.indices[1:i+1], ps.indices[0:i])
				ps.indices[0] = idx
			}
			return idx.PackPath, off, ln, true
		}
	}
	return "", 0, 0, false
}

// IndexCount returns the number of loaded indices.
func (ps *PrefixState) IndexCount() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.indices)
}

// ObjectCount returns the total number of objects across all indices.
func (ps *PrefixState) ObjectCount() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var total int
	for _, idx := range ps.indices {
		total += idx.ObjectCount()
	}
	return total
}

// PackBytes returns the total bytes of pack files for this prefix.
func (ps *PrefixState) PackBytes() int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var total int64
	for _, idx := range ps.indices {
		total += idx.PackSize
	}
	return total
}

// Indices returns a snapshot of the currently loaded indices.
// The caller must not modify the returned slice.
func (ps *PrefixState) Indices() []*PackIndex {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	out := make([]*PackIndex, len(ps.indices))
	copy(out, ps.indices)
	return out
}

// Close releases all pack indices held by this PrefixState.
func (ps *PrefixState) Close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, idx := range ps.indices {
		idx.Close()
	}
	ps.indices = nil
}

// --------------------------------------------------------------------------
// Store: 256-way prefix lookup
// --------------------------------------------------------------------------

// Store manages pack indices for all 256 byte prefixes.
type Store struct {
	packsDir string
	prefixes [256]*PrefixState
}

// NewStore creates a Store rooted at the given packs/ directory.
func NewStore(packsDir string) *Store {
	s := &Store{packsDir: packsDir}
	for i := 0; i < 256; i++ {
		dir := filepath.Join(packsDir, fmt.Sprintf("%02x", i))
		s.prefixes[i] = NewPrefixState(dir)
	}
	return s
}

// RefreshAll rescans all 256 prefix directories.
func (s *Store) RefreshAll() error {
	for i := 0; i < 256; i++ {
		if err := s.prefixes[i].Refresh(); err != nil {
			return err
		}
	}
	return nil
}

// RefreshPrefix rescans a single prefix directory.
func (s *Store) RefreshPrefix(prefix byte) error {
	return s.prefixes[prefix].Refresh()
}

// Contains returns true if the hash exists in any pack.
func (s *Store) Contains(hash [20]byte) bool {
	return s.prefixes[hash[0]].Contains(hash)
}

// Lookup searches the packs for the given hash.
func (s *Store) Lookup(hash [20]byte) (packPath string, offset, length int64, found bool) {
	return s.prefixes[hash[0]].Lookup(hash)
}

// Prefix returns the PrefixState for the given byte prefix.
func (s *Store) Prefix(prefix byte) *PrefixState {
	return s.prefixes[prefix]
}

// TotalObjects returns the total number of packed objects.
func (s *Store) TotalObjects() int {
	total := 0
	for i := 0; i < 256; i++ {
		total += s.prefixes[i].ObjectCount()
	}
	return total
}

// TotalPackBytes returns the total size (bytes) of all pack files.
func (s *Store) TotalPackBytes() int64 {
	var total int64
	for i := 0; i < 256; i++ {
		total += s.prefixes[i].PackBytes()
	}
	return total
}

// PackFileCount returns the total number of pack files (indices).
func (s *Store) PackFileCount() int {
	total := 0
	for i := 0; i < 256; i++ {
		total += s.prefixes[i].IndexCount()
	}
	return total
}

// MaxPrefixIndices returns the maximum IndexCount across all 256 prefixes.
func (s *Store) MaxPrefixIndices() int {
	max := 0
	for i := 0; i < 256; i++ {
		if n := s.prefixes[i].IndexCount(); n > max {
			max = n
		}
	}
	return max
}

// Close releases all pack indices across all prefixes.
func (s *Store) Close() {
	for i := 0; i < 256; i++ {
		s.prefixes[i].Close()
	}
}
