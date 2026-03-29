package packfile

import (
	"bufio"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Size thresholds (exported so mirror / GC can reference them).
const (
	// LooseThreshold: objects ≥ this size are stored as loose files
	// in data/XX/ instead of being packed.
	LooseThreshold = 256 * 1024 // 256 KB

	// MaxPackEntries is the maximum number of objects per pack file,
	// chosen so that the companion mmap'd .idx file stays within a
	// 16 MB memory budget.
	//
	// Per-entry index cost: 20 (hash) + 4 (length) + 4 (offset) = 28 bytes.
	// Fixed overhead: magic(4) + version(4) + fanout(256×4) + checksums(40) = 1072 bytes.
	// (16*1024*1024 − 1072) / 28 = 599,148
	MaxPackEntries = 599_148
)

// pendingEntry tracks an object appended to the pack but not yet indexed.
type pendingEntry struct {
	hash   [20]byte
	offset int64
	length int64
}

// PackWriter appends raw object blobs to a pack file and tracks metadata
// needed to build the companion index.
type PackWriter struct {
	dir    string // packs/XX directory
	prefix byte   // first byte of all hashes in this pack

	f        *os.File      // pack.tmp file handle
	bw       *bufio.Writer // buffered writer wrapping f
	sha256   hash.Hash     // running SHA-256 of all bytes written
	sha1     hash.Hash     // running SHA-1 for the pack checksum in the index
	offset   int64         // current write offset
	entries  []pendingEntry
	packPath string // full path to pack.tmp
}

// NewPackWriter creates a new pack.tmp in the given directory.
// prefix is the first byte of all object hashes that will be packed here
// (e.g. 0xab for packs/ab/).
func NewPackWriter(dir string, prefix byte) (*PackWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	packPath := filepath.Join(dir, "pack.tmp")
	f, err := os.Create(packPath)
	if err != nil {
		return nil, err
	}
	return &PackWriter{
		dir:      dir,
		prefix:   prefix,
		f:        f,
		bw:       bufio.NewWriterSize(f, 256*1024),
		sha256:   sha256.New(),
		sha1:     sha1.New(),
		offset:   0,
		packPath: packPath,
	}, nil
}

// Append writes a raw object blob to the pack.  hash is the 20-byte
// SHA-1 object name.  Returns the offset at which the object was stored.
func (pw *PackWriter) Append(hash [20]byte, data []byte) (int64, error) {
	off := pw.offset

	// Write through bw → f, and also update running hashes.
	n, err := pw.bw.Write(data)
	if err != nil {
		return 0, err
	}
	pw.sha256.Write(data)
	pw.sha1.Write(data)
	pw.offset += int64(n)

	pw.entries = append(pw.entries, pendingEntry{
		hash:   hash,
		offset: off,
		length: int64(n),
	})

	return off, nil
}

// AppendReader streams object bytes from r into the pack.  hash is the
// 20-byte SHA-1 object name.  The caller does NOT need to know the size
// upfront — bytes are streamed until r returns io.EOF.  Returns the
// offset at which the object starts and the number of bytes written.
func (pw *PackWriter) AppendReader(hash [20]byte, r io.Reader) (offset int64, written int64, err error) {
	off := pw.offset

	// Use a small buffer to stream through.
	buf := make([]byte, 32*1024)
	var total int64
	for {
		nr, readErr := r.Read(buf)
		if nr > 0 {
			chunk := buf[:nr]
			nw, writeErr := pw.bw.Write(chunk)
			if writeErr != nil {
				return 0, 0, writeErr
			}
			pw.sha256.Write(chunk)
			pw.sha1.Write(chunk)
			total += int64(nw)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, 0, readErr
		}
	}

	pw.offset += total

	pw.entries = append(pw.entries, pendingEntry{
		hash:   hash,
		offset: off,
		length: total,
	})

	return off, total, nil
}

// Size returns the current pack size (bytes written so far).
func (pw *PackWriter) Size() int64 {
	return pw.offset
}

// Count returns the number of objects appended so far.
func (pw *PackWriter) Count() int {
	return len(pw.entries)
}

// Finalize flushes, fsyncs, and closes the pack file, computes the
// content hash, renames pack.tmp → <hash>.pack, and writes the
// companion .idx file atomically.
//
// Returns the content-hash hex string used as the pack/idx base name,
// or "" if the pack has no entries (in which case pack.tmp is removed).
func (pw *PackWriter) Finalize() (string, error) {
	if err := pw.bw.Flush(); err != nil {
		pw.f.Close()
		return "", err
	}
	if err := pw.f.Sync(); err != nil {
		pw.f.Close()
		return "", err
	}

	if len(pw.entries) == 0 {
		pw.f.Close()
		os.Remove(pw.packPath)
		return "", nil
	}

	if err := pw.f.Close(); err != nil {
		return "", err
	}

	// Content hash (SHA-256) → filename
	contentHash := hex.EncodeToString(pw.sha256.Sum(nil))

	// Pack checksum (SHA-1) → stored in the index trailer
	var packChecksum [20]byte
	copy(packChecksum[:], pw.sha1.Sum(nil))

	// Build sorted index entries
	idxEntries := make([]IndexEntry, len(pw.entries))
	for i, pe := range pw.entries {
		idxEntries[i] = IndexEntry{
			Hash:   pe.hash,
			Offset: pe.offset,
			Length: pe.length,
		}
	}
	sort.Slice(idxEntries, func(i, j int) bool {
		for k := 0; k < 20; k++ {
			if idxEntries[i].Hash[k] != idxEntries[j].Hash[k] {
				return idxEntries[i].Hash[k] < idxEntries[j].Hash[k]
			}
		}
		return false
	})

	// Rename pack.tmp → <hash>.pack
	finalPack := filepath.Join(pw.dir, contentHash+".pack")
	if err := os.Rename(pw.packPath, finalPack); err != nil {
		return "", fmt.Errorf("renaming pack: %w", err)
	}

	// Write index atomically
	finalIdx := filepath.Join(pw.dir, contentHash+".idx")
	if err := WriteIndexFile(finalIdx, idxEntries, packChecksum); err != nil {
		return "", fmt.Errorf("writing index: %w", err)
	}

	// Fsync the directory so the new pack and index directory entries
	// are persisted.  Without this, a crash could leave the renames
	// visible in the VFS cache but not on stable storage.
	if err := syncDir(pw.dir); err != nil {
		return "", fmt.Errorf("syncing directory: %w", err)
	}

	return contentHash, nil
}

// Abort closes and removes the pack.tmp without finalizing.
func (pw *PackWriter) Abort() {
	if pw.f != nil {
		pw.f.Close()
	}
	os.Remove(pw.packPath)
}

// --------------------------------------------------------------------------
// Pack reader
// --------------------------------------------------------------------------

// ObjectReader wraps a pack file handle and an io.SectionReader to
// stream a single object.  The caller must Close it when done.
type ObjectReader struct {
	f    *os.File
	sr   *io.SectionReader
	size int64
}

// Read implements io.Reader.
func (or *ObjectReader) Read(p []byte) (int, error) { return or.sr.Read(p) }

// Close releases the underlying file handle.
func (or *ObjectReader) Close() error { return or.f.Close() }

// Size returns the object size in bytes.
func (or *ObjectReader) Size() int64 { return or.size }

// OpenObject opens a pack file and returns an ObjectReader positioned at
// the given offset for length bytes.  The caller must Close the returned
// reader.
func OpenObject(packPath string, offset, length int64) (*ObjectReader, error) {
	f, err := os.Open(packPath)
	if err != nil {
		return nil, err
	}
	sr := io.NewSectionReader(f, offset, length)
	return &ObjectReader{f: f, sr: sr, size: length}, nil
}

// ReadObject reads a single object from a pack file at the given offset
// and length.  For serving data prefer OpenObject which streams without
// buffering the entire object.
func ReadObject(packPath string, offset, length int64) ([]byte, error) {
	or, err := OpenObject(packPath, offset, length)
	if err != nil {
		return nil, err
	}
	defer or.Close()

	buf := make([]byte, length)
	n, err := or.sr.Read(buf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading pack at offset %d: %w", offset, err)
	}
	return buf[:n], nil
}

// ReadObjectWriter copies a single object from a pack file directly into
// the given writer.  More efficient than ReadObject when the data is
// being streamed (e.g. HTTP response).
func ReadObjectWriter(packPath string, offset, length int64, w io.Writer) error {
	or, err := OpenObject(packPath, offset, length)
	if err != nil {
		return err
	}
	defer or.Close()

	_, err = io.Copy(w, or)
	return err
}

// syncDir opens a directory and fsyncs it to persist directory entry changes
// (renames, new files) to stable storage.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	return err
}

// CleanupOrphanedPacks removes any pack.tmp files (incomplete packs from
// a crash) in the given directory.
func CleanupOrphanedPacks(dir string) {
	for i := 0; i < 256; i++ {
		prefix := fmt.Sprintf("%02x", i)
		packDir := filepath.Join(dir, prefix)
		tmp := filepath.Join(packDir, "pack.tmp")
		os.Remove(tmp)
	}
}
