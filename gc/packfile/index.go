// Package packfile implements a git-v2-inspired pack index format for
// storing CVMFS content-addressable objects in concatenated pack files.
//
// Each pack file is a flat concatenation of raw object blobs (no delta
// encoding, no per-object headers).  The companion index file uses a
// binary format inspired by git pack index v2, adapted so that the
// fan-out table is keyed on the second byte of the SHA-1 hash (byte
// index 1) because all objects in a given packs/XX/ directory share
// the same first byte.
//
// An explicit 4-byte length table (between the hash names and offsets)
// records each object's size, avoiding the need to derive lengths from
// the offset order at load time.
//
// Index v2 on-disk layout:
//
//	4 bytes   magic: 0xff 0x74 0x4f 0x63
//	4 bytes   version: 2  (network byte order)
//	256×4     fan-out table  (keyed on hash byte 1)
//	N×20      sorted SHA-1 names
//	N×4       4-byte lengths (object sizes, network order)
//	N×4       4-byte offsets (MSB → 8-byte table)
//	M×8       8-byte offsets (if any offset ≥ 2³¹)
//	20 bytes  pack checksum (SHA-1 of pack file)
//	20 bytes  index checksum (SHA-1 of all preceding bytes)
//
// The PackIndex returned by ReadIndex / ReadIndexFile is backed by the
// raw index bytes (mmap'd when loaded from a file, heap-allocated when
// loaded from an io.Reader).  Call Close() to release the mapping.
package packfile

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"syscall"
)

// index magic and version
var idxMagic = [4]byte{0xff, 0x74, 0x4f, 0x63}

const idxVersion = 2

// headerSize is the fixed portion of the index: magic + version + fanout.
const headerSize = 8 + 256*4 // 1032 bytes

// IndexEntry describes one object inside a pack file.
// It is returned by query methods and used by WriteIndex; it is NOT
// stored in bulk inside a PackIndex.
type IndexEntry struct {
	Hash   [20]byte // SHA-1 object name
	Offset int64    // byte offset within the .pack file
	Length int64    // object size in bytes
}

// PackIndex is an mmap-backed (or byte-backed) pack index.
//
// All hash, length, and offset data is read directly from the
// underlying byte slice — no per-entry Go structs are allocated.
// Call Close() to release the mmap (or allow GC of heap bytes).
type PackIndex struct {
	// PackPath is the filesystem path to the .pack file.
	PackPath string
	// PackSize is the total byte size of the .pack file.
	PackSize int64

	data    []byte // raw index file content (mmap'd or heap)
	mmapped bool   // true → must syscall.Munmap on Close

	n       int // object count (fanout[255])
	hashOff int // byte offset of hash table in data
	lenOff  int // byte offset of 4-byte length table
	off4Off int // byte offset of 4-byte offset table
	off8Off int // byte offset of 8-byte offset table
	nLarge  int // number of 8-byte offsets

	packChecksum [20]byte
}

// Close releases the underlying index data.  For mmap-backed indices
// this unmaps the memory; for heap-backed indices it drops the reference.
// It is safe to call Close multiple times.
func (idx *PackIndex) Close() error {
	if idx.data == nil {
		return nil
	}
	var err error
	if idx.mmapped {
		err = syscall.Munmap(idx.data)
	}
	idx.data = nil
	return err
}

// ObjectCount returns the number of objects in this index.
func (idx *PackIndex) ObjectCount() int { return idx.n }

// Contains returns true if the given hash is present in the index.
func (idx *PackIndex) Contains(hash [20]byte) bool {
	_, found := idx.find(hash)
	return found
}

// Lookup returns the offset and length of the object with the given
// hash.  Returns found=false if the hash is not in this index.
func (idx *PackIndex) Lookup(hash [20]byte) (offset, length int64, found bool) {
	pos, ok := idx.find(hash)
	if !ok {
		return 0, 0, false
	}
	return idx.offsetAt(pos), idx.lengthAt(pos), true
}

// find uses the fan-out table and binary search to locate a hash.
// Returns the position (0-based) and true, or 0 and false.
func (idx *PackIndex) find(hash [20]byte) (int, bool) {
	if idx.n == 0 {
		return 0, false
	}
	b := hash[1] // fan-out keyed on 2nd byte
	var lo int
	if b > 0 {
		lo = int(idx.fanoutAt(b - 1))
	}
	hi := int(idx.fanoutAt(b))
	pos := sort.Search(hi-lo, func(i int) bool {
		return bytes.Compare(idx.hashSlice(lo+i), hash[:]) >= 0
	})
	pos += lo
	if pos < hi && bytes.Equal(idx.hashSlice(pos), hash[:]) {
		return pos, true
	}
	return 0, false
}

// Iterate calls fn for each entry in hash-sorted order.  If fn returns
// false the iteration stops early.
func (idx *PackIndex) Iterate(fn func(entry IndexEntry) bool) {
	for i := 0; i < idx.n; i++ {
		var e IndexEntry
		copy(e.Hash[:], idx.hashSlice(i))
		e.Offset = idx.offsetAt(i)
		e.Length = idx.lengthAt(i)
		if !fn(e) {
			return
		}
	}
}

// --------------------------------------------------------------------------
// Low-level accessors — read directly from the backing byte slice
// --------------------------------------------------------------------------

// fanoutAt returns the cumulative count for the given second-byte value.
func (idx *PackIndex) fanoutAt(b byte) uint32 {
	off := 8 + int(b)*4
	return binary.BigEndian.Uint32(idx.data[off:])
}

// hashSlice returns a slice into the backing data for hash at position i.
func (idx *PackIndex) hashSlice(i int) []byte {
	start := idx.hashOff + i*20
	return idx.data[start : start+20]
}

// lengthAt returns the object length at position i.
func (idx *PackIndex) lengthAt(i int) int64 {
	start := idx.lenOff + i*4
	return int64(binary.BigEndian.Uint32(idx.data[start:]))
}

// offsetAt resolves the pack-file offset at position i, handling the
// MSB flag that redirects to the 8-byte offset table.
func (idx *PackIndex) offsetAt(i int) int64 {
	start := idx.off4Off + i*4
	v := binary.BigEndian.Uint32(idx.data[start:])
	const msbMask = uint32(1) << 31
	if v&msbMask != 0 {
		li := int(v & ^msbMask)
		s8 := idx.off8Off + li*8
		return int64(binary.BigEndian.Uint64(idx.data[s8:]))
	}
	return int64(v)
}

// --------------------------------------------------------------------------
// Writing
// --------------------------------------------------------------------------

// WriteIndex writes a pack index to w.
// entries must be sorted by Hash and each entry must have Length set.
func WriteIndex(w io.Writer, entries []IndexEntry, packChecksum [20]byte) error {
	h := sha1.New() // running checksum of all bytes written
	mw := io.MultiWriter(w, h)

	// Magic + version
	if err := binary.Write(mw, binary.BigEndian, idxMagic); err != nil {
		return err
	}
	if err := binary.Write(mw, binary.BigEndian, uint32(idxVersion)); err != nil {
		return err
	}

	// Fan-out table (keyed on byte 1 of the hash)
	var fanout [256]uint32
	for _, e := range entries {
		b := e.Hash[1]
		fanout[b]++
	}
	for i := 1; i < 256; i++ {
		fanout[i] += fanout[i-1]
	}
	if err := binary.Write(mw, binary.BigEndian, fanout); err != nil {
		return err
	}

	// Sorted hash names
	for i := range entries {
		if _, err := mw.Write(entries[i].Hash[:]); err != nil {
			return err
		}
	}

	// 4-byte lengths
	for i := range entries {
		if err := binary.Write(mw, binary.BigEndian, uint32(entries[i].Length)); err != nil {
			return err
		}
	}

	// Determine if we need 8-byte offset table
	var largeOffsets []int64
	const msbMask = uint32(1) << 31
	for i := range entries {
		if entries[i].Offset >= (1 << 31) {
			largeOffsets = append(largeOffsets, entries[i].Offset)
		}
	}

	// 4-byte offsets
	largeIdx := 0
	for i := range entries {
		if entries[i].Offset >= (1 << 31) {
			v := msbMask | uint32(largeIdx)
			if err := binary.Write(mw, binary.BigEndian, v); err != nil {
				return err
			}
			largeIdx++
		} else {
			if err := binary.Write(mw, binary.BigEndian, uint32(entries[i].Offset)); err != nil {
				return err
			}
		}
	}

	// 8-byte offsets (if any)
	for _, off := range largeOffsets {
		if err := binary.Write(mw, binary.BigEndian, off); err != nil {
			return err
		}
	}

	// Pack checksum
	if _, err := mw.Write(packChecksum[:]); err != nil {
		return err
	}

	// Index checksum (SHA-1 of everything above)
	sum := h.Sum(nil)
	if _, err := w.Write(sum[:20]); err != nil {
		return err
	}

	return nil
}

// WriteIndexFile atomically writes an index to the given path.
// It writes to a temp file first then renames.
func WriteIndexFile(path string, entries []IndexEntry, packChecksum [20]byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := WriteIndex(f, entries, packChecksum); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// --------------------------------------------------------------------------
// Reading
// --------------------------------------------------------------------------

// initIndex validates the raw data in idx.data and computes the section
// offsets.  Called by both ReadIndex (heap) and ReadIndexFile (mmap).
func initIndex(idx *PackIndex) error {
	data := idx.data

	if len(data) < headerSize+40 {
		return fmt.Errorf("index too small: %d bytes", len(data))
	}

	// Magic
	if !bytes.Equal(data[:4], idxMagic[:]) {
		return fmt.Errorf("bad index magic: %x", data[:4])
	}

	// Version
	ver := binary.BigEndian.Uint32(data[4:8])
	if ver != idxVersion {
		return fmt.Errorf("unsupported index version %d", ver)
	}

	// Object count from fanout[255]
	idx.n = int(binary.BigEndian.Uint32(data[8+255*4:]))
	if idx.n < 0 || idx.n > 100_000_000 {
		return fmt.Errorf("unreasonable object count: %d", idx.n)
	}

	n := idx.n
	idx.hashOff = headerSize             // after magic + version + fanout
	idx.lenOff = idx.hashOff + n*20      // after hash names
	idx.off4Off = idx.lenOff + n*4       // after length table
	off4End := idx.off4Off + n*4         // end of 4-byte offset table

	// Validate we have enough data for the fixed sections.
	if len(data) < off4End+40 {
		return fmt.Errorf("index truncated: need at least %d bytes, got %d", off4End+40, len(data))
	}

	// Count large offsets by scanning the 4-byte offset table.
	const msbMask = uint32(1) << 31
	idx.nLarge = 0
	for i := 0; i < n; i++ {
		v := binary.BigEndian.Uint32(data[idx.off4Off+i*4:])
		if v&msbMask != 0 {
			idx.nLarge++
		}
	}

	idx.off8Off = off4End
	expectedSize := off4End + idx.nLarge*8 + 40
	if len(data) < expectedSize {
		return fmt.Errorf("index truncated: need %d bytes (with %d large offsets), got %d",
			expectedSize, idx.nLarge, len(data))
	}

	// Pack checksum (20 bytes after 8-byte offsets)
	csOff := idx.off8Off + idx.nLarge*8
	copy(idx.packChecksum[:], data[csOff:csOff+20])

	// Verify index checksum (last 20 bytes = SHA-1 of everything before)
	idxCsOff := csOff + 20
	var stored [20]byte
	copy(stored[:], data[idxCsOff:idxCsOff+20])

	computed := sha1.Sum(data[:idxCsOff])
	if stored != computed {
		return fmt.Errorf("index checksum mismatch: stored=%x computed=%x", stored, computed)
	}

	return nil
}

// ReadIndex reads a pack index from r into a heap-backed PackIndex.
// This is useful for tests and small indices.  packPath is stored for
// later object retrieval; packSize is informational.
func ReadIndex(r io.Reader, packPath string, packSize int64) (*PackIndex, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading index data: %w", err)
	}
	idx := &PackIndex{PackPath: packPath, PackSize: packSize, data: data}
	if err := initIndex(idx); err != nil {
		return nil, err
	}
	return idx, nil
}

// ReadIndexFile memory-maps a pack index file and returns an mmap-backed
// PackIndex.  The caller must call Close() when the index is no longer
// needed to release the mmap.
func ReadIndexFile(idxPath, packPath string, packSize int64) (*PackIndex, error) {
	f, err := os.Open(idxPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat index: %w", err)
	}
	size := int(fi.Size())
	if size == 0 {
		return nil, fmt.Errorf("empty index file: %s", idxPath)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap index %s: %w", idxPath, err)
	}

	idx := &PackIndex{
		PackPath: packPath,
		PackSize: packSize,
		data:     data,
		mmapped:  true,
	}
	if err := initIndex(idx); err != nil {
		syscall.Munmap(data)
		return nil, err
	}
	return idx, nil
}
