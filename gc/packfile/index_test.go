package packfile

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"testing"
)

func makeHash(hexStr string) [20]byte {
	var h [20]byte
	b, _ := hex.DecodeString(hexStr)
	copy(h[:], b)
	return h
}

func TestIndexRoundTrip(t *testing.T) {
	// Three objects with varying 2nd bytes to exercise the fanout.
	entries := []IndexEntry{
		{Hash: makeHash("aa00000000000000000000000000000000000001"), Offset: 0, Length: 1000},
		{Hash: makeHash("aa55000000000000000000000000000000000002"), Offset: 1000, Length: 4000},
		{Hash: makeHash("aaff000000000000000000000000000000000003"), Offset: 5000, Length: 5000},
	}

	// Sort by hash (they already are, but be explicit).
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Hash[:], entries[j].Hash[:]) < 0
	})

	packChecksum := sha1.Sum([]byte("fake-pack-data"))

	// Write
	var buf bytes.Buffer
	if err := WriteIndex(&buf, entries, packChecksum); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	// Read back
	idx, err := ReadIndex(bytes.NewReader(buf.Bytes()), "/fake/pack.pack", 10000)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	defer idx.Close()

	if idx.ObjectCount() != 3 {
		t.Fatalf("expected 3 objects, got %d", idx.ObjectCount())
	}

	// Verify entries round-tripped correctly via Lookup.
	for _, want := range entries {
		off, ln, found := idx.Lookup(want.Hash)
		if !found {
			t.Errorf("Lookup(%x) not found", want.Hash)
			continue
		}
		if off != want.Offset {
			t.Errorf("Lookup(%x) offset = %d, want %d", want.Hash, off, want.Offset)
		}
		if ln != want.Length {
			t.Errorf("Lookup(%x) length = %d, want %d", want.Hash, ln, want.Length)
		}
	}

	// Contains / Lookup
	for _, e := range entries {
		if !idx.Contains(e.Hash) {
			t.Errorf("Contains(%x) = false, want true", e.Hash)
		}
	}

	// Non-existent hash
	missing := makeHash("bb00000000000000000000000000000000000000")
	if idx.Contains(missing) {
		t.Error("Contains(missing) = true, want false")
	}
}

func TestIndexLargeOffsets(t *testing.T) {
	// An entry with an offset >= 2^31 to exercise the 8-byte offset path.
	entries := []IndexEntry{
		{Hash: makeHash("cc00000000000000000000000000000000000001"), Offset: 100, Length: 500},
		{Hash: makeHash("cc00000000000000000000000000000000000002"), Offset: 1 << 32, Length: 1024},
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Hash[:], entries[j].Hash[:]) < 0
	})

	packChecksum := sha1.Sum([]byte("large-pack"))

	var buf bytes.Buffer
	if err := WriteIndex(&buf, entries, packChecksum); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	idx, err := ReadIndex(bytes.NewReader(buf.Bytes()), "/fake/large.pack", 1<<33)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	defer idx.Close()

	off, ln, found := idx.Lookup(entries[1].Hash)
	if !found {
		t.Fatal("large offset entry not found")
	}
	if off != 1<<32 {
		t.Errorf("offset = %d, want %d", off, 1<<32)
	}
	if ln != 1024 {
		t.Errorf("length = %d, want 1024", ln)
	}
}

func TestIndexEmpty(t *testing.T) {
	packChecksum := sha1.Sum([]byte("empty"))
	var buf bytes.Buffer
	if err := WriteIndex(&buf, nil, packChecksum); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	idx, err := ReadIndex(bytes.NewReader(buf.Bytes()), "/fake/empty.pack", 0)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	defer idx.Close()
	if idx.ObjectCount() != 0 {
		t.Fatalf("expected 0 objects, got %d", idx.ObjectCount())
	}
	if idx.Contains(makeHash("0000000000000000000000000000000000000000")) {
		t.Error("empty index should not contain anything")
	}
}

func TestIndexChecksumIntegrity(t *testing.T) {
	entries := []IndexEntry{
		{Hash: makeHash("dd11000000000000000000000000000000000001"), Offset: 0, Length: 100},
	}
	packChecksum := sha1.Sum([]byte("integrity"))

	var buf bytes.Buffer
	if err := WriteIndex(&buf, entries, packChecksum); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	// Corrupt a byte in the middle.
	data := buf.Bytes()
	data[len(data)/2] ^= 0xff

	_, err := ReadIndex(bytes.NewReader(data), "/fake/bad.pack", 100)
	if err == nil {
		t.Fatal("expected checksum error, got nil")
	}
}
