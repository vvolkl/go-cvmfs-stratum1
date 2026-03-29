package packfile

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestPackWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "aa")

	pw, err := NewPackWriter(packDir, 0xaa)
	if err != nil {
		t.Fatalf("NewPackWriter: %v", err)
	}

	// Append two small objects.
	hash1 := makeHash("aa11000000000000000000000000000000000001")
	data1 := []byte("hello world")
	off1, err := pw.Append(hash1, data1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off1 != 0 {
		t.Errorf("first offset = %d, want 0", off1)
	}

	hash2 := makeHash("aa22000000000000000000000000000000000002")
	data2 := []byte("second object with more bytes")
	off2, err := pw.Append(hash2, data2)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off2 != int64(len(data1)) {
		t.Errorf("second offset = %d, want %d", off2, len(data1))
	}

	if pw.Count() != 2 {
		t.Errorf("Count = %d, want 2", pw.Count())
	}

	contentHash, err := pw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if contentHash == "" {
		t.Fatal("Finalize returned empty hash")
	}

	// Verify files exist.
	packPath := filepath.Join(packDir, contentHash+".pack")
	idxPath := filepath.Join(packDir, contentHash+".idx")

	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("pack file missing: %v", err)
	}
	if _, err := os.Stat(idxPath); err != nil {
		t.Fatalf("index file missing: %v", err)
	}

	// Read back via index.
	fi, _ := os.Stat(packPath)
	idx, err := ReadIndexFile(idxPath, packPath, fi.Size())
	if err != nil {
		t.Fatalf("ReadIndexFile: %v", err)
	}
	defer idx.Close()

	if idx.ObjectCount() != 2 {
		t.Fatalf("index has %d objects, want 2", idx.ObjectCount())
	}

	// Read object 1.
	off, ln, found := idx.Lookup(hash1)
	if !found {
		t.Fatal("hash1 not found in index")
	}
	got, err := ReadObject(packPath, off, ln)
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if string(got) != string(data1) {
		t.Errorf("object 1 = %q, want %q", got, data1)
	}

	// Read object 2.
	off, ln, found = idx.Lookup(hash2)
	if !found {
		t.Fatal("hash2 not found in index")
	}
	got, err = ReadObject(packPath, off, ln)
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if string(got) != string(data2) {
		t.Errorf("object 2 = %q, want %q", got, data2)
	}
}

func TestPackWriterEmptyFinalize(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "bb")

	pw, err := NewPackWriter(packDir, 0xbb)
	if err != nil {
		t.Fatalf("NewPackWriter: %v", err)
	}

	hash, err := pw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if hash != "" {
		t.Errorf("expected empty hash for empty pack, got %q", hash)
	}

	// pack.tmp should have been removed.
	if _, err := os.Stat(filepath.Join(packDir, "pack.tmp")); !os.IsNotExist(err) {
		t.Error("pack.tmp should not exist after empty finalize")
	}
}

func TestPackWriterAbort(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "cc")

	pw, err := NewPackWriter(packDir, 0xcc)
	if err != nil {
		t.Fatalf("NewPackWriter: %v", err)
	}

	hash := makeHash("cc00000000000000000000000000000000000001")
	pw.Append(hash, []byte("data"))

	pw.Abort()

	if _, err := os.Stat(filepath.Join(packDir, "pack.tmp")); !os.IsNotExist(err) {
		t.Error("pack.tmp should not exist after abort")
	}
}

func TestLooseThreshold(t *testing.T) {
	// Just verify the constants are set correctly.
	if LooseThreshold != 256*1024 {
		t.Errorf("LooseThreshold = %d, want %d", LooseThreshold, 256*1024)
	}
	if MaxPackEntries != 599_148 {
		t.Errorf("MaxPackEntries = %d, want %d", MaxPackEntries, 599_148)
	}
}

func TestCleanupOrphanedPacks(t *testing.T) {
	dir := t.TempDir()

	// Create a pack.tmp in one prefix dir.
	prefix := filepath.Join(dir, "ab")
	os.MkdirAll(prefix, 0755)
	tmp := filepath.Join(prefix, "pack.tmp")
	os.WriteFile(tmp, []byte("orphaned"), 0644)

	CleanupOrphanedPacks(dir)

	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Error("pack.tmp should have been cleaned up")
	}
}

func TestReadObjectWriter(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "dd")

	pw, err := NewPackWriter(packDir, 0xdd)
	if err != nil {
		t.Fatalf("NewPackWriter: %v", err)
	}

	hash1 := makeHash("dd11000000000000000000000000000000000001")
	data1 := []byte("stream me")
	pw.Append(hash1, data1)

	contentHash, err := pw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	packPath := filepath.Join(packDir, contentHash+".pack")
	idxPath := filepath.Join(packDir, contentHash+".idx")

	fi, _ := os.Stat(packPath)
	idx, _ := ReadIndexFile(idxPath, packPath, fi.Size())
	defer idx.Close()

	off, ln, _ := idx.Lookup(hash1)
	var buf []byte
	w := &sliceWriter{buf: &buf}
	if err := ReadObjectWriter(packPath, off, ln, w); err != nil {
		t.Fatalf("ReadObjectWriter: %v", err)
	}
	if string(buf) != string(data1) {
		t.Errorf("got %q, want %q", buf, data1)
	}
}

// sliceWriter is a trivial io.Writer that appends to a byte slice.
type sliceWriter struct {
	buf *[]byte
}

func (sw *sliceWriter) Write(p []byte) (int, error) {
	*sw.buf = append(*sw.buf, p...)
	return len(p), nil
}

// makeHash is already defined in index_test.go for same package tests,
// but since Go tests are per-package we need it here too.  Since this is
// the same package, the one in index_test.go suffices.  We'll use a
// different helper name to avoid redeclaration.
func makeHash2(hexStr string) [20]byte {
	var h [20]byte
	b, _ := hex.DecodeString(hexStr)
	copy(h[:], b)
	return h
}
