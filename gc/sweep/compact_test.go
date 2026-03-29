package sweep

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/packfile"
)

// makeHash builds a [20]byte from a hex prefix, zero-padded.
func makeHash(hexPrefix string) [20]byte {
	full := hexPrefix
	for len(full) < 40 {
		full += "0"
	}
	var h [20]byte
	hex.Decode(h[:], []byte(full))
	return h
}

// TestCompactMergesPacks verifies that CompactAll always merges packs,
// keeping only reachable objects, and that reclaimed bytes are reported.
func TestCompactMergesPacks(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	packsDir := filepath.Join(tmpDir, "packs")
	chunkDir := filepath.Join(tmpDir, "chunks")

	// We'll work with prefix 0xaa.
	prefix := byte(0xaa)
	prefixHex := fmt.Sprintf("%02x", prefix)
	packDir := filepath.Join(packsDir, prefixHex)
	os.MkdirAll(filepath.Join(dataDir, prefixHex), 0755)
	os.MkdirAll(packDir, 0755)
	os.MkdirAll(chunkDir, 0755)

	// Create two packs with 3 objects each (some overlap).
	hashA := makeHash("aa01") // reachable
	hashB := makeHash("aa02") // unreachable
	hashC := makeHash("aa03") // reachable
	hashD := makeHash("aa04") // unreachable
	hashE := makeHash("aa05") // reachable (in pack 2)

	dataA := []byte("object-A-data")
	dataB := []byte("object-B-data-unreachable")
	dataC := []byte("object-C-data")
	dataD := []byte("object-D-data-unreachable")
	dataE := []byte("object-E-data-new")

	// Pack 1: A, B, C
	pw1, err := packfile.NewPackWriter(packDir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	pw1.Append(hashA, dataA)
	pw1.Append(hashB, dataB)
	pw1.Append(hashC, dataC)
	name1, err := pw1.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if name1 == "" {
		t.Fatal("pack 1 should have a name")
	}

	// Pack 2: C (duplicate), D, E
	pw2, err := packfile.NewPackWriter(packDir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	pw2.Append(hashC, dataC) // duplicate of pack 1
	pw2.Append(hashD, dataD)
	pw2.Append(hashE, dataE)
	name2, err := pw2.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if name2 == "" {
		t.Fatal("pack 2 should have a name")
	}

	// Write chunk file: reachable = {hashA, hashC, hashE}
	writeChunkFile(t, chunkDir, prefix, []hashsort.Entry{
		hashsort.EntryFromHexSuffix(hex.EncodeToString(hashA[:]), 0),
		hashsort.EntryFromHexSuffix(hex.EncodeToString(hashC[:]), 0),
		hashsort.EntryFromHexSuffix(hex.EncodeToString(hashE[:]), 0),
	})

	// Run compaction.
	cfg := CompactConfig{
		DataDir:  dataDir,
		PacksDir: packsDir,
		ChunkDir: chunkDir,
	}
	var stats CompactStats
	if err := CompactAll(cfg, &stats); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	t.Logf("Stats: packs_read=%d packs_written=%d kept=%d removed=%d reclaimed=%d",
		stats.PacksRead, stats.PacksWritten,
		stats.PackObjectsKept, stats.PackObjectsRemoved,
		stats.PackBytesReclaimed)

	// Verify stats.
	if stats.PacksRead != 2 {
		t.Errorf("PacksRead = %d, want 2", stats.PacksRead)
	}
	if stats.PackObjectsKept != 3 { // A, C, E (C deduplicated)
		t.Errorf("PackObjectsKept = %d, want 3", stats.PackObjectsKept)
	}
	if stats.PackObjectsRemoved != 2 { // B, D (duplicate C is skipped, not counted)
		t.Errorf("PackObjectsRemoved = %d, want 2", stats.PackObjectsRemoved)
	}
	if stats.PacksWritten < 1 {
		t.Error("expected at least 1 new pack written")
	}
	if stats.PackBytesReclaimed <= 0 {
		t.Error("expected positive bytes reclaimed")
	}

	// Verify old packs are deleted.
	for _, name := range []string{name1, name2} {
		p := filepath.Join(packDir, name+".pack")
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("old pack %s should be deleted", p)
		}
	}

	// Verify new pack contains only reachable objects.
	store := packfile.NewStore(packsDir)
	store.RefreshAll()

	for _, tc := range []struct {
		hash [20]byte
		want bool
		name string
	}{
		{hashA, true, "A"},
		{hashB, false, "B"},
		{hashC, true, "C"},
		{hashD, false, "D"},
		{hashE, true, "E"},
	} {
		found := store.Contains(tc.hash)
		if found != tc.want {
			t.Errorf("object %s: found=%v, want=%v", tc.name, found, tc.want)
		}
	}

	// Verify we can read the kept objects back correctly.
	for _, tc := range []struct {
		hash [20]byte
		data []byte
		name string
	}{
		{hashA, dataA, "A"},
		{hashC, dataC, "C"},
		{hashE, dataE, "E"},
	} {
		pp, off, ln, found := store.Lookup(tc.hash)
		if !found {
			t.Fatalf("object %s not found in new pack", tc.name)
		}
		if ln <= 0 {
			t.Fatalf("object %s has invalid length %d (offset=%d)", tc.name, ln, off)
		}
		got, err := packfile.ReadObject(pp, off, ln)
		if err != nil {
			t.Errorf("reading object %s: %v", tc.name, err)
			continue
		}
		if string(got) != string(tc.data) {
			t.Errorf("object %s data = %q, want %q", tc.name, got, tc.data)
		}
	}
}

// TestCompactDeletesLooseFiles verifies that unreachable loose files are
// deleted and reachable ones are kept.
func TestCompactDeletesLooseFiles(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	packsDir := filepath.Join(tmpDir, "packs")
	chunkDir := filepath.Join(tmpDir, "chunks")

	prefix := byte(0xbb)
	prefixHex := fmt.Sprintf("%02x", prefix)
	os.MkdirAll(filepath.Join(dataDir, prefixHex), 0755)
	os.MkdirAll(filepath.Join(packsDir, prefixHex), 0755)
	os.MkdirAll(chunkDir, 0755)

	hashR := makeHash("bb01") // reachable
	hashU := makeHash("bb02") // unreachable

	// Create loose files.
	rHex := hex.EncodeToString(hashR[:])
	uHex := hex.EncodeToString(hashU[:])

	rPath := filepath.Join(dataDir, prefixHex, rHex[2:])
	uPath := filepath.Join(dataDir, prefixHex, uHex[2:])
	os.WriteFile(rPath, []byte("reachable-data"), 0644)
	os.WriteFile(uPath, []byte("unreachable-data"), 0644)

	// Write chunk file: only hashR is reachable.
	writeChunkFile(t, chunkDir, prefix, []hashsort.Entry{
		hashsort.EntryFromHexSuffix(rHex, 0),
	})

	cfg := CompactConfig{
		DataDir:  dataDir,
		PacksDir: packsDir,
		ChunkDir: chunkDir,
	}
	var stats CompactStats
	if err := CompactAll(cfg, &stats); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	t.Logf("Stats: loose_kept=%d loose_deleted=%d loose_freed=%d",
		stats.LooseFilesKept, stats.LooseFilesDeleted, stats.LooseBytesFreed)

	if stats.LooseFilesKept != 1 {
		t.Errorf("LooseFilesKept = %d, want 1", stats.LooseFilesKept)
	}
	if stats.LooseFilesDeleted != 1 {
		t.Errorf("LooseFilesDeleted = %d, want 1", stats.LooseFilesDeleted)
	}
	if stats.LooseBytesFreed != int64(len("unreachable-data")) {
		t.Errorf("LooseBytesFreed = %d, want %d", stats.LooseBytesFreed, len("unreachable-data"))
	}

	// Reachable file should still exist.
	if _, err := os.Stat(rPath); err != nil {
		t.Errorf("reachable file should still exist: %v", err)
	}
	// Unreachable file should be deleted.
	if _, err := os.Stat(uPath); !os.IsNotExist(err) {
		t.Error("unreachable file should be deleted")
	}
}

// TestCompactNoChunkFile verifies that if no chunk file exists (no
// reachable hashes), all objects are removed.
func TestCompactNoChunkFile(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	packsDir := filepath.Join(tmpDir, "packs")
	chunkDir := filepath.Join(tmpDir, "chunks")
	os.MkdirAll(chunkDir, 0755) // empty, no chunk files

	prefix := byte(0xcc)
	prefixHex := fmt.Sprintf("%02x", prefix)
	packDir := filepath.Join(packsDir, prefixHex)
	os.MkdirAll(filepath.Join(dataDir, prefixHex), 0755)
	os.MkdirAll(packDir, 0755)

	hash1 := makeHash("cc01")
	data1 := []byte("should-be-removed")

	// Create a pack with one object.
	pw, err := packfile.NewPackWriter(packDir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	pw.Append(hash1, data1)
	name, err := pw.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if name == "" {
		t.Fatal("expected pack name")
	}

	// Create a loose file.
	h1Hex := hex.EncodeToString(hash1[:])
	loosePath := filepath.Join(dataDir, prefixHex, h1Hex[2:])
	os.WriteFile(loosePath, []byte("loose-unreachable"), 0644)

	cfg := CompactConfig{
		DataDir:  dataDir,
		PacksDir: packsDir,
		ChunkDir: chunkDir,
	}
	var stats CompactStats
	if err := CompactAll(cfg, &stats); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	if stats.PackObjectsRemoved != 1 {
		t.Errorf("PackObjectsRemoved = %d, want 1", stats.PackObjectsRemoved)
	}
	if stats.PackObjectsKept != 0 {
		t.Errorf("PackObjectsKept = %d, want 0", stats.PackObjectsKept)
	}
	if stats.LooseFilesDeleted != 1 {
		t.Errorf("LooseFilesDeleted = %d, want 1", stats.LooseFilesDeleted)
	}

	// Verify pack file was deleted and not recreated.
	entries, _ := os.ReadDir(packDir)
	for _, e := range entries {
		if e.Name() != "." {
			t.Errorf("pack dir should be empty after GC, found: %s", e.Name())
		}
	}

	// Verify loose file was deleted.
	if _, err := os.Stat(loosePath); !os.IsNotExist(err) {
		t.Error("loose file should be deleted")
	}
}

// TestCompactDryRun verifies that dry-run mode counts but doesn't delete.
func TestCompactDryRun(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	packsDir := filepath.Join(tmpDir, "packs")
	chunkDir := filepath.Join(tmpDir, "chunks")
	os.MkdirAll(chunkDir, 0755)

	prefix := byte(0xdd)
	prefixHex := fmt.Sprintf("%02x", prefix)
	packDir := filepath.Join(packsDir, prefixHex)
	os.MkdirAll(filepath.Join(dataDir, prefixHex), 0755)
	os.MkdirAll(packDir, 0755)

	hash1 := makeHash("dd01")
	data1 := []byte("should-survive-dry-run")

	pw, err := packfile.NewPackWriter(packDir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	pw.Append(hash1, data1)
	packName, err := pw.Finalize()
	if err != nil {
		t.Fatal(err)
	}

	// Create a loose file.
	h1Hex := hex.EncodeToString(hash1[:])
	loosePath := filepath.Join(dataDir, prefixHex, h1Hex[2:])
	os.WriteFile(loosePath, data1, 0644)

	// No chunk file → everything is unreachable, but dry-run.
	cfg := CompactConfig{
		DataDir:  dataDir,
		PacksDir: packsDir,
		ChunkDir: chunkDir,
		DryRun:   true,
	}
	var stats CompactStats
	if err := CompactAll(cfg, &stats); err != nil {
		t.Fatalf("CompactAll: %v", err)
	}

	if stats.PackObjectsRemoved != 1 {
		t.Errorf("PackObjectsRemoved = %d, want 1", stats.PackObjectsRemoved)
	}
	if stats.LooseFilesDeleted != 1 {
		t.Errorf("LooseFilesDeleted = %d, want 1", stats.LooseFilesDeleted)
	}

	// Verify files still exist (dry-run).
	packPath := filepath.Join(packDir, packName+".pack")
	if _, err := os.Stat(packPath); err != nil {
		t.Errorf("pack should still exist in dry-run: %v", err)
	}
	if _, err := os.Stat(loosePath); err != nil {
		t.Errorf("loose file should still exist in dry-run: %v", err)
	}
}

// writeChunkFile writes a sorted binary chunk file for the given prefix.
func writeChunkFile(t *testing.T, chunkDir string, prefix byte, entries []hashsort.Entry) {
	t.Helper()
	chunkPath := hashsort.ChunkFileForPrefix(chunkDir, prefix)
	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, e := range entries {
		if _, err := f.Write(e[:]); err != nil {
			t.Fatal(err)
		}
	}
}
