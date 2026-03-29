package packfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrefixStateRefreshAndLookup(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "aa")

	// Create a pack with two objects.
	pw, err := NewPackWriter(packDir, 0xaa)
	if err != nil {
		t.Fatalf("NewPackWriter: %v", err)
	}
	hash1 := makeHash("aa11000000000000000000000000000000000001")
	data1 := []byte("object one")
	pw.Append(hash1, data1)

	hash2 := makeHash("aa22000000000000000000000000000000000002")
	data2 := []byte("object two")
	pw.Append(hash2, data2)

	_, err = pw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Create PrefixState and refresh.
	ps := NewPrefixState(packDir)
	if err := ps.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if ps.IndexCount() != 1 {
		t.Fatalf("IndexCount = %d, want 1", ps.IndexCount())
	}
	if ps.ObjectCount() != 2 {
		t.Fatalf("ObjectCount = %d, want 2", ps.ObjectCount())
	}

	// Lookup existing hash.
	if !ps.Contains(hash1) {
		t.Error("Contains(hash1) = false, want true")
	}
	packPath, off, ln, found := ps.Lookup(hash1)
	if !found {
		t.Fatal("Lookup(hash1) not found")
	}
	got, err := ReadObject(packPath, off, ln)
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if string(got) != string(data1) {
		t.Errorf("got %q, want %q", got, data1)
	}

	// Lookup missing hash.
	missing := makeHash("aa99000000000000000000000000000000000099")
	if ps.Contains(missing) {
		t.Error("Contains(missing) = true, want false")
	}
}

func TestPrefixStateMultiplePacks(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "bb")

	// Create first pack.
	pw1, _ := NewPackWriter(packDir, 0xbb)
	hash1 := makeHash("bb11000000000000000000000000000000000001")
	pw1.Append(hash1, []byte("pack one obj"))
	pw1.Finalize()

	// Create second pack.
	pw2, _ := NewPackWriter(packDir, 0xbb)
	hash2 := makeHash("bb22000000000000000000000000000000000002")
	pw2.Append(hash2, []byte("pack two obj"))
	pw2.Finalize()

	ps := NewPrefixState(packDir)
	ps.Refresh()

	if ps.IndexCount() != 2 {
		t.Fatalf("IndexCount = %d, want 2", ps.IndexCount())
	}

	// Both objects should be findable.
	if !ps.Contains(hash1) {
		t.Error("hash1 not found")
	}
	if !ps.Contains(hash2) {
		t.Error("hash2 not found")
	}
}

func TestPrefixStateRefreshAfterDeletion(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "cc")

	// Create a pack.
	pw, _ := NewPackWriter(packDir, 0xcc)
	hash1 := makeHash("cc11000000000000000000000000000000000001")
	pw.Append(hash1, []byte("doomed"))
	contentHash, _ := pw.Finalize()

	ps := NewPrefixState(packDir)
	ps.Refresh()
	if ps.IndexCount() != 1 {
		t.Fatalf("IndexCount = %d, want 1", ps.IndexCount())
	}

	// Delete the pack + index.
	os.Remove(filepath.Join(packDir, contentHash+".pack"))
	os.Remove(filepath.Join(packDir, contentHash+".idx"))

	ps.Refresh()
	if ps.IndexCount() != 0 {
		t.Fatalf("IndexCount after deletion = %d, want 0", ps.IndexCount())
	}
	if ps.Contains(hash1) {
		t.Error("hash1 should not be found after deletion")
	}
}

func TestStoreBasic(t *testing.T) {
	dir := t.TempDir()
	packsDir := filepath.Join(dir, "packs")

	// Create a pack in prefix aa.
	packDir := filepath.Join(packsDir, "aa")
	pw, _ := NewPackWriter(packDir, 0xaa)
	hash1 := makeHash("aa11000000000000000000000000000000000001")
	pw.Append(hash1, []byte("stored in aa"))
	pw.Finalize()

	store := NewStore(packsDir)
	store.RefreshAll()

	if !store.Contains(hash1) {
		t.Error("store.Contains(hash1) = false, want true")
	}

	// Different prefix should not find it.
	hashBB := makeHash("bb11000000000000000000000000000000000001")
	if store.Contains(hashBB) {
		t.Error("store.Contains(hashBB) should be false")
	}

	if store.TotalObjects() != 1 {
		t.Errorf("TotalObjects = %d, want 1", store.TotalObjects())
	}
}

func TestMRUPromotion(t *testing.T) {
	dir := t.TempDir()
	packDir := filepath.Join(dir, "packs", "dd")

	// Create two packs.
	pw1, _ := NewPackWriter(packDir, 0xdd)
	hash1 := makeHash("dd11000000000000000000000000000000000001")
	pw1.Append(hash1, []byte("first pack"))
	pw1.Finalize()

	pw2, _ := NewPackWriter(packDir, 0xdd)
	hash2 := makeHash("dd22000000000000000000000000000000000002")
	pw2.Append(hash2, []byte("second pack"))
	pw2.Finalize()

	ps := NewPrefixState(packDir)
	ps.Refresh()

	if ps.IndexCount() != 2 {
		t.Fatalf("IndexCount = %d, want 2", ps.IndexCount())
	}

	// Lookup hash1 — this should promote its index to MRU position.
	ps.Lookup(hash1)

	// The first index should now contain hash1.
	indices := ps.Indices()
	if !indices[0].Contains(hash1) {
		t.Error("after MRU promotion, first index should contain hash1")
	}

	// Now lookup hash2 — it should become MRU.
	ps.Lookup(hash2)
	indices = ps.Indices()
	if !indices[0].Contains(hash2) {
		t.Error("after MRU promotion, first index should contain hash2")
	}
}
