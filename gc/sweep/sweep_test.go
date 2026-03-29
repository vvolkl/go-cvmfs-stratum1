package sweep

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestSweepDeletesUnreachable(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	chunkDir := filepath.Join(tmpDir, "chunks")

	// Create some fake object files under data/XX/...
	// Reachable: aa/1111... and bb/2222...
	// Unreachable: aa/9999... and cc/3333...
	reachableHashes := []string{
		"aa" + pad("1111", 38),
		"bb" + pad("2222", 38),
	}
	unreachableHashes := []string{
		"aa" + pad("9999", 38),
		"cc" + pad("3333", 38),
	}

	allHashes := append(reachableHashes, unreachableHashes...)
	for _, h := range allHashes {
		prefix := h[:2]
		rest := h[2:]
		dir := filepath.Join(dataDir, prefix)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(dir, rest))
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("dummy content")
		f.Close()
	}

	// Write sorted chunk files from the reachable hashes.
	sort.Strings(reachableHashes)
	if err := WriteChunkFromHashes(chunkDir, reachableHashes); err != nil {
		t.Fatalf("WriteChunkFromHashes: %v", err)
	}

	cfg := Config{
		DataDir:  dataDir,
		ChunkDir: chunkDir,
		DryRun:   false,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Check that reachable files still exist
	for _, h := range reachableHashes {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("reachable file was deleted: %s", h)
		}
	}

	// Check that unreachable files were deleted
	for _, h := range unreachableHashes {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("unreachable file was not deleted: %s", h)
		}
	}

	t.Logf("Stats: checked=%d deleted=%d retained=%d",
		stats.FilesChecked, stats.FilesDeleted, stats.FilesRetained)
}

func TestSweepDryRun(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	chunkDir := filepath.Join(tmpDir, "chunks") // empty - no reachable hashes

	// Create one unreachable file
	hash := "ab" + pad("face", 36)
	dir := filepath.Join(dataDir, "ab")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Create(filepath.Join(dir, hash[2:]))
	f.WriteString("data")
	f.Close()

	// No chunk files -- everything is unreachable
	os.MkdirAll(chunkDir, 0755)
	cfg := Config{
		DataDir:  dataDir,
		ChunkDir: chunkDir,
		DryRun:   true,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// File should still exist because dry run
	p := filepath.Join(dir, hash[2:])
	if _, err := os.Stat(p); os.IsNotExist(err) {
		t.Error("dry run deleted a file")
	}

	if stats.FilesDeleted != 1 {
		t.Errorf("expected 1 file marked for deletion, got %d", stats.FilesDeleted)
	}
}

func TestSweepMultipleChunks(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	chunkDir := filepath.Join(tmpDir, "chunks")

	// Create files across several prefixes.
	reachable := []string{
		"11" + pad("aaaa", 38),
		"22" + pad("bbbb", 38),
		"33" + pad("cccc", 38),
		"ff" + pad("dddd", 38),
	}
	unreachable := []string{
		"11" + pad("zzzz", 38),
		"22" + pad("zzzz", 38),
		"44" + pad("eeee", 38),
	}

	for _, h := range append(reachable, unreachable...) {
		prefix := h[:2]
		rest := h[2:]
		dir := filepath.Join(dataDir, prefix)
		os.MkdirAll(dir, 0755)
		f, _ := os.Create(filepath.Join(dir, rest))
		f.WriteString("content")
		f.Close()
	}

	// Write chunk files from reachable hashes.
	if err := WriteChunkFromHashes(chunkDir, reachable); err != nil {
		t.Fatalf("WriteChunkFromHashes: %v", err)
	}

	cfg := Config{
		DataDir:  dataDir,
		ChunkDir: chunkDir,
		DryRun:   false,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	for _, h := range reachable {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("reachable file was deleted: %s", h)
		}
	}

	for _, h := range unreachable {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("unreachable file was not deleted: %s", h)
		}
	}

	if stats.FilesDeleted != int64(len(unreachable)) {
		t.Errorf("expected %d deletions, got %d", len(unreachable), stats.FilesDeleted)
	}

	t.Logf("Stats: checked=%d deleted=%d retained=%d",
		stats.FilesChecked, stats.FilesDeleted, stats.FilesRetained)
}

// pad returns s repeated/truncated to exactly n characters.
func pad(s string, n int) string {
	result := ""
	for len(result) < n {
		result += s
	}
	return result[:n]
}

func TestSweepDeletedBySuffix(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	chunkDir := filepath.Join(tmpDir, "chunks")
	os.MkdirAll(chunkDir, 0755)

	// Create files with various suffixes — all unreachable.
	// CVMFS filenames: prefix dir (2 hex chars) + rest-of-hash + optional suffix.
	unreachable := []string{
		"aa/" + pad("1111", 38),       // no suffix (data object)
		"aa/" + pad("2222", 38) + "P", // partial chunk
		"bb/" + pad("3333", 38) + "C", // catalog
		"bb/" + pad("4444", 38) + "P", // partial chunk
		"cc/" + pad("5555", 38) + "L", // micro-catalog
	}
	for _, h := range unreachable {
		dir := filepath.Join(dataDir, h[:2])
		os.MkdirAll(dir, 0755)
		f, _ := os.Create(filepath.Join(dataDir, h))
		f.WriteString("data")
		f.Close()
	}

	// No reachable hashes — everything gets deleted.
	cfg := Config{
		DataDir:  dataDir,
		ChunkDir: chunkDir,
		DryRun:   true,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if stats.FilesDeleted != int64(len(unreachable)) {
		t.Errorf("expected %d deletions, got %d", len(unreachable), stats.FilesDeleted)
	}

	// Check per-suffix counts.
	if got := stats.DeletedBySuffix[0]; got != 1 {
		t.Errorf("DeletedBySuffix[none]: want 1, got %d", got)
	}
	if got := stats.DeletedBySuffix['P']; got != 2 {
		t.Errorf("DeletedBySuffix['P']: want 2, got %d", got)
	}
	if got := stats.DeletedBySuffix['C']; got != 1 {
		t.Errorf("DeletedBySuffix['C']: want 1, got %d", got)
	}
	if got := stats.DeletedBySuffix['L']; got != 1 {
		t.Errorf("DeletedBySuffix['L']: want 1, got %d", got)
	}

	t.Logf("Suffix breakdown: none=%d P=%d C=%d L=%d",
		stats.DeletedBySuffix[0], stats.DeletedBySuffix['P'],
		stats.DeletedBySuffix['C'], stats.DeletedBySuffix['L'])
}

func TestCollectCandidates(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	chunkDir := filepath.Join(tmpDir, "chunks")

	reachable := []string{
		"aa" + pad("1111", 38),
		"bb" + pad("2222", 38),
	}
	unreachable := []string{
		"aa" + pad("9999", 38),
		"cc" + pad("3333", 38),
	}

	for _, h := range append(reachable, unreachable...) {
		dir := filepath.Join(dataDir, h[:2])
		os.MkdirAll(dir, 0755)
		f, _ := os.Create(filepath.Join(dir, h[2:]))
		f.WriteString("dummy")
		f.Close()
	}

	if err := WriteChunkFromHashes(chunkDir, reachable); err != nil {
		t.Fatalf("WriteChunkFromHashes: %v", err)
	}

	cfg := Config{
		DataDir:  dataDir,
		ChunkDir: chunkDir,
	}

	var stats Stats
	candidates, err := CollectCandidates(cfg, &stats)
	if err != nil {
		t.Fatalf("CollectCandidates: %v", err)
	}

	if len(candidates) != len(unreachable) {
		t.Errorf("got %d candidates, want %d", len(candidates), len(unreachable))
	}

	for _, h := range unreachable {
		if _, ok := candidates[h]; !ok {
			t.Errorf("missing candidate: %s", h)
		}
	}
	for _, h := range reachable {
		if _, ok := candidates[h]; ok {
			t.Errorf("reachable hash should not be a candidate: %s", h)
		}
	}

	// Files should still exist — CollectCandidates doesn't delete.
	for _, h := range unreachable {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("CollectCandidates deleted a file: %s", h)
		}
	}

	if stats.CandidatesFound != int64(len(unreachable)) {
		t.Errorf("CandidatesFound: got %d, want %d", stats.CandidatesFound, len(unreachable))
	}
	t.Logf("Candidates: %d found, %d checked, %d retained",
		stats.CandidatesFound, stats.FilesChecked, stats.FilesRetained)
}

func TestSubtractReachable(t *testing.T) {
	candidates := map[string]Candidate{
		"aa1111": {FullHash: "aa1111"},
		"bb2222": {FullHash: "bb2222"},
		"cc3333": {FullHash: "cc3333"},
		"dd4444": {FullHash: "dd4444"},
	}

	reachable := map[string]struct{}{
		"bb2222": {},
		"dd4444": {},
		"ee5555": {}, // not in candidates — should be harmless
	}

	var stats Stats
	SubtractReachable(candidates, reachable, &stats)

	if len(candidates) != 2 {
		t.Errorf("expected 2 candidates after subtract, got %d", len(candidates))
	}
	if _, ok := candidates["aa1111"]; !ok {
		t.Error("aa1111 should remain")
	}
	if _, ok := candidates["cc3333"]; !ok {
		t.Error("cc3333 should remain")
	}
	if _, ok := candidates["bb2222"]; ok {
		t.Error("bb2222 should have been subtracted")
	}
	if _, ok := candidates["dd4444"]; ok {
		t.Error("dd4444 should have been subtracted")
	}

	if stats.CandidatesProtected != 2 {
		t.Errorf("CandidatesProtected: got %d, want 2", stats.CandidatesProtected)
	}
}

func TestDeleteCandidates(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files to delete.
	paths := make(map[string]string)
	for _, name := range []string{"aa1111", "bb2222C", "cc3333P"} {
		p := filepath.Join(tmpDir, name)
		f, _ := os.Create(p)
		f.WriteString("content")
		f.Close()
		paths[name] = p
	}

	candidates := map[string]Candidate{
		"aa1111":  {FullHash: "aa1111", FilePath: paths["aa1111"]},
		"bb2222C": {FullHash: "bb2222C", FilePath: paths["bb2222C"]},
		"cc3333P": {FullHash: "cc3333P", FilePath: paths["cc3333P"]},
	}

	cfg := Config{DryRun: false}
	var stats Stats
	DeleteCandidates(candidates, cfg, &stats)

	if stats.FilesDeleted != 3 {
		t.Errorf("FilesDeleted: got %d, want 3", stats.FilesDeleted)
	}
	if stats.BytesFreed != 21 {
		t.Errorf("BytesFreed: got %d, want 21", stats.BytesFreed)
	}

	// Check suffix counters.
	if stats.DeletedBySuffix[0] != 1 {
		t.Errorf("DeletedBySuffix[0]: got %d, want 1", stats.DeletedBySuffix[0])
	}
	if stats.DeletedBySuffix['C'] != 1 {
		t.Errorf("DeletedBySuffix['C']: got %d, want 1", stats.DeletedBySuffix['C'])
	}
	if stats.DeletedBySuffix['P'] != 1 {
		t.Errorf("DeletedBySuffix['P']: got %d, want 1", stats.DeletedBySuffix['P'])
	}

	// Files should be gone.
	for name, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("file %s should have been deleted", name)
		}
	}
}
