package hashsort

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrefixWriteAndSortChunks(t *testing.T) {
	tmpDir := t.TempDir()
	chunkDir := filepath.Join(tmpDir, "chunks")

	// Generate 10000 pseudo-random entries.
	input := make(chan Entry, 256)
	go func() {
		for i := 0; i < 10000; i++ {
			var raw [20]byte
			raw[0] = byte(i >> 8)
			raw[1] = byte(i)
			raw[2] = byte(i >> 16)
			for j := 3; j < 20; j++ {
				raw[j] = byte(i*7 + j*13)
			}
			var e Entry
			copy(e[:20], raw[:])
			e[20] = 0 // no suffix
			input <- e
		}
		close(input)
	}()

	// PrefixWrite with a small heap to force evictions.
	cfg := PrefixWriterConfig{
		HeapBytes: 50 * 1024, // ~2400 entries per prefix
	}
	chunkFiles, err := PrefixWrite(cfg, input, chunkDir, nil, nil, nil)
	if err != nil {
		t.Fatalf("PrefixWrite failed: %v", err)
	}

	if len(chunkFiles) == 0 {
		t.Fatal("Expected at least one chunk file")
	}

	t.Logf("Produced %d chunk files", len(chunkFiles))

	// Verify each chunk file has valid entries.
	for _, cf := range chunkFiles {
		fi, err := os.Stat(cf)
		if err != nil {
			t.Fatalf("stat %s: %v", cf, err)
		}
		if fi.Size()%EntrySize != 0 {
			t.Errorf("chunk %s size %d not a multiple of %d", cf, fi.Size(), EntrySize)
		}
	}

	// Sort the chunks.
	sortCfg := DefaultChunkSortConfig()
	if err := SortChunks(chunkFiles, sortCfg, nil, nil); err != nil {
		t.Fatalf("SortChunks failed: %v", err)
	}

	// Verify each chunk is now sorted and deduplicated.
	totalEntries := int64(0)
	for _, cf := range chunkFiles {
		entries, err := ReadAllEntries(cf)
		if err != nil {
			t.Fatalf("ReadAllEntries %s: %v", cf, err)
		}
		for i := 1; i < len(entries); i++ {
			if CompareEntries(entries[i-1], entries[i]) >= 0 {
				t.Errorf("chunk %s not sorted/deduped at index %d", cf, i)
				break
			}
		}
		totalEntries += int64(len(entries))
	}

	t.Logf("Total entries after sort+dedup: %d", totalEntries)
}

func TestChunkReader(t *testing.T) {
	tmpDir := t.TempDir()
	chunkDir := filepath.Join(tmpDir, "chunks")

	// Generate entries, write, sort.
	input := make(chan Entry, 256)
	go func() {
		for i := 0; i < 5000; i++ {
			var raw [20]byte
			raw[0] = byte(i >> 8)
			raw[1] = byte(i)
			raw[2] = byte(i >> 16)
			for j := 3; j < 20; j++ {
				raw[j] = byte(i*7 + j*13)
			}
			var e Entry
			copy(e[:20], raw[:])
			e[20] = 0
			input <- e
		}
		close(input)
	}()

	cfg := PrefixWriterConfig{HeapBytes: 30 * 1024}
	chunkFiles, err := PrefixWrite(cfg, input, chunkDir, nil, nil, nil)
	if err != nil {
		t.Fatalf("PrefixWrite failed: %v", err)
	}

	sortCfg := DefaultChunkSortConfig()
	if err := SortChunks(chunkFiles, sortCfg, nil, nil); err != nil {
		t.Fatalf("SortChunks failed: %v", err)
	}

	// Read each chunk file with ChunkReader and verify sorted.
	totalRead := 0
	for _, cf := range chunkFiles {
		reader, err := NewChunkReader(cf)
		if err != nil {
			t.Fatalf("NewChunkReader %s: %v", cf, err)
		}

		var prev Entry
		first := true
		count := 0
		for reader.Next() {
			val := reader.Value()
			if !first && CompareEntries(val, prev) <= 0 {
				t.Errorf("chunk %s not sorted at entry %d", cf, count)
				break
			}
			prev = val
			first = false
			count++
		}
		reader.Close()
		totalRead += count
	}

	if totalRead == 0 {
		t.Error("ChunkReader produced no output")
	}

	t.Logf("ChunkReader read %d total entries from %d chunks", totalRead, len(chunkFiles))
}

func TestEntryFromHexSuffix(t *testing.T) {
	hexHash := "aabbccddee0011223344aabbccddee0011223344"
	suffix := byte('C')
	e := EntryFromHexSuffix(hexHash, suffix)

	if got := e.Hex(); got != hexHash {
		t.Errorf("Hex(): got %s, want %s", got, hexHash)
	}
	if got := e.Suffix(); got != suffix {
		t.Errorf("Suffix(): got %c, want %c", got, suffix)
	}
	if got := e.String(); got != hexHash+"C" {
		t.Errorf("String(): got %s, want %s", got, hexHash+"C")
	}
	if got := e.Prefix(); got != 0xaa {
		t.Errorf("Prefix(): got %02x, want aa", got)
	}
}

func TestEntryNoSuffix(t *testing.T) {
	hexHash := "0000000000000000000000000000000000000000"
	e := EntryFromHexSuffix(hexHash, 0)
	if got := e.String(); got != hexHash {
		t.Errorf("String(): got %s, want %s", got, hexHash)
	}
}

func TestPrefixFromChunkFile(t *testing.T) {
	tests := []struct {
		path       string
		wantPrefix byte
		wantOK     bool
	}{
		{"/tmp/chunks/chunk-ab", 0xab, true},
		{"/tmp/chunks/chunk-00", 0x00, true},
		{"/tmp/chunks/chunk-ff", 0xff, true},
		{"/tmp/chunks/chunk-000000.txt", 0, false}, // old format
		{"/tmp/chunks/notachunk", 0, false},
	}

	for _, tt := range tests {
		prefix, ok := PrefixFromChunkFile(tt.path)
		if ok != tt.wantOK || prefix != tt.wantPrefix {
			t.Errorf("PrefixFromChunkFile(%q) = (%02x, %v), want (%02x, %v)",
				tt.path, prefix, ok, tt.wantPrefix, tt.wantOK)
		}
	}
}

func TestPrefixPartitioning(t *testing.T) {
	// Verify that entries are partitioned correctly: each chunk file
	// should only contain entries whose first byte matches the prefix.
	tmpDir := t.TempDir()
	chunkDir := filepath.Join(tmpDir, "chunks")

	input := make(chan Entry, 256)
	go func() {
		// Create entries spanning multiple prefixes.
		for prefix := 0; prefix < 256; prefix += 17 {
			for i := 0; i < 50; i++ {
				var e Entry
				e[0] = byte(prefix)
				e[1] = byte(i)
				for j := 2; j < 20; j++ {
					e[j] = byte(prefix ^ i ^ j)
				}
				e[20] = 0
				input <- e
			}
		}
		close(input)
	}()

	cfg := PrefixWriterConfig{HeapBytes: 1024}
	chunkFiles, err := PrefixWrite(cfg, input, chunkDir, nil, nil, nil)
	if err != nil {
		t.Fatalf("PrefixWrite failed: %v", err)
	}

	sortCfg := DefaultChunkSortConfig()
	if err := SortChunks(chunkFiles, sortCfg, nil, nil); err != nil {
		t.Fatalf("SortChunks failed: %v", err)
	}

	// Verify partitioning.
	for _, cf := range chunkFiles {
		expectedPrefix, ok := PrefixFromChunkFile(cf)
		if !ok {
			t.Errorf("can't parse prefix from %s", cf)
			continue
		}

		entries, err := ReadAllEntries(cf)
		if err != nil {
			t.Fatalf("ReadAllEntries %s: %v", cf, err)
		}

		for i, e := range entries {
			if e.Prefix() != expectedPrefix {
				t.Errorf("chunk %s entry %d: prefix %02x, expected %02x",
					cf, i, e.Prefix(), expectedPrefix)
			}
		}
	}
}

func TestCountEntries(t *testing.T) {
	tmpDir := t.TempDir()
	chunkDir := filepath.Join(tmpDir, "chunks")

	input := make(chan Entry, 256)
	go func() {
		for i := 0; i < 100; i++ {
			var e Entry
			e[0] = 0xaa
			e[1] = byte(i)
			input <- e
		}
		close(input)
	}()

	cfg := PrefixWriterConfig{HeapBytes: 10 * 1024}
	chunkFiles, err := PrefixWrite(cfg, input, chunkDir, nil, nil, nil)
	if err != nil {
		t.Fatalf("PrefixWrite failed: %v", err)
	}

	sortCfg := DefaultChunkSortConfig()
	if err := SortChunks(chunkFiles, sortCfg, nil, nil); err != nil {
		t.Fatalf("SortChunks failed: %v", err)
	}

	total, err := CountEntriesMulti(chunkFiles)
	if err != nil {
		t.Fatalf("CountEntriesMulti: %v", err)
	}

	if total != 100 {
		t.Errorf("CountEntriesMulti = %d, want 100", total)
	}
}
