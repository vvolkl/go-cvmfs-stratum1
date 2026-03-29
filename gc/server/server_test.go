package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbockelm/cvmfs-gc-optim/gc/packfile"
)

func TestHandleManifest(t *testing.T) {
	dir := t.TempDir()
	manifest := []byte("Cabcdef0123456789abcdef0123456789abcdef01\nNtest.repo\n")
	os.WriteFile(filepath.Join(dir, ".cvmfspublished"), manifest, 0644)

	srv, err := New(Config{RepoDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/.cvmfspublished", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != string(manifest) {
		t.Fatalf("body mismatch: %q", body)
	}
}

func TestHandleDataLoose(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data", "ab")
	os.MkdirAll(dataDir, 0755)
	os.WriteFile(filepath.Join(dataDir, "cdef01234567890123456789abcdef0123456789"), []byte("hello"), 0644)

	srv, err := New(Config{RepoDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/data/ab/cdef01234567890123456789abcdef0123456789", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "hello" {
		t.Fatalf("expected 'hello', got %q", body)
	}
}

func TestHandleDataPack(t *testing.T) {
	dir := t.TempDir()
	packsDir := filepath.Join(dir, "packs", "ab")
	os.MkdirAll(packsDir, 0755)

	// Write a pack with one object.
	pw, err := packfile.NewPackWriter(packsDir, 0xab)
	if err != nil {
		t.Fatal(err)
	}
	var hash [20]byte
	hash[0] = 0xab
	hash[1] = 0xcd
	hash[2] = 0xef
	objData := []byte("packed-content")
	pw.Append(hash, objData)
	pw.Finalize()

	srv, err := New(Config{RepoDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	// Construct the URL: /data/ab/<rest>
	hexHash := "abcdef0000000000000000000000000000000000"
	req := httptest.NewRequest("GET", "/data/ab/"+hexHash[2:], nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "packed-content" {
		t.Fatalf("expected 'packed-content', got %q", body)
	}
}

func TestHandleData404(t *testing.T) {
	dir := t.TempDir()
	srv, err := New(Config{RepoDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/data/ab/0000000000000000000000000000000000000000", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestHandleStatus(t *testing.T) {
	dir := t.TempDir()
	srv, err := New(Config{RepoDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/.cvmfs_status.json", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var st Status
	if err := json.NewDecoder(rr.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.State != "idle" {
		t.Fatalf("expected state=idle, got %q", st.State)
	}
}

func TestHandleDataUpstreamFallback(t *testing.T) {
	// Spin up a fake stratum-1 that serves one object.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/data/ab/cdef01234567890123456789abcdef0123456789" {
			w.Header().Set("Content-Length", "14")
			w.Write([]byte("upstream-data!"))
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	dir := t.TempDir()
	srv, err := New(Config{RepoDir: dir, Stratum1URL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	// Request an object not present locally — should proxy to upstream.
	req := httptest.NewRequest("GET", "/data/ab/cdef01234567890123456789abcdef0123456789", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "upstream-data!" {
		t.Fatalf("expected 'upstream-data!', got %q", body)
	}
	if srv.UpstreamHits != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", srv.UpstreamHits)
	}
}

func TestHandleDataUpstream404(t *testing.T) {
	// Upstream that 404s everything.
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()

	dir := t.TempDir()
	srv, err := New(Config{RepoDir: dir, Stratum1URL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/data/ab/0000000000000000000000000000000000000000", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}
