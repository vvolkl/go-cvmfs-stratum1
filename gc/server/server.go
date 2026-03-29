// Package server implements an HTTP server for a CVMFS repository mirror
// with automatic mirror+GC scheduling.
//
// The server serves repository objects from both loose files (data/XX/)
// and pack files (packs/XX/), using MRU-ordered index caching for fast
// pack lookups.  Mirror and GC cycles run in the background without
// blocking serving.
package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/packfile"
	"golang.org/x/time/rate"
)

// Config controls the mirror server.
type Config struct {
	ListenAddr         string        // e.g. ":8080"
	RepoDir            string        // local repo root
	Stratum1URL        string        // upstream stratum-1 URL
	MirrorInterval     time.Duration // interval between mirror cycles (0 = disabled)
	GCInterval         time.Duration // interval between GC cycles (0 = disabled)
	Parallelism        int           // download parallelism for mirror
	UpstreamRateLimit  float64       // upstream proxy requests/sec (0 = no limit)
}

// Status is returned by the /.cvmfs_status.json endpoint.
type Status struct {
	State           string `json:"state"`             // "idle", "mirroring", "gc"
	LastMirror      string `json:"last_mirror"`       // ISO 8601
	LastGC          string `json:"last_gc"`           // ISO 8601
	TotalPackedObjs int64  `json:"total_packed_objs"` // across all packs
	Uptime          string `json:"uptime"`
}

// Server is a CVMFS repository HTTP server with background mirror+GC.
type Server struct {
	cfg       Config
	store     *packfile.Store
	dataDir   string
	packsDir  string
	startTime time.Time

	mu         sync.RWMutex
	state      string
	lastMirror time.Time
	lastGC     time.Time

	// cancelFunc stops the background loop.
	cancelFunc context.CancelFunc

	// httpSrv is the HTTP server (stored for graceful shutdown).
	httpSrv *http.Server

	// bgWg tracks the background goroutine so Wait() can block
	// until mirror/GC cycles finish.
	bgWg sync.WaitGroup

	// upstreamSem limits concurrent upstream proxy requests.
	upstreamSem chan struct{}

	// rateLimiter is a token-bucket rate limiter for upstream requests.
	// Used in addition to the concurrency semaphore.
	rateLimiter *rate.Limiter

	// ServeErrors counts errors returned while serving objects.
	ServeErrors int64
	// UpstreamHits counts requests proxied to the upstream stratum-1.
	UpstreamHits int64
}

// New creates a new Server. Call ListenAndServe or Serve to start.
func New(cfg Config) (*Server, error) {
	dataDir := filepath.Join(cfg.RepoDir, "data")
	packsDir := filepath.Join(cfg.RepoDir, "packs")

	// Ensure directories exist.
	for i := 0; i < 256; i++ {
		prefix := fmt.Sprintf("%02x", i)
		os.MkdirAll(filepath.Join(dataDir, prefix), 0755)
		os.MkdirAll(filepath.Join(packsDir, prefix), 0755)
	}

	store := packfile.NewStore(packsDir)
	store.RefreshAll()

	// Limit concurrent upstream proxy requests to avoid overwhelming
	// the stratum-1 when many clients request uncached objects during
	// the initial mirror.
	const maxUpstreamConcurrency = 10
	upstreamSem := make(chan struct{}, maxUpstreamConcurrency)

	var limiter *rate.Limiter
	if cfg.UpstreamRateLimit > 0 {
		burst := int(cfg.UpstreamRateLimit)
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.UpstreamRateLimit), burst)
	}

	return &Server{
		cfg:         cfg,
		store:       store,
		dataDir:     dataDir,
		packsDir:    packsDir,
		startTime:   time.Now(),
		state:       "idle",
		upstreamSem: upstreamSem,
		rateLimiter: limiter,
	}, nil
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.cvmfspublished", s.handleManifest)
	mux.HandleFunc("GET /.cvmfswhitelist", s.handleWhitelist)
	mux.HandleFunc("GET /.cvmfswhitelist.pkcs7", s.handleWhitelistPKCS7)
	mux.HandleFunc("GET /.cvmfs_last_snapshot", s.handleLastSnapshot)
	mux.HandleFunc("GET /data/", s.handleData)
	mux.HandleFunc("GET /.cvmfs_status.json", s.handleStatus)
	return mux
}

// ListenAndServe starts the HTTP server and background tasks.
func (s *Server) ListenAndServe() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelFunc = cancel

	s.bgWg.Add(1)
	go func() {
		defer s.bgWg.Done()
		s.backgroundLoop(ctx)
	}()

	srv := &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: s.Handler(),
	}
	s.httpSrv = srv

	log.Printf("CVMFS Mirror Server listening on %s", s.cfg.ListenAddr)
	return srv.ListenAndServe()
}

// Serve starts the HTTP server on an existing listener and runs
// background tasks.
func (s *Server) Serve(ln net.Listener) error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelFunc = cancel

	go s.backgroundLoop(ctx)

	srv := &http.Server{Handler: s.Handler()}
	log.Printf("CVMFS Mirror Server listening on %s", ln.Addr())
	return srv.Serve(ln)
}

// Stop cancels background tasks.
func (s *Server) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

// ShutdownHTTP gracefully shuts down the HTTP server, allowing
// in-flight requests to complete within the context deadline.
func (s *Server) ShutdownHTTP(ctx context.Context) error {
	s.Stop() // cancel the background loop
	if s.httpSrv != nil {
		return s.httpSrv.Shutdown(ctx)
	}
	return nil
}

// Wait blocks until the background loop (and any in-flight mirror/GC
// cycle) has finished.  Call after ShutdownHTTP.
func (s *Server) Wait() {
	s.bgWg.Wait()
}

// Store returns the underlying pack store (useful for testing).
func (s *Server) Store() *packfile.Store {
	return s.store
}

// handleManifest serves /.cvmfspublished.
func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	p := filepath.Join(s.cfg.RepoDir, ".cvmfspublished")
	http.ServeFile(w, r, p)
}

// handleWhitelist serves /.cvmfswhitelist.
func (s *Server) handleWhitelist(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.cfg.RepoDir, ".cvmfswhitelist"))
}

// handleWhitelistPKCS7 serves /.cvmfswhitelist.pkcs7.
func (s *Server) handleWhitelistPKCS7(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.cfg.RepoDir, ".cvmfswhitelist.pkcs7"))
}

// handleLastSnapshot serves /.cvmfs_last_snapshot.
func (s *Server) handleLastSnapshot(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.cfg.RepoDir, ".cvmfs_last_snapshot"))
}

// manifestChanged fetches the upstream .cvmfspublished and returns true
// if it differs from the local copy on disk.  Returns false on any
// error (network, missing local file on first run, etc.) so that
// transient failures don't trigger unnecessary mirrors.
func (s *Server) manifestChanged() bool {
	if s.cfg.Stratum1URL == "" {
		return false
	}

	upstreamURL := strings.TrimRight(s.cfg.Stratum1URL, "/") + "/.cvmfspublished"
	resp, err := http.Get(upstreamURL)
	if err != nil {
		log.Printf("[Server] manifest probe failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Server] manifest probe: HTTP %d", resp.StatusCode)
		return false
	}

	// Read upstream manifest (bounded — these are a few KB at most).
	upstream, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		log.Printf("[Server] manifest probe read error: %v", err)
		return false
	}

	localPath := filepath.Join(s.cfg.RepoDir, ".cvmfspublished")
	local, err := os.ReadFile(localPath)
	if err != nil {
		// No local manifest yet (first run) — treat as changed.
		return true
	}

	return string(upstream) != string(local)
}

// handleStatus serves /.cvmfs_status.json.
// If an on-disk file exists (written by mirror/GC in CVMFS-compatible
// format), serve that. Otherwise return the in-memory status.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	// Try on-disk CVMFS-compatible status first.
	statusPath := filepath.Join(s.cfg.RepoDir, ".cvmfs_status.json")
	if data, err := os.ReadFile(statusPath); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}

	// Fall back to in-memory status.
	s.mu.RLock()
	st := Status{
		State:           s.state,
		TotalPackedObjs: int64(s.store.TotalObjects()),
		Uptime:          time.Since(s.startTime).Truncate(time.Second).String(),
	}
	if !s.lastMirror.IsZero() {
		st.LastMirror = s.lastMirror.Format(time.RFC3339)
	}
	if !s.lastGC.IsZero() {
		st.LastGC = s.lastGC.Format(time.RFC3339)
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

// handleData serves /data/XX/rest[suffix].
//
// Lookup order:
//  1. Loose file: data/XX/<rest><suffix>
//  2. Pack lookup: packs/XX/ via MRU index (streamed, not buffered)
//  3. Upstream proxy: GET from stratum-1 (rate-limited)
//  4. 404
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	// URL: /data/XX/restSUFFIX
	// Strip "/data/" prefix.
	path := strings.TrimPrefix(r.URL.Path, "/data/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || len(parts[0]) != 2 {
		http.NotFound(w, r)
		return
	}
	prefix := parts[0]
	rest := parts[1]

	// 1. Try loose file — stream directly via sendfile.
	loosePath := filepath.Join(s.dataDir, prefix, rest)
	if f, err := os.Open(loosePath); err == nil {
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			http.Error(w, "stat error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
		io.Copy(w, f)
		return
	}

	// 2. Try pack lookup — stream from pack file.
	// Reconstruct full hex hash. The rest may have a suffix char appended.
	// CVMFS hashes are 40 hex chars; prefix is 2 chars, so rest should
	// be 38 chars (no suffix) or 39 chars (1-char suffix).
	fullHex := prefix + rest
	hashHex := fullHex
	if len(hashHex) == 41 {
		hashHex = hashHex[:40]
	}

	var binHash [20]byte
	validHash := len(hashHex) == 40
	if validHash {
		_, err := hex.Decode(binHash[:], []byte(hashHex))
		validHash = err == nil
	}

	if validHash {
		if s.serveFromPack(w, binHash) {
			return
		}
	}

	// 3. Upstream proxy fallback — rate-limited.
	if s.cfg.Stratum1URL != "" {
		s.serveFromUpstream(w, r, prefix, rest)
		return
	}

	// 4. Not found.
	http.NotFound(w, r)
}

// serveFromPack streams an object from pack storage into w.
// Returns true if the object was found and served.
func (s *Server) serveFromPack(w http.ResponseWriter, binHash [20]byte) bool {
	packPath, offset, length, found := s.store.Lookup(binHash)
	if !found {
		return false
	}

	or, err := packfile.OpenObject(packPath, offset, length)
	if err != nil {
		// Pack may have been deleted by GC. Refresh and retry once.
		s.store.RefreshPrefix(binHash[0])
		packPath, offset, length, found = s.store.Lookup(binHash)
		if !found {
			atomic.AddInt64(&s.ServeErrors, 1)
			return false
		}
		or, err = packfile.OpenObject(packPath, offset, length)
		if err != nil {
			atomic.AddInt64(&s.ServeErrors, 1)
			http.Error(w, "read error", http.StatusInternalServerError)
			return true // error sent, don't try upstream
		}
	}
	defer or.Close()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", or.Size()))
	io.Copy(w, or)
	return true
}

// serveFromUpstream proxies a data request to the stratum-1 server.
// Concurrency is bounded by upstreamSem so that a thundering herd of
// client requests during the initial mirror doesn't overwhelm upstream.
func (s *Server) serveFromUpstream(w http.ResponseWriter, r *http.Request, prefix, rest string) {
	// Acquire semaphore — blocks if too many concurrent upstream requests.
	select {
	case s.upstreamSem <- struct{}{}:
		defer func() { <-s.upstreamSem }()
	case <-r.Context().Done():
		// Client gave up while waiting.
		return
	}

	atomic.AddInt64(&s.UpstreamHits, 1)

	// Token-bucket rate limit (in addition to concurrency semaphore).
	if s.rateLimiter != nil {
		if err := s.rateLimiter.Wait(r.Context()); err != nil {
			return // client disconnected while rate-limited
		}
	}

	upstreamURL := strings.TrimRight(s.cfg.Stratum1URL, "/") + "/data/" + prefix + "/" + rest
	resp, err := http.Get(upstreamURL)
	if err != nil {
		atomic.AddInt64(&s.ServeErrors, 1)
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.NotFound(w, r)
		return
	}

	// Forward Content-Length if provided.
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.ContentLength))
	}
	io.Copy(w, resp.Body)
}

// backgroundLoop runs periodic mirror + GC cycles.
func (s *Server) backgroundLoop(ctx context.Context) {
	// Run an initial mirror on startup if configured.
	if s.cfg.Stratum1URL != "" && s.cfg.MirrorInterval > 0 {
		s.runMirrorCycle()
	}

	// Probe the upstream .cvmfspublished every 60 seconds.  If it
	// differs from the local copy a mirror cycle is triggered
	// immediately, so changes propagate within a minute.
	manifestTick := time.NewTicker(60 * time.Second)
	defer manifestTick.Stop()

	// MirrorInterval acts as a safety-net maximum interval; the
	// manifest probe is the primary trigger.
	mirrorTick := newOptionalTicker(s.cfg.MirrorInterval)
	gcTick := newOptionalTicker(s.cfg.GCInterval)
	refreshTick := time.NewTicker(60 * time.Second)
	defer refreshTick.Stop()
	if mirrorTick != nil {
		defer mirrorTick.Stop()
	}
	if gcTick != nil {
		defer gcTick.Stop()
	}

	mirrorC := tickerChan(mirrorTick)
	gcC := tickerChan(gcTick)

	for {
		select {
		case <-ctx.Done():
			return
		case <-manifestTick.C:
			if s.manifestChanged() {
				log.Println("[Server] Upstream manifest changed, triggering mirror")
				s.runMirrorCycle()
			}
		case <-mirrorC:
			s.runMirrorCycle()
		case <-gcC:
			s.runGCCycle()
		case <-refreshTick.C:
			s.store.RefreshAll()
		}
	}
}

// runMirrorCycle runs a single mirror-v2 cycle.
func (s *Server) runMirrorCycle() {
	s.mu.Lock()
	s.state = "mirroring"
	s.mu.Unlock()

	log.Println("[Server] Starting mirror cycle")
	// Import mirror v2 at the call site to avoid circular dependency.
	// We call it via the public RunV2 function.
	// Note: to avoid import cycles, the mirror package is used via
	// a function reference set during initialization. For simplicity
	// in this implementation, we call it directly.
	mirrorV2(s.cfg)

	s.store.RefreshAll()

	s.mu.Lock()
	s.state = "idle"
	s.lastMirror = time.Now()
	s.mu.Unlock()
	log.Println("[Server] Mirror cycle complete")
}

// runGCCycle runs a single GC cycle: catalog traversal → hashsort →
// always merge packs → delete unreachable loose files.
func (s *Server) runGCCycle() {
	s.mu.Lock()
	s.state = "gc"
	s.mu.Unlock()

	log.Println("[Server] Starting GC cycle")
	gcFunc(s.cfg)
	s.store.RefreshAll()

	s.mu.Lock()
	s.state = "idle"
	s.lastGC = time.Now()
	s.mu.Unlock()
	log.Println("[Server] GC cycle complete")
}

// gcFunc runs the GC logic.  It is a package-level variable so it can
// be replaced by the main package to avoid import cycles.
var gcFunc = func(cfg Config) {
	log.Println("[Server] GC function not configured; skipping")
}

// SetGCFunc allows the main package to inject the GC implementation,
// avoiding circular imports between server and sweep.
func SetGCFunc(fn func(Config)) {
	gcFunc = fn
}

// mirrorV2 runs a mirror-v2 cycle. This is a package-level variable
// so it can be replaced in tests.
var mirrorV2 = func(cfg Config) {
	// Default implementation calls mirror.RunV2 via a wrapper to avoid
	// import cycles. The cmd/cvmfs-mirror main.go sets this up.
	log.Println("[Server] mirror function not configured; skipping")
}

// SetMirrorFunc allows the main package to inject the mirror.RunV2
// function, avoiding circular imports.
func SetMirrorFunc(fn func(Config)) {
	mirrorV2 = fn
}

// newOptionalTicker returns a ticker or nil if d is 0.
func newOptionalTicker(d time.Duration) *time.Ticker {
	if d <= 0 {
		return nil
	}
	return time.NewTicker(d)
}

// tickerChan returns the C channel of a ticker, or a nil channel if
// the ticker is nil.
func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
