// multi.go implements a multi-repository CVMFS mirror server.
//
// Real CVMFS Stratum-1 servers host multiple independent repositories
// under a common HTTP namespace:
//
//	/cvmfs/<repo.fqdn>/.cvmfspublished
//	/cvmfs/<repo.fqdn>/.cvmfswhitelist
//	/cvmfs/<repo.fqdn>/.cvmfs_status.json
//	/cvmfs/<repo.fqdn>/.cvmfs_last_snapshot
//	/cvmfs/<repo.fqdn>/data/XX/...
//	/cvmfs/info/v1/repositories.json
//	/cvmfs/info/v1/meta.json
//
// MultiServer manages one Server per repository, demuxes HTTP requests
// by repo name, and serves the global info endpoints.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RepoConfig defines a single repository to mirror.
type RepoConfig struct {
	Name        string // FQDN, e.g. "atlas.cern.ch"
	Dir         string // local directory for this repo's data
	Stratum1URL string // upstream URL (without repo name component)
}

// MultiConfig controls the multi-repo server.
type MultiConfig struct {
	ListenAddr        string        // e.g. ":8080"
	Repos             []RepoConfig  // one per repository
	MirrorInterval    time.Duration // default mirror interval
	GCInterval        time.Duration // default GC interval
	Parallelism       int           // download parallelism per repo
	UpstreamRateLimit float64       // upstream rate limit per repo
	DrainTimeout      time.Duration // drain timeout for mirror cycles
}

// MultiServer manages multiple independent CVMFS repository mirrors
// behind a single HTTP endpoint.
type MultiServer struct {
	cfg       MultiConfig
	repos     map[string]*Server // keyed by repo name (FQDN)
	httpSrv   *http.Server
	startTime time.Time

	mu         sync.RWMutex
	cancelFunc context.CancelFunc
	bgWg       sync.WaitGroup
}

// NewMulti creates a MultiServer from the given config.
func NewMulti(cfg MultiConfig) (*MultiServer, error) {
	ms := &MultiServer{
		cfg:       cfg,
		repos:     make(map[string]*Server),
		startTime: time.Now(),
	}

	for _, rc := range cfg.Repos {
		srvCfg := Config{
			RepoDir:           rc.Dir,
			Stratum1URL:       rc.Stratum1URL,
			MirrorInterval:    cfg.MirrorInterval,
			GCInterval:        cfg.GCInterval,
			Parallelism:       cfg.Parallelism,
			UpstreamRateLimit: cfg.UpstreamRateLimit,
		}
		srv, err := New(srvCfg)
		if err != nil {
			return nil, fmt.Errorf("creating server for %s: %w", rc.Name, err)
		}
		ms.repos[rc.Name] = srv
		log.Printf("[Multi] Registered repo %s -> %s (upstream: %s)", rc.Name, rc.Dir, rc.Stratum1URL)
	}

	return ms, nil
}

// ListenAndServe starts the HTTP server and background tasks for all repos.
func (ms *MultiServer) ListenAndServe(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	ms.cancelFunc = cancel

	// Start background loops for each repo.
	for name, srv := range ms.repos {
		bgCtx, bgCancel := context.WithCancel(ctx)
		_ = bgCancel // cancelled via parent ctx
		ms.bgWg.Add(1)
		go func(s *Server, n string, c context.Context) {
			defer ms.bgWg.Done()
			s.backgroundLoop(c)
		}(srv, name, bgCtx)
	}

	httpSrv := &http.Server{
		Addr:    ms.cfg.ListenAddr,
		Handler: ms.Handler(),
	}
	ms.httpSrv = httpSrv

	log.Printf("[Multi] CVMFS Mirror Server listening on %s (%d repos)", ms.cfg.ListenAddr, len(ms.repos))
	return httpSrv.ListenAndServe()
}

// ShutdownHTTP gracefully stops the HTTP server and cancels background tasks.
func (ms *MultiServer) ShutdownHTTP(ctx context.Context) error {
	if ms.cancelFunc != nil {
		ms.cancelFunc()
	}
	if ms.httpSrv != nil {
		return ms.httpSrv.Shutdown(ctx)
	}
	return nil
}

// Wait blocks until all background loops finish.
func (ms *MultiServer) Wait() {
	ms.bgWg.Wait()
}

// Repos returns the map of repo name → Server (for wiring mirror/GC funcs).
func (ms *MultiServer) Repos() map[string]*Server {
	return ms.repos
}

// Handler returns the HTTP handler that routes by repo name.
func (ms *MultiServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// Global info endpoints.
	mux.HandleFunc("GET /cvmfs/info/v1/repositories.json", ms.handleRepositoriesJSON)
	mux.HandleFunc("GET /cvmfs/info/v1/meta.json", ms.handleMetaJSON)

	// Per-repo routing: /cvmfs/<repo>/...
	mux.HandleFunc("/cvmfs/", ms.handleRepoRequest)

	return mux
}

// handleRepositoriesJSON serves /cvmfs/info/v1/repositories.json
// in the standard CVMFS format.
func (ms *MultiServer) handleRepositoriesJSON(w http.ResponseWriter, r *http.Request) {
	type repoEntry struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	type repoList struct {
		Schema       int         `json:"schema"`
		Repositories []repoEntry `json:"repositories,omitempty"`
		Replicas     []repoEntry `json:"replicas"`
	}

	replicas := make([]repoEntry, 0, len(ms.repos))
	for name := range ms.repos {
		replicas = append(replicas, repoEntry{
			Name: name,
			URL:  "/cvmfs/" + name,
		})
	}

	resp := repoList{
		Schema:   1,
		Replicas: replicas,
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(resp)
}

// handleMetaJSON serves /cvmfs/info/v1/meta.json with basic server info.
func (ms *MultiServer) handleMetaJSON(w http.ResponseWriter, r *http.Request) {
	meta := map[string]interface{}{
		"administrator": "",
		"email":         "",
		"organisation":  "",
		"custom": map[string]string{
			"_comment": "cvmfs-mirror-go",
		},
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(meta)
}

// handleRepoRequest routes /cvmfs/<repo>/... to the appropriate per-repo Server.
func (ms *MultiServer) handleRepoRequest(w http.ResponseWriter, r *http.Request) {
	// Strip /cvmfs/ prefix, extract repo name.
	path := strings.TrimPrefix(r.URL.Path, "/cvmfs/")
	slashIdx := strings.IndexByte(path, '/')
	if slashIdx < 0 {
		http.NotFound(w, r)
		return
	}
	repoName := path[:slashIdx]
	remainder := path[slashIdx:] // includes leading '/'

	srv, ok := ms.repos[repoName]
	if !ok {
		http.NotFound(w, r)
		return
	}

	// Route based on the remainder path.
	switch {
	case remainder == "/.cvmfspublished":
		srv.handleManifest(w, r)
	case remainder == "/.cvmfswhitelist":
		ms.serveRepoFile(w, r, srv.cfg.RepoDir, ".cvmfswhitelist")
	case remainder == "/.cvmfswhitelist.pkcs7":
		ms.serveRepoFile(w, r, srv.cfg.RepoDir, ".cvmfswhitelist.pkcs7")
	case remainder == "/.cvmfs_status.json":
		ms.handleRepoStatus(w, r, srv)
	case remainder == "/.cvmfs_last_snapshot":
		ms.serveRepoFile(w, r, srv.cfg.RepoDir, ".cvmfs_last_snapshot")
	case strings.HasPrefix(remainder, "/data/"):
		// Rewrite r.URL.Path so handleData sees /data/...
		r.URL.Path = remainder
		srv.handleData(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleRepoStatus serves /.cvmfs_status.json for a repo.
// Uses the on-disk file (written by mirror/GC) in the CVMFS-compatible
// format, falling back to the in-memory status.
func (ms *MultiServer) handleRepoStatus(w http.ResponseWriter, r *http.Request, srv *Server) {
	statusPath := filepath.Join(srv.cfg.RepoDir, ".cvmfs_status.json")
	if data, err := os.ReadFile(statusPath); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}
	// Fall back to in-memory status.
	srv.handleStatus(w, r)
}

// serveRepoFile serves a static file from the repo directory.
func (ms *MultiServer) serveRepoFile(w http.ResponseWriter, r *http.Request, repoDir, filename string) {
	http.ServeFile(w, r, filepath.Join(repoDir, filename))
}
