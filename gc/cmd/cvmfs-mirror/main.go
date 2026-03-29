package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/mirror"
	"github.com/bbockelm/cvmfs-gc-optim/gc/server"
	"github.com/bbockelm/cvmfs-gc-optim/gc/sweep"
)

// configFile is the JSON format for multi-repo configuration.
type configFile struct {
	Listen         string       `json:"listen"`
	MirrorInterval string       `json:"mirror_interval"` // Go duration string
	GCInterval     string       `json:"gc_interval"`
	Parallelism    int          `json:"parallelism"`
	UpstreamRate   float64      `json:"upstream_rate"`
	DrainTimeout   string       `json:"drain_timeout"`
	Repos          []configRepo `json:"repos"`
}

type configRepo struct {
	Name string `json:"name"` // FQDN, e.g. "atlas.cern.ch"
	URL  string `json:"url"`  // upstream stratum-1 base URL
	Dir  string `json:"dir"`  // local directory
}

func main() {
	url := flag.String("url", "", "Stratum-1 base URL (single-repo mode)")
	dir := flag.String("dir", "", "Local repository directory (single-repo mode)")
	listen := flag.String("listen", ":8080", "HTTP listen address")
	mirrorInterval := flag.Duration("mirror-interval", 15*time.Minute, "Mirror cycle interval (0 to disable)")
	gcInterval := flag.Duration("gc-interval", 1*time.Hour, "GC cycle interval (0 to disable)")
	parallelism := flag.Int("parallelism", 8, "Download parallelism")
	once := flag.Bool("once", false, "Run a single mirror cycle and exit (no server)")
	upstreamRate := flag.Float64("upstream-rate", 0, "Upstream proxy rate limit (requests/sec, 0=unlimited)")
	drainTimeout := flag.Duration("drain-timeout", 60*time.Second, "Grace period after ^C to keep downloading before discarding queued work")
	configPath := flag.String("config", "", "Path to JSON config file for multi-repo mode")
	flag.Parse()

	// -once mode: single mirror cycle, no server.
	if *once {
		if *url == "" || *dir == "" {
			fmt.Fprintln(os.Stderr, "error: -url and -dir are required with -once")
			flag.Usage()
			os.Exit(1)
		}
		runOnce(*url, *dir, *parallelism, *drainTimeout)
		return
	}

	// Multi-repo mode (config file) or single-repo mode (flags).
	if *configPath != "" {
		runMultiServer(*configPath, *listen, *mirrorInterval, *gcInterval,
			*parallelism, *upstreamRate, *drainTimeout)
	} else {
		if *url == "" || *dir == "" {
			fmt.Fprintln(os.Stderr, "error: -url and -dir are required (or use -config for multi-repo)")
			flag.Usage()
			os.Exit(1)
		}
		runSingleServer(*url, *dir, *listen, *mirrorInterval, *gcInterval,
			*parallelism, *upstreamRate, *drainTimeout)
	}
}

// runOnce runs a single mirror-v2 cycle and exits.
func runOnce(baseURL, localDir string, parallelism int, drainTimeout time.Duration) {
	log.Printf("Running single mirror-v2 cycle: %s -> %s", baseURL, localDir)

	log.Printf("Installing signal handler for SIGINT/SIGTERM (pid=%d)", os.Getpid())
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Printf("Signal received (ctx.Err=%v) — draining for up to %s, then flushing packs to disk...",
			ctx.Err(), drainTimeout)
		signal.Ignore(os.Interrupt, syscall.SIGTERM)
	}()

	cfg := mirror.V2Config{
		BaseURL:      baseURL,
		LocalDir:     localDir,
		Parallelism:  parallelism,
		Ctx:          ctx,
		DrainTimeout: drainTimeout,
	}
	stats, err := mirror.RunV2(cfg)
	if err != nil {
		log.Fatalf("mirror failed: %v", err)
	}
	if ctx.Err() != nil {
		log.Println("Exiting (interrupted). Re-run to resume.")
	} else {
		log.Printf("Mirror complete: %d catalogs, %d loose + %d packed, %d skipped, %d failed",
			stats.CatalogsProcessed, stats.DownloadedLoose, stats.DownloadedPacked,
			stats.HashesSkipped, stats.DownloadFailed)
	}
}

// runSingleServer runs the legacy single-repo server mode.
func runSingleServer(baseURL, localDir, listenAddr string,
	mirrorInt, gcInt time.Duration, parallelism int,
	upstreamRate float64, drainTimeout time.Duration) {

	log.Printf("Installing signal handler for SIGINT/SIGTERM (pid=%d)", os.Getpid())
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	srvCfg := server.Config{
		ListenAddr:        listenAddr,
		RepoDir:           localDir,
		Stratum1URL:       baseURL,
		MirrorInterval:    mirrorInt,
		GCInterval:        gcInt,
		Parallelism:       parallelism,
		UpstreamRateLimit: upstreamRate,
	}

	server.SetMirrorFunc(func(cfg server.Config) {
		mirrorCfg := mirror.V2Config{
			BaseURL:      cfg.Stratum1URL,
			LocalDir:     cfg.RepoDir,
			Parallelism:  cfg.Parallelism,
			Ctx:          ctx,
			DrainTimeout: drainTimeout,
		}
		if _, err := mirror.RunV2(mirrorCfg); err != nil {
			log.Printf("mirror cycle error: %v", err)
		}
	})

	server.SetGCFunc(func(cfg server.Config) {
		gcCfg := sweep.GCConfig{
			RepoDir:     cfg.RepoDir,
			Parallelism: cfg.Parallelism,
		}
		if _, err := sweep.RunGCCycle(gcCfg); err != nil {
			log.Printf("GC cycle error: %v", err)
		}
	})

	srv, err := server.New(srvCfg)
	if err != nil {
		log.Fatalf("creating server: %v", err)
	}

	log.Printf("Starting CVMFS mirror server: %s -> %s (listen=%s mirror=%s gc=%s)",
		baseURL, localDir, listenAddr, mirrorInt, gcInt)

	go func() {
		<-ctx.Done()
		signal.Ignore(os.Interrupt, syscall.SIGTERM)
		log.Printf("Signal received — draining for up to %s, then shutting down server...", drainTimeout)
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = srv.ShutdownHTTP(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}

	log.Println("Server stopped. Waiting for in-flight mirror/GC to finish...")
	srv.Wait()
	log.Println("Shutdown complete.")
}

// runMultiServer reads a JSON config file and starts a multi-repo server.
func runMultiServer(configPath, listenAddr string,
	defaultMirrorInt, defaultGCInt time.Duration,
	defaultParallelism int, defaultUpstreamRate float64,
	defaultDrainTimeout time.Duration) {

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("reading config file: %v", err)
	}
	var cfg configFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("parsing config file: %v", err)
	}

	// Apply config-file overrides, falling back to CLI flags.
	if cfg.Listen != "" {
		listenAddr = cfg.Listen
	}
	if cfg.Parallelism > 0 {
		defaultParallelism = cfg.Parallelism
	}
	if cfg.UpstreamRate > 0 {
		defaultUpstreamRate = cfg.UpstreamRate
	}
	if cfg.MirrorInterval != "" {
		if d, err := time.ParseDuration(cfg.MirrorInterval); err == nil {
			defaultMirrorInt = d
		}
	}
	if cfg.GCInterval != "" {
		if d, err := time.ParseDuration(cfg.GCInterval); err == nil {
			defaultGCInt = d
		}
	}
	if cfg.DrainTimeout != "" {
		if d, err := time.ParseDuration(cfg.DrainTimeout); err == nil {
			defaultDrainTimeout = d
		}
	}

	if len(cfg.Repos) == 0 {
		log.Fatal("config file contains no repos")
	}

	repos := make([]server.RepoConfig, len(cfg.Repos))
	for i, r := range cfg.Repos {
		if r.Name == "" || r.URL == "" || r.Dir == "" {
			log.Fatalf("repo #%d: name, url, and dir are all required", i+1)
		}
		repos[i] = server.RepoConfig{
			Name:        r.Name,
			Dir:         r.Dir,
			Stratum1URL: r.URL,
		}
	}

	multiCfg := server.MultiConfig{
		ListenAddr:        listenAddr,
		Repos:             repos,
		MirrorInterval:    defaultMirrorInt,
		GCInterval:        defaultGCInt,
		Parallelism:       defaultParallelism,
		UpstreamRateLimit: defaultUpstreamRate,
		DrainTimeout:      defaultDrainTimeout,
	}

	log.Printf("Installing signal handler for SIGINT/SIGTERM (pid=%d)", os.Getpid())
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Wire mirror and GC functions (global; dispatch by config).
	server.SetMirrorFunc(func(cfg server.Config) {
		mirrorCfg := mirror.V2Config{
			BaseURL:      cfg.Stratum1URL,
			LocalDir:     cfg.RepoDir,
			Parallelism:  cfg.Parallelism,
			Ctx:          ctx,
			DrainTimeout: defaultDrainTimeout,
		}
		if _, err := mirror.RunV2(mirrorCfg); err != nil {
			log.Printf("mirror cycle error: %v", err)
		}
	})

	server.SetGCFunc(func(cfg server.Config) {
		gcCfg := sweep.GCConfig{
			RepoDir:     cfg.RepoDir,
			Parallelism: cfg.Parallelism,
		}
		if _, err := sweep.RunGCCycle(gcCfg); err != nil {
			log.Printf("GC cycle error: %v", err)
		}
	})

	ms, err := server.NewMulti(multiCfg)
	if err != nil {
		log.Fatalf("creating multi-server: %v", err)
	}

	go func() {
		<-ctx.Done()
		signal.Ignore(os.Interrupt, syscall.SIGTERM)
		log.Printf("Signal received — draining for up to %s, then shutting down server...", defaultDrainTimeout)
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = ms.ShutdownHTTP(shutCtx)
	}()

	if err := ms.ListenAndServe(ctx); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}

	log.Println("Server stopped. Waiting for in-flight mirror/GC to finish...")
	ms.Wait()
	log.Println("Shutdown complete.")
}
