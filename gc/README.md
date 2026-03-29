# cvmfs-gc — Optimized CVMFS Garbage Collector

A high-performance garbage collector for
[CVMFS](https://cernvm.cern.ch/fs/) Stratum-1 servers, designed to handle
repositories with hundreds of millions of content-addressable objects.

## How it works

The built-in CVMFS GC holds all reachable hashes in memory and then
walks every object in the data directory.  That approach falls over on
very large repositories.  This tool replaces it with a three-phase
external-sort pipeline that keeps memory usage bounded:

1. **Catalog traversal + semi-sort** — A parallel worker pool walks the
   catalog tree (nested SQLite databases).  Discovered content hashes are
   streamed into a heap-based semi-sorter that flushes sorted runs to
   disk when memory pressure is reached.

2. **Chunk sort** — The semi-sorted runs are concatenated and split into
   ~100 MB chunks.  Each chunk is sorted and deduplicated in memory, then
   written to disk.

3. **Sweep** — The 256 hash-prefix directories (`00`–`ff`) are scanned
   sequentially.  Directory entries are merge-joined against a streaming
   k-way merge of the sorted chunks.  Any object not present in the
   merge stream is unreachable and deleted.

ReadDir I/O is overlapped with the merge-join via a one-ahead goroutine,
and `os.DirEntry.Info()` is deferred until after a successful delete to
avoid unnecessary stat syscalls.

## Building

```bash
# Native build (both cvmfs-gc and cvmfs-mirror)
make

# Cross-compile for Linux x86-64
make linux

# Run tests
make test
```

### Cross-compilation

SQLite is provided by [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a pure-Go translation of SQLite with no cgo dependency.  This means
cross-compilation requires nothing beyond the Go toolchain:

```bash
make linux
scp cvmfs-gc-linux-amd64 cvmfs-mirror-linux-amd64 stratum1:/usr/local/bin/
```

The resulting binaries are statically linked and can be copied directly to
a Stratum-1 server with no runtime dependencies.

## Usage

### Garbage collection

```bash
# List unreachable objects (default — nothing is deleted)
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org

# Actually delete unreachable objects
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org -delete
```

The tool automatically reads `.cvmfspublished` from the repo root to
discover the root catalog hash plus any history, certificate, and
metainfo objects.  You can also supply these explicitly:

```bash
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org \
         -manifest /path/to/.cvmfspublished \
         -root-hash abc123...
```

### Mirror server

`cvmfs-mirror` is a long-running HTTP server that maintains a local
pack-file-based mirror of a CVMFS repository.  It serves objects from
local storage, falling back to the upstream Stratum-1 for cache misses,
and runs periodic mirror + GC cycles in the background.

```bash
cvmfs-mirror \
    -url http://stratum1.example.org/cvmfs/repo.example.org \
    -dir /srv/cvmfs/myrepo.example.org \
    -listen :8080 \
    -mirror-interval 15m \
    -gc-interval 1h
```

Key features:

- **Pack-file storage** — small objects (< 256 KB) are packed into large
  files with mmap-backed indices, reducing inode pressure and improving
  I/O patterns.
- **Upstream fallback** — cache misses are proxied from the upstream
  Stratum-1, with configurable concurrency and rate limiting.
- **Background mirroring** — a full catalog-traversal mirror runs on a
  configurable interval, streaming new objects into local packs.
- **Background GC** — unreachable objects are identified via sorted-hash
  join and removed by compacting pack files.
- **Status endpoint** — `GET /.cvmfs_status.json` returns server state,
  last mirror/GC timestamps, and object counts.

#### One-shot mirror

To run a single mirror cycle without starting the HTTP server:

```bash
cvmfs-mirror -once \
    -url http://stratum1.example.org/cvmfs/repo.example.org \
    -dir /tmp/test-mirror
```

Then run GC against the mirror:

```bash
cvmfs-gc -repo /tmp/test-mirror
```

### cvmfs-gc flags

| Flag | Default | Description |
|------|---------|-------------|
| `-repo` | | Path to the local CVMFS repository |
| `-root-hash` | | Root catalog hash (auto-detected from manifest) |
| `-manifest` | | Path to `.cvmfspublished` (defaults to `<repo>/.cvmfspublished`) |
| `-parallelism` | 8 | Number of parallel catalog-traversal workers |
| `-heap-mb` | 100 | Memory budget (MB) for the semi-sort heap |
| `-chunk-mb` | 100 | Chunk size (MB) for the chunk-sort phase |
| `-delete` | false | Actually delete unreachable files (default is list only) |
| `-temp-dir` | | Temp directory for intermediate files (auto-created if omitted) |
| `-mirror` | false | Mirror mode: download a repo from a Stratum-1 |
| `-stratum1-url` | | Base URL of the Stratum-1 (mirror mode only) |
| `-mirror-jobs` | 8 | Parallel downloads for mirroring |

### cvmfs-mirror flags

| Flag | Default | Description |
|------|---------|-------------|
| `-url` | | Upstream Stratum-1 base URL (required) |
| `-dir` | | Local repository directory (required) |
| `-listen` | `:8080` | HTTP listen address |
| `-mirror-interval` | `15m` | Interval between mirror cycles (`0` to disable) |
| `-gc-interval` | `1h` | Interval between GC cycles (`0` to disable) |
| `-parallelism` | `8` | Download parallelism for mirroring |
| `-upstream-rate` | `0` | Upstream request rate limit (req/s, `0` = unlimited) |
| `-once` | `false` | Run a single mirror cycle and exit (no server) |

## Project layout

```
cmd/cvmfs-gc/       CLI entry point, manifest parsing, three-phase GC pipeline
cmd/cvmfs-mirror/   Mirror server CLI, wires mirror + GC into the HTTP server
catalog/            Parallel CVMFS catalog-tree traversal, hash types, zlib decompression
hashsort/           Semi-sort, chunk sort, k-way streaming merge reader
sweep/              Directory sweep, pack compaction, GC cycle orchestration
mirror/             Catalog-traversal mirror with pack-file storage
packfile/           Pack-file writer, mmap-backed index, 256-way prefix lookup
server/             HTTP server with upstream fallback, background mirror + GC
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
