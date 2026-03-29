# Mirror V2: Packfile-Based CVMFS Mirror

## Overview

Mirror V2 replaces the current loose-object mirror (`mirror/mirror.go`) with a
**git-packfile-inspired** storage layer.  Small objects (<256 KB) are
concatenated into ~16 MB pack files with a binary index; large objects stay as
loose files in `data/XX`.  A built-in HTTP server exposes the repository and
runs mirror+GC automatically.  GC rewrites packs to reclaim space.

The design reuses the 256-way prefix partitioning already proven in the GC
pipeline: every data structure is sharded by the first byte of the SHA-1 hash
so that 256 goroutines can work on disjoint state with no contention.

---

## 1  On-disk Layout

```
<repo>/
  .cvmfspublished          # latest manifest
  data/
    00/ … ff/              # loose objects (≥ 256 KB, or legacy)
  packs/
    00/ … ff/              # per-prefix pack dirs (new)
      <contenthash>.pack   # pack data file
      <contenthash>.idx    # pack index (git v2 format)
      pack.tmp             # open pack being written (mirror phase)
      pack.tmp.idx.wip     # in-memory; not persisted until finalized
```

Each `packs/XX/` directory is the counterpart of `data/XX/`.  Objects whose
first hash byte is `XX` live in packs under `packs/XX/`.

### 1.1  Pack Data File (`<contenthash>.pack`)

A pack is a flat concatenation of raw object blobs.  Unlike git, we do **not**
delta-compress objects and do **not** use the git pack header/trailer:

```
┌─────────────────────┐  offset 0
│  object 0  (raw)    │
├─────────────────────┤
│  object 1  (raw)    │
├─────────────────────┤
│  …                  │
└─────────────────────┘
```

There is no per-object header — the index stores the offset and length.  We
omit the git pack header (`PACK` magic + version + count) because we never
delta-encode and the index already records the total number of objects.  This
keeps the pack trivially appendable during mirroring (just `write(blob)`).

**Content hash.**  As objects are appended, the writer maintains a running
SHA-256 of the entire pack byte stream.  When the pack is finalised
(`pack.tmp` → `<hash>.pack`), the final SHA-256 digest becomes the filename.
SHA-256 is used for the *pack identity* (not for CVMFS object identity, which
is SHA-1).

### 1.2  Pack Index File (`<contenthash>.idx`)

We use git index **version 2** format, adapted for our use-case:

```
 4 bytes   magic: 0xff 0x74 0x4f 0x63  ("\377tOc")
 4 bytes   version: 2  (network order)
 256×4     fan-out table F[0..255]
           F[i] = number of objects with 2nd hash byte ≤ i
           (the 1st byte is already implicit — all objects in
            packs/XX/ share the same first byte XX)
 N×20      sorted SHA-1 names (20 bytes each, N = F[255])
 N×4       CRC-32 of each packed object (network order)
 N×4       4-byte offsets (network order, MSB flags 8-byte table)
 M×8       8-byte offsets (only if any offset ≥ 2^31)
 20 bytes  SHA-1 of the pack file
 20 bytes  SHA-1 checksum of the index (all of the above)
```

**Fan-out key.**  In standard git the fan-out uses the *first* byte of the
object name.  Since all objects in `packs/XX/` share byte 0 == `XX`, we use
**byte 1** (the second byte) of the hash as the fan-out key.  This preserves
the O(1) fan-out lookup.

**Object length.**  Git stores only offsets (lengths are inferred from the next
object's offset).  We do the same — pack objects are laid out contiguously and
the last object extends to EOF (or to the recorded pack SHA-1 trailer, if we
decide to add one — see §1.1).  During reading, `len = offset[i+1] − offset[i]`
(or `len = filesize − offset[i]` for the last object).

### 1.3  Size Thresholds

| Constant | Value | Purpose |
|----------|-------|---------|
| `LooseThreshold` | 256 KB | Objects ≥ this go to `data/XX/` as loose files |
| `PackTargetSize` | 16 MB | When `pack.tmp` exceeds this, finalize + rotate |
| `MinReclaimPct` | 5 % | GC only compacts if reclaimable ratio exceeds this |
| `MaxGCConcurrency` | 10 | At most 10 prefix dirs being GC'd concurrently |

---

## 2  Mirror Phase

### 2.1  Catalog Discovery (same pattern as GC Phase 1)

1. Fetch `.cvmfspublished`.  Parse `C` (root hash), `H` (history), `X`, `M`.
2. Download the history database (`H` hash with suffix `H`).  Call
   `catalog.ReadTaggedRoots()` to enumerate all tagged root catalog hashes.
3. Spawn one `catalog.TraverseFromRootHash()` per tree (current root + all
   tags).  Each traversal uses its own private `chan catalog.Hash`.  A shared
   `catalog.Progress` with `LoadOrStore` deduplicates catalogs across trees
   (identical to GC Phase 1).

### 2.2  256-Way Prefix Pipeline

A merger goroutine collects hashes from all traversal channels and routes
each hash to one of 256 **prefix workers** (indexed by `hash[0]`):

```
 traversals ──► merger ──► prefixCh[0x00]  →  prefixWorker(0x00)
                         ├ prefixCh[0x01]  →  prefixWorker(0x01)
                         ├ …
                         └ prefixCh[0xff]  →  prefixWorker(0xff)
```

Each prefix worker uses the same min-heap + eviction strategy as
`hashsort.PrefixWrite` to deduplicate:

* maintain a ~1 MB min-heap of `hashsort.Entry` values
* on eviction (or final flush), the entry is guaranteed unique within
  the stream (consecutive-duplicate check via `lastWritten`)

The evicted/flushed entries are the *download work items*.

### 2.3  Download + Pack Logic (per prefix worker)

For each unique hash emitted from the heap:

```
1. already_exists?(hash, suffix)
   → Check data/XX/<rest><suffix>  (loose file)
   → Check all .idx files in packs/XX/  (packed, see §4 Lookup)
   If found → skip (count as Skipped)

2. download from stratum-1
   → GET <baseURL>/data/<XX>/<rest><suffix>
   If HTTP error → count as Failed, continue

3. if len(blob) >= LooseThreshold
   → write to data/XX/<rest><suffix>  (loose file)
   → count as DownloadedLoose

4. else  (small object)
   → append blob to packs/XX/pack.tmp at current offset
   → record in in-memory map:  hash+suffix → {offset, length}
   → update running SHA-256 with blob bytes
   → count as DownloadedPacked

5. if pack.tmp size ≥ PackTargetSize
   → finalize pack (see §2.4)
```

### 2.4  Pack Finalisation

When `pack.tmp` reaches 16 MB (or when the prefix worker finishes):

1. Close `pack.tmp` for writing.
2. Compute the final pack content hash (from the running SHA-256).
3. Build the git v2 index file from the in-memory map (see §3).
4. Rename:
   ```
   pack.tmp  →  <hash>.pack
   ```
5. Write `<hash>.idx` atomically (write to `.idx.tmp`, then rename).
6. Clear the in-memory map; open a new `pack.tmp` if more work remains.

### 2.5  Statistics

```go
type MirrorStats struct {
    CatalogsProcessed  int64
    CatalogsSkipped    int64   // cross-tree dedup
    HashesDiscovered   int64
    HashesSkipped      int64   // already present
    DownloadedLoose    int64   // ≥ 256 KB → data/
    DownloadedPacked   int64   // < 256 KB → pack
    PacksFinalized     int64
    DownloadFailed     int64
    TotalBytes         int64   // total bytes downloaded
}
```

---

## 3  Index Construction

### 3.1  Building an Index from the In-Memory Map

After accumulating entries in the map (`hash → {offset, length}`):

1. Collect all entries into a slice and sort by the full 20-byte hash.
2. Build the fan-out table: `F[i] = count of entries where byte 1 ≤ i`.
3. Write the five sections of the v2 index:
   - Magic + version
   - Fan-out (256 × 4 bytes)
   - Sorted hash names (N × 20 bytes)
   - CRC-32 values (N × 4 bytes) — computed during append to pack.tmp
   - 4-byte offsets (N × 4 bytes), with MSB set → 8-byte table entry
   - 8-byte offsets (only if pack > 2 GB — unlikely for 16 MB packs)
   - Pack SHA-1 (we'll store the pack SHA-256 truncated to 20 bytes,
     or alternatively compute a SHA-1 of the pack; TBD — sha-256 of
     pack can be stored separately since git assumes SHA-1 here)
   - Index checksum (SHA-1 of all preceding bytes)

### 3.2  Reading an Index (Lookup)

To check whether a hash exists in a pack:

```
1. Read bytes [1] of the target hash (= b).
2. From the fan-out:  lo = F[b-1] (0 if b==0),  hi = F[b].
3. Binary search the sorted-names table [lo, hi) for the 20-byte hash.
4. If found at position i:
   a. Read offset[i] from the offset table.
   b. If MSB set, use 8-byte offset table.
   c. Length = offset[i+1] − offset[i]  (or packsize − offset[i]).
5. Seek in the pack file and read `length` bytes.
```

### 3.3  Index Caching

Each prefix worker keeps a slice of loaded index files, **ordered MRU**
(most-recently-used first).  When checking existence:

```
for each idx in mruList:
    if idx.Contains(hash):
        move idx to front
        return true
return false
```

This exploits temporal locality: recently-added packs contain the most
recently published catalog objects, which are the ones most likely to be
queried again by neighboring catalog entries.

---

## 4  Garbage Collection (Pack Compaction)

### 4.1  Goal

GC removes unreachable objects from packs (and loose files).  The reachable
set is determined exactly as in the existing GC pipeline (catalog traversal →
256-way prefix sort → merge-join).

### 4.2  Algorithm

For each prefix `XX` (at most `MaxGCConcurrency` = 10 in parallel):

```
Phase A: Compute reachable set for prefix XX
  (from the existing GC chunk files: chunk-XX contains all reachable
   hashes for this prefix, sorted and deduplicated)

Phase B: Count reclaimable volume
  For every pack index in packs/XX/:
    for each entry in the index:
      if entry NOT in reachable set:
        reclaimable += entry.length
  For every loose file in data/XX/:
    if file NOT in reachable set:
      reclaimable += file.size
  Compute totalVolume = sum of all pack sizes + loose file sizes.

Phase C: Decide whether to compact
  ratio = reclaimable / totalVolume
  if ratio < MinReclaimPct (5%):
    log "prefix XX: %.1f%% reclaimable, skipping"
    continue

Phase D: Compact
  1. Lock the repository (if not already locked).
  2. K-way merge sort of all packs + loose objects:
     - Open a ChunkReader on the reachable chunk file.
     - Open each pack index as a sorted iterator.
     - Scan loose files in sorted order.
     - Merge these N+1 sorted streams.
     - For each hash present in BOTH the reachable set AND at least
       one source (pack or loose):
       → copy the object bytes to a new pack.tmp
       → if object ≥ LooseThreshold, write as loose instead
     - This produces new, compacted packs + loose files with zero
       dead objects.
  3. Atomically replace old packs:
     - Finalise the new pack (rename pack.tmp → <hash>.pack, write
       <hash>.idx).
     - Delete old .pack + .idx files.
     - Delete loose files that were folded into the new pack.
```

### 4.3  K-Way Merge Detail

Sources to merge for prefix `XX`:

| Source | Iterator | Key |
|--------|----------|-----|
| Reachable set | `hashsort.ChunkReader` on `chunk-XX` | 21-byte Entry |
| Pack 0 | index-sorted scan of `packs/XX/aaa.idx` | 20-byte hash |
| Pack 1 | index-sorted scan of `packs/XX/bbb.idx` | 20-byte hash |
| … | | |
| Loose files | `os.ReadDir("data/XX")` sorted | hex filename |

The merge produces a stream of `(hash, source)` tuples in sorted order.
For each hash:

- If it appears in the reachable set: **keep** — copy from the best
  source (prefer pack over loose to avoid re-downloading).
- If it does not appear in the reachable set: **discard**.

Objects kept are written to a new pack (or as loose files if ≥ 256 KB).
After all sources are exhausted, the new pack is finalised and old
packs + unreachable loose files are deleted.

### 4.4  Estimation Before Locking

The volume estimation (Phase B) runs **without** the repository lock.
Only if the reclaimable ratio exceeds 5% do we acquire the lock and
perform the actual compaction (Phase D).  This mirrors the two-phase
approach of the existing GC: scan unlocked, delete locked.

Between estimation and compaction, a new publish may have landed.  We
handle this the same way the existing GC does: re-read the manifest,
delta-traverse new catalogs, and protect any newly-reachable hashes.

---

## 5  Web Server

### 5.1  Endpoints

```
GET /.cvmfspublished                → serve manifest file
GET /data/<XX>/<rest>[<suffix>]     → serve object from loose or pack
GET /.cvmfs_status.json             → JSON status (mirror progress, GC stats)
```

### 5.2  Object Serving

When a request arrives for `/data/XX/restSUFFIX`:

```
1. Reconstruct full hash = XX + rest (strip suffix for lookup).
2. Check data/XX/<rest><suffix>  (loose file):
   → if exists, serve with sendfile / io.Copy
3. Check active index files for packs/XX/:
   → for each idx (MRU order):
       if idx.Contains(hash):
         open pack, seek to offset, serve `length` bytes
         return
4. 404 Not Found
```

### 5.3  Concurrency with GC/Mirror

The server must keep serving while GC rewrites packs and mirror adds new
ones.  The key invariant is:

> **Index files are immutable once written.**  GC creates new packs+indices,
> then atomically deletes old ones.  The server holds a snapshot of known
> index files per prefix and refreshes periodically.

Implementation:

```go
type PrefixState struct {
    mu       sync.RWMutex
    indices  []*PackIndex   // currently active indices, MRU order
}

// Refresh rescans packs/XX/ and loads any new .idx files.
func (ps *PrefixState) Refresh() { … }

// Lookup checks all active indices for the given hash.
func (ps *PrefixState) Lookup(hash [20]byte) (packPath string, offset, length int64, found bool) { … }
```

The server holds `[256]PrefixState`.  Each `PrefixState` is refreshed:
- After a mirror cycle completes (new packs may have been added).
- After a GC cycle completes (old packs removed, new ones created).
- Periodically (every 60s) as a fallback.

GC ensures old pack files are not deleted until the new ones are visible:
1. Write new `<hash>.pack` and `<hash>.idx`.
2. Call `PrefixState.Refresh()` — new indices are added.
3. Delete old `.pack` and `.idx` files.

Any in-flight reads from old packs may get an error (file deleted); the
server retries from the refreshed index list.

### 5.4  Automatic Mirror + GC

The server runs mirror and GC on a configurable schedule:

```go
type ServerConfig struct {
    ListenAddr     string        // e.g. ":8080"
    RepoDir        string        // local repo root
    Stratum1URL    string        // upstream stratum-1
    MirrorInterval time.Duration // e.g. 15 * time.Minute
    GCInterval     time.Duration // e.g. 1 * time.Hour
    Parallelism    int           // download parallelism
}
```

Mirror and GC run sequentially within each cycle (mirror first, then GC)
to avoid contention on the same prefix directories.  Serving continues
uninterrupted during both operations.

---

## 6  Package Structure

```
gc/
  packfile/
    index.go         // PackIndex: read/write git v2 index files
    index_test.go
    pack.go          // PackWriter: append objects, finalize packs
    pack_test.go
    lookup.go        // PrefixState, MRU index cache, Lookup
    lookup_test.go
  mirror/
    mirror.go        // v1 (kept for now)
    mirrorv2.go      // v2: 256-way parallel mirror with packs
    mirrorv2_test.go
  server/
    server.go        // HTTP server, automatic mirror+GC scheduling
    server_test.go
  catalog/           // unchanged
  hashsort/          // unchanged (reused by GC compaction)
  sweep/             // extended: CompactPrefix for pack-aware GC
  cmd/
    cvmfs-gc/
      main.go        // existing GC entry point
    cvmfs-mirror/
      main.go        // new entry point: mirror-v2 + server
```

---

## 7  Data Flow Summary

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                        Mirror Cycle                              │
 │                                                                  │
 │  manifest ──► history DB ──► tagged roots                        │
 │                                │                                 │
 │  ┌─────────────────────────────┼──────────────────────────┐      │
 │  │  Parallel catalog traversals (LoadOrStore dedup)       │      │
 │  │  current root + tag1 + tag2 + …                        │      │
 │  └──────────────┬─────────────────────────────────────────┘      │
 │                 │  chan catalog.Hash                              │
 │                 ▼                                                 │
 │  ┌──────────── merger ────────────┐                              │
 │  │  routes each hash to           │                              │
 │  │  prefixCh[hash[0]]             │                              │
 │  └─────────────┬──────────────────┘                              │
 │                │                                                  │
 │     ┌──────────┴──────────┐                                      │
 │     │  256 prefix workers │                                      │
 │     │  min-heap dedup     │                                      │
 │     │  download if needed │                                      │
 │     │  append to pack.tmp │                                      │
 │     │  finalize when full │                                      │
 │     └─────────────────────┘                                      │
 └──────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────┐
 │                        GC Cycle                                  │
 │                                                                  │
 │  Same catalog traversal → hashsort → chunk files (reachable set) │
 │                                                                  │
 │  For each prefix XX (≤10 concurrently):                          │
 │    1. Scan pack indices + loose files                             │
 │    2. Count reclaimable volume                                    │
 │    3. If > 5%: lock → k-way merge → new packs → delete old       │
 └──────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────┐
 │                        HTTP Server                               │
 │                                                                  │
 │  GET /data/XX/rest  ──► loose file?  ──► pack lookup (MRU idx)   │
 │                                                                  │
 │  Background:  mirror timer → GC timer → refresh index caches     │
 └──────────────────────────────────────────────────────────────────┘
```

---

## 8  Key Invariants

1. **Index immutability.**  Once `<hash>.idx` is written, it is never
   modified.  Readers can `mmap` or cache it without locking.

2. **Pack-index atomicity.**  A pack's `.idx` is written *after* the
   `.pack` is complete and `fsync`'d.  The index file is written to a
   temp name and renamed atomically.  Therefore:
   - If `.idx` exists, the `.pack` is guaranteed complete.
   - If `.pack` exists without `.idx`, it is incomplete (leftover from
     a crash) and should be removed on startup.

3. **No partial packs at rest.**  `pack.tmp` is only present while a
   mirror or GC is actively writing.  On clean shutdown or crash
   recovery, orphaned `pack.tmp` files are deleted.

4. **Sorted names in index.**  Objects within each index are sorted by
   the full 20-byte hash (bytes 1–19, since byte 0 is the same for all
   entries).  This enables binary search and merge-join.

5. **Fan-out on byte 1.**  Because all objects in `packs/XX/` share byte
   0 = `XX`, the fan-out is keyed on the second byte.  This is an
   adaptation of the git format, not a violation of it — the index is
   self-contained and the interpretation is documented.

6. **GC never blocks serving.**  GC creates new packs before deleting
   old ones.  The server refreshes its index list between these steps.
   In-flight reads against a just-deleted pack may error; the server
   retries from the refreshed index (the data now lives in the new
   pack).

---

## 9  Open Questions / TBD

1. **Pack checksum type.**  Git uses SHA-1 for the 20-byte trailer.  We
   could use SHA-256 (truncated) since we already compute SHA-256 for
   pack naming.  Alternatively, compute both.  Decision: defer to
   implementation — the index format allows either as long as we are
   consistent.

2. **Object length storage.**  Inferring length from adjacent offsets
   (git style) works but requires reading offset[i+1] or knowing the
   pack size for the last object.  An alternative is adding an explicit
   length table after the offsets.  Decision: start with offset-diff
   (git compatible); add explicit lengths only if profiling shows a
   need.

3. **CRC-32 usage.**  Git stores CRC-32 of the *compressed* object data
   in the index.  Since we store objects uncompressed (CVMFS objects are
   already zlib-compressed by the publisher), we CRC-32 the raw bytes
   as written to the pack.  This allows bit-rot detection without
   re-reading the entire pack.

4. **Legacy transition.**  Existing mirrors store everything as loose
   files in `data/`.  The first GC cycle after enabling packs could
   optionally fold small loose files into packs during compaction.
   This is a natural consequence of the k-way merge algorithm — loose
   files below `LooseThreshold` that are reachable get packed.

5. **Concurrent mirror + serve edge case.**  A half-written `pack.tmp`
   contains objects that are not yet indexed.  If the server receives a
   request for such an object during mirroring, it will 404.  This is
   acceptable: the object was not previously available either, and will
   become available once the pack is finalised.  If stricter guarantees
   are needed, the prefix worker could serve from `pack.tmp` by
   consulting its in-memory map, but this adds complexity.
