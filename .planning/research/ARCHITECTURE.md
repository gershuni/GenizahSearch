# Architecture Patterns: Server-Side IIIF Image Cache

**Domain:** Image caching infrastructure for manuscript research platform
**Researched:** 2026-04-03

## Recommended Architecture

### Overview

Add a server-side image cache layer between IIIF sources (NLI, Cambridge, Manchester, JTS) and consumers (web browse, desktop browse, puzzle canvas). The cache sits on EC2 disk, served by nginx as static files for the hot path, with a Python batch fetcher populating the cache and a thin API for cache metadata/status.

```
                                    IIIF Sources
                                    (NLI, CUL, Manchester, JTS)
                                           |
                                    [Batch Fetcher]
                                    (scripts/image_cache_fetcher.py)
                                           |
                                           v
     +----------------------------------+
     |  EC2 Disk: /data/image_cache/    |
     |  /{sys_id_prefix}/{sys_id}/      |
     |  page_{N}_{width}.jpg            |
     +----------------------------------+
           |                    |
     [nginx static]      [Python API]
     /cached-images/     /api/cache_status
           |                    |
     +-----+----+         +----+----+
     |          |          |        |
   Web       Desktop    Admin    Batch
   Browse    Browse     Page     Monitor
```

### Component Boundaries

| Component | Responsibility | New/Modified | Communicates With |
|-----------|---------------|--------------|-------------------|
| **Batch Fetcher** (`scripts/image_cache_fetcher.py`) | NEW | Fetches IIIF images from NLI/CUL/etc, writes to disk | nli_crossref.db (read FL IDs), IIIF servers (fetch), disk cache (write), cache_status.db (track) |
| **Cache Status DB** (`image_cache/cache_status.db`) | NEW | Tracks which images are cached, fetch timestamps, errors | Batch fetcher (write), cache service (read), admin page (read) |
| **Image Cache Service** (`shared/image_cache_service.py`) | NEW | Resolves cache paths, checks availability, provides URLs | cache_status.db (read), nli_crossref.db (read FL IDs) |
| **nginx static location** (`/cached-images/`) | NEW | Serves cached JPEG files directly, bypassing Python | EC2 disk (read), browser/desktop (serve) |
| **web/services.py** (`get_thumbnail_url`, `get_full_image_url`) | MODIFIED | Returns cached URL when available, IIIF URL as fallback | image_cache_service (check), nli_crossref.db (FL IDs) |
| **web/api.py** (`/api/nli_image_by_sysid`) | MODIFIED | Check disk cache before proxying to NLI | disk cache (read), NLI (fallback fetch) |
| **web/pages/browse.py** | MODIFIED (minimal) | Consume new URL format from services.py | web/services.py |
| **manuscript_viewer.js** | MODIFIED | Fallback chain: cached -> server proxy -> client IIIF | nginx cached-images, /api/nli_image_by_sysid |
| **Desktop image loading** (`genizah_core.py`, `gui_threads.py`) | MODIFIED | Try server cache URL before direct NLI fetch | genizahsearch.com/cached-images/ (fetch) |
| **Desktop bulk download** (new dialog/thread) | NEW | Download full cache to local disk for offline use | genizahsearch.com/api/cache_manifest (list), genizahsearch.com/cached-images/ (fetch) |
| **Cache admin page** (`web/pages/admin_cache.py`) | NEW (optional) | Monitor cache fill progress, trigger priority fetches | cache_status.db (read), batch fetcher (control) |

## Data Flow: Detailed Per Use Case

### 1. Web Browse Image Load

**Current flow:**
```
browse.py -> get_thumbnail_url(fl_id) -> direct NLI IIIF URL in <img src>
  -> browser fetches from iiif.nli.org.il
  -> on error: handleImageError() -> /api/nli_image_by_sysid (server proxy)
  -> on error: client-side manifest fetch -> direct NLI URL with discovered FL ID
```

**New flow:**
```
browse.py -> image_cache_service.get_image_url(sys_id, page, width)
  -> IF cached: returns /cached-images/{prefix}/{sys_id}/page_{N}_{width}.jpg
     -> nginx serves static file (zero Python overhead)
  -> IF not cached: returns /api/nli_image_by_sysid/{sys_id}?page={N}
     -> api.py checks disk cache (belt-and-suspenders)
     -> falls through to NLI proxy as today

<img src> error fallback chain (manuscript_viewer.js):
  1. /cached-images/{prefix}/{sys_id}/page_{N}_{width}.jpg (nginx static)
  2. /api/nli_image_by_sysid/{sys_id}?page={N} (Python proxy, may also cache-fill on miss)
  3. Client-side IIIF manifest fetch (existing handleImageError logic)
```

**Key change:** `web/services.py:get_thumbnail_url()` and `get_full_image_url()` gain a cache-aware variant. The BrowsePage dataclass gets `cached_thumb_url` and `cached_image_url` fields populated during Phase A (fast local check against cache_status.db or filesystem stat).

### 2. Desktop Browse Image Load

**Current flow:**
```
genizah_core.py:get_thumbnail() -> _fetch_fl_ids() from NLI IIIF manifest
  -> _resolve_thumbnail(fl_ids) -> NLI IIIF URL
  -> ImageLoaderThread fetches directly from iiif.nli.org.il
```

**New flow:**
```
genizah_core.py:get_thumbnail() -> check local cache dir first
  -> IF local cache exists (bulk download): serve from disk
  -> IF no local cache: try https://genizahsearch.com/cached-images/{prefix}/{sys_id}/page_0_400.jpg
  -> IF server cache miss (404): fall back to direct NLI IIIF (existing path)
```

**Key change:** `ImageLoaderThread` in `gui_threads.py` gets a URL resolution step: try server cache URL first, fall back to NLI on 404. No structural change to the thread model -- just a URL prefix swap.

### 3. Batch Fetch Job

```
scripts/image_cache_fetcher.py (runs as systemd service or cron on EC2)
  |
  1. Query nli_crossref.db for uncached sys_ids (JOIN against cache_status.db)
  2. Sort by priority: NLI-only manuscripts first, then by popularity (PostHog optional)
  3. For each sys_id:
     a. Fetch FL IDs from nli_crossref.db (local, no network needed for NLI)
        OR fetch IIIF manifest for non-NLI libraries
     b. For each page/FL ID:
        - Rate-limit: configurable delay (e.g., 200ms between requests)
        - Fetch IIIF image: /full/{width},/0/default.jpg
        - Write to disk: /data/image_cache/{prefix}/{sys_id}/page_{N}_{width}.jpg
        - Update cache_status.db: (sys_id, page, width, fetched_at, size_bytes, source)
     c. On failure: record error in cache_status.db, skip, continue
  4. Log progress: images/hour, estimated completion, error rate
  5. Graceful shutdown on SIGTERM

Concurrency: single-threaded sequential fetch with configurable rate limit.
  (Burst parallel fetching risks IP blocking from IIIF servers.)
```

### 4. Cache Status Monitoring

```
/api/cache_status -> JSON summary:
  {
    "total_images": 815000,
    "cached_images": 342000,
    "cache_percent": 42.0,
    "disk_used_gb": 36.2,
    "disk_free_gb": 113.8,
    "fetcher_running": true,
    "fetch_rate_per_hour": 4500,
    "eta_hours": 105,
    "last_error": "2026-04-03T14:22:00Z: NLI timeout on FL7734473",
    "by_library": {
      "CUL": {"total": 340000, "cached": 200000},
      "JTS": {"total": 80000, "cached": 45000},
      ...
    }
  }
```

Optional admin page at `/admin/cache` shows this visually. Low priority -- JSON endpoint suffices for monitoring.

## Storage Layout

### Disk Path Structure

```
/data/image_cache/
  status.db                          # SQLite: cache tracking metadata
  nli/                               # NLI-sourced images
    00/                              # First 2 chars of sys_id (sharding)
      003549876/                     # sys_id directory
        page_0_800.jpg               # Page 0, 800px width
        page_1_800.jpg               # Page 1, 800px width
        page_0_400.jpg               # Page 0, 400px (thumbnail)
    99/
      997234561/
        page_0_800.jpg
  cambridge/                         # Cambridge CUDL images (future)
    MS-TS-00012-00001/
      page_0_800.jpg
```

**Why this layout:**
- **2-char prefix sharding**: Prevents any single directory from exceeding filesystem limits (~255K entries). With 253K distinct AlmaIds, even distribution means ~2,500 per shard bucket.
- **sys_id as directory**: Natural grouping, easy to check "is this manuscript cached?" with a single stat().
- **page_N_width.jpg naming**: Deterministic, no FL ID needed in path (FL IDs are an NLI implementation detail; sys_id is the stable identifier).
- **Separate library dirs**: Different provenance, different update cycles, easy to cache Cambridge separately later.

### Storage Estimates

| Resolution | Per Image | 815K Images | 815K x 2 sizes | Notes |
|------------|-----------|-------------|-----------------|-------|
| 400px (thumb) | ~25 KB | ~20 GB | -- | Thumbnail for search results |
| 800px (browse) | ~105 KB | ~86 GB | -- | Minimum for reading text |
| 400 + 800 | -- | -- | ~106 GB | Both sizes cached |

**Recommendation:** Cache 800px as primary (readable text), generate 400px thumbnails on-demand or in a second pass. 800px at ~86GB fits within 150GB with ~64GB headroom.

### cache_status.db Schema

```sql
CREATE TABLE cached_images (
    sys_id TEXT NOT NULL,
    page_index INTEGER NOT NULL,
    width INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'nli',  -- 'nli', 'cambridge', 'manchester', 'jts'
    fl_id TEXT,                          -- Original FL ID (for NLI provenance tracking)
    size_bytes INTEGER,
    fetched_at TEXT NOT NULL,            -- ISO 8601
    PRIMARY KEY (sys_id, page_index, width, source)
);

CREATE TABLE fetch_errors (
    sys_id TEXT NOT NULL,
    page_index INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'nli',
    error_message TEXT,
    failed_at TEXT NOT NULL,
    retry_count INTEGER DEFAULT 0
);

CREATE TABLE fetch_progress (
    key TEXT PRIMARY KEY,
    value TEXT
);
-- Keys: 'last_sys_id', 'total_fetched', 'start_time', 'fetcher_pid'

CREATE INDEX idx_cached_sys ON cached_images(sys_id);
CREATE INDEX idx_errors_retry ON fetch_errors(retry_count, failed_at);
```

## Serving Architecture Decision: nginx Static Files

**Decision: Serve cached images via nginx `location /cached-images/` block.**

### Why nginx, not Python:

| Factor | nginx Static | Python Endpoint |
|--------|-------------|-----------------|
| **Throughput** | 10K+ req/s, zero Python GIL contention | ~200 req/s through NiceGUI/Starlette, blocks event loop |
| **Latency** | <1ms (sendfile syscall) | 5-50ms (Python overhead + async context) |
| **Memory** | Zero per-request allocation | Python response objects per concurrent request |
| **Cache headers** | Native `expires`, `Cache-Control`, `ETag` | Manual header management |
| **Complexity** | 5 lines of nginx config | New API route + file reading + error handling |
| **CDN-friendly** | Cloudflare caches `/cached-images/*` trivially | Need explicit cache rules |

### nginx Config Addition

```nginx
# Add inside the server { } block, BEFORE the catch-all location / { }
location /cached-images/ {
    alias /data/image_cache/;
    expires 30d;
    add_header Cache-Control "public, immutable";
    add_header Access-Control-Allow-Origin "*";
    try_files $uri =404;

    # Disable logging for cached images (high volume)
    access_log off;

    # Gzip off for JPEG (already compressed)
    gzip off;

    # Limit to GET/HEAD
    limit_except GET HEAD { deny all; }
}
```

**Desktop access:** The desktop app fetches from `https://genizahsearch.com/cached-images/nli/{prefix}/{sys_id}/page_{N}_{width}.jpg` -- same URL, served by nginx, passes through Cloudflare CDN. No Python involvement.

### Python API Role (Complementary)

Python handles only:
1. **`/api/nli_image_by_sysid`** (existing) -- modified to check disk cache before proxying to NLI. Acts as a warm-fill: if user requests an uncached image, Python proxies it AND writes to disk cache simultaneously.
2. **`/api/cache_status`** (new) -- JSON cache statistics for monitoring.
3. **`/api/cache_manifest`** (new, for desktop bulk download) -- Returns list of cached sys_ids + page counts for desktop download manager.

## Patterns to Follow

### Pattern 1: Cache-Through on Miss (Opportunistic Fill)
**What:** When `/api/nli_image_by_sysid` fetches an image from NLI (cache miss), simultaneously write it to the disk cache so subsequent requests are served by nginx.
**When:** Every NLI proxy request that succeeds.
**Example:**
```python
# In web/api.py, nli_image_by_sysid handler
if resp.status_code == 200 and len(resp.content) > min_size:
    # Serve to client immediately
    # Also write to disk cache (fire-and-forget)
    _save_to_image_cache(sys_id, page, width, resp.content, suffix)
    return Response(content=resp.content, ...)
```

### Pattern 2: Deterministic Path Resolution (No DB Lookup for Hot Path)
**What:** The nginx URL for a cached image is computed purely from (sys_id, page_index, width) with no database query. The client can construct the URL and try it; nginx returns 404 if not cached, triggering the fallback chain.
**When:** Every image load in browse and search.
**Example:**
```python
def get_cached_image_url(sys_id: str, page: int = 0, width: int = 800) -> str:
    prefix = sys_id[:2] if len(sys_id) >= 2 else '00'
    return f"/cached-images/nli/{prefix}/{sys_id}/page_{page}_{width}.jpg"
```

### Pattern 3: Priority Queue for Batch Fetcher
**What:** Fetch NLI-only manuscripts first (those without Cambridge/Manchester/JTS alternatives), then CUL-only, then others.
**When:** Batch fetcher ordering.
**Rationale:** NLI-only manuscripts have zero fallback -- when NLI is down, these images are completely unavailable. Manuscripts with Cambridge/Manchester alternatives already have reliable image sources.

### Pattern 4: Shared Service Layer (Consistent with Project Architecture)
**What:** `shared/image_cache_service.py` provides cache path resolution and status queries used by both web and desktop.
**When:** Both apps need to know if an image is cached and construct the correct URL.
**Rationale:** Follows the established shared service pattern (document_service, fjms_service, nli_crossref_service, etc.).

## Anti-Patterns to Avoid

### Anti-Pattern 1: Serving Images Through Python
**What:** Using a FastAPI/Starlette endpoint to read files from disk and return them.
**Why bad:** Python GIL contention, event loop blocking on file I/O, 50x slower than nginx sendfile, NiceGUI server becomes the bottleneck for the most frequent request type (images).
**Instead:** nginx `location /cached-images/` with `alias` directive. Python only handles the miss/fallback path.

### Anti-Pattern 2: Using FL IDs in Cache Paths
**What:** Organizing cache by FL ID instead of sys_id.
**Why bad:** FL IDs are NLI-internal identifiers that can change (manifest updates). sys_id is the stable manuscript identifier used throughout the app. FL ID paths would require a DB lookup to resolve what the user is viewing.
**Instead:** Use `sys_id/page_N_width.jpg`. Store FL ID in cache_status.db for provenance tracking only.

### Anti-Pattern 3: Parallel Burst Fetching from NLI
**What:** Running multiple threads/processes to speed up the batch fetch.
**Why bad:** NLI rate-limits aggressively. Burst traffic from a single IP will get blocked, potentially for days. Other IIIF servers (Cambridge CUDL, Manchester LUNA) have similar policies.
**Instead:** Single-threaded sequential fetch with 200-500ms delay between requests. At 200ms delay = 18K images/hour = ~45 hours for 815K images. Acceptable for a background job running over days.

### Anti-Pattern 4: Cache Invalidation Complexity
**What:** Building a sophisticated invalidation/refresh system.
**Why bad:** IIIF images of historical manuscripts essentially never change. The underlying manuscripts are centuries old. Adding version checking, ETag validation, or periodic re-fetch adds complexity for near-zero benefit.
**Instead:** Fetch once, keep forever. If an image genuinely changes (extremely rare), manual re-fetch for that sys_id via admin action.

## Integration Points With Existing Code

### Files to Modify

| File | Change | Complexity |
|------|--------|------------|
| `web/services.py` | Add `get_cached_image_url()`, modify `get_thumbnail_url()`/`get_full_image_url()` to prefer cached URL | Low |
| `web/api.py` | Add cache-through write in `nli_image_by_sysid`, add `/api/cache_status`, `/api/cache_manifest` | Medium |
| `web/static/manuscript_viewer.js` | Add cached URL as first entry in `handleImageError` fallback chain | Low |
| `genizah_core.py` | Add server cache URL as first try in `_resolve_thumbnail()` / desktop image loading | Low |
| `gui_threads.py` | `ImageLoaderThread` tries server cache URL before direct NLI | Low |
| nginx config on EC2 | Add `location /cached-images/` block | Low |

### New Files

| File | Purpose | Complexity |
|------|---------|------------|
| `shared/image_cache_service.py` | Cache path resolution, status queries, shared between web/desktop | Medium |
| `scripts/image_cache_fetcher.py` | Batch fetch daemon, rate-limited, priority-ordered | Medium |
| `scripts/image_cache_admin.py` | CLI tools: stats, re-fetch, purge (optional) | Low |

### Files NOT Changed

| File | Why |
|------|-----|
| `shared/puzzle_image_service.py` | Puzzle cache is a separate concern (background-removed PNGs, not raw JPEGs). No overlap. |
| `shared/nli_crossref_service.py` | Read-only data source. Used by batch fetcher but not modified. |
| `web/pages/browse.py` | Consumes URLs from services.py. If services.py returns a cached URL, browse.py uses it transparently. Minimal or zero changes needed. |

## Desktop Bulk Download Architecture

For offline use, the desktop app can download the full image cache locally.

```
Desktop Settings -> "Download Image Cache" button
  -> GET /api/cache_manifest -> JSON: [{sys_id, pages, total_bytes}, ...]
  -> Download thread iterates:
     For each sys_id in manifest:
       For each page:
         GET /cached-images/nli/{prefix}/{sys_id}/page_{N}_800.jpg
         -> Write to {LOCALAPPDATA}/GenizahSearchPro/cache/images/nli/{prefix}/{sys_id}/...
  -> Progress dialog: X of Y images, Z GB / N GB, ETA
  -> Resumable: skip files that already exist locally + match expected size
```

**Storage on user machine:** ~86GB for full 800px cache. Users can choose partial download (e.g., CUL only = ~35GB estimated). The desktop image loading path checks local cache before trying the server.

## Scalability Considerations

| Concern | Current (0 cached) | At 100K cached | At 815K cached |
|---------|--------------------|--------------------|---------------------|
| Disk usage | 0 | ~10 GB | ~86 GB (800px only) |
| nginx memory | Unchanged | Unchanged | Unchanged (sendfile) |
| Fetch time (batch) | N/A | ~6 hours | ~45 hours (sequential 200ms) |
| Browse latency (cache hit) | N/A | <5ms (nginx + disk) | <5ms |
| Browse latency (cache miss) | 200-2000ms (NLI proxy) | Same | Rare |
| Desktop download | N/A | ~1 hour (broadband) | ~12 hours (broadband) |

## Build Order (Dependency-Aware)

```
Phase 1: Investigation & Foundation
  - Rate limit testing (manual NLI fetch experiments)
  - Create /data/image_cache/ directory structure on EC2
  - Write shared/image_cache_service.py (path resolution, URL construction)
  - Create cache_status.db schema
  - Add nginx location block for /cached-images/

Phase 2: Batch Fetcher
  - Write scripts/image_cache_fetcher.py
  - Priority ordering (NLI-only first)
  - Rate limiting, error handling, progress tracking
  - systemd unit file for running as background service
  - Start fetching (runs for days in background)
  Depends on: Phase 1 (disk layout, status DB)

Phase 3: Web Integration
  - Modify web/services.py URL resolution (cached URL preference)
  - Modify web/api.py with cache-through on miss
  - Update manuscript_viewer.js fallback chain
  - Add /api/cache_status endpoint
  Depends on: Phase 1 (cache service), Phase 2 can run in parallel

Phase 4: Desktop Integration
  - Modify genizah_core.py to try server cache URL first
  - Modify gui_threads.py ImageLoaderThread fallback
  - Add /api/cache_manifest endpoint
  - Desktop bulk download dialog + thread
  Depends on: Phase 3 (server endpoints exist)

Phase 5: Monitoring & Polish
  - Cache status admin page (optional)
  - Desktop download progress/resume UI
  - Documentation
  Depends on: Phases 3-4
```

## Sources

- Existing codebase analysis: `web/api.py`, `shared/puzzle_image_service.py`, `shared/nli_crossref_service.py`, `web/services.py`, `web/static/manuscript_viewer.js`
- `.planning/seeds/SEED-001-server-iiif-image-cache.md` -- seed exploration notes with storage estimates
- `docs/guides/DEPLOYMENT_TECHNICAL.md` -- nginx config, EC2 deployment details
- IIIF Image API specification for URL format: `{base}/{identifier}/full/{width},/0/default.jpg`
- nginx documentation for `alias`, `try_files`, `sendfile` directives (HIGH confidence -- well-established patterns)
