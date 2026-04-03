# Technology Stack: Server-Side IIIF Image Cache

**Project:** GenizahSearch v7.8
**Researched:** 2026-04-03

## Recommended Stack

### Batch Fetcher (Offline Script)

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| aiohttp | 3.10+ | Async HTTP client for batch IIIF downloads | Fastest async HTTP client for Python -- 2x faster than httpx for high-concurrency requests. Async-only design is fine here since the batch fetcher is a standalone script, not mixed into the sync codebase. |
| asyncio.Semaphore | stdlib | Rate limiting concurrent requests | Built-in, no external dependency. Cap at 5-10 concurrent requests to avoid triggering NLI rate limits. |
| tqdm | (already installed) | Progress bars for batch operations | Already in requirements.txt. Shows download progress, ETA, and failure counts. |
| sqlite3 | stdlib | Read FL IDs from nli_crossref.db | Already used everywhere. The batch script reads nli_crossref.db to enumerate all FL IDs to fetch. |

**Why aiohttp over httpx:** The batch fetcher is a single-purpose async script. aiohttp's raw throughput advantage matters when downloading 815K images. httpx's sync+async flexibility is unnecessary here. The existing `requests` library stays for the web app's synchronous proxy endpoints -- no migration needed.

**Why NOT use `requests` with ThreadPoolExecutor:** The existing api.py uses `requests` with a threading.Semaphore for NLI fetches. That pattern works for on-demand proxying (8 concurrent fetches). For batch downloading 815K images, true async with aiohttp is significantly more efficient -- lower memory per connection, better connection reuse, no thread overhead.

### Image Storage (Flat Files on Disk)

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Flat JPEG files | N/A | Store cached images as individual files | Nginx can serve them directly with zero Python involvement. SQLite BLOBs would require Python to read+serve. S3 adds latency and cost for no benefit on a single EC2 server. |
| Directory sharding | N/A | Prevent filesystem slowdown from 815K files in one dir | Two-level hash prefix: `cache/12/34/FL12345678.jpg`. ext4 handles ~10K files per directory well; 815K in one dir would degrade `ls` and lookup performance. |
| SQLite manifest DB | N/A | Track cache state (what's cached, when, size) | Lightweight metadata index: FL ID, file path, size bytes, fetched timestamp, HTTP status. Enables progress tracking, retry logic, disk usage reporting. |

**Why flat files over SQLite BLOBs:** The entire point is to let nginx serve images directly. With flat files, nginx does `try_files` to check disk, serves with sendfile (zero-copy kernel path), and Python never wakes up for cached images. SQLite BLOBs would mean every image request goes through Python, defeating the performance goal.

**Why flat files over S3:** The server has ~150GB free. S3 adds per-request latency (~50-100ms), egress costs, and deployment complexity. All images are public read, so no access control needed. If EC2 storage becomes insufficient, S3 migration is straightforward later.

**Storage format:** JPEG at 800px width. Based on seed testing (T-S 12.1 = 105KB at 800px), estimated total: ~815K images x 105KB = ~86GB. Fits within 150GB with 64GB headroom. No re-encoding needed -- save NLI's JPEG response bytes directly.

### Serving Layer (nginx Static + Python Fallback)

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| nginx try_files | (already installed) | Serve cached images directly from disk | Zero Python involvement for cache hits. Kernel-level sendfile for maximum throughput. Already running as reverse proxy. |
| Existing Python /api/nli_image_by_sysid | N/A | Fallback for cache misses | Existing endpoint already handles live IIIF fetching. Just need to let nginx check disk first, falling through to Python on miss. |

**nginx configuration pattern:**
```nginx
# Serve cached IIIF images directly, fall back to Python for misses
location /cache/iiif/ {
    alias /home/ubuntu/genizah_image_cache/nli/;
    expires 30d;
    add_header Cache-Control "public, immutable";
    try_files $uri @iiif_fallback;
}

location @iiif_fallback {
    proxy_pass http://127.0.0.1:8081;
    # Python endpoint handles live IIIF fetch
}
```

**Why nginx over a Python static endpoint:** The existing `/api/nli_image_by_sysid` fetches from NLI on every request and caches only in-memory (dict with TTL). For 815K pre-cached images, nginx serving static files is orders of magnitude faster -- no Python GIL, no memory overhead, kernel sendfile. The Python endpoint becomes the fallback for cache misses only.

### Desktop Bulk Download

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| requests | (already installed) | Download cache archive from server | Already in requirements.txt. Desktop is sync-only (PyQt6). No need for async here -- single sequential download of a large archive. |
| zipfile / tarfile | stdlib | Extract downloaded archive | Standard library. The server provides a pre-built archive that desktop extracts to local cache dir. |
| QThread + signals | (already used) | Background download with progress | Existing pattern from gui_threads.py. Show download progress in desktop UI. |

**Desktop download mechanism:** Server pre-builds tar.gz archives segmented by library (CUL ~13GB, JTS ~4GB, etc.). Desktop downloads to `{LOCALAPPDATA}/GenizahSearchPro/cache/iiif/`, same directory structure as server. Desktop browse code checks local cache before making any IIIF request.

**Why NOT per-image download:** Downloading 815K individual images over HTTP is slow (connection overhead per request). Archive downloads are far more efficient. Since ~86GB total is large, segment by library so users can download only what they need.

### Cache Management

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| SQLite manifest DB | N/A | Track what's cached, disk usage | Query "how many images cached", "how much disk used", "what's missing". Simple schema: fl_id, path, size_bytes, fetched_at, http_status. |
| systemd timer | N/A | Schedule incremental batch fetches | Run nightly to fetch new/missing images. Not a Python dependency. |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| HTTP client (batch) | aiohttp | httpx | httpx is slower for pure async workloads; its sync+async flexibility is unnecessary for a batch script |
| HTTP client (batch) | aiohttp | requests + ThreadPoolExecutor | Higher memory per thread, worse connection reuse at 815K scale |
| Storage | Flat JPEG files | SQLite BLOBs | Cannot serve via nginx; Python must handle every request |
| Storage | Flat JPEG files | S3 / R2 | Adds latency, egress cost, complexity; EC2 has enough disk |
| Serving | nginx try_files | Python endpoint (new) | Wasteful -- Python handles what nginx does better at kernel level |
| Serving | nginx try_files | Cloudflare CDN/R2 | Over-engineering; single EC2 serves current user base fine |
| Desktop download | Pre-built archive | Per-image sync | 815K HTTP requests vs segmented archive download; archive is far faster |
| Directory structure | 2-level hash sharding | Flat directory | ext4 degrades with >10K files per directory |
| Directory structure | 2-level hash sharding | Date-based directories | FL IDs don't correlate with dates; hash sharding is evenly distributed |
| Image format | JPEG (pass-through) | WebP conversion | Adds processing time, NLI already returns optimized JPEG, browsers handle JPEG fine |

## What NOT to Add

- **No CDN (Cloudflare R2, CloudFront):** Current user base doesn't justify CDN cost/complexity. Cloudflare is already in front of the site for general caching. Flat files migrate trivially to S3+CloudFront later if needed.
- **No Redis/Memcached:** The cache is static files on disk. nginx's filesystem cache and OS page cache handle hot files naturally.
- **No image processing pipeline:** Store NLI's JPEG bytes as-is. No resizing, re-encoding, or thumbnail generation. The 800px width is requested directly from NLI's IIIF endpoint.
- **No message queue (Celery, RQ):** The batch fetcher is a standalone script run via systemd timer. It reads the manifest DB, finds gaps, and fills them. No job queuing needed.
- **No changes to existing web app cache logic:** The existing in-memory image caches (`_image_cache`, `_nli_cache`) in api.py stay as-is. The nginx layer intercepts requests before they reach Python. No changes to existing cache logic needed.

## Installation

```bash
# Batch fetcher (server only -- NOT in main requirements.txt)
# Create scripts/requirements-batch.txt:
pip install aiohttp>=3.10

# Main requirements.txt -- NO changes needed
# requests, tqdm, sqlite3, Pillow all already present
```

**Important:** aiohttp is a server-side batch tool dependency only. It does NOT go into the main `requirements.txt` (which is shared with desktop). Create a separate `scripts/requirements-batch.txt`.

## Integration Points with Existing Code

### 1. Batch fetcher reads nli_crossref.db
The batch script imports `NliCrossrefService` or directly queries `nli_images` to enumerate all distinct FL IDs (via FGPImageNumberId). Cross-references with manifest.db to find uncached images, then fetches them.

### 2. nginx intercepts before Python
New nginx `location` block for `/cache/iiif/` serves files directly from disk. Cache misses fall through to the existing `/api/nli_image_by_sysid` endpoint. The Python endpoint itself does not change -- nginx handles the cache layer transparently.

### 3. Web app URL mapping
The main code change: browse page maps `(sys_id, page_index)` to a cached FL ID path like `/cache/iiif/12/34/FL12345678.jpg`. The existing `fetch_fl_ids_from_nli()` already resolves sys_id to FL IDs -- the new code maps those FL IDs to cache paths.

### 4. Desktop local cache
Desktop browse checks `{LOCALAPPDATA}/GenizahSearchPro/cache/iiif/` before making network requests. Same directory structure as server. Populated by optional bulk download feature or incrementally as images are viewed.

### 5. Puzzle cache coexistence
The existing puzzle derivative cache (`cache/puzzle/`) is separate and unchanged. Puzzle images are processed PNGs with background removal at specific thresholds; the IIIF cache is unprocessed JPEGs at 800px. Different purpose, different directory, no conflict.

## Directory Structure

```
# Server (EC2)
/home/ubuntu/genizah_image_cache/
  manifest.db                # SQLite: fl_id, path, size_bytes, fetched_at, http_status
  nli/
    12/34/FL12345678.jpg     # 2-level sharding from FL ID digits
    56/78/FL56789012.jpg
  # Future: cambridge/, manchester/ if needed

# Desktop (Windows)
{LOCALAPPDATA}/GenizahSearchPro/cache/iiif/
  nli/
    12/34/FL12345678.jpg     # Same structure as server
```

**Sharding scheme:** Take FL ID digit string, use characters [0:2] as level 1, [2:4] as level 2. Example: FL12345678 -> `12/34/FL12345678.jpg`. This distributes 815K files across up to 10K directories (~80 files each on average).

## Manifest DB Schema

```sql
CREATE TABLE cached_images (
    fl_id TEXT PRIMARY KEY,       -- NLI FL ID (digits only)
    file_path TEXT NOT NULL,      -- Relative path: nli/12/34/FL12345678.jpg
    size_bytes INTEGER,           -- File size for disk usage tracking
    width INTEGER DEFAULT 800,    -- Requested width
    fetched_at TEXT NOT NULL,     -- ISO 8601 timestamp
    http_status INTEGER,          -- NLI response status (200, 404, 503, etc.)
    retry_count INTEGER DEFAULT 0 -- For retry logic on failures
);

CREATE INDEX idx_status ON cached_images(http_status);
CREATE INDEX idx_fetched ON cached_images(fetched_at);
```

## Sources

- [aiohttp vs httpx performance comparison](https://miguel-mendez-ai.com/2024/10/20/aiohttp-vs-httpx) (MEDIUM confidence -- benchmark from Oct 2024, consistent across multiple sources)
- [Python HTTP clients comparison](https://dev.to/leapcell/comparing-requests-aiohttp-and-httpx-which-http-client-should-you-use-3784) (MEDIUM confidence)
- [nginx static content serving](https://docs.nginx.com/nginx/admin-guide/web-server/serving-static-content/) (HIGH confidence -- official nginx docs)
- [nginx try_files directive](https://fideloper.com/nginx-try-files) (HIGH confidence -- well-known reference)
- Existing codebase: `web/api.py`, `shared/puzzle_image_service.py`, `shared/nli_crossref_service.py` (HIGH confidence -- direct code review)
- SEED-001 storage estimates: 815K images x 105KB = ~86GB (MEDIUM confidence -- based on single T-S 12.1 test, actual average may vary; investigation phase should sample more images to validate)
