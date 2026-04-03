# Domain Pitfalls: Server-Side IIIF Image Caching

**Domain:** IIIF image caching for a manuscript research platform (NLI primary, CUL/Manchester/JTS/Oxford secondary)
**Researched:** 2026-04-03
**Overall confidence:** HIGH (most pitfalls verified against existing codebase behavior and documented NLI testing)

## Critical Pitfalls

Mistakes that cause the entire caching strategy to fail or require fundamental rearchitecting.

### Pitfall 1: NLI Blocks All Datacenter IPs -- Cannot Batch-Fetch From EC2

**What goes wrong:** You write a batch fetcher, deploy it on EC2, and every request to `iiif.nli.org.il` returns HTTP 500 or 403. Zero images cached.

**Why it happens:** NLI blocks requests from datacenter/CDN IP ranges broadly -- not just AWS, but also Cloudflare Workers and other cloud providers. This was tested and confirmed on 2026-03-17: server curl with full browser headers returns HTTP 500 (XML error response). Cloudflare Worker proxy also returns upstream 500. NLI blocks by IP range, not by headers or User-Agent.

**Consequences:** The entire batch-fetch architecture is dead on arrival if it assumes server-to-NLI access. This is not a rate limiting issue -- it is a total block.

**Prevention:**
- Batch fetching MUST run from a residential or university IP, not from EC2 or any datacenter
- Options: (a) run the fetcher on a home machine and rsync/scp results to EC2, (b) use a residential proxy service (cost + TOS concerns), (c) leverage the existing browser extension pattern where user browsers seed the cache organically
- The investigation phase must validate which approach works before building any batch infrastructure
- Do NOT assume "we'll just add the right headers" -- this was already tested and ruled out

**Detection:** Test `curl -I "https://iiif.nli.org.il/IIIFv21/FL990051753360205171/full/800,/0/default.jpg"` from any candidate IP before writing any batch code. If it returns anything other than 200 with image content, the IP is blocked.

**Phase:** Investigation (must be resolved first -- everything else depends on this)

**Confidence:** HIGH -- verified in codebase at `docs/archive/plans/PUZZLE_IIIF_SERVER_SIDE_PROCESSING.md` lines 317-320, and confirmed by the entire browser extension architecture that was built specifically to work around this block.

---

### Pitfall 2: 815K Files Exhaust Inodes Before Filling Disk Space

**What goes wrong:** After caching ~200K images, `df -h` shows 60GB free but file creation fails with "No space left on device." The server crashes for unrelated operations that need to create temp files.

**Why it happens:** Linux ext4 filesystems allocate a fixed number of inodes at format time (typically 1 inode per 16KB of disk). With 815K image files at ~105KB each (86GB), you also need 815K inodes. Depending on the EC2 EBS volume's filesystem formatting, the inode budget may be exhausted well before disk space runs out. Additionally, if images are organized in flat directories, even `ls` becomes unusably slow with hundreds of thousands of entries.

**Consequences:** Server stops working entirely -- not just image serving but the web app, Tantivy index, SQLite sidecars, and any process that needs to create a file. Recovery requires emergency disk expansion or inode cleanup while the site is down.

**Prevention:**
- Check inode budget before starting: `df -i` on the target volume. Need at least 900K free inodes (815K images + overhead)
- Use a hierarchical directory structure: `cache/images/{FL_ID[0:3]}/{FL_ID[3:6]}/{FL_ID}.jpg` -- each directory holds at most ~1000 files
- Consider storing images in a SQLite blob database to reduce inode consumption to near-zero (one file, 815K blobs)
- Set up CloudWatch alarm on inode usage (`df -i`) in addition to disk space
- Alternative: use a separate EBS volume mounted at `/mnt/image-cache` with XFS (no fixed inode limit) or ext4 formatted with `-i 4096` (more inodes per block)

**Detection:** Monitor `df -i` alongside `df -h`. Alert at 80% inode usage.

**Phase:** Implementation (directory structure decision during architecture)

**Confidence:** HIGH -- well-documented Linux operations issue, especially relevant with hundreds of thousands of small files.

---

### Pitfall 3: Serving 86GB of Static Images Starves the Web App of RAM and I/O

**What goes wrong:** After populating the cache, users browsing manuscripts trigger heavy disk I/O for image serving, and the NiceGUI web app becomes sluggish. Search queries slow down, SQLite reads stall, and the server becomes unresponsive during traffic peaks.

**Why it happens:** The EC2 instance runs the NiceGUI web app, Tantivy index, and all SQLite sidecars on the same hardware. Adding 86GB of image serving creates competition for: (a) disk I/O bandwidth (especially on EBS gp3 with baseline 3000 IOPS), (b) Linux page cache (OS tries to cache hot images in RAM, evicting SQLite/Tantivy pages), (c) network bandwidth. If images are served through the Python app (NiceGUI endpoints), each image request also ties up a Python thread/async task.

**Consequences:** Degraded search and browse performance. Potential timeouts. SQLite "database is locked" errors under concurrent I/O pressure. The web app appears broken even though the server is technically up.

**Prevention:**
- Serve cached images directly through nginx (`location /cache/images/ { alias /path/to/cache/; }`) -- this bypasses Python entirely, uses sendfile for zero-copy I/O, and nginx handles thousands of concurrent static file requests efficiently
- Set `Cache-Control: public, max-age=604800` (1 week) on cached images so browsers and Cloudflare cache them aggressively, reducing repeat requests to the server
- Leverage Cloudflare's free CDN caching: images served through Cloudflare will be cached at edge, dramatically reducing origin load after initial fetch
- If disk I/O becomes a bottleneck, move the image cache to a separate EBS volume so its I/O budget doesn't compete with the app's data volumes
- Monitor EBS IOPS and burst balance via CloudWatch

**Detection:** Compare Tantivy search latency and SQLite query times before and after enabling image serving. If P95 latency doubles, I/O contention is the cause.

**Phase:** Serving layer (architecture decision for how images are exposed)

**Confidence:** HIGH -- standard infrastructure concern, validated by existing deployment architecture docs showing single EC2 instance.

---

### Pitfall 4: Partial Cache Creates Inconsistent User Experience

**What goes wrong:** Some manuscripts load images instantly (cached), while others show the slow NLI loading spinner or fail entirely (NLI is down). Users don't understand why some pages work and others don't. Worse, the fallback logic has bugs where it shows a broken image instead of falling back.

**Why it happens:** With 815K target images, the cache population will take days or weeks. During this period, and permanently for any missed images, the system operates in a hybrid state. Every image load path (browse, search thumbnails, puzzle, reading desk, fullscreen viewer) must correctly handle: (a) cache hit, (b) cache miss + NLI available, (c) cache miss + NLI down, (d) corrupted/truncated cache file. The current codebase has multiple independent image loading paths that each need updating.

**Consequences:** User confusion ("why does this manuscript have images but that one doesn't?"). False bug reports. If fallback is broken in any of the ~6 image loading codepaths, some features appear completely broken.

**Prevention:**
- Design a single image resolution function used by ALL codepaths (browse, puzzle, search, desktop): `resolve_image(fl_id, size) -> (bytes, source)` with a unified fallback chain: cache -> NLI live -> placeholder
- Populate cache in priority order: (a) manuscripts without CUL/Oxford/Manchester/JTS alternatives first (these have no fallback), (b) most-viewed manuscripts (PostHog data), (c) remaining by library_code
- Provide a cache coverage indicator (e.g., "87% cached" in admin dashboard) so the operator knows the state
- Handle truncated/corrupted cache files: verify file size > minimum threshold (e.g., 1KB) before serving, re-fetch if corrupt
- Add a `X-Image-Source: cache|nli-live|placeholder` response header for debugging

**Detection:** Track cache hit rate in PostHog or server logs. Alert if cache miss rate spikes (could indicate cache corruption or disk issues).

**Phase:** Implementation + Serving layer (unified resolver in implementation, monitoring in serving)

**Confidence:** HIGH -- this is a common distributed caching issue, and the codebase already has ~6 separate image loading paths that would all need updating.

---

### Pitfall 5: Legal/TOS Risk of Bulk-Downloading NLI Content

**What goes wrong:** After spending weeks building and populating the cache, NLI contacts you demanding takedown, or blocks your residential IP permanently, cutting off the batch pipeline.

**Why it happens:** The NLI IIIF API is intended for interactive, on-demand image viewing -- not bulk archival downloading of their entire collection. Their IP blocking of datacenters suggests they are actively trying to prevent automated bulk access. Downloading 815K images (86GB) in a systematic sweep could be viewed as scraping/crawling, potentially violating their Terms of Service even from a residential IP.

**Consequences:** Loss of NLI cooperation. Potential legal action. Permanent IP block on any IPs used for fetching. Reputational damage for the project in the small Genizah studies community where NLI relationships matter.

**Prevention:**
- Contact NLI before any bulk downloading. Frame it as: "We're building a research tool for the Genizah community and want to cache images for reliability. Can we coordinate?" NLI may provide a bulk export, an API key with higher limits, or explicit permission
- If no response or permission denied: only cache images that users have already viewed (organic caching), not proactive bulk fetching. This is defensible -- it's standard browser caching behavior at a server level
- Rate limit any automated fetching aggressively: 1 request per 2-3 seconds maximum, with random jitter
- Keep a clear separation: the cached files are a performance optimization, not a competing digital library. Don't redistribute the cache, don't make it downloadable as a dataset
- Document the caching rationale in case of inquiry: "We cache at 800px for reliability; researchers still visit NLI for full-resolution images"

**Detection:** No technical detection -- this is a legal/relationship risk. Mitigate by reaching out proactively.

**Phase:** Investigation (must be addressed before any batch fetching begins)

**Confidence:** MEDIUM -- NLI's specific policies for academic research tools are unknown. The datacenter IP blocking is aggressive but could be anti-scraping rather than anti-academic. NLI does provide a public IIIF API, suggesting some level of intended automated access. But 815K systematic downloads is qualitatively different from interactive use.

## Moderate Pitfalls

### Pitfall 6: FL ID Mapping Gap -- nli_crossref Has FGP Numbers, Not FL IDs

**What goes wrong:** You try to read FL IDs from nli_crossref.db to build the fetch list, but the FGPImageNumberId column contains Friedberg photo numbers, NOT NLI IIIF FL IDs. These are completely different numbering systems.

**Why it happens:** This was a lesson learned from Phase 30: "crossref FGPImageNumberId is a Friedberg photo number, NOT an NLI IIIF FL ID. Different numbering systems. IIIF manifest fetch remains required for image URLs."

**Consequences:** Batch fetcher constructs wrong URLs, gets 404s for every image, wastes time and generates useless error logs.

**Prevention:**
- The batch fetcher must resolve sys_ids to FL IDs via IIIF manifest fetches, using the same `fetch_fl_ids_from_nli()` logic in `web/api.py`
- The persistent FL ID cache (`nli_fl_ids_cache.json`) already has many resolved mappings -- seed the batch fetcher from this
- Build a separate FL ID resolution pass (from a non-blocked IP) before the image fetch pass
- Store resolved FL IDs in the cache metadata DB for reuse

**Phase:** Implementation (batch fetcher architecture must account for two-pass: resolve FL IDs, then fetch images)

**Confidence:** HIGH -- documented lesson learned from Phase 30, verified in CLAUDE.md and PROJECT.md.

---

### Pitfall 7: Cache Invalidation -- NLI Re-Digitizes Manuscripts

**What goes wrong:** NLI re-scans a manuscript at higher quality or corrects a misfiled image. Your cache serves the old version indefinitely. A researcher sees a degraded or wrong image and doesn't know to check NLI directly.

**Why it happens:** IIIF manifests can change when institutions re-digitize content, correct metadata, or reorganize collections. NLI has been actively upgrading their digitization. The IIIF Change Discovery API exists for exactly this purpose, but NLI may not implement it.

**Prevention:**
- Store cache date per image (file mtime or metadata DB). Treat cache as "stale after N months" (e.g., 6 months)
- Implement a slow background re-validation cycle: check 1000 manifests per day against NLI (from a non-blocked IP) to see if FL IDs or image counts have changed
- For critical use (puzzle/research), offer a "refresh from source" button that bypasses cache
- Don't cache at full resolution -- cache at 800px and let researchers who need full-res go to NLI directly. This also makes staleness less impactful

**Phase:** Serving layer (add staleness tracking and refresh mechanism)

**Confidence:** MEDIUM -- NLI re-digitization frequency is unknown, but it does happen.

---

### Pitfall 8: Desktop Download UX -- 86GB Is Not a Casual Download

**What goes wrong:** You add a "Download full image cache" button. Users click it expecting a quick download, get 86GB transferred over hours, fill their disk, or the download fails at 60% with no resume capability.

**Why it happens:** The seed mentions "Desktop option to download the full image cache locally for offline use." 86GB at 800px is enormous for a desktop download. Even on fast broadband (100 Mbps), that's ~2 hours. Many users will have slower connections, data caps, or insufficient disk space.

**Consequences:** Failed downloads waste user time. Disk space exhaustion on user machines. No resume means starting over after any interruption.

**Prevention:**
- Offer tiered download: (a) metadata-only cache (just the FL ID -> filename mappings, ~5MB), (b) thumbnail cache (200px, ~8-10GB), (c) full 800px cache (86GB)
- Use a resumable download mechanism (HTTP Range requests + local file verification), not a single monolithic transfer
- Show disk space requirements upfront: "This will use approximately 86GB of disk space. You have X GB free."
- Consider library-specific downloads: "Download CUL images (40GB)" vs "Download all (86GB)"
- Show progress with ETA, and allow pause/resume across sessions
- Alternative: don't offer bulk desktop download at all. Cache organically as the desktop user browses (the existing puzzle cache pattern). This is simpler and uses only disk space for manuscripts the user actually views

**Phase:** Desktop integration (if bulk download is pursued)

**Confidence:** HIGH -- large file download UX is well-understood. The existing puzzle cache pattern of organic caching is proven in the codebase.

---

### Pitfall 9: Multiple Image Loading Codepaths Get Out of Sync

**What goes wrong:** The cache works for browse but not for puzzle. Or it works for web but not desktop. Or search thumbnails use the cache but the fullscreen viewer doesn't. One codepath gets updated, the others silently fall back to live NLI, and nobody notices until NLI goes down.

**Why it happens:** The codebase currently has many separate image loading sites:
- `web/api.py`: `/api/nli_image/{fl_id}`, `/api/nli_image_by_sysid/{sys_id}`, `/api/puzzle_image`
- `web/pages/browse.py`: Image URL construction via `/api/nli_image_by_sysid/`
- `web/pages/puzzle.py`: JS `_loadImageWithFallbacks()` chain (server cache -> extension -> localhost helper -> direct NLI)
- `genizah_app.py`: Desktop image fetch threads in `gui_threads.py`
- `shared/puzzle_image_service.py`: Puzzle-specific IIIF fetch + cache
- `web/pages/search.py`: Thumbnail loading in search results

Each of these has its own URL construction, error handling, and fallback logic. Adding a cache layer means updating ALL of them consistently.

**Consequences:** Inconsistent behavior. Some features benefit from the cache, others don't. Debugging becomes nightmarish because the symptom depends on which feature the user is using.

**Prevention:**
- Create a single `shared/image_cache_service.py` that encapsulates ALL cache logic: check cache -> serve from cache -> fallback to live -> handle errors
- All image endpoints in `web/api.py` call this service. Desktop app calls the same service
- The puzzle's special pipeline (background removal) layers ON TOP of the base cache service, not as a parallel path
- Test all 6+ image loading paths explicitly as part of verification: browse page, search thumbnail, fullscreen viewer, puzzle canvas, reading desk, desktop browse

**Phase:** Implementation (architecture decision to unify before adding cache)

**Confidence:** HIGH -- verified by inspecting the codebase: there are at least 6 independent image loading paths.

---

### Pitfall 10: Batch Fetcher Overwhelms Home Network or Gets Residential IP Banned

**What goes wrong:** You run the batch fetcher from home at 10 requests/second. After 50K images, your home IP gets blocked by NLI, or your ISP throttles you, or your home router runs out of NAT entries and drops connections.

**Why it happens:** Even from a non-blocked residential IP, 815K requests at any meaningful rate is heavy. NLI may have per-IP rate limits that kick in after sustained traffic. ISPs may flag sustained high-volume HTTPS as suspicious.

**Prevention:**
- Start with a conservative rate: 1 request per 3 seconds (28K images/day, ~29 days for full corpus). This is slow but safe
- Monitor for HTTP 429 (Too Many Requests) or increasing error rates -- back off exponentially
- Use multiple residential IPs if available (home + university) to distribute load
- Implement checkpoint/resume: save progress to a JSON/DB file so the fetcher can be stopped and restarted without re-downloading
- Fetch in priority order (see Pitfall 4) so that even if you only get 100K images before hitting issues, they're the most valuable 100K
- Consider running only during off-peak hours (NLI is in Israel, UTC+2/+3; US evening is NLI deep night)

**Phase:** Investigation + Implementation (rate testing during investigation, batch architecture during implementation)

**Confidence:** MEDIUM -- NLI's specific per-IP rate limits from residential IPs are unknown. The conservative approach is precautionary.

---

### Pitfall 11: Storage Estimate Wrong -- 86GB Based on Single Test Image

**What goes wrong:** The 86GB estimate (815K x 105KB) is extrapolated from a single test image (T-S 12.1 at 800px = 105KB). Actual average could be 50KB (small fragments) or 200KB (large folios), yielding 41GB-163GB total.

**Why it happens:** Manuscript images vary hugely in physical size and content density. Multi-folio manuscripts have more/larger images. Some fragments are tiny scraps, others are full codex pages.

**Consequences:** If actual is 163GB, it won't fit in 150GB free. Project stalls or requires disk expansion.

**Prevention:**
- Sample first: fetch 1000 random images across libraries (CUL, Manchester, JTS) and compute actual average + P95 size
- Set disk usage alerts at 80% and 90% capacity
- Build the batch fetcher to check available disk before each write and pause gracefully when reaching a floor (e.g., 20GB remaining)

**Phase:** Investigation (sample before committing to full batch)

**Confidence:** HIGH -- trivially verifiable by sampling.

## Minor Pitfalls

### Pitfall 12: Cache Directory Grows With Duplicate Resolutions

**What goes wrong:** The cache ends up containing both 800px AND 2000px versions of the same image (browse requests 800px, puzzle requests different resolution), doubling storage consumption unexpectedly.

**Prevention:** Cache at exactly one resolution (800px per the seed). If higher resolution is needed, serve it live from NLI via the user's browser. Document this as a design constraint. The existing puzzle-specific cache (separate directory at `cache/puzzle/`) remains independent.

**Phase:** Implementation (enforce single resolution in cache service)

---

### Pitfall 13: nginx Configuration Breaks Existing Routes

**What goes wrong:** The new nginx `location /cache/iiif/` block interferes with existing route handling (e.g., catches requests meant for Python, or the NiceGUI reverse proxy stops working).

**Prevention:**
- Use a specific, non-overlapping path prefix (`/cache/iiif/nli/`)
- Test with `nginx -t` before reloading
- Use `alias` not `root` (alias replaces the match, root appends)
- The deployment guide warns: "502 only on /api/ routes -- Check nginx has NO separate location /api/ block." Apply same discipline to the new block
- Test the full route matrix: `/cache/iiif/nli/` serves files, `/api/nli_image_by_sysid/` still reaches Python, `/` still reaches NiceGUI

**Phase:** Serving (nginx configuration)

---

### Pitfall 14: rsync/scp From Home Machine Is Fragile for 86GB

**What goes wrong:** The batch fetcher runs on a home machine, but transferring 86GB to EC2 via rsync takes hours and is interrupted by network drops. You end up with a partial transfer and no way to verify completeness.

**Prevention:**
- Use `rsync --checksum --partial --progress` for resumable, verified transfers
- Transfer in batches (e.g., one library_code at a time: CUL first = ~40GB, then smaller libraries)
- Verify after transfer: compare file count and total size between source and destination
- Consider compressing the transfer: tar+ssh avoids per-file overhead for many small files

**Phase:** Implementation (transfer pipeline)

---

### Pitfall 15: Placeholder Image Detection Regression

**What goes wrong:** NLI returns a valid 200 response but with a tiny placeholder image (< 5KB). This gets cached as a "valid" image, and the cache permanently serves a blank/broken thumbnail for that manuscript.

**Prevention:** The existing pattern in `web/api.py` already checks `len(resp.content) > 5000` (or 1000 for thumbnails). Apply the same threshold in the batch fetcher. Log rejected images separately for manual review.

**Phase:** Implementation (batch fetcher validation)

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Investigation | Assuming EC2 can fetch from NLI (#1) | Test from actual IP first. Plan residential-IP strategy. Contact NLI. |
| Investigation | Underestimating legal/TOS risk (#5) | Reach out to NLI before any automated fetching |
| Investigation | Wrong storage estimate (#11) | Sample 1000 diverse images before committing |
| Investigation | Not testing other IIIF providers from EC2 | curl-test Cambridge/Manchester/DPUL from server before scoping |
| Implementation (batch) | Inode exhaustion (#2) | Choose hierarchical dirs or blob DB, check `df -i` first |
| Implementation (batch) | Using FGP numbers instead of FL IDs (#6) | Use manifest resolution, not crossref FGPImageNumberId |
| Implementation (batch) | No checkpoint/resume (#10) | Implement from day one, not as an afterthought |
| Implementation (batch) | Fetching multiple resolutions (#12) | Enforce single resolution (800px) in fetcher config |
| Implementation (integration) | Codepath fragmentation (#9) | Build unified image_cache_service.py before touching any endpoints |
| Implementation (integration) | Partial cache UX confusion (#4) | Priority-order population + cache coverage indicator |
| Serving | Starving web app of I/O (#3) | nginx static serving + Cloudflare CDN caching |
| Serving | nginx route collision (#13) | Specific path prefix, `nginx -t`, full route matrix test |
| Serving | Stale images from re-digitization (#7) | Staleness tracking + manual refresh button |
| Desktop | 86GB download UX disaster (#8) | Tiered downloads or organic-only caching |

## Sources

- Codebase: `docs/archive/plans/PUZZLE_IIIF_SERVER_SIDE_PROCESSING.md` -- verified NLI IP blocking behavior (tested 2026-03-17)
- Codebase: `web/api.py` -- current image proxy endpoints, NLI session management, placeholder detection
- Codebase: `shared/puzzle_image_service.py` -- existing puzzle derivative cache pattern
- Codebase: `CLAUDE.md` / `PROJECT.md` -- Phase 30 lesson: "FGPImageNumberId is a Friedberg photo number, NOT an NLI IIIF FL ID"
- Codebase: `docs/guides/DEPLOYMENT_TECHNICAL.md` -- EC2 architecture, nginx configuration
- [NLI IIIF developer portal](http://iiif.nli.org.il/) -- NLI's IIIF API documentation
- [IIIF discuss: Putting a cache in front of IIIF image servers](https://groups.google.com/g/iiif-discuss/c/xAPZIkHb6ds) -- community discussion on IIIF caching architecture
- [IIIF Change Discovery API 0.3](https://iiif.io/api/discovery/0.3/) -- mechanism for detecting changed manifests
- [IIIF Image API 2.1](https://iiif.io/api/image/2.1/) -- NLI uses IIIFv21, canonical URI structure for caching
- [Museum-digital scraping impact (2025)](https://blog.museum-digital.org/2025/12/09/updates-ai-scrapers-and-resilience/) -- real-world example of aggressive scraping destabilizing a digital library
- [AWS EC2 inode exhaustion](https://copyprogramming.com/howto/ec2-instance-on-amazon-and-i-am-greeted-with-no-space-left-on-the-disk) -- EBS volume inode limits with many small files
- [Nginx static content serving](https://docs.nginx.com/nginx/admin-guide/web-server/serving-static-content/) -- sendfile + open_file_cache for efficient image serving
