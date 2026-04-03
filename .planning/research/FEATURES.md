# Feature Landscape: Server-Side IIIF Image Cache

**Domain:** Image caching infrastructure for a Cairo Genizah manuscript research platform
**Researched:** 2026-04-03
**Overall confidence:** HIGH (based on existing codebase patterns and IIIF ecosystem research)

## Table Stakes

Features users expect from an image cache. Missing = the cache feels unreliable or pointless.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Cache-first image serving | Core value prop -- if users still hit NLI on every request, cache is wasted | Low | Modify `/api/nli_image_by_sysid` to check disk before IIIF fetch |
| Transparent IIIF fallback | Cache misses must still work; can't break images for uncached manuscripts | Low | Existing code already fetches IIIF; add cache-check layer in front |
| Per-FL-ID disk storage | Each manuscript page cached as individual file, keyed by FL ID + resolution | Low | Proven pattern in puzzle derivative cache (`{fl_id}_{size}_original.jpg`) |
| Batch fetching script | Manual or cron-triggered script that walks NLI crossref DB and fetches missing images | Medium | Core workhorse; needs rate limiting, resumability, progress logging |
| Rate limiting (polite fetcher) | NLI has no published rate limit; aggressive fetching risks IP ban | Low | 1-2 req/sec with exponential backoff on 429/503; standard crawler etiquette |
| Priority: NLI-only manuscripts first | ~82% of multi-IE manuscripts have no CUL/Oxford/Manchester/JTS alternative | Low | Query nli_crossref.db for sys_ids WITHOUT alternative-source matches |
| Progress tracking (batch) | Long-running batch jobs (815K images) need visibility into completion | Low | SQLite tracking table or JSON manifest: total/done/failed/skipped counts |
| Deterministic file paths | Predictable path from FL ID so nginx can serve directly without Python | Low | `cache/nli/{prefix}/{fl_id}_{width}.jpg` with directory sharding |

## Differentiators

Features that set the cache apart from "just a CDN proxy." Not expected, but valued.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Nginx direct-serve (bypass Python) | 10-100x faster than proxying through NiceGUI; nginx serves static JPEGs directly | Medium | `try_files` with fallback to Python endpoint for cache misses |
| Read-through caching | When a user browses an uncached image, the served response also writes to disk cache | Medium | Already implemented for puzzle derivatives; extends cache organically |
| Desktop offline bundle download | Researchers download image set for offline fieldwork/travel | High | Incremental download; needs download manager UI in PyQt6, progress tracking, pause/resume |
| Multi-resolution cache tiers | Store 800px (browse) + 1200px (reading/analysis) on demand | Medium | 800px as default batch-fetched tier; 1200px fetched on-demand when user zooms |
| Cache warming by popularity | Prioritize manuscripts with highest PostHog page views | Low | Export PostHog browse events, sort sys_ids by view count, feed to batch fetcher |
| Incremental cache warming (cron) | Nightly/weekly job that fetches N images per run, eventually reaching full coverage | Low | Systemd timer or cron running batch script with configurable rate and count limits |
| Cache health dashboard | Admin view showing cache coverage stats: total cached, by library, by priority tier | Low | Simple `/admin/cache-stats` endpoint querying the tracking table |
| Desktop selective download | Download images only for manuscripts in user's saved lists | Medium | Filter by list sys_ids; much smaller download than full corpus |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Full-resolution cache (2000px+) | 815K images at 2000px = ~400-600GB; exceeds 150GB budget, diminishing returns vs IIIF tile-based zoom | Cache at 1200px max; let IIIF handle full-resolution zoom on demand |
| Real-time cache invalidation via webhooks | IIIF manifests for historical manuscripts essentially never change | Simple TTL-based staleness (90+ days); manual purge command for exceptional cases |
| CDN/S3 for cached images | Adds cost and complexity; EC2 local disk is free and fast for single-server architecture | Nginx serves from local disk; revisit only if multi-server or bandwidth becomes an issue |
| Cache ALL libraries (Cambridge, Oxford, etc.) | Cambridge CUDL, Oxford, Manchester, JTS servers are reliable; caching them wastes disk for no reliability gain | Cache NLI-sourced images only; other libraries serve as natural fallback already |
| Desktop P2P image sharing | Complexity of peer discovery, NAT traversal, trust model far exceeds benefit | Desktop downloads from server cache (single source of truth) |
| Image format conversion (WebP/AVIF) | Adds processing complexity; JPEG is universally supported and already compressed for manuscripts | Serve original JPEG from NLI; format optimization is a separate future concern |
| Puzzle-style background removal on cached images | Cached images are for browse/reading, not puzzle assembly; BG removal is puzzle-specific | Keep puzzle derivative cache separate; browse cache stores unprocessed originals |
| Per-user cache quotas | Single researcher platform, not multi-tenant SaaS; all users benefit from same cache | Cache is shared, global, server-side; no per-user accounting needed |
| Admin web UI for cache management | Over-engineering for single-server deployment | CLI scripts for status, retry, cleanup are sufficient |

## Resolution Analysis

Critical decision: what width to cache at. Tested with T-S 12.1 (a typical Genizah fragment).

| Resolution | File Size (est.) | Total for 815K | Quality for Manuscripts | Use Case |
|------------|-----------------|----------------|------------------------|----------|
| 400px | ~25KB | ~20GB | Thumbnails only; text illegible | Search result previews (already via existing proxy) |
| **800px** | **~105KB** | **~86GB** | **Bare minimum for reading text**; adequate for most browse | **Default browse; fits 150GB budget with 64GB headroom** |
| 1200px | ~200KB | ~163GB | Good reading quality; scholarly work comfortable | Exceeds 150GB for full corpus; viable for priority subset or on-demand |
| 1600px | ~350KB | ~285GB | Excellent quality; diminishing returns vs 1200px | Not feasible on current storage |
| 2000px | ~500KB | ~408GB | Current live-fetch default; full IIIF quality | Keep as live-fetch only |

**Recommendation: 800px as batch-cached tier, 1200px on-demand.**

Rationale:
- 800px fits the 150GB budget for all 815K NLI images with 64GB headroom for growth
- 800px is proven readable for Genizah manuscripts (tested in puzzle cache at this size)
- 1200px can be fetched on-demand and cached when a user actually views a manuscript in reading desk or zooms; this incrementally builds a higher-quality tier without upfront storage commitment
- The existing `/api/nli_image_by_sysid` defaults to `width=2000` for live fetch; cached 800px replaces this as the fast-path default, with 2000px still available via live IIIF when user needs full zoom
- The puzzle cache already uses 800px (`SIZE_PRESETS['medium'] = 800`) confirming this as the project's established "good enough" resolution

## Feature Dependencies

```
nli_crossref.db (sys_id list) ---> Batch fetcher (knows WHAT to cache)
                                   |
NLI IIIF manifest fetch ---------> FL ID resolution (knows HOW to cache)
                                   |
                                   v
Disk cache (deterministic paths) -> Nginx try_files (serves cached)
                                   |
                                   +-> Python fallback (fetches + caches on miss)
                                   |
                                   v
Cache tracking table/manifest ----> Progress / admin dashboard
                                   |
                                   v
Desktop download endpoint -------> PyQt6 download manager UI
```

Key dependency chain:
1. **Deterministic file path scheme** -- foundation for everything; must be decided first
2. **Batch fetcher** depends on: nli_crossref.db sys_id list + NLI manifest FL ID resolution + rate limiter
3. **Cache-first API** depends on: deterministic file path scheme + disk cache (even partially populated)
4. **Nginx direct-serve** depends on: deterministic file path scheme + nginx config
5. **Desktop offline bundle** depends on: server cache substantially populated + download API endpoint
6. **Priority ordering** depends on: nli_crossref.db `get_image_sources()` queries + optionally PostHog data

**Critical note on FL ID resolution:** nli_crossref.db `FGPImageNumberId` is NOT the same as IIIF FL ID (learned in Phase 30). The batch fetcher must still hit NLI IIIF manifests to resolve FL IDs. The crossref identifies WHICH manuscripts to cache; manifest fetch resolves the actual image URLs. The existing `fetch_fl_ids_from_nli()` in `web/api.py` handles this with caching, and the persistent FL ID cache (`nli_fl_ids_cache.json`) survives server restarts.

## MVP Recommendation

Prioritize (Phase 1 -- gets immediate reliability value):
1. **Investigation** -- sample NLI rate limits from EC2 IP, validate 800px across diverse manuscripts, confirm nginx try_files
2. **Deterministic file path scheme** -- foundation for everything else
3. **Cache-first `/api/nli_image_by_sysid`** -- read-through cache; every browse populates cache
4. **Batch fetcher script with rate limiting** -- cron job that walks NLI-only manuscripts first
5. **Progress tracking** -- SQLite table for batch job status

Defer to Phase 2:
- **Nginx direct-serve** -- optimization; Python serving is adequate initially
- **Multi-resolution tiers** -- 800px only in Phase 1
- **Cache warming by popularity** -- sequential walk by library priority is good enough initially

Defer to Phase 3:
- **Desktop offline bundle** -- needs substantial cache population first; complex UI work
- **Desktop selective download** -- depends on offline bundle infrastructure
- **Admin dashboard** -- CLI progress output suffices at first

## Priority Ordering for Batch Fetch

Based on library distribution (255K total records) and alternative image source availability:

| Priority | Library/Category | Count (est.) | Rationale |
|----------|-----------------|-------------|-----------|
| P0 | NLI-only manuscripts (no CUL/Oxford/Manchester/JTS) | ~70-80K | No alternative; NLI downtime = complete blackout |
| P1 | BL (British Library) via NLI | ~8K | NLI is sole image source for BL Genizah |
| P2 | RNL (National Library of Russia) via NLI | ~17K | NLI is primary; no CUDL/LUNA fallback |
| P3 | AIU, Mosseri, Gaster, smaller collections via NLI | ~15K | NLI-only; lower traffic but still vulnerable |
| P4 | CUL manuscripts (NLI copies) | ~128K | Cambridge CUDL available as reliable fallback |
| P5 | JTS, Oxford, Manchester (NLI copies) | ~55K | DPUL/Bodleian/LUNA available as reliable fallback |

**P0-P3 are the high-impact targets (~100-120K images, ~10-13GB at 800px).** These should be fully cached before starting P4-P5.

## Existing Infrastructure to Leverage

| Component | How It Helps | Confidence |
|-----------|-------------|------------|
| `shared/puzzle_image_service.py` | Proven disk cache with `_safe_filename()`, `get_cache_path()`, size presets (800px = 'medium') | HIGH |
| `web/api.py` `_nli_session` | Persistent requests.Session with connection pooling and semaphore cap (8 concurrent) | HIGH |
| `shared/nli_crossref_service.py` | 815K NLI image records; `get_image_sources()` identifies alternative-source availability | HIGH |
| `web/api.py` `fetch_fl_ids_from_nli()` | FL ID resolution from IIIF manifests with in-memory + disk-persistent caching | HIGH |
| `NLI_PERSISTENT_CACHE_FILE` | Disk-persisted FL ID cache survives server restarts (30-day TTL) | HIGH |
| Nginx reverse proxy | Already configured (port 80/443 -> 8081); `try_files` for static paths is straightforward | HIGH |
| PostHog analytics | Browse page view events for popularity-based prioritization | MEDIUM |
| `web/api.py` `_image_cache` | In-memory image cache (10-min TTL) -- can be replaced with disk cache lookup | HIGH |

## Sources

- Existing codebase: `shared/puzzle_image_service.py`, `web/api.py`, `shared/nli_crossref_service.py`
- SEED-001 exploration notes: storage estimates, investigation questions
- [Cantaloupe IIIF Server Caching](https://cantaloupe-project.github.io/manual/3.4/caching.html) -- derivative cache, HTTP headers, tile seeding
- [IIIF Implementer Guide](https://iiif.io/guides/guide_for_implementers/) -- cache headers, design principles
- [IIIF-Crawler](https://github.com/Jean-Baptiste-Camps/IIIF-Crawler) -- batch IIIF download patterns
- [NLI Developer Portal](http://iiif.nli.org.il/) -- NLI IIIF API (no published rate limits found)
- [Nginx Content Caching](https://docs.nginx.com/nginx/admin-guide/content-cache/content-caching/) -- try_files, proxy_cache
- Phase 60 plan notes: NLI-only manuscripts = ~82% of multi-IE set
- Key lesson (Phase 30): FGPImageNumberId != IIIF FL ID -- different numbering systems
