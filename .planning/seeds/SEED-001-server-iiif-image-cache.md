---
id: SEED-001
status: dormant
planted: 2026-03-31
planted_during: v7.7 Volume-Aware Browse (Phase 60)
trigger_when: infrastructure/reliability milestone, image performance work, or NLI downtime escalation
scope: Medium
---

# SEED-001: Server-side IIIF image cache — batch-fetch low-res images for reliability

## Why This Matters

NLI IIIF servers (iiif.nli.org.il) are frequently down, breaking image display for all users across both web and desktop apps. Currently every image view requires a live IIIF fetch — if NLI is down, users see nothing. We have ~150GB free on EC2 and could pre-cache low-resolution images (e.g. 400px thumbnails) to serve as primary source with IIIF fallback, rather than the reverse.

The puzzle derivative cache already proves this pattern works at small scale (extension/desktop users seed it organically). This seed is about scaling that to the full corpus proactively.

## When to Surface

**Trigger:** Next infrastructure/reliability milestone, or when NLI downtime becomes a recurring user complaint.

This seed should be presented during `/gsd:new-milestone` when the milestone scope matches any of these conditions:
- Infrastructure, reliability, or performance milestone
- Image loading/viewing improvements
- Offline or degraded-mode features
- Server resource optimization
- User complaints about image availability

## Scope Estimate

**Medium** — Needs investigation first (rate limiting, serving architecture), then a phased implementation. Likely 2-3 phases: investigation/prototype, batch fetcher, serving layer integration.

**Target resolution: 800px width minimum** (IIIF: `/full/800,/0/default.jpg`). Tested with T-S 12.1: 105KB per image, bare minimum for reading manuscript text. ~86GB for 815K NLI images, fits in 150GB with headroom.

## Investigation Needed

1. **Storage estimate**: ~815K NLI images at 800px ≈ ~86GB (tested: T-S 12.1 = 105KB at 800px). Fits in 150GB with ~64GB headroom for other libraries. 800px is the minimum acceptable quality for reading manuscript text.
2. **Rate limiting**: Can we batch-fetch from NLI without getting blocked? What rate is safe? Run from home/university IP?
3. **Serving architecture**: Serve cached images as primary, fall back to live IIIF. Nginx static files or Python endpoint?
4. **Incremental strategy**: Priority by popularity (PostHog page views), library (CUL first = 128K), or sequential?
5. **Cambridge/Manchester/JTS**: These IIIF servers are more reliable but could also benefit from local cache
6. **Cache invalidation**: How often do IIIF manifests change? Do we need versioning?

## Breadcrumbs

Related code and decisions found in the current codebase:

- `docs/specs/PUZZLE_WEB_TECHNICAL_SPEC.md:679-685` — "Long-term recommendation" section with three future options
- `web/api.py` — `fetch_fl_ids_from_nli()` with 60s negative cache (`NLI_FAIL_CACHE_TTL`)
- `shared/puzzle_image_service.py` — IIIF fetch + background removal + disk cache (puzzle derivative cache pattern)
- `shared/nli_crossref_service.py` — NLI crossref service (815K images, FL IDs, metadata)
- `genizah_core.py` — IIIF manifest fetching, `_iiif_manifest_cache`
- `web/pages/browse.py` — Image display, NLI image loading
- `web/pages/search.py` — Thumbnail loading in search results
- `gui_threads.py` — Desktop image fetch threads

## Notes

- The puzzle derivative cache at `web/api.py` already implements HMAC-secured upload + server-side disk cache — same pattern could extend to browse thumbnails
- NLI negative cache (`NLI_FAIL_CACHE_TTL = 60s`) was added in v7.5 to prevent hammering during outages, but doesn't solve the fundamental availability problem
- Desktop app already downloads images on-demand and could contribute to server cache (mentioned in puzzle spec)
- Consider whether Cambridge CUDL images (141K manifests already in nli_crossref.db) need caching too — their servers are generally more reliable
