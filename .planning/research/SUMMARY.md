# Project Research Summary

**Project:** Server-Side IIIF Image Cache (GenizahSearch v7.8)
**Domain:** Image caching infrastructure for manuscript research platform
**Researched:** 2026-04-03
**Confidence:** MEDIUM (strong on architecture/features, significant unknown on NLI access strategy)

## Executive Summary

This project adds a server-side image cache for ~815K NLI manuscript images (~86GB at 800px), served as static files by nginx to eliminate Python overhead and provide resilience against NLI downtime. The architecture is well-understood: flat JPEG files on EC2 disk, sharded by sys_id prefix, with nginx `try_files` serving cache hits and the existing Python proxy handling misses. The stack requires no new major dependencies -- aiohttp for the batch fetcher is the only addition, and it stays isolated in a server-only requirements file.

However, the most critical finding from research is that NLI blocks ALL datacenter IP ranges (AWS, Cloudflare, etc.) at the network level -- not by headers, not by User-Agent, but by IP range. This was verified on 2026-03-17 and is the reason the browser extension architecture exists. This means the batch fetcher CANNOT run from EC2. It must run from a residential IP (home machine or university network) with results transferred to EC2 via rsync. This fundamentally reshapes the project: Phase 1 must validate the residential-IP fetching strategy and NLI rate tolerance before any infrastructure is built, and the project should seriously consider contacting NLI for bulk access permission before systematic downloading begins.

The key risks are: (1) NLI IP blocking invalidating the fetch strategy, (2) legal/TOS exposure from bulk-downloading 815K images without permission, (3) storage estimate uncertainty (86GB is extrapolated from one test image), and (4) six independent image-loading codepaths in the codebase that all need consistent cache integration. Mitigations exist for all of these, but the NLI access question is a hard gate -- if residential IP fetching proves unreliable or gets blocked, the project falls back to organic-only caching (cache images as users browse them), which still delivers value but at a much slower fill rate.

## Key Findings

### Recommended Stack

The stack is deliberately minimal, leveraging existing infrastructure. No new databases, no CDN, no message queue. See [STACK.md](STACK.md) for full rationale.

**Core technologies:**
- **aiohttp** (batch fetcher only): Async HTTP client for high-throughput IIIF downloads -- 2x faster than httpx for pure async workloads. Isolated in `scripts/requirements-batch.txt`, NOT in main requirements.txt
- **Flat JPEG files on disk**: nginx serves directly via sendfile (zero-copy kernel path). SQLite BLOBs would force every image through Python, defeating the purpose
- **nginx try_files**: Static file serving for cache hits, Python fallback for misses. 10K+ req/s vs ~200 req/s through NiceGUI
- **SQLite manifest DB** (cache_status.db): Tracks cache state -- what is cached, when, file size, errors. Enables progress tracking and retry logic
- **2-level directory sharding**: `nli/{prefix}/{sys_id}/page_{N}_{width}.jpg` -- prevents filesystem degradation from 815K files

**Critical version note:** aiohttp 3.10+ required. No changes to main requirements.txt.

### Expected Features

See [FEATURES.md](FEATURES.md) for full landscape including resolution analysis.

**Must have (table stakes):**
- Cache-first image serving (check disk before IIIF fetch)
- Transparent fallback (cache miss still works via existing NLI proxy)
- Batch fetching script with rate limiting and resumability
- Priority ordering: NLI-only manuscripts first (~100-120K images with no alternative source)
- Progress tracking via SQLite manifest

**Should have (differentiators):**
- Nginx direct-serve bypassing Python entirely (10-100x faster)
- Read-through caching (user browse populates cache organically)
- Multi-resolution tiers (800px batch-cached, 1200px on-demand)
- Cache health dashboard for monitoring fill progress

**Defer (v2+):**
- Desktop offline bundle download (complex UX for 86GB, needs substantial cache first)
- Desktop selective download by saved lists
- Cache warming by PostHog popularity data
- Admin web UI for cache management (CLI scripts suffice)

### Architecture Approach

The cache is a transparent layer between IIIF sources and consumers. nginx serves static JPEGs for cache hits; Python handles misses and writes through to disk. A shared `image_cache_service.py` provides path resolution for both web and desktop apps. See [ARCHITECTURE.md](ARCHITECTURE.md) for full component diagram and data flows.

**Major components:**
1. **Batch Fetcher** (`scripts/image_cache_fetcher.py`) -- runs from residential IP, fetches IIIF images, writes to disk, transfers to EC2
2. **Image Cache Service** (`shared/image_cache_service.py`) -- deterministic path resolution from (sys_id, page, width), cache availability checks, shared between web/desktop
3. **nginx static location** (`/cached-images/`) -- serves cached JPEGs directly, bypasses Python, immutable Cache-Control headers
4. **Cache Status DB** (`image_cache/cache_status.db`) -- tracks cached images, fetch errors, progress state
5. **Modified web/api.py** -- cache-through on miss (proxy from NLI AND write to disk simultaneously)

### Critical Pitfalls

See [PITFALLS.md](PITFALLS.md) for all 15 pitfalls with detailed prevention strategies.

1. **NLI blocks all datacenter IPs** -- Batch fetcher CANNOT run from EC2. Must use residential IP with rsync transfer to server. This is a verified hard block, not a rate limit. The entire browser extension was built to work around this. **Investigation phase must validate residential IP strategy before any infrastructure work.**
2. **Legal/TOS risk of bulk downloading** -- 815K systematic downloads may violate NLI terms. Contact NLI proactively to request permission or a bulk export. If denied, fall back to organic-only caching (defensible as server-level browser caching).
3. **Inode exhaustion** -- 815K files can exhaust ext4 inode budget before filling disk. Check `df -i` before starting; use hierarchical directory sharding; consider separate XFS volume.
4. **Six independent image-loading codepaths** -- Browse, search, puzzle, reading desk, fullscreen viewer, and desktop all load images differently. Build unified `image_cache_service.py` BEFORE touching any individual codepath.
5. **Storage estimate uncertainty** -- 86GB extrapolated from one test image. Sample 1000 diverse images during investigation to validate. Set disk alerts at 80%.

## Implications for Roadmap

Based on combined research, the project should have 5 phases with a hard gate after Phase 1.

### Phase 1: Investigation and Validation
**Rationale:** The NLI IP blocking discovery means NOTHING should be built until the fetching strategy is proven. This phase answers three existential questions before any code is written.
**Delivers:** Validated fetch strategy, accurate storage estimates, NLI relationship clarity
**Tasks:**
- Test residential IP fetching from home machine (curl + small batch of 100 images)
- Contact NLI about bulk access permission for academic research tool
- Sample 1000 diverse images to validate 86GB storage estimate
- Check EC2 inode budget (`df -i`) and plan volume strategy
- Test Cambridge/Manchester/DPUL from EC2 (may not be blocked -- would change scope)
**Avoids:** Pitfall 1 (datacenter IP block), Pitfall 5 (TOS risk), Pitfall 11 (storage estimate)
**Gate:** If residential IP is blocked or NLI objects, pivot to organic-only caching (skip Phase 2, go directly to Phase 3 with read-through only)

### Phase 2: Batch Fetcher and Transfer Pipeline
**Rationale:** Once fetching is validated, build the pipeline that populates the cache. Runs from home machine, not EC2. This is the long-running background work (days/weeks).
**Delivers:** Batch fetcher script, rsync transfer pipeline, cache_status.db with progress tracking
**Addresses:** Batch fetching, rate limiting, priority ordering, progress tracking (FEATURES table stakes)
**Uses:** aiohttp (STACK), priority queue pattern (ARCHITECTURE Pattern 3)
**Avoids:** Pitfall 6 (FGP != FL ID -- must resolve via manifest), Pitfall 10 (residential IP rate limits -- conservative 1 req/3sec), Pitfall 15 (placeholder detection)
**Key detail:** Two-pass architecture -- first resolve sys_id to FL IDs via IIIF manifests, then fetch images. Seed from existing `nli_fl_ids_cache.json`.

### Phase 3: Web Integration (Cache-First Serving)
**Rationale:** Can start in parallel with Phase 2 (even a partially populated cache delivers value). This is where users see the benefit.
**Delivers:** Cache-first image serving, nginx static file serving, read-through caching on miss
**Addresses:** Cache-first serving, nginx direct-serve, read-through caching (FEATURES table stakes + differentiators)
**Implements:** Image Cache Service, nginx location block, modified api.py (ARCHITECTURE components 2, 3, 5)
**Avoids:** Pitfall 3 (I/O starvation -- nginx serves, not Python), Pitfall 4 (partial cache UX -- unified fallback chain), Pitfall 9 (codepath fragmentation -- shared service first), Pitfall 13 (nginx route collision -- specific prefix, test matrix)

### Phase 4: Desktop Integration
**Rationale:** Depends on Phase 3 (server endpoints must exist). Desktop benefits from server cache even without local download.
**Delivers:** Desktop tries server cache URL before direct NLI, organic local caching as user browses
**Addresses:** Desktop image loading improvement
**Implements:** Modified genizah_core.py and gui_threads.py (ARCHITECTURE desktop components)
**Note:** Defer bulk desktop download to v2+. Organic caching (save images as viewed) is simpler and proven by puzzle cache pattern.

### Phase 5: Monitoring and Polish
**Rationale:** After cache is serving images, add observability and operational tooling.
**Delivers:** Cache status endpoint, admin monitoring, documentation
**Addresses:** Cache health dashboard (FEATURES differentiator)

### Phase Ordering Rationale

- **Investigation MUST come first** because the NLI IP block is a project-killing constraint. Building any infrastructure before validating the fetch strategy risks wasted work.
- **Batch fetcher before web integration** because cache-first serving is most valuable with a populated cache. However, Phase 3 can START in parallel once Phase 2 is running -- read-through caching works with zero pre-populated images.
- **Web before desktop** because desktop depends on server endpoints (`/cached-images/` URL), and web has higher traffic impact.
- **Desktop bulk download deferred** because 86GB is a UX disaster (Pitfall 8) and organic caching is proven. Revisit when cache is substantially populated and user demand is clear.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 1 (Investigation):** This IS the research phase. No additional `/gsd:research-phase` needed -- the tasks are empirical validation (curl tests, NLI outreach, image sampling).
- **Phase 2 (Batch Fetcher):** Needs research on optimal rsync/transfer patterns for 86GB of small files. Also needs FL ID resolution strategy (two-pass vs. inline).

Phases with standard patterns (skip research-phase):
- **Phase 3 (Web Integration):** Well-documented nginx `try_files` pattern, existing puzzle cache proves the disk-cache approach, shared service layer is established project pattern.
- **Phase 4 (Desktop Integration):** Minimal changes -- URL prefix swap in existing image loading threads. Follows established `gui_threads.py` patterns.
- **Phase 5 (Monitoring):** Standard SQLite queries exposed via JSON endpoint. Trivial.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Minimal new dependencies; aiohttp well-benchmarked; nginx static serving is textbook |
| Features | HIGH | Feature landscape well-mapped against existing codebase; resolution analysis grounded in real test data |
| Architecture | HIGH | Follows established project patterns (shared service layer, SQLite sidecars, nginx reverse proxy) |
| Pitfalls | HIGH on #1 (IP block), MEDIUM on #5 (TOS) | NLI IP block is verified; TOS risk is judgment call requiring NLI outreach |

**Overall confidence:** MEDIUM -- the architecture and implementation patterns are solid, but the fundamental feasibility depends on NLI access strategy (residential IP + permission), which is unvalidated.

### Gaps to Address

- **NLI rate limits from residential IPs:** Unknown. Must be tested empirically during Phase 1. Conservative assumption: 1 req/3sec.
- **Actual average image size:** 105KB is one data point. Need 1000-image sample to confirm 86GB estimate fits 150GB budget.
- **NLI's position on bulk academic caching:** Unknown. Outreach during Phase 1 may yield permission, a bulk export, or a cease-and-desist. Plan for all three outcomes.
- **Cambridge/Manchester/DPUL server-side accessibility:** Not tested from EC2. If these are NOT blocked (likely -- they don't show the same anti-scraping behavior as NLI), caching them from EC2 is straightforward and expands the cache beyond NLI-only.
- **EC2 inode budget:** Not checked. Must verify during Phase 1 before committing to flat-file approach.

## Sources

### Primary (HIGH confidence)
- Existing codebase: `web/api.py`, `shared/puzzle_image_service.py`, `shared/nli_crossref_service.py`, `web/services.py`
- `docs/archive/plans/PUZZLE_IIIF_SERVER_SIDE_PROCESSING.md` -- verified NLI IP blocking (2026-03-17)
- nginx official documentation -- static content serving, `try_files`, `alias` directives
- IIIF Image API 2.1 specification -- canonical URI structure

### Secondary (MEDIUM confidence)
- aiohttp vs httpx benchmarks (multiple sources, Oct 2024)
- IIIF community discussions on caching architecture
- Storage estimate: 815K x 105KB = ~86GB (single test image extrapolation)

### Tertiary (LOW confidence)
- NLI rate limit tolerance from residential IPs (untested assumption)
- NLI TOS position on academic bulk caching (unknown, requires outreach)

---
*Research completed: 2026-04-03*
*Ready for roadmap: yes (pending Phase 1 validation gate)*
