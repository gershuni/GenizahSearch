# Requirements: GenizahSearch v7.8

**Defined:** 2026-04-03
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.8 Requirements

Requirements for server-side image cache milestone. 3 phases: Investigation, Fetch Pipeline, Integration.

### Investigation & Validation

- [ ] **INV-01**: Rate limit testing from residential IP confirms safe NLI fetch rate (requests/sec) without triggering blocks
- [ ] **INV-02**: Storage validation samples 1000+ NLI images at target resolution for actual average file size; determines NLI-only subset (manuscripts without CUL/Oxford/JTS/Manchester image alternatives)
- [ ] **INV-03**: EC2 filesystem verified for 815K+ files; hierarchical directory structure designed (2-3 level hash)
- [ ] **INV-04**: NLI contacted about TOS — hard go/no-go gate before large-scale fetch begins
- [ ] **INV-05**: Target image resolution decided (800px minimum, potentially higher) based on storage/quality tradeoff

### Batch Fetcher & Transfer

- [ ] **FETCH-01**: Batch fetch script runs from residential IP with ramp-up rate and exponential backoff
- [ ] **FETCH-02**: Priority ordering fetches NLI-only manuscripts first; cache-first rollout gated on 90%+ coverage of NLI-only corpus
- [ ] **FETCH-03**: FL ID resolution via IIIF manifest fetch with persistent sys_id-to-FL-ID mapping DB
- [ ] **FETCH-04**: Resumable fetching with atomic file writes and content validation
- [ ] **FETCH-05**: Rsync pipeline to staging tree on EC2 with atomic promotion to live cache

### Serving & Integration

- [ ] **SERVE-01**: nginx serves cached images via try_files with deterministic paths from hierarchical directory structure
- [ ] **SERVE-02**: Cache miss returns graceful degradation (placeholder/message) — no live NLI fetch from EC2
- [ ] **SERVE-03**: Cache status API endpoint shows coverage %, by library, disk usage, miss rate
- [ ] **WEB-01**: Unified image URL resolver used by all web image-loading codepaths (browse, search, puzzle, reading desk, fullscreen)
- [ ] **WEB-02**: Web loads from server cache first, falls back to client-side IIIF for non-NLI sources
- [ ] **WEB-03**: Uncached NLI images during outage show clear "image unavailable" state
- [ ] **DESK-01**: Desktop app tries server cache URL before direct NLI IIIF fetch
- [ ] **DESK-02**: Desktop maintains incremental local cache of viewed manuscripts with configurable size quota and LRU eviction

## Future Requirements

Deferred to future milestone:

- **CACHE-F01**: Cache Cambridge/Manchester/JTS images (lower priority — these servers are more reliable)
- **CACHE-F02**: Admin web UI for cache status and batch job progress
- **CACHE-F03**: Cache invalidation when NLI re-digitizes manuscripts

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full 86GB desktop download | Unrealistic for most users; incremental cache is sufficient |
| EC2 read-through live fetch from NLI | NLI blocks datacenter IPs; fundamentally infeasible |
| Multiple cached resolutions per image | Storage multiplier; cache one master, derive thumbnails locally |
| CDN/S3 for image serving | Adds cost and complexity; nginx static serving is sufficient for current scale |

## Traceability

Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| INV-01 | — | Pending |
| INV-02 | — | Pending |
| INV-03 | — | Pending |
| INV-04 | — | Pending |
| INV-05 | — | Pending |
| FETCH-01 | — | Pending |
| FETCH-02 | — | Pending |
| FETCH-03 | — | Pending |
| FETCH-04 | — | Pending |
| FETCH-05 | — | Pending |
| SERVE-01 | — | Pending |
| SERVE-02 | — | Pending |
| SERVE-03 | — | Pending |
| WEB-01 | — | Pending |
| WEB-02 | — | Pending |
| WEB-03 | — | Pending |
| DESK-01 | — | Pending |
| DESK-02 | — | Pending |

**Coverage:**
- v7.8 requirements: 18 total
- Mapped to phases: 0 (pending roadmap)
- Unmapped: 18

---
*Requirements defined: 2026-04-03*
*Last updated: 2026-04-03 after cross-AI review (Gemini + Codex, 2 rounds)*
