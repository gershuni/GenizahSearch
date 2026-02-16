---
phase: 33-metadata-enrichment
plan: 07
subsystem: desktop-ui
tags: [pyqt6, enrichment, oxford, nli-crossref, cache-bug]

# Dependency graph
requires:
  - phase: 33-05
    provides: _browse_load_part EnrichMetadataThread startup (had cache-first bug)
  - phase: 33-06
    provides: browse_render_page nli_cache read for catalog_entry/IsNotGenizah badge
provides:
  - Unconditional EnrichMetadataThread startup in _browse_load_part and browse_navigate
  - Oxford manuscripts receive full NLI crossref enrichment (catalog_entry, is_not_genizah, bibliography)
affects: [desktop-browse, metadata-enrichment]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "ALWAYS start EnrichMetadataThread unconditionally; enrich_metadata handles caching internally"

key-files:
  created: []
  modified:
    - genizah_app.py

key-decisions:
  - "Remove cache-first short-circuit: nli_cache contains basic CSV metadata before enrichment runs, so cache-check is always truthy and prevents enrichment from ever executing"

patterns-established:
  - "Unconditional thread start: EnrichMetadataThread must always start; enrich_metadata() internally handles cache logic and builds enriched metadata on top of basic cached metadata"

# Metrics
duration: 1min
completed: 2026-02-16
---

# Phase 33 Plan 07: Remove Cache-First Short-Circuit Summary

**Removed cache-first short-circuit from _browse_load_part and browse_navigate so Oxford manuscripts receive full NLI crossref enrichment (Neubauer-Cowley catalog, IsNotGenizah badge, bibliography)**

## Performance

- **Duration:** 1 min
- **Started:** 2026-02-16T15:08:30Z
- **Completed:** 2026-02-16T15:09:47Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Removed cache-first short-circuit from `_browse_load_part` that prevented EnrichMetadataThread from starting when basic CSV metadata was already cached
- Removed identical cache-first short-circuit from `browse_navigate` for the same reason
- Both locations now unconditionally start EnrichMetadataThread, matching the pattern already used in `browse_load` (line 19287)
- Closes UAT test 9 gap: Oxford manuscripts now receive full enrichment metadata including Neubauer-Cowley catalog entries, IsNotGenizah badges, and bibliography references

## Task Commits

Each task was committed atomically:

1. **Task 1: Remove cache-first short-circuit from _browse_load_part and browse_navigate** - `2b41c31d` (fix)

## Files Created/Modified
- `genizah_app.py` - Removed cache-first pattern from `_browse_load_part` (line ~19436) and `browse_navigate` (line ~19489), replacing with unconditional EnrichMetadataThread startup

## Decisions Made
- Remove cache-first short-circuit entirely rather than modifying the cache check. The `nli_cache` contains basic CSV metadata (shelfmark, library, titles) which is populated early and is always truthy for Oxford. The `EnrichMetadataThread.enrich_metadata()` method already handles caching internally and builds enriched metadata (NLI crossref, FJMS) on top of the basic cached metadata. The cache check was preventing enrichment from ever running.

## Root Cause Analysis

The bug was introduced in plans 33-05 and 33-06 which added `EnrichMetadataThread` startup to `_browse_load_part` and `browse_navigate` but used a "cache-first" pattern:

```python
cached_meta = self.meta_mgr.nli_cache.get(sid)
if cached_meta:
    self.on_browse_enriched_loaded(sid, cached_meta)  # Called with UN-ENRICHED data
else:
    # EnrichMetadataThread only started when cache EMPTY
```

The problem: `nli_cache` always contains basic CSV metadata for Oxford (populated during initial load from `libraries.csv`). The cache check was always truthy, so `on_browse_enriched_loaded` was called with basic metadata that lacked enrichment fields (`catalog_entry`, `is_not_genizah`, `bibliography`, etc.), and `EnrichMetadataThread` never started.

The fix: Remove the cache check entirely. `EnrichMetadataThread` now starts unconditionally, matching the pattern already used successfully in `browse_load`.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 33 gap closure complete: all 7 plans executed
- Desktop and web browse pages both display full enrichment metadata for all libraries
- Ready for UAT verification of Oxford manuscript enrichment

## Self-Check: PASSED

- FOUND: genizah_app.py
- FOUND: .planning/phases/33-metadata-enrichment/33-07-SUMMARY.md
- FOUND: commit 2b41c31d in git log

---
*Phase: 33-metadata-enrichment*
*Completed: 2026-02-16*
