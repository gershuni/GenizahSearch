---
phase: quick
plan: 260318-jf1
subsystem: images
tags: [manchester, iiif, luna, crossref, folio-navigation]

requires:
  - phase: 34
    provides: "Manchester LUNA crossref data in nli_crossref.db"
provides:
  - "get_manchester_canvases() method for multi-page Manchester image resolution"
  - "Correct recto/verso display for Manchester manuscripts"
affects: [enrich_metadata, manchester_image_endpoint, folio_navigation]

tech-stack:
  added: []
  patterns: ["Direct canvas resolution from crossref (bypassing single-manifest fetch)"]

key-files:
  created: []
  modified:
    - shared/nli_crossref_service.py
    - genizah_core.py
    - tests/test_nli_crossref_service.py

key-decisions:
  - "Build canvas entries directly from crossref images instead of fetching IIIF manifest (each Manchester luna_id has its own manifest with 1 canvas)"
  - "Use sentinel ext_link '__manchester_direct__' to prevent JTS fallback while skipping unnecessary HTTP fetch"

patterns-established:
  - "Direct canvas resolution: when each image has its own manifest, resolve URLs directly from DB rather than fetching N manifests"

requirements-completed: [QUICK-FIX]

duration: 4min
completed: 2026-03-18
---

# Quick Task 260318-jf1: Fix Manchester Image Fetch Summary

**Multi-canvas Manchester image resolution via direct crossref lookup, fixing recto shown for both sides**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-18T12:08:35Z
- **Completed:** 2026-03-18T12:12:24Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added `get_manchester_canvases()` to NliCrossrefService that resolves ALL crossref images to individual IIIF canvas entries with distinct URLs
- Wired the new method into `enrich_metadata()` to bypass the single-manifest fetch that was causing recto to be shown for both sides
- 6 new tests covering recto/verso, multi-leaf, missing luna_id, no images, URL format, and folio_num derivation

## Task Commits

Each task was committed atomically:

1. **Task 1: Add get_manchester_canvases() and test it (TDD)**
   - `f7ff9ec2` (test) RED: add failing tests for get_manchester_canvases
   - `d98065dc` (feat) GREEN: implement get_manchester_canvases() method
2. **Task 2: Wire get_manchester_canvases() into enrich_metadata()** - `500ff460` (fix)

## Files Created/Modified
- `shared/nli_crossref_service.py` - Added `get_manchester_canvases()` method (~30 lines) after `get_manchester_manifest_url()`
- `genizah_core.py` - Replaced Manchester single-manifest block with direct multi-canvas resolution in `enrich_metadata()`
- `tests/test_nli_crossref_service.py` - Added 6 new tests for `get_manchester_canvases()`

## Decisions Made
- **Direct canvas resolution over manifest fetch:** Each Manchester luna_id maps to its own IIIF manifest containing exactly 1 canvas. Rather than fetching N manifests (one per page), we build canvas entries directly from the crossref DB. This is both faster (no HTTP) and correct (each page gets its own URL).
- **Sentinel ext_link pattern:** Using `'__manchester_direct__'` as ext_link prevents JTS/other fallback paths from running while signaling to the `fetch_external_iiif_data` call that no HTTP fetch is needed. This is minimally invasive to the existing control flow.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Pre-existing test failures in `test_puzzle_image_service.py` (cache invalidation), `test_puzzle_model.py` (model fields), and `test_responsa_core.py` (explosion guard) -- all unrelated to this change, confirmed by running without changes.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Manchester manuscripts now display correct multi-page images
- The `web/api.py` `manchester_image` endpoint requires no changes (it already indexes into `images_ext[page]`)
- Both web and desktop apps benefit from the fix since `images_ext` is populated in the shared `enrich_metadata()` method

---
*Quick task: 260318-jf1*
*Completed: 2026-03-18*
