---
phase: 47-foundation-background-removal
plan: 03
subsystem: imaging
tags: [iiif, cache, background-removal, pillow, puzzle, shared-service]

requires:
  - phase: 47-02
    provides: background_removal.remove_background function (HSV segmentation engine)
provides:
  - PuzzleImageService: IIIF fetch -> background removal -> deterministic disk cache
  - Module-level convenience functions (resolve_fragment_image, get_cache_path, invalidate_cache)
  - Platform-aware cache directory (LOCALAPPDATA on Windows)
  - Original/processed toggle for image mode
affects: [48-canvas-desktop, 49-canvas-web, 50-index-distribution]

tech-stack:
  added: []
  patterns: [iiif-fetch-cache-pipeline, deterministic-cache-key, singleton-service]

key-files:
  created:
    - shared/puzzle_image_service.py
    - tests/test_puzzle_image_service.py
  modified: []

key-decisions:
  - "Cache key = (fl_id, size, threshold) ensures threshold changes produce fresh entries"
  - "Duplicated NLI_IIIF_BASE constant rather than importing from web/services.py to keep shared/ independent of web/"
  - "Fallback to original bytes on background removal failure rather than returning None"

patterns-established:
  - "IIIF fetch-cache pipeline: fetch -> process -> deterministic disk cache"
  - "Platform-aware cache dir: LOCALAPPDATA on Windows, project root otherwise"

requirements-completed: [BGRM-01, BGRM-02, BGRM-03]

duration: 4min
completed: 2026-03-16
---

# Phase 47 Plan 03: Image Resolver/Cache Summary

**PuzzleImageService fetches NLI IIIF images, applies HSV background removal, and caches processed RGBA PNG results to disk with deterministic (fl_id, size, threshold) keys**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-16T03:12:57Z
- **Completed:** 2026-03-16T03:17:00Z
- **Tasks:** 1 (TDD: RED + GREEN)
- **Files modified:** 2

## Accomplishments
- PuzzleImageService with IIIF fetch, background removal, and deterministic disk caching
- Cache keys include fl_id, size, and threshold -- changing any produces a distinct entry
- invalidate_cache supports per-threshold removal or full fl_id invalidation
- Original (unprocessed) mode returns raw JPEG for toggle feature
- Platform-aware cache directory (LOCALAPPDATA on Windows)
- 10 tests covering cache paths, resolution, caching, invalidation, and original mode

## Task Commits

Each task was committed atomically:

1. **Task 1 (RED): Failing tests for puzzle image service** - `474ecd73` (test)
2. **Task 1 (GREEN): Implement puzzle image service** - `401a1937` (feat)

_TDD task: test commit followed by implementation commit._

## Files Created/Modified
- `shared/puzzle_image_service.py` - Shared image resolver/cache service (PuzzleImageService class + singleton + convenience functions)
- `tests/test_puzzle_image_service.py` - 10 tests covering deterministic cache paths, resolution, caching, invalidation, original mode

## Decisions Made
- Duplicated `NLI_IIIF_BASE` constant in shared module rather than importing from `web/services.py` -- keeps `shared/` package independent of `web/` (per Finding 8 in research)
- Cache key format `{safe_fl_id}_{size}_{threshold:.1f}.png` uses 1-decimal precision to avoid float noise while remaining human-readable
- On background removal failure, returns original raw bytes as fallback rather than None -- ensures callers always get displayable content

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- PuzzleImageService ready for consumption by desktop canvas (Phase 48) and web canvas (Phase 49)
- Plans 47-01 (data model + joins.db), 47-02 (background removal engine), and 47-03 (image service + cache) complete the foundation phase
- Remaining: Plan 47-04 (web API endpoints for serving processed images)

---
*Phase: 47-foundation-background-removal*
*Completed: 2026-03-16*
