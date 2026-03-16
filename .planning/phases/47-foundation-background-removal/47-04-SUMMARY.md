---
phase: 47-foundation-background-removal
plan: 04
subsystem: tooling
tags: [pyqt6, iiif, background-removal, preview, pillow, numpy]

requires:
  - phase: 47-02
    provides: "Background removal engine (shared/background_removal.py)"
  - phase: 47-03
    provides: "Puzzle image service with IIIF fetch and disk cache (shared/puzzle_image_service.py)"
provides:
  - "Interactive visual preview tool for background removal tuning across library types"
  - "Empirical validation of bg removal pipeline on real IIIF manuscript images"
affects: [47-foundation-background-removal]

tech-stack:
  added: []
  patterns: ["PyQt6 split-view preview for image processing validation"]

key-files:
  created:
    - scripts/preview_background_removal.py
  modified: []

key-decisions:
  - "Cambridge IIIF fetched directly (not NLI-hosted), separate code path from NLI FL IDs"
  - "Checkerboard composited via Pillow alpha_composite for transparency visualization"
  - "Threshold slider re-processes from cached original bytes without re-fetching"

patterns-established:
  - "Preview/dev tools live in scripts/ and import from shared/ only, never from web/"

requirements-completed: [BGRM-01, BGRM-02, BGRM-03]

duration: 4min
completed: 2026-03-16
---

# Phase 47 Plan 04: Interactive Background Removal Preview Summary

**PyQt6 split-view preview tool with threshold slider, toggle view, and multi-library IIIF samples for empirical bg removal validation**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-16T03:16:43Z
- **Completed:** 2026-03-16T03:21:00Z
- **Tasks:** 1 of 1 auto tasks (Task 2 is checkpoint:human-verify)
- **Files modified:** 1

## Accomplishments
- Interactive PyQt6 window with original vs background-removed split view
- Threshold slider (5-150) re-processes on the fly without re-fetching images
- Toggle button switches right panel between stripped (with checkerboard transparency) and original
- Sample dropdown with 8 images from NLI (CUL), Oxford, Manchester, and Cambridge
- Info panel showing detected HSV background color, low-saturation fallback status, foreground ratio, and processing time
- Cambridge IIIF handled via direct fetch (separate from NLI pipeline)

## Task Commits

Each task was committed atomically:

1. **Task 1: Interactive background removal preview tool** - `299abd43` (feat)

**Plan metadata:** (pending -- awaiting checkpoint completion)

## Files Created/Modified
- `scripts/preview_background_removal.py` - PyQt6 preview tool with split view, threshold slider, toggle, sample dropdown, and detection info panel

## Decisions Made
- Cambridge images fetched directly via Cambridge IIIF endpoint since they are not NLI-hosted (different URL pattern)
- Checkerboard transparency rendered via Pillow alpha_composite rather than QPainter for simplicity
- Threshold slider triggers immediate re-processing from cached original bytes (no network round-trip)

## Deviations from Plan

- **NLI FL IDs fixed**: Original sample IDs were FGP image numbers (6 digits), not real NLI FL IDs (9+ digits from manifests). Fixed with verified FL IDs from NLI manifests.
- **Manchester LUNA fallback**: NLI returns 503 for Manchester FL IDs. Added Manchester LUNA Size4 direct URL support as fallback with `_load_direct_url()` method.
- **Oxford deferred**: No easy IIIF access without UUID-style image identifiers. Oxford images available through Oxford parts JSON files (noted for future phases).
- **Threshold tuning**: CUL works best at ~115 (higher than default 30). Per-library defaults may be beneficial in future.

## Checkpoint Verification (approved 2026-03-16)

- CUL (NLI IIIF): Background removal works, threshold ~115 optimal
- AIU (NLI IIIF): Background removal works
- Manchester (LUNA direct): Background removal works
- Cambridge (direct IIIF): Background removal works
- **User approved** visual quality across 4 library sources

## Issues Encountered
- NLI IIIF returns 503 for some library FL IDs (Manchester, Oxford) — rate limiting or incomplete coverage.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Preview tool ready for manual visual verification (Task 2 checkpoint)
- User needs to run `python scripts/preview_background_removal.py` and verify bg removal quality across library types
- After checkpoint approval, Phase 47 foundation is complete

## Self-Check: PASSED

- [x] scripts/preview_background_removal.py exists
- [x] Commit 299abd43 found in git log
- [x] All acceptance criteria verified (syntax, imports, widgets, no web/ imports)

---
*Phase: 47-foundation-background-removal*
*Completed: 2026-03-16*
