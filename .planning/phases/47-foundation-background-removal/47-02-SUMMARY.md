---
phase: 47-foundation-background-removal
plan: 02
subsystem: image-processing
tags: [pillow, numpy, hsv, background-removal, alpha-channel]

# Dependency graph
requires: []
provides:
  - "HSV-based background removal engine (shared/background_removal.py)"
  - "remove_background(), detect_background_color(), create_mask() API"
  - "Low-saturation fallback for gray/cream backgrounds"
  - "Pillow and numpy declared in requirements.txt"
affects: [47-03, 48-canvas-assembly, fragment-puzzle]

# Tech tracking
tech-stack:
  added: [Pillow, numpy]
  patterns: [HSV-color-segmentation, corner-sampling, morphological-cleanup, value-only-fallback]

key-files:
  created:
    - shared/background_removal.py
    - tests/test_background_removal.py
  modified:
    - requirements.txt

key-decisions:
  - "Pillow HSV 0-255 scale for all channels (not 0-360/0-100)"
  - "Low-saturation threshold S<30 triggers value-only distance fallback"
  - "MIN_FOREGROUND_RATIO=0.05 (5%) for small fragments on large scanning backgrounds"
  - "Morphological cleanup: MinFilter(3) erode then MaxFilter(5) dilate"

patterns-established:
  - "Background removal via HSV color distance from corner-sampled median"
  - "Safety fallback: skip removal when foreground ratio below threshold"
  - "Synthetic Pillow test images with make_test_image() helper"

requirements-completed: [BGRM-01, BGRM-03]

# Metrics
duration: 3min
completed: 2026-03-16
---

# Phase 47 Plan 02: Background Removal Engine Summary

**HSV-based background removal with Pillow/NumPy, low-saturation fallback for gray/cream, corner sampling, and 5% safety threshold**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T03:07:39Z
- **Completed:** 2026-03-16T03:10:06Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Background removal engine strips solid-color scanning backgrounds from manuscript images
- Low-saturation fallback handles gray/cream backgrounds where hue is meaningless
- Safety check prevents over-removal with configurable MIN_FOREGROUND_RATIO (default 5%)
- All 11 tests pass with synthetic test images covering blue, green, gray, cream backgrounds

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Pillow to requirements.txt and install** - `b1a7eb3b` (chore)
2. **Task 2 RED: Failing tests for background removal** - `917e829f` (test)
3. **Task 2 GREEN: Background removal engine implementation** - `1a2725df` (feat)

## Files Created/Modified
- `shared/background_removal.py` - HSV-based background removal engine with remove_background(), detect_background_color(), create_mask()
- `tests/test_background_removal.py` - 11 tests covering solid-color, low-saturation, threshold, safety, output format
- `requirements.txt` - Added Pillow and numpy dependencies

## Decisions Made
- Used Pillow HSV 0-255 scale consistently (not OpenCV 0-180/0-255)
- Low-saturation threshold at S<30 for value-only distance fallback
- MIN_FOREGROUND_RATIO=0.05 (5%) to handle small fragments on large backgrounds
- Morphological cleanup with MinFilter(3) + MaxFilter(5) for noise removal

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed threshold test with gradient image**
- **Found during:** Task 2 (TDD GREEN phase)
- **Issue:** test_threshold_affects_mask used perfectly uniform background, so both tight (5.0) and aggressive (100.0) thresholds removed identical pixels
- **Fix:** Changed test to use gradient border pixels between foreground and background, making threshold sensitivity observable
- **Files modified:** tests/test_background_removal.py
- **Verification:** Test now passes, tight_opaque > aggressive_opaque confirmed
- **Committed in:** 1a2725df (Task 2 GREEN commit)

---

**Total deviations:** 1 auto-fixed (1 bug in test design)
**Impact on plan:** Test fix was necessary for correctness. No scope creep.

## Issues Encountered
None beyond the test fix above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Background removal engine is ready for integration into canvas assembly (Phase 48)
- API accepts image bytes and returns RGBA PNG bytes -- simple to wire into IIIF fetch pipeline
- Threshold and min_foreground_ratio are parameterizable for UI slider controls

---
*Phase: 47-foundation-background-removal*
*Completed: 2026-03-16*
