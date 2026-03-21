---
phase: quick
plan: 260321-tiv
subsystem: ui
tags: [refactoring, filter-panel, image-viewer, javascript, nicegui]

provides:
  - "Shared filter panel component (web/components/filter_panel.py) with 10 reusable functions"
  - "Shared manuscript viewer JS factory (web/static/manuscript_viewer.js) with createManuscriptViewer"
affects: [search, parallels, browse, image-viewer]

tech-stack:
  added: []
  patterns:
    - "Filter panel shared module pattern: pure functions + async recompute + handler factory"
    - "JS viewer factory pattern: createManuscriptViewer(options) with configurable selectors"
    - "Lazy viewer name resolution in handleImageError (prevents stale window refs)"

key-files:
  created:
    - web/components/filter_panel.py
    - web/static/manuscript_viewer.js
  modified:
    - web/pages/search.py
    - web/pages/parallels.py
    - web/pages/browse.py

key-decisions:
  - "Pass lang parameter explicitly to build_*_options rather than calling get_language() inside run.io_bound"
  - "handleImageError uses lazy viewer name string resolution via window[viewerName]"
  - "Fullscreen viewer (fsViewer) and reading desk viewers (rdViewers) NOT extracted -- different lifecycle"
  - "UI layout construction stays in each page -- only data/logic functions shared"
  - "Generation counter added to recompute_filter_count for race condition prevention"

requirements-completed: [REFACTOR-FILTER-PANEL, REFACTOR-IMAGE-VIEWER-JS]

duration: 10min
completed: 2026-03-21
---

# Quick Task 260321-tiv: Extract Shared Filter Panel and Image Viewer

**Extracted ~936 lines of duplicated filter logic and JS viewer code into two shared modules (filter_panel.py + manuscript_viewer.js)**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-21T19:26:39Z
- **Completed:** 2026-03-21T19:36:41Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Created web/components/filter_panel.py with 10 shared functions replacing identical filter logic in search.py and parallels.py
- Created web/static/manuscript_viewer.js with createManuscriptViewer factory, fetchFlIdsFromManifest, and handleImageError
- Removed 936 lines from source files (396 from search.py, 270 from parallels.py, 270 from browse.py)
- Added generation guard to recompute_filter_count preventing out-of-order race conditions
- handleImageError now uses lazy viewer name resolution preventing stale window refs

## Task Commits

1. **Task 1: Extract shared filter panel component** - `1b5b7f49` (refactor)
2. **Task 2: Extract shared manuscript viewer JavaScript** - `6b54a1f4` (refactor)

## Files Created/Modified
- `web/components/filter_panel.py` (NEW, 463 lines) - Shared filter panel logic: build_domain/author/work_options, build_filter_summary, has_active_filters, persist_value, load_filter_state, consume_incoming_filters, recompute_filter_count, create_filter_handlers
- `web/static/manuscript_viewer.js` (NEW, 343 lines) - Shared manuscript viewer: createManuscriptViewer factory, fetchFlIdsFromManifest, handleImageError with lazy viewer resolution
- `web/pages/search.py` (5708 -> 5312, -396 lines) - Imports from filter_panel and uses shared JS
- `web/pages/parallels.py` (3702 -> 3432, -270 lines) - Imports from filter_panel
- `web/pages/browse.py` (4984 -> 4714, -270 lines) - Uses shared manuscript_viewer.js

## Line Count Summary

| File | Before | After | Delta |
|------|--------|-------|-------|
| search.py | 5,708 | 5,312 | -396 |
| parallels.py | 3,702 | 3,432 | -270 |
| browse.py | 4,984 | 4,714 | -270 |
| filter_panel.py | 0 | 463 | +463 |
| manuscript_viewer.js | 0 | 343 | +343 |
| **Total** | **14,394** | **14,264** | **-130 net** |

Duplication eliminated: ~936 lines from source files, centralized into 806 lines of shared code.

## Decisions Made
- Passed `lang` parameter explicitly to build_*_options instead of calling get_language() inside run.io_bound() -- prevents cross-client label drift in multi-user NiceGUI
- handleImageError uses string-based viewer name lookup (window[viewerName]) instead of direct object reference -- prevents stale refs when viewer doesn't exist at handler registration time
- Did NOT extract fullscreen viewer (fsViewer) or reading desk viewers (rdViewers) -- they have different lifecycles (created dynamically inside dialog JS)
- Added generation counter to shared recompute_filter_count for race condition prevention (latent bug fix)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added generation guard to recompute_filter_count**
- **Found during:** Task 1
- **Issue:** Plan noted the race condition (REVIEW P2-4) but original code lacked the guard
- **Fix:** Added _filter_recompute_gen counter, increment before async, check after await
- **Files modified:** web/components/filter_panel.py
- **Committed in:** 1b5b7f49

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential correctness fix for async filter recomputation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Self-Check: PASSED
