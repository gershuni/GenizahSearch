---
phase: 31-image-navigation-indicators
plan: 02
subsystem: ui
tags: [folio-navigation, source-indicators, desktop, pyqt6, nli-crossref]

# Dependency graph
requires:
  - phase: 31-image-navigation-indicators
    plan: 01
    provides: "parse_folio_label(), get_folio_images(), source indicator pattern"
  - phase: 30-direct-image-access
    provides: "FL ID resolution from sidecar, enrich_metadata crossref_svc"
provides:
  - "Desktop browse tab folio-labeled page combo (1r, 1v, 2r)"
  - "KTIV viewer button in ManuscriptViewerWidget (opens NLI viewer)"
  - "Folio label display and page count in browse nav bar"
  - "image_source_info and folio_images populated in enrich_metadata cache"
  - "Source combo with page count context in MSViewer"
affects: [32-metadata-display, 33-fragment-relationships]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "KTIV button as styled chip QPushButton with green border matching web NLI chip"
    - "Folio images cached per-manuscript in _browse_folio_images for page combo labels"
    - "Source code inspection test pattern for desktop Qt features (no QApplication)"

key-files:
  created:
    - tests/test_desktop_folio_navigation.py
  modified:
    - genizah_app.py
    - genizah_core.py

key-decisions:
  - "KTIV button styled as chip (green border, 12px radius) matching web NLI source indicator"
  - "Folio labels populated in browse_render_page using _browse_folio_images cached from enrichment"
  - "image_source_info and folio_images added to enrich_metadata in genizah_core.py for both apps"
  - "btn_external made visible when external_url exists (was always hidden before)"
  - "sys_id stored in meta dict during enrich_metadata for downstream KTIV button use"

patterns-established:
  - "NLI crossref import with try/except and _HAS_NLI_CROSSREF flag for graceful degradation"
  - "Enrichment callback stores folio data for later use by page render"

# Metrics
duration: 7min
completed: 2026-02-15
---

# Phase 31 Plan 02: Desktop Folio Navigation & Source Indicators Summary

**Folio-labeled page combo, KTIV viewer button, and source indicator enhancements in desktop browse tab matching web app patterns from Plan 01**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-15T16:33:12Z
- **Completed:** 2026-02-15T16:39:48Z
- **Tasks:** 2
- **Files modified:** 3 (+ 1 created)

## Accomplishments
- Desktop browse page combo shows folio labels (1r, 1v, 2r) from NLI crossref data with fallback to generic numbers
- KTIV button appears in ManuscriptViewerWidget when NLI FGP images available, opens NLI viewer in browser
- Folio label display ("Folio 1r") and page count ("of N pages") added to browse nav bar
- Source combo labels enhanced with "pages" suffix for clarity (e.g., "NLI (4 pages)")
- enrich_metadata in genizah_core.py now populates image_source_info and folio_images for both apps
- btn_external now visible when Cambridge/Oxford external_url available (was hidden before)
- 9 tests verify all desktop folio features without Qt dependency

## Task Commits

Each task was committed atomically:

1. **Task 1: Add folio-labeled page navigation to desktop browse tab** - `43385c34` (feat)
2. **Task 2: Add tests for desktop folio navigation integration** - `867258fd` (test)

## Files Created/Modified
- `genizah_app.py` - NLI crossref import, KTIV button, folio label/count in nav bar, folio-labeled page combo, source combo page count suffix
- `genizah_core.py` - image_source_info and folio_images enrichment in enrich_metadata, sys_id stored in meta
- `tests/test_desktop_folio_navigation.py` - 9 source inspection + functional tests for desktop folio navigation

## Decisions Made
- KTIV button uses same green chip styling (#4caf50 border, 12px radius) as web NLI source indicator for visual consistency
- Folio images stored as `_browse_folio_images` instance variable, set during `on_browse_enriched_loaded`, consumed during `browse_render_page` -- avoids re-querying crossref on every page render
- `btn_external` visibility changed from always-hidden to shown when `external_url` exists -- existing Cambridge/Oxford buttons were never visible; now they appear correctly
- `sys_id` stored in meta dict during `enrich_metadata` so ManuscriptViewerWidget can construct KTIV URL without needing back-reference to the browse tab

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] btn_external was always hidden**
- **Found during:** Task 1
- **Issue:** `self.btn_external.setVisible(False)` was always called regardless of whether external_url existed, meaning Cambridge/Oxford website buttons never appeared
- **Fix:** Changed to `self.btn_external.setVisible(bool(self.external_url))` so buttons show when URLs exist
- **Files modified:** genizah_app.py
- **Verification:** Code inspection confirms correct visibility logic
- **Committed in:** 43385c34 (Task 1 commit)

**2. [Rule 2 - Missing Critical] sys_id not stored in meta dict**
- **Found during:** Task 1
- **Issue:** enrich_metadata built the meta dict from nli_cache but never stored `sys_id` as a field, making it impossible for ManuscriptViewerWidget to construct KTIV URLs
- **Fix:** Added `current_meta['sys_id'] = system_id` in enrich_metadata
- **Files modified:** genizah_core.py
- **Verification:** KTIV button can now resolve sys_id from meta
- **Committed in:** 43385c34 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing critical)
**Impact on plan:** Both fixes necessary for correct button visibility. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 31 complete: both web and desktop have folio navigation and source indicators
- image_source_info available in enrich_metadata for Phase 32 metadata display
- folio_images available for Phase 33 fragment relationships
- All 41 related tests passing (9 new + 32 existing crossref tests)

## Self-Check: PASSED

All files verified present. All commit hashes verified in git log.

---
*Phase: 31-image-navigation-indicators*
*Completed: 2026-02-15*
