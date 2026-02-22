---
phase: quick-15
plan: 01
subsystem: ui
tags: [nicegui, browse, buttons, bibliography, catalog, enrichment]

requires:
  - phase: 29-multi-source
    provides: "NLI crossref bibliography, enrichment deferred loading pattern"
  - phase: 25-fjms
    provides: "FJMS bibliography and catalog data via fjms_service"
provides:
  - "Bibliography and catalog buttons always visible in page navigation pane"
  - "Fixed catalog dialog bug (no longer depends on metadata panel scope)"
affects: [browse, enrichment]

tech-stack:
  added: []
  patterns: ["enrichment_refs deferred population for bib/catalog buttons"]

key-files:
  created: []
  modified:
    - web/pages/browse.py

key-decisions:
  - "Used show_catalog_dialog auto-fallback (fjms_service=None) instead of passing scoped fjms variable -- simpler, no scope dependency"
  - "Placed buttons in div below nav card rather than inside it -- avoids crowding the already-dense navigation toolbar"

patterns-established:
  - "Enrichment-deferred buttons: register container in enrichment_refs, populate via _update_enrichment_sections callback"

requirements-completed: [QUICK-15]

duration: 3min
completed: 2026-02-22
---

# Quick Task 15: Move Bibliography & Catalog Buttons to Page Navigation Pane

**Bibliography FJMS, Bibliography Ktiv, and Catalog Records buttons relocated from hidden metadata panel to always-visible page navigation pane with catalog dialog bug fix**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-22T10:01:08Z
- **Completed:** 2026-02-22T10:04:20Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Moved 3 research tool buttons (Bibliography FJMS, Bibliography Ktiv, Catalog Records) from the expandable metadata panel to the page navigation area
- Fixed bug where Catalog Records button relied on scoped `fjms` variable that could be unavailable
- Wired deferred enrichment loading so buttons appear after Phase B data fetch completes
- Cleaned up unused `get_fjms_service` import from metadata panel section

## Task Commits

Each task was committed atomically:

1. **Task 1: Move bibliography and catalog buttons from metadata panel to page navigation pane** - `28bc9983` (feat)

## Files Created/Modified
- `web/pages/browse.py` - Removed buttons from metadata panel, added `_populate_bib_catalog_buttons` helper, placed deferred container in nav pane area, wired enrichment callback

## Decisions Made
- Used `show_catalog_dialog` auto-fallback (`fjms_service=None`) instead of passing a scoped `fjms` variable -- the catalog dialog already has built-in fallback logic at line 30-32
- Placed buttons in a `div` element below the navigation card rather than inside it to avoid crowding the dense navigation toolbar
- Followed existing enrichment_refs deferred pattern (same as pgp_link, version_selector, joins_button)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Cleanup] Removed unused get_fjms_service import from metadata panel**
- **Found during:** Task 1
- **Issue:** After removing the `fjms = get_fjms_service(thread_safe=True)` line from the metadata panel, the `get_fjms_service` import at line 2293 was no longer used in that scope
- **Fix:** Removed `get_fjms_service` from the import statement, keeping only `merge_catalog_records` and `parse_textual_frame`
- **Files modified:** web/pages/browse.py
- **Committed in:** 28bc9983

---

**Total deviations:** 1 auto-fixed (cleanup)
**Impact on plan:** Minor cleanup, no scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Buttons are now always visible in the browse page navigation area
- Metadata panel still shows FJMS catalog detail, domains, cross-refs, and source names inline
- No breaking changes to any other component

## Self-Check: PASSED

- [x] web/pages/browse.py exists
- [x] 15-SUMMARY.md exists
- [x] Commit 28bc9983 exists in git log

---
*Phase: quick-15*
*Completed: 2026-02-22*
