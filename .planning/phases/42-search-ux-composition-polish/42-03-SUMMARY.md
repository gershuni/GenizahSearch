---
phase: 42-search-ux-composition-polish
plan: 03
subsystem: ui
tags: [fjms, printed-badge, fragment-material, enrichment, batch-lookup]

# Dependency graph
requires:
  - phase: 25-fjms-integration
    provides: "fjms_enrichment.db sidecar with catalog_fields table"
  - phase: 42-01
    provides: "search progress instrumentation (enrichment patterns)"
provides:
  - "get_printed_sys_ids() batch lookup method for FragmentMaterial=Printed"
  - "Printed badge in web search, parallels, catalog browse result views"
  - "Printed badge in desktop search results table and composition tree"
  - "PrintedBadgeWorker async worker for desktop"
  - "PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE constants"
affects: [filtered-search, desktop-composition, catalog-browse]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Batch printed status lookup via catalog_fields FieldCategory=FragmentMaterial"
    - "Parallel enrichment queries (asyncio.gather for web, QThread worker for desktop)"

key-files:
  created: []
  modified:
    - shared/fjms_service.py
    - web/pages/search.py
    - web/pages/parallels.py
    - web/pages/catalog_browse.py
    - genizah_app.py
    - gui_threads.py

key-decisions:
  - "Used set return type for get_printed_sys_ids (boolean check only, simpler than dict)"
  - "Red attention color (#fee2e2 bg, #dc2626 text) for printed badge to stand out against purple domain chips"
  - "Desktop: COL_PRINTED as dedicated column (index 11) with async worker, matching PGP badge pattern"
  - "Composition: [Printed] prefix on title column with red foreground instead of new tree column"

patterns-established:
  - "Printed badge pattern: batch lookup in parallel with domain enrichment, red chip in all result views"

requirements-completed: [UX-07]

# Metrics
duration: 17min
completed: 2026-03-01
---

# Phase 42 Plan 03: Printed Badge Summary

**Red-tinted Printed badge across all result views using FragmentMaterial=Printed from catalog_fields (12,421 AlmaIds)**

## Performance

- **Duration:** 17 min
- **Started:** 2026-03-01T14:19:22Z
- **Completed:** 2026-03-01T14:36:22Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- New `get_printed_sys_ids()` batch method on FjmsService queries catalog_fields for FragmentMaterial=Printed
- Web app: Printed badge in search results, advanced view, parallels manuscript groups, and catalog browse (table + detail)
- Desktop app: Printed column in search results table + [Printed] label in composition tree title column
- All lookups run in parallel with existing domain enrichment for zero added latency

## Task Commits

Each task was committed atomically:

1. **Task 1: Service layer -- get_printed_sys_ids batch method** - `4926a198` (feat)
2. **Task 2: Web app -- Printed badge on search, parallels, and catalog browse** - `e258de94` (feat)
3. **Task 3: Desktop app -- Printed label in search and composition results** - `eed0f88e` (feat)

## Files Created/Modified
- `shared/fjms_service.py` - New get_printed_sys_ids() method + PRINTED_BADGE_COLORS/LABEL constants
- `web/pages/search.py` - Printed chip in result cards + advanced view; parallel enrichment
- `web/pages/parallels.py` - Printed chip in manuscript group header; parallel enrichment
- `web/pages/catalog_browse.py` - Printed q-badge in shelfmark cell + expanded detail; batch lookup in _resolve_all
- `genizah_app.py` - COL_PRINTED column, PrintedBadgeWorker launch, composition printed labels
- `gui_threads.py` - New PrintedBadgeWorker class

## Decisions Made
- Used `set` return type for `get_printed_sys_ids` since only boolean membership check needed (not dict mapping)
- Chose red attention color (#fee2e2/#dc2626) to visually distinguish from purple domain chips and green PGP badges
- Desktop uses dedicated column with async worker (matching PGP badge pattern) rather than inline text modification
- Composition tree uses title column prefix since adding a new tree column would require schema changes

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Printed badge complete across all views
- Ready for Phase 42-02 (cancel with partial results) if not yet executed
- Phase 43+ can build on the enrichment parallel patterns established here

## Self-Check: PASSED

All 7 files verified present. All 3 task commits verified in git log.

---
*Phase: 42-search-ux-composition-polish*
*Completed: 2026-03-01*
