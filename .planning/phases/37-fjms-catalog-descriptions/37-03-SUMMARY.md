---
phase: 37-fjms-catalog-descriptions
plan: 03
subsystem: ui
tags: [nicegui, fjms, catalog, search, batch-enrichment, sqlite]

# Dependency graph
requires:
  - phase: 37-01
    provides: "catalog_dialog.py component and get_catalog_record_counts batch method"
provides:
  - "Catalog Records (N) button on search result cards"
  - "Batch catalog count enrichment in search execution flow"
affects: [search, fjms]

# Tech tracking
tech-stack:
  added: []
  patterns: ["visible-when-data-exists for search cards (vs always-visible-disabled on browse)"]

key-files:
  created: []
  modified: ["web/pages/search.py"]

key-decisions:
  - "Separate io_bound call for catalog counts (not combined with domain fetch) for minimal code change"
  - "Visible-only-when-data-exists pattern for search cards (not always-visible-disabled like browse)"
  - "Lazy-load full records on button click (only counts pre-fetched in batch)"

patterns-established:
  - "Search card enrichment: batch counts in execute_search, lazy detail fetch on click"
  - "Closure factory pattern for async click handlers in NiceGUI render loops"

requirements-completed: [FJMS-02]

# Metrics
duration: 3min
completed: 2026-02-17
---

# Phase 37 Plan 03: Search Catalog Records Button Summary

**Batch catalog count enrichment in search flow with "Catalog Records (N)" button on result cards, lazy-loading full descriptions on click**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-17T11:07:04Z
- **Completed:** 2026-02-17T11:10:50Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Added `catalog_record_counts` state tracking with batch fetch during search enrichment
- Search result cards show "Catalog Records (N)" button (purple styling) when FJMS catalog data exists
- Clicking the button lazy-loads full catalog records and opens the dialog from Plan 37-01
- No performance regression -- batch count query runs alongside existing domain enrichment

## Task Commits

Each task was committed atomically:

1. **Task 1: Add batch catalog count enrichment to search flow** - `92444004` (feat)
2. **Task 2: Add catalog records button to search result cards** - `1cffe66f` (feat)

**Plan metadata:** (pending)

## Files Created/Modified
- `web/pages/search.py` - Added catalog_record_counts to SearchUIState, batch enrichment in execute_search, and "Catalog Records (N)" button in create_result_card

## Decisions Made
- **Separate io_bound call for catalog counts:** Rather than combining with the existing domain fetch into a single io_bound call (which would modify more existing code), added a separate parallel `run.io_bound` call. Cleaner separation, minimal code change.
- **Visible-only-when-data-exists for search cards:** Unlike the browse page (which uses always-visible-disabled pattern), search result cards only show the catalog button when `cat_count > 0`. With up to 200 results, disabled buttons would be too noisy.
- **Lazy-load full records on click:** Only batch-fetched counts (integers) during search enrichment. Full catalog records are fetched via `io_bound` only when user clicks the button, keeping the search flow fast.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- 3 pre-existing test failures detected (KTIV button styling, 2 responsa explosion guard tests) -- all confirmed pre-existing and unrelated to this plan's changes.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 37 complete (all 3 plans finished)
- FJMS catalog records integrated in browse page (37-02) and search results (37-03)
- Ready for Phase 38 or milestone completion

## Self-Check: PASSED

- FOUND: web/pages/search.py
- FOUND: commit 92444004 (Task 1)
- FOUND: commit 1cffe66f (Task 2)
- catalog_record_counts: 6 occurrences in search.py (init, clear, fetch, populate, 2x button usage)
- create_catalog_records_dialog: referenced in search.py click handler
- get_catalog_record_counts: called in batch enrichment

---
*Phase: 37-fjms-catalog-descriptions*
*Completed: 2026-02-17*
