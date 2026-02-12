---
phase: 26-scientific-joins
plan: 03
subsystem: ui
tags: [fjms, joins, dual-badge, source-merging, deduplication, nicegui, pyqt6]

# Dependency graph
requires:
  - phase: 26-scientific-joins
    plan: 02
    provides: "Deduplicated get_join_group() with aggregated scholar_names and join_types"
provides:
  - "Dual PGP+FJMS badge display for fragments appearing in both join sources (web app)"
  - "Dual PGP, FJMS source text for overlapping entries in desktop joins table"
  - "Sources stored as list instead of string throughout joins pipeline"
  - "3 new tests verifying dual-source badge and merge behavior"
affects: [27-domain-classification, 28-catalog-metadata]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "'sources' list field replaces 'source' string in formatted_joins entries"
    - "Source merging on dedup collision instead of dropping FJMS entries"
    - "MockTable/MockTableItem pattern for testing QTableWidget operations without Qt"

key-files:
  created: []
  modified:
    - web/components/joins_panel.py
    - web/pages/browse.py
    - corrections_ui.py
    - tests/test_fjms_joins_integration.py

key-decisions:
  - "Changed from 'source' string to 'sources' list throughout formatted_joins pipeline for multi-source support"
  - "Source merging strategy: append to existing entry's sources list on dedup collision"
  - "Neutral color (#555555) for dual-source text in desktop table cells"
  - "MockTable/MockTableItem helpers created for testing desktop table operations without Qt initialization"

patterns-established:
  - "Sources as list pattern: all consumers read 'sources' list, badge renderers loop through it"
  - "Merge-on-collision dedup: instead of dropping duplicate entries, merge metadata from second source"

# Metrics
duration: 6min
completed: 2026-02-12
---

# Phase 26 Plan 03: Dual Badge Display Summary

**Source merging replaces source-dropping dedup so fragments in both PGP and FJMS show dual badges (blue PGP + purple FJMS) in web and "PGP, FJMS" in desktop**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-12T19:44:04Z
- **Completed:** 2026-02-12T19:49:58Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Web app formatted_joins entries now use `sources` list instead of `source` string, enabling multi-badge rendering
- FJMS merge block appends 'FJMS' to existing PGP entry's sources instead of `continue`-skipping
- Desktop _merge_fjms_joins_into_display updates existing row's source column to "PGP, FJMS" and merges scholar/relationship data
- Browse page frag_info_map aggregates sources from multiple join entries per fragment
- 3 new tests (2 web dual-source, 1 desktop dual-source merge) with MockTable helper

## Task Commits

Each task was committed atomically:

1. **Task 1: Merge sources instead of dropping duplicates in web app** - `f9b1732` (feat)
2. **Task 2: Merge sources in desktop JoinsDialog for dual PGP+FJMS entries** - `b315865` (feat)

## Files Created/Modified
- `web/components/joins_panel.py` - sources list in formatted_joins, relationship_map source aggregation, badge loop
- `web/pages/browse.py` - frag_info_map source aggregation, multi-badge rendering loop
- `corrections_ui.py` - _merge_fjms_joins_into_display source merging on dedup collision
- `tests/test_fjms_joins_integration.py` - 3 new tests, updated existing tests for sources list format

## Decisions Made
- Changed `'source': 'PGP'` to `'sources': ['PGP']` throughout formatted_joins pipeline -- enables multi-source per entry
- Source merging strategy: on dedup collision, append new source to existing entry's sources list
- Neutral dark color (#555555) used for dual-source text in desktop table cells
- MockTable/MockTableItem pattern created for testing QTableWidget cell operations without Qt

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated browse.py frag_info_map to use sources list**
- **Found during:** Task 1
- **Issue:** browse.py reads `join_entry.get('source', 'user')` which no longer exists after changing to `sources` list
- **Fix:** Updated frag_info_map building to aggregate sources from list, updated badge rendering to loop through sources
- **Files modified:** web/pages/browse.py
- **Verification:** Full test suite passes (510 passed)
- **Committed in:** f9b1732 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Essential fix to prevent regression in browse page badge rendering. No scope creep.

## Issues Encountered
- Test patches for `get_document_for_fragment` and `get_fragments_for_document` needed to target `web.document_service` (source module) rather than `web.components.joins_panel` (import is inside function body) -- fixed by correcting patch paths

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 26 gap closure complete -- all three plans (joins integration, deduplication, dual badges) shipped
- Ready for Phase 27 (Domain Classification)
- No blockers for next phase

## Self-Check: PASSED

- All 4 modified files verified on disk
- Both task commits verified: f9b1732, b315865
- Full test suite: 510 passed, 5 skipped, 0 failures

---
*Phase: 26-scientific-joins*
*Completed: 2026-02-12*
