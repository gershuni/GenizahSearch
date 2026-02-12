---
phase: 26-scientific-joins
plan: 01
subsystem: ui
tags: [fjms, joins, scholarly-attribution, related-fragments, nicegui, pyqt6]

# Dependency graph
requires:
  - phase: 25-data-infrastructure
    provides: "FjmsService with get_join_group() method and SQLite sidecar"
provides:
  - "FJMS scholarly joins visible in web app Related Fragments panel"
  - "FJMS scholarly joins visible in desktop app joins dropdown and JoinsDialog"
  - "Scholar name and join type display for FJMS join entries"
  - "Deduplication of FJMS joins against user and PGP joins"
  - "Integration tests for FJMS joins pipeline"
affects: [27-domain-classification, 28-catalog-metadata]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "FJMS joins as third data source in merge pipeline (user -> PGP -> FJMS)"
    - "Purple badge/color for FJMS source attribution in both apps"
    - "_merge_fjms_joins_into_display pattern matching existing _merge_pgp_joins_into_display"

key-files:
  created:
    - tests/test_fjms_joins_integration.py
  modified:
    - web/components/joins_panel.py
    - web/pages/browse.py
    - genizah_app.py
    - corrections_ui.py

key-decisions:
  - "FJMS joins merged as third source after user and PGP in fetch_connected_fragments"
  - "Purple color/badge for FJMS source visual distinction (user=none, PGP=blue, FJMS=purple)"
  - "Scholar name stored in created_by_username field in desktop for table column reuse"
  - "Desktop _get_fjms_joins follows same tuple pattern as _get_pgp_joins for consistency"

patterns-established:
  - "Three-way join merge: user -> PGP -> FJMS with deduplication at each stage"
  - "MagicMock stub pattern for testing QDialog methods without Qt initialization"

# Metrics
duration: 8min
completed: 2026-02-12
---

# Phase 26 Plan 01: Joins Integration Summary

**FJMS scholarly join groups merged into Related Fragments panel in both web and desktop apps with scholar name, join type display, purple badge, and deduplication against user/PGP joins**

## Performance

- **Duration:** 8 min
- **Started:** 2026-02-12T03:59:32Z
- **Completed:** 2026-02-12T04:07:25Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- FJMS joins appear as third data source in web app's Related Fragments section with purple badge, scholar name, and join type
- Desktop app shows FJMS joins in dropdown menu (both standalone fallback and merged with existing joins) and in JoinsDialog
- Full deduplication pipeline: FJMS entries are skipped when the same fragment already appears from user or PGP joins
- 7 integration tests covering merge behavior, deduplication, scholar attribution, graceful degradation, and desktop dialog structure
- All 501 tests passing (including 27 existing FJMS service tests + 7 new integration tests)

## Task Commits

Each task was committed atomically:

1. **Task 1: Integrate FJMS joins into web app Related Fragments** - `55af096` (feat)
2. **Task 2: Integrate FJMS joins into desktop app and add tests** - `9f293ba` (feat)

## Files Created/Modified
- `web/components/joins_panel.py` - FJMS merge block in fetch_connected_fragments, FJMS badge and scholar name in joins dialog
- `web/pages/browse.py` - FJMS badge (purple), scholar name display, scholar_name extraction in frag_info_map
- `genizah_app.py` - FJMS fallback and merge in _update_joins_dropdown with [FJMS] prefix labels
- `corrections_ui.py` - _get_fjms_joins(), _merge_fjms_joins_into_display(), FJMS merge in all display methods
- `tests/test_fjms_joins_integration.py` - 7 integration tests (4 web, 3 desktop)

## Decisions Made
- FJMS joins merged as third source after user and PGP in fetch_connected_fragments, maintaining the existing merge pipeline pattern
- Purple color chosen for FJMS badge/source to visually distinguish from PGP (blue) and user (no badge)
- Scholar name stored in `created_by_username` field in desktop JoinsDialog for table column reuse
- Desktop `_get_fjms_joins` follows identical tuple return pattern as `_get_pgp_joins` for consistency
- MagicMock stub pattern used for testing QDialog-based methods without Qt initialization

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- `object.__new__(JoinsDialog)` fails because JoinsDialog extends QDialog (C++ class) -- resolved by using MagicMock stubs with the real method bound via `__get__` descriptor protocol

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- FJMS joins visible in both apps, ready for Phase 27 (Domain Classification)
- FjmsService get_domains() and get_all_domains() methods ready for domain filter integration
- No blockers for next phase

---
*Phase: 26-scientific-joins*
*Completed: 2026-02-12*
