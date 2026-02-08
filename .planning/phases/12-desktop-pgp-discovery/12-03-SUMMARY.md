---
phase: 12-desktop-pgp-discovery
plan: 03
subsystem: ui
tags: [pyqt6, joins, pgp, fragments, desktop]

# Dependency graph
requires:
  - phase: 08-foundation
    provides: shared/document_service.py with get_document_for_fragment, get_fragments_for_document
  - phase: 09-data-import
    provides: document_fragments table with multi-fragment PGP documents
provides:
  - PGP multi-fragment joins visible in desktop JoinsDialog
  - PGP join rows with green source label and deletion protection
affects: [13-transcription-search]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "PGP join merge pattern: _get_pgp_joins() -> _merge_pgp_joins_into_display() with dedup"
    - "Null join ID convention: PGP joins use None as ID to prevent deletion"

key-files:
  created: []
  modified:
    - corrections_ui.py

key-decisions:
  - "Synchronous PGP lookup in dialog (acceptable latency for 2 fast queries)"
  - "Deduplication by shelfmark pair (upper-cased) to avoid duplicate entries when user and PGP joins overlap"

patterns-established:
  - "PGP join source pattern: green '#27ae60' foreground on Source column for PGP rows"
  - "Helper method pattern: _add_pgp_join_rows() shared by pgp-only and merge paths"

# Metrics
duration: 5min
completed: 2026-02-08
---

# Phase 12 Plan 03: PGP Joins in Desktop JoinsDialog Summary

**PGP multi-fragment document joins merged into desktop JoinsDialog with green source labels, deletion protection, and deduplication against user joins**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-08T17:04:44Z
- **Completed:** 2026-02-08T17:09:22Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- PGP multi-fragment joins now appear alongside user-created joins in desktop Related Fragments dialog
- PGP joins display green "PGP" label in Source column for visual distinction
- PGP joins cannot be deleted (None join ID check in both table selection handler and delete method)
- Single-fragment PGP documents correctly filtered out (no false join entries)
- PGP-only display path works when no user joins exist
- Deduplication prevents duplicate rows when user and PGP joins reference the same fragment pair

## Task Commits

Each task was committed atomically:

1. **Task 1: Merge PGP joins into JoinsDialog load_joins** - `5d098a4` (feat)

## Files Created/Modified
- `corrections_ui.py` - Added _get_pgp_joins(), _display_pgp_only_joins(), _add_pgp_join_rows(), _merge_pgp_joins_into_display() methods; modified load_joins(), _display_cached_joins(), _display_connected_data(), on_table_selection_changed()

## Decisions Made
- Synchronous PGP lookup: The dialog already shows a loading state while fetching user joins; adding 2 fast Supabase queries (document_fragments lookup) is acceptable without QThread overhead
- Deduplication strategy: Upper-cased shelfmark pairs compared to avoid showing same relationship from both user and PGP sources
- Shared helper method: _add_pgp_join_rows() reused by both _display_pgp_only_joins() and _merge_pgp_joins_into_display()

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Desktop JoinsDialog now shows PGP joins, completing the D3 item deferred from Phase 11
- Phase 12 plans 01-03 provide desktop PGP metadata, transcription display, and join visibility
- Ready for Phase 13: Transcription Search

## Self-Check: PASSED

---
*Phase: 12-desktop-pgp-discovery*
*Completed: 2026-02-08*
