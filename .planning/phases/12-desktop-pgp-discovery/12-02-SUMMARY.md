---
phase: 12-desktop-pgp-discovery
plan: 02
subsystem: ui
tags: [pyqt6, nicegui, pgp, search, qthread, supabase, badges, filters, tags]

# Dependency graph
requires:
  - phase: 08-foundation
    provides: shared/document_service.py with Supabase access
  - phase: 09-data-import
    provides: documents table with tags JSONB column, document_fragments table
provides:
  - get_all_distinct_tags() service function (251 PGP tags)
  - PGPBadgeWorker, PGPTagsWorker, PGPTagSearchWorker QThread classes
  - Desktop PGP badge column (COL_PGP = 9) in search results
  - Desktop PGP filter checkbox and tag search dropdown
  - Web PGP text badge (replacing icon) and PGP filter toggle
affects: [12-desktop-pgp-discovery, 13-transcription-search]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "PGP badge worker pattern: launch after search, emit set of matching sys_ids"
    - "Tag search conversion: convert Supabase tag results to search result format"
    - "Lazy tag loading: PGPTagsWorker launches at search tab creation"

key-files:
  created: []
  modified:
    - shared/document_service.py
    - gui_threads.py
    - web/document_service.py
    - genizah_app.py
    - web/pages/search.py

key-decisions:
  - "DEC-12-02-01: PGP badge column (COL_PGP=9) added after SRC column, 40px fixed width"
  - "DEC-12-02-02: PGP controls on separate row3 in desktop to avoid crowding row2"
  - "DEC-12-02-03: Web PGP text badge styled like library badge (success-100/700 colors)"
  - "DEC-12-02-04: Tag search results converted to search result format for table display"

patterns-established:
  - "PGP badge async pattern: search completes -> PGPBadgeWorker launched -> badges applied to existing rows"
  - "Tag search flow: combo selection -> PGPTagSearchWorker -> format results -> load_next_batch"

# Metrics
duration: 9min
completed: 2026-02-08
---

# Phase 12 Plan 02: PGP Search Discovery Summary

**PGP text badges in both apps, PGP-only filter toggles, and desktop tag search dropdown with 251 PGP tags**

## Performance

- **Duration:** 9 min
- **Started:** 2026-02-08T17:02:51Z
- **Completed:** 2026-02-08T17:12:34Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments
- Added `get_all_distinct_tags()` service function returning 251 distinct PGP tags
- Desktop search results now show green "PGP" badge in dedicated column for manuscripts with transcriptions
- Desktop "PGP Only" checkbox filters results to only PGP manuscripts
- Desktop tag search dropdown (editable, searchable, 220px) with "Search Tag" button
- Web PGP indicator changed from icon to styled text badge matching library badge pattern
- Web filters panel now includes "PGP Only" checkbox integrated into apply/clear filter flow

## Task Commits

Each task was committed atomically:

1. **Task 1: Service layer + QThread workers for badges and tags** - `20606e2` (feat)
2. **Task 2: Desktop PGP column, filter, and tag search in Search tab** - `6719b09` (feat)
3. **Task 3: Web PGP text badge and filter toggle** - `5555892` (feat)

## Files Created/Modified
- `shared/document_service.py` - Added `get_all_distinct_tags()` function
- `gui_threads.py` - Added PGPBadgeWorker, PGPTagsWorker, PGPTagSearchWorker QThread classes
- `web/document_service.py` - Re-exported `get_all_distinct_tags` in shim
- `genizah_app.py` - PGP column, filter checkbox, tag search dropdown, badge worker integration
- `web/pages/search.py` - PGP text badge (replacing icon), PGP filter checkbox in filters panel

## Decisions Made
- **PGP controls placement (DEC-12-02-02):** Added a new row3 layout in the desktop search tab for PGP controls rather than crowding row2, which already has mode, gap, exclude, lab mode, and settings controls
- **Web badge style (DEC-12-02-03):** Used success-100 background with success-700 text (matching the library badge pattern), replacing the description icon with a styled text badge for visual consistency
- **Tag search result format (DEC-12-02-04):** Tag search results are converted to the same format as regular search results (with `display` dict containing id, shelfmark, title) so they can be displayed in the standard results table using `load_next_batch()`

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- PGP badges and filters ready for both apps
- Desktop tag search functional with 251 tags
- `_search_by_pgp_tag(tag)` entry point available for cross-page navigation (Phase 12-01 browse page can call it)
- Ready for Phase 12-03 (if not already complete) or Phase 13

## Self-Check: PASSED

---
*Phase: 12-desktop-pgp-discovery*
*Completed: 2026-02-08*
