---
phase: 21-debug-pgp-integration
plan: 04
subsystem: ui
tags: [pyqt6, desktop, translation, language-grouping]

# Dependency graph
requires:
  - phase: 21-debug-pgp-integration/02
    provides: "document_sources with language field populated"
  - phase: 21-debug-pgp-integration/03
    provides: "display pipeline wiring for structured sections"
provides:
  - "Desktop translation ordering matches web app (Hebrew first, English second)"
  - "Language-based grouping in _populate_pgp_combo"
affects: [desktop-app, pgp-integration]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Language-based translation grouping (Hebrew > English > other)"

key-files:
  created: []
  modified:
    - "genizah_app.py"

key-decisions:
  - "Adopted same grouping pattern as web app (version_selector.py:256-264) for consistency"

patterns-established:
  - "Translation display order: Hebrew first, English second, others last (both apps)"

# Metrics
duration: 1min
completed: 2026-02-11
---

# Phase 21 Plan 04: Desktop Translation Ordering Fix Summary

**Language-based translation grouping in desktop _populate_pgp_combo matching web app Hebrew-first, English-second order**

## Performance

- **Duration:** 1 min
- **Started:** 2026-02-11T12:49:39Z
- **Completed:** 2026-02-11T12:50:35Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Added language-based grouping to desktop `_populate_pgp_combo` method
- Translations now display Hebrew first, English second, others last -- matching web app behavior
- Fixes UAT gap where Hebrew translation label showed English content due to database sequence_order

## Task Commits

Each task was committed atomically:

1. **Task 1: Add language-based grouping to desktop translation display** - `26ce37d` (fix)

**Plan metadata:** (pending)

## Files Created/Modified
- `genizah_app.py` - Added hebrew_trans/english_trans/other_trans grouping in _populate_pgp_combo (lines 6104-6122)

## Decisions Made
- Adopted identical grouping pattern from web app (version_selector.py:256-264) for cross-app consistency
- No new dependencies or architectural changes needed -- language field already populated from Plan 02 import

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 21 gap closure complete (all 4 plans done)
- Desktop and web apps now have consistent translation ordering
- User verification needed: open desktop app, select PGP document with both Hebrew and English translations, confirm correct content per language label

---
*Phase: 21-debug-pgp-integration*
*Completed: 2026-02-11*

## Self-Check: PASSED
- genizah_app.py: FOUND
- 21-04-SUMMARY.md: FOUND
- Commit 26ce37d: FOUND in git log
