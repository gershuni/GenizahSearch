---
phase: 44-quick-ux-wins
plan: 02
subsystem: ui
tags: [localization, hebrew, library-names, i18n]

# Dependency graph
requires:
  - phase: none
    provides: none
provides:
  - LIBRARY_CODES_HE dictionary with Hebrew names for all 81 library codes
  - get_library_display() with lang parameter for language-aware library name lookup
  - All web callers pass lang=get_language() for Hebrew mode support
affects: [export-service, search, browse, parallels, lists]

# Tech tracking
tech-stack:
  added: []
  patterns: [lang parameter pattern for localized display functions]

key-files:
  created: []
  modified:
    - genizah_core.py
    - web/services.py
    - web/pages/search.py
    - web/pages/browse.py
    - web/pages/parallels.py
    - web/pages/lists.py
    - web/export_service.py
    - web/components/add_to_list_dialog.py

key-decisions:
  - "Hebrew dict mirrors English dict 1:1 with fallback to English for unknown codes"
  - "Desktop callers unchanged - use CURRENT_LANG automatically via lang=None default"
  - "Web callers explicitly pass lang=get_language() for per-session language"

patterns-established:
  - "lang parameter pattern: display functions accept optional lang param, default to CURRENT_LANG for desktop, explicit for web"

requirements-completed: [QUX-03]

# Metrics
duration: 5min
completed: 2026-03-02
---

# Phase 44 Plan 02: Hebrew Library Names Summary

**LIBRARY_CODES_HE dictionary with 81 Hebrew library names, lang-aware get_library_display(), all web callers updated for Hebrew mode**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-02T09:11:32Z
- **Completed:** 2026-03-02T09:16:03Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Added LIBRARY_CODES_HE dictionary with Hebrew names for all 81 library codes (matching LIBRARY_CODES 1:1)
- Updated get_library_display() with optional lang parameter -- backward compatible for desktop callers
- Updated all 15+ web call sites across 7 files to pass lang=get_language() for Hebrew mode support
- Export service wrapper passes language through to core function for Hebrew exports

## Task Commits

Each task was committed atomically:

1. **Task 1: Add LIBRARY_CODES_HE dictionary and update get_library_display()** - `46784750` (feat)
2. **Task 2: Update all web app callers to pass language parameter** - `81e56350` (feat)

## Files Created/Modified
- `genizah_core.py` - Added LIBRARY_CODES_HE dict (81 entries), updated get_library_display() with lang param
- `web/services.py` - Added get_language import, 2 call sites updated
- `web/pages/search.py` - Added get_language import, 5 call sites updated
- `web/pages/browse.py` - 1 call site updated (already had get_language import)
- `web/pages/parallels.py` - Added get_language import, 3 call sites updated
- `web/pages/lists.py` - Added get_language import, 2 call sites updated
- `web/export_service.py` - Wrapper method updated to pass lang through to core
- `web/components/add_to_list_dialog.py` - Added get_language import, 1 call site updated

## Decisions Made
- Hebrew dict mirrors English dict 1:1 -- every library code in LIBRARY_CODES has a Hebrew translation in LIBRARY_CODES_HE
- Desktop callers remain unchanged -- they rely on CURRENT_LANG automatically via the lang=None default parameter
- Web callers explicitly pass lang=get_language() since the web app has its own per-session language state
- Fallback chain: LIBRARY_CODES_HE -> LIBRARY_CODES -> raw code, ensuring unknown codes never break

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 44 is now fully complete (2/2 plans)
- Hebrew library names are active in both apps when Hebrew mode is selected
- Ready to proceed to Phase 45 (Filtered Search Context)

---
*Phase: 44-quick-ux-wins*
*Completed: 2026-03-02*
