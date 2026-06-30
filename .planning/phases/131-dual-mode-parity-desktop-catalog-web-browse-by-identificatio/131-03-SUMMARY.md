---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: "03"
subsystem: ui
tags: [desktop, PyQt6, library-filter, dual-mode, catalog-browse]

# Dependency graph
requires:
  - phase: 131-02
    provides: get_browse_results(library_mode=) EXISTS/NOT EXISTS dispatch in shared/fjms_service.py
  - phase: 130
    provides: Phase-130 pluralized translation keys for 3-state button wording
provides:
  - LibraryFilterDialog Show-only/Hide mode toggle with get_mode() accessor
  - D-04 reset via _on_mode_changed(*args) Qt-correct slot
  - Mode-aware OK guard (Hide: always enabled; Show-only: requires >=1 checked)
  - library_codes_with_manuscripts() universe in dialog (DMF-13)
  - _catalog_library_mode in-memory field on GenizahGUI (D-05 default 'hide')
  - 3-state library filter button using real Phase-130 pluralized keys (DMF-07)
  - _CatalogRefreshWorker.library_mode param threading into get_browse_results
  - Hide-suppress handoff in _catalog_build_browse_filters + search/parallels paths
affects:
  - 131-04  # web catalog_browse.py — same dual-mode pattern
  - 131-05  # web parallels.py — same dual-mode pattern

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "QButtonGroup.buttonToggled slot must accept *args (emits (button, checked) args)"
    - "D-04 mode-flip reset: blockSignals + uncheck-all + _update_ok_button"
    - "Hide-mode suppress: statusBar notice instead of inverting to allowlist"
    - "_catalog_library_mode field init matches sibling _catalog_pgp_filter/_catalog_editions_filter (in-memory only, no QSettings)"

key-files:
  created: []
  modified:
    - desktop/dialogs_filter.py
    - genizah_app.py

key-decisions:
  - "LibraryFilterDialog default mode='hide' (D-05 fresh default matches D-09)"
  - "Hide mode OK is always enabled — empty hide-set = show all (D-05/D-08)"
  - "D-04 reset fires unconditionally on mode toggle (idempotent uncheck-all is safe)"
  - "Hide-mode search/composition handoff: SUPPRESS restriction + show status bar notice (Codex HIGH #4 — do not invert to allowlist)"
  - "_all_codes source changed from LIBRARY_CODES.keys() to library_codes_with_manuscripts() (DMF-13); local c != 'LOCAL' guard preserved (DMF-10)"
  - "Button total computed from library_codes_with_manuscripts() not LIBRARY_CODES.keys() (Codex N4)"

patterns-established:
  - "mode-aware _update_ok_button: check get_mode() not a passed param"
  - "3-state button: neutral/show_only(red)/hide(deep-orange) using REAL pluralized tr() keys"
  - "_on_mode_changed(self, *args) for QButtonGroup.buttonToggled connections"

requirements-completed: [DMF-07, DMF-10, DMF-13]

# Metrics
duration: 15min
completed: 2026-06-30
---

# Phase 131 Plan 03: Desktop Catalog Dual-Mode Library Filter Summary

**Desktop catalog LibraryFilterDialog gains Show-only/Hide toggle with D-04 reset, mode-aware OK guard, library_codes_with_manuscripts() universe, and Hide-suppress handoff — at full model parity with Phase 130 web /search**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-06-30T12:55:00Z
- **Completed:** 2026-06-30T12:59:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- LibraryFilterDialog upgraded to dual-mode (Show-only/Hide) with QButtonGroup toggle, get_mode() accessor, _on_mode_changed(*args) D-04 reset, and mode-aware OK guard
- GenizahGUI _catalog_library_mode in-memory field (hide default) wired through dialog, 3-state button, _CatalogRefreshWorker, clear handler, and search/parallels handoff
- Hide-mode suppress in _catalog_build_browse_filters and both _catalog_search_in_results / _catalog_parallels_in_results paths with status bar notice (Codex HIGH #4)
- All 22 desktop test_libfilter_desktop.py tests green (was 8 red in Wave 0); all 18 test_fjms_browse_library_mode.py tests still green

## Task Commits

1. **Task 1: LibraryFilterDialog mode toggle + get_mode + D-04 reset + mode-aware OK + DMF-13** - `15d99059` (feat)
2. **Task 2: Thread _catalog_library_mode through GenizahGUI** - `a7021ee5` (feat)

## Files Created/Modified
- `desktop/dialogs_filter.py` - LibraryFilterDialog: mode param, QButtonGroup with _rb_show_only/_rb_hide, get_mode(), _on_mode_changed(*args), mode-aware _update_ok_button and _on_accept, library_codes_with_manuscripts() _all_codes source
- `genizah_app.py` - _catalog_library_mode init field, _open_catalog_library_dialog dual-mode, _catalog_update_library_filter_btn 3-state, _CatalogRefreshWorker library_mode param, worker construction site, _catalog_remove_filter mode reset, _catalog_build_browse_filters Show-only gate, _catalog_search_in_results + _catalog_parallels_in_results Hide-suppress

## Decisions Made
- Hide mode OK guard is unconditionally enabled (empty hide-set = show all per D-05/D-08); no separate "all-selected" sentinel needed in Hide mode
- D-04 reset fires on every buttonToggled signal — uncheck-all is idempotent so double-fire (one for de-checked, one for newly-checked button) is safe without arg inspection
- Hide-mode handoff: SUPPRESS the library restriction + statusBar notice at 5000ms (Codex HIGH #4); full-corpus complement is out of scope for restrict_sys_ids
- Button total from library_codes_with_manuscripts() (Codex N4) — the same universe as the dialog, preventing shown > selectable

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## Known Stubs
None — no placeholder or hardcoded empty values in modified code.

## Threat Flags
None — no new network endpoints, auth paths, file access patterns, or schema changes introduced. Desktop-only, single-user, in-memory attribute.

## Self-Check: PASSED
- `desktop/dialogs_filter.py` modified — confirmed present
- `genizah_app.py` modified — confirmed present
- `15d99059` — confirmed in git log
- `a7021ee5` — confirmed in git log
- 22/22 tests/test_libfilter_desktop.py passing
- 18/18 tests/test_fjms_browse_library_mode.py passing

## Next Phase Readiness
- Plan 04 (web catalog_browse.py dual-mode) can proceed; the service layer (Plan 02) and desktop surface (Plan 03) are both complete
- Patterns: QButtonGroup toggle, D-04 reset, 3-state button wording, Hide-suppress handoff all established

---
*Phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio*
*Completed: 2026-06-30*
