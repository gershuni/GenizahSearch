---
phase: 128-search-results-space-scroll-seed-025
plan: 01
subsystem: ui
tags: [nicegui, javascript, keyboard, scroll, pytest, pyqt6, web]

requires:
  - phase: 127-update-ui-final-cleanup
    provides: stable genizah_app.py base with GenizahGUI.eventFilter (desktop tests reference it)

provides:
  - "Web Space-scroll: setup_space_scroll() injected in web/pages/search.py — client-side IIFE scrolls .results-scroll-area > .q-scrollarea__container on Space/Shift+Space with full D-01 suppression set"
  - "Phase test scaffold: tests/test_space_scroll.py (7 non-gui tests) + tests/test_space_scroll_gui.py (1 gui wiring test); conftest._GUI_TEST_FILES updated"

affects: [128-02-desktop-space-scroll]

tech-stack:
  added: []
  patterns:
    - "Cat-2 deferred JS setup pattern: asyncio.ensure_future(_after_delay(1.0, fn)) + ui.run_javascript IIFE — same as setup_scroll_collapse; installs client-side listener once at page load"
    - "Wave-0 test scaffold: non-gui source guards in test_space_scroll.py (bulk slice) + single gui wiring test in test_space_scroll_gui.py (gui slice); conftest _GUI_TEST_FILES registers only the gui file"
    - "Pure decision helper pattern: space_scroll_action() extracted to module-level for honest RED-before/GREEN-after testing without QApplication (lands in 128-02)"

key-files:
  created:
    - tests/test_space_scroll.py
    - tests/test_space_scroll_gui.py
  modified:
    - web/pages/search.py
    - tests/conftest.py

key-decisions:
  - "Space-scroll uses pure client-side document.addEventListener IIFE via ui.run_javascript — NOT handle_keyboard_shortcut — because ui.keyboard fires a server round-trip that cannot call preventDefault synchronously (Finding W-1)"
  - "Suppression set includes both 'A' tagName and closest('a[href]') ancestor check to protect ui.link anchors at search_results.py:538/682/1527/1665/1774 (MEDIUM fix, D-01)"
  - "scrollTop += delta used instead of scrollBy({behavior:'instant'}) for universal Safari < 15.4 compatibility (Open Question 1 RESOLVED)"
  - "Test split mandatory: conftest auto-marks ENTIRE files gui by filename; pure decision test + web guards stay non-gui in test_space_scroll.py; single QApplication-requiring wiring test lives in test_space_scroll_gui.py"
  - "Desktop decision test (test_desktop_space_scroll_action_decision) intentionally RED in this plan — space_scroll_action helper lands in 128-02; do NOT skip or stub"

patterns-established:
  - "Wave-0 scaffold pattern: write failing tests first against not-yet-existent helpers; committed as RED to document the interface contract before implementation"

requirements-completed: [SCROLL-01, GUARD-02]

duration: 20min
completed: 2026-06-27
---

# Phase 128 Plan 01: Space-Scroll Web Handler + Test Scaffold Summary

**Client-side Space-scroll IIFE injected in web/pages/search.py with full D-01 suppression set (INPUT/BUTTON/TEXTAREA/SELECT/A/closest('a[href]')/role=button/isContentEditable/.q-dialog); Wave-0 two-file test scaffold created with conftest registration**

## Performance

- **Duration:** ~20 min
- **Started:** 2026-06-27T21:00:00Z
- **Completed:** 2026-06-27T21:20:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Web SCROLL-01 delivered: `setup_space_scroll()` in `web/pages/search.py` injects a single self-contained IIFE that listens for `keydown` Space, guards the full D-01 actionable-focus suppression set (including anchor checks for `ui.link` result cards), and scrolls `.results-scroll-area > .q-scrollarea__container` via `scrollTop += ±clientHeight`; Shift toggles direction; `window._gsSpaceScrollInstalled` prevents double-install
- `handle_keyboard_shortcut` (Escape/'/') untouched; expand-toggle `.keydown.space.self.prevent` in `search_results.py` verified intact (GUARD-02)
- Wave-0 test scaffold: `tests/test_space_scroll.py` (7 non-gui tests — 6 web source guards GREEN + `test_desktop_space_scroll_action_decision` intentionally RED) and `tests/test_space_scroll_gui.py` (1 gui wiring test intentionally RED); `tests/conftest.py` updated with `test_space_scroll_gui.py` in `_GUI_TEST_FILES` only

## Task Commits

1. **Task 1: Create two-file test scaffold + conftest registration** - `65e33120` (test)
2. **Task 2: Inject client-side Space-scroll handler into web/pages/search.py** - `144dddc6` (feat)

## Files Created/Modified
- `tests/test_space_scroll.py` — 7 non-gui tests: 6 web source guards (assert substrings in search.py/search_results.py) + `test_desktop_space_scroll_action_decision` (imports real `genizah_app.space_scroll_action`, RED until 128-02)
- `tests/test_space_scroll_gui.py` — 1 gui-marked test: `test_desktop_eventfilter_triggers_scroll` (QApplication + QTableWidget + mocked `verticalScrollBar().triggerAction`, RED until 128-02)
- `tests/conftest.py` — `_GUI_TEST_FILES` set updated: added `"test_space_scroll_gui.py"` alphabetically; `test_space_scroll.py` NOT registered (stays non-gui)
- `web/pages/search.py` — `setup_space_scroll()` async function added adjacent to `setup_scroll_collapse`; wired via `asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))` once at page setup

## Decisions Made
- Used pure client-side IIFE (not `handle_keyboard_shortcut`) because `ui.keyboard` cannot `preventDefault` synchronously after a server round-trip (Finding W-1 / RESEARCH confirmed)
- Included both `'A'` tagName and `closest('a[href]')` checks in suppression set — `ui.link` renders real `<a href>` anchors at five locations in `search_results.py`; without these a focused link's Space would be stolen (MEDIUM, D-01)
- Used `scrollTop += delta` for universal compatibility including Safari < 15.4 (Open Question 1 RESOLVED)
- Used plain (non-f) triple-quoted string for the JS to avoid brace-doubling confusion (no Python interpolation needed)

## Deviations from Plan

None — plan executed exactly as written. The web guards go GREEN in this plan; the desktop decision test and gui wiring test remain intentionally RED (Wave-0 stubs) to be satisfied by 128-02.

## Issues Encountered

One pre-existing test failure observed during the broader sanity check (`test_my_library_tab.py::test_delete_then_search_no_local_hits` — `FakeQueue` attribute error in `api_hardening.py`) — pre-existing, unrelated to this plan's changes, out-of-scope per deviation Rule scope boundary.

## Next Phase Readiness
- 128-02 (desktop): `tests/test_space_scroll.py::test_desktop_space_scroll_action_decision` and `tests/test_space_scroll_gui.py::test_desktop_eventfilter_triggers_scroll` are already committed as RED stubs; 128-02 adds `space_scroll_action()` to `genizah_app.py` + Key_Space branch to `GenizahGUI.eventFilter` to satisfy them
- The `--collect-only` gate for both files passes; 128-02 can verify collection immediately

## Self-Check: PASSED

- FOUND: tests/test_space_scroll.py
- FOUND: tests/test_space_scroll_gui.py
- FOUND: web/pages/search.py (modified)
- FOUND: 128-01-SUMMARY.md
- FOUND commit: 65e33120 (test scaffold)
- FOUND commit: 144dddc6 (web JS injection)

---
*Phase: 128-search-results-space-scroll-seed-025*
*Completed: 2026-06-27*
