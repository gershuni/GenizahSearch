---
phase: 128-search-results-space-scroll-seed-025
verified: 2026-06-28T00:00:00Z
status: human_needed
score: 8/8 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Web end-to-end: load /search, run a query, click empty results area, press Space"
    expected: "Results pane scrolls down approximately one viewport"
    why_human: "NiceGUI render-smoke gap — headless pytest cannot dispatch a real browser keydown and observe the scroll offset change"
  - test: "Web suppression (real browser): Tab to a checkbox, expand-toggle, Browse link, or PGP <a> anchor, then press Space"
    expected: "Space performs that control's action (check/toggle/navigate) and the results pane does NOT scroll"
    why_human: "Requires real browser focus + document.activeElement reads; not exercisable headlessly"
  - test: "Web dialog suppression: open a Quick View detail dialog, then press Space"
    expected: "Space does not scroll the background results pane (dialog is open, .q-dialog check fires)"
    why_human: "Requires live browser with a real Quasar dialog in the DOM"
  - test: "Desktop end-to-end: run a search, focus the results table on a non-checkbox column, press Space"
    expected: "Table scrolls down approximately one viewport"
    why_human: "Pure decision test + eventFilter wiring test cover logic and triggerAction wiring, but not the live focus/integration in the running desktop app"
  - test: "Desktop checkbox preservation: click a checkbox cell so it has focus, press Space"
    expected: "Space toggles the checkbox, no scroll occurs"
    why_human: "Live interactive desktop only — the pure decision test verifies None return for col==COL_CHECKBOX, but live toggle behavior requires the full Qt event loop"
  - test: "Desktop native keys: after the Space-scroll branch is installed, press PageDown and PageUp"
    expected: "Native table scroll unaffected (branch does not intercept Key_PageDown/Key_PageUp)"
    why_human: "Live interactive desktop only"
---

# Phase 128: Search Results Space-Scroll (SEED-025) Verification Report

**Phase Goal:** Pressing Space page-scrolls the search-results area (Shift+Space up) ONLY when no result control holds an actionable focus (checkbox / expand-collapse / open-detail / open dialog); otherwise Space falls through to scroll the results container by ~one viewport. Web (NiceGUI) + desktop (PyQt6) parity. Zero regression (GUARD-02).
**Verified:** 2026-06-28T00:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Web /search: Space with no actionable control focused page-scrolls results pane down | VERIFIED | `setup_space_scroll()` in `web/pages/search.py:2061` injects IIFE; wired via `asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))` at line 2111; JS uses `inner.scrollTop += delta` where delta is `+inner.clientHeight` |
| 2 | Shift+Space scrolls results pane up | VERIFIED | Same IIFE: `var delta = e.shiftKey ? -inner.clientHeight : inner.clientHeight` |
| 3 | Space NOT stolen when INPUT/BUTTON/TEXTAREA/SELECT/A or role=button or isContentEditable has focus | VERIFIED | JS suppression set at lines 2087-2090 checks tagName for all five + `getAttribute('role') === 'button'` + `isContentEditable`; `test_web_suppression_set_complete` PASSED |
| 4 | Space NOT stolen when a `.q-dialog` is open | VERIFIED | `document.querySelector('.q-dialog')` guard at line 2081; `test_web_dialog_guard` PASSED |
| 5 | Anchor focus does not steal Space (MEDIUM fix) | VERIFIED | Both `tag === 'A'` (line 2087) and `ae.closest('a[href]')` (line 2088) in suppression set; anchor check in `test_web_suppression_set_complete` PASSED |
| 6 | Double-install guard prevents multiple listener registrations | VERIFIED | `window._gsSpaceScrollInstalled` set on first install, early-return if already set; `test_web_no_double_install_guard` PASSED |
| 7 | Existing Escape and / keyboard shortcuts still work | VERIFIED | `handle_keyboard_shortcut` at lines 1961-1968 still has `e.key == 'Escape'` and `e.key == '/'` branches untouched; `test_existing_shortcuts_preserved` PASSED |
| 8 | expand-toggle `.keydown.space.self.prevent` intact | VERIFIED | `web/pages/search_results.py:454` still has the binding; `test_expand_toggle_space_prevent_intact` PASSED |
| 9 | Desktop: Space on non-checkbox column → page_down | VERIFIED | `space_scroll_action(3, 0, False) == 'page_down'`; `test_desktop_space_scroll_action_decision` PASSED |
| 10 | Desktop: Shift+Space on non-checkbox column → page_up | VERIFIED | `space_scroll_action(3, 0, True) == 'page_up'`; same test PASSED |
| 11 | Desktop: Space on COL_CHECKBOX column → None (checkbox toggle preserved) | VERIFIED | `space_scroll_action(0, 0, False) is None`; same test PASSED |
| 12 | Desktop: Space with no current item (col == -1) → scroll, not no-op | VERIFIED | `space_scroll_action(-1, 0, False) == 'page_down'`; same test PASSED |
| 13 | Desktop eventFilter wires decision to verticalScrollBar().triggerAction | VERIFIED | `test_desktop_eventfilter_triggers_scroll` PASSED: `triggerAction(QAbstractSlider.SliderAction.SliderPageStepAdd)` called once, return True |
| 14 | Space branch fires only for results_table source (not query_input etc.) | VERIFIED | Branch gated on `source is self.results_table` at `genizah_app.py:17943` |
| 15 | GUARD-02: zero regression on existing test suite | VERIFIED | 542 tests passed (`-m "not gui and not render_smoke" -k "search or keyboard"`) + all 75 back-edge/facade guard tests PASSED; no failures |

**Score:** 8/8 must-haves verified (all truths VERIFIED; 6 interactive behaviors require human confirmation)

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/pages/search.py` | Client-side Space-scroll IIFE injected via `setup_space_scroll()` | VERIFIED | Function defined at line 2061; wired at line 2111; JS contains all D-01 suppression conditions + `.q-dialog` guard + `scrollTop += delta` + `e.preventDefault()` |
| `tests/test_space_scroll.py` | 7 non-gui tests: 6 web source guards + 1 pure desktop decision test | VERIFIED | All 7 tests PASSED; file not registered in `_GUI_TEST_FILES`; 0 tests collected under `-m gui` |
| `tests/test_space_scroll_gui.py` | 1 gui-marked wiring test: `test_desktop_eventfilter_triggers_scroll` | VERIFIED | 1 test PASSED under `-m gui`; registered in `_GUI_TEST_FILES` |
| `tests/conftest.py` | `"test_space_scroll_gui.py"` in `_GUI_TEST_FILES`; `"test_space_scroll.py"` NOT present | VERIFIED | `_GUI_TEST_FILES` set at line 92 contains `"test_space_scroll_gui.py"` (alphabetically inserted); `"test_space_scroll.py"` absent |
| `genizah_app.py` | `space_scroll_action` pure helper + `QAbstractSlider` import + Key_Space eventFilter branch | VERIFIED | `space_scroll_action` defined at line 117 (pure, no Qt); `QAbstractSlider` in `from PyQt6.QtWidgets import (...)` at line 21; Key_Space branch at lines 17941-17956 |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `web/pages/search.py setup_space_scroll()` | `.results-scroll-area .q-scrollarea__container` | `ui.run_javascript` IIFE with `document.querySelector('.results-scroll-area')` then `.querySelector('.q-scrollarea__container')` | WIRED | Lines 2093-2095 |
| `web/pages/search.py` page setup | `setup_space_scroll` | `asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))` | WIRED | Line 2111, adjacent to existing `setup_scroll_collapse` call |
| `GenizahGUI.eventFilter` Key_Space branch | `space_scroll_action(col, self.COL_CHECKBOX, is_shift)` | pure decision helper call returning `'page_up'`/`'page_down'`/`None` | WIRED | Line 17948 |
| `eventFilter` Space branch (non-None action) | `self.results_table.verticalScrollBar().triggerAction(...)` | `QAbstractSlider.SliderAction.SliderPageStepAdd/Sub` | WIRED | Lines 17950-17954 |
| `eventFilter` Space branch gate | `self.results_table` / `self.COL_CHECKBOX` | `source is self.results_table` identity check | WIRED | Line 17943 |

---

### Data-Flow Trace (Level 4)

Not applicable — this phase adds UI scroll behavior (event handling), not data rendering from a database or API. There is no dynamic data source to trace.

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `space_scroll_action` importable without QApplication | `python -m pytest tests/test_space_scroll.py::test_desktop_space_scroll_action_decision -q` | 1 passed | PASS |
| All 7 non-gui tests pass | `python -m pytest tests/test_space_scroll.py -q` | 7 passed | PASS |
| gui wiring test passes | `python -m pytest tests/test_space_scroll_gui.py -m gui -q` | 1 passed | PASS |
| Non-gui file excluded from -m gui | `python -m pytest tests/test_space_scroll.py -m gui --collect-only -q` | 0 tests collected | PASS |
| gui file included in -m gui | `python -m pytest tests/test_space_scroll_gui.py -m gui --collect-only -q` | 1 test collected | PASS |
| Python AST parse of genizah_app.py | `python -c "import ast; ast.parse(open('genizah_app.py', encoding='utf-8').read())"` | AST OK | PASS |
| Python AST parse of web/pages/search.py | `python -c "import ast; ast.parse(open('web/pages/search.py', encoding='utf-8').read())"` | AST OK | PASS |
| No regression on keyboard/search tests | `pytest -m "not gui and not render_smoke" -k "search or keyboard"` | 542 passed, 0 failed | PASS |
| Back-edge and facade guard tests | `pytest tests/test_no_back_edges_core.py tests/test_no_back_edges_desktop.py tests/test_genizah_core_facade.py -q` | 75 passed | PASS |

---

### Probe Execution

Step 7c: SKIPPED — no probe scripts declared or found for this phase.

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| SCROLL-01 | 128-01 | Web /search Space page-scrolls results container; Shift+Space up; actionable controls not stolen; a11y intact | SATISFIED | `setup_space_scroll()` in `web/pages/search.py` with full D-01 suppression set; 6 web source guards PASSED |
| SCROLL-02 | 128-02 | Desktop results table Space → page-down/up for non-checkbox column; checkbox column preserves toggle | SATISFIED | `space_scroll_action` pure helper + Key_Space eventFilter branch; `test_desktop_space_scroll_action_decision` + `test_desktop_eventfilter_triggers_scroll` both PASSED |
| GUARD-02 | 128-01, 128-02 | Zero behavior change — full pytest suite passes at every phase boundary | SATISFIED | 542 search/keyboard tests pass; back-edge guards (75) pass; `keydown.space.self.prevent` and `handle_keyboard_shortcut` Escape/'/' branches intact; no regressions observed |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | None found | — | — |

Scanned `web/pages/search.py` (new `setup_space_scroll` function), `genizah_app.py` (new `space_scroll_action` helper + eventFilter branch), `tests/test_space_scroll.py`, `tests/test_space_scroll_gui.py`, `tests/conftest.py`. No TBD/FIXME/XXX markers, no placeholder returns, no hardcoded empty state, no stubs.

---

### Human Verification Required

The automated test suite confirms all implementation logic (decision table, suppression set, eventFilter wiring, double-install guard, GUARD-02 regressions). Six interactive behaviors require a live browser and/or the running desktop app:

#### 1. Web Space Scroll — basic

**Test:** Load `/search`, run a query so results appear, click on an empty area of the results pane (to defocus any control), then press Space.
**Expected:** The results pane scrolls down approximately one viewport. Press Shift+Space — it scrolls back up.
**Why human:** NiceGUI render-smoke gap — headless pytest cannot dispatch a real browser keydown event and observe scroll offset changes.

#### 2. Web Suppression — actionable controls

**Test:** Tab through the results until a checkbox, expand-toggle, Browse link, or PGP `<a>` anchor has keyboard focus (check the browser's focus indicator), then press Space.
**Expected:** Space performs that control's natural action (checkbox toggles, expand-toggle expands/collapses, link navigates). The results pane does NOT scroll.
**Why human:** Requires real browser `document.activeElement` state which headless tests cannot reproduce.

#### 3. Web Suppression — open dialog

**Test:** Open a Quick View dialog (e.g., click "Quick View" on a result). While the dialog is open, press Space.
**Expected:** Space does not scroll the background results pane. The `.q-dialog` guard fires.
**Why human:** Requires a live Quasar dialog in the browser DOM.

#### 4. Desktop Space Scroll — basic

**Test:** Run a search in the desktop app, click the results table to give it focus (on a non-checkbox column cell), press Space.
**Expected:** The results table scrolls down approximately one viewport. Press Shift+Space — it scrolls up.
**Why human:** The pure decision test and gui wiring test verify the logic and triggerAction call, but not the live vertical scroll in the running app.

#### 5. Desktop Checkbox Preservation

**Test:** Click a cell in the checkbox column (column 0) so it has focus. Press Space.
**Expected:** The checkbox toggles (checked/unchecked). No scroll occurs.
**Why human:** The decision test verifies `None` return for col==COL_CHECKBOX, but the full `QEvent` fallthrough to Qt's native checkbox toggle requires the interactive desktop.

#### 6. Desktop Native PageDown/PageUp Unaffected

**Test:** After a search, focus the results table and press PageDown and PageUp.
**Expected:** Native table scroll works exactly as before. The new Space-scroll branch does not intercept `Key_PageDown` or `Key_PageUp`.
**Why human:** Live interactive desktop only.

---

### Gaps Summary

No automated gaps. All 8 must-have tests pass (7 non-gui + 1 gui), all 3 requirements (SCROLL-01, SCROLL-02, GUARD-02) are satisfied by the implementation. All locked decisions D-01 through D-04 are implemented and verified:

- **D-01** (suppression set): INPUT/BUTTON/TEXTAREA/SELECT/A/closest('a[href]')/role=button/isContentEditable — all present in the injected JS and asserted by `test_web_suppression_set_complete`.
- **D-02** (one viewport, Shift reverses): `delta = e.shiftKey ? -inner.clientHeight : inner.clientHeight` — verified.
- **D-03** (scroll target: `.results-scroll-area > .q-scrollarea__container`): verified in JS and by `test_web_space_scroll_js_installed`.
- **D-04** (desktop checkbox column falls through to Qt toggle): `space_scroll_action` returns `None` for `col == COL_CHECKBOX`; eventFilter does not return `True` on `None`, falls through to `super()`.

Status is `human_needed` because the six live-browser/live-desktop behaviors cannot be verified headlessly, per the project's established NiceGUI render-smoke gap policy and the 128-VALIDATION.md Manual-Only section.

---

_Verified: 2026-06-28T00:00:00Z_
_Verifier: Claude (gsd-verifier)_
