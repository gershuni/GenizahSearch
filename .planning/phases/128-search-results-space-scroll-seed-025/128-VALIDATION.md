---
phase: 128
slug: search-results-space-scroll-seed-025
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-27
---

# Phase 128 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `128-RESEARCH.md` § Validation Architecture. Task IDs are filled by the planner; rows are keyed by requirement until then.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing) |
| **Config file** | none — standard `pytest tests/` |
| **Quick run command** | `python -m pytest tests/test_space_scroll.py -x -q && python -m pytest tests/test_space_scroll_gui.py -m gui -x -q` |
| **Full suite command** | `python -m pytest tests/ -m "not gui and not render_smoke" -x -q` |
| **Estimated runtime** | ~5s quick; full bulk slice ~minutes |

> The phase test set is split across TWO files because `tests/conftest.py` auto-applies the `gui` marker to EVERY test in any file listed in `_GUI_TEST_FILES` (whole-file marking). To keep the web guards + the pure desktop decision test in the bulk `-m "not gui"` slice, they live in the NON-gui `tests/test_space_scroll.py` (which is NOT registered). The single desktop wiring test (`test_desktop_eventfilter_triggers_scroll`, needs `QApplication.instance() or QApplication([])`) lives in `tests/test_space_scroll_gui.py`, and ONLY that file is added to `_GUI_TEST_FILES`. Most desktop proof is the PURE helper test (`test_desktop_space_scroll_action_decision`) that needs NO QApplication and runs in the bulk slice.

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_space_scroll.py -x -q` (+ `tests/test_space_scroll_gui.py -m gui -x -q` once the desktop branch lands)
- **After every plan wave:** Run `python -m pytest tests/ -m "not gui and not render_smoke" -x -q` + the `gui` slice (`tests/test_space_scroll_gui.py -m gui`) for the desktop wiring test
- **Before `/gsd:verify-work`:** Full suite (bulk + gui) green
- **Max feedback latency:** ~5s (quick)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 128-01 T2 | 128-01 | 1 | SCROLL-01 | T-128-01 | N/A | source guard | `pytest tests/test_space_scroll.py::test_web_space_scroll_js_installed -x` | ❌ W0 | ⬜ pending |
| 128-01 T2 | 128-01 | 1 | SCROLL-01 | T-128-01 | N/A | source guard | `pytest tests/test_space_scroll.py::test_web_suppression_set_complete -x` | ❌ W0 | ⬜ pending |
| 128-01 T2 | 128-01 | 1 | SCROLL-01 | T-128-01 | N/A | source guard | `pytest tests/test_space_scroll.py::test_web_dialog_guard -x` | ❌ W0 | ⬜ pending |
| 128-01 T2 | 128-01 | 1 | GUARD-02 | — | N/A | source guard | `pytest tests/test_space_scroll.py::test_expand_toggle_space_prevent_intact -x` | ❌ W0 | ⬜ pending |
| 128-01 T2 | 128-01 | 1 | GUARD-02 | T-128-02 | N/A | source guard | `pytest tests/test_space_scroll.py::test_web_no_double_install_guard -x` | ❌ W0 | ⬜ pending |
| 128-01 T2 | 128-01 | 1 | GUARD-02 | — | N/A | source guard | `pytest tests/test_space_scroll.py::test_existing_shortcuts_preserved -x` | ❌ W0 | ⬜ pending |
| 128-02 T1 | 128-02 | 2 | SCROLL-02 | T-128-D1 | N/A | unit (pure helper, no QApplication) | `pytest tests/test_space_scroll.py::test_desktop_space_scroll_action_decision -x` | ❌ W0 | ⬜ pending |
| 128-02 T1 | 128-02 | 2 | SCROLL-02 / GUARD-02 | T-128-D1 | N/A | gui (widget wiring) | `pytest tests/test_space_scroll_gui.py::test_desktop_eventfilter_triggers_scroll -m gui -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

> Coverage note: `test_desktop_space_scroll_action_decision` (in the NON-gui `tests/test_space_scroll.py`) covers the full SCROLL-02 / GUARD-02 desktop decision table in one pure test — col != COL_CHECKBOX → page_down; +shift → page_up; col == COL_CHECKBOX → None (checkbox toggle preserved); col == -1 → page_down/up (no-current-item scroll). `test_desktop_eventfilter_triggers_scroll` (in the gui-marked `tests/test_space_scroll_gui.py`) proves the eventFilter branch actually wires that decision to `verticalScrollBar().triggerAction`.

---

## Wave 0 Requirements

- [ ] `tests/test_space_scroll.py` — new NON-gui file (NOT registered in `_GUI_TEST_FILES`): six web source/static guards (no QApplication) + `test_desktop_space_scroll_action_decision` (pure, calls REAL `genizah_app.space_scroll_action`, no QApplication). Runs in the bulk `-m "not gui"` slice.
- [ ] `tests/test_space_scroll_gui.py` — new gui file: ONE `gui`-marked wiring test `test_desktop_eventfilter_triggers_scroll` (`_app = QApplication.instance() or QApplication([])` + bare QTableWidget + mocked `verticalScrollBar().triggerAction`).
- [ ] `tests/conftest.py` — add ONLY `"test_space_scroll_gui.py"` to `_GUI_TEST_FILES` so the single desktop widget wiring test runs in the gui slice. Do NOT add `"test_space_scroll.py"` (it must stay non-gui).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Web end-to-end keypress (real browser): Space scrolls results pane ~1 viewport; Shift+Space up | SCROLL-01 | Headless render-smoke can't dispatch real browser keydown + observe scroll (project NiceGUI render-smoke gap) | Load `/search`, run a query, click empty results area, press Space → pane scrolls down; Shift+Space → up |
| Web suppression (real browser): checkbox/expand/action-button/link/open-dialog focus → Space does its action, results do NOT scroll | SCROLL-01 | Same render-smoke gap; needs real focus + activeElement | Tab to each control type (incl. a Browse/PGP `<a>` link), press Space, confirm action fires and no scroll; open Quick View dialog, press Space, confirm no background scroll |
| Desktop end-to-end (real app): Space page-scrolls table when no checkbox cell focused; toggles checkbox when it is; native PageDown/PageUp unaffected | SCROLL-02, GUARD-02 | Full interactive desktop launch (the pure decision test + gui wiring test cover the logic + triggerAction wiring, but not the live focus/native-key integration) | Run a search, focus the table, press Space → page down; click a checkbox cell, press Space → toggles; press PageDown → native scroll |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`tests/test_space_scroll.py`, `tests/test_space_scroll_gui.py`, conftest `_GUI_TEST_FILES`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s (quick)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
</content>
