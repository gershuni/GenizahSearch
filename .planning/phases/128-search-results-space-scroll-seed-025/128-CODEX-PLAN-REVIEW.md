# Phase 128 — Codex PLAN Pre-flight Review (record)

**Outcome:** APPROVE (round 4). Converged over 4 rounds. Brief at `128-CODEX-BRIEF.md`.

## Round 1 — CHANGES NEEDED
- **[HIGH]** Desktop gui test could pass with a hand-copied `eventFilter` stub without exercising production code → extract a pure importable helper `space_scroll_action(current_column, checkbox_column, is_shift) -> 'page_up'|'page_down'|None` in `genizah_app.py`; `eventFilter` delegates; tests call the REAL helper.
- **[MEDIUM]** Web suppression set missed `<a>` anchors (result cards have `ui.link` at `web/pages/search_results.py:538/682/...`) → add `tagName==='A'` / `closest('a[href]')` to the JS guard + `test_web_suppression_set_complete`.
- **[LOW]** Path drift `web/components/` → `web/pages/search_results.py`.
- **[LOW]** `QAbstractSlider` enum couldn't be runtime-verified by Codex (no working local Python) → verified locally: `QAbstractSlider.SliderAction.SliderPageStepAdd/Sub` correct; hedging dropped, import addition kept.
- Confirmed sound: scroll-area target (`.results-scroll-area .q-scrollarea__container`), `_after_delay(1.0, …)` hook, Escape/`/` shortcuts intact, `results_table`/`COL_CHECKBOX=0`/`ItemIsUserCheckable`, conftest auto-marker.

## Round 2 — CHANGES NEEDED
- **[HIGH]** `conftest.py` auto-marks gui by FILENAME (whole file); registering `test_space_scroll.py` in `_GUI_TEST_FILES` would gui-mark all 8 tests → bulk `-m "not gui"` would deselect the file (web guards + pure decision test never run) → **split**: non-gui `tests/test_space_scroll.py` (6 web guards + pure `test_desktop_space_scroll_action_decision`) + gui `tests/test_space_scroll_gui.py` (only `test_desktop_eventfilter_triggers_scroll`, the only file registered).

## Round 3 — CHANGES NEEDED
- Split + all prior fixes confirmed consistent. 2 doc-consistency nits: stale `-k web` acceptance command (→ `-k "web or shortcuts or expand"`); `128-VALIDATION.md` `nyquist_compliant: false` vs its own sign-off (→ true).

## Round 4 — APPROVE
- No findings. `128-01-PLAN.md` uses `-k "web or shortcuts or expand"` everywhere; `128-VALIDATION.md` `status: approved`, `nyquist_compliant: true`, sign-off checked, no stray tag; gui/non-gui split consistent; `git diff --check` clean.

## Net plan shape entering execution
- **128-01** (wave 1): two-file test scaffold (`tests/test_space_scroll.py` non-gui = 6 web source guards + pure desktop decision test; `tests/test_space_scroll_gui.py` gui = 1 wiring test; register only the gui file in conftest `_GUI_TEST_FILES`) + the web client-side `document.addEventListener('keydown')` injection (suppression set incl. `<a>`, `scrollTop += delta`, `.q-scrollarea__container`, double-install guard, no change to `handle_keyboard_shortcut`).
- **128-02** (wave 2, depends 128-01): pure `space_scroll_action` helper + `QAbstractSlider` import + `eventFilter` Key_Space branch delegating to it (checkbox column → toggle preserved; other / col==-1 → page scroll; shift → up).
