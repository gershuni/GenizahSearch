# Codex PLAN pre-flight brief — GenizahSearch Phase 128 (Search Results Space-Scroll, SEED-025)

You are doing a **plan pre-flight review** (not a code review — no code is written yet). Goal: catch plan↔code drift and correctness problems BEFORE execution. Be skeptical and concrete; verify claims against the LIVE codebase, do not take the plan's word for file/line/symbol claims.

## Read these planning docs first
- `.planning/phases/128-search-results-space-scroll-seed-025/128-01-PLAN.md` (web client-side JS + test scaffold; reqs SCROLL-01, GUARD-02)
- `.planning/phases/128-search-results-space-scroll-seed-025/128-02-PLAN.md` (desktop eventFilter Space branch; reqs SCROLL-02, GUARD-02)
- `.planning/phases/128-search-results-space-scroll-seed-025/128-CONTEXT.md` (LOCKED decisions D-01..D-04)
- `.planning/phases/128-search-results-space-scroll-seed-025/128-RESEARCH.md` (mechanism findings + Assumptions A1-A3)

## The feature (locked, do not relitigate)
Pressing Space page-scrolls the search results (Shift+Space up) UNLESS a result control holds actionable focus (checkbox / expand-collapse / open-detail / open dialog). Web (NiceGUI) via pure client-side `document.addEventListener('keydown')` injected through `ui.run_javascript` (the server `ui.keyboard` on_key CANNOT preventDefault). Desktop (PyQt6) via a `Key_Space` branch in the existing `results_table` eventFilter, gated on `currentColumn() != COL_CHECKBOX`.

## Verify against the LIVE code (grep/read — line numbers may have drifted post-v8.3.0 decomposition)
1. **web/pages/search.py** — Does the results `ui.scroll_area().classes('...results-scroll-area')` exist? Is the Quasar inner scroller `.q-scrollarea__container` the right element (cross-check the existing `setup_scroll_collapse` JS pattern)? Is there a real call site to register a post-render JS hook (the plan claims an `_after_delay(1.0, ...)` near the existing `setup_scroll_collapse`)? Does the existing `ui.keyboard(on_key=...)` / `handle_keyboard_shortcut` (Escape, `/`) still need to stay intact (GUARD-02)?
2. **web/components/search_results.py** — Confirm the expand-toggle already has `keydown.space.self.prevent` (the plan relies on this so the global handler needs no special case for the expand control). Confirm the suppression set (INPUT / BUTTON / role=button / contenteditable / .q-dialog) actually matches the real result-card controls (checkbox, action buttons, expand div, Quick View dialog).
3. **genizah_app.py** — Confirm `self.results_table` is a QTableWidget with `installEventFilter(self)` already wired and a `COL_CHECKBOX` column constant. Confirm `QAbstractSlider` is NOT already imported (the plan adds it) and that `QAbstractSlider.SliderAction.SliderPageStepAdd` / `SliderPageStepSub` is the correct PyQt6 enum path (Assumption A2). Confirm the existing `eventFilter` signature/structure so adding a `Key_Space` branch composes (and that Space on the checkbox column currently toggles via Qt's native ItemIsUserCheckable so the fall-through to `super()` preserves it). Check nothing else in eventFilter already consumes Space.
4. **tests/conftest.py** — Confirm the `_GUI_TEST_FILES` set exists and the auto-gui-marker convention works as the plan assumes (adding `"test_space_scroll.py"` makes the desktop tests run in the gui slice).

## Specifically pressure-test
- **Web preventDefault reality:** will a client-side keydown listener on `document` actually be able to scroll `.q-scrollarea__container` and `preventDefault` the page's default Space-scroll, given Quasar's scroll-area structure? Any risk the listener fires on the wrong element or the page body still scrolls?
- **Desktop col == -1** (no current cell / nothing selected): plan routes it to scroll — correct? Any case where `currentColumn()` is COL_CHECKBOX but the cell is not actually a checkable checkbox (e.g. header, or a row without a checkbox)?
- **Don't-steal / a11y:** does the web suppression set cover every actionable control on a result card? Anything missed (e.g. links `<a>`, the folio nav `<` `>`, language/source selectors)?
- **GUARD-02 regressions:** any way the new web JS or desktop branch breaks existing Escape/`/` shortcuts, native PageUp/PageDown, typing in the search box, or the checkbox toggle?
- **Testability:** the plan's gui-test harness — is the thin-stub approach (bare QTableWidget + direct eventFilter call + mocked verticalScrollBar) actually sufficient to make the 3 desktop tests RED-before / GREEN-after without importing the full GenizahGUI (which would pull in Tantivy/SQLite)? Plan-checker flagged this as underspecified.

## Output format
For each finding: `[BLOCKER | HIGH | MEDIUM | LOW] <file/area> — <problem> — <concrete fix>`.
If the plan is sound, say so explicitly per area.
End with a single line: `VERDICT: APPROVE` or `VERDICT: CHANGES NEEDED` (use CHANGES NEEDED if any BLOCKER or HIGH remains).
