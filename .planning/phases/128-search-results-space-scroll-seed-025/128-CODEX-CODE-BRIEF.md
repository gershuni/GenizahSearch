# Codex CODE review brief — GenizahSearch Phase 128 (Search Results Space-Scroll, SEED-025)

You are reviewing the ACTUAL implementation diff (post-execution) for correctness bugs, regressions, and quality. The plan was already pre-flight-approved; now review the CODE.

## The diff
`scratchpad/128-source.diff` (also read the live files for full context):
- `web/pages/search.py` — new `setup_space_scroll()` injected via `ui.run_javascript`, wired via `_after_delay(1.0, …)`.
- `genizah_app.py` — new module-level `space_scroll_action(current_column, checkbox_column, is_shift)` + `QAbstractSlider` import + a `Key_Space` branch in `GenizahGUI.eventFilter`.
- `tests/test_space_scroll.py` (non-gui), `tests/test_space_scroll_gui.py` (gui), `tests/conftest.py` (registers only the _gui file).

## Feature contract (what the code MUST do)
Space page-scrolls the search results (Shift+Space up) ONLY when no actionable result control holds focus. Web: client-side keydown on `document`; suppress (do nothing, let the control handle Space) when `activeElement` is INPUT/BUTTON/TEXTAREA/SELECT/`A`/`closest('a[href]')`/`role=button`/contentEditable, or an open `.q-dialog` exists; otherwise `preventDefault` + scroll `.results-scroll-area > .q-scrollarea__container` by ±clientHeight via `scrollTop +=`. Desktop: in the results QTableWidget, Space on the checkbox column toggles (fall through to Qt); other columns (and col == -1) page-scroll; Shift flips direction.

## Review focus — find real bugs
1. **Web keydown correctness:** Is `activeElement` null-safe? Does the suppression correctly let Space through to typing in the search box (INPUT/TEXTAREA) and to focused links/buttons? Could the handler `preventDefault` Space globally and break space-typing anywhere on the page (e.g. focus on body but user is mid-interaction)? Is the scroll target lookup null-safe if `.q-scrollarea__container` isn't found? Is the double-install guard (`_gsSpaceScrollInstalled`) correct? Does it leak/duplicate across SPA navigations or re-renders? Is the injected JS syntactically valid (IIFE, escaping)? Does `_after_delay(1.0, …)` reliably run after the results render?
2. **Web regression (GUARD-02):** Escape / `/` shortcuts in `handle_keyboard_shortcut` untouched and still work? Any interaction/conflict with the existing `ui.keyboard(on_key=…)` handler? Does Space still work normally in the search input?
3. **Desktop `space_scroll_action` + eventFilter:** Decision table correct (checkbox col → None; others incl. -1 → page_down; shift → page_up)? Is the `Key_Space` branch correctly gated on `source is self.results_table` so it does NOT hijack Space in OTHER widgets sharing the eventFilter (e.g. catalog table, other tables, the viewport)? Is `is_shift` read correctly from the event modifiers? Does it correctly `return True` only when scrolling and fall through (`super().eventFilter`) otherwise? Any risk it breaks native PageUp/PageDown, type-ahead, or the checkbox toggle? Does it handle key auto-repeat / press-vs-release (KeyPress vs KeyRelease) correctly (avoid double-scroll)?
4. **Tests:** Do the tests actually assert the behavior (not tautologies)? Does `test_desktop_space_scroll_action_decision` import and call the REAL `genizah_app.space_scroll_action` (not a copy)? Is the gui wiring test meaningful (asserts triggerAction called with the right enum)? Does the gui/non-gui split hold (conftest registers only `test_space_scroll_gui.py`)?
5. **Quality:** dead code, missing null-guards, i18n (none expected — keyboard behavior), anything sloppy.

## Output
For each finding: `[BLOCKER | HIGH | MEDIUM | LOW] <file:area> — <bug/problem> — <concrete fix>`.
If a focus area is correct, say so briefly. End with a single line: `VERDICT: APPROVE` or `VERDICT: CHANGES NEEDED` (CHANGES NEEDED if any BLOCKER or HIGH).
