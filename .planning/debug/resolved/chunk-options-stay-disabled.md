---
status: resolved
trigger: "two bugs in desktop app: 2. Composition search, for Chunk Search the frequency and search type options are disabled after choosing Letter-level search and switching back."
created: 2026-09-17T00:00:00Z
updated: 2026-09-17T00:10:00Z
---

## Current Focus

reasoning_checkpoint:
  hypothesis: "_apply_passage_mode_ui(False) (genizah_app.py:18573-18612) never re-enables comp_mode_combo/spin_freq because the elif branch explicitly excludes any name in _PASSAGE_CONTROLS_LAB_ALSO_OWNS, and no caller of the revert path (16872, 18686, 18708) ever re-derives their enabled state from the Lab predicate afterwards -- so they stay exactly as letter-level mode left them (disabled) until Lab Mode is toggled on and off by hand."
  confirming_evidence:
    - "Read genizah_app.py:18588-18612 directly: `if on: w.setEnabled(False)` / `elif name not in self._PASSAGE_CONTROLS_LAB_ALSO_OWNS: w.setEnabled(True)` -- for the two Lab-owned names, on=False takes neither branch, so setEnabled is never called on revert."
    - "update_lab_ui_state (18748-18766), the only place that ever calls setEnabled(not checked) on these two widgets, is only reachable from on_lab_mode_toggled_search/_comp (18794/18814) -- grepped, no other caller."
    - "Wrote and ran a real (unstubbed) unit test calling APP._apply_passage_mode_ui(w, True) then (w, False) with Lab off: comp_mode_combo.enabled and spin_freq.enabled were both False after the revert, confirming the bug mechanically rather than by reading code."
  falsification_test: "If some other code path called update_lab_ui_state (or re-derived `not lab_btn.isChecked()`) right after _apply_passage_mode_ui(False), the two widgets would come back enabled whenever Lab was off -- the failing test above would pass instead."
  fix_rationale: "Re-enable comp_mode_combo/spin_freq on revert by evaluating the SAME Lab predicate _apply_passage_mode_ui already has everything it needs to evaluate (btn_lab_mode_toggle_comp.isChecked()), inline in the loop -- addresses the root cause (the predicate is never consulted on revert) rather than a symptom (e.g. blindly setEnabled(True), which would incorrectly re-enable a control Lab is legitimately holding disabled)."
  blind_spots: "Did not exercise the real Qt widgets (offscreen QApplication) -- used the file's existing Qt-free stub pattern (tests/test_desktop_passage_gate.py) instead, consistent with how every other test in that file exercises this window class."
  candidate_causes:
    - "code: _apply_passage_mode_ui's revert branch omits the Lab-predicate re-derivation for the two shared-ownership controls (confirmed root cause)."
    - "config/environment: none found -- this is a pure control-flow omission, not an environment or data issue. AND-gate below is 'no'."
  and_gate: "no -- a single code-level omission fully explains the symptom; no second contributing condition (config/environment/data) is needed to reproduce it every time letter-level is deselected."

hypothesis: CONFIRMED and FIXED. Verified GREEN.
next_action: none -- awaiting human confirmation in the real desktop app, then archive.

## Symptoms

expected: In the Composition tab, switching the search method from "Letter-level search" back to "Chunk search" should re-enable the chunk-only controls (search type combo and frequency spinner) exactly as they were before, unless Lab mode is holding them disabled
actual: After choosing Letter-level search and switching back to Chunk search, the frequency and search type options remain disabled (greyed out)
errors: None reported
reproduction: Desktop app, Composition tab: method combo -> "New! Letter-level search" (index must be built), then method combo -> "Chunk search (slower)"; observe search type combo and frequency spinner are disabled
started: Presumably since v9.0.0 desktop / Phase 146 (letter-level search, 2026-08-26); reported 2026-09-17

## Pre-gathered evidence (orchestrator, 2026-09-17)

- genizah_app.py:16242-16249 `_PASSAGE_FORCED_CONTROLS` = comp_mode_combo(index 0), spin_chunk(5), spin_freq(50), spin_min_chunks(1), boundary_mode_combo(index 0); `_PASSAGE_CONTROLS_LAB_ALSO_OWNS = ('comp_mode_combo', 'spin_freq')`.
- genizah_app.py:18573-18608 `_apply_passage_mode_ui(on)`: for on=True every forced control gets `setEnabled(False)`; for on=False only controls NOT in `_PASSAGE_CONTROLS_LAB_ALSO_OWNS` get `setEnabled(True)`. The docstring says the two Lab-owned controls "go back through Lab's own predicate", but the function body never calls it.
- genizah_app.py:18748 `update_lab_ui_state(checked)` is the only place that sets enabled state for comp_mode_combo/spin_freq (`setEnabled(not checked)`). Its only callers are lines 18794 and 18814 (the Lab toggles). It is NOT called from `_apply_passage_mode_ui`, `_revert_comp_method_to_chunk` (18679), or `_on_comp_method_changed` (18690).
- Existing tests touching this area: tests/test_desktop_passage_gate.py and tests/test_desktop_multi_witness.py (grep hit on `_apply_passage_mode_ui`). Use them as the pattern for an offscreen Qt test (QT_QPA_PLATFORM=offscreen; run the single file, never the whole suite).
- Fix shape to evaluate: on revert, re-enable the two Lab-owned controls through the Lab predicate, e.g. `enabled = not (lab_btn is not None and lab_btn.isChecked())` using `btn_lab_mode_toggle_comp` (the same predicate `_refresh_comp_method_enabled` at 18659 uses), or call `self.update_lab_ui_state(lab_on)` restricted to those two controls.

## Eliminated

(none -- the pre-gathered hypothesis was confirmed on the first test)

## Evidence

- timestamp: 2026-09-17T00:05:00Z
  checked: genizah_app.py:18577-18626 `_apply_passage_mode_ui` (pre-fix source)
  found: "`if on: w.setEnabled(False)` / `elif name not in self._PASSAGE_CONTROLS_LAB_ALSO_OWNS: w.setEnabled(True)` -- for comp_mode_combo/spin_freq on revert (on=False), NEITHER branch runs, so setEnabled is never called."
  implication: Confirms the docstring's claim ("go back through Lab's own predicate") was aspirational, not implemented.
- timestamp: 2026-09-17T00:06:00Z
  checked: "grep for every setEnabled call on comp_mode_combo/spin_freq across genizah_app.py"
  found: "Exactly two writers: `_apply_passage_mode_ui` (the bug) and `update_lab_ui_state` (18770-18771, correct, only reachable from the two Lab-toggle handlers)."
  implication: No third code path duplicates or interferes with the fix; fixing the one function is sufficient.
- timestamp: 2026-09-17T00:08:00Z
  checked: "New unstubbed test test_reverting_from_letter_level_re_enables_the_lab_owned_controls, run against pre-fix code"
  found: "RED as predicted -- comp_mode_combo.enabled and spin_freq.enabled both False after APP._apply_passage_mode_ui(w, False) with Lab off."
  implication: Mechanically confirmed root cause (not just read from source).
- timestamp: 2026-09-17T00:12:00Z
  checked: "Same test plus two siblings (Lab-on case, single-owner-controls case), run against post-fix code; full tests/test_desktop_passage_gate.py (194 tests) and tests/test_desktop_multi_witness.py (169 tests)"
  found: "All GREEN. Reverting the fix (git stash) reproduced the original RED failure exactly; restoring it turned GREEN again."
  implication: Fix verified against the original symptom, and does not regress adjacent passage/Lab-mode/multi-witness behavior.
- timestamp: 2026-09-17T00:13:00Z
  checked: "All 6 callers of _apply_passage_mode_ui (16389, 16863, 16872, 18449 via _apply_default_comp_method, 18700 via _revert_comp_method_to_chunk, 18700-area _on_comp_method_changed) including the session-restore path _restore_comp_passage_preferences (16826-16876, calls _apply_passage_mode_ui(False) at 16872)"
  found: "All 6 route through the single shared function; none re-implements enable/disable logic separately."
  implication: The restart/session-restore path named in the project constraints is covered by the same fix -- no separate patch needed there.

## Resolution

root_cause: "`_apply_passage_mode_ui`'s revert branch (on=False) excluded `comp_mode_combo` and `spin_freq` from re-enabling because they are also owned by Lab Mode, but never substituted the Lab predicate in their place -- so once letter-level mode disabled them, nothing on any revert path (manual switch-back, session restore, or the letter-level default demoting back to chunk) ever re-enabled them again until Lab Mode was toggled on and off by hand."
fix: "In `_apply_passage_mode_ui`, read `btn_lab_mode_toggle_comp.isChecked()` once per call and, for the two Lab-owned controls on revert, `setEnabled(not lab_on)` instead of skipping them; the other three forced controls keep their unconditional `setEnabled(True)`. Updated the misleading docstring (18578-18586) and the block comment above `_PASSAGE_FORCED_CONTROLS` (16234-16244) to describe what the code now actually does."
verification: "tests/test_desktop_passage_gate.py::test_reverting_from_letter_level_re_enables_the_lab_owned_controls (RED before the fix, GREEN after; reproduced RED again via git stash to confirm the fix -- not an unrelated change -- is what closes the gap), plus two companion tests (Lab-on case stays disabled; the three single-owner controls are unaffected). Full file: 194/194 passed. tests/test_desktop_multi_witness.py: 169/169 passed (adjacent Lab/passage code, unaffected)."
oracle_type: derived
files_changed:
  - genizah_app.py (fix + two comments)
  - tests/test_desktop_passage_gate.py (3 new regression tests + 2 supporting stub classes)
