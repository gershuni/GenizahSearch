---
phase: 109-visual-similarity-merge-soft-retire
plan: "06"
subsystem: desktop/join_workbench + genizah_app + corrections_ui
tags: [visual-similarity, pick-mode, callback, reroute, tdd, gap-closure]
dependency_graph:
  requires:
    - phase: 109-04
      provides: "Pre-seeded tr() keys: 'Select as partner' + 'Pick a partner in the Join Lab'"
    - phase: 109-05
      provides: "set_source('visual') toggle ON + BLOCKER B set_anchor grid-clear (makes pre-anchor re-render safe)"
  provides:
    - "JoinWorkbenchWindow.pick_callback capability (set_pick_callback / clear_pick_callback / _rerender_candidate_cards)"
    - "_invoke_pick(callback, c): module-level pure helper (D-18 safe)"
    - "CandidateCard 'Select as partner' button in pick mode (_on_pick_partner slot)"
    - "open_joins_workbench accepts pick_callback param; set/clear BEFORE set_anchor (HIGH-4)"
    - "corrections_ui._show_vs_picker rerouted to open_joins_workbench(source='visual', pick_callback=)"
    - "btn_vs_pick tooltip updated to tr('Pick a partner in the Join Lab')"
    - "_show_vs_dialog marker re-worded: 'pending parity sign-off; normal AND pick callers rerouted' (MEDIUM-2)"
  affects:
    - "Plan 07: human parity UAT (D-14b) — Scenario J can now be verified"
tech_stack:
  added: []
  patterns:
    - "pick_callback set/cleared BEFORE set_anchor (HIGH-4 ordering) in open_joins_workbench"
    - "set_pick_callback/clear_pick_callback both call _rerender_candidate_cards() (belt-and-braces)"
    - "CandidateCard pick button: conditional on _pick_callback is not None; slot _on_pick_partner (no _vs_ prefix, D-18)"
    - "_invoke_pick(callback, c): pure module-level helper (headless-testable seam)"
    - "Reroute shape: drop VS pre-fetch in _show_vs_picker; open_joins_workbench handles VS load"
key_files:
  created: []
  modified:
    - desktop/join_workbench.py
    - genizah_app.py
    - corrections_ui.py
    - tests/test_join_workbench_vs.py
decisions:
  - "test_set_pick_callback_rerenders uses _FakeWinStub with JoinWorkbenchWindow._rerender_candidate_cards as a class attribute to make the unbound-call pattern work without Qt construction"
  - "_show_vs_picker drops VS pre-fetch and QMessageBox; the Workbench greys the toggle (D-08) when anchor has no VS data — no redundant pre-check needed"
  - "pick button is ADDITIVE (appended to lay before arow layout) so existing 6 action buttons remain present in normal mode"
metrics:
  duration: "~5 minutes"
  completed: "2026-06-07T18:06:31Z"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 4
requirements-completed: [JWB-12]
---

# Phase 109 Plan 06: JoinsDialog Pick-Mode Reroute to Workbench Summary

**JoinsDialog visual partner-picker opens the Join Workbench in pick capacity; picking a candidate fills fragment B and closes the picker; pick_callback wired BEFORE first render (HIGH-4); _show_vs_dialog retained one cycle with 'pending parity sign-off; normal AND pick callers rerouted' marker (MEDIUM-2).**

## Performance

- **Duration:** ~5 min
- **Started:** 2026-06-07T18:01:52Z
- **Completed:** 2026-06-07T18:06:31Z
- **Tasks:** 2 (Task 1 TDD RED+GREEN; Task 2 auto/structural)
- **Files modified:** 4

## Accomplishments

- Added `_invoke_pick(callback, c)` as a pure module-level helper (no Qt, testable, no `_vs_` prefix — D-18 safe)
- Added `JoinWorkbenchWindow._pick_callback = None` in `__init__`
- Added `set_pick_callback(cb)` / `clear_pick_callback()` — each store/clear the callback AND call `_rerender_candidate_cards()` (HIGH-4 belt-and-braces)
- Added `_rerender_candidate_cards()` — calls `pane.render_results()` via `getattr` guard (no-op if pane not built)
- Added "Select as partner" pick button on `CandidateCard` in pick mode: conditional on `_pick_callback is not None`; click handler `_on_pick_partner` calls `_invoke_pick` then `wb.close()`; no `_vs_` prefix (D-18 green)
- `open_joins_workbench` gains `pick_callback=None` param; sets/clears pick_callback BEFORE `set_anchor` (HIGH-4 ordering ensures first-page cards already show pick button)
- `corrections_ui._show_vs_picker` rerouted: drop VS pre-fetch + `QMessageBox` + `_show_vs_dialog` call; now calls `open_joins_workbench(res, source="visual", pick_callback=self._on_vs_pick)`
- `btn_vs_pick` tooltip changed from `tr("Pick from visual suggestions")` to `tr("Pick a partner in the Join Lab")` (Plan 04 key, no genizah_translations.py edit)
- `_show_vs_dialog` deprecation marker re-worded: "pending parity sign-off; normal AND pick callers rerouted" (MEDIUM-2); "EXCEPTION (D-12): ... remains ACTIVE" line removed (both callers now rerouted); code retained (D-11)

## Task Commits

| # | Phase | Commit | Type | Description |
|---|-------|--------|------|-------------|
| 1 | RED | f3f48515 | test | Task 1: add failing tests for pick_callback capability |
| 1 | GREEN | 5af85698 | feat | Task 1: pick_callback + 'Select as partner' affordance |
| 2 | — | 3e203383 | feat | Task 2: reroute + tooltip + deprecation marker |

## Files Created/Modified

- `desktop/join_workbench.py` — `_invoke_pick` helper, `_pick_callback = None` in __init__, `set_pick_callback` / `clear_pick_callback` / `_rerender_candidate_cards` methods, CandidateCard "Select as partner" pick button
- `genizah_app.py` — `open_joins_workbench` `pick_callback` param + HIGH-4 set/clear-before-anchor; `_show_vs_dialog` marker re-worded (MEDIUM-2)
- `corrections_ui.py` — `_show_vs_picker` rerouted to Workbench; `btn_vs_pick` tooltip updated
- `tests/test_join_workbench_vs.py` — 2 new Plan-06 tests: `test_invoke_pick_forwards_sysid_shelfmark` + `test_set_pick_callback_rerenders`

## Decisions Made

- `test_set_pick_callback_rerenders` uses `JoinWorkbenchWindow._rerender_candidate_cards` as a class attribute on the stub so the unbound-method pattern works without Qt construction (project lesson: headless tests miss Qt `__init__` ordering)
- `_show_vs_picker` drops the VS pre-fetch and `QMessageBox`: the Workbench handles VS loading and greys the toggle (D-08) when the anchor has no VS data — the old pre-check was redundant and required the old orange dialog path
- pick button is ADDITIVE: inserted via `lay.addWidget(pick_btn)` before `lay.addLayout(arow)` so the 6 existing icon-only buttons remain in all modes

## Deviations from Plan

None — plan executed exactly as written. All HIGH-4, MEDIUM-2, D-11, D-18 requirements satisfied.

## Known Stubs

None. All functionality is wired:
- `_invoke_pick` is a real module-level helper called by the pick button's click handler
- `set_pick_callback/clear_pick_callback` store the callback AND call `_rerender_candidate_cards()`
- `open_joins_workbench` sets/clears the callback before `set_anchor` (HIGH-4 ordering)
- `_show_vs_picker` calls `open_joins_workbench` with the real `_on_vs_pick` callback

## Threat Flags

No new network or auth surface. All three threat register items are addressed:

| Flag | File | Description |
|------|------|-------------|
| T-109-12 resolved | desktop/join_workbench.py + genizah_app.py | Stale pick_callback / stale pick buttons mitigated: open_joins_workbench sets/clears BEFORE set_anchor (HIGH-4); set/clear also calls _rerender_candidate_cards(); Plan 05 BLOCKER B set_anchor grid-clear wipes pre-anchor re-render's stale-anchor cards before new anchor repaints |

## Self-Check: PASSED

- `desktop/join_workbench.py` contains `self._pick_callback`, `def set_pick_callback`, `def clear_pick_callback`, `def _rerender_candidate_cards`: FOUND
- `desktop/join_workbench.py` `set_pick_callback`/`clear_pick_callback` each call `_rerender_candidate_cards`: CONFIRMED (lines 4399 + 4409)
- `desktop/join_workbench.py` contains `def _invoke_pick(callback, c)`: FOUND (line 279)
- `desktop/join_workbench.py` contains `tr("Select as partner")` on pick-only button and `_invoke_pick(...)` then `self.pane.wb.close()`: CONFIRMED
- `genizah_translations.py` NOT modified (Plan 04 owns it): CONFIRMED
- `tests/test_join_workbench_vs.py` contains `test_invoke_pick_forwards_sysid_shelfmark` and `test_set_pick_callback_rerenders`: CONFIRMED
- Task commits exist: f3f48515, 5af85698, 3e203383: CONFIRMED
- `python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_no_private.py tests/test_join_workbench_i18n.py tests/test_visual_similarity_dialog.py tests/test_join_workbench_construct.py -q` → 36 passed
- `python -m ruff check desktop/join_workbench.py genizah_app.py corrections_ui.py` → All checks passed
- `_show_vs_dialog` STILL exists (D-11 retention): CONFIRMED
- `_show_vs_dialog` marker contains "pending parity sign-off; normal AND pick callers rerouted": CONFIRMED
- "EXCEPTION (D-12)" line removed: CONFIRMED
- No `_vs_*` private calls in join_workbench.py (D-18): CONFIRMED (test passes)
- `corrections_ui._show_vs_picker` calls `open_joins_workbench` with `pick_callback=self._on_vs_pick`: CONFIRMED
- `corrections_ui._show_vs_picker` does NOT call `_show_vs_dialog`: CONFIRMED
- `btn_vs_pick` tooltip references `tr("Pick a partner in the Join Lab")`: CONFIRMED
