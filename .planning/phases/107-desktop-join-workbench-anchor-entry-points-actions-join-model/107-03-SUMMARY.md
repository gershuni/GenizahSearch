---
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
plan: "03"
subsystem: desktop-join-workbench
tags: [join-workbench, desktop, qt, entry-points, host-wiring, public-wrappers]
dependency_graph:
  requires: [desktop/join_workbench.py (Plan 02 JoinWorkbenchWindow), genizah_translations.py (Plan 01 i18n), corrections_ui.JoinsDialog]
  provides: [GenizahGUI.open_joins_workbench, GenizahGUI.open_anchor_in_puzzle, GenizahGUI.open_anchor_as_join, ResultDialog Find-joins button, Browse Find-joins button]
  affects: [genizah_app.py, desktop/result_dialog.py — host files wired to JoinWorkbenchWindow]
tech_stack:
  added: []
  patterns:
    - Single-instance modeless window pattern (open_joins_workbench reuses self._join_workbench, re-anchors on second call)
    - Public-wrapper-over-private pattern (open_anchor_in_puzzle/open_anchor_as_join wrap _vs_* without deleting them)
    - Direct QPushButton entry-point pattern (must-fix #3: no _create_action_button for these rows)
    - Live-page-state anchor dict pattern (self.data + current_sys_id/p_num/text/uid overlaid per CODEX must-fix #1)
    - current_browse_p (not self.p) for Browse live-page state (CODEX must-fix #2)
key_files:
  created: []
  modified:
    - genizah_app.py
    - desktop/result_dialog.py
decisions:
  - "Used self.data (not self.current_result which does not exist) as the base anchor dict in ResultDialog — CODEX must-fix #1"
  - "Used current_browse_p (not self.p which does not exist) for Browse page number — CODEX must-fix #2"
  - "Both entry-point buttons are direct QPushButtons — not _create_action_button + add_btn (neither row uses that pattern) — CODEX must-fix #3"
  - "open_anchor_as_join leaves frag_b_input empty (anchor-only open, R-02) — scholar enters Fragment B freely"
  - "_vs_open_joins_with_partner and _vs_add_to_puzzle retained (VS dialog still uses them; Phase 109 retires the VS path)"
  - "Full-suite Windows access violation crash (os._walk + genizah_core mock threads) confirmed pre-existing — not caused by Phase 107 changes; Phase-107 suite 78/78 green"
metrics:
  duration: "~21 minutes (2026-06-04T08:32:00Z – 2026-06-04T08:53:56Z)"
  completed_date: "2026-06-04"
  tasks_completed: 4
  files_changed: 2
---

# Phase 107 Plan 03: Host Wiring, Entry Points, and Public Action Wrappers Summary

Single-sentence summary: GenizahGUI gains `open_joins_workbench` (modeless single-instance D-01/D-02), `open_anchor_in_puzzle` and `open_anchor_as_join` public wrappers (SC#5), plus direct QPushButton Find-joins entry points in ResultDialog (closes after launch) and Browse (stays open), each anchoring live page state.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add public wrappers + open_joins_workbench + _join_workbench ref to GenizahGUI | 77ab9ed4 | genizah_app.py |
| 2 | Add ResultDialog Find joins entry button (direct QPushButton, closes dialog after launch) | 28bea0ff | desktop/result_dialog.py |
| 3 | Add Browse ext_info_row Find joins entry button (direct QPushButton, Browse stays open) | c0187a4c | genizah_app.py |
| 4 | Full-suite + AST guards green after host wiring | (no code) | — |

## Verification Results

- `pytest tests/test_join_workbench.py tests/test_join_workbench_no_private.py tests/test_join_workbench_i18n.py` → **78 passed** (was 77+1 xfail; `test_phase107_host_keys_translated_and_wrapped` now passes as expected — must-fix #10 confirmed)
- `grep -c "_vs_" desktop/join_workbench.py` → 0 (SC#5 clean)
- `python -m ruff check genizah_app.py desktop/result_dialog.py desktop/join_workbench.py` → All checks passed
- `python -c "import ast; ast.parse(open('genizah_app.py',encoding='utf-8').read()); print('OK')"` → OK
- `python -c "import ast; ast.parse(open('desktop/result_dialog.py',encoding='utf-8').read()); print('OK')"` → OK
- Full suite: Phase-107 suites and desktop-relevant suites (59 passed, 1 skipped) pass; pre-existing Windows access-violation crash (os._walk + genizah_core mock threads at ~40% of full suite) is scope-external and pre-dates this plan

## Success Criteria Status

- [x] JWB-01: `open_joins_workbench(res)` opens modeless single-instance window, re-anchors + raises on second call (D-01/D-02)
- [x] SC#5/JWB-09: `open_anchor_in_puzzle` + `open_anchor_as_join` public wrappers exist; workbench path has zero `_vs_*` calls (AST guard confirms)
- [x] JWB-02/D-03 #1: ResultDialog Find joins (direct QPushButton, closes after launch, anchored via `self.data` + `current_sys_id`/`current_p_num`/`current_page_text`/`current_page_uid` per must-fix #1)
- [x] JWB-02/D-03 #2: Browse Find joins (direct QPushButton, stays open, anchored via `current_browse_sid`/`current_browse_p`/`browse_original_text` per must-fix #2)
- [x] must-fix #3: neither button uses `_create_action_button`; both are direct QPushButtons
- [x] must-fix #10: `test_phase107_host_keys_translated_and_wrapped` flips from xfail to pass (tr("Find joins") in both host files)
- [x] R-02: `open_anchor_as_join` leaves `frag_b_input` empty (anchor-only open); scholar enters Fragment B freely
- [x] `_vs_open_joins_with_partner` and `_vs_add_to_puzzle` retained (VS dialog still uses them)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan acceptance check `'self.current_result' not in src` false-positive on pre-existing `self.current_result_idx`**
- **Found during:** Task 2 verification
- **Issue:** The plan's automated verify check `assert 'self.current_result' not in src` is a substring match; `self.current_result_idx` (pre-existing in `result_dialog.py`) contains that substring, causing the check to fail even though the method correctly uses `self.data`
- **Fix:** Applied a precise regex check on the method body only (not the whole file) to confirm the new method uses `self.data` and does not reference `self.current_result` as a data attribute — the intent of the check is satisfied
- **Files modified:** none (acceptance check deviation only; code was correct)
- **Commit:** 28bea0ff

## Known Stubs

None. All entry points are fully wired to `open_joins_workbench`, which delegates to `JoinWorkbenchWindow.set_anchor()` (Plan 02). No placeholder data or hardcoded values.

## Threat Flags

No new threat surface introduced beyond what was modeled in the plan's threat register:
- T-107-03-01 (Fragment B injection via JoinsDialog) — mitigated by delegation to existing NormalizingCompleter path
- T-107-03-02 (public wrappers) — accepted (thin pass-throughs, no privilege change)
- T-107-03-03 (duplicate windows) — mitigated by single `_join_workbench` instance + `set_anchor` + `raise_()`

No new network endpoints, storage, auth, or schema changes.

## Self-Check: PASSED

Files exist:
- `genizah_app.py` — FOUND (contains `def open_joins_workbench`, `def open_anchor_in_puzzle`, `def open_anchor_as_join`, `self._join_workbench = None`, `def _browse_open_join_workbench`, `self.btn_b_find_joins`)
- `desktop/result_dialog.py` — FOUND (contains `Find joins`, `def _open_join_workbench`, `self.btn_rd_find_joins`)

Commits exist (verified via `git log --oneline`):
- 77ab9ed4 — FOUND (Task 1: GenizahGUI host methods)
- 28bea0ff — FOUND (Task 2: ResultDialog entry button)
- c0187a4c — FOUND (Task 3: Browse entry button)

Key symbols confirmed:
- `def open_joins_workbench` in genizah_app.py ✓
- `def open_anchor_in_puzzle` in genizah_app.py ✓
- `def open_anchor_as_join` in genizah_app.py ✓
- `self._join_workbench = None` in genizah_app.py ✓
- `from desktop.join_workbench import JoinWorkbenchWindow` in genizah_app.py ✓
- `def _vs_open_joins_with_partner` in genizah_app.py ✓ (retained)
- `def _vs_add_to_puzzle` in genizah_app.py ✓ (retained)
- `self.btn_b_find_joins = QPushButton(` in genizah_app.py ✓
- `ext_info_row.addWidget(self.btn_b_find_joins)` in genizah_app.py ✓
- `def _browse_open_join_workbench` in genizah_app.py ✓
- `self.btn_rd_find_joins = QPushButton(` in desktop/result_dialog.py ✓
- `action_row.addWidget(self.btn_rd_find_joins)` in desktop/result_dialog.py ✓
- `def _open_join_workbench` in desktop/result_dialog.py ✓
- `dict(self.data)` in desktop/result_dialog.py ✓ (must-fix #1)
- `current_p_num` in _open_join_workbench body ✓ (must-fix #1)
- `current_browse_p` in _browse_open_join_workbench body ✓ (must-fix #2)
