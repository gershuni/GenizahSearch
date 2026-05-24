---
phase: "96"
plan: "09"
iteration: 8
subsystem: desktop-my-library
tags: [fix, polish, session-restore, opt-out-persistence, local-buttons, corpus-scope]
dependency_graph:
  requires: [96-09-SUMMARY-FIX7.md]
  provides: [phase-96-closeout-ready]
  affects: [genizah_app.py, desktop/result_dialog.py, docs/OPEN_ISSUES.md]
tech_stack:
  patterns: [finally-block-cleanup, early-restore-gate, local-hit-button-guard]
key_files:
  modified:
    - genizah_app.py
    - desktop/result_dialog.py
    - docs/OPEN_ISSUES.md
  created:
    - tests/test_local_nav_codex_fix8.py
decisions:
  - "I1+I2: notify_session_restored() moved into the finally block via _notify_done guard — fires on ALL code paths (no-data, user-declined, exception), not just successful full restore"
  - "I1b: corpus scope restored BEFORE has_data gate — combo shows saved value even when no search results exist in session file"
  - "I3: setEnabled(False) on 6 community buttons + version combo in both load_result_by_index (_is_local_hit branch) and _open_local_browse_page; re-enabled on non-LOCAL hits"
metrics:
  duration: "~30 minutes"
  completed: "2026-05-24T18:30:00Z"
  tasks_completed: 3
  files_modified: 3
---

# Phase 96 Plan 09: My Library Polish — Iteration 8 (Last Polish Before Close) Summary

Closes Phase 96. Three UAT-verified issues remaining after iteration 7 are fixed.
Root-cause analysis for the opt-out persistence bug that was "fixed" in iterations
1–7 but kept regressing is documented here definitively.

## Root Cause Analysis: Why Opt-Out Persistence Broke 8 Times

### The fundamental issue: `notify_session_restored()` placement

The opt-out persistence bug has been "fixed" multiple times because each iteration
addressed a SYMPTOM without finding the root cause.

**Iterations 1–4:** Tried QTimer delays of various lengths — always a timing race.

**Iterations 5–6:** Tried `closeEvent` flush (`flush_pending()`) — correct for the
WRITE side but did not fix the READ side.

**Iteration 7 (the regression):** Removed the 300ms QTimer from
`_refresh_folder_list_ui()` and added `notify_session_restored()` at the end of
`_restore_session()`. However, placed it AFTER the `try/except/finally` block.
This was the **regression that broke everything again**: Python `return` statements
inside the `try` block cause the function to return after running `finally` — the
code AFTER `finally` never executes. Early return paths:
- `if not has_data: return` — user has no search results (opt-outs only)
- `if reply != QMessageBox.StandardButton.Yes: return` — user clicked No
- Any exception in the try block

When `notify_session_restored()` was not called, `_auto_select_first_folder()`
was never called, so the folder list had items but none selected, and the file tree
was never populated with correct checkbox states.

**Iteration 8 (definitive fix):** Moved `notify_session_restored()` INSIDE the
`finally` block using a `_notify_done` guard flag. The `finally` block runs
unconditionally regardless of how the `try` block exits (normal return, early
return, or exception). Now `_auto_select_first_folder` always fires, always reads
`_local_file_optouts` which is already set at line 24248 (before the `has_data`
gate), and the tree always shows correct checkbox states.

### Why the opt-outs ARE in memory but tree didn't show them

Even before this fix, `_local_file_optouts` WAS populated at line 24248 — before
the `has_data` check. So the set survived in memory. But the TREE was never
populated because the auto-select never fired. When the user manually clicked a
folder, `populate_for_folder` would read the in-memory set correctly. But on the
NEXT close, if `_displayed_paths` was populated (from the manual click), the
`_commit_changes` diff would work correctly. If NOT clicked, `_displayed_paths`
was empty, the diff was a no-op, and `_save_session` saved the unchanged set.

The user experience: "opt-outs not remembered" was actually "tree doesn't show
them until I click" — which FELT like a bug because the tree appeared to start
empty.

## What Was Done

### Issue 1 + 2 — Session Restore Regression + Corpus Scope

**Root cause (confirmed above):** `notify_session_restored()` was placed after
`try/except/finally` — early returns bypassed it.

**Fix:**
1. Added `_notify_done = False` flag before the `try` block.
2. In the `finally` block: if `not _notify_done`, call `notify_session_restored()`
   and set `_notify_done = True`.
3. Restored `search_corpus_scope` from session JSON BEFORE the `has_data` gate so
   the `corpus_scope_combo` (Genizah/Local/ALL) shows the saved value even when
   there are no search results in the session file.
4. Removed the now-duplicate corpus scope restoration that happened later in the
   function body (replaced with an explanatory comment).

**Files:** `genizah_app.py` — `_restore_session()` method.

### Issue 3 — Genizah-Only Buttons Disabled for LOCAL Hits

**Root cause:** `_open_local_browse_page()` and `load_result_by_index()` did not
disable community buttons (Edit, Comment, Corrections, Joins, Puzzle, View on
Ktiv, Version) when loading a LOCAL file. These features require Friedberg/NLI
catalog entries and have no meaning for user-owned local files.

**Fix in ResultDialog (`desktop/result_dialog.py`):**
In `load_result_by_index()`, after the `_is_local_hit` detection block:
- If `_is_local_hit`: call `setEnabled(False)` on `btn_img`, `btn_add_to_puzzle`,
  `btn_rd_edit`, `btn_comment`, `btn_view_corrections`, `btn_joins`, and
  `rd_version_combo`.
- Else (Genizah hit): call `setEnabled(True)` on the same buttons to restore them
  after returning from a LOCAL result. (`rd_version_combo` is managed separately
  by the enrichment callback for Genizah hits.)

**Fix in Browse panel (`genizah_app.py`):**
In `_open_local_browse_page()`, after `_show_local_browse_controls(True)`:
- Call `setEnabled(False)` on `btn_b_catalog`, `btn_b_add_to_puzzle`,
  `btn_b_edit`, `btn_b_comment`, `btn_b_view_corrections`, `btn_b_joins`,
  `btn_b_view_comments`, and `browse_version_combo`.
- These are re-enabled by the normal Genizah enrichment callback when the user
  navigates to a Genizah manuscript (the `on_meta_loaded` callback at line 7885).

## Tests Added

`tests/test_local_nav_codex_fix8.py` — 9 new tests:

| Test | Covers |
|------|--------|
| `test_notify_session_restored_in_finally_block` | I1: finally-block placement verified |
| `test_notify_session_restored_not_after_try_block` | I1: not only after try/except/finally |
| `test_corpus_scope_restored_before_has_data_gate` | I1b: corpus scope before has_data |
| `test_optout_survives_save_restore_cycle` | I2: end-to-end save→restore lifecycle |
| `test_corpus_scope_survives_save_restore_cycle_no_results` | I2: scope survives with no results |
| `test_empty_optouts_roundtrip` | I2: empty set round-trips correctly |
| `test_restore_session_loads_optouts_before_has_data` | I2: structural load order |
| `test_result_dialog_disables_buttons_for_local_hits` | I3a: ResultDialog LOCAL disabling |
| `test_browse_panel_disables_buttons_for_local_hits` | I3b: Browse panel LOCAL disabling |

Full suite: **2589 passed, 24 skipped, 4 xfailed** (up from 2580 in iter-7).

## OPEN_ISSUES.md Updates

- **D-F6**: Updated from iter-7 fix to iter-8 fix — full root cause documented.
- **D-F11** (new): Genizah-only buttons for LOCAL hits — marked Fixed (2026-05-24).

## Deviations from Plan

None. All three issues addressed as specified. No architectural changes required.

## Commits

| Hash | Description |
|------|-------------|
| `97676224` | fix(96-09): I1+I2 — notify_session_restored in finally; corpus scope before has_data gate |
| `2bb9779d` | fix(96-09): I3 — disable Genizah-only buttons for LOCAL hits (ResultDialog + Browse) |
| `c85af6f4` | test(96-09): add fix-8 regression tests (9 new tests) |

## Self-Check

- [x] All 3 commits exist in git log
- [x] Full test suite: 2589 passed, 24 skipped, 4 xfailed — no regressions
- [x] ruff clean on all modified files
- [x] iter-7 regression tests (test_local_nav_codex_fix7.py) still pass — 12/12
- [x] iter-4 regression tests (test_local_optout_persistence.py) still pass — 5/5
- [x] OPEN_ISSUES.md D-F6 updated to iter-8 root cause; D-F11 added as Fixed
- [x] No STATE.md / ROADMAP.md modified (per objective)

## Self-Check: PASSED
