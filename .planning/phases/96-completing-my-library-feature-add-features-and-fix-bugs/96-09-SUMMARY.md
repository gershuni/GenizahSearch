---
phase: 96
plan: "09"
subsystem: desktop-my-library
tags: [phase-96, my-library, polish, docs, freestyle, blocker-5]
dependency_graph:
  requires: [96-02, 96-03, 96-04, 96-05, 96-06, 96-07, 96-08]
  provides: [phase-96-close, D-F1-fixed, D-F4-fixed, D-F5-fixed]
  affects: [desktop/my_library_tab.py, desktop/result_dialog.py, genizah_app.py, docs/OPEN_ISSUES.md, CHANGELOG.md, CLAUDE.md]
tech_stack:
  added: []
  patterns:
    - flush_pending() QTimer drain pattern (closeEvent persistence race)
    - autoDefault=False + setFocus() (dialog keyboard focus discipline)
    - p_num int() coercion (header string → int defensive parse)
    - _UnifiedFileTreeWidget 3-column tree (QSplitter replacement)
key_files:
  created: []
  modified:
    - desktop/my_library_tab.py
    - desktop/result_dialog.py
    - genizah_app.py
    - tests/test_local_filter_cascade.py
    - tests/test_local_hit_highlighting.py
    - tests/test_local_nav_page_chunk.py
    - tests/test_local_optout_persistence.py
    - docs/OPEN_ISSUES.md
    - CHANGELOG.md
    - CLAUDE.md
decisions:
  - "UX redesign: replaced QSplitter + _OptoutTreeWidget + _status_table with single _UnifiedFileTreeWidget (3 columns: Filename checkbox / Pages / Status)"
  - "flush_pending() added to closeEvent before _save_session() to drain 150ms debounce timer on close"
  - "autoDefault=False on Prev/Next Result buttons; setFocus(spin_page) after LOCAL page load"
  - "p_num coerced via int() in _open_local_browse() — _build_local_result_dict stores as string from header parse"
  - "D-F2 (OCR) and D-F3 (side-by-side PDF) remain open — explicitly deferred to v7.15+ per CONTEXT D-01"
metrics:
  duration: "~90 minutes (continuation session)"
  completed: "2026-05-24"
  tasks_completed: 3
  tasks_total: 5
  files_changed: 10
---

# Phase 96 Plan 09: Freestyle Polish + Phase Close Summary

**One-liner:** Five UAT bugs fixed (unified tree widget, persistence race, Enter-key focus, p_num coercion, LOCAL widget leak) + BLOCKER-5 stale-skip audit (10 skips → positive assertions) + docs closure (OPEN_ISSUES, CHANGELOG, CLAUDE.md).

## Tasks Completed

| # | Task | Commit | Status |
|---|------|--------|--------|
| 1 | Freestyle polish bugs + BLOCKER-5 stale-skip audit | `e0ee9156` | Done |
| 2 | Update docs/OPEN_ISSUES.md (D-F1/D-F4/D-F5 closed) | `d2f2459d` | Done |
| 3 | CHANGELOG [vNEXT] + CLAUDE.md Recently Changed | `edc8040f` | Done |
| 4 | checkpoint:decision — version bump strategy | — | Awaiting |
| 5 | Version bump + final verification gate | — | Awaiting |

## Task 1 Details: UAT Bugs Fixed

### Bug 1 — MUST-FIX UX Redesign (Gap from 96-06 UAT)
**File:** `desktop/my_library_tab.py`
Replaced the QSplitter + separate `_OptoutTreeWidget` + `_status_table` with a single `_UnifiedFileTreeWidget` (3 columns: Filename checkbox / Pages / Status). The `_leaf_by_path` dict provides O(1) lookup when `file_finished` signals arrive. External callers use the `self._optout_tree` alias which maps to `self._unified_tree`. `reset_for_scan()` clears column data without destroying tree structure.

### Bug 2 — P2 Persistence Race (Gap from 96-06 UAT)
**File:** `genizah_app.py` (`closeEvent`)
Added `flush_pending()` call before `_save_session()`. The 150ms debounce `QTimer.singleShot` is abandoned when Qt event loop shuts down — the flush stops the timer and immediately calls `_commit_changes()` synchronously, ensuring the final opt-out state is captured in the session JSON.

### Bug 3 — P1 Enter Focus (Gap from 96-08 UAT)
**File:** `desktop/result_dialog.py`
Added `self.btn_res_prev.setAutoDefault(False)` and `self.btn_res_next.setAutoDefault(False)` after button declarations. Added `self.spin_page.setFocus()` at end of `load_local_page()`. This prevents Enter from firing the previously-focused Prev Result button when the user wants to jump pages.

### Bug 4 — P1 Browse at Page 1 (Gap from 96-08 UAT)
**File:** `genizah_app.py` (`_open_local_browse()`)
`_build_local_result_dict` stores `p_num` as a string (parsed from `{sys_id}_LOCAL_P{n}_F{file_id}` header). The existing `isinstance(hit_p, int)` guard always failed. Fixed with `int(hit_p)` in a try/except, so a hit on page 7 now correctly opens Browse at page 7.

### Bug 5 — P2 LOCAL Widget Leak (Gap from 96-08 UAT)
**File:** `genizah_app.py` (`open_result_in_browse()` non-LOCAL branch)
Added `self._show_local_browse_controls(False)` inside the non-LOCAL branch so the LOCAL prev/next/view-all widgets are hidden when loading a Genizah manuscript.

### BLOCKER-5: Stale pytest.skip Audit
Converted 10 stale `pytest.skip("Phase 96 ... not yet implemented")` markers across 4 test files to positive assertions:
- `tests/test_local_filter_cascade.py` — 1 skip → assert (96-05 shipped)
- `tests/test_local_hit_highlighting.py` — 4 skips → asserts (96-03 shipped)
- `tests/test_local_nav_page_chunk.py` — 4 skips → direct imports (96-03/96-08 shipped)
- `tests/test_local_optout_persistence.py` — 1 skip → direct import (96-04 shipped)

`grep -rn 'pytest.skip.*Phase 96.*not yet implemented' tests/` now returns empty.

### Cross-AI Review HIGH Invariants (all pass)
- **HIGH #1 (algebra):** `_commit_changes` uses `difference_update` + `update`, no `.clear()` on global opt-out set.
- **HIGH #2 / D-04.1 (filter-out):** `_build_local_result_dict` returns `None` on regex non-match; `_query_local_index` skips `None` hits.
- **HIGH #4 (HTML-escape):** `_open_local_browse_page` uses `html.escape` before `\n→<br>` replacement.

## Task 2 Details: OPEN_ISSUES.md

- D-F1 → ✅ Fixed (2026-05-24) — plans 96-04/96-05/96-06
- D-F4 → ✅ Fixed (2026-05-24) — plan 96-02
- D-F5 → ✅ Fixed (2026-05-24) — plan 96-03
- D-F2, D-F3 → unchanged (remain Open, deferred to v7.15+)
- Last Updated header bumped to 2026-05-24

## Task 3 Details: Docs

- `CHANGELOG.md`: Added `[vNEXT]` section between `[Unreleased]` and `[7.14.0]` covering the 5 UAT bug fixes, D-F1/D-F4/D-F5 closures, and BLOCKER-5 skip audit.
- `CLAUDE.md`: Prepended "Phase 96 — My Library Polish (2026-05-24)" bullet to "Recently Changed" list.

## Deviations from Plan

None — plan executed exactly as written. Task 1 Polish Bucket had 5 confirmed UAT bugs from 96-06-UAT.md and 96-08-UAT.md; all 5 fixed inline (all qualify as "small" per CONTEXT D-15 criteria).

## Known Stubs

None. All functionality is wired and tested.

## Threat Flags

None. No new network endpoints, auth paths, or schema changes introduced. All changes are desktop-local UI and test files.

## Self-Check: PASSED

Commits verified:
- `e0ee9156` — fix(96-09): 5 UAT polish bugs + BLOCKER-5 stale-skip audit
- `d2f2459d` — docs(96-09): close D-F1/D-F4/D-F5 in OPEN_ISSUES.md
- `edc8040f` — docs(96-09): CHANGELOG [vNEXT] entry + CLAUDE.md Recently Changed

Files modified exist and were committed. All 27 Phase 96 tests pass (verified before commit `e0ee9156`). ruff clean on all 3 modified source files.

Tasks 4 (checkpoint:decision) and 5 (version bump + verification) are pending user decision at checkpoint.
