---
phase: 120-actions-persistence
plan: "05"
subsystem: puzzle-handoff
tags: [joins-lab, puzzle, bulk-add, staging, multitenant, tdd]
dependency_graph:
  requires: ["120-01", "120-04"]
  provides: ["ACT-02"]
  affects: [web/pages/puzzle.py, web/pages/joins_lab.py, web/components/candidate_grid.py]
tech_stack:
  added: []
  patterns: [safe_user_pop, asyncio.ensure_future, _after_delay, deferred-async-in-sync-page]
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - web/pages/puzzle.py
    - web/components/candidate_grid.py
    - genizah_translations.py
    - tests/test_joins_lab.py
decisions:
  - "puzzle_staging key consumed via safe_user_pop (one-shot atomic read+delete) in sync create_puzzle_page body — avoids stale handoff on later visits (Pitfall 6)"
  - "auto_add_bulk scheduled via asyncio.ensure_future(_after_delay(1.5, auto_add_bulk)) — mirrors single-fragment pattern; no await in sync page body (B2 compliance)"
  - "Redundant sleep(1.0) removed from auto_add_bulk body since _after_delay already delays 1.5s before calling the coroutine"
  - "on_add_to_puzzle callback in create_candidate_table is Optional[Callable] defaulting to None — backward-compatible with Phase-118/119 callers"
metrics:
  duration: "~25 minutes (including context-window handoff resume)"
  completed: "2026-06-21"
  tasks_completed: 2
  files_changed: 5
  tests_added: 15
---

# Phase 120 Plan 05: ACT-02 Bulk Add-to-Puzzle Summary

**One-liner:** Multitenant-safe puzzle_staging handoff from Joins Lab anchor+candidates to /puzzle via safe_user_set/pop + deferred async sequential add-by-sys_id.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| RED | Failing tests for Tasks 1+2 | `cda68059` | tests/test_joins_lab.py |
| 1 GREEN | Bulk handler in joins_lab + button in candidate_grid | `ef706508` | web/pages/joins_lab.py, web/components/candidate_grid.py, genizah_translations.py |
| 2 GREEN | Consume puzzle_staging in create_puzzle_page | `9ad680f2` | web/pages/puzzle.py, tests/test_joins_lab.py (ruff fix) |

## What Was Built

### Task 1 — Staging Write (joins_lab + candidate_grid)

`_on_add_to_puzzle_click()` in `web/pages/joins_lab.py`:
- Reads anchor `sys_id` from `_anchor_state` (warns if absent)
- Caps selected candidates to 20, prepends anchor at index 0
- Shows bilingual notification if >20 candidates truncated
- Writes `safe_user_set('puzzle_staging', {'schema_version': 1, 'fragments': [...], 'source': 'joins_lab', 'created_at': ...})`
- Navigates to `/puzzle`

`create_candidate_table()` in `web/components/candidate_grid.py`:
- New optional `on_add_to_puzzle: Optional[Callable] = None` parameter
- "Add to Puzzle" button (flat, `extension` icon) rendered inside the bulk action bar when callback provided
- Bilingual tooltip via `tr()`

Three translation keys added to `genizah_translations.py` (EN+HE).

### Task 2 — Staging Consume (puzzle.py)

`create_puzzle_page` (SYNC function):
- Pops `puzzle_staging` atomically via `safe_user_pop('puzzle_staging', None)` — one-shot clear prevents stale replay on subsequent visits
- Validates: `isinstance(bulk, dict) and schema_version==1 and fragments` non-empty
- Caps to 21 entries (anchor + max 20)
- Defines inner `async def auto_add_bulk()` that iterates `bulk_fragments` sequentially, resolving shelfmarks via `state.meta_mgr.get_meta_for_id()` and awaiting `_add_fragment_by_sys_id()` for each
- Schedules via `asyncio.ensure_future(_after_delay(1.5, auto_add_bulk))` — mirrors the existing single-fragment auto_add pattern; B2-compliant (no await in sync page body)

## TDD Gate Compliance

RED commit (`cda68059`): 8 structural tests FAIL (correct) + 7 pure-logic tests PASS
GREEN commits (`ef706508`, `9ad680f2`): all 15 tests PASS

## Test Results

```
15 passed (TestBulkAnchorAlwaysIncluded + TestBulkPuzzleStaging)
119 passed broader: test_joins_lab.py + test_no_raw_storage_access.py + test_puzzle_service.py
test_no_raw_storage_access.py: CLEAN (Phase-87 allowlist stays [])
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Lint] Removed unused `import re` in test**
- **Found during:** Task 2 GREEN, pre-commit ruff run
- **Issue:** `test_no_raw_app_storage_user_in_puzzle_for_staging` imported `re` but never used it
- **Fix:** Removed the `import re` line from the test method
- **Files modified:** tests/test_joins_lab.py
- **Commit:** `9ad680f2`

**2. [Rule 1 - Bug] Removed redundant sleep inside auto_add_bulk**
- **Found during:** Task 2 code review
- **Issue:** auto_add_bulk started with `await asyncio.sleep(1.0)` but _after_delay already sleeps 1.5s before calling the coroutine — would have added unnecessary 2.5s total delay
- **Fix:** Removed the redundant sleep from auto_add_bulk body; _after_delay's 1.5s delay is sufficient
- **Files modified:** web/pages/puzzle.py

## Known Stubs

None — puzzle_staging integration is fully wired end-to-end. Shelfmark resolution falls back to empty string on `meta_mgr` exception (graceful degradation, not a stub).

## Threat Flags

No new network endpoints, auth paths, or file access patterns introduced. The `puzzle_staging` key follows the existing `safe_user_*` pattern and is cleared atomically on consume (no persistent state leakage).

## Self-Check: PASSED

- [x] web/pages/joins_lab.py modified — `_on_add_to_puzzle_click` and `safe_user_set('puzzle_staging'...` present
- [x] web/pages/puzzle.py modified — `safe_user_pop('puzzle_staging'...` and `auto_add_bulk` present
- [x] web/components/candidate_grid.py modified — `on_add_to_puzzle` parameter present
- [x] genizah_translations.py modified — 'Add to Puzzle' key present
- [x] tests/test_joins_lab.py modified — TestBulkAnchorAlwaysIncluded + TestBulkPuzzleStaging classes present
- [x] Commits ef706508 and 9ad680f2 exist in git log
- [x] All 15 targeted tests pass GREEN
- [x] test_no_raw_storage_access.py clean (allowlist = [])
- [x] ruff clean on all 5 modified files
