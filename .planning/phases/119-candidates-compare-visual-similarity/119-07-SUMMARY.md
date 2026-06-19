---
phase: "119"
plan: "07"
subsystem: joins-lab
tags: [bug-fix, uat-defect, visual-similarity, candidate-grid, tdd]
dependency_graph:
  requires: ["119-06"]
  provides: ["G2-fix", "A1-fix", "A2-fix", "A3-fix", "A4-fix"]
  affects: ["web/pages/joins_lab.py"]
tech_stack:
  added: []
  patterns:
    - "_raw_text_candidates raw baseline + _compute_display_candidates() derived display set"
    - "VS unavailability affordance via run.io_bound probe at first toggle"
    - "dataclasses.replace for frozen Candidate enrichment"
    - "named sync closures for run.io_bound (AST off-loop guard contract)"
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - tests/test_vs_adapter.py
    - tests/test_joins_lab_page.py
    - tests/test_joins_lab_off_loop.py
    - tests/test_candidate_enrichment.py
    - genizah_translations.py
decisions:
  - "Introduced _raw_text_candidates as the canonical pre-merge baseline; _all_candidates is now derived (computed) not the merge input — prevents VS toggle from filtering the already-merged set (G2)"
  - "_compute_display_candidates() is a page-local helper that always recomputes from the raw baseline — single source of truth for display candidates"
  - "VS availability probed off-loop via _check_vs_service_available() module-level sync helper on first toggle call (F-VSavail)"
  - "A4 metadata enrichment done inside _do_vs_fetch_and_update via run_vs_meta_core() named closure; _fetch_vs_candidates stays module-level (cannot see page-local executor)"
  - "load_anchor lambda replaced with run_get_meta_for_anchor() named closure to satisfy AST off-loop guard (Rule 2 auto-fix)"
metrics:
  duration: "~35 minutes"
  completed: "2026-06-19T13:21:29Z"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 6
---

# Phase 119 Plan 07: G2/A1/A2/A3/A4 UAT Defect Fixes Summary

**One-liner:** Fixed VS toggle pollution (raw baseline split), dead table-view code (branch on view mode), anchor page/shelfmark in Compare, anchor in enrichment batch for size-mismatch, and VS-only candidate metadata enrichment off-loop.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | G2 raw baseline + _compute_display_candidates (GREEN) | 15c42faf | joins_lab.py, test_vs_adapter.py |
| 2 | A2 grid/table toggle + F-VSavail affordance | 9bd54d59 | joins_lab.py, test_joins_lab_page.py, genizah_translations.py |
| 3 | A1/A3/A4 anchor-page, enrichment anchor batch, VS metadata | 73996cbf | joins_lab.py, test_candidate_enrichment.py, test_joins_lab_off_loop.py |

## What Was Built

### G2 — VS Toggle Shows Same Candidates On/Off (Root Cause Fixed)

The root cause: `_re_render_candidates_surface()` was reading `_all_candidates` which had already been VS-merged in Step-9. Toggling VS on/off re-filtered the same merged set.

Fix: introduced `_raw_text_candidates: list = []` as the canonical pre-merge text+cross-side baseline. `_compute_display_candidates()` always calls `_apply_vs_merge(_raw_text_candidates, _vs_candidates, _vs_on['value'], builder_has_query)` — recomputing from the raw baseline on every call. `_all_candidates` is now DERIVED output, not the merge input.

### A2 — Table View Dead Code

`_render_candidates_surface()` always called `create_candidate_grid()`, ignoring `_view_mode['value']`. Added branch: `'table'` → `create_candidate_table(...)`, `'grid'` → `create_candidate_grid(...)`. Added `_on_view_toggle_click()` handler and Grid/Table toggle button wired to it. Toggle does NOT clear `_triage` or reset `_current_page` (D-10 behavioral spec).

### A1 — Compare Anchor Opens at Page 1 with No Shelfmark

`_anchor_state` extended with `'page': None, 'shelfmark': ''`. `load_anchor` now stores `_anchor_state['page'] = page` and `_anchor_state['shelfmark'] = shelfmark` after meta resolution. `_open_compare` uses `_anchor_state.get('page') or 1` and passes the shelfmark to the anchor Candidate constructor.

### A3 — Size-Mismatch Flag Lacks Anchor Dimensions

`_do_enrich_and_update` now extracts `anchor_sid = _anchor_state.get('sys_id')` and adds it to the enrichment batch (deduped) before dispatching to `_enrich_candidates`. The anchor's width/height are now available for `is_size_mismatch` evaluation.

### A4 — VS-Only Candidates Are Metadata-Poor

`_map_vs_suggestions_to_candidates` only sets sys_id/rank/score. Added `run_vs_meta_core()` named sync closure inside `_do_vs_fetch_and_update` that calls `executor.get_meta_for_id(sid)` and `executor.get_library_for_id(sid)` for each VS candidate, then applies the result via `dataclasses.replace(cand, shelfmark=..., title=..., library_code=...)`. Dispatched off-loop via `run.io_bound(run_vs_meta_core)`.

### F-VSavail — VS Service Probed Off-Loop

Added `_check_vs_service_available()` module-level sync helper that calls `get_vs_service(thread_safe=True).is_available()`. On first VS toggle call inside `_do_vs_fetch_and_update`, this is probed via `run.io_bound(_check_vs_service_available)`. If unavailable, `_apply_vs_unavailable_affordance()` disables the VS switch and sets the label. Translations added: "Visual similarity unavailable" / "No visual similarity data for this fragment".

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical Functionality] load_anchor lambda replaced with named closure**
- **Found during:** Task 3 test run — `test_vs_meta_lookup_not_on_event_loop` off-loop guard fired on the pre-existing `load_anchor` lambda
- **Issue:** `await run.io_bound(lambda: executor.get_meta_for_id(sys_id))` — the AST detector only recognizes `run.io_bound(ast.Name)` nodes; anonymous lambdas appear as direct calls, violating the off-loop guard contract
- **Fix:** Replaced lambda with `run_get_meta_for_anchor()` named sync closure inside `load_anchor`
- **Files modified:** `web/pages/joins_lab.py`
- **Commit:** 73996cbf (part of Task 3)

## Known Stubs

None — all candidate display, enrichment, and metadata paths are wired.

## Threat Flags

None — no new network endpoints, auth paths, or trust-boundary schema changes introduced.

## Self-Check: PASSED

- `web/pages/joins_lab.py`: exists and modified
- `tests/test_vs_adapter.py`: contains `_compute_display_candidates` assertions
- `tests/test_joins_lab_page.py`: contains `TestA2ViewModeRender` class
- `tests/test_joins_lab_off_loop.py`: contains `test_vs_meta_lookup_not_on_event_loop`
- `tests/test_candidate_enrichment.py`: contains A3/A4 tests
- `genizah_translations.py`: contains "Visual similarity unavailable" translation
- Commits: 15c42faf (Task 1), 9bd54d59 (Task 2), 73996cbf (Task 3) — all present in git log
- Test suite: 144 passed, 7 xpassed — 0 failures
