---
phase: 96
plan: "05"
subsystem: desktop/my-library
tags: [phase-96, my-library, filter-cascade, query-time, D-F1]
dependency_graph:
  requires: [96-04]
  provides: [_apply_local_optout_filter, cascade-wiring-D-F1]
  affects: [genizah_app.py/_apply_results_table_filters, genizah_app.py/_apply_comp_tree_filters]
tech_stack:
  added: []
  patterns: [filter-after-filter cascade, silent opt-out filter, OR-gated visible-set]
key_files:
  modified:
    - genizah_app.py
decisions:
  - "Filter-after-filter order: three-state (_apply_local_filter) runs first, opt-out (_apply_local_optout_filter) runs second — minor optimization (three-state may remove LOCAL hits, reducing opt-out work)"
  - "Silent filter design: opt-out sets no chip flag — opt-out is a persistent user preference, not a transient UI state (D-15 spirit)"
  - "OR-gated visible-set: _local_filter_active OR'd with _optout_active at both joinpoints so visible-set computation still happens when only opt-out is engaged (three-state='all')"
  - "Fast no-op path: when _local_file_optouts is empty, _apply_local_optout_filter returns results unchanged with no per-hit overhead"
  - "Defensive lookup failure: when _lookup_local_filepath returns None, the hit is kept (transient indexer state should not cause result disappearance)"
metrics:
  duration: "~8 minutes"
  completed: "2026-05-24"
  tasks_completed: 2
  files_modified: 1
---

# Phase 96 Plan 05: Query-Time Opt-Out Filter Cascade Wiring Summary

**One-liner:** Wired `_apply_local_optout_filter` into both cascade joinpoints after `_apply_local_filter`, completing the Phase 96 D-F1 query-time opt-out filter.

## What Was Built

Two tasks added the production opt-out filter method and wired it into both cascade joinpoints in `genizah_app.py`:

### Task 1: `_apply_local_optout_filter` method (commit `69604481`)

New method added immediately after `_apply_local_filter` (line 17351). Behavior:
- Fast no-op when `self._local_file_optouts` is empty (zero overhead when feature unused)
- For each LOCAL hit: resolves `sys_id` → filepath via `self._lookup_local_filepath(sid)`, drops the result iff filepath is in `_local_file_optouts`
- Non-LOCAL hits (Genizah corpus) pass through unconditionally
- Handles missing/malformed `display` dict using same `(r.get('display', {}) or {})` pattern
- Handles lookup failure (`None` return) defensively — keeps the hit rather than dropping it
- No mutation of `_local_filter_inactive_chip_visible` — opt-out is silent (no chip)

### Task 2: Cascade wiring at both joinpoints (commit `2465021c`)

**Joinpoint A** (`_apply_results_table_filters`): added after line 17518:
```python
# Phase 96 D-F1: drop user-opted-out LOCAL files from the cascade.
_local_filtered = self._apply_local_optout_filter(_local_filtered)
```

**Joinpoint B** (`_apply_comp_tree_filters`): added after line 17855:
```python
# Phase 96 D-F1: drop user-opted-out LOCAL files from the cascade.
_local_filtered_comp = self._apply_local_optout_filter(_local_filtered_comp)
```

**OR-gated visible-set subtlety:** At both joinpoints, `_local_visible_sys_ids` was previously only computed when `_local_filter_active` was true (three-state != 'all'). With opt-out, the set must also be computed when only opt-outs are active (three-state = 'all'). Both joinpoints updated:
```python
_optout_active = bool(getattr(self, '_local_file_optouts', set()))
_local_visible_sys_ids = {
    ...
} if (_local_filter_active or _optout_active) and not self._local_filter_inactive_chip_visible else None
```

The fast-path early-return guards at both joinpoints were also updated to include `not _optout_active`.

## Design Notes

**Filter-after-filter cascade order:** Three-state (`_apply_local_filter`) runs first, opt-out (`_apply_local_optout_filter`) runs second. This is an optimization — three-state may already have removed some LOCAL hits, reducing opt-out work. The order also matches the UX mental model: user first decides whether to show LOCAL at all (three-state), then refines which specific LOCAL files to include (opt-out).

**Silent filter design:** Unlike the Phase 95 three-state filter which sets `_local_filter_inactive_chip_visible` and shows a chip when inactive, opt-out is silent. It is a persistent user preference (saved in session JSON by plan 96-04), not a transient UI state. Showing a chip would be confusing — the user knows which files they opted out.

**OR-gating visible-set:** The downstream `_local_visible_sys_ids` predicate is what gates row visibility in the results table and tree. If we didn't OR-gate it with `_optout_active`, opted-out files would still appear when the three-state filter is 'all', because `_local_filter_active` would be False and `_local_visible_sys_ids` would be None (meaning "show all"). The OR ensures the predicate is computed whenever any LOCAL filtering is active.

**Downstream:** Plan 96-06 adds the tree widget UI that mutates `self._local_file_optouts`. This plan provides the consumer — the filter will take effect immediately when 96-06 adds the UI.

## Test Results

| Test file | Before | After |
|-----------|--------|-------|
| `tests/test_local_optout_filter.py` | 6 passed | 6 passed |
| `tests/test_local_filter_cascade.py` | 4 passed, 2 skipped | 6 passed |
| `tests/test_local_filter_persistence.py` | 4 passed | 4 passed |
| `tests/test_local_post_dedup_merge.py` | 3 passed | 3 passed |
| Full LOCAL regression bundle | 184 passed, 4 skipped, 5 xfailed | same |

Both Phase 96 AST guards flipped from SKIPPED to PASSED:
- `test_optout_filter_applied_within_both_cascades` — SKIPPED -> PASSED
- `test_apply_local_optout_filter_function_exists` — SKIPPED -> PASSED

Phase 95 cascade invariants remain green:
- `test_local_filter_applied_within_results_cascade` — PASSED (unchanged)
- `test_apply_local_filter_function_exists` — PASSED (unchanged)

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | `69604481` | feat(96-05): add _apply_local_optout_filter method |
| 2 | `2465021c` | feat(96-05): wire _apply_local_optout_filter into both cascade joinpoints |

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None — the filter method is fully wired. `_local_file_optouts` is populated by plan 96-04 (persistence) and will be mutated by plan 96-06 (tree widget UI).

## Self-Check: PASSED

- `genizah_app.py` — modified (exists)
- Commits `69604481` and `2465021c` exist in git log
- 6/6 cascade tests pass
- 6/6 opt-out filter tests pass
- Ruff clean
