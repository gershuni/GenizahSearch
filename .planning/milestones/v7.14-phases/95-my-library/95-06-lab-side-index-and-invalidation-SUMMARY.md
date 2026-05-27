---
phase: 95
plan: "06"
subsystem: my-library
tags: [local-lab-index, weights-hash, invalidation, composition-search, fingerprint, tdd]
dependency_graph:
  requires: [95-02, 95-03, 95-05]
  provides: [build_lab_side_index, read_lab_meta, _check_local_lab_freshness, rebuild_local_lab_index, local_lab_searcher, search_composition_logic-local-lab-extension]
  affects: [genizah_core.py, shared/local_indexer.py]
tech_stack:
  added: []
  patterns: [W5-option-c-callback-injection, D-38-weights-hash-invalidation, HIGH-4-content-from-tantivy, TDD-red-green]
key_files:
  created:
    - tests/test_local_lab_invalidation.py
  modified:
    - shared/local_indexer.py
    - genizah_core.py
decisions:
  - "W5 Option C LOCKED: fingerprint helpers injected as keyword-only callbacks into build_lab_side_index — no circular import risk"
  - "HIGH-4 option b LOCKED: _iterate_lab_source_rows reads content from main LOCAL Tantivy stored field (not source files, not SQLite page_text column)"
  - "LabEngine vs SearchEngine guard: lab_composition_search is on LabEngine; used getattr(_check_local_lab_freshness) for safe no-op when called from LabEngine context"
  - "I14 confirmed: search_composition_logic lives on SearchEngine at line 8300 (shifted from 7923 by earlier edits in Wave 1-2)"
  - "_local_lab_index stored alongside local_lab_searcher for parse_query calls (same pattern as local_index + local_searcher)"
metrics:
  duration_minutes: 35
  completed_date: "2026-05-21"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 3
  tests_added: 16
  tests_passing: 16
---

# Phase 95 Plan 06: LAB Side-Index and Invalidation Summary

LOCAL LAB side-index builder (`build_lab_side_index`) + D-38 weights_hash invalidation contract + Composition Search / Parallels LOCAL LAB extension using custom fingerprint scoring (not RRF).

## Tasks Completed

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| RED | Failing tests for LAB invalidation (TDD red) | e4be68e6 | tests/test_local_lab_invalidation.py |
| 1 | build_lab_side_index + _iterate_lab_source_rows + read_lab_meta | 71e996ca | shared/local_indexer.py |
| 2 | local_lab_searcher init + D-38 freshness + lab/composition hooks | 8d3dd4c0 | genizah_core.py, tests/test_local_lab_invalidation.py |

## W5 Confirmation: Option C Locked

`build_lab_side_index` uses a `*` separator making all three callbacks keyword-only:

```python
def build_lab_side_index(
    self,
    lab_weights: dict,
    *,
    fingerprint_dyn_fn: Callable,
    fingerprint_static_fn: Callable,
    normalize_text_fn: Callable,
    lab_schema_version: int = 1,
    dynamic_rank_map=None,
) -> None:
```

The Option C wire-up in `SearchEngine.rebuild_local_lab_index` passes bound methods:

```python
local_indexer.build_lab_side_index(
    lab_weights=lab_weights,
    fingerprint_dyn_fn=self._compute_fingerprint_dyn,
    fingerprint_static_fn=self._compute_fingerprint_static,
    normalize_text_fn=self._normalize_text,
    ...
)
```

Options A (static methods + import) and B (shared/lab_fingerprint.py) are STRUCK.

## HIGH-4 Content Source

`_iterate_lab_source_rows()` reads page content exclusively from the main LOCAL Tantivy stored `content` field (by uid term query). Source file deletion or D-40 folder unavailability do not affect LAB rebuild — the main LOCAL Tantivy snapshot is durable. Explicit `_delete_file` is the only path to remove a doc from LAB.

## D-38 Weights Hash

`.meta.json` written on every `build_lab_side_index` call:

```json
{
  "weights_hash": "<sha256 of json.dumps(lab_weights, sort_keys=True)>",
  "lab_schema_version": 1,
  "last_built_at": "2026-05-21T..."
}
```

`_check_local_lab_freshness()` computes `_current_lab_weights_hash()` (sha256 of `dynamic_rank_map` + `use_dynamic_weights`) and compares to stored value. Mismatch sets `self.local_lab_searcher_stale = True` and returns False — LAB query skipped. UI banner surfaced in Plan 07.

## LAB-Affecting Settings in `_current_lab_weights_hash`

- `dynamic_rank_map` (the full word→rank dict, or None)
- `use_dynamic_weights` (bool from settings)

These two settings determine which fingerprint field is used (`fingerprint_dyn` vs `fingerprint`) and what the rank map is. Any future LAB-affecting setting should be added here to ensure D-38 invalidation fires.

## Staleness Banner

Uses a flag (`self.local_lab_searcher_stale = True`) polled by MyLibraryTab (Plan 07). No Qt signal emitted from `SearchEngine` (avoids threading concerns at this layer). MyLibraryTab reads `engine.local_lab_searcher_stale` after each Composition Search call.

## I14 Confirmation: search_composition_logic Line Number

`def search_composition_logic` is at **line 8300** in the post-edit file (shifted from the originally planned 7923 by Wave 1-2 edits). The function is on `SearchEngine` (confirmed via AST). The LOCAL LAB extension hook is inserted after the main scan `except InterruptedError: was_cancelled = True` block.

## _ensure_lab_tokenizers

Kept on `SearchEngine` (not extracted to shared). `build_lab_side_index` registers whitespace + simple tokenizers inline using the same pattern — no shared helper needed. Option B (move to shared) was not pursued per plan scope.

## LabEngine vs SearchEngine Architecture Note

`lab_composition_search` is defined on `LabEngine` (line 678), not `SearchEngine`. The LOCAL LAB extension added to that function uses `getattr(self, "_check_local_lab_freshness", None)` with a `callable()` guard so the hook is a no-op when called from a plain `LabEngine` context (no LOCAL index). When called from `SearchEngine` (which inherits both), the full freshness check runs.

`search_composition_logic` is on `SearchEngine` (line 8300) and calls `self._check_local_lab_freshness()` directly — no guard needed.

## Deviations from Plan

**1. [Rule 1 - Bug] LabEngine.lab_composition_search needed getattr guard**

- **Found during:** Task 2 regression tests
- **Issue:** `lab_composition_search` is defined on `LabEngine`, not `SearchEngine`. Tests in `test_lab_composition_chunk_hits.py` instantiate a bare `LabEngine` which has no `_check_local_lab_freshness` method — calling it directly raised `AttributeError`.
- **Fix:** Changed the LOCAL LAB hook in `lab_composition_search` from `self._check_local_lab_freshness()` to `getattr(self, "_check_local_lab_freshness", None)` with a `callable()` check. When called from a plain `LabEngine`, the hook is silently skipped (correct — LabEngine has no LOCAL index). When called from `SearchEngine`, the full D-38 check runs.
- **Files modified:** genizah_core.py (lab_composition_search hook guard)
- **Commit:** 8d3dd4c0

**2. [Rule 3 - Blocking] search_composition_logic line shifted from 7923 to 8300**

- **Found during:** Task 2 implementation
- **Issue:** I14 planned line 7923; Wave 1-2 edits shifted the function to 8300.
- **Fix:** Extended the function at its actual line 8300. Documented new line in SUMMARY.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes introduced. All changes are local-only (disk Tantivy + SQLite reads). T-95-22 (weights_hash staleness) and T-95-34 (callback signature) mitigations shipped as planned.

## Self-Check: PASSED

All files confirmed present. All commits confirmed in git log.

| Item | Status |
|------|--------|
| shared/local_indexer.py | FOUND |
| genizah_core.py | FOUND |
| tests/test_local_lab_invalidation.py | FOUND |
| SUMMARY.md | FOUND |
| Commit e4be68e6 (RED tests) | FOUND |
| Commit 71e996ca (Task 1) | FOUND |
| Commit 8d3dd4c0 (Task 2) | FOUND |
| 16/16 tests GREEN | PASSED |
| ruff clean (all 3 files) | PASSED |
