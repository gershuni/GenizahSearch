---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "01"
subsystem: shared/fjms_service
tags: [library-filter, catalog, push-down, seed-026, libfilter-02, guard-02]
dependency_graph:
  requires: []
  provides:
    - shared/fjms_service.py::get_browse_results(library_codes, library_sys_ids)
    - shared/fjms_service.py::resolve_library_sys_ids
    - tests/test_libfilter_catalog.py (6 LIBFILTER-02 service tests)
  affects:
    - 129-03-PLAN.md (web catalog Plan C — imports resolve_library_sys_ids, calls new args)
    - 129-04-PLAN.md (desktop catalog Plan D — same)
tech_stack:
  added: []
  patterns:
    - TEMP-table EXISTS push-down (SEED-023 pattern, extended for library filter)
    - Content-derived hash token for dynamic multi-select TEMP invalidation
key_files:
  created:
    - tests/test_libfilter_catalog.py
  modified:
    - shared/fjms_service.py
decisions:
  - "Content-derived token hash(tuple(sorted(library_codes))) prevents same-size-different-selection stale TEMP reuse (Codex REQUIRED CHANGE 1)"
  - "Fail-open warning path when library_codes truthy but library_sys_ids empty/None (Codex REQUIRED CHANGE 2) — logs distinctly, never silently returns 0"
  - "resolve_library_sys_ids placed at module level (not staticmethod) so both Plan C and Plan D can import it without touching FjmsService"
  - "Stale-TEMP guard tested via result-ID set difference (not totals) — two same-size disjoint selections must return disjoint ID sets"
metrics:
  duration: 4m
  completed: "2026-06-28"
  tasks_completed: 2
  files_modified: 2
---

# Phase 129 Plan 01: Library Filter Service Push-Down — Summary

**One-liner:** Additive `library_codes`/`library_sys_ids` args to `get_browse_results` with content-derived TEMP token + shared `resolve_library_sys_ids` reverse-lookup helper.

## What Was Built

### Task 1 — Test scaffold (RED)

Created `tests/test_libfilter_catalog.py` with 6 tests (LIBFILTER-02) over a 6-row in-memory tiny_fjms fixture:

1. `test_allowlist_contains_library_table` — `_browse_filter_library` in `_FILTER_TEMP_TABLES`.
2. `test_library_filter_changes_total_full_set_not_page` — total reflects the full filtered set (not the page limit of 2).
3. `test_library_none_or_empty_is_noop` — None/empty library args return the unfiltered total (backward-compatible).
4. `test_same_size_different_selection_not_stale` — two same-size different selections return DIFFERENT result-ID sets (proves content-derived token, not len).
5. `test_composition_pgp_editions_library_3way_and` — 3-way AND intersection is exact.
6. `test_selected_but_resolved_empty_fails_open` — truthy `library_codes` + falsy `library_sys_ids` returns all results, not 0.

### Task 2 — Implementation (GREEN)

Modified `shared/fjms_service.py`:

- `_FILTER_TEMP_TABLES` extended to include `"_browse_filter_library"`.
- `_ensure_filter_temp` docstring updated to document the static (PGP/Editions = len token) vs dynamic (library = content hash token) distinction.
- `get_browse_results` signature extended with `library_codes: list = None` and `library_sys_ids=None` (both additive, both documented in docstring).
- Library EXISTS block inserted after editions block and before `where = ...`, using `hash(tuple(sorted(library_codes)))` as the content-derived token.
- Fail-open warning path when `library_codes` truthy but `library_sys_ids` falsy (Codex REQUIRED CHANGE 2).
- Module-level `resolve_library_sys_ids(library_codes, meta_mgr) -> set` added at end of file, importable by Plans C and D; validates codes against `LIBRARY_CODES`, O(255K) csv_bank comprehension, with threading note in docstring.

## Verification Results

```
tests/test_libfilter_catalog.py       6/6  PASS  (LIBFILTER-02)
tests/test_seed023_catalog_filters.py        PASS  (GUARD-02 — PGP/Editions unchanged)
tests/test_fjms_service.py                   PASS  (GUARD-02 — FJMS service unchanged)
Total: 130 passed / 0 failed
ruff check: All checks passed
```

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1 | `e7dd2571` | test(129-01): scaffold LIBFILTER-02 service tests (RED — Task 1) |
| Task 2 | `f943bc53` | feat(129-01): library filter push-down in get_browse_results + resolve helper |

## Deviations from Plan

None — plan executed exactly as written.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes. The library filter uses the existing `_ensure_filter_temp` parameterized-insert pattern (T-129-03 mitigated). TEMP table name is a fixed literal in the allowlist (T-129-01 mitigated). No new packages.

## Self-Check: PASSED

- `tests/test_libfilter_catalog.py` exists: YES
- `shared/fjms_service.py` contains `_browse_filter_library`: YES
- `shared/fjms_service.py` contains `hash(tuple(sorted(library_codes)))`: YES
- `resolve_library_sys_ids` importable: YES
- Commits `e7dd2571` and `f943bc53` exist: YES
- 130 tests pass: YES
