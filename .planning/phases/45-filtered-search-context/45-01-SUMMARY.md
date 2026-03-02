---
phase: 45-filtered-search-context
plan: 01
status: complete
completed: "2026-03-02"
duration: 5min
tasks_completed: 2
tasks_total: 2
subsystem: search-engine
tags: [filtering, performance, fjms, pre-search]
requires: [fjms_enrichment.db]
provides: [get_filter_sys_ids, restrict_sys_ids]
affects: [shared/fjms_service.py, genizah_core.py]
tech_stack:
  patterns: [pre-search filtering, set intersection, O(1) membership test]
key_files:
  created: []
  modified: [shared/fjms_service.py, genizah_core.py, tests/test_fjms_service.py]
key_decisions:
  - "get_filter_sys_ids returns None (not empty set) when no filters are active, enabling callers to skip filtering entirely"
  - "restrict_sys_ids check placed BEFORE regex.search() in both execute_search and search_composition_logic for genuine speed improvement"
  - "Material filters use subquery against catalog_fields table (include via IN, exclude via NOT IN)"
---

# Phase 45 Plan 01: Filter Service & Core Engine Integration Summary

**One-liner:** Pre-search filtering via FjmsService.get_filter_sys_ids() and restrict_sys_ids parameter on both search methods, skipping non-matching manuscripts before expensive regex verification.

## Summary

Created the foundational plumbing for filtered search: a shared filter service method that returns the set of manuscript sys_ids matching domain/author/work/date/material criteria, and integrated this into the core search engine so that filtered searches are genuinely faster (manuscripts outside the filter set are skipped before regex processing).

## Changes

- `shared/fjms_service.py`: Added `get_filter_sys_ids()` method (lines 764-898) that accepts domain, author, work, date_from, date_to, include_undated, material_include, material_exclude parameters. Returns None when no filters active, set of matching AlmaId strings when any filter is active, empty set when filters match nothing. Uses single SQL query with dynamic WHERE clause and JOINs against domains, catalog, genizah_titles, genizah_persons, and catalog_fields tables.

- `genizah_core.py`: Added `restrict_sys_ids: set = None` parameter to both `execute_search()` (line 5636) and `search_composition_logic()` (line 5986). In execute_search: metadata modes (Title/Shelfmark) filter sys_ids list before iteration; Tantivy loop checks restrict_sys_ids BEFORE regex.search(). In search_composition_logic: checks restrict_sys_ids BEFORE regex.search() in the per-chunk hit processing loop.

- `tests/test_fjms_service.py`: Added 17 tests covering all filter combinations: no filters (returns None), domain filter (direct and parent), author filter (person_id and legacy string), work filter, date range (from/to/both/include_undated), material include, material exclude, combined filters (AND intersection), no-match returns empty set, set type verification, graceful degradation when DB unavailable.

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 (RED) | Failing tests for get_filter_sys_ids | cc8191b6 | tests/test_fjms_service.py |
| 1 (GREEN) | Implement get_filter_sys_ids() | 3af12c6b | shared/fjms_service.py |
| 2 | Add restrict_sys_ids to execute_search and search_composition_logic | 15f22b94 | genizah_core.py |

## Verification

- 17/17 filter_sys_ids tests pass (0.70s)
- 91/92 full test suite passes (1 pre-existing failure in test_desktop_folio_navigation.py unrelated to this plan -- tests for a specific CSS border style string in genizah_app.py)
- Code review confirmed: restrict_sys_ids check appears BEFORE regex.search() in both execute_search (line 5878 vs 5888) and search_composition_logic (line 6055 vs 6063)
- Both methods are fully backward compatible when restrict_sys_ids=None

## Deviations from Plan

None -- plan executed exactly as written. Task 1 (TDD) was completed in a prior session (commits cc8191b6 and 3af12c6b). Task 2 was the remaining work.

## Notes for Downstream Plans 02-05

- **Plan 02 (Web search filter panel):** Call `fjms_service.get_filter_sys_ids(...)` to get the restrict set, then pass it as `restrict_sys_ids=result` to `engine.execute_search()` and `engine.search_composition_logic()`.
- **Plan 03 (Desktop filter integration):** Same pattern -- call get_filter_sys_ids() in the SearchThread, pass result to execute_search/search_composition_logic.
- **Plan 04 (Web parallels filter):** Pass restrict_sys_ids to search_composition_logic.
- **Plan 05 (Word search exclusion):** Unrelated to restrict_sys_ids, focuses on per-manuscript exclusion in regular search mode.
- **Key contract:** `restrict_sys_ids=None` means no filtering (full search). `restrict_sys_ids=set()` means filter matches nothing (all results skipped). Callers should pass None, not set(), when no filters are active.
