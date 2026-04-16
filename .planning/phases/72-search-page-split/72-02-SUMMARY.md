---
phase: 72-search-page-split
plan: 02
subsystem: web-search
status: complete
tags: [refactor, extraction, search-results, web]
dependency_graph:
  requires: [72-01]
  provides: [toggle_expansion, render_results, create_result_card, open_advanced_dialog, copy_result_text, show_add_to_list_dialog]
  affects: [web/pages/search.py, web/pages/search_results.py]
tech_stack:
  added: []
  patterns: [thin-wrapper-delegation, default-arg-lambda-capture, explicit-state-refs]
key_files:
  created:
    - web/pages/search_results.py
  modified:
    - web/pages/search.py
decisions:
  - Used default arg binding (_ss=search_state, _r=refs) for all lambda callbacks inside extracted functions to prevent late-binding closure bugs
  - Pagination callbacks (on_page_change_top/bottom) capture _ss/_r for recursive render_results calls
  - copy_result_text and show_add_to_list_dialog kept as standalone functions (no search_state/refs params) since they have zero closure dependencies
  - Cleaned 11 unused imports from search.py that were only needed by the extracted functions
  - open_advanced_dialog internal nested functions (~10) left unchanged since they capture from the outer function's local scope which moves with them
metrics:
  duration_seconds: 4214
  completed: 2026-04-16T15:15:00Z
  tasks_completed: 2
  tasks_total: 3
  files_created: 1
  files_modified: 1
  lines_removed: 2001
  lines_added: 2088
  test_count: 1066
  test_status: all_pass
---

# Phase 72 Plan 02: Search Results Extraction Summary

Extracted 6 rendering functions (1,987 lines) from search.py into search_results.py with explicit SearchUIState + SearchPageRefs parameters, thin local wrappers in search.py, and lambda default-arg binding for all closure callbacks.

## What Changed

### Task 1: Created web/pages/search_results.py (a7a7181a)
- **6 functions extracted** from create_search_page() closures to module-level functions:
  - `toggle_expansion(search_state, refs, index)` -- 26 lines, refs.page_client for lazy loader context
  - `render_results(search_state, refs, results, ...)` -- 228 lines, all UI refs via refs.*, pagination callbacks with _ss/_r capture
  - `create_result_card(search_state, refs, index, result)` -- 403 lines, domain_display_name imported from search_state, refs.query_input for snippet enrichment
  - `open_advanced_dialog(search_state, refs, index, result)` -- 1,296 lines with ~10 nested functions intact
  - `copy_result_text(text)` -- 13 lines, standalone (zero closure deps)
  - `show_add_to_list_dialog(result)` -- 19 lines, standalone, renamed from `_local` suffix
- **Closure variable replacements:**
  - `_page_client` -> `refs.page_client`
  - `results_container` -> `refs.results_container`
  - `PAGE_SIZE` -> `refs.page_size`
  - `query_input` -> `refs.query_input`
  - `_update_search_within_btn()` -> `refs.update_search_within_btn()`
  - `_update_refinement_strip()` -> `refs.update_refinement_strip()`
  - `_undo_zero_result_refine()` -> `refs.undo_zero_result_refine()`
  - `_apply_word_search_exclusions_and_render()` -> `refs.apply_word_search_exclusions_and_render()`
  - `update_selection_ui()` -> `refs.update_selection_ui()`
  - `_domain_display_name(name)` -> `domain_display_name(search_state, name)` (imported from search_state)
  - `show_add_to_list_dialog_local(r)` -> `show_add_to_list_dialog(r)` (same module)

### Task 2: Updated web/pages/search.py (11b8b9a6)
- **Import block** added: 6 symbols from search_results (aliased with _ prefix for 4 that get wrappers)
- **SearchPageRefs construction** placed before execute_search, after all callbacks defined (12 fields wired)
- **4 thin wrappers** (`toggle_expansion`, `render_results`, `create_result_card`, `open_advanced_dialog`) preserve original calling convention for 15+ call sites
- **11 unused imports cleaned**: WEB_PUZZLE_ENABLED, get_service, BrowsePage, get_oxford_direct_image_url, is_oxford_manuscript, get_library_display, SearchEngine, AdvancedViewState, enrich_snippet_with_chain_terms, compute_all_terms_filter, get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page, Optional, List, Set, re, datetime
- **search.py reduced** from 6,553 to 4,596 lines (-30%, -1,957 net lines)

### Task 3: Web search smoke test -- PENDING
Awaiting human verification (checkpoint:human-verify).

## Line Count Summary

| Module | Before | After | Change |
|--------|--------|-------|--------|
| search.py | 6,553 (post Plan 01) | 4,596 | -1,957 (-30%) |
| search_results.py | 0 (new) | 2,044 | +2,044 |
| search_state.py | 267 (from Plan 01) | 267 | 0 |
| **Total** | **6,820** | **6,907** | **+87 (+1.3% overhead)** |

The 87-line overhead comes from module docstring, imports, and 4 thin wrappers in search.py.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Cleaned 11 unused imports from search.py**
- **Found during:** Task 2
- **Issue:** After removing 6 function definitions, many imports were only used by the extracted code
- **Fix:** Iteratively removed all ruff F401 violations (11 import symbols across 8 import lines)
- **Files modified:** web/pages/search.py
- **Commit:** 11b8b9a6

## Verification Results

- Import smoke (search_results.py): all 6 functions importable -- PASS
- Import smoke (search.py): create_search_page importable -- PASS
- ruff (search.py): All checks passed -- PASS
- ruff (search_results.py): All checks passed -- PASS
- pytest: 1066 passed, 9 skipped -- PASS
- Web smoke test: PENDING (Task 3 checkpoint)

## Known Stubs

None -- all extracted code is fully functional.

## Self-Check: PASSED

- [x] web/pages/search_results.py exists (2,044 lines)
- [x] web/pages/search.py modified (4,596 lines)
- [x] Commit a7a7181a exists (Task 1)
- [x] Commit 11b8b9a6 exists (Task 2)
- [x] All 6 functions importable from search_results
- [x] create_search_page importable from search
- [x] ruff clean on both files
- [x] pytest 1066 passed
