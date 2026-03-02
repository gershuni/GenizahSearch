---
phase: 45-filtered-search-context
plan: 02
status: complete
completed: "2026-03-02"
duration: 34min
tasks_completed: 2
tasks_total: 2
subsystem: web-search
tags: [filtering, ui, pre-search, chips, exclusion]
requires: [shared/fjms_service.py, genizah_core.py]
provides: [advanced-filters-panel, chip-bar, restrict-sys-ids-wiring, word-search-exclusion]
affects: [web/pages/search.py, web/main.py]
tech_stack:
  patterns: [collapsible expansion, removable chips, cross-filtered selects, async filter count, session persistence]
key_files:
  created: []
  modified: [web/pages/search.py, web/main.py]
key_decisions:
  - "Advanced Filters panel placed between progress bar and splitter, using ui.expansion for collapsibility"
  - "Filter chip bar is always visible (even when panel collapsed) and shows manuscript count badge"
  - "Cross-filtering implemented synchronously for UX responsiveness: domain change refreshes author/work options"
  - "restrict_sys_ids computed before run_core_search via await run.io_bound, empty set triggers early return with warning"
  - "Word search exclusions stored as separate set from domain exclusions, with restore and clear-all controls"
  - "Filter state persisted with _persist() pattern keys prefixed search_filter_"
  - "from_browse URL param added to route signature for Path B browse-to-search navigation"
---

# Phase 45 Plan 02: Web Search Filter Panel & Pre-Search Integration Summary

**One-liner:** Collapsible Advanced Filters panel with domain/author/work/date/material controls, removable chip bar with manuscript count, pre-search restrict_sys_ids wiring to execute_search, and per-result word search exclusion.

## Summary

Added the primary user-facing filter interface to the web search page. Researchers can now constrain searches by scholarly categories (domain, author, work, date range, material type) before running them. Active filters are displayed as removable chips with a total manuscript count. The filter set is computed via get_filter_sys_ids() and passed as restrict_sys_ids to the core search engine, making filtered searches genuinely faster. Word search results now have per-result exclude buttons matching the composition mode UX pattern.

## Changes

- `web/pages/search.py` (722 lines added):
  - **SearchUIState**: Added 12 filter state fields (filter_domain, filter_author, filter_author_name, filter_work, filter_work_name, filter_date_from, filter_date_to, filter_material_exclude, filter_manuscript_count, restrict_sys_ids, word_search_excluded_ids, word_search_excluded_results)
  - **create_search_page signature**: Added `from_browse: int = None` parameter
  - **Incoming filters consumption**: On page load with from_browse=1, reads incoming_filters from app.storage.user, applies to filter state, clears from storage
  - **Session restore**: All filter fields restored from app.storage.user on page load (browse takes priority over session)
  - **Advanced Filters panel**: Collapsible ui.expansion below progress bar with domain select (hierarchical options from get_domain_hierarchy), author select (cross-filtered by domain, from get_browse_authors), work select (cross-filtered by domain+author, from get_browse_works), date range inputs (From Year / To Year), and Exclude Printed checkbox
  - **Chip bar**: Row of removable ui.chip elements for each active filter, plus manuscript count badge. Visible even when panel is collapsed. Chips have distinct colors per filter type (purple=domain, blue=author, teal=work, orange=date, red=material)
  - **Filter change handlers**: Each filter change persists to storage, cross-filters dependent selects, and recomputes manuscript count via async run.io_bound call to get_filter_sys_ids
  - **Pre-search filtering**: In execute_search(), computes restrict_sys_ids before run_core_search if any filters active. Passes restrict_sys_ids=restrict_sys_ids to state.searcher.execute_search(). Empty filter set triggers early return with "No manuscripts match" warning
  - **Word search exclusion**: Per-result exclude button (remove_circle_outline icon) on non-composition results. Excluded items shown in collapsible section with restore buttons and clear-all control. Exclusions persisted via word_search_excluded_ids storage key
  - **Search history integration**: Filter params included in history entries (filters dict with domain/author/work/date/material). Filter icon shown on history entries that had active filters. Filters restored when recalling history entry
  - **Search summary**: Status line includes filter info suffix when active (e.g., "filtered: Halakha, 3,241 manuscripts")

- `web/main.py` (6 lines changed):
  - Added `from_browse: int = None` parameter to search_page_route
  - Passed from_browse to create_search_page

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1+2 | Advanced Filters panel, chip bar, pre-search filtering, word search exclusion | a6952f79 | web/pages/search.py, web/main.py |

## Verification

- File parses without syntax errors (ast.parse OK)
- Module imports cleanly: `from web.pages.search import create_search_page` OK
- All required keywords present: filter_domain, filter_author, restrict_sys_ids, word_search_excluded_ids, incoming_filters, get_filter_sys_ids
- restrict_sys_ids=restrict_sys_ids parameter passing confirmed in run_core_search
- 89/89 FJMS service tests pass (including all 17 get_filter_sys_ids tests)
- Pre-existing test failure in test_desktop_folio_navigation.py (CSS border style assertion) unrelated to this plan

## Deviations from Plan

None -- plan executed exactly as written.

## Notes for Downstream Plans 03-05

- **Plan 03 (Desktop filter integration):** Desktop filter dialog should use same get_filter_sys_ids() call pattern, passing result as restrict_sys_ids to SearchThread
- **Plan 04 (Web parallels filter):** Can reuse the _build_domain_options, _build_author_options, _build_work_options helpers (consider extracting to shared module)
- **Plan 05 (Browse-to-search navigation):** incoming_filters consumption is ready. Write to app.storage.user['incoming_filters'] dict with keys: domain, author, author_name, work, work_name, date_from, date_to, material_exclude. Navigate to /search?from_browse=1
