---
phase: 72-search-page-split
plan: 01
subsystem: web-search
status: pending-verification
tags: [refactor, extraction, search-state, web]
dependency_graph:
  requires: []
  provides: [SearchUIState, AdvancedViewState, SearchPageRefs, search-history-helpers, domain-display-name]
  affects: [web/pages/search.py, web/pages/search_results.py]
tech_stack:
  added: []
  patterns: [module-level-state-class, dataclass-refs, closure-to-module-function]
key_files:
  created:
    - web/pages/search_state.py
  modified:
    - web/pages/search.py
decisions:
  - Kept thin _domain_display_name wrapper in search.py to avoid touching 6 call sites -- delegates to module-level function
  - Added header_container, info_bar_container, brightness_sl to AdvancedViewState.__init__ (were set dynamically)
  - Added expanded_index, expansion_refs, _lazy_loaders to SearchUIState.__init__ per Pitfall 4
metrics:
  duration_seconds: 388
  completed: 2026-04-16T14:04:47Z
  tasks_completed: 2
  tasks_total: 3
  files_created: 1
  files_modified: 1
  lines_removed: 179
  test_count: 1067
  test_status: all_pass
---

# Phase 72 Plan 01: Search State Extraction Summary

Extracted SearchUIState, AdvancedViewState, SearchPageRefs dataclass, 4 search history helpers, and domain_display_name from the 6,732-line search.py into a new 267-line search_state.py module with zero UI dependencies.

## What Changed

### Task 1: Created web/pages/search_state.py (50da78aa)
- **SearchUIState** class with all 90+ fields, including 3 dynamic attributes previously set outside __init__ (expanded_index, expansion_refs, _lazy_loaders)
- **AdvancedViewState** class with all fields including 3 UI refs previously set only during dialog construction (header_container, info_bar_container, brightness_sl)
- **SearchPageRefs** dataclass with 12 fields for UI element/callback references (defined but not populated -- Plan 02 will wire it)
- **4 search history functions** converted from closures to module-level functions using `from nicegui import app`
- **domain_display_name** converted from closure to module-level function taking `search_state` parameter

### Task 2: Updated web/pages/search.py (b3df8bc3)
- Added import block for all extracted symbols
- Removed SearchUIState class definition (93 lines)
- Removed AdvancedViewState class definition (35 lines)
- Removed 4 search history closure functions, updated 6 call sites to use new names (without underscore prefix)
- Added thin `_domain_display_name` wrapper that delegates to module-level function (avoids touching 6 call sites that pass no search_state)
- search.py reduced from 6,732 to 6,553 lines (-179 lines, -2.7%)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing fields] Added dynamic AdvancedViewState UI ref fields to __init__**
- **Found during:** Task 1
- **Issue:** header_container, info_bar_container, brightness_sl were set dynamically during dialog construction but not in __init__
- **Fix:** Added all three to AdvancedViewState.__init__ with None defaults
- **Files modified:** web/pages/search_state.py

**2. [Rule 3 - Blocking] Kept _domain_display_name wrapper instead of updating 6 call sites**
- **Found during:** Task 2
- **Issue:** 6 call sites use `_domain_display_name(name)` without passing search_state. Updating all would increase diff risk.
- **Fix:** Kept thin closure wrapper in search.py that delegates to module-level function
- **Files modified:** web/pages/search.py

## Verification Results

- Import smoke: `from web.pages.search_state import SearchUIState, AdvancedViewState, SearchPageRefs` -- PASS
- Import smoke: `from web.pages.search import create_search_page` -- PASS
- pytest: 1067 passed, 8 skipped -- PASS
- SearchUIState instantiation with dynamic attrs -- PASS
- SearchPageRefs instantiation -- PASS

## Known Stubs

None -- all extracted code is fully functional.

## Self-Check: PASSED

- [x] web/pages/search_state.py exists (267 lines)
- [x] web/pages/search.py modified (6,553 lines)
- [x] Commit 50da78aa exists (Task 1)
- [x] Commit b3df8bc3 exists (Task 2)
