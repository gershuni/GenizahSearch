---
phase: 27-domain-classifications
plan: 02
subsystem: Search UI
tags: [fjms, domains, search, filter, web, desktop]
dependencies:
  requires:
    - shared/fjms_service.py (get_domain_hierarchy, get_manuscripts_by_domain from Phase 25)
    - Plan 01 (domain browse display and hierarchy method)
  provides:
    - Domain filter UI in both web and desktop search interfaces
    - Standalone domain browsing capability
  affects:
    - web/pages/search.py (domain filter and search integration)
    - genizah_app.py (DomainFilterDialog and search integration)
tech_stack:
  added: []
  patterns:
    - Hierarchical multi-select with type-ahead filtering
    - OR logic for multi-domain filtering
    - Standalone browsing without text query
    - Batch domain lookup alongside transcriptions
    - Purple badge styling (#7c3aed, #f3e8ff) for domain indicators
key_files:
  created: []
  modified:
    - web/pages/search.py (domain filter UI, filtering logic, result indicators)
    - genizah_app.py (DomainFilterDialog, filter button, search integration)
decisions:
  - Domain filter placed after mode selector in search controls row
  - Multi-select with chips UI pattern for selected domains
  - Type-ahead search enabled via use-input prop
  - Standalone browse capped at 500 results for performance
  - Domain indicators show primary domain + "+N more" pattern
  - Desktop uses QTreeWidget with checkboxes for hierarchical display
  - Parent checkbox propagates to visible children only
  - Domain filter persisted to URL state and user storage (web)
metrics:
  duration: 6
  completed: 2026-02-13
---

# Phase 27 Plan 02: Domain Search Filter Summary

Domain-based search filtering added to both web and desktop apps with hierarchical display, multi-select, type-ahead search, and standalone browsing capability.

## Tasks Completed

### Task 1: Web Domain Filter and Search Integration (d2af1b0)

**Domain Filter UI:**
- Added `initial_domain` parameter to `create_search_page()` function signature
- Created `_get_domain_hierarchy_cached()` module-level function for cached hierarchy
- Built domain options dict with parent/child structure and manuscript counts
- Added domain multi-select with type-ahead, chips, and clearable UI
- Pre-select from URL parameter or user storage
- Persist selections to user storage on change

**Search Integration:**
- Modified `execute_search()` to support standalone domain browsing (no query required)
- Created `_execute_domain_browse()` for domain-only manuscript listing
- Created `_apply_domain_filter()` for OR-logic filtering of search results
- Apply domain filter after core search but before result capping
- Cap standalone browse at 500 results with notification

**Result Indicators:**
- Added `result_domains` to SearchUIState
- Created `fetch_result_domains()` batch lookup function
- Batch lookup domains alongside transcriptions for displayed results
- Added domain badges to result cards with purple styling (#f3e8ff, #7c3aed)
- Show primary domain + "+N more" indicator with tooltip for multiple domains
- Deduplicate parent/child domains in display

**URL State:**
- Added domain parameters to URL persistence
- Multiple domains appended as `&domain=X&domain=Y`
- URL state restoration works via initial_domain parameter

**Files Modified:**
- web/pages/search.py (175 insertions, 3 deletions)

### Task 2: Desktop Domain Filter Dialog and Search Integration (d15520d)

**DomainFilterDialog Class:**
- Created hierarchical dialog with QTreeWidget
- Two-column display: Domain name and manuscript count
- Checkboxes for multi-select with parent-child propagation
- Type-ahead search filtering via search input box
- Selection summary label showing count/selected domains
- Clear All button and OK/Cancel actions
- `_populate_tree()` builds hierarchy from FJMS service
- `_filter_tree()` hides non-matching items during type-ahead
- `_handle_item_changed()` propagates parent checks to visible children
- `_restore_selections()` restores previously selected domains
- `get_selected_domains()` returns list of checked domain names

**Search Tab Integration:**
- Added "Domains" button to row2 search controls
- Added domain filter label badge (purple, hidden when empty)
- Initialize `_selected_domains` list and `_pending_domain_filter` in create_search_tab
- Created `_open_domain_filter_dialog()` to show filter dialog
- Created `_update_domain_filter_label()` to show "[Domain]" or "[N domains]" badge
- Implemented `_navigate_to_search_with_domain()` for browse navigation

**Search Execution:**
- Modified `start_search()` to support standalone domain browsing
- Created `_execute_domain_browse()` to browse manuscripts by domain
- Cap standalone browse at 500 results with status message
- Apply domain filter in `on_search_finished()` after results arrive
- Filter with OR logic: manuscript in ANY selected domain
- Early return with "No results matching domain filter" message when appropriate

**Files Modified:**
- genizah_app.py (282 insertions, 8 deletions)

## Deviations from Plan

None - plan executed exactly as written.

## Verification Status

**Web App:**
- Domain filter appears in search controls row after mode selector
- Hierarchical dropdown shows parent/child structure with counts
- Type-ahead search narrows domain list dynamically
- Multi-select with chips displays selected domains
- Standalone browse (no query) shows 500 manuscripts with notification
- Text query + domain filter shows filtered results only
- Multiple domain selection uses OR logic (any domain matches)
- Result cards show domain badges with "+N more" pattern
- Navigating from browse with ?domain=X pre-selects domain
- URL state includes domain parameters

**Desktop App:**
- "Domains" button appears in search controls row
- Filter dialog opens with hierarchical tree and checkboxes
- Type-ahead search filters tree items
- Parent checkbox checks all visible children
- Selected domains show in badge label ("[Piyyut]" or "[3 domains]")
- Standalone browse (no query) shows 500 manuscripts with status
- Text query + domain filter shows filtered results only
- Multiple domains use OR logic
- Browse domain links navigate to search with filter active
- Clear All button resets all checkboxes

**Domain Filter Logic:**
- get_manuscripts_by_domain() returns sys_ids for parent and children
- OR logic: results include manuscripts from ANY selected domain
- Filtering happens after core search (Tantivy) but before display
- Standalone browse fetches domains without running core search
- Result count accurate after domain filtering applied

## Next Steps

**Plan 03 (if any):** Domain filter is fully integrated in search. Next steps could include:
- Domain statistics/analytics
- Domain browsing page (separate from search)
- Domain hierarchy visualization
- Domain-based collections

**Phase completion:** With Plans 01-02 complete, domain classifications are fully integrated into both browse and search workflows.

## Self-Check

Verifying files and commits exist:

### Files Check
```bash
# Web domain filter UI
grep -n "domain_select = ui.select" C:/GenizahSearch/web/pages/search.py
# Output: Line 523

# Desktop DomainFilterDialog
grep -n "class DomainFilterDialog" C:/GenizahSearch/genizah_app.py
# Output: Line 4391
```

### Commits Check
```bash
git log --oneline | grep -E "(d2af1b0|d15520d)"
# Output:
# d15520d feat(27-02): add domain filter dialog and search integration to desktop app
# d2af1b0 feat(27-02): add domain filter and search integration to web app
```

## Self-Check: PASSED

All files modified as expected. Both task commits exist in history. Domain filter working in both apps.
