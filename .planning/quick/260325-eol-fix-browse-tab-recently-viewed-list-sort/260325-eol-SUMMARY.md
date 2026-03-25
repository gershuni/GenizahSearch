---
phase: quick
plan: 260325-eol
subsystem: lists
tags: [bugfix, recently-viewed, web, desktop]
key-files:
  modified:
    - web/user_lists.py
    - genizah_app.py
decisions: []
metrics:
  duration: 1min
  completed: "2026-03-25"
  tasks: 2
  files: 2
---

# Quick Task 260325-eol: Fix Browse Tab Recently Viewed List Sort

**One-liner:** Fix empty Recently Viewed for authenticated web users and wrong sort order on desktop browse tab.

## Changes Made

### Task 1: Fix web authenticated user Recently Viewed (empty list bug)
- **Commit:** 30e5e331
- **File:** `web/user_lists.py`
- **Problem:** `get_items_in_list_sync('recent')` tried `int('recent')`, caught ValueError, returned `[]`. Authenticated users saw empty Recently Viewed on lists page, home page, comment dialog, and joins panel.
- **Fix:** Added special case before `int()` conversion: when `list_id == 'recent'` and user is authenticated, call `get_recent_items(self.user_id)` which returns items ordered by `viewed_at desc`. Added `_format_recent_items()` static method to map Supabase row format to the item dict format expected by callers. Applied to both async `get_items_in_list()` and sync `get_items_in_list_sync()`.

### Task 2: Fix desktop browse tab Recently Viewed sort order
- **Commit:** 0f037bc7
- **File:** `genizah_app.py`
- **Problem:** `browse_on_list_selected()` called `get_items_sorted(list_id, sort_by='shelfmark')` for all lists including Recently Viewed, destroying the view-time ordering.
- **Fix:** Special-cased `list_id == 'recent'` to call `get_items_in_list('recent')` directly (preserves insertion order, most recently viewed first), with metadata enrichment from `meta_mgr`. Other lists continue to use shelfmark sort.

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PASSED
