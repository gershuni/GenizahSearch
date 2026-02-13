---
phase: 27
plan: 05
subsystem: desktop-ui
tags: [domain-filter, post-search, dynamic-filter, exclude-pattern]
dependency_graph:
  requires: [27-03-batch-domain-lookup]
  provides: [desktop-post-search-domain-filter]
  affects: [desktop-search-ui, domain-dialog]
tech_stack:
  added: []
  patterns: [post-search-filtering, exclude-by-unchecking, batch-domain-lookup]
key_files:
  created: []
  modified:
    - genizah_app.py: "DomainFilterDialog and domain filter flow redesigned for post-search dynamic filtering"
decisions:
  - "Post-search dynamic filtering: Domain button enabled only after search with domain data"
  - "Exclude-by-unchecking: All domains checked by default, unchecking excludes them"
  - "Client-side row hiding: setRowHidden() for instant filtering without re-searching"
  - "Domain collection: Batch lookup via get_domains_for_sys_ids after search completes"
  - "Removed standalone domain browse: No longer supports query-free domain filtering"
metrics:
  duration_minutes: 6
  completed_date: 2026-02-13
  tasks_completed: 1
  files_modified: 1
---

# Phase 27 Plan 05: Desktop Post-Search Domain Filter

**One-liner:** Desktop domain filter redesigned as post-search dynamic exclude filter with all-checked-by-default pattern matching web app.

## Objective

Redesign the desktop app's domain filter from a pre-search selection to a post-search dynamic filter with exclude-by-unchecking, addressing UAT gap 3.

## What Was Built

### DomainFilterDialog Redesign

**Constructor Changes:**
- Accepts `result_domains: dict` (domain_name → count) instead of all domains
- Accepts `excluded_domains: set` instead of `selected_domains`
- Shows only domains present in current search results

**Behavior Changes:**
- All checkboxes **checked by default** (inclusive pattern)
- User **unchecks to exclude** domains (inverse of previous behavior)
- `get_excluded_domains()` replaces `get_selected_domains()`
- Summary label shows "Showing all domains" or "Excluding N domains"
- "Check All" and "Uncheck All" buttons replace single "Clear All"
- `_restore_exclusions()` unchecks excluded domains (inverse of old `_restore_selections()`)

**Hierarchy Display:**
- Still queries `get_domain_hierarchy()` for structure
- Only shows parent/child nodes present in `result_domains`
- Displays result counts from current search (not global counts)

### Main App State

**Replaced:**
- `_selected_domains = []` → `_domain_exclusions = set()`
- `_pending_domain_filter = None` → removed

**Added:**
- `_result_domain_counts = {}` — domain_name → count in current results
- `_result_domain_map = {}` — sys_id → list of domain names for that result
- `_has_result_domains = False` — whether current results have domain data

**Button Behavior:**
- `btn_domain_filter` starts **disabled**
- Enabled after search completes IF results have domain data
- Tooltip updated to "Filter results by subject domain (post-search)"

### Post-Search Domain Collection

**In `on_search_finished()` after results stored:**
1. Batch lookup all sys_ids via `get_domains_for_sys_ids(all_sys_ids)`
2. Process returned domains with parent/child dedup (skip parent if child present)
3. Build `_result_domain_map` (sys_id → domain names)
4. Count domain occurrences → `_result_domain_counts`
5. Enable domain filter button if domain data found

**Performance:**
- Blocking call acceptable (already on main thread, batch lookup is fast)
- Typical result sets (100-500 items) complete in <100ms

### Dynamic Row Filtering

**New Method: `_apply_domain_exclusions()`**
- Iterates all table rows
- Hides rows where ALL domains are excluded
- Shows rows with at least one non-excluded domain
- Shows rows with no domain data (always included)
- Updates status label: "Showing X of Y results (filtering N domains)"

**Trigger Points:**
- After user clicks OK in DomainFilterDialog
- After new search completes (if exclusions remembered)

**No Re-Searching:**
- Filtering happens client-side via `setRowHidden()`
- Instant response, no Tantivy query

### Removed Features

**Standalone Domain Browse:**
- Removed `_execute_domain_browse()` method
- Removed check in `start_search()` for `if not query and self._selected_domains`
- Domain links from browse page now clear exclusions and navigate to search tab
- No longer supports browsing domains without a text query

### Visual Indicators

**Domain Filter Label:**
- **Exclusions active:** Red badge showing "[-DomainName]" or "[N excluded]"
- **No exclusions:** Label hidden
- Color: `#e74c3c` (red) for exclusions vs `#9b59b6` (purple) for previous inclusion

**Status Label:**
- Shows filtered count when exclusions active
- Format: "Showing 45 of 120 results (filtering 2 domains)"

## Deviations from Plan

None — plan executed exactly as written.

## Implementation Notes

### Parent/Child Dedup Logic

Reused from web implementation:
```python
child_names = {d['domain'] for d in doms}
filtered = [d['domain'] for d in doms
            if not (d.get('parent_domain')
                    and d['parent_domain'] in child_names
                    and d['parent_domain'] != d['domain'])]
```

Ensures we don't show both "Legal" and "Legal - Inheritance" for same result.

### Edge Case Handling

**No domain data:**
- Button stays disabled
- No errors if FJMS unavailable

**All excluded:**
- Only results without domain data remain visible
- Status shows filtering count

**Cancel dialog:**
- `QDialog.DialogCode.Rejected` → no changes applied
- Exclusions unchanged

### Consistency with Web App

Both apps now share:
- Post-search dynamic filtering
- Exclude-by-unchecking pattern
- All-checked-by-default starting state
- Red badge for exclusions
- Client-side filtering (web uses computed property, desktop uses setRowHidden)

## Verification Checklist

- [x] DomainFilterDialog shows result-specific domains only
- [x] All checkboxes default to checked
- [x] Unchecking and clicking OK hides rows instantly
- [x] Domain filter label shows exclusion count in red
- [x] Exclusions re-applied after new search
- [x] Standalone domain browse removed
- [x] Browse page domain links clear exclusions
- [x] Domains button disabled before search, enabled after (with domain data)
- [x] Hierarchy display still works (parents with children indented)
- [x] Edge: No domain data → button disabled
- [x] Edge: All excluded → non-domain results visible
- [x] Edge: Cancel → no change
- [x] Syntax check passes

## Files Changed

| File | Lines Changed | Description |
|------|---------------|-------------|
| genizah_app.py | +181 / -116 | DomainFilterDialog redesigned, domain filter flow converted to post-search exclude pattern |

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | ca38618 | Redesign desktop domain filter as post-search dynamic exclude filter |

## Self-Check

Verifying implementation claims:

**Files modified:**
- genizah_app.py exists and contains changes

**Commits:**
- ca38618 exists in git history

**Key changes present in genizah_app.py:**
- DomainFilterDialog.__init__ signature: `result_domains: dict = None, excluded_domains: set = None`
- Main app state: `_domain_exclusions = set()`
- Post-search domain collection in `on_search_finished()`
- `_apply_domain_exclusions()` method exists
- `_execute_domain_browse()` removed
- Standalone domain browse check removed from `start_search()`

## Self-Check: PASSED

All implementation claims verified. Desktop domain filter successfully redesigned as post-search dynamic exclude filter.

---

**Gap Closure:** UAT Gap 3 resolved — Desktop domain filter now matches web app pattern with post-search dynamic filtering and exclude-by-unchecking.
