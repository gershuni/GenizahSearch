---
phase: 27-domain-classifications
plan: 04
subsystem: web-search
tags: [domain-filter, post-search, dynamic-filter, gap-closure]

dependency_graph:
  requires: [27-03-batch-domain-lookup]
  provides: [web-post-search-domain-filter]
  affects: [web-search-page]

tech_stack:
  added: []
  patterns: [post-search-filtering, checkbox-tree-dialog, client-side-filter]

key_files:
  created: []
  modified:
    - web/pages/search.py: "Removed pre-search domain dropdown, added post-search Domains button with checkbox tree dialog, client-side exclusion filtering"

decisions:
  - "Post-search domain filter (not pre-search) for dynamic visibility"
  - "Exclude-by-unchecking pattern (all checked by default) matches desktop UX"
  - "Batch domain collection for all results (not just displayed 200) enables accurate filtering"
  - "Results without domain data always shown (not filtered out)"
  - "Domain exclusions persisted to app.storage.user['domain_exclusions']"
  - "initial_domain parameter clears exclusions (browse->search navigation)"

metrics:
  duration_minutes: 7
  completed_date: 2026-02-13
---

# Phase 27 Plan 04: Web Post-Search Domain Filter Summary

Post-search dynamic domain filter with button+dialog UI for web app, replacing pre-search dropdown

## What Was Built

### Removed Components
- **Pre-search domain filter dropdown** (lines ~494-534)
  - `_get_domain_hierarchy_cached()` module-level function
  - `_domain_hierarchy_cache` global
  - `domain_select` UI element and all references
  - `_apply_domain_filter()` function (OR logic pre-search filtering)
  - `_execute_domain_browse()` function (standalone domain browse)
  - Domain URL parameters (`&domain=` in state persistence)
  - `app.storage.user['search_domains']` handling

### Added Components

**SearchUIState new fields:**
```python
self.all_result_domains: dict = {}    # sys_id -> list of domain names (deduped)
self.domain_exclusions: set = set()   # domain names user has excluded
self.has_domain_data: bool = False     # whether any results have domain data
```

**Domain collection in execute_search (post-search):**
- Collects domain data for ALL results via `fjms.get_domains_for_sys_ids()` batch lookup
- Deduplicates parent/child domains (children first, skip parent if child shown)
- Sets `has_domain_data` flag
- Slices `result_domains` from `all_result_domains` for badge rendering

**Domains button in results header:**
- Hidden by default (`set_visibility(False)`)
- Shown after search when `has_domain_data` is True
- Text updates when exclusions active: "Domains (N excluded)"
- Opens `_open_domain_filter_dialog()` on click

**Dialog UI (`_open_domain_filter_dialog()`):**
- Modal with checkbox tree showing domains from current results only
- Hierarchical: parent (bold) and children (indented) with counts
- All checkboxes checked by default (exclude-by-unchecking pattern)
- Parent checkbox toggles all children
- Live summary line: "Showing X of Y results"
- Check All button resets all to checked
- Apply button: persists exclusions to `app.storage.user['domain_exclusions']`, calls `_apply_domain_exclusions()`, updates button text, closes dialog
- Cancel button closes without changes

**Client-side filtering (`_apply_domain_exclusions()`):**
- Filters `search_state.results` by exclusion set
- Logic: keep result if (no domain data) OR (not all domains excluded)
- Updates `results_count.text` to show "X of Y Results (N domains excluded)"
- Updates `result_domains` slice for badge rendering
- Re-renders with `filtered[:200]`

**Persistence:**
- Save: `app.storage.user['domain_exclusions'] = list(excluded)` on Apply
- Restore: `search_state.domain_exclusions = set(app.storage.user.get('domain_exclusions', []))` on page load
- Clear: if `initial_domain` provided (browse navigation), clear exclusions

**Remembered exclusions on new search:**
- Applied before first render in `execute_search()`
- If exclusions active and domain data available, filter results before calling `render_results()`
- Update count text and result_domains slice

## Deviations from Plan

None - plan executed exactly as written.

## Edge Cases Handled

1. **No results have domain data:** Button stays hidden, remembered exclusions preserved but not applied, no user-visible impact.

2. **Remembered exclusions on new search:** Re-applied to new results. If a remembered exclusion domain is not in new results, it's silently ignored (no filtering effect).

3. **ALL domains excluded:** Shows results without domain data only. Count shows "0 of N Results (M domains excluded)" if all results have domain data. User can click Domains button to re-check.

4. **Cancel dialog:** Closes without modifying `search_state.domain_exclusions`, no filtering change.

5. **Browse->search navigation (initial_domain):** Clears remembered exclusions so user sees all results.

## Implementation Details

**Domain collection timing:**
- After `run_core_search()` completes and error check passes
- Before `render_results()` call
- Collects for ALL results (not just [:200]) for accurate filtering

**Button visibility logic (in execute_search):**
```python
domain_filter_btn.set_visibility(search_state.has_domain_data)
if search_state.has_domain_data and search_state.domain_exclusions:
    n_excl = len(search_state.domain_exclusions)
    domain_filter_btn.text = f"{tr('Domains')} ({n_excl} {tr('excluded')})"
else:
    domain_filter_btn.text = tr('Domains')
```

**Dialog hierarchy building:**
- Fetches full `hierarchy` from `fjms.get_domain_hierarchy()`
- Counts results per domain from `all_result_domains`
- Builds `result_hierarchy` dict with only domains present in current results
- Handles orphan domains (in results but not in hierarchy)
- Sorts by count (descending)

**Exclusion filter logic:**
```python
if not result_domains:
    filtered.append(r)  # No domain data -- always keep
elif all(d in search_state.domain_exclusions for d in result_domains):
    continue  # ALL domains excluded -- hide
else:
    filtered.append(r)  # At least one domain not excluded -- keep
```

## Testing Notes

**Verified scenarios:**
1. Pre-search domain dropdown removed from search controls
2. After search with domain data, Domains button appears
3. After search with no domain data, button stays hidden
4. Dialog shows hierarchical checkbox tree with only result domains
5. All checkboxes checked by default
6. Parent toggles children
7. Summary line updates live
8. Check All resets
9. Apply filters results immediately, persists exclusions, updates button text
10. Cancel closes without effect
11. Remembered exclusions re-applied on new search
12. Domain badges still render on result cards after filtering

## Technical Debt

None.

## Files Changed

| File | Lines Changed | Description |
|------|---------------|-------------|
| web/pages/search.py | -147, +255 (+108 net) | Removed pre-search domain dropdown and browse logic, added post-search domain collection, Domains button, checkbox tree dialog, client-side filtering, persistence |

## Performance Impact

- **Domain collection:** Single batch lookup via `get_domains_for_sys_ids()` for all results (not per-sys_id calls) - negligible overhead
- **Filtering:** Client-side filtering is instant (no re-search) - O(N) where N = result count
- **Dialog rendering:** Only domains from current results shown (not full 322K catalog) - fast

## Commits

| Commit | Message |
|--------|---------|
| aea8fc7 | feat(27-04): remove pre-search domain filter and add post-search domain collection |
| 0d4d164 | feat(27-04): add Domains button and checkbox tree dialog UI |
| bcfd5c1 | feat(27-04): wire domain exclusion filtering and persistence |

## Verification

✅ Pre-search domain dropdown removed
✅ Domains button appears after search (only when domain data exists)
✅ Dialog shows hierarchical checkbox tree with counts
✅ All checkboxes default to checked
✅ Unchecking and applying filters results without re-searching
✅ Button shows "(N excluded)" when filtering active
✅ Domain badges still render on result cards
✅ Exclusions persisted to `app.storage.user['domain_exclusions']`
✅ Remembered exclusions re-applied on new search
✅ No standalone domain browse path
✅ Edge case: No domain data -> button hidden
✅ Edge case: All domains excluded -> shows 0 results with correct count text
✅ Edge case: Remembered exclusions on new search -> re-applied correctly
✅ Edge case: Cancel dialog -> no filtering change
✅ Edge case: initial_domain navigation -> clears exclusions

## Self-Check: PASSED

✅ All modified files exist
✅ All commits exist
