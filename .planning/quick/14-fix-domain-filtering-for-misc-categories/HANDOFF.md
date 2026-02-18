# Handoff: Domain Filter Dialog Bugs

## Status
- **Quick task 14 (committed):** Added `qualify_domain_name()` helpers + updated dedup in all 4 locations (web search, web parallels, desktop search, desktop composition). Commits: `8ce212a7`, `b5dd8abb`, `9bc50777`, `b522812b`.
- **Optimization attempts (stashed):** `git stash pop` to restore. Multiple rounds of changes to search.py, parallels.py, fjms_service.py that did NOT fix the user's reported issues. **Recommend discarding stash and starting fresh.**

## Two Remaining Bugs

### Bug 1: Dialog opens very slowly (12+ seconds)
- **Symptom:** Clicking "Filter by domains" button → 12 seconds before dialog appears.
- **Not caused by our quick-14 changes** — pre-existing.
- **Root cause:** Creating ~200 NiceGUI `ui.checkbox` elements with event handlers in a loop inside `_open_domain_filter_dialog` (search.py ~line 1682). Each element requires Python object creation + WebSocket serialization.
- **The hierarchy DB query** (`get_domain_hierarchy()`) takes 1.15s cold on 390K domain rows, but this is only part of the delay.
- **Key data:** 25 parent categories, ~175 child domains = ~200 total checkboxes.
- **Attempted fixes that didn't help:**
  - Making the function async + `run.io_bound()` for DB call
  - Pre-fetching hierarchy during `execute_search()` and caching on `search_state`
  - Adding `_domain_hierarchy_cache` to FjmsService
  - Removing per-checkbox `on_value_change` handlers
- **Likely fix needed:** Replace 200 individual `ui.checkbox` elements with a single fast-rendering approach:
  - Option A: Use `ui.html()` with raw HTML checkboxes + JavaScript handlers, read state on Apply
  - Option B: Use Quasar `q-tree` with tick-strategy (single component, native virtual scrolling)
  - Option C: Only show parent-level checkboxes initially, expand children on demand (25 elements initially vs 200)

### Bug 2: Select All / Select None doesn't toggle "Other" checkboxes
- **Symptom:** Clicking "Select All" checks most checkboxes but "Other" entries stay unchecked. "Select None" has same issue.
- **Root cause confirmed:** `get_domain_hierarchy()` has a dedup step that promotes grandchildren to parent level. When "Business Documents" (child of "Documentary") is merged into "Documentary", its child "Other" gets promoted. Same for "Communal Documents" → its "Other" also gets promoted. Result: "Documentary" has **TWO children named "Other"** with different counts (33 and 3).
  - In the dialog, both create `ui.checkbox` elements, but `checkboxes[child['domain']]` uses the same key for both → **second overwrites first** in the dict.
  - `check_all()` iterates dict values, only reaching the second checkbox. First checkbox (visible in UI) stays unchanged.
- **Verified with data:**
  ```
  Documentary has 2 "Other" children:
    Other (from Business Documents): count=33
    Other (from Communal Documents): count=3
  Same for Philosophy, Theology, Ethical literature
  ```
- **Fix location:** `shared/fjms_service.py:get_domain_hierarchy()` — after the orphan promotion loop (line ~643), add a merge step to consolidate duplicate children within each parent by summing counts.
- **Stashed fix** includes this merge step but user reported it didn't work. Possible reasons:
  - User may not have restarted the web app after changes
  - The hierarchy cache may have served stale data
  - The `_domain_hierarchy_cache` was a class variable, may not have been invalidated

## Key Code Locations

| File | Lines | What |
|------|-------|------|
| `web/pages/search.py` | ~1682-1890 | `_open_domain_filter_dialog` — creates dialog with checkbox tree |
| `web/pages/search.py` | ~2055-2080 | Domain collection during `execute_search()` |
| `web/pages/parallels.py` | ~1386-1585 | Parallels domain filter dialog (same pattern) |
| `shared/fjms_service.py` | ~549-660 | `get_domain_hierarchy()` — SQL query + hierarchy building + dedup |
| `shared/fjms_service.py` | ~612-643 | The dedup/orphan promotion step that creates duplicate children |
| `genizah_app.py` | ~4780-4995 | Desktop `DomainFilterDialog` (PyQt6, separate implementation) |

## Data Context

- `fjms_enrichment.db` domains table: 390K rows
- 25 parent categories, ~175 unique child domains
- "Other" appears as child of 15 different parents (always with a ParentDomain, never standalone)
- Hierarchy query: `GROUP BY Domain, ParentDomain` → ~200 rows → Python dedup/merge

## What NOT to Do
- Don't add more per-checkbox event handlers — that's what made it slow
- Don't use `_batch_updating` flags — they didn't help with the "Other" bug
- Don't just make the function async — the bottleneck is element creation, not the DB query
- Don't add `_domain_hierarchy_cache` as a class variable — instance lifecycle issues with `get_fjms_service(thread_safe=True)` creating new instances

## Recommended Approach
1. Fix `get_domain_hierarchy()` duplicate children merge (simple dict merge after orphan promotion)
2. Replace the checkbox-per-domain dialog with a faster UI approach (HTML/JS or Quasar tree)
3. Test by restarting web app fresh, doing a new search, then opening the filter dialog
