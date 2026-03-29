---
plan: 56-04
status: complete
started: 2026-03-29
completed: 2026-03-29
gap_closure: true
---

## Summary

Fixed two HIGH gaps from post-implementation code review: export and bulk actions now respect manuscript exclusions.

### Desktop (5 functions patched)
- `on_search_select_all_toggled` — skips hidden rows
- `on_search_result_item_changed` — skips hidden rows in all_checked computation
- `_update_search_export_label` — skips hidden rows in selection detection
- `search_add_selected_to_list` — skips hidden rows
- `export_results` — skips hidden rows in selection loop + fallback uses `_collect_sorted_results()` instead of `self.last_results`

### Web (1 line)
- `render_results` syncs `state.last_results = results` alongside `displayed_results` so export API endpoints reflect post-search filtering

## Verification
- 7 `isRowHidden` hits in genizah_app.py across all target functions
- `state.last_results` sync in render_results at line 4471
- Both apps import clean
