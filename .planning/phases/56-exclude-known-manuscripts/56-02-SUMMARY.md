---
plan: 56-02
status: complete
started: 2026-03-29
completed: 2026-03-29
---

## Summary

Wired manuscript exclusion into the web search page with full feature set:

- **Two entry points (D-01)**: Results header button + filter panel button
- **Picker dialog**: Tabbed UI with "From List" (async list fetch) and "From File" (txt/CSV upload with resolution report table)
- **Resolution report (D-04)**: Per-row status table (Shelfmark, Normalized, sys_id, Status) with 200-row cap
- **Unified filter pipeline**: `_apply_manuscript_exclusions()` called from 7+ re-render paths (search completion, printed filter toggle, domain filter, history restore, enrichment re-render)
- **Per-source clear (D-06)**: Removable chips next to exclude button
- **Count display (D-07)**: Button text shows total excluded count, red coloring when active
- **Collapsible excluded section (D-05)**: Shows excluded manuscripts with source labels at bottom of results
- **Session persistence (D-10)**: `search_exclusion_sources` in app.storage.user
- **Independence (D-08/D-09)**: No cross-contamination with refinement chain

## Key Files

### Modified
- `web/pages/search.py` — +358 lines: imports, state fields, session restore, _apply_manuscript_exclusions, _update_exclude_btn, _remove_exclusion_source, _show_exclusion_dialog, two button entry points, exclusion chips, excluded section in render_results, wiring at all re-render paths

## Verification

- `python -c "from web.pages import search; print('OK')"` — import clean
- `_apply_manuscript_exclusions` appears at 8 locations (1 def + 7 call sites)
- 2 async `run.io_bound` calls (build_shelf_map, get_items_in_list_sync)
- No `refinement_restrict_sys_ids` modification in exclusion code

## Deviations

None significant. Filter panel button uses inline rendering rather than a separate component file.
