---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "05"
subsystem: web-search-ui
tags: [gap-closure, library-filter, dialog, chips, visibility, i18n]
dependency_graph:
  requires: [129-02, 129-03, 129-04]
  provides: [_open_library_filter_dialog, _library_apply_selection, library_chip_row]
  affects: [web/pages/search.py, genizah_translations.py]
tech_stack:
  added: []
  patterns: [ui.dialog+JS-readback (mirroring _open_domain_filter_dialog), CSS-visibility _set_btn_visible]
key_files:
  created: []
  modified:
    - web/pages/search.py
    - genizah_translations.py
    - tests/test_libfilter_web_search.py
decisions:
  - "GAP-C dialog: HTML checkbox + JS readback approach (mirrors _open_domain_filter_dialog) preferred over plain ui.checkbox for consistency; ~20 codes is small so either works"
  - "FINDING-1 guard: both JS-disabled-Apply (client-side) and Python short-circuit (server-side) applied for defense-in-depth"
  - "_library_apply_selection pure helper: all-checked=>[] (clear sentinel) / subset=>subset — never fed empty set"
  - "library_chip_row declared in post-search column after results_header; _update_library_chips closure captures it by name (late-binding Python semantics)"
metrics:
  duration: "8 minutes"
  completed: "2026-06-28"
  tasks: 2
  files_changed: 3
---

# Phase 129 Plan 05: Library Filter Dialog + Chip Relocation (GAP-A/B/C/D + FINDING-1) Summary

**One-liner:** Replaced broken library ui.menu with a checkbox dialog (mirroring Filter by Domains), fixed button visibility to use CSS visibility consistently, and relocated library chips to a dedicated post-search row.

## What Was Built

Four gaps from the 2026-06-28 human smoke test closed:

**GAP-A (button never renders):** `library_filter_btn.set_visibility(False)` at construction was using NiceGUI `display:none` while the results-arrive reveal path used `_set_btn_visible()` (CSS `visibility:hidden/visible`). The two mechanisms fought — the button never appeared. Fixed: changed construction call to `_set_btn_visible(library_filter_btn, False)`. Now 3 sites all use `_set_btn_visible` consistently (construct False + New-Search reset False + results-arrive reveal True).

**GAP-B (domain button opened two menus):** The bare `ui.menu()` inside the shared filter button row caused Quasar to anchor it to the entire row. Clicking the domain button also triggered the library menu. Fixed: deleted the `ui.menu()` entirely, switching to a dialog.

**GAP-C (wrong interaction model):** The `ui.menu` with `ui.menu_item` rows was the wrong pattern — users wanted a checkbox dialog like "Filter by Domains". Removed: `_library_menu_ref`, `_rebuild_library_menu`, `_toggle_library_code`. Added: `_open_library_filter_dialog()` mirroring `_open_domain_filter_dialog` — a `ui.dialog` with HTML checkboxes + JS readback. Inclusion model (all checked by default, uncheck = hide). `library_filter_btn.on('click')` now opens the dialog.

**FINDING-1 (all-unchecked collides with `[]` show-all sentinel):** Added two-layer guard:
1. JS handler `libFilterUpdateApply` disables the Apply button client-side when `checked-count == 0`
2. Python `apply_library_filter` defensively short-circuits with `ui.notify(tr('Select at least one library, or check all to clear the filter'))` if the checked set arrives empty (tamper defense)
No "deselect-all" / "Select None" action is provided. "Select All" is the only bulk action.

**GAP-D (chips in wrong area):** Library chips were rendering in `chip_bar_container` (the pre-search "search only in…" bar). Moved to `library_chip_row` — a dedicated `ui.row` in the post-search column, between `results_header` and `refinement_strip`. Changes:
- Removed the `account_balance` chip block from `_update_chip_bar`
- Reverted `has_any = _has_active_filters() or _pos_active or bool(search_state.library_filter)` back to `has_any = _has_active_filters() or _pos_active` (library no longer drives chip_bar visibility)
- `_update_library_chips()` now clears/rebuilds `library_chip_row` directly

**Pure mapping helper `_library_apply_selection`:** Added as a closure-level testable function implementing the apply mapping: `all-checked => []` (clear filter / show all), `strict subset => that subset`. Never called with an empty checked set.

**i18n:** Added `"Filter by Library"` (title-case dialog header) and `"Select at least one library, or check all to clear the filter"` (FINDING-1 hint) to `genizah_translations.TRANSLATIONS` with correct Hebrew values.

## Deviations from Plan

None — plan executed exactly as written.

## Test Results

| Suite | Result |
|-------|--------|
| `tests/test_libfilter_web_search.py` (12 tests) | 12/12 passed |
| `tests/test_pgp_filter_cascade.py` (4 tests) | 4/4 passed |
| `tests/test_no_raw_storage_access.py` (6 tests) | 6/6 passed |
| `ruff check web/pages/search.py genizah_translations.py` | All checks passed |

**TDD gate compliance:**
- RED commit: `f6971930` — 5 new control-surface tests fail, 7 pre-existing data-layer tests pass
- GREEN commit: `afef49de` — all 12 tests pass

## Known Stubs

None. The filter is fully wired: dialog opens, Apply reads checked codes via JS, `_library_apply_selection` maps them, `search_state.library_filter` is set, `persist_value` saves state, `_update_library_btn` + `_update_library_chips` refresh UI, the existing cascade dispatch (`_apply_manuscript_exclusions` / `_apply_domain_exclusions` / `_apply_printed_filter_and_render`) re-filters and re-renders.

## Threat Surface Scan

No new security-relevant surface introduced. The library dialog reads back checkbox states via `ui.run_javascript` (same pattern as the domain dialog) — the Python Apply handler validates that the checked set is non-empty before committing (T-129-05-03 mitigated). Library codes are filtered through `_compute_library_facets` which only offers codes present in actual results; load-time validation against `LIBRARY_CODES` at line 189 is unchanged (T-129-05-01).

## Render-Smoke Gap Note

Per CLAUDE.md NiceGUI render-smoke gap: headless tests cannot verify the dialog opens visually, the Apply button disables on zero-checked, the chips appear in the post-search area at the correct position, or the Hebrew RTL layout. A live-browser smoke test at release time should confirm:
1. Library button becomes visible after results arrive
2. Clicking opens the checkbox dialog (not a menu)
3. Apply button is disabled when all checkboxes are unchecked
4. Select All enables Apply and clears the filter (`library_filter == []`)
5. Applying a subset shows chips in the post-search filter row (not in the "search only in…" bar)
6. Domain button opens only the domain dialog (no library menu interference)
7. Pre-search chip bar still shows domain/measurement/text-position chips correctly

## Self-Check

### Created files exist

- `tests/test_libfilter_web_search.py` — FOUND (modified)
- `web/pages/search.py` — FOUND (modified)
- `genizah_translations.py` — FOUND (modified)

### Commits exist

- RED: `f6971930` — FOUND
- GREEN: `afef49de` — FOUND

## Self-Check: PASSED
