---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "06"
subsystem: web-catalog
tags: [gap-closure, library-filter, catalog-browse, browse-to-search, checkbox-dialog, finding-1]
dependency_graph:
  requires: [129-05-SUMMARY.md]
  provides: [GAP-E-closed, GAP-F-closed, FINDING-1-closed]
  affects: [web/pages/catalog_browse.py, web/components/filter_panel.py, tests/test_libfilter_catalog.py]
tech_stack:
  added: []
  patterns: [NiceGUI ui.dialog + HTML+JS checkbox readback, persist→reload lifecycle via safe_storage]
key_files:
  modified:
    - web/pages/catalog_browse.py
    - web/components/filter_panel.py
    - tests/test_libfilter_catalog.py
decisions:
  - "FINDING 1 guarded: Apply disabled client-side at zero-checked + Python defensive short-circuit; only 'Select All' provided (no 'Select None'); all-unchecked state unreachable as applied filter"
  - "persist→reload lifecycle: consume_incoming_filters persists 'search_library_filter' (flat literal key, NOT {pfx}_filter_*) AND setattr for live render; search.py load at :187 < consume at :199 so setattr is not overwritten"
  - "_library_apply_selection: all-checked => [] (clear filter / show all); strict subset => that subset; never called with zero-checked"
  - "library_filter_ctrl_ref renamed to library_filter_btn_ref; _update_library_filter_ctrl replaced by _update_library_filter_btn"
metrics:
  duration: "~7 minutes"
  completed: "2026-06-28"
  tasks_completed: 3
  files_changed: 3
---

# Phase 129 Plan 06: GAP-E + GAP-F Closure (Catalog Library Filter) Summary

**One-liner:** Catalog library filter replaced from ui.select dropdown to checkbox dialog (GAP-E); browse→search handoff threads library selection via consume_incoming_filters persist→reload lifecycle (GAP-F); FINDING 1 all-unchecked guard implemented.

## What Was Built

### Task 1 — Tests (RED, TDD)
Added 9 new tests to `tests/test_libfilter_catalog.py` encoding the full GAP-E/F closure contracts:

- **GAP-F build:** `test_has_active_filters_true_when_library_selected` + `test_build_incoming_filters_includes_library_filter` — source-scan that `_has_active_filters` and `_build_incoming_filters` include `current_library_filter`
- **GAP-F consume:** `test_consume_sets_library_filter_and_persists` (monkeypatched storage), `test_consume_does_not_crash_without_library_filter_attr` (parallels state safety)
- **GAP-F lifecycle:** `test_lifecycle_persist_then_reload` (full persist→reload round-trip), `test_lifecycle_source_order_load_before_consume` (source-line ordering: `_safe_get('search_library_filter')` < `consume_incoming_filters(`)
- **GAP-E control:** `test_catalog_library_control_is_dialog_not_select` (dialog present; `Select libraries...` old label absent)
- **FINDING 1:** `test_catalog_dialog_no_deselect_all_and_apply_guarded` (no `tr('Select None')`; hint string present; `Select All` present), `test_catalog_apply_mapping_all_checked_clears_filter` (all-checked branch yields `[]`)

### Task 2 — GAP-F threading (GREEN for threading tests)
**`web/pages/catalog_browse.py`:**
- `_has_active_filters()`: added `current_library_filter['value']` to the `any([...])` list so library-only selections enable "Search in these results" (previously disabled)
- `_build_incoming_filters()`: added `incoming['library_filter'] = list(current_library_filter['value'])` when a library selection is active, so the key is sent through safe_storage to the receiving page

**`web/components/filter_panel.py` — `consume_incoming_filters()`:**
- Added library_filter branch after the `material_exclude` block (before `safe_user_pop`):
  - `setattr(state, 'library_filter', [str(c) for c in incoming['library_filter']])` — live this render
  - `persist_value('search_library_filter', getattr(state, 'library_filter', _lib_codes))` — durable for next fresh render
  - Defensive try/except so a parallels state lacking `.library_filter` does not crash
  - Comment documenting the literal-key rationale and lifecycle

**Persist→reload lifecycle confirmed:**
- search.py load at line 187-189: `_lib0 = _safe_get('search_library_filter', [])` → `search_state.library_filter = [c for c in _lib0 if c in LIBRARY_CODES]`
- consume at line 199: `consume_incoming_filters(search_state, 'search', require_from_browse=False)`
- Ordering: load (char ~5600) < consume (char ~5800) — setattr is NOT overwritten by load
- Both paths required: setattr = live this render; persist = durable next render
- `SearchUIState.library_filter` is a plain list attribute (search_state.py:61), no property side effects; direct setattr works correctly

### Task 3 — GAP-E dialog + FINDING 1 (GREEN for all tests)
**`web/pages/catalog_browse.py`:**

Replaced the `ui.select(multiple=True)` Library Filter Card with:

1. **`library_filter_btn_ref`** (renamed from `library_filter_ctrl_ref`) — ref for the dialog-opening button
2. **`_update_library_filter_btn()`** — syncs button label: "All Libraries" (no filter) vs "Libraries (N)" (active)
3. **`_library_apply_selection(checked_codes, all_codes)`** — pure mapping helper:
   - `set(checked_codes) == set(all_codes)` ⇒ `return []` (clear filter / show all)
   - strict non-empty subset ⇒ `return list(checked_codes)` (inclusion filter)
   - never called with empty checked_codes (FINDING 1 guard)
4. **`_open_library_filter_dialog()`** — checkbox dialog mirroring the web-search library dialog:
   - `ui.dialog()` + scrollable checkbox list (all LIBRARY_CODES minus 'LOCAL', D-02 plain list no counts)
   - HTML+JS: `catLibFilterUpdateApply` (disables Apply when checked=0), `catLibFilterGetChecked` (JS readback), `catLibFilterSelectAll` (re-check all)
   - **Select All** only bulk action; no "Select None"/deselect-all (FINDING 1)
   - Python Apply: defensive short-circuit `if not checked: ui.notify(tr('Select at least one library, or check all to clear the filter'))` — never writes `[]` via the all-unchecked path
   - Apply (≥1 checked): calls `_library_apply_selection` → `safe_user_set` → `_update_library_filter_btn()` → `refresh_results()` → `render_chips()` → `_update_search_buttons()`
5. **UI card** replaced: button + tooltip, `library_filter_btn_ref['ref'] = lib_btn`, `_update_library_filter_btn()` initial sync
6. **Call sites updated**: `_update_library_filter_ctrl()` → `_update_library_filter_btn()` in `clear_filter('library')`, `clear_all_filters()`, `clear_library_code()`

## Verified Push-Down Layer (Unchanged)
The verified data layer from plans 129-01/02/03 is untouched:
- `_fetch_results_blocking` → `resolve_library_sys_ids(library_codes, _state.meta_mgr)` inside `io_bound` → `get_browse_results(library_codes=, library_sys_ids=)` — push-down before COUNT/LIMIT
- `clear_library_code`, `clear_filter('library')`, `clear_all_filters()` all call `await refresh_results()` (not `fetch_results()`)
- Per-code chips in `render_chips()` still call `clear_library_code` (unchanged)

## Search.py Source Ordering (Confirmed, No Edit Needed)
Confirmed search.py has the correct load-before-consume ordering:
- Load: `_safe_get('search_library_filter', [])` at line 187-189 (early state-init block)
- Consume: `consume_incoming_filters(search_state, 'search', require_from_browse=False)` at line 199
- Because consume runs AFTER the load and (a) sets `search_state.library_filter` directly via setattr and (b) persists 'search_library_filter', the incoming codes survive into the first use of `search_state.library_filter` AND into the next fresh render.
- **No search.py edit required** — confirmed 2026-06-28.

## Deviations from Plan
None — plan executed exactly as written.

## Known Stubs
None — all data paths wired.

## Threat Flags
No new network endpoints, auth paths, file access patterns, or schema changes introduced beyond what the plan specified. The `library_filter` key in `incoming_filters` is consumed and validated (coerced to `list[str]`); `search_library_filter` in safe_storage is validated against `LIBRARY_CODES` on load (search.py:189). T-129-06-01 through T-129-06-04 all mitigated as designed.

## Commits
- `ec044617` — test(129-06): add failing tests (RED phase, 9 new tests)
- `5587e05d` — feat(129-06): thread library filter through browse→search handoff (GAP-F)
- `ffd11c36` — feat(129-06): replace catalog ui.select with checkbox dialog (GAP-E + FINDING 1)

## Test Results
```
tests/test_libfilter_catalog.py: 21/21 passed (6 original LIBFILTER-02 + 15 new GAP-E/F/FINDING-1)
tests/test_seed023_catalog_filters.py: 11/11 passed (GUARD-02)
tests/test_no_raw_storage_access.py: 6/6 passed (Phase 87)
tests/test_catalog_availability_filter.py: 4/4 passed
Total: 35 passed, 0 failed
ruff: clean (catalog_browse.py, filter_panel.py)
```

## Live Render Smoke (CLAUDE.md Render-Smoke Gap)
Cannot verify headlessly per CLAUDE.md note. Manual verifications needed:
1. Catalog: click "All Libraries" button → dialog opens; Apply disabled when nothing checked; applying CUL subset narrows total; "Search in these results" enabled
2. Browse→search: selecting CUL in catalog then clicking "Search in these results" navigates to /search with library_filter='CUL' applied; filter survives a /search reload
3. Chips: per-code chips render; clicking × calls clear_library_code; Clear All works

## Self-Check: PASSED

| Item | Result |
|------|--------|
| `web/pages/catalog_browse.py` exists | FOUND |
| `web/components/filter_panel.py` exists | FOUND |
| `tests/test_libfilter_catalog.py` exists | FOUND |
| `129-06-SUMMARY.md` exists | FOUND |
| Commit `ec044617` (RED tests) | FOUND |
| Commit `5587e05d` (GAP-F) | FOUND |
| Commit `ffd11c36` (GAP-E) | FOUND |
| 35 tests pass | PASSED |
| ruff clean | PASSED |
