---
phase: 54-dimensions-display-filtering
plan: 04
subsystem: search-filtering
tags: [measurements, filtering, pre-search, post-search, ui, both-apps]
dependency_graph:
  requires: [54-03]
  provides: [measurement-filter-ui-web, measurement-filter-ui-desktop]
  affects: [web/pages/search.py, web/components/filter_panel.py, genizah_app.py, genizah_translations.py]
tech_stack:
  added: []
  patterns: [shared-rerender-helper, blur-debounced-recompute, separate-post-filter-state, race-guard-pattern]
key_files:
  created: []
  modified:
    - web/pages/search.py
    - web/components/filter_panel.py
    - genizah_app.py
    - genizah_translations.py
decisions:
  - "Material labeled 'Material (measured)' to distinguish from existing printed filter (review concern #7)"
  - "Pre-search recompute fires on blur event, not on every keystroke (review concern #6)"
  - "Post-search state is SEPARATE from pre-search in both apps (review concern #1/#3)"
  - "Web: _apply_measurement_post_filters shared helper called from 5 rerender paths (review concern #1)"
  - "Desktop: _measurement_fetch_complete race guard shows rows during fetch, excludes only after (review concern #8)"
  - "Desktop session persistence via _save_session/_restore_session, NOT QSettings (review concern #5)"
metrics:
  duration: 25min
  completed: "2026-03-27T10:43:21Z"
  tasks_completed: 2
  tasks_total: 2
  files_modified: 4
---

# Phase 54 Plan 04: Measurement Filter UI (Both Apps) Summary

Wired 6-field measurement range filtering (width, height, lines, line height, text density, material) into user-facing filter panels for both web and desktop apps, with separate pre-search and post-search state.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | bf066455 | Web measurement filter UI in pre-search and post-search panels |
| 2 | 8e53556b | Desktop measurement filter UI in PreSearchFilterDialog + post-search filtering |

## Key Changes

### Task 1: Web UI (search.py, filter_panel.py, translations)

- Added 22 state attributes to search_state (11 pre-search filter_* + 11 post-search post_filter_*)
- Added collapsible "Measurements" expansion section in pre-search filter panel with 5 min/max number input pairs and material multi-select
- Added identical "Measurements" expansion in post-search filter panel
- Created `_apply_measurement_post_filters(results, state)` shared helper function
- Wired shared helper into ALL 5 rerender paths: apply_filters, _apply_printed_filter_and_render, _apply_domain_exclusions, history restore, _render_with_filters (staged enrichment)
- Updated `has_active_filters()` with 11 measurement getattr checks (backward-compatible)
- Updated `load_filter_state()` to restore 11 measurement attrs from session storage
- Updated `recompute_filter_count()` to snapshot and pass measurement params
- Updated `_compute_restrict()` to pass measurement params to get_filter_sys_ids
- Added measurement data to staged enrichment (collect_fjms_enrichment returns 4-tuple now)
- Added teal measurement filter chips with range formatting
- Updated clear_filters and _clear_all_adv_filters to reset measurement state

### Task 2: Desktop UI (genizah_app.py)

- Added Measurements QGroupBox in PreSearchFilterDialog with QDoubleSpinBox pairs and material QCheckBox widgets
- Increased dialog minimum size from 620 to 720 height
- Added `_get_measurement_filters()` method for extracting values from spin boxes
- Merged measurement filters into `_get_current_filter_dict()` return value
- Updated FilterCountWorker.run with 11 measurement kwargs
- Added separate `_post_measurement_filters` dict (NOT reusing pre_search_filters)
- Batch-fetches `_result_measurement_map` via get_measurement_summaries_batch in _launch_enrichment_workers
- Added measurement section E in `_apply_results_table_filters` with race guard
- Persists post_measurement_filters via existing _save_session/_restore_session machinery
- Added teal measurement chips (#e0f2f1) in _rebuild_dialog_chips

### Translations

- Added: "Material (measured)", "Density", "Min", "Width (cm)", "Height (cm)", "Line Height (mm)"

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None - all data paths are wired to live backend APIs from plan 54-03.

## Self-Check: PASSED

- All 4 modified files exist on disk
- Both commits (bf066455, 8e53556b) present in git log
- _apply_measurement_post_filters: 6 occurrences in search.py (1 def + 5 call sites)
- _post_measurement_filters: 7 occurrences in genizah_app.py
- "Material (measured)": present in search.py (2), genizah_app.py (2), genizah_translations.py (1)
- pytest tests/test_measurements.py: 12/12 passed
