---
phase: 45-filtered-search-context
plan: 04
status: complete
completed: "2026-03-03"
duration: 9min
tasks_completed: 2
tasks_total: 2
subsystem: web-parallels
tags: [filtering, pre-search, chips, exclusion, parallels, per-manuscript]

requires:
  - phase: 45-01
    provides: get_filter_sys_ids, restrict_sys_ids on search_composition_logic
  - phase: 45-02
    provides: search page filter panel pattern to replicate
provides:
  - Advanced Filters panel on web parallels page
  - Chip bar with removable filter chips and manuscript count
  - Per-manuscript exclusion with exclude buttons
  - Auto-exclude source manuscript
  - Import exclusions from word search
  - restrict_sys_ids wiring to search_composition_logic
affects: [web/pages/parallels.py]

tech_stack:
  added: []
  patterns: [collapsible expansion, removable chips, cross-filtered selects, async filter count, per-manuscript exclusion]

key_files:
  created: []
  modified: [web/pages/parallels.py]

key_decisions:
  - "Advanced Filters panel placed between input card and Filter Text section using ui.expansion"
  - "Filter chip bar always visible (even when panel collapsed) with per-manuscript exclusion count chip"
  - "Per-manuscript excluded results shown in separate collapsible section with restore buttons, distinct from high-frequency/source-text filtered section"
  - "restrict_sys_ids computed before run_search via await run.io_bound, empty set triggers early return with warning"
  - "Auto-exclude source manuscript by parsing sys_id from initial_text URL param"
  - "Filter state persisted with _persist() pattern, keys prefixed parallels_filter_"
  - "Lab mode search does not receive restrict_sys_ids (lab_composition_search does not support it)"
  - "Pre-search material_exclude coexists with existing post-search printed badges (no printed_filter toggle on parallels)"

requirements_completed: [FILT-01, FILT-02, FILT-03, FILT-04, FILT-06]
---

# Phase 45 Plan 04: Web Parallels Filter Panel & Pre-Search Integration Summary

**Collapsible Advanced Filters panel with domain/author/work/date/material controls, removable chip bar, per-manuscript exclude buttons, auto-exclude source, import exclusions, and restrict_sys_ids wiring to search_composition_logic on the web parallels page.**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-02T22:51:35Z
- **Completed:** 2026-03-02T23:00:13Z
- **Tasks:** 2/2
- **Files modified:** 1

## Summary

Added the full pre-search filtering interface and per-manuscript exclusion system to the web parallels page, matching the search page pattern established in Plan 02. Researchers can now constrain composition/parallels searches by domain, author, work, date range, and material type before searching. Active filters are displayed as removable chips. The filter set is computed via get_filter_sys_ids() and passed as restrict_sys_ids to search_composition_logic for genuine speed improvement. Per-manuscript exclude buttons on each result allow removing individual manuscripts from view. Source manuscripts are auto-excluded when navigating from another module.

## Changes

- `web/pages/parallels.py` (713 lines added):
  - **ParallelsState**: Added 12 filter state fields (filter_domain, filter_author, filter_author_name, filter_work, filter_work_name, filter_date_from, filter_date_to, filter_material_exclude, filter_manuscript_count, restrict_sys_ids, excluded_manuscript_ids, auto_excluded_source_id)
  - **_persist() helper**: Session persistence pattern matching search page
  - **Incoming filters consumption**: Reads incoming_filters from app.storage.user for Path B navigation, clears after consuming, takes priority over session restore
  - **Session restore**: All filter fields + excluded_manuscript_ids restored from app.storage.user on page load
  - **_has_active_filters()**: Helper to check if any pre-search filters are active
  - **Auto-exclude source manuscript**: Parses sys_id from initial_text URL param, adds to excluded_manuscript_ids
  - **Advanced Filters panel**: Collapsible ui.expansion with domain select (hierarchical from get_domain_hierarchy), author select (cross-filtered by domain), work select (cross-filtered by domain+author), date range inputs, Exclude Printed checkbox, Import exclusions button, Clear All button
  - **Chip bar**: Row of removable ui.chip elements for each active filter, per-manuscript exclusion count, and manuscript count badge. Colors match search page (purple=domain, blue=author, teal=work, orange=date, red=material)
  - **Filter change handlers**: Each filter change persists to storage, cross-filters dependent selects, recomputes manuscript count via async _recompute_p_filter_count
  - **Per-manuscript exclusion**: Exclude button (remove_circle_outline icon) on each manuscript group header. Excluded items shown in separate collapsible "Excluded Manuscripts" section with restore buttons
  - **_rerender_with_exclusions()**: Helper to re-render results applying both domain and per-manuscript exclusions
  - **Pre-search restrict_sys_ids**: Computed before run_search via get_filter_sys_ids, merged with excluded_manuscript_ids, passed to search_composition_logic. Empty set triggers early return with warning
  - **Search summary filter suffix**: Status line includes filter info when active (e.g., "filtered: Halakha, 3,241 manuscripts")
  - **Composition history integration**: Filter state saved in params.filters dict, per-manuscript exclusions in state_snapshot.excluded_manuscript_ids. Both restored when recalling history entries

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Filter panel, chip bar, per-manuscript exclusion | c6f80448 | web/pages/parallels.py |
| 2 | restrict_sys_ids integration with search execution | 1fb2d869 | web/pages/parallels.py |

## Verification

- File parses without syntax errors (ast.parse OK)
- Module imports cleanly: `from web.pages.parallels import create_parallels_page` OK
- All 19 required keywords present: filter_domain, filter_author, restrict_sys_ids, excluded_manuscript_ids, auto_excluded_source_id, incoming_filters, get_filter_sys_ids, _update_p_chip_bar, _import_exclusions_from_word_search, _rerender_with_exclusions, Advanced Filters, Excluded Manuscripts, Source manuscript, _filter_suffix, etc.
- restrict_sys_ids=captured_restrict_sys_ids parameter passing confirmed in search_composition_logic call

## Deviations from Plan

None -- plan executed exactly as written.

## Notes for Downstream Plan 05

- **Path B navigation**: incoming_filters consumption is ready. Write to app.storage.user['incoming_filters'] dict with keys: domain, author, author_name, work, work_name, date_from, date_to, material_exclude. Navigate to /parallels with text param
- **Hebrew translations needed**: All new user-facing strings use tr() and will need Hebrew entries in Plan 05 (translations plan)
- **Lab mode limitation**: lab_composition_search() does not accept restrict_sys_ids, so lab mode searches are not filtered by pre-search criteria. This matches the desktop behavior where lab mode is secondary

## Self-Check: PASSED
