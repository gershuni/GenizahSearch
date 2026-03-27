---
phase: 54-dimensions-display-filtering
plan: 03
subsystem: database
tags: [sqlite, measurement-filtering, fjms-service, batch-lookup, coalesce]

requires:
  - phase: 54-01
    provides: manuscript_measurements table with catalog/computed dimensions
  - phase: 54-02
    provides: measurements dialog display in web/desktop browse

provides:
  - get_filter_sys_ids with 11 measurement params for pre-search filtering
  - get_measurement_summaries_batch for post-search filtering
  - _normalize_range helper for reversed min/max guard
  - avg_line_height_mm column in manuscript_measurements
  - Indexes for avg_line_height_mm, avg_text_density, material, avg_num_lines

affects: [54-04, web-search-filtering, desktop-search-filtering]

tech-stack:
  added: []
  patterns:
    - "COALESCE(catalog, computed) for width/height maximizes coverage"
    - "_normalize_range backend guard for reversed min/max bounds"
    - "Subquery IN pattern for measurement filtering in get_filter_sys_ids"
    - "Batch lookup with chunked queries (500 per chunk) and deduplication"

key-files:
  created: []
  modified:
    - shared/fjms_service.py
    - scripts/import_measurements.py
    - tests/test_measurements.py

key-decisions:
  - "COALESCE(catalog_width_cm, max_computed_width_cm) for width/height filtering prefers catalog but falls back to computed for maximum coverage"
  - "_normalize_range swaps reversed min/max at backend layer as guard clause (D-19)"
  - "measurement_material uses IN clause for multi-value list, distinct from existing material_include/material_exclude which filters on catalog_fields FragmentMaterial"

patterns-established:
  - "_normalize_range helper: reusable for any numeric range normalization"
  - "get_measurement_summaries_batch: chunk-based batch fetch with tuple row fallback"

requirements-completed: [DIM-02, DIM-03]

duration: 12min
completed: 2026-03-27
---

# Phase 54 Plan 03: Measurement Filtering Backend Summary

**Extended get_filter_sys_ids with 11 measurement params (width/height/line-count/line-height/text-density/material), added batch summary lookup, avg_line_height_mm column, and 17 filter tests**

## Performance

- **Duration:** 12 min
- **Started:** 2026-03-27T10:03:18Z
- **Completed:** 2026-03-27T10:15:38Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- get_filter_sys_ids now accepts 11 measurement params with COALESCE(catalog, computed) for width/height
- _normalize_range helper swaps reversed min/max bounds at backend layer
- get_measurement_summaries_batch returns measurement summaries with dedup, missing-column grace, tuple row fallback
- avg_line_height_mm column added to manuscript_measurements with idempotent migration
- 4 new indexes for filter columns (avg_num_lines, avg_line_height_mm, avg_text_density, material)
- 17 new filter tests covering all 6 measurement fields, NULL exclusion, COALESCE fallback, reversed bounds, multi-material IN clause, batch lookup edge cases

## Task Commits

Each task was committed atomically:

1. **Task 1: Add avg_line_height_mm column** - `b6ffdfbd` (test: RED), `b2a02691` (feat: GREEN)
2. **Task 2: Extend get_filter_sys_ids + batch lookup** - `1dec919a` (test: RED), `d5539a2e` (feat: GREEN)

_Note: TDD tasks have RED (failing test) + GREEN (implementation) commits._

## Files Created/Modified
- `shared/fjms_service.py` - Added _normalize_range, 11 measurement params to get_filter_sys_ids, get_measurement_summaries_batch
- `scripts/import_measurements.py` - Added avg_line_height_mm column, filter indexes, migrate_add_line_height function
- `tests/test_measurements.py` - Added TestLineHeightColumn (4 tests), TestMeasurementFiltering (17 tests)

## Decisions Made
- COALESCE(catalog_width_cm, max_computed_width_cm) prefers catalog (authoritative) but falls back to computed (image-derived) for manuscripts without catalog data
- _normalize_range swaps reversed min/max at backend -- UI may also validate, but backend is the guard
- measurement_material param is separate from existing material_include/material_exclude to avoid confusion (former filters on manuscript_measurements.material, latter on catalog_fields.FragmentMaterial)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None - all methods are fully wired with real SQL queries.

## Next Phase Readiness
- Measurement filtering backend is complete, ready for UI wiring in plan 54-04
- Both pre-search (get_filter_sys_ids) and post-search (get_measurement_summaries_batch) APIs available

---
*Phase: 54-dimensions-display-filtering*
*Completed: 2026-03-27*
