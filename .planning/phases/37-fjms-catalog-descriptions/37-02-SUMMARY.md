---
phase: 37-fjms-catalog-descriptions
plan: 02
subsystem: service
tags: [sqlite, fjms, service-layer, testing, translations]

# Dependency graph
requires:
  - phase: 37-fjms-catalog-descriptions
    provides: "v3.0.0 sidecar with 4 new catalog child tables"
provides:
  - "get_catalog_source_counts() for batch button label counts excluding generic sources"
  - "get_catalog_detail() returning structured dict with records, running_titles, sizes, fields, free_descriptions"
  - "v3.0.0-compatible get_catalog() and get_catalog_records() with new columns"
  - "Test fixtures updated to v3.0.0 schema with 4 child tables"
  - "10 translation keys for catalog dialog labels"
affects: [37-03, 37-04, fjms-catalog-dialog]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Graceful child-table fallback: try/except per sub-query returning empty defaults for backward compat"
    - "Batch source count query with generic name exclusion via NOT IN clause"

key-files:
  created: []
  modified:
    - "shared/fjms_service.py"
    - "tests/test_fjms_service.py"
    - "genizah_translations.py"

key-decisions:
  - "get_catalog_detail() wraps each child-table query in try/except for backward compat with old sidecars missing tables"
  - "New v3.0.0 columns accessed via col_names membership check for backward compat"
  - "Skipped 'Size' translation key (already exists at line 1658)"

patterns-established:
  - "Child-table graceful fallback: each sub-query independently fails to empty defaults"

requirements-completed: [FJMS-01, FJMS-03]

# Metrics
duration: 5min
completed: 2026-02-17
---

# Phase 37 Plan 02: Service Layer for Catalog Detail Summary

**Added get_catalog_source_counts() and get_catalog_detail() methods to FjmsService with v3.0.0 schema support, 46 passing tests, and 10 Hebrew translation keys for dialog labels**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-17T15:38:37Z
- **Completed:** 2026-02-17T15:43:31Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added 2 new service methods: `get_catalog_source_counts()` for batch button labels and `get_catalog_detail()` for structured dialog data
- Updated `get_catalog()` and `get_catalog_records()` to remove DescriptionEng/DescriptionHeb and add v3.0.0 columns (UnitCatalogRecId, NumFolio, NumColumn, NumRow, GenizahTitleOrgTitle, GenizahTitleEngTitle)
- Updated test fixture from old schema to v3.0.0 with 4 new child tables; all 46 tests pass (40 existing + 6 new)
- Added 10 Hebrew translation keys for catalog dialog labels

## Task Commits

Each task was committed atomically:

1. **Task 1: Add get_catalog_source_counts() and get_catalog_detail() to FjmsService** - `bd3137bc` (feat)
2. **Task 2: Update test fixtures and add tests for new methods, plus translation keys** - `9bb36dc8` (test)

## Files Created/Modified
- `shared/fjms_service.py` - Added get_catalog_source_counts(), get_catalog_detail(); updated get_catalog() and get_catalog_records() for v3.0.0 schema
- `tests/test_fjms_service.py` - Updated fixture to v3.0.0 schema with child tables; added 6 new tests
- `genizah_translations.py` - Added 10 translation keys (Catalog Records, Running Title, Free Description, etc.)

## Decisions Made
- Each child-table sub-query in get_catalog_detail() is independently wrapped in try/except, returning empty defaults if the table doesn't exist -- this provides backward compatibility with old sidecars
- New columns (UnitCatalogRecId, NumFolio, etc.) are accessed via `col_names` membership check rather than unconditional access, for same backward compatibility
- "Size" translation key was already present (line 1658), so it was not duplicated

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Service API complete for dialog consumption (Plans 03, 04)
- get_catalog_source_counts() ready for search card button labels
- get_catalog_detail() returns all 5 data sections: records, running_titles, sizes, fields, free_descriptions
- Translation keys ready for web and desktop dialog UI

---
*Phase: 37-fjms-catalog-descriptions*
*Completed: 2026-02-17*
