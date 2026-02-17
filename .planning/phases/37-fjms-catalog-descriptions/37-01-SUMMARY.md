---
phase: 37-fjms-catalog-descriptions
plan: 01
subsystem: database
tags: [sqlite, fts5, fist, fjms, sidecar, export]

# Dependency graph
requires:
  - phase: 25-data-infrastructure
    provides: "Original fjms_enrichment.db export script and sidecar schema"
  - phase: 28-catalog-enrichment
    provides: "catalog_refs table and reference lookups in sidecar"
provides:
  - "4 new sidecar tables: catalog_running_titles (317K), catalog_sizes (178K), catalog_fields (1.3M), catalog_free_desc (303K)"
  - "Catalog v2 schema with UnitCatalogRecId, NumFolio, NumColumn, NumRow, GenizahTitle columns"
  - "Contentless FTS5 index spanning catalog + running titles + free descriptions (226K entries)"
  - "Sidecar version 3.0.0"
affects: [37-02, 37-03, 37-04, fjms-service]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Contentless FTS5 aggregation across multiple tables via per-AlmaId GROUP_CONCAT"
    - "Standard batch-insert export pattern with JOIN chain through InventoryAlma->Inventory->InventorySignature->Signature->UnitCatalogRec"

key-files:
  created: []
  modified:
    - "scripts/export_fist_enrichment.py"
    - "fist_data/fjms_enrichment.db"

key-decisions:
  - "Contentless FTS5 (content='') instead of content-synced since RunningTitle/FreeDescription come from separate tables"
  - "4 normalized tables (not flattened into catalog) for catalog_running_titles, catalog_sizes, catalog_fields, catalog_free_desc"
  - "catalog_fields resolves category names via CODE_FullCode -> CODE_FCDTable JOIN chain"
  - "catalog_free_desc joins via SignatureId (not UnitCatalogRecId) per FIST schema design"

patterns-established:
  - "Contentless FTS5 aggregation: one row per AlmaId with GROUP_CONCAT from child tables"
  - "Multi-table export: new tables follow same batch-insert pattern as export_domains()"

requirements-completed: [FJMS-01]

# Metrics
duration: 22min
completed: 2026-02-17
---

# Phase 37 Plan 01: FIST Enrichment Export v3 Summary

**Extended fjms_enrichment.db with 4 new catalog tables (2.1M rows), v2 catalog schema with GenizahTitle/NumFolio/UnitCatalogRecId, and contentless FTS5 index spanning RunningTitle + FreeDescription**

## Performance

- **Duration:** 22 min
- **Started:** 2026-02-17T15:13:23Z
- **Completed:** 2026-02-17T15:35:00Z
- **Tasks:** 2
- **Files modified:** 1 (script) + 1 (sidecar regenerated)

## Accomplishments
- Added 4 new sidecar tables totaling 2,114,884 rows (317K running titles, 178K sizes, 1.3M fields, 303K free descriptions)
- Extended catalog table from 12 to 16 columns: added UnitCatalogRecId, NumFolio, NumColumn, NumRow, GenizahTitleOrgTitle, GenizahTitleEngTitle; removed empty DescriptionEng/DescriptionHeb
- Rebuilt FTS5 as contentless aggregated index (226K entries) spanning catalog text + running titles + free descriptions
- Bumped sidecar version to 3.0.0; final file size 592 MB

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend catalog table schema and add 4 new export functions** - `96dabb18` (feat)
2. **Task 2: Rebuild FTS5 index with RunningTitle and FreeDescription content** - `ccdd9dbd` (feat)

## Files Created/Modified
- `scripts/export_fist_enrichment.py` - Extended with 4 new export functions, v2 catalog schema, contentless FTS5
- `fist_data/fjms_enrichment.db` - Regenerated sidecar with all new tables (592 MB)

## Decisions Made
- Used contentless FTS5 (`content=''`) because the index aggregates data from 3 separate tables (catalog, catalog_running_titles, catalog_free_desc) -- content-synced approach cannot span multiple source tables
- catalog_free_desc joins through SignatureId rather than UnitCatalogRecId, following the FIST schema where free descriptions are signature-level not catalog-record-level
- catalog_fields resolves coded values through a two-hop JOIN: dbo_CatalogMultiField -> CODE_FullCode -> CODE_FCDTable to get human-readable FieldCategory names
- Added UnitCatalogRecId index on catalog table for efficient child-table lookups

## Deviations from Plan

None - plan executed exactly as written.

## Row Count Comparison

| Table | Plan Estimate | Actual | Notes |
|-------|--------------|--------|-------|
| catalog_running_titles | ~235K | 317,412 | Higher due to DISTINCT across full JOIN chain |
| catalog_sizes | ~161K | 178,579 | Slightly higher with AlmaId expansion |
| catalog_fields | ~1.1M | 1,315,501 | Consistent with estimate |
| catalog_free_desc | ~190K | 303,392 | Higher -- multiple descriptions per signature |
| catalog | ~500K | 730,624 | More rows with GenizahTitle LEFT JOIN expansion |
| catalog_fts | ~226K | 226,456 | Distinct AlmaIds in catalog |

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Sidecar data foundation complete for catalog records dialog (Plans 02-04)
- All 4 new tables indexed and ready for service layer consumption
- FTS5 index ready for full-text search across catalog descriptions

---
*Phase: 37-fjms-catalog-descriptions*
*Completed: 2026-02-17*
