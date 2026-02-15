---
phase: 29-data-infrastructure
plan: 01
subsystem: database
tags: [sqlite, sidecar, nli, cambridge, iiif, csv-import, shelfmark-normalization]

# Dependency graph
requires: []
provides:
  - "nli_data/nli_crossref.db sidecar with 815K NLI image records and 141K Cambridge IIIF manifests"
  - "scripts/import_nli_crossref.py idempotent import script"
  - "Indexed NLI_AlmaId, FGPImageNumberId, Shelfmark, normalized_shelfmark for downstream queries"
affects: [30-image-resolution, 31-metadata-display, 32-relationships, 33-service-layer, 34-ui-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [cudl-label-normalization, multi-source-sidecar, csv-dictreader-utf8sig]

key-files:
  created:
    - scripts/import_nli_crossref.py
    - nli_data/nli_crossref.db
  modified:
    - .gitignore

key-decisions:
  - "Separate sidecar file (nli_crossref.db) rather than adding to fjms_enrichment.db -- different provenance and update cycles"
  - "All 25 NLI CSV columns stored as TEXT -- no filtering per user decision"
  - "CUDL label normalization: strip MS- prefix, split by dash, strip leading zeros, rejoin with dots between numerics"

patterns-established:
  - "cudl_label_to_shelfmark(): CUDL label to human-readable shelfmark conversion"
  - "Multi-source sidecar pattern: separate .db files per data provenance"

# Metrics
duration: 4min
completed: 2026-02-15
---

# Phase 29 Plan 01: NLI Crossref + Cambridge IIIF Import Summary

**815K NLI image records and 141K Cambridge IIIF manifests imported into nli_crossref.db sidecar with normalized shelfmarks and indexed join keys**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-15T13:13:20Z
- **Completed:** 2026-02-15T13:17:12Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- Imported all 814,954 NLI crossreference rows (25 columns each, 253,103 distinct AlmaIds) into nli_images table
- Imported all 141,368 Cambridge IIIF manifest records with normalized shelfmarks into cambridge_manifests table
- Created indexes on NLI_AlmaId, FGPImageNumberId, Shelfmark (nli_images) and normalized_shelfmark, label (cambridge_manifests)
- Sidecar is 241 MB, idempotent (DROP+recreate), VACUUM-compacted

## Task Commits

Each task was committed atomically:

1. **Task 1: Create NLI crossref + Cambridge IIIF import script and generate sidecar database** - `5441d36` (feat)

## Files Created/Modified
- `scripts/import_nli_crossref.py` - Import script: reads CSV and JSON, creates SQLite sidecar with 3 tables, batched inserts, WAL mode, VACUUM
- `nli_data/nli_crossref.db` - SQLite sidecar (241 MB) with nli_images, cambridge_manifests, meta tables
- `.gitignore` - Added `nli_data/` exclusion

## Decisions Made
- Used separate sidecar file (`nli_crossref.db`) rather than adding to `fjms_enrichment.db` -- different data provenance (NLI vs FIST), independent update cycles
- All 25 NLI CSV columns stored as TEXT with no filtering -- per user decision to import everything
- CUDL label normalization follows 5-step process: strip MS-, split by dash, strip leading zeros from numerics, rejoin with dots between consecutive numeric parts and spaces otherwise, then apply normalize_shelfmark()

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- nli_crossref.db sidecar ready for service layer (Plan 02) to wrap with NliCrossrefService
- All indexes present for downstream image URL resolution (Phase 30) and metadata queries (Phase 31)
- Cambridge normalized_shelfmark enables join to libraries.csv via normalize_shelfmark()

## Self-Check: PASSED

- FOUND: scripts/import_nli_crossref.py
- FOUND: nli_data/nli_crossref.db
- FOUND: .planning/phases/29-data-infrastructure/29-01-SUMMARY.md
- FOUND: commit 5441d36
- FOUND: nli_data/ in .gitignore

---
*Phase: 29-data-infrastructure*
*Completed: 2026-02-15*
