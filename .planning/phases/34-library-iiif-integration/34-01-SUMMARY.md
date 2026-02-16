---
phase: 34-library-iiif-integration
plan: 01
subsystem: data-import
tags: [luna, manchester, iiif, sqlite, sidecar, bulk-import]

# Dependency graph
requires:
  - phase: 29-nli-crossref-sidecar
    provides: "nli_crossref.db sidecar with nli_images table containing Manchester ImageSourceName values"
provides:
  - "manchester_luna table mapping JRL filenames to LUNA internal IDs"
  - "scripts/import_manchester_luna.py bulk import CLI tool"
  - "Sidecar version bumped to 1.1.0 with Manchester source metadata"
affects: [34-02, 34-03, 34-04, 34-05]

# Tech tracking
tech-stack:
  added: [requests-retry, urllib3-retry]
  patterns: [luna-api-pagination, jrl-filename-extraction, sidecar-table-extension]

key-files:
  created:
    - scripts/import_manchester_luna.py
  modified:
    - nli_data/nli_crossref.db (manchester_luna table + meta version bump)

key-decisions:
  - "LUNA identity field used as luna_id -- id and identity are identical in API response"
  - "JRL filename extracted from urlSize0 mediafile path, lowercased to match crossref ImageSourceName"
  - "Batch size 500 + 0.3s delay for production import -- completes in ~90 seconds"
  - "INSERT OR REPLACE for idempotent writes with checkpoint saves every 1000 items"

patterns-established:
  - "LUNA API pagination: fetchMediaSearch with lc=ManchesterDev~95~2, bs/os params, stop on 3 consecutive empty responses"
  - "Sidecar extension pattern: add new table to existing nli_crossref.db rather than creating separate DB"

# Metrics
duration: 7min
completed: 2026-02-16
---

# Phase 34 Plan 01: Manchester LUNA Bulk Import Summary

**Bulk LUNA API pagination script importing 27,940 Manchester JRL-to-luna_id mappings into sidecar with 83.9% crossref match rate**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-16T03:40:43Z
- **Completed:** 2026-02-16T03:47:43Z
- **Tasks:** 2
- **Files modified:** 1 (script) + 1 (sidecar DB, not tracked in git)

## Accomplishments
- Created import script that paginates through all 27,944 LUNA items in ~90 seconds
- Extracted JRL filenames from urlSize0 URLs and mapped to crossref ImageSourceName values
- Achieved 83.9% manuscript match rate (11,321 of 13,496 Manchester manuscripts matched)
- Verified LUNA detail URLs and IIIF manifest URLs work for sample luna_ids
- Sidecar version bumped to 1.1.0 with Manchester source metadata

## Task Commits

Each task was committed atomically:

1. **Task 1: Create Manchester LUNA bulk import script** - `a63a0516` (feat)
2. **Task 2: Run Manchester LUNA import and verify sidecar data** - No code changes (data-only operation on gitignored sidecar DB)

## Files Created/Modified
- `scripts/import_manchester_luna.py` - LUNA API pagination, JRL extraction, sidecar import with CLI interface
- `nli_data/nli_crossref.db` - Added manchester_luna table (27,940 rows), meta version 1.1.0

## Decisions Made
- LUNA API `id` and `identity` fields are identical; used `id` as primary luna_id source
- JRL filenames extracted by splitting urlSize0 mediafile path on `/`, taking last segment, stripping `.jpg`, lowercasing
- Production import used batch-size 500 and 0.3s delay (vs plan's default 100/0.5s) to complete in ~90 seconds
- Stop condition: 3 consecutive empty API responses (not just 1, to handle potential glitches)
- 4 LUNA items out of 27,944 had no extractable JRL filename and were skipped (27,940 stored)

## Deviations from Plan

None - plan executed exactly as written.

## Import Statistics

| Metric | Value |
|--------|-------|
| Total LUNA items fetched | 27,944 |
| Items with JRL filename | 27,940 |
| Crossref Manchester manuscripts | 13,496 |
| Matched to LUNA | 11,321 (83.9%) |
| Crossref Manchester images | 29,931 |
| Matched to LUNA (images) | 25,729 (86.0%) |

## Issues Encountered
None - LUNA API was responsive and stable throughout the full import.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- manchester_luna table is populated and ready for querying by downstream plans
- luna_id values verified to produce working LUNA detail and IIIF manifest URLs
- Plan 02 (Manchester IIIF service integration) can now use these luna_ids to construct IIIF endpoints

## Self-Check: PASSED

- FOUND: scripts/import_manchester_luna.py
- FOUND: commit a63a0516
- FOUND: 34-01-SUMMARY.md
- FOUND: manchester_luna table (27,940 rows)

---
*Phase: 34-library-iiif-integration*
*Completed: 2026-02-16*
