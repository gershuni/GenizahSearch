---
phase: 33-metadata-enrichment
plan: 01
subsystem: database
tags: [sqlite, fist, bibliography, catalog, sidecar, export]

# Dependency graph
requires:
  - phase: 25-fjms-data-export
    provides: "export_fist_enrichment.py script with domains/joins/catalog export pattern"
provides:
  - "bibliography table (542K denormalized rows) in fjms_enrichment.db"
  - "catalog_refs table (64K rows) mapping manuscripts to 80 scholarly catalogs"
  - "ref_catalogs, ref_titles, ref_authors lookup tables"
  - "Sidecar version 2.0.0 with 8 data tables"
affects: [33-02, 33-03, 33-04, fjms_service, browse_page]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Denormalized bibliography export with resolved author/title/mention types at export time"
    - "Graceful locked-DB handling (overwrite tables in-place when file is locked)"

key-files:
  created: []
  modified:
    - "scripts/export_fist_enrichment.py"

key-decisions:
  - "Denormalize all JOINs at export time: bibliography rows contain resolved RunningTitle, ArticleAuthorEng/Heb, MentionType, TranscriptionType, TranslationType"
  - "Use ABS(MentionTypeCode) to handle negative FIST code values"
  - "Column names CityEng and PublisherEng (not City/Publisher) in CODE_Title"
  - "Graceful locked-DB: skip delete, overwrite tables via DROP TABLE IF EXISTS"
  - "SELECT DISTINCT deduplicates join chain: 542K bibliography (not 733K), 64K catalog_refs (not 78K)"

patterns-established:
  - "Reference table export pattern: small CODE_* tables exported whole for display lookups"

# Metrics
duration: 8min
completed: 2026-02-16
---

# Phase 33 Plan 01: FIST Data Export Summary

**Denormalized bibliography (542K rows), catalog cross-references (64K rows), and 3 reference lookup tables exported to fjms_enrichment.db v2.0.0**

## Performance

- **Duration:** 8 min
- **Started:** 2026-02-16T07:15:24Z
- **Completed:** 2026-02-16T07:23:44Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Exported 542,487 denormalized bibliography rows with resolved author names, publication titles, mention/transcription/translation types covering 133,019 distinct manuscripts
- Exported 64,027 catalog cross-reference rows linking manuscripts to 80 scholarly catalogs (56,629 distinct manuscripts)
- Exported 3 reference lookup tables: ref_catalogs (80), ref_titles (4,309), ref_authors (2,969)
- Bumped sidecar version from 1.1.0 to 2.0.0; all existing tables (domains, joins, catalog, FTS5) preserved
- Final sidecar size: 245.3 MB

## Task Commits

Each task was committed atomically:

1. **Task 1: Add bibliography, catalog cross-ref, and reference table export functions** - `015bde36` (feat)

## Files Created/Modified
- `scripts/export_fist_enrichment.py` - Extended with 5 new export functions (export_bibliography, export_catalog_refs, export_ref_catalogs, export_ref_titles, export_ref_authors), stale file guard, locked-DB handling, version bump to 2.0.0

## Decisions Made
- **Denormalize at export time:** All 6+ table JOINs resolved during export. Bibliography rows contain display-ready RunningTitle, TitleYear, ArticleAuthorEng/Heb, MentionType, TranscriptionType, TranslationType. Service layer queries will be simple single-table lookups.
- **ABS(MentionTypeCode):** FIST stores negative code values (-1035) for some mention types. Using ABS() ensures they resolve correctly against CODE_FullCode positive ComputedCode values.
- **Column names corrected:** CODE_Title uses `CityEng` and `PublisherEng` (not `City`/`Publisher` as plan assumed from research). Fixed during execution.
- **Graceful locked-DB handling:** When web app holds the sidecar open, the script now skips the delete step and overwrites tables in-place using DROP TABLE IF EXISTS, allowing safe re-export without stopping the server.
- **Row count differences from estimates:** SELECT DISTINCT deduplication reduces bibliography from ~733K estimated to 542K actual, and catalog_refs from ~78K to 64K. The estimates were based on raw table counts before the full join chain deduplication. The actual counts reflect clean, unique rows.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed CODE_Title column names (City -> CityEng, Publisher -> PublisherEng)**
- **Found during:** Task 1 (export_ref_titles function)
- **Issue:** Plan specified `City` and `Publisher` columns but actual CODE_Title schema uses `CityEng` and `PublisherEng`
- **Fix:** Updated SELECT query to use correct column names
- **Files modified:** scripts/export_fist_enrichment.py
- **Verification:** Export completes successfully, 4,309 rows exported
- **Committed in:** 015bde36

**2. [Rule 3 - Blocking] Added graceful locked-DB handling**
- **Found during:** Task 1 (main() execution)
- **Issue:** Web app holds fjms_enrichment.db open; os.remove() raises PermissionError on Windows
- **Fix:** Wrapped delete in try/except PermissionError; tables overwrite in-place via DROP TABLE IF EXISTS
- **Files modified:** scripts/export_fist_enrichment.py
- **Verification:** Export completes fully with locked file, all tables created correctly
- **Committed in:** 015bde36

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- fjms_enrichment.db v2.0.0 ready with all tables needed for Plans 02-04
- Plan 02 can add FjmsService.get_bibliography() and get_catalog_refs() methods
- Plan 03/04 can build web and desktop UI sections

## Self-Check: PASSED

- scripts/export_fist_enrichment.py: FOUND
- fist_data/fjms_enrichment.db: FOUND
- 33-01-SUMMARY.md: FOUND
- Commit 015bde36: FOUND

---
*Phase: 33-metadata-enrichment*
*Completed: 2026-02-16*
