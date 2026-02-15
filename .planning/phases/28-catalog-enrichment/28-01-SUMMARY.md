---
phase: 28-catalog-enrichment
plan: 01
subsystem: database
tags: [sqlite, sidecar, catalog, fjms, textual-frame, source-attribution]

# Dependency graph
requires:
  - phase: 25-data-infra
    provides: "SQLite sidecar export script and FjmsService"
provides:
  - "SourceName/SourceNameHeb columns in catalog table"
  - "get_catalog_records() multi-record retrieval with dedup"
  - "merge_catalog_records() display-ready structure builder"
  - "parse_textual_frame() [$Category$]: Content parser"
affects: [28-02, catalog-display, browse-page]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Multi-record catalog retrieval with empty/sentinel filtering"
    - "First-non-empty metadata merge with distinct TextualFrame collection"
    - "[$Category$]: Content regex parsing with @ prefix handling"

key-files:
  created: []
  modified:
    - "scripts/export_fist_enrichment.py"
    - "shared/fjms_service.py"

key-decisions:
  - "SourceName join via LEFT JOIN dbo_CodeSource ON sig.SourceId = cs.TeamCode"
  - "Sentinel CopyDate values (0, -99, -1) normalized to None in get_catalog_records"
  - "Deduplication by (textual_frame_eng, copy_date, title) tuple"
  - "Graceful fallback for old sidecars without SourceName columns"

patterns-established:
  - "get_catalog_records returns filtered/deduped list instead of fetchone"
  - "merge_catalog_records collects distinct TextualFrames with source attribution"

# Metrics
duration: 6min
completed: 2026-02-15
---

# Phase 28 Plan 01: Data Foundation Summary

**SourceName source-attribution columns added to sidecar export, plus multi-record catalog retrieval, merging, and TextualFrame parsing in FjmsService**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-15T06:51:05Z
- **Completed:** 2026-02-15T06:57:16Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Re-exported sidecar with SourceName/SourceNameHeb columns: 500,888 catalog rows, 466,083 with source data
- Created get_catalog_records() returning filtered, deduped list of all catalog records per manuscript
- Created merge_catalog_records() producing display-ready structure with distinct TextualFrame collection
- Created parse_textual_frame() extracting category and content from [$Category$]: Content notation

## Task Commits

Each task was committed atomically:

1. **Task 1: Add SourceName to export script and re-export sidecar** - `8868afb` (feat)
2. **Task 2: Add get_catalog_records, merge_catalog_records, and parse_textual_frame** - `98e790c` (feat)

## Files Created/Modified
- `scripts/export_fist_enrichment.py` - Added LEFT JOIN dbo_CodeSource, SourceName/SourceNameHeb columns, VERSION bump to 1.1.0
- `shared/fjms_service.py` - Added get_catalog_records(), merge_catalog_records(), parse_textual_frame(), import re

## Decisions Made
- Used LEFT JOIN dbo_CodeSource ON sig.SourceId = cs.TeamCode for source attribution (attaches to existing sig join)
- Sentinel CopyDate values (0, -99, -1 and their .0 variants) normalized to None during retrieval
- Deduplication key is (textual_frame_eng, copy_date, title) -- covers the dominant differentiator (TextualFrame)
- SourceName columns handled gracefully via row.keys() check for backward compat with old sidecars
- Existing get_catalog() method preserved unchanged for backward compatibility

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Handled locked sidecar DB during re-export**
- **Found during:** Task 1 (export script execution)
- **Issue:** Existing fjms_enrichment.db was locked by a running Python process (web/desktop app)
- **Fix:** Exported to fjms_enrichment_new.db, then used shutil.copy2() to overwrite the locked file
- **Files modified:** fist_data/fjms_enrichment.db (binary, not committed)
- **Verification:** PRAGMA table_info confirms new columns; 466,083 rows with SourceName data
- **Committed in:** N/A (binary file not in git)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Workaround for file lock was necessary; no scope change.

## Issues Encountered
- Windows file lock on fjms_enrichment.db prevented direct deletion by the export script. Resolved by exporting to a new filename and copying over the locked file with shutil.copy2().

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Sidecar has SourceName columns populated (466K rows)
- FjmsService has all three new methods ready for UI integration
- Plan 02 (catalog display in web and desktop) can proceed immediately

## Self-Check: PASSED

- [x] scripts/export_fist_enrichment.py exists
- [x] shared/fjms_service.py exists
- [x] fist_data/fjms_enrichment.db exists
- [x] .planning/phases/28-catalog-enrichment/28-01-SUMMARY.md exists
- [x] Commit 8868afb exists
- [x] Commit 98e790c exists

---
*Phase: 28-catalog-enrichment*
*Plan: 01*
*Completed: 2026-02-15*
