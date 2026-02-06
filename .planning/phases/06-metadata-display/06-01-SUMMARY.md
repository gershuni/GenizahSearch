---
phase: 06-metadata-display
plan: 01
subsystem: database
tags: [supabase, migration, import, metadata, languages, dates]

requires:
  - phase: 02-pgp-data-import
    provides: "Import script and documents table schema"
provides:
  - "4 new metadata columns in documents table: languages_primary, languages_secondary, inferred_date_standard, inferred_date_rationale"
  - "Updated import script reading all metadata columns from CSV"
affects: [06-metadata-display plans 02 and 03]

tech-stack:
  added: []
  patterns: ["ALTER TABLE IF NOT EXISTS for safe re-runnable migrations"]

key-files:
  created:
    - migrations/add_pgp_metadata_columns.sql
  modified:
    - scripts/import_pgp_documents.py

key-decisions:
  - "Added dotenv loading to import script for convenience (consistent with rest of project)"
  - "Empty strings converted to None for clean NULL storage in Supabase"

duration: 5min
completed: 2026-02-06
---

# Phase 6 Plan 01: Add Missing Metadata Columns Summary

**ALTER TABLE migration adding languages_primary, languages_secondary, inferred_date_standard, inferred_date_rationale to documents table with updated import script**

## Performance

- **Duration:** 5 min
- **Tasks:** 2 (1 auto + 1 human-action checkpoint)
- **Files modified:** 2

## Accomplishments
- Created migration SQL with 4 ALTER TABLE IF NOT EXISTS statements and column comments
- Updated import script to read and include 4 new fields from documents.csv
- Added dotenv loading for convenience (.env file support)
- Successfully imported 7,090 documents with new metadata columns

## Data Coverage After Import

| Column | Records | Percentage |
|--------|---------|------------|
| languages_primary | ~6,149 | 86.7% |
| languages_secondary | ~825 | 11.6% |
| inferred_date_standard | ~428 | 6.0% |
| inferred_date_rationale | ~428 | 6.0% |

## Task Commits

1. **Task 1: Create migration SQL and update import script** - `f97c40c` (feat)
2. **Task 2: Run migration and re-import** - `8b04d41` (fix - dotenv addition during checkpoint)

## Files Created/Modified
- `migrations/add_pgp_metadata_columns.sql` - ALTER TABLE migration adding 4 columns with comments
- `scripts/import_pgp_documents.py` - Updated to read new CSV columns + dotenv loading

## Decisions Made
- Added dotenv loading to import script (consistent with other project scripts)
- Empty string values converted to None for clean NULL storage

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added dotenv loading to import script**
- **Found during:** Task 2 (human-action checkpoint)
- **Issue:** Script couldn't read SUPABASE_SERVICE_KEY from .env file, only from environment
- **Fix:** Added `from dotenv import load_dotenv; load_dotenv()` at top of script
- **Files modified:** scripts/import_pgp_documents.py
- **Verification:** Import ran successfully after fix
- **Committed in:** 8b04d41

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor convenience fix, consistent with project patterns.

## Issues Encountered
None

## Next Phase Readiness
- All 4 metadata columns populated in documents table
- get_document_for_fragment() already returns new columns (uses SELECT *)
- Ready for plan 06-02 (metadata display UI) and 06-03 (tag search)

---
*Phase: 06-metadata-display*
*Completed: 2026-02-06*
