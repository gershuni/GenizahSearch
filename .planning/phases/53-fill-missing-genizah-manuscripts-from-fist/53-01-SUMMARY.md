---
phase: 53-fill-missing-genizah-manuscripts-from-fist
plan: 01
subsystem: data
tags: [csv, fist, libraries, shelfmark, normalization, sqlite]

# Dependency graph
requires:
  - phase: none
    provides: none
provides:
  - "38,673 new libraries.csv rows from FIST.db gap (total 255,615)"
  - "7 new library codes (Solomon, Reinach, Vatican, Mehlman, CentralArch, JCMainz, Corwin)"
  - "Yevr->EVR and Halper->Genizah shelfmark normalization aliases"
  - "scripts/generate_fist_gap_csv.py for repeatable CSV generation"
affects: [53-02-PLAN, search, browse, image-viewer]

# Tech tracking
tech-stack:
  added: []
  patterns: ["LibraryId->library_code mapping via LIBRARY_ID_MAP dict", "AlmaId-based deduplication with cross-library collision overrides"]

key-files:
  created:
    - scripts/generate_fist_gap_csv.py
    - fist_gap_rows.csv
    - fist_gap_manifest.txt
    - fist_gap_ambiguous_titles.txt
  modified:
    - libraries.csv
    - genizah_core.py
    - genizah_translations.py

key-decisions:
  - "9 new codes confirmed from gap set, not 12 as initially estimated (Halpern/Copenhagen/Chapira had 0 gap records)"
  - "Cross-library collision for 3 AlmaIds resolved via hardcoded override table"
  - "828 ambiguous titles (multi-value GenizahTitleOrgTitle) left empty, logged for human review"
  - "LibraryId 230 (Vernadsky) mapped to Harkavy code due to Harkavi-prefixed shelfmarks"
  - "LibraryId 168 (Strasbourg) and 183 (Italian university) mapped to closest existing codes"

patterns-established:
  - "FIST gap analysis: CAST(AlmaId AS TEXT) for integer/string comparison across databases"
  - "Pipe-separated call_numbers for AlmaIds with multiple FIST shelfmark variants"

requirements-completed: [GAP-01, GAP-02, GAP-06, GAP-07]

# Metrics
duration: 7min
completed: 2026-03-19
---

# Phase 53 Plan 01: CSV Gap Fill + Library Codes Summary

**38,673 FIST-only manuscripts merged into libraries.csv (216,942 -> 255,615) with 7 new library codes and Yevr/Halper shelfmark normalization aliases**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-19T03:09:00Z
- **Completed:** 2026-03-19T03:16:07Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Generated 38,673 gap CSV rows from FIST.db with full AlmaId deduplication (109 multi-row, 3 cross-library)
- Merged into libraries.csv: total now 255,615 records across 52 distinct library codes
- Added 7 new library codes to LIBRARY_CODES and LIBRARY_CODES_HE (1:1 parity)
- Added Yevr->EVR alias (fixes 15,594 RNL matches) and Halper->Genizah alias (fixes 534 CAJS matches) with Halpern guard
- Extracted 7,804 unambiguous titles from fjms_enrichment.db GenizahTitleOrgTitle
- All 17 existing shelfmark normalization tests pass; 440/441 full suite tests pass (1 pre-existing failure in puzzle image cache)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create CSV generation script and produce gap rows** - `86afd94b` (feat)
2. **Task 2: Register new library codes and add shelfmark normalization aliases** - `6ae5b31f` (feat)

## Files Created/Modified
- `scripts/generate_fist_gap_csv.py` - CSV generation script with --dry-run, --validate-only, manifest output
- `fist_gap_rows.csv` - 38,673 validated gap rows (standalone before merge)
- `fist_gap_manifest.txt` - One AlmaId per line for downstream testing
- `fist_gap_ambiguous_titles.txt` - 828 ambiguous title cases for human review
- `libraries.csv` - Master metadata file (216,942 + 38,673 = 255,615 rows)
- `genizah_core.py` - 7 new LIBRARY_CODES entries + Yevr/Halper normalize_shelfmark aliases
- `genizah_translations.py` - 7 matching LIBRARY_CODES_HE Hebrew entries

## Decisions Made
- Only 7 new library codes added (not 12 from plan) -- Halpern, Copenhagen, Chapira confirmed absent from gap set
- LibraryId 230 (Vernadsky National Library, Ukraine) mapped to existing 'Harkavy' code because all shelfmarks begin with "Harkavi"
- LibraryId 168 mapped to 'Strasbourg', LibraryId 183 (1 record) mapped to 'Turin' as closest match
- 828 AlmaIds with multiple distinct GenizahTitleOrgTitle values left untitled (logged to fist_gap_ambiguous_titles.txt)
- Integer/string type mismatch between FIST AlmaId (int) and CSV system_number (str) handled via CAST(AlmaId AS TEXT)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing LibraryId 103 in LIBRARY_ID_MAP**
- **Found during:** Task 1 (first --dry-run attempt)
- **Issue:** LibraryId 103 (State Library, Berlin) had 2 gap records but was missing from the mapping
- **Fix:** Added `103: 'SBB'` to LIBRARY_ID_MAP
- **Files modified:** scripts/generate_fist_gap_csv.py
- **Verification:** Second --dry-run succeeded with all 38,673 records
- **Committed in:** 86afd94b (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor mapping omission, no scope creep.

## Issues Encountered
None beyond the LibraryId 103 mapping gap (handled as deviation above).

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- libraries.csv has 255,615 rows ready for csv_bank loading
- 53-02-PLAN (metadata search guard fix + validation) can proceed
- All new library codes registered in both English and Hebrew
- Shelfmark normalization aliases active for Yevr and Halper patterns

---
## Self-Check: PASSED

All files verified present, all commits found in git log.

---
*Phase: 53-fill-missing-genizah-manuscripts-from-fist*
*Completed: 2026-03-19*
