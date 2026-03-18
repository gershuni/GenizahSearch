---
phase: quick-260318-tkj
plan: 01
subsystem: image-resolution
tags: [cudl, iiif, mosseri, cambridge, shelfmark-normalization]

# Dependency graph
requires:
  - phase: 29-34 (v5.9.0)
    provides: nli_crossref.db cambridge_manifests table, get_cambridge_manifest_by_label()
provides:
  - construct_mosseri_cudl_label() function for Mosseri shelfmark to CUDL label conversion
  - Mosseri CUDL fallback in enrich_metadata image resolution chain
  - call_numbers_raw stored in csv_bank for Mosseri records
affects: [enrich_metadata, browse, puzzle, ResultDialog]

# Tech tracking
tech-stack:
  added: []
  patterns: [collection-specific CUDL label construction with whitelist validation]

key-files:
  created:
    - tests/test_mosseri_cudl.py
  modified:
    - genizah_core.py
    - docs/OPEN_ISSUES.md

key-decisions:
  - "Whitelist-based series validation instead of pure regex for Roman numeral disambiguation"
  - "Iterate all call_number variants per Mosseri record for 98.3% coverage vs 61% from shortest-only"
  - "Use get_cambridge_manifest_by_label() crossref lookup rather than direct URL construction for verified manifests"

patterns-established:
  - "Collection-specific CUDL label fallback: lib_code check -> construct label -> crossref lookup"

requirements-completed: [CUDL-01, CUDL-02, CUDL-03]

# Metrics
duration: 17min
completed: 2026-03-18
---

# Quick Task 260318-tkj: Add CUDL as Image Source for Mosseri Collection - Summary

**Mosseri CUDL image resolution via shelfmark-to-label construction fallback in enrich_metadata, covering 3,141/3,194 (98.3%) Mosseri records**

## Performance

- **Duration:** 17 min
- **Started:** 2026-03-18T19:32:08Z
- **Completed:** 2026-03-18T19:49:02Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added `construct_mosseri_cudl_label()` that converts 6 Mosseri shelfmark patterns to CUDL manifest labels (MS-MOSSERI-{SERIES}-{PADDED_NUM})
- Wired Mosseri CUDL fallback into enrich_metadata between crossref Cambridge lookup and Manchester block
- Stored all call_number variants for Mosseri records in csv_bank for multi-variant iteration
- 17 unit tests covering all pattern variants, edge cases, and rejection scenarios
- Marked Mosseri CUDL open issue as fixed in docs/OPEN_ISSUES.md

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: Failing tests for construct_mosseri_cudl_label** - `eed866bb` (test)
2. **Task 1 GREEN: Implement construct_mosseri_cudl_label()** - `7e688382` (feat)
3. **Task 2: Wire Mosseri CUDL fallback + csv_bank + open issue** - `398e44f7` (feat)

## Files Created/Modified
- `genizah_core.py` - Added `construct_mosseri_cudl_label()` function (after `normalize_shelfmark()`), Mosseri CUDL fallback block in `enrich_metadata()`, `call_numbers_raw` field in csv_bank for Mosseri records
- `tests/test_mosseri_cudl.py` - 17 unit tests for CUDL label construction (basic patterns, long-form prefix, rejection cases, edge cases)
- `docs/OPEN_ISSUES.md` - Marked Mosseri CUDL issue as fixed (2026-03-18)

## Decisions Made
- Used whitelist-based series validation (`_MOSSERI_CUDL_SERIES` set) instead of purely regex-based Roman numeral matching, because single letters like "L" are ambiguous (valid Roman numeral but used as 2nd-series designator in Mosseri)
- Stored `call_numbers_raw` only for Mosseri records (3,194 lists vs 217K None values) to minimize memory overhead while enabling multi-variant iteration
- Used `get_cambridge_manifest_by_label()` crossref lookup rather than direct URL construction, ensuring only verified manifests are used (avoids 404s for the 1.7% without CUDL entries)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Two pre-existing test failures in `test_puzzle_model.py` (default `join_type` changed from 'uncertain' to 'physical') and `test_puzzle_image_service.py` (cache invalidation) -- both unrelated to this task, documented as out-of-scope

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Mosseri manuscripts now resolve CUDL high-res images in browse, ResultDialog, and puzzle across both web and desktop
- No further action needed; the fix flows through all consumers of `enrich_metadata()`

## Self-Check: PASSED

All files exist, all commits verified.

---
*Quick Task: 260318-tkj*
*Completed: 2026-03-18*
