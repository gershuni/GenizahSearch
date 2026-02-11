---
phase: 19-search-normalization
plan: 03
subsystem: search
tags: [normalization, apostrophe, geresh, unicode, regex]

# Dependency graph
requires:
  - phase: 19-01
    provides: "strip_search_diacritics and COMBINING_DIACRITICALS_PATTERN"
  - phase: 19-02
    provides: "Mark-tolerant pattern integration in search pipeline"
provides:
  - "ASCII apostrophe and curly quote normalization in search queries"
  - "Mark-tolerant patterns that match text with any apostrophe variant"
affects: [search, normalization, UAT]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Unicode apostrophe variant normalization in search"]

key-files:
  created: []
  modified:
    - genizah_core.py
    - tests/test_search_normalization.py

key-decisions:
  - "Added 3 apostrophe variants (U+0027, U+2018, U+2019) to both stripping and mark-tolerant patterns for full coverage"
  - "No index rebuild needed -- query-side normalization sufficient because mark-tolerant regex patterns match all variants in source text"

patterns-established:
  - "Apostrophe variants grouped with geresh/gershayim in normalization patterns"

# Metrics
duration: 2min
completed: 2026-02-11
---

# Phase 19 Plan 03: Apostrophe Variant Normalization Summary

**Added ASCII apostrophe and curly quote variants to search normalization, closing the UAT gap where typing ' (keyboard apostrophe) returned 503 results instead of 11,006**

## Performance

- **Duration:** 2 min
- **Started:** 2026-02-11T08:22:35Z
- **Completed:** 2026-02-11T08:24:34Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments
- ASCII apostrophe (U+0027), left curly quote (U+2018), and right curly quote (U+2019) now stripped by strip_search_diacritics alongside Hebrew geresh/gershayim
- MARK_TOLERANT_INSERTER updated to allow optional apostrophe variants between regex tokens, so patterns match source text containing any apostrophe type
- 6 new tests added (25 total in test_search_normalization.py) covering all apostrophe variant normalization
- UAT Test #2 gap closed -- all 5 UAT tests now pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Add apostrophe variants to COMBINING_DIACRITICALS_PATTERN and MARK_TOLERANT_INSERTER** - `20b3303` (feat)
2. **Task 2: Add tests for apostrophe variant stripping and matching** - `0c812f7` (test)
3. **Task 3: Verify UAT gap closure with real search queries** - verification only, no commit

## Files Created/Modified
- `genizah_core.py` - Added U+0027, U+2018, U+2019 to COMBINING_DIACRITICALS_PATTERN and MARK_TOLERANT_INSERTER; updated docstrings
- `tests/test_search_normalization.py` - Added 6 new tests for apostrophe variant stripping and mark-tolerant matching

## Decisions Made
- Added 3 apostrophe variants (ASCII U+0027, left curly U+2018, right curly U+2019) to cover all common keyboard and typographic inputs
- No index rebuild required -- the fix works at query normalization time and mark-tolerant regex matching handles source text variants

## Deviations from Plan

None -- plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None -- no external service configuration required.

## Next Phase Readiness
- Phase 19 (Search Normalization) is fully complete with all 3 plans and all 5 UAT tests passing
- Ready for Phase 20 or any next milestone work

## Self-Check: PASSED

- FOUND: genizah_core.py
- FOUND: tests/test_search_normalization.py
- FOUND: .planning/phases/19-search-normalization/19-03-SUMMARY.md
- FOUND: commit 20b3303 (feat: apostrophe variants)
- FOUND: commit 0c812f7 (test: apostrophe variant tests)

---
*Phase: 19-search-normalization*
*Completed: 2026-02-11*
