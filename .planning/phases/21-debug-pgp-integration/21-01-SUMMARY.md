---
phase: 21-debug-pgp-integration
plan: 01
subsystem: parsing
tags: [html-parser, regex, pgp-text, canvas-mapping, tdd]

# Dependency graph
requires:
  - phase: 08-shared-service-layer
    provides: shared/document_service.py structure and web shim pattern
provides:
  - parse_html_sections function for parsing PGP HTML into structured canvas sections
  - Fixed regex in parse_transcription_sections handling all marker variants
  - 28 new tests (14 HTML parser + 14 regex) all passing
affects: [21-02-import-script, 21-03-display-pipeline]

# Tech tracking
tech-stack:
  added: [html.parser.HTMLParser (stdlib)]
  patterns: [PGPHTMLParser stateful parser, structured alternation regex]

key-files:
  created: []
  modified:
    - shared/document_service.py
    - tests/test_shared_service.py
    - web/document_service.py

key-decisions:
  - "Used explicit [Rr]ecto/[Vv]erso casing instead of re.IGNORECASE to avoid false positives on content lines"
  - "Used structured regex alternation (period|parens|dash|word) instead of catch-all [^\\n]{0,60} to prevent matching content lines as markers"
  - "HTML entity &hellip; decodes to Unicode ellipsis U+2026, not three ASCII dots"

patterns-established:
  - "PGPHTMLParser: stateful HTMLParser subclass with div-depth tracking for nested canvas divs"
  - "Canvas subsection merging: multiple h3+ol pairs within one data-canvas div merged into single section entry with subsections list"

# Metrics
duration: 6min
completed: 2026-02-11
---

# Phase 21 Plan 01: HTML Parser and Regex Fix Summary

**PGP HTML canvas parser using stdlib HTMLParser with 14 tests, plus fixed section regex handling all 712 missed marker variants**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-11T11:26:54Z
- **Completed:** 2026-02-11T11:33:10Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- parse_html_sections correctly extracts per-canvas sections from PGP HTML where h3 elements are INSIDE data-canvas divs
- Multiple h3+ol sub-sections within a single data-canvas div merged into one canvas entry with subsections list
- Fixed regex handles all known marker variants: bare, period, period+address, parenthetical, space-modifier, upside-down, case variants
- 28 new tests all passing, full suite green (438 passed, 5 skipped, 0 failures)

## Task Commits

Each task was committed atomically (TDD: RED then GREEN):

1. **Task 1: TDD parse_html_sections** (RED) - `2e6933b` (test)
2. **Task 1: TDD parse_html_sections** (GREEN) - `d0733bc` (feat)
3. **Task 2: TDD fix regex fallback** (RED) - `bde9158` (test)
4. **Task 2: TDD fix regex fallback** (GREEN) - `a8f2886` (fix)

## Files Created/Modified
- `shared/document_service.py` - Added PGPHTMLParser class, parse_html_sections function, fixed section_pattern regex
- `tests/test_shared_service.py` - Added TestParseHtmlSections (14 tests) and TestParseTranscriptionSections (14 tests)
- `web/document_service.py` - Added parse_html_sections to shim imports

## Decisions Made
- Used explicit `[Rr]ecto|[Vv]erso` casing instead of `re.IGNORECASE` to avoid false positives where content lines start with "recto"/"verso" in lowercase. The catch-all `[^\n]{0,60}` approach from the plan matched content lines like "recto text" as markers.
- Used structured regex alternation (period patterns | parenthetical | dash/comma | capitalized qualifier) instead of a single broad character class. This is more precise about what constitutes a valid marker modifier.
- HTML entity `&hellip;` decodes to Unicode horizontal ellipsis (U+2026), not three ASCII dots. Test expectation adjusted accordingly.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed regex approach from plan that caused false positives**
- **Found during:** Task 2 (GREEN phase)
- **Issue:** The plan specified `r'^(Recto|Verso)\b[^\n]{0,60}\n'` with IGNORECASE, which matched content lines like "recto text\n" as section markers because "recto" matches case-insensitively and " text" is within the 60-char limit.
- **Fix:** Used explicit `[Rr]ecto|[Vv]erso` casing (not IGNORECASE) with structured alternation for valid modifier patterns (period, parenthetical, dash, capitalized qualifier word).
- **Files modified:** shared/document_service.py
- **Verification:** All 14 regex tests pass including false positive prevention tests
- **Committed in:** a8f2886 (Task 2 GREEN commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Regex pattern improved from plan specification. Same coverage of all marker variants but without false positives on content lines.

## Issues Encountered
None beyond the regex deviation described above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- parse_html_sections ready for import script (Plan 02) to call with pgp-text HTML files
- Fixed regex ready as fallback for sources not covered by pgp-text
- web/document_service.py shim already exports parse_html_sections

## Self-Check: PASSED

- All 4 files exist (shared/document_service.py, tests/test_shared_service.py, web/document_service.py, SUMMARY.md)
- All 4 commits found (2e6933b, d0733bc, bde9158, a8f2886)
- parse_html_sections importable from shared.document_service and web.document_service
- parse_transcription_sections importable from shared.document_service

---
*Phase: 21-debug-pgp-integration*
*Completed: 2026-02-11*
