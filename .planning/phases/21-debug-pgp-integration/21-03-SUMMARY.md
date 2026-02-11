---
phase: 21-debug-pgp-integration
plan: 03
subsystem: display-pipeline
tags: [structured-sections, canvas-lookup, recto-verso, display-pipeline, integration-tests]

# Dependency graph
requires:
  - phase: 21-debug-pgp-integration
    plan: 01
    provides: parse_html_sections function and fixed regex in parse_transcription_sections
  - phase: 21-debug-pgp-integration
    plan: 02
    provides: sections JSONB column populated on 9,068 document_sources records
provides:
  - get_section_for_page with optional sections parameter for canvas-based lookup
  - All 5 source-level consumer call sites wired to pass structured sections
  - 8 integration tests verifying structured sections display path
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns: [canvas_num lookup with regex fallback, source.get('sections') consumer pattern]

key-files:
  created: []
  modified:
    - shared/document_service.py
    - web/pages/browse.py
    - web/pages/search.py
    - gui_threads.py
    - tests/test_shared_service.py

key-decisions:
  - "Renamed local variable from 'sections' to 'parsed' in regex fallback path to avoid shadowing the new parameter"
  - "Empty list sections=[] treated same as None (falsy) for regex fallback"

patterns-established:
  - "Canvas-based section lookup: when source has structured sections JSONB, use canvas_num matching instead of regex parsing"
  - "Consumer pattern: source.get('sections') passed as third arg to get_section_for_page at source-level call sites"

# Metrics
duration: 3min
completed: 2026-02-11
---

# Phase 21 Plan 03: Display Pipeline Wiring Summary

**Canvas-based section lookup wired into both web and desktop display pipelines with 5 consumer sites and 8 integration tests**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-11T12:12:56Z
- **Completed:** 2026-02-11T12:16:17Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- get_section_for_page gains optional `sections` parameter for canvas_num-based lookup, with regex fallback
- All 5 source-level consumer call sites across 3 files (browse.py, search.py, gui_threads.py) pass source.get('sections')
- 3 document-level fallback call sites correctly left unchanged (documents table has no sections column)
- 8 integration tests verify structured sections priority, regex fallback, edge cases, and backward compatibility
- Full test suite green: 446 passed, 5 skipped, 0 failures

## Task Commits

Each task was committed atomically:

1. **Task 1: Update get_section_for_page + wire all consumers** - `d63faad` (feat)
2. **Task 2: Integration tests for structured sections display path** - `051825a` (test)

## Files Created/Modified
- `shared/document_service.py` - Added sections parameter to get_section_for_page with canvas_num lookup path
- `web/pages/browse.py` - 2 source-level call sites pass source.get('sections')
- `web/pages/search.py` - 1 source-level call site passes src.get('sections')
- `gui_threads.py` - 2 source-level call sites in PGPSourceWorker pass source.get('sections')
- `tests/test_shared_service.py` - Added TestStructuredSectionsIntegration with 8 tests

## Decisions Made
- Renamed local variable from `sections` to `parsed` in the regex fallback path of get_section_for_page to avoid shadowing the new `sections` parameter name
- Empty list `sections=[]` treated as falsy (same as None), falling through to regex path -- consistent with Python truthiness

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 21 complete: HTML parser (Plan 01) + import script (Plan 02) + display pipeline (Plan 03) all wired
- Both web and desktop apps will now use canvas-based section lookup for the 9,068 sources with structured sections
- Sources without structured sections continue to use regex fallback transparently

## Self-Check: PASSED

- shared/document_service.py: FOUND
- web/pages/browse.py: FOUND
- web/pages/search.py: FOUND
- gui_threads.py: FOUND
- tests/test_shared_service.py: FOUND
- Commit d63faad: FOUND
- Commit 051825a: FOUND
- get_section_for_page signature has sections parameter: VERIFIED
- 5 consumer call sites pass source.get('sections'): VERIFIED
- 3 document-level fallback sites unchanged: VERIFIED
- TestStructuredSectionsIntegration: 8 tests pass

---
*Phase: 21-debug-pgp-integration*
*Completed: 2026-02-11*
