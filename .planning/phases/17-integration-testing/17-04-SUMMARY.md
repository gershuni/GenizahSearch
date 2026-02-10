---
phase: 17-integration-testing
plan: 04
subsystem: search-engine, ui
tags: [responsa, wildcard, sofit, explosion-guard, cascade, hebrew, regex, ui-notify]

# Dependency graph
requires:
  - phase: 14-responsa-core-engine
    provides: "Responsa search pipeline: parse, expand, build Tantivy/regex queries"
  - phase: 15-responsa-search-ui
    provides: "Web search UI with Responsa mode checkboxes"
provides:
  - "Sofit-aware wildcard regex: trailing sofit replaced with [sofit|normal] character class"
  - "Tantivy recall for suffix wildcards: sofit-converted stems added to query"
  - "Expanded explosion guard cascade: 6 downgrade steps (variants basic, variants off, JA off, plene off, suffixes off, prefixes off)"
  - "ValueError surfaced to user via ui.notify warning toast in web UI"
affects: [17-integration-testing, search-engine, web-ui]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Sentinel dict pattern for thread-to-async error communication"
    - "Character class regex for sofit/normal Hebrew letter alternation"

key-files:
  created: []
  modified:
    - genizah_core.py
    - web/pages/search.py
    - tests/test_responsa_core.py
    - tests/test_responsa_edge_cases.py

key-decisions:
  - "Sofit conversion uses character class [sofit|normal] rather than replacing sofit entirely, so both forms match"
  - "Tantivy recall adds sofit-converted stem with ^3 boost (lower than original's ^5)"
  - "Explosion guard cascade order: plene -> suffixes -> prefixes (highest multiplier disabled first)"
  - "ValueError surfaced via sentinel dict {'error': msg} from io_bound thread, handled in async context"

patterns-established:
  - "Character class [sofit|normal] for Hebrew wildcard regex across final-form letter boundaries"
  - "Sentinel dict pattern: io_bound thread returns {'error': msg}, async caller shows ui.notify"

# Metrics
duration: 4min
completed: 2026-02-10
---

# Phase 17 Plan 04: Wildcard Sofit Fix + Explosion Guard Cascade + ValueError UI Summary

**Sofit-aware wildcard regex with [sofit|normal] character class, 6-step explosion guard cascade, and ValueError surfaced to user via ui.notify**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-10T17:52:02Z
- **Completed:** 2026-02-10T17:56:03Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Suffix wildcard regex now matches across sofit/normal letter boundaries (e.g., `[chars][chars][chars]*` matches both standalone and continuation forms)
- Explosion guard cascade expanded from 3 to 6 downgrade steps, successfully handling `#%word#` queries that previously errored
- Web UI now shows meaningful error notification when explosion guard fires, instead of silent 0 results
- Updated 2 existing tests that expected ValueError to verify cascade-downgrade behavior instead

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix wildcard regex sofit conversion and expand explosion guard cascade** - `3cc1cb9` (fix)
2. **Task 2: Surface explosion guard ValueError in web UI** - `b04c787` (fix)

## Files Created/Modified
- `genizah_core.py` - _build_wildcard_regex: sofit-to-normal character class; build_tantivy_query: sofit-converted stem for recall; _apply_explosion_guard: 3 new cascade steps (plene, suffixes, prefixes)
- `web/pages/search.py` - run_core_search: separate ValueError catch returning sentinel dict; async handler: ui.notify with error message
- `tests/test_responsa_core.py` - Updated test_suffixes_counted_in_explosion_guard to expect cascade-downgrade instead of ValueError
- `tests/test_responsa_edge_cases.py` - Updated test_prefix_plus_suffix_exceeds_500_raises_error to expect cascade-downgrade instead of ValueError

## Decisions Made
- Sofit conversion uses character class `[sofit|normal]` rather than replacing sofit entirely, so both forms match in regex
- Tantivy recall for suffix wildcards adds sofit-converted stem with `^3` boost (lower than original's `^5` to avoid false ranking)
- Explosion guard cascade disables plene first (highest single multiplier ~5x), then suffixes (~25x), then prefixes (~24x)
- ValueError communication uses sentinel dict pattern because io_bound thread cannot call ui.notify directly

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Two existing tests (test_responsa_core.py and test_responsa_edge_cases.py) expected ValueError for prefix+suffix queries that now cascade-downgrade successfully. Updated both to verify the new cascade behavior instead.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All 3 diagnosed UAT bugs (suffix wildcard sofit, explosion guard cascade, silent ValueError) are now fixed
- 216 Responsa tests passing across all test files
- Ready for 17-05 (if exists) or UAT re-verification

## Self-Check: PASSED

All files exist, both commits verified in git log.

---
*Phase: 17-integration-testing*
*Completed: 2026-02-10*
