---
phase: 16-tabular-query-builder
plan: 01
subsystem: search
tags: [responsa, parser, regex, tabular-builder, gap-notation, syntax-generation]

# Dependency graph
requires:
  - phase: 14-responsa-core-engine
    provides: "parse_responsa_query, build_regex_pattern Responsa branch, execute_search Responsa pipeline"
provides:
  - "extract_per_pair_gaps() for [N] gap token parsing"
  - "generate_tabular_syntax() for builder-to-syntax conversion"
  - "per_pair_gaps parameter in build_regex_pattern for per-pair distance control"
  - "parse_responsa_query [N] token skipping"
affects: [16-02 web-tabular-builder, 16-03 desktop-tabular-builder]

# Tech tracking
tech-stack:
  added: []
  patterns: ["per-pair gap regex joining with fallback to max_gap", "tabular state dict format: components with words/mods"]

key-files:
  created: []
  modified:
    - genizah_core.py
    - tests/test_responsa_core.py
    - tests/test_responsa_integration.py

key-decisions:
  - "generate_tabular_syntax accepts component dicts with {words: [{text, mods}], distances, scope} format"
  - "Modifier application order: plene -> prefix -> suffix -> wildcard_prefix -> wildcard_suffix"
  - "Distance 0 between components produces no [0] token (just space)"
  - "Negated words extracted into separate list, not embedded in syntax string"
  - "per_pair_gaps=None entries fall back to max_gap; full None list = uniform gap behavior"

patterns-established:
  - "Tabular builder state format: components list of {words: [{text: str, mods: dict}]}, distances list, scope string"
  - "Gap notation: [N] tokens between Responsa components for per-pair distance"

# Metrics
duration: 5min
completed: 2026-02-10
---

# Phase 16 Plan 01: Core Engine Extensions Summary

**[N] gap notation parser, per-pair regex gaps, and generate_tabular_syntax() for builder-to-syntax conversion with all 6 modifier types**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-10T08:47:36Z
- **Completed:** 2026-02-10T08:53:14Z
- **Tasks:** 2 (TDD: RED + GREEN)
- **Files modified:** 3

## Accomplishments
- `extract_per_pair_gaps()` parses [N] tokens from Responsa query strings, returning per-pair gap values
- `parse_responsa_query()` now skips [N] tokens so they don't become components
- `generate_tabular_syntax()` converts tabular builder state into valid Responsa syntax strings with all 6 modifiers, OR alternatives, empty slot filtering, negation extraction, and scope modes
- `build_regex_pattern()` uses per-pair gaps for different distances between component pairs, with None fallback to max_gap
- Bidirectional regex correctly reverses both parts and gap list
- `execute_search()` extracts and passes per_pair_gaps through the full pipeline
- 25 new tests, all 124 Responsa tests passing, zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: RED -- Failing tests for gap parsing, syntax generation, per-pair regex** - `f94da0f` (test)
2. **Task 2: GREEN -- Implement gap parsing, per-pair regex gaps, syntax generation** - `6c0605c` (feat)

_TDD plan: RED phase wrote 25 failing tests, GREEN phase implemented all functions to pass them._

## Files Created/Modified
- `genizah_core.py` - Added extract_per_pair_gaps(), generate_tabular_syntax(), _GAP_TOKEN_RE, per_pair_gaps in build_regex_pattern, wired into execute_search
- `tests/test_responsa_core.py` - Added TestGapNotation (6 tests), TestGenerateTabularSyntax (15 tests)
- `tests/test_responsa_integration.py` - Added TestPerPairGapRegex (4 tests)

## Decisions Made
- `generate_tabular_syntax` takes a flat dict format (components with words/mods) rather than ResponsaComponent dataclasses, since it's called from UI code that doesn't need to import the dataclass
- Modifier application order in syntax generation: plene -> prefix -> suffix -> wildcard_prefix -> wildcard_suffix (matches the parsing order)
- Distance 0 produces no bracket token (just space between components) for cleaner output
- Negated words are extracted into a separate return value, not embedded in the syntax string

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Core engine extensions complete and tested -- ready for web tabular builder (16-02) and desktop tabular builder (16-03)
- `generate_tabular_syntax()` is importable from genizah_core for both web and desktop UIs
- `extract_per_pair_gaps()` automatically wired into execute_search pipeline -- no additional wiring needed by UI plans
- Tabular builder state format documented: `{words: [{text: str, mods: dict}]}` per component

---
## Self-Check: PASSED

All files found, all commits verified.

---
*Phase: 16-tabular-query-builder*
*Completed: 2026-02-10*
