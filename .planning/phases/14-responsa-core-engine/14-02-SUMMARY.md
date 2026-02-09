---
phase: 14-responsa-core-engine
plan: 02
subsystem: search
tags: [responsa, search-pipeline, tantivy, regex, flex-spacing, bidirectional, wildcard, tdd]

# Dependency graph
requires:
  - "14-01: ResponsaComponent, parse_responsa_query, expand_grammatical_prefixes/suffixes, expand_plene_defective, expand_judeo_arabic, _apply_explosion_guard"
provides:
  - "build_tantivy_query Responsa branch: OR groups with ^5 exact boosting, AND-joined across components"
  - "build_regex_pattern Responsa branch: suffix/prefix/character wildcards, flex spacing, bidirectional, inline alternation"
  - "parse_query_syntax responsa_mode bypass: all prefix shortcuts disabled when Responsa active"
  - "execute_search Responsa pipeline: parse -> plene -> prefixes -> suffixes -> JA -> variants -> guard -> build queries -> hit processing"
  - "Helper functions: _make_flex_spacing_pattern, _build_wildcard_regex, _expand_inline_alternation"
affects: [phase-15-search-ui, phase-16-tabular-builder, phase-17-integration-testing]

# Tech tracking
tech-stack:
  added: []
  patterns: [early-return branch pattern for mode dispatch, module-level regex helpers, component-dict intermediate representation]

key-files:
  created: [tests/test_responsa_integration.py]
  modified: [genizah_core.py]

key-decisions:
  - "Expansion order: plene/defective -> prefixes -> suffixes -> JA -> variants (plene first to generate base variants before morphological expansion)"
  - "Component dicts as intermediate representation between ResponsaComponent objects and query builders"
  - "Flex spacing patterns stored in component_dicts, added as alternation alternatives (not replacing terms)"
  - "Explosion guard runs before material expansion (uses estimation), warning propagated to first result"
  - "Module-restore fixture in tests to handle test_missing_tantivy.py module cache corruption"

patterns-established:
  - "Early-return Responsa branch in build_tantivy_query/build_regex_pattern/execute_search: existing path completely untouched"
  - "Component dict keys: tantivy_terms, regex_terms, original_words, wildcard, wildcard_pattern, flex_patterns, inline_pattern"
  - "responsa_options dict keys: responsa_mode, variants, ja, flex_spacing, bidirectional, variant_mode"

# Metrics
duration: 10min
completed: 2026-02-09
---

# Phase 14 Plan 02: Responsa Pipeline Integration Summary

**Responsa search pipeline wired into existing two-phase engine: Tantivy OR groups with ^5 boosting, regex wildcards/flex-spacing/bidirectional, parse_query_syntax bypass, and full expansion chain (plene -> prefixes -> suffixes -> JA -> variants -> explosion guard)**

## Performance

- **Duration:** 10 min
- **Started:** 2026-02-09T20:11:02Z
- **Completed:** 2026-02-09T20:20:34Z
- **Tasks:** 1 TDD cycle (RED + GREEN, no refactor needed)
- **Files modified:** 2

## Accomplishments

- build_tantivy_query Responsa branch producing OR groups with ^5 exact boost, ^3 for different-length variants, AND-joined across components
- build_regex_pattern Responsa branch handling 5 wildcard types (suffix, prefix, character-pattern, inline-alternation-single, inline-alternation-multi), flex spacing (\s* on original terms only), bidirectional (forward|backward), flex separator (* instead of + at gap=0)
- parse_query_syntax accepts responsa_mode=False parameter; when True, returns (None, query) immediately, preventing # from being interpreted as Shelfmark
- execute_search accepts responsa_options dict; when responsa_mode=True, routes through full Responsa pipeline with 6-step expansion chain and explosion guard warning propagation
- 3 module-level helper functions: _make_flex_spacing_pattern, _build_wildcard_regex, _expand_inline_alternation
- 31 integration tests covering all Responsa branches plus existing-path-unchanged regression tests
- Zero regressions: all 99 Responsa tests pass (68 core + 31 integration), all existing tests unaffected

## Task Commits

Each task was committed atomically:

1. **RED: Failing integration tests** - `13942e2` (test) -- 31 tests for Tantivy, regex, parse_query_syntax, execute_search Responsa branches
2. **GREEN: Implementation** - `ee405cc` (feat) -- all 4 functions modified, 3 helpers added, all tests passing

_Note: No refactor commit needed -- code was clean after GREEN._

## Files Created/Modified

- `genizah_core.py` -- Modified: build_tantivy_query (added responsa_components/responsa_options params + Responsa branch), build_regex_pattern (same + wildcard/flex/bidirectional logic), parse_query_syntax (added responsa_mode param), execute_search (added responsa_options param + full Responsa pipeline). Added: _make_flex_spacing_pattern, _build_wildcard_regex, _expand_inline_alternation as module-level helpers.
- `tests/test_responsa_integration.py` -- Created: 31 tests in 4 test classes (TestBuildTantivyQueryResponsa, TestBuildRegexResponsa, TestParseQuerySyntaxResponsaBypass, TestExecuteSearchResponsa) with module-restore fixture for test isolation.

## Decisions Made

- **Expansion order**: plene/defective runs first (before prefixes/suffixes) because plene generates base spelling variants that should then be morphologically expanded. Order: plene -> prefixes -> suffixes -> JA -> variants.
- **Component dict intermediate representation**: Rather than passing ResponsaComponent objects to build_tantivy_query/build_regex_pattern (which would create coupling), we build plain dicts with keys tantivy_terms, regex_terms, original_words, wildcard, etc. This keeps the builders generic.
- **Flex patterns as alternation alternatives**: Rather than replacing terms with flex-spaced versions, flex patterns are added as additional alternatives in the regex alternation group. This means the non-flex version can still match for non-OCR text.
- **Module-restore fixture**: Added autouse fixture in test file to restore genizah_core in sys.modules after test_missing_tantivy.py corrupts it. This is a localized fix that doesn't modify other test files.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed debug log referencing undefined `terms` variable**
- **Found during:** GREEN phase implementation
- **Issue:** The debug log `LOGGER.info(f"[DEBUG] Mode: {mode}, Terms: {terms}")` referenced `terms` which is undefined in the Responsa branch (only defined in the else branch)
- **Fix:** Changed to `LOGGER.info(f"[DEBUG] Mode: {mode}, Query: {query_str[:200]}")` which uses the always-available query_str
- **Files modified:** genizah_core.py
- **Verification:** No NameError during Responsa pipeline execution
- **Committed in:** ee405cc (GREEN commit)

**2. [Rule 3 - Blocking] Added module-restore fixture for test isolation**
- **Found during:** GREEN phase full-suite verification
- **Issue:** test_missing_tantivy.py does `sys.modules.pop("genizah_core")` which corrupts module references, causing `patch('genizah_core.parse_responsa_query')` to patch a different module object than the one execute_search uses
- **Fix:** Added autouse fixture `_ensure_genizah_core_module()` that restores the correct module object in sys.modules before each test
- **Files modified:** tests/test_responsa_integration.py
- **Verification:** All 31 tests pass in isolation AND when run after test_missing_tantivy.py in the full test suite
- **Committed in:** ee405cc (GREEN commit)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking test infrastructure issue)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered

- Pre-existing test failures (13 tests in boundary_search, export_service, shelfmark_normalization_unified) confirmed to exist before this plan's changes -- not regressions.
- Pre-existing legacy backend test import errors (3 tests referencing removed FastAPI backend) -- known issue per CLAUDE.md.

## User Setup Required

None -- no external service configuration required.

## Next Phase Readiness

- Responsa core engine is fully implemented: Plan 14-01 pure functions + Plan 14-02 pipeline integration
- Phase 14 complete: parse_responsa_query -> expansion chain -> build_tantivy_query/build_regex_pattern -> existing hit processing pipeline
- Ready for Phase 15 (Search UI): web and desktop checkboxes can call execute_search with responsa_options dict
- The responsa_options dict interface: `{responsa_mode: bool, variants: bool, ja: bool, flex_spacing: bool, bidirectional: bool, variant_mode: str}`
- No blockers for Phase 15

---

## Self-Check: PASSED

- [x] genizah_core.py exists and has Responsa branches
- [x] tests/test_responsa_integration.py exists with 31 tests
- [x] Commit 13942e2 (RED) exists
- [x] Commit ee405cc (GREEN) exists
- [x] build_tantivy_query accepts responsa_components parameter
- [x] build_regex_pattern accepts responsa_components parameter
- [x] parse_query_syntax accepts responsa_mode parameter
- [x] execute_search accepts responsa_options parameter
- [x] All 99 Responsa tests pass (68 core + 31 integration)
- [x] Zero regressions in full test suite

*Phase: 14-responsa-core-engine*
*Completed: 2026-02-09*
