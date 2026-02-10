---
phase: 14-responsa-core-engine
plan: 01
subsystem: search
tags: [responsa, hebrew-nlp, query-parser, prefix-expansion, judeo-arabic, explosion-guard, tdd]

# Dependency graph
requires: []
provides:
  - ResponsaComponent dataclass for structured Responsa query tokens (with suffix/plene fields)
  - parse_responsa_query() tokenizer for Responsa syntax (including word#, #word#, %word)
  - expand_grammatical_prefixes() with 24 Hebrew prefix forms
  - expand_grammatical_suffixes() with 25 Hebrew suffix forms
  - expand_plene_defective() for plene/defective spelling variants (bidirectional ו/י)
  - expand_judeo_arabic() with simplified 8-form model
  - _apply_explosion_guard() with MAX_EXPANDED_TERMS=500 cascade
affects: [14-02-PLAN, phase-15-search-ui, phase-16-tabular-builder]

# Tech tracking
tech-stack:
  added: []
  patterns: [dataclass-based query IR, pure-function expansion pipeline, cascade guard pattern]

key-files:
  created: [tests/test_responsa_core.py]
  modified: [genizah_core.py]

key-decisions:
  - "Simplified Judeo-Arabic: always 'al' (no sun letter assimilation) -- 8 forms per word"
  - "24 grammatical prefix entries (including empty string for bare word)"
  - "Tokenizer uses paren-depth tracking state machine rather than regex"
  - "Explosion guard counts terms via estimation before materializing expansions"

patterns-established:
  - "ResponsaComponent as intermediate representation between raw query and search execution"
  - "Pure function expansion pipeline: parse -> expand prefixes -> expand JA -> guard"
  - "Cascade downgrade pattern for explosion guard: mode->basic->off->JA off->error"

# Metrics
duration: 10min
completed: 2026-02-09
---

# Phase 14 Plan 01: Responsa Core Functions Summary

**TDD-driven pure functions: Responsa query parser, Hebrew prefix expansion (24 forms), simplified Judeo-Arabic expansion (8 forms), and explosion guard with 500-term cascade**

## Performance

- **Duration:** 10 min
- **Started:** 2026-02-09T18:45:07Z
- **Completed:** 2026-02-09T18:51:05Z
- **Tasks:** 1 TDD cycle (RED already committed, GREEN + test updates implemented)
- **Files modified:** 2

## Accomplishments
- ResponsaComponent dataclass providing structured IR for all Responsa query syntax elements (including suffix/plene/defective fields)
- parse_responsa_query() correctly handles: plain words, suffix/prefix wildcards, character patterns, #prefix, suffix#, #both#, %plene/defective, OR groups, hash+OR, inline alternations, multi-component queries, combined operators (%#word#)
- expand_grammatical_prefixes() generates 24 unique prefixed forms per Hebrew word
- expand_grammatical_suffixes() generates 25 unique suffixed forms per Hebrew stem (Bar-Ilan style)
- expand_plene_defective() generates bidirectional ו/י spelling variants (removal + addition)
- expand_judeo_arabic() generates 8 forms per word using simplified model (no sun letter assimilation per user directive)
- _apply_explosion_guard() enforces MAX_EXPANDED_TERMS=500 with 4-step cascade (now counts suffix/plene expansion in estimates)
- 68 comprehensive tests all passing (31 new tests for suffix/plene/% features)

## Task Commits

Each task was committed atomically:

1. **RED: Failing tests** - `dd0b515` (test) -- pre-existing commit from prior session
2. **GREEN: Implementation + test updates** - `d6bfdde` (feat) -- all functions implemented, tests updated for simplified JA model

_Note: RED commit was from a prior session. This execution focused on GREEN implementation and test correction._

## Files Created/Modified
- `genizah_core.py` - Added ResponsaComponent dataclass, parse_responsa_query(), expand_grammatical_prefixes(), expand_judeo_arabic(), _apply_explosion_guard(), _count_expanded_terms(), _tokenize_responsa_query(), _parse_single_token() (all above SearchEngine class around line 4149)
- `tests/test_responsa_core.py` - Updated: removed SUN_LETTERS import, rewrote JA tests for 8-form model, fixed explosion guard test data for realistic cascade triggering

## Decisions Made
- **No sun letter assimilation** (user directive): All Judeo-Arabic words get exactly 8 forms regardless of first letter. This simplifies the model significantly and removes the SUN_LETTERS constant.
- **24 grammatical prefixes**: The GRAMMATICAL_PREFIXES list has 24 entries (including empty string), producing 24 unique forms per word.
- **State-machine tokenizer**: Used paren-depth tracking rather than regex for tokenization, which correctly handles nested syntax like `#(שלום/שלומות)`.
- **Estimation-based guard**: The explosion guard counts terms via `_count_expanded_terms()` estimation before materializing, avoiding unnecessary computation for queries that need downgrading.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed test data for explosion guard JA cascade test**
- **Found during:** GREEN phase test execution
- **Issue:** Test `test_over_limit_disables_ja` used 50 words * 24 prefixes = 1200 base terms, which exceeds 500 even after disabling JA, causing unexpected ValueError instead of successful downgrade
- **Fix:** Reduced to 20 words (20 * 24 = 480 <= 500 without JA, 20 * 24 * 8 = 3840 > 500 with JA)
- **Files modified:** tests/test_responsa_core.py
- **Verification:** Test now correctly exercises the JA disable cascade step
- **Committed in:** d6bfdde (GREEN commit)

**2. [User Override] Removed sun letter assimilation from tests and implementation**
- **Found during:** Plan analysis (user critical override)
- **Issue:** Plan specified 14 forms for sun letters with assimilation; user explicitly overrode to 8 forms for all words
- **Fix:** Removed SUN_LETTERS constant, rewrote all JA tests for 8-form model, implemented simplified expand_judeo_arabic()
- **Files modified:** genizah_core.py, tests/test_responsa_core.py
- **Verification:** All 37 tests pass with 8-form model
- **Committed in:** d6bfdde (GREEN commit)

---

**Total deviations:** 2 (1 auto-fixed bug in test data, 1 user-directed design override)
**Impact on plan:** User override simplified the JA model. Test data fix was necessary for correctness. No scope creep.

## Issues Encountered
- Pre-existing test failures (13 tests in boundary_search, export_service, shelfmark_normalization) confirmed to exist before this plan's changes -- not regressions.
- Pre-existing legacy backend test import errors (3 tests referencing removed FastAPI backend) -- known issue per CLAUDE.md.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All 5 pure functions ready for Plan 14-02 to wire into the existing search pipeline
- ResponsaComponent serves as the intermediate representation between parse_responsa_query() output and the Tantivy/Regex query builders
- Explosion guard ready to wrap the full expansion pipeline
- No blockers for Plan 14-02

---
## Self-Check: PASSED

- [x] genizah_core.py exists
- [x] tests/test_responsa_core.py exists
- [x] 14-01-SUMMARY.md exists
- [x] Commit dd0b515 (RED) exists
- [x] Commit d6bfdde (GREEN) exists
- [x] All imports succeed (ResponsaComponent, parse_responsa_query, expand_grammatical_prefixes, expand_judeo_arabic, _apply_explosion_guard, GRAMMATICAL_PREFIXES, Config)
- [x] MAX_EXPANDED_TERMS=500
- [x] GRAMMATICAL_PREFIXES count=24
- [x] JA expansion produces 8 forms

*Phase: 14-responsa-core-engine*
*Completed: 2026-02-09*
