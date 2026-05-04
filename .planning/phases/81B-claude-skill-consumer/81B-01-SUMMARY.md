---
phase: 81B-claude-skill-consumer
plan: 01
subsystem: testing
tags: [skill, anthropic-skill, tdd, fixtures, red-tests, token-bucket, honesty-annotation]

requires:
  - phase: 79-api-browse-drill-down
    provides: text_source enum (pgp_transcription|snippet|none), image best-effort shape, browse envelope
  - phase: 80-api-parallels
    provides: parallels envelope (filtered[], matches[], aggregate_score), mode enum
  - phase: 77-serializer-json-export
    provides: schema_version=1, uid+locator on every result, matches[] per chunk

provides:
  - skills/cairo-genizah-research/ source tree skeleton with __init__.py and state/.gitkeep
  - 6 fixture JSON files capturing locked API envelope shapes for offline testing
  - 15 RED tests (test_skill_consumer.py): SKILL-04/05/02 contracts Plan 03 will flip GREEN
  - 7 RED tests (test_skill_throttle.py): SKILL-06 token-bucket contracts Plan 02 will flip GREEN
  - 2 skipped smoke tests (test_skill_smoke.py): SKILL-01 live harness Plan 05 will execute

affects:
  - 81B-02 (transport + throttle — throttle tests are its acceptance contract)
  - 81B-03 (business logic — consumer tests are its acceptance contract)
  - 81B-05 (acceptance run — smoke harness is its live gate)

tech-stack:
  added: []
  patterns:
    - "RED scaffolding: import-fails-at-collection as intentional test gate"
    - "Fixture corpus: offline JSON files mirror locked API envelope shapes for unit tests"
    - "conftest_skill.py module (not conftest.py): explicit import by test files, no pytest auto-loading"

key-files:
  created:
    - skills/cairo-genizah-research/scripts/__init__.py
    - skills/cairo-genizah-research/scripts/fixtures/__init__.py
    - skills/cairo-genizah-research/scripts/fixtures/search_response.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json
    - skills/cairo-genizah-research/scripts/fixtures/parallels_response.json
    - skills/cairo-genizah-research/scripts/fixtures/error_rate_limited.json
    - skills/cairo-genizah-research/state/.gitkeep
    - tests/conftest_skill.py
    - tests/test_skill_consumer.py
    - tests/test_skill_throttle.py
    - tests/test_skill_smoke.py
  modified: []

key-decisions:
  - "R2 mapping locked: text_source='pgp_transcription' treated as 'full'; honesty annotation skipped. Phase 79 D-10 enum has no 'full' value — skill maps pgp_transcription as the full-text signal."
  - "conftest_skill.py is NOT a pytest conftest; imported explicitly by test files to avoid auto-loading side effects"
  - "skills/ directory uses hyphenated path (cairo-genizah-research) — Python import path uses underscores (cairo_genizah_research); the mismatch causes intentional ModuleNotFoundError at collection time"
  - "Throttle tests use monkeypatch + fake clock (never time.sleep) to keep CI fast; real throttle logic exercised in Plan 02 integration"

patterns-established:
  - "RED gate pattern: test imports fail at collection with ModuleNotFoundError; downstream plans flip GREEN by landing the referenced modules"
  - "Fixture corpus: recorded API envelopes as JSON for offline unit testing; avoids network dependency in CI"

requirements-completed: [SKILL-01, SKILL-04, SKILL-05, SKILL-06]

duration: 5min
completed: 2026-05-04
---

# Phase 81B Plan 01: Wave 0 RED Scaffolding Summary

**9-file skill source-tree skeleton + 6 fixture JSON files capturing locked Phase 77/79/80 API envelope shapes, plus 24 RED tests (15 consumer + 7 throttle + 2 smoke) establishing executable contracts for Plans 02-05**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-05-04T19:00:30Z
- **Completed:** 2026-05-04T19:04:42Z
- **Tasks:** 3
- **Files created:** 14 (9 task 1, 2 task 2, 2 task 3, +1 conftest_skill)

## Accomplishments

- Created `skills/cairo-genizah-research/` source tree with `__init__.py` files enabling Python import paths and `state/.gitkeep` for throttle JSON persistence
- Laid down 6 fixture JSON files (search, browse-pgp_full, browse-snippet, browse-no_image, parallels, error-rate_limited) capturing all three locked API endpoint shapes from Phases 77/79/80
- Authored 15 RED consumer tests locking SKILL-04 honesty annotations (R2 mapping), SKILL-05 known-witness policy, and SKILL-02 merge-by-uid tier ranking
- Authored 7 RED throttle tests exercising token-bucket math, burst handling, bucket isolation, state persistence, fault recovery, and SKILL-06 verification math (15+10 calls <= 60s)
- Added 2 live smoke tests gated behind `SKILL_SMOKE=1` — skip cleanly in CI, provide the Plan 05 acceptance harness

## Task Commits

1. **Task 1: Create skill source-tree skeleton + fixture corpus** - `6f760fed` (feat)
2. **Task 2: Author RED test scaffolds for SKILL-04, SKILL-05, SKILL-02** - `ed79335f` (test)
3. **Task 3: Author RED test scaffolds for SKILL-06 throttle + live smoke harness** - `a2e8e5c6` (test)

## Files Created/Modified

- `skills/cairo-genizah-research/scripts/__init__.py` - Package marker enabling `skills.cairo_genizah_research.scripts.*` imports
- `skills/cairo-genizah-research/scripts/fixtures/__init__.py` - Fixtures subpackage marker
- `skills/cairo-genizah-research/scripts/fixtures/search_response.json` - Locked /api/search envelope (schema_version=1, uid+locator, 2 results)
- `skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json` - Browse with text_source=pgp_transcription (the "full" case per R2)
- `skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json` - Browse with text_source=snippet + enrichment_timeout warning
- `skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json` - Oxford fragment with image.url=null, sources=[]
- `skills/cairo-genizah-research/scripts/fixtures/parallels_response.json` - Parallels with filtered=[], matches[], aggregate_score
- `skills/cairo-genizah-research/scripts/fixtures/error_rate_limited.json` - rate_limited error envelope
- `skills/cairo-genizah-research/state/.gitkeep` - Ensures state/ dir exists for throttle.json writes
- `tests/conftest_skill.py` - Explicit load_fixture() helper (not a pytest conftest)
- `tests/test_skill_consumer.py` - 15 RED tests for SKILL-04/05/02
- `tests/test_skill_throttle.py` - 7 RED tests for SKILL-06 token-bucket
- `tests/test_skill_smoke.py` - 2 live smoke tests, skip unless SKILL_SMOKE=1

## Decisions Made

- **R2 mapping locked in test name:** `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2` encodes the decision that `text_source='pgp_transcription'` triggers no honesty annotation. Phase 79 D-10 enum is `pgp_transcription|snippet|none` — REQUIREMENTS.md SKILL-04 says `!= 'full'` but no 'full' value exists in the contract. Skill maps `pgp_transcription` as the "full text available" signal per RESEARCH.md §4 R2.
- **conftest_skill.py is NOT a pytest conftest:** Named explicitly to avoid pytest auto-discovering it as a conftest, which could cause import-time side effects across the test suite.
- **Fixture Hebrew content stored as raw UTF-8:** Hebrew literals in fixture JSON files use raw UTF-8 characters (not `\uXXXX` escapes) per plan requirement.

## Deviations from Plan

None — plan executed exactly as written.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. Files created are test fixtures and test modules only; no production runtime paths affected.

## Known Stubs

None — this is a scaffolding/RED-tests plan. No UI or data-rendering paths exist to stub.

## Issues Encountered

- The `skills/cairo-genizah-research/` directory name uses hyphens; Python cannot import `skills.cairo_genizah_research` because the directory has hyphens (no `skills/__init__.py` exists and the dir name is non-importable). This produces the intended `ModuleNotFoundError` at pytest collection time — the RED state the plan specifies. Plan 02/03 will install the skill to an importable location (`~/.claude/skills/cairo-genizah-research/`) using `PYTHONPATH` or package installation; this is tracked in those plans.

## Next Phase Readiness

- Plan 02 (transport + throttle) has executable contracts in `tests/test_skill_throttle.py` — 7 tests it must flip GREEN
- Plan 03 (business logic) has executable contracts in `tests/test_skill_consumer.py` — 15 tests it must flip GREEN
- Plan 05 (acceptance run) has the live smoke harness in `tests/test_skill_smoke.py` — 2 tests it activates via `SKILL_SMOKE=1`
- Fixture corpus is available at `skills/cairo-genizah-research/scripts/fixtures/` for offline testing in all downstream plans

## Self-Check: PASSED

All 14 created files exist on disk. All 3 task commits verified in git log:
- `6f760fed` feat(81B-01): create skill source-tree skeleton + fixture corpus
- `ed79335f` test(81B-01): add RED tests for SKILL-04 honesty annotations, SKILL-05 known-witness, SKILL-02 merge-by-uid
- `a2e8e5c6` test(81B-01): add RED throttle tests (SKILL-06) + live smoke harness (SKILL-01)

---
*Phase: 81B-claude-skill-consumer*
*Completed: 2026-05-04*
