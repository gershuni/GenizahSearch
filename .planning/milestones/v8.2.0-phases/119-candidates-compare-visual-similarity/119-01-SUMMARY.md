---
phase: 119-candidates-compare-visual-similarity
plan: 01
subsystem: testing
tags: [joins-lab, visual-similarity, candidate-surface, ast-guard, pytest, shared-core]

# Dependency graph
requires:
  - phase: 117-vertical-spine
    provides: WebSearchExecutor adapter, off-loop discipline, joins_lab.py Candidate dataclass
  - phase: 118-joins-entry-full-builders
    provides: merge_candidates, dedup_candidates, detect_self_match in shared/joins_lab.py

provides:
  - badge_and_tooltip(cand) pure helper in shared/joins_lab.py with ⚓›⇄›👁 precedence
  - _find_blocking_call_violations() generic AST guard in tests/test_joins_lab_off_loop.py
  - Off-loop guard extended to cover VS lookup (get_suggestions) + enrichment batch (get_measurement_summaries_batch)
  - 7 RED test scaffold files covering all Wave 1-2 seams (CND-03/04/06/07/08, CMP-01/02/03, VSM-01)

affects:
  - 119-02 (Wave 1 — candidate grid extension; turns CND-03/04/06/07 scaffolds green)
  - 119-03 (Wave 1 — Compare modal; turns CMP-01/02/03 scaffolds green)
  - 119-04 (Wave 2 — VS toggle + enrichment; turns VSM-01 + CND-08 scaffolds green, off-loop guard live)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "_find_blocking_call_violations(source, blocking_attrs) — generic parameterized AST off-loop detector"
    - "xfail(strict=False) RED scaffold pattern — seams that import symbols inside test body to prevent collection errors"
    - "badge_and_tooltip() precedence pattern — is_anchor_self > via_other_side > via_vs with Material Icon names"

key-files:
  created:
    - tests/test_candidate_surface.py
    - tests/test_candidate_triage.py
    - tests/test_candidate_filters.py
    - tests/test_candidate_pagination.py
    - tests/test_candidate_enrichment.py
    - tests/test_compare_modal.py
    - tests/test_vs_adapter.py
  modified:
    - shared/joins_lab.py
    - tests/test_joins_lab.py
    - tests/test_joins_lab_off_loop.py

key-decisions:
  - "badge_and_tooltip Material Icon names locked: 'anchor' (⚓), 'swap_horiz' (⇄), 'visibility' (👁) — per RESEARCH.md Open Question #1 resolution"
  - "Two scaffold tests promoted from xfail to passing: test_vs_score_none_is_no_data_not_dissimilar (badge_and_tooltip already works) and test_enrichment_call_site_is_covered_by_off_loop_guard (guard already set up in Task 2)"
  - "Off-loop guard generalized via _find_blocking_call_violations(blocking_attrs) — keeps existing execute_search coverage intact, adds VS + enrichment coverage with skip-until-present semantics"

patterns-established:
  - "RED scaffold imports target symbols inside the test body (not at module level) so collection succeeds even when production code is absent"
  - "_find_blocking_call_violations(source, blocking_attrs) reuses all four existing AST helpers unchanged; no duplication"

requirements-completed: [VSM-02, CND-05, CND-08, VSM-01]

# Metrics
duration: 7min
completed: 2026-06-19
---

# Phase 119 Plan 01: Wave 0 Foundation Summary

**badge_and_tooltip() pure helper added to shared/joins_lab.py with ⚓›⇄›👁 precedence; off-loop AST guard generalized to cover VS lookup + enrichment batch; 7 RED scaffolds seed all Wave 1-2 requirements**

## Performance

- **Duration:** ~7 min
- **Started:** 2026-06-19T07:36:47Z
- **Completed:** 2026-06-19T07:43:33Z
- **Tasks:** 3
- **Files modified:** 10 (3 modified, 7 created)

## Accomplishments
- Added `badge_and_tooltip(cand) -> tuple` to `shared/joins_lab.py` — removes the import-error blocker for all Wave 1-2 badge rendering; implements `is_anchor_self > via_other_side > via_vs` precedence with Material Icon names `anchor`/`swap_horiz`/`visibility` (desktop parity `join_workbench.py:452-457`)
- Generalized the off-loop AST guard: new `_find_blocking_call_violations(source, blocking_attrs)` in `tests/test_joins_lab_off_loop.py` + two live-file tests (skip-until-present semantics) + four synthetic controls proving the detector fires and passes correctly for both `get_suggestions` and `get_measurement_summaries_batch`
- Created 7 RED test scaffold files covering all 9 Wave 1-2 requirements (CND-03/04/06/07/08, CMP-01/02/03, VSM-01) — all 32 tests collect without errors (30 xfail, 2 properly passing)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add badge_and_tooltip() pure helper + precedence test** - `1c72c0dc` (feat)
2. **Task 2: Extend off-loop AST guard to cover VS lookup + enrichment batch** - `6d1ca2de` (feat)
3. **Task 3: Create RED test scaffolds for Wave 1-2 seams** - `0eec3815` (test)

## Files Created/Modified
- `shared/joins_lab.py` — added `badge_and_tooltip(cand)` pure function after `detect_self_match` at line 625
- `tests/test_joins_lab.py` — appended `test_badge_and_tooltip_precedence` (4 behavior cases, all icon names asserted)
- `tests/test_joins_lab_off_loop.py` — added `_find_blocking_call_violations()`, `test_vs_lookup_not_on_event_loop`, `test_enrichment_batch_not_on_event_loop`, `TestSyntheticViolationsPhase119` (4 synthetic controls)
- `tests/test_candidate_surface.py` — CND-03 scaffolds (3 xfail)
- `tests/test_candidate_triage.py` — CND-04 scaffolds (4 xfail)
- `tests/test_candidate_filters.py` — CND-06 scaffolds (6 xfail)
- `tests/test_candidate_pagination.py` — CND-07 scaffolds (5 xfail)
- `tests/test_candidate_enrichment.py` — CND-08 scaffolds (2 xfail + 1 passing guard)
- `tests/test_compare_modal.py` — CMP-01/02/03 scaffolds (5 xfail)
- `tests/test_vs_adapter.py` — VSM-01 scaffolds (5 xfail + 1 passing guard)

## Decisions Made
- **Material Icon names locked** at plan-time: `anchor` (⚓), `swap_horiz` (⇄), `visibility` (👁) — these are the web equivalents of the desktop Qt glyphs, resolved as RESEARCH.md Open Question #1 and baked into the test and implementation.
- **Two scaffold tests promoted from xfail to passing**: `test_vs_score_none_is_no_data_not_dissimilar` (tests `badge_and_tooltip` which was just created in Task 1) and `test_enrichment_call_site_is_covered_by_off_loop_guard` (verifies the guard test file structure which was completed in Task 2). These are legitimate "already works" tests.
- **Generic detector preserves execute_search coverage unchanged** — `_find_blocking_call_violations` is ADDITIVE; `_find_execute_search_violations` and all its tests remain intact.

## Deviations from Plan

None — plan executed exactly as written. All three tasks completed in order with all acceptance criteria satisfied.

## Issues Encountered

None. The two `xpassed` scaffold tests were expected once it became clear that Task 1 (`badge_and_tooltip`) and Task 2 (off-loop guard extension) provide the exact infrastructure those tests validate. Promoted to `passed` status by removing the `xfail` marker.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. `badge_and_tooltip` is a pure function with static literal returns (no user input). The off-loop guard changes are test-only. No threat flags.

## Known Stubs

None. This plan adds only a pure helper function + test infrastructure. No production UI surface or data flows are wired.

## Self-Check

- `shared/joins_lab.py` contains `def badge_and_tooltip` — VERIFIED (grep confirms)
- `tests/test_joins_lab_off_loop.py` contains `_find_blocking_call_violations` and `get_measurement_summaries_batch` — VERIFIED
- Commits `1c72c0dc`, `6d1ca2de`, `0eec3815` exist — VERIFIED (`git rev-parse --short` confirmed each)
- All 7 scaffold files exist under `tests/` — VERIFIED (pytest collected 32 items)
- `python -m pytest tests/test_no_raw_storage_access.py -q` green — VERIFIED (6 passed)

## Self-Check: PASSED

## Next Phase Readiness
- Wave 0 complete: `badge_and_tooltip` importable, off-loop guard generalized, RED scaffolds in place
- Wave 1 (Plan 119-02) can now import `from shared.joins_lab import badge_and_tooltip` without ImportError
- Wave 2 (Plans 119-03, 119-04) can make CMP-*/VSM-01 scaffolds green
- The off-loop guard tests will go live (un-skip) automatically once Wave 2 adds `get_suggestions` and `get_measurement_summaries_batch` call sites to `web/pages/joins_lab.py`

---
*Phase: 119-candidates-compare-visual-similarity*
*Completed: 2026-06-19*
