---
phase: 83-public-release
plan: 01
subsystem: api
tags: [security-audit, openapi, fastapi, rate-limiting, posthog, red-tests, public-release]

requires:
  - phase: 78-api-search-hardening-shell
    provides: api_hardening primitives, error envelope, RateLimiter, mode gate, neutral api_errors module
  - phase: 79-api-browse-drill-down
    provides: /api/browse endpoint, BrowseRequest model, core_timeout
  - phase: 80-api-parallels
    provides: /api/parallels endpoint, ParallelsRequest model
  - phase: 81A-api-contract-expansion
    provides: error code taxonomy expansion, invalid_combination code
provides:
  - 83-SECURITY.md (12-row mitigation table covering D-05 a-f + Post-Deploy Verification checklist)
  - tests/test_search_api_docs.py (8 RED stubs for PUBLIC-01/03/05/06)
  - tests/test_openapi_scope.py (4 RED stubs for PUBLIC-04, imports web.main per Codex concern #1)
  - tests/test_release_artifacts.py (3 RED stubs for PUBLIC-08)
affects: [83-02 docs reframe, 83-03 OpenAPI sub-mount, 83-04 README and skill, 83-05 release]

tech-stack:
  added: []
  patterns:
    - "RED-stub Wave 0 test scaffolding gates downstream plans (continues Phase 78–81 convention)"
    - "Security audit cites file:line for falsifiability (Section A) plus operator-runnable Post-Deploy checklist (Section B)"
    - "Audit scope is baseline-only; new-surface threats deferred to plan that introduces them + corresponding RED tests"

key-files:
  created:
    - .planning/phases/83-public-release/83-SECURITY.md
    - tests/test_search_api_docs.py
    - tests/test_openapi_scope.py
    - tests/test_release_artifacts.py
  modified: []

key-decisions:
  - "Audit verdict: APPROVED — all 12 baseline mitigations VERIFIED at cited file:line; no remediation plan needed before Plan 05"
  - "Phase 83 OpenAPI threats (T-83-OAS-LEAK, T-83-OAS-DOS) explicitly deferred to Plan 03 + Wave 0 OpenAPI scope tests per Codex review concern #4"
  - "test_openapi_scope.py imports web.main (not nicegui.app) per Codex concern #1 so the sub-mount is actually exercised"
  - "test_openapi_request_schemas_populated added per Codex concern #2 to catch buttonless Swagger UI failure mode"

patterns-established:
  - "Security-audit-as-document: Markdown file with mitigation table + post-deploy checklist + verdict, committed to .planning/phases/"
  - "RED tests use pytest.mark.skipif on import failure so collection succeeds even before downstream plans land"

requirements-completed: [PUBLIC-02]

duration: ~25min
completed: 2026-05-05
---

# Phase 83 Plan 01: Security Audit & RED Test Scaffold Summary

**12-mitigation security audit (APPROVED verdict) plus 15 Wave 0 RED test stubs gating Plans 02–05 of the v7.10.0 public API release.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-05-05T09:48Z
- **Completed:** 2026-05-05T10:13Z
- **Tasks:** 2 (Task 1 auto + Task 2 checkpoint:human-verify)
- **Files created:** 4 (1 audit doc + 3 test files)

## Accomplishments

- 83-SECURITY.md produced with Section A (12-row mitigation-coverage table covering D-05 items a–f + Phase 78 Concerns #1, #3, #4, #5, #9) and Section B (7-item Post-Deploy Verification checklist for Plan 05 operator). Verdict: **APPROVED**.
- Every Section A row carries a falsifiable file:line citation (e.g., `web/api_hardening.py:84`, `shared/api_errors.py:24-45`, `shared/fjms_service.py:1375`, `web/search_api.py:175`, `genizah_core.py:1996`).
- Out-of-scope section explicitly defers Phase 83 NEW threats (T-83-OAS-LEAK, T-83-OAS-DOS) to Plan 03's threat-model + Wave 0 OpenAPI scope tests.
- 15 RED test stubs collected cleanly across 3 new files (8 + 4 + 3), all currently failing for the right reason (target docs/code not yet produced by Plans 02–05).
- test_openapi_scope.py imports `web.main` per Codex concern #1 so the sub-mount is exercised, not a sibling FastAPI instance.
- test_openapi_request_schemas_populated guards against the Codex concern #2 failure mode (buttonless Swagger UI from missing requestBody schemas).

## Task Commits

1. **Task 1: Wave 0 RED test stubs (15 tests)** — `f6ff07f8` (test)
2. **Task 2: 83-SECURITY.md audit + Post-Deploy checklist** — `b7803a84` (docs)

## Files Created/Modified

- `.planning/phases/83-public-release/83-SECURITY.md` — 12-row STRIDE mitigation audit + 7-item Post-Deploy Verification checklist + APPROVED verdict
- `tests/test_search_api_docs.py` — 8 content-presence assertions on docs/SEARCH_API.md, README.md, skills/cairo-genizah-research/SKILL.md (PUBLIC-01/03/05/06)
- `tests/test_openapi_scope.py` — 4 FastAPI TestClient assertions against the mounted NiceGUI app (PUBLIC-04 / D-08); imports web.main; covers includes-helper-endpoints, excludes-legacy-routes, request-schemas-populated, swagger-ui-renders
- `tests/test_release_artifacts.py` — 3 file-content assertions for v7.10.0 (version.py, CHANGELOG.md, CLAUDE.md "Recently Changed") (PUBLIC-08)

## Decisions Made

See `key-decisions` in frontmatter — 4 decisions: APPROVED verdict, deferred new-surface threats, web.main import for OpenAPI tests, Codex concern #2 schema test.

## Deviations from Plan

None — plan executed exactly as written. The plan's task action specs included literal source for all 3 test files; produced verbatim. The audit document was authored directly from grepped citations against the live codebase at HEAD `a3282dc0`.

## Issues Encountered

- The `Write` tool initially routed the 83-SECURITY.md path to the main repo's `.planning/` directory rather than the worktree's. Resolved by `cp` into the worktree path and `rm` from the wrong location. The committed file is correctly inside the worktree at `.planning/phases/83-public-release/83-SECURITY.md`.

## User Setup Required

None — Phase 83 Plan 01 produces audit documentation and test scaffolding only; no external service configuration.

## Checkpoint Status

Task 2 is `type=checkpoint:human-verify`. The audit document already records verdict **APPROVED** based on direct grep evidence. Per the orchestrator's parallel-execution contract, the document is committed; human verification of the verdict (per the Plan's `how-to-verify` instructions) is the gate for Plan 02 to begin.

## Next Plan Readiness

- Plan 02 (docs reframe) ready to start — its tests in tests/test_search_api_docs.py will turn green as it lands.
- Plan 03 (OpenAPI sub-mount) ready to start — its tests in tests/test_openapi_scope.py will turn green as the sub-mount and Pydantic-bound endpoints land.
- Plan 04 (README + skill) tests in tests/test_search_api_docs.py share the same RED→GREEN trigger.
- Plan 05 (version bump + release) tests in tests/test_release_artifacts.py turn green when v7.10.0 is bumped and CHANGELOG / CLAUDE.md "Recently Changed" updated.

## Self-Check

Verifying claims before return:

- `.planning/phases/83-public-release/83-SECURITY.md` — FOUND
- `tests/test_search_api_docs.py` — FOUND
- `tests/test_openapi_scope.py` — FOUND
- `tests/test_release_artifacts.py` — FOUND
- Commit `f6ff07f8` — FOUND
- Commit `b7803a84` — FOUND
- pytest collected 15 tests across the 3 files — VERIFIED
- 12 mitigation rows in SECURITY.md (`grep -c '^| T-' = 12`) — VERIFIED
- Post-Deploy Verification section present (`grep -c 'Post-Deploy Verification' = 1`) — VERIFIED
- 7 file:line citations to api_hardening.py — VERIFIED

## Self-Check: PASSED

---
*Phase: 83-public-release*
*Plan: 01*
*Completed: 2026-05-05*
