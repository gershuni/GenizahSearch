---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 06
subsystem: database
tags: [discovery, sidecar-build, release-verifier, precision-authorization, sqlite, tdd]

# Dependency graph
requires:
  - phase: 136-01
    provides: "The dated §1.6 tier_a authorization amendment in docs/specs/discovery-sidecar-schema-v1.md, naming the six lockstep sites"
provides:
  - "The D-02a tier_a CERT-01 authorization lockstep: the frozen builder row, the widened build-time validator, the widened release verifier (both the tier_a-specific check and an independent smuggling check), and both-branch (pass/fail) test fixtures at both the Python-function layer and the raw SQL/DDL layer"
  - "tier_a is default-eligible (is_default_eligible() reads measurement_status='measured_pass' + ci_low=0.9084 >= STRICT_FLOOR) with zero measured-precision numbers anywhere in the build path -- the main pool can now include tier_a's 81%-of-corpus population instead of collapsing to ~2,241 identifications"
  - "A scoped carve-out in check_measurement_status_ci_consistency (gate 12) for the authorization-only tier_a shape, discovered and fixed as a direct consequence of this plan's own change"
affects: [136-12, 136-13, 136-15, 136-17, 136-19, 136-20]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Local-literal mirroring of a shared/ module's closed vocabulary (MEASUREMENT_STATUSES_FROZEN in build_discovery_sidecar.py, _MEASUREMENT_STATUSES already established in verify_discovery_sidecar.py) instead of importing shared/ from a stdlib-only build/verify script"
    - "Independent double-gating: the builder's _validate_precision_spec and the release verifier's M4 check assert the SAME authorized pair without either importing the other's row-set"

key-files:
  created: []
  modified:
    - scripts/build_discovery_sidecar.py
    - scripts/verify_discovery_sidecar.py
    - tests/test_discovery_build.py
    - tests/test_discovery_schema.py
    - tests/test_discovery_release_contract.py

key-decisions:
  - "The tier_a frozen row gains exactly two keys (ci_low=0.9084, measurement_status=measured_pass); precision/ci_high/numerator/denominator stay NULL -- the CERT-01 AUTHORIZATION, never a measured number, per docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment 2026-08-02"
  - "check_measurement_status_ci_consistency (gate 12) needed a scoped carve-out for evidence_source=track1_direct/confidence_band=tier_a: its pre-existing 'measured_pass requires all five fields non-NULL' rule is unchanged for every other band, but tier_a's measured_pass now means the authorization shape (ci_low only) -- this was NOT one of the schema doc's six named lockstep sites, but is a direct, load-bearing consequence of amending site #2 (the frozen row), verified by running the actual build+verify pipeline before treating Task 1/2 as done"
  - "check_band_precision's SQL SELECT degrades to a literal NULL for measurement_status when the column is absent (_has_column guard) so the pinned pre-135-05 golden fixture stays byte-identical and unbroken"
  - "requirements-completed left empty, following 136-01's own precedent: PANEL-01/PANEL-02 are shared frontmatter IDs spanning multiple later panel-UI plans (136-15/17/19/20); marking them Complete after only this data-layer plan would be premature"

patterns-established:
  - "When a lockstep amendment changes a stored value's SHAPE (not just its presence), re-run the full existing test suite for that subsystem BEFORE declaring a task done -- a schema-doc's own enumerated lockstep list may not anticipate every consumer of the changed field"

requirements-completed: []

# Metrics
duration: 23min
completed: 2026-08-02
---

# Phase 136 Plan 06: D-02a Tier-A Authorization Lockstep Summary

**Lands the one authorized `band_precision.tier_a` change (measurement_status='measured_pass', ci_low=0.9084, precision stays NULL) across the builder, the build-time validator, the release verifier's M4 check plus a new smuggling check, and both-branch test fixtures at two independent layers -- and fixes a gate-12 regression the amendment directly caused.**

## Performance

- **Duration:** 23 min
- **Started:** 2026-08-02T20:32:00Z
- **Completed:** 2026-08-02T20:55:00Z
- **Tasks:** 3 completed
- **Files modified:** 5

## Accomplishments

- `scripts/build_discovery_sidecar.py`: `_frozen_real_band_precision_rows`'s `tier_a` dict gains `ci_low=0.9084` and `measurement_status='measured_pass'` (precision stays `None`); `_validate_precision_spec`'s per-band loop now also asserts `ci_low`/`measurement_status` match the frozen row exactly, with an independent closed-vocabulary cross-check (`MEASUREMENT_STATUSES_FROZEN`, a local literal mirror of `shared/discovery_band_labels.MEASUREMENT_STATUSES`, matching the existing `STRICT_FLOOR_FROZEN` mirror convention). The `band_precision` INSERT statement is confirmed unchanged; a one-line comment now points at the frozen row as the dict-override's source of truth.
- `scripts/verify_discovery_sidecar.py`: the pre-existing "tier_a precision must be NULL" M4 check is unmodified; new companion assertions require `measurement_status`/`ci_low` to match the authorized pair and `ci_high`/`numerator`/`denominator` to stay NULL, each violation naming only the frozen expected value (never a supplied one). A new independent smuggling check rejects `measurement_status='measured_pass'` on any band other than `tier_a`. `check_band_precision`'s SELECT now reads `measurement_status`, degrading to a literal `NULL` on the pre-135-05 pinned golden fixture via `_has_column`.
- Rule-1 fix: `check_measurement_status_ci_consistency` (gate 12) previously required ALL FIVE precision/CI/num/denom fields non-NULL for any `measured_pass` row -- a real assumption this plan's own tier_a change violates (the authorized shape carries only `ci_low`). Added a scoped carve-out for `track1_direct`/`tier_a` specifically; every other band's rule, and tier_a's own `measured_fail`/`not_measured` outcomes, are unchanged.
- Both-branch test fixtures: 7 new tests in `tests/test_discovery_build.py` (PASS: frozen rows validate clean + `is_default_eligible` flips True/False across the amendment; 4 FAIL cases (a)-(d); a mechanism test pinning the `{"measurement_status": None, **r}` dict-literal override; a masking test with a sentinel `ci_low`), plus 2 new raw-SQL/DDL-layer tests in `tests/test_discovery_schema.py` proving the `band_precision.measurement_status` CHECK constraint independently accepts/rejects the same pair.

## Task Commits

Each task was committed atomically:

1. **Task 1: Amend the frozen tier_a registry row and widen `_validate_precision_spec`** - `b409c1be` (feat)
2. **Task 2: Widen the release verifier's tier_a check without relaxing it** - `e35dcedc` (feat)
3. **Task 3: Both-branch fixtures — the authorized pair flips the gate, every variant is rejected** - `b66beb4b` (test)

_No plan-metadata-only commit was needed beyond the three task commits above; the final metadata commit (SUMMARY + STATE + ROADMAP + REQUIREMENTS, if any) is handled by the orchestrator per this plan's explicit instructions._

## Files Created/Modified

- `scripts/build_discovery_sidecar.py` - frozen `tier_a` row + widened `_validate_precision_spec` + `MEASUREMENT_STATUSES_FROZEN` local mirror
- `scripts/verify_discovery_sidecar.py` - widened M4 tier_a check + new smuggling check + `_has_column`-guarded SELECT + gate-12 carve-out
- `tests/test_discovery_build.py` - 7 new D-02a pass/fail/mechanism/masking tests
- `tests/test_discovery_schema.py` - 2 new raw-SQL CHECK-constraint tests
- `tests/test_discovery_release_contract.py` - `_make_band_precision_only_db` helper updated to carry `measurement_status` through its INSERT (Rule-1 fix, see Deviations)

## Decisions Made

See `key-decisions` in the frontmatter above. Most consequential: keeping the tier_a authorization to exactly two keys (never a numeric precision), and scoping the gate-12 carve-out narrowly to `track1_direct`/`tier_a` so every other band's stricter "genuinely measured" rule is untouched.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `check_measurement_status_ci_consistency` rejected the new tier_a authorization shape**
- **Found during:** Task 2, while running the plan's own `<verification>` command (`pytest tests/ -k discovery`) before considering Task 1/2 complete
- **Issue:** This pre-existing gate-12 check requires ALL FIVE precision/CI/num/denom fields non-NULL for any stored `measured_pass` row. After Task 1's change, the frozen `tier_a` row legitimately carries `measured_pass` with only `ci_low` set -- three end-to-end tests (`test_finalize_build_end_to_end_success`, `test_v2_release_valid_asset_full_verify_passes_require_v2`, `test_mutation_d_pure_v1_asset_green_but_fails_require_v2`) failed with `verify() == 1` instead of `0`.
- **Fix:** Added a scoped `is_tier_a_authorization` branch in `check_measurement_status_ci_consistency` requiring only `ci_low >= STRICT_FLOOR` and `precision`/`ci_high`/`numerator`/`denominator` all NULL for this ONE (evidence_source, confidence_band) pair; every other band's `measured_pass`/`measured_fail` rule, and tier_a's own `not_measured`/reband-invalidated outcomes, are unchanged.
- **Files modified:** `scripts/verify_discovery_sidecar.py`
- **Verification:** `pytest tests/ -k discovery -q` green (401 passed, 8 skipped) after the fix; re-confirmed with the broader discovery-adjacent suites (`test_rebuild_preservation.py`, `test_discovery_band_labels.py`, `test_discovery_v2_bake.py`).
- **Committed in:** `e35dcedc` (Task 2 commit)

**2. [Rule 3 - Blocking] `check_band_precision`'s new `measurement_status` SELECT broke the pinned pre-135-05 golden fixture**
- **Found during:** Task 2, same verification pass
- **Issue:** The pinned `tests/fixtures/discovery/discovery-v1-fixture.db` (134-03 golden fixture, must stay byte-identical) predates the 135-05 `measurement_status` column entirely; the unconditional SELECT raised `sqlite3.OperationalError: no such column`.
- **Fix:** Added a `_has_column(conn, "band_precision", "measurement_status")` guard, degrading the SELECT to a literal `NULL` expression when the column is absent -- mirrors the same guard already used by `check_measurement_status_ci_consistency`/`check_reband_precision_invalidation`.
- **Files modified:** `scripts/verify_discovery_sidecar.py`
- **Verification:** `test_verify_clean_fixture_passes` and the other 21 previously-failing `tests/test_discovery_release_contract.py` tests all pass.
- **Committed in:** `e35dcedc` (Task 2 commit)

**3. [Rule 1 - Bug] `tests/test_discovery_release_contract.py`'s `_make_band_precision_only_db` test helper silently dropped `measurement_status`**
- **Found during:** Task 2, same verification pass
- **Issue:** This pre-existing helper's INSERT statement had no `measurement_status` column at all, so passing `sidecar_build._frozen_real_band_precision_rows()` into it (as `test_m4_release_mode_strict_check_passes_on_frozen_rows` does) silently wrote `NULL` for every row, including the now-amended `tier_a` row -- causing the new M4 assertions to correctly (but unintentionally, for this test) fail.
- **Fix:** Added `measurement_status` to the INSERT's column list and switched to the SAME `{"measurement_status": None, **r} for r in ...` dict-literal-override pattern the real builder uses, so this helper now reproduces exactly what a real build would write.
- **Files modified:** `tests/test_discovery_release_contract.py`
- **Verification:** `test_m4_release_mode_strict_check_passes_on_frozen_rows` and the full `tests/test_discovery_release_contract.py` suite pass.
- **Committed in:** `e35dcedc` (Task 2 commit)

**4. [Self-caught, pre-commit] An early draft of the FAIL-case-(c) test used the measured point estimate `0.9382`**
- **Found during:** Task 3, before committing, while re-running the plan's own `grep -rn "0.9382" ...` verification command
- **Issue:** The plan's acceptance criteria and `<verification>` section both explicitly require zero occurrences of `0.9382` (the CERT-01 measured point estimate) across the builder/verifier/test files. An early draft of `test_d02a_fail_c_non_null_precision_on_tier_a_rejected` used `0.9382` as the fabricated non-NULL value to reject.
- **Fix:** Changed the fabricated value to `0.90` (any non-NULL number proves the same rejection; `0.9382` specifically must never appear).
- **Files modified:** `tests/test_discovery_build.py`
- **Verification:** `grep -rn "0.9382" scripts/build_discovery_sidecar.py scripts/verify_discovery_sidecar.py tests/test_discovery_build.py tests/test_discovery_schema.py` returns nothing (exit 1); the 7 D-02a tests still pass.
- **Committed in:** `b66beb4b` (Task 3 commit -- caught before this commit was made, never landed in git history)

---

**Total deviations:** 4 (3 Rule-1/Rule-3 auto-fixes to code the plan's own change broke, 1 self-caught pre-commit correction)
**Impact on plan:** All four were necessary to satisfy the plan's own `<verification>` requirement that the full `-k discovery` suite stays green and that `0.9382` never appears. No scope creep -- every fix is a direct, provable consequence of the Task 1 change, confirmed by actually running the pipeline rather than assuming the schema doc's six named lockstep sites were exhaustive.

## Issues Encountered

None beyond the deviations documented above (each was found and resolved within the same task's verification loop, never carried forward to a later task).

## User Setup Required

None - no external service configuration required. This plan is code + tests only.

## Next Phase Readiness

- Tier A is now default-eligible in any real build carrying the frozen `band_precision` defaults, with zero measured-precision numbers anywhere in the build/verify path -- plans 136-12 (build wiring) and 136-13 (the rebuild + gate battery + production redeploy) can proceed on the strength of this authorization.
- `requirements-completed` deliberately left empty (PANEL-01/PANEL-02 are shared across this plan and the later panel-UI plans 136-15/17/19/20; marking them Complete here would be premature, following 136-01's own precedent).
- No blockers. The `.masking_patterns` file was present locally; all 5 modified files scanned clean (`check_atlas_masking.py --scan-asset`).
- Worth flagging for whoever runs plan 136-13's gate battery: `check_measurement_status_ci_consistency`'s tier_a carve-out (Deviation 1 above) was NOT one of the schema doc's six named lockstep sites -- future amendments to the tier_a authorization shape should re-check this gate too, not just the six enumerated sites.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*

## Self-Check: PASSED

All 5 modified files confirmed present on disk (`scripts/build_discovery_sidecar.py`,
`scripts/verify_discovery_sidecar.py`, `tests/test_discovery_build.py`,
`tests/test_discovery_schema.py`, `tests/test_discovery_release_contract.py`); all 3 task
commit hashes (`b409c1be`, `e35dcedc`, `b66beb4b`) confirmed in `git log --oneline --all`.
