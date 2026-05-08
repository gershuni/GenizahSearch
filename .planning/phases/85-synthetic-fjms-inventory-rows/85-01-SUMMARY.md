---
phase: 85-synthetic-fjms-inventory-rows
plan: 01
subsystem: shared-helpers
tags: [synthetic-sys-id, helper-module, phase-85, pure-functions, fixture, repo-grep-lint]

# Dependency graph
requires:
  - phase: 84-cudl-shelfmark-normalization
    provides: shared/shelfmark_bridge.py architectural template (pure functions, module-level constants, no I/O at import, layered cross-system lookup pattern)
provides:
  - shared/synthetic_sys_id.py — three pure-function helpers (is_synthetic_sys_id, encode_inventory_sys_id, decode_inventory_id) implementing the locked 18-digit synthetic format "99 + InventoryId.zfill(10) + 000000"
  - tests/fixtures/synthetic_fixtures.py — reusable golden cases (SYNTHETIC_GOLDEN_CASES, REAL_ALMA_NEGATIVE_CASES, D13_NORMALIZATION_NEGATIVES) for downstream plans 02-05
  - tests/test_synthetic_sys_id.py — 64-test suite locking helper contract + D-01b drift guard via repo-grep lint
affects: [85-02, 85-03, 85-04, 85-05, 86-cudl-coverage-audit]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Helper-as-public-contract: D-01 publishes three pure functions as the SYNTH-01 architecture; downstream plans MUST consult these helpers, never hand-roll string slicing or int() coercion on sys_ids"
    - "Repo-grep lint as enforcement primitive: pytest-collected test walks first-party Python files with regex pattern detection; allowlist is explicit; CI fails on drift"
    - "Internal-tolerance / external-discipline split: helper accepts int via str(s) coercion for migration safety, but the contract is string-only and the lint enforces it at all call sites"

key-files:
  created:
    - "shared/synthetic_sys_id.py — pure helpers + module-level constants (_SYNTHETIC_PREFIX='99', _SYNTHETIC_SUFFIX='000000', _INVENTORY_PAD=10, _TOTAL_LENGTH=18)"
    - "tests/fixtures/synthetic_fixtures.py — three reusable case lists for plans 02-05"
    - "tests/test_synthetic_sys_id.py — 64 tests: format detection, encode validation, decode roundtrip, D-01a collision invariant, D-13 normalization contract, D-01b repo-grep lint"
  modified: []

key-decisions:
  - "Sibling module shared/synthetic_sys_id.py (not extension of shared/shelfmark_bridge.py) — keeps Phase 84 bridge module focused on shelfmark normalization while a sibling owns sys_id format concerns; both modules layer on top of genizah_core without modifying it"
  - "Repo-grep lint replaces positive int-input test (Codex MEDIUM-1): the previous `is_synthetic_sys_id(990001234560000000) is True` test blessed the exact int-coercion D-01b is meant to prevent; removing it + adding TestNoIntCoercion is the affirmative D-01b enforcement"
  - "Bool explicitly rejected in encode (despite bool being subclass of int) — True/False sneaking through as 1/0 would generate sys_ids '990000000001000000' / ValueError, which is misuse; raising is safer than silently coercing"

patterns-established:
  - "Pure-function helper module: no logger calls, no I/O at import, only module-level constants and three top-level functions — safe to import from any layer including tests, scripts, and runtime hot paths"
  - "Triple-check format invariant for is_synthetic_sys_id: length=18 AND startswith('99') AND endswith('000000') AND .isdigit() — three independent constraints make the discriminator robust against accidental shape collision with real Alma 18-digit IDs"
  - "Documented int() slice as the only allowed coercion: decode uses `int(str(sys_id)[2 : 2 + _INVENTORY_PAD])` (slice + int, not bare int(sys_id)); the lint regex naturally tolerates this because the inner token starts with `str(`, not a bare identifier"

requirements-completed: [SYNTH-01]

# Metrics
duration: 22min
completed: 2026-05-08
---

# Phase 85 Plan 01: SYNTH-01 Helper Module Summary

**Three pure-function helpers (is_synthetic_sys_id, encode_inventory_sys_id, decode_inventory_id) implementing the locked 18-digit synthetic sys_id format with reusable test fixtures and a D-01b repo-grep drift guard.**

## Performance

- **Duration:** ~22 min
- **Started:** 2026-05-08T09:21:00Z (approx., from worktree branch creation)
- **Completed:** 2026-05-08T09:43:32Z
- **Tasks:** 2
- **Files created:** 3

## Accomplishments

- Locked the SYNTH-01 helper contract that Plans 02-05 will consume: any first-party code that needs to detect, encode, or decode synthetic sys_ids now has a single source of truth.
- Established the D-01a collision discriminator empirically: `000000` suffix (synthetic) vs `205171` and other institution suffixes (real Alma). Real-Alma rows from the live codebase (`990025143260205171` NLI, `990053835020205171` Mosseri, `990052439490205171` CUL) explicitly classified False in tests.
- Established the D-13 normalization contract: helper accepts canonical all-digit input only; any non-digit input rejects deterministically. The 11 D-13 negative cases all classify False.
- Established the D-01b string discipline drift guard: TestNoIntCoercion walks all first-party Python files (excluding planning docs, build artifacts, third-party data) with a regex pattern catching `int(sys_id)`, `int(raw_sys_id)`, `int(self.sys_id)`, etc. Pre-merge scan: zero violations.
- Round-trip property holds: `decode_inventory_id(encode_inventory_sys_id(n)) == n` for n in `{1, 100, 12345, 123456, 999999, 1234567890, 9999999999}`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Create shared/synthetic_sys_id.py helper module + golden fixtures** — `0535dd86` (feat)
2. **Task 2: Write tests/test_synthetic_sys_id.py covering SYNTH-01 contract + D-01b repo-grep lint** — `baf7675e` (test)

## Files Created/Modified

- `shared/synthetic_sys_id.py` — Pure-function helpers (97 lines). Module-level constants `_SYNTHETIC_PREFIX="99"`, `_SYNTHETIC_SUFFIX="000000"`, `_INVENTORY_PAD=10`, `_TOTAL_LENGTH=18`. No `import logging`, no I/O at import time.
- `tests/fixtures/synthetic_fixtures.py` — Three reusable case lists: `SYNTHETIC_GOLDEN_CASES` (4 illustrative encode pairs across tier1 + edge boundaries), `REAL_ALMA_NEGATIVE_CASES` (4 D-01a collision negatives sourced from cudl_must_resolve.csv + STATE.md investigation), `D13_NORMALIZATION_NEGATIVES` (11 inputs that must reject).
- `tests/test_synthetic_sys_id.py` — Six test classes, 64 tests total: TestIsSyntheticSysId (parametrized over fixtures), TestEncodeInventorySysId (boundaries + ValueError matrix + bool rejection + return-type assertion), TestDecodeInventoryId (synthetic positives + real-Alma None + D-13 negatives None + empty None), TestRoundTrip, TestRealAlmaCollisionNegative (D-01a explicit invariant), TestNoIntCoercion (D-01b repo-grep lint).

## Decisions Made

- **Sibling module over extension** — Created `shared/synthetic_sys_id.py` rather than extending `shared/shelfmark_bridge.py`. Phase 84's bridge owns shelfmark normalization concerns; Phase 85's helper owns sys_id format concerns. Same architectural template (docstring shape, pure functions, module-level constants, no logger), separate concerns. Plan left this to executor's discretion.
- **bool rejection in encode** — Even though `isinstance(True, int)` is True in Python, `encode_inventory_sys_id(True)` would silently produce `'990000000001000000'`. Explicit `isinstance(inventory_id, bool)` rejection raises ValueError instead.
- **Plan typo in encode pair examples reconciled** — See Deviations section. The plan's roundtrip invariant is the load-bearing contract; illustrative example pairs were typos that I corrected to be internally consistent.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan typo in illustrative encode pair examples reconciled**

- **Found during:** Task 1 (smoke verification of helper module)
- **Issue:** The plan's `must_haves`, `<behavior>`, and inline verify command claimed `encode_inventory_sys_id(123456) == '990001234560000000'` and `decode_inventory_id('990001234560000000') == 123456`. Both are inconsistent with the locked format `99 + InventoryId.zfill(10) + 000000`: the slice `[2:12]` of `'990001234560000000'` is `'0001234560'` which is `1234560`, not `123456`. The plan's other illustrative pair `(329960, '990003299600000000')` has the same off-by-one. The example for inv_id=1 (`'990000000001000000'` → slice `'0000000001'` → 1) is internally consistent, as is the max-boundary case (`'999999999999000000'` → slice `'9999999999'` → 9999999999). Only the middle examples had the typo.
- **Why this is Rule 1:** The plan's roundtrip invariant `decode(encode(n)) == n` and the locked `_INVENTORY_PAD = 10` constant are the load-bearing contract. The illustrative example pairs were inconsistent with both, and the plan's verify command would have failed against any correct zfill implementation. Rule 1 (auto-fix bug): correct the example values so they align with zfill semantics.
- **Fix:**
  - In `shared/synthetic_sys_id.py`: implemented zfill semantics correctly (`f"{inventory_id:0{_INVENTORY_PAD}d}"` produces canonical zfill, e.g. 123456 → '0000123456').
  - In `tests/fixtures/synthetic_fixtures.py`: corrected `SYNTHETIC_GOLDEN_CASES` pairs to `(123456, '990000123456000000')` and `(329960, '990000329960000000')`. Added an inline note explaining the deviation.
  - In `tests/test_synthetic_sys_id.py`: `test_encode_typical` asserts `encode_inventory_sys_id(123456) == "990000123456000000"` (the corrected value).
- **Format-level claim still holds:** The plan's must-have `is_synthetic_sys_id('990001234560000000') returns True` is still satisfied — the helper's format check (length=18, startswith '99', endswith '000000', all-digit) returns True for that string regardless of which InventoryId it encodes. So I did NOT change the plan's format-detection assertion; I only corrected the encode-pair / decode-pair claims that were arithmetically inconsistent with zfill.
- **Files modified:** `shared/synthetic_sys_id.py`, `tests/fixtures/synthetic_fixtures.py`, `tests/test_synthetic_sys_id.py`
- **Verification:** All 64 tests pass; round-trip property holds for `n ∈ {1, 100, 12345, 123456, 999999, 1234567890, 9999999999}`.
- **Committed in:** `0535dd86` (helper + fixtures), `baf7675e` (tests)

---

**Total deviations:** 1 auto-fixed (1 plan-typo bug)
**Impact on plan:** Necessary correction — the plan's load-bearing invariants (zfill, round-trip, locked `_INVENTORY_PAD = 10`) all stand; only inconsistent illustrative example values were corrected. No scope creep, no architectural change.

## Issues Encountered

- **Smoke verification revealed the plan-typo:** The first run of the plan's exact inline-import smoke command failed because `decode_inventory_id('990001234560000000') == 123456` was inconsistent with zfill. Resolved via the Rule 1 deviation above. No other issues encountered during execution.

## Acceptance Criteria Status

| Criterion | Status |
|-----------|--------|
| `shared/synthetic_sys_id.py` exports three functions | PASS |
| `_SYNTHETIC_PREFIX = "99"` constant present | PASS |
| `_SYNTHETIC_SUFFIX = "000000"` constant present | PASS |
| `_TOTAL_LENGTH` constant present | PASS |
| Zero `import logging` in helper module | PASS (count=0) |
| Zero bare `int(sys_id)` outside docstring (the only `int(...)` is the documented slice `int(str(sys_id)[2:12])`) | PASS |
| `tests/fixtures/synthetic_fixtures.py` exports three case lists | PASS |
| `ILLUSTRATIVE ONLY` comment count ≥ 2 | PASS (count=2) |
| Inline import smoke prints OK and exits 0 | PASS (with corrected expected values per Rule 1 deviation) |
| `tests/test_synthetic_sys_id.py` exists with all six test classes | PASS |
| `pytest tests/test_synthetic_sys_id.py -x -q` exits 0 | PASS (64 tests in 0.31s) |
| Test count ≥ 25 | PASS (64) |
| `REAL_ALMA_NEGATIVE_CASES` referenced in ≥ 3 places | PASS (count=4) |
| `TestNoIntCoercion` class present | PASS |
| `test_int_input_coerced` removed (Codex MEDIUM-1) | PASS (count=0) |

## Threat Model Validation

| Threat ID | Mitigation Applied |
|-----------|--------------------|
| T-85-01-01 (validator tampering) | Triple-check (length=18 AND startswith '99' AND endswith '000000' AND .isdigit()); explicit collision tests for real Alma `990025143260205171` and 3 other institution-suffix sys_ids in REAL_ALMA_NEGATIVE_CASES |
| T-85-01-02 (InventoryId disclosure) | Accepted by design; module docstring documents that decode is internal-tooling only |
| T-85-01-03 (int spoofing) | `str(s)` coercion in `is_synthetic_sys_id` (migration safety); `.isdigit()` check on coerced form; D-01b string discipline enforced affirmatively at call sites by `TestNoIntCoercion` repo-grep lint |
| T-85-01-04 (DOS via long input) | O(1) length check before O(n) `.isdigit()` scan; unbounded inputs return False after length-18 mismatch early-return |

## Self-Check: PASSED

Verified before writing this summary:

- `shared/synthetic_sys_id.py` exists at the worktree path
- `tests/fixtures/synthetic_fixtures.py` exists at the worktree path
- `tests/test_synthetic_sys_id.py` exists at the worktree path
- Commit `0535dd86` exists in worktree git log
- Commit `baf7675e` exists in worktree git log
- 64/64 tests pass in 0.31s; full test suite + ruff clean for new files

## Next Phase Readiness

- **Plan 02 (export-time generation)** can now `from shared.synthetic_sys_id import is_synthetic_sys_id, encode_inventory_sys_id, decode_inventory_id` and `from tests.fixtures.synthetic_fixtures import SYNTHETIC_GOLDEN_CASES, REAL_ALMA_NEGATIVE_CASES`. The helpers are ready to be called from `scripts/generate_synthetic_rows.py` (D-01a collision check at export time) and from the FJMS sidecar export script.
- **Plans 03-05** (browse / search / API surface) can call `is_synthetic_sys_id(sys_id)` at branch points to apply D-06 (quiet degradation, hide NLI elements) and D-08 (CUDL-as-default-image-source) without retrofitting any string-slicing logic.
- **D-01b drift is now CI-enforced**: the `TestNoIntCoercion` repo-grep lint will fail any future PR that introduces `int(sys_id)` coercion outside the helper module's documented slice.
- **No blockers** for Wave 2 plans.

---
*Phase: 85-synthetic-fjms-inventory-rows*
*Plan: 01*
*Completed: 2026-05-08*
