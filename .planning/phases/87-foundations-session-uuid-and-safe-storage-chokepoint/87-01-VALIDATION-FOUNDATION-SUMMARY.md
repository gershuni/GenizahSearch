---
phase: 87-foundations-session-uuid-and-safe-storage-chokepoint
plan: 01
subsystem: testing
tags: [phase87, validation, test-skeleton, nicegui, storage, ast-lint, pyyaml]

# Dependency graph
requires:
  - phase: phase-86-cudl-coverage-audit
    provides: stable web/safe_storage.py module + safe_user_get/set/pop helpers (the chokepoint this plan validates)
provides:
  - tests/test_session_uuid.py with 10 failing-stub tests for FOUND-01 (5 base + 4 M5 validation + 1 B1-residual route-coverage)
  - tests/test_no_raw_storage_access.py with 6 tests (AST lint scanner, allowlist well-formed, synthetic-violation guard, alias resolution guard, parent-tracking guard, H1 expected_count enforcement, production scan)
  - .planning/phase87_storage_allowlist.yaml with 4 bootstrap-site entries (auth_state.py x8 patterns, main.py x3 OAuth-callback patterns, supabase_client.py x1 captured-handle, export_state.py x1 _TEST_BACKEND fallthrough)
  - B2-corrected AST chain walker (inner-first order, `chain[-2:] == ['user', 'storage']`) with NodeVisitor parent tracking (`_seen_inner_ids`)
  - H1 schema: each pattern is a dict with `source` + `expected_count`, enforced by `test_allowlist_counts_exact` (with Fix 3 loud-fail when nicegui import is missing)
  - PyYAML 6.0.3 availability confirmed
  - Baseline SHA-256 of tests/test_safe_storage.py recorded for FOUND-05 invariant tracking
affects: [87-02-session-uuid-helpers, 87-03-leaf-file-migrations, 87-04-main-and-alias-migrations, 87-05-browse-cluster-migrations, 87-06-search-cluster-migrations, 87-07-lint-finalization, 87-08-acceptance-and-docs]

# Tech tracking
tech-stack:
  added: [PyYAML (transitive via NiceGUI; now explicitly relied on by tests)]
  patterns:
    - "AST NodeVisitor with parent tracking via id(node) set to prevent double-reporting nested Call/Subscript/Attribute chains"
    - "Inner-first attribute chain walking with explicit chain order verification (chain[-2:] == ['user', 'storage'])"
    - "Allowlist schema: source-substring + expected_count for exact-match scope enforcement"
    - "Route-coverage regression guard: parse web/main.py AST, assert every @ui.page handler wires create_layout() or ensure_session_uuid()"

key-files:
  created:
    - tests/test_session_uuid.py (215 lines, 10 tests)
    - tests/test_no_raw_storage_access.py (371 lines, 6 tests)
    - .planning/phase87_storage_allowlist.yaml (135 lines, 4 entries, 13 patterns)
  modified: []

key-decisions:
  - "Wave 0 = failing-test gate. All 10 test_session_uuid.py tests fail by design (9 with ImportError on get_session_uuid, 1 with AssertionError naming /reset-hints and /auth/callback). Plan 02 makes the first 9 green; the 10th gates Plan 02's wiring."
  - "Allowlist matcher uses substring containment against ast.get_source_segment output. Patterns must be substrings of the AST node's source segment (NOT of the full source line). For bare Attribute access like `return app.storage.user`, the segment is `app.storage.user` (no return keyword)."
  - "Route-coverage test 10 may already be RED at Wave 0 (and IS — names /reset-hints and /auth/callback as missing wiring). This is intentional: it gates Plan 02 from regressing the bootstrap wiring."
  - "Buggy `chain[-2:] == ['storage', 'user']` string is intentionally retained in docstrings as B2-regression documentation, but the actual code check uses the corrected `['user', 'storage']` form."

patterns-established:
  - "Pattern: failing-test scaffolding ahead of helper implementation — tests reference web.safe_storage.get_session_uuid and ensure_session_uuid before they exist; the ImportError at Wave 0 is the evidence that the skeleton is correctly wired."
  - "Pattern: allowlist as source-of-truth for security-relevant exception scoping — every entry requires file path, multi-line justification referencing the eliminating downstream phase, and per-pattern expected_count for tamper detection."

requirements-completed: [FOUND-01, FOUND-03, FOUND-04]

# Metrics
duration: ~6min
completed: 2026-05-13
---

# Phase 87 Plan 01: Validation Foundation Summary

**Wave 0 failing-test gate established — 16 test stubs + 4-entry YAML allowlist with H1 expected_count schema scaffold every downstream plan's pass/fail signal.**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-05-13T04:56:16Z
- **Completed:** 2026-05-13T05:01:38Z
- **Tasks:** 3 / 3
- **Files created:** 3 (721 lines total)
- **Files modified:** 0

## Accomplishments

- Created 10-test failing-stub file (`tests/test_session_uuid.py`) covering FOUND-01 SC1 (100-session uniqueness), M5 validation (uppercase/non-string/malformed/AssertionError-on-write rejection), and B1-residual route-coverage (every `@ui.page` handler in `web/main.py` must wire `create_layout()` or `ensure_session_uuid()`, with `/privacy-extension` exempt).
- Created 6-test AST lint scanner (`tests/test_no_raw_storage_access.py`) with B2-corrected inner-first chain walking, NodeVisitor parent tracking against double-reporting, alias resolution for `app` / `nicegui_app` / `_app`, and H1 `test_allowlist_counts_exact` enforcing per-pattern expected_count (with Fix 3 loud-fail when an allowlisted file loses its nicegui app import).
- Created 4-entry allowlist YAML (`.planning/phase87_storage_allowlist.yaml`) covering the 4 known bootstrap sites: `web/auth_state.py` (8 distinct patterns × 9 access sites), `web/main.py` (OAuth callback 3-key atomic write at 1458/1460/1463), `web/supabase_client.py:111` (captured-handle pattern), `web/export_state.py:48` (`_TEST_BACKEND` production fallthrough). Each justification references the downstream phase that eliminates it (91 / 91 / 90 / 88).
- Confirmed PyYAML 6.0.3 importable (L2 gate).
- Confirmed `tests/test_safe_storage.py` byte-unchanged from baseline (FOUND-05 invariant): SHA-256 `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Create tests/test_session_uuid.py with 10 unit-test stubs** — `cbbbac75` (test)
2. **Task 2: Create .planning/phase87_storage_allowlist.yaml with 4 bootstrap-site entries** — `7b4b1fdd` (docs)
3. **Task 3: Create tests/test_no_raw_storage_access.py with AST scanner + 6 tests** — `38362597` (test)

**Plan metadata:** *(pending — added in final docs commit)*

## Files Created/Modified

- `tests/test_session_uuid.py` (215 lines) — 10 failing tests for FOUND-01 session-UUID helpers. References `web.safe_storage.get_session_uuid` and `ensure_session_uuid` (do not exist until Plan 02). The 10th test parses `web/main.py` AST to enforce that every `@ui.page` handler wires session-UUID minting via `create_layout()` or direct call.
- `tests/test_no_raw_storage_access.py` (371 lines) — AST lint scanner. `_StorageAccessVisitor(ast.NodeVisitor)` with `_seen_inner_ids` parent tracking. `_walk_attribute_chain` returns inner-first chain. `_matches_storage_user_access` checks `chain[-2:] == ['user', 'storage']` (B2 corrected order). 6 tests: 4 standalone-passing + 2 production-scanning-failing (Wave 0 expected).
- `.planning/phase87_storage_allowlist.yaml` (135 lines) — 4 entries, 13 patterns total. Schema: `file:` + `patterns: [{source, expected_count, enclosing?}]` + `justification:` (multi-line, mandatory per FOUND-03). Patterns substring-match against `ast.get_source_segment` output.

## B2 Verification (corrected AST chain order)

- `_matches_storage_user_access` function body (from `ast.unparse`): `return len(chain) >= 2 and chain[-2:] == ['user', 'storage']` — correct inner-first order.
- `test_lint_rejects_synthetic_violation` PASSED at Wave 0 (proves the scanner detects `app.storage.user.get('foo')`).
- `test_lint_does_not_double_report_nested_nodes` PASSED at Wave 0 (proves parent tracking via `_seen_inner_ids` prevents the inner Attribute from being reported again after a Call/Subscript consumes it — verified by 4 statements producing exactly 4 violations).
- The string `chain[-2:] == ['storage', 'user']` appears 2 times in the file BUT only inside docstrings documenting the B2 fix (lines 19, 70). The actual code check (line 90) uses the corrected `['user', 'storage']` form. Verified by AST inspection of `_matches_storage_user_access`.

## H1 Verification (expected_count schema)

- All 13 patterns across 4 allowlist entries are dicts with both `source` and `expected_count` keys. Verified by `python -c "import yaml, pathlib; data = yaml.safe_load(...); assert all(isinstance(p, dict) and 'source' in p and 'expected_count' in p for e in data['allowed_raw_access'] for p in e['patterns'])"` (printed `OK`).
- `test_allowlist_counts_exact` is present (function `test_allowlist_counts_exact` exists in test file, verified by AST scan).
- `test_allowlist_counts_exact` FAILS at Wave 0 with mismatch: `web/supabase_client.py: pattern '_app.storage.user' expected_count=1 but found 2 matching AST nodes`. The 2nd match comes from line 263 in `sign_out` (`_app.storage.user.get('auth_session')`), which Plan 04 migrates — once that migration lands, the count will be 1 and the test will go GREEN.
- Fix 3 regression path (loud-fail when allowlisted file's nicegui import is removed) implemented: lines 306-314 in test file iterate `entry['patterns']` and append to `mismatches` whenever `expected_count > 0` but `aliases` is empty.

## M5 Verification (UUID validation tests)

The 4 new validation tests are present in `tests/test_session_uuid.py`:

| Test | Poisoned value(s) | Expected behavior |
|------|-------------------|-------------------|
| `test_session_uuid_rejects_uppercase_hex` | `'ABCDEF...90'` (32 char uppercase) | Reject → mint fresh lowercase |
| `test_session_uuid_rejects_non_string` | `12345`, `None`, `{...}`, `[1,2,3]`, `b'bytes'` | Reject all → mint fresh string |
| `test_session_uuid_rejects_malformed_length` | `'short'`, `'a'*31`, `'a'*33`, `'!'*32`, `'g'*32`, `'0'*31 + ' '` | Reject all → mint fresh 32-hex |
| `test_ensure_session_uuid_returns_false_on_assertion` | `__setitem__` raises AssertionError | Return False, do not raise |

All 4 fail with ImportError at Wave 0 (expected — `get_session_uuid` does not exist until Plan 02).

## L2 Verification (PyYAML available)

- `python -c "import yaml; print(yaml.__version__)"` → `6.0.3` (exits 0).
- PyYAML is reachable in the project's Python environment without explicit installation — comes transitively via NiceGUI. No requirements.txt edit needed.

## Test Run Summary

| File | Total | Pass at Wave 0 | Fail at Wave 0 (expected) |
|------|-------|----------------|---------------------------|
| `tests/test_session_uuid.py` | 10 | 0 | 10 (9 ImportError, 1 AssertionError naming /reset-hints + /auth/callback) |
| `tests/test_no_raw_storage_access.py` | 6 | 4 (well_formed, synthetic, aliased, no_double_report) | 2 (production scan: 131 violations; counts test: supabase_client.py mismatch) |

Test 10 (`test_every_ui_page_handler_mints_uuid`) names the 2 routes Plan 02 must wire:
- `/reset-hints` (line 1280): no `create_layout()` or `ensure_session_uuid()` call
- `/auth/callback` (line 1437): no `create_layout()` or `ensure_session_uuid()` call

All 19 other `@ui.page` handlers in `web/main.py` already route through `create_layout()`, so they pre-satisfy the test once `create_layout()` calls `ensure_session_uuid()`.

## FOUND-05 Invariant (test_safe_storage.py byte-identical)

- Baseline SHA-256 (taken at plan start): `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f`
- Post-plan SHA-256 (taken before final commit): `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f`
- Match: TRUE. Subsequent plans should re-verify this hash at their start.

## Allowlist Entry Summary

| File | Patterns | Total expected_count | Justification gist | Eliminated by |
|------|----------|----------------------|---------------------|----------------|
| `web/auth_state.py` | 8 | 9 (PROFILE_KEY appears twice) | `GlobalAuthState` class methods already in try/except; atomic auth-key writes need coordinated refactor | Phase 91 AUTHW-01 |
| `web/main.py` | 3 | 3 | OAuth callback at 1458-1463 is a 3-key atomic block; half-login state is worse than no-login | Phase 91 AUTHW-02 |
| `web/supabase_client.py` | 1 | 1 | Captured-handle pattern at line 111 inside `get_user_client()` — entire function deleted in Phase 90 | Phase 90 AUTHC-01 |
| `web/export_state.py` | 1 | 1 | `_backend()` production fallthrough at line 48 for `_TEST_BACKEND` shim — entire shim deleted | Phase 88 STATE-04 |

## Decisions Made

- **Allowlist matcher contract:** substring match against `ast.get_source_segment` of the violation node, NOT against the full source line. For bare Attribute access (`return app.storage.user`), the segment is just `app.storage.user`. This is documented explicitly in the YAML header and the test file docstring.
- **`enclosing:` field is documentation-only in this revision.** A future strict-mode could walk parent nodes to verify the function name; for now, it documents human-readable intent.
- **Buggy chain check documented in docstrings**: the strings `chain[-2:] == ['storage', 'user']` appear in test file docstrings as B2-regression documentation. The actual executed code uses `['user', 'storage']` (verified via AST inspection of `_matches_storage_user_access`).

## Deviations from Plan

### Acceptance Criterion Reconciliation (informational, no code change)

**1. [Rule N/A — interpretation] Acceptance criterion "File does NOT contain `chain[-2:] == ['storage', 'user']`" interpreted at code level, not raw-text level**
- **Found during:** Task 3 (verification)
- **Issue:** A literal substring scan rejected the file because the buggy form appears twice in docstrings documenting the B2 fix.
- **Interpretation:** The plan's `<context>` block (`<ast_chain_facts>`) explicitly retains the buggy form as documentation. The acceptance criterion's intent is that the *executed code* must not implement the buggy check. The function body of `_matches_storage_user_access` (verified via `ast.unparse`) uses only the corrected `['user', 'storage']` form. The 2 docstring occurrences are intentional B2-regression documentation, consistent with the plan's `<ast_chain_facts>` block.
- **Files modified:** None — no code change.
- **Verification:** `ast.unparse` of `_matches_storage_user_access` shows only `chain[-2:] == ['user', 'storage']`. Documented in this SUMMARY under "B2 Verification".

**Total deviations:** 0 auto-fixes (1 informational reconciliation only).
**Impact on plan:** No scope creep. All 3 tasks completed exactly as specified. The reconciliation note clarifies how the acceptance criterion applies in the presence of B2-regression documentation that the plan itself authored.

## Issues Encountered

None. The 4 standalone lint tests pass on first run; the 2 production-scanning tests fail as documented Wave 0 expected state.

## User Setup Required

None — no external service configuration required for Wave 0 scaffolding.

## Next Phase Readiness

**Plan 02 (Session UUID Helpers) is unblocked.** Plan 02 will implement `web/safe_storage.py::get_session_uuid()` and `ensure_session_uuid()`, then wire `ensure_session_uuid()` into `create_layout()` plus `/reset-hints` and `/auth/callback`. Once Plan 02 lands:

- All 10 tests in `tests/test_session_uuid.py` should turn GREEN.
- Specifically, `test_every_ui_page_handler_mints_uuid` will pass once `/reset-hints` and `/auth/callback` are wired (the only 2 routes flagged at Wave 0).

**Plans 03-06 will independently work on raw-access migrations.** Their pass/fail signal is `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist` (currently RED with 131 violations) and `test_allowlist_counts_exact` (currently RED on supabase_client.py count). By Plan 07, both must be GREEN.

**Blockers/Concerns:** None. PyYAML 6.0.3 confirmed importable; no requirements.txt edit needed.

## Self-Check: PASSED

- File `tests/test_session_uuid.py` exists. ✅ FOUND
- File `tests/test_no_raw_storage_access.py` exists. ✅ FOUND
- File `.planning/phase87_storage_allowlist.yaml` exists. ✅ FOUND
- Commit `cbbbac75` (Task 1) exists in git log. ✅ FOUND
- Commit `7b4b1fdd` (Task 2) exists in git log. ✅ FOUND
- Commit `38362597` (Task 3) exists in git log. ✅ FOUND

---
*Phase: 87-foundations-session-uuid-and-safe-storage-chokepoint*
*Plan: 01 - Validation Foundation*
*Completed: 2026-05-13*
