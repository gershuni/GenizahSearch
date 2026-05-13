---
phase: 87-foundations-session-uuid-and-safe-storage-chokepoint
plan: 07
subsystem: testing
tags: [phase87, lint, allowlist, acceptance-gate, ci-guard, verification-only]

# Dependency graph
requires:
  - phase: 87-01-validation-foundation
    provides: tests/test_no_raw_storage_access.py AST scanner + .planning/phase87_storage_allowlist.yaml allowlist with H1 schema
  - phase: 87-02-session-uuid-helpers
    provides: web/safe_storage.py + session UUID helpers
  - phase: 87-03-leaf-file-migrations
    provides: 5 leaf files migrated (16 sites)
  - phase: 87-04-main-and-alias-migrations
    provides: 3 central files migrated (18 sites)
  - phase: 87-05-browse-cluster-migrations
    provides: 3 browse-cluster files migrated (17 sites)
  - phase: 87-06-search-cluster-migrations
    provides: 3 search-cluster files migrated (80 sites)
provides:
  - All 6 lint scanner tests GREEN against production code
  - Acceptance gate closed for FOUND-04: zero unallowlisted raw access in web/
  - Permanent CI guard against future raw storage access regression (test runs in pytest tests/ on every CI invocation)
affects: [87-08-acceptance-and-docs, 88-state-separation, 89-lists-cache, 90-auth-caching, 91-atomic-auth-writes, 92-final-sweep]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Verification-only plan pattern: when prior waves have already brought the codebase to the target state, this plan's role is to confirm, sanity-check, and document — no source changes needed"
    - "Wave 3 confirmation gate: the orchestrator's pre-spawn check verified 6/6 lint tests + 1854/1854 pytest pass; this plan re-runs every individual acceptance criterion as a freestanding verification step"

key-files:
  created:
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-07-LINT-FINALIZATION-SUMMARY.md
  modified: []

key-decisions:
  - "No code or allowlist changes required: all 6 lint scanner tests pass GREEN against the post-Wave-2 codebase. The allowlist's 4 entries / 13 patterns / 14 expected_count exactly match the AST-counted raw access nodes in the 4 allowlisted files (auth_state=9, main=3, supabase_client=1, export_state=1)."
  - "Outcome A from the plan's decision tree applies (Step 1: 'All 6 tests pass: skip directly to Task 2'). The plan's diagnose-fix-rerun loop (Steps 2-6) was not entered because Step 1's expected baseline was met."
  - "Task 1 has no commit (verification-only outcome A); Task 2 has no commit (verification-only). The plan's final 'docs commit' captures the SUMMARY + STATE + ROADMAP updates."

patterns-established:
  - "Pattern: verification-only plan — when a Wave 3 plan's purpose is to certify the Wave 2 work, and Wave 2 already produces a clean baseline, the Wave 3 plan reduces to running each acceptance criterion as evidence and writing the SUMMARY. No source files change."
  - "Pattern: per-SC verifiable test command — every ROADMAP Success Criterion is mapped to a single pytest invocation that returns exit 0. Future regressions are caught by re-running pytest tests/ in CI."

requirements-completed: [FOUND-02, FOUND-03, FOUND-04]

# Metrics
duration: ~7min
completed: 2026-05-13
---

# Phase 87 Plan 07: Lint Finalization Summary

**Acceptance gate closed: all 6 lint scanner tests pass GREEN against production code; allowlist preserved verbatim from Plan 01 (4 entries / 13 patterns / 14 expected_count nodes — exactly matching the AST-counted reality after Plans 03-06 migrated 131 sites across 14 files). The lint scanner is now the permanent CI guard against raw `app.storage.user` regression (FOUND-04). Full pytest suite 1879 passed / 20 skipped. ruff clean. check_docs healthy.**

## Performance

- **Duration:** ~7 min
- **Started:** 2026-05-13T05:48:51Z
- **Completed:** 2026-05-13T05:55:55Z
- **Tasks:** 2 / 2 (both verification-only — no source changes)
- **Files modified:** 0
- **Files created:** 1 (this SUMMARY)
- **Commits:** 0 task commits + 1 final docs commit

## Outcome Classification

Per the plan's Task 1 decision tree (Step 1):

> **A. All 6 tests pass:** skip directly to Task 2 (no adjustments needed).

This plan landed in outcome **A**. The post-Wave-2 codebase is already in the target state. Tasks 1 and 2 reduced to pure verification — no allowlist tuning, no missed-site migration, no scanner refinement.

## Task 1: Lint Scanner Verification — Outcome A

All 6 lint scanner tests individually verified (`pytest tests/test_no_raw_storage_access.py::<test> -x`):

| Test | Result | Time |
|------|--------|------|
| `test_allowlist_well_formed` | PASSED | 0.08s |
| `test_lint_rejects_synthetic_violation` | PASSED | 0.10s |
| `test_lint_handles_aliased_imports` | PASSED | 0.11s |
| `test_lint_does_not_double_report_nested_nodes` | PASSED | 0.08s |
| `test_allowlist_counts_exact` | PASSED | 0.29s |
| `test_no_raw_storage_access_outside_allowlist` (THE BIG GATE) | PASSED | 1.07s |
| **Full file** | **6 passed in 1.17s** | |

### Allowlist Sanity (Plan Step 7)

```
Total allowlist entries: 4
Files: ['web/auth_state.py', 'web/main.py', 'web/supabase_client.py', 'web/export_state.py']
Total allowlisted patterns: 13
Total expected raw-access nodes: 14
```

Within the plan's acceptance band (4-6 entries; expected_count ≤ 25). The 14 expected raw-access nodes are explicitly distributed: auth_state.py=9, main.py=3, supabase_client.py=1, export_state.py=1.

### Per-File Production Scan (Plan Step 5 — Task 2 sanity check)

Using `tests.test_no_raw_storage_access._scan_file` against every `web/**.py` file (excluding `safe_storage.py`):

```
web/auth_state.py:       9 raw access (ALLOWLISTED)
web/main.py:             3 raw access (ALLOWLISTED)
web/supabase_client.py:  1 raw access (ALLOWLISTED)
web/export_state.py:     1 raw access (ALLOWLISTED)

Total files with raw access: 4
Sum of raw accesses: 14
Allowlist files: ['web/auth_state.py', 'web/export_state.py', 'web/main.py', 'web/supabase_client.py']
```

Zero files need migration. Zero files have unallowlisted raw access. The lint scanner's `test_no_raw_storage_access_outside_allowlist` correctly recognizes the 14 raw accesses as ALLOWLISTED and reports zero violations.

### Migrated-File Preservation Audit

Re-scanned all 14 files migrated by Plans 03-06 to confirm no regressions:

| File | Plan | Raw Access | Unallowlisted | Status |
|------|------|------------|---------------|--------|
| `web/components/text_editor.py` | 03 | 0 | 0 | Clean |
| `web/components/translation_report.py` | 03 | 0 | 0 | Clean |
| `web/pages/home.py` | 03 | 0 | 0 | Clean |
| `web/pages/settings.py` | 03 | 0 | 0 | Clean |
| `web/pages/search_results.py` | 03 | 0 | 0 | Clean |
| `web/main.py` | 04 | 3 | 0 | OAuth allowlist (3 sites) |
| `web/api.py` | 04 | 0 | 0 | Clean |
| `web/supabase_client.py` | 04 | 1 | 0 | Captured-handle allowlist (1 site) |
| `web/pages/browse.py` | 05 | 0 | 0 | Clean |
| `web/pages/browse_state.py` | 05 | 0 | 0 | Clean |
| `web/pages/catalog_browse.py` | 05 | 0 | 0 | Clean |
| `web/pages/parallels.py` | 06 | 0 | 0 | Clean |
| `web/pages/search.py` | 06 | 0 | 0 | Clean |
| `web/pages/search_state.py` | 06 | 0 | 0 | Clean |

All 12 fully-migrated files report 0 raw accesses. Both partially-allowlisted files (main.py + supabase_client.py) report only their allowlisted bootstrap sites.

## Task 2: Full Test Suite Verification

### Full pytest run

```
python -m pytest tests/ -x --tb=short
========== 1879 passed, 20 skipped, 2 warnings in 232.91s (0:03:52) ===========
```

**1879 tests pass / 20 skip / 0 fail.** This matches the orchestrator's expectation (~1878 tests expected; actual 1879 — 1 extra because test_session_uuid.py has 11 tests, not the 10 originally planned in Plan 01 — Plan 02 added `test_create_layout_mints_session_uuid` to its 10).

Two warnings observed (both pre-existing, unrelated to this plan):
- `test_parallels_api::test_parallels_malformed_json`: httpx deprecation warning on `headers, stream = encode_request(...)` (use `content=` instead). Internal to test wiring.
- `test_search_api_v2::test_posthog_event_carries_search_mode_value_for_exact`: thread exception from PostHog drain queue using `FakeQueue.get()` shim. Pre-existing test fixture, not caused by Plan 07.

Neither warning affects test outcomes.

### Ruff lint clean

```
ruff check .
All checks passed!
```

### Docs health green

```
PYTHONIOENCODING=utf-8 python scripts/check_docs.py
✅ All critical documents exist
✅ No outdated terms found
✅ All documents updated within 90 days
✅ All internal links valid
✅ All checks passed! Documentation is healthy.
```

(Note: bare `python scripts/check_docs.py` errors on Windows because cp1255 console codepage cannot encode the U+1F4C1 folder emoji in the script's print statements. Setting `PYTHONIOENCODING=utf-8` bypasses the issue. The script's logic itself is correct — only the terminal output encoding is locale-dependent.)

### Phase 87 final gate (23 tests across 3 files)

```
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v
========== 23 passed in 2.58s ==========
```

Breakdown:
- `tests/test_safe_storage.py`: 6 passed (FOUND-05 invariant preserved)
- `tests/test_session_uuid.py`: 11 passed (FOUND-01 + M5 validation + B1 wiring)
- `tests/test_no_raw_storage_access.py`: 6 passed (FOUND-04 acceptance gate)

### FOUND-05 invariant (test_safe_storage.py byte-stable)

```
git diff --stat tests/test_safe_storage.py
(empty output — file unchanged)
```

This plan did NOT touch `tests/test_safe_storage.py`. The Plan 01 baseline SHA-256 (`e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f`) remains verified by git-diff equivalence.

### test_session_uuid.py byte-stable

```
git diff --stat tests/test_session_uuid.py
(empty output — file unchanged)
```

Plan 07 did not modify `tests/test_session_uuid.py`. Plan 02's wiring tests + Plan 01's M5 validation tests remain intact.

## 5 ROADMAP Success Criteria — Final Verification

Each SC mapped to a single pytest command, all GREEN:

| SC | Description | Test Command | Result |
|----|-------------|--------------|--------|
| **SC1** | 100 sessions never share UUID | `pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions` | PASS (1.42s) |
| **SC2** | Static scan of `web/` returns only allowlisted entries | `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist` | PASS (1.02s) |
| **SC3** | Allowlist file has per-entry justification AND H1 expected_count | `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed` | PASS (0.10s) |
| **SC4** | Lint rejects synthetic violation; passes production code | `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation` + `::test_no_raw_storage_access_outside_allowlist` + `::test_allowlist_counts_exact` | PASS (each) |
| **SC5** | All 6 existing safe_storage tests pass unchanged | `pytest tests/test_safe_storage.py` + `git diff --stat tests/test_safe_storage.py` empty | PASS (6 passed in 1.29s; diff empty) |

All 5 Phase 87 ROADMAP success criteria satisfied via automated test commands.

## Allowlist Composition (Final)

The allowlist is preserved verbatim from Plan 01. No additions, removals, or expected_count adjustments were needed in Plan 07 — Plans 03-06 migrated exactly the sites the allowlist expected to be NOT included, and left exactly the bootstrap sites the allowlist enumerates.

| File | Patterns | expected_count sum | Eliminated by | Status |
|------|----------|---------------------|----------------|--------|
| `web/auth_state.py` | 8 | 9 (PROFILE_KEY counts 2) | Phase 91 AUTHW-01 | Bootstrap — atomic auth-write refactor |
| `web/main.py` | 3 | 3 | Phase 91 AUTHW-02 | Bootstrap — OAuth callback 3-key atomic write |
| `web/supabase_client.py` | 1 | 1 | Phase 90 AUTHC-01 | Bootstrap — captured-handle pattern (line 111) |
| `web/export_state.py` | 1 | 1 | Phase 88 STATE-04 | Bootstrap — `_TEST_BACKEND` shim fallthrough |
| **Total** | **13** | **14** | | |

Every entry has a multi-line `justification:` block citing the downstream phase that eliminates it. Every pattern has an `expected_count:` integer enforcing exact match counts. The H1 schema (source + expected_count per pattern) is uniformly applied.

## Cumulative Phase 87 Site Count

| Plan | Files migrated | Sites eliminated | Status |
|------|----------------|------------------|--------|
| Plan 01 (validation foundation) | 0 (test infra) | 0 | Landed |
| Plan 02 (session UUID helpers) | 0 (additive) | 0 | Landed |
| Plan 03 (leaf files) | 5 | 16 | Landed |
| Plan 04 (main + aliases) | 3 | 18 (+ 2 helper deletions, 18 caller routings) | Landed |
| Plan 05 (browse cluster) | 3 production + 1 test | 17 | Landed |
| Plan 06 (search cluster) | 3 production + 1 test | 80 | Landed |
| **Plan 07 (lint finalization)** | **0** | **0** (verification only) | **Landed** |
| **Phase 87 total** | **14 production + 2 test** | **131 sites** | **6/7 plans complete** |

131 raw access sites migrated; 14 remain (all allowlisted, scoped to bootstrap code that downstream phases 88-91 explicitly target).

## Sites That Required Migration in Plan 07

**Zero.** Plans 03-06 did not miss any sites. Every site that should have been migrated, was migrated. Every site that should have been allowlisted, was allowlisted. The plan's diagnose-fix-rerun loop (outcomes B/C/D/E) was never entered.

## Allowlist Entries Added Beyond Plan 01

**Zero.** The 4 original entries from Plan 01 remained sufficient. No new bootstrap sites surfaced during Plans 03-06.

## Decisions Made

- **No source changes:** Tasks 1 and 2 are pure verification. Plans 03-06 already brought the codebase to the acceptance state; Plan 07's job was to confirm and certify.
- **Outcome A path taken:** The plan's decision tree explicitly accommodates this case ("All 6 tests pass: skip directly to Task 2"). Following outcome A means no diagnosis, no migration, no allowlist tuning.
- **`check_docs.py` Windows encoding workaround documented:** The script's emoji print statements (`📁`, `🔍`, etc.) fail under cp1255 console encoding; `PYTHONIOENCODING=utf-8` fixes it. The script's logic is correct — the issue is purely terminal-output encoding. Verified docs are healthy under UTF-8 output.

## Deviations from Plan

None. The plan was a pure verification gate, and the verification passed on every check. The only minor friction was the `check_docs.py` Windows encoding quirk (resolved by `PYTHONIOENCODING=utf-8`) — this is not a deviation from the plan's expected behavior, just a Windows-specific invocation detail.

**Total deviations:** 0.
**Impact on plan:** None. All 2 tasks executed exactly as specified.

## Issues Encountered

- **`check_docs.py` Unicode encoding under Windows cp1255 codepage:** Setting `PYTHONIOENCODING=utf-8` resolves it. Not a Plan 07 regression — pre-existing Windows-locale interaction.

No other issues. All 6 lint tests pass first run; all 1879 pytest tests pass first run; ruff clean first run; per-file scan matches the expected baseline first run.

## User Setup Required

None. Pure verification plan — no code, config, env-var, or DB change.

## Threat Flags

None. This plan changes no code surface. It certifies that the codebase reached the target state declared by Plan 01.

Per the plan's `<threat_model>`:
- **T-87-04 (Tampering, allowlist drift)** — mitigated. `test_allowlist_well_formed` enforces every entry has a non-empty `justification` and every pattern has an `expected_count`. The 13 patterns / 14 expected_count / 4 entries are exactly what Plan 01 specified.
- **T-87-04b (Tampering, allowlist over-expansion)** — mitigated. `test_allowlist_counts_exact` enforces exact match counts. Any future raw access that substring-matches an existing pattern will bump the actual count above `expected_count`, failing the test until the YAML is updated (which requires explicit code review).
- **T-87-05 (Information disclosure, alias resolution)** — mitigated. `test_lint_handles_aliased_imports` continues to cover the 3 known aliases. Future code paths adding new aliases would require a `from nicegui import app as X` import that the AST scanner detects.

## Phase 87 ROADMAP Progress After Plan 07

| Plan | Status | Notes |
|------|--------|-------|
| 87-01 (validation foundation) | Landed | Test scaffolding + H1 schema + 4-entry allowlist |
| 87-02 (session UUID helpers) | Landed | safe_storage.py extended + B1 wiring |
| 87-03 (leaf files) | Landed | 5 files, 16 sites |
| 87-04 (main + alias) | Landed | 3 files, 18 sites + 2 helper deletions |
| 87-05 (browse cluster) | Landed | 3 production + 1 test, 17 sites |
| 87-06 (search cluster) | Landed | 3 production + 1 test, 80 sites |
| **87-07 (lint finalization)** | **Landed** | **Acceptance gate closed — all 6 lint tests GREEN; 131 sites cleaned + 14 allowlisted; full suite 1879/1879** |
| 87-08 (acceptance and docs) | Pending | Final phase documentation + closeout |

Phase 87 is 7 of 8 plans complete. The remaining plan (87-08) is documentation/closeout only — the technical work is finished.

## Next Phase Readiness

**Plan 87-08 (Acceptance and Docs) is unblocked.** With all 6 lint tests GREEN and all 23 Phase 87 invariant tests passing, Plan 08 can proceed to final phase documentation: CHANGELOG entry, OPEN_ISSUES updates, completion of the phase-level milestone tracking.

**Phase 88 (State Separation by Deletion) is unblocked once 87-08 closes.** Phase 88 will delete the `_TEST_BACKEND` shim that holds open the export_state.py allowlist entry, which then self-eliminates.

**Phases 90 (Auth Caching) and 91 (Atomic Auth Writes)** will self-eliminate the supabase_client.py and auth_state.py + main.py OAuth allowlist entries respectively. After all of Phases 88-91 land, the allowlist YAML's 4 entries collapse to 0, and the lint test continues to enforce zero raw access permanently.

**Blockers/Concerns:** None.

## Self-Check: PASSED

- File `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-07-LINT-FINALIZATION-SUMMARY.md` exists. ✅ FOUND (just written)
- All 6 lint scanner tests pass GREEN. ✅ FOUND (6 passed in 1.17s)
- All 23 Phase 87 invariant tests pass GREEN. ✅ FOUND (23 passed in 2.58s)
- Full pytest suite passes (1879 / 20 skipped / 0 failed). ✅ FOUND
- ruff clean. ✅ FOUND
- check_docs healthy (with UTF-8 encoding). ✅ FOUND
- Allowlist is 4 entries / 13 patterns / 14 expected_count nodes — matches Plan 01 baseline exactly. ✅ FOUND
- 14 migrated production files (Plans 03-06) all confirmed clean or correctly allowlisted. ✅ FOUND
- tests/test_safe_storage.py byte-unchanged (git diff empty). ✅ FOUND
- tests/test_session_uuid.py byte-unchanged (git diff empty). ✅ FOUND
- All 5 ROADMAP Success Criteria satisfied via automated test commands. ✅ FOUND

---
*Phase: 87-foundations-session-uuid-and-safe-storage-chokepoint*
*Plan: 07 - Lint Finalization*
*Completed: 2026-05-13*
