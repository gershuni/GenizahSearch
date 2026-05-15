---
phase: 91-atomic-auth-state-writes
plan: 01
subsystem: web/auth
tags:
  - auth
  - safe_storage
  - oauth
  - multitenant
  - phase-87-allowlist
  - phase-91
  - authw-01
  - authw-02
  - authw-05
dependency_graph:
  requires:
    - phase-87-safe_storage-chokepoint
    - phase-90-auth-caching-rewrite
  provides:
    - empty-phase-87-allowlist
    - oauth-callback-resilience-tests
    - atomic-set_auth-with-symmetric-rollback
    - defensive-3-key-cleanup-at-caller-level
    - method-tagged-posthog-telemetry
  affects:
    - web/auth_state.py
    - web/main.py
    - tests/test_no_raw_storage_access.py
    - tests/test_auth_callback_resilience.py
    - .planning/phase87_storage_allowlist.yaml
tech_stack:
  added:
    - none (pure refactor + new test file; no new runtime dependencies; pytest-asyncio NOT introduced per Revision MUST-1)
  patterns:
    - multi-write-rollback-discipline (Phase 91 D-04/D-05/D-06)
    - symmetric-2-key-rollback-at-set_auth (Revision MUST-2; user/profile owned by set_auth)
    - defensive-3-key-cleanup-at-caller (Revision MUST-2; do_login/_oauth_complete_login own auth_session)
    - profile-is-None-clears-stale-profile (Revision MUST-2)
    - asyncio.run-test-pattern (Revision MUST-1; avoids pytest-asyncio dep)
    - monkeypatch.setattr-string-form (Phase 87 B3 + Phase 90 D-17)
    - _RoutingStorage-dict-subclass-with-stale-preseeding (NEW for Phase 91 T-D/T-E)
key_files:
  created:
    - tests/test_auth_callback_resilience.py
  modified:
    - web/auth_state.py
    - web/main.py
    - tests/test_no_raw_storage_access.py
    - .planning/phase87_storage_allowlist.yaml
decisions:
  - D-01 substituted 9 raw app.storage.user accesses in web/auth_state.py with safe_user_get/set/pop
  - D-04 (REVISED MUST-2) set_auth returns bool with SYMMETRIC 2-key user/profile rollback + profile-is-None-clears-stale semantics
  - D-05 (REVISED MUST-2) do_login session-first ordering + DEFENSIVE 3-key cleanup on set_auth failure + NEW-L2 method=password posthog tags
  - D-06 (REVISED MUST-2) _oauth_complete_login factored to module-level helper + DEFENSIVE 3-key cleanup
  - D-07 / F3 replaced hard-assert allowlist-non-empty with explanatory comment
  - D-07b emptied .planning/phase87_storage_allowlist.yaml to `allowed_raw_access: []` (2 entries -> 0)
  - D-08 + MUST-3 installed 6 resilience tests + 1 companion (T-A through T-F)
  - NEW-H1 added module-top `from web.auth_state import GlobalAuthState` to web/main.py
  - NEW-H3 omitted top-level `import pytest` from new test file (monkeypatch is fixture-name-injected)
  - NEW-H4 + NEW-H5 pre-seeded T-D / T-E storage stubs with stale prior-session data (proves rollback CLEARS, not merely leaves-empty)
  - NEW-L2 added 'method': 'password' tag to all do_login posthog events for parity with _oauth_complete_login's 'method': 'google_oauth'
  - Deviation Rule-1: NEW-H2 ('drop nicegui.app from auth_state.py import') NOT applied — `app.storage.browser.*` references in `create_login_dialog` still need `app`. Plan claim was incorrect.
metrics:
  duration: ~50min (worktree wall-time)
  completed: 2026-05-15
  tasks_completed: 5
  files_created: 1
  files_modified: 4
  tests_added: 7
  full_suite_passed: 1956
  full_suite_skipped: 21
  full_suite_failed: 0
---

# Phase 91 Plan 01: Atomic Auth State Writes Summary

**One-liner:** Migrate 12 remaining raw `app.storage.user` accesses in `web/auth_state.py` + `web/main.py:complete_login` to `safe_storage` chokepoint helpers, install symmetric set_auth rollback + defensive caller-level cleanup against multi-write atomicity gaps, factor `_oauth_complete_login` as a module-level testability seam, install 7 OAuth-callback resilience tests, and self-eliminate the last 2 Phase 87 allowlist entries (allowlist 2 → 0).

## Summary

Plan 91-01 closes the v7.12 Path B multitenant refactor's auth-write atomicity gap. Phase 87 created the `safe_storage` chokepoint and Phase 88/90 migrated 131+ access sites; Phase 91 finishes the job for the last 12 raw `app.storage.user` accesses (9 in `web/auth_state.py`, 3 in `web/main.py:complete_login`). On top of the mechanical migration, the plan installs multi-write rollback discipline so a prune-race mid-login surfaces a user-visible "Session storage unavailable" error instead of either a 500 (raw AssertionError propagating up) or a stale-role leak (where a successful `auth_user` write but failed `auth_profile` write would leave a prior session's admin profile observable via `GlobalAuthState.get_role()`).

The rollback design is layered (NEW-M1 wording):
- **`set_auth` does SYMMETRIC 2-key rollback** (`USER_KEY` + `PROFILE_KEY` only; `auth_session` is the outer caller's responsibility, NOT set_auth's). On profile-write failure, BOTH the new auth_user write AND any stale auth_profile are popped.
- **`do_login` / `_oauth_complete_login` (callers) do DEFENSIVE 3-key cleanup** (`auth_session` + `auth_user` + `auth_profile`). When `set_auth` returns False, all 3 keys are popped at the caller level — defensive against the case where `set_auth`'s own SYMMETRIC rollback also failed during a prune race.

The `set_auth(user, profile=None)` semantics shift from "skip the profile branch" (Phase 90 behavior, which left any stale prior-session profile observable) to "clear stale profile" (Phase 91; `safe_user_pop(PROFILE_KEY, None)` on the `profile is None` branch). This closes Codex's HIGH catch from round-1 reviews.

## Tasks Completed

| Task | Description | Commit |
|------|-------------|--------|
| 1 | Migrate web/auth_state.py: 9 raw accesses → safe_storage helpers; set_auth returns bool with SYMMETRIC 2-key rollback; do_login session-first + DEFENSIVE 3-key cleanup; NEW-L2 method-tagged posthog | 656e5a17 |
| 2 | Add NEW-H1 module-top `from web.auth_state import GlobalAuthState` import to web/main.py; factor `_oauth_complete_login` module-level helper; migrate 3 raw OAuth-callback writes → safe_user_set with DEFENSIVE 3-key cleanup | 74712a87 |
| 3 | Empty Phase 87 allowlist (2 → 0); D-07 relax `test_allowlist_well_formed` hard-assert to explanatory comment | af28cc8a |
| 4 | Install tests/test_auth_callback_resilience.py with T-A/T-B/T-C/T-D/T-E/T-F + companion = 7 tests using asyncio.run() (Revision MUST-1), no top-level pytest import (NEW-H3), NEW-H4 + NEW-H5 stale pre-seeding | 0c4cda29 |
| 5 | Plan-boundary verification (no files modified): 5 migration audits + test-fixture audit (MAY-9) + lint scanner + full pytest + ruff — all green | (verification only) |

## Revision Items Applied

### Round 1 (MUST items)
- **MUST-1** (no pytest-asyncio dependency): tests use plain `def test_*(...)` with `asyncio.run(_oauth_complete_login(...))` rather than `@pytest.mark.asyncio` + `async def`.
- **MUST-2** (SYMMETRIC 2-key set_auth + DEFENSIVE 3-key caller cleanup + profile-is-None-clears-stale): encoded in Task 1's `set_auth` shape + Task 1's `do_login` outer cleanup + Task 2's `_oauth_complete_login` outer cleanup.
- **MUST-3** (partial-write rollback tests T-D, T-E, T-F): 3 new tests + `_RoutingStorage` helper class with stale pre-seeding.
- **MUST-4** (show_error audit): Performed inline in plan's `<audit_show_error>` block; `show_error` encapsulates 6 UI/observability ops (posthog, spinner hide, status_label hide, error_label show, home_btn show); passing it directly as `show_error_fn` is safe.

### Round 2 (NEW-H/M/L items)
- **NEW-H1** (`_oauth_complete_login` NameError on GlobalAuthState): added `from web.auth_state import GlobalAuthState` to `web/main.py` module-top import block.
- **NEW-H2** (drop `nicegui.app` from auth_state.py): **NOT applied — Rule 1 deviation. See Deviations section.**
- **NEW-H3** (no top-level `import pytest`): omitted from `tests/test_auth_callback_resilience.py`; `monkeypatch` is auto-injected by pytest at fixture-name resolution.
- **NEW-H4** (T-D stale auth_profile pre-seeding): canonical AFTER block from 91-CODEX-HFIXES.md verbatim — T-D now seeds `{'auth_profile': {'role': 'admin', 'username': 'old_admin'}}` and asserts `GlobalAuthState.get_role() is None` post-failure.
- **NEW-H5** (T-E stale auth_user + auth_profile pre-seeding): canonical AFTER block from 91-CODEX-HFIXES.md verbatim — T-E seeds both `'auth_user'` and `'auth_profile'` from a prior session AND configures `'auth_user'` writes to fail; asserts all 3 keys absent post-failure.
- **NEW-M1** (wording: SYMMETRIC 2-key vs DEFENSIVE 3-key): all rollback references throughout the migration reflect the 2-key set_auth + 3-key caller-level layered design.
- **NEW-M2** (PowerShell-friendly verification commands): all verification commands cross-shell (Python one-liners + AST checks, not Unix `grep | tail` pipelines).
- **NEW-L1** (double-posthog disposition deferred): the `show_error` closure's posthog_capture('login_failed', ...) + `_oauth_complete_login`'s direct posthog_capture both fire on partial-write failure. Acceptable for Phase 91; PostHog dashboard consumers can filter on the rich reason tag. Single-event consolidation deferred to Phase 92 / future telemetry-polish phase.
- **NEW-L2** (method-tagged posthog in do_login): all 4 login_failed posthog events + the login_success event in `do_login` now carry `'method': 'password'` for parity with `_oauth_complete_login`'s `'method': 'google_oauth'`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Plan premise error] NEW-H2 `nicegui.app` import drop NOT applied**

- **Found during:** Task 1 (web/auth_state.py imports edit)
- **Issue:** The plan's NEW-H2 directive (and corresponding `must_haves.truths`, threat-model claim, and acceptance criteria) claimed that after migrating all `app.storage.user.*` accesses, the `app` name is unused in `web/auth_state.py` and ruff F401 would fail at plan boundary if `app` were retained in the `from nicegui import` line. This claim is incorrect: `create_login_dialog()` (lines ~382-412 in the original file) still uses `app.storage.browser.get(...)`, `app.storage.browser[...] = ...`, and `app.storage.browser.pop(...)` for the "Remember me" email/checkbox persistence — a separate NiceGUI storage backend (browser localStorage) outside Phase 87's lint scope (which only flags `app.storage.user`).
- **Fix:** Kept `from nicegui import app, ui` unchanged. Added an inline comment in the imports block documenting why `app` is retained. Ruff `check` exits 0 because `app` is genuinely referenced (6 sites in `create_login_dialog`).
- **Verification of correctness:**
  - `python -m ruff check web/auth_state.py` → 0 findings (the F401 risk the plan claimed does NOT materialize)
  - `python -c "from web.auth_state import GlobalAuthState; print('OK')"` → succeeds
  - `python -m pytest tests/test_no_raw_storage_access.py -v` → all 6 pass (lint scanner does not flag `app.storage.browser`)
- **Acceptance-criteria impact:** Plan acceptance criterion line 668 ("AST check (NEW-H2): no `ImportFrom(module='nicegui', names=[alias(name='app')])` anywhere in the parse tree") would FAIL under this deviation. The underlying invariant the plan tries to enforce (zero raw `app.storage.user` accesses) IS satisfied — the AST check was conflating "no raw storage.user access" with "no nicegui.app import at all", which are different invariants. The runtime invariant (no raw `app.storage.user` in web/) is fully enforced by the Phase 87 lint scanner (6/6 tests green).
- **Files modified:** `web/auth_state.py` (line 12 retained as `from nicegui import app, ui`).
- **Commit:** 656e5a17

**2. [Rule 3 - Blocking] Removed redundant local-scope `from web.auth_state import GlobalAuthState` inside `auth_callback_route`**

- **Found during:** Task 2 (web/main.py ruff check after factoring)
- **Issue:** The plan's Step 1 of Task 2 said "The local-scope import at line 1427 is now redundant but kept as-is to minimize churn (Python re-imports of already-loaded modules are no-ops)." After our changes, the inner `complete_login` shim no longer directly references `GlobalAuthState` (it delegates to `_oauth_complete_login`), so the local-scope import becomes unused. Ruff F401 fired: ``web.auth_state.GlobalAuthState` imported but unused`.
- **Fix:** Removed the local-scope import and replaced it with a comment explaining why the module-top import covers this site. The factored `_oauth_complete_login` module-level helper resolves `GlobalAuthState` via the NEW-H1 module-top import.
- **Files modified:** `web/main.py` (auth_callback_route inner imports).
- **Commit:** 74712a87 (rolled into Task 2 commit).

### Auth Gates

None encountered during execution.

## Verification Commands

### 5 migration audits (NEW-M2 PowerShell-friendly)
```
OK: web/auth_state.py 'app\.storage\.user\['        = 0 (expected 0)
OK: web/auth_state.py 'app\.storage\.user\.pop'     = 0 (expected 0)
OK: web/auth_state.py 'app\.storage\.user\.get\('   = 0 (expected 0)
OK: web/main.py 'app\.storage\.user\[GlobalAuthState' = 0 (expected 0)
OK: web/main.py "app\.storage\.user\['auth_session'\]" = 0 (expected 0)
```

### Test-fixture audit (Revision MAY-9)
```
TOTAL: 0 matches
OK: no raw-fixture bypass outside lint scanner synthetic snippets
```

### Pytest invocations
```
tests/test_no_raw_storage_access.py        ........  6/6 passed
tests/test_auth_callback_resilience.py     ........  7/7 passed
tests/test_refresh_lock_per_session.py     ........  3/3 passed
tests/test_auth_revocation_and_headers.py  ........  6/6 passed
tests/test_session_uuid.py                 ........ 11/11 passed
Full suite: 1956 passed, 21 skipped, 0 failed (146.23s)
```

### YAML + lint scanner empty-allowlist
```
allowed_raw_access: []
file-entry count: 0
```

### Ruff
```
$ python -m ruff check web/auth_state.py web/main.py tests/test_auth_callback_resilience.py tests/test_no_raw_storage_access.py
All checks passed!
```

## Threat Model Status

| Threat ID | Disposition | Verification |
|-----------|-------------|--------------|
| T-91-01 (set_auth multi-key write) | mitigated | Task 4 T-D pre-seeded stale auth_profile + SYMMETRIC 2-key rollback assertion |
| T-91-02 (do_login multi-key write) | mitigated | Task 4 T-A (analog via _oauth_complete_login) + T-E DEFENSIVE 3-key cleanup |
| T-91-03 (OAuth multi-key write) | mitigated | Task 4 T-A + T-E |
| T-91-04 (reader AssertionError propagation) | mitigated | Task 4 T-C |
| T-91-05 (half-logged-in state info disclosure) | mitigated | SYMMETRIC 2-key + DEFENSIVE 3-key layered defense |
| T-91-06 (stale auth_profile role leak) | **mitigated (NEW per Codex HIGH round-1)** | Task 4 T-D (set_auth SYMMETRIC) + T-E (DEFENSIVE caller cleanup) + T-F (profile=None) — all 3 paths verified |
| T-91-07 (OAuth code replay / session resurrect — inherited Phase 90) | inherited | Phase 90 D-11/D-11b discipline preserved in clear_auth (only the 3 raw pops were swapped) |
| T-91-08 (lint scanner regression — empty allowlist hard-fail) | mitigated | D-07 explanatory-comment replacement; test_allowlist_well_formed passes |
| T-91-09 (half-state telemetry visibility) | mitigated | NEW-L2 method-tagged posthog events; D-05 + D-06 emit reason tags |
| T-91-10 (UI state desync; double-posthog event — NEW per Revision MUST-4) | **accepted; NEW-L1 single-event consolidation DEFERRED** | show_error encapsulates 6 UI/observability ops including posthog; UI-recovery transition fires atomically. Double-posthog acceptable for Phase 91 (rich reason filterable in PostHog); single-event consolidation deferred to Phase 92 / telemetry-polish phase. |

## Hand-off

This is the migration commit in the 3-plan Phase 91 split (D-10 + NEW-M3). Plan 91-02 (single test file, depends_on: 91-01) installs the AUTHW-06 retention guard for `web/components/filter_panel.py:persist_value` safe-wrap. Plan 91-03 (closeout docs, depends_on: 91-02) updates STATE.md, ROADMAP.md, CLAUDE.md, and docs/OPEN_ISSUES.md.

Plan 91-01 does NOT modify STATE.md / ROADMAP.md / CLAUDE.md / docs/OPEN_ISSUES.md (per the 3-plan-split discipline, those are Plan 91-03's responsibility). The orchestrator owns the central STATE.md / ROADMAP.md update after all worktree agents in this wave complete.

## Self-Check: PASSED

- **Files created:**
  - `tests/test_auth_callback_resilience.py` → FOUND
- **Files modified:**
  - `web/auth_state.py` → FOUND (committed in 656e5a17)
  - `web/main.py` → FOUND (committed in 74712a87)
  - `tests/test_no_raw_storage_access.py` → FOUND (committed in af28cc8a)
  - `.planning/phase87_storage_allowlist.yaml` → FOUND (committed in af28cc8a)
- **Commits:**
  - 656e5a17 (Task 1) → FOUND
  - 74712a87 (Task 2) → FOUND
  - af28cc8a (Task 3) → FOUND
  - 0c4cda29 (Task 4) → FOUND
- **Plan-boundary verification:**
  - Full pytest: 1956 passed, 21 skipped, 0 failed → PASSED
  - Phase 87 lint scanner: 6/6 → PASSED
  - AUTHW-05 resilience tests: 7/7 → PASSED
  - Phase 90 regression check: 3+6+11 = 20/20 → PASSED
  - Ruff: All checks passed → PASSED
  - Test-fixture audit: 0 matches → PASSED
  - 5 migration audits: 0/0/0/0/0 (all expected 0) → PASSED
