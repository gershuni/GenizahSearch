---
phase: 90-auth-caching-rewrite-no-set-session
plan: 02
subsystem: web/auth
tags: [auth, supabase, cache-deletion, regression-guards, ast-scanner, refresh-locks, ci-discipline]
requires:
  - 90-01 (Wave 1 rewrite — _refresh_user_session + _apply_user_auth_to_client + throwaway clients in 5 bootstrap helpers)
provides:
  - permanent CI guard banning set_session / exchange_code_for_session outside per-helper allowlist (D-15 Class A)
  - permanent CI guard banning get_client().auth.<mutating>(...) singleton-resurrection vectors (D-15 Class B, with intra-function alias tracking)
  - permanent runtime attr-absence guard over 6 deleted module-level names (D-16)
  - deterministic behavioral verification of refresh-lock semantics (D-17, Tests A/B/C)
affects:
  - web/supabase_client.py — 4 globals + 2 helper functions + 1 unused typing import deleted (~52 lines net negative)
  - tests/test_no_set_session_outside_oauth.py — created (16 tests)
  - tests/test_no_client_cache_globals.py — created (6 tests, xfail removed in atomic commit)
  - tests/test_refresh_lock_per_session.py — created (3 tests)
tech-stack:
  added: []
  patterns:
    - AST-based deletion verification (R3-H1 fix b — comment-blind, durable)
    - Atomic deletion-with-test commit (Phase 89 D-09 + R9 + R10 discipline)
    - threading.Barrier-coordinated parallelism tests with ConcurrencyRecorder.max_concurrent assertion
    - Per-thread storage routing via threading.local-backed _ThreadRoutedApp proxy
key-files:
  created:
    - tests/test_no_set_session_outside_oauth.py (272 lines, 16 tests)
    - tests/test_no_client_cache_globals.py (62 lines, 6 tests)
    - tests/test_refresh_lock_per_session.py (264 lines, 3 tests)
  modified:
    - web/supabase_client.py (4 module globals + 2 helper functions + 1 typing import deleted; net ~-52 lines)
decisions:
  - Deletion + 3 test files landed in ONE atomic commit (4 paths total — Phase 89 D-09 + R9 + R10 discipline). Test 2's xfail markers removed in the same atomic commit as the deletion, so CI never crosses a red window.
  - Task 5 (L1 perf sanity check) deferred to a follow-up — execution requires running the live web server and hitting a representative authenticated page; the worktree agent is forbidden to launch web servers (per user MEMORY: feedback_no_background_webserver.md). The skipped task is purely measurement; it documents per-request `get_user_client()` overhead but does not affect Phase 90 success criteria SC #1, #3, or #4. Plan 90-02 still closes its primary deliverables.
  - SUMMARY.md committed in a separate follow-on commit (per Task 4 Step 6 — keeps atomic deletion+guards diff readable).
  - Removed the now-unused `Tuple` import from typing in supabase_client.py (no longer referenced after `_client_cache: Dict[str, Tuple[Client, float]]` deletion). Defense against ruff lint regression.
  - Static AST scanner uses Class B intra-function alias tracking (Pass 2) per Codex review round 1 M4 — catches `c = get_client(); c.auth.<X>(...)` patterns alongside the literal-chain `get_client().auth.<X>(...)`. 3 of the 13 seed traps exercise the aliased form.
  - Behavioral Test B uses `_ThreadRoutedApp` (threading.local-backed proxy) + `_ConcurrencyRecorder` to prove real parallelism via `max_concurrent == 2`. The prior 5ms-stagger workaround would pass trivially under serialization — plan-checker round caught this and required the proxy-based solution.
metrics:
  duration_minutes: ~25
  tasks_completed: 4
  tasks_deferred: 1 (Task 5 perf sanity — requires live web server)
  files_modified: 1
  files_created: 3
  commits: 1 (atomic deletion+guards; SUMMARY commit pending parent agent)
  tests_added: 25 (16 + 6 + 3)
  tests_total_at_boundary: 1948 passed, 21 skipped, 0 failures
  completed_date: 2026-05-15
---

# Phase 90 Plan 02: Auth Cache Deletion + Permanent CI Guards Summary

Deletes the 4 dead-code module globals (`_session_locks`, `_locks_guard`, `_client_cache`, `_CLIENT_CACHE_TTL`) and 2 dead-code helper functions (`_prune_session_client_cache`, `_clear_stale_auth`) from `web/supabase_client.py` — all unreferenced after Plan 90-01's rewrite. Installs 3 permanent CI guards: static AST scanner banning `set_session` + `exchange_code_for_session` outside their allowed helpers AND `get_client().auth.<mutating>` resurrection vectors; runtime attr-absence test over the 6 deleted names; deterministic behavioral test for refresh-lock semantics. All four file changes land in a SINGLE atomic commit (Phase 89 D-09 deletion-with-test discipline).

## Deletions

| Name | Type | Source Location (pre-deletion) | Lines |
|------|------|--------------------------------|-------|
| `_session_locks` | Dict[str, threading.Lock] global | `web/supabase_client.py:31` | 1 |
| `_locks_guard` | threading.Lock() global | `web/supabase_client.py:32` | 1 |
| `_client_cache` | Dict[str, Tuple[Client, float]] global | `web/supabase_client.py:34` | 1 |
| `_CLIENT_CACHE_TTL` | int = 50 constant | `web/supabase_client.py:35` | 1 |
| `_prune_session_client_cache(now)` | helper function | `web/supabase_client.py:91-105` | 15 |
| `_clear_stale_auth(storage)` | helper function | `web/supabase_client.py:114-125` | 12 |

Also deleted: the 7-line legacy comment block above the 4 globals describing the prior cache-keying strategy (2026-05-12 Codex CRITICAL fix narrative), plus the now-unused `Tuple` import from `typing`. Total net deletion: ~52 lines.

## Test Files Installed

### `tests/test_no_set_session_outside_oauth.py` — Static AST Scanner (D-15)

16 tests total:
- 1 test_no_set_session_class_a_violations — scans web/ + tests/ for `<X>.set_session(...)` / `<X>.exchange_code_for_session(...)` Call nodes outside per-method allowlist
- 1 test_no_get_client_class_b_violations — scans for `get_client().auth.<mutating>(...)` chains AND intra-function aliased forms (Pass 2 per Codex M4)
- 13 test_seed_traps_are_flagged (parametrized) — verifies the scanner correctly detects each known-bad snippet
- 1 test_exempt_files_includes_self — sanity that the test file exempts itself from scanning

#### 13 Seed Traps

**Class A traps (5):**
1. `class_a_direct` — `client.auth.set_session(a, r)`
2. `class_a_short_alias` — `c.auth.set_session(a, r)`
3. `class_a_aliased` — `auth = client.auth; auth.set_session(a, r)`
4. `class_a_oauth_direct` — `client.auth.exchange_code_for_session({})`
5. `class_a_oauth_aliased` — `auth = c.auth; auth.exchange_code_for_session({})`

**Class B traps (5 literal-chain):**
6. `class_b_set_session` — `get_client().auth.set_session(a, r)`
7. `class_b_sign_in` — `get_client().auth.sign_in_with_password({})`
8. `class_b_oauth` — `get_client().auth.exchange_code_for_session({})`
9. `class_b_refresh` — `get_client().auth.refresh_session(r)`
10. `class_b_update_user` — `get_client().auth.update_user({})`

**Class B traps (3 aliased — Codex review round 1 M4):**
11. `class_b_aliased_sign_out` — `def f(): c = get_client(); c.auth.sign_out()`
12. `class_b_aliased_sign_in` — `def f(): client = get_client(); client.auth.sign_in_with_password({})`
13. `class_b_aliased_update_user` — `def f(): c = get_client(); c.auth.update_user({})`

All 13 verified to be caught by the scanner. Result on production code: **0 violations** — Plan 90-01's rewrites left only the two allowed Class A calls (one `set_session` inside `set_session_from_url`, one `exchange_code_for_session` inside `exchange_code_for_session`) and zero Class B resurrection vectors.

### `tests/test_no_client_cache_globals.py` — Runtime Attr-Absence (D-16)

6 tests, parametrized over `DELETED_GLOBALS = ['_client_cache', '_session_locks', '_locks_guard', '_CLIENT_CACHE_TTL', '_clear_stale_auth', '_prune_session_client_cache']`. Each asserts `not hasattr(web.supabase_client, name)`. The xfail decorator was installed pre-deletion (xfail-strict converts the failing assertion to pass) and removed in the same atomic commit as the deletion — so the test transitioned from xfailed → passed for real without crossing a red CI window.

### `tests/test_refresh_lock_per_session.py` — Behavioral (D-17)

3 deterministic tests using `threading.Barrier(2)` for true simultaneity:

- **Test A (`test_a_same_uuid_serialization`):** Two threads of the same `_session_uuid` call `_refresh_user_session()` simultaneously. Assertions: `recorder.max_concurrent == 1` (per-uuid serialization), `recorder.call_count == 1` (only one refresh fires), both threads return True, and rotated token is persisted.

- **Test B (`test_b_distinct_uuid_parallelism`):** Two threads of distinct `_session_uuid`s refresh simultaneously through a `_ThreadRoutedApp` proxy (threading.local-routed `app.storage.user`). With `hold_ms=50` inside the mocked `refresh_session`, the `_ConcurrencyRecorder.max_concurrent` reaches 2 ONLY if per-uuid locks don't cross-serialize. **PRIMARY INVARIANT: `recorder.max_concurrent == 2`** — proves real parallelism. `call_count == 2` alone would pass trivially even under serialization (plan-checker round catch).

- **Test C (`test_c_stale_snapshot_short_circuits`):** Sequential test — first `_refresh_user_session(stale_refresh_token='rt-original')` rotates the token; second call with the same `stale_refresh_token='rt-original'` detects the stored refresh_token has rotated and returns True WITHOUT invoking `refresh_session`. Assertion: `recorder.call_count == 1` (NOT 2) — proves the D-06 stale-snapshot short-circuit prevents refresh-token burn.

All 3 tests deterministic via `threading.Barrier` (no time-based stagger as synchronization primitive); time.sleep only used inside the mocked refresh handler to widen the parallel window so concurrency is observable.

## Test B `_ThreadRoutedApp` + `_ConcurrencyRecorder` Invariant

The plan-checker round explicitly required completing the per-thread proxy because the prior workaround (a 5ms `time.sleep` stagger) only asserted `recorder.call_count == 2`, which passes trivially even when refreshes are perfectly serialized — defeating the entire point of the test. With `_ThreadRoutedApp` providing per-thread `app.storage.user` isolation and `_ConcurrencyRecorder` tracking the peak `_active` count via thread-safe `enter()`/`exit()` hooks, the test asserts `max_concurrent == 2` — a load-bearing invariant proving the per-uuid locks do NOT accidentally serialize across distinct sessions.

Outcome on the just-shipped Plan 90-01 implementation: `recorder.max_concurrent == 2` PASSES. The two refreshes overlap in time, distinct lock objects appear in `mod._refresh_locks`, and `mod._refresh_locks[uuid_a] is not mod._refresh_locks[uuid_b]` holds. SC #3 (refresh locks keyed by `_session_uuid`) verified behaviorally.

## Atomic-Commit Verification

The atomic deletion+guards commit at HEAD lists exactly 4 paths:

```
tests/test_no_client_cache_globals.py
tests/test_no_set_session_outside_oauth.py
tests/test_refresh_lock_per_session.py
web/supabase_client.py
```

Commit hash: `7c40cfba`. Verified via `git show --stat --name-only HEAD` post-commit.

**SUMMARY commit:** This SUMMARY file is committed by the parent orchestrator agent in a separate follow-on commit (worktree-mode discipline — the executor returns to the orchestrator after the atomic commit and a SUMMARY-only commit).

## Phase 90 Closure

### ROADMAP Success Criteria (5 of 5 across Plan 90-01 + Plan 90-02)

| SC | Owner | Status | Evidence |
|----|-------|--------|----------|
| SC #1 — cache globals deleted | Plan 90-02 | CLOSED | AST scan returns 0 references; runtime hasattr() returns False for all 4 globals; runtime attr-absence test passes for real (not xfailed) |
| SC #2 — set_session allowlist enforced | Plan 90-01 (rewrite) + Plan 90-02 (durable AST scanner) | CLOSED | D-15 Class A scanner finds 0 violations on production code; the only `set_session(...)` call lives inside `set_session_from_url`, the only `exchange_code_for_session(...)` lives inside `exchange_code_for_session` |
| SC #3 — refresh locks keyed by _session_uuid | Plan 90-01 (implementation) + Plan 90-02 (behavioral verification) | CLOSED | Test B asserts `max_concurrent == 2` for distinct uuids AND `mod._refresh_locks[uuid_a] is not mod._refresh_locks[uuid_b]`; Test A asserts `max_concurrent == 1` for same uuid |
| SC #4 — auth-resurrection guard deleted | Plan 90-02 | CLOSED | AST scan returns 0 FunctionDef nodes for `_clear_stale_auth`; runtime hasattr() returns False; runtime attr-absence test passes |
| SC #5 — AUTHC-05 docstring present | Plan 90-01 | CLOSED | `get_user_client()` docstring cites `gotrue_client.py:713` (verified in 90-01-SUMMARY.md) |

### AUTHC Requirements

- **AUTHC-01** (cache globals deleted) — CLOSED via Plan 90-02 deletion
- **AUTHC-02** (set_session avoided in get_user_client) — CLOSED via Plan 90-01 rewrite
- **AUTHC-03** (refresh locks keyed by _session_uuid) — CLOSED via Plan 90-01 implementation + Plan 90-02 behavioral verification
- **AUTHC-04** (auth-resurrection guard deleted) — CLOSED via Plan 90-02 `_clear_stale_auth` deletion
- **AUTHC-05** (AUTHC-05 docstring citing gotrue_client.py:713) — CLOSED via Plan 90-01

## Hand-off to Phase 91

Phase 90 already pulled AUTHW-03 + AUTHW-04 forward (per Codex round-2 P1) — `sign_out` now uses throwaway + `admin.sign_out("global")` for actual server-side revocation, and `clear_auth` reordered to revoke-before-pop. Remaining Phase 91 scope:

- **AUTHW-01:** Migrate `web/auth_state.py:set_auth/clear_auth/do_login` raw `app.storage.user` pops to `safe_storage` helpers (deletes the `web/auth_state.py` entry from the Phase 87 allowlist; allowlist 2 → 1 entries).
- **AUTHW-02:** Migrate OAuth callback in `web/main.py:1419+` to `safe_storage` helpers (deletes the `web/main.py` OAuth allowlist entry; allowlist 1 → 0 entries).
- **AUTHW-05, AUTHW-06:** Atomic multi-key auth-state writes (auth_user + auth_profile + auth_session as a single safe operation).

After Phase 91, the Phase 87 allowlist should be 0 entries and the Phase 87 lint scanner becomes the sole permanent guard.

## Deviations from Plan

**Task 5 (L1 Perf Sanity Check) — DEFERRED.** Per user MEMORY constraint (`feedback_no_background_webserver.md`: "Never launch web server from Bash — creates unkillable zombie processes on Windows"), the executor cannot run `python -m web.main` to time `get_user_client()` calls in a live request context. The measurement is purely informational — it documents per-request overhead but does not affect any Phase 90 success criterion (SC #1, #3, #4 are all closed without it). Recommended path forward: user runs the measurement manually on next deploy or queues it as a Phase 92 / follow-up item. Tracking flag: `## Perf Sanity (L1)` section will be appended to this SUMMARY by user or follow-up agent when measurements are available.

No other deviations. The 4-task atomic-commit discipline, AST-based deletion verification, ruff cleanup of the unused `Tuple` import, and full pytest green at plan boundary all executed as written.

## Test Results at Plan Boundary

- Full pytest suite: **1948 passed, 21 skipped, 0 failures** in 169.13s
- New test files (all 3):
  - `tests/test_no_set_session_outside_oauth.py`: 16 passed
  - `tests/test_no_client_cache_globals.py`: 6 passed (NOT xfailed — xfail decorator removed in the atomic commit)
  - `tests/test_refresh_lock_per_session.py`: 3 passed
- Ruff check on changed files: **All checks passed!**
- AST verification:
  - 0 Name/Assign/AnnAssign nodes for the 4 deleted globals in `web/supabase_client.py`
  - 0 FunctionDef nodes for the 2 deleted helper functions in `web/supabase_client.py`
- Runtime hasattr() verification: all 6 deleted names absent from `web.supabase_client` module

## Threat Flags

None. All threats in the plan's threat register (T-90-06 through T-90-08) are mitigated by the 3 permanent CI guards installed here. Triple defense in depth:
1. D-15 AST scanner catches re-introductions via direct/aliased/intermediate forms in static analysis
2. D-16 runtime attr-absence catches dynamic re-introductions in runtime hasattr() checks
3. D-17 behavioral test catches semantic regressions where the lock keying degrades from per-uuid to global

## Known Stubs

None. The deletions remove dead code only; no placeholder/stub remains.

## Self-Check: PASSED

Files verified to exist:
- FOUND: tests/test_no_set_session_outside_oauth.py
- FOUND: tests/test_no_client_cache_globals.py
- FOUND: tests/test_refresh_lock_per_session.py
- FOUND: web/supabase_client.py (modified — 4 globals + 2 helpers + 1 typing import deleted)

Commit verified to exist:
- FOUND: 7c40cfba (atomic deletion+guards commit, 4 paths)
