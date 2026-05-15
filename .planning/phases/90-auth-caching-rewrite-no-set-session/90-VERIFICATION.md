---
phase: 90-auth-caching-rewrite-no-set-session
verified: 2026-05-15T00:00:00Z
status: passed
score: 5/5 must-haves verified
overrides_applied: 0
re_verification:
  previous_status: null
  initial: true
---

# Phase 90: Auth Caching Rewrite — No set_session — Verification Report

**Phase Goal:** Replace the process-wide auth client cache with request-scoped auth that does NOT call `auth.set_session()` to set headers; refresh locking keyed by `_session_uuid` with no cached client objects.

**Verified:** 2026-05-15
**Status:** PASSED
**Re-verification:** No — initial verification
**Plans Merged:** 90-01 (`c8f0a1a2`) + 90-02 (`8869f449`)
**Test Status at Verification:** 1949 passed, 20 skipped, 0 failures (full pytest)

---

## Goal Achievement

### ROADMAP Success Criteria (5/5)

| #   | Success Criterion                                                                                               | Status     | Evidence |
| --- | --------------------------------------------------------------------------------------------------------------- | ---------- | -------- |
| 1   | Zero matches for `_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL` in `web/supabase_client.py` | ✓ VERIFIED | Word-boundary grep returns 0 matches for all 4 names. Substring grep matches only `_refresh_locks_guard` (NEW Phase 90 variable, not a deleted name) at lines 33, 171 |
| 2   | `.auth.set_session(` across `web/` matches ONLY inside OAuth bootstrap helper                                   | ✓ VERIFIED | Grep `\.auth\.set_session\(` returns 1 match: `web/supabase_client.py:609` inside `set_session_from_url` (function starts line 591) |
| 3   | Refresh-only locks keyed by `_session_uuid` values (not access tokens, not storage object IDs)                  | ✓ VERIFIED | `web/supabase_client.py:162` calls `get_persisted_session_uuid()`; `:172` keys `_refresh_locks.setdefault(session_uuid, ...)`; behavioral test `test_b_distinct_uuid_parallelism` asserts `max_concurrent == 2` for distinct UUIDs (proves real parallelism not serialization) |
| 4   | Zero matches for `auth_resurrection` or resurrection guard function name (introduced in `cca23db3`)             | ✓ VERIFIED | Grep `auth_resurrection|_clear_stale_auth|_prune_session_client_cache` returns 0 matches across `web/`. Commit `cca23db3` introduced `_clear_stale_auth`; both names absent |
| 5   | Code comment in auth path documents Codex finding citing `gotrue_client.py:713`                                 | ✓ VERIFIED | 2 citations in `web/supabase_client.py`: line 83 in `_apply_user_auth_to_client` docstring, line 258 in `get_user_client` docstring — both citing `gotrue_client.py:713` with rationale (helper is networked, calls `get_user`/`_refresh_access_token`) |

**Score:** 5/5 success criteria verified.

### REQUIREMENTS.md Coverage (AUTHC-01..05)

| Requirement | Source Plan          | Description                                                                              | Status     | Evidence |
| ----------- | -------------------- | ---------------------------------------------------------------------------------------- | ---------- | -------- |
| AUTHC-01    | 90-02                | `_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL` deleted           | ✓ SATISFIED | Runtime test `test_no_client_cache_globals.py::test_attr_absent` passes 6/6 (4 globals + 2 helpers); word-boundary grep returns 0 matches |
| AUTHC-02    | 90-01                | Request-scoped auth strategy without `auth.set_session()`                                | ✓ SATISFIED | `get_user_client()` at `web/supabase_client.py:233-320` reads via `safe_user_get` (line 278-279), no caching, applies auth via `_apply_user_auth_to_client` (line 316) which uses local header mutation only (postgrest.auth + functions.set_auth + storage.session.headers — `:99-101`); zero `.auth.set_session(` calls outside `set_session_from_url` |
| AUTHC-03    | 90-01 + 90-02        | Refresh-only locking keyed by `_session_uuid` from Phase 87; no cached `supabase.Client` | ✓ SATISFIED | `_refresh_user_session` keyed by `get_persisted_session_uuid()` returning `Optional[str]` (`web/supabase_client.py:162-172`); behavioral test `test_refresh_lock_per_session.py` Tests A/B/C all pass — Test B asserts `max_concurrent == 2` for distinct UUIDs (real parallelism), Test A asserts `max_concurrent == 1` for same UUID (per-uuid serialization), Test C asserts stale-snapshot short-circuit (D-06) |
| AUTHC-04    | 90-02                | Auth-resurrection guard from `cca23db3` removed                                          | ✓ SATISFIED | `_clear_stale_auth` and `_prune_session_client_cache` both absent from `web/supabase_client.py` (runtime `hasattr` test passes); origin commit `cca23db3` verified to be the introduction commit |
| AUTHC-05    | 90-01                | Code comment near auth path documents WHY `set_session()` is avoided                     | ✓ SATISFIED | `get_user_client()` docstring at `web/supabase_client.py:256-266` cites `gotrue_client.py:713` and explains networked behavior; mirrored at `_apply_user_auth_to_client` docstring `:82-86` |

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `web/supabase_client.py` | Full auth path rewritten, 4 globals + 2 helpers deleted, refresh_session() standalone deleted | ✓ VERIFIED | All 6 deleted names absent (word-boundary grep + runtime hasattr); `def refresh_session(` (anchored) returns 0 matches; new helpers `_apply_user_auth_to_client` `:78`, `_access_token_near_expiry` `:104`, `_refresh_user_session` `:135`, `change_password` `:439` all present and substantive; new module globals `_refresh_locks` `:32`, `_refresh_locks_guard` `:33`, `REFRESH_SKEW_SEC` present |
| `web/auth_state.py` | `clear_auth` reordered to revoke-before-pop | ✓ VERIFIED | `:119-145` shows `supabase_sign_out(access_token)` called BEFORE `app.storage.user.pop(...)` in finally block; D-11 comment present at `:121-127` |
| `web/pages/profile.py` | Password change migrated to `change_password` REST helper | ✓ VERIFIED | `:152-153` imports `change_password as supabase_change_password` and calls it; D-02 comment present at `:148-150` |
| `web/safe_storage.py` | `get_persisted_session_uuid()` added — strict `Optional[str]` variant | ✓ VERIFIED | `:150-181` defines function; returns `None` on prune race / poisoned value / storage AssertionError; refuses to mint or return ephemeral UUID |
| `.planning/phase87_storage_allowlist.yaml` | `web/supabase_client.py` entry removed (3→2 files) | ✓ VERIFIED | File contains exactly 2 entries: `web/auth_state.py` (8 patterns) and `web/main.py` (3 patterns); no `web/supabase_client.py` entry. Phase 87 lint scanner `test_no_raw_storage_access.py` passes 6/6 |
| `tests/test_auth_revocation_and_headers.py` | 6 mocked regression tests | ✓ VERIFIED | All 6 tests pass: admin.sign_out("global"), sign_out None token noop, change_password 4-header tetrad, change_password not-logged-in error, apply_user_auth storage header, get_user_client returns anonymous on refresh-fail |
| `tests/test_no_set_session_outside_oauth.py` | D-15 Class A + Class B static AST scanner with 13 seed traps | ✓ VERIFIED | 16 tests pass (2 production scans + 13 seed traps + 1 self-exempt sanity); covers literal + intra-function aliased forms (Codex M4) |
| `tests/test_no_client_cache_globals.py` | D-16 runtime attr-absence test for 6 deleted names | ✓ VERIFIED | 6 parametrized tests pass; xfail decorator confirmed removed (test passes for real, not xfailed) |
| `tests/test_refresh_lock_per_session.py` | D-17 behavioral test: serialization / parallelism / stale-snapshot | ✓ VERIFIED | 3 tests pass: Test A (same UUID → max_concurrent==1), Test B (distinct UUIDs → max_concurrent==2 via `_ThreadRoutedApp` proxy + `_ConcurrencyRecorder`), Test C (stale-snapshot short-circuit, call_count==1 not 2) |

### Key Link Verification

| From                                          | To                                            | Via                                                                                | Status  | Details |
| --------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------- | ------- | ------- |
| `get_user_client`                             | `safe_user_get('auth_session')`               | Phase 87 chokepoint                                                                | WIRED   | `web/supabase_client.py:278-279`; only one `app.storage.user` reference in file is inside docstring (`:240`) — production code routes through `safe_user_get` |
| `get_user_client`                             | `_refresh_user_session`                       | proactive refresh on near-expiry; honors return value (R3-M1 short-circuit)        | WIRED   | `:296-313`; refreshed=False → `return get_client()` (anonymous) — never builds authenticated client with stale token |
| `_refresh_user_session`                       | `get_persisted_session_uuid()`                | strict Optional[str] (vs `get_session_uuid` which mints ephemerals)                | WIRED   | `:162-170`; None → log + return False, no ephemeral mint → no race |
| `_refresh_user_session`                       | `_refresh_locks[session_uuid]`                | per-UUID lock allocated under guard                                                | WIRED   | `:171-173` (`with _refresh_locks_guard:` → `_refresh_locks.setdefault(...)`) |
| `clear_auth` (auth_state.py)                  | `supabase_sign_out(access_token)`             | revoke-before-pop ordering (D-11 / AUTHW-04)                                       | WIRED   | `web/auth_state.py:131-140`; try/finally ensures local cleanup even on server-revocation failure |
| `sign_out`                                    | `throwaway.auth.admin.sign_out(jwt, "global")` | bypasses GoTrue's high-level (no-op when no local session)                         | WIRED   | `web/supabase_client.py:433`; admin namespace direct call |
| `change_password` (profile.py)                | `supabase_client.change_password()`           | REST helper bypasses GoTrue update_user                                            | WIRED   | `web/pages/profile.py:152-153`; D-02 comment cites rationale |
| `get_oauth_url` ↔ `exchange_code_for_session` | PKCE verifier round-trip via `safe_user_set`/`safe_user_pop` | Codex H1 fix                                                                       | WIRED   | Verifier persisted via `safe_user_set('oauth_code_verifier', v)` and popped via `safe_user_pop('oauth_code_verifier', None)`; throwaway clients on both sides |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| All 4 Phase 90 test files pass | `pytest tests/test_no_set_session_outside_oauth.py tests/test_no_client_cache_globals.py tests/test_refresh_lock_per_session.py tests/test_auth_revocation_and_headers.py -v` | 31 passed in 5.39s | ✓ PASS |
| Phase 87 allowlist scanner stays green (3→2 entries) | `pytest tests/test_no_raw_storage_access.py -v` | 6 passed in 1.01s | ✓ PASS |
| Full pytest suite green (no regressions) | `pytest tests/ -q` | 1949 passed, 20 skipped, 0 failures in 161.57s | ✓ PASS |
| Runtime hasattr: 6 deleted names absent from `web.supabase_client` | `python -c "import web.supabase_client as m; print([n for n in ['_client_cache','_session_locks','_locks_guard','_CLIENT_CACHE_TTL','_clear_stale_auth','_prune_session_client_cache'] if hasattr(m,n)])"` | `[]` (proven by test_no_client_cache_globals.py runtime test) | ✓ PASS |
| Test B proves real parallelism (not trivial pass) | `pytest tests/test_refresh_lock_per_session.py::test_b_distinct_uuid_parallelism` | Pass — `max_concurrent == 2` confirmed via `_ConcurrencyRecorder` + `_ThreadRoutedApp` | ✓ PASS |
| Static AST scanner: 0 production violations | `pytest tests/test_no_set_session_outside_oauth.py::test_no_set_session_class_a_violations tests/test_no_set_session_outside_oauth.py::test_no_get_client_class_b_violations` | Both pass | ✓ PASS |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| (none) | — | — | — | — |

Code is clean. No TODOs, FIXMEs, placeholders, empty handlers, or stub returns introduced by the phase.

### Human Verification Required

None. The phase is goal-aligned and verifiable entirely by static analysis + automated tests. Optional follow-up (deferred from Plan 90-02):

- **Task 5 (L1 perf sanity check)** was explicitly DEFERRED in 90-02-SUMMARY.md (lines 168-170) because it requires running the live web server, which is forbidden in worktree-mode per MEMORY constraint `feedback_no_background_webserver.md`. This is informational measurement only — it does not affect any of the 5 ROADMAP success criteria (all closed without it).

### Gaps Summary

No gaps. All 5 ROADMAP success criteria verified PASS with file:line evidence. All 5 AUTHC requirements satisfied. All 4 new test files exist and pass (16 + 6 + 3 + 6 = 31 tests, all green). Phase 87 allowlist correctly trimmed from 3 to 2 entries with the Phase 87 lint scanner still green (6/6). Full pytest suite green at 1949 passed / 0 failures / 20 skipped — matches the objective's stated result.

The phase achieves its goal: process-wide auth-client cache is gone; request-scoped auth uses local header mutation (no `set_session` network call mid-flight); refresh locks key by persisted `_session_uuid` (proven by Test B's `max_concurrent == 2` assertion for distinct UUIDs); `_clear_stale_auth` resurrection guard removed; Codex finding documented inline at `gotrue_client.py:713` in two docstrings.

Hand-off to Phase 91 is clean: `clear_auth`'s revoke-before-pop reorder + `admin.sign_out("global")` server-side revocation were pulled forward (AUTHW-03 + AUTHW-04) per Codex round-2 P1. The Phase 87 allowlist is now 2 entries; Phase 91 will reduce it to 0 by migrating `web/auth_state.py` + `web/main.py` raw access sites.

---

_Verified: 2026-05-15_
_Verifier: Claude (gsd-verifier)_
