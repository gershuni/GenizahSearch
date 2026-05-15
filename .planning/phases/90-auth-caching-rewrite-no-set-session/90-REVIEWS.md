---
phase: 90
reviewers: [codex]
reviewed_at: 2026-05-15T06:36:59Z
plans_reviewed: [90-01-PLAN.md, 90-02-PLAN.md]
---

# Cross-AI Plan Review — Phase 90 (Auth Caching Rewrite -- No set_session)

## Codex Review

## Plan 90-01

**Summary**
Strong architecture direction, but not execution-ready as written. The major behavior rewrite addresses the right threats: no cached authenticated clients, no mid-flight `set_session`, proactive refresh, and real logout revocation. However, the plan appears to break PKCE OAuth unless the code verifier is carried across `get_oauth_url()` and `exchange_code_for_session()`. There are also several grep-based success gates that will fail because the plan itself introduces banned substrings in comments/docstrings.

**Strengths**
- Correctly identifies that deleting `_client_cache` alone is insufficient because Supabase auth events mutate the singleton client.
- Pulling `sign_out` into Phase 90 is the right security call; otherwise logout revocation regresses.
- Proactive refresh in `get_user_client()` is the right fix for the many write paths without reactive retry.
- Storage header mutation is included, preserving authenticated puzzle publish paths.
- `change_password()` correctly avoids GoTrue's local-session requirement and includes the missing `apikey` header.

**Concerns**
- **HIGH:** PKCE OAuth likely breaks. `sign_in_with_oauth()` stores the PKCE code verifier in the auth client's storage. Moving `get_oauth_url()` and `exchange_code_for_session()` to separate throwaway clients loses that verifier unless it is explicitly persisted per NiceGUI session and passed into exchange.
- **HIGH:** Several static grep gates will fail. The new comments/docstrings include exact substrings like `.set_session(` and deleted names such as `_session_locks`, while success criteria require zero or allowlisted grep matches.
- **HIGH:** `_refresh_user_session()` swallows invalid/consumed refresh-token failures and leaves `auth_session` in storage. That can create repeated refresh attempts and stale logged-in UI state. Removing `_clear_stale_auth` is fine, but there should be a replacement invalid-session cleanup path through `safe_user_pop`.
- **MEDIUM:** The plan's assumed `get_session_uuid() -> Optional[str]` contract does not match current code. Current `get_session_uuid()` never returns `None`; it may mint an ephemeral UUID during storage failure. That weakens the "keyed by persisted `_session_uuid`" guarantee under prune races.
- **MEDIUM:** The `sign_out()` docstring still says it applies local headers and calls `sign_out()` on the throwaway, while the body correctly calls `admin.sign_out(...)`. That contradiction is dangerous in this phase.
- **MEDIUM:** No direct unit test is planned for `admin.sign_out(access_token, "global")`, `change_password()`'s four headers, or storage header application.
- **LOW:** Per-request `create_client()` may be acceptable, but this phase should include at least a lightweight perf sanity check on a page path with multiple authenticated calls.

**Suggestions**
- Add per-session PKCE verifier persistence: store the verifier after `get_oauth_url()` via `safe_user_set`, then read/pop it in `exchange_code_for_session()` and pass `code_verifier`.
- Remove exact banned substrings from production comments/docstrings, or replace grep gates with AST-based checks that ignore strings.
- Add terminal refresh failure handling: on known invalid/expired/consumed refresh errors, clear auth keys via safe storage and return anonymous.
- Align D-06 with actual `get_session_uuid()` behavior, or add a helper that requires a persisted UUID and refuses ephemeral fallback for refresh locking.
- Add mocked tests for logout revocation, password-change headers, and `_apply_user_auth_to_client()` storage headers.

**Risk Assessment**
**HIGH** until the PKCE verifier issue and grep-gate contradictions are fixed. After that, the plan drops to **MEDIUM** because the rewrite touches the core auth boundary and refresh behavior.

## Plan 90-02

**Summary**
The deletion-and-guard plan is well-motivated and mostly follows the prior Phase 88/89 pattern. The three guard classes are appropriate, especially the behavioral refresh-lock tests. The main risks are test/acceptance inconsistencies, insufficient singleton-alias detection, and brittle exact-path commit rules that conflict with required summary outputs.

**Strengths**
- Deleting globals and installing guards in one atomic step is the right discipline.
- Runtime attr-absence test is simple and durable.
- AST seed traps are valuable, especially the aliased `auth = client.auth` cases for Class A.
- Refresh-lock behavioral tests cover the important properties: same-session serialization, cross-session parallelism, stale-snapshot short-circuit.
- Test B's `max_concurrent == 2` assertion is the right improvement over call-count-only checks.

**Concerns**
- **MEDIUM:** Class B scanner only catches literal `get_client().auth.X(...)`. It misses `client = get_client(); client.auth.X(...)`, which is exactly the current code style before Plan 90-01.
- **MEDIUM:** `tests/test_no_client_cache_globals.py` contains `@pytest.mark.xfail` in its docstring, so the later acceptance gate `grep -n "@pytest.mark.xfail" ... returns 0` will fail unless the docstring is changed too.
- **MEDIUM:** The Task 2 acceptance grep for `xfail(strict=True` will not match the provided multi-line decorator form.
- **MEDIUM:** Plan 90-02 requires `git show ...` to list exactly four paths, but the plan also requires creating `90-02-SUMMARY.md`. Clarify whether summaries are committed separately or included in the plan commit.
- **LOW:** The proposed static scanner imports `os` unused; `ruff` will likely fail.
- **LOW:** Test C is sequential despite the thread-oriented description. That is acceptable for stale-snapshot logic, but the description should be tightened.

**Suggestions**
- Extend Class B with simple intra-function alias tracking for `name = get_client()` followed by `name.auth.<mutating>()`.
- Make grep acceptance checks distinguish decorators from docstring mentions, or avoid exact `@pytest.mark.xfail` text in docstrings.
- Remove unused imports from generated tests before baking the plan.
- Reconcile commit manifests with required summary files.
- Add a seed trap for `c = get_client(); c.auth.sign_out()` if alias tracking is added.

**Risk Assessment**
**MEDIUM**. The guard strategy is sound, but the current scanner gap and acceptance-command contradictions will cause either missed regressions or avoidable execution churn. The plan becomes **LOW-MEDIUM** after those are corrected.

---

## Consensus Summary

Single-reviewer run (Codex only); no cross-reviewer consensus. Distilled signal:

### Top Concerns (must-address before execute)
1. **HIGH — PKCE verifier lifetime (Plan 90-01).** Throwaway clients for `get_oauth_url()` and `exchange_code_for_session()` will lose the PKCE code verifier unless explicitly persisted per NiceGUI session via `safe_user_set` / `safe_user_pop`. Without this, OAuth login breaks.
2. **HIGH — Self-defeating grep gates (Plan 90-01).** Success-criterion greps for `.set_session(`, `_session_locks`, `_client_cache`, etc. will hit the docstrings/comments the plan itself introduces. Either strip those substrings from prod comments or replace grep gates with AST-based checks that ignore string literals.
3. **HIGH — Refresh-failure cleanup (Plan 90-01).** Removing `_clear_stale_auth` without a replacement means invalid/consumed refresh tokens leave `auth_session` stuck in storage; need an explicit cleanup path through `safe_user_pop` on known terminal refresh errors.
4. **MEDIUM — `get_session_uuid()` contract mismatch (Plan 90-01).** Current helper never returns `None` and can mint an ephemeral UUID under prune; the lock-keying guarantee in D-06 is weaker than the plan assumes. Either align D-06 with actual behavior or add a helper that refuses ephemeral fallback for refresh locking.
5. **MEDIUM — Class B AST scanner gap (Plan 90-02).** Catches `get_client().auth.X()` but misses the dominant `c = get_client(); c.auth.X()` style. Needs simple intra-function alias tracking, otherwise the guard sleeps through real regressions.
6. **MEDIUM — Acceptance/commit-manifest contradictions (Plan 90-02).** `@pytest.mark.xfail` in docstrings will trip its own acceptance grep; the multi-line `xfail(strict=True` decorator won't match the proposed single-line grep; and the `git show` exact-four-paths gate conflicts with the requirement to create `90-02-SUMMARY.md`.

### Lower-priority issues
- Missing unit tests for `admin.sign_out("global")`, `change_password()` 4-header path, and `_apply_user_auth_to_client()` storage headers.
- Sign-out docstring/body contradiction (`sign_out` on throwaway vs `admin.sign_out`).
- Unused `os` import in generated scanner — ruff will fail.
- Plan 90-02 Test C described as thread-oriented but actually sequential.
- No perf sanity check on multi-auth-call page paths.

### Agreed Strengths
- Right diagnosis: deleting `_client_cache` alone is insufficient; the singleton client's auth events mutate global state.
- Pulling `sign_out` revocation into Phase 90 prevents a logout-revocation regression.
- Proactive refresh in `get_user_client()` correctly removes the dependency on per-write reactive retry.
- Atomic delete + install-guards step follows the proven Phase 88/89 discipline.
- Refresh-lock behavioral tests (Tests A/B/C) cover the meaningful properties — same-session serialization, cross-session parallelism, stale-snapshot short-circuit.

### Divergent Views
N/A — single-reviewer run.

## How to incorporate

```
/gsd-plan-phase 90 --reviews
```
