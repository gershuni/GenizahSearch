---
phase: 91
reviewers: [gemini, codex]
reviewed_at: 2026-05-15
plans_reviewed: [91-01-PLAN.md, 91-02-PLAN.md]
reviewers_skipped: [claude]
skip_reason: running inside Claude Code CLI — skipped self-review per workflow independence rule
---

# Cross-AI Plan Review — Phase 91 (Atomic Auth State Writes)

## Gemini Review

# Cross-AI Plan Review: Phase 91 (Atomic Auth State Writes)

Here is the structured review of the Phase 91 implementation plans, focusing on multitenant safety, atomicity, and test resilience.

## Plan 91-01 (Migration & Rollbacks)

**1. Summary**
Plan 91-01 effectively tackles the complex migration of `auth_state.py` and the OAuth callback by applying `safe_storage` helpers and strict multi-write rollback discipline. It smartly avoids full-block atomicity traps (which NiceGUI storage cannot support) and focuses on per-key atomicity with fallback logic to prevent corrupted half-states.

**2. Strengths**
*   **Session-First Ordering in `do_login`**: Writing the session key before calling `set_auth` ensures that if user/profile storage fails, no orphaned login session is left hanging.
*   **`update_profile_cache` Best-Effort**: Correctly identifying that a profile cache update failure during an active session should *not* trigger a full session rollback.
*   **Reshaped T-C Test**: Testing `GlobalAuthState.get_user()` directly validates the actual contract change (`safe_user_get` returning `None` instead of propagating `AssertionError`), which is more deterministic than trying to intercept a mocked redirect.

**3. Concerns**
*   **HIGH: UI State Desync in OAuth Callback (Factoring Risk).** Factoring `complete_login` out to `_oauth_complete_login` passes `status_label` and `show_error_fn`. If the original `complete_login` closure manipulated other UI elements (e.g., hiding a loading spinner, toggling container visibility) that are not fully encapsulated within `show_error_fn`, an early exit during storage failure will leave the UI in a hanging state.
*   **MEDIUM: Observability Loss on Read.** Removing the `try/except Exception` wrappers around `app.storage.user.get()` in `get_auth` means we rely entirely on `safe_user_get`'s internal error handling. If `safe_user_get` silently swallows non-AssertionError exceptions without logging, diagnosing future storage corruption will be extremely difficult.
*   **MEDIUM: Behavioral Shift with `if profile is not None:`**. Changing `if profile:` to `if profile is not None:` means passing `{}` (an empty dict) will now overwrite an existing profile, whereas previously it was skipped. If any upstream caller passes `{}` expecting a no-op, this will clobber valid profile data.
*   **LOW: Concurrent Refresh Race During Rollback.** While unlikely during a *new* login, if a user somehow triggers `do_login` while a background refresh is actively processing an existing token, rolling back `auth_session` via `safe_user_pop` could theoretically clobber the newly refreshed token.

**4. Suggestions**
*   **UI Closure Audit**: Ensure the `show_error_fn` passed from the route handler specifically encapsulates ALL necessary UI recovery steps (hiding spinners, restoring inputs), not just showing the error message.
*   **Log swallowed exceptions**: Verify that `safe_user_get` uses `logging.warning` or `logging.error` before returning `None` on generic exceptions, compensating for the removed wrappers.
*   **Profile Dictionary Guard**: Explicitly verify if any flow (e.g., partial Supabase profile fetches) yields an empty dict `{}` and decide if overwriting the cache with `{}` is the intended behavior.
*   **End-to-End Route Test**: While T-A tests the factored helper perfectly, consider adding a lightweight integration test that actually calls the outer `auth_callback_route` to guarantee the shim correctly wires the closures.
*   **Empty Allowlist Lint Fix**: When modifying `test_no_raw_storage_access.py`, verify that replacing the assertion doesn't inadvertently break other tests in that file (e.g., tests checking for exact allowlist counts).

**5. Risk Assessment**
**MEDIUM.** The core rollback logic is robust, but the structural extraction of the OAuth closure and potential hidden behavioral shifts with empty dicts carry moderate regression risks for the UI flow.

---

## Plan 91-02 (AST Retention Guard)

**1. Summary**
Plan 91-02 implements a precise, AST-based defense-in-depth guard for `persist_value`, locking in the critical fix from Phase 90/Codex 3rd-pass. It correctly scopes the test to function-level validation, complementing the existing file-level lint scanner perfectly.

**2. Strengths**
*   **Precision over Regex**: AST checking completely avoids the fragility of regex or grep, ignoring docstrings and commented-out code.
*   **Dual-Constraint Verification**: It enforces both a negative constraint (no raw assignment) and a positive constraint (must call the helper and read the flag), ensuring the exact required semantic shape.
*   **Isolation Discipline**: Per-test isolated stubs and parsed snippets keep the test suite fast and decoupled.

**3. Concerns**
*   **MEDIUM: Import Aliasing Fragility.** If a future developer reformats the imports to `from web.safe_storage import safe_user_set as safe_set`, the AST `Call` check searching specifically for the string `"safe_user_set"` will fail, breaking the build despite the code being functionally correct and safe.
*   **LOW: Overly Strict Plan Dependency.** The plan states Plan 91-02 depends on Plan 91-01. Since the target code (`filter_panel.py`) was already migrated in a prior commit (`cca23db3`), these two plans are orthogonal and could be executed in parallel.

**4. Suggestions**
*   **Enhance AST Call Check**: Instead of hardcoding the check for the string `"safe_user_set"` in the `Call` node, have the AST walker first inspect the `ImportFrom` node to resolve the actual imported alias, and check the `Call` node against that alias.
*   **Helper Deflection**: Acknowledge in the test comments that if a developer moves the raw assignment into a nested local helper function inside `persist_value`, the AST check on `persist_value` might miss it. (The file-scope linter in Phase 87 mitigates this, but documenting the AST test's boundary is good practice).

**5. Risk Assessment**
**LOW.** The plan strictly adds test infrastructure without modifying production code. The only risk is test flakiness due to future benign refactoring.

---

## Overall Assessment & Global Considerations

**1. Summary**
Overall, Phase 91 represents a highly mature approach to multitenant state safety. By explicitly rejecting the trap of cross-key atomicity in favor of sequential writes with rollback, the plan aligns perfectly with NiceGUI's underlying storage constraints.

**2. Global Strengths**
*   The architectural pivot away from composite keys (Codex F1 catch) shows strong responsiveness to platform limitations.
*   The strict allowance degradation (allowlist going to 0) proves the efficacy of the transitional architecture introduced in Phase 87.

**3. Global Concerns**
*   **MEDIUM: Test Fixture Blindspots.** The plan relies on `tests/test_auth_revocation_and_headers.py`, etc., passing. However, many test suites manually construct authenticated states by directly mutating `app.storage.user["auth_user"]` in their `pytest.fixture` definitions. If these fixtures bypass `safe_user_set`, they might start failing or failing to accurately reflect production behavior.
*   **LOW: Async/Pytest Configuration.** The new tests rely on `@pytest.mark.asyncio`. Ensure `pytest-asyncio` is properly configured in the test environment (e.g., `asyncio_mode = "auto"`) and available in the dependency tree.

**4. Global Suggestions**
*   **Audit Test Fixtures**: Run a global grep for `app.storage.user\['auth_user'\]` across the `tests/` directory to ensure no legacy mock setups are bypassing the new storage paradigms before running the plan-boundary verification.
*   **Document the Atomicity Pivot**: The architectural decision to use "separate keys with explicit rollback" rather than "composite blocks" is a crucial project constraint. This MUST be documented in `docs/guides/MULTITENANT.md` or `docs/OPEN_ISSUES.md` to prevent a future contributor from "optimizing" the keys back into a single block and re-introducing the race conditions.

**5. Overall Risk Assessment**
**MEDIUM.** The core logic changes are solid, but managing the side-effects of closure refactoring and ensuring the test suite fixtures are fully updated to the new storage paradigms will dictate the phase's success. Proceed with implementation.

---

## Codex Review

## Plan 91-01

**Summary**
Strong plan overall. The architectural pivot away from `_auth_block` is correct, and the move to checked `safe_user_set`/`safe_user_pop` calls is the right shape for NiceGUI's non-transactional storage. The main gap is that the rollback model still leaves stale `auth_profile` possible, which matters because role checks read profile independently of `auth_user`.

**Strengths**
- Correctly rejects composite auth state; avoids new read-modify-write races with `_refresh_user_session()`.
- Keeps the 3-key contract, which matches current `web/supabase_client.py` and existing tests.
- Session-first write ordering in `do_login`/OAuth is mostly right: a user/profile without tokens is worse than an orphaned session.
- Empty allowlist transition is handled; the current scanner really does hard-fail on empty allowlist.
- Factoring `_oauth_complete_login` is a reasonable test seam and should not introduce a serious import cycle if imports stay lazy or use already-safe modules.

**Concerns**
- **HIGH:** `set_auth` rollback only pops `auth_user`. It does not pop `auth_profile`. If a previous profile remains, `GlobalAuthState.get_role()`, `is_admin()`, and `is_editor()` can still return stale role data even when `auth_user` was rolled back or when the new login has `profile is None`. I found several role checks that are not always paired with `is_logged_in()`.
- **MEDIUM:** `profile is None` is not treated as "clear the old profile." This preserves a pre-existing bug surface and is more important than the `{}` truthiness change.
- **MEDIUM:** AUTHW-05 tests as described do not directly exercise partial-write rollback. T-A fails on the first session write, so it never verifies `auth_session` success followed by `auth_user`/`auth_profile` failure.
- **MEDIUM:** A refresh race with rollback is unlikely for fresh login tokens, but not impossible in principle: a concurrent refresh could read `auth_session` before rollback and write a rotated `auth_session` after rollback. Keeping separate keys avoids the worst races, but this residual should be documented as "orphan session possible, UI logged out," not fully closed.
- **MEDIUM:** The new tests plan to use `@pytest.mark.asyncio`, but this repo has no `pytest-asyncio` or `asyncio_mode` in `requirements*.txt`/`pyproject.toml`. Use `asyncio.run()` in sync tests or explicitly add/configure the dependency.
- **LOW:** The success-criteria wording around `sign_out` should match the Phase 90 implementation. Current code uses `throwaway.auth.admin.sign_out(access_token, "global")`; do not reintroduce high-level `client.auth.sign_out()` if Phase 90 proved it can no-op.

**Suggestions**
- Change `set_auth` so it always normalizes profile state: write `auth_profile` when `profile is not None`, and pop `auth_profile` when `profile is None`. If that clear/write fails during login establishment, return `False` and roll back `auth_user`.
- On any failed auth establishment, roll back all local auth keys best-effort: `auth_session`, `auth_user`, and `auth_profile`.
- Add direct tests for:
  - `set_auth`: user write succeeds, profile write fails, both `auth_user` and stale `auth_profile` are cleared.
  - `do_login`/OAuth: session write succeeds, user/profile write fails, all 3 keys are cleaned up and no navigation occurs.
  - `profile is None` after a previous profile exists clears the old profile.
- Add a small route-level or UI-side test, or at least a test using the real `show_error` closure, to prove the visible callback page state changes, not only that `show_error_fn(...)` was called.
- Replace async pytest markers with `asyncio.run(_oauth_complete_login(...))` unless `pytest-asyncio` becomes a pinned test dependency.

**Risk Assessment: MEDIUM**
The strategy is sound, but the stale-profile/role issue is significant enough that I would treat Plan 91-01 as needing revision before execution.

---

## Plan 91-02

**Summary**
Good defense-in-depth plan for retaining the `persist_value` safe-wrap. The function-local AST approach is useful, but the proposed assertions are shape checks, not full behavioral checks.

**Strengths**
- Function-local AST guard is better than grep for this specific contract.
- Seed-trap tests are useful for proving the scanner is checking what the authors think it checks.
- No production-code touch keeps the plan low blast-radius.
- It complements the Phase 87 file-scope scanner instead of replacing it.

**Concerns**
- **MEDIUM:** The 3 AST assertions can pass while behavior is broken. Example: `persist_value` could call `safe_user_get("session_persistence_enabled", True)` and ignore the result, then call `safe_user_set(...)` unconditionally.
- **LOW:** The check may false-fail a legitimate refactor where `persist_value` delegates to a helper that preserves behavior.
- **LOW:** The dependency on 91-01 is organizational, not technical. This guard could be parallelized because `filter_panel.py:persist_value` is already safe-wrapped.

**Suggestions**
- Add one behavioral test in addition to AST:
  - monkeypatch `web.safe_storage.app.storage.user` to `{"session_persistence_enabled": False}`;
  - call `persist_value("x", 1)`;
  - assert `"x"` was not written.
  - repeat with `True` and assert it was written.
- Strengthen the AST check to verify `safe_user_set(key, value)` specifically, not just any `safe_user_set` call.
- Keep the raw-subscript negative assertion, but rely on the Phase 87 scanner for broader raw-helper regressions.

**Risk Assessment: LOW**
This is test-only and useful. The main risk is false confidence from incomplete AST semantics, easily fixed with one behavioral test.

---

## Overall

**Summary**
The phase is directionally correct and much better than the rejected composite-key design. The biggest remaining issue is not "true atomicity" across NiceGUI keys, which is unavailable; it is stale auth profile cleanup. Because role helpers read `auth_profile` independently, rollback must treat stale profile as security-relevant state, not harmless UI cache.

**Strengths**
- Correct architectural pivot: 3 independent keys plus checked writes is the pragmatic model for this storage backend.
- Phase 87 scanner empty-allowlist transition is correctly identified.
- Existing key-contract tests mostly cover the right compatibility surface; my grep found the named test files are the relevant literal-key test files.
- Phase 92 owning `docs/guides/MULTITENANT.md` is fine; no need to force that into Phase 91.

**Concerns**
- **HIGH:** Stale `auth_profile` can survive failed/partial login unless explicitly cleared.
- **MEDIUM:** Rollback behavior is under-tested exactly where the plan's security claim is strongest.
- **MEDIUM:** `update_profile_cache` as best-effort is acceptable only if it cannot write a profile for a no-longer-current user. Consider checking `profile["id"] == auth_user["id"]` before writing.
- **LOW:** `docs/OPEN_ISSUES.md` should get a narrow note only if Phase 91 changes the P1 storage-audit status; do not mark the broader memory leak fixed.

**Suggestions**
- Make rollback cleanup symmetric and stale-safe: all failed login establishment paths should best-effort clear `auth_session`, `auth_user`, and `auth_profile`.
- Add explicit stale-profile regression tests before relying on the plan's atomicity claim.
- Use `asyncio.run()` unless test dependencies are updated.
- Update roadmap wording for logout revocation to preserve the Phase 90 `admin.sign_out(access_token, "global")` invariant.

**Overall Risk Assessment: MEDIUM**
The plan is close, but I would not execute it as-is. Fix stale profile cleanup and add partial-write rollback tests; after that, the residual NiceGUI non-transactional windows are acceptable for Phase 91.

---

## Consensus Summary

Both reviewers agree the **architectural pivot is correct** and the plan shape is sound, but **both raise MEDIUM overall risk** and recommend revising Plan 91-01 before execution. Two issues surfaced independently by both reviewers; three more by one each.

### Agreed Strengths

- **Architectural pivot from composite-key to 3-key + checked writes is the right call** for NiceGUI's non-transactional storage (Gemini, Codex).
- **Session-first write ordering in `do_login`/OAuth is correct** — orphan session is recoverable; orphan user/profile is not (Gemini, Codex).
- **Empty-allowlist transition correctly identified as a blocker** Codex F3 caught and D-07 encodes (Gemini, Codex).
- **AST retention guard for `persist_value` is the right tool** — function-local precision beats grep; seed traps prove the scanner works (Gemini, Codex).

### Agreed Concerns (HIGHEST PRIORITY)

- **HIGH/HIGH: pytest-asyncio is NOT in test deps** (Codex) — both reviewers flagged the async config gap (Gemini at LOW, Codex at MEDIUM); plan to use `@pytest.mark.asyncio` but repo has no `pytest-asyncio` install/config. **MUST fix**: either pin `pytest-asyncio` + add `asyncio_mode = "auto"` to pytest config, OR rewrite tests to wrap calls in `asyncio.run(...)`.
- **HIGH: Stale `auth_profile` survives partial rollback** (Codex, implicit Gemini concern about UI desync) — `set_auth`'s D-04 rollback only pops `USER_KEY`, never `PROFILE_KEY`. Role helpers (`get_role`, `is_admin`, `is_editor`) read profile independently of user. A new login with `profile is None` does NOT clear stale profile. **Codex calls this a security issue** because not all role checks pair with `is_logged_in()`. **Likely MUST fix**: extend D-04 rollback to also pop `PROFILE_KEY` on any rollback path, AND treat `profile is None` in `set_auth` as "clear the old profile" (pop on None).
- **MEDIUM/MEDIUM: AUTHW-05 tests under-exercise the rollback path** (Codex; Gemini suggests "end-to-end route test"). T-A fails on the FIRST session write, never verifying the actual partial-write rollback scenario (session succeeds → user/profile fails → all keys cleaned). **Recommendation**: add direct partial-write rollback tests for both `set_auth` and `_oauth_complete_login`/`do_login`.
- **MEDIUM/MEDIUM: Closure factoring observability/UI risks** (Gemini HIGH, Codex implicit) — Gemini worries the route's `show_error` closure may do more than emit a message (hide spinner, container visibility); passing it through `show_error_fn` risks UI desync. Codex less concerned but agrees a real-`show_error` test would be valuable. **Recommendation**: audit `web/main.py:show_error` body before Plan 91-01 execution; verify it's pure UI-state via the closure and not coupled to other route-local state.
- **MEDIUM/MEDIUM: AST retention guard's 3 assertions are shape-only, not behavioral** (Codex; Gemini import-aliasing concern is related) — `persist_value` could call `safe_user_get('session_persistence_enabled')` and ignore the result, then call `safe_user_set` unconditionally; all 3 AST assertions pass. **Recommendation**: add one behavioral test that monkeypatches storage with `session_persistence_enabled = False`, calls `persist_value`, and asserts no write occurred.

### Divergent Views

- **Plan 91-02 dependency on 91-01**: Both reviewers flag this as unnecessary (LOW), but Codex specifically notes it could parallelize (`depends_on: []` instead of `["91-01"]`). Plan execution may be marginally faster with parallel waves. NOT a correctness issue — purely a sequencing optimization.
- **Empty-dict behavior change** (`if profile:` → `if profile is not None:`): Gemini flags as MEDIUM regression risk (could clobber profile with `{}`). Codex says this is less important than the bigger `profile is None` should-clear gap. **Synthesis**: the codebase should be audited for callers passing `{}` (likely zero — Supabase profiles always have at least an `id`), AND `profile is None` should clear stale profile (Codex's larger fix).
- **`update_profile_cache` best-effort**: Gemini calls out as a STRENGTH (correctly best-effort). Codex says "acceptable only if it cannot write a profile for a no-longer-current user; consider checking `profile['id'] == auth_user['id']` before writing." **Synthesis**: Codex's mitigation is reasonable and cheap; add a sanity check.

### Recommended Plan Revisions Before Execution

1. **MUST: Resolve pytest-asyncio dependency.** Verify `pytest-asyncio` is in `requirements*.txt` AND configured in pyproject/setup.cfg (`asyncio_mode = "auto"`). If absent, either add it (cleaner) OR rewrite tests with `asyncio.run(...)` (lower-risk). Check `tests/test_refresh_lock_per_session.py` for the existing pattern.

2. **MUST: Extend D-04 rollback to symmetric 3-key cleanup.** On any failed-login rollback path (`set_auth` profile-write failure, `do_login`/`complete_login` set_auth failure), pop `auth_session`, `auth_user`, AND `auth_profile`. Treat `profile is None` in `set_auth` as "pop the profile key".

3. **MUST: Add direct partial-write rollback tests.** Two new tests in `test_auth_callback_resilience.py`:
   - `set_auth(user, profile)` where `auth_user` write succeeds but `auth_profile` fails → assert both `auth_user` and stale `auth_profile` cleared.
   - `_oauth_complete_login` where session write succeeds but `set_auth` fails → assert all 3 keys cleaned + no navigate.

4. **SHOULD: Audit `web/main.py:show_error` for hidden UI state coupling.** If it does more than `error_label.text = msg; error_label.visible = True; spinner.visible = False`, document the coupling and verify the factored helper still triggers all of it.

5. **SHOULD: Add behavioral test for `persist_value` in Plan 91-02.** Monkeypatch storage with `session_persistence_enabled = False/True`, call `persist_value`, assert write absent/present.

6. **MAY: Audit codebase for callers passing `{}` as profile.** Likely zero, but cheap to confirm. Audit test fixtures for raw `app.storage.user['auth_user'] = ...` setups (Gemini's global suggestion).

7. **MAY: Parallelize Plan 91-02 with 91-01.** Change `depends_on: []` since persist_value is already safe-wrapped. Minor speedup.

8. **MAY: Add `update_profile_cache` sanity guard.** Skip write if `profile['id'] != auth_user['id']` (Codex's defense against profile-for-wrong-user). Cheap, defensive, and closes a residual stale-state surface.

### Risk Synthesis

| Plan | Gemini Risk | Codex Risk | Recommended Action |
|------|-------------|-----------|---------------------|
| 91-01 | MEDIUM | MEDIUM (needs revision) | **Revise before execution** — items 1, 2, 3 above are blocking |
| 91-02 | LOW | LOW | **Execute after item 5 added** — behavioral test fills the shape-only gap |
| Overall | MEDIUM | MEDIUM | Both reviewers landed at MEDIUM; ship after addressing the agreed HIGH items |

**Next step:** Update `91-CONTEXT.md` decisions D-04, D-05, D-06 to reflect symmetric 3-key rollback + `profile is None` clearing; expand AUTHW-05 test list to include partial-write scenarios; resolve pytest-asyncio config; then either re-plan or update plans in place via `/gsd-plan-phase 91 --reviews`.
