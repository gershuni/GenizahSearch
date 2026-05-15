---
phase: 89
reviewers: [gemini, codex]
reviewed_at: 2026-05-15
plans_reviewed: [89-01-PLAN.md, 89-02-PLAN.md]
---

# Cross-AI Plan Review — Phase 89

## Gemini Review

# Cross-AI Plan Review — Phase 89

## Summary
The plans for Phase 89 are exceptionally well-thought-out and effectively address the critical architectural flaw of singleton-based cross-user state leakage. Plan 89-01 correctly transitions `UserListsManager` to a stateless, per-access factory model, which inherently solves both the cross-user leak and the UI-callback stale-capture bug (Codex catch). Plan 89-02 executes a precise and safe deletion of the singleton plumbing, reinforced by rigorous AST and runtime assertions. The plan boundary is solid, but minor adjustments to test execution order and expected failures will ensure a smooth, CI-green integration.

## Strengths
- **Stateless Per-Access Factory (D-02, D-04):** The shift to a stateless `UserListsManager` instantiated dynamically via `AppState.lists_mgr` cleanly eliminates the risk of cross-user leakage and state corruption without heavy refactoring.
- **UI Callback Bug Fix (D-03):** Recognizing that UI callbacks capture manager instances into closures and moving to stateless fetching is a brilliant catch. It ensures late-invoked callbacks fetch fresh data rather than serving stale data indefinitely.
- **Backward Compatibility (D-05, D-06):** Making `invalidate_cache()` a no-op and adding the `color=None` default ensures a blast-radius-free transition for existing web mutations and desktop clients.
- **Comprehensive Verification:** The combination of behavior-driven isolation tests (Plan 89-01 Task 3), AST scanners (D-10), and runtime absence checks (D-11) forms a highly robust, multi-layered safety net.
- **Strict Atomicity (D-09):** The requirement for a 3-file atomic commit correctly anticipates and prevents transient CI failures associated with the Phase 88 survivor test.

## Concerns
- **CI Breakage via Expected-Failing Tests (MEDIUM):** Plan 89-02 Tasks 1 and 2 introduce "expected-failing" tests prior to Task 3's implementation. If these tasks correspond to separate git commits in a PR, the CI pipeline will turn red between Tasks 1/2 and Task 3.
- **AST Scanner False Positives (LOW):** Flagging `_cache_entry` and `_cache_ttl` *anywhere* in the codebase (Plan 89-02 Task 1) is slightly aggressive. While safe today, if another module (e.g., a generic caching utility) later uses these highly generic terms, the AST test will fail inappropriately.
- **Callback Auth State Expiration (LOW):** With UI callbacks capturing the stateless manager and fetching late, a callback fired *after* a user's session has expired (or network drops) might attempt a Supabase fetch and fail. The original 10s cache might have masked this by serving stale data.
- **Missing Error Handling Specifics (LOW):** Plan 89-01 Task 1 mentions `_get_cached_data()` will "always call `get_user_lists`... build dict, return." It does not explicitly state that the network/database error handling from the original method must be preserved.

## Suggestions
- **Consolidate or Xfail Plan 89-02 Tasks:** To maintain a clean, bisectable git history, either bundle Tasks 1, 2, and 3 into a single massive atomic commit, OR use `@pytest.mark.xfail(reason="Phase 89 implementation pending")` for the new tests in Tasks 1 and 2, and remove the marker during Task 3.
- **Scope the AST Scanner:** Limit the `_cache_entry` and `_cache_ttl` AST scan strictly to `web/user_lists.py` and `web/state.py` rather than scanning the entire codebase. This prevents future naming collisions while still protecting the target files.
- **Explicit Error Handling Preservation:** Explicitly instruct the implementation of the new stateless `_get_cached_data()` in Plan 89-01 to retain or adapt any existing `try/except` blocks (like logging or fallback behaviors) from the original method.
- **Double-Check Desktop `ListsManager`:** Verify that `ListsManager` in `genizah_core.py` doesn't have an overridden `create_project` definition in a desktop-specific subclass (e.g., in `desktop/`) that might also need the `color=None` signature update to remain compatible.

## Risk Assessment
**LOW**
The risk is low because the implementation replaces complex, stateful caching with a much simpler, stateless per-request model. The rigorous backward compatibility strategies (no-op invalidation, optional parameters) and extensive testing mitigate almost all structural risks. The cycle concerns around the lazy import (D-01) are perfectly handled by standard Python property-level imports. Addressing the test-ordering CI concern will make this execution virtually foolproof.

---

## Codex Review

I checked the current code around `web/state.py`, `web/user_lists.py`, `genizah_core.py`, and the Phase 88 survivor test. The plans are directionally sound, but Plan 89-02 has a real test-ordering/scanner self-conflict risk that should be fixed before implementation.

### PLAN 89-01

**Summary**

Strong plan. It removes the dangerous per-instance cache and changes `state.lists_mgr` into the intended factory access point. The main risk is that the proposed isolation test must prove the late-callback/captured-manager case, not only fresh property accesses, and it must patch `GlobalAuthState.is_logged_in()` as well as `get_user_id()`.

**Strengths**

- Correctly deletes `_cache_entry` / `_cache_ttl`, which fixes both the original cross-user leak and the stale captured-dialog-manager bug.
- The factory-property shape preserves the pre-bootstrap `None` guard, which current call sites rely on.
- Keeping `invalidate_cache()` as a no-op is the right compatibility move if every read now fetches fresh authenticated data.
- The `create_project(name, color=None)` fix is backwards-compatible for desktop callers that pass only `name`.

**Concerns**

- **HIGH:** `test_authenticated_fetch_does_not_leak_across_users` only works if it patches `GlobalAuthState.is_logged_in()` to `True`; patching `get_user_id()` alone may exercise the default/local path instead of Supabase.
- **HIGH:** The cross-user test should use the same captured `UserListsManager` instance while switching User A to User B. That proves the D-03 callback-capture issue. Two fresh `state.lists_mgr` accesses only prove factory behavior.
- **MEDIUM:** `state.lists_mgr` is accessed repeatedly in expressions like `if state.lists_mgr: state.lists_mgr...`; after the change those are separate wrapper instances. That is acceptable only because the wrapper is truly stateless. Tests should lock that in.
- **MEDIUM:** `_get_cached_data()` should handle the edge case where `is_logged_in()` is true but `get_user_id()` returns `None`, instead of calling Supabase with `None`.
- **LOW:** Remove now-unused `time` and `Tuple` imports from `web/user_lists.py`.

**Suggestions**

- Add/shape the leak test as: create one `mgr = UserListsManager(...)`, patch logged-in true, return User A, call `mgr.data`, switch to User B, call `mgr.data`, assert Supabase was called twice with `user-A` and `user-B`.
- In `genizah_core.ListsManager.create_project`, store `color or self._get_next_project_color()` so desktop behavior remains identical.
- Make the delegation audit explicit: fail or document "no drift beyond `create_project` found," rather than deferring discovered runtime-incompatible drift.
- Keep `refresh_data()` behavior meaningful: no-op invalidation followed by `_get_cached_data()` is fine because it forces a fetch under the stateless model.

**Risk Assessment**

**MEDIUM.** The implementation direction is right, but the test must specifically cover captured-manager late invocation and authenticated Supabase access. Without that, the main Phase 89 bug class is only partially proven.

### PLAN 89-02

**Summary**

The deletion plan is good and correctly calls out the D-09 atomicity hazard. The biggest issue is the proposed scanner/test ordering: a scanner that scans `tests/` for `hasattr(..., "_cache_entry")` / `"_user_lists_mgr"` can flag the new runtime attr-absence tests themselves unless those guard files are excluded or the scanner scope is narrowed.

**Strengths**

- Correctly deletes all singleton entry points: field, setter update block, `init_user_lists_mgr()`, and startup call.
- Correctly updates `tests/test_no_appstate_export_fields.py` in the same commit as field deletion. This is essential.
- Runtime absence tests for `_user_lists_mgr`, `_cache_entry`, and `_cache_ttl` are valuable regression guards.
- AST scanner is the right class of guard for preventing direct/indirect restoration.

**Concerns**

- **HIGH:** Static scanner over `web/+tests/` may conflict with `tests/test_no_user_lists_mgr_field.py`, because that test necessarily uses `hasattr(..., "_user_lists_mgr")`, `"_cache_entry"`, and `"_cache_ttl"`.
- **HIGH:** "Expected-failing until Task 3" is risky. Avoid landing or running intentionally failing production-scan tests between scanner creation and deletion.
- **HIGH:** D-09 should be enforced operationally: only these three files should be staged in that commit: `web/state.py`, `web/main.py`, `tests/test_no_appstate_export_fields.py`.
- **MEDIUM:** Scanner should also catch calls like `state.init_user_lists_mgr()` / `.init_user_lists_mgr(...)`, not only `def init_user_lists_mgr`.
- **MEDIUM:** `_cache_entry` / `_cache_ttl` flagged anywhere is fine for production, but too aggressive for guard tests unless allowlisted.
- **LOW:** The lazy import in `lists_mgr` looks acceptable. `web.user_lists` does not appear to import `web.state`, so cycle risk is low; Python import caching makes per-access import cost negligible.

**Suggestions**

- Reorder Plan 89-02 to avoid expected-failing states: either add the scanner after deletion, or add seed-trap tests first and enable production scanning only after Task 3.
- Exclude the scanner test file and runtime attr-absence test file from the production scan, or scan only `web/` plus selected non-guard tests.
- Add seed traps using `ast.parse(...)` as planned, but ensure raw string mentions in tests/comments do not count as production failures.
- Before the D-09 commit, run `git status --short` and stage exactly the three required files. After commit, verify with `git show --stat --name-only HEAD`.
- Add one scanner case for deleted method calls: `state.init_user_lists_mgr()`.

**Risk Assessment**

**MEDIUM-HIGH** as written, mostly due to scanner/test self-conflict and expected-failing sequencing. If those are fixed, risk drops to **MEDIUM-LOW** because the deletion itself is straightforward and well scoped.

### Overall

The two-plan split is reasonable: 89-01 changes behavior safely first, 89-02 deletes the obsolete singleton surface and installs guards. The main implementation bar is that tests must prove the captured stateless manager case, and the static scanner must not fail on the very tests that verify absence. D-09 is correctly identified as critical and should be treated as a staging/commit invariant, not just a code-edit note.

---

## Consensus Summary

Both reviewers agree the plans are directionally sound and address the right bug class (cross-user singleton leak + UI-callback stale capture). Gemini lands at LOW overall risk; Codex lands at MEDIUM (89-01) / MEDIUM-HIGH (89-02 as written). The disagreement is about how serious the test-sequencing problems are — Gemini treats them as fixable polish, Codex treats them as load-bearing.

### Agreed Strengths

- **Stateless + per-access factory is the correct architecture.** Both reviewers flag this as a "brilliant catch" because it neutralizes both the original cross-user leak AND the late-callback captured-manager stale-data class.
- **Backwards compatibility is well handled.** `invalidate_cache()` as no-op (D-05) and `create_project(name, color=None)` (D-06) preserve all existing call sites.
- **D-09 atomic-commit requirement is correctly identified** as critical. Both reviewers reinforce it.
- **Multi-layer regression guards** (3 behavior tests + AST scanner + runtime attr-absence test) form a robust safety net.

### Agreed Concerns (highest priority)

1. **AST scanner false-positive risk on its own companion test files (HIGH per Codex, LOW per Gemini).** The scanner over `web/+tests/` will flag `tests/test_no_user_lists_mgr_field.py` because that file legitimately uses `hasattr(..., "_user_lists_mgr")` / `"_cache_entry"` / `"_cache_ttl"` as test literals. The plan already exempts these files via `EXEMPT_FILES` in Plan 89-02 Task 1 — **verify the exemption actually works** before merging.
2. **Expected-failing test sequencing between Plan 89-02 Tasks 1-2 and Task 3 leaves the suite intentionally red mid-plan.** Both reviewers flag this. Within a single plan execution (no PR splits), this is acceptable; if executed across separate commits/CI runs, this is a problem.
3. **AST scanner scope on `_cache_entry`/`_cache_ttl` flagged anywhere is too aggressive.** Both reviewers suggest scoping these two names to `web/user_lists.py` + `web/state.py` only (or to `web/` only), to avoid future naming collisions with unrelated cache utilities.
4. **D-09 atomic commit needs operational enforcement, not just a code-edit note.** Codex recommends `git status --short` pre-commit + `git show --stat --name-only HEAD` post-commit verification. Both reviewers reinforce this is the most fragile point.

### Divergent Views

- **`test_authenticated_fetch_does_not_leak_across_users` design (Codex HIGH, Gemini didn't flag).** Codex argues the test as currently planned uses *two distinct* `state.lists_mgr` accesses (fresh-instance-per-user) which only proves the factory behavior — not the captured-manager case that D-03 was meant to defend against. Codex recommends:
  - Create ONE `mgr = UserListsManager(...)` (or `mgr = state.lists_mgr`)
  - Patch logged-in=True, user_id=user-A, call `mgr.data`
  - Switch to user_id=user-B, call `mgr.data` on the SAME `mgr`
  - Assert both Supabase calls happened with distinct user_ids
  - This proves the captured-manager case directly, which is the actual D-03 bug.
- **Need to patch `GlobalAuthState.is_logged_in` too (Codex HIGH, Gemini didn't flag).** The current Plan 89-01 Task 3 test code does patch `is_logged_in` — Codex's concern appears already addressed; worth verifying.
- **`get_user_id()` returns None edge case (Codex MEDIUM, Gemini didn't flag).** Currently the stateless `_get_cached_data()` would call Supabase with `None` if `is_logged_in()` is true but `get_user_id()` returns None. Probably can't happen, but a defensive `if not user_id: return self._get_default_data()` is cheap.
- **Desktop subclass check (Gemini LOW, Codex didn't flag).** Gemini suggests verifying no desktop-specific `ListsManager` subclass overrides `create_project`. The CONTEXT.md already documents `genizah_app.py:12237, 12996` as the two desktop call sites (both single-arg), but a subclass search is one more grep.
- **Risk delta.** Gemini = LOW; Codex = MEDIUM (89-01) + MEDIUM-HIGH (89-02). Codex's higher risk grade is driven by the sequencing and scanner-self-conflict issues; if those are addressed before execution, both converge to LOW.

## Recommended Plan Edits Before Execution

1. **Plan 89-01 Task 3 test redesign:** Add a 4th test (or reshape `test_authenticated_fetch_does_not_leak_across_users`) that uses ONE captured manager across two `user_id` patches to prove the D-03 captured-manager case directly.
2. **Plan 89-01 Task 1 `_get_cached_data`:** Add defensive `if not user_id: return self._get_default_data()` after the `is_logged_in()` check, and preserve any existing `try/except` blocks from the original method.
3. **Plan 89-01 Task 1 cleanup:** Explicitly remove unused `time` and `Tuple` imports from `web/user_lists.py`.
4. **Plan 89-02 Task 1 scanner scope:** Restrict `_cache_entry` / `_cache_ttl` anywhere-flag to `web/user_lists.py` + `web/state.py` instead of `web/+tests/` global.
5. **Plan 89-02 Task 1 scanner:** Add a `Call` node check for `state.init_user_lists_mgr()` (call-site, not just FunctionDef) — covers the case where someone restores the method body without the call.
6. **Plan 89-02 Task 3 (D-09):** Add explicit pre-commit `git status --short` verification step and post-commit `git show --stat --name-only HEAD` verification step. Promote D-09 from "edit note" to "staging-invariant + commit-invariant."
7. **Plan 89-02 expected-failing test sequencing:** Either (a) bundle Tasks 1-3 into one commit, or (b) `@pytest.mark.xfail(reason="...")` on the two known-failing tests until Task 3, then remove the marker in Task 3.

To incorporate these into the plans, run `/gsd-plan-phase 89 --reviews`.
