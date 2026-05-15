---
phase: 91
reviewers: [codex]
reviewed_at: 2026-05-15T13:48:22Z
plans_reviewed:
  - 91-01-PLAN.md
  - 91-02-PLAN.md
notes: |
  Gemini was attempted in parallel but failed with HTTP 429 rate-limit / quota
  exhaustion (account-side, repeated retries also 429); user opted to skip
  rather than retry. This round-2 review is single-reviewer (Codex).
  Round-1 reviews (Gemini + Codex) are preserved in git history at
  e5447a93 "docs: cross-AI review for phase 91" -- their MUST/SHOULD/MAY
  items are already integrated as Revisions MUST-1..5 + SHOULD-6 in the
  current plans.
---

# Cross-AI Plan Review -- Phase 91 (Round 2)

This is a **second-round** cross-AI review after the plans were revised
in-place per round-1 feedback (commits `d881c4e5` replan + `e5447a93`
round-1 REVIEWS). Round-1 surfaced MUST-1..5 + SHOULD-6 items that have
already been encoded into both PLAN files; this round audits whether the
revisions were applied correctly **and** looks for new issues that
round-1 missed.

## Codex Review

## Overall Assessment

The revised Phase 91 direction is strong: keeping the three existing auth keys and routing writes through `safe_storage` is the right architectural move, and the plans mostly encode the prior review findings. However, Plan 91-01 still has a few execution-breaking issues and one important test-coverage gap around the stale `auth_profile` leak that the revision is supposed to close. Plan 91-02 is lower risk, but its "single-test-file" framing conflicts with the documentation-update task.

## Plan 91-01 Review

### Summary

Plan 91-01 is directionally correct and covers the main phase goal: remove raw auth storage writes, handle `safe_user_set(False)` explicitly, and empty the Phase 87 allowlist. The rollback model is also improved beyond the original context. But as written, it has several concrete problems that would likely break plan-boundary verification: `_oauth_complete_login` references `GlobalAuthState` without a module-level or helper-local import, `auth_state.py` would leave an unused `app` import, and the new test file imports unused `pytest`. More importantly, T-D and T-E do not actually seed stale auth keys, so they would not catch the stale-profile rollback bug that motivated the revision.

### Strengths

- Correctly rejects composite `_auth_block` consolidation and avoids introducing refresh read-modify-write races.
- Preserves Phase 90 revoke-before-pop behavior while migrating the final raw pops.
- Adds explicit handling for `safe_user_set(False)` instead of allowing silent half-login.
- Factoring OAuth completion into `_oauth_complete_login` is a good testability move.
- Empty-allowlist fix is correctly identified as required before deleting the last two allowlist blocks.
- Avoiding `pytest-asyncio` via `asyncio.run()` is reasonable for these one-shot helper tests.

### Concerns

- **HIGH:** `_oauth_complete_login` will fail with `NameError` unless `GlobalAuthState` is imported in helper scope or module scope. Current `web/main.py:1427` imports `GlobalAuthState` only inside `auth_callback_route`; a module-level helper cannot see that local binding.
- **HIGH:** Ruff will fail after Task 1 unless `from nicegui import app, ui` in `web/auth_state.py:11` becomes `from nicegui import ui`. After migration, `app` is unused and `ruff.toml` enforces `F401`.
- **HIGH:** `tests/test_auth_callback_resilience.py` imports `pytest` but does not use it. Plan 91-01's own ruff check includes that file, so this is another `F401` failure.
- **HIGH:** T-D does not prove stale `auth_profile` cleanup on profile-write failure. It starts with empty storage, so a buggy rollback that only pops `auth_user` would still pass. This misses the exact Codex HIGH stale-role leak.
- **HIGH:** T-E does not prove defensive all-3-key cleanup. It starts with no stale `auth_user` or `auth_profile`, so a buggy outer rollback that only pops `auth_session` would still pass.
- **MEDIUM:** The plan repeatedly calls `set_auth` rollback "3-key" rollback, but `set_auth` only writes user/profile. The actual 3-key cleanup is in `do_login` and `_oauth_complete_login`. The wording can mislead future implementers.
- **MEDIUM:** Several verification commands are Unix-style `grep | tail` commands in a PowerShell repo context. That risks false execution failures; prefer `rg` or Python snippets.
- **LOW:** Direct `login_failed` capture plus `show_error` capture creates duplicate PostHog events. The plan documents it, but metrics consumers may need filtering.
- **LOW:** `do_login` storage-failure telemetry lacks `method: 'password'`, despite the threat model saying password and OAuth failures are method-tagged.

### Suggestions

- Add inside `_oauth_complete_login`:
  ```python
  from web.auth_state import GlobalAuthState
  ```
  or add a module-level import if circularity is confirmed safe.
- Change `web/auth_state.py:11` import to `from nicegui import ui` after migration.
- Remove `import pytest` from `tests/test_auth_callback_resilience.py`.
- Strengthen T-D by pre-seeding stale profile:
  ```python
  storage = _RoutingStorage(
      {'auth_profile': {'role': 'admin'}},
      fail_writes_for={'auth_profile'},
  )
  ```
  Then assert `auth_profile` absent and `GlobalAuthState.get_role() is None`.
- Strengthen T-E by pre-seeding stale `auth_user` and `auth_profile` before the session succeeds and `auth_user` write fails. Then assert all three are absent.
- Rename plan wording from "set_auth 3-key rollback" to "set_auth user/profile symmetric rollback; callers perform 3-key cleanup."
- Convert verification greps to `rg` or Python so the plan is reliable on Windows.

### Risk Assessment

**Risk: MEDIUM-HIGH.** The architecture is right, but the current plan has execution blockers and insufficient tests for the highest-value rollback bug. These are straightforward to fix before implementation, but I would not approve Plan 91-01 as-is.

## Plan 91-02 Review

### Summary

Plan 91-02 is mostly solid. The AST guard plus behavioral test directly addresses the shape-only weakness in the original retention test idea. The strict `safe_user_set(key, value)` argument check is useful and appropriately scoped. The main issue is plan hygiene: the plan claims single-test-file atomicity, but Task 2 updates `.planning/STATE.md`, `.planning/ROADMAP.md`, `CLAUDE.md`, and maybe `docs/OPEN_ISSUES.md`, while frontmatter lists only the test file.

### Strengths

- Combines AST contract checks with a behavioral test, closing the "flag read is ignored" loophole.
- Strictly verifies `safe_user_set` receives the function's own `key` and `value` parameters.
- Seed-trap snippets are useful sanity checks for scanner logic.
- Explicitly documents intentional brittleness around import aliases.
- No production code changes are needed for AUTHW-06.
- T-Beh uses the established `web.safe_storage.app` monkeypatch pattern correctly.

### Concerns

- **MEDIUM:** Frontmatter says only `tests/test_persist_value_uses_safe_storage.py` is modified, but Task 2 modifies milestone docs. This conflicts with the "single-test-file atomic commit" claim.
- **MEDIUM:** If the desired discipline is truly atomic CI guard, docs finalization should be a separate plan or at least reflected in `files_modified`.
- **LOW:** The raw-write AST check only looks at `ast.Assign` subscript targets. File-scope lint still catches other raw accesses, so this is not a functional gap, but the test's own claim is slightly broader than its implementation.
- **LOW:** The test intentionally rejects import aliasing. That is acceptable, but future maintainers should see a clear failure message saying "update this retention test if aliasing is intentional."

### Suggestions

- Either keep Plan 91-02 as a true single-test-file plan and move STATE/ROADMAP/CLAUDE updates to a small Phase 91 closeout plan, or add those docs to `files_modified` and stop calling it single-test-file.
- Add a helper assertion function shared by production AST tests and seed traps to reduce duplicated scanner logic.
- Consider adding one negative seed trap for "flag read ignored, write unconditional" if you want the behavioral rationale encoded in tests, not just docstring.

### Risk Assessment

**Risk: LOW-MEDIUM.** The technical test design is good and likely to work. The main risk is process inconsistency and documentation-scope creep, not code behavior.

## Final Recommendation

Revise before execution. Plan 91-02 only needs cleanup around scope/documentation. Plan 91-01 needs concrete fixes for `GlobalAuthState` import visibility, ruff failures, and stale-key test seeding. Once those are addressed, the two-plan structure should achieve the Phase 91 goals.

---

## Consensus Summary

Single-reviewer round (Codex only; Gemini failed with 429 rate-limit and was skipped per user direction). The findings below are Codex-only but treated as the authoritative round-2 consensus -- they are NEW issues that round-1's Gemini + Codex review did not surface (round-1 caught the architectural F1/F3/HIGH-stale-profile items already integrated as Revisions MUST-1..5 + SHOULD-6).

### New HIGH-severity findings (must address before execution)

These are execution blockers that would fail Plan 91-01's plan-boundary verification:

- **NEW-H1 (Plan 91-01, Task 2):** `_oauth_complete_login` references `GlobalAuthState.set_auth` but `GlobalAuthState` is currently imported only inside `auth_callback_route`'s local scope (at `web/main.py:1427`). Factoring the helper to module scope without lifting the import will produce `NameError` at runtime. **Fix:** Add `from web.auth_state import GlobalAuthState` at module top OR add a helper-local import inside `_oauth_complete_login`. Verify no circular-import problem (`web.main` imports from `web.auth_state` -- already true for `complete_login`'s closure path, so a module-top import should work; if it doesn't, fall back to helper-local).
- **NEW-H2 (Plan 91-01, Task 1):** After Task 1 migrates all `app.storage.user.*` references in `web/auth_state.py`, the `app` name from `from nicegui import app, ui` becomes unused. `ruff.toml` enforces `F401`, so `ruff check` will fail at plan boundary. **Fix:** Change `from nicegui import app, ui` -> `from nicegui import ui` as part of Task 1.
- **NEW-H3 (Plan 91-01, Task 4):** `tests/test_auth_callback_resilience.py` imports `pytest` but never uses it (no `@pytest.mark.*`, no `pytest.raises`, no `pytest.fixture`). Plan 91-01's Task 5 ruff check covers this file, so F401 will fail. **Fix:** Drop the `import pytest` line. `monkeypatch` is injected by pytest as a fixture without requiring the import.
- **NEW-H4 (Plan 91-01, Task 4 / T-D):** T-D as currently written starts with empty storage and verifies post-failure storage is empty. That assertion would also pass for a buggy implementation that only pops `auth_user` (since `auth_profile` was never written in the first place). The test does NOT prove SYMMETRIC rollback in the presence of a stale prior `auth_profile`. **Fix:** Pre-seed storage with `{'auth_profile': {'role': 'admin', 'username': 'old_admin'}}` before invoking `set_auth(new_user, new_profile)` where the profile-write fails; then assert `'auth_profile' not in storage` AND `GlobalAuthState.get_role() is None` (or default, not 'admin'). This is the exact stale-role leak Revision MUST-2 was supposed to close, and the current test does not verify it.
- **NEW-H5 (Plan 91-01, Task 4 / T-E):** T-E similarly starts with empty storage. A buggy outer rollback that only pops `auth_session` would still pass the test. **Fix:** Pre-seed storage with `{'auth_user': {'id': 'old_u'}, 'auth_profile': {'role': 'admin'}}` from a "prior session" before the OAuth callback fires the session-then-set_auth-fails path; then assert all 3 keys absent post-failure (proving the DEFENSIVE outer rollback actually pops auth_user + auth_profile, not just auth_session).

### New MEDIUM-severity findings

- **NEW-M1 (Plan 91-01 wording):** The plan repeatedly describes `set_auth`'s rollback as "3-key" or "SYMMETRIC 3-key" -- but `set_auth` only writes 2 keys (USER_KEY + PROFILE_KEY). The 3-key cleanup is the OUTER defensive rollback in `do_login` / `_oauth_complete_login`. Misleading wording could push a future contributor to add an `auth_session` pop inside `set_auth`, which would be incorrect (set_auth doesn't own auth_session). **Fix:** Reword consistently as "set_auth: SYMMETRIC user/profile rollback; do_login / _oauth_complete_login: DEFENSIVE 3-key cleanup."
- **NEW-M2 (Plan 91-01 verification step):** Several verification grep commands in Task 5 use Unix-style `grep | tail -40` pipelines. The project shell is PowerShell. **Fix:** Convert verification commands to `rg` (ripgrep -- already standard in the repo) or Python one-liners for cross-shell reliability.
- **NEW-M3 (Plan 91-02 frontmatter vs. body mismatch):** Plan 91-02's frontmatter `files_modified` lists ONLY `tests/test_persist_value_uses_safe_storage.py`, but Task 2 in the body modifies `.planning/STATE.md`, `.planning/ROADMAP.md`, `CLAUDE.md`, and conditionally `docs/OPEN_ISSUES.md`. This violates the "single-test-file atomic commit" claim. **Fix options:** (a) Add the docs to `files_modified` and reframe Plan 91-02 as "AUTHW-06 retention guard + Phase 91 closeout docs"; OR (b) Split Task 2 into a separate small closeout plan (91-03) keeping 91-02 as a strict single-test-file commit. Option (a) is simpler; option (b) preserves the atomic-CI-guard discipline more strictly.

### New LOW-severity findings

- **NEW-L1:** Direct `posthog_capture('login_failed', {'reason': '...'})` in `_oauth_complete_login` PLUS `show_error_fn` invocation (which itself emits `posthog_capture('login_failed', {'reason': <user_message>})`) produces TWO `login_failed` events per partial-write failure. The plan's `<audit_show_error>` block documents this as acceptable, but PostHog metric consumers will see inflated `login_failed` counts unless they filter on the rich reason tags. Consider routing the rich reason through `show_error_fn` (as an extra argument) so only one event fires.
- **NEW-L2:** `do_login`'s storage-failure path emits `posthog_capture('login_failed', {'reason': 'session_storage_unavailable'})` WITHOUT a `method` tag, but `_oauth_complete_login` adds `'method': 'google_oauth'`. For consistency and easier dashboard slicing, add `'method': 'password'` to `do_login`'s storage-failure posthog calls.

### Agreed Strengths (vs. round-1)

The following round-1 strengths are confirmed by round-2:

- Composite-key consolidation correctly rejected; 3-key model preserved.
- Phase 90 revoke-before-pop behavior preserved during migration.
- Multi-write boundary explicitly handles `safe_user_set(False)` return values.
- Factoring `_oauth_complete_login` is a good testability seam.
- Empty-allowlist assertion fix (Codex F3) correctly identified pre-execution.
- `asyncio.run()` choice (Revision MUST-1) avoids new pytest-asyncio dependency.
- AST + behavioral test combo (Plan 91-02 Revisions MUST-5 + SHOULD-6) closes the shape-only loophole.

### Divergent Views

This was a single-reviewer round; no inter-reviewer divergence to report. Round-1's Gemini + Codex divergence is captured in the historical 91-REVIEWS.md commit (`e5447a93`).

### Recommended Action

**Revise before execution.** Apply the 5 NEW-H fixes (NEW-H1..H5) to Plan 91-01 and pick one of NEW-M3's options for Plan 91-02. NEW-M1, NEW-M2 are wording / cross-shell hygiene; NEW-L1, NEW-L2 are telemetry polish. After revisions, run `/gsd-plan-phase 91 --reviews` to integrate this round-2 feedback into the plans, then proceed to `/gsd-execute-phase 91`.

### Pointer to round-1

The round-1 cross-AI REVIEWS.md (Gemini + Codex) is preserved in git at commit `e5447a93` and was integrated as Revisions MUST-1, MUST-2, MUST-3, MUST-4, MUST-5, SHOULD-6 in the current plans. This round-2 audit confirms those round-1 integrations are correctly encoded **except for the 2 test-coverage gaps NEW-H4 / NEW-H5** (T-D and T-E need pre-seeded stale-state fixtures to actually verify the symmetric/defensive rollback contracts that the round-1 Codex HIGH catch identified).
