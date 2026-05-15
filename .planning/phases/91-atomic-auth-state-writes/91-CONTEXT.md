# Phase 91: Atomic Auth State Writes — Context

**Gathered:** 2026-05-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Migrate the remaining 12 raw `app.storage.user` access sites that survive Phase 90 — the 9 sites in `web/auth_state.py` (GlobalAuthState class methods + `do_login` at line 187) and the 3 OAuth-callback writes in `web/main.py:1441-1449:complete_login` — to `web/safe_storage.py` helpers (`safe_user_get`/`safe_user_set`/`safe_user_pop`), eliminating the last two Phase 87 allowlist entries (2 → 0). Add multi-write error handling to `do_login` and `complete_login` so a `safe_user_set(...)` returning `False` (storage prune mid-flight) surfaces a user-visible error rather than leaving a half-state. Install `tests/test_auth_callback_resilience.py` (AUTHW-05) asserting (T-A) prune-pre-write returns a friendly error label + no `ui.navigate.to('/')` + no `AssertionError` propagation, (T-B) happy-path writes-then-navigates, (T-C) `GlobalAuthState.get_user()` under pruned storage returns `None` without propagating `AssertionError`. Install `tests/test_persist_value_uses_safe_storage.py` (AUTHW-06) as the AST-based retention guard for the `web/components/filter_panel.py:220:persist_value` safe-wrap (originally landed in commit `cca23db3`).

**Architectural decision (Codex round-1 F1 + Decision verdict):** **Keep the 3 separate storage keys (`auth_user`, `auth_profile`, `auth_session`) — do NOT consolidate into a composite `_auth_block`.** Composite-key consolidation was the original proposal; Codex caught that under NiceGUI's `app.storage.user` semantics (no compare-and-swap primitive), composite-key turns the high-frequency `_refresh_user_session()` write at `web/supabase_client.py:191` into a read-modify-write of the entire auth block. That RMW creates new race surface that does not exist today: (a) concurrent refresh + logout — request A reads block, request B `safe_user_pop('_auth_block')`, A writes stale block resurrecting logged-out user/profile; (b) in-place mutation `block['profile'] = ...; safe_user_set('_auth_block', block)` may mutate the dict object already stored before the set runs, racing any concurrent reader. **Phase 91 keeps the 3 keys; "atomic" here means per-key atomicity under prune-race AssertionError (which `safe_user_set` already provides) plus multi-write error-handling discipline (return-value checks + rollback on partial-write failure) — NOT cross-key transactional atomicity, which NiceGUI storage cannot deliver.**

**Scope (after Phase 90 pulled AUTHW-03 + AUTHW-04 forward):**
- **AUTHW-01:** `web/auth_state.py:set_auth/clear_auth/do_login/update_profile_cache/get_user/get_profile` writes migrated to safe_storage helpers.
- **AUTHW-02:** OAuth callback in `web/main.py:1441-1449:complete_login` migrated to safe_storage helpers.
- **AUTHW-05:** `tests/test_auth_callback_resilience.py` per D-08 (T-A/T-B/T-C below).
- **AUTHW-06:** `tests/test_persist_value_uses_safe_storage.py` AST retention guard per D-09.

**Out of scope:**
- **Composite-key consolidation** — rejected per Codex F1 (see above). Multi-key atomicity in NiceGUI storage requires copy-on-write + a per-session block lock + a central accessor layer; Phase 91 cost-benefit does not justify that complexity given the surviving threats. Captured in `<deferred>` for future consideration.
- **Migration helper for legacy keys** — not needed since we keep the 3 keys. Phase 90 already migrated `web/supabase_client.py` readers to `safe_user_get('auth_session')`; Phase 91 only flips the writers in `auth_state.py` + `main.py`, which match the existing reader contract.
- **Server-side revocation behavior** — already shipped in Phase 90 (D-11, D-11b). `sign_out(access_token)` uses `throwaway.auth.admin.sign_out(jwt, "global")`; `clear_auth` revokes before pop with `finally:` cleanup.
- **`supabase_client.py` write-side changes** — Phase 90 already updated all 6 reader sites (lines 159, 175, 279, 309, 417, 459) and 1 writer (line 191) to `safe_user_*`. Phase 91 makes no `supabase_client.py` changes.
- **Cross-user concurrent smoke test, `docs/guides/MULTITENANT.md`** — Phase 92.
- **`get_oauth_url` / `get_profile` callers** — already migrated in Phases 87/90.

</domain>

<decisions>
## Implementation Decisions

### Area 1: Migration Pattern — Direct Raw→Safe Substitution (Codex Decision)

- **D-01:** Substitute all 9 raw access sites in `web/auth_state.py` with their `safe_storage` equivalents in-place, preserving the existing storage keys (`auth_user`, `auth_profile`, `auth_session`):

  | Line | Before | After |
  |------|--------|-------|
  | 42 | `return app.storage.user.get(cls.USER_KEY)` | `return safe_user_get(cls.USER_KEY)` |
  | 50 | `return app.storage.user.get(cls.PROFILE_KEY)` | `return safe_user_get(cls.PROFILE_KEY)` |
  | 95 | `app.storage.user[cls.USER_KEY] = user` | `safe_user_set(cls.USER_KEY, user)` (checked, see D-04) |
  | 97 | `app.storage.user[cls.PROFILE_KEY] = profile` | `safe_user_set(cls.PROFILE_KEY, profile)` (checked, see D-04) |
  | 117 | `app.storage.user[cls.PROFILE_KEY] = profile` | `safe_user_set(cls.PROFILE_KEY, profile)` (best-effort) |
  | 138 | `app.storage.user.pop(cls.USER_KEY, None)` | `safe_user_pop(cls.USER_KEY, None)` |
  | 139 | `app.storage.user.pop(cls.PROFILE_KEY, None)` | `safe_user_pop(cls.PROFILE_KEY, None)` |
  | 140 | `app.storage.user.pop('auth_session', None)` | `safe_user_pop('auth_session', None)` |
  | 187 | `app.storage.user['auth_session'] = {...}` | `safe_user_set('auth_session', {...})` (checked, see D-05) |

  The `try/except Exception: return None` wrappers at lines 41-44 and 49-52 are **removed** in the new shape — `safe_user_get` already catches both `AssertionError` (debug-logged, Phase 87 contract) and `Exception` (warning-logged); the manual wrapper is now redundant and would suppress the warning logging that `safe_user_get` provides for unexpected failures (a slight observability regression). Codex M3-adjacent: removal aligns the file with the chokepoint discipline.

- **D-02:** Substitute all 3 raw access sites in `web/main.py:1441-1449:complete_login`:

  | Line | Before | After |
  |------|--------|-------|
  | 1441 | `app.storage.user[GlobalAuthState.USER_KEY] = user` | `safe_user_set(GlobalAuthState.USER_KEY, user)` (checked) |
  | 1443 | `app.storage.user[GlobalAuthState.PROFILE_KEY] = profile` | `safe_user_set(GlobalAuthState.PROFILE_KEY, profile)` (checked) |
  | 1446 | `app.storage.user['auth_session'] = {...}` | `safe_user_set('auth_session', {...})` (checked) |

  All three writes wrapped in return-value checks per D-04 (rollback discipline).

- **D-03 (key-constant audit):** `GlobalAuthState.USER_KEY = 'auth_user'` and `PROFILE_KEY = 'auth_profile'` class constants stay. They are referenced by `web/supabase_client.py:220-222` (Phase 90 terminal-refresh-cleanup) via string literals `'auth_user'`/`'auth_profile'`, and by tests at `tests/test_session_uuid.py:62`, `tests/test_auth_revocation_and_headers.py:65,69,164`, `tests/test_refresh_lock_per_session.py:114-122,182,289` via literal `'auth_session'`. Keeping the literal key contract intact means **zero changes** to `supabase_client.py` or test files for Phase 91 — the migration is strictly local to `auth_state.py` + `main.py`. (Composite-key migration would have required ~12 reader updates and 7 test rewrites; one of the reasons Codex's verdict is correct.)

### Area 2: Multi-Write Error Handling (Codex M2 + extended)

- **D-04 (Codex M2 — `set_auth` and `complete_login` return-value check + rollback):** `safe_user_set` returns `False` when the underlying storage write raises `AssertionError` (prune race) or other exception. For multi-write blocks (`GlobalAuthState.set_auth`, `complete_login`, `do_login`), checking the return value of each write and rolling back on failure prevents the half-state Codex M2 surfaces (e.g., password login closes the dialog, reports success, and reloads into a logged-out state because the storage write failed silently).

  **`set_auth` shape after Phase 91:**
  ```python
  @classmethod
  def set_auth(cls, user: Dict, profile: Dict = None) -> bool:
      """Set authentication after successful login. Returns False if any
      write fails (prune race) — caller MUST handle by surfacing an error
      and rolling back any pre-existing partial state.

      Phase 91 D-04: multi-write atomicity is best-effort under NiceGUI
      storage semantics (no compare-and-swap). On partial-write failure
      we roll back any successful writes so callers never observe a
      half-state.
      """
      if not safe_user_set(cls.USER_KEY, user):
          return False
      if profile is not None:
          if not safe_user_set(cls.PROFILE_KEY, profile):
              # Rollback the user write to avoid half-state
              safe_user_pop(cls.USER_KEY, None)
              return False
      cls._posthog_identify(user, profile)
      return True
  ```

  Callers of `set_auth` (currently `do_login` line 202, OAuth-callback indirectly through `complete_login`) must check the return value. `update_profile_cache` (line 117 → 117') stays **best-effort, no rollback** because it's an update-on-existing-state path; a failed write leaves the prior profile in storage unchanged, which is the correct half-state for a profile-only update.

- **D-05 (`do_login` multi-write check):** `do_login` writes `auth_session` at line 187 BEFORE calling `set_auth(user, profile)` at line 202. New ordering and error-handling shape:

  ```python
  async def do_login(email: str, password: str) -> Dict:
      from nicegui import run
      result = await run.io_bound(supabase_sign_in, email, password)
      if "error" in result:
          # ... unchanged posthog + return ...
      session = result.get('session', {})
      user = result.get('user')
      if not user:
          posthog_capture('login_failed', {'reason': 'No user returned'})
          return {"error": "No user returned"}
      profile = get_profile(user['id'])

      # D-05: write auth_session first (smallest-blast-radius write);
      # only persist user/profile if the session write succeeds. On
      # session-write failure, return an explicit error so the caller
      # doesn't reload into a half-logged-in state (Codex M2).
      if session:
          if not safe_user_set('auth_session', {
              'access_token': session.get('access_token'),
              'refresh_token': session.get('refresh_token'),
          }):
              posthog_capture('login_failed', {'reason': 'session_storage_unavailable'})
              return {"error": "Session storage unavailable. Please try again."}
      if not GlobalAuthState.set_auth(user, profile):
          # set_auth handled its own rollback; we also pop the session
          # write to keep the rollback complete.
          safe_user_pop('auth_session', None)
          posthog_capture('login_failed', {'reason': 'auth_state_storage_unavailable'})
          return {"error": "Session storage unavailable. Please try again."}
      posthog_capture('login_success', {})
      return {"success": True, "user": user, "profile": profile}
  ```

- **D-06 (OAuth callback `complete_login` error-handling per AUTHW-05 + Codex M2):** Same rollback discipline as `do_login`, but the failure mode goes through the OAuth-callback UI (`show_error` label + home button), not a return-dict — matching the existing OAuth UX:

  ```python
  async def complete_login(user, profile, session=None):
      """Store user in session and redirect.

      Phase 91 AUTHW-02 + D-06: every write is checked. On partial-write
      failure, roll back any successful writes and surface an error label
      so the user doesn't reload into a half-logged-in state. The OAuth
      code has already been consumed by exchange_code_for_session so there
      is no auto-retry path; the user must restart the OAuth flow.
      """
      # Session token write FIRST (no user-visible side-effect yet)
      if session:
          if not safe_user_set('auth_session', {
              'access_token': session.get('access_token'),
              'refresh_token': session.get('refresh_token'),
          }):
              posthog_capture('login_failed', {
                  'reason': 'session_storage_unavailable',
                  'method': 'google_oauth',
              })
              show_error('Session storage unavailable. Please try again.')
              return
      # User + profile next (use set_auth's built-in rollback)
      if not GlobalAuthState.set_auth(user, profile):
          safe_user_pop('auth_session', None)
          posthog_capture('login_failed', {
              'reason': 'auth_state_storage_unavailable',
              'method': 'google_oauth',
          })
          show_error('Session storage unavailable. Please try again.')
          return
      posthog_capture('login_success', {'method': 'google_oauth'})
      status_label.text = 'Login successful! Redirecting...'
      await asyncio.sleep(0.5)
      ui.navigate.to('/')
  ```

  **OAuth UX decision (Codex Decision-verdict):** Show error + Return to Home button (existing UI elements). Do NOT auto-retry — the OAuth code is one-time-use, already consumed by `exchange_code_for_session`; a retry would require a fresh provider redirect. Do NOT silently redirect home — that hides a successful provider login that failed only at local persistence.

### Area 3: Phase 87 Allowlist & Lint Scanner (Codex F3)

- **D-07 (Codex F3 — `test_allowlist_well_formed` empty-allowlist fix):** The Phase 87 lint scanner at `tests/test_no_raw_storage_access.py:200` currently hard-asserts `assert entries, "Allowlist is empty — at minimum web/auth_state.py should be allowlisted"`. Plan 91-01 MUST update this assertion before deleting the last two allowlist entries, otherwise the plan-boundary pytest will fail.

  **Fix:** replace line 200 with a comment explaining the post-Phase-91 invariant:
  ```python
  # Post-Phase-91 (AUTHW-01 + AUTHW-02): allowlist may be empty —
  # `web/auth_state.py` and `web/main.py` raw accesses are now migrated
  # to safe_storage helpers. An empty list still passes the lint
  # invariant (every raw access in `web/` outside the empty allowlist is
  # rejected) and is the goal state for v7.12 Path B.
  # If a new raw access is ever justified, re-add it with explicit
  # justification + expected_count. The validators below still apply
  # to every present entry.
  ```
  The `for entry in entries:` loop below stays — it validates schema for any future re-additions but is a no-op when `entries == []`. The other 5 tests in the file (`test_lint_rejects_synthetic_violation`, `test_lint_handles_aliased_imports`, `test_lint_does_not_double_report_nested_nodes`, `test_allowlist_counts_exact`, `test_no_raw_storage_access_outside_allowlist`) are unaffected by an empty allowlist — they iterate over allowlist entries (no-op for empty) or scan production code (still asserts zero raw accesses outside the empty allowlist).

- **D-07b (allowlist YAML deletion):** Delete BOTH file-entry blocks from `.planning/phase87_storage_allowlist.yaml`:
  - Lines 21-69 (`web/auth_state.py` block: 8 patterns / 9 expected_count nodes)
  - Lines 71-93 (`web/main.py` OAuth-callback block: 3 patterns / 3 expected_count nodes)

  Final YAML state: `allowed_raw_access: []` (empty list, header comments preserved). The 2 → 0 transition matches the milestone end-state (Phase 87 allowlist count: started at 4 after Plan 87-01, dropped to 3 by Phase 88's `web/export_state.py` self-elimination, to 2 by Phase 90's `web/supabase_client.py` self-elimination, to 0 by Phase 91).

### Area 4: AUTHW-05 Test Shape (Codex M3 — refined T-C)

- **D-08:** `tests/test_auth_callback_resilience.py` — Phase 87 monkeypatch pattern (`monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(storage=SimpleNamespace(user=...)))`), instance-isolated per test (Phase 88 D-01 + D-02 + Refinement 6 pattern):

  - **T-A (prune-pre-write → friendly error, no AssertionError propagation, no navigate):**
    - Setup: monkeypatch `web.safe_storage.app` so `storage.user[key] = value` raises `AssertionError("user storage not created")`. `storage.user.get(key, default)` similarly raises.
    - Call: `await complete_login({'id': 'u1', 'email': 'a@b.c'}, {'username': 'u1'}, session={'access_token': 'at', 'refresh_token': 'rt'})`.
    - Assert: `safe_user_set` returned False for the first write → `show_error` was called with "Session storage unavailable. Please try again." → `ui.navigate.to('/')` was NOT called → no `AssertionError` propagated to the test runner → posthog `login_failed` captured with reason `session_storage_unavailable`.
    - Cleanup verification: no keys present in the test's monkeypatched storage dict (no half-state).

  - **T-B (happy-path: writes succeed, navigate called, all 3 keys persisted):**
    - Setup: monkeypatch storage to a plain `{}` dict-backed `SimpleNamespace`.
    - Call: same as T-A.
    - Assert: all 3 keys present in the monkeypatched storage dict — `'auth_user'` = user, `'auth_profile'` = profile, `'auth_session'` = {'access_token': 'at', 'refresh_token': 'rt'} → `ui.navigate.to('/')` called once → posthog `login_success` with method `google_oauth`.

  - **T-C (Codex M3 refined — `get_user()` under pruned storage returns None, no AssertionError):**
    - Setup: monkeypatch storage with a stub that raises `AssertionError` on ALL access (read AND write).
    - Call: `GlobalAuthState.get_user()` directly (not through OAuth flow).
    - Assert: returns `None` → no `AssertionError` propagated. This proves the `safe_user_get` migration at line 42 actually swallows the prune-race exception, which is the entire point of AUTHW-01 for reader sites.

  **Codex M3 rationale for T-C reshape:** the original T-C ("`AssertionError` during the redirect target") relied on `ui.navigate.to('/')` synchronously executing the home-page route handler, which mocks/stubs of NiceGUI's `ui.navigate.to` will NOT do — the redirect is registered, not invoked. T-C as reshaped tests the migration's actual contract (readers don't 500 under prune) without depending on navigation execution semantics.

  **Test isolation pattern:** each test instantiates its own `SimpleNamespace` storage stub via a fixture; no module-level state. Mirrors `tests/test_browse_state.py` / `tests/test_search_state.py` (Phase 87 B3 fix) and `tests/test_refresh_lock_per_session.py` `_ThreadRoutedApp` pattern.

### Area 5: AUTHW-06 Retention Guard (Codex M5 — AST justified)

- **D-09:** `tests/test_persist_value_uses_safe_storage.py` — AST-based retention guard for `web/components/filter_panel.py:220:persist_value`. Codex M5: grep is enough for *raw* `app.storage.user` lint, but `persist_value`'s contract is **function-local** — the function body must (a) call `safe_user_get('session_persistence_enabled', True)` to gate persistence, AND (b) write via `safe_user_set(key, value)`. AST walking gives function-body precision without false positives from comments/strings.

  **Test shape (3 assertions):**

  ```python
  def test_persist_value_imports_safe_storage_helpers():
      """AUTHW-06: persist_value must import safe_user_get + safe_user_set."""
      tree = ast.parse(Path('web/components/filter_panel.py').read_text())
      fn = _find_function_def(tree, 'persist_value')
      assert fn is not None, "persist_value() function disappeared from filter_panel.py"
      # Walk function body for ImportFrom from web.safe_storage
      imports = [n for n in ast.walk(fn) if isinstance(n, ast.ImportFrom)
                 and n.module == 'web.safe_storage']
      assert imports, "persist_value() must import from web.safe_storage"
      imported_names = {alias.name for imp in imports for alias in imp.names}
      assert 'safe_user_get' in imported_names
      assert 'safe_user_set' in imported_names

  def test_persist_value_reads_persistence_flag():
      """AUTHW-06: persist_value must gate on session_persistence_enabled."""
      tree = ast.parse(Path('web/components/filter_panel.py').read_text())
      fn = _find_function_def(tree, 'persist_value')
      # Find Call to safe_user_get with first arg literal 'session_persistence_enabled'
      calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_get'
               and n.args and isinstance(n.args[0], ast.Constant)
               and n.args[0].value == 'session_persistence_enabled']
      assert calls, ("persist_value() must read 'session_persistence_enabled' "
                     "via safe_user_get to gate persistence (AUTHW-06 retention)")

  def test_persist_value_writes_via_safe_user_set():
      """AUTHW-06: persist_value must NOT use raw app.storage.user[...] = ..."""
      tree = ast.parse(Path('web/components/filter_panel.py').read_text())
      fn = _find_function_def(tree, 'persist_value')
      raw_writes = [n for n in ast.walk(fn) if isinstance(n, ast.Assign)
                    and any(_is_app_storage_user_subscript(t) for t in n.targets)]
      assert not raw_writes, ("persist_value() reintroduced raw app.storage.user[...] = ..."
                              "; must use safe_user_set (AUTHW-06 retention)")
      safe_set_calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set']
      assert safe_set_calls, "persist_value() must call safe_user_set to write"
  ```

  **Seed traps (2 minimum):**
  1. **Passing snippet** — `def persist_value(k, v): from web.safe_storage import safe_user_get, safe_user_set; \n    if safe_user_get('session_persistence_enabled', True): safe_user_set(k, v)` → asserts all 3 tests PASS against the snippet.
  2. **Failing snippet** — `def persist_value(k, v): from nicegui import app; app.storage.user[k] = v` → asserts at least one test FAILS against the snippet.

  Seed traps live as in-test docstring snippets parsed via `ast.parse(...)`, mirroring Phase 88 D-07 / Phase 90 D-15 pattern.

### Area 6: Plan Decomposition

- **D-10:** **2 plans** (matches Phase 89 + Phase 90 split discipline):

  - **Plan 91-01 — Direct migration + multi-write error handling + AUTHW-05 test + allowlist self-elimination + Phase 87 lint fix.**
    - Migrate the 9 raw access sites in `web/auth_state.py` per D-01.
    - Migrate the 3 raw access sites in `web/main.py:complete_login` per D-02.
    - Add return-value checks + rollback per D-04 (`set_auth`), D-05 (`do_login`), D-06 (`complete_login`).
    - Update `tests/test_no_raw_storage_access.py:200` per D-07 to allow empty allowlist.
    - Delete both file-entry blocks from `.planning/phase87_storage_allowlist.yaml` per D-07b (2 → 0).
    - Add `tests/test_auth_callback_resilience.py` per D-08 (T-A/T-B/T-C).
    - Plan-boundary: full pytest green. Phase 87 lint scanner still GREEN (it now scans against empty allowlist and asserts zero raw accesses in `web/` — both invariants hold after migration). User-visible behavior change: prune-race during login now shows an error instead of silently leaving a half-logged-in state — security/correctness improvement.

  - **Plan 91-02 — AUTHW-06 retention guard install (atomic-commit pattern).**
    - Add `tests/test_persist_value_uses_safe_storage.py` per D-09 (3 tests + 2 seed traps).
    - Plan-boundary: full pytest green. No production-code changes. Zero user-visible behavior change.

  **Why 2 plans instead of 1?** The retention guard (Plan 91-02) is conceptually separate from the migration (Plan 91-01) and lives in its own commit per the same atomic-commit-as-deletion discipline Phase 89 D-09 and Phase 90 D-13 established. Plan 91-02 is a single-test-file commit with no production code — ideal for an atomic CI-guard commit. Splitting them keeps `git log --pretty=oneline` readable and makes rollback granular (if the retention test produces a false positive in a future refactor, we can revert just 91-02 without touching the migration).

  **Why NOT 3 plans (e.g., auth_state.py first, main.py second, tests + guards third)?** Plan-boundary pytest green discipline (Phase 88 D-05) would force Plan 91-01a (auth_state.py only) to either keep `web/main.py` allowlisted OR also delete the `main.py` block — but if 91-01a keeps `main.py` allowlisted and the migration plan boundary leaves only the OAuth-callback raw accesses, the lint scanner still passes. Either way, splitting the migration into 2 plans adds churn without reducing blast radius. The 2-plan split (migration + retention) is the right granularity.

### Area 7: Codex External Review Pattern

- **D-11:** Same Claude→Codex→user pattern as Phases 88/89/90. Round-1 dispatched on 2026-05-15 (`_tmp/codex_phase91_discuss_review_prompt.md` + `_tmp/codex_phase91_discuss_review_response.txt`). Codex returned **3 BLOCKING findings + 5 MEDIUM**:
  - **F1 (BLOCKING):** Composite-key consolidation creates new RMW race surface. Rejected; keep 3 keys. **Encoded as: the entire architectural pivot in `<domain>` "Architectural decision".**
  - **F2 (BLOCKING):** Migration helper in `auth_state.py` would be bypassed by `supabase_client.py` readers. **Mooted by F1** — no migration helper needed when we keep 3 keys.
  - **F3 (BLOCKING):** Phase 87 lint scanner hard-fails on empty allowlist at line 200. **Encoded as D-07.**
  - **M1 (MEDIUM):** Migration helper ordering issues. **Mooted by F1.**
  - **M2 (MEDIUM):** `do_login` must check `safe_user_set` return value. **Encoded as D-05.**
  - **M3 (MEDIUM):** T-C is not well-shaped — navigate is mocked. **Encoded as D-08 T-C reshape.**
  - **M4 (MEDIUM):** Rewriting existing tests for composite-key risks weakening Phase 90 refresh-lock coverage. **Mooted by F1** — no test rewrites needed.
  - **M5 (MEDIUM):** AUTHW-06 AST guard is justified over grep. **Encoded as D-09.**

  No round-2 or plan-checker round dispatched yet — the pivot to "keep 3 keys" simplified the surface enough that the residual surface is mechanical (1:1 substitution + return-value checks + 1 test file + 1 retention test). If the plan-checker round (post-plan, pre-execution) surfaces residual issues, address them then per Phase 90 plan-checker pattern.

### Claude's Discretion

- Whether `set_auth` should return `bool` (D-04 proposed shape) or remain `void` and raise on partial-write failure. Recommend `bool` per D-04 — callers (`do_login`, `complete_login`) already need to branch on success/failure. Raising would force every caller (and any future caller in Phase 92 cleanup) to wrap in try/except.
- Whether `do_login`'s `auth_session`-first ordering (D-05) versus `set_auth`-first ordering matters for the error path. Recommend `auth_session`-first because it's the simplest single-key write and least likely to leave residue under failure (one key to pop on rollback instead of two). Code reviewer may flip if the cognitive load of a different write order matters more.
- The exact wording of the user-facing error message ("Session storage unavailable. Please try again."). Match existing OAuth-callback error message style; planner may adjust phrasing.
- Whether `update_profile_cache` (line 117) needs the same return-value check as `set_auth`. Recommend `False` — it's an existing-state update path, not a login boundary; a failed write leaves the prior profile in storage, which is correct. Best-effort write per D-04 note.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 91 Locked Requirements
- `.planning/REQUIREMENTS.md` §"Auth State Writes — Phase 91" — AUTHW-01 through AUTHW-06. Note: AUTHW-03 + AUTHW-04 are "subsumed by Phase 90" per Phase 90 D-11 + D-11b — Phase 91's effective surface is AUTHW-01, AUTHW-02, AUTHW-05, AUTHW-06.
- `.planning/ROADMAP.md` §"Phase 91: Atomic Auth State Writes" — 5 success criteria. SC #2 (sign_out revoke-before-pop) is already satisfied by Phase 90 D-11b code path.

### Phase 87 Foundations (load-bearing for Phase 91)
- `web/safe_storage.py` — Phase 87 chokepoint. `safe_user_get` (line 46), `safe_user_set` (line 63), `safe_user_pop` (line 76) are the migration targets. `ensure_session_uuid` (line 184) is already wired at `web/main.py:1425` for the OAuth callback route.
- `.planning/phase87_storage_allowlist.yaml` — Plan 91-01 deletes both file-entry blocks. **Allowlist count: currently 2 file entries (`web/auth_state.py`, `web/main.py`); Phase 91 takes it 2 → 0** (verified by `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml`). Milestone progression: started 4 (Phase 87) → 3 (Phase 88) → 2 (Phase 90) → 0 (Phase 91).
- `tests/test_no_raw_storage_access.py:200` — Plan 91-01 must update the `assert entries` assertion per D-07 to allow empty allowlist (Codex F3). The other 5 tests in this file are unaffected by the change.

### Phase 88 / 89 / 90 Patterns (templates Phase 91 mirrors)
- `.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md` D-04, D-05, D-07 — plan-boundary green discipline, ordering rationale (writes-then-deletes, not deletes-then-writes), AST scanner shape with parsed seed-trap snippets.
- `.planning/phases/89-lists-cache-per-request/89-CONTEXT.md` D-08, D-09, D-10, D-11 — 2-plan split, same-commit-as-deletion discipline, runtime attr-absence template (not used in Phase 91 — no globals to delete).
- `.planning/phases/90-auth-caching-rewrite-no-set-session/90-CONTEXT.md` D-11, D-11b, D-13, D-15, D-16, D-17 — `clear_auth` reorder (already shipped, Phase 91 inherits), 2-plan boundary discipline, AST scanner pattern (D-15 — Phase 91 D-09 mirrors).
- `tests/test_browse_state.py`, `tests/test_search_state.py` (Phase 87 B3 fix) — `monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(...))` pattern. **D-08 directly mirrors.**
- `tests/test_refresh_lock_per_session.py` (Phase 90 D-17) — instance-isolated storage stubs via SimpleNamespace + per-thread routing. **D-08 reuses the stub-construction shape (no threading needed for AUTHW-05).**

### Source files modified by Phase 91

Plan 91-01:
- `web/auth_state.py` — primary surgery: 9 raw access sites → safe_user_* helpers per D-01; `set_auth` return-value type changed to `bool` with rollback per D-04; `do_login` ordering + error handling per D-05; existing `try/except Exception: return None` wrappers at lines 41-44 and 49-52 removed (redundant with `safe_user_get` contract).
- `web/main.py:1441-1449:complete_login` — 3 raw access sites → `safe_user_set` calls with rollback per D-06.
- `tests/test_no_raw_storage_access.py:200` — empty-allowlist assertion fix per D-07.
- `.planning/phase87_storage_allowlist.yaml` — delete lines 21-69 (`web/auth_state.py` block) and lines 71-93 (`web/main.py` block) per D-07b. Header comments + schema docs preserved.

Plan 91-01 — test files created:
- `tests/test_auth_callback_resilience.py` — T-A / T-B / T-C per D-08.

Plan 91-02 — test files created:
- `tests/test_persist_value_uses_safe_storage.py` — 3 AST assertions + 2 seed-trap snippets per D-09.

### Existing tests touching Phase 91 surface (planner-audit gate)
- `tests/test_auth_revocation_and_headers.py` — seeds `'auth_session'` literal key at lines 65, 69, 164. **No changes needed** since Phase 91 keeps the 3-key model.
- `tests/test_refresh_lock_per_session.py` — seeds `'auth_session'` at lines 114-122, 182, 289. **No changes needed.**
- `tests/test_session_uuid.py` — references `'auth_session'` at line 62. **No changes needed.**

### External red-team review (Codex round 1)
- `_tmp/codex_phase91_discuss_review_prompt.md` — Claude's round-1 proposal sent to Codex. Originally proposed composite-key consolidation.
- `_tmp/codex_phase91_discuss_review_response.txt` — Codex round-1 verdicts. 3 BLOCKING + 5 MEDIUM. Decision-verdict: "I would not approve composite-key migration as written. There is a strong argument to keep the three legacy keys and just migrate raw access to `safe_user_*`."

### Upstream contract references (Phase 90 already verified)
- `supabase_auth/_sync/gotrue_client.py:713` — `set_session()` networked behavior. Cited in `web/supabase_client.py:get_user_client()` AUTHC-05 docstring. Phase 91 does not modify this contract.
- `web/safe_storage.py:46-86` — `safe_user_get/set/pop` AssertionError-swallow contract. Phase 91's multi-write error handling relies on `safe_user_set` returning `False` rather than raising.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/safe_storage.py:safe_user_get/set/pop` — the exact substitutions for the 12 raw access sites. Already battle-tested across 131 sites migrated in Phase 87 + 1 site in Phase 90.
- `web/safe_storage.py:ensure_session_uuid()` — already wired at `web/main.py:1425` before the OAuth callback writes. The session UUID is guaranteed present when `complete_login` runs.
- `tests/test_browse_state.py` / `tests/test_search_state.py` — monkeypatch pattern for `web.safe_storage.app`. `tests/test_auth_callback_resilience.py` mirrors verbatim.
- Phase 90 D-11b ordering — `clear_auth` already calls `safe_user_get('auth_session')` to read the access token before pops; Phase 91 only changes the pops themselves (3 raw → 3 `safe_user_pop`). The revoke-before-pop / `finally:` discipline is preserved.

### Established Patterns
- **Per-key atomicity, not multi-key transactional atomicity** (Phase 91 NEW pattern): NiceGUI's `app.storage.user` is per-key write — no compare-and-swap. The "atomic" goal of Phase 91 is per-write AssertionError-safety (`safe_user_set` returns `False` on prune-race) + explicit multi-write error handling at the boundary (return-value check + rollback). Cross-key transactional atomicity is impossible without a CAS primitive and is explicitly not the goal.
- **Allowlist self-elimination** (Phase 88: export_state, Phase 89: lists, Phase 90: supabase_client, Phase 91: auth_state + main): each phase migrates its own allowlist entries to safe_storage and deletes the YAML block in the same commit. By Phase 92, the allowlist is empty + the lint scanner enforces zero raw accesses in `web/`.
- **Codex-pivoting-Claude pattern** (Phase 88/89/90, repeated here): Claude's first instinct was composite-key consolidation; Codex caught that it introduces new race surface via the refresh-RMW path. The user has consistently delegated technical synthesis to Codex review. This is the highest-leverage feedback loop in the v7.12 milestone.

### Integration Points
- **`web/supabase_client.py:159, 175, 191, 220-222, 279, 309, 417, 459`** — unchanged. Phase 90 already migrated these to `safe_user_get/set/pop` with `'auth_session'` literal key. The 3-key contract that Phase 91 preserves means Phase 91 is a strict no-op on `supabase_client.py`.
- **`web/auth_state.py:GlobalAuthState` 11 helper methods** (`get_user_id`, `get_role`, `is_admin`, `is_editor`, `can_edit`, `can_comment`, `get_username`, `_posthog_identify`, etc.) — all read via `get_user()` / `get_profile()`, which Phase 91 migrates. Helper methods themselves are unchanged.
- **`web/main.py:auth_callback_route`** — `complete_login` is the only writer inside the callback body. The outer try/except at lines 1494-1500 catches general exceptions and shows the error label; D-06's `show_error` calls work inside this try/except boundary — if `safe_user_set` fails AND `show_error` itself raises (unlikely under normal storage failure), the outer try/except is the safety net.
- **`web/components/filter_panel.py:persist_value`** (line 220) — already correct. Phase 91 only INSTALLS the retention test that fails if a future refactor un-does the safe-wrap.

### Why Codex's F1 Catch Matters (High-Value Insight)
Original Claude framing assumed that consolidating into a `_auth_block` composite key would deliver true atomicity. Codex traced the threat model:

1. The refresh path at `web/supabase_client.py:191` is currently a SINGLE-KEY write — `safe_user_set('auth_session', {...})`. Under the composite model, it becomes a READ-MODIFY-WRITE of the entire auth block (`block = safe_user_get('_auth_block'); block['session'] = {...}; safe_user_set('_auth_block', block)`).

2. Two concrete races emerge:
   - **Logout-resurrect:** Request A (refresh) reads block, calls Supabase refresh; Request B (logout via `clear_auth`) pops `_auth_block`; Request A returns and writes its stale block back, resurrecting the logged-out user/profile/session.
   - **Profile clobber:** Request A (refresh, slow Supabase call) reads block; Request B (`update_profile_cache`) writes new profile to block; Request A returns, writes its stale block with old profile, silently clobbering B's update.

3. Mitigation requires either copy-on-write + per-session block lock + version-stamping (significant complexity) OR keeping 3 separate keys (current model). Codex's verdict: keeping 3 keys is correct given the cost-benefit.

**Fix (D-01 + D-04 + D-05 + D-06):** keep 3 keys, swap raw → safe_user_*, add return-value checks + rollback at the multi-write boundaries. The race surface Codex F1 surfaces is eliminated by NOT introducing the RMW pattern in the first place.

This is the Phase-90-equivalent high-value Codex catch (event-listener-mutation vs. dict-cache in Phase 90; here, RMW-race vs. per-key-atomicity in Phase 91).

</code_context>

<specifics>
## Specific Ideas

- **User direction (repeated from Phases 88/89/90):** Out of 4 gray areas presented (atomicity model, AUTHW-05 test shape, plan decomposition, Codex red-team), the user selected ONLY "External Codex red-team round". Same pattern as Phases 88/89/90: user explicitly delegates technical synthesis to Codex review. Pattern locked in for the remainder of v7.12.

- **Codex's 3 blocking findings (high-value catches):**
  1. **F1 (composite-key creates new race surface):** Claude's original "single composite key = atomic by construction" claim was correct for single-writer scenarios but wrong once refresh became RMW. Forces the architectural decision to keep 3 keys.
  2. **F2 (migration helper bypassed):** If we HAD gone with composite, the helper in `auth_state.py` would have been invisible to `supabase_client.py` readers. Mooted by F1.
  3. **F3 (Phase 87 lint scanner empty-allowlist assertion):** A literal `assert entries` at line 200 would fail the plan-boundary pytest the moment the last entry is deleted. Forces D-07.

- **Codex's Decision verdict (high-value direction):** "I would not approve composite-key migration as written. There is a strong argument to keep the three legacy keys and just migrate raw access to `safe_user_*`: token refresh is a separate high-frequency update domain from user/profile cache, and NiceGUI storage gives you no compare-and-swap." This is a clear architectural direction that resets the entire phase shape.

- **Phase 91 simplifies after the pivot:** The original composite-key proposal had 8 reader updates in `supabase_client.py`, 7 test file rewrites, a migration helper with concurrent-migration handling, and a new `_auth_block` key contract. The revised 3-key proposal has 12 in-place substitutions, 2 small return-value-check additions, 2 new test files. ~70% complexity reduction. Plan-boundary green is now trivially achievable.

- **Surviving threats post-Phase-91** (Codex's "Surviving threats" list, all explicitly accepted):
  - **Narrow half-state window:** A prune-race that lands between `safe_user_set('auth_user', ...)` succeeding and `safe_user_set('auth_profile', ...)` failing would leave `auth_user` set without `auth_profile`. **Mitigation: D-04 rollback** — `set_auth` pops the user key on profile-write failure. The window where half-state is observable is the few microseconds between the two `safe_user_set` calls, AND it requires the prune to fire in that exact window. Acceptable.
  - **`update_profile_cache` failure leaves stale profile:** Best-effort write per D-04 note. Stale profile auto-refreshes on next `get_profile()` call (which reads from Supabase if cache is None). Acceptable.
  - **Surviving sessions from before the deploy:** Users with pre-Phase-91 storage still have `auth_user`/`auth_profile`/`auth_session` keys — same keys Phase 91 writes — so no migration is needed. Zero user-visible behavior change for existing logged-in users.
  - **The OAuth code is one-shot:** If `complete_login` fails the storage write, the user must restart the OAuth flow. Acceptable — auto-retry would require holding the consumed code, which the OAuth provider would reject. Show error + home button is the right UX.

- **Phase 91's strategic position in v7.12:** After Phase 91 ships, the allowlist is empty and the Phase 87 lint scanner enforces zero raw `app.storage.user` accesses in `web/`. Phase 92's final-sweep audit (SWEEP-01, SWEEP-02) becomes a verification step rather than a discovery step — the work is done; we just need to confirm zero residual matches.

</specifics>

<deferred>
## Deferred Ideas

- **Composite-key `_auth_block` consolidation** (Codex F1 — explicitly rejected for Phase 91, captured for future consideration): The architectural narrative for "true multi-key atomicity" would require copy-on-write + a per-session block lock + version-stamping for every auth-block writer (refresh, set_auth, update_profile_cache, clear_auth, complete_login, do_login, terminal-refresh-cleanup). NiceGUI storage gives no CAS primitive, so the lock would have to live in module-level Python state keyed by `_session_uuid`. Phase 89's per-request lists factory was the simpler answer for that domain; here the analog would be a per-session auth-block manager. Defer until a concrete cross-key race surfaces in production traces — none has surfaced yet across Phases 87-90, and the Phase 91 multi-write error handling closes the immediate prune-race window.

- **`web/auth_state.py:get_user_id, get_role, is_admin, etc.` helper consolidation:** The 11 helper methods on `GlobalAuthState` all do `get_user()` or `get_profile()` calls then attribute access. Each of those reads goes through `safe_user_get` after Phase 91 — that's 2+ storage reads per `is_admin()` invocation in pages that call multiple helpers. Could be hot-path-optimized via a per-request memo, but `safe_user_get` is already cheap (dict access + try/except), and PRoiding a stale-cache layer is risky. Defer until a profiler trace shows it matters.

- **`update_profile_cache` rollback semantics:** Currently best-effort per D-04. If the profile update fails, the cache stays stale. A user-experience improvement would be to show a "Profile cache could not be updated; refresh in a moment" toast — but that's UI polish, not security. Defer to a future UX phase.

- **Removing the `try/except Exception` wrappers at `auth_state.py:41-44, 49-52` is part of D-01** (not deferred) — but the same pattern exists elsewhere in the codebase wrapping `safe_user_get` calls. A `/gsd-cleanup` pass could find and remove redundant outer wrappers. Out of Phase 91 scope.

- **OAuth callback "retry" affordance:** The error-shown shape per D-06 shows a "Return to Home" button. A "Retry login" button could re-trigger `get_oauth_url('google', ...)` for a fresh OAuth flow. Useful UX but adds a button + handler; defer to future polish phase.

- **PostHog `login_failed` instrumentation richness:** D-05 + D-06 add `reason: 'session_storage_unavailable'` to `login_failed` posthog events. A dashboard could quantify the rate of prune-race-during-login failures in production to validate that the "narrow half-state window" really is narrow. Out of Phase 91 scope; PostHog tagging is the lightweight enabler.

</deferred>

---

*Phase: 91-atomic-auth-state-writes*
*Context gathered: 2026-05-15*
*Workflow note: This CONTEXT.md captures decisions refined by **one round** of Codex external review. Round 1 (`_tmp/codex_phase91_discuss_review_response.txt`): three blocking findings (F1 composite-key creates RMW race surface, F2 migration helper bypassed by supabase_client.py readers, F3 Phase 87 lint scanner hard-fails on empty allowlist) plus five medium-severity refinements (M1 migration helper ordering — mooted by F1, M2 do_login return-value check, M3 AUTHW-05 T-C reshape, M4 test-rewrite assertions — mooted by F1, M5 AUTHW-06 AST justified). **Codex's Decision-verdict drove the entire phase shape pivot**: from composite-key consolidation (original Claude proposal) to direct raw→safe_user_* substitution with multi-write error handling (revised, accepted). Pattern matches Phase 88/89/90. The pivot reduced the surface ~70% (12 in-place substitutions + 2 small return-value checks + 2 new test files, vs. original 8 reader updates + 7 test rewrites + migration helper + new key contract) — plan-boundary green is now trivial. No round 2 dispatched; the architectural surface after pivot is mechanical enough that a plan-checker round (post-plan, pre-execution) should be sufficient to catch any residual issues per Phase 90 plan-checker pattern.*
