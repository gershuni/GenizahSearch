# Phase 90: Auth Caching Rewrite — No `set_session` — Context

**Gathered:** 2026-05-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Delete the process-wide auth client cache (`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`) and the auth-resurrection guard (`_clear_stale_auth` + the "cleared while waiting for lock" re-read at lines 161-171) from `web/supabase_client.py`. Rewrite `get_user_client()` to build a fresh request-scoped client each call, authenticated by **local header mutation** across all three Supabase sub-clients (PostgREST + functions + storage) — **never** via the networked `auth.set_session(...)`. Refactor the OAuth bootstrap helpers (`sign_in`, `set_session_from_url`, `exchange_code_for_session`) to operate on **throwaway clients**, not the module singleton `get_client()`, so the singleton remains anonymous-only. Add a dedicated `change_password(new_password)` REST helper to replace `profile.py:149-150`'s `get_user_client().auth.update_user(...)` (GoTrue's `update_user` requires a local session, which we can't establish without `set_session`). Add proactive refresh: `get_user_client()` decodes the access-token `exp` claim, refreshes via `_refresh_user_session()` if expired/near-expiry, under a `_session_uuid`-keyed lock with post-lock re-read + stale-token-snapshot comparison to prevent concurrent-refresh token burn. Migrate the captured-handle `_app.storage.user` access at line 128 to `safe_user_get('auth_session')`, self-eliminating the Phase 87 allowlist entry. Install 3 permanent CI guards (static AST scanner with per-helper allowlist + aliased-form seed traps, runtime attr-absence on deleted globals, deterministic refresh-lock behavioral tests with Barrier/Event).

**Out of scope (carved off for other phases):**
- `web/auth_state.py:set_auth/clear_auth/do_login` migration to `safe_storage` helpers — Phase 91 (AUTHW-01).
- OAuth callback in `web/main.py:1419+` migration to safe_storage helpers — Phase 91 (AUTHW-02).
- `sign_out` ordering: server-side revocation BEFORE popping `auth_session`; use the user's authenticated client (not the anonymous singleton) — Phase 91 (AUTHW-03, AUTHW-04).
- Cross-user concurrent smoke test, `docs/guides/MULTITENANT.md` — Phase 92.
- The `reset_client()` function in `web/supabase_client.py:67-70` — kept; legacy retry helper unrelated to auth caching.
- The `_is_jwt_expired()` helper — kept; still useful as a residual reactive-retry signal in the 4 read paths after the proactive primary strategy lands.

</domain>

<decisions>
## Implementation Decisions

### Token Application — Three Sub-Clients, All Local Headers (Area 1, Codex F1)

- **D-01:** Build a helper `_apply_user_auth_to_client(client: Client, access_token: str) -> None` that mutates **all three** sub-client header surfaces:
  ```python
  def _apply_user_auth_to_client(client: Client, access_token: str) -> None:
      bearer = f"Bearer {access_token}"
      # PostgREST (table operations) — supabase-py official helper, local-only
      client.postgrest.auth(access_token)
      # Functions — supabase-py official helper, local-only
      client.functions.set_auth(access_token)
      # Storage — direct header mutation; no supabase-py helper exists,
      # but client.storage.session.headers is a writable httpx Headers dict
      client.storage.session.headers["Authorization"] = bearer
  ```
  Verified local-only (no network call) by reading source: `postgrest/base_client.py:54`, `supabase_functions/_sync/functions_client.py:111`, `httpx.Headers.__setitem__`.
- **D-02 (Codex F1 catch — `client.auth.update_user(...)` path):** `web/pages/profile.py:149-150` calls `get_user_client().auth.update_user({'password': ...})`. GoTrue's `update_user` requires a **local** auth session (`get_session()` at `gotrue_client.py:690`), which we explicitly avoid creating. Header mutation alone cannot satisfy this contract. **Add a dedicated `change_password(new_password: str) -> Dict` helper** in `web/supabase_client.py` that issues a direct `httpx.put` to `{SUPABASE_URL}/auth/v1/user` with the bearer header, bypassing the GoTrue client entirely. Migrate `web/pages/profile.py:149-150` to call this helper instead. This is a small but real change in scope that Plan 90-01 must include.
- **D-03 (Codex F1 catch — authenticated storage path):** `shared/puzzle_publish_service.py:81 publish_join` and line 152 `unpublish_join` use `client.storage.from_(STORAGE_BUCKET).upload/remove`. The puzzle-publish flow at `web/pages/puzzle.py:2665, 2742` hands the authenticated client into the service. D-01's storage header mutation makes this path keep working without code change at the service layer — but the assertion that "storage is anonymous-only in our code" (Claude's original proposal) is **false** and was a near-miss bug. Plan 90-01 must include a smoke check confirming that an authenticated storage upload works with the new header-mutation path.

### Refresh Strategy — Proactive Under Lock, with Reactive Fallback (Area 2, Codex F2)

- **D-04 (Codex F2 catch — coverage gap):** Reactive-only refresh is **insufficient**. Only 4 of ~30 `get_user_client()` callers have JWT-expired retry blocks (`web/supabase_client.py:516, 756, 935, 1101` — `get_user_lists`, `get_projects`, `get_comments`, `get_fragment_joins`). The other ~26 callers — all writes (`update_profile`, `create_list`, `create_project`, `create_comment`, `create_fragment_join`, `add_list_item`, `update_list`, `delete_list_item`, `vote_discovery`, etc.) — return errors directly on exception. Under reactive-only refresh, every authenticated write would silently fail when the access token expires (~60min cadence). **Primary strategy: PROACTIVE refresh in `get_user_client()`.**
- **D-05:** `get_user_client()` decodes the access token's `exp` claim (base64-decode JWT payload; no signature check needed — we trust supabase's tokens locally). If `exp - now < REFRESH_SKEW_SEC` (60s), call `_refresh_user_session(stale_refresh_token=current_refresh_token)` before applying headers. Re-read `auth_session` after the refresh attempt and apply headers from the refreshed tokens. The reactive `_is_jwt_expired` retry blocks at lines 516/756/935/1101 are **kept** as defense-in-depth — when a write returns PGRST303 mid-flight despite proactive refresh (e.g., refresh-clock skew, transient 500 turning JWT stale), the existing retry calls `_refresh_user_session()` and retries.
- **D-06 (Codex refresh-race verdict):** `_refresh_user_session(stale_refresh_token: Optional[str] = None) -> bool` body:
  ```python
  def _refresh_user_session(stale_refresh_token: Optional[str] = None) -> bool:
      """Refresh tokens. Lock by _session_uuid (Phase 87 primitive).

      Codex 2026-05-15: post-lock re-read MUST include both an expiry
      check AND a stale-snapshot comparison to prevent a second thread
      from burning the just-rotated refresh token.
      """
      from web.safe_storage import (
          safe_user_get, safe_user_set, get_session_uuid
      )
      auth_session = safe_user_get('auth_session') or {}
      if not auth_session.get('refresh_token'):
          return False
      session_uuid = get_session_uuid()
      if not session_uuid:
          return False
      with _refresh_locks_guard:
          lock = _refresh_locks.setdefault(session_uuid, threading.Lock())
      with lock:
          # Re-read inside lock — another thread may have rotated.
          auth_session = safe_user_get('auth_session') or {}
          access_token = auth_session.get('access_token')
          refresh_token = auth_session.get('refresh_token')
          if not refresh_token:
              return False
          # Check 1: access token no longer expired → another thread refreshed.
          if access_token and not _access_token_near_expiry(access_token):
              return True
          # Check 2: refresh token rotated since caller's snapshot → done.
          if stale_refresh_token is not None and refresh_token != stale_refresh_token:
              return True
          # We are the rotator.
          try:
              throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
              resp = throwaway.auth.refresh_session(refresh_token)
              if resp and resp.session:
                  safe_user_set('auth_session', {
                      'access_token': resp.session.access_token,
                      'refresh_token': resp.session.refresh_token,
                  })
                  return True
              return False
          except Exception as e:
              logger.warning(f"_refresh_user_session: refresh failed: {e}")
              return False
  ```
- **D-07:** **Do not prune `_refresh_locks` on `sign_out`** (Codex verdict). If a refresh lock is held by another thread when `sign_out` pops it, a subsequent caller can mint a second lock for the same `_session_uuid`, defeating serialization. Lock dict growth is bounded by distinct session uuids ever seen — acceptable (Phase 89 accepted the equivalent for the lists factory). Process restart prunes naturally.
- **D-08 (Codex refresh subtlety — token freshness):** `refresh_session()` calls go through `throwaway.auth.refresh_session(refresh_token)` on a **fresh throwaway client**, not on the module singleton (per D-09). The throwaway is discarded after the refresh; we extract `resp.session.{access_token, refresh_token}` and persist via `safe_user_set`. The throwaway client never gets reused.

### Anonymous Singleton Stays Anonymous (Area 1.5, Codex F3 catch)

- **D-09 (Codex F3 catch — singleton authentication leak):** Verified at `supabase/_sync/client.py:338-346`: when supabase Client receives `SIGNED_IN`/`TOKEN_REFRESHED`/`SIGNED_OUT` events, `_listen_to_auth_events` mutates `self.options.headers["Authorization"]` AND `self.auth._headers["Authorization"]`. Today's `sign_in()` (line 254), `set_session_from_url()` (line 388), and `exchange_code_for_session()` (line 414) all call auth-mutating methods on `get_client()` (the module singleton) — which means the singleton becomes authenticated with the most recently signed-in user's token. **Subsequent callers of `get_client()` receive a singleton with someone else's auth header.** This is a real cross-user leak path that survives `_client_cache` deletion. **Phase 90 MUST refactor the OAuth bootstrap helpers to use throwaway clients.**
- **D-10:** Rewrite `sign_in`, `set_session_from_url`, `exchange_code_for_session` to create local `throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)` instances, call `throwaway.auth.<method>(...)`, extract the returned `user` + `session` dicts, return them — the throwaway client is discarded. `get_client()` is **never** the recipient of `auth.sign_in_*` or `auth.set_session` or `auth.exchange_code_for_session` calls. This makes the singleton **provably anonymous-only**.
- **D-11:** `sign_out()` is more delicate. The current impl calls `get_client().auth.sign_out()` to revoke at Supabase. Phase 90 must NOT change `sign_out`'s server-side semantics — Phase 91 (AUTHW-03, AUTHW-04) explicitly owns the "use the user's authenticated client, revoke first, pop second" refactor. **Phase 90 scope for `sign_out`:** only delete the `_client_cache.pop / _session_locks.pop / _prune_session_client_cache` eviction block at lines 281-293 (the cache it referenced is gone). The `client.auth.sign_out()` call on the singleton stays; Phase 91 will move it to a user-client. Document this seam explicitly in code comments at the `sign_out` definition so Phase 91 can find it.

### Caller-Side: Signature Stable, No Migration Churn (Pre-decided)

- **D-12:** `get_user_client() -> Client` signature unchanged. All 30+ existing callers continue working without edits. Returns a freshly-built, fully-authenticated `Client` (all 3 sub-clients carry the bearer header) per call. If no auth tokens are in storage, returns the anonymous singleton `get_client()` (existing fallback semantics preserved).

### Plan Decomposition (Area 3, Codex revised)

- **D-13 (Codex plan-boundary catch):** The 2-plan split stands, but **Plan 90-01 scope grew** to include all 3 of Codex's blocking findings. The deletion can't go first because the cache fields are still referenced (sign_out eviction block, get_user_client cache lookup) before the rewrite lands. Phase 90 ships as **2 plans**:
  - **Plan 90-01 — Behavior rewrite + Codex-mandated fixes + tests + comments.**
    - Add `_apply_user_auth_to_client(client, access_token)` helper (D-01).
    - Add `_access_token_near_expiry(access_token, skew_sec=REFRESH_SKEW_SEC)` helper — base64-decode JWT payload, parse `exp` claim, compare to `time.time() + skew_sec`. No signature check (we trust supabase tokens locally; if a malformed JWT lands here, treat as expired and let the refresh attempt fail loudly).
    - Add `_refresh_locks: Dict[str, threading.Lock] = {}` and `_refresh_locks_guard = threading.Lock()` module globals (the **new** locks, keyed by `_session_uuid`; NOT to be confused with the old `_session_locks` keyed by `access_token` which dies in Plan 90-02).
    - Add `_refresh_user_session(stale_refresh_token=None) -> bool` per D-06.
    - Rewrite `get_user_client()` per D-05: read tokens via `safe_user_get('auth_session')` (replaces the captured-handle `_app.storage.user` at line 128), proactive expiry check → call `_refresh_user_session(stale_refresh_token=current_refresh_token)` if needed → re-read tokens → build fresh client → `_apply_user_auth_to_client(client, access_token)` → return.
    - Add `change_password(new_password: str) -> Dict` REST helper per D-02; migrate `web/pages/profile.py:149-150` to call it.
    - Refactor `sign_in()`, `set_session_from_url()`, `exchange_code_for_session()` per D-10 to use throwaway clients.
    - Update the 4 reactive retry blocks at `web/supabase_client.py:516, 756, 935, 1101` to call `_refresh_user_session()` instead of `reset_client()` (which only resets the anonymous singleton and has no effect on auth). `reset_client()` itself stays (legacy; used elsewhere indirectly), only its uses inside the JWT-expired retry blocks change.
    - Add the AUTHC-05 docstring/comment at the top of `get_user_client()` citing `gotrue_client.py:713`, explaining why `set_session()` is intentionally avoided mid-flight.
    - Delete the Phase 87 allowlist entry for `web/supabase_client.py` from `.planning/phase87_storage_allowlist.yaml` (it goes from 4 → 3 entries; matches Phase 88's self-elimination pattern for `web/export_state.py`).
    - **`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`, `_clear_stale_auth`, `_prune_session_client_cache`** are kept as dead code in this plan — their only references after the rewrite are inside `sign_out()` (the eviction block at lines 281-293), which Plan 90-02 deletes together with the globals.
    - Plan boundary: full pytest green. The Phase 87 lint scanner still passes (allowlist entry removed, raw access migrated). No user-visible behavior change.
  - **Plan 90-02 — Deletion + static enforcement.**
    - Delete the 4 module globals `_session_locks`, `_locks_guard`, `_client_cache`, `_CLIENT_CACHE_TTL`.
    - Delete the helper functions `_clear_stale_auth(storage)` (lines 96-107) and `_prune_session_client_cache(now)` (lines 73-87).
    - Delete the eviction block in `sign_out()` at lines 281-293 (the `try: from web.safe_storage import safe_user_get; auth_session = ...; access_token = auth_session.get('access_token'); if access_token: _client_cache.pop(...); _session_locks.pop(...); _prune_session_client_cache(); except Exception: pass` block).
    - Install 3 test files per D-14, D-15, D-16.
    - Plan boundary: full pytest green. No user-visible behavior change.
- **D-14:** Plan 90-01 MUST keep all tests green at its boundary. The current `tests/test_version_selector_pending.py` mocks `set_session` (per memory: "mocks `set_session` at the client level"); audit its assertions in Plan 90-01 and retarget mocks to whichever auth helper survives the refactor — most likely it can pass unchanged because version selector doesn't go through `get_user_client()`'s rewritten path, but **the planner must confirm** during the migration.

### Regression Guards — Static AST + Runtime Attr-Absence + Behavioral (Area 4, Codex-refined)

- **D-15 — Static AST scanner (`tests/test_no_set_session_outside_oauth.py`):** Mirrors Phase 89 D-10 structure (scan `web/` + `tests/`, AST walker, seed traps as parsed snippets).
  - Disallowed Call nodes: any Call whose attribute chain matches `.auth.set_session(...)` OR `.auth.exchange_code_for_session(...)`, regardless of receiver name (catches `client.auth.set_session(...)`, `c.auth.set_session(...)`, and `<any>.auth.<method>(...)`).
  - **Per-helper allowlist (Codex catch — narrower than original Claude proposal):**
    - `.auth.set_session(...)` allowed only inside the FunctionDef body of `set_session_from_url` in `web/supabase_client.py`.
    - `.auth.exchange_code_for_session(...)` allowed only inside the FunctionDef body of `exchange_code_for_session` in `web/supabase_client.py`.
    - The original "one shared allowlist for both APIs" was too loose; per-method enforcement makes accidental mixing of bootstrap roles impossible.
  - **Seed-trap snippets (Codex catch — aliased forms):** include at least 6 known-bad strings AS PARSED CODE SNIPPETS via `ast.parse(...)`:
    1. `client.auth.set_session(a, r)` — direct
    2. `c.auth.set_session(a, r)` — alias receiver
    3. `auth = client.auth; auth.set_session(a, r)` — aliased intermediate
    4. `get_client().auth.set_session(a, r)` — singleton resurrection
    5. `client.auth.exchange_code_for_session({...})` — direct OAuth variant
    6. `auth = client.auth; auth.exchange_code_for_session({...})` — aliased OAuth
  - The scanner asserts each parsed snippet, when run through the walker, IS flagged as a violation. Defends against false-negatives.
- **D-16 — Runtime attr-absence test (`tests/test_no_client_cache_globals.py`):** Parametrized over the **6 deleted module-level names**:
  ```python
  DELETED_GLOBALS = [
      '_client_cache', '_session_locks', '_locks_guard',
      '_CLIENT_CACHE_TTL', '_clear_stale_auth',
      '_prune_session_client_cache',
  ]

  @pytest.mark.parametrize('name', DELETED_GLOBALS)
  def test_attr_absent(name):
      import web.supabase_client as mod
      assert not hasattr(mod, name), (
          f"{name} survived Phase 90 deletion. The cache + lock plumbing "
          "was removed; reintroducing it must be a deliberate decision, "
          "not a silent regression."
      )
  ```
- **D-17 — Behavioral refresh-lock test (`tests/test_refresh_lock_per_session.py`):** Codex catch — use `threading.Barrier` / `threading.Event` for deterministic ordering, monkeypatch `web.safe_storage.app` to instance-isolated stubs, and monkeypatch `_refresh_user_session`'s `create_client` to a Mock that records concurrent invocations. **Do NOT spin up real NiceGUI storage contexts in worker threads** — the Phase 87 monkeypatch pattern (`tests/test_browse_state.py` style) is the orthodox shape.
  - **Test A — Same-uuid serialization:** Two threads of same `_session_uuid` call `_refresh_user_session()` simultaneously (Barrier 2). The Mock `refresh_session` checks `concurrent_invocations` counter — must be ≤ 1 at any point. First thread's rotated tokens are persisted; second thread sees the updated `auth_session` post-lock-acquire and returns True without burning another refresh.
  - **Test B — Distinct-uuid parallelism:** Two threads of distinct `_session_uuid`s refresh in parallel. Both Mock `refresh_session` calls fire (no cross-session serialization).
  - **Test C — Stale-snapshot short-circuit (D-06 logic):** Thread 1 acquires lock, refreshes, writes new tokens, releases. Thread 2 acquires lock with `stale_refresh_token = <thread-1's-pre-rotation-token>`. Reads current refresh_token from storage; sees it's different from stale → returns True without calling `refresh_session`. Mock's `concurrent_invocations` ends at 1, not 2.
- **D-18 — Test file count revision (Codex-refined):** **Three** test files (matches Phase 89's 2 + 1 split where the third is behavioral). The 3 cover: static (D-15), runtime (D-16), behavioral (D-17). All three live in Phase 90, all are permanent CI guards.

### Claude's Discretion

- The exact `REFRESH_SKEW_SEC` value (defaults to 60s; supabase tokens last ~60min; 60s skew means we refresh within the last minute of validity). Planner can adjust; trivially configurable via module constant.
- The exact body shape of `change_password(new_password)` — whether it returns the parsed JSON `user` dict on success or a normalized `{'success': True, 'user': ...}` wrapper. Match existing helper conventions in `web/supabase_client.py` (`sign_in`, `sign_up` return `{'success': True, 'user': ..., 'session': ...}`).
- Whether `_access_token_near_expiry` lives as a private module function or as a tiny inline computation in `get_user_client()`. Both are fine; separating it makes D-17 Test C easier to unit-test in isolation.
- Whether the `_refresh_locks` dict pruning (or absence thereof — see D-07) should add a comment about memory growth bounds for future contributors. Recommend: yes, brief inline note matching Phase 89's "unbounded growth accepted for safety-over-leak trade-off" style.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 90 Locked Requirements
- `.planning/REQUIREMENTS.md` §"Auth Caching — Phase 90" — AUTHC-01 through AUTHC-05, the locked scope.
- `.planning/ROADMAP.md` §"Phase 90: Auth Caching Rewrite -- No set_session" — 5 success criteria.
- `.planning/HANDOFF_v7.11.1_path_b.md` §"Path B — proposed scope" item 3 ("Auth: rip out `get_user_client()` caching entirely") — original architectural narrative; flagged the `set_session` networked-call question that Codex resolved.

### Phase 87 + Phase 88 + Phase 89 Foundations (load-bearing for Phase 90)
- `web/safe_storage.py` — Phase 87 chokepoint. Phase 90's storage access (token read at the new line replacing `_app.storage.user` at line 128, token write in `_refresh_user_session`) MUST go through `safe_user_get`/`safe_user_set`. `get_session_uuid()` is the stable cache-key primitive for the new `_refresh_locks` dict.
- `.planning/phase87_storage_allowlist.yaml` — Plan 90-01 deletes the `web/supabase_client.py` entry (currently lines 95-114, the captured-handle pattern with `expected_count=1`). After Phase 90, the allowlist drops to 3 entries (was 4 at v7.12 start, became 3 after Phase 88's `web/export_state.py` self-elimination, and now drops again to 2 with `web/supabase_client.py` self-eliminating).
- `tests/test_no_raw_storage_access.py` — Phase 87 permanent CI lint scanner. Phase 90 makes one allowlist deletion (no additions).
- `.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md` D-04, D-05, D-07 — plan-boundary green discipline, ordering rationale, and static AST guard shape. Phase 90 mirrors D-07's seed-trap-as-parsed-code-snippet pattern.
- `.planning/phases/89-lists-cache-per-request/89-CONTEXT.md` D-08, D-09, D-10, D-11 — 2-plan split, same-commit-as-deletion test-update discipline, static AST scanner shape, runtime attr-absence parametrized test shape. Phase 90 D-15/D-16 are direct mirrors.
- `tests/test_no_deleted_state_references.py` (Phase 88 D-07) — direct template for Phase 90's `tests/test_no_set_session_outside_oauth.py`. Same AST-walker shape; broader disallowed-call surface (Call nodes, not Attribute access).
- `tests/test_no_user_lists_mgr_field.py` (Phase 89 D-11) — direct template for Phase 90's `tests/test_no_client_cache_globals.py`. Same parametrized `pytest.raises(AttributeError)` shape, broader name list.

### Source files modified by Phase 90

Plan 90-01:
- `web/supabase_client.py` — primary surgery:
  - Add `_refresh_locks` (line ~36 area), `_refresh_locks_guard`, `_apply_user_auth_to_client`, `_access_token_near_expiry`, `_refresh_user_session`, `change_password` helpers.
  - Rewrite `get_user_client()` body (lines 110-203) per D-05.
  - Refactor `sign_in()` (line 246), `set_session_from_url()` (line 376), `exchange_code_for_session()` (line 403) to use throwaway clients per D-10.
  - Update 4 reactive retry blocks: lines 516, 756, 935, 1101 (`reset_client()` → `_refresh_user_session()`).
  - Migrate `_app.storage.user` at line 128 to `safe_user_get('auth_session')` (self-eliminates Phase 87 allowlist entry).
  - Add AUTHC-05 docstring/comment at `get_user_client`.
- `web/pages/profile.py:149-150` — replace `client = get_user_client(); response = client.auth.update_user({'password': ...})` with `result = change_password(new_password_input.value)`. Adjust the success/error branch accordingly.
- `.planning/phase87_storage_allowlist.yaml` — delete the `web/supabase_client.py` block (lines 95-114).

Plan 90-02:
- `web/supabase_client.py` — deletions:
  - Lines 31-35: 4 module globals (`_session_locks`, `_locks_guard`, `_client_cache`, `_CLIENT_CACHE_TTL`).
  - Lines 73-87: `_prune_session_client_cache(now)`.
  - Lines 96-107: `_clear_stale_auth(storage)`.
  - Lines 281-293: the `_client_cache.pop / _session_locks.pop / _prune_session_client_cache()` eviction block inside `sign_out()`. The `client.auth.sign_out()` call on `get_client()` STAYS (Phase 91 owns its refactor).

### Test files created by Phase 90 (Plan 90-02)
- `tests/test_no_set_session_outside_oauth.py` — static AST scanner per D-15.
- `tests/test_no_client_cache_globals.py` — runtime attr-absence per D-16.
- `tests/test_refresh_lock_per_session.py` — deterministic behavioral test per D-17.

### Existing tests touching the auth path (planner-audit gate)
- `tests/test_supabase_corrections_client.py` — does not touch `get_user_client` directly per grep. Should pass unchanged.
- `tests/test_version_selector_pending.py` — mocks `set_session` at the client level. Plan 90-01 must audit and either retarget the mocks or confirm the test still passes (the function under test does not go through `get_user_client`'s rewritten path).

### External red-team review (Codex round — Phase 88/89 pattern)
- `_tmp/codex_phase90_discuss_review_prompt.md` — Claude's proposed decisions sent to Codex.
- `_tmp/codex_phase90_discuss_review_response.txt` — Codex's verdicts. Three blocking findings:
  1. **F1: Token application incomplete** — postgrest.auth + functions.set_auth alone breaks `profile.py:149` password change (GoTrue `update_user` needs local session) and `puzzle.py` authenticated storage upload (storage IS used authenticated, not anonymous-only).
  2. **F2: Reactive refresh insufficient** — only 4 of ~30 callers retry on JWT-expired; reactive-only would silently break ~26 write paths. Need proactive refresh in `get_user_client()`.
  3. **F3: Singleton anonymous claim is false** — `supabase/_sync/client.py:338-346` event listener mutates `get_client()`'s headers on `SIGNED_IN`/`TOKEN_REFRESHED`. Today's `sign_in`/`set_session_from_url`/`exchange_code_for_session` on the singleton produce a real cross-user leak path that survives `_client_cache` deletion. Bootstrap helpers must use throwaway clients.
  Plus refined refresh-race verdict (post-lock expiry-check + stale-snapshot comparison; do NOT prune locks on sign_out) and per-helper static-allowlist scope.

### Upstream source-read references (verified facts, NOT in the project tree)
- `supabase_auth/_sync/gotrue_client.py:713` — `set_session()` networked behavior (Codex finding; verified by source-read). Cited in AUTHC-05 docstring.
- `supabase_auth/_sync/gotrue_client.py:690` — `update_user()` requires local session via `get_session()` (D-02 rationale).
- `supabase/_sync/client.py:334-346` — `_listen_to_auth_events` mutates singleton headers (D-09 rationale).
- `postgrest/base_client.py:37-54` — `auth(token)` is local-only header mutation (D-01 verification).
- `supabase_functions/_sync/functions_client.py:111` — `set_auth(token)` is local-only header mutation (D-01 verification).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/safe_storage.py:safe_user_get/set/pop` + `get_session_uuid()`/`ensure_session_uuid()` — Phase 87 chokepoint. The token read at line 128 of `supabase_client.py` becomes `safe_user_get('auth_session')`; the refresh token write becomes `safe_user_set('auth_session', {...})`. `_refresh_locks` are keyed by `get_session_uuid()`.
- `web/supabase_client.py:_is_jwt_expired(error)` — existing helper at lines 90-93. Kept as-is for the reactive defense-in-depth path in the 4 read helpers.
- `tests/test_no_deleted_state_references.py` (Phase 88 D-07) — AST-walker template. Phase 90 D-15 widens the disallowed-node set from `Attribute` to `Call` (catches method invocations like `.auth.set_session(...)`).
- `tests/test_no_user_lists_mgr_field.py` (Phase 89 D-11) — runtime attr-absence template; Phase 90 D-16 mirrors verbatim with different `DELETED_GLOBALS` list.
- `tests/test_browse_state.py` (Phase 87 B3 fix), `tests/test_search_state.py` (Phase 87 B3 fix) — monkeypatch `web.safe_storage.app` pattern for isolated test storage. Phase 90 D-17 reuses this for the behavioral lock test (no real NiceGUI context in worker threads).

### Established Patterns
- **Deletion-not-migration discipline** (Phase 87, 88, 89): Plan 90-01 leaves the 4 module globals + 2 helper functions + 1 eviction block as dead code temporarily; Plan 90-02 deletes them in one commit alongside the regression-guard installation. Plan-boundary green discipline (Phase 88 D-05).
- **Codex red-team after Claude proposes** (Phase 88/89 specifics, repeated here): user is non-technical for these decisions; Claude proposes, Codex red-teams, user picks the synthesis. Worked again — Codex caught 3 blocking findings (storage/auth path completeness, reactive-refresh coverage gap, singleton authentication leak via supabase event listener) that the original Claude analysis missed. Same pattern locked in for Phases 91-92.
- **Static AST guard as durable CI lint** (Phase 87 + 88 + 89): AST walker over `web/` + `tests/` with seed-trap parsed snippets is the orthodox shape for "this name/call is gone forever" enforcement. Phase 90 extends the pattern from `Attribute` access (Phase 88/89) to `Call` invocation (Phase 90 D-15 catches `.auth.set_session(...)` method calls).
- **Throwaway clients for one-shot auth-mutating calls** (Phase 90 NEW pattern): bootstrap helpers (`sign_in`, `set_session_from_url`, `exchange_code_for_session`) and the refresh helper (`_refresh_user_session`) create local `create_client(URL, ANON_KEY)` instances and discard them post-call. The module singleton `get_client()` is **provably anonymous-only** after Phase 90. This pattern generalizes the v7.12 principle of "no cached authenticated clients" beyond the explicit `_client_cache` deletion.

### Integration Points
- **30+ call sites of `get_user_client()`** — unchanged. Plan 90-01 keeps the signature stable; callers receive a freshly-built fully-authenticated client. No caller migration needed.
- **4 reactive JWT-expired retry blocks** at `web/supabase_client.py:516, 756, 935, 1101` — Plan 90-01 updates these to call `_refresh_user_session()` instead of `reset_client()`. Pattern is a 2-3 line per-site change.
- **`web/pages/profile.py:149-150`** — Plan 90-01 migrates the one call site of `client.auth.update_user(...)` to the new `change_password(...)` REST helper. This is the only caller of `client.auth.<method>(...)` returned from `get_user_client()` per Codex grep.
- **`shared/puzzle_publish_service.py:81, 152`** — keeps working without code change; D-01's storage header mutation in `_apply_user_auth_to_client` covers authenticated storage upload/remove.
- **Phase 91 seam at `sign_out()`** — Plan 90-02 deletes the cache-eviction block (lines 281-293) but leaves the `client.auth.sign_out()` call on `get_client()` for Phase 91 to refactor into a user-authenticated revocation. Document the seam with an inline comment so Phase 91 can locate it.
- **`web/auth_state.py:do_login` at line 176** — writes `auth_session` to `app.storage.user` raw (Phase 87 allowlisted, Phase 91 will migrate). Phase 90's `_refresh_user_session` writes through `safe_user_set('auth_session', ...)` already; both writers will converge in Phase 91.

### Why Codex's F3 Catch Matters (High-Value Insight)
Original Claude framing assumed that deleting `_client_cache` was sufficient to eliminate cross-user authenticated-client leakage. Codex traced the supabase Client internals to `_listen_to_auth_events` at `supabase/_sync/client.py:338-346`, where the auth event listener MUTATES `self.options.headers["Authorization"]` and `self.auth._headers["Authorization"]` on `SIGNED_IN`/`TOKEN_REFRESHED`. Today's `sign_in()` calls `client.auth.sign_in_with_password(...)` on the module singleton — which fires `SIGNED_IN` → singleton header becomes `Bearer <user-A's-token>`. Subsequent unrelated callers of `get_client()` (e.g., the anonymous `get_user_lists` read path at line 500) receive a singleton already authenticated as user A. This is **the same class of bug** as the original `_client_cache` cross-user leak, surviving the cache deletion via a different vector (event-listener mutation, not dictionary lookup).

**Fix (D-09, D-10):** bootstrap helpers use throwaway clients. The singleton never receives `auth.sign_in_*`/`auth.set_session`/`auth.exchange_code_for_session` calls, so its event listener never fires, so its `Authorization` header stays at the anon key forever. Genuinely anonymous-only singleton.

This is the Phase-89-equivalent high-value Codex catch (per-ACCESS vs. per-request reframe in Phase 89; here, event-listener-mutation vs. dict-cache reframe).

</code_context>

<specifics>
## Specific Ideas

- **User direction (repeated from Phase 89):** Out of 4 gray areas presented (token mechanism, refresh strategy, plan decomposition, Codex red-team), the user selected ONLY "External red-team round (Codex)". Same pattern as Phase 89: user explicitly delegates technical synthesis to Codex review. The four areas were still fully synthesized via Claude → Codex → user-locked-Codex-synthesis, identical to the Phase 89 flow.
- **Codex's 3 blocking findings (high-value catches):**
  1. **F1 (storage + auth path completeness):** Claude's original "PostgREST + functions covers everything" claim was false; `profile.py` password change uses GoTrue's local-session-required `update_user`, and `puzzle_publish_service.py` uses authenticated storage upload. Forces the `change_password` REST helper (D-02) and the storage header mutation (D-01).
  2. **F2 (reactive-refresh coverage gap):** Claude's "reactive refresh + 4 retry blocks" plan would silently break ~26 write paths on token expiry. Forces proactive refresh in `get_user_client()` (D-04, D-05).
  3. **F3 (singleton authentication leak via event listener):** Real cross-user leak path surviving `_client_cache` deletion, via `supabase/_sync/client.py:334-346`'s mutation of singleton `Authorization` headers on `SIGNED_IN`/`TOKEN_REFRESHED`. Forces throwaway-client refactor of OAuth bootstrap helpers (D-09, D-10).
- **Refresh-race refinement (Codex catch):** "Post-lock token-equality check alone burns the newly-rotated token if the snapshot was taken after another thread already refreshed." Forces the stale-token-snapshot parameter on `_refresh_user_session` (D-06) and the post-lock expiry-check + stale-comparison combined gate.
- **Static scanner allowlist scope refinement (Codex catch):** Original Claude proposal had one shared allowlist for both `.auth.set_session` and `.auth.exchange_code_for_session`. Codex tightened to per-helper enforcement: `set_session` only inside `set_session_from_url`; `exchange_code_for_session` only inside `exchange_code_for_session`. Prevents accidental mixing of OAuth bootstrap roles.
- **Seed-trap expansion (Codex catch):** Original Claude proposal had 4 seed traps; Codex added "aliased auth" forms (`auth = client.auth; auth.set_session(...)`) and "singleton resurrection" (`get_client().auth.set_session(...)`). Final D-15 has 6 seed traps minimum.

</specifics>

<deferred>
## Deferred Ideas

- **`sign_out` refactor to use user-authenticated client + revocation-before-pop ordering:** Codex F3's logic suggests `sign_out` should ALSO use a throwaway-style authenticated client (not the singleton) to actually revoke the user's token server-side. Phase 91 AUTHW-03/AUTHW-04 explicitly own this; Phase 90 only deletes the dead cache-eviction block and leaves a documenting seam comment. Do not bundle into Phase 90.
- **`web/auth_state.py:set_auth/clear_auth/do_login` migration:** Phase 91 AUTHW-01. Phase 90's `_refresh_user_session` does write `safe_user_set('auth_session', ...)` — that's the only Phase-91-adjacent write Phase 90 introduces, and it goes through the chokepoint already. Phase 91 will further atomize the multi-key write block (`auth_user` + `auth_profile` + `auth_session` as a single safe operation).
- **`reset_client()` deletion:** Function at lines 67-70 is unrelated to auth caching — it resets the anonymous singleton, which is now provably anonymous post-Phase-90. Its callers in the 4 retry blocks are replaced by `_refresh_user_session()`; if no other callers exist, the function could be deleted in a future cleanup. Not in Phase 90 scope — out of REQUIREMENTS.md mapping.
- **Background proactive refresh worker:** Pre-refresh at e.g. `exp - 5min` via a per-session background job. Considered, rejected: NiceGUI doesn't have a clean per-session worker primitive, and the in-flight proactive check at `get_user_client()` covers the same ground at zero infra cost. Revisit only if production traces show measurable refresh-storms.
- **JWT signature verification on the access token:** D-05 base64-decodes the JWT payload for `exp` without verifying the signature. We trust supabase's tokens locally; a forged JWT with a future `exp` would just delay our refresh (server would then 401, reactive retry takes over). Not worth the libsodium/cryptography dependency for a defensive read-only check.
- **`_refresh_locks` pruning on long-idle sessions:** D-07 explicitly accepts unbounded growth bounded by distinct session uuids ever seen. Phase 89 accepted the same trade-off for the lists factory. If memory pressure ever appears in production traces, add a TTL-pruning sweeper keyed by `_session_uuid` last-seen timestamp. Not load-bearing today.
- **Async-refresh (use `asyncio.Lock` instead of `threading.Lock`):** GenizahSearch web is NiceGUI on Uvicorn; some handlers are async, some sync. The current `_session_locks: Dict[str, threading.Lock]` is the precedent. Mixing async + threading locks is hairy; defer to a future async-storage refactor (also explicitly out of scope per REQUIREMENTS.md "Out of Scope" §"Async session storage").

</deferred>

---

*Phase: 90-auth-caching-rewrite-no-set-session*
*Context gathered: 2026-05-15*
*Workflow note: This CONTEXT.md captures recommendations refined by 1 round of Codex external review (see `_tmp/codex_phase90_discuss_review_response.txt`). Three blocking findings (storage/auth path completeness, reactive-refresh coverage gap, singleton authentication leak via supabase event listener) plus the refresh-race subtlety (post-lock stale-snapshot comparison) all incorporated Codex catches that the original Claude-only analysis missed. Pattern matches Phases 88 and 89; user direction: "I'm non-technical for these decisions; ask Codex".*
