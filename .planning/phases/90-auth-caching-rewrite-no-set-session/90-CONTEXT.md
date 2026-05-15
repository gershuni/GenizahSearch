# Phase 90: Auth Caching Rewrite — No `set_session` — Context

**Gathered:** 2026-05-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Delete the process-wide auth client cache (`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`) and the auth-resurrection guard (`_clear_stale_auth` + the "cleared while waiting for lock" re-read at lines 161-171) from `web/supabase_client.py`. Rewrite `get_user_client()` to build a fresh request-scoped client each call, authenticated by **local header mutation** across all three Supabase sub-clients (PostgREST + functions + storage) — **never** via the networked `auth.set_session(...)`. Refactor the OAuth bootstrap helpers (`sign_in`, `set_session_from_url`, `exchange_code_for_session`) to operate on **throwaway clients**, not the module singleton `get_client()`, so the singleton remains anonymous-only. Add a dedicated `change_password(new_password)` REST helper to replace `profile.py:149-150`'s `get_user_client().auth.update_user(...)` (GoTrue's `update_user` requires a local session, which we can't establish without `set_session`). Add proactive refresh: `get_user_client()` decodes the access-token `exp` claim, refreshes via `_refresh_user_session()` if expired/near-expiry, under a `_session_uuid`-keyed lock with post-lock re-read + stale-token-snapshot comparison to prevent concurrent-refresh token burn. Migrate the captured-handle `_app.storage.user` access at line 128 to `safe_user_get('auth_session')`, self-eliminating the Phase 87 allowlist entry. Install 3 permanent CI guards (static AST scanner with per-helper allowlist + aliased-form seed traps, runtime attr-absence on deleted globals, deterministic refresh-lock behavioral tests with Barrier/Event).

**Scope expansion (Codex round-2 P1 catch — AUTHW-03 + AUTHW-04 pulled forward from Phase 91):** Today's `web/auth_state.py:clear_auth` (lines 120-134) pops `auth_session` BEFORE calling `supabase_sign_out()`. By the time `sign_out` runs, the access token is gone from storage, so today's eviction-by-access-token is already a no-op — but `client.auth.sign_out()` still fires on the module singleton, which is currently authenticated by the event-listener leak Codex F3 catches (`supabase/_sync/client.py:338-346`). That accidental authentication is what revokes a token server-side today (often the wrong user's token, but at least *some* token). The moment Phase 90 D-10 makes the singleton provably anonymous, `client.auth.sign_out()` revokes nothing, and **the user's refresh token stays valid forever on Supabase's servers**. This is a security regression worse than today's broken behavior. AUTHW-03 (revoke before pop) + AUTHW-04 (use user's authenticated client) MUST land in the same phase as the throwaway-bootstrap refactor — i.e., Phase 90.

**Scope expansion (plan-checker round catch — 3 additional singleton-resurrection vectors):** Beyond the 3 helpers in D-10 (`sign_in`, `set_session_from_url`, `exchange_code_for_session`), the plan-checker found that `sign_up` (web/supabase_client.py:210-243) and `get_oauth_url` (web/supabase_client.py:340-373) ALSO call auth-mutating methods on the module singleton via `client = get_client()`. `sign_up` fires `SIGNED_IN` under Supabase auto-confirm (identical F3 leak vector). `get_oauth_url` calls `sign_in_with_oauth` which D-15 Class B bans on the singleton — once the scanner lands in Plan 90-02 it would FLAG this call. Both have active callers (`web/auth_state.py:215` for `sign_up`; `web/auth_state.py:334, 401` for `get_oauth_url`). Both MUST be refactored to throwaway clients in Plan 90-01 (D-10 expanded to 5 helpers). Additionally, the standalone `refresh_session()` at lines 325-339 is DEAD CODE (verified zero non-self callers in `web/` and `shared/`; the new `_refresh_user_session` helper added in Plan 90-01 is the live replacement); it is DELETED in Plan 90-01 atomically with its callsite-free state.

**Out of scope (carved off for other phases):**
- `web/auth_state.py:set_auth/do_login` migration to `safe_storage` helpers — Phase 91 (AUTHW-01). Phase 90 modifies `clear_auth` for the revoke-before-pop reorder, but the raw `app.storage.user.pop(cls.USER_KEY, None)` etc. allowlist entries stay; Phase 91 owns the per-key `safe_user_pop` migration.
- OAuth callback in `web/main.py:1419+` migration to safe_storage helpers — Phase 91 (AUTHW-02).
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
- **D-02 (Codex F1 catch — `client.auth.update_user(...)` path):** `web/pages/profile.py:149-150` calls `get_user_client().auth.update_user({'password': ...})`. GoTrue's `update_user` requires a **local** auth session (`get_session()` at `gotrue_client.py:690`), which we explicitly avoid creating. Header mutation alone cannot satisfy this contract. **Add a dedicated `change_password(new_password: str) -> Dict` helper** in `web/supabase_client.py` that issues a direct `httpx.put` to `{SUPABASE_URL}/auth/v1/user`, bypassing the GoTrue client entirely. Migrate `web/pages/profile.py:149-150` to call this helper instead. Plan 90-01 must include this.

  **Required request shape (Codex round-2 P2 catch — earlier draft only said "with the bearer header" which is incomplete; GoTrue's base client at `supabase_auth/_sync/gotrue_base_api.py:54-58` merges instance headers with per-call headers, so all four headers below must be present explicitly because we're bypassing the GoTrue client and therefore the instance-header merge):**

  ```python
  def change_password(new_password: str) -> Dict:
      """Change current user's password without GoTrue's local-session requirement.

      Direct PUT to /auth/v1/user with explicit headers — Codex P2: the GoTrue
      base client (gotrue_base_api.py:54-58) merges instance headers (apikey +
      Content-Type) with per-call headers (Authorization). When we bypass GoTrue
      entirely we lose that merge, so we must supply all four explicitly.
      """
      import httpx
      from web.safe_storage import safe_user_get
      auth_session = safe_user_get('auth_session') or {}
      access_token = auth_session.get('access_token')
      if not access_token:
          return {'error': 'Not logged in'}
      url = f"{SUPABASE_URL}/auth/v1/user"
      headers = {
          'apikey': SUPABASE_ANON_KEY,                # project-scope API key
          'Authorization': f'Bearer {access_token}',  # user-scope JWT
          'Content-Type': 'application/json',         # request body type
          'Accept': 'application/json',               # explicit response type
      }
      try:
          resp = httpx.put(
              url, headers=headers,
              json={'password': new_password},        # JSON body
              timeout=30.0,
          )
          if resp.status_code == 200:
              return {'success': True, 'user': resp.json()}
          # Surface server error message when available
          try:
              return {'error': resp.json().get('msg') or resp.text}
          except Exception:
              return {'error': resp.text}
      except Exception as e:
          return {'error': str(e)}
  ```

  Per Codex round 2: the previous one-line "bearer header" instruction would have produced a request without `apikey`, which Supabase rejects at the gateway — making `change_password` always return an error in production. The header tetrad above matches what GoTrue's own client sends when `update_user` runs normally.
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
- **D-10 (plan-checker round — expanded from 3 helpers to 5 + dead-code deletion):** Rewrite the **complete set of singleton auth-mutating helpers** to create local `throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)` instances, call `throwaway.auth.<method>(...)`, extract the returned `user` + `session` dicts (or URL dict, for OAuth), return them — the throwaway client is discarded. `get_client()` is **never** the recipient of auth-mutating calls.

  **The 5 helpers covered by D-10:**

  | # | Function | Today's call (lines) | Auth-mutating method | Active callers |
  |---|----------|----------------------|----------------------|----------------|
  | 1 | `sign_in(email, password)` | `web/supabase_client.py:246-275` | `client.auth.sign_in_with_password({...})` | `web/auth_state.py` via `supabase_sign_in` |
  | 2 | `sign_up(email, password, metadata)` | `web/supabase_client.py:210-243` | `client.auth.sign_up({...})` — fires `SIGNED_IN` under auto-confirm | `web/auth_state.py:215` via `supabase_sign_up` |
  | 3 | `set_session_from_url(at, rt)` | `web/supabase_client.py:376-400` | `client.auth.set_session(at, rt)` | OAuth callback (web/main.py) |
  | 4 | `exchange_code_for_session(code)` | `web/supabase_client.py:403-426` | `client.auth.exchange_code_for_session({...})` | OAuth PKCE callback (web/main.py) |
  | 5 | `get_oauth_url(provider, redirect_to)` | `web/supabase_client.py:340-373` | `client.auth.sign_in_with_oauth({...})` | `web/auth_state.py:334, 401` |

  **Why all 5 belong in the same set (the F3 invariant, formalized):** Any `client.auth.<auth-mutating-method>(...)` call where `client` is the module singleton (whether obtained by direct `get_client()` call or via an alias) triggers the Codex F3 event-listener leak at `supabase/_sync/client.py:338-346`. The D-15 Class B scanner declares the invariant; the D-10 throwaway pattern delivers it. The 5 functions above are the **complete** set of singleton auth-mutators in the current codebase — verified via:

  ```bash
  grep -n "client.auth\." web/supabase_client.py | grep -v "get_user\|get_session\|sign_out"
  ```

  The remaining `client.auth.sign_out` is handled separately by D-11 (sign_out throwaway refactor). `client.auth.get_user` / `client.auth.get_session` are READ operations, not auth-mutating, and stay on the singleton safely.

  **Even `get_oauth_url` despite URL-only practice:** In practice today `sign_in_with_oauth` returns a URL without firing `SIGNED_IN` (browser is redirected, callback handles the actual session). The leak is theoretical for this method TODAY. But the D-15 Class B scanner bans `sign_in_with_oauth` on the singleton categorically — once the scanner lands in Plan 90-02 it would FLAG `get_oauth_url`. Refactoring `get_oauth_url` to a throwaway aligns the implementation with the scanner's declared invariant, removing the ambiguity entirely. (Alternative: add `get_oauth_url` to a scanner allowlist; rejected because the throwaway refactor is ~3 LOC and has identical semantics.)

  **Dead-code deletion (atomic with D-10 expansion):** The standalone `refresh_session()` at `web/supabase_client.py:325-339` calls `client = get_client(); client.auth.refresh_session()` — auth-mutating on the singleton. It is DEAD CODE: `grep -rn "\bsupabase_client\.refresh_session\b" web/ shared/` returns zero non-self-references, and the new `_refresh_user_session` helper added in Plan 90-01 is the live replacement. **DELETE the standalone `refresh_session()` function entirely in Plan 90-01** (atomic with its callsite-free state — there is no migration to perform, only deletion). Doing this in 90-01 keeps the D-15 Class B scanner happy when it lands in 90-02 without needing a transient allowlist exception.

- **D-11 (Codex round-2 P1 catch — AUTHW-03 + AUTHW-04 pulled forward):** `sign_out()` cannot stay on the singleton once D-10 makes the singleton anonymous-only. Today's accidental token revocation depends on the singleton being authenticated by the event-listener leak; remove that leak (D-10) and `client.auth.sign_out()` on the singleton revokes nothing. **Phase 90 MUST rewrite `sign_out()` to use a user-authenticated throwaway client.** New `sign_out` signature accepts an optional `access_token` parameter so the caller (`web/auth_state.py:clear_auth`) can pass the token BEFORE popping `auth_session` from storage:

  ```python
  def sign_out(access_token: Optional[str] = None) -> Dict:
      """Revoke the user's session server-side via a throwaway authenticated client.

      Phase 90 D-11 (Codex round-2 P1): we cannot rely on get_client() being
      authenticated — D-10 makes it provably anonymous-only — so we build a
      throwaway, apply the user's token via local headers (no set_session),
      and call sign_out() on the throwaway. Token is passed as a parameter
      so clear_auth can revoke BEFORE popping auth_session from storage.
      """
      if access_token is None:
          # Fall back to reading from storage (covers callers other than
          # clear_auth, though there are none in production today).
          from web.safe_storage import safe_user_get
          auth_session = safe_user_get('auth_session') or {}
          access_token = auth_session.get('access_token')
      if not access_token:
          return {'success': True, 'note': 'no active session to revoke'}
      try:
          throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
          # CODEX ROUND-3 P1: do NOT call throwaway.auth.sign_out().
          # GoTrue's high-level sign_out() reads self.get_session() first
          # (gotrue_client.py:789-793) and only invokes admin.sign_out when
          # a LOCAL session exists. Since v7.12 AUTHC-02 forbids
          # set_session(), the throwaway never has a local session — so
          # auth.sign_out() degenerates to a no-op on the network side.
          # The admin namespace (gotrue_admin_api.py:69-79) exposes the
          # raw POST /auth/v1/logout?scope=global call which carries the
          # JWT directly — that IS the actual revocation we need.
          throwaway.auth.admin.sign_out(access_token, "global")
          return {'success': True}
      except Exception as e:
          return {'error': f'Logout error: {str(e)}'}
  ```

  **Why not `_apply_user_auth_to_client(throwaway, access_token)` first?** Because `admin.sign_out(jwt, scope)` carries the access token as its own `jwt=` parameter, which GoTrue's `_request` helper puts into the `Authorization` header directly (`gotrue_base_api.py:60`). The throwaway's `apikey` instance header (set by `create_client`) supplies the gateway requirement. The postgrest/functions/storage sub-clients are irrelevant for this call — sign_out is a pure GoTrue admin API operation.

  **Alternative direct-httpx path (equivalent):** the same effect is achievable with a direct `httpx.post(f"{SUPABASE_URL}/auth/v1/logout", params={"scope": "global"}, headers={...4 headers...})` — same shape as `change_password` (D-02). Use the `admin.sign_out` helper because it's the public API entry, error-handling is consistent with the rest of `web/supabase_client.py`, and it survives gotrue version bumps that might change the endpoint path.

- **D-11b (clear_auth reorder — Codex round-2 P1):** Modify `web/auth_state.py:120-134:GlobalAuthState.clear_auth` to read the access token, call `supabase_sign_out(access_token)`, **then** pop the local keys. Final shape:

  ```python
  @classmethod
  def clear_auth(cls):
      """Clear authentication (logout). Revoke server-side BEFORE local cleanup
      so the token is actually invalidated on Supabase's side (Phase 90 D-11).

      Local key cleanup happens in a finally block so it runs even when
      server revocation fails — half-state (revoked server-side but local
      keys still present) is worse than no revocation at all.
      """
      from web.safe_storage import safe_user_get
      auth_session = safe_user_get('auth_session') or {}
      access_token = auth_session.get('access_token')
      try:
          # AUTHW-04: server-side revocation FIRST, with the user's own token
          supabase_sign_out(access_token)
      except Exception:
          pass  # Server revocation failed; local cleanup still runs below
      finally:
          # AUTHW-03: local keys popped unconditionally (no half-state)
          app.storage.user.pop(cls.USER_KEY, None)
          app.storage.user.pop(cls.PROFILE_KEY, None)
          app.storage.user.pop('auth_session', None)
      # PostHog reset stays as-is (line 126-129)
      try:
          ui.run_javascript('if(window.posthog)posthog.reset()')
      except Exception:
          pass
  ```

  **Allowlist impact:** the three raw pops at `web/auth_state.py:122-124` stay (they're inside the Phase 87 `web/auth_state.py` allowlist block — Phase 91 AUTHW-01 will migrate them to `safe_user_pop`). Phase 90 only changes the ORDERING and adds the `safe_user_get` read above (which is NOT raw access — it goes through the chokepoint). The `expected_count` values in the allowlist YAML stay correct.

  **The `desktop` desktop app does NOT have this code path** (PyQt6 client uses `supabase_corrections_client.py`); this change is web-only and bypasses desktop entirely.

### Caller-Side: Signature Stable, No Migration Churn (Pre-decided)

- **D-12:** `get_user_client() -> Client` signature unchanged. All 30+ existing callers continue working without edits. Returns a freshly-built, fully-authenticated `Client` (all 3 sub-clients carry the bearer header) per call. If no auth tokens are in storage, returns the anonymous singleton `get_client()` (existing fallback semantics preserved).

### Plan Decomposition (Area 3, Codex revised)

- **D-13 (Codex plan-boundary catch, expanded in round 2):** The 2-plan split stands, but **Plan 90-01 scope grew further** to include AUTHW-03 + AUTHW-04 (the `sign_out` user-client + revoke-before-pop refactor) per Codex round-2 P1. The deletion can't go first because the cache fields are still referenced (`get_user_client` cache lookup) before the rewrite lands. Phase 90 ships as **2 plans**:
  - **Plan 90-01 — Behavior rewrite + Codex round-1 fixes (F1/F2/F3) + Codex round-2 fixes (P1 sign_out, P2 change_password headers) + plan-checker round (D-10 expansion to 5 helpers + dead-code refresh_session deletion) + tests + comments.**
    - Add `_apply_user_auth_to_client(client, access_token)` helper (D-01).
    - Add `_access_token_near_expiry(access_token, skew_sec=REFRESH_SKEW_SEC)` helper — base64-decode JWT payload, parse `exp` claim, compare to `time.time() + skew_sec`. No signature check (we trust supabase tokens locally; if a malformed JWT lands here, treat as expired and let the refresh attempt fail loudly).
    - Add `_refresh_locks: Dict[str, threading.Lock] = {}` and `_refresh_locks_guard = threading.Lock()` module globals (the **new** locks, keyed by `_session_uuid`; NOT to be confused with the old `_session_locks` keyed by `access_token` which dies in Plan 90-02).
    - Add `_refresh_user_session(stale_refresh_token=None) -> bool` per D-06.
    - Rewrite `get_user_client()` per D-05: read tokens via `safe_user_get('auth_session')` (replaces the captured-handle `_app.storage.user` at line 128), proactive expiry check → call `_refresh_user_session(stale_refresh_token=current_refresh_token)` if needed → re-read tokens → build fresh client → `_apply_user_auth_to_client(client, access_token)` → return.
    - Add `change_password(new_password: str) -> Dict` REST helper per D-02 (full 4-header request shape — `apikey` + `Authorization` + `Content-Type` + `Accept` + JSON body); migrate `web/pages/profile.py:149-150` to call it.
    - Refactor **5 helpers** per D-10 to use throwaway clients: `sign_in()`, `sign_up()`, `set_session_from_url()`, `exchange_code_for_session()`, `get_oauth_url()`.
    - **DELETE the standalone `refresh_session()`** at lines 325-339 per D-10 (dead code; zero non-self callers verified; `_refresh_user_session` is the live replacement).
    - **Rewrite `sign_out()` per D-11** — accept optional `access_token` parameter, build throwaway, call `throwaway.auth.admin.sign_out(access_token, "global")` (Codex round-3 P1). Deletes the old cache-eviction block inline as part of the rewrite (lines 281-293 are gone).
    - **Reorder `web/auth_state.py:120-134:GlobalAuthState.clear_auth` per D-11b** — `safe_user_get('auth_session')` to read access_token → call `supabase_sign_out(access_token)` in `try:` → local pops in `finally:` block (atomic local cleanup even if server revocation fails).
    - Update the 4 reactive retry blocks at `web/supabase_client.py:516, 756, 935, 1101` to call `_refresh_user_session()` instead of `reset_client()` (which only resets the anonymous singleton and has no effect on auth). `reset_client()` itself stays (legacy; used elsewhere indirectly), only its uses inside the JWT-expired retry blocks change.
    - Add the AUTHC-05 docstring/comment at the top of `get_user_client()` citing `gotrue_client.py:713`, explaining why `set_session()` is intentionally avoided mid-flight.
    - Delete the Phase 87 allowlist entry for `web/supabase_client.py` from `.planning/phase87_storage_allowlist.yaml` (allowlist drops from **3 → 2** file entries — the current state has 3 entries: `web/auth_state.py`, `web/main.py`, `web/supabase_client.py`; matches Phase 88's `web/export_state.py` self-elimination pattern). Phase 91 will subsequently take 2 → 0 by migrating the `auth_state.py` pops and the `main.py` OAuth callback writes.
    - **`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`, `_clear_stale_auth`, `_prune_session_client_cache`** are kept as dead code in this plan — they have **no remaining references** after the `sign_out` rewrite removes the eviction block. Plan 90-02 deletes the dead globals + helpers.
    - Plan boundary: full pytest green. The Phase 87 lint scanner still passes (allowlist entry removed, raw access migrated). **User-visible behavior change in this plan: logout now ACTUALLY revokes the token server-side** (D-11 + D-11b) — a security correctness improvement over today's accidental-revocation-via-event-listener-leak. No other user-visible behavior change.
  - **Plan 90-02 — Deletion + static enforcement.**
    - Delete the 4 module globals `_session_locks`, `_locks_guard`, `_client_cache`, `_CLIENT_CACHE_TTL`.
    - Delete the helper functions `_clear_stale_auth(storage)` (lines 96-107) and `_prune_session_client_cache(now)` (lines 73-87).
    - The `sign_out` eviction block at lines 281-293 is already gone (Plan 90-01 rewrote `sign_out` and removed it as part of the rewrite, not as a separate later deletion).
    - Install 3 test files per D-15, D-16, D-17.
    - Plan boundary: full pytest green. No user-visible behavior change.
- **D-14:** Plan 90-01 MUST keep all tests green at its boundary. Per a stale MEMORY.md note, `tests/test_version_selector_pending.py` was thought to mock `set_session` at the client level; current grep shows 0 such references in that file (the note is stale). Plan 90-01 audits this by re-running `grep -c "set_session" tests/test_version_selector_pending.py` — if 0, no retarget needed; the commit message records the audit was a confirmed no-op.

### Regression Guards — Static AST + Runtime Attr-Absence + Behavioral (Area 4, Codex-refined)

- **D-15 — Static AST scanner (`tests/test_no_set_session_outside_oauth.py`):** Mirrors Phase 89 D-10 structure (scan `web/` + `tests/`, AST walker, seed traps as parsed snippets). **Codex round-2 P3 catch: the original "chain matching" approach (`.auth.set_session(...)` chain) silently misses aliased forms like `auth = client.auth; auth.set_session(...)` — the Call node's attribute chain is `auth.set_session`, not `.auth.set_session`. Switched to attribute-name matching, which catches all aliases trivially without requiring alias tracking.**

  **Scanner scope boundary (plan-checker round, explicit limitation):** The Class B clause matches the pattern `get_client().auth.<X>(...)` — a *literal* `get_client()` Call node in the receiver chain. It does NOT track alias assignments like `c = get_client(); c.auth.<X>(...)` because alias tracking adds significant complexity (function-scope dataflow, import-alias resolution, etc.) for marginal coverage given that the codebase has zero such aliased uses today (verified by grep). The runtime guard at Plan 90-01's must_haves — explicit enumeration of the 2 remaining `client.auth.` callsites in `web/supabase_client.py` (`get_user()` at line 305, `get_session()` at line 317, both READ-only) — covers the residual gap for the single file where singleton-auth-mutation could plausibly appear. If a future change introduces an aliased pattern outside `web/supabase_client.py`, the planner-time grep gate will catch it; the AST scanner's job is to catch the *common* pattern with deterministic AST matching, not to be a full taint analyzer.

  **Two enforcement classes:**

  **Class A — Auth-mutating method name bans (regardless of receiver chain):**
  - Disallowed Call nodes: ANY Call whose **terminal attribute name** is `set_session` OR `exchange_code_for_session`, regardless of the receiver expression. The walker matches `node.func.attr in {"set_session", "exchange_code_for_session"}` — no alias tracking, no chain reconstruction, just the leaf method name. Catches:
    - `client.auth.set_session(...)` — direct
    - `c.auth.set_session(...)` — short-alias receiver
    - `auth = client.auth; auth.set_session(...)` — aliased intermediate (only `set_session` matters; the LHS is irrelevant)
    - `get_client().auth.set_session(...)` — singleton resurrection
    - `self.client.auth.set_session(...)` — attribute path
    - `client.auth.exchange_code_for_session({...})` — direct OAuth variant
    - `c.auth.exchange_code_for_session({...})` — aliased OAuth variant
  - **Per-helper allowlist:** the walker permits matches only when the enclosing FunctionDef name (anywhere in the lexical chain of `ast.walk` parents) matches the expected pair:
    - `set_session` allowed only inside `set_session_from_url` in `web/supabase_client.py`
    - `exchange_code_for_session` allowed only inside `exchange_code_for_session` in `web/supabase_client.py`
    - The original "one shared allowlist for both APIs" was too loose; per-method enforcement makes accidental mixing of bootstrap roles impossible.
  - **Theoretical false-positive risk:** a user-written method also named `set_session` on an unrelated class would be flagged. Mitigation: the scanner's narrow scope (`web/` only) and the codebase's lack of any other `set_session` consumer (verified by grep) make this acceptable. Worst-case future addition can claim an allowlist exception.

  **Class B — Singleton-resurrection ban (D-10 invariant enforcement, Codex round-2 P3):** The invariant from D-10 — "`get_client()` is never used for auth-mutating calls" — needs its own scanner clause. The walker additionally flags any Call whose receiver chain begins with a Call to a function named `get_client` AND whose path traverses `.auth` AND whose terminal attribute is in the auth-mutating method set. Mathematically:
  - Pattern: `get_client().auth.<X>(...)` where `<X>` ∈ {`set_session`, `sign_in_with_password`, `sign_in_with_oauth`, `sign_in_with_otp`, `sign_up`, `exchange_code_for_session`, `refresh_session`, `update_user`, `sign_out`}
  - Banned EVERYWHERE in `web/` and `tests/`. No allowlist — there is no legitimate reason to perform auth mutation on the singleton.
  - This catches the cross-user leak vector Codex F3 surfaced: even if someone deletes `_client_cache` correctly, doing `get_client().auth.sign_in_with_password(...)` re-authenticates the singleton via the event listener.

  **Seed-trap snippets — 10 minimum (Codex round-2 P3 widened from 6):** include AS PARSED CODE SNIPPETS via `ast.parse(...)`. Walker MUST flag each:

  Class A traps (auth-mutating method anywhere outside the 2 helpers):
  1. `client.auth.set_session(a, r)` — direct
  2. `c.auth.set_session(a, r)` — short-alias receiver
  3. `auth = client.auth\nauth.set_session(a, r)` — aliased intermediate
  4. `client.auth.exchange_code_for_session({})` — direct OAuth variant
  5. `auth = c.auth\nauth.exchange_code_for_session({})` — aliased OAuth variant

  Class B traps (`get_client()` resurrection vectors):
  6. `get_client().auth.set_session(a, r)` — singleton set_session
  7. `get_client().auth.sign_in_with_password({})` — singleton sign-in
  8. `get_client().auth.exchange_code_for_session({})` — singleton OAuth
  9. `get_client().auth.refresh_session(r)` — singleton refresh
  10. `get_client().auth.update_user({})` — singleton user-mutation

  The scanner asserts each parsed snippet IS flagged. Defends against false-negatives where future "improvements" to the walker silently weaken coverage.
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
  - **Test B — Distinct-uuid parallelism:** Two threads of distinct `_session_uuid`s refresh in parallel. Both Mock `refresh_session` calls fire (no cross-session serialization). Use a `threading.local`-backed `_ThreadRoutedApp` proxy so each worker sees its own `safe_storage.app.storage.user` dict (no time-based stagger workaround). A `ConcurrencyRecorder` tracking `max_concurrent` proves the parallelism is real — the assertion is `recorder.max_concurrent == 2`, not just `call_count == 2` (which would pass trivially even if calls were serialized).
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
- `.planning/phase87_storage_allowlist.yaml` — Plan 90-01 deletes the `web/supabase_client.py` entry (currently lines 95-114, the captured-handle pattern with `expected_count=1`). **Allowlist count: currently 3 file entries (`web/auth_state.py`, `web/main.py`, `web/supabase_client.py`); Phase 90 takes it 3 → 2** (verified by `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml`). v7.12 started with 4 entries; Phase 88 took 4 → 3 by self-eliminating `web/export_state.py`. Phase 90 takes 3 → 2 by self-eliminating `web/supabase_client.py`. Phase 91 takes 2 → 0 by migrating `web/auth_state.py` (AUTHW-01) and the `web/main.py` OAuth callback (AUTHW-02).
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
  - Refactor 5 helpers to throwaway clients per D-10: `sign_in()` (line 246), `sign_up()` (line 210), `set_session_from_url()` (line 376), `exchange_code_for_session()` (line 403), `get_oauth_url()` (line 340).
  - **DELETE standalone `refresh_session()`** (lines 325-339) per D-10 — dead code, no non-self callers, replaced by `_refresh_user_session`.
  - **Rewrite `sign_out()` (lines 278-298) per D-11** — accept optional `access_token` parameter, build throwaway, `_apply_user_auth_to_client(throwaway, token)`, call `throwaway.auth.sign_out()` on the throwaway. As part of the rewrite, the old cache-eviction block (lines 281-293 in the current file) is deleted inline — no longer a separate 90-02 step.
  - Update 4 reactive retry blocks: lines 516, 756, 935, 1101 (`reset_client()` → `_refresh_user_session()`).
  - Migrate `_app.storage.user` at line 128 to `safe_user_get('auth_session')` (self-eliminates Phase 87 allowlist entry).
  - Add AUTHC-05 docstring/comment at `get_user_client`.
- `web/auth_state.py:120-134:GlobalAuthState.clear_auth` — reorder per D-11b: read `auth_session.access_token` via `safe_user_get`, call `supabase_sign_out(access_token)` in `try:` BEFORE the local pops, move local pops into `finally:` for atomicity. The three raw `app.storage.user.pop(...)` calls stay (Phase 87 allowlisted; Phase 91 AUTHW-01 will migrate them to `safe_user_pop`). `expected_count` in the allowlist YAML stays correct because no new raw accesses are added.
- `web/pages/profile.py:149-150` — replace `client = get_user_client(); response = client.auth.update_user({'password': ...})` with `result = change_password(new_password_input.value)`. Adjust the success/error branch accordingly.
- `.planning/phase87_storage_allowlist.yaml` — delete the `web/supabase_client.py` block (lines 95-114). Allowlist drops 3 → 2 entries.

Plan 90-02:
- `web/supabase_client.py` — deletions:
  - Lines 31-35: 4 module globals (`_session_locks`, `_locks_guard`, `_client_cache`, `_CLIENT_CACHE_TTL`).
  - Lines 73-87: `_prune_session_client_cache(now)`.
  - Lines 96-107: `_clear_stale_auth(storage)`.
  - **NOTE:** The `sign_out` cache-eviction block at lines 281-293 is already gone — Plan 90-01 removed it as part of the `sign_out` rewrite (D-11). Plan 90-02 only deletes the dead module globals + 2 dead helpers above.

### Test files created by Phase 90 (Plan 90-02)
- `tests/test_no_set_session_outside_oauth.py` — static AST scanner per D-15.
- `tests/test_no_client_cache_globals.py` — runtime attr-absence per D-16.
- `tests/test_refresh_lock_per_session.py` — deterministic behavioral test per D-17.

### Existing tests touching the auth path (planner-audit gate)
- `tests/test_supabase_corrections_client.py` — does not touch `get_user_client` directly per grep. Should pass unchanged.
- `tests/test_version_selector_pending.py` — MEMORY.md historical note claimed it "mocks `set_session` at the client level". Current grep returns 0 such references — the note is stale. Plan 90-01 confirms the audit via `grep -c "set_session" tests/test_version_selector_pending.py` returns 0; no retarget needed.

### External red-team review (Codex rounds 1 + 2 + 3 — Phase 88/89 pattern, tripled here)
- `_tmp/codex_phase90_discuss_review_prompt.md` — Claude's round-1 proposed decisions sent to Codex.
- `_tmp/codex_phase90_discuss_review_response.txt` — Codex round-1 verdicts. Three blocking findings:
  1. **F1: Token application incomplete** — postgrest.auth + functions.set_auth alone breaks `profile.py:149` password change (GoTrue `update_user` needs local session) and `puzzle.py` authenticated storage upload (storage IS used authenticated, not anonymous-only).
  2. **F2: Reactive refresh insufficient** — only 4 of ~30 callers retry on JWT-expired; reactive-only would silently break ~26 write paths. Need proactive refresh in `get_user_client()`.
  3. **F3: Singleton anonymous claim is false** — `supabase/_sync/client.py:338-346` event listener mutates `get_client()`'s headers on `SIGNED_IN`/`TOKEN_REFRESHED`. Today's `sign_in`/`set_session_from_url`/`exchange_code_for_session` on the singleton produce a real cross-user leak path that survives `_client_cache` deletion. Bootstrap helpers must use throwaway clients.
  Plus refined refresh-race verdict (post-lock expiry-check + stale-snapshot comparison; do NOT prune locks on sign_out) and per-helper static-allowlist scope.
- **Codex round 2** (user-supplied review of the round-1-synthesized CONTEXT.md):
  - **P1 (BLOCKING): `sign_out` regression after D-10.** Making the singleton anonymous-only (D-10) breaks the accidental token revocation that today's `client.auth.sign_out()` on the singleton produces via the event-listener leak. After Phase 90 lands as originally drafted, the user's refresh token would NEVER be revoked server-side on logout. **Fix:** pull AUTHW-03 + AUTHW-04 forward from Phase 91 into Phase 90; rewrite `sign_out` to use a user-authenticated throwaway, reorder `clear_auth` to revoke-before-pop with finally-block local cleanup. Encoded as D-11 + D-11b.
  - **P1 (BLOCKING): `change_password` headers incomplete.** Original "with the bearer header" instruction was missing `apikey`; Supabase's gateway requires both. GoTrue's `gotrue_base_api.py:54-58` merges instance headers (which include `apikey`) with per-call headers — bypassing GoTrue means we lose that merge. **Fix:** D-02 expanded to spell out all four headers explicitly (`apikey`, `Authorization: Bearer ...`, `Content-Type: application/json`, `Accept: application/json`) plus JSON body shape.
  - **P2 (MEDIUM): D-15 chain-matching misses aliased forms.** Seed-trap #3 (`auth = client.auth; auth.set_session(...)`) requires alias tracking to catch under chain-matching. **Fix:** D-15 switched to terminal-attribute-name matching (`node.func.attr in {...}`) — catches all aliases trivially. Also added Class B (singleton-resurrection ban: `get_client().auth.<mutating>(...)` with broader method set including `sign_in_*`, `refresh_session`, `update_user`, `sign_out`). Seed traps expanded from 6 to 10 with the Class B traps.
  - **P3 (LOW): Allowlist count inconsistency.** Document said both "4 → 3" and "3 → 2" in different places. Actual count today: 3 file entries. Phase 90 takes 3 → 2. Fixed in canonical_refs and D-13 plan list.
- **Codex round 3** (user-supplied review of the round-2-revised CONTEXT.md):
  - **P1 (BLOCKING): D-11's `throwaway.auth.sign_out()` is still a no-op revocation.** Verified at `gotrue_client.py:789-793`: high-level `sign_out()` reads `self.get_session()` first and only calls `admin.sign_out(...)` when a LOCAL session exists. Since v7.12 AUTHC-02 forbids `set_session()` (the whole point of the throwaway pattern), the throwaway has no local session — so `auth.sign_out()` evaluates `access_token = None` and skips the `admin.sign_out` branch. Network revocation never fires. **Fix:** D-11 body changed to call `throwaway.auth.admin.sign_out(access_token, "global")` directly (`gotrue_admin_api.py:69-79` exposes the raw `POST /auth/v1/logout?scope=global` with the bearer JWT — exactly the revocation we need). Removed the unnecessary `_apply_user_auth_to_client(throwaway, access_token)` preamble because `admin.sign_out(jwt, scope)` carries the JWT directly via the `jwt=` parameter (which gotrue's `_request` injects as the Authorization header per `gotrue_base_api.py:60`); the throwaway's apikey instance header satisfies the gateway. The postgrest/functions/storage sub-clients are irrelevant for the sign_out call. Direct httpx POST to `/auth/v1/logout?scope=global` would be an equivalent alternative — chose `admin.sign_out` for consistency with the rest of `web/supabase_client.py`'s use of the supabase-py public API.
- **Plan-checker round** (post-CONTEXT-lock, pre-execution review of Plan 90-01 + Plan 90-02):
  - **BLOCKER (CRITICAL): D-10's enumerated helper set was incomplete — 3 additional singleton-resurrection vectors found.** Beyond the 3 helpers originally in D-10 (`sign_in`, `set_session_from_url`, `exchange_code_for_session`), the plan-checker found `sign_up` (web/supabase_client.py:210-243, fires `SIGNED_IN` under auto-confirm — identical F3 leak), `get_oauth_url` (web/supabase_client.py:340-373, calls `sign_in_with_oauth` which D-15 Class B bans on the singleton), and the standalone `refresh_session()` (web/supabase_client.py:325-339, dead code with zero non-self callers, replaced by `_refresh_user_session`). All three have active callers (sign_up: auth_state.py:215; get_oauth_url: auth_state.py:334, 401) or none-at-all (refresh_session). **Fix:** D-10 expanded from 3 to 5 helpers (sign_in, sign_up, set_session_from_url, exchange_code_for_session, get_oauth_url) with verbatim refactor instructions per Plan 90-01 Task 3 Steps 1.5 and 1.6; standalone refresh_session DELETED atomically in Plan 90-01 Task 3 Step 1.7 (no migration to perform — it's dead code). Plan 90-01 must_haves replaced the vacuous `grep -n "get_client()\.auth\." web/supabase_client.py returns 0` check (silently passes today because all usage is aliased-form `client = get_client(); client.auth.X(...)`) with an explicit enumeration of the 2 surviving READ-only `client.auth.` callsites (`get_user()` at line 305, `get_session()` at line 317).
  - **WARNING (MEDIUM): Plan 90-02 Test B used a 5ms stagger workaround instead of a real `_ThreadRoutedApp` proxy.** The original Test B implementation had a half-finished proxy class and fell back to a time-based stagger that only asserted `recorder.call_count == 2` (which trivially passes even when calls are serialized). **Fix:** Plan 90-02 Task 3 Test B body replaced with a complete `_ThreadRoutedApp` (`threading.local`-backed dispatcher) + a `ConcurrencyRecorder` helper tracking `max_concurrent`. New assertion: `recorder.max_concurrent == 2` proves real parallelism. Uses `threading.Barrier(2)` for true simultaneity.
  - **WARNING (LOW): Plan 90-01 Task 3 Step 7 audit description was stale.** Said `tests/test_version_selector_pending.py` "mocks `set_session` at the client level"; current grep shows 0 such references — the MEMORY.md note is stale. **Fix:** Step 7 rewritten to confirm via `grep -c "set_session" tests/test_version_selector_pending.py` returning 0 → no retarget needed; record audit as no-op in commit message.
  - **WARNING (LOW): Plan 90-01 Task 3 size grew with the new sub-steps.** Added an EXECUTOR ATTENTION reminder at the top of Task 3's `<action>` to re-read CONTEXT D-11 verbatim before writing the `sign_out` body — prevents paraphrasing the Codex round-3 P1 fix (which would re-introduce a HIGH-severity T-90-03 regression).

### Upstream source-read references (verified facts, NOT in the project tree)
- `supabase_auth/_sync/gotrue_client.py:713` — `set_session()` networked behavior (Codex round-1 finding; verified by source-read). Cited in AUTHC-05 docstring.
- `supabase_auth/_sync/gotrue_client.py:690` — `update_user()` requires local session via `get_session()` (D-02 rationale).
- `supabase_auth/_sync/gotrue_client.py:778-797` — high-level `sign_out()` reads `self.get_session()` and skips `admin.sign_out` when no local session exists (Codex round-3 P1 finding — the reason D-11 calls `admin.sign_out` directly).
- `supabase_auth/_sync/gotrue_admin_api.py:69-79` — `admin.sign_out(jwt, scope)` POSTs `/auth/v1/logout?scope=...` with the bearer JWT carried via the `jwt=` parameter. The actual revocation API.
- `supabase_auth/_sync/gotrue_base_api.py:54-60` — header merge behavior: instance headers (`apikey`) + per-call headers (`Authorization` from `jwt=` parameter) + computed `Content-Type` + API version header. Bypassing GoTrue means losing the instance-header merge (D-02 rationale).
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
- **Deletion-not-migration discipline** (Phase 87, 88, 89): Plan 90-01 leaves the 4 module globals + 2 helper functions as dead code temporarily (the eviction block is deleted inline as part of the `sign_out` rewrite in 90-01 per Codex round-2 P1, not deferred to 90-02); Plan 90-02 deletes the dead globals + helpers in one commit alongside the regression-guard installation. Plan-boundary green discipline (Phase 88 D-05).
- **Codex red-team after Claude proposes** (Phase 88/89 specifics, repeated here): user is non-technical for these decisions; Claude proposes, Codex red-teams, user picks the synthesis. Worked again — Codex caught 3 blocking findings (storage/auth path completeness, reactive-refresh coverage gap, singleton authentication leak via supabase event listener) that the original Claude analysis missed. Same pattern locked in for Phases 91-92.
- **Static AST guard as durable CI lint** (Phase 87 + 88 + 89): AST walker over `web/` + `tests/` with seed-trap parsed snippets is the orthodox shape for "this name/call is gone forever" enforcement. Phase 90 extends the pattern from `Attribute` access (Phase 88/89) to `Call` invocation (Phase 90 D-15 catches `.auth.set_session(...)` method calls).
- **Throwaway clients for one-shot auth-mutating calls** (Phase 90 NEW pattern): bootstrap helpers (`sign_in`, `sign_up`, `set_session_from_url`, `exchange_code_for_session`, `get_oauth_url`) and the refresh helper (`_refresh_user_session`) create local `create_client(URL, ANON_KEY)` instances and discard them post-call. The module singleton `get_client()` is **provably anonymous-only** after Phase 90. This pattern generalizes the v7.12 principle of "no cached authenticated clients" beyond the explicit `_client_cache` deletion.

### Integration Points
- **30+ call sites of `get_user_client()`** — unchanged. Plan 90-01 keeps the signature stable; callers receive a freshly-built fully-authenticated client. No caller migration needed.
- **4 reactive JWT-expired retry blocks** at `web/supabase_client.py:516, 756, 935, 1101` — Plan 90-01 updates these to call `_refresh_user_session()` instead of `reset_client()`. Pattern is a 2-3 line per-site change.
- **`web/pages/profile.py:149-150`** — Plan 90-01 migrates the one call site of `client.auth.update_user(...)` to the new `change_password(...)` REST helper. This is the only caller of `client.auth.<method>(...)` returned from `get_user_client()` per Codex grep.
- **`shared/puzzle_publish_service.py:81, 152`** — keeps working without code change; D-01's storage header mutation in `_apply_user_auth_to_client` covers authenticated storage upload/remove.
- **`sign_out()` + `clear_auth()` rewrite — pulled forward from Phase 91** (Codex round-2 P1). Plan 90-01 rewrites both: `sign_out(access_token)` uses a user-authenticated throwaway client to actually revoke server-side (D-11); `clear_auth` reorders to revoke-before-pop with finally-block local cleanup (D-11b). After Phase 90, the only Phase 91 work remaining on this code path is migrating the three `app.storage.user.pop(...)` calls in `clear_auth` to `safe_user_pop` (AUTHW-01) — a strictly local-state-migration concern with no security-correctness implications.
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
- **Seed-trap expansion (Codex catches across both rounds):** Round 1 added "aliased auth" forms (`auth = client.auth; auth.set_session(...)`) and "singleton resurrection" (`get_client().auth.set_session(...)`). Round 2 caught that chain-matching can't see through aliases — switched D-15 to attribute-name matching plus added a Class B clause for `get_client().auth.<mutating>(...)` with a broader method set including `sign_in_*`, `refresh_session`, `update_user`, `sign_out`. Final D-15 has **10** seed traps minimum (5 Class A + 5 Class B).
- **Codex round-2 catches (4 additional findings on the round-1 synthesis):**
  1. **P1 (BLOCKING) — `sign_out` regression after D-10:** D-10's anonymous-singleton-only invariant breaks today's accidental token revocation. Without pulling AUTHW-03 + AUTHW-04 into Phase 90, refresh tokens are NEVER revoked server-side on logout. Encoded as D-11 + D-11b (`sign_out(access_token=...)` rewrite + `clear_auth` revoke-before-pop reorder).
  2. **P1 (BLOCKING) — `change_password` request headers:** Original "with the bearer header" guidance missed `apikey` (Supabase's gateway rejects without it). GoTrue's base client at `gotrue_base_api.py:54-58` merges instance + per-call headers; bypassing GoTrue loses that merge. D-02 expanded to all 4 headers + JSON body shape.
  3. **P2 (MEDIUM) — Static scanner alias gap:** Chain-matching `.auth.set_session(...)` can't catch aliased `auth = client.auth; auth.set_session(...)` without explicit alias tracking. Switched to attribute-name matching (catches all aliases trivially). Added Class B singleton-resurrection ban.
  4. **P3 (LOW) — Allowlist count inconsistency:** "4 → 3" vs "3 → 2" in different places. Actual: 3 file entries today, Phase 90 takes 3 → 2.
- **Codex round-3 catch (1 additional blocking finding on the round-2 revision):**
  1. **P1 (BLOCKING) — D-11's `throwaway.auth.sign_out()` is a no-op:** GoTrue's high-level `sign_out()` at `gotrue_client.py:789-793` reads `self.get_session()` first; since the throwaway has no local session (no `set_session` call ever made), the `admin.sign_out` branch is skipped and revocation never fires. **Fix:** D-11 now calls `throwaway.auth.admin.sign_out(access_token, "global")` directly, which POSTs `/auth/v1/logout?scope=global` with the bearer JWT (`gotrue_admin_api.py:69-79`). Removed the unnecessary `_apply_user_auth_to_client(throwaway, access_token)` preamble — `admin.sign_out` carries the JWT via its `jwt=` parameter.
- **Plan-checker round catch (1 critical finding on the plans, post-CONTEXT-lock):**
  1. **BLOCKER (CRITICAL) — D-10 helper set incomplete:** 3 additional singleton-resurrection vectors found beyond the original 3 helpers — `sign_up` (active F3 vector on auto-confirm), `get_oauth_url` (D-15 Class B scanner would flag it once installed), and the standalone dead-code `refresh_session()` (zero non-self callers). **Fix:** D-10 expanded from 3 to 5 helpers (sign_in + sign_up + set_session_from_url + exchange_code_for_session + get_oauth_url) with verbatim refactor instructions in Plan 90-01 Task 3 Steps 1.5–1.6; dead-code `refresh_session()` DELETED in Plan 90-01 Task 3 Step 1.7 (no migration; it's dead). Plan 90-01 must_haves grep gate strengthened from a vacuous `get_client().auth.` literal check to an explicit enumeration of the 2 surviving READ-only callsites. Plan 90-02 Test B threading proxy + concurrency recorder completed (max_concurrent assertion replaces trivial call_count check). Plan 90-01 Step 7 audit description rewritten to reflect current grep state (MEMORY.md note was stale). EXECUTOR ATTENTION reminder added to Plan 90-01 Task 3 to prevent paraphrasing D-11 verbatim body.

</specifics>

<deferred>
## Deferred Ideas

- ~~**`sign_out` refactor to use user-authenticated client + revocation-before-pop ordering:**~~ **No longer deferred — pulled forward into Phase 90 per Codex round-2 P1 (D-11 + D-11b).** Without this, Phase 90 would ship a security regression (refresh tokens never revoked server-side on logout).
- **`web/auth_state.py:set_auth/do_login` migration to `safe_user_*`:** Phase 91 AUTHW-01. Phase 90 now modifies `clear_auth` for the revoke-before-pop reorder (D-11b) but does NOT migrate the three raw `app.storage.user.pop(...)` calls — Phase 91 owns the `safe_user_pop` swap. Phase 90's `_refresh_user_session` already writes via `safe_user_set('auth_session', ...)` through the chokepoint. Phase 91 will further atomize the multi-key write block (`auth_user` + `auth_profile` + `auth_session` as a single safe operation).
- **`reset_client()` deletion:** Function at lines 67-70 is unrelated to auth caching — it resets the anonymous singleton, which is now provably anonymous post-Phase-90. Its callers in the 4 retry blocks are replaced by `_refresh_user_session()`; if no other callers exist, the function could be deleted in a future cleanup. Not in Phase 90 scope — out of REQUIREMENTS.md mapping.
- **Background proactive refresh worker:** Pre-refresh at e.g. `exp - 5min` via a per-session background job. Considered, rejected: NiceGUI doesn't have a clean per-session worker primitive, and the in-flight proactive check at `get_user_client()` covers the same ground at zero infra cost. Revisit only if production traces show measurable refresh-storms.
- **JWT signature verification on the access token:** D-05 base64-decodes the JWT payload for `exp` without verifying the signature. We trust supabase's tokens locally; a forged JWT with a future `exp` would just delay our refresh (server would then 401, reactive retry takes over). Not worth the libsodium/cryptography dependency for a defensive read-only check.
- **`_refresh_locks` pruning on long-idle sessions:** D-07 explicitly accepts unbounded growth bounded by distinct session uuids ever seen. Phase 89 accepted the same trade-off for the lists factory. If memory pressure ever appears in production traces, add a TTL-pruning sweeper keyed by `_session_uuid` last-seen timestamp. Not load-bearing today.
- **Async-refresh (use `asyncio.Lock` instead of `threading.Lock`):** GenizahSearch web is NiceGUI on Uvicorn; some handlers are async, some sync. The current `_session_locks: Dict[str, threading.Lock]` is the precedent. Mixing async + threading locks is hairy; defer to a future async-storage refactor (also explicitly out of scope per REQUIREMENTS.md "Out of Scope" §"Async session storage").
- **D-15 scanner alias tracking (e.g., `c = get_client(); c.auth.X(...)`):** The Class B clause matches the *literal* `get_client()` Call node in the receiver chain; it does NOT track alias assignments because the codebase has zero such patterns today (verified by grep). Adding alias tracking would require function-scope dataflow analysis — significant complexity for marginal coverage. The Plan 90-01 must_haves grep gate (explicit enumeration of the 2 surviving READ-only `client.auth.` callsites in `web/supabase_client.py`) covers the residual gap for the single file where singleton-auth-mutation could plausibly appear. If a future pattern emerges, extend the scanner with a 4th Class C clause for `Name → get_client()` aliasing — well-defined extension point.

</deferred>

---

*Phase: 90-auth-caching-rewrite-no-set-session*
*Context gathered: 2026-05-15*
*Workflow note: This CONTEXT.md captures recommendations refined by **three rounds** of Codex external review plus **one round of plan-checker review** (post-CONTEXT-lock, pre-execution). Round 1 (`_tmp/codex_phase90_discuss_review_response.txt`): three blocking findings (F1 storage/auth path completeness, F2 reactive-refresh coverage gap, F3 singleton authentication leak via supabase event listener) plus the refresh-race subtlety (post-lock stale-snapshot comparison). Round 2 (user-supplied review of the round-1 synthesis): four findings — P1 sign_out regression after D-10 (forcing AUTHW-03 + AUTHW-04 pull-forward via D-11 + D-11b), P1 change_password headers incomplete (D-02 expanded to all 4 headers + JSON body), P2 D-15 chain-matching alias gap (D-15 switched to attribute-name matching + Class B singleton-resurrection ban + 10 seed traps), P3 allowlist count inconsistency (fixed to 3 → 2). Round 3 (user-supplied review of the round-2 revision): one blocking finding — P1 D-11's `throwaway.auth.sign_out()` is a no-op because gotrue's high-level sign_out skips revocation when no local session exists (`gotrue_client.py:789-793`); D-11 now calls `throwaway.auth.admin.sign_out(access_token, "global")` directly to hit `/auth/v1/logout?scope=global` with the bearer JWT. Plan-checker round (post-CONTEXT-lock, pre-execution review of Plans 90-01 + 90-02): one CRITICAL finding — D-10 helper set incomplete (3 additional singleton-resurrection vectors: sign_up + get_oauth_url + dead-code refresh_session); D-10 expanded from 3 to 5 helpers with dead-code refresh_session deleted atomically; plus 3 lower-severity warnings (Plan 90-02 Test B threading proxy completed, Plan 90-01 Step 7 audit description corrected for stale MEMORY.md note, EXECUTOR ATTENTION reminder added to Task 3). Pattern matches Phases 88 and 89; user direction: "I'm non-technical for these decisions; ask Codex" — then user authorized the plan-checker round to catch what Codex missed; "when the plan-checker catches what Codex missed, fix it directly". Three Codex rounds plus a plan-checker round is unusual — this is the highest-correctness-stakes phase of the milestone, and each round uncovered a real bug class the prior synthesis missed.*
