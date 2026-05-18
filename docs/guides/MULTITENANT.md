# GenizahSearch Multitenant Architecture (v7.12 Path B)

> **Audience:** developers extending the web app's per-user state, auth, caching, or any code that touches `app.storage.user`. Read this BEFORE adding new code under `web/` that interacts with user-scoped data.
>
> **Status:** This is the steady-state reference. Historical context (4 Codex review rounds, 6 phase CONTEXT.md files, cross-AI revision rounds) is preserved under `_tmp/codex_*.txt` and `.planning/phases/87-92/*-CONTEXT.md`. This doc distills the architecture without dragging history into the contributor surface.
>
> **Updated:** 2026-05-18 (Phase 92 closure; v7.12 Path B shipped).

## §1 Why this exists

GenizahSearch is a desktop application (PyQt6) with a long single-user history. The web app (NiceGUI) inherited that single-user mental model, but unlike the desktop app, the web app runs ONE Python process serving MANY concurrent users. Pre-v7.12, several global state surfaces (singleton mirrors on `AppState`, instance-cached `UserListsManager`, access-token-keyed Supabase client cache, raw `app.storage.user` accesses across 14+ files) carried the assumption that the process has ONE user. Under concurrent load, this caused real cross-user leaks — the most user-visible was the v7.11.1 cross-user xlsx export bug where User B's export contained User A's results.

The v7.12 Path B milestone refactored the web layer so per-user state, auth, and caches cannot leak across sessions sharing one Python process. The discipline is:

1. **Single chokepoint.** Every per-user state access routes through `web/safe_storage.py`.
2. **Deletion-not-migration.** Singleton mirrors of per-user state were DELETED (not dual-written) so there is no second source of truth.
3. **Request-scoped auth.** No persistent authenticated client objects; auth headers applied per request via local header mutation.
4. **No `set_session()` mid-flight.** Codex verified at `gotrue_client.py:713` that `set_session()` is networked, not local. Use throwaway clients instead.
5. **Live enforcement.** A pytest AST scanner (`tests/test_no_raw_storage_access.py`) rejects new raw `app.storage.user` accesses in CI.

**This doc is web-only.** The desktop app is genuinely single-user and unaffected.

## §2 The chokepoint (web/safe_storage.py)

`web/safe_storage.py` is THE module that may directly access `app.storage.user`. Every other file in `web/` MUST use the chokepoint helpers.

**API surface:**

```python
from web.safe_storage import (
    safe_user_get,        # read with default fallback; returns default on prune-race
    safe_user_set,        # write; returns False on prune-race -- caller decides recovery
    safe_user_pop,        # delete and return; returns default on prune-race
    get_session_uuid,     # read the 32-char hex _session_uuid if minted
    ensure_session_uuid,  # lazy-mint; idempotent; safe to call multiple times
)

# Read
last_query = safe_user_get('last_search_query', default='')

# Write -- check the return value if the write is load-bearing
if not safe_user_set('last_search_query', query):
    # Storage was pruned (likely concurrent logout / session expiry).
    # Decide whether to retry, show an error, or proceed silently.
    pass

# Delete
old = safe_user_pop('temporary_breadcrumb')
```

**The `_session_uuid` stable cache key:**

Every page render calls `ensure_session_uuid()` at the top of `web/main.py:create_layout`. This minimum-once-per-page-render call guarantees that downstream code can call `get_session_uuid()` and receive a stable 32-char lowercase hex string (`^[0-9a-f]{32}$`) that:

- Is minted ONCE per browser session
- Survives token rotation (it's not derived from the access_token)
- Is stable across all routes in the session
- Is GUARANTEED-DIFFERENT across concurrent sessions

Use `get_session_uuid()` as the keying primitive for anything that needs to scope state per-session-not-per-token. Phase 90's refresh-lock map (`_refresh_locks: Dict[str, asyncio.Lock]`) is keyed on `_session_uuid` for exactly this reason — a token refresh mid-flight does not orphan the lock.

## §3 State separation by deletion (Phase 88 discipline)

When a state mirror creeps in (a field on a module-level singleton or class attribute that holds per-user data), the multitenant-safe response is: **delete the mirror**, not "synchronize it carefully." The dual-write path will eventually drift; the delete is final.

Phase 88 deleted 10 fields from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`). The single replacement path is `web/export_state.py`, which routes through `web.safe_storage` helpers.

**Two regression guards in `tests/`:**

- `tests/test_no_appstate_export_fields.py` (D-06 runtime attr-absence): asserts `hasattr(AppState, field)` is False for each deleted field plus one survivor sanity check.
- `tests/test_no_deleted_state_references.py` (D-07 static AST scanner): walks `web/` + `tests/` for `state.<deleted_field>` / `setattr(state, ...)` / `getattr(state, ...)` AND aliased imports (`from web.state import state as s` / `import web.state as web_state`) — prevents resurrection via aliases.

**The deletion-not-migration discipline applies to any new per-user state too:** if you find yourself adding a mirror field to a singleton, stop and put the state behind `safe_storage` directly instead.

## §4 Request-scoped auth (Phase 90)

**Hard constraint:** No mid-flight `auth.set_session()` calls. Codex verified at `gotrue_client.py:713` that `set_session()` is a NETWORK call (it invokes `get_user(access_token)` when the JWT is valid and `_refresh_access_token(refresh_token)` when expired). It is not a local-only header mutation. Calling `set_session()` to "apply credentials" mid-flight makes a real HTTP roundtrip and can race against concurrent requests in the same Python process.

**The Phase 90 model:**

- `web/supabase_client.py:get_user_client()` builds an authenticated `Client` per call.
- `_apply_user_auth_to_client(client, access_token)` writes 4 headers directly:
  - `client.postgrest.auth(access_token)`
  - `client.functions.set_auth(access_token)`
  - `client.storage._client.headers['Authorization'] = f'Bearer {access_token}'`
  - `client.storage._client.headers['apikey'] = SUPABASE_ANON_KEY`
- Proactive refresh is gated by `_refresh_locks: Dict[str, asyncio.Lock]` keyed on `_session_uuid` (NOT on access tokens, NOT on storage object IDs).
- 4 reactive JWT-expired retry blocks in `get_user_client()` call `_refresh_user_session()` instead of `reset_client()`.
- Auth-mutating helpers (`sign_in`, `sign_up`, `set_session_from_url`, `exchange_code_for_session`, `get_oauth_url`) all use THROWAWAY clients to sidestep the supabase event-listener leak vector at `supabase/_sync/client.py:338-346`.
- `sign_out` calls `throwaway.auth.admin.sign_out(jwt, "global")` for REAL server-side revocation (the high-level `sign_out` on a throwaway is a no-op per `gotrue_client.py:789-793` because the throwaway has no session attached — Codex round-3 P1 catch).

**Three permanent CI guards:**

- `tests/test_no_set_session_outside_oauth.py` (Phase 90 D-15): static AST scanner banning `.auth.set_session(` and `get_client().auth.<mutating>(...)` outside allowed helpers. 13 seed-trap snippets prove the scanner catches real misuse.
- `tests/test_no_client_cache_globals.py` (Phase 90 D-16): runtime attr-absence over 6 names (`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`, `_clear_stale_auth`, `_prune_session_client_cache`).
- `tests/test_refresh_lock_per_session.py` (Phase 90 D-17): behavioral test proving distinct-`_session_uuid` parallelism (Test B observes `max_concurrent == 2` — not a trivial single-thread pass).

If you add new code that touches Supabase auth, route through the existing helpers. If you find yourself wanting to call `set_session()` "just to set headers," re-read this section — it does not do what you think it does.

## §5 Per-request lists (Phase 89)

`UserListsManager` is instantiated per-request in page handlers that need it. No module-level singleton, no time-based cache, no user-id-key plumbing.

Pre-v7.12, `AppState._user_lists_mgr` cached a single `UserListsManager` instance with a 10s TTL `_cache_entry` tuple keyed by user ID. Under concurrent sessions, the cache could resurrect User A's lists for User B during the TTL window. Phase 89 deleted:

- `AppState._user_lists_mgr` singleton attribute
- `_cache_entry` tuple
- 10s TTL constant
- user-id-key plumbing

The replacement is a per-request factory call in the page handlers (`/lists`, `/lists/<id>`). Performance impact is negligible — the manager is cheap to construct and Supabase queries dominate latency.

**Regression guards:**
- Runtime attr-absence test confirms `_user_lists_mgr` is gone from `AppState`.
- Static AST scanner bans re-introducing `_cache_entry` / TTL constants in `web/user_lists.py`.

If you need to cache lists data, do it inside a single request flow (e.g., a local variable in the page handler) — NOT at module scope.

## §6 Atomic auth writes (Phase 91)

Auth state writes touch THREE storage keys (`auth_user`, `auth_profile`, `auth_session`). Partial writes are a real failure mode — a NiceGUI session prune can interrupt a multi-write between keys. Phase 91 made the writes safer:

**`GlobalAuthState.set_auth(user, profile=None) -> bool`** — writes `auth_user` then `auth_profile`. Returns `False` on prune-race during the profile write AND pops `auth_user` to roll back the partial state (SYMMETRIC 2-key rollback). `set_auth` does NOT own `auth_session` — the caller writes session first, then calls `set_auth`. If `set_auth` returns False, the caller MUST defensively pop `auth_session` too (DEFENSIVE 3-key caller cleanup pattern — see `do_login` and `_oauth_complete_login`).

**`GlobalAuthState.clear_auth()`** — revokes server-side via `throwaway.auth.admin.sign_out(jwt, "global")` BEFORE popping local keys, with `finally:` cleanup ensuring local pops happen even if server revocation fails.

**`_oauth_complete_login(...)`** — the OAuth callback path. Factored out of `auth_callback_route` so it can be unit-tested. Implements the same DEFENSIVE 3-key caller cleanup pattern + `show_error` UX on partial-write failure.

**The OAuth callback prune-race fix:** Before Phase 91, a NiceGUI session prune during the OAuth callback could surface an `AssertionError` 500 to the browser. Now the callback gracefully shows an error message and does not propagate the AssertionError. `tests/test_auth_callback_resilience.py` T-A locks this behavior.

## §7 Adding a new per-user state value

**Step 1: Pick a stable string key.** Convention: lowercase, dot-separated for namespacing (e.g., `ui.show_line_numbers`, `search.last_filters_applied`).

**Step 2: Read/write through `safe_storage`:**

```python
from web.safe_storage import safe_user_get, safe_user_set

# Read with default
value = safe_user_get('ui.show_line_numbers', default=True)

# Write -- check return for prune-race recovery if the write is load-bearing
if not safe_user_set('ui.show_line_numbers', new_value):
    # Optional: show a toast, log, or retry
    pass
```

**Step 3: Do NOT add a mirror field on `AppState` or any module-level singleton.** Phase 88 deleted 10 such mirrors. Read directly from storage every time — the cost is negligible (in-memory dict access).

**Step 4: If the new state interacts with auth (e.g., extending profile data):** use `GlobalAuthState.set_auth` rather than direct `safe_user_set('auth_profile', ...)` so the SYMMETRIC 2-key rollback semantics are preserved.

> ⚠️ **WARNING — `profile=None` clears, not "no change"**
>
> `GlobalAuthState.set_auth(user, profile=None)` **deletes** the
> `auth_profile` storage key (i.e., clears any stale profile data).
> This violates the Pythonic Principle of Least Astonishment, where
> `kwarg=None` usually means "no change." We chose `None`-clears
> semantics in Phase 91 to close a Codex HIGH catch: a stale
> `auth_profile` after a partial-write rollback could leak admin/editor
> role to a logged-out user because `GlobalAuthState.get_role()`
> reads `auth_profile` independently of `auth_user`.
>
> **There is no "no change" mode for `profile` in `set_auth`** —
> omitting it AND passing `profile=None` both clear `auth_profile`
> (because the default value is `None`). To **replace** the profile,
> pass the new dict explicitly. To **preserve** an existing profile
> across calls, do not call `set_auth` at all — read the current
> profile via `GlobalAuthState.get_profile()`, mutate your local copy,
> and pass it back via `set_auth(user, profile=<updated_dict>)`.
>
> See `tests/test_auth_callback_resilience.py:T-F` for the regression
> test that locks this semantic in place.

**Step 5: If the new value should be lint-enforced:** it already is — `tests/test_no_raw_storage_access.py` rejects any raw `app.storage.user.{get,pop}` or `app.storage.user[...]` accesses in `web/` outside the (currently empty) allowlist. As long as you route through `safe_storage` helpers, the lint passes automatically.

**Step 6: If the new state must scope to the SESSION (not the request):** key it by `get_session_uuid()` or use `safe_user_*` directly — both survive token refresh. Do NOT key state by access token.

## §8 Enforcement layer

The chokepoint discipline is enforced both automatically and manually.

**Automated (pytest CI):**

- `tests/test_no_raw_storage_access.py` — AST scanner walking every `.py` file under `web/` for `<app_alias>.storage.user.{get,pop}` and `<app_alias>.storage.user[...]` and bare `<app_alias>.storage.user` attribute access. The scanner reads `.planning/phase87_storage_allowlist.yaml` to know which sites are explicitly exempted. **Post-Phase-91 final state: `allowed_raw_access: []`** — no exemptions remain. Any new raw access requires a PR re-adding an allowlist entry with explicit justification AND `expected_count` (H1 schema).
- `tests/test_no_set_session_outside_oauth.py` — bans `.auth.set_session(` and `get_client().auth.<mutating>(...)` outside allowed helpers (Phase 90 D-15).
- `tests/test_no_client_cache_globals.py` — runtime attr-absence over 6 names that Phase 90 deleted (Phase 90 D-16).
- `tests/test_refresh_lock_per_session.py` — behavioral test proving `_session_uuid`-keyed refresh-lock parallelism (Phase 90 D-17).
- `tests/test_no_appstate_export_fields.py` — 10 deleted AppState fields stay deleted (Phase 88 D-06).
- `tests/test_no_deleted_state_references.py` — AST scanner for static references to the deleted fields, with alias-import coverage (Phase 88 D-07).
- `tests/test_auth_callback_resilience.py` — 7 tests covering OAuth callback prune resilience, SYMMETRIC 2-key rollback, DEFENSIVE 3-key caller cleanup, `profile=None` clears-stale (Phase 91 AUTHW-05).
- `tests/test_persist_value_uses_safe_storage.py` — 6 tests retaining the `cca23db3` safe-wrap of `filter_panel.persist_value` (Phase 91 AUTHW-06).

**Manual (no automated scanner):**

The Phase 87 lint scanner covers `app.storage.user.*` AST nodes only. The following surfaces are OUTSIDE its scope and require manual audit on any new code touching them. They were verified clean during Phase 92 SWEEP-01 (see `.planning/phases/92-final-sweep-and-acceptance/92-SWEEP-01-AUDIT.md`):

- **`app.storage.browser`** — NiceGUI cookie-backed per-browser store. Cookies are browser-scoped, not Python-process-scoped, so in-process cross-user leakage is structurally impossible. Known site: `web/auth_state.py:create_login_dialog` "Remember me" boolean. New code should not put PII here.
- **`app.storage.client`** — NiceGUI connection-scoped (per-WebSocket-connection) store. Audit any new code touching it for cross-user leakage.
- **`shared/puzzle_service.py` + `joins.db`** — local SQLite sidecar. Currently community-share semantics (no per-user ownership columns: schema is `id, title, notes, fragments_json, thumbnail_b64`). Per-user puzzle ownership lives in Supabase (RLS-protected cloud DB). If a future phase adds per-user ownership to local joins.db, the multitenant audit re-opens.
- **`web/analytics.py`** — PostHog client is `ui.run_javascript(...)` injection only; no Python-side caching of per-user identifiers. New API integrations (e.g., new analytics providers) must NOT cache user-scoped state at the Python module level.

**Adding a new lint guard:** if you add new code that introduces a new resurrection surface (a module-level dict that could accumulate per-user data, a class attribute that holds session-scoped state), add a corresponding regression guard in `tests/` BEFORE the code lands. Look at `test_no_client_cache_globals.py` for the runtime-attr-absence pattern and `test_no_deleted_state_references.py` for the static-AST pattern.

---

*Architecture reference distilled from v7.12 Path B milestone (Phases 87-92, shipped 2026-05-18).*
*Historical context: `_tmp/codex_*.txt` + `.planning/phases/87-92/*-CONTEXT.md`.*
*Live enforcement: `tests/test_no_raw_storage_access.py` and companion guards in `tests/`.*
