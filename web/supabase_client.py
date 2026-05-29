# -*- coding: utf-8 -*-
"""
Supabase Client for GenizahSearch

This module provides the Supabase client and helper functions for:
- Authentication (sign up, sign in, sign out)
- User profile management
- Lists and items CRUD operations
- Corrections, comments, discoveries, and joins

Replaces the former standalone `genizah-backend` data service (removed Jan
2026) for data operations — NOT FastAPI the framework, which still serves
the in-process `/api/*` routes.
"""

import asyncio as _asyncio_memo
import logging
import os
import threading
import time
import weakref as _weakref_memo
from contextvars import ContextVar
from typing import Optional, Dict, List
from supabase import create_client, Client
from supabase_auth.errors import AuthApiError
from shared.supabase_provider import get_url, get_anon_key

logger = logging.getLogger(__name__)

# ============= Phase 92.2 D-VER-01 instrumentation =============
# Dormant unless GS_LISTS_PERF_INSTRUMENTATION=1 is set in environment.
# Per-task isolation via ContextVar (Reviews MUST-FIX 4):
#   - One ContextVar value per asyncio.Task (= per NiceGUI HTTP request)
#   - No threading.Lock needed — ContextVar.set() writes to the current
#     context only; concurrent requests in the same process cannot
#     contaminate each other's counters.
#   - Safe under pytest -n auto parallel runners (Gemini-MEDIUM).

_INSTRUMENTATION_ENABLED: bool = os.getenv('GS_LISTS_PERF_INSTRUMENTATION') == '1'
_inst_query_count: ContextVar[int] = ContextVar('_inst_query_count', default=0)
_inst_client_build_count: ContextVar[int] = ContextVar('_inst_client_build_count', default=0)
_inst_query_latencies_ms: ContextVar[tuple] = ContextVar(
    '_inst_query_latencies_ms',
    default=(),
)
# Reviews Round-2 MEDIUM-3: per-request correlation ID set by the ASGI
# middleware (web/main.py _ListsPerfRouteTimingMiddleware) at the start of
# each /lists request; consumed by the sidebar decomposition log (lists.py)
# and any later log emissions that need to be paired with the canonical
# wall-clock record. Without this, pairing log lines by timestamp
# proximity silently fails under any concurrency (multiple browser tabs
# hitting /lists at the same moment, or pytest -n auto).
_inst_request_id: ContextVar[str] = ContextVar('_inst_request_id', default='')


def _inst_set_request_id(rid: str) -> None:
    """Set the per-request correlation ID. Called by the ASGI middleware
    at request entry; the value is then read by `_inst_request_id.get()`
    from every downstream emitter in the same task."""
    if not _INSTRUMENTATION_ENABLED:
        return
    _inst_request_id.set(rid or '')


def _inst_reset() -> None:
    """Reset all instrumentation counters for the current task context."""
    if not _INSTRUMENTATION_ENABLED:
        return
    _inst_query_count.set(0)
    _inst_client_build_count.set(0)
    _inst_query_latencies_ms.set(())
    _inst_request_id.set('')


def _inst_record_query(latency_ms: float) -> None:
    """Record a Supabase query with its latency in milliseconds."""
    if not _INSTRUMENTATION_ENABLED:
        return
    _inst_query_count.set(_inst_query_count.get() + 1)
    _inst_query_latencies_ms.set(_inst_query_latencies_ms.get() + (float(latency_ms),))


def _inst_record_client_build() -> None:
    """Record that a new authenticated Client was built (create_client call)."""
    if not _INSTRUMENTATION_ENABLED:
        return
    _inst_client_build_count.set(_inst_client_build_count.get() + 1)


def _inst_snapshot() -> Dict:
    """Return a snapshot dict of current instrumentation counters for this task."""
    latencies = sorted(_inst_query_latencies_ms.get())
    n = len(latencies)

    def _pct(p: float) -> float:
        if n == 0:
            return 0.0
        idx = min(n - 1, round((n - 1) * p))
        return float(latencies[idx])

    return {
        'query_count': _inst_query_count.get(),
        'client_build_count': _inst_client_build_count.get(),
        'p50_query_latency_ms': _pct(0.50),
        'p95_query_latency_ms': _pct(0.95),
        'max_query_latency_ms': max(latencies) if latencies else 0.0,
        'request_id': _inst_request_id.get(),  # Reviews Round-2 MEDIUM-3
    }

# ============= END Phase 92.2 D-VER-01 instrumentation =============

# ============================================================================
# Phase 92.2 D-MEMO-01..04: Task-scoped get_user_client() memo
# ============================================================================
# WHY: Per-render /lists fanout was building ~30 Clients per request (Phase
# 92.1 reader migration moved 12 readers to get_user_client()). Each build
# costs 50-100ms (create_client + _apply_user_auth_to_client headers).
#
# WHAT: A weakref.WeakKeyDictionary keyed by asyncio.current_task(). The
# value is a dict keyed by (_session_uuid, access_token). When the task
# finishes (= when the /lists request completes in NiceGUI), Python GC
# reclaims the entry. The memo CANNOT survive across requests — that is
# the Phase 90 D-12 "no cross-request caching" invariant, preserved by
# construction here.
#
# WHY NOT nicegui.context.client (Codex Issue 1 + CONTEXT.md D-MEMO-01):
# NiceGUI Client is per-page (per-tab), not per-request. Memoizing on it
# would create a per-tab long-lived cache — directly reopens Phase 90 D-12.
#
# KEY COMPOSITION (Codex CONFIRM, CONTEXT.md D-MEMO-02 + Reviews Codex-MEDIUM-2):
# (get_persisted_session_uuid(), access_token). get_persisted_session_uuid()
# is the strict UUID-validating accessor — it refuses malformed UUIDs and
# never mints. This prevents the memo from being poisoned by a malformed
# or absent session_uuid (which would otherwise key the dict on None or a
# garbage string).
#
# PATTERN DRIFT FROM Phase 90 D-12:
# Phase 90 D-12 explicitly deleted _client_cache + _CLIENT_CACHE_TTL +
# _prune_session_client_cache from this file to close the multitenant
# safety hole. This is the FIRST re-introduction of any caching in
# get_user_client(). It is permitted because WeakKeyDictionary keyed
# by asyncio.Task makes the cache lifetime equal to the request
# lifetime in NiceGUI — Python GC enforces the invariant rather than
# explicit eviction. Future contributors: do NOT widen this to a
# cross-request cache; the Phase 90 D-12 hole reopens immediately.
_user_client_memo: '_weakref_memo.WeakKeyDictionary' = _weakref_memo.WeakKeyDictionary()

# ============= Phase 90 (Plan 90-01): refresh-only locks keyed by _session_uuid.
# _refresh_locks does NOT cache any Client objects -- it serializes refresh
# ROTATIONS only, so the one-time-use refresh token never races concurrent
# consumption. Per CONTEXT D-07: lock-dict growth is bounded by distinct
# session uuids ever seen; we deliberately do NOT prune on sign_out (a
# held lock + pop = a second lock for the same uuid = defeated
# serialization). Process restart prunes naturally. Same trade-off as
# Phase 89 lists factory.
_refresh_locks: Dict[str, threading.Lock] = {}
_refresh_locks_guard = threading.Lock()
REFRESH_SKEW_SEC = 60  # refresh within last minute of token validity

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load from centralized provider (reads from environment variables)
SUPABASE_URL = get_url()
SUPABASE_ANON_KEY = get_anon_key()

# Singleton client instance
_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL:
            raise ValueError(
                "SUPABASE_URL not set! "
                "Set it in environment variables or .env file."
            )
        if not SUPABASE_ANON_KEY:
            raise ValueError(
                "SUPABASE_ANON_KEY not set! "
                "Set it in environment variables or .env file."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client


def reset_client():
    """Reset the singleton client (e.g., after JWT expiry)."""
    global _client
    _client = None


def _is_jwt_expired(error) -> bool:
    """Check if a Supabase error is a JWT expiry."""
    msg = str(error)
    return 'JWT expired' in msg or 'PGRST303' in msg


def _apply_user_auth_to_client(client: Client, access_token: str) -> None:
    """Authenticate a supabase Client by LOCAL header mutation across all
    three sub-clients (PostgREST + functions + storage).

    Phase 90 (Plan 90-01, D-01): we deliberately avoid the GoTrue
    session-setting helper at `gotrue_client.py:713` because it is
    networked (it calls `get_user` or `_refresh_access_token`). Local
    header mutation is sufficient for every authenticated read/write/
    upload path in the codebase:
      - PostgREST  -> `client.postgrest.auth(token)` (postgrest/base_client.py:37-54, local-only).
      - Functions  -> `client.functions.set_auth(token)` (supabase_functions/_sync/functions_client.py:111, local-only).
      - Storage    -> direct header mutation; no supabase-py helper exists,
                     but `client.storage.session.headers` is a writable
                     httpx Headers dict. Authenticated uploads
                     (shared/puzzle_publish_service.py:81, 152) use this.

    Codex round-1 F1 catch (storage path completeness): Claude's original
    proposal of "PostgREST + functions covers everything" was false -- the
    puzzle publish flow uploads to authenticated storage.
    """
    bearer = f"Bearer {access_token}"
    client.postgrest.auth(access_token)
    client.functions.set_auth(access_token)
    client.storage.session.headers["Authorization"] = bearer


def _access_token_near_expiry(access_token: str, skew_sec: int = REFRESH_SKEW_SEC) -> bool:
    """Return True if the JWT's `exp` claim is within `skew_sec` of now.

    Decodes the JWT payload via base64 -- NO signature verification (we
    trust supabase's tokens locally; a forged JWT with a future `exp`
    would just delay refresh until the server returns 401, at which point
    the reactive defense-in-depth retry path takes over). Returns True
    on any decode/parse failure so refresh fires loudly rather than
    silently skipping.
    """
    import base64
    import json
    try:
        parts = access_token.split('.')
        if len(parts) != 3:
            return True  # malformed -> treat as expired
        payload_b64 = parts[1]
        # JWT base64url uses no padding; restore for stdlib decoder.
        padding = '=' * (-len(payload_b64) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(payload_bytes)
        exp = payload.get('exp')
        if not isinstance(exp, (int, float)):
            return True
        return (exp - time.time()) < skew_sec
    except Exception:
        return True  # any failure -> refresh defensively


def _refresh_user_session(stale_refresh_token: Optional[str] = None) -> bool:
    """Refresh tokens. Lock by persisted _session_uuid (Phase 87 primitive).

    Codex 2026-05-15 (D-06): post-lock re-read MUST include both an
    expiry check AND a stale-snapshot comparison to prevent a second
    thread from burning the just-rotated refresh token.

    Codex review round 1 M1: keys by ``get_persisted_session_uuid()``
    (returns None under prune), NOT ``get_session_uuid()`` (mints
    ephemeral). Ephemeral UUIDs would defeat the per-session
    serialization invariant. When persistence is unavailable, refresh
    is skipped and the caller falls back to the anonymous singleton --
    safer than racing.

    Codex review round 1 H3: on known terminal refresh failures
    (invalid/consumed/expired refresh token), local auth keys are
    popped so the UI stops believing the user is logged in. Once
    Plan 90-02 removes the previous stale-auth cleanup hook nothing
    else cleans up `auth_session` from storage; without this branch
    the UI would keep retrying refresh forever against a server that
    will always return 400.
    """
    from web.safe_storage import (
        safe_user_get, safe_user_set, safe_user_pop, get_persisted_session_uuid,
    )
    from supabase_auth.errors import AuthApiError
    auth_session = safe_user_get('auth_session') or {}
    if not auth_session.get('refresh_token'):
        return False
    session_uuid = get_persisted_session_uuid()
    if session_uuid is None:
        # No persisted UUID (prune race or first-request before
        # ensure_session_uuid). Skip refresh -- caller will fall back
        # to the anonymous singleton. Better than minting an
        # ephemeral UUID that would defeat per-session serialization
        # (Codex review round 1 M1).
        logger.debug("_refresh_user_session: no persisted _session_uuid; skipping refresh")
        return False
    with _refresh_locks_guard:
        lock = _refresh_locks.setdefault(session_uuid, threading.Lock())
    with lock:
        # Re-read inside lock -- another thread may have rotated.
        auth_session = safe_user_get('auth_session') or {}
        access_token = auth_session.get('access_token')
        refresh_token = auth_session.get('refresh_token')
        if not refresh_token:
            return False
        # Check 1: access token no longer expired -> another thread refreshed.
        if access_token and not _access_token_near_expiry(access_token):
            return True
        # Check 2: refresh token rotated since caller's snapshot -> done.
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
        except AuthApiError as e:
            # Codex review round 1 H3: known terminal refresh failures
            # mean the refresh token is invalid/consumed/expired on
            # the server side. Match on message substrings (covers
            # older GoTrue versions without a `code` attribute), the
            # `code` attribute itself, AND HTTP 400 (Supabase's
            # canonical response for consumed/missing refresh tokens
            # at /auth/v1/token?grant_type=refresh_token).
            msg = str(e).lower()
            code = (getattr(e, 'code', '') or '').lower() if getattr(e, 'code', None) is not None else ''
            status = getattr(e, 'status', 0)
            terminal = (
                'refresh_token_not_found' in msg or
                'refresh token not found' in msg or
                'invalid refresh token' in msg or
                code in {'refresh_token_not_found', 'invalid_refresh_token'} or
                status == 400
            )
            if terminal:
                logger.warning(
                    "_refresh_user_session: terminal refresh failure (%s, code=%r, status=%r) - clearing local auth keys",
                    e, code, status,
                )
                safe_user_pop('auth_session', None)
                safe_user_pop('auth_user', None)
                safe_user_pop('auth_profile', None)
            else:
                logger.warning("_refresh_user_session: refresh failed (non-terminal): %s", e)
            return False
        except Exception as e:
            # Transient errors (network, 5xx) - do NOT clear auth
            # keys. The next proactive-refresh attempt will retry.
            logger.warning(f"_refresh_user_session: refresh failed: {e}")
            return False


def get_user_client() -> Client:
    """Return a freshly-built, fully-authenticated supabase Client for the
    current user. Stable signature -- all 30+ call sites use this verbatim.

    Phase 90 (D-05, D-12) request-scoped strategy:
      1. Read tokens via safe_user_get('auth_session') -- routes through the
         Phase 87 chokepoint (replaces the old captured-handle pattern
         `storage = _app.storage.user`, since removed, which was unsafe per
         Codex round-4 CRITICAL-1: FilePersistentDict can be GC'd mid-flight
         when prune_user_storage fires).
      2. No tokens -> return the anonymous module singleton get_client()
         (existing fallback semantics preserved).
      3. Proactively check `_access_token_near_expiry(access_token)`; if
         True, call `_refresh_user_session(stale_refresh_token=...)` which
         rotates under a `_session_uuid`-keyed lock with stale-snapshot
         short-circuit. Re-read tokens post-refresh.
      4. Build a fresh request-scoped client via `create_client(...)`.
      5. Apply user auth by LOCAL header mutation via
         `_apply_user_auth_to_client(client, access_token)` -- sets all
         three sub-clients (PostgREST, functions, storage) without any
         network call.
      6. Return the fresh client. NO caching -- Phase 90 D-12 invariant.

    AUTHC-05: Why we do NOT invoke the GoTrue session-setting helper
    on the client here -- verified at
    `supabase_auth/_sync/gotrue_client.py:713`: that helper is NETWORKED
    (it calls `get_user(access_token)` when JWT is valid, or
    `_refresh_access_token(refresh_token)` when expired). Calling it on
    every authenticated request would add a round-trip per call, and
    burning the one-time-use refresh token concurrently across requests
    would log users out. Local header mutation (D-01) is sufficient for
    every PostgREST/functions/storage path in this codebase; GoTrue's
    `auth.update_user(...)` is the one exception, handled by the dedicated
    `change_password(...)` REST helper (D-02).

    Singleton-anonymous-only invariant (D-09, D-10): all five bootstrap
    helpers (sign_in / sign_up / set_session_from_url /
    exchange_code_for_session / get_oauth_url) use throwaway clients --
    they never touch the singleton's auth state. This prevents the
    supabase event-listener leak at
    `supabase/_sync/client.py:338-346` from authenticating get_client()
    with the most-recently-signed-in user's token (Codex round-1 F3 +
    plan-checker round catch).
    """
    try:
        from web.safe_storage import safe_user_get
        auth_session = safe_user_get('auth_session') or {}
        access_token = auth_session.get('access_token')
        refresh_token = auth_session.get('refresh_token')

        if not access_token or not refresh_token:
            logger.info("[get_user_client] No auth_session tokens — returning anonymous singleton")
            return get_client()

        # Proactive refresh: if access_token is near expiry, rotate
        # BEFORE building the client. Stale-snapshot comparison inside
        # _refresh_user_session prevents concurrent burn (D-06).
        #
        # R3-M1 fix: honor _refresh_user_session's return value. False
        # means either no persisted UUID (prune race) OR a terminal
        # refresh failure cleared auth_session. Either way, fall back
        # to the anonymous singleton -- do NOT build an authenticated
        # client with a stale token.
        if _access_token_near_expiry(access_token):
            refreshed = _refresh_user_session(stale_refresh_token=refresh_token)
            if not refreshed:
                logger.info(
                    "[get_user_client] Refresh failed/skipped (no persisted "
                    "UUID or terminal refresh error cleared auth_session) -- "
                    "returning anonymous singleton"
                )
                return get_client()
            # Re-read after refresh attempt -- _refresh_user_session may
            # have rotated tokens AND/OR popped auth_session entirely on
            # terminal failure (Codex review round 1 H3 cleanup branch).
            # Reading post-refresh observes whichever side effect won.
            auth_session = safe_user_get('auth_session') or {}
            access_token = auth_session.get('access_token')
            if not access_token:
                logger.info("[get_user_client] Refresh attempt left no access_token — returning anonymous")
                return get_client()

        # Phase 92.2 D-MEMO-01..04: task-scoped memo (Reviews Codex-MEDIUM-2 +
        # Gemini-MEDIUM — use the validated get_persisted_session_uuid() accessor;
        # this is the SOLE _session_uuid read in get_user_client() — no redundancy
        # by construction because get_user_client() did NOT previously read
        # _session_uuid at all; it reads auth_session via safe_user_get at the top).
        from web.safe_storage import get_persisted_session_uuid as _gp_uuid
        session_uuid = _gp_uuid()  # validated; returns None for malformed/missing

        memo_task = None
        memo_key = None
        if session_uuid:
            try:
                memo_task = _asyncio_memo.current_task()
            except RuntimeError:
                memo_task = None
            if memo_task is not None:
                memo_key = (session_uuid, access_token)
                task_memo = _user_client_memo.get(memo_task)
                if task_memo is not None:
                    cached = task_memo.get(memo_key)
                    if cached is not None:
                        logger.debug(
                            "[get_user_client] memo hit for task=%s key=(uuid=%s..., tok=%s...)",
                            id(memo_task), session_uuid[:8], access_token[:8] if access_token else None,
                        )
                        return cached

        user_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        _inst_record_client_build()  # Phase 92.2 D-VER-01: count authenticated client builds
        _apply_user_auth_to_client(user_client, access_token)

        # Store in memo if task scope is available (asyncio.current_task() is not None).
        # Reviews Codex-MEDIUM-3: when called from run.io_bound / asyncio.to_thread,
        # asyncio.current_task() returns None and we correctly fall through to "no memo
        # entry written" — fresh client built each call. Test 8 verifies.
        if memo_task is not None and memo_key is not None:
            task_memo = _user_client_memo.get(memo_task)
            if task_memo is None:
                task_memo = {}
                _user_client_memo[memo_task] = task_memo
            task_memo[memo_key] = user_client

        return user_client
    except Exception as e:
        logger.error(f"[get_user_client] Error building per-user client: {e}")
        return get_client()


# ============================================================================
# AUTHENTICATION
# ============================================================================

def sign_up(email: str, password: str, metadata: Dict = None) -> Dict:
    """Register a new user via a throwaway client (D-10 invariant).

    Phase 90 (D-09, D-10, plan-checker round catch): does NOT touch the
    module singleton get_client(). Supabase fires SIGNED_IN under auto-
    confirm mode, which causes the event listener at
    supabase/_sync/client.py:338-346 to mutate the singleton's
    Authorization header -- identical F3 leak vector as sign_in. The
    plan-checker round caught this; D-10 expanded to 5 helpers.

    Args:
        email: User's email
        password: User's password
        metadata: Optional user metadata (full_name, affiliation, etc.)

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        options = {'data': metadata} if metadata else {}
        response = throwaway.auth.sign_up({
            'email': email,
            'password': password,
            'options': options if options else None
        })
        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session) if response.session else None
            }
        return {'error': 'Registration failed - no user returned'}
    except AuthApiError as e:
        return {'error': str(e)}
    except Exception as e:
        return {'error': f'Registration error: {str(e)}'}


def sign_in(email: str, password: str) -> Dict:
    """Sign in an existing user via a throwaway client (D-10 invariant).

    Phase 90 (D-09, D-10): does NOT touch the module singleton
    get_client(). The supabase event listener at
    supabase/_sync/client.py:338-346 mutates the singleton's
    Authorization header on SIGNED_IN -- using the singleton here would
    leave it authenticated as this user for subsequent unrelated
    callers, a cross-user leak path that survives even after Plan 90-02
    deletes the prior singleton client cache (Codex round-1 F3).
    """
    try:
        throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        response = throwaway.auth.sign_in_with_password({
            'email': email,
            'password': password
        })
        if response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Login failed'}
    except AuthApiError as e:
        return {
            'error': str(e),
            'error_code': getattr(e, 'code', ''),
            'status_code': getattr(e, 'status', ''),
        }
    except Exception as e:
        return {'error': f'Login error: {str(e)}'}


def sign_out(access_token: Optional[str] = None) -> Dict:
    """Revoke the user's session server-side via admin-scoped global logout.

    Phase 90 D-11 (Codex round-3 P1 + Codex review round 1 M2): we
    cannot rely on get_client() being authenticated -- D-10 makes it
    provably anonymous-only. We build a throwaway client and invoke
    the admin namespace's scoped logout directly, supplying the
    user's JWT and the "global" scope. This bypasses GoTrue's
    high-level sign_out() entirely (see body comment for the
    gotrue_client.py:789-793 rationale referenced in Codex transcript).
    Token is passed as a parameter so clear_auth can revoke BEFORE
    popping auth_session from storage (D-11b atomic local cleanup).
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
        # CODEX ROUND-3 P1: do NOT call the high-level auth-namespace logout
        # on the throwaway. GoTrue's high-level sign_out() reads
        # self.get_session() first (gotrue_client.py:789-793) and only
        # invokes admin.sign_out when a LOCAL session exists. Since
        # v7.12 AUTHC-02 forbids the GoTrue session-setting helper, the
        # throwaway never has a local session -- so the high-level path
        # degenerates to a no-op on the network side. The admin namespace
        # (gotrue_admin_api.py:69-79) exposes the raw
        # POST /auth/v1/logout?scope=global call which carries the
        # JWT directly -- that IS the actual revocation we need.
        throwaway.auth.admin.sign_out(access_token, "global")
        return {'success': True}
    except Exception as e:
        return {'error': f'Logout error: {str(e)}'}


def change_password(new_password: str) -> Dict:
    """Change current user's password without GoTrue's local-session requirement.

    Direct PUT to /auth/v1/user with explicit headers -- Codex round-2 P2:
    the GoTrue base client (gotrue_base_api.py:54-58) merges instance
    headers (apikey + Content-Type) with per-call headers (Authorization).
    When we bypass GoTrue entirely we lose that merge, so we must supply
    all four explicitly. The 4-header tetrad below matches what GoTrue's
    own client sends when update_user runs normally -- without `apikey`,
    Supabase's gateway rejects the request and change_password would
    always return an error in production.

    Replaces the old `client.auth.update_user({'password': ...})` call at
    profile.py:149-150 -- GoTrue's update_user requires a LOCAL session
    (gotrue_client.py:690 via get_session()), which we explicitly avoid
    creating to honor AUTHC-02. Header mutation alone cannot satisfy
    update_user's contract; bypass GoTrue entirely.
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


def get_current_user() -> Optional[Dict]:
    """Get the currently authenticated user."""
    try:
        # Phase 92.1 KEEP get_client(): GoTrue auth-API inspection of the
        # anonymous singleton (NOT a table read). Migrating to get_user_client
        # would build a fresh Client just to read its auth state -- wasteful
        # and pointless. Singleton stays anonymous-only post-Phase 90 D-10.
        client = get_client()
        response = client.auth.get_user()
        if response and response.user:
            return _user_to_dict(response.user)
        return None
    except Exception:
        return None  # Operation failed; use fallback value


def get_session() -> Optional[Dict]:
    """Get the current session."""
    try:
        # Phase 92.1 KEEP get_client(): same rationale as get_current_user above
        # -- auth-API inspection of the singleton, not a table read.
        client = get_client()
        session = client.auth.get_session()
        if session:
            return _session_to_dict(session)
        return None
    except Exception:
        return None  # Operation failed; use fallback value


def get_oauth_url(provider: str = 'google', redirect_to: str = None) -> Dict:
    """Get OAuth URL for social login via a throwaway client (D-10 invariant)
    AND persist the PKCE code verifier per NiceGUI session (Codex review
    round 1 H1).

    Phase 90 (D-09, D-10, plan-checker round catch): does NOT touch the
    module singleton get_client(). While sign_in_with_oauth in practice
    returns a URL without firing SIGNED_IN, the D-15 Class B scanner
    installed in Plan 90-02 categorically bans sign_in_with_oauth on
    the singleton -- refactoring to a throwaway here aligns
    implementation with the scanner's declared invariant.

    Codex review round 1 H1: PKCE code verifier persistence.
    sign_in_with_oauth generates the verifier and stashes it in the
    throwaway's in-memory storage at `{storage_key}-code-verifier`
    (gotrue_client.py:_get_url_for_provider, "PKCE storage stash").
    The throwaway is GC'd when this function returns -- without
    explicit persistence the verifier is LOST and the subsequent
    exchange_code_for_session() call cannot recover it. We extract
    it from the throwaway's storage and re-persist via safe_user_set
    so exchange_code_for_session can read it back and pass it
    explicitly as the `code_verifier` parameter.

    Args:
        provider: OAuth provider ('google', 'github', etc.)
        redirect_to: URL to redirect after auth

    Returns:
        Dict with 'url' on success, or 'error' on failure
    """
    try:
        from web.safe_storage import safe_user_set
        # Codex round 2 P1 fix: dict-options form raises AttributeError
        # at runtime (create_client expects Optional[SyncClientOptions],
        # not dict -- see supabase/_sync/client.py:108). The default
        # flow_type in SyncClientOptions is already 'pkce' (verified
        # via `python -c "from supabase.lib.client_options import
        # SyncClientOptions; print(SyncClientOptions().flow_type)"` ->
        # 'pkce'), so the bare 2-arg form is equivalent AND simpler.
        throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        options_dict = {}
        if redirect_to:
            options_dict['redirect_to'] = redirect_to
        response = throwaway.auth.sign_in_with_oauth({
            'provider': provider,
            'options': options_dict,
        })
        if response and response.url:
            # Codex review round 1 H1: extract the PKCE verifier from
            # the throwaway's storage and persist it per-session.
            # GoTrue stores it at `{storage_key}-code-verifier`; the
            # default storage_key is supabase_auth.constants.STORAGE_KEY
            # ("supabase.auth.token"). Read via the private storage
            # accessor on the auth client -- this is the stable
            # contract verified at gotrue_client.py:1063-1078 (set)
            # and gotrue_client.py:exchange_code_for_session (get).
            try:
                storage = throwaway.auth._storage
                storage_key = throwaway.auth._storage_key
                verifier = storage.get_item(f"{storage_key}-code-verifier")
                if verifier:
                    safe_user_set('oauth_code_verifier', verifier)
                else:
                    logger.warning(
                        "get_oauth_url: PKCE verifier missing from throwaway storage "
                        "(key=%s-code-verifier) -- OAuth callback will fail with "
                        "'Code verifier and code challenge do not match'",
                        storage_key,
                    )
            except Exception as e:
                logger.warning(
                    "get_oauth_url: PKCE verifier extraction failed: %s "
                    "-- OAuth callback may fail", e,
                )
            return {'success': True, 'url': response.url}
        return {'error': 'Failed to generate OAuth URL'}
    except Exception as e:
        return {'error': f'OAuth error: {str(e)}'}


def set_session_from_url(access_token: str, refresh_token: str) -> Dict:
    """Set session from OAuth callback tokens via throwaway client (D-10).

    Phase 90 D-09/D-10: legitimate bootstrap use of set_session -- this is
    the ONLY place `set_session` is allowed by the D-15 static AST
    scanner (Class A allowlist: enclosing function name must match
    `set_session_from_url`). Throwaway prevents the singleton-leak path
    Codex F3 surfaced.

    Args:
        access_token: The access token from URL
        refresh_token: The refresh token from URL

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        response = throwaway.auth.set_session(access_token, refresh_token)
        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Failed to set session'}
    except Exception as e:
        return {'error': f'Session error: {str(e)}'}


def exchange_code_for_session(code: str) -> Dict:
    """Exchange OAuth code for session via throwaway client (D-10) with
    explicit PKCE code verifier round-trip (Codex review round 1 H1).

    Phase 90 D-09/D-10: PKCE bootstrap -- this is the ONLY function
    allowed to call `auth.exchange_code_for_session` per the D-15
    static AST scanner (Class A allowlist: enclosing function name
    must match `exchange_code_for_session`).

    Codex review round 1 H1: the code_verifier is read from
    `safe_user_get('oauth_code_verifier')` (set by get_oauth_url
    Step 1.6 above) and passed explicitly as the `code_verifier`
    parameter to GoTrue's exchange_code_for_session. The verifier
    cannot be retrieved from the throwaway's own in-memory storage
    because that storage is fresh on every throwaway construction --
    the verifier set by the EARLIER throwaway in get_oauth_url is
    gone. We pop after read so the verifier isn't reusable across
    OAuth attempts (defense in depth -- verifier should be one-shot).

    Args:
        code: The authorization code from URL query parameter

    Returns:
        Dict with 'user' and 'session' on success, or 'error' on failure
    """
    try:
        from web.safe_storage import safe_user_pop
        # Pop the PKCE verifier persisted by get_oauth_url (Step 1.6).
        # Pop (not get) so one verifier serves one OAuth round-trip.
        code_verifier = safe_user_pop('oauth_code_verifier', None)
        if not code_verifier:
            # No verifier in storage -- either the user came directly to
            # the callback without going through get_oauth_url, or the
            # NiceGUI session was pruned between the URL request and the
            # callback. The exchange will fail; surface a clearer error
            # than GoTrue's "Code verifier and code challenge do not match".
            logger.warning(
                "exchange_code_for_session: no PKCE code_verifier in storage -- "
                "OAuth round-trip lost the verifier (session prune or direct callback)"
            )
            return {'error': 'OAuth verifier missing -- please retry login'}
        # Codex round 2 P1 fix: dict-options form raises AttributeError
        # at runtime; default flow_type in SyncClientOptions is already
        # 'pkce'. Bare 2-arg form is equivalent and simpler.
        throwaway = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        response = throwaway.auth.exchange_code_for_session({
            'auth_code': code,
            'code_verifier': code_verifier,  # H1: explicit verifier round-trip
        })
        if response and response.user:
            return {
                'success': True,
                'user': _user_to_dict(response.user),
                'session': _session_to_dict(response.session)
            }
        return {'error': 'Failed to exchange code for session'}
    except Exception as e:
        return {'error': f'Code exchange error: {str(e)}'}


def _user_to_dict(user) -> Dict:
    """Convert Supabase user object to dictionary."""
    return {
        'id': str(user.id),
        'email': user.email,
        'created_at': user.created_at,
        'updated_at': user.updated_at,
        'user_metadata': user.user_metadata or {}
    }


def _session_to_dict(session) -> Dict:
    """Convert Supabase session object to dictionary."""
    if not session:
        return None
    return {
        'access_token': session.access_token,
        'refresh_token': session.refresh_token,
        'expires_at': session.expires_at,
        'token_type': session.token_type
    }


# ============================================================================
# PROFILE OPERATIONS
# ============================================================================

def get_profile(user_id: str) -> Optional[Dict]:
    """Get a user's profile."""
    try:
        # Phase 92.1 KEEP get_client(): TO public fields only. MUST migrate to
        # get_user_client if private fields (e.g., email, settings) are ever added
        # to this SELECT. Today the profiles SELECT is `TO public USING (true)`;
        # the anon singleton returns rows correctly. Migrating would build a fresh
        # authenticated Client (refresh check + header mutation) on every authenticated
        # profile read for zero correctness gain. get_profile is called from
        # page-render fast paths (/me, /lists, /admin badge).
        # See PLAN 92.1-01 reader_disposition_table.
        client = get_client()
        response = client.table('profiles').select('*').eq('id', user_id).single().execute()
        return response.data
    except Exception:
        return None  # Operation failed; use fallback value


def get_user_corrections_count(user_id: str) -> int:
    """Get count of approved corrections for a user."""
    try:
        # Phase 92.1 KEEP get_client(): TO public fields only (query is approved-only,
        # TO public branch suffices). MUST migrate to get_user_client if extended to
        # count non-approved-self or any other user-private filter. Current query is
        # `.eq('status', 'approved')` -- the corrections `TO public USING (status='approved')`
        # RLS branch handles anon and authenticated identically. Anon already returns the
        # correct count.
        client = get_client()
        response = client.table('corrections').select('id', count='exact').eq('author_id', user_id).eq('status', 'approved').execute()
        return response.count if response.count is not None else 0
    except Exception:
        return 0  # Operation failed; use fallback value


def update_profile(user_id: str, data: Dict) -> Dict:
    """Update a user's profile."""
    try:
        client = get_user_client()
        response = client.table('profiles').update(data).eq('id', user_id).execute()
        if response.data:
            return {'success': True, 'profile': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# LISTS OPERATIONS
# ============================================================================

def get_list_item_counts() -> Dict[int, int]:
    """Batched item-count lookup for the authenticated caller's lists.

    Phase 92.2 D-FANOUT-02 + Reviews MUST-FIX 1, 2: replaces the per-list
    `_get_list_item_count` fanout with ONE RPC call. The RPC
    `get_list_item_counts_for_user()` is `security invoker` so the
    caller's auth.uid() applies to the inner user_lists RLS policy.
    Zero-arg signature — identity derived from JWT inside the function
    (Reviews MUST-FIX 2 — eliminates `p_user_id` anti-pattern).

    Returns:
        Dict[int, int] mapping list_id to item_count. Lists with zero
        items are absent from the result; callers should default to 0
        via `counts.get(list_id, 0)`. Empty dict means "no rows" (a
        valid batched result for a user with no items) — NOT an error
        indicator.

    Raises:
        Exception: re-raises any non-JWT-expired error so the caller's
            `counts=None` fallback path is reachable (Reviews MUST-FIX 1).
            The previous design swallowed all errors to `{}` which broke
            the fallback semantics — `counts={}` reads as "0 items for
            every list" instead of "per-list legacy fetch needed."
    """
    def _execute() -> Dict[int, int]:
        client = get_user_client()
        response = client.rpc('get_list_item_counts_for_user', {}).execute()
        rows = response.data or []
        counts: Dict[int, int] = {}
        for row in rows:
            if 'list_id' not in row:
                continue
            counts[int(row['list_id'])] = int(row.get('item_count') or 0)
        return counts

    try:
        return _execute()
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_list_item_counts, refreshing session and retrying")
            if _refresh_user_session():
                return _execute()
        logger.error("Error getting list item counts: %s", e)
        raise


def get_user_lists(user_id: str, include_deleted: bool = False) -> List[Dict]:
    """Get all lists for a user.

    Args:
        user_id: The user's UUID
        include_deleted: If True, include soft-deleted lists (for trash view)
    """
    try:
        # READER-01 (Phase 92.1) BUG-FIX ROOT: user_lists SELECT RLS is `TO authenticated`;
        # anon singleton returns 0 rows. Smoke run 1 Symptom 1 -- 2026-05-17.
        client = get_user_client()
        query = client.table('user_lists').select('*').eq('user_id', user_id)
        if not include_deleted:
            query = query.is_('deleted_at', 'null')
        _t = time.perf_counter()
        response = query.order('created_at').execute()
        _inst_record_query((time.perf_counter() - _t) * 1000.0)  # Phase 92.2 D-VER-01
        return response.data or []
    except Exception as e:
        # Fallback if deleted_at column doesn't exist yet
        if 'deleted_at' in str(e):
            try:
                # READER-01 (Phase 92.1): same migration as main path -- fallback when
                # deleted_at column doesn't exist yet.
                client = get_user_client()
                response = client.table('user_lists').select('*').eq('user_id', user_id).order('created_at').execute()
                return response.data or []
            except Exception as e2:
                logger.error(f"Error getting lists (fallback): {e2}")
                return []
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_user_lists, refreshing session and retrying")
            _refresh_user_session()
            try:
                return get_user_lists(user_id=user_id, include_deleted=include_deleted)
            except Exception as e2:
                logger.error(f"Error getting lists (retry): {e2}")
                return []
        logger.error(f"Error getting lists: {e}")
        return []


def get_deleted_lists(user_id: str) -> List[Dict]:
    """Get soft-deleted lists for a user (trash view)."""
    try:
        # READER-01 (Phase 92.1): same RLS policy as user_lists; trash view broken when anon.
        client = get_user_client()
        response = client.table('user_lists').select('*').eq('user_id', user_id).not_.is_('deleted_at', 'null').order('deleted_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        # Return empty if deleted_at column doesn't exist yet
        if 'deleted_at' in str(e):
            return []  # No trash feature until migration is run
        logger.error(f"Error getting deleted lists: {e}")
        return []


def create_list(user_id: str, name: str, name_en: str = None, color: str = '#FFD700',
                project_id: int = None, is_default: bool = False) -> Dict:
    """Create a new list."""
    try:
        client = get_user_client()
        data = {
            'user_id': user_id,
            'name': name,
            'name_en': name_en or name,
            'color': color,
            'is_default': is_default,
            'is_system': False
        }
        if project_id:
            data['project_id'] = project_id

        response = client.table('user_lists').insert(data).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Failed to create list'}
    except Exception as e:
        return {'error': str(e)}


def update_list(list_id: int, data: Dict) -> Dict:
    """Update a list."""
    try:
        client = get_user_client()
        response = client.table('user_lists').update(data).eq('id', list_id).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_list(list_id: int, permanent: bool = False) -> Dict:
    """Soft-delete a list (move to trash).

    Args:
        list_id: The list ID
        permanent: If True, permanently delete (no recovery). Default is soft delete.
    """
    try:
        client = get_user_client()
        if permanent:
            # Permanent delete - also deletes items via CASCADE
            client.table('user_lists').delete().eq('id', list_id).execute()
        else:
            # Soft delete - set deleted_at timestamp
            from datetime import datetime, timezone
            try:
                client.table('user_lists').update({
                    'deleted_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', list_id).execute()
            except Exception as soft_err:
                # Fallback to hard delete if deleted_at column doesn't exist
                if 'deleted_at' in str(soft_err):
                    client.table('user_lists').delete().eq('id', list_id).execute()
                else:
                    raise soft_err
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def restore_list(list_id: int) -> Dict:
    """Restore a soft-deleted list from trash."""
    try:
        client = get_user_client()
        response = client.table('user_lists').update({
            'deleted_at': None
        }).eq('id', list_id).execute()
        if response.data:
            return {'success': True, 'list': response.data[0]}
        return {'error': 'Restore failed'}
    except Exception as e:
        # If deleted_at column doesn't exist, trash feature is not available
        if 'deleted_at' in str(e):
            return {'error': 'Trash feature not available - run migration first'}
        return {'error': str(e)}


def empty_trash(user_id: str) -> Dict:
    """Permanently delete all soft-deleted lists for a user."""
    try:
        client = get_user_client()
        # Get all deleted lists
        deleted = get_deleted_lists(user_id)
        count = len(deleted)
        # Permanently delete each one
        for lst in deleted:
            client.table('user_lists').delete().eq('id', lst['id']).execute()
        return {'success': True, 'deleted_count': count}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# LIST ITEMS OPERATIONS
# ============================================================================

def get_list_items(list_id: int) -> List[Dict]:
    """Get all items in a list."""
    try:
        # READER-01 (Phase 92.1): list_items SELECT RLS is `TO authenticated`; anon returns 0 rows.
        client = get_user_client()
        _t = time.perf_counter()
        response = client.table('list_items').select('*').eq('list_id', list_id).order('added_at', desc=True).execute()
        _inst_record_query((time.perf_counter() - _t) * 1000.0)  # Phase 92.2 D-VER-01
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting list items: {e}")
        return []


def add_list_item(list_id: int, sys_id: str, shelfmark: str = None, title: str = None,
                  fl_id: str = None, note: str = '', tags: List[str] = None) -> Dict:
    """Add an item to a list."""
    try:
        client = get_user_client()
        data = {
            'list_id': list_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'title': title,
            'fl_id': fl_id,
            'note': note or '',
            'tags': tags or []
        }
        response = client.table('list_items').insert(data).execute()
        if response.data:
            return {'success': True, 'item': response.data[0]}
        return {'error': 'Failed to add item'}
    except Exception as e:
        return {'error': str(e)}


def update_list_item(item_id: int, data: Dict) -> Dict:
    """Update a list item."""
    try:
        client = get_user_client()
        response = client.table('list_items').update(data).eq('id', item_id).execute()
        if response.data:
            return {'success': True, 'item': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_list_item(item_id: int) -> Dict:
    """Delete a list item."""
    try:
        client = get_user_client()
        client.table('list_items').delete().eq('id', item_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# RECENT ITEMS OPERATIONS
# ============================================================================

def get_recent_items(user_id: str, limit: int = 50) -> List[Dict]:
    """Get recent items for a user."""
    try:
        # READER-01 (Phase 92.1): recent_items is user-scoped `TO authenticated`.
        client = get_user_client()
        _t = time.perf_counter()
        response = client.table('recent_items').select('*').eq('user_id', user_id).order('viewed_at', desc=True).limit(limit).execute()
        _inst_record_query((time.perf_counter() - _t) * 1000.0)  # Phase 92.2 D-VER-01
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting recent items: {e}")
        return []


def add_recent_item(user_id: str, sys_id: str, shelfmark: str = None,
                    title: str = None, fl_id: str = None) -> Dict:
    """Add an item to recent history."""
    try:
        client = get_user_client()

        # Remove existing entry for this sys_id (to move it to top)
        client.table('recent_items').delete().eq('user_id', user_id).eq('sys_id', sys_id).execute()

        # Add new entry
        data = {
            'user_id': user_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'title': title,
            'fl_id': fl_id
        }
        response = client.table('recent_items').insert(data).execute()

        # Cleanup: keep only last 50 items
        all_recent = client.table('recent_items').select('id').eq('user_id', user_id).order('viewed_at', desc=True).execute()
        if all_recent.data and len(all_recent.data) > 50:
            ids_to_delete = [item['id'] for item in all_recent.data[50:]]
            if ids_to_delete:
                client.table('recent_items').delete().in_('id', ids_to_delete).execute()

        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# PROJECTS OPERATIONS
# ============================================================================

def get_projects(user_id: str) -> List[Dict]:
    """Get all projects for a user."""
    try:
        # READER-01 (Phase 92.1): projects SELECT RLS is `TO authenticated`; projects panel
        # was empty for logged-in users before this migration (same bug class as user_lists).
        client = get_user_client()
        _t = time.perf_counter()
        response = client.table('projects').select('*').eq('user_id', user_id).order('created_at').execute()
        _inst_record_query((time.perf_counter() - _t) * 1000.0)  # Phase 92.2 D-VER-01
        return response.data or []
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_projects, refreshing session and retrying")
            _refresh_user_session()
            try:
                # READER-01 (Phase 92.1): retry path -- after _refresh_user_session rotates
                # tokens, get_user_client reads the fresh tokens from safe_storage.
                client = get_user_client()
                return client.table('projects').select('*').eq('user_id', user_id).order('created_at').execute().data or []
            except Exception as e2:
                logger.error(f"Error getting projects (retry): {e2}")
                return []
        logger.error(f"Error getting projects: {e}")
        return []


def create_project(user_id: str, name: str, color: str = '#4CAF50') -> Dict:
    """Create a new project."""
    try:
        client = get_user_client()
        data = {
            'user_id': user_id,
            'name': name,
            'color': color
        }
        response = client.table('projects').insert(data).execute()
        if response.data:
            return {'success': True, 'project': response.data[0]}
        return {'error': 'Failed to create project'}
    except Exception as e:
        return {'error': str(e)}


def update_project(project_id: int, data: Dict) -> Dict:
    """Update a project."""
    try:
        client = get_user_client()
        response = client.table('projects').update(data).eq('id', project_id).execute()
        if response.data:
            return {'success': True, 'project': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


def delete_project(project_id: int) -> Dict:
    """Delete a project."""
    try:
        client = get_user_client()
        client.table('projects').delete().eq('id', project_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# CORRECTIONS OPERATIONS
# ============================================================================

def get_corrections(sys_id: str = None, author_id: str = None, status: str = None, ie_id: str = None) -> List[Dict]:
    """Get corrections with optional filters, including author profile data."""
    try:
        # READER-01 (Phase 92.1): corrections has dual RLS (`TO public` for approved AND
        # `TO authenticated` for own-any-status). Anon path missed user's own pending. When not
        # logged in, get_user_client falls back to anon singleton (`TO public` still works).
        client = get_user_client()
        # Fetch corrections (profile data is fetched separately below)
        query = client.table('corrections').select('*')

        if sys_id:
            query = query.eq('sys_id', sys_id)
        if author_id:
            query = query.eq('author_id', author_id)
        if status:
            query = query.eq('status', status)
        if ie_id:
            query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")

        response = query.order('created_at', desc=True).execute()
        corrections = response.data or []

        # Fetch profile data for authors
        if corrections:
            user_ids = set(c.get('author_id') for c in corrections if c.get('author_id'))
            if user_ids:
                profiles_response = client.table('profiles').select(
                    'id, full_name, username'
                ).in_('id', list(user_ids)).execute()
                profiles_map = {p['id']: p for p in (profiles_response.data or [])}

                # Merge profile data into corrections
                for c in corrections:
                    aid = c.get('author_id')
                    if aid and aid in profiles_map:
                        c['profiles'] = profiles_map[aid]
                    else:
                        c['profiles'] = {}

        return corrections
    except Exception as e:
        logger.error(f"Error getting corrections: {e}")
        return []


def create_correction(author_id: str, sys_id: str, shelfmark: str, page_number: int,
                      original_text: str, corrected_text: str, notes: str = '',
                      status: str = 'pending', ie_id: str = None) -> Dict:
    """Create a new correction."""
    try:
        client = get_user_client()
        data = {
            'author_id': author_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'page_number': page_number,
            'original_text': original_text,
            'corrected_text': corrected_text,
            'notes': notes,
            'status': status
        }
        if ie_id:
            data['ie_id'] = ie_id
        response = client.table('corrections').insert(data).execute()
        if response.data:
            return {'success': True, 'correction': response.data[0]}
        return {'error': 'Failed to create correction'}
    except Exception as e:
        return {'error': str(e)}


def update_correction(correction_id: int, data: Dict) -> Dict:
    """Update a correction."""
    try:
        client = get_user_client()
        response = client.table('corrections').update(data).eq('id', correction_id).execute()
        if response.data:
            return {'success': True, 'correction': response.data[0]}
        return {'error': 'Update failed'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# COMMENTS OPERATIONS
# ============================================================================

def _enrich_with_profiles(client, rows: List[Dict], id_field: str = 'author_id') -> List[Dict]:
    """Batch-resolve user IDs to profile data and attach as 'profiles' key."""
    if not rows:
        return rows
    user_ids = set(r.get(id_field) for r in rows if r.get(id_field))
    if not user_ids:
        return rows
    try:
        profiles_response = client.table('profiles').select(
            'id, full_name, username'
        ).in_('id', list(user_ids)).execute()
        profiles_map = {p['id']: p for p in (profiles_response.data or [])}
    except Exception:
        profiles_map = {}  # Lookup failed; use empty dict
    for r in rows:
        aid = r.get(id_field)
        r['profiles'] = profiles_map.get(aid, {}) if aid else {}
    return rows


def get_comments(sys_id: str = None, author_id: str = None, is_public: bool = True, ie_id: str = None) -> List[Dict]:
    """Get comments with optional filters."""
    try:
        # READER-01 (Phase 92.1): comments has dual RLS (`TO public` for is_public=true,
        # `TO authenticated` for own private). Anonymous path missed user's own private comments.
        client = get_user_client()
        query = client.table('comments').select('*')

        if sys_id:
            query = query.eq('sys_id', sys_id)
        if author_id:
            query = query.eq('author_id', author_id)
        if is_public is not None:
            query = query.eq('is_public', is_public)
        if ie_id:
            query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")

        response = query.order('created_at', desc=True).execute()
        comments = response.data or []
        return _enrich_with_profiles(client, comments)
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_comments, refreshing session and retrying")
            _refresh_user_session()
            try:
                # READER-01 (Phase 92.1): retry path after refresh -- same dual-RLS rationale.
                client = get_user_client()
                query = client.table('comments').select('*')
                if sys_id:
                    query = query.eq('sys_id', sys_id)
                if author_id:
                    query = query.eq('author_id', author_id)
                if is_public is not None:
                    query = query.eq('is_public', is_public)
                if ie_id:
                    query = query.or_(f"ie_id.eq.{ie_id},ie_id.is.null")
                comments = query.order('created_at', desc=True).execute().data or []
                return _enrich_with_profiles(client, comments)
            except Exception as e2:
                logger.error(f"Error getting comments (retry): {e2}")
                return []
        logger.error(f"Error getting comments: {e}")
        return []


def create_comment(author_id: str, sys_id: str, content: str, shelfmark: str = None,
                   page_number: int = None, scope: str = 'page', is_public: bool = True,
                   parent_id: int = None, ie_id: str = None) -> Dict:
    """Create a new comment."""
    try:
        client = get_user_client()
        data = {
            'author_id': author_id,
            'sys_id': sys_id,
            'shelfmark': shelfmark,
            'page_number': page_number,
            'content': content,
            'scope': scope,
            'is_public': is_public,
            'parent_id': parent_id
        }
        if ie_id:
            data['ie_id'] = ie_id
        response = client.table('comments').insert(data).execute()
        if response.data:
            return {'success': True, 'comment': response.data[0]}
        return {'error': 'Failed to create comment'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERIES OPERATIONS
# ============================================================================

def get_discoveries(user_id: str = None, type: str = None, status: str = None) -> List[Dict]:
    """Get discoveries with optional filters."""
    try:
        # READER-01 (Phase 92.1): SELECT is `TO public` (is_hidden=false) but auth client
        # carries auth.uid() context for downstream votes / ownership checks.
        client = get_user_client()
        query = client.table('discoveries').select('*')

        if user_id:
            query = query.eq('user_id', user_id)
        if type:
            query = query.eq('type', type)
        if status:
            query = query.eq('status', status)

        response = query.order('created_at', desc=True).execute()
        return response.data or []
    except Exception as e:
        logger.error(f"Error getting discoveries: {e}")
        return []


def create_discovery(user_id: str, title: str, content: str, type: str = 'discovery',
                     document_id: str = None, shelfmark: str = None, page_number: int = None,
                     is_anonymous: bool = False, additional_shelfmarks: List[Dict] = None,
                     related_manuscripts: List[Dict] = None) -> Dict:
    """Create a new discovery.

    Note: The database schema uses a 'shelfmarks' JSONB array field.
    The document_id, shelfmark, page_number and additional entries are
    stored in this array.
    """
    try:
        client = get_user_client()

        # Build shelfmarks array from the various inputs
        shelfmarks = []

        # Add primary shelfmark/document if provided
        if document_id or shelfmark:
            primary = {}
            if document_id:
                primary['document_id'] = document_id
            if shelfmark:
                primary['shelfmark'] = shelfmark
            if page_number is not None:
                primary['page_number'] = page_number
            if primary:
                shelfmarks.append(primary)

        # Add additional shelfmarks
        if additional_shelfmarks:
            shelfmarks.extend(additional_shelfmarks)

        # Add related manuscripts
        if related_manuscripts:
            shelfmarks.extend(related_manuscripts)

        data = {
            'user_id': user_id,
            'title': title,
            'content': content,
            'type': type,
            'is_anonymous': is_anonymous,
            'shelfmarks': shelfmarks
        }

        response = client.table('discoveries').insert(data).execute()
        if response.data:
            return {'success': True, 'discovery': response.data[0]}
        return {'error': 'Failed to create discovery'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# FRAGMENT JOINS OPERATIONS
# ============================================================================

def get_fragment_joins(user_id: str = None, fragment_sys_id: str = None,
                       status: str = None) -> List[Dict]:
    """Get fragment joins with optional filters."""
    try:
        # Use authenticated client when available — RLS only shows
        # proposed joins to their creator (status='confirmed' OR auth.uid()=user_id)
        try:
            client = get_user_client()
        except Exception:
            # Phase 92.1 KEEP get_client(): legitimate Exception fallback ONLY when
            # get_user_client itself raised. fragment_joins SELECT is `TO public` so
            # the anon singleton still returns rows. Pattern verified correct.
            client = get_client()  # Operation failed; use fallback value
        query = client.table('fragment_joins').select('*')

        if user_id:
            query = query.eq('user_id', user_id)
        if fragment_sys_id:
            query = query.or_(f'fragment_a_sys_id.eq.{fragment_sys_id},fragment_b_sys_id.eq.{fragment_sys_id}')
        if status:
            query = query.eq('status', status)

        response = query.order('created_at', desc=True).execute()
        rows = response.data or []

        # Batch-resolve user_ids to display names via profiles table
        uid_set = {r.get('user_id') for r in rows if r.get('user_id')}
        profiles_map = {}
        if uid_set:
            try:
                profiles_resp = client.table('profiles').select('id, full_name, username').in_('id', list(uid_set)).execute()
                for p in (profiles_resp.data or []):
                    profiles_map[p['id']] = p.get('full_name') or p.get('username') or ''
            except Exception:
                pass  # Render/display failed; continue
        for row in rows:
            row['created_by_username'] = profiles_map.get(row.get('user_id'), '')
        return rows
    except Exception as e:
        if _is_jwt_expired(e):
            logger.warning("JWT expired in get_fragment_joins, refreshing session and retrying")
            _refresh_user_session()
            try:
                return get_fragment_joins(user_id=user_id, fragment_sys_id=fragment_sys_id, status=status)
            except Exception as e2:
                logger.error(f"Error getting joins (retry): {e2}")
                return []
        logger.error(f"Error getting joins: {e}")
        return []


def create_fragment_join(user_id: str, fragment_a_sys_id: str, fragment_a_shelfmark: str,
                         fragment_b_sys_id: str, fragment_b_shelfmark: str,
                         join_type: str = 'uncertain', confidence: str = 'possible',
                         notes: str = '', evidence: str = '') -> Dict:
    """Create a new fragment join."""
    try:
        client = get_user_client()
        # Map UI join type values to DB CHECK constraint values
        # DB allows: 'physical', 'content', 'uncertain'
        join_type_map = {
            'physical_join': 'physical',
            'same_composition': 'content',
        }
        db_join_type = join_type_map.get(join_type, join_type) or 'uncertain'
        data = {
            'user_id': user_id,
            'fragment_a_sys_id': fragment_a_sys_id,
            'fragment_a_shelfmark': fragment_a_shelfmark,
            'fragment_b_sys_id': fragment_b_sys_id,
            'fragment_b_shelfmark': fragment_b_shelfmark,
            'join_type': db_join_type,
            'confidence': confidence,
            'notes': notes,
            'evidence': evidence
        }
        response = client.table('fragment_joins').insert(data).execute()
        if response.data:
            return {'success': True, 'join': response.data[0]}
        return {'error': 'Failed to create join'}
    except Exception as e:
        return {'error': str(e)}


def delete_fragment_join(join_id: int) -> Dict:
    """Delete a fragment join."""
    try:
        client = get_user_client()
        client.table('fragment_joins').delete().eq('id', join_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DELETE OPERATIONS
# ============================================================================

def delete_comment(comment_id: int) -> Dict:
    """Delete a comment."""
    try:
        client = get_user_client()
        client.table('comments').delete().eq('id', comment_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def delete_correction(correction_id: int) -> Dict:
    """Delete a correction."""
    try:
        client = get_user_client()
        client.table('corrections').delete().eq('id', correction_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def delete_discovery(discovery_id: int) -> Dict:
    """Delete a discovery."""
    try:
        client = get_user_client()
        client.table('discoveries').delete().eq('id', discovery_id).execute()
        return {'success': True}
    except Exception as e:
        return {'error': str(e)}


def update_discovery(discovery_id: int, data: Dict) -> Dict:
    """Update a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update(data).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True, 'discovery': response.data[0]}
        return {'error': 'Failed to update discovery'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERY RESPONSES
# ============================================================================

def get_discovery_responses(discovery_id: int) -> List[Dict]:
    """Get responses for a discovery."""
    try:
        # READER-01 (Phase 92.1): consistency with companion writer create_discovery_response
        # (which already uses get_user_client). Reader filters only discovery_id (no per-user
        # scoping), so anon fallback is preserved if RLS is TO public; if RLS is TO authenticated
        # the migration ensures auth.uid() is bound. Per Reviews M2 -- rationale tightened.
        client = get_user_client()
        response = client.table('discovery_responses').select('*').eq(
            'discovery_id', discovery_id
        ).order('created_at', desc=False).execute()
        rows = response.data or []
        return _enrich_with_profiles(client, rows, id_field='user_id')
    except Exception as e:
        logger.error(f"Error getting discovery responses: {e}")
        return []


def create_discovery_response(discovery_id: int, user_id: str, content: str,
                               is_anonymous: bool = False) -> Dict:
    """Create a response to a discovery."""
    try:
        client = get_user_client()
        data = {
            'discovery_id': discovery_id,
            'user_id': user_id,
            'content': content,
            'is_anonymous': is_anonymous
        }
        response = client.table('discovery_responses').insert(data).execute()
        if response.data:
            return {'success': True, 'response': response.data[0]}
        return {'error': 'Failed to create response'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# DISCOVERY VOTING AND STATUS
# ============================================================================

def vote_discovery(discovery_id: int, user_id: str, vote_type: str) -> Dict:
    """Vote on a discovery (up or down)."""
    try:
        client = get_user_client()
        # Check if user already voted
        existing = client.table('discovery_votes').select('*').eq(
            'discovery_id', discovery_id
        ).eq('user_id', user_id).execute()

        if existing.data:
            # Update existing vote
            vote = existing.data[0]
            if vote.get('vote_type') == vote_type:
                # Remove vote if same type
                client.table('discovery_votes').delete().eq('id', vote['id']).execute()
                return {'success': True, 'action': 'removed'}
            else:
                # Change vote
                client.table('discovery_votes').update({'vote_type': vote_type}).eq(
                    'id', vote['id']
                ).execute()
                return {'success': True, 'action': 'changed'}
        else:
            # Create new vote
            client.table('discovery_votes').insert({
                'discovery_id': discovery_id,
                'user_id': user_id,
                'vote_type': vote_type
            }).execute()
            return {'success': True, 'action': 'created'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_answered(discovery_id: int, answered: bool) -> Dict:
    """Toggle answered status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_answered': answered
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_pin(discovery_id: int, pinned: bool) -> Dict:
    """Toggle pinned status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_pinned': pinned
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


def toggle_discovery_hidden(discovery_id: int, hidden: bool) -> Dict:
    """Toggle hidden status of a discovery."""
    try:
        client = get_user_client()
        response = client.table('discoveries').update({
            'is_hidden': hidden
        }).eq('id', discovery_id).execute()
        if response.data:
            return {'success': True}
        return {'error': 'Failed to update'}
    except Exception as e:
        return {'error': str(e)}


# ============================================================================
# FEED OPERATIONS
# ============================================================================

def get_feed_items(item_type: str = None, period: str = None,
                   limit: int = 50, offset: int = 0, include_hidden: bool = False) -> Dict:
    """Get activity feed items combining discoveries, corrections, comments, and joins."""
    try:
        # Phase 92.1 KEEP get_client() (Reviews R2-1 Option C, 2026-05-17): RLS evidence
        # verified at docs/guides/SUPABASE_GUIDE.md:498-502 + scripts/fix_rls_policies.sql:60-95
        # -- the ONLY SELECT policy on `discoveries` is `TO public USING (is_hidden = false)`;
        # admin policies exist for UPDATE/DELETE only, NOT for SELECT. Hidden rows are
        # invisible to BOTH anon and authenticated client roles under RLS. The Python-side
        # `.eq('is_hidden', False)` filter at line 1587 is SKIPPED when `include_hidden=True`
        # (callers: web/pages/discoveries.py:423 + :588-593 pass `include_hidden=is_admin`),
        # but the database still filters server-side -- so get_client() and get_user_client()
        # return IDENTICAL rows for this query regardless of include_hidden. Migration would
        # NOT surface hidden rows; it would only add per-request Client build cost. The
        # corrections branch is `.eq('status', 'approved')` (line 1622) -- the TO public
        # USING (status='approved') branch handles both roles identically. The comments
        # branch is `.eq('is_public', True)` (line 1644) -- the TO public USING (is_public=true)
        # branch ditto. MUST migrate to get_user_client if a `TO authenticated USING (role=
        # 'admin')` SELECT policy is ever added to discoveries (then admin-visible hidden
        # rows would actually appear in this query when called with include_hidden=True),
        # OR if any branch is extended to return user-private rows (e.g., user's own pending
        # corrections in the feed).
        client = get_client()
        items = []

        # Get discoveries
        if not item_type or item_type in ('discovery', 'question', 'identification', 'note'):
            try:
                query = client.table('discoveries').select('*')
                if not include_hidden:
                    query = query.eq('is_hidden', False)
                if item_type:
                    query = query.eq('type', item_type)
                discoveries = query.order('created_at', desc=True).limit(limit).execute()
                for d in (discoveries.data or []):
                    items.append({
                        'id': f"{d.get('type', 'discovery')}_{d.get('id')}",
                        'item_type': d.get('type', 'discovery'),
                        'title': d.get('title', ''),
                        'content_preview': d.get('content', '')[:500] if d.get('content') else '',
                        'document_id': d.get('document_id'),
                        'shelfmark': d.get('shelfmark'),
                        'page_number': d.get('page_number'),
                        'created_at': d.get('created_at'),
                        'is_pinned': d.get('is_pinned', False),
                        'is_featured': d.get('is_featured', False),
                        'is_answered': d.get('is_answered', False),
                        'is_hidden': d.get('is_hidden', False),
                        'upvotes': d.get('upvotes', 0),
                        'downvotes': d.get('downvotes', 0),
                        'response_count': d.get('response_count', 0),
                        'additional_shelfmarks': d.get('additional_shelfmarks'),
                        'related_manuscripts': d.get('related_manuscripts'),
                        'author': {
                            'id': d.get('user_id'),
                            'is_anonymous': d.get('is_anonymous', False)
                        }
                    })
            except Exception as e:
                logger.error(f"Error loading discoveries: {e}")

        # Get corrections
        if not item_type or item_type == 'correction':
            try:
                corrections = client.table('corrections').select('*').eq(
                    'status', 'approved'
                ).order('created_at', desc=True).limit(limit).execute()
                for c in (corrections.data or []):
                    items.append({
                        'id': f"correction_{c.get('id')}",
                        'item_type': 'correction',
                        'title': '',
                        'original_text': c.get('original_text', ''),
                        'corrected_text': c.get('corrected_text', ''),
                        'document_id': c.get('sys_id'),
                        'shelfmark': c.get('shelfmark'),
                        'page_number': c.get('page_number'),
                        'created_at': c.get('created_at'),
                        'author': {'id': c.get('author_id')}  # corrections uses author_id
                    })
            except Exception as e:
                logger.error(f"Error loading corrections: {e}")

        # Get comments
        if not item_type or item_type == 'comment':
            try:
                comments = client.table('comments').select('*').eq(
                    'is_public', True
                ).order('created_at', desc=True).limit(limit).execute()
                for c in (comments.data or []):
                    items.append({
                        'id': f"comment_{c.get('id')}",
                        'item_type': 'comment',
                        'title': '',
                        'content_preview': c.get('content', '')[:500] if c.get('content') else '',
                        'document_id': c.get('sys_id'),
                        'shelfmark': c.get('shelfmark'),
                        'page_number': c.get('page_number'),
                        'created_at': c.get('created_at'),
                        'author': {'id': c.get('author_id')}  # comments uses author_id
                    })
            except Exception as e:
                logger.error(f"Error loading comments: {e}")

        # Get joins
        if not item_type or item_type == 'join':
            try:
                joins = client.table('fragment_joins').select('*').order(
                    'created_at', desc=True
                ).limit(limit).execute()
                for j in (joins.data or []):
                    # Normalize column names for consumers (author resolved in second pass below)
                    normalized_join = {
                        'id': j.get('id'),
                        'fragment_a': j.get('fragment_a_shelfmark', ''),
                        'fragment_b': j.get('fragment_b_shelfmark', ''),
                        'document_id_a': j.get('fragment_a_sys_id'),
                        'document_id_b': j.get('fragment_b_sys_id'),
                        'relationship_type': j.get('join_type'),
                        'notes': j.get('notes', ''),
                        'created_by_username': '',  # Resolved in second-pass profile lookup
                        'created_at': j.get('created_at'),
                    }
                    items.append({
                        'id': f"join_{j.get('id')}",
                        'item_type': 'join',
                        'title': f"{j.get('fragment_a_shelfmark', '')} ↔ {j.get('fragment_b_shelfmark', '')}",
                        'content_preview': j.get('notes', ''),
                        'document_id': j.get('fragment_a_sys_id'),
                        'shelfmark': j.get('fragment_a_shelfmark'),
                        'created_at': j.get('created_at'),
                        'cluster_fragments': [j.get('fragment_a_shelfmark'), j.get('fragment_b_shelfmark')],
                        'cluster_joins': [normalized_join],
                        'author': {'id': j.get('user_id')}
                    })
            except Exception as e:
                logger.error(f"Error loading joins: {e}")

        # Get published puzzle joins
        if not item_type or item_type == 'puzzle_join':
            try:
                pj_resp = client.table('published_joins').select(
                    'id, user_id, title, notes, shelfmarks, thumbnail_path, created_at'
                ).eq('is_published', True).order('created_at', desc=True).limit(limit).execute()
                for pj in (pj_resp.data or []):
                    thumb_url = ''
                    if pj.get('thumbnail_path'):
                        thumb_url = client.storage.from_('puzzle-images').get_public_url(pj['thumbnail_path'])
                    items.append({
                        'id': f"puzzle_join_{pj['id']}",
                        'item_type': 'puzzle_join',
                        'title': pj.get('title', ''),
                        'content_preview': pj.get('notes', '')[:500] if pj.get('notes') else '',
                        'shelfmarks': pj.get('shelfmarks', []),
                        'thumbnail_url': thumb_url,
                        'created_at': pj.get('created_at'),
                        'author': {'id': pj.get('user_id')},
                    })
            except Exception as e:
                logger.error(f"Error loading published puzzle joins: {e}")

        # Sort by created_at descending
        items.sort(key=lambda x: x.get('created_at', '') or '', reverse=True)

        # Apply offset and limit
        total = len(items)
        items = items[offset:offset + limit]

        # Fetch profile data for authors (to get full_name and username)
        try:
            user_ids = set()
            for item in items:
                author_id = item.get('author', {}).get('id')
                if author_id:
                    user_ids.add(author_id)

            if user_ids:
                profiles_response = client.table('profiles').select('id, full_name, username, affiliation').in_('id', list(user_ids)).execute()
                profiles_map = {p['id']: p for p in (profiles_response.data or [])}

                # Merge profile data into author dicts and cluster_joins
                for item in items:
                    author = item.get('author', {})
                    author_id = author.get('id')
                    if author_id and author_id in profiles_map:
                        profile = profiles_map[author_id]
                        author['full_name'] = profile.get('full_name')
                        author['username'] = profile.get('username')
                        author['affiliation'] = profile.get('affiliation')
                        # Propagate into cluster_joins for join feed items
                        display_name = profile.get('full_name') or profile.get('username') or ''
                        for cj in item.get('cluster_joins', []):
                            if isinstance(cj, dict) and not cj.get('created_by_username'):
                                cj['created_by_username'] = display_name
        except Exception as e:
            logger.error(f"Error fetching profiles for feed: {e}")

        return {'items': items, 'total': total}
    except Exception as e:
        logger.exception(f"Error getting feed items: {e}")
        return {'items': [], 'total': 0, 'error': str(e)}
