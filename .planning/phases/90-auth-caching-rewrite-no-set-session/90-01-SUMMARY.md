---
phase: 90-auth-caching-rewrite-no-set-session
plan: 01
subsystem: web/auth
tags: [auth, supabase, refresh-tokens, set_session, pkce, oauth, revocation, security]
requires:
  - phase-87 (safe_storage chokepoint + _session_uuid primitive)
  - phase-88 (state separation by deletion, regression guards)
  - phase-89 (lists cache per-request factory)
provides:
  - request-scoped get_user_client() with proactive refresh + local-header mutation
  - _refresh_user_session() locked by persisted _session_uuid (refresh-token-burn-safe)
  - throwaway-client bootstrap for all 5 singleton auth-mutating helpers
    (sign_in, sign_up, set_session_from_url, exchange_code_for_session, get_oauth_url)
  - admin.sign_out("global") actual server-side revocation in sign_out()
  - clear_auth revoke-before-pop with finally-block local cleanup
  - change_password REST helper (bypasses GoTrue update_user)
  - get_persisted_session_uuid() in web/safe_storage.py (strict Optional[str] variant)
  - PKCE code verifier round-trip across get_oauth_url -> exchange_code_for_session
  - 6 mocked regression tests in tests/test_auth_revocation_and_headers.py
affects:
  - web/supabase_client.py — all auth path rewritten
  - web/auth_state.py — clear_auth reordered
  - web/pages/profile.py — password change via REST helper
  - web/safe_storage.py — get_persisted_session_uuid helper added
  - .planning/phase87_storage_allowlist.yaml — supabase_client.py entry self-eliminated (3 -> 2 files)
tech-stack:
  added:
    - supabase_auth.errors.AuthApiError (already in tree, now actively imported in helper)
    - direct httpx.put for change_password (bypasses GoTrue)
  patterns:
    - throwaway-client pattern (D-10) to avoid singleton event-listener leak
    - persisted-UUID-keyed refresh locks (D-06) with stale-snapshot short-circuit
    - local-header mutation via PostgREST.auth + functions.set_auth + storage.session.headers
    - revoke-before-pop atomic local cleanup (D-11b)
key-files:
  created:
    - tests/test_auth_revocation_and_headers.py (203 lines, 6 mocked tests)
  modified:
    - web/supabase_client.py (helpers added + 5 helpers rewritten + sign_out rewritten + refresh_session DELETED + 4 retry blocks updated)
    - web/auth_state.py (clear_auth reordered to revoke-before-pop)
    - web/pages/profile.py (password change uses change_password REST helper)
    - web/safe_storage.py (get_persisted_session_uuid added)
    - .planning/phase87_storage_allowlist.yaml (supabase_client.py entry removed; 3 -> 2 files)
decisions:
  - All five singleton auth-mutating helpers (sign_in, sign_up, set_session_from_url,
    exchange_code_for_session, get_oauth_url) use throwaway clients per D-10 — the
    plan-checker round expanded the original 3-helper set to 5 after catching sign_up
    + get_oauth_url as F3 leak vectors.
  - sign_out routes through throwaway.auth.admin.sign_out(jwt, "global") instead of
    the high-level auth.sign_out — Codex round-3 P1 verified the high-level path is
    a no-op when the throwaway has no local session (gotrue_client.py:789-793).
  - Standalone refresh_session() at lines 325-339 DELETED — zero non-self callers
    verified by grep; _refresh_user_session is the live replacement.
  - get_user_client() honors _refresh_user_session()'s return value (R3-M1) — on False
    short-circuits to anonymous singleton, never builds authenticated client with stale token.
  - _refresh_locks dict keyed by get_persisted_session_uuid() (returns Optional[str]),
    NOT get_session_uuid() (mints ephemeral UUIDs under prune race) — Codex M1.
  - get_persisted_session_uuid lives in web/safe_storage.py as a sibling to
    get_session_uuid; the distinction is "mint if absent" vs "return None if absent".
  - Dead-code 6 names (_client_cache, _session_locks, _locks_guard, _CLIENT_CACHE_TTL,
    _clear_stale_auth, _prune_session_client_cache) remain as unreferenced declarations
    awaiting Plan 90-02 atomic deletion (12 occurrences in file body, all in
    declarations + the now-unused helper bodies — zero references from active code).
  - clear_auth reorder pulls AUTHW-03 + AUTHW-04 forward from Phase 91 per Codex
    round-2 P1 — without this, T-90-03 repudiation (refresh tokens never revoked)
    would ship in production after D-10 makes the singleton anonymous-only.
  - change_password REST helper sends the FULL 4-header tetrad (apikey/Authorization/
    Content-Type/Accept) — Codex round-2 P2 caught that bypassing GoTrue loses the
    apikey instance-header merge so Supabase's gateway would reject the request.
  - PKCE verifier round-trip: get_oauth_url extracts the verifier from the throwaway's
    in-memory storage and persists via safe_user_set('oauth_code_verifier', v);
    exchange_code_for_session pops it and passes as explicit code_verifier= parameter.
metrics:
  duration_minutes: ~50
  tasks_completed: 5
  files_modified: 5 (4 source + 1 allowlist)
  files_created: 1 (test file)
  commits: 5
  tests_added: 6 (all passing)
  tests_total_at_boundary: 1917 passed, 18 skipped, 0 failures
  completed_date: 2026-05-15
---

# Phase 90 Plan 01: Auth Caching Rewrite (no set_session) Summary

Request-scoped auth path with proactive refresh + local-header mutation + provably-anonymous singleton + actual server-side revocation. Zero `client.auth.set_session()` calls in `get_user_client()`; all auth-mutating bootstrap helpers use throwaway clients.

## Helpers Added

### `web/supabase_client.py`

```python
def _apply_user_auth_to_client(client: Client, access_token: str) -> None
def _access_token_near_expiry(access_token: str, skew_sec: int = REFRESH_SKEW_SEC) -> bool
def _refresh_user_session(stale_refresh_token: Optional[str] = None) -> bool
def change_password(new_password: str) -> Dict
```

Module globals added:
```python
_refresh_locks: Dict[str, threading.Lock] = {}
_refresh_locks_guard = threading.Lock()
REFRESH_SKEW_SEC = 60
```

### `web/safe_storage.py`

```python
def get_persisted_session_uuid() -> Optional[str]
```

Strict variant of `get_session_uuid()` that refuses to mint or return an ephemeral UUID under prune race. Returns None when persistence is unavailable so `_refresh_user_session` can deterministically skip refresh rather than racing with an ephemeral UUID that defeats per-session serialization (Codex review round 1 M1).

## Functions Rewritten

| Function | Rationale |
|----------|-----------|
| `get_user_client()` | Request-scoped, no caching, proactive refresh + local-header mutation; safe_user_get replaces captured-handle `_app.storage.user`; R3-M1 short-circuit when refresh returns False; AUTHC-05 docstring cites gotrue_client.py:713 |
| `sign_in()` | Throwaway client (D-10) — singleton would inherit user's JWT via event-listener leak (Codex F3) |
| `sign_up()` | Throwaway client (D-10, plan-checker round catch) — fires SIGNED_IN under auto-confirm, same F3 vector |
| `get_oauth_url()` | Throwaway client + PKCE verifier extraction + safe_user_set persistence (D-10 + Codex H1) |
| `set_session_from_url()` | Throwaway client — the ONE legitimate `set_session` callsite (D-15 Class A allowlist) |
| `exchange_code_for_session()` | Throwaway client + safe_user_pop verifier round-trip (D-10 + Codex H1) |
| `sign_out()` | Throwaway + `auth.admin.sign_out(jwt, "global")` — actual revocation, bypasses GoTrue's no-op high-level path (Codex round-3 P1). Accepts `access_token: Optional[str] = None` parameter so clear_auth can revoke before popping local keys |

## Standalone `refresh_session()` Deletion

The dead-code standalone `refresh_session()` at lines 325-339 was DELETED in Task 3a Step 1.7 (plan-checker round catch). Zero non-self callers verified by `grep -rn "supabase_client\.refresh_session\b" web/ shared/` returning 0. The private `_refresh_user_session` helper added in Task 1 is the live replacement.

## PKCE Code Verifier Round-Trip (Codex H1)

`get_oauth_url` builds a throwaway client (with default `flow_type='pkce'` per SyncClientOptions). `auth.sign_in_with_oauth({'provider', 'options'})` generates a verifier and stashes it in `throwaway.auth._storage` at `{storage_key}-code-verifier`. We extract it via `storage.get_item(...)` and persist via `safe_user_set('oauth_code_verifier', verifier)`.

`exchange_code_for_session` pops via `safe_user_pop('oauth_code_verifier', None)` and passes as explicit `code_verifier=` parameter to GoTrue. Without this round-trip, the throwaway's in-memory storage is GC'd at function return and the callback exchange would fail with "Code verifier and code challenge do not match".

## Terminal Refresh-Failure Cleanup (Codex H3)

`_refresh_user_session` catches `AuthApiError` and inspects message substrings, `code` attribute, and HTTP `status == 400` to identify terminal refresh failures (consumed/invalid/expired refresh tokens). On terminal, pops `auth_session`, `auth_user`, `auth_profile` from storage so the UI stops believing the user is logged in. Transient errors (network, 5xx) fall through to the broad `except Exception` which does NOT clear auth keys — next proactive refresh retries.

## sign_out Docstring/Body Alignment (Codex M2)

The new `sign_out` docstring describes the admin-scoped global revocation path it actually implements. The body comment cites `gotrue_client.py:789-793` to document why we bypass the high-level `auth.sign_out()` (it reads `self.get_session()` first and skips the admin call when no local session exists — and the throwaway never has a local session since AUTHC-02 forbids set_session).

## Test Audit Outcome (Step 7)

`tests/test_version_selector_pending.py` audit confirmed no-op: `grep -c "set_session" tests/test_version_selector_pending.py` returned 0. The MEMORY.md historical note was stale. No retargeting needed. The 5 existing tests in the file still pass after the rewrite.

## Allowlist Count: 3 -> 2

`.planning/phase87_storage_allowlist.yaml` had 3 file entries (web/auth_state.py, web/main.py, web/supabase_client.py). The supabase_client.py entry (line ~95-114) was deleted because `get_user_client` now reads `auth_session` via `safe_user_get(...)` instead of the captured-handle `_app.storage.user` pattern. Phase 87 lint scanner (`tests/test_no_raw_storage_access.py` — 6 tests) stays GREEN.

## Dead-Code Names Awaiting Plan 90-02

These 6 names remain in `web/supabase_client.py` as unreferenced declarations:
- `_client_cache`
- `_session_locks`
- `_locks_guard`
- `_CLIENT_CACHE_TTL`
- `_clear_stale_auth` (function)
- `_prune_session_client_cache` (function)

`grep -c "_client_cache\|_session_locks\|_locks_guard\|_CLIENT_CACHE_TTL\|_clear_stale_auth\|_prune_session_client_cache" web/supabase_client.py` returns 12 (declarations + the now-unused functions internally referencing each other). Plan 90-02 deletes all 6 atomically in a single commit alongside the regression guards (D-15 Class A + Class B AST scanners).

## Final AST-Anchored Invariants

```
OK 1: no throwaway.auth.sign_out Call nodes
OK 2: 2 client.auth.* calls, all read-only (get_user, get_session)
OK 3: refresh_session deleted
OK 4: no dict-options create_client calls
OK 5: AUTHC-05 docstring present (gotrue_client.py:713 cited)
OK 6: admin.sign_out present
```

All Codex review round 1 H2 + Codex round 2 plan-checker BLOCKER 2 + R3-H1 fixes verified.

## Commits

| # | Hash | Task | Description |
|---|------|------|-------------|
| 1 | 093c575c | Task 1 | Add helpers + change_password + get_persisted_session_uuid |
| 2 | 9792462a | Task 2 | Rewrite get_user_client + 4 retry blocks + remove allowlist entry |
| 3 | 859d151e | Task 3a | Rewrite 5 auth helpers to throwaways + sign_out admin + DELETE refresh_session |
| 4 | bc9fbe36 | Task 3b | Reorder clear_auth + migrate profile.py change_password + test audit |
| 5 | ba36d20d | Task 4 | 6 mocked tests for sign_out admin / change_password / apply_user_auth / R3-M1 |

## Deviations from Plan

None — plan executed exactly as written. All 4 review rounds (Round 1 H1/H2/H3/M1/M2/M3, Round 2 BLOCKER 1/2 + WARNING 3/5 + NIT 6, Round 3 R3-H1/R3-M1/R3-M2/R3-M3, Round 4 W1/W2) had their fixes pre-baked into the plan and executed verbatim.

## Test Results at Plan Boundary

- Full pytest suite: **1917 passed, 18 skipped, 0 failures** in 228.91s
- Plan-boundary subset (4 files): **29 passed** in 3.99s
- New test file `tests/test_auth_revocation_and_headers.py`: **6 passed** in 2.05s
- Phase 87 lint scanner (`tests/test_no_raw_storage_access.py`): **6 passed** (allowlist 3 -> 2 verified)
- Ruff check: **All checks passed** on web/supabase_client.py, web/auth_state.py, web/pages/profile.py, web/safe_storage.py, tests/test_auth_revocation_and_headers.py

## Unexpected Behaviors Encountered

None substantive. One minor adjustment from plan template: in `_refresh_user_session`'s AuthApiError branch, the `code` attribute can be `None` (Optional[ErrorCode] per `inspect.signature(AuthApiError.__init__)`). The plan template used `(getattr(e, 'code', '') or '').lower()` which would crash if `code` is `None` (NoneType.lower()). Updated to `(getattr(e, 'code', '') or '').lower() if getattr(e, 'code', None) is not None else ''` for defensive None handling. This is a Rule 1 (auto-fix bug) inline fix — does not change observable behavior because the `code in {...}` check would have been falsy anyway when `code` is None; the explicit guard just prevents an AttributeError if a future Codex/Supabase update makes ErrorCode an object with `.lower()` semantics rather than a string.

## Threat Flags

None. All 5 high/medium threats in the plan's threat model (T-90-01 through T-90-05) have been MITIGATED by the implementations described above. No new threat surface introduced.

## Self-Check: PASSED

Files verified to exist:
- FOUND: tests/test_auth_revocation_and_headers.py
- FOUND: web/safe_storage.py (modified — get_persisted_session_uuid added)
- FOUND: web/supabase_client.py (modified — full auth path rewritten)
- FOUND: web/auth_state.py (modified — clear_auth reordered)
- FOUND: web/pages/profile.py (modified — change_password REST helper)
- FOUND: .planning/phase87_storage_allowlist.yaml (modified — entry removed)

Commits verified to exist:
- FOUND: 093c575c (Task 1)
- FOUND: 9792462a (Task 2)
- FOUND: 859d151e (Task 3a)
- FOUND: bc9fbe36 (Task 3b)
- FOUND: ba36d20d (Task 4)
