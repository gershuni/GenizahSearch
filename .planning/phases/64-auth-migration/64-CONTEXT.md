# Phase 64: Auth Migration - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Migrate Supabase authentication from the deprecated `gotrue` package to the current `supabase_auth` API, and switch OAuth flow from implicit to PKCE. Zero behavior change for users — login with email/password and Google OAuth must work identically before and after.

</domain>

<decisions>
## Implementation Decisions

### OAuth Flow (Web)
- **D-01:** Switch `flow_type` from `'implicit'` to `'pkce'` in `web/supabase_client.py:58`
- **D-02:** Remove the implicit flow (hash token) code path from the OAuth callback (`web/main.py:1378`). PKCE becomes the only path — no fallback to implicit.
- **D-03:** The callback handler already has PKCE code exchange logic — make it the primary (and only) path.

### Package Migration
- **D-04:** Replace `from gotrue.errors import AuthApiError` with `from supabase_auth.errors import AuthApiError` in all source files (2 files: `web/supabase_client.py`, `supabase_corrections_client.py`).
- **D-05:** Remove `gotrue` from `requirements.txt` (it's bundled in `supabase` as `supabase_auth`). Re-pin `requirements-lock.txt` after removal.

### Desktop Auth
- **D-06:** Claude's Discretion — fix the import path at minimum. If the keyring/credential storage or session refresh logic looks fragile during implementation, flag it for external review but don't expand scope beyond the auth migration.

### Testing
- **D-07:** Manual testing checklist (no automated auth tests). Checklist items:
  1. Web email/password login
  2. Web Google OAuth login (full redirect flow)
  3. Desktop email/password login
  4. Token refresh / session persistence across app restart (both apps)
  5. Logout works in both apps

### Review Process
- **D-08:** External AI review (Gemini + Codex) BEFORE merging — auth is a sensitive area and the user defers to external technical validation.

### Claude's Discretion
- Whether to also audit/clean up the legacy compatibility shim in `web/auth_state.py` (`api_call()`, `get_api_base()`)
- Whether desktop credential storage warrants broader review (D-06)
- Exact cleanup of the implicit flow JavaScript in the callback page

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Auth Code (primary targets)
- `web/supabase_client.py` — Singleton client, `flow_type='implicit'` (line 58), `gotrue.errors` import (line 19), all auth functions
- `supabase_corrections_client.py` — Desktop auth client, `gotrue.errors` import (line 25), `sign_in_with_password` (line 582)
- `web/auth_state.py` — Global auth state, login/register/logout flows, OAuth redirect, legacy shims
- `web/main.py` lines 1378-1470 — OAuth callback handler (both implicit and PKCE paths)

### Dependencies
- `requirements.txt` — Current deps including `gotrue==2.12.4` (line 10) and `supabase==2.28.0` (line 9)
- `requirements-lock.txt` — Full pinned deps (must regenerate after gotrue removal)

### CI (Phase 63 output)
- `.github/workflows/ci.yml` — CI safety net for regression detection

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `supabase_auth` package already installed (v2.28.0, bundled with `supabase`) — `from supabase_auth.errors import AuthApiError` confirmed working
- OAuth callback already has PKCE code exchange path (`exchange_code_for_session`) — just needs to become the only path
- `shared/supabase_provider.py` — centralized URL/key provider (no changes needed)

### Established Patterns
- Web uses NiceGUI `app.storage.user` for session tokens (access_token, refresh_token)
- Desktop uses `keyring` for credential storage + `sign_in_with_password` only
- Per-user client cache with TTL and thread-safe locks (`get_user_client()`)

### Integration Points
- `web/supabase_client.py:get_client()` — singleton creation point where `flow_type` is set
- `web/main.py:auth_callback_route()` — OAuth callback endpoint
- All pages that import from `web.supabase_client` or `web.auth_state` (browse, search, corrections, discoveries, profile, admin, lists, comments, joins)

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants plain-English discussion, technical depth deferred to external AI review
- "Not my expertise" — the external review (D-08) is important, not optional

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 64-auth-migration*
*Context gathered: 2026-04-14*
