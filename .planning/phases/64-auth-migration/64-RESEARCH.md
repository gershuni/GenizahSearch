# Phase 64: Auth Migration - Research

**Researched:** 2026-04-14
**Domain:** Supabase authentication (gotrue -> supabase_auth, implicit -> PKCE)
**Confidence:** HIGH

## Summary

This phase migrates GenizahSearch's Supabase authentication from the deprecated `gotrue` package to the current `supabase_auth` package, and switches the OAuth flow from implicit to PKCE. The migration is well-scoped: exactly 2 source files need import changes, 1 line needs flow_type changed, the OAuth callback loses its implicit code path, and requirements.txt/requirements-lock.txt get updated.

A critical finding: the current `except AuthApiError` handlers import from `gotrue.errors`, but the supabase client (v2.28.0) actually raises `supabase_auth.errors.AuthApiError`. These are **distinct classes** (verified: cross-catch test fails). This means the current error handling is silently broken -- auth errors fall through to the generic `except Exception` handlers. The migration to `supabase_auth` imports is therefore a correctness fix, not just a cosmetic change.

**Primary recommendation:** Change 2 import statements, remove `flow_type='implicit'` (default is already `'pkce'`), delete the implicit flow JavaScript block from the callback handler, remove `gotrue` from requirements.txt, re-pin requirements-lock.txt. Manual testing per D-07 checklist.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Switch `flow_type` from `'implicit'` to `'pkce'` in `web/supabase_client.py:58`
- **D-02:** Remove the implicit flow (hash token) code path from the OAuth callback (`web/main.py:1378`). PKCE becomes the only path -- no fallback to implicit.
- **D-03:** The callback handler already has PKCE code exchange logic -- make it the primary (and only) path.
- **D-04:** Replace `from gotrue.errors import AuthApiError` with `from supabase_auth.errors import AuthApiError` in all source files (2 files: `web/supabase_client.py`, `supabase_corrections_client.py`).
- **D-05:** Remove `gotrue` from `requirements.txt` (it's bundled in `supabase` as `supabase_auth`). Re-pin `requirements-lock.txt` after removal.
- **D-06:** Claude's Discretion -- fix the import path at minimum. If the keyring/credential storage or session refresh logic looks fragile during implementation, flag it for external review but don't expand scope beyond the auth migration.
- **D-07:** Manual testing checklist (no automated auth tests).
- **D-08:** External AI review (Gemini + Codex) BEFORE merging.

### Claude's Discretion
- Whether to also audit/clean up the legacy compatibility shim in `web/auth_state.py` (`api_call()`, `get_api_base()`)
- Whether desktop credential storage warrants broader review (D-06)
- Exact cleanup of the implicit flow JavaScript in the callback page

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| BLDG-03 | Supabase auth migrated to the current supported API -- desktop login, web login, OAuth callback, token refresh preserved -- current pytest baseline green | Import migration (`gotrue` -> `supabase_auth`), flow_type change (implicit -> pkce), callback cleanup, requirements update. All verified as viable. |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Email/password auth (web) | API / Backend (Supabase) | Frontend Server (NiceGUI) | Supabase handles auth; NiceGUI stores session tokens |
| OAuth flow (web) | API / Backend (Supabase) | Browser (redirect) | Supabase generates OAuth URL; browser handles redirect; server exchanges code |
| Email/password auth (desktop) | API / Backend (Supabase) | Desktop Client (PyQt6) | Desktop calls Supabase directly; keyring stores credentials |
| Token refresh / session | API / Backend (Supabase) | Frontend Server + Desktop | Both apps store tokens and use `set_session` for refresh |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase | 2.28.0 | Supabase Python client | Already installed, provides auth + DB [VERIFIED: pip show] |
| supabase_auth | 2.28.0 | Auth module (bundled with supabase) | Replacement for deprecated gotrue [VERIFIED: import test] |

### Removed
| Library | Version | Purpose | Why Removed |
|---------|---------|---------|-------------|
| gotrue | 2.12.4 | Legacy auth package | Deprecated, emits DeprecationWarning, not required-by any other package [VERIFIED: pip show gotrue] |

**Version verification:**
- `supabase==2.28.0` -- installed, verified [VERIFIED: pip show]
- `supabase_auth==2.28.0` -- bundled with supabase, verified [VERIFIED: python import]
- `gotrue==2.12.4` -- installed but deprecated, zero reverse dependencies [VERIFIED: pip show]

## Architecture Patterns

### PKCE OAuth Flow (After Migration)

```
User clicks "Login with Google"
    |
    v
get_oauth_url('google', redirect_url)
    |
    v
[Singleton client: sign_in_with_oauth]
    |-- _get_url_for_provider generates code_verifier
    |-- Stores code_verifier in client._storage (in-memory dict)
    |-- Appends code_challenge + code_challenge_method to URL
    |
    v
User redirects to Google -> authenticates -> redirects to /auth/callback?code=XXX
    |
    v
auth_callback_route(code=XXX)
    |
    v
exchange_code_for_session({'auth_code': code})
    |-- Reads code_verifier from client._storage
    |-- Sends auth_code + code_verifier to Supabase /token endpoint
    |-- Returns AuthResponse with user + session
    |
    v
complete_login(user, profile, session)
    |-- Stores tokens in app.storage.user['auth_session']
    |-- Redirects to /
```

### Pattern 1: Import Migration
**What:** Replace deprecated gotrue imports with supabase_auth
**When to use:** Every file that imports from gotrue
**Example:**
```python
# BEFORE (deprecated, and silently broken for error catching!)
from gotrue.errors import AuthApiError

# AFTER
from supabase_auth.errors import AuthApiError
```
Source: Verified via Python import test -- `supabase_auth.errors.AuthApiError` is the class actually raised by supabase client v2.28.0 [VERIFIED: exception type test]

### Pattern 2: Flow Type Change
**What:** Remove explicit `flow_type='implicit'` (default is already `'pkce'`)
**When to use:** `web/supabase_client.py:get_client()`
**Example:**
```python
# BEFORE
_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY,
                        options=ClientOptions(flow_type='implicit'))

# AFTER (omit flow_type entirely -- default is 'pkce')
_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
```
Source: `ClientOptions` signature shows `flow_type: Literal['pkce', 'implicit'] = 'pkce'` [VERIFIED: inspect.signature]

### Pattern 3: Callback Cleanup
**What:** Remove implicit flow JavaScript block, keep only PKCE code path
**When to use:** `web/main.py:auth_callback_route()`
**Current structure:**
- Lines 1427-1442: PKCE path (code in query param) -- **KEEP, make primary**
- Lines 1444-1498: Implicit path (tokens in URL hash via JS) -- **DELETE**

### Anti-Patterns to Avoid
- **Catching wrong exception class:** The current code catches `gotrue.errors.AuthApiError` but the client raises `supabase_auth.errors.AuthApiError`. These are different classes -- the except block never triggers. [VERIFIED: cross-catch test]
- **Setting flow_type explicitly to pkce:** Not needed -- it's the default. Omitting it is cleaner and future-proof.
- **Keeping implicit fallback "just in case":** Per D-02, PKCE is the only path. A dead implicit path is confusing and untested.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| PKCE code_verifier | Manual generation/storage | supabase_auth built-in | Already handles generation, storage, and retrieval [VERIFIED: source inspection] |
| OAuth URL construction | Manual URL building | `client.auth.sign_in_with_oauth()` | Handles PKCE params, provider scoping automatically |
| Token refresh | Manual JWT decode + refresh | `client.auth.set_session()` | Already refreshes expired tokens when called |

## Common Pitfalls

### Pitfall 1: PKCE Code Verifier Concurrency
**What goes wrong:** Two users start OAuth simultaneously. The singleton client stores only one code_verifier in its in-memory storage. The first user's callback gets the second user's code_verifier and fails.
**Why it happens:** The singleton client uses `SyncMemoryStorage` (a plain dict), and `_get_url_for_provider` overwrites the stored code_verifier with a fixed key (`{storage_key}-code-verifier`).
**How to avoid:** For this phase, document as a known limitation. The user base is small and concurrent OAuth logins are rare. A proper fix would require per-session auth clients for the OAuth initiation step, which is out of scope.
**Warning signs:** Intermittent "invalid code_verifier" errors during OAuth callback in production logs.

### Pitfall 2: Supabase Dashboard Redirect URL Configuration
**What goes wrong:** After switching to PKCE, the OAuth callback URL must be configured in the Supabase dashboard to allow `?code=` query parameter redirects. If the dashboard has only the implicit flow URL, PKCE redirects may be rejected.
**Why it happens:** Supabase server-side validates the redirect URL against its allowlist.
**How to avoid:** Verify `https://genizahsearch.com/auth/callback` is in the Supabase dashboard's redirect URL allowlist. PKCE uses the same URL -- just with query params instead of hash fragments. [ASSUMED]
**Warning signs:** OAuth redirect fails with "redirect_uri mismatch" or similar.

### Pitfall 3: Forgetting to Update Tests That Reference 'gotrue'
**What goes wrong:** The test suite has 2 references to 'gotrue' in `test_offline_verification.py` (lines 447, 509) -- these are FORBIDDEN-import checks for service modules. After removing the gotrue package, the string 'gotrue' should remain in the forbidden list (it's still something service modules shouldn't import), but 'supabase_auth' should be added to the forbidden list as well.
**Why it happens:** Oversight -- focusing only on source files, not test fixtures.
**How to avoid:** Add `'supabase_auth'` to the forbidden package lists in test_offline_verification.py.
**Warning signs:** Tests pass but service modules could accidentally import supabase_auth without detection.

### Pitfall 4: AuthApiError Class Mismatch (Current Bug)
**What goes wrong:** The `except AuthApiError` blocks in both web and desktop auth code import from `gotrue.errors`, but supabase v2.28.0 raises `supabase_auth.errors.AuthApiError`. The except blocks never catch auth-specific errors.
**Why it happens:** The gotrue package was installed separately and its classes are distinct from supabase_auth's classes despite identical names.
**How to avoid:** This is the primary fix in D-04. After migration, error handling will work correctly.
**Warning signs:** Auth errors show generic "Login error: ..." messages instead of specific ones like "Invalid email or password."

## Code Examples

### File 1: web/supabase_client.py (3 changes)

```python
# Line 19: Import change
# BEFORE:
from gotrue.errors import AuthApiError
# AFTER:
from supabase_auth.errors import AuthApiError

# Line 57-58: Flow type change
# BEFORE:
_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY,
                        options=ClientOptions(flow_type='implicit'))
# AFTER:
_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
# (default flow_type is 'pkce', ClientOptions import can be removed if unused elsewhere)
```

### File 2: supabase_corrections_client.py (1 change)

```python
# Line 25: Import change
# BEFORE:
from gotrue.errors import AuthApiError
# AFTER:
from supabase_auth.errors import AuthApiError
```

### File 3: web/main.py callback (structural change)

```python
# Lines 1378-1508: Remove implicit flow block
# KEEP: lines 1378-1442 (PKCE code exchange path)
# DELETE: lines 1444-1498 (implicit flow JS + token extraction)
# Add: early redirect if no code param (no implicit fallback)
```

### File 4: requirements.txt

```
# REMOVE this line:
gotrue==2.12.4
```

### File 5: requirements-lock.txt

```bash
# Regenerate after removing gotrue:
pip freeze > requirements-lock.txt
# Or manually remove the gotrue==2.12.4 line
```

### File 6: tests/test_offline_verification.py (2 changes)

```python
# Line 447: Add supabase_auth to forbidden list
forbidden_supabase = ['supabase', 'postgrest', 'gotrue', 'realtime', 'supabase_auth']

# Line 509: Add supabase_auth to forbidden set
FORBIDDEN_PACKAGES = frozenset({
    'supabase', 'postgrest', 'gotrue', 'realtime', 'supabase_auth',
})
```

## Discretion Recommendations

### Legacy shim cleanup (auth_state.py)
**Recommendation: Leave as-is.** The `api_call()` and `get_api_base()` functions in `web/auth_state.py` are legacy compatibility shims. They are self-contained, clearly documented, and harmless. Cleaning them up adds risk and scope for zero functional benefit. They can be removed in a future hygiene phase.

### Desktop credential storage (D-06)
**Recommendation: Fix import only.** The desktop auth in `supabase_corrections_client.py` uses keyring for credential storage and `sign_in_with_password` for auth. The keyring pattern is standard and the auth flow is email/password only (no OAuth). The import fix (`gotrue.errors` -> `supabase_auth.errors`) is sufficient. The desktop client doesn't use `flow_type` or OAuth, so no other changes needed.

### Implicit flow JS cleanup
**Recommendation: Delete the entire implicit block (lines 1444-1498).** After the PKCE switch, the hash-based token extraction JavaScript is dead code. The callback should handle `code` param or redirect home. No gradual deprecation needed.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `gotrue` package | `supabase_auth` package | supabase-py v2.x | gotrue emits DeprecationWarning on import [VERIFIED] |
| Implicit OAuth flow | PKCE OAuth flow | Industry standard since ~2023 | PKCE is more secure (no tokens in URL fragment) [ASSUMED] |
| `flow_type='implicit'` default | `flow_type='pkce'` default | supabase-py ClientOptions | Default is already 'pkce' [VERIFIED: inspect.signature] |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Supabase dashboard redirect URL allowlist works for both implicit and PKCE flows with same URL | Pitfall 2 | OAuth callback would fail after migration -- easy to verify in dashboard before deploying |
| A2 | PKCE is more secure than implicit flow | State of the Art | No practical impact on migration (security improvement is a bonus, not a requirement) |

## Open Questions

1. **PKCE code_verifier concurrency with singleton client**
   - What we know: The singleton client stores one code_verifier in memory. Concurrent OAuth logins would overwrite it.
   - What's unclear: Whether this is a real problem given the user base size.
   - Recommendation: Document as known limitation. If issues arise in production, create per-session clients for OAuth initiation.

2. **Supabase dashboard redirect URL configuration**
   - What we know: The callback URL `https://genizahsearch.com/auth/callback` is already configured (it works with implicit flow).
   - What's unclear: Whether PKCE requires any additional dashboard configuration.
   - Recommendation: Verify in Supabase dashboard during manual testing (D-07 item 2).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (no specific version pinned) |
| Config file | tests/ directory |
| Quick run command | `pytest tests/ -x --timeout=30` |
| Full suite command | `pytest tests/` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BLDG-03 | Auth imports use supabase_auth, not gotrue | unit (static check) | `pytest tests/test_offline_verification.py -x` | Yes (update forbidden list) |
| BLDG-03 | Desktop login with email/password | manual-only | N/A (requires live Supabase) | N/A |
| BLDG-03 | Web login with email/password | manual-only | N/A (requires live Supabase) | N/A |
| BLDG-03 | Web OAuth callback (PKCE) | manual-only | N/A (requires live Supabase + Google) | N/A |
| BLDG-03 | Token refresh / session persistence | manual-only | N/A (requires live Supabase) | N/A |
| BLDG-03 | pytest baseline green | regression | `pytest tests/` | Yes (1072 tests) |

### Sampling Rate
- **Per task commit:** `pytest tests/ -x --timeout=30`
- **Per wave merge:** `pytest tests/`
- **Phase gate:** Full suite green + manual testing checklist (D-07)

### Wave 0 Gaps
None -- existing test infrastructure covers all automated checks. Manual testing per D-07.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes | Supabase Auth (managed service) -- no custom auth logic |
| V3 Session Management | yes | Supabase JWT tokens + NiceGUI storage / keyring |
| V4 Access Control | no | Unchanged by this phase |
| V5 Input Validation | no | No new user inputs |
| V6 Cryptography | no | PKCE uses Supabase's built-in S256 challenge |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Token in URL fragment (implicit flow) | Information Disclosure | Migrate to PKCE (tokens never in URL) |
| PKCE code interception | Spoofing | code_verifier + S256 challenge prevents replay |
| Concurrent OAuth code_verifier overwrite | Denial of Service | Singleton storage limitation -- low risk given user base |

## Sources

### Primary (HIGH confidence)
- Python runtime verification: import tests, exception class comparison, ClientOptions signature inspection -- all performed live on the project's installed packages
- Source code inspection: `supabase_auth._sync.gotrue_client` (exchange_code_for_session, sign_in_with_oauth, _get_url_for_provider)
- `pip show gotrue` -- confirmed zero reverse dependencies
- `pip show supabase` -- confirmed v2.28.0

### Secondary (MEDIUM confidence)
- Supabase PKCE flow behavior inferred from source code inspection of `_get_url_for_provider` and `exchange_code_for_session`

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all packages verified locally, versions confirmed
- Architecture: HIGH -- PKCE flow traced through actual source code
- Pitfalls: HIGH -- exception class mismatch verified empirically; concurrency issue identified from source inspection

**Research date:** 2026-04-14
**Valid until:** 2026-05-14 (stable -- auth package rarely changes within minor versions)
