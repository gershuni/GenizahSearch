---
phase: 64-auth-migration
plan: 01
subsystem: auth
tags: [auth, supabase, pkce, oauth, security]
dependency_graph:
  requires: []
  provides: [correct-auth-error-handling, pkce-oauth-flow]
  affects: [web/supabase_client.py, supabase_corrections_client.py, web/main.py, web/api.py]
tech_stack:
  added: []
  patterns: [pkce-oauth, supabase-auth-v2]
key_files:
  created: []
  modified:
    - web/supabase_client.py
    - supabase_corrections_client.py
    - web/main.py
    - web/api.py
decisions:
  - "Removed ClientOptions import entirely since it was only used for flow_type='implicit'"
  - "Added error/error_description as query params to callback signature for OAuth error feedback"
metrics:
  duration: 145s
  completed: "2026-04-14"
  tasks: 2
  files: 4
---

# Phase 64 Plan 01: Auth Import Migration & PKCE Flow Summary

**One-liner:** Fix broken AuthApiError catch blocks by migrating from gotrue to supabase_auth imports, switch OAuth from implicit to PKCE flow, remove dead implicit-flow artifacts.

## What Was Done

### Task 1: Migrate auth imports and flow type (dd4bdb84)
- Changed `from gotrue.errors import AuthApiError` to `from supabase_auth.errors import AuthApiError` in both `web/supabase_client.py` and `supabase_corrections_client.py`
- Removed `ClientOptions(flow_type='implicit')` from web client creation, enabling PKCE default
- Removed unused `ClientOptions` import

### Task 2: Remove implicit flow and dead endpoint (04193a68)
- Deleted the entire implicit flow JavaScript block (~55 lines) from `web/main.py` auth callback
- Added `error` and `error_description` query parameters to callback function signature
- Added explicit error param handling before PKCE code path (replaces error detection that was inside deleted JS block)
- Updated docstring to reflect PKCE-only flow
- Removed dead `/api/auth/oauth-callback` POST endpoint (~49 lines) from `web/api.py`
- Removed `set_session_from_url` import from both files (no longer needed)

## Threat Mitigations Applied

| Threat ID | Mitigation |
|-----------|------------|
| T-64-01 | Implicit flow removed -- tokens no longer exposed in URL fragments |
| T-64-02 | PKCE enabled via default flow_type -- code_verifier + S256 challenge active |
| T-64-04 | AuthApiError import fixed -- except blocks now catch the correct class |
| T-64-05 | Dead POST endpoint removed -- no more raw token acceptance via request body |

## Deviations from Plan

None - plan executed exactly as written.

## Verification Results

- Zero `gotrue` references in auth client files
- Two `supabase_auth.errors.AuthApiError` imports confirmed
- Zero `implicit` references in callback handler
- Zero `ClientOptions` references in web client
- `error_description` handling present in callback
- Dead `/api/auth/oauth-callback` endpoint removed from api.py
- `set_session_from_url` removed from both main.py and api.py (zero references)
- 1067 tests passed, 8 skipped (baseline maintained)

## Known Stubs

None.

## Self-Check: PASSED

All 4 modified files exist. Both task commits (dd4bdb84, 04193a68) verified in git log.
