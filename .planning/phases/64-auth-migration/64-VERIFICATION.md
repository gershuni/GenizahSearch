---
phase: 64-auth-migration
verified: 2026-04-14T00:00:00Z
status: human_needed
score: 11/12
overrides_applied: 0
human_verification:
  - test: "Web OAuth cancellation — click Login with Google, then cancel/deny on Google consent screen"
    expected: "Redirects to /auth/callback?error=access_denied, user sees 'Authentication failed: access_denied' error message (not a silent redirect home)"
    why_human: "Requires live Google OAuth + Supabase; localhost limitation confirmed in 64-02-SUMMARY.md (item 3 marked 'Not tested'). Code path is present and correct, but end-to-end flow not verified."
  - test: "Expired/used OAuth code handling — use browser back button after successful OAuth to replay the callback URL"
    expected: "Error message displayed, not a crash or silent redirect"
    why_human: "Requires live Supabase session; cannot stage the condition programmatically. Code has generic except block that calls show_error(), but token rejection path untested."
---

# Phase 64: Auth Migration — Verification Report

**Phase Goal:** Supabase authentication uses the current supported API with zero behavior change for users
**Verified:** 2026-04-14
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (merged: ROADMAP Success Criteria + Plan must-haves)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Desktop login with email/password works identically to before | ? HUMAN | Reported passing in 64-02-SUMMARY item 6; code path unchanged (supabase_corrections_client.py uses sign_in_with_password, no flow_type); import fix confirmed. Cannot auto-verify. |
| 2 | Web login with email/password and OAuth callback work identically | VERIFIED (partial) | Email/password: same code path, import fix only. OAuth PKCE callback: code confirmed in web/main.py lines 1379–1452. Reported passing in production (64-02-SUMMARY items 1–2). OAuth cancellation NOT tested (see human items). |
| 3 | Token refresh / session persistence works in both apps | ? HUMAN | Reported passing in 64-02-SUMMARY items 4, 7. Code stores session tokens in app.storage.user['auth_session'] — unchanged behavior. Cannot auto-verify persistence. |
| 4 | Current pytest baseline remains green after migration | VERIFIED | 64-02-SUMMARY confirms 1067 passed, 8 skipped. Matches pre-migration baseline. |
| 5 | requirements.txt re-pinned to reflect dependency changes | VERIFIED | gotrue removed from requirements.txt (13 packages confirmed); requirements-lock.txt updated (114 lines, no gotrue). |
| 6 | web/supabase_client.py imports AuthApiError from supabase_auth.errors | VERIFIED | Line 19: `from supabase_auth.errors import AuthApiError` confirmed. |
| 7 | supabase_corrections_client.py imports AuthApiError from supabase_auth.errors | VERIFIED | Line 25: `from supabase_auth.errors import AuthApiError` confirmed. |
| 8 | Supabase client uses PKCE flow (default), not implicit | VERIFIED | Line 57: `_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)` — no ClientOptions, no flow_type='implicit'. Default is 'pkce'. |
| 9 | OAuth callback handles only PKCE code exchange, no implicit flow path | VERIFIED | web/main.py lines 1379–1452: PKCE-only. Zero occurrences of "implicit", "Method 2", or `window.location.hash` in callback area. |
| 10 | OAuth callback displays error feedback when provider returns ?error= or ?error_description= | VERIFIED | Lines 1426–1430: `if error or error_description:` check before PKCE path, calls show_error(). Function signature includes both params. |
| 11 | The /api/auth/oauth-callback POST endpoint (implicit flow artifact) is removed | VERIFIED | grep of web/api.py for "oauth-callback" and "oauth_callback" returns no matches. |
| 12 | gotrue is no longer listed as a direct dependency | VERIFIED | requirements.txt has 13 packages, no gotrue line. requirements-lock.txt (114 lines) has no gotrue. |
| 13 | Service module forbidden-import tests also block supabase_auth imports | VERIFIED | tests/test_offline_verification.py line 447: `['supabase', 'postgrest', 'gotrue', 'realtime', 'supabase_auth']`; line 509: frozenset includes 'supabase_auth'. |
| 14 | No remaining gotrue references in any first-party Python file | VERIFIED | grep -r "gotrue" --include="*.py" returns only venv (third-party) and test_offline_verification.py forbidden list strings. Zero source file imports from gotrue. |

**Score:** 11/12 truths verified (12th item — OAuth cancellation + expired code — needs human; items 1, 3 reported by developer but require human confirmation)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/supabase_client.py` | Correct auth import + PKCE flow | VERIFIED | `from supabase_auth.errors import AuthApiError` at line 19; `create_client(SUPABASE_URL, SUPABASE_ANON_KEY)` at line 57; no ClientOptions, no gotrue |
| `supabase_corrections_client.py` | Correct auth import | VERIFIED | `from supabase_auth.errors import AuthApiError` at line 25 inside try/except ImportError guard |
| `web/main.py` | PKCE-only callback with error param handling | VERIFIED | Signature: `(code=None, error=None, error_description=None)`; error check at line 1426; PKCE path at 1432; no-params redirect at 1450 |
| `web/api.py` | No implicit-flow oauth-callback endpoint | VERIFIED | Zero matches for "oauth-callback" or "oauth_callback" in file |
| `requirements.txt` | No gotrue dependency (13 packages) | VERIFIED | 13 lines, gotrue absent |
| `requirements-lock.txt` | No gotrue in lock | VERIFIED | 114 lines, gotrue absent |
| `tests/test_offline_verification.py` | supabase_auth in both forbidden lists | VERIFIED | Lines 447 and 509 both contain 'supabase_auth' |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `web/supabase_client.py` | `supabase_auth.errors` | `from supabase_auth.errors import AuthApiError` | WIRED | Line 19 confirmed |
| `supabase_corrections_client.py` | `supabase_auth.errors` | `from supabase_auth.errors import AuthApiError` | WIRED | Line 25 confirmed |
| `web/main.py` | `auth_callback_route` | `error_description.*error` query params | WIRED | Signature and handler at lines 1379, 1426 |

### Data-Flow Trace (Level 4)

Not applicable — this phase modifies import paths and control flow, not data rendering components. No UI components render dynamic data that requires data-flow tracing.

### Behavioral Spot-Checks

| Behavior | Check | Result | Status |
|----------|-------|--------|--------|
| web/supabase_client.py has no gotrue | `grep gotrue web/supabase_client.py` | 0 matches | PASS |
| supabase_corrections_client.py has no gotrue | `grep gotrue supabase_corrections_client.py` | 0 matches | PASS |
| No ClientOptions in web client | `grep ClientOptions web/supabase_client.py` | 0 matches | PASS |
| No implicit flow in callback | `grep -c "implicit\|Method 2" web/main.py` | 0 | PASS |
| Error params in callback signature | `grep "error_description" web/main.py` | Line 1379, 1427 | PASS |
| Dead endpoint removed | `grep "oauth-callback" web/api.py` | 0 matches | PASS |
| gotrue not in requirements.txt | `grep gotrue requirements.txt` | 0 matches | PASS |
| supabase_auth in test forbidden lists | `grep supabase_auth tests/test_offline_verification.py` | Lines 447, 509 | PASS |
| requirements.txt has 13 packages | `wc -l requirements.txt` | 13 | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| BLDG-03 | 64-01-PLAN.md, 64-02-PLAN.md | Supabase auth migrated to current supported API; desktop login, web login, OAuth callback, token refresh preserved; pytest baseline green | SATISFIED (pending human for live auth flows) | All code changes verified; pytest baseline 1067/pass confirmed; 3 manual items (OAuth cancel, expired code, desktop persistence) require human confirmation |

No orphaned requirements — REQUIREMENTS.md maps only BLDG-03 to Phase 64.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found | — | — | — | — |

No TODOs, placeholders, empty implementations, or stub patterns found in modified files. The implicit flow removal was complete (no dead code left behind).

### Human Verification Required

#### 1. OAuth Cancellation Flow

**Test:** In a browser, navigate to https://genizahsearch.com, click "Login with Google", then on the Google consent screen click "Cancel" or deny permissions (or use browser back button).
**Expected:** The browser redirects to /auth/callback?error=access_denied (or similar), and the page displays "Authentication failed: access_denied" (or the provider's error text). User should NOT see a crash, an unhandled JS error, or a silent redirect to home without any message.
**Why human:** Requires live Google OAuth + Supabase on production. The code path at lines 1426–1430 is correct and the error display via show_error() is verified in code, but the end-to-end flow (including Supabase redirecting with ?error= when cancelled) was not tested due to localhost limitations — OAuth callback for PKCE requires same-process code_verifier, which means only production can be tested.

#### 2. Expired/Used OAuth Code Handling

**Test:** After a successful Google OAuth login, use browser history to navigate back to the /auth/callback?code=XXXX URL that was used during login (the code should now be expired/already consumed).
**Expected:** An error message is displayed (e.g., "Login failed" or the Supabase error for an invalid code), not a crash or silent redirect.
**Why human:** Requires a live Supabase session to trigger the code rejection response. The generic `except Exception` block at line 1454 does call show_error(), but the specific Supabase error response for a consumed code was not observed during testing.

### Gaps Summary

No blocking gaps found. All automated assertions pass. The two human verification items above are edge-case flows (OAuth cancellation and expired code replay) that require production testing. The core migration — import paths, flow type, callback handler, dependency removal — is fully verified.

**Note on 3 untested manual items from 64-02-SUMMARY.md:**
- Item 3 (OAuth cancellation): Promoted to human_needed above — code path is correct but end-to-end not confirmed.
- Item 9 (Expired code): Promoted to human_needed above.
- Item 10 (Direct /auth/callback no params): Code verified at line 1450–1452 (`ui.navigate.to('/')`); this is a simple redirect with no error handling needed — considered satisfied by code inspection.

---

_Verified: 2026-04-14_
_Verifier: Claude (gsd-verifier)_
