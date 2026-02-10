---
phase: quick-8
plan: 1
subsystem: auth
tags: [supabase, rls, multi-user, nicegui, per-user-client]

# Dependency graph
requires: []
provides:
  - "Per-user Supabase client (get_user_client) for multi-user write correctness"
  - "Session token storage in NiceGUI per-user storage"
  - "Improved desktop login error messages"
affects: [web-auth, web-corrections, web-comments, web-discoveries, web-joins, desktop-login]

# Tech tracking
tech-stack:
  added: []
  patterns: ["per-user Supabase client via app.storage.user session tokens"]

key-files:
  created: []
  modified:
    - web/supabase_client.py
    - web/auth_state.py
    - web/main.py
    - web/api.py
    - supabase_corrections_client.py

key-decisions:
  - "Read-only functions keep singleton client for efficiency; only write functions use per-user client"
  - "Session tokens stored in app.storage.user['auth_session'] dict (access_token + refresh_token)"
  - "get_user_client() falls back to singleton with warning if no session tokens found"

patterns-established:
  - "Per-user client pattern: write functions call get_user_client(), reads call get_client()"

# Metrics
duration: 7min
completed: 2026-02-10
---

# Quick Task 8: Fix Web Corrections Singleton Supabase Client

**Per-user Supabase client for all 28 write operations, session token storage across email/OAuth login paths, and specific desktop login error messages**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-10T12:21:44Z
- **Completed:** 2026-02-10T12:28:56Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Added `get_user_client()` that creates per-user Supabase client from session tokens stored in NiceGUI's `app.storage.user`, fixing the multi-user RLS bug where User B's corrections would fail because the singleton client was authenticated as User A
- Switched all 28 write functions (corrections, comments, discoveries, joins, lists, projects, votes, toggles) to use per-user client while keeping read-only functions on the efficient singleton
- Session tokens now stored during email login (do_login), OAuth PKCE flow, OAuth implicit flow, and API OAuth callback
- Session tokens cleared on logout
- Desktop login now shows specific error messages: "Invalid email or password", "Email not confirmed", "Too many login attempts", network errors, etc.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add per-user Supabase client and store session tokens** - `76e187c` (fix)
2. **Task 2: Improve desktop login error messages** - `88a652d` (fix)

## Files Created/Modified
- `web/supabase_client.py` - Added get_user_client(); switched 28 write functions from get_client() to get_user_client()
- `web/auth_state.py` - Store auth_session tokens in do_login(); clear in clear_auth()
- `web/main.py` - Store auth_session tokens in OAuth callback (PKCE + implicit flow)
- `web/api.py` - Store auth_session tokens in API OAuth callback endpoint
- `supabase_corrections_client.py` - Parse AuthApiError for specific desktop login error messages

## Decisions Made
- Read-only functions keep the singleton client (`get_client()`) for efficiency -- no RLS concern since reads use public policies or anon key
- Per-user client created fresh each call via `create_client()` + `set_session()` -- avoids session state leaking between users
- Fallback to singleton with print warning if no session tokens found (graceful degradation)
- Also converted project CRUD and restore_list/empty_trash functions (not in original plan) since they are also write operations subject to RLS (Rule 2 deviation)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Also converted project and trash write functions**
- **Found during:** Task 1 (auditing write functions)
- **Issue:** Plan listed 23 specific functions but missed create_project, update_project, delete_project, restore_list, empty_trash which are also write operations subject to RLS
- **Fix:** Also converted these 5 functions to use get_user_client()
- **Files modified:** web/supabase_client.py
- **Verification:** Audit script confirms all write functions (create_/update_/delete_/add_/vote_/toggle_/restore_/empty_) use get_user_client()
- **Committed in:** 76e187c (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for completeness -- leaving any write function on the singleton would leave the same multi-user bug unfixed for those operations.

## Issues Encountered
- Legacy test files (test_api_flow.py, test_corrections_api.py, test_corrections_integration.py) fail with `ModuleNotFoundError: No module named 'backend'` -- pre-existing issue from FastAPI removal in Jan 2026, not related to this change
- All 135 Responsa core/integration tests pass

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Multi-user web corrections now work correctly under RLS
- Desktop login provides actionable error messages
- No blockers

## Self-Check: PASSED

- All 5 modified files exist on disk
- Commit 76e187c (Task 1) found in git log
- Commit 88a652d (Task 2) found in git log
- get_user_client and get_client both importable
- All write functions confirmed using get_user_client()
- 135 existing tests pass

---
*Quick Task: 8-fix-web-corrections-singleton-supabase-c*
*Completed: 2026-02-10*
