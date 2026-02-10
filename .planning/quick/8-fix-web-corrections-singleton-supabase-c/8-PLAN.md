---
phase: quick-8
plan: 1
type: execute
wave: 1
depends_on: []
files_modified:
  - web/supabase_client.py
  - web/auth_state.py
  - web/pages/browse.py
  - web/pages/search.py
  - web/pages/discoveries.py
  - web/pages/corrections.py
  - web/components/comment_dialog.py
  - web/components/text_editor.py
  - web/components/joins_panel.py
  - web/main.py
  - supabase_corrections_client.py
autonomous: true
must_haves:
  truths:
    - "User A's corrections are submitted under User A's identity even when User B is concurrently logged in on the same NiceGUI server"
    - "All authenticated write operations (corrections, comments, discoveries, joins, votes) use per-user Supabase client"
    - "Desktop login shows specific error guidance for common failure cases"
  artifacts:
    - path: "web/supabase_client.py"
      provides: "get_user_client() function creating per-user Supabase client from stored tokens"
      contains: "get_user_client"
    - path: "web/auth_state.py"
      provides: "Session token storage in app.storage.user during login"
      contains: "access_token"
    - path: "supabase_corrections_client.py"
      provides: "Improved desktop login error messages"
      contains: "Invalid login credentials"
  key_links:
    - from: "web/auth_state.py"
      to: "web/supabase_client.py"
      via: "do_login stores tokens from sign_in response, get_user_client reads them"
      pattern: "app\\.storage\\.user.*access_token"
    - from: "web/pages/browse.py"
      to: "web/supabase_client.py"
      via: "handle_submit_correction calls create_correction with per-user client"
      pattern: "get_user_client"
---

<objective>
Fix the singleton Supabase client bug that causes web corrections and other authenticated writes to fail in multi-user scenarios, and improve desktop login error messages.

Purpose: The web app uses a singleton Supabase client. When multiple users are logged in simultaneously on the NiceGUI server, the singleton's auth session belongs to whichever user signed in last. RLS policies reject writes from other users because auth.uid() does not match the author_id. This causes silent failures for corrections, comments, discoveries, and joins.

Output: Per-user Supabase client for all authenticated writes; improved desktop login errors.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/supabase_client.py
@web/auth_state.py
@web/pages/browse.py
@web/main.py
@supabase_corrections_client.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add per-user Supabase client and store session tokens</name>
  <files>web/supabase_client.py, web/auth_state.py, web/main.py</files>
  <action>
  **In web/supabase_client.py:**

  1. Add a new function `get_user_client()` near the top (after `get_client()`):
     - Import `from nicegui import app` at the top of the file
     - The function reads `access_token` and `refresh_token` from `app.storage.user.get('auth_session', {})`
     - If tokens exist, creates a NEW `Client` via `create_client(SUPABASE_URL, SUPABASE_ANON_KEY)` and then calls `client.auth.set_session(access_token, refresh_token)` to authenticate it as the user
     - Returns the per-user client
     - If no tokens stored, falls back to `get_client()` (singleton) with a print warning
     - Wrap in try/except: if set_session fails (expired tokens), print warning and fall back to singleton

  2. Update ALL write functions that need RLS user identity to use per-user client. For each function below, replace `client = get_client()` with `client = get_user_client()`:
     - `create_correction()` (line ~662)
     - `update_correction()` (line ~682)
     - `create_comment()` (line ~718)
     - `create_discovery()` (line ~777)
     - `create_discovery_response()` (line ~943)
     - `vote_discovery()` (line ~966)
     - `create_fragment_join()` (line ~844)
     - `create_list()` (line ~360)
     - `update_list()` (line ~381)
     - `delete_list()` (line ~401)
     - `add_list_item()` (line ~470)
     - `update_list_item()` (line ~492)
     - `delete_list_item()` (line ~504)
     - `add_recent_item()` (line ~529)
     - `update_profile()` (line ~299)
     - `delete_comment()` (line ~884)
     - `delete_correction()` (line ~894)
     - `delete_discovery()` (line ~904)
     - `update_discovery()` (line ~914)
     - `delete_fragment_join()` (line ~870)
     - `toggle_discovery_answered()` (line ~999)
     - `toggle_discovery_pin()` (line ~1013)
     - `toggle_discovery_hidden()` (line ~1027)

  NOTE: Leave READ-ONLY functions using `get_client()` (singleton is fine for reads — `profiles`, `get_corrections`, `get_comments`, `get_feed_items`, etc. since these use anon key reads or public RLS policies).

  **In web/auth_state.py:**

  1. In `do_login()` (line ~126): After `result = supabase_sign_in(email, password)` and before the success check, extract session tokens from the result. The `sign_in()` function already returns `{'session': {'access_token': ..., 'refresh_token': ...}}` via `_session_to_dict()`. Store them:
     ```python
     session = result.get('session', {})
     if session:
         app.storage.user['auth_session'] = {
             'access_token': session.get('access_token'),
             'refresh_token': session.get('refresh_token'),
         }
     ```

  2. In `GlobalAuthState.clear_auth()` (line ~98): Also clear the session tokens:
     ```python
     app.storage.user.pop('auth_session', None)
     ```

  **In web/main.py:**

  1. Find the OAuth callback handler (around line ~2044) where `app.storage.user[GlobalAuthState.USER_KEY] = user` is set. Also store session tokens there. Look for `set_session_from_url` calls and store the session from that result similarly:
     ```python
     session = result.get('session', {})
     if session:
         app.storage.user['auth_session'] = {
             'access_token': session.get('access_token'),
             'refresh_token': session.get('refresh_token'),
         }
     ```

  Also check `web/api.py` around line ~721 for the same OAuth callback pattern and add token storage there too.
  </action>
  <verify>
  1. `python -c "from web.supabase_client import get_user_client; print('import ok')"` succeeds
  2. Grep confirms no write function still uses bare `get_client()`: search for `client = get_client()` inside create_*/update_*/delete_*/add_* functions — should only appear in read functions
  3. Grep confirms `auth_session` is stored in do_login and cleared in clear_auth
  </verify>
  <done>
  - `get_user_client()` exists and creates per-user Supabase client from stored tokens
  - All write functions use `get_user_client()` instead of `get_client()`
  - Session tokens stored during email login and OAuth callback
  - Session tokens cleared on logout
  </done>
</task>

<task type="auto">
  <name>Task 2: Improve desktop login error messages</name>
  <files>supabase_corrections_client.py</files>
  <action>
  In `supabase_corrections_client.py`, update the `login()` method (line ~562) to provide more specific error messages:

  1. In the `except AuthApiError as e:` block (line ~582), parse the error string and return user-friendly messages:
     ```python
     except AuthApiError as e:
         error_msg = str(e).lower()
         if 'invalid login credentials' in error_msg:
             return False, "Invalid email or password. Please check your credentials and try again."
         elif 'email not confirmed' in error_msg:
             return False, "Email not confirmed. Please check your inbox for a confirmation link."
         elif 'user not found' in error_msg:
             return False, "No account found with this email. Please register first at genizahsearch.com."
         elif 'too many requests' in error_msg or 'rate limit' in error_msg:
             return False, "Too many login attempts. Please wait a few minutes and try again."
         elif 'network' in error_msg or 'connection' in error_msg:
             return False, "Network error. Please check your internet connection."
         else:
             return False, f"Login failed: {str(e)}"
     ```

  2. In the general `except Exception as e:` block (line ~584), also improve:
     ```python
     except Exception as e:
         error_msg = str(e).lower()
         if 'connection' in error_msg or 'timeout' in error_msg or 'resolve' in error_msg:
             return False, "Cannot reach server. Please check your internet connection and try again."
         return False, f"Login error: {str(e)}"
     ```
  </action>
  <verify>
  `python -c "from supabase_corrections_client import SupabaseCorrectionsClient; print('import ok')"` succeeds
  </verify>
  <done>
  - Desktop login shows "Invalid email or password" for wrong credentials
  - Desktop login shows "Email not confirmed" for unconfirmed accounts
  - Desktop login shows network-specific errors for connection issues
  - Generic errors still show the original error text for debugging
  </done>
</task>

</tasks>

<verification>
1. Import check: `python -c "from web.supabase_client import get_user_client, get_client; print('Both functions exist')"`
2. Write function audit: grep for all `def create_|def update_|def delete_|def add_` in supabase_client.py and verify each write function uses `get_user_client()`
3. Token storage: grep `auth_session` in auth_state.py confirms store on login and clear on logout
4. Desktop: `python -c "from supabase_corrections_client import SupabaseCorrectionsClient; print('ok')"`
5. Run existing tests: `pytest tests/ -x -q` to ensure nothing is broken
</verification>

<success_criteria>
- Per-user Supabase client function exists and is used by all authenticated write operations
- Session tokens are stored in NiceGUI's per-user storage on login (email + OAuth)
- Session tokens are cleared on logout
- Read-only operations continue using the efficient singleton client
- Desktop login shows specific, actionable error messages for common failure modes
- All existing tests pass
</success_criteria>

<output>
After completion, create `.planning/quick/8-fix-web-corrections-singleton-supabase-c/8-SUMMARY.md`
</output>
