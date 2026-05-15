# Phase 91: Atomic Auth State Writes — Pattern Map

**Mapped:** 2026-05-15
**Files analyzed:** 6 (2 source modified, 1 lint test modified, 1 YAML modified, 2 new test files)
**Analogs found:** 6 / 6

## File Classification

| File (modified/new) | Role | Data Flow | Closest Analog | Match Quality |
|---------------------|------|-----------|----------------|---------------|
| `web/auth_state.py` (modify) | auth-state / module-class | request-response (storage R/W) | self (in-place migration) + `web/safe_storage.py` (call targets) | exact |
| `web/main.py:1418-1500:auth_callback_route` (modify) | route handler (OAuth callback) | request-response | self (in-place migration) + Phase 90 `clear_auth` rollback discipline | exact |
| `tests/test_no_raw_storage_access.py:200` (modify) | lint scanner test | static AST scan | self (single-line assertion replacement) | exact |
| `.planning/phase87_storage_allowlist.yaml` (modify) | config / allowlist | static | self (delete 2 file-entry blocks) | exact |
| `tests/test_auth_callback_resilience.py` (NEW) | test (resilience / monkeypatch) | unit + async | `tests/test_browse_state.py` (Phase 87 B3) + `tests/test_refresh_lock_per_session.py` (Phase 90 D-17) | exact |
| `tests/test_persist_value_uses_safe_storage.py` (NEW) | test (AST retention guard) | static AST scan | `tests/test_no_deleted_state_references.py` (Phase 88 D-07) + `tests/test_no_set_session_outside_oauth.py` (Phase 90 D-15) | exact |

---

## Pattern Assignments

### 1. `web/auth_state.py` — direct raw→safe substitution + `set_auth: bool` + `do_login` rollback

**Primary analog:** `web/safe_storage.py` (the call targets); current `web/auth_state.py:clear_auth` (Phase 90 — already migrated read-token + has `finally:` rollback discipline).

**Import-block pattern to add** (mirrors existing `clear_auth` deferred import at `web/auth_state.py:128`):
```python
# Top of file with other imports — module-level import is preferred over
# the per-method deferred import used by clear_auth (which only needed
# safe_user_get and had a circular-import concern that no longer applies
# after Phase 87 stabilized safe_storage).
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

**Substitution table (D-01 — exact 9-site replacement):**

| Line | BEFORE | AFTER |
|------|--------|-------|
| 41-44 | `try:\n    return app.storage.user.get(cls.USER_KEY)\nexcept Exception:\n    return None` | `return safe_user_get(cls.USER_KEY)` (try/except removed — `safe_user_get` already handles both AssertionError + Exception, with better logging) |
| 49-52 | `try:\n    return app.storage.user.get(cls.PROFILE_KEY)\nexcept Exception:\n    return None` | `return safe_user_get(cls.PROFILE_KEY)` |
| 95 | `app.storage.user[cls.USER_KEY] = user` | `if not safe_user_set(cls.USER_KEY, user): return False` (checked per D-04) |
| 97 | `app.storage.user[cls.PROFILE_KEY] = profile` | `if not safe_user_set(cls.PROFILE_KEY, profile): safe_user_pop(cls.USER_KEY, None); return False` (rollback per D-04) |
| 117 | `app.storage.user[cls.PROFILE_KEY] = profile` | `safe_user_set(cls.PROFILE_KEY, profile)` (best-effort — `update_profile_cache` is non-boundary path per D-04 note) |
| 138 | `app.storage.user.pop(cls.USER_KEY, None)` | `safe_user_pop(cls.USER_KEY, None)` |
| 139 | `app.storage.user.pop(cls.PROFILE_KEY, None)` | `safe_user_pop(cls.PROFILE_KEY, None)` |
| 140 | `app.storage.user.pop('auth_session', None)` | `safe_user_pop('auth_session', None)` |
| 187 | `app.storage.user['auth_session'] = {...}` | `safe_user_set('auth_session', {...})` (checked per D-05) |

**`set_auth` final shape — copy verbatim per D-04:**
```python
@classmethod
def set_auth(cls, user: Dict, profile: Dict = None) -> bool:
    """Set authentication after successful login. Returns False if any
    write fails (prune race) -- caller MUST handle by surfacing an error.

    Phase 91 D-04: multi-write atomicity is best-effort under NiceGUI
    storage semantics (no compare-and-swap). On partial-write failure
    we roll back any successful writes so callers never observe a
    half-state.
    """
    if not safe_user_set(cls.USER_KEY, user):
        return False
    if profile is not None:
        if not safe_user_set(cls.PROFILE_KEY, profile):
            # Rollback the user write to avoid half-state
            safe_user_pop(cls.USER_KEY, None)
            return False
    cls._posthog_identify(user, profile)
    return True
```

**`do_login` final shape — copy verbatim per D-05** (note: `auth_session`-first ordering, smallest-blast-radius write goes first):
```python
async def do_login(email: str, password: str) -> Dict:
    from nicegui import run
    from web.analytics import posthog_capture
    result = await run.io_bound(supabase_sign_in, email, password)

    if "error" in result:
        posthog_capture('login_failed', {
            'reason': str(result.get('error', ''))[:100],
            'error_code': str(result.get('error_code', ''))[:50],
            'status_code': result.get('status_code', ''),
        })
        return result

    session = result.get('session', {})
    user = result.get('user')
    if not user:
        posthog_capture('login_failed', {'reason': 'No user returned'})
        return {"error": "No user returned"}
    profile = get_profile(user['id'])

    # D-05: session-first; pop on later failure (rollback discipline)
    if session:
        if not safe_user_set('auth_session', {
            'access_token': session.get('access_token'),
            'refresh_token': session.get('refresh_token'),
        }):
            posthog_capture('login_failed', {'reason': 'session_storage_unavailable'})
            return {"error": "Session storage unavailable. Please try again."}
    if not GlobalAuthState.set_auth(user, profile):
        # set_auth already rolled back its own user-key write; we
        # also pop the session write to keep the rollback complete.
        safe_user_pop('auth_session', None)
        posthog_capture('login_failed', {'reason': 'auth_state_storage_unavailable'})
        return {"error": "Session storage unavailable. Please try again."}
    posthog_capture('login_success', {})
    return {"success": True, "user": user, "profile": profile}
```

**`clear_auth` after Phase 91 — DROP the raw-`app.storage.user.pop()` 3 lines, KEEP the revoke-first + `finally:` shape from Phase 90 (D-11b):**
```python
@classmethod
def clear_auth(cls):
    """Phase 90 D-11: server-side revocation BEFORE local cleanup so the
    token is actually invalidated on Supabase's side. Local cleanup in
    a finally block runs even when server revocation fails.
    """
    # safe_user_get is already imported at module top (per Phase 91 D-01)
    auth_session = safe_user_get('auth_session') or {}
    access_token = auth_session.get('access_token')
    try:
        supabase_sign_out(access_token)
    except Exception:
        pass
    finally:
        # Phase 91 D-01: 3 raw pops -> safe_user_pop
        safe_user_pop(cls.USER_KEY, None)
        safe_user_pop(cls.PROFILE_KEY, None)
        safe_user_pop('auth_session', None)
    try:
        ui.run_javascript('if(window.posthog)posthog.reset()')
    except Exception:
        pass
```

**Reader patterns to mirror** (from `web/safe_storage.py:46-86`):
```python
def safe_user_get(key: str, default: Any = None) -> Any:
    """Read app.storage.user[key], returning default on any failure."""
    try:
        return app.storage.user.get(key, default)
    except AssertionError as e:
        logger.debug("safe_user_get(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_get(%r) unexpected failure: %s", key, e, exc_info=False)
        return default

def safe_user_set(key: str, value: Any) -> bool:
    """Write app.storage.user[key] = value. Return True on success."""
    try:
        app.storage.user[key] = value
        return True
    except AssertionError as e:
        logger.debug("safe_user_set(%r): session storage unavailable: %s", key, e)
        return False
    except Exception as e:
        logger.warning("safe_user_set(%r) unexpected failure: %s", key, e, exc_info=False)
        return False
```

The `try/except Exception: return None` wrappers at lines 41-44 and 49-52 are **deleted** in Phase 91 — `safe_user_get` already provides better error swallowing with explicit AssertionError vs. Exception logging.

---

### 2. `web/main.py:1439-1453:complete_login` — 3 raw access sites → `safe_user_set` with rollback

**Analog:** Phase 90 `clear_auth` revoke-then-`finally:`-pop discipline; the OAuth-callback `show_error()` helper at `web/main.py:1455-1462` (already exists, just used in new error path).

**Existing imports already in `web/main.py`** (no new top-level import needed — already has `from web.safe_storage import safe_user_set` and the page handler at line 1418 calls `ensure_session_uuid()` at line 1425):
```python
# web/main.py already imports safe_user_set at module level (verified
# via line 1401: `safe_user_set('current_page', '/download')`). Only
# add safe_user_pop if not already imported.
```

**`complete_login` BEFORE (lines 1439-1453):**
```python
async def complete_login(user, profile, session=None):
    """Store user in session and redirect."""
    app.storage.user[GlobalAuthState.USER_KEY] = user
    if profile:
        app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
    # Store session tokens for per-user Supabase client
    if session:
        app.storage.user['auth_session'] = {
            'access_token': session.get('access_token'),
            'refresh_token': session.get('refresh_token'),
        }
    posthog_capture('login_success', {'method': 'google_oauth'})
    status_label.text = 'Login successful! Redirecting...'
    await asyncio.sleep(0.5)
    ui.navigate.to('/')
```

**`complete_login` AFTER — copy verbatim per D-06:**
```python
async def complete_login(user, profile, session=None):
    """Store user in session and redirect.

    Phase 91 AUTHW-02 + D-06: every write is checked. On partial-write
    failure, roll back any successful writes and surface an error label
    so the user doesn't reload into a half-logged-in state. The OAuth
    code has already been consumed by exchange_code_for_session so there
    is no auto-retry path; the user must restart the OAuth flow.
    """
    # Session token write FIRST (no user-visible side-effect yet)
    if session:
        if not safe_user_set('auth_session', {
            'access_token': session.get('access_token'),
            'refresh_token': session.get('refresh_token'),
        }):
            posthog_capture('login_failed', {
                'reason': 'session_storage_unavailable',
                'method': 'google_oauth',
            })
            show_error('Session storage unavailable. Please try again.')
            return
    # User + profile next (use set_auth's built-in rollback)
    if not GlobalAuthState.set_auth(user, profile):
        safe_user_pop('auth_session', None)
        posthog_capture('login_failed', {
            'reason': 'auth_state_storage_unavailable',
            'method': 'google_oauth',
        })
        show_error('Session storage unavailable. Please try again.')
        return
    posthog_capture('login_success', {'method': 'google_oauth'})
    status_label.text = 'Login successful! Redirecting...'
    await asyncio.sleep(0.5)
    ui.navigate.to('/')
```

**Note:** The outer `try/except Exception` at `web/main.py:1494-1500` is the safety net for any unexpected exception inside `show_error` itself (extremely unlikely under normal storage failure). Phase 91 makes no changes to the outer try/except.

---

### 3. `tests/test_no_raw_storage_access.py:200` — empty-allowlist assertion fix (Codex F3, D-07)

**Analog:** self (single-line assertion replacement). The other 5 tests in this file are unaffected by an empty allowlist.

**BEFORE (line 200):**
```python
def test_allowlist_well_formed():
    """FOUND-03 schema check: every allowlist entry has file + patterns + justification.

    Per H1, each pattern must be a dict with `source` and `expected_count`.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    assert entries, "Allowlist is empty — at minimum web/auth_state.py should be allowlisted"
    for entry in entries:
        ...
```

**AFTER — replace `assert entries, ...` with explanatory comment per D-07:**
```python
def test_allowlist_well_formed():
    """FOUND-03 schema check: every allowlist entry has file + patterns + justification.

    Per H1, each pattern must be a dict with `source` and `expected_count`.
    """
    allowlist = _load_allowlist()
    entries = allowlist.get('allowed_raw_access', [])
    # Post-Phase-91 (AUTHW-01 + AUTHW-02): allowlist may be empty --
    # web/auth_state.py and web/main.py raw accesses are now migrated
    # to safe_storage helpers. An empty list still passes the lint
    # invariant (every raw access in web/ outside the empty allowlist
    # is rejected) and is the goal state for v7.12 Path B.
    # If a new raw access is ever justified, re-add it with explicit
    # justification + expected_count. The validators below still apply
    # to every present entry.
    for entry in entries:
        ...  # validators below stay -- no-op when entries == []
```

---

### 4. `.planning/phase87_storage_allowlist.yaml` — delete both file-entry blocks (D-07b)

**Analog:** self (mechanical deletion of YAML lines 21-69 + 71-93).

**BEFORE — current YAML state (lines 20-94):**
```yaml
allowed_raw_access:
  - file: web/auth_state.py
    patterns:
      - source: "app.storage.user.get(cls.USER_KEY)"
        expected_count: 1
        enclosing: "GlobalAuthState.get_user"
      - source: "app.storage.user.get(cls.PROFILE_KEY)"
        expected_count: 1
        enclosing: "GlobalAuthState.get_profile"
      ... (8 patterns total) ...
    justification: |
      GlobalAuthState class methods (lines 42, 50, 95, 97, 117, 122, 123, 124, 176)
      ...

  - file: web/main.py
    patterns:
      - source: "app.storage.user[GlobalAuthState.USER_KEY]"
        expected_count: 1
        enclosing: "OAuth callback at main.py:1458"
      ... (3 patterns total) ...
    justification: |
      OAuth callback handler at main.py:1458-1463 ...
```

**AFTER — empty list, header comments preserved:**
```yaml
# Phase 87 Storage Allowlist
# Each entry exempts specific raw `app.storage.user` access patterns from the
# lint test in tests/test_no_raw_storage_access.py. Per FOUND-03, every entry
# MUST have a justification.
#
# Schema (revised per 87-REVIEWS.md H1):
#   patterns: list of objects, each with:
#     - source: exact substring matched against ast.get_source_segment output
#     - expected_count: int — how many AST nodes in the file are expected to
#       match this pattern. Enforced by test_allowlist_counts_exact.
#     - enclosing (optional): function or class name for human readability.
#
# Pattern matching is substring-based against the AST node source segment.
# Patterns are matched per-file; an entry applies ONLY to its `file:` path.
#
# To remove an entry: migrate the call site to web.safe_storage helpers.
# To add an entry: open a PR with justification — entries require review.
# To extend an entry: bump expected_count if the new raw access is also justified.

# Post-Phase-91 state: list is empty -- v7.12 Path B goal achieved.
# All previous entries (Phase 87: 4 -> 88: 3 -> 90: 2 -> 91: 0) have
# been self-eliminated as each phase migrated its surface to safe_storage.
allowed_raw_access: []
```

**Verification command** (planner / executor):
```bash
# After deletion, this must report 0:
grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml
```

---

### 5. `tests/test_auth_callback_resilience.py` (NEW) — T-A / T-B / T-C per D-08

**Primary analog:** `tests/test_browse_state.py` (Phase 87 B3 monkeypatch shape — `patch('web.safe_storage.app')` + `mock_app.storage.user = storage`).
**Secondary analog:** `tests/test_refresh_lock_per_session.py:138-180` (Phase 90 D-17 instance-isolated `SimpleNamespace` stub pattern with `monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(storage=SimpleNamespace(user=...)))`).

**Imports pattern — copy from `tests/test_refresh_lock_per_session.py:1-21`:**
```python
"""Phase 91 AUTHW-05 -- resilience tests for OAuth complete_login under
session-storage prune races. Three scenarios:
  T-A: prune-pre-write -> friendly error, no AssertionError, no navigate.
  T-B: happy-path -> all 3 keys persisted, ui.navigate.to('/') called.
  T-C: GlobalAuthState.get_user() under fully-pruned storage -> returns
       None, no AssertionError propagated.

Mirrors Phase 87 B3 monkeypatch pattern from tests/test_browse_state.py
and Phase 90 D-17 instance-isolated SimpleNamespace stubs from
tests/test_refresh_lock_per_session.py. No threading needed -- each
test is single-flow.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock
import pytest
```

**Per-test monkeypatch pattern — happy-path (T-B) — derived from `tests/test_browse_state.py:91-117` + `tests/test_refresh_lock_per_session.py:148-151`:**
```python
@pytest.mark.asyncio
async def test_oauth_callback_happy_path(monkeypatch):
    """T-B: writes succeed -> all 3 keys persisted -> ui.navigate.to('/') called."""
    storage = {}  # plain dict-backed SimpleNamespace storage stub
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    # Mock ui.navigate.to + posthog_capture + ui status_label (NiceGUI
    # widgets we don't instantiate in a unit test).
    nav_mock = MagicMock()
    monkeypatch.setattr('web.main.ui.navigate.to', nav_mock)
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    status_label_mock = SimpleNamespace(text='')
    # ... patch asyncio.sleep to no-op so test runs fast ...
    monkeypatch.setattr('web.main.asyncio.sleep', AsyncMock())

    # Import LATE so monkeypatch is already in effect (Phase 87 B3 idiom)
    from web.main import auth_callback_route  # noqa
    # Manually invoke complete_login inner function -- prefer factoring
    # it out for testability, OR construct minimal page context.
    # See D-08 for the exact call: complete_login({'id': 'u1', ...}, {'username': 'u1'}, session={...})

    # Assertions
    assert storage['auth_user'] == {'id': 'u1', 'email': 'a@b.c'}
    assert storage['auth_profile'] == {'username': 'u1'}
    assert storage['auth_session'] == {'access_token': 'at', 'refresh_token': 'rt'}
    nav_mock.assert_called_once_with('/')
    posthog_mock.assert_any_call('login_success', {'method': 'google_oauth'})
```

**Prune-pre-write (T-A) pattern — derived from `tests/test_browse_state.py:120-140`:**
```python
@pytest.mark.asyncio
async def test_oauth_callback_prune_pre_write_shows_error(monkeypatch):
    """T-A: prune race -> safe_user_set returns False -> show_error called,
    no AssertionError propagated, no navigate, login_failed posthog event."""
    # storage stub raises AssertionError on EVERY access (read or write)
    storage = MagicMock()
    storage.__setitem__.side_effect = AssertionError(
        'user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be created before accessing it'
    )
    storage.get.side_effect = AssertionError('...')
    storage.pop.side_effect = AssertionError('...')
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    nav_mock = MagicMock()
    monkeypatch.setattr('web.main.ui.navigate.to', nav_mock)
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    show_error_called = []
    monkeypatch.setattr(...)  # capture show_error invocation

    # Call complete_login -- must NOT raise AssertionError
    # ... (factor complete_login out of auth_callback_route or use a
    #      test-only seam; see D-08 for exact construction)

    # Assertions:
    nav_mock.assert_not_called()  # No navigate on partial-write failure
    assert any('Session storage unavailable' in str(c) for c in show_error_called)
    posthog_mock.assert_any_call('login_failed', {
        'reason': 'session_storage_unavailable',
        'method': 'google_oauth',
    })
```

**Direct-method test (T-C) — derived from `tests/test_browse_state.py:120-140`:**
```python
def test_get_user_under_pruned_storage_returns_none(monkeypatch):
    """T-C (Codex M3 reshape): GlobalAuthState.get_user() under fully-pruned
    storage returns None, no AssertionError propagated.

    Tests the safe_user_get migration at auth_state.py:42 actually swallows
    the prune-race exception -- the entire point of AUTHW-01 for readers.
    """
    storage = MagicMock()
    storage.get.side_effect = AssertionError(
        'user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be created before accessing it'
    )
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )
    from web.auth_state import GlobalAuthState
    # Must NOT raise AssertionError
    assert GlobalAuthState.get_user() is None
    assert GlobalAuthState.get_profile() is None
```

**Test isolation rules (per D-08 + Phase 88 D-01 + D-02 + Refinement 6):**
- Each test instantiates its own `SimpleNamespace` storage stub via the `monkeypatch` fixture.
- No module-level state — no shared `storage = {}` at file scope.
- Import `from web.auth_state import GlobalAuthState` INSIDE each test (Phase 87 B3 idiom — ensures monkeypatch is in effect before any code touches `web.safe_storage.app`).
- Use `monkeypatch.setattr('web.safe_storage.app', ...)` (string-form) — NOT `monkeypatch.setattr(safe_storage, 'app', ...)` which would fail to redirect imports.

**Testability seam (planner note):** `complete_login` is currently an INNER function of `auth_callback_route` (`web/main.py:1439`). To test it cleanly, either:
1. **Factor it out** to module level (`web/main.py:async def _oauth_complete_login(user, profile, session, status_label, show_error_fn): ...`) and have `auth_callback_route` call it. T-A / T-B inject mocks for `status_label` + `show_error_fn`.
2. **Or test via the route handler** by mocking `exchange_code_for_session` to return a fixed user+profile+session and invoking the page route through NiceGUI's test client.

Option 1 is preferred — cleaner unit test, no NiceGUI page-render dependency. Plan 91-01 may need to add the factoring as a prerequisite micro-task.

---

### 6. `tests/test_persist_value_uses_safe_storage.py` (NEW) — AST retention guard per D-09

**Primary analog:** `tests/test_no_deleted_state_references.py` (Phase 88 D-07 — `ast.parse(Path(...).read_text())` + `_DeletedStateAccessVisitor` walker pattern with seed-trap snippets).
**Secondary analog:** `tests/test_no_set_session_outside_oauth.py` (Phase 90 D-15 — `_find_function_def`-style enclosing-function precision + `SEED_TRAP_SNIPPETS` parametrize idiom).

**Imports + helpers — copy from `tests/test_no_deleted_state_references.py:29-35` + Phase 90 `tests/test_no_set_session_outside_oauth.py:32-69`:**
```python
"""Phase 91 AUTHW-06 -- AST retention guard for filter_panel.py:persist_value
safe-wrap (originally landed in commit cca23db3 / 2026-05-12 Codex 3rd-pass
CRITICAL fix). Prevents a future refactor from un-doing the safe-wrap.

Per Codex M5: grep is enough for the *raw* app.storage.user lint (which
test_no_raw_storage_access.py covers). persist_value's contract is
*function-local* -- the function body must (a) import from web.safe_storage,
(b) gate on safe_user_get('session_persistence_enabled', True), AND (c)
write via safe_user_set. AST walking gives function-body precision without
false positives from comments/strings.

Mirrors Phase 88 D-07 (test_no_deleted_state_references.py) seed-trap idiom
and Phase 90 D-15 (test_no_set_session_outside_oauth.py) AST-Call shape.
"""
import ast
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_FILE = REPO_ROOT / 'web' / 'components' / 'filter_panel.py'


def _find_function_def(tree, name):
    """Return the first ast.FunctionDef whose .name == `name`, or None.

    Mirrors Phase 88's _scan_file pattern (test_no_deleted_state_references.py:
    151-155) but scoped to one named function instead of file-wide walk.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _is_app_storage_user_subscript(target):
    """Return True iff `target` is an ast.Subscript matching `<app>.storage.user[...]`.

    Used to detect raw `app.storage.user[k] = v` writes inside persist_value.
    """
    if not isinstance(target, ast.Subscript):
        return False
    val = target.value
    if not isinstance(val, ast.Attribute) or val.attr != 'user':
        return False
    if not isinstance(val.value, ast.Attribute) or val.value.attr != 'storage':
        return False
    return True
```

**Three assertions — copy verbatim per D-09:**
```python
def test_persist_value_imports_safe_storage_helpers():
    """AUTHW-06: persist_value must import safe_user_get + safe_user_set."""
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "persist_value() function disappeared from filter_panel.py"
    imports = [n for n in ast.walk(fn) if isinstance(n, ast.ImportFrom)
               and n.module == 'web.safe_storage']
    assert imports, "persist_value() must import from web.safe_storage"
    imported_names = {alias.name for imp in imports for alias in imp.names}
    assert 'safe_user_get' in imported_names
    assert 'safe_user_set' in imported_names


def test_persist_value_reads_persistence_flag():
    """AUTHW-06: persist_value must gate on session_persistence_enabled."""
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_get'
             and n.args and isinstance(n.args[0], ast.Constant)
             and n.args[0].value == 'session_persistence_enabled']
    assert calls, ("persist_value() must read 'session_persistence_enabled' "
                   "via safe_user_get to gate persistence (AUTHW-06 retention)")


def test_persist_value_writes_via_safe_user_set():
    """AUTHW-06: persist_value must NOT use raw app.storage.user[...] = ..."""
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    raw_writes = [n for n in ast.walk(fn) if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert not raw_writes, ("persist_value() reintroduced raw "
                            "app.storage.user[...] = ...; must use safe_user_set "
                            "(AUTHW-06 retention)")
    safe_set_calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set']
    assert safe_set_calls, "persist_value() must call safe_user_set to write"
```

**Seed-trap snippets — mirror Phase 88 `test_scanner_detects_synthetic_attribute_access` + Phase 90 `SEED_TRAP_SNIPPETS` pattern:**
```python
# Per D-09: seed traps live as in-test parsed snippets, mirroring Phase 88 D-07
# (test_no_deleted_state_references.py:158-197) and Phase 90 D-15
# (test_no_set_session_outside_oauth.py:230-270).

_PASSING_SNIPPET = (
    "def persist_value(k, v):\n"
    "    from web.safe_storage import safe_user_get, safe_user_set\n"
    "    if safe_user_get('session_persistence_enabled', True):\n"
    "        safe_user_set(k, v)\n"
)

_FAILING_SNIPPET = (
    "def persist_value(k, v):\n"
    "    from nicegui import app\n"
    "    app.storage.user[k] = v\n"
)


def test_seed_trap_passing_snippet_passes_all_three_checks():
    """Sanity: the canonical correct shape passes all 3 assertions."""
    tree = ast.parse(_PASSING_SNIPPET)
    fn = _find_function_def(tree, 'persist_value')
    # Import check
    imports = [n for n in ast.walk(fn) if isinstance(n, ast.ImportFrom)
               and n.module == 'web.safe_storage']
    imported_names = {alias.name for imp in imports for alias in imp.names}
    assert 'safe_user_get' in imported_names
    assert 'safe_user_set' in imported_names
    # Flag-read check
    flag_calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_get'
                  and n.args and isinstance(n.args[0], ast.Constant)
                  and n.args[0].value == 'session_persistence_enabled']
    assert flag_calls
    # Write check
    raw_writes = [n for n in ast.walk(fn) if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert not raw_writes
    safe_set_calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Name) and n.func.id == 'safe_user_set']
    assert safe_set_calls


def test_seed_trap_failing_snippet_fails_at_least_one_check():
    """Sanity: a deliberately-bad shape fails at least one of the 3 assertions.

    Specifically the failing snippet uses raw app.storage.user[k] = v
    which must trip test_persist_value_writes_via_safe_user_set.
    """
    tree = ast.parse(_FAILING_SNIPPET)
    fn = _find_function_def(tree, 'persist_value')
    # The failing snippet must trip the raw-subscript check
    raw_writes = [n for n in ast.walk(fn) if isinstance(n, ast.Assign)
                  and any(_is_app_storage_user_subscript(t) for t in n.targets)]
    assert raw_writes, ("Seed-trap failing snippet should have been flagged "
                        "by _is_app_storage_user_subscript -- scanner has a "
                        "false-negative gap.")
```

---

## Shared Patterns

### Pattern A: `safe_storage` helper import

**Source:** `web/safe_storage.py:46-86`
**Apply to:** `web/auth_state.py` (module-top import); `web/main.py` (already imports — verify `safe_user_pop` is in the imports if needed for the OAuth rollback).

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

### Pattern B: Multi-write rollback discipline (Phase 91 NEW)

**Source:** Phase 91 D-04 (`set_auth`), D-05 (`do_login`), D-06 (`complete_login`).
**Apply to:** Every multi-key auth write boundary.

**Shape:**
```python
# Write smallest-blast-radius key first
if not safe_user_set('key_a', value_a):
    return False  # OR show_error + return  (caller-shape dependent)
# Write second key; pop first on failure (rollback)
if not safe_user_set('key_b', value_b):
    safe_user_pop('key_a', None)
    return False
```

**Rationale:** NiceGUI's `app.storage.user` is per-key write — no compare-and-swap. The narrow window between key A write success and key B write failure is the only half-state surface; rollback closes it on best-effort terms.

### Pattern C: `try/finally:` revoke-then-cleanup (Phase 90 — inherited unchanged)

**Source:** `web/auth_state.py:120-145:clear_auth` (Phase 90 D-11b).
**Apply to:** Any logout/teardown path that does a network revoke before local cleanup.

```python
auth_session = safe_user_get('auth_session') or {}
access_token = auth_session.get('access_token')
try:
    supabase_sign_out(access_token)  # Network call; may fail
except Exception:
    pass  # Local cleanup must still run
finally:
    safe_user_pop(cls.USER_KEY, None)
    safe_user_pop(cls.PROFILE_KEY, None)
    safe_user_pop('auth_session', None)
```

### Pattern D: `monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(...))` test stub

**Source:** `tests/test_browse_state.py` (Phase 87 B3 — original); `tests/test_refresh_lock_per_session.py:148-151` (Phase 90 D-17 — instance-isolated SimpleNamespace).
**Apply to:** Every new test in `tests/test_auth_callback_resilience.py`.

```python
storage = {}  # plain dict OR MagicMock with side_effect=AssertionError(...) for prune-race tests
monkeypatch.setattr(
    'web.safe_storage.app',
    SimpleNamespace(storage=SimpleNamespace(user=storage)),
)
# Import AFTER monkeypatch (B3 idiom):
from web.auth_state import GlobalAuthState
```

**Test-isolation rules:**
- Per-test fixture; no module-level shared state.
- Use the STRING form of `monkeypatch.setattr` (redirect imports) — not the object-attribute form.
- For prune-race scenarios, raise `AssertionError("user storage for {uuid} should be created before accessing it")` from `storage.get.side_effect` / `storage.__setitem__.side_effect` / `storage.pop.side_effect` (matches the actual NiceGUI exception message from `nicegui/storage.py:121`).

### Pattern E: AST scanner with `_find_function_def` + seed-trap snippets

**Source:** `tests/test_no_deleted_state_references.py` (Phase 88 D-07) + `tests/test_no_set_session_outside_oauth.py:230-270` (Phase 90 D-15 — `SEED_TRAP_SNIPPETS`).
**Apply to:** `tests/test_persist_value_uses_safe_storage.py` (AUTHW-06).

**Skeleton:**
```python
import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_FILE = REPO_ROOT / 'web' / 'components' / 'filter_panel.py'

def _find_function_def(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None

# Production guard:
def test_some_invariant():
    tree = ast.parse(TARGET_FILE.read_text(encoding='utf-8'))
    fn = _find_function_def(tree, 'persist_value')
    assert fn is not None, "function disappeared"
    # ... ast.walk(fn) to find required/forbidden constructs ...

# Seed traps as parsed snippets:
_PASSING = "def foo(): ..."
_FAILING = "def foo(): ..."

def test_seed_passing_passes(): ...
def test_seed_failing_fails(): ...
```

---

## No Analog Found

None. Every Phase 91 file modification or creation has a direct in-codebase analog from Phases 87 / 88 / 90.

---

## Metadata

**Analog search scope:**
- `web/safe_storage.py` (call targets — Pattern A, B helpers)
- `web/auth_state.py` (in-place migration target + `clear_auth` rollback shape — Pattern C)
- `web/main.py:1418-1500` (OAuth callback — in-place migration target)
- `web/components/filter_panel.py:220-231` (AUTHW-06 retention target)
- `tests/test_browse_state.py` (Phase 87 B3 monkeypatch pattern — Pattern D)
- `tests/test_refresh_lock_per_session.py` (Phase 90 D-17 SimpleNamespace stub — Pattern D)
- `tests/test_no_raw_storage_access.py` (Phase 87 lint scanner — assertion fix target)
- `tests/test_no_deleted_state_references.py` (Phase 88 D-07 AST scanner — Pattern E)
- `tests/test_no_set_session_outside_oauth.py` (Phase 90 D-15 AST scanner + seed traps — Pattern E)
- `tests/test_no_appstate_export_fields.py` (Phase 88 D-06 runtime guard — referenced for context, no direct Phase 91 mirror)
- `.planning/phase87_storage_allowlist.yaml` (allowlist self-elimination target)

**Files scanned:** 11 (5 production source, 1 config, 5 test analogs)

**Pattern extraction date:** 2026-05-15
