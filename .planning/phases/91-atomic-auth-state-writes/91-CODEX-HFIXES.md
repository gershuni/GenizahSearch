### NEW-H1
**File:** `web/main.py` lines `26-30`
**BEFORE:**
```python
from nicegui import ui, app, run
from web.framework_patches import apply_all_patches
from web.crawler_visibility import should_block_archive_request, should_mark_noindex
from web.safe_storage import ensure_session_uuid, safe_user_get, safe_user_set, safe_user_pop
apply_all_patches()
```
**AFTER:**
```python
from nicegui import ui, app, run
from web.framework_patches import apply_all_patches
from web.crawler_visibility import should_block_archive_request, should_mark_noindex
from web.safe_storage import ensure_session_uuid, safe_user_get, safe_user_set, safe_user_pop
from web.auth_state import GlobalAuthState
apply_all_patches()
```
**Rationale (≤1 line):** Module-top is safe here: `web.auth_state` imports `web.supabase_client`/`safe_storage`, not `web.main`, so no circular import route is introduced.

### NEW-H2
**File:** `web/auth_state.py` line `12`
**BEFORE:**
```python
from nicegui import app, ui
```
**AFTER:**
```python
from nicegui import ui
```

### NEW-H3
**File:** `tests/test_auth_callback_resilience.py` import block
**BEFORE:**
```python
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock

import pytest
```
**AFTER:**
```python
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock
```

### NEW-H4
**File:** `tests/test_auth_callback_resilience.py` section `test_set_auth_symmetric_rollback_on_profile_write_failure`
**BEFORE:**
```python
def test_set_auth_symmetric_rollback_on_profile_write_failure(monkeypatch):
    """T-D (Revision MUST-3): set_auth user-write succeeds, profile-write fails.

    Per Revision MUST-2 SYMMETRIC rollback: BOTH auth_user AND auth_profile
    must be absent post-failure. The previous D-04 only popped auth_user,
    leaving stale auth_profile observable to GlobalAuthState.get_role() /
    is_admin() / is_editor() -- Codex's HIGH severity catch.
    """
    # Routing storage: writes to 'auth_profile' raise; all other writes succeed.
    storage = _RoutingStorage(fail_writes_for={'auth_profile'})
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    from web.auth_state import GlobalAuthState

    result = GlobalAuthState.set_auth(
        user={'id': 'u1', 'email': 'a@b.c'},
        profile={'username': 'u1', 'role': 'user'},
    )

    assert result is False, "set_auth should return False on profile-write failure"
    # SYMMETRIC rollback: BOTH keys absent.
    assert 'auth_user' not in storage, (
        f"auth_user was NOT rolled back (Revision MUST-2 fail): {dict(storage)}"
    )
    assert 'auth_profile' not in storage, (
        f"auth_profile somehow present (write should have failed): {dict(storage)}"
    )
    # auth_session is unrelated -- set_auth doesn't touch it.
    assert 'auth_session' not in storage
```
**AFTER:**
```python
def test_set_auth_symmetric_rollback_on_profile_write_failure(monkeypatch):
    """T-D (Revision MUST-3): set_auth user-write succeeds, profile-write fails."""
    storage = _RoutingStorage(
        {'auth_profile': {'role': 'admin', 'username': 'old_admin'}},
        fail_writes_for={'auth_profile'},
    )
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    from web.auth_state import GlobalAuthState

    result = GlobalAuthState.set_auth(
        user={'id': 'u1', 'email': 'a@b.c'},
        profile={'username': 'u1', 'role': 'user'},
    )

    assert result is False, "set_auth should return False on profile-write failure"
    assert 'auth_user' not in storage, (
        f"auth_user was NOT rolled back (Revision MUST-2 fail): {dict(storage)}"
    )
    assert 'auth_profile' not in storage, (
        f"stale auth_profile NOT rolled back (Revision MUST-2 fail): {dict(storage)}"
    )
    assert 'auth_session' not in storage
    assert GlobalAuthState.get_role() is None
```
**Rationale (≤1 line):** Seeding stale admin profile proves rollback clears the role source, not merely that a failed profile write left no new value.

### NEW-H5
**File:** `tests/test_auth_callback_resilience.py` section `test_oauth_complete_login_defensive_3_key_rollback`
**BEFORE:**
```python
def test_oauth_complete_login_defensive_3_key_rollback(monkeypatch):
    """T-E (Revision MUST-3): session-write succeeds, then set_auth fails.

    Per Revision MUST-2 DEFENSIVE 3-key rollback in _oauth_complete_login:
    when set_auth returns False, ALL 3 auth keys must be popped (best-effort)
    -- not just auth_session. This is defensive against the case where
    set_auth's own SYMMETRIC rollback (T-D) ALSO failed during partial-write.
    """
    # Routing storage: session writes succeed, but auth_user writes raise.
    # set_auth(user, profile) will try to write auth_user first -- fail -- and
    # internally try to return False without rollback (since user-write itself
    # failed before any rollback could happen). _oauth_complete_login then
    # invokes the DEFENSIVE outer rollback that pops all 3 keys.
    storage = _RoutingStorage(fail_writes_for={'auth_user'})
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    nav_mock = MagicMock()
    monkeypatch.setattr('web.main.ui.navigate.to', nav_mock)
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    monkeypatch.setattr('web.main.asyncio.sleep', AsyncMock())

    from web.main import _oauth_complete_login

    show_error_calls = []
    def show_error_fn(message):
        show_error_calls.append(message)

    status_label_stub = SimpleNamespace(text='')

    asyncio.run(_oauth_complete_login(
        user={'id': 'u1', 'email': 'a@b.c'},
        profile={'username': 'u1'},
        session={'access_token': 'at', 'refresh_token': 'rt'},
        status_label=status_label_stub,
        show_error_fn=show_error_fn,
    ))

    # DEFENSIVE 3-key cleanup: all 3 auth keys must be absent post-failure.
    assert 'auth_session' not in storage, (
        f"auth_session NOT cleaned (DEFENSIVE rollback fail): {dict(storage)}"
    )
    assert 'auth_user' not in storage, (
        f"auth_user somehow present (write should have failed + been cleaned): {dict(storage)}"
    )
    assert 'auth_profile' not in storage, (
        f"auth_profile NOT cleaned (DEFENSIVE rollback fail): {dict(storage)}"
    )

    nav_mock.assert_not_called()
    assert show_error_calls, "show_error_fn was never called"
    assert 'Session storage unavailable' in show_error_calls[0]
    posthog_mock.assert_any_call('login_failed', {
        'reason': 'auth_state_storage_unavailable',
        'method': 'google_oauth',
    })
```
**AFTER:**
```python
def test_oauth_complete_login_defensive_3_key_rollback(monkeypatch):
    """T-E (Revision MUST-3): session-write succeeds, then set_auth fails."""
    storage = _RoutingStorage(
        {
            'auth_user': {'id': 'old_u', 'email': 'old@b.c'},
            'auth_profile': {'role': 'admin', 'username': 'old_admin'},
        },
        fail_writes_for={'auth_user'},
    )
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    nav_mock = MagicMock()
    monkeypatch.setattr('web.main.ui.navigate.to', nav_mock)
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    monkeypatch.setattr('web.main.asyncio.sleep', AsyncMock())

    from web.main import _oauth_complete_login

    show_error_calls = []
    def show_error_fn(message):
        show_error_calls.append(message)

    status_label_stub = SimpleNamespace(text='')

    asyncio.run(_oauth_complete_login(
        user={'id': 'u1', 'email': 'a@b.c'},
        profile={'username': 'u1'},
        session={'access_token': 'at', 'refresh_token': 'rt'},
        status_label=status_label_stub,
        show_error_fn=show_error_fn,
    ))

    assert 'auth_session' not in storage, (
        f"auth_session NOT cleaned (DEFENSIVE rollback fail): {dict(storage)}"
    )
    assert 'auth_user' not in storage, (
        f"stale auth_user NOT cleaned (DEFENSIVE rollback fail): {dict(storage)}"
    )
    assert 'auth_profile' not in storage, (
        f"stale auth_profile NOT cleaned (DEFENSIVE rollback fail): {dict(storage)}"
    )

    nav_mock.assert_not_called()
    assert show_error_calls, "show_error_fn was never called"
    assert 'Session storage unavailable' in show_error_calls[0]
    posthog_mock.assert_any_call('login_failed', {
        'reason': 'auth_state_storage_unavailable',
        'method': 'google_oauth',
    })
```
**Rationale (≤1 line):** Pre-seeded stale user/profile prove the outer rollback pops all auth keys, not only the newly written session key.
