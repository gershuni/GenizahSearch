"""Phase 91 AUTHW-05 -- resilience tests for OAuth complete_login under
session-storage prune races + set_auth partial-write rollback.

Six scenarios (D-08 + Revision MUST-3 from 91-REVIEWS.md Consensus Summary):
  T-A: prune-pre-write -> friendly error, no AssertionError, no navigate.
  T-B: happy-path -> all 3 keys persisted, ui.navigate.to('/') called.
  T-C: GlobalAuthState.get_user() under fully-pruned storage -> returns
       None, no AssertionError propagated.
  T-D: set_auth(user, profile) where user-write succeeds but profile-write
       fails AND a STALE auth_profile from a prior session is present ->
       SYMMETRIC 2-key rollback pops BOTH the new auth_user write AND the
       stale auth_profile (Revision MUST-2, NEW-H4 round-2 cross-AI catch
       -- pre-seeding stale state proves the rollback CLEARS stale data,
       not merely that empty-storage stays empty).
  T-E: _oauth_complete_login where session-write succeeds but set_auth
       fails AND stale auth_user + auth_profile from a prior session are
       present -> DEFENSIVE 3-key cleanup at the CALLER level (all keys
       absent post-failure; NEW-H5 round-2 cross-AI catch -- pre-seeding
       stale auth_user + auth_profile proves the defensive cleanup pops
       all 3 keys including stale, not just the newly-written auth_session).
  T-F: set_auth(new_user, profile=None) with stale auth_profile present
       -> stale profile cleared (Revision MUST-2 profile-is-None semantics).

Plus one companion positive get_user test for paired-with-T-C sanity.

ASYNC INVOCATION PATTERN (Revision MUST-1): tests use plain `def test_*(...)`
with `asyncio.run(_oauth_complete_login(...))` rather than @pytest.mark.asyncio
+ async def. The repo has NO pytest-asyncio in requirements*.txt or
pyproject.toml (verified via grep on 2026-05-15), and the existing async-aware
tests at tests/test_refresh_lock_per_session.py test SYNC code paths. Adding
pytest-asyncio as a new dependency just for this file is unwarranted scope
expansion; asyncio.run() is the standard library pattern and works without
any pytest configuration changes.

NO TOP-LEVEL PYTEST IMPORT (NEW-H3 round-2 cross-AI catch): pytest is NOT
imported at module top. The `monkeypatch` fixture is auto-injected by
pytest as a fixture NAME at test discovery -- the name resolves via pytest's
fixture system without needing the module imported. `pytest` would be
unused at module scope, and ruff F401 (covered by Plan 91-01's verify
step) would fail at plan boundary if it were imported. If a future test
in this file needs `pytest.raises` / `pytest.fixture` / `pytest.mark.*`,
ADD the import at that time.

Mirrors Phase 87 B3 monkeypatch pattern from tests/test_browse_state.py and
Phase 90 D-17 instance-isolated SimpleNamespace stubs from
tests/test_refresh_lock_per_session.py. No threading needed -- each test is
single-flow.

Test isolation rules (per D-08 + Phase 88 D-01 + D-02 + Refinement 6):
  - Each test instantiates its own SimpleNamespace storage stub via the
    `monkeypatch` fixture; no module-level shared state.
  - Use the STRING form of `monkeypatch.setattr('web.safe_storage.app', ...)`
    -- NOT `monkeypatch.setattr(safe_storage, 'app', ...)`. The string form
    redirects the bound name inside the imported module so the next call
    to `safe_user_get/set/pop` sees the stub.
  - For prune-race scenarios, raise AssertionError with the NiceGUI prune
    message format (matches nicegui/storage.py:121 actual exception text).
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock


# Shared prune-error message that mirrors NiceGUI's actual exception text.
_PRUNE_MSG = (
    'user storage for 6432b6d0-538a-4129-90a3-3ba9a6085e93 should be '
    'created before accessing it'
)


def _build_pruned_storage():
    """Storage stub where every access raises the NiceGUI prune AssertionError.

    Used by T-A and T-C. Each test calls this fresh to avoid shared state.
    """
    storage = MagicMock()
    storage.__setitem__.side_effect = AssertionError(_PRUNE_MSG)
    storage.__getitem__.side_effect = AssertionError(_PRUNE_MSG)
    storage.get.side_effect = AssertionError(_PRUNE_MSG)
    storage.pop.side_effect = AssertionError(_PRUNE_MSG)
    return storage


class _RoutingStorage(dict):
    """Dict subclass that routes writes by key.

    Used by T-D and T-E. A subset of keys can be configured to raise
    AssertionError on write; all others behave as a normal dict. Reads
    (get / __getitem__) are normal dict semantics. Pop semantics are
    normal dict semantics -- the stale-key cleanup paths (SYMMETRIC
    rollback in set_auth, DEFENSIVE cleanup in _oauth_complete_login)
    pop using safe_user_pop which calls .pop with a default.

    Constructor signature (verified against NEW-H4 + NEW-H5 canonical
    AFTER blocks in 91-CODEX-HFIXES.md):
        _RoutingStorage(initial_dict={}, fail_writes_for={'key1', ...})

    Example NEW-H4: `_RoutingStorage({'auth_profile': {'role': 'admin'}},
                                     fail_writes_for={'auth_profile'})`
    pre-seeds a stale admin profile from a prior session, then raises
    AssertionError when set_auth tries to write a new auth_profile.

    Example NEW-H5: `_RoutingStorage({'auth_user': {...}, 'auth_profile':
                                     {...}}, fail_writes_for={'auth_user'})`
    pre-seeds stale prior-session user + profile, then raises
    AssertionError when set_auth tries to write a new auth_user.
    """
    def __init__(self, *args, fail_writes_for=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._fail_writes_for = set(fail_writes_for or ())

    def __setitem__(self, key, value):
        if key in self._fail_writes_for:
            raise AssertionError(_PRUNE_MSG)
        super().__setitem__(key, value)


# ---------------------------------------------------------------------------
# T-A: prune-pre-write -> friendly error path (D-08)
# ---------------------------------------------------------------------------

def test_oauth_callback_prune_pre_write_shows_error(monkeypatch):
    """T-A (D-08): Storage prune races during the first safe_user_set call.

    safe_user_set returns False -> _oauth_complete_login calls show_error_fn
    with the friendly message, emits login_failed posthog, and returns
    WITHOUT calling ui.navigate.to. No AssertionError propagates to caller.
    """
    storage = _build_pruned_storage()
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    # Mock NiceGUI surface that _oauth_complete_login touches.
    nav_mock = MagicMock()
    monkeypatch.setattr('web.main.ui.navigate.to', nav_mock)
    posthog_mock = MagicMock()
    monkeypatch.setattr('web.main.posthog_capture', posthog_mock)
    # Patch asyncio.sleep to no-op so the test completes immediately when
    # the happy-path branch is taken (defensive; T-A should NOT reach it).
    monkeypatch.setattr('web.main.asyncio.sleep', AsyncMock())

    # Late import: monkeypatch is in effect before the helper resolves
    # web.safe_storage.app on its first call.
    from web.main import _oauth_complete_login

    show_error_calls = []
    def show_error_fn(message):
        show_error_calls.append(message)

    status_label_stub = SimpleNamespace(text='')

    # Must NOT raise AssertionError -- the entire point of safe_user_set's
    # return-False contract.
    asyncio.run(_oauth_complete_login(
        user={'id': 'u1', 'email': 'a@b.c'},
        profile={'username': 'u1'},
        session={'access_token': 'at', 'refresh_token': 'rt'},
        status_label=status_label_stub,
        show_error_fn=show_error_fn,
    ))

    nav_mock.assert_not_called()
    assert show_error_calls, "show_error_fn was never called"
    assert 'Session storage unavailable' in show_error_calls[0], (
        f"Expected friendly error message, got: {show_error_calls[0]!r}"
    )
    posthog_mock.assert_any_call('login_failed', {
        'reason': 'session_storage_unavailable',
        'method': 'google_oauth',
    })
    assert 'Login successful' not in status_label_stub.text, (
        f"status_label was prematurely updated: {status_label_stub.text!r}"
    )


# ---------------------------------------------------------------------------
# T-B: happy-path -> all 3 keys + navigate (D-08)
# ---------------------------------------------------------------------------

def test_oauth_callback_happy_path_persists_keys_and_navigates(monkeypatch):
    """T-B (D-08): Storage is healthy. All 3 keys persisted, navigate called."""
    storage = {}
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

    user = {'id': 'u1', 'email': 'a@b.c'}
    profile = {'username': 'u1'}
    session = {'access_token': 'at', 'refresh_token': 'rt'}

    asyncio.run(_oauth_complete_login(
        user=user, profile=profile, session=session,
        status_label=status_label_stub, show_error_fn=show_error_fn,
    ))

    assert storage.get('auth_user') == user, f"auth_user mismatch: {storage}"
    assert storage.get('auth_profile') == profile, f"auth_profile mismatch: {storage}"
    assert storage.get('auth_session') == {
        'access_token': 'at',
        'refresh_token': 'rt',
    }, f"auth_session mismatch: {storage}"
    nav_mock.assert_called_once_with('/')
    assert not show_error_calls, f"show_error_fn was unexpectedly called: {show_error_calls}"
    posthog_mock.assert_any_call('login_success', {'method': 'google_oauth'})
    assert 'Login successful' in status_label_stub.text, (
        f"status_label.text not updated: {status_label_stub.text!r}"
    )


# ---------------------------------------------------------------------------
# T-C: get_user() / get_profile() under pruned storage -> None, no raise (D-08)
# ---------------------------------------------------------------------------

def test_get_user_under_pruned_storage_returns_none(monkeypatch):
    """T-C (Codex M3 reshape, D-08): the whole point of AUTHW-01 for readers."""
    storage = _build_pruned_storage()
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    from web.auth_state import GlobalAuthState

    assert GlobalAuthState.get_user() is None
    assert GlobalAuthState.get_profile() is None


def test_get_user_returns_value_on_healthy_storage(monkeypatch):
    """T-C companion (positive case): with a healthy storage dict, get_user
    returns the value -- sanity that migration didn't break the happy-path read.
    """
    storage = {
        'auth_user': {'id': 'u1', 'email': 'a@b.c'},
        'auth_profile': {'username': 'u1'},
    }
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    from web.auth_state import GlobalAuthState
    assert GlobalAuthState.get_user() == {'id': 'u1', 'email': 'a@b.c'}
    assert GlobalAuthState.get_profile() == {'username': 'u1'}


# ---------------------------------------------------------------------------
# T-D: set_auth SYMMETRIC partial-write rollback (Revision MUST-3, Codex HIGH;
#      NEW-H4 -- pre-seeded stale auth_profile from prior session)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# T-E: _oauth_complete_login DEFENSIVE 3-key cleanup (Revision MUST-3;
#      NEW-H5 -- pre-seeded stale auth_user + auth_profile from prior session)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# T-F: set_auth(profile=None) clears stale profile (Revision MUST-3)
# ---------------------------------------------------------------------------

def test_set_auth_profile_is_none_clears_stale_profile(monkeypatch):
    """T-F (Revision MUST-3): a new login with profile=None must clear any
    stale auth_profile from a prior session (Revision MUST-2 semantics).

    Without this, GlobalAuthState.get_role()/is_admin()/is_editor() could
    leak admin role from a previous user's session into the new login.
    Codex HIGH catch: role checks read profile INDEPENDENTLY of user.
    """
    # Pre-populated storage with stale admin profile from a prior session.
    storage = {'auth_profile': {'role': 'admin', 'username': 'old_admin'}}
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=storage)),
    )

    from web.auth_state import GlobalAuthState

    new_user = {'id': 'u2', 'email': 'new@b.c'}
    result = GlobalAuthState.set_auth(new_user, profile=None)

    assert result is True, "set_auth should succeed when only user is written"
    assert storage.get('auth_user') == new_user, (
        f"new user not written: {storage}"
    )
    assert 'auth_profile' not in storage, (
        f"stale auth_profile NOT cleared (Revision MUST-2 profile-is-None "
        f"semantics fail): {storage}"
    )

    # get_role() should now return the default, not the stale admin role.
    role = GlobalAuthState.get_role()
    assert role != 'admin', (
        f"stale admin role leaked through get_role() after profile=None login: {role}"
    )
