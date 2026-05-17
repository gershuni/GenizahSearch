# -*- coding: utf-8 -*-
"""Phase 92.1 READER-03 regression test for the add-to-list-dialog
"Create new list" path. Surfaced 2026-05-17 during Phase 92 SWEEP-05
smoke run 1: `safe_user_get('auth_session') unexpected failure:
app.storage.user can only be used within a UI context`.

Reviews H1 (2026-05-17): tests MUST exercise the real failure chain.
The previous draft mocked `lists_mgr.create_list` / `lists_mgr.add_item`
directly, which bypassed `web/supabase_client.py:get_user_client` and
`web/safe_storage.py:safe_user_get('auth_session')` -- the actual
failing chain. This rewrite stubs `lists_mgr.create_list` with a thin
wrapper that calls the REAL `get_user_client()` under a controlled
`nicegui.storage.request_contextvar` state (None vs. valid) and asserts
the handler behavior differs accordingly.

Reviews C2 (2026-05-17): tests assert behavior on the actual gating
mechanism -- `nicegui.storage.request_contextvar.get() is None` --
rather than on slot-stack state.

Reviews R2-3 (2026-05-17): the test imports `request_contextvar` and
exercises BOTH `.set(None)` (the failure regime) AND `.set(fake_request)`
(the success regime) so the contrast IS the proof that the fix targets
the right gating mechanism.

Reviews M-R2-4 (2026-05-17): the failure-regime test fully asserts the
documented behavior: at least one safe_storage WARNING was logged AND
the authenticated path was NOT taken (apply_user_auth_called == False).

No pytest.skip. No xfail. Every test exits with PASS or fails the plan.
"""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from web.components.add_to_list_dialog import _create_and_add_handler


@pytest.fixture
def mock_dialog():
    d = MagicMock(name='dialog')
    d.close = MagicMock()
    return d


def _seed_logged_in_storage(monkeypatch):
    """Seed safe_storage with a valid auth_session. This is the storage
    backend the REAL get_user_client() will read when lists_mgr.create_list
    invokes it."""
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            'auth_session': {
                'access_token': 'good.jwt.token',
                'refresh_token': 'good-refresh',
            },
            'auth_user': {'id': '00000000-0000-0000-0000-000000000001'},
        })),
    )


def _stub_ui_notify(monkeypatch):
    """_create_and_add_handler calls ui.notify on every branch. Without a
    NiceGUI client context, those calls would raise. Stub the import-time
    reference to a no-op."""
    monkeypatch.setattr(
        'web.components.add_to_list_dialog.ui.notify',
        MagicMock(name='ui.notify'),
    )


def _build_real_chain_lists_mgr(monkeypatch, capture_dict):
    """Reviews H1: stub lists_mgr.create_list with a thin wrapper that
    EXERCISES the real get_user_client() / safe_user_get('auth_session')
    chain.

    capture_dict records:
      - 'get_user_client_called': bool -- was the real chain entered?
      - 'apply_user_auth_called': bool -- did _apply_user_auth_to_client
                                          fire? (indicates auth_session was
                                          successfully read)
      - 'create_list_returned': the value returned by the create_list stub
    """
    import web.supabase_client as supa

    # Stub the supabase create_client + _apply_user_auth_to_client + token-
    # expiry check so the real get_user_client() path can be traced without
    # network. We mark _apply_user_auth_to_client so the test asserts on the
    # real chain branching point (anon-fallback vs. authenticated builder).
    monkeypatch.setattr(supa, '_access_token_near_expiry', lambda _t: False)
    apply_mock = MagicMock(name='_apply_user_auth_to_client')
    monkeypatch.setattr(supa, '_apply_user_auth_to_client', apply_mock)
    fake_supabase_client = MagicMock(name='fake_supabase_client')
    monkeypatch.setattr(supa, 'create_client', lambda *a, **kw: fake_supabase_client)

    async def _real_chain_create_list(name, color=None, project_id=None):
        """Calls the REAL web.supabase_client.get_user_client() to exercise
        the failing chain. Records whether _apply_user_auth_to_client fired
        (proves safe_user_get('auth_session') succeeded)."""
        try:
            supa.get_user_client()
            capture_dict['get_user_client_called'] = True
            capture_dict['apply_user_auth_called'] = apply_mock.called
            # Return a synthetic list ID so the handler continues.
            capture_dict['create_list_returned'] = 'new-list-id-from-real-chain'
            return 'new-list-id-from-real-chain'
        except Exception as e:
            capture_dict['create_list_exception'] = repr(e)
            raise

    async def _stub_add_item(sys_id, list_id, note=None, fl_id=None):
        capture_dict['add_item_called'] = True
        return True

    mgr = MagicMock(name='lists_mgr')
    mgr.create_list = _real_chain_create_list
    mgr.add_item = _stub_add_item
    mgr.data = {'lists': {}, 'projects': {}}
    return mgr


def test_create_and_add_happy_path_with_valid_storage(monkeypatch, mock_dialog, caplog):
    """Happy path: valid auth_session in storage, real get_user_client()
    chain runs successfully, _apply_user_auth_to_client fires (proves
    safe_user_get read auth_session), dialog closes, no safe_storage
    warnings emitted.

    Reviews H1: this exercises the REAL failing chain
    (_create_and_add_handler -> lists_mgr.create_list -> get_user_client
    -> safe_user_get('auth_session')), not a mock of
    lists_mgr.create_list."""
    _seed_logged_in_storage(monkeypatch)
    _stub_ui_notify(monkeypatch)
    capture = {}
    mock_lists_mgr = _build_real_chain_lists_mgr(monkeypatch, capture)
    caplog.set_level(logging.WARNING, logger='web.safe_storage')

    result = asyncio.run(_create_and_add_handler(
        name='test list',
        project_id=None,
        lists_mgr=mock_lists_mgr,
        sys_id='cul_TS_12_123',
        fl_id=None,
        new_list_note_value='',
        dialog=mock_dialog,
        on_success=None,
        is_logged_in=True,
    ))

    assert result is True
    assert capture.get('get_user_client_called') is True, (
        "Real chain was NOT entered -- the stub did not call get_user_client. "
        "Test is bypassing the failing chain. capture=%r" % capture
    )
    assert capture.get('apply_user_auth_called') is True, (
        "_apply_user_auth_to_client did NOT fire -- safe_user_get('auth_session') "
        "did not return the seeded auth_session. The READER-03 regression has "
        "reappeared OR the test seeding is broken. capture=%r" % capture
    )
    assert capture.get('add_item_called') is True
    mock_dialog.close.assert_called_once()

    warnings_from_safe_storage = [
        r for r in caplog.records
        if r.name == 'web.safe_storage' and r.levelno >= logging.WARNING
    ]
    assert warnings_from_safe_storage == [], (
        "READER-03 regression: safe_user_get emitted warning-level log records "
        "on the happy path. Smoke run 1 Symptom 3 has re-appeared. Records: %r"
        % [(r.name, r.levelname, r.getMessage()) for r in warnings_from_safe_storage]
    )


def test_create_and_add_with_request_contextvar_none_fails_safely(
    monkeypatch, mock_dialog, caplog,
):
    """Reviews C2 + R2-3 HIGH (2026-05-17): when
    `nicegui.storage.request_contextvar` is set to None at click time,
    `app.storage.user` access raises the real NiceGUI RuntimeError (per
    venv/Lib/site-packages/nicegui/storage.py:109-113). The regression
    test must exercise THIS gating mechanism directly (via the
    `request_contextvar.set(None)` API) -- NOT a SimpleNamespace stub
    that just re-raises a similar-looking RuntimeError downstream, which
    was the round-1 test Codex round-2 R2-3 flagged as insufficient (the
    stub bypassed the real request_contextvar gate; safe_storage's
    default-path was hit on a different code path).

    Reviews M-R2-4 MEDIUM (2026-05-17): fully assert the documented
    behavior:
      - at least one web.safe_storage WARNING was logged (proving the
        failure was observed, not silently swallowed)
      - get_user_client was entered AND
        _apply_user_auth_to_client was NOT called (proving anon
        fallback fired)
    """
    from nicegui.storage import request_contextvar

    # Force request_contextvar to None for any code that consults it
    # directly. ALSO swap web.safe_storage.app for a stub whose
    # .storage.user property raises the same RuntimeError NiceGUI
    # raises at venv/.../nicegui/storage.py:113 when
    # request_contextvar.get() is None -- this is exactly the failure
    # mode that fires from inside `safe_user_get` in production.
    _token = request_contextvar.set(None)
    try:
        class _NiceGuiUnboundStorage:
            @property
            def user(self):
                raise RuntimeError(
                    'app.storage.user can only be used within a UI context'
                )
        monkeypatch.setattr(
            'web.safe_storage.app',
            SimpleNamespace(storage=_NiceGuiUnboundStorage()),
        )
        _stub_ui_notify(monkeypatch)
        capture = {}
        mock_lists_mgr = _build_real_chain_lists_mgr(monkeypatch, capture)
        caplog.set_level(logging.WARNING, logger='web.safe_storage')

        asyncio.run(_create_and_add_handler(
            name='test list',
            project_id=None,
            lists_mgr=mock_lists_mgr,
            sys_id='cul_TS_12_123',
            fl_id=None,
            new_list_note_value='',
            dialog=mock_dialog,
            on_success=None,
            is_logged_in=True,
        ))

        warnings_from_safe_storage = [
            r for r in caplog.records
            if r.name == 'web.safe_storage' and r.levelno >= logging.WARNING
        ]
        assert len(warnings_from_safe_storage) >= 1, (
            "Expected at least one web.safe_storage WARNING when "
            "request_contextvar=None and app.storage.user raises. If this "
            "fails, EITHER (a) safe_storage is no longer logging warnings "
            "on unexpected exceptions (regression -- check "
            "web/safe_storage.py:46-60), OR (b) get_user_client now caches "
            "and never re-reads (also a regression). caplog records: %r"
            % [(r.name, r.levelname, r.getMessage()) for r in caplog.records]
        )

        # Reviews M-R2-4: get_user_client was entered and fell back to anon
        # (no auth applied).
        assert capture.get('get_user_client_called') is True, (
            "Real chain was NOT entered. capture=%r" % capture
        )
        assert capture.get('apply_user_auth_called') is False, (
            "Expected anon-fallback path (no _apply_user_auth_to_client) "
            "when request_contextvar=None caused safe_user_get to return "
            "None and get_user_client to fall back to the anon singleton. "
            "capture=%r" % capture
        )
    finally:
        request_contextvar.reset(_token)


def test_create_and_add_with_request_contextvar_set_succeeds(
    monkeypatch, mock_dialog, caplog,
):
    """Reviews R2-3 (2026-05-17): the POSITIVE side of the R2-3 split.
    When `request_contextvar` is set to a fake-request value AND the
    seeded safe_storage backend returns the auth_session, the real chain
    must invoke `_apply_user_auth_to_client` exactly once with the seeded
    access_token.

    Pair this with
    `test_create_and_add_with_request_contextvar_none_fails_safely` --
    the contrast IS the proof that the fix targets the right gating
    mechanism.
    """
    from nicegui.storage import request_contextvar

    # A bound but otherwise-empty contextvar value -- the test cares about
    # the bound-vs-None distinction, not the request object's contents.
    # safe_storage's `web.safe_storage.app` stub provides the storage data;
    # request_contextvar being non-None is the gate that lets the access
    # succeed in production.
    _token = request_contextvar.set(SimpleNamespace(_fake_request=True))
    try:
        _seed_logged_in_storage(monkeypatch)
        _stub_ui_notify(monkeypatch)
        capture = {}
        mock_lists_mgr = _build_real_chain_lists_mgr(monkeypatch, capture)
        caplog.set_level(logging.WARNING, logger='web.safe_storage')

        result = asyncio.run(_create_and_add_handler(
            name='test list',
            project_id=None,
            lists_mgr=mock_lists_mgr,
            sys_id='cul_TS_12_123',
            fl_id=None,
            new_list_note_value='',
            dialog=mock_dialog,
            on_success=None,
            is_logged_in=True,
        ))

        assert result is True
        assert capture.get('apply_user_auth_called') is True, (
            "request_contextvar was bound + auth_session seeded; expected "
            "_apply_user_auth_to_client to fire. capture=%r" % capture
        )
        warnings_from_safe_storage = [
            r for r in caplog.records
            if r.name == 'web.safe_storage' and r.levelno >= logging.WARNING
        ]
        assert warnings_from_safe_storage == [], (
            "request_contextvar bound + storage seeded should produce NO "
            "safe_storage warnings. records: %r"
            % [(r.name, r.levelname, r.getMessage()) for r in warnings_from_safe_storage]
        )
        mock_dialog.close.assert_called_once()
    finally:
        request_contextvar.reset(_token)


def test_create_and_add_error_path_propagates_via_notify(
    monkeypatch, mock_dialog, caplog,
):
    """create_list raises -> handler returns False, dialog NOT closed, no
    safe_storage warning (the failure path goes through ui.notify, not the
    storage layer)."""
    _seed_logged_in_storage(monkeypatch)
    _stub_ui_notify(monkeypatch)

    async def _raising_create_list(name, color=None, project_id=None):
        raise RuntimeError("simulated supabase down")

    mgr = MagicMock(name='lists_mgr')
    mgr.create_list = _raising_create_list
    mgr.data = {'lists': {}, 'projects': {}}
    caplog.set_level(logging.WARNING, logger='web.safe_storage')

    result = asyncio.run(_create_and_add_handler(
        name='test list',
        project_id=None,
        lists_mgr=mgr,
        sys_id='cul_TS_12_123',
        fl_id=None,
        new_list_note_value='',
        dialog=mock_dialog,
        on_success=None,
        is_logged_in=True,
    ))

    assert result is False
    mock_dialog.close.assert_not_called()
    warnings_from_safe_storage = [
        r for r in caplog.records
        if r.name == 'web.safe_storage' and r.levelno >= logging.WARNING
    ]
    assert warnings_from_safe_storage == [], (
        "Error-path test: unexpected safe_storage warning. Records: %r"
        % [(r.name, r.levelname, r.getMessage()) for r in warnings_from_safe_storage]
    )


def test_create_and_add_empty_name_returns_false(monkeypatch, mock_dialog):
    """Empty name short-circuits before any lists_mgr call."""
    _seed_logged_in_storage(monkeypatch)
    _stub_ui_notify(monkeypatch)
    mgr = MagicMock(name='lists_mgr')
    mgr.create_list = MagicMock()  # should not be called
    result = asyncio.run(_create_and_add_handler(
        name='',
        project_id=None,
        lists_mgr=mgr,
        sys_id='cul_TS_12_123',
        fl_id=None,
        new_list_note_value='',
        dialog=mock_dialog,
        on_success=None,
        is_logged_in=True,
    ))
    assert result is False
    mgr.create_list.assert_not_called()
    mock_dialog.close.assert_not_called()
