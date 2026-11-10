# -*- coding: utf-8 -*-
"""/puzzle "Add from List" shows a signed-in visitor's own lists and items.

2026-10-20 (RLS-06). The picker offloaded ``get_user_lists`` and
``get_list_items`` with ``run.io_bound`` and no client. The worker has no
NiceGUI request, so ``get_user_client()`` there returned the anonymous
singleton, the owner-only tables answered ``[]``, and every signed-in visitor
was told "No lists found". The handler now builds the client on the event loop
and passes it in.

Drives the real page with a NiceGUI ``User`` browser (the harness in
tests/test_web_saved_joins_isolation.py) and the REAL ``get_user_client`` and
storage chokepoint; only the Supabase clients are fakes that answer by role.
``UserInteraction`` calls handlers without setting ``request_contextvar``, which
NiceGUI's own event dispatch does (``Element._handle_event``), so the driver
sets it to the browser's request first -- what production does on every click.
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
from unittest.mock import MagicMock, patch

import pytest

USER_ID = 'user-a'


def _b64(d):
    return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b'=').decode()


USER_TOKEN = f"{_b64({'alg': 'HS256'})}.{_b64({'sub': USER_ID, 'exp': int(time.time()) + 3600})}.sig"


class _Resp:
    def __init__(self, data):
        self.data = data
        self.count = len(data)


class _Query:
    def __init__(self, client, table):
        self.client, self.table = client, table

    def __getattr__(self, name):
        # select / eq / is_ / order / ...: filters, no effect here.
        return lambda *a, **k: self

    def execute(self):
        self.client.log.append((self.table, self.client.role))
        if self.client.role == 'USER' and self.table == 'user_lists':
            return _Resp([{'id': 7, 'name': 'My list'}])
        if self.client.role == 'USER' and self.table == 'list_items':
            return _Resp([{'id': 1, 'sys_id': '990001', 'shelfmark': 'T-S 1.1'}])
        # Owner-only tables give anon nothing; the page's other reads get no rows.
        return _Resp([])


class _RoleClient:
    def __init__(self, role, log):
        self.role, self.log = role, log

    def table(self, name):
        return _Query(self, name)

    def __getattr__(self, name):
        return MagicMock()  # rpc / storage / auth: not what this test is about


@pytest.fixture
def role_clients(monkeypatch, tmp_path):
    import shared.puzzle_service as ps
    import web.supabase_client as sc

    svc = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, '_service_instance', svc)
    log = []
    anon = _RoleClient('ANON', log)
    monkeypatch.setattr(sc, 'get_client', lambda: anon)
    monkeypatch.setattr(sc, 'create_client', lambda url, key: _RoleClient('UNAUTHED', log))

    def _apply(client, token):
        client.role = 'USER' if token == USER_TOKEN else 'OTHER'
    monkeypatch.setattr(sc, '_apply_user_auth_to_client', _apply)
    yield log
    if svc._conn is not None:
        svc._conn.close()


def _sign_in(user):
    """Store a signed-in session for this browser, as do_login does."""
    from nicegui.storage import request_contextvar
    from web.safe_storage import ensure_session_uuid, safe_user_set
    token = request_contextvar.set(user._client.request)
    try:
        with user._client:
            ensure_session_uuid()
            assert safe_user_set('auth_session', {'access_token': USER_TOKEN, 'refresh_token': 'rt-a'})
            assert safe_user_set('auth_user', {'id': USER_ID})
    finally:
        request_contextvar.reset(token)


def test_add_from_list_shows_the_signed_in_visitors_lists_and_items(role_clients):
    from nicegui import ui
    from nicegui.storage import request_contextvar
    from nicegui.testing.user_interaction import UserInteraction

    from tests.test_web_saved_joins_isolation import _run, _texts

    log = role_clients
    shown = {}

    def _elements(user, kind, pred=lambda el: True):
        return [el for el in user._client.elements.values() if isinstance(el, kind) and pred(el)]

    async def _wait(pred, timeout=10.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while not pred() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        return pred()

    async def driver(a, b):
        await a.open('/puzzle')
        _sign_in(a)
        # What Element._handle_event does before every handler in production.
        request_contextvar.set(a._client.request)

        button = _elements(a, ui.button, lambda el: el.props.get('icon') == 'list')
        assert button, '"Add from List" button not found'
        UserInteraction(a, {button[0]}, None).click()

        def _picker():
            return _elements(a, ui.select, lambda el: el.options == {7: 'My list'})
        await _wait(lambda: _picker() or 'No lists found' in a.notify.messages)
        shown['messages'] = list(a.notify.messages)
        picker = _picker()
        shown['picker'] = bool(picker)
        if not picker:
            return

        # What the browser sends on picking the first option (NiceGUI stores
        # the page's 'update:model-value' listener under the camelCase name).
        UserInteraction(a, {picker[0]}, None).trigger('update:modelValue', {'value': 0, 'label': 'My list'})
        await _wait(lambda: 'T-S 1.1' in _texts(a) or 'Empty list' in _texts(a))
        shown['items'] = 'T-S 1.1' in _texts(a)
        shown['empty'] = 'Empty list' in _texts(a)

    with patch('shared.puzzle_publish_service.get_published_join_detail', return_value=None), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)

    assert 'No lists found' not in shown['messages'], (
        'a signed-in visitor was told "No lists found": the lists read ran without '
        f'the user\'s client (reads: {log})')
    assert shown['picker'], f'the list picker did not open (messages: {shown["messages"]})'
    assert shown['items'] and not shown['empty'], f'the chosen list showed no items (reads: {log})'
    assert ('user_lists', 'USER') in log and ('list_items', 'USER') in log, log
    assert ('user_lists', 'ANON') not in log and ('list_items', 'ANON') not in log, log


def test_a_pick_after_an_account_switch_reads_nothing(role_clients):
    """Codex 2026-10-21 (P1): the picker kept the client it opened with, so a
    pick after signing out and in as someone else (another tab) read the first
    account's private items with its still-valid token. The account is checked
    again on each pick."""
    from nicegui import ui
    from nicegui.storage import request_contextvar
    from nicegui.testing.user_interaction import UserInteraction

    from tests.test_web_saved_joins_isolation import _run

    log = role_clients
    shown = {}

    def _elements(user, kind, pred=lambda el: True):
        return [el for el in user._client.elements.values() if isinstance(el, kind) and pred(el)]

    async def _wait(pred, timeout=10.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while not pred() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        return pred()

    async def driver(a, b):
        from web.safe_storage import safe_user_set

        await a.open('/puzzle')
        _sign_in(a)
        request_contextvar.set(a._client.request)

        button = _elements(a, ui.button, lambda el: el.props.get('icon') == 'list')
        UserInteraction(a, {button[0]}, None).click()

        def _picker():
            return _elements(a, ui.select, lambda el: el.options == {7: 'My list'})
        shown['picker'] = await _wait(lambda: bool(_picker()))
        if not shown['picker']:
            return
        with a._client:
            assert safe_user_set('auth_user', {'id': 'user-b'})  # signed in as someone else
        del log[:]

        picker = _picker()[0]
        dialog = next(d for d in _elements(a, ui.dialog) if picker in list(d.descendants()))
        shown['open_before'] = dialog.value
        UserInteraction(a, {picker}, None).trigger('update:modelValue', {'value': 0, 'label': 'My list'})
        shown['closed'] = await _wait(lambda: dialog.value is False)
        await asyncio.sleep(0.3)

    with patch('shared.puzzle_publish_service.get_published_join_detail', return_value=None), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)

    assert shown['picker'], 'the list picker did not open'
    assert shown['open_before'] is True
    assert not [entry for entry in log if entry[0] == 'list_items'], (
        f'a pick after the account changed still read list items: {log}')
    assert shown['closed'], 'the picker stayed open for the new account'
