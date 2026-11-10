# -*- coding: utf-8 -*-
"""A user-scoped Supabase call in a worker thread must carry the user's client.

2026-10-20 (RLS-06). NiceGUI's ``run.io_bound`` submits with
``loop.run_in_executor``, which does not copy contextvars, so in the worker
``app.storage.user`` raises, ``safe_user_get`` returns ``{}`` and
``get_user_client()`` hands back the ANONYMOUS singleton. Owner-only tables then
answer ``[]`` and owner-only updates change 0 rows, with no error anywhere:

* /puzzle "Add from List" always said "No lists found" for a signed-in user
  (``get_user_lists`` / ``get_list_items`` offloaded with no client);
* web sign-up never saved the chosen username: ``update_profile`` ran in a
  worker AND before the account had a session, so it was anonymous twice over.
  With email confirmation (production) there is no session at sign-up at all;
  the database trigger writes it from the sign-up metadata (2026-10-21; the
  last section).

These tests use the REAL ``get_user_client``, ``web.safe_storage`` and NiceGUI
``app.storage.user`` (with a storage secret set, the production branch), a fake
request in ``request_contextvar`` the way NiceGUI's event dispatch sets it, and
fake Supabase clients that answer by role. No network.

The static guard for the call sites is in tests/test_offloaded_readers.py
(``test_no_user_scoped_helper_is_offloaded_without_a_client``); the page itself
is driven in tests/test_puzzle_add_from_list_signed_in.py.
"""
from __future__ import annotations

import asyncio
import base64
import json
import time

import pytest
from nicegui import app, run
from nicegui import core as nicegui_core
from nicegui import storage as nicegui_storage

import web.supabase_client as sc


def _b64(d):
    return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b'=').decode()


def _jwt(sub, exp_in):
    return f"{_b64({'alg': 'HS256'})}.{_b64({'sub': sub, 'exp': int(time.time()) + exp_in})}.sig"


USER_ID = 'user-a'
USER_TOKEN = _jwt(USER_ID, 3600)

# Tables whose RLS lets only the owner read or change rows: anon gets nothing.
_OWNER_ONLY = {'user_lists', 'list_items', 'recent_items', 'profiles'}


class _Resp:
    def __init__(self, data):
        self.data = data
        self.count = len(data)


class _Query:
    def __init__(self, client, table):
        self.client, self.table, self.op, self.payload = client, table, 'select', None

    def update(self, data):
        self.op, self.payload = 'update', data
        return self

    def insert(self, data):
        self.op, self.payload = 'insert', data
        return self

    def delete(self):
        self.op = 'delete'
        return self

    def __getattr__(self, name):
        # select / eq / is_ / in_ / order / limit / single: filters, no effect here.
        return lambda *a, **k: self

    def execute(self):
        self.client.log.append((self.table, self.op, self.client.role))
        if self.client.role != 'USER' and self.table in _OWNER_ONLY:
            return _Resp([])
        if self.table == 'user_lists':
            return _Resp([{'id': 7, 'name': 'My list'}])
        if self.table == 'list_items':
            return _Resp([{'id': 1, 'sys_id': '990001', 'shelfmark': 'T-S 1.1'}])
        if self.table == 'profiles' and self.op == 'update':
            return _Resp([{'id': USER_ID, **self.payload}])
        return _Resp([{'id': 1}])


class _RoleClient:
    def __init__(self, role, log):
        self.role, self.log = role, log

    def table(self, name):
        return _Query(self, name)


class _FakeRequest:
    def __init__(self, session_id):
        self.session = {'id': session_id}


@pytest.fixture
def harness(monkeypatch):
    """Role-aware fake clients behind the real get_user_client.

    Returns (log, enter_session). ``enter_session(sid, data)`` seeds that
    session's user storage and makes it the current request -- call it inside
    the coroutine, so the contextvar stays in that asyncio.run's context.
    """
    log = []
    anon = _RoleClient('ANON', log)
    monkeypatch.setattr(sc, 'get_client', lambda: anon)
    monkeypatch.setattr(sc, 'create_client', lambda url, key: _RoleClient('UNAUTHED', log))

    def _apply(client, token):
        client.role = 'USER' if token == USER_TOKEN else 'OTHER'
    monkeypatch.setattr(sc, '_apply_user_auth_to_client', _apply)
    # Production runs with a secret, which takes safe_user_get's quiet
    # 'UI context' branch rather than the 'needs a storage_secret' one.
    monkeypatch.setattr(nicegui_storage.Storage, 'secret', 's' * 40)
    # Under pytest NiceGUI is in script mode before ui.run (conftest touches
    # the slot stack), and app.storage.user is then an empty stand-in for
    # everyone. Production is never in that state.
    monkeypatch.setattr(nicegui_core, 'script_mode', False)

    def enter_session(session_id, data):
        monkeypatch.setitem(app.storage._users, session_id, dict(data))
        nicegui_storage.request_contextvar.set(_FakeRequest(session_id))

    return log, enter_session


_SIGNED_IN = {
    'auth_session': {'access_token': USER_TOKEN, 'refresh_token': 'rt-a'},
    'auth_user': {'id': USER_ID},
    '_session_uuid': 'a' * 32,
}


def _role(client):
    return getattr(client, 'role', repr(client))


# ---------------------------------------------------------------------------
# The mechanism, and the shape that survives it
# ---------------------------------------------------------------------------

def test_get_user_client_in_a_worker_is_anonymous(harness):
    """Why the guard exists: the same call answers differently off the loop.

    If a NiceGUI upgrade starts copying contextvars into run.io_bound workers,
    this fails -- the guard is then still harmless, only no longer necessary.
    """
    _log, enter_session = harness

    async def main():
        enter_session('sess-a', _SIGNED_IN)
        on_loop = sc.get_user_client()
        in_worker = await run.io_bound(sc.get_user_client)
        return _role(on_loop), _role(in_worker)

    assert asyncio.run(main()) == ('USER', 'ANON')


def test_a_client_built_on_the_loop_keeps_the_user_in_the_worker(harness):
    """The /puzzle picker's fixed shape: build on the loop, pass client= in.

    The bare shape it replaces (``run.io_bound(get_user_lists, uid)``) returns
    no lists for the same signed-in visitor.
    """
    log, enter_session = harness

    async def main():
        enter_session('sess-a', _SIGNED_IN)
        bare = await run.io_bound(sc.get_user_lists, USER_ID)
        client = sc.get_user_client()
        lists = await run.io_bound(lambda: sc.get_user_lists(USER_ID, client=client))
        items = await run.io_bound(sc.get_list_items, 7, client=client)
        return bare, lists, items

    bare, lists, items = asyncio.run(main())
    assert bare == [], 'the bare offload was expected to run anonymously'
    assert [lst['name'] for lst in lists] == ['My list']
    assert [it['shelfmark'] for it in items] == ['T-S 1.1']
    assert ('user_lists', 'select', 'USER') in log
    assert ('list_items', 'select', 'USER') in log


# ---------------------------------------------------------------------------
# The helpers honour an explicit client on every path
# ---------------------------------------------------------------------------

def _no_ambient_client(monkeypatch):
    calls = []
    monkeypatch.setattr(sc, 'get_user_client', lambda: calls.append('called') or _RoleClient('ANON', []))
    return calls


def test_get_user_lists_uses_the_given_client(monkeypatch):
    calls = _no_ambient_client(monkeypatch)
    log = []
    rows = sc.get_user_lists(USER_ID, client=_RoleClient('USER', log))
    assert [r['name'] for r in rows] == ['My list']
    assert calls == [], 'get_user_client was called despite an explicit client'


def test_get_user_lists_deleted_at_fallback_uses_the_given_client(monkeypatch):
    """Before the deleted_at migration the first query fails; the retry must
    not quietly swap the caller's client for get_user_client()."""
    calls = _no_ambient_client(monkeypatch)
    log = []

    class _NoDeletedAt(_RoleClient):
        def table(self, name):
            q = _Query(self, name)
            if not any(entry[0] == 'user_lists' for entry in self.log):
                def _fail():
                    self.log.append(('user_lists', 'select-failed', self.role))
                    raise Exception('column user_lists.deleted_at does not exist')
                q.execute = _fail
            return q

    rows = sc.get_user_lists(USER_ID, client=_NoDeletedAt('USER', log))
    assert [r['name'] for r in rows] == ['My list']
    assert [entry[1] for entry in log] == ['select-failed', 'select']
    assert calls == [], 'the fallback re-derived its client'


def test_get_user_lists_default_still_uses_get_user_client(monkeypatch):
    log = []
    monkeypatch.setattr(sc, 'get_user_client', lambda: _RoleClient('USER', log))
    assert [r['name'] for r in sc.get_user_lists(USER_ID)] == ['My list']


def test_update_profile_uses_the_given_client(monkeypatch):
    calls = _no_ambient_client(monkeypatch)
    log = []
    res = sc.update_profile(USER_ID, {'username': 'chosen'}, client=_RoleClient('USER', log))
    assert res == {'success': True, 'profile': {'id': USER_ID, 'username': 'chosen'}}
    assert log == [('profiles', 'update', 'USER')]
    assert calls == []


def test_add_recent_item_uses_the_given_client(monkeypatch):
    calls = _no_ambient_client(monkeypatch)
    log = []
    assert sc.add_recent_item(USER_ID, '990001', client=_RoleClient('USER', log)) == {'success': True}
    assert [entry[1] for entry in log] == ['delete', 'insert', 'select']
    assert {entry[2] for entry in log} == {'USER'}
    assert calls == []


# ---------------------------------------------------------------------------
# Web sign-up (2026-10-21). Production Supabase requires email confirmation,
# so sign_up returns no session and the app cannot write the profile (RLS:
# owner only). The Register form's fields go in the sign-up metadata, which
# the database's new-user trigger writes into the profile
# (migrations/profiles_no_email_username.sql); the app no longer tries.
# ---------------------------------------------------------------------------

TYPED = {'username': 'chosen_name', 'full_name': 'Full Name', 'affiliation': 'Uni'}


def _patch_sign_up(monkeypatch, *, session=None, already_registered=False):
    """Fake Supabase sign-up and do_login; return what each saw, in order."""
    import web.auth_state as auth_state

    seen = {'order': [], 'metadata': None}

    def fake_sign_up(email, password, metadata=None):
        seen['order'].append('sign_up')
        seen['metadata'] = metadata
        return {'success': True, 'user': {'id': USER_ID, 'email': email},
                'session': session, 'already_registered': already_registered}

    async def fake_login(email, password):
        seen['order'].append('login')
        return {'success': True, 'user': {'id': USER_ID}}

    monkeypatch.setattr(auth_state, 'supabase_sign_up', fake_sign_up)
    monkeypatch.setattr(auth_state, 'do_login', fake_login)
    return auth_state, seen


def test_register_without_a_session_asks_to_check_email_and_does_not_sign_in(monkeypatch):
    auth_state, seen = _patch_sign_up(monkeypatch)

    result = asyncio.run(auth_state.do_register('a@b.c', 'chosen_name', 'pw', 'Full Name', 'Uni'))

    assert result == {'success': True, 'check_email': True}
    assert seen['order'] == ['sign_up'], (
        'an unconfirmed account cannot sign in; the attempt only showed "Email not confirmed"')
    assert seen['metadata'] == {'full_name': 'Full Name', 'affiliation': 'Uni', 'signup_profile': TYPED}


def test_register_for_an_address_with_an_account_answers_the_same(monkeypatch):
    """Supabase answers a sign-up for a registered address with a stand-in user
    and no session; any different answer would say who is registered."""
    auth_state, seen = _patch_sign_up(monkeypatch, already_registered=True)

    assert asyncio.run(auth_state.do_register('a@b.c', 'x', 'pw')) == {'success': True, 'check_email': True}
    assert seen['order'] == ['sign_up']


def test_register_with_a_session_signs_in(monkeypatch):
    auth_state, seen = _patch_sign_up(monkeypatch, session={'access_token': 't', 'refresh_token': 'r'})

    result = asyncio.run(auth_state.do_register('a@b.c', 'chosen_name', 'pw'))

    assert seen['order'] == ['sign_up', 'login']
    assert result == {'success': True, 'user': {'id': USER_ID}}


@pytest.mark.parametrize('identities, expected', [([], True), (None, False), (['email'], False)])
def test_sign_up_flags_an_address_that_already_has_an_account(monkeypatch, identities, expected):
    from types import SimpleNamespace

    user = SimpleNamespace(id='u', email='a@b.c', created_at=None, updated_at=None,
                           user_metadata={}, identities=identities)
    sent = {}

    class _Auth:
        def sign_up(self, payload):
            sent.update(payload)
            return SimpleNamespace(user=user, session=None)

    monkeypatch.setattr(sc, 'create_client', lambda url, key: SimpleNamespace(auth=_Auth()))

    result = sc.sign_up('a@b.c', 'pw', {'signup_profile': TYPED})

    assert result['already_registered'] is expected
    assert result['session'] is None
    assert sent['options'] == {'data': {'signup_profile': TYPED}}


@pytest.mark.parametrize('code, raw', [
    ('email_not_confirmed', 'Email not confirmed'),
    ('invalid_credentials', 'Invalid login credentials'),
])
def test_the_two_common_sign_in_errors_say_what_to_do_in_the_visitors_language(monkeypatch, code, raw):
    """PostHog, 180 days to 2026-10-21: these were 133 of 139 web sign-in
    failures, shown as Supabase's English text in both languages."""
    import web.analytics as analytics
    import web.auth_state as auth_state
    from shared.genizah_translations import TRANSLATIONS
    from web.translations import using_language

    captured = []
    monkeypatch.setattr(auth_state, 'supabase_sign_in',
                        lambda email, password: {'error': raw, 'error_code': code, 'status_code': 400})
    monkeypatch.setattr(analytics, 'posthog_capture', lambda event, props=None, **k: captured.append((event, props)))

    async def main(lang):
        with using_language(lang):
            return await auth_state.do_login('a@b.c', 'pw')

    english = asyncio.run(main('en'))['error']
    hebrew = asyncio.run(main('he'))['error']

    assert english != raw and TRANSLATIONS.get(english) == hebrew, (english, hebrew)
    assert captured[0] == ('login_failed', {'reason': raw, 'error_code': code, 'status_code': 400,
                                            'method': 'password'}), "analytics keep Supabase's own reason"


def test_other_sign_in_errors_pass_through(monkeypatch):
    import web.analytics as analytics
    import web.auth_state as auth_state

    raw = 'Request rate limit reached'
    monkeypatch.setattr(auth_state, 'supabase_sign_in', lambda email, password: {
        'error': raw, 'error_code': 'over_request_rate_limit', 'status_code': 429})
    monkeypatch.setattr(analytics, 'posthog_capture', lambda *a, **k: None)

    assert asyncio.run(auth_state.do_login('a@b.c', 'pw'))['error'] == raw


def test_the_register_form_says_check_your_email_and_stays_open(monkeypatch):
    """The call site: what the visitor sees after Register when the account
    waits for its confirmation link."""
    import itertools

    from nicegui import core, ui
    from nicegui.client import Client
    from nicegui.testing.general import prepare_simulation

    import web.auth_state as auth_state
    from shared.genizah_translations import TRANSLATIONS

    async def fake_register(email, username, password, full_name=None, affiliation=None):
        return {'success': True, 'check_email': True}

    monkeypatch.setattr(auth_state, 'do_register', fake_register)
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    seen = {}

    async def main():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_register_probe_{next(_register_paths)}')) as client:
            dialog = auth_state.create_login_dialog()
            dialog.open()
        login_panel, register_panel = [e for e in client.elements.values() if isinstance(e, ui.tab_panel)]
        login_email = [e for e in login_panel.descendants() if isinstance(e, ui.input)][0]
        reg_inputs = [e for e in register_panel.descendants() if isinstance(e, ui.input)]
        reg_email, reg_username, _name, _affiliation, reg_pw, reg_confirm = reg_inputs
        reg_email.value, reg_username.value = 'new@b.c', 'chosen'
        reg_pw.value = reg_confirm.value = 'pw-1'
        labels = {'Register', TRANSLATIONS['Register']}
        register = [e for e in client.elements.values() if isinstance(e, ui.button) and e.text in labels]
        assert len(register) == 1, register
        for listener in register[0]._event_listeners.values():
            if listener.type == 'click':
                listener.handler(None)
        await asyncio.sleep(0.3)
        seen['notice'] = [e for e in client.elements.values() if isinstance(e, ui.label)
                          and 'new@b.c' in (e.text or '')]
        seen['dialog_open'] = dialog.value
        seen['passwords'] = (reg_pw.value, reg_confirm.value)
        seen['login_email'] = login_email.value

    _register_paths = itertools.count()
    asyncio.run(main())

    assert len(seen['notice']) == 1, 'no "check your email" notice naming the address'
    assert 'hidden' not in seen['notice'][0].classes
    assert seen['dialog_open'] is True, 'the dialog closed as if the visitor were signed in'
    assert seen['passwords'] == ('', ''), 'the password fields kept the password'
    assert seen['login_email'] == 'new@b.c', 'the Login tab is not ready for the address'
