# -*- coding: utf-8 -*-
"""The one-time "how should your name appear?" question (web/public_name.py).

Since migrations/profiles_public_aliases.sql every account has a public name --
its own, or an alias such as "Curious Scribe" -- and ``public_name_confirmed``
says whether the person chose it. The header asks once while the flag is false
and there is no full name. The dialog is driven for real inside a NiceGUI
Client; no network.
"""
from __future__ import annotations

import asyncio
import itertools
import pathlib
import threading

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

import web.public_name as pn

_paths = itertools.count()
SENTINEL = object()
USER = {'id': 'user-1', 'user_metadata': {'full_name': 'Ada Lovelace', 'name': 'Ada'}}
ALIAS_PROFILE = {'id': 'user-1', 'username': 'Curious Scribe', 'full_name': None,
                 'public_name_confirmed': False}


@pytest.fixture(autouse=True)
def _english():
    """Outside a page the visitor language is Hebrew; the texts here are English."""
    from web.translations import using_language
    with using_language('en'):
        yield


# ---------------------------------------------------------------- the rules

@pytest.mark.parametrize('raw, cleaned', [
    ('  Ada   Lovelace ', 'Ada Lovelace'),
    ('הלל גרשוני', 'הלל גרשוני'),
    ('', None),
    ('   ', None),
    (None, None),
    ('ada@example.org', None),
    ('x' * 250, 'x' * 200),
])
def test_clean_public_name(raw, cleaned):
    assert pn.clean_public_name(raw) == cleaned


def test_the_suggestion_is_the_sign_in_name_never_an_address():
    assert pn.suggested_public_name(USER) == 'Ada Lovelace'
    assert pn.suggested_public_name({'user_metadata': {'name': 'Ada'}}) == 'Ada'
    assert pn.suggested_public_name({'user_metadata': {'full_name': 'ada@example.org'}}) == ''
    assert pn.suggested_public_name({}) == '' and pn.suggested_public_name(None) == ''


@pytest.mark.parametrize('profile, asked', [
    (ALIAS_PROFILE, True),
    ({**ALIAS_PROFILE, 'public_name_confirmed': True}, False),
    ({**ALIAS_PROFILE, 'full_name': 'Ada Lovelace'}, False),  # set in My Profile
    ({**ALIAS_PROFILE, 'full_name': '   '}, True),
    ({'id': 'user-1', 'username': None, 'full_name': None}, False),  # before the migration
    (None, False),
])
def test_who_is_asked(profile, asked):
    assert pn.needs_public_name(profile) is asked


# ---------------------------------------------------------------- the header hook

@pytest.fixture
def header(monkeypatch):
    h = {'storage': {}, 'shown': [], 'refreshed': [], 'profile': dict(ALIAS_PROFILE)}
    monkeypatch.setattr(pn.GlobalAuthState, 'get_user', staticmethod(lambda: USER))
    monkeypatch.setattr(pn.GlobalAuthState, 'get_profile', staticmethod(lambda: h['profile']))
    monkeypatch.setattr(pn, 'safe_user_get', lambda key, default=None: h['storage'].get(key, default))
    monkeypatch.setattr(pn, 'show_public_name_dialog', lambda *a: h['shown'].append(a))
    monkeypatch.setattr(pn, '_refresh_cached_profile', lambda uid: h['refreshed'].append(uid))
    return h


def test_an_unanswered_alias_opens_the_dialog(header):
    pn.maybe_ask_public_name('button')
    assert header['shown'] == [(USER, ALIAS_PROFILE, 'button')]


def test_a_profile_cached_before_the_column_is_re_read_not_asked(header):
    header['profile'] = {'id': 'user-1', 'username': 'Curious Scribe', 'full_name': None}
    pn.maybe_ask_public_name()
    assert header['shown'] == [] and header['refreshed'] == ['user-1']


def test_a_failed_save_is_not_asked_again_on_every_page(header):
    header['storage'][pn._SKIP_KEY] = 'user-1'
    pn.maybe_ask_public_name()
    assert header['shown'] == [] and header['refreshed'] == []


def test_the_header_asks_only_when_signed_in():
    src = pathlib.Path(pn.__file__).with_name('auth_state.py').read_text(encoding='utf-8')
    body = src.split('def create_auth_buttons', 1)[1]
    signed_in, signed_out = body.split('\n    else:', 1)
    assert 'maybe_ask_public_name(name_button)' in signed_in
    assert 'maybe_ask_public_name' not in signed_out


# ---------------------------------------------------------------- the dialog

def _handler_of(button, name):
    """The dialog's own handler ``name``, from the button's click listener."""
    for listener in button._event_listeners.values():
        for cell in getattr(listener.handler, '__closure__', None) or ():
            try:
                fn = cell.cell_contents
            except ValueError:
                continue
            if callable(fn) and getattr(fn, '__name__', '') == name:
                return fn
    raise AssertionError(f'{name} is not wired to its button')


@pytest.fixture
def dialog(monkeypatch):
    """The real dialog; ``run(handler_name, typed=...)`` types and clicks."""
    h = {'calls': [], 'answer': None, 'notes': [], 'storage': {}, 'cached': [],
         'client_threads': [], 'loop_thread': None,
         'signed_in': 'user-1', 'signs_in_during_save': None}

    def record(user_id, data, *, client=None):
        h['calls'].append({'thread': threading.get_ident(), 'client': client,
                           'user_id': user_id, 'data': dict(data)})
        if h['signs_in_during_save']:  # another tab of this browser, meanwhile
            h['signed_in'] = h['signs_in_during_save']
        return h['answer'] if h['answer'] is not None else {'success': True, 'profile': {**ALIAS_PROFILE, **data}}

    def build_client():
        h['client_threads'].append(threading.get_ident())
        return SENTINEL

    monkeypatch.setattr(pn, 'update_profile', record)
    monkeypatch.setattr(pn, 'get_user_client', build_client)
    monkeypatch.setattr(pn, 'safe_user_set', lambda key, value: h['storage'].__setitem__(key, value) or True)
    monkeypatch.setattr(pn.GlobalAuthState, 'update_profile_cache', staticmethod(h['cached'].append))
    monkeypatch.setattr(pn.GlobalAuthState, 'get_user_id', staticmethod(lambda: h['signed_in']))
    monkeypatch.setattr(pn.ui, 'notify', lambda message, *a, **k: h['notes'].append((str(message), k.get('type'))))
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    async def run(name, typed=None):
        core.loop = asyncio.get_running_loop()
        h['loop_thread'] = threading.get_ident()
        with Client(ui.page(f'/_public_name_probe_{next(_paths)}')) as client:
            header_button = ui.button('Curious Scribe')
            box = pn.show_public_name_dialog(USER, ALIAS_PROFILE, header_button)
            elements = list(client.elements.values())
            name_input = next(e for e in elements if isinstance(e, ui.input))
            h['prefilled'] = name_input.value
            if typed is not None:
                name_input.value = typed
            handlers = []
            for button in (e for e in elements if isinstance(e, ui.button) and e is not header_button):
                try:
                    handlers.append(_handler_of(button, name))
                except AssertionError:
                    pass
            assert len(handlers) == 1, f'{name}: {len(handlers)} buttons'
            await handlers[0]()
            h['open'] = box.value
            h['header_text'] = header_button.text
            h['error'] = next(e for e in elements if isinstance(e, ui.label) and 'text-red-500' in e._classes)

    h['run'] = lambda name, typed=None: asyncio.run(run(name, typed))
    return h


def test_save_writes_the_name_off_the_loop_and_shows_it(dialog):
    dialog['run']('save', typed='  Ada   Lovelace ')

    assert dialog['prefilled'] == 'Ada Lovelace', 'the Google name is the suggestion'
    [call] = dialog['calls']
    assert call['data'] == {'public_name_confirmed': True, 'full_name': 'Ada Lovelace'}
    assert call['user_id'] == 'user-1' and call['client'] is SENTINEL
    assert call['thread'] != dialog['loop_thread'], 'the profile was written on the event loop'
    assert dialog['client_threads'] == [dialog['loop_thread']], 'get_user_client must run on the loop'
    assert dialog['cached'][0]['full_name'] == 'Ada Lovelace'
    assert dialog['header_text'] == 'Ada Lovelace'
    assert dialog['open'] is False
    assert [kind for _m, kind in dialog['notes']] == ['positive']


def test_keep_confirms_the_alias_and_writes_no_name(dialog):
    dialog['run']('keep')

    [call] = dialog['calls']
    assert call['data'] == {'public_name_confirmed': True}
    assert dialog['header_text'] == 'Curious Scribe'
    assert dialog['open'] is False and dialog['notes'] == []


@pytest.mark.parametrize('typed, message', [
    ('ada@example.org', 'A public name cannot contain @.'),
    ('   ', 'Please enter a name, or keep the one you have.'),
])
def test_a_name_that_cannot_be_one_is_refused_in_place(dialog, typed, message):
    dialog['run']('save', typed=typed)

    assert dialog['calls'] == [], 'nothing may be written'
    assert dialog['error'].text == message and dialog['error'].visible
    assert dialog['open'] is True


def test_a_refused_save_is_said_and_not_asked_again_here(dialog):
    dialog['answer'] = {'error': 'Update failed'}
    dialog['run']('save', typed='Ada Lovelace')

    assert dialog['storage'] == {pn._SKIP_KEY: 'user-1'}
    assert dialog['cached'] == [] and dialog['header_text'] == 'Curious Scribe'
    assert [kind for _m, kind in dialog['notes']] == ['negative']


def test_an_account_switched_in_another_tab_gets_nothing_written(dialog):
    """Codex (PR #447): the browser's storage is shared by its tabs."""
    dialog['signed_in'] = 'user-2'
    dialog['run']('save', typed='Ada Lovelace')

    assert dialog['calls'] == [], "the other account's client would have written"
    assert dialog['cached'] == [] and dialog['open'] is False


def test_an_account_switched_during_the_save_keeps_its_own_cache(dialog):
    """Codex (PR #447): A's saved profile -- name and role -- must not land in B's cache."""
    dialog['signs_in_during_save'] = 'user-2'
    dialog['run']('save', typed='Ada Lovelace')

    assert len(dialog['calls']) == 1
    assert dialog['cached'] == [], "account A's profile was cached for account B"
    assert dialog['header_text'] == 'Curious Scribe' and dialog['notes'] == []
    assert dialog['storage'] == {}


# ---------------------------------------------------------------- where names are shown

def test_published_join_authors_fall_back_to_the_username():
    from unittest.mock import MagicMock
    from shared.puzzle_publish_service import resolve_author_profiles

    table = MagicMock()
    table.select.return_value = table
    table.in_.return_value = table
    table.execute.return_value.data = [
        {'id': 'u1', 'full_name': 'Ada Lovelace', 'username': 'Curious Scribe'},
        {'id': 'u2', 'full_name': None, 'username': 'Patient Copyist'},
        {'id': 'u3', 'full_name': None, 'username': None},
    ]
    client = MagicMock()
    client.table.return_value = table

    assert resolve_author_profiles(client, ['u1', 'u2', 'u3', 'u4']) == {
        'u1': 'Ada Lovelace', 'u2': 'Patient Copyist', 'u3': 'Anonymous', 'u4': 'Anonymous'}
