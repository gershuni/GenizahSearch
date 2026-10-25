# -*- coding: utf-8 -*-
"""The admin Users tab shows each account's email under its name.

Since migrations/profiles_no_email_username.sql (2026-10-21) the username is no
longer the email, so the email comes from the admin-only
`admin_list_user_emails` RPC (migrations/admin_list_user_emails.sql), and a
lookup that fails says why on the tab: on 2026-10-25 the deployed tab showed no
emails and nothing on the page or in a plain log read told the cause.
"""

import asyncio
import itertools
import pathlib

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

admin = pytest.importorskip('web.pages.admin')

_paths = itertools.count()


class _Response:
    def __init__(self, data):
        self.data = data


class _Call:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return _Response(self._data)


class _APIError(Exception):
    """The shape of postgrest.exceptions.APIError: a code and a message."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


class _AdminClient:
    def __init__(self, rows=None, error=None):
        self.rows = rows
        self.error = error
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))
        if self.error:
            raise self.error
        return _Call(self.rows)


@pytest.fixture(autouse=True)
def _english():
    """Outside a page the visitor language is Hebrew; the labels here are English."""
    from web.translations import using_language
    with using_language('en'):
        yield


def test_the_accounts_come_by_id():
    client = _AdminClient([
        {'id': 'a', 'email': 'a@x.org', 'sign_in_name': 'Ada Lovelace', 'provider': 'google'},
        {'id': 'b', 'email': None, 'sign_in_name': None, 'provider': 'email'},
        {'id': 'c', 'email': 'c@x.org'},  # the function as first deployed: (id, email)
    ])
    assert admin.get_user_accounts(client) == ({
        'a': {'email': 'a@x.org', 'sign_in_name': 'Ada Lovelace', 'provider': 'google'},
        'b': {'provider': 'email'},
        'c': {'email': 'c@x.org'},
    }, None)
    assert client.calls == [('admin_list_user_emails', {})]


@pytest.mark.parametrize('client, problem', [
    (None, 'no_session'),
    # What Supabase answered on 2026-10-25 to the anon key: the grant refuses it.
    (_AdminClient(error=_APIError('42501', 'permission denied for function admin_list_user_emails')),
     'no_session'),
    (_AdminClient(error=_APIError('42501', 'admin role required')), 'not_admin'),
    (_AdminClient(error=_APIError('PGRST202', 'Could not find the function public.admin_list_user_emails')),
     'missing'),
    (_AdminClient(error=RuntimeError('connection reset')), 'error'),
])
def test_a_failed_lookup_says_why(client, problem):
    assert admin.get_user_accounts(client) == ({}, problem)
    assert problem in admin.EMAILS_PROBLEMS


def test_the_failure_log_names_the_cause_and_no_address(caplog):
    admin.get_user_accounts(_AdminClient(error=_APIError('42501', 'admin role required')))
    assert '42501' in caplog.text and 'admin role required' in caplog.text


def test_display_name_falls_back_to_the_email():
    assert admin.user_display_name({'full_name': 'John Locke', 'username': 'jl'}) == 'John Locke'
    assert admin.user_display_name({'username': 'Curious Scribe', 'email': 'c@x.org'}) == 'Curious Scribe'
    assert admin.user_display_name({'email': 'c@x.org'}) == 'c@x.org'
    assert admin.user_display_name({}) == 'No name set'


def test_the_rpc_is_admin_only_and_closed_to_anon():
    sql = (pathlib.Path(__file__).resolve().parents[1]
           / 'migrations' / 'admin_list_user_emails.sql').read_text(encoding='utf-8')
    body = sql.split('as $$', 1)[1].split('$$;', 1)[0]
    gate = body.index("p.role = 'admin'")
    assert gate < body.index('raise exception') < body.index('return query'), \
        'the admin check must come before the emails are returned'
    assert 'security definer' in sql and "set search_path = ''" in sql
    # 2026-10-25 widened the return type, which CREATE OR REPLACE cannot do.
    assert sql.index('drop function if exists public.admin_list_user_emails();') \
        < sql.index('create function public.admin_list_user_emails()')
    assert 'returns table (id uuid, email text, sign_in_name text, provider text)' in sql
    assert 'revoke all on function public.admin_list_user_emails() from public, anon;' in sql
    assert 'grant execute on function public.admin_list_user_emails() to authenticated;' in sql


# ---------------------------------------------------------------- the rendered tab

def _texts_of(users, problem=None, monkeypatch=None):
    """The labels of the real Users tab, row by row."""
    async def render():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_admin_users_probe_{next(_paths)}')) as client:
            await admin.create_users_list_view(users, problem)
            return [e.text for e in client.elements.values() if isinstance(e, ui.label)]
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    return asyncio.run(render())


def test_each_row_shows_the_name_the_sign_in_name_and_the_email(monkeypatch):
    texts = _texts_of([
        {'id': '1', 'full_name': 'John Locke', 'username': 'johnlockejrr', 'email': 'jl@x.org',
         'sign_in_name': 'John Locke'},
        {'id': '2', 'username': 'Discerning Seeker', 'email': 'ds@x.org', 'sign_in_name': 'Dana Seeker'},
        {'id': '3', 'full_name': 'יעקב חלבי', 'username': 'Eager Bookseller'},
        {'id': '4', 'username': 'Nimble Bookbinder'},
    ], monkeypatch=monkeypatch)

    assert 'jl@x.org' in texts and '@johnlockejrr' not in texts, 'with an email, the email is under the name'
    assert texts.count('John Locke') == 1, 'a sign-in name that is the public name is not repeated'
    assert texts.index('Discerning Seeker') < texts.index('Dana Seeker') < texts.index('ds@x.org'), \
        'the public name, then the name the account signed in with, then the email'
    assert '@Discerning Seeker' not in texts
    assert '@Eager Bookseller' in texts, 'without an email, the username, when it is not the name'
    assert texts.count('Nimble Bookbinder') == 1 and '@Nimble Bookbinder' not in texts, \
        'an alias that is the name is not repeated under it'
    assert not any(t.startswith('Emails could not be loaded') for t in texts)


def test_the_tab_says_why_there_are_no_emails(monkeypatch):
    texts = _texts_of([{'id': '1', 'username': 'Curious Scribe'}], 'no_session', monkeypatch)
    assert admin.EMAILS_PROBLEMS['no_session'] in texts
