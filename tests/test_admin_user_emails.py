# -*- coding: utf-8 -*-
"""The admin Users tab names every account, even one with no name or username.

Since migrations/profiles_no_email_username.sql (2026-10-21) the username is no
longer the email, and most accounts have no full name either, so the tab listed
them as "Unknown". get_all_users now adds each account's email from the
admin-only `admin_list_user_emails` RPC (migrations/admin_list_user_emails.sql).
"""

import pathlib

import pytest

admin = pytest.importorskip('web.pages.admin')

PROFILES = [
    {'id': 'a', 'full_name': 'John Locke', 'username': 'johnlockejrr', 'role': 'user'},
    {'id': 'b', 'full_name': None, 'username': None, 'role': 'user'},
    {'id': 'c', 'full_name': None, 'username': None, 'role': 'editor'},
]


class _Response:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, data):
        self._data = data

    def select(self, *_a, **_kw):
        return self

    def order(self, *_a, **_kw):
        return self

    def execute(self):
        return _Response([dict(row) for row in self._data])


class _AnonClient:
    def table(self, name):
        assert name == 'profiles'
        return _Query(PROFILES)


class _AdminClient:
    def __init__(self, rows=None, error=None):
        self.rows = rows
        self.error = error
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))
        if self.error:
            raise self.error
        return _Query(self.rows)


@pytest.fixture(autouse=True)
def _anon(monkeypatch):
    monkeypatch.setattr(admin, 'get_client', lambda: _AnonClient())


@pytest.fixture(autouse=True)
def _english():
    """Outside a page the visitor language is Hebrew; the labels here are English."""
    from web.translations import using_language
    with using_language('en'):
        yield


def test_unnamed_accounts_get_their_email():
    client = _AdminClient([{'id': 'a', 'email': 'a@x.org'},
                           {'id': 'b', 'email': 'b@x.org'}])
    users = admin.get_all_users(client)

    assert client.calls == [('admin_list_user_emails', {})]
    by_id = {u['id']: u for u in users}
    assert by_id['a']['email'] == 'a@x.org'
    assert by_id['b']['email'] == 'b@x.org'
    assert 'email' not in by_id['c'], 'an account the RPC did not return gets no email'
    assert [admin.user_display_name(u) for u in users] == \
        ['John Locke', 'b@x.org', 'No name set']


@pytest.mark.parametrize('client', [
    None,  # the fallback path (no admin client)
    _AdminClient(error=RuntimeError('function admin_list_user_emails does not exist')),
])
def test_without_the_rpc_the_list_still_loads(client):
    """Before the migration runs, or without an admin client: names only, no crash."""
    users = admin.get_all_users(client)

    assert [u['id'] for u in users] == ['a', 'b', 'c']
    assert not any('email' in u for u in users)
    assert admin.user_display_name(users[1]) == 'No name set'


def test_the_failure_log_does_not_carry_the_error_text(caplog):
    """A Supabase error string can quote row data; only its type is logged."""
    client = _AdminClient(error=RuntimeError('b@x.org'))
    admin.get_all_users(client)
    assert 'b@x.org' not in caplog.text


def test_the_rpc_is_admin_only_and_closed_to_anon():
    sql = (pathlib.Path(__file__).resolve().parents[1]
           / 'migrations' / 'admin_list_user_emails.sql').read_text(encoding='utf-8')
    body = sql.split('as $$', 1)[1].split('$$;', 1)[0]
    gate = body.index("p.role = 'admin'")
    assert gate < body.index('raise exception') < body.index('return query'), \
        'the admin check must come before the emails are returned'
    assert 'security definer' in sql and "set search_path = ''" in sql
    assert 'revoke all on function public.admin_list_user_emails() from public, anon;' in sql
    assert 'grant execute on function public.admin_list_user_emails() to authenticated;' in sql
