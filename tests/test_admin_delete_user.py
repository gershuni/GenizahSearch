# -*- coding: utf-8 -*-
"""The admin Users tab's "Delete User" deletes the account, not just its profile.

It used to delete only the profiles row: the person could still sign in, with
no profile -- no public name, never asked for one, My Profile unable to save
(two such accounts found 2026-10-25). It now calls the admin-only
`admin_delete_user` RPC (migrations/admin_delete_user.sql), which deletes from
auth.users.
"""

import asyncio
import itertools
import pathlib
import re

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

admin = pytest.importorskip('web.pages.admin')

SQL = (pathlib.Path(__file__).resolve().parents[1] / 'migrations' / 'admin_delete_user.sql') \
    .read_text(encoding='utf-8')
_paths = itertools.count()


class _APIError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


class _Client:
    def __init__(self, data=None, error=None):
        self.data, self.error, self.calls = data, error, []

    def rpc(self, name, params):
        self.calls.append((name, params))
        if self.error:
            raise self.error
        return self

    def execute(self):
        return type('R', (), {'data': self.data})()

    def table(self, name):  # the old path: deleting the profiles row
        raise AssertionError(f'delete_user touched the {name} table directly')


@pytest.fixture(autouse=True)
def _english():
    from web.translations import using_language
    with using_language('en'):
        yield


# ---------------------------------------------------------------- the call

def test_delete_goes_through_the_rpc_never_the_profiles_table():
    client = _Client(data=True)
    assert admin.delete_user('u-1', client=client) == {'success': True}
    assert client.calls == [('admin_delete_user', {'p_user_id': 'u-1'})]


def test_nothing_deleted_is_not_called_a_success():
    assert admin.delete_user('u-1', client=_Client(data=False)) == \
        {'error': 'No such account: it may already have been deleted.'}


@pytest.mark.parametrize('error', [
    _APIError('42501', 'this account is an admin: change its role first'),
    _APIError('23503', 'not deleted: the account is still referenced by corrections_reviewed_by_fkey'),
])
def test_a_refusal_is_reported_in_its_own_words(error):
    assert admin.delete_user('u-1', client=_Client(error=error)) == {'error': error.message}


# ---------------------------------------------------------------- the function

def _body():
    return SQL.split('as $$', 1)[1].split('$$;', 1)[0]


def test_the_function_checks_before_it_deletes():
    body = _body()
    delete = body.index('delete from auth.users')
    assert body.index("p.role = 'admin'") < delete, 'the caller must be an admin first'
    assert body.index('p_user_id = auth.uid()') < delete, "an admin's own account is refused"
    assert body.index("p.id = p_user_id and p.role = 'admin'") < delete, 'other admins are refused'
    assert 'exception when foreign_key_violation' in body and 'constraint_name' in body


def test_the_function_is_out_of_anons_reach_and_pins_its_search_path():
    assert re.search(r"security definer\s+set search_path = ''", SQL)
    assert 'revoke all on function public.admin_delete_user(uuid) from public, anon;' in SQL
    assert 'grant execute on function public.admin_delete_user(uuid) to authenticated;' in SQL
    # Its owner must be able to delete, or every admin delete would fail.
    assert SQL.index("has_table_privilege(current_user, 'auth.users', 'delete')") \
        < SQL.index('create or replace function public.admin_delete_user')


# ---------------------------------------------------------------- the menu

def _menu_texts(user, monkeypatch):
    async def render():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_admin_delete_probe_{next(_paths)}')) as client:
            admin.create_user_row(user)
            # A menu item keeps its text in a child element, so: every text on the row.
            return [getattr(e, 'text', None) for e in client.elements.values()]
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    return asyncio.run(render())


def test_delete_is_offered_for_users_not_for_admins(monkeypatch):
    assert 'Delete User' in _menu_texts({'id': 'u', 'username': 'Curious Scribe', 'role': 'user'}, monkeypatch)
    assert 'Delete User' not in _menu_texts({'id': 'a', 'full_name': 'Admin', 'role': 'admin'}, monkeypatch)
