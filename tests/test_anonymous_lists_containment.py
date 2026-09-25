# -*- coding: utf-8 -*-
"""Anonymous web Lists containment (improvement sweep C2, critical).

The web app loads ONE process-wide ``ListsManager`` (the server's
``lists.pkl``) at startup, and ``UserListsManager`` used to fall back to it
whenever a visitor was not signed in; four readers (star state, item lists,
tags and list export) consulted it even for signed-in users. Web lists are now
Supabase-only.

The containment contract pinned here:

* ``UserListsManager`` never reads or writes the shared store for anyone,
  whether it was handed in through the constructor or assigned afterwards.
* Driving every anonymous path leaves a seeded ``lists.pkl`` byte-identical
  (same hash, same mtime, no new ``.bak`` rotation) and returns none of its
  content.
* The "Move to account" migration is retired and never touches the store.
* The UI entry points ask anonymous visitors to sign in before any lists
  read, and the Excel list export refuses anonymous requests with 401.

Nothing here touches the network, Supabase, or a real ``lists.pkl``:
``ListsManager.LISTS_FILE`` and ``Config.INDEX_DIR`` are redirected into
``tmp_path`` for every test that builds a real store.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import os
import pathlib
import pickle
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

SECRET_LIST_ID = 'list_privatea'
SECRET_LIST_NAME = 'Private A'
SECRET_NOTE = 'secret note'
SECRET_SYS_ID = '990000000000001'
SECRET_RECENT_SYS_ID = '990000000000002'
SECRET_TAG = 'secret-tag'
SECRETS = (SECRET_LIST_NAME, SECRET_NOTE, SECRET_SYS_ID, SECRET_RECENT_SYS_ID, SECRET_TAG)

# Every Supabase function web.user_lists imports at module level. Anonymous
# paths must never reach them; a stub that raises makes any accidental call loud.
_SUPABASE_NAMES = (
    'get_user_lists', 'sb_create_list', 'sb_update_list', 'sb_delete_list',
    'get_list_items', 'add_list_item', 'update_list_item', 'delete_list_item',
    'get_recent_items', 'add_recent_item', 'get_projects', 'sb_create_project',
    'sb_update_project', 'sb_delete_project',
)


class Tripwire:
    """Stand-in for the shared ListsManager that records every attribute read.

    ``__bool__`` is left at its default (truthy) on purpose, so an
    ``elif self.local_mgr:`` branch really enters and the access is recorded.
    Returned values are MagicMocks so a leaking caller keeps running and the
    test fails on the recorded access, not on an incidental crash.
    """

    def __init__(self):
        object.__setattr__(self, 'accessed', [])

    def __getattr__(self, name):
        self.accessed.append(name)
        return MagicMock(name=f'tripwire.{name}')


def _set_auth(monkeypatch, *, logged_in: bool, user_id=None):
    import web.auth_state as auth_state
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_logged_in',
                        classmethod(lambda cls: logged_in))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'get_user_id',
                        classmethod(lambda cls: user_id))


def _forbid_supabase(monkeypatch):
    import web.user_lists as user_lists
    calls = []

    def _make(name):
        def _forbidden(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f'anonymous path reached Supabase: {name}')
        return _forbidden

    for name in _SUPABASE_NAMES:
        monkeypatch.setattr(user_lists, name, _make(name))
    return calls


def _drive_every_method(mgr, *, list_id='default', item_id='x', sys_id='x'):
    """Call every public UserListsManager method; return {name: result}."""
    run = asyncio.run
    results = {}
    results['data'] = mgr.data
    results['get_all_lists'] = mgr.get_all_lists()
    results['get_all_lists_no_recent'] = mgr.get_all_lists(include_recent=False)
    results['_get_list_item_count'] = mgr._get_list_item_count(list_id)
    results['create_list'] = run(mgr.create_list('New', project_id='p1'))
    results['create_list_sync'] = mgr.create_list_sync('New', project_id='p1')
    results['update_list'] = run(mgr.update_list(list_id, name='Renamed'))
    results['update_list_project'] = run(mgr.update_list_project(list_id, 'p1'))
    results['delete_list'] = run(mgr.delete_list(list_id))
    results['get_deleted_lists'] = mgr.get_deleted_lists()
    results['restore_list'] = run(mgr.restore_list(list_id))
    results['permanently_delete_list'] = run(mgr.permanently_delete_list(list_id))
    results['empty_trash'] = run(mgr.empty_trash())
    results['add_item'] = run(mgr.add_item(sys_id, list_id, note='n'))
    results['add_item_sync'] = mgr.add_item_sync(sys_id, list_id, note='n')
    results['remove_item_from_list'] = run(mgr.remove_item_from_list(item_id, list_id))
    results['remove_item_from_list_sync'] = mgr.remove_item_from_list_sync(item_id, list_id)
    results['update_item_note'] = run(mgr.update_item_note(item_id, 'note'))
    results['update_item_tags'] = run(mgr.update_item_tags(item_id, ['t']))
    results['get_items_in_list'] = run(mgr.get_items_in_list(list_id))
    results['get_items_in_list_sync'] = mgr.get_items_in_list_sync(list_id)
    results['get_items_in_list_sync_recent'] = mgr.get_items_in_list_sync(
        'recent', is_authenticated=False)
    results['is_item_in_any_list'] = mgr.is_item_in_any_list(item_id)
    results['get_item_lists'] = mgr.get_item_lists(item_id)
    results['add_to_recent'] = run(mgr.add_to_recent(sys_id))
    results['add_to_recent_sync'] = mgr.add_to_recent_sync(sys_id)
    results['get_all_tags'] = mgr.get_all_tags()
    results['get_projects'] = mgr.get_projects()
    results['create_project'] = run(mgr.create_project('Proj'))
    results['create_project_sync'] = mgr.create_project_sync('Proj')
    results['update_project'] = run(mgr.update_project('p1', name='P'))
    results['delete_project'] = run(mgr.delete_project('p1'))
    results['move_list_to_project'] = run(mgr.move_list_to_project(list_id, 'p1'))
    results['get_list_display_color'] = mgr.get_list_display_color(list_id)
    results['get_lists_by_project'] = mgr.get_lists_by_project()
    results['export_list'] = mgr.export_list(list_id)
    results['has_local_lists'] = mgr.has_local_lists()
    results['save'] = mgr.save()
    results['load'] = mgr.load()
    results['refresh_data'] = run(mgr.refresh_data())
    results['migrate_local_to_user'] = run(mgr.migrate_local_to_user())
    return results


_WRITES = (
    'create_list', 'create_list_sync', 'update_list', 'update_list_project',
    'delete_list', 'restore_list', 'permanently_delete_list', 'empty_trash',
    'add_item', 'add_item_sync', 'remove_item_from_list',
    'remove_item_from_list_sync', 'update_item_note', 'update_item_tags',
    'create_project', 'create_project_sync', 'update_project',
    'delete_project', 'move_list_to_project', 'add_to_recent',
    'add_to_recent_sync', 'save', 'load',
)
_EMPTY_READS = (
    'get_all_lists', 'get_all_lists_no_recent', 'get_deleted_lists',
    'get_items_in_list', 'get_items_in_list_sync',
    'get_items_in_list_sync_recent', 'get_item_lists', 'get_all_tags',
    'get_projects',
)


# ---------------------------------------------------------------------------
# Seeded real store in tmp_path
# ---------------------------------------------------------------------------

@pytest.fixture
def seeded_pkl(tmp_path, monkeypatch):
    """A real pickled store for 'visitor A', with LISTS_FILE pointed at it.

    ``LISTS_FILE`` is a class attribute bound at import to the real
    ``Config.INDEX_DIR``; it is redirected here so nothing can read or rotate
    the developer's (or the server's) real file. ``Config.INDEX_DIR`` is
    redirected too because ``ListsManager.save()`` calls ``os.makedirs`` on it.
    """
    from shared import lists_manager as lm

    pkl = tmp_path / 'lists.pkl'
    monkeypatch.setattr(lm.ListsManager, 'LISTS_FILE', str(pkl))
    monkeypatch.setattr(lm.Config, 'INDEX_DIR', str(tmp_path))

    data = lm.ListsManager.__new__(lm.ListsManager)._get_default_data()
    data['lists'][SECRET_LIST_ID] = {
        'name': SECRET_LIST_NAME, 'color': '#4CAF50', 'created': 0.0, 'project_id': None,
    }
    data['lists_order'] = ['default', 'recent', SECRET_LIST_ID]
    data['items'][SECRET_SYS_ID] = {
        'sys_id': SECRET_SYS_ID, 'lists': [SECRET_LIST_ID, 'default'],
        'tags': [SECRET_TAG], 'note': SECRET_NOTE, 'source': '', 'added': 0.0,
        'modified': 0.0, 'shelfmark_override': None, 'fl_id': None, 'img': None,
    }
    data['items'][SECRET_RECENT_SYS_ID] = {
        'sys_id': SECRET_RECENT_SYS_ID, 'lists': [], 'tags': [], 'note': '',
        'source': '', 'added': 0.0, 'modified': 0.0, 'shelfmark_override': None,
        'fl_id': None, 'img': None,
    }
    data['recent_items'] = [SECRET_RECENT_SYS_ID]
    data['all_tags'] = [SECRET_TAG]
    with open(pkl, 'wb') as fh:
        pickle.dump(data, fh)

    real_mgr = lm.ListsManager(None)
    # Fixture self-check: the real manager really loaded visitor A's store,
    # so a green result below cannot come from an empty or unread file.
    assert SECRET_LIST_ID in real_mgr.data['lists']
    assert real_mgr.is_item_in_any_list(SECRET_SYS_ID)
    return SimpleNamespace(path=pkl, dir=tmp_path, real_mgr=real_mgr)


def _fingerprint(seeded):
    raw = seeded.path.read_bytes()
    return (
        hashlib.sha256(raw).hexdigest(),
        os.stat(seeded.path).st_mtime_ns,
        sorted(p.name for p in seeded.dir.iterdir()),
    )


# ---------------------------------------------------------------------------
# 1. The chokepoint: UserListsManager
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('wiring', ['constructor', 'assigned_after'])
def test_anonymous_manager_never_touches_shared_store(monkeypatch, wiring):
    """No anonymous call reads the shared store, however it was wired in.

    ``assigned_after`` covers callers and tests that set ``mgr.local_mgr``
    after construction: the fallback branches must be gone, not merely
    skipped because the constructor dropped the argument.
    """
    import web.user_lists as user_lists

    _set_auth(monkeypatch, logged_in=False)
    supabase_calls = _forbid_supabase(monkeypatch)
    trip = Tripwire()
    if wiring == 'constructor':
        mgr = user_lists.UserListsManager(local_mgr=trip, meta_mgr=None)
    else:
        mgr = user_lists.UserListsManager(meta_mgr=None)
        mgr.local_mgr = trip

    results = _drive_every_method(mgr)

    assert trip.accessed == [], f'anonymous calls read the shared store: {trip.accessed}'
    assert supabase_calls == []
    for name in _WRITES:
        assert not results[name], f'{name} reported success for an anonymous visitor: {results[name]!r}'
    for name in _EMPTY_READS:
        assert results[name] == [], f'{name} returned data for an anonymous visitor: {results[name]!r}'
    assert results['is_item_in_any_list'] is False
    assert results['_get_list_item_count'] == 0
    assert results['export_list'] is None
    assert results['has_local_lists'] is False
    default = mgr._get_default_data()
    assert results['data'] == default
    assert results['refresh_data'] == default
    assert 'error' in results['migrate_local_to_user']


def test_anonymous_paths_leave_lists_pkl_byte_identical(monkeypatch, seeded_pkl):
    """Every anonymous path, plus a signed-in 'Move to account', leaves the
    seeded store byte-identical and returns none of visitor A's content."""
    import web.user_lists as user_lists

    before = _fingerprint(seeded_pkl)
    _set_auth(monkeypatch, logged_in=False)
    _forbid_supabase(monkeypatch)
    mgr = user_lists.UserListsManager(seeded_pkl.real_mgr, None)

    seen = []
    for list_id, item_id in (
        ('default', SECRET_SYS_ID),
        (SECRET_LIST_ID, SECRET_SYS_ID),
        ('recent', SECRET_RECENT_SYS_ID),
    ):
        results = _drive_every_method(mgr, list_id=list_id, item_id=item_id, sys_id=SECRET_SYS_ID)
        seen.append(repr(results))

    # A signed-in visitor pressing "Move to account" used to copy the whole
    # store into their account and then clear_all() it.
    _set_auth(monkeypatch, logged_in=True, user_id='u1')
    created = []
    monkeypatch.setattr(user_lists, 'sb_create_list',
                        lambda *a, **k: created.append(a) or {'success': True, 'list': {'id': 1}})
    monkeypatch.setattr(user_lists, 'add_list_item', lambda *a, **k: {'success': True})
    migrated = asyncio.run(mgr.migrate_local_to_user())
    seen.append(repr(migrated))
    seen.append(repr(mgr.has_local_lists()))

    assert _fingerprint(seeded_pkl) == before, 'lists.pkl was rewritten or rotated'
    assert created == [], 'migration copied the shared store into an account'
    blob = '\n'.join(seen)
    for secret in SECRETS:
        assert secret not in blob, f'returned shared-store content: {secret!r}'
    # The in-memory store is untouched as well.
    assert SECRET_LIST_ID in seeded_pkl.real_mgr.data['lists']


def test_logged_in_star_state_does_not_read_shared_store(monkeypatch):
    """The four readers that had no auth check leaked for signed-in users too."""
    import web.user_lists as user_lists

    _set_auth(monkeypatch, logged_in=True, user_id='u1')
    _forbid_supabase(monkeypatch)
    trip = Tripwire()
    mgr = user_lists.UserListsManager(local_mgr=trip)
    mgr.local_mgr = trip

    assert mgr.is_item_in_any_list('x') is False
    assert mgr.get_item_lists('x') == []
    assert mgr.get_all_tags() == []
    assert mgr.export_list('default') is None
    assert trip.accessed == [], f'signed-in readers consulted the shared store: {trip.accessed}'


def test_migrate_local_to_user_never_reads_or_clears_shared_store(monkeypatch):
    import web.user_lists as user_lists

    _set_auth(monkeypatch, logged_in=True, user_id='u1')
    created, added = [], []
    monkeypatch.setattr(user_lists, 'sb_create_list',
                        lambda *a, **k: created.append(a) or {'success': True, 'list': {'id': 1}})
    monkeypatch.setattr(user_lists, 'add_list_item',
                        lambda *a, **k: added.append(a) or {'success': True})
    trip = Tripwire()
    mgr = user_lists.UserListsManager(local_mgr=trip)
    mgr.local_mgr = trip

    result = asyncio.run(mgr.migrate_local_to_user())

    assert trip.accessed == [], f'migration touched the shared store: {trip.accessed}'
    assert created == [] and added == []
    assert isinstance(result, dict) and 'error' in result
    assert mgr.has_local_lists() is False
    assert trip.accessed == []


# ---------------------------------------------------------------------------
# 2. The UI gate
# ---------------------------------------------------------------------------

def _patch_dialog_module(monkeypatch):
    import web.components.add_to_list_dialog as atl

    fake_ui = MagicMock(name='ui')
    monkeypatch.setattr(atl, 'ui', fake_ui)
    h3_calls = []
    monkeypatch.setattr(atl, 'h3', lambda *a, **k: h3_calls.append(a))
    login_opened = []
    login_dialog = MagicMock(name='login_dialog')
    login_dialog.open.side_effect = lambda: login_opened.append(True)
    monkeypatch.setattr(atl, 'create_login_dialog', lambda: login_dialog, raising=False)
    return atl, fake_ui, h3_calls, login_opened


def _label_texts(fake_ui):
    return [c.args[0] for c in fake_ui.label.call_args_list if c.args]


def _button_calls(fake_ui, text):
    return [c for c in fake_ui.button.call_args_list if c.args and c.args[0] == text]


def test_add_to_list_dialog_requires_login(monkeypatch):
    atl, fake_ui, h3_calls, login_opened = _patch_dialog_module(monkeypatch)
    from web.translations import tr

    _set_auth(monkeypatch, logged_in=False)
    spy = MagicMock(name='lists_mgr')

    atl.show_add_to_list_dialog(sys_id=SECRET_SYS_ID, shelfmark='T-S 1', lists_mgr=spy)

    assert spy.mock_calls == [], f'lists manager used before sign-in: {spy.mock_calls}'
    assert h3_calls == [], 'the Add-to-List dialog was built for an anonymous visitor'
    assert not fake_ui.select.called
    assert tr('Sign in to access your saved research lists.') in _label_texts(fake_ui)
    sign_in = _button_calls(fake_ui, tr('Sign in'))
    assert len(sign_in) == 1, 'no Sign in button in the prompt'
    sign_in[0].kwargs['on_click']()
    assert login_opened == [True], 'Sign in did not open the login dialog'


def test_anonymous_lists_page_returns_before_reading_state(monkeypatch):
    """/lists for an anonymous visitor builds the sign-in card and reads nothing.

    The source guard below only proves a login check is PRESENT before the
    first lists_mgr read; an early return that no longer fires (for example
    ``if False and not is_logged_in()``) would still satisfy it. This drives
    create_lists_page itself with a fake ``ui`` so that regression is caught
    in the default (non render_smoke) run. Regression guard: green on the
    fixed code, proven able to fail by neutralising that early return.
    """
    import web.pages.lists as lists_page
    from web.translations import tr

    _set_auth(monkeypatch, logged_in=False)
    fake_ui = MagicMock(name='ui')
    monkeypatch.setattr(lists_page, 'ui', fake_ui)
    monkeypatch.setattr(lists_page, 'h1', lambda *a, **k: None)
    trip = Tripwire()
    monkeypatch.setattr(lists_page, 'state', trip)

    error = None
    try:
        lists_page.create_lists_page()
    except Exception as exc:  # the signed-in page cannot build on a fake ui
        error = exc

    assert trip.accessed == [], f'/lists read app state for an anonymous visitor: {trip.accessed}'
    assert error is None, f'/lists went past the anonymous early return: {error!r}'
    card = fake_ui.card.return_value.classes.return_value
    card.mark.assert_called_once_with('lists-anonymous-gate')
    assert tr('Sign in to access your saved research lists.') in _label_texts(fake_ui)
    assert len(_button_calls(fake_ui, tr('Sign in'))) == 1


def test_require_login_for_lists_passes_signed_in_users_through(monkeypatch):
    """A signed-in visitor gets True and no prompt is built.

    Red before the fix only because the helper did not exist yet; the
    behavioural red for the gate is test_add_to_list_dialog_requires_login.
    """
    atl, fake_ui, _h3, login_opened = _patch_dialog_module(monkeypatch)
    _set_auth(monkeypatch, logged_in=True, user_id='u1')

    assert atl.require_login_for_lists() is True
    assert fake_ui.mock_calls == []
    assert login_opened == []


def test_expired_session_add_does_not_write_or_claim_already_in_list(monkeypatch):
    """The dialog opened while signed in; the session expires before Add.

    The anonymous sync branch used to write through ``add_item_sync`` (to the
    shared store) and, once that is gone, would say 'Already in list'.
    """
    atl, fake_ui, _h3, _login = _patch_dialog_module(monkeypatch)
    from web.translations import tr

    _set_auth(monkeypatch, logged_in=True, user_id='u1')
    spy = MagicMock(name='lists_mgr')
    spy.data = {'lists': {'7': {'name': 'Mine'}}, 'projects': {}}
    atl.show_add_to_list_dialog(sys_id=SECRET_SYS_ID, shelfmark='T-S 1', lists_mgr=spy)
    add_buttons = _button_calls(fake_ui, tr('Add'))
    assert len(add_buttons) == 1
    do_add = add_buttons[0].kwargs['on_click']

    _set_auth(monkeypatch, logged_in=False)
    fake_ui.notify.reset_mock()
    asyncio.run(do_add())

    assert not spy.add_item_sync.called and not spy.add_item.called
    notices = [c.args[0] for c in fake_ui.notify.call_args_list if c.args]
    assert tr('Please log in to access lists') in notices, notices
    assert tr('Already in list') not in notices


def test_expired_session_create_and_add_does_not_write(monkeypatch):
    import web.components.add_to_list_dialog as atl
    from web.translations import tr

    notify = MagicMock()
    monkeypatch.setattr(atl.ui, 'notify', notify)
    spy = MagicMock(name='lists_mgr')
    dialog = MagicMock(name='dialog')

    result = asyncio.run(atl._create_and_add_handler(
        name='New list', project_id=None, lists_mgr=spy, sys_id=SECRET_SYS_ID,
        fl_id=None, new_list_note_value='', dialog=dialog, on_success=None,
        is_logged_in=False,
    ))

    assert result is False
    assert not spy.create_list_sync.called and not spy.add_item_sync.called
    notices = [c.args[0] for c in notify.call_args_list if c.args]
    assert notices == [tr('Please log in to access lists')], notices
    assert not dialog.close.called


# ---------------------------------------------------------------------------
# 3. The Excel list export
# ---------------------------------------------------------------------------

@pytest.fixture
def export_client(monkeypatch, seeded_pkl):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import web.api as api
    from web.state import state

    recorder = SimpleNamespace(calls=[])

    class _ExportSvc:
        def export_list_excel(self, list_id, name, items):
            recorder.calls.append((list_id, name, items))
            return b'xlsx-bytes', 'list.xlsx'

    monkeypatch.setattr(api, 'get_export_service', lambda meta: _ExportSvc())
    bare = FastAPI()
    api.init_api_routes(app_override=bare)
    saved = state._local_lists_mgr
    state._local_lists_mgr = seeded_pkl.real_mgr
    try:
        yield SimpleNamespace(client=TestClient(bare), recorder=recorder, seeded=seeded_pkl)
    finally:
        state._local_lists_mgr = saved


def test_anonymous_list_export_is_refused(monkeypatch, export_client):
    before = _fingerprint(export_client.seeded)
    monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(storage=SimpleNamespace(user={})))

    resp = export_client.client.get(f'/api/export/list/{SECRET_LIST_ID}/excel')
    assert resp.status_code == 401, (resp.status_code, resp.text)
    for secret in SECRETS:
        assert secret not in resp.text

    assert export_client.recorder.calls == []
    assert _fingerprint(export_client.seeded) == before


def test_signed_in_list_export_still_works(monkeypatch, export_client):
    """Regression guard, green before and after: the 401 gate must not block a
    signed-in user, whose list comes from Supabase (stubbed here)."""
    import web.user_lists as user_lists

    monkeypatch.setattr('web.safe_storage.app', SimpleNamespace(storage=SimpleNamespace(
        user={'auth_user': {'id': 'u1'}})))
    monkeypatch.setattr(user_lists, 'get_user_lists', lambda uid: [{'id': 5, 'name': 'Mine'}])
    monkeypatch.setattr(user_lists, 'get_projects', lambda uid: [])
    monkeypatch.setattr(user_lists, 'get_list_items',
                        lambda lid, client=None: [{'sys_id': '990000000000009'}])

    resp = export_client.client.get('/api/export/list/5/excel')

    assert resp.status_code == 200, resp.text
    assert export_client.recorder.calls == [('5', 'Mine', [{'sys_id': '990000000000009'}])]


# ---------------------------------------------------------------------------
# 4. Source guards for the closures that cannot be driven without a client
# ---------------------------------------------------------------------------

# (file, function name). Every named function must exist; each must make a
# login check, and that check must come before the first lists_mgr access.
_LOGIN_FIRST = (
    ('web/pages/search.py', 'bulk_add_to_list'),
    ('web/pages/browse.py', 'show_add_from_list_dialog'),
    ('web/components/comment_dialog.py', 'load_recent'),
    ('web/components/comment_dialog.py', 'load_lists'),
    ('web/components/joins_panel.py', 'show_add_join_form'),
    ('web/pages/home.py', '_deferred_load_recent'),
    ('web/pages/lists.py', 'create_lists_page'),
)

_LOGIN_CALLS = {'is_logged_in', 'require_login_for_lists'}


def _call_name(node: ast.Call):
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _pos(node):
    return (node.lineno, node.col_offset)


def _functions(tree, name):
    return [
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
    ]


def _first_login_check(fn):
    checks = [n for n in ast.walk(fn)
              if isinstance(n, ast.Call) and _call_name(n) in _LOGIN_CALLS]
    return min(checks, key=_pos) if checks else None


def _first_lists_access(fn):
    hits = [n for n in ast.walk(fn)
            if (isinstance(n, ast.Attribute) and n.attr == 'lists_mgr')
            or (isinstance(n, ast.Name) and n.id == 'lists_mgr')]
    return min(hits, key=_pos) if hits else None


def _login_first_violations(source: str, name: str):
    tree = ast.parse(source)
    fns = _functions(tree, name)
    if not fns:
        return [f'{name}: function not found']
    problems = []
    for fn in fns:
        check = _first_login_check(fn)
        if check is None:
            problems.append(f'{name}@{fn.lineno}: no login check')
            continue
        access = _first_lists_access(fn)
        if access is not None and _pos(access) < _pos(check):
            problems.append(
                f'{name}@{fn.lineno}: lists_mgr read at line {access.lineno} before '
                f'the login check at line {check.lineno}')
    return problems


def test_login_first_guard_can_fail():
    """The guard itself: seeded violations must be caught."""
    missing = 'def f():\n    x = state.lists_mgr\n'
    late = ('async def f():\n    x = state.lists_mgr\n'
            '    if not GlobalAuthState.is_logged_in():\n        return\n')
    comment_only = ('def f():\n    # GlobalAuthState.is_logged_in()\n'
                    '    x = state.lists_mgr\n')
    same_line = ('def f():\n    x = state.lists_mgr if GlobalAuthState.is_logged_in() else None\n')
    good = ('async def f():\n    if not GlobalAuthState.is_logged_in():\n        return\n'
            '    x = state.lists_mgr\n')
    good_helper = ('def f():\n    if not require_login_for_lists():\n        return\n'
                   '    lists_mgr = state.lists_mgr\n')
    assert _login_first_violations(missing, 'f')
    assert _login_first_violations(late, 'f')
    assert _login_first_violations(comment_only, 'f')
    assert _login_first_violations(same_line, 'f')
    assert _login_first_violations(good, 'nope') == ['nope: function not found']
    assert _login_first_violations(good, 'f') == []
    assert _login_first_violations(good_helper, 'f') == []


@pytest.mark.parametrize('rel_path,func_name', _LOGIN_FIRST)
def test_every_list_entry_point_checks_login_first(rel_path, func_name):
    source = (REPO_ROOT / rel_path).read_text(encoding='utf-8')
    assert _login_first_violations(source, func_name) == []


def test_visual_similarity_join_checks_login_before_closing_dialog():
    """An anonymous 'Add as Join' click must not dismiss the dialog for nothing."""
    source = (REPO_ROOT / 'web/components/visual_similarity_dialog.py').read_text(encoding='utf-8')
    fns = _functions(ast.parse(source), '_add_as_join')
    assert fns, '_add_as_join not found'
    for fn in fns:
        check = _first_login_check(fn)
        assert check is not None, '_add_as_join makes no login check'
        closes = [n for n in ast.walk(fn)
                  if isinstance(n, ast.Call) and _call_name(n) == 'close']
        assert closes, '_add_as_join no longer closes the dialog; revisit this guard'
        assert _pos(check) < _pos(min(closes, key=_pos))


# ---------------------------------------------------------------------------
# 5. "Move to account" is gone
# ---------------------------------------------------------------------------

def _code_only(path: pathlib.Path) -> str:
    return '\n'.join(
        line for line in path.read_text(encoding='utf-8').splitlines()
        if not line.lstrip().startswith('#')
    )


def test_migration_card_and_dialog_are_gone():
    lists_src = _code_only(REPO_ROOT / 'web/pages/lists.py')
    for needle in ('show_migration_dialog', 'has_local_lists', 'get_local_lists_mgr'):
        assert needle not in lists_src, f'web/pages/lists.py still references {needle}'
    offenders = []
    for path in sorted((REPO_ROOT / 'web').rglob('*.py')):
        code = _code_only(path)
        for needle in ('.migrate_local_to_user(', '.get_local_lists_mgr('):
            if needle in code:
                offenders.append(f'{path.relative_to(REPO_ROOT)}: {needle}')
    assert offenders == []
    from web.state import AppState
    assert not hasattr(AppState, 'get_local_lists_mgr')
