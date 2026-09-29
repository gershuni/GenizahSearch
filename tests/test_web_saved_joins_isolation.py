# -*- coding: utf-8 -*-
"""Fragment Puzzle: saved joins are kept per visitor.

On the web, a saved join ("draft") belongs to the account that saved it
(signed in) or to the browser that saved it (signed out), until it is
published. The desktop app has its own per-install joins.db and never passes
an owner, so it keeps seeing every row in its file.

Covers four layers, each through its real entry point:

* the service (``shared/puzzle_service.py``) with and without an owner;
* the owner wrapper (``web/saved_joins.py``) and the rule that it is the only
  web module that opens the service;
* the FastAPI app: the unused document routes are gone;
* the /puzzle page itself, driven by two NiceGUI ``User`` browsers.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import pathlib
import re
import sqlite3
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'saved-joins-test-secret-0123456789abcdefghij')

from shared.puzzle_model import PuzzleDocument, PuzzleFragment  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[1]


def _doc(title, doc_id=None, sys_id='990001', notes='note'):
    kwargs = {} if doc_id is None else {'id': doc_id}
    return PuzzleDocument(
        title=title, notes=notes,
        fragments=[PuzzleFragment(sys_id=sys_id, folio_label='1r', fl_id='FL-' + sys_id)],
        **kwargs,
    )


@pytest.fixture
def svc(tmp_path):
    from shared.puzzle_service import PuzzleService
    s = PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    yield s
    if s._conn is not None:
        s._conn.close()


@pytest.fixture
def shared_svc(tmp_path, monkeypatch):
    """Point the service singleton at a temp joins.db for the whole test."""
    import shared.puzzle_service as ps
    s = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, '_service_instance', s)
    yield s
    if s._conn is not None:
        s._conn.close()


# ── Service ──────────────────────────────────────────────────────────


def test_service_keeps_each_owner_apart(svc):
    a = _doc('A private')
    assert svc.save_document(a, owner_key='b:browser-a') == a.id

    # Another owner sees nothing of it and cannot change or remove it.
    assert svc.list_documents(owner_key='b:browser-b') == []
    assert svc.load_document(a.id, owner_key='b:browser-b') is None
    assert svc.list_documents_for_fragment(fl_id='FL-990001', owner_key='b:browser-b') == []
    assert svc.save_document(_doc('B title', doc_id=a.id), owner_key='b:browser-b') is None
    assert svc.delete_document(a.id, owner_key='b:browser-b') is False

    # The owner still has it, unchanged.
    mine = svc.load_document(a.id, owner_key='b:browser-a')
    assert mine is not None and mine.title == 'A private'
    assert [d['id'] for d in svc.list_documents(owner_key='b:browser-a')] == [a.id]
    assert svc.list_documents_for_fragment(fl_id='FL-990001', owner_key='b:browser-a') == [a.id]

    # The owner can update it, and the owner is kept on the row.
    a.title = 'A renamed'
    assert svc.save_document(a, owner_key='b:browser-a') == a.id
    assert svc.load_document(a.id, owner_key='b:browser-a').title == 'A renamed'
    assert svc.delete_document(a.id, owner_key='b:browser-a') is True
    assert svc.load_document(a.id) is None


def test_service_without_owner_behaves_as_before(svc):
    """The desktop never passes an owner: it sees and edits every row, and a
    desktop save keeps whatever owner a row already has."""
    a = _doc('A private')
    svc.save_document(a, owner_key='u:user-a')
    local = _doc('desktop join', sys_id='990002')
    svc.save_document(local)

    ids = {d['id'] for d in svc.list_documents()}
    assert ids == {a.id, local.id}
    assert svc.load_document(a.id).title == 'A private'

    a.title = 'edited on desktop'
    assert svc.save_document(a) == a.id
    assert svc.load_document(a.id, owner_key='u:user-a').title == 'edited on desktop'

    # A row with no owner is never shown to, or replaced by, an owner.
    assert [d['id'] for d in svc.list_documents(owner_key='u:user-a')] == [a.id]
    assert svc.load_document(local.id, owner_key='u:user-a') is None
    assert svc.save_document(_doc('x', doc_id=local.id), owner_key='u:user-a') is None
    assert svc.load_document(local.id).title == 'desktop join'


def test_existing_file_gains_owner_column_and_rows_stay(tmp_path):
    """A joins.db written before owners existed opens with every row intact;
    those rows have no owner, so no web visitor sees them."""
    path = tmp_path / 'joins.db'
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO meta VALUES ('schema_version', '2');
        CREATE TABLE join_documents (
            id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', notes TEXT DEFAULT '',
            join_type TEXT DEFAULT 'physical', fragments_json TEXT NOT NULL DEFAULT '[]',
            thumbnail_b64 TEXT DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE join_document_fragments (
            doc_id TEXT NOT NULL REFERENCES join_documents(id) ON DELETE CASCADE,
            fl_id TEXT, sys_id TEXT NOT NULL, folio_label TEXT NOT NULL,
            PRIMARY KEY (doc_id, sys_id, folio_label));
        INSERT INTO join_documents VALUES
            ('old-1', 'older join', 'n', 'physical', '[]', '', '2026-03-17', '2026-03-17');
    """)
    conn.commit()
    conn.close()

    from shared.puzzle_service import PuzzleService
    s = PuzzleService(db_path=str(path), thread_safe=True)
    try:
        cols = {r[1] for r in s._conn.execute('PRAGMA table_info(join_documents)')}
        assert 'owner_key' in cols
        assert [d['id'] for d in s.list_documents()] == ['old-1']
        assert s.list_documents(owner_key='b:anyone') == []
        assert s.load_document('old-1', owner_key='b:anyone') is None
        assert s._conn.execute("SELECT owner_key FROM join_documents").fetchone()[0] is None
    finally:
        s._conn.close()


# ── Owner wrapper ───────────────────────────────────────────────────


def test_wrapper_refuses_to_work_without_an_owner():
    from web.saved_joins import NoVisitorKey, SavedJoins
    for bad in (None, '', '   ', 'u:', 'b:'):
        with pytest.raises(NoVisitorKey):
            SavedJoins(bad)


def test_owner_key_is_the_account_when_signed_in_else_the_browser(monkeypatch):
    import web.saved_joins as sj
    from web.auth_state import GlobalAuthState
    import web.safe_storage as st

    monkeypatch.setattr(st, 'ensure_session_uuid', lambda: True)
    monkeypatch.setattr(st, 'get_persisted_session_uuid', lambda: 'a' * 32)

    monkeypatch.setattr(GlobalAuthState, 'get_user_id', classmethod(lambda cls: 'user-123'))
    assert sj.owner_key() == 'u:user-123'

    monkeypatch.setattr(GlobalAuthState, 'get_user_id', classmethod(lambda cls: None))
    assert sj.owner_key() == 'b:' + 'a' * 32

    # No stored browser id: no owner at all (never a throwaway one).
    monkeypatch.setattr(st, 'get_persisted_session_uuid', lambda: None)
    assert sj.owner_key() is None
    with pytest.raises(sj.NoVisitorKey):
        sj.for_current_visitor()


def test_fork_is_saved_under_the_forking_visitor(shared_svc, monkeypatch):
    """The community-joins fork saves through the visitor's wrapper."""
    import web.pages.discoveries as disc
    import web.saved_joins as sj

    detail = {'id': 'pub-1', 'title': 'Published join', 'notes': '',
              'fragments_json': json.loads(_doc('p').to_json()), 'is_published': True}
    monkeypatch.setattr('shared.puzzle_publish_service.get_published_join_detail',
                        lambda client, join_id: dict(detail))
    monkeypatch.setattr('web.supabase_client.get_client', lambda: MagicMock())
    monkeypatch.setattr(sj, 'owner_key', lambda: 'b:browser-fork')
    navigated = []
    monkeypatch.setattr(disc.ui.navigate, 'to', lambda url: navigated.append(url))
    monkeypatch.setattr(disc.ui, 'notify', lambda *a, **k: None)

    async def _io_bound(fn, *args, **kwargs):
        return fn(*args, **kwargs)
    monkeypatch.setattr(disc.run, 'io_bound', _io_bound)

    asyncio.run(disc._fork_puzzle_join_and_navigate('pub-1'))

    mine = shared_svc.list_documents(owner_key='b:browser-fork')
    assert len(mine) == 1 and mine[0]['title'] == 'Fork of: Published join'
    assert navigated == [f"/puzzle?doc={mine[0]['id']}"]
    assert shared_svc.list_documents(owner_key='b:someone-else') == []


def _calls_get_puzzle_service(path: pathlib.Path) -> bool:
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else fn.attr if isinstance(fn, ast.Attribute) else None
            if name == 'get_puzzle_service':
                return True
    return False


def test_only_saved_joins_module_opens_the_puzzle_service():
    """Every web read or write of a saved join must carry an owner, so the
    service is opened in exactly one web module."""
    offenders = sorted(
        str(p.relative_to(REPO)).replace('\\', '/')
        for p in (REPO / 'web').rglob('*.py')
        if p.name != 'saved_joins.py' and _calls_get_puzzle_service(p)
    )
    assert offenders == []
    assert _calls_get_puzzle_service(REPO / 'web' / 'saved_joins.py')


# ── FastAPI app ─────────────────────────────────────────────────────


def test_puzzle_document_routes_are_not_registered():
    from fastapi import FastAPI
    from web.api import init_api_routes

    bare = FastAPI()
    init_api_routes(app_override=bare)
    registered = {(m, r.path) for r in bare.routes for m in (getattr(r, 'methods', None) or ())}
    for method, path in [
        ('GET', '/api/puzzle_documents'),
        ('GET', '/api/puzzle_document/{doc_id}'),
        ('POST', '/api/puzzle_document'),
        ('DELETE', '/api/puzzle_document/{doc_id}'),
        ('GET', '/api/puzzle_thumbnail/{doc_id}'),
    ]:
        assert (method, path) not in registered, (method, path)
    # The stateless export route is kept.
    assert ('POST', '/api/puzzle_export') in registered


# ── The /puzzle page, two browsers ─────────────────────────────────


@asynccontextmanager
async def _two_browsers():
    import httpx
    from nicegui import core
    from nicegui import storage as nicegui_storage
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    import web.main  # noqa: F401  -- registers /puzzle on core.app
    from web.state import state as web_state

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    saved_is_ready = web_state.is_ready
    web_state.is_ready = lambda: True
    # An earlier test in the same process may already have sent a request
    # through core.app (test_openapi_scope.py and test_capabilities_api.py do),
    # which builds Starlette's middleware stack; set_storage_secret then cannot
    # add the session middleware. Start from an unbuilt stack and put the
    # middleware state back afterwards.
    saved_user_middleware = list(core.app.user_middleware)
    saved_stack = core.app.middleware_stack
    saved_secret = nicegui_storage.Storage.secret
    try:
        prepare_simulation()
        core.app.middleware_stack = None
        set_storage_secret('saved-joins-render-secret', {})
        os.environ['NICEGUI_USER_SIMULATION'] = 'true'
        try:
            async with core.app.router.lifespan_context(core.app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                             base_url='http://test') as ca, \
                        httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                          base_url='http://test') as cb:
                    users = []
                    for c in (ca, cb):
                        u = User(c)
                        # Answer the awaited canvas calls at once ("no state").
                        u.javascript_rules[re.compile(r'\s*window\.puzzleCanvas\.(getState|getCropState|clearAll)\(\)\s*$')] = lambda m: None
                        users.append(u)
                    yield users[0], users[1]
        finally:
            os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        web_state.is_ready = saved_is_ready
        core.app.user_middleware = saved_user_middleware
        core.app.middleware_stack = saved_stack
        nicegui_storage.Storage.secret = saved_secret


def _run(driver):
    from nicegui.context import context as nicegui_context
    saved_slot_stack = list(nicegui_context.slot_stack)

    async def _go():
        async with _two_browsers() as (a, b):
            await driver(a, b)

    try:
        asyncio.run(_go())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slot_stack)


def _visitor_key(user):
    """This browser's owner key: 'b:' + its stored session id (signed out)."""
    from nicegui.storage import request_contextvar
    from web.safe_storage import ensure_session_uuid, get_persisted_session_uuid
    token = request_contextvar.set(user._client.request)
    try:
        with user._client:
            ensure_session_uuid()
            uid = get_persisted_session_uuid()
            return 'b:' + uid if uid else None
    finally:
        request_contextvar.reset(token)


def _seed(svc, doc, owner):
    """Save ``doc`` for ``owner``. The fallback lets this page test also run
    against a checkout from before saved joins had owners."""
    try:
        return svc.save_document(doc, owner_key=owner)
    except TypeError:
        return svc.save_document(doc)


def _texts(user):
    """Every label text and input value on the page."""
    out = set()
    for el in user._client.elements.values():
        for attr in ('text', 'value'):
            v = getattr(el, attr, None)
            if isinstance(v, str) and v:
                out.add(v)
    return out


async def _open_drawer(user):
    from nicegui import ui
    from nicegui.testing.user_interaction import UserInteraction
    btns = [el for el in user._client.elements.values()
            if isinstance(el, ui.button) and el.props.get('icon') == 'folder_open']
    assert btns, 'Saved Joins button not found'
    UserInteraction(user, {btns[0]}, None).click()
    for _ in range(50):
        await asyncio.sleep(0.05)
        texts = _texts(user)
        if any(t for t in texts if 'Only you can see' in t or 'Saved in this browser' in t
               or 'רק אתה' in t or 'נשמרים בדפדפן' in t) or 'No saved joins' in texts:
            break
    await asyncio.sleep(0.2)


async def _save_canvas(user):
    """Press the toolbar Save button, then Save in the dialog it opens.
    Returns False when no dialog opened (an empty canvas is not saved)."""
    from nicegui import ui
    from nicegui.testing.user_interaction import UserInteraction
    from web.translations import tr

    def _buttons(pred):
        return [el for el in user._client.elements.values() if isinstance(el, ui.button) and pred(el)]

    def _is_save(el):
        return getattr(el, 'text', None) == tr('Save')

    already = set(map(id, _buttons(_is_save)))
    toolbar = _buttons(lambda el: el.props.get('icon') == 'save')
    assert toolbar, 'toolbar Save not found'
    UserInteraction(user, {toolbar[0]}, None).click()
    new = []
    for _ in range(40):
        await asyncio.sleep(0.05)
        new = [el for el in _buttons(_is_save) if id(el) not in already]
        if new:
            break
    if not new:
        return False
    UserInteraction(user, {new[-1]}, None).click()
    await asyncio.sleep(0.8)
    return True


def _row(path):
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        return conn.execute(
            'SELECT title, notes, fragments_json, updated_at FROM join_documents WHERE id = ?',
            ('doc-browser-a',)).fetchone()
    finally:
        conn.close()


def _fragment_dict():
    return {'sys_id': '990001', 'folio_label': '1r', 'fl_id': 'FL-990001', 'shelfmark': 'T-S 1.1'}


@pytest.mark.parametrize('published', [False, True], ids=['unpublished', 'published'])
def test_page_shows_and_saves_only_the_visitors_own_joins(shared_svc, published):
    detail = {
        'id': 'doc-browser-a', 'title': 'A published', 'notes': '',
        'fragments_json': {'fragments': [_fragment_dict()], 'join_type': 'physical'},
        'is_published': True,
    }

    def _published(client, join_id):
        return dict(detail) if (published and join_id == 'doc-browser-a') else None

    async def driver(a, b):
        await a.open('/puzzle')
        key_a = _visitor_key(a)
        assert key_a and key_a.startswith('b:')
        doc = PuzzleDocument(id='doc-browser-a', title='A private', notes='notes of browser a',
                             fragments=[PuzzleFragment(**_fragment_dict())])
        assert _seed(shared_svc, doc, key_a) == 'doc-browser-a'

        # Browser A sees its own join in the drawer (the control) ...
        await _open_drawer(a)
        assert 'A private' in _texts(a)

        # ... browser B does not.
        await b.open('/puzzle')
        key_b = _visitor_key(b)
        assert key_b and key_b != key_a
        await _open_drawer(b)
        assert 'A private' not in _texts(b)

        # B follows a link to A's join, then saves the canvas.
        before = _row(shared_svc._db_path)
        await b.open('/puzzle?doc=doc-browser-a')
        await asyncio.sleep(3.2)   # the page loads ?doc= after its start-up delay
        assert 'A private' not in _texts(b)
        assert 'notes of browser a' not in _texts(b)
        if published:
            # B got the published version, as an unsaved canvas.
            assert 'A published' in _texts(b)
        saved = await _save_canvas(b)
        ids = {d['id'] for d in shared_svc.list_documents()}
        if published:
            # Saving made B a join of its own.
            assert saved and 'doc-browser-a' in ids and len(ids) == 2
        else:
            # Nothing opened, so there was nothing to save.
            assert ids == {'doc-browser-a'}
        # A's row is untouched either way.
        assert _row(shared_svc._db_path) == before
        assert shared_svc.load_document('doc-browser-a').title == 'A private'

    with patch('shared.puzzle_publish_service.get_published_join_detail', side_effect=_published), \
            patch('web.supabase_client.get_client', return_value=MagicMock()), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)



# ── Two first opens of an older file at once ────────────────────────


_V2_SCHEMA = """
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    INSERT INTO meta VALUES ('schema_version', '2');
    CREATE TABLE join_documents (
        id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', notes TEXT NOT NULL DEFAULT '',
        join_type TEXT NOT NULL DEFAULT 'uncertain', fragments_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        thumbnail_b64 TEXT DEFAULT '');
    CREATE TABLE join_document_fragments (
        doc_id TEXT NOT NULL, fl_id TEXT NOT NULL, sys_id TEXT NOT NULL,
        FOREIGN KEY (doc_id) REFERENCES join_documents(id) ON DELETE CASCADE);
    INSERT INTO join_documents (id, title, notes, fragments_json, created_at, updated_at)
        VALUES ('old-1', 'older join', 'n', '[]', '2026-03-17', '2026-03-17');
"""


def test_two_first_opens_of_an_older_file_both_work(tmp_path, monkeypatch):
    """Two service objects open a v2 WAL joins.db for the first time at once.
    The second one starts (in another thread) just as the first is about to
    add the owner column. Both must end up usable, with the rows intact."""
    import threading

    import shared.puzzle_service as ps

    path = tmp_path / 'joins.db'
    conn = sqlite3.connect(str(path))
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript(_V2_SCHEMA)
    conn.commit()
    conn.close()

    real_connect = sqlite3.connect
    other = {}

    def _open_other():
        other['svc'] = ps.PuzzleService(db_path=str(path), thread_safe=True)

    class _FirstOpener(sqlite3.Connection):
        def execute(self, sql, *args):
            if 'ADD COLUMN owner_key' in sql and 'thread' not in other:
                t = threading.Thread(target=_open_other, daemon=True)
                other['thread'] = t
                t.start()
                t.join(timeout=1.0)  # the other opener runs as far as it can
            return super().execute(sql, *args)

    opened = []

    def _connect(*args, **kwargs):
        if not opened:
            kwargs['factory'] = _FirstOpener
        opened.append(1)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(ps.sqlite3, 'connect', _connect)
    first = ps.PuzzleService(db_path=str(path), thread_safe=True)
    assert 'thread' in other, 'the owner column was not added by the first opener'
    other['thread'].join(timeout=30)
    monkeypatch.undo()
    second = other.get('svc')
    try:
        for svc in (first, second):
            assert svc is not None and svc.is_available()
            assert [d['id'] for d in svc.list_documents()] == ['old-1']
        mine = _doc('mine')
        assert first.save_document(mine, owner_key='b:browser-a') == mine.id
        assert second.load_document(mine.id, owner_key='b:browser-a').title == 'mine'
        theirs = _doc('theirs', sys_id='990002')
        assert second.save_document(theirs, owner_key='b:browser-b') == theirs.id
        assert first.load_document(theirs.id, owner_key='b:browser-a') is None
    finally:
        for svc in (first, second):
            if svc is not None and svc._conn is not None:
                svc._conn.close()


def _v2_file(tmp_path, with_owner_column=False):
    path = tmp_path / 'joins.db'
    conn = sqlite3.connect(str(path))
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript(_V2_SCHEMA)
    if with_owner_column:
        conn.execute('ALTER TABLE join_documents ADD COLUMN owner_key TEXT')
    conn.commit()
    conn.close()
    return path


def test_owner_column_is_checked_and_added_in_one_immediate_transaction(tmp_path, monkeypatch):
    """The check that the column is missing and the ALTER that adds it run
    inside one BEGIN IMMEDIATE transaction, so a second opener waits and then
    finds the column instead of adding it twice."""
    import shared.puzzle_service as ps
    path = _v2_file(tmp_path)
    seen = []

    class _Recording(sqlite3.Connection):
        def execute(self, sql, *args):
            seen.append(' '.join(str(sql).split()))
            return super().execute(sql, *args)

    real_connect = sqlite3.connect
    monkeypatch.setattr(ps.sqlite3, 'connect',
                        lambda *a, **k: real_connect(*a, **dict(k, factory=_Recording)))
    svc = ps.PuzzleService(db_path=str(path), thread_safe=True)
    monkeypatch.undo()
    try:
        assert svc.is_available()
        alter = next(i for i, sql in enumerate(seen) if 'ADD COLUMN owner_key' in sql)
        begins = [i for i, sql in enumerate(seen[:alter]) if sql.upper().startswith('BEGIN IMMEDIATE')]
        assert begins, 'the owner column was added outside an IMMEDIATE transaction'
        assert any('table_info' in sql for sql in seen[begins[-1] + 1:alter]), \
            'the column was not checked again inside the transaction'
    finally:
        svc._conn.close()


def test_a_duplicate_column_error_counts_as_already_added(tmp_path, monkeypatch):
    """A writer outside this code path has already added the column, but this
    opener's checks did not see it: the ALTER then fails with 'duplicate
    column', which must be read as 'already added', not as a broken file."""
    import shared.puzzle_service as ps
    path = _v2_file(tmp_path, with_owner_column=True)
    state = {'altered': False}

    class _StaleSchema(sqlite3.Connection):
        def execute(self, sql, *args):
            text = str(sql)
            if 'ADD COLUMN owner_key' in text:
                state['altered'] = True
            elif 'table_info' in text and not state['altered']:
                return super().execute(
                    "SELECT * FROM pragma_table_info('join_documents') WHERE name != 'owner_key'")
            return super().execute(sql, *args)

    real_connect = sqlite3.connect
    monkeypatch.setattr(ps.sqlite3, 'connect',
                        lambda *a, **k: real_connect(*a, **dict(k, factory=_StaleSchema)))
    svc = ps.PuzzleService(db_path=str(path), thread_safe=True)
    monkeypatch.undo()
    try:
        assert state['altered'], 'the test did not reach the ALTER'
        assert svc.is_available()
        assert [d['id'] for d in svc.list_documents()] == ['old-1']
    finally:
        if svc._conn is not None:
            svc._conn.close()


# ── The /puzzle page: the tab's canvas belongs to one visitor ───────


def _in_page(user, fn):
    """Run ``fn`` in ``user``'s page context (storage, tab, request)."""
    from nicegui.storage import request_contextvar
    token = request_contextvar.set(user._client.request)
    try:
        with user._client:
            return fn()
    finally:
        request_contextvar.reset(token)


def _sign_in_as(user, account):
    """Sign this browser in as ``account`` (or out, for None), the way the
    login flow leaves it in user storage."""
    from nicegui import app

    def _set():
        if account is None:
            app.storage.user.pop('auth_user', None)
            app.storage.user.pop('auth_profile', None)
        else:
            app.storage.user['auth_user'] = {'id': account, 'email': account + '@example.org'}
            app.storage.user['auth_profile'] = {'role': 'user'}
    _in_page(user, _set)


def _current_owner(user):
    from web.saved_joins import owner_key
    return _in_page(user, owner_key)


def _tab(user):
    from nicegui import app
    return _in_page(user, lambda: dict(app.storage.tab))


class _AddFragmentRecorder:
    """A javascript rule that only records ``addFragment`` calls. Those calls
    are not awaited (they carry no request id), so it never answers them."""
    pattern = 'addFragment recorder'

    def __init__(self):
        self.calls = []

    def match(self, code):
        if 'window.puzzleCanvas.addFragment(' in code:
            self.calls.append(code)
        return None


def _add_fragment_calls(user):
    recorder = _AddFragmentRecorder()
    user.javascript_rules[recorder] = lambda m: None
    return recorder.calls


@pytest.mark.parametrize('first, then', [
    ('acct-a', 'acct-b'),   # another account in the same tab
    ('acct-a', None),       # signed out
], ids=['account-switch', 'sign-out'])
def test_tab_canvas_is_restored_only_for_the_visitor_who_left_it(shared_svc, first, then):
    async def driver(a, _b):
        calls = _add_fragment_calls(a)
        await a.open('/puzzle')
        _sign_in_as(a, first)
        await a.open('/puzzle')
        await asyncio.sleep(1.0)            # the page starts its canvas after 0.5 s
        owner_first = _current_owner(a)
        assert owner_first and owner_first.startswith('u:' if first else 'b:')

        # The first visitor has a draft open and a canvas in this tab.
        doc = PuzzleDocument(id='doc-first', title='First visitor draft', notes='first notes',
                             fragments=[PuzzleFragment(**_fragment_dict())])
        assert _seed(shared_svc, doc, owner_first) == 'doc-first'
        from nicegui import app

        def _leave_canvas():
            app.storage.tab['puzzle_fragments'] = {'990001,1r': {
                'sys_id': '990001', 'shelfmark': 'T-S 1.1', 'folio_label': '1r',
                'fl_id': 'FL-990001', 'threshold': 30, 'processed': True, 'size': 800}}
            app.storage.tab['puzzle_state'] = json.dumps({'990001,1r': {'x': 321, 'y': 654}})
            app.storage.tab['puzzle_doc_id'] = 'doc-first'
        _in_page(a, _leave_canvas)

        # The same visitor reopening the tab gets it back (the control).
        calls.clear()
        await a.open('/puzzle')
        await asyncio.sleep(1.5)
        assert any('FL-990001' in c for c in calls)
        assert 'First visitor draft' in _texts(a)

        # Someone else in the same tab gets an empty canvas ...
        _sign_in_as(a, then)
        owner_then = _current_owner(a)
        assert owner_then and owner_then != owner_first
        calls.clear()
        await a.open('/puzzle')
        await asyncio.sleep(1.5)
        assert not any('FL-990001' in c for c in calls)
        assert 'First visitor draft' not in _texts(a)
        tab = _tab(a)
        assert not tab.get('puzzle_fragments') and not tab.get('puzzle_state')
        assert not tab.get('puzzle_doc_id')

        # ... and cannot save the first visitor's arrangement as their own.
        await _save_canvas(a)
        assert shared_svc.list_documents(owner_key=owner_then) == []
        assert {d['id'] for d in shared_svc.list_documents()} == {'doc-first'}
        assert shared_svc.load_document('doc-first').title == 'First visitor draft'

    with patch('shared.puzzle_publish_service.get_published_join_detail', return_value=None), \
            patch('web.supabase_client.get_client', return_value=MagicMock()), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)


def test_signed_out_canvas_follows_the_visitor_into_their_account(shared_svc):
    """An unsaved canvas made in this browser while signed out is kept when the
    visitor signs in (signing in reloads the page). The draft the browser had
    open is not carried over: it stays with the browser, and saving creates a
    new join for the account."""
    async def driver(a, _b):
        calls = _add_fragment_calls(a)
        await a.open('/puzzle')
        await asyncio.sleep(1.0)
        owner_browser = _current_owner(a)
        assert owner_browser and owner_browser.startswith('b:')
        doc = PuzzleDocument(id='doc-first', title='Browser draft', notes='browser notes',
                             fragments=[PuzzleFragment(**_fragment_dict())])
        assert _seed(shared_svc, doc, owner_browser) == 'doc-first'
        from nicegui import app

        def _leave_canvas():
            app.storage.tab['puzzle_fragments'] = {'990001,1r': {
                'sys_id': '990001', 'shelfmark': 'T-S 1.1', 'folio_label': '1r',
                'fl_id': 'FL-990001', 'threshold': 30, 'processed': True, 'size': 800}}
            app.storage.tab['puzzle_state'] = json.dumps({'990001,1r': {'x': 321, 'y': 654}})
            app.storage.tab['puzzle_doc_id'] = 'doc-first'
        _in_page(a, _leave_canvas)

        _sign_in_as(a, 'acct-a')
        assert _current_owner(a) == 'u:acct-a'
        calls.clear()
        await a.open('/puzzle')
        await asyncio.sleep(1.5)
        assert any('FL-990001' in c for c in calls)     # the canvas came along ...
        assert not _tab(a).get('puzzle_doc_id')          # ... the browser's draft did not
        assert 'Browser draft' not in _texts(a)

        assert await _save_canvas(a)
        mine = shared_svc.list_documents(owner_key='u:acct-a')
        assert len(mine) == 1 and mine[0]['id'] != 'doc-first'
        assert shared_svc.load_document('doc-first').title == 'Browser draft'

    with patch('shared.puzzle_publish_service.get_published_join_detail', return_value=None), \
            patch('web.supabase_client.get_client', return_value=MagicMock()), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)


# ── The /puzzle page: background auto-save ─────────────────────────


def _fire(user, event, detail):
    """Deliver a canvas CustomEvent to the page, as the browser would."""
    from nicegui import helpers
    wraps = [el for el in user._client.elements.values() if 'puzzle-canvas-wrap' in el.classes]
    assert wraps, 'canvas element not found'
    wanted = helpers.event_type_to_camel_case(event)
    listeners = [lid for lid, lst in wraps[0]._event_listeners.items() if lst.type == wanted]
    assert listeners, f'no {event} listener'
    for lid in listeners:
        wraps[0]._handle_event({'listener_id': lid, 'args': {'detail': detail}})


def _canvas_rules(user, state):
    """Answer the canvas calls: getState from ``state`` (a dict the test can
    change), everything else with no value."""
    for rule in [r for r in user.javascript_rules if 'getState' in r.pattern]:
        del user.javascript_rules[rule]
    user.javascript_rules[re.compile(r'\s*window\.puzzleCanvas\.getState\(\)\s*$')] = \
        lambda m: json.dumps(state)
    user.javascript_rules[re.compile(
        r'\s*window\.puzzleCanvas(\.getCropState\(\)|\.clearAll\(\)| && window\.puzzleCanvas\.fitAll\(\))\s*$')] = \
        lambda m: None


def _saved_x(path):
    frags = json.loads(_row(path)[2])
    return [f['x'] for f in frags]


@pytest.mark.parametrize('published', [False, True], ids=['unpublished', 'published'])
def test_another_visitors_canvas_events_never_change_the_owners_draft(shared_svc, published):
    """Canvas events on another browser's page never change the owner's row.

    A guard rather than a regression test: web auto-save does not write at
    this point (its task has no page slot; see the tracker), so this also
    passes on older code. It pins the rule for when auto-save is enabled."""
    detail = {
        'id': 'doc-browser-a', 'title': 'A published', 'notes': '',
        'fragments_json': {'fragments': [_fragment_dict()], 'join_type': 'physical'},
        'is_published': True,
    }

    def _published(client, join_id):
        return dict(detail) if (published and join_id == 'doc-browser-a') else None

    async def driver(a, b):
        state_a = {'990001,1r': {'x': 321, 'y': 654}}
        _canvas_rules(a, state_a)
        await a.open('/puzzle')
        key_a = _visitor_key(a)
        doc = PuzzleDocument(id='doc-browser-a', title='A private', notes='notes of browser a',
                             fragments=[PuzzleFragment(**_fragment_dict())])
        assert _seed(shared_svc, doc, key_a) == 'doc-browser-a'
        assert _saved_x(shared_svc._db_path) == [0.0]

        # A opens its own draft; the fragment image finishes loading.
        await a.open('/puzzle?doc=doc-browser-a')
        await asyncio.sleep(3.2)
        _fire(a, 'puzzle-add-result', {'key': '990001,1r', 'success': True})
        await asyncio.sleep(3.0)            # past the 1.5 s auto-save delay

        # A moves the fragment on its own canvas.
        state_a['990001,1r'] = {'x': 555, 'y': 666}
        _fire(a, 'puzzle-object-modified', {'key': '990001,1r'})
        await asyncio.sleep(3.0)
        after_a = _row(shared_svc._db_path)
        assert after_a[0] == 'A private'

        # B follows the link to A's draft and moves things on its canvas.
        state_b = {'990001,1r': {'x': 999, 'y': 999}}
        _canvas_rules(b, state_b)
        await b.open('/puzzle?doc=doc-browser-a')
        await asyncio.sleep(3.2)
        assert _visitor_key(b) != key_a
        _fire(b, 'puzzle-add-result', {'key': '990001,1r', 'success': True})
        await asyncio.sleep(0.3)
        _fire(b, 'puzzle-object-modified', {'key': '990001,1r'})
        await asyncio.sleep(3.0)

        # A's row is exactly as it was before B's events.
        assert _row(shared_svc._db_path) == after_a
        assert {d['id'] for d in shared_svc.list_documents()} == {'doc-browser-a'}

    with patch('shared.puzzle_publish_service.get_published_join_detail', side_effect=_published), \
            patch('web.supabase_client.get_client', return_value=MagicMock()), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)


def test_open_page_saves_nothing_after_the_visitor_changes_elsewhere(shared_svc):
    """The page stays open while this browser signs in from another tab: its
    canvas still belongs to the visitor it was opened for, so Save writes
    nothing for the new account."""
    async def driver(a, _b):
        calls = _add_fragment_calls(a)
        await a.open('/puzzle')
        await asyncio.sleep(1.0)
        from nicegui import app

        def _leave_canvas():
            app.storage.tab['puzzle_fragments'] = {'990001,1r': {
                'sys_id': '990001', 'shelfmark': 'T-S 1.1', 'folio_label': '1r',
                'fl_id': 'FL-990001', 'threshold': 30, 'processed': True, 'size': 800}}
        _in_page(a, _leave_canvas)
        await a.open('/puzzle')
        await asyncio.sleep(1.5)
        assert any('FL-990001' in c for c in calls)   # the canvas is on the page

        _sign_in_as(a, 'acct-b')                      # as another tab would
        await _save_canvas(a)
        assert shared_svc.list_documents(owner_key='u:acct-b') == []
        assert shared_svc.list_documents() == []

    with patch('shared.puzzle_publish_service.get_published_join_detail', return_value=None), \
            patch('web.supabase_client.get_client', return_value=MagicMock()), \
            patch('shared.puzzle_export.generate_thumbnail', return_value=''), \
            patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()):
        _run(driver)
