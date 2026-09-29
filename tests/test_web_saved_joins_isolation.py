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
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    import web.main  # noqa: F401  -- registers /puzzle on core.app
    from web.state import state as web_state

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    saved_is_ready = web_state.is_ready
    web_state.is_ready = lambda: True
    try:
        prepare_simulation()
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
