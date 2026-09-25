# -*- coding: utf-8 -*-
"""The connected-fragments (joins) lookup never runs on the web event loop.

``fetch_connected_fragments`` does synchronous Supabase HTTP (fragment_joins +
profiles) and SQLite work. uvicorn runs one worker, so a call on the loop
stalls every other visitor for as long as Supabase takes. Two sites called it
directly: the /browse metadata panel's "Related Fragments" block (inside the
sync ``update_content``) and the Fragment Puzzle's "Add from joins" handler.
The Joins dialog also ran three community-puzzle PostgREST reads inline in its
``async def load_content``.

Now:
  * /browse reads the joins cache synchronously (``peek_connected_fragments``)
    and, on a miss, fills a placeholder slot later through
    ``load_connected_fragments_into`` (``run.io_bound`` + staleness guards),
    scheduled with ``background_tasks.create``;
  * the puzzle handler awaits ``run.io_bound(fetch_connected_fragments, ...)``;
  * the dialog's community reads live in a sync helper awaited via
    ``run.io_bound``.
"""

import ast
import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

joins_panel = pytest.importorskip('web.components.joins_panel')

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB = REPO_ROOT / 'web'
BROWSE = WEB / 'pages' / 'browse.py'
JOINS_PANEL = WEB / 'components' / 'joins_panel.py'
IO_BOUND_NAMES = {'io_bound', 'bounded_io_bound'}


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _parents(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _nearest_function(node, parents):
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return cur
        cur = parents.get(cur)
    return None


def _name(expr):
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return None


def _direct_fetch_calls(source):
    """Calls whose callee IS fetch_connected_fragments (not an io_bound argument)."""
    tree = ast.parse(source)
    return sorted(node.lineno for node in ast.walk(tree)
                  if isinstance(node, ast.Call) and _name(node.func) == 'fetch_connected_fragments')


def test_fetch_connected_fragments_is_never_called_directly_in_web():
    hits = []
    for path in sorted(WEB.rglob('*.py')):
        for line in _direct_fetch_calls(path.read_text(encoding='utf-8')):
            hits.append('%s:%d' % (path.relative_to(REPO_ROOT).as_posix(), line))
    assert hits == [], (
        "fetch_connected_fragments is called directly (blocking Supabase I/O on the "
        "event loop). Pass it to run.io_bound instead: %r" % hits)


@pytest.mark.parametrize('snippet,expected', [
    ("def f():\n    data = fetch_connected_fragments(document_id='1')\n", 1),
    ("async def f():\n    data = fetch_connected_fragments(document_id='1')\n", 1),
    ("async def f():\n    data = jp.fetch_connected_fragments(document_id='1')\n", 1),
    ("async def f():\n    data = await run.io_bound(fetch_connected_fragments, document_id='1')\n", 0),
    ("async def f():\n    data = await bounded_io_bound(sem, fetch_connected_fragments, document_id='1')\n", 0),
])
def test_direct_fetch_detector_fires_on_synthetic_code(snippet, expected):
    """Regression guard, green before and after: proves the detector can fail."""
    assert len(_direct_fetch_calls(snippet)) == expected


def _update_content_node():
    tree = ast.parse(BROWSE.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == 'create_browse_page':
            for inner in ast.walk(node):
                if isinstance(inner, ast.FunctionDef) and inner.name == 'update_content':
                    return inner
    raise AssertionError('update_content not found in create_browse_page')


def test_update_content_renders_joins_from_cache_or_a_deferred_fill():
    fn = _update_content_node()
    parents = _parents(fn)
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)]
    names = [_name(c.func) for c in calls]
    assert 'fetch_connected_fragments' not in names, (
        'update_content calls fetch_connected_fragments directly (on the event loop)')
    for node in ast.walk(fn):
        if isinstance(node, (ast.Name, ast.Attribute)) and _name(node) == 'fetch_connected_fragments':
            parent = parents.get(node)
            assert (isinstance(parent, ast.Call) and _name(parent.func) in IO_BOUND_NAMES
                    and node in parent.args), (
                'fetch_connected_fragments referenced in update_content outside run.io_bound')
            assert isinstance(_nearest_function(parent, parents), ast.AsyncFunctionDef)
    assert 'peek_connected_fragments' in names, 'update_content must read the joins cache first'
    fills = [c for c in calls if _name(c.func) == 'load_connected_fragments_into']
    assert fills, 'update_content must fill a cache miss through load_connected_fragments_into'
    for fill in fills:
        parent = parents.get(fill)
        assert isinstance(parent, ast.Call) and _name(parent.func) == 'create', (
            'the deferred fill must be scheduled with background_tasks.create (strong reference)')


# ---------------------------------------------------------------------------
# peek_connected_fragments: cache only, never the network
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_cache():
    with joins_panel._joins_cache_lock:
        saved = dict(joins_panel._joins_cache)
        joins_panel._joins_cache.clear()
    yield joins_panel._joins_cache
    with joins_panel._joins_cache_lock:
        joins_panel._joins_cache.clear()
        joins_panel._joins_cache.update(saved)


def test_peek_connected_fragments_is_cache_only(clean_cache, monkeypatch):
    from web.components.joins_panel import peek_connected_fragments

    def _boom(**kw):
        raise AssertionError('peek_connected_fragments reached the network')

    monkeypatch.setattr(joins_panel, 'get_fragment_joins', _boom)
    assert peek_connected_fragments(document_id='99001', pgpid=None) is None

    data = {'fragments': ['A', 'B'], 'joins': [], 'total_fragments': 2,
            'total_joins': 1, 'fragment_details': []}
    clean_cache['doc:99001:pgp:None'] = (time.time(), data)
    assert peek_connected_fragments(document_id='99001', pgpid=None) is data
    # the confirmed-only key is separate
    assert peek_connected_fragments(document_id='99001', pgpid=None, confirmed_only=True) is None

    clean_cache['doc:99001:pgp:None'] = (time.time() - joins_panel._CACHE_TTL - 1, data)
    assert peek_connected_fragments(document_id='99001', pgpid=None) is None


def test_peek_and_fetch_share_one_cache_key(clean_cache, monkeypatch):
    """Regression guard: what fetch_connected_fragments stores, peek finds."""
    from web.components.joins_panel import peek_connected_fragments
    monkeypatch.setattr(joins_panel, 'get_fragment_joins', lambda **kw: [])
    monkeypatch.setattr(joins_panel, 'WEB_PUZZLE_ENABLED', False)
    monkeypatch.setattr(joins_panel, 'state', SimpleNamespace(meta_mgr=None))
    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])
    import web.fjms_service as fjms
    monkeypatch.setattr(fjms, 'get_fjms_service',
                        lambda **kw: SimpleNamespace(is_available=lambda: False,
                                                     get_join_group=lambda sid: []))
    stored =joins_panel.fetch_connected_fragments(shelfmark='T-S 1.1', document_id='99002', pgpid=None)
    assert peek_connected_fragments(shelfmark='T-S 1.1', document_id='99002', pgpid=None) is stored


# ---------------------------------------------------------------------------
# load_connected_fragments_into: off-loop fetch, then a guarded render
# ---------------------------------------------------------------------------

class _Slot:
    def __init__(self, deleted=False, client_deleted=False):
        self.is_deleted = deleted
        self.client = SimpleNamespace(_deleted=client_deleted)
        self.entered = 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *exc):
        return False


def _on_loop():
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


@pytest.fixture
def fake_fetch(monkeypatch):
    seen = []
    data = {'fragments': ['A', 'B'], 'total_fragments': 2}

    def _fetch(**kw):
        seen.append((_on_loop(), kw))
        return data

    monkeypatch.setattr(joins_panel, 'fetch_connected_fragments', _fetch)
    return seen, data


def test_deferred_joins_fill_runs_fetch_off_loop_and_skips_stale_slots(fake_fetch, monkeypatch):
    from web.components.joins_panel import load_connected_fragments_into
    seen, data = fake_fetch
    rendered = []

    def _render(d):
        rendered.append(d)

    # (1)+(2): fetch in a worker, render once, inside the slot.
    slot = _Slot()
    asyncio.run(load_connected_fragments_into(slot, _render, lambda: True,
                                              shelfmark='T-S 1.1', document_id='99001', pgpid=None))
    assert seen == [(False, {'shelfmark': 'T-S 1.1', 'document_id': '99001', 'pgpid': None})]
    assert rendered == [data]
    assert slot.entered == 1

    # (3): stale in every way -> no render.
    for stale_slot, is_current in [
        (_Slot(deleted=True), lambda: True),
        (_Slot(client_deleted=True), lambda: True),
        (_Slot(), lambda: False),
    ]:
        rendered.clear()
        asyncio.run(load_connected_fragments_into(stale_slot, _render, is_current, document_id='99001'))
        assert rendered == []
        assert stale_slot.entered == 0


def test_deferred_joins_fill_handles_a_none_result(monkeypatch):
    """run.io_bound returns None when the app is stopping; nothing is rendered."""
    from web.components.joins_panel import load_connected_fragments_into
    monkeypatch.setattr(joins_panel, 'fetch_connected_fragments', lambda **kw: None)
    rendered = []
    slot = _Slot()
    asyncio.run(load_connected_fragments_into(slot, rendered.append, lambda: True, document_id='99001'))
    assert rendered == [] and slot.entered == 0


def test_deferred_joins_fill_swallows_a_torn_down_client(fake_fetch):
    """A RuntimeError from rendering into a deleted client must not escape the task."""
    from web.components.joins_panel import load_connected_fragments_into

    def _render(d):
        raise RuntimeError('The client this element belongs to has been deleted.')

    asyncio.run(load_connected_fragments_into(_Slot(), _render, lambda: True, document_id='99001'))


# ---------------------------------------------------------------------------
# Joins dialog: no blocking PostgREST call inside async code
# ---------------------------------------------------------------------------

def _async_execute_calls(source):
    tree = ast.parse(source)
    parents = _parents(tree)
    return sorted((n.lineno, _nearest_function(n, parents).name) for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and _name(n.func) == 'execute'
                  and isinstance(_nearest_function(n, parents), ast.AsyncFunctionDef))


def test_joins_dialog_has_no_blocking_supabase_execute_in_async_code():
    hits = _async_execute_calls(JOINS_PANEL.read_text(encoding='utf-8'))
    assert hits == [], (
        "Blocking PostgREST .execute() inside an async def in joins_panel.py; move it "
        "into a sync helper awaited via run.io_bound: %r" % hits)


@pytest.mark.parametrize('snippet,expected', [
    ("async def f():\n    c.table('x').select('*').execute()\n", 1),
    ("def f():\n    c.table('x').select('*').execute()\n", 0),
    ("async def f():\n    def g():\n        return c.table('x').select('*').execute()\n    await run.io_bound(g)\n", 0),
])
def test_execute_detector_fires_on_synthetic_code(snippet, expected):
    """Regression guard, green before and after: proves the detector can fail."""
    assert len(_async_execute_calls(snippet)) == expected
