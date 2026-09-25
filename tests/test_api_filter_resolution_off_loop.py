# -*- coding: utf-8 -*-
"""/api/search and /api/parallels resolve FJMS filters OFF the event loop.

``validate_filter_values`` and ``get_filter_sys_ids`` read the FJMS SQLite
sidecar; a date filter can take seconds. Both endpoints are ``async def`` on a
single-worker uvicorn, so calling them inline stalled every other request.
They now run through ``_resolve_fjms_filters_sync`` via
``loop.run_in_executor``, like the library-filter sibling
``_intersect_library_filter`` next to it.

The stubs record whether ``asyncio.get_running_loop()`` succeeds where they
run: it raises RuntimeError in an executor thread, and succeeds on the loop.
"""

import ast
import asyncio
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import init_search_api

REPO_ROOT = Path(__file__).resolve().parent.parent
SEARCH_API = REPO_ROOT / 'web' / 'search_api.py'
FJMS_HELPERS = {'validate_filter_values', 'get_filter_sys_ids'}


# ---------------------------------------------------------------------------
# Fixtures (shapes copied from tests/test_search_api_v2.py and
# tests/test_parallels_api.py)
# ---------------------------------------------------------------------------

class _StubSearcher:
    def __init__(self):
        self.calls = []

    def execute_search(self, **kwargs):
        self.calls.append(kwargs)
        return [{
            'uid': 'uid_001',
            'display': {'shelfmark': 'T-S 12.345', 'title': 'Test',
                        'id': '9912345678901234', 'library_code': 'CUL'},
            'raw_header': 'header_9912345678901234_IE99_P7',
            'snippet': 'a *match* here',
            'full_text': 'lorem ipsum',
            'sort_score': 0.5,
        }]

    def search_composition_logic(self, *args, **kwargs):
        return {'main': [], 'filtered': [], 'boundary_stats': None}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    from web.search_api import (
        _rate_limiter, _browse_rate_limiter, _parallels_rate_limiter,
        _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY,
        _PassageSemaphoreState, DEFAULT_PASSAGE_CONCURRENCY,
    )
    for rl in (_rate_limiter, _browse_rate_limiter, _parallels_rate_limiter):
        rl.reset_for_tests()
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)
    yield
    for rl in (_rate_limiter, _browse_rate_limiter, _parallels_rate_limiter):
        rl.reset_for_tests()
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)


@pytest.fixture
def client():
    from web.state import state
    saved_searcher, saved_meta = state.searcher, state.meta_mgr
    state.searcher = _StubSearcher()
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234', 'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    state.meta_mgr = mgr
    bare = FastAPI()
    init_search_api(app_override=bare)
    yield TestClient(bare)
    state.searcher, state.meta_mgr = saved_searcher, saved_meta


def _on_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


@pytest.fixture
def recorder(monkeypatch):
    from shared import fjms_service as fjms_module
    calls = []

    def _validate(d):
        calls.append(('validate', _on_loop()))

    def _lookup(**kw):
        calls.append(('lookup', _on_loop()))
        return {'9912345678901234'}

    monkeypatch.setattr(fjms_module, 'validate_filter_values', _validate)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', _lookup)
    return calls


def _post(client, endpoint):
    if endpoint == 'search':
        return client.post('/api/search', json={
            'query': 'x', 'search_mode': 'exact', 'filters': {'domains': ['liturgy']},
        })
    return client.post('/api/parallels', json={
        'text': 'hello world here', 'mode': 'exact', 'filters': {'domains': ['liturgy']},
    })


def test_search_filter_resolution_runs_off_the_event_loop(client, recorder):
    r = _post(client, 'search')
    assert r.status_code == 200, r.text
    assert [name for name, _ in recorder] == ['validate', 'lookup']
    assert recorder == [('validate', False), ('lookup', False)], (
        "FJMS filter resolution ran ON the event loop in /api/search: %r" % recorder)


def test_parallels_filter_resolution_runs_off_the_event_loop(client, recorder):
    r = _post(client, 'parallels')
    assert r.status_code == 200, r.text
    assert [name for name, _ in recorder] == ['validate', 'lookup']
    assert recorder == [('validate', False), ('lookup', False)], (
        "FJMS filter resolution ran ON the event loop in /api/parallels: %r" % recorder)


@pytest.mark.parametrize('endpoint', ['search', 'parallels'])
def test_offloaded_filter_validation_error_still_returns_4xx_on_both_endpoints(client, monkeypatch, endpoint):
    """Regression guard (green before and after): an APIError raised by validation
    inside the worker still reaches the client as 400, and the lookup never runs."""
    from shared import fjms_service as fjms_module
    from shared.api_errors import APIError
    lookup = MagicMock(return_value=None)

    def _validate(d):
        raise APIError('unresolvable_filter_value', 'no such domain', http_status=400)

    monkeypatch.setattr(fjms_module, 'validate_filter_values', _validate)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lookup)
    r = _post(client, endpoint)
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'unresolvable_filter_value'
    assert not lookup.called


# ---------------------------------------------------------------------------
# AST guard: no async def in web/search_api.py calls the FJMS helpers directly.
# ---------------------------------------------------------------------------

def _parents(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _nearest_function(node, parents) -> Optional[ast.AST]:
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return cur
        cur = parents.get(cur)
    return None


def _callee_name(call: ast.Call) -> Optional[str]:
    f = call.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return None


def _async_direct_calls(source: str):
    tree = ast.parse(source)
    parents = _parents(tree)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _callee_name(node) in FJMS_HELPERS:
            fn = _nearest_function(node, parents)
            if isinstance(fn, ast.AsyncFunctionDef):
                hits.append((node.lineno, fn.name, _callee_name(node)))
    return sorted(hits)


def test_no_async_def_in_search_api_calls_fjms_filter_helpers_directly():
    hits = _async_direct_calls(SEARCH_API.read_text(encoding='utf-8'))
    assert hits == [], (
        "FJMS filter helpers called directly inside an async def in web/search_api.py "
        "(they block the event loop; await them via run_in_executor): %r" % hits)


@pytest.mark.parametrize('snippet,expected', [
    ("async def ep():\n    m.validate_filter_values({})\n", 1),
    ("async def ep():\n    x = get_filter_sys_ids(domains=None)\n", 1),
    ("async def ep():\n    if True:\n        m.get_filter_sys_ids()\n", 1),
    ("def helper():\n    m.validate_filter_values({})\n    return m.get_filter_sys_ids()\n", 0),
    ("async def ep():\n    await loop.run_in_executor(None, helper, {})\n", 0),
    ("async def ep():\n    def inner():\n        return m.get_filter_sys_ids()\n    await loop.run_in_executor(None, inner)\n", 0),
])
def test_fjms_guard_detector_fires_on_synthetic_code(snippet, expected):
    """Regression guard, green before and after: proves the detector can fail."""
    assert len(_async_direct_calls(snippet)) == expected
