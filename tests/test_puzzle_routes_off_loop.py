# -*- coding: utf-8 -*-
"""Fragment Puzzle POST routes keep their heavy work off the event loop.

* ``/api/puzzle_process``: Pillow's check, background removal and the cache
  write run in a worker thread (up to seconds of CPU at 2000 px); an offload
  that gives no answer (the app stopping) is a 503;
* ``/api/puzzle_export``: composing the up-to-3000 px PNG and encoding it run
  in a worker thread; the route shares the puzzle rate limit, takes at most
  ``_EXPORT_MAX_FRAGMENTS`` fragments (413 above it), and a fragment it cannot
  read is a 400, not a 500.

Both routes are ``async def`` on the single event loop that serves every
visitor. The spies record where they run: ``browser_key_for_request`` runs on
the loop before the offload (the reference point), the heavy calls must not.
``asyncio.get_running_loop()`` succeeds on the loop and raises RuntimeError in
a worker thread.

Same fixture and real routes as tests/test_puzzle_upload_owner_scope.py.
"""
from __future__ import annotations

import asyncio
import threading

import pytest
from starlette.testclient import TestClient

from tests.test_puzzle_upload_owner_scope import (  # noqa: F401 (puzzle_app: the fixture)
    FL, _jpeg, _miss_and_upload, _params, puzzle_app,
)

_EXPORT_MAX_FRAGMENTS = 20


def _where():
    """(thread id, whether an event loop is running on this thread)."""
    try:
        asyncio.get_running_loop()
        on_loop = True
    except RuntimeError:
        on_loop = False
    return threading.get_ident(), on_loop


def _spy(seen, original):
    def _call(*args, **kwargs):
        seen.append(_where())
        return original(*args, **kwargs)
    return _call


@pytest.fixture
def loop_side(monkeypatch):
    """Where ``browser_key_for_request`` runs: on the loop, before any offload.
    The GET that hands out an upload token calls it too (a sync route, so in a
    worker thread); the last call is the POST under test."""
    import web.puzzle_image_access as access
    seen = []
    monkeypatch.setattr(access, 'browser_key_for_request',
                        _spy(seen, access.browser_key_for_request))
    return seen


def _assert_off_the_loop(loop_side, heavy):
    assert loop_side, 'the loop-side reference spy never ran'
    loop_thread, on_loop = loop_side[-1]
    assert on_loop, 'the reference spy is not on the event loop'
    assert heavy, 'the heavy call never ran'
    for thread, heavy_on_loop in heavy:
        assert thread != loop_thread, 'heavy work ran on the event-loop thread'
        assert not heavy_on_loop


def _fragment(**extra):
    return dict({'sys_id': '990000000000001', 'folio_label': '1r', 'fl_id': FL,
                 'shelfmark': 'ENA 1.1', 'processed': True,
                 'bg_removal_threshold': 30.0}, **extra)


# ── /api/puzzle_process ─────────────────────────────────────────────────────

def test_an_upload_is_processed_and_stored_off_the_event_loop(puzzle_app, loop_side,  # noqa: F811
                                                                monkeypatch):
    import shared.background_removal as bg
    app, service, _signed_in, _cache_dir = puzzle_app
    removed, stored = [], []
    monkeypatch.setattr(bg, 'remove_background', _spy(removed, bg.remove_background))
    monkeypatch.setattr(service, 'store_upload', _spy(stored, service.store_upload))

    resp = _miss_and_upload(TestClient(app), _jpeg((200, 30, 30)), processed=True)

    assert resp.content[:4] == b'\x89PNG'
    _assert_off_the_loop(loop_side, removed)
    _assert_off_the_loop(loop_side, stored)


def test_an_upload_the_offload_never_answers_is_a_503_and_keeps_nothing(puzzle_app,  # noqa: F811
                                                                        monkeypatch):
    import nicegui.run
    app, service, _signed_in, cache_dir = puzzle_app

    async def _no_answer(*_args, **_kwargs):
        return None  # what run.io_bound returns while the app is stopping

    monkeypatch.setattr(nicegui.run, 'io_bound', _no_answer)
    client = TestClient(app)
    miss = client.get('/api/puzzle_image', params=_params(False))
    resp = client.post('/api/puzzle_process', params=_params(False),
                       headers={'X-Puzzle-Upload-Token': miss.headers['X-Puzzle-Upload-Token'],
                                'Content-Type': 'image/jpeg'},
                       content=_jpeg((90, 90, 90)))

    assert resp.status_code == 503
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    browser_dir = cache_dir / '_browser'
    assert not browser_dir.exists() or not any(p.is_file() for p in browser_dir.rglob('*'))


# ── /api/puzzle_export ──────────────────────────────────────────────────────

def test_an_export_is_composed_off_the_event_loop(puzzle_app, loop_side, monkeypatch):  # noqa: F811
    import shared.puzzle_export as pe
    app = puzzle_app[0]
    client = TestClient(app)
    _miss_and_upload(client, _jpeg((30, 30, 200)), processed=True)
    composed = []
    monkeypatch.setattr(pe, 'compose_puzzle_export', _spy(composed, pe.compose_puzzle_export))

    resp = client.post('/api/puzzle_export', json={'fragments': [_fragment()]})

    assert resp.status_code == 200
    assert resp.content[:4] == b'\x89PNG'
    _assert_off_the_loop(loop_side, composed)


def test_an_export_of_more_than_the_cap_is_a_413_and_composes_nothing(puzzle_app,  # noqa: F811
                                                                      monkeypatch):
    import shared.puzzle_export as pe
    sizes = []
    monkeypatch.setattr(pe, 'compose_puzzle_export',
                        lambda fragments, *a, **k: sizes.append(len(fragments)))
    client = TestClient(puzzle_app[0])

    at_cap = client.post('/api/puzzle_export',
                         json={'fragments': [_fragment()] * _EXPORT_MAX_FRAGMENTS})
    over_cap = client.post('/api/puzzle_export',
                           json={'fragments': [_fragment()] * (_EXPORT_MAX_FRAGMENTS + 1)})

    assert at_cap.status_code == 500  # composed (nothing to draw), not refused
    assert at_cap.json() == {'error': 'export failed'}
    assert over_cap.status_code == 413
    assert sizes == [_EXPORT_MAX_FRAGMENTS]


@pytest.mark.parametrize('body', [
    {'fragments': [_fragment(no_such_field=1)]},
    {'fragments': [{'sys_id': '990000000000001', 'folio_label': '1r'}]},  # no fl_id
    {'fragments': ['not an object']},
    {'fragments': 5},
    ['not', 'an', 'object'],
    b'not json',
], ids=['unknown-field', 'missing-field', 'string-fragment', 'number-fragments',
        'list-body', 'not-json'])
def test_an_export_it_cannot_read_is_a_400(puzzle_app, body):  # noqa: F811
    client = TestClient(puzzle_app[0])
    if isinstance(body, bytes):
        resp = client.post('/api/puzzle_export', content=body,
                           headers={'Content-Type': 'application/json'})
    else:
        resp = client.post('/api/puzzle_export', json=body)

    assert resp.status_code == 400


def test_the_export_shares_the_puzzle_rate_limit(puzzle_app):  # noqa: F811
    client = TestClient(puzzle_app[0])

    answers = [client.post('/api/puzzle_export', json={'fragments': []}).status_code
               for _ in range(61)]

    assert answers[:60] == [400] * 60  # 'no fragments', each one counted
    assert answers[60] == 429
