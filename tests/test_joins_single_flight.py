# -*- coding: utf-8 -*-
"""fetch_connected_fragments runs ONE lookup per cache key at a time.

On a cold cache the Browse "Related Fragments" deferred fill and the toolbar
Joins button's count both ask for the same key within ~100 ms. Both used to
miss the cache and do the same Supabase + SQLite work in two pool threads.
Concurrent callers for one key now share the first caller's result; a caller
whose leader produced nothing cacheable does its own lookup, and force_refresh
always goes to the source.
"""
from __future__ import annotations

import threading
import time

import pytest

from web.components import joins_panel


@pytest.fixture
def slow_joins(monkeypatch):
    """Stub the joins sources; get_fragment_joins is slow and counts its calls."""
    with joins_panel._joins_cache_lock:
        joins_panel._joins_cache.clear()
    calls = []
    behaviour = {'raise_first': False}

    def fake_get_fragment_joins(**kwargs):
        calls.append(kwargs)
        time.sleep(0.3)
        if behaviour['raise_first'] and len(calls) == 1:
            raise RuntimeError('source down')
        return [{
            'id': 1, 'fragment_a_shelfmark': 'T-S 12.1', 'fragment_b_shelfmark': 'T-S 12.2',
            'fragment_a_sys_id': '99001', 'fragment_b_sys_id': '99002', 'join_type': 'direct_join',
            'notes': '', 'created_by_username': 'u', 'created_at': '2026-01-01T00:00:00Z',
            'status': 'proposed',
        }]

    monkeypatch.setattr(joins_panel, 'get_fragment_joins', fake_get_fragment_joins)
    monkeypatch.setattr(joins_panel, 'state', type('S', (), {'meta_mgr': None})())
    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])
    import web.fjms_service as fjms

    class _NoFjms:
        def is_available(self):
            return False

        def get_join_group(self, sid):
            return []

    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _NoFjms())
    yield calls, behaviour
    with joins_panel._joins_cache_lock:
        joins_panel._joins_cache.clear()


def _two_concurrent(fn):
    results = [None, None]

    def run(i):
        results[i] = fn()

    t1 = threading.Thread(target=run, args=(0,))
    t2 = threading.Thread(target=run, args=(1,))
    t1.start()
    time.sleep(0.05)  # the second caller arrives while the first is inside the lookup
    t2.start()
    t1.join(10)
    t2.join(10)
    return results


def test_concurrent_callers_for_one_key_share_one_lookup(slow_joins):
    calls, _ = slow_joins
    results = _two_concurrent(lambda: joins_panel.fetch_connected_fragments(document_id='99001'))
    assert len(calls) == 1, f'{len(calls)} lookups for one key'
    assert results[0] is not None and results[0] == results[1]
    assert results[0]['total_fragments'] == 2


def test_a_follower_whose_leader_failed_does_its_own_lookup(slow_joins):
    calls, behaviour = slow_joins
    behaviour['raise_first'] = True
    results = _two_concurrent(lambda: joins_panel.fetch_connected_fragments(document_id='99001'))
    assert len(calls) == 2
    assert results[1]['total_fragments'] == 2


def test_force_refresh_always_goes_to_the_source(slow_joins):
    calls, _ = slow_joins
    _two_concurrent(lambda: joins_panel.fetch_connected_fragments(document_id='99001', force_refresh=True))
    assert len(calls) == 2


def test_no_in_flight_marker_is_left_behind(slow_joins):
    _two_concurrent(lambda: joins_panel.fetch_connected_fragments(document_id='99001'))
    assert joins_panel._joins_inflight == {}
