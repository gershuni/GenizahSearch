# -*- coding: utf-8 -*-
"""POST /api/parallels with a text shorter than chunk_size.

Through the real endpoint, the real shared.parallels_service, and the REAL
SearchEngine.search_composition_logic -- only the Tantivy index is mocked.
The mock-searcher tests in tests/test_parallels_api.py cannot see this
defect: their fake searcher answers whatever it is told, so the engine's
"fewer words than chunk_size" branch is never reached.
"""
from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import (
    init_search_api,
    _rate_limiter,
    _browse_rate_limiter,
    _parallels_rate_limiter,
)

_WORDS = ['אבגד', 'הוזח', 'טיכל', 'מנסע', 'פצקר', 'שתאב', 'גדהו', 'זחטי']


@pytest.fixture
def client():
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()


@pytest.fixture(autouse=True)
def _reset_heavy_semaphore():
    from web.search_api import _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    yield
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)


@pytest.fixture
def real_engine():
    """state.searcher = a real SearchEngine whose index is a mock."""
    from genizah_core import SearchEngine
    from web.state import state

    engine = SearchEngine.__new__(SearchEngine)
    engine.index = MagicMock(name='genizah_index')
    engine.index.parse_query.return_value = MagicMock(name='q')
    hits = MagicMock()
    hits.hits = []
    engine.searcher = MagicMock(name='genizah_searcher')
    engine.searcher.search.return_value = hits
    engine.local_index = None
    engine.local_searcher = None
    engine._my_library_tab_ref = None
    engine._has_content_search = False
    engine.build_tantivy_query = MagicMock(return_value='content:x')
    engine.build_regex_pattern = MagicMock(return_value=re.compile('x'))
    engine._load_browse_map = MagicMock(return_value={})

    saved = (state.searcher, state.meta_mgr)
    state.searcher = engine
    state.meta_mgr = MagicMock(name='meta_mgr')
    yield engine
    state.searcher, state.meta_mgr = saved


def _codes(body):
    return [w.get('code') if isinstance(w, dict) else w for w in body.get('warnings', [])]


def _notice(body, code):
    for w in body.get('warnings', []):
        if isinstance(w, dict) and w.get('code') == code:
            return w
    return None


def test_short_text_is_searched_and_the_response_says_so(client, clean_env, real_engine):
    r = client.post('/api/parallels', json={'text': ' '.join(_WORDS[:3])})
    assert r.status_code == 200, r.text
    body = r.json()
    queried = [list(c.args[0]) for c in real_engine.build_tantivy_query.call_args_list]
    assert queried == [_WORDS[:3]], queried
    assert _notice(body, 'text_shorter_than_chunk_size') == {
        'code': 'text_shorter_than_chunk_size',
        'words': 3, 'chunk_size': 5, 'effective_chunk_size': 3,
    }, body.get('warnings')
    # The echo reports what was asked (documented as unmodified); the
    # warning carries what was used.
    assert body['request']['chunk_size'] == 5


def test_one_word_warns_instead_of_answering_nothing(client, clean_env, real_engine):
    r = client.post('/api/parallels', json={'text': _WORDS[0]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['results'] == []
    assert _notice(body, 'text_too_short') == {
        'code': 'text_too_short', 'words': 1, 'minimum': 2}, body.get('warnings')


def test_jobs_endpoint_carries_the_same_warning(client, clean_env, real_engine):
    """/api/parallels/jobs (the background route) reports it too."""
    import time
    with client:
        created = client.post('/api/parallels/jobs', json={'text': ' '.join(_WORDS[:3])})
        assert created.status_code == 202, created.text
        urls = created.json()
        deadline = time.monotonic() + 5
        while client.get(urls['status_url']).json()['state'] not in ('completed', 'failed'):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        body = client.get(urls['result_url']).json()
    assert 'text_shorter_than_chunk_size' in _codes(body), body


def test_full_length_text_response_shape_is_unchanged(client, clean_env, real_engine):
    """Non-regression pin (passes before and after the fix): a text at least
    chunk_size long gets no new warning, the same 7-key request echo, and the
    same stride-1 windows."""
    r = client.post('/api/parallels', json={'text': ' '.join(_WORDS[:8])})
    assert r.status_code == 200, r.text
    body = r.json()
    assert not ({'text_shorter_than_chunk_size', 'text_too_short', 'chunk_size_raised'}
                & set(_codes(body)))
    assert set(body['request']) == {
        'mode', 'chunk_size', 'max_freq', 'boundary_options',
        'limit_effective', 'filters', 'method',
    }
    assert len(real_engine.build_tantivy_query.call_args_list) == 4


# GitHub review (Codex on #387, round 2): filters that match nothing answer without
# searching; the text's warnings must still be there, the same as a search gives.

_NOTICE_CODES = {'text_shorter_than_chunk_size', 'text_too_short', 'chunk_size_raised',
                 'min_chunk_matches_lowered'}


@pytest.mark.parametrize('words,chunk_size', [(1, 5), (3, 5), (8, 5), (2, 3)])
def test_filters_that_match_nothing_still_give_the_texts_warnings(client, clean_env, real_engine,
                                                                   monkeypatch, words, chunk_size):
    import web.search_api as search_api
    payload = {'text': ' '.join(_WORDS[:words]), 'chunk_size': chunk_size}
    searched = client.post('/api/parallels', json=payload)
    assert searched.status_code == 200, searched.text
    expected = [w for w in searched.json().get('warnings', [])
                if isinstance(w, dict) and w.get('code') in _NOTICE_CODES]
    real_engine.build_tantivy_query.reset_mock()
    monkeypatch.setattr(search_api, '_resolve_fjms_filters_sync', lambda filters: set())
    empty = client.post('/api/parallels', json={**payload, 'filters': {'domains': ['Halakha']}})
    assert empty.status_code == 200, empty.text
    assert empty.json()['results'] == []
    assert not real_engine.build_tantivy_query.called, 'nothing is searched'
    got = [w for w in empty.json().get('warnings', [])
           if isinstance(w, dict) and w.get('code') in _NOTICE_CODES]
    assert got == expected
    assert bool(expected) == (words < chunk_size), expected
