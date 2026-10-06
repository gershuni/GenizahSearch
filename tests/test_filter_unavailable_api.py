# -*- coding: utf-8 -*-
"""/api/search and /api/parallels answer 503 filter_unavailable, not 200 (#17).

Drives the real endpoints and the real FjmsService (a small sidecar file or
no sidecar at all), with only the searcher stubbed. A date filter is used
because validate_filter_values does not check dates, so the lookup itself
is what decides the answer.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_sidecar, FailingFinalExecute  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402
from web.search_api import init_search_api  # noqa: E402


class _StubSearcher:
    def __init__(self):
        self.search_calls = []
        self.composition_calls = []

    def execute_search(self, *args, **kwargs):
        self.search_calls.append(kwargs)
        return []

    def search_composition_logic(self, *args, **kwargs):
        self.composition_calls.append(kwargs)
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


@pytest.fixture
def searcher():
    from web.state import state
    saved_searcher, saved_meta = state.searcher, state.meta_mgr
    stub = _StubSearcher()
    state.searcher = stub
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    state.meta_mgr = mgr
    yield stub
    state.searcher, state.meta_mgr = saved_searcher, saved_meta


@pytest.fixture
def client():
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


@pytest.fixture
def failing_service(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=True))
    svc._conn = FailingFinalExecute(svc._conn)
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    return svc


@pytest.fixture
def absent_service(monkeypatch, tmp_path):
    svc = FjmsService(db_path=str(tmp_path / "absent.db"))
    assert not svc.is_available()
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    return svc


DATE_FILTER = {'date_from': 1000, 'date_to': 1100}


def _post(client, endpoint):
    if endpoint == 'search':
        return client.post('/api/search', json={
            'query': 'x', 'search_mode': 'exact', 'filters': DATE_FILTER,
        })
    return client.post('/api/parallels', json={
        'text': 'hello world here', 'mode': 'exact', 'filters': DATE_FILTER,
    })


def _assert_503(r, searcher):
    assert r.status_code == 503, (r.status_code, r.text[:400])
    assert r.json()['error']['code'] == 'filter_unavailable', r.text[:400]
    assert 'Retry-After' not in r.headers, "no honest retry time exists for this"
    assert not searcher.search_calls and not searcher.composition_calls, (
        "the search ran although its filters could not be applied")


@pytest.mark.parametrize('endpoint', ['search', 'parallels'])
def test_query_failure_is_503_not_an_empty_result(client, searcher, failing_service, endpoint):
    _assert_503(_post(client, endpoint), searcher)


@pytest.mark.parametrize('endpoint', ['search', 'parallels'])
def test_missing_sidecar_is_503_not_an_unfiltered_result(client, searcher, absent_service, endpoint):
    _assert_503(_post(client, endpoint), searcher)


@pytest.mark.parametrize('endpoint', ['search', 'parallels'])
def test_unfiltered_request_is_unaffected_by_a_missing_sidecar(client, searcher, absent_service, endpoint):
    if endpoint == 'search':
        r = client.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    else:
        r = client.post('/api/parallels', json={'text': 'hello world here', 'mode': 'exact'})
    assert r.status_code == 200, r.text[:400]


def test_filter_unavailable_is_a_registered_error_code():
    from shared.api_errors import ERROR_CODES
    assert 'filter_unavailable' in ERROR_CODES
