"""A refused Responsa query is an API error with its code, never zero results
(docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md: since Phase 3, a bare star; Phase 1 refused more).

The engine raises ResponsaQueryRefused (shared/responsa_match.py, code
`responsa_unsupported_star`, or `responsa_query_too_large` for a query still over the
guard's limit after every step down -- a 500 until 2026-10-22). The API answers it as a
400 with that code, on the synchronous route and as a job; the research worker carries it
across the process.
Joins Lab and the web highlight: tests/test_responsa_refusal_surfaces.py.
"""
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.api_errors import ERROR_CODES
from shared.responsa_match import (
    GUARD_REFUSAL_CODE, ResponsaQueryRefused, STAR_REFUSAL_CODE, guard_refusal,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


class _RefusingSearcher:
    def __init__(self, refusal=ResponsaQueryRefused):
        self.calls = []
        self.refusal = refusal

    def execute_search(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise self.refusal()


def _client(refusal):
    from web.state import state
    from web.search_api import init_search_api
    saved = state.searcher
    state.searcher = _RefusingSearcher(refusal)
    bare = FastAPI()
    init_search_api(app_override=bare)
    yield TestClient(bare)
    state.searcher = saved


@pytest.fixture
def client(monkeypatch):
    yield from _client(ResponsaQueryRefused)


@pytest.fixture
def guard_client(monkeypatch):
    yield from _client(guard_refusal)


def test_the_code_is_in_the_contract():
    assert STAR_REFUSAL_CODE == 'responsa_unsupported_star'
    assert STAR_REFUSAL_CODE in ERROR_CODES
    assert GUARD_REFUSAL_CODE == 'responsa_query_too_large'
    assert GUARD_REFUSAL_CODE in ERROR_CODES


def test_the_api_answers_a_refused_query_with_its_code(client):
    with client:
        r = client.post('/api/search', json={'query': '*', 'search_mode': 'responsa'})
    assert r.status_code == 400, r.text
    error = r.json()['error']
    assert error['code'] == STAR_REFUSAL_CODE
    assert '*' in error['message']


def _job_result(client, query):
    with client:
        created = client.post('/api/search/jobs', json={'query': query, 'search_mode': 'responsa'})
        assert created.status_code == 202, created.text
        urls = created.json()
        deadline = time.monotonic() + 5
        while client.get(urls['status_url']).json()['state'] not in ('completed', 'failed'):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        return client.get(urls['result_url'])


def test_a_job_answers_a_refused_query_with_its_code(client):
    result = _job_result(client, '*')
    body = result.json()
    assert result.status_code == 400, result.text
    assert body['error']['code'] == STAR_REFUSAL_CODE


QUERY_OVER_THE_GUARD = 'א' + '(ב/ג)' * 13


def test_the_api_answers_a_query_over_the_guard_with_its_code(guard_client):
    with guard_client:
        r = guard_client.post('/api/search', json={'query': QUERY_OVER_THE_GUARD, 'search_mode': 'responsa'})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == GUARD_REFUSAL_CODE


def test_a_job_answers_a_query_over_the_guard_with_its_code(guard_client):
    result = _job_result(guard_client, QUERY_OVER_THE_GUARD)
    assert result.status_code == 400, result.text
    assert result.json()['error']['code'] == GUARD_REFUSAL_CODE


def test_the_worker_hands_the_refusal_back_with_its_code():
    from web.research_jobs import raise_worker_error
    message = str(ResponsaQueryRefused())
    with pytest.raises(ResponsaQueryRefused) as info:
        raise_worker_error({'error': message, 'validation': True, 'code': STAR_REFUSAL_CODE})
    assert info.value.code == STAR_REFUSAL_CODE and str(info.value) == message
    # Another validation error stays a plain ValueError.
    with pytest.raises(ValueError) as other:
        raise_worker_error({'error': 'bad gap', 'validation': True, 'code': None})
    assert not isinstance(other.value, ResponsaQueryRefused)
