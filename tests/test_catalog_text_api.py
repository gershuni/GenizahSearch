# -*- coding: utf-8 -*-
"""search_mode='catalog' on POST /api/search (catalogue plan §4.4): refused where it is not
served, its switches valid only with it, and its rows' catalogue fields in the response.
The engine is a stub: the rows themselves are tests/test_catalog_text_rows.py's."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import init_search_api


class _StubSearcher:
    def __init__(self):
        self.calls: list[dict] = []
        self.results: list[dict] = []

    def execute_search(self, **kwargs) -> list:
        self.calls.append(kwargs)
        return list(self.results)


@pytest.fixture(autouse=True)
def _api_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    from web.search_api import (DEFAULT_HEAVY_CONCURRENCY, DEFAULT_INTERACTIVE_CONCURRENCY,
                                _daily_budget, _HeavySemaphoreState, _InteractiveSemaphoreState,
                                _rate_limiter)

    def reset():
        _rate_limiter.reset_for_tests()
        _daily_budget.reset_for_tests()
        _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
        _InteractiveSemaphoreState.reset(DEFAULT_INTERACTIVE_CONCURRENCY)
    reset()
    yield
    reset()


@pytest.fixture
def stub(monkeypatch):
    from web.state import state
    fake = _StubSearcher()
    fake.results = [{
        'uid': '',
        'display': {'shelfmark': 'T-S 12.345', 'title': 'Test', 'id': '9912345678901234',
                    'library_code': 'CUL'},
        'raw_header': '',
        'snippet': 'A shipment of *cheese* from Sicily',
        'full_text': '',
        'metadata_only': True,
        'catalogue': {'source': 'fjms', 'field': 'FreeDesc', 'lang': 'en', 'automatic': True,
                      'near': False, 'exact': True},
    }]
    monkeypatch.setattr(state, 'searcher', fake)
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {'sys_id': '9912345678901234', 'ie_id': None,
                                                 'p_num': None, 'fl_id': None}
    monkeypatch.setattr(state, 'meta_mgr', mgr)
    return fake


@pytest.fixture
def client(stub):
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


@pytest.fixture
def served(monkeypatch):
    import web.catalog_assets as catalog_assets
    monkeypatch.setattr(catalog_assets, 'catalog_available', lambda: True)


def _code(response):
    return response.json().get('error', {}).get('code')


def test_refused_where_it_is_not_served(client, stub):
    from shared.api_errors import ERROR_CODES
    r = client.post('/api/search', json={'query': 'cheese', 'search_mode': 'catalog'})
    assert r.status_code == 400 and _code(r) == 'catalog_unavailable'
    assert 'catalog_unavailable' in ERROR_CODES     # one of the codes clients branch on
    assert stub.calls == []


def test_answered_with_the_catalogue_fields_where_it_is_served(client, stub, served):
    r = client.post('/api/search', json={'query': 'cheese', 'search_mode': 'catalog',
                                         'near_spellings': True})
    assert r.status_code == 200, r.text
    call = stub.calls[-1]
    assert call['mode'] == 'Catalog'
    assert call['catalog_options'] == {'include_translations': True, 'near_spellings': True}
    item = r.json()['results'][0]
    assert item['catalogue_source'] == 'fjms' and item['catalogue_field'] == 'FreeDesc'
    assert item['automatic_translation'] is True and item['near_spelling'] is False
    assert item['snippet'] == 'A shipment of cheese from Sicily'
    echo = r.json()['request']
    assert echo['near_spellings'] is True and echo['include_translations'] is True


def test_the_count_is_the_catalogues_exact_count_not_its_rows(client, stub, served):
    # The rows stop at CATALOG_MAX_ROWS; the first row carries the exact count.
    stub.results[0]['catalogue_search'] = {'near_forms': [], 'total': 12345}
    r = client.post('/api/search', json={'query': 'cheese', 'search_mode': 'catalog', 'limit': 5})
    assert r.status_code == 200, r.text
    assert r.json()['total'] == 12345 and len(r.json()['results']) == 1


def test_another_mode_never_reads_a_catalogue_count(client, stub):
    stub.results[0]['catalogue_search'] = {'near_forms': [], 'total': 12345}
    r = client.post('/api/search', json={'query': 'שלום', 'search_mode': 'exact'})
    assert r.status_code == 200, r.text
    assert r.json()['total'] == 1


def test_translations_can_be_left_out(client, stub, served):
    r = client.post('/api/search', json={'query': 'cheese', 'search_mode': 'catalog',
                                         'include_translations': False})
    assert r.status_code == 200, r.text
    assert stub.calls[-1]['catalog_options'] == {'include_translations': False, 'near_spellings': False}


def test_other_modes_keep_their_response_shape(client, stub):
    stub.results[0].pop('catalogue')
    r = client.post('/api/search', json={'query': 'שלום', 'search_mode': 'exact'})
    assert r.status_code == 200, r.text
    item = r.json()['results'][0]
    assert not {'catalogue_source', 'catalogue_field', 'automatic_translation', 'near_spelling'} & set(item)
    assert 'near_spellings' not in r.json()['request']
    assert 'catalog_options' not in stub.calls[-1]


@pytest.mark.parametrize('body', [
    {'query': 'x', 'search_mode': 'title', 'near_spellings': True},
    {'query': 'x', 'search_mode': 'exact', 'include_translations': False},
    {'query': 'x', 'search_mode': 'catalog', 'gap': 2},
    {'query': 'x', 'search_mode': 'catalog', 'sources': 'editions'},
])
def test_invalid_combinations(client, stub, served, body):
    r = client.post('/api/search', json=body)
    assert r.status_code == 400 and _code(r) == 'invalid_combination', r.text
    assert stub.calls == []
