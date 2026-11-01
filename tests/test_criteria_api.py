# -*- coding: utf-8 -*-
"""filters.criteria on /api/search and /api/parallels, the older keys read as
criteria, and GET /api/criteria (PR 2 of docs/plans/CATALOGUE_CRITERIA_PLAN.md,
§6.4).

Drives the real endpoints over the real FjmsService and the criteria fixture
sidecar (tests/criteria_sidecar.py), with only the searcher stubbed, so what is
asserted is what reaches the engine: the restrict set, or no search at all.
"""
import os
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.catalog_criteria import (  # noqa: E402
    CRITERIA_KEYS, SECTIONS, criteria_with_legacy_keys, legacy_conflicts,
)
from shared.fjms_service import FilterUnavailable, FjmsService  # noqa: E402
from web.search_api import FiltersModel, init_search_api  # noqa: E402

NAMES = {A: 'A', B: 'B', C: 'C', D: 'D', E: 'E', F: 'F'}


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


def _meta(corpus=lambda timeout=30.0: UNIVERSE):
    return SimpleNamespace(corpus_sys_ids=corpus,
                           get_meta_for_id=lambda sid: ('T-S 1', ''),
                           get_library_for_id=lambda sid: 'CUL')


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    from web.search_api import (
        _rate_limiter, _parallels_rate_limiter, _criteria_rate_limiter,
        _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY,
    )
    for rl in (_rate_limiter, _parallels_rate_limiter, _criteria_rate_limiter):
        rl.reset_for_tests()
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    yield
    for rl in (_rate_limiter, _parallels_rate_limiter, _criteria_rate_limiter):
        rl.reset_for_tests()


@pytest.fixture
def searcher():
    from web.state import state
    saved = state.searcher, state.meta_mgr
    stub = _StubSearcher()
    state.searcher, state.meta_mgr = stub, _meta()
    yield stub
    state.searcher, state.meta_mgr = saved


@pytest.fixture(scope='module')
def new_file(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('n') / 'f.db')


@pytest.fixture(scope='module')
def old_file(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('o') / 'f.db', upgrade=False)


@pytest.fixture
def svc(monkeypatch, new_file):
    service = FjmsService(db_path=new_file)
    monkeypatch.setattr(fjms_service, '_default_service', service)
    return service


@pytest.fixture
def old_svc(monkeypatch, old_file):
    service = FjmsService(db_path=old_file)
    monkeypatch.setattr(fjms_service, '_default_service', service)
    return service


@pytest.fixture
def client():
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


def _search(client, filters):
    return client.post('/api/search', json={'query': 'x', 'search_mode': 'exact', 'filters': filters})


def _restricted(searcher):
    """The restrict set the search ran with; set() when it short-circuited."""
    if not searcher.search_calls and not searcher.composition_calls:
        return set()
    call = (searcher.search_calls or searcher.composition_calls)[-1]
    return {NAMES[s] for s in call['restrict_sys_ids']}


def _error(r, status, code):
    assert r.status_code == status, (r.status_code, r.text[:400])
    assert r.json()['error']['code'] == code, r.text[:400]
    return r.json()['error']['message']


# ── filters.criteria reaches the engine ─────────────────────────────────

@pytest.mark.parametrize('criteria,expected', [
    ({'style': {'values': ['Square']}}, {'A'}),
    # "all except" and "not recorded" need the corpus: uncatalogued D and E come in.
    ({'region': {'values': ['Oriental'], 'mode': 'except'}}, {'B', 'C', 'D', 'E', 'F'}),
    ({'style': {'values': ['_none']}}, {'C', 'D', 'E', 'F'}),
    ({'lines': {'min': 20, 'max': 25, 'source': 'measured'}}, {'A', 'B', 'D'}),
    ({'period': {'min': 1000, 'max': 1060}}, {'A', 'B'}),
    ({'attributes': {'values': ['colophon', 'massorah']}}, {'B', 'C'}),
    ({'material': {'values': ['Paper'], 'source': 'measured'}}, {'D'}),
    ({'style': {'values': ['Square']}, 'material': {'values': ['Vellum']}}, {'A'}),
])
def test_criteria_restrict_the_search(client, searcher, svc, criteria, expected):
    r = _search(client, {'criteria': criteria})
    assert r.status_code == 200, r.text[:400]
    assert _restricted(searcher) == expected


def test_criteria_restrict_the_parallels(client, searcher, svc):
    r = client.post('/api/parallels', json={
        'text': 'hello world here', 'mode': 'exact',
        'filters': {'criteria': {'region': {'values': ['Oriental'], 'mode': 'except'}}}})
    assert r.status_code == 200, r.text[:400]
    assert _restricted(searcher) == {'B', 'C', 'D', 'E', 'F'}


# ── the older keys, read as criteria ────────────────────────────────────

def test_materials_is_material_from_the_catalogue(client, searcher, svc):
    assert _search(client, {'materials': ['Paper']}).status_code == 200
    assert _restricted(searcher) == {'A'}      # not D, whose Paper is measured


def test_dates_are_the_period_span_overlap(client, searcher, svc):
    """A was copied 1050-1100: 1080-1090 overlaps the span although the start
    year (the old rule) is outside it."""
    assert _search(client, {'date_from': 1080, 'date_to': 1090}).status_code == 200
    assert _restricted(searcher) == {'A'}
    assert svc.get_filter_sys_ids(date_from=1080, date_to=1090) == set()  # the old rule


@pytest.mark.parametrize('filters', [
    {'materials': ['Paper'], 'criteria': {'material': {'values': ['Vellum']}}},
    {'date_from': 1000, 'criteria': {'period': {'max': 1200}}},
    {'date_to': 1100, 'criteria': {'period': {'not_recorded': True}}},
    # An entry that asks nothing still says which of the two the caller meant (Codex).
    {'date_from': 1000, 'criteria': {'period': {}}},
    {'materials': ['Paper'], 'criteria': {'material': {'values': []}}},
])
def test_an_older_key_beside_its_criterion_is_refused(client, searcher, svc, filters):
    message = _error(_search(client, filters), 400, 'invalid_combination')
    old = next(k for k in filters if k != 'criteria')
    assert f'filters.{old}' in message and 'filters.criteria.' in message
    assert not searcher.search_calls


def test_legacy_helpers():
    assert legacy_conflicts({'materials': ['Paper'], 'criteria': {'style': {}}}) == []
    assert legacy_conflicts({'date_to': 0, 'criteria': {'period': {}}}) == [('date_to', 'period')]
    assert criteria_with_legacy_keys({'materials': ['Paper'], 'date_from': 900,
                                      'criteria': {'style': {'values': ['Square']}}}) == {
        'period': {'min': 900},
        'style': {'values': ['Square'], 'mode': 'only'},
        'material': {'source': 'catalogue', 'values': ['Paper'], 'mode': 'only'},
    }
    assert criteria_with_legacy_keys({}) == {}


# ── refused before any lookup ───────────────────────────────────────────

@pytest.mark.parametrize('criteria,code', [
    ({'style': {'values': ['Square'], 'source': 'catalogue'}}, 'invalid_request'),  # one source
    ({'lines': {'min': 2.5}}, 'invalid_request'),                 # whole numbers
    ({'scribe': {'values': ['x']}}, 'invalid_request'),           # no such criterion
    ({'attributes': {'values': ['_none']}}, 'invalid_request'),   # attributes: yes-only
    ({'attributes': {'values': ['gilded']}}, 'invalid_request'),
    ({'style': {'values': ['square']}}, 'unresolvable_filter_value'),  # values are exact
    ({'material': {'values': ['Vellum'], 'source': 'measured'}}, 'unresolvable_filter_value'),
    # Nothing recorded at all: every value is unknown -- a 400, not a 503 (Codex).
    ({'alphabet': {'values': ['Latin']}}, 'unresolvable_filter_value'),
    # Attributes are checked too: only those present are listed (Codex).
    ({'attributes': {'values': ['autograph']}}, 'unresolvable_filter_value'),
])
def test_malformed_or_unknown_criteria_are_400(client, searcher, svc, criteria, code):
    r = _search(client, {'criteria': criteria})
    message = _error(r, 400, code)
    named = [message, *r.json()['error'].get('fields', [])]   # Pydantic names the field there
    assert any('criteria.' in text for text in named), r.text[:400]
    assert not searcher.search_calls


def test_an_unknown_value_names_its_source_and_where_to_look(client, searcher, svc):
    message = _error(_search(client, {'criteria': {'material': {'values': ['Vellum'],
                                                                'source': 'measured'}}}),
                     400, 'unresolvable_filter_value')
    assert "source 'measured'" in message and '/api/criteria' in message


def test_values_are_checked_against_the_corpus_as_listed(client, searcher, svc):
    """Validation and GET /api/criteria agree: a value no corpus manuscript has
    is not listed, and is refused (Codex: validation had no universe)."""
    from web.state import state
    state.meta_mgr = _meta(lambda timeout=30.0: UNIVERSE - {A})
    listed = {v['value'] for v in _criterion(client.get('/api/criteria').json(), 'style')['values']}
    assert 'Square' not in listed
    _error(_search(client, {'criteria': {'style': {'values': ['Square']}}}), 400,
           'unresolvable_filter_value')


def test_nothing_recorded_is_an_unknown_value_not_an_outage(client, searcher, svc):
    """With no corpus list (no metadata manager yet) an empty value list is
    still an answer read from the file: the value is unknown (400), not 503."""
    from web.state import state
    state.meta_mgr = None
    _error(_search(client, {'criteria': {'alphabet': {'values': ['Latin']}}}), 400,
           'unresolvable_filter_value')


@pytest.mark.parametrize('key', ['script_styles', 'script_places'])
def test_the_unreleased_script_lists_are_refused(client, searcher, svc, key):
    _error(_search(client, {key: ['Square']}), 400, 'invalid_request')


def test_the_request_model_offers_every_registry_criterion():
    props = FiltersModel.model_json_schema()['$defs']['CriteriaModel']['properties']
    assert tuple(props) == CRITERIA_KEYS


# ── fail closed ─────────────────────────────────────────────────────────

@pytest.mark.parametrize('filters', [
    {'criteria': {'vocal_extent': {'values': ['Full']}}},
    {'criteria': {'columns': {'min': 2}}},
    {'date_from': 1000, 'date_to': 1100},
])
def test_a_5_0_0_file_is_503_for_the_new_criteria(client, searcher, old_svc, filters):
    message = _error(_search(client, filters), 503, 'filter_unavailable')
    assert 'column_missing' in message
    assert not searcher.search_calls


def test_a_5_0_0_file_still_answers_the_catalogue_lists(client, searcher, old_svc):
    assert _search(client, {'criteria': {'style': {'values': ['Square']}}}).status_code == 200
    assert _restricted(searcher) == {'A'}


def _unloaded(timeout=30.0):
    raise FilterUnavailable('corpus_unavailable', 'the list of all manuscripts is not loaded yet')


@pytest.mark.parametrize('meta', [_meta(_unloaded), None])
def test_no_corpus_list_is_503_for_all_except(client, searcher, svc, meta):
    from web.state import state
    state.meta_mgr = meta
    r = _search(client, {'criteria': {'region': {'values': ['Oriental'], 'mode': 'except'}}})
    assert 'corpus_unavailable' in _error(r, 503, 'filter_unavailable')
    assert not searcher.search_calls


# ── GET /api/criteria ───────────────────────────────────────────────────

def _criterion(body, key):
    return next(c for s in body['sections'] for c in s['criteria'] if c['key'] == key)


def test_the_vocabulary(client, searcher, svc):
    r = client.get('/api/criteria')
    assert r.status_code == 200, r.text[:400]
    body = r.json()
    assert (body['schema_version'], body['corpus'], body['uncatalogued']) == (1, 6, 2)
    assert [s['key'] for s in body['sections']] == [s.key for s in SECTIONS]
    assert [c['key'] for s in body['sections'] for c in s['criteria']] == list(CRITERIA_KEYS)
    style = _criterion(body, 'style')
    assert [(v['value'], v['count']) for v in style['values']] == [
        ('Semi-Cursive', 1), ('Square', 1), ('_none', 4)]
    assert style['label_he'] == 'כתב' and style['recorded'] == 2 and 'sources' not in style
    material = _criterion(body, 'material')
    assert material['sources'] == ['either', 'catalogue', 'measured']
    assert material['recorded_by_source'] == {'catalogue': 2, 'measured': 1}
    assert [(v['value'], v['count'], v['count_by_source']) for v in material['values']] == [
        ('Paper', 2, {'catalogue': 1, 'measured': 1}),
        ('Vellum', 2, {'catalogue': 2, 'measured': 0}),
        ('_none', 3, {'catalogue': 4, 'measured': 5})]
    assert '_none' not in {v['value'] for v in _criterion(body, 'attributes')['values']}
    lines = _criterion(body, 'lines')
    assert (lines['kind'], lines['whole_numbers'], 'values' in lines) == ('range', True, False)
    assert _criterion(body, 'width')['unit'] == 'cm'
    assert all(c['available'] for s in body['sections'] for c in s['criteria'])


def test_every_listed_value_is_accepted(client, searcher, svc):
    """What GET /api/criteria lists is exactly what filters.criteria accepts."""
    body = client.get('/api/criteria').json()
    for section in body['sections']:
        for criterion in section['criteria']:
            for value in criterion.get('values', []):
                sources = [s for s, n in value.get('count_by_source', {'either': 1}).items() if n]
                for source in sources:
                    spec = {'values': [value['value']]}
                    if source != 'either':
                        spec['source'] = source
                    r = _search(client, {'criteria': {criterion['key']: spec}})
                    assert r.status_code == 200, (criterion['key'], value['value'], r.text[:300])


def test_a_5_0_0_file_marks_the_new_criteria_unavailable(client, searcher, old_svc):
    body = client.get('/api/criteria').json()
    for key in ('period', 'columns', 'lines', 'vocal_extent', 'attributes'):
        entry = _criterion(body, key)
        assert entry['available'] is False and 'values' not in entry and 'recorded' not in entry
    assert _criterion(body, 'style')['available'] is True


@pytest.mark.parametrize('meta', [_meta(_unloaded), None])
def test_the_vocabulary_without_a_corpus_list_is_503(client, searcher, svc, meta):
    from web.state import state
    state.meta_mgr = meta
    message = _error(client.get('/api/criteria'), 503, 'filter_vocabulary_unavailable')
    assert 'corpus_unavailable' in message


def test_the_vocabulary_without_a_sidecar_is_503(client, searcher, monkeypatch, tmp_path):
    monkeypatch.setattr(fjms_service, '_default_service', FjmsService(db_path=str(tmp_path / 'x.db')))
    _error(client.get('/api/criteria'), 503, 'filter_vocabulary_unavailable')


def test_the_vocabulary_is_built_once_and_handed_out_as_copies(svc, monkeypatch):
    first = svc.criteria_vocabulary(UNIVERSE)
    first['sections'].clear()
    monkeypatch.setattr(svc, '_build_criteria_vocabulary',
                        lambda universe: pytest.fail('built again'))
    again = svc.criteria_vocabulary(UNIVERSE)
    assert len(again['sections']) == len(SECTIONS)


# ── docs/SEARCH_API.md ──────────────────────────────────────────────────

def test_the_documented_example_matches_a_real_response(client, searcher, svc):
    """The gate compares first list elements only (tests/doc_response_shapes.py),
    so each documented criterion is also checked against the real one of the
    same key."""
    from tests.doc_response_shapes import documented_example, key_paths, missing_from
    body = client.get('/api/criteria').json()
    assert missing_from(body, 'criteria', 'reference') == set()
    for section in documented_example('criteria', 'reference')['sections']:
        for documented in section['criteria']:
            assert key_paths(documented) <= key_paths(_criterion(body, documented['key'])), documented['key']
