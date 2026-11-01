# -*- coding: utf-8 -*-
"""Script Style / Script Place filters in "Search only in..." (2026-11-01).

A user asked whether the site finds manuscripts by their script, as the
Friedberg site did. The catalogue records it per catalogue record
(catalog_fields TypeOfScriptStyle / TypeOfScriptPlace). These tests pin the
lookup, the choices offered and the API's checks (the API keeps the two lists
until PR 2 of docs/plans/CATALOGUE_CRITERIA_PLAN.md). The web pages now use the
catalogue criteria: tests/test_web_criteria_state.py.
"""
import os
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import (  # noqa: E402
    build_script_sidecar,
)

from shared import fjms_service  # noqa: E402
from shared.api_errors import APIError  # noqa: E402
from shared.fjms_service import (  # noqa: E402
    FilterUnavailable, FjmsService, script_filter_options, script_value_label,
)

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def svc(tmp_path, monkeypatch):
    service = FjmsService(db_path=build_script_sidecar(tmp_path / "script.db"))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    yield service
    service.close()


@pytest.fixture
def absent(tmp_path, monkeypatch):
    service = FjmsService(db_path=str(tmp_path / "absent.db"))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    return service


# -- the choices --

def test_choices_are_most_manuscripts_first_each_counted_once(svc):
    assert svc.get_script_values('script_styles') == [
        {'value': 'Square', 'value_heb': 'מרובע', 'count': 2},
        {'value': 'Semi-Cursive', 'value_heb': 'בינוני', 'count': 1},
    ]
    assert [(v['value'], v['count']) for v in svc.get_script_values('script_places')] == [
        ('Oriental', 2), ('Spanish', 1), ('Yemenite', 1)], "'Unknown' describes no script"


def test_choice_labels_follow_the_language(svc):
    assert script_filter_options('script_places', True) == {
        'Oriental': 'מזרחי (2)', 'Spanish': 'ספרדי (1)', 'Yemenite': 'תימני (1)'}
    assert script_filter_options('script_styles', False) == {
        'Square': 'Square (2)', 'Semi-Cursive': 'Semi-Cursive (1)'}
    assert script_value_label('script_places', 'Spanish', True) == 'ספרדי'
    assert script_value_label('script_places', 'Spanish', False) == 'Spanish'
    assert script_value_label('script_places', 'Nowhere', True) == 'Nowhere'


def test_no_catalogue_offers_no_choices_and_does_not_cache_that(absent):
    assert absent.get_script_values('script_styles') == []
    assert absent._script_values_cache == {}


class _FailOnce:
    """Wraps a real connection; the first read of catalog_fields raises."""

    def __init__(self, real):
        self._real, self.failed = real, False

    def execute(self, sql, params=()):
        if 'FROM catalog_fields' in sql and not self.failed:
            self.failed = True
            raise sqlite3.OperationalError("database is locked")
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_a_failed_read_is_not_cached_and_the_next_call_recovers(svc):
    svc._conn = _FailOnce(svc._conn)
    assert svc.get_script_values('script_places') == []
    assert svc._conn.failed and svc._script_values_cache == {}
    assert [v['value'] for v in svc.get_script_values('script_places')] == ['Oriental', 'Spanish', 'Yemenite']


def test_an_unknown_filter_kind_is_a_programming_error(svc):
    with pytest.raises(ValueError):
        svc.get_script_values('script_colour')


# -- the lookup --

@pytest.mark.parametrize('kwargs,expected', [
    ({'script_styles': ['Square']}, {'990001', '990003'}),
    ({'script_styles': ['Square', 'Semi-Cursive']}, {'990001', '990002', '990003'}),
    ({'script_places': ['Yemenite']}, {'990003'}),
    ({'script_places': ['Oriental']}, {'990002', '990003'}),
    ({'script_styles': ['Square'], 'script_places': ['Oriental']}, {'990003'}),
    ({'script_places': ['Spanish', 'Oriental'], 'date_from': 1100, 'date_to': 1200},
     {'990001', '990003'}),
    ({'script_styles': ['Rabbinical']}, set()),
])
def test_lookup(svc, kwargs, expected):
    assert svc.get_filter_sys_ids(**kwargs) == expected


def test_a_script_filter_alone_restricts_the_search(absent):
    """None would mean "no restriction": a script-only filter the lookup cannot
    answer must raise, not run the search over every manuscript."""
    with pytest.raises(FilterUnavailable) as exc:
        absent.get_filter_sys_ids(script_places=['Spanish'])
    assert exc.value.reason == 'sidecar_unavailable'


# -- the API's checks --

def test_known_values_pass_validation(svc):
    svc.validate_filter_values({'script_styles': ['Square'], 'script_places': ['Oriental', 'Yemenite']})


@pytest.mark.parametrize('key,value', [('script_styles', 'square'), ('script_places', 'Sephardic'),
                                       ('script_places', 'Unknown')])
def test_unknown_values_are_400(svc, key, value):
    with pytest.raises(APIError) as exc:
        svc.validate_filter_values({key: [value]})
    assert (exc.value.code, exc.value.http_status) == ('unresolvable_filter_value', 400)


def test_no_vocabulary_is_503_never_allow_all(absent):
    with pytest.raises(APIError) as exc:
        absent.validate_filter_values({'script_places': ['Spanish']})
    assert (exc.value.code, exc.value.http_status) == ('filter_vocabulary_unavailable', 503)


def test_api_and_mcp_accept_the_fields_and_pass_them_to_the_lookup(monkeypatch):
    from web import search_api
    from web.mcp_server import SearchFilters
    body = {'script_styles': ['Square'], 'script_places': ['Spanish']}
    assert SearchFilters(**body).model_dump(exclude_none=True) == body
    filters = search_api.FiltersModel(**body).model_dump(exclude_none=True)
    seen = {}
    monkeypatch.setattr(fjms_service, 'validate_filter_values', lambda f: None)
    monkeypatch.setattr(fjms_service, 'get_filter_sys_ids', lambda **kw: seen.update(kw) or {'990001'})
    assert search_api._resolve_fjms_filters_sync(filters) == {'990001'}
    assert seen['script_styles'] == ['Square'] and seen['script_places'] == ['Spanish']
