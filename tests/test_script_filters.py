# -*- coding: utf-8 -*-
"""Script Style / Script Place filters in "Search only in..." (2026-11-01).

A user asked whether the site finds manuscripts by their script, as the
Friedberg site did. The catalogue records it per catalogue record
(catalog_fields TypeOfScriptStyle / TypeOfScriptPlace). These tests pin the
lookup and the choices offered. Every surface now uses the catalogue criteria
(web: tests/test_web_criteria_state.py; API: tests/test_criteria_api.py), and
the API refuses the two lists, which were never released.
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


# -- the API: the two lists were never released; criteria replaced them (PR 2) --

@pytest.mark.parametrize('key', ['script_styles', 'script_places'])
def test_the_api_and_mcp_refuse_the_unreleased_lists(key):
    from pydantic import ValidationError
    from web import search_api
    from web.mcp_server import SearchFilters
    for model in (search_api.FiltersModel, SearchFilters):
        with pytest.raises(ValidationError):
            model(**{key: ['Square']})
