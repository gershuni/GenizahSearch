# -*- coding: utf-8 -*-
"""Catalog Browse lists manuscripts by script and size (2026-11-01).

The owner asked to "show all manuscripts that have a certain trait (like sort of
script or size)": "Search only in..." could only narrow a text search. Catalog
Browse -- the page that lists manuscripts with no text -- now takes the script
style / script place lists and the width / height / lines / material bounds.

Pinned here: the browse query lists exactly what "Search only in..." would
restrict a search to (one definition of each trait); the count, the library
facets and the date filter combine with the traits. The page itself now uses the
catalogue criteria (PR 1b of docs/plans/CATALOGUE_CRITERIA_PLAN.md); the service's
``traits`` stay for the API until PR 2.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_browse_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService, clean_traits  # noqa: E402


@pytest.fixture
def svc(tmp_path, monkeypatch):
    service = FjmsService(db_path=build_browse_sidecar(tmp_path / "browse.db"))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    yield service
    service.close()


TRAIT_CASES = [
    ({'script_places': ['Oriental']}, {'990002', '990003'}),
    # 990003's width is known only from its images (COALESCE onto computed).
    ({'width_min': 14}, {'990002', '990003', '990005'}),
    ({'width_min': 14, 'width_max': 26, 'measurement_material': ['Paper']}, {'990002', '990003'}),
    ({'script_styles': ['Square'], 'line_count_min': 20}, {'990003'}),
    ({'height_max': 21}, {'990001'}),
    ({'width_min': 26, 'width_max': 14}, {'990002', '990003'}),  # reversed bounds are swapped
    ({'measurement_material': ['Vellum', 'Papyrus']}, {'990001'}),
]


@pytest.mark.parametrize('traits,expected', TRAIT_CASES)
def test_browse_lists_exactly_what_search_only_in_restricts_to(svc, traits, expected):
    data = svc.get_browse_results(limit=50, traits=traits)
    assert data['total'] == len(expected)
    assert {r['sys_id'] for r in data['results']} == expected
    assert svc.get_filter_sys_ids(**traits) == expected, "browse and search must agree"


def test_traits_combine_with_the_date_and_paginate(svc):
    traits = {'script_places': ['Spanish', 'Oriental']}
    data = svc.get_browse_results(date_from=1100, date_to=1200, traits=traits)
    assert {r['sys_id'] for r in data['results']} == {'990001', '990003'}  # 990002 is 1300
    page2 = svc.get_browse_results(offset=1, limit=1, traits=traits)
    assert page2['total'] == 3 and len(page2['results']) == 1


def test_library_facets_count_only_manuscripts_with_the_trait(svc):
    library = {'990001': 'CUL', '990002': 'JTS', '990003': 'CUL', '990005': 'CUL'}
    facets = svc.get_browse_library_facets(sys_id_to_library=library.get, traits={'width_min': 14})
    assert facets == {'JTS': 1, 'CUL': 2}


def test_clean_traits_keeps_only_real_values():
    assert clean_traits({
        'script_places': ['Spanish', ''], 'script_styles': [], 'width_min': '12.5',
        'line_count_max': '18.7', 'height_max': 'tall', 'domain': 'Bible', 'width_max': None,
        'measurement_material': 'Paper',
    }) == {'script_places': ['Spanish'], 'width_min': 12.5, 'line_count_max': 18,
           'measurement_material': ['Paper']}
