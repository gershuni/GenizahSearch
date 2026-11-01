# -*- coding: utf-8 -*-
"""Catalog Browse lists manuscripts by script and size (2026-11-01).

The owner asked to "show all manuscripts that have a certain trait (like sort of
script or size)": "Search only in..." could only narrow a text search. Catalog
Browse -- the page that lists manuscripts with no text -- now takes the script
style / script place lists and the width / height / lines / material bounds.

Pinned here: the browse query lists exactly what "Search only in..." would
restrict a search to (one definition of each trait); the count, the library
facets and the date filter combine with the traits; the /catalog-browse address
carries them; and a browse list hands them on to /search (all of them) and to
/parallels (the script ones only: it has no size controls).
"""
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_browse_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService, clean_traits  # noqa: E402
from web.pages.catalog_browse import traits_from_query, traits_to_query  # noqa: E402
import web.components.filter_panel as fp  # noqa: E402


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


def test_the_address_carries_the_traits_both_ways():
    traits = {'script_styles': ['Square', 'Semi-Cursive'], 'script_places': ['Spanish'],
              'width_min': 10.0, 'width_max': 20.5, 'line_count_min': 15,
              'measurement_material': ['Vellum']}
    params = dict(p.split('=', 1) for p in traits_to_query(traits))
    from urllib.parse import unquote
    back = traits_from_query(script_style=unquote(params['script_style']),
                             script_place=params['script_place'], width=params['width'],
                             lines=params['lines'], material=params['material'])
    assert back == traits
    assert params['lines'] == '15-' and params['width'] == '10-20.5'
    # A malformed value is ignored, never an error.
    assert traits_from_query(width='10-20-30', height='tall', lines='') == {}


def _consume(monkeypatch, incoming, prefix):
    import web.safe_storage as ss
    store = {'incoming_filters': incoming}
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(ss, 'safe_user_pop', lambda key, default=None: store.pop(key, default))
    written = {}
    monkeypatch.setattr(fp, 'persist_value', lambda key, value: written.__setitem__(key, value))
    st = SimpleNamespace(filter_script_styles=[], filter_script_places=[], filter_width_min=None,
                         filter_measurement_material=[])
    assert fp.consume_incoming_filters(st, prefix)
    return st, written


HANDOFF = {'script_places': ['Spanish'], 'width_min': 14.0, 'measurement_material': ['Paper']}


def test_a_browse_list_hands_every_trait_to_search(monkeypatch):
    st, written = _consume(monkeypatch, dict(HANDOFF), 'search')
    assert st.filter_script_places == ['Spanish'] and st.filter_width_min == 14.0
    assert st.filter_measurement_material == ['Paper']
    assert written['search_filter_width_min'] == 14.0


def test_parallels_takes_only_the_script_traits(monkeypatch):
    """/parallels has no size controls or chips: applied there they would be invisible."""
    st, written = _consume(monkeypatch, dict(HANDOFF), 'parallels')
    assert st.filter_script_places == ['Spanish']
    assert st.filter_width_min is None and st.filter_measurement_material == []
    assert not any('width' in k or 'material' in k for k in written), written
