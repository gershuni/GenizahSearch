# -*- coding: utf-8 -*-
"""Script Style / Script Place filters in "Search only in..." (2026-11-01).

A user asked whether the site finds manuscripts by their script, as the
Friedberg site did. The catalogue records it per catalogue record
(catalog_fields TypeOfScriptStyle / TypeOfScriptPlace). These tests pin the
lookup, the choices offered, the API's checks, and every web call site that
carries the two lists to the lookup -- a list dropped on the way would run the
search over every manuscript.
"""
import ast
import asyncio
import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import (  # noqa: E402
    LONG_EXCLUDE, LONG_EXCLUDE_50, brackets_balanced, build_script_sidecar,
)

from shared import fjms_service  # noqa: E402
from shared.api_errors import APIError  # noqa: E402
from shared.fjms_service import (  # noqa: E402
    FilterUnavailable, FjmsService, script_filter_options, script_value_label,
)
import web.components.filter_panel as fp  # noqa: E402

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


# -- the web panel --

def _state(**over):
    s = SimpleNamespace(
        filter_include_mode=True, filter_domains=[], filter_authors=[], filter_works=[],
        filter_date_from=None, filter_date_to=None, filter_material_exclude=[],
        filter_script_styles=[], filter_script_places=[],
        filter_text_all=[], filter_text_any=[], filter_text_not=[],
        filter_measurement_material=[],
        filter_manuscript_count='untouched', restrict_sys_ids='untouched',
    )
    for k, v in over.items():
        setattr(s, k, v)
    return s


def _recompute(state, monkeypatch):
    async def _to_thread(fn, *a, **kw):
        return await asyncio.to_thread(fn, *a, **kw)
    monkeypatch.setattr(fp.run, 'io_bound', _to_thread)
    statuses = []
    asyncio.run(fp.recompute_filter_count(state, lambda: None, on_state=statuses.append))
    return statuses


def test_the_live_count_applies_both_lists(svc, monkeypatch):
    st = _state(filter_script_styles=['Square'], filter_script_places=['Oriental'])
    assert fp.has_active_filters(st)
    assert _recompute(st, monkeypatch)[-1] == 'done'
    assert st.restrict_sys_ids == {'990003'} and st.filter_manuscript_count == 1


def test_include_exclude_mode_does_not_turn_the_script_filter_around(svc, monkeypatch):
    st = _state(filter_include_mode=False, filter_script_places=['Spanish'])
    _recompute(st, monkeypatch)
    assert st.restrict_sys_ids == {'990001'}


def test_saved_choices_load_per_page(monkeypatch):
    import web.safe_storage as ss
    saved = {'parallels_filter_script_places': ['Yemenite'], 'search_filter_script_styles': ['Square']}
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: saved.get(key, default))
    monkeypatch.setattr(fp, 'persist_value', lambda key, value: None)
    st = SimpleNamespace()
    fp.load_filter_state(st, 'parallels')
    assert st.filter_script_places == ['Yemenite'] and st.filter_script_styles == []


def test_the_change_handler_saves_and_recounts(monkeypatch):
    written, calls = {}, []
    monkeypatch.setattr(fp, 'persist_value', lambda key, value: written.__setitem__(key, value))

    async def recompute():
        calls.append('recompute')
    refs = {'script_places': SimpleNamespace(value=['Spanish', 'Italian'])}
    st = _state()
    handlers = fp.create_filter_handlers(st, 'search', refs, None, None, recompute,
                                         lambda: calls.append('chips'))
    asyncio.run(handlers['on_script_places_change']())
    assert st.filter_script_places == ['Spanish', 'Italian']
    assert written == {'search_filter_script_places': ['Spanish', 'Italian']}
    assert calls == ['recompute', 'chips']


def test_history_summary_names_the_script(svc):
    svc.get_script_values('script_places')  # the panel has loaded its choices
    summary = fp.build_filter_summary({'script_places': ['Spanish']}, lambda s: s, lambda: 'he', 80)
    assert summary == '[include: ספרדי]'


@pytest.mark.parametrize('include_mode,expected', [
    (True, '[include: Bible, Spanish]'),
    # Exclude mode turns the domain around, never the script (Codex round 1).
    (False, '[exclude: Bible] [include: Spanish]'),
])
def test_history_summary_never_puts_a_script_under_exclude(svc, include_mode, expected):
    filters = {'include_mode': include_mode, 'domains': ['Bible'], 'script_places': ['Spanish']}
    assert fp.build_filter_summary(filters, lambda s: s, lambda: 'en', 80) == expected


def test_a_truncated_summary_keeps_its_brackets_balanced(svc):
    """Codex round 2: the cut fell between the two brackets and left "] ...]"."""
    summary = fp.build_filter_summary(LONG_EXCLUDE, lambda s: s, lambda: 'en', 50)
    assert summary == LONG_EXCLUDE_50 and len(summary) <= 50
    for max_len in range(20, 70):  # every cut point, inside either bracket or between them
        s = fp.build_filter_summary(LONG_EXCLUDE, lambda s: s, lambda: 'en', max_len)
        assert len(s) <= max_len and brackets_balanced(s), (max_len, s)


def _filter_kwargs_values(path: Path) -> list:
    """{keyword: the state attributes its value reads} for every
    ``filter_kwargs = dict(...)`` in a page."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [
        {kw.arg: {n.attr for n in ast.walk(kw.value) if isinstance(n, ast.Attribute)}
         for kw in node.value.keywords}
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
        and getattr(node.value.func, 'id', None) == 'dict'
        and any(getattr(t, 'id', None) == 'filter_kwargs' for t in node.targets)
    ]


@pytest.mark.parametrize('page', ['web/pages/search.py', 'web/pages/parallels.py'])
def test_the_search_itself_is_restricted_by_both_lists(page):
    """The count is one call site; the search's own lookup is another. Each
    keyword must READ its own state list -- ``script_places=None`` would not."""
    found = _filter_kwargs_values(ROOT / page)
    assert found, f"{page}: no filter_kwargs = dict(...) found; update this guard"
    for kws in found:
        for key in ('script_styles', 'script_places'):
            assert f'filter_{key}' in kws.get(key, set()), (page, key, kws.get(key))


def test_run_filters_and_clear_all_carry_the_lists():
    storage = {}
    with patch('web.pages.search_state.app') as mock_app, patch('web.safe_storage.app') as mock_safe:
        mock_app.storage.user = storage
        mock_safe.storage.user = storage
        from web.pages.search_state import (
            SearchUIState, apply_run_filter_state, clear_search_filters, run_filter_state,
        )
        st = SearchUIState()
        st.filter_script_styles = ['Square']
        saved = run_filter_state(st)
        assert saved['filter_script_styles'] == ['Square']
        assert saved['filter_script_styles'] is not st.filter_script_styles, "a run never shares a list"
        fresh = SearchUIState()
        apply_run_filter_state(fresh, saved)
        assert fresh.filter_script_styles == ['Square'] and fresh.filter_script_places == []
        storage['search_filter_script_places'] = ['Spanish']
        clear_search_filters()
        assert storage['search_filter_script_places'] == [] and storage['search_filter_script_styles'] == []
