# -*- coding: utf-8 -*-
"""/search with the catalogue criteria (PR 1b of
docs/plans/CATALOGUE_CRITERIA_PLAN.md), on the real page in a NiceGUI User
simulation with the FJMS singleton pointed at a small sidecar file.

* The "Manuscript features" button replaced the date range, the script lists
  and the pre-search Measurements group: the old marks are gone, the button
  is there, and saved criteria come back as chips with the badge counted.
* Saved dates are read in as the period of copy (D18).
* Saved measurement filters are not restored, silently, and the rows saved
  with them are dropped so the search runs again (R2).
* A 5.0.0 file drops a saved criterion it cannot answer, with a notice
  (rule 9); with no catalog at all nothing is dropped.
* The post-search Measurements group shows when the catalog has
  measurements (#17).
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from unittest.mock import patch

import httpx
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret
from tests.render_smoke._bounded_run import run_bounded

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from criteria_sidecar import build_criteria_sidecar  # noqa: E402
from fjms_filter_sidecar import build_sidecar  # noqa: E402

import web.main  # noqa: E402,F401  (registers the /search route)
import web.components.filter_panel as _filter_panel  # noqa: E402
import web.pages.search_state as _search_state  # noqa: E402
import web.safe_storage as _safe_storage  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

DROPPED_NOTICE = 'Some saved filters were removed: this catalog data is not available.'
OLD_MARKS = ('filter-line-height-row', 'filter-measurements-group', 'filter-script-row')


@asynccontextmanager
async def _user(saved: dict, written: dict):
    real_get = _safe_storage.safe_user_get

    def _get(key, default=None):
        if key in written:
            return written[key]
        if key in saved:
            return saved[key]
        return real_get(key, default)

    def _set(key, value):
        written[key] = value

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('search-criteria-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value='en'))
            stack.enter_context(patch.object(_safe_storage, 'safe_user_get', _get))
            stack.enter_context(patch.object(_safe_storage, 'safe_user_set', _set))
            stack.enter_context(patch.object(_filter_panel, 'persist_value', _set))
            # search_state reads the saved rows through its own names.
            stack.enter_context(patch.object(_search_state, 'safe_user_get', _get))
            stack.enter_context(patch.object(_search_state, 'safe_user_set', _set))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _marked(user, mark):
    with user._client:
        return [e for e in user._client.elements.values()
                if mark in (getattr(e, '_markers', None) or [])]


def _texts(user):
    with user._client:
        return [getattr(e, 'text', '') or '' for e in user._client.elements.values()]


def _open_search(monkeypatch, svc, saved=None, wait_for=None):
    """Open /search on ``svc``; return (page facts, notices, storage writes)."""
    monkeypatch.setattr(fjms_service, '_default_service', svc)
    facts, notices, written = {}, [], {}
    saved_slots = list(nicegui_context.slot_stack)

    async def run():
        async with _user(saved or {}, written) as user:
            await user.open('/search')
            if wait_for:
                deadline = asyncio.get_running_loop().time() + 5.0
                while (not any(wait_for in t for t in _texts(user))
                       and asyncio.get_running_loop().time() < deadline):
                    await asyncio.sleep(0.05)
            for mark in OLD_MARKS + ('search-criteria-button', 'post-filter-measurements-group'):
                found = _marked(user, mark)
                facts[mark] = found[0].visible if found else None
            badge = _marked(user, 'search-criteria-badge')
            facts['badge'] = (badge[0].text, badge[0].visible) if badge else None
            facts['texts'] = _texts(user)
            notices.extend(user.notify.messages)

    try:
        run_bounded(run())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)
    return facts, notices, written


def test_the_button_replaces_the_old_controls(tmp_path, monkeypatch):
    facts, _, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_criteria_sidecar(tmp_path / 's.db')))
    for mark in OLD_MARKS:
        assert facts[mark] is None, (mark, facts)
    assert facts['search-criteria-button'] is True, facts
    assert facts['badge'] == ('0', False), facts


def test_saved_criteria_come_back_as_chips(tmp_path, monkeypatch):
    facts, _, _ = _open_search(
        monkeypatch, FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db')),
        saved={'search_filter_criteria': {'region': {'values': ['Oriental'], 'mode': 'except'},
                                          'lines': {'min': 20, 'max': 25}}},
        wait_for='Region: all except Oriental')
    assert 'Region: all except Oriental' in facts['texts'], facts['texts']
    assert 'Lines: 20–25' in facts['texts'], facts['texts']
    assert facts['badge'] == ('2', True), facts


def test_saved_dates_are_read_in_as_the_period(tmp_path, monkeypatch):
    facts, _, written = _open_search(
        monkeypatch, FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db')),
        saved={'search_filter_date_from': 1000, 'search_filter_date_to': 1200},
        wait_for='Period: 1000')
    assert written['search_filter_criteria'] == {'period': {'min': 1000, 'max': 1200}}, written
    assert written['search_filter_date_from'] is None and written['search_filter_date_to'] is None
    assert 'Period: 1000–1200' in facts['texts'], facts['texts']


def test_saved_measurement_filters_are_retired_with_their_rows(tmp_path, monkeypatch):
    facts, notices, written = _open_search(
        monkeypatch, FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db')),
        saved={'search_filter_width_min': 10.0, 'search_filter_measurement_material': ['Paper'],
               'search_results': [{'sys_id': '990000000010205171'}]})
    assert written.get('search_filter_width_min', 'unset') is None, written
    assert written.get('search_filter_measurement_material', 'unset') is None, written
    assert written.get('search_results') == [], written       # the search runs again
    assert written.get('search_last_run', 'unset') is None, written   # not "a finished search"
    assert DROPPED_NOTICE not in notices, notices              # R2: no note
    assert facts['badge'] == ('0', False), facts


def test_unchanged_saved_rows_are_kept(tmp_path, monkeypatch):
    _, _, written = _open_search(
        monkeypatch, FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db')),
        saved={'search_filter_criteria': {'style': {'values': ['Square']}}})
    assert 'search_results' not in written, written


def test_a_5_0_0_file_drops_a_saved_criterion_it_cannot_answer(tmp_path, monkeypatch):
    facts, notices, written = _open_search(
        monkeypatch,
        FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db', upgrade=False)),
        saved={'search_filter_criteria': {'columns': {'min': 2, 'max': 2},
                                          'style': {'values': ['Square']}}})
    assert DROPPED_NOTICE in notices, notices
    assert written['search_filter_criteria'] == {
        'style': {'values': ['Square'], 'mode': 'only'}}, written


def test_saved_criteria_are_kept_silently_when_there_is_no_catalog(tmp_path, monkeypatch):
    _, notices, written = _open_search(
        monkeypatch, FjmsService(db_path=str(tmp_path / 'absent.db')),
        saved={'search_filter_criteria': {'columns': {'min': 2, 'max': 2}}})
    assert DROPPED_NOTICE not in notices, notices
    assert 'search_filter_criteria' not in written, written


def test_post_search_measurements_show_with_measurement_data(tmp_path, monkeypatch):
    facts, _, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_sidecar(tmp_path / 's.db', with_line_height=False)))
    assert facts['post-filter-measurements-group'] is True, facts


def test_post_search_measurements_hide_without_measurement_data(tmp_path, monkeypatch):
    facts, _, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_sidecar(tmp_path / 's.db', with_measurements=False)))
    assert facts['post-filter-measurements-group'] is False, facts


def test_post_search_measurements_filter_the_shown_rows_as_the_panel(tmp_path, monkeypatch):
    """D20 at the page: the restored rows, a width range in the post-search
    Measurements, Apply Filters -> the rows the panel's rules keep (A by its
    catalogue size, B and D by a measured page; C's size is flagged)."""
    from criteria_sidecar import A, B, C, D, E, F
    from nicegui import ui
    from nicegui.testing.user_interaction import UserInteraction
    svc = FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db'))
    monkeypatch.setattr(fjms_service, '_default_service', svc)
    rows = [{'sys_id': sid, 'display': {'id': sid, 'shelfmark': f'T-S {sid[-6:]}', 'title': 'x'},
             'snippet': 'x'} for sid in (A, B, C, D, E, F)]
    saved = {'search_query': 'שבת', 'search_mode': 'exact', 'search_results': rows}
    seen = {}
    saved_slots = list(nicegui_context.slot_stack)

    async def run():
        written = {}
        async with _user(saved, written) as user:
            await user.open('/search?q=שבת')
            row = _marked(user, 'post-filter-width-row')[0]
            with user._client:
                low, high = [e for e in row.descendants() if isinstance(e, ui.number)]
                low.value, high.value = 10, 12
            for box in (low, high):
                UserInteraction(user, {box}, None).trigger('blur')
            with user._client:
                apply = next(e for e in user._client.elements.values()
                             if isinstance(e, ui.button) and e.text == 'Apply Filters')
            UserInteraction(user, {apply}, None).click()
            deadline = asyncio.get_running_loop().time() + 5.0
            while (not any(' / 6 Results' in t for t in _texts(user))
                   and asyncio.get_running_loop().time() < deadline):
                await asyncio.sleep(0.05)
            seen['texts'] = _texts(user)

    try:
        run_bounded(run())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)
    assert any(t.startswith('3 / 6 Results') for t in seen['texts']), [
        t for t in seen['texts'] if 'Results' in t]
