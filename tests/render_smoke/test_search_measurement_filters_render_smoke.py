# -*- coding: utf-8 -*-
"""/search hides the measurement filters the catalog data cannot answer (#17).

Opens the real /search page in a NiceGUI User simulation with the FJMS
singleton pointed at a small sidecar file, and checks the marked rows the
page builds at open:

* 'filter-line-height-row' / 'post-filter-line-height-row' -- the Line Height
  rows of the pre-search and post-search Measurements panels;
* 'filter-measurements-group' / 'post-filter-measurements-group' -- the two
  Measurements expansions.

Each case is asserted both ways (data present -> visible, data missing ->
hidden) so a page that hides the row unconditionally, or never, fails.
Absence is asserted on the page's own marks, never on body text.

Also: a saved Line Height filter the catalog is known to lack is dropped at
page open WITH a notice (owner ruling), and kept when there is no catalog.
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fjms_filter_sidecar import build_sidecar  # noqa: E402

import web.main  # noqa: E402,F401  (registers the /search route)
import web.safe_storage as _safe_storage  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

DROPPED_NOTICE = 'Some saved filters were removed: this catalog data is not available.'
MARKS = ('filter-line-height-row', 'post-filter-line-height-row',
         'filter-measurements-group', 'post-filter-measurements-group')


@asynccontextmanager
async def _user(saved: dict):
    real_get = _safe_storage.safe_user_get

    def _get(key, default=None):
        if key in saved:
            return saved[key]
        return real_get(key, default)

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('measurement-filters-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value='en'))
            stack.enter_context(patch.object(_safe_storage, 'safe_user_get', _get))
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


def _open_search(monkeypatch, svc, saved=None):
    """Open /search on ``svc``; return (visibility by mark, notices seen)."""
    monkeypatch.setattr(fjms_service, '_default_service', svc)
    out = {}
    notices = []
    saved_slots = list(nicegui_context.slot_stack)

    async def run():
        async with _user(saved or {}) as user:
            await user.open('/search')
            for mark in MARKS:
                found = _marked(user, mark)
                assert len(found) == 1, f"{mark}: expected one marked element, found {len(found)}"
                out[mark] = found[0].visible
            notices.extend(user.notify.messages)

    try:
        asyncio.run(run())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)
    return out, notices


def test_line_height_rows_show_when_the_data_has_line_heights(tmp_path, monkeypatch):
    vis, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_sidecar(tmp_path / 's.db', with_line_height=True)))
    assert vis == {'filter-line-height-row': True, 'post-filter-line-height-row': True,
                   'filter-measurements-group': True, 'post-filter-measurements-group': True}, vis


def test_line_height_rows_hide_when_the_data_lacks_them(tmp_path, monkeypatch):
    vis, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_sidecar(tmp_path / 's.db', with_line_height=False)))
    assert vis == {'filter-line-height-row': False, 'post-filter-line-height-row': False,
                   'filter-measurements-group': True, 'post-filter-measurements-group': True}, vis


def test_measurements_hide_when_the_data_has_no_measurements(tmp_path, monkeypatch):
    vis, _ = _open_search(monkeypatch, FjmsService(
        db_path=build_sidecar(tmp_path / 's.db', with_measurements=False)))
    assert vis['filter-measurements-group'] is False, vis
    assert vis['post-filter-measurements-group'] is False, vis


def test_a_saved_line_height_the_catalog_lacks_is_dropped_with_a_notice(tmp_path, monkeypatch):
    _, notices = _open_search(
        monkeypatch,
        FjmsService(db_path=build_sidecar(tmp_path / 's.db', with_line_height=False)),
        saved={'search_filter_line_height_min': 3.0})
    assert DROPPED_NOTICE in notices, notices


def test_saved_filters_are_kept_silently_when_there_is_no_catalog(tmp_path, monkeypatch):
    _, notices = _open_search(
        monkeypatch, FjmsService(db_path=str(tmp_path / 'absent.db')),
        saved={'search_filter_line_height_min': 3.0})
    assert DROPPED_NOTICE not in notices, notices
