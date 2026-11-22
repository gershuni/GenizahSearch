# -*- coding: utf-8 -*-
"""Exclude manuscripts from the shelf on /search (shelf tree picker PR 4, plan §10.13, §6).

The call site, on the real page in a NiceGUI User simulation with the fixture tree served: the
exclusion dialog offers "Pick from the shelf"; a collection ticked whole and confirmed becomes one
exclusion source of kind ``shelf`` holding exactly the collection's manuscripts, kept through the
same ``persist_value('search_exclusion_sources', ...)`` as the Paste, List and File tabs, and shown
as a chip. With the switch on and the file not served the button is absent.
"""
from __future__ import annotations

import asyncio
import time

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

import shared.shelfmark_tree_service as tree_module
import web.pages.search as search_page
from shared.exclusion_service import deserialize_sources
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import build_fixture


async def _until(predicate, timeout=6.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.03)
    return predicate()


def _all(user):
    with user.client:
        return [e for e in user.client.elements.values() if not e.is_deleted]


def _button(user, text):
    return [e for e in _all(user) if isinstance(e, ui.button) and (e.text or '') == text]


def _prop(user, name, value=None):
    return [e for e in _all(user) if name in e.props and (value is None or e.props[name] == value)]


def _open_exclusion_dialog(monkeypatch, svc, written, seen, after=None):
    monkeypatch.setattr(tree_module, '_default_service', svc)
    real_persist = search_page.persist_value

    def _persist(key, value):
        written[key] = value
        return real_persist(key, value)

    monkeypatch.setattr(search_page, 'persist_value', _persist)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/search')
            await _connect(user)
            assert await _until(lambda: _button(user, 'Exclude known manuscripts')), 'no exclusion button'
            UserInteraction(user, {_button(user, 'Exclude known manuscripts')[0]}, None).click()
            assert await _until(lambda: _button(user, 'Paste Shelfmarks') or any(
                isinstance(e, ui.tab) for e in _all(user)))
            seen['shelf_button'] = bool(_prop(user, 'data-testid', 'exclude-from-shelf'))
            if after is not None:
                await after(user)

    _run(go)


def test_a_collection_chosen_on_the_shelf_is_excluded_through_the_one_add_path(tmp_path, monkeypatch):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    wanted = set(svc.members_under('col:12'))
    written, seen = {}, {}

    async def after(user):
        UserInteraction(user, {_prop(user, 'data-testid', 'exclude-from-shelf')[0]}, None).click()
        assert await _until(lambda: _prop(user, 'data-node', 'lib:1')), 'the picker did not open'
        UserInteraction(user, {_prop(user, 'data-node', 'lib:1')[0]}, None).click()
        assert await _until(lambda: _prop(user, 'data-node-box', 'col:12')), 'no box on the collection'
        UserInteraction(user, {_prop(user, 'data-node-box', 'col:12')[0]}, None).trigger('click.stop')
        assert await _until(lambda: any('stp-confirm' in e.classes and e.enabled for e in _all(user)))
        confirm = next(e for e in _all(user) if 'stp-confirm' in e.classes)
        seen['confirm_text'] = confirm.text
        UserInteraction(user, {confirm}, None).click()
        assert await _until(lambda: 'search_exclusion_sources' in written)
        await asyncio.sleep(0.2)
        seen['chips'] = [e.text for e in _all(user) if isinstance(e, ui.chip)]

    try:
        _open_exclusion_dialog(monkeypatch, svc, written, seen, after)
    finally:
        svc.close()
    assert seen['shelf_button'] is True
    assert seen['confirm_text'] == 'Exclude them'
    sources = deserialize_sources(written['search_exclusion_sources'])
    assert len(sources) == 1 and sources[0].source_type == 'shelf'
    assert sources[0].source_id.startswith('shelf:')
    assert sources[0].sys_ids == wanted
    assert any(c.startswith('From the shelf: CUL Or.') and c.endswith(f'({len(wanted)})') for c in seen['chips']), seen['chips']


def test_with_the_file_not_served_there_is_no_shelf_button(tmp_path, monkeypatch):
    missing = ShelfmarkTreeService(str(tmp_path / 'missing.db'))
    assert not missing.is_ready()
    written, seen = {}, {}
    _open_exclusion_dialog(monkeypatch, missing, written, seen)
    assert seen['shelf_button'] is False


def test_with_the_switch_off_there_is_no_shelf_button(tmp_path, monkeypatch):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    monkeypatch.setenv('SHELFMARK_TREE_ENABLED', '0')
    written, seen = {}, {}
    try:
        _open_exclusion_dialog(monkeypatch, svc, written, seen)
    finally:
        svc.close()
    assert seen['shelf_button'] is False
