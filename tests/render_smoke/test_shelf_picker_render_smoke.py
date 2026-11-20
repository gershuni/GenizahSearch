# -*- coding: utf-8 -*-
"""The web "Pick from the shelf" dialog renders and picks (shelf tree picker plan §3.3, PR 2).

On a test page in a NiceGUI User simulation, with the fixture tree of
tests/shelfmark_tree_fixture.py: opened at a manuscript, the dialog shows the libraries, the
manuscript's collection and volume columns and its volume's shelfmarks with the manuscript
marked; a click on a library opens its collections; a click on a shelfmark hands the host
``{sys_id, shelfmark, placement_id}`` and closes the dialog. The harness is the one
tests/render_smoke/test_search_url_open_dispatch.py built.
"""
from __future__ import annotations

import asyncio
import time

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

PICKED: list = []
SERVICE: dict = {}


@ui.page('/_test/shelf-picker')
def _picker_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None)


def _rows(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props]


async def _until(predicate, timeout=5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def test_the_picker_opens_at_a_manuscript_navigates_and_picks(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    PICKED.clear()
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-leaf')), 'no shelfmark rendered'
            nodes = {e.props['data-node']: e for e in _rows(user, 'data-node')}
            seen['nodes'] = set(nodes)
            leaves = _rows(user, 'data-leaf')
            seen['marked'] = [e.props['data-sys-id'] for e in leaves if 'stp-on' in e.classes]
            seen['on_nodes'] = sorted(n for n, e in nodes.items() if 'stp-on' in e.classes)

            # a library: its collections replace the columns to the right
            UserInteraction(user, {nodes['lib:2']}, None).click()
            assert await _until(lambda: 'col:20' in {e.props['data-node'] for e in _rows(user, 'data-node')}
                                or any(e.props.get('data-sys-id') == SYS['stras'][0] for e in _rows(user, 'data-leaf')))
            seen['after_library'] = {e.props['data-node'] for e in _rows(user, 'data-node')}
            seen['stras_leaves'] = sorted(e.props['data-sys-id'] for e in _rows(user, 'data-leaf'))

            # a shelfmark: picked, and the dialog closes
            target = next(e for e in _rows(user, 'data-leaf') if e.props['data-sys-id'] == SYS['stras'][2])
            UserInteraction(user, {target}, None).click()
            assert await _until(lambda: PICKED)
            with user.client:
                seen['open_dialogs'] = [d for d in user.client.elements.values()
                                        if isinstance(d, ui.dialog) and d.value]

    try:
        _run(go)
    finally:
        svc.close()
    assert {'lib:1', 'lib:2', 'lib:3', 'col:10', 'col:11', 'col:12', 'vol:100', 'vol:101'} <= seen['nodes']
    assert seen['on_nodes'] == ['col:10', 'lib:1', 'vol:100']
    assert seen['marked'] == [SYS['as2'][6]]
    assert 'vol:100' not in seen['after_library']                    # CUL's columns are gone
    assert SYS['stras'][0] in seen['stras_leaves']                    # one collection: passed through
    assert len(PICKED) == 1 and PICKED[0]['sys_id'] == SYS['stras'][2] and PICKED[0]['shelfmark'] == 'Ms. 3'
    assert PICKED[0]['placement_id'].startswith('inv:')
    assert seen['open_dialogs'] == []


def test_a_phone_keeps_the_volumes_of_a_collection_that_also_holds_shelfmarks(tmp_path):
    # PR review: T-S AS holds volumes and shelfmarks outside any volume; on a phone (one level at a
    # time) both the volume column and the shelfmarks must be shown, not only the last panel.
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker?start=col:10')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-leaf')), 'no shelfmark rendered'
            with user.client:
                panels = [e for e in user.client.elements.values() if 'stp-panel' in e.classes]
                seen['phone'] = []
                for panel in panels:
                    if 'stp-phone' not in panel.classes:
                        continue
                    inner = [c for c in panel.descendants() if 'data-node' in c.props or 'data-leaf' in c.props]
                    seen['phone'].append(sorted({('node' if 'data-node' in c.props else 'leaf') for c in inner}))
                seen['titles'] = [e.text for e in user.client.elements.values()
                                  if isinstance(e, ui.label) and 'stp-title' in e.classes]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['phone'] == [['node'], ['leaf']], seen['phone']     # the volumes, then the shelfmarks
    # PR review: labels made in the worker thread are in the visitor's language (English here)
    assert 'Library' in seen['titles'], seen['titles']


def test_a_known_shelfmark_is_found_from_a_column_above_its_groups(tmp_path, monkeypatch):
    # Owner, 2026-11-19: "the user who knows the shelfmark will find it easily". With T-S AS divided
    # into groups, its column's filter finds the volume and the shelfmarks below them.
    from scripts import build_shelfmark_tree as builder
    monkeypatch.setattr(builder, "PREFIX_GROUP_MIN", 1)
    monkeypatch.setattr(builder, "FGP_RANGES", {10: (("T-S AS 1-5", 1, 5),)})
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker?start=col:10')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-node'))
            with user.client:
                boxes = [e for e in user.client.elements.values() if isinstance(e, ui.input)]
                box = next(b for b in boxes if b.parent_slot.parent.parent_slot.parent.default_slot.children
                           and any('grp:col:10' in (c.props.get('data-node') or '')
                                   for c in b.parent_slot.parent.descendants()))
                box.value = '2.4'
            assert await _until(lambda: any(e.props.get('data-sys-id') == SYS['as2'][3]
                                            for e in _rows(user, 'data-leaf')))
            seen['found'] = sorted(e.props['data-sys-id'] for e in _rows(user, 'data-leaf'))

    try:
        _run(go)
    finally:
        svc.close()
    assert {SYS['as2'][3], SYS['shared_row']} <= set(seen['found']), seen['found']


def test_a_slow_older_filter_does_not_replace_a_newer_one(tmp_path, monkeypatch):
    # PR review: draws overlap; the broad filter finishing last must not win over the narrow one.
    from web.components import shelfmark_tree_picker as picker
    real = picker._load_view

    def slow_for_broad(service, path, filters, *rest):
        if 'T-S' in filters.values():
            time.sleep(0.6)
        return real(service, path, filters, *rest)

    monkeypatch.setattr(picker, '_load_view', slow_for_broad)
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker?start=col:10')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-node'))
            with user.client:
                boxes = [e for e in user.client.elements.values() if isinstance(e, ui.input)]
                box = next(b for b in boxes if any('vol:100' == c.props.get('data-node')
                                                   for c in b.parent_slot.parent.descendants()))
                box.value = 'T-S'
                await asyncio.sleep(0.05)
                box.value = '2.4'
            assert await _until(lambda: any(e.props.get('data-sys-id') == SYS['as2'][3]
                                            for e in _rows(user, 'data-leaf')))
            await asyncio.sleep(1.0)                         # the broad load finishes after
            seen['after'] = {e.props.get('data-node') for e in _rows(user, 'data-node')}
            seen['leaf'] = any(e.props.get('data-sys-id') == SYS['as2'][3] for e in _rows(user, 'data-leaf'))

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['leaf'], 'the newer filter was replaced'
    assert 'vol:101' not in seen['after'], seen['after']


def test_the_last_click_wins_over_a_slower_earlier_one(tmp_path, monkeypatch):
    # PR review: two rows clicked before the first lookup ends; the first must not win.
    from web.components import shelfmark_tree_picker as picker
    real = picker._passed_through

    def slow_for_cul(service, node_id):
        if node_id == 'lib:1':
            time.sleep(0.6)
        return real(service, node_id)

    monkeypatch.setattr(picker, '_passed_through', slow_for_cul)
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-node'))
            rows = {e.props['data-node']: e for e in _rows(user, 'data-node')}
            UserInteraction(user, {rows['lib:1']}, None).click()
            await asyncio.sleep(0.05)
            UserInteraction(user, {rows['lib:2']}, None).click()
            assert await _until(lambda: any(e.props.get('data-sys-id') == SYS['stras'][0]
                                            for e in _rows(user, 'data-leaf')))
            await asyncio.sleep(1.0)                         # the slow first lookup ends after
            seen['on'] = sorted(e.props['data-node'] for e in _rows(user, 'data-node') if 'stp-on' in e.classes)

    try:
        _run(go)
    finally:
        svc.close()
    assert 'lib:2' in seen['on'] and 'lib:1' not in seen['on'], seen['on']


def test_the_keyboard_opens_a_level_and_picks(tmp_path):
    # PR review: rows are divs with role=button; Enter and Space must act like a click.
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    PICKED.clear()
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-node'))
            lib = next(e for e in _rows(user, 'data-node') if e.props['data-node'] == 'lib:2')
            UserInteraction(user, {lib}, None).trigger('keydown.enter')
            assert await _until(lambda: any(e.props.get('data-sys-id') == SYS['stras'][1] for e in _rows(user, 'data-leaf')))
            leaf = next(e for e in _rows(user, 'data-leaf') if e.props['data-sys-id'] == SYS['stras'][1])
            UserInteraction(user, {leaf}, None).trigger('keydown.space.prevent')
            seen['picked'] = await _until(lambda: PICKED)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['picked'] and PICKED[0]['sys_id'] == SYS['stras'][1]


def test_the_keyboard_reaches_the_breadcrumbs(tmp_path):
    # PR review: on a wide screen the breadcrumbs are the way back to all libraries; a link without
    # a target is not focusable, so they take the focus and Enter like a button.
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-leaf'))
            with user.client:
                crumbs = [e for e in user.client.elements.values() if isinstance(e, ui.link)]
            seen['focusable'] = [e.props.get('tabindex') == '0' for e in crumbs]
            home = next(e for e in crumbs if 'stp-crumb' in e.classes)
            UserInteraction(user, {home}, None).trigger('keydown.enter')
            seen['home'] = await _until(lambda: 'col:10' not in {e.props['data-node'] for e in _rows(user, 'data-node')})

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['focusable'] and all(seen['focusable']), seen['focusable']
    assert seen['home']


def test_a_filter_keeps_its_box_while_its_list_is_drawn_again(tmp_path):
    # Owner, 2026-11-20: after the second letter the box lost what was typed and the caret. A box
    # drawn again is a new element; only the list below it is drawn again now.
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    seen = {}

    def shown():
        return {e.props.get('data-node') for e in _rows(user_box['user'], 'data-node')}

    user_box = {}

    async def go():
        async with _app(object()) as user:
            user_box['user'] = user
            await _get_page(user, '/_test/shelf-picker?start=col:10')
            await _connect(user)
            assert await _until(lambda: {'vol:100', 'vol:101'} <= shown())
            with user.client:
                boxes = [e for e in user.client.elements.values() if isinstance(e, ui.input)]
                box = next(b for b in boxes if any('vol:100' == c.props.get('data-node')
                                                   for c in b.parent_slot.parent.descendants()))
                seen['before'] = {b.id for b in boxes}
                box.value = 'AS 2'
            assert await _until(lambda: 'vol:101' not in shown() and 'vol:100' in shown())
            with user.client:
                box.value = 'AS 10'                                # typed on, into the same box
            assert await _until(lambda: 'vol:100' not in shown() and 'vol:101' in shown())
            await asyncio.sleep(0.2)
            with user.client:
                seen['after'] = {e.id for e in user.client.elements.values() if isinstance(e, ui.input)}
                seen['alive'] = box.id in user.client.elements and not box.is_deleted
                seen['value'] = box.value
            seen['nodes'] = {e.props.get('data-node') for e in _rows(user, 'data-node')}

    try:
        _run(go)
    finally:
        svc.close()
    assert seen["alive"] and seen["value"] == "AS 10", seen
    assert seen['after'] == seen['before'], 'a filter box was drawn again'
    assert 'vol:101' in seen['nodes'] and 'vol:100' not in seen['nodes'], seen['nodes']


def test_two_filters_typed_together_both_draw_their_lists(tmp_path, monkeypatch):
    # PR review: a filter typed in one column while another column's load was under way kept its
    # text but not its list (the later load drew only its own column).
    from web.components import shelfmark_tree_picker as picker
    real = picker._load_view
    calls = {'n': 0}

    def slow(*args):
        calls['n'] += 1
        if calls['n'] > 1:                          # the opening draw is not slowed
            time.sleep(0.4)
        return real(*args)

    monkeypatch.setattr(picker, '_load_view', slow)
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _rows(user, 'data-leaf'))
            with user.client:
                boxes = [e for e in user.client.elements.values() if isinstance(e, ui.input)]
                volumes = next(b for b in boxes if any('vol:100' == c.props.get('data-node')
                                                       for c in b.parent_slot.parent.descendants()))
                leaves = next(b for b in boxes if any('data-leaf' in c.props
                                                      for c in b.parent_slot.parent.descendants()))
                volumes.value = 'AS 10'
                await asyncio.sleep(0.05)
                leaves.value = '2.4'                         # while the first list is still being read
            await asyncio.sleep(1.5)
            seen['nodes'] = {e.props.get('data-node') for e in _rows(user, 'data-node')}
            seen['leaves'] = {e.props.get('data-sys-id') for e in _rows(user, 'data-leaf')}

    try:
        _run(go)
    finally:
        svc.close()
    assert 'vol:100' not in seen['nodes'] and 'vol:101' in seen['nodes'], seen['nodes']
    assert SYS['as2'][3] in seen['leaves'] and SYS['as2'][0] not in seen['leaves'], seen['leaves']
