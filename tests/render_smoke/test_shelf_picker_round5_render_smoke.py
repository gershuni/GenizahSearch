# -*- coding: utf-8 -*-
"""The web shelf picker, round 5 (a reader sifting through the shelf, 2026-11-25), on test pages like
round 4's and its fixture tree:

* the header's button hides the details pane on a wide screen (a class on the main area; a phone folds
  the pane its own way) and shows it again, the choice kept per reader;
* the box before "Joins" chooses every partner on the shelf with the shelfmark the pane shows (the
  multiple choice turned on first), or, when all are chosen, none of the partners, in one change; it
  shows all, some or none ticked; one partner has no such box;
* the pane's picture has previous / next through the manuscript's images and says where it is.
"""
from __future__ import annotations

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

import asyncio
import time

from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

# round 4's pages and stand-ins, this module's own (a page is registered once, by the module that holds
# the dictionaries it reads: importing round 4's would register its pages again over these)
PICKED: list = []
SERVICE: dict = {}
CONFIRMED: list = []
LABEL: dict = {}


async def _on_confirm(entries, label):
    CONFIRMED.append([e['sys_id'] for e in entries])
    return None


@ui.page('/_test/shelf-round5-open')
def _open_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, pick_label='Open')


@ui.page('/_test/shelf-round5-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, purpose='test_round5_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open')


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


def _leaf(user, sys_id):
    return next(e for e in _els(user, 'data-leaf') if e.props['data-sys-id'] == sys_id)


async def _until(predicate, timeout=5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def _setup(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    PICKED.clear()
    CONFIRMED.clear()
    for sid in tuple(SYS['as2']) + tuple(SYS['as10']):
        leaf = svc.path_of(sid)[0].leaf
        LABEL[sid] = leaf.label or leaf.shelfmark or leaf.sys_id
    return svc


HERE, NEAR, FAR = SYS['as2'][6], SYS['as2'][2], SYS['as10'][0]
JOINS = [{'sys_id': NEAR, 'shelfmark': 'T-S AS 2.3', 'sources': ['pgp']},
         {'sys_id': FAR, 'shelfmark': 'T-S AS 10.1', 'sources': ['fjms', 'community']},
         {'sys_id': '', 'shelfmark': 'ENA 9.9', 'sources': ['community']}]


def _details_of(joins_for_here):
    """A stand-in for ``item_details``: the shelfmark as the picker shows it, and HERE's joins."""
    def details(service, leaf, lang='en', **_kw):
        return {'sys_id': leaf.sys_id, 'shelfmark': LABEL.get(leaf.sys_id, leaf.label), 'others': [], 'part': '',
                'title': '', 'subjects': [], 'date': '', 'pages': None, 'preview': '', 'text': [],
                'transcriptions': [], 'comments': None, 'catalogue': None, 'bibliography': None, 'links': [],
                'joins': [dict(j, sources=list(j['sources'])) for j in joins_for_here] if leaf.sys_id == HERE else []}
    return details


def _main(user):
    with user.client:
        return next(e for e in user.client.elements.values() if 'stp-main' in e.classes and not e.is_deleted)


def test_the_header_button_hides_the_pane_and_the_choice_is_kept(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-toggle'))
            [button] = _testid(user, 'shelf-details-toggle')
            seen['before'] = ('stp-pane-hidden' in _main(user).classes, button.props.get('aria-pressed'))
            UserInteraction(user, {button}, None).click()
            assert await _until(lambda: 'stp-pane-hidden' in _main(user).classes)
            seen['hidden'] = button.props.get('aria-pressed')
            from web.safe_storage import safe_user_get
            with user.client:
                seen['stored'] = safe_user_get(picker.DETAILS_KEY)
            # the next opening starts with it hidden
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-toggle'))
            [again] = _testid(user, 'shelf-details-toggle')
            seen['again'] = ('stp-pane-hidden' in _main(user).classes, again.props.get('aria-pressed'))
            UserInteraction(user, {again}, None).click()
            assert await _until(lambda: 'stp-pane-hidden' not in _main(user).classes)
            with user.client:
                seen['shown'] = safe_user_get(picker.DETAILS_KEY)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['before'] == (False, 'true')
    assert seen['hidden'] == 'false' and seen['stored'] is False
    assert seen['again'] == (True, 'false')
    assert seen['shown'] is True


def test_the_joins_box_chooses_every_partner_and_again_none(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    def ticked(user):
        return {e.props['data-sys-id'] for e in _els(user, 'data-leaf') if e.props.get('aria-checked') == 'true'}

    def boxes(user):
        return {e.props['data-join-box']: e.props.get('aria-checked') for e in _els(user, 'data-join-box')}

    def all_box(user):
        return _testid(user, 'shelf-joins-all')[0]

    def first_in_head(user):
        """What comes first in the joins' header line: the box, then "Joins"."""
        head = all_box(user).parent_slot.parent
        kids = [c for c in head.default_slot.children]
        return [('box' if c is all_box(user) else c.text) for c in kids]

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-joins-all'))
            [switch] = _testid(user, 'shelf-picker-switch')
            seen['before'] = (switch.value, all_box(user).props.get('aria-checked'), first_in_head(user))
            UserInteraction(user, {all_box(user)}, None).click()
            assert await _until(lambda: ticked(user) == {HERE, NEAR})         # with the shelfmark the pane shows
            assert await _until(lambda: boxes(user) == {NEAR: 'true', FAR: 'true'})
            assert await _until(lambda: all_box(user).props.get('aria-checked') == 'true')
            seen['on'] = (switch.value, _leaf(user, NEAR).props.get('role'))
            UserInteraction(user, {all_box(user)}, None).click()            # all ticked: a click unticks them
            assert await _until(lambda: boxes(user) == {NEAR: 'false', FAR: 'false'})
            assert await _until(lambda: ticked(user) == {HERE})
            assert await _until(lambda: all_box(user).props.get('aria-checked') == 'false')
            seen['after'] = ticked(user)
            one = next(e for e in _els(user, 'data-join-box') if e.props['data-join-box'] == FAR)
            UserInteraction(user, {one}, None).click()                       # one of two: partly
            assert await _until(lambda: all_box(user).props.get('aria-checked') == 'mixed')
            UserInteraction(user, {all_box(user)}, None).click()            # partly: a click ticks them all
            assert await _until(lambda: boxes(user) == {NEAR: 'true', FAR: 'true'})
            assert await _until(lambda: all_box(user).props.get('aria-checked') == 'true')

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['before'] == (False, 'false', ['box', 'Joins'])            # the box before "Joins"
    assert seen['on'] == (True, 'checkbox')
    assert seen['after'] == {HERE}                                         # unticking leaves the shelfmark shown
    assert CONFIRMED == []


def test_one_partner_has_no_joins_box(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS[1:]))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: len(_els(user, 'data-join-box')) == 1)
            seen['links'] = _testid(user, 'shelf-joins-all')

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['links'] == []


def test_previous_and_next_walk_the_manuscript_s_images(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}/1')
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 3)
    monkeypatch.setattr(picker, '_page_src', lambda sys_id, shelfmark, page, width: f'/img/{sys_id}/{page}')
    scripts = []
    # every script through the element's client: ui.run_javascript has no page in the background task
    # that draws the arrows (it raised there, in the owner's console)
    monkeypatch.setattr(picker, '_run_js', lambda element, code: scripts.append((element.client is not None, code)))
    monkeypatch.setattr(picker.ui, 'run_javascript', lambda *a, **k: scripts.append((False, 'ui.run_javascript')))
    svc = _setup(tmp_path)
    seen = {}

    def img(user):
        [thumb] = [e for e in _els(user, 'src') if str(e.props['src']).startswith('/img/')]
        return thumb.props['src']

    def where(user):
        [nav] = _testid(user, 'shelf-details-pages')
        return next(d.text for d in nav.descendants() if isinstance(d, ui.label))

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-pages'))
            [prev_btn], [next_btn] = _testid(user, 'shelf-page-prev'), _testid(user, 'shelf-page-next')
            seen['first'] = (img(user), where(user), prev_btn.enabled, next_btn.enabled)
            UserInteraction(user, {next_btn}, None).click()
            assert await _until(lambda: img(user).endswith('/2'))
            seen['second'] = (where(user), prev_btn.enabled, next_btn.enabled)
            UserInteraction(user, {next_btn}, None).click()
            assert await _until(lambda: img(user).endswith('/3'))
            seen['last'] = (where(user), next_btn.enabled)
            UserInteraction(user, {prev_btn}, None).click()
            UserInteraction(user, {prev_btn}, None).click()
            assert await _until(lambda: img(user).endswith('/1'))
            seen['back'] = (where(user), prev_btn.enabled)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['first'] == (f'/img/{HERE}/1', 'Image 1 of 3', False, True)
    assert (True, f'(new Image()).src = "/img/{HERE}/2";') in scripts       # the next one, asked of the browser
    assert all(ok for ok, _code in scripts)
    assert seen['second'] == ('Image 2 of 3', True, True)
    assert seen['last'] == ('Image 3 of 3', False)
    assert seen['back'] == ('Image 1 of 3', False)


def test_one_image_has_no_arrows(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}/1')
    asked = []
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: asked.append(sys_id) or 1)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: asked)
            assert not await _until(lambda: _testid(user, 'shelf-details-pages'), timeout=0.5)
            seen['nav'] = _testid(user, 'shelf-details-pages')

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['nav'] == []


def test_the_picture_has_zoom_buttons_and_a_page_step_fits_it_again(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}/1?width=300')
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 2)
    monkeypatch.setattr(picker, '_page_src', lambda sys_id, shelfmark, page, width: f'/img/{sys_id}/{page}?width=300')
    scripts = []
    monkeypatch.setattr(picker, '_run_js', lambda _element, code: scripts.append(code))
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-zoom') and _testid(user, 'shelf-details-pages'))
            [box] = _testid(user, 'shelf-details-zoom')
            seen['init'] = any('window.__stpZoom' in s and f'__stpZoom.init({box.id});' in s for s in scripts)
            seen['img_in_box'] = [c.props.get('src') for c in box.default_slot.children]
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-zoom-in')[0]}, None).click()
            UserInteraction(user, {_testid(user, 'shelf-zoom-out')[0]}, None).click()
            assert await _until(lambda: len(scripts) == 2)
            seen['zoom'] = list(scripts)
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-page-next')[0]}, None).click()
            assert await _until(lambda: scripts)
            seen['step'] = list(scripts)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['init'] and seen['img_in_box'] == [f'/img/{HERE}/1?width=300']
    box_id = seen['zoom'][0].split('(')[-1].split(',')[0]
    assert seen['zoom'] == [f'window.__stpZoom && __stpZoom.by({box_id}, 1.25);',
                            f'window.__stpZoom && __stpZoom.by({box_id}, 0.8);']
    assert f'__stpZoom.reset({box_id});' in seen['step'][0]                  # the next page opens fitted


def test_the_zoom_script_parses():
    import shutil
    import subprocess
    import pytest
    from web.components import shelfmark_tree_picker as picker
    node = shutil.which('node')
    if node is None:
        pytest.skip('node is not installed')
    out = subprocess.run([node, '--check', '-'], input=picker._ZOOM_JS, text=True, capture_output=True, timeout=60)
    assert out.returncode == 0, out.stderr


def test_enter_in_the_multiple_choice_opens_what_is_ticked(tmp_path, monkeypatch):
    # Seen in the app: with several at once, Enter ticked like Space and never opened
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of([]))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-switch?start={HERE}')
            await _connect(user)
            def role():
                rows = [e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == NEAR]
                return rows[0].props.get('role') if rows else None
            assert await _until(lambda: role() == 'option')
            [switch] = _testid(user, 'shelf-picker-switch')
            switch.value = True
            assert await _until(lambda: role() == 'checkbox')
            UserInteraction(user, {_leaf(user, NEAR)}, None).trigger('keydown.enter')   # nothing ticked: it, then Open
            assert await _until(lambda: CONFIRMED)

    try:
        _run(go)
    finally:
        svc.close()
    assert CONFIRMED == [[NEAR]]


def test_the_picture_has_rotate_buttons(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}/1?width=300')
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 1)
    scripts = []
    monkeypatch.setattr(picker, '_run_js', lambda _element, code: scripts.append(code))
    svc = _setup(tmp_path)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round5-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-rotate-right'))
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-rotate-right')[0]}, None).click()
            UserInteraction(user, {_testid(user, 'shelf-rotate-left')[0]}, None).click()
            assert await _until(lambda: len(scripts) == 2)

    try:
        _run(go)
    finally:
        svc.close()
    assert '__stpZoom.rotate(' in scripts[0] and scripts[0].endswith(', 90);')
    assert scripts[1].endswith(', -90);')
