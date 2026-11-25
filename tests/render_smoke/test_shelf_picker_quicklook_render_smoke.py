# -*- coding: utf-8 -*-
"""The web shelf picker's quick look (owner, 2026-11-25), on test pages of its own and the fixture tree:

* the pane's button opens it on the pane's shelfmark, at the pane's page and turn; Q on a row (the
  ``stp-look`` event of ``_KEYS_JS``) on that row's;
* the browser is asked for each image by the site's own routes with its width left to it (``__W__``),
  the next image and the next shelfmark's first ahead;
* its arrows go through the list's shelfmarks and the manuscript's images; Space chooses (several at
  once: ticks), Enter takes it as the picker's button; closed, the list is at the shelfmark seen last.
"""
from __future__ import annotations

import asyncio
import json
import time

from nicegui import events, ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

PICKED: list = []
SERVICE: dict = {}
CONFIRMED: list = []


async def _on_confirm(entries, label):
    CONFIRMED.append([e['sys_id'] for e in entries])
    return None


@ui.page('/_test/shelf-look-open')
def _open_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, pick_label='Open')


@ui.page('/_test/shelf-look-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, purpose='test_look_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open')


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


async def _until(predicate, timeout=5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


FIRST, SECOND = SYS['as2'][0], SYS['as2'][1]


def _setup(tmp_path, monkeypatch, scripts):
    from web.components import shelfmark_tree_picker as picker

    def details(service, leaf, lang='en', **_kw):
        return {'sys_id': leaf.sys_id, 'shelfmark': leaf.label, 'others': [], 'part': '', 'title': '',
                'subjects': [], 'date': '', 'pages': None, 'preview': '', 'text': [], 'transcriptions': [],
                'comments': None, 'catalogue': None, 'bibliography': None, 'links': [], 'joins': []}
    monkeypatch.setattr(picker, 'item_details', details)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: f'/img/{sys_id}/1?width={width}')
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 3)
    monkeypatch.setattr(picker, '_page_src', lambda sys_id, shelfmark, page, width: f'/img/{sys_id}/{page}?width={width}')
    monkeypatch.setattr(picker, '_run_js', lambda element, code: scripts.append(code))
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    PICKED.clear()
    CONFIRMED.clear()
    return svc


def _shows(scripts):
    """The quick look's ``__stpLook.show`` calls: (low, page, ahead, opts)."""
    out = []
    for code in scripts:
        if '__stpLook.show(' in code:
            inner = code.split('__stpLook.show(', 1)[1].rsplit(');', 1)[0]
            args = json.loads('[' + inner.split(',', 1)[1] + ']')
            out.append(tuple(args))
    return out


def _look_parts(user):
    [card] = _testid(user, 'shelf-look')
    labels = [d for d in card.descendants() if isinstance(d, ui.label)]
    return card, {', '.join(d.classes): d for d in labels}


def _label(user, cls):
    _card, labels = _look_parts(user)
    return next(el.text for key, el in labels.items() if cls in key)


def test_the_pane_s_button_opens_it_at_the_pane_s_page_and_the_arrows_walk(tmp_path, monkeypatch):
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-open?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-pages') and _testid(user, 'shelf-quick-look'))
            UserInteraction(user, {_testid(user, 'shelf-page-next')[0]}, None).click()
            [pane_box] = _testid(user, 'shelf-details-zoom')
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-quick-look')[0]}, None).click()
            assert await _until(lambda: len(_shows(scripts)) >= 2)
            seen['shows'] = _shows(scripts)
            seen['pane_box'] = pane_box.id
            seen['title'] = _label(user, 'stp-look-title')
            seen['where'] = _label(user, 'stp-look-where')
            seen['page'] = _label(user, 'stp-look-plabel')
            seen['prev_enabled'] = _testid(user, 'shelf-look-prev')[0].enabled
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-look-page-next')[0]}, None).click()
            assert await _until(lambda: _shows(scripts))
            seen['page3'] = (_label(user, 'stp-look-plabel'), _shows(scripts)[-1],
                             _testid(user, 'shelf-look-page-next')[0].enabled, any('reset(' in s for s in scripts))
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-look-next')[0]}, None).click()
            assert await _until(lambda: len(_shows(scripts)) >= 2)
            seen['next'] = (_label(user, 'stp-look-title'), _label(user, 'stp-look-where'), _shows(scripts),
                            _label(user, 'stp-look-plabel'))
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-look-close')[0]}, None).click()
            assert await _until(lambda: any('K.pending' in s for s in scripts))
            seen['back'] = [s for s in scripts if 'K.pending' in s][0]
            rows = [e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == SECOND]
            assert await _until(lambda: rows and rows[0].props.get('aria-selected') == 'true')

    try:
        _run(go)
    finally:
        svc.close()
    low, page, ahead, opts = seen['shows'][0]
    assert low == f'/img/{FIRST}/2?width=300'                       # the pane's page, at once
    assert opts == {'fresh': True, 'like': seen['pane_box'], 'none': False}       # its turn
    low, page, ahead, opts = seen['shows'][-1]
    assert page == f'/img/{FIRST}/2?width=__W__'                    # its width the browser's
    assert ahead == [f'/img/{FIRST}/3?width=__W__', f'/img/{SECOND}/1?width=__W__']
    assert seen['title'] and seen['where'].startswith('1 of ') and seen['prev_enabled']   # it goes on past the list
    assert seen['page'] == 'Image 2 of 3'
    label, show, next_enabled, reset = seen['page3']
    assert label == 'Image 3 of 3' and show[1] == f'/img/{FIRST}/3?width=__W__' and not next_enabled and reset
    title, where, shows, page_label = seen['next']
    assert where.startswith('2 of ') and page_label == 'Image 1 of 3'
    assert shows[0][3] == {'fresh': True, 'like': None, 'none': False} and shows[-1][1] == f'/img/{SECOND}/1?width=__W__'
    assert 'kind: "leaves"' in seen['back']                      # the keyboard back on its row


def test_q_on_a_row_opens_it_and_enter_takes_it(tmp_path, monkeypatch):
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-open?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: [e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == SECOND])
            [card] = _testid(user, 'shelf-picker')
            row = next(e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == SECOND)
            listener = next(lst for lst in card._event_listeners.values() if lst.type in ('stp-look', 'stpLook'))
            events.handle_event(listener.handler, events.GenericEventArguments(
                sender=card, client=user.client, args={'detail': row.props['data-leaf']}))
            assert await _until(lambda: _testid(user, 'shelf-look'))
            assert await _until(lambda: _label(user, 'stp-look-where').startswith('2 of '))
            [open_btn] = _testid(user, 'shelf-look-open')
            assert open_btn.text == 'Open'
            UserInteraction(user, {open_btn}, None).click()
            assert await _until(lambda: PICKED)

    try:
        _run(go)
    finally:
        svc.close()
    assert [p['sys_id'] for p in PICKED] == [SECOND]


def test_several_at_once_the_box_ticks_it(tmp_path, monkeypatch):
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-switch?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-quick-look'))
            [switch] = _testid(user, 'shelf-picker-switch')
            switch.value = True
            assert await _until(lambda: [e for e in _els(user, 'data-leaf') if e.props.get('role') == 'checkbox'])
            UserInteraction(user, {_testid(user, 'shelf-quick-look')[0]}, None).click()
            assert await _until(lambda: _testid(user, 'shelf-look-choose'))
            [choose] = _testid(user, 'shelf-look-choose')
            seen['before'] = ('stp-look-off' in choose.classes, choose.props.get('aria-pressed'))
            UserInteraction(user, {choose}, None).click()
            assert await _until(lambda: choose.props.get('aria-pressed') == 'true')
            [open_btn] = _testid(user, 'shelf-look-open')
            seen['open'] = open_btn.text
            UserInteraction(user, {open_btn}, None).click()
            assert await _until(lambda: CONFIRMED)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['before'] == (False, 'false')
    assert seen['open'] == 'Open them'
    assert CONFIRMED == [[FIRST]]


def test_past_the_list_s_end_it_goes_on_in_the_shelf_s_order_and_closing_goes_there(tmp_path, monkeypatch):
    # owner, 2026-11-25: past the current group to the next item in the next group
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-open?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-quick-look'))
            UserInteraction(user, {_testid(user, 'shelf-quick-look')[0]}, None).click()
            assert await _until(lambda: _testid(user, 'shelf-look-last'))
            UserInteraction(user, {_testid(user, 'shelf-look-last')[0]}, None).click()
            assert await _until(lambda: _label(user, 'stp-look-where').split(' of ')[0]
                                == _label(user, 'stp-look-where').split(' of ')[-1])
            seen['last'] = _label(user, 'stp-look-title')
            UserInteraction(user, {_testid(user, 'shelf-look-next')[0]}, None).click()
            assert await _until(lambda: _label(user, 'stp-look-title') != seen['last'])
            seen['after'] = (_label(user, 'stp-look-title'), _label(user, 'stp-look-where'))
            zoom_in = _testid(user, 'shelf-look-zoom-in')[0]
            with user.client:
                [tip] = [e for e in user.client.elements.values()
                         if isinstance(e, ui.tooltip) and e.props.get('target') == f'#{zoom_in.html_id}']
            seen['tip'] = tip.text
            UserInteraction(user, {_testid(user, 'shelf-look-close')[0]}, None).click()
            title = seen['after'][0]

            def chosen():
                return [e for e in _els(user, 'data-leaf') if e.props.get('aria-selected') == 'true']
            assert await _until(lambda: chosen() and title in ' '.join(
                d.text for d in chosen()[0].descendants() if isinstance(d, ui.label)))

    try:
        _run(go)
    finally:
        svc.close()
    title, where = seen['after']
    assert ' of ' not in where and where                              # its group's name, outside the list
    assert seen['tip'] == 'Zoom in (+)'


def test_a_microfilm_opens_on_its_middle_image_and_q_on_the_pane_s_shelfmark_at_its_page(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 9)
    monkeypatch.setattr(picker, '_code_of', lambda sys_id: 'RNL')
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-open?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-pages'))
            with user.client:
                imgs = [e for e in user.client.elements.values()
                        if str(e.props.get('src', '')).startswith(f'/img/{FIRST}/')]
            seen['pane'] = imgs[0].props['src']
            UserInteraction(user, {_testid(user, 'shelf-page-next')[0]}, None).click()     # the pane at 6
            [card] = _testid(user, 'shelf-picker')
            row = next(e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == FIRST)
            listener = next(lst for lst in card._event_listeners.values() if lst.type in ('stp-look', 'stpLook'))
            scripts.clear()
            events.handle_event(listener.handler, events.GenericEventArguments(
                sender=card, client=user.client, args={'detail': row.props['data-leaf']}))
            assert await _until(lambda: len(_shows(scripts)) >= 2)
            seen['q'] = (_label(user, 'stp-look-plabel'), _shows(scripts)[0][0])
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-look-next')[0]}, None).click()
            assert await _until(lambda: len(_shows(scripts)) >= 2)
            seen['next'] = (_label(user, 'stp-look-plabel'), _shows(scripts)[-1])

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['pane'] == f'/img/{FIRST}/5?width=300'                # past START and the card: the middle
    assert seen['q'] == ('Image 6 of 9', f'/img/{FIRST}/6?width=300')  # the pane's page, at once
    label, (low, page, ahead, _opts) = seen['next']
    assert label == 'Image 5 of 9' and page == f'/img/{SECOND}/5?width=__W__'
    assert low.startswith(f'/img/{SECOND}/5?width=')                  # that page small, blurred meanwhile


def test_a_microfilm_s_tile_is_its_middle_image_and_the_pane_keeps_it(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    thumbnail_src = picker._thumbnail_src                              # the real one: _setup replaces it
    scripts = []
    svc = _setup(tmp_path, monkeypatch, scripts)
    monkeypatch.setattr(picker, '_thumbnail_src', thumbnail_src)
    monkeypatch.setattr(picker, '_image_total', lambda sys_id: 9)
    monkeypatch.setattr(picker, '_code_of', lambda sys_id: 'RNL')
    tile = f'/api/nli_image_by_sysid/{FIRST}?page=0&width=300&middle=1'
    assert picker._thumbnail_src(FIRST, 'x', 300) == tile
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-look-open?start={FIRST}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-pages'))

            def src():
                with user.client:
                    return [str(e.props.get('src')) for e in user.client.elements.values()
                            if FIRST in str(e.props.get('src', ''))]
            seen['pane'] = src()
            UserInteraction(user, {_testid(user, 'shelf-page-next')[0]}, None).click()
            seen['next'] = src()
            UserInteraction(user, {_testid(user, 'shelf-page-prev')[0]}, None).click()
            seen['back'] = src()
            scripts.clear()
            UserInteraction(user, {_testid(user, 'shelf-quick-look')[0]}, None).click()
            assert await _until(lambda: len(_shows(scripts)) >= 2)
            seen['look'] = (_label(user, 'stp-look-plabel'), _shows(scripts)[-1])

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['pane'] == [tile]                                      # no second picture for the middle
    assert seen['next'] == [f'/img/{FIRST}/6?width=300'] and seen['back'] == [tile]
    label, (low, page, _ahead, _opts) = seen['look']
    assert label == 'Image 5 of 9' and low == tile and page == f'/img/{FIRST}/5?width=__W__'
    monkeypatch.setattr(picker, '_code_of', lambda sys_id: 'CUL')
    assert 'middle' not in thumbnail_src(FIRST, 'x', 300)
