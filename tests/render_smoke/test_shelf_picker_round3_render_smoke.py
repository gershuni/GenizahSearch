# -*- coding: utf-8 -*-
"""The web shelf picker, round 3 (the handoff's DX19-DX25), on test pages in a NiceGUI User
simulation with the fixture tree of tests/shelfmark_tree_fixture.py.

* DX19: the breadcrumb's arrow is the one mirrored character in Hebrew too.
* DX20: a library code ("Harkavy", the Kiev library's) finds the library in the Hebrew filter.
* DX21: a click selects a shelfmark (shown chosen, nothing picked); the foot's button, with the
  host's word, picks it; a double click, or Enter on the selected one, is the button.
* DX22: Add to List and Add to Puzzle beside it, in both modes; the ticks stay after either.
* DX23: no node boxes in the multiple choice, "Select all" only; the exclusions keep them.
* DX24: a shelfmark's tooltip has the manuscript's title on its own line.
* DX25 (as revised): the pane shows the shelfmark clicked last and what the manuscript has (a few
  lines of text, the facts lines, the community's joins and comments last), its thumbnail after the
  text; a newer click wins.
"""
from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import patch

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_service import ShelfmarkTreeService
from shared.shelfmark_tree_view import CRUMB_SEP
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

PICKED: list = []
SERVICE: dict = {}
LISTS: dict = {}
CONFIRMED: list = []


async def _on_confirm(entries, label):
    CONFIRMED.append([e['sys_id'] for e in entries])
    return None


@ui.page('/_test/shelf-round3-he')
def _hebrew_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    from web.translations import set_language
    set_language('he')
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None)


@ui.page('/_test/shelf-round3-open')
def _open_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, pick_label='Open',
                      add_to=True, lists_mgr=LISTS.get('mgr'))


@ui.page('/_test/shelf-round3-multi')
def _multi_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_round3',
                      on_confirm=_on_confirm, confirm_label='Open them', add_to=True,
                      lists_mgr=LISTS.get('mgr'))


@ui.page('/_test/shelf-round3-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, purpose='test_round3_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open', add_to=True, lists_mgr=LISTS.get('mgr'))


@ui.page('/_test/shelf-round3-exclude')
def _exclude_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_round3_ex',
                      on_confirm=_on_confirm, confirm_label='Exclude them', node_boxes=True)


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _classed(user, cls):
    with user.client:
        return [e for e in user.client.elements.values() if cls in e.classes and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


def _leaf(user, sys_id):
    return next(e for e in _els(user, 'data-leaf') if e.props['data-sys-id'] == sys_id)


def _pane_shelfmark(user) -> str:
    found = _testid(user, 'shelf-details-shelfmark')
    return found[0].text if found else ''


def _texts_in(element) -> list:
    return [d.text for d in element.descendants() if isinstance(d, ui.label)]


def _dialog_open(user) -> bool:
    with user.client:
        return any(isinstance(d, ui.dialog) and d.value and not d.is_deleted
                   for d in user.client.elements.values())


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
    for sid in SYS['as2']:
        leaf = svc.path_of(sid)[0].leaf
        LABEL[sid] = leaf.label or leaf.shelfmark or leaf.sys_id
    return svc


LABEL: dict = {}


def _label_of(_svc, sys_id) -> str:
    """A shelfmark as the picker shows it (read when the tree was opened: asserts run after it is closed)."""
    return LABEL[sys_id]


# -- DX19, DX20 ----------------------------------------------------------------------------------

def test_hebrew_crumbs_use_the_mirrored_arrow_and_a_code_finds_its_library(tmp_path):
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-he?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf')), 'no shelfmark rendered'
            seen['seps'] = [e.text for e in _classed(user, 'stp-crumb-sep')]
            with user.client:
                boxes = [e for e in user.client.elements.values() if isinstance(e, ui.input) and not e.is_deleted]
                boxes[0].value = 'harkavy'                     # the library column's filter, in Hebrew
            assert await _until(lambda: {e.props['data-node'] for e in _els(user, 'data-node')
                                         if e.props['data-node'].startswith('lib:')} == {'lib:3'})
            seen['libs'] = {e.props['data-node'] for e in _els(user, 'data-node')
                            if e.props['data-node'].startswith('lib:')}

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['seps'] and set(seen['seps']) == {CRUMB_SEP}         # not "‹", which bidi mirrors back
    assert seen['libs'] == {'lib:3'}


# -- DX21, DX25 ----------------------------------------------------------------------------------

def test_a_click_selects_the_button_opens_and_the_pane_follows(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark: f'/api/nli_image_by_sysid/{sys_id}?page=0&width=320')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-open?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            [button] = _testid(user, 'shelf-picker-confirm')
            seen['word'] = button.text
            # opened at a manuscript: it is the one selected, and the pane shows it
            assert await _until(lambda: _pane_shelfmark(user) == _label_of(svc, SYS['as2'][6]))
            seen['start_selected'] = _leaf(user, SYS['as2'][6]).props.get('aria-selected')
            assert await _until(lambda: any(isinstance(d, ui.element) and d.tag == 'img'
                                            for d in _testid(user, 'shelf-details')[0].descendants()))
            img = next(d for d in _testid(user, 'shelf-details')[0].descendants() if d.tag == 'img')
            seen['img'] = (img.props.get('src'), img.props.get('alt'), img.props.get('loading'))
            holder = next(d for d in _testid(user, 'shelf-details')[0].descendants()
                          if 'stp-d-thumb' in d.classes)
            seen['thumb_shown'] = holder.visible
            UserInteraction(user, {img}, None).trigger('error')      # no image there: none shown, not a broken one
            assert await _until(lambda: not holder.visible)
            # a click selects another: shown chosen, the pane follows, nothing is picked
            target = _leaf(user, SYS['as2'][5])
            UserInteraction(user, {target}, None).click()
            assert await _until(lambda: _pane_shelfmark(user) == _label_of(svc, SYS['as2'][5]))
            seen['after_click'] = (target.props.get('aria-selected'), 'stp-on' in target.classes,
                                   _leaf(user, SYS['as2'][6]).props.get('aria-selected'), list(PICKED),
                                   _dialog_open(user))
            # Enter on another row selects it; Enter on the selected one is the button
            other = _leaf(user, SYS['as2'][3])
            UserInteraction(user, {other}, None).trigger('keydown.enter')
            assert await _until(lambda: other.props.get('aria-selected') == 'true')
            seen['after_enter_other'] = list(PICKED)
            UserInteraction(user, {other}, None).trigger('keydown.enter')
            assert await _until(lambda: PICKED)
            seen['picked'] = [p['sys_id'] for p in PICKED]
            seen['open_after'] = _dialog_open(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['word'] == 'Open'
    assert seen['start_selected'] == 'true'
    assert seen['thumb_shown'] is True
    assert seen['img'] == (f'/api/nli_image_by_sysid/{SYS["as2"][6]}?page=0&width=320', _label_of(svc, SYS['as2'][6]), 'lazy')
    assert seen['after_click'] == ('true', True, 'false', [], True)
    assert seen['after_enter_other'] == []
    assert seen['picked'] == [SYS['as2'][3]] and seen['open_after'] is False


def test_a_double_click_is_the_button(tmp_path):
    svc = _setup(tmp_path)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-open?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            UserInteraction(user, {_leaf(user, SYS['as2'][2])}, None).trigger('dblclick')
            assert await _until(lambda: PICKED)

    try:
        _run(go)
    finally:
        svc.close()
    assert [p['sys_id'] for p in PICKED] == [SYS['as2'][2]]


def test_the_pane_is_empty_until_a_shelfmark_is_chosen_and_a_newer_click_wins(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    real = picker.item_details
    hold = threading.Event()

    def slow_for_the_first(service, leaf, lang='en', **kw):
        if leaf.sys_id == SYS['as2'][1]:
            hold.wait(5)
        return real(service, leaf, lang, **kw)

    monkeypatch.setattr(picker, 'item_details', slow_for_the_first)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-round3-open?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            seen['empty'] = _texts_in(_testid(user, 'shelf-details')[0])
            seen['disabled'] = [b.enabled for b in _testid(user, 'shelf-picker-confirm')]
            UserInteraction(user, {_leaf(user, SYS['as2'][1])}, None).click()     # its details are slow
            await asyncio.sleep(0.1)
            UserInteraction(user, {_leaf(user, SYS['as2'][2])}, None).click()
            assert await _until(lambda: _pane_shelfmark(user) == _label_of(svc, SYS['as2'][2]))
            hold.set()
            await asyncio.sleep(0.4)
            seen['last'] = _pane_shelfmark(user)
            seen['no_img'] = not any(d.tag == 'img' for d in _testid(user, 'shelf-details')[0].descendants())

    try:
        _run(go)
    finally:
        hold.set()
        svc.close()
    assert seen['empty'] == ['Choose a shelfmark to see it here']
    assert seen['disabled'] == [False]
    assert seen['last'] == _label_of(svc, SYS['as2'][2])                 # the older answer was dropped
    assert seen['no_img']                                                # no thumbnail: none drawn


# -- DX24 ----------------------------------------------------------------------------------------

def test_a_shelfmarks_tooltip_has_its_title_on_its_own_line(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, '_title_of', lambda sys_id: 'A letter of the Gaon' if sys_id == SYS['as2'][5] else '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-open?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            for sid in (SYS['as2'][5], SYS['as2'][4]):
                tips = [d for d in _leaf(user, sid).descendants() if isinstance(d, ui.tooltip)]
                seen[sid] = [(t.text, 'stp-tip' in t.classes) for t in tips]

    try:
        _run(go)
    finally:
        svc.close()
    label = _label_of(svc, SYS['as2'][5])
    assert seen[SYS['as2'][5]] == [(f'{label}\nA letter of the Gaon', True)]
    assert seen[SYS['as2'][4]] == [(_label_of(svc, SYS['as2'][4]), True)]   # no title: the shelfmark alone


# -- DX22 ----------------------------------------------------------------------------------------

class _Lists:
    """A reader's lists: one of their own, a system list (not offered)."""

    def __init__(self):
        self.data = {'lists': {'L1': {'name': 'Letters'}, 'SYS': {'name': 'Recent', 'is_system': True}}}
        self.calls = []

    async def create_list(self, name):
        self.calls.append(('create', name))
        return 'L9'

    async def add_items(self, ids, list_id):
        self.calls.append(('add', list_id, list(ids)))
        return len(ids)

    async def sys_ids_in_list(self, list_id):
        return set()


def test_add_to_list_and_to_the_puzzle_in_the_single_choice(tmp_path, monkeypatch):
    from web import joins_lab_storage
    from web.components import add_to_list_dialog
    staged = []
    monkeypatch.setattr(joins_lab_storage, 'stage_fragments_for_puzzle',
                        lambda ids, source='join': staged.append((list(ids), source)) or len(ids))
    asked = []
    monkeypatch.setattr(add_to_list_dialog, 'show_add_to_list_dialog',
                        lambda sys_id, shelfmark, mgr, **_kw: asked.append((sys_id, shelfmark, mgr)))
    LISTS['mgr'] = mgr = _Lists()
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-open?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            seen['words'] = [b.text for b in _classed(user, 'stp-foot')[0].descendants() if isinstance(b, ui.button)]
            UserInteraction(user, {_leaf(user, SYS['as2'][5])}, None).click()
            assert await _until(lambda: _leaf(user, SYS['as2'][5]).props.get('aria-selected') == 'true')
            UserInteraction(user, {_classed(user, 'stp-save')[0]}, None).click()
            assert await _until(lambda: asked)
            seen['open_after_list'] = _dialog_open(user)
            with patch('nicegui.ui.navigate.to') as navigate:
                UserInteraction(user, {_classed(user, 'stp-puzzle')[0]}, None).click()
                assert await _until(lambda: navigate.called)
                seen['to'] = [c.args for c in navigate.call_args_list]
            seen['open_after_puzzle'] = _dialog_open(user)

    try:
        _run(go)
    finally:
        svc.close()
        LISTS.clear()
    assert seen['words'] == ['Add to List', 'Add to Puzzle', 'Open']
    assert asked == [(SYS['as2'][5], _label_of(svc, SYS['as2'][5]), mgr)]
    assert seen['open_after_list'] is True                              # the picker stays open
    # one goes as several do: the Puzzle takes it only if its canvas has room (Codex on #88)
    assert staged == [([SYS['as2'][5]], 'shelf')]
    assert seen['to'] == [('/puzzle',)] and seen['open_after_puzzle'] is False
    assert PICKED == []


def test_add_to_list_and_to_the_puzzle_keep_the_ticks(tmp_path, monkeypatch):
    from web import joins_lab_storage
    from web.auth_state import GlobalAuthState
    monkeypatch.setattr(GlobalAuthState, 'is_logged_in', classmethod(lambda cls: True))
    staged = []
    monkeypatch.setattr(joins_lab_storage, 'stage_fragments_for_puzzle',
                        lambda ids, source='join': staged.append((list(ids), source)) or len(ids))
    LISTS['mgr'] = mgr = _Lists()
    svc = _setup(tmp_path)
    seen = {}
    a, b = SYS['as2'][1], SYS['as2'][2]

    def ticked(user):
        return {e.props['data-sys-id'] for e in _els(user, 'data-leaf') if e.props.get('aria-checked') == 'true'}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-round3-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            seen['words'] = [b_.text for b_ in _classed(user, 'stp-foot')[0].descendants()
                             if isinstance(b_, ui.button)]
            UserInteraction(user, {_leaf(user, a)}, None).click()
            assert await _until(lambda: ticked(user) == {a})
            UserInteraction(user, {_leaf(user, b)}, None).click()
            assert await _until(lambda: ticked(user) == {a, b})
            # the pane shows the row clicked last
            assert await _until(lambda: _pane_shelfmark(user) == _label_of(svc, b))
            # Add to List: into one of the reader's lists (the system list not offered)
            UserInteraction(user, {_classed(user, 'stp-save')[0]}, None).click()
            assert await _until(lambda: _testid(user, 'shelf-save-target'))
            [target] = _testid(user, 'shelf-save-target')
            seen['options'] = dict(target.options)
            with user.client:
                target.value = 'L1'
            save = next(e for e in _testid(user, 'shelf-save-list')[0].descendants()
                        if isinstance(e, ui.button) and e.text == 'Save')
            UserInteraction(user, {save}, None).click()
            assert await _until(lambda: any(c[0] == 'add' for c in mgr.calls))
            await asyncio.sleep(0.2)
            seen['ticked_after_list'] = ticked(user)
            # Add to Puzzle: handed over, the Puzzle opened, the ticks kept
            with patch('nicegui.ui.navigate.to') as navigate:
                UserInteraction(user, {_classed(user, 'stp-puzzle')[0]}, None).click()
                assert await _until(lambda: navigate.called)
                seen['to'] = [c.args for c in navigate.call_args_list]
            seen['ticked_after_puzzle'] = ticked(user)

    try:
        _run(go)
    finally:
        svc.close()
        LISTS.clear()
    assert seen['words'] == ['Clear', 'Add to List', 'Add to Puzzle', 'Open them']
    assert seen['options'] == {'__new__': '+ Create new list', 'L1': 'Letters'}
    assert mgr.calls == [('add', 'L1', [a, b])]                         # into the list chosen, none made
    assert seen['ticked_after_list'] == {a, b}
    assert staged == [([a, b], 'shelf')] and seen['to'] == [('/puzzle',)]
    assert seen['ticked_after_puzzle'] == {a, b}
    assert CONFIRMED == []                                               # only the confirm takes them


# -- DX23 ----------------------------------------------------------------------------------------

def test_no_node_boxes_in_the_multiple_choice_but_the_exclusions_keep_them(tmp_path):
    svc = _setup(tmp_path)
    seen = {}

    def opened(page):
        async def go():
            async with _app(object()) as user:
                await _get_page(user, f'/_test/shelf-round3-{page}?start=col:10')
                await _connect(user)
                assert await _until(lambda: _els(user, 'data-whole') and _els(user, 'data-node'))
                await asyncio.sleep(0.2)
                seen[page] = (len(_els(user, 'data-node-box')), len(_els(user, 'data-whole')),
                              [e.text for e in _classed(user, 'stp-hint')
                               if e.text != 'Choose a shelfmark to see it here'])
        return go

    try:
        _run(opened('multi'))
        _run(opened('exclude'))
    finally:
        svc.close()
    assert seen['multi'] == (0, 1, ['Tick shelfmarks to choose them.'])
    boxes, whole, hint = seen['exclude']
    assert boxes > 0 and whole == 1 and hint == ['Tick shelfmarks, or a whole volume, to choose them.']


def test_the_thumbnail_is_asked_of_the_sites_own_image_route():
    # never NLI's own address: the site's proxy, with its breaker, as every thumbnail on the site
    from web.components import shelfmark_tree_picker as picker
    src = picker._thumbnail_src('990000010205171', 'T-S AS 2.1')
    assert src.startswith('/api/') and 'iiif.nli.org.il' not in src


def test_add_to_puzzle_pressed_with_several_does_nothing_if_the_switch_is_flipped_meanwhile(tmp_path, monkeypatch):
    # Codex on this PR: the button read the mode again after the choice was resolved, and sent only
    # the selected shelfmark of the single choice the switch had gone back to
    from web import joins_lab_storage
    from web.components import shelfmark_tree_picker as picker
    staged = []
    monkeypatch.setattr(joins_lab_storage, 'stage_fragments_for_puzzle',
                        lambda ids, source='join': staged.append((list(ids), source)) or len(ids))
    real = picker._resolve

    def slow(*args):
        time.sleep(0.5)
        return real(*args)

    monkeypatch.setattr(picker, '_resolve', slow)
    svc = _setup(tmp_path)
    a, b = SYS['as2'][1], SYS['as2'][2]
    seen = {}

    def ticked(user):
        return {e.props['data-sys-id'] for e in _els(user, 'data-leaf') if e.props.get('aria-checked') == 'true'}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-switch?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            [switch] = _testid(user, 'shelf-picker-switch')
            with user.client:
                switch.value = True
            assert await _until(lambda: any(e.props.get('role') == 'checkbox' for e in _els(user, 'data-leaf')))
            UserInteraction(user, {_leaf(user, a)}, None).click()
            assert await _until(lambda: ticked(user) == {a})
            UserInteraction(user, {_leaf(user, b)}, None).click()
            assert await _until(lambda: ticked(user) == {a, b})
            with patch('nicegui.ui.navigate.to') as navigate:
                UserInteraction(user, {_classed(user, 'stp-puzzle')[0]}, None).click()
                await asyncio.sleep(0.1)
                with user.client:
                    switch.value = False                   # while the choice is read
                await asyncio.sleep(1.0)
                seen['to'] = [c.args for c in navigate.call_args_list]
            seen['open'] = _dialog_open(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert staged == [] and seen['to'] == [] and seen['open'] is True


def test_the_pane_says_what_the_manuscript_has(tmp_path, monkeypatch):
    # DX25 as revised, laid out as round 4 has it: its text page by page (``read_pages``), the field
    # lines, and what the community database knows (its joins with the other sources' partners once,
    # its public comments), read last
    from web.components import shelfmark_tree_picker as picker
    page = ('בשם השם נעשה ונצליח ' * 3 + '\n') * 5               # five lines, all of them shown
    monkeypatch.setattr(picker, '_page_texts', lambda: (lambda sys_id: iter([page, '', 'שורה אחרונה'])))
    monkeypatch.setattr(picker, '_page_count', lambda: (lambda sys_id: 3))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark: '')
    asked = []

    def community(sys_id):
        asked.append(sys_id)
        return [('990000000000123', 'T-S 99.1'), ('', 'ENA 2.2')], 4

    monkeypatch.setattr(picker, '_community', community)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round3-open?start={SYS["as2"][6]}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-text'))
            assert await _until(lambda: any(e.props.get('data-fact') == 'comments' for e in _els(user, 'data-fact')))
            seen['text'] = _texts_in(_testid(user, 'shelf-details-text')[0])
            seen['lines'] = {e.props['data-fact']: e.content for e in _els(user, 'data-fact')}
            seen['joins'] = [e.props['data-join'] for e in _els(user, 'data-join')]
            seen['place'] = _testid(user, 'shelf-details-place')

    try:
        _run(go)
    finally:
        svc.close()
    assert asked == [SYS['as2'][6]]
    assert seen['text'] == ['Page 1', page.strip(), 'Page 3', 'שורה אחרונה']
    assert seen['lines']['pages'] == '<b>Pages</b>: 3 · <b>With text</b>: 2'
    assert seen['lines']['comments'] == '<b>Comments</b>: 4'
    assert seen['joins'] == ['990000000000123', 'ENA 2.2']
    assert seen['place'] == []                                     # the reader is in the picker: no place


def test_the_list_chooser_reads_the_lists_already_loaded():
    # Codex on #88: asking the manager whether each list is the recent one read all the lists again
    # from the database, on the event loop, once per list; the recent list is a system list anyway
    from web.components import shelfmark_tree_picker as picker

    class Lists:
        data = {'lists': {'L1': {'name': 'Letters'}, 'R': {'name': 'Recently viewed', 'is_system': True}}}

        def _is_recent_list(self, list_id):
            raise AssertionError('asked about a list one by one')

    assert picker._list_options(Lists()) == {'L1': 'Letters'}
