# -*- coding: utf-8 -*-
"""The web shelf picker's multiple-choice mode (plan §10.13, D11, D12; picker PR 4).

On a test page in a NiceGUI User simulation, with the fixture tree of
tests/shelfmark_tree_fixture.py: shelfmarks and a whole volume are ticked; a whole volume is one
chip; the running count is of distinct manuscripts; the confirm button hands the host the entries
in the order chosen and clears what it took; a partial take keeps the rest and the dialog open;
the pending choice comes back when the picker is opened again (and goes once applied); a node's
box ticks the node without opening it; a single-choice dialog has no boxes at all.
"""
from __future__ import annotations

import asyncio
import time

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

CONFIRMED: list = []
SERVICE: dict = {}
TAKE: dict = {'n': None}          # None: the host takes everything; a number: only that many


async def _on_confirm(entries, label):
    CONFIRMED.append((entries, label))
    if TAKE['n'] is None:
        return None
    return [e['sys_id'] for e in entries[:TAKE['n']]]


@ui.page('/_test/shelf-picker-multi')
def _picker_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    # the node boxes, as the search's exclusions keep them (handoff DX23); the other hosts' pickers
    # have none (tests/render_smoke/test_shelf_picker_round3_render_smoke.py)
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_multi',
                      on_confirm=_on_confirm, confirm_label='Use them', node_boxes=True)


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _classed(user, cls):
    with user.client:
        return [e for e in user.client.elements.values() if cls in e.classes and not e.is_deleted]


def _total(user) -> str:
    labels = _classed(user, 'stp-total')
    return labels[0].text if labels else ''


async def _until(predicate, timeout=5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def _leaf(user, sys_id):
    return next(e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == sys_id)


def _setup(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    CONFIRMED.clear()
    TAKE['n'] = None
    return svc


def test_ticks_a_volume_and_shelfmarks_confirms_and_clears(tmp_path):
    svc = _setup(tmp_path)
    vol = svc.members_under('vol:100')
    wanted = [s for s in svc.sys_ids_under('vol:100') if s != SYS['as2'][2]]
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-multi?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole')), 'no "Select all"'
            seen['no_pick_rows'] = all(e.props.get('role') == 'checkbox' for e in _els(user, 'data-leaf'))
            # one shelfmark
            UserInteraction(user, {_leaf(user, SYS['as2'][1])}, None).click()
            assert await _until(lambda: _total(user).startswith('1 '))
            seen['one_ticked'] = _leaf(user, SYS['as2'][1]).props.get('aria-checked')
            # the whole volume: one chip, which takes the shelfmark's chip in
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            seen['chips_after_volume'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]
            seen['all_ticked'] = all(e.props.get('aria-checked') == 'true' for e in _els(user, 'data-leaf')
                                     if e.props.get('data-sys-id') in vol)
            # untick one inside the volume: still one chip, one fewer
            UserInteraction(user, {_leaf(user, SYS['as2'][2])}, None).click()
            assert await _until(lambda: _total(user).startswith(f'{len(vol) - 1} '))
            seen['chips_after_untick'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]
            seen['whole_state'] = _els(user, 'data-whole')[0].props.get('aria-checked')
            # confirm: the host gets them in shelf order, the selection is cleared and the dialog closes
            confirm = _classed(user, 'stp-confirm')[0]
            UserInteraction(user, {confirm}, None).click()
            assert await _until(lambda: CONFIRMED)
            await asyncio.sleep(0.3)
            with user.client:
                seen['open'] = [d for d in user.client.elements.values() if isinstance(d, ui.dialog) and d.value]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['no_pick_rows']
    assert seen['one_ticked'] == 'true'
    assert seen['chips_after_volume'] == ['node:all:vol:100'], seen['chips_after_volume']
    assert seen['all_ticked']
    assert seen['chips_after_untick'] == ['node:all:vol:100']
    assert seen['whole_state'] == 'mixed'
    entries, label = CONFIRMED[0]
    assert [e['sys_id'] for e in entries] == wanted
    assert all(e['placement_id'] and e['shelfmark'] for e in entries)
    assert label == 'CUL T-S AS 2'                                  # the chip's name, its library's code first
    assert seen['open'] == []


def test_a_partial_take_keeps_the_rest_and_a_reopened_picker_shows_it(tmp_path):
    svc = _setup(tmp_path)
    TAKE['n'] = 2
    vol = svc.sys_ids_under('vol:100')
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)
            assert await _until(lambda: _total(user).startswith(f'{len(vol) - 2} '))
            with user.client:
                seen['open_after_partial'] = any(isinstance(d, ui.dialog) and d.value
                                                 for d in user.client.elements.values())
            # the same browser opens the picker again (a reload): the rest is still chosen
            await _get_page(user, '/_test/shelf-picker-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _total(user).startswith(f'{len(vol) - 2} '))
            seen['chips_again'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]
            seen['ticked_again'] = sorted(e.props['data-sys-id'] for e in _els(user, 'data-leaf')
                                          if e.props.get('aria-checked') == 'true')
            # applied in full this time: opened once more, nothing is chosen
            TAKE['n'] = None
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: len(CONFIRMED) == 2)
            await asyncio.sleep(0.3)
            await _get_page(user, '/_test/shelf-picker-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _total(user) != '')
            seen['after_full'] = _total(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['open_after_partial'] is True
    assert seen['chips_again'] == ['node:all:vol:100']
    assert seen['ticked_again'] == sorted(vol[2:])
    assert [e['sys_id'] for e in CONFIRMED[1][0]] == vol[2:]
    assert seen['after_full'].startswith('0 ')


def test_a_node_box_ticks_the_node_without_opening_it(tmp_path):
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi?start=col:10')
            await _connect(user)
            assert await _until(lambda: any(e.props.get('data-node-box') == 'vol:101'
                                            for e in _els(user, 'data-node-box')))
            seen['libraries_boxed'] = [e.props['data-node-box'] for e in _els(user, 'data-node-box')
                                       if e.props['data-node-box'].startswith('lib:')]
            box = next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == 'vol:101')
            UserInteraction(user, {box}, None).trigger('click.stop')
            assert await _until(lambda: _total(user).startswith(f'{len(SYS["as10"])} '))
            seen['on_nodes'] = sorted(e.props['data-node'] for e in _els(user, 'data-node') if 'stp-on' in e.classes)
            seen['box'] = next(e for e in _els(user, 'data-node-box')
                               if e.props['data-node-box'] == 'vol:101').props.get('aria-checked')
            seen['col_box'] = next((e.props.get('aria-checked') for e in _els(user, 'data-node-box')
                                    if e.props['data-node-box'] == 'col:10'), None)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['libraries_boxed'] == []
    assert 'vol:101' not in seen['on_nodes'], seen['on_nodes']          # ticked, not opened
    assert seen['box'] == 'true'
    assert seen['col_box'] in ('mixed', None)


@ui.page('/_test/shelf-picker-single-of-multi')
def _single_page(start: str = ''):
    # its own page: importing the single-choice test module from inside a test registered its page
    # late, and that module's own tests then found no page to open
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], lambda _picked: None, start=start or None)


def test_the_single_choice_dialog_has_no_boxes(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    SERVICE['svc'] = svc
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-single-of-multi?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            seen['boxes'] = _els(user, 'data-node-box') + _els(user, 'data-whole') + _classed(user, 'stp-chips')
            seen['roles'] = {e.props.get('role') for e in _els(user, 'data-leaf')}

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['boxes'] == []
    assert seen['roles'] == {'option'}                                 # selected, not ticked (DX21)


# -- Codex review of PR 4, round 1 ----------------------------------------------------------------

LISTS: dict = {}


@ui.page('/_test/shelf-picker-multi-lists')
def _picker_page_lists(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_multi',
                      on_confirm=_on_confirm, lists_mgr=LISTS['mgr'])


def test_a_catalogue_list_of_a_chosen_volume_shows_ticked_as_its_click_reads_it(tmp_path):
    # Codex P2: a box drawn empty by a guess from shelf positions, whose click then unticked the
    # manuscripts it holds. The catalogue's list of T-S NS 5 holds T-S NS 5.1-5.4.
    from tests.test_shelfmark_tree import _with_catalogue
    db, _libraries, _meta = _with_catalogue(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    SERVICE['svc'] = svc
    CONFIRMED.clear()
    TAKE['n'] = None
    group = next(c.id for c in svc.children('col:11') if c.kind == 'group')
    assert svc.members_under(group) <= svc.members_under('vol:110')
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi?start=col:11')
            await _connect(user)
            assert await _until(lambda: any(e.props.get('data-node-box') == 'vol:110'
                                            for e in _els(user, 'data-node-box')))
            box = next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == 'vol:110')
            UserInteraction(user, {box}, None).trigger('click.stop')
            assert await _until(lambda: _total(user).startswith(f"{len(svc.members_under('vol:110'))} "))
            seen['group'] = next(e for e in _els(user, 'data-node-box')
                                 if e.props['data-node-box'] == group).props.get('aria-checked')

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['group'] == 'true'


def test_a_sign_in_while_the_choice_is_read_hands_the_host_nothing(tmp_path, monkeypatch):
    # Codex P2: the account changed while the choice was resolved; the host still took it.
    from web import shelf_selection_store as store
    from web.components import shelfmark_tree_picker as picker
    svc = _setup(tmp_path)
    who = {'id': 'anon'}
    monkeypatch.setattr(store, 'current_owner', lambda: who['id'])
    real = picker._resolve

    def slow(*args):
        time.sleep(0.4)
        return real(*args)

    monkeypatch.setattr(picker, '_resolve', slow)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: not _total(user).startswith('0 ') and _total(user) != '')
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            await asyncio.sleep(0.1)
            who['id'] = 'u2'                                  # signed in in another tab meanwhile
            await asyncio.sleep(0.8)
            with user.client:
                seen['open'] = any(isinstance(d, ui.dialog) and d.value for d in user.client.elements.values())

    try:
        _run(go)
    finally:
        svc.close()
    assert CONFIRMED == []
    assert seen['open'] is False


class _SlowLists:
    def __init__(self):
        self.calls = []
        self.fill = 0

    async def create_list(self, name):
        self.calls.append(('create', name))
        await asyncio.sleep(0.3)
        return 100 + len([c for c in self.calls if c[0] == 'create'])

    async def add_items(self, ids, list_id):
        self.calls.append(('add', list_id))
        self.fill += 1
        return len(ids) if self.fill > 1 else 0                # the first fill fails

    async def sys_ids_in_list(self, list_id):
        return set()                                           # the failed fill added nothing


def test_save_as_a_list_once_per_click_and_a_retry_fills_its_own_list(tmp_path, monkeypatch):
    # Codex P2: two quick clicks made two lists; a retry left behind by a closed dialog filled an
    # earlier list under another name.
    from web.auth_state import GlobalAuthState
    monkeypatch.setattr(GlobalAuthState, 'is_logged_in', classmethod(lambda cls: True))
    svc = _setup(tmp_path)
    LISTS['mgr'] = mgr = _SlowLists()
    seen = {}

    def save_button(user):
        return next(e for e in _all_buttons(user) if 'stp-save' in e.classes)

    def dialog_save(user):
        return [e for e in _all_buttons(user) if (e.text or '') == 'Save' and 'stp-save' not in e.classes]

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-lists?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: not _total(user).startswith('0 ') and _total(user) != '')
            UserInteraction(user, {save_button(user)}, None).click()
            assert await _until(lambda: dialog_save(user))
            button = dialog_save(user)[0]
            UserInteraction(user, {button}, None).click()
            UserInteraction(user, {button}, None).click()      # a second click while the list is made
            assert await _until(lambda: any(c[0] == 'add' for c in mgr.calls))
            await asyncio.sleep(0.3)
            seen['first'] = list(mgr.calls)
            UserInteraction(user, {dialog_save(user)[0]}, None).click()    # try again: the same list
            assert await _until(lambda: len(mgr.calls) == 3)
            await asyncio.sleep(0.3)
            seen['retry'] = list(mgr.calls)
            # a new dialog starts afresh
            mgr.fill = 0
            before = {e.id for e in dialog_save(user)}
            UserInteraction(user, {save_button(user)}, None).click()
            assert await _until(lambda: {e.id for e in dialog_save(user)} - before)
            newest = max(dialog_save(user), key=lambda e: e.id)         # the new dialog's, not a closed one's
            UserInteraction(user, {newest}, None).click()
            assert await _until(lambda: len(mgr.calls) >= 5)
            seen['again'] = list(mgr.calls)

    try:
        _run(go)
    finally:
        svc.close()
    assert [c[0] for c in seen['first']] == ['create', 'add']
    assert seen['retry'][2] == ('add', seen['first'][1][1])          # filled the list made first
    assert seen['again'][3][0] == 'create'                            # a new list, not the earlier one
    assert seen['again'][4] == ('add', '102')


def _all_buttons(user):
    with user.client:
        return [e for e in user.client.elements.values() if isinstance(e, ui.button) and not e.is_deleted]


def test_a_kept_choice_takes_the_trees_shelfmark_when_it_opens(tmp_path, monkeypatch):
    # Codex P2: a rebuilt tree writes a shelfmark anew; the kept chip's old writing was handed on.
    from shared.shelfmark_tree_selection import Chip, Selection
    from web import shelf_selection_store as store
    svc = _setup(tmp_path)
    leaf = next(p.leaf for p in svc.path_of(SYS['as2'][0]))
    stale = Selection([Chip('leaf', leaf.id, 'OLD', leaf.sys_id, 'OLD')])
    monkeypatch.setattr(store, 'load_selection', lambda purpose, owner: stale.copy())
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi?start=vol:100')
            await _connect(user)
            assert await _until(lambda: _total(user).startswith('1 '))
            seen['chips'] = [e.text for e in _classed(user, 'stp-chip-name')]
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['chips'] == [f'CUL {leaf.label}']                     # named with its library's code
    assert CONFIRMED[0][0] == [{'sys_id': leaf.sys_id, 'shelfmark': leaf.shelfmark, 'placement_id': leaf.id}]

# -- Codex review of PR #70, round 1 ---------------------------------------------------------------

HOLD: dict = {}


async def _held_confirm(entries, label):
    CONFIRMED.append((entries, label))
    await HOLD['event'].wait()              # the host is still working while the reader goes on
    return None


@ui.page('/_test/shelf-picker-multi-held')
def _picker_page_held(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_multi',
                      on_confirm=_held_confirm, node_boxes=True)


def test_what_is_chosen_again_while_the_host_works_stays_chosen(tmp_path):
    # Codex P2 (PR #70): sent a volume, then Clear and the same volume again before the host
    # answered; the host's "took everything" took the new choice too.
    svc = _setup(tmp_path)
    vol = svc.sys_ids_under('vol:101')
    seen = {}

    async def go():
        HOLD['event'] = asyncio.Event()
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-held?start=vol:101')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)               # the host has it, and holds
            UserInteraction(user, {_classed(user, 'stp-clear')[0]}, None).click()
            assert await _until(lambda: _total(user).startswith('0 '))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()     # the same volume again
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            HOLD['event'].set()                                  # the host answers: it took all it was sent
            await asyncio.sleep(0.8)
            seen['total'] = _total(user)
            seen['chips'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]
            with user.client:
                seen['open'] = any(isinstance(d, ui.dialog) and d.value for d in user.client.elements.values())

    try:
        _run(go)
    finally:
        svc.close()
    assert [e['sys_id'] for e in CONFIRMED[0][0]] == vol
    assert seen['total'].startswith(f'{len(vol)} '), seen['total']
    assert seen['chips'] == ['node:all:vol:101']
    assert seen['open'] is True


def test_what_is_not_touched_while_the_host_works_is_taken(tmp_path):
    # the other side of the same rule: a choice left as it was sent leaves with the host
    svc = _setup(tmp_path)
    vol = svc.sys_ids_under('vol:101')
    other = svc.sys_ids_under('vol:100')
    seen = {}

    async def go():
        HOLD['event'] = asyncio.Event()
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-held?start=col:10')
            await _connect(user)
            assert await _until(lambda: any(e.props.get('data-node-box') == 'vol:101'
                                            for e in _els(user, 'data-node-box')))
            box = next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == 'vol:101')
            UserInteraction(user, {box}, None).trigger('click.stop')
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)
            box = next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == 'vol:100')
            UserInteraction(user, {box}, None).trigger('click.stop')                # a new volume meanwhile
            assert await _until(lambda: _total(user).startswith(f'{len(set(vol) | set(other))} '))
            HOLD['event'].set()
            await asyncio.sleep(0.8)
            seen['total'] = _total(user)
            seen['chips'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['chips'] == ['node:all:vol:100']
    assert seen['total'].startswith(f'{len(set(other) - set(vol))} '), seen['total']

def test_the_host_answering_while_a_reselect_is_still_made_takes_nothing_of_it(tmp_path, monkeypatch):
    # Codex P2 (PR #70, round 2): the host answered while the re-select's change was still being
    # made off the loop; the tracker was read and cleared without the lock, so the change could not
    # record what it chose and the host's answer took it out again.
    import threading

    from web.components import shelfmark_tree_picker as picker
    real = picker._apply_change
    gate = {'on': False, 'open': threading.Event()}

    def held(fn, selection, service, track=False, **kw):
        if gate['on']:
            gate['open'].wait(5)                # the re-select's worker: held until the host has answered
        return real(fn, selection, service, track, **kw)

    monkeypatch.setattr(picker, '_apply_change', held)
    svc = _setup(tmp_path)
    vol = svc.sys_ids_under('vol:101')
    seen = {}

    async def go():
        HOLD['event'] = asyncio.Event()
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-held?start=vol:101')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()
            assert await _until(lambda: _total(user).startswith(f'{len(vol)} '))
            UserInteraction(user, {_classed(user, 'stp-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)
            UserInteraction(user, {_classed(user, 'stp-clear')[0]}, None).click()
            assert await _until(lambda: _total(user).startswith('0 '))
            gate['on'] = True
            UserInteraction(user, set(_els(user, 'data-whole')), None).click()     # the re-select, held
            await asyncio.sleep(0.3)
            gate['on'] = False
            HOLD['event'].set()                                  # the host answers first
            await asyncio.sleep(0.3)
            gate['open'].set()                                   # then the re-select is made
            await asyncio.sleep(1.0)
            seen['total'] = _total(user)
            seen['chips'] = [e.props['data-chip'] for e in _els(user, 'data-chip')]

    try:
        _run(go)
    finally:
        gate['open'].set()
        svc.close()
    assert seen['total'].startswith(f'{len(vol)} '), seen['total']
    assert seen['chips'] == ['node:all:vol:101']


def test_select_all_keeps_the_sentence_direction_around_a_name(tmp_path):
    # owner, 2026-11-21: "בחירת הכל ב־Schoeyen (26)" was not right to left. The name is isolated
    # (FSI ... PDI), the count stays outside it, and the label takes the dialog's direction.
    svc = _setup(tmp_path)
    vol = svc.members_under('vol:100')
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-multi?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-whole'))
            [row] = _els(user, 'data-whole')
            [label] = [e for e in row.descendants() if isinstance(e, ui.label)]
            assert await _until(lambda: label.text.endswith(f'({len(vol)})'))
            seen['text'], seen['dir'] = label.text, label.props.get('dir')

    try:
        _run(go)
    finally:
        svc.close()
    name = seen['text'].split('\u2068', 1)[-1].split('\u2069', 1)[0]
    assert seen['text'] == f'Select all in \u2068{name}\u2069 ({len(vol)})', seen['text']
    assert name and seen['dir'] is None


# -- limits when ticking (owner, 2026-11-21; shared/shelfmark_tree_limits.py) ---------------------

LIMIT: dict = {'limit': None}


@ui.page('/_test/shelf-picker-multi-limited')
def _picker_page_limited(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], start=start or None, mode='multi', purpose='test_limited',
                      on_confirm=_on_confirm, confirm_label='Use them', limit=LIMIT['limit'], node_boxes=True)


def _told() -> list:
    """What the reader was told: the test harness's recorder, which stands for ``ui.notify``."""
    return [str(m) for m in getattr(ui.notify, 'messages', [])]


def _clean_store(monkeypatch):
    from shared.shelfmark_tree_selection import Selection
    from web import shelf_selection_store as store
    monkeypatch.setattr(store, 'load_selection', lambda purpose, owner: Selection())
    monkeypatch.setattr(store, 'save_selection', lambda purpose, owner, sel: True)


def test_a_tick_past_the_limit_is_not_kept_and_says_so(tmp_path, monkeypatch):
    from shared.shelfmark_tree_limits import SelectionLimit
    svc = _setup(tmp_path)
    LIMIT['limit'] = SelectionLimit(max_items=2)
    _clean_store(monkeypatch)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-multi-limited?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            for n in (0, 1):
                UserInteraction(user, {_leaf(user, SYS['as2'][n])}, None).click()
                assert await _until(lambda n=n: _total(user).startswith(f'{n + 1} '))
            UserInteraction(user, {_leaf(user, SYS['as2'][2])}, None).click()
            assert await _until(lambda: _told())
            await asyncio.sleep(0.2)
            seen['total'] = _total(user)
            seen['third'] = _leaf(user, SYS['as2'][2]).props.get('aria-checked')
            # unticking always passes
            UserInteraction(user, {_leaf(user, SYS['as2'][0])}, None).click()
            assert await _until(lambda: _total(user).startswith('1 '))
            seen['told'] = _told()

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['told'] == ['Up to 2 manuscripts here']
    assert seen['total'].startswith('2 ') and seen['third'] == 'false'


def test_a_tick_past_the_page_limit_says_the_pages(tmp_path, monkeypatch):
    from shared.shelfmark_tree_limits import SelectionLimit
    svc = _setup(tmp_path)
    LIMIT['limit'] = SelectionLimit(max_pages=5, pages_of=lambda sid: 3)
    _clean_store(monkeypatch)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-multi-limited?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            UserInteraction(user, {_leaf(user, SYS['as2'][0])}, None).click()
            assert await _until(lambda: _total(user).startswith('1 '))
            UserInteraction(user, {_leaf(user, SYS['as2'][1])}, None).click()
            assert await _until(lambda: _told())
            seen['told'] = _told()
            seen['total'] = _total(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['told'] == ['The Reading Desk holds up to 5 pages; these would make 6']
    assert seen['total'].startswith('1 ')


def test_a_node_too_big_is_faded_and_its_box_says_so(tmp_path, monkeypatch):
    from shared.shelfmark_tree_limits import SelectionLimit
    svc = _setup(tmp_path)
    big = svc.node('vol:100').manuscripts
    small = svc.node('vol:101').manuscripts
    assert small <= 4 < big
    LIMIT['limit'] = SelectionLimit(max_items=4)
    _clean_store(monkeypatch)
    seen = {}

    def box(user, nid):
        return next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == nid)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-limited?start=col:10')
            await _connect(user)
            assert await _until(lambda: any(e.props.get('data-node-box') == 'vol:100'
                                            for e in _els(user, 'data-node-box')))
            seen['big'] = ('stp-too-big' in box(user, 'vol:100').classes, box(user, 'vol:100').props.get('aria-disabled'),
                           box(user, 'vol:100').props.get('title'))
            seen['small'] = 'stp-too-big' in box(user, 'vol:101').classes
            UserInteraction(user, {box(user, 'vol:100')}, None).trigger('click.stop')
            assert await _until(lambda: _told())
            seen['told'] = _told()
            seen['total_after_big'] = _total(user)
            UserInteraction(user, {box(user, 'vol:101')}, None).trigger('click.stop')
            assert await _until(lambda: _total(user).startswith(f'{small} '))

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['big'] == (True, 'true', 'Too many to choose here (up to 4)')
    assert seen['small'] is False
    assert seen['told'] == ['Too many to choose here (up to 4)']
    assert not seen['total_after_big'].startswith(f'{big} ')


def test_a_ticked_node_too_big_stays_removable(tmp_path, monkeypatch):
    # a choice kept from before the limits: the oversized node is ticked, and unticking it passes
    from shared.shelfmark_tree_limits import SelectionLimit
    from shared.shelfmark_tree_selection import ALL, Selection, members_function
    from web import shelf_selection_store as store
    svc = _setup(tmp_path)
    kept = Selection()
    assert kept.add_node(svc.node('vol:100'), ALL, members_function(svc))
    big = svc.node('vol:100').manuscripts
    monkeypatch.setattr(store, 'load_selection', lambda purpose, owner: kept.copy())
    monkeypatch.setattr(store, 'save_selection', lambda purpose, owner, sel: True)
    LIMIT['limit'] = SelectionLimit(max_items=4)
    seen = {}

    def box(user, nid):
        return next(e for e in _els(user, 'data-node-box') if e.props['data-node-box'] == nid)

    async def go():
        async with _app(object()) as user:
            await _get_page(user, '/_test/shelf-picker-multi-limited?start=col:10')
            await _connect(user)
            assert await _until(lambda: _total(user).startswith(f'{big} '))
            assert await _until(lambda: box(user, 'vol:100').props.get('aria-checked') == 'true')
            seen['faded'] = 'stp-too-big' in box(user, 'vol:100').classes
            UserInteraction(user, {box(user, 'vol:100')}, None).trigger('click.stop')
            assert await _until(lambda: _total(user).startswith('0 '))
            seen['told'] = _told()

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['faded'] is False and seen['told'] == []


def test_a_tick_is_asked_again_as_it_is_kept(tmp_path, monkeypatch):
    # Codex on #82: the host filled while the tick was counted off the loop (a desk's confirm
    # landing); asked again with no wait before the commit, the tick is not kept
    from shared.shelfmark_tree_limits import SelectionLimit
    svc = _setup(tmp_path)
    asked = {'n': 0}

    def base():
        asked['n'] += 1
        return [f'desk-{i}' for i in range(29 if asked['n'] == 1 else 30)]
    LIMIT['limit'] = SelectionLimit(max_items=30, base_ids=base)
    _clean_store(monkeypatch)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-picker-multi-limited?start={SYS["as2"][0]}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            assert await _until(lambda: _total(user))          # the first count is painted (not raced)
            UserInteraction(user, {_leaf(user, SYS['as2'][0])}, None).click()
            assert await _until(lambda: _told())
            seen['told'] = _told()
            seen['total'] = _total(user)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['told'] == ['Up to 30 manuscripts here']
    assert seen['total'].startswith('0 ')
    assert asked['n'] == 2                                            # counted, then asked again
