# -*- coding: utf-8 -*-
"""Render-smoke: "Pick from the shelf" on the web hosts that name one manuscript (shelf tree
picker PRs 5-6, web; plan §6 "test the call sites").

Each host's button opens the shared picker (``web.components.shelf_pick``); here the picker is
replaced by a stand-in that records what it was opened with, and the test hands the host's
handler a pick, as a click on a shelfmark would. What is checked is where the pick goes: the
same function the host's typed box or list already reaches.

* Puzzle and Reading Desk: several at once since the multiple choice
  (tests/render_smoke/test_shelf_pick_multi_hosts_render_smoke.py).
* Joins Lab: ``load_anchor`` (the anchor viewer is built for the pick).
* New join: one or several from the shelf (DX15: a join may hold many fragments); one is fragment B
  through the form's one ``choose_fragment``, several are each joined to this fragment in one
  insert; the fragment itself and repeats are left out; more than ``JOIN_SHELF_MAX`` takes none.
* Home: Browse at the pick, with its place (``?place=``), which Browse takes as the reader's.
* The tree not served (flag on, file missing): no button on any host.
* A LOCAL ``97…`` id never reaches a handler.
"""
from __future__ import annotations

import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nicegui import events, ui

from web.components import shelf_pick

PICKED_SID = '990000000000000777'
PLACEMENT = f'inv:4242:{PICKED_SID}'
TREE = object()


class Picker:
    """Stands in for ``open_shelf_picker``: records each opening; ``pick`` answers the last."""

    def __init__(self):
        self.opened = []

    def __call__(self, container, service, on_pick=None, start=None, title=None, **multi):
        self.opened.append({'service': service, 'on_pick': on_pick, 'start': start, **multi})

    async def pick(self, user, sys_id=PICKED_SID, shelfmark='T-S 99.1', placement_id=PLACEMENT):
        with user._client:
            await self.opened[-1]['on_pick'](
                {'sys_id': sys_id, 'shelfmark': shelfmark, 'placement_id': placement_id})


@pytest.fixture
def picker(monkeypatch):
    stand_in = Picker()
    monkeypatch.setattr(shelf_pick, 'open_shelf_picker', stand_in)
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: TREE)
    return stand_in


@pytest.fixture
def no_tree(monkeypatch):
    stand_in = Picker()
    monkeypatch.setattr(shelf_pick, 'open_shelf_picker', stand_in)
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: None)
    return stand_in


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _by_testid(user, testid):
    return [e for e in _elements(user) if (e.props or {}).get('data-testid') == testid]


def _click(user, element):
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args=None))


async def _until(check, timeout=10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out')
        await asyncio.sleep(0.05)


# -- the helper -------------------------------------------------------------------------------

def test_only_a_corpus_manuscript_reaches_a_handler():
    assert shelf_pick.corpus_selection({'sys_id': PICKED_SID, 'shelfmark': 'X', 'placement_id': PLACEMENT}) == {
        'sys_id': PICKED_SID, 'shelfmark': 'X', 'placement_id': PLACEMENT}
    assert shelf_pick.corpus_selection({'sys_id': '970000000000000777', 'shelfmark': 'mine'}) is None
    assert shelf_pick.corpus_selection({'sys_id': '99 1', 'shelfmark': 'X'}) is None
    assert shelf_pick.corpus_selection(None) is None


def test_a_place_in_a_link_must_look_like_one():
    assert shelf_pick.is_placement_id(PLACEMENT) and shelf_pick.is_placement_id('cat:7:1')
    for bad in ('', None, 'x', 'inv:', '<script>:1', 'inv:1 2', 'INV:1', 'inv:' + 'a' * 200):
        assert not shelf_pick.is_placement_id(bad), bad
    assert shelf_pick.browse_url_for_pick({'sys_id': PICKED_SID, 'placement_id': PLACEMENT}) == (
        f'/browse?sys_id={PICKED_SID}&place=inv%3A4242%3A{PICKED_SID}')
    assert shelf_pick.browse_url_for_pick({'sys_id': PICKED_SID, 'placement_id': 'x y'}) == (
        f'/browse?sys_id={PICKED_SID}')


def test_the_button_is_absent_while_the_tree_is_not_served(monkeypatch):
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: None)
    built = []
    monkeypatch.setattr(shelf_pick.ui, 'button', lambda *a, **k: built.append(a))
    assert shelf_pick.shelf_pick_button(lambda s: None) is None
    assert built == []


# -- Fragment Puzzle --------------------------------------------------------------------------

def _puzzle_page(user):
    from tests.render_smoke.test_puzzle_export_render_smoke import _answer_save_token
    user.javascript_rules[re.compile(r'.*puzzleCanvas\.get(Crop)?State\(\)')] = lambda _m: '{}'
    user.javascript_rules[re.compile(r'.*clearAll\(\)')] = lambda _m: None
    _answer_save_token(user)






def test_puzzle_has_no_button_without_the_tree(no_tree):
    from tests.render_smoke.test_puzzle_export_render_smoke import _run

    async def driver(user):
        _puzzle_page(user)
        await user.open('/puzzle')
        assert _by_testid(user, 'puzzle-shelf-pick') == []

    _run(driver)


# -- Reading Desk -----------------------------------------------------------------------------

def _desk_run(tmp_path, monkeypatch, driver):
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    from web.pages import browse
    desks = []
    real = browse.persist_browse_snapshot

    def persist(state, page=None):
        desks.append([e.get('sys_id') for e in state.reading_desk_entries] if state.view_joined else None)
        return real(state, page)

    monkeypatch.setattr(browse, 'persist_browse_snapshot', persist)
    rd._run(tmp_path, lambda user, enter: driver(user, enter, desks))






def test_browse_takes_the_place_a_link_names_and_keeps_it(tmp_path, monkeypatch):
    # The home page's pick opens /browse?sys_id=..&place=..: Browse shows the manuscript at that
    # place (it may sit at two) and keeps it in the address; its first place, or a malformed or
    # foreign one, leaves the address without it.
    from nicegui import Client
    from shared.shelfmark_tree_service import ShelfmarkTreeService
    from tests.render_smoke.test_browse_button_icons_render_smoke import _run_home_smoke
    from tests.shelfmark_tree_fixture import SYS, build_fixture
    from web.pages import browse
    from web.services import BrowsePage
    db, _libraries, _meta = build_fixture(tmp_path)
    tree = ShelfmarkTreeService(str(db))
    two = SYS['two_places']
    first, second = tree.path_of(two)
    asked, sent = [], []
    real_place_of, real_js = browse.shelf_place_of, Client.run_javascript

    def place_of(service, sys_id, placement_id=None):
        asked.append(placement_id)
        return real_place_of(service, sys_id, placement_id)

    def record(self, code, *args, **kwargs):
        sent.append(code)
        return real_js(self, code, *args, **kwargs)

    monkeypatch.setattr(browse, 'shelf_tree_service', lambda: tree)
    monkeypatch.setattr(browse, 'shelf_place_of', place_of)
    monkeypatch.setattr(Client, 'run_javascript', record)
    service = MagicMock()
    service.is_ready.return_value = True
    service.get_browse_page.side_effect = lambda sys_id, p_num=None, **_kw: BrowsePage(
        uid=f'{sys_id}-1', p_num=1, text='t', full_header='V0.8', total_pages=1, current_idx=1,
        sys_id=sys_id, shelfmark='T-S NS 5.3', library_code='CUL')
    service.get_browse_page_by_fl.return_value = None

    def addresses():
        return [c for c in sent if 'history.replaceState' in c]

    async def driver(user):
        async def opened(query):
            asked.clear()
            sent.clear()
            await user.open(f'/browse?sys_id={two}{query}')
            await _until(addresses)
            return addresses()[-1]

        address = await opened(f'&place={second.leaf.id}')
        assert second.leaf.id in asked
        assert 'place=' + second.leaf.id.replace(':', '%3A') in address
        for query in (f'&place={first.leaf.id}', '&place=<b>', '&place=inv:0:990000000000000001', ''):
            assert 'place=' not in await opened(query), query

    crossref = MagicMock()
    crossref.is_available.return_value = False
    try:
        with patch.object(browse, 'get_service', return_value=service), \
                patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
                patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
                patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
            _run_home_smoke(driver, lang='en')
    finally:
        tree.close()


# -- Joins Lab anchor -------------------------------------------------------------------------

def test_joins_lab_anchors_the_pick_as_load_anchor_does(picker, monkeypatch):
    from tests.render_smoke.conftest import STUB_ANCHOR_SID, run_joins_lab_smoke
    from web.pages import joins_lab
    built = []

    class Viewer(joins_lab.AnchorViewer):
        def __init__(self, *args, sys_id=None, **kwargs):
            built.append(sys_id)
            super().__init__(*args, sys_id=sys_id, **kwargs)

    monkeypatch.setattr(joins_lab, 'AnchorViewer', Viewer)

    async def driver(user):
        await user.open('/joins-lab')
        [button] = _by_testid(user, 'joins-lab-shelf-pick')
        assert not button.props.get('unelevated')       # Load Anchor stays the unelevated one
        _click(user, button)
        assert picker.opened[-1]['pick_label'] == 'Load Anchor'     # the host's own words (DX21)
        await picker.pick(user, sys_id=STUB_ANCHOR_SID, shelfmark='T-S 12.001')
        await _until(lambda: built)
        assert built == [STUB_ANCHOR_SID]

    run_joins_lab_smoke(driver)


def test_joins_lab_has_no_button_without_the_tree(no_tree):
    from tests.render_smoke.conftest import run_joins_lab_smoke

    async def driver(user):
        await user.open('/joins-lab')
        assert _by_testid(user, 'joins-lab-shelf-pick') == []

    run_joins_lab_smoke(driver)


# -- A new join, one or several fragments -----------------------------------------------------

FRAGMENT_A = '990000000000000111'


@ui.page('/_test/join-form')
def _join_form_page():
    from web.components.joins_panel import show_add_join_form
    show_add_join_form('T-S A1.1', document_id=FRAGMENT_A)


def _join_entry(sys_id, shelfmark):
    return {'sys_id': sys_id, 'shelfmark': shelfmark, 'placement_id': f'inv:1:{sys_id}'}


async def _join_confirm(user, stand_in, entries):
    with user._client:
        return await stand_in.opened[-1]['on_confirm'](entries, 'chosen')


def _labels(user):
    return [e.text for e in _elements(user) if isinstance(e, ui.label)]


def _create_button(user):
    [create] = [e for e in _elements(user) if isinstance(e, ui.button)
                and getattr(e, 'text', '') == 'Create Join']
    return create


def _join_form_run(monkeypatch, driver, created=None, created_several=None):
    from tests.render_smoke.test_home_teaser_render_smoke import _run_home_smoke
    from web.components import joins_panel
    monkeypatch.setattr(joins_panel.GlobalAuthState, 'is_logged_in', lambda: True)
    monkeypatch.setattr(joins_panel.GlobalAuthState, 'get_user_id', lambda: 'user-1')
    monkeypatch.setattr(joins_panel.state, 'lists_mgr', None)
    monkeypatch.setattr(joins_panel, 'invalidate_joins_cache', lambda **_k: None)
    monkeypatch.setattr(joins_panel, 'create_fragment_join', created or MagicMock(return_value={'id': 1}))
    monkeypatch.setattr(joins_panel, 'create_fragment_joins',
                        created_several or MagicMock(return_value={'success': True, 'joins': []}))
    _run_home_smoke(driver, lang='en')


def test_a_join_takes_fragment_b_from_the_shelf(picker, monkeypatch):
    created = MagicMock(return_value={'id': 1})

    async def driver(user):
        await user.open('/_test/join-form')
        await _until(lambda: _by_testid(user, 'join-shelf-pick'))
        _click(user, _by_testid(user, 'join-shelf-pick')[0])
        opened = picker.opened[-1]
        assert opened['start'] == FRAGMENT_A                        # opens at fragment A's place
        assert (opened['mode'], opened['purpose'], opened['confirm_label']) == (
            'multi', 'join', 'Add them to the join')
        # the fragment itself alone: taken out of the choice, nothing joined, the picker closes
        assert await _join_confirm(user, picker, [_join_entry(FRAGMENT_A, 'T-S A1.1')]) is None
        assert 'That is this fragment itself' in user.notify.messages      # not joined to itself
        assert 'Selected fragment:' not in _labels(user)
        assert await _join_confirm(user, picker, [_join_entry(PICKED_SID, 'T-S 99.1')]) is None
        await _until(lambda: 'Selected fragment:' in _labels(user))
        assert 'T-S 99.1' in _labels(user)
        # what the form submits is the pick: both its id and its shelfmark
        _click(user, _create_button(user))
        await _until(lambda: created.called)
        kwargs = created.call_args.kwargs
        assert (kwargs['fragment_b_sys_id'], kwargs['fragment_b_shelfmark']) == (PICKED_SID, 'T-S 99.1')
        assert kwargs['fragment_a_sys_id'] == FRAGMENT_A
        assert 'Join created' in user.notify.messages

    _join_form_run(monkeypatch, driver, created=created)


SID_P, SID_Q, SID_R = '990000000000000201', '990000000000000202', '990000000000000203'


def test_a_join_takes_several_from_the_shelf_each_joined_to_this_fragment(picker, monkeypatch):
    several = MagicMock(return_value={'success': True, 'joins': [{}, {}]})
    single = MagicMock(return_value={'id': 1})

    async def driver(user):
        await user.open('/_test/join-form')
        await _until(lambda: _by_testid(user, 'join-shelf-pick'))
        _click(user, _by_testid(user, 'join-shelf-pick')[0])
        took = await _join_confirm(user, picker, [
            _join_entry(SID_P, 'T-S 2.1'), _join_entry(FRAGMENT_A, 'T-S A1.1'),
            _join_entry(SID_Q, 'T-S 2.2'), _join_entry(SID_P, 'T-S 2.1'), _join_entry(SID_R, 'T-S 2.3')])
        assert took is None                                         # all taken: the picker closes
        assert 'That is this fragment itself' in user.notify.messages
        await _until(lambda: 'Selected fragments:' in _labels(user))
        assert [t for t in _labels(user) if t.startswith('T-S 2.')] == ['T-S 2.1', 'T-S 2.2', 'T-S 2.3']
        # one taken out again: the others stay, in their order
        [card] = _by_testid(user, 'join-several')
        removes = [e for e in card.descendants() if isinstance(e, ui.button) and e.props.get('icon') == 'close']
        assert len(removes) == 3
        _click(user, removes[1])
        await _until(lambda: 'T-S 2.2' not in _labels(user))
        assert 'Selected fragments:' in _labels(user)
        _click(user, _create_button(user))
        await _until(lambda: several.called)
        kwargs = several.call_args.kwargs
        assert kwargs['fragment_a_sys_id'] == FRAGMENT_A
        assert kwargs['fragments_b'] == [(SID_P, 'T-S 2.1'), (SID_R, 'T-S 2.3')]
        assert '2 joins created' in user.notify.messages
        assert not single.called
        # a second click after the joins were made makes no copy
        _click(user, _create_button(user))
        await asyncio.sleep(0.1)
        assert several.call_count == 1

    _join_form_run(monkeypatch, driver, created=single, created_several=several)


def test_one_left_of_several_is_fragment_b_and_the_notes_stay(picker, monkeypatch):
    single = MagicMock(return_value={'id': 1})

    async def driver(user):
        await user.open('/_test/join-form')
        await _until(lambda: _by_testid(user, 'join-shelf-pick'))
        _click(user, _by_testid(user, 'join-shelf-pick')[0])
        await _join_confirm(user, picker, [_join_entry(SID_P, 'T-S 2.1'), _join_entry(SID_Q, 'T-S 2.2')])
        await _until(lambda: 'Selected fragments:' in _labels(user))
        [notes] = [e for e in _elements(user) if isinstance(e, ui.textarea)]
        with user._client:
            notes.set_value('same hand')
        [card] = _by_testid(user, 'join-several')
        removes = [e for e in card.descendants() if isinstance(e, ui.button) and e.props.get('icon') == 'close']
        _click(user, removes[0])
        await _until(lambda: 'Selected fragment:' in _labels(user))
        [notes] = [e for e in _elements(user) if isinstance(e, ui.textarea)]
        assert notes.value == 'same hand'
        _click(user, _create_button(user))
        await _until(lambda: single.called)
        assert (single.call_args.kwargs['fragment_b_sys_id'], single.call_args.kwargs['notes']) == (
            SID_Q, 'same hand')

    _join_form_run(monkeypatch, driver, created=single)


def test_more_than_the_most_a_join_takes_takes_none(picker, monkeypatch):
    from web.components.joins_panel import JOIN_SHELF_MAX

    async def driver(user):
        await user.open('/_test/join-form')
        await _until(lambda: _by_testid(user, 'join-shelf-pick'))
        _click(user, _by_testid(user, 'join-shelf-pick')[0])
        entries = [_join_entry(f'9900000000001{n:05d}', f'T-S 3.{n}') for n in range(JOIN_SHELF_MAX + 1)]
        assert await _join_confirm(user, picker, entries) == []    # none taken: all stay chosen
        assert f'A join takes at most {JOIN_SHELF_MAX} fragments at a time' in user.notify.messages
        assert 'Selected fragments:' not in _labels(user)
        took = await _join_confirm(user, picker, entries[:JOIN_SHELF_MAX])
        assert took is None
        await _until(lambda: 'Selected fragments:' in _labels(user))

    _join_form_run(monkeypatch, driver)


def test_a_failed_insert_keeps_the_form_and_may_be_tried_again(picker, monkeypatch):
    several = MagicMock(return_value={'error': 'insert refused'})

    async def driver(user):
        await user.open('/_test/join-form')
        await _until(lambda: _by_testid(user, 'join-shelf-pick'))
        _click(user, _by_testid(user, 'join-shelf-pick')[0])
        await _join_confirm(user, picker, [_join_entry(SID_P, 'T-S 2.1'), _join_entry(SID_Q, 'T-S 2.2')])
        await _until(lambda: 'Selected fragments:' in _labels(user))
        _click(user, _create_button(user))
        await _until(lambda: 'insert refused' in _labels(user))
        assert 'Selected fragments:' in _labels(user)               # nothing made: all still there
        _click(user, _create_button(user))
        await _until(lambda: several.call_count == 2)

    _join_form_run(monkeypatch, driver, created_several=several)


# -- Home -------------------------------------------------------------------------------------

async def _opens_several_in_the_desk(user, opened):
    """The home page's and the header's picker carry Browse's switch and its kept choice; several
    ticked are kept for Browse, which is opened at the first with ``desk=shelf``
    (web/reading_desk_handoff.py)."""
    from web.reading_desk_handoff import take_reading_desk_handoff
    assert (opened['switch_label'], opened['purpose'], opened['confirm_label']) == (
        'Open more than one', 'reading_desk', 'Open')
    assert opened.get('mode') is None and opened['on_pick'] is not None     # opens as the single choice
    assert (opened['limit'].max_items, opened['limit'].max_pages, opened['limit'].base()) == (30, 1000, frozenset())
    second = '990000000000000888'
    entries = [{'sys_id': PICKED_SID, 'shelfmark': 'T-S 99.1', 'placement_id': PLACEMENT},
               {'sys_id': second, 'shelfmark': 'T-S 99.2', 'placement_id': None},
               {'sys_id': '970000000000000777', 'shelfmark': 'mine', 'placement_id': None}]
    with patch('nicegui.ui.navigate.to') as navigate:
        with user._client:
            took = await opened['on_confirm'](entries, 'chosen')
    assert took == [PICKED_SID, second]                     # handed over: they leave the choice
    assert [c.args for c in navigate.call_args_list] == [
        (f'/browse?sys_id={PICKED_SID}&place=inv%3A4242%3A{PICKED_SID}&desk=shelf',)]
    with user._client:
        assert take_reading_desk_handoff('990000000000000888') == []        # Browse at another: not its
    with patch('nicegui.ui.navigate.to'):
        with user._client:
            await opened['on_confirm'](entries, 'chosen')
            handed = take_reading_desk_handoff(PICKED_SID)
            again = take_reading_desk_handoff(PICKED_SID)
    assert [h['sys_id'] for h in handed] == [PICKED_SID, second] and again == []    # taken once


def test_home_opens_the_pick_in_browse_at_its_place(picker):
    from tests.render_smoke.test_home_teaser_render_smoke import _run_home_smoke

    async def driver(user):
        await user.open('/')
        [button] = _by_testid(user, 'home-shelf-pick')
        _click(user, button)
        # the pick opens the manuscript: "Open", with Add to List and Add to Puzzle beside it (DX21, DX22)
        assert (picker.opened[-1]['pick_label'], picker.opened[-1]['add_to']) == ('Open', True)
        with patch('nicegui.ui.navigate.to') as navigate:
            await picker.pick(user)
        assert [c.args for c in navigate.call_args_list] == [
            (f'/browse?sys_id={PICKED_SID}&place=inv%3A4242%3A{PICKED_SID}',)]
        # a LOCAL id never gets there
        with patch('nicegui.ui.navigate.to') as navigate:
            await picker.pick(user, sys_id='970000000000000777')
        assert not navigate.called
        # Browse's picker: "Open more than one" opens the ticked ones in Browse's Reading Desk
        await _opens_several_in_the_desk(user, picker.opened[-1])
        # the tree closed after the page was drawn: the button opens nothing
        opened = len(picker.opened)
        shelf_pick.shelf_tree_service = lambda: None
        _click(user, button)
        assert len(picker.opened) == opened

    _run_home_smoke(driver, lang='en')


def test_the_button_speaks_the_visitors_language(picker):
    from tests.render_smoke.test_home_teaser_render_smoke import _run_home_smoke

    async def driver(user):
        await user.open('/')
        [button] = _by_testid(user, 'home-shelf-pick')
        assert button.text == 'בחירה מהמדף'
        assert button.props['aria-label'] == 'בחירה מהמדף'

    _run_home_smoke(driver, lang='he')


def test_home_has_no_button_without_the_tree(no_tree):
    from tests.render_smoke.test_home_teaser_render_smoke import _run_home_smoke

    async def driver(user):
        await user.open('/')
        assert _by_testid(user, 'home-shelf-pick') == []

    _run_home_smoke(driver, lang='en')
