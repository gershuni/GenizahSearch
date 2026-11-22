# -*- coding: utf-8 -*-
"""Render-smoke: several manuscripts at once from the shelf, on the web hosts that take several
(plan §10.13; handoff DX5, DX13, DX16): the Reading Desk (from its toolbar, and from Browse's
one shelf button with its switch "Open more than one") and the Fragment Puzzle. The Lists page is driven in tests/test_lists_add_from_shelf.py.

The picker is replaced by a stand-in (tests/render_smoke/test_shelf_pick_hosts_render_smoke.py)
that records how it was opened; the test confirms a choice as the picker's confirm button does,
``on_confirm(entries, label)``, and checks where it goes and what the host answers: the ids it
took, the rest staying chosen (``Selection.consume``).

* Reading Desk: each through ``_add_sys_id_to_reading_desk``, one already there counted as taken,
  what fits its limit with what it holds (owner, 2026-11-21: 30 manuscripts and 1,000 pages; the rest
  stay ticked and the reader is told which limit), a desk started with the manuscript shown when none
  was open, nothing for a desk left meanwhile; no picker from the desk's button without a manuscript.
* Browse: one shelf button; its picker carries both modes and the switch between them (the switch
  itself: tests/render_smoke/test_shelf_picker_render_smoke.py).
  Nothing shown: the first ticked opens in Browse and the others begin a desk with it; one alone
  just opens.
* Puzzle: each through ``_add_fragment_by_sys_id`` with the canvas check, up to 30 on the canvas;
  taken only once the canvas says the image drew (an image that failed stays chosen); nothing
  once the canvas was replaced.
* The pending choice is kept per purpose and per account (S2's store).
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from nicegui import events, ui

from tests.render_smoke import test_shelf_pick_hosts_render_smoke as hosts
from web.components import shelf_pick

_by_testid, _click, _elements, _until = hosts._by_testid, hosts._click, hosts._elements, hosts._until


@pytest.fixture
def picker(monkeypatch):
    stand_in = hosts.Picker()
    monkeypatch.setattr(shelf_pick, 'open_shelf_picker', stand_in)
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: hosts.TREE)
    return stand_in


@pytest.fixture
def no_tree(monkeypatch):
    monkeypatch.setattr(shelf_pick, 'open_shelf_picker', hosts.Picker())
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: None)


def _entry(sys_id, shelfmark=None):
    return {'sys_id': sys_id, 'shelfmark': shelfmark or f'Frag {sys_id[-3:]}', 'placement_id': f'inv:1:{sys_id}'}


async def _confirm(user, stand_in, entries, label='chosen'):
    with user._client:
        return await stand_in.opened[-1]['on_confirm'](entries, label)


def _back_button(user):
    return [e for e in _elements(user) if isinstance(e, ui.button) and getattr(e, 'text', '') == 'Back to Page View']


# -- Reading Desk -----------------------------------------------------------------------------

def test_the_desk_takes_the_choice_through_its_add(tmp_path, monkeypatch, picker):
    from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS, SID_A, SID_B, SID_C
    seen = []

    async def driver(user, enter, desks):
        enter(FRAGMENTS, None)
        await _until(lambda: _by_testid(user, 'rd-shelf-pick'))
        _click(user, _by_testid(user, 'rd-shelf-pick')[0])
        opened = picker.opened[-1]
        seen.append((opened['mode'], opened['purpose'], opened['confirm_label']))
        took = await _confirm(user, picker, [_entry(SID_A, 'Frag A'), _entry(SID_C, 'Frag C')])
        assert took == [SID_A, SID_C]                     # A was on the desk: taken, not added twice
        await _until(lambda: desks and desks[-1] == [SID_A, SID_B, SID_C])
        assert '1 added to the Reading Desk' in user.notify.messages

    hosts._desk_run(tmp_path, monkeypatch, driver)
    assert seen == [('multi', 'reading_desk', 'Add them all to the Reading Desk')]


def _pages(monkeypatch, pages):
    """The Reading Desk tests' service, answering each manuscript's page count from ``pages``."""
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    real = rd._service

    def with_pages():
        svc = real()
        svc.page_count.side_effect = lambda sid: pages.get(sid, 1)
        return svc
    monkeypatch.setattr(rd, '_service', with_pages)


def test_the_desk_adds_up_to_thirty_manuscripts_with_what_it_holds(tmp_path, monkeypatch, picker):
    # owner, 2026-11-21: limited by its limit, not by 12 at a time; the desk's own count included
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    extra = [f'990000000000000{n:03d}' for n in range(900, 940)]
    for sid in extra:
        monkeypatch.setitem(rd.SHELFMARKS, sid, f'Frag {sid[-2:]}')
    _pages(monkeypatch, {})

    async def driver(user, enter, desks):
        enter(rd.FRAGMENTS, None)
        await _until(lambda: _by_testid(user, 'rd-shelf-pick'))
        _click(user, _by_testid(user, 'rd-shelf-pick')[0])
        assert picker.opened[-1]['limit'].max_items == 30
        took = await _confirm(user, picker, [_entry(rd.SID_A, 'Frag A')] + [_entry(s) for s in extra])
        assert took == [rd.SID_A] + extra[:28]            # the one there, then 28 new: 30 on the desk
        assert desks[-1] == [rd.SID_A, rd.SID_B] + extra[:28]
        assert 'Up to 30 manuscripts here' in user.notify.messages

    hosts._desk_run(tmp_path, monkeypatch, driver)


def test_the_desk_adds_up_to_a_thousand_pages_with_what_it_holds(tmp_path, monkeypatch, picker):
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    extra = [f'990000000000000{n:03d}' for n in range(900, 905)]
    for sid in extra:
        monkeypatch.setitem(rd.SHELFMARKS, sid, f'Frag {sid[-2:]}')
    _pages(monkeypatch, {rd.SID_A: 100, rd.SID_B: 100, **{s: 300 for s in extra}})

    async def driver(user, enter, desks):
        enter(rd.FRAGMENTS, None)
        await _until(lambda: _by_testid(user, 'rd-shelf-pick'))
        _click(user, _by_testid(user, 'rd-shelf-pick')[0])
        took = await _confirm(user, picker, [_entry(s) for s in extra])
        assert took == extra[:2]                          # 200 on the desk + 600; the third would make 1,100
        assert desks[-1] == [rd.SID_A, rd.SID_B] + extra[:2]
        assert 'The Reading Desk holds up to 1,000 pages; these would make 1,100' in user.notify.messages

    hosts._desk_run(tmp_path, monkeypatch, driver)


def test_one_that_fails_to_load_leaves_its_room_to_the_next(tmp_path, monkeypatch, picker):
    # Codex on #88: room for one more; the first chosen does not load, so the second takes its room
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    broken, second, third = [f'990000000000000{n:03d}' for n in range(900, 903)]
    for sid in (broken, second, third):
        monkeypatch.setitem(rd.SHELFMARKS, sid, f'Frag {sid[-2:]}')
    _pages(monkeypatch, {rd.SID_A: 100, rd.SID_B: 100, broken: 700, second: 700, third: 700})
    real = rd._service

    def failing():
        svc = real()
        loads = svc.get_full_manuscript.side_effect

        def get_full_manuscript(sys_id):
            if sys_id == broken:
                raise OSError('not loaded')
            return loads(sys_id)
        svc.get_full_manuscript.side_effect = get_full_manuscript
        return svc
    monkeypatch.setattr(rd, '_service', failing)

    async def driver(user, enter, desks):
        enter(rd.FRAGMENTS, None)
        await _until(lambda: _by_testid(user, 'rd-shelf-pick'))
        _click(user, _by_testid(user, 'rd-shelf-pick')[0])
        took = await _confirm(user, picker, [_entry(s) for s in (broken, second, third)])
        assert took == [second]                           # the one that failed stays chosen
        assert desks[-1] == [rd.SID_A, rd.SID_B, second]
        assert '1 added to the Reading Desk' in user.notify.messages
        # the third would make 200 + 700 + 700: the limit is told as the desk now is
        assert 'The Reading Desk holds up to 1,000 pages; these would make 1,600' in user.notify.messages

    hosts._desk_run(tmp_path, monkeypatch, driver)


def test_a_desk_left_while_the_picker_was_open_takes_none(tmp_path, monkeypatch, picker):
    from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS, SID_C

    async def driver(user, enter, desks):
        enter(FRAGMENTS, None)
        await _until(lambda: _by_testid(user, 'rd-shelf-pick'))
        _click(user, _by_testid(user, 'rd-shelf-pick')[0])
        _click(user, _back_button(user)[0])
        await asyncio.sleep(0.3)
        seen = len(desks)
        assert await _confirm(user, picker, [_entry(SID_C, 'Frag C')]) == []
        await asyncio.sleep(0.3)
        assert not any(d and SID_C in d for d in desks[seen:])

    hosts._desk_run(tmp_path, monkeypatch, driver)


@pytest.fixture
def browse_picker(monkeypatch):
    """Browse's own picker (its lookup row and place line), replaced by the stand-in."""
    from web.pages import browse
    stand_in = hosts.Picker()
    monkeypatch.setattr(browse, 'open_shelf_picker', stand_in)
    monkeypatch.setattr(browse, 'shelf_tree_service', lambda: hosts.TREE)
    return stand_in


def test_browse_has_one_shelf_button_whose_switch_opens_several_in_the_desk(tmp_path, monkeypatch,
                                                                           browse_picker):
    # A manuscript shown, no desk: the switch turns the picker into the multiple choice where it is,
    # and the choice begins a desk with the manuscript shown, as "Add to Reading Desk" begins one.
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_A, SID_B, SID_C

    async def driver(user, enter, desks):
        await _until(lambda: _by_testid(user, 'shelf-picker-open'))
        assert _by_testid(user, 'browse-desk-shelf-pick') == []           # no second button (DX16)
        _click(user, _by_testid(user, 'shelf-picker-open')[0])
        [one] = browse_picker.opened                       # one dialog, both modes (the switch in it)
        assert one['on_pick'] is not None and one.get('mode') is None      # opens as the single choice
        assert (one['switch_label'], one['purpose'], one['confirm_label']) == (
            'Open more than one', 'reading_desk', 'Open them in the Reading Desk')
        # "Open", with Add to List and Add to Puzzle beside it (DX21, DX22); a tick is limited by the
        # choice alone, not by what the desk holds (the manuscript shown is not counted): the desk's
        # room is applied by "Open them in the Reading Desk" (Codex on the round-3 design)
        assert (one['pick_label'], one['add_to']) == ('Open', True)
        assert (one['limit'].max_items, one['limit'].max_pages, one['limit'].base()) == (30, 1000, frozenset())
        took = await _confirm(user, browse_picker, [_entry(SID_B, 'Frag B'), _entry(SID_C, 'Frag C')])
        assert took == [SID_B, SID_C]
        await _until(lambda: desks and desks[-1] == [SID_A, SID_B, SID_C])
        assert _back_button(user)                         # the desk is drawn

    hosts._desk_run(tmp_path, monkeypatch, driver)


def test_the_desk_buttons_are_absent_without_the_tree(tmp_path, monkeypatch, no_tree):
    from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS

    async def driver(user, enter, desks):
        enter(FRAGMENTS, None)
        await _until(lambda: _back_button(user))
        assert _by_testid(user, 'rd-shelf-pick') == []

    hosts._desk_run(tmp_path, monkeypatch, driver)


def _empty_browse_run(tmp_path, monkeypatch, driver, url='/browse', before=None):
    """/browse with nothing open, over the Reading Desk tests' manuscripts. ``driver(user, desks)``.
    ``before(user)``: run on the home page first (what another page left for Browse)."""
    from unittest.mock import patch
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    from web.pages import browse
    desks = []
    real = browse.persist_browse_snapshot

    def persist(state, page=None):
        desks.append([e.get('sys_id') for e in state.reading_desk_entries] if state.view_joined else None)
        return real(state, page)

    async def run(user):
        if before is not None:
            await user.open('/')
            await before(user)
        await user.open(url)
        await driver(user, desks)

    crossref = MagicMock()
    crossref.is_available.return_value = False
    with rd.sidecar(tmp_path), \
            patch.object(browse, 'persist_browse_snapshot', persist), \
            patch.object(browse, 'get_service', return_value=rd._service()), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        rd._run_home_smoke(run, lang='en')


def test_nothing_shown_the_first_ticked_opens_and_the_others_begin_a_desk(tmp_path, monkeypatch,
                                                                          browse_picker):
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_B, SID_C

    async def driver(user, desks):
        await _until(lambda: _by_testid(user, 'shelf-picker-open'))
        _click(user, _by_testid(user, 'shelf-picker-open')[0])
        assert browse_picker.opened                                       # the picker opens (no refusal)
        took = await _confirm(user, browse_picker, [_entry(SID_B, 'Frag B'), _entry(SID_C, 'Frag C')])
        assert took == [SID_B, SID_C]
        await _until(lambda: desks and desks[-1] == [SID_B, SID_C])
        assert _back_button(user)
        assert 'Please load a manuscript first' not in user.notify.messages

    _empty_browse_run(tmp_path, monkeypatch, driver)


def test_nothing_shown_the_first_counts_in_the_limit(tmp_path, monkeypatch, browse_picker):
    # Codex on #77: the manuscript opened is one of this confirm's; the rest stay ticked
    from tests.render_smoke import test_reading_desk_document_render_smoke as rd
    extra = [f'990000000000000{n:03d}' for n in range(900, 940)]
    for sid in extra:
        monkeypatch.setitem(rd.SHELFMARKS, sid, f'Frag {sid[-2:]}')
    _pages(monkeypatch, {})

    async def driver(user, desks):
        await _until(lambda: _by_testid(user, 'shelf-picker-open'))
        _click(user, _by_testid(user, 'shelf-picker-open')[0])
        took = await _confirm(user, browse_picker, [_entry(s) for s in extra])
        assert took == extra[:30]
        await _until(lambda: desks and desks[-1] == extra[:30])
        assert 'Up to 30 manuscripts here' in user.notify.messages

    _empty_browse_run(tmp_path, monkeypatch, driver)


def test_nothing_shown_a_first_past_the_pages_begins_no_desk(tmp_path, monkeypatch, browse_picker):
    # Codex on #82: the first ticked (1,001 pages) opens here; nothing fits beside it, so no desk of
    # one past the limit is begun, and the rest stay ticked with the limit named
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_B, SID_C
    _pages(monkeypatch, {SID_B: 1001, SID_C: 5})

    async def driver(user, desks):
        await _until(lambda: _by_testid(user, 'shelf-picker-open'))
        _click(user, _by_testid(user, 'shelf-picker-open')[0])
        took = await _confirm(user, browse_picker, [_entry(SID_B, 'Frag B'), _entry(SID_C, 'Frag C')])
        assert took == [SID_B]
        assert not any(desks) and not _back_button(user)
        assert 'The Reading Desk holds up to 1,000 pages; these would make 1,006' in user.notify.messages

    _empty_browse_run(tmp_path, monkeypatch, driver)


def test_nothing_shown_one_ticked_just_opens(tmp_path, monkeypatch, browse_picker):
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_C

    def shown(user):
        return any('automatic Frag C p1' in str(getattr(e, 'content', '') or getattr(e, 'text', ''))
                   for e in _elements(user))

    async def driver(user, desks):
        await _until(lambda: _by_testid(user, 'shelf-picker-open'))
        _click(user, _by_testid(user, 'shelf-picker-open')[0])
        assert await _confirm(user, browse_picker, [_entry(SID_C, 'Frag C')]) == [SID_C]
        await _until(lambda: shown(user))                                 # opened here
        assert not any(desks) and not _back_button(user)                  # and no desk

    _empty_browse_run(tmp_path, monkeypatch, driver)


def test_several_from_the_header_or_the_home_page_open_in_a_new_desk(tmp_path, monkeypatch):
    # The header's and the home page's picker with "Open more than one" (web/reading_desk_handoff.py):
    # Browse opened at the first takes the list kept for it, opens the first and begins a desk with
    # the others, as its own picker does
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_B, SID_C
    from web.reading_desk_handoff import stage_for_reading_desk

    async def leave(user):
        with user._client:
            assert stage_for_reading_desk([_entry(SID_B, 'Frag B'), _entry(SID_C, 'Frag C')])

    async def driver(user, desks):
        await _until(lambda: desks and desks[-1] == [SID_B, SID_C])
        assert _back_button(user)

    _empty_browse_run(tmp_path, monkeypatch, driver, url=f'/browse?sys_id={SID_B}&desk=shelf', before=leave)


def test_without_the_list_the_address_opens_its_manuscript(tmp_path, monkeypatch):
    # a reload, another tab took it, or Browse opened at another manuscript: no desk, the address's own
    from tests.render_smoke.test_reading_desk_document_render_smoke import SID_B, SID_C
    from web.reading_desk_handoff import stage_for_reading_desk

    async def leave(user):
        with user._client:
            assert stage_for_reading_desk([_entry(SID_B, 'Frag B'), _entry(SID_C, 'Frag C')])

    def shown(user):
        return any('automatic Frag C p1' in str(getattr(e, 'content', '') or getattr(e, 'text', ''))
                   for e in _elements(user))

    async def driver(user, desks):
        await _until(lambda: shown(user))
        await asyncio.sleep(0.5)
        assert not any(desks) and not _back_button(user)

    _empty_browse_run(tmp_path, monkeypatch, driver, url=f'/browse?sys_id={SID_C}&desk=shelf', before=leave)


# -- Fragment Puzzle --------------------------------------------------------------------------

def _queued(sys_id, *_args, **_kwargs):
    """What _add_fragment_by_sys_id answers once it sent the image: the fragment's key."""
    return f'{sys_id},1r'


def test_the_puzzle_adds_the_choice_as_the_typed_box_does(picker, monkeypatch):
    from tests.render_smoke.test_puzzle_autosave_render_smoke import _fire
    from tests.render_smoke.test_puzzle_export_render_smoke import _run
    from web.pages import puzzle
    added = AsyncMock(side_effect=_queued)
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', added)
    monkeypatch.setattr(puzzle, 'PUZZLE_CONFIRM_WAIT', 5.0)
    meta = MagicMock()
    meta.resolve_system_by_shelfmark.return_value = {'sys_id': '990000000000000888'}
    monkeypatch.setattr(puzzle.state, 'meta_mgr', meta)
    chosen = [_entry(f'990000000000000{n:03d}') for n in range(700, 732)]

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        [button] = _by_testid(user, 'puzzle-shelf-pick')
        _click(user, button)
        opened = picker.opened[-1]
        assert (opened['mode'], opened['purpose'], opened['limit'].max_items) == ('multi', 'puzzle', 30)
        with user._client:
            confirm = asyncio.ensure_future(opened['on_confirm'](chosen, 'chosen'))
        await _until(lambda: added.await_count == 30)
        # the canvas answers: one image failed, the rest drew
        for n, entry in enumerate(chosen[:30]):
            _fire(user, 'puzzle-add-result', {'key': f"{entry['sys_id']},1r", 'success': n != 3})
        took = await confirm
        assert took == [e['sys_id'] for n, e in enumerate(chosen[:30]) if n != 3]   # the failed one stays chosen
        assert [c.args[0] for c in added.await_args_list] == [e['sys_id'] for e in chosen[:30]]
        assert all(c.kwargs['still_wanted']() for c in added.await_args_list)
        assert 'Up to 30 fragments here' in user.notify.messages

        # the typed box reaches the same function
        [box] = [e for e in _elements(user) if isinstance(e, ui.input)
                 and e.props.get('placeholder') == 'Enter shelfmark...']
        with user._client:
            box.value = 'T-S 8.8'
            for listener in list(box._event_listeners.values()):
                if listener.type == 'keydown.enter':
                    events.handle_event(listener.handler, events.GenericEventArguments(
                        sender=box, client=user._client, args=None))
        await _until(lambda: added.await_count == 31)
        assert added.await_args.args[0] == '990000000000000888'

    _run(driver)


def test_the_puzzle_keeps_what_it_could_not_add(picker, monkeypatch):
    # No images (nothing sent) and an add that raised stay chosen; an image the canvas has not
    # answered for in time was sent and counts as added.
    from tests.render_smoke.test_puzzle_export_render_smoke import _run
    from web.pages import puzzle
    ids = ['990000000000000701', '990000000000000702', '990000000000000703']

    async def add(sys_id, *_a, **_k):
        if sys_id == ids[0]:
            return None                                   # no images
        if sys_id == ids[1]:
            raise RuntimeError('lookup failed')
        return _queued(sys_id)

    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', AsyncMock(side_effect=add))
    monkeypatch.setattr(puzzle, 'PUZZLE_CONFIRM_WAIT', 0.3)

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        _click(user, _by_testid(user, 'puzzle-shelf-pick')[0])
        assert await _confirm(user, picker, [_entry(s) for s in ids]) == [ids[2]]

    _run(driver)


def test_the_puzzle_counts_fragments_not_manuscripts(picker, monkeypatch):
    # Codex on #82: two folios of one manuscript are two fragments on the canvas. Each add here puts
    # two on its way, so 15 manuscripts fill the 30 and the rest stay chosen.
    from tests.render_smoke.test_puzzle_export_render_smoke import _run
    from web.pages import puzzle
    ids = [f'990000000000000{n:03d}' for n in range(700, 720)]

    async def add(sys_id, shelfmark, puzzle_meta, pending, *_a, **_k):
        pending[f'{sys_id},1r'] = {'sys_id': sys_id}
        pending[f'{sys_id},2r'] = {'sys_id': sys_id}
        return f'{sys_id},1r'

    added = AsyncMock(side_effect=add)
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', added)
    monkeypatch.setattr(puzzle, 'PUZZLE_CONFIRM_WAIT', 0.3)

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        _click(user, _by_testid(user, 'puzzle-shelf-pick')[0])
        took = await _confirm(user, picker, [_entry(s) for s in ids])
        assert took == ids[:15]
        assert added.await_count == 15
        assert 'Up to 30 fragments here' in user.notify.messages

    _run(driver)


def test_the_puzzle_asks_for_room_again_after_the_folio_lookup(picker, monkeypatch):
    # Codex on #82: typed adds filled the canvas while the picker's fragment was looked up; it is not
    # added (the add asks still_wanted after the lookup, with no wait before recording it)
    from tests.render_smoke.test_puzzle_export_render_smoke import _run
    from web.pages import puzzle
    ids = ['990000000000000701', '990000000000000702', '990000000000000703']

    async def add(sys_id, shelfmark, puzzle_meta, pending, *_a, still_wanted=None, **_k):
        for n in range(30):                       # typed adds landing during the lookup
            pending.setdefault(f'typed-{n}', {'sys_id': f'typed-{n}'})
        if still_wanted is not None and not still_wanted():
            return None
        pending[f'{sys_id},1r'] = {'sys_id': sys_id}
        return f'{sys_id},1r'

    added = AsyncMock(side_effect=add)
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', added)
    monkeypatch.setattr(puzzle, 'PUZZLE_CONFIRM_WAIT', 0.3)

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        _click(user, _by_testid(user, 'puzzle-shelf-pick')[0])
        assert await _confirm(user, picker, [_entry(s) for s in ids]) == []
        assert added.await_count == 1                     # the rest were not even looked up
        assert 'Up to 30 fragments here' in user.notify.messages

    _run(driver)


def test_the_puzzle_takes_nothing_for_a_canvas_replaced_meanwhile(picker, monkeypatch):
    from tests.render_smoke.test_puzzle_export_render_smoke import _click_button, _icon, _run
    from web.pages import puzzle
    added = AsyncMock(side_effect=_queued)
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', added)

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        _click(user, _by_testid(user, 'puzzle-shelf-pick')[0])
        _click_button(user, _icon('note_add'))          # New Puzzle while the picker is open
        await asyncio.sleep(0.3)
        assert await _confirm(user, picker, [_entry('990000000000000701')]) == []
        assert added.await_count == 0

    _run(driver)


def test_a_puzzle_replaced_after_some_images_drew_takes_none(picker, monkeypatch):
    # Codex review of #71: one image drew, then the reader began another puzzle before the second
    # answered. The canvas shown holds neither: both stay chosen.
    from tests.render_smoke.test_puzzle_autosave_render_smoke import _fire
    from tests.render_smoke.test_puzzle_export_render_smoke import _click_button, _icon, _run
    from web.pages import puzzle
    added = AsyncMock(side_effect=_queued)
    monkeypatch.setattr(puzzle, '_add_fragment_by_sys_id', added)
    monkeypatch.setattr(puzzle, 'PUZZLE_CONFIRM_WAIT', 1.0)
    ids = ['990000000000000701', '990000000000000702']

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        _click(user, _by_testid(user, 'puzzle-shelf-pick')[0])
        with user._client:
            confirm = asyncio.ensure_future(picker.opened[-1]['on_confirm']([_entry(s) for s in ids], 'chosen'))
        await _until(lambda: added.await_count == 2)
        _fire(user, 'puzzle-add-result', {'key': f'{ids[0]},1r', 'success': True})
        _click_button(user, _icon('note_add'))          # New Puzzle before the second answers
        await asyncio.sleep(0.2)
        assert await confirm == []

    _run(driver)


def test_the_puzzle_has_no_button_without_the_tree(no_tree):
    from tests.render_smoke.test_puzzle_export_render_smoke import _run

    async def driver(user):
        hosts._puzzle_page(user)
        await user.open('/puzzle')
        assert _by_testid(user, 'puzzle-shelf-pick') == []

    _run(driver)


# -- The pending choice, per purpose and per account ------------------------------------------

def test_a_pending_choice_is_kept_per_purpose_and_per_account(monkeypatch):
    from types import SimpleNamespace

    from shared.shelfmark_tree_selection import Selection
    from web import shelf_selection_store as store
    kept = {}
    monkeypatch.setattr(store, 'safe_user_get', lambda key: kept.get(key))
    monkeypatch.setattr(store, 'safe_user_set', lambda key, value: kept.__setitem__(key, value) or True)
    monkeypatch.setattr(store, 'safe_user_remove', lambda key: kept.pop(key, None) is None or True)   # #70: removal answers success
    chosen = Selection()
    leaf = SimpleNamespace(id='inv:1:990000000000000701', sys_id='990000000000000701',
                           label='T-S 1.1', shelfmark='T-S 1.1')
    assert chosen.add_leaf(leaf, lambda _chip: set())
    for purpose in ('reading_desk', 'puzzle', 'lists'):
        assert store.save_selection(purpose, 'user-a', chosen)
    assert len(store.load_selection('reading_desk', 'user-a')) == 1
    assert len(store.load_selection('reading_desk', 'user-b')) == 0      # another account
    assert len(store.load_selection('reading_desk', 'anon')) == 0        # signed out
    store.clear_selection('puzzle', 'user-a')
    assert len(store.load_selection('puzzle', 'user-a')) == 0
    assert len(store.load_selection('lists', 'user-a')) == 1             # its own purpose
