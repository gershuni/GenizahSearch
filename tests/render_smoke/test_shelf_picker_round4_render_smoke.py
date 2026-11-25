# -*- coding: utf-8 -*-
"""The web shelf picker, round 4, on test pages in a NiceGUI User simulation with the fixture tree of
tests/shelfmark_tree_fixture.py.

* The pane is labelled fields: the shelfmark (and its other shelfmarks), the title, a box with every
  page's text as stored, the field lines (``field_lines``: a bold label, its value plain), the joins
  and the links.
* A join with a place on the shelf is a link: the dialog goes to that shelfmark, selects it and the
  pane shows it. Its box chooses it as its box in the list would, turning the multiple choice on in
  a picker that has it; unticked, it is no longer chosen. A partner with no id or no place is its
  shelfmark alone. Ticking a join ticks the shelfmark the pane shows too (round 5, as on the desktop).
* The rows' signs (PGP, a scholarly transcription, a join), read once off the loop: the rows are
  drawn without them, then again with them; the tooltip names them.
"""
from __future__ import annotations

import asyncio
import threading
import time

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from shared.shelfmark_tree_details import field_lines
from shared.shelfmark_tree_service import ShelfmarkTreeService
from shared.shelfmark_tree_signs import SIGN_LABELS
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _get_page, _run
from tests.shelfmark_tree_fixture import SYS, build_fixture

PICKED: list = []
SERVICE: dict = {}
CONFIRMED: list = []
LABEL: dict = {}
LEAF: dict = {}


async def _on_confirm(entries, label):
    CONFIRMED.append([e['sys_id'] for e in entries])
    return None


@ui.page('/_test/shelf-round4-open')
def _open_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, pick_label='Open')


@ui.page('/_test/shelf-round4-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, purpose='test_round4_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open')


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


def _leaf(user, sys_id):
    return next(e for e in _els(user, 'data-leaf') if e.props['data-sys-id'] == sys_id)


def _has_leaf(user, sys_id) -> bool:
    return any(e.props['data-sys-id'] == sys_id for e in _els(user, 'data-leaf'))


def _pane_shelfmark(user) -> str:
    found = _testid(user, 'shelf-details-shelfmark')
    return found[0].text if found else ''


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
        LEAF[sid] = leaf
    return svc


HERE, NEAR, FAR = SYS['as2'][6], SYS['as2'][2], SYS['as10'][0]
NOWHERE = '990000000000999'               # an id the shelf does not hold


def _details_of(joins_for_here):
    """A stand-in for ``item_details``: the shelfmark as the picker shows it, and for HERE all of a
    manuscript's fields."""
    def details(service, leaf, lang='en', **_kw):
        out = {'sys_id': leaf.sys_id, 'shelfmark': LABEL.get(leaf.sys_id, leaf.label), 'others': [], 'part': '',
               'title': '', 'subjects': [], 'date': '', 'pages': None, 'preview': '', 'text': [],
               'transcriptions': [], 'joins': [], 'comments': None, 'catalogue': None, 'bibliography': None,
               'links': []}
        if leaf.sys_id == HERE:
            out.update({'others': ['Or. 1081 J 1'], 'title': 'A letter of the Gaon',
                        'pages': 2, 'text': [{'page': 1, 'text': 'first line\n  second line\nthird line'},
                                             {'page': 2, 'text': 'שורה ראשונה\nשורה שנייה'}],
                        'transcriptions': ['pgp'], 'catalogue': 3, 'bibliography': 5,
                        'joins': [dict(j, sources=list(j['sources'])) for j in joins_for_here],
                        'links': [{'key': 'ktiv', 'label': 'Ktiv', 'url': 'https://www.nli.org.il/x'},
                                  {'key': 'cudl', 'label': 'Cambridge Digital Library',
                                   'url': 'https://cudl.lib.cam.ac.uk/view/x'}]})
        return out
    return details


JOINS = [{'sys_id': NEAR, 'shelfmark': 'T-S AS 2.3', 'sources': ['pgp']},
         {'sys_id': FAR, 'shelfmark': 'T-S AS 10.1', 'sources': ['fjms', 'community']},
         {'sys_id': '', 'shelfmark': 'ENA 9.9', 'sources': ['community']},
         {'sys_id': NOWHERE, 'shelfmark': 'Nowhere 1', 'sources': ['pgp']}]


def _join_rows(user) -> dict:
    """Each join row by its partner: (a box?, a link?, its text)."""
    out = {}
    for row in _els(user, 'data-join'):
        kids = list(row.descendants())
        out[row.props['data-join']] = (any('data-join-box' in k.props for k in kids),
                                       any(isinstance(k, ui.link) for k in kids),
                                       [k.text for k in kids if isinstance(k, (ui.label, ui.link))])
    return out


# -- the pane as labelled fields ------------------------------------------------------------------

def test_the_pane_is_labelled_fields_in_their_order(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    monkeypatch.setattr(picker, '_community', lambda sys_id: ([], 7))         # its comments, counted last
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _testid(user, 'shelf-details-text'))
            assert await _until(lambda: any(e.props.get('data-fact') == 'comments' for e in _els(user, 'data-fact')))
            [pane] = _testid(user, 'shelf-details')
            seen['fields'] = [d.props['data-field'] for d in pane.descendants() if 'data-field' in d.props]
            [box] = _testid(user, 'shelf-details-text')
            seen['box'] = ([d.text for d in box.descendants() if isinstance(d, ui.label)],
                           [d.props.get('dir') for d in box.descendants() if 'stp-d-pagetext' in d.classes],
                           box.props.get('tabindex'), box.props.get('aria-label'))
            seen['lines'] = [(e.props['data-fact'], e.content) for e in _els(user, 'data-fact')]
            seen['links'] = [(d.props.get('data-link'), d.text) for d in _testid(user, 'shelf-details-links')[0].descendants()
                             if isinstance(d, ui.link)]
            seen['no_preview'] = _testid(user, 'shelf-details-preview') == []
            seen['live'] = pane.props.get('aria-live')
            with user.client:                                  # in the page's language
                from web.translations import tr
                expected = _details_of(JOINS)(None, LEAF[HERE])
                expected['comments'] = 7
                seen['want'] = [(key, ' · '.join(f'<b>{label}</b>: {value}' for label, value in pairs))
                                for key, pairs in field_lines(expected, tr)]

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['fields'] == ['sm', 'others', 'title', 'text', 'joins', 'links']
    texts, dirs, tabindex, label = seen['box']
    # every page's text as stored, each under its number: its line breaks and its spaces kept
    assert texts == ['Page 1', 'first line\n  second line\nthird line', 'Page 2', 'שורה ראשונה\nשורה שנייה']
    assert dirs == ['auto', 'auto'] and tabindex == '0' and label == 'Text'
    # the field lines as field_lines has them, with the comments counted by the community's read
    want = seen['want']
    assert seen['lines'] == want and any(key == 'comments' for key, _ in want)
    assert seen['links'] == [('ktiv', 'Ktiv'), ('cudl', 'Cambridge Digital Library')]
    assert seen['no_preview'] and seen['live'] is None         # the whole text is not read out at each click


# -- joins ----------------------------------------------------------------------------------------

def test_a_joins_link_takes_the_dialog_to_that_shelfmark(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: len(_els(user, 'data-join')) == 4)
            seen['rows'] = _join_rows(user)
            seen['far_shown_before'] = _has_leaf(user, FAR)
            link = next(k for row in _els(user, 'data-join') if row.props['data-join'] == FAR
                        for k in row.descendants() if isinstance(k, ui.link))
            UserInteraction(user, {link}, None).trigger('click')     # its handler (no address to go to)
            assert await _until(lambda: _pane_shelfmark(user) == LABEL[FAR])
            assert await _until(lambda: _has_leaf(user, FAR))
            seen['far'] = _leaf(user, FAR).props.get('aria-selected')
            seen['here_shown'] = _has_leaf(user, HERE)
            seen['picked'] = list(PICKED)
            # selected: the foot's button opens it
            UserInteraction(user, {_testid(user, 'shelf-picker-confirm')[0]}, None).click()
            assert await _until(lambda: PICKED)
            seen['opened'] = [p['sys_id'] for p in PICKED]

    try:
        _run(go)
    finally:
        svc.close()
    rows = seen['rows']
    # no multiple choice in this picker: no boxes; a link only where the shelf has the partner
    assert rows[NEAR] == (False, True, ['T-S AS 2.3', 'PGP'])
    assert rows[FAR] == (False, True, ['T-S AS 10.1', 'FJMS, Community'])
    assert rows['ENA 9.9'] == (False, False, ['ENA 9.9', 'Community'])
    assert rows[NOWHERE] == (False, False, ['Nowhere 1', 'PGP'])
    assert seen['far_shown_before'] is False
    assert seen['far'] == 'true' and seen['here_shown'] is False and seen['picked'] == []
    assert seen['opened'] == [FAR]


def test_a_join_followed_in_the_multiple_choice_is_the_one_selected_after_the_switch_back(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: len(_els(user, 'data-join')) == 4)
            [switch] = _testid(user, 'shelf-picker-switch')
            with user.client:
                switch.value = True                                    # the multiple choice
            assert await _until(lambda: _leaf(user, HERE).props.get('role') == 'checkbox')
            assert await _until(lambda: len(_els(user, 'data-join')) == 4)
            link = next(k for row in _els(user, 'data-join') if row.props['data-join'] == FAR
                        for k in row.descendants() if isinstance(k, ui.link))
            UserInteraction(user, {link}, None).trigger('click')
            assert await _until(lambda: _pane_shelfmark(user) == LABEL[FAR])
            assert await _until(lambda: _has_leaf(user, FAR))
            with user.client:
                switch.value = False                                   # back to the single choice
            assert await _until(lambda: _leaf(user, FAR).props.get('role') == 'option')
            seen['far'] = _leaf(user, FAR).props.get('aria-selected')
            UserInteraction(user, {_testid(user, 'shelf-picker-confirm')[0]}, None).click()
            assert await _until(lambda: PICKED)
            seen['opened'] = [p['sys_id'] for p in PICKED]

    try:
        _run(go)
    finally:
        svc.close()
    # the row shown selected is the one Open acts on: the join followed, not the one before it
    assert seen['far'] == 'true'
    assert seen['opened'] == [FAR]


def test_a_joins_box_turns_the_multiple_choice_on_and_chooses_it(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    def box_of(user, sys_id):
        return next(e for e in _els(user, 'data-join-box') if e.props['data-join-box'] == sys_id)

    def ticked(user):
        return {e.props['data-sys-id'] for e in _els(user, 'data-leaf') if e.props.get('aria-checked') == 'true'}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: len(_els(user, 'data-join-box')) == 2)
            [switch] = _testid(user, 'shelf-picker-switch')
            seen['boxes'] = sorted(e.props['data-join-box'] for e in _els(user, 'data-join-box'))
            seen['before'] = (switch.value, box_of(user, NEAR).props.get('aria-checked'))
            UserInteraction(user, {box_of(user, NEAR)}, None).click()
            # the multiple choice is on, and the partner is chosen with the shelfmark the pane shows (the
            # fragments of a join together, as on the desktop): its box here and both rows in the list
            assert await _until(lambda: ticked(user) == {HERE, NEAR})
            assert await _until(lambda: box_of(user, NEAR).props.get('aria-checked') == 'true')
            seen['on'] = (switch.value, _leaf(user, NEAR).props.get('role'), _pane_shelfmark(user))
            UserInteraction(user, {box_of(user, FAR)}, None).click()          # one in another volume
            assert await _until(lambda: box_of(user, FAR).props.get('aria-checked') == 'true')
            UserInteraction(user, {box_of(user, NEAR)}, None).click()         # unticked: the partner alone out
            assert await _until(lambda: ticked(user) == {HERE})
            assert await _until(lambda: box_of(user, NEAR).props.get('aria-checked') == 'false')
            UserInteraction(user, {_testid(user, 'shelf-picker-confirm')[0]}, None).click()
            assert await _until(lambda: CONFIRMED)

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['boxes'] == sorted([NEAR, FAR])                            # only partners with a place
    assert seen['before'] == (False, 'false')
    assert seen['on'] == (True, 'checkbox', LABEL[HERE])                    # the pane stays where it was
    assert CONFIRMED == [[HERE, FAR]]


# -- the rows' signs ------------------------------------------------------------------------------

class _Signs:
    """The rows' signs, loaded when the test lets them: off the loop, once."""

    def __init__(self, marks):
        self.marks, self.ready, self.loads, self.go = marks, False, [], threading.Event()

    def load(self):
        try:
            asyncio.get_running_loop()
            self.loads.append('on the loop')
        except RuntimeError:
            self.loads.append('off the loop')
        self.go.wait(5)
        self.ready = True

    def of(self, sys_id):
        return self.marks.get(sys_id, ())


def test_the_rows_signs_come_when_they_are_loaded(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    signs = _Signs({SYS['as2'][5]: ('pgp', 'edition', 'join'), SYS['as2'][4]: ('join',)})
    monkeypatch.setattr(picker, 'shelf_signs', lambda: signs)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    def signs_on(user):
        return {e.parent_slot.parent.props.get('data-sys-id'): e.props['data-signs'] for e in _els(user, 'data-signs')}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: _els(user, 'data-leaf'))
            seen['before'] = signs_on(user)                    # drawn at once, without them
            signs.go.set()
            assert await _until(lambda: signs_on(user))
            seen['after'] = signs_on(user)
            row = _leaf(user, SYS['as2'][5])
            seen['tip'] = [d.text for d in row.descendants() if isinstance(d, ui.tooltip)]
            seen['marks'] = [(d.text if isinstance(d, ui.label) else d.props.get('name'), d.props.get('aria-label'))
                             for d in row.descendants() if d.props.get('role') == 'img']
            seen['selected'] = row.props.get('aria-selected'), _leaf(user, HERE).props.get('aria-selected')

    try:
        _run(go)
    finally:
        signs.go.set()
        svc.close()
    assert seen['before'] == {}
    assert seen['after'] == {SYS['as2'][5]: 'pgp edition join', SYS['as2'][4]: 'join'}
    assert signs.loads == ['off the loop']
    names = [SIGN_LABELS[m] for m in ('pgp', 'edition', 'join')]
    assert seen['tip'] == [LABEL[SYS['as2'][5]] + '\n' + ' · '.join(names)]
    assert seen['marks'] == [('PGP', names[0]), ('description', names[1]), ('link', names[2])]
    assert seen['selected'] == ('false', 'true')               # drawn again, the selection kept


def _classed(user, cls):
    with user.client:
        return [e for e in user.client.elements.values() if cls in e.classes and not e.is_deleted]


def _bar(user):
    """A phone's folded pane: (text, hint?, button shown?)."""
    [bar] = _testid(user, 'shelf-details-bar')
    label = next(d for d in bar.descendants() if 'stp-pbar-sm' in d.classes)
    button = next(d for d in bar.descendants() if 'stp-pbar-btn' in d.classes)
    return label.text, 'stp-hint' in label.classes, button.visible


# From the phone's tests (#106), here so that it uses this module's pages: imported from another test
# module, this one was loaded a second time under another name, and that copy's pages took the place of
# these (its service closed), failing every test here after it in the same run.
def test_a_join_followed_with_the_pane_open_keeps_it_open(tmp_path, monkeypatch):
    """Codex on #106: a join's partner outside the view is reached by drawing the dialog anew, which
    empties (and so folds) the pane on the way; the pane the reader had open stays open on the partner."""
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, 'item_details', _details_of(JOINS))
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-round4-open?start={HERE}')
            await _connect(user)
            assert await _until(lambda: len(_els(user, 'data-join')) == 4)
            [bar] = _testid(user, 'shelf-details-bar')
            button = next(d for d in bar.descendants() if 'stp-pbar-btn' in d.classes)
            [main] = _classed(user, 'stp-main')
            UserInteraction(user, {button}, None).trigger('click')
            assert await _until(lambda: 'stp-pane-open' in main.classes)
            seen['far_in_view'] = _has_leaf(user, FAR)
            link = next(k for row in _els(user, 'data-join') if row.props['data-join'] == FAR
                        for k in row.descendants() if isinstance(k, ui.link))
            UserInteraction(user, {link}, None).trigger('click')
            assert await _until(lambda: _pane_shelfmark(user) == LABEL[FAR])
            seen['after'] = ('stp-pane-open' in main.classes, button.props.get('aria-expanded'), _bar(user))

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['far_in_view'] is False                 # reached by drawing the dialog anew
    assert seen['after'] == (True, 'true', (LABEL[FAR], False, True))
