# -*- coding: utf-8 -*-
"""The web shelf picker on a phone (owner, 2026-11-23): the list gets the screen.

On test pages in a NiceGUI User simulation with the fixture tree of tests/shelfmark_tree_fixture.py:

* The pane folds to one line under the list on a phone: the shelfmark the pane shows and a Details
  button that opens the pane over the list (in the browser), or the pane's hint while it shows
  none. The line follows the pane: opened at a manuscript, a click on another row, a way back that
  leaves the shown one behind.
* The foot carries ``stp-foot-multi`` in the multiple choice only (a phone draws it on one line, its
  other buttons as their icons), flipped with the switch; those buttons keep their names in
  ``aria-label``.
* The CSS a phone needs is there: every column fills the height (the libraries' own 270px width
  rule was a height there), the filter carries the class the shelfmarks' grid places.

What the CSS does with these is measured in the browser at 375x812 (PR description).
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
LABEL: dict = {}


async def _on_confirm(entries, label):
    return None


@ui.page('/_test/shelf-phone-switch')
def _switch_page(start: str = ''):
    from web.components.shelfmark_tree_picker import open_shelf_picker
    container = ui.column()
    open_shelf_picker(container, SERVICE['svc'], PICKED.append, start=start or None, purpose='test_phone_sw',
                      on_confirm=_on_confirm, confirm_label='Open them', switch_label='Open more than one',
                      pick_label='Open', add_to=True, lists_mgr=lambda: None)


def _els(user, attr):
    with user.client:
        return [e for e in user.client.elements.values() if attr in e.props and not e.is_deleted]


def _classed(user, cls):
    with user.client:
        return [e for e in user.client.elements.values() if cls in e.classes and not e.is_deleted]


def _testid(user, testid):
    return [e for e in _els(user, 'data-testid') if e.props['data-testid'] == testid]


def _bar(user):
    """The folded pane's (text, hint?, button shown?)."""
    [bar] = _testid(user, 'shelf-details-bar')
    label = next(d for d in bar.descendants() if 'stp-pbar-sm' in d.classes)
    button = next(d for d in bar.descendants() if 'stp-pbar-btn' in d.classes)
    return label.text, 'stp-hint' in label.classes, button.visible


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
    for sid in SYS['as2']:
        leaf = svc.path_of(sid)[0].leaf
        LABEL[sid] = leaf.shelfmark or leaf.label
    return svc


HERE, OTHER = SYS['as2'][6], SYS['as2'][2]


def _stub_details(monkeypatch):
    from web.components import shelfmark_tree_picker as picker

    def details(service, leaf, lang='en', **_kw):
        return {'sys_id': leaf.sys_id, 'shelfmark': leaf.shelfmark or leaf.label, 'others': [], 'part': '',
                'title': '', 'subjects': [], 'date': '', 'pages': None, 'preview': '', 'text': [],
                'transcriptions': [], 'joins': [], 'comments': None, 'catalogue': None, 'bibliography': None,
                'links': []}
    monkeypatch.setattr(picker, 'item_details', details)
    monkeypatch.setattr(picker, '_thumbnail_src', lambda sys_id, shelfmark, width=320: '')


def test_the_folded_pane_line_follows_the_pane(tmp_path, monkeypatch):
    _stub_details(monkeypatch)
    svc = _setup(tmp_path)
    seen = {}

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-phone-switch?start={HERE}')
            await _connect(user)
            # opened at a manuscript: the pane shows it, and so does its folded line
            assert await _until(lambda: _pane_shelfmark(user) == LABEL[HERE])
            assert await _until(lambda: _bar(user)[0] == LABEL[HERE])
            seen['opened'] = _bar(user)
            [bar] = _testid(user, 'shelf-details-bar')
            button = next(d for d in bar.descendants() if 'stp-pbar-btn' in d.classes)
            [pane] = _testid(user, 'shelf-details')
            [main] = _classed(user, 'stp-main')
            seen['button'] = (button.props.get('aria-expanded'), button.props.get('aria-controls'), f'c{pane.id}')
            # Details opens the pane over the list, and folds it again
            UserInteraction(user, {button}, None).trigger('click')
            assert await _until(lambda: 'stp-pane-open' in main.classes)
            seen['opened_pane'] = button.props.get('aria-expanded')
            UserInteraction(user, {button}, None).trigger('click')
            assert await _until(lambda: 'stp-pane-open' not in main.classes)
            seen['folded'] = button.props.get('aria-expanded')
            # another row clicked: the line follows
            row = next(e for e in _els(user, 'data-leaf') if e.props.get('data-sys-id') == OTHER)
            UserInteraction(user, {row}, None).trigger('click')
            assert await _until(lambda: _bar(user)[0] == LABEL[OTHER])
            seen['other'] = _bar(user)
            # left behind (back to all the libraries) while open: the pane is empty, the line its hint,
            # no button, and the pane folded, not left over the list (Codex #106)
            UserInteraction(user, {button}, None).trigger('click')
            assert await _until(lambda: 'stp-pane-open' in main.classes)
            [crumb] = [e for e in _classed(user, 'stp-crumb') if isinstance(e, ui.link)]
            UserInteraction(user, {crumb}, None).trigger('click')
            assert await _until(lambda: _bar(user)[1])
            seen['left'] = _bar(user)
            seen['left_open'] = ('stp-pane-open' in main.classes, button.props.get('aria-expanded'))

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['opened'] == (LABEL[HERE], False, True)
    expanded, controls, pane_id = seen['button']
    assert expanded == 'false' and controls == pane_id      # the button names the pane it opens
    assert seen['opened_pane'] == 'true' and seen['folded'] == 'false'
    assert seen['other'] == (LABEL[OTHER], False, True)
    assert seen['left'] == ('Choose a shelfmark to see it here', True, False)
    assert seen['left_open'] == (False, 'false')


def test_the_foot_is_marked_in_the_multiple_choice_only(tmp_path, monkeypatch):
    _stub_details(monkeypatch)
    svc = _setup(tmp_path)
    seen = {}

    def foot_multi(user) -> bool:
        [foot] = _classed(user, 'stp-foot')
        return 'stp-foot-multi' in foot.classes

    async def go():
        async with _app(object()) as user:
            await _get_page(user, f'/_test/shelf-phone-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: bool(_els(user, 'data-leaf')))
            seen['single'] = foot_multi(user)
            [switch] = _testid(user, 'shelf-picker-switch')
            switch.set_value(True)
            assert await _until(lambda: foot_multi(user))
            seen['names'] = sorted((cls, e.props.get('aria-label')) for cls in ('stp-clear', 'stp-save', 'stp-puzzle')
                                   for e in _classed(user, cls))
            with user.client:                       # and a tooltip each (owner, 2026-11-23: none there)
                tips = [t for t in user.client.elements.values() if isinstance(t, ui.tooltip)]
                seen['tips'] = sorted((cls, [t.text for t in tips if t.props.get('target') == f'#{e.html_id}'])
                                      for cls in ('stp-clear', 'stp-save', 'stp-puzzle') for e in _classed(user, cls))
            # opened again: as the reader left it (owner, 2026-11-23)
            await _get_page(user, f'/_test/shelf-phone-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: bool(_els(user, 'data-leaf')))
            [switch] = _testid(user, 'shelf-picker-switch')
            seen['again'] = (switch.value, foot_multi(user))
            switch.set_value(False)
            assert await _until(lambda: not foot_multi(user))
            await _get_page(user, f'/_test/shelf-phone-switch?start={HERE}')
            await _connect(user)
            assert await _until(lambda: bool(_els(user, 'data-leaf')))
            [switch] = _testid(user, 'shelf-picker-switch')
            seen['off_again'] = (switch.value, foot_multi(user))

    try:
        _run(go)
    finally:
        svc.close()
    assert seen['single'] is False
    # a phone shows these three as their icons alone: each keeps its name
    assert seen['names'] == [('stp-clear', 'Clear'), ('stp-puzzle', 'Add to Puzzle'), ('stp-save', 'Add to List')]
    assert seen['tips'] == [('stp-clear', ['Clear']), ('stp-puzzle', ['Add to Puzzle']), ('stp-save', ['Add to List'])]
    assert seen['again'] == (True, True)                # the switch as it was left
    assert seen['off_again'] == (False, False)


def test_the_phone_css_lets_every_column_fill_the_height():
    from web.components.shelfmark_tree_picker import _CSS
    phone = _CSS.split('@media (max-width: 900px)')[1:]
    assert phone, 'the picker has no phone rules'
    # the libraries' width rule (.stp-col.stp-libs, flex: 0 1 270px) is outranked on a phone
    assert any('.stp-col, .stp-col.stp-libs { flex: 1 1 auto;' in block for block in phone)
