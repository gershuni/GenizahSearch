# -*- coding: utf-8 -*-
"""Render-smoke: the dictionary lookup in the search page's Quick View (web/components/dictionary_lookup.py).

Drives the real dialog (``open_advanced_dialog``) over the stubs of
test_quick_view_without_automatic_text_render_smoke.py, with a stubbed Blau service, and fires the events the
click script (web/static/js/blau_lookup.js) would dispatch on the popover. Pinned: a served dictionary gives
Quick View one popover, a child of the dialog's card beside the scroll area (not inside it, where Quasar's
``contain: strict`` would clip it), the text pane marked for the look-up key and the reader's switch; a
lookup draws the panel; a lookup still running when the reader moves to the next result, or closes the
dialog, draws nothing; closing the dialog hides a panel that is open. Without a served dictionary Quick View
is as it was: no popover, no marks, no switch. The search page puts the click script in its head from its
build.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from nicegui import ui

from tests.render_smoke.test_blau_panel_render_smoke import _dictionary, _fire, _slow_dictionary
from tests.render_smoke.test_quick_view_without_automatic_text_render_smoke import (
    RESULTS, SLOW_FETCH_DONE, _button, _click, _elements, _run, _wait_until,
)
from web.components import blau_panel
from web.components.dictionary_lookup import DictionaryLookup
from web.pages import search_results
from web.translations import using_language

PAGE = '/_render_smoke/quick-view-dictionary'
_BLAU = {}   # the Blau stub of the running test


def _not_served():
    return SimpleNamespace(is_available=lambda: False, served_to=lambda editor: False, credit='', credit_he='')


@ui.page(PAGE)
def _quick_view_page(case: str = 'hit', served: str = '1'):
    results = [RESULTS[c] for c in case.split(',')]
    blau = _BLAU['service'] if served == '1' else _not_served()
    search_state = SimpleNamespace(
        displayed_results=results, title_translations={}, printed_ids=set(), translation_data={},
        dictionaries=DictionaryLookup(blau, _not_served(), _not_served(), editor=False, capture=MagicMock()))
    with using_language('en'):
        search_results.open_advanced_dialog(search_state, SimpleNamespace(), 0 if len(results) > 1 else None,
                                            results[0])


def _with_class(user, cls):
    return [e for e in _elements(user) if cls in e.classes]


def _popover(user):
    found = _with_class(user, blau_panel.POPOVER_CLASS)
    assert len(found) <= 1, 'one popover per page'
    return found[0] if found else None


def _marked(user, attr):
    return [e for e in _elements(user) if attr in e.props]


def _ancestors(element):
    return list(element.ancestors())


async def _open(user, case='hit', served='1'):
    await user.open(f'{PAGE}?case={case}&served={served}')
    await _wait_until(lambda: _marked(user, blau_panel.KEY_PANE_ATTR) or served != '1')
    await _wait_until(lambda: False, timeout=0.3)


def _shown(popover):
    return popover._style.get('display') == 'flex'


def test_a_served_dictionary_puts_the_panel_in_the_dialog_beside_the_scroll_area():
    _BLAU['service'] = _dictionary()

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        assert popover is not None
        dialog = next(e for e in _elements(user) if isinstance(e, ui.dialog))
        card = popover.parent_slot.parent
        assert isinstance(card, ui.card) and card.parent_slot.parent is dialog, 'a child of the dialog card'
        assert not any(isinstance(a, ui.scroll_area) for a in _ancestors(popover)), 'not inside a scroll area'
        assert _marked(user, blau_panel.KEY_PANE_ATTR), 'the text pane takes the look-up key'
        assert _with_class(user, blau_panel.SWITCH_CLASS), "the reader's switch"
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: _shown(popover))
        assert _shown(popover) and _with_class(user, blau_panel.CARD_CLASS)

    _run(driver)


def test_the_switch_marks_the_quick_view_text():
    _BLAU['service'] = _dictionary()

    async def driver(user):
        await _open(user)
        assert not _marked(user, blau_panel.PANE_ATTR), 'off by default'
        [switch] = _with_class(user, blau_panel.SWITCH_CLASS)
        _click(user, switch)
        await _wait_until(lambda: _marked(user, blau_panel.PANE_ATTR))
        [pane] = _marked(user, blau_panel.PANE_ATTR)
        assert blau_panel.KEY_PANE_ATTR in pane.props, 'the same pane the key works in'

    _run(driver)


def test_a_lookup_running_when_the_reader_moves_to_the_next_result_draws_nothing():
    _BLAU['service'] = _slow_dictionary('slow')

    async def driver(user):
        await _open(user, case='hit,no-edition')
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        [next_result] = _button(user, 'Next')
        _click(user, next_result)
        await _wait_until(lambda: _BLAU['service'].lookup.called)
        await _wait_until(lambda: False, timeout=1.0)    # the slow lookup finishes meanwhile
        assert _popover(user) is popover, 'one popover for the dialog, kept across results'
        assert not _shown(popover), 'the stale lookup opened the panel on another result'

    _run(driver)


def test_a_lookup_answered_while_the_next_result_loads_draws_nothing():
    """Codex on the Quick View PR: the next result's page takes longer to come than the answer, so the
    panel must be stale from the click on Next, not from the new content's drawing."""
    _BLAU['service'] = _slow_dictionary('slow')
    SLOW_FETCH_DONE.clear()   # set by an earlier run of the shared stub

    async def driver(user):
        await _open(user, case='hit,slow-fetch')
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        [next_result] = _button(user, 'Next')
        _click(user, next_result)
        await _wait_until(lambda: _BLAU['service'].lookup.called)
        await _wait_until(lambda: False, timeout=1.5)    # answered; the page (4.5 s) not yet
        assert not SLOW_FETCH_DONE.is_set(), 'the next result came before the answer: no interval tested'
        assert not _shown(popover), 'the answer opened the panel while the next result loaded'

    _run(driver)


def test_a_lookup_answered_after_the_reader_turned_to_another_tab_draws_nothing():
    """Codex on the Quick View PR: on a phone, selected words looked up, then the Image or Info tab:
    a selection leaves no marked word, so the click script sends no close."""
    _BLAU['service'] = _slow_dictionary('slow')

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        [tabs] = [e for e in _elements(user) if isinstance(e, ui.tabs) and 'adv-tabs' in e.classes]
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow', 'how': 'selection'})
        with user._client:
            tabs.set_value('info')
        await _wait_until(lambda: _BLAU['service'].lookup.called)
        await _wait_until(lambda: False, timeout=1.0)
        assert not _shown(popover), 'the answer opened the panel over the Info tab'

    _run(driver)


def test_redrawing_the_text_hides_the_panel_and_stops_a_lookup_in_flight():
    """Codex on #21: the line-number toggle redraws the text without the rest of the dialog; the word
    looked up is gone, so an open panel closes and an answer still coming draws nothing."""
    _BLAU['service'] = _slow_dictionary('slow')

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: _shown(popover))
        [toggle] = _button(user, 'Toggle line numbers')
        _click(user, toggle)
        assert not _shown(popover), 'the panel stayed open over the redrawn text'
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        _click(user, toggle)
        await _wait_until(lambda: False, timeout=1.0)
        assert not _shown(popover), 'an answer drew after the text was redrawn'

    _run(driver)


def test_closing_the_dialog_hides_the_panel_and_stops_a_lookup_in_flight():
    _BLAU['service'] = _slow_dictionary('slow')

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        dialog = next(e for e in _elements(user) if isinstance(e, ui.dialog))
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: _shown(popover))
        with user._client:
            dialog.close()
        assert not _shown(popover), 'the panel closes with the dialog'
        with user._client:
            dialog.open()
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        with user._client:
            dialog.close()
        await _wait_until(lambda: False, timeout=1.0)
        assert not _shown(popover), 'a lookup in flight drew into the closed dialog'

    _run(driver)


def test_without_a_served_dictionary_quick_view_is_as_it_was():
    _BLAU['service'] = _dictionary()

    async def driver(user):
        await _open(user, served='0')
        await _wait_until(lambda: _button(user, 'Copy Text'))
        assert _popover(user) is None
        assert not _marked(user, blau_panel.KEY_PANE_ATTR) and not _marked(user, blau_panel.PANE_ATTR)
        assert not _with_class(user, blau_panel.SWITCH_CLASS)
        assert not _BLAU['service'].lookup.called

    _run(driver)


def test_the_search_page_puts_the_click_script_in_its_head_from_its_build():
    """A script added to a live page does not run: the head line is added when the page is built
    (DictionaryLookup.head adds it only when a dictionary is served)."""
    search = (Path(__file__).resolve().parents[2] / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
    build = search[search.index('def create_search_page'):]
    assert 'search_state.dictionaries.head()' in build
    assert 'search_state.dictionaries = DictionaryLookup(' in build
