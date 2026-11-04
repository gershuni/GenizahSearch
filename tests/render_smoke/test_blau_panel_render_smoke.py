# -*- coding: utf-8 -*-
"""Render-smoke: Blau's dictionary on /browse (docs/plans/BLAU_DICTIONARY_PLAN.md §6.2).

Drives the real ``/browse`` over a stubbed browse service (harness: test_browse_button_icons_render_smoke.py)
and a stubbed dictionary service, and fires the events the click script (web/static/js/blau_lookup.js) would
dispatch on the popover. Pinned: a served dictionary gives the page the popover (hidden) and the reader's
switch, OFF by default (owner, 2026-11-04: the page never guesses the text's language) -- no marked pane, no
hint; the switch marks the pane and shows the hint and the "type a word" button, and the choice holds on the
next page; a lookup draws one card per entry with its senses, the matched one marked, the example entries,
the site links, the close control and the credit line -- the site's Hebrew line in both UI languages, the English
rendering beside it only in the English UI -- and shows the popover, with the word in the box at its top; the
box looks up what is typed; the close hides it; an empty answer says what the dictionary covers and what was looked for; a result without a credit
line is not shown; a service that is not available leaves the page without any of it; a payload without a word
asks the dictionary nothing.
"""
from __future__ import annotations

import json
import re
import time
from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import events, ui

from shared.blau_service import SITE_URL, BlauCard, BlauExample, BlauLookup, BlauSense, empty_lookup
from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE, SID, _all_elements, _click, _run_home_smoke, _service, _wait_until,
)
from web.components import blau_panel
from web.pages import browse

CREDIT_HE = 'יהושע בלאו, מילון לטקסטים ערביים־יהודיים מימי הביניים (שורת קרדיט לבדיקה)'
CREDIT_EN = 'Joshua Blau, A Dictionary of Mediaeval Judaeo-Arabic Texts (a credit line for the test)'
ENTRY_URL = 'https://blau-dict.netlify.app/entry/blau-e-002260'
EXAMPLE_URL = 'https://blau-dict.netlify.app/entry/blau-e-002925'


def _sense(form_id, gloss_he, gloss_en, *, ja_text='', binyan='', matched=False, classical=False):
    return BlauSense(form_id=form_id, ja_text=ja_text, arabic='', binyan=binyan, gloss_he=gloss_he,
                     gloss_en=gloss_en, cross_reference='', see=(), page=587, is_classical=classical,
                     is_reconstructed=False, is_erroneous=False, matched=matched)


def _lookup(word='כתאב'):
    card = BlauCard(
        root_id='blau-e-002260', name='כתב', arabic='كتب', page=587, url=ENTRY_URL,
        candidate='כתאב', matched_key='כתאב', matched_kind='sub_entry', sub_entry='כתאב', via='',
        senses=(_sense('f1', "'מכתב, איגרת'", "'letter'", classical=True),
                _sense('f2', "'מסמך, שטר'", "'document, deed'", ja_text='אלכתאב', binyan='I', matched=True)),
        more_senses=3)
    example = BlauExample(root_id='blau-e-002925', name='וג̇ה', url=EXAMPLE_URL, phrase='... כתאב')
    return BlauLookup(word=word, key='כתאב', candidates=('כתאב', 'תאב'), cards=(card,), more_cards=0,
                      examples=(example,), more_examples=2)


def _dictionary(available=True, credit_he=CREDIT_HE, result=None):
    svc = MagicMock()
    svc.is_available.return_value = available
    svc.credit_he = credit_he
    svc.credit_en = CREDIT_EN
    svc.lookup.side_effect = lambda word: result if result is not None else _lookup(word)
    return svc


def _run(driver, dictionary, lang='en'):
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=dictionary), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(driver, lang=lang)


def _with_class(user, cls):
    return [e for e in _all_elements(user) if cls in e.classes]


def _popover(user):
    found = _with_class(user, blau_panel.POPOVER_CLASS)
    assert len(found) <= 1, 'one popover per page'
    return found[0] if found else None


def _marked_panes(user):
    return [e for e in _all_elements(user) if blau_panel.PANE_ATTR in e.props]


def _texts(user):
    return [e.text for e in _all_elements(user) if isinstance(e, (ui.label, ui.link)) and e.text]


def _links(user):
    return {e.props.get('href') for e in _all_elements(user) if isinstance(e, ui.link)}


def _fire(user, element, event, payload):
    """Dispatch ``event`` on ``element`` the way the browser would after the
    script's ``CustomEvent`` (``detail`` is a JSON string)."""
    args = {'detail': json.dumps(payload)} if payload is not None else {}
    # NiceGUI registers `on('blau-lookup')` under the camel-case name Vue
    # listens with ('blauLookup'); the DOM event the script dispatches keeps
    # its kebab-case name.
    names = {event, re.sub(r'-(\w)', lambda m: m.group(1).upper(), event)}
    fired = 0
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.element_id == element.id and listener.type in names:
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args=args))
                fired += 1
    assert fired == 1, f'{event}: {fired} listeners'


def _enriched(user):
    """Phase B has run: the version chooser is up. `update_content()` rebuilds
    the whole content container then, the popover with it, so an event fired
    at the popover of the first render reaches an element the page has
    already deleted (which the handler ignores, by design)."""
    return any(isinstance(e, ui.button) and e.props.get('icon') == 'history' for e in _all_elements(user))


async def _open(user):
    await user.open(f'/browse?sys_id={SID}')
    await _wait_until(lambda: _enriched(user))
    assert _enriched(user), 'the browse page did not finish its enrichment'


def _switch(user):
    found = _with_class(user, blau_panel.SWITCH_CLASS)
    assert len(found) <= 1, 'one switch per page'
    return found[0] if found else None


def _visible_texts(user):
    """Label and link texts of elements that are shown (themselves and every ancestor)."""
    def shown(element):
        while element is not None:
            if not element.visible:
                return False
            element = element.parent_slot.parent if element.parent_slot else None
        return True
    return [e.text for e in _all_elements(user)
            if isinstance(e, (ui.label, ui.link)) and e.text and shown(e)]


def _hint_shown(user):
    return any(e.visible for e in _with_class(user, blau_panel.HINT_CLASS))


def _opener_shown(user):
    return any(e.visible for e in _with_class(user, blau_panel.OPEN_CLASS))


async def _turn_on(user):
    switch = _switch(user)
    _click(user, switch)
    await _wait_until(lambda: _marked_panes(user))
    assert _marked_panes(user), 'the switch did not mark the pane'


def test_the_switch_is_off_by_default_and_turns_the_lookup_on_and_off():
    async def driver(user):
        await _open(user)
        switch = _switch(user)
        assert switch is not None and switch.props.get('icon') == 'menu_book'
        assert switch.props.get('aria-pressed') == 'false' and 'active' not in switch.classes
        assert not _marked_panes(user), 'off by default: a click on a word looks nothing up'
        assert not _hint_shown(user) and not _opener_shown(user)
        assert _popover(user)._style.get('display') == 'none'

        await _turn_on(user)
        assert len(_marked_panes(user)) == 1
        assert switch.props.get('aria-pressed') == 'true' and 'active' in switch.classes
        assert _hint_shown(user) and _opener_shown(user)
        assert 'Click a word for its dictionary entry' in _visible_texts(user)
        tips = [e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]
        assert "Turn off Blau's dictionary" in tips

        # Off again, with the panel up: the pane is unmarked and the panel hides.
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        _click(user, switch)
        await _wait_until(lambda: not _marked_panes(user))
        assert not _marked_panes(user) and popover._style.get('display') == 'none'
        assert not _hint_shown(user) and switch.props.get('aria-pressed') == 'false'
        assert "Turn on Blau's dictionary (Judeo-Arabic)" in [
            e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]

    _run(driver, _dictionary(), lang='en')


def test_the_reader_s_choice_holds_on_the_next_page():
    async def driver(user):
        await _open(user)
        await _turn_on(user)
        await _open(user)
        assert len(_marked_panes(user)) == 1 and _hint_shown(user)
        assert _switch(user).props.get('aria-pressed') == 'true'

    _run(driver, _dictionary(), lang='en')


def test_a_lookup_draws_the_cards_and_the_credit_line_and_the_close_hides_them():
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        assert popover is not None and popover._style.get('display') == 'none'
        assert popover.props.get('role') == 'dialog'
        await _turn_on(user)
        assert len(_marked_panes(user)) == 1
        assert 'Click a word for its dictionary entry' in _visible_texts(user)
        assert not _with_class(user, blau_panel.CARD_CLASS)

        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        assert popover._style.get('display') == 'flex', popover._style
        dictionary.lookup.assert_called_once_with('כתאב')

        texts = _texts(user)
        assert len(_with_class(user, blau_panel.CARD_CLASS)) == 1
        assert "Blau's dictionary" in texts and _shown_word(user) == ['כתאב']
        assert 'כתב' in texts and 'كتب' in texts and 'p. 587' in texts
        senses = _with_class(user, blau_panel.SENSE_CLASS)
        assert len(senses) == 2 and [('matched' in s.classes) for s in senses] == [False, True]
        assert "'letter'" in texts and "'מכתב, איגרת'" in texts and 'classical' in texts
        assert 'אלכתאב' in texts and 'I' in texts and "'document, deed'" in texts
        assert '+3 more senses on the site' in texts
        assert 'Also in the examples under' in texts and 'וג̇ה' in texts and '+2' in texts
        assert {ENTRY_URL, EXAMPLE_URL, SITE_URL} <= _links(user)
        assert CREDIT_HE in texts and CREDIT_EN in texts
        credit_links = [e for e in _all_elements(user) if isinstance(e, ui.link) and e.props.get('href') == SITE_URL]
        assert sorted(e.text for e in credit_links) == sorted([CREDIT_HE, CREDIT_EN])
        [close] = [e for e in _all_elements(user) if isinstance(e, ui.button) and blau_panel.CLOSE_CLASS in e.classes]
        assert close.props.get('aria-label') == 'Close'

        _fire(user, popover, blau_panel.CLOSE_EVENT, {})
        await _wait_until(lambda: popover._style.get('display') == 'none')
        assert popover._style.get('display') == 'none'
        # The cards stay drawn under the hidden popover; the next lookup replaces them.
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'אלכתאב'})
        await _wait_until(lambda: dictionary.lookup.call_count == 2 and popover._style.get('display') == 'flex')
        assert len(_with_class(user, blau_panel.CARD_CLASS)) == 1

    _run(driver, dictionary, lang='en')


def test_the_hebrew_ui_shows_the_site_s_line_only_and_its_own_words():
    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        texts = _texts(user)
        assert CREDIT_HE in texts and CREDIT_EN not in texts
        assert 'מילון בלאו' in texts and 'עמ׳ 587' in texts and 'קלאסי' in texts
        assert 'לחיצה על מילה פותחת את ערכה במילון בלאו' in _visible_texts(user)
        assert 'כיבוי מילון בלאו' in [e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]
        # The Hebrew gloss comes first in a sense, the English after it.
        glosses = [e for e in _all_elements(user) if isinstance(e, ui.label) and 'gloss' in e.classes]
        assert [('he' in g.classes) for g in glosses[:2]] == [True, False]

    _run(driver, _dictionary(), lang='he')


def test_an_empty_answer_says_what_was_looked_for():
    result = empty_lookup('וזזז', 'וזזז', ('וזזז', 'זזז'))

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'וזזז'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        texts = _texts(user)
        assert _with_class(user, blau_panel.EMPTY_CLASS) and not _with_class(user, blau_panel.CARD_CLASS)
        assert "Blau's dictionary covers Judeo-Arabic; nothing for this word." in texts
        assert 'Looked for' in texts and 'וזזז' in texts and 'זזז' in texts
        assert CREDIT_HE in texts
        assert SITE_URL in _links(user), 'an empty answer still links the credited site'

    _run(driver, _dictionary(result=result), lang='en')


def test_a_result_without_a_credit_line_is_not_shown():
    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: dictionary.lookup.called)
        await _wait_until(lambda: False, timeout=0.5)  # give a wrong render its chance
        assert popover._style.get('display') == 'none'
        assert not _with_class(user, blau_panel.CARD_CLASS) and not _with_class(user, blau_panel.CREDIT_CLASS)

    dictionary = _dictionary(credit_he='')
    _run(driver, dictionary, lang='en')


def _slow_dictionary(slow_word, seconds=0.6):
    """A dictionary whose lookup of ``slow_word`` takes ``seconds`` (in the
    worker thread, as the real one would on a slow disk)."""
    dictionary = _dictionary()

    def lookup(word):
        if word == slow_word:
            time.sleep(seconds)
        return _lookup(word)

    dictionary.lookup.side_effect = lookup
    return dictionary


def _shown_word(user):
    """The word the panel shows: the value of the box at its top."""
    return [e.value for e in _all_elements(user) if isinstance(e, ui.input) and blau_panel.INPUT_CLASS in e.classes]


def test_the_type_a_word_button_opens_an_empty_box_and_the_box_looks_up_what_is_typed():
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        [opener] = _with_class(user, blau_panel.OPEN_CLASS)
        assert not [li for li in opener._event_listeners.values() if li.type == 'click'], \
            'the script handles this click; a server handler would race its close'
        _fire(user, popover, blau_panel.OPEN_EVENT, {})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        assert _shown_word(user) == [''] and not dictionary.lookup.called
        [box] = [e for e in _all_elements(user) if isinstance(e, ui.input) and blau_panel.INPUT_CLASS in e.classes]
        assert 'autofocus' in box.props
        assert 'Type a Judeo-Arabic word in Hebrew letters, or an Arabic word.' in _texts(user)
        assert CREDIT_HE in _texts(user)

        box.value = ' أخذ '
        [search] = [e for e in _all_elements(user) if isinstance(e, ui.button) and blau_panel.SEARCH_CLASS in e.classes]
        _click(user, search)
        await _wait_until(lambda: dictionary.lookup.called)
        dictionary.lookup.assert_called_once_with('أخذ')
        await _wait_until(lambda: _with_class(user, blau_panel.CARD_CLASS))
        assert _shown_word(user) == ['أخذ'] and len(_with_class(user, blau_panel.CARD_CLASS)) == 1

        # An empty box asks nothing.
        [box] = [e for e in _all_elements(user) if isinstance(e, ui.input) and blau_panel.INPUT_CLASS in e.classes]
        box.value = '   '
        [search] = [e for e in _all_elements(user) if isinstance(e, ui.button) and blau_panel.SEARCH_CLASS in e.classes]
        _click(user, search)
        await _wait_until(lambda: False, timeout=0.3)
        assert dictionary.lookup.call_count == 1

    _run(driver, dictionary, lang='en')


def test_a_slow_earlier_lookup_does_not_overwrite_the_newer_word():
    # Codex on #473: the reader clicks a second word while the first lookup runs.
    dictionary = _slow_dictionary('slow')

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        assert _shown_word(user) == ['כתאב']
        await _wait_until(lambda: False, timeout=1.0)  # the slow lookup finishes meanwhile
        assert dictionary.lookup.call_count == 2
        assert _shown_word(user) == ['כתאב'], 'the stale lookup redrew the panel'
        assert len(_with_class(user, blau_panel.CARD_CLASS)) == 1

    _run(driver, dictionary, lang='en')


def test_a_close_during_a_lookup_keeps_the_panel_closed():
    dictionary = _slow_dictionary('slow')

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'slow'})
        _fire(user, popover, blau_panel.CLOSE_EVENT, {})
        await _wait_until(lambda: dictionary.lookup.called)
        await _wait_until(lambda: False, timeout=1.0)
        assert popover._style.get('display') == 'none', 'the stale lookup reopened the panel'
        assert not _with_class(user, blau_panel.CARD_CLASS)

    _run(driver, dictionary, lang='en')


def test_a_payload_without_a_word_asks_the_dictionary_nothing():
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        for payload in ({'word': 7}, {'word': '   '}, {}, None):
            _fire(user, popover, blau_panel.LOOKUP_EVENT, payload)
        await _wait_until(lambda: False, timeout=0.5)
        assert not dictionary.lookup.called
        assert popover._style.get('display') == 'none'

    _run(driver, dictionary, lang='en')


def test_an_unavailable_dictionary_leaves_the_page_as_it_was():
    dictionary = _dictionary(available=False)

    async def driver(user):
        await _open(user)
        assert _popover(user) is None and _switch(user) is None
        assert not _with_class(user, blau_panel.OPEN_CLASS)
        assert not _marked_panes(user)
        assert 'Click a word for its dictionary entry' not in _texts(user)
        assert not _with_class(user, blau_panel.PANEL_CLASS)
        assert not dictionary.lookup.called

    _run(driver, dictionary, lang='en')
