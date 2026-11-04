# -*- coding: utf-8 -*-
"""Render-smoke: Blau's dictionary on /browse (docs/plans/BLAU_DICTIONARY_PLAN.md §6.2).

Drives the real ``/browse`` over a stubbed browse service (harness: test_browse_button_icons_render_smoke.py)
and a stubbed dictionary service, and fires the events the click script (web/static/js/blau_lookup.js) would
dispatch on the popover. Pinned: a served dictionary gives the page the popover (hidden) and the reader's
switch, OFF by default (owner, 2026-11-04: the page never guesses the text's language) -- no marked pane, no
hint; the switch marks the pane and shows the hint and the "type a word" button, and the choice holds on the
next page; the switch is the letters עع with a label, which says "New!" until the visitor first turns it on
(owner, 2026-11-04), and PostHog counts the switch, the lookups and the tabs, never the word; a lookup draws one card per entry with its senses, the matched one marked, the example entries,
the site links, the close control and the credit line -- the site's Hebrew line in both UI languages, the English
rendering beside it only in the English UI -- and shows the popover, with the word in the box at its top; the
box looks up what is typed; the close hides it; an empty answer says what the dictionary covers and what was looked for; a result without a credit
line is not shown; a service that is not available leaves the page without any of it; a payload without a word
asks the dictionary nothing. With Lane's Lexicon served too, each dictionary has its own tab (owner, 2026-11-04;
plan D12): the panel opens on the reader's last chosen tab while it has an answer, else on the first that has
one; a tab with nothing is greyed; Lane alone gives the page the panel without a tab row.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import events, ui

from shared import arabic_service
from shared.arabic_service import LaneEntry, LaneLookup, LaneReading, LaneRoot, empty_lane
from shared.blau_service import SITE_URL, BlauCard, BlauExample, BlauLookup, BlauSense, empty_lookup
from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE, SID, _all_elements, _click, _run_home_smoke, _service, _wait_until,
)
from web.components import blau_panel, lane_panel
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


PERSEUS = arabic_service.PERSEUS_CREDIT
LANE_ENTRY_URL = arabic_service.entry_url('lane:t:ذا')
LANE_ROOT_URL = arabic_service.root_url('ذا')


def _lane_result(word):
    entries = (
        LaneEntry(entry_id='lane:t:ذا', headword='ذَا', page=948, text='ذَا is said to be originally ذَىْ …',
                  cut=True, url=LANE_ENTRY_URL, matched=True),
        LaneEntry(entry_id='lane:t:ذو', headword='ذُو', page=984, text='ذُو meaning صَاحِب.', cut=False,
                  url=arabic_service.entry_url('lane:t:ذو'), matched=False),
    )
    root = LaneRoot(root_key='ذWW', name='ذا', page=948, url=LANE_ROOT_URL, entries=entries, more_entries=15)
    return LaneLookup(word=word, script='ja', readings=(LaneReading('ذلك', 'ذلك', 1227282),
                                                        LaneReading('دلك', 'دلك', 595)), roots=(root,))


def _lane(result=None, credit=PERSEUS, available=True):
    """The Lane service: by default NOT available, so the Blau tests see Blau only."""
    svc = MagicMock()
    svc.credit = credit
    svc.is_available.return_value = available
    if available:
        svc.lookup.side_effect = lambda word: result(word) if callable(result) else (result or _lane_result(word))
    else:
        svc.lookup.side_effect = lambda word: empty_lane(word)
    return svc


def _run(driver, dictionary, lang='en', lane=None):
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=dictionary), \
            patch.object(browse, 'get_arabic_service', return_value=lane or _lane(available=False)), \
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


def _switch_label(user):
    [label] = _with_class(user, blau_panel.LABEL_CLASS)
    return label.text


def _has_letters(switch):
    """The switch carries the letters עع, drawn (no icon, no font)."""
    return [e.content for e in switch.descendants()
            if isinstance(e, ui.html) and blau_panel.GLYPH_CLASS in e.classes] == [blau_panel.GLYPH_SVG]


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
        assert switch is not None and not switch.props.get('icon') and _has_letters(switch)
        assert 'color' not in switch.props, "Quasar's !important text-primary would beat the state colours"
        assert switch.props.get('aria-pressed') == 'false' and 'active' not in switch.classes
        assert blau_panel.NEW_CLASS in switch.classes, 'new to this visitor: highlighted'
        assert _switch_label(user) == 'New! Judeo-Arabic dictionaries!'
        assert switch.props.get('aria-label') == 'New! Judeo-Arabic dictionaries!', 'named on a phone too'
        assert not _marked_panes(user), 'off by default: a click on a word looks nothing up'
        assert not _hint_shown(user) and not _opener_shown(user)
        assert _popover(user)._style.get('display') == 'none'

        await _turn_on(user)
        assert len(_marked_panes(user)) == 1
        assert switch.props.get('aria-pressed') == 'true' and 'active' in switch.classes
        assert blau_panel.NEW_CLASS not in switch.classes, 'turned on once: no longer new'
        assert _switch_label(user) == 'Judeo-Arabic dictionaries'
        assert switch.props.get('aria-label') == 'Judeo-Arabic dictionaries'
        assert _hint_shown(user) and _opener_shown(user)
        assert 'Click a word to look it up in the dictionaries' in _visible_texts(user)
        tips = [e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]
        assert 'Turn off the dictionaries' in tips

        # Off again, with the panel up: the pane is unmarked and the panel hides.
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        _click(user, switch)
        await _wait_until(lambda: not _marked_panes(user))
        assert not _marked_panes(user) and popover._style.get('display') == 'none'
        assert not _hint_shown(user) and switch.props.get('aria-pressed') == 'false'
        assert blau_panel.NEW_CLASS not in switch.classes, 'off again, but not new again'
        assert 'Turn on the dictionaries (Judeo-Arabic)' in [
            e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]

    _run(driver, _dictionary(), lang='en')


def test_the_reader_s_choice_holds_on_the_next_page():
    async def driver(user):
        await _open(user)
        await _turn_on(user)
        await _open(user)
        assert len(_marked_panes(user)) == 1 and _hint_shown(user)
        assert _switch(user).props.get('aria-pressed') == 'true'
        assert blau_panel.NEW_CLASS not in _switch(user).classes

    _run(driver, _dictionary(), lang='en')


def test_the_switch_says_new_in_the_owner_s_hebrew_until_it_is_first_turned_on():
    """Owner, 2026-11-04: the feature was too hidden. Until the visitor first turns the dictionaries
    on, the switch is highlighted and its label says "חדש! מילונים לערבית־יהודית!" (with the maqaf),
    on every page; once turned on, it is the plain label for good, turned off or not."""
    async def driver(user):
        await _open(user)
        assert blau_panel.NEW_CLASS in _switch(user).classes
        assert _switch_label(user) == 'חדש! מילונים לערבית־יהודית!'
        await _open(user)
        assert blau_panel.NEW_CLASS in _switch(user).classes, 'not turned on yet: still new on the next page'
        await _turn_on(user)
        assert _switch_label(user) == 'מילונים לערבית־יהודית'
        assert _switch(user).props.get('aria-label') == 'מילונים לערבית־יהודית'
        _click(user, _switch(user))
        await _wait_until(lambda: not _marked_panes(user))
        await _open(user)
        assert blau_panel.NEW_CLASS not in _switch(user).classes and not _marked_panes(user)
        assert _switch_label(user) == 'מילונים לערבית־יהודית'

    _run(driver, _dictionary(), lang='he')


def test_the_switch_the_lookups_and_the_tabs_are_counted_without_the_word():
    """PostHog (owner, 2026-11-04): is the switch found, and used? The switch says whether it was new;
    a lookup says how it was asked and which dictionary had something; a tab says which the reader
    chose. The word itself is never sent."""
    dictionary, lane = _dictionary(), _lane()

    def sent(captured, name):
        return [c.args[1] for c in captured.call_args_list if c.args and c.args[0] == name]

    with patch.object(browse, 'posthog_capture') as captured:
        async def driver(user):
            await _open(user)
            await _turn_on(user)
            assert sent(captured, blau_panel.POSTHOG_SWITCH) == [{'on': True, 'was_new': True}]
            await _look_up(user, 'דלך', lane, 1)
            await _wait_until(lambda: sent(captured, blau_panel.POSTHOG_LOOKUP))
            assert sent(captured, blau_panel.POSTHOG_LOOKUP) == [{'how': 'word', 'blau': True, 'lane': True}]
            _choose(user, 'lane')
            await _wait_until(lambda: sent(captured, blau_panel.POSTHOG_TAB))
            assert sent(captured, blau_panel.POSTHOG_TAB) == [{'tab': 'lane'}]
            _click(user, _switch(user))
            await _wait_until(lambda: len(sent(captured, blau_panel.POSTHOG_SWITCH)) == 2)
            assert sent(captured, blau_panel.POSTHOG_SWITCH)[1] == {'on': False, 'was_new': False}
            assert all('דלך' not in json.dumps(c.args, ensure_ascii=False) for c in captured.call_args_list)
            assert all(c.kwargs.get('client') is not None for c in captured.call_args_list), \
                "through the page's client: a lookup answers from a background task"

        _run(driver, dictionary, lane=lane)


def test_a_lookup_draws_the_cards_and_the_credit_line_and_the_close_hides_them():
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        assert popover is not None and popover._style.get('display') == 'none'
        assert popover.props.get('role') == 'dialog'
        await _turn_on(user)
        assert len(_marked_panes(user)) == 1
        assert 'Click a word to look it up in the dictionaries' in _visible_texts(user)
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
        assert blau_panel.DISCLAIMER in texts, 'the disclaimer is on every panel'
        credit_links = [e for e in _all_elements(user) if isinstance(e, ui.link) and e.props.get('href') == SITE_URL]
        assert sorted(e.text for e in credit_links) == sorted([CREDIT_HE, CREDIT_EN])
        midrash = [e for e in _all_elements(user) if isinstance(e, ui.link) and e.props.get('href') == blau_panel.ABOUT_URL]
        assert [e.text for e in midrash] == [blau_panel.MIDRASH_CREDIT], 'the digital edition is credited, linked'
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
        assert any(t.startswith('מילון בלאו מתעד') for t in texts), 'the disclaimer in Hebrew'
        assert any(t.startswith('המהדורה הדיגיטלית: פרויקט MiDRASH') for t in texts), 'MiDRASH, in Hebrew'
        assert 'מילון בלאו' in texts and 'עמ׳ 587' in texts and 'קלאסי' in texts
        assert 'לחיצה על מילה מחפשת אותה במילונים' in _visible_texts(user)
        assert 'כיבוי המילונים' in [e.text for e in _all_elements(user) if isinstance(e, ui.tooltip)]
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
        assert 'Nothing in the dictionary for this word.' in texts and blau_panel.DISCLAIMER in texts
        assert 'Looked for' in texts and 'וזזז' in texts and 'זזז' in texts
        assert CREDIT_HE in texts and blau_panel.MIDRASH_CREDIT in texts
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
        assert blau_panel.DISCLAIMER in _texts(user)
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


def test_an_entry_spelled_without_the_word_s_dot_is_marked():
    exact = _lookup('ד\'לך')
    dotted = replace(exact.cards[0], name='ד̇לך ו', matched_key='ד̇לך ו')
    undotted = replace(exact.cards[0], name='דלך', matched_key='דלך', dot_mismatch=True)
    result = replace(exact, cards=(dotted, undotted), examples=(), more_examples=0)

    async def driver(user):
        await _open(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'ד\'לך'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        cards = _with_class(user, blau_panel.CARD_CLASS)
        flags = [e for e in _all_elements(user) if isinstance(e, ui.label) and 'dot-mismatch' in e.classes]
        assert len(cards) == 2 and [f.text for f in flags] == ['spelled without the dot']
        assert cards[1].id in {a.id for a in flags[0].ancestors()}, 'the flag sits on the undotted entry'

    _run(driver, _dictionary(result=result), lang='en')


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
        assert 'Click a word to look it up in the dictionaries' not in _texts(user)
        assert not _with_class(user, blau_panel.PANEL_CLASS)
        assert not dictionary.lookup.called

    _run(driver, dictionary, lang='en')


def _lane_sections(user):
    return _with_class(user, lane_panel.SECTION_CLASS)


def _tab_row(user):
    found = _with_class(user, blau_panel.TABS_CLASS)
    assert len(found) <= 1, 'one tab row per panel'
    return found[0] if found else None


def _tabs(user):
    """(name, label, has an answer) of each tab, in order."""
    return [(e.props.get('name'), e.props.get('label'), 'found' in e.classes)
            for e in _with_class(user, blau_panel.TAB_CLASS)]


def _tab_of(element):
    """The name of the tab panel ``element`` is drawn in (None outside the tabs)."""
    return next((a.props.get('name') for a in element.ancestors() if isinstance(a, ui.tab_panel)), None)


def _head_title(user):
    return [e.text for e in _all_elements(user)
            if isinstance(e, ui.label) and 'title' in e.classes and 'panel-head' in e.parent_slot.parent.classes]


def _choose(user, name):
    """The reader clicks a tab: Quasar sets the tab row's value."""
    with user._client:
        _tab_row(user).set_value(name)


async def _look_up(user, word, lane, calls):
    _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': word})
    await _wait_until(lambda: lane.lookup.call_count == calls and _shown_word(user) == [word] and _tab_row(user))
    assert _shown_word(user) == [word]


def test_lane_has_its_own_tab_beside_blau_s_with_the_perseus_line():
    """D12 (owner, 2026-11-04): Lane in a tab, not under Blau's cards. Lane's tab holds the readings as
    chips (the commonest first, marked), the root card with Lane's entries, the matched one marked, each
    linked to its entry on the source site, the rest counted and linked, and the Perseus line verbatim
    (D3); Blau's tab holds Blau's cards and credit. Both have an answer: the panel opens on Blau's."""
    dictionary, lane = _dictionary(), _lane()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'דלך'})
        await _wait_until(lambda: _lane_sections(user))
        lane.lookup.assert_called_once_with('דלך')
        dictionary.lookup.assert_called_once_with('דלך')
        assert _head_title(user) == ['Dictionaries'], 'the head names no one dictionary'
        assert _tabs(user) == [('blau', 'Blau', True), ('lane', 'Lane', True)]
        assert _tab_row(user).value == 'blau'
        [card] = _with_class(user, blau_panel.CARD_CLASS)
        [section] = _lane_sections(user)
        assert _tab_of(card) == 'blau' and _tab_of(section) == 'lane'
        [blau_credit] = _with_class(user, blau_panel.CREDIT_CLASS)
        [lane_credit] = _with_class(user, lane_panel.CREDIT_CLASS)
        assert _tab_of(blau_credit) == 'blau' and _tab_of(lane_credit) == 'lane', 'each credit in its own tab'
        texts = _texts(user)
        assert lane_panel.TITLE in texts and lane_panel.READINGS_LABEL in texts
        chips = [e for e in _all_elements(user) if isinstance(e, ui.button) and lane_panel.CHIP_CLASS in e.classes]
        assert [c.text for c in chips] == ['ذلك', 'دلك']
        assert ['active' in c.classes for c in chips] == [True, False]
        assert 'ذا' in texts and 'ذَا' in texts and 'p. 948' in texts and 'p. 984' in texts
        entries = _with_class(user, lane_panel.ENTRY_CLASS)
        assert [('matched' in e.classes) for e in entries] == [True, False]
        assert '+15 more entries under this root' in texts
        links = _links(user)
        assert {LANE_ENTRY_URL, LANE_ROOT_URL, lane_panel.PERSEUS_URL, arabic_service.SITE_URL,
                lane_panel.OPENITI_URL} <= links
        full = [e for e in _all_elements(user) if isinstance(e, ui.link) and e.text == 'full entry']
        assert [e.props.get('href') for e in full] == [LANE_ENTRY_URL], 'only the cut entry links to its full text'
        perseus = [e for e in _all_elements(user) if isinstance(e, ui.link)
                   and e.props.get('href') == lane_panel.PERSEUS_URL]
        assert [e.text for e in perseus] == [PERSEUS], 'the Perseus line, verbatim'

    _run(driver, dictionary, lane=lane)


def test_a_word_blau_lacks_opens_on_lane_s_tab_and_greys_blau_s():
    dictionary = _dictionary(result=empty_lookup('דלך', 'דלך', ('דלך',)))
    lane = _lane()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלך'})
        await _wait_until(lambda: _lane_sections(user))
        assert _tabs(user) == [('blau', 'Blau', False), ('lane', 'Lane', True)]
        assert _tab_row(user).value == 'lane'
        [blau_tab] = [e for e in _with_class(user, blau_panel.TAB_CLASS) if e.props.get('name') == 'blau']
        assert 'not-found' in blau_tab.classes, 'greyed, not hidden'
        [empty] = _with_class(user, blau_panel.EMPTY_CLASS)
        assert _tab_of(empty) == 'blau', "Blau's tab says it has nothing, when opened"

    _run(driver, dictionary, lane=lane)


def test_the_reader_s_tab_holds_for_the_next_word_while_it_has_an_answer():
    def lane_result(word):
        return _lane_result(word) if word != 'כתאב' else LaneLookup(word=word, script='ja', readings=(), roots=())

    dictionary, lane = _dictionary(), _lane(result=lane_result)

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        await _look_up(user, 'דלך', lane, 1)
        assert _tab_row(user).value == 'blau'
        _choose(user, 'lane')
        await _look_up(user, 'הדא', lane, 2)
        assert _tab_row(user).value == 'lane', "the reader's choice holds for the next word"
        await _look_up(user, 'כתאב', lane, 3)
        assert _tab_row(user).value == 'blau', 'Lane has nothing for this word: the tab that has'
        assert ('lane', 'Lane', False) in _tabs(user)
        await _look_up(user, 'צדיק', lane, 4)
        assert _tab_row(user).value == 'lane', 'the choice was not forgotten'

    _run(driver, dictionary, lane=lane)


def test_a_reading_chip_looks_that_reading_up_in_the_box():
    dictionary, lane = _dictionary(), _lane()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלך'})
        await _wait_until(lambda: _lane_sections(user))
        [first, second] = [e for e in _all_elements(user)
                           if isinstance(e, ui.button) and lane_panel.CHIP_CLASS in e.classes]
        assert not [li for li in first._event_listeners.values() if li.type == 'click'], \
            'the reading shown is not a link to itself'
        _click(user, second)
        await _wait_until(lambda: lane.lookup.call_count == 2)
        assert [c.args[0] for c in lane.lookup.call_args_list] == ['דלך', 'دلك']
        assert dictionary.lookup.call_args_list[-1].args[0] == 'دلك'
        await _wait_until(lambda: _shown_word(user) == ['دلك'])
        assert _shown_word(user) == ['دلك'], 'the box holds the reading looked up'

    _run(driver, dictionary, lane=lane)


def test_lane_without_its_credit_line_has_no_tab():
    dictionary, lane = _dictionary(), _lane(credit='')

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex' and lane.lookup.called)
        assert len(_with_class(user, blau_panel.CARD_CLASS)) == 1
        assert not _lane_sections(user) and _tab_row(user) is None and not _with_class(user, blau_panel.TAB_CLASS)
        assert _head_title(user) == ["Blau's dictionary"], 'one dictionary: the head names it'

    _run(driver, dictionary, lane=lane)


def test_lane_s_tab_says_when_it_has_no_reading():
    dictionary = _dictionary()
    lane = _lane(result=lambda word: LaneLookup(word=word, script='ja', readings=(), roots=()))

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex' and lane.lookup.called)
        assert not _lane_sections(user)
        assert _tabs(user) == [('blau', 'Blau', True), ('lane', 'Lane', False)]
        assert _tab_row(user).value == 'blau'
        [none] = _with_class(user, lane_panel.EMPTY_SECTION_CLASS)
        assert _tab_of(none) == 'lane'
        said = [e.text for e in _all_elements(user) if isinstance(e, ui.label) and none.id in {a.id for a in e.ancestors()}]
        assert said == [lane_panel.TITLE, 'Nothing in the dictionary for this word.']

    _run(driver, dictionary, lane=lane)


def test_the_type_a_word_panel_has_both_tabs():
    dictionary, lane = _dictionary(), _lane()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.OPEN_EVENT, {})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        assert _shown_word(user) == [''] and not dictionary.lookup.called and not lane.lookup.called
        assert _tabs(user) == [('blau', 'Blau', False), ('lane', 'Lane', False)]
        texts = _texts(user)
        assert 'Type a Judeo-Arabic word in Hebrew letters, or an Arabic word.' in texts
        assert blau_panel.DISCLAIMER in texts and CREDIT_HE in texts
        assert 'Nothing in the dictionary for this word.' not in texts, 'no word yet: nothing to say was not found'

    _run(driver, dictionary, lane=lane)


def test_an_unavailable_lane_leaves_blau_s_panel_as_it_was():
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        assert len(_with_class(user, blau_panel.CARD_CLASS)) == 1 and not _lane_sections(user)
        assert _tab_row(user) is None and _head_title(user) == ["Blau's dictionary"]

    _run(driver, dictionary)


def test_lane_alone_gives_the_page_the_panel():
    """Blau not served, Lane served: the switch, the hint and the panel are there, with Lane's
    answer, no tab row, and the head titled with Lane's name (not repeated in the section)."""
    dictionary, lane = _dictionary(available=False), _lane()

    async def driver(user):
        await _open(user)
        assert _switch(user) is not None and _popover(user) is not None
        await _turn_on(user)
        assert 'Click a word to look it up in the dictionaries' in _visible_texts(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'דלך'})
        await _wait_until(lambda: _lane_sections(user))
        assert not dictionary.lookup.called, 'a dictionary not served is not asked'
        assert _tab_row(user) is None and _head_title(user) == [lane_panel.TITLE]
        assert lane_panel.TITLE not in [e.text for e in _all_elements(user)
                                        if isinstance(e, ui.label) and 'lane-title' in e.classes]
        assert not _with_class(user, blau_panel.CARD_CLASS) and not _with_class(user, blau_panel.CREDIT_CLASS)
        assert PERSEUS in _texts(user)

    _run(driver, dictionary, lane=lane)
