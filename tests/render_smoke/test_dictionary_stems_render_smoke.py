# -*- coding: utf-8 -*-
"""Render-smoke: a root's stems in Blau's and Friedman's tabs on /browse (the private dictionary
lookup plan, section 4.6; owner, 2026-11-11).

Drives the real ``/browse`` over stubbed browse, Blau and Friedman services (harness:
test_blau_panel_render_smoke.py) and fires the events the click script would. Pinned: a root's card
shows the stems its dictionary lists as buttons under the existing label 'verb stem', none chosen;
pressing one looks up "<root> <stem>" in every dictionary (the box then holds it), stays on the tab
that was pressed, and the card opens on that stem's paragraph with the button marked as chosen;
pressing the chosen one goes back to the root; a typed stem the root does not list says so above
the root's cards (the owner's wording, both languages), and the buttons are still there; PostHog
counts a press as how 'stem', never the word.
"""
from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nicegui import ui

from shared.blau_service import BlauCard, BlauLookup
from shared.friedman_service import VIA_CHOSEN, FriedmanLookup, FriedmanMatch
from shared.ja_roots import root_and_stem
from tests.render_smoke.test_blau_panel_render_smoke import (
    CREDIT_HE, _fire, _lane, _open, _popover, _sense, _shown_word, _switch, _tab_of, _tab_row, _texts, _turn_on,
    _with_class,
)
from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE, _all_elements, _click, _run_home_smoke, _service, _wait_until,
)
from tests.render_smoke.test_friedman_panel_render_smoke import CREDIT as FRIEDMAN_CREDIT
from tests.render_smoke.test_friedman_panel_render_smoke import STATUS, _unit
from web.components import blau_panel, friedman_panel, stem_buttons
from web.pages import browse

ROOT_URL = 'https://blau-dict.netlify.app/entry/blau-e-000866'
BLAU_STEMS = (1, 2, 4, 5, 10)
FRIEDMAN_STEMS = (1, 2, 4, 10)

PLAIN_SENSES = (_sense('f1', "'הוכיח'", "'to prove'", binyan='I'), _sense('f3', "'פינק'", "'to pamper'", binyan='II'))
STEM_X_SENSE = _sense('f6', "'הסיק'", "'to deduce'", binyan='X')

ENTRY = _unit('f001149', 'entry', 'דלל אַן', "'הראה, הנחה'", binyan='I')
STEM_IV = _unit('f001152', 'binyan', 'עלי', "'סמך על'", parent='f001149', binyan='IV')
SUB = _unit('f001150', 'subentry', '-ה אן', "'הורה לו'", parent='f001149')


def _blau_answer(word, gap=None, stems=True):
    """Blau as the service answers: the root's card with its stems; with stem X typed, that
    stem's paragraph; with a stem the root does not list, the root's card and the stem asked."""
    _root, stem = root_and_stem(word) if stems else ('', 0)
    if stem == 10:
        card = BlauCard(root_id='blau-e-000866', name='דלל', arabic='دلل', page=232, url=ROOT_URL, candidate='דלל',
                        matched_key='דלל', matched_kind='root', sub_entry='', via='', senses=(STEM_X_SENSE,),
                        more_senses=0, root_label='דלל', stems=BLAU_STEMS, stem=10)
    else:
        card = BlauCard(root_id='blau-e-000866', name='דלל', arabic='دلل', page=232, url=ROOT_URL, candidate='דלל',
                        matched_key='דלל', matched_kind='root', sub_entry='', via='', senses=PLAIN_SENSES,
                        more_senses=4, root_label='דלל', stems=BLAU_STEMS)
    return BlauLookup(word=word, key='דלל', candidates=('דלל',), cards=(card,), more_cards=0, examples=(),
                      more_examples=0, stem=stem)


def _friedman_answer(word, gap=None, stems=True):
    _root, stem = root_and_stem(word) if stems else ('', 0)
    if stem == 4:
        matches = (FriedmanMatch(STEM_IV, VIA_CHOSEN, 'דלל', ENTRY, (), 0, stems=FRIEDMAN_STEMS, root='דלל', stem=4),)
    else:
        matches = (FriedmanMatch(ENTRY, 'head', 'דלל', None, (SUB, STEM_IV), 2, stems=FRIEDMAN_STEMS, root='דלל'),)
    return FriedmanLookup(word, 'דלל', ('דלל',), matches, False, STATUS, FRIEDMAN_CREDIT, stem=stem)


def _services():
    blau, friedman = MagicMock(), MagicMock()
    for svc, answer in ((blau, _blau_answer), (friedman, _friedman_answer)):
        svc.is_available.return_value = True
        svc.answer.side_effect = answer
        svc.lookup.side_effect = answer
    for svc in (blau, friedman):   # the page's borrowed roots: none added here
        svc.borrow.side_effect = lambda result, borrowed: result
    blau.credit_he, blau.credit_en = CREDIT_HE, ''
    friedman.credit_he, friedman.status = FRIEDMAN_CREDIT, STATUS
    return blau, friedman


def _run(driver, blau, friedman, lang='en'):
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=blau), \
            patch.object(browse, 'get_arabic_service', return_value=_lane(available=False)), \
            patch.object(browse, 'get_friedman_service', return_value=friedman), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(driver, lang=lang)


def _stem_buttons(user, tab):
    """(label, chosen) of the stem buttons drawn in ``tab``."""
    return [(e.text, stem_buttons.CHOSEN_CLASS in e.classes) for e in _all_elements(user)
            if isinstance(e, ui.button) and stem_buttons.STEM_CLASS in e.classes and _tab_of(e) == tab]


def _press(user, tab, label):
    [button] = [e for e in _all_elements(user) if isinstance(e, ui.button) and stem_buttons.STEM_CLASS in e.classes
                and _tab_of(e) == tab and e.text == label]
    _click(user, button)


def _answered(svc, word):
    return any(c.args and c.args[0] == word for c in svc.answer.call_args_list)


def _stem_field(user):
    """The verb-stem field beside the panel's box."""
    [field] = [e for e in _all_elements(user) if isinstance(e, ui.select) and blau_panel.STEM_SELECT_CLASS in e.classes]
    return field


def _box(user):
    [box] = [e for e in _all_elements(user) if isinstance(e, ui.input) and blau_panel.INPUT_CLASS in e.classes]
    return box


def _search(user):
    [button] = [e for e in _all_elements(user) if isinstance(e, ui.button) and blau_panel.SEARCH_CLASS in e.classes]
    _click(user, button)


def test_the_stem_field_beside_the_box_looks_the_root_up_with_that_stem():
    blau, friedman = _services()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
        await _wait_until(lambda: _stem_buttons(user, blau_panel.BLAU_TAB))
        field = _stem_field(user)
        assert field.value is None and field.props.get('label') == 'verb stem'
        assert list(field.options.values()) == ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']
        with user._client:
            field.set_value(10)
        await _wait_until(lambda: _answered(blau, 'דלל X') and _with_class(user, 'stem-chosen'))
        assert _answered(friedman, 'דלל X') and _shown_word(user) == ['דלל'] and _stem_field(user).value == 10
        # Another root typed in the box keeps the stem chosen beside it.
        with user._client:
            _box(user).value = 'קרב'
        _search(user)
        await _wait_until(lambda: _answered(blau, 'קרב X'))
        # A stem typed in the box wins over the field.
        await _wait_until(lambda: _shown_word(user) == ['קרב'])
        with user._client:
            _box(user).value = 'דלל IV'
        _search(user)
        await _wait_until(lambda: _answered(blau, 'דלל IV'))
        await _wait_until(lambda: _shown_word(user) == ['דלל'] and _stem_field(user).value == 4)
        # Cleared: the root alone.
        with user._client:
            _stem_field(user).set_value(None)
        await _wait_until(lambda: blau.answer.call_args_list[-1].args[0] == 'דלל')

    _run(driver, blau, friedman)


def test_no_stem_field_where_only_lane_is_served():
    blau, friedman = _services()
    blau.is_available.return_value = False
    friedman.is_available.return_value = False
    lane = _lane()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
        await _wait_until(lambda: _shown_word(user) == ['דלל'])
        assert not [e for e in _all_elements(user) if blau_panel.STEM_SELECT_CLASS in e.classes]

    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=blau), \
            patch.object(browse, 'get_arabic_service', return_value=lane), \
            patch.object(browse, 'get_friedman_service', return_value=friedman), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(driver, lang='en')


def test_a_root_s_stems_are_buttons_and_a_press_opens_that_stem_s_paragraph():
    blau, friedman = _services()

    def sent(captured):
        return [c.args[1] for c in captured.call_args_list if c.args and c.args[0] == blau_panel.POSTHOG_LOOKUP]

    with patch.object(browse, 'posthog_capture') as captured:
        async def driver(user):
            await _open(user)
            await _turn_on(user)
            _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
            await _wait_until(lambda: _stem_buttons(user, blau_panel.BLAU_TAB))
            assert _stem_buttons(user, blau_panel.BLAU_TAB) == [(n, False) for n in ('I', 'II', 'IV', 'V', 'X')]
            assert _stem_buttons(user, blau_panel.FRIEDMAN_TAB) == [(n, False) for n in ('I', 'II', 'IV', 'X')]
            assert stem_buttons.STEMS_LABEL in _texts(user)
            assert not _with_class(user, 'stem-chosen') and "'to pamper'" in _texts(user)

            _press(user, blau_panel.BLAU_TAB, 'X')
            await _wait_until(lambda: _answered(blau, 'דלל X') and _with_class(user, 'stem-chosen'))
            assert _answered(friedman, 'דלל X'), 'one press asks every dictionary'
            assert _shown_word(user) == ['דלל'] and _stem_field(user).value == 10, \
                'the box holds the root and the field beside it the stem'
            assert _tab_row(user).value == blau_panel.BLAU_TAB, 'the panel stays on the tab that was pressed'
            [chosen] = [c for c in _with_class(user, 'stem-chosen') if _tab_of(c) == blau_panel.BLAU_TAB]
            assert blau_panel.CARD_CLASS in chosen.classes
            texts = _texts(user)
            assert "'to deduce'" in texts and "'to pamper'" not in texts, "the card shows stem X's paragraph"
            assert ('X', True) in _stem_buttons(user, blau_panel.BLAU_TAB)
            assert sent(captured)[-1] == {'how': blau_panel.STEM_HOW, 'blau': True, 'lane': None,
                                          'friedman': True, 'stem': True}
            assert all('דלל' not in str(c.args) for c in captured.call_args_list), 'never the word'

            _press(user, blau_panel.BLAU_TAB, 'X')   # the chosen stem again: back to the root
            await _wait_until(lambda: blau.answer.call_args_list[-1].args[0] == 'דלל' and _stem_field(user).value is None)
            assert _shown_word(user) == ['דלל'] and _stem_field(user).value is None
            assert _stem_buttons(user, blau_panel.BLAU_TAB) == [(n, False) for n in ('I', 'II', 'IV', 'V', 'X')]

        _run(driver, blau, friedman)


def test_a_press_in_friedman_s_tab_opens_the_stem_paragraph_there():
    blau, friedman = _services()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
        await _wait_until(lambda: _stem_buttons(user, blau_panel.FRIEDMAN_TAB))
        _press(user, blau_panel.FRIEDMAN_TAB, 'IV')
        await _wait_until(lambda: _answered(friedman, 'דלל IV')
                          and [c for c in _with_class(user, 'stem-chosen') if _tab_of(c) == blau_panel.FRIEDMAN_TAB])
        assert _tab_row(user).value == blau_panel.FRIEDMAN_TAB, "Blau's tab has an answer too, but the press was here"
        [card] = [c for c in _with_class(user, 'stem-chosen') if _tab_of(c) == blau_panel.FRIEDMAN_TAB]
        assert friedman_panel.MATCH_CLASS in card.classes
        texts = [e.text for e in card.descendants() if isinstance(e, ui.label) and e.text]
        assert "'סמך על'" in texts and 'verb stem' in texts and 'under' in texts, "the stem paragraph, under its entry"
        assert not any(v in texts for v in friedman_panel.VIA_LABELS.values()), 'a chosen stem was not found by a word'
        assert ('IV', True) in _stem_buttons(user, blau_panel.FRIEDMAN_TAB)

    _run(driver, blau, friedman)


def _not_listed(user):
    return [(e.text, _tab_of(e)) for e in _with_class(user, stem_buttons.NOT_LISTED_CLASS)]


async def _typed(user, text):
    """The panel open on a clicked word, then ``text`` typed in its box and looked up."""
    _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
    await _wait_until(lambda: _stem_buttons(user, blau_panel.BLAU_TAB))
    with user._client:
        _box(user).value = text
    _search(user)


def test_a_typed_stem_the_root_does_not_list_says_so_above_the_root_s_cards():
    blau, friedman = _services()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        await _typed(user, 'דלל III')
        await _wait_until(lambda: _not_listed(user))
        assert sorted(_not_listed(user), key=lambda t: t[1]) == [
            ('Stem III is not listed under this root.', blau_panel.BLAU_TAB),
            ('Stem III is not listed under this root.', blau_panel.FRIEDMAN_TAB)]
        assert _stem_buttons(user, blau_panel.BLAU_TAB) == [(n, False) for n in ('I', 'II', 'IV', 'V', 'X')], \
            'the root with the stems it has'
        assert "'to pamper'" in _texts(user)

    _run(driver, blau, friedman)


def test_the_stems_in_the_hebrew_ui():
    blau, friedman = _services()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        await _typed(user, 'דלל III')
        await _wait_until(lambda: _not_listed(user))
        texts = _texts(user)
        assert 'בניין III אינו מופיע תחת השורש הזה.' in texts and 'בניין' in texts
        assert [b for b in _all_elements(user) if isinstance(b, ui.button) and stem_buttons.STEM_CLASS in b.classes
                and b.props.get('title') == 'בניין IV'], 'the tooltip names the stem'

    _run(driver, blau, friedman, lang='he')


def test_words_selected_in_the_manuscript_are_never_a_typed_stem():
    """A number or numeral after a word in the manuscript ("פרק 2") is two words, not a root with a
    stem: the page asks both dictionaries with ``stems=False`` (the expression search), and only the
    box, a stem button or a chip reads a stem. The gap control keeps an expression one."""
    blau, friedman = _services()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל X'})
        await _wait_until(lambda: _answered(blau, 'דלל X') and _answered(friedman, 'דלל X'))
        await _wait_until(lambda: _stem_buttons(user, blau_panel.BLAU_TAB))
        for svc in (blau, friedman):
            call = [c for c in svc.answer.call_args_list if c.args[0] == 'דלל X'][-1]
            assert call.kwargs.get('stems') is False, call
        assert not _with_class(user, 'stem-chosen'), 'no stem paragraph opened'
        # The same text typed in the box is the typed shortcut.
        with user._client:
            _box(user).value = 'דלל X'
        _search(user)
        await _wait_until(lambda: _with_class(user, 'stem-chosen'))
        assert blau.answer.call_args_list[-1].kwargs.get('stems') is True

    _run(driver, blau, friedman)


@pytest.mark.parametrize('lang, label', [('en', 'Other possible roots'), ('he', 'שורשים אפשריים נוספים')])
def test_the_other_roots_a_word_s_verb_forms_give_are_chips_that_open_that_root_on_that_stem(lang, label):
    """Owner, 2026-11-11 (the graded sample): beside the cards the word itself reached, the roots its
    verb forms give are a line of chips at the top of the tab ("קרב V", or the root alone when several
    stems fit); a press looks that up in every dictionary, as a stem button does, on the same tab, and
    the word's chips stay, the pressed one marked, with the word first to go back to its answer."""
    blau, friedman = _services()

    def blau_answer(word, gap=None, stems=True):
        answer = _blau_answer(word, gap)
        return replace(answer, suggestions=(('קרב', 5), ('ד̇לל', 0))) if word == 'תדלל' else answer
    blau.answer.side_effect = blau_answer
    blau.lookup.side_effect = blau_answer

    def chips(user):
        return [e for e in _all_elements(user) if isinstance(e, ui.button) and stem_buttons.SUGGESTION_CLASS in e.classes]

    def hows(captured):
        return [c.args[1]['how'] for c in captured.call_args_list if c.args and c.args[0] == blau_panel.POSTHOG_LOOKUP]

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'תדלל'})
        await _wait_until(lambda: chips(user))
        assert [c.text for c in chips(user)] == ['קרב V', 'ד̇לל'] and all(_tab_of(c) == blau_panel.BLAU_TAB for c in chips(user))
        [row] = _with_class(user, stem_buttons.SUGGESTIONS_CLASS)
        assert [e.text for e in row.descendants() if isinstance(e, ui.label)] == [label]
        cards = _with_class(user, blau_panel.CARD_CLASS)
        assert row.id not in {a.id for c in cards for a in c.descendants()}, 'the line sits above the cards'
        _click(user, chips(user)[0])
        await _wait_until(lambda: _answered(blau, 'קרב V') and len(chips(user)) == 3)
        assert _answered(friedman, 'קרב V') and _tab_row(user).value == blau_panel.BLAU_TAB
        await _wait_until(lambda: hows(captured)[-1:] == [blau_panel.SUGGEST_HOW])
        assert hows(captured)[-1] == blau_panel.SUGGEST_HOW, 'PostHog counts a chip press as a suggestion'
        # The chips stay (owner, 2026-11-11), the pressed one marked, the word first to go back.
        assert [(c.text, stem_buttons.CHOSEN_CLASS in c.classes) for c in chips(user)] == [
            ('תדלל', False), ('קרב V', True), ('ד̇לל', False)]
        _click(user, chips(user)[0])   # the word: back to its own answer
        await _wait_until(lambda: len(chips(user)) == 2)
        assert blau.answer.call_args_list[-1].args[0] == 'תדלל'
        assert [(c.text, stem_buttons.CHOSEN_CLASS in c.classes) for c in chips(user)] == [('קרב V', False), ('ד̇לל', False)]
        _click(user, chips(user)[1])   # another chip
        await _wait_until(lambda: _answered(blau, 'ד̇לל') and len(chips(user)) == 3)
        assert [c.text for c in chips(user) if stem_buttons.CHOSEN_CLASS in c.classes] == ['ד̇לל']
        _click(user, [c for c in chips(user) if stem_buttons.CHOSEN_CLASS in c.classes][0])   # pressed again: back
        await _wait_until(lambda: len(chips(user)) == 2)
        assert blau.answer.call_args_list[-1].args[0] == 'תדלל'

    with patch.object(browse, 'posthog_capture') as captured:
        _run(driver, blau, friedman, lang=lang)


@pytest.mark.parametrize("editor", [False, True])
def test_a_dictionary_switched_to_editors_only_is_on_the_page_for_an_editor_alone(editor):
    """BLAU_/FRIEDMAN_DICTIONARY_ENABLED=editors (shared/dictionary_switch.py; owner, 2026-11-11): a
    preview before release. The services answer the public question (``is_available``) no; the page
    asks once whether the reader is an editor or admin (``GlobalAuthState.is_editor``) and, if so,
    whether the dictionary is served to them (``served_to``). A visitor gets no switch and no panel;
    an editor gets both, and the lookups answer in both tabs."""
    blau, friedman = _services()
    for svc in (blau, friedman):
        svc.is_available.return_value = False
        svc.served_to.side_effect = lambda reader: bool(reader)
    lane = _lane(available=False)
    lane.served_to.return_value = False   # Lane switched off: no tab even for an editor

    async def driver(user):
        await _open(user)
        if not editor:
            assert _switch(user) is None and _popover(user) is None, "a visitor sees no dictionary"
            assert not blau.answer.called and not friedman.answer.called
            return
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'דלל'})
        await _wait_until(lambda: _stem_buttons(user, blau_panel.BLAU_TAB) and _stem_buttons(user, blau_panel.FRIEDMAN_TAB))
        assert _answered(blau, 'דלל') and _answered(friedman, 'דלל') and not lane.lookup.called

    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=blau), \
            patch.object(browse, 'get_arabic_service', return_value=lane), \
            patch.object(browse, 'get_friedman_service', return_value=friedman), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=editor), \
            patch.object(browse.GlobalAuthState, 'is_editor', return_value=editor):
        _run_home_smoke(driver, lang='en')
