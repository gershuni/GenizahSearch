# -*- coding: utf-8 -*-
"""Blau's tab with several words in the box (the expression search, 2026-11-08), rendered through
web/pages/browse.py without a browser, with a stubbed service: the page asks
``BlauService.answer`` with the gap for every lookup (one word stays a lookup), the Blau tab
shows the expression section with the entry, its name holding the words marked, the citation
with its words marked, its translation, the form it illustrates with its gloss, the locator,
the matched-word pairs and the gap control; a change of the gap asks again with the new gap;
PostHog counts the words of an expression, never which."""
from __future__ import annotations

import json
from dataclasses import replace
from unittest.mock import patch

import pytest
from nicegui import ui

from shared.blau_service import BlauCitation, BlauExpression, BlauExpressionHit, BlauFormDetails, BlauPlace, entry_url
from shared.ja_expression import DEFAULT_GAP, ExpressionComponent
from tests.render_smoke.test_blau_panel_render_smoke import (
    CREDIT_HE, _dictionary, _fire, _links, _open, _popover, _run, _shown_word, _texts, _turn_on, _with_class,
)
from tests.render_smoke.test_browse_button_icons_render_smoke import _all_elements, _click, _wait_until
from tests.render_smoke.test_friedman_panel_render_smoke import _box_search
from web.components import blau_panel, expression_panel, friedman_panel, show_more
from web.pages import browse

ROOT = 'blau-e-000172'
TRANSLATION = "'מפני שהם מן השמור בהכרח'"
LOCATOR = 'חיבור יפה, 145:1'


@pytest.mark.parametrize('phrase', [False, True])
@pytest.mark.parametrize('retry', [False, True])
def test_sense_examples_load_only_on_click_and_stay_with_the_selected_sense(phrase, retry):
    dictionary = _dictionary()
    if phrase:
        base = _blau_expression('לא בד', DEFAULT_GAP)
        places = tuple(replace(p, details_available=True,
                               quotation=replace(p.quotation, id='c1') if p.quotation else None) for p in base.hits[0].places)
        answer = replace(base, hits=(replace(base.hits[0], places=places),))
    else:
        base = dictionary.lookup('כתאב')
        answer = replace(base, cards=(replace(base.cards[0], senses=(replace(base.cards[0].senses[0], details_available=True),)),),
                         quotes=(), examples=())
    dictionary.answer.side_effect = lambda *a, **k: answer
    details = BlauFormDetails('f1', 'synthetic grammar note', 'synthetic classical note', (
        BlauCitation(0, 'לאנהם מן השמור ולא בד', TRANSLATION, LOCATOR, 'חיבור יפה', id='c1'),
        BlauCitation(1, 'דוגמה שלא הוצגה', 'example translation', 'source 17', 'source',
                     id='c2', commentary='synthetic commentary', bibliography='full bibliography'),))
    dictionary.form_details.return_value = details
    if retry:
        dictionary.form_details.side_effect = [None, details]

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'לא בד' if phrase else 'כתאב'})
        await _wait_until(lambda: _with_class(user, show_more.DETAILS_CLASS))
        dictionary.form_details.assert_not_called()
        assert 'דוגמה שלא הוצגה' not in _texts(user)
        buttons = [e for e in _all_elements(user) if isinstance(e, ui.button) and e.text == show_more.DETAILS]
        assert len(buttons) == 1, 'one button per matching sense, even when its citation is also shown'
        _click(user, buttons[0])
        if retry:
            await _wait_until(lambda: buttons[0].text == 'Could not load dictionary details. Try again.')
            assert 'דוגמה שלא הוצגה' not in _texts(user)
            _click(user, buttons[0])
        await _wait_until(lambda: 'דוגמה שלא הוצגה' in _texts(user))
        assert dictionary.form_details.call_count == (2 if retry else 1)
        assert all(c.args == ('f1',) for c in dictionary.form_details.call_args_list)
        for text in ('example translation', 'synthetic grammar note', 'synthetic classical note', 'synthetic commentary', 'full bibliography'):
            assert text in _texts(user)
        assert _texts(user).count(TRANSLATION) == 1

    _run(driver, dictionary)


def _blau_expression(text, gap):
    components = (ExpressionComponent('לא', 'לא', 'ולא', 'לא', 'stem'), ExpressionComponent('בד', 'בד', 'בד', 'בד', 'word'))
    name = BlauPlace('form', 0, ('ולא', 'בד'), (0, 1), components, None,
                     form_id='f1', form_text='ולא בד', gloss_he="'בהכרח, דווקא'", gloss_en="'inevitably'")
    citation = BlauPlace('citation', 0, ('לאנהם', 'מן', 'השמור', 'ולא', 'בד'), (3, 4), components,
                         BlauCitation(0, 'לאנהם מן השמור ולא בד', TRANSLATION, LOCATOR, 'חיבור יפה'),
                         form_id='f1', form_text='ולא בד', gloss_he="'בהכרח, דווקא'", gloss_en="'inevitably'")
    hit = BlauExpressionHit(root_id=ROOT, name='בדד', arabic='بدد', page=32, url=entry_url(ROOT), sub_entry='בד',
                            places=(name, citation), places_total=3)
    return BlauExpression(text, tuple(text.split()), (('לא',), ('בד',)), gap, (hit,), False)


def _marked_html(user):
    """The HTML of every marked text in Blau's expression section."""
    return [e.content for e in _all_elements(user) if isinstance(e, ui.html)
            and any(blau_panel.EXPRESSION_CLASS in a.classes for a in e.ancestors())]


def test_inherited_arabic_construction_exposes_the_matching_sense_and_stem():
    place = BlauPlace('construction', 0, ('וקף', 'عَلى', 'فِعْلٍ'), (0, 1), (), None,
                      form_id='f1', form_text='عَلى فِعْلٍ', gloss_he='synthetic decision sense', binyan='I')
    hit = BlauExpressionHit(ROOT, 'וקף', 'وقف', 7, entry_url(ROOT), '', (place,), 1)
    dictionary = _dictionary(result=BlauExpression('ואקפא עלי', ('ואקפא', 'עלי'), (('וקפ',), ('עלי',)), 2, (hit,), False))

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'ואקפא עלי'})
        await _wait_until(lambda: 'synthetic decision sense' in _texts(user))
        assert 'I' in _texts(user)
        assert 'צירוף עם הפועל של הערך הראשי' in _texts(user)
        assert any('عَلى' in html and 'فِعْلٍ' in html for html in _marked_html(user))

    _run(driver, dictionary, lang='he')


def test_several_words_in_the_box_search_blau_as_an_expression_with_marks_and_a_gap_control():
    dictionary = _dictionary()
    dictionary.answer.side_effect = lambda word, gap=DEFAULT_GAP, stems=True, before=(), after=(), ordered=True: (
        replace(_blau_expression(word, gap), ordered=ordered) if len(word.split()) > 1 else dictionary.lookup(word))

    def sent(captured, name):
        return [c.args[1] for c in captured.call_args_list if c.args and c.args[0] == name]

    with patch.object(browse, 'posthog_capture') as captured:
        async def driver(user):
            await _open(user)
            await _turn_on(user)
            _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
            await _wait_until(lambda: _with_class(user, blau_panel.CARD_CLASS))
            dictionary.answer.assert_called_once_with('כתאב', gap=DEFAULT_GAP, stems=False, before=(), after=(), ordered=True)
            dictionary.lookup.assert_called_once_with('כתאב')
            assert not _with_class(user, blau_panel.EXPRESSION_CLASS), 'one word is a lookup'
            _box_search(user, ' לא בד ')
            await _wait_until(lambda: _with_class(user, blau_panel.EXPRESSION_CLASS))
            dictionary.answer.assert_called_with('לא בד', gap=DEFAULT_GAP, stems=True, before=(), after=(), ordered=True)
            assert _shown_word(user) == ['לא בד']
            texts = _texts(user)
            assert blau_panel.DISCLAIMER in texts and CREDIT_HE in texts
            assert 'בדד' in texts and 'בד' in texts and 'p. 32' in texts and "'inevitably'" in texts
            assert _marked_html(user) == [
                f'<mark class="{expression_panel.HIT_MARK_CLASS}">ולא</mark> '
                f'<mark class="{expression_panel.HIT_MARK_CLASS}">בד</mark>',
                f'לאנהם מן השמור <mark class="{expression_panel.HIT_MARK_CLASS}">ולא</mark> '
                f'<mark class="{expression_panel.HIT_MARK_CLASS}">בד</mark>',
            ], 'the name that holds the expression, then the citation, the matched words marked'
            assert friedman_panel.IN_HEAD in texts and expression_panel.MATCHED_LABEL in texts and 'לא (ולא)' in texts, \
                'a typed word reached by a clitic alternative names the printed word'
            assert TRANSLATION in texts and LOCATOR in texts and friedman_panel.UNDER in texts and 'ולא בד' in texts
            assert f'+1 {expression_panel.MORE_PLACES}' in texts
            [select] = [e for e in _all_elements(user) if isinstance(e, ui.select) and 'gap-select' in e.classes]
            assert select.value == DEFAULT_GAP and expression_panel.GAP_LABEL in texts
            with user._client:
                select.set_value(0)
            await _wait_until(lambda: dictionary.answer.call_count == 3)
            dictionary.answer.assert_called_with('לא בד', gap=0, stems=False, before=(), after=(), ordered=True)
            await _wait_until(lambda: any(isinstance(e, ui.select) and e.value == 0 for e in _all_elements(user)))
            assert _shown_word(user) == ['לא בד'], 'the same words, with the gap the reader chose'
            events = sent(captured, blau_panel.POSTHOG_LOOKUP)
            assert events[-1] == {'how': 'box', 'blau': True, 'lane': None, 'expression': 2}
            assert all('בד' not in json.dumps(c.args, ensure_ascii=False) for c in captured.call_args_list)
            [order] = _with_class(user, 'expr-any-order')
            with user._client:
                order.set_value(True)
            await _wait_until(lambda: dictionary.answer.call_count == 4)
            assert dictionary.answer.call_args.kwargs['ordered'] is False
            await _wait_until(lambda: any(e.value is True for e in _with_class(user, 'expr-any-order')))
            [select] = [e for e in _all_elements(user) if isinstance(e, ui.select) and 'gap-select' in e.classes]
            assert 20 in select.options and select.props['popup-content-class'] == 'dict-gap-menu'
            with user._client:
                select.set_value(20)
            await _wait_until(lambda: dictionary.answer.call_count == 5)
            assert dictionary.answer.call_args.kwargs['gap'] == 20
            assert dictionary.answer.call_args.kwargs['ordered'] is False

        _run(driver, dictionary)


def test_an_expression_blau_lacks_keeps_the_gap_control():
    dictionary = _dictionary()
    dictionary.answer.side_effect = lambda word, gap=DEFAULT_GAP, stems=True, before=(), after=(), ordered=True: (
        BlauExpression(word, tuple(word.split()), (('לא',), ('בד',)), gap, (), False)
        if len(word.split()) > 1 else dictionary.lookup(word))

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: _with_class(user, blau_panel.CARD_CLASS))
        _box_search(user, 'לא בד')
        await _wait_until(lambda: _with_class(user, blau_panel.EXPRESSION_CLASS))
        texts = _texts(user)
        assert expression_panel.NOTHING_TOGETHER in texts and CREDIT_HE in texts
        assert [e for e in _all_elements(user) if isinstance(e, ui.select) and 'gap-select' in e.classes], \
            'the gap control stays, so the reader can widen it'
        assert not _with_class(user, blau_panel.CARD_CLASS)

    _run(driver, dictionary)


def test_a_word_found_only_in_a_citation_is_listed_under_its_entry_with_the_word_marked():
    """One word, no entry of its own: the entry that quotes it is drawn under "Also in the
    examples under", the citation with the word marked, and the answer is not "nothing"."""
    from shared.blau_service import BlauLookup
    component = (ExpressionComponent('השמור', 'השמור', 'השמור', 'השמור', 'word'),)
    citation = BlauPlace('citation', 0, ('לאנהם', 'מן', 'השמור', 'ולא', 'בד'), (2,), component,
                         BlauCitation(0, 'לאנהם מן השמור ולא בד', TRANSLATION, LOCATOR, 'חיבור יפה'),
                         form_id='blau-f-1', form_text='בד', gloss_he="'הכרח'", gloss_en="'necessity'")
    hit = BlauExpressionHit(root_id=ROOT, name='בדד', arabic='بدد', page=32, url=entry_url(ROOT), sub_entry='בד',
                            places=(citation,), places_total=1)
    result = BlauLookup(word='השמור', key='השמור', candidates=('השמור',), cards=(), more_cards=0,
                        examples=(), more_examples=0, quotes=(hit,), more_quotes=3)

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        popover = _popover(user)
        _fire(user, popover, blau_panel.LOOKUP_EVENT, {'word': 'השמור'})
        await _wait_until(lambda: popover._style.get('display') == 'flex')
        texts = _texts(user)
        assert len(_with_class(user, blau_panel.EXAMPLES_CLASS)) == 1
        assert blau_panel.EXAMPLES_LABEL in texts and 'בדד' in texts and TRANSLATION in texts
        assert entry_url(ROOT) in _links(user)
        assert any(isinstance(e, ui.button) and e.text.startswith('+3 more entries') for e in _all_elements(user))
        assert blau_panel.NOTHING not in texts and not _with_class(user, blau_panel.EMPTY_CLASS)
        marked = [e.content for e in _all_elements(user) if isinstance(e, ui.html)
                  and any(blau_panel.EXAMPLES_CLASS in a.classes for a in e.ancestors())]
        assert any(expression_panel.HIT_MARK_CLASS in html and 'השמור' in html for html in marked)

    _run(driver, _dictionary(result=result))
