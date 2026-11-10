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
from unittest.mock import patch

from nicegui import ui

from shared.blau_service import BlauCitation, BlauExpression, BlauExpressionHit, BlauPlace, entry_url
from shared.ja_expression import DEFAULT_GAP, ExpressionComponent
from tests.render_smoke.test_blau_panel_render_smoke import (
    CREDIT_HE, _dictionary, _fire, _open, _popover, _run, _shown_word, _texts, _turn_on, _with_class,
)
from tests.render_smoke.test_browse_button_icons_render_smoke import _all_elements, _wait_until
from tests.render_smoke.test_friedman_panel_render_smoke import _box_search
from web.components import blau_panel, expression_panel, friedman_panel
from web.pages import browse

ROOT = 'blau-e-000172'
TRANSLATION = "'מפני שהם מן השמור בהכרח'"
LOCATOR = 'חיבור יפה, 145:1'


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


def test_several_words_in_the_box_search_blau_as_an_expression_with_marks_and_a_gap_control():
    dictionary = _dictionary()
    dictionary.answer.side_effect = lambda word, gap=DEFAULT_GAP: (
        _blau_expression(word, gap) if len(word.split()) > 1 else dictionary.lookup(word))

    def sent(captured, name):
        return [c.args[1] for c in captured.call_args_list if c.args and c.args[0] == name]

    with patch.object(browse, 'posthog_capture') as captured:
        async def driver(user):
            await _open(user)
            await _turn_on(user)
            _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
            await _wait_until(lambda: _with_class(user, blau_panel.CARD_CLASS))
            dictionary.answer.assert_called_once_with('כתאב', gap=DEFAULT_GAP)
            dictionary.lookup.assert_called_once_with('כתאב')
            assert not _with_class(user, blau_panel.EXPRESSION_CLASS), 'one word is a lookup'
            _box_search(user, ' לא בד ')
            await _wait_until(lambda: _with_class(user, blau_panel.EXPRESSION_CLASS))
            dictionary.answer.assert_called_with('לא בד', gap=DEFAULT_GAP)
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
            dictionary.answer.assert_called_with('לא בד', gap=0)
            await _wait_until(lambda: any(isinstance(e, ui.select) and e.value == 0 for e in _all_elements(user)))
            assert _shown_word(user) == ['לא בד'], 'the same words, with the gap the reader chose'
            events = sent(captured, blau_panel.POSTHOG_LOOKUP)
            assert events[-1] == {'how': 'box', 'blau': True, 'lane': None, 'expression': 2}
            assert all('בד' not in json.dumps(c.args, ensure_ascii=False) for c in captured.call_args_list)

        _run(driver, dictionary)


def test_an_expression_blau_lacks_keeps_the_gap_control():
    dictionary = _dictionary()
    dictionary.answer.side_effect = lambda word, gap=DEFAULT_GAP: (
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
