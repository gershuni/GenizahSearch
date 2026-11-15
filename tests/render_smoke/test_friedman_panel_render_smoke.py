# -*- coding: utf-8 -*-
"""Render-smoke: Friedman's dictionary in its tab of the dictionary panel on /browse
(the private Friedman plan steps 3 and 4).

Drives the real ``/browse`` over the stubbed browse, Blau and Friedman services (harness:
test_blau_panel_render_smoke.py) and fires the events the click script would. Pinned: a served
Friedman gives the panel a third tab, 'Friedman', whose section carries the status line, one card
per unit the word reaches (its kind, its head with the vowels, how it was reached, the definition,
the quotations with their locators, the sub-entries, the link to the original page) and the
author's credit line verbatim; several words in the box are an EXPRESSION search (plan step 4),
drawn with the matched words marked inside the quotation, the printed word named when a typed
word was reached by a clitic alternative, and the gap control, which runs the same words again
with the gap the reader chose; the words not found together say so and keep the gap control;
PostHog counts an expression lookup by its number of words, never the words.
"""
from __future__ import annotations

import json
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nicegui import ui

from shared.friedman_service import (
    DEFAULT_GAP, SCOPE_HEAD, SCOPE_QUOTATION, VIA_BORROWED, VIA_VERB_FORM, FriedmanComponent, FriedmanExpression, FriedmanExpressionHit,
    FriedmanLookup, FriedmanMatch, FriedmanPlace, FriedmanQuotation, FriedmanUnit, empty_expression,
)
from shared.ja_roots import BORROW_BLAU, BORROW_LANE, BorrowedRoot
from tests.render_smoke.test_blau_panel_render_smoke import (
    _choose, _dictionary, _fire, _lane, _links, _open, _popover, _shown_word, _tab_of, _tab_row, _tabs, _texts,
    _turn_on, _with_class,
)
from tests.render_smoke.test_browse_button_icons_render_smoke import OPEN_PAGE, _all_elements, _click, _run_home_smoke, _service, _wait_until
from tests.render_smoke.test_browse_button_icons_render_smoke import _wait_until as _wait
from web.components import blau_panel, friedman_panel, show_more
from web.pages import browse

CREDIT = 'מרדכי עקיבא פרידמן, מילון הערבית-היהודית מימי הביניים (שורת קרדיט לבדיקה)'
STATUS = 'candidate_not_reviewed_not_for_production'
PAGE_URL = 'https://ja.genizah.org/JA_Dictionary_pdfjs/web/viewer.html?file=PDF/1.pdf#page=5'


def _unit(uid, kind, head, definition, quotations=(), parent=None, binyan=''):
    return FriedmanUnit(id=uid, kind=kind, letter='ע', parent=parent, head_text=head, head_plain=head, root=head,
                        lemma=head, binyan=binyan, definition=definition, references='', printed_page='97',
                        pdf_url=PAGE_URL, flags=(), quotations=tuple(quotations), quotations_total=len(quotations))


QUOTE = FriedmanQuotation(1, 'נתכלם בעון אללה פי הדה אלורקה', "'נדבר בעזרת האל בדף הזה'", '(ח2, שורה 3)', '')
ENTRY = _unit('f005299', 'entry', 'עַוְן', "'עזרה'", [QUOTE])
CHILD = _unit('f005300', 'subentry', 'בִעַוְנִ אללה', "'בעזרת האל'", parent='f005299')


def _lookup(word):
    return FriedmanLookup(word, 'עונ', ('עונ',), (FriedmanMatch(ENTRY, 'head', 'עונ', None, (CHILD,), 1),),
                          False, STATUS, CREDIT)


def _expression(text, gap):
    place = FriedmanPlace(SCOPE_QUOTATION, 1, tuple(QUOTE.quote.split()), (1, 2),
                          (FriedmanComponent('עון', 'עונ', 'בעון', 'עונ', 'stem'),
                           FriedmanComponent('אללה', 'אללה', 'אללה', 'אללה', 'word')), QUOTE)
    head_place = FriedmanPlace(SCOPE_HEAD, 0, tuple(CHILD.head_text.split()), (0, 1),
                               (FriedmanComponent('עון', 'עונ', 'בִעַוְנִ', 'עונ', 'stem'),
                                FriedmanComponent('אללה', 'אללה', 'אללה', 'אללה', 'word')), None)
    hits = (FriedmanExpressionHit(CHILD, ENTRY, (head_place,), 1), FriedmanExpressionHit(ENTRY, None, (place,), 3))
    return FriedmanExpression(text, tuple(text.split()), (('עונ',), ('אללה',)), gap, hits, False, STATUS, CREDIT)


def _friedman(available=True, credit=CREDIT, expression=None):
    svc = MagicMock()
    svc.is_available.return_value = available
    svc.credit_he = credit
    svc.status = STATUS
    svc.lookup.side_effect = _lookup
    svc.expression.side_effect = expression or (lambda text, gap=DEFAULT_GAP: _expression(text, gap))
    # The page asks ``answer`` (several words an expression search, one word a lookup); the double
    # routes it to ``expression`` / ``lookup`` so a test that replaces either, or counts its calls,
    # still holds (as Blau's double does).
    svc.answer.side_effect = lambda text, gap=DEFAULT_GAP, stems=True, before=(), after=(), ordered=True: (
        svc.expression(text, gap=gap) if len(text.split()) > 1 else svc.lookup(text))
    svc.borrow.side_effect = lambda result, borrowed: result   # the page's borrowed roots: none added here
    return svc


def _run(driver, dictionary, friedman, lang='en', lane=None):
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(OPEN_PAGE)), \
            patch.object(browse, 'get_blau_service', return_value=dictionary), \
            patch.object(browse, 'get_arabic_service', return_value=lane or _lane(available=False)), \
            patch.object(browse, 'get_friedman_service', return_value=friedman), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(driver, lang=lang)


def _sections(user):
    return _with_class(user, friedman_panel.SECTION_CLASS)


def _marked_html(user):
    """The HTML of every marked text (a quotation's words or a head's) in Friedman's section."""
    return [e.content for e in _all_elements(user) if isinstance(e, ui.html)
            and any(friedman_panel.SECTION_CLASS in a.classes for a in e.ancestors())]


def _box_search(user, text):
    [box] = [e for e in _all_elements(user) if isinstance(e, ui.input) and blau_panel.INPUT_CLASS in e.classes]
    box.value = text
    [search] = [e for e in _all_elements(user) if isinstance(e, ui.button) and blau_panel.SEARCH_CLASS in e.classes]
    _click(user, search)


def test_friedman_has_its_tab_with_the_status_the_units_and_the_credit():
    dictionary, friedman = _dictionary(), _friedman()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'עון'})
        await _wait_until(lambda: _sections(user))
        friedman.lookup.assert_called_once_with('עון')
        assert not friedman.expression.called, 'one word is a lookup, not an expression search'
        assert _tabs(user) == [('blau', 'Blau', True), ('friedman', 'Friedman', True)]
        [section] = _sections(user)
        assert _tab_of(section) == 'friedman'
        texts = _texts(user)
        assert friedman_panel.STATUS_NOTE in texts and CREDIT in texts
        assert 'entry' in texts and 'sub-entry' in texts and 'עַוְן' in texts and 'בִעַוְנִ אללה' in texts
        assert friedman_panel.VIA_LABELS['head'] in texts and "'עזרה'" in texts
        assert QUOTE.quote in texts and QUOTE.translation in texts and QUOTE.locator in texts
        assert 'p. 97' in texts and friedman_panel.OPEN_PAGE in texts
        assert {PAGE_URL, friedman_panel.SITE_URL} <= _links(user)
        [credit] = _with_class(user, friedman_panel.CREDIT_CLASS)
        assert _tab_of(credit) == 'friedman'

    _run(driver, dictionary, friedman)


def test_an_entry_that_sends_the_reader_to_another_shows_the_reference_and_a_button_that_looks_it_up():
    """The entry חפו ("ראה הערך חפי.") showed two full stops. The reference stands
    in the unit's references, and the entry it names is a button that looks it up."""
    from dataclasses import replace
    pointer = replace(_unit('f002467', 'entry', 'חפו', ''), references='ראה הערך חפי.')
    dictionary, friedman = _dictionary(), _friedman()
    friedman.lookup.side_effect = lambda word: (
        FriedmanLookup(word, 'חפו', ('חפו',), (FriedmanMatch(pointer, 'head', 'חפו', None, (), 0, referred=(ENTRY,)),),
                       False, STATUS, CREDIT)
        if word == 'חפו' else _lookup(word))

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'חפו'})
        await _wait_until(lambda: _sections(user))
        assert 'ראה הערך חפי.' in _texts(user)
        [shown] = _with_class(user, friedman_panel.REFERRED_CLASS)
        inside = [e.text for e in shown.descendants() if isinstance(e, ui.label)]
        assert 'עַוְן' in inside and "'עזרה'" in inside and QUOTE.quote in inside, 'the entry referred to, with its text'
        [row] = _with_class(user, friedman_panel.SEE_CLASS)
        [button] = [e for e in row.descendants() if isinstance(e, ui.button)]
        assert button.text == 'חפי'
        _click(user, button)
        await _wait_until(lambda: any(call.args == ('חפי',) for call in friedman.lookup.call_args_list))

    _run(driver, dictionary, friedman)


def test_a_head_printed_without_its_root_reads_root_stem_then_the_construction():
    """"VIII -הُ" under שכו reads as root שכו, stem VIII, suffix -הُ: the root first,
    then the stem, then what the book prints; under its own entry's card, the stem and the suffix."""
    entry = _unit('f007476', 'entry', 'שכו לה', "'התלונן בפניו'", binyan='I')
    stem = replace(_unit('f007478', 'binyan', '-הُ', "'התלונן עליו'", parent='f007476', binyan='VIII'),
                   entry_root='שכו')
    friedman = _friedman()
    friedman.lookup.side_effect = lambda word: FriedmanLookup(word, 'אשתכאה', ('אשתכאה',), (
        FriedmanMatch(stem, 'head', 'אשתכאה', entry, (), 0),
        FriedmanMatch(entry, 'head', 'שכו', None, (stem,), 1)), False, STATUS, CREDIT)

    def heads(user):
        return [[getattr(c, 'text', None) for c in e.default_slot.children] for e in _all_elements(user)
                if 'entry' in e.classes and any(friedman_panel.SECTION_CLASS in a.classes for a in e.ancestors())]

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'אשתכאה'})
        await _wait_until(lambda: heads(user))
        drawn = heads(user)
        assert drawn[0][:4] == ['verb stem', 'שכו', 'VIII', '-הُ'], drawn[0]
        assert ['verb stem', 'VIII', '-הُ'] in [h[:3] for h in drawn[2:]], drawn
        assert drawn[1][:3] == ['entry', 'שכו לה', 'I'], 'an entry as printed'
        assert _with_class(user, friedman_panel.ROOT_CLASS) and _with_class(user, friedman_panel.CONSTRUCTION_CLASS)

    _run(driver, _dictionary(), friedman)


def test_construction_results_show_governing_root_and_references():
    entry = _unit('f000900', 'entry', 'וקף אליה', 'synthetic first construction', binyan='I')
    child = replace(_unit('f000901', 'subentry', 'עלי', 'synthetic sense', parent=entry.id),
                    entry_root='וקף', governing_binyan='I', references='Synthetic reference, p. 42')
    assert friedman_panel._parent_label(child, replace(entry, flags=('braced_head',))) == '{וקף I}'
    friedman = _friedman()
    friedman.expression.side_effect = lambda text, gap=DEFAULT_GAP: replace(
        _expression(text, gap), hits=(FriedmanExpressionHit(child, entry, (), 0),))
    friedman.lookup.side_effect = lambda word: FriedmanLookup(word, 'וקפ', ('וקפ',), (
        FriedmanMatch(entry, 'head', 'וקפ', None, (child,), 1),), False, STATUS, CREDIT)

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'ואקפא עלי'})
        await _wait_until(lambda: 'וקף I' in _texts(user))
        assert 'וקף אליה' not in _texts(user)
        assert child.references in _texts(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'וקף'})
        await _wait_until(lambda: 'וקף אליה' in _texts(user))
        assert child.references in _texts(user), 'references also appear in expanded word senses'

    _run(driver, _dictionary(), friedman)


@pytest.mark.parametrize('quoted_match', [False, True])
def test_phrase_examples_open_on_click_without_repeating_a_visible_quotation(quoted_match):
    extra = FriedmanQuotation(2, 'דוגמה נוספת לבדיקה', 'synthetic translation', 'source 42', '')
    unit = replace(ENTRY, quotations=(QUOTE, extra) if quoted_match else (extra,),
                   quotations_total=2 if quoted_match else 1)
    place = _expression('עון אללה', DEFAULT_GAP).hits[1].places[0]
    hit = FriedmanExpressionHit(unit, None, (place,) if quoted_match else (), 1 if quoted_match else 0)
    friedman = _friedman(expression=lambda text, gap=DEFAULT_GAP: replace(_expression(text, gap), hits=(hit,)))

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'עון אללה'})
        await _wait_until(lambda: any(getattr(e, 'text', '') == 'Show examples (1)' for e in _all_elements(user)))
        assert extra.quote not in _texts(user)
        button = next(e for e in _all_elements(user) if isinstance(e, ui.button) and e.text == 'Show examples (1)')
        _click(user, button)
        await _wait_until(lambda: extra.quote in _texts(user))
        assert extra.translation in _texts(user) and extra.locator in _texts(user)
        if quoted_match:
            assert _texts(user).count(QUOTE.translation) == 1

    _run(driver, _dictionary(), friedman)


def _show_buttons(user):
    return [e for e in _all_elements(user) if isinstance(e, ui.button) and show_more.BUTTON_CLASS in e.classes]


def _quotes_drawn(user):
    return len(_with_class(user, friedman_panel.QUOTE_CLASS))


def test_quotations_and_sub_entries_beyond_the_first_open_in_place_on_a_click():
    """Nothing only on the PDF page. A unit's first QUOTATIONS_SHOWN quotations, a
    sub-entry's first quotation, then "Show N more quotations" in place; an entry that is a chosen
    stem's own paragraph shows its sub-entries and stem paragraphs only on "Show N more sub-entries and
    verb stems". In Hebrew: "הצג N מובאות נוספות", "הצג N ערכי משנה ובניינים נוספים"."""
    quotes = [FriedmanQuotation(n, f'קול {n}', f"'תרגום {n}'", f'(ח{n})', '') for n in range(1, 11)]
    entry = _unit('f000900', 'entry', 'קול', "'דיבור'", quotes)
    children = tuple(_unit(f'f00090{n}', 'binyan', 'קאל', "'אמר'", quotes[:3], parent='f000900', binyan=label)
                     for n, label in ((1, 'II'), (2, 'IV')))
    friedman = _friedman()
    friedman.lookup.side_effect = lambda word: FriedmanLookup(
        word, 'קול', ('קול',), (FriedmanMatch(entry, 'chosen', 'קול', None, children, 2, stems=(1, 2, 4), root='קול',
                                              stem=1),), False, STATUS, CREDIT, stem=1)

    for lang, more_quotes, more_children in (
            ('en', 'Show 2 more quotations', 'Show 2 more sub-entries and verb stems'),
            ('he', 'הצג 2 מובאות נוספות', 'הצג 2 ערכי משנה ובניינים נוספים')):
        async def driver(user, more_quotes=more_quotes, more_children=more_children):
            await _open(user)
            await _turn_on(user)
            _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'קול'})
            await _wait_until(lambda: _sections(user))
            assert _quotes_drawn(user) == friedman_panel.QUOTATIONS_SHOWN == 8
            assert not _with_class(user, friedman_panel.CHILD_CLASS), 'a chosen stem: its siblings wait for a click'
            assert [b.text for b in _show_buttons(user)] == [more_quotes, more_children]
            assert not [t for t in _texts(user) if 'on the page' in t or 'בעמוד' in t]
            _click(user, _show_buttons(user)[0])
            await _wait_until(lambda: _quotes_drawn(user) == 10)
            assert "'תרגום 10'" in _texts(user)
            _click(user, _show_buttons(user)[0])
            await _wait_until(lambda: len(_with_class(user, friedman_panel.CHILD_CLASS)) == 2)
            # Each stem paragraph shows its first quotation; its other two wait for their own click.
            assert [b.text for b in _show_buttons(user)] == [more_quotes] * 2
            _click(user, _show_buttons(user)[0])
            await _wait_until(lambda: _quotes_drawn(user) == 10 + 2 + 2)
            assert not [t for t in _texts(user) if 'on the page' in t or 'בעמוד' in t]

        _run(driver, _dictionary(), friedman, lang=lang)


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_friedman_s_tab_shows_the_entries_of_the_roots_blau_and_lane_give(lang):
    """ויכון reached nothing in Friedman though Lane gave كون. The page hands Friedman
    the roots of Blau's OWN cards (כתב, which כתאב matched) and Lane's first root (ذا, in Hebrew letters
    as Blau spells a root), and Friedman's tab shows its entries for them after its own, with no line at
    all: neither the dictionary that gave the root (redundant) nor "found by"."""
    dictionary, friedman, lane = _dictionary(), _friedman(), _lane()
    asked = []

    def borrow(result, borrowed):
        asked.append(borrowed)
        extra = FriedmanMatch(_unit('f000500', 'entry', 'כתב', "'כתב'"), VIA_BORROWED, '', None, (), 0,
                              borrowed=tuple((b.source, b.evidence) for b in borrowed))
        return replace(result, matches=result.matches + (extra,))
    friedman.borrow.side_effect = borrow

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'כתאב'})
        await _wait_until(lambda: len(_with_class(user, friedman_panel.MATCH_CLASS)) == 2)
        assert asked == [(BorrowedRoot('כתב', BORROW_BLAU, 'כתאב (כתב)'), BorrowedRoot('ד̇א', BORROW_LANE, 'ذا', ('script_reading', 'root')))]
        matches = _with_class(user, friedman_panel.MATCH_CLASS)
        texts = [e.text for e in matches[1].descendants() if isinstance(e, ui.label)]
        assert not any(t in texts for t in ("Root from Blau's dictionary", "Root from Lane's Lexicon",
                                            'השורש לפי מילון בלאו', 'השורש לפי הלקסיקון של ליין',
                                            'כתאב (כתב)', 'ذا')), "no line names the dictionary nor its evidence"
        vias = [v for v in _with_class(user, friedman_panel.VIA_CLASS)]
        assert len(vias) == 1 and matches[0].id in {a.id for a in vias[0].ancestors()}, \
            'only the own match says how it was found'

    _run(driver, dictionary, friedman, lang=lang, lane=lane)


@pytest.mark.parametrize('lang, label', [('en', 'found by its verb form'), ('he', 'נמצא לפי צורת הפועל')])
def test_a_match_found_by_verb_form_says_so_with_the_form_the_root_and_the_stem(lang, label):
    """PR 3: a fourth way of being found beside the word itself, its
    base form and the index, followed by the form, the root and the stem."""
    dictionary, friedman = _dictionary(), _friedman()

    def lookup(word):
        own = _lookup(word)
        verb = FriedmanMatch(_unit('f000600', 'entry', 'דלל', "'הוכיח'"), VIA_VERB_FORM, 'אסתדלו (דלל X)', None, (), 0,
                             stems=(1, 10), root='דלל', kind='A')
        return replace(own, matches=own.matches + (verb,))
    friedman.lookup.side_effect = lookup

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'אסתדלו'})
        await _wait_until(lambda: len(_with_class(user, friedman_panel.MATCH_CLASS)) == 2)
        matches = _with_class(user, friedman_panel.MATCH_CLASS)
        vias = _with_class(user, friedman_panel.VIA_CLASS)
        assert len(vias) == 2 and matches[1].id in {a.id for a in vias[1].ancestors()}
        assert [e.text for e in vias[1].descendants() if isinstance(e, ui.label)] == [label, 'אסתדלו (דלל X)']

    _run(driver, dictionary, friedman, lang=lang)


def test_several_words_in_the_box_are_an_expression_search_with_marks_and_a_gap_control():
    dictionary, friedman = _dictionary(), _friedman()

    def sent(captured, name):
        return [c.args[1] for c in captured.call_args_list if c.args and c.args[0] == name]

    with patch.object(browse, 'posthog_capture') as captured:
        async def driver(user):
            await _open(user)
            await _turn_on(user)
            _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'עון'})
            await _wait_until(lambda: _sections(user))
            _box_search(user, ' עון אללה ')
            await _wait_until(lambda: friedman.expression.called)
            friedman.expression.assert_called_once_with('עון אללה', gap=DEFAULT_GAP)
            await _wait_until(lambda: _with_class(user, friedman_panel.EXPRESSION_CLASS))
            assert _shown_word(user) == ['עון אללה']
            assert _tabs(user) == [('blau', 'Blau', True), ('friedman', 'Friedman', True)]
            texts = _texts(user)
            assert friedman_panel.STATUS_NOTE in texts and CREDIT in texts
            marked = _marked_html(user)
            assert marked == [
                f'<mark class="{friedman_panel.HIT_MARK_CLASS}">בִעַוְנִ</mark> '
                f'<mark class="{friedman_panel.HIT_MARK_CLASS}">אללה</mark>',
                f'נתכלם <mark class="{friedman_panel.HIT_MARK_CLASS}">בעון</mark> '
                f'<mark class="{friedman_panel.HIT_MARK_CLASS}">אללה</mark> פי הדה אלורקה',
            ], 'the head that holds the expression, then the quotation, the matched words marked'
            assert friedman_panel.IN_HEAD in texts and friedman_panel.MATCHED_LABEL in texts
            assert 'עון (בעון)' in texts and 'עון (בִעַוְנִ)' in texts, \
                'a typed word reached by a clitic alternative names the printed word'
            assert QUOTE.translation in texts and QUOTE.locator in texts and f'+2 {friedman_panel.MORE_PLACES}' in texts
            assert 'under' in texts, "the sub-entry names its entry"
            [select] = [e for e in _all_elements(user) if isinstance(e, ui.select) and 'gap-select' in e.classes]
            assert select.value == DEFAULT_GAP and friedman_panel.GAP_LABEL in texts
            assert _tab_row(user).value == 'friedman', 'The matching expression outranks the isolated word'
            _choose(user, 'friedman')
            with user._client:
                select.set_value(0)
            await _wait_until(lambda: friedman.expression.call_count == 2)
            friedman.expression.assert_called_with('עון אללה', gap=0)
            await _wait_until(lambda: any(isinstance(e, ui.select) and e.value == 0 for e in _all_elements(user)))
            assert _shown_word(user) == ['עון אללה'], 'the same words, with the gap the reader chose'
            assert _tab_row(user).value == 'friedman', "the gap control's tab stays"
            events = sent(captured, blau_panel.POSTHOG_LOOKUP)
            assert events[-1] == {'how': 'box', 'blau': True, 'lane': None, 'friedman': True, 'expression': 2}
            assert all('אללה' not in json.dumps(c.args, ensure_ascii=False) for c in captured.call_args_list)

        _run(driver, dictionary, friedman)


def test_words_not_found_together_say_so_and_keep_the_gap_control():
    friedman = _friedman(expression=lambda text, gap=DEFAULT_GAP: empty_expression(text, gap, STATUS, CREDIT,
                                                                                   tuple(text.split())))
    dictionary = _dictionary()

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'עון'})
        await _wait_until(lambda: _sections(user))
        _box_search(user, 'עון קקק')
        await _wait_until(lambda: _with_class(user, friedman_panel.EMPTY_SECTION_CLASS))
        assert _tabs(user) == [('blau', 'Blau', True), ('friedman', 'Friedman', False)], "Friedman's tab is greyed"
        assert friedman_panel.NOTHING_TOGETHER in _texts(user)
        assert not _sections(user) and CREDIT not in [e.text for e in _with_class(user, friedman_panel.CREDIT_CLASS)]
        assert [e for e in _all_elements(user) if isinstance(e, ui.select) and 'gap-select' in e.classes], \
            'the gap control stays, so the reader can widen the gap'

    _run(driver, dictionary, friedman)


def test_a_result_without_a_credit_line_is_not_shown():
    dictionary, friedman = _dictionary(), _friedman(credit='')

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'עון'})
        await _wait(lambda: _with_class(user, blau_panel.CARD_CLASS))
        assert _tabs(user) == [], 'Blau alone: no tab row'
        assert not _sections(user) and friedman_panel.STATUS_NOTE not in _texts(user)
        _box_search(user, 'עון אללה')
        await _wait(lambda: friedman.expression.called)
        await _wait(lambda: False, timeout=0.3)
        assert not _with_class(user, friedman_panel.EXPRESSION_CLASS)

    _run(driver, dictionary, friedman)


def test_a_word_found_only_in_a_quotation_is_listed_under_its_unit_with_the_word_marked():
    place = FriedmanPlace(SCOPE_QUOTATION, 1, tuple(QUOTE.quote.split()), (0,),
                          (FriedmanComponent('נתכלם', 'נתכלמ', 'נתכלם', 'נתכלמ', 'word'),), QUOTE)
    quoted = FriedmanLookup('נתכלם', 'נתכלמ', ('נתכלמ',), (), False, STATUS, CREDIT,
                            quotes=(FriedmanExpressionHit(ENTRY, None, (place,), 3),), more_quotes=2)
    dictionary, friedman = _dictionary(), _friedman()
    friedman.lookup.side_effect = lambda word: quoted

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'נתכלם'})
        await _wait_until(lambda: _sections(user))
        assert ('friedman', 'Friedman', True) in _tabs(user), 'a quotation is an answer: the tab is not greyed'
        assert len(_with_class(user, friedman_panel.QUOTES_CLASS)) == 1
        texts = _texts(user)
        assert friedman_panel.EXAMPLES_LABEL in texts and 'עַוְן' in texts and QUOTE.translation in texts
        assert '+2 more places in this unit' in texts
        assert any(isinstance(e, ui.button) and e.text.startswith('+2 more entries') for e in _all_elements(user))
        assert friedman_panel.NOTHING not in texts and friedman_panel.STATUS_NOTE in texts and CREDIT in texts
        assert any(friedman_panel.HIT_MARK_CLASS in html and 'נתכלם' in html for html in _marked_html(user))

    _run(driver, dictionary, friedman)


def test_partial_component_match_keeps_the_full_selection_and_names_the_subset():
    full = 'נתכלם בעון אללה'
    related = _expression('עון אללה', DEFAULT_GAP)
    answer = replace(related, text=full, words=tuple(full.split()), hits=(), related=(related,))
    dictionary, friedman = _dictionary(), _friedman()
    friedman.answer.side_effect = lambda *args, **kwargs: answer

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': full})
        await _wait_until(lambda: _with_class(user, 'dict-related-components'))
        assert _shown_word(user) == [full]
        assert 'No match for all selected words. Related components:' in _texts(user)
        assert 'עון אללה' in _texts(user) and QUOTE.translation in _texts(user)

    _run(driver, dictionary, friedman)


def test_more_quotation_results_append_without_replacing_the_query():
    hit = _expression('עון אללה', DEFAULT_GAP).hits[-1]
    quoted = FriedmanLookup('נתכלם', 'נתכלמ', ('נתכלמ',), (), False, STATUS, CREDIT,
                            quotes=(hit,), more_quotes=1, quote_offset=5)
    extra = replace(hit, unit=_unit('extra', 'entry', 'נוסף', 'additional sense'))
    dictionary, friedman = _dictionary(), _friedman()
    friedman.lookup.side_effect = lambda word: quoted
    friedman.quotation_page.return_value = ((extra,), 0)

    async def driver(user):
        await _open(user)
        await _turn_on(user)
        _fire(user, _popover(user), blau_panel.LOOKUP_EVENT, {'word': 'נתכלם'})
        await _wait_until(lambda: _sections(user))
        [button] = [e for e in _all_elements(user) if isinstance(e, ui.button) and 'dict-more-quotes-button' in e.classes]
        _click(user, button)
        await _wait_until(lambda: 'additional sense' in _texts(user))
        friedman.quotation_page.assert_called_once_with(quoted, 5)
        assert _shown_word(user) == ['נתכלם'] and ENTRY.definition in _texts(user)
        assert not button.visible

    _run(driver, dictionary, friedman)

