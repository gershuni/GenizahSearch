# -*- coding: utf-8 -*-
"""M. A. Friedman's Judeo-Arabic dictionary on /browse, RENDERED, in its own tab of the dictionary
panel (the private Friedman plan, step 3).

web/components/blau_panel.py calls ``render_friedman`` in Friedman's tab of the dictionary popover
(``render_friedman_empty`` when the dictionary has nothing for the word), with what
``shared.friedman_service`` answered for the same word. This module draws that answer and decides
nothing itself: which units a word reaches, in what order, by what route (the word, a peeled stem,
the book's index of forms) and how many, are the service's.

What is drawn: the status line first (the text is a candidate read from the author's PDFs by
their typography, not yet reviewed), then one card per unit the word reaches: its kind (entry,
sub-entry, verb stem), its head as printed (with the vowels), the binyan, how it was reached, the
entry it belongs to when it is a sub-entry, the definition, the references, the quotations with
their translations and locators, the sub-entries and verb stems under an entry, and the link to
the original page in the author's viewer; and on every section the author's credit line verbatim,
linked to the host page. A section without the credit line does not render.

The expression search (plan step 4): with several words in the box or selected, the service
answers ``FriedmanExpression`` and ``render_friedman_expression`` draws it: the status line, the
gap control (printed words allowed between two of the words; a change runs the search again
through ``on_gap``), then one card per unit with every place that holds the expression, the
matched words marked in the quotation or in the head, and, when a word was reached by a clitic
alternative, which printed word carried which typed word. ``render_friedman_expression_empty``
keeps the gap control when nothing was found, so the reader can widen it.

The gap control, the marks on the matched words and the typed-word/printed-word pairs are
web/components/expression_panel.py's, shared with Blau's tab; the class names and strings below
are the same literals (tests/test_friedman_panel.py pins them equal).

Every class here is styled in web/static/common.css under ``.gs-blau`` (the popover's scope).
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Sequence

from nicegui import ui

from shared.friedman_service import (
    SCOPE_HEAD, SCOPE_QUOTATION, FriedmanExpression, FriedmanExpressionHit, FriedmanLookup, FriedmanMatch,
    FriedmanPlace, FriedmanUnit,
)
from web.components import expression_panel
from web.translations import tr

logger = logging.getLogger(__name__)

#: Marker classes, scoped so a render-smoke check cannot pass on another element's text.
SECTION_CLASS = 'friedman-section'
EMPTY_SECTION_CLASS = 'friedman-none'
STATUS_CLASS = 'friedman-status'
MATCH_CLASS = 'friedman-match'
VIA_CLASS = 'friedman-via'
QUOTE_CLASS = 'friedman-quote'
CHILD_CLASS = 'friedman-child'
CREDIT_CLASS = 'friedman-credit'
#: The expression search: its section, and (expression_panel's literals) one place holding the
#: expression, the typed-word/printed-word pairs, the gap control, the mark on a matched word.
EXPRESSION_CLASS = 'friedman-expression'
PLACE_CLASS = 'expr-place'
COMPONENTS_CLASS = 'expr-components'
GAP_CLASS = 'expr-gap'
HIT_MARK_CLASS = 'expr-hit'

TITLE = "Friedman's dictionary"
#: The expression search's strings (approved 2026-11-08; expression_panel's literals, shared with Blau's).
NOTHING_TOGETHER = 'The words were not found together in the dictionary.'
GAP_LABEL = 'words allowed between'
MATCHED_LABEL = 'Matched words'
IN_HEAD = 'in the headword'
MORE_PLACES = 'more places in this unit'
#: The same words as the other tabs when a dictionary has nothing (one string, one translation).
NOTHING = 'Nothing in the dictionary for this word.'
#: The sidecar is a candidate (its own ``meta.status``); the reader is told on every section.
STATUS_NOTE = ("Read by machine from the dictionary's PDF files and not checked; it may contain errors. "
               "Open the linked page to verify.")
KIND_LABELS = {'entry': 'entry', 'subentry': 'sub-entry', 'binyan': 'verb stem'}
VIA_LABELS = {'head': 'found by the word itself', 'stem': 'found by its base form',
              'index': "found through the book's index of forms"}
OPEN_PAGE = 'Open the original page'
UNDER = 'under'
MORE_CHILDREN = 'more sub-entries and verb stems on the page'
MORE_QUOTATIONS = 'more quotations on the page'
MORE_MATCHES = 'more units reached; narrow the word'
#: The host page of the dictionary (the author's viewer), linked from the credit.
SITE_URL = 'https://ja.genizah.org/JA_Dictionary_pdfjs/JA_Dictionary.html'


def _attr(value: Any) -> str:
    return str(value or '').replace('"', "'")


def _rtl(text: str):
    return ui.label(text).props('dir="rtl"')


def _head_label(unit: FriedmanUnit) -> str:
    """The head as the book prints it: in braces when the book lists the entry only to reject it."""
    return f'{{{unit.head_text}}}' if 'braced_head' in unit.flags else unit.head_text


def _braced_words(unit: FriedmanUnit, words: Sequence[str]) -> tuple[str, ...]:
    """The head's words as the book prints them: the braces of a rejected entry on the first and last."""
    if 'braced_head' not in unit.flags or not words:
        return tuple(words)
    inner = list(words)
    inner[0] = '{' + inner[0]
    inner[-1] = inner[-1] + '}'
    return tuple(inner)


def _render_head(unit: FriedmanUnit, main: bool, marked: Optional[FriedmanPlace] = None) -> None:
    with ui.element('div').classes('entry'):
        ui.label(tr(KIND_LABELS.get(unit.kind, unit.kind))).classes('kind')
        if marked is not None and marked.words:   # the printed head, in its own order, the matched words marked
            expression_panel.render_marked(_braced_words(unit, marked.words), marked.positions,
                                           'name' + ('' if main else ' sub'))
        elif unit.head_text:
            _rtl(_head_label(unit)).classes('name' + ('' if main else ' sub'))
        if unit.binyan:
            ui.label(unit.binyan).classes('binyan').props(f'title="{_attr(tr("binyan"))}"')
        if main and unit.printed_page:
            ui.label(f'{tr("p.")} {unit.printed_page}').classes('page')
        if main and unit.pdf_url:
            link = ui.link(tr(OPEN_PAGE), unit.pdf_url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr(OPEN_PAGE))}"')


def _render_quotation(n: int, quote: Any) -> None:
    with ui.element('div').classes(f'{QUOTE_CLASS} quotation'):
        if quote.quote:
            _rtl(quote.quote).classes('ja')
        if quote.translation:
            _rtl(quote.translation).classes('gloss he')
        if quote.foreign:
            ui.label(quote.foreign).props('dir="rtl" lang="ar"').classes('ar')
        if quote.locator:
            _rtl(quote.locator).classes('locator')


def _render_child(child: FriedmanUnit) -> None:
    with ui.element('div').classes(f'{CHILD_CLASS} sense'):
        _render_head(child, main=False)
        if child.definition:
            _rtl(child.definition).classes('gloss he')
        if child.quotations:
            _render_quotation(1, child.quotations[0])
            if child.quotations_total > 1:
                ui.label(f'+{child.quotations_total - 1} {tr(MORE_QUOTATIONS)}').classes('more')


def _render_match(match: FriedmanMatch) -> None:
    unit = match.unit
    with ui.element('div').classes(f'{MATCH_CLASS} card'):
        _render_head(unit, main=True)
        with ui.element('div').classes(f'{VIA_CLASS} via'):
            ui.label(tr(VIA_LABELS.get(match.via, match.via))).classes('via-label')
            if match.matched and match.via != 'head':
                _rtl(match.matched)
        if match.parent is not None and match.parent.head_text:
            with ui.element('div').classes('matched-key'):
                ui.label(tr(UNDER)).classes('matched-label')
                _rtl(_head_label(match.parent))
        if unit.definition:
            _rtl(unit.definition).classes('gloss he definition')
        if unit.references:
            _rtl(unit.references).classes('references')
        for n, quote in enumerate(unit.quotations, 1):
            _render_quotation(n, quote)
        if unit.quotations_total > len(unit.quotations):
            ui.label(f'+{unit.quotations_total - len(unit.quotations)} {tr(MORE_QUOTATIONS)}').classes('more')
        for child in match.children:
            _render_child(child)
        if match.children_total > len(match.children):
            ui.label(f'+{match.children_total - len(match.children)} {tr(MORE_CHILDREN)}').classes('more')


def _render_credit(credit: str, lang: str) -> None:
    """The author's credit line verbatim (the source asks for it), linked to the host page."""
    with ui.element('div').classes(f'{CREDIT_CLASS} credit'):
        ui.link(credit, SITE_URL, new_tab=True).props('dir="rtl"').classes('credit-he')


def render_friedman_empty(word: str, *, titled: bool = True) -> None:
    """Friedman's tab when the dictionary has nothing for ``word`` (or no word was typed yet)."""
    with ui.element('div').classes(f'{EMPTY_SECTION_CLASS} friedman'):
        if titled:
            ui.label(tr(TITLE)).classes('friedman-title')
        if word:
            ui.label(tr(NOTHING)).classes('empty')


def _render_gap(result: FriedmanExpression, on_gap: Optional[Callable[[int, str], Any]]) -> None:
    """The gap control (expression_panel's): a change runs the search again through ``on_gap``."""
    expression_panel.render_gap(result.gap, result.text, on_gap)


def _render_place(place: FriedmanPlace) -> None:
    """One quotation (its words, the matched ones marked, its translation and locator) or the
    head; then, when a word was reached by a clitic alternative, which printed word carried it."""
    with ui.element('div').classes(f'{PLACE_CLASS} place'):
        if place.scope == SCOPE_QUOTATION and place.quotation is not None:
            with ui.element('div').classes(f'{QUOTE_CLASS} quotation'):
                expression_panel.render_marked(place.words, place.positions, 'ja')
                if place.quotation.translation:
                    _rtl(place.quotation.translation).classes('gloss he')
                if place.quotation.foreign:
                    ui.label(place.quotation.foreign).props('dir="rtl" lang="ar"').classes('ar')
                if place.quotation.locator:
                    _rtl(place.quotation.locator).classes('locator')
        else:
            ui.label(tr(IN_HEAD)).classes('where')
        expression_panel.render_components(place)


def _render_hit(hit: FriedmanExpressionHit) -> None:
    unit = hit.unit
    with ui.element('div').classes(f'{MATCH_CLASS} card'):
        _render_head(unit, main=True, marked=next((p for p in hit.places if p.scope == SCOPE_HEAD), None))
        if hit.parent is not None and hit.parent.head_text:
            with ui.element('div').classes('matched-key'):
                ui.label(tr(UNDER)).classes('matched-label')
                _rtl(_head_label(hit.parent))
        if unit.definition:
            _rtl(unit.definition).classes('gloss he definition')
        for place in hit.places:
            _render_place(place)
        if hit.places_total > len(hit.places):
            ui.label(f'+{hit.places_total - len(hit.places)} {tr(MORE_PLACES)}').classes('more')


def render_friedman_expression_empty(result: FriedmanExpression, *, on_gap: Optional[Callable[[int, str], Any]] = None,
                                     titled: bool = True) -> None:
    """Friedman's tab when the words were not found together: the gap control stays, so the
    reader can allow more words between them."""
    with ui.element('div').classes(f'{EMPTY_SECTION_CLASS} {EXPRESSION_CLASS} friedman'):
        if titled:
            ui.label(tr(TITLE)).classes('friedman-title')
        _render_gap(result, on_gap)
        if result.text:
            ui.label(tr(NOTHING_TOGETHER)).classes('empty')


def render_friedman_expression(result: Optional[FriedmanExpression], *, credit: str, lang: str = 'he',
                               on_gap: Optional[Callable[[int, str], Any]] = None, titled: bool = True) -> bool:
    """Draw an expression search's answer into the current container. Returns False, drawing
    nothing, when there is no result, no hit, or no credit line."""
    if result is None or not result.found:
        return False
    credit = (credit or '').strip()
    if not credit:
        logger.warning('friedman_panel: no credit line; the expression %r is not shown', result.text)
        return False
    with ui.element('div').classes(f'{SECTION_CLASS} {EXPRESSION_CLASS} friedman'):
        if titled:
            ui.label(tr(TITLE)).classes('friedman-title')
        ui.label(tr(STATUS_NOTE)).classes(f'{STATUS_CLASS} disclaimer')
        _render_gap(result, on_gap)
        for hit in result.hits:
            _render_hit(hit)
        if result.truncated:
            ui.label(f'+ {tr(MORE_MATCHES)}').classes('more')
        _render_credit(credit, lang)
    return True


def render_friedman(result: Optional[FriedmanLookup], *, credit: str, lang: str = 'he',
                    on_search: Optional[Callable[[str], Any]] = None, titled: bool = True) -> bool:
    """Draw ``result`` into the current container. Returns False, drawing nothing, when there is
    no result, no match, or no credit line (no dictionary text without its credit).
    ``titled=False`` leaves out the section's title (the panel's head already names it)."""
    if result is None or not result.found:
        return False
    credit = (credit or '').strip()
    if not credit:
        logger.warning('friedman_panel: no credit line; the lookup of %r is not shown', result.word)
        return False
    with ui.element('div').classes(f'{SECTION_CLASS} friedman'):
        if titled:
            ui.label(tr(TITLE)).classes('friedman-title')
        ui.label(tr(STATUS_NOTE)).classes(f'{STATUS_CLASS} disclaimer')
        for match in result.matches:
            _render_match(match)
        if result.truncated:
            ui.label(f'+ {tr(MORE_MATCHES)}').classes('more')
        _render_credit(credit, lang)
    return True
