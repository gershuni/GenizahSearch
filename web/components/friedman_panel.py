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
    SCOPE_HEAD, SCOPE_QUOTATION, VIA_BORROWED, VIA_CHOSEN, FriedmanExpression, FriedmanExpressionHit, FriedmanLookup, FriedmanMatch,
    FriedmanPlace, FriedmanUnit,
)
from web.components import expression_panel, show_more, stem_buttons
from web.translations import tr

logger = logging.getLogger(__name__)

#: Marker classes, scoped so a render-smoke check cannot pass on another element's text.
SECTION_CLASS = 'friedman-section'
EMPTY_SECTION_CLASS = 'friedman-none'
STATUS_CLASS = 'friedman-status'
MATCH_CLASS = 'friedman-match'
VIA_CLASS = 'friedman-via'
SEE_CLASS = 'friedman-see'
REFERRED_CLASS = 'friedman-referred'
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
#: A one-word lookup's quotations (FriedmanLookup.quotes): the section, and its label (Blau's words).
QUOTES_CLASS = 'friedman-quotes'
EXAMPLES_LABEL = 'Also in the examples under'
#: The same words as Blau's tab when the dictionary has nothing (one string, one translation; step B
#: of the dictionary lookup plan).
NOTHING = 'No match was retrieved for this spelling. Try another reading or the root.'
#: The tab's name for the stem buttons' report (blau_panel.FRIEDMAN_TAB, equal by test).
TAB = 'friedman'
#: Beside a unit whose entry is not spelled with the dot the looked-up word carries (Blau's words).
DOT_MISMATCH = 'spelled without the dot'
#: The sidecar is a candidate (its own ``meta.status``); the reader is told on every section.
STATUS_NOTE = ("Read by machine from the dictionary's PDF files and not checked; it may contain errors. "
               "Open the linked page to verify.")
KIND_LABELS = {'entry': 'entry', 'subentry': 'sub-entry', 'binyan': 'verb stem'}
#: 'verb_form' (VIA_VERB_FORM, equal by test): a match found by the generated forms of a stem its entry
#: lists (PR 3), followed by the form, the root and the stems; the same
#: words as blau_panel.VERB_FORM (equal by test). Literals: tests/test_friedman_panel.py reads them.
VIA_LABELS = {'head': 'found by the word itself', 'stem': 'found by its base form',
              'index': "found through the book's index of forms", 'verb_form': 'found by its verb form'}
OPEN_PAGE = 'Open the original page'
UNDER = 'under'
#: A head the book prints without its root (``FriedmanUnit.entry_root``): the root drawn before it, and
#: the printed construction after the stem.
ROOT_CLASS = 'entry-root'
CONSTRUCTION_CLASS = 'construction'
#: Only what a match does not carry (beyond MAX_CHILDREN / MAX_QUOTATIONS; none on the local sidecar)
#: is pointed to on the page; the rest opens in place (web/components/show_more.py).
MORE_CHILDREN = 'more sub-entries and verb stems on the page'
MORE_QUOTATIONS = 'more quotations on the page'
#: What a match shows before its "Show N more" buttons: quotations, sub-entries and stem paragraphs.
QUOTATIONS_SHOWN = 8
CHILDREN_SHOWN = 30
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
    """A unit's head: its kind, the head as printed, its stem. A head the book prints without its root
    (``FriedmanUnit.entry_root``: "VIII -הُ" under שכו) reads root, stem, then what the book prints
; under its own entry (``main`` False) the entry's head names the root."""
    construction = bool(unit.entry_root)
    head_class = (CONSTRUCTION_CLASS if construction else 'name') + ('' if main else ' sub')
    with ui.element('div').classes('entry'):
        ui.label(tr(KIND_LABELS.get(unit.kind, unit.kind))).classes('kind')
        if construction and main:
            _rtl(unit.entry_root).classes(f'name {ROOT_CLASS}')
        if construction and unit.binyan:
            _render_binyan(unit)
        if marked is not None and marked.words:   # the printed head, in its own order, the matched words marked
            expression_panel.render_marked(_braced_words(unit, marked.words), marked.positions, head_class)
        elif unit.head_text:
            _rtl(_head_label(unit)).classes(head_class)
        if unit.binyan and not construction:
            _render_binyan(unit)
        if main and unit.printed_page:
            ui.label(f'{tr("p.")} {unit.printed_page}').classes('page')
        if main and unit.pdf_url:
            link = ui.link(tr(OPEN_PAGE), unit.pdf_url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr(OPEN_PAGE))}"')


def _render_binyan(unit: FriedmanUnit) -> None:
    ui.label(unit.binyan).classes('binyan').props(f'title="{_attr(tr("binyan"))}"')


def _foreign_apart(quote: Any) -> bool:
    """Is the quotation's Arabic text to be drawn on its own line? Not when the quotation or its
    translation already prints it in place."""
    printed = set(f"{quote.quote} {quote.translation}".split())
    return bool(quote.foreign) and not set(quote.foreign.split()) <= printed


def _render_quotation(n: int, quote: Any) -> None:
    with ui.element('div').classes(f'{QUOTE_CLASS} quotation'):
        if quote.quote:
            _rtl(quote.quote).classes('ja')
        if quote.translation:
            _rtl(quote.translation).classes('gloss he')
        if _foreign_apart(quote):
            ui.label(quote.foreign).props('dir="rtl" lang="ar"').classes('ar')
        if quote.locator:
            _rtl(quote.locator).classes('locator')


def _render_child(child: FriedmanUnit) -> None:
    with ui.element('div').classes(f'{CHILD_CLASS} sense'):
        _render_head(child, main=False)
        if child.definition:
            _rtl(child.definition).classes('gloss he')
        if child.references:
            _rtl(child.references).classes('references')
        if child.quotations:
            # Its first quotation; the rest on a click, in place.
            _render_quotation(1, child.quotations[0])
            show_more.render_more(child.quotations[1:], lambda quote: _render_quotation(0, quote),
                                  show_more.MORE_QUOTATIONS)
        _render_beyond(child.quotations_total - len(child.quotations), tr(MORE_QUOTATIONS))


def _render_beyond(count: int, text: str) -> None:
    """What a unit has beyond the bound a match carries (``MAX_QUOTATIONS``, ``MAX_CHILDREN``; none
    on the local sidecar): counted, and on the PDF page (``text``, translated)."""
    if count > 0:
        ui.label(f'+{count} {text}').classes('more')


def _parent_label(unit: FriedmanUnit, parent: FriedmanUnit) -> str:
    """A construction belongs to its root and numbered paragraph, not the first construction."""
    if unit.entry_root:
        label = ' '.join(filter(None, (unit.entry_root, unit.governing_binyan)))
        return f'{{{label}}}' if 'braced_head' in parent.flags else label
    return _head_label(parent)


def _render_see(unit: FriedmanUnit, on_search: Optional[Callable[[str], Any]]) -> None:
    """The entries the unit's references send the reader to ("ראה הערך חפי"), each a button that
    looks it up (``FriedmanUnit.see``); the words themselves stand in the references above."""
    if on_search is None or not unit.see:
        return
    with ui.element('div').classes(f'{SEE_CLASS} chips'):
        for name in unit.see:
            ui.button(name, on_click=lambda _e=None, text=name: on_search(text)).props(
                'flat dense no-caps dir="rtl"').classes('chip')


def _render_match(match: FriedmanMatch, on_stem: Optional[Callable[[str, str], Any]] = None,
                  tab: str = TAB, on_search: Optional[Callable[[str], Any]] = None) -> None:
    unit = match.unit
    with ui.element('div').classes(f'{MATCH_CLASS} card' + (' stem-chosen' if match.stem else '')):
        _render_head(unit, main=True)
        expression_panel.render_match_details(match)
        if match.dot_mismatch:
            ui.label(tr(DOT_MISMATCH)).classes('flag dot-mismatch')
        # The stems the unit's entry lists: a press opens that stem's paragraph.
        stem_buttons.render_stems(match.stems, match.stem, match.root, tab, on_stem)
        # A match for a root another dictionary gave (VIA_BORROWED) has no line at all: naming that
        # dictionary was redundant. A chosen stem is marked on its button.
        if match.via not in (VIA_CHOSEN, VIA_BORROWED):
            with ui.element('div').classes(f'{VIA_CLASS} via'):
                ui.label(tr(VIA_LABELS.get(match.via, match.via))).classes('via-label')
                if match.matched and match.via != 'head':
                    _rtl(match.matched)
        if match.parent is not None and match.parent.head_text:
            with ui.element('div').classes('matched-key'):
                ui.label(tr(UNDER)).classes('matched-label')
                _rtl(_parent_label(unit, match.parent))
        if unit.definition:
            _rtl(unit.definition).classes('gloss he definition')
        if unit.references:
            _rtl(unit.references).classes('references')
        _render_see(unit, on_search)
        # The units the references send the reader to, as the book prints them.
        for referred in match.referred:
            with ui.element('div').classes(REFERRED_CLASS):
                _render_child(referred)
        # The first quotations, sub-entries and stem paragraphs; the rest on a click, in place
        # (never only on the PDF page). A chosen stem that is the entry's own
        # paragraph shows the entry's other units only on the click.
        for n, quote in enumerate(unit.quotations[:QUOTATIONS_SHOWN], 1):
            _render_quotation(n, quote)
        show_more.render_more(unit.quotations[QUOTATIONS_SHOWN:], lambda quote: _render_quotation(0, quote),
                              show_more.MORE_QUOTATIONS)
        _render_beyond(unit.quotations_total - len(unit.quotations), tr(MORE_QUOTATIONS))
        first = 0 if match.stem else CHILDREN_SHOWN
        for child in match.children[:first]:
            _render_child(child)
        show_more.render_more(match.children[first:], _render_child, show_more.MORE_CHILDREN)
        _render_beyond(match.children_total - len(match.children), tr(MORE_CHILDREN))


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
        if place.scope in (SCOPE_QUOTATION, "arabic_quotation") and place.quotation is not None:
            with ui.element('div').classes(f'{QUOTE_CLASS} quotation'):
                expression_panel.render_marked(place.words, place.positions, 'ja')
                if place.quotation.translation:
                    _rtl(place.quotation.translation).classes('gloss he')
                if _foreign_apart(place.quotation) and place.scope != "arabic_quotation":
                    ui.label(place.quotation.foreign).props('dir="rtl" lang="ar"').classes('ar')
                if place.quotation.locator:
                    _rtl(place.quotation.locator).classes('locator')
        elif place.scope == "construction":
            ui.label(tr("Construction with inherited verb")).classes('where')
            expression_panel.render_marked(place.words, place.positions, 'ja')
        else:
            ui.label(tr(IN_HEAD)).classes('where')
        expression_panel.render_components(place)


def _render_hit(hit: FriedmanExpressionHit) -> None:
    unit = hit.unit
    with ui.element('div').classes(f'{MATCH_CLASS} card'):
        _render_head(unit, main=True, marked=next((p for p in hit.places if p.scope == SCOPE_HEAD), None))
        if getattr(hit, 'possible_spelling', False):
            ui.label(tr('Possible spelling')).classes('where dict-evidence')
        if hit.parent is not None and hit.parent.head_text:
            with ui.element('div').classes('matched-key'):
                ui.label(tr(UNDER)).classes('matched-label')
                _rtl(_parent_label(unit, hit.parent))
        if unit.definition:
            _rtl(unit.definition).classes('gloss he definition')
        if unit.references:
            _rtl(unit.references).classes('references')
        for place in hit.places:
            _render_place(place)
        shown = {place.quotation.n for place in hit.places if place.quotation is not None}
        remaining = tuple(q for q in unit.quotations if q.n not in shown)
        show_more.render_more(remaining, lambda q: _render_quotation(q.n, q), show_more.EXAMPLES, collapsed=True)
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
                               on_gap: Optional[Callable[[int, str], Any]] = None, titled: bool = True,
                               on_more_entries=None) -> bool:
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
        expression_panel.render_automatic_note()
        ui.label(tr(STATUS_NOTE)).classes(f'{STATUS_CLASS} disclaimer')
        _render_gap(result, on_gap)
        for hit in result.hits:
            _render_hit(hit)
        expression_panel.render_related(result, _render_hit)
        expression_panel.render_more_entries(result.hits, result.truncated, on_more_entries,
            _render_hit, lambda hit: hit.unit.id, 20)
        _render_credit(credit, lang)
    return True


def render_friedman(result: Optional[FriedmanLookup], *, credit: str, lang: str = 'he',
                    on_search: Optional[Callable[[str], Any]] = None, titled: bool = True,
                    on_stem: Optional[Callable[[str, str], Any]] = None, tab: str = TAB,
                    on_suggest: Optional[Callable[[str, str], Any]] = None, chips: tuple[str, str] = ('', ''),
                    on_more_entries=None) -> bool:
    """Draw ``result`` into the current container. Returns False, drawing nothing, when there is
    no result, no match, or no credit line (no dictionary text without its credit).
    ``titled=False`` leaves out the section's title (the panel's head already names it). A stem
    button reports ``tab`` and what to look up to ``on_stem``; a typed stem the root does not
    list is said above its matches."""
    if result is None or not result.found:
        return False
    credit = (credit or '').strip()
    if not credit:
        logger.warning('friedman_panel: no credit line; the lookup of %r is not shown', result.word)
        return False
    with ui.element('div').classes(f'{SECTION_CLASS} friedman'):
        if titled:
            ui.label(tr(TITLE)).classes('friedman-title')
        expression_panel.render_automatic_note()
        ui.label(tr(STATUS_NOTE)).classes(f'{STATUS_CLASS} disclaimer')
        if result.stem_missing:
            stem_buttons.render_not_listed(result.stem)
        # The other roots the word's verb forms give, beside the matches the word itself reached; a
        # press opens one and the chips stay (``chips``: the word, the chip shown now).
        stem_buttons.render_suggestions(result.suggestions, tab, on_suggest or on_stem, *chips)
        # The units that print the word with the words beside it in the text, first.
        if not result.context_quoted:
            for found in result.context:
                expression_panel.render_context(found, _render_hit)
        for match in result.matches:
            _render_match(match, on_stem, tab, on_search)
        expression_panel.render_more_entries(result.matches, result.truncated, on_more_entries,
            lambda match: _render_match(match, on_stem, tab, on_search), lambda match: match.unit.id, 12)
        if result.context_quoted:
            for found in result.context:
                expression_panel.render_context(found, _render_hit)
        if result.quotes:   # the units whose quotations or head print the word, the word marked
            with ui.element('div').classes(f'{QUOTES_CLASS} examples'):
                ui.label(tr(EXAMPLES_LABEL)).classes('examples-label')
                for hit in result.quotes:
                    _render_hit(hit)
                if result.more_quotes:
                    ui.label(f'+{result.more_quotes} {tr("more entries")}').classes('more')
        _render_credit(credit, lang)
    return True
