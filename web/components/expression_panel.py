# -*- coding: utf-8 -*-
"""What the dictionaries' expression searches draw alike on /browse: the gap control, the printed
words with the matched ones marked, and the typed-word/printed-word pairs.

Friedman's tab (web/components/friedman_panel.py) and Blau's (web/components/blau_panel.py) show
the same kind of answer -- ``shared.ja_expression``'s places, each a quotation or a name holding
the expression -- and draw these three parts through this module so a reader meets one control
and one mark in both. The line every dictionary tab opens with, Lane's too, and the expressions a
clicked word makes with the words beside it are drawn here as well. Which places a dictionary has, how a hit is headed and what it links to
stay the dictionary's own. Every class here is styled in web/static/common.css under ``.gs-blau``.
"""
from __future__ import annotations

import html
import logging
from typing import Any, Callable, Optional, Sequence

from nicegui import ui

from shared.blau_normalize import BROKEN_PLURAL
from shared.ja_expression import MAX_GAP, MAX_EXPRESSION_WORDS
from web.translations import tr

#: The gap control, the mark on a matched word, one place and its typed-word/printed-word pairs.
GAP_CLASS = 'expr-gap'
HIT_MARK_CLASS = 'expr-hit'
PLACE_CLASS = 'expr-place'
COMPONENTS_CLASS = 'expr-components'
#: The expressions a clicked word makes with the words beside it (``BlauLookup.context``,
#: ``FriedmanLookup.context``), above the word's own answer.
CONTEXT_CLASS = 'expr-context'

#: The expression search's strings (owner-approved for Friedman's tab, 2026-11-08; Blau's reuses them).
NOTHING_TOGETHER = 'The words were not found together in the dictionary.'
GAP_LABEL = 'words allowed between'
MATCHED_LABEL = 'Matched words'
MORE_PLACES = 'more places in this unit'
#: Heads the units that print a clicked word with the words beside it.
CONTEXT_LABEL = 'As an expression with the words beside it'
QUOTED_CONTEXT_LABEL = 'Nearby words found together in a quotation'
#: The line every dictionary tab opens with, Blau's, Friedman's and Lane's: the lookup is automatic
#: (the limitations should be clear).
AUTOMATIC_NOTE = ("Found automatically from the word's letters: an entry may belong to another word"
                  ", and the word's own entry may be missing. Check the dictionary.")
AUTOMATIC_NOTE_CLASS = 'dict-automatic-note'


def _attr(value: Any) -> str:
    return str(value or '').replace('"', "'")


def render_marked(words: Sequence[str], positions: Sequence[int], classes: str) -> None:
    """The printed words, the matched ones marked (the text escaped; the marks are the only HTML)."""
    marked = set(positions)
    parts = [f'<mark class="{HIT_MARK_CLASS}">{html.escape(word)}</mark>' if i in marked else html.escape(word)
             for i, word in enumerate(words)]
    ui.html(' '.join(parts)).props('dir="rtl"').classes(classes)


def render_gap(gap: int, text: str, on_gap: Optional[Callable[[int, str], Any]]) -> None:
    """The printed words allowed between two of the expression's words; a change runs the
    search again (``on_gap(gap, text)``). Without a handler there is nothing to change."""
    if on_gap is None:
        return
    with ui.element('div').classes(f'{GAP_CLASS} gap'):
        ui.label(tr(GAP_LABEL)).classes('gap-label')
        select = ui.select({n: str(n) for n in range(MAX_GAP + 1)}, value=gap,
                           on_change=lambda e: on_gap(e.value, text)).props(
            'dense outlined options-dense popup-content-class=dict-gap-menu').classes('gap-select')
        select.props(f'aria-label="{_attr(tr(GAP_LABEL))}"')


def render_automatic_note() -> None:
    """The line every dictionary tab opens with (``AUTOMATIC_NOTE``), above the tab's own note."""
    ui.label(tr(AUTOMATIC_NOTE)).classes(f'{AUTOMATIC_NOTE_CLASS} automatic-note')


def render_context(found: Any, render_hit: Callable[[Any], Any]) -> None:
    """One expression a clicked word makes with the words beside it (``found``: a dictionary's
    expression answer, ``.text`` and ``.hits``): its head, the words, then each hit as the
    dictionary draws it (``render_hit``)."""
    with ui.element('div').classes(f'{CONTEXT_CLASS} context'):
        with ui.element('div').classes('context-head'):
            quoted = all(place.scope in {'quotation', 'arabic_quotation', 'citation'}
                         for hit in found.hits for place in hit.places)
            ui.label(tr(QUOTED_CONTEXT_LABEL if quoted else CONTEXT_LABEL)).classes('context-label')
            ui.label(found.text).props('dir="rtl"').classes('context-words')
        for hit in found.hits:
            render_hit(hit)


def render_components(place: Any) -> None:
    """When a typed word was reached by a clitic alternative (on either side), which printed
    word carried which typed word; nothing when every word matched as itself."""
    if not any(c.via != 'word' for c in place.components):
        return
    with ui.element('div').classes(f'{COMPONENTS_CLASS} components'):
        ui.label(tr(MATCHED_LABEL)).classes('matched-label')
        for c in place.components:
            ui.label(c.word if c.via == 'word' else f'{c.word} ({c.printed})').props('dir="rtl"').classes(
                f'component {c.via}')

def render_problem(answer: Any) -> bool:
    """Operational and input limits must not look like an absent dictionary entry."""
    problem = getattr(answer, 'problem', '')
    messages = {
        'word_only': 'Lane supports single-word lookup. Use the other tabs for expressions.',
        'unavailable': 'This dictionary is currently unavailable.',
        'unsupported': 'Enter a word or expression in Hebrew or Arabic letters.',
        'limit': f'Expressions support up to {MAX_EXPRESSION_WORDS} words. Edit the full selection above.',
        'work_limit': 'This search exceeded the component limit. Reduce the gap or use fewer words.',
        'error': 'The dictionary lookup failed. Please try again.',
    }
    if problem:
        ui.label(tr(messages.get(problem, messages['error']))).classes('dict-search-problem')
        return True
    return False


def render_match_details(record: Any) -> None:
    analyses = getattr(record, 'analyses', ())
    if analyses and all('mater' in path for path in analyses):
        spelling = getattr(record, 'candidate', '') or getattr(record, 'matched', '')
        ui.label(f'{tr("Possible spelling")}: {spelling}').classes('where dict-evidence')
    elif (getattr(record, 'via', '') == 'verb_form' or getattr(record, 'verb_form', '')
          or analyses and all(BROKEN_PLURAL in path for path in analyses)):
        # A verb's form, or a noun reached by a plural's pattern alone (shared.blau_normalize).
        ui.label(tr('Possible grammatical analysis')).classes('where dict-evidence')
    elif getattr(record, 'borrowed', ()):
        ui.label(tr('Possible root from another dictionary')).classes('where dict-evidence')
    paths = [" → ".join(path) for path in getattr(record, "analyses", ()) if path]
    paths += [f"{source}: {evidence}" for source, evidence in getattr(record, "borrowed", ())]
    if paths:
        ui.icon('info_outline', size='14px').classes('dict-match-details').tooltip("; ".join(paths))


def render_related(result: Any, render_hit: Callable[[Any], Any]) -> None:
    for related in getattr(result, 'related', ()):
        with ui.element('div').classes('dict-related-components'):
            ui.label(tr('No match for all selected words. Related components:')).classes('where')
            ui.label(related.text).props('dir="rtl"').classes('context-words')
            for hit in related.hits:
                render_hit(hit)


def render_more_entries(items, remaining, fetch, render_item, identity, initial_limit):
    """Append ten further entries per press, leaving the tab, context and search box in place."""
    if not remaining:
        return
    if fetch is None:
        ui.label(f'+ {tr("more entries")}').classes('more')
        return
    extra = ui.column().classes('dict-more-entries w-full')
    seen = {identity(item) for item in items}
    limit = initial_limit
    async def more():
        nonlocal limit
        button.disable()
        try:
            next_limit = limit + 10
            entries, left = await fetch(next_limit)
            if extra.is_deleted:
                return
            with extra:
                for item in entries:
                    key = identity(item)
                    if key not in seen:
                        render_item(item)
                        seen.add(key)
            limit = next_limit
            button.set_visibility(bool(left))
            if left:
                button.set_text(f'+{left if isinstance(left, int) and not isinstance(left, bool) else ""} {tr("more entries")}')
        except Exception:
            logging.getLogger(__name__).exception('Additional dictionary entries could not be read')
            if not extra.is_deleted:
                with extra:
                    ui.label(tr('The dictionary lookup failed. Please try again.'))
        finally:
            if not button.is_deleted:
                button.enable()
    count = remaining if isinstance(remaining, int) and not isinstance(remaining, bool) else ''
    button = ui.button(f'+{count} {tr("more entries")}', on_click=more).props('flat dense').classes('dict-more-entries-button')
