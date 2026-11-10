# -*- coding: utf-8 -*-
"""What the dictionaries' expression searches draw alike on /browse: the gap control, the printed
words with the matched ones marked, and the typed-word/printed-word pairs.

Friedman's tab (web/components/friedman_panel.py) and Blau's (web/components/blau_panel.py) show
the same kind of answer -- ``shared.ja_expression``'s places, each a quotation or a name holding
the expression -- and draw these three parts through this module so a reader meets one control
and one mark in both. Which places a dictionary has, how a hit is headed and what it links to
stay the dictionary's own. Every class here is styled in web/static/common.css under ``.gs-blau``.
"""
from __future__ import annotations

import html
from typing import Any, Callable, Optional, Sequence

from nicegui import ui

from shared.ja_expression import MAX_GAP
from web.translations import tr

#: The gap control, the mark on a matched word, one place and its typed-word/printed-word pairs.
GAP_CLASS = 'expr-gap'
HIT_MARK_CLASS = 'expr-hit'
PLACE_CLASS = 'expr-place'
COMPONENTS_CLASS = 'expr-components'

#: The expression search's strings (owner-approved for Friedman's tab, 2026-11-08; Blau's reuses them).
NOTHING_TOGETHER = 'The words were not found together in the dictionary.'
GAP_LABEL = 'words allowed between'
MATCHED_LABEL = 'Matched words'
MORE_PLACES = 'more places in this unit'


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
            'dense outlined options-dense').classes('gap-select')
        select.props(f'aria-label="{_attr(tr(GAP_LABEL))}"')


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
