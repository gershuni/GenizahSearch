# -*- coding: utf-8 -*-
"""What Blau's and Friedman's tabs draw alike for a root's verb stems on /browse: the row of stem
buttons on a card, and the line that says a typed stem is not listed under the root.

The private dictionary lookup plan, section 4.6 (owner, 2026-11-11): a card for an entry that lists
stems shows them as buttons (דלל: I, II, IV, V, X). Pressing one is the typed shortcut "דלל X": the
page looks that up in every dictionary and the card opens on that stem's paragraph; pressing the
chosen one again goes back to the root. The service decides which stems an entry has and which
paragraph a stem opens (``BlauService.root_stems``, ``FriedmanService.entry_stems``); this module
only draws them. Every class here is styled in web/static/common.css under ``.gs-blau``.
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from nicegui import ui

from shared.ja_roots import stem_label, stem_query
from web.translations import tr

#: The row of buttons, one button, the chosen one, and the not-listed line.
STEMS_CLASS = 'dict-stems'
STEM_CLASS = 'dict-stem'
CHOSEN_CLASS = 'chosen'
NOT_LISTED_CLASS = 'dict-stem-missing'
#: The row's label and each button's tooltip: the dictionary panel's existing word for a stem.
STEMS_LABEL = 'verb stem'
STEM_TITLE = 'binyan'
#: Above the root's cards when the typed stem is not listed under it (owner, 2026-11-11); the
#: stem's numeral goes in ``{stem}``.
NOT_LISTED = 'Stem {stem} is not listed under this root.'
#: At the top of a tab whose cards the word itself reached: the other roots its verb forms give, as
#: chips "root stem" (BlauLookup / FriedmanLookup ``suggestions``; owner's wording, 2026-11-11).
SUGGESTIONS_CLASS = 'dict-suggestions'
SUGGESTION_CLASS = 'dict-suggestion'
SUGGESTIONS_LABEL = 'Other possible roots'


def _attr(value: Any) -> str:
    return str(value or '').replace('"', "'")


def render_stems(stems: Sequence[int], chosen: int, root: str, tab: str,
                 on_stem: Optional[Callable[[str, str], Any]]) -> None:
    """The stems an entry lists, as buttons; ``chosen`` (0: none) is marked and goes back to the
    root. A press reports the dictionary's tab and what the box should look up ("דלל X", or the
    root alone) to ``on_stem``. Nothing is drawn without stems or without a root to look up."""
    if not stems or not root.strip():
        return
    with ui.element('div').classes(f'{STEMS_CLASS} stems'):
        ui.label(tr(STEMS_LABEL)).classes('stems-label')
        for number in stems:
            label = stem_label(number)
            if not label:
                continue
            is_chosen = number == chosen
            target = stem_query(root, 0 if is_chosen else number)
            button = ui.button(label, on_click=(lambda t=target: on_stem(tab, t)) if on_stem else None).props(
                f'flat dense no-caps size=sm aria-pressed={"true" if is_chosen else "false"} '
                f'title="{_attr(tr(STEM_TITLE))} {label}"'
            ).classes(f'{STEM_CLASS} stem' + (f' {CHOSEN_CLASS}' if is_chosen else ''))
            if on_stem is None:
                button.disable()


def render_suggestions(suggestions: Sequence[tuple[str, int]], tab: str,
                       on_stem: Optional[Callable[[str, str], Any]], word: str = '', chosen: str = '') -> None:
    """The other roots the word's verb forms give, as chips: "דלל X" (the root, and the one stem the
    word fits) or the root alone. A press looks it up in the tab, as a stem button does, and the page
    keeps drawing the same chips (owner, 2026-11-11: so the reader can check each root in turn):
    ``chosen`` is what is shown now, its chip marked; while it is not the ``word`` itself,
    a first chip with the word goes back to it, and so does the marked chip pressed again. Nothing is
    drawn without suggestions or without a lookup to report to."""
    chips = [stem_query(root, stem) for root, stem in suggestions if root.strip()]
    if not chips or on_stem is None:
        return

    def chip(label: str, is_chosen: bool) -> None:
        target = word if is_chosen and word else label
        ui.button(label, on_click=lambda t=target: on_stem(tab, t)).props(
            f'flat dense no-caps size=sm dir="rtl" aria-pressed={"true" if is_chosen else "false"}'
        ).classes(f'{SUGGESTION_CLASS} {STEM_CLASS} stem' + (f' {CHOSEN_CLASS}' if is_chosen else ''))

    with ui.element('div').classes(f'{SUGGESTIONS_CLASS} stems'):
        ui.label(tr(SUGGESTIONS_LABEL)).classes('stems-label')
        if word and chosen:   # a chip was pressed: the word itself, to go back
            chip(word, False)
        for label in chips:
            chip(label, bool(chosen) and label == chosen)


def render_not_listed(stem: int) -> None:
    """The line above a root's cards when the stem typed after it is not listed under it."""
    label = stem_label(stem)
    if label:
        ui.label(tr(NOT_LISTED).format(stem=label)).classes(f'{NOT_LISTED_CLASS} empty')
