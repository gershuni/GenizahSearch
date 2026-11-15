# -*- coding: utf-8 -*-
"""The rest of a list behind one button that draws it in place (owner, 2026-11-11: a dictionary's
senses, quotations and sub-entries are never only on the site or the PDF page; they may be hidden
behind a button, and a click shows them).

Blau's cards (web/components/blau_panel.py: the senses after the first few) and Friedman's
(web/components/friedman_panel.py: the quotations, a sub-entry's quotations after its first, and the
sub-entries and stem paragraphs) call ``render_more`` with what they did not draw: the button says
how many, a click replaces it with them. Nothing is fetched on the click: the services already
carry everything (``MAX_SENSES``, ``MAX_CHILDREN``, ``MAX_QUOTATIONS`` are bounds far above the
largest entry). The class lives in web/static/common.css under ``.gs-blau``.
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from nicegui import ui

from web.translations import tr

#: The holder of the rest, and the button inside it until it is pressed.
MORE_CLASS = 'dict-more'
BUTTON_CLASS = 'dict-show-more'
#: The owner's wording, 2026-11-11 (Hebrew "הצג", not "הצגת"): ``{n}`` is how many the click shows.
MORE_SENSES = 'Show {n} more senses'
MORE_QUOTATIONS = 'Show {n} more quotations'
MORE_CHILDREN = 'Show {n} more sub-entries and verb stems'
#: Lane's root card: its other entries (owner, 2026-11-11; the whole text of a cut entry is
#: web/components/lane_panel.py's FULL_ENTRY).
MORE_ENTRIES = 'Show {n} more entries'


def render_more(items: Sequence[Any], draw: Callable[[Any], Any], label: str) -> Optional[ui.element]:
    """``items`` behind one button, in the current container: the button reads ``label`` with
    ``{n}`` the count; a press draws each item with ``draw`` in the button's place, in order.
    Nothing (None) for no items. One item is drawn as it is: a button would hide no more than it
    takes, and "Show 1 more quotations" reads badly in both languages."""
    items = tuple(items)
    if not items:
        return None
    holder = ui.element('div').classes(MORE_CLASS)
    if len(items) == 1:
        with holder:
            draw(items[0])
        return holder

    def show(_e: Any = None) -> None:
        holder.clear()
        with holder:
            for item in items:
                draw(item)

    with holder:
        ui.button(tr(label).format(n=len(items)), on_click=show).props('flat dense no-caps').classes(
            f'{BUTTON_CLASS} more')
    return holder
