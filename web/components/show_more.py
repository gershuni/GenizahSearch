# -*- coding: utf-8 -*-
"""The rest of a list behind one button that draws it in place (a dictionary's
senses, quotations and sub-entries are never only on the site or the PDF page; they may be hidden
behind a button, and a click shows them).

Blau's cards (web/components/blau_panel.py: the senses after the first few) and Friedman's
(web/components/friedman_panel.py: the quotations, a sub-entry's quotations after its first, and the
sub-entries and stem paragraphs) call ``render_more`` with what they did not draw: the button says
how many, a click replaces it with them. Nothing is fetched on the click: the services already
carry everything (``MAX_SENSES``, ``MAX_CHILDREN``, ``MAX_QUOTATIONS`` are bounds far above the
largest entry). ``render_details`` instead fetches one Blau sense's examples and notes on demand.
The class lives in web/static/common.css under ``.gs-blau``.
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Sequence
import logging

from nicegui import ui

from web.translations import tr

#: The holder of the rest, and the button inside it until it is pressed.
MORE_CLASS = 'dict-more'
BUTTON_CLASS = 'dict-show-more'
#: Hebrew "הצג", not "הצגת": ``{n}`` is how many the click shows.
MORE_SENSES = 'Show {n} more senses'
MORE_QUOTATIONS = 'Show {n} more quotations'
MORE_CHILDREN = 'Show {n} more sub-entries and verb stems'
#: Lane's root card: its other entries (the whole text of a cut entry is
#: web/components/lane_panel.py's FULL_ENTRY).
MORE_ENTRIES = 'Show {n} more entries'
EXAMPLES = 'Show examples ({n})'
DETAILS = 'Show examples and additional information'
DETAILS_CLASS = 'dict-sense-details'


def render_more(items: Sequence[Any], draw: Callable[[Any], Any], label: str, *, collapsed: bool = False) -> Optional[ui.element]:
    """``items`` behind one button, in the current container: the button reads ``label`` with
    ``{n}`` the count; a press draws each item with ``draw`` in the button's place, in order.
    Nothing (None) for no items. One item is drawn directly unless ``collapsed`` is requested
    with a label suitable for a single example."""
    items = tuple(items)
    if not items:
        return None
    holder = ui.element('div').classes(MORE_CLASS)
    if len(items) == 1 and not collapsed:
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


def render_details(load: Callable, draw: Callable) -> ui.element:
    """Load and draw supporting material on a click; stale results cannot redraw a new lookup."""
    holder = ui.element('div').classes(f'{MORE_CLASS} {DETAILS_CLASS}')
    busy = False
    async def show():
        nonlocal busy
        if busy:
            return
        busy = True
        button.disable()
        try:
            details = await load()
            if holder.is_deleted:
                return
            if details is None:
                raise RuntimeError('Dictionary details unavailable')
            holder.clear()
            with holder:
                draw(details)
        except Exception:
            logging.getLogger(__name__).exception('Dictionary details could not be loaded')
            if not holder.is_deleted:
                button.set_text(tr('Could not load dictionary details. Try again.'))
        finally:
            busy = False
            if not button.is_deleted:
                button.enable()
    with holder:
        button = ui.button(tr(DETAILS), on_click=show).props('flat dense no-caps').classes(f'{BUTTON_CLASS} more')
    return holder
