# -*- coding: utf-8 -*-
"""The reading text's size: A- and A+ beside the text, as the desktop app has them (owner, 2026-11-23).

One size per visitor, kept in their storage (``ui.text_scale``) and applied as one CSS variable,
``--gs-text-scale``, by which the transcriptions' font sizes are multiplied: Browse's page and its
Full Manuscript View, and the Reading Desk's texts. A click changes the variable in the page and
keeps the new size for the next page; every text on the page follows at once.

The steps are the desktop app's (``desktop/result_dialog.py``: 16pt, 9 to 40, steps of 2) as
ratios of its default: 9/16 to 2.5, steps of 0.125, every size a whole desktop point (a sixteenth),
so the desktop's own floor, 9pt, is a size here too (Codex on #108: it stopped at 10pt).

A visitor who has not chosen starts a step below the page's sizes (owner, 2026-11-23: the text a bit
smaller by default), as one press of A-: 14pt of the desktop's 16.
"""
from __future__ import annotations

from nicegui import ui

from web.safe_storage import safe_user_get, safe_user_set
from web.translations import tr

KEY = 'ui.text_scale'
STEP, MIN, MAX, DEFAULT = 0.125, 9 / 16, 2.5, 0.875
POINT = 1 / 16                              # one desktop point at its 16pt default
VAR = '--gs-text-scale'


def clamp(scale) -> float:
    """A size within the steps (anything else stored is the default)."""
    try:
        value = float(scale)
    except (TypeError, ValueError):
        return DEFAULT
    if value != value:                      # NaN
        return DEFAULT
    value = min(MAX, max(MIN, value))
    return round(round(value / POINT) * POINT, 4)


def current() -> float:
    """The visitor's size."""
    return clamp(safe_user_get(KEY, DEFAULT))


def scaled(size: str) -> str:
    """A CSS font size multiplied by the visitor's size (``1.2rem`` -> ``calc(1.2rem * var(...))``)."""
    return f'calc({size} * var({VAR}, 1))'


def head_style(scale: float) -> str:
    """The page's starting size, in its head (a page drawn again keeps the variable the page set)."""
    return f'<style>:root {{ {VAR}: {clamp(scale)}; }}</style>'


def set_js(scale: float) -> str:
    return f"document.documentElement.style.setProperty('{VAR}', '{clamp(scale)}')"


def text_size_buttons(classes: str = '') -> ui.row:
    """A- and A+: a step smaller or larger, kept for the visitor. Left-to-right marks keep "A-" from
    showing as "-A" in the Hebrew page (as the desktop app does)."""
    def change(step: float) -> None:
        new = clamp(current() + step)
        safe_user_set(KEY, new)
        ui.run_javascript(set_js(new))

    with ui.row().classes(f'items-center gap-0 no-wrap gs-text-size {classes}'.strip()) as row:
        for text, step, name in (('‎A−‎', -STEP, tr('Smaller text')),
                                 ('‎A+‎', STEP, tr('Larger text'))):
            button = ui.button(text, on_click=lambda _e=None, s=step: change(s)).props(
                'flat dense no-caps size=sm').classes('gs-text-size-btn').tooltip(name)
            button.props['aria-label'] = name
    return row
