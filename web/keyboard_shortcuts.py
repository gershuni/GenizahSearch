# -*- coding: utf-8 -*-
"""Keyboard shortcuts on the website, and the card that lists them (owner, 2026-11-05).

The owner asked which shortcuts work; the answer was that /browse had several (page
arrows, zoom, full screen) and nothing said so, while readers clicked the same
buttons hundreds of times a month (PostHog, 30 days to 2026-11-05: zoom + 427 clicks
by 40 readers, full screen 444 by 55, the page arrows 406 by 48). So the shortcuts
are named where the buttons are (their tooltips) and listed in one card:

- ``?`` on any page, or "Keyboard shortcuts" in the sidebar, opens the card
  (web/static/js/shortcuts.js dispatches ``SHORTCUT_EVENT`` on the page's hidden
  ``TRIGGER_CLASS`` element; web/main.py draws the card there, once per page);
- ``/`` jumps to the page's search box (``PRIMARY_SEARCH_ATTR``) or the header's
  go-to box.

``SECTIONS`` is what the card lists, and each row is a shortcut that exists: the
keys are handled in web/static/js/shortcuts.js (every page), web/pages/browse.py's
key script (Browse by Shelfmark), web/static/js/blau_lookup.js (the dictionaries,
the M key) and web/pages/search.py (Space through the results). The tests read
those scripts for each key.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

from nicegui import ui

from web.translations import tr

logger = logging.getLogger(__name__)

SHORTCUT_EVENT = 'gs-shortcuts'
TRIGGER_CLASS = 'gs-shortcuts-trigger'
CARD_CLASS = 'gs-shortcuts-card'
KEY_CLASS = 'gs-kbd'
#: The attribute that marks a page's own search box, for ``/``.
PRIMARY_SEARCH_ATTR = 'data-gs-primary-search'

_JS_PATH = Path(__file__).resolve().parent / 'static' / 'js' / 'shortcuts.js'

TITLE = 'Keyboard shortcuts'
EVERY_PAGE = 'On every page'


#: Words in a row's keys that are not keys: drawn as words, and translated. Key names
#: (Enter, Space, Shift, letters) stay as printed on the keyboard.
PLAIN_WORDS = ('click', 'Select words', 'or', 'then')
#: Words that join keys pressed one after the other or as alternatives: no "+" beside them.
CONNECTORS = ('or', 'then')


def sections(rtl: bool, dictionaries: bool) -> list[tuple[str, list[tuple[tuple[str, ...], str]]]]:
    """The card's sections: ``(title, [(keys, what they do)])``, untranslated; ``keys``
    are pressed together (``PLAIN_WORDS`` are actions, not keys).

    The page arrows follow the page's direction, as the buttons do: in Hebrew the next
    page is to the left. The dictionary rows only where a dictionary is served."""
    next_key, prev_key = ('←', '→') if rtl else ('→', '←')
    browse = [
        ((next_key,), 'Next Page'),
        ((prev_key,), 'Previous Page'),
        (('+',), 'Zoom in'),
        (('−',), 'Zoom out'),
        (('0',), 'Reset View'),
        (('F',), 'Fullscreen Image'),
    ]
    if dictionaries:
        # Owner, 2026-11-05: D or M tapped arms the next click (a held letter blocks a laptop
        # touchpad), Alt + a click does the same, Esc closes the panel.
        browse += [
            (('D', 'or', 'M', 'then', 'click'), 'Look up a word in the dictionaries'),
            (('Alt', 'click'), 'Look up a word in the dictionaries'),
            (('Select words', 'D'), 'Look up the selected words'),
            (('Esc',), 'Close the dictionary'),
        ]
    return [
        (EVERY_PAGE, [(('?',), 'Show this list'), (('/',), 'Jump to the search box')]),
        ('Browse by Shelfmark', browse),
        ('Search', [(('Enter',), 'Run the search'),
                    (('Space',), 'Scroll the results down'),
                    (('Shift', 'Space'), 'Scroll the results up')]),
    ]


def strings(dictionaries: bool = True) -> set[str]:
    """Every string the card translates, for the translation test."""
    out = {TITLE}
    for title, rows in sections(False, dictionaries):
        out.add(title)
        for keys, text in rows:
            out.add(text)
            out.update(k for k in keys if k in PLAIN_WORDS)
    return out


def head_html() -> str:
    """The ``<head>`` line that loads the key script, versioned by its content (as
    web/components/blau_panel.py does); '' when the file is missing."""
    try:
        version = hashlib.sha256(_JS_PATH.read_bytes()).hexdigest()[:10]
    except OSError:
        logger.warning('keyboard_shortcuts: %s is missing; ? and / do nothing', _JS_PATH)
        return ''
    return f'<script src="/static/js/shortcuts.js?v={version}" defer></script>\n'


#: The dictionaries of the /browse panel, in its tab order (web/components/blau_panel.py).
BLAU, FRIEDMAN, LANE = 'blau', 'friedman', 'lane'


def served_dictionaries() -> dict[str, bool]:
    """Which dictionaries are served now, by name in the panel's tab order: the same three
    services /browse asks before it puts the panel on the page. All False when a service
    cannot be asked (the card and the Help page never cost the page)."""
    try:
        from shared.arabic_service import get_arabic_service
        from shared.blau_service import get_blau_service
        from shared.friedman_service import get_friedman_service
        return {BLAU: bool(get_blau_service().is_available()),
                FRIEDMAN: bool(get_friedman_service().is_available()),
                LANE: bool(get_arabic_service().is_available())}
    except Exception:  # noqa: BLE001 - the card never costs the page
        logger.warning('keyboard_shortcuts: dictionary availability unknown', exc_info=True)
        return {BLAU: False, FRIEDMAN: False, LANE: False}


def expression_dictionaries() -> dict[str, bool]:
    """Which served dictionaries have the expression search now: Friedman's whenever it is
    served (its sidecar carries the tokens by schema), Blau's only on a 1.1.0 sidecar
    (``BlauService.expressions_available``): a 1.0.0 file is served without it while the code
    is on the server ahead of the rebuilt file, and the Help page must not promise it then
    (Codex on #488). All False when a service cannot be asked."""
    try:
        from shared.blau_service import get_blau_service
        from shared.friedman_service import get_friedman_service
        blau = get_blau_service()
        return {BLAU: bool(blau.is_available() and blau.expressions_available()),
                FRIEDMAN: bool(get_friedman_service().is_available())}
    except Exception:  # noqa: BLE001 - the Help page never costs the page
        logger.warning('keyboard_shortcuts: expression-search availability unknown', exc_info=True)
        return {BLAU: False, FRIEDMAN: False}


def dictionaries_served() -> bool:
    """Is a dictionary served now (the M key's rows belong on the card)? Blau's, Friedman's
    or Lane's."""
    return any(served_dictionaries().values())


def build_card(rtl: bool, dictionaries: Optional[bool] = None) -> ui.dialog:
    """The card, as a dialog the caller opens (created in the caller's slot)."""
    if dictionaries is None:
        dictionaries = dictionaries_served()
    with ui.dialog() as dialog, ui.card().classes(CARD_CLASS).style(
            f'direction: {"rtl" if rtl else "ltr"};'):
        with ui.row(wrap=False).classes('w-full items-center justify-between'):
            ui.label(tr(TITLE)).classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props(
                f'flat round dense aria-label="{tr("Close")}"')
        for title, rows in sections(rtl, dictionaries):
            ui.label(tr(title)).classes('gs-shortcuts-section')
            with ui.element('div').classes('gs-shortcuts-grid'):
                for keys, text in rows:
                    with ui.element('div').classes('gs-shortcuts-keys'):
                        for i, key in enumerate(keys):
                            if i and key not in CONNECTORS and keys[i - 1] not in CONNECTORS:
                                ui.label('+').classes('gs-shortcuts-plus')
                            if key in PLAIN_WORDS:
                                ui.label(tr(key)).classes('gs-shortcuts-word')
                            else:
                                ui.label(key).classes(KEY_CLASS).props('dir=ltr')
                    ui.label(tr(text)).classes('gs-shortcuts-text')
    return dialog
