# -*- coding: utf-8 -*-
"""An on-screen Hebrew keyboard for the main search box (pilot).

For visitors with no Hebrew layout on their computer: students, Arabists reading
Judeo-Arabic, a borrowed machine. A ⌨ button inside the /search box opens a panel
of keys in alphabet order, each final form beside its letter; a key puts its
character in at the cursor, in the browser (web/static/js/hebrew_keyboard.js), so
a letter costs no round trip.

The pilot's scope (owner, 2026-11-15, after a Codex design review):

* the main search box only, and only the alphabet layout. The Israeli layout is a
  second step if the numbers ask for it;
* no typing with the physical keys: a visitor who opened the panel for one letter
  and then types a shelfmark must get the shelfmark;
* no ``?`` key: at the start of the box ``?`` and a space switch the mode to
  Variants (``_shortcut_prefixes`` in web/pages/search.py), which a newcomer
  taking it for a wildcard would not notice. ``*`` is the wildcard;
* ``'`` and ``"`` as ASCII, the characters a Hebrew keyboard types: ``'`` marks a
  dotted Judeo-Arabic letter the way the dictionary box reads it (``ד'``), ``"``
  an abbreviation.

The script types through the box's own ``input`` event, so the page sees a key
exactly as a typed one (the model, the prefix detection, Enter).

PostHog, with no characters and no query text: ``hebrew_keyboard_opened`` (from
the page, when the panel opens), ``hebrew_keyboard_used`` (from the script, at a
search attempt's first key) and ``hebrew_keyboard`` on ``search_executed``.
posthog-js adds the screen width to every event, so the device class is there.

``tests/test_hebrew_keyboard.py``.
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
from pathlib import Path
from typing import Any, Callable, List, Tuple

logger = logging.getLogger(__name__)

_JS_PATH = Path(__file__).resolve().parent / 'static' / 'js' / 'hebrew_keyboard.js'

#: The layout's name in PostHog. The only one in the pilot.
LAYOUT = 'alphabetical'
#: The field's name in PostHog.
FIELD = 'main_search'

#: On the search box (``query_input``): the script types into the box carrying it.
TARGET_ATTR = 'data-gs-hebrew-keyboard-target'
#: On the panel: the script answers clicks on keys inside it.
PANEL_ATTR = 'data-gs-hebrew-keyboard'
#: Dispatched on the panel at a search attempt's first key (the page notes it for
#: ``search_executed``).
USED_EVENT = 'gs-hebrew-keyboard-used'

POSTHOG_OPENED = 'hebrew_keyboard_opened'
POSTHOG_USED = 'hebrew_keyboard_used'

#: The letters in alphabet order, each final form right after its letter (the
#: order a reader looks for it in). Rows of at most ten, so a 320px phone fits.
LETTER_ROWS: Tuple[Tuple[str, ...], ...] = (
    ('א', 'ב', 'ג', 'ד', 'ה', 'ו', 'ז', 'ח', 'ט', 'י'),
    ('כ', 'ך', 'ל', 'מ', 'ם', 'נ', 'ן', 'ס', 'ע', 'פ'),
    ('ף', 'צ', 'ץ', 'ק', 'ר', 'ש', 'ת'),
)

#: Final form -> its letter, for the key's label ("final kaf" says which).
FINALS = {'ך': 'כ', 'ם': 'מ', 'ן': 'נ', 'ף': 'פ', 'ץ': 'צ'}

#: The characters the last row types, with the label a screen reader reads.
SYMBOL_KEYS: Tuple[Tuple[str, str], ...] = (
    ('*', 'Wildcard: any letters'),
    ("'", 'Apostrophe (geresh)'),
    ('"', 'Quotation mark (abbreviation)'),
)


def all_characters() -> List[str]:
    """Every character a key types (letters, then symbols), in panel order."""
    return [c for row in LETTER_ROWS for c in row] + [c for c, _ in SYMBOL_KEYS] + [' ']


def _key(char: str, label: str, title: str, extra_class: str = '') -> str:
    cls = 'gs-hk-key' + (' ' + extra_class if extra_class else '')
    return (f'<button type="button" class="{cls}" data-hk-char="{html.escape(char, quote=True)}"'
            f' aria-label="{html.escape(title, quote=True)}" title="{html.escape(title, quote=True)}">'
            f'{html.escape(label)}</button>')


def panel_html(tr: Callable[[str], str]) -> str:
    """The keys, as one block of markup (no script: the page loads the script).

    *tr* translates the labels. Each key is a real ``<button>``: Tab reaches it,
    Enter and Space press it, and a screen reader reads its label.
    """
    rows = []
    for row in LETTER_ROWS:
        keys = []
        for ch in row:
            if ch in FINALS:
                title = tr('Final form of {}').format(FINALS[ch])
                keys.append(_key(ch, ch, title, 'gs-hk-final'))
            else:
                keys.append(_key(ch, ch, ch))
        rows.append('<div class="gs-hk-row">' + ''.join(keys) + '</div>')
    last = [_key(' ', tr('Space'), tr('Space'), 'gs-hk-space')]
    last += [_key(ch, ch, tr(title)) for ch, title in SYMBOL_KEYS]
    last.append(
        f'<button type="button" class="gs-hk-key gs-hk-wide" data-hk-action="backspace"'
        f' aria-label="{html.escape(tr("Delete"), quote=True)}" title="{html.escape(tr("Delete"), quote=True)}">⌫</button>')
    rows.append('<div class="gs-hk-row">' + ''.join(last) + '</div>')
    group = html.escape(tr('Hebrew keyboard'), quote=True)
    return (f'<div class="gs-hk-panel" role="group" aria-label="{group}" dir="rtl">'
            + ''.join(rows) + '</div>')


STYLES = '''<style>
/* Each row is ten equal columns of the panel's own width, so the keys fit any
   box from a 320px phone up; the panel stops growing at 30rem. */
.gs-hk-panel { display: flex; flex-direction: column; gap: 4px; padding: 6px 0 2px;
  max-width: 30rem; user-select: none; }
.gs-hk-row { display: grid; grid-template-columns: repeat(10, minmax(0, 1fr)); gap: 4px; }
.gs-hk-key {
  min-width: 0; height: 2.25rem; padding: 0; border-radius: 6px;
  border: 1px solid var(--border-light, #d0d4da); background: var(--bg-card, #fff);
  color: var(--text-primary, inherit); font: inherit; font-size: 1.15rem; line-height: 1;
  cursor: pointer; touch-action: manipulation; overflow: hidden; white-space: nowrap;
}
.gs-hk-key:hover { border-color: var(--primary-600, #2563eb); }
.gs-hk-key:focus-visible { outline: 2px solid var(--primary-600, #2563eb); outline-offset: 1px; }
.gs-hk-key:active { transform: translateY(1px); }
.gs-hk-final { color: var(--primary-600, #2563eb); }
.gs-hk-wide { grid-column: span 2; font-size: 0.9rem; }
.gs-hk-space { grid-column: span 3; font-size: 0.9rem; }
@media (max-width: 640px) {
  .gs-hk-row { gap: 3px; }
  .gs-hk-key { height: 2.5rem; }
}
</style>
'''


def head_html() -> str:
    """The panel's styles and the ``<script>`` line, versioned by the script's
    content; only the styles when the script is missing, so a page never
    references a script that 404s (the keys then do nothing)."""
    try:
        version = hashlib.sha256(_JS_PATH.read_bytes()).hexdigest()[:10]
    except OSError:
        logger.warning('hebrew_keyboard: %s is missing; the keys type nothing', _JS_PATH)
        return STYLES
    return STYLES + f'<script src="/static/js/hebrew_keyboard.js?v={version}" defer></script>\n'


def reset_script() -> str:
    """The JavaScript that starts a new search attempt in the script (its next
    key sends ``hebrew_keyboard_used`` again)."""
    return 'window.gsHebrewKeyboard && window.gsHebrewKeyboard.reset()'


def used_payload(args: Any) -> dict:
    """The ``USED_EVENT`` payload from NiceGUI's event arguments; {} when it is
    not one. Never raises."""
    payload = args
    if isinstance(payload, dict) and 'detail' in payload:
        payload = payload['detail']
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return {}
    return payload if isinstance(payload, dict) else {}
