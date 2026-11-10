# -*- coding: utf-8 -*-
"""Copy text to the viewer's clipboard, and say "copied" only when it worked.

Every web copy button that sends Python text to the browser goes through
``copy_text_to_clipboard`` (tests/test_web_clipboard_copy.py enforces it).

Two things this module gets right that hand-built JavaScript did not:

* The text reaches ``navigator.clipboard.writeText`` as a JSON string literal,
  so the browser copies it exactly. Never build it as a template literal
  (backticks): that alters backslash sequences and other characters in the
  text, and a trailing backslash copies nothing.
* The toast follows the browser's answer. The JavaScript is ONE expression
  whose value is a Promise resolving to true or false; NiceGUI's runJavascript
  adopts that Promise and sends its value back. It never rejects (a rejection
  would send no answer at all, which Python sees only as a timeout).

Imports only the stdlib, nicegui and web.translations, so any page can use it.
"""

from __future__ import annotations

import json
import re

from nicegui import ui

from web.translations import tr

_WRITE_JS = (
    '(async () => { try { await navigator.clipboard.writeText(%s); return true; } '
    'catch (e) { return false; } })()'
)

# A lone surrogate cannot be encoded for the websocket; as a \uXXXX escape it
# is still a valid JavaScript string literal and decodes to the same code unit.
_LONE_SURROGATE_RE = re.compile('[\ud800-\udfff]')


def _js_string_literal(text: str) -> str:
    literal = json.dumps(text, ensure_ascii=False)
    return _LONE_SURROGATE_RE.sub(lambda m: '\\u%04x' % ord(m.group()), literal)


def clipboard_write_js(text: str) -> str:
    """The JavaScript expression that writes ``text`` exactly and resolves to a bool."""
    return _WRITE_JS % _js_string_literal(text)


async def copy_text_to_clipboard(text: str, *, success_message: str | None = None,
                                 timeout: float = 5.0) -> bool:
    """Copy ``text`` in the browser; toast the real outcome; return True on success.

    The client is read BEFORE the await: the button that started the copy can be
    rebuilt during the round trip, and its slot would then be gone. The timeout
    covers the whole socket round trip (NiceGUI's default is 1 s), so a timeout
    means "not confirmed", not "failed".
    """
    if not text:
        ui.notify(tr('No text to copy'), type='warning')
        return False
    client = ui.context.client
    try:
        ok = await client.run_javascript(clipboard_write_js(text), timeout=timeout)
    except TimeoutError:
        with client:
            ui.notify(tr('Could not confirm the copy.'), type='warning')
        return False
    with client:
        if ok is True:
            ui.notify(success_message or tr('Text copied to clipboard'), type='positive')
            return True
        ui.notify(tr('Copy failed. Select the text and copy it manually.'), type='negative')
        return False
