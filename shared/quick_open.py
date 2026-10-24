# -*- coding: utf-8 -*-
"""What a typed "go to" query is: a manuscript to open, or words to search.

The website's homepage box routes what a visitor types (``web/home_query.py``,
owner 2026-10-19); the desktop's Go to box (Ctrl+G, owner 2026-10-24) routes the
same way, with two desktop additions:

* a system number              -> open it in Browse. Both namespaces: on the
  desktop a ``97`` My Library id is a real document (``ANY_SYS_ID_RE``)
* ``FL`` and digits            -> open that FL number in Browse
* a Hebrew or Arabic letter    -> a full-text search for the words
* anything else                -> open it in Browse as a shelfmark; Browse's own
  lookup decides what it matches (one manuscript, a choice, or none)

Pure, no Qt: ``tests/test_quick_open.py``.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional, Tuple

from shared.sys_id_patterns import ANY_SYS_ID_RE

_FL_RE = re.compile(r'FL\s*\d+', re.IGNORECASE)


def has_hebrew_or_arabic_letter(text: str) -> bool:
    """True when ``text`` holds at least one Hebrew or Arabic LETTER.

    Letters only: niqqud, cantillation and Arabic-Indic digits do not count,
    so a string of them alone is not routed to full-text search.
    """
    for ch in text:
        if ch.isalpha() and unicodedata.name(ch, '').startswith(('HEBREW ', 'ARABIC ')):
            return True
    return False


def quick_open_target(text: Optional[str]) -> Optional[Tuple[str, str]]:
    """``(kind, value)`` for what was typed, or None if it is blank.

    ``kind`` is ``'sys_id'``, ``'fl'``, ``'search'`` or ``'shelfmark'``.
    """
    query = (text or '').strip()
    if not query:
        return None
    # isascii(): `\d` also matches Arabic-Indic and other Unicode digits.
    if query.isascii() and ANY_SYS_ID_RE.fullmatch(query):
        return 'sys_id', query
    if query.isascii() and _FL_RE.fullmatch(query):
        return 'fl', 'FL' + re.sub(r'\D', '', query)
    if has_hebrew_or_arabic_letter(query):
        return 'search', query
    return 'shelfmark', query
