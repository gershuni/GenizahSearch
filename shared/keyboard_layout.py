# -*- coding: utf-8 -*-
"""Hebrew typed with the keyboard still on the English layout.

A reader who types a Hebrew word without switching the keyboard sends the Latin
characters printed on the same keys: ``dcuk`` for גבול, ``fufch anh`` for
כוכבי שמי, ``bghkv`` for נעילה (PostHog ``search_executed``, 2026-11-03; every
one of them found nothing). This module reads those keys back through the
Israeli standard layout (the QWERTY key -> the Hebrew character on it) and says
whether a Latin-script query reads as Hebrew that way.

Pure: no index, no UI. Whether the Hebrew occurs in the corpus is the engine's
question (``SearchEngine.words_occur``); the search page asks it before it
offers the suggestion. ``tests/test_keyboard_layout.py``.
"""
from __future__ import annotations

import re
from typing import Optional

# QWERTY key -> the character on the same key of the Israeli standard layout,
# unshifted. The alphabet with its five final forms where the layout puts them
# (ך on L, ם on O, ן on I, ף on ;, ץ on .), and the three keys the layout gives
# to punctuation (Q, W and /), kept so a typed string maps to exactly what the
# reader's keys would have given.
ENGLISH_TO_HEBREW = {
    'q': '/', 'w': "'", 'e': 'ק', 'r': 'ר', 't': 'א', 'y': 'ט', 'u': 'ו', 'i': 'ן', 'o': 'ם', 'p': 'פ',
    'a': 'ש', 's': 'ד', 'd': 'ג', 'f': 'כ', 'g': 'ע', 'h': 'י', 'j': 'ח', 'k': 'ל', 'l': 'ך', ';': 'ף',
    'z': 'ז', 'x': 'ס', 'c': 'ב', 'v': 'ה', 'b': 'נ', 'n': 'מ', 'm': 'צ', ',': 'ת', '.': 'ץ', '/': '.',
}

HEBREW_LETTERS = frozenset('אבגדהוזחטיךכלםמןנסעףפץצקרשת')
FINAL_LETTERS = frozenset('ךםןףץ')

# Fewer mapped letters than this is not a word anyone meant to search for.
MIN_LETTERS = 2

_LATIN_LETTER_RE = re.compile(r'[A-Za-z]')


def map_keys_to_hebrew(text: str) -> Optional[str]:
    """*text* read key by key on the Hebrew layout, or None when a character
    has no key there (a digit, a shifted mark, anything beyond ASCII)."""
    out = []
    for ch in text:
        if ch.isspace():
            out.append(' ')
            continue
        mapped = ENGLISH_TO_HEBREW.get(ch.lower())
        if mapped is None:
            return None
        out.append(mapped)
    return ''.join(out)


def is_latin_query(text: str) -> bool:
    """ASCII with at least one Latin letter: what a wrong-layout query looks like."""
    return bool(text) and text.isascii() and _LATIN_LETTER_RE.search(text) is not None


def layout_suggestion(query: Optional[str]) -> Optional[str]:
    """The Hebrew a Latin-script query spells on the Hebrew layout, or None.

    Offered only when every character has a key there and none is a digit, each
    word comes out as Hebrew letters alone (a Q, W or / would have given
    punctuation), a final letter stands only at the end of its word (a reader
    on the Hebrew layout presses the final-form key there and nowhere else), and
    at least ``MIN_LETTERS`` letters came out. Case is ignored: a phone's
    keyboard capitalises the first letter of what is typed.
    """
    text = (query or '').strip()
    if not is_latin_query(text) or any(ch.isdigit() for ch in text):
        return None
    mapped = map_keys_to_hebrew(text)
    if mapped is None:
        return None
    words = mapped.split()
    if sum(len(w) for w in words) < MIN_LETTERS:
        return None
    for word in words:
        if not all(ch in HEBREW_LETTERS for ch in word):
            return None
        if any(ch in FINAL_LETTERS for ch in word[:-1]):
            return None
    return ' '.join(words)
