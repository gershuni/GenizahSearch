# -*- coding: utf-8 -*-
"""Where the homepage search box sends what the visitor typed.

The hero box on ``/`` used to run every input as a full-text query, so a
reader who typed a shelfmark got a text search for "T-S 8J6.1". It now routes
by what the input is (owner, 2026-10-19):

* a corpus sys_id -- ASCII digits in the ``99`` namespace -> ``/browse?sys_id=``
* anything with a Hebrew or Arabic letter in it          -> ``/search?q=``
* anything else                                          -> ``/browse?shelfmark=``

The owner asked for numbers that "end with 99"; corpus sys_ids BEGIN with 99
(255,723 of 255,723 in ``libraries.csv``, and none ends with 99), so the test is
the canonical ``CORPUS_SYS_ID_RE`` -- the same namespace ``/browse`` itself
treats as a sys_id.

The shelfmark branch hands the string to ``/browse`` unchanged, so the browse
page's own lookup (``search_shelfmark`` in ``web/pages/browse.py``) decides what
it matches: the manuscript for one match, the "Select Manuscript" list for
several, "No manuscript found" for none. Nothing here re-implements it.

Pure (no NiceGUI import) so it is unit-tested directly:
``tests/test_home_query_routing.py``.
"""
from __future__ import annotations

import unicodedata
from typing import Optional
from urllib.parse import quote

from shared.sys_id_patterns import CORPUS_SYS_ID_RE


def has_hebrew_or_arabic_letter(text: str) -> bool:
    """True when ``text`` holds at least one Hebrew or Arabic LETTER.

    Letters only: niqqud, cantillation and Arabic-Indic digits do not count,
    so a string of them alone is not routed to full-text search.
    """
    for ch in text:
        if ch.isalpha() and unicodedata.name(ch, '').startswith(('HEBREW ', 'ARABIC ')):
            return True
    return False


def home_query_target(text: Optional[str]) -> Optional[str]:
    """The URL the homepage box navigates to for ``text``, or None if blank."""
    query = (text or '').strip()
    if not query:
        return None
    # isascii(): `\d` also matches Arabic-Indic and other Unicode digits.
    if query.isascii() and CORPUS_SYS_ID_RE.fullmatch(query):
        return f'/browse?sys_id={query}'
    if has_hebrew_or_arabic_letter(query):
        return f'/search?q={quote(query, safe="")}'
    return f'/browse?shelfmark={quote(query, safe="")}'
