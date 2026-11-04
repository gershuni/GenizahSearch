# -*- coding: utf-8 -*-
"""What a typed "go to" query is: a manuscript to open, or words to search.

The desktop's Go to box (Ctrl+G, owner 2026-10-24) routes what was typed as
the website's homepage and header boxes do (``web/home_query.py``; one rule,
``shared/search_box_input.py::go_to_kind``, since 2026-11-04), with two
desktop additions:

* a system number              -> open it in Browse. Both namespaces: on the
  desktop a ``97`` My Library id is a real document (``ANY_SYS_ID_RE``)
* ``FL`` and digits            -> open that FL number in Browse
* a shelfmark (``ts 20.57``, ``add 3356``, the NLI catalogue form; or one the
  catalogue holds, ``Mingana 9``) -> open it in Browse as a shelfmark
* anything else -- Hebrew, Arabic, English words, Hebrew typed on the English
  layout (``dcuk``), ``Psalm 23``, a bare ``12345`` -> a text search

Until 2026-11-04 everything without a Hebrew or Arabic letter went to Browse,
so ``dcuk`` ended on "No manuscript found" and never reached the Search tab's
"Did you mean <Hebrew>?".

Pure, no Qt: ``tests/test_quick_open.py``.
"""
from __future__ import annotations

import re
from typing import Callable, Optional, Tuple

# has_hebrew_or_arabic_letter is re-exported: it lived here until 2026-11-04.
from shared.search_box_input import go_to_kind, has_hebrew_or_arabic_letter  # noqa: F401
from shared.sys_id_patterns import ANY_SYS_ID_RE

_FL_RE = re.compile(r'FL\s*\d+', re.IGNORECASE)


def quick_open_target(
    text: Optional[str], has_shelfmark: Optional[Callable[[str], bool]] = None,
) -> Optional[Tuple[str, str]]:
    """``(kind, value)`` for what was typed, or None if it is blank.

    ``kind`` is ``'sys_id'``, ``'fl'``, ``'search'`` or ``'shelfmark'``.
    ``has_shelfmark`` is the catalogue check ``go_to_kind`` takes
    (``MetadataManager.has_shelfmark``).
    """
    query = (text or '').strip()
    if not query:
        return None
    # isascii(): `\d` also matches Arabic-Indic and other Unicode digits.
    if query.isascii() and ANY_SYS_ID_RE.fullmatch(query):
        return 'sys_id', query
    if query.isascii() and _FL_RE.fullmatch(query):
        return 'fl', 'FL' + re.sub(r'\D', '', query)
    if go_to_kind(query, has_shelfmark) == 'shelfmark':
        return 'shelfmark', query
    return 'search', query
