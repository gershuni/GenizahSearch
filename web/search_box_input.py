# -*- coding: utf-8 -*-
"""What the /search box was given, when it is not Hebrew text.

The homepage box routes a shelfmark or a system number to /browse
(``web/home_query.py``); the /search box ran the same strings as text and found
nothing: ``ts 20.57``, ``t-s ns 307.61b``, ``add 3356``, ``ts k2.27``,
``t-s 10g4.5``, ``tad c2.1``, ``5559a.10`` (PostHog ``search_executed``,
2026-11-03). The page now runs such a query in its Shelfmark mode (what the
``#`` prefix selects) and says so under the count, with a way to run it as text
instead.

The recogniser is the homepage's -- a corpus sys_id is ``CORPUS_SYS_ID_RE``, a
Hebrew or Arabic letter means text -- narrowed for a box that also takes words:
only a string with a digit in it is a shelfmark here, so ``dcuk`` (גבול typed
on the English layout, shared/keyboard_layout.py) and an English word stay
text. The Shelfmark mode's own lookup then decides what the string matches.

Pure (no NiceGUI import): ``tests/test_search_box_input.py``; the page's wiring
is ``tests/render_smoke/test_search_box_input_render_smoke.py``.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from shared.quick_open import has_hebrew_or_arabic_letter
from shared.sys_id_patterns import CORPUS_SYS_ID_RE

# The page modes in which what was typed is words to find in the text: the only
# ones a shelfmark is recognised in, and the only ones the keyboard-layout
# suggestion is made for. Regex, Responsa, Title, Shelfmark and PGP tags mean
# what they say.
TEXT_MODES = frozenset({'exact', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'})

# Letters, digits and the marks shelfmarks are written with (``T-S NS 307.61b``,
# ``Halper 331``, ``Moss. IV,12.2``, ``MS Heb c 57``, ``ENA 2156.1``).
_SHELFMARK_CHARS_RE = re.compile(r"^[A-Za-z0-9 .,:;'()\[\]/\-]+$")


def typed_as_shelfmark(query: Optional[str]) -> bool:
    """True when the search box holds a shelfmark or a corpus sys_id, not words."""
    text = (query or '').strip()
    if not text or has_hebrew_or_arabic_letter(text) or not text.isascii():
        return False
    if CORPUS_SYS_ID_RE.fullmatch(text):
        return True
    return bool(_SHELFMARK_CHARS_RE.match(text)) and any(ch.isdigit() for ch in text)


def split_placeholder(template: str, placeholder: str = '{query}') -> Tuple[str, str]:
    """``(before, after)`` the placeholder in a translated sentence, so the query
    can be rendered as a link between the two halves. A sentence without the
    placeholder is all ``before``."""
    before, found, after = template.partition(placeholder)
    return (before, after) if found else (template, '')
