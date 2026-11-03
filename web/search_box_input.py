# -*- coding: utf-8 -*-
"""What the /search box was given, when it is not Hebrew text.

The homepage box routes a shelfmark or a system number to /browse
(``web/home_query.py``); the /search box ran the same strings as text and found
nothing: ``ts 20.57``, ``t-s ns 307.61b``, ``add 3356``, ``ts k2.27``,
``t-s 10g4.5``, ``tad c2.1``, ``5559a.10`` (PostHog ``search_executed``,
2026-11-03). The page now opens a system number in Browse, as the homepage
does, and runs a shelfmark in its Shelfmark mode (what the ``#`` prefix
selects), saying so under the count with a way to run it as text instead.

``search_box_input_kind`` is the homepage's recogniser -- a corpus sys_id is
``CORPUS_SYS_ID_RE``, a Hebrew or Arabic letter means text -- narrowed for a box
that also takes words: a shelfmark here must have a digit and the shape of one
(the Shelfmark mode's own lookup then decides what it matches):

* a number with a dot in it (``20.57``, ``5559a.10``), or a letter and a digit
  run together (``k2.27``, ``10g4``, ``307.61b``); or
* a collection prefix first (``T-S``, ``ENA``, ``Add.``, ``Or.``, ``Moss.``:
  the first tokens of libraries.csv's shelfmarks, counted 2026-11-03), a
  library name or code first (``_strip_library_prefix``: ``Cambridge``,
  ``JTS``), or the NLI catalogue's own form (``looks_like_catalogue_form``:
  ``..., Ms. T-S K 08.026``).

So ``Psalm 23``, ``chapter 2`` and a bare ``12`` (15,533 catalogue rows) stay
text, as do ``dcuk`` (גבול typed on the English layout,
shared/keyboard_layout.py) and English words.

Pure (no NiceGUI import): ``tests/test_search_box_input.py``; the page's wiring
is ``tests/render_smoke/test_search_box_input_render_smoke.py``.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from shared.browse_map_utils import _strip_library_prefix
from shared.catalog_shelfmark import looks_like_catalogue_form
from shared.quick_open import has_hebrew_or_arabic_letter
from shared.sys_id_patterns import CORPUS_SYS_ID_RE

# The page modes in which what was typed is words to find in the text: the only
# ones a shelfmark or a system number is recognised in, and the only ones the
# keyboard-layout suggestion is made for. Regex, Responsa, Title, Shelfmark and
# PGP tags mean what they say.
TEXT_MODES = frozenset({'exact', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'})

# The first token of a shelfmark as the catalogue writes it, lower-cased, its
# dots dropped: every token that heads 100 or more libraries.csv shelfmarks
# (2026-11-03), plus the spellings readers type for them (``ts``, ``lg``).
COLLECTION_PREFIXES = frozenset({
    't-s', 'ts', 'ena', 'adler', 'heb', 'gaster', 'evr', 'yevr', 'arab-yevr', 'yevr-arab', 'or', 'moss',
    'mosseri', 'ns', 'as', 'ar', 'arab', 'l-g', 'lg', 'lutzki', 'westminster', 'lon', 'kaufmann', 'genizah',
    'halper', 'add', 'misc', 'bib', 'lit', 'talm', 'freer', 'krengel', 'rainer', 'gen', 'bible', 'af',
})

# Letters, digits and the marks shelfmarks are written with (``T-S NS 307.61b``,
# ``Halper 331``, ``Moss. IV,12.2``, ``MS Heb c 57``, ``ENA 2156.1``).
_SHELFMARK_CHARS_RE = re.compile(r"^[A-Za-z0-9 .,:;'()\[\]/\-]+$")
_FIRST_TOKEN_RE = re.compile(r'^[A-Za-z][A-Za-z.\-]*')
_DOTTED_NUMBER_RE = re.compile(r'[0-9A-Za-z]\.\d')
_LETTER_DIGIT_RE = re.compile(r'[A-Za-z]\d|\d[A-Za-z]')


def search_box_input_kind(query: Optional[str]) -> str:
    """``'sys_id'``, ``'shelfmark'`` or ``'text'`` for what the search box holds."""
    text = (query or '').strip()
    if not text or has_hebrew_or_arabic_letter(text) or not text.isascii():
        return 'text'
    if CORPUS_SYS_ID_RE.fullmatch(text):
        return 'sys_id'
    if not _SHELFMARK_CHARS_RE.match(text) or not any(ch.isdigit() for ch in text):
        return 'text'
    if _DOTTED_NUMBER_RE.search(text) or _LETTER_DIGIT_RE.search(text):
        return 'shelfmark'
    head = _FIRST_TOKEN_RE.match(text)
    if head and head.group(0).lower().replace('.', '') in COLLECTION_PREFIXES:
        return 'shelfmark'
    if _strip_library_prefix(text) != text or looks_like_catalogue_form(text):
        return 'shelfmark'
    return 'text'


def typed_as_shelfmark(query: Optional[str]) -> bool:
    """True when the search box holds a shelfmark (not a system number, not words)."""
    return search_box_input_kind(query) == 'shelfmark'


def split_placeholder(template: str, placeholder: str = '{query}') -> Tuple[str, str]:
    """``(before, after)`` the placeholder in a translated sentence, so the query
    can be rendered as a link between the two halves. A sentence without the
    placeholder is all ``before``."""
    before, found, after = template.partition(placeholder)
    return (before, after) if found else (template, '')
