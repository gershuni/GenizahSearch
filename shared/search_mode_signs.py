# -*- coding: utf-8 -*-
"""The search-mode signs: a sign before a query chooses its mode (``$ שלום`` is a Title search).

One table for every place that reads them:

* the engine's own prefix parsing (``shared/search_engine.py::parse_query_syntax``), which
  every search on the website and in the desktop app goes through;
* the website's /search box (``web/pages/search.py``), which switches its mode selector
  as the space after a sign is typed;
* the website's go-to boxes, the homepage box and the header box
  (``web/home_query.py::home_query_target``), which open /search in the sign's mode.

``%`` (the catalogue text search) is a sign only where that search is offered: the caller
says so (``catalog_served``; on the website ``web/catalog_assets.py::catalog_available``).

Pure (no app import), so both apps can share it. Tests: ``tests/test_search_mode_signs.py``.
"""
from __future__ import annotations

from typing import Optional, Tuple

#: The mode the ``%`` sign chooses, offered only where the catalogue text search is served.
CATALOG_MODE = 'Catalog'

#: (sign, mode). Where one sign begins another, the longer comes first
#: (``???`` before ``??`` before ``?``).
MODE_SIGNS: Tuple[Tuple[str, str], ...] = (
    ('???', 'variants_maximum'),
    ('??', 'variants_extended'),
    ('?', 'variants'),
    ('=', 'exact'),
    ('~', 'fuzzy'),
    ('/', 'Regex'),
    ('$', 'Title'),
    ('#', 'Shelfmark'),
    ('%', CATALOG_MODE),
    ('R', 'responsa'),
)


def split_mode_sign(text: Optional[str], *, catalog_served: bool) -> Tuple[Optional[str], str]:
    """``(mode, query)`` when *text* begins with a sign and a space, else ``(None, text)``.

    ``'$ שלום'`` -> ``('Title', 'שלום')``; ``'?? שלום'`` -> ``('variants_extended', 'שלום')``.
    The query is what follows, its leading spaces dropped, and may be empty (``'$ '`` chooses
    the mode alone, as typing it into the /search box does). A sign with no space after it
    (``'$שלום'``) is not split here: the /search box leaves it for the engine's own parsing.
    ``%`` counts only when *catalog_served*.
    """
    if not text:
        return None, text or ''
    for sign, mode in MODE_SIGNS:
        if mode == CATALOG_MODE and not catalog_served:
            continue
        if text.startswith(sign + ' '):
            return mode, text[len(sign):].lstrip()
    return None, text
