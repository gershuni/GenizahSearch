# -*- coding: utf-8 -*-
"""Where a "go to" box sends what the visitor typed.

One routing for the homepage hero box (``web/pages/home.py``), the search box
in the site header (``web/main.py::create_layout``) and the system-number case
of the /search box (``web/pages/search.py``). It routes by what
``web/search_box_input.py::search_box_input_kind`` -- the /search box's own
recogniser -- says the input is:

* a corpus sys_id (ASCII digits, the ``99`` namespace)  -> ``/browse?sys_id=``
* a shelfmark (``ts 20.57``, ``add 3356``, the NLI
  catalogue form, a library-prefixed one)               -> ``/browse?shelfmark=``
* anything else -- Hebrew, Arabic, English words,
  Hebrew typed on the English layout (``dcuk``)          -> ``/search?q=``

Until 2026-11-04 everything without a Hebrew or Arabic letter went to
``/browse?shelfmark=``, so wrong-layout Hebrew and English words ended on
"No manuscript found" (PostHog review 2026-11-03) and never reached /search,
which offers "Did you mean <Hebrew>?" for the first (PR #469).

The owner asked for numbers that "end with 99"; corpus sys_ids BEGIN with 99
(255,723 of 255,723 in ``libraries.csv``, and none ends with 99), so the test is
the canonical ``CORPUS_SYS_ID_RE`` -- the same namespace ``/browse`` itself
treats as a sys_id.

The shelfmark branch hands the string to ``/browse`` unchanged, so the browse
page's own lookup (``search_shelfmark`` in ``web/pages/browse.py``) decides what
it matches. The text branch hands it to /search, which runs it in its default
text mode (and, if its own recogniser disagrees, as a shelfmark there).

Pure (no NiceGUI import) so it is unit-tested directly:
``tests/test_home_query_routing.py``.
"""
from __future__ import annotations

from typing import Optional
from urllib.parse import quote

# Re-exported: tests/test_home_query_routing.py and older callers import it from here.
from shared.quick_open import has_hebrew_or_arabic_letter  # noqa: F401
from web.search_box_input import search_box_input_kind


def home_query_target(text: Optional[str]) -> Optional[str]:
    """The URL a go-to box navigates to for ``text``, or None if blank."""
    query = (text or '').strip()
    if not query:
        return None
    kind = search_box_input_kind(query)
    if kind == 'sys_id':
        return f'/browse?sys_id={query}'
    if kind == 'shelfmark':
        return f'/browse?shelfmark={quote(query, safe="")}'
    return f'/search?q={quote(query, safe="")}'
