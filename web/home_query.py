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

except that Latin text with a digit the recogniser calls text is first looked up
in the catalogue (``has_shelfmark``; the pages pass ``catalogue_has_shelfmark``):
no set of rules knows every catalogue form (``Mingana 9``, ``P. Mich. Inv. 531``;
Codex on #476), and before 2026-11-04 every such string reached Browse.

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

from typing import Callable, Optional
from urllib.parse import quote

# Re-exported: tests/test_home_query_routing.py and older callers import it from here.
from shared.quick_open import has_hebrew_or_arabic_letter
from web.search_box_input import search_box_input_kind


def home_query_target(
    text: Optional[str], has_shelfmark: Optional[Callable[[str], bool]] = None,
) -> Optional[str]:
    """The URL a go-to box navigates to for ``text``, or None if blank.

    ``has_shelfmark(query)`` says whether the catalogue holds ``query`` as a
    shelfmark; it is asked only about text with a digit and no Hebrew or Arabic
    letter that the recogniser did not take. Without it the recogniser alone decides.
    """
    query = (text or '').strip()
    if not query:
        return None
    kind = search_box_input_kind(query)
    if kind == 'sys_id':
        return f'/browse?sys_id={query}'
    if kind == 'shelfmark' or (has_shelfmark is not None and _worth_looking_up(query) and has_shelfmark(query)):
        return f'/browse?shelfmark={quote(query, safe="")}'
    return f'/search?q={quote(query, safe="")}'


def _worth_looking_up(query: str) -> bool:
    """A digit, and no Hebrew or Arabic letter: words alone are never a shelfmark."""
    return any(ch.isdigit() for ch in query) and not has_hebrew_or_arabic_letter(query)


def catalogue_has_shelfmark(query: str) -> bool:
    """``has_shelfmark`` over the loaded catalogue (``MetadataManager.has_shelfmark``,
    a binary search of the index the server builds at start; called off the event loop
    all the same). False while the catalogue or that index is not ready."""
    from web.state import state   # here, so this module stays importable without the app
    meta = state.meta_mgr
    if meta is None:
        return False
    try:
        return bool(meta.has_shelfmark(query))
    except Exception:                                        # noqa: BLE001 -- a failed lookup searches
        return False
