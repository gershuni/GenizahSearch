# -*- coding: utf-8 -*-
"""Where a "go to" box sends what the visitor typed.

One routing for the homepage hero box (``web/pages/home.py``), the search box
in the site header (``web/main.py::create_layout``) and the system-number case
of the /search box (``web/pages/search.py``). It routes by what
``shared/search_box_input.py::search_box_input_kind`` -- the /search box's own
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

The owner asked for numbers that end with 99; corpus sys_ids BEGIN with 99
(255,723 of 255,723 in ``libraries.csv``, and none ends with 99), so the test is
the canonical ``CORPUS_SYS_ID_RE`` -- the same namespace ``/browse`` itself
treats as a sys_id.

A search-mode sign and a space come first (2026-11-18): ``$ שלום`` opens
``/search?q=שלום&mode=Title``, as typing it into the /search box chooses Title there.
The signs are ``shared/search_mode_signs.py``'s, one table for every box; ``%`` (the
catalogue text search) only where the page says that search is served (``catalog_served``).
The one sign that is a letter, ``R`` (Responsa), begins 33 libraries.csv shelfmarks
(``R 15`` Mosseri, ``R II 5`` Turin): text of that shape (``R_SHELFMARK_RE``) is routed as
before, decided from the text alone, whether or not the catalogue's shelfmark index is
ready yet; any other text after ``R`` is a Responsa query, digits only too
(``R 99 [3] 100``) (Codex review of PR #52).

The shelfmark branch hands the string to ``/browse`` unchanged, so the browse
page's own lookup (``search_shelfmark`` in ``web/pages/browse.py``) decides what
it matches. The text branch hands it to /search, which runs it in its default
text mode (and, if its own recogniser disagrees, as a shelfmark there).

Pure (no NiceGUI import) so it is unit-tested directly:
``tests/test_home_query_routing.py``.
"""
from __future__ import annotations

import re
from typing import Callable, Optional
from urllib.parse import quote

# Re-exported: tests/test_home_query_routing.py and older callers import it from here.
from shared.search_box_input import go_to_kind, has_hebrew_or_arabic_letter  # noqa: F401
from shared.search_mode_signs import split_mode_sign

#: The shape of every libraries.csv shelfmark that begins with ``R `` -- the Responsa sign --
#: (``R 1`` .. ``R 34`` Mosseri, ``R II 5`` Turin; 33 on 2026-11-18): an optional Roman
#: numeral, then a number. ``tests/test_home_query_routing.py`` checks it against the file.
R_SHELFMARK_RE = re.compile(r'R (?:[IVXLC]+ )?\d+[A-Za-z]?')


def home_query_target(
    text: Optional[str], has_shelfmark: Optional[Callable[[str], bool]] = None,
    *, catalog_served: bool = False,
) -> Optional[str]:
    """The URL a go-to box navigates to for ``text``, or None if blank.

    A search-mode sign and a space (``$ שלום``) open /search in that mode with the
    rest as its query (``mode_sign_target``); ``%`` only when *catalog_served*.
    ``has_shelfmark(query)`` says whether the catalogue holds ``query`` as a
    shelfmark; it is asked only about text with a digit and no Hebrew or Arabic
    letter that the recogniser did not take. Without it the recogniser alone decides.
    The decision is ``shared/search_box_input.py::go_to_kind``, which the desktop's
    Go to box (``shared/quick_open.py``) shares.
    """
    signed = mode_sign_target(text, catalog_served=catalog_served)
    if signed is not None:
        return signed
    kind = go_to_kind(text, has_shelfmark)
    if kind is None:
        return None
    query = text.strip()
    if kind == 'sys_id':
        return f'/browse?sys_id={query}'
    if kind == 'shelfmark':
        return f'/browse?shelfmark={quote(query, safe="")}'
    return f'/search?q={quote(query, safe="")}'


def mode_sign_target(text: Optional[str], *, catalog_served: bool = False) -> Optional[str]:
    """``/search?q=<query>&mode=<mode>`` when *text* begins with a search-mode sign and a
    space, else None. The sign alone (``$ ``) opens /search in its mode with an empty box.
    ``R``, the one sign that is a letter, also begins shelfmarks: text of their shape
    (``R_SHELFMARK_RE``: ``R 15``, ``R II 5``) is None, so it is routed as any other input;
    any other text after ``R`` is a Responsa query, digits only too (``R 99 [3] 100``).
    Decided from the text alone, never from the catalogue lookup, which says False until
    its index is built (Codex review of PR #52)."""
    mode, query = split_mode_sign((text or '').lstrip(), catalog_served=catalog_served)
    if mode is None:
        return None
    if mode == 'responsa' and R_SHELFMARK_RE.fullmatch(text.strip()):
        return None
    query = query.strip()
    if not query:
        return f'/search?mode={mode}'
    return f'/search?q={quote(query, safe="")}&mode={mode}'


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
