# -*- coding: utf-8 -*-
"""The shelfmarks pane's table and thumbnail views, in both apps' shelf pickers: which view a reader
chose, the table's columns, and the details of a page of shelfmarks, so the web and the desktop draw
the same values (``SHELFMARK_PICKER_VIEWS_PLAN.md`` in the private repository, VX1-VX9).

The pane shows a node's shelfmarks as a list (the picker as it was), a table (VX3: the desktop search
results table's columns -- Shelfmark, Title, Snippet, Domain, Pages, Printed, PGP, Edition) or
thumbnails (each manuscript's first image, in the Joins Lab's three sizes). ``normal_view`` and
``normal_size`` read a stored choice: anything unknown is the list and the medium size, never an error.

``row_details`` answers, for a page of shelfmarks at once (the picker pages its leaves,
``PAGE_SIZE``), each manuscript's title, snippet, domains, pages, printed, PGP, edition and, where the
app can name it without the network, its thumbnail's address. The sources are batched lookups:

- title: the app's catalogue row (``libraries.csv``, as the rows' tooltips have it);
- snippet: ``snippet`` of the first pages the text index holds (read one at a time, at most
  ``SNIPPET_PAGES`` a manuscript);
- pages: the text index's count (``SearchEngine.page_count``: the browse map, no text read);
- printed and domains: FJMS (``get_printed_sys_ids``, ``get_domains_for_sys_ids``), the domains named
  as the details pane names them (``shared.fragment_details.domain_labels``);
- PGP: a PGP document names the fragment (``get_sys_ids_with_transcriptions``: the search results'
  green badge, and the row's ``pgp`` sign);
- edition: a scholarly transcription, PGP's editions or FGP's digital editions: the rule of the row's
  ``edition`` sign and of the details pane (``shared.shelfmark_tree_signs``, DX35), so the column, the
  sign and the pane never disagree.

A source that fails or is missing leaves its field None (not known: the cell stays blank) and the
others stand. It never raises. Blocking (SQLite and index reads): call it off the UI thread / event
loop. Its domains are named in ``lang``: a cache of its answers is one language's.

Pure: no Qt, no NiceGUI.
"""
from __future__ import annotations

import logging
from itertools import islice
from typing import Any, Callable, Iterable, Mapping, Optional

from shared.shelfmark_tree_details import (
    LONG_MANUSCRIPT_PAGES, READABLE_LETTERS, SHORT_MANUSCRIPT_LETTERS, readable,
)
from shared.shelfmark_tree_view import PAGE_SIZE

logger = logging.getLogger(__name__)

#: The pane's views, in the order the switch shows them.
VIEWS = ('list', 'table', 'gallery')
#: The thumbnails' sizes, the Joins Lab's keys (small, medium, large, extra large).
SIZES = ('s', 'm', 'l', 'xl')
DEFAULT_VIEW, DEFAULT_SIZE = 'list', 'm'
#: What each view and size is called (English; each app translates).
VIEW_LABELS = {'list': 'List', 'table': 'Table', 'gallery': 'Thumbnails'}
SIZE_LABELS = {'s': 'Small', 'm': 'Medium', 'l': 'Large', 'xl': 'Extra large'}
#: Where the choice is kept: the web's per-reader storage key, the desktop's app-config key.
STORAGE_KEY = 'shelf_picker_view'
#: Whether the details pane is shown (the reader may hide it, to give the shelfmarks its room), kept
#: under the same two stores; shown when nothing is kept.
DETAILS_KEY = 'shelf_picker_details'


def details_shown(value) -> bool:
    """A kept details-pane choice: hidden only when ``False`` was kept (anything else: shown)."""
    return value is not False

#: The table's columns, in order: each one's key and its header (English, the desktop search results
#: table's word; Pages is the picker's own: neither results view counts pages).
COLUMNS = (('shelfmark', 'Shelfmark'), ('title', 'Title'), ('snippet', 'Snippet'), ('domain', 'Domain'),
           ('pages', 'Pages'), ('printed', 'Printed'), ('pgp', 'PGP'), ('edition', 'Edition'))
#: The headers' tooltips (English): the results table's for PGP and Printed; Edition says the picker's
#: rule, the edition sign's.
HEADER_TIPS = {'pgp': 'Scholarly transcriptions/data available from the Princeton Geniza Project',
               'printed': 'Printed material (not handwritten manuscript)',
               'edition': 'Scholarly transcription'}

#: The snippet: the pages read at most for it, and its length on its one line.
SNIPPET_PAGES = 8
SNIPPET_CHARS = 160
#: Between a page's lines on the snippet's one line, as the search results' snippets have it.
LINE_MARK = ' ‖ '

#: The fields of a row's details; None is "not known".
FIELDS = ('title', 'snippet', 'domains', 'pages', 'printed', 'pgp', 'edition', 'thumb')


def normal_view(value: Any) -> str:
    """A stored view, or the list when it is none of ``VIEWS``."""
    return value if isinstance(value, str) and value in VIEWS else DEFAULT_VIEW


def normal_size(value: Any) -> str:
    """A stored thumbnail size, or the medium one when it is none of ``SIZES``."""
    return value if isinstance(value, str) and value in SIZES else DEFAULT_SIZE


def read_choice(raw: Any) -> tuple:
    """``(view, size)`` from what was stored (``choice_value``'s dict); anything else is the default."""
    raw = raw if isinstance(raw, Mapping) else {}
    return normal_view(raw.get('view')), normal_size(raw.get('size'))


def choice_value(view: str, size: str) -> dict:
    """What is stored for a choice."""
    return {'view': normal_view(view), 'size': normal_size(size)}


def folds_columns(view: str, has_leaves: bool) -> bool:
    """The layout rule (VX2, the owner's): in the table and thumbnail views the shelfmarks take the
    room and only the last node column stays; with no shelfmarks pane shown there is nothing to give
    it to, and every column is drawn, as in the list."""
    return normal_view(view) != 'list' and bool(has_leaves)


def shown_columns(columns: list, view: str, has_leaves: bool, wide: int) -> list:
    """The node columns a picker draws: the last ``wide`` (the list), or the last one alone
    (``folds_columns``)."""
    return list(columns[-1:] if folds_columns(view, has_leaves) else columns[-wide:])


def _one_line(text: str) -> str:
    """A page's text on one line: its lines joined by ``LINE_MARK``, the blank inside a line kept to
    one space, cut at ``SNIPPET_CHARS`` (at a space where there is one near) with an ellipsis."""
    lines = [' '.join(line.split()) for line in (text or '').replace('\r', '\n').split('\n')]
    line = LINE_MARK.join(x for x in lines if x)
    if len(line) <= SNIPPET_CHARS:
        return line
    cut = line[:SNIPPET_CHARS]
    space = cut.rfind(' ')
    return (cut[:space] if space > SNIPPET_CHARS * 2 // 3 else cut).rstrip(' ‖') + '…'


def snippet(page_texts: Iterable[str], pages: Optional[int] = None) -> str:
    """A manuscript's snippet, from its pages in Browse's order (read one at a time, at most
    ``SNIPPET_PAGES``): the first page that reads (``readable``, with the short manuscript's floor
    when ``pages`` says it has at most ``LONG_MANUSCRIPT_PAGES``), the details pane's rule
    (``read_pages``): a long manuscript's first page -- a microfilm's title page -- only when no
    other page read has text; none reads: the first with any text; none has text: ''. On one line."""
    long = pages is not None and pages > LONG_MANUSCRIPT_PAGES
    floor = SHORT_MANUSCRIPT_LETTERS if pages is not None and 0 < pages <= LONG_MANUSCRIPT_PAGES else READABLE_LETTERS
    first_page, first_any = None, None
    for i, text in enumerate(islice(page_texts or (), SNIPPET_PAGES)):
        text = text or ''
        if not text.strip():
            continue
        if i == 0 and long:
            first_page = text                       # a title page: the last resort
            continue
        if readable(text, floor):
            return _one_line(text)
        if first_any is None:
            first_any = text
    chosen = first_any if first_any is not None else first_page
    return _one_line(chosen) if chosen is not None else ''


def wanted(sys_ids: Iterable[str], known: Mapping, size: int = PAGE_SIZE) -> list:
    """The ids not in ``known`` yet, each once, in the order given, in batches of at most ``size``:
    what a view asks ``row_details`` for, one batch at a time."""
    todo = [s for s in dict.fromkeys(str(x) for x in sys_ids if x) if s not in known]
    return [todo[i:i + size] for i in range(0, len(todo), max(1, size))]


# -- the sources (each one batch; tests replace them) -------------------------------------------
# Each answers a set (or a dict) for the ids, or None when its sidecar is not here (the cells stay
# blank, not "no"); a query that fails raises where the service can tell (the editions' ``strict``),
# and is caught by ``row_details`` for that source alone. FJMS's and PGP's own batch methods answer
# empty on a failed query: for them a failure reads as "none", as in the search results.

def _fjms_service():
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    return fjms if fjms.is_available() else None


def _printed(ids: list) -> Optional[set]:
    fjms = _fjms_service()
    return None if fjms is None else set(fjms.get_printed_sys_ids(ids) or ())


def _domains(ids: list) -> Optional[dict]:
    fjms = _fjms_service()
    return None if fjms is None else dict(fjms.get_domains_for_sys_ids(ids) or {})


def _pgp(ids: list) -> Optional[set]:
    from shared.document_service import get_pgp_service, get_sys_ids_with_transcriptions
    if not get_pgp_service().is_available():
        return None
    return set(get_sys_ids_with_transcriptions(ids) or ())


def _pgp_editions(ids: list) -> set:
    from shared.document_service import get_sys_ids_with_editions
    return set(get_sys_ids_with_editions(ids, strict=True) or ())


def _fgp_editions(ids: list) -> set:
    from shared.fgp_service import get_sys_ids_with_fgp_editions   # honours the FGP switch
    return set(get_sys_ids_with_fgp_editions(ids, strict=True) or ())


#: The edition's two sources: one that answers yes is enough; "no" needs both to have answered.
EDITION_SOURCES = ('_pgp_editions', '_fgp_editions')


def _call(name: str, ids: list):
    """A source by its name (so a test's replacement is the one called); None when it fails."""
    try:
        return globals()[name](ids)
    except Exception as exc:   # noqa: BLE001 -- this source alone is unknown
        logger.debug("shelf rows: the %s lookup failed: %s", name, exc)
        return None


def row_details(sys_ids: Iterable[str], lang: str = 'en', *, meta_mgr: Any = None,
                page_count: Optional[Callable[[str], int]] = None,
                page_texts: Optional[Callable[[str], Iterable[str]]] = None,
                thumb_of: Optional[Callable[[str], str]] = None) -> dict:
    """``{sys_id: {title, snippet, domains, pages, printed, pgp, edition, thumb}}`` for a page of
    shelfmarks (each id once): ``title`` and ``snippet`` text, ``domains`` the names in ``lang``,
    ``pages`` an int, ``printed``, ``pgp`` and ``edition`` booleans, ``thumb`` the image's address
    (``thumb_of(sys_id)``: only an app that names it without the network passes one). A field is None
    when its source is missing or failed. See the module docstring for the sources."""
    ids = list(dict.fromkeys(str(s) for s in sys_ids if s))
    out = {sid: dict.fromkeys(FIELDS) for sid in ids}
    if not ids:
        return out
    try:
        bank = getattr(meta_mgr, 'csv_bank', None) if meta_mgr is not None else None
        if bank is not None:
            for sid in ids:
                row = bank.get(sid)
                out[sid]['title'] = ' '.join(((row or {}).get('title') or '').split())
    except Exception as exc:   # noqa: BLE001 -- the titles stay unknown
        logger.debug("shelf rows: no titles: %s", exc)
    printed, domains, pgp = _call('_printed', ids), _call('_domains', ids), _call('_pgp', ids)
    editions = [_call(name, ids) for name in EDITION_SOURCES]
    if domains is not None:
        from shared.fragment_details import domain_labels
    for sid in ids:
        row = out[sid]
        if printed is not None:
            row['printed'] = sid in printed
        if domains is not None:
            row['domains'] = domain_labels(domains.get(sid) or [], hebrew=lang == 'he')
        if pgp is not None:
            row['pgp'] = sid in pgp
        if any(found is not None and sid in found for found in editions):
            row['edition'] = True
        elif all(found is not None for found in editions):
            row['edition'] = False
    for sid in ids:
        row = out[sid]
        if page_count is not None:
            try:
                row['pages'] = max(0, int(page_count(sid) or 0))
            except Exception as exc:   # noqa: BLE001 -- unknown, not none
                logger.debug("shelf rows: no page count for %s: %s", sid, exc)
        if row['pages'] == 0:
            row['snippet'] = ''
        elif page_texts is not None:
            try:
                row['snippet'] = snippet(page_texts(sid) or (), row['pages'])
            except Exception as exc:   # noqa: BLE001 -- no snippet, never a crash
                logger.debug("shelf rows: no snippet for %s: %s", sid, exc)
        if thumb_of is not None:
            try:
                row['thumb'] = thumb_of(sid) or ''
            except Exception as exc:   # noqa: BLE001 -- no image
                logger.debug("shelf rows: no thumbnail address for %s: %s", sid, exc)
    return out
