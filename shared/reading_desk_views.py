# -*- coding: utf-8 -*-
"""Reading Desk, image side: the rules of its three views, for both apps.

The desk shows its fragments' images in one of three views:

* ``one``  -- one image at a time beside a strip of thumbnails. The default on a desk that is
  a PGP document's: the reader follows one text against one image.
* ``all``  -- every image of every fragment, one under another. The default on any other
  desk, and what the desk always was.
* ``join`` -- puzzle images: the pictures of the saved puzzles of these fragments
  (``shared/reading_desk_puzzles.py``), offered on a desk of two or more fragments.

With one image on show and one text per fragment, the text side shows that image's text alone:
its fragment, and its page of the fragment when the page can be told (``text_page_for_image``).

The fragments are in the desk's order, which the reader changes with the up / down buttons
beside each fragment's name (``move_entry``).

The same rules are in the web desk (``web/pages/browse_desk_images.py``), which keeps its
own copies until its open puzzle-images and fragment-order changes have landed.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Optional

VIEW_ONE = 'one'
VIEW_ALL = 'all'
VIEW_JOIN = 'join'
VIEWS = (VIEW_ONE, VIEW_ALL, VIEW_JOIN)


def image_view(wanted: Optional[str], document_desk: bool, join_offered: bool = False) -> str:
    """The reader's choice, else one at a time on a document's desk and all images on any
    other. The puzzle images are theirs only while they are on offer."""
    if wanted in (VIEW_ONE, VIEW_ALL) or (wanted == VIEW_JOIN and join_offered):
        return wanted
    return VIEW_ONE if document_desk else VIEW_ALL


def join_is_offered(sys_ids: Iterable[str], can_arrange: bool = True) -> bool:
    """The puzzle images are on offer: the Fragment Puzzle is there to arrange in, and the
    desk holds two or more fragments. One rule for the image side and for the text side,
    which must agree on the view the reader is in."""
    return bool(can_arrange) and len({s for s in sys_ids if s}) >= 2


def _document_has_text(pgpid: int) -> bool:
    from shared.document_service import get_editions_for_document, get_translations_for_document
    return any((s.get('content') or '').strip()
               for s in (get_editions_for_document(pgpid) or []) + (get_translations_for_document(pgpid) or []))


def is_document_desk(pgpid: Optional[int], sys_ids: Iterable[str],
                     fragments_of: Optional[Callable[[int], List[Dict[str, Any]]]] = None,
                     has_text: Optional[Callable[[int], bool]] = None) -> bool:
    """The desk shows a PGP document's text: it was opened for ``pgpid``, every fragment on it
    is one of that document's (removing some keeps it; adding one from elsewhere ends it), and
    the document has an edition or a translation with text -- the web desk's rule for its
    document mode (``has_document_text``). ``fragments_of``: pgpid -> the document's fragment
    rows; ``has_text``: pgpid -> whether it has such a text (both default to the PGP sidecar).
    A sidecar that cannot be read answers no."""
    on_desk = {str(s) for s in sys_ids if s}
    if not pgpid or not on_desk:
        return False
    if fragments_of is None:
        from shared.document_service import get_fragments_for_document as fragments_of
    try:
        of_document = {str(f.get('sys_id')) for f in (fragments_of(pgpid) or []) if f.get('sys_id')}
        if not of_document or not on_desk <= of_document:
            return False
        return bool((has_text or _document_has_text)(pgpid))
    except Exception:
        return False


def move_entry(entries: List[Any], sys_id: str, delta: int) -> Optional[List[Any]]:
    """The desk's fragments with ``sys_id`` moved ``delta`` places (-1 up, +1 down); None when
    it is not on the desk or would leave it (the first moved up, the last moved down). The
    entries are dicts (the website's) or objects with ``sys_id`` (the desktop's)."""
    def sid(entry: Any) -> str:
        return entry.get('sys_id') if isinstance(entry, dict) else getattr(entry, 'sys_id', '')

    index = next((i for i, e in enumerate(entries) if sid(e) == sys_id), None)
    if index is None or not 0 <= index + delta < len(entries):
        return None
    moved = list(entries)
    moved.insert(index + delta, moved.pop(index))
    return moved


def page_label(page_index: int, pages_in_fragment: int, tr: Callable[[str], str] = lambda s: s) -> str:
    """Recto / Verso for a fragment of one leaf (two images); a plain number beyond that,
    where the side cannot be told from the position."""
    if pages_in_fragment <= 2:
        return tr('Recto') if page_index == 0 else tr('Verso')
    return str(page_index + 1)


def image_key(sys_id: str, image_index: int) -> str:
    """The key an image is chosen by: its fragment and its position in the fragment."""
    return f'{sys_id}:{image_index}'


def shown_image(images: List[Dict[str, Any]], wanted_key: Optional[str],
                current_sys_id: str = '') -> Optional[Dict[str, Any]]:
    """The image on show in the one-at-a-time view: the reader's, else the first image of the
    fragment they came from, else the first. ``images``: dicts with ``key`` and ``sys_id``."""
    if not images:
        return None
    return (next((i for i in images if wanted_key and i['key'] == wanted_key), None)
            or next((i for i in images if current_sys_id and i['sys_id'] == current_sys_id), None)
            or images[0])


_FL_DIGITS = re.compile(r'FL(\d+)', re.IGNORECASE)


def image_fl_id(image: Dict[str, Any]) -> str:
    """The NLI FL id of an image (its digits), from its ``fl_id`` or its address; '' for an
    image with none (a library's own image: Cambridge, the Bodleian...)."""
    fl_id = re.sub(r'\D', '', str(image.get('fl_id') or ''))
    if fl_id:
        return fl_id
    match = _FL_DIGITS.search(str(image.get('url') or ''))
    return match.group(1) if match else ''


def text_page_for_image(image: Dict[str, Any], pages: List[Dict[str, Any]]) -> Optional[int]:
    """The position, among a fragment's text ``pages``, of the page shown in ``image``; None
    when that image has no text page (a cover, a ruler, a page with no text): its text alone
    is shown, never every page of the fragment together (owner, 2026-11-22).

    By the NLI image id when a page records the image's own; else by the page number, page n
    with the fragment's n-th image (``image['index']`` + 1), which is how Browse pairs a page
    with its image. The id rarely decides: a transcription page records the FL id of its own
    file, which is not the image's (measured 2026-11-22 on ENA 2740.3, ENA 2805.2 and
    Bodl. MS heb. f 56/59: 0 of 6 pages). The number counts the page itself, not its place
    among the pages that have text, so a page with no text does not shift the rest. Where a
    library's own images are shown in an order of their own (Cambridge's Or. 2245,
    ``shared/nli_crossref_service.py``), the pairing is Browse's, right or wrong alike."""
    fl_id = image_fl_id(image)
    if fl_id:
        for position, page in enumerate(pages or []):
            if re.sub(r'\D', '', str(page.get('fl_id') or '')) == fl_id:
                return position
    index = image.get('index')
    if index is None:
        return None
    for position, page in enumerate(pages or []):
        try:
            if int(page.get('p_num')) == int(index) + 1:
                return position
        except (TypeError, ValueError):
            continue
    return None
