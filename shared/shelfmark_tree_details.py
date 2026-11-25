# -*- coding: utf-8 -*-
"""What both apps' shelf pickers show about the shelfmark clicked last, in the pane beside the
columns: a peek at the manuscript before it is opened. The reader is in the picker, so the library
and the place on the shelf are not repeated; the pane says what the manuscript has.

``item_details`` answers, from local data only (the app's catalogue rows, the text index, the FJMS,
PGP and FGP sidecars, through ``shared.fragment_details`` where it can): the shelfmark and all its
other call numbers, the title, the subjects and the date, the pages the text index holds and how
many have text, the text of the first pages as stored (``read_pages``: the pane's scrolling text
box), the scholarly transcriptions (PGP, FGP), the joins the sidecars know (PGP's documents of
several fragments, FJMS's join groups), how many catalogue and bibliography entries FJMS has, and
the links (Ktiv, the library's viewer: CUDL, Manchester, Princeton, the BL, Oxford; PGP). It never
raises: a source that fails or is missing leaves its part out. Call it off the UI thread all the
same, these are SQLite and index reads. ``field_lines`` is the pane's labelled fields, the same in
both apps. A join partner the shelf holds is named as its row is (``_shelf_names``).

What only the community database knows -- the joins readers made and their comments -- each app
reads with its own client, off the UI thread, and adds: ``add_joins`` (one partner once, its
sources named) and ``details['comments']``.

``thumbnail_url`` and ``page_image_urls`` are apart: finding a manuscript's images may ask NLI for
its image ids, so a picker shows the text first and the image when it comes.

Pure: no Qt, no NiceGUI.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable, Iterable, Optional

from shared.fragment_details import domain_labels, fetch_fragment_details
from shared.shelfmark_tree_columns import call_numbers_once, restates

logger = logging.getLogger(__name__)

#: Transcription sources, in the order the pane names them.
TRANSCRIPTION_SOURCES = ('pgp', 'fgp')
TRANSCRIPTION_LABELS = {'pgp': 'PGP', 'fgp': 'FGP'}
#: Join sources, in the order the pane names them.
JOIN_SOURCES = ('pgp', 'fjms', 'community')

#: The text box: the pages it shows at most, and the characters (a longer page is cut there).
TEXT_BOX_PAGES = 3
TEXT_BOX_CHARS = 6000
#: A manuscript longer than this is a microfilm whose first image is a title page: the text box
#: starts there only when no other page has text.
LONG_MANUSCRIPT_PAGES = 20
#: What reads as text: enough letters, mostly letters, words longer than scattered marks.
READABLE_LETTERS = 120
#: The letters asked of a short manuscript's page (at most ``LONG_MANUSCRIPT_PAGES`` pages) to read:
#: a small fragment holds a few short lines, and has no title page to keep out.
SHORT_MANUSCRIPT_LETTERS = 30
READABLE_LETTER_SHARE = 0.75
READABLE_WORD_LENGTH = 2.5

_WORD = re.compile(r"\S+")


def other_shelfmarks(leaf) -> list:
    """Every other call number of a shelfmark: the apps' own shelfmark where the picker shows FGP's,
    then the record's other call numbers, each once, in the record's order (the long "Library ...
    Ms." forms too: the pane has room, unlike the tooltip). One that only restates the shelfmark is
    left out, and of two forms of one call number only the short one is kept ("Ms. T-S AS 39.7" of
    "T-S AS 39.7": ``restates``; ``call_numbers_once``)."""
    shown = leaf.label or leaf.shelfmark
    names = []
    for i, name in enumerate((leaf.shelfmark, *(leaf.aliases or '').split(' | '))):
        name = (name or '').strip()
        if not name or restates(name, shown) or (i and restates(name, leaf.shelfmark or '')):
            continue
        names.append(name)
    return call_numbers_once(names)


def readable(text: str, min_letters: int = READABLE_LETTERS) -> bool:
    """A page that reads as text, not a garbled title page or a few stray marks: at least
    ``min_letters`` letters, letters at least ``READABLE_LETTER_SHARE`` of what is not space,
    and words of ``READABLE_WORD_LENGTH`` characters on average."""
    text = text or ''
    solid = [ch for ch in text if not ch.isspace()]
    letters = sum(1 for ch in solid if ch.isalpha())
    words = _WORD.findall(text)
    if letters < min_letters or not solid or not words:
        return False
    return letters / len(solid) >= READABLE_LETTER_SHARE and len(solid) / len(words) >= READABLE_WORD_LENGTH


def _as_is(text: str) -> str:
    """A page's text as stored, for the text box: its line breaks one kind, the blank around it
    dropped; nothing within it changed."""
    return (text or '').replace('\r\n', '\n').replace('\r', '\n').strip()


def read_pages(page_texts: Iterable[str], pages: Optional[int] = None) -> dict:
    """One reading of a manuscript's pages (``page_texts``: all of them, in Browse's order) for the
    pane: ``text_pages``, how many have text (None when fewer pages came than ``pages``, the count
    the browse map gives); and ``text``, the text box, ``[{'page': its place in Browse, 1 up,
    'text': as stored}]``. The box starts at the first page that reads
    (``readable``; a long manuscript's first page, a microfilm's title page, only when no other page
    has text) or, when none reads, at the first with any text -- shown as is, whatever it holds --
    and goes on with the next pages with text, ``TEXT_BOX_PAGES`` and ``TEXT_BOX_CHARS`` at most."""
    texts = [text or '' for text in page_texts]
    count = pages if pages is not None else len(texts)
    with_text = [i for i, text in enumerate(texts) if text.strip()]
    out = {'text_pages': len(with_text) if pages is None or len(texts) >= pages else None, 'text': []}
    candidates = [i for i in with_text if not (i == 0 and count > LONG_MANUSCRIPT_PAGES)] or with_text
    floor = SHORT_MANUSCRIPT_LETTERS if 0 < count <= LONG_MANUSCRIPT_PAGES else READABLE_LETTERS
    start = next((i for i in candidates if readable(texts[i], floor)), candidates[0] if candidates else None)
    if start is None:
        return out
    used = 0
    for i in with_text:
        if i < start:
            continue
        if len(out['text']) >= TEXT_BOX_PAGES or used >= TEXT_BOX_CHARS:
            break
        text = _as_is(texts[i])
        if len(text) > TEXT_BOX_CHARS - used:
            text = text[:TEXT_BOX_CHARS - used].rstrip() + '…'
        out['text'].append({'page': i + 1, 'text': text})
        used += len(text)
    return out


def _same_shelfmark(a: str, b: str) -> bool:
    return bool(a) and ' '.join(a.split()).casefold() == ' '.join(b.split()).casefold()


def add_joins(details: dict, partners: Iterable[tuple], source: str) -> None:
    """Add join partners (``(sys_id, shelfmark)``) from ``source`` to ``details['joins']``: a partner
    the pane has from another source is not added again, it gains the source. One is the same
    partner by its id, or by its shelfmark where either side has no id (the community's rows may
    carry only the shelfmark); two different ids are two partners. The manuscript itself is left out,
    by its id or its shelfmark."""
    joins = details.setdefault('joins', [])
    own, own_shelfmark = details.get('sys_id') or '', details.get('shelfmark') or ''
    for sys_id, shelfmark in partners or ():
        sys_id, shelfmark = (sys_id or '').strip(), (shelfmark or '').strip()
        if not (sys_id or shelfmark) or (sys_id and sys_id == own):
            continue
        if not sys_id and _same_shelfmark(shelfmark, own_shelfmark):
            continue
        found = next((j for j in joins if (sys_id and j['sys_id'] == sys_id)
                      or ((not sys_id or not j['sys_id']) and _same_shelfmark(shelfmark, j['shelfmark']))), None)
        if found is None:
            joins.append({'sys_id': sys_id, 'shelfmark': shelfmark, 'sources': [source]})
            continue
        if source not in found['sources']:
            found['sources'].append(source)
        if not found['shelfmark'] and shelfmark:
            found['shelfmark'] = shelfmark
        if not found['sys_id'] and sys_id:
            found['sys_id'] = sys_id


def _shelfmark_of(meta_mgr: Any, sys_id: str) -> str:
    try:
        return ((getattr(meta_mgr, 'csv_bank', None) or {}).get(sys_id) or {}).get('shelfmark') or ''
    except Exception:   # noqa: BLE001
        return ''


def _transcriptions(sys_id: str) -> list:
    """The scholarly transcriptions of a manuscript: PGP's editions and FGP's digital editions,
    the rule of the catalogue's editions filter and of the rows' sign (``shelfmark_tree_signs``), so
    the pane and the sign never disagree. A translation alone is not a transcription."""
    out = []
    try:
        from shared.document_service import get_sys_ids_with_editions
        if sys_id in get_sys_ids_with_editions([sys_id]):
            out.append('pgp')
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: PGP editions lookup failed for %s: %s", sys_id, exc)
    try:
        from shared.fgp_service import get_sys_ids_with_fgp_editions   # honours the FGP switch
        if sys_id in get_sys_ids_with_fgp_editions([sys_id]):
            out.append('fgp')
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: FGP editions lookup failed for %s: %s", sys_id, exc)
    return out


def _local_joins(details: dict, meta_mgr: Any) -> None:
    sys_id = details['sys_id']
    try:
        from shared.document_service import get_document_ids_for_fragment, get_fragments_for_document
        # every PGP document of the fragment (one for its recto and one for its verso), not one chosen
        for pgpid in get_document_ids_for_fragment(sys_id) or ():
            # the apps' own shelfmark where the catalogue has the partner, PGP's spelling otherwise
            add_joins(details, ((f.get('sys_id') or '',
                                 _shelfmark_of(meta_mgr, f.get('sys_id') or '') or f.get('shelfmark') or '')
                                for f in get_fragments_for_document(pgpid)), 'pgp')
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: PGP joins failed for %s: %s", sys_id, exc)
    try:
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        if fjms.is_available():
            add_joins(details, ((str(m.get('alma_id') or ''), _shelfmark_of(meta_mgr, str(m.get('alma_id') or '')))
                                for m in fjms.get_join_group(sys_id)), 'fjms')
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: FJMS joins failed for %s: %s", sys_id, exc)


def _shelf_names(details: dict, service) -> None:
    """Each partner the shelf holds named as the picker's rows name it (the tree's label of its first
    place), so a join reads as the row its link moves to; one off the shelf keeps its catalogue or
    PGP name."""
    for join in details.get('joins') or ():
        if not join.get('sys_id') or service is None:
            continue
        try:
            places = service.path_of(join['sys_id'])
        except Exception as exc:   # noqa: BLE001 -- the name it has, never a crash
            logger.debug("shelf details: no place for join partner %s: %s", join['sys_id'], exc)
            continue
        if places:
            join['shelfmark'] = places[0].leaf.label or places[0].leaf.shelfmark or join['shelfmark']


def _catalogue_counts(details: dict) -> None:
    sys_id = details['sys_id']
    try:
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        if fjms.is_available():
            details['catalogue'] = len((fjms.get_catalog_detail(sys_id) or {}).get('records') or [])
            details['bibliography'] = len(fjms.get_bibliography(sys_id) or [])
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: FJMS catalogue counts failed for %s: %s", sys_id, exc)


def item_details(service, leaf, lang: str = 'en', *, meta_mgr: Any = None,
                 page_count: Optional[Callable[[str], int]] = None,
                 page_texts: Optional[Callable[[str], Iterable[str]]] = None) -> dict:
    """The pane's contents for a shelfmark (a ``ShelfLeaf``): ``sys_id``, ``shelfmark`` (as the
    picker shows it), ``others``, ``part``, ``title``, ``subjects``, ``date``, ``pages`` (what the
    text index holds; None when not known), ``text_pages`` and ``text`` (``read_pages`` of
    ``page_texts(sys_id)``, all the index's pages in order), ``transcriptions`` (the scholarly
    ones, ``TRANSCRIPTION_SOURCES`` present),
    ``joins`` (``sys_id``, ``shelfmark``, ``sources``), ``comments`` (None: the app's community
    client fills it), ``catalogue`` and ``bibliography`` (FJMS's entries; None when not known) and
    ``links`` (``key``, ``label``, ``url``). ``service`` is the shelf tree: a partner it holds is
    named as the rows name it (``_shelf_names``)."""
    out = {'sys_id': leaf.sys_id, 'shelfmark': leaf.label or leaf.shelfmark or leaf.sys_id,
           'others': other_shelfmarks(leaf), 'part': leaf.part or '', 'title': '', 'subjects': [],
           'date': '', 'pages': None, 'text_pages': None, 'text': [], 'transcriptions': [], 'joins': [],
           'comments': None, 'catalogue': None, 'bibliography': None, 'links': []}
    try:
        found = fetch_fragment_details(leaf.sys_id, meta_mgr=meta_mgr)
        out['title'] = found.get('title') or ''
        out['subjects'] = domain_labels(found.get('domains') or [], hebrew=lang == 'he')
        out['date'] = found.get('dating') or ''
        out['links'] = list(found.get('links') or [])
    except Exception as exc:   # noqa: BLE001
        logger.debug("shelf details: catalogue lookup failed for %s: %s", leaf.sys_id, exc)
    if page_count is not None:
        try:
            out['pages'] = max(0, int(page_count(leaf.sys_id) or 0))
        except Exception as exc:   # noqa: BLE001 -- unknown, not none
            logger.debug("shelf details: no page count for %s: %s", leaf.sys_id, exc)
    if out['pages'] == 0:
        out['text_pages'] = 0
    elif page_texts is not None:
        try:
            out.update(read_pages(page_texts(leaf.sys_id) or (), out['pages']))
        except Exception as exc:   # noqa: BLE001 -- no text, never a crash
            logger.debug("shelf details: no text for %s: %s", leaf.sys_id, exc)
    out['transcriptions'] = _transcriptions(leaf.sys_id)
    _local_joins(out, meta_mgr)
    _shelf_names(out, service)
    _catalogue_counts(out)
    return out


def field_lines(details: dict, tr: Callable[[str], str]) -> list:
    """The pane's labelled fields, the same in both apps: ``[(key, [(label, value), ...])]``, one
    entry a line, labels and values translated, only what is known: the pages and how many have
    text; the catalogue and bibliography entries (none too, once FJMS answered); the scholarly
    transcriptions; the readers' comments; the date; the subjects. The joins and the links are each
    app's own blocks (``details['joins']``: a tick box and a link a partner; ``details['links']``)."""
    out = []
    pages, with_text = details.get('pages'), details.get('text_pages')
    if pages:
        line = [(tr('Pages'), f"{pages:,}")]
        if with_text is not None:
            line.append((tr('With text'), f"{with_text:,}"))
        out.append(('pages', line))
    elif pages == 0:
        out.append(('pages', [(tr('Pages with text'), '0')]))
    cat = [(tr(label), f"{details[key]:,}") for key, label in (('catalogue', 'Catalog entries'),
                                                              ('bibliography', 'Bibliography entries'))
           if details.get(key) is not None]
    if cat:
        out.append(('catalogue', cat))
    names = [TRANSCRIPTION_LABELS[s] for s in TRANSCRIPTION_SOURCES if s in (details.get('transcriptions') or ())]
    if names:
        out.append(('transcriptions', [(tr('Scholarly transcriptions'), ', '.join(names))]))
    if details.get('comments'):
        out.append(('comments', [(tr('Comments'), f"{details['comments']:,}")]))
    if details.get('date'):
        out.append(('date', [(tr('Date'), details['date'])]))
    if details.get('subjects'):
        out.append(('subjects', [(tr('Subjects'), ', '.join(details['subjects']))]))
    return out


#: The width NLI's thumbnail address asks for (``MetadataManager._resolve_thumbnail``).
_NLI_THUMB_SIZE = '/full/400,/'


def sized_image_url(url: str, width: int) -> str:
    """An NLI image address (``.../full/400,/0/default.jpg``) asking for ``width`` pixels instead; any
    other address as it is."""
    if url and width and _NLI_THUMB_SIZE in url:
        return url.replace(_NLI_THUMB_SIZE, f'/full/{int(width)},/', 1)
    return url


def thumbnail_url(sys_id: str, meta_mgr: Any, width: int = 400) -> str:
    """The manuscript's first image, ``width`` pixels wide, from NLI's image server ('' when it has
    none or the lookup fails). May ask NLI: call it off the UI thread, after the text is shown."""
    if not sys_id or meta_mgr is None:
        return ''
    try:
        return sized_image_url(meta_mgr.get_thumbnail(sys_id, size=320) or '', width)
    except Exception as exc:   # noqa: BLE001 -- no image, never a crash
        logger.debug("shelf details: no thumbnail for %s: %s", sys_id, exc)
        return ''


def page_image_urls(sys_id: str, meta_mgr: Any, width: int = 400) -> list:
    """Every image of the manuscript, in its order, ``width`` pixels wide, from NLI's image server (the
    list Browse shows: ``MetadataManager.fetch_volume_manifest``); [] when it has none or the lookup
    fails. Asks NLI for the manifest when it is not kept: call it off the UI thread."""
    if not sys_id or meta_mgr is None:
        return []
    try:
        images = (meta_mgr.fetch_volume_manifest(sys_id, 1) or {}).get('images_nli') or []
    except Exception as exc:   # noqa: BLE001 -- the first image alone, never a crash
        logger.debug("shelf details: no image list for %s: %s", sys_id, exc)
        return []
    return [f"{image['url']}/full/{int(width)},/0/default.jpg" for image in images
            if isinstance(image, dict) and image.get('url')]


#: The quick look's picture: asked for at a width that fills its box, in steps (an address seen again
#: comes from a cache), never narrower than the tiles' nor wider than this.
LOOK_STEP = 200
LOOK_MIN = 400
LOOK_MAX = 1600
#: Zoomed in, the quick look's picture is asked for again this many times wider, up to LOOK_SHARP_MAX.
LOOK_SHARP_FACTOR = 2
LOOK_SHARP_MAX = 2000


def look_width(box_width: float, box_height: float, aspect: float = 0.0) -> int:
    """The width a picture is asked for to fill a box of ``box_width`` x ``box_height`` device pixels,
    ``aspect`` its width over its height (a portrait leaf, 3:4, when not known): the fitted width,
    rounded up to ``LOOK_STEP``, between ``LOOK_MIN`` and ``LOOK_MAX``. Asked by width alone (``w,``),
    the size every IIIF server answers."""
    aspect = aspect if aspect and aspect > 0 else 0.75
    fitted = min(max(0.0, box_width), max(0.0, box_height) * aspect)
    stepped = -(-int(fitted) // LOOK_STEP) * LOOK_STEP
    return max(LOOK_MIN, min(LOOK_MAX, stepped))


def look_sharp_width(width: int) -> int:
    """The width of the zoomed picture's sharper copy (``look_width``'s, larger), stepped as it is."""
    return max(width, min(LOOK_SHARP_MAX, -(-int(width * LOOK_SHARP_FACTOR) // LOOK_STEP) * LOOK_STEP))


_FL_ID = re.compile(r'/FL(\d+)(?:/|$)')


#: Libraries whose images are microfilm frames: a START target first, then a catalogue card, the leaves
#: after them (the National Library of Russia's; owner, 2026-11-25).
MICROFILM_LIBRARIES = frozenset({'RNL'})


def middle_image(total: int) -> int:
    """The middle one of ``total`` images, at least the third; the first when there are fewer than three."""
    return min(total - 1, max(2, total // 2)) if total >= 3 else 0


def is_microfilm(library_code: str) -> bool:
    """A library whose images are microfilm frames (``MICROFILM_LIBRARIES``)."""
    return (library_code or '').strip().upper() in MICROFILM_LIBRARIES


def opening_image(library_code: str, total: int) -> int:
    """Which of a manuscript's ``total`` images the details and the quick look open on: its first; a
    microfilm's (``MICROFILM_LIBRARIES``, three images or more) its middle one, at least its third: past
    the START target and the catalogue card, among the leaves."""
    return middle_image(total) if is_microfilm(library_code) else 0


def image_key(url: str) -> str:
    """What names an image whatever address and width ask for it: NLI's image id (``FL...``), else the
    address itself."""
    found = _FL_ID.search(url or '')
    return f"FL{found.group(1)}" if found else (url or '')


def image_index(urls: list, url: str) -> Optional[int]:
    """Which of ``urls`` (a manuscript's images, NLI's addresses) is the image of ``url``, by NLI's image id
    (``FL...``), whatever width either asks for; None when none is."""
    found = _FL_ID.search(url or '')
    if not found:
        return None
    for i, other in enumerate(urls):
        hit = _FL_ID.search(other or '')
        if hit and hit.group(1) == found.group(1):
            return i
    return None
