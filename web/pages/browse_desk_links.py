# -*- coding: utf-8 -*-
"""Reading Desk: each image's links, to the library that holds it and to its catalogue records.

The single page's "External link" row (``browse.py``) is built from the page's enrichment, which
the desk does not load for its fragments. Here the same links are found for one fragment of the
desk when the reader opens the links menu beside one of its images (``links_button``), off the
event loop, and kept for the life of the process (they are the same for every reader):

* that image's own leaf at the Bodleian -- recto, verso, or the image itself
  (``bodleian_links_for_page``, as Browse and Quick View find it);
* NLI Ktiv (every corpus record);
* the Bodleian part and the whole volume;
* the holding library's own viewer (Cambridge CUDL, JTS, Manchester, BL, Katz Center:
  ``NliCrossrefService.get_library_viewer_url``) and each Katz Center component;
* the PGP document the fragment belongs to (each side's, where its recto and verso are in two).

A result is kept only when every source answered, and for ten minutes: one that failed is asked again
next time, and one a service failed without saying so after that.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from nicegui import run, ui

from shared.synthetic_sys_id import is_synthetic_sys_id
from web.translations import get_language, tr, using_language

logger = logging.getLogger(__name__)

KTIV_URL = ('https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage'
            '?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{}')

# A link: the parts of its label (each translated, joined by " · ") and its address.
Link = Tuple[Tuple[str, ...], str]

_BODL_PREFIX = re.compile(r'^\s*Bodl\.\s*', re.IGNORECASE)  # as browse_desk_images strips it

_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}  # sys_id -> (when found, links)
_CACHE_MAX = 512
# How long found links are kept. The catalogue services answer a failed query as "nothing
# found" (get_library_viewer_url, get_document_for_fragment return None), which cannot be told
# from a record with no such link: a kept answer is therefore asked again after this, as the
# single page asks every time it is opened (GitHub Codex, #85).
_CACHE_TTL = 600.0
_CACHE_LOCK = threading.Lock()


def _library_code(sys_id: str, meta_mgr: Any) -> str:
    try:
        return (meta_mgr.get_library_for_id(sys_id) or '') if meta_mgr is not None else ''
    except Exception:
        return ''


def _catalogue_shelfmark(sys_id: str, shelfmark: str, meta_mgr: Any) -> str:
    """The record's own shelfmark, as the single page has it: a desk opened from a PGP document
    names a Bodleian fragment as PGP does ("Bodl. MS heb. f 56/59"), which the Bodleian's leaf
    cannot be read from."""
    try:
        own = (meta_mgr.get_meta_for_id(sys_id) or ('',))[0] if meta_mgr is not None else ''
    except Exception:
        own = ''
    return own or _BODL_PREFIX.sub('', shelfmark or '')


def fragment_links(entry: Dict[str, Any], meta_mgr: Any = None, crossref: Any = None,
                   cambridge_url: Optional[str] = None) -> Dict[str, Any]:
    """The links of one desk fragment. BLOCKING (the catalogue sidecars).

    ``{'fragment': [Link], 'pages': {page_index: [Link]}, 'complete': bool}``: ``pages`` are the
    links of one image (the Bodleian's leaf; the PGP document of that side, where its recto and
    verso are in two documents); ``fragment`` those of the whole fragment, in the order the single
    page shows them. ``cambridge_url``: the CUDL item the record's Cambridge decision named, when
    known. Each source may fail without taking the others with it; ``complete`` is False when one
    did (the result is then not kept)."""
    from web.services import bodleian_links_for_page, is_oxford_manuscript

    sys_id = str(entry.get('sys_id') or '')
    out: Dict[str, Any] = {'fragment': [], 'pages': {}, 'complete': True}
    entry_pgp = (entry.get('pgp_doc') or {}).get('pgp_url')
    if not sys_id or is_synthetic_sys_id(sys_id):
        # A catalogue-only record: nothing at NLI, but its library's item may be known (Codex CLI).
        if cambridge_url:
            out['fragment'].append((('Cambridge CUDL',), cambridge_url))
        if entry_pgp:
            out['fragment'].append((('Princeton Geniza Project',), entry_pgp))
        return out

    def failed(what: str) -> None:
        logger.debug('[ReadingDesk] %s of %s', what, sys_id, exc_info=True)
        out['complete'] = False

    fragment: List[Link] = [(('NLI Ktiv',), KTIV_URL.format(sys_id))]
    library = _library_code(sys_id, meta_mgr)
    pages = entry.get('pages') or []
    # The desk's images: each page's number, else the two sides of one leaf (desk_images).
    page_nums = [n for n in (getattr(p, 'p_num', 0) or 0 for p in pages) if n >= 1] or [1, 2]
    shelfmark = _catalogue_shelfmark(sys_id, entry.get('shelfmark') or '', meta_mgr)

    if is_oxford_manuscript(shelfmark, library):
        part_meta, volume_url = None, ''
        try:
            part_id = meta_mgr.get_part_for_folio(sys_id) if meta_mgr is not None else None
            part_meta = meta_mgr.get_part_metadata(part_id) if part_id else None
        except Exception:
            failed('Bodleian part')
        folio_images = []
        try:
            if crossref is not None and crossref.is_available():
                folio_images = crossref.get_folio_images(sys_id) or []
        except Exception:
            failed('folio images')
        codico = getattr(meta_mgr, 'codico_mgr', None)
        from shared.codicological import bodleian_image_link_items
        for p_num in page_nums:
            try:
                leaf = SimpleNamespace(sys_id=sys_id, shelfmark=shelfmark, volume_suffix=1, p_num=p_num,
                                       total_pages=len(page_nums))
                links = bodleian_links_for_page(leaf, codico, folio_images, part_meta)
            except Exception:
                failed(f'Bodleian links p{p_num}')
                continue
            volume_url = volume_url or links.get('volume', '')
            own = [(('Bodleian', label), url) for key, label, url in bodleian_image_link_items(links)
                   if key != 'volume']
            if own:
                out['pages'][p_num - 1] = own
        part_url = (part_meta or {}).get('direct_link') or ''
        if part_url and part_url != volume_url:
            fragment.append((('Bodleian part',), part_url))
        if volume_url:
            fragment.append((('Bodleian', 'Whole volume'), volume_url))

    if cambridge_url:
        fragment.append((('Cambridge CUDL',), cambridge_url))
    if library == 'Katz':
        # From the bundled references: there with or without the NLI sidecar.
        try:
            from shared.nli_crossref_service import katz_component_links, katz_viewer_url
            fragment += [(('Katz Center', c['label']), c['url']) for c in katz_component_links(sys_id) or []]
            viewer = katz_viewer_url(sys_id)
            if viewer and viewer.get('url'):
                fragment.append(((viewer.get('label') or 'Katz Center',), viewer['url']))
        except Exception:
            failed('Katz links')
    try:
        if crossref is not None and crossref.is_available():
            viewer = crossref.get_library_viewer_url(sys_id, cambridge_viewer_url=cambridge_url)
            if viewer and viewer.get('url'):
                fragment.append(((viewer.get('label') or 'View in Library Catalog',), viewer['url']))
    except Exception:
        failed('library viewer')

    # The PGP document: of each side, where the recto and verso are in two documents (the single
    # page asks by its page number); else the fragment's.
    by_side: Dict[int, str] = {}
    for p_num in [n for n in page_nums if n in (1, 2)]:
        try:
            from shared.document_service import get_document_for_fragment
            url = (get_document_for_fragment(sys_id, page_num=p_num) or {}).get('pgp_url')
        except Exception:
            failed(f'PGP document p{p_num}')
            continue
        if url:
            by_side[p_num - 1] = url
    if len(set(by_side.values())) > 1:
        for page_index, url in by_side.items():
            out['pages'].setdefault(page_index, []).append((('Princeton Geniza Project',), url))
    else:
        pgp_url = next(iter(by_side.values()), None) or entry_pgp
        if pgp_url:
            fragment.append((('Princeton Geniza Project',), pgp_url))

    seen, unique = set(), []
    for page_links in out['pages'].values():
        seen.update(url for _label, url in page_links)
    for label, url in fragment:
        if url and url not in seen:
            seen.add(url)
            unique.append((label, url))
    out['fragment'] = unique
    return out


def _resolve(entry: Dict[str, Any]) -> Dict[str, Any]:
    """``fragment_links`` with the app's catalogue, and the record's Cambridge decision (which may
    ask NLI once, as Browse does: breaker-guarded). BLOCKING. Kept per sys_id only when whole: a
    source that failed, or a Cambridge decision that could not be made yet ("unavailable"), is
    asked again at the next opening, as the single page would."""
    sys_id = str(entry.get('sys_id') or '')
    with _CACHE_LOCK:
        kept = _CACHE.get(sys_id)
        if kept is not None and time.monotonic() - kept[0] < _CACHE_TTL:
            return kept[1]
        _CACHE.pop(sys_id, None)
    from web.state import state as app_state
    meta_mgr = getattr(app_state, 'meta_mgr', None)
    whole = True
    crossref = None
    try:
        from shared.nli_crossref_service import get_nli_crossref_service
        crossref = get_nli_crossref_service(thread_safe=True)
    except Exception:
        logger.debug('[ReadingDesk] no crossref service', exc_info=True)
        whole = False
    cambridge_url = None
    if sys_id and meta_mgr is not None:  # a catalogue-only record's too, as the single page asks it
        try:
            from shared.cudl_identity import resolution_is_current
            from web.components.image_resolution import resolve_external_images
            decision = (resolve_external_images(sys_id, meta_mgr=meta_mgr) or {}).get('cambridge_resolution')
            # Only a decision made by the current rules is final: none (the enrichment failed),
            # "unavailable" (NLI could not be asked) and a provisional match made without the
            # crossref (no revision) are shown but not kept.
            if not resolution_is_current(decision):
                whole = False
            if (decision or {}).get('status') == 'match':
                cambridge_url = decision.get('viewer_url') or None
        except Exception:
            logger.debug('[ReadingDesk] Cambridge decision of %s', sys_id, exc_info=True)
            whole = False
    found = fragment_links(entry, meta_mgr, crossref, cambridge_url)
    found['complete'] = bool(whole and found.get('complete'))
    if found['complete']:
        with _CACHE_LOCK:
            if len(_CACHE) >= _CACHE_MAX:
                _CACHE.pop(next(iter(_CACHE)))
            _CACHE[sys_id] = (time.monotonic(), found)
    return found


def image_links(found: Dict[str, Any], page_index: int) -> List[Link]:
    """One image's links: its own leaf's first, then the fragment's."""
    return list((found.get('pages') or {}).get(page_index, [])) + list(found.get('fragment') or [])


def link_label(parts: Tuple[str, ...]) -> str:
    return ' · '.join(tr(part) for part in parts)


def links_button(entry: Dict[str, Any], page_index: int) -> None:
    """The links menu beside one image: filled the first time it is opened."""
    lite = {'sys_id': entry.get('sys_id'), 'shelfmark': entry.get('shelfmark'),
            'pages': list(entry.get('pages') or []), 'pgp_doc': dict(entry.get('pgp_doc') or {})}
    with ui.button(icon='open_in_new').props('flat round dense size=sm').classes('rd-links').tooltip(
            tr('External link')):
        with ui.menu().classes('rd-links-menu') as menu:
            body = ui.column().classes('p-2').style('gap: 4px; min-width: 200px;')
            with body:
                ui.spinner(size='sm', color='green')

    filled = {'done': False}
    lang = get_language()  # the menu is filled later, from a handler

    def draw(found: Dict[str, Any]) -> None:
        body.clear()
        with body, using_language(lang):
            links = image_links(found, page_index)
            if not links:
                ui.label(tr('No links')).classes('text-sm').style('color: var(--text-secondary);')
            for parts, url in links:
                ui.link(link_label(parts), url, new_tab=True).classes('rd-link text-sm')

    async def fill(e) -> None:
        if not e.value or filled['done']:
            return
        filled['done'] = True
        try:
            found = await run.io_bound(_resolve, lite)
        except Exception:
            logger.exception('[ReadingDesk] links of %s', lite.get('sys_id'))
            found = {'fragment': [], 'pages': {}}
            filled['done'] = False  # the next opening tries again
        if not found.get('complete', True):
            filled['done'] = False  # a source did not answer: the next opening asks again
        try:
            draw(found)
        except RuntimeError:
            pass  # the desk was redrawn while the links were looked for

    menu.on_value_change(fill)
