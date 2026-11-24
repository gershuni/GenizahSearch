# -*- coding: utf-8 -*-
"""Reading Desk, document mode: a PGP document's text, shown once for the whole document.

One set of rules for both apps: the website's desk (``web/pages/browse_desk_document.py``) and
the desktop's (``desktop/reading_desk_text.py``).

A document spread over several fragments has ONE edition (or several rival editions of the
whole document), not one per fragment. The desks used to ask for sources fragment by fragment
(``get_all_sources_for_fragment``), which returns the sources of EVERY document the fragment
belongs to: the same text was repeated under each fragment, and an edition of another document
could be the one shown (GS-307: a desk of PGP 1702 showed edition 5171, PGP 1246's, under all
three of its fragments).

Here the sources are asked for by document. ``document_desk`` is the data and the rule for when
the desk is "the document": it shows the document's text while every fragment on it belongs to
the document it was opened for. Removing a fragment keeps it (the edition is still the
document's, and the desk says how many of its fragments are on it). Adding a fragment from
elsewhere ends it: the desk is then a hand-built one and goes back to one text per fragment,
until that fragment is removed again.

The text side then has three views: the document's edition, its translation, or one text per
fragment ("By fragment"), where each fragment opens on a transcription of its own.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional

VIEW_EDITION = 'edition'
VIEW_TRANSLATION = 'translation'
VIEW_FRAGMENTS = 'fragments'


def _relation(source: Dict[str, Any]) -> str:
    return source.get('doc_relation') or ''


def _has_text(source: Dict[str, Any]) -> bool:
    return bool((source.get('content') or '').strip())


def document_desk(pgpid: Optional[int], entry_sys_ids: Iterable[str]) -> Dict[str, Any]:
    """What the desk may show for document ``pgpid`` with these fragments on it.

    ``known``: the sidecar has the document's fragments. ``whole``: every fragment on the
    desk is one of them (the desk is the document, or part of it). ``editions`` and
    ``translations`` are the document's own sources, never another document's; a source
    that is both ("Edition ; Translation") counts as an edition.
    """
    from shared.document_service import (
        get_document_metadata, get_editions_for_document, get_fragments_for_document,
        get_translations_for_document,
    )
    on_desk = {str(s) for s in entry_sys_ids if s}
    desk: Dict[str, Any] = {
        'pgpid': pgpid, 'known': False, 'whole': False, 'editions': [], 'translations': [],
        'fragments_total': 0, 'fragments_on_desk': len(on_desk), 'pgp_url': None,
    }
    if not pgpid or not on_desk:
        return desk
    of_document = {str(f['sys_id']) for f in get_fragments_for_document(pgpid) if f.get('sys_id')}
    desk['known'] = bool(of_document)
    desk['fragments_total'] = len(of_document)
    desk['whole'] = bool(of_document) and on_desk <= of_document
    if not desk['whole']:
        return desk
    desk['editions'] = [s for s in get_editions_for_document(pgpid) if _has_text(s)]
    desk['translations'] = [s for s in get_translations_for_document(pgpid)
                            if _has_text(s) and 'Edition' not in _relation(s)]
    desk['pgp_url'] = (get_document_metadata(pgpid) or {}).get('pgp_url')
    return desk


def is_other_desk(desk: Dict[str, Any]) -> bool:
    """The desk was opened for a document but now holds a fragment from outside it."""
    return bool(desk.get('known')) and not desk.get('whole')


def has_document_text(desk: Dict[str, Any]) -> bool:
    return bool(desk.get('whole')) and bool(desk.get('editions') or desk.get('translations'))


def document_view(desk: Dict[str, Any], wanted: Optional[str]) -> Optional[str]:
    """Which text view the desk is in: an edition, a translation, one text per fragment --
    or None when the document's text is not on offer at all (the per-fragment desk).
    ``wanted``: the reader's choice, if any."""
    if not has_document_text(desk):
        return None
    if wanted == VIEW_FRAGMENTS:
        return VIEW_FRAGMENTS
    if wanted == VIEW_TRANSLATION and desk['translations']:
        return VIEW_TRANSLATION
    return VIEW_EDITION if desk['editions'] else VIEW_TRANSLATION


def views_on_offer(desk: Dict[str, Any]) -> List[str]:
    """The views the reader can choose among, in their order: those with a text, then By fragment."""
    return ([VIEW_EDITION] if desk.get('editions') else []) + \
        ([VIEW_TRANSLATION] if desk.get('translations') else []) + [VIEW_FRAGMENTS]


def view_sources(desk: Dict[str, Any], view: str) -> List[Dict[str, Any]]:
    """The document's sources a view chooses among: its translations, else its editions."""
    return desk.get('translations' if view == VIEW_TRANSLATION else 'editions') or []


def chosen_source(desk: Dict[str, Any], view: str, wanted: Any) -> Optional[Dict[str, Any]]:
    """The edition (or translation) on show: the reader's choice (``wanted``, a source id) if
    it is still one of the document's, else the first."""
    sources = view_sources(desk, view)
    if not sources:
        return None
    wanted = str(wanted or '')
    return next((s for s in sources if str(s.get('id')) == wanted), sources[0])


def text_direction(source: Dict[str, Any]) -> str:
    """'rtl' or 'ltr' for a source's text: the sidecar's own word, else English reads left
    to right and everything else right to left."""
    direction = source.get('source_direction')
    if direction in ('rtl', 'ltr'):
        return direction
    return 'ltr' if source.get('language') == 'English' and 'Edition' not in _relation(source) else 'rtl'


def source_label(source: Dict[str, Any], view: str, tr: Callable[[str], str] = lambda s: s) -> str:
    """The attribution as PGP gives it; a translation is led by its language."""
    scholar = (source.get('source_scholar') or '').strip()
    if view != VIEW_TRANSLATION:
        return scholar or tr('PGP edition')
    language = tr(source['language']) if source.get('language') else tr('Translation')
    return f'{language}: {scholar}' if scholar else language


def source_labels(sources: List[Dict[str, Any]], view: str, tr: Callable[[str], str] = lambda s: s) -> List[str]:
    """Each source's label; labels that read alike get a number, so two sources of one scholar
    can be told apart."""
    from shared.fgp_service import number_repeated_labels
    return number_repeated_labels([source_label(s, view, tr) for s in sources])
