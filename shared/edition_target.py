# -*- coding: utf-8 -*-
"""Opening a search hit found in a scholarly transcription (editions plan section 3.4), for the
website's /browse (web/pages/browse_editions.py) and the desktop's viewers
(desktop/edition_view.py) alike.

A hit's ``edition`` record (shared/editions_search.py::edition_record) names the edition by
its version (``fgp:<id>`` / ``pgp:<id>``) and the match by its span in the edition's *search
text* -- the text the editions index holds (shared/edition_search_text.py). A viewer

* keeps that edition among the page's sources even where the page filter would leave it out
  (:func:`keep_target`), and chooses it;
* marks the match on the edition's own characters: the span is mapped back by re-running the
  versioned cleaner on the sidecar's text (:func:`resolve_mark`), and the same occurrence of
  that stretch is marked in the text shown (:func:`occurrence_at`);
* treats the hit as stale when the edition is gone from the sidecar or its text is no longer
  the one the search read: the manuscript opens as usual and the reader is told.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

_VERSION_RE = re.compile(r'^(fgp|pgp):(\d+)$')


def source_version(source: Dict[str, Any]) -> Optional[str]:
    """A chooser source's version (``fgp:<id>`` / ``pgp:<id>``), the edition records' key.
    *source* is a source as shared/fgp_service.py and shared/document_service.py give them,
    or the desktop chooser's item data (``source`` 'fgp_edition' / 'pgp_edition', with
    ``source_id``)."""
    from shared.fgp_service import SOURCE_FGP, source_provider
    kind = source.get('source')
    if kind in ('fgp_edition', 'fgp_translation'):
        sid = source.get('source_id')
        return source.get('uid') or (f'fgp:{sid}' if sid is not None else None)
    if kind in ('pgp_edition', 'pgp_translation'):
        sid = source.get('source_id')
        return f'pgp:{sid}' if sid is not None else None
    if source.get('id') is None:
        return None
    if source_provider(source) == SOURCE_FGP:
        return source.get('uid') or f'fgp:{source.get("id")}'
    return f'pgp:{source.get("id")}'


def keep_target(all_sources: List[Dict[str, Any]], filtered: Optional[List[Dict[str, Any]]],
                version: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    """The page's sources after its filter, with the linked edition kept when the filter left
    it out (it is the text the search matched on this page)."""
    if not version:
        return filtered
    kept = list(filtered or [])
    if any(source_version(s) == version for s in kept):
        return kept
    wanted = [s for s in all_sources or [] if source_version(s) == version]
    return (kept + wanted[:1]) or filtered


def resolve_mark(engine, resolver, sys_id: str, version: str, span: Optional[Sequence[int]],
                 fp: Optional[str] = None) -> Dict[str, Any]:
    """``{'stale': bool, 'mark': str or None, 'occurrence': int}``: the edition's own characters
    the search matched, and which occurrence of them in the edition it is.

    *engine*: the SearchEngine holding the editions index (``edition_entry``,
    ``get_full_text_by_id``); *resolver*: an EditionResolver (the sidecar's edition now). The
    hit is stale when the edition of that manuscript is not in the index or the sidecar, when
    its text is not the indexed one, or when *fp* (what the span held when the search read it,
    editions_search.span_fingerprint) no longer matches. Blocking: a sidecar read and an index
    lookup."""
    stale = {'stale': True, 'mark': None, 'occurrence': 0}
    find = getattr(engine, 'edition_entry', None)
    entry = find(sys_id, version) if find is not None else None
    resolved = resolver.resolve(version) if entry is not None else None
    if entry is None or resolved is None:
        return stale
    try:
        indexed = engine.get_full_text_by_id(entry['uid'])
    except Exception:
        indexed = None
    search = resolved.get('search')
    if indexed is None or search is None or search.text != indexed:
        # The edition's text is not the one the search read (refreshed since the build).
        return stale
    if not span or not (0 <= span[0] < span[1] <= len(search.text)):
        # A span past the end of the text the edition has now cannot be the search's.
        return {'stale': bool(span and fp), 'mark': None, 'occurrence': 0}
    from shared.editions_search import span_fingerprint
    if fp and span_fingerprint(search.text, span) != fp:
        return stale
    start, end = search.span_to_original(*span)
    original = resolved.get('original') or ''
    mark = original[start:end]
    return {'stale': False, 'mark': mark or None, 'occurrence': original[:start].count(mark) if mark else 0}


def occurrence_at(text: str, mark: Optional[str], occurrence: int = 0) -> Optional[Tuple[int, int]]:
    """Where the *occurrence*-th stretch equal to *mark* lies in *text* (the first one when the
    text shown has fewer: a viewer may show the edition's sections apart), or None when the
    text does not hold it."""
    if not text or not mark:
        return None
    positions = []
    at = text.find(mark)
    while at != -1:
        positions.append(at)
        at = text.find(mark, at + len(mark))
    if not positions:
        return None
    at = positions[occurrence] if occurrence < len(positions) else positions[0]
    return at, at + len(mark)


def target_of(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The edition a search result opens on: ``{'version', 'span', 'fp', 'sys_id', 'located'}``
    of a result whose best match is in a scholarly transcription (its ``edition`` record), or
    None."""
    edition = (row or {}).get('edition')
    if not isinstance(edition, dict) or not _VERSION_RE.match(str(edition.get('version') or '')):
        return None
    span = edition.get('span')
    span = (int(span[0]), int(span[1])) if isinstance(span, (list, tuple)) and len(span) == 2 else None
    return {'version': str(edition['version']), 'span': span if span and span[0] < span[1] else None,
            'fp': edition.get('span_fp'), 'sys_id': str(edition.get('sys_id') or ''),
            'located': bool(edition.get('located'))}
