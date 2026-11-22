# -*- coding: utf-8 -*-
"""Reading Desk, puzzle images: which saved puzzles of the desk's fragments there are, and
which are on show. For both apps; no UI here.

The Fragment Puzzle is where fragments are arranged; the desk only shows the pictures of the
arrangements that already exist for its fragments. One join of fragments may have several
puzzles -- its recto and its verso, or one for each page of a document on several leaves -- so
several are shown, one under another, the reader choosing which. Two kinds are looked for:

* the reader's own saved puzzles: on the website each visitor's own (``web/saved_joins.py``),
  on the desktop the install's ``joins.db`` (``shared/puzzle_service.py``, no owner);
* published puzzles (Supabase), which carry their picture already.

A puzzle counts when it holds at least two of the desk's fragments (one, on a desk of one).

The web desk (``web/pages/browse_desk_puzzle.py``) has the same rules in its own copy until its
open puzzle-images change has landed; then it imports these.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, fields
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

#: The ids of the puzzles shown are kept one per line (a string). This value is the reader
#: having taken every puzzle off the list (no puzzle id is "-").
NONE_SHOWN = '-'
#: Puzzles shown before the reader chooses: each of the reader's own is composed when shown,
#: so a desk whose fragments are in many puzzles does not compose all of them at once.
SHOWN_FIRST = 4


def own_joins(joins: Any, sys_ids: List[str]) -> List[Dict[str, Any]]:
    """The reader's own saved puzzles holding any of ``sys_ids`` (unranked; ``rank_joins``).
    ``joins``: any object with ``list_documents_for_fragment(sys_id=)`` and
    ``load_document(doc_id)``. BLOCKING (a local SQLite read)."""
    found: Dict[str, Dict[str, Any]] = {}
    for sys_id in sys_ids:
        for doc_id in joins.list_documents_for_fragment(sys_id=sys_id):
            if doc_id in found:
                continue
            doc = joins.load_document(doc_id)
            if doc is None or not doc.fragments:
                continue
            held = {f.sys_id for f in doc.fragments}
            found[doc_id] = {
                'kind': 'own', 'id': doc.id, 'title': doc.title or '', 'when': doc.updated_at or '',
                'covers': len(held & set(sys_ids)), 'fragments': [asdict(f) for f in doc.fragments],
            }
    return list(found.values())


def published_joins(client: Any, sys_ids: List[str]) -> List[Dict[str, Any]]:
    """The published puzzles holding any of ``sys_ids`` that have a picture (unranked).
    BLOCKING (network)."""
    from shared.puzzle_publish_service import get_published_join_detail, get_published_joins_for_fragment
    seen: Dict[str, int] = {}
    rows: Dict[str, Dict[str, Any]] = {}
    for sys_id in sys_ids:
        for row in get_published_joins_for_fragment(client, sys_id):
            seen[row['id']] = seen.get(row['id'], 0) + 1
            rows[row['id']] = row
    found = []
    for join_id, row in rows.items():
        detail = get_published_join_detail(client, join_id) or {}
        if not detail.get('is_published') or not detail.get('image_url'):
            continue
        found.append({
            'kind': 'published', 'id': join_id, 'title': row.get('title') or '', 'when': row.get('created_at') or '',
            'covers': seen[join_id], 'author': row.get('author_name') or '', 'image_url': detail['image_url'],
            'revision': published_revision(detail),
        })
    return found


def published_revision(detail: Dict[str, Any]) -> str:
    """A mark of a published puzzle's arrangement. Publishing again keeps the puzzle's id, its
    creation date and its picture's address, so only the arrangement tells a new picture from
    the old one (a picture kept under the old mark is not shown for the new)."""
    arrangement = json.dumps(detail.get('fragments_json') or {}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(f"{arrangement}|{detail.get('updated_at') or ''}".encode('utf-8')).hexdigest()[:12]


def find_joins(sys_ids: List[str], own: Any = None, client: Any = None) -> List[Dict[str, Any]]:
    """The puzzles that can be shown for a desk holding ``sys_ids``, best first: the one
    holding most of the desk's fragments, the reader's own before a published one, newest
    first. BLOCKING. ``own``: the reader's saved puzzles, any object with
    ``list_documents_for_fragment(sys_id=)`` and ``load_document(doc_id)`` (None: not looked
    in); ``client``: a Supabase client (None: published puzzles not looked for). Either half
    may fail without taking the other with it.
    """
    sys_ids = [s for s in dict.fromkeys(sys_ids) if s]
    found: List[Dict[str, Any]] = []
    if own is not None:
        try:
            found += own_joins(own, sys_ids)
        except Exception:
            logger.exception('[ReadingDesk] saved puzzles lookup')
    published: List[Dict[str, Any]] = []
    if client is not None:
        try:
            published = published_joins(client, sys_ids)
        except Exception as e:
            logger.info('[ReadingDesk] published puzzles lookup failed: %s', e)
    return rank_joins(sys_ids, found, published)


def rank_joins(sys_ids: List[str], own: List[Dict[str, Any]], published: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """``own`` and ``published`` together (a published puzzle that is one of the reader's own
    counted once, as theirs), those holding too few of the desk's fragments dropped, best
    first (see ``find_joins``)."""
    sys_ids = [s for s in dict.fromkeys(sys_ids) if s]
    own_ids = {j['id'] for j in own}
    found = list(own) + [j for j in published if j['id'] not in own_ids]
    needed = min(2, len(sys_ids))
    found = [j for j in found if j['covers'] >= needed]
    # Stable sorts, least important key first.
    found.sort(key=lambda j: j['when'], reverse=True)
    found.sort(key=lambda j: j['kind'] != 'own')
    found.sort(key=lambda j: j['covers'], reverse=True)
    return found


def join_label(join: Dict[str, Any], tr: Callable[[str], str] = lambda s: s) -> str:
    """The puzzle's name and whose it is."""
    whose = tr('Your saved join') if join['kind'] == 'own' else tr('Published by {}').format(join.get('author') or '?')
    return f"{join['title']} · {whose}" if join['title'] else whose


def shown_joins(joins: List[Dict[str, Any]], chosen: Optional[str]) -> List[Dict[str, Any]]:
    """The puzzles on show, in the order found: the reader's choice of them (ids one per
    line), else the first SHOWN_FIRST. A choice naming none still found (each was deleted) is
    no choice; a choice cleared by the reader (NONE_SHOWN) shows none. One puzzle alone is
    always shown: there is no chooser to bring it back with."""
    if len(joins) <= 1:
        return list(joins)
    raw = str(chosen or '')
    if raw == NONE_SHOWN:
        return []
    wanted = set(raw.split('\n')) - {''}
    shown = [j for j in joins if j['id'] in wanted]
    return shown if shown else joins[:SHOWN_FIRST]


def choice_of(joins: List[Dict[str, Any]], ticked: List[str]) -> str:
    """The stored choice for the puzzles ``ticked``: in the order found, whatever order they
    were ticked in. None ticked is kept as none: the reader cleared the list on purpose."""
    ticked_set = set(ticked)
    return '\n'.join(j['id'] for j in joins if j['id'] in ticked_set) or NONE_SHOWN


def join_fragments(join: Dict[str, Any]) -> list:
    """The reader's own puzzle's fragments as ``PuzzleFragment``s, for composing its picture
    (keys a newer or older save may add or lack are tolerated)."""
    from shared.puzzle_model import PuzzleFragment
    names = {f.name for f in fields(PuzzleFragment)}
    out = []
    for raw in join.get('fragments') or []:
        try:
            out.append(PuzzleFragment(**{k: v for k, v in raw.items() if k in names}))
        except TypeError:
            # A row without its sys_id, folio or image id cannot be placed; said, not dropped
            # in silence, so a picture missing a fragment can be traced.
            logger.warning('[ReadingDesk] puzzle %s: a fragment row cannot be read: %r', join.get('id'), raw)
    return out
