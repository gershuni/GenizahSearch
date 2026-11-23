# -*- coding: utf-8 -*-
"""
Reading Desk Data Model

Shared data containers for the Virtual Reading Desk feature.
Used by both web (NiceGUI) and desktop (PyQt6) applications.

The reading desk shows multiple manuscript fragments side-by-side
with synchronized scrolling between image and text panes.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class ReadingDeskEntry:
    """A single manuscript entry in the reading desk view."""
    sys_id: str
    shelfmark: str
    title: str = ''
    library_code: str = ''
    pgpid: Optional[int] = None
    pages: List[Dict[str, Any]] = field(default_factory=list)
    # pages: list of dicts with keys: p_num, text, full_header, fl_id
    sources: List[Dict[str, Any]] = field(default_factory=list)
    # sources: PGP transcription sources for this fragment
    pgp_doc: Optional[Dict[str, Any]] = None
    sequence_order: int = 0
    # The text the reader chose for this fragment: '' = the desk's default (an edition, else the
    # first source with text, else the automatic transcription), 'v08' = the automatic
    # transcription, else a source's key (desktop: genizah_app.py::_browse_rd_source_key).
    chosen: str = ''


@dataclass
class ReadingDeskState:
    """State container for the reading desk (multi-fragment view)."""
    entries: List[ReadingDeskEntry] = field(default_factory=list)
    source_description: str = ''
    pgpid: Optional[int] = None
    # The image side's choices, kept while the desk is open (shared/reading_desk_views.py):
    # the view ('' = the default), the image on show in one at a time (its key), and the
    # puzzle images on show (ids one per line; '' = the default, shared/reading_desk_puzzles.py).
    image_view: str = ''
    image_shown: str = ''
    puzzles_shown: str = ''


# -- The desk in the desktop session file (genizah_app.py::_save_session, ``browse_reading_desk``) --

# The image side's choices the session file keeps with the desk (``ReadingDeskState`` fields).
DESK_SESSION_CHOICES = ('image_view', 'image_shown', 'puzzles_shown')


def desk_session_state(state: Optional[ReadingDeskState]) -> Optional[Dict[str, Any]]:
    """What the session file keeps of an open desk: its manuscripts in their order (system
    number, shelfmark, the text chosen for each), the PGP document it was opened for, and the
    image side's choices. The pages and sources are not kept: they are read again on restore.
    None for no desk or an empty one."""
    if state is None:
        return None
    entries = [{'sys_id': e.sys_id, 'shelfmark': e.shelfmark or '', 'chosen': e.chosen or ''}
               for e in state.entries if e.sys_id]
    if not entries:
        return None
    kept = {'entries': entries, 'pgpid': state.pgpid}
    kept.update({name: getattr(state, name, '') or '' for name in DESK_SESSION_CHOICES})
    return kept


def desk_from_session(raw: Any) -> Optional[Dict[str, Any]]:
    """A desk read back from the session file, checked, in the shape ``desk_session_state``
    writes: ``entries`` (dicts with ``sys_id``, ``shelfmark`` and ``chosen``, in their order, each
    system number once), ``pgpid`` (an int or None) and the image side's choices (strings, ''
    when absent). A malformed entry or value is dropped, never raised on: the file may be
    hand-edited or written by another version. None when no entry is left."""
    if not isinstance(raw, dict) or not isinstance(raw.get('entries'), list):
        return None
    entries, seen = [], set()
    for item in raw['entries']:
        if not isinstance(item, dict):
            continue
        sys_id = item.get('sys_id')
        if not isinstance(sys_id, str) or not sys_id.strip() or sys_id.strip() in seen:
            continue
        sys_id = sys_id.strip()
        seen.add(sys_id)
        shelfmark, chosen = item.get('shelfmark'), item.get('chosen')
        entries.append({'sys_id': sys_id,
                        'shelfmark': shelfmark if isinstance(shelfmark, str) else '',
                        'chosen': chosen if isinstance(chosen, str) else ''})
    if not entries:
        return None
    pgpid = raw.get('pgpid')
    if isinstance(pgpid, str) and pgpid.strip().isdigit():
        pgpid = int(pgpid.strip())
    if not isinstance(pgpid, int) or isinstance(pgpid, bool):
        pgpid = None
    back = {'entries': entries, 'pgpid': pgpid}
    back.update({name: raw[name] if isinstance(raw.get(name), str) else '' for name in DESK_SESSION_CHOICES})
    return back
