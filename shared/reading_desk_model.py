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
