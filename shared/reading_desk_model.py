# -*- coding: utf-8 -*-
"""
Reading Desk data model.

Pure data containers for the Virtual Reading Desk (multi-fragment view).
Used by both web and desktop applications.
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


@dataclass
class ReadingDeskState:
    """State container for the reading desk (multi-fragment view)."""
    entries: List[ReadingDeskEntry] = field(default_factory=list)
    source_description: str = ''
    # source_description: e.g. "PGP Document #12345" or "My List: Favourite Joins"
    pgpid: Optional[int] = None
    # pgpid: If opened from a PGP joined document, the document pgpid
