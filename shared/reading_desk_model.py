# -*- coding: utf-8 -*-
"""
Reading Desk Data Model

Plain dataclasses for reading desk state. No Supabase calls, no UI code --
just data containers shared between web and desktop apps.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class ReadingDeskEntry:
    """A single manuscript on the reading desk."""
    sys_id: str
    shelfmark: str
    title: str = ''
    library_code: str = ''
    pgpid: Optional[int] = None
    pages: List[Dict[str, Any]] = field(default_factory=list)
    # Each page: {p_num, text, image_url, is_oxford, full_header}
    sources: List[Dict[str, Any]] = field(default_factory=list)
    pgp_doc: Optional[Dict[str, Any]] = None
    sequence_order: int = 0


@dataclass
class ReadingDeskState:
    """Full reading desk state."""
    entries: List[ReadingDeskEntry] = field(default_factory=list)
    source_description: str = ''  # e.g. "PGP Document #1234" or "Custom selection"
    pgpid: Optional[int] = None   # If populated from a joined document
