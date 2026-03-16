# -*- coding: utf-8 -*-
"""
Puzzle Data Model for Fragment Puzzle feature.

Shared dataclass containers for multi-fragment puzzle state.
Used by both web and desktop apps, persisted via PuzzleService to joins.db.
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import List
from datetime import datetime


@dataclass
class PuzzleFragment:
    """A single fragment positioned on the puzzle canvas."""
    sys_id: str
    folio_label: str          # e.g., "1r", "2v"
    fl_id: str                # NLI FL ID for image loading
    x: float = 0.0
    y: float = 0.0
    rotation: float = 0.0    # degrees
    scale: float = 1.0
    flip_h: bool = False
    flip_v: bool = False
    bg_removal_threshold: float = 30.0  # HSV color distance threshold


@dataclass
class PuzzleDocument:
    """A puzzle document containing positioned fragments."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ''
    notes: str = ''
    join_type: str = 'uncertain'  # physical, content, uncertain
    fragments: List[PuzzleFragment] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> 'PuzzleDocument':
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        fragments = [PuzzleFragment(**f) for f in data.pop('fragments', [])]
        return cls(fragments=fragments, **data)
