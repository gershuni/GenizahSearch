"""
Fragment Join Model - Pairwise joins between Genizah fragments
Supports connected components for showing all related fragments
"""
import enum
import re
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text,
    ForeignKey, Index
)
from sqlalchemy.orm import relationship
from .database import Base


class RelationshipType(str, enum.Enum):
    """Type of relationship between fragments"""
    PHYSICAL_JOIN = "physical_join"       # Same original document, torn apart
    SAME_COMPOSITION = "same_composition" # Different MSS of same text


def normalize_shelfmark(shelfmark: str) -> str:
    """
    Normalize shelfmark for reliable matching across sources.

    Handles variations like:
    - "T-S 8J6.1" vs "TS 8J6.1" vs "T-S 8J 6.1"
    - "AIU VII.A.23" vs "AIU VII. A. 23"
    - Different dash types (-, –, —)
    """
    if not shelfmark:
        return ""

    s = shelfmark.strip()

    # Uppercase for consistency
    s = s.upper()

    # Standardize dash types
    s = s.replace('–', '-').replace('—', '-')

    # T-S (Taylor-Schechter) normalization
    s = re.sub(r'^TS[\s\-]*', 'T-S ', s)
    s = re.sub(r'^T[\s]*-[\s]*S[\s]*', 'T-S ', s)

    # Remove extra spaces
    s = re.sub(r'\s+', ' ', s)

    # Normalize number spacing in common patterns
    # "8J 6.1" -> "8J6.1"
    s = re.sub(r'(\d+[A-Z]+)\s+(\d)', r'\1\2', s)

    # Normalize Roman numerals spacing
    # "VII. A. 23" -> "VII.A.23"
    s = re.sub(r'([IVX]+)\.\s*([A-Z])\.\s*(\d+)', r'\1.\2.\3', s)

    return s.strip()


def normalize_join_order(frag_a: str, frag_b: str) -> tuple:
    """
    Ensure consistent ordering for deduplication.
    Always store fragments alphabetically to prevent A-B and B-A duplicates.
    """
    a_norm = normalize_shelfmark(frag_a)
    b_norm = normalize_shelfmark(frag_b)
    if a_norm <= b_norm:
        return (a_norm, b_norm)
    return (b_norm, a_norm)


class FragmentJoin(Base):
    """
    Pairwise join between two Genizah fragments.

    Joins form a graph. When viewing any fragment, the UI shows all
    fragments in its connected component.

    Example:
        A-B, B-C joins mean viewing A, B, or C shows all three.
    """
    __tablename__ = "fragment_joins"

    id = Column(Integer, primary_key=True, index=True)

    # The two joined fragments (stored alphabetically for deduplication)
    fragment_a = Column(String(200), nullable=False)
    fragment_b = Column(String(200), nullable=False)

    # Optional: sys_ids for faster lookups
    document_id_a = Column(String(100), nullable=True)
    document_id_b = Column(String(100), nullable=True)

    # Relationship type (optional - user may not know)
    relationship_type = Column(String(50), nullable=True)

    # Metadata
    notes = Column(Text, nullable=True)
    source = Column(String(50), default='user')  # 'user', 'princeton', 'cambridge', etc.
    source_url = Column(Text, nullable=True)     # for imports: original URL

    # Tracking
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Soft delete
    is_active = Column(Boolean, default=True)

    # Relationships
    creator = relationship("User", backref="created_joins", foreign_keys=[created_by])

    # Indexes and constraints
    __table_args__ = (
        Index('ix_fragment_joins_a', 'fragment_a'),
        Index('ix_fragment_joins_b', 'fragment_b'),
        Index('ix_fragment_joins_source', 'source'),
        Index('ix_fragment_joins_active', 'is_active'),
        Index('ix_fragment_joins_unique', 'fragment_a', 'fragment_b', unique=True),
    )

    def __repr__(self):
        return f"<FragmentJoin(id={self.id}, {self.fragment_a} <-> {self.fragment_b})>"

    @classmethod
    def create_normalized(
        cls,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
        source: str = 'user',
        source_url: Optional[str] = None,
        created_by: Optional[int] = None,
        document_id_a: Optional[str] = None,
        document_id_b: Optional[str] = None
    ) -> 'FragmentJoin':
        """
        Create a join with normalized and ordered shelfmarks.
        """
        # Normalize and order
        norm_a, norm_b = normalize_join_order(fragment_a, fragment_b)

        # Also reorder document_ids if provided
        orig_a_norm = normalize_shelfmark(fragment_a)
        if document_id_a and document_id_b:
            if orig_a_norm != norm_a:
                # Order was swapped, swap document_ids too
                document_id_a, document_id_b = document_id_b, document_id_a

        return cls(
            fragment_a=norm_a,
            fragment_b=norm_b,
            relationship_type=relationship_type,
            notes=notes,
            source=source,
            source_url=source_url,
            created_by=created_by,
            document_id_a=document_id_a,
            document_id_b=document_id_b
        )
