"""
Discovery Model - Discoveries, Questions, and Notable Findings

Allows researchers to share:
- Discoveries (new findings, identifications)
- Questions to the community
- Notable corrections/identifications

Supports:
- Multiple shelfmarks per discovery
- Related manuscripts linking
"""
import enum
import json
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text,
    ForeignKey, Index, Enum, JSON
)
from sqlalchemy.orm import relationship
from .database import Base


class DiscoveryType(str, enum.Enum):
    """Type of discovery/post"""
    DISCOVERY = "discovery"           # New finding, identification
    QUESTION = "question"             # Question for the community
    IDENTIFICATION = "identification" # Identified a text/author
    NOTE = "note"                     # General scholarly note
    CORRECTION_HIGHLIGHT = "correction_highlight"  # Notable correction


class DiscoveryStatus(str, enum.Enum):
    """Status of the discovery"""
    PUBLISHED = "published"     # Visible to all
    DRAFT = "draft"             # Not yet published
    HIDDEN = "hidden"           # Hidden by admin
    FEATURED = "featured"       # Featured/highlighted


class Discovery(Base):
    """
    Discovery model for sharing findings and questions with the community.

    Attributes:
        id: Unique identifier
        user_id: Author (can be displayed as anonymous)
        discovery_type: Type of post (discovery, question, etc.)

        title: Short title/headline
        content: Full description (supports markdown)

        document_id: Related document (optional)
        page_number: Specific page (optional)
        shelfmark: Library shelfmark for reference

        is_anonymous: Hide author identity
        is_featured: Highlighted by admin
        status: Visibility status
    """
    __tablename__ = "discoveries"

    id = Column(Integer, primary_key=True, index=True)

    # Author
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Classification
    discovery_type = Column(
        Enum(DiscoveryType),
        default=DiscoveryType.NOTE,
        nullable=False
    )
    status = Column(
        Enum(DiscoveryStatus),
        default=DiscoveryStatus.PUBLISHED,
        nullable=False,
        index=True
    )

    # Content
    title = Column(String(300), nullable=False)
    content = Column(Text, nullable=False)

    # Related document (optional)
    document_id = Column(String(100), nullable=True, index=True)
    page_number = Column(Integer, nullable=True)
    shelfmark = Column(String(255), nullable=True)

    # Multiple shelfmarks support - JSON array of {shelfmark, document_id, page_number}
    # Example: [{"shelfmark": "T-S 13J1.1", "document_id": "xyz", "page_number": 2}]
    additional_shelfmarks = Column(JSON, nullable=True, default=list)

    # Related manuscripts - JSON array of {document_id, shelfmark, relationship_type, notes}
    # relationship_type: "parallel", "continuation", "fragment", "related", "citation"
    # Example: [{"document_id": "abc", "shelfmark": "T-S 8.1", "relationship_type": "parallel", "notes": "Same text"}]
    related_manuscripts = Column(JSON, nullable=True, default=list)

    # Related correction (if highlighting a correction)
    correction_id = Column(Integer, ForeignKey("corrections.id"), nullable=True)

    # Privacy and display
    is_anonymous = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False, index=True)
    is_pinned = Column(Boolean, default=False, index=True)  # Admin pinned
    is_answered = Column(Boolean, default=False)  # For questions

    # Engagement counts
    view_count = Column(Integer, default=0)
    response_count = Column(Integer, default=0)
    upvotes = Column(Integer, default=0)
    downvotes = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="discoveries")
    correction = relationship("Correction")
    responses = relationship("DiscoveryResponse", back_populates="discovery", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('ix_discoveries_type_status', 'discovery_type', 'status'),
        Index('ix_discoveries_featured', 'is_featured', 'created_at'),
        Index('ix_discoveries_document', 'document_id'),
    )

    def __repr__(self):
        return f"<Discovery(id={self.id}, type={self.discovery_type}, title='{self.title[:30]}...')>"

    @property
    def display_author(self) -> str:
        """Get display name respecting anonymity"""
        if self.is_anonymous:
            return "חוקר אנונימי"  # Anonymous researcher
        return self.user.full_name or self.user.username if self.user else "Unknown"

    @property
    def vote_score(self) -> int:
        """Net vote score (upvotes - downvotes)"""
        return (self.upvotes or 0) - (self.downvotes or 0)

    @property
    def all_shelfmarks(self) -> list:
        """Get all shelfmarks (primary + additional)"""
        shelfmarks = []
        if self.shelfmark:
            shelfmarks.append({
                'shelfmark': self.shelfmark,
                'document_id': self.document_id,
                'page_number': self.page_number,
                'is_primary': True
            })
        if self.additional_shelfmarks:
            for sm in self.additional_shelfmarks:
                sm['is_primary'] = False
                shelfmarks.append(sm)
        return shelfmarks

    @property
    def related_docs(self) -> list:
        """Get related manuscripts list"""
        return self.related_manuscripts or []

    def add_shelfmark(self, shelfmark: str, document_id: str = None, page_number: int = None):
        """Add an additional shelfmark"""
        if not self.additional_shelfmarks:
            self.additional_shelfmarks = []
        self.additional_shelfmarks.append({
            'shelfmark': shelfmark,
            'document_id': document_id,
            'page_number': page_number
        })

    def add_related_manuscript(
        self,
        document_id: str,
        shelfmark: str = None,
        relationship_type: str = "related",
        notes: str = None
    ):
        """Add a related manuscript link"""
        if not self.related_manuscripts:
            self.related_manuscripts = []
        self.related_manuscripts.append({
            'document_id': document_id,
            'shelfmark': shelfmark,
            'relationship_type': relationship_type,
            'notes': notes
        })


class DiscoveryVote(Base):
    """
    Track individual user votes on discoveries.
    Each user can vote once per discovery (up or down).
    """
    __tablename__ = "discovery_votes"

    id = Column(Integer, primary_key=True, index=True)
    discovery_id = Column(Integer, ForeignKey("discoveries.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    vote_type = Column(String(10), nullable=False)  # 'up' or 'down'
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    discovery = relationship("Discovery")
    user = relationship("User")

    __table_args__ = (
        Index('ix_discovery_votes_unique', 'discovery_id', 'user_id', unique=True),
    )


class DiscoveryResponse(Base):
    """
    Responses/comments on discoveries.
    """
    __tablename__ = "discovery_responses"

    id = Column(Integer, primary_key=True, index=True)
    discovery_id = Column(Integer, ForeignKey("discoveries.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Content
    content = Column(Text, nullable=False)

    # Privacy
    is_anonymous = Column(Boolean, default=False)

    # Status
    is_deleted = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    discovery = relationship("Discovery", back_populates="responses")
    user = relationship("User", back_populates="discovery_responses")

    __table_args__ = (
        Index('ix_discovery_responses_discovery', 'discovery_id', 'is_deleted'),
    )

    def __repr__(self):
        return f"<DiscoveryResponse(id={self.id}, discovery={self.discovery_id})>"

    @property
    def display_author(self) -> str:
        """Get display name respecting anonymity"""
        if self.is_anonymous:
            return "חוקר אנונימי"
        return self.user.full_name or self.user.username if self.user else "Unknown"
