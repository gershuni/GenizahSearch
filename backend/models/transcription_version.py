"""
TranscriptionVersion Model - Full Page Transcription Versioning

Stores complete transcriptions of document pages with version tracking.
Supports multiple sources: V0.7, V0.8, and user corrections.
"""
import enum
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Enum, Text,
    ForeignKey, Index, Float, UniqueConstraint
)
from sqlalchemy.orm import relationship
from .database import Base


class VersionSource(str, enum.Enum):
    """Source of the transcription version"""
    V07 = "V0.7"           # Original V0.7 transcription
    V08 = "V0.8"           # Updated V0.8 transcription
    USER = "user"          # User-submitted correction
    MERGED = "merged"      # Merged from multiple sources
    IMPORTED = "imported"  # Imported from external source


class TranscriptionVersion(Base):
    """
    Full transcription version for a document page.

    Each page can have multiple versions from different sources.
    One version is marked as the current default for display.

    Attributes:
        id: Unique identifier
        sys_id: System ID of the document (e.g., 99123456789)
        page_num: Page number within the document

        # Content
        content: Full transcription text for this page
        source: Where this version came from (V0.7, V0.8, user)

        # User info (for user-submitted versions)
        user_id: User who created this version (NULL for V0.7/V0.8)
        correction_id: Link to original correction (if from correction)

        # Version metadata
        change_description: Brief description of changes from previous
        is_current_default: Whether this is the default version shown

        # For Tantivy indexing
        is_indexed: Whether this version has been indexed
        index_timestamp: When it was last indexed
    """
    __tablename__ = "transcription_versions"

    id = Column(Integer, primary_key=True, index=True)

    # Document identification
    sys_id = Column(String(50), nullable=False, index=True)
    page_num = Column(Integer, nullable=False, default=1)

    # Transcription content
    content = Column(Text, nullable=False)

    # Source tracking
    source = Column(
        Enum(VersionSource),
        default=VersionSource.USER,
        nullable=False
    )

    # User info (for user corrections)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    correction_id = Column(Integer, ForeignKey("corrections.id"), nullable=True)

    # Version metadata
    change_description = Column(Text, nullable=True)
    version_number = Column(Integer, default=1)  # Sequential version for this page

    # Default status
    is_current_default = Column(Boolean, default=False, index=True)

    # Indexing status (for Tantivy)
    is_indexed = Column(Boolean, default=False)
    index_timestamp = Column(DateTime, nullable=True)

    # Quality/confidence
    quality_score = Column(Float, default=1.0)  # 0-1 scale

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("User", backref="transcription_versions")
    correction = relationship("Correction", backref="transcription_version")

    # Indexes and constraints
    __table_args__ = (
        Index('ix_versions_sys_page', 'sys_id', 'page_num'),
        Index('ix_versions_default', 'sys_id', 'page_num', 'is_current_default'),
        Index('ix_versions_source', 'source', 'sys_id'),
        Index('ix_versions_indexed', 'is_indexed', 'source'),
        Index('ix_versions_user', 'user_id', 'created_at'),
    )

    def __repr__(self):
        return f"<TranscriptionVersion(id={self.id}, sys_id='{self.sys_id}', page={self.page_num}, source={self.source})>"

    def set_as_default(self):
        """Mark this version as the current default"""
        self.is_current_default = True

    def mark_indexed(self):
        """Mark as indexed in Tantivy"""
        self.is_indexed = True
        self.index_timestamp = datetime.utcnow()


class PageDefault(Base):
    """
    Quick lookup table for current default version per page.
    Denormalized for fast queries.
    """
    __tablename__ = "page_defaults"

    sys_id = Column(String(50), primary_key=True)
    page_num = Column(Integer, primary_key=True, default=1)
    default_version_id = Column(Integer, ForeignKey("transcription_versions.id"), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship
    default_version = relationship("TranscriptionVersion")

    __table_args__ = (
        Index('ix_page_defaults_version', 'default_version_id'),
    )
