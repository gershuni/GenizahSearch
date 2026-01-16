"""
Document Metadata Model - Extended metadata for Genizah documents
Tracks document-level information beyond what's in Tantivy index
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text,
    Index, Float, JSON
)
from .database import Base


class DocumentMetadata(Base):
    """
    Extended metadata for Genizah documents.

    This model supplements the Tantivy index with:
    - Correction statistics
    - Quality scores
    - User-contributed metadata
    - Edit history tracking

    The document_id corresponds to unique_id in the Tantivy index.
    """
    __tablename__ = "document_metadata"

    id = Column(Integer, primary_key=True, index=True)

    # Document identification (matches Tantivy unique_id)
    document_id = Column(String(100), unique=True, nullable=False, index=True)
    system_id = Column(String(50), nullable=True, index=True)
    shelfmark = Column(String(255), nullable=True, index=True)

    # Derived from original source
    source_version = Column(String(20), nullable=True)  # V0.7, V0.8

    # User-contributed metadata
    user_title = Column(String(500), nullable=True)
    user_description = Column(Text, nullable=True)
    user_date_attribution = Column(String(100), nullable=True)
    user_provenance = Column(String(255), nullable=True)
    user_genre = Column(String(100), nullable=True)
    user_language = Column(String(50), nullable=True)  # Hebrew, Arabic, Judeo-Arabic, etc.

    # Statistics
    correction_count = Column(Integer, default=0)
    approved_correction_count = Column(Integer, default=0)
    pending_correction_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    view_count = Column(Integer, default=0)

    # Quality metrics
    transcription_quality_score = Column(Float, default=0.0)  # 0-1 based on corrections
    completeness_score = Column(Float, default=0.0)  # How complete is transcription

    # Flags
    has_corrections = Column(Boolean, default=False)
    needs_review = Column(Boolean, default=False)
    is_featured = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)  # Expert verified

    # Tags and categories (JSON array)
    tags = Column(JSON, nullable=True)
    categories = Column(JSON, nullable=True)

    # Related documents (JSON array of document_ids)
    related_documents = Column(JSON, nullable=True)

    # External links (JSON object with source -> URL)
    external_links = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_correction_at = Column(DateTime, nullable=True)
    last_comment_at = Column(DateTime, nullable=True)

    # Indexes
    __table_args__ = (
        Index('ix_doc_meta_shelfmark', 'shelfmark'),
        Index('ix_doc_meta_corrections', 'has_corrections', 'correction_count'),
        Index('ix_doc_meta_quality', 'transcription_quality_score'),
        Index('ix_doc_meta_featured', 'is_featured'),
    )

    def __repr__(self):
        return f"<DocumentMetadata(id={self.id}, doc='{self.document_id}')>"

    def increment_corrections(self):
        """Increment correction count"""
        self.correction_count = (self.correction_count or 0) + 1
        self.has_corrections = True

    def increment_approved(self):
        """Increment approved correction count"""
        self.approved_correction_count = (self.approved_correction_count or 0) + 1
        self.update_quality_score()

    def decrement_pending(self):
        """Decrement pending correction count"""
        self.pending_correction_count = max(0, (self.pending_correction_count or 0) - 1)

    def increment_pending(self):
        """Increment pending correction count"""
        self.pending_correction_count = (self.pending_correction_count or 0) + 1

    def increment_comments(self):
        """Increment comment count"""
        self.comment_count = (self.comment_count or 0) + 1
        self.last_comment_at = datetime.utcnow()

    def increment_views(self):
        """Increment view count"""
        self.view_count = (self.view_count or 0) + 1

    def update_quality_score(self):
        """Update quality score based on corrections"""
        if self.correction_count and self.correction_count > 0:
            # Higher ratio of approved = higher quality
            self.transcription_quality_score = (
                self.approved_correction_count or 0
            ) / self.correction_count
