"""
Correction Model - Core of the User Corrections System
Handles transcription corrections with full review workflow
"""
import enum
from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Enum, Text,
    ForeignKey, Index, Float, JSON
)
from sqlalchemy.orm import relationship
from .database import Base


class CorrectionStatus(str, enum.Enum):
    """Status of a correction through its lifecycle"""
    DRAFT = "draft"           # User is still editing
    PENDING = "pending"       # Submitted, awaiting review
    UNDER_REVIEW = "under_review"  # Reviewer is examining
    APPROVED = "approved"     # Accepted, applied to document
    REJECTED = "rejected"     # Not accepted
    NEEDS_REVISION = "needs_revision"  # Author needs to modify
    SUPERSEDED = "superseded"  # Replaced by newer correction
    MERGED = "merged"         # Combined with another correction


class CorrectionType(str, enum.Enum):
    """Type of correction being made"""
    TEXT_CORRECTION = "text_correction"   # Fix transcription error
    TEXT_ADDITION = "text_addition"       # Add missing text
    TEXT_DELETION = "text_deletion"       # Remove erroneous text
    METADATA_CORRECTION = "metadata"       # Fix document metadata
    TRANSLATION = "translation"            # Add/fix translation
    READING_SUGGESTION = "reading_suggestion"  # Alternative reading
    PALEOGRAPHIC_NOTE = "paleographic"     # Note about writing style
    UNCERTAIN_READING = "uncertain"        # Flag uncertain section


class Correction(Base):
    """
    Correction model representing a user's suggested change to a transcription.

    Attributes:
        id: Unique identifier
        document_id: Reference to the Genizah document (unique_id from index)
        shelfmark: Library shelfmark for quick reference
        author_id: User who submitted the correction
        reviewer_id: User who reviewed (if reviewed)

        # Content
        original_text: The original text being corrected
        corrected_text: The suggested correction
        line_number: Which line in the document (if applicable)
        char_start: Start character position
        char_end: End character position

        # Metadata
        correction_type: Type of correction
        status: Current status in workflow
        confidence_score: User's confidence (0-1)
        source_reference: Academic source for correction
        notes: Additional notes from author

        # Review
        review_notes: Notes from reviewer
        reviewed_at: When reviewed
        rejection_reason: Why rejected (if rejected)

        # Scoring
        upvotes/downvotes: Community voting
        quality_score: Calculated quality metric
    """
    __tablename__ = "corrections"

    id = Column(Integer, primary_key=True, index=True)

    # Document reference (using the unique_id from Tantivy index)
    document_id = Column(String(100), nullable=False, index=True)
    shelfmark = Column(String(255), nullable=True, index=True)
    system_id = Column(String(50), nullable=True)

    # Author and reviewer
    author_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    reviewer_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Location in document
    line_number = Column(Integer, nullable=True)
    char_start = Column(Integer, nullable=True)
    char_end = Column(Integer, nullable=True)
    page_number = Column(Integer, nullable=True)

    # Content of correction
    original_text = Column(Text, nullable=False)
    corrected_text = Column(Text, nullable=False)
    context_before = Column(Text, nullable=True)  # Text before for context
    context_after = Column(Text, nullable=True)   # Text after for context

    # Classification
    correction_type = Column(
        Enum(CorrectionType),
        default=CorrectionType.TEXT_CORRECTION,
        nullable=False
    )
    status = Column(
        Enum(CorrectionStatus),
        default=CorrectionStatus.DRAFT,
        nullable=False,
        index=True
    )

    # Author metadata
    confidence_score = Column(Float, default=0.8)  # 0-1 scale
    source_reference = Column(Text, nullable=True)  # Academic citation
    notes = Column(Text, nullable=True)
    tags = Column(JSON, nullable=True)  # List of tags

    # Review information
    review_notes = Column(Text, nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    rejection_reason = Column(Text, nullable=True)

    # Community voting
    upvotes = Column(Integer, default=0)
    downvotes = Column(Integer, default=0)

    # Calculated scores
    quality_score = Column(Float, default=0.0)
    relevance_score = Column(Float, default=0.0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    submitted_at = Column(DateTime, nullable=True)  # When moved to pending
    applied_at = Column(DateTime, nullable=True)    # When applied to document

    # For tracking changes
    revision_number = Column(Integer, default=1)
    parent_correction_id = Column(Integer, ForeignKey("corrections.id"), nullable=True)

    # Flag for indexing
    is_indexed = Column(Boolean, default=False)

    # Privacy
    is_anonymous = Column(Boolean, default=False)  # Hide author identity in public views

    # Relationships
    author = relationship("User", back_populates="corrections", foreign_keys=[author_id])
    reviewer = relationship("User", back_populates="reviewed_corrections", foreign_keys=[reviewer_id])
    votes = relationship("CorrectionVote", back_populates="correction", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="correction", cascade="all, delete-orphan")
    parent = relationship("Correction", remote_side=[id], backref="revisions")

    # Indexes
    __table_args__ = (
        Index('ix_corrections_doc_status', 'document_id', 'status'),
        Index('ix_corrections_author_status', 'author_id', 'status'),
        Index('ix_corrections_review', 'status', 'created_at'),
        Index('ix_corrections_quality', 'quality_score', 'status'),
        Index('ix_corrections_system_id', 'system_id', 'status'),
    )

    def __repr__(self):
        return f"<Correction(id={self.id}, doc='{self.document_id}', status={self.status})>"

    def submit(self):
        """Submit correction for review"""
        if self.status == CorrectionStatus.DRAFT:
            self.status = CorrectionStatus.PENDING
            self.submitted_at = datetime.utcnow()

    def approve(self, reviewer_id: int, notes: str = None):
        """Approve the correction"""
        self.status = CorrectionStatus.APPROVED
        self.reviewer_id = reviewer_id
        self.reviewed_at = datetime.utcnow()
        self.review_notes = notes
        self.applied_at = datetime.utcnow()

    def reject(self, reviewer_id: int, reason: str):
        """Reject the correction"""
        self.status = CorrectionStatus.REJECTED
        self.reviewer_id = reviewer_id
        self.reviewed_at = datetime.utcnow()
        self.rejection_reason = reason

    def request_revision(self, reviewer_id: int, notes: str):
        """Request author to revise"""
        self.status = CorrectionStatus.NEEDS_REVISION
        self.reviewer_id = reviewer_id
        self.review_notes = notes

    def calculate_quality_score(self):
        """Calculate quality score based on multiple factors"""
        # Base from votes
        vote_score = (self.upvotes - self.downvotes) / max(1, self.upvotes + self.downvotes)

        # Author confidence
        confidence = self.confidence_score or 0.5

        # Has source reference
        source_bonus = 0.1 if self.source_reference else 0

        # Combine
        self.quality_score = (vote_score * 0.4 + confidence * 0.4 + source_bonus * 0.2)
        return self.quality_score

    @property
    def vote_score(self) -> int:
        """Net vote score"""
        return (self.upvotes or 0) - (self.downvotes or 0)


class CorrectionVote(Base):
    """
    Track individual votes on corrections.
    Each user can vote once per correction.
    """
    __tablename__ = "correction_votes"

    id = Column(Integer, primary_key=True, index=True)
    correction_id = Column(Integer, ForeignKey("corrections.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    vote_value = Column(Integer, nullable=False)  # +1 or -1
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    correction = relationship("Correction", back_populates="votes")
    user = relationship("User", back_populates="votes")

    __table_args__ = (
        Index('ix_correction_votes_unique', 'correction_id', 'user_id', unique=True),
    )
