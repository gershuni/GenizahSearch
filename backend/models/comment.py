"""
Comment Model - Threaded Discussions on Documents and Corrections
Supports nested replies and reactions
"""
import enum
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text,
    ForeignKey, Index, Enum
)
from sqlalchemy.orm import relationship
from .database import Base


class CommentType(str, enum.Enum):
    """Type of comment"""
    GENERAL = "general"           # General discussion
    QUESTION = "question"         # Question about the text
    SCHOLARLY_NOTE = "scholarly_note"  # Academic annotation
    SUGGESTION = "suggestion"     # Suggested interpretation
    ISSUE = "issue"               # Report an issue
    REPLY = "reply"               # Reply to another comment


class Comment(Base):
    """
    Comment model for discussions on documents and corrections.

    Supports:
    - Comments on documents (by document_id)
    - Comments on corrections (by correction_id)
    - Threaded replies (parent_id)
    - Reactions (separate model)

    Attributes:
        id: Unique identifier
        document_id: Document being discussed (optional)
        correction_id: Correction being discussed (optional)
        author_id: Who wrote the comment
        parent_id: Parent comment for threading

        content: The comment text (supports markdown)
        comment_type: Classification of the comment
        is_pinned: Whether the comment is pinned to top
        is_resolved: For questions/issues - has it been resolved
    """
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True)

    # Target - either document or correction (or both)
    document_id = Column(String(100), nullable=True, index=True)
    correction_id = Column(Integer, ForeignKey("corrections.id"), nullable=True)

    # Author
    author_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Threading
    parent_id = Column(Integer, ForeignKey("comments.id"), nullable=True)

    # Content
    content = Column(Text, nullable=False)
    comment_type = Column(
        Enum(CommentType),
        default=CommentType.GENERAL,
        nullable=False
    )

    # Location in document (optional, for inline comments)
    line_number = Column(Integer, nullable=True)
    char_start = Column(Integer, nullable=True)
    char_end = Column(Integer, nullable=True)

    # Status flags
    is_pinned = Column(Boolean, default=False)
    is_resolved = Column(Boolean, default=False)
    is_edited = Column(Boolean, default=False)
    is_deleted = Column(Boolean, default=False)  # Soft delete
    is_anonymous = Column(Boolean, default=False)  # Hide author identity in public views

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)

    # Cached counts
    reply_count = Column(Integer, default=0)
    reaction_count = Column(Integer, default=0)

    # Relationships
    author = relationship("User", back_populates="comments")
    correction = relationship("Correction", back_populates="comments")
    reactions = relationship("CommentReaction", back_populates="comment", cascade="all, delete-orphan")
    replies = relationship("Comment", backref="parent", remote_side=[id])

    # Indexes
    __table_args__ = (
        Index('ix_comments_document', 'document_id', 'is_deleted'),
        Index('ix_comments_correction', 'correction_id', 'is_deleted'),
        Index('ix_comments_thread', 'parent_id'),
    )

    def __repr__(self):
        return f"<Comment(id={self.id}, author={self.author_id}, type={self.comment_type})>"

    def resolve(self):
        """Mark as resolved"""
        self.is_resolved = True
        self.resolved_at = datetime.utcnow()

    def soft_delete(self):
        """Soft delete the comment"""
        self.is_deleted = True
        self.content = "[deleted]"

    def mark_edited(self):
        """Mark as edited"""
        self.is_edited = True


class CommentReaction(Base):
    """
    Reactions to comments (like, helpful, etc.)
    """
    __tablename__ = "comment_reactions"

    id = Column(Integer, primary_key=True, index=True)
    comment_id = Column(Integer, ForeignKey("comments.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    reaction_type = Column(String(50), nullable=False)  # 'like', 'helpful', 'insightful', etc.
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    comment = relationship("Comment", back_populates="reactions")
    user = relationship("User", back_populates="reactions")

    __table_args__ = (
        Index('ix_comment_reactions_unique', 'comment_id', 'user_id', 'reaction_type', unique=True),
    )


# Predefined reaction types
REACTION_TYPES = [
    'like',       # General approval
    'helpful',    # Particularly useful
    'insightful', # Scholarly value
    'thanks',     # Gratitude
    'disagree',   # Respectful disagreement
]
