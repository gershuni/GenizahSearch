"""
User Model with Role-Based Access Control
Supports researchers, reviewers, and administrators
"""
import enum
from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Enum, Text, Index
)
from sqlalchemy.orm import relationship
from .database import Base


class UserRole(str, enum.Enum):
    """User roles with escalating permissions"""
    GUEST = "guest"           # Can only view, no contributions
    CONTRIBUTOR = "contributor"  # Can submit corrections and comments
    REVIEWER = "reviewer"     # Can approve/reject corrections
    EDITOR = "editor"         # Can edit any correction
    ADMIN = "admin"           # Full system access


class User(Base):
    """
    User model for the Genizah Corrections System.

    Attributes:
        id: Unique identifier
        email: User's email (unique, used for login)
        username: Display name (unique)
        password_hash: Hashed password (bcrypt)
        full_name: User's full name
        affiliation: Academic/institutional affiliation
        bio: Short biography
        role: User's permission level
        is_active: Whether the account is active
        is_verified: Whether email has been verified
        created_at: Account creation timestamp
        updated_at: Last update timestamp
        last_login: Last login timestamp
        contribution_count: Cached count of contributions
        reputation_score: Gamification score based on activity
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(100), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)

    # Profile information
    full_name = Column(String(255), nullable=True)
    affiliation = Column(String(255), nullable=True)  # University, institute
    bio = Column(Text, nullable=True)
    avatar_url = Column(String(500), nullable=True)

    # Permissions and status
    role = Column(Enum(UserRole), default=UserRole.CONTRIBUTOR, nullable=False)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)

    # Statistics (cached for performance)
    contribution_count = Column(Integer, default=0)
    approved_corrections_count = Column(Integer, default=0)
    reputation_score = Column(Integer, default=0)

    # API key for programmatic access
    api_key = Column(String(64), unique=True, nullable=True, index=True)

    # Settings stored as JSON-like string
    settings = Column(Text, nullable=True)

    # Relationships
    corrections = relationship("Correction", back_populates="author", foreign_keys="Correction.author_id")
    reviewed_corrections = relationship("Correction", back_populates="reviewer", foreign_keys="Correction.reviewer_id")
    comments = relationship("Comment", back_populates="author")
    votes = relationship("CorrectionVote", back_populates="user")
    reactions = relationship("CommentReaction", back_populates="user")
    activities = relationship("ActivityLog", back_populates="user")
    discoveries = relationship("Discovery", back_populates="user")
    discovery_responses = relationship("DiscoveryResponse", back_populates="user")

    # Indexes for common queries
    __table_args__ = (
        Index('ix_users_role_active', 'role', 'is_active'),
        Index('ix_users_reputation', 'reputation_score'),
    )

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', role={self.role})>"

    def can_submit_corrections(self) -> bool:
        """Check if user can submit corrections"""
        return self.is_active and self.role in [
            UserRole.CONTRIBUTOR, UserRole.REVIEWER, UserRole.EDITOR, UserRole.ADMIN
        ]

    def can_review_corrections(self) -> bool:
        """Check if user can review/approve corrections"""
        return self.is_active and self.role in [
            UserRole.REVIEWER, UserRole.EDITOR, UserRole.ADMIN
        ]

    def can_edit_any_correction(self) -> bool:
        """Check if user can edit others' corrections"""
        return self.is_active and self.role in [
            UserRole.EDITOR, UserRole.ADMIN
        ]

    def is_admin(self) -> bool:
        """Check if user is administrator"""
        return self.is_active and self.role == UserRole.ADMIN

    def add_reputation(self, points: int):
        """Add reputation points"""
        self.reputation_score = max(0, (self.reputation_score or 0) + points)

    def increment_contributions(self):
        """Increment contribution count"""
        self.contribution_count = (self.contribution_count or 0) + 1

    def increment_approved(self):
        """Increment approved corrections count"""
        self.approved_corrections_count = (self.approved_corrections_count or 0) + 1
