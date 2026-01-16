"""
Activity Log Model - Track all user actions for audit and analytics
"""
import enum
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, ForeignKey, Index, JSON, Enum
)
from sqlalchemy.orm import relationship
from .database import Base


class ActivityType(str, enum.Enum):
    """Types of activities to track"""
    # User activities
    USER_REGISTER = "user_register"
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    USER_UPDATE_PROFILE = "user_update_profile"
    USER_CHANGE_PASSWORD = "user_change_password"

    # Correction activities
    CORRECTION_CREATE = "correction_create"
    CORRECTION_UPDATE = "correction_update"
    CORRECTION_SUBMIT = "correction_submit"
    CORRECTION_APPROVE = "correction_approve"
    CORRECTION_REJECT = "correction_reject"
    CORRECTION_REQUEST_REVISION = "correction_request_revision"
    CORRECTION_DELETE = "correction_delete"
    CORRECTION_VOTE = "correction_vote"

    # Comment activities
    COMMENT_CREATE = "comment_create"
    COMMENT_UPDATE = "comment_update"
    COMMENT_DELETE = "comment_delete"
    COMMENT_RESOLVE = "comment_resolve"
    COMMENT_REACT = "comment_react"

    # Document activities
    DOCUMENT_VIEW = "document_view"
    DOCUMENT_UPDATE_META = "document_update_meta"

    # Admin activities
    ADMIN_USER_ROLE_CHANGE = "admin_user_role_change"
    ADMIN_USER_ACTIVATE = "admin_user_activate"
    ADMIN_USER_DEACTIVATE = "admin_user_deactivate"
    ADMIN_CORRECTION_OVERRIDE = "admin_correction_override"


class ActivityLog(Base):
    """
    Activity log for audit trail and analytics.

    Tracks all significant user actions with:
    - Who did it (user_id)
    - What they did (activity_type)
    - On what (target_type, target_id)
    - Details (metadata JSON)
    - When (created_at)
    - From where (ip_address, user_agent)
    """
    __tablename__ = "activity_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Who
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # Null for anonymous

    # What
    activity_type = Column(Enum(ActivityType), nullable=False, index=True)

    # On what
    target_type = Column(String(50), nullable=True)  # 'correction', 'comment', 'user', 'document'
    target_id = Column(String(100), nullable=True)   # ID of the target object

    # Details
    description = Column(Text, nullable=True)  # Human-readable description
    extra_data = Column(JSON, nullable=True)   # Additional structured data

    # Client info
    ip_address = Column(String(45), nullable=True)  # IPv4 or IPv6
    user_agent = Column(String(500), nullable=True)
    session_id = Column(String(100), nullable=True)

    # When
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    user = relationship("User", back_populates="activities")

    # Indexes for common queries
    __table_args__ = (
        Index('ix_activity_user_type', 'user_id', 'activity_type'),
        Index('ix_activity_target', 'target_type', 'target_id'),
        Index('ix_activity_date_type', 'created_at', 'activity_type'),
    )

    def __repr__(self):
        return f"<ActivityLog(id={self.id}, type={self.activity_type}, user={self.user_id})>"

    @classmethod
    def log(cls, db, activity_type: ActivityType, user_id: int = None,
            target_type: str = None, target_id: str = None,
            description: str = None, extra_data: dict = None,
            ip_address: str = None, user_agent: str = None,
            session_id: str = None):
        """
        Convenience method to create activity log entry.

        Usage:
            ActivityLog.log(
                db,
                ActivityType.CORRECTION_SUBMIT,
                user_id=current_user.id,
                target_type='correction',
                target_id=str(correction.id),
                description=f"User submitted correction for {correction.document_id}"
            )
        """
        log_entry = cls(
            activity_type=activity_type,
            user_id=user_id,
            target_type=target_type,
            target_id=target_id,
            description=description,
            extra_data=extra_data,
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id
        )
        db.add(log_entry)
        db.commit()
        return log_entry
