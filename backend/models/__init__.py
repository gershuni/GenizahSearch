# Backend Models Package
from .database import Base, engine, SessionLocal, get_db, init_db
from .user import User, UserRole
from .correction import Correction, CorrectionStatus, CorrectionVote
from .comment import Comment, CommentReaction
from .document_metadata import DocumentMetadata
from .activity_log import ActivityLog

__all__ = [
    'Base', 'engine', 'SessionLocal', 'get_db', 'init_db',
    'User', 'UserRole',
    'Correction', 'CorrectionStatus', 'CorrectionVote',
    'Comment', 'CommentReaction',
    'DocumentMetadata',
    'ActivityLog'
]
