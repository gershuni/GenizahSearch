# Backend Models Package
from .database import Base, engine, SessionLocal, get_db, init_db
from .user import User, UserRole
from .correction import Correction, CorrectionStatus, CorrectionVote
from .comment import Comment, CommentReaction
from .document_metadata import DocumentMetadata
from .activity_log import ActivityLog
from .transcription_version import TranscriptionVersion, PageDefault, VersionSource
from .discovery import Discovery, DiscoveryResponse, DiscoveryType, DiscoveryStatus
from .fragment_link import FragmentLink, RelationshipType, normalize_shelfmark, normalize_link_order

__all__ = [
    'Base', 'engine', 'SessionLocal', 'get_db', 'init_db',
    'User', 'UserRole',
    'Correction', 'CorrectionStatus', 'CorrectionVote',
    'Comment', 'CommentReaction',
    'DocumentMetadata',
    'ActivityLog',
    'TranscriptionVersion', 'PageDefault', 'VersionSource',
    'Discovery', 'DiscoveryResponse', 'DiscoveryType', 'DiscoveryStatus',
    'FragmentLink', 'RelationshipType', 'normalize_shelfmark', 'normalize_link_order',
]
