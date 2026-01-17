"""
Discovery Schemas - Pydantic models for discovery API
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field

from ..models.discovery import DiscoveryType, DiscoveryStatus


# ============================================
# Author info (respects anonymity)
# ============================================

class AuthorInfo(BaseModel):
    """Author information, respecting anonymity settings"""
    id: Optional[int] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    is_anonymous: bool = False

    @classmethod
    def from_user(cls, user, is_anonymous: bool = False):
        if is_anonymous:
            return cls(is_anonymous=True)
        return cls(
            id=user.id,
            username=user.username,
            full_name=user.full_name,
            is_anonymous=False
        )


# ============================================
# Discovery schemas
# ============================================

class DiscoveryBase(BaseModel):
    """Base discovery fields"""
    title: str = Field(..., min_length=5, max_length=300)
    content: str = Field(..., min_length=10)
    discovery_type: DiscoveryType = DiscoveryType.NOTE
    document_id: Optional[str] = None
    page_number: Optional[int] = None
    shelfmark: Optional[str] = None
    is_anonymous: bool = False


class DiscoveryCreate(DiscoveryBase):
    """Create a new discovery"""
    correction_id: Optional[int] = None


class DiscoveryUpdate(BaseModel):
    """Update an existing discovery"""
    title: Optional[str] = Field(None, min_length=5, max_length=300)
    content: Optional[str] = Field(None, min_length=10)
    discovery_type: Optional[DiscoveryType] = None
    document_id: Optional[str] = None
    page_number: Optional[int] = None
    shelfmark: Optional[str] = None
    is_anonymous: Optional[bool] = None


class DiscoveryResponse(DiscoveryBase):
    """Discovery response with all fields"""
    id: int
    author: AuthorInfo
    status: DiscoveryStatus
    is_featured: bool = False
    is_pinned: bool = False
    is_answered: bool = False
    view_count: int = 0
    response_count: int = 0
    upvotes: int = 0
    downvotes: int = 0
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class DiscoveryListResponse(BaseModel):
    """List of discoveries with pagination"""
    items: List[DiscoveryResponse]
    total: int
    offset: int
    limit: int


# ============================================
# Response (comment on discovery) schemas
# ============================================

class ResponseCreate(BaseModel):
    """Create a response to a discovery"""
    content: str = Field(..., min_length=1)
    is_anonymous: bool = False


class ResponseUpdate(BaseModel):
    """Update a response"""
    content: Optional[str] = Field(None, min_length=1)


class ResponseItem(BaseModel):
    """A single response"""
    id: int
    author: AuthorInfo
    content: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ResponseListResponse(BaseModel):
    """List of responses"""
    items: List[ResponseItem]
    total: int


# ============================================
# Statistics schemas
# ============================================

class DiscoveryStats(BaseModel):
    """Overall statistics for discoveries page"""
    total_discoveries: int = 0
    total_questions: int = 0
    open_questions: int = 0
    total_corrections: int = 0
    approved_corrections: int = 0
    words_corrected: int = 0
    documents_edited: int = 0
    total_comments: int = 0
    active_contributors: int = 0


# ============================================
# Feed item schemas
# ============================================

class FeedItem(BaseModel):
    """A single item in the activity feed"""
    id: str  # Prefixed ID like "discovery_123" or "correction_456"
    item_type: str  # "discovery", "question", "correction", "comment"
    title: str
    content_preview: str
    author: AuthorInfo
    document_id: Optional[str] = None
    shelfmark: Optional[str] = None
    page_number: Optional[int] = None
    created_at: datetime
    response_count: int = 0
    is_featured: bool = False
    is_pinned: bool = False
    is_answered: bool = False
    upvotes: int = 0
    downvotes: int = 0

    # Type-specific fields
    discovery_type: Optional[DiscoveryType] = None
    correction_status: Optional[str] = None
    original_text: Optional[str] = None  # For corrections
    corrected_text: Optional[str] = None  # For corrections


class FeedResponse(BaseModel):
    """Activity feed response"""
    items: List[FeedItem]
    total: int
    offset: int
    limit: int
    stats: Optional[DiscoveryStats] = None
