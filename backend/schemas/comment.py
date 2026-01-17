"""
Comment Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class CommentTypeEnum(str, Enum):
    GENERAL = "general"
    QUESTION = "question"
    SCHOLARLY_NOTE = "scholarly_note"
    SUGGESTION = "suggestion"
    ISSUE = "issue"
    REPLY = "reply"


class CommentBase(BaseModel):
    """Base comment fields"""
    content: str = Field(..., min_length=1, max_length=10000)
    comment_type: CommentTypeEnum = CommentTypeEnum.GENERAL
    line_number: Optional[int] = Field(None, ge=1)
    char_start: Optional[int] = Field(None, ge=0)
    char_end: Optional[int] = Field(None, ge=0)


class CommentCreate(CommentBase):
    """Schema for creating a comment"""
    document_id: Optional[str] = Field(None, max_length=100)
    correction_id: Optional[int] = None
    parent_id: Optional[int] = None  # For replies
    is_public: bool = True  # False = private, only visible to author


class CommentUpdate(BaseModel):
    """Schema for updating a comment"""
    content: str = Field(..., min_length=1, max_length=10000)


class CommentReactionCreate(BaseModel):
    """Schema for creating a reaction"""
    reaction_type: str = Field(..., pattern="^(like|helpful|insightful|thanks|disagree)$")


class AuthorInfo(BaseModel):
    """Brief author information"""
    id: int
    username: str
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None
    reputation_score: int = 0

    class Config:
        from_attributes = True


class CommentResponse(BaseModel):
    """Schema for comment response"""
    id: int
    document_id: Optional[str] = None
    correction_id: Optional[int] = None
    parent_id: Optional[int] = None
    author_id: int
    author: Optional[AuthorInfo] = None

    content: str
    comment_type: CommentTypeEnum

    line_number: Optional[int] = None
    char_start: Optional[int] = None
    char_end: Optional[int] = None

    is_pinned: bool = False
    is_resolved: bool = False
    is_edited: bool = False
    is_deleted: bool = False
    is_public: bool = True

    reply_count: int = 0
    reaction_count: int = 0

    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime] = None

    # Nested replies (optional, for thread view)
    replies: Optional[List['CommentResponse']] = None

    # User's reactions (if applicable)
    user_reactions: Optional[List[str]] = None

    # Reaction counts by type
    reactions_summary: Optional[dict] = None

    class Config:
        from_attributes = True


# Enable self-reference
CommentResponse.model_rebuild()


class CommentListResponse(BaseModel):
    """Paginated list of comments"""
    items: List[CommentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


class CommentThread(BaseModel):
    """Full comment thread with all replies"""
    root: CommentResponse
    replies: List[CommentResponse]
    total_replies: int


class ReactionResponse(BaseModel):
    """Reaction response"""
    id: int
    comment_id: int
    user_id: int
    reaction_type: str
    created_at: datetime

    class Config:
        from_attributes = True


class ReactionSummary(BaseModel):
    """Summary of reactions on a comment"""
    like: int = 0
    helpful: int = 0
    insightful: int = 0
    thanks: int = 0
    disagree: int = 0
    total: int = 0
