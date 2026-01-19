"""
Correction Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum


class CorrectionTypeEnum(str, Enum):
    TEXT_CORRECTION = "text_correction"
    TEXT_ADDITION = "text_addition"
    TEXT_DELETION = "text_deletion"
    METADATA_CORRECTION = "metadata"
    TRANSLATION = "translation"
    READING_SUGGESTION = "reading_suggestion"
    PALEOGRAPHIC_NOTE = "paleographic"
    UNCERTAIN_READING = "uncertain"


class CorrectionStatusEnum(str, Enum):
    DRAFT = "draft"
    PENDING = "pending"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVISION = "needs_revision"
    SUPERSEDED = "superseded"
    MERGED = "merged"


class CorrectionBase(BaseModel):
    """Base correction fields"""
    document_id: str = Field(..., min_length=1, max_length=100)
    original_text: str = Field(..., min_length=1)
    corrected_text: str = Field(..., min_length=1)
    correction_type: CorrectionTypeEnum = CorrectionTypeEnum.TEXT_CORRECTION
    line_number: Optional[int] = Field(None, ge=1)
    char_start: Optional[int] = Field(None, ge=0)
    char_end: Optional[int] = Field(None, ge=0)
    page_number: Optional[int] = Field(None, ge=1)
    context_before: Optional[str] = None
    context_after: Optional[str] = None
    confidence_score: float = Field(default=0.8, ge=0.0, le=1.0)
    source_reference: Optional[str] = Field(None, max_length=1000)
    notes: Optional[str] = Field(None, max_length=5000)
    tags: Optional[List[str]] = None


class CorrectionCreate(CorrectionBase):
    """Schema for creating a correction"""
    shelfmark: Optional[str] = None
    system_id: Optional[str] = None

    @field_validator('corrected_text')
    @classmethod
    def text_differs(cls, v, info):
        if 'original_text' in info.data and v == info.data['original_text']:
            raise ValueError('Corrected text must differ from original text')
        return v


class CorrectionUpdate(BaseModel):
    """Schema for updating a correction (draft or needs_revision)"""
    original_text: Optional[str] = None
    corrected_text: Optional[str] = None
    correction_type: Optional[CorrectionTypeEnum] = None
    line_number: Optional[int] = Field(None, ge=1)
    char_start: Optional[int] = Field(None, ge=0)
    char_end: Optional[int] = Field(None, ge=0)
    context_before: Optional[str] = None
    context_after: Optional[str] = None
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    source_reference: Optional[str] = Field(None, max_length=1000)
    notes: Optional[str] = Field(None, max_length=5000)
    tags: Optional[List[str]] = None


class CorrectionSubmit(BaseModel):
    """Schema for submitting a draft correction for review"""
    notes: Optional[str] = Field(None, max_length=5000)


class CorrectionReview(BaseModel):
    """Schema for reviewing a correction"""
    action: str = Field(..., pattern="^(approve|reject|request_revision)$")
    review_notes: Optional[str] = Field(None, max_length=5000)
    rejection_reason: Optional[str] = Field(None, max_length=1000)


class CorrectionVoteCreate(BaseModel):
    """Schema for voting on a correction"""
    vote_value: int = Field(..., ge=-1, le=1)

    @field_validator('vote_value')
    @classmethod
    def vote_valid(cls, v):
        if v not in [-1, 1]:
            raise ValueError('Vote must be +1 (upvote) or -1 (downvote)')
        return v


class AuthorInfo(BaseModel):
    """Brief author information"""
    id: int
    username: str
    full_name: Optional[str] = None
    reputation_score: int = 0

    class Config:
        from_attributes = True


class CorrectionResponse(BaseModel):
    """Schema for correction response"""
    id: int
    document_id: str
    shelfmark: Optional[str] = None
    system_id: Optional[str] = None
    author_id: int
    author: Optional[AuthorInfo] = None
    reviewer_id: Optional[int] = None
    reviewer: Optional[AuthorInfo] = None

    line_number: Optional[int] = None
    char_start: Optional[int] = None
    char_end: Optional[int] = None
    page_number: Optional[int] = None

    original_text: str
    corrected_text: str
    context_before: Optional[str] = None
    context_after: Optional[str] = None

    correction_type: CorrectionTypeEnum
    status: CorrectionStatusEnum
    confidence_score: float
    source_reference: Optional[str] = None
    notes: Optional[str] = None
    tags: Optional[List[str]] = None

    review_notes: Optional[str] = None
    rejection_reason: Optional[str] = None
    reviewed_at: Optional[datetime] = None

    upvotes: int = 0
    downvotes: int = 0
    quality_score: float = 0.0

    revision_number: int = 1
    parent_correction_id: Optional[int] = None

    created_at: datetime
    updated_at: datetime
    submitted_at: Optional[datetime] = None
    applied_at: Optional[datetime] = None

    # Computed fields
    vote_score: Optional[int] = None
    user_vote: Optional[int] = None  # Current user's vote if any
    comment_count: Optional[int] = None

    class Config:
        from_attributes = True


class CorrectionListResponse(BaseModel):
    """Paginated list of corrections"""
    items: List[CorrectionResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


class CorrectionSearchParams(BaseModel):
    """Search/filter parameters for corrections"""
    document_id: Optional[str] = None
    shelfmark: Optional[str] = None
    system_id: Optional[str] = None
    author_id: Optional[int] = None
    reviewer_id: Optional[int] = None
    status: Optional[CorrectionStatusEnum] = None
    statuses: Optional[List[CorrectionStatusEnum]] = None
    correction_type: Optional[CorrectionTypeEnum] = None
    min_quality_score: Optional[float] = None
    has_source: Optional[bool] = None
    search_text: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


class CorrectionStats(BaseModel):
    """Statistics about corrections"""
    total: int = 0
    draft: int = 0
    pending: int = 0
    under_review: int = 0
    approved: int = 0
    rejected: int = 0
    needs_revision: int = 0
    by_type: dict = {}
    recent_activity: int = 0  # Last 7 days


class CorrectionDiff(BaseModel):
    """Diff view of a correction"""
    original_lines: List[str]
    corrected_lines: List[str]
    diff_html: str  # HTML formatted diff
