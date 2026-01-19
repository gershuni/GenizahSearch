"""
TranscriptionVersion Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class VersionSourceEnum(str, Enum):
    V07 = "V0.7"
    V08 = "V0.8"
    USER = "user"
    MERGED = "merged"
    IMPORTED = "imported"


class UserBrief(BaseModel):
    """Brief user info for responses"""
    id: int
    username: str
    full_name: Optional[str] = None

    class Config:
        from_attributes = True


class VersionBase(BaseModel):
    """Base version fields"""
    sys_id: str = Field(..., min_length=1, max_length=50)
    page_num: int = Field(default=1, ge=1)
    content: str = Field(..., min_length=1)
    change_description: Optional[str] = Field(None, max_length=1000)


class VersionCreate(VersionBase):
    """Schema for creating a new version (from user correction)"""
    source: VersionSourceEnum = VersionSourceEnum.USER
    correction_id: Optional[int] = None


class VersionResponse(BaseModel):
    """Schema for version response"""
    id: int
    sys_id: str
    page_num: int
    content: str
    source: VersionSourceEnum
    user: Optional[UserBrief] = None
    change_description: Optional[str] = None
    version_number: int
    is_current_default: bool
    quality_score: float
    created_at: datetime
    is_indexed: bool

    class Config:
        from_attributes = True


class VersionListItem(BaseModel):
    """Brief version info for listing"""
    id: int
    source: VersionSourceEnum
    user_name: Optional[str] = None
    version_number: int
    is_current_default: bool
    created_at: datetime
    change_description: Optional[str] = None

    class Config:
        from_attributes = True


class PageVersionsResponse(BaseModel):
    """Response for getting all versions of a page"""
    sys_id: str
    page_num: int
    current_default: Optional[VersionResponse] = None
    all_versions: List[VersionListItem]
    total: int


class SetDefaultRequest(BaseModel):
    """Request to set a version as default"""
    version_id: int


class ImportVersionRequest(BaseModel):
    """Request to import V0.7/V0.8 versions"""
    sys_id: str
    page_num: int = 1
    content: str
    source: VersionSourceEnum


class VersionSearchParams(BaseModel):
    """Parameters for searching versions"""
    sys_id: Optional[str] = None
    source: Optional[VersionSourceEnum] = None
    user_id: Optional[int] = None
    is_indexed: Optional[bool] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


class VersionStats(BaseModel):
    """Statistics about versions"""
    total_versions: int
    by_source: dict
    user_versions_count: int
    indexed_count: int
    unindexed_count: int
