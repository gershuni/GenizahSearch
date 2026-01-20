"""
Fragment Join Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum


class RelationshipTypeEnum(str, Enum):
    PHYSICAL_JOIN = "physical_join"
    SAME_COMPOSITION = "same_composition"


class JoinCreate(BaseModel):
    """Schema for creating a join between two fragments"""
    fragment_a: str = Field(..., min_length=1, max_length=200, description="First fragment shelfmark")
    fragment_b: str = Field(..., min_length=1, max_length=200, description="Second fragment shelfmark")
    relationship_type: Optional[RelationshipTypeEnum] = Field(None, description="Type of relationship (optional)")
    notes: Optional[str] = Field(None, max_length=2000, description="Additional notes")
    document_id_a: Optional[str] = Field(None, max_length=100, description="System ID for fragment A")
    document_id_b: Optional[str] = Field(None, max_length=100, description="System ID for fragment B")

    @field_validator('fragment_b')
    @classmethod
    def fragments_different(cls, v, info):
        if 'fragment_a' in info.data:
            # Basic check - full normalization done in service
            if v.strip().upper() == info.data['fragment_a'].strip().upper():
                raise ValueError('Cannot join a fragment to itself')
        return v


class JoinUpdate(BaseModel):
    """Schema for updating a join"""
    relationship_type: Optional[RelationshipTypeEnum] = None
    notes: Optional[str] = Field(None, max_length=2000)


class CreatorInfo(BaseModel):
    """Brief creator information"""
    id: int
    username: str

    class Config:
        from_attributes = True


class JoinResponse(BaseModel):
    """Schema for join response"""
    id: int
    fragment_a: str
    fragment_b: str
    document_id_a: Optional[str] = None
    document_id_b: Optional[str] = None
    relationship_type: Optional[str] = None
    notes: Optional[str] = None
    source: str = "user"
    source_url: Optional[str] = None
    created_by: Optional[CreatorInfo] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class JoinedFragment(BaseModel):
    """A fragment in the connected component"""
    shelfmark: str
    document_id: Optional[str] = None
    is_current: bool = False
    relationship_type: Optional[str] = None
    join_id: Optional[int] = None  # ID of the direct join (if direct)
    join_source: Optional[str] = None


class ConnectedFragmentsResponse(BaseModel):
    """Response for getting all connected fragments"""
    shelfmark: str
    shelfmark_normalized: str
    fragments: List[str]
    fragment_details: List[JoinedFragment]
    joins: List[JoinResponse]
    total_fragments: int
    total_joins: int


class JoinSearchResponse(BaseModel):
    """Response for join search"""
    results: List[JoinResponse]
    total: int


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = True
    message: Optional[str] = None
