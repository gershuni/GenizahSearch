"""
Fragment Link Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum


class RelationshipTypeEnum(str, Enum):
    PHYSICAL_JOIN = "physical_join"
    SAME_COMPOSITION = "same_composition"


class LinkCreate(BaseModel):
    """Schema for creating a link between two fragments"""
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
                raise ValueError('Cannot link a fragment to itself')
        return v


class LinkUpdate(BaseModel):
    """Schema for updating a link"""
    relationship_type: Optional[RelationshipTypeEnum] = None
    notes: Optional[str] = Field(None, max_length=2000)


class CreatorInfo(BaseModel):
    """Brief creator information"""
    id: int
    username: str

    class Config:
        from_attributes = True


class LinkResponse(BaseModel):
    """Schema for link response"""
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


class LinkedFragment(BaseModel):
    """A fragment in the connected component"""
    shelfmark: str
    document_id: Optional[str] = None
    is_current: bool = False
    relationship_type: Optional[str] = None
    link_id: Optional[int] = None  # ID of the direct link (if direct)
    link_source: Optional[str] = None


class ConnectedFragmentsResponse(BaseModel):
    """Response for getting all connected fragments"""
    shelfmark: str
    shelfmark_normalized: str
    fragments: List[str]
    fragment_details: List[LinkedFragment]
    links: List[LinkResponse]
    total_fragments: int
    total_links: int


class LinkSearchResponse(BaseModel):
    """Response for link search"""
    results: List[LinkResponse]
    total: int


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = True
    message: Optional[str] = None
