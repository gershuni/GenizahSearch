"""
Common Pydantic Schemas used across the API
"""
from typing import Optional, List, Generic, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime

T = TypeVar('T')


class PaginationParams(BaseModel):
    """Pagination parameters for list endpoints"""
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=20, ge=1, le=100, description="Items per page")
    sort_by: Optional[str] = Field(default=None, description="Field to sort by")
    sort_desc: bool = Field(default=False, description="Sort descending")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper"""
    items: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool

    class Config:
        from_attributes = True


class StatusResponse(BaseModel):
    """Simple status response"""
    success: bool
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ErrorResponse(BaseModel):
    """Error response format"""
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SuccessResponse(BaseModel):
    """Success response format"""
    success: bool = True
    message: str
    data: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthCheck(BaseModel):
    """Health check response"""
    status: str = "healthy"
    version: str
    database: str = "connected"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
