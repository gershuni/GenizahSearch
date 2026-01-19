"""
User Pydantic Schemas for API validation
"""
from typing import Optional, List
from pydantic import BaseModel, EmailStr, Field, field_validator
from datetime import datetime
import re


class UserBase(BaseModel):
    """Base user fields"""
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=50)
    full_name: Optional[str] = Field(None, max_length=255)
    affiliation: Optional[str] = Field(None, max_length=255)
    bio: Optional[str] = Field(None, max_length=2000)

    @field_validator('username')
    @classmethod
    def username_alphanumeric(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username must contain only letters, numbers, underscores, and hyphens')
        return v


class UserCreate(UserBase):
    """Schema for user registration"""
    password: str = Field(..., min_length=8, max_length=100)
    confirm_password: str = Field(..., min_length=8, max_length=100)

    @field_validator('password')
    @classmethod
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[0-9]', v):
            raise ValueError('Password must contain at least one number')
        return v

    @field_validator('confirm_password')
    @classmethod
    def passwords_match(cls, v, info):
        if 'password' in info.data and v != info.data['password']:
            raise ValueError('Passwords do not match')
        return v


class UserUpdate(BaseModel):
    """Schema for updating user profile"""
    full_name: Optional[str] = Field(None, max_length=255)
    affiliation: Optional[str] = Field(None, max_length=255)
    bio: Optional[str] = Field(None, max_length=2000)
    avatar_url: Optional[str] = Field(None, max_length=500)


class UserLogin(BaseModel):
    """Schema for login request"""
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    """Schema for user response (public info)"""
    id: int
    email: EmailStr
    username: str
    full_name: Optional[str] = None
    affiliation: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    role: str
    is_active: bool
    is_verified: bool
    created_at: datetime
    contribution_count: int = 0
    approved_corrections_count: int = 0
    reputation_score: int = 0

    class Config:
        from_attributes = True


class UserProfile(BaseModel):
    """Public profile view"""
    id: int
    username: str
    full_name: Optional[str] = None
    affiliation: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    role: str
    created_at: datetime
    contribution_count: int = 0
    approved_corrections_count: int = 0
    reputation_score: int = 0

    class Config:
        from_attributes = True


class UserInDB(UserResponse):
    """Internal user with password hash (for auth)"""
    password_hash: str


class Token(BaseModel):
    """JWT token response"""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int  # seconds


class TokenData(BaseModel):
    """Data extracted from JWT token"""
    user_id: int
    email: str
    username: str
    role: str
    exp: datetime


class PasswordChange(BaseModel):
    """Schema for password change"""
    current_password: str
    new_password: str = Field(..., min_length=8, max_length=100)
    confirm_password: str = Field(..., min_length=8, max_length=100)

    @field_validator('new_password')
    @classmethod
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[0-9]', v):
            raise ValueError('Password must contain at least one number')
        return v

    @field_validator('confirm_password')
    @classmethod
    def passwords_match(cls, v, info):
        if 'new_password' in info.data and v != info.data['new_password']:
            raise ValueError('Passwords do not match')
        return v


class UserStats(BaseModel):
    """User statistics"""
    total_corrections: int = 0
    pending_corrections: int = 0
    approved_corrections: int = 0
    rejected_corrections: int = 0
    total_comments: int = 0
    total_votes_given: int = 0
    total_votes_received: int = 0
    reputation_score: int = 0


class UserList(BaseModel):
    """List of users (for admin)"""
    users: List[UserResponse]
    total: int
    page: int
    page_size: int
