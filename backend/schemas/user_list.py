"""
Schemas for User Lists API
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


# === List Schemas ===

class ListItemCreate(BaseModel):
    """Schema for adding an item to a list."""
    sys_id: str
    shelfmark: Optional[str] = None
    title: Optional[str] = None
    fl_id: Optional[str] = None
    note: Optional[str] = None
    tags: Optional[List[str]] = []


class ListItemUpdate(BaseModel):
    """Schema for updating an item in a list."""
    note: Optional[str] = None
    tags: Optional[List[str]] = None


class ListItemResponse(BaseModel):
    """Response schema for a list item."""
    item_id: str
    sys_id: str
    shelfmark: Optional[str]
    title: Optional[str]
    fl_id: Optional[str]
    note: Optional[str]
    tags: List[str]
    added_at: Optional[float]
    lists: List[str]

    class Config:
        from_attributes = True


class UserListCreate(BaseModel):
    """Schema for creating a new list."""
    name: str
    name_en: Optional[str] = None
    color: Optional[str] = '#FFD700'
    project_id: Optional[int] = None


class UserListUpdate(BaseModel):
    """Schema for updating a list."""
    name: Optional[str] = None
    name_en: Optional[str] = None
    color: Optional[str] = None
    project_id: Optional[int] = None


class UserListResponse(BaseModel):
    """Response schema for a list."""
    id: str
    name: str
    name_en: Optional[str]
    color: str
    is_default: bool
    is_system: bool
    project_id: Optional[str]
    created: Optional[float]
    count: int

    class Config:
        from_attributes = True


class UserListDetailResponse(UserListResponse):
    """Response schema for a list with items."""
    items: List[ListItemResponse] = []


# === Project Schemas ===

class ProjectCreate(BaseModel):
    """Schema for creating a new project."""
    name: str
    color: Optional[str] = '#4CAF50'


class ProjectUpdate(BaseModel):
    """Schema for updating a project."""
    name: Optional[str] = None
    color: Optional[str] = None


class ProjectResponse(BaseModel):
    """Response schema for a project."""
    id: int
    name: str
    color: str
    created: Optional[float]

    class Config:
        from_attributes = True


# === Migration Schemas ===

class LocalListData(BaseModel):
    """Schema for a local list being migrated."""
    name: str
    name_en: Optional[str] = None
    color: str = '#FFD700'
    is_default: bool = False
    project_id: Optional[str] = None
    items: List[ListItemCreate] = []


class MigrateListsRequest(BaseModel):
    """Schema for migrating local lists to user account."""
    lists: List[LocalListData]
    projects: List[ProjectCreate] = []
    recent_items: List[ListItemCreate] = []


class MigrateListsResponse(BaseModel):
    """Response schema for migration."""
    lists_migrated: int
    items_migrated: int
    projects_migrated: int
    recent_items_migrated: int


# === Recent Items ===

class RecentItemResponse(BaseModel):
    """Response schema for a recently viewed item."""
    item_id: str
    sys_id: str
    shelfmark: Optional[str]
    title: Optional[str]
    fl_id: Optional[str]
    viewed_at: Optional[float]

    class Config:
        from_attributes = True


class AddRecentItemRequest(BaseModel):
    """Schema for adding a recent item."""
    sys_id: str
    shelfmark: Optional[str] = None
    title: Optional[str] = None
    fl_id: Optional[str] = None


# === Bulk Operations ===

class BulkAddItemsRequest(BaseModel):
    """Schema for adding multiple items to a list."""
    items: List[ListItemCreate]


class BulkRemoveItemsRequest(BaseModel):
    """Schema for removing multiple items from a list."""
    item_ids: List[str]


# === All Lists Response ===

class AllListsResponse(BaseModel):
    """Response schema for getting all user lists data."""
    lists: List[UserListResponse]
    projects: List[ProjectResponse]
    lists_order: List[str]
    projects_order: List[int]
    all_tags: List[str]
