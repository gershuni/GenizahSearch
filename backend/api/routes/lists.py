"""
Lists Routes - Personal manuscript lists management.

Provides endpoints for managing user lists, list items, projects,
and migration from local storage.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...models.user_list import UserList, ListItem
from ...schemas.user_list import (
    UserListCreate, UserListUpdate, UserListResponse, UserListDetailResponse,
    ListItemCreate, ListItemUpdate, ListItemResponse,
    ProjectCreate, ProjectUpdate, ProjectResponse,
    MigrateListsRequest, MigrateListsResponse,
    RecentItemResponse, AddRecentItemRequest,
    BulkAddItemsRequest, BulkRemoveItemsRequest,
    AllListsResponse
)
from ...schemas.common import SuccessResponse
from ...services.lists_service import ListsService
from ..deps import get_current_active_user, get_current_user_optional

router = APIRouter(prefix="/lists", tags=["Lists"])


# === List Endpoints ===

@router.get("/", response_model=AllListsResponse)
async def get_all_lists(
    include_recent: bool = Query(True, description="Include system lists like Recently Viewed"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all lists for the current user.
    Automatically creates default lists if they don't exist.
    """
    # Ensure default lists exist
    ListsService.ensure_default_lists(db, current_user.id)

    lists = ListsService.get_all_lists(db, current_user.id, include_recent)
    projects = ListsService.get_projects(db, current_user.id)
    all_tags = ListsService.get_all_tags(db, current_user.id)

    return AllListsResponse(
        lists=[UserListResponse(**l.to_dict()) for l in lists],
        projects=[ProjectResponse(**p.to_dict()) for p in projects],
        lists_order=[str(l.id) for l in lists],
        projects_order=[p.id for p in projects],
        all_tags=all_tags
    )


@router.post("/", response_model=UserListResponse, status_code=status.HTTP_201_CREATED)
async def create_list(
    data: UserListCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Create a new list."""
    new_list = ListsService.create_list(db, current_user.id, data)
    return UserListResponse(**new_list.to_dict())


@router.get("/{list_id}", response_model=UserListDetailResponse)
async def get_list(
    list_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get a specific list with its items."""
    user_list = ListsService.get_list(db, current_user.id, list_id)
    if not user_list:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="List not found"
        )
    return UserListDetailResponse(**user_list.to_dict(include_items=True))


@router.put("/{list_id}", response_model=UserListResponse)
async def update_list(
    list_id: int,
    data: UserListUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update a list's properties."""
    user_list = ListsService.update_list(db, current_user.id, list_id, data)
    if not user_list:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="List not found or cannot be modified"
        )
    return UserListResponse(**user_list.to_dict())


@router.delete("/{list_id}", response_model=SuccessResponse)
async def delete_list(
    list_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Delete a list and all its items."""
    success = ListsService.delete_list(db, current_user.id, list_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="List not found or cannot be deleted"
        )
    return SuccessResponse(success=True, message="List deleted successfully")


# === List Item Endpoints ===

@router.get("/{list_id}/items", response_model=List[ListItemResponse])
async def get_list_items(
    list_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get all items in a list."""
    items = ListsService.get_items_in_list(db, current_user.id, list_id)
    return [ListItemResponse(**item.to_dict()) for item in items]


@router.post("/{list_id}/items", response_model=ListItemResponse, status_code=status.HTTP_201_CREATED)
async def add_item_to_list(
    list_id: int,
    data: ListItemCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Add an item to a list."""
    item = ListsService.add_item_to_list(db, current_user.id, list_id, data)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="List not found"
        )
    return ListItemResponse(**item.to_dict())


@router.post("/{list_id}/items/bulk", response_model=SuccessResponse)
async def bulk_add_items(
    list_id: int,
    data: BulkAddItemsRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Add multiple items to a list."""
    user_list = ListsService.get_list(db, current_user.id, list_id)
    if not user_list:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="List not found"
        )

    added = 0
    for item_data in data.items:
        if ListsService.add_item_to_list(db, current_user.id, list_id, item_data):
            added += 1

    return SuccessResponse(success=True, message=f"Added {added} items to list")


@router.put("/{list_id}/items/{item_id}", response_model=ListItemResponse)
async def update_item(
    list_id: int,
    item_id: int,
    data: ListItemUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update an item's note or tags."""
    item = ListsService.update_item(db, current_user.id, list_id, item_id, data)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found"
        )
    return ListItemResponse(**item.to_dict())


@router.delete("/{list_id}/items/{item_id}", response_model=SuccessResponse)
async def remove_item_from_list(
    list_id: int,
    item_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Remove an item from a list."""
    success = ListsService.remove_item_from_list(db, current_user.id, list_id, item_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found"
        )
    return SuccessResponse(success=True, message="Item removed from list")


@router.delete("/{list_id}/items/bulk", response_model=SuccessResponse)
async def bulk_remove_items(
    list_id: int,
    data: BulkRemoveItemsRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Remove multiple items from a list."""
    removed = 0
    for item_id in data.item_ids:
        try:
            if ListsService.remove_item_from_list(db, current_user.id, list_id, int(item_id)):
                removed += 1
        except ValueError:
            continue

    return SuccessResponse(success=True, message=f"Removed {removed} items from list")


# === Check Item Status ===

@router.get("/check/{sys_id}", response_model=List[str])
async def check_item_lists(
    sys_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Check which lists contain a specific item."""
    list_ids = ListsService.is_item_in_any_list(db, current_user.id, sys_id)
    return [str(lid) for lid in list_ids]


# === Project Endpoints ===

@router.get("/projects/", response_model=List[ProjectResponse])
async def get_projects(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get all projects for the current user."""
    projects = ListsService.get_projects(db, current_user.id)
    return [ProjectResponse(**p.to_dict()) for p in projects]


@router.post("/projects/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    data: ProjectCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Create a new project."""
    project = ListsService.create_project(db, current_user.id, data)
    return ProjectResponse(**project.to_dict())


@router.put("/projects/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: int,
    data: ProjectUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update a project's properties."""
    project = ListsService.update_project(db, current_user.id, project_id, data)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    return ProjectResponse(**project.to_dict())


@router.delete("/projects/{project_id}", response_model=SuccessResponse)
async def delete_project(
    project_id: int,
    delete_lists: bool = Query(False, description="Also delete lists in this project"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Delete a project."""
    success = ListsService.delete_project(db, current_user.id, project_id, delete_lists)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    return SuccessResponse(success=True, message="Project deleted successfully")


# === Recent Items ===

@router.get("/recent/items", response_model=List[RecentItemResponse])
async def get_recent_items(
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get recently viewed items."""
    items = ListsService.get_recent_items(db, current_user.id, limit)
    return [RecentItemResponse(**item.to_dict()) for item in items]


@router.post("/recent/items", response_model=RecentItemResponse)
async def add_recent_item(
    data: AddRecentItemRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Add or update a recently viewed item."""
    item = ListsService.add_recent_item(
        db, current_user.id, data.sys_id,
        data.shelfmark, data.title, data.fl_id
    )
    return RecentItemResponse(**item.to_dict())


# === Migration ===

@router.post("/migrate", response_model=MigrateListsResponse)
async def migrate_local_lists(
    data: MigrateListsRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Migrate local (device) lists to user account.
    This endpoint is called when a user logs in and has local lists.
    """
    lists_migrated, items_migrated, projects_migrated, recent_migrated = \
        ListsService.migrate_local_lists(db, current_user.id, data)

    return MigrateListsResponse(
        lists_migrated=lists_migrated,
        items_migrated=items_migrated,
        projects_migrated=projects_migrated,
        recent_items_migrated=recent_migrated
    )
