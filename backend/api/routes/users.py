"""
User Routes - Profile, Statistics, Admin User Management
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User, UserRole
from ...schemas.user import (
    UserResponse, UserUpdate, UserProfile, UserStats, UserList
)
from ...schemas.common import SuccessResponse, PaginatedResponse
from ...services.user_service import UserService
from ..deps import (
    get_current_active_user, get_current_user_optional,
    require_admin, get_client_info, Pagination
)

router = APIRouter(prefix="/users", tags=["Users"])


@router.get("/me", response_model=UserResponse)
async def get_my_profile(
    current_user: User = Depends(get_current_active_user)
):
    """Get current user's full profile"""
    return current_user


@router.put("/me", response_model=UserResponse)
async def update_my_profile(
    update_data: UserUpdate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update current user's profile"""
    client_info = get_client_info(request)
    return UserService.update_user(
        db, current_user, update_data,
        ip_address=client_info.get('ip_address')
    )


@router.get("/me/stats", response_model=UserStats)
async def get_my_stats(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get current user's statistics"""
    return UserService.get_user_stats(db, current_user.id)


@router.get("/profile/{username}", response_model=UserProfile)
async def get_user_profile(
    username: str,
    db: Session = Depends(get_db)
):
    """Get public profile by username"""
    user = UserService.get_user_by_username(db, username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return user


@router.get("/profile/{username}/stats", response_model=UserStats)
async def get_user_stats(
    username: str,
    db: Session = Depends(get_db)
):
    """Get user's public statistics"""
    user = UserService.get_user_by_username(db, username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return UserService.get_user_stats(db, user.id)


@router.get("/{user_id}", response_model=UserProfile)
async def get_user_by_id(
    user_id: int,
    db: Session = Depends(get_db)
):
    """Get user profile by ID"""
    user = UserService.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return user


@router.get("/", response_model=UserList)
async def list_users(
    query: Optional[str] = None,
    role: Optional[UserRole] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    List users with filters (Admin only).
    """
    users, total = UserService.search_users(
        db,
        query=query,
        role=role,
        page=page,
        page_size=page_size
    )

    return UserList(
        users=users,
        total=total,
        page=page,
        page_size=page_size
    )


@router.put("/{user_id}/role", response_model=UserResponse)
async def update_user_role(
    user_id: int,
    role: UserRole,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Update a user's role (Admin only).
    """
    client_info = get_client_info(request)
    user, error = UserService.update_user_role(
        db, current_user, user_id, role,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return user


@router.post("/{user_id}/deactivate", response_model=SuccessResponse)
async def deactivate_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Deactivate a user account (Admin only)"""
    client_info = get_client_info(request)
    success, error = UserService.deactivate_user(
        db, current_user, user_id,
        ip_address=client_info.get('ip_address')
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="User deactivated successfully"
    )


@router.post("/{user_id}/activate", response_model=SuccessResponse)
async def activate_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Activate a user account (Admin only)"""
    client_info = get_client_info(request)
    success, error = UserService.activate_user(
        db, current_user, user_id,
        ip_address=client_info.get('ip_address')
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="User activated successfully"
    )


@router.get("/leaderboard/top", response_model=list[UserProfile])
async def get_leaderboard(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get top users by reputation"""
    return UserService.get_leaderboard(db, limit=limit)
