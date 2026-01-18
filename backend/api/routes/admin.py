"""
Admin Routes - User Approval and System Management
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User, UserRole
from ...schemas.user import UserResponse, UserList
from ...schemas.common import SuccessResponse
from ...services.user_service import UserService
from ..deps import require_admin, get_client_info

router = APIRouter(prefix="/admin", tags=["Admin"])


@router.get("/users/pending", response_model=List[UserResponse])
async def list_pending_users(
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    List users pending approval (inactive users).

    Admin only.
    """
    pending_users = db.query(User).filter(
        User.is_active == False,
        User.role != UserRole.ADMIN  # Don't list deactivated admins
    ).order_by(User.created_at.desc()).all()

    return pending_users


@router.post("/users/{user_id}/approve", response_model=SuccessResponse)
async def approve_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Approve a pending user (activate their account).

    Admin only.
    """
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    if user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is already active"
        )

    user.is_active = True
    db.commit()

    return SuccessResponse(
        success=True,
        message=f"User {user.username} approved successfully"
    )


@router.post("/users/{user_id}/reject", response_model=SuccessResponse)
async def reject_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Reject a pending user (delete their account).

    Admin only.
    """
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    if user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot reject an active user. Deactivate first."
        )

    if user.role == UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot reject admin users"
        )

    # Delete the user
    db.delete(user)
    db.commit()

    return SuccessResponse(
        success=True,
        message="User rejected and removed"
    )


@router.delete("/users/{user_id}", response_model=SuccessResponse)
async def delete_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Delete a user account permanently.

    Admin only. Cannot delete yourself or other admins.
    """
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    if user.role == UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete admin users"
        )

    username = user.username
    db.delete(user)
    db.commit()

    return SuccessResponse(
        success=True,
        message=f"User {username} deleted successfully"
    )


@router.get("/stats/overview")
async def get_admin_stats(
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Get system statistics for admin dashboard.
    """
    from ...models.correction import Correction, CorrectionStatus
    from ...models.comment import Comment
    from sqlalchemy import func

    total_users = db.query(func.count(User.id)).scalar()
    active_users = db.query(func.count(User.id)).filter(User.is_active == True).scalar()
    pending_users = db.query(func.count(User.id)).filter(User.is_active == False).scalar()

    editors_admins = db.query(func.count(User.id)).filter(
        User.role.in_([UserRole.EDITOR, UserRole.ADMIN, UserRole.REVIEWER])
    ).scalar()

    total_corrections = db.query(func.count(Correction.id)).scalar()
    pending_corrections = db.query(func.count(Correction.id)).filter(
        Correction.status == CorrectionStatus.PENDING
    ).scalar()
    approved_corrections = db.query(func.count(Correction.id)).filter(
        Correction.status == CorrectionStatus.APPROVED
    ).scalar()

    total_comments = db.query(func.count(Comment.id)).scalar()

    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "pending": pending_users,
            "editors_admins": editors_admins
        },
        "corrections": {
            "total": total_corrections,
            "pending": pending_corrections,
            "approved": approved_corrections
        },
        "comments": {
            "total": total_comments
        }
    }
