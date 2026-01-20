"""
Discoveries API Routes - Endpoints for discoveries, questions, and activity feed
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...models.discovery import DiscoveryType, DiscoveryStatus
from ...schemas.discovery import (
    DiscoveryCreate, DiscoveryUpdate, DiscoveryResponse as DiscoveryResponseSchema,
    DiscoveryListResponse, ResponseCreate, ResponseItem, ResponseListResponse,
    DiscoveryStats, FeedResponse, AuthorInfo
)
from ...schemas.common import SuccessResponse
from ...services.discovery_service import DiscoveryService
from ..deps import get_current_active_user, get_current_user_optional, require_admin

router = APIRouter(prefix="/discoveries", tags=["Discoveries"])


# ============================================
# Helper to convert Discovery to response
# ============================================

def _to_response(discovery, db: Session = None) -> DiscoveryResponseSchema:
    """Convert Discovery model to response schema"""
    from backend.models.discovery import DiscoveryStatus
    return DiscoveryResponseSchema(
        id=discovery.id,
        title=discovery.title,
        content=discovery.content,
        discovery_type=discovery.discovery_type,
        document_id=discovery.document_id,
        page_number=discovery.page_number,
        shelfmark=discovery.shelfmark,
        is_anonymous=discovery.is_anonymous,
        author=AuthorInfo.from_user(discovery.user, discovery.is_anonymous),
        status=discovery.status,
        is_featured=discovery.is_featured,
        is_pinned=getattr(discovery, 'is_pinned', False),
        is_answered=getattr(discovery, 'is_answered', False),
        is_hidden=discovery.status == DiscoveryStatus.HIDDEN,
        view_count=discovery.view_count,
        response_count=discovery.response_count,
        upvotes=getattr(discovery, 'upvotes', 0) or 0,
        downvotes=getattr(discovery, 'downvotes', 0) or 0,
        created_at=discovery.created_at,
        updated_at=discovery.updated_at,
        # Include new fields
        additional_shelfmarks=getattr(discovery, 'additional_shelfmarks', None),
        related_manuscripts=getattr(discovery, 'related_manuscripts', None),
        all_shelfmarks=discovery.all_shelfmarks if hasattr(discovery, 'all_shelfmarks') else None,
        related_docs=discovery.related_docs if hasattr(discovery, 'related_docs') else None
    )


# ============================================
# Discovery CRUD endpoints
# ============================================

@router.post("/", response_model=DiscoveryResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_discovery(
    data: DiscoveryCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new discovery/question/note.

    Requires authentication.
    """
    discovery, error = DiscoveryService.create_discovery(db, data, current_user)

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _to_response(discovery, db)


@router.get("/{discovery_id}", response_model=DiscoveryResponseSchema)
async def get_discovery(
    discovery_id: int,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Get a single discovery by ID.

    Public endpoint (but may show limited info for anonymous users).
    Admins can view hidden discoveries.
    """
    # Admins can see hidden discoveries
    is_admin = current_user and current_user.role == 'admin'
    discovery = DiscoveryService.get_discovery(
        db, discovery_id, increment_views=True, include_hidden=is_admin
    )

    if not discovery:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Discovery not found"
        )

    return _to_response(discovery, db)


@router.put("/{discovery_id}", response_model=DiscoveryResponseSchema)
async def update_discovery(
    discovery_id: int,
    data: DiscoveryUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a discovery.

    Only the author or admin can update.
    """
    discovery, error = DiscoveryService.update_discovery(db, discovery_id, data, current_user)

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _to_response(discovery, db)


@router.delete("/{discovery_id}", response_model=SuccessResponse)
async def delete_discovery(
    discovery_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete (hide) a discovery.

    Only the author or admin can delete.
    """
    success, error = DiscoveryService.delete_discovery(db, discovery_id, current_user)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Discovery deleted")


@router.get("/", response_model=DiscoveryListResponse)
async def list_discoveries(
    discovery_type: Optional[DiscoveryType] = None,
    featured_only: bool = False,
    include_hidden: bool = False,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    List discoveries with optional filters.

    Public endpoint. include_hidden only works for admins.
    """
    # Only admins can see hidden discoveries
    can_see_hidden = include_hidden and current_user and current_user.role == 'admin'

    discoveries, total = DiscoveryService.list_discoveries(
        db,
        discovery_type=discovery_type,
        featured_only=featured_only,
        include_hidden=can_see_hidden,
        limit=limit,
        offset=offset
    )

    return DiscoveryListResponse(
        items=[_to_response(d, db) for d in discoveries],
        total=total,
        offset=offset,
        limit=limit
    )


# ============================================
# Response endpoints
# ============================================

@router.post("/{discovery_id}/responses", response_model=ResponseItem, status_code=status.HTTP_201_CREATED)
async def add_response(
    discovery_id: int,
    data: ResponseCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Add a response to a discovery.

    Requires authentication.
    """
    response, error = DiscoveryService.add_response(db, discovery_id, data, current_user)

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return ResponseItem(
        id=response.id,
        author=AuthorInfo.from_user(response.user, response.is_anonymous),
        content=response.content,
        created_at=response.created_at,
        updated_at=response.updated_at
    )


@router.get("/{discovery_id}/responses", response_model=ResponseListResponse)
async def get_responses(
    discovery_id: int,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Get all responses for a discovery.

    Public endpoint.
    """
    responses = DiscoveryService.get_responses(db, discovery_id)

    return ResponseListResponse(
        items=[
            ResponseItem(
                id=r.id,
                author=AuthorInfo.from_user(r.user, r.is_anonymous),
                content=r.content,
                created_at=r.created_at,
                updated_at=r.updated_at
            )
            for r in responses
        ],
        total=len(responses)
    )


# ============================================
# Feed and stats endpoints
# ============================================

@router.get("/feed/items", response_model=FeedResponse)
async def get_feed(
    item_type: Optional[str] = Query(None, pattern="^(all|discovery|question|correction|comment|join)$"),
    period: Optional[str] = Query(None, pattern="^(day|week|month|all)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_stats: bool = True,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Get activity feed combining discoveries, questions, corrections.

    Public endpoint. Admins see hidden items.
    """
    # Check if current user is admin
    is_admin = current_user and current_user.role == 'admin'

    items, total = DiscoveryService.get_feed(
        db,
        item_type=item_type,
        period=period,
        limit=limit,
        offset=offset,
        is_admin=is_admin
    )

    stats = DiscoveryService.get_stats(db) if include_stats else None

    return FeedResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit,
        stats=stats
    )


@router.get("/stats/summary", response_model=DiscoveryStats)
async def get_stats(
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Get overall statistics.

    Public endpoint.
    """
    return DiscoveryService.get_stats(db)


# ============================================
# Admin endpoints
# ============================================

@router.post("/{discovery_id}/feature", response_model=SuccessResponse)
async def feature_discovery(
    discovery_id: int,
    featured: bool = True,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Feature or unfeature a discovery.

    Admin only.
    """
    success, error = DiscoveryService.feature_discovery(db, discovery_id, featured)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Discovery featured" if featured else "Discovery unfeatured")


@router.post("/{discovery_id}/hide", response_model=SuccessResponse)
async def hide_discovery(
    discovery_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Hide a discovery (admin moderation).

    Admin only.
    """
    success, error = DiscoveryService.hide_discovery(db, discovery_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Discovery hidden")


@router.post("/{discovery_id}/unhide", response_model=SuccessResponse)
async def unhide_discovery(
    discovery_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Unhide a discovery (admin moderation).

    Admin only.
    """
    success, error = DiscoveryService.unhide_discovery(db, discovery_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Discovery unhidden")


@router.post("/{discovery_id}/pin", response_model=SuccessResponse)
async def pin_discovery(
    discovery_id: int,
    pinned: bool = True,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Pin or unpin a discovery.

    Admin only.
    """
    success, error = DiscoveryService.pin_discovery(db, discovery_id, pinned)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Discovery pinned" if pinned else "Discovery unpinned")


@router.post("/{discovery_id}/answer", response_model=SuccessResponse)
async def mark_answered(
    discovery_id: int,
    answered: bool = True,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Mark a question as answered.

    Author or admin only.
    """
    success, error = DiscoveryService.mark_answered(db, discovery_id, current_user, answered)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Question marked as answered" if answered else "Question marked as unanswered")


@router.post("/{discovery_id}/vote", response_model=SuccessResponse)
async def vote_discovery(
    discovery_id: int,
    vote_type: str,  # 'up', 'down', or 'none'
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Vote on a discovery (thumbs up/down).

    Requires authentication.
    """
    success, error, votes = DiscoveryService.vote_discovery(db, discovery_id, current_user, vote_type)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(success=True, message="Vote recorded", data=votes)
