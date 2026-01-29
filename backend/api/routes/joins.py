"""
Joins Routes - CRUD for fragment joins
"""
from typing import Optional
from urllib.parse import unquote
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...schemas.join import (
    JoinCreate, JoinUpdate, JoinResponse,
    ConnectedFragmentsResponse, JoinSearchResponse,
    JoinedFragment, SuccessResponse
)
from ...services.join_service import JoinService
from ..deps import get_current_active_user, get_current_user_optional
from ...rate_limiting import limiter


router = APIRouter(prefix="/joins", tags=["Joins"])


def _join_to_response(join, db: Session) -> JoinResponse:
    """Convert join model to response schema"""
    creator_info = None
    if join.creator:
        creator_info = {
            "id": join.creator.id,
            "username": join.creator.username
        }

    return JoinResponse(
        id=join.id,
        fragment_a=join.fragment_a,
        fragment_b=join.fragment_b,
        document_id_a=join.document_id_a,
        document_id_b=join.document_id_b,
        relationship_type=join.relationship_type,
        notes=join.notes,
        source=join.source,
        source_url=join.source_url,
        created_by=creator_info,
        created_at=join.created_at,
        updated_at=join.updated_at
    )


@router.post("/", response_model=JoinResponse, status_code=status.HTTP_201_CREATED)
async def create_join(
    data: JoinCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a join between two fragments.

    - **fragment_a**: First fragment shelfmark
    - **fragment_b**: Second fragment shelfmark
    - **relationship_type**: Optional - 'physical_join' or 'same_composition'
    - **notes**: Optional notes about the join
    """
    join, error = JoinService.create_join(
        db=db,
        fragment_a=data.fragment_a,
        fragment_b=data.fragment_b,
        relationship_type=data.relationship_type.value if data.relationship_type else None,
        notes=data.notes,
        source="user",
        user=current_user,
        document_id_a=data.document_id_a,
        document_id_b=data.document_id_b
    )

    if error:
        if "already exists" in error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _join_to_response(join, db)


@router.get("/connected", response_model=ConnectedFragmentsResponse)
async def get_connected_fragments_query(
    shelfmark: str = Query(None, description="Shelfmark to look up connected fragments"),
    document_id: str = Query(None, description="Document ID (sys_id) to look up connected fragments"),
    db: Session = Depends(get_db)
):
    """
    Get all fragments connected to the given shelfmark or document_id (query param version).

    Returns the full connected component - if A joins to B and B joins to C,
    querying any of them returns all three.

    Use document_id for reliable lookups: /joins/connected?document_id=990001234567890
    Or use shelfmark: /joins/connected?shelfmark=T-S%2013J35.3
    """
    # Prefer document_id lookup if provided
    if document_id:
        result = JoinService.get_connected_fragments_by_document_id(db, document_id)
    elif shelfmark:
        result = JoinService.get_connected_fragments(db, shelfmark)
    else:
        # Return empty result if neither provided
        result = {
            "shelfmark": None,
            "shelfmark_normalized": None,
            "fragments": [],
            "fragment_details": [],
            "joins": [],
            "total_fragments": 0,
            "total_joins": 0
        }

    # Convert joins to response format
    joins_response = [_join_to_response(join, db) for join in result["joins"]]

    # Convert fragment details
    fragment_details = [
        JoinedFragment(
            shelfmark=fd['shelfmark'],
            document_id=fd.get('document_id'),
            is_current=fd['is_current'],
            relationship_type=fd.get('relationship_type'),
            join_id=fd.get('join_id'),
            join_source=fd.get('join_source')
        )
        for fd in result["fragment_details"]
    ]

    return ConnectedFragmentsResponse(
        shelfmark=result["shelfmark"],
        shelfmark_normalized=result["shelfmark_normalized"],
        fragments=result["fragments"],
        fragment_details=fragment_details,
        joins=joins_response,
        total_fragments=result["total_fragments"],
        total_joins=result["total_joins"]
    )


@router.get("/connected/{shelfmark:path}", response_model=ConnectedFragmentsResponse)
async def get_connected_fragments(
    shelfmark: str,
    db: Session = Depends(get_db)
):
    """
    Get all fragments connected to the given shelfmark (path param version).

    Returns the full connected component - if A joins to B and B joins to C,
    querying any of them returns all three.

    The shelfmark is URL-encoded, so "T-S 13J35.3" becomes "T-S%2013J35.3"

    Note: For shelfmarks with / character, prefer using /joins/connected?shelfmark=...
    """
    # URL decode the shelfmark
    decoded_shelfmark = unquote(shelfmark)

    result = JoinService.get_connected_fragments(db, decoded_shelfmark)

    # Convert joins to response format
    joins_response = [_join_to_response(join, db) for join in result["joins"]]

    # Convert fragment details
    fragment_details = [
        JoinedFragment(
            shelfmark=fd['shelfmark'],
            document_id=fd.get('document_id'),
            is_current=fd['is_current'],
            relationship_type=fd.get('relationship_type'),
            join_id=fd.get('join_id'),
            join_source=fd.get('join_source')
        )
        for fd in result["fragment_details"]
    ]

    return ConnectedFragmentsResponse(
        shelfmark=result["shelfmark"],
        shelfmark_normalized=result["shelfmark_normalized"],
        fragments=result["fragments"],
        fragment_details=fragment_details,
        joins=joins_response,
        total_fragments=result["total_fragments"],
        total_joins=result["total_joins"]
    )


@router.get("/my", response_model=JoinSearchResponse)
async def get_my_joins(
    q: Optional[str] = Query(None, description="Search shelfmarks"),
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get current user's joins.

    - **q**: Search query for shelfmarks (partial match)
    - **relationship_type**: Filter by relationship type
    """
    joins, total = JoinService.search_joins(
        db=db,
        query=q,
        source='user',  # User-created joins only
        relationship_type=relationship_type,
        created_by_id=current_user.id,
        limit=limit,
        offset=offset
    )

    return JoinSearchResponse(
        results=[_join_to_response(join, db) for join in joins],
        total=total
    )


@router.get("/{join_id}", response_model=JoinResponse)
async def get_join(
    join_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific join by ID"""
    join = JoinService.get_join_by_id(db, join_id)

    if not join:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Join not found"
        )

    return _join_to_response(join, db)


@router.patch("/{join_id}", response_model=JoinResponse)
async def update_join(
    join_id: int,
    data: JoinUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a join's metadata.

    - **relationship_type**: New relationship type (or null to clear)
    - **notes**: New notes (or null to clear)
    """
    join = JoinService.update_join(
        db=db,
        join_id=join_id,
        relationship_type=data.relationship_type.value if data.relationship_type else None,
        notes=data.notes
    )

    if not join:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Join not found"
        )

    return _join_to_response(join, db)


@router.delete("/{join_id}", response_model=SuccessResponse)
async def delete_join(
    join_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Delete a join (soft delete)"""
    success = JoinService.delete_join(db, join_id, current_user)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Join not found"
        )

    return SuccessResponse(success=True, message="Join deleted")


@router.get("/", response_model=JoinSearchResponse)
@limiter.limit("30/minute")
async def search_joins(
    request: Request,
    q: Optional[str] = Query(None, description="Search shelfmarks"),
    source: Optional[str] = Query(None, description="Filter by source"),
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Search joins by shelfmark pattern or filters.

    - **q**: Search query for shelfmarks (partial match)
    - **source**: Filter by source ('user', 'princeton', etc.)
    - **relationship_type**: Filter by relationship type
    """
    joins, total = JoinService.search_joins(
        db=db,
        query=q,
        source=source,
        relationship_type=relationship_type,
        limit=limit,
        offset=offset
    )

    return JoinSearchResponse(
        results=[_join_to_response(join, db) for join in joins],
        total=total
    )


@router.get("/between/{fragment_a}/{fragment_b}", response_model=JoinResponse)
async def get_join_between(
    fragment_a: str,
    fragment_b: str,
    db: Session = Depends(get_db)
):
    """
    Get the join between two specific fragments if it exists.

    Shelfmarks are URL-encoded.
    """
    decoded_a = unquote(fragment_a)
    decoded_b = unquote(fragment_b)

    join = JoinService.get_join_between(db, decoded_a, decoded_b)

    if not join:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No join exists between these fragments"
        )

    return _join_to_response(join, db)
