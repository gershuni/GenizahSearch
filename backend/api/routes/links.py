"""
Links Routes - CRUD for fragment links
"""
from typing import Optional
from urllib.parse import unquote
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...schemas.link import (
    LinkCreate, LinkUpdate, LinkResponse,
    ConnectedFragmentsResponse, LinkSearchResponse,
    LinkedFragment, SuccessResponse
)
from ...services.link_service import LinkService
from ..deps import get_current_active_user, get_current_user_optional


router = APIRouter(prefix="/links", tags=["Links"])


def _link_to_response(link, db: Session) -> LinkResponse:
    """Convert link model to response schema"""
    creator_info = None
    if link.creator:
        creator_info = {
            "id": link.creator.id,
            "username": link.creator.username
        }

    return LinkResponse(
        id=link.id,
        fragment_a=link.fragment_a,
        fragment_b=link.fragment_b,
        document_id_a=link.document_id_a,
        document_id_b=link.document_id_b,
        relationship_type=link.relationship_type,
        notes=link.notes,
        source=link.source,
        source_url=link.source_url,
        created_by=creator_info,
        created_at=link.created_at,
        updated_at=link.updated_at
    )


@router.post("/", response_model=LinkResponse, status_code=status.HTTP_201_CREATED)
async def create_link(
    data: LinkCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a link between two fragments.

    - **fragment_a**: First fragment shelfmark
    - **fragment_b**: Second fragment shelfmark
    - **relationship_type**: Optional - 'physical_join' or 'same_composition'
    - **notes**: Optional notes about the link
    """
    link, error = LinkService.create_link(
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

    return _link_to_response(link, db)


@router.get("/connected/{shelfmark}", response_model=ConnectedFragmentsResponse)
async def get_connected_fragments(
    shelfmark: str,
    db: Session = Depends(get_db)
):
    """
    Get all fragments connected to the given shelfmark.

    Returns the full connected component - if A links to B and B links to C,
    querying any of them returns all three.

    The shelfmark is URL-encoded, so "T-S 13J35.3" becomes "T-S%2013J35.3"
    """
    # URL decode the shelfmark
    decoded_shelfmark = unquote(shelfmark)

    result = LinkService.get_connected_fragments(db, decoded_shelfmark)

    # Convert links to response format
    links_response = [_link_to_response(link, db) for link in result["links"]]

    # Convert fragment details
    fragment_details = [
        LinkedFragment(
            shelfmark=fd['shelfmark'],
            document_id=fd.get('document_id'),
            is_current=fd['is_current'],
            relationship_type=fd.get('relationship_type'),
            link_id=fd.get('link_id'),
            link_source=fd.get('link_source')
        )
        for fd in result["fragment_details"]
    ]

    return ConnectedFragmentsResponse(
        shelfmark=result["shelfmark"],
        shelfmark_normalized=result["shelfmark_normalized"],
        fragments=result["fragments"],
        fragment_details=fragment_details,
        links=links_response,
        total_fragments=result["total_fragments"],
        total_links=result["total_links"]
    )


@router.get("/{link_id}", response_model=LinkResponse)
async def get_link(
    link_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific link by ID"""
    link = LinkService.get_link_by_id(db, link_id)

    if not link:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found"
        )

    return _link_to_response(link, db)


@router.patch("/{link_id}", response_model=LinkResponse)
async def update_link(
    link_id: int,
    data: LinkUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a link's metadata.

    - **relationship_type**: New relationship type (or null to clear)
    - **notes**: New notes (or null to clear)
    """
    link = LinkService.update_link(
        db=db,
        link_id=link_id,
        relationship_type=data.relationship_type.value if data.relationship_type else None,
        notes=data.notes
    )

    if not link:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found"
        )

    return _link_to_response(link, db)


@router.delete("/{link_id}", response_model=SuccessResponse)
async def delete_link(
    link_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Delete a link (soft delete)"""
    success = LinkService.delete_link(db, link_id, current_user)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found"
        )

    return SuccessResponse(success=True, message="Link deleted")


@router.get("/", response_model=LinkSearchResponse)
async def search_links(
    q: Optional[str] = Query(None, description="Search shelfmarks"),
    source: Optional[str] = Query(None, description="Filter by source"),
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Search links by shelfmark pattern or filters.

    - **q**: Search query for shelfmarks (partial match)
    - **source**: Filter by source ('user', 'princeton', etc.)
    - **relationship_type**: Filter by relationship type
    """
    links, total = LinkService.search_links(
        db=db,
        query=q,
        source=source,
        relationship_type=relationship_type,
        limit=limit,
        offset=offset
    )

    return LinkSearchResponse(
        results=[_link_to_response(link, db) for link in links],
        total=total
    )


@router.get("/between/{fragment_a}/{fragment_b}", response_model=LinkResponse)
async def get_link_between(
    fragment_a: str,
    fragment_b: str,
    db: Session = Depends(get_db)
):
    """
    Get the link between two specific fragments if it exists.

    Shelfmarks are URL-encoded.
    """
    decoded_a = unquote(fragment_a)
    decoded_b = unquote(fragment_b)

    link = LinkService.get_link_between(db, decoded_a, decoded_b)

    if not link:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No link exists between these fragments"
        )

    return _link_to_response(link, db)
