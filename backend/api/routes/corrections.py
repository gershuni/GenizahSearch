"""
Correction Routes - CRUD, Submission, Review, Voting
"""
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...schemas.correction import (
    CorrectionCreate, CorrectionUpdate, CorrectionResponse,
    CorrectionSubmit, CorrectionReview, CorrectionVoteCreate,
    CorrectionListResponse, CorrectionSearchParams, CorrectionStats,
    CorrectionStatusEnum, CorrectionTypeEnum
)
from ...schemas.common import SuccessResponse
from ...services.correction_service import CorrectionService
from ...services.comment_service import CommentService
from ..deps import (
    get_current_active_user, get_current_user_optional,
    require_reviewer, get_client_info
)

router = APIRouter(prefix="/corrections", tags=["Corrections"])


def _enrich_correction_response(
    correction,
    db: Session,
    current_user: Optional[User] = None
) -> CorrectionResponse:
    """Add computed fields to correction response"""
    response = CorrectionResponse.model_validate(correction)
    response.vote_score = correction.vote_score

    # Add user's vote if authenticated
    if current_user:
        response.user_vote = CorrectionService.get_user_vote(
            db, current_user.id, correction.id
        )

    # Add comment count
    comments, total = CommentService.get_comments_for_correction(db, correction.id)
    response.comment_count = total

    return response


@router.post("/", response_model=CorrectionResponse, status_code=status.HTTP_201_CREATED)
async def create_correction(
    data: CorrectionCreate,
    request: Request,
    save_as_draft: bool = Query(False, description="Save as draft without submitting"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new correction.

    If save_as_draft=True: Saves as draft (DRAFT status)
    If save_as_draft=False (default): Auto-submits
      - For Editors/Admins: Auto-approved immediately
      - For Contributors: Submitted for review (PENDING status)

    - **document_id**: ID of the document being corrected
    - **original_text**: The original text being corrected
    - **corrected_text**: The suggested correction
    - **correction_type**: Type of correction
    - **line_number**: Optional line number
    - **confidence_score**: Your confidence in the correction (0-1)
    - **source_reference**: Academic source if applicable
    - **notes**: Additional notes
    - **save_as_draft**: If true, save as draft without submitting
    """
    client_info = get_client_info(request)

    # Step 1: Create correction (DRAFT status)
    correction, error = CorrectionService.create_correction(
        db, current_user, data,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    # If saving as draft, return without submitting
    if save_as_draft:
        return _enrich_correction_response(correction, db, current_user)

    # Step 2: Auto-submit the correction
    # This will auto-approve for editors/admins, or set to PENDING for contributors
    submitted, submit_error = CorrectionService.submit_correction(
        db, current_user, correction.id,
        notes=data.notes,
        ip_address=client_info.get('ip_address')
    )

    if submit_error:
        # Submission failed - return the draft correction with error info
        # This shouldn't normally happen
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=submit_error
        )

    # Return the submitted (and possibly auto-approved) correction
    return _enrich_correction_response(submitted, db, current_user)


@router.get("/", response_model=CorrectionListResponse)
async def list_corrections(
    document_id: Optional[str] = None,
    shelfmark: Optional[str] = None,
    system_id: Optional[str] = None,
    author_id: Optional[int] = None,
    status: Optional[CorrectionStatusEnum] = None,
    correction_type: Optional[CorrectionTypeEnum] = None,
    min_quality_score: Optional[float] = None,
    has_source: Optional[bool] = None,
    search_text: Optional[str] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    List corrections with various filters.
    """
    params = CorrectionSearchParams(
        document_id=document_id,
        shelfmark=shelfmark,
        system_id=system_id,
        author_id=author_id,
        status=status,
        correction_type=correction_type,
        min_quality_score=min_quality_score,
        has_source=has_source,
        search_text=search_text,
        created_after=created_after,
        created_before=created_before
    )

    corrections, total = CorrectionService.search_corrections(
        db, params, page=page, page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CorrectionListResponse(
        items=[_enrich_correction_response(c, db, current_user) for c in corrections],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/my", response_model=CorrectionListResponse)
async def list_my_corrections(
    status: Optional[CorrectionStatusEnum] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """List current user's corrections"""
    params = CorrectionSearchParams(
        author_id=current_user.id,
        status=status
    )

    corrections, total = CorrectionService.search_corrections(
        db, params, page=page, page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CorrectionListResponse(
        items=[_enrich_correction_response(c, db, current_user) for c in corrections],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/pending", response_model=CorrectionListResponse)
async def list_pending_corrections(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(require_reviewer),
    db: Session = Depends(get_db)
):
    """
    List corrections pending review (Reviewers only).
    """
    corrections, total = CorrectionService.get_pending_corrections(
        db, page=page, page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CorrectionListResponse(
        items=[_enrich_correction_response(c, db, current_user) for c in corrections],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/stats", response_model=CorrectionStats)
async def get_correction_stats(
    db: Session = Depends(get_db)
):
    """Get overall correction statistics"""
    return CorrectionService.get_correction_stats(db)


@router.get("/document/{document_id}", response_model=List[CorrectionResponse])
async def get_document_corrections(
    document_id: str,
    include_drafts: bool = Query(False, description="Include your draft corrections"),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Get all approved corrections for a document.
    Optionally include current user's drafts.
    """
    user_id = current_user.id if current_user and include_drafts else None

    corrections = CorrectionService.get_corrections_for_document(
        db, document_id,
        include_drafts=include_drafts,
        user_id=user_id
    )

    return [_enrich_correction_response(c, db, current_user) for c in corrections]


@router.get("/{correction_id}", response_model=CorrectionResponse)
async def get_correction(
    correction_id: int,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get a specific correction by ID"""
    correction = CorrectionService.get_correction(db, correction_id)

    if not correction:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Correction not found"
        )

    return _enrich_correction_response(correction, db, current_user)


@router.put("/{correction_id}", response_model=CorrectionResponse)
async def update_correction(
    correction_id: int,
    data: CorrectionUpdate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update a correction (only drafts or needs_revision).
    """
    client_info = get_client_info(request)

    correction, error = CorrectionService.update_correction(
        db, current_user, correction_id, data,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_correction_response(correction, db, current_user)


@router.post("/{correction_id}/submit", response_model=CorrectionResponse)
async def submit_correction(
    correction_id: int,
    data: Optional[CorrectionSubmit] = None,
    request: Request = None,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Submit a draft correction for review.
    """
    client_info = get_client_info(request) if request else {}
    notes = data.notes if data else None

    correction, error = CorrectionService.submit_correction(
        db, current_user, correction_id, notes=notes,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_correction_response(correction, db, current_user)


@router.post("/{correction_id}/review", response_model=CorrectionResponse)
async def review_correction(
    correction_id: int,
    review: CorrectionReview,
    request: Request,
    current_user: User = Depends(require_reviewer),
    db: Session = Depends(get_db)
):
    """
    Review a correction (approve, reject, or request revision).
    Requires reviewer role.

    - **action**: 'approve', 'reject', or 'request_revision'
    - **review_notes**: Optional notes (required for request_revision)
    - **rejection_reason**: Required when rejecting
    """
    client_info = get_client_info(request)

    correction, error = CorrectionService.review_correction(
        db, current_user, correction_id, review,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_correction_response(correction, db, current_user)


@router.post("/{correction_id}/vote", response_model=CorrectionResponse)
async def vote_on_correction(
    correction_id: int,
    vote: CorrectionVoteCreate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Vote on a correction (upvote or downvote).
    Voting again with same value removes the vote.
    Voting with different value changes the vote.

    - **vote_value**: +1 for upvote, -1 for downvote
    """
    client_info = get_client_info(request)

    correction, error = CorrectionService.vote_on_correction(
        db, current_user, correction_id, vote.vote_value,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_correction_response(correction, db, current_user)


@router.delete("/{correction_id}", response_model=SuccessResponse)
async def delete_correction(
    correction_id: int,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete a correction (only drafts, or admin can delete any).
    """
    client_info = get_client_info(request)

    success, error = CorrectionService.delete_correction(
        db, current_user, correction_id,
        ip_address=client_info.get('ip_address')
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="Correction deleted successfully"
    )
