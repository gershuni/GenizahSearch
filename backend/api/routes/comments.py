"""
Comment Routes - CRUD, Reactions, Threading
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...models.comment import CommentType
from ...schemas.comment import (
    CommentCreate, CommentUpdate, CommentResponse,
    CommentReactionCreate, CommentListResponse, CommentThread,
    ReactionSummary
)
from ...schemas.common import SuccessResponse
from ...services.comment_service import CommentService
from ..deps import (
    get_current_active_user, get_current_user_optional,
    get_client_info
)

router = APIRouter(prefix="/comments", tags=["Comments"])


def _enrich_comment_response(
    comment,
    db: Session,
    current_user: Optional[User] = None
) -> CommentResponse:
    """Add computed fields to comment response"""
    response = CommentResponse.model_validate(comment)

    # Get reaction summary
    summary = CommentService.get_reactions_summary(db, comment.id)
    response.reactions_summary = {
        'like': summary.like,
        'helpful': summary.helpful,
        'insightful': summary.insightful,
        'thanks': summary.thanks,
        'disagree': summary.disagree,
        'total': summary.total
    }

    # Get user's reactions if authenticated
    if current_user:
        response.user_reactions = CommentService.get_user_reactions(
            db, current_user.id, comment.id
        )

    return response


@router.post("/", response_model=CommentResponse, status_code=status.HTTP_201_CREATED)
async def create_comment(
    data: CommentCreate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new comment.

    - **document_id**: ID of the document (optional if on correction)
    - **correction_id**: ID of the correction (optional if on document)
    - **parent_id**: ID of parent comment for replies
    - **content**: Comment text (supports markdown)
    - **comment_type**: Type of comment (general, question, etc.)
    - **line_number**: Optional line for inline comments
    """
    client_info = get_client_info(request)

    comment, error = CommentService.create_comment(
        db, current_user, data,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_comment_response(comment, db, current_user)


@router.get("/my", response_model=CommentListResponse)
async def get_my_comments(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get comments by the current user"""
    comments, total = CommentService.get_comments_by_user(
        db, current_user.id,
        page=page,
        page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CommentListResponse(
        items=[_enrich_comment_response(c, db, current_user) for c in comments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/document/{document_id}", response_model=CommentListResponse)
async def get_document_comments(
    document_id: str,
    include_replies: bool = Query(True, description="Include nested replies"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get comments for a document"""
    comments, total = CommentService.get_comments_for_document(
        db, document_id,
        include_replies=include_replies,
        page=page,
        page_size=page_size,
        current_user_id=current_user.id if current_user else None
    )

    total_pages = (total + page_size - 1) // page_size

    return CommentListResponse(
        items=[_enrich_comment_response(c, db, current_user) for c in comments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/document/{document_id}/inline", response_model=List[CommentResponse])
async def get_inline_comments(
    document_id: str,
    line_start: Optional[int] = None,
    line_end: Optional[int] = None,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get inline comments for specific lines of a document"""
    comments = CommentService.get_inline_comments(
        db, document_id,
        line_start=line_start,
        line_end=line_end
    )

    return [_enrich_comment_response(c, db, current_user) for c in comments]


@router.get("/correction/{correction_id}", response_model=CommentListResponse)
async def get_correction_comments(
    correction_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get comments on a correction"""
    comments, total = CommentService.get_comments_for_correction(
        db, correction_id,
        page=page,
        page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CommentListResponse(
        items=[_enrich_comment_response(c, db, current_user) for c in comments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@router.get("/{comment_id}", response_model=CommentResponse)
async def get_comment(
    comment_id: int,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get a specific comment by ID"""
    comment = CommentService.get_comment(db, comment_id)

    if not comment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Comment not found"
        )

    return _enrich_comment_response(comment, db, current_user)


@router.get("/{comment_id}/thread", response_model=CommentThread)
async def get_comment_thread(
    comment_id: int,
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Get a comment and all its replies"""
    root, replies = CommentService.get_comment_thread(db, comment_id)

    if not root:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Comment not found"
        )

    return CommentThread(
        root=_enrich_comment_response(root, db, current_user),
        replies=[_enrich_comment_response(r, db, current_user) for r in replies],
        total_replies=len(replies)
    )


@router.put("/{comment_id}", response_model=CommentResponse)
async def update_comment(
    comment_id: int,
    data: CommentUpdate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update a comment (only author can edit)"""
    client_info = get_client_info(request)

    comment, error = CommentService.update_comment(
        db, current_user, comment_id, data,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_comment_response(comment, db, current_user)


@router.delete("/{comment_id}", response_model=SuccessResponse)
async def delete_comment(
    comment_id: int,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Delete a comment (soft delete)"""
    client_info = get_client_info(request)

    success, error = CommentService.delete_comment(
        db, current_user, comment_id,
        ip_address=client_info.get('ip_address')
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="Comment deleted successfully"
    )


@router.post("/{comment_id}/resolve", response_model=CommentResponse)
async def resolve_comment(
    comment_id: int,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Mark a question/issue comment as resolved"""
    client_info = get_client_info(request)

    comment, error = CommentService.resolve_comment(
        db, current_user, comment_id,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_comment_response(comment, db, current_user)


@router.post("/{comment_id}/pin", response_model=CommentResponse)
async def toggle_pin_comment(
    comment_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Toggle pin status on a comment (editor/admin only)"""
    comment, error = CommentService.toggle_pin(db, current_user, comment_id)

    if error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error
        )

    return _enrich_comment_response(comment, db, current_user)


@router.post("/{comment_id}/react", response_model=CommentResponse)
async def react_to_comment(
    comment_id: int,
    reaction: CommentReactionCreate,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Add a reaction to a comment.
    If same reaction exists, it will be removed (toggle).

    - **reaction_type**: 'like', 'helpful', 'insightful', 'thanks', 'disagree'
    """
    client_info = get_client_info(request)

    comment, error = CommentService.add_reaction(
        db, current_user, comment_id, reaction.reaction_type,
        ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _enrich_comment_response(comment, db, current_user)


@router.get("/{comment_id}/reactions", response_model=ReactionSummary)
async def get_comment_reactions(
    comment_id: int,
    db: Session = Depends(get_db)
):
    """Get reaction summary for a comment"""
    comment = CommentService.get_comment(db, comment_id)

    if not comment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Comment not found"
        )

    return CommentService.get_reactions_summary(db, comment_id)


@router.get("/search/", response_model=CommentListResponse)
async def search_comments(
    query: str,
    document_id: Optional[str] = None,
    author_id: Optional[int] = None,
    comment_type: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """Search comments by content"""
    ctype = CommentType(comment_type) if comment_type else None

    comments, total = CommentService.search_comments(
        db, query,
        document_id=document_id,
        author_id=author_id,
        comment_type=ctype,
        current_user_id=current_user.id if current_user else None,
        page=page,
        page_size=page_size
    )

    total_pages = (total + page_size - 1) // page_size

    return CommentListResponse(
        items=[_enrich_comment_response(c, db, current_user) for c in comments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )
