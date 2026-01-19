"""
Document Routes - Document metadata, corrected text, statistics
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime

from ...models.database import get_db
from ...models.user import User
from ...models.document_metadata import DocumentMetadata
from ...models.correction import CorrectionStatus
from ...services.correction_service import CorrectionService
from ...services.comment_service import CommentService
from ..deps import get_current_user_optional

router = APIRouter(prefix="/documents", tags=["Documents"])


class DocumentStats(BaseModel):
    """Statistics for a document"""
    document_id: str
    shelfmark: Optional[str] = None
    correction_count: int = 0
    approved_correction_count: int = 0
    pending_correction_count: int = 0
    comment_count: int = 0
    view_count: int = 0
    has_corrections: bool = False
    needs_review: bool = False
    transcription_quality_score: float = 0.0
    last_correction_at: Optional[datetime] = None
    last_comment_at: Optional[datetime] = None


class DocumentMetadataResponse(BaseModel):
    """Extended document metadata"""
    document_id: str
    system_id: Optional[str] = None
    shelfmark: Optional[str] = None
    source_version: Optional[str] = None
    user_title: Optional[str] = None
    user_description: Optional[str] = None
    user_date_attribution: Optional[str] = None
    user_provenance: Optional[str] = None
    user_genre: Optional[str] = None
    user_language: Optional[str] = None
    tags: Optional[list] = None
    categories: Optional[list] = None
    related_documents: Optional[list] = None
    external_links: Optional[dict] = None
    is_featured: bool = False
    is_verified: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CorrectedTextResponse(BaseModel):
    """Response with original and corrected text"""
    document_id: str
    original_text: str
    corrected_text: str
    corrections_applied: int
    has_corrections: bool


class DocumentMetadataUpdate(BaseModel):
    """Update document metadata"""
    user_title: Optional[str] = None
    user_description: Optional[str] = None
    user_date_attribution: Optional[str] = None
    user_provenance: Optional[str] = None
    user_genre: Optional[str] = None
    user_language: Optional[str] = None
    tags: Optional[list] = None
    categories: Optional[list] = None
    related_documents: Optional[list] = None
    external_links: Optional[dict] = None


@router.get("/{document_id}/stats", response_model=DocumentStats)
async def get_document_stats(
    document_id: str,
    db: Session = Depends(get_db)
):
    """
    Get statistics for a document.
    """
    metadata = db.query(DocumentMetadata).filter(
        DocumentMetadata.document_id == document_id
    ).first()

    if not metadata:
        # Return default stats if no metadata yet
        return DocumentStats(
            document_id=document_id,
            correction_count=0,
            approved_correction_count=0,
            pending_correction_count=0,
            comment_count=0,
            view_count=0,
            has_corrections=False,
            needs_review=False,
            transcription_quality_score=0.0
        )

    return DocumentStats(
        document_id=document_id,
        shelfmark=metadata.shelfmark,
        correction_count=metadata.correction_count or 0,
        approved_correction_count=metadata.approved_correction_count or 0,
        pending_correction_count=metadata.pending_correction_count or 0,
        comment_count=metadata.comment_count or 0,
        view_count=metadata.view_count or 0,
        has_corrections=metadata.has_corrections or False,
        needs_review=metadata.needs_review or False,
        transcription_quality_score=metadata.transcription_quality_score or 0.0,
        last_correction_at=metadata.last_correction_at,
        last_comment_at=metadata.last_comment_at
    )


@router.get("/{document_id}/metadata", response_model=DocumentMetadataResponse)
async def get_document_metadata(
    document_id: str,
    db: Session = Depends(get_db)
):
    """
    Get extended metadata for a document.
    """
    metadata = db.query(DocumentMetadata).filter(
        DocumentMetadata.document_id == document_id
    ).first()

    if not metadata:
        # Return basic metadata if not found
        return DocumentMetadataResponse(document_id=document_id)

    return DocumentMetadataResponse.model_validate(metadata)


@router.put("/{document_id}/metadata", response_model=DocumentMetadataResponse)
async def update_document_metadata(
    document_id: str,
    data: DocumentMetadataUpdate,
    current_user: User = Depends(get_current_user_optional),
    db: Session = Depends(get_db)
):
    """
    Update document metadata.
    Creates metadata record if it doesn't exist.
    """
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )

    metadata = db.query(DocumentMetadata).filter(
        DocumentMetadata.document_id == document_id
    ).first()

    if not metadata:
        metadata = DocumentMetadata(document_id=document_id)
        db.add(metadata)

    # Update fields
    if data.user_title is not None:
        metadata.user_title = data.user_title
    if data.user_description is not None:
        metadata.user_description = data.user_description
    if data.user_date_attribution is not None:
        metadata.user_date_attribution = data.user_date_attribution
    if data.user_provenance is not None:
        metadata.user_provenance = data.user_provenance
    if data.user_genre is not None:
        metadata.user_genre = data.user_genre
    if data.user_language is not None:
        metadata.user_language = data.user_language
    if data.tags is not None:
        metadata.tags = data.tags
    if data.categories is not None:
        metadata.categories = data.categories
    if data.related_documents is not None:
        metadata.related_documents = data.related_documents
    if data.external_links is not None:
        metadata.external_links = data.external_links

    db.commit()
    db.refresh(metadata)

    return DocumentMetadataResponse.model_validate(metadata)


@router.post("/{document_id}/corrected-text", response_model=CorrectedTextResponse)
async def get_corrected_text(
    document_id: str,
    original_text: str,
    db: Session = Depends(get_db)
):
    """
    Apply all approved corrections to document text.

    Send the original text and receive the corrected version.
    """
    corrected = CorrectionService.get_corrected_text_for_document(
        db, document_id, original_text
    )

    corrections = CorrectionService.get_corrections_for_document(
        db, document_id, include_drafts=False
    )

    return CorrectedTextResponse(
        document_id=document_id,
        original_text=original_text,
        corrected_text=corrected,
        corrections_applied=len(corrections),
        has_corrections=len(corrections) > 0
    )


@router.post("/{document_id}/view")
async def record_document_view(
    document_id: str,
    db: Session = Depends(get_db)
):
    """
    Record a view for a document.
    Used for tracking popularity.
    """
    metadata = db.query(DocumentMetadata).filter(
        DocumentMetadata.document_id == document_id
    ).first()

    if not metadata:
        metadata = DocumentMetadata(document_id=document_id)
        db.add(metadata)

    metadata.increment_views()
    db.commit()

    return {"success": True, "view_count": metadata.view_count}


@router.get("/featured", response_model=list[DocumentMetadataResponse])
async def get_featured_documents(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get featured documents"""
    documents = db.query(DocumentMetadata).filter(
        DocumentMetadata.is_featured == True
    ).limit(limit).all()

    return [DocumentMetadataResponse.model_validate(d) for d in documents]


@router.get("/most-corrected", response_model=list[DocumentStats])
async def get_most_corrected_documents(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get documents with the most corrections"""
    documents = db.query(DocumentMetadata).filter(
        DocumentMetadata.has_corrections == True
    ).order_by(
        DocumentMetadata.approved_correction_count.desc()
    ).limit(limit).all()

    return [DocumentStats(
        document_id=d.document_id,
        shelfmark=d.shelfmark,
        correction_count=d.correction_count or 0,
        approved_correction_count=d.approved_correction_count or 0,
        pending_correction_count=d.pending_correction_count or 0,
        comment_count=d.comment_count or 0,
        view_count=d.view_count or 0,
        has_corrections=d.has_corrections or False,
        needs_review=d.needs_review or False,
        transcription_quality_score=d.transcription_quality_score or 0.0,
        last_correction_at=d.last_correction_at,
        last_comment_at=d.last_comment_at
    ) for d in documents]


@router.get("/most-viewed", response_model=list[DocumentStats])
async def get_most_viewed_documents(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get most viewed documents"""
    documents = db.query(DocumentMetadata).filter(
        DocumentMetadata.view_count > 0
    ).order_by(
        DocumentMetadata.view_count.desc()
    ).limit(limit).all()

    return [DocumentStats(
        document_id=d.document_id,
        shelfmark=d.shelfmark,
        correction_count=d.correction_count or 0,
        approved_correction_count=d.approved_correction_count or 0,
        pending_correction_count=d.pending_correction_count or 0,
        comment_count=d.comment_count or 0,
        view_count=d.view_count or 0,
        has_corrections=d.has_corrections or False,
        needs_review=d.needs_review or False,
        transcription_quality_score=d.transcription_quality_score or 0.0,
        last_correction_at=d.last_correction_at,
        last_comment_at=d.last_comment_at
    ) for d in documents]
