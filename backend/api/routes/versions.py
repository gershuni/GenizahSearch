"""
Version Routes - API for transcription versions
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...models.transcription_version import TranscriptionVersion, VersionSource
from ...schemas.version import (
    VersionCreate, VersionResponse, VersionListItem,
    PageVersionsResponse, SetDefaultRequest, ImportVersionRequest,
    VersionStats
)
from ...schemas.common import SuccessResponse
from ...services.version_service import VersionService
from ..deps import get_current_active_user, get_current_user_optional, require_admin, require_editor

router = APIRouter(prefix="/versions", tags=["Versions"])


def _to_response(version: TranscriptionVersion, db: Session) -> VersionResponse:
    """Convert version model to response schema"""
    user_brief = None
    if version.user:
        from ...schemas.version import UserBrief
        user_brief = UserBrief(
            id=version.user.id,
            username=version.user.username,
            full_name=version.user.full_name
        )

    return VersionResponse(
        id=version.id,
        sys_id=version.sys_id,
        page_num=version.page_num,
        content=version.content,
        source=version.source,
        user=user_brief,
        change_description=version.change_description,
        version_number=version.version_number,
        is_current_default=version.is_current_default,
        quality_score=version.quality_score,
        created_at=version.created_at,
        is_indexed=version.is_indexed
    )


def _to_list_item(version: TranscriptionVersion) -> VersionListItem:
    """Convert version to list item"""
    return VersionListItem(
        id=version.id,
        source=version.source,
        user_name=version.user.full_name if version.user else None,
        version_number=version.version_number,
        is_current_default=version.is_current_default,
        created_at=version.created_at,
        change_description=version.change_description
    )


@router.get("/{sys_id}/{page_num}", response_model=PageVersionsResponse)
async def get_page_versions(
    sys_id: str,
    page_num: int = 1,
    db: Session = Depends(get_db)
):
    """
    Get all transcription versions for a specific page.

    Returns the current default version and a list of all available versions.
    """
    current_default, all_versions = VersionService.get_page_versions(db, sys_id, page_num)

    return PageVersionsResponse(
        sys_id=sys_id,
        page_num=page_num,
        current_default=_to_response(current_default, db) if current_default else None,
        all_versions=[_to_list_item(v) for v in all_versions],
        total=len(all_versions)
    )


@router.get("/{sys_id}/{page_num}/default", response_model=Optional[VersionResponse])
async def get_default_version(
    sys_id: str,
    page_num: int = 1,
    db: Session = Depends(get_db)
):
    """
    Get the current default version for a page.

    This is the version that should be displayed by default.
    """
    version = VersionService.get_default_version(db, sys_id, page_num)

    if not version:
        return None

    return _to_response(version, db)


@router.get("/id/{version_id}", response_model=VersionResponse)
async def get_version_by_id(
    version_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific version by its ID"""
    version = VersionService.get_version_by_id(db, version_id)

    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Version not found"
        )

    return _to_response(version, db)


@router.post("/{sys_id}/{page_num}", response_model=VersionResponse, status_code=status.HTTP_201_CREATED)
async def create_version(
    sys_id: str,
    page_num: int,
    data: VersionCreate,
    set_as_default: bool = Query(True, description="Set this as the new default version"),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a new user version for a page.

    Requires authentication. The version will be attributed to the current user.
    """
    # Override sys_id and page_num from path
    data.sys_id = sys_id
    data.page_num = page_num

    version, error = VersionService.create_user_version(
        db, data, current_user, set_as_default=set_as_default
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _to_response(version, db)


@router.post("/{sys_id}/{page_num}/set-default", response_model=SuccessResponse)
async def set_default_version(
    sys_id: str,
    page_num: int,
    request: SetDefaultRequest,
    current_user: User = Depends(require_editor),
    db: Session = Depends(get_db)
):
    """
    Set a specific version as the default for a page.

    Requires editor role or higher.
    """
    # Verify the version belongs to this page
    version = VersionService.get_version_by_id(db, request.version_id)
    if not version or version.sys_id != sys_id or version.page_num != page_num:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Version not found for this page"
        )

    success, error = VersionService.set_default_version(
        db, request.version_id, current_user
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="Default version updated"
    )


@router.post("/import", response_model=VersionResponse, status_code=status.HTTP_201_CREATED)
async def import_base_version(
    data: ImportVersionRequest,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Import a V0.7 or V0.8 base version.

    Admin only. Used for initial data import.
    """
    version, error = VersionService.import_base_version(db, data)

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return _to_response(version, db)


@router.get("/stats", response_model=VersionStats)
async def get_version_stats(
    db: Session = Depends(get_db)
):
    """Get statistics about transcription versions"""
    stats = VersionService.get_version_stats(db)
    return VersionStats(**stats)


@router.get("/unindexed", response_model=List[VersionResponse])
async def get_unindexed_versions(
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Get versions that haven't been indexed yet.

    Admin only. Used for Tantivy indexing.
    """
    versions = VersionService.get_unindexed_versions(db, limit)
    return [_to_response(v, db) for v in versions]


@router.post("/mark-indexed", response_model=SuccessResponse)
async def mark_versions_indexed(
    version_ids: List[int],
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Mark versions as indexed in Tantivy.

    Admin only. Called by the indexer after indexing.
    """
    VersionService.mark_as_indexed(db, version_ids)

    return SuccessResponse(
        success=True,
        message=f"Marked {len(version_ids)} versions as indexed"
    )
