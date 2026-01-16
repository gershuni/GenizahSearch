"""
Version Service - Manage TranscriptionVersion records

Handles:
- Creating new versions from user corrections
- Importing V0.7/V0.8 versions
- Setting default versions
- Querying versions for pages
"""
from typing import Optional, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from ..models.transcription_version import TranscriptionVersion, PageDefault, VersionSource
from ..models.user import User
from ..models.correction import Correction, CorrectionStatus
from ..schemas.version import VersionCreate, ImportVersionRequest


class VersionService:
    """Service for managing transcription versions"""

    @staticmethod
    def get_page_versions(
        db: Session,
        sys_id: str,
        page_num: int = 1
    ) -> Tuple[Optional[TranscriptionVersion], List[TranscriptionVersion]]:
        """
        Get all versions for a specific page.

        Returns:
            Tuple of (current_default, all_versions)
        """
        # Get all versions for this page
        versions = db.query(TranscriptionVersion).filter(
            TranscriptionVersion.sys_id == sys_id,
            TranscriptionVersion.page_num == page_num
        ).order_by(TranscriptionVersion.created_at.desc()).all()

        # Find current default
        current_default = None
        for v in versions:
            if v.is_current_default:
                current_default = v
                break

        return current_default, versions

    @staticmethod
    def get_version_by_id(
        db: Session,
        version_id: int
    ) -> Optional[TranscriptionVersion]:
        """Get a specific version by ID"""
        return db.query(TranscriptionVersion).filter(
            TranscriptionVersion.id == version_id
        ).first()

    @staticmethod
    def get_default_version(
        db: Session,
        sys_id: str,
        page_num: int = 1
    ) -> Optional[TranscriptionVersion]:
        """Get the current default version for a page"""
        return db.query(TranscriptionVersion).filter(
            TranscriptionVersion.sys_id == sys_id,
            TranscriptionVersion.page_num == page_num,
            TranscriptionVersion.is_current_default == True
        ).first()

    @staticmethod
    def create_version_from_correction(
        db: Session,
        correction: Correction,
        user: User,
        set_as_default: bool = True
    ) -> Tuple[Optional[TranscriptionVersion], Optional[str]]:
        """
        Create a new version from an approved correction.

        Args:
            db: Database session
            correction: The approved correction
            user: User who submitted the correction
            set_as_default: Whether to set this as the new default

        Returns:
            Tuple of (version, error_message)
        """
        if correction.status != CorrectionStatus.APPROVED:
            return None, "Correction must be approved to create a version"

        # Determine sys_id and page_num with fallbacks
        sys_id = correction.system_id or correction.document_id
        page_num = correction.page_number or 1

        # Get the next version number for this page
        max_version = db.query(func.max(TranscriptionVersion.version_number)).filter(
            TranscriptionVersion.sys_id == sys_id,
            TranscriptionVersion.page_num == page_num
        ).scalar() or 0

        # Create the new version
        version = TranscriptionVersion(
            sys_id=sys_id,
            page_num=page_num,
            content=correction.corrected_text,
            source=VersionSource.USER,
            user_id=user.id,
            correction_id=correction.id,
            change_description=correction.notes,
            version_number=max_version + 1,
            is_current_default=False,
            quality_score=correction.quality_score or 0.8
        )

        db.add(version)
        db.flush()  # Get the ID

        if set_as_default:
            VersionService._set_default(db, sys_id, page_num, version.id)

        db.commit()
        db.refresh(version)

        return version, None

    @staticmethod
    def create_user_version(
        db: Session,
        data: VersionCreate,
        user: User,
        set_as_default: bool = True
    ) -> Tuple[Optional[TranscriptionVersion], Optional[str]]:
        """
        Create a new user version directly (without going through correction workflow).

        Args:
            db: Database session
            data: Version data
            user: User creating the version
            set_as_default: Whether to set this as the new default

        Returns:
            Tuple of (version, error_message)
        """
        # Get the next version number
        max_version = db.query(func.max(TranscriptionVersion.version_number)).filter(
            TranscriptionVersion.sys_id == data.sys_id,
            TranscriptionVersion.page_num == data.page_num
        ).scalar() or 0

        version = TranscriptionVersion(
            sys_id=data.sys_id,
            page_num=data.page_num,
            content=data.content,
            source=VersionSource.USER,
            user_id=user.id,
            correction_id=data.correction_id,
            change_description=data.change_description,
            version_number=max_version + 1,
            is_current_default=False,
            quality_score=0.8
        )

        db.add(version)
        db.flush()

        if set_as_default:
            VersionService._set_default(db, data.sys_id, data.page_num, version.id)

        db.commit()
        db.refresh(version)

        return version, None

    @staticmethod
    def import_base_version(
        db: Session,
        data: ImportVersionRequest
    ) -> Tuple[Optional[TranscriptionVersion], Optional[str]]:
        """
        Import a V0.7 or V0.8 base version.

        Args:
            db: Database session
            data: Import data including sys_id, page_num, content, source

        Returns:
            Tuple of (version, error_message)
        """
        if data.source not in [VersionSource.V07, VersionSource.V08, VersionSource.IMPORTED]:
            return None, "Import source must be V0.7, V0.8, or imported"

        # Check if this version already exists
        existing = db.query(TranscriptionVersion).filter(
            TranscriptionVersion.sys_id == data.sys_id,
            TranscriptionVersion.page_num == data.page_num,
            TranscriptionVersion.source == data.source
        ).first()

        if existing:
            # Update existing
            existing.content = data.content
            db.commit()
            db.refresh(existing)
            return existing, None

        # Create new
        version = TranscriptionVersion(
            sys_id=data.sys_id,
            page_num=data.page_num,
            content=data.content,
            source=data.source,
            user_id=None,  # No user for base versions
            version_number=1 if data.source == VersionSource.V07 else 2,
            is_current_default=data.source == VersionSource.V08,  # V0.8 is default if present
            quality_score=1.0
        )

        db.add(version)
        db.flush()

        # Set as default if it's V0.8 or if there's no default yet
        if data.source == VersionSource.V08:
            VersionService._set_default(db, data.sys_id, data.page_num, version.id)
        elif not VersionService.get_default_version(db, data.sys_id, data.page_num):
            VersionService._set_default(db, data.sys_id, data.page_num, version.id)

        db.commit()
        db.refresh(version)

        return version, None

    @staticmethod
    def set_default_version(
        db: Session,
        version_id: int,
        user: User
    ) -> Tuple[bool, Optional[str]]:
        """
        Set a specific version as the default for its page.

        Args:
            db: Database session
            version_id: ID of the version to set as default
            user: User making the change (for audit)

        Returns:
            Tuple of (success, error_message)
        """
        version = db.query(TranscriptionVersion).filter(
            TranscriptionVersion.id == version_id
        ).first()

        if not version:
            return False, "Version not found"

        VersionService._set_default(db, version.sys_id, version.page_num, version_id)
        db.commit()

        return True, None

    @staticmethod
    def _set_default(
        db: Session,
        sys_id: str,
        page_num: int,
        version_id: int
    ):
        """Internal: Set a version as default, clearing others"""
        # Clear current default
        db.query(TranscriptionVersion).filter(
            TranscriptionVersion.sys_id == sys_id,
            TranscriptionVersion.page_num == page_num,
            TranscriptionVersion.is_current_default == True
        ).update({'is_current_default': False})

        # Set new default
        db.query(TranscriptionVersion).filter(
            TranscriptionVersion.id == version_id
        ).update({'is_current_default': True})

        # Update PageDefault lookup table
        page_default = db.query(PageDefault).filter(
            PageDefault.sys_id == sys_id,
            PageDefault.page_num == page_num
        ).first()

        if page_default:
            page_default.default_version_id = version_id
        else:
            page_default = PageDefault(
                sys_id=sys_id,
                page_num=page_num,
                default_version_id=version_id
            )
            db.add(page_default)

    @staticmethod
    def get_unindexed_versions(
        db: Session,
        limit: int = 100
    ) -> List[TranscriptionVersion]:
        """Get versions that haven't been indexed yet"""
        return db.query(TranscriptionVersion).filter(
            TranscriptionVersion.is_indexed == False
        ).limit(limit).all()

    @staticmethod
    def mark_as_indexed(
        db: Session,
        version_ids: List[int]
    ):
        """Mark versions as indexed"""
        db.query(TranscriptionVersion).filter(
            TranscriptionVersion.id.in_(version_ids)
        ).update({
            'is_indexed': True,
            'index_timestamp': datetime.utcnow()
        }, synchronize_session=False)
        db.commit()

    @staticmethod
    def get_version_stats(db: Session) -> dict:
        """Get statistics about versions"""
        total = db.query(func.count(TranscriptionVersion.id)).scalar()

        by_source = {}
        for source in VersionSource:
            count = db.query(func.count(TranscriptionVersion.id)).filter(
                TranscriptionVersion.source == source
            ).scalar()
            by_source[source.value] = count

        user_count = db.query(func.count(TranscriptionVersion.id)).filter(
            TranscriptionVersion.user_id.isnot(None)
        ).scalar()

        indexed = db.query(func.count(TranscriptionVersion.id)).filter(
            TranscriptionVersion.is_indexed == True
        ).scalar()

        return {
            'total_versions': total,
            'by_source': by_source,
            'user_versions_count': user_count,
            'indexed_count': indexed,
            'unindexed_count': total - indexed
        }
