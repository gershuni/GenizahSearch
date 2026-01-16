"""
Correction Service - Core Business Logic for Corrections
"""
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from ..config import settings
from ..models.user import User, UserRole
from ..models.correction import Correction, CorrectionStatus, CorrectionType, CorrectionVote
from ..models.document_metadata import DocumentMetadata
from ..models.activity_log import ActivityLog, ActivityType
from ..schemas.correction import (
    CorrectionCreate, CorrectionUpdate, CorrectionReview,
    CorrectionSearchParams, CorrectionStats
)
from .user_service import UserService


class CorrectionService:
    """Service for correction management operations"""

    @staticmethod
    def create_correction(
        db: Session,
        user: User,
        data: CorrectionCreate,
        ip_address: str = None
    ) -> Tuple[Optional[Correction], Optional[str]]:
        """
        Create a new correction (starts as draft).

        Args:
            db: Database session
            user: User creating the correction
            data: Correction data
            ip_address: Request IP for logging

        Returns:
            Tuple of (Correction or None, error message or None)
        """
        if not user.can_submit_corrections():
            return None, "You don't have permission to submit corrections"

        # Create correction
        correction = Correction(
            document_id=data.document_id,
            shelfmark=data.shelfmark,
            system_id=data.system_id,
            author_id=user.id,
            original_text=data.original_text,
            corrected_text=data.corrected_text,
            line_number=data.line_number,
            char_start=data.char_start,
            char_end=data.char_end,
            page_number=data.page_number,
            context_before=data.context_before,
            context_after=data.context_after,
            correction_type=CorrectionType(data.correction_type.value),
            status=CorrectionStatus.DRAFT,
            confidence_score=data.confidence_score,
            source_reference=data.source_reference,
            notes=data.notes,
            tags=data.tags
        )

        db.add(correction)
        db.commit()
        db.refresh(correction)

        # Update user stats
        user.increment_contributions()
        db.commit()

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.CORRECTION_CREATE,
            user_id=user.id,
            target_type='correction',
            target_id=str(correction.id),
            description=f"Created correction for document {data.document_id}",
            ip_address=ip_address
        )

        return correction, None

    @staticmethod
    def get_correction(db: Session, correction_id: int) -> Optional[Correction]:
        """Get a correction by ID"""
        return db.query(Correction).filter(Correction.id == correction_id).first()

    @staticmethod
    def update_correction(
        db: Session,
        user: User,
        correction_id: int,
        data: CorrectionUpdate,
        ip_address: str = None
    ) -> Tuple[Optional[Correction], Optional[str]]:
        """
        Update a correction (only drafts or needs_revision).

        Args:
            db: Database session
            user: User making the update
            correction_id: ID of correction to update
            data: Update data
            ip_address: Request IP for logging

        Returns:
            Tuple of (Updated Correction or None, error message or None)
        """
        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            return None, "Correction not found"

        # Check permissions
        if correction.author_id != user.id and not user.can_edit_any_correction():
            return None, "You don't have permission to edit this correction"

        # Check status
        if correction.status not in [CorrectionStatus.DRAFT, CorrectionStatus.NEEDS_REVISION]:
            return None, f"Cannot edit correction in {correction.status.value} status"

        # Update fields
        if data.original_text is not None:
            correction.original_text = data.original_text
        if data.corrected_text is not None:
            correction.corrected_text = data.corrected_text
        if data.correction_type is not None:
            correction.correction_type = CorrectionType(data.correction_type.value)
        if data.line_number is not None:
            correction.line_number = data.line_number
        if data.char_start is not None:
            correction.char_start = data.char_start
        if data.char_end is not None:
            correction.char_end = data.char_end
        if data.context_before is not None:
            correction.context_before = data.context_before
        if data.context_after is not None:
            correction.context_after = data.context_after
        if data.confidence_score is not None:
            correction.confidence_score = data.confidence_score
        if data.source_reference is not None:
            correction.source_reference = data.source_reference
        if data.notes is not None:
            correction.notes = data.notes
        if data.tags is not None:
            correction.tags = data.tags

        correction.updated_at = datetime.utcnow()
        correction.revision_number += 1
        db.commit()
        db.refresh(correction)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.CORRECTION_UPDATE,
            user_id=user.id,
            target_type='correction',
            target_id=str(correction.id),
            ip_address=ip_address
        )

        return correction, None

    @staticmethod
    def submit_correction(
        db: Session,
        user: User,
        correction_id: int,
        notes: str = None,
        ip_address: str = None
    ) -> Tuple[Optional[Correction], Optional[str]]:
        """
        Submit a draft correction for review.

        Args:
            db: Database session
            user: User submitting
            correction_id: ID of correction to submit
            notes: Optional submission notes
            ip_address: Request IP for logging

        Returns:
            Tuple of (Submitted Correction or None, error message or None)
        """
        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            return None, "Correction not found"

        if correction.author_id != user.id:
            return None, "Only the author can submit this correction"

        if correction.status not in [CorrectionStatus.DRAFT, CorrectionStatus.NEEDS_REVISION]:
            return None, f"Cannot submit correction in {correction.status.value} status"

        # Auto-approve for editors and admins
        if user.role in [UserRole.EDITOR, UserRole.ADMIN]:
            correction.status = CorrectionStatus.APPROVED
            correction.submitted_at = datetime.utcnow()
            correction.reviewed_at = datetime.utcnow()
            correction.applied_at = datetime.utcnow()
            correction.reviewer_id = user.id
            correction.review_notes = "Auto-approved (Editor/Admin)"
            if notes:
                correction.notes = notes
            db.commit()

            # Update document metadata
            CorrectionService._update_document_metadata(db, correction.document_id, approved_delta=1)

            # Create a TranscriptionVersion from this approved correction
            # This allows the correction to show up in the version selector
            from .version_service import VersionService
            VersionService.create_version_from_correction(
                db, correction, user, set_as_default=True
            )

            # Award reputation to self
            UserService.add_reputation(
                db, user,
                settings.REPUTATION_CORRECTION_APPROVED,
                reason="Correction auto-approved"
            )

            # Log activity
            ActivityLog.log(
                db,
                ActivityType.CORRECTION_APPROVE,
                user_id=user.id,
                target_type='correction',
                target_id=str(correction.id),
                description=f"Correction auto-approved (Editor/Admin)",
                ip_address=ip_address
            )
        else:
            # Submit for review (regular contributors)
            correction.status = CorrectionStatus.PENDING
            correction.submitted_at = datetime.utcnow()
            if notes:
                correction.notes = notes
            db.commit()

            # Update document metadata
            CorrectionService._update_document_metadata(db, correction.document_id, pending_delta=1)

            # Log activity
            ActivityLog.log(
                db,
                ActivityType.CORRECTION_SUBMIT,
                user_id=user.id,
                target_type='correction',
                target_id=str(correction.id),
                description=f"Submitted correction for review",
                ip_address=ip_address
            )

        return correction, None

    @staticmethod
    def review_correction(
        db: Session,
        reviewer: User,
        correction_id: int,
        review: CorrectionReview,
        ip_address: str = None
    ) -> Tuple[Optional[Correction], Optional[str]]:
        """
        Review a correction (approve, reject, or request revision).

        Args:
            db: Database session
            reviewer: User reviewing
            correction_id: ID of correction to review
            review: Review action and notes
            ip_address: Request IP for logging

        Returns:
            Tuple of (Reviewed Correction or None, error message or None)
        """
        if not reviewer.can_review_corrections():
            return None, "You don't have permission to review corrections"

        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            return None, "Correction not found"

        if correction.status not in [CorrectionStatus.PENDING, CorrectionStatus.UNDER_REVIEW]:
            return None, f"Cannot review correction in {correction.status.value} status"

        # Can't review own corrections (unless editor/admin)
        if correction.author_id == reviewer.id and not reviewer.can_edit_any_correction():
            return None, "Cannot review your own corrections"

        author = db.query(User).filter(User.id == correction.author_id).first()

        if review.action == "approve":
            correction.approve(reviewer.id, review.review_notes)

            # Award reputation
            if author:
                UserService.add_reputation(
                    db, author,
                    settings.REPUTATION_CORRECTION_APPROVED,
                    "Correction approved"
                )
                author.increment_approved()

            # Update document metadata
            CorrectionService._update_document_metadata(
                db, correction.document_id,
                pending_delta=-1, approved_delta=1
            )

            # Create a TranscriptionVersion from this approved correction
            # This allows the correction to show up in the version selector
            from .version_service import VersionService
            VersionService.create_version_from_correction(
                db, correction, reviewer, set_as_default=True
            )

            activity_type = ActivityType.CORRECTION_APPROVE

        elif review.action == "reject":
            if not review.rejection_reason:
                return None, "Rejection reason is required"

            correction.reject(reviewer.id, review.rejection_reason)

            # Deduct reputation
            if author:
                UserService.add_reputation(
                    db, author,
                    settings.REPUTATION_CORRECTION_REJECTED,
                    "Correction rejected"
                )

            # Update document metadata
            CorrectionService._update_document_metadata(
                db, correction.document_id,
                pending_delta=-1
            )

            activity_type = ActivityType.CORRECTION_REJECT

        elif review.action == "request_revision":
            if not review.review_notes:
                return None, "Review notes are required when requesting revision"

            correction.request_revision(reviewer.id, review.review_notes)
            activity_type = ActivityType.CORRECTION_REQUEST_REVISION

        else:
            return None, f"Invalid review action: {review.action}"

        db.commit()
        db.refresh(correction)

        # Log activity
        ActivityLog.log(
            db,
            activity_type,
            user_id=reviewer.id,
            target_type='correction',
            target_id=str(correction.id),
            description=f"Reviewed correction: {review.action}",
            ip_address=ip_address
        )

        return correction, None

    @staticmethod
    def vote_on_correction(
        db: Session,
        user: User,
        correction_id: int,
        vote_value: int,
        ip_address: str = None
    ) -> Tuple[Optional[Correction], Optional[str]]:
        """
        Vote on a correction (upvote or downvote).

        Args:
            db: Database session
            user: User voting
            correction_id: ID of correction to vote on
            vote_value: +1 for upvote, -1 for downvote
            ip_address: Request IP for logging

        Returns:
            Tuple of (Updated Correction or None, error message or None)
        """
        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            return None, "Correction not found"

        # Can't vote on own corrections
        if correction.author_id == user.id:
            return None, "Cannot vote on your own corrections"

        # Check if user already voted
        existing_vote = db.query(CorrectionVote).filter(
            CorrectionVote.correction_id == correction_id,
            CorrectionVote.user_id == user.id
        ).first()

        author = db.query(User).filter(User.id == correction.author_id).first()

        if existing_vote:
            if existing_vote.vote_value == vote_value:
                # Remove vote
                if vote_value == 1:
                    correction.upvotes = max(0, (correction.upvotes or 0) - 1)
                    if author:
                        author.add_reputation(-settings.REPUTATION_UPVOTE_RECEIVED)
                else:
                    correction.downvotes = max(0, (correction.downvotes or 0) - 1)
                    if author:
                        author.add_reputation(-settings.REPUTATION_DOWNVOTE_RECEIVED)
                db.delete(existing_vote)
            else:
                # Change vote
                old_value = existing_vote.vote_value
                existing_vote.vote_value = vote_value

                if old_value == 1:
                    correction.upvotes = max(0, (correction.upvotes or 0) - 1)
                    correction.downvotes = (correction.downvotes or 0) + 1
                    if author:
                        author.add_reputation(
                            -settings.REPUTATION_UPVOTE_RECEIVED +
                            settings.REPUTATION_DOWNVOTE_RECEIVED
                        )
                else:
                    correction.downvotes = max(0, (correction.downvotes or 0) - 1)
                    correction.upvotes = (correction.upvotes or 0) + 1
                    if author:
                        author.add_reputation(
                            -settings.REPUTATION_DOWNVOTE_RECEIVED +
                            settings.REPUTATION_UPVOTE_RECEIVED
                        )
        else:
            # New vote
            new_vote = CorrectionVote(
                correction_id=correction_id,
                user_id=user.id,
                vote_value=vote_value
            )
            db.add(new_vote)

            if vote_value == 1:
                correction.upvotes = (correction.upvotes or 0) + 1
                if author:
                    author.add_reputation(settings.REPUTATION_UPVOTE_RECEIVED)
            else:
                correction.downvotes = (correction.downvotes or 0) + 1
                if author:
                    author.add_reputation(settings.REPUTATION_DOWNVOTE_RECEIVED)

        correction.calculate_quality_score()
        db.commit()
        db.refresh(correction)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.CORRECTION_VOTE,
            user_id=user.id,
            target_type='correction',
            target_id=str(correction.id),
            extra_data={'vote': vote_value},
            ip_address=ip_address
        )

        return correction, None

    @staticmethod
    def get_user_vote(
        db: Session,
        user_id: int,
        correction_id: int
    ) -> Optional[int]:
        """Get user's vote on a correction"""
        vote = db.query(CorrectionVote).filter(
            CorrectionVote.correction_id == correction_id,
            CorrectionVote.user_id == user_id
        ).first()
        return vote.vote_value if vote else None

    @staticmethod
    def delete_correction(
        db: Session,
        user: User,
        correction_id: int,
        ip_address: str = None
    ) -> Tuple[bool, str]:
        """
        Delete a correction (only drafts can be deleted).
        """
        correction = db.query(Correction).filter(Correction.id == correction_id).first()

        if not correction:
            return False, "Correction not found"

        # Only author or admin can delete
        if correction.author_id != user.id and not user.is_admin():
            return False, "You don't have permission to delete this correction"

        # Only drafts can be deleted
        if correction.status != CorrectionStatus.DRAFT and not user.is_admin():
            return False, "Only draft corrections can be deleted"

        db.delete(correction)
        db.commit()

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.CORRECTION_DELETE,
            user_id=user.id,
            target_type='correction',
            target_id=str(correction_id),
            ip_address=ip_address
        )

        return True, ""

    @staticmethod
    def search_corrections(
        db: Session,
        params: CorrectionSearchParams,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """
        Search corrections with various filters.

        Returns:
            Tuple of (list of corrections, total count)
        """
        q = db.query(Correction)

        # Apply filters
        if params.document_id:
            q = q.filter(Correction.document_id == params.document_id)

        if params.shelfmark:
            q = q.filter(Correction.shelfmark.ilike(f"%{params.shelfmark}%"))

        if params.system_id:
            q = q.filter(Correction.system_id == params.system_id)

        if params.author_id:
            q = q.filter(Correction.author_id == params.author_id)

        if params.reviewer_id:
            q = q.filter(Correction.reviewer_id == params.reviewer_id)

        if params.status:
            q = q.filter(Correction.status == CorrectionStatus(params.status.value))

        if params.statuses:
            status_values = [CorrectionStatus(s.value) for s in params.statuses]
            q = q.filter(Correction.status.in_(status_values))

        if params.correction_type:
            q = q.filter(Correction.correction_type == CorrectionType(params.correction_type.value))

        if params.min_quality_score is not None:
            q = q.filter(Correction.quality_score >= params.min_quality_score)

        if params.has_source is not None:
            if params.has_source:
                q = q.filter(Correction.source_reference.isnot(None))
            else:
                q = q.filter(Correction.source_reference.is_(None))

        if params.search_text:
            q = q.filter(or_(
                Correction.original_text.ilike(f"%{params.search_text}%"),
                Correction.corrected_text.ilike(f"%{params.search_text}%"),
                Correction.notes.ilike(f"%{params.search_text}%")
            ))

        if params.created_after:
            q = q.filter(Correction.created_at >= params.created_after)

        if params.created_before:
            q = q.filter(Correction.created_at <= params.created_before)

        total = q.count()

        corrections = q.order_by(Correction.created_at.desc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return corrections, total

    @staticmethod
    def get_corrections_for_document(
        db: Session,
        document_id: str,
        include_drafts: bool = False,
        user_id: int = None
    ) -> List[Correction]:
        """
        Get all corrections for a document.
        Optionally include drafts (only user's own).
        """
        q = db.query(Correction).filter(Correction.document_id == document_id)

        if include_drafts and user_id:
            # Include approved + user's drafts
            q = q.filter(or_(
                Correction.status == CorrectionStatus.APPROVED,
                and_(
                    Correction.author_id == user_id,
                    Correction.status == CorrectionStatus.DRAFT
                )
            ))
        else:
            # Only approved
            q = q.filter(Correction.status == CorrectionStatus.APPROVED)

        return q.order_by(Correction.line_number, Correction.char_start).all()

    @staticmethod
    def get_pending_corrections(
        db: Session,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Correction], int]:
        """Get corrections pending review"""
        q = db.query(Correction).filter(
            Correction.status == CorrectionStatus.PENDING
        )

        total = q.count()
        corrections = q.order_by(Correction.submitted_at.asc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return corrections, total

    @staticmethod
    def get_correction_stats(db: Session) -> CorrectionStats:
        """Get overall correction statistics"""
        total = db.query(func.count(Correction.id)).scalar() or 0

        by_status = {}
        for status in CorrectionStatus:
            count = db.query(func.count(Correction.id)).filter(
                Correction.status == status
            ).scalar() or 0
            by_status[status.value] = count

        by_type = {}
        for ctype in CorrectionType:
            count = db.query(func.count(Correction.id)).filter(
                Correction.correction_type == ctype
            ).scalar() or 0
            by_type[ctype.value] = count

        # Recent activity (last 7 days)
        week_ago = datetime.utcnow() - timedelta(days=7)
        recent = db.query(func.count(Correction.id)).filter(
            Correction.created_at >= week_ago
        ).scalar() or 0

        return CorrectionStats(
            total=total,
            draft=by_status.get('draft', 0),
            pending=by_status.get('pending', 0),
            under_review=by_status.get('under_review', 0),
            approved=by_status.get('approved', 0),
            rejected=by_status.get('rejected', 0),
            needs_revision=by_status.get('needs_revision', 0),
            by_type=by_type,
            recent_activity=recent
        )

    @staticmethod
    def _update_document_metadata(
        db: Session,
        document_id: str,
        pending_delta: int = 0,
        approved_delta: int = 0
    ):
        """Update document metadata counters"""
        metadata = db.query(DocumentMetadata).filter(
            DocumentMetadata.document_id == document_id
        ).first()

        if not metadata:
            metadata = DocumentMetadata(document_id=document_id)
            db.add(metadata)

        if pending_delta != 0:
            if pending_delta > 0:
                metadata.increment_pending()
            else:
                metadata.decrement_pending()

        if approved_delta != 0:
            metadata.increment_approved()
            metadata.last_correction_at = datetime.utcnow()

        metadata.increment_corrections()
        db.commit()

    @staticmethod
    def get_corrected_text_for_document(
        db: Session,
        document_id: str,
        original_text: str
    ) -> str:
        """
        Apply all approved corrections to a document's text.
        Returns the corrected text.
        """
        corrections = db.query(Correction).filter(
            Correction.document_id == document_id,
            Correction.status == CorrectionStatus.APPROVED
        ).order_by(Correction.char_start.desc()).all()  # Reverse order to preserve positions

        result = original_text

        for correction in corrections:
            if correction.char_start is not None and correction.char_end is not None:
                # Position-based replacement
                result = (
                    result[:correction.char_start] +
                    correction.corrected_text +
                    result[correction.char_end:]
                )
            else:
                # Text-based replacement
                result = result.replace(
                    correction.original_text,
                    correction.corrected_text,
                    1  # Only first occurrence
                )

        return result
