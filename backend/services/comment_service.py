"""
Comment Service - Business Logic for Comments and Discussions
"""
from datetime import datetime
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from ..config import settings
from ..models.user import User
from ..models.comment import Comment, CommentType, CommentReaction, REACTION_TYPES
from ..models.document_metadata import DocumentMetadata
from ..models.activity_log import ActivityLog, ActivityType
from ..schemas.comment import CommentCreate, CommentUpdate, ReactionSummary
from .user_service import UserService


class CommentService:
    """Service for comment and discussion management"""

    @staticmethod
    def create_comment(
        db: Session,
        user: User,
        data: CommentCreate,
        ip_address: str = None
    ) -> Tuple[Optional[Comment], Optional[str]]:
        """
        Create a new comment.

        Args:
            db: Database session
            user: User creating the comment
            data: Comment data
            ip_address: Request IP for logging

        Returns:
            Tuple of (Comment or None, error message or None)
        """
        # Validate target
        if not data.document_id and not data.correction_id:
            return None, "Comment must be on a document or correction"

        # If reply, verify parent exists
        if data.parent_id:
            parent = db.query(Comment).filter(Comment.id == data.parent_id).first()
            if not parent:
                return None, "Parent comment not found"
            # Force reply type for replies
            comment_type = CommentType.REPLY
        else:
            comment_type = CommentType(data.comment_type.value)

        comment = Comment(
            document_id=data.document_id,
            correction_id=data.correction_id,
            parent_id=data.parent_id,
            author_id=user.id,
            content=data.content,
            comment_type=comment_type,
            line_number=data.line_number,
            char_start=data.char_start,
            char_end=data.char_end
        )

        db.add(comment)
        db.commit()
        db.refresh(comment)

        # Update parent reply count
        if data.parent_id:
            parent.reply_count = (parent.reply_count or 0) + 1
            db.commit()

        # Update document metadata
        if data.document_id:
            CommentService._update_document_metadata(db, data.document_id)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.COMMENT_CREATE,
            user_id=user.id,
            target_type='comment',
            target_id=str(comment.id),
            description=f"Created comment on {data.document_id or f'correction {data.correction_id}'}",
            ip_address=ip_address
        )

        return comment, None

    @staticmethod
    def get_comment(db: Session, comment_id: int) -> Optional[Comment]:
        """Get a comment by ID"""
        return db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

    @staticmethod
    def update_comment(
        db: Session,
        user: User,
        comment_id: int,
        data: CommentUpdate,
        ip_address: str = None
    ) -> Tuple[Optional[Comment], Optional[str]]:
        """
        Update a comment (only author can edit).

        Args:
            db: Database session
            user: User making the update
            comment_id: ID of comment to update
            data: Update data
            ip_address: Request IP for logging

        Returns:
            Tuple of (Updated Comment or None, error message or None)
        """
        comment = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not comment:
            return None, "Comment not found"

        # Only author or admin can edit
        if comment.author_id != user.id and not user.is_admin():
            return None, "You don't have permission to edit this comment"

        comment.content = data.content
        comment.mark_edited()
        db.commit()
        db.refresh(comment)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.COMMENT_UPDATE,
            user_id=user.id,
            target_type='comment',
            target_id=str(comment.id),
            ip_address=ip_address
        )

        return comment, None

    @staticmethod
    def delete_comment(
        db: Session,
        user: User,
        comment_id: int,
        ip_address: str = None
    ) -> Tuple[bool, str]:
        """
        Soft delete a comment.
        """
        comment = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not comment:
            return False, "Comment not found"

        # Only author or admin can delete
        if comment.author_id != user.id and not user.is_admin():
            return False, "You don't have permission to delete this comment"

        comment.soft_delete()
        db.commit()

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.COMMENT_DELETE,
            user_id=user.id,
            target_type='comment',
            target_id=str(comment_id),
            ip_address=ip_address
        )

        return True, ""

    @staticmethod
    def resolve_comment(
        db: Session,
        user: User,
        comment_id: int,
        ip_address: str = None
    ) -> Tuple[Optional[Comment], Optional[str]]:
        """Mark a question/issue comment as resolved"""
        comment = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not comment:
            return None, "Comment not found"

        if comment.comment_type not in [CommentType.QUESTION, CommentType.ISSUE]:
            return None, "Only questions and issues can be resolved"

        comment.resolve()
        db.commit()
        db.refresh(comment)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.COMMENT_RESOLVE,
            user_id=user.id,
            target_type='comment',
            target_id=str(comment.id),
            ip_address=ip_address
        )

        return comment, None

    @staticmethod
    def toggle_pin(
        db: Session,
        user: User,
        comment_id: int
    ) -> Tuple[Optional[Comment], Optional[str]]:
        """Toggle pin status (admin/editor only)"""
        if not user.can_edit_any_correction():
            return None, "Only editors and admins can pin comments"

        comment = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not comment:
            return None, "Comment not found"

        comment.is_pinned = not comment.is_pinned
        db.commit()
        db.refresh(comment)

        return comment, None

    @staticmethod
    def add_reaction(
        db: Session,
        user: User,
        comment_id: int,
        reaction_type: str,
        ip_address: str = None
    ) -> Tuple[Optional[Comment], Optional[str]]:
        """
        Add a reaction to a comment.
        If same reaction exists, removes it (toggle).
        """
        if reaction_type not in REACTION_TYPES:
            return None, f"Invalid reaction type. Must be one of: {REACTION_TYPES}"

        comment = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not comment:
            return None, "Comment not found"

        # Check if user already has this reaction
        existing = db.query(CommentReaction).filter(
            CommentReaction.comment_id == comment_id,
            CommentReaction.user_id == user.id,
            CommentReaction.reaction_type == reaction_type
        ).first()

        if existing:
            # Remove reaction (toggle off)
            db.delete(existing)
            comment.reaction_count = max(0, (comment.reaction_count or 0) - 1)
        else:
            # Add reaction
            reaction = CommentReaction(
                comment_id=comment_id,
                user_id=user.id,
                reaction_type=reaction_type
            )
            db.add(reaction)
            comment.reaction_count = (comment.reaction_count or 0) + 1

            # Award reputation for helpful reactions
            if reaction_type == 'helpful':
                author = db.query(User).filter(User.id == comment.author_id).first()
                if author and author.id != user.id:
                    UserService.add_reputation(
                        db, author,
                        settings.REPUTATION_HELPFUL_COMMENT,
                        "Received helpful reaction"
                    )

        db.commit()
        db.refresh(comment)

        # Log activity
        ActivityLog.log(
            db,
            ActivityType.COMMENT_REACT,
            user_id=user.id,
            target_type='comment',
            target_id=str(comment.id),
            extra_data={'reaction': reaction_type, 'action': 'remove' if existing else 'add'},
            ip_address=ip_address
        )

        return comment, None

    @staticmethod
    def get_reactions_summary(db: Session, comment_id: int) -> ReactionSummary:
        """Get summary of reactions on a comment"""
        reactions = db.query(
            CommentReaction.reaction_type,
            func.count(CommentReaction.id)
        ).filter(
            CommentReaction.comment_id == comment_id
        ).group_by(CommentReaction.reaction_type).all()

        summary = ReactionSummary()
        total = 0
        for reaction_type, count in reactions:
            setattr(summary, reaction_type, count)
            total += count
        summary.total = total

        return summary

    @staticmethod
    def get_user_reactions(
        db: Session,
        user_id: int,
        comment_id: int
    ) -> List[str]:
        """Get list of reaction types user has on a comment"""
        reactions = db.query(CommentReaction.reaction_type).filter(
            CommentReaction.comment_id == comment_id,
            CommentReaction.user_id == user_id
        ).all()
        return [r[0] for r in reactions]

    @staticmethod
    def get_comments_for_document(
        db: Session,
        document_id: str,
        include_replies: bool = True,
        page: int = 1,
        page_size: int = 50
    ) -> Tuple[List[Comment], int]:
        """
        Get comments for a document.

        Args:
            db: Database session
            document_id: Document ID
            include_replies: If True, includes nested replies
            page: Page number
            page_size: Items per page

        Returns:
            Tuple of (list of comments, total count)
        """
        q = db.query(Comment).filter(
            Comment.document_id == document_id,
            Comment.is_deleted == False
        )

        if not include_replies:
            # Only top-level comments
            q = q.filter(Comment.parent_id.is_(None))

        total = q.count()

        # Pinned first, then by date
        comments = q.order_by(
            Comment.is_pinned.desc(),
            Comment.created_at.desc()
        ).offset((page - 1) * page_size).limit(page_size).all()

        return comments, total

    @staticmethod
    def get_comments_for_correction(
        db: Session,
        correction_id: int,
        page: int = 1,
        page_size: int = 50
    ) -> Tuple[List[Comment], int]:
        """Get comments on a correction"""
        q = db.query(Comment).filter(
            Comment.correction_id == correction_id,
            Comment.is_deleted == False
        )

        total = q.count()
        comments = q.order_by(Comment.created_at.asc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return comments, total

    @staticmethod
    def get_comment_thread(
        db: Session,
        comment_id: int
    ) -> Tuple[Optional[Comment], List[Comment]]:
        """
        Get a comment and all its replies (flat list).
        """
        root = db.query(Comment).filter(
            Comment.id == comment_id,
            Comment.is_deleted == False
        ).first()

        if not root:
            return None, []

        replies = db.query(Comment).filter(
            Comment.parent_id == comment_id,
            Comment.is_deleted == False
        ).order_by(Comment.created_at.asc()).all()

        return root, replies

    @staticmethod
    def get_inline_comments(
        db: Session,
        document_id: str,
        line_start: int = None,
        line_end: int = None
    ) -> List[Comment]:
        """
        Get inline comments for a document, optionally within a line range.
        """
        q = db.query(Comment).filter(
            Comment.document_id == document_id,
            Comment.is_deleted == False,
            Comment.line_number.isnot(None)
        )

        if line_start is not None:
            q = q.filter(Comment.line_number >= line_start)
        if line_end is not None:
            q = q.filter(Comment.line_number <= line_end)

        return q.order_by(Comment.line_number, Comment.char_start).all()

    @staticmethod
    def _update_document_metadata(db: Session, document_id: str):
        """Update document metadata for comment count"""
        metadata = db.query(DocumentMetadata).filter(
            DocumentMetadata.document_id == document_id
        ).first()

        if not metadata:
            metadata = DocumentMetadata(document_id=document_id)
            db.add(metadata)

        metadata.increment_comments()
        db.commit()

    @staticmethod
    def search_comments(
        db: Session,
        query: str,
        document_id: str = None,
        author_id: int = None,
        comment_type: CommentType = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[Comment], int]:
        """Search comments with filters"""
        q = db.query(Comment).filter(
            Comment.is_deleted == False,
            Comment.content.ilike(f"%{query}%")
        )

        if document_id:
            q = q.filter(Comment.document_id == document_id)
        if author_id:
            q = q.filter(Comment.author_id == author_id)
        if comment_type:
            q = q.filter(Comment.comment_type == comment_type)

        total = q.count()
        comments = q.order_by(Comment.created_at.desc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return comments, total
