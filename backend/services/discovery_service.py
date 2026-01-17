"""
Discovery Service - Business logic for discoveries and activity feed
"""
from typing import Optional, List, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_, desc

from ..models.discovery import Discovery, DiscoveryResponse, DiscoveryType, DiscoveryStatus
from ..models.correction import Correction, CorrectionStatus
from ..models.comment import Comment
from ..models.user import User
from ..schemas.discovery import (
    DiscoveryCreate, DiscoveryUpdate, ResponseCreate,
    AuthorInfo, DiscoveryStats, FeedItem
)


class DiscoveryService:
    """Service for managing discoveries and activity feed"""

    # ============================================
    # Discovery CRUD
    # ============================================

    @staticmethod
    def create_discovery(
        db: Session,
        data: DiscoveryCreate,
        user: User
    ) -> Tuple[Optional[Discovery], Optional[str]]:
        """Create a new discovery"""
        discovery = Discovery(
            user_id=user.id,
            title=data.title,
            content=data.content,
            discovery_type=data.discovery_type,
            document_id=data.document_id,
            page_number=data.page_number,
            shelfmark=data.shelfmark,
            correction_id=data.correction_id,
            is_anonymous=data.is_anonymous,
            status=DiscoveryStatus.PUBLISHED
        )

        db.add(discovery)
        db.commit()
        db.refresh(discovery)

        return discovery, None

    @staticmethod
    def get_discovery(
        db: Session,
        discovery_id: int,
        increment_views: bool = True
    ) -> Optional[Discovery]:
        """Get a single discovery by ID"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id,
            Discovery.status != DiscoveryStatus.HIDDEN
        ).first()

        if discovery and increment_views:
            discovery.view_count += 1
            db.commit()

        return discovery

    @staticmethod
    def update_discovery(
        db: Session,
        discovery_id: int,
        data: DiscoveryUpdate,
        user: User
    ) -> Tuple[Optional[Discovery], Optional[str]]:
        """Update an existing discovery"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return None, "Discovery not found"

        # Check ownership (unless admin)
        if discovery.user_id != user.id and not user.is_admin():
            return None, "Not authorized to update this discovery"

        # Update fields
        if data.title is not None:
            discovery.title = data.title
        if data.content is not None:
            discovery.content = data.content
        if data.discovery_type is not None:
            discovery.discovery_type = data.discovery_type
        if data.document_id is not None:
            discovery.document_id = data.document_id
        if data.page_number is not None:
            discovery.page_number = data.page_number
        if data.shelfmark is not None:
            discovery.shelfmark = data.shelfmark
        if data.is_anonymous is not None:
            discovery.is_anonymous = data.is_anonymous

        db.commit()
        db.refresh(discovery)

        return discovery, None

    @staticmethod
    def delete_discovery(
        db: Session,
        discovery_id: int,
        user: User
    ) -> Tuple[bool, Optional[str]]:
        """Delete (hide) a discovery"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found"

        # Check ownership (unless admin)
        if discovery.user_id != user.id and not user.is_admin():
            return False, "Not authorized to delete this discovery"

        discovery.status = DiscoveryStatus.HIDDEN
        db.commit()

        return True, None

    @staticmethod
    def list_discoveries(
        db: Session,
        discovery_type: Optional[DiscoveryType] = None,
        status: DiscoveryStatus = DiscoveryStatus.PUBLISHED,
        featured_only: bool = False,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[Discovery], int]:
        """List discoveries with filters"""
        query = db.query(Discovery).filter(
            Discovery.status == status
        )

        if discovery_type:
            query = query.filter(Discovery.discovery_type == discovery_type)

        if featured_only:
            query = query.filter(Discovery.is_featured == True)

        total = query.count()

        discoveries = query.order_by(
            desc(Discovery.is_featured),
            desc(Discovery.created_at)
        ).offset(offset).limit(limit).all()

        return discoveries, total

    # ============================================
    # Responses (comments on discoveries)
    # ============================================

    @staticmethod
    def add_response(
        db: Session,
        discovery_id: int,
        data: ResponseCreate,
        user: User
    ) -> Tuple[Optional[DiscoveryResponse], Optional[str]]:
        """Add a response to a discovery"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id,
            Discovery.status != DiscoveryStatus.HIDDEN
        ).first()

        if not discovery:
            return None, "Discovery not found"

        response = DiscoveryResponse(
            discovery_id=discovery_id,
            user_id=user.id,
            content=data.content,
            is_anonymous=data.is_anonymous
        )

        db.add(response)

        # Update response count
        discovery.response_count += 1

        db.commit()
        db.refresh(response)

        return response, None

    @staticmethod
    def get_responses(
        db: Session,
        discovery_id: int
    ) -> List[DiscoveryResponse]:
        """Get all responses for a discovery"""
        return db.query(DiscoveryResponse).filter(
            DiscoveryResponse.discovery_id == discovery_id,
            DiscoveryResponse.is_deleted == False
        ).order_by(DiscoveryResponse.created_at).all()

    # ============================================
    # Statistics
    # ============================================

    @staticmethod
    def get_stats(db: Session) -> DiscoveryStats:
        """Get overall statistics for the discoveries page"""
        # Discoveries
        total_discoveries = db.query(Discovery).filter(
            Discovery.status == DiscoveryStatus.PUBLISHED
        ).count()

        total_questions = db.query(Discovery).filter(
            Discovery.status == DiscoveryStatus.PUBLISHED,
            Discovery.discovery_type == DiscoveryType.QUESTION
        ).count()

        # Count open questions (no responses or not resolved)
        open_questions = db.query(Discovery).filter(
            Discovery.status == DiscoveryStatus.PUBLISHED,
            Discovery.discovery_type == DiscoveryType.QUESTION,
            Discovery.response_count == 0
        ).count()

        # Corrections
        total_corrections = db.query(Correction).count()
        approved_corrections = db.query(Correction).filter(
            Correction.status == CorrectionStatus.APPROVED
        ).count()

        # Estimate words corrected (rough - based on average correction size)
        # This would need actual calculation in production
        words_corrected = approved_corrections * 3  # Rough estimate

        # Documents edited
        documents_edited = db.query(func.count(func.distinct(Correction.document_id))).filter(
            Correction.status == CorrectionStatus.APPROVED
        ).scalar() or 0

        # Comments
        total_comments = db.query(Comment).filter(
            Comment.is_deleted == False
        ).count()

        # Active contributors (users with at least one contribution in last 30 days)
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        active_contributors = db.query(func.count(func.distinct(Correction.author_id))).filter(
            Correction.created_at >= thirty_days_ago
        ).scalar() or 0

        return DiscoveryStats(
            total_discoveries=total_discoveries,
            total_questions=total_questions,
            open_questions=open_questions,
            total_corrections=total_corrections,
            approved_corrections=approved_corrections,
            words_corrected=words_corrected,
            documents_edited=documents_edited,
            total_comments=total_comments,
            active_contributors=active_contributors
        )

    # ============================================
    # Activity Feed
    # ============================================

    @staticmethod
    def get_feed(
        db: Session,
        item_type: Optional[str] = None,  # "all", "discovery", "question", "correction", "comment"
        period: Optional[str] = None,  # "day", "week", "month", "all"
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[FeedItem], int]:
        """Get activity feed combining discoveries, corrections, comments, etc."""
        feed_items = []

        # Calculate date filter
        date_filter = None
        if period == "day":
            date_filter = datetime.utcnow() - timedelta(days=1)
        elif period == "week":
            date_filter = datetime.utcnow() - timedelta(weeks=1)
        elif period == "month":
            date_filter = datetime.utcnow() - timedelta(days=30)

        # Get discoveries
        if item_type in (None, "all", "discovery", "question"):
            disc_query = db.query(Discovery).filter(
                Discovery.status == DiscoveryStatus.PUBLISHED
            )

            if item_type == "question":
                disc_query = disc_query.filter(
                    Discovery.discovery_type == DiscoveryType.QUESTION
                )

            if date_filter:
                disc_query = disc_query.filter(Discovery.created_at >= date_filter)

            for d in disc_query.all():
                feed_items.append(FeedItem(
                    id=f"discovery_{d.id}",
                    item_type="discovery" if d.discovery_type != DiscoveryType.QUESTION else "question",
                    title=d.title,
                    content_preview=d.content,  # Full content, no truncation
                    author=AuthorInfo.from_user(d.user, d.is_anonymous),
                    document_id=d.document_id,
                    shelfmark=d.shelfmark,
                    page_number=d.page_number,
                    created_at=d.created_at,
                    response_count=d.response_count,
                    is_featured=d.is_featured,
                    is_pinned=getattr(d, 'is_pinned', False),
                    is_answered=getattr(d, 'is_answered', False),
                    upvotes=getattr(d, 'upvotes', 0) or 0,
                    downvotes=getattr(d, 'downvotes', 0) or 0,
                    discovery_type=d.discovery_type
                ))

        # Get approved corrections
        if item_type in (None, "all", "correction"):
            corr_query = db.query(Correction).filter(
                Correction.status == CorrectionStatus.APPROVED
            )

            if date_filter:
                corr_query = corr_query.filter(Correction.applied_at >= date_filter)

            for c in corr_query.all():
                # Create a title from the correction
                title = f"תיקון ב-{c.shelfmark or c.document_id}"

                feed_items.append(FeedItem(
                    id=f"correction_{c.id}",
                    item_type="correction",
                    title=title,
                    content_preview=c.corrected_text or '',  # Just the corrected text
                    author=AuthorInfo.from_user(c.author, getattr(c, 'is_anonymous', False)),
                    document_id=c.document_id,
                    shelfmark=c.shelfmark,
                    page_number=c.page_number,
                    created_at=c.applied_at or c.created_at,
                    response_count=len(c.comments) if c.comments else 0,
                    is_featured=False,
                    correction_status=c.status.value,
                    original_text=c.original_text,
                    corrected_text=c.corrected_text
                ))

        # Get public comments (document-level, not on corrections)
        if item_type in (None, "all", "comment"):
            comment_query = db.query(Comment).filter(
                Comment.is_deleted == False,
                Comment.is_public == True,
                Comment.document_id.isnot(None),  # Only document comments
                Comment.parent_id.is_(None)  # Only top-level comments
            )

            if date_filter:
                comment_query = comment_query.filter(Comment.created_at >= date_filter)

            for c in comment_query.all():
                # Create a title from the comment
                comment_type_labels = {
                    'general': 'הערה',
                    'question': 'שאלה',
                    'scholarly_note': 'הערה מדעית',
                    'suggestion': 'הצעה',
                    'issue': 'בעיה'
                }
                type_label = comment_type_labels.get(c.comment_type.value, 'הערה')
                title = f"{type_label} על {c.document_id}"

                feed_items.append(FeedItem(
                    id=f"comment_{c.id}",
                    item_type="comment",
                    title=title,
                    content_preview=c.content[:200] + "..." if len(c.content) > 200 else c.content,
                    author=AuthorInfo.from_user(c.author, getattr(c, 'is_anonymous', False)),
                    document_id=c.document_id,
                    shelfmark=None,  # Comments don't have shelfmark directly
                    created_at=c.created_at,
                    response_count=c.reply_count or 0,
                    is_featured=c.is_pinned
                ))

        # Sort by date
        feed_items.sort(key=lambda x: x.created_at, reverse=True)

        total = len(feed_items)

        # Apply pagination
        feed_items = feed_items[offset:offset + limit]

        return feed_items, total

    # ============================================
    # Admin functions
    # ============================================

    @staticmethod
    def feature_discovery(
        db: Session,
        discovery_id: int,
        featured: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """Feature or unfeature a discovery"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found"

        discovery.is_featured = featured
        db.commit()

        return True, None

    @staticmethod
    def hide_discovery(
        db: Session,
        discovery_id: int
    ) -> Tuple[bool, Optional[str]]:
        """Hide a discovery (admin action)"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found"

        discovery.status = DiscoveryStatus.HIDDEN
        db.commit()

        return True, None

    @staticmethod
    def pin_discovery(
        db: Session,
        discovery_id: int,
        pinned: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """Pin or unpin a discovery (admin action)"""
        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found"

        discovery.is_pinned = pinned
        db.commit()

        return True, None

    @staticmethod
    def mark_answered(
        db: Session,
        discovery_id: int,
        user: "User",
        answered: bool = True
    ) -> Tuple[bool, Optional[str]]:
        """Mark a question as answered (author or admin)"""
        from ..models.discovery import DiscoveryType

        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found"

        # Only author or admin can mark as answered
        if discovery.user_id != user.id and not user.is_admin():
            return False, "Not authorized"

        # Only questions can be marked as answered
        if discovery.discovery_type != DiscoveryType.QUESTION:
            return False, "Only questions can be marked as answered"

        discovery.is_answered = answered
        db.commit()

        return True, None

    @staticmethod
    def vote_discovery(
        db: Session,
        discovery_id: int,
        user: "User",
        vote_type: str  # 'up', 'down', or 'none' to remove vote
    ) -> Tuple[bool, Optional[str], Optional[dict]]:
        """Vote on a discovery (up/down or remove vote)"""
        from ..models.discovery import DiscoveryVote

        discovery = db.query(Discovery).filter(
            Discovery.id == discovery_id
        ).first()

        if not discovery:
            return False, "Discovery not found", None

        # Check for existing vote
        existing_vote = db.query(DiscoveryVote).filter(
            DiscoveryVote.discovery_id == discovery_id,
            DiscoveryVote.user_id == user.id
        ).first()

        if vote_type == 'none':
            # Remove existing vote
            if existing_vote:
                if existing_vote.vote_type == 'up':
                    discovery.upvotes = max(0, (discovery.upvotes or 0) - 1)
                else:
                    discovery.downvotes = max(0, (discovery.downvotes or 0) - 1)
                db.delete(existing_vote)
                db.commit()
            return True, None, {"upvotes": discovery.upvotes or 0, "downvotes": discovery.downvotes or 0}

        if vote_type not in ('up', 'down'):
            return False, "Invalid vote type", None

        if existing_vote:
            # Change vote
            if existing_vote.vote_type != vote_type:
                if existing_vote.vote_type == 'up':
                    discovery.upvotes = max(0, (discovery.upvotes or 0) - 1)
                    discovery.downvotes = (discovery.downvotes or 0) + 1
                else:
                    discovery.downvotes = max(0, (discovery.downvotes or 0) - 1)
                    discovery.upvotes = (discovery.upvotes or 0) + 1
                existing_vote.vote_type = vote_type
        else:
            # New vote
            new_vote = DiscoveryVote(
                discovery_id=discovery_id,
                user_id=user.id,
                vote_type=vote_type
            )
            db.add(new_vote)
            if vote_type == 'up':
                discovery.upvotes = (discovery.upvotes or 0) + 1
            else:
                discovery.downvotes = (discovery.downvotes or 0) + 1

        db.commit()
        return True, None, {"upvotes": discovery.upvotes or 0, "downvotes": discovery.downvotes or 0}

    @staticmethod
    def get_user_vote(
        db: Session,
        discovery_id: int,
        user_id: int
    ) -> Optional[str]:
        """Get user's vote on a discovery ('up', 'down', or None)"""
        from ..models.discovery import DiscoveryVote

        vote = db.query(DiscoveryVote).filter(
            DiscoveryVote.discovery_id == discovery_id,
            DiscoveryVote.user_id == user_id
        ).first()

        return vote.vote_type if vote else None
