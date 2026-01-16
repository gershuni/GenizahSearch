"""
User Service - User Management Operations
"""
from datetime import datetime
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import or_, func

from ..config import settings, generate_api_key
from ..models.user import User, UserRole
from ..models.correction import Correction, CorrectionStatus
from ..models.comment import Comment
from ..models.activity_log import ActivityLog, ActivityType
from ..schemas.user import UserCreate, UserUpdate, UserStats
from .auth_service import AuthService


class UserService:
    """Service for user management operations"""

    @staticmethod
    def create_user(
        db: Session,
        user_data: UserCreate,
        ip_address: str = None
    ) -> Tuple[Optional[User], Optional[str]]:
        """
        Create a new user account.

        Args:
            db: Database session
            user_data: User creation data
            ip_address: Request IP for logging

        Returns:
            Tuple of (User object or None, error message or None)
        """
        # Check if registration is enabled
        if not settings.ENABLE_REGISTRATION:
            return None, "Registration is currently disabled"

        # Check if email already exists
        if db.query(User).filter(User.email == user_data.email).first():
            return None, "Email already registered"

        # Check if username already exists
        if db.query(User).filter(User.username == user_data.username).first():
            return None, "Username already taken"

        # Validate password
        is_valid, error = AuthService.validate_password_strength(user_data.password)
        if not is_valid:
            return None, error

        # Create user
        user = User(
            email=user_data.email,
            username=user_data.username,
            password_hash=AuthService.hash_password(user_data.password),
            full_name=user_data.full_name,
            affiliation=user_data.affiliation,
            bio=user_data.bio,
            role=UserRole.CONTRIBUTOR,  # Default role
            is_active=True,
            is_verified=not settings.REQUIRE_EMAIL_VERIFICATION
        )

        db.add(user)
        db.commit()
        db.refresh(user)

        # Log registration
        ActivityLog.log(
            db,
            ActivityType.USER_REGISTER,
            user_id=user.id,
            target_type='user',
            target_id=str(user.id),
            description=f"New user registered: {user.username}",
            ip_address=ip_address
        )

        return user, None

    @staticmethod
    def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
        """Get user by ID"""
        return db.query(User).filter(User.id == user_id).first()

    @staticmethod
    def get_user_by_email(db: Session, email: str) -> Optional[User]:
        """Get user by email"""
        return db.query(User).filter(User.email == email).first()

    @staticmethod
    def get_user_by_username(db: Session, username: str) -> Optional[User]:
        """Get user by username"""
        return db.query(User).filter(User.username == username).first()

    @staticmethod
    def get_user_by_api_key(db: Session, api_key: str) -> Optional[User]:
        """Get user by API key"""
        return db.query(User).filter(User.api_key == api_key).first()

    @staticmethod
    def update_user(
        db: Session,
        user: User,
        update_data: UserUpdate,
        ip_address: str = None
    ) -> User:
        """
        Update user profile.

        Args:
            db: Database session
            user: User to update
            update_data: Update data
            ip_address: Request IP for logging

        Returns:
            Updated user object
        """
        # Update fields that are provided
        if update_data.full_name is not None:
            user.full_name = update_data.full_name
        if update_data.affiliation is not None:
            user.affiliation = update_data.affiliation
        if update_data.bio is not None:
            user.bio = update_data.bio
        if update_data.avatar_url is not None:
            user.avatar_url = update_data.avatar_url

        user.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(user)

        # Log update
        ActivityLog.log(
            db,
            ActivityType.USER_UPDATE_PROFILE,
            user_id=user.id,
            target_type='user',
            target_id=str(user.id),
            ip_address=ip_address
        )

        return user

    @staticmethod
    def generate_api_key_for_user(db: Session, user: User) -> str:
        """Generate a new API key for the user"""
        if not settings.ENABLE_API_KEYS:
            raise ValueError("API keys are disabled")

        user.api_key = generate_api_key()
        db.commit()
        return user.api_key

    @staticmethod
    def revoke_api_key(db: Session, user: User) -> None:
        """Revoke user's API key"""
        user.api_key = None
        db.commit()

    @staticmethod
    def get_user_stats(db: Session, user_id: int) -> UserStats:
        """Get statistics for a user"""
        # Count corrections by status
        total_corrections = db.query(func.count(Correction.id)).filter(
            Correction.author_id == user_id
        ).scalar() or 0

        pending = db.query(func.count(Correction.id)).filter(
            Correction.author_id == user_id,
            Correction.status == CorrectionStatus.PENDING
        ).scalar() or 0

        approved = db.query(func.count(Correction.id)).filter(
            Correction.author_id == user_id,
            Correction.status == CorrectionStatus.APPROVED
        ).scalar() or 0

        rejected = db.query(func.count(Correction.id)).filter(
            Correction.author_id == user_id,
            Correction.status == CorrectionStatus.REJECTED
        ).scalar() or 0

        # Count comments
        total_comments = db.query(func.count(Comment.id)).filter(
            Comment.author_id == user_id,
            Comment.is_deleted == False
        ).scalar() or 0

        # Get user reputation
        user = db.query(User).filter(User.id == user_id).first()
        reputation = user.reputation_score if user else 0

        return UserStats(
            total_corrections=total_corrections,
            pending_corrections=pending,
            approved_corrections=approved,
            rejected_corrections=rejected,
            total_comments=total_comments,
            reputation_score=reputation
        )

    @staticmethod
    def search_users(
        db: Session,
        query: str = None,
        role: UserRole = None,
        is_active: bool = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[User], int]:
        """
        Search users with filters.

        Returns:
            Tuple of (list of users, total count)
        """
        q = db.query(User)

        if query:
            q = q.filter(or_(
                User.username.ilike(f"%{query}%"),
                User.full_name.ilike(f"%{query}%"),
                User.email.ilike(f"%{query}%")
            ))

        if role:
            q = q.filter(User.role == role)

        if is_active is not None:
            q = q.filter(User.is_active == is_active)

        total = q.count()

        users = q.order_by(User.created_at.desc()) \
            .offset((page - 1) * page_size) \
            .limit(page_size) \
            .all()

        return users, total

    @staticmethod
    def update_user_role(
        db: Session,
        admin: User,
        target_user_id: int,
        new_role: UserRole,
        ip_address: str = None
    ) -> Tuple[Optional[User], Optional[str]]:
        """
        Update a user's role (admin only).

        Args:
            db: Database session
            admin: Admin user performing the action
            target_user_id: User to update
            new_role: New role to assign
            ip_address: Request IP for logging

        Returns:
            Tuple of (updated user or None, error message or None)
        """
        if not admin.is_admin():
            return None, "Only administrators can change user roles"

        target_user = db.query(User).filter(User.id == target_user_id).first()
        if not target_user:
            return None, "User not found"

        old_role = target_user.role
        target_user.role = new_role
        target_user.updated_at = datetime.utcnow()
        db.commit()

        # Log the change
        ActivityLog.log(
            db,
            ActivityType.ADMIN_USER_ROLE_CHANGE,
            user_id=admin.id,
            target_type='user',
            target_id=str(target_user.id),
            description=f"Role changed from {old_role} to {new_role}",
            extra_data={'old_role': old_role.value, 'new_role': new_role.value},
            ip_address=ip_address
        )

        return target_user, None

    @staticmethod
    def deactivate_user(
        db: Session,
        admin: User,
        target_user_id: int,
        ip_address: str = None
    ) -> Tuple[bool, str]:
        """Deactivate a user account (admin only)"""
        if not admin.is_admin():
            return False, "Only administrators can deactivate users"

        target_user = db.query(User).filter(User.id == target_user_id).first()
        if not target_user:
            return False, "User not found"

        if target_user.id == admin.id:
            return False, "Cannot deactivate your own account"

        target_user.is_active = False
        target_user.updated_at = datetime.utcnow()
        db.commit()

        ActivityLog.log(
            db,
            ActivityType.ADMIN_USER_DEACTIVATE,
            user_id=admin.id,
            target_type='user',
            target_id=str(target_user.id),
            ip_address=ip_address
        )

        return True, ""

    @staticmethod
    def activate_user(
        db: Session,
        admin: User,
        target_user_id: int,
        ip_address: str = None
    ) -> Tuple[bool, str]:
        """Activate a user account (admin only)"""
        if not admin.is_admin():
            return False, "Only administrators can activate users"

        target_user = db.query(User).filter(User.id == target_user_id).first()
        if not target_user:
            return False, "User not found"

        target_user.is_active = True
        target_user.updated_at = datetime.utcnow()
        db.commit()

        ActivityLog.log(
            db,
            ActivityType.ADMIN_USER_ACTIVATE,
            user_id=admin.id,
            target_type='user',
            target_id=str(target_user.id),
            ip_address=ip_address
        )

        return True, ""

    @staticmethod
    def add_reputation(db: Session, user: User, points: int, reason: str = None):
        """Add reputation points to a user"""
        user.add_reputation(points)
        db.commit()

    @staticmethod
    def get_leaderboard(
        db: Session,
        limit: int = 10,
        period_days: int = None
    ) -> List[User]:
        """
        Get top users by reputation.

        Args:
            db: Database session
            limit: Number of users to return
            period_days: If set, only count activity in this period

        Returns:
            List of top users
        """
        return db.query(User) \
            .filter(User.is_active == True) \
            .order_by(User.reputation_score.desc()) \
            .limit(limit) \
            .all()
