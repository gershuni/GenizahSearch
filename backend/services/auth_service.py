"""
Authentication Service - JWT and Password Management
"""
from datetime import datetime, timedelta
from typing import Optional, Tuple
from jose import JWTError, jwt
import bcrypt
from sqlalchemy.orm import Session

from ..config import settings
from ..models.user import User, UserRole
from ..models.activity_log import ActivityLog, ActivityType
from ..schemas.user import Token, TokenData


class AuthService:
    """Authentication and authorization service"""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using bcrypt"""
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password_bytes, salt).decode('utf-8')

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        try:
            password_bytes = plain_password.encode('utf-8')
            hash_bytes = hashed_password.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hash_bytes)
        except Exception:
            return False

    @staticmethod
    def create_access_token(
        user: User,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create a JWT access token for a user.

        Args:
            user: User object
            expires_delta: Optional custom expiration time

        Returns:
            Encoded JWT token string
        """
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(
                minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
            )

        to_encode = {
            "sub": str(user.id),
            "email": user.email,
            "username": user.username,
            "role": user.role.value,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        }

        return jwt.encode(
            to_encode,
            settings.SECRET_KEY,
            algorithm=settings.ALGORITHM
        )

    @staticmethod
    def create_refresh_token(user: User) -> str:
        """
        Create a refresh token for a user.

        Args:
            user: User object

        Returns:
            Encoded JWT refresh token string
        """
        expire = datetime.utcnow() + timedelta(
            days=settings.REFRESH_TOKEN_EXPIRE_DAYS
        )

        to_encode = {
            "sub": str(user.id),
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        }

        return jwt.encode(
            to_encode,
            settings.SECRET_KEY,
            algorithm=settings.ALGORITHM
        )

    @staticmethod
    def create_tokens(user: User) -> Token:
        """
        Create both access and refresh tokens.

        Args:
            user: User object

        Returns:
            Token schema with both tokens
        """
        access_token = AuthService.create_access_token(user)
        refresh_token = AuthService.create_refresh_token(user)

        return Token(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_in=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )

    @staticmethod
    def decode_token(token: str) -> Optional[TokenData]:
        """
        Decode and validate a JWT token.

        Args:
            token: JWT token string

        Returns:
            TokenData if valid, None otherwise
        """
        try:
            payload = jwt.decode(
                token,
                settings.SECRET_KEY,
                algorithms=[settings.ALGORITHM]
            )

            user_id = int(payload.get("sub"))
            email = payload.get("email")
            username = payload.get("username")
            role = payload.get("role")
            exp = datetime.fromtimestamp(payload.get("exp"))

            if user_id is None:
                return None

            return TokenData(
                user_id=user_id,
                email=email,
                username=username,
                role=role,
                exp=exp
            )

        except JWTError:
            return None

    @staticmethod
    def decode_refresh_token(token: str) -> Optional[int]:
        """
        Decode a refresh token and return user ID.

        Args:
            token: JWT refresh token

        Returns:
            User ID if valid, None otherwise
        """
        try:
            payload = jwt.decode(
                token,
                settings.SECRET_KEY,
                algorithms=[settings.ALGORITHM]
            )

            if payload.get("type") != "refresh":
                return None

            return int(payload.get("sub"))

        except JWTError:
            return None

    @staticmethod
    def authenticate_user(
        db: Session,
        email: str,
        password: str,
        ip_address: str = None,
        user_agent: str = None
    ) -> Tuple[Optional[User], Optional[str]]:
        """
        Authenticate a user by email and password.

        Args:
            db: Database session
            email: User's email
            password: User's password
            ip_address: Request IP (for logging)
            user_agent: Request user agent (for logging)

        Returns:
            Tuple of (User object or None, error message or None)
        """
        user = db.query(User).filter(User.email == email).first()

        if not user:
            return None, "Invalid email or password"

        if not AuthService.verify_password(password, user.password_hash):
            return None, "Invalid email or password"

        if not user.is_active:
            return None, "Account is deactivated"

        # Update last login
        user.last_login = datetime.utcnow()
        db.commit()

        # Log the login
        ActivityLog.log(
            db,
            ActivityType.USER_LOGIN,
            user_id=user.id,
            target_type='user',
            target_id=str(user.id),
            ip_address=ip_address,
            user_agent=user_agent
        )

        return user, None

    @staticmethod
    def get_current_user(db: Session, token: str) -> Optional[User]:
        """
        Get the current user from a token.

        Args:
            db: Database session
            token: JWT access token

        Returns:
            User object if valid, None otherwise
        """
        token_data = AuthService.decode_token(token)

        if not token_data:
            return None

        user = db.query(User).filter(User.id == token_data.user_id).first()

        if not user or not user.is_active:
            return None

        return user

    @staticmethod
    def refresh_tokens(db: Session, refresh_token: str) -> Optional[Token]:
        """
        Refresh access token using refresh token.

        Args:
            db: Database session
            refresh_token: JWT refresh token

        Returns:
            New Token or None if invalid
        """
        user_id = AuthService.decode_refresh_token(refresh_token)

        if not user_id:
            return None

        user = db.query(User).filter(User.id == user_id).first()

        if not user or not user.is_active:
            return None

        return AuthService.create_tokens(user)

    @staticmethod
    def validate_password_strength(password: str) -> Tuple[bool, str]:
        """
        Validate password meets requirements.

        Args:
            password: Password to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if len(password) < settings.PASSWORD_MIN_LENGTH:
            return False, f"Password must be at least {settings.PASSWORD_MIN_LENGTH} characters"

        if settings.PASSWORD_REQUIRE_UPPERCASE and not any(c.isupper() for c in password):
            return False, "Password must contain at least one uppercase letter"

        if settings.PASSWORD_REQUIRE_NUMBERS and not any(c.isdigit() for c in password):
            return False, "Password must contain at least one number"

        return True, ""

    @staticmethod
    def change_password(
        db: Session,
        user: User,
        current_password: str,
        new_password: str,
        ip_address: str = None
    ) -> Tuple[bool, str]:
        """
        Change user's password.

        Args:
            db: Database session
            user: User object
            current_password: Current password for verification
            new_password: New password to set
            ip_address: Request IP (for logging)

        Returns:
            Tuple of (success, error_message)
        """
        # Verify current password
        if not AuthService.verify_password(current_password, user.password_hash):
            return False, "Current password is incorrect"

        # Validate new password
        is_valid, error = AuthService.validate_password_strength(new_password)
        if not is_valid:
            return False, error

        # Update password
        user.password_hash = AuthService.hash_password(new_password)
        user.updated_at = datetime.utcnow()
        db.commit()

        # Log the change
        ActivityLog.log(
            db,
            ActivityType.USER_CHANGE_PASSWORD,
            user_id=user.id,
            target_type='user',
            target_id=str(user.id),
            ip_address=ip_address
        )

        return True, ""

    @staticmethod
    def check_permission(user: User, required_role: UserRole) -> bool:
        """
        Check if user has at least the required role level.

        Role hierarchy: GUEST < CONTRIBUTOR < REVIEWER < EDITOR < ADMIN
        """
        role_hierarchy = {
            UserRole.GUEST: 0,
            UserRole.CONTRIBUTOR: 1,
            UserRole.REVIEWER: 2,
            UserRole.EDITOR: 3,
            UserRole.ADMIN: 4
        }

        user_level = role_hierarchy.get(user.role, 0)
        required_level = role_hierarchy.get(required_role, 0)

        return user_level >= required_level
