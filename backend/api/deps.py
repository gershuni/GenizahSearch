"""
API Dependencies - Authentication, Authorization, Common Parameters
"""
from typing import Optional
from fastapi import Depends, HTTPException, status, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from ..models.database import get_db
from ..models.user import User, UserRole
from ..services.auth_service import AuthService
from ..services.user_service import UserService

# Security scheme
security = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_api_key: Optional[str] = Header(None),
    db: Session = Depends(get_db)
) -> User:
    """
    Get the current authenticated user.
    Supports both JWT token and API key authentication.
    """
    user = None

    # Try JWT token first
    if credentials:
        user = AuthService.get_current_user(db, credentials.credentials)

    # Try API key if no token
    if not user and x_api_key:
        user = UserService.get_user_by_api_key(db, x_api_key)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_api_key: Optional[str] = Header(None),
    db: Session = Depends(get_db)
) -> Optional[User]:
    """
    Get current user if authenticated, None otherwise.
    Used for endpoints that have different behavior for authenticated users.
    """
    try:
        return await get_current_user(credentials, x_api_key, db)
    except HTTPException:
        return None


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Ensure user is active"""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated"
        )
    return current_user


async def get_current_verified_user(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """Ensure user is verified"""
    if not current_user.is_verified:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email verification required"
        )
    return current_user


def require_role(required_role: UserRole):
    """
    Dependency factory for role-based access control.

    Usage:
        @router.get("/admin-only")
        async def admin_endpoint(user: User = Depends(require_role(UserRole.ADMIN))):
            ...
    """
    async def role_checker(
        current_user: User = Depends(get_current_active_user)
    ) -> User:
        if not AuthService.check_permission(current_user, required_role):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires {required_role.value} role or higher"
            )
        return current_user

    return role_checker


# Common role dependencies
require_contributor = require_role(UserRole.CONTRIBUTOR)
require_reviewer = require_role(UserRole.REVIEWER)
require_editor = require_role(UserRole.EDITOR)
require_admin = require_role(UserRole.ADMIN)


def get_client_info(request: Request) -> dict:
    """Extract client info from request for logging"""
    return {
        'ip_address': request.client.host if request.client else None,
        'user_agent': request.headers.get('user-agent')
    }


class Pagination:
    """Pagination query parameters"""

    def __init__(
        self,
        page: int = 1,
        page_size: int = 20
    ):
        self.page = max(1, page)
        self.page_size = min(max(1, page_size), 100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size

    @property
    def limit(self) -> int:
        return self.page_size
