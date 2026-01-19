"""
Authentication Routes - Login, Register, Token Refresh
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session

from ...models.database import get_db
from ...models.user import User
from ...schemas.user import (
    UserCreate, UserResponse, UserLogin, Token, PasswordChange
)
from ...schemas.common import SuccessResponse, ErrorResponse
from ...services.auth_service import AuthService
from ...services.user_service import UserService
from ..deps import get_current_active_user, get_client_info

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Register a new user account.

    - **email**: Valid email address (unique)
    - **username**: Alphanumeric username (3-50 chars, unique)
    - **password**: Strong password (8+ chars, uppercase, number)
    - **full_name**: Optional full name
    - **affiliation**: Optional academic affiliation
    - **bio**: Optional biography
    """
    client_info = get_client_info(request)

    user, error = UserService.create_user(
        db, user_data, ip_address=client_info.get('ip_address')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return user


@router.post("/login", response_model=Token)
async def login(
    credentials: UserLogin,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Login with email and password.
    Returns access and refresh tokens.
    """
    client_info = get_client_info(request)

    user, error = AuthService.authenticate_user(
        db,
        credentials.email,
        credentials.password,
        ip_address=client_info.get('ip_address'),
        user_agent=client_info.get('user_agent')
    )

    if error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error,
            headers={"WWW-Authenticate": "Bearer"},
        )

    return AuthService.create_tokens(user)


@router.post("/refresh", response_model=Token)
async def refresh_token(
    refresh_token: str,
    db: Session = Depends(get_db)
):
    """
    Refresh access token using refresh token.
    """
    tokens = AuthService.refresh_tokens(db, refresh_token)

    if not tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return tokens


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get current authenticated user's information.
    """
    return current_user


@router.post("/change-password", response_model=SuccessResponse)
async def change_password(
    password_data: PasswordChange,
    request: Request,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Change current user's password.
    Requires current password for verification.
    """
    client_info = get_client_info(request)

    success, error = AuthService.change_password(
        db,
        current_user,
        password_data.current_password,
        password_data.new_password,
        ip_address=client_info.get('ip_address')
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    return SuccessResponse(
        success=True,
        message="Password changed successfully"
    )


@router.post("/logout", response_model=SuccessResponse)
async def logout(
    current_user: User = Depends(get_current_active_user)
):
    """
    Logout current user.
    Note: Since we use stateless JWT, this is mostly for client-side token clearing.
    In production, you might want to implement token blacklisting.
    """
    # In a production system, you might add the token to a blacklist here
    return SuccessResponse(
        success=True,
        message="Logged out successfully"
    )


@router.post("/api-key", response_model=SuccessResponse)
async def generate_api_key(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Generate a new API key for the current user.
    Previous API key (if any) will be replaced.
    """
    try:
        api_key = UserService.generate_api_key_for_user(db, current_user)
        return SuccessResponse(
            success=True,
            message="API key generated successfully",
            data={"api_key": api_key}
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/api-key", response_model=SuccessResponse)
async def revoke_api_key(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Revoke current user's API key.
    """
    UserService.revoke_api_key(db, current_user)
    return SuccessResponse(
        success=True,
        message="API key revoked successfully"
    )
