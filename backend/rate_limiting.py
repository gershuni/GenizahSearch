# -*- coding: utf-8 -*-
"""
Rate Limiting Configuration for Genizah API

Uses slowapi for rate limiting with different limits for:
- Login endpoints: 5 requests per minute per IP (brute-force protection)
- Search endpoints: 30 requests per minute per user/IP (resource protection)
- General API: 100 requests per minute per user/IP (fair usage)

Note: Requires 'slowapi' package to be installed:
    pip install slowapi
"""
import logging
from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

logger = logging.getLogger(__name__)


def get_user_identifier(request: Request) -> str:
    """
    Get rate limit key based on authenticated user or IP address.
    """
    # Try to get user from request state
    user = getattr(request.state, 'user', None)
    if user and hasattr(user, 'id'):
        return f"user:{user.id}"

    # Try Authorization header
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if token:
            return f"token:{token[:16]}"

    # Check for API key
    api_key = request.headers.get('X-API-Key', '')
    if api_key:
        return f"apikey:{api_key[:16]}"

    # Fall back to IP address
    return get_remote_address(request)


def get_ip_only(request: Request) -> str:
    """Always use IP address for rate limiting (for login endpoints)."""
    return get_remote_address(request)


# Create limiter instance
limiter = Limiter(
    key_func=get_user_identifier,
    default_limits=["100/minute"],
    storage_uri="memory://",
    strategy="fixed-window"
)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """Custom handler for rate limit exceeded errors."""
    limit_value = getattr(exc, 'detail', 'Rate limit exceeded')
    client_ip = get_remote_address(request)
    user_key = get_user_identifier(request)

    logger.warning(
        f"Rate limit exceeded - IP: {client_ip}, User Key: {user_key}, "
        f"Path: {request.url.path}, Limit: {limit_value}"
    )

    retry_after = 60

    return JSONResponse(
        status_code=429,
        content={
            "error": "Too Many Requests",
            "detail": f"Rate limit exceeded: {limit_value}",
            "message": "You have exceeded the allowed number of requests. Please wait before trying again.",
            "retry_after_seconds": retry_after
        },
        headers={
            "Retry-After": str(retry_after),
            "X-RateLimit-Limit": str(limit_value),
        }
    )


def setup_rate_limiting(app):
    """Configure rate limiting for the FastAPI application."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    logger.info("Rate limiting configured successfully")


__all__ = [
    'limiter',
    'setup_rate_limiting',
    'get_user_identifier',
    'get_ip_only',
    'RateLimitExceeded'
]
