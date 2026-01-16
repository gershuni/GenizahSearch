"""
Backend Configuration
Central configuration for the FastAPI backend
"""
import os
import secrets
from pathlib import Path
from typing import List, Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "Genizah Corrections API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"  # development, staging, production

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.environ.get('GENIZAH_PORT', 8081))

    # Database
    DATABASE_URL: str = "sqlite:///./data/genizah_users.db"

    # Security
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30
    ALGORITHM: str = "HS256"

    # Password requirements
    PASSWORD_MIN_LENGTH: int = 8
    PASSWORD_REQUIRE_UPPERCASE: bool = True
    PASSWORD_REQUIRE_NUMBERS: bool = True

    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080", "http://localhost:8081", "http://127.0.0.1:8000", "http://127.0.0.1:8081"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: List[str] = ["*"]
    CORS_ALLOW_HEADERS: List[str] = ["*"]

    # Rate limiting
    RATE_LIMIT_PER_MINUTE: int = 100
    RATE_LIMIT_BURST: int = 20

    # Email (for future email verification)
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAILS_FROM_EMAIL: Optional[str] = None
    EMAILS_FROM_NAME: Optional[str] = "Genizah Corrections"

    # Pagination defaults
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100

    # File paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    LOGS_DIR: Path = DATA_DIR / "logs"

    # Tantivy integration
    TANTIVY_INDEX_PATH: Optional[str] = None

    # Feature flags
    ENABLE_REGISTRATION: bool = True
    REQUIRE_EMAIL_VERIFICATION: bool = False
    ENABLE_SOCIAL_AUTH: bool = False
    ENABLE_API_KEYS: bool = True

    # Reputation system
    REPUTATION_CORRECTION_APPROVED: int = 10
    REPUTATION_CORRECTION_REJECTED: int = -2
    REPUTATION_UPVOTE_RECEIVED: int = 2
    REPUTATION_DOWNVOTE_RECEIVED: int = -1
    REPUTATION_HELPFUL_COMMENT: int = 5

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Convenience access
settings = get_settings()


# Ensure directories exist
def setup_directories():
    """Create required directories"""
    settings.DATA_DIR.mkdir(exist_ok=True)
    settings.LOGS_DIR.mkdir(exist_ok=True)


# Security utilities
def generate_api_key() -> str:
    """Generate a secure API key"""
    return secrets.token_urlsafe(48)


def generate_verification_token() -> str:
    """Generate email verification token"""
    return secrets.token_urlsafe(32)
