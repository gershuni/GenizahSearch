"""
Genizah Corrections API - Main Application Entry Point
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from .config import settings, setup_directories
from .models.database import init_db
from .api.routes import auth, users, corrections, comments, documents, versions, discoveries, admin

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting Genizah Corrections API...")
    setup_directories()
    init_db()
    logger.info("Database initialized")
    yield
    # Shutdown
    logger.info("Shutting down Genizah Corrections API...")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
## Genizah Corrections API

A comprehensive system for managing user corrections and annotations
on Cairo Genizah manuscript transcriptions.

### Features

- **User Management**: Registration, authentication, roles
- **Corrections**: Submit, review, approve/reject transcription corrections
- **Comments**: Threaded discussions on documents and corrections
- **Voting**: Community voting on correction quality
- **Reputation**: Gamification system for contributors

### Authentication

Most endpoints require authentication via JWT token:
```
Authorization: Bearer <token>
```

Or via API key:
```
X-API-Key: <api_key>
```
    """,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.DEBUG else None
        }
    )


# Include routers
app.include_router(auth.router, prefix="/api/v1")
app.include_router(users.router, prefix="/api/v1")
app.include_router(corrections.router, prefix="/api/v1")
app.include_router(comments.router, prefix="/api/v1")
app.include_router(documents.router, prefix="/api/v1")
app.include_router(versions.router, prefix="/api/v1")
app.include_router(discoveries.router, prefix="/api/v1")
app.include_router(admin.router, prefix="/api/v1")

# Mount web frontend
try:
    import sys
    frontend_path = Path(__file__).parent.parent / "frontend_web"
    sys.path.insert(0, str(frontend_path.parent))
    from frontend_web.web_app import web_app
    app.mount("/web", web_app)
    logger.info("Web frontend mounted at /web")
except Exception as e:
    logger.warning(f"Could not mount web frontend: {e}")


# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT
    }


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/api/docs",
        "health": "/api/health"
    }


# Mount static files if directory exists
static_dir = Path(__file__).parent.parent / "frontend_web" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# Run with uvicorn if executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
