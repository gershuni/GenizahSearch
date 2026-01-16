#!/usr/bin/env python3
"""
Run the Genizah Corrections API Server
"""
import sys
import os
import argparse

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn
from backend.config import settings
from backend.models.database import init_db

def main():
    """Run the server"""
    parser = argparse.ArgumentParser(description="Genizah Corrections API Server")
    parser.add_argument("--port", "-p", type=int, default=8080, help="Port to run on (default: 8080)")
    parser.add_argument("--host", "-H", type=str, default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    parser.add_argument("--reload", "-r", action="store_true", help="Enable auto-reload")
    args = parser.parse_args()

    print("=" * 50)
    print("Genizah Corrections API Server")
    print("=" * 50)
    print(f"Environment: {settings.ENVIRONMENT}")
    print(f"Database: {settings.DATABASE_URL}")
    print("=" * 50)

    # Initialize database
    print("Initializing database...")
    init_db()

    # Run server
    print(f"\nStarting server at http://{args.host}:{args.port}")
    print("API docs available at /api/docs")
    print("Press Ctrl+C to stop\n")

    uvicorn.run(
        "backend.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )

if __name__ == "__main__":
    main()
