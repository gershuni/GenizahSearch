# -*- coding: utf-8 -*-
"""
Unified Supabase client provider for GenizahSearch.

Provides a shared Supabase client singleton for both the web and desktop apps.
This module handles ONLY the client factory -- no auth, lists, corrections,
or other operations. It is used exclusively for read-only PGP data access.
"""

import os
from typing import Optional
from supabase import create_client, Client

# Load .env file if present, so desktop entry points (which do not call
# load_dotenv() themselves) still pick up credentials. Idempotent — web
# calls load_dotenv() earlier, and python-dotenv does not override existing
# process env vars, so this is safe regardless of import order.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; caller must set env vars directly

# Configuration from environment variables with development defaults
SUPABASE_URL = os.environ.get(
    'SUPABASE_URL',
    'https://ylcpglwxompwjcufdemz.supabase.co'
)
SUPABASE_ANON_KEY = os.environ.get(
    'SUPABASE_ANON_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlsY3BnbHd4b21wd2pjdWZkZW16Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk3Njc0NzUsImV4cCI6MjA4NTM0MzQ3NX0.xKzlyKrBV0MxADYHqD0lyyymoVxTX91hyI4T6TGchpE'
)

# Singleton client instance
_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_ANON_KEY:
            raise ValueError(
                "SUPABASE_ANON_KEY not set! "
                "Set it in environment variables or .env file."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client


def reset_client():
    """Reset the client singleton (useful for testing)."""
    global _client
    _client = None


def get_url() -> str:
    """Get Supabase URL from environment."""
    return SUPABASE_URL


def get_anon_key() -> str:
    """Get Supabase anonymous key from environment."""
    return SUPABASE_ANON_KEY
