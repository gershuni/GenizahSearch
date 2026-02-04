# -*- coding: utf-8 -*-
"""
Shared Export Utilities for GenizahSearch.

This module provides unified text processing and export utilities used by both
the Desktop application (genizah_app.py) and the Web application (export_service.py).

This consolidates previously duplicated code to ensure consistent behavior across platforms.
"""

import re
from typing import Optional


# ============================================================================
# Text Sanitization for Excel
# ============================================================================

def sanitize_text_for_excel(text: Optional[str], max_length: int = 32700) -> str:
    """
    Sanitize text for safe use in Excel cells.

    Uses a whitelist approach to keep only characters valid in XML 1.0,
    handles formula injection prevention, and respects Excel cell limits.

    This unified function combines the best practices from both Desktop and Web:
    - Whitelist approach for XML safety (from Web)
    - Formula injection prevention (from Desktop)
    - Cell length limit (from Desktop)

    Args:
        text: The text to sanitize
        max_length: Maximum cell length (Excel limit is ~32767)

    Returns:
        Sanitized text safe for Excel cells
    """
    if not text:
        return ""

    t = str(text)

    # Whitelist approach: Keep only printable characters valid in XML 1.0
    # Range: U+0020-U+D7FF, U+E000-U+FFFD, plus tab (\t)
    t = "".join(
        ch for ch in t
        if (0x20 <= ord(ch) <= 0xD7FF) or (0xE000 <= ord(ch) <= 0xFFFD) or ch == "\t"
    )

    # Handle malicious formulas that could execute when opened in Excel
    # Prefix with single quote to prevent formula interpretation
    t = t.strip()
    if t.startswith(('=', '+', '-', '@')):
        t = "'" + t

    # Excel cell character limit (~32767, use 32700 for safety margin)
    if len(t) > max_length:
        t = t[:max_length] + "..."

    return t


def clean_text_single_line(text: Optional[str]) -> str:
    """
    Replace line breaks with spaces and normalize whitespace.
    Useful for Excel cells where we want continuous text.

    Args:
        text: The text to clean

    Returns:
        Text with newlines replaced by spaces and collapsed whitespace
    """
    if not text:
        return ""
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    # Collapse multiple spaces
    while '  ' in text:
        text = text.replace('  ', ' ')
    return text.strip()


def remove_highlight_markers(text: Optional[str]) -> str:
    """
    Remove * markers used for highlighting in snippets.

    Args:
        text: Text possibly containing * highlight markers

    Returns:
        Text with markers removed
    """
    if not text:
        return ""
    return text.replace('*', '')


# ============================================================================
# Filename Sanitization
# ============================================================================

def make_safe_filename(
    text: str,
    default: str = "genizah",
    max_length: int = 50,
    preserve_hebrew: bool = True
) -> str:
    """
    Create a filesystem-safe filename from text.

    This unified function works for both cache filenames and export filenames.

    Args:
        text: The text to convert to a filename
        default: Default name if text is empty or invalid
        max_length: Maximum filename length (excluding extension)
        preserve_hebrew: If True, keep Hebrew characters; if False, use ASCII-only

    Returns:
        A safe filename string
    """
    if not text:
        return default

    if preserve_hebrew:
        # Keep Hebrew, alphanumeric, underscore, hyphen, space
        # Pattern: \w includes [a-zA-Z0-9_], plus Hebrew range U+0590-U+05FF
        safe = re.sub(r"[^\w\u0590-\u05FF\s-]", "", text)
        # Replace spaces with underscores
        safe = re.sub(r"\s+", "_", safe).strip("_")
    else:
        # Strict ASCII-only mode (for cache filenames, prevents path traversal)
        safe = re.sub(r'[^a-zA-Z0-9_\-]', '_', text)

    # Truncate to max length
    safe = safe[:max_length]

    return safe.strip("_") or default


def sanitize_cache_filename(ref: str) -> str:
    """
    Sanitize a reference string to create a safe cache filename.

    Uses a strict whitelist approach: only alphanumeric characters, underscores,
    and hyphens are allowed. This prevents path traversal attacks.

    Args:
        ref: Reference string to sanitize

    Returns:
        Safe filename string for caching
    """
    return make_safe_filename(ref, default="cache", preserve_hebrew=False)


# ============================================================================
# Content-Disposition Header Encoding
# ============================================================================

def encode_filename_for_header(filename: str) -> str:
    """
    Encode filename for HTTP Content-Disposition header.
    Uses RFC 5987 encoding for non-ASCII characters (e.g., Hebrew).

    Args:
        filename: The filename to encode

    Returns:
        Properly formatted Content-Disposition header value
    """
    from urllib.parse import quote

    try:
        filename.encode('ascii')
        # Pure ASCII - simple format
        return f'attachment; filename="{filename}"'
    except UnicodeEncodeError:
        # Contains non-ASCII - use RFC 5987 format
        encoded = quote(filename, safe='')
        return f"attachment; filename*=UTF-8''{encoded}"


# ============================================================================
# Search Term Utilities
# ============================================================================

def extract_search_terms(query: str) -> list:
    """
    Extract meaningful search terms from a query string.
    Filters out special operators used in advanced search syntax.

    Args:
        query: The search query string

    Returns:
        List of search terms without operators
    """
    if not query:
        return []
    return [
        t.strip() for t in query.split()
        if t.strip() and not t.startswith(('=', '?', '~', '/', '$', '#'))
    ]


def contains_any_term(text: str, terms: list) -> bool:
    """
    Check if text contains any of the given search terms (case-insensitive).

    Args:
        text: Text to search in
        terms: List of terms to look for

    Returns:
        True if any term is found in text
    """
    if not text or not terms:
        return False
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in terms)
