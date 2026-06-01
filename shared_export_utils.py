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
    # Ranges: tab (\t), U+0020-U+007E (printable ASCII, excluding DEL 0x7F),
    # U+0080-U+D7FF (extended chars including Hebrew), U+E000-U+FFFD
    t = "".join(
        ch for ch in t
        if ch == "\t" or (0x20 <= ord(ch) <= 0x7E) or (0x80 <= ord(ch) <= 0xD7FF) or (0xE000 <= ord(ch) <= 0xFFFD)
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


def coerce_img_page_cell(value):
    """Return ``value`` as ``int`` when it's a pure integer string; else as a
    sanitized string.

    The Image/Page column on the main search-results sheet usually holds a
    page number (``'5'``, ``'42'``) but can also hold non-numeric folio
    markers (``'5r'``, ``'12-14'``, ``'fol. 3v'``). Smoke verification
    round 6 (2026-05-21): writing ``str('5')`` triggers Excel's green
    "Number stored as text" warning. Return an ``int`` so Excel recognizes
    it as numeric; non-numeric values fall through to the sanitized string
    path unchanged.

    Returns:
        int when the trimmed value is a pure decimal integer (including a
        leading minus sign);
        sanitized string otherwise (empty string for empty / None input).
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        # bool is an int subclass — keep as-is to avoid silently coercing
        # ``True``/``False`` into ``1``/``0``.
        return sanitize_text_for_excel(str(value))
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s:
        return ''
    if s.isdigit() or (s.startswith('-') and len(s) > 1 and s[1:].isdigit()):
        try:
            return int(s)
        except (TypeError, ValueError):
            pass
    return sanitize_text_for_excel(s)


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
# Expanded context for DOCX / TXT exports (Phase 105 EXPUX-04)
# ============================================================================

# DOCX/TXT show fuller matched-passage context than the ±60-char on-screen
# snippet, capped so a multi-thousand-word PDF page cannot bloat the document.
EXPORT_CONTEXT_CAP = 2000


def _matched_terms_from_hl(raw_file_hl: Optional[str]) -> list:
    """Return the highlighted (matched) substrings from a ``*``-marked snippet.

    Odd-indexed parts of a ``*``-split are the highlighted spans. Returns them
    deduped, stripped, non-empty, longest-first (so a single alternation regex
    marks maximal spans before their substrings).
    """
    if not raw_file_hl:
        return []
    parts = str(raw_file_hl).split('*')
    terms = [parts[i].strip() for i in range(1, len(parts), 2) if parts[i].strip()]
    seen = set()
    uniq = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    uniq.sort(key=len, reverse=True)
    return uniq


def build_expanded_context(full_text: Optional[str],
                           raw_file_hl: Optional[str],
                           cap: int = EXPORT_CONTEXT_CAP) -> str:
    """Phase 105 EXPUX-04: a wider, highlighted context window for DOCX/TXT.

    Recovers the matched term(s) from ``raw_file_hl`` (text between ``*``
    markers), locates the first occurrence in ``full_text``, builds a window of
    ``<= cap`` chars centred on it, and re-inserts ``*`` markers around every
    in-window occurrence of the matched term(s). Truncation is flagged with
    leading/trailing ellipses.

    Fallback (NO regression): if ``full_text`` is empty, or no matched term can
    be located inside it (e.g. normalization mismatch), the original
    ``raw_file_hl`` snippet is returned unchanged.
    """
    ft = str(full_text or "")
    hl = str(raw_file_hl or "")
    if not ft.strip():
        return hl
    terms = _matched_terms_from_hl(hl)
    if not terms:
        return hl

    first_pos = None
    for t in terms:
        idx = ft.find(t)
        if idx != -1 and (first_pos is None or idx < first_pos):
            first_pos = idx
    if first_pos is None:
        return hl  # matched term not present in full_text -> safe fallback

    if cap and cap > 0 and len(ft) > cap:
        half = cap // 2
        start = max(0, first_pos - half)
        end = min(len(ft), start + cap)
        start = max(0, end - cap)  # re-extend left if we clipped the right edge
        window = ft[start:end]
    else:
        start = 0
        end = len(ft)
        window = ft

    # Drop stray source ``*`` so they cannot be mistaken for highlight markers.
    window = window.replace('*', ' ')
    pattern = re.compile("|".join(re.escape(t) for t in terms))
    marked = pattern.sub(lambda m: f"*{m.group(0)}*", window)

    prefix = "… " if start > 0 else ""
    suffix = " …" if end < len(ft) else ""
    return prefix + marked + suffix


# ============================================================================
# Rich-text snippet rendering (Phase 94 D-14)
# ============================================================================

def build_rich_snippet_cell(text, sanitize_fn=None):
    """Render snippet text with ``*``-bracketed highlights as openpyxl rich text.

    Phase 94 D-14: extracted from desktop's ``write_rich_cell`` inner helper at
    ``genizah_app.py:18000`` so web's main-sheet Snippet column can adopt the
    same red+bold highlight rendering. Both apps consume this helper
    identically; the sanitize callback lets desktop pass its
    ``self._sanitize_for_excel`` and web pass the module-level
    ``sanitize_text_for_excel``.

    Args:
        text: snippet text containing ``*foo*`` markers for highlighted runs.
        sanitize_fn: optional callable applied BEFORE splitting on ``*``.
            Typically :func:`sanitize_text_for_excel` from this module. Pass
            ``None`` to skip sanitization (caller has already sanitized).

    Returns:
        - ``str`` (the sanitized text) when no ``*`` markers present.
        - :class:`openpyxl.cell.rich_text.CellRichText` when markers present:
          odd-indexed parts (between markers) become red+bold; even-indexed
          parts (outside markers) become normal black.
        - Empty string ``''`` when text is empty / None.

    Sub-sheets (Manuscripts, Bibliography) should NOT use this helper —
    Phase 94 D-14 limits rich-text to the main-sheet Snippet column only.

    Sanitize-first ordering (T-94-01 mitigation): the sanitize callback
    runs BEFORE the ``*``-split so that formula-injection prefix
    (``"'="`` for ``'='``-leading text) is preserved into the first split
    part rather than getting interleaved with highlight markers.
    """
    if not text:
        return ''
    safe_text = sanitize_fn(text) if sanitize_fn else str(text)
    if '*' not in safe_text:
        return safe_text

    # Late import: openpyxl rich-text is a hot dependency we don't want to
    # pay on import for callers that never render snippets.
    from openpyxl.cell.rich_text import CellRichText, TextBlock
    from openpyxl.cell.text import InlineFont

    font_red = InlineFont(color='FF0000', b=True)
    font_normal = InlineFont(color='000000', b=False)

    parts = safe_text.split('*')
    rich = CellRichText()
    for i, part in enumerate(parts):
        if not part:
            continue
        # Odd indices = highlighted (between markers); even indices = plain.
        if i % 2 == 1:
            rich.append(TextBlock(font_red, part))
        else:
            rich.append(TextBlock(font_normal, part))
    return rich


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
