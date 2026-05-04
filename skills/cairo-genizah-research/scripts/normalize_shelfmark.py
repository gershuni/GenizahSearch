"""Tier-1 lightweight shelfmark normalization for known-witness comparison.

Best-effort, intentionally simple. Edge cases (paired-leaf bifolios, library
code prefixes) drop to Tier 2 — `/api/search?search_mode=shelfmark` resolution
invoked by the model from SKILL.md instructions, NOT this script.

Per SKILL-05: this skill does NOT depend on genizah_core. All normalization
logic is intentionally duplicated for portability to systems without the
GenizahSearch repo.
"""
from __future__ import annotations
import re
import unicodedata

_MS_PREFIX_RE = re.compile(r"^(MS\.?\s+|MS_)", re.IGNORECASE)
_MULTI_WS_RE = re.compile(r"\s+")
_PUNCT_PAD_RE = re.compile(r"\s*([.\-])\s*")


def normalize(s: str) -> str:
    """Return a normalized shelfmark string for case-insensitive comparison.

    Idempotent: normalize(normalize(s)) == normalize(s) for all str s.

    Steps:
    1. NFKC unicode normalization + strip
    2. Strip leading 'MS ' or 'MS. ' prefix (case-insensitive)
    3. Collapse multiple whitespace to single space
    4. Remove padding whitespace around '.' and '-'
    5. Uppercase
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).strip()
    s = _MS_PREFIX_RE.sub("", s)
    s = _MULTI_WS_RE.sub(" ", s)
    s = _PUNCT_PAD_RE.sub(r"\1", s)
    return s.upper()
