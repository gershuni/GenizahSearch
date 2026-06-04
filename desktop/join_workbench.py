# -*- coding: utf-8 -*-
"""Desktop Join Workbench — anchor-pinned join-hunting shell (Phase 107).

This module holds the pure helpers (Plan 01) and the JoinWorkbenchWindow shell (Plan 02).
Pure helpers are import-safe without a QApplication so they can be unit-tested headlessly.
"""
from genizah_core import CURRENT_LANG, get_logger, tr  # noqa: F401
from shared.joins_lab import htmlify, page_of, snippet_html, snippet_plain  # D-18: reuse Phase 106

logger = get_logger(__name__)

__all__ = [
    # Phase 106 re-exports (D-18)
    "htmlify",
    "page_of",
    "snippet_html",
    "snippet_plain",
    # Result-dict accessors
    "r_sid",
    "r_shelf",
    "r_title",
    "r_text",
    "r_lib",
    # Pure helpers
    "iiif_full",
    "meta_brief",
    "badge_for_source",
    "dedup_join_rows",
]

# ---------------------------------------------------------------------------
# Result-dict accessors — transplanted verbatim from the spike sketch.
# These are part of the workbench's PUBLIC test surface (the private _r_*
# twins in shared.joins_lab are NOT imported here — D-18 only applies to the
# shared pure functions; the workbench accessors are new public API).
# ---------------------------------------------------------------------------


def r_sid(res):
    """Return the system-id string from a search result dict."""
    return (res.get("display") or {}).get("id") or res.get("sys_id") or ""


def r_shelf(res):
    """Return the shelfmark string from a search result dict."""
    d = res.get("display") or {}
    return d.get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?"


def r_title(res):
    """Return the title string from a search result dict."""
    return (res.get("display") or {}).get("title") or ""


def r_text(res):
    """Return the full text from a search result dict."""
    return res.get("full_text") or res.get("text") or ""


def r_lib(res):
    """Return the library code from a search result dict."""
    d = res.get("display") or {}
    return d.get("library_code") or d.get("library") or ""


# ---------------------------------------------------------------------------
# iiif_full — build a full-resolution IIIF image URL from a base URL.
# Source: spike sketch join_workbench.py.txt:151-157 (D-05 / RESEARCH verified).
# ---------------------------------------------------------------------------


def iiif_full(base_url, width=2000):
    """Build a full-resolution IIIF image URL.

    If base_url already ends with .jpg it is already a direct URL and is
    returned unchanged.  An empty / None base_url returns "".
    """
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url
    return f"{base_url}/full/{width},/0/default.jpg"


# ---------------------------------------------------------------------------
# meta_brief — one-line summary for the anchor panel (library · N img · title).
# tr('img') is bootstrapped in Plan-01 Task-1 (the closed i18n set).
# ---------------------------------------------------------------------------


def meta_brief(meta: dict, shelfmark: str = "") -> str:
    """Return a short summary string: library · N img · title (title truncated to 60)."""
    lib = meta.get("library_code", "") or ""
    n_img = len(meta.get("images_nli") or meta.get("images_ext") or [])
    title = (meta.get("title") or "")[:60]
    parts = [p for p in [lib, f"{n_img} {tr('img')}" if n_img else "", title] if p]
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# badge_for_source — source-provenance badge (D-09).
#
# CODEX-VERIFIED (107-REVIEWS.md must-fix #9): do NOT store tr("User") /
# tr("Community") / tr("Known join") at IMPORT time — that freezes the label
# in whatever language was active at import, so a later CURRENT_LANG switch
# (en <-> he) would not be reflected.  Instead store SOURCE KEYS in the config
# and call tr() AT CALL TIME inside badge_for_source / _label_for_source.
# ---------------------------------------------------------------------------

# Config stores (label_kind, light_bg, dark_bg).
#   label_kind: "literal:PGP" / "literal:FJMS"  => use the literal (proper noun, no tr())
#               "tr:User" / "tr:Community"       => resolve via tr() at call time
_BADGE_CONFIG = {
    "PGP":       ("literal:PGP",   "#0ea5e9", "#38bdf8"),
    "FJMS":      ("literal:FJMS",  "#8b5cf6", "#a78bfa"),
    "user":      ("tr:User",       "#10b981", "#34d399"),
    "community": ("tr:Community",  "#10b981", "#34d399"),
}


def _label_for_source(label_kind: str) -> str:
    """Resolve a badge label at call time (must-fix #9: no frozen import-time tr())."""
    if label_kind.startswith("literal:"):
        return label_kind.split(":", 1)[1]
    if label_kind.startswith("tr:"):
        return tr(label_kind.split(":", 1)[1])
    return label_kind


def badge_for_source(source: str, is_dark: bool) -> tuple:
    """Return (label, hex_color) for a join source string.

    Known sources: 'PGP', 'FJMS', 'user', 'community'.
    Any other / unknown source falls back to tr("Known join") + gray color.
    Labels for 'user' and 'community' are resolved via tr() at call time so
    a CURRENT_LANG switch is immediately reflected (must-fix #9).
    """
    cfg = _BADGE_CONFIG.get(source or "")
    if cfg is None:
        # Generic fallback (D-09); tr("Known join") resolved at call time.
        return tr("Known join"), ("#9ca3af" if is_dark else "#6b7280")
    label_kind, light_bg, dark_bg = cfg
    return _label_for_source(label_kind), (dark_bg if is_dark else light_bg)


# ---------------------------------------------------------------------------
# dedup_join_rows — merge join dicts from multiple sources, dedup by
# order-insensitive (a, b) pair.  Mirrors corrections_ui.py:3607
# _merge_fjms_joins_into_display (PATTERNS.md § "Known-joins dedup pattern").
# ---------------------------------------------------------------------------


def dedup_join_rows(join_lists: list) -> list:
    """Merge join dicts from multiple sources; dedup by order-insensitive (a,b) pair.

    Call order = priority: the FIRST source that supplies a pair wins its
    badge/source field.  Subsequent occurrences of the same pair are ignored.
    """
    seen = {}
    for joins in join_lists:
        for j in joins:
            a = (j.get("fragment_a") or "").upper().strip()
            b = (j.get("fragment_b") or "").upper().strip()
            key = (min(a, b), max(a, b))
            if key not in seen:
                seen[key] = j
    return list(seen.values())
