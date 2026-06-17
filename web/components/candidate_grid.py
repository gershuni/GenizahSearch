# -*- coding: utf-8 -*-
"""Candidate grid component for the Joins Lab (Phase 117 CND-02).

Renders a read-only grid of Candidate cards — thumbnail + shelfmark + library chip
+ title + "View in Browse" link.  Phase 119 will extend this component with triage
Y/?/N, table view, and VS badges without requiring a structural rewrite (D-14, D-04).

Public API
----------
create_candidate_grid(candidates, *, on_browse_click=None) -> ui.element
build_thumbnail_url(sys_id, page, shelfmark='', library_code='') -> str | None
build_browse_url(cand) -> str

SECURITY BOUNDARY (T-117-07):
  - Thumbnail URLs are proxy-only: /api/nli_image_by_sysid/... or Oxford Bodleian.
  - Never a direct iiif.nli.org.il URL.
  - Synthetic sys_ids return None → placeholder box rendered directly.
  - Image onerror stops at an inline placeholder; no handleImageError.

MULTITENANT (T-117-03):
  - Zero raw app.storage.user access — CI-guarded by test_no_raw_storage_access.py.

SCOPE LOCK (D-14):
  - No triage state, no checkboxes, no Compare, no VS.  Those are Phase 119.
"""
from __future__ import annotations

from typing import Optional, Callable

from nicegui import ui

from shared.synthetic_sys_id import is_synthetic_sys_id
from web.services import is_oxford_manuscript, get_oxford_direct_image_url
from web.translations import tr, get_language


# ---------------------------------------------------------------------------
# Pure helpers (importable without a NiceGUI runtime — for headless tests)
# ---------------------------------------------------------------------------

_TITLE_TRUNCATE_AT = 80
"""Characters at which to truncate candidate titles before appending '...'."""

_PLACEHOLDER_STYLE = (
    "width:48px; height:48px; background:var(--bg-tertiary); "
    "border-radius:4px; display:flex; align-items:center; "
    "justify-content:center; color:var(--text-muted); font-size:18px; "
    "flex-shrink:0;"
)


def build_thumbnail_url(
    sys_id: str,
    page: Optional[int],
    shelfmark: str = "",
    library_code: str = "",
) -> Optional[str]:
    """Derive a proxy thumbnail URL for a candidate.

    Mirrors search_results.py:645-681 IN FULL (MEDIUM-6 — includes Oxford fork).

    Returns:
        A proxy URL string, or None for synthetic sys_ids (no proxy image exists).

    Rules:
        1. Synthetic sys_ids → None (placeholder path only).
        2. Oxford manuscripts (is_oxford_manuscript) → Bodleian direct URL when
           derivable, else /api/oxford_image/{sys_id}?page=...  NOT the NLI proxy.
        3. Everything else → /api/nli_image_by_sysid/{sys_id}?page=...&width=300.
        4. NEVER a direct iiif.nli.org.il URL.
    """
    page_idx = max(0, (page or 1) - 1)

    # Synthetic sys_ids: the proxy returns 204 (no image).  Skip URL entirely.
    if is_synthetic_sys_id(sys_id):
        return None

    # Oxford fork (MEDIUM-6): direct Bodleian URL or /api/oxford_image proxy.
    try:
        is_oxford = is_oxford_manuscript(shelfmark, library_code)
    except Exception:
        is_oxford = False

    if is_oxford:
        try:
            ox_url = get_oxford_direct_image_url(shelfmark, page_idx)
        except Exception:
            ox_url = ""
        if ox_url:
            return ox_url  # Direct Bodleian (documented MEDIUM-5 exception)
        return f"/api/oxford_image/{sys_id}?page={page_idx}"

    # NLI default (all other providers — Cambridge/Manchester/JTS defer to Phase 119/CND-08
    # async enrichment, exactly as the search result card does).
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300"


def build_browse_url(cand) -> str:
    """Build the /browse deep-link URL for a candidate.

    Includes &page=N only when cand.page is set.
    """
    url = f"/browse?sys_id={cand.sys_id}"
    if cand.page is not None:
        url += f"&page={cand.page}"
    return url


def _truncate_title(title: str, max_chars: int = _TITLE_TRUNCATE_AT) -> str:
    """Truncate a title to max_chars and append '...' if needed."""
    if not title:
        return ""
    if len(title) > max_chars:
        return title[:max_chars] + "..."
    return title


# ---------------------------------------------------------------------------
# Private card renderer
# ---------------------------------------------------------------------------

def _create_candidate_card(cand, on_browse_click: Optional[Callable] = None) -> None:
    """Render a single read-only candidate card.

    Card layout (reading order):
        ┌──────────────────────────────────────┐
        │ [thumbnail 48×48]                    │
        │ [library chip] [shelfmark]           │
        │ [title — RTL, truncated]             │
        │ [View in Browse link]                │
        └──────────────────────────────────────┘

    Phase 119 will add a triage row below the title without restructuring.
    """
    thumb_url = build_thumbnail_url(
        cand.sys_id,
        cand.page,
        shelfmark=cand.shelfmark,
        library_code=cand.library_code,
    )

    with ui.card().classes("w-full p-4").style(
        "border-radius: 8px; border: 1px solid var(--border-light);"
    ):
        # ── Thumbnail ──────────────────────────────────────────────────
        with ui.row().classes("items-start gap-3"):
            if thumb_url:
                # Proxy image with inline onerror → placeholder (NO handleImageError,
                # NO direct-IIIF fallback).  T-117-07 boundary.
                img_el = ui.image(thumb_url).style(
                    "width:48px; height:48px; object-fit:cover; border-radius:4px; flex-shrink:0;"
                )
                # Replace with a placeholder box on load error (no IIIF fallback).
                img_el.on(
                    "error",
                    js_handler=(
                        "(e) => {"
                        " e.target.style.display='none';"
                        " const ph=document.createElement('div');"
                        " ph.innerHTML='&#128196;';"
                        " ph.setAttribute('style','" + _PLACEHOLDER_STYLE.replace("'", "\\'") + "');"
                        " e.target.parentNode.insertBefore(ph,e.target);"
                        "}"
                    ),
                )
            else:
                # Synthetic sys_id: render placeholder directly.
                ui.element("div").style(_PLACEHOLDER_STYLE).html("&#128196;")

            # ── Metadata column (library chip + shelfmark + title + link) ──
            with ui.column().classes("flex-grow min-w-0 gap-1"):

                # Library chip + shelfmark row
                with ui.row().classes("items-center gap-2 flex-wrap"):
                    if cand.library_code:
                        from genizah_core import get_library_display
                        full_name = get_library_display(
                            cand.library_code, short=False, lang=get_language()
                        )
                        ui.label(cand.library_code).classes(
                            "text-xs px-2 py-0.5 rounded shrink-0"
                        ).style(
                            "background: var(--primary-100); color: var(--primary-700);"
                        ).tooltip(full_name)

                    ui.label(cand.shelfmark or "?").classes(
                        "text-sm font-semibold truncate"
                    )

                # Title (RTL, truncated, muted)
                if cand.title:
                    title_display = _truncate_title(cand.title)
                    ui.label(title_display).classes("text-sm").style(
                        "color: var(--text-secondary); direction: rtl; "
                        "overflow: hidden; display: -webkit-box; "
                        "-webkit-line-clamp: 2; -webkit-box-orient: vertical;"
                    )

                # "View in Browse" link
                browse_url = build_browse_url(cand)
                if on_browse_click:
                    # Caller provided a Python handler (for testing / customisation).
                    ui.link(tr("View in Browse"), "#").style(
                        "color: var(--primary-700); font-size: 0.85rem;"
                    ).on("click", js_handler=f"() => {{ window.location.href='{browse_url}'; }}")
                else:
                    ui.link(tr("View in Browse"), browse_url).style(
                        "color: var(--primary-700); font-size: 0.85rem;"
                    )


# ---------------------------------------------------------------------------
# Public grid factory
# ---------------------------------------------------------------------------

def create_candidate_grid(
    candidates: list,
    *,
    on_browse_click: Optional[Callable] = None,
) -> ui.element:
    """Render a read-only deduped candidate grid.

    Args:
        candidates: List of shared.joins_lab.Candidate objects.
        on_browse_click: Optional Python callback called when a browse link is
            clicked (useful for testing; if omitted the link navigates directly).

    Returns:
        The outer ui.column() element wrapping the section header + grid.

    Phase 119 extension points:
        - Add a triage row inside `_create_candidate_card` below the title.
        - Replace the grid with a table by wrapping this call from the page.
        - Add VS badges / per-card action buttons.
    """
    with ui.column().classes("w-full gap-3") as outer:

        if not candidates:
            # Empty state
            ui.label(
                tr("No candidates found. Try different lines or broader terms.")
            ).classes("text-sm").style("color: var(--text-secondary);")
        else:
            # Section header with count
            ui.label(f"{tr('Candidates')} ({len(candidates)})").classes(
                "text-base font-semibold"
            )

            # Responsive 2-column grid: single column on narrow (<640px).
            with ui.grid(columns=2).classes("w-full gap-3").style(
                "@media (max-width:639px) { grid-template-columns: 1fr; }"
            ):
                for cand in candidates:
                    _create_candidate_card(cand, on_browse_click=on_browse_click)

    return outer
