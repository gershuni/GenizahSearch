# -*- coding: utf-8 -*-
"""Candidate grid component for the Joins Lab (Phase 117 CND-02 / Phase 119 CND-03..07).

Phase 117: Renders a read-only grid of Candidate cards — thumbnail + shelfmark + library
chip + title + "View in Browse" link.

Phase 119 (this file): Extended with triage Y/?/N, pagination (24/page), large 160×160
thumbnails, table view (sortable 8-column multi-select), filter dialog, and 👁 VS badge.

Public API
----------
create_candidate_grid(candidates, *, on_browse_click=None, on_compare=None,
                      triage=None, page=0) -> ui.element
build_thumbnail_url(sys_id, page, shelfmark='', library_code='') -> str | None
build_browse_url(cand) -> str
paginate(all_candidates, page, page_size) -> (slice, current_page, total_pages)
compute_filtered(candidates, filter_state, enrichment, triage, anchor_sys_id) -> list
is_size_mismatch(candidate_width_cm, anchor_width_cm, threshold=1.4) -> bool
make_triage_state() -> TriageState
get_table_columns() -> list[dict]
get_table_config() -> dict

SECURITY BOUNDARY (T-117-07 / T-119-04/05/06/07):
  - Thumbnail URLs are proxy-only: /api/nli_image_by_sysid/... or Oxford Bodleian.
  - Never a direct iiif.nli.org.il URL.
  - Synthetic sys_ids return None → placeholder box rendered directly.
  - Image onerror stops at an inline placeholder; no handleImageError.
  - Triage/filter state is in-memory only — zero raw app.storage.user access (CI-guarded).
  - Nested links use js_handler='(e) => e.stopPropagation()' (AST guard enforced).

MULTITENANT (T-117-03 / T-119-07):
  - Zero raw app.storage.user access — CI-guarded by test_no_raw_storage_access.py.
"""
from __future__ import annotations

import json
import math
from typing import Optional, Callable

from nicegui import ui

from shared.joins_lab import TRIAGE_ICONS
from shared.synthetic_sys_id import is_synthetic_sys_id
from web.services import is_oxford_manuscript, get_oxford_direct_image_url
from web.translations import tr, get_language, is_rtl


# ---------------------------------------------------------------------------
# Pure helpers (importable without a NiceGUI runtime — for headless tests)
# ---------------------------------------------------------------------------

_TITLE_TRUNCATE_AT = 80
"""Characters at which to truncate candidate titles before appending '...'."""

_MAX_RENDERED_CANDIDATES = 200
"""Defensive cap — kept as a safety net but no longer the PRIMARY render bound.

Phase 119 (D-08): Pagination (24/page) is now the rendering bound. The 200-cap
remains as a fallback defensive net only. See _PAGE_SIZE and paginate().
"""

_PAGE_SIZE = 24
"""Candidates per page (D-08). Replaces _MAX_RENDERED_CANDIDATES as the primary bound."""

_PLACEHOLDER_STYLE = (
    "width:48px; height:48px; background:var(--bg-tertiary); "
    "border-radius:4px; display:flex; align-items:center; "
    "justify-content:center; color:var(--text-muted); font-size:18px; "
    "flex-shrink:0;"
)

_PLACEHOLDER_STYLE_160 = (
    "width:100%; height:160px; background:var(--bg-tertiary); "
    "border-radius:8px 8px 0 0; display:flex; align-items:center; "
    "justify-content:center; color:var(--text-muted); font-size:48px; "
    "flex-shrink:0;"
)


# ---------------------------------------------------------------------------
# Pagination (D-08 — replaces _MAX_RENDERED_CANDIDATES as the primary bound)
# ---------------------------------------------------------------------------

def paginate(
    all_candidates: list,
    page: int,
    page_size: int = _PAGE_SIZE,
) -> tuple:
    """Paginate a list of candidates.

    Pure function — headlessly testable.

    Args:
        all_candidates: The full (filtered) candidate list.
        page:           0-indexed page number. Clamped into [0, total_pages-1].
        page_size:      Items per page (default: _PAGE_SIZE = 24).

    Returns:
        (page_slice, clamped_page, total_pages)
        - page_slice:    The candidates for this page.
        - clamped_page:  The clamped page index (in case page was out of bounds).
        - total_pages:   Total number of pages (always >= 1).
    """
    total = len(all_candidates)
    if total == 0:
        return [], 0, 1
    total_pages = max(1, math.ceil(total / page_size))
    clamped = max(0, min(page, total_pages - 1))
    start = clamped * page_size
    return all_candidates[start:start + page_size], clamped, total_pages


# Keep the private alias for internal use
def _paginate(filtered: list, page: int = 0) -> tuple:
    """Internal alias for paginate() using the module-level _PAGE_SIZE."""
    return paginate(filtered, page, _PAGE_SIZE)


# ---------------------------------------------------------------------------
# Size-mismatch predicate (D-15, parity desktop join_workbench.py:1687-1695)
# ---------------------------------------------------------------------------

def is_size_mismatch(
    candidate_width_cm: Optional[float],
    anchor_width_cm: Optional[float],
    threshold: float = 1.4,
) -> bool:
    """Return True if candidate width differs from anchor width by more than threshold×.

    Pure function — headlessly testable.

    Formula (D-15, parity join_workbench.py:1687-1695):
        ratio = max(w, anchor_w) / min(w, anchor_w)
        mismatch = ratio > threshold

    Guards:
        - None on either side → False (no data → not flagged)
        - min == 0 → False (guards division by zero)
    """
    if candidate_width_cm is None or anchor_width_cm is None:
        return False
    if min(candidate_width_cm, anchor_width_cm) == 0:
        return False
    ratio = max(candidate_width_cm, anchor_width_cm) / min(candidate_width_cm, anchor_width_cm)
    return ratio > threshold


# Private alias for internal use (consistent with PATTERNS.md)
_is_size_mismatch = is_size_mismatch


# ---------------------------------------------------------------------------
# Triage state container (D-11 — in-memory, never written to safe_storage)
# ---------------------------------------------------------------------------

_VALID_VERDICTS = frozenset(("yes", "maybe", "no"))


class TriageState:
    """In-memory triage state keyed by sys_id.

    Values are 'yes' | 'maybe' | 'no'.  Never written to safe_storage
    (Phase 87 invariant — CI-guarded by test_no_raw_storage_access.py).
    Phase 120 adds persistence via safe_storage.

    CR-01 fix: accepts an optional ``backing`` dict so the page's ``_triage``
    dict and the TriageState share the SAME dict object — no copy, no drift.
    ``TriageState()`` with no args still works (existing tests call it that way).
    """

    def __init__(self, backing: dict | None = None) -> None:
        # Share the caller's dict directly when provided; otherwise create a new one.
        # This makes _triage (page-level) and TriageState._data the SAME object,
        # so verdicts set via either path are immediately visible to the other.
        self._data: dict = backing if backing is not None else {}

    def set(self, sys_id: str, verdict: str) -> None:
        """Set verdict for a sys_id. Raises ValueError for invalid verdicts."""
        if verdict not in _VALID_VERDICTS:
            raise ValueError(
                f"Invalid triage verdict {verdict!r}; must be one of {sorted(_VALID_VERDICTS)}"
            )
        self._data[sys_id] = verdict

    def set_bulk(self, sys_ids: list, verdict: str) -> None:
        """Set the same verdict for multiple sys_ids. Raises ValueError for invalid verdicts."""
        if verdict not in _VALID_VERDICTS:
            raise ValueError(
                f"Invalid triage verdict {verdict!r}; must be one of {sorted(_VALID_VERDICTS)}"
            )
        for sid in sys_ids:
            self._data[sid] = verdict

    def get(self, sys_id: str) -> Optional[str]:
        """Return the verdict for sys_id, or None if not triaged."""
        return self._data.get(sys_id)

    def reset(self) -> None:
        """Clear all triage verdicts (called on re-anchor / new search)."""
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, sys_id: str) -> bool:
        return sys_id in self._data


def make_triage_state() -> TriageState:
    """Factory for a fresh in-memory triage state object (D-11)."""
    return TriageState()


# ---------------------------------------------------------------------------
# Filter predicates (D-14 — apply before pagination per D-08)
# ---------------------------------------------------------------------------

def compute_filtered(
    all_candidates: list,
    filter_state: dict,
    enrichment: dict,
    triage: dict,
    anchor_sys_id: str,
) -> list:
    """Apply filter_state predicates to all_candidates and return the filtered list.

    Pure function — no closures over module globals; headlessly testable.

    Args:
        all_candidates: Full list of Candidate objects.
        filter_state:   Dict with keys:
                          - materials: list[str] — include only these materials (empty=all)
                          - has_dims: bool — require both width_cm and height_cm
                          - exclude_mismatch: bool — exclude size-mismatch candidates
                          - triage_states: list[str] — 'All'/'Not triaged'/'Yes'/'Maybe'/'No'
                          - text_q: str — case-insensitive substring on shelfmark+title
        enrichment:     dict[sys_id → {width_cm, height_cm, material, ...}]
        triage:         dict[sys_id → 'yes'|'maybe'|'no'] (or TriageState)
        anchor_sys_id:  sys_id of the anchor fragment (for size-mismatch comparison)

    Returns:
        Filtered list of Candidate objects (in the same order as all_candidates).
    """
    text_q = (filter_state.get("text_q") or "").strip().lower()
    materials = set(filter_state.get("materials") or [])
    has_dims = bool(filter_state.get("has_dims", False))
    exclude_mismatch = bool(filter_state.get("exclude_mismatch", False))
    triage_states = set(filter_state.get("triage_states") or [])

    # Look up anchor width for size-mismatch comparison
    anchor_w = enrichment.get(anchor_sys_id, {}).get("width_cm")

    # Support both TriageState objects and plain dicts
    def _get_verdict(sys_id: str) -> Optional[str]:
        if isinstance(triage, TriageState):
            return triage.get(sys_id)
        return triage.get(sys_id)

    out = []
    for c in all_candidates:
        # Text filter (case-insensitive substring on shelfmark + title)
        if text_q:
            shelfmark_l = (getattr(c, "shelfmark", None) or "").lower()
            title_l = (getattr(c, "title", None) or "").lower()
            if text_q not in shelfmark_l and text_q not in title_l:
                continue

        # Material filter
        m = enrichment.get(c.sys_id, {})
        if materials and m.get("material") not in materials:
            continue

        # Has-dims filter
        if has_dims and not (m.get("width_cm") and m.get("height_cm")):
            continue

        # Size-mismatch filter
        if exclude_mismatch and is_size_mismatch(m.get("width_cm"), anchor_w):
            continue

        # Triage-state filter (WR-03: simplified from three-branch form)
        if triage_states and "All" not in triage_states:
            verdict = _get_verdict(c.sys_id)
            verdict_key = verdict.capitalize() if verdict else "Not triaged"
            if verdict_key not in triage_states:
                continue

        out.append(c)
    return out


# ---------------------------------------------------------------------------
# Table configuration (D-10, CND-03)
# ---------------------------------------------------------------------------

def get_table_columns() -> list:
    """Return the 8-column table column definitions (D-10, CND-03).

    Web ADDS sortable columns — divergence from desktop setSortingEnabled(False) (D-10).
    Default sort: score descending. VS-rank ascending when 👁 ON.
    """
    return [
        {"name": "select", "label": "", "field": "sys_id", "sortable": False},
        {"name": "shelfmark", "label": tr("Shelfmark"), "field": "shelfmark", "sortable": True},
        {"name": "score", "label": tr("Score"), "field": "score", "sortable": True},
        {"name": "snippet", "label": tr("Snippet"), "field": "snippet", "sortable": True},
        {"name": "material", "label": tr("Material"), "field": "material", "sortable": True},
        {"name": "dimensions", "label": tr("Dimensions"), "field": "dimensions", "sortable": True},
        {"name": "page", "label": tr("Page"), "field": "page", "sortable": True},
        {"name": "triage", "label": tr("Triage"), "field": "triage", "sortable": False},
    ]


def get_table_config() -> dict:
    """Return table configuration dict (used by tests to assert multi-select etc.)."""
    return {
        "row_key": "uid",
        "selection": "multiple",
    }


# ---------------------------------------------------------------------------
# Table row builder (D-10, CND-03)
# ---------------------------------------------------------------------------

def _make_table_rows(
    candidates: list,
    triage: object,
    enrichment: dict,
    sort_mode: str = "score",
) -> list:
    """Build row dicts for the ui.table from a candidate list.

    Args:
        candidates:  List of Candidate objects.
        triage:      TriageState or dict[sys_id → verdict].
        enrichment:  dict[sys_id → {width_cm, height_cm, material, ...}].
        sort_mode:   'score' (default, desc) or 'vs_rank' (asc when 👁 ON).

    Returns:
        List of row dicts — each has all 8 column fields plus 'uid' for row identity.
    """
    from shared.joins_lab import badge_and_tooltip

    def _get_verdict(sys_id: str) -> Optional[str]:
        if isinstance(triage, TriageState):
            return triage.get(sys_id)
        return triage.get(sys_id) if isinstance(triage, dict) else None

    rows = []
    for c in candidates:
        m = enrichment.get(c.sys_id, {})
        icon_name, tooltip_text = badge_and_tooltip(c)
        badge_marker = f"[{icon_name}] " if icon_name else ""
        width_cm = m.get("width_cm")
        height_cm = m.get("height_cm")
        if width_cm is not None and height_cm is not None:
            dims = f"{width_cm:.1f}×{height_cm:.1f} cm"
        else:
            dims = "—"
        material = m.get("material") or "—"
        verdict = _get_verdict(c.sys_id)
        triage_glyph = {"yes": "✓", "maybe": "?", "no": "✗"}.get(verdict, "")
        snippet = getattr(c, "snippet", None) or ""
        if len(snippet) > 80:
            snippet = snippet[:80] + "..."

        score_val = getattr(c, "score", None)
        score_display = f"{score_val:.2f}" if score_val is not None else "—"
        vs_rank_val = getattr(c, "vs_rank", None)

        rows.append({
            "uid": c.uid,
            "sys_id": c.sys_id,
            "shelfmark": badge_marker + (c.shelfmark or "?"),
            "shelfmark_raw": c.shelfmark or "?",
            "score": score_display,
            "score_sort": score_val if score_val is not None else 0.0,
            "snippet": snippet,
            "material": material,
            "dimensions": dims,
            "page": c.page if c.page is not None else "—",
            "triage": triage_glyph,
            "vs_rank": vs_rank_val if vs_rank_val is not None else 9999,
        })

    # Sort
    if sort_mode == "vs_rank":
        rows.sort(key=lambda r: r["vs_rank"])
    else:
        # Default: score descending
        rows.sort(key=lambda r: r["score_sort"], reverse=True)

    return rows


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
        3. Everything else → /api/nli_image_by_sysid/{sys_id}?page=...&width=300 while
           NLI is UP; when the Phase-98 NLI breaker is OPEN (NLI down), route by
           library_code to the provider proxy (/api/cambridge_image, etc.) so CUDL/
           Manchester/JTS thumbnails still resolve during an NLI outage (SEED-010).
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

    # Non-Oxford providers (Cambridge/CUDL, Manchester, JTS) — SEED-010:
    # When NLI's image API is DOWN (Phase-98 breaker OPEN) route the thumbnail to
    # the provider's OWN proxy (resolved from the local crossref sidecar + CUDL/
    # LUNA/Figgy — independent of NLI) instead of the NLI proxy that would 404. The
    # provider proxies enrich-on-demand when their cache is cold (web/api.py). When
    # NLI is UP, keep the fast crossref-resolved NLI proxy (no enrich) so the common
    # path is unchanged and provider resolution is only paid during an actual outage.
    try:
        from shared import nli_circuit_breaker
        _nli_down = nli_circuit_breaker.is_open()
    except Exception:
        _nli_down = False
    if _nli_down and library_code:
        # Canonical library→proxy table (same one /api/browse uses, search_serializer).
        from shared.search_serializer import _BROWSE_PROXY_BY_LIBRARY, _BROWSE_DEFAULT_PROXY
        base, _provider = _BROWSE_PROXY_BY_LIBRARY.get(library_code, _BROWSE_DEFAULT_PROXY)
        return f"{base}/{sys_id}?page={page_idx}&width=300"
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


def cap_candidates(
    candidates: list,
    max_rendered: int = _MAX_RENDERED_CANDIDATES,
) -> tuple:
    """Decide how many candidate cards to render (WebSocket-safety cap).

    Pure helper (headlessly testable) so the render-cap decision is not buried
    in the NiceGUI render path.

    Returns:
        (to_render, total) where ``to_render`` is the (possibly sliced) list and
        ``total`` is the full count.  ``len(to_render) < total`` signals the
        caller to show a "showing first N" truncation notice.
    """
    total = len(candidates)
    if total > max_rendered:
        return candidates[:max_rendered], total
    return candidates, total


# ---------------------------------------------------------------------------
# Card-restyle infrastructure (D-11 / D-09 triage border feedback)
# ---------------------------------------------------------------------------

# R2-4: derive from the single source of truth in shared.joins_lab.TRIAGE_ICONS.
# This keeps the border-restyle logic (_make_restyle_fn) working unchanged while
# ensuring the card triage icons and table triage colors stay in sync.
_TRIAGE_COLORS = {k: v["color"] for k, v in TRIAGE_ICONS.items()}


def _make_restyle_fn(card_refs: dict, triage_btn_refs: dict | None = None):
    """Return a render-scoped restyle callable that operates on ``card_refs``.

    CR-04 fix: ``card_refs`` is a fresh dict created per ``create_candidate_grid``
    call, so refs from different user sessions never mix.  The returned function
    has the same public signature as the old module-level ``_restyle_all`` so all
    existing callers inside the same render pass work unchanged.

    Args:
        card_refs:       Render-local dict mapping sys_id → list[ui.card].
        triage_btn_refs: Optional render-local dict mapping sys_id → list of per-card
                         ``{verdict → ui.button}`` dicts.  Phase-120 D (UAT 2026-06-21):
                         when provided, a verdict change ALSO updates the V/?/X triage
                         button active-fill on the matching card(s) — not just the
                         border — so a verdict set from the Compare modal is reflected
                         on the grid immediately (without a full re-render).

    Returns:
        A callable ``restyle(sys_id, triage)`` bound to this render's refs.
    """
    def _restyle_all(sys_id: str, triage: object) -> None:
        """Update every visible card whose sys_id matches — apply triage border (D-11).

        Desktop parity: _restyle_card at join_workbench.py:3344.

        Also updates the V/?/X triage button active-fill on the matching card(s)
        (Phase-120 D) so a Compare verdict propagates to the grid buttons immediately.

        Args:
            sys_id: The candidate sys_id whose triage verdict changed.
            triage: TriageState or dict[sys_id → verdict].
        """
        if isinstance(triage, TriageState):
            verdict = triage.get(sys_id)
        else:
            verdict = triage.get(sys_id) if isinstance(triage, dict) else None

        color = _TRIAGE_COLORS.get(verdict)
        border = f"2px solid {color}" if color else "1px solid var(--border-light)"
        for ref in card_refs.get(sys_id, []):
            try:
                ref.style(f"border-radius:8px; border:{border};")
            except Exception:
                pass

        # Phase-120 D: update the triage button active-fill on matching cards so a
        # verdict change (e.g. from the Compare modal) is reflected on the grid's
        # ✓/?/✗ buttons immediately — mirrors the in-card _make_triage_handler logic.
        if triage_btn_refs is not None:
            for btn_map in triage_btn_refs.get(sys_id, []):
                for _v, _btn in btn_map.items():
                    try:
                        if _v == verdict:
                            _c = _TRIAGE_COLORS.get(_v, "")
                            _btn.style(
                                f"min-height:44px; font-size:0.85rem; "
                                f"background:{_c}; color:#fff;"
                            )
                        else:
                            # Round-4 Issue 6: element.style() UPDATES named props but
                            # does NOT remove a previously-set background — so the prior
                            # verdict button would stay lit.  Explicitly reset the fill
                            # so only the active verdict button is highlighted.
                            _btn.style(
                                "min-height:44px; font-size:0.85rem; "
                                "background:transparent; color:inherit;"
                            )
                    except Exception:
                        pass

    return _restyle_all


# ---------------------------------------------------------------------------
# Private card renderer (Phase 119 — 160×160 image-first, triage row, badge, Compare)
# ---------------------------------------------------------------------------

def _create_candidate_card(
    cand,
    triage: object = None,
    on_browse_click: Optional[Callable] = None,
    on_compare: Optional[Callable] = None,
    *,
    card_refs: dict | None = None,
    triage_btn_refs: dict | None = None,
    restyle_fn: Optional[Callable] = None,
    on_set_as_anchor: Optional[Callable] = None,
    on_add_as_join: Optional[Callable] = None,
    on_add_to_puzzle: Optional[Callable] = None,
    on_card_select: Optional[Callable] = None,
    on_triage_change: Optional[Callable] = None,
    selected: bool = False,
) -> None:
    """Render a single candidate card (Phase 119 D-09/D-11/D-07, Phase 120 D-07/ACT-01).

    Card layout (reading order):
        ┌──────────────────────────────────────────┐
        │ [thumbnail 160×160, full-width]          │  ← rounded-top, object-fit:cover
        │ [library chip] [shelfmark]               │
        │ [title — RTL, 2-line clamp, muted]       │
        │ [👁 badge]   [triage: Y ? N]             │  ← new Phase 119
        │ [View in Browse] [Compare fragment]      │  ← Compare button new Phase 119
        └──────────────────────────────────────────┘

    Security (T-119-04/05/06):
        - Thumbnail URL via build_thumbnail_url() (proxy-only; Oxford fork preserved)
        - Text fields rendered via ui.label (auto-escaped; no .html())
        - Nested links use js_handler='(e) => e.stopPropagation()' (AST guard)
        - Triage state is the caller-provided dict/TriageState — never safe_storage

    CR-04: ``card_refs`` is a render-scoped dict passed from ``create_candidate_grid``.
    ``restyle_fn(sys_id, triage)`` is the closure returned by ``_make_restyle_fn``.
    Callers that omit them fall back to a fresh local dict (cards can't be restyled
    from outside, but no cross-session leak occurs).
    """
    from shared.joins_lab import badge_and_tooltip, snippet_html

    if triage is None:
        triage = {}

    # CR-04: use render-scoped card_refs; fall back to a local (non-shared) dict.
    _refs = card_refs if card_refs is not None else {}

    thumb_url = build_thumbnail_url(
        cand.sys_id,
        cand.page,
        shelfmark=cand.shelfmark,
        library_code=cand.library_code,
    )

    # Derive current verdict for initial render (IN-01: both branches are identical)
    current_verdict = triage.get(cand.sys_id) if triage else None

    # Initial border reflects current triage state
    color = _TRIAGE_COLORS.get(current_verdict)
    initial_border = f"2px solid {color}" if color else "1px solid var(--border-light)"

    # ── Compare handler — hoisted so image click can reuse it (G4) ──────────
    # Carries the FULL candidate (uid / sys_id+page) to on_compare.
    # NOT keyed by sys_id alone — same sys_id can appear on multiple folios.
    _cand_ref = cand  # explicit closure capture

    def _make_compare_handler(c=_cand_ref, handler=on_compare):
        def _handler():
            if handler:
                handler(c)
        return _handler

    with ui.card().classes("w-full p-0").style(
        f"border-radius:8px; border:{initial_border}; overflow:hidden;"
    ) as card_el:
        # Register card ref for restyle (keyed by sys_id — D-11, render-scoped)
        _refs.setdefault(cand.sys_id, []).append(card_el)

        # ── Thumbnail (160×160, full-width, rounded-top) ──────────────
        if thumb_url:
            # Proxy image with inline onerror → placeholder (NO handleImageError,
            # NO direct-IIIF fallback).  T-117-07 / T-119-04 boundary.
            # G4: click opens Compare for this candidate; cursor:pointer signals clickability.
            img_el = ui.image(thumb_url).style(
                "width:100%; height:160px; object-fit:cover;"
                "border-radius:8px 8px 0 0; flex-shrink:0; display:block; cursor:pointer;"
            )
            img_el.on("click", _make_compare_handler())
            # Replace with a placeholder box on load error (no IIIF fallback).
            img_el.on(
                "error",
                js_handler=(
                    "(e) => {"
                    " e.target.style.display='none';"
                    " const ph=document.createElement('div');"
                    " ph.innerHTML='&#128196;';"
                    " ph.setAttribute('style','" + _PLACEHOLDER_STYLE_160.replace("'", "\\'") + "');"
                    " e.target.parentNode.insertBefore(ph,e.target);"
                    "}"
                ),
            )
        else:
            # Synthetic sys_id: render placeholder directly.
            # G4: placeholder is also clickable → opens Compare.
            (
                ui.element("div")
                .style(_PLACEHOLDER_STYLE_160 + " cursor:pointer;")
                .html("&#128196;")
                .on("click", _make_compare_handler())
            )

        # ── Metadata column (below thumbnail) ─────────────────────────
        with ui.column().classes("flex-grow min-w-0 gap-2 p-2"):

            # Library chip + shelfmark row
            with ui.row().classes("items-center gap-2 flex-wrap"):
                # Round-4 Issue 8: per-card selection checkbox.  Toggles the
                # candidate's sys_id in the page-level _selected set (same set the
                # table feeds), so SELECTION-scoped bulk actions (Add-to-Puzzle /
                # Add-to-List) work from the grid too.  Keyed by sys_id to match the
                # table's selection semantics.
                if on_card_select is not None:
                    _sel_sid = cand.sys_id

                    def _make_select_handler(sid=_sel_sid, _fn=on_card_select):
                        def _h(e) -> None:
                            # ValueChangeEventArguments carries .value; be defensive
                            # and fall back to .args (raw payload) so the callback
                            # always receives the correct boolean.
                            val = getattr(e, "value", None)
                            if val is None:
                                val = getattr(e, "args", None)
                            try:
                                _fn(sid, bool(val))
                            except Exception:
                                pass
                        return _h

                    (
                        ui.checkbox(value=selected)
                        .props("dense")
                        .classes("shrink-0")
                        .on_value_change(_make_select_handler())
                        .tooltip(tr("Select for bulk actions"))
                    )

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

            # Transcription snippet with highlighted search terms (G1 — CND-03)
            # T-119-05: ONLY snippet_html()/htmlify() output passes to ui.html(sanitize=False).
            # These helpers escape corpus text first, then inject <b style='color:#dc2626'>
            # highlight spans — corpus text never reaches the DOM unescaped.
            _snippet_source = getattr(cand, "full_text", None) or getattr(cand, "snippet", None) or ""
            _highlight_pattern = getattr(cand, "highlight_pattern", None)
            if _snippet_source:
                _snippet_rendered = snippet_html(_snippet_source, _highlight_pattern)
                ui.html(_snippet_rendered, sanitize=False).style(
                    "direction:rtl; text-align:right; "
                    "color:var(--text-secondary); font-size:0.75rem; "
                    "overflow:hidden; display:-webkit-box; "
                    "-webkit-line-clamp:3; -webkit-box-orient:vertical;"
                )

            # 👁 badge (Phase 119 VSM-02) — badge_and_tooltip() precedence
            # T-119-05: rendered via ui.icon (auto-escaped; not .html())
            icon_name, tooltip_text = badge_and_tooltip(cand)
            if icon_name:
                ui.icon(icon_name).classes("text-sm").style(
                    "color: #f59e0b;"  # var(--accent-amber)
                ).tooltip(tr(tooltip_text))

            # Triage + action row (R2-4 / R2-9 — Phase 119-10)
            # ✓ / ? / ✗ icon-glyph buttons with Mark-yes/maybe/no tooltips (desktop parity);
            # browse + compare icon-only buttons in the SAME row (R2-9).
            # G3: render-local per-card button refs so the click handler can update
            # fills immediately — not only when the grid is later rebuilt.
            # T-119-07: refs are per-card local dict, never module-global.
            _triage_btn_refs: dict[str, object] = {}  # verdict → button element

            browse_url = build_browse_url(cand)
            with ui.row().classes("gap-1 items-center justify-between w-full"):
                # ── left cluster: ✓ / ? / ✗ triage icon-glyph buttons ────────
                with ui.row().classes("gap-1 items-center"):
                    for verdict in ("yes", "maybe", "no"):
                        _v = verdict  # closure capture
                        _sid = cand.sys_id
                        _v_icon = TRIAGE_ICONS[verdict]
                        _glyph = _v_icon["glyph"]
                        _v_color = _v_icon["color"]
                        _tooltip_key = _v_icon["tooltip"]

                        # Active state: filled background in triage color
                        if current_verdict == verdict:
                            btn_style = (
                                f"min-height:44px; font-size:0.85rem; "
                                f"background:{_v_color}; color:#fff;"
                            )
                        else:
                            btn_style = "min-height:44px; font-size:0.85rem;"

                        def _make_triage_handler(
                            v=_v, sid=_sid, t=triage, _rf=restyle_fn,
                            _btn_refs=_triage_btn_refs, _otc=on_triage_change,
                        ):
                            def _handler():
                                if isinstance(t, TriageState):
                                    t.set(sid, v)
                                elif isinstance(t, dict):
                                    t[sid] = v
                                if _rf is not None:
                                    _rf(sid, t)
                                # G3: immediately update triage button fills (not only on grid rebuild)
                                for _verdict, _btn in _btn_refs.items():
                                    try:
                                        if _verdict == v:
                                            _c = _TRIAGE_COLORS.get(_verdict, "")
                                            _btn.style(
                                                f"min-height:44px; font-size:0.85rem; "
                                                f"background:{_c}; color:#fff;"
                                            )
                                        else:
                                            # Round-4 Issue 6: explicitly clear the prior
                                            # verdict's fill — element.style() does NOT
                                            # remove a previously-set background, so without
                                            # this the previously-clicked button stays lit
                                            # (all three could be highlighted at once).
                                            _btn.style(
                                                "min-height:44px; font-size:0.85rem; "
                                                "background:transparent; color:inherit;"
                                            )
                                    except Exception:
                                        pass
                                # Round-5: notify the page so the verdict is PERSISTED
                                # (the grid mutates the triage dict locally; without this
                                # callback _persist_state never runs → verdict lost on
                                # restore). Best-effort — never break the click handler.
                                if _otc is not None:
                                    try:
                                        _otc(sid, v)
                                    except Exception:
                                        pass
                            return _handler

                        _btn_el = (
                            ui.button(_glyph)
                            .props("flat dense")
                            .style(btn_style)
                            .tooltip(tr(_tooltip_key))
                            .on("click", _make_triage_handler())
                        )
                        _triage_btn_refs[verdict] = _btn_el

                # Phase-120 D: register this card's triage-button map into the
                # render-scoped dict so the restyle fn can update the ✓/?/✗ active
                # fill when a verdict changes elsewhere (e.g. from the Compare modal).
                if triage_btn_refs is not None:
                    triage_btn_refs.setdefault(cand.sys_id, []).append(_triage_btn_refs)

                # ── right cluster: browse + compare icon-only buttons (R2-9) ─
                with ui.row().classes("gap-1 items-center"):
                    # Browse icon button — navigates to /browse deep-link.
                    # T-119-R9: json.dumps-escaped JS literal (no server-side stop_propagation).
                    if on_browse_click:
                        # Test-hook branch: client-side navigation via js_handler.
                        (
                            ui.button(icon="menu_book")
                            .props("flat dense")
                            .style("min-height:44px;")
                            .tooltip(tr("View in Browse"))
                            .on(
                                "click",
                                js_handler=(
                                    f"() => {{ window.location.href={json.dumps(browse_url)}; }}"
                                ),
                            )
                        )
                    else:
                        # Normal branch: plain client-side navigation.
                        (
                            ui.button(icon="menu_book")
                            .props("flat dense")
                            .style("min-height:44px;")
                            .tooltip(tr("View in Browse"))
                            .on(
                                "click",
                                js_handler=(
                                    f"() => {{ window.location.href={json.dumps(browse_url)}; }}"
                                ),
                            )
                        )

                    # Compare icon button (Phase 119 CND-04, D-02, R2-9)
                    # Reuses the hoisted _make_compare_handler (defined above the card)
                    # which carries the FULL candidate. NOT keyed by sys_id alone —
                    # same sys_id can appear on multiple folios.
                    (
                        ui.button(icon="compare_arrows")
                        .props("flat dense")
                        .style("min-height:44px;")
                        .tooltip(tr("Compare fragment"))
                        .on("click", _make_compare_handler())
                    )

                    # Phase-120 D-07: Set as Anchor — pivots the workbench in place
                    # (triage resets per 119 D-11; no confirm dialog — not destructive).
                    if on_set_as_anchor is not None:
                        _sid_capture = cand.sys_id

                        def _make_anchor_handler(sid=_sid_capture, _fn=on_set_as_anchor):
                            def _h():
                                _fn(sid)
                            return _h

                        (
                            ui.button(icon="push_pin")
                            .props("flat dense")
                            .style("min-height:44px;")
                            .tooltip(tr("Pivot the workbench: make this fragment the new anchor"))
                            .on("click", _make_anchor_handler())
                        )

                    # Phase-120 ACT-01: Add as Join — community write, login-gated.
                    # The callback handles anonymous/auth branching; the card passes
                    # the candidate sys_id + shelfmark to the handler.
                    if on_add_as_join is not None:
                        _aj_sid = cand.sys_id
                        _aj_sm = cand.shelfmark or "?"

                        def _make_add_join_handler(sid=_aj_sid, sm=_aj_sm, _fn=on_add_as_join):
                            def _h():
                                _fn(sid, sm)
                            return _h

                        (
                            ui.button(icon="add_link")
                            .props("flat dense color=primary")
                            .style("min-height:44px;")
                            .tooltip(tr("Add as Join"))
                            .on("click", _make_add_join_handler())
                        )

                    # Round-4 Issue 7: Add to Puzzle — stages the ANCHOR + this ONE
                    # candidate into the puzzle and navigates to /puzzle.  The
                    # callback (joins_lab._on_add_candidate_to_puzzle_click) builds the
                    # [anchor, candidate] puzzle_staging payload.
                    if on_add_to_puzzle is not None:
                        _ap_sid = cand.sys_id

                        def _make_add_puzzle_handler(sid=_ap_sid, _fn=on_add_to_puzzle):
                            def _h():
                                _fn(sid)
                            return _h

                        (
                            ui.button(icon="extension")
                            .props("flat dense")
                            .style("min-height:44px;")
                            .mark("grid_card_add_to_puzzle")
                            .tooltip(tr("Add anchor + this candidate to the Fragment Puzzle"))
                            .on("click", _make_add_puzzle_handler())
                        )


# ---------------------------------------------------------------------------
# Filter dialog (D-14 — opens as ui.dialog popover, D-14/D-15 predicates)
# ---------------------------------------------------------------------------

def open_filter_dialog(
    filter_state: dict,
    enrichment: dict,
    enrichment_ready: bool,
    on_apply: Callable,
    on_reset: Callable,
) -> None:
    """Open the candidate filter dialog (D-14, CND-06).

    Args:
        filter_state:     Current filter state dict (mutated in place on Apply).
        enrichment:       dict[sys_id → {material, width_cm, height_cm, ...}]
        enrichment_ready: True when enrichment batch has completed.
        on_apply:         Called after Apply closes the dialog.
        on_reset:         Called after Reset closes the dialog.
    """
    # Gather material options from enrichment (disabled until enrichment ready — Pitfall 7)
    mat_options = sorted({v.get("material") for v in enrichment.values() if v.get("material")})

    with ui.dialog() as dlg, ui.card().classes("p-4 gap-4").style("min-width:360px;"):
        ui.label(tr("Filters")).classes("text-base font-semibold")

        # 1. Material (multi-select; disabled until enrichment ready)
        mat_sel = ui.select(
            options=mat_options,
            value=list(filter_state.get("materials") or []),
            label=tr("Material"),
            multiple=True,
        ).props("dense outlined use-chips")
        if not enrichment_ready:
            mat_sel.disable()

        # 2. Has dimensions
        dims_sw = ui.switch(tr("Has dimensions data"), value=bool(filter_state.get("has_dims", False)))

        # 3. Size mismatch (disabled until enrichment ready — Pitfall 7)
        mismatch_sw = ui.switch(tr("Exclude size mismatch"), value=bool(filter_state.get("exclude_mismatch", False)))
        if not enrichment_ready:
            mismatch_sw.disable()

        # 4. Triage state (multi-select)
        triage_opts = ["All", "Not triaged", "Yes", "Maybe", "No"]
        tri_sel = ui.select(
            options=triage_opts,
            value=list(filter_state.get("triage_states") or []),
            label=tr("Triage state"),
            multiple=True,
        ).props("dense outlined use-chips")

        # 5. Text filter (D-14 discretion — included per UI-SPEC)
        text_inp = ui.input(
            placeholder=tr("Filter by shelfmark…"),
            value=filter_state.get("text_q", ""),
        ).props("dense outlined")

        with ui.row().classes("justify-end gap-2 mt-2"):
            def _do_reset():
                filter_state.update({
                    "materials": [],
                    "has_dims": False,
                    "exclude_mismatch": False,
                    "triage_states": [],
                    "text_q": "",
                })
                dlg.close()
                on_reset()

            def _do_apply():
                filter_state["materials"] = list(mat_sel.value or [])
                filter_state["has_dims"] = bool(dims_sw.value)
                filter_state["exclude_mismatch"] = bool(mismatch_sw.value)
                filter_state["triage_states"] = list(tri_sel.value or [])
                filter_state["text_q"] = text_inp.value or ""
                dlg.close()
                on_apply()

            ui.button(tr("Reset"), on_click=_do_reset).props("flat")
            ui.button(tr("Apply"), on_click=_do_apply).props("color=primary unelevated")

    dlg.open()


# ---------------------------------------------------------------------------
# Table view renderer (D-10, CND-03 — sortable 8-column multi-select table)
# ---------------------------------------------------------------------------

def create_candidate_table(
    candidates: list,
    triage: object = None,
    enrichment: dict = None,
    sort_mode: str = "score",
    on_compare: Optional[Callable] = None,
    restyle_fn: Optional[Callable] = None,
    on_selection_change: Optional[Callable] = None,
    on_add_to_puzzle: Optional[Callable] = None,
    on_add_to_list: Optional[Callable] = None,
    on_add_as_join: Optional[Callable] = None,
    on_triage_change: Optional[Callable] = None,
) -> ui.element:
    """Render the multi-select sortable table view of candidates (D-10, CND-03).

    Args:
        candidates:        Filtered (not paginated) list of Candidate objects.
        triage:            TriageState or dict[sys_id → verdict].
        enrichment:        dict[sys_id → {material, width_cm, height_cm, ...}]
        sort_mode:         'score' (default, desc) or 'vs_rank' (asc when 👁 ON).
        on_compare:        Optional callback(cand) launched on row double-click.
        restyle_fn:        Optional render-scoped restyle callable from _make_restyle_fn().
                           When None, bulk-triage verdict changes are recorded in triage but
                           the visual border update is deferred until the next full re-render.
        on_selection_change: Optional Callable[[list], None] — called from the selection
                           handler with the current list of selected sys_ids.  Defaults to
                           None (Phase-118/119 callers without it are unaffected — backward
                           compat).  Phase-120 H2: joins_lab.py passes this to wire the
                           table selection into the page-level ``_selected`` set so
                           SELECTION-scoped bulk actions (Add-to-Puzzle / Add-to-List)
                           see the same selection.
        on_add_to_puzzle:  Optional Callable[[], None] — invoked when the user clicks
                           "Add to Puzzle" in the bulk action bar.  The caller (joins_lab.py)
                           reads its own ``_selected`` set to build the payload.  Defaults to
                           None (Phase-118/119 callers without it are unaffected).
                           Phase-120 ACT-02: the button appears in the bulk bar alongside
                           the existing triage buttons (TABLE view only — R2-H2).
        on_add_to_list:    Optional Callable[[], None] — invoked when the user clicks
                           "Add to List" in the bulk action bar.  The caller (joins_lab.py)
                           checks login state and opens the list-picker or login dialog.
                           Defaults to None (Phase-118/119/120-05 callers unaffected).
                           Phase-120 ACT-03/D-05: login-gated; anonymous users see lock icon.
        on_add_as_join:    Optional Callable[[sys_id, shelfmark], None] — invoked when the
                           user clicks "Add as Join" in the bulk action bar.  Add-as-Join is
                           PAIRWISE (anchor + exactly ONE candidate), so the button is ENABLED
                           only when exactly ONE row is selected and disabled otherwise.  The
                           single selected row's sys_id + shelfmark are passed to the callback.
                           Defaults to None (callers without it are unaffected — backward compat).

    Returns:
        The outer ui.element wrapping the table and bulk-triage bar.
    """
    if triage is None:
        triage = {}
    if enrichment is None:
        enrichment = {}

    rows = _make_table_rows(candidates, triage, enrichment, sort_mode=sort_mode)
    columns = get_table_columns()

    with ui.column().classes("w-full gap-2") as outer:
        # Bulk-triage bar (D-12) — appears when rows are selected
        bulk_bar = ui.row().classes("gap-2 items-center p-2 flex-wrap").style(
            "background:var(--bg-tertiary); border-radius:4px; display:none;"
        )
        selected_sys_ids: list = []
        # Track the selected rows (sys_id + raw shelfmark) so the single-select
        # Add-as-Join button can resolve the candidate's shelfmark without the
        # badge marker prefix (the table column's "shelfmark" field is prefixed).
        selected_rows: list = []

        with bulk_bar:
            bulk_count_label = ui.label(tr("Mark N selected as:").replace("N", "0"))
            for verdict, label, v_color in [
                ("yes", tr("Yes"), _TRIAGE_COLORS["yes"]),
                ("maybe", tr("Maybe"), _TRIAGE_COLORS["maybe"]),
                ("no", tr("No"), _TRIAGE_COLORS["no"]),
            ]:
                _v = verdict

                def _make_bulk_handler(v=_v, _rf=restyle_fn, _otc=on_triage_change):
                    def _handler():
                        if isinstance(triage, TriageState):
                            triage.set_bulk(selected_sys_ids, v)
                        elif isinstance(triage, dict):
                            for sid in selected_sys_ids:
                                triage[sid] = v
                        if _rf is not None:
                            for sid in selected_sys_ids:
                                _rf(sid, triage)
                        # Round-5: persist the bulk verdict change (else lost on restore).
                        if _otc is not None:
                            try:
                                _otc(None, v)
                            except Exception:
                                pass
                    return _handler

                ui.button(label).props("flat dense").style(
                    f"color:{v_color};"
                ).on("click", _make_bulk_handler())

            # Phase-120 ACT-01: Add as Join — PAIRWISE (anchor + exactly ONE
            # candidate).  Enabled only when exactly one row is selected; the
            # selection handler toggles its enabled state.  On click it resolves
            # the single selected row's sys_id + raw shelfmark and calls the
            # callback (which handles the anonymous/auth + confirm branching).
            #
            # Round-5 NOTE: Add-to-Puzzle / Add-to-List for the SELECTION are no
            # longer rendered here.  They moved to the page-level toolbar bar
            # (next to Export) so they work identically in BOTH grid and table
            # view (the table feeds the same page-level _selected set via
            # on_selection_change).  on_add_to_puzzle / on_add_to_list remain in
            # the signature for back-compat but are intentionally unused here.
            add_join_btn = None  # late-bound; toggled by the selection handler
            if on_add_as_join is not None:
                with ui.row().classes("ml-auto items-center gap-2"):
                    def _on_add_join_bulk_click() -> None:
                        if len(selected_rows) != 1:
                            return
                        row = selected_rows[0]
                        sid = row.get("sys_id")
                        sm = row.get("shelfmark_raw") or "?"
                        if sid:
                            on_add_as_join(sid, sm)

                    add_join_btn = ui.button(
                        tr("Add as Join"),
                        icon="add_link",
                        on_click=_on_add_join_bulk_click,
                    ).props("flat color=primary disable")
                    add_join_btn.tooltip(
                        tr("Select exactly one candidate to add as a join")
                    )

        table = ui.table(
            columns=columns,
            rows=rows,
            row_key="uid",
            selection="multiple",
        ).classes("w-full joins-candidate-table")

        # Row double-click → Compare (D-02)
        if on_compare:
            def _on_row_dblclick(e):
                uid = e.args.get("uid") or e.args.get("key")
                # Find the matching candidate by uid
                for c in candidates:
                    if getattr(c, "uid", None) == uid:
                        on_compare(c)
                        break
            table.on("rowDblclick", _on_row_dblclick)

        # Track selection for bulk-triage bar
        def _on_selection(e):
            sel = e.args if isinstance(e.args, list) else []
            selected_sys_ids.clear()
            selected_rows.clear()
            for row in sel:
                sys_id = row.get("sys_id")
                if sys_id:
                    selected_sys_ids.append(sys_id)
                    selected_rows.append(row)
            n = len(selected_sys_ids)
            bulk_count_label.set_text(tr("Mark N selected as:").replace("N", str(n)))
            bulk_bar.style("display:flex;" if n > 0 else "display:none;")
            # Phase-120 ACT-01: Add-as-Join is PAIRWISE — enable only on exactly one
            # selected row; disable for 0 or >1 (toggle tooltip to explain the gate).
            if add_join_btn is not None:
                if n == 1:
                    add_join_btn.props(remove="disable")
                    add_join_btn.tooltip(tr("Add as Join"))
                else:
                    add_join_btn.props("disable")
                    add_join_btn.tooltip(
                        tr("Select exactly one candidate to add as a join")
                    )
            # Phase-120 H2: notify the page-level selection callback so
            # SELECTION-scoped bulk actions (Add-to-Puzzle / Add-to-List) see the
            # current selection.  Backward-compat: skipped when None.
            if on_selection_change is not None:
                try:
                    on_selection_change(list(selected_sys_ids))
                except Exception:
                    pass  # never crash the render path from a callback error

        table.on("selection", _on_selection)

    return outer


# ---------------------------------------------------------------------------
# Public grid factory (Phase 119 — paginated, triage-aware, 160×160 thumbnails)
# ---------------------------------------------------------------------------

def create_candidate_grid(
    candidates: list,
    *,
    on_browse_click: Optional[Callable] = None,
    on_compare: Optional[Callable] = None,
    triage: object = None,
    page: int = 0,
    # Phase 119 integration kwargs (Plan 04 — joins_lab page wiring)
    enrichment: Optional[dict] = None,
    enrichment_ready: bool = False,
    filter_state: Optional[dict] = None,
    anchor_sys_id: str = '',
    on_page_change: Optional[Callable] = None,
    on_filter_open: Optional[Callable] = None,
    on_restyle_ready: Optional[Callable] = None,
    # Phase 120 D-07: Set-as-Anchor (pivots workbench in place)
    on_set_as_anchor: Optional[Callable] = None,
    # Phase 120 ACT-01: Add-as-Join (community write, login-gated)
    on_add_as_join: Optional[Callable] = None,
    # Round-4 Issue 7: per-card Add-to-Puzzle (anchor + this candidate)
    on_add_to_puzzle: Optional[Callable] = None,
    # Round-4 Issue 8: per-card selection toggle (sys_id, is_selected) → page _selected set
    on_card_select: Optional[Callable] = None,
    selected_sys_ids: Optional[set] = None,
    # Round-5: per-card triage verdict change (sys_id, verdict) → page persist hook.
    # WITHOUT this the grid mutates the triage dict locally but never persists, so
    # Y/?/X verdicts are lost on a session restore.
    on_triage_change: Optional[Callable] = None,
) -> ui.element:
    """Render a paginated candidate grid with triage, 👁 badge, and Compare hook.

    Args:
        candidates:       Full filtered list of shared.joins_lab.Candidate objects.
        on_browse_click:  Optional Python callback for browse links (test hook).
        on_compare:       Optional callback(cand) for the Compare button — receives the
                          FULL candidate (uid/(sys_id,page)) NOT just sys_id (D-02).
        triage:           TriageState or dict[sys_id → verdict]. Shared single source
                          of truth across grid, table, Compare (D-11).
        page:             0-indexed page number to render.
        enrichment:       dict[sys_id → {material, width_cm, height_cm, ...}] (D-16).
        enrichment_ready: True once enrichment batch has completed (Pitfall 7).
        filter_state:     Current filter dialog state dict (D-14).
        anchor_sys_id:    The anchor's sys_id — excluded from self-match display (D-13).
        on_page_change:   Callback(page: int) for Prev/Next pagination clicks (D-08).
        on_filter_open:   Callback() for the Filter button click (D-14).
        on_restyle_ready: Optional callback(restyle_fn) invoked with the render-scoped
                          restyle function immediately after the grid is built (WR-01).
                          The page stores this so Compare verdicts can restyle grid cards.

    Returns:
        The outer ui.column() element wrapping the section header + grid + pagination.
    """
    if triage is None:
        triage = {}
    if enrichment is None:
        enrichment = {}
    if filter_state is None:
        filter_state = {}

    # CR-04: per-render card_refs dict — never module-global, never shared across users.
    _render_card_refs: dict = {}
    # Phase-120 D: per-render triage-button refs — sys_id → list of {verdict → button}
    # maps, one per card.  Passed to the restyle fn so a Compare verdict updates the
    # grid's ✓/?/✗ button fills immediately (UAT 2026-06-21).
    _render_triage_btn_refs: dict = {}
    _render_restyle = _make_restyle_fn(_render_card_refs, _render_triage_btn_refs)

    total = len(candidates)
    page_slice, current_page, total_pages = paginate(candidates, page)

    with ui.column().classes("w-full gap-2") as outer:

        if not candidates:
            # Empty state
            ui.label(
                tr("No candidates found. Try different lines or broader terms.")
            ).classes("text-sm").style("color: var(--text-secondary);")
        else:
            # Section header row: candidate count + Filter button (D-14)
            with ui.row().classes("w-full items-center justify-between"):
                ui.label(f"{tr('Candidates')} ({total})").classes(
                    "text-base font-semibold"
                )
                if on_filter_open is not None:
                    ui.button(
                        icon='filter_list',
                        on_click=on_filter_open,
                    ).props('flat dense round').tooltip(tr('Filter candidates'))

            # Responsive grid (D-09): 1 col <640px, 2 col 640–1023px, 3 col ≥1024px.
            with ui.grid().classes("w-full gap-2 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3"):
                _sel_set = selected_sys_ids if selected_sys_ids is not None else set()
                for cand in page_slice:
                    _create_candidate_card(
                        cand,
                        triage=triage,
                        on_browse_click=on_browse_click,
                        on_compare=on_compare,
                        card_refs=_render_card_refs,
                        triage_btn_refs=_render_triage_btn_refs,
                        restyle_fn=_render_restyle,
                        on_set_as_anchor=on_set_as_anchor,
                        on_add_as_join=on_add_as_join,
                        on_add_to_puzzle=on_add_to_puzzle,
                        on_card_select=on_card_select,
                        on_triage_change=on_triage_change,
                        selected=cand.sys_id in _sel_set,
                    )

            # Pagination controls (D-08): ‹ Prev | Page N of M | Next ›
            if total_pages > 1:
                _cp = current_page  # capture for closures

                def _prev_click(_cp=_cp):
                    if on_page_change is not None and _cp > 0:
                        on_page_change(_cp - 1)

                def _next_click(_cp=_cp, _tp=total_pages):
                    if on_page_change is not None and _cp < _tp - 1:
                        on_page_change(_cp + 1)

                # RTL: reverse the row for Hebrew (app idiom — joins_lab.py:1169 —
                # it does NOT rely on browser flex auto-flip), so ‹ Prev (הקודם) is
                # on the RIGHT and Next › (הבא) on the LEFT (round-5 UAT).
                _pg_dir = "flex-row-reverse" if is_rtl() else "flex-row"
                with ui.row().classes(
                    f"w-full items-center justify-center gap-2 mt-2 {_pg_dir}"
                ):
                    ui.button(tr("‹ Prev"), on_click=_prev_click).props("flat dense").props(
                        "disable" if current_page == 0 else ""
                    )
                    ui.label(tr("Page N of M").replace("N", str(current_page + 1)).replace("M", str(total_pages))).classes("text-sm")
                    ui.button(tr("Next ›"), on_click=_next_click).props("flat dense").props(
                        "disable" if current_page >= total_pages - 1 else ""
                    )

    # WR-01: expose the render-scoped restyle fn to the caller so Compare verdicts
    # can update card borders without a full re-render.
    if on_restyle_ready is not None:
        try:
            on_restyle_ready(_render_restyle)
        except Exception:
            pass

    return outer
