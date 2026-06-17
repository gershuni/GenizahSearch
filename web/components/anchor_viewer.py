# -*- coding: utf-8 -*-
"""
AnchorViewer — reusable fragment image viewer with zoom/pan, folio navigation,
and RTL numbered transcription.

Phase 117 Plan 06 (ANC-01 / ANC-02 / ANC-03).

Design invariants
-----------------
HIGH-1: Images are driven by the RICH ``web.services.BrowsePage`` from
    ``service.get_browse_page(...)`` — NOT by the narrow WebSearchExecutor
    Protocol dict (which lacks is_oxford/shelfmark/cambridge_images/…).

new-HIGH (round 2): ``service.get_browse_page()`` does NOT populate
    cambridge_images / external_provider / cambridge_alignment.  AnchorViewer
    ALSO calls ``resolve_external_images(sys_id)`` inside ``run.io_bound`` to
    populate those fields BEFORE calling ``resolve_image_url``.  Without this
    Cambridge/Manchester/JTS images would never resolve.

HIGH-2: ``resolve_image_url`` is the ONLY source of image URLs.  AnchorViewer
    wires NO ``onerror="handleImageError(...)"`` and carries no
    ``fetchFlIdsFromManifest`` / ``NLI_IIIF_BASE`` reference — bypassing the
    Phase-98 server-side NLI circuit breaker is structurally impossible.

Phase 119 Compare readiness: the head HTML (``_VIEWER_HEAD``) is injected
    behind a ``window._msViewerLoaded`` idempotency guard so two AnchorViewer
    instances on one page do NOT re-run ``createManuscriptViewer``.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from nicegui import run, ui

from web.components.image_resolution import resolve_external_images, resolve_image_url
from web.components.typography import render_line_numbered_html
from web.translations import is_rtl, tr

logger = logging.getLogger(__name__)

# ============================================================================
# Head HTML — manuscriptViewer CSS + JS init (injected at most once per page)
# ============================================================================

# The idempotency guard ``if (!window._msViewerLoaded)`` ensures that even if
# two AnchorViewer instances (Phase 119 Compare) both call
# ``ui.add_head_html(_VIEWER_HEAD)`` the ``createManuscriptViewer`` factory
# and the CSS are only actually executed / inserted once.
#
# NOTE: handleImageError / fetchFlIdsFromManifest / NLI_IIIF_BASE are defined
# in /static/manuscript_viewer.js (loaded by the <script src=…> tag).  They
# MUST NOT appear in AnchorViewer — see HIGH-2 invariant above.

_VIEWER_HEAD = '''
<script src="/static/manuscript_viewer.js"></script>
<script>
(function() {
    if (!window._msViewerLoaded) {
        window._msViewerLoaded = true;
        // Create viewer via shared factory — AnchorViewer uses .zoomable-image
        // and .image-container selectors, consistent with /browse.
        window.manuscriptViewer = createManuscriptViewer({
            imageSelector: '.zoomable-image',
            containerSelector: '.image-container',
            zoomLabelSelector: '.zoom-level-label',
            gammaFilterId: 'gamma-main'
        });
    }
})();
</script>
<style>
    /* Image viewer container */
    .anchor-viewer-container .image-container {
        position: relative;
        width: 100%;
        height: 65vh;
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden;
        background: linear-gradient(45deg, #1a1a1a 25%, #222 25%, #222 50%, #1a1a1a 50%, #1a1a1a 75%, #222 75%);
        background-size: 20px 20px;
        border-radius: 8px 8px 0 0;
    }

    .anchor-viewer-container .image-container img {
        max-width: 100%;
        max-height: 100%;
        object-fit: contain;
        transition: transform 0.2s ease-out;
        cursor: grab;
    }

    .anchor-viewer-container .image-container img:active {
        cursor: grabbing;
    }

    /* Loading skeleton */
    .anchor-viewer-skeleton {
        width: 100%;
        height: 65vh;
        background: linear-gradient(90deg, #2a2a2a 25%, #333 37%, #2a2a2a 63%);
        background-size: 400% 100%;
        animation: anchor-skeleton-shine 1.6s ease infinite;
        border-radius: 8px 8px 0 0;
    }

    @keyframes anchor-skeleton-shine {
        0%   { background-position: 100% 50%; }
        100% { background-position: -100% 50%; }
    }

    /* Image-error placeholder */
    .anchor-image-error {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 65vh;
        color: #888;
        background: #1a1a1a;
        border-radius: 8px 8px 0 0;
    }

    /* Controls bar */
    .anchor-controls-bar {
        display: flex;
        align-items: center;
        gap: 4px;
        padding: 6px 12px;
        background: rgba(30, 30, 30, 0.92);
        backdrop-filter: blur(6px);
        border-radius: 0 0 8px 8px;
    }

    /* Transcription panel */
    .anchor-transcription-panel {
        background: linear-gradient(to bottom, #fffef5, #fff9e6);
        border: 1px solid #e8e4d4;
        border-radius: 8px;
        max-height: 65vh;
        overflow-y: auto;
        padding: 20px 24px;
        margin-top: 8px;
    }
</style>
<svg style="position:absolute;width:0;height:0">
  <filter id="gamma-main">
    <feComponentTransfer>
      <feFuncR type="gamma" amplitude="1" exponent="1.0"/>
      <feFuncG type="gamma" amplitude="1" exponent="1.0"/>
      <feFuncB type="gamma" amplitude="1" exponent="1.0"/>
    </feComponentTransfer>
  </filter>
</svg>
'''


# ============================================================================
# AnchorViewer class
# ============================================================================

class AnchorViewer:
    """Reusable fragment image viewer with zoom/pan, folio nav, and RTL transcription.

    Inject ``browse_resolver`` and ``external_resolver`` in tests to avoid
    requiring a live AppState / meta_mgr.

    Args:
        sys_id:           Manuscript system ID (required to start).
        fl_id:            Optional fragment leaf ID (informational; not used for nav).
        p_num:            Initial page number (1-based).  None = first page.
        volume_ie:        Optional IE identifier for multi-IE manuscripts.
        browse_resolver:  Callable matching ``service.get_browse_page`` signature.
                          Defaults to the real AppState-backed singleton.
        external_resolver: Callable matching ``resolve_external_images`` signature.
                           Defaults to the shared helper from image_resolution.py.
    """

    def __init__(
        self,
        sys_id: str,
        fl_id: Optional[str] = None,
        p_num: Optional[int] = None,
        volume_ie: Optional[str] = None,
        browse_resolver: Optional[Callable] = None,
        external_resolver: Optional[Callable] = None,
    ) -> None:
        self._sys_id = sys_id
        self._fl_id = fl_id
        self._p_num: Optional[int] = p_num
        self._volume_ie = volume_ie

        # Inject real defaults lazily so the module can be imported without
        # a live AppState (test safety).
        if browse_resolver is None:
            from web.services import service as _svc
            browse_resolver = _svc.get_browse_page
        if external_resolver is None:
            external_resolver = resolve_external_images

        self._browse_resolver: Callable = browse_resolver
        self._external_resolver: Callable = external_resolver

        # Per-instance zoom state (mirrors browse.py state.zoom_level)
        self._zoom: float = 1.0

        # UI element references set during _build_ui()
        self._image_container: Optional[Any] = None
        self._controls_row: Optional[Any] = None
        self._transcription_container: Optional[Any] = None
        self._prev_btn: Optional[Any] = None
        self._next_btn: Optional[Any] = None

        # Inject head HTML with idempotency guard (safe to call multiple times)
        ui.add_head_html(_VIEWER_HEAD)

        # Build the component UI
        self._build_ui()

    # ──────────────────────────────────────────────────────────────────────────
    # Zoom helpers (per-instance state; mirror browse.py clamps)
    # ──────────────────────────────────────────────────────────────────────────

    def zoom_in(self) -> None:
        """Increase zoom by 0.25, clamped at 4.0."""
        self._zoom = min(self._zoom + 0.25, 4.0)
        ui.run_javascript(
            f"if(window.manuscriptViewer) {{ "
            f"window.manuscriptViewer.state.scale = {self._zoom}; "
            f"window.manuscriptViewer.applyTransform(); "
            f"window.manuscriptViewer.updateLabel(); }}"
        )

    def zoom_out(self) -> None:
        """Decrease zoom by 0.25, clamped at 0.25."""
        self._zoom = max(self._zoom - 0.25, 0.25)
        ui.run_javascript(
            f"if(window.manuscriptViewer) {{ "
            f"window.manuscriptViewer.state.scale = {self._zoom}; "
            f"window.manuscriptViewer.applyTransform(); "
            f"window.manuscriptViewer.updateLabel(); }}"
        )

    def zoom_reset(self) -> None:
        """Reset zoom to 1.0 and reset the viewer pan/rotation."""
        self._zoom = 1.0
        ui.run_javascript("if(window.manuscriptViewer) window.manuscriptViewer.reset();")

    # ──────────────────────────────────────────────────────────────────────────
    # Testable sync resolution core (no run.io_bound, no NiceGUI)
    # ──────────────────────────────────────────────────────────────────────────

    def _resolve_off_loop(
        self,
        p_num: Optional[int] = None,
        direction: int = 0,
    ) -> Optional[tuple]:
        """Synchronous resolution core — safe to call from a worker thread.

        Steps:
        1. Fetch the rich BrowsePage via the injected browse_resolver (HIGH-1).
        2. Call external_resolver(sys_id) to populate cambridge_images /
           external_provider / cambridge_alignment — which service.get_browse_page
           leaves empty (new-HIGH).
        3. Call resolve_image_url with the merged fields.

        Returns:
            (page, resolved_dict) tuple, or None if browse_resolver returned None
            (boundary / unknown sys_id).
        """
        page = self._browse_resolver(
            self._sys_id,
            p_num=p_num,
            direction=direction,
            volume_ie=self._volume_ie,
        )
        if page is None:
            return None

        # Populate the external-provider fields that service.get_browse_page
        # leaves at their dataclass defaults (empty list / '' / None).
        ext = self._external_resolver(page.sys_id)

        # Merge: prefer the enriched values; fall back to whatever the page
        # already carries (handles test scenarios where page has them pre-set).
        cambridge_images = ext.get('cambridge_images') or page.cambridge_images
        external_provider = ext.get('external_provider') or page.external_provider
        cambridge_alignment = (
            ext['cambridge_alignment']
            if ext.get('cambridge_alignment') is not None
            else page.cambridge_alignment
        )

        resolved = resolve_image_url(
            sys_id=page.sys_id,
            p_num=page.p_num,
            is_oxford=page.is_oxford,
            shelfmark=page.shelfmark,
            volume_suffix=page.volume_suffix,
            cambridge_images=cambridge_images,
            external_provider=external_provider,
            cambridge_alignment=cambridge_alignment,
            volumes=page.volumes,
            total_pages=page.total_pages,
        )
        return (page, resolved)

    # ──────────────────────────────────────────────────────────────────────────
    # Image HTML builder (testable — no NiceGUI render needed)
    # ──────────────────────────────────────────────────────────────────────────

    def _build_img_html(self, img_url: str) -> str:
        """Build the ``<img>`` HTML string for the resolved URL.

        HIGH-2 invariants (enforced structurally — NOT by runtime checks):
        - NO ``onerror="handleImageError(...)"``
        - NO ``iiif.nli.org.il`` URL
        - On image error the browser shows the native broken-image placeholder;
          the inline error-state UI (``_show_image_error()``) is set via the
          server-rendered state, not a client-side onerror handler.

        The only ``onload`` handler is ``window.manuscriptViewer.init()``,
        which wires zoom/pan to this image element — the same pattern as
        /browse (without the NLI fallback escape hatch).
        """
        return (
            f'<img src="{img_url}" '
            f'class="zoomable-image" '
            f'style="transform: translate(0px,0px) scale({self._zoom}); cursor: grab;" '
            f'draggable="false" '
            f'onload="if(window.manuscriptViewer) window.manuscriptViewer.init()" '
            f'/>'
        )

    # ──────────────────────────────────────────────────────────────────────────
    # UI construction
    # ──────────────────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        """Build the full AnchorViewer component (image + controls + transcription)."""
        rtl = is_rtl()
        direction_class = "flex-row-reverse" if rtl else "flex-row"

        with ui.column().classes("anchor-viewer-container w-full gap-0"):
            # Image container (shows skeleton initially)
            self._image_container = ui.element("div").classes("image-container relative")
            with self._image_container:
                self._skeleton = ui.element("div").classes("anchor-viewer-skeleton")
                self._img_html_elem: Optional[Any] = None
                self._error_elem: Optional[Any] = None

            # Controls bar (below image, full width)
            with ui.row().classes(f"anchor-controls-bar w-full justify-between {direction_class}"):
                # Folio navigation group
                with ui.row().classes("gap-1 items-center"):
                    self._prev_btn = (
                        ui.button(icon="chevron_left", on_click=self._on_prev_folio)
                        .props(f'flat round dense min-h-[44px] min-w-[44px] aria-label="{tr("Previous folio")}"')
                        .classes("text-white")
                    )
                    ui.tooltip(tr("Previous folio")).bind_visibility_from(self._prev_btn)

                    self._page_label = ui.label("…").classes("text-white text-sm px-2")

                    self._next_btn = (
                        ui.button(icon="chevron_right", on_click=self._on_next_folio)
                        .props(f'flat round dense min-h-[44px] min-w-[44px] aria-label="{tr("Next folio")}"')
                        .classes("text-white")
                    )
                    ui.tooltip(tr("Next folio")).bind_visibility_from(self._next_btn)

                # Zoom controls group
                with ui.row().classes("gap-1 items-center"):
                    _zoom_out_btn = (
                        ui.button(icon="remove", on_click=self.zoom_out)
                        .props(f'flat round dense min-h-[44px] min-w-[44px] aria-label="{tr("Zoom out")}"')
                        .classes("text-white")
                    )
                    ui.tooltip(tr("Zoom out")).bind_visibility_from(_zoom_out_btn)

                    ui.label("100%").classes("zoom-level-label text-white text-sm px-1")

                    _zoom_in_btn = (
                        ui.button(icon="add", on_click=self.zoom_in)
                        .props(f'flat round dense min-h-[44px] min-w-[44px] aria-label="{tr("Zoom in")}"')
                        .classes("text-white")
                    )
                    ui.tooltip(tr("Zoom in")).bind_visibility_from(_zoom_in_btn)

                    _zoom_reset_btn = (
                        ui.button(icon="fit_screen", on_click=self.zoom_reset)
                        .props(f'flat round dense min-h-[44px] min-w-[44px] aria-label="{tr("Reset zoom")}"')
                        .classes("text-white")
                    )
                    ui.tooltip(tr("Reset zoom")).bind_visibility_from(_zoom_reset_btn)

            # Transcription area
            self._transcription_container = ui.element("div").classes(
                "anchor-transcription-panel w-full"
            )
            with self._transcription_container:
                self._transcription_html = ui.html("", sanitize=False)

    # ──────────────────────────────────────────────────────────────────────────
    # Content update
    # ──────────────────────────────────────────────────────────────────────────

    async def update_content(
        self,
        p_num: Optional[int] = None,
        direction: int = 0,
    ) -> None:
        """Load (or navigate to) a folio, resolving images and transcription off-loop.

        Both ``service.get_browse_page`` and ``resolve_external_images`` perform
        I/O (Tantivy + breaker-guarded enrich_metadata) — they run together inside
        a single ``run.io_bound`` call so the NiceGUI event loop is never blocked.

        On boundary (browse_resolver returns None) the viewer shows a "not found"
        state without raising.

        Args:
            p_num:      Target page number (1-based).  None = use direction only.
            direction:  +1 = next folio, -1 = prev folio, 0 = stay / explicit p_num.
        """
        # Show loading skeleton while resolving
        self._show_loading()

        # Reset zoom on folio change (mirrors /browse behaviour)
        if direction != 0:
            self._zoom = 1.0

        # Capture resolution params for the worker closure
        _p_num = p_num
        _direction = direction

        def _resolve() -> Optional[tuple]:
            return self._resolve_off_loop(p_num=_p_num, direction=_direction)

        result = await run.io_bound(_resolve)

        if result is None:
            self._show_boundary()
            return

        page, resolved = result

        # Update page label
        self._p_num = page.p_num
        if page.total_pages:
            self._page_label.set_text(f"{page.p_num} / {page.total_pages}")
        else:
            self._page_label.set_text(str(page.p_num))

        # Render image or error placeholder
        if resolved.get("has_image") and resolved.get("img_url"):
            self._show_image(resolved["img_url"])
        else:
            self._show_image_error()

        # Render transcription
        html_text = render_line_numbered_html(
            text=page.text or "",
            show_line_numbers=True,
            line_height="2.2",
            font_size="1.4rem",
        )
        self._transcription_html.set_content(html_text)

    # ──────────────────────────────────────────────────────────────────────────
    # State display helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _show_loading(self) -> None:
        """Show the loading skeleton; hide image and error."""
        self._image_container.clear()
        with self._image_container:
            ui.element("div").classes("anchor-viewer-skeleton")

    def _show_image(self, img_url: str) -> None:
        """Replace skeleton with the actual image element."""
        self._image_container.clear()
        with self._image_container:
            # HIGH-2: NO onerror="handleImageError(...)".  NO iiif.nli.org.il URL.
            # Zoom init only via onload.
            img_html = self._build_img_html(img_url)
            ui.html(img_html, sanitize=False)
            ui.run_javascript(
                "if(window.manuscriptViewer) setTimeout(() => window.manuscriptViewer.init(), 50);"
            )

    def _show_image_error(self) -> None:
        """Show inline broken-image placeholder (no external fetch, no NLI fallback)."""
        self._image_container.clear()
        with self._image_container:
            with ui.column().classes("anchor-image-error w-full").style("height:65vh"):
                ui.icon("image_not_supported", size="4rem").style("color:#666")
                ui.label(tr("Image not available")).classes("text-sm mt-2")

    def _show_boundary(self) -> None:
        """Show 'not found' when browse_resolver returns None (boundary or bad sys_id)."""
        self._image_container.clear()
        with self._image_container:
            with ui.column().classes("anchor-image-error w-full").style("height:65vh"):
                ui.icon("find_in_page", size="4rem").style("color:#666")
                ui.label(tr("Fragment not found")).classes("text-sm mt-2")

    # ──────────────────────────────────────────────────────────────────────────
    # Folio navigation button handlers
    # ──────────────────────────────────────────────────────────────────────────

    async def _on_prev_folio(self) -> None:
        await self.update_content(direction=-1)

    async def _on_next_folio(self) -> None:
        await self.update_content(direction=+1)
