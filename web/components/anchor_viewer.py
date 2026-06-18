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
from web.translations import tr

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
        /* Hug the image instead of forcing a tall 65vh box (which left big
           empty striped bands around landscape double-folio scans). */
        height: auto;
        max-height: 72vh;
        min-height: 200px;
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
        height: 52vh;
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

    /* Transcription panel — light cream background, so force DARK text.
       Without this the body inherited the dark-theme's light text colour
       and rendered white-on-cream (illegible). */
    .anchor-transcription-panel {
        background: linear-gradient(to bottom, #fffef5, #fff9e6);
        border: 1px solid #e8e4d4;
        border-radius: 8px;
        max-height: 65vh;
        overflow-y: auto;
        padding: 20px 24px;
        margin-top: 8px;
        color: #2d2d2d;
    }
    .anchor-transcription-panel .line-numbered-body-row,
    .anchor-transcription-panel .transcription-text {
        color: #2d2d2d;
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


def inject_viewer_assets() -> None:
    """Inject the manuscriptViewer JS + CSS into the page ``<head>``.

    CRITICAL: call this during INITIAL page render (at the top of the page
    builder), NOT lazily when an AnchorViewer is constructed in response to a
    user click. ``<script>`` tags added to an already-live NiceGUI SPA page via
    ``ui.add_head_html`` do NOT execute (only ``<style>`` applies) — so a
    dynamically-constructed viewer would render its image and CSS but
    ``window.manuscriptViewer`` would never be created and zoom/pan would be
    dead. The ``window._msViewerLoaded`` guard makes repeat calls safe, so a
    page hosting two viewers (Phase 119 Compare) can call this once at build.
    """
    ui.add_head_html(_VIEWER_HEAD)


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
            # web/services.py exposes the singleton via get_service() — there is
            # no module-level `service` symbol (browse.py binds `service =
            # get_service()` the same way). Importing `service` directly raised
            # ImportError at runtime only, since every unit test injects a resolver.
            from web.services import get_service
            browse_resolver = get_service().get_browse_page
        if external_resolver is None:
            external_resolver = resolve_external_images

        self._browse_resolver: Callable = browse_resolver
        self._external_resolver: Callable = external_resolver

        # Per-instance zoom state (mirrors browse.py state.zoom_level)
        self._zoom: float = 1.0

        # Latest-wins folio-navigation generation (WR-03): a newer
        # update_content() supersedes any in-flight one so rapid prev/next
        # clicks can never render a stale folio. Mirrors the search path's
        # generation-counter discipline (web/pages/joins_lab.py).
        self._nav_gen: int = 0

        # UI element references set during _build_ui()
        self._image_container: Optional[Any] = None
        self._controls_row: Optional[Any] = None
        self._transcription_container: Optional[Any] = None
        self._prev_btn: Optional[Any] = None
        self._next_btn: Optional[Any] = None
        self._zoom_label: Optional[Any] = None
        # Info header (shelfmark + library + title) — populated by update_content.
        self._info_header: Optional[Any] = None
        self._shelfmark_label: Optional[Any] = None
        self._meta_label: Optional[Any] = None
        # Last successfully-rendered image URL — used to restore the view when a
        # folio navigation hits a boundary / sparse page (don't destroy the view).
        self._last_img_url: Optional[str] = None

        # NOTE: the manuscriptViewer JS/CSS is injected by inject_viewer_assets()
        # at PAGE-BUILD time (see web/pages/joins_lab.py), NOT here — a viewer
        # constructed dynamically (on a user click) cannot execute a freshly
        # injected <script>. Calling it here too would only duplicate the CSS
        # and never run the script. See inject_viewer_assets() docstring.

        # Build the component UI
        self._build_ui()

    # ──────────────────────────────────────────────────────────────────────────
    # Zoom helpers (per-instance state; mirror browse.py clamps)
    # ──────────────────────────────────────────────────────────────────────────

    def zoom_in(self) -> None:
        """Increase zoom by 0.25, clamped at 4.0."""
        self._zoom = min(self._zoom + 0.25, 4.0)
        self._apply_zoom()

    def zoom_out(self) -> None:
        """Decrease zoom by 0.25, clamped at 0.25."""
        self._zoom = max(self._zoom - 0.25, 0.25)
        self._apply_zoom()

    def zoom_reset(self) -> None:
        """Reset zoom to 1.0 and reset the viewer pan/rotation."""
        self._zoom = 1.0
        ui.run_javascript("if(window.manuscriptViewer) window.manuscriptViewer.reset();")
        self._apply_zoom()

    def _apply_zoom(self) -> None:
        """Push the current zoom level to the client.

        Mirrors browse.py's proven path: drive the shared ``manuscriptViewer``
        via ``update(scale, rotation)`` (which sets the scale and re-applies the
        transform). The % label is updated SERVER-SIDE here rather than relying
        on the viewer's JS ``updateLabel()`` selector. A direct-transform
        fallback keeps zoom working even if the viewer object never initialised.
        """
        if self._zoom_label is not None:
            self._zoom_label.set_text(f"{int(self._zoom * 100)}%")
        ui.run_javascript(
            "(function(){"
            "  var mv = window.manuscriptViewer;"
            "  if (mv && typeof mv.update === 'function') {"
            f"    mv.update({self._zoom}, (mv.state ? mv.state.rotation : 0));"
            "  } else {"
            "    var im = document.querySelector('.anchor-viewer-container .zoomable-image');"
            f"    if (im) im.style.transform = 'translate(0px,0px) scale({self._zoom})';"
            "  }"
            "})();"
        )

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
        with ui.column().classes("anchor-viewer-container w-full gap-0"):
            # Info header — shelfmark (identifier) + library + title. Populated
            # by update_content. Uses theme CSS vars so it stays legible in both
            # light and dark themes, and follows the page RTL direction so Hebrew
            # library/title metadata reads correctly under a Hebrew UI.
            self._info_header = ui.column().classes("anchor-info-header w-full gap-0").style(
                "padding: 2px 4px 6px;"
            )
            with self._info_header:
                self._shelfmark_label = (
                    ui.label("").classes("text-base font-semibold").style(
                        "color: var(--text-primary); line-height: 1.3;"
                    )
                )
                self._meta_label = (
                    ui.label("").classes("text-xs").style(
                        "color: var(--text-secondary); line-height: 1.3;"
                    )
                )

            # Image container (shows skeleton initially)
            self._image_container = ui.element("div").classes("image-container relative")
            with self._image_container:
                self._skeleton = ui.element("div").classes("anchor-viewer-skeleton")
                self._img_html_elem: Optional[Any] = None
                self._error_elem: Optional[Any] = None

            # Controls bar (below image, full width).
            # Forced LTR so the media-control layout is stable regardless of the
            # app's RTL direction: prev (<) on the left, next (>) on the right,
            # zoom group on the far side. This matches the universal pager/player
            # convention scholars expect (clicking > advances), and avoids the
            # RTL flex-reversal that made the arrows read backwards (UAT round 2).
            with ui.row().classes("anchor-controls-bar w-full justify-between").style(
                "direction: ltr;"
            ):
                # Folio navigation group
                with ui.row().classes("gap-1 items-center"):
                    self._prev_btn = (
                        ui.button(icon="chevron_left", on_click=self._on_prev_folio)
                        .props(f'flat round dense aria-label="{tr("Previous folio")}"')
                        .classes("text-white min-h-[44px] min-w-[44px]")
                    )
                    ui.tooltip(tr("Previous folio")).bind_visibility_from(self._prev_btn)

                    self._page_label = ui.label("…").classes("text-white text-sm px-2")

                    self._next_btn = (
                        ui.button(icon="chevron_right", on_click=self._on_next_folio)
                        .props(f'flat round dense aria-label="{tr("Next folio")}"')
                        .classes("text-white min-h-[44px] min-w-[44px]")
                    )
                    ui.tooltip(tr("Next folio")).bind_visibility_from(self._next_btn)

                # Zoom controls group
                with ui.row().classes("gap-1 items-center"):
                    _zoom_out_btn = (
                        ui.button(icon="remove", on_click=self.zoom_out)
                        .props(f'flat round dense aria-label="{tr("Zoom out")}"')
                        .classes("text-white min-h-[44px] min-w-[44px]")
                    )
                    ui.tooltip(tr("Zoom out")).bind_visibility_from(_zoom_out_btn)

                    self._zoom_label = ui.label("100%").classes(
                        "zoom-level-label text-white text-sm px-1"
                    )

                    _zoom_in_btn = (
                        ui.button(icon="add", on_click=self.zoom_in)
                        .props(f'flat round dense aria-label="{tr("Zoom in")}"')
                        .classes("text-white min-h-[44px] min-w-[44px]")
                    )
                    ui.tooltip(tr("Zoom in")).bind_visibility_from(_zoom_in_btn)

                    _zoom_reset_btn = (
                        ui.button(icon="fit_screen", on_click=self.zoom_reset)
                        .props(f'flat round dense aria-label="{tr("Reset zoom")}"')
                        .classes("text-white min-h-[44px] min-w-[44px]")
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
        # Latest-wins guard (WR-03): claim a generation token; a newer
        # navigation started after this one supersedes it.
        self._nav_gen += 1
        my_gen = self._nav_gen

        # Show loading skeleton while resolving
        self._show_loading()

        # Reset zoom on folio change (mirrors /browse behaviour)
        if direction != 0:
            self._zoom = 1.0

        # Capture resolution params for the worker closure.
        #
        # NAVIGATION FIX (UAT round 3): the core get_browse_page is STATELESS —
        # to move by ``direction`` it must be told the CURRENT folio so it can
        # compute ``index(p_num) + direction``. An explicit ``p_num`` (initial
        # load / jump-to-page) wins; otherwise navigate RELATIVE to the folio we
        # are currently showing (``self._p_num``). This mirrors the proven
        # /browse path (web/pages/browse.py:792-798). Without it every prev/next
        # restarted from index 0, so the viewer could only ever reach folio 2
        # ("advance once, then stuck, then 'no more folios'").
        _p_num = p_num if p_num is not None else self._p_num
        _direction = direction

        def _resolve() -> Optional[tuple]:
            return self._resolve_off_loop(p_num=_p_num, direction=_direction)

        result = await run.io_bound(_resolve)

        # Discard stale results: a newer navigation began while we awaited.
        if my_gen != self._nav_gen:
            return

        if result is None:
            if direction != 0 and self._last_img_url:
                # Folio-navigation boundary (before the first / past the last
                # folio) or a sparse/metadata-only page: keep the current view
                # instead of destroying it with a "not found" state.
                ui.notify(tr('No more folios in this direction'), type='info')
                self._show_image(self._last_img_url)
            else:
                # Initial load (or bad sys_id) with no prior image — show boundary.
                self._show_boundary()
            return

        page, resolved = result

        # Update page label
        self._p_num = page.p_num
        if page.total_pages:
            self._page_label.set_text(f"{page.p_num} / {page.total_pages}")
        else:
            self._page_label.set_text(str(page.p_num))

        # Info header — shelfmark + library + title. These come already
        # localized from get_browse_page() (via get_language()), so the library
        # name and title render in Hebrew under a Hebrew UI with no extra
        # translation. The shelfmark is a language-neutral identifier.
        if self._shelfmark_label is not None:
            self._shelfmark_label.set_text(page.shelfmark or page.sys_id or "")
        if self._meta_label is not None:
            meta_bits = [
                b for b in (
                    getattr(page, "library_name", "") or "",
                    getattr(page, "title", "") or "",
                )
                if b
            ]
            self._meta_label.set_text(" · ".join(meta_bits))

        # Disable nav at the boundaries so the user cannot navigate off either
        # end. Use current_idx (the DENSE 1-based ordinal in the page list), not
        # p_num — p_num can be sparse/non-contiguous, which would mis-compute the
        # boundary. Mirrors /browse (browse.py:3774 / :3827).
        cur_idx = getattr(page, "current_idx", 0) or 0
        if self._prev_btn is not None:
            self._prev_btn.set_enabled(cur_idx > 1)
        if self._next_btn is not None:
            self._next_btn.set_enabled(
                not page.total_pages or cur_idx < page.total_pages
            )

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
        self._last_img_url = img_url
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
