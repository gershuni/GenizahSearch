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

import html as _html_module
import logging
import re
from typing import Any, Callable, Optional

from nicegui import run, ui

from shared.joins_lab import MARK_A, MARK_B
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
        window.__msViewers = window.__msViewers || {};
        // SEED-010: per-instance viewer registry. Each AnchorViewer carries a
        // UNIQUE container class (.avcN); __msInitViewer creates a manuscriptViewer
        // scoped to THAT container so Compare's two panes (plus the page behind the
        // modal) each get their own zoom/pan wiring. The old single global viewer +
        // document.querySelector('.zoomable-image') wired only the FIRST image, so
        // the Compare panes' zoom was dead.
        window.__msInitViewer = function(vid) {
            if (typeof createManuscriptViewer !== 'function') return;
            if (!window.__msViewers[vid]) {
                window.__msViewers[vid] = createManuscriptViewer({
                    imageSelector: '.' + vid + ' .zoomable-image',
                    containerSelector: '.' + vid + ' .image-container',
                    zoomLabelSelector: '.' + vid + ' .zoom-level-label',
                    gammaFilterId: 'gamma-main'
                });
            }
            window.__msViewers[vid].init();
        };
        // Back-compat global (consistent with /browse; some fallbacks reference it).
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

    /* SEED-017 (#10): the image + controls bar live inside .anchor-image-pane so
       the native Fullscreen API can expand BOTH (the transcription stays out of
       fullscreen — full-bleed image, like /browse). Native :fullscreen escapes the
       Compare dialog's stacking/transform context; a position:fixed overlay would
       be trapped inside the maximized ui.dialog. Specificity (.anchor-image-pane +
       :fullscreen + .image-container = 3) beats the base
       .anchor-viewer-container .image-container rule (2). */
    .anchor-image-pane:-webkit-full-screen {
        background: #000;
        display: flex;
        flex-direction: column;
        justify-content: center;
        width: 100vw;
        height: 100vh;
    }
    .anchor-image-pane:fullscreen {
        background: #000;
        display: flex;
        flex-direction: column;
        justify-content: center;
        width: 100vw;
        height: 100vh;
    }
    /* !important is required: Compare sets an inline `max-height: {image_max_height}`
       (e.g. 40vh) on .image-container, and an inline style beats a stylesheet rule.
       Without !important the Compare panes would stay capped at 40vh in fullscreen. */
    .anchor-image-pane:-webkit-full-screen .image-container {
        max-height: calc(100vh - 56px) !important;
        height: calc(100vh - 56px) !important;
        min-height: 0;
        flex: 1 1 auto;
        border-radius: 0;
    }
    .anchor-image-pane:fullscreen .image-container {
        max-height: calc(100vh - 56px) !important;
        height: calc(100vh - 56px) !important;
        min-height: 0;
        flex: 1 1 auto;
        border-radius: 0;
    }
    .anchor-image-pane:-webkit-full-screen .anchor-controls-bar {
        border-radius: 0;
        flex-shrink: 0;
    }
    .anchor-image-pane:fullscreen .anchor-controls-bar {
        border-radius: 0;
        flex-shrink: 0;
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


def _highlight_html_line_safe(text: str, pattern: Optional[str]) -> str:
    """Build a LINE-SAFE HTML string with pattern matches highlighted.

    Uses the same ``<b style='color:#dc2626'>`` span as ``shared.joins_lab.htmlify``
    (F-G1a) but WITHOUT the outer ``<div dir='rtl'>`` wrapper and WITHOUT
    converting ``\\n`` to ``<br>`` (F-G1b).  This makes the output safe to pass
    directly to ``render_line_numbered_html(highlight_html=...)`` which splits on
    ``\\n`` to build per-line grid rows.  An outer wrapper would be torn across rows.

    Security (T-119-10):
        - MARK_A/MARK_B sentinel bytes are stripped from the input first (WR-01)
          so corpus text cannot forge a highlight region.
        - ``html.escape()`` is applied to the whole string before inserting the
          ``<b>`` tags — only the fixed tag survives; raw text is fully escaped.
        - ``\\n`` is preserved as-is (no ``<br>`` conversion).
        - The output is suitable for ``ui.html(sanitize=False)`` only because it
          is pre-escaped; raw ``page.text`` is never passed to ``ui.html``.

    Args:
        text:     Corpus text (may be multi-line; ``\\n`` is the line separator).
        pattern:  Optional regex pattern.  If None or invalid, text is returned
                  HTML-escaped with no highlight spans.

    Returns:
        HTML-escaped string with highlight ``<b>`` spans injected around matches
        and ``\\n`` preserved (no outer wrapper element).
    """
    # 0. Strip sentinel bytes from input so corpus text cannot forge highlights.
    text = (text or "").replace(MARK_A, "").replace(MARK_B, "")

    # 1. Wrap match regions with sentinels (before escaping).
    if pattern:
        try:
            rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            text = rx.sub(lambda m: MARK_A + m.group(0) + MARK_B, text)
        except re.error:
            pass

    # 2. HTML-escape the whole string (corpus content fully escaped; sentinels survive).
    t = _html_module.escape(text)

    # 3. Replace sentinels with the fixed <b> tag.
    t = t.replace(MARK_A, "<b style='color:#dc2626'>").replace(MARK_B, "</b>")

    # 4. Return as-is — \n is preserved, NO <br> conversion, NO outer wrapper.
    return t


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
        highlight_pattern: Optional regex pattern for transcription term highlighting
                           (G1-compare).  When set, ``update_content`` builds a
                           LINE-SAFE escaped highlight string via
                           ``_highlight_html_line_safe`` and passes it to
                           ``render_line_numbered_html(highlight_html=...)``.
                           The same ``<b style='color:#dc2626'>`` span the rest of the
                           app uses (F-G1a); no outer wrapper (F-G1b); ``\\n`` preserved.
        browse_resolver:  Callable matching ``service.get_browse_page`` signature.
                          Defaults to the real AppState-backed singleton.
        external_resolver: Callable matching ``resolve_external_images`` signature.
                           Defaults to the shared helper from image_resolution.py.
        active_source:    Initial image source ('nli' default). SEED-010: lets a
                          caller (e.g. Compare) seed a known provider so the fresh
                          viewer doesn't re-default differently from grid/anchor.
        source_user_override: When True, suppress provider auto-default (the user's
                          choice is authoritative). Passed through to resolve_image_url.
    """

    # Monotonic per-page instance counter → a unique container class per viewer so
    # each AnchorViewer gets its OWN scoped manuscriptViewer (SEED-010: a single
    # global viewer + first-match querySelector wired only ONE image, so Compare's
    # two panes had dead zoom). Incremented on the NiceGUI event loop (single-threaded).
    _instance_seq = 0

    def __init__(
        self,
        sys_id: str,
        fl_id: Optional[str] = None,
        p_num: Optional[int] = None,
        volume_ie: Optional[str] = None,
        highlight_pattern: Optional[str] = None,
        browse_resolver: Optional[Callable] = None,
        external_resolver: Optional[Callable] = None,
        suppress_shelfmark_header: bool = False,
        image_max_height: Optional[str] = None,
        active_source: str = 'nli',
        source_user_override: bool = False,
    ) -> None:
        self._sys_id = sys_id
        self._fl_id = fl_id
        self._p_num: Optional[int] = p_num
        self._volume_ie = volume_ie
        # Optional regex pattern for transcription term highlighting (G1-compare).
        # Passed to _highlight_html_line_safe which builds a LINE-SAFE escaped
        # highlight string compatible with render_line_numbered_html (F-G1b).
        self._highlight_pattern: Optional[str] = highlight_pattern
        # R2-6 (Compare): when True, skip the inner shelfmark/meta info header so
        # Compare's green column subtitle is the only shelfmark shown.
        # Default False — main Joins-Lab anchor pane is UNCHANGED.
        self._suppress_shelfmark_header: bool = suppress_shelfmark_header
        # R2-3 (Compare): when set (e.g. "40vh"), override the default 72vh
        # image-container max-height so both image + transcription fit in Compare.
        # Default None — non-Compare callers get the global 72vh CSS rule unchanged.
        self._image_max_height: Optional[str] = image_max_height

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
        # SEED-017 (#10): per-instance rotation in degrees (0/90/180/270), mirrors
        # browse.py state.rotation. Pushed to the JS viewer via _apply_transform;
        # reset to 0 on zoom-reset and on folio change.
        self._rotation: int = 0
        # SEED-010: per-instance image source (persisted across folio nav so the
        # chosen provider sticks) + a unique container CLASS so this viewer's
        # zoom/pan is scoped to ITS OWN image, not the first .zoomable-image on
        # the page (Compare puts three on the page → the old global viewer wired
        # only the first, leaving both modal panes' zoom dead).
        self._active_source: str = active_source or 'nli'
        self._source_user_override: bool = bool(source_user_override)
        AnchorViewer._instance_seq += 1
        self._viewer_id: str = f"avc{AnchorViewer._instance_seq}"

        # Latest-wins folio-navigation generation (WR-03): a newer
        # update_content() supersedes any in-flight one so rapid prev/next
        # clicks can never render a stale folio. Mirrors the search path's
        # generation-counter discipline (web/pages/joins_lab.py).
        self._nav_gen: int = 0

        # UI element references set during _build_ui()
        self._image_pane: Optional[Any] = None  # SEED-017 (#10) fullscreen target
        self._image_container: Optional[Any] = None
        self._controls_row: Optional[Any] = None
        self._transcription_container: Optional[Any] = None
        self._prev_btn: Optional[Any] = None
        self._next_btn: Optional[Any] = None
        self._zoom_label: Optional[Any] = None
        # Info header (shelfmark + library + title) — populated by update_content.
        # When suppress_shelfmark_header=True, all three are left as None (R2-6).
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
        self._apply_transform()

    def zoom_out(self) -> None:
        """Decrease zoom by 0.25, clamped at 0.25."""
        self._zoom = max(self._zoom - 0.25, 0.25)
        self._apply_transform()

    def zoom_reset(self) -> None:
        """Reset zoom to 1.0 and reset the viewer pan/rotation."""
        self._zoom = 1.0
        # SEED-017 (#10): mv.reset() already zeroes the JS rotation; sync the
        # Python state so the next rotate starts from 0 and the % label is right.
        self._rotation = 0
        ui.run_javascript(
            f"(function(){{ var mv=(window.__msViewers||{{}})['{self._viewer_id}']; if(mv) mv.reset(); }})();"
        )
        self._apply_transform()

    # ──────────────────────────────────────────────────────────────────────────
    # Rotation + fullscreen (SEED-017 / audit #10 — parity with /browse + desktop)
    # ──────────────────────────────────────────────────────────────────────────

    def rotate_left(self) -> None:
        """Rotate the image 90° counter-clockwise (parity /browse + desktop, #10).

        Signed accumulation (NO ``% 360``): clicking left from 0 yields -90 so the CSS
        ``transition: transform`` animates a 90° counter-clockwise turn — not a 270°
        clockwise spin to the same visual position (UAT: "270 right seems odd"). CSS
        handles arbitrary degree values; folio change re-renders a fresh <img> at 0.
        """
        self._rotation -= 90
        self._apply_rotation()

    def rotate_right(self) -> None:
        """Rotate the image 90° clockwise (signed accumulation; see rotate_left)."""
        self._rotation += 90
        self._apply_rotation()

    def _apply_rotation(self) -> None:
        """Push ONLY rotation, preserving the LIVE client scale (P2 review fix).

        Mouse-wheel zoom (manuscript_viewer.js ``onWheel``) updates ``mv.state.scale``
        client-side only — ``self._zoom`` stays at its last server value (often 1.0).
        If rotation went through ``_apply_transform`` (which forces ``scale=self._zoom``),
        rotating after a wheel-zoom would snap the image back to 100%. So the rotation
        path reads the live ``mv.state.scale`` and leaves the zoom label untouched (the
        wheel handler keeps it in sync). The fallback (no ``mv``) uses ``self._zoom``.
        """
        ui.run_javascript(
            "(function(){"
            f"  var mv = (window.__msViewers || {{}})['{self._viewer_id}'];"
            "  if (mv && typeof mv.update === 'function') {"
            f"    mv.update((mv.state ? mv.state.scale : {self._zoom}), {self._rotation});"
            "  } else {"
            f"    var im = document.querySelector('.{self._viewer_id} .zoomable-image');"
            f"    if (im) im.style.transform = 'translate(0px,0px) rotate({self._rotation}deg) scale({self._zoom})';"
            "  }"
            "})();"
        )

    def _fullscreen_js_handler(self) -> str:
        """Return the CLIENT-SIDE click-handler JS that toggles native fullscreen.

        Bound via ``button.on("click", js_handler=...)`` — NOT server-side
        ``ui.run_javascript`` — because the Fullscreen API requires transient user
        activation, which is lost when a click round-trips to the server (Codex HIGH).
        A pure ``js_handler`` runs synchronously inside the browser click, preserving
        activation (the same pattern compare_modal.py uses for window.open).

        Targets the per-instance ``.anchor-image-pane`` wrapper (image + controls) so
        BOTH expand together (transcription stays out — full-bleed image, like /browse).
        Native fullscreen escapes the Compare dialog's stacking/transform context (a
        ``position:fixed`` overlay would be trapped inside the maximized ``ui.dialog``)
        and ESC exits natively. Scoped to this viewer's unique ``{viewer_id}`` class so
        Compare's two panes fullscreen independently. ``webkit``-prefixed fallbacks
        cover older Safari.

        HIGH-2: only requestFullscreen/exitFullscreen on a DOM node — no
        handleImageError / iiif.nli.org.il / fetchFlIdsFromManifest.
        """
        return (
            "(e) => {"
            f"  const pane = document.querySelector('.{self._viewer_id} .anchor-image-pane');"
            "  if (!pane) return;"
            "  const fsEl = document.fullscreenElement || document.webkitFullscreenElement;"
            "  if (!fsEl) {"
            "    const rfs = pane.requestFullscreen || pane.webkitRequestFullscreen;"
            "    if (rfs) rfs.call(pane);"
            "  } else {"
            "    const efs = document.exitFullscreen || document.webkitExitFullscreen;"
            "    if (efs) efs.call(document);"
            "  }"
            "}"
        )

    def _apply_transform(self) -> None:
        """Push the current zoom + rotation to the client.

        Mirrors browse.py's proven path: drive the per-instance ``manuscriptViewer``
        via ``update(scale, rotation)`` (which sets scale + rotation and re-applies
        the transform). The % label is updated SERVER-SIDE here rather than relying
        on the viewer's JS ``updateLabel()`` selector. A direct-transform fallback
        keeps zoom + rotation working even if the viewer object never initialised.
        """
        if self._zoom_label is not None:
            self._zoom_label.set_text(f"{int(self._zoom * 100)}%")
        ui.run_javascript(
            "(function(){"
            f"  var mv = (window.__msViewers || {{}})['{self._viewer_id}'];"
            "  if (mv && typeof mv.update === 'function') {"
            f"    mv.update({self._zoom}, {self._rotation});"
            "  } else {"
            f"    var im = document.querySelector('.{self._viewer_id} .zoomable-image');"
            f"    if (im) im.style.transform = 'translate(0px,0px) rotate({self._rotation}deg) scale({self._zoom})';"
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
            # SEED-010: carry the (persisted) source + override so a seeded/chosen
            # provider survives folio navigation and Compare pane creation.
            active_source=self._active_source,
            source_user_override=self._source_user_override,
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

        The only ``onload`` handler is ``window.__msInitViewer(viewer_id)`` (SEED-010),
        which creates/initialises a manuscriptViewer scoped to THIS viewer's unique
        container class so each pane (incl. Compare's two) wires its own zoom/pan —
        the same factory as /browse (without the NLI fallback escape hatch).
        """
        return (
            f'<img src="{img_url}" '
            f'class="zoomable-image" '
            f'style="transform: translate(0px,0px) rotate({self._rotation}deg) scale({self._zoom}); cursor: grab;" '
            f'draggable="false" '
            f'onload="if(window.__msInitViewer) window.__msInitViewer(\'{self._viewer_id}\')" '
            f'/>'
        )

    # ──────────────────────────────────────────────────────────────────────────
    # UI construction
    # ──────────────────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        """Build the full AnchorViewer component (image + controls + transcription)."""
        _container = ui.column().classes(f"anchor-viewer-container {self._viewer_id} w-full gap-0")
        with _container:
            # Info header — shelfmark (identifier) + library + title. Populated
            # by update_content. Uses theme CSS vars so it stays legible in both
            # light and dark themes, and follows the page RTL direction so Hebrew
            # library/title metadata reads correctly under a Hebrew UI.
            # R2-6: when suppress_shelfmark_header=True, skip the header entirely
            # so Compare's green column subtitle is the only shelfmark shown.
            if not self._suppress_shelfmark_header:
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
            # When suppressed, _info_header/_shelfmark_label/_meta_label remain None
            # (set in __init__). update_content guards all three with `is not None`.

            # SEED-017 (#10): wrap the image + controls bar in .anchor-image-pane so
            # toggle_fullscreen's native Fullscreen API can expand BOTH together (the
            # transcription stays out of fullscreen). The wrapper is a descendant of
            # .anchor-viewer-container, so the existing .image-container /
            # .anchor-controls-bar CSS (descendant/class selectors) is unaffected.
            self._image_pane = ui.element("div").classes("anchor-image-pane w-full")
            with self._image_pane:
                # Image container (shows skeleton initially).
                # .mark("anchor-viewer-image-pane") enables Plan-08 render-smoke to
                # assert that the skeleton is gone after update_content resolves (F-A3).
                # R2-3: when image_max_height is set (Compare context), apply it as an
                # inline style override so both image + transcription fit in the pane.
                image_container_style = ""
                if self._image_max_height:
                    # Compare context: cap the image to the given (viewport-relative)
                    # height and drop the 200px min-height floor so it can shrink on a
                    # short window. The transcription below gets its own bounded inner
                    # scroll (see _transcription_container) so the pane fits the window.
                    image_container_style = (
                        f"max-height: {self._image_max_height}; min-height: 0;"
                    )
                self._image_container = (
                    ui.element("div")
                    .classes("image-container relative")
                    .mark("anchor-viewer-image-pane")
                )
                if image_container_style:
                    self._image_container.style(image_container_style)
                with self._image_container:
                    self._skeleton = ui.element("div").classes("anchor-viewer-skeleton")
                    self._img_html_elem: Optional[Any] = None
                    self._error_elem: Optional[Any] = None

                # Controls bar (below image, full width).
                # #41: folio arrows follow the UI reading direction, mirroring the
                # canonical browse-page pager (web/pages/browse.py): in an RTL UI
                # "previous" points right (chevron_right) and "next" points left
                # (chevron_left), and the row itself follows page direction so the
                # arrows physically sit on the side a Hebrew reader expects. This
                # replaces the earlier hard-coded direction:ltr override, which made
                # the arrows read backwards under the RTL Hebrew interface.
                _rtl = is_rtl()
                with ui.row().classes("anchor-controls-bar w-full justify-between"):
                    # Folio navigation group
                    with ui.row().classes("gap-1 items-center"):
                        self._prev_btn = (
                            ui.button(
                                icon="chevron_right" if _rtl else "chevron_left",
                                on_click=self._on_prev_folio,
                            )
                            .props(f'flat round dense aria-label="{tr("Previous folio")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        ui.tooltip(tr("Previous folio")).bind_visibility_from(self._prev_btn)

                        self._page_label = ui.label("…").classes("text-white text-sm px-2")

                        self._next_btn = (
                            ui.button(
                                icon="chevron_left" if _rtl else "chevron_right",
                                on_click=self._on_next_folio,
                            )
                            .props(f'flat round dense aria-label="{tr("Next folio")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        ui.tooltip(tr("Next folio")).bind_visibility_from(self._next_btn)

                    # Zoom + rotate + fullscreen group (SEED-017 #10: rotate/fullscreen
                    # bring the Lab/Compare viewer to parity with /browse + desktop).
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

                        _rotate_left_btn = (
                            ui.button(icon="rotate_left", on_click=self.rotate_left)
                            .props(f'flat round dense aria-label="{tr("Rotate left")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        ui.tooltip(tr("Rotate left")).bind_visibility_from(_rotate_left_btn)

                        _rotate_right_btn = (
                            ui.button(icon="rotate_right", on_click=self.rotate_right)
                            .props(f'flat round dense aria-label="{tr("Rotate right")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        ui.tooltip(tr("Rotate right")).bind_visibility_from(_rotate_right_btn)

                        # Reset icon matches the /browse viewer (restart_alt "Reset View")
                        # per UAT — resets zoom + rotation + pan.
                        _zoom_reset_btn = (
                            ui.button(icon="restart_alt", on_click=self.zoom_reset)
                            .props(f'flat round dense aria-label="{tr("Reset View")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        ui.tooltip(tr("Reset View")).bind_visibility_from(_zoom_reset_btn)

                        # Fullscreen is bound CLIENT-SIDE (js_handler, no on_click) so the
                        # Fullscreen API call stays inside the browser's user-activation
                        # window — a server round-trip would have it rejected (Codex HIGH).
                        _fullscreen_btn = (
                            ui.button(icon="fullscreen")
                            .props(f'flat round dense aria-label="{tr("Fullscreen")}"')
                            .classes("text-white min-h-[44px] min-w-[44px]")
                        )
                        _fullscreen_btn.on("click", js_handler=self._fullscreen_js_handler())
                        ui.tooltip(tr("Fullscreen")).bind_visibility_from(_fullscreen_btn)

            # Transcription area.
            # .mark("anchor-viewer-transcription-pane") enables Plan-08 render-smoke
            # to query the loaded transcription state (F-A3).
            self._transcription_container = (
                ui.element("div")
                .classes("anchor-transcription-panel w-full")
                .mark("anchor-viewer-transcription-pane")
            )
            # Compare context (image_max_height set): the panel lives INSIDE the
            # pane's own scroll area, so drop its 65vh cap + own overflow scroll.
            # Otherwise there are NESTED scroll regions — at normal zoom the
            # image (40vh) + the 65vh panel overflow the pane and scrolling lands
            # on the inner panel, so the bottom of the text is unreachable
            # (round-5 UAT: "cannot see the bottom of the text even if I scroll").
            # Letting it flow naturally hands all scrolling to the single outer
            # pane scroll area.
            if self._image_max_height:
                # Compare context: the transcription flows at its FULL natural
                # height (no own scroll, no 65vh cap) so the OUTER two-pane body
                # scrolls everything above the navigation as one unit (round-5
                # UAT: "the outer layout should be scrollable", not an inner box).
                self._transcription_container.style(
                    "max-height:none; overflow:visible;"
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

        # Reset zoom + rotation on folio change (mirrors /browse behaviour). The
        # re-rendered <img> carries no rotation, and the fresh per-instance viewer
        # inits at rotation 0, so clearing _rotation here keeps Python state in sync.
        if direction != 0:
            self._zoom = 1.0
            self._rotation = 0

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

        # SEED-010: persist the resolved source so folio nav keeps the chosen
        # provider and a seeded source isn't lost on the next resolve.
        self._active_source = resolved.get('active_source', self._active_source)

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

        # Render transcription — with optional term highlighting (G1-compare, F-G1a/b).
        # When highlight_pattern is set, build a LINE-SAFE escaped HTML string via
        # _highlight_html_line_safe (escape → <b> span → preserve \n, NO outer wrapper)
        # and pass it to render_line_numbered_html(highlight_html=...) so per-line
        # grid rows split correctly.  Security (T-119-10): only the escaped output of
        # _highlight_html_line_safe reaches ui.html(sanitize=False) — never raw page.text.
        raw_text = page.text or ""
        if self._highlight_pattern and raw_text:
            highlight_html = _highlight_html_line_safe(raw_text, self._highlight_pattern)
        else:
            highlight_html = None
        html_text = render_line_numbered_html(
            text=raw_text,
            highlight_html=highlight_html,
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
            # SEED-010: init THIS viewer's scoped manuscriptViewer (per-instance, by
            # unique container class) via a short post-render fallback in addition to
            # the <img> onload (covers a cached image whose onload already fired).
            img_html = self._build_img_html(img_url)
            ui.html(img_html, sanitize=False)
            # SEED-010 init + SEED-017 (#10) state-sync: the per-instance viewer
            # object is reused across folios, so after re-init we MUST push the
            # current Python (rotation, zoom, pan) into mv.state — otherwise a
            # rotation/zoom from the previous folio survives in mv.state and snaps
            # back on the next wheel/drag/applyTransform (Codex MEDIUM). _build_img_html
            # already renders the <img> with this transform; this keeps mv.state in sync.
            ui.run_javascript(
                "(function(){"
                f"  var vid='{self._viewer_id}';"
                "  function sync(){"
                "    if(window.__msInitViewer) window.__msInitViewer(vid);"
                "    var mv=(window.__msViewers||{})[vid];"
                "    if(mv && mv.state){"
                "      mv.state.x=0; mv.state.y=0;"
                f"      mv.state.rotation={self._rotation}; mv.state.scale={self._zoom};"
                "      if(typeof mv.applyTransform==='function') mv.applyTransform();"
                "    }"
                "  }"
                "  sync(); setTimeout(sync, 50);"
                "})();"
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
