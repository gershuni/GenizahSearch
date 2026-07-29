# -*- coding: utf-8 -*-
"""
Connections Atlas (Visual Atlas Preview) — Dicta Genizah Search

Phase 133 (ATLAS-01). A claim-free, offline-baked overview of the textual
connections across the connected Cairo Genizah corpus, rendered as a Canvas 2D
"galaxy". This module owns ONLY the page chrome (shared-shell embedded, Pattern
1 — the RESOLVED architecture decision): a Beta badge, the standing honesty
banner (D-15), a one-line intro, and a CLS-reserved canvas container with a
documented JS injection point.

The actual Canvas 2D renderer (zoom/pan, title+shelfmark search, domain<->library
color toggle, library hide/solo, click-a-region focus constellation,
click-through to /browse, reduced-motion-aware bloom-in intro — D-08) is filled
in by plan 133-04 against the contract this module establishes: it fetches
``/atlas-data/manifest.json`` (mutable pointer, revalidated), reads the
content-hashed ``asset_basename`` from it, fetches ``/atlas-data/<asset_basename>.bin``
(immutable, Brotli-negotiated), decodes it per docs/specs/atlas-asset-schema-v1.md,
and draws into ``#atlas-canvas``.

This page is only ever reached when web.atlas_assets.atlas_preview_available() is
True (the /atlas route clean-hides otherwise), so this module assumes the asset
is loaded and does NOT itself re-check readiness.
"""

import json

from nicegui import ui

from web.translations import tr, is_rtl, get_language
from web.components.typography import h1

# Canvas dimensions — reserved up front so the (later-injected) renderer never
# shifts layout when it attaches (CLS-safe, D-10). The height is an explicit
# resolvable value, NOT a max-height, so the box occupies its full space before
# any draw.
#
# EVERY pointer-driven layout keeps the original flat 720px. TOUCH-primary
# devices cap the box at a fraction of the viewport (2026-07-29): the height was
# a FLAT 720px on every viewport, which on a phone is taller than the visible
# area — and because the renderer sets `touch-action: none` on the canvas to own
# pan/pinch, the canvas then swallowed every vertical swipe. Mobile users (two
# thirds of atlas traffic) could not scroll past the map at all. Capping the
# height guarantees there is always page above and below the canvas to grab.
#
# The cap is gated on `(hover: none) and (pointer: coarse)` — the standard
# touch-primary query — NOT on viewport width or height. Both alternatives are
# wrong here, and a first cut of this fix got it wrong twice (caught in review):
#   * Height-gated (a bare `min(720px, 60vh)` inline) silently shrank DESKTOP:
#     60vh < 720px for any viewport under 1200px tall, so 1920x1080 dropped to
#     648px and a 1366x768 laptop to 461px — i.e. almost every desktop.
#   * Width-gated (<= 640px) misses LANDSCAPE phones and tablets, which are
#     exactly where a 720px box over a ~390px-tall viewport is worst.
# Touch-primary is the population with the swipe-trap problem, so it is the
# population that gets the cap. Mouse/trackpad layouts are untouched at any size.
#
# This lives in a real stylesheet rule rather than the inline style because a
# media query cannot be expressed inline, and because NiceGUI parses .style()
# into a DICT keyed by property name — so an inline progressive-enhancement pair
# (`height:...vh; height:...dvh`) does not survive, keeping only the last value.
# `!important` is required to beat the inline reservation. The rule deliberately
# uses a CLASS selector (0-1-0): atlas_decode.js's `.atlas-fs-box:fullscreen`
# rules (0-2-0 / 0-2-1, also !important) out-specify it, so entering full screen
# still fills the screen on a phone. Plain `vh`, not `dvh` — `dvh` is unparseable
# on iOS < 15.4, which would drop the declaration and collapse the box.
_ATLAS_CANVAS_HEIGHT_PX = 720
_ATLAS_CANVAS_VIEWPORT_FRACTION = 60
# Class carried by BOTH the reserved box and the canvas, targeted by the rule below.
_ATLAS_HEIGHT_CAP_CLASS = 'atlas-h-cap'
_ATLAS_CANVAS_HEIGHT_CSS = f'height: {_ATLAS_CANVAS_HEIGHT_PX}px;'
_ATLAS_HEIGHT_CAP_STYLE = (
    '<style id="atlas-height-cap">'
    '@media (hover: none) and (pointer: coarse){'
    f'.{_ATLAS_HEIGHT_CAP_CLASS}{{'
    f'height: min({_ATLAS_CANVAS_HEIGHT_PX}px, {_ATLAS_CANVAS_VIEWPORT_FRACTION}vh) !important;'
    '}}'
    '</style>'
)

# ---------------------------------------------------------------------------
# 133-04 RENDERER (contract fulfilled):
#   web/static/js/atlas_decode.js is the self-contained Canvas 2D renderer. On
#   the client it:
#     1. fetch('/atlas-data/manifest.json')  -> reads asset_basename + counts
#     2. fetch('/atlas-data/' + asset_basename + '.bin')  (browser negotiates
#        Content-Encoding: br transparently) — NEVER an inline blob (Pitfall #3)
#     3. decodes the ArrayBuffer per docs/specs/atlas-asset-schema-v1.md
#        (BigUint64Array for NODE_SYS_ID — never Number(), §7)
#     4. draws into the '#atlas-canvas' element reserved below and wires the D-08
#        interactions (zoom/pan, title+shelfmark search, domain<->library color
#        toggle, library filter, focus constellation, reduced-motion intro,
#        click-through to /browse). Positions are baked — no request-time layout.
#   ``_inject_atlas_renderer()`` loads the module + hands it a tr()'d, language-
#   aware config; the module (not Python) owns all drawing + DOM building, and
#   builds every catalogue-derived DOM node XSS-safely (createElement/textContent
#   — HIGH-7), never via innerHTML string interpolation.
# ---------------------------------------------------------------------------

# The decoder/renderer module, served from the public /static mount (it carries
# no data — the data is fetched from the flag+readiness-gated /atlas-data routes).
_ATLAS_DECODER_SRC = '/static/js/atlas_decode.js'
# The mutable manifest pointer the renderer fetches first (revalidated); it then
# reads asset_basename from it and fetches the immutable content-hashed .bin.
_ATLAS_MANIFEST_URL = '/atlas-data/manifest.json'
_ATLAS_DATA_BASE = '/atlas-data/'


def _renderer_labels() -> dict:
    """tr()'d UI strings handed to the client renderer. Every catalogue-derived
    label the JS paints/builds is selected from this dict (D-15 bilingual) so no
    English leaks under a Hebrew UI. All keys are pre-registered in
    genizah_translations.py (133-03)."""
    return {
        'searchPlaceholder': tr('Search by title or shelfmark…'),
        'colorByDomain': tr('Color by domain'),
        'colorByLibrary': tr('Color by library'),
        'hideLibrary': tr('Hide library'),
        'showOnly': tr('Show only this library'),
        'showAll': tr('Show all'),
        'focusConstellation': tr('Focus constellation'),
        'connections': tr('Connections'),
        'continuation': tr('Continuation (same-work evidence)'),
        'citation': tr('Citation / quotation'),
        'skipIntro': tr('Skip intro'),
        'zoomIn': tr('Zoom in'),
        'zoomOut': tr('Zoom out'),
        'resetView': tr('Reset view'),
        'fullScreen': tr('Full screen'),
        'exitFullScreen': tr('Exit full screen'),
        'hideDomainLabels': tr('Hide domain labels'),
        'showDomainLabels': tr('Show domain labels'),
        'close': tr('Close'),
        'openFullBrowse': tr('Open full browse ↗'),
        'manuscriptViewer': tr('Manuscript viewer'),
        'backToMap': tr('Back to map'),
        'titles': tr('Titles'),
        'hideTitles': tr('Hide titles'),
        'title': tr('Title'),
        'shelfmark': tr('Shelfmark'),
        'domain': tr('Domain'),
        'library': tr('Library'),
        'loadError': tr('The atlas could not be loaded.'),
    }


def _inject_atlas_renderer() -> None:
    """Load web/static/js/atlas_decode.js and start it with a tr()'d config.

    The manifest URL + data base are passed as data (never the bytes — Pitfall
    #3); the renderer fetches /atlas-data/manifest.json, then the content-hashed
    asset, decodes per the frozen schema, and draws into '#atlas-canvas'. The
    bootstrap polls for ``window.AtlasDecode`` because the external module loads
    asynchronously and NiceGUI mounts the canvas after the socket connects."""
    config = {
        'manifestUrl': _ATLAS_MANIFEST_URL,
        'dataBase': _ATLAS_DATA_BASE,
        'canvasId': 'atlas-canvas',
        # Reserved-box + loading-placeholder ids (Codex MEDIUM-2/-3 hardening):
        # the renderer hides #atlas-loading on a successful first draw AND
        # before showing the load-error overlay (never a stuck "Loading…"
        # under either the canvas or the error state), and falls back to
        # #atlas-canvas-box (or document.body) to surface the error UI if the
        # canvas itself never mounts within the poll window (MEDIUM-3).
        'loadingId': 'atlas-loading',
        'boxId': 'atlas-canvas-box',
        'lang': get_language(),
        'rtl': is_rtl(),
        'labels': _renderer_labels(),
    }
    ui.add_body_html(f'<script src="{_ATLAS_DECODER_SRC}"></script>')
    ui.add_body_html(
        '<script>(function(){'
        f'window.__ATLAS_CONFIG__ = {json.dumps(config, ensure_ascii=False)};'
        'var tries=0;(function boot(){'
        'if(window.AtlasDecode&&typeof window.AtlasDecode.init==="function"){'
        'window.AtlasDecode.init(window.__ATLAS_CONFIG__);return;}'
        'if(tries++>200)return;setTimeout(boot,50);})();'
        '})();</script>'
    )


def create_atlas_page() -> None:
    """Render the Connections Atlas beta page chrome inside the shared shell."""
    rtl = is_rtl()
    direction = 'rtl' if rtl else 'ltr'
    align = 'right' if rtl else 'left'

    # Touch-primary height cap (see _ATLAS_HEIGHT_CAP_STYLE). In <head>, so it
    # applies at first paint and the CLS reservation still holds.
    ui.add_head_html(_ATLAS_HEIGHT_CAP_STYLE)

    with ui.column().classes('w-full max-w-7xl mx-auto gap-3 fade-in').style(
        f'direction: {direction}; text-align: {align};'
    ):
        # --- Header: title + Beta badge -----------------------------------
        with ui.row().classes('w-full items-center gap-3 flex-wrap'):
            h1(tr('The Visual Genizah Atlas'))
            ui.label(tr('Beta')).classes('px-2 py-0.5 rounded-full text-xs font-bold').style(
                'background: var(--primary-600); color: white; letter-spacing: 0.05em;'
            )

        # --- Intro / how-to (elaborated 2026-07-21) — describes the map, its
        #     interactions, and how to read it to spot connections. Two paragraphs.
        ui.label(
            tr("A graphical view of textual connections between manuscripts across the Genizah. Manuscripts containing similar text are grouped together into clusters, and connections between manuscripts are marked with a thin line. Alongside the clusters and manuscripts, catalogue information from the National Library and the Friedberg Genizah Project is shown. You can zoom in and out, focus on a particular cluster, and open a preview of a specific manuscript to read it. For the best experience on a phone, tap Full screen — there you can pan and zoom the map freely; a larger screen simply shows more of it at once.")
        ).classes('text-sm').style('color: var(--text-secondary); line-height: 1.7;')
        ui.label(
            tr("Use the atlas to get an overall sense of the structure of the Genizah corpus and the connections within it, and to discover new, previously unknown connections. For example, if within a cluster of linguistics manuscripts you find a manuscript identified as 'Biblical fragments,' this manuscript too may be a work of linguistics.")
        ).classes('text-sm').style('color: var(--text-secondary); line-height: 1.7;')

        # --- Standing honesty banner (D-15) --------------------------------
        # Positions & clusters are algorithmic; proximity is not physical
        # provenance. This banner stands regardless of any interaction.
        with ui.element('div').classes('w-full px-4 py-3 flex items-start gap-3 rounded-lg').style(
            f'background: var(--bg-tertiary); border: 1px solid var(--border-light); direction: {direction};'
        ):
            ui.icon('info').classes('text-lg mt-0.5').style('color: var(--primary-600);')
            ui.label(
                tr(
                    'Positions and clusters are algorithmically derived from textual '
                    'connections — proximity reflects textual similarity, not physical provenance.'
                )
            ).classes('text-sm').style('color: var(--text-secondary); line-height: 1.5;')

        # --- CLS-reserved canvas container ---------------------------------
        # Explicit width AND an explicit resolvable height (not max-height) so
        # the box occupies its full area before the 133-04 renderer attaches —
        # no layout shift. The height caps against the viewport on phones; see
        # _ATLAS_CANVAS_HEIGHT_CSS.
        with ui.element('div').classes(
            f'w-full rounded-lg overflow-hidden {_ATLAS_HEIGHT_CAP_CLASS}'
        ).style(
            f'position: relative; width: 100%; {_ATLAS_CANVAS_HEIGHT_CSS} '
            f'background: var(--bg-secondary); border: 1px solid var(--border-light);'
        ).props('id="atlas-canvas-box"'):
            # The canvas the renderer draws into. Sized to fill the reserved box.
            # Rendered as a native NiceGUI element (mirrors the proven puzzle
            # Fabric.js canvas at web/pages/puzzle.py) — NOT ui.html, whose
            # default sanitize=True (client-side setHTML/DOMPurify) strips the
            # id, so document.getElementById('atlas-canvas') never resolves and
            # the renderer's whenCanvasReady poll times out ("could not be
            # loaded"). Setting id via .props() keeps it queryable in the DOM.
            ui.element('canvas').props('id=atlas-canvas').props(
                f'aria-label="{tr("The Visual Genizah Atlas")}"'
            ).classes(_ATLAS_HEIGHT_CAP_CLASS).style(
                f'display:block; width:100%; {_ATLAS_CANVAS_HEIGHT_CSS}'
            )
            # Loading placeholder (centered), shown until 133-04's renderer draws.
            # The JS renderer removes this element (by id) on a successful first
            # draw AND before showing the load-error overlay, so it never sits
            # stuck over either state (Codex MEDIUM-2).
            with ui.element('div').classes('flex items-center justify-center').style(
                'position: absolute; inset: 0; pointer-events: none;'
            ).props('id="atlas-loading"'):
                ui.label(tr('Loading the atlas…')).classes('text-sm').style(
                    'color: var(--text-secondary);'
                )

    # Load + start the client-side Canvas 2D renderer (fetch manifest -> asset ->
    # decode per the frozen schema -> draw + D-08 interactions). All drawing and
    # catalogue-derived DOM building happens client-side in atlas_decode.js.
    _inject_atlas_renderer()
