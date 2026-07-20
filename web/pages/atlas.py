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

from nicegui import ui

from web.translations import tr, is_rtl
from web.components.typography import h1

# Fixed canvas dimensions — reserved up front so the (later-injected) renderer
# never shifts layout when it attaches (CLS-safe, D-10). Height is a fixed px
# value, not a max-height, so the box occupies its full space before any draw.
_ATLAS_CANVAS_HEIGHT_PX = 720

# ---------------------------------------------------------------------------
# 133-04 RENDERER INJECTION POINT (contract):
#   Plan 133-04 injects the Canvas 2D renderer here. The expected shape is a
#   ``ui.add_body_html(...)`` / ``ui.run_javascript(...)`` (client-side) block
#   that, once the page is connected:
#     1. fetch('/atlas-data/manifest.json')  -> read asset_basename + counts
#     2. fetch('/atlas-data/' + asset_basename + '.bin')  (browser negotiates
#        Content-Encoding: br transparently)
#     3. decode the ArrayBuffer per docs/specs/atlas-asset-schema-v1.md
#        (BigUint64Array for NODE_SYS_ID — never Number(), §7)
#     4. draw into the '#atlas-canvas' element reserved below, wiring the D-08
#        interactions (search, color toggle, library filter, focus constellation,
#        click-through to /browse, reduced-motion-aware intro).
#   The renderer MUST NOT compute layout at request time — positions are baked.
# ---------------------------------------------------------------------------


def create_atlas_page() -> None:
    """Render the Connections Atlas beta page chrome inside the shared shell."""
    rtl = is_rtl()
    direction = 'rtl' if rtl else 'ltr'
    align = 'right' if rtl else 'left'

    with ui.column().classes('w-full max-w-7xl mx-auto gap-3 fade-in').style(
        f'direction: {direction}; text-align: {align};'
    ):
        # --- Header: title + Beta badge -----------------------------------
        with ui.row().classes('w-full items-center gap-3 flex-wrap'):
            h1(tr('Connections Atlas'))
            ui.label(tr('Beta')).classes('px-2 py-0.5 rounded-full text-xs font-bold').style(
                'background: var(--primary-600); color: white; letter-spacing: 0.05em;'
            )

        # --- One-line intro (names it a preview of the connections work) ---
        ui.label(
            tr('A preview map of textual connections across the connected Cairo Genizah corpus.')
        ).classes('text-base').style('color: var(--text-secondary);')

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
        # Explicit width AND fixed height (not max-height) so the box occupies
        # its full area before the 133-04 renderer attaches — no layout shift.
        with ui.element('div').classes('w-full rounded-lg overflow-hidden').style(
            f'position: relative; width: 100%; height: {_ATLAS_CANVAS_HEIGHT_PX}px; '
            f'background: var(--bg-secondary); border: 1px solid var(--border-light);'
        ):
            # The canvas the renderer draws into. Sized to fill the reserved box.
            ui.html(
                f'<canvas id="atlas-canvas" '
                f'style="display:block; width:100%; height:{_ATLAS_CANVAS_HEIGHT_PX}px;" '
                f'aria-label="{tr("Connections Atlas")}"></canvas>'
            )
            # Loading placeholder (centered), shown until 133-04's renderer draws.
            with ui.element('div').classes('flex items-center justify-center').style(
                'position: absolute; inset: 0; pointer-events: none;'
            ):
                ui.label(tr('Loading the atlas…')).classes('text-sm').style(
                    'color: var(--text-secondary);'
                )
