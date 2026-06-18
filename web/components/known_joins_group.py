# -*- coding: utf-8 -*-
"""Known-joins group renderer (ANC-04, D-15/D-16).

A pure render component that displays source-attributed known-joins for the
current anchor fragment inside the Joins Lab anchor pane.  This component
holds zero per-user state (T-118-02) and never fetches data — the caller
performs the off-loop ``fetch_connected_fragments(confirmed_only=True)`` call
and passes the result dict in.

Exports
-------
render_known_joins_group(data, current_shelfmark, current_sys_id, on_reanchor, on_open_browse)
badge_for_source(source) -> (label, css_color)
"""

from __future__ import annotations

from typing import Callable

from nicegui import ui

from web.translations import is_rtl, tr


# ---------------------------------------------------------------------------
# Source badge helper
# ---------------------------------------------------------------------------

# Color map — parity with desktop/join_workbench.py:166-179 and 118-UI-SPEC
# §"Source badge color map".  WCAG-compliant contrast ratios per UI-SPEC.
_SOURCE_COLORS: dict[str, tuple[str, str]] = {
    'PGP':       ('PGP',       '#1d4ed8'),  # blue-700   (7.0:1 contrast)
    'FJMS':      ('FJMS',      '#7e22ce'),  # purple-700 (7.2:1 contrast)
    'user':      ('User',      '#047857'),  # emerald-700 (5.4:1 contrast)
    'community': ('Community', '#525252'),  # neutral-600 (sufficient contrast)
}
_SOURCE_DEFAULT = ('?', '#71717a')  # neutral-500 fallback


def badge_for_source(source: str) -> tuple[str, str]:
    """Return (label, css_hex_color) for a source identifier.

    Covers 'PGP', 'FJMS', 'user', 'community'.  Unknown sources fall back
    to a neutral grey so the UI never crashes on unexpected source strings.
    """
    return _SOURCE_COLORS.get(source, _SOURCE_DEFAULT)


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------


def render_known_joins_group(
    data: dict,
    current_shelfmark: str,
    current_sys_id: str,
    on_reanchor: Callable[[str, str], None],
    on_open_browse: Callable[[str], None],
) -> None:
    """Render the known-joins group into the current NiceGUI parent slot.

    The component renders an ``expansion_item`` that is expanded by default
    when there are known joins, and collapsed (empty-state) when there are
    none.  Every user-facing string goes through ``tr()`` for EN/HE bilingual
    support.

    Args:
        data: Dict returned by ``fetch_connected_fragments(confirmed_only=True)``
              with keys: fragments, joins, total_joins, fragment_details.
        current_shelfmark: Shelfmark of the anchor fragment (skipped in list).
        current_sys_id: sys_id of the anchor (used to avoid re-anchoring to self).
        on_reanchor: Callback(sys_id: str, shelfmark: str) — called when the user
                     clicks the re-anchor pin icon on a member row.
        on_open_browse: Callback(shelfmark: str) — called when the user clicks the
                        open-in-browse icon on a member row.
    """
    joins: list[dict] = data.get('joins', [])
    fragment_details: list[dict] = data.get('fragment_details', [])
    count: int = data.get('total_joins', len(joins))

    # Build a shelfmark-to-sys_id lookup from fragment_details
    shelfmark_to_sys: dict[str, str] = {}
    for fd in fragment_details:
        sm = fd.get('shelfmark', '')
        sid = fd.get('document_id', '')
        if sm:
            shelfmark_to_sys[sm.upper()] = sid

    current_upper = current_shelfmark.upper() if current_shelfmark else ''

    header_text = tr('Known Joins') + f' ({count})'

    with ui.expansion(
        text=header_text,
        value=count > 0,
    ).classes('w-full'):
        if count == 0:
            # Empty state — ANC-05 multitenant disclosure
            with ui.column().classes('gap-1 py-2'):
                ui.label(tr('No known joins')).classes('text-sm').style(
                    'color: var(--text-primary);'
                )
                ui.label(tr('Only confirmed public joins are shown')).classes(
                    'text-xs'
                ).style('color: var(--text-muted); font-size: 12px;')
        else:
            with ui.column().classes('w-full gap-0').style(
                'max-height: 240px; overflow-y: auto;'
            ):
                for join in joins:
                    # Determine the member shelfmark — the side that is NOT the anchor
                    frag_a = join.get('fragment_a', '')
                    frag_b = join.get('fragment_b', '')
                    if frag_a.upper() == current_upper:
                        member_shelfmark = frag_b
                    elif frag_b.upper() == current_upper:
                        member_shelfmark = frag_a
                    else:
                        # Join not directly anchored — use fragment_b as the member
                        member_shelfmark = frag_b

                    if not member_shelfmark:
                        continue
                    # Skip the anchor itself
                    if member_shelfmark.upper() == current_upper:
                        continue

                    member_sys_id = shelfmark_to_sys.get(member_shelfmark.upper(), '')
                    sources: list[str] = join.get('sources', [])

                    _render_member_row(
                        member_shelfmark=member_shelfmark,
                        member_sys_id=member_sys_id,
                        sources=sources,
                        on_reanchor=on_reanchor,
                        on_open_browse=on_open_browse,
                    )


def _render_member_row(
    member_shelfmark: str,
    member_sys_id: str,
    sources: list[str],
    on_reanchor: Callable[[str, str], None],
    on_open_browse: Callable[[str], None],
) -> None:
    """Render one 36px compact member row.

    Layout: [source badge(s)] [shelfmark label] [spacer] [pin icon] [open icon]
    """
    direction_class = 'flex-row-reverse' if is_rtl() else 'flex-row'
    with ui.row().classes(
        f'w-full items-center gap-2 px-1 {direction_class}'
    ).style(
        'height: 36px; min-height: 36px; border-radius: 4px;'
    ):
        # Source attribution badges
        for src in sources:
            label, color = badge_for_source(src)
            ui.badge(label).style(
                f'background: {color}; color: #ffffff; font-size: 10px; '
                f'padding: 1px 4px; border-radius: 3px; font-weight: 600;'
            )

        # Shelfmark label (13px, weight 700, RTL-aware)
        text_align = 'right' if is_rtl() else 'left'
        ui.label(member_shelfmark).style(
            f'font-size: 13px; font-weight: 700; '
            f'color: var(--text-primary); text-align: {text_align}; flex: 1;'
        )

        # Trailing action icons
        # Re-anchor pin
        def _make_reanchor(sm: str = member_shelfmark, sid: str = member_sys_id) -> Callable:
            def _do() -> None:
                on_reanchor(sid, sm)
            return _do

        # Open in browse
        def _make_browse(sm: str = member_shelfmark) -> Callable:
            def _do() -> None:
                on_open_browse(sm)
            return _do

        # CR-02: only render the re-anchor pin when a sys_id was resolved.
        # Community members whose shelfmark could not be mapped to a sys_id
        # have member_sys_id='' — re-anchoring to an empty sys_id would corrupt
        # the stored anchor. Such members are still reachable via the
        # open-in-browse icon below.
        if member_sys_id:
            ui.button(
                icon='push_pin',
                on_click=_make_reanchor(),
            ).props('flat dense').classes('text-gray-500').tooltip(
                tr('Re-anchor to this fragment')
            )
        ui.button(
            icon='open_in_new',
            on_click=_make_browse(),
        ).props('flat dense').classes('text-gray-500').tooltip(
            tr('View in Browse')
        )
