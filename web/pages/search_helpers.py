# -*- coding: utf-8 -*-
"""
Helpers shared by search.py and search_results.py for Phase 77 Plan 06
(gap closure). Kept in a separate module to avoid the search.py <->
search_results.py circular-import surface that came up during Phase 72
extraction.
"""
from __future__ import annotations
from typing import Optional, List


def tag_result_shelfmark(result) -> None:
    """A PGP tag result's shelfmark and, for a joined document's fragment, which document and
    fragment it is ("PGP 7403 · 1/4"): the rows of a join all show the document's text."""
    from nicegui import ui
    from shared.document_service import pgp_join_label
    ui.label(result.get('shelfmark', 'Unknown')).classes('font-bold break-all').style('color: var(--primary-700);')
    join_label = pgp_join_label(result)
    if join_label:
        ui.label(join_label).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
            'background: var(--bg-tertiary); color: var(--text-secondary); direction: ltr;')


def selection_key(row) -> str:
    """A result's identity for the selection: its uid (one per page), else its manuscript.

    Not its position: the cards number the rows the filters leave, so a position in the
    full list named another result whenever a filter hid an earlier row (Codex on #26).
    """
    if isinstance(row, dict):
        key = row.get('uid') or (row.get('display') or {}).get('id')
        if key:
            return str(key)
    return f'@{id(row)}'


def selected_rows(search_state) -> List[dict]:
    """The selected results among those the page shows (`displayed_results`), in its order.

    A selected result a filter hides is neither counted nor acted on; it shows selected
    again once the filter is off.
    """
    keys = search_state.selected_keys
    if not keys:
        return []
    return [r for r in (search_state.displayed_results or []) if selection_key(r) in keys]


def compute_selected_uids(search_state) -> Optional[List[str]]:
    """Map page-scoped checkbox selection to the uid list the export
    handlers consume.

    Returns None when no shown row is selected (preserves "export full set"
    behavior), else the selected shown rows' uids in the page's order
    (`selected_rows`). Empty-uid items (metadata-only hits per D-04 locator
    semantics) are preserved as empty strings in the list and will
    naturally fail to match anything in the export handler.

    Args:
        search_state: SearchUIState instance with `.displayed_results`
                      (list[dict]) and `.selected_keys` (set[str]).
    """
    rows = selected_rows(search_state)
    if not rows:
        return None
    return [r.get('uid', '') if isinstance(r, dict) else '' for r in rows]
