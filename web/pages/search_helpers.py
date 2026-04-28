# -*- coding: utf-8 -*-
"""
Helpers shared by search.py and search_results.py for Phase 77 Plan 06
(gap closure). Kept in a separate module to avoid the search.py <->
search_results.py circular-import surface that came up during Phase 72
extraction.
"""
from __future__ import annotations
from typing import Optional, List


def compute_selected_uids(search_state) -> Optional[List[str]]:
    """Map page-scoped checkbox selection to the uid list the export
    handlers consume.

    Returns None when no rows are selected (preserves "export full set"
    behavior). Returns a list of uids (in sorted-index order) when at
    least one row is selected. Out-of-bounds indices are silently
    skipped -- defensive against stale selected_indices after results
    change underneath us. Empty-uid items (metadata-only hits per D-04
    locator semantics) are preserved as empty strings in the list and
    will naturally fail to match anything in the export handler.

    Args:
        search_state: SearchUIState instance with `.results` (list[dict])
                      and `.selected_indices` (set[int]).
    """
    if not search_state.selected_indices:
        return None
    uids: List[str] = []
    for i in sorted(search_state.selected_indices):
        if 0 <= i < len(search_state.results):
            r = search_state.results[i]
            uids.append(r.get('uid', '') if isinstance(r, dict) else '')
    return uids
