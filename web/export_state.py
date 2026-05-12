# -*- coding: utf-8 -*-
"""Per-session export payload storage.

Replaces the cross-user-leaky process-wide singleton fields on
``AppState`` (``last_results``, ``current_search_query``,
``last_selected_uids``, ``parallels_results``, ``parallels_filtered``,
``parallels_search_meta``) for the FastAPI export handlers in
``web/api.py``.

Bug 2026-05-12: cross-user state contamination -- User A's search query
appeared as the suggested .xlsx filename in User B's export dialog
(different device + network) because ``state.current_search_query`` is a
process-wide singleton overwritten on every search. Routing the export
read/write path through ``app.storage.user`` keys it by session, so
``/api/export/*`` can only ever read the data belonging to the
requesting cookie's session.

All accesses are wrapped in try/except so a pruned-session race (see
``restore_browse_snapshot`` for the same pattern) degrades to "no
payload" instead of bubbling NiceGUI's ``AssertionError: user storage
for {uuid} should be created before accessing it``.

The singleton ``state.*`` writes are intentionally left in place for
the moment; they are dead code once the export handlers stop reading
them, and will be removed in a follow-up cleanup phase.
"""
from typing import Optional, List, Dict, Any

from nicegui import app

_SEARCH_KEY = 'export_search_payload'
_PARALLELS_KEY = 'export_parallels_payload'

# Test hook: when non-None, all set/get/update/clear operations target this
# dict instead of ``app.storage.user``. Tests set it via the
# ``session_storage`` fixture in conftest; production always sees None.
_TEST_BACKEND: Optional[Dict[str, Any]] = None


def _backend():
    """Return the read/write target for export payloads.

    Production: ``app.storage.user`` (per-session via NiceGUI session cookie).
    Tests: the module-level ``_TEST_BACKEND`` dict (set via monkeypatch).
    """
    if _TEST_BACKEND is not None:
        return _TEST_BACKEND
    return app.storage.user


# ---------------------------------------------------------------------------
# Search export payload
# ---------------------------------------------------------------------------

def set_search_export(
    results: List[Dict[str, Any]],
    query: str,
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    selected_uids: Optional[List[str]] = None,
) -> None:
    """Write the entire search export payload to this user's session.

    Mirrors the singleton writes at search.py:~4089-4109 (partial-result
    cancel path) and :~4163-4188 (full-result happy path) plus the
    history-restore path at :~3789-3799.
    """
    try:
        _backend()[_SEARCH_KEY] = {
            'results': results,
            'query': query,
            'mode': mode,
            'gap': gap,
            'filters': filters,
            'warnings': warnings or [],
            'selected_uids': selected_uids,
        }
    except (AssertionError, Exception):
        pass


def get_search_export() -> Optional[Dict[str, Any]]:
    """Read this session's search export payload, or None if unset/pruned."""
    try:
        return _backend().get(_SEARCH_KEY)
    except (AssertionError, Exception):
        return None


def update_search_export_results(results: List[Dict[str, Any]]) -> None:
    """Patch only the ``results`` field (post-display-filter sync).

    Called from search_results.py:~125 after the page-scoped filter
    pipeline narrows down what the user sees so the export matches.
    """
    try:
        backend = _backend()
        payload = backend.get(_SEARCH_KEY)
        if payload:
            payload['results'] = results
            backend[_SEARCH_KEY] = payload
    except (AssertionError, Exception):
        pass


def update_search_export_selection(selected_uids: Optional[List[str]]) -> None:
    """Patch only the ``selected_uids`` field (per-row checkbox sync).

    Called from search.py:~2094 (toggle_select_all) and
    search_results.py:~373 (per-row toggle_card_selection).
    """
    try:
        backend = _backend()
        payload = backend.get(_SEARCH_KEY)
        if payload:
            payload['selected_uids'] = selected_uids
            backend[_SEARCH_KEY] = payload
    except (AssertionError, Exception):
        pass


def clear_search_export() -> None:
    """Remove the search export payload (New Search reset)."""
    try:
        _backend().pop(_SEARCH_KEY, None)
    except (AssertionError, Exception):
        pass


# ---------------------------------------------------------------------------
# Parallels export payload
# ---------------------------------------------------------------------------

def set_parallels_export(
    results: List[Dict[str, Any]],
    filtered: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write the parallels export payload to this user's session."""
    try:
        _backend()[_PARALLELS_KEY] = {
            'results': results,
            'filtered': filtered,
            'meta': meta,
        }
    except (AssertionError, Exception):
        pass


def get_parallels_export() -> Optional[Dict[str, Any]]:
    """Read this session's parallels export payload, or None."""
    try:
        return _backend().get(_PARALLELS_KEY)
    except (AssertionError, Exception):
        return None


def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None:
    """Patch only the ``filtered`` field (post-filter sync)."""
    try:
        backend = _backend()
        payload = backend.get(_PARALLELS_KEY)
        if payload:
            payload['filtered'] = filtered
            backend[_PARALLELS_KEY] = payload
    except (AssertionError, Exception):
        pass


def clear_parallels_export() -> None:
    """Remove the parallels export payload (New Search reset)."""
    try:
        _backend().pop(_PARALLELS_KEY, None)
    except (AssertionError, Exception):
        pass
