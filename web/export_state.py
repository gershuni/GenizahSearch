# -*- coding: utf-8 -*-
"""Per-session export payload storage.

Reads/writes export payloads through ``web.safe_storage`` chokepoint helpers
(``safe_user_get`` / ``safe_user_set`` / ``safe_user_pop``), which route to
``app.storage.user`` per NiceGUI session. The Phase 87 chokepoint provides:

  - prune-race protection (AssertionError absorbed -> default/no-op)
  - aliased-import enforcement (lint scanner verifies no raw access)
  - sole legal access pattern for per-user state outside the allowlist

Phase 88 (this rewrite) removed the ``_TEST_BACKEND`` shim and the
``_backend()`` helper that selected between it and ``app.storage.user``.
Tests now monkeypatch ``web.safe_storage.app`` directly (mirrors the
Phase 87 pattern in tests/test_browse_state.py), so production-code
shims are no longer required.

Update functions adopt:
  - ``isinstance(payload, dict)`` guard before mutating retrieved payloads
    (defends against poisoned-shape storage state)
  - copy-on-update (``payload = dict(payload)``) before reassigning
    (defends against shared-reference races between same-session requests)

Read functions adopt (Phase 88 D-11 extension, Refinement 4 -- Codex review):
  - ``isinstance(payload, dict)`` guard on return -- returns None if storage
    holds a non-dict value at the key (defends callers from
    ``payload.get('results')`` TypeError on poisoned storage).
"""
from typing import Optional, List, Dict, Any

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

_SEARCH_KEY = 'export_search_payload'
_PARALLELS_KEY = 'export_parallels_payload'


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
    """Write the search export payload to this user's session."""
    safe_user_set(_SEARCH_KEY, {
        'results': results,
        'query': query,
        'mode': mode,
        'gap': gap,
        'filters': filters,
        'warnings': warnings or [],
        'selected_uids': selected_uids,
    })


def get_search_export() -> Optional[Dict[str, Any]]:
    """Read this session's search export payload, or None if unset/pruned.

    Phase 88 D-11 extension (Codex review): returns None if storage holds a
    non-dict at the key (poisoned-shape defense). Callers in web/api.py do
    ``payload.get('results')`` and would crash on a non-dict; this guard makes
    the contract explicit -- return type is dict-or-None, never list/str/etc.
    """
    payload = safe_user_get(_SEARCH_KEY, None)
    return payload if isinstance(payload, dict) else None


def update_search_export_results(results: List[Dict[str, Any]]) -> None:
    """Patch only the ``results`` field (post-display-filter sync)."""
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return  # D-11: poisoned-shape or missing payload
    payload = dict(payload)  # D-12: copy-on-update
    payload['results'] = results
    safe_user_set(_SEARCH_KEY, payload)


def update_search_export_selection(selected_uids: Optional[List[str]]) -> None:
    """Patch only the ``selected_uids`` field (per-row checkbox sync)."""
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    payload['selected_uids'] = selected_uids
    safe_user_set(_SEARCH_KEY, payload)


def clear_search_export() -> None:
    """Remove the search export payload (New Search reset)."""
    safe_user_pop(_SEARCH_KEY, None)


# ---------------------------------------------------------------------------
# Parallels export payload
# ---------------------------------------------------------------------------

def set_parallels_export(
    results: List[Dict[str, Any]],
    filtered: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write the parallels export payload to this user's session.

    The ``meta`` dict carries ``source_text`` plus envelope-echo metadata
    (chunk_size, mode, max_freq, filters, boundary_options, warnings).
    Per Phase 88 D-13, ``source_text`` is folded into ``meta`` instead of
    living in a separate ``app.storage.user['parallels_source_text']`` key.
    """
    safe_user_set(_PARALLELS_KEY, {
        'results': results,
        'filtered': filtered,
        'meta': meta,
    })


def get_parallels_export() -> Optional[Dict[str, Any]]:
    """Read this session's parallels export payload, or None.

    Phase 88 D-11 extension (Codex review): see get_search_export() -- same
    poisoned-shape defense applies. Returns dict-or-None invariant.
    """
    payload = safe_user_get(_PARALLELS_KEY, None)
    return payload if isinstance(payload, dict) else None


def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None:
    """Patch only the ``filtered`` field (post-filter sync)."""
    payload = safe_user_get(_PARALLELS_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    payload['filtered'] = filtered
    safe_user_set(_PARALLELS_KEY, payload)


def clear_parallels_export() -> None:
    """Remove the parallels export payload (New Search reset)."""
    safe_user_pop(_PARALLELS_KEY, None)
