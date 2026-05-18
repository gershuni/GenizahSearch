# -*- coding: utf-8 -*-
"""Per-session export payload storage.

Reads/writes export payloads through ``web.safe_storage`` chokepoint helpers
(``safe_user_get`` / ``safe_user_set`` / ``safe_user_pop``), which route to
``app.storage.user`` per NiceGUI session. The Phase 87 chokepoint provides:

  - prune-race protection (AssertionError absorbed -> default/no-op)
  - aliased-import enforcement (lint scanner verifies no raw access)
  - sole legal access pattern for per-user state outside the allowlist

Phase 88 (this rewrite) removed the pre-Phase-88 test-backend shim and
its production-code selector helper. Tests now monkeypatch
``web.safe_storage.app`` directly (mirrors the Phase 87 pattern in
tests/test_browse_state.py), so production-code shims are no longer
required.

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
from typing import Optional, List, Dict, Any, Tuple

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

_SEARCH_KEY = 'export_search_payload'
_PARALLELS_KEY = 'export_parallels_payload'

# Hard cap on per-session export payload size. A Hebrew wildcard search can
# return 30K+ results; persisting the full list into app.storage.user
# produced 500 MB JSON files that NiceGUI rehydrated into RAM on every
# session load (forensic evidence captured 2026-05-18: a single user
# storage file held a 498 MB results list out of 864 MB total).
# Capping at 5,000 keeps worst-case payload under ~80 MB and covers every
# realistic export use case (Excel UI shows 200 rows/page; nobody scrolls
# past 5K before refining).
_EXPORT_RESULTS_CAP = 5000


def _cap_results(results: Any) -> Tuple[List[Dict[str, Any]], bool, int]:
    """Truncate ``results`` to ``_EXPORT_RESULTS_CAP``.

    Returns ``(capped_list, was_truncated, original_length)``.
    Non-list inputs degrade to ``([], False, 0)`` so callers stay total.
    """
    if not isinstance(results, list):
        return [], False, 0
    n = len(results)
    if n <= _EXPORT_RESULTS_CAP:
        return results, False, n
    return results[:_EXPORT_RESULTS_CAP], True, n


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
    """Write the search export payload to this user's session.

    Results are capped to ``_EXPORT_RESULTS_CAP`` before storage to prevent
    the per-session leak that grew RSS unbounded under heavy wildcard
    searches. ``truncated`` and ``total_count`` are emitted so downstream
    consumers can surface a "showing first N of M" hint if they choose.
    """
    capped, truncated, original = _cap_results(results)
    safe_user_set(_SEARCH_KEY, {
        'results': capped,
        'query': query,
        'mode': mode,
        'gap': gap,
        'filters': filters,
        'warnings': warnings or [],
        'selected_uids': selected_uids,
        'truncated': truncated,
        'total_count': original,
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
    """Patch only the ``results`` field (post-display-filter sync).

    Applies the same cap as ``set_search_export``; a filter that re-inflates
    above the cap (rare but possible after an undo) is still bounded.
    """
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return  # D-11: poisoned-shape or missing payload
    payload = dict(payload)  # D-12: copy-on-update
    capped, truncated, original = _cap_results(results)
    payload['results'] = capped
    payload['truncated'] = truncated
    payload['total_count'] = original
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

    Both ``results`` and ``filtered`` are independently capped to
    ``_EXPORT_RESULTS_CAP`` to bound payload size.
    """
    results_capped, results_trunc, results_total = _cap_results(results)
    filtered_capped, filtered_trunc, filtered_total = _cap_results(filtered)
    safe_user_set(_PARALLELS_KEY, {
        'results': results_capped,
        'filtered': filtered_capped,
        'meta': meta,
        'truncated': results_trunc or filtered_trunc,
        'total_count': results_total,
        'filtered_total_count': filtered_total,
    })


def get_parallels_export() -> Optional[Dict[str, Any]]:
    """Read this session's parallels export payload, or None.

    Phase 88 D-11 extension (Codex review): see get_search_export() -- same
    poisoned-shape defense applies. Returns dict-or-None invariant.
    """
    payload = safe_user_get(_PARALLELS_KEY, None)
    return payload if isinstance(payload, dict) else None


def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None:
    """Patch only the ``filtered`` field (post-filter sync).

    Applies the same cap as ``set_parallels_export`` on the filtered list.
    """
    payload = safe_user_get(_PARALLELS_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    capped, truncated, original = _cap_results(filtered)
    payload['filtered'] = capped
    payload['filtered_total_count'] = original
    # OR-merge truncation flag: if either list was capped at any point, the
    # payload as a whole is truncated.
    payload['truncated'] = bool(payload.get('truncated', False)) or truncated
    safe_user_set(_PARALLELS_KEY, payload)


def clear_parallels_export() -> None:
    """Remove the parallels export payload (New Search reset)."""
    safe_user_pop(_PARALLELS_KEY, None)
