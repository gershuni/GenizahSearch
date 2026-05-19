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
import json
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Callable

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

_SEARCH_KEY = 'export_search_payload'
_PARALLELS_KEY = 'export_parallels_payload'

# Hard cap on per-session export payload size. A Hebrew wildcard search can
# return 30K+ results; persisting the full list into app.storage.user
# produced 500 MB JSON files that NiceGUI rehydrated into RAM on every
# session load (forensic evidence captured 2026-05-18: a single user
# storage file held a 498 MB results list out of 864 MB total).
# Capping by row count was not enough by itself: a handful of rows can still
# weigh many MB when each carries a full manuscript transcription. The write
# paths below also strip heavyweight text fields and keep only a short excerpt;
# full text is rehydrated from Tantivy at download time.
_EXPORT_RESULTS_CAP = 5000
_SEARCH_FULL_TEXT_EXCERPT_CHARS = 500
_PARALLELS_TEXT_STORAGE_CHARS = 4000
_PARALLELS_CHUNK_HITS_CAP = 100
_PARALLELS_CHUNK_TEXT_STORAGE_CHARS = 1000


def _text_prefix(value: Any, max_chars: int) -> str:
    if value is None:
        return ''
    text = value if isinstance(value, str) else str(value)
    return text[:max_chars]


def _compact_search_result_row(row: Any) -> Tuple[Any, bool]:
    """Return a storage-safe search result row plus a changed flag."""
    if not isinstance(row, dict):
        return row, False

    compact = dict(row)
    changed = False

    full_text = compact.pop('full_text', None)
    if full_text is not None:
        changed = True
        excerpt = _text_prefix(full_text, _SEARCH_FULL_TEXT_EXCERPT_CHARS)
        if excerpt:
            compact['full_text_excerpt'] = excerpt
    elif 'full_text_excerpt' in compact:
        excerpt = _text_prefix(compact.get('full_text_excerpt'), _SEARCH_FULL_TEXT_EXCERPT_CHARS)
        if compact.get('full_text_excerpt') != excerpt:
            changed = True
            compact['full_text_excerpt'] = excerpt

    for key in ('raw_file_hl', 'content'):
        if key in compact:
            compact.pop(key, None)
            changed = True

    display = compact.get('display')
    if isinstance(display, dict):
        display_compact = dict(display)
        for key in ('full_text', 'raw_file_hl', 'content'):
            if key in display_compact:
                display_compact.pop(key, None)
                changed = True
        if display_compact is not display:
            compact['display'] = display_compact

    return compact, changed


def _compact_chunk_hit(hit: Any) -> Tuple[Any, bool]:
    """Compact one parallels chunk_hit while preserving tuple/list shape."""
    changed = False
    if isinstance(hit, tuple):
        items = list(hit)
        original_type = tuple
    elif isinstance(hit, list):
        items = list(hit)
        original_type = list
    else:
        return hit, False

    for idx in (1, 3):
        if idx < len(items) and isinstance(items[idx], str):
            truncated = items[idx][:_PARALLELS_CHUNK_TEXT_STORAGE_CHARS]
            if truncated != items[idx]:
                items[idx] = truncated
                changed = True

    return (tuple(items) if original_type is tuple else items), changed


def _compact_parallels_result_row(row: Any) -> Tuple[Any, bool]:
    """Return a storage-safe parallels result row plus a changed flag."""
    if not isinstance(row, dict):
        return row, False

    compact = dict(row)
    changed = False

    for key in ('full_text', 'content', 'raw_file_hl'):
        if key in compact:
            compact.pop(key, None)
            changed = True

    for key in ('source_ctx', 'text'):
        if key in compact and isinstance(compact[key], str):
            truncated = compact[key][:_PARALLELS_TEXT_STORAGE_CHARS]
            if truncated != compact[key]:
                compact[key] = truncated
                changed = True

    chunk_hits = compact.get('chunk_hits')
    if isinstance(chunk_hits, list):
        source_hits = chunk_hits[:_PARALLELS_CHUNK_HITS_CAP]
        if len(source_hits) != len(chunk_hits):
            changed = True
        compact_hits = []
        for hit in source_hits:
            compact_hit, hit_changed = _compact_chunk_hit(hit)
            compact_hits.append(compact_hit)
            changed = changed or hit_changed
        compact['chunk_hits'] = compact_hits

    return compact, changed


def _identity_result_row(row: Any) -> Tuple[Any, bool]:
    return row, False


def _compact_results(
    results: Any,
    row_compactor: Callable[[Any], Tuple[Any, bool]],
) -> Tuple[List[Any], bool, int, bool]:
    """Cap and compact a result list.

    Returns ``(rows, was_truncated, original_length, was_changed)``.
    """
    if not isinstance(results, list):
        return [], False, 0, results is not None
    n = len(results)
    truncated = n > _EXPORT_RESULTS_CAP
    source_rows = results[:_EXPORT_RESULTS_CAP] if truncated else results
    compacted: List[Any] = []
    changed = truncated
    for row in source_rows:
        compact_row, row_changed = row_compactor(row)
        compacted.append(compact_row)
        changed = changed or row_changed
    return compacted, truncated, n, changed


def _cap_results(results: Any) -> Tuple[List[Dict[str, Any]], bool, int]:
    """Truncate ``results`` to ``_EXPORT_RESULTS_CAP``.

    Returns ``(capped_list, was_truncated, original_length)``.
    Non-list inputs degrade to ``([], False, 0)`` so callers stay total.
    """
    capped, truncated, original, _changed = _compact_results(results, _identity_result_row)
    return capped, truncated, original


def compact_parallels_result_rows(results: Any) -> List[Any]:
    """Return parallels rows stripped of heavyweight fields for live UI state."""
    if not isinstance(results, list):
        return []
    compacted = []
    for row in results:
        compact_row, _changed = _compact_parallels_result_row(row)
        compacted.append(compact_row)
    return compacted


def _compact_search_export_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    compacted, truncated, original, changed = _compact_results(
        payload.get('results'),
        _compact_search_result_row,
    )
    new_payload = dict(payload)
    if changed or new_payload.get('results') is not compacted:
        new_payload['results'] = compacted
    if truncated:
        new_payload['truncated'] = True
        new_payload['total_count'] = original
    else:
        new_payload['total_count'] = int(new_payload.get('total_count') or original)
        new_payload['truncated'] = bool(new_payload.get('truncated', False))
    return new_payload, changed


def _compact_parallels_export_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    results_capped, results_trunc, results_total, results_changed = _compact_results(
        payload.get('results'),
        _compact_parallels_result_row,
    )
    filtered_capped, filtered_trunc, filtered_total, filtered_changed = _compact_results(
        payload.get('filtered'),
        _compact_parallels_result_row,
    )
    changed = results_changed or filtered_changed
    new_payload = dict(payload)
    new_payload['results'] = results_capped
    new_payload['filtered'] = filtered_capped
    new_payload['truncated'] = bool(new_payload.get('truncated', False)) or results_trunc or filtered_trunc
    new_payload['total_count'] = int(new_payload.get('total_count') or results_total)
    new_payload['filtered_total_count'] = int(new_payload.get('filtered_total_count') or filtered_total)
    if results_trunc:
        new_payload['total_count'] = results_total
    if filtered_trunc:
        new_payload['filtered_total_count'] = filtered_total
    return new_payload, changed


def compact_user_storage_export_payloads(storage_payload: Any) -> bool:
    """Compact export payloads inside one NiceGUI user-storage dictionary.

    Returns True when the payload was modified. This is used both on normal
    writes and by startup maintenance for legacy oversized sessions.
    """
    if not isinstance(storage_payload, dict):
        return False
    changed = False
    search_payload = storage_payload.get(_SEARCH_KEY)
    if isinstance(search_payload, dict):
        compacted, did_change = _compact_search_export_payload(search_payload)
        if did_change:
            storage_payload[_SEARCH_KEY] = compacted
            changed = True
    parallels_payload = storage_payload.get(_PARALLELS_KEY)
    if isinstance(parallels_payload, dict):
        compacted, did_change = _compact_parallels_export_payload(parallels_payload)
        if did_change:
            storage_payload[_PARALLELS_KEY] = compacted
            changed = True
    return changed


def compact_nicegui_export_storage(storage: Any) -> Dict[str, Any]:
    """Compact loaded and on-disk NiceGUI export payloads.

    The function deliberately touches NiceGUI's storage internals rather than
    ``app.storage.user`` because it runs outside any single UI session.
    """
    summary = {
        'loaded_users_checked': 0,
        'loaded_users_compacted': 0,
        'files_checked': 0,
        'files_compacted': 0,
        'bytes_before': 0,
        'bytes_after': 0,
        'errors': 0,
    }

    for _session_id, payload in list((getattr(storage, '_users', {}) or {}).items()):
        summary['loaded_users_checked'] += 1
        try:
            if compact_user_storage_export_payloads(payload):
                summary['loaded_users_compacted'] += 1
        except Exception:
            summary['errors'] += 1

    storage_path = Path(getattr(storage, 'path', '.nicegui'))
    if not storage_path.exists():
        return summary

    for filepath in storage_path.glob('storage-user-*.json'):
        summary['files_checked'] += 1
        try:
            before = filepath.stat().st_size
            summary['bytes_before'] += before
            with filepath.open('r', encoding='utf-8') as handle:
                payload = json.load(handle)
            if not compact_user_storage_export_payloads(payload):
                summary['bytes_after'] += before
                continue

            tmp_path = filepath.with_name(
                f"{filepath.name}.tmp.{os.getpid()}"
            )
            with tmp_path.open('w', encoding='utf-8') as handle:
                json.dump(payload, handle, ensure_ascii=False, separators=(',', ':'))
            os.replace(tmp_path, filepath)
            after = filepath.stat().st_size
            summary['bytes_after'] += after
            summary['files_compacted'] += 1
        except Exception:
            summary['errors'] += 1
            try:
                summary['bytes_after'] += filepath.stat().st_size
            except Exception:
                pass
    return summary


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
    capped, truncated, original, _changed = _compact_results(
        results,
        _compact_search_result_row,
    )
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
    if not isinstance(payload, dict):
        return None
    compacted, changed = _compact_search_export_payload(payload)
    if changed:
        safe_user_set(_SEARCH_KEY, compacted)
    return compacted


def update_search_export_results(results: List[Dict[str, Any]]) -> None:
    """Patch only the ``results`` field (post-display-filter sync).

    Applies the same cap as ``set_search_export``; a filter that re-inflates
    above the cap (rare but possible after an undo) is still bounded.
    """
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return  # D-11: poisoned-shape or missing payload
    payload = dict(payload)  # D-12: copy-on-update
    capped, truncated, original, _changed = _compact_results(
        results,
        _compact_search_result_row,
    )
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
    results_capped, results_trunc, results_total, _results_changed = _compact_results(
        results,
        _compact_parallels_result_row,
    )
    filtered_capped, filtered_trunc, filtered_total, _filtered_changed = _compact_results(
        filtered,
        _compact_parallels_result_row,
    )
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
    if not isinstance(payload, dict):
        return None
    compacted, changed = _compact_parallels_export_payload(payload)
    if changed:
        safe_user_set(_PARALLELS_KEY, compacted)
    return compacted


def update_parallels_export_filtered(filtered: List[Dict[str, Any]]) -> None:
    """Patch only the ``filtered`` field (post-filter sync).

    Applies the same cap as ``set_parallels_export`` on the filtered list.
    """
    payload = safe_user_get(_PARALLELS_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    capped, truncated, original, _changed = _compact_results(
        filtered,
        _compact_parallels_result_row,
    )
    payload['filtered'] = capped
    payload['filtered_total_count'] = original
    # OR-merge truncation flag: if either list was capped at any point, the
    # payload as a whole is truncated.
    payload['truncated'] = bool(payload.get('truncated', False)) or truncated
    safe_user_set(_PARALLELS_KEY, payload)


def clear_parallels_export() -> None:
    """Remove the parallels export payload (New Search reset)."""
    safe_user_pop(_PARALLELS_KEY, None)
