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
import re
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
# weigh many MB when each carries a full manuscript transcription.
#
# SEED-002 (2026-05-19, fixup 2026-05-19): the row compactors use explicit
# allowlists storing query-specific fields plus the minimal identity fields
# needed to rehydrate display metadata at export/serialize time:
#   - search:    {uid, sys_id, sort_score, snippet, match_terms, raw_header}
#   - parallels: {uid, sys_id, sort_score, score, snippet, match_terms,
#                 source_ctx, text, raw_header, chunk_hits}
# Display fields (shelfmark, title, library_code, library_name) rehydrate
# from meta_mgr at export/serialize time via
# ``web.export_service._resolve_result_display`` and the analogous fallback
# in ``shared.search_serializer._serialize_item``. Full text rehydrates from
# Tantivy via ``_resolve_result_full_text``. The ed6f89c4 field-strip
# invariants are subsumed by the allowlist (full_text, raw_file_hl, content
# are all absent post-compaction).
#
# Why ``sys_id`` AND ``raw_header`` on search rows (initial fix kept neither):
#   - production text-search uids are ``IE..._P..._FL...`` with no sys_id digits
#     so ``parse_full_id_components(uid)['sys_id']`` returns None for the common
#     case (verified at genizah_core.py:3617-3666, 3652-3666). Without an
#     identity field, every exported shelfmark collapsed to 'Unknown'.
#   - metadata-only rows (Title/Shelfmark mode at genizah_core.py:7440-7462)
#     have uid='' AND raw_header=''; only ``display.id`` carries the sys_id.
#     The compactor extracts it into a top-level ``sys_id`` field so the
#     metadata-only path survives compaction.
#   - ``raw_header`` is also needed by ``shared.search_serializer._serialize_item``
#     to populate ``locator.volume_ie`` / ``locator.p_num`` for the public
#     /api/search export envelope.
#
# Why ``chunk_hits`` is kept on parallels (initial fix dropped it):
#   - shared/search_serializer.py:828 reads ``sub.get('chunk_hits')`` to build
#     the per-row ``matches`` array in /api/export/parallels/json. Dropping it
#     collapses every row to one degenerate match. Capped at 100 entries x
#     1000 chars (same caps as ed6f89c4) to bound storage.
#
# ``score`` and ``raw_header`` are INTENTIONALLY KEPT on parallels rows:
#   - live parallels UI reads them at 13 sites in web/pages/parallels.py
#     (score: 2827/2831/2865/2868/3123/3310/3372; raw_header:
#     2841/3134/3140/3359/3373) via compact_parallels_result_rows feeding
#     p_state.results
#   - shared/search_serializer.py:691 sums score into the public
#     /api/parallels aggregate_score; dropping it collapses sort order
#
# Why the 7 live-UI scalars (final_score, has_boundary_matches,
# boundary_quality, boundary_match_count, filter_reason, is_text_filtered,
# is_filtered) are kept on parallels rows (Codex round-2 catch + self-audit):
#   - compact_parallels_result_rows() at web/pages/parallels.py:2338-2339
#     overwrites main_results / filtered_results with the compacted version
#     BEFORE assigning into p_state.results. The live UI then reads:
#       * final_score / has_boundary_matches / boundary_quality /
#         boundary_match_count at parallels.py:3124-3127 to render the
#         "boost" score badge and boundary-match indicator chips.
#       * filter_reason / is_text_filtered / is_filtered at
#         parallels.py:3063-3071 to show the specific filter reason
#         ("Found in source text" / "High frequency" / generic "Filtered")
#         on filtered-result chips.
#     Without these in the allowlist the boost badge disappears, boundary
#     match counts collapse to 0, and filtered chips lose their specific
#     reason text (`is_filtered` fall-through is currently benign because
#     the post-loop default also says 'Filtered' -- but locking the contract
#     prevents future drift if non-display callers ever depend on the flag).
#     Cost: ~50 bytes/row of small scalars (1 float, 1 int, 3 bools,
#     1 short string, 1 derived int).
_EXPORT_RESULTS_CAP = 5000
_PARALLELS_TEXT_STORAGE_CHARS = 4000
_PARALLELS_CHUNK_HITS_CAP = 100
_PARALLELS_CHUNK_TEXT_STORAGE_CHARS = 1000

# Allowlists: only these keys are kept in stored / live-state rows.
_SEARCH_ROW_ALLOWLIST = frozenset((
    'uid', 'sys_id', 'sort_score', 'snippet', 'match_terms', 'raw_header',
))
_PARALLELS_ROW_ALLOWLIST = frozenset((
    'uid', 'sys_id', 'sort_score', 'score', 'snippet', 'match_terms',
    'source_ctx', 'text', 'raw_header', 'chunk_hits',
    # Live-UI scalars (see module docstring "Why the 7 live-UI scalars..."):
    'final_score', 'has_boundary_matches', 'boundary_quality',
    'boundary_match_count', 'filter_reason', 'is_text_filtered',
    'is_filtered',
))

# Pre-compiled at module load for the per-row compaction hot path. Matches
# the production sys_id pattern (99 followed by 8+ digits, e.g. 99001234567890
# or the Phase-85 synthetic 18-digit shape).
_SYS_ID_RE = re.compile(r'(99\d{8,})')


def _extract_sys_id_from_row(row: Dict[str, Any]) -> str:
    """Best-effort sys_id extraction at compaction time.

    Used to populate the top-level ``sys_id`` field before allowlist filtering
    drops the heavy ``display`` dict. Looking in priority order:

      1. ``row['sys_id']`` (already set by caller / earlier compaction step)
      2. ``row['display']['id']`` (legacy / metadata-only rows -- the only
         channel that carries sys_id when both uid and raw_header are empty)
      3. ``row['raw_header']`` regex (normal text-search rows)
      4. ``row['uid']`` regex (legacy rows where uid was a sys-id-bearing string)

    Returns '' when no sys_id can be derived. The resolver / serializer then
    falls back to the 'Unknown' / 'ID: {sys_id}' chain.
    """
    existing = row.get('sys_id')
    if isinstance(existing, str) and existing:
        return existing
    display = row.get('display')
    if isinstance(display, dict):
        did = display.get('id')
        if isinstance(did, str) and did:
            return did
    raw_header = row.get('raw_header')
    if isinstance(raw_header, str) and raw_header:
        m = _SYS_ID_RE.search(raw_header)
        if m:
            return m.group(1)
    uid = row.get('uid')
    if isinstance(uid, str) and uid:
        m = _SYS_ID_RE.search(uid)
        if m:
            return m.group(1)
    return ''


def _compact_chunk_hit(hit: Any) -> Tuple[Any, bool]:
    """Compact one parallels chunk_hit while preserving tuple/list shape.

    Restored from ed6f89c4 for SEED-002 fixup. chunk_hits tuples are
    (chunk_index, source_chunk_text, score, manuscript_snippet); indices 1
    and 3 are strings capped at ``_PARALLELS_CHUNK_TEXT_STORAGE_CHARS``.
    """
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


def _compact_search_result_row(row: Any) -> Tuple[Any, bool]:
    """Return an allowlist-pruned search result row plus a changed flag.

    SEED-002 fixup (2026-05-19): kept keys are the allowlist intersection of
    the input dict, plus a synthesized ``sys_id`` extracted from
    ``display.id`` / ``raw_header`` / ``uid`` BEFORE the display dict is
    dropped. All other fields (display, full_text, full_text_excerpt,
    raw_file_hl, content, score, etc.) are dropped — they rehydrate from
    meta_mgr / Tantivy at export time.
    """
    if not isinstance(row, dict):
        return row, False

    # Synthesize sys_id BEFORE allowlist filtering -- this is the only chance
    # to read display.id (the canonical channel for metadata-only rows, which
    # have uid='' AND raw_header='').
    synth_sys_id = _extract_sys_id_from_row(row)

    kept: Dict[str, Any] = {}
    for key in _SEARCH_ROW_ALLOWLIST:
        if key in row:
            kept[key] = row[key]
    if synth_sys_id and not kept.get('sys_id'):
        kept['sys_id'] = synth_sys_id

    changed = bool(set(row.keys()) - _SEARCH_ROW_ALLOWLIST)
    # Also flag changed when we ADDED sys_id (caller didn't supply it but we
    # synthesized one) -- ensures downstream truncated/changed tracking is
    # honest about the rewrite.
    if synth_sys_id and not row.get('sys_id'):
        changed = True
    return kept, changed


def _compact_parallels_result_row(row: Any) -> Tuple[Any, bool]:
    """Return an allowlist-pruned parallels result row plus a changed flag.

    SEED-002 fixup (2026-05-19): kept keys are the allowlist intersection of
    the input dict; ``source_ctx`` and ``text`` retain the 4000-char
    truncation cap from ed6f89c4. ``chunk_hits`` is kept (capped at
    ``_PARALLELS_CHUNK_HITS_CAP`` entries x
    ``_PARALLELS_CHUNK_TEXT_STORAGE_CHARS`` chars per index, matching
    ed6f89c4) because the public ``/api/export/parallels/json`` serializer
    reads it at ``shared/search_serializer.py:828``. ``sys_id`` is synthesized
    from display.id / raw_header before display is dropped. All other fields
    (display, full_text, raw_file_hl, content, etc.) are dropped.
    """
    if not isinstance(row, dict):
        return row, False

    synth_sys_id = _extract_sys_id_from_row(row)

    kept: Dict[str, Any] = {}
    for key in _PARALLELS_ROW_ALLOWLIST:
        if key in row:
            kept[key] = row[key]
    if synth_sys_id and not kept.get('sys_id'):
        kept['sys_id'] = synth_sys_id

    changed = bool(set(row.keys()) - _PARALLELS_ROW_ALLOWLIST)
    if synth_sys_id and not row.get('sys_id'):
        changed = True

    # Preserve the source_ctx / text 4000-char cap for storage safety —
    # these fields are the longest survivors in the new allowlist and a
    # pathological caller could still push KB-range strings here.
    for key in ('source_ctx', 'text'):
        value = kept.get(key)
        if isinstance(value, str):
            truncated = value[:_PARALLELS_TEXT_STORAGE_CHARS]
            if truncated != value:
                kept[key] = truncated
                changed = True

    # chunk_hits cap: 100 entries x 1000 chars per text index, restored from
    # ed6f89c4 to bound worst-case storage at ~200 KB/row. Without this a
    # composition-search row with thousands of chunk_hits could blow past
    # the 5MB-per-row target on its own.
    chunk_hits = kept.get('chunk_hits')
    if isinstance(chunk_hits, list):
        source_hits = chunk_hits[:_PARALLELS_CHUNK_HITS_CAP]
        if len(source_hits) != len(chunk_hits):
            changed = True
        compact_hits = []
        for hit in source_hits:
            compact_hit, hit_changed = _compact_chunk_hit(hit)
            compact_hits.append(compact_hit)
            changed = changed or hit_changed
        kept['chunk_hits'] = compact_hits

    return kept, changed


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
