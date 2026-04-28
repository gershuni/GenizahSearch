# -*- coding: utf-8 -*-
"""
Search Serializer for Phase 77 JSON exports + Phase 78+ /api/* responses.

This module is the SINGLE SOURCE OF TRUTH for the "Claude-friendly JSON" payload
shape. Two named functions share a private _serialize_item() per D-14 / EXPORT-03:

    serialize_search_payload(results, *, meta_mgr, query, mode, ...) -> dict
    serialize_parallels_payload(main, filtered, *, meta_mgr, source_text, ...) -> dict

Both Phase 77 download handlers (web/api.py /api/export/json + /api/export/parallels/json)
AND Phase 78 /api/search + Phase 80 /api/parallels import these functions; modifying
_serialize_item() updates download AND API in lockstep.

Architecture decisions locked in 77-01-PLAN.md (deviations from 77-CONTEXT.md):
    - `domains: list[str]` (plural, forward-compatible) instead of D-01 singular `domain`
    - Image URL is server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` or null
      (CONTEXT.md D-08 was based on a misconception: display['img'] is a page number, NOT a URL)
    - HIGH-07: image_url is also null for non-NLI providers (Oxford and any future provider
      lacking NLI mirroring) -- see NLI_RESOLVABLE_LIBRARY_CODES below
    - chunk_index is 0-based (matches existing `for i, ...` loop in lab_composition_search)
    - D-13 Path A: consume `chunk_hits` field that Plan 02 added to results_map[uid]
    - HIGH-05: DO NOT call .close() on the FJMS service -- it is a module-level singleton at
      shared/fjms_service.py:3160-3169; close is reserved for reset_fjms_service()
    - HIGH-06: filename builders use MILLISECOND resolution so consecutive same-second
      calls produce distinct filenames (EXPORT-04). Prior second-resolution scheme collided.

The serializer is read-only. It does not mutate inputs. It calls existing helpers:
    - shared_export_utils.remove_highlight_markers (strip *term* markers)
    - meta_mgr.parse_full_id_components (header -> {sys_id, ie_id, p_num, fl_id})
    - shared.fjms_service for batch domain/catalog lookup (graceful when fjms unavailable)
    - genizah_core.get_library_display for library full names (graceful when import fails)

Memory note (LOW-02): the parallels code path consumes `chunk_hits` populated by Plan 02
in genizah_core. Each chunk_hit tuple includes a manuscript_snippet substring of the
already-retained `content` string; per-result memory grows by O(hits x snippet_len) but
is bounded by what's already in memory. No new pressure beyond Plan 02's STRIDE T-77.02-02.
"""

from __future__ import annotations

import itertools
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Schema version -- bump if envelope/item shape changes incompatibly.
# Phase 77 ships with version 1; Phase 78+ inherit unchanged.
SCHEMA_VERSION = 1


# -----------------------------------------------------------------------------
# Provider whitelist for image_url (HIGH-07)
# -----------------------------------------------------------------------------
# library_code values whose manuscripts have NLI IIIF coverage. Hits from any
# other provider (Oxford, future providers) get image_url=null. Conservative
# whitelist -- when in doubt, return null and let Phase 79 /api/browse resolve.
NLI_RESOLVABLE_LIBRARY_CODES = frozenset({
    'CUL', 'JTS', 'BL', 'Manchester', 'RNL', 'AIU', 'Mosseri', 'Gaster', 'Halper',
})


# -----------------------------------------------------------------------------
# Filename uniqueness counter (HIGH-06)
# -----------------------------------------------------------------------------
# Module-level monotonic counter ensures consecutive same-millisecond calls also
# produce distinct filenames (extreme edge case but covered). The counter is
# combined with the millisecond timestamp so filenames remain time-sortable.
_filename_counter = itertools.count()


# -----------------------------------------------------------------------------
# Private helpers
# -----------------------------------------------------------------------------

def _extract_match_terms(snippet: Optional[str]) -> list[str]:
    """D-03: Extract unique *term* markers in order of first appearance.

    Snippet 'foo *bar* baz *qux* *bar*' -> ['bar', 'qux'] (deduped, in-order).
    """
    if not snippet:
        return []
    found = re.findall(r'\*([^*]+)\*', snippet)
    seen: set[str] = set()
    out: list[str] = []
    for t in found:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _build_image_url(
    sys_id: Optional[str],
    p_num: Optional[str],
    library_code: Optional[str] = None,
) -> Optional[str]:
    """Plan 01 lock + HIGH-07: server-relative image URL or None.

    Returns `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` (0-based page index
    matching existing UI in web/pages/search_results.py:629-657) ONLY when:
      1. sys_id is non-empty
      2. p_num is non-empty and parses to a non-negative int
      3. library_code is in the NLI-resolvable whitelist

    Returns None for:
      - metadata-only hits (no p_num)
      - non-NLI providers (Oxford, etc.) -- even when sys_id+p_num are populated
      - unknown/empty library_code (conservative)

    No Oxford fallback, no multi-IE branching. Phase 79 /api/browse owns image
    canonicalization per CONTEXT.md D-08.
    """
    if not sys_id or not p_num:
        return None
    # HIGH-07: gate on provider. Conservative -- null when unknown.
    if not library_code or library_code not in NLI_RESOLVABLE_LIBRARY_CODES:
        return None
    try:
        page_idx = max(0, int(p_num) - 1)
    except (ValueError, TypeError):
        return None
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"


def _safe_library_name(code: Optional[str]) -> str:
    """Resolve library code -> full English name; degrade gracefully on import failure."""
    if not code:
        return ''
    try:
        from genizah_core import get_library_display
        return get_library_display(code, short=False, lang='en') or code
    except Exception:
        return code or ''


def _safe_fjms_lookups(sys_ids: list[str]) -> tuple[dict, dict]:
    """Batch domain + catalog lookup via shared.fjms_service. Graceful when unavailable.

    Returns (domain_batch, catalog_batch).
        domain_batch: dict[sys_id, list[domain_dict]] where domain_dict has 'domain', 'domain_heb' keys
        catalog_batch: dict[sys_id, catalog_dict] where catalog_dict has 'copy_date' key

    Both empty when fjms.is_available() returns False.

    HIGH-05: DO NOT call .close() on the returned service. shared/fjms_service.py:3160-3169
    returns a module-level singleton; closing it leaves subsequent callers (search
    enrichment, parallels enrichment, browse enrichment) with a closed connection.
    The serializer uses the service for the duration of the call but does not
    own the lifecycle. close() is reserved for shared.fjms_service.reset_fjms_service()
    which is called only when the fjms_enrichment.db sidecar file is replaced.
    """
    domain_batch: dict[str, list[dict]] = {}
    catalog_batch: dict[str, dict] = {}
    if not sys_ids:
        return domain_batch, catalog_batch
    try:
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        if fjms.is_available():
            try:
                domain_batch = fjms.get_domains_for_sys_ids(sys_ids) or {}
            except Exception as e:
                logger.warning(f"FJMS domain batch failed: {e}")
            for sid in sys_ids:
                try:
                    cat = fjms.get_catalog(sid)
                    if cat:
                        catalog_batch[sid] = cat
                except Exception:
                    pass  # Per-record failure is non-fatal; field falls back to None
        # HIGH-05: DO NOT close -- singleton is shared system-wide.
    except ImportError:
        # FJMS service not available; fields will be empty
        pass
    return domain_batch, catalog_batch


def _serialize_item(
    result: dict,
    *,
    meta_mgr: Any,
    domain_batch: dict[str, list[dict]],
    catalog_batch: dict[str, dict],
) -> dict:
    """Single source of truth for per-item shape (D-14 / EXPORT-03).

    Used by serialize_search_payload directly and by serialize_parallels_payload
    via _to_parallels_envelope_item which wraps a synthetic result dict.

    Per-item shape (D-01 modified, D-02, D-03, D-04):
        {uid, locator, score, shelfmark, title, library, domains, dating,
         snippet, excerpt, match_terms, image_url}
    """
    from shared_export_utils import remove_highlight_markers

    display = result.get('display', {}) or {}
    sys_id_raw = display.get('id', '') or ''

    # Locator from raw_header -- D-04 always-present, fields may be null
    raw_header = result.get('raw_header', '') or ''
    if raw_header and meta_mgr is not None:
        try:
            parsed = meta_mgr.parse_full_id_components(raw_header) or {}
        except Exception:
            parsed = {}
    else:
        parsed = {}
    final_sys_id = sys_id_raw or parsed.get('sys_id') or ''

    # Snippet stripped + match_terms (D-03)
    snippet_raw = result.get('snippet', '') or ''
    snippet_clean = remove_highlight_markers(snippet_raw)
    match_terms = _extract_match_terms(snippet_raw)

    # Excerpt slice (D-02 -- no full_text field)
    full_text = result.get('full_text', '') or ''
    excerpt = full_text[:500] if full_text else ''

    # Library
    library_code = display.get('library_code', '') or ''
    library_name = _safe_library_name(library_code)

    # Domains (Plan 01 lock: plural list -- empty list when unknown)
    domains: list[str] = []
    if final_sys_id:
        domain_records = domain_batch.get(final_sys_id) or []
        domains = [d.get('domain', '') for d in domain_records if d.get('domain')]

    # Dating (FJMS catalog.copy_date; Hebrew text from FJMS)
    dating: Optional[str] = None
    if final_sys_id:
        cat = catalog_batch.get(final_sys_id) or {}
        dating = cat.get('copy_date') or None

    # Score -- round to 4 decimals
    score_raw = result.get('sort_score')
    if score_raw is None:
        score_raw = result.get('score', 0)
    try:
        score = round(float(score_raw or 0), 4)
    except (ValueError, TypeError):
        score = 0.0

    return {
        'uid': result.get('uid', '') or '',
        'locator': {
            'sys_id': final_sys_id or None,
            'volume_ie': parsed.get('ie_id'),
            'p_num': parsed.get('p_num'),
        },
        'score': score,
        'shelfmark': display.get('shelfmark', '') or '',
        'title': display.get('title', '') or '',
        'library': {'code': library_code, 'name': library_name},
        'domains': domains,
        'dating': dating,
        'snippet': snippet_clean,
        'excerpt': excerpt,
        'match_terms': match_terms,
        # HIGH-07: pass library_code so non-NLI providers get null
        'image_url': _build_image_url(final_sys_id, parsed.get('p_num'), library_code),
    }


def _utc_iso_now() -> str:
    """Generated-at ISO8601 UTC, second-resolution: '2026-04-27T15:30:42Z'."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# -----------------------------------------------------------------------------
# Filename builders (EXPORT-04 -- HIGH-06 fix: millisecond resolution + counter)
# -----------------------------------------------------------------------------

def _filename_timestamp_with_ms() -> str:
    """Millisecond-resolution UTC timestamp + monotonic counter for filenames.

    Format: '2026-04-27T153042_837_n' where _837 is the millisecond and _n is
    a monotonically-incrementing counter (0, 1, 2, ...). The counter guarantees
    distinct filenames even if two calls land in the same millisecond -- extreme
    edge case but eliminates the failure mode entirely.

    EXPORT-04 / HIGH-06: prior revision used second-resolution and would collide
    on fast consecutive clicks (`time.sleep(1.0)` was hiding the bug in tests).
    This scheme makes the unit test `test_filename_uniqueness_consecutive` pass
    without sleeping at all.
    """
    now = datetime.now(timezone.utc)
    # %f produces microseconds; slice to first 3 chars for milliseconds.
    base = now.strftime('%Y-%m-%dT%H%M%S')
    ms = f"{now.microsecond // 1000:03d}"
    counter = next(_filename_counter)
    return f"{base}_{ms}_{counter}"


def build_search_filename() -> str:
    """Search download filename: 'genizah-search-{ts}.json'."""
    return f"genizah-search-{_filename_timestamp_with_ms()}.json"


def build_parallels_filename() -> str:
    """Parallels download filename: 'genizah-parallels-{ts}.json'."""
    return f"genizah-parallels-{_filename_timestamp_with_ms()}.json"


# -----------------------------------------------------------------------------
# Public: serialize_search_payload
# -----------------------------------------------------------------------------

def serialize_search_payload(
    results: list[dict],
    *,
    meta_mgr: Any,
    query: str = '',
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
    total: Optional[int] = None,
) -> dict:
    """Phase 77 EXPORT-01/03. Same shape Phase 78 /api/search will inherit.

    Args:
        results: list of dicts as returned by SearchEngine.execute_search; each has
                 'display', 'snippet', 'full_text', 'sort_score', 'uid', 'raw_header'.
        meta_mgr: MetadataManager instance with parse_full_id_components(header).
        query: clean query string echoed in envelope (D-06).
        mode: search mode (text/Title/Shelfmark/Responsa) echoed (D-06).
        gap: word-gap parameter echoed when present (D-06).
        filters: page-level filter dict echoed (D-06).
        warnings: cascade-downgrade or cap-truncation warnings (D-07; always emitted as []).
        total: server-side total count when results is a paginated subset; defaults to len(results).

    Returns:
        Envelope dict per D-05/06/07/09/10. JSON-serializable with json.dumps(ensure_ascii=False).
    """
    # Batch domain + catalog lookup
    sys_ids = [
        (r.get('display') or {}).get('id', '')
        for r in results
        if (r.get('display') or {}).get('id')
    ]
    sys_ids = [s for s in sys_ids if s]
    domain_batch, catalog_batch = _safe_fjms_lookups(sys_ids)

    items = [
        _serialize_item(
            r,
            meta_mgr=meta_mgr,
            domain_batch=domain_batch,
            catalog_batch=catalog_batch,
        )
        for r in results
    ]

    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'search',
        'query': query or '',
        'mode': mode or 'text',
        'gap': gap,
        'filters': filters,
        'count': len(items),
        'total': total if total is not None else len(items),
        'warnings': list(warnings) if warnings else [],
        'generated_at': _utc_iso_now(),
        'results': items,
    }


# -----------------------------------------------------------------------------
# Public: serialize_parallels_payload
# -----------------------------------------------------------------------------

def _group_parallels_by_sys_id(items: list[dict], *, meta_mgr: Any) -> list[dict]:
    """Group raw parallels items by sys_id derived from raw_header. D-13 grouping core.

    Returns sorted-descending list of group dicts:
        {sys_id, representative, items: [...], aggregate_score: float}
    """
    groups: dict[str, dict] = {}
    for item in items:
        raw_header = item.get('raw_header', '') or ''
        parsed = {}
        if raw_header and meta_mgr is not None:
            try:
                parsed = meta_mgr.parse_full_id_components(raw_header) or {}
            except Exception:
                parsed = {}
        sys_id = parsed.get('sys_id') or 'unknown'
        grp = groups.setdefault(sys_id, {
            'sys_id': sys_id,
            'representative': item,
            'items': [],
            'aggregate_score': 0.0,
        })
        grp['items'].append(item)
        # Plan 01 lock: SUM aggregation across uids in same sys_id
        try:
            grp['aggregate_score'] += float(item.get('score', 0.0) or 0.0)
        except (ValueError, TypeError):
            pass
    return sorted(groups.values(), key=lambda g: g['aggregate_score'], reverse=True)


def _to_parallels_envelope_item(
    group: dict,
    *,
    meta_mgr: Any,
    domain_batch: dict,
    catalog_batch: dict,
) -> dict:
    """Convert a sys_id group to a parallels envelope item with matches[]. D-13.

    Wraps the shared _serialize_item by building a synthetic "result-like" dict
    from the representative item, then adds the parallels-specific `matches` array.
    """
    from shared_export_utils import remove_highlight_markers

    rep = group['representative']
    sys_id = group['sys_id']

    # Build a synthetic result dict the shared _serialize_item can consume.
    # Use rep['raw_header'] and synthesize a minimal display from meta_mgr if missing.
    synth: dict = dict(rep)
    synth['sort_score'] = round(group['aggregate_score'], 4)
    if 'display' not in synth or not synth['display']:
        shelf = ''
        title = ''
        lib_code = ''
        if meta_mgr is not None and sys_id and sys_id != 'unknown':
            try:
                meta = meta_mgr.get_meta_for_id(sys_id)
                if isinstance(meta, tuple) and len(meta) >= 2:
                    shelf, title = meta[0] or '', meta[1] or ''
                try:
                    lib_code = meta_mgr.get_library_for_id(sys_id) or ''
                except Exception:
                    lib_code = ''
            except Exception:
                pass
        synth['display'] = {
            'id': sys_id if sys_id != 'unknown' else '',
            'shelfmark': shelf,
            'title': title,
            'library_code': lib_code,
        }

    # Map parallels-shape fields to search-shape so _serialize_item produces
    # a meaningful snippet/excerpt/match_terms. Parallels items use 'text'
    # (manuscript snippet WITH *term* markers) and either 'full_text' (lab
    # mode, line 1488) or 'content' (standard mode does not surface this) --
    # fall back to 'text' when neither is present so excerpt is non-empty.
    if not synth.get('snippet'):
        synth['snippet'] = rep.get('text', '') or ''
    if not synth.get('full_text'):
        synth['full_text'] = (
            rep.get('full_text', '')
            or rep.get('content', '')
            or rep.get('text', '')
            or ''
        )

    item = _serialize_item(
        synth,
        meta_mgr=meta_mgr,
        domain_batch=domain_batch,
        catalog_batch=catalog_batch,
    )

    # D-13 matches[] -- consume Plan 02's chunk_hits when it is a list of
    # per-chunk tuples (lab_composition_search path). Two collision cases fall
    # through to the Path B degenerate single-match:
    #   1. chunk_hits is missing/empty (legacy callers, future producers).
    #   2. chunk_hits is an int -- search_composition_logic uses the same key
    #      name for a chunk-COUNT counter (genizah_core.py:7651, 7740, 7854).
    #      Iterating an int raises TypeError; defensively skip and degrade.
    # Group-level dedup: NLI sometimes catalogs the same physical manuscript
    # under multiple Alma uids on the same sys_id. Per-uid dedup in core can't
    # catch this because each uid has its own rec. Keep the highest-scoring
    # match per (chunk_index, stripped_snippet) within the group.
    seen: dict[tuple[Optional[int], str], int] = {}
    matches: list[dict] = []

    def _push_match(entry: dict) -> None:
        key = (entry.get('chunk_index'), entry.get('manuscript_snippet') or '')
        existing_idx = seen.get(key)
        if existing_idx is None:
            seen[key] = len(matches)
            matches.append(entry)
        elif entry.get('score', 0) > matches[existing_idx].get('score', 0):
            matches[existing_idx] = entry

    for sub in group['items']:
        chunk_hits = sub.get('chunk_hits')
        if isinstance(chunk_hits, list) and chunk_hits:
            for tup in chunk_hits:
                # Tuple shape: (chunk_index, source_chunk_text, score, manuscript_snippet)
                if len(tup) < 4:
                    continue
                ch_idx, ch_text, ch_score, ms_snip = tup[0], tup[1], tup[2], tup[3]
                _push_match({
                    'chunk_index': int(ch_idx) if isinstance(ch_idx, (int, float)) else None,
                    'source_chunk_text': ch_text or '',
                    'manuscript_snippet': remove_highlight_markers(ms_snip or ''),
                    'score': round(float(ch_score or 0), 4),
                })
        else:
            # Path B fallback (degenerate single match) -- triggers for callers
            # without Plan 02 attribution AND for the standard-mode int-counter
            # collision described above.
            _push_match({
                'chunk_index': None,
                'source_chunk_text': sub.get('source_ctx', '') or '',
                'manuscript_snippet': remove_highlight_markers(sub.get('text', '') or ''),
                'score': round(float(sub.get('score', 0.0) or 0.0), 4),
            })
    # Sort by chunk_index (ascending; None goes first) for stable, readable output.
    matches.sort(key=lambda m: (m.get('chunk_index') is not None, m.get('chunk_index') or 0))
    item['matches'] = matches
    return item


def serialize_parallels_payload(
    main_results: list[dict],
    filtered_results: Optional[list[dict]] = None,
    *,
    meta_mgr: Any,
    source_text: str = '',
    chunk_size: Optional[int] = 5,
    mode: str = 'exact',
    max_freq: Optional[float] = None,
    boundary_options: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
) -> dict:
    """Phase 77 EXPORT-02/03. Same shape Phase 80 /api/parallels will inherit.

    Args:
        main_results: list of raw parallels result dicts (each has raw_header, score,
                      source_ctx, text, optionally chunk_hits per Plan 02).
        filtered_results: separate list of high-frequency / filtered hits per D-11.
        meta_mgr: MetadataManager for parse_full_id_components and get_meta_for_id.
        source_text: the input composition text echoed in envelope (D-06).
        chunk_size, mode, max_freq, boundary_options: search parameters echoed (D-06).
        warnings: any cascade-downgrade or restored-from-history warnings (D-07).

    Returns:
        Envelope dict with separate `results` (main, sorted desc by aggregate_score)
        and `filtered` arrays per D-11. Each item has matches[] per D-13.
    """
    filtered_results = filtered_results or []

    main_groups = _group_parallels_by_sys_id(main_results, meta_mgr=meta_mgr)
    filt_groups = _group_parallels_by_sys_id(filtered_results, meta_mgr=meta_mgr)

    # Batch lookups across both groups
    all_sys_ids = list({
        g['sys_id'] for g in (main_groups + filt_groups)
        if g['sys_id'] and g['sys_id'] != 'unknown'
    })
    domain_batch, catalog_batch = _safe_fjms_lookups(all_sys_ids)

    main_envelope = [
        _to_parallels_envelope_item(
            g, meta_mgr=meta_mgr,
            domain_batch=domain_batch, catalog_batch=catalog_batch,
        )
        for g in main_groups
    ]
    filt_envelope = [
        _to_parallels_envelope_item(
            g, meta_mgr=meta_mgr,
            domain_batch=domain_batch, catalog_batch=catalog_batch,
        )
        for g in filt_groups
    ]

    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'parallels',
        'source_text': source_text or '',
        'chunk_size': chunk_size,
        'mode': mode or 'exact',
        'max_freq': max_freq,
        'boundary_options': boundary_options,
        'count': len(main_envelope),
        'total': len(main_envelope),
        'warnings': list(warnings) if warnings else [],
        'generated_at': _utc_iso_now(),
        'results': main_envelope,
        'filtered': filt_envelope,
    }
