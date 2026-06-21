# -*- coding: utf-8 -*-
"""Versioned safe_storage helpers for the ``joins_lab`` namespace.

All Joins Lab per-user state is funnelled through this module — callers never
touch NiceGUI storage directly; that raw access is forbidden by the Phase 87
CI guard (``tests/test_no_raw_storage_access.py``, allowlist ``[]``).

Schema (extended in Phase 120; same ``schema_version: 1`` as Phase 117):

.. code-block:: python

    {
        'schema_version': 1,            # int; increment ONLY on breaking shape change
        # Phase 117 (anchor identity):
        'anchor_sys_id': str | None,
        'anchor_fl_id': str | None,
        'anchor_volume_ie': str | None,
        # Phase 120 additions (additive, non-breaking):
        # builder_rows is the builder's WORD-MODEL lines_state — each row is a LINE:
        #   {'words': [{'term', 'mods', 'gap_to_next_word'}], 'line_start',
        #    'line_end', 'gap_to_next_line'}.  (A legacy flat shape
        #   {'term', 'gap_to_next', 'modifiers'} is still tolerated on read.)
        'builder_rows': list,           # word-model lines_state; max 20 lines
        'builder_mode': str,            # UI SEARCH TYPE: responsa|exact|variants|fuzzy|regex
        'variants_on': bool,            # Responsa-style Variants toggle
        'single_text': str,             # single-line-mode query (not in lines_state)
        'text_position': str,           # 'anywhere' | 'start' | 'end' | 'line_start' | 'line_end'
        'flex_spacing': bool,
        'bidirectional': bool,
        'other_side_enabled': bool,
        'other_side_rows': list,        # same shape as builder_rows; max 20 lines
        'other_side_combine': str,      # 'narrow' | 'widen'
        'triage': dict,                 # {sys_id: 'yes'|'maybe'|'no'} max 500 entries
        'active_filter': dict,          # compact filter discriminants only (< 4 KB)
        'view_mode': str,               # 'grid' | 'table'
    }

**Schema version note (STAYS AT 1):**
Phase 120 only ADDS keys — they are non-breaking.  Old v1 blobs (anchor only)
are missing the new keys; callers read them with ``.get(key, default)`` so they
restore cleanly.  Do NOT bump to 2 — ``read_joins_lab_state()`` performs an
exact-match version check and would DISCARD every existing user's anchor blob.
Bump the version ONLY when a key is removed or its type changes.

**Size discipline (search-history-bloat class of bug):**
NEVER write ``full_text``, image bytes, result lists, or other blobs.  Payload
is built explicitly from named parameters — no arbitrary ``**kwargs`` are
copied into the stored dict.  See CHANGELOG v7.16 for the 778 MB
``search_history.json`` incident that motivated this constraint.

**puzzle_staging companion key:**
:func:`clear_joins_lab_state` also pops the ``puzzle_staging`` key (D-16) so a
reset cannot leave stale cross-session staging state.
"""
from __future__ import annotations

import dataclasses

from typing import Any, Optional

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JOINS_LAB_KEY = 'joins_lab'
_SCHEMA_VERSION = 1  # KEEP AT 1 — Phase 120 additions are non-breaking (see docstring)

_PUZZLE_STAGING_KEY = 'puzzle_staging'

# ---------------------------------------------------------------------------
# Results snapshot — survives navigation so /joins-lab restores results intact
# ---------------------------------------------------------------------------
# The candidate result set is persisted under its OWN per-user key (NOT the main
# ``joins_lab`` blob, so its schema-version gate is independent), via the
# safe_storage chokepoint — exactly how /search persists ``search_results``
# (``persist_search_snapshot``).  This is read in the deferred page bootstrap,
# where per-user storage is reliably available (``app.storage.tab`` is NOT — the
# client has not re-handshaked its tab id yet, so a tab read comes back empty).
#
# Blob discipline (the 778 MB ``search_history.json`` lesson): this is a SINGLE,
# strictly-bounded snapshot — full_text truncated to _SNAPSHOT_FULLTEXT_CAP and
# at most _MAX_SNAPSHOT_CANDIDATES candidates per list (well under /search's
# 5000 cap).  It is NOT the unbounded "store every result of every search"
# pattern that caused the incident.
_RESULTS_KEY = 'joins_lab_results'
_RESULTS_SNAPSHOT_VERSION = 1
_MAX_SNAPSHOT_CANDIDATES = 300       # hard cap on either candidate list
_SNAPSHOT_FULLTEXT_CAP = 500         # chars; truncate heavy transcription text

# Size-cap constants (threat model T-120-blob)
_MAX_BUILDER_ROWS = 20          # max lines per builder (matches UI widget max)
_MAX_WORDS_PER_ROW = 50         # max words per line (generous; UI rarely exceeds a few)
_MAX_TERM_CHARS = 200           # max chars per word term
_MAX_TRIAGE_ENTRIES = 500       # max entries in the sys_id-keyed triage dict


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cap_rows(rows: Any) -> list:
    """Sanitise and cap a builder ``lines_state`` list (blob discipline).

    Caps at :data:`_MAX_BUILDER_ROWS` lines and truncates every term to
    :data:`_MAX_TERM_CHARS`.  Preserves the builder's CURRENT word-model line
    shape so the persisted query actually round-trips:

        {'words': [{'term', 'mods', 'gap_to_next_word'}, ...],
         'line_start': bool, 'line_end': bool, 'gap_to_next_line': int}

    Historical bug (fixed here): this used to coerce every row to a flat
    ``{'term', 'gap_to_next', 'modifiers'}`` shape.  The builder hasn't produced
    that shape since the word-level model landed — each line carries ``words``,
    NOT a top-level ``term`` — so persistence silently flattened every query to
    empty terms and a session restore came back with an empty builder ("Enter at
    least one search line to run").  We now preserve the word-model and only fall
    back to the flat shape for genuinely legacy rows (no ``words`` key).
    """
    if not isinstance(rows, list):
        return []
    capped: list = []
    for row in rows[:_MAX_BUILDER_ROWS]:
        if not isinstance(row, dict):
            continue
        if isinstance(row.get('words'), list):
            # Current word-model line — preserve words + line anchors/gap.
            words: list = []
            for w in row['words'][:_MAX_WORDS_PER_ROW]:
                if not isinstance(w, dict):
                    continue
                term = w.get('term', '')
                if not isinstance(term, str):
                    term = str(term)
                mods = w.get('mods', {})
                words.append({
                    'term': term[:_MAX_TERM_CHARS],
                    'mods': mods if isinstance(mods, dict) else {},
                    'gap_to_next_word': w.get('gap_to_next_word', 0),
                })
            capped.append({
                'words': words,
                'line_start': bool(row.get('line_start', False)),
                'line_end': bool(row.get('line_end', False)),
                'gap_to_next_line': row.get('gap_to_next_line', 0),
            })
        else:
            # Legacy flat row {'term', 'gap_to_next', 'modifiers'} — tolerated.
            term = row.get('term', '')
            if not isinstance(term, str):
                term = str(term)
            capped.append({
                'term': term[:_MAX_TERM_CHARS],
                'gap_to_next': row.get('gap_to_next', 0),
                'modifiers': row.get('modifiers', {}),
            })
    return capped


def _cap_triage(triage: Any) -> dict:
    """Cap the triage dict at :data:`_MAX_TRIAGE_ENTRIES` entries.

    Eviction policy (LRU-style): when ``len(triage) > _MAX_TRIAGE_ENTRIES``,
    preserve all entries whose verdict is ``'yes'`` or ``'no'`` (decided), then
    fill up to the cap with ``'maybe'`` entries (undecided), discarding the
    oldest undecided entries first (dict insertion order preserved in Python
    3.7+).
    """
    if not isinstance(triage, dict):
        return {}
    if len(triage) <= _MAX_TRIAGE_ENTRIES:
        return dict(triage)

    # Split into decided (yes/no) and undecided (maybe)
    decided: dict = {}
    undecided: dict = {}
    for sys_id, verdict in triage.items():
        if verdict in ('yes', 'no'):
            decided[sys_id] = verdict
        else:
            undecided[sys_id] = verdict

    # Fill from decided first, then undecided up to cap
    result: dict = {}
    for sys_id, verdict in decided.items():
        if len(result) >= _MAX_TRIAGE_ENTRIES:
            break
        result[sys_id] = verdict

    remaining_slots = _MAX_TRIAGE_ENTRIES - len(result)
    # Guard remaining_slots > 0 BEFORE slicing: when the cap is already filled by
    # decided verdicts (≥500 yes/no on one anchor), remaining_slots is 0 and the
    # slice `undecided_values[-0:]` would return the WHOLE undecided list (Python
    # `[-0:]` == `[:]`), defeating the cap (120-VERIFICATION WARNING). With the
    # guard the result stays ≤ _MAX_TRIAGE_ENTRIES in every case.
    if remaining_slots > 0:
        undecided_values = list(undecided.items())
        # Keep the NEWEST undecided entries (tail of insertion order = most recent)
        for sys_id, verdict in undecided_values[-remaining_slots:]:
            result[sys_id] = verdict

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_joins_lab_state() -> Optional[dict]:
    """Return the stored ``joins_lab`` dict, or ``None`` on cold start.

    Returns ``None`` when:
    - the key is absent from storage,
    - the stored value is not a ``dict``,
    - the ``schema_version`` field does not equal :data:`_SCHEMA_VERSION`.

    Callers treat a ``None`` return as a cold start (no previously pinned
    anchor).
    """
    data: Any = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:
        return None  # stale or forged schema — discard, cold start
    return data


def write_full_state(
    anchor_sys_id: Optional[str] = None,
    anchor_fl_id: Optional[str] = None,
    anchor_volume_ie: Optional[str] = None,
    builder_rows: Optional[list] = None,
    builder_mode: str = 'exact',
    variants_on: bool = False,
    single_text: str = '',
    text_position: str = 'anywhere',
    flex_spacing: bool = False,
    bidirectional: bool = False,
    other_side_enabled: bool = False,
    other_side_rows: Optional[list] = None,
    other_side_combine: str = 'narrow',
    triage: Optional[dict] = None,
    active_filter: Optional[dict] = None,
    view_mode: str = 'grid',
) -> bool:
    """Persist the full Joins Lab working state to per-user storage.

    Builds the payload explicitly from named parameters so that no blob key
    (``full_text``, image bytes, candidate lists, etc.) can ever be copied
    in — even if a caller mistakenly tries to pass one.  Size caps are enforced
    at write time.

    :returns: ``True`` if the write succeeded, ``False`` on storage failure.

    Blob discipline: ONLY the named parameters are persisted.  Do NOT add
    ``full_text``, image data, result lists, or other blobs.  See the module
    docstring for the 778 MB ``search_history.json`` incident reference.
    """
    # Build the payload explicitly — whitelist ONLY known schema keys.
    # This is the primary blob-prevention mechanism (T-120-blob).
    payload: dict = {
        'schema_version': _SCHEMA_VERSION,  # MUST stay 1 (see module docstring)

        # Phase 117 anchor identity fields
        'anchor_sys_id': str(anchor_sys_id) if anchor_sys_id is not None else None,
        'anchor_fl_id': anchor_fl_id,
        'anchor_volume_ie': anchor_volume_ie,

        # Phase 120 additions — caps enforced below
        'builder_rows': _cap_rows(builder_rows),
        # builder_mode holds the UI SEARCH TYPE ('responsa'|'exact'|'variants'|
        # 'fuzzy'|'regex'), not the engine mode — see joins_lab._persist_state.
        'builder_mode': builder_mode if isinstance(builder_mode, str) else 'responsa',
        'variants_on': bool(variants_on),
        # single-line-mode query text (Exact/Variants/Fuzzy/Regex) — NOT in the
        # word-model lines_state, so it must be persisted separately or it is lost
        # on restore (round-5: "does not remember the search phrase").
        'single_text': (single_text[:_MAX_TERM_CHARS] if isinstance(single_text, str) else ''),
        'text_position': text_position if isinstance(text_position, str) else 'anywhere',
        'flex_spacing': bool(flex_spacing),
        'bidirectional': bool(bidirectional),
        'other_side_enabled': bool(other_side_enabled),
        'other_side_rows': _cap_rows(other_side_rows),
        'other_side_combine': other_side_combine if isinstance(other_side_combine, str) else 'narrow',
        'triage': _cap_triage(triage),
        'active_filter': active_filter if isinstance(active_filter, dict) else {},
        'view_mode': view_mode if isinstance(view_mode, str) else 'grid',
    }
    return safe_user_set(_JOINS_LAB_KEY, payload)


def read_full_state() -> Optional[dict]:
    """Return the stored ``joins_lab`` dict (including Phase-120 fields), or ``None``.

    Delegates to :func:`read_joins_lab_state` for the schema-version gate.
    Phase-120 keys absent in legacy v1 blobs can be read by callers with
    ``.get(key, <default>)`` — they will simply be missing from the dict, not
    ``None`` at the top level.

    Returns ``None`` on cold start, schema mismatch, or absent key.
    """
    return read_joins_lab_state()


def write_anchor(
    anchor_sys_id: Optional[str],
    anchor_fl_id: Optional[str] = None,
    anchor_volume_ie: Optional[str] = None,
) -> bool:
    """Persist the current anchor fragment identity to per-user storage.

    Backward-compatible convenience wrapper (Phase 117).  Writes ONLY the
    anchor identity fields; Phase-120 keys are absent in the blob (which is
    fine — :func:`read_full_state` callers use ``.get()`` to access them with
    defaults).

    :param anchor_sys_id: The manuscript system number (e.g. ``'990001234'``).
        Pass ``None`` to write an explicit "no anchor" record (prefer
        :func:`clear_joins_lab_state` instead if erasing is the intent).
    :param anchor_fl_id: Optional folio/leaf ID (e.g. ``'T-S 12.123.1r'``).
    :param anchor_volume_ie: Optional volume IE identifier for multi-volume MSS.
    :returns: ``True`` if the write succeeded, ``False`` on prune-race or other
        storage failure (mirroring :func:`web.safe_storage.safe_user_set`).

    Blob discipline: only identity fields are written.  Do NOT pass
    ``full_text``, image data, or search result lists.
    """
    payload: dict = {
        'schema_version': _SCHEMA_VERSION,
        'anchor_sys_id': str(anchor_sys_id) if anchor_sys_id is not None else None,
        'anchor_fl_id': anchor_fl_id,
        'anchor_volume_ie': anchor_volume_ie,
    }
    return safe_user_set(_JOINS_LAB_KEY, payload)


def read_anchor() -> Optional[dict]:
    """Convenience alias for the page restore path (D-13).

    Returns the validated storage dict (or ``None``) — identical to
    :func:`read_joins_lab_state`.  Phase 120 can call this to check for a
    previously pinned anchor without knowing the internal schema details.
    """
    return read_joins_lab_state()


def clear_joins_lab_state() -> None:
    """Remove the ``joins_lab`` key AND the ``puzzle_staging`` key from per-user storage.

    Used by Phase 120's "Clear / Reset" action (D-16).  Wipes the joins_lab blob,
    the puzzle_staging key, AND the results snapshot so a reset cannot leave any
    stale cross-session state.
    """
    safe_user_pop(_JOINS_LAB_KEY, None)
    safe_user_pop(_PUZZLE_STAGING_KEY, None)
    safe_user_pop(_RESULTS_KEY, None)


# ---------------------------------------------------------------------------
# Results snapshot helpers (per-TAB transient cache — see constants above)
# ---------------------------------------------------------------------------

def _compact_candidate(c: Any) -> dict:
    """Serialise a ``Candidate`` dataclass (or dict) to a light JSON-able dict.

    Truncates the heavy ``full_text`` transcription field to
    :data:`_SNAPSHOT_FULLTEXT_CAP` chars — enough to keep the card snippet but
    not the whole page (blob discipline, even though tab storage is transient).
    """
    if dataclasses.is_dataclass(c) and not isinstance(c, type):
        d = dataclasses.asdict(c)
    elif isinstance(c, dict):
        d = dict(c)
    else:
        return {}
    ft = d.get('full_text') or ''
    if ft:
        d['full_text'] = str(ft)[:_SNAPSHOT_FULLTEXT_CAP]
    return d


def persist_results_snapshot(
    *,
    anchor_sys_id: Optional[str],
    raw_text_candidates: Any,
    vs_candidates: Any,
    vs_on: bool,
    vs_anchor_sid: Optional[str],
    enrichment: Any,
) -> bool:
    """Persist the current candidate result set under the per-user results key.

    Strictly bounded (see module constants): full_text truncated, candidate
    lists capped.  ``raw_text_candidates`` is the RAW text+cross-side baseline
    (NOT the merged display set) so the VS toggle keeps working after restore.

    :returns: ``True`` on success, ``False`` on storage failure (prune race).
    """
    payload: dict = {
        'version': _RESULTS_SNAPSHOT_VERSION,
        'anchor_sys_id': str(anchor_sys_id or ''),
        'raw_text_candidates': [
            _compact_candidate(c)
            for c in list(raw_text_candidates or [])[:_MAX_SNAPSHOT_CANDIDATES]
        ],
        'vs_candidates': [
            _compact_candidate(c)
            for c in list(vs_candidates or [])[:_MAX_SNAPSHOT_CANDIDATES]
        ],
        'vs_on': bool(vs_on),
        'vs_anchor_sid': vs_anchor_sid,
        'enrichment': enrichment if isinstance(enrichment, dict) else {},
    }
    return safe_user_set(_RESULTS_KEY, payload)


def read_results_snapshot() -> Optional[dict]:
    """Return the per-user candidate snapshot, or ``None`` when absent/stale.

    Returns ``None`` when the key is absent, the value is not a dict, or its
    ``version`` does not match the current snapshot version.
    """
    data: Any = safe_user_get(_RESULTS_KEY, default=None)
    if not isinstance(data, dict) or data.get('version') != _RESULTS_SNAPSHOT_VERSION:
        return None
    return data


def clear_results_snapshot() -> None:
    """Drop the per-user candidate snapshot (e.g. on New Search)."""
    safe_user_pop(_RESULTS_KEY, None)
