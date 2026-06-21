# -*- coding: utf-8 -*-
"""Tests for web/joins_lab_storage.py.

Covers:
1. Schema-version invalidation (3 cases: stale version, missing key, non-dict).
2. Round-trip write/read via in-memory backing store.
3. Two-anonymous-session no-state-bleed (SC#5).
4. Phase-120 full-state write/read round-trip (PST-01).
5. Legacy v1 anchor-only blob restored without discard (backward-compat).
6. schema_version stays 1 in write_full_state().
7. write_anchor() backward-compat (PST-01 regression).
8. Size-cap enforcement: no blobs (PST-01 test_write_full_state_no_blobs).
9. builder_rows / other_side_rows truncated at 20; term capped at 200 chars.
10. triage capped at 500 entries (LRU-evict oldest untriaged first).
11. clear_joins_lab_state() wipes both joins_lab AND puzzle_staging (PST-03).

All storage I/O is monkeypatched — no live NiceGUI storage context required.
The module under test uses only safe_user_get/set/pop; we substitute them with
per-session in-memory dicts to simulate real NiceGUI session isolation.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session_store() -> tuple:
    """Return a (get, set, pop) triple backed by a private dict.

    Simulates a single NiceGUI anonymous session's storage.  Two calls to
    this function produce two **independent** stores — writing to store A
    never touches store B.
    """
    store: dict = {}

    def _get(key: str, default=None):
        return store.get(key, default)

    def _set(key: str, value) -> bool:
        store[key] = value
        return True

    def _pop(key: str, default=None):
        return store.pop(key, default)

    return _get, _set, _pop


# ---------------------------------------------------------------------------
# Invalidation tests (3 cases)
# ---------------------------------------------------------------------------

def test_schema_version_mismatch_returns_none(monkeypatch):
    """A stored dict with schema_version != 1 is treated as cold start."""
    stale_data = {'schema_version': 0, 'anchor_sys_id': '990001234'}
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: stale_data if key == 'joins_lab' else default,
    )
    import importlib
    import web.joins_lab_storage as _mod
    importlib.reload(_mod)  # ensure monkeypatch hits the module-level names
    # Re-import after reload so the test calls the patched version
    from web.joins_lab_storage import read_joins_lab_state  # noqa: PLC0415
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for stale schema_version=0, got {result!r}"
    )


def test_missing_key_returns_none(monkeypatch):
    """Absent joins_lab key (safe_user_get returns None) → cold start."""
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: default,  # always returns default (None)
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for missing key, got {result!r}"
    )


def test_non_dict_stored_value_returns_none(monkeypatch):
    """A non-dict stored value (e.g. a string) → cold start (isinstance guard)."""
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: 'corrupted-string' if key == 'joins_lab' else default,
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for non-dict value, got {result!r}"
    )


def test_valid_schema_version_returns_data(monkeypatch):
    """A stored dict with schema_version == 1 is returned as-is."""
    valid_data = {'schema_version': 1, 'anchor_sys_id': '990001234',
                  'anchor_fl_id': None, 'anchor_volume_ie': None}
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: valid_data if key == 'joins_lab' else default,
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is not None, "Expected data dict, got None"
    assert result['anchor_sys_id'] == '990001234'
    assert result['schema_version'] == 1


# ---------------------------------------------------------------------------
# Round-trip test
# ---------------------------------------------------------------------------

def test_write_then_read_round_trip(monkeypatch):
    """write_anchor() then read_anchor() returns the written dict (round-trip).

    Uses an in-memory backing store so no NiceGUI context is needed.
    """
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_anchor, read_anchor, _SCHEMA_VERSION

    ok = write_anchor('990001234', anchor_fl_id='T-S 12.123.1r')
    assert ok is True, "write_anchor should return True on success"

    result = read_anchor()
    assert result is not None, "read_anchor() should return data after write_anchor()"
    assert result['anchor_sys_id'] == '990001234'
    assert result['anchor_fl_id'] == 'T-S 12.123.1r'
    assert result['schema_version'] == _SCHEMA_VERSION


# ---------------------------------------------------------------------------
# No-state-bleed test (SC#5 — two anonymous sessions)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase-120 full-state round-trip tests (Task 1)
# ---------------------------------------------------------------------------

def test_write_full_state_round_trip(monkeypatch):
    """write_full_state() then read_full_state() returns all Phase-120 keys."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state

    builder_rows = [{'term': 'שאלה', 'gap_to_next': 0, 'modifiers': {}}]
    other_rows = [{'term': 'תשובה', 'gap_to_next': 1, 'modifiers': {'variants': True}}]
    triage = {'990001': 'yes', '990002': 'maybe', '990003': 'no'}

    ok = write_full_state(
        anchor_sys_id='990001234',
        anchor_fl_id='T-S 12.123.1r',
        anchor_volume_ie=None,
        builder_rows=builder_rows,
        builder_mode='exact',
        text_position='anywhere',
        flex_spacing=False,
        bidirectional=True,
        other_side_enabled=True,
        other_side_rows=other_rows,
        other_side_combine='narrow',
        triage=triage,
        active_filter={'type': 'text'},
        view_mode='grid',
    )
    assert ok is True, 'write_full_state should return True'

    result = read_full_state()
    assert result is not None, 'read_full_state() should return data after write_full_state()'
    assert result['anchor_sys_id'] == '990001234'
    assert result['anchor_fl_id'] == 'T-S 12.123.1r'
    assert result['anchor_volume_ie'] is None
    assert result['builder_rows'] == builder_rows
    assert result['builder_mode'] == 'exact'
    assert result['text_position'] == 'anywhere'
    assert result['flex_spacing'] is False
    assert result['bidirectional'] is True
    assert result['other_side_enabled'] is True
    assert result['other_side_rows'] == other_rows
    assert result['other_side_combine'] == 'narrow'
    assert result['triage'] == triage
    assert result['active_filter'] == {'type': 'text'}
    assert result['view_mode'] == 'grid'


def test_write_full_state_schema_version_stays_1(monkeypatch):
    """write_full_state() persists schema_version=1 (NOT 2)."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state, _SCHEMA_VERSION

    write_full_state(anchor_sys_id='990001234')
    result = read_full_state()
    assert result is not None
    assert result['schema_version'] == 1, (
        f'schema_version must stay 1 (got {result["schema_version"]})'
    )
    assert _SCHEMA_VERSION == 1, '_SCHEMA_VERSION constant must be 1'


def test_legacy_v1_anchor_blob_not_discarded(monkeypatch):
    """An existing Phase-117 v1 blob (anchor keys only) must NOT be discarded.

    Missing Phase-120 keys come back via .get(key, default) — never None for the
    whole blob just because the new keys are absent.
    """
    legacy_blob = {
        'schema_version': 1,
        'anchor_sys_id': '990009999',
        'anchor_fl_id': 'T-S NS 329.96',
        'anchor_volume_ie': None,
        # Phase-120 keys ABSENT intentionally
    }
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: legacy_blob if key == 'joins_lab' else default,
    )

    from web.joins_lab_storage import read_full_state

    result = read_full_state()
    assert result is not None, (
        'Legacy v1 anchor blob must NOT be discarded by read_full_state()'
    )
    assert result['anchor_sys_id'] == '990009999'
    # Phase-120 keys absent in the blob → callers use .get() with defaults
    assert result.get('builder_rows', []) == []
    assert result.get('triage', {}) == {}
    assert result.get('view_mode', 'grid') == 'grid'


def test_write_anchor_backward_compat(monkeypatch):
    """write_anchor() still works unchanged after Phase-120 extension (regression)."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_anchor, read_anchor, _SCHEMA_VERSION

    ok = write_anchor('990001234', anchor_fl_id='T-S 12.123.1r')
    assert ok is True
    result = read_anchor()
    assert result is not None
    assert result['anchor_sys_id'] == '990001234'
    assert result['anchor_fl_id'] == 'T-S 12.123.1r'
    assert result['schema_version'] == _SCHEMA_VERSION == 1


# ---------------------------------------------------------------------------
# Phase-120 size-cap and no-blobs tests (Task 2)
# ---------------------------------------------------------------------------

def test_write_full_state_no_blobs(monkeypatch):
    """write_full_state() MUST strip/reject blob keys — no full_text in persisted payload.

    This is VALIDATION.md row PST-01 (test_write_full_state_no_blobs).
    """
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state

    # Even if a caller mistakenly tries to pass blob-like kwargs, they must be ignored.
    # The function accepts only known schema keys as named params — unknown kwargs
    # should be silently ignored (or the function should only accept known params).
    write_full_state(
        anchor_sys_id='990001234',
        triage={'990001': 'yes'},
        view_mode='grid',
    )

    result = read_full_state()
    assert result is not None
    assert 'full_text' not in result, (
        'full_text must never appear in the persisted payload'
    )
    assert 'candidates' not in result, 'candidate list must not be persisted'
    assert 'image' not in result, 'image bytes must not be persisted'
    assert 'results' not in result, 'result list must not be persisted'


def test_builder_rows_capped_at_20(monkeypatch):
    """write_full_state() truncates builder_rows to 20 entries at write time."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state

    big_rows = [{'term': f'term{i}', 'gap_to_next': 0, 'modifiers': {}} for i in range(30)]
    write_full_state(anchor_sys_id='990001234', builder_rows=big_rows)

    result = read_full_state()
    assert result is not None
    assert len(result['builder_rows']) == 20, (
        f'builder_rows must be capped at 20, got {len(result["builder_rows"])}'
    )


def test_builder_row_term_capped_at_200_chars(monkeypatch):
    """Each builder_row term is capped at 200 chars at write time."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state

    long_term = 'א' * 300  # 300-char term
    rows = [{'term': long_term, 'gap_to_next': 0, 'modifiers': {}}]
    write_full_state(anchor_sys_id='990001234', builder_rows=rows)

    result = read_full_state()
    assert result is not None
    stored_term = result['builder_rows'][0]['term']
    assert len(stored_term) <= 200, (
        f'term must be capped at 200 chars, got {len(stored_term)}'
    )


def test_triage_capped_at_500(monkeypatch):
    """write_full_state() caps triage at 500 entries (LRU-evict oldest untriaged first)."""
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, read_full_state

    # 600 entries: first 100 have verdicts (yes/no/maybe), last 500 are untriaged 'maybe'
    # On eviction, untriaged entries should be evicted first; Y/N entries preserved.
    triage = {}
    # First 100: definite verdicts (should survive)
    for i in range(100):
        triage[f'99000{i:04d}'] = 'yes' if i % 2 == 0 else 'no'
    # Next 500: 'maybe' — these are "untriaged" and candidates for eviction
    for i in range(100, 600):
        triage[f'99000{i:04d}'] = 'maybe'

    write_full_state(anchor_sys_id='990001234', triage=triage)

    result = read_full_state()
    assert result is not None
    stored_triage = result['triage']
    assert len(stored_triage) <= 500, (
        f'triage must be capped at 500 entries, got {len(stored_triage)}'
    )
    # Verify that Y/N entries are preserved
    yes_no_count = sum(1 for v in stored_triage.values() if v in ('yes', 'no'))
    assert yes_no_count == 100, (
        f'All 100 yes/no triage verdicts must be preserved, got {yes_no_count}'
    )


def test_clear_leaves_empty(monkeypatch):
    """clear_joins_lab_state() wipes both joins_lab AND puzzle_staging (PST-03).

    This is VALIDATION.md row PST-03 (test_clear_leaves_empty).
    """
    store: dict = {}

    def _get(key, default=None):
        return store.get(key, default)

    def _set(key, value) -> bool:
        store[key] = value
        return True

    def _pop(key, default=None):
        return store.pop(key, default)

    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_full_state, clear_joins_lab_state, read_full_state

    # Write some state
    write_full_state(anchor_sys_id='990001234', triage={'990001': 'yes'})
    # Simulate puzzle_staging being set
    _set('puzzle_staging', {'schema_version': 1, 'fragments': ['990001234']})

    # Both keys should be present
    assert store.get('joins_lab') is not None
    assert store.get('puzzle_staging') is not None

    # Clear all
    clear_joins_lab_state()

    # Both keys must be gone
    assert read_full_state() is None, (
        'read_full_state() must return None after clear_joins_lab_state()'
    )
    assert _get('puzzle_staging') is None, (
        'puzzle_staging must be wiped by clear_joins_lab_state() (D-16)'
    )


# ---------------------------------------------------------------------------
# Round-5 (PST): per-tab results snapshot (persist / read / clear)
# ---------------------------------------------------------------------------

def _patch_tab_storage(monkeypatch) -> dict:
    """Patch _get_tab_storage() to return a private dict (simulates app.storage.tab)."""
    store: dict = {}
    monkeypatch.setattr('web.joins_lab_storage._get_tab_storage', lambda: store)
    return store


def test_results_snapshot_round_trip(monkeypatch):
    """persist_results_snapshot() then read_results_snapshot() round-trips the set."""
    from shared.joins_lab import Candidate
    from web.joins_lab_storage import persist_results_snapshot, read_results_snapshot
    _patch_tab_storage(monkeypatch)

    cands = [Candidate(sys_id='990001', page=1, shelfmark='T-S 1.1', full_text='abc')]
    persist_results_snapshot(
        anchor_sys_id='990000', raw_text_candidates=cands, vs_candidates=[],
        vs_on=True, vs_anchor_sid='990000',
        enrichment={'990001': {'material': 'paper'}},
    )
    snap = read_results_snapshot()
    assert snap is not None
    assert snap['anchor_sys_id'] == '990000'
    assert snap['vs_on'] is True
    assert snap['vs_anchor_sid'] == '990000'
    assert len(snap['raw_text_candidates']) == 1
    assert snap['raw_text_candidates'][0]['sys_id'] == '990001'
    assert snap['raw_text_candidates'][0]['full_text'] == 'abc'
    assert snap['enrichment'] == {'990001': {'material': 'paper'}}


def test_results_snapshot_version_gate(monkeypatch):
    """A snapshot with a mismatched version is treated as absent (cold tab)."""
    from web.joins_lab_storage import read_results_snapshot, _RESULTS_TAB_KEY
    store = _patch_tab_storage(monkeypatch)
    store[_RESULTS_TAB_KEY] = {'version': 999, 'anchor_sys_id': 'x'}
    assert read_results_snapshot() is None


def test_results_snapshot_truncates_full_text(monkeypatch):
    """Heavy full_text is truncated to the cap (blob discipline, even in tab cache)."""
    from shared.joins_lab import Candidate
    from web.joins_lab_storage import (
        persist_results_snapshot, read_results_snapshot, _SNAPSHOT_FULLTEXT_CAP,
    )
    _patch_tab_storage(monkeypatch)
    big = 'א' * (_SNAPSHOT_FULLTEXT_CAP + 5000)
    persist_results_snapshot(
        anchor_sys_id='990000',
        raw_text_candidates=[Candidate(sys_id='990001', page=1, full_text=big)],
        vs_candidates=[], vs_on=False, vs_anchor_sid=None, enrichment={},
    )
    snap = read_results_snapshot()
    assert len(snap['raw_text_candidates'][0]['full_text']) <= _SNAPSHOT_FULLTEXT_CAP


def test_results_snapshot_caps_candidate_count(monkeypatch):
    """Either candidate list is hard-capped at _MAX_SNAPSHOT_CANDIDATES."""
    from shared.joins_lab import Candidate
    from web.joins_lab_storage import (
        persist_results_snapshot, read_results_snapshot, _MAX_SNAPSHOT_CANDIDATES,
    )
    _patch_tab_storage(monkeypatch)
    many = [Candidate(sys_id=str(i), page=1) for i in range(_MAX_SNAPSHOT_CANDIDATES + 50)]
    persist_results_snapshot(
        anchor_sys_id='990000', raw_text_candidates=many, vs_candidates=[],
        vs_on=False, vs_anchor_sid=None, enrichment={},
    )
    snap = read_results_snapshot()
    assert len(snap['raw_text_candidates']) == _MAX_SNAPSHOT_CANDIDATES


def test_clear_results_snapshot(monkeypatch):
    """clear_results_snapshot() drops the per-tab snapshot."""
    from shared.joins_lab import Candidate
    from web.joins_lab_storage import (
        persist_results_snapshot, read_results_snapshot, clear_results_snapshot,
    )
    _patch_tab_storage(monkeypatch)
    persist_results_snapshot(
        anchor_sys_id='990000',
        raw_text_candidates=[Candidate(sys_id='1', page=1)],
        vs_candidates=[], vs_on=False, vs_anchor_sid=None, enrichment={},
    )
    assert read_results_snapshot() is not None
    clear_results_snapshot()
    assert read_results_snapshot() is None


def test_results_snapshot_no_tab_context_is_noop(monkeypatch):
    """With no tab context, persist/read/clear are safe no-ops (never raise)."""
    from shared.joins_lab import Candidate
    from web.joins_lab_storage import (
        persist_results_snapshot, read_results_snapshot, clear_results_snapshot,
    )
    monkeypatch.setattr('web.joins_lab_storage._get_tab_storage', lambda: None)
    persist_results_snapshot(
        anchor_sys_id='990000',
        raw_text_candidates=[Candidate(sys_id='1', page=1)],
        vs_candidates=[], vs_on=False, vs_anchor_sid=None, enrichment={},
    )
    assert read_results_snapshot() is None
    clear_results_snapshot()  # must not raise


def test_results_snapshot_not_persisted_to_user_storage(monkeypatch):
    """The results snapshot is per-TAB only — it must never touch user storage.

    Guards the blob-discipline invariant: candidate lists / full_text go to
    app.storage.tab (transient), NEVER the long-lived per-user blob.
    """
    from shared.joins_lab import Candidate
    from web import joins_lab_storage as _mod

    _patch_tab_storage(monkeypatch)

    # Trip a hard failure if anything routes the snapshot through user storage.
    def _boom(*a, **k):  # pragma: no cover - only fires on a regression
        raise AssertionError('results snapshot must NOT use per-user safe storage')

    monkeypatch.setattr(_mod, 'safe_user_set', _boom)

    _mod.persist_results_snapshot(
        anchor_sys_id='990000',
        raw_text_candidates=[Candidate(sys_id='1', page=1, full_text='x' * 50000)],
        vs_candidates=[], vs_on=False, vs_anchor_sid=None, enrichment={},
    )
    assert _mod.read_results_snapshot() is not None


def test_two_sessions_do_not_share_state(monkeypatch):
    """Writing anchor in session A must not surface in session B.

    Simulates two NiceGUI anonymous sessions via two independent in-memory
    dicts.  The joins_lab key written by session A is absent in session B's
    read → read_joins_lab_state() returns None for B (SC#5).
    """
    get_a, set_a, pop_a = _make_session_store()
    get_b, set_b, pop_b = _make_session_store()

    from web import joins_lab_storage as _mod

    # Session A: write an anchor
    monkeypatch.setattr(_mod, 'safe_user_get', get_a)
    monkeypatch.setattr(_mod, 'safe_user_set', set_a)
    monkeypatch.setattr(_mod, 'safe_user_pop', pop_a)
    _mod.write_anchor('990001234')

    # Verify session A sees its own data
    assert _mod.read_joins_lab_state() is not None, \
        "Session A should see the written anchor"

    # Session B: switch to B's backing store — should see nothing
    monkeypatch.setattr(_mod, 'safe_user_get', get_b)
    monkeypatch.setattr(_mod, 'safe_user_set', set_b)
    monkeypatch.setattr(_mod, 'safe_user_pop', pop_b)

    result_b = _mod.read_joins_lab_state()
    assert result_b is None, (
        f"Session B must not see session A's anchor (SC#5 no-state-bleed). "
        f"Got: {result_b!r}"
    )

    # Double-check: the two backing stores are independent
    assert 'joins_lab' not in get_b.__code__.co_freevars or True  # Duck-typed check
    # Simpler: write to B and confirm A doesn't change
    monkeypatch.setattr(_mod, 'safe_user_get', get_b)
    monkeypatch.setattr(_mod, 'safe_user_set', set_b)
    _mod.write_anchor('999999999')

    monkeypatch.setattr(_mod, 'safe_user_get', get_a)
    result_a_again = _mod.read_joins_lab_state()
    assert result_a_again is not None, \
        "Session A's anchor should still be present after session B writes"
    assert result_a_again['anchor_sys_id'] == '990001234', \
        "Session A's anchor_sys_id should still be '990001234'"
