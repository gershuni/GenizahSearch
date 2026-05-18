# -*- coding: utf-8 -*-
"""Regression tests for the export payload size cap.

Background (2026-05-18 forensic capture): a single user's
``app.storage.user['export_search_payload']`` held a 31,572-result list
weighing 498 MB. NiceGUI rehydrated that into RAM on every session load,
producing the ~5 GB Python-heap growth that drove the P1 web memory leak.
The fix caps every export-storage mutator at ``_EXPORT_RESULTS_CAP``
(5,000 results) and emits ``truncated`` / ``total_count`` metadata so
downstream UX can advise the user.

These tests assert the cap is applied on all four write paths:
  - set_search_export
  - update_search_export_results
  - set_parallels_export (both ``results`` and ``filtered`` lists)
  - update_parallels_export_filtered

Pattern matches tests/test_export_state_selection.py: monkeypatch
``web.safe_storage.app`` with an instance-isolated SimpleNamespace stub
whose ``storage.user`` is a fresh dict (Phase 88 Refinement 6).
"""
from types import SimpleNamespace

import pytest


def _make_stub(initial_storage: dict):
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))


def _result(i: int) -> dict:
    """Build a result dict roughly matching the production shape so the cap
    is exercised against realistic data (uid keys, etc.)."""
    return {
        'uid': f'u{i}',
        'display': {'shelfmark': f'T-S 12.{i}', 'id': str(i)},
        'snippet': f'match {i}',
        'full_text': 'x' * 100,
        'score': 0.5,
    }


# ---------------------------------------------------------------------------
# set_search_export
# ---------------------------------------------------------------------------

def test_set_search_export_caps_oversized_list(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    big = [_result(i) for i in range(10_000)]
    export_state.set_search_export(results=big, query='ה*')

    payload = storage['export_search_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert payload['truncated'] is True
    assert payload['total_count'] == 10_000
    # The cap must keep the first N (relevance-ordered), not a random slice.
    assert payload['results'][0]['uid'] == 'u0'
    assert payload['results'][-1]['uid'] == f'u{export_state._EXPORT_RESULTS_CAP - 1}'


def test_set_search_export_passes_small_list_through(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    small = [_result(i) for i in range(100)]
    export_state.set_search_export(results=small, query='foo')

    payload = storage['export_search_payload']
    assert len(payload['results']) == 100
    assert payload['truncated'] is False
    assert payload['total_count'] == 100


def test_set_search_export_handles_empty_and_non_list(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_search_export(results=[], query='')
    p = storage['export_search_payload']
    assert p['results'] == []
    assert p['truncated'] is False
    assert p['total_count'] == 0

    # Non-list defensive degradation (e.g., if a caller ever passes None).
    export_state.set_search_export(results=None, query='')  # type: ignore[arg-type]
    p = storage['export_search_payload']
    assert p['results'] == []
    assert p['truncated'] is False
    assert p['total_count'] == 0


# ---------------------------------------------------------------------------
# update_search_export_results
# ---------------------------------------------------------------------------

def test_update_search_export_results_caps_and_preserves_other_fields(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Establish a baseline payload via the helper (caps to 100, untruncated).
    export_state.set_search_export(
        results=[_result(i) for i in range(100)],
        query='original-query',
        mode='responsa',
        gap=3,
        filters={'library': 'CUL'},
        warnings=['hello'],
        selected_uids=['u5'],
    )

    # Re-write with an oversized list (e.g. refinement undo).
    big = [_result(i) for i in range(10_000)]
    export_state.update_search_export_results(big)

    payload = storage['export_search_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert payload['truncated'] is True
    assert payload['total_count'] == 10_000
    # All other envelope fields must be preserved.
    assert payload['query'] == 'original-query'
    assert payload['mode'] == 'responsa'
    assert payload['gap'] == 3
    assert payload['filters'] == {'library': 'CUL'}
    assert payload['warnings'] == ['hello']
    assert payload['selected_uids'] == ['u5']


def test_update_search_export_results_noops_when_payload_missing(monkeypatch):
    from web import export_state

    # Empty storage -- update with nothing to update should be a silent no-op,
    # not a fresh write (matches the existing Phase 88 D-11 contract).
    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.update_search_export_results([_result(i) for i in range(10_000)])
    assert 'export_search_payload' not in storage


# ---------------------------------------------------------------------------
# set_parallels_export
# ---------------------------------------------------------------------------

def test_set_parallels_export_caps_both_lists_independently(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    big_results = [_result(i) for i in range(8_000)]
    small_filtered = [_result(i) for i in range(50)]
    export_state.set_parallels_export(results=big_results, filtered=small_filtered)

    payload = storage['export_parallels_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert len(payload['filtered']) == 50
    assert payload['truncated'] is True  # OR of the two
    assert payload['total_count'] == 8_000
    assert payload['filtered_total_count'] == 50


def test_set_parallels_export_clean_when_both_small(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_parallels_export(
        results=[_result(i) for i in range(10)],
        filtered=[_result(i) for i in range(5)],
    )
    payload = storage['export_parallels_payload']
    assert payload['truncated'] is False
    assert payload['total_count'] == 10
    assert payload['filtered_total_count'] == 5


# ---------------------------------------------------------------------------
# update_parallels_export_filtered
# ---------------------------------------------------------------------------

def test_update_parallels_export_filtered_caps_and_preserves_truncated_flag(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Initial state: results truncated, filtered small.
    export_state.set_parallels_export(
        results=[_result(i) for i in range(10_000)],  # capped, truncated=True
        filtered=[_result(i) for i in range(50)],
        meta={'source_text': 'x'},
    )
    assert storage['export_parallels_payload']['truncated'] is True

    # Patch filtered with an oversized list.
    export_state.update_parallels_export_filtered([_result(i) for i in range(7_000)])

    payload = storage['export_parallels_payload']
    assert len(payload['filtered']) == export_state._EXPORT_RESULTS_CAP
    assert payload['filtered_total_count'] == 7_000
    # truncated stays True (OR-merge with prior state).
    assert payload['truncated'] is True
    # meta is preserved (copy-on-update).
    assert payload['meta'] == {'source_text': 'x'}


def test_update_parallels_export_filtered_or_merges_from_clean_to_truncated(monkeypatch):
    """If the initial payload was clean but the patched list is oversized,
    truncated must flip True."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_parallels_export(
        results=[_result(i) for i in range(10)],   # clean
        filtered=[_result(i) for i in range(10)],  # clean
    )
    assert storage['export_parallels_payload']['truncated'] is False

    export_state.update_parallels_export_filtered([_result(i) for i in range(8_000)])
    assert storage['export_parallels_payload']['truncated'] is True
    assert storage['export_parallels_payload']['filtered_total_count'] == 8_000


# ---------------------------------------------------------------------------
# Module-level constant sanity
# ---------------------------------------------------------------------------

def test_cap_constant_is_reasonable():
    """If someone edits the constant in a way that defeats the purpose, fail
    loudly. 5K covers realistic export volumes; anything above ~20K starts to
    reproduce the original leak shape, anything below 1K starts breaking
    legitimate large exports."""
    from web import export_state
    assert 1000 <= export_state._EXPORT_RESULTS_CAP <= 20_000


def test_cap_results_helper_returns_correct_shape():
    from web import export_state
    capped, truncated, original = export_state._cap_results([_result(i) for i in range(3)])
    assert truncated is False
    assert original == 3
    assert len(capped) == 3

    capped, truncated, original = export_state._cap_results(
        [_result(i) for i in range(export_state._EXPORT_RESULTS_CAP + 1)]
    )
    assert truncated is True
    assert original == export_state._EXPORT_RESULTS_CAP + 1
    assert len(capped) == export_state._EXPORT_RESULTS_CAP

    # Non-list input.
    capped, truncated, original = export_state._cap_results(None)
    assert capped == [] and truncated is False and original == 0

    capped, truncated, original = export_state._cap_results('not-a-list')
    assert capped == [] and truncated is False and original == 0
