# -*- coding: utf-8 -*-
"""
Phase 77 Plan 06 -- unit tests for compute_selected_uids helper.

The helper maps page-scoped checkbox selection (search_state.selected_indices)
into the uid list the export handlers consume (state.last_selected_uids).

Tests:
  1. empty selected_indices -> None (preserves "export full set")
  2. non-empty selected_indices -> uid list in sorted-index order
  3. out-of-bounds indices silently skipped (defensive)
  4. results with missing uid field -> empty string preserved in list
"""
from web.pages.search_helpers import compute_selected_uids


class StubSearchState:
    """Tiny stand-in for SearchUIState. Only attributes the helper reads."""
    def __init__(self, results, selected_indices):
        self.results = results
        self.selected_indices = selected_indices


def test_empty_selection_returns_none():
    ss = StubSearchState(
        results=[{'uid': 'u0'}, {'uid': 'u1'}, {'uid': 'u2'}],
        selected_indices=set(),
    )
    assert compute_selected_uids(ss) is None


def test_non_empty_selection_returns_uids_in_sorted_order():
    ss = StubSearchState(
        results=[
            {'uid': 'u0'},
            {'uid': 'u1'},
            {'uid': 'u2'},
            {'uid': 'u3'},
            {'uid': 'u4'},
        ],
        selected_indices={2, 0, 1},  # deliberately unsorted
    )
    # sorted index order -> u0, u1, u2 (NOT insertion order)
    assert compute_selected_uids(ss) == ['u0', 'u1', 'u2']


def test_out_of_bounds_index_skipped():
    ss = StubSearchState(
        results=[{'uid': 'u0'}, {'uid': 'u1'}],
        selected_indices={0, 99},  # 99 is out of bounds
    )
    # 99 silently dropped, no exception raised
    assert compute_selected_uids(ss) == ['u0']


def test_missing_uid_field_returns_empty_string():
    ss = StubSearchState(
        results=[
            {'uid': 'u0'},
            {'no_uid_key': 'metadata-only hit per D-04'},
            {'uid': 'u2'},
        ],
        selected_indices={0, 1, 2},
    )
    # Empty-uid item preserved as '' (will fail to match in export handler;
    # documented behavior per Plan 06 Task 1 behavior section).
    assert compute_selected_uids(ss) == ['u0', '', 'u2']
