# -*- coding: utf-8 -*-
"""
The search selection helpers (web/pages/search_helpers.py): a result is selected by its
identity (`selection_key`: its uid, else its manuscript), and the selection is read against
the rows the page shows (`displayed_results`), never by position in the full list -- the
cards number the rows the filters leave, so a position there named another result whenever
a filter hid an earlier row (Codex on #26).

compute_selected_uids maps that selection into the uid list the export handlers consume.
"""
from web.pages.search_helpers import compute_selected_uids, selected_rows, selection_key


class StubSearchState:
    """Tiny stand-in for SearchUIState. Only attributes the helpers read."""
    def __init__(self, displayed, selected_keys, results=None):
        self.displayed_results = displayed
        self.selected_keys = selected_keys
        self.results = results if results is not None else displayed


def test_empty_selection_returns_none():
    ss = StubSearchState([{'uid': 'u0'}, {'uid': 'u1'}], set())
    assert compute_selected_uids(ss) is None
    assert selected_rows(ss) == []


def test_the_selection_is_read_against_the_shown_rows_in_their_order():
    # u0 is hidden by a filter: the first shown row is u1, and it is the one selected.
    full = [{'uid': 'u0'}, {'uid': 'u1'}, {'uid': 'u2'}, {'uid': 'u3'}]
    ss = StubSearchState(displayed=full[1:], selected_keys={'u3', 'u1'}, results=full)
    assert compute_selected_uids(ss) == ['u1', 'u3']
    assert selected_rows(ss) == [full[1], full[3]]


def test_a_selected_row_a_filter_hides_is_left_out_until_it_shows_again():
    full = [{'uid': 'u0'}, {'uid': 'u1'}]
    ss = StubSearchState(displayed=[full[1]], selected_keys={'u0'}, results=full)
    assert compute_selected_uids(ss) is None
    ss.displayed_results = full
    assert compute_selected_uids(ss) == ['u0']


def test_a_row_without_a_uid_is_selected_by_its_manuscript():
    row = {'display': {'id': '990000000000000001'}}
    assert selection_key(row) == '990000000000000001'
    ss = StubSearchState([{'uid': 'u0'}, row], {selection_key(row)})
    # Empty-uid items (metadata-only hits per D-04) stay '' in the export's list.
    assert compute_selected_uids(ss) == ['']


def test_rows_with_no_identity_are_told_apart():
    a, b = {'display': {}}, {'display': {}}
    assert selection_key(a) != selection_key(b)
