# -*- coding: utf-8 -*-
"""The composition result viewer: manuscript-level navigation, and the pane
that showed a letter-level match with nothing marked in it.

Qt-FREE, in the pattern of tests/test_desktop_passage_gate.py. Every method
under test reads `all_results`, `meta_mgr` and two button-shaped objects, so
each is exercised by binding the UNBOUND method to a stub -- no
QApplication, no event loop, and none of the segfault risk that would put
the file in the gui lane.
"""
from __future__ import annotations

import ast
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.result_dialog as result_dialog          # noqa: E402
from shared.metadata_manager import MetadataManager    # noqa: E402

RD = result_dialog.ResultDialog
RD_PY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'desktop', 'result_dialog.py')


def _method_source(name):
    """The source of ONE method of ResultDialog, by name."""
    tree = ast.parse(io.open(RD_PY, encoding='utf-8').read())
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == 'ResultDialog')
    fn = next(n for n in cls.body
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == name)
    lines = io.open(RD_PY, encoding='utf-8').read().splitlines()
    return '\n'.join(lines[fn.lineno - 1:fn.end_lineno])


# ---------------------------------------------------------------------------
# Stubs.
# ---------------------------------------------------------------------------

class _Button:
    def __init__(self):
        self.visible = True
        self.enabled = True

    def setVisible(self, b):
        self.visible = bool(b)

    def setEnabled(self, b):
        self.enabled = bool(b)


class _Meta:
    """The REAL header parser. It touches no instance state, and a
    hand-rolled stand-in would only prove the test agrees with itself."""
    parse_full_id_components = MetadataManager.parse_full_id_components


class _RD:
    _result_manuscript_key = RD._result_manuscript_key
    _manuscript_keys = RD._manuscript_keys
    _next_manuscript_index = RD._next_manuscript_index
    navigate_manuscript_results = RD.navigate_manuscript_results
    _update_manuscript_nav = RD._update_manuscript_nav
    _source_pane = RD._source_pane
    _apply_source_highlights = RD._apply_source_highlights

    def __init__(self, results, idx=0):
        self.all_results = results
        self.current_result_idx = idx
        self.meta_mgr = _Meta()
        self.btn_res_prev_ms = _Button()
        self.btn_res_next_ms = _Button()
        self.loaded = []

    def load_result_by_index(self, i):
        self.current_result_idx = i
        self.loaded.append(i)


def _hdr(ms, page):
    """A realistic corpus header -- the shape sys_id extraction expects."""
    return f'99{ms:08d}_IE{10_000_000 + ms}_P{page:07d}_FL{page}'


def _row(ms, page):
    return {'raw_header': _hdr(ms, page), 'display': {'id': f'99{ms:08d}'}}


# A composition-shaped list: three pages of manuscript 1, two of 2, one of 3.
def _comp_list():
    return [_row(1, 1), _row(1, 2), _row(1, 3), _row(2, 1), _row(2, 2),
            _row(3, 1)]


# ---------------------------------------------------------------------------
# 1. Skipping.
# ---------------------------------------------------------------------------

def test_next_manuscript_leaves_the_one_you_are_reading():
    d = _RD(_comp_list(), idx=0)
    d.navigate_manuscript_results(1)
    assert d.loaded == [3], (
        'Next Manuscript landed on a page of the manuscript it started in')


def test_next_from_the_middle_of_a_manuscript_still_leaves_it():
    """The reason the button exists: from page 2 of 3, Next Result would
    show page 3 of the same fragment."""
    d = _RD(_comp_list(), idx=1)
    d.navigate_manuscript_results(1)
    assert d.loaded == [3]


def test_prev_lands_on_the_NEAREST_earlier_result_in_another_manuscript():
    """Owner ruling 2026-08-28: the button means the next result that is a
    new manuscript, in whichever direction it is pressed. An earlier draft
    walked back past the whole previous manuscript to its first page, so
    that Prev-then-Next returned you exactly where you started -- but that
    skips over results the reader has not seen."""
    d = _RD(_comp_list(), idx=4)          # manuscript 2, page 2
    d.navigate_manuscript_results(-1)
    assert d.loaded == [2], (
        'Prev skipped past the pages of manuscript 1 to its first')


def test_prev_from_the_top_of_a_manuscript_leaves_it():
    """The one case where Prev Result and Prev Manuscript agree: standing
    on the first page of a manuscript, both move one row back."""
    d = _RD(_comp_list(), idx=3)          # first page of manuscript 2
    d.navigate_manuscript_results(-1)
    assert d.loaded == [2]


def test_repeated_presses_visit_each_manuscript_exactly_once():
    """Forwards then backwards over the whole list: every manuscript is
    entered once per pass and none is skipped."""
    d = _RD(_comp_list(), idx=0)
    for _ in range(5):
        d.navigate_manuscript_results(1)
    assert d.loaded == [3, 5], 'a manuscript was visited twice or skipped'
    for _ in range(5):
        d.navigate_manuscript_results(-1)
    assert d.loaded == [3, 5, 4, 2]


def test_the_last_manuscript_has_no_next():
    d = _RD(_comp_list(), idx=5)
    d.navigate_manuscript_results(1)
    assert d.loaded == [], 'navigation ran off the end of the list'


def test_the_first_manuscript_has_no_prev():
    d = _RD(_comp_list(), idx=1)          # mid-way through manuscript 1
    d.navigate_manuscript_results(-1)
    assert d.loaded == []


# ---------------------------------------------------------------------------
# 2. When the buttons are worth their space.
# ---------------------------------------------------------------------------

def test_shown_for_a_composition_shaped_list():
    d = _RD(_comp_list(), idx=0)
    d._update_manuscript_nav()
    assert d.btn_res_next_ms.visible and d.btn_res_prev_ms.visible


def test_hidden_when_every_result_is_a_different_manuscript():
    """Then they would do exactly what Prev/Next Result already does."""
    d = _RD([_row(1, 1), _row(2, 1), _row(3, 1)], idx=0)
    d._update_manuscript_nav()
    assert not d.btn_res_next_ms.visible
    assert not d.btn_res_prev_ms.visible


def test_hidden_when_the_whole_list_is_one_manuscript():
    d = _RD([_row(1, 1), _row(1, 2), _row(1, 3)], idx=0)
    d._update_manuscript_nav()
    assert not d.btn_res_next_ms.visible
    assert not d.btn_res_prev_ms.visible


def test_disabled_at_the_two_ends():
    first = _RD(_comp_list(), idx=0)
    first._update_manuscript_nav()
    assert not first.btn_res_prev_ms.enabled
    assert first.btn_res_next_ms.enabled

    last = _RD(_comp_list(), idx=5)
    last._update_manuscript_nav()
    assert last.btn_res_prev_ms.enabled
    assert not last.btn_res_next_ms.enabled


def test_the_nav_is_refreshed_on_every_result_load():
    """Without this call the buttons keep the enabled state of whichever
    result the dialog opened on, and Next stays clickable past the end."""
    assert '_update_manuscript_nav()' in _method_source('load_result_by_index')


# ---------------------------------------------------------------------------
# 3. Manuscript identity.
# ---------------------------------------------------------------------------

def test_two_unparseable_headers_are_not_one_manuscript():
    """A shared fallback key would merge every row the parser cannot place
    into one giant pseudo-manuscript, and Next would skip the lot."""
    rows = [{'raw_header': '', 'display': {}},
            {'raw_header': '', 'display': {}}]
    d = _RD(rows, idx=0)
    assert len(set(d._manuscript_keys())) == 2


def test_a_shelfmark_groups_rows_the_parser_cannot_place():
    rows = [{'raw_header': '', 'display': {'shelfmark': 'T-S 12.123'}},
            {'raw_header': '', 'display': {'shelfmark': 'T-S 12.123'}},
            {'raw_header': '', 'display': {'shelfmark': 'T-S 8.5'}}]
    d = _RD(rows, idx=0)
    assert d._manuscript_keys() == ['T-S 12.123', 'T-S 12.123', 'T-S 8.5']


def test_the_key_cache_follows_a_list_that_grows():
    """`load_by_shelfmark` appends to `all_results` while the dialog is
    open; a cache computed once would leave the new row unkeyed."""
    rows = _comp_list()
    d = _RD(rows, idx=0)
    assert len(d._manuscript_keys()) == 6
    rows.append(_row(4, 1))
    assert len(d._manuscript_keys()) == 7
    assert d._manuscript_keys()[-1] == '9900000004'


# ---------------------------------------------------------------------------
# 4. The source pane.
# ---------------------------------------------------------------------------

class _Box:
    def __init__(self, text):
        self._t = text

    def toPlainText(self):
        return self._t


class _App:
    def __init__(self, text):
        self.comp_text_area = _Box(text)


def _pane(data, pattern, box_text='alpha beta gamma delta'):
    d = _RD([], idx=0)
    d._app = _App(box_text) if box_text is not None else None
    return d._source_pane(data, pattern)


def test_chunk_rows_are_unchanged_the_one_pattern_marks_both_panes():
    text, pat = _pane({'source_ctx': 'beta', 'highlight_pattern': 'beta'},
                      'beta')
    assert pat == 'beta'
    assert text == 'alpha *beta* gamma delta'


def test_a_letter_level_row_marks_the_pane_with_its_QUERY_side_pattern():
    """The record-side pattern is the text the MANUSCRIPT carried; running
    it over the composition marks nothing, which is the reported bug."""
    data = {'source_ctx': '*gamma*',
            'highlight_pattern': 'zeta',            # what the page matched
            'source_highlight_pattern': 'gamma'}    # what the query matched
    text, pat = _pane(data, 'zeta')
    assert pat == 'gamma'
    assert text == 'alpha beta *gamma* delta'


def test_when_the_query_pattern_finds_nothing_the_excerpt_wins():
    """A promoted manuscript, or another witness of the same work: the
    winning text was never in the box, so the full text cannot be marked
    and the row's own already-marked excerpt says more."""
    data = {'source_ctx': 'context *epsilon* context',
            'highlight_pattern': 'zeta',
            'source_highlight_pattern': 'epsilon'}
    text, _pat = _pane(data, 'zeta')
    assert text == 'context *epsilon* context'


def test_a_chunk_pattern_that_finds_nothing_does_NOT_fall_back():
    """The fallback is guarded on the query-side pattern precisely so chunk
    search keeps the behaviour it has always had."""
    data = {'source_ctx': 'context *epsilon* context',
            'highlight_pattern': 'epsilon'}
    text, _pat = _pane(data, 'epsilon')
    assert text == 'alpha beta gamma delta'


def test_with_no_composition_box_the_excerpt_is_the_pane():
    data = {'source_ctx': 'context *epsilon* context',
            'source_highlight_pattern': 'epsilon'}
    text, _pat = _pane(data, None, box_text=None)
    assert text == 'context *epsilon* context'


def test_a_row_with_no_source_context_shows_no_pane():
    text, _pat = _pane({'highlight_pattern': 'beta'}, 'beta')
    assert text == ''
