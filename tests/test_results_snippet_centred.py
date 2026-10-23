# -*- coding: utf-8 -*-
"""The Search tab's Snippet cell shows the found words in the middle of the cell.

THE DEFECT. The cell was a QLabel holding the snippet as one centred RTL
paragraph (`GenizahGUI.render_asterisks_to_html`). Qt centres a paragraph only
while it fits its label; a longer line is laid out from the label's left edge,
which for Hebrew is the END of the snippet. The engine cuts the snippet 60
characters either side of the match (`SearchEngine._highlight_by_span`), so
the match sat in the middle of the string and was visible only while the whole
string was under twice the cell's width. In the app's font it seldom was: the
match stood at the cell's right edge, cut, or beyond it (owner, 2026-10-23, a
three-word Responsa search: 14 of the 19 rows on screen showed no red at all).

The cell is now `desktop.ui_widgets.MatchCentredSnippet`. These tests render
the cell offscreen and read where the red pixels land, so they assert what a
reader sees, not a geometry the widget reports about itself. The HTML goes
through the app's own renderer, so the colour the widget finds the mark by is
the colour the app paints it in.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QImage
from PyQt6.QtWidgets import QApplication

from desktop.ui_widgets import MatchCentredSnippet
from genizah_app import GenizahGUI
from tests.test_exclusion_surfaces import (A, _personal_state_in_tmp,  # noqa: F401
                                           _res, _search, slot_errors, window)

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

_APP = QApplication.instance() or QApplication([])

_HEBREW = "אבגדהוזחטיכלמנסעפצקרשת "
MATCH = "*שלום שלום לך*"
HEIGHT = 30
TOLERANCE = 3   # px: half a narrow letter


def _context(n):
    """n characters of running Hebrew, as the engine's 60-character cut."""
    return (_HEBREW * (n // len(_HEBREW) + 1))[:n]


def _html(marked):
    return GenizahGUI.render_asterisks_to_html(None, marked)


def _cell(marked, width):
    cell = MatchCentredSnippet(_html(marked))
    cell.resize(width, HEIGHT)
    cell.show()
    QApplication.processEvents()
    return cell


def _red(widget):
    """(first, last) x of the red pixels the widget paints, or None."""
    width, height = widget.width(), widget.height()
    img = QImage(width, height, QImage.Format.Format_ARGB32)
    img.fill(Qt.GlobalColor.white)
    widget.render(img)
    xs = [x for x in range(width) for y in range(height)
          if (lambda c: c.red() > 200 and c.green() < 80 and c.blue() < 80)(img.pixelColor(x, y))]
    return (min(xs), max(xs)) if xs else None


def _middle(span):
    return (span[0] + span[1]) / 2


def _painted_label(cell):
    """The label's geometry once the cell has painted: it places the label at
    its first paint, not on insertion (500 rows a batch)."""
    _red(cell)
    return cell.label().geometry()


def test_a_match_cut_sixty_characters_either_side_is_in_the_middle_of_the_cell():
    cell = _cell(_context(60) + MATCH + _context(60), 600)
    red = _red(cell)
    assert cell.label().width() > 600, "the case needs a snippet wider than the cell"
    assert red is not None, "the match is not visible at all"
    assert abs(_middle(red) - 300) <= TOLERANCE


@pytest.mark.parametrize("before,after", [(40, 110), (110, 40)])
def test_the_match_is_centred_not_the_string(before, after):
    """A match well off the middle of a long snippet: centring the string
    would put it near the cell's edge or past it. (40 characters are about
    280 px here: more than the half-cell a centred match needs on its short
    side; with less, the text's end holds the cell's edge, as tested below.)"""
    cell = _cell(_context(before) + MATCH + _context(after), 400)
    red = _red(cell)
    assert red is not None
    assert abs(_middle(red) - 200) <= TOLERANCE


def test_the_cell_resizing_keeps_the_match_in_the_middle():
    cell = _cell(_context(40) + MATCH + _context(110), 600)
    for width in (400, 250, 500):
        cell.resize(width, HEIGHT)
        QApplication.processEvents()
        red = _red(cell)
        assert red is not None, f"no match at {width} px"
        assert abs(_middle(red) - width / 2) <= TOLERANCE, f"at {width} px"


def test_a_match_at_the_start_of_a_page_keeps_the_text_start_at_the_cell_edge():
    """No blank cell on the right of the text's start: the match is shown
    whole against the cell's right edge, as far as it can be centred."""
    whole = _red(_cell(MATCH + _context(60), 1000))     # fits: the mark's own width
    cell = _cell(MATCH + _context(60), 300)
    red = _red(cell)
    assert cell.label().width() > 300
    assert red is not None
    assert red[1] >= 300 - TOLERANCE, "the text's start is not at the cell's edge"
    assert abs((red[1] - red[0]) - (whole[1] - whole[0])) <= 1, "the match is cut"


def test_a_match_at_the_end_of_a_page_keeps_the_text_end_at_the_cell_edge():
    whole = _red(_cell(_context(60) + MATCH, 1000))
    cell = _cell(_context(60) + MATCH, 300)
    red = _red(cell)
    assert cell.label().width() > 300
    assert red is not None
    assert red[0] <= TOLERANCE, "the text's end is not at the cell's edge"
    assert abs((red[1] - red[0]) - (whole[1] - whole[0])) <= 1, "the match is cut"


def test_a_snippet_that_fits_is_shown_whole_and_centred():
    cell = _cell("אב " + MATCH + " גד", 600)
    label = _painted_label(cell)
    assert label.width() < 600
    assert label.left() >= 0 and label.right() <= 599
    assert abs(label.center().x() - 300) <= 1


def test_several_marks_are_centred_as_one_span():
    """Earlier chain terms and word-level marks: the span from the first mark
    to the last is centred, so all of them are on screen when they can be."""
    marked = _context(60) + "*שלום* ולקרוב אמר *שלום לכחוק*" + _context(30)
    cell = _cell(marked, 400)
    red = _red(cell)
    assert cell.label().width() > 400
    assert red is not None
    assert abs(_middle(red) - 200) <= TOLERANCE


def test_a_snippet_without_a_mark_shows_its_start():
    """Nothing to centre on: the start of the text, at the cell's right edge
    (it is a right-to-left paragraph), not its end as the old label showed."""
    cell = _cell(_context(120), 300)
    label = _painted_label(cell)
    assert label.width() > 300
    assert label.right() == 299


def _long_row(sid):
    row = _res(sid, 1)
    row["snippet"] = _context(40) + MATCH + _context(110)
    return row


def test_the_results_table_centres_the_match_and_still_filters_on_the_snippet(window):  # noqa: F811 (the imported fixture)
    """Through on_search_finished: the cell the table holds is the new widget,
    the match is in the middle of the column, and the Snippet filter and
    "Copy Result" still read the row's snippet from it."""
    w = window
    row = _long_row(A)
    w.results_table.setColumnWidth(w.COL_SNIPPET, 500)
    w.results_table.resize(1200, 300)
    w.results_table.show()
    _search(w, [row])
    QApplication.processEvents()
    cell = w.results_table.cellWidget(0, w.COL_SNIPPET)
    assert isinstance(cell, MatchCentredSnippet)
    assert abs(cell.width() - 500) <= 1        # the grid line
    red = _red(cell)
    assert red is not None, "the match is not visible in the table's cell"
    assert abs(_middle(red) - cell.width() / 2) <= TOLERANCE
    assert w._results_filter_text_for_row(0, w.COL_SNIPPET) == row["snippet"]
    w.results_table.hide()
