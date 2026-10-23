# -*- coding: utf-8 -*-
"""The found words sit in the middle of a snippet cell: the Search tab's
Snippet column and the Composition tab's Context / MS Context columns.

THE DEFECT (Search). The cell was a QLabel holding the snippet as one centred
RTL paragraph (`GenizahGUI.render_asterisks_to_html`). Qt centres a paragraph
only while it fits its label; a longer line is laid out from the label's left
edge, which for Hebrew is the END of the snippet. The engine cuts the snippet
60 characters either side of the match (`SearchEngine._highlight_by_span`), so
the match sat in the middle of the string and was visible only while the whole
string was under twice the cell's width. In the app's font it seldom was: the
match stood at the cell's right edge, cut, or beyond it (owner, 2026-10-23, a
three-word Responsa search: 14 of the 19 rows on screen showed no red at all).

THE DEFECT (Composition). The two context cells were `HiddenScrollArea`, a
QScrollArea whose scrollbar was set from the mark's share of the text's
CHARACTERS, so the match stood near the middle only as the letters' widths
allowed, after a 10 ms timer per cell and per column resize.

Both cells are now `desktop.ui_widgets.MatchCentredSnippet` (the Composition
ones its `ContextSnippet`). These tests render the cells offscreen and read
where the red pixels land, so they assert what a reader sees, not a geometry
the widget reports about itself. The Search HTML goes through the app's own
renderer, so the colour the widget finds the mark by is the colour the app
paints it in.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import QPoint, QPointF, Qt
from PyQt6.QtGui import QFont, QImage, QWheelEvent
from PyQt6.QtWidgets import QApplication, QMainWindow, QTreeWidget, QTreeWidgetItem

from desktop.ui_widgets import ContextSnippet, MatchCentredSnippet
from genizah_app import GenizahGUI
from tests.test_exclusion_surfaces import (A, _personal_state_in_tmp,  # noqa: F401
                                           _res, _search, slot_errors, window)

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True, scope="module")
def _text_rendered_aliased():
    """Every pixel is the text's colour or the background's. With subpixel
    (LCD) antialiasing, which CI's runners and most desktops render with,
    the fringes of BLACK glyphs are pure red or blue pixels and read as the
    mark (gui-tests on PR #445: a 12-letter match "470 px wide"). The cells
    measure with the same font they paint with, so what they centre is what
    the app centres; only the edges of the letters differ."""
    before = QApplication.font()
    aliased = QFont(before)
    aliased.setStyleStrategy(QFont.StyleStrategy.NoAntialias)
    QApplication.setFont(aliased)
    yield
    QApplication.setFont(before)


_HEBREW = "אבגדהוזחטיכלמנסעפצקרשת "
MATCH = "*שלום שלום לך*"
HEIGHT = 30
TOLERANCE = 3   # px: half a narrow letter


def _context(n):
    """n characters of running Hebrew, as the engine's 60-character cut."""
    return (_HEBREW * (n // len(_HEBREW) + 1))[:n]


def _html(marked):
    return GenizahGUI.render_asterisks_to_html(None, marked)


def _shown(cell, width):
    cell.resize(width, HEIGHT)
    cell.show()
    QApplication.processEvents()
    return cell


def _cell(marked, width):
    return _shown(MatchCentredSnippet(_html(marked)), width)


def _context_cell(marked, width, anchor_text=None):
    return _shown(ContextSnippet(marked, anchor_text=anchor_text), width)


def _red(widget):
    """(first, last) x of the red pixels the widget paints, or None."""
    width, height = widget.width(), widget.height()
    img = QImage(width, height, QImage.Format.Format_ARGB32)
    img.fill(Qt.GlobalColor.white)
    widget.render(img)
    xs = [x for x in range(width) for y in range(height)
          if (lambda c: c.red() > 150 and c.green() < 80 and c.blue() < 80)(img.pixelColor(x, y))]
    return (min(xs), max(xs)) if xs else None


def _middle(span):
    return (span[0] + span[1]) / 2


def _painted_label(cell):
    """The label's geometry once the cell has painted: it places the label at
    its first paint, not on insertion (500 rows a batch)."""
    _red(cell)
    return cell.label().geometry()


# =========================================================================
# The Search tab's Snippet cell
# =========================================================================

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


def test_the_search_cell_leaves_the_wheel_to_the_table():
    """The wheel over the Snippet column scrolls the results, as it always
    did: the cell does not take the event."""
    cell = _cell(_context(40) + MATCH + _context(110), 400)
    x_before = _painted_label(cell).x()
    wheel = _wheel_up(cell)
    QApplication.sendEvent(cell, wheel)
    assert not wheel.isAccepted(), "the cell consumed the wheel"
    assert cell.label().geometry().x() == x_before


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


# =========================================================================
# The Composition tab's Context and MS Context cells
# =========================================================================

GAP = ContextSnippet.MARGIN


def _wheel_up(widget, notches=1):
    pos = QPointF(widget.width() / 2, widget.height() / 2)
    return QWheelEvent(pos, widget.mapToGlobal(pos.toPoint()).toPointF(), QPoint(), QPoint(0, 120 * notches),
                       Qt.MouseButton.NoButton, Qt.KeyboardModifier.NoModifier,
                       Qt.ScrollPhase.NoScrollPhase, False)


def test_the_context_cell_centres_the_marked_chunk_inside_its_gap():
    """The chunk is in the middle of the cell, and the 5 px either side that
    keep the two columns' texts apart stay clear of text."""
    cell = _context_cell(_context(40) + MATCH + _context(110), 400)
    red = _red(cell)
    assert cell.label().width() > 400
    assert red is not None, "the chunk is not visible"
    assert abs(_middle(red) - 200) <= TOLERANCE
    assert red[0] >= GAP and red[1] < 400 - GAP, "text is painted in the gap"


def test_the_ms_context_cell_centres_on_the_chunk_the_context_cell_shows():
    """Two marked chunks in the MS text, too far apart to both be on screen
    whole: with the Context cell's chunk as the anchor, that chunk is centred
    and whole and the other is off screen; without an anchor the span of the
    two is centred, so parts of both are on screen, cut at the edges.

    The gap between the chunks is sized in pixels from this font's letters:
    about 190 px in a 300 px cell. Further than the half-cell less half a
    chunk, so centring one chunk puts the other off screen; nearer than the
    cell, so centring the span shows both ends. Thirty letters were that on
    Ubuntu and more than the cell on Windows, whose Hebrew is wider (gui-tests
    on PR #445: no red at all)."""
    other = "*אאא בבב גגג*"
    whole = _red(_context_cell(MATCH, 400))                 # the anchor's own width
    mark_w = whole[1] - whole[0]
    letter_w = _painted_label(_context_cell(_context(30), 1000)).width() / 30
    ms = _context(40) + other + _context(round(190 / letter_w)) + MATCH + _context(40)
    anchored = _context_cell(ms, 300, anchor_text=MATCH.strip("*"))
    red = _red(anchored)
    assert red is not None
    assert abs(_middle(red) - 150) <= TOLERANCE
    assert abs((red[1] - red[0]) - mark_w) <= 1, "not the anchor alone, whole"
    unanchored = _context_cell(ms, 300)
    red = _red(unanchored)
    assert red is not None, "neither chunk is on screen"
    assert red[1] - red[0] > 1.5 * mark_w, "only one chunk is on screen"


def test_the_marked_html_keeps_the_old_marker_rules_and_escapes_the_text():
    """The rules HiddenScrollArea applied, verbatim: a pair is a mark; a lone
    marker followed by text opens a mark that the end of the text closes (a
    chunk the context's cut ran through); a lone marker ending the text closes
    one the start opened. New: HTML in the text is text."""
    mark = f"<b style='color:{ContextSnippet.MARK_COLOUR.name()};'>"
    assert ContextSnippet.marked_html("ab *cd* ef") == f"<div dir='rtl'>ab {mark}cd</b> ef</div>"
    assert ContextSnippet.marked_html("ab *cd") == f"<div dir='rtl'>ab {mark}cd</b></div>"
    assert ContextSnippet.marked_html("ab cd*") == f"<div dir='rtl'>{mark}ab cd</b></div>"
    assert ContextSnippet.marked_html("a < b & *c*") == f"<div dir='rtl'>a &lt; b &amp; {mark}c</b></div>"
    assert ContextSnippet.marked_html("") == "<div dir='rtl'></div>"
    assert ContextSnippet("ab *cd* ef").toolTip() == "ab cd ef"


def test_the_wheel_slides_the_context_text_no_further_than_its_ends():
    cell = _context_cell(_context(40) + MATCH + _context(110), 400)
    x0 = _painted_label(cell).x()
    assert x0 < 0
    assert QApplication.sendEvent(cell, _wheel_up(cell)), "the cell did not take the wheel"
    assert cell.label().geometry().x() == x0 + 120
    QApplication.sendEvent(cell, _wheel_up(cell, notches=50))
    assert cell.label().geometry().x() == 0, "slid past the text's start"
    # Wheeling on past the end banked nothing: one turn back moves the text
    # at once (Codex on PR #445: the old scrollbar clamped its value too).
    QApplication.sendEvent(cell, _wheel_up(cell, notches=-1))
    assert cell.label().geometry().x() == -120
    QApplication.sendEvent(cell, _wheel_up(cell, notches=1))
    assert cell.label().geometry().x() == 0
    # And the start is held when the column grows (resizing re-places it).
    cell.resize(500, HEIGHT)
    QApplication.processEvents()
    assert cell.label().geometry().x() == 0


def _composition_window():
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    w.comp_tree = QTreeWidget()
    w.comp_tree.setColumnCount(7)
    w.comp_col_context = 5
    w.comp_col_ms_context = 6
    return w


def test_the_composition_tree_centres_the_ms_chunk_and_keeps_its_rows_slim():
    """Through _set_comp_node_previews, as the tab fills a node: the two cells
    are ContextSnippets, the MS cell shows the Context cell's chunk in its
    middle with the whole text as its tooltip, and the row is no taller than
    a row without cells asks or the old cell's fixed height."""
    w = _composition_window()
    tree = w.comp_tree
    plain = QTreeWidgetItem(["1", "CUL", "T-S 1.1", "t", "9901", "", ""])
    node = QTreeWidgetItem(["2", "CUL", "T-S 1.2", "t", "9902", "", ""])
    tree.addTopLevelItem(plain)
    tree.addTopLevelItem(node)
    for col, width in ((0, 40), (1, 40), (2, 70), (3, 40), (4, 60), (5, 300), (6, 400)):
        tree.setColumnWidth(col, width)
    tree.resize(1000, 200)
    tree.show()
    src = _context(20) + MATCH + _context(20)
    ms = _context(40) + MATCH + _context(110)
    w._set_comp_node_previews(node, src, ms)
    QApplication.processEvents()
    cell = tree.itemWidget(node, w.comp_col_ms_context)
    assert isinstance(cell, ContextSnippet)
    assert isinstance(tree.itemWidget(node, w.comp_col_context), ContextSnippet)
    assert cell.width() >= 400                  # the last column stretches to the tree
    assert cell.label().width() > cell.width(), "the case needs a text wider than the cell"
    red = _red(cell)
    assert red is not None, "the chunk is not visible in the tree's cell"
    assert abs(_middle(red) - cell.width() / 2) <= TOLERANCE
    assert cell.toolTip() == ms.replace("*", "")
    slim = max(tree.visualItemRect(plain).height(), tree.fontMetrics().lineSpacing() + 4)
    assert tree.visualItemRect(node).height() <= slim
    tree.hide()
