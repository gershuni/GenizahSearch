"""FindNavigator: the way through the matches of a "Find in text" box (Browse by
Shelfmark and the Manuscript Viewer; asked for by a reader, 2026-11-17).

Pinned here: the count and the match it is on (orange, the rest yellow); Enter /
Shift+Enter / F3 and the arrow buttons move round the matches, and a dialog's
default button never gets the Enter; a match off screen is scrolled into view
and the text cursor never moves; typing starts from the top of what is on
screen; new text from a render path is marked again with no match chosen and
no scroll; text changed under the marks is marked again before a move; and both
windows build one, attached to their text.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from pathlib import Path

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QApplication, QDialog, QLineEdit, QPushButton, QTextEdit, QVBoxLayout

from desktop.widgets import (
    FIND_CURRENT_COLOR, FIND_MATCH_COLOR, FindNavigator, apply_find_highlight,
)

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
ROOT = Path(__file__).resolve().parent.parent

# 200 lines; "target" on lines 5, 60, 120 and 190, far apart in a short window.
TARGET_LINES = (5, 60, 120, 190)
TEXT = "\n".join(f"line {i} target" if i in TARGET_LINES else f"line {i}" for i in range(200))


@pytest.fixture
def host():
    dialog = QDialog()
    layout = QVBoxLayout(dialog)
    box = QLineEdit()
    nav = FindNavigator(box)
    view = QTextEdit()
    view.setReadOnly(True)
    view.setPlainText(TEXT)
    default = QPushButton("OK")
    default.setDefault(True)
    clicks = []
    default.clicked.connect(lambda: clicks.append(1))
    for widget in (box, nav, view, default):
        layout.addWidget(widget)
    nav.attach(view)
    dialog.resize(400, 200)
    dialog.show()
    QApplication.processEvents()
    yield dialog, box, nav, view, clicks
    dialog.close()
    dialog.deleteLater()


def _colors(view):
    return [s.format.background().color().name() for s in view.extraSelections()]


def _line_of(view, index, nav):
    return nav.matches[index].block().blockNumber()


def _in_view(view, cursor):
    rect = view.cursorRect(cursor)
    return rect.top() >= 0 and rect.bottom() <= view.viewport().height()


def test_typing_marks_every_match_and_goes_to_the_first(host):
    _dialog, box, nav, view, _clicks = host
    box.setText("target")
    assert len(nav.matches) == 4
    assert nav.current == 0 and nav.count_label.text() == "‎1/4"
    assert _colors(view) == [FIND_CURRENT_COLOR] + [FIND_MATCH_COLOR] * 3
    assert nav.btn_next.isEnabled() and nav.btn_prev.isEnabled()


def test_enter_shift_enter_and_f3_go_round_the_matches_and_the_dialog_keeps_its_button(host):
    _dialog, box, nav, view, clicks = host
    box.setText("target")
    box.setFocus()
    seen = []
    for key, mods in ((Qt.Key.Key_Return, Qt.KeyboardModifier.NoModifier),
                      (Qt.Key.Key_Enter, Qt.KeyboardModifier.NoModifier),
                      (Qt.Key.Key_F3, Qt.KeyboardModifier.NoModifier),
                      (Qt.Key.Key_Return, Qt.KeyboardModifier.NoModifier),     # past the last
                      (Qt.Key.Key_Return, Qt.KeyboardModifier.ShiftModifier),  # back round
                      (Qt.Key.Key_F3, Qt.KeyboardModifier.ShiftModifier)):
        QTest.keyClick(box, key, mods)
        seen.append(nav.current)
        assert _in_view(view, nav.matches[nav.current])
    assert seen == [1, 2, 3, 0, 3, 2]
    assert clicks == []                       # the Enter never reached the default button
    assert nav.count_label.text() == "‎3/4"
    assert _colors(view) == [FIND_MATCH_COLOR] * 2 + [FIND_CURRENT_COLOR, FIND_MATCH_COLOR]


def test_the_buttons_move_and_only_the_view_scrolls(host):
    _dialog, box, nav, view, _clicks = host
    box.setText("target")
    before = view.textCursor().position()
    assert view.verticalScrollBar().value() == 0
    nav.btn_next.click()
    assert _line_of(view, nav.current, nav) == 60
    assert view.verticalScrollBar().value() > 0 and _in_view(view, nav.matches[1])
    nav.btn_prev.click()
    assert nav.current == 0 and _in_view(view, nav.matches[0])
    assert view.textCursor().position() == before and not view.textCursor().hasSelection()


def _scroll_to_line(view, line):
    block = view.document().findBlockByNumber(line)
    top = view.document().documentLayout().blockBoundingRect(block).top()
    view.verticalScrollBar().setValue(int(top))
    QApplication.processEvents()


def test_typing_starts_from_the_top_of_what_is_on_screen(host):
    _dialog, box, nav, view, _clicks = host
    _scroll_to_line(view, 100)                     # matches above (5, 60) and below (120, 190)
    box.setText("target")
    assert _line_of(view, nav.current, nav) == 120 and _in_view(view, nav.matches[nav.current])


def test_typing_does_not_scroll_when_the_match_is_on_screen(host):
    _dialog, box, nav, view, _clicks = host
    _scroll_to_line(view, 59)
    value = view.verticalScrollBar().value()
    box.setText("target")
    assert _line_of(view, nav.current, nav) == 60
    assert view.verticalScrollBar().value() == value


def test_new_text_is_marked_again_with_no_match_chosen_and_no_scroll(host):
    _dialog, box, nav, view, _clicks = host
    box.setText("target")
    view.setPlainText("target\n" * 3 + "\n".join("x" for _ in range(100)) + "\ntarget")
    view.verticalScrollBar().setValue(0)
    apply_find_highlight(view, box.text().strip())          # what the render paths call
    assert len(nav.matches) == 4 and nav.current == -1
    assert nav.count_label.text() == "‎0/4"
    assert _colors(view) == [FIND_MATCH_COLOR] * 4
    assert view.verticalScrollBar().value() == 0
    nav.next()                                               # the first one on screen
    assert nav.current == 0


def test_text_changed_under_the_marks_is_marked_again_before_a_move(host):
    _dialog, box, nav, view, _clicks = host
    box.setText("target")
    view.setPlainText("one target only")                    # no render path re-marked it
    nav.next()
    assert len(nav.matches) == 1 and nav.current == 0
    assert nav.matches[0].selectedText() == "target"


def test_no_match_says_0_of_0_in_red_and_an_empty_box_says_nothing(host):
    _dialog, box, nav, view, _clicks = host
    box.setText("absent")
    assert nav.count_label.text() == "‎0/0" and "c62828" in nav.count_label.styleSheet()
    assert not nav.btn_next.isEnabled() and not nav.btn_prev.isEnabled()
    nav.next()                                               # nothing to go to
    assert nav.current == -1
    box.setText("")
    assert nav.count_label.isHidden() and view.extraSelections() == []


def test_a_view_without_a_navigator_is_marked_as_before():
    view = QTextEdit()
    view.setPlainText("a target and a target")
    apply_find_highlight(view, "target")
    assert _colors(view) == [FIND_MATCH_COLOR] * 2
    apply_find_highlight(view, "")
    assert view.extraSelections() == []


def test_browse_and_the_manuscript_viewer_build_one_on_their_text():
    """The call sites: each window puts a navigator in its Find row and
    attaches it to the text it finds in; the box no longer marks on its own."""
    app = (ROOT / "genizah_app.py").read_text(encoding="utf-8")
    viewer = (ROOT / "desktop" / "result_dialog.py").read_text(encoding="utf-8")
    for src, box, nav, view, row in (
            (app, "browse_find_input", "browse_find_nav", "browse_text", "browse_find_row"),
            (viewer, "find_ms_input", "find_ms_nav", "text_ms", "ms_find_row")):
        assert f"self.{nav} = FindNavigator(self.{box})" in src
        assert f"{row}.addWidget(self.{nav})" in src
        assert f"self.{nav}.attach(self.{view})" in src
        assert f"self.{box}.textChanged.connect(lambda text: apply_find_highlight" not in src
