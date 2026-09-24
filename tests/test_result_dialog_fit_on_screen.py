"""The Manuscript Viewer must open with its title bar on screen (user report, 2026-09-24).

A user on a short screen found the viewer opening with its top above the screen
edge: the taskbar's "Move" did not help and only "Maximize" recovered it, every
time. Since v9.2.1 the unparented viewer is centred over the main window with an
explicit move(), which also turns off Qt's own QDialog placement (the step that
used to keep a dialog's top inside the screen), and the viewer is 1300x850. On
1366x768 at 125% scaling the usable height is about 580 logical px.

Pinned here: ``fit_window_geometry`` (the rule), and the real
``ResultDialog.fit_on_screen`` against a fake screen.
"""

import os
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QMargins, QPoint, QRect, QSize
from PyQt6.QtWidgets import QApplication

import desktop.result_dialog as rd

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

_APP = QApplication.instance() or QApplication([])

FRAME = QMargins(8, 32, 8, 8)
SHORT = QRect(0, 0, 1092, 580)          # 1366x768 at 125%, taskbar at the bottom
WANT = QSize(1300, 850)
MIN = QSize(920, 492)                   # the viewer's measured minimumSizeHint


def _frame_rect(top_left, size, frame=FRAME):
    return QRect(top_left.x(), top_left.y(),
                 size.width() + frame.left() + frame.right(),
                 size.height() + frame.top() + frame.bottom())


def _inside(rect, avail):
    return (rect.left() >= avail.left() and rect.top() >= avail.top()
            and rect.right() <= avail.right() and rect.bottom() <= avail.bottom())


def test_short_screen_shrinks_the_viewer_and_keeps_it_inside():
    top_left, size = rd.fit_window_geometry(SHORT, WANT, MIN, QPoint(546, 290), FRAME)
    assert size == QSize(1092 - 16, 580 - 40)
    assert _inside(_frame_rect(top_left, size), SHORT)


def test_the_title_bar_wins_when_even_the_minimum_does_not_fit():
    tiny = QRect(0, 0, 800, 450)
    top_left, size = rd.fit_window_geometry(tiny, WANT, MIN, QPoint(400, 225), FRAME)
    assert size == MIN                   # never below the layout's minimum
    assert top_left == QPoint(0, 0)      # the frame's top-left, i.e. the title bar, on screen


def test_a_big_screen_keeps_the_size_and_centres_on_the_main_window():
    big = QRect(0, 0, 1920, 1040)
    center = QPoint(960, 520)
    top_left, size = rd.fit_window_geometry(big, WANT, MIN, center, FRAME)
    assert size == WANT
    assert _frame_rect(top_left, size).center().x() in (center.x() - 1, center.x())
    assert abs(_frame_rect(top_left, size).center().y() - center.y()) <= 1


def test_a_main_window_near_the_edge_pulls_the_viewer_back_on_screen():
    big = QRect(0, 0, 1920, 1040)
    top_left, size = rd.fit_window_geometry(big, WANT, MIN, QPoint(1900, 30), FRAME)
    assert _inside(_frame_rect(top_left, size), big)


def test_a_second_monitor_offset_is_respected():
    right = QRect(1920, 0, 1092, 580)
    top_left, size = rd.fit_window_geometry(right, WANT, MIN, QPoint(2466, 290), FRAME)
    assert _inside(_frame_rect(top_left, size), right)


class _Screen:
    def __init__(self, rect):
        self._r = rect

    def availableGeometry(self):
        return self._r


@pytest.fixture
def viewer(monkeypatch):
    monkeypatch.setattr(rd.ResultDialog, "load_result_by_index", lambda self, i: None)
    dlg = rd.ResultDialog(MagicMock(), [{}], 0, MagicMock(), MagicMock())
    yield dlg
    dlg.deleteLater()


def test_real_viewer_fits_a_short_screen_before_it_is_shown(viewer):
    assert viewer.size() == WANT         # what init_ui asks for
    viewer.fit_on_screen(QPoint(546, 290), _Screen(SHORT))
    assert viewer.pos().y() >= SHORT.top()
    assert viewer.pos().x() >= SHORT.left()
    assert viewer.height() + FRAME.top() + FRAME.bottom() <= SHORT.height()
    assert viewer.width() + FRAME.left() + FRAME.right() <= SHORT.width()


def test_real_viewer_keeps_its_size_on_a_big_screen(viewer):
    viewer.fit_on_screen(QPoint(960, 520), _Screen(QRect(0, 0, 1920, 1040)))
    assert viewer.size() == WANT
    assert viewer.pos().y() >= 0
