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
MIN = QSize(920, 492)                   # a representative minimum for the pure-rule tests
# NOT the real viewer's minimum: before show() Qt treats unshown children as empty,
# so a pre-show minimumSizeHint understates it (it was ~1654 px wide after show).
# test_real_viewer_with_every_button_fits_the_reported_screen measures it properly.


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


_OPTIONAL = ('btn_search_parallels', 'btn_add_to_puzzle', 'btn_rd_find_joins', 'btn_ext_info',
             'btn_rd_bib_fjms', 'btn_rd_bib_nli', 'btn_rd_catalog', 'btn_rd_measurements',
             'btn_toggle_image', 'btn_toggle_text', 'btn_rd_translations', 'btn_res_prev_ms', 'btn_res_next_ms',
             'btn_back_to_results', 'btn_res_filters', 'lbl_res_category',
             'btn_rd_open_file', 'btn_rd_open_file_location')


def _populated(viewer):
    for name in _OPTIONAL:
        getattr(viewer, name).setVisible(True)
    viewer.btn_rd_bib_fjms.setText("Bib FJMS (4)")
    viewer.btn_rd_bib_nli.setText("Bib Ktiv (1)")
    viewer.lbl_res_category.setText("Main results group")
    viewer.lbl_shelf.setText("The National Library of Russia | Ms. EVR II A 223/1")
    viewer.lbl_title.setText("Commentary on the Torah (Genesis), with a long catalogue title")
    viewer.show()
    QApplication.processEvents()


def test_real_viewer_with_every_button_fits_the_reported_screen(viewer):
    """Measured AFTER show(): every optional button, a long shelfmark and title.

    Owner screenshot 4 (300% zoom): the viewer was wider than the screen because
    each button row was a QHBoxLayout whose minimum is the sum of its buttons
    (~1654 px here). The rows now overflow into menus and wrap instead.
    """
    _populated(viewer)
    need = viewer.minimumSizeHint()
    assert need.width() + FRAME.left() + FRAME.right() <= SHORT.width(), need
    assert need.height() + FRAME.top() + FRAME.bottom() <= SHORT.height(), need


def test_narrow_viewer_keeps_result_navigation_and_moves_the_rest_to_menus(viewer):
    _populated(viewer)
    viewer.resize(560, 540)
    QApplication.processEvents()
    viewer.layout().activate()
    QApplication.processEvents()
    for pinned in (viewer.btn_res_prev, viewer.btn_res_next, viewer.lbl_res_count):
        assert pinned not in viewer.nav_bar.overflowed()
        assert viewer.nav_bar.rect().intersects(pinned.geometry())
    over = viewer.actions_row.overflowed()
    assert viewer.btn_search_parallels in over          # owner: Parallels is rare
    assert viewer.btn_view_transcription not in over    # Browse, List, Cite stay
    assert viewer.btn_add_to_list not in over
    assert viewer.btn_cite not in over


def test_sources_fold_into_a_menu_only_when_there_is_no_room(viewer):
    """Owner, 2026-09-24: on a wide window each source is its own button, and
    Info is never inside the Sources menu."""
    _populated(viewer)
    src = (viewer.btn_rd_bib_fjms, viewer.btn_rd_bib_nli, viewer.btn_rd_catalog,
           viewer.btn_rd_measurements)
    group = viewer.actions_row.group_button(rd.tr("Sources"))
    viewer.resize(1900, 700)
    QApplication.processEvents(); viewer.layout().activate(); QApplication.processEvents()
    assert not any(b in viewer.actions_row.overflowed() for b in src)
    assert not viewer.actions_row.rect().intersects(group.geometry())      # no Sources menu
    viewer.resize(560, 540)
    QApplication.processEvents(); viewer.layout().activate(); QApplication.processEvents()
    folded = viewer.actions_row.row.overflowed_in(rd.tr("Sources"))
    assert folded and all(b in src for b in folded)
    assert viewer.actions_row.rect().intersects(group.geometry())          # Sources shown
    assert viewer.btn_ext_info not in folded                                 # Info never there
    group.menu().aboutToShow.emit()
    texts = [a.text() for a in group.menu().actions()]
    assert "Bib FJMS (4)" in texts or "Bib Ktiv (1)" in texts


def test_text_toggle_follows_the_image_pane(viewer):
    """The text toggle is the image toggle's twin: pressed = text shown."""
    viewer.show()
    QApplication.processEvents()
    viewer.external_pane.setVisible(True)
    QApplication.processEvents()
    assert not viewer.btn_toggle_text.isHidden() and viewer.btn_toggle_text.isChecked()
    viewer.btn_toggle_text.click()                       # hide the text
    assert viewer.ms_widget.isHidden()
    viewer.btn_toggle_text.click()                       # and back
    assert not viewer.ms_widget.isHidden()
    viewer.btn_toggle_text.click()
    viewer.external_pane.setVisible(False)              # e.g. the next result has no image
    QApplication.processEvents()
    assert viewer.btn_toggle_text.isChecked()
    assert not viewer.ms_widget.isHidden()               # the text is never lost
    assert viewer.btn_toggle_text.isHidden()


def test_small_screen_starts_with_the_image_sliders_folded(viewer):
    assert not viewer.ms_viewer.adj_row.isHidden()
    viewer.fit_on_screen(QPoint(546, 290), _Screen(SHORT))
    assert viewer.ms_viewer.adj_row.isHidden()
    assert not viewer.ms_viewer.btn_adjust.isChecked()
    viewer.ms_viewer.btn_adjust.setChecked(True)          # one click brings them back
    assert not viewer.ms_viewer.adj_row.isHidden()


def test_text_size_buttons_resize_both_panes_and_remember(viewer, monkeypatch):
    saved = {}
    monkeypatch.setattr(rd, "save_app_config", lambda d: saved.update(d))
    start = viewer.text_ms.font().pointSize()
    viewer.btn_text_larger.click()
    assert viewer.text_ms.font().pointSize() == start + rd.TEXT_PT_STEP
    assert viewer.text_src.font().pointSize() == start + rd.TEXT_PT_STEP
    assert saved[rd.VIEWER_TEXT_PT_KEY] == start + rd.TEXT_PT_STEP
    for _ in range(40):
        viewer.btn_text_smaller.click()
    assert viewer.text_ms.font().pointSize() == rd.TEXT_PT_MIN
    assert not viewer.btn_text_smaller.isEnabled()      # at the floor


def test_a_fitted_viewer_keeps_a_margin_from_the_screen_edge(viewer):
    """Windows' real frame is wider than Qt reports; flush to the edge, the owner's
    viewer ended ~15 px past the right edge and the start of every row was cut."""
    viewer.fit_on_screen(QPoint(546, 290), _Screen(SHORT))
    m = rd.SCREEN_MARGIN
    assert viewer.pos().x() >= SHORT.left() + m
    assert viewer.pos().y() >= SHORT.top() + m
    assert viewer.pos().x() + viewer.width() + FRAME.left() + FRAME.right() <= SHORT.right() + 1 - m
