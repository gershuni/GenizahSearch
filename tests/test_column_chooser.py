"""ColumnChooser: the results header's show/hide-columns menu (2026-09-24)."""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTableWidget

import desktop.column_chooser as cc

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture
def cfg(monkeypatch):
    store = {}
    monkeypatch.setattr(cc, "load_app_config", lambda: dict(store))
    monkeypatch.setattr(cc, "save_app_config", lambda d: store.update(d))
    return store


def _table():
    t = QTableWidget(0, 5)
    t.setHorizontalHeaderLabels(["", "System ID", "Library", "Shelfmark", "Snippet"])
    return t


def _chooser(t, **kw):
    return cc.ColumnChooser(t, t.horizontalHeader(), [1, 2, 3], "hidden_cols",
                            lambda c: t.horizontalHeaderItem(c).text(), **kw)


def test_default_applies_only_when_nothing_was_saved(cfg):
    t = _table()
    _chooser(t, default_hidden=[1])
    assert t.isColumnHidden(1)
    cfg["hidden_cols"] = []            # the user chose to show everything
    t2 = _table()
    _chooser(t2, default_hidden=[1])
    assert not t2.isColumnHidden(1)


def test_menu_toggles_and_saves(cfg):
    t = _table()
    ch = _chooser(t)
    menu = ch.build_menu()
    acts = {a.text(): a for a in menu.actions() if a.text()}
    assert set(acts) >= {"System ID", "Library", "Shelfmark"}
    assert "Snippet" not in acts       # not offered: always shown
    acts["Library"].setChecked(False)
    assert t.isColumnHidden(2) and cfg["hidden_cols"] == [2]
    acts["Library"].setChecked(True)
    assert not t.isColumnHidden(2) and cfg["hidden_cols"] == []


def test_show_all_and_unknown_saved_columns_are_ignored(cfg):
    cfg["hidden_cols"] = [1, 3, 99]    # 99: a column that no longer exists
    t = _table()
    ch = _chooser(t)
    assert t.isColumnHidden(1) and t.isColumnHidden(3)
    assert ch.hidden == {1, 3}
    ch.show_all()
    assert not any(t.isColumnHidden(c) for c in (1, 2, 3))
    assert cfg["hidden_cols"] == []


# --- ColumnFitter: every shown column inside the view ---------------------------

def _fit_table(width=900, rtl=False):
    from PyQt6.QtCore import Qt
    t = QTableWidget(5, 6)
    if rtl:
        t.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    for c, w in enumerate((30, 120, 150, 600, 90, 110)):   # col 3 = the snippet
        t.setColumnWidth(c, w)
    t.resize(width, 300)
    t.show()
    QApplication.processEvents()
    f = cc.ColumnFitter(t, t.horizontalHeader(), [3])
    f.fit()
    return t, f


def _used(t):
    h = t.horizontalHeader()
    return sum(h.sectionSize(c) for c in range(h.count()) if not h.isSectionHidden(c))


@pytest.mark.parametrize("rtl", [False, True])
def test_all_columns_fit_without_sideways_scrolling(rtl):
    t, f = _fit_table(900, rtl)
    assert _used(t) <= t.viewport().width()
    assert t.columnWidth(3) >= f.min_flex
    assert [t.columnWidth(c) for c in (0, 1, 2, 4, 5)] == [30, 120, 150, 90, 110]


def test_a_narrow_view_shrinks_the_other_columns_before_hiding_anything():
    t, f = _fit_table(450)
    assert _used(t) <= t.viewport().width()
    assert t.columnWidth(3) == f.min_flex or t.columnWidth(3) >= f.min_flex
    assert all(t.columnWidth(c) >= f.min_other or t.columnWidth(c) == 30 for c in (0, 1, 2, 4, 5))


def test_widening_another_column_takes_from_the_snippet_and_is_kept():
    t, f = _fit_table(900)
    before = t.columnWidth(3)
    t.horizontalHeader().resizeSection(2, 250)      # as a drag would
    QApplication.processEvents()
    f.fit()
    assert t.columnWidth(2) == 250
    assert t.columnWidth(3) == before - 100
    assert _used(t) <= t.viewport().width()


def test_hiding_a_column_gives_its_room_to_the_snippet():
    t, f = _fit_table(900)
    before = t.columnWidth(3)
    t.setColumnHidden(5, True)
    QApplication.processEvents()
    f.fit()
    assert t.columnWidth(3) == before + 110


def test_resizing_the_snippet_by_hand_stops_fitting_until_asked():
    t, f = _fit_table(900)
    t.horizontalHeader().resizeSection(3, 800)       # the reader wants it wide
    QApplication.processEvents()
    f.fit()
    assert t.columnWidth(3) == 800 and not f.auto
    f.refit()
    assert _used(t) <= t.viewport().width() and f.auto


@pytest.mark.parametrize("rtl", [False, True])
def test_dragging_a_column_in_the_real_header_moves_the_edge_the_reader_drags(rtl):
    """The complaint that led here: moving an edge right resized the wrong side.
    With the real header (CheckBoxHeader) and fitting on, widening column 2 by its
    own edge widens column 2, the snippet gives way, and nothing scrolls."""
    from PyQt6.QtCore import QEvent, QPoint, QPointF, Qt
    from PyQt6.QtGui import QMouseEvent
    from desktop.ui_widgets import CheckBoxHeader

    def ev(kind, pos):
        b = Qt.MouseButton.NoButton if kind == QEvent.Type.MouseButtonRelease else Qt.MouseButton.LeftButton
        return QMouseEvent(kind, QPointF(pos), QPointF(pos), Qt.MouseButton.LeftButton, b,
                           Qt.KeyboardModifier.NoModifier)

    t = QTableWidget(5, 6)
    if rtl:
        t.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    h = CheckBoxHeader(t, non_sortable_cols=[0, 1])
    t.setHorizontalHeader(h)
    for c, w in enumerate((30, 120, 150, 600, 90, 110)):
        t.setColumnWidth(c, w)
    t.resize(900, 300)
    t.show()
    QApplication.processEvents()
    f = cc.ColumnFitter(t, h, [3])
    f.fit()
    snippet = t.columnWidth(3)
    start, width = h.sectionViewportPosition(2), h.sectionSize(2)
    edge = (start + 1) if rtl else (start + width - 2)
    drag = -30 if rtl else 30
    y = h.height() // 2
    h.mousePressEvent(ev(QEvent.Type.MouseButtonPress, QPoint(edge, y)))
    h.mouseMoveEvent(ev(QEvent.Type.MouseMove, QPoint(edge + drag, y)))
    h.mouseReleaseEvent(ev(QEvent.Type.MouseButtonRelease, QPoint(edge + drag, y)))
    QApplication.processEvents()
    f.fit()
    assert t.columnWidth(2) == 180
    assert t.columnWidth(3) == snippet - 30
    assert _used(t) <= t.viewport().width()
