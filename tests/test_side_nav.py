"""SideNav: the main window's menu down one side (owner, 2026-10-24).

Pinned here: the current section is highlighted and nothing is ever ticked (no
checkable menu item); a relabelled entry keeps its icon; compact shows icons with
the label as tooltip; ☰ toggles labels when there is room and opens the menu when
there is not; the expanded column is wide enough for every label and capped for a
long one; the menu opens toward the content in both directions; a short window
scrolls the column instead of growing; and the main window builds it with the
sections, Puzzle, Joins Lab and Settings in place of the tab bar.
"""

import os
import re
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QPoint, Qt
from PyQt6.QtWidgets import QApplication, QHBoxLayout, QLabel, QMenu, QPushButton, QWidget

from desktop.widgets.side_nav import NavButton, SideNav

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

SECTIONS = ["Search", "Composition Search", "Browse by Shelfmark", "Browse by Identification"]


def _nav(width=1400, rtl=False, expanded=True):
    host = QWidget()
    if rtl:
        host.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    lay = QHBoxLayout(host)
    lay.setContentsMargins(0, 0, 0, 0)
    nav = SideNav(expanded=expanded)
    for i, label in enumerate(SECTIONS):
        nav.add_section(chr(0x1F50D + i), label)
    nav.add_separator()
    puzzle = nav.add_button("\U0001F9E9", "Fragment Puzzle")
    nav.add_separator(footer=True)
    settings = nav.add_button("⚙️", "Settings", footer=True)
    lay.addWidget(nav)
    lay.addWidget(QLabel("content"), 1)
    host.resize(width, 700)
    host.show()
    nav.fit_to(width)
    QApplication.processEvents()
    return host, nav, puzzle, settings


def _shown(btn):
    """What the button actually draws (NavButton.text() is the label)."""
    return QPushButton.text(btn)


def test_one_section_is_current_and_nothing_is_checkable_but_sections():
    host, nav, puzzle, settings = _nav()
    nav.set_current(2)
    assert [b.isChecked() for b in nav.section_buttons] == [False, False, True, False]
    nav.section_buttons[0].click()
    assert [b.isChecked() for b in nav.section_buttons] == [True, False, False, False]
    assert not puzzle.isCheckable() and not settings.isCheckable()
    nav.set_current(99)                                   # out of range: unchanged
    assert nav.section_buttons[0].isChecked()


def test_set_text_sets_the_label_and_keeps_the_icon():
    host, nav, puzzle, settings = _nav()
    icon_before = settings.icon().cacheKey()
    settings.setText("Hillel G (Logout)")
    assert settings.text() == "Hillel G (Logout)"
    assert _shown(settings) == "Hillel G (Logout)"
    assert settings.icon().cacheKey() == icon_before
    assert settings.accessibleName() == "Hillel G (Logout)"


def test_compact_shows_icons_with_the_label_as_tooltip():
    host, nav, puzzle, settings = _nav(expanded=False)
    assert not nav.is_expanded()
    for b in nav.section_buttons + [puzzle, settings]:
        assert _shown(b) == ""
        assert not b.icon().isNull()
    puzzle.setToolTip("Open the puzzle")
    assert puzzle.toolTip() == "Open the puzzle"          # the hint, as set
    assert QWidget.toolTip(puzzle) == "Fragment Puzzle\nOpen the puzzle"
    assert QWidget.toolTip(settings) == "Settings"


def test_expanded_column_fits_every_label():
    host, nav, puzzle, settings = _nav()
    assert nav.is_expanded()
    for b in nav.section_buttons + [puzzle, settings]:
        assert _shown(b) == b.text()
        assert b.width() >= b.sizeHint().width()          # never clipped by the column
    assert QWidget.toolTip(puzzle) == ""                  # the label is showing


def test_a_long_label_is_elided_and_the_column_capped():
    host, nav, puzzle, settings = _nav()
    settings.setText("A very long account name that nobody would want to see in full (Logout)")
    QApplication.processEvents()
    assert nav.width() <= SideNav.MAX_EXPANDED + nav._scroll.verticalScrollBar().sizeHint().width()
    assert _shown(settings).endswith("…")
    assert QWidget.toolTip(settings) == settings.text()


def test_toggle_switches_labels_when_there_is_room():
    host, nav, puzzle, settings = _nav()
    seen = []
    nav.toggled.connect(seen.append)
    wide = nav.width()
    nav.toggle_button.click()
    assert not nav.is_expanded() and seen == [False]
    assert nav.width() < wide
    nav.toggle_button.click()
    assert nav.is_expanded() and seen == [False, True]


def test_no_room_keeps_the_column_compact_and_toggle_opens_the_menu(monkeypatch):
    host, nav, puzzle, settings = _nav(width=600)
    assert not nav.is_expanded()                          # expanded would take > 20%
    opened, seen = [], []
    monkeypatch.setattr(nav, "_show_menu", lambda: opened.append(True))
    nav.toggled.connect(seen.append)
    nav.toggle_button.click()
    assert opened == [True] and seen == []                # the choice is untouched
    nav.fit_to(1600)
    assert nav.is_expanded()                              # room again: the choice applies


def test_menu_has_icons_labels_and_no_tick_boxes():
    host, nav, puzzle, settings = _nav()
    nav.set_current(1)
    settings.setEnabled(False)
    puzzle_clicks = []
    puzzle.clicked.connect(lambda: puzzle_clicks.append(True))
    hidden = nav.add_button("\U0001F310", "Hidden", footer=True)
    hidden.setVisible(False)
    menu = nav.build_menu(QMenu())
    items = [a for a in menu.actions() if not a.isSeparator()]
    assert [a.text() for a in items] == SECTIONS + ["Fragment Puzzle", "Settings"]
    assert not any(a.isCheckable() for a in menu.actions())
    assert [a.font().bold() for a in items[:4]] == [False, True, False, False]
    assert not items[-1].isEnabled()
    assert all(not a.icon().isNull() for a in items)
    assert sum(a.isSeparator() for a in menu.actions()) == 2   # between the three groups
    items[4].trigger()
    assert puzzle_clicks == [True]
    items[0].trigger()
    assert nav.section_buttons[0].isChecked()


@pytest.mark.parametrize("rtl", [False, True])
def test_menu_opens_toward_the_content(rtl):
    host, nav, puzzle, settings = _nav(width=600, rtl=rtl)
    menu = nav.build_menu(QMenu(nav))
    pos = nav.menu_position(menu)
    anchor = nav.toggle_button
    left = anchor.mapToGlobal(QPoint(0, 0)).x()
    if rtl:
        assert pos.x() + menu.sizeHint().width() == left + anchor.width()
    else:
        assert pos.x() == left
    assert pos.y() > anchor.mapToGlobal(QPoint(0, 0)).y()


def test_a_short_window_scrolls_the_column():
    host, nav, puzzle, settings = _nav()
    for i in range(20):
        nav.add_button("\U0001F4CB", f"Extra {i}")
    QApplication.processEvents()
    entries = sum(b.sizeHint().height() for b, _g in nav._entries)
    assert nav.minimumSizeHint().height() < entries / 2


def test_nav_button_is_a_drop_in_for_qpushbutton_text():
    b = NavButton("\U0001F464", "Login")
    b.setText("Logging out...")
    b.setEnabled(False)
    assert b.text() == "Logging out..." and not b.isEnabled()
    b.setText("")
    assert b.text() == ""


def test_main_window_builds_the_side_menu_in_place_of_the_tab_bar():
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    init_ui = src[src.index("    def init_ui(self):"):src.index("    def _update_corner_login_state(self):")]
    assert "self.tabs.tabBar().hide()" in init_ui
    assert "setCornerWidget" not in init_ui
    for label in ("Fragment Puzzle", "Joins Lab", "Settings"):
        assert re.search(r'self\.side_nav\.add_button\([^)]*tr\("%s"\)' % label, init_ui), label
    assert "self.tabs.currentChanged.connect(self.side_nav.set_current)" in init_ui
    assert "_fill_sections_menu" not in src
