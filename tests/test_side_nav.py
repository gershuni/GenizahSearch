"""SideNav and the two menu layouts (owner, 2026-10-24).

Pinned here: the current section is highlighted and nothing is ever ticked (no
checkable menu item); a relabelled entry keeps its icon; compact shows icons with
the label as tooltip; ☰ toggles labels when there is room and opens the menu when
there is not; the expanded column is wide enough for every label and capped for a
long one; menus open toward the content in both directions; a short window
scrolls the column instead of growing. And in the window: the tab layout is the
default and the side menu an option that applies at once (Cancel in Settings puts
it back); the tab layout's ☰ menu lists the sections with their Ctrl+N
shortcuts, Puzzle, Joins Lab and Settings and no tick boxes; the login button and
the language switch relabel their side-menu twins; a logged-in click opens the
account menu (Sync lists now, Logout) instead of logging out.
"""

import os
import re
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QPoint, Qt
from PyQt6.QtWidgets import (
    QApplication, QComboBox, QHBoxLayout, QLabel, QMenu, QPushButton, QTabWidget, QWidget,
)

import genizah_app
from desktop.widgets.side_nav import LinkedButton, NavButton, SideNav, ltr, popup_point
from genizah_app import GenizahGUI

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


@pytest.mark.parametrize("rtl", [False, True])
def test_a_side_menu_entry_opens_its_menu_beside_it(rtl):
    host, nav, puzzle, settings = _nav(rtl=rtl)
    menu = QMenu(nav)
    menu.addAction("Logout")
    pos = popup_point(settings, menu, beside=True)
    left = settings.mapToGlobal(QPoint(0, 0)).x()
    if rtl:
        assert pos.x() + menu.sizeHint().width() == left          # toward the content
    else:
        assert pos.x() == left + settings.width()
    bottom = settings.mapToGlobal(QPoint(0, settings.height())).y()
    assert pos.y() + menu.sizeHint().height() == bottom             # grows up from its foot


def test_sections_carry_their_shortcut_into_the_hint_and_the_menu():
    host = QWidget()
    nav = SideNav()
    nav.add_section("\U0001F50D", "Search", "Ctrl+1")
    nav.add_section("\U0001F4DC", "Composition Search", "Ctrl+2")
    QHBoxLayout(host).addWidget(nav)
    nav.set_current(0)
    # Marked left-to-right: in the Hebrew UI "Ctrl+," would show as ",Ctrl+".
    assert nav.section_buttons[0].toolTip() == ltr("Ctrl+1") == "\u200eCtrl+1\u200e"
    menu = nav.build_menu(QMenu(), extras=[("Back to tabs at the top", lambda: None)])
    texts = [a.text() for a in menu.actions()]
    assert texts == [f"Search\t{ltr('Ctrl+1')}", f"Composition Search\t{ltr('Ctrl+2')}", "",
                     "Back to tabs at the top"]


def test_linked_button_relabels_and_disables_its_twin():
    twin = NavButton("\U0001F464", "")
    btn = LinkedButton("Login")
    btn.link(twin)
    assert twin.text() == "Login"                      # the state at link time
    btn.setEnabled(False)
    btn.setText("Logging out...")
    assert twin.text() == "Logging out..." and not twin.isEnabled()
    btn.setEnabled(True)
    btn.setText("Hillel G \u25be")
    assert twin.text() == "Hillel G \u25be" and twin.isEnabled()
    assert not twin.icon().isNull()                    # the twin keeps its icon


# -- the window's two layouts ----------------------------------------------------

class _Window:
    """The parts of GenizahGUI the menu layouts use, with the real methods."""

    _apply_nav_layout = GenizahGUI._apply_nav_layout
    set_nav_layout = GenizahGUI.set_nav_layout
    cancel_settings = GenizahGUI.cancel_settings
    _fill_sections_menu = GenizahGUI._fill_sections_menu
    _fill_account_menu = GenizahGUI._fill_account_menu
    nav_layout = GenizahGUI.nav_layout
    # Cancel also puts the text size back (tests/test_text_size.py); no lists here.
    RESULTS_TEXT_KEY = GenizahGUI.RESULTS_TEXT_KEY
    RESULTS_TEXT_STEPS = GenizahGUI.RESULTS_TEXT_STEPS
    _results_text_widgets = GenizahGUI._results_text_widgets
    _apply_results_text_level = GenizahGUI._apply_results_text_level

    def __init__(self):
        self.tabs = QTabWidget()
        for name in ("Search", "Composition Search", "Personal Lists"):
            self.tabs.addTab(QWidget(), name)
        self.corner_row = QLabel("corner")
        self.tabs.setCornerWidget(self.corner_row)
        self.corner_menu_holder = QLabel("\u2630")
        self.tabs.setCornerWidget(self.corner_menu_holder, Qt.Corner.TopLeftCorner)
        self.side_nav = SideNav()
        for i in range(self.tabs.count()):
            btn = self.side_nav.add_section("\u2022", self.tabs.tabText(i), f"Ctrl+{i + 1}")
            btn.clicked.connect(lambda _c=False, idx=i: self.tabs.setCurrentIndex(idx))
        self.tabs.currentChanged.connect(self.side_nav.set_current)
        self.side_nav.set_current(0)
        self.side_nav.add_separator()
        self.puzzle_opened = []
        self.nav_puzzle_btn = self.side_nav.add_button("\U0001F9E9", "Fragment Puzzle")
        self.nav_puzzle_btn.clicked.connect(lambda: self.puzzle_opened.append(True))
        self.nav_joins_btn = self.side_nav.add_button("\U0001F517", "Joins Lab")
        self.side_nav.add_separator(footer=True)
        self.nav_settings_btn = self.side_nav.add_button("\u2699", "Settings", footer=True)
        self.nav_login_btn = self.side_nav.add_button("\U0001F464", "", footer=True)
        self.nav_lang_btn = self.side_nav.add_button("\U0001F524", "", footer=True)
        self.nav_website_btn = self.side_nav.add_button("\U0001F310", "GenizahSearch.com", footer=True)
        self.corner_login_btn = LinkedButton("Login")
        self.corner_login_btn.link(self.nav_login_btn)
        self.host = QWidget()
        lay = QHBoxLayout(self.host)
        lay.addWidget(self.side_nav)
        lay.addWidget(self.tabs, 1)
        self._corner_sections_menu = QMenu()
        combo = QComboBox()
        combo.addItems(["Tabs at the top", "Side menu"])
        self.cancelled = []
        self.settings_dialog = type("Dlg", (), {
            "combo_nav_layout": combo,
            "_on_cancel": lambda _self: self.cancelled.append(True)})()
        self.btn_lists_sync_now = QPushButton("Sync lists now")
        self.logouts = []
        self._apply_nav_layout("tabs")
        self.host.show()
        QApplication.processEvents()

    def _do_logout(self):
        self.logouts.append(True)


@pytest.fixture
def window(monkeypatch):
    cfg = {}
    monkeypatch.setattr(genizah_app, "save_app_config", cfg.update)
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: dict(cfg))
    w = _Window()
    w.cfg = cfg
    return w


def test_tabs_are_the_default_and_the_side_menu_applies_at_once(window):
    w = window
    assert w.nav_layout == "tabs"
    assert w.tabs.tabBar().isVisible() and w.corner_row.isVisible() and not w.side_nav.isVisible()
    assert w.corner_menu_holder.isVisible()
    w.set_nav_layout("side")
    assert not w.tabs.tabBar().isVisible() and not w.corner_row.isVisible() and w.side_nav.isVisible()
    assert not w.corner_menu_holder.isVisible()
    assert w.cfg == {"nav_layout": "side"}
    assert w.settings_dialog.combo_nav_layout.currentIndex() == 1
    w.set_nav_layout("tabs")
    assert w.tabs.tabBar().isVisible() and w.corner_row.isVisible() and not w.side_nav.isVisible()
    assert w.cfg == {"nav_layout": "tabs"} and w.settings_dialog.combo_nav_layout.currentIndex() == 0


def test_cancel_in_settings_puts_the_layout_back(window):
    w = window
    w.cfg["nav_layout"] = "tabs"                       # what the snapshot restores
    w._apply_nav_layout("side")                        # picked in the dialog, not kept
    w.cancel_settings()
    assert w.cancelled == [True] and w.nav_layout == "tabs" and not w.side_nav.isVisible()


def test_the_tab_layouts_menu_has_sections_tools_settings_and_no_tick_boxes(window):
    w = window
    w.tabs.setCurrentIndex(1)
    w._fill_sections_menu()
    acts = w._corner_sections_menu.actions()
    texts = [a.text() for a in acts if not a.isSeparator()]
    assert texts == [f"Search\t{ltr('Ctrl+1')}", f"Composition Search\t{ltr('Ctrl+2')}",
                     f"Personal Lists\t{ltr('Ctrl+3')}",
                     "Fragment Puzzle", "Joins Lab", "Settings", "Move the menu to the side"]
    assert not any(a.isCheckable() for a in acts)
    assert [a.font().bold() for a in acts[:3]] == [False, True, False]
    next(a for a in acts if a.text() == "Fragment Puzzle").trigger()
    assert w.puzzle_opened == [True]
    acts[0].trigger()
    assert w.tabs.currentIndex() == 0
    acts[-1].trigger()
    assert w.nav_layout == "side" and w.side_nav.isVisible()


def test_the_account_menu_offers_sync_and_logout(window):
    w = window
    w.btn_lists_sync_now.setEnabled(False)
    menu = w._fill_account_menu(QMenu())
    acts = [a for a in menu.actions() if not a.isSeparator()]
    assert [a.text() for a in acts] == ["Sync lists now", "Logout"]
    assert not acts[0].isEnabled()                     # as the Lists tab's button stands
    assert w.logouts == []                             # opening it logs nobody out
    acts[1].trigger()
    assert w.logouts == [True]
    w.btn_lists_sync_now = None                        # before the Lists tab exists
    menu = w._fill_account_menu(QMenu())
    assert [a.text() for a in menu.actions()] == ["Logout"]


def test_main_window_wiring():
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    init_ui = src[src.index("    def init_ui(self):"):src.index("    def _update_corner_login_state(self):")]
    assert "self.tabs.setCornerWidget(corner_widget" in init_ui                    # tabs kept
    # ☰ at the window's start edge, before the first tab (mirrored in Hebrew).
    assert "self.tabs.setCornerWidget(self.corner_menu_holder, Qt.Corner.TopLeftCorner)" in init_ui
    assert "corner_widget.add(self.corner_sections_btn" not in init_ui
    assert "self._apply_nav_layout(load_app_config().get('nav_layout', 'tabs'))" in init_ui
    for label in ("Fragment Puzzle", "Joins Lab", "Settings"):
        assert re.search(r'self\.side_nav\.add_button\([^)]*tr\("%s"\)' % label, init_ui), label
    assert 'QKeySequence(f"Ctrl+{i + 1}")' in init_ui
    assert "self.corner_login_btn.link(self.nav_login_btn)" in init_ui
    assert "self.lang_btn.link(self.nav_lang_btn)" in init_ui
    login = src[src.index("    def _corner_login_clicked(self"):src.index("    def _fill_account_menu(self")]
    assert "_show_account_menu" in login and "_do_logout" not in login
