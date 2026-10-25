"""The desktop Go to box (Ctrl+G) and its neighbours, F1 and Ctrl+F (owner, 2026-10-24).

Pinned here: what a typed line is (shared/quick_open.py, the website homepage's
rule plus My Library ids and FL numbers); that the window opens a system number,
FL number or shelfmark in Browse with only that field filled -- browse_load reads
all three, so a leftover number would compete -- or runs a search for Hebrew or
Arabic words, without starting a second search over a running one; that F1 opens
Help at the section in front; and that Ctrl+F puts the cursor in the section's
own search or find field. And (owner, 2026-10-25) the recent manuscripts under
the box: one row per manuscript, narrowed by typing, picked with Down, and
opened the way Browse's own Recently Viewed list opens them; the hint no longer
names FL numbers.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QLineEdit, QPlainTextEdit, QTabWidget, QWidget

from genizah_app import GenizahGUI
from shared.quick_open import has_hebrew_or_arabic_letter, quick_open_target

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.mark.parametrize("text, expected", [
    ("990001234560205171", ("sys_id", "990001234560205171")),
    ("  990001234560205171 ", ("sys_id", "990001234560205171")),
    ("971234567812345678", ("sys_id", "971234567812345678")),   # a My Library document
    ("FL12345678", ("fl", "FL12345678")),
    ("fl 12345678", ("fl", "FL12345678")),
    ("T-S 12.123", ("shelfmark", "T-S 12.123")),
    ("MS Heb c 57", ("shelfmark", "MS Heb c 57")),
    ("12345", ("shelfmark", "12345")),          # not a system number: Browse decides
    ("אל תיראו", ("search", "אל תיראו")),
    ("T-S אל", ("search", "T-S אל")),           # a Hebrew letter makes it words
    ("قال", ("search", "قال")),
    ("٩٩٠٠٠١٢٣٤٥٦٠٢٠٥١٧١", ("shelfmark", "٩٩٠٠٠١٢٣٤٥٦٠٢٠٥١٧١")),  # Arabic-Indic digits
    ("", None),
    ("   ", None),
    (None, None),
])
def test_what_a_line_is(text, expected):
    assert quick_open_target(text) == expected


def test_the_letter_rule_is_the_websites():
    import web.home_query as home
    assert home.has_hebrew_or_arabic_letter is has_hebrew_or_arabic_letter
    assert not has_hebrew_or_arabic_letter("ְ֑")      # niqqud and cantillation only


class _Window:
    """The parts of GenizahGUI Go to, F1 and Ctrl+F use, with the real methods."""

    go_to = GenizahGUI.go_to
    open_help_for_current_section = GenizahGUI.open_help_for_current_section
    _focus_section_field = GenizahGUI._focus_section_field
    _SECTION_HELP = GenizahGUI._SECTION_HELP
    _SECTION_FIND_FIELD = GenizahGUI._SECTION_FIND_FIELD

    def __init__(self):
        self.tabs = QTabWidget()
        for name in ("search_tab", "composition_tab", "browse_tab", "catalog_browse_tab",
                     "lists_tab", "community_tab", "my_library_tab"):
            setattr(self, name, QWidget())
            self.tabs.addTab(getattr(self, name), name)
        self.query_input = QLineEdit(self.search_tab)
        self.comp_text_area = QPlainTextEdit(self.composition_tab)
        self.browse_find_input = QLineEdit(self.browse_tab)
        self.catalog_author_input = QLineEdit(self.catalog_browse_tab)
        self.lists_filter_input = QLineEdit(self.lists_tab)
        self.browse_sys_input = QLineEdit()
        self.browse_shelf_input = QLineEdit()
        self.browse_fl_input = QLineEdit()
        self.is_searching = False
        self.calls = []
        self.help_anchors = []
        self.tabs.show()
        QApplication.processEvents()

    def _set_active_tab(self, target):
        self.tabs.setCurrentWidget(target)

    def _set_last_browse_field(self, name):
        self.calls.append(("last", name))

    def browse_load(self):
        self.calls.append(("browse", self.browse_sys_input.text(),
                           self.browse_shelf_input.text(), self.browse_fl_input.text()))

    def toggle_search(self):
        self.calls.append(("search", self.query_input.text()))

    def open_help_center(self, anchor=None):
        self.help_anchors.append(anchor)


def test_go_to_opens_a_system_number_with_only_that_field():
    w = _Window()
    w.browse_shelf_input.setText("T-S old")              # left from an earlier look
    w.browse_fl_input.setText("FL1")
    assert w.go_to("990001234560205171") == "sys_id"
    assert w.calls == [("last", "sys"), ("browse", "990001234560205171", "", "")]
    assert w.tabs.currentWidget() is w.browse_tab


def test_go_to_opens_a_shelfmark_and_an_fl_number():
    w = _Window()
    w.browse_sys_input.setText("990000000000000000")
    w.go_to("T-S 12.123")
    w.go_to("fl 42")
    assert w.calls == [("last", "shelf"), ("browse", "", "T-S 12.123", ""),
                       ("last", "fl"), ("browse", "", "", "FL42")]


def test_go_to_searches_words_but_not_over_a_running_search():
    w = _Window()
    assert w.go_to("אל תיראו") == "search"
    assert w.calls == [("search", "אל תיראו")] and w.tabs.currentWidget() is w.search_tab
    w.is_searching = True
    w.go_to("ישועת")
    assert w.calls == [("search", "אל תיראו")]           # the words wait in the box
    assert w.query_input.text() == "ישועת"
    assert w.go_to("  ") is None


def test_f1_opens_help_at_the_section_in_front():
    w = _Window()
    for tab, anchor in (("composition_tab", "composition"), ("catalog_browse_tab", "catalog-browse"),
                        ("my_library_tab", "my-library"), ("search_tab", "search")):
        w.tabs.setCurrentWidget(getattr(w, tab))
        w.open_help_for_current_section()
        assert w.help_anchors[-1] == anchor


def test_ctrl_f_focuses_the_sections_own_field():
    w = _Window()
    w.tabs.setCurrentWidget(w.browse_tab)
    w.browse_find_input.setText("abc")
    assert w._focus_section_field() is w.browse_find_input
    assert w.browse_find_input.selectedText() == "abc"
    w.tabs.setCurrentWidget(w.lists_tab)
    assert w._focus_section_field() is w.lists_filter_input
    w.tabs.setCurrentWidget(w.community_tab)
    assert w._focus_section_field() is None                # nothing to search there


def test_the_dialog_returns_what_was_typed_and_gives_the_completer_back():
    from PyQt6.QtWidgets import QCompleter
    from desktop.go_to_dialog import GoToDialog
    shared = QCompleter(["T-S 12.123"])
    browse_field = QLineEdit()
    browse_field.setCompleter(shared)
    dlg = GoToDialog(None, completer=shared)
    dlg.input.setText("  T-S 12.123 ")
    assert dlg.text() == "T-S 12.123"
    dlg.detach_completer()
    assert dlg.input.completer() is None and browse_field.completer() is shared
    dlg.deleteLater()


def test_the_hint_does_not_offer_fl_numbers():
    from desktop.go_to_dialog import GoToDialog
    from PyQt6.QtWidgets import QLabel
    dlg = GoToDialog(None)
    hints = " ".join(lbl.text() for lbl in dlg.findChildren(QLabel))
    assert "FL" not in hints
    assert not dlg.recent_list.isVisibleTo(dlg)          # nothing opened yet: no list
    assert dlg.choice() == ("text", "")
    dlg.deleteLater()


def _recent_dialog():
    from desktop.go_to_dialog import GoToDialog
    return GoToDialog(None, recent=[("T-S NS 13.15 \u2014 Piyyut", "id1"),
                                    ("ENA 2345.1", "id2"),
                                    ("T-S 12.123", "id3")])


def _press(widget, key):
    from PyQt6.QtCore import Qt
    from PyQt6.QtGui import QKeyEvent
    from PyQt6.QtCore import QEvent
    QApplication.sendEvent(widget, QKeyEvent(QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier))


def test_down_picks_a_recent_manuscript_and_typing_narrows_the_list():
    from PyQt6.QtCore import Qt
    dlg = _recent_dialog()
    assert dlg.recent_list.count() == 3 and dlg.recent_list.isVisibleTo(dlg)
    _press(dlg.input, Qt.Key.Key_Down)
    assert dlg.choice() == ("recent", "id1")
    _press(dlg.input, Qt.Key.Key_Down)
    assert dlg.choice() == ("recent", "id2")
    _press(dlg.input, Qt.Key.Key_Up)
    _press(dlg.input, Qt.Key.Key_Up)                    # above the first: back to the box
    assert dlg.choice() == ("text", "")
    dlg.input.setText("ts 12")                          # letters and digits only
    shown = [dlg.recent_list.item(r).text() for r in range(3)
             if not dlg.recent_list.item(r).isHidden()]
    assert shown == ["T-S 12.123"]
    assert dlg.choice() == ("text", "ts 12")            # typed, nothing picked
    _press(dlg.input, Qt.Key.Key_Down)
    assert dlg.choice() == ("recent", "id3")
    dlg.input.setText("ts 1")                           # typing again drops the pick
    assert dlg.choice() == ("text", "ts 1")
    dlg.deleteLater()


class _Lists:
    def __init__(self, items):
        self.items = items

    def get_items_in_list(self, list_id):
        assert list_id == "recent"
        return [dict(i) for i in self.items]

    def get_item(self, item_id):
        return next((dict(i) for i in self.items if i["item_id"] == item_id), None)


class _Meta:
    def get_meta_for_id(self, sid):
        return {"990001": ("T-S 12.123", "Siddur " + "x" * 80),
                "990002": ("Unknown", "")}.get(sid, ("", ""))


def test_recent_items_are_one_row_per_manuscript():
    w = _Window()
    w._go_to_recent_items = GenizahGUI._go_to_recent_items.__get__(w)
    w.lists_mgr = _Lists([
        {"item_id": "990001_3", "sys_id": "990001", "img": "3"},
        {"item_id": "990002", "sys_id": "990002"},
        {"item_id": "990001_1", "sys_id": "990001", "img": "1"},   # the same manuscript again
        {"item_id": "971234", "sys_id": "971234", "shelfmark_override": "My letter"},
    ])
    w.meta_mgr = _Meta()
    rows = w._go_to_recent_items()
    assert [payload for _label, payload in rows] == ["990001_3", "990002", "971234"]
    assert rows[0][0].startswith("T-S 12.123 \u2014 Siddur") and rows[0][0].endswith("\u2026")
    assert rows[1][0] == "990002"                       # no shelfmark known: the number
    assert rows[2][0] == "My letter"
    assert w._go_to_recent_items(limit=1) == rows[:1]
    w.lists_mgr = None
    assert w._go_to_recent_items() == []


def test_a_recent_manuscript_opens_as_browse_recently_viewed_does():
    w = _Window()
    w._open_recent_item = GenizahGUI._open_recent_item.__get__(w)
    w._browse_open_list_entry = GenizahGUI._browse_open_list_entry.__get__(w)
    w._normalize_fl_id = lambda v: v
    w.lists_mgr = _Lists([{"item_id": "a", "sys_id": "990001", "fl_id": "FL77"},
                          {"item_id": "b", "sys_id": "990002"}])
    w.browse_sys_input.setText("leftover")
    assert w._open_recent_item("a")
    assert w.tabs.currentWidget() is w.browse_tab
    assert w.calls == [("last", "fl"), ("browse", "", "", "77")]
    assert w._open_recent_item("b")
    assert w.calls[-2:] == [("last", "sys"), ("browse", "990002", "", "")]
    assert not w._open_recent_item("gone")


def test_main_window_wiring():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    init_ui = src[src.index("    def init_ui(self):"):src.index("    def _update_corner_login_state(self):")]
    for keys, slot in (('"Ctrl+G"', "self.open_go_to"), ('"F1"', "self.open_help_for_current_section"),
                       ('"Ctrl+,"', "self._open_settings_dialog"), ('"Ctrl+F"', "self._focus_section_field")):
        assert f"({keys}, {slot})" in init_ui
    assert 'tr("Go to..."), shortcut="Ctrl+G")' in init_ui
    assert 'tr("Help"), footer=True, shortcut="F1")' in init_ui
    go = src[src.index("    def open_go_to(self):"):src.index("    def _go_to_recent_items(")]
    assert "recent=self._go_to_recent_items()" in go and "self._open_recent_item(value)" in go
