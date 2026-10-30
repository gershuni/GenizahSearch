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
names FL numbers. And (owner, 2026-10-30) Ctrl+O is Go to too, Ctrl+E the
search box too, and the cursor starts in the search box: at launch, and on
Ctrl+1 .. Ctrl+7 -- on Browse with nothing open, the shelfmark box; Ctrl+/ shows
every key, a list checked against the keys the window binds.
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
    _open_section = GenizahGUI._open_section
    _restore_session_then_lists_sync = GenizahGUI._restore_session_then_lists_sync
    _focus_on_launch = GenizahGUI._focus_on_launch
    _window_focus = GenizahGUI._window_focus
    _refocus_after_restore = GenizahGUI._refocus_after_restore
    _SECTION_HELP = GenizahGUI._SECTION_HELP
    _SECTION_FIND_FIELD = GenizahGUI._SECTION_FIND_FIELD

    def __init__(self):
        self.tabs = QTabWidget()
        for name in ("search_tab", "composition_tab", "browse_tab", "catalog_browse_tab",
                     "lists_tab", "community_tab", "my_library_tab"):
            setattr(self, name, QWidget())
            self.tabs.addTab(getattr(self, name), name)
        self.query_input = QLineEdit(self.search_tab)
        self.gap_input = QLineEdit(self.search_tab)       # another field of the Search tab
        self.comp_text_area = QPlainTextEdit(self.composition_tab)
        self.browse_find_input = QLineEdit(self.browse_tab)
        self.catalog_author_input = QLineEdit(self.catalog_browse_tab)
        self.lists_filter_input = QLineEdit(self.lists_tab)
        self.browse_sys_input = QLineEdit()
        self.browse_shelf_input = QLineEdit(self.browse_tab)
        self.browse_fl_input = QLineEdit()
        self.current_browse_sid = None                    # no manuscript open in Browse
        self.is_searching = False
        self.calls = []
        self.help_anchors = []
        self.tabs.show()
        QApplication.processEvents()

    def _set_active_tab(self, target):
        self.tabs.setCurrentWidget(target)

    def focusWidget(self):                               # noqa: N802 - Qt API
        return self.tabs.focusWidget()

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
    w.current_browse_sid = "990001234560205171"          # a manuscript is open
    w.browse_find_input.setText("abc")
    assert w._focus_section_field() is w.browse_find_input
    assert w.browse_find_input.selectedText() == "abc"
    w.tabs.setCurrentWidget(w.lists_tab)
    assert w._focus_section_field() is w.lists_filter_input
    w.tabs.setCurrentWidget(w.community_tab)
    assert w._focus_section_field() is None                # nothing to search there


def test_browse_with_nothing_open_takes_the_shelfmark():
    """Nothing to find in yet: the cursor goes where a shelfmark is typed."""
    w = _Window()
    w.browse_shelf_input.setText("T-S 12.123")
    assert w._open_section(2) is w.browse_shelf_input
    assert w.tabs.focusWidget() is w.browse_shelf_input
    assert w.browse_shelf_input.selectedText() == "T-S 12.123"
    w.current_browse_sid = "990001234560205171"
    assert w._focus_section_field() is w.browse_find_input


def test_a_section_key_opens_the_section_with_the_cursor_in_its_box():
    w = _Window()
    w.comp_text_area.setPlainText("abc")
    assert w._open_section(1) is w.comp_text_area
    assert w.tabs.currentWidget() is w.composition_tab
    assert w.tabs.focusWidget() is w.comp_text_area
    assert w.comp_text_area.textCursor().selectedText() == "abc"
    assert w._open_section(0) is w.query_input and w.tabs.focusWidget() is w.query_input
    assert w._open_section(5) is None                     # Community: the section, no box
    assert w.tabs.currentWidget() is w.community_tab


def _restore_into(w, words=None, fails=False, during=None, section=None):
    """Run the startup restore on ``w``: ``during`` stands for what the user did
    while the index loaded; the restore may put ``words`` in the box and bring
    the saved ``section`` to the front."""
    def restore():
        w.calls.append("restore")
        if words is not None:
            w.query_input.setText(words)
        if section is not None:
            w.tabs.setCurrentWidget(section)
        if fails:
            raise RuntimeError("restore failed")

    w._restore_session = restore
    w._start_restored_lists_sync = lambda: w.calls.append(("sync", w.focusWidget()))
    if during is not None:
        during()
    if fails:
        with pytest.raises(RuntimeError):
            w._restore_session_then_lists_sync()
    else:
        w._restore_session_then_lists_sync()


def test_the_cursor_is_in_the_box_as_the_window_first_shows():
    w = _Window()
    assert w._focus_on_launch() is w.query_input
    assert w.focusWidget() is w.query_input


@pytest.mark.parametrize("restore_fails", [False, True])
def test_after_the_restore_the_restored_words_are_selected_before_list_sync(restore_fails):
    """After the restore (its questions are modal) and before list sync, whose
    sync choice may be a dialog: closing it gives the cursor back to the box."""
    w = _Window()
    w._focus_on_launch()
    _restore_into(w, words="restored words", fails=restore_fails)
    assert w.calls == ["restore", ("sync", w.query_input)]
    assert w.query_input.selectedText() == "restored words"   # typing replaces it


def test_typing_while_the_index_loads_is_not_selected_away():
    """The restore runs once the index has loaded, seconds after the window
    shows: what the user typed meanwhile must not be selected (Codex review)."""
    from PyQt6.QtTest import QTest
    w = _Window()
    w._focus_on_launch()
    _restore_into(w, during=lambda: QTest.keyClicks(w.query_input, "abc"))
    assert w.query_input.text() == "abc" and w.query_input.selectedText() == ""
    assert w.focusWidget() is w.query_input


def test_a_pasted_prefix_still_counts_as_typing():
    """Pasting "= words" makes the prefix handling setText the words, which
    clears isModified(); the user's text must still not be selected (Codex review)."""
    from PyQt6.QtTest import QTest
    w = _Window()
    w._focus_on_launch()

    def paste_with_prefix():
        QTest.keyClicks(w.query_input, "= abc")
        w.query_input.setText("abc")                      # the prefix handling
        assert not w.query_input.isModified()

    _restore_into(w, during=paste_with_prefix)
    assert w.query_input.selectedText() == ""


def test_a_cursor_moved_while_the_index_loads_stays_put():
    w = _Window()
    w._focus_on_launch()
    _restore_into(w, words="restored", during=w.gap_input.setFocus)
    assert w.calls == ["restore", ("sync", w.gap_input)]
    assert w.query_input.selectedText() == ""


def test_a_restored_section_gets_the_cursor_in_its_box():
    """The session brings Composition Search to the front: its source text is
    the box, selected -- the restore moved the section, not the user (Codex review)."""
    w = _Window()
    w._focus_on_launch()
    w.comp_text_area.setPlainText("restored source")
    _restore_into(w, section=w.composition_tab)
    assert w.focusWidget() is w.comp_text_area
    assert w.comp_text_area.textCursor().selectedText() == "restored source"


def test_a_section_opened_while_the_index_loads_keeps_what_was_typed_there():
    """Search's box is hidden then (its tab is not in front), which is not the
    PGP Tags case: Composition's text must not be selected (Codex review)."""
    w = _Window()
    w._focus_on_launch()

    def type_in_composition():
        w.tabs.setCurrentWidget(w.composition_tab)
        w.comp_text_area.setFocus()
        w.comp_text_area.setPlainText("source text")

    _restore_into(w, during=type_in_composition)
    assert w.comp_text_area.textCursor().selectedText() == ""
    assert w.focusWidget() is w.comp_text_area


def _pgp_tags_mode(w):
    """The Search tab in PGP Tags mode: the query row hidden, the tag box shown."""
    from PyQt6.QtWidgets import QComboBox
    w.MODE_PGP_TAGS = 7
    w.mode_combo = QComboBox(w.search_tab)
    w.mode_combo.addItems([str(i) for i in range(8)])
    w.tag_search_combo = QComboBox(w.search_tab)
    w.tag_search_combo.setEditable(True)
    w.tag_search_combo.setCurrentText("Letter")
    w.mode_combo.setCurrentIndex(7)
    w.query_input.hide()
    w.mode_combo.show()                                   # made after the window showed
    w.tag_search_combo.show()


def test_pgp_tags_mode_takes_the_tag_box():
    w = _Window()
    _pgp_tags_mode(w)
    assert w._focus_section_field() is w.tag_search_combo
    assert w.focusWidget() is w.tag_search_combo
    assert w.tag_search_combo.lineEdit().selectedText() == "Letter"
    w.mode_combo.setCurrentIndex(0)
    assert w._focus_section_field() is w.query_input


def test_a_restored_pgp_tags_mode_gives_the_cursor_to_the_tag_box():
    w = _Window()
    w._focus_on_launch()
    _restore_into(w, during=lambda: _pgp_tags_mode(w))   # the restore hid the query row
    assert w.focusWidget() is w.tag_search_combo


def _method_source(src, name):
    start = src.index(f"    def {name}(")
    return src[start:src.index("\n    def ", start + 1)]


def _init_ui_source():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    return src[src.index("    def init_ui(self):"):src.index("    def _update_corner_login_state(self):")]


def test_the_shortcuts_card_names_every_bound_key_and_only_those():
    import re
    from desktop.shortcuts_dialog import SHORTCUTS
    init_ui = _init_ui_source()
    bound = set(re.findall(r'\("((?:Ctrl\+|F\d)[^"]*)", (?:self\.|-?\d)', init_ui))
    assert {"Ctrl+G", "F1", "Ctrl+=", "Ctrl+/"} <= bound          # the pattern finds them
    bound.discard("Ctrl++")                                       # Ctrl+= by another name
    shown = {key for keys, _what in SHORTCUTS
             for key in keys.replace(" … ", " / ").split(" / ")}
    assert bound - shown == set()
    # Ctrl+1 .. Ctrl+7 are bound in a loop; the others are one widget's own keys,
    # bound where that widget is: Down in the search box, Space in the results,
    # Esc on Composition Search's text.
    assert 'QKeySequence(f"Ctrl+{i + 1}")' in init_ui
    assert shown - bound == {"Ctrl+1", "Ctrl+7", "Down", "Space", "Shift+Space", "Esc"}
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    events = _method_source(src, "eventFilter")
    assert "event.key() == Qt.Key.Key_Down" in events and "self._show_search_history_menu()" in events
    assert "event.key() == Qt.Key.Key_Space" in events and "source is self.results_table" in events
    assert 'QShortcut(QKeySequence("Escape"), self.comp_text_area)' in src


def test_the_shortcuts_card_shows_each_key_beside_what_it_does():
    from PyQt6.QtWidgets import QFormLayout
    from desktop.shortcuts_dialog import SHORTCUTS, ShortcutsDialog
    from desktop.widgets.side_nav import ltr, windows_virtual_key
    from genizah_core import tr
    dlg = ShortcutsDialog(None)
    form = dlg.findChild(QFormLayout)
    assert form.rowCount() == len(SHORTCUTS)
    rows = [(form.itemAt(r, QFormLayout.ItemRole.LabelRole).widget().text(),
             form.itemAt(r, QFormLayout.ItemRole.FieldRole).widget().text())
            for r in range(form.rowCount())]
    assert rows == [(ltr(keys), tr(what)) for keys, what in SHORTCUTS]
    assert rows[0][0] == "‎Ctrl+1 … Ctrl+7‎"
    assert windows_virtual_key("/") == 0xBF                       # Ctrl+/ on a Hebrew layout
    dlg.deleteLater()


class _SearchTab:
    """The Search tab's parts the end of a search looks at."""

    _focus_results_after_search = GenizahGUI._focus_results_after_search
    _first_shown_result_row = GenizahGUI._first_shown_result_row
    _window_focus = GenizahGUI._window_focus
    _all_terms_after_completion = GenizahGUI._all_terms_after_completion
    _run_deferred_after_filter = GenizahGUI._run_deferred_after_filter
    COL_CHECKBOX = 0
    is_searching = False

    def __init__(self):
        from PyQt6.QtWidgets import QComboBox, QHBoxLayout, QPushButton, QTableWidget, QVBoxLayout
        self.tabs = QTabWidget()
        self.search_tab, self.browse_tab = QWidget(), QWidget()
        self.tabs.addTab(self.search_tab, "search")
        self.tabs.addTab(self.browse_tab, "browse")
        page = QVBoxLayout(self.search_tab)
        self.search_row1_container = QWidget()
        row1 = QHBoxLayout(self.search_row1_container)
        self.query_input, self.btn_search = QLineEdit(), QPushButton("Search")
        self.mode_combo = QComboBox()                     # in the query row too
        row1.addWidget(self.query_input)
        row1.addWidget(self.mode_combo)
        row1.addWidget(self.btn_search)
        self.gap_input = QLineEdit()                      # a field of another row
        self.results_table = QTableWidget(3, 4)           # column 0: the check boxes
        for w in (self.search_row1_container, self.gap_input, self.results_table):
            page.addWidget(w)
        self.browse_field = QLineEdit(self.browse_tab)
        self.tabs.show()
        QApplication.processEvents()

    def focusWidget(self):                               # noqa: N802 - Qt API
        return self.tabs.focusWidget()


def _start_search_from(w, widget):
    widget.setFocus()
    w._focus_at_search_start = w.focusWidget()           # as start_search keeps it


@pytest.mark.parametrize("start", ["query_input", "btn_search", "gap_input"])
def test_a_search_ends_in_the_results_on_a_cell_where_space_pages(start):
    """From the box, the button, or wherever Go to left the cursor; the current
    cell is not the check box, where Space ticks instead of paging (Codex review)."""
    from genizah_app import space_scroll_action
    w = _SearchTab()
    _start_search_from(w, getattr(w, start))
    assert w._focus_results_after_search() is True
    assert w.focusWidget() is w.results_table
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (0, 1)
    assert space_scroll_action(w.results_table.currentColumn(), w.COL_CHECKBOX, False) == 'page_down'
    assert w.results_table.selectedIndexes() == []        # nothing selected, nothing opened


def test_the_first_shown_cell_takes_it():
    w = _SearchTab()
    w.results_table.setRowHidden(0, True)                 # a filter hides the first row
    w.results_table.setColumnHidden(1, True)
    _start_search_from(w, w.query_input)
    assert w._focus_results_after_search() is True
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (1, 2)


def test_a_cell_the_user_chose_in_the_results_stays():
    w = _SearchTab()
    _start_search_from(w, w.query_input)
    w.results_table.setCurrentCell(2, 3)
    w.results_table.setFocus()                            # clicked a row while it ran
    assert w._focus_results_after_search() is True
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (2, 3)


def test_the_end_of_a_search_leaves_a_cursor_the_user_moved():
    w = _SearchTab()
    _start_search_from(w, w.query_input)
    w.mode_combo.setFocus()                               # moved, even within the query row
    assert w._focus_results_after_search() is False
    assert w.focusWidget() is w.mode_combo
    w.gap_input.setFocus()                                # or to another field
    assert w._focus_results_after_search() is False
    w.query_input.setFocus()
    w.tabs.setCurrentWidget(w.browse_tab)                 # or to another section
    w.browse_field.setFocus()
    assert w._focus_results_after_search() is False
    assert w.focusWidget() is w.browse_field
    w.browse_field.clearFocus()                           # another section, nothing focused
    assert w.focusWidget() is None
    assert w._focus_results_after_search() is False
    assert w.tabs.currentWidget() is w.browse_tab and w.focusWidget() is None


@pytest.mark.parametrize("hide", ["no_rows", "all_hidden"])
def test_no_shown_row_leaves_the_cursor_in_the_box(hide):
    w = _SearchTab()
    if hide == "no_rows":
        w.results_table.setRowCount(0)
    else:
        for r in range(w.results_table.rowCount()):
            w.results_table.setRowHidden(r, True)
    _start_search_from(w, w.query_input)
    assert w._focus_results_after_search() is False
    assert w.focusWidget() is w.query_input


def _hide_every_row(table):
    for r in range(table.rowCount()):
        table.setRowHidden(r, True)


@pytest.mark.parametrize("rerender", [lambda t: t.setRowCount(0), _hide_every_row],
                         ids=["no_rows", "excluded_rows_hidden"])
def test_an_all_terms_filter_that_shows_no_row_gives_the_box_back(rerender):
    w = _SearchTab()
    _start_search_from(w, w.query_input)
    w._focus_results_after_search()
    w._apply_all_terms_filter_and_rerender = lambda: rerender(w.results_table)
    w._focus_section_field = lambda: w.query_input.setFocus()
    w._all_terms_after_completion(None)
    assert w.focusWidget() is w.query_input


def _queue_search_from(w, widget):
    """A search asked for from ``widget`` that waits for the filter lookup (as
    start_search and a picked history entry queue it)."""
    widget.setFocus()
    w._deferred_search_focus = w.focusWidget()
    w._rerun_search_after_filter = True


def test_a_search_that_waited_for_the_filters_keeps_where_it_was_asked_for():
    """The user pressed Search in the box, then went to another field while the
    filter lookup ran: the deferred start must not take that field as its
    starting point (Codex review)."""
    w = _SearchTab()
    w.query_input.setText("words")
    _queue_search_from(w, w.query_input)
    w.gap_input.setFocus()                                # meanwhile
    w.start_search = lambda: setattr(w, '_focus_at_search_start', w.focusWidget())
    w._run_deferred_after_filter()
    assert w._focus_at_search_start is w.query_input
    assert w._focus_results_after_search() is False       # the field keeps the cursor
    assert w.focusWidget() is w.gap_input


def test_a_queued_search_does_not_inherit_the_running_searchs_place():
    """Search started from the button; while it ran, a history entry that waits
    for the filters was picked from the box. The first search ends; the queued
    one starts later and must count from the box (Codex review, round 5)."""
    w = _SearchTab()
    w.query_input.setText("words")
    _start_search_from(w, w.btn_search)                   # the running search's place
    _queue_search_from(w, w.query_input)                  # the history pick's
    assert w._focus_at_search_start is w.btn_search       # the running one keeps its own
    w.start_search = lambda: setattr(w, '_focus_at_search_start', w.focusWidget())
    w._run_deferred_after_filter()
    assert w._focus_at_search_start is w.query_input
    assert w._focus_results_after_search() is True
    assert w.focusWidget() is w.results_table


def test_search_end_and_corner_button_wiring():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    done = src[src.index("    def on_search_finished(self, results):"):src.index("    def _focus_results_after_search(self):")]
    # Results only (no results returns earlier, the cursor stays in the box), not on restore.
    assert done.index("        if not results:") < done.index("self._focus_results_after_search()")
    assert "        if not restoring:\n            self._focus_results_after_search()" in done
    # Where the cursor was when a search started: a text search, a tag search.
    keep = "self._focus_at_search_start = self._window_focus()"
    start = _method_source(src, "start_search").replace("\r\n", "\n")
    # Read before the filter lookup can make it wait; a waiting search keeps it
    # as the queued one's, a starting one as its own (Codex review, rounds 2-5).
    assert start.index("asked_from = self._window_focus()") < start.index(
        "self._pre_search_filters_blocked(rerun='search')")
    assert ("if self._pre_search_filters_blocked(rerun='search'):\n"
            "            self._deferred_search_focus = asked_from\n"
            "            return\n"
            "        self._focus_at_search_start = asked_from\n") in start
    assert keep in _method_source(src, "_execute_tag_search")
    # A history entry: the shown snapshot's place, or the queued search's -- never
    # over a running search's own.
    history = _method_source(src, "_restore_regular_search_from_state")
    assert keep not in history
    assert history.index("picked_from = self._window_focus()") < history.index("if results:")
    snapshot = history[history.index("if results:"):history.index("if filter_pending:")]
    assert snapshot.index("self._focus_at_search_start = picked_from") < snapshot.index(
        "self.on_search_finished(results)")
    queued = history[history.index("if filter_pending:"):history.index("self._rerun_search_after_filter = True")]
    assert "self._deferred_search_focus = picked_from" in queued
    assert "self._focus_at_search_start" not in queued
    # The first shown row may come with the rest of the first page.
    fill = _method_source(src, "_fill_first_results_page")
    assert (fill.index("self.load_next_batch(batch_size=missing)")
            < fill.index("        self._focus_results_after_search()\n"))
    tag_done = _method_source(src, "_on_tag_search_results")
    assert tag_done.rstrip().endswith("self._focus_results_after_search()")
    # The cursor in the box from the start, before the index loads.
    assert "        self.init_ui()\n        self._focus_on_launch()\n" in src
    init_ui = _init_ui_source()
    assert "self.corner_shortcuts_btn.clicked.connect(self.open_shortcuts)" in init_ui
    assert ("corner_widget.add(self.corner_shortcuts_btn, 3)" in init_ui
            and init_ui.index("corner_widget.add(self.corner_shortcuts_btn, 3)")
            < init_ui.index("('corner_text_smaller_btn'"))     # beside A-


def test_the_dialog_returns_what_was_typed_and_leaves_browses_completer_alone():
    """A QCompleter serves one widget at a time. The box borrowed Browse's and
    left it attached to nothing once it closed: typing in Browse's shelfmark
    field then crashed (Codex review of PR #448). It now has its own."""
    from PyQt6.QtCore import Qt
    from PyQt6.QtGui import QStandardItem, QStandardItemModel
    from PyQt6.QtTest import QTest
    from desktop.go_to_dialog import GoToDialog
    from desktop.widgets import ShelfmarkCompleter
    model = QStandardItemModel()                       # as the window builds it: the key in UserRole
    for shelfmark in ("T-S 12.123", "T-S 13.15"):
        item = QStandardItem(shelfmark)
        item.setData(ShelfmarkCompleter.normalize(shelfmark), Qt.ItemDataRole.UserRole)
        model.appendRow(item)
    shared = ShelfmarkCompleter(model, valid_keys={"ts12.123"})
    shared.setCompletionRole(Qt.ItemDataRole.UserRole)
    shared.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
    browse_field = QLineEdit()
    browse_field.setCompleter(shared)
    browse_field.show()
    QApplication.processEvents()
    dlg = GoToDialog(None, completer=shared)
    own = dlg.input.completer()
    assert isinstance(own, ShelfmarkCompleter) and own is not shared
    assert own.model() is model and own.valid_keys == {"ts12.123"}
    assert own.caseSensitivity() == Qt.CaseSensitivity.CaseInsensitive
    assert own.completionRole() == Qt.ItemDataRole.UserRole
    dlg.input.setText("  T-S 12.123 ")
    assert dlg.text() == "T-S 12.123"
    dlg.close()
    dlg.deleteLater()
    QApplication.processEvents()
    assert browse_field.completer() is shared and shared.widget() is browse_field
    browse_field.setFocus()
    QTest.keyClicks(browse_field, "T-S 1")             # crashed before
    QApplication.processEvents()
    assert shared.completionCount() == 2
    browse_field.close()


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
    # Ctrl+8 / Ctrl+9 (owner, 2026-10-25), and the keys in the menus.
    assert '("Ctrl+8", self._open_puzzle_window)' in init_ui and '("Ctrl+9", self.open_join_workbench)' in init_ui
    assert 'tr("Fragment Puzzle"), shortcut="Ctrl+8")' in init_ui
    assert 'tr("Joins Lab"), shortcut="Ctrl+9")' in init_ui
    # Ctrl+O is Go to as well, Ctrl+E the search box as well, Ctrl+/ the list of
    # keys (owner, 2026-10-30); Ctrl+1 .. Ctrl+7 move the cursor too.
    assert '("Ctrl+O", self.open_go_to)' in init_ui
    assert '("Ctrl+E", self._focus_section_field)' in init_ui
    assert '("Ctrl+/", self.open_shortcuts)' in init_ui
    assert 'tr("Keyboard shortcuts"), footer=True, shortcut="Ctrl+/")' in init_ui
    assert "self.nav_shortcuts_btn.clicked.connect(self.open_shortcuts)" in init_ui
    assert "lambda idx=i: self._open_section(idx)" in init_ui
    # Ctrl+G / Ctrl+O / Ctrl+F / Ctrl+E / Ctrl+, / Ctrl+/ also by the physical key on Windows.
    assert "LayoutProofShortcuts(self, {" in init_ui and "('G', Qt.KeyboardModifier.ControlModifier): self.open_go_to" in init_ui
    for key, slot in (("O", "self.open_go_to"), ("E", "self._focus_section_field"),
                      ("/", "self.open_shortcuts")):
        assert f"('{key}', Qt.KeyboardModifier.ControlModifier): {slot}" in init_ui
    # A- / A+ beside the tabs and in the side menu.
    assert "self._step_text_size(s)" in init_ui and 'tr("Larger text"), footer=True, shortcut="Ctrl+="' in init_ui
    go = src[src.index("    def open_go_to(self):"):src.index("    def _go_to_recent_items(")]
    assert "recent=self._go_to_recent_items()" in go and "self._open_recent_item(value)" in go
