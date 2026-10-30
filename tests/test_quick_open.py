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
    _land_in_results = GenizahGUI._land_in_results
    _land_after_click = GenizahGUI._land_after_click
    _focus_section_results = GenizahGUI._focus_section_results
    _focus_results_view = GenizahGUI._focus_results_view
    _SECTION_RESULTS = GenizahGUI._SECTION_RESULTS
    COL_CHECKBOX = 0
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
        from PyQt6.QtWidgets import QTableWidget, QTreeWidget
        self.results_table = QTableWidget(0, 3, self.search_tab)      # no results yet
        self.comp_tree = QTreeWidget(self.composition_tab)
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


def _fill_results(w, rows=3):
    from PyQt6.QtWidgets import QTableWidgetItem
    w.results_table.setRowCount(rows)
    for r in range(rows):
        w.results_table.setItem(r, 1, QTableWidgetItem(f"row{r}"))


def test_back_to_search_with_results_lands_in_them():
    """Owner, 2026-10-30: going back to a search is going back to its results."""
    w = _Window()
    _fill_results(w)
    w.tabs.setCurrentWidget(w.browse_tab)
    assert w._open_section(0) is w.results_table
    assert w.focusWidget() is w.results_table
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (0, 1)   # not the box column
    assert w.results_table.selectedIndexes() == []
    w.results_table.setCurrentCell(2, 2)                   # the row the user was on stays
    w._open_section(2)
    w._open_section(0)
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (2, 2)


def test_composition_with_results_lands_in_them_and_without_in_its_text():
    from PyQt6.QtWidgets import QTreeWidgetItem
    w = _Window()
    assert w._open_section(1) is w.comp_text_area           # no results: the source text
    w.comp_tree.addTopLevelItems([QTreeWidgetItem(["ms0"]), QTreeWidgetItem(["ms1"])])
    w.comp_tree.topLevelItem(0).setHidden(True)              # a filter hides the first
    w._open_section(2)
    assert w._open_section(1) is w.comp_tree
    assert w.focusWidget() is w.comp_tree and w.comp_tree.currentItem().text(0) == "ms1"


def _comp_groups(w):
    """Composition Search's tree as it is drawn: group headings, results under them."""
    from PyQt6.QtCore import Qt
    from PyQt6.QtWidgets import QTreeWidgetItem
    main = QTreeWidgetItem(w.comp_tree, ["Main Results (2)"])
    for name in ("ms0", "ms1"):
        QTreeWidgetItem(main, [name]).setData(0, Qt.ItemDataRole.UserRole, {"sys_id": name})
    filt = QTreeWidgetItem(w.comp_tree, ["Filtered (1)"])
    QTreeWidgetItem(filt, ["ms2"]).setData(0, Qt.ItemDataRole.UserRole, {"sys_id": "ms2"})
    main.setExpanded(True)
    return main, filt


def test_in_composition_the_cursor_lands_on_a_result_that_shows_not_a_heading():
    """Codex review: Enter cannot open a group's heading, and a result under a
    collapsed group does not show, though Qt keeps it current."""
    w = _Window()
    main, filt = _comp_groups(w)
    w.tabs.setCurrentWidget(w.composition_tab)
    assert w._focus_section_results() is w.comp_tree
    assert w.comp_tree.currentItem().text(0) == "ms0"        # the first result, not its heading
    w.comp_tree.setCurrentItem(main.child(1))
    main.setExpanded(False)                                   # its group collapsed by the user
    assert w.comp_tree.currentItem() is main.child(1)         # Qt leaves it current, unseen
    filt.setExpanded(True)
    w._focus_section_results()
    assert w.comp_tree.currentItem().text(0) == "ms2"         # the first result that shows
    filt.setExpanded(False)
    w._focus_section_results()                                # no result shows: the first heading
    assert w.comp_tree.currentItem() is main
    main.setExpanded(True)
    w.comp_tree.setCurrentItem(filt)                          # a row the user chose that shows stays
    w._focus_section_results()
    assert w.comp_tree.currentItem() is filt


def test_a_click_on_search_or_composition_lands_as_its_key_does():
    """Codex review: a click on their tab or side-menu entry with no results
    puts the cursor in the search box, as Ctrl+1 / Ctrl+2 do, not where Qt
    left it; a click on another section leaves the cursor to Qt."""
    w = _Window()
    w.gap_input.setFocus()
    assert w._land_after_click() is w.query_input and w.focusWidget() is w.query_input
    _fill_results(w)
    assert w._land_after_click() is w.results_table and w.focusWidget() is w.results_table
    w.tabs.setCurrentWidget(w.composition_tab)
    assert w._land_after_click() is w.comp_text_area
    w.tabs.setCurrentWidget(w.catalog_browse_tab)
    w.tabs.tabBar().setFocus()                                # not its author box
    assert w._land_after_click() is None and w.focusWidget() is w.tabs.tabBar()


def test_ctrl_r_goes_to_the_sections_results_and_does_nothing_without_them():
    w = _Window()
    w.query_input.setFocus()
    assert w._focus_section_results() is None               # Search with no results
    assert w.focusWidget() is w.query_input
    _fill_results(w, 2)
    for r in range(2):
        w.results_table.setRowHidden(r, True)
    assert w._focus_section_results() is None               # every row hidden: none shown
    w.results_table.setRowHidden(1, False)
    assert w._focus_section_results() is w.results_table
    assert w.results_table.currentRow() == 1
    w.tabs.setCurrentWidget(w.browse_tab)
    w.browse_shelf_input.setFocus()
    assert w._focus_section_results() is None               # Browse has no results list
    assert w.focusWidget() is w.browse_shelf_input


def test_clicks_on_a_section_land_in_results_and_ctrl_r_is_bound():
    src = _app_source()
    init_ui = _init_ui_source()
    assert '("Ctrl+R", self._focus_section_results)' in init_ui
    assert "('R', Qt.KeyboardModifier.ControlModifier): self._focus_section_results" in init_ui
    assert "self.tabs.tabBarClicked.connect(" in init_ui and "QTimer.singleShot(0, self._land_after_click)" in init_ui
    assert "lambda i: None if i == self.tabs.currentIndex()" in init_ui   # the tab in front: nothing
    assert "self._land_after_click()))" in init_ui                       # the side menu's sections
    opened = _method_source(src, "_open_section")
    assert "landed = self._land_in_results()" in opened and "self._focus_section_field()" in opened


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


def _app_source():
    from pathlib import Path
    return (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")


def _card_keys(rows):
    return {key for keys, _what in rows for key in keys.replace(" … ", " / ").split(" / ")}


def test_the_card_names_every_window_key_and_only_those():
    import re
    from desktop.shortcuts_dialog import shortcut_sections
    init_ui = _init_ui_source()
    bound = set(re.findall(r'\("((?:Ctrl\+|F\d)[^"]*)", (?:self\.|-?\d)', init_ui))
    assert {"Ctrl+G", "F1", "Ctrl+=", "Ctrl+/"} <= bound          # the pattern finds them
    bound.discard("Ctrl++")                                       # Ctrl+= by another name
    window = _card_keys(dict(shortcut_sections(False))["In the whole window"])
    assert bound - window == set()
    # Ctrl+1 .. Ctrl+7 are bound in a loop; Down in the search box, Esc on the
    # Search tab and on Composition Search's text.
    assert 'QKeySequence(f"Ctrl+{i + 1}")' in init_ui
    assert window - bound == {"Ctrl+1", "Ctrl+7", "Down", "Esc"}
    src = _app_source()
    events = _method_source(src, "eventFilter")
    assert "event.key() == Qt.Key.Key_Down" in events and "self._show_search_history_menu()" in events
    assert 'QShortcut(QKeySequence("Escape"), self.comp_text_area)' in src
    search_tab = _method_source(src, "create_search_tab")
    assert 'stop_key = QShortcut(QKeySequence("Escape"), panel)' in search_tab
    assert "self.stop_search() if self.is_searching else None" in search_tab


@pytest.mark.parametrize("hebrew", [False, True])
def test_the_card_names_the_list_browse_and_viewer_keys_as_bound(hebrew):
    """The keys of the other sections, per language: the arrows mirrored in
    Hebrew (owner, 2026-10-30). What each key does is pinned below."""
    from desktop.shortcuts_dialog import arrow_keys, shortcut_sections
    nxt, prev = arrow_keys(hebrew)
    assert (nxt, prev) == (("Left", "Right") if hebrew else ("Right", "Left"))
    sections = dict(shortcut_sections(hebrew))
    assert list(sections) == ["In the whole window", "In a list of results", "Composition Search",
                              "Browse by Shelfmark", "Manuscript Viewer"]
    assert _card_keys(sections["In a list of results"]) == {"Space", "Ctrl+A", "Enter", "Ctrl+Enter", "Ctrl+B"}
    assert _card_keys(sections["Composition Search"]) == {"Ctrl+Enter"}
    moves = {"Ctrl+PgDown", "Ctrl+PgUp", f"Ctrl+Shift+{nxt}", f"Ctrl+Shift+{prev}"}
    assert _card_keys(sections["Browse by Shelfmark"]) == moves
    assert _card_keys(sections["Manuscript Viewer"]) == moves | {
        f"Ctrl+{nxt}", f"Ctrl+{prev}", "Ctrl+B", "Ctrl+F", "Ctrl+=", "Ctrl+-", "Ctrl+0", "Esc"}
    # Browse by Shelfmark: bound on its tab, to its buttons.
    browse = _method_source(_app_source(), "create_browse_tab")
    for needle in ("nxt, prev = arrow_keys(CURRENT_LANG == 'he')",
                   '("Ctrl+PgDown", self.btn_b_next,', '("Ctrl+PgUp", self.btn_b_prev,',
                   '(f"Ctrl+Shift+{nxt}", self.btn_next_ms,', '(f"Ctrl+Shift+{prev}", self.btn_prev_ms,',
                   "shortcut.setContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)",
                   "shortcut.activated.connect(button.click)"):
        assert needle in browse


@pytest.mark.parametrize("hebrew", [False, True])
def test_the_card_shows_each_key_beside_what_it_does_under_its_heading(hebrew):
    from PyQt6.QtWidgets import QFormLayout
    from desktop.shortcuts_dialog import ShortcutsDialog, key_label, shortcut_sections
    from desktop.widgets.side_nav import windows_virtual_key
    from genizah_core import tr
    dlg = ShortcutsDialog(None, hebrew=hebrew)
    form = dlg.findChild(QFormLayout)
    shown = []
    for r in range(form.rowCount()):
        span = form.itemAt(r, QFormLayout.ItemRole.SpanningRole)
        if span is not None:
            shown.append(("#", span.widget().text()))
        else:
            shown.append((form.itemAt(r, QFormLayout.ItemRole.LabelRole).widget().text(),
                          form.itemAt(r, QFormLayout.ItemRole.FieldRole).widget().text()))
    expected = []
    for title, rows in shortcut_sections(hebrew):
        expected.append(("#", tr(title)))
        expected += [(key_label(keys, hebrew), tr(what)) for keys, what in rows]
    assert shown == expected
    from PyQt6.QtWidgets import QLabel
    from desktop.shortcuts_dialog import TEXT_BOX_NOTE                # under the list (Codex review)
    assert tr(TEXT_BOX_NOTE) in [label.text() for label in dlg.findChildren(QLabel)]
    if hebrew:
        assert shown[1][0] == "‏Ctrl+1‎‏ … ‏Ctrl+7‎"
        assert any("חץ שמאלה" in keys for keys, _w in shown)   # חץ שמאלה
    else:
        assert shown[1][0] == "‎Ctrl+1 … Ctrl+7‎"
    assert windows_virtual_key("/") == 0xBF                       # Ctrl+/ on a Hebrew layout
    dlg.deleteLater()


def test_hebrew_key_names_read_right_to_left_like_their_text():
    """Hebrew names for the keys named in words (owner, 2026-10-30), Ctrl first
    on the right, the first of a pair on the right as "הבא / הקודם" is, and a
    trailing "/" or "," kept with its key; English unchanged."""
    from desktop.shortcuts_dialog import key_label
    from desktop.widgets.side_nav import ltr
    lrm, rlm = "‎", "‏"
    assert key_label("Ctrl+Shift+Left / Ctrl+Shift+Right", True) == (
        f"{rlm}Ctrl+Shift+חץ שמאלה{lrm}{rlm} / {rlm}"
        f"Ctrl+Shift+חץ ימינה{lrm}")
    assert key_label("Space", True) == f"{rlm}רווח{lrm}"          # רווח
    assert key_label("Down", True) == f"{rlm}חץ למטה{lrm}"   # חץ למטה
    assert key_label("Ctrl+,", True) == f"{rlm}Ctrl+,{lrm}"
    assert key_label("Ctrl++", True) == f"{rlm}Ctrl++{lrm}"
    assert key_label("Ctrl+PgDown / Ctrl+PgUp", True) == f"{rlm}Ctrl+PgDown{lrm}{rlm} / {rlm}Ctrl+PgUp{lrm}"
    for keys in ("Ctrl+Shift+Left / Ctrl+Shift+Right", "Space", "Ctrl+,"):
        assert key_label(keys, False) == ltr(keys)


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
def test_a_search_ends_in_the_results_on_the_first_row(start):
    """From the box, the button, or wherever Go to left the cursor; the first
    row is current, so Space and Enter act on it at once."""
    w = _SearchTab()
    _start_search_from(w, getattr(w, start))
    assert w._focus_results_after_search() is True
    assert w.focusWidget() is w.results_table
    assert (w.results_table.currentRow(), w.results_table.currentColumn()) == (0, 1)
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


# ---- The keys of the result lists and the Manuscript Viewer (owner, 2026-10-30) ----

def _key(key, mods=None):
    from PyQt6.QtCore import QEvent, Qt
    from PyQt6.QtGui import QKeyEvent
    return QKeyEvent(QEvent.Type.KeyPress, key, mods or Qt.KeyboardModifier.NoModifier)


class _ResultLists:
    """The Search results, Personal Lists and Composition Search's tree and text,
    with the real key methods; what a key opens is recorded."""

    _enter_action = GenizahGUI._enter_action
    _open_action = GenizahGUI._open_action
    _KEY_MODIFIERS = GenizahGUI._KEY_MODIFIERS
    _results_table_key = GenizahGUI._results_table_key
    _current_result_res = GenizahGUI._current_result_res
    _tick_current_result = GenizahGUI._tick_current_result
    _tick_all_results = GenizahGUI._tick_all_results
    on_search_select_all_toggled = GenizahGUI.on_search_select_all_toggled
    _lists_table_key = GenizahGUI._lists_table_key
    _comp_tree_key = GenizahGUI._comp_tree_key
    _comp_text_key = GenizahGUI._comp_text_key
    eventFilter = GenizahGUI.eventFilter
    COL_CHECKBOX, COL_SYS_ID = 0, 1

    def __init__(self):
        from PyQt6.QtCore import Qt
        from PyQt6.QtWidgets import (QPushButton, QTableWidget, QTableWidgetItem,
                                     QTreeWidget, QTreeWidgetItem)
        from desktop.ui_widgets import CheckBoxHeader
        self.calls = []
        self.results_table = QTableWidget(4, 3)
        for r in range(4):
            box = QTableWidgetItem()
            box.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled
                         | Qt.ItemFlag.ItemIsSelectable)
            box.setCheckState(Qt.CheckState.Unchecked)
            self.results_table.setItem(r, 0, box)
            sid = QTableWidgetItem(f"99{r}")
            sid.setData(Qt.ItemDataRole.UserRole, {"uid": f"u{r}"})
            self.results_table.setItem(r, 1, sid)
        # The real header box (9.12.0's Ctrl+A broke on it while a QCheckBox
        # stand-in passed): isChecked is a field, setChecked sends nothing.
        self.chk_search_header = CheckBoxHeader(self.results_table)
        self.results_table.setHorizontalHeader(self.chk_search_header)
        self.chk_search_header.toggled.connect(self.on_search_select_all_toggled)
        self.lists_items_table = QTableWidget(2, 2)
        for r in range(2):
            item = QTableWidgetItem()
            item.setData(Qt.ItemDataRole.UserRole, f"item{r}")
            self.lists_items_table.setItem(r, 0, item)
        self.comp_tree = QTreeWidget()
        self.comp_tree.addTopLevelItems([QTreeWidgetItem([f"ms{r}"]) for r in range(2)])
        self.comp_text_area = QPlainTextEdit()
        self.btn_comp_run = QPushButton("Analyze")
        self.btn_comp_run.clicked.connect(lambda: self.calls.append("run"))
        self.is_comp_running = False

    def box(self, row):
        return self.results_table.item(row, 0).checkState()

    def show_full_text_for_result(self, res):
        self.calls.append(("view", res["uid"]))

    def open_result_in_browse_from_table(self, res):
        self.calls.append(("browse", res["uid"]))

    def _update_search_export_label(self):
        pass

    def lists_quick_view_by_id(self, item_id):
        self.calls.append(("quick_view", item_id))

    def lists_browse_by_id(self, item_id):
        self.calls.append(("lists_browse", item_id))

    def on_comp_item_double_clicked(self, item, column, browse=False):
        self.calls.append(("comp", item.text(0), browse))


def test_space_ticks_the_current_result_in_any_column_and_again_unticks_it():
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    w.results_table.setCurrentCell(2, 2)                   # not the check box column
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_Space)) is True
    assert w.box(2) == Qt.CheckState.Checked and w.box(1) == Qt.CheckState.Unchecked
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_Space)) is True
    assert w.box(2) == Qt.CheckState.Unchecked
    assert w.calls == []                                   # Space opens nothing


@pytest.mark.parametrize("mods", ["ControlModifier", "ShiftModifier"])
def test_ctrl_space_and_shift_space_do_nothing(mods):
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    w.results_table.setCurrentCell(1, 1)
    assert w._results_table_key(_key(Qt.Key.Key_Space, getattr(Qt.KeyboardModifier, mods))) is True
    assert all(w.box(r) == Qt.CheckState.Unchecked for r in range(4))


def test_enter_opens_the_result_and_ctrl_enter_opens_it_in_browse():
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    w.results_table.setCurrentCell(3, 1)
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_Return)) is True
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_Return, Qt.KeyboardModifier.ControlModifier)) is True
    # the keypad's Enter too, and Ctrl+B as in the Manuscript Viewer
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_Enter, Qt.KeyboardModifier.KeypadModifier)) is True
    assert w.eventFilter(w.results_table, _key(Qt.Key.Key_B, Qt.KeyboardModifier.ControlModifier)) is True
    assert w.calls == [("view", "u3"), ("browse", "u3"), ("view", "u3"), ("browse", "u3")]
    assert w._results_table_key(_key(Qt.Key.Key_Return, Qt.KeyboardModifier.AltModifier)) is False
    assert w._results_table_key(_key(Qt.Key.Key_B)) is False      # B alone: Qt's own
    w.results_table.setRowHidden(3, True)                   # a hidden row opens nothing
    w._results_table_key(_key(Qt.Key.Key_Return))
    assert len(w.calls) == 4


def test_ctrl_a_ticks_every_shown_result_and_again_unticks_them():
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    w.results_table.setRowHidden(3, True)
    w.results_table.item(0, 0).setCheckState(Qt.CheckState.Checked)
    ctrl_a = _key(Qt.Key.Key_A, Qt.KeyboardModifier.ControlModifier)
    assert w.eventFilter(w.results_table, ctrl_a) is True
    assert [w.box(r) for r in range(4)] == [Qt.CheckState.Checked] * 3 + [Qt.CheckState.Unchecked]
    assert w.chk_search_header.isChecked is True                # the header box shows it
    assert w._results_table_key(ctrl_a) is True
    assert all(w.box(r) == Qt.CheckState.Unchecked for r in range(4))
    assert w.chk_search_header.isChecked is False


def test_lists_and_composition_results_open_with_enter_and_ctrl_enter():
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    ctrl = Qt.KeyboardModifier.ControlModifier
    w.lists_items_table.setCurrentCell(1, 1)
    assert w.eventFilter(w.lists_items_table, _key(Qt.Key.Key_Return)) is True
    assert w.eventFilter(w.lists_items_table, _key(Qt.Key.Key_Return, ctrl)) is True
    assert w.eventFilter(w.lists_items_table, _key(Qt.Key.Key_B, ctrl)) is True
    w.comp_tree.setCurrentItem(w.comp_tree.topLevelItem(1))
    assert w.eventFilter(w.comp_tree, _key(Qt.Key.Key_Return)) is True
    assert w.eventFilter(w.comp_tree, _key(Qt.Key.Key_Enter, ctrl | Qt.KeyboardModifier.KeypadModifier)) is True
    assert w.eventFilter(w.comp_tree, _key(Qt.Key.Key_B, ctrl)) is True
    assert w.calls == [("quick_view", "item1"), ("lists_browse", "item1"), ("lists_browse", "item1"),
                       ("comp", "ms1", False), ("comp", "ms1", True), ("comp", "ms1", True)]
    assert w._lists_table_key(_key(Qt.Key.Key_Space)) is False   # Qt's own Space there


def test_ctrl_enter_runs_composition_search_from_its_text_and_never_stops_it():
    from PyQt6.QtCore import Qt
    w = _ResultLists()
    ctrl_enter = _key(Qt.Key.Key_Return, Qt.KeyboardModifier.ControlModifier)
    assert w._comp_text_key(_key(Qt.Key.Key_Return)) is False     # Enter stays a new line
    assert w._comp_text_key(_key(Qt.Key.Key_B, Qt.KeyboardModifier.ControlModifier)) is False  # not a run
    assert w.eventFilter(w.comp_text_area, ctrl_enter) is True
    assert w.calls == ["run"]
    w.is_comp_running = True                                       # running: Ctrl+Enter is not Stop
    assert w._comp_text_key(ctrl_enter) is True and w.calls == ["run"]
    w.is_comp_running = False
    w.btn_comp_run.setEnabled(False)
    w._comp_text_key(ctrl_enter)
    assert w.calls == ["run"]


def test_the_icons_name_their_keys_and_the_lists_take_them():
    src = _app_source()
    keys = "key_label('Ctrl+Enter / Ctrl+B', CURRENT_LANG == 'he')"
    assert f"f\"{{tr('Browse manuscript')}} ({{{keys}}})\"" in src
    assert "f\"{tr('View result')} ({ltr('Enter')})\"" in src
    assert "f\"{tr('Quick View')} ({ltr('Enter')})\"" in src and f"f\"{{tr('Browse')}} ({{{keys}}})\"" in src
    assert "self.comp_tree.installEventFilter(self)" in src and "self.comp_text_area.installEventFilter(self)" in src
    assert "self.lists_items_table.installEventFilter(self)" in src
    done = _method_source(src, "on_comp_item_double_clicked")
    assert "def on_comp_item_double_clicked(self, item, column, browse=False):" in done
    assert done.index("if browse:") < done.index("self._show_result_dialog(")
    assert "self.open_result_in_browse(entry," in done


class _Viewer(QWidget):
    """The Manuscript Viewer's buttons, with its real key installer."""

    def __init__(self):
        from PyQt6.QtWidgets import QPushButton
        import desktop.result_dialog as rd
        super().__init__()
        self._install_shortcuts = rd.ResultDialog._install_shortcuts.__get__(self)
        self._focus_find = rd.ResultDialog._focus_find.__get__(self)
        self.clicked, self.sizes = [], []
        for name in ("btn_res_next", "btn_res_prev", "btn_res_next_ms", "btn_res_prev_ms",
                     "btn_pg_next", "btn_pg_prev", "btn_compact_pg_next", "btn_compact_pg_prev",
                     "btn_view_transcription"):
            button = QPushButton(name, self)
            button.clicked.connect(lambda _c=False, n=name: self.clicked.append(n))
            setattr(self, name, button)
        self.btn_res_next_ms.setToolTip("next ms")
        self.btn_res_prev_ms.setToolTip("prev ms")
        self.btn_view_transcription.setToolTip("Browse manuscript")
        self.btn_text_smaller, self.btn_text_larger = QPushButton("A-", self), QPushButton("A+", self)
        self.ms_widget = QWidget(self)                    # the text pane, with Find in it
        self.find_ms_input = QLineEdit(self.ms_widget)
        self.find_ms_input.setText("abc")
        self.btn_toggle_text = QPushButton("text", self)  # hides the text beside the image
        self.btn_toggle_text.setCheckable(True)
        self.btn_toggle_text.setChecked(True)
        self.btn_toggle_text.toggled.connect(self.ms_widget.setVisible)

    def _change_text_size(self, delta):
        self.sizes.append(delta)


@pytest.mark.parametrize("hebrew", [False, True])
def test_the_viewer_keys_press_their_buttons_and_mirror_in_hebrew(monkeypatch, hebrew):
    import desktop.result_dialog as rd
    from desktop.shortcuts_dialog import key_label
    monkeypatch.setattr(rd, "CURRENT_LANG", "he" if hebrew else "en")
    reset = []
    monkeypatch.setattr(rd, "reading_text_size", lambda: type("S", (), {"set": lambda _s, pt: reset.append(pt)})())
    v = _Viewer()
    v.show()
    v._install_shortcuts()
    try:
        keys = {sc.key().toString(): sc for sc in v._shortcuts}
        assert len(keys) == len(v._shortcuts)                     # no key bound twice
        nxt, prev = ("Left", "Right") if hebrew else ("Right", "Left")
        presses = {f"Ctrl+{nxt}": "btn_res_next", f"Ctrl+{prev}": "btn_res_prev",
                   f"Ctrl+Shift+{nxt}": "btn_res_next_ms", f"Ctrl+Shift+{prev}": "btn_res_prev_ms",
                   "Ctrl+PgDown": "btn_pg_next", "Ctrl+PgUp": "btn_pg_prev",
                   "Ctrl+B": "btn_view_transcription"}
        for k, name in presses.items():
            v.clicked.clear()
            keys[k].activated.emit()
            assert v.clicked == [name], k
            assert getattr(v, name).toolTip().endswith(f"({key_label(k, hebrew)})")
        assert v.btn_compact_pg_next.toolTip().endswith(f"({key_label('Ctrl+PgDown', hebrew)})")
        assert v.btn_res_next_ms.toolTip() == f"next ms ({key_label(f'Ctrl+Shift+{nxt}', hebrew)})"
        if hebrew:                                                 # the Hebrew key name
            assert "חץ שמאלה" in v.btn_res_next.toolTip()
        v.btn_res_next.setEnabled(False)                           # a disabled button: nothing
        v.clicked.clear()
        keys[f"Ctrl+{nxt}"].activated.emit()
        assert v.clicked == []
        keys["Ctrl+F"].activated.emit()
        assert v.focusWidget() is v.find_ms_input and v.find_ms_input.selectedText() == "abc"
        v.btn_toggle_text.setChecked(False)                        # the text hidden (Codex review)
        v.btn_res_next.setFocus()
        keys["Ctrl+F"].activated.emit()
        assert v.btn_toggle_text.isChecked() and v.ms_widget.isVisible()
        assert v.focusWidget() is v.find_ms_input
        for button, tip, k in ((v.btn_text_smaller, "Smaller text", "Ctrl+-"),
                               (v.btn_text_larger, "Larger text", "Ctrl+=")):
            assert button.toolTip() == f"{rd.tr(tip)} ({key_label(k, hebrew)})"
        for k in ("Ctrl+=", "Ctrl++", "Ctrl+-"):
            keys[k].activated.emit()
        keys["Ctrl+0"].activated.emit()
        assert v.sizes == [rd.TEXT_PT_STEP, rd.TEXT_PT_STEP, -rd.TEXT_PT_STEP]
        assert reset == [rd.TEXT_PT_DEFAULT]
    finally:
        layout_keys = getattr(v, "_layout_keys", None)
        if layout_keys is not None:
            QApplication.instance().removeEventFilter(layout_keys)
        v.deleteLater()


def test_a_viewer_key_typed_in_find_moves_by_word_there(monkeypatch):
    """Real key presses (Codex review): Ctrl+Right pages the results, but in
    the Find box Qt keeps it for moving by word, as the card and Help say."""
    from PyQt6.QtCore import Qt
    from PyQt6.QtTest import QTest
    import desktop.result_dialog as rd
    monkeypatch.setattr(rd, "CURRENT_LANG", "en")
    v = _Viewer()
    v.show()
    v._install_shortcuts()
    try:
        v.activateWindow()
        if not QTest.qWaitForWindowActive(v):
            pytest.skip("the window cannot become active on this platform")
        v.btn_res_prev.setFocus()
        QTest.keyClick(v.btn_res_prev, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert v.clicked == ["btn_res_next"]
        v.find_ms_input.setFocus()
        v.find_ms_input.setCursorPosition(0)
        QTest.keyClick(v.find_ms_input, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert v.clicked == ["btn_res_next"]                       # no second result
        assert v.find_ms_input.cursorPosition() == 3               # one word on
    finally:
        layout_keys = getattr(v, "_layout_keys", None)
        if layout_keys is not None:
            QApplication.instance().removeEventFilter(layout_keys)
        v.deleteLater()


def test_a_genizah_result_after_a_one_page_local_one_has_working_image_keys():
    """Codex review: a one-page My Library document disables the image buttons
    (and so Ctrl+PgDown / Ctrl+PgUp); the next Genizah result in an all-sources
    list enables them again."""
    from PyQt6.QtWidgets import QPushButton
    import desktop.result_dialog as rd
    names = ("btn_pg_next", "btn_pg_prev", "btn_compact_pg_next", "btn_compact_pg_prev")
    v = type("V", (), {})()
    for name in names:
        button = QPushButton()
        button.setEnabled(False)
        setattr(v, name, button)
    v._set_local_page_nav_enabled = rd.ResultDialog._set_local_page_nav_enabled.__get__(v)
    v.cancel_image_thread = lambda: None
    v.current_sys_id = "990001234560205171"
    v.current_internal_idx, v.current_p_num, v.current_volume_ie = 0, 1, None
    v.searcher = type("S", (), {"get_browse_page": lambda *_a, **_k: None})()
    rd.ResultDialog.load_page(v, offset=1)
    assert [getattr(v, n).isEnabled() for n in names] == [True] * 4


def test_the_viewer_installs_its_keys_once_its_buttons_exist():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "desktop" / "result_dialog.py").read_text(encoding="utf-8")
    assert "        self.init_ui()\n        self._install_shortcuts()\n" in src.replace("\r\n", "\n")
