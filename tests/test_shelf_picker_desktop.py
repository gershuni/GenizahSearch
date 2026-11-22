# -*- coding: utf-8 -*-
"""The desktop "Pick from the shelf" (shelf tree picker PR 3): the dialog
(desktop/widgets/shelfmark_tree_picker.py) and its hosts in genizah_app.py and the Go to box.

On the fixture tree of tests/shelfmark_tree_fixture.py, the lessons the web picker's render smoke
pins (tests/render_smoke/test_shelf_picker_render_smoke.py), on the desktop: opened at a manuscript
it is marked; a click goes down, the breadcrumb back; a filter with a number finds shelfmarks from a
column above their volume; a slower, older filter or click never replaces a newer one; a filter keeps
its box while its list is drawn again; two filters typed together both draw; the keyboard opens a
level and picks; library names wrap, never cut; Hebrew names in Hebrew; 3, 2 or 1 shelfmark columns.

The call sites (handoff "Every host PR"): a pick reaches browse_load through the path a typed system
number takes in Go to; Go to's own button does the same; the flag on and the file missing hide the
button and the place on the shelf; LOCAL ids never get a place; previous / next and page crossing
follow the Oxford Part, then the shelf, then the file order.

The language is pinned and the four profile variables point at scratch folders: the developer's
desktop profile (Hebrew on the owner's machine) is never read.
"""
from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QEvent, QPointF, Qt
from PyQt6.QtGui import QMouseEvent
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QWidget

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    _APP.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
    yield


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)   # the app's: a connection per thread
    yield service
    service.close()


def _wait(pred, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if pred():
            return True
        time.sleep(0.01)
    _APP.processEvents()
    return pred()


def _settled(dialog):
    return dialog._view is not None and not dialog._loads.busy()


def _open(svc, start=None):
    dialog = picker.ShelfPickerDialog(svc, start=start)
    dialog.resize(1200, 700)
    dialog.show()
    assert _wait(lambda: _settled(dialog)), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _columns(dialog):
    return [c for c in dialog.node_columns if c.isVisibleTo(dialog)]


def _node_rows(column):
    return [r for r in column.rows if 'id' in r]


def _row_of(column, node_id):
    return next(i for i, r in enumerate(column.rows) if r.get('id') == node_id)


def _leaf_sys_ids(dialog):
    grid = dialog.leaf_panel.grid
    return [grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id for i in range(grid.count())]


def _spy_draws(dialog):
    """Every view the dialog draws, whole or a list at a time."""
    drawn = []
    whole, lists = dialog._draw, dialog._draw_lists

    def draw(view):
        drawn.append(view)
        return whole(view)

    def draw_lists(view, owed):
        drawn.append(view)
        return lists(view, owed)

    dialog._draw, dialog._draw_lists = draw, draw_lists
    return drawn


def _click_row(column, row):
    rect = column.table.visualRect(column.table.model().index(row, 0))
    QTest.mouseClick(column.table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                     rect.center())


# -- the dialog -------------------------------------------------------------------------------

def test_opened_at_a_manuscript_it_is_marked_and_a_pick_answers_the_contract(svc):
    sid = SYS["as2"][2]                                       # T-S AS 2.3
    placement = svc.path_of(sid)[0].leaf
    dialog = _open(svc, start=sid)
    try:
        assert dialog.path()[-1] == "vol:100"
        assert dialog.leaf_panel.isVisibleTo(dialog)
        assert dialog._highlight == placement.id
        chosen = dialog.leaf_panel.chosen
        assert chosen is not None and chosen.font().bold()
        assert dialog.leaf_panel.grid.currentItem() is chosen
        assert dialog.leaf_panel.title.text().startswith("Shelfmarks (12)")
        # Enter on the shelfmark the keyboard is on picks it
        dialog.leaf_panel.grid.setFocus()
        QTest.keyClick(dialog.leaf_panel.grid, Qt.Key.Key_Return)
        assert dialog.result() == dialog.DialogCode.Accepted
        assert dialog.selection() == {"sys_id": sid, "shelfmark": "T-S AS 2.3", "placement_id": placement.id}
    finally:
        _close(dialog)


def test_closed_without_a_pick_it_answers_none(svc):
    dialog = _open(svc)
    dialog.reject()
    assert dialog.selection() is None
    _close(dialog)


def test_a_click_goes_down_and_the_breadcrumb_back(svc):
    dialog = _open(svc)
    try:
        libraries = _columns(dialog)[0]
        assert libraries.col.get("libs")
        _click_row(libraries, _row_of(libraries, "lib:1"))
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:1",))
        keys = [c.key for c in _columns(dialog)]
        assert keys == ["n:None", "n:lib:1"]
        assert {r["id"] for r in _node_rows(_columns(dialog)[1])} >= {"col:10", "col:11", "col:12"}
        assert _columns(dialog)[0].table.selectedItems()[0].text().startswith("Cambridge")
        # a collection holding volumes and shelfmarks of its own shows both (T-S AS)
        column = _columns(dialog)[1]
        _click_row(column, _row_of(column, "col:10"))
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:1", "col:10"))
        assert {r["id"] for r in _node_rows(_columns(dialog)[-1])} == {"vol:100", "vol:101"}
        assert dialog.leaf_panel.isVisibleTo(dialog)
        # the breadcrumb goes back to any level
        names = [b.text() for b in dialog.crumb_buttons]
        assert names[0] == "All libraries" and "Cambridge" in names[1]
        dialog.crumb_buttons[0].click()
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ())
        assert len(_columns(dialog)) == 1 and not dialog.leaf_panel.isVisibleTo(dialog)
    finally:
        _close(dialog)


def test_a_known_shelfmark_is_found_from_a_column_above_its_volume(svc):
    dialog = _open(svc, start="lib:1")
    try:
        column = next(c for c in _columns(dialog) if c.key == "n:lib:1")
        dialog.set_filter(column.key, "2.4")
        assert _wait(lambda: _settled(dialog) and any("leaf" in r for r in column.rows))
        row = next(i for i, r in enumerate(column.rows) if "leaf" in r and r["leaf"].sys_id == SYS["as2"][3])
        _click_row(column, row)                                # a click chooses it (DX21) ...
        assert dialog.selection() is None and dialog._chosen_leaf.sys_id == SYS["as2"][3]
        dialog.act_button.click()                              # ... the button takes it
        assert dialog.selection()["sys_id"] == SYS["as2"][3]
    finally:
        _close(dialog)


def test_a_slower_older_filter_does_not_replace_a_newer_one(svc, monkeypatch):
    real = picker.load_view

    def slow_for_broad(service, path, filters, *rest, **kw):
        if "T-S" in filters.values():
            time.sleep(0.6)
        return real(service, path, filters, *rest, **kw)

    monkeypatch.setattr(picker, "load_view", slow_for_broad)
    dialog = _open(svc, start="col:10")
    try:
        drawn = _spy_draws(dialog)
        column = next(c for c in _columns(dialog) if c.key == "n:col:10")
        dialog.set_filter(column.key, "T-S")
        _wait(lambda: False, timeout=0.05)
        dialog.set_filter(column.key, "2.4")
        assert _wait(lambda: _settled(dialog))
        _wait(lambda: False, timeout=0.8)                     # the broad load is long over
        leaves = [r["leaf"].sys_id for r in column.rows if "leaf" in r]
        assert SYS["as2"][3] in leaves, column.rows
        assert "vol:101" not in {r.get("id") for r in column.rows}
        # ... and the broad list was never drawn, not even for a moment
        broad = [v for v in drawn for c in v["columns"] if c["filter_key"] == "n:col:10"
                 and "vol:101" in {r.get("id") for r in c["rows"]}]
        assert not broad and drawn
    finally:
        _close(dialog)


def test_the_last_click_wins_over_a_slower_earlier_one(svc, monkeypatch):
    real = picker.load_view

    def slow_for_cul(service, path, *rest, **kw):
        if path[:1] == ("lib:1",):
            time.sleep(0.6)
        return real(service, path, *rest, **kw)

    monkeypatch.setattr(picker, "load_view", slow_for_cul)
    dialog = _open(svc)
    try:
        drawn = _spy_draws(dialog)
        libraries = _columns(dialog)[0]
        _click_row(libraries, _row_of(libraries, "lib:1"))
        _wait(lambda: False, timeout=0.05)
        dialog.choose_node(0, "lib:2")                        # a second row of the same list
        assert _wait(lambda: _settled(dialog))
        _wait(lambda: False, timeout=0.8)
        assert dialog.path() == ("lib:2", "col:20")
        assert set(_leaf_sys_ids(dialog)) == set(SYS["stras"]) | {SYS["single"]}
        on = [r["id"] for r in _columns(dialog)[0].rows if r.get("on")]
        assert on == ["lib:2"], on
        assert [v["crumbs"][0][1] for v in drawn] == ["lib:2"]   # Cambridge's level never shown
    finally:
        _close(dialog)


def test_a_row_drawn_before_a_newer_click_keeps_its_own_path(svc, monkeypatch):
    # Codex pre-flight: a row still on screen from library A, clicked after library B, opens A's level.
    real = picker.load_view

    def slow(service, path, *rest):
        if path[:1] == ("lib:3",):
            time.sleep(0.5)
        return real(service, path, *rest)

    dialog = _open(svc, start="lib:1")
    try:
        monkeypatch.setattr(picker, "load_view", slow)
        dialog.choose_node(0, "lib:3")                        # its lists are not drawn yet
        collections = next(c for c in _columns(dialog) if c.key == "n:lib:1")
        collections.dialog._open_row(collections, _row_of(collections, "col:12"))
        assert _wait(lambda: _settled(dialog))
        assert dialog.path() == ("lib:1", "col:12")
    finally:
        _close(dialog)


def test_a_double_click_is_one_click_and_a_later_click_still_counts(svc):
    # Codex pre-flight: the second press of a double click lands on the list its first click has
    # drawn meanwhile; it must open nothing there. A distinct later click is not dropped.
    dialog = _open(svc)
    try:
        libraries = _columns(dialog)[0]
        viewport = libraries.table.viewport()
        signals = []
        libraries.table.clicked.connect(lambda _i: signals.append("clicked"))
        libraries.table.doubleClicked.connect(lambda _i: signals.append("double"))
        point = QPointF(libraries.table.visualRect(libraries.table.model().index(_row_of(libraries, "lib:1"), 0)).center())
        QTest.mouseClick(viewport, Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, point.toPoint())
        assert signals == ["clicked"]
        for kind, buttons in ((QEvent.Type.MouseButtonDblClick, Qt.MouseButton.LeftButton),
                              (QEvent.Type.MouseButtonRelease, Qt.MouseButton.NoButton)):
            QApplication.sendEvent(viewport, QMouseEvent(kind, point, point, Qt.MouseButton.LeftButton, buttons,
                                                         Qt.KeyboardModifier.NoModifier))
        assert signals == ["clicked"], signals                 # nothing more reached the list
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:1",))
        libraries = _columns(dialog)[0]
        _click_row(libraries, _row_of(libraries, "lib:2"))
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:2", "col:20"))
    finally:
        _close(dialog)


def test_a_filter_keeps_its_box_while_its_list_is_drawn_again(svc):
    dialog = _open(svc, start="col:10")
    try:
        column = next(c for c in _columns(dialog) if c.key == "n:col:10")
        box = column.filter
        box.setFocus()
        QTest.keyClicks(box, "10")
        assert dialog._filters[column.key] == "10"             # kept at once, before the load
        assert _wait(lambda: _settled(dialog) and column.rows
                     and {r.get("id") for r in column.rows if "id" in r} == {"vol:101"})
        assert column.filter is box and box.text() == "10" and box.cursorPosition() == 2
        assert dialog.focusWidget() is box
    finally:
        _close(dialog)


def test_two_filters_typed_together_both_draw_their_lists(svc):
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        column = next(c for c in _columns(dialog) if c.key == "n:col:10")
        QTest.keyClicks(column.filter, "10")
        QTest.keyClicks(dialog.leaf_panel.filter, "2.5")
        assert _wait(lambda: _settled(dialog)
                     and {r.get("id") for r in column.rows if "id" in r} == {"vol:101"}
                     and _leaf_sys_ids(dialog) == [SYS["as2"][4]])
        assert column.filter.text() == "10" and dialog.leaf_panel.filter.text() == "2.5"
    finally:
        _close(dialog)


def test_a_filter_typed_just_before_a_click_is_kept(svc):
    # Codex pre-flight: the text is the list's filter at once; only its load waits for the pause.
    dialog = _open(svc, start="lib:1")
    try:
        column = next(c for c in _columns(dialog) if c.key == "n:lib:1")
        QTest.keyClicks(column.filter, "T-S")
        dialog.choose_node(1, "col:10")                        # before the 250 ms pause
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:1", "col:10"))
        again = next(c for c in _columns(dialog) if c.key == "n:lib:1")
        assert again.filter.text() == "T-S" and dialog._filters["n:lib:1"] == "T-S"
    finally:
        _close(dialog)


def test_the_keyboard_opens_a_level_and_picks(svc):
    dialog = _open(svc)
    try:
        libraries = _columns(dialog)[0]
        libraries.table.setFocus()
        libraries.table.setCurrentCell(_row_of(libraries, "lib:2"), 0)
        QTest.keyClick(libraries.table, Qt.Key.Key_Return)
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ("lib:2", "col:20"))
        grid = dialog.leaf_panel.grid
        assert dialog.focusWidget() is grid
        grid.setCurrentRow(1)
        QTest.keyClick(grid, Qt.Key.Key_Space)                 # Space chooses (DX21) ...
        assert dialog.selection() is None and dialog._chosen_leaf.sys_id == SYS["stras"][1]
        QTest.keyClick(grid, Qt.Key.Key_Return)                # ... Enter is the button
        assert dialog.selection()["sys_id"] == SYS["stras"][1]
    finally:
        _close(dialog)


def test_the_keyboard_reaches_the_breadcrumbs_and_enter_presses_them(svc):
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        crumb = dialog.crumb_buttons[0]
        assert crumb.focusPolicy() == Qt.FocusPolicy.StrongFocus
        crumb.setFocus()
        QTest.keyClick(crumb, Qt.Key.Key_Return)
        assert _wait(lambda: _settled(dialog) and dialog._drawn_path == ())
        assert dialog.isVisible()                              # Enter did not close the dialog
    finally:
        _close(dialog)


def test_enter_in_a_filter_does_not_close_the_dialog(svc):
    dialog = _open(svc)
    try:
        box = _columns(dialog)[0].filter
        QTest.keyClicks(box, "Stras")
        QTest.keyClick(box, Qt.Key.Key_Return)
        assert _wait(lambda: _settled(dialog))
        assert dialog.isVisible() and dialog.selection() is None
        assert [r["id"] for r in _node_rows(_columns(dialog)[0])] == ["lib:2"]
    finally:
        _close(dialog)


def test_library_names_wrap_and_are_never_cut(svc):
    dialog = _open(svc)
    try:
        table = _columns(dialog)[0].table
        assert table.wordWrap() and table.textElideMode() == Qt.TextElideMode.ElideNone
        texts = [table.item(r, 0).text() for r in range(table.rowCount()) if table.item(r, 0)]
        assert any("Strasbourg" in t for t in texts) and not any("…" in t for t in texts)
    finally:
        _close(dialog)


def test_hebrew_names_in_hebrew(svc, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    _APP.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = _open(svc)
    try:
        hebrew = svc.node("lib:1").name_he
        assert hebrew
        rows = _columns(dialog)[0].rows
        assert any(r.get("label") == hebrew for r in rows)
        # a Latin name in a right-to-left list keeps its closing dot at its end
        assert picker.own_direction("T-S Ar.", True) == "\u200eT-S Ar.\u200e"
        assert picker.own_direction(hebrew, True) == hebrew
    finally:
        _close(dialog)


@pytest.mark.parametrize("avail, need, columns", [(900, 140, 3), (500, 160, 3), (400, 160, 2),
                                                  (200, 160, 1), (100, 160, 1)])
def test_shelfmarks_stand_in_three_else_two_else_one_columns(avail, need, columns):
    assert picker.leaf_columns(avail, need) == columns


def test_the_grid_is_as_wide_as_its_longest_shelfmark(svc):
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        panel = dialog.leaf_panel
        cols = panel.columns()
        assert 1 <= cols <= 3
        assert panel.grid.gridSize().width() * cols <= panel.grid.viewport().width()
        assert panel.grid.gridSize().width() >= picker.LEAF_MIN_WIDTH or cols == 1
    finally:
        _close(dialog)


def test_the_desktop_switch_needs_the_file_opened_and_checked(svc, monkeypatch):
    import shared.shelfmark_tree_service as service_module
    monkeypatch.setattr(service_module, "_default_service", None)
    assert picker.shelf_tree_service() is None                 # never opened
    monkeypatch.setattr(service_module, "_default_service", svc)
    assert picker.shelf_tree_service() is svc
    monkeypatch.setenv("SHELFMARK_TREE_ENABLED", "0")
    assert picker.shelf_tree_service() is None                 # switched off
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED")
    missing = ShelfmarkTreeService(str(svc.db_path) + ".missing")
    monkeypatch.setattr(service_module, "_default_service", missing)
    assert picker.shelf_tree_service() is None                 # on, but the file is missing


# -- the Go to box ------------------------------------------------------------------------------

def test_go_to_offers_the_shelf_only_when_it_is_served(svc, monkeypatch):
    from desktop.go_to_dialog import GoToDialog
    plain = GoToDialog(None, shelf_service=None)
    assert plain.shelf_button is None
    plain.deleteLater()
    asked = {}

    def fake_pick(parent, service, start=None, **kw):
        asked.update(service=service, start=start, act=kw.get("act_label"), actions=kw.get("actions"))
        return ("open", {"sys_id": SYS["as2"][0], "shelfmark": "T-S AS 2.1", "placement_id": "p"})

    monkeypatch.setattr(picker, "pick_from_shelf_or_several", fake_pick)
    box = GoToDialog(None, shelf_service=svc, shelf_start="vol:100")
    assert box.shelf_button.toolTip().startswith("Pick from the shelf (")    # an icon button, its key said
    box.shelf_button.click()
    assert box.result() == box.DialogCode.Accepted
    assert box.choice() == ("shelf", {"sys_id": SYS["as2"][0], "shelfmark": "T-S AS 2.1", "placement_id": "p"})
    assert (asked["service"], asked["start"], asked["act"]) == (svc, "vol:100", "Open")
    box.deleteLater()
    typed = GoToDialog(None, shelf_service=svc, shelf_start="vol:100")
    typed.input.setText("T-S AS 10")
    typed.shelf_button.click()
    assert asked["start"] == "T-S AS 10"                       # what is typed comes first
    typed.deleteLater()


# -- the hosts in genizah_app.py ---------------------------------------------------------------

def _app_module(monkeypatch):
    import genizah_app
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en")
    return genizah_app


class _Field:
    def __init__(self, text=""):
        self._text = text

    def text(self):
        return self._text

    def setText(self, text):
        self._text = text

    def clear(self):
        self._text = ""


class _Host:
    """The parts of GenizahGUI the shelf's Browse paths read, with the real methods."""

    METHODS = ("_sync_browse_shelf_place", "_draw_browse_shelf_place", "_shelf_place_sid",
               "_browse_shelf_start", "open_shelf_picker", "_browse_open_shelf_pick", "_browse_open_field",
               "_browse_shelf_step", "_browse_in_part", "_browse_ms_tooltip", "_browse_endpoint_page",
               "_browse_cross", "navigate_manuscript", "browse_navigate", "_on_shelf_tree_opened",
               "_on_browse_shelf_link", "_browse_open_several_from_shelf", "_browse_rd_limit",
               "_browse_rd_base_ids", "_browse_rd_images_drawn", "_shelf_library_code",
               "_shelf_title", "_shelf_page_count", "_shelf_page_texts", "_shelf_choice_limit", "_shelf_open_actions", "_shelf_picker_options",
               "_shelf_outcome", "_shelf_add_to_list", "_shelf_add_to_puzzle",
               # navigate_manuscript asks it since the Browse prefetch (#98): None here, as the
               # stand-in meta_mgr has no enriched_recently.
               "_browse_fresh_meta")

    def __init__(self, app, sid=None):
        gui = app.GenizahGUI
        for name in self.METHODS:
            setattr(self, name, getattr(gui, name).__get__(self))
        self._STAYED = gui._STAYED
        self._box = QWidget()                                  # never shown: its children are not windows
        self.browse_shelf_place = QLabel(self._box)
        self.btn_browse_shelf = QPushButton(self._box)
        self.btn_next_ms, self.btn_prev_ms = QPushButton(self._box), QPushButton(self._box)
        self._browse_key_labels = {}
        self.browse_sys_input, self.browse_fl_input, self.browse_shelf_input = _Field(), _Field("FL1"), _Field("x")
        self._browse_shelf_want = None
        self._browse_shelf_for = None
        self._browse_shelf = None
        self.current_browse_sid = sid
        self.browse_reading_desk_active = False
        self.current_browse_part_id = None
        self.current_browse_part_folios = []
        self.current_browse_part_folio_idx = 0
        self.current_browse_p = 1
        self.current_browse_internal_idx = 0
        self.current_browse_volume_ie = None
        self.browse_tab = object()
        self.loads = []
        self.infos = []
        self.rendered = []
        self.last_field = None

    # what browse_load would do: the manuscript the fields name becomes the one shown
    def browse_load(self):
        self.loads.append((self.browse_sys_input.text(), self.browse_shelf_input.text(),
                           self.browse_fl_input.text(), self.last_field))
        self.current_browse_sid = None
        self._sync_browse_shelf_place()
        self.current_browse_sid = self.browse_sys_input.text() or None
        self._sync_browse_shelf_place()

    def _set_last_browse_field(self, name):
        self.last_field = name

    def _lists_after_shelf_save(self):
        pass

    def _set_active_tab(self, _tab):
        pass

    def _sync_browse_cite_button(self):
        self._sync_browse_shelf_place()


def _served(monkeypatch, service):
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: service)
    for hook in ("library_code_of", "title_of", "catalogue", "page_count_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)                    # the window sets them: restored after


def _no_boxes(monkeypatch, app, host):
    monkeypatch.setattr(app.QMessageBox, "information", lambda _parent, _title, text: host.infos.append(text))
    monkeypatch.setattr(app.QMessageBox, "warning", lambda _parent, _title, text: host.infos.append(text))


def test_a_pick_opens_through_the_path_a_typed_system_number_takes(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    sid = SYS["two_places"]
    first, second = (p.leaf for p in svc.path_of(sid))      # T-S NS 5.3 and T-S AS 2.3
    assert first.node_id != second.node_id
    monkeypatch.setattr(picker, "pick_from_shelf_or_several",
                        lambda *_a, **_k: ("open", {"sys_id": sid, "shelfmark": second.shelfmark,
                                                    "placement_id": second.id}))
    host = _Host(app)
    assert host.open_shelf_picker("vol:110")["sys_id"] == sid
    # browse_load saw the system number alone, as Go to leaves it, with the sys field chosen
    assert host.loads == [(sid, "", "", "sys")]
    assert host._browse_shelf["leaf"].id == second.id         # the placement picked, not the first
    host._sync_browse_shelf_place()
    host._sync_browse_shelf_place()
    assert host._browse_shelf["leaf"].id == second.id         # and it stays while the manuscript does
    assert f'href="node:{second.node_id}"' in host.browse_shelf_place.text()
    assert f'href="node:{first.node_id}"' not in host.browse_shelf_place.text()
    assert not host.browse_shelf_place.isHidden()
    # previous / next go on from the placement picked
    assert host._browse_shelf_step(sid) == svc.neighbours(second.id) != svc.neighbours(first.id)
    assert host._browse_shelf_start() == second.id


def test_the_go_to_box_hands_its_pick_to_the_same_adapter(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    pick = {"sys_id": SYS["as2"][1], "shelfmark": "T-S AS 2.2", "placement_id": svc.path_of(SYS["as2"][1])[0].leaf.id}

    class _Box:
        def __init__(self, *_a, **kw):
            self.kw = kw

        def exec(self):
            return True

        def choice(self):
            return ("shelf", pick)

        def deleteLater(self):
            pass

    monkeypatch.setattr(app, "GoToDialog", _Box)
    monkeypatch.setattr(app.startup_tips, "mark_used", lambda *_a: None)
    host = _Host(app)
    host._go_to_recent_items = lambda: []
    app.GenizahGUI.open_go_to(host)
    assert host.loads == [(SYS["as2"][1], "", "", "sys")]


def test_the_tree_never_opens_a_local_manuscript(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    host = _Host(app)
    assert host._browse_open_shelf_pick({"sys_id": "9700000001", "shelfmark": "x", "placement_id": "p"}) is None
    assert host.loads == []
    host.current_browse_sid = "970000000123"
    host._sync_browse_shelf_place()
    assert host.browse_shelf_place.isHidden() and host._browse_shelf_step("970000000123") is None


def test_with_the_file_missing_the_button_and_the_place_are_absent(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, None)
    host = _Host(app, sid=SYS["as2"][0])
    host.btn_browse_shelf.setVisible(True)
    host._on_shelf_tree_opened(False)
    assert host.btn_browse_shelf.isHidden() and host.browse_shelf_place.isHidden()
    assert host.open_shelf_picker() is None
    _served(monkeypatch, svc)
    host._on_shelf_tree_opened(True)
    assert not host.btn_browse_shelf.isHidden() and not host.browse_shelf_place.isHidden()
    assert host.browse_shelf_place.text().count('href="node:') == 3     # library, collection, volume
    assert "1 of 12" in host.browse_shelf_place.text()


def test_previous_and_next_follow_the_shelf_and_say_where(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    host = _Host(app, sid=SYS["as2"][2])
    host.meta_mgr = type("M", (), {"get_part_for_folio": lambda _s, _sid: None,
                                   "get_meta_for_id": lambda _s, sid: ("shelf of " + sid, "")})()
    host.searcher = type("S", (), {"get_adjacent_sys_id_by_file_order": lambda *_a: pytest.fail("file order used")})()
    _no_boxes(monkeypatch, app, host)
    host._sync_browse_shelf_place()
    nxt = svc.neighbours(svc.path_of(SYS["as2"][2])[0].leaf.id)[1]
    assert host._browse_ms_tooltip(1) == f"Next on the shelf: {nxt.shelfmark}"
    host.navigate_manuscript(1)
    assert host.loads[-1][0] == nxt.sys_id and host.current_browse_sid == nxt.sys_id
    assert host._browse_shelf["leaf"].id == nxt.id             # the placement stepped to
    # at the library's end the reader is told, and nothing moves
    last = svc.leaves("other:lib:1", 0, 10)[0][-1]
    host.current_browse_sid = last.sys_id
    host._sync_browse_shelf_place()
    loads = len(host.loads)
    host.navigate_manuscript(1)
    assert host.infos[-1] == "At the end of the shelf" and len(host.loads) == loads
    assert host._browse_ms_tooltip(1) == "At the end of the shelf"


def test_without_a_place_or_a_tree_previous_and_next_keep_the_file_order(svc, monkeypatch):
    app = _app_module(monkeypatch)
    host = _Host(app, sid="990099999990205171")               # not in the tree
    host.meta_mgr = type("M", (), {"get_part_for_folio": lambda _s, _sid: None,
                                   "get_meta_for_id": lambda _s, sid: ("Unknown", "")})()
    host.searcher = type("S", (), {"get_adjacent_sys_id_by_file_order": lambda _s, sid, d: SYS["kiev"][0]})()
    for served in (svc, None):
        _served(monkeypatch, served)
        host.current_browse_sid = "990099999990205171"
        host._sync_browse_shelf_place()
        assert host._browse_ms_tooltip(1) == "Next Manuscript (File Order)"
        host.navigate_manuscript(1)
        assert host.loads[-1][0] == SYS["kiev"][0]


def test_an_oxford_part_keeps_its_own_walk(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    host = _Host(app, sid=SYS["as2"][2])
    host.current_browse_part_id = "MS. Heb. b. 10/43"
    host.current_browse_part_folios = [SYS["as2"][2], SYS["kiev"][1]]
    assert host._browse_ms_tooltip(1) == "Next manuscript"
    walked = []
    host.browse_load_page = lambda: walked.append(host.current_browse_sid)
    host._browse_refresh_pgp_for_page = lambda: None
    host._update_part_image_for_folio = lambda _i: None
    host.meta_mgr = type("M", (), {"get_meta_for_id": lambda _s, sid: ("Unknown", "")})()
    host.navigate_manuscript(1)
    assert walked == [SYS["kiev"][1]]                          # the Part's next folio, not the shelf's


class _Pages:
    """A searcher: ``pages[sid]`` is its page count (0: metadata only); records every call."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get_browse_page(self, sid, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None):
        self.calls.append(dict(sid=sid, absolute_index=absolute_index, next_prev=next_prev,
                               allow_cross=allow_cross, volume_ie=volume_ie))
        total = self.pages.get(sid, 0)
        if total == 0:
            return {"sys_id": sid, "p_num": 1, "total_pages": 0, "current_idx": 1, "internal_index": 0,
                    "metadata_only": True}
        index = absolute_index if absolute_index is not None else 0
        index += next_prev
        if not 0 <= index < total:
            if allow_cross:
                return {"sys_id": "FILE-ORDER", "p_num": 1, "total_pages": 1, "current_idx": 1, "internal_index": 0}
            return None
        return {"sys_id": sid, "p_num": index + 1, "total_pages": total, "current_idx": index + 1,
                "internal_index": index}


def _crossing_host(app, svc, monkeypatch, sid, pages):
    host = _Host(app, sid=sid)
    host.searcher = _Pages(pages)
    codico = type("C", (), {"get_adjacent_part": lambda _s, _part, _d: None})()
    host.meta_mgr = type("M", (), {"get_meta_for_id": lambda _s, s: ("Unknown", ""),
                                   "get_folios_for_part": lambda _s, _p: [], "codico_mgr": codico})()
    host._update_part_state_for_sid = lambda _sid: None
    host.browse_render_page = lambda page: host.rendered.append(page)
    host._start_browse_enrichment = lambda *_a, **_k: None
    host._browse_refresh_pgp_for_page = lambda: None
    _no_boxes(monkeypatch, app, host)
    host._sync_browse_shelf_place()
    return host


def test_paging_past_the_last_page_goes_to_the_next_on_the_shelf(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    a = SYS["as2"][2]
    b = svc.neighbours(svc.path_of(a)[0].leaf.id)[1].sys_id
    host = _crossing_host(app, svc, monkeypatch, a, {a: 3, b: 5})
    host.current_browse_internal_idx = 2                       # its last page
    host.browse_navigate(1)
    assert host.rendered[-1]["sys_id"] == b and host.rendered[-1]["internal_index"] == 0
    assert host.current_browse_sid == b
    assert not any(c["allow_cross"] for c in host.searcher.calls)


def test_paging_back_lands_on_the_previous_manuscript_s_last_page(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    a = SYS["as2"][2]
    b = svc.neighbours(svc.path_of(a)[0].leaf.id)[0].sys_id
    host = _crossing_host(app, svc, monkeypatch, a, {a: 3, b: 5})
    host.current_browse_internal_idx = 0
    host.browse_navigate(-1)
    assert host.rendered[-1]["sys_id"] == b and host.rendered[-1]["internal_index"] == 4
    assert all(c["absolute_index"] is None or c["absolute_index"] >= 0 for c in host.searcher.calls)


def test_at_the_end_of_the_shelf_paging_stops_and_says_so(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    last = svc.leaves("other:lib:1", 0, 10)[0][-1].sys_id
    host = _crossing_host(app, svc, monkeypatch, last, {last: 2})
    host.current_browse_internal_idx = 1
    host.browse_navigate(1)
    assert host.infos == ["At the end of the shelf"] and host.rendered == []
    assert not any(c["allow_cross"] for c in host.searcher.calls)


def test_without_a_place_paging_crosses_in_the_file_order_as_before(svc, monkeypatch):
    app = _app_module(monkeypatch)
    for served in (svc, None):
        _served(monkeypatch, served)
        sid = "990099999990205171"
        host = _crossing_host(app, svc, monkeypatch, sid, {sid: 2})
        host.current_browse_internal_idx = 1
        host.browse_navigate(1)
        assert host.rendered[-1]["sys_id"] == "FILE-ORDER"
        assert host.searcher.calls[-1]["allow_cross"] is True


def test_a_manuscript_without_transcription_pages_does_not_cross(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    sid = SYS["as2"][2]
    host = _crossing_host(app, svc, monkeypatch, sid, {sid: 0})
    host.browse_navigate(1)
    assert host.rendered[-1]["sys_id"] == sid and host.rendered[-1].get("metadata_only")
    assert len(host.searcher.calls) == 1


def test_paging_inside_an_oxford_part_goes_to_its_next_folio(svc, monkeypatch):
    app = _app_module(monkeypatch)
    _served(monkeypatch, svc)
    a, folio = SYS["as2"][2], SYS["kiev"][1]
    host = _crossing_host(app, svc, monkeypatch, a, {a: 1, folio: 2})
    host.current_browse_part_id = "MS. Heb. b. 10/43"
    host.current_browse_part_folios = [a, folio]
    host.browse_navigate(1)
    assert host.rendered[-1]["sys_id"] == folio
    host.current_browse_sid = folio
    host.current_browse_part_folio_idx = 1
    host.current_browse_internal_idx = 1
    host.browse_navigate(1)                                   # the last Part: told, not crossed
    assert host.infos[-1] == "End of Parts in this volume."


def _self_calls_in(path, function):
    """The ``self.<name>(...)`` calls inside ``function`` of ``path``."""
    import ast
    from pathlib import Path
    tree = ast.parse((Path(__file__).resolve().parents[1] / path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function:
            return {n.func.attr for n in ast.walk(node) if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
                    and n.func.value.id == "self"}
    raise AssertionError(f"{function} not found in {path}")


def test_every_change_of_manuscript_reads_its_place_on_the_shelf(monkeypatch):
    # _sync_browse_cite_button runs wherever the manuscript Browse shows changes (a guard in
    # tests/test_web_links.py holds every assignment to it); the place on the shelf rides on it.
    app = _app_module(monkeypatch)

    class _Window:
        current_browse_sid = SYS["as2"][0]
        btn_b_cite = None
        synced = 0

        def _browse_web_url(self):
            return None

        def _sync_browse_shelf_place(self):
            self.synced += 1

    window = _Window()
    app.GenizahGUI._sync_browse_cite_button(window)
    assert window.synced == 1


def test_the_shelf_button_is_created_hidden_and_shown_by_the_opener():
    calls = _self_calls_in("genizah_app.py", "on_startup_finished")
    assert "_on_shelf_tree_opened" not in calls                  # connected, not called: it waits for the file
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / "genizah_app.py").read_text(encoding="utf-8")
    assert "self._shelf_tree_opener.opened.connect(self._on_shelf_tree_opened)" in source
    assert "self.btn_browse_shelf.setVisible(False)" in source


def test_the_installer_carries_the_tree_and_the_build_refuses_one_the_reader_would_refuse():
    # Packaging (shelf tree picker plan section 10.6); a direct PyInstaller run skips build_app.bat.
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    spec = (root / "GenizahSearchPro.spec").read_text(encoding="utf-8")
    gate = spec.index("verify_shelfmark_tree_sidecar.py")
    assert "--leaves-from-meta" in spec[gate:gate + 200]
    assert gate < spec.index("Analysis(")
    assert "raise SystemExit" in spec[gate:spec.index("Analysis(")]
    two_backslashes = "\\" * 2                                  # as the spec's Python literal writes it
    assert f"('shelfmark_tree_data{two_backslashes}shelfmark_tree.db', 'shelfmark_tree_data')" in spec
    bat = (root / "build_app.bat").read_text(encoding="utf-8")
    assert bat.index("shelfmark_tree.db is missing") < bat.index("python -m PyInstaller")
    from scripts.checkpoint_sidecars import SIDECARS
    assert "shelfmark_tree_data/shelfmark_tree.db" in SIDECARS


def test_in_hebrew_the_place_reads_right_to_left(svc, monkeypatch):
    # Seen in the app: unmarked, the Latin levels ran together left to right, in reverse order.
    app = _app_module(monkeypatch)
    monkeypatch.setattr(app, "CURRENT_LANG", "he")
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    _served(monkeypatch, svc)
    host = _Host(app, sid=SYS["as2"][0])
    host._sync_browse_shelf_place()
    text = host.browse_shelf_place.text()
    assert text.count("</a>\u200f \u203a \u200f<a ") == 2               # each separator held to the line
    assert "\u200f \u00b7 \u200f" in text                              # and the position too
    assert ">\u200eT-S AS 2\u200e</a>" in text                         # a Latin level in its own direction


def test_every_row_is_as_tall_as_its_name_at_the_width_it_is_drawn_at(svc):
    # Seen in the app: rows measured during a narrower first layout kept two lines' height for
    # one-line names ("T-S 6F - 8K"), while "Oriental" got one.
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        _wait(lambda: False, timeout=0.2)
        for column in _columns(dialog):
            table = column.table
            heights = {table.rowHeight(r): table.sizeHintForRow(r) for r in range(table.rowCount())}
            assert all(h == hint for h, hint in heights.items()), (column.key, heights)
        dialog.resize(700, 700)                                  # narrower: names may wrap now
        _wait(lambda: False, timeout=0.2)
        for column in _columns(dialog):
            table = column.table
            assert all(table.rowHeight(r) == table.sizeHintForRow(r) for r in range(table.rowCount())), column.key
            if column.chosen_row is not None:                    # and the chosen row is still in sight
                rect = table.visualItemRect(table.item(column.chosen_row, 0))
                assert table.viewport().rect().contains(rect.center()), column.key
    finally:
        _close(dialog)


def test_a_chosen_row_is_drawn_by_its_row_not_marked_cell_by_cell(monkeypatch):
    # Seen in the app: the Windows 11 style marks every selected cell with a bar at its start (one
    # before the name, another before the count). The style is told neither "selected" nor "current".
    from PyQt6.QtCore import QModelIndex
    from PyQt6.QtGui import QImage, QPainter
    from PyQt6.QtWidgets import QStyle, QStyledItemDelegate, QStyleOptionViewItem
    told = []
    monkeypatch.setattr(QStyledItemDelegate, "paint", lambda self, painter, option, index: told.append(option.state))
    delegate = picker._RowDelegate()
    option = QStyleOptionViewItem()
    option.state = QStyle.StateFlag.State_Selected | QStyle.StateFlag.State_HasFocus | QStyle.StateFlag.State_Enabled
    image = QImage(40, 20, QImage.Format.Format_ARGB32)
    painter = QPainter(image)
    try:
        delegate.paint(painter, option, QModelIndex())
    finally:
        painter.end()
    assert told and not told[0] & QStyle.StateFlag.State_Selected and not told[0] & QStyle.StateFlag.State_HasFocus


def test_the_chosen_row_stays_in_sight_when_the_rows_are_measured_again(svc):
    # The rows are measured again once the columns have their widths; the chosen row, scrolled to
    # at the first heights, must still be in sight at the new ones.
    dialog = _open(svc)
    try:
        column = dialog.node_columns[0]
        rows = [{"id": f"n{i}", "label": f"Collection number {i} with a long name", "count": i,
                 "on": i == 250, "title": ""} for i in range(300)]
        column.setMinimumWidth(100)
        column.setMaximumWidth(2000)
        column.table.setFixedWidth(900)
        column.show_column({"depth": 0, "parent": None, "title": "T", "rows": rows, "more": 0, "filter_key": "n:test"})
        column.scroll_to(None)
        _wait(lambda: False, timeout=0.2)
        column.table.setFixedWidth(160)                          # the names wrap: every row grows
        _wait(lambda: False, timeout=0.3)
        table = column.table
        assert table.rowHeight(0) > table.fontMetrics().height() + picker._RowDelegate.PAD  # several lines now
        rect = table.visualItemRect(table.item(250, 0))
        assert table.viewport().rect().intersects(rect), (rect, table.viewport().rect())
        # once the reader scrolls the list, a later measure leaves the list where they put it
        QTest.mouseClick(table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         table.viewport().rect().center())
        table.verticalScrollBar().setValue(0)
        column.table.setFixedWidth(300)
        _wait(lambda: False, timeout=0.3)
        assert table.verticalScrollBar().value() == 0
    finally:
        _close(dialog)
