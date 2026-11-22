# -*- coding: utf-8 -*-
"""The desktop shelf picker's round 4: the shelfmarks' grid lays out where Qt lays it out (no endless
relayout as its scroll bar comes and goes, no blank first line), its rows as tall as the columns'; an
unticked box shows on a dark theme; Go to opens Browse's picker, switch and all, and closes once that
picker has opened several.

The real dialog on the fixture tree. The language is pinned per test and the four profile variables
point at scratch folders.
"""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QEvent, QObject, Qt
from PyQt6.QtGui import QColor, QPalette
from PyQt6.QtWidgets import QApplication, QListWidget, QListWidgetItem, QStyleFactory, QTableWidget, QTableWidgetItem

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_service import ShelfLeaf, ShelfmarkTreeService
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
    for hook in ("title_of", "catalogue", "page_count_of", "library_code_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)
    yield
    _APP.setLayoutDirection(Qt.LayoutDirection.LeftToRight)


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
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


def _turns(seconds):
    _wait(lambda: False, timeout=seconds)


def _open(svc, start=None, width=1240, height=760, **kw):
    dialog = picker.ShelfPickerDialog(svc, start=start, **kw)
    dialog.resize(width, height)
    dialog.show()
    assert _wait(lambda: dialog._view is not None and not dialog._loads.busy()), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _leaves(labels, node="vol:test"):
    rows = [{"leaf": ShelfLeaf(id=f"p{i}", node_id=node, library_id="lib:test", sys_id=f"99{i:08d}",
                               shelfmark=label, shelf_label="", label=label, part="", inventory_id=None,
                               how="fist", pos=i),
             "label": label} for i, label in enumerate(labels)]
    return {"rows": rows, "total": len(rows), "title": "Test", "node": node, "filter_key": f"l:{node}", "more": 0}


class _BarCount(QObject):
    def __init__(self):
        super().__init__()
        self.n = 0

    def eventFilter(self, obj, event):
        if event.type() in (QEvent.Type.Show, QEvent.Type.Hide):
            self.n += 1
        return False


def _first_row(grid, upto):
    """The tops of the first ``upto`` items, as Qt laid them out."""
    return [grid.visualItemRect(grid.item(i)).top() for i in range(min(upto, grid.count()))]


# -- the grid: Qt's room, not the viewport's ----------------------------------------------------------

def test_the_grid_settles_when_its_scroll_bar_would_come_and_go(svc):
    # Seen in the app (Schoeyen, 26 shelfmarks): measured on the viewport, three columns fitted while the
    # scroll bar was hidden, but Qt keeps the bar's room and laid out two; the rows outgrew the view,
    # the bar came, three fitted again, the bar went... the shelfmarks jumped without end.
    dialog = _open(svc, start=SYS["as2"][0], width=1900)
    try:
        panel, grid = dialog.leaf_panel, dialog.leaf_panel.grid
        panel.fill(_leaves(["1"]))
        _turns(0.2)
        columns, row = panel.columns(), panel._row_height
        assert columns >= 2, "the test needs room for two columns at least"
        fit = grid.maximumViewportSize().height() // row
        assert fit >= columns
        # as many as fill the view's rows exactly at `columns` a row: at one column less they overflow
        labels = [str(n) for n in range(columns * fit)]
        counter = _BarCount()
        grid.verticalScrollBar().installEventFilter(counter)
        panel.fill(_leaves(labels))
        _turns(0.3)
        counter.n = 0
        _turns(0.6)
        assert counter.n == 0, f"the scroll bar came and went {counter.n} times after the grid settled"
        assert not grid.verticalScrollBar().isVisible()
        # and Qt put as many in a row as the grid was measured for
        tops = _first_row(grid, columns + 1)
        assert tops[:columns] == [0] * columns and tops[columns] > 0, tops
    finally:
        _close(dialog)


@pytest.mark.parametrize("count", [1, 5])
def test_the_first_shelfmark_stands_on_the_first_line(svc, count):
    # Seen in the app (T-S C4, the Lewis-Gibson collection): with one column the cell, measured on the
    # viewport, was wider than the room Qt lays out in, so the first shelfmark went down a line.
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        panel, grid = dialog.leaf_panel, dialog.leaf_panel.grid
        long = "T-S AS 2." + "1" * 60                                    # one column, whatever the width
        panel.fill(_leaves([f"{long}{i}" for i in range(count)]))
        _turns(0.3)
        assert panel.columns() == 1
        assert _first_row(grid, 1) == [0]
        assert grid.gridSize().width() <= panel.flow_width()
    finally:
        _close(dialog)


@pytest.mark.parametrize("mode", ["single", "multi"])
def test_the_shelfmarks_rows_are_as_tall_as_the_columns_rows(svc, mode):
    # Seen in the app (T-S C4): the shelfmarks' rows two pixels taller than the columns' beside them.
    # Multiple choice with boxes on the nodes too (the exclusion pickers): a box may be taller than a
    # line of text, and a row with one is as tall as it needs
    kw = ({"mode": "multi", "purpose": "test", "on_confirm": lambda e, l: None, "node_boxes": True}
          if mode == "multi" else {})
    dialog = _open(svc, start=SYS["as2"][0], **kw)
    try:
        grid = dialog.leaf_panel.grid
        assert grid.count()
        heights = []
        for column in dialog.node_columns:
            if column.isVisible():
                table = column.table
                heights += [table.rowHeight(r) for r in range(table.rowCount())
                            if "\n" not in (table.item(r, column.name_col).text() if table.item(r, column.name_col) else "\n")]
        assert heights
        one_line = min(heights)
        assert grid.gridSize().height() == one_line
        assert grid.visualItemRect(grid.item(0)).height() == one_line
    finally:
        _close(dialog)


# -- an unticked box on a dark theme --------------------------------------------------------------------

def _palette(dark):
    pal = QPalette()
    colours = ({"Window": "#1e1e1e", "Base": "#2d2d2d", "Text": "#ffffff", "WindowText": "#ffffff",
                "Button": "#2d2d2d", "ButtonText": "#ffffff"} if dark else
               {"Window": "#efefef", "Base": "#ffffff", "Text": "#000000", "WindowText": "#000000",
                "Button": "#efefef", "ButtonText": "#000000"})
    for role, colour in colours.items():
        pal.setColor(getattr(QPalette.ColorRole, role), QColor(colour))
    return pal


def _box_contrast(view_kind, dark):
    """The most a pixel of an unticked, unlabelled box differs in lightness from the list's background,
    drawn by the picker's delegate in Fusion, the desktop's style."""
    fusion = QStyleFactory.create("Fusion")
    if view_kind == "table":
        view = QTableWidget(1, 1)
        item = QTableWidgetItem("")
        view.setItem(0, 0, item)
    else:
        view = QListWidget()
        item = QListWidgetItem("")
        view.addItem(item)
    item.setFlags(picker._BOX_FLAGS)
    item.setCheckState(Qt.CheckState.Unchecked)
    view.setStyle(fusion)
    view.setPalette(_palette(dark))
    view.setItemDelegate(picker._RowDelegate(view))
    view.resize(240, 120)
    view.show()
    _turns(0.1)
    rect = view.visualItemRect(item)
    image = view.viewport().grab().toImage()
    base = _palette(dark).color(QPalette.ColorRole.Base).lightness()
    most = max(abs(image.pixelColor(x, y).lightness() - base)
               for x in range(rect.left(), min(rect.right(), image.width() - 1))
               for y in range(rect.top(), min(rect.bottom(), image.height() - 1)))
    view.close()
    view.deleteLater()
    return most, fusion


@pytest.mark.parametrize("view_kind", ["table", "list"])
def test_an_unticked_box_shows_on_a_dark_theme(view_kind):
    # Seen in the app: the exclusion picker's node boxes (a table) and the shelfmarks' (a list) were
    # invisible until ticked; Fusion draws the outline in the window's colour, darkened
    most, _style = _box_contrast(view_kind, dark=True)
    assert most >= 100, f"the box's outline is {most} from the background"


@pytest.mark.parametrize("view_kind", ["table", "list"])
def test_a_light_theme_keeps_its_own_box(view_kind):
    light = _palette(dark=False)
    assert picker.box_palette(light) is light                             # left as it is
    most, _style = _box_contrast(view_kind, dark=False)
    assert most >= 50


# -- Go to: Browse's picker ------------------------------------------------------------------------------

def test_go_to_opens_browse_s_picker_with_its_switch(svc, monkeypatch):
    import genizah_app
    gui = genizah_app.GenizahGUI
    boxes = []

    class _Box:
        def __init__(self, *_a, **kw):
            self.kw = kw
            boxes.append(self)

        def exec(self):
            return True

        def choice(self):
            return ("opened", [SYS["as2"][0]])

        def deleteLater(self):
            pass

    monkeypatch.setattr(genizah_app, "GoToDialog", _Box)
    monkeypatch.setattr(genizah_app.startup_tips, "mark_used", lambda *_a: None)
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: svc)
    acted = []
    win = SimpleNamespace(lists_mgr=None, _shelf_selections=None, searcher=None,
                          _browse_shelf_start=lambda typed=True: None, _go_to_recent_items=lambda: [],
                          _browse_rd_images_drawn=lambda: 0, _lists_after_shelf_save=lambda: None,
                          go_to=lambda v: acted.append(("go", v)), _open_recent_item=lambda v: acted.append(v),
                          _browse_open_shelf_pick=lambda v: acted.append(v),
                          _shelf_add_to_puzzle=lambda v: acted.append(v))
    for name in ("open_go_to", "_shelf_picker_options", "_shelf_choice_limit", "_shelf_open_actions",
                 "_browse_open_several_from_shelf"):
        setattr(win, name, getattr(gui, name).__get__(win))
    gui.open_go_to(win)
    options = boxes[0].kw["shelf_options"]
    assert options["switch_label"] == "Open more than one" and options["purpose"] == "reading_desk"
    assert options["on_confirm"] == win._browse_open_several_from_shelf
    assert options["act_label"] == "Open" and [a.key for a in options["actions"]] == ["puzzle"]
    assert options == win._shelf_picker_options() | {"limit": options["limit"], "actions": options["actions"]}
    assert acted == []                                                    # opened already: nothing more


@pytest.mark.parametrize("taken, closes", [([SYS["as2"][1]], [SYS["as2"][1]]), (None, None), ([], False)])
def test_go_to_closes_once_its_picker_has_opened_several(svc, monkeypatch, taken, closes):
    from desktop.go_to_dialog import GoToDialog
    entries = [{"sys_id": s, "shelfmark": s, "placement_id": ""} for s in SYS["as2"][:2]]
    hosted, seen = [], {}

    def host(entries_, label):
        hosted.append((entries_, label))
        return taken

    def fake(parent, service, **kw):
        seen.update(kw)
        kw["on_confirm"](entries, "T-S AS 2")                             # the switch on, two ticked, confirmed
        return None                                                       # closed by the confirm
    monkeypatch.setattr(picker, "pick_from_shelf_or_several", fake)
    box = GoToDialog(None, shelf_service=svc, shelf_start="vol:100",
                     shelf_options={"switch_label": "Open more than one", "purpose": "reading_desk",
                                    "on_confirm": host})
    try:
        box.shelf_button.click()
        assert hosted == [(entries, "T-S AS 2")]                         # the window's own confirm
        assert seen["switch_label"] == "Open more than one" and seen["act_label"] == "Open"
        if closes is False:                                               # nothing opened: the box stays
            assert box.result() != box.DialogCode.Accepted and box.choice()[0] == "text"
        else:
            expected = closes if closes is not None else [e["sys_id"] for e in entries]
            assert box.result() == box.DialogCode.Accepted and box.choice() == ("opened", expected)
    finally:
        box.deleteLater()


# -- the details pane as labelled fields ---------------------------------------------------------------

def _plain(text):
    import re
    return re.sub(r"<[^>]+>", "", text).replace("‎", "").replace("‏", "")


def _field(pane, key):
    from PyQt6.QtWidgets import QLabel
    label = pane.lines_box.findChild(QLabel, f"field_{key}")
    return _plain(label.text()) if label is not None else None


def _click_item(dialog, sid):
    from PyQt6.QtTest import QTest
    grid = dialog.leaf_panel.grid
    item = next(grid.item(i) for i in range(grid.count()) if grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id == sid)
    QTest.mouseClick(grid.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                     grid.visualItemRect(item).center())


HEBREW = ("שורה ראשונה של הטקסט ובה מילים ארוכות למדי, ושורה שנייה של אותו הטקסט ממש\n"
          "ושורה שלישית שלו")
ENGLISH = "First line of the text, long enough to read as text\nsecond line\n\nfourth line after a blank one"


def test_the_text_box_holds_each_page_as_stored_under_its_number(svc, monkeypatch):
    monkeypatch.setattr(picker, "page_count_of", lambda sid: 3)
    monkeypatch.setattr(picker, "page_texts_of", lambda sid: iter(["", ENGLISH, HEBREW]))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.text is not None and pane.textbox.isVisibleTo(dialog))
        text = pane.textbox.toPlainText()
        assert "Page 2\n" + ENGLISH in text and "Page 3\n" + HEBREW in text       # as stored, lines and all
        assert "Page 1" not in text                                         # a page without text is not shown
        assert pane.textbox.isReadOnly()
        directions = {}
        block = pane.textbox.document().begin()
        while block.isValid():
            directions.setdefault(block.text(), block.blockFormat().layoutDirection())
            block = block.next()
        assert directions[ENGLISH.splitlines()[0]] == Qt.LayoutDirection.LeftToRight
        assert directions[HEBREW.splitlines()[0]] == Qt.LayoutDirection.RightToLeft    # its own direction
        lines = pane.textbox.height() / pane.textbox.fontMetrics().lineSpacing()
        assert 10 <= lines <= 13                                            # about 10-12 lines high
        assert _field(pane, "pages") == "Pages: 3 · With text: 2"
        assert "<b>Pages:</b>" in pane.lines_box.findChild(type(pane.links), "field_pages").text()
    finally:
        _close(dialog)


class _Community:
    def __init__(self, partners, comments=3):
        self.partners, self.comments = partners, comments

    def get_connected_fragments_by_id(self, sid):
        from desktop.corrections_client import ConnectedFragments, FragmentJoin
        joins = [FragmentJoin(id=f"j{i}", fragment_a="T-S AS 2.1", fragment_b=shelf, document_id_a=sid,
                              document_id_b=other, relationship_type="join", notes="", source="user",
                              created_by_username="", created_at=None)
                 for i, (other, shelf) in enumerate(self.partners)]
        return ConnectedFragments(shelfmark="T-S AS 2.1", shelfmark_normalized="", joins=joins)

    def get_document_stats(self, sid):
        return {"comments_count": self.comments, "corrections_count": 0}


def _switch_picker(svc, **kw):
    return _open(svc, start="vol:100", switch_label="Open more than one", purpose="test",
                 on_confirm=lambda e, l: None, **kw)


def test_a_join_partner_is_a_link_to_its_place_and_a_box_that_ticks_it(svc, monkeypatch):
    partner = SYS["as10"][0]
    monkeypatch.setattr(picker, "community", _Community([(partner, "T-S AS 10.1"),
                                                         ("990000999990205171", "ENA 123.4")]))
    dialog = _switch_picker(svc)
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: partner in pane.join_links)
        assert _field(pane, "comments") == "Comments: 3"
        assert set(pane.join_boxes) == {partner}                           # a partner off the shelf: no box ...
        assert "990000999990205171" not in pane.join_links                 # ... and no link, its name only
        texts = [_plain(label.text()) for label in pane.joins_box.findChildren(type(pane.links))]
        assert "ENA 123.4" in texts and "Community" in texts               # the sources, small
        # the box ticks it as the lists' box would, the multiple choice turned on first
        assert not dialog.multi
        pane.join_boxes[partner].click()
        assert dialog.multi and dialog.switch.isChecked()
        members = picker.members_function(svc)
        assert partner in dialog.chosen().chosen_set(members) and pane.join_boxes[partner].isChecked()
        assert _wait(lambda: dialog._paint_state is not None and partner in dialog._paint_state["chosen"])
        pane.join_boxes[partner].click()                                   # and unticking takes it out
        assert partner not in dialog.chosen().chosen_set(members) and not pane.join_boxes[partner].isChecked()
    finally:
        _close(dialog)


@pytest.mark.parametrize("mode", ["single", "multi"])
def test_a_partner_s_link_goes_to_it_in_the_columns(svc, monkeypatch, mode):
    partner = SYS["as10"][0]
    place = svc.path_of(partner)[0].leaf
    monkeypatch.setattr(picker, "community", _Community([(partner, "T-S AS 10.1")]))
    dialog = _switch_picker(svc)
    try:
        if mode == "multi":
            dialog.switch.setChecked(True)
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: partner in pane.join_links)
        pane.join_links[partner].linkActivated.emit(partner)
        assert _wait(lambda: dialog.leaf_panel.leaves is not None and dialog.leaf_panel.leaves["node"] == place.node_id
                     and pane.text is not None and pane.text["sys_id"] == partner)
        current = dialog.leaf_panel.grid.currentItem()
        assert current is not None and current.data(Qt.ItemDataRole.UserRole).sys_id == partner   # chosen
        members = picker.members_function(svc)
        if mode == "single":
            assert dialog._chosen_leaf.sys_id == partner and dialog.act_button.isEnabled()
        else:
            assert partner not in dialog.chosen().chosen_set(members)      # shown, never ticked by a link
    finally:
        _close(dialog)


def test_a_partner_s_link_clears_a_filter_that_would_hide_it(svc, monkeypatch):
    own, partner = SYS["as2"][0], SYS["as2"][3]
    monkeypatch.setattr(picker, "community", _Community([(partner, "T-S AS 2.4")]))
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        panel.filter.setText("2.1")                                         # typed: only T-S AS 2.1 shown
        dialog.set_filter(panel.key, "2.1")
        assert _wait(lambda: not dialog._loads.busy() and [panel.grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id
                                                            for i in range(panel.grid.count())] == [own])
        _click_item(dialog, own)
        assert _wait(lambda: partner in dialog.details.join_links)
        dialog.details.join_links[partner].linkActivated.emit(partner)
        assert _wait(lambda: dialog.details.text is not None and dialog.details.text["sys_id"] == partner
                     and not dialog._loads.busy())
        current = panel.grid.currentItem()
        assert current is not None and current.data(Qt.ItemDataRole.UserRole).sys_id == partner
        assert panel.filter.text() == "" and dialog._filters.get(panel.key, "") == ""
    finally:
        _close(dialog)


@pytest.mark.parametrize("mode", ["single", "multi"])
def test_ticking_a_join_partner_ticks_the_shelfmark_shown_too(svc, monkeypatch, mode):
    # The fragments of a join are chosen together: ticking a partner in the pane ticks the shelfmark
    # whose pane it is as well, first; unticking the partner leaves it chosen
    own, partner = SYS["as2"][0], SYS["as10"][0]
    monkeypatch.setattr(picker, "community", _Community([(partner, "T-S AS 10.1")]))
    dialog = _switch_picker(svc)
    try:
        if mode == "multi":
            dialog.switch.setChecked(True)
            assert _wait(lambda: dialog.multi and not dialog._loads.busy())
            dialog._toggle_leaf(dialog.details.place_leaf(own))           # ticked, then unticked by its click
        pane = dialog.details
        _click_item(dialog, own)
        assert _wait(lambda: partner in pane.join_boxes)
        members = picker.members_function(svc)
        assert own not in dialog.chosen().chosen_set(members)
        pane.join_boxes[partner].click()
        chosen = dialog.chosen()
        assert chosen.chosen_set(members) == {own, partner}
        assert [e["sys_id"] for e in chosen.resolve_entries(svc)] == [own, partner]   # it first
        assert _wait(lambda: dialog._paint_state is not None and dialog._paint_state["count"] == 2)
        box = next(dialog.leaf_panel.grid.item(i) for i in range(dialog.leaf_panel.grid.count())
                   if dialog.leaf_panel.grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id == own)
        assert box.checkState() == Qt.CheckState.Checked                   # its own box in the list
        pane.join_boxes[partner].click()
        assert dialog.chosen().chosen_set(members) == {own}
    finally:
        _close(dialog)


def test_a_picker_that_chooses_one_only_has_links_and_no_boxes(svc, monkeypatch):
    partner = SYS["as10"][0]
    monkeypatch.setattr(picker, "community", _Community([(partner, "T-S AS 10.1")]))
    dialog = _open(svc, start="vol:100")
    try:
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: partner in dialog.details.join_links)
        assert dialog.details.join_boxes == {}
    finally:
        _close(dialog)


def test_the_links_row_is_labelled_and_never_cut(svc, monkeypatch):
    links = [{"key": k, "label": label, "url": f"https://example.org/{k}/" + "x" * 30}
             for k, label in (("ktiv", "Ktiv"), ("cudl", "Cambridge Digital Library"), ("luna", "Manchester LUNA"),
                              ("pgp", "Princeton Geniza Project"))]
    real = picker.item_details

    def details(*a, **k):
        out = real(*a, **k)
        out["links"] = links
        return out
    monkeypatch.setattr(picker, "item_details", details)
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.links.isVisibleTo(dialog))
        text = _plain(pane.links.text())
        assert text.startswith("Links:") and "Cambridge Digital Library" in text and "Manchester LUNA" in text
        assert pane.links.wordWrap()
        _turns(0.1)
        viewport = pane.scroll.viewport()
        assert pane.links.geometry().right() <= viewport.width()           # wrapped inside the pane, never cut
        assert pane.links.height() >= pane.links.heightForWidth(pane.links.width())
    finally:
        _close(dialog)


# -- the signs on the rows ------------------------------------------------------------------------------

def test_the_rows_carry_their_signs_once_loaded_without_blocking(svc, monkeypatch):
    import threading
    from shared.shelfmark_tree_signs import ShelfSigns
    gate = threading.Event()
    a, b, c = SYS["as2"][:3]

    def joins():
        gate.wait(5)
        return [b]
    source = ShelfSigns(loaders={"pgp": lambda: [a], "edition": lambda: [a, b], "join": joins})
    monkeypatch.setattr(picker, "signs", source)
    dialog = _open(svc, start="vol:100")                                # drawn while the signs load
    try:
        grid = dialog.leaf_panel.grid
        role = picker._SIGNS_ROLE

        def item(sid):
            return next(grid.item(i) for i in range(grid.count())
                        if grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id == sid)
        assert not source.ready and item(a).data(role) is None
        width = grid.gridSize().width()
        gate.set()
        assert _wait(lambda: item(a).data(role) is not None)
        assert item(a).data(role) == ("pgp", "edition") and item(b).data(role) == ("edition", "join")
        assert item(c).data(role) is None
        assert "PGP · Scholarly transcription" in item(a).toolTip()
        # a scholarly transcription is the app's document icon (as "View Document"), not a check
        assert picker.SIGN_MARKS["edition"] == ("\U0001F4C4", None)
        assert grid.gridSize().width() == width                            # their room was kept: nothing moves
        from PyQt6.QtGui import QFont, QFontMetrics
        bold = QFont(grid.font())
        bold.setBold(True)
        widest = max(QFontMetrics(bold).horizontalAdvance(grid.item(i).text()) for i in range(grid.count()))
        assert grid.visualItemRect(item(a)).width() >= widest + picker.signs_room(grid.font())   # beside, never over
        # drawn: the results' green "PGP" at the row's end
        _turns(0.1)
        image = grid.viewport().grab().toImage()
        rect = grid.visualItemRect(item(a))

        def is_green(colour):
            return colour.green() > 120 and colour.red() < 110 and colour.blue() < 140
        green = [x for x in range(rect.left(), min(rect.right(), image.width() - 1))
                 for y in range(rect.top(), min(rect.bottom(), image.height() - 1)) if is_green(image.pixelColor(x, y))]
        assert green and min(green) > rect.left() + rect.width() // 2
    finally:
        _close(dialog)
    dialog = _open(svc, start="vol:100")                                # loaded: drawn with the rows
    try:
        grid = dialog.leaf_panel.grid
        found = {grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id: grid.item(i).data(picker._SIGNS_ROLE)
                 for i in range(grid.count())}
        assert found[a] == ("pgp", "edition")
    finally:
        _close(dialog)


def test_the_window_gives_the_pickers_the_signs():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "genizah_app.py").read_text(encoding="utf-8")
    body = src[src.index("    def _on_shelf_tree_opened("):src.index("    def _shelf_title(")]
    assert "shelf_picker.signs = shelf_signs()" in body


def test_on_a_chosen_row_the_signs_are_drawn_lighter():
    # Seen in the app: the results' green "PGP" on the highlight of the chosen row did not read
    from PyQt6.QtCore import QRect
    from PyQt6.QtGui import QImage, QPainter
    from PyQt6.QtWidgets import QStyleOptionViewItem

    def brightest(selected):
        image = QImage(200, 30, QImage.Format.Format_ARGB32)
        image.fill(QColor("#527a8e"))
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, 200, 30)
        option.font = _APP.font()
        option.direction = Qt.LayoutDirection.LeftToRight
        painter = QPainter(image)
        try:
            picker._draw_signs(painter, option, ("pgp",), selected)
        finally:
            painter.end()
        return max(image.pixelColor(x, y).lightness() for x in range(200) for y in range(30))
    assert brightest(True) > brightest(False) + 30


def test_the_desktop_never_makes_the_crossref_service_for_one_thread_only():
    # Seen in the real app: the window made the process's one NLI crossref service with a
    # single-thread connection, and the shelf picker's details, read off the UI thread, lost every
    # library viewer link (Cambridge Digital Library, Manchester) to "SQLite objects created in a
    # thread can only be used in that same thread"
    import re
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    calls = []
    for path in [root / "genizah_app.py", *sorted((root / "desktop").rglob("*.py"))]:
        text = path.read_text(encoding="utf-8")
        calls += [(path.name, m.group(0)) for m in re.finditer(r"get_nli_crossref_service\([^)]*\)", text)]
    assert calls
    assert all(call == "get_nli_crossref_service(thread_safe=True)" for _name, call in calls), calls


def test_back_to_one_the_shelfmark_chosen_is_the_one_clicked_last(svc):
    # Codex #87: A chosen, the switch on, B clicked, the switch off: the pane showed B, Open took A
    a, b = SYS["as2"][0], SYS["as2"][2]
    dialog = _switch_picker(svc)
    try:
        _click_item(dialog, a)
        assert dialog._chosen_leaf.sys_id == a
        dialog.switch.setChecked(True)
        assert _wait(lambda: dialog.multi and not dialog._loads.busy())
        _click_item(dialog, b)
        assert _wait(lambda: dialog.details.text is not None and dialog.details.text["sys_id"] == b)
        dialog.switch.setChecked(False)
        assert _wait(lambda: not dialog.multi and not dialog._loads.busy())
        assert dialog._chosen_leaf is not None and dialog._chosen_leaf.sys_id == b
        current = dialog.leaf_panel.grid.currentItem()
        assert current is not None and current.data(Qt.ItemDataRole.UserRole).sys_id == b
        assert dialog.act_button.isEnabled()
        dialog.act_button.click()
        assert dialog.selection()["sys_id"] == b                            # Open takes what the pane shows
    finally:
        _close(dialog)
