# -*- coding: utf-8 -*-
"""The desktop shelf picker's round 3 (DX19-DX28 and Codex's amendments): one breadcrumb separator in
both languages; the columns from the start edge with the details pane at the end (and away when the
window is narrow); a click chooses a shelfmark and a button takes it ("Open" where it opens a
manuscript), double-click and Enter too; "Add to List" (the picker stays open) and "Add to Puzzle"
(it closes; through Go to as well) where the picker opens manuscripts, on the shelfmark chosen or the
ones ticked, which stay ticked; no node boxes but in the exclusion pickers; titles in the tooltips;
the details pane, read off the UI thread, a newer click winning; Go to's shelf as an icon button at
the end of its box; Ctrl+W.

The real dialog on the fixture tree. The language is pinned per test and the four profile variables
point at scratch folders.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QPoint, Qt
from PyQt6.QtGui import QImage
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QApplication, QDialogButtonBox, QLabel, QToolButton

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_selection import Selection, members_function
from shared.shelfmark_tree_service import ShelfmarkTreeService
from shared.shelfmark_tree_view import CRUMB_SEP
from tests.shelfmark_tree_fixture import SYS, build_fixture

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
ROOT = Path(__file__).resolve().parents[1]


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


def _open(svc, start=None, width=1240, **kw):
    dialog = picker.ShelfPickerDialog(svc, start=start, **kw)
    dialog.resize(width, 760)
    dialog.show()
    assert _wait(lambda: dialog._view is not None and not dialog._loads.busy()), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _grid_items(dialog):
    grid = dialog.leaf_panel.grid
    return [grid.item(i) for i in range(grid.count())]


def _item_of(dialog, sid):
    return next(it for it in _grid_items(dialog) if it.data(Qt.ItemDataRole.UserRole).sys_id == sid)


def _click_item(dialog, item, double=False):
    grid = dialog.leaf_panel.grid
    point = grid.visualItemRect(item).center()
    if double:
        QTest.mouseDClick(grid.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, point)
    else:
        QTest.mouseClick(grid.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, point)


# -- DX19 the breadcrumb ----------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_breadcrumb_arrow_points_the_way_each_language_reads(svc, monkeypatch, lang):
    # a QLabel holding only the arrow has no direction of its own and draws it as written, unlike the
    # web's text, which mirrors CRUMB_SEP: each language writes its own arrow
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    if lang == "he":
        _APP.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = _open(svc, start=SYS["as2"][0])
    try:
        seps = [w.text() for w in dialog.crumbs_box.findChildren(QLabel) if w.text() in ("›", "‹")]
        assert seps and set(seps) == ({"‹"} if lang == "he" else {CRUMB_SEP})
    finally:
        _close(dialog)


# -- DX26 the columns from the start, DX25 the pane at the end --------------------------------------

@pytest.mark.parametrize("rtl", [False, True])
def test_the_columns_start_at_the_start_edge_and_the_pane_sits_at_the_end(svc, rtl):
    if rtl:
        _APP.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = _open(svc)                                                   # the libraries alone
    try:
        libraries = next(c for c in dialog.node_columns if c.isVisibleTo(dialog))
        _wait(lambda: libraries.width() > 0)
        left = libraries.mapTo(dialog, QPoint(0, 0)).x()
        right = left + libraries.width()
        pane = dialog.details.mapTo(dialog, QPoint(0, 0)).x()
        assert not dialog.leaf_panel.isVisibleTo(dialog) and dialog._filler.isVisibleTo(dialog)
        if rtl:
            assert right > dialog.width() - 40 and pane < left              # library column at the right edge
        else:
            assert left < 40 and pane > right                               # at the left edge
        assert libraries.width() <= 270                                     # its own width, not stretched
    finally:
        _close(dialog)


def test_a_narrow_window_leaves_the_pane_out(svc):
    dialog = _open(svc, width=980)
    try:
        assert not dialog.details.isVisible()
        dialog.resize(1240, 760)
        assert _wait(lambda: dialog.details.isVisible())
    finally:
        _close(dialog)


# -- DX21 a click chooses, a button takes --------------------------------------------------------------

def test_a_click_chooses_and_the_button_takes_it(svc):
    dialog = _open(svc, start="vol:100", act_label="Open")
    try:
        assert dialog.act_button.text() == "Open" and not dialog.act_button.isEnabled()
        item = _item_of(dialog, SYS["as2"][2])
        _click_item(dialog, item)
        assert dialog.isVisible() and dialog.selection() is None           # chosen, not opened
        assert dialog._chosen_leaf.sys_id == SYS["as2"][2] and dialog.act_button.isEnabled()
        assert dialog.leaf_panel.grid.currentItem() is item                  # shown chosen
        dialog.act_button.click()
        assert dialog.outcome() == ("open", dialog.selection()) and dialog.selection()["sys_id"] == SYS["as2"][2]
    finally:
        _close(dialog)


def test_a_double_click_and_enter_are_the_button(svc):
    dialog = _open(svc, start="vol:100")
    try:
        assert dialog.act_button.text() == "Select"                          # the default words
        _click_item(dialog, _item_of(dialog, SYS["as2"][3]), double=True)
        assert dialog.selection()["sys_id"] == SYS["as2"][3]
    finally:
        _close(dialog)
    dialog = _open(svc, start="vol:100")
    try:
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        grid.setCurrentItem(_item_of(dialog, SYS["as2"][4]))
        QTest.keyClick(grid, Qt.Key.Key_Return)
        assert dialog.selection()["sys_id"] == SYS["as2"][4]
    finally:
        _close(dialog)


def test_a_filter_s_shelfmark_is_chosen_by_a_click_and_taken_by_a_double_click(svc):
    dialog = _open(svc, start="col:10")
    try:
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and c.key == "n:col:10")
        dialog.set_filter(column.key, "2.4")
        assert _wait(lambda: not dialog._loads.busy() and any("leaf" in r for r in column.rows))
        row = next(i for i, r in enumerate(column.rows) if "leaf" in r and r["leaf"].sys_id == SYS["as2"][3])
        rect = column.table.visualRect(column.table.model().index(row, 0))
        QTest.mouseClick(column.table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         rect.center())
        assert dialog.selection() is None and dialog._chosen_leaf.sys_id == SYS["as2"][3]
        QTest.mouseDClick(column.table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                          rect.center())
        assert dialog.selection()["sys_id"] == SYS["as2"][3]
    finally:
        _close(dialog)


def test_a_single_choice_host_says_its_own_words(svc, monkeypatch):
    from desktop.widgets.shelf_pick_button import ShelfPickButton
    seen = []
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: svc)
    monkeypatch.setattr(picker, "pick_from_shelf", lambda *a, **k: seen.append(k.get("act_label")))
    ShelfPickButton(None, lambda _sel: None, act_label="Add").pick()
    ShelfPickButton(None, lambda _sel: None).pick()
    assert seen == ["Add", None]                                           # None: the picker's "Select"
    src = (ROOT / "desktop/corrections_ui.py").read_text(encoding="utf-8")
    assert 'act_label=tr("Add")' in src                                     # Share a Discovery's references


# -- DX22 Add to List and Add to Puzzle -----------------------------------------------------------------

def _actions(log):
    return [picker.ShelfAction("list", "Add to List", keep_open=True,
                               handler=lambda entries, dialog: log.append(("list", entries)) or "a note"),
            picker.ShelfAction("puzzle", "Add to Puzzle")]


def test_add_to_list_keeps_the_picker_open_and_add_to_puzzle_closes_it(svc):
    log = []
    dialog = _open(svc, start="vol:100", act_label="Open", actions=_actions(log))
    try:
        buttons = dialog.action_buttons
        assert set(buttons) == {"list", "puzzle"} and not any(b.isEnabled() for b in buttons.values())
        _click_item(dialog, _item_of(dialog, SYS["as2"][1]))
        assert all(b.isEnabled() for b in buttons.values())
        buttons["list"].click()
        assert dialog.isVisible() and [e["sys_id"] for e in log[0][1]] == [SYS["as2"][1]]
        assert dialog.note.text() == "a note"                               # what the host said (a limit)
        buttons["puzzle"].click()
        assert not dialog.isVisible()
        assert dialog.outcome() == ("puzzle", [log[0][1][0]])
    finally:
        _close(dialog)


def test_in_the_multiple_choice_the_actions_take_the_ticked_ones_which_stay_ticked(svc):
    log = []
    store = picker.SelectionStore()
    dialog = _open(svc, start="vol:100", mode="multi", purpose="reading_desk", store=store,
                   on_confirm=lambda *_a: None, act_label="Open", actions=_actions(log))
    try:
        leaves = [it.data(Qt.ItemDataRole.UserRole) for it in _grid_items(dialog)]
        assert not any(b.isEnabled() for b in dialog.action_buttons.values())   # nothing ticked yet
        dialog._toggle_leaf(leaves[0])
        dialog._toggle_leaf(leaves[1])
        assert _wait(lambda: all(b.isEnabled() for b in dialog.action_buttons.values()))
        dialog.action_buttons["list"].click()
        assert _wait(lambda: log and not dialog._busy)
        assert [e["sys_id"] for e in log[0][1]] == [leaves[0].sys_id, leaves[1].sys_id]
        assert store.load("reading_desk").count(members_function(svc)) == 2   # still ticked
        dialog.action_buttons["puzzle"].click()
        assert _wait(lambda: not dialog.isVisible())
        assert dialog.outcome()[0] == "puzzle" and len(dialog.outcome()[1]) == 2
        assert store.load("reading_desk").count(members_function(svc)) == 2   # still ticked
    finally:
        _close(dialog)


def test_go_to_hands_add_to_puzzle_back_to_the_window(svc, monkeypatch):
    from desktop.go_to_dialog import GoToDialog
    entries = [{"sys_id": SYS["as2"][0], "shelfmark": "T-S AS 2.1", "placement_id": "p"}]
    seen = {}

    def fake(parent, service, **kw):
        seen.update(kw)
        return ("puzzle", entries)
    monkeypatch.setattr(picker, "pick_from_shelf_or_several", fake)
    actions = _actions([])
    box = GoToDialog(None, shelf_service=svc, shelf_start="vol:100", shelf_actions=actions)
    try:
        box.shelf_button.click()
        assert box.result() == box.DialogCode.Accepted and box.choice() == ("puzzle", entries)
        assert seen["actions"] is actions and seen["act_label"] == "Open" and seen.get("switch_label") is None
    finally:
        box.deleteLater()


def _window(app_module, **attrs):
    gui = app_module.GenizahGUI
    win = SimpleNamespace(**{"LIST_SHELF_MAX": gui.LIST_SHELF_MAX, "status_label": QLabel(), **attrs})
    for name in ("_shelf_outcome", "_shelf_add_to_list", "_shelf_add_to_puzzle", "_shelf_open_actions"):
        setattr(win, name, getattr(gui, name).__get__(win))
    return win


def test_the_window_adds_to_a_list_within_its_limit_and_to_the_puzzle(svc, monkeypatch):
    import genizah_app
    menus, taken = [], []
    win = _window(genizah_app, lists_mgr=object(), LIST_SHELF_MAX=2,
                  show_add_to_list_menu=lambda items, source='', anchor_widget=None: menus.append(items),
                  _open_puzzle_window=lambda: None,
                  _puzzle_window=SimpleNamespace(take_entries_from_shelf=lambda e, s: taken.append(e) or True),
                  _browse_open_shelf_pick=lambda sel: taken.append(("open", sel)))
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: svc)
    entries = [{"sys_id": s, "shelfmark": s, "placement_id": ""} for s in SYS["as2"][:3]]
    note = win._shelf_add_to_list(entries)
    assert menus == [[{"sys_id": SYS["as2"][0]}, {"sys_id": SYS["as2"][1]}]]
    assert note == "Up to 2 manuscripts here"                              # the list's room said
    assert [a.key for a in win._shelf_open_actions()] == ["list", "puzzle"]
    assert win._shelf_outcome(("puzzle", entries)) is None and taken == [entries]
    assert win._shelf_outcome(("open", entries[0])) == entries[0] and taken[-1] == ("open", entries[0])


def test_the_puzzle_s_hand_off_waits_for_a_batch_still_loading_and_takes_it_after(monkeypatch):
    # Codex #87: the picker has closed, so a batch handed over while another loads is queued, never dropped
    from desktop import puzzle as puzzle_module
    from desktop.puzzle import PuzzleCanvasWindow
    calls, timers = [], []
    monkeypatch.setattr(puzzle_module.QTimer, "singleShot", lambda ms, fn: timers.append((ms, fn)))
    monkeypatch.setattr(puzzle_module.sip, "isdeleted", lambda _obj: False)
    win = SimpleNamespace(_shelf_waiting={"x": (1, None, None, time.monotonic())}, _canvas_gen=1, _shelf_queued=None,
                          SHELF_WAIT_S=30, statusBar=lambda: SimpleNamespace(showMessage=lambda *a: calls.append(a)),
                          _take_from_shelf=lambda e, s, store: calls.append(("take", [x["sys_id"] for x in e])))
    for name in ("_shelf_ready", "take_entries_from_shelf", "_take_queued", "_shelf_wait"):
        setattr(win, name, getattr(PuzzleCanvasWindow, name).__get__(win))
    assert win.take_entries_from_shelf([{"sys_id": "a"}], object()) is False
    assert win.take_entries_from_shelf([{"sys_id": "b"}], object()) is False
    assert not [c for c in calls if c[0] == "take"] and timers                # queued, a retry set
    retries = len(timers)
    assert win._shelf_wait("x", 1) is not None                                # the loading one lands ...
    assert len(timers) == retries + 1 and timers[-1][0] == 0                 # ... the queue is asked at once
    timers[-1][1]()
    assert calls[-1] == ("take", ["a", "b"]) and win._shelf_queued is None
    # Codex #87: one queued for a canvas replaced since (a new puzzle, another join) is dropped
    win._shelf_waiting = {"y": (1, None, None, time.monotonic())}
    assert win.take_entries_from_shelf([{"sys_id": "c"}], object()) is False
    taken = len([c for c in calls if c[0] == "take"])
    win._canvas_gen = 2
    timers[-1][1]()                                                         # its retry comes
    assert len([c for c in calls if c[0] == "take"]) == taken and win._shelf_queued is None


# -- DX23 no node boxes but in the exclusions ----------------------------------------------------------

@pytest.mark.parametrize("node_boxes", [False, True])
def test_node_rows_have_boxes_only_in_the_exclusion_pickers(svc, node_boxes):
    dialog = _open(svc, start="col:10", mode="multi", purpose="test", on_confirm=lambda *_a: None,
                   node_boxes=node_boxes)
    try:
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and c.key == "n:col:10")
        assert column.tickable is node_boxes and bool(column.boxes) is node_boxes
        hint = dialog.chips_bar.hint.text()
        assert hint == ("Tick shelfmarks, or a whole volume, to choose them." if node_boxes
                        else "Tick shelfmarks to choose them.")
        dialog.leaf_panel.whole.isVisible()                                 # "Select all" stays the way
    finally:
        _close(dialog)
    src = (ROOT / "desktop/dialogs_filter.py").read_text(encoding="utf-8")
    assert "node_boxes=True" in src                                         # the exclusion picker


# -- DX24 the title in the tooltip ---------------------------------------------------------------------

def test_a_shelfmark_s_tooltip_carries_its_title(svc, monkeypatch):
    monkeypatch.setattr(picker, "title_of", lambda sid: f"A long title of {sid}")
    dialog = _open(svc, start="vol:100")
    try:
        item = _item_of(dialog, SYS["as2"][1])
        assert f"A long title of {SYS['as2'][1]}" in item.toolTip()
    finally:
        _close(dialog)


# -- DX25 the details pane -------------------------------------------------------------------------

def _field(pane, key):
    """A labelled field line of the pane, as read ("Pages: 7 · With text: 5")."""
    import re
    label = pane.lines_box.findChild(QLabel, f"field_{key}")
    return re.sub(r"<[^>]+>", "", label.text()).replace("‎", "").replace("‏", "") if label else None


def test_the_pane_shows_the_shelfmark_clicked_last_and_its_image(svc, monkeypatch):
    images = {}

    def thumb(sid, _meta):
        # the first one clicked has an image, slowly; the second none: its pane must not show the first's
        if sid != SYS["as2"][1]:
            images[sid] = None
            return None
        time.sleep(0.2)
        image = QImage(40, 60, QImage.Format.Format_RGB32)
        images[sid] = image
        return image
    monkeypatch.setattr(picker, "_thumbnail_image", thumb)
    monkeypatch.setattr(picker, "catalogue", object())
    monkeypatch.setattr(picker, "page_count_of", lambda sid: 7)
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        assert pane.empty.isVisibleTo(dialog) and pane.empty.text() == "Choose a shelfmark to see it here"
        _click_item(dialog, _item_of(dialog, SYS["as2"][1]))
        assert _wait(lambda: pane.text is not None)                          # its image is on its way ...
        _click_item(dialog, _item_of(dialog, SYS["as2"][2]))               # ... when a newer click wins
        assert _wait(lambda: pane.text is not None and pane.text["sys_id"] == SYS["as2"][2])
        assert "T-S AS 2.3" in pane.shelfmark.text()
        assert _field(pane, "pages") == "Pages: 7"                         # round 4: labelled fields
        assert "place" not in pane.fields                                   # the reader is in the picker
        assert _wait(lambda: len(images) == 2, timeout=3)
        _wait(lambda: False, timeout=0.3)
        assert not pane.image.isVisibleTo(dialog)                           # the older click's image never drawn
    finally:
        _close(dialog)
    dialog = _open(svc, start="vol:100")
    try:
        _click_item(dialog, _item_of(dialog, SYS["as2"][1]))
        assert _wait(lambda: dialog.details.image.isVisibleTo(dialog))      # an image, when there is one
    finally:
        _close(dialog)


def test_the_pane_follows_the_row_clicked_last_in_the_multiple_choice(svc):
    dialog = _open(svc, start="vol:100", mode="multi", purpose="test", on_confirm=lambda *_a: None)
    try:
        _click_item(dialog, _item_of(dialog, SYS["as2"][0]))
        assert _wait(lambda: dialog.details.text is not None)
        assert dialog.details.text["sys_id"] == SYS["as2"][0]
        assert SYS["as2"][0] in dialog.chosen().chosen_set(members_function(svc))   # ticked as well
    finally:
        _close(dialog)


# -- DX27 Go to's icon button, DX28 Ctrl+W --------------------------------------------------------

def test_go_to_s_shelf_is_an_icon_button_at_the_end_of_its_box(svc, monkeypatch):
    from desktop.go_to_dialog import GoToDialog
    opened = []
    monkeypatch.setattr(picker, "pick_from_shelf_or_several", lambda *a, **k: opened.append(k) or None)
    box = GoToDialog(None, shelf_service=svc, shelf_start="vol:100")
    try:
        assert isinstance(box.shelf_button, QToolButton)
        assert box.shelf_button.toolTip().replace("\u200e", "") == "Pick from the shelf (Ctrl+W)"
        buttons = box.findChild(QDialogButtonBox)
        assert box.shelf_button not in buttons.buttons()                     # not in the button row
        box.show()
        _APP.processEvents()
        assert box.shelf_button.mapTo(box, QPoint(0, 0)).x() > box.input.mapTo(box, QPoint(0, 0)).x()
        QTest.keyClick(box.input, Qt.Key.Key_W, Qt.KeyboardModifier.ControlModifier)
        assert len(opened) == 1                                             # Ctrl+W in Go to too
    finally:
        box.deleteLater()


def test_ctrl_w_opens_the_shelf_from_any_section():
    src = (ROOT / "genizah_app.py").read_text(encoding="utf-8")
    assert '("Ctrl+W", self._open_shelf_anywhere)' in src
    assert "('W', Qt.KeyboardModifier.ControlModifier): self._open_shelf_anywhere" in src
    assert "key_label('Ctrl+W'" in src                                      # Browse's button says it
    from desktop.shortcuts_dialog import shortcut_sections
    for hebrew in (False, True):
        keys = {k for _title, rows in shortcut_sections(hebrew) for k, _what in rows}
        assert "Ctrl+W" in keys
    import genizah_app
    opened = []
    win = SimpleNamespace(open_shelf_picker=opened.append, _browse_shelf_start=lambda: "vol:100")
    win._open_shelf_anywhere = genizah_app.GenizahGUI._open_shelf_anywhere.__get__(win)
    import desktop.widgets.shelfmark_tree_picker as shelf_picker
    real = shelf_picker.shelf_tree_service
    try:
        shelf_picker.shelf_tree_service = lambda: object()
        win._open_shelf_anywhere()
        shelf_picker.shelf_tree_service = lambda: None
        win._open_shelf_anywhere()
    finally:
        shelf_picker.shelf_tree_service = real
    assert opened == ["vol:100"]                                            # without the tree: nothing
    _ = Selection


def test_the_pane_shows_a_preview_and_adds_the_community_s_joins_and_comments(svc, monkeypatch):
    from desktop.corrections_client import ConnectedFragments, FragmentJoin
    own = SYS["as2"][0]
    asked = []

    class Client:
        def get_connected_fragments_by_id(self, sid):
            asked.append(sid)
            time.sleep(0.1)
            join = FragmentJoin(id="j1", fragment_a="T-S AS 2.1", fragment_b="ENA 123.4", document_id_a=sid,
                                document_id_b="990000999990205171", relationship_type="join", notes="",
                                source="user", created_by_username="", created_at=None)
            return ConnectedFragments(shelfmark="T-S AS 2.1", shelfmark_normalized="", joins=[join])

        def get_document_stats(self, sid):
            return {"comments_count": 3, "corrections_count": 0}
    monkeypatch.setattr(picker, "community", Client())
    monkeypatch.setattr(picker, "page_count_of", lambda sid: 2)
    monkeypatch.setattr(picker, "page_texts_of",
                        lambda sid: iter(["", ("שורה ראשונה של הטקסט ובה מילים ארוכות למדי, "
                                               "ושורה שנייה של אותו הטקסט ממש, להיקרא כטקסט שלם\n") * 3]))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, _item_of(dialog, own))
        # round 4: the text box, not a preview (tests/test_shelf_picker_desktop_round4.py)
        assert _wait(lambda: pane.text is not None and pane.textbox.isVisibleTo(dialog))
        assert "שורה ראשונה" in pane.textbox.toPlainText()
        assert _wait(lambda: _field(pane, "comments") == "Comments: 3")     # drawn again when they arrive
        assert any("ENA 123.4" in label.text() for label in pane.joins_box.findChildren(QLabel)) and asked == [own]
        joins = pane.text["joins"]
        assert [j["sources"] for j in joins if j["shelfmark"] == "ENA 123.4"] == [["community"]]
    finally:
        _close(dialog)
