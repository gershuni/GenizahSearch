# -*- coding: utf-8 -*-
"""The desktop picker's multiple-choice mode and "Exclude from the shelf" (shelf tree picker PR 4,
desktop half): desktop/widgets/shelfmark_tree_picker.py (``mode='multi'``, ``SelectionStore``,
``pick_several_from_shelf``), desktop/dialogs_filter.py::ExcludeDialog and its host
genizah_app.py::open_exclude_dialog, and the pending choice's place in the session file (D12).

On the fixture tree of tests/shelfmark_tree_fixture.py: a whole volume is one chip and unticking a
shelfmark inside it lowers its count; quick ticks all count; a node's box ticks and its name opens;
libraries have no box; "+N more" opens every chip; a slower, older count never replaces a newer one;
a confirm keeps what the host did not take, a failing host keeps everything; a filter's shelfmark is
ticked, not picked; a kept choice comes back on reopening and after a restart, whatever the restore
mode; a broken one is an empty choice. The Exclude call site: the picker's ids reach the replace-all
handler the Paste and List tabs reach, on both surfaces and both tabs, as they are (never resolved
again by shelfmark); Cancel applies nothing and keeps the choice; Apply consumes it; Clear All
clears the List tab's selection and the shelf's sources too; the file missing hides the button.

The language is pinned and the four profile variables point at scratch folders.
"""
from __future__ import annotations

import json
import os
import time
import types

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QApplication, QLabel, QWidget

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.exclusion_service import ExclusionSource, shelf_exclusion_source
from shared.shelfmark_tree_selection import ALL, Selection, members_function
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


def _quiet(dialog):
    """Drawn, and the boxes painted for what is chosen now."""
    return (dialog._view is not None and not dialog._loads.busy() and not dialog._state_loads.busy()
            and dialog._paint_state is not None)


def _open(svc, start=None, store=None, on_confirm=None, purpose="search_exclude", node_boxes=True, **kw):
    # the exclusion pickers' dialog: a box on every node below the libraries (DX23)
    taken = []

    def take(entries, label):
        taken.append((entries, label))
        return None

    dialog = picker.ShelfPickerDialog(svc, start=start, mode="multi", purpose=purpose,
                                      on_confirm=on_confirm or take, store=store, node_boxes=node_boxes, **kw)
    dialog.taken = taken
    dialog.resize(1200, 700)
    dialog.show()
    assert _wait(lambda: _quiet(dialog)), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _grid_item(dialog, sys_id):
    grid = dialog.leaf_panel.grid
    for i in range(grid.count()):
        if grid.item(i).data(Qt.ItemDataRole.UserRole).sys_id == sys_id:
            return grid.item(i)
    raise AssertionError(f"{sys_id} is not shown")


def _click_item(dialog, item):
    grid = dialog.leaf_panel.grid
    QTest.mouseClick(grid.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                     grid.visualItemRect(item).center())


def _click_cell(column, row, col):
    rect = column.table.visualRect(column.table.model().index(row, col))
    QTest.mouseClick(column.table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                     rect.center())


def _chosen(dialog, svc):
    return dialog.chosen().chosen_set(members_function(svc))


def _column_of(dialog, parent):
    return next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and c.col and c.col['parent'] == parent)


# -- the dialog -------------------------------------------------------------------------------

def test_a_whole_volume_is_one_chip_and_unticking_a_shelfmark_inside_lowers_it(svc):
    store = picker.SelectionStore()
    dialog = _open(svc, start="vol:100", store=store)
    try:
        members = svc.members_under("vol:100")
        whole = dialog.leaf_panel.whole
        assert whole.isVisibleTo(dialog) and f"({len(members):,})" in whole.text()
        whole.click()
        assert _wait(lambda: _quiet(dialog) and dialog._paint_state['count'] == len(members))
        chips = dialog._paint_state['chips']
        assert [(c['node'], c['count']) for c in chips] == [(True, len(members))]       # ONE chip
        assert whole.checkState() == Qt.CheckState.Checked
        assert dialog.chips_bar.widgets and dialog.chips_bar.widgets[0].count.text() == f"· {len(members):,}"
        one = SYS["as2"][0]
        item = _grid_item(dialog, one)
        assert item.checkState() == Qt.CheckState.Checked
        _click_item(dialog, item)                                            # untick one inside the chip
        assert _wait(lambda: _quiet(dialog) and dialog._paint_state['count'] == len(members) - 1)
        assert len(dialog._paint_state['chips']) == 1 and dialog._paint_state['chips'][0]['count'] == len(members) - 1
        assert item.checkState() == Qt.CheckState.Unchecked
        assert whole.checkState() == Qt.CheckState.PartiallyChecked
        assert one not in _chosen(dialog, svc)
        assert store.load("search_exclude").to_dict() == dialog.chosen().to_dict()    # kept at once (D12)
    finally:
        _close(dialog)


def test_quick_ticks_all_count_in_the_order_made(svc):
    dialog = _open(svc, start="vol:100")
    try:
        first, second, third = SYS["as2"][:3]
        for sid in (first, second, third):                                   # no wait between the clicks
            _click_item(dialog, _grid_item(dialog, sid))
        assert _chosen(dialog, svc) == {first, second, third}
        assert [c.sys_id for c in dialog.chosen().chips] == [first, second, third]
        _click_item(dialog, _grid_item(dialog, second))                      # the same box again: unticked
        assert _chosen(dialog, svc) == {first, third}
        assert _wait(lambda: _quiet(dialog) and dialog._paint_state['count'] == 2)
        assert dialog.count_label.text() == "2 manuscripts chosen"
    finally:
        _close(dialog)


def test_a_node_s_box_ticks_it_and_its_name_opens_it_and_libraries_have_no_box(svc):
    dialog = _open(svc, start="col:10")
    try:
        libraries = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and c.col.get('libs'))
        assert not libraries.tickable and not libraries.boxes
        volumes = _column_of(dialog, "col:10")
        assert volumes.tickable and "vol:100" in volumes.boxes
        row = volumes.boxes["vol:100"]
        path = dialog.path()
        _click_cell(volumes, row, 0)                                          # the box
        assert dialog.path() == path, "a box ticks; it does not open"
        assert _chosen(dialog, svc) == set(svc.members_under("vol:100"))
        assert _wait(lambda: _quiet(dialog) and volumes.table.item(row, 0).checkState() == Qt.CheckState.Checked)
        _click_cell(volumes, row, volumes.name_col)                           # the name
        assert _wait(lambda: _quiet(dialog) and dialog.path()[-1] == "vol:100")
    finally:
        _close(dialog)


def test_space_ticks_a_row_and_enter_opens_it(svc):
    dialog = _open(svc, start="col:10")
    try:
        volumes = _column_of(dialog, "col:10")
        row = volumes.boxes["vol:100"]
        volumes.table.setFocus()
        volumes.table.setCurrentCell(row, volumes.name_col)
        QTest.keyClick(volumes.table, Qt.Key.Key_Space)
        assert _chosen(dialog, svc) == set(svc.members_under("vol:100"))
        QTest.keyClick(volumes.table, Qt.Key.Key_Return)
        assert _wait(lambda: _quiet(dialog) and dialog.path()[-1] == "vol:100")
        assert dialog.result() != dialog.DialogCode.Accepted
    finally:
        _close(dialog)


def test_a_filter_s_shelfmark_is_ticked_not_picked(svc):
    dialog = _open(svc, start="col:10")
    try:
        dialog.set_filter("n:col:10", "2.3")
        assert _wait(lambda: _quiet(dialog) and _column_of(dialog, "col:10").leaf_rows)
        column = _column_of(dialog, "col:10")
        sid, rows = next(iter(column.leaf_rows.items()))
        _click_cell(column, rows[0], column.name_col)                         # anywhere on a shelfmark's row
        assert dialog.result() != dialog.DialogCode.Accepted and dialog.selection() is None
        assert sid in _chosen(dialog, svc)
        assert _wait(lambda: _quiet(dialog) and column.table.item(rows[0], 0).checkState() == Qt.CheckState.Checked)
    finally:
        _close(dialog)


def _many_leaves(svc, n):
    """Shelfmarks from across the fixture's libraries, one per manuscript."""
    out, seen = [], set()
    for library in svc.children(None):
        for node in svc.children(library.id):
            for leaf in svc.leaves_under(node.id):
                if leaf.sys_id not in seen:
                    seen.add(leaf.sys_id)
                    out.append(leaf)
    assert len(out) >= n, f"the fixture has {len(out)} manuscripts"
    return out[:n]


def test_the_chips_that_do_not_fit_are_behind_plus_n_more_which_opens_every_chip(svc):
    dialog = _open(svc, start="vol:100")
    try:
        _click_item(dialog, _grid_item(dialog, SYS["as2"][0]))
        assert _wait(lambda: _quiet(dialog) and len(dialog._paint_state['chips']) == 1)
        assert not dialog.chips_bar.more.isVisibleTo(dialog) and not dialog.chips_bar.hint.isVisibleTo(dialog)
        leaves = _many_leaves(svc, 24)
        for leaf in leaves:                                                   # what a click on each does
            dialog._toggle_leaf(leaf) if leaf.sys_id != SYS["as2"][0] else None
        assert _wait(lambda: _quiet(dialog) and len(dialog._paint_state['chips']) == 24)
        bar = dialog.chips_bar
        shown = bar.shown()
        hidden = bar.hidden_count()
        assert shown and hidden and len(shown) + hidden == 24                 # one line, the rest counted
        assert [w.key for w in shown] == [c.key for c in dialog.chosen().chips[:len(shown)]]   # in order
        assert bar.more.isVisibleTo(dialog) and bar.more.text() == f"+{hidden} more"
        bar.more.click()
        everything = dialog._all_chips
        assert everything is not None and len(everything.chips) == 24
        assert everything.total.text() == "24 manuscripts chosen"
        gone = everything.chips[0].key
        everything.chips[0].remove.click()
        assert _wait(lambda: _quiet(dialog) and dialog._all_chips is not None and len(dialog._all_chips.chips) == 23)
        assert gone not in [c.key for c in dialog.chosen().chips]
    finally:
        _close(dialog)


def test_beyond_forty_chips_the_bar_counts_the_rest(svc):
    leaves = _many_leaves(svc, picker.CHIPS_DRAWN + 2)
    store = picker.SelectionStore()
    selection = Selection()
    for leaf in leaves:
        selection.add_leaf(leaf, members_function(svc))
    store.save("search_exclude", selection)
    dialog = _open(svc, start="vol:100", store=store)
    try:
        assert len(dialog.chips_bar.widgets) == picker.CHIPS_DRAWN            # drawn: forty at most
        assert dialog.chips_bar.hidden_count() >= 2
        dialog.chips_bar.more.click()
        assert len(dialog._all_chips.chips) == picker.CHIPS_DRAWN + 2          # the list has every chip
    finally:
        _close(dialog)


def test_a_slower_older_count_never_replaces_a_newer_one(svc, monkeypatch):
    real = picker.paint_state
    calls = []

    def slow_first(service, selection, *rest):
        calls.append(len(selection.chips))
        if len(calls) == 2:                                                   # the first change's count
            time.sleep(0.6)
        return real(service, selection, *rest)

    dialog = _open(svc, start="vol:100")
    try:
        monkeypatch.setattr(picker, "paint_state", slow_first)
        drawn = []
        real_paint = dialog.leaf_panel.paint
        dialog.leaf_panel.paint = lambda state: (drawn.append(state['count']), real_paint(state))
        calls.clear()
        calls.append(0)                                                       # the next count is the slow one
        first, second = SYS["as2"][:2]
        _click_item(dialog, _grid_item(dialog, first))                        # its count is slow ...
        _APP.processEvents()
        _click_item(dialog, _grid_item(dialog, second))                       # ... and this one asked meanwhile
        assert _wait(lambda: _quiet(dialog) and dialog._paint_state['count'] == 2)
        time.sleep(0.7)
        _APP.processEvents()
        assert drawn == [2], f"an older count was drawn: {drawn}"
    finally:
        _close(dialog)


def test_a_confirm_keeps_what_the_host_did_not_take(svc):
    store = picker.SelectionStore()
    first, second, third = SYS["as2"][:3]
    answers = [[first], None]
    handed = []

    def host(entries, label):
        handed.append([e["sys_id"] for e in entries])
        return answers.pop(0)

    dialog = _open(svc, start="vol:100", store=store, on_confirm=host)
    try:
        for sid in (first, second, third):
            _click_item(dialog, _grid_item(dialog, sid))
        dialog.confirm_button.click()
        assert _wait(lambda: not dialog._busy and handed)
        assert handed == [[first, second, third]]
        assert _chosen(dialog, svc) == {second, third}
        assert dialog.isVisible() and dialog.note.text() == "2 are still chosen"
        assert Selection.from_dict(store.load("search_exclude").to_dict()).chips[0].sys_id == second
        dialog.confirm_button.click()                                         # the host takes the rest
        assert _wait(lambda: dialog.result() == dialog.DialogCode.Accepted)
        assert not len(store.load("search_exclude"))
    finally:
        _close(dialog)


def _slow_resolving(dialog, seconds=0.4):
    real = dialog._resolving

    def slow():
        read = real()
        return lambda: (time.sleep(seconds), read())[1]
    dialog._resolving = slow


@pytest.mark.parametrize("staged", [False, True])
def test_what_is_chosen_again_while_the_host_works_stays_chosen(svc, staged):
    # the web's rule (Codex PR #70): Clear, then the same shelfmark again, while a confirm is with
    # the host -- the host took the choice it was sent, not the later one
    store = picker.SelectionStore()
    first, second = SYS["as2"][:2]
    handed, kept_by_host = [], []

    def host(entries, _label):
        handed.append([e["sys_id"] for e in entries])
        return picker.STAGED if staged else None

    dialog = _open(svc, start="vol:100", store=store, on_confirm=host, on_staged=kept_by_host.append)
    try:
        for sid in (first, second):
            _click_item(dialog, _grid_item(dialog, sid))
        _slow_resolving(dialog)
        dialog.confirm_button.click()
        assert dialog._busy
        dialog._clear()                                                       # while the host works ...
        _click_item(dialog, _grid_item(dialog, first))                        # ... the same one again
        assert _wait(lambda: handed and not dialog._busy)
        assert handed == [[first, second]]
        if staged:
            assert kept_by_host == [frozenset({first})]                       # the host must not take it
            assert dialog.result() == dialog.DialogCode.Accepted
        else:
            assert _chosen(dialog, svc) == {first} and dialog.isVisible()
            assert dialog.note.text() == "1 are still chosen"
            assert store.load("search_exclude").chips[0].sys_id == first
    finally:
        _close(dialog)


def test_the_exclude_dialog_does_not_consume_what_was_chosen_again(svc):
    store = picker.SelectionStore()
    selection = Selection()
    for sid in SYS["as2"][:2]:
        selection.add_leaf(next(lf for lf in svc.leaves_under("vol:100") if lf.sys_id == sid), members_function(svc))
    store.save("search_exclude", selection)
    dlg = _exclude_dialog(svc, store)
    try:
        assert dlg._take_from_shelf([{"sys_id": s, "shelfmark": "", "placement_id": ""} for s in SYS["as2"][:2]],
                                    "x") is picker.STAGED
        dlg._note_chosen_again({SYS["as2"][0]})
        dlg.accept()
        assert [c.sys_id for c in store.load("search_exclude").chips] == [SYS["as2"][0]]
    finally:
        dlg.deleteLater()


def test_a_failing_host_keeps_the_whole_choice(svc):
    def host(_entries, _label):
        raise RuntimeError("no")

    dialog = _open(svc, start="vol:100", on_confirm=host)
    try:
        _click_item(dialog, _grid_item(dialog, SYS["as2"][0]))
        dialog.confirm_button.click()
        assert _wait(lambda: not dialog._busy and dialog.note.text())
        assert dialog.note.text() == "The choice could not be applied. It is kept; try again."
        assert _chosen(dialog, svc) == {SYS["as2"][0]} and dialog.isVisible()
        assert dialog.confirm_button.isEnabled()
    finally:
        _close(dialog)


def test_save_as_a_list_goes_through_the_desktop_list_path(svc, monkeypatch):
    class Lists:
        def __init__(self):
            self.made, self.added = [], []

        def create_list(self, name):
            self.made.append(name)
            return "list_1"

        def add_items_bulk(self, ids, list_id, source=""):
            self.added.append((list(ids), list_id, source))
            return len(ids)

    lists, refreshed = Lists(), []
    monkeypatch.setattr(picker.QInputDialog, "getText", staticmethod(lambda *a, **k: ("My shelf", True)))
    dialog = _open(svc, start="vol:100", lists_mgr=lists, on_list_saved=lambda: refreshed.append(1))
    try:
        dialog.leaf_panel.whole.click()
        assert _wait(lambda: _quiet(dialog) and dialog._paint_state['count'])
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and lists.made)
        assert lists.made == ["My shelf"]
        assert lists.added == [(svc.sys_ids_under("vol:100"), "list_1", "shelf")]
        assert refreshed == [1] and dialog.note.text() == "Saved as a list: My shelf"
        assert _chosen(dialog, svc) == set(svc.members_under("vol:100"))      # it stays chosen
    finally:
        _close(dialog)


def test_a_list_whose_save_failed_is_not_reported_saved(svc, monkeypatch, tmp_path):
    # Codex, PR #73: create_list and add_items_bulk ignore what save() returns; a read-only or full
    # profile left the list in memory only while the picker said "Saved as a list"
    from shared import lists_manager as lm
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lm.Config, "INDEX_DIR", str(tmp_path))
    lists = lm.ListsManager(None)
    saves = []
    real_save = lists.save

    def failing_save():
        saves.append(1)
        with monkeypatch.context() as m:                  # the write itself fails: save() answers False
            m.setattr(lm, "write_bytes_atomic", lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk full")))
            return real_save()

    monkeypatch.setattr(lists, "save", failing_save)
    monkeypatch.setattr(picker.QInputDialog, "getText", staticmethod(lambda *a, **k: ("My shelf", True)))
    refreshed = []
    dialog = _open(svc, start="vol:100", lists_mgr=lists, on_list_saved=lambda: refreshed.append(1))
    try:
        _click_item(dialog, _grid_item(dialog, SYS["as2"][0]))
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and saves)
        assert lists.saves_failing() and not (tmp_path / "lists.pkl").exists()
        assert dialog.note.text() == "The list could not be saved."
        assert refreshed == []                                                # no "saved" refresh either
        assert _chosen(dialog, svc) == {SYS["as2"][0]}                        # the choice stays
    finally:
        _close(dialog)


def test_a_retry_after_a_failed_save_fills_the_same_list(svc, monkeypatch, tmp_path):
    # Codex, PR #73 round 2: the failed try's list is in memory already; a retry that made another
    # left two lists of one name once storage worked again
    from shared import lists_manager as lm
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lm.Config, "INDEX_DIR", str(tmp_path))
    lists = lm.ListsManager(None)
    broken = {"on": True}
    real_write = lm.write_bytes_atomic

    def write(*a, **k):
        if broken["on"]:
            raise OSError("disk full")
        return real_write(*a, **k)

    monkeypatch.setattr(lm, "write_bytes_atomic", write)
    asked = []
    monkeypatch.setattr(picker.QInputDialog, "getText",
                        staticmethod(lambda *a, **k: (asked.append(1), ("My shelf", True))[1]))
    dialog = _open(svc, start="vol:100", lists_mgr=lists)
    try:
        first, second = SYS["as2"][:2]
        for sid in (first, second):
            _click_item(dialog, _grid_item(dialog, sid))
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and asked)
        assert dialog.note.text() == "The list could not be saved."
        broken["on"] = False                                                   # storage works again
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and dialog.note.text().startswith("Saved"))
        assert asked == [1]                                                    # the retry asks no name again
        mine = [lid for lid, lst in lists.data["lists"].items() if lst.get("name") == "My shelf"]
        assert len(mine) == 1
        held = [it["sys_id"] for it in lists.data["items"].values() if mine[0] in it.get("lists", [])]
        assert sorted(held) == sorted([first, second])                         # each manuscript once
        reread = lm.ListsManager(None)                                         # and on disk
        assert [lst.get("name") for lst in reread.data["lists"].values()].count("My shelf") == 1
    finally:
        _close(dialog)


def test_a_retry_after_the_failed_list_was_put_in_the_trash_makes_it_again(svc, monkeypatch, tmp_path):
    # Codex, PR #73 round 3: delete_list keeps the id (deleted_at); a retry into it hid the manuscripts
    from shared import lists_manager as lm
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lm.Config, "INDEX_DIR", str(tmp_path))
    lists = lm.ListsManager(None)
    # the list's creation is written, the add of its manuscripts is not (a failed creation drops the
    # list: test_a_failed_save_as_a_list_is_rolled_back_...)
    broken = {"on": True, "writes": 0}
    real_write = lm.write_bytes_atomic

    def write(*a, **k):
        broken["writes"] += 1
        if broken["on"] and broken["writes"] > 1:
            raise OSError("full")
        return real_write(*a, **k)

    monkeypatch.setattr(lm, "write_bytes_atomic", write)
    asked = []
    monkeypatch.setattr(picker.QInputDialog, "getText",
                        staticmethod(lambda *a, **k: (asked.append(1), ("My shelf", True))[1]))
    dialog = _open(svc, start="vol:100", lists_mgr=lists)
    try:
        _click_item(dialog, _grid_item(dialog, SYS["as2"][0]))
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and asked)
        first = dialog._list_retry[0]
        lists.delete_list(first)                                               # to the trash meanwhile
        assert lists.data["lists"][first].get("deleted_at")
        broken["on"] = False
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and dialog.note.text().startswith("Saved"))
        assert asked == [1]                                                    # the name it was given
        shown = [lid for lid, lst in lists.data["lists"].items()
                 if lst.get("name") == "My shelf" and not lst.get("deleted_at")]
        assert len(shown) == 1 and shown[0] != first
        held = [it["sys_id"] for it in lists.data["items"].values() if shown[0] in it.get("lists", [])]
        assert held == [SYS["as2"][0]]
    finally:
        _close(dialog)


def test_a_failed_save_as_a_list_is_rolled_back_and_the_retry_writes_the_choice_then(svc, monkeypatch, tmp_path):
    # Codex on a65dfe5: a failed try left its list and manuscripts in memory; a retry after the
    # reader changed the choice wrote the old manuscripts too
    from shared import lists_manager as lm
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lm.Config, "INDEX_DIR", str(tmp_path))
    lists = lm.ListsManager(None)
    broken = {"on": True}
    real_write = lm.write_bytes_atomic
    monkeypatch.setattr(lm, "write_bytes_atomic",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("full")) if broken["on"] else real_write(*a, **k))
    monkeypatch.setattr(picker.QInputDialog, "getText", staticmethod(lambda *a, **k: ("My shelf", True)))
    dialog = _open(svc, start="vol:100", lists_mgr=lists)
    try:
        a, b, c = SYS["as2"][:3]
        for sid in (a, b):
            _click_item(dialog, _grid_item(dialog, sid))
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and dialog.note.text())
        assert dialog.note.text() == "The list could not be saved."
        assert not [lst for lst in lists.data["lists"].values() if lst.get("name") == "My shelf"]   # dropped
        assert not any(sid in lists.data["items"] for sid in (a, b))
        _click_item(dialog, _grid_item(dialog, b))                             # untick one ...
        _click_item(dialog, _grid_item(dialog, c))                             # ... tick another
        broken["on"] = False
        dialog.save_button.click()
        assert _wait(lambda: not dialog._busy and dialog.note.text().startswith("Saved"))
        mine = [lid for lid, lst in lists.data["lists"].items() if lst.get("name") == "My shelf"]
        assert len(mine) == 1
        reread = lm.ListsManager(None)
        held = sorted(it["sys_id"] for it in reread.data["items"].values() if mine[0] in it["lists"])
        assert held == sorted([a, c])
    finally:
        _close(dialog)


def test_a_kept_choice_comes_back_on_reopening_and_after_a_restart(svc):
    store = picker.SelectionStore()
    dialog = _open(svc, start="vol:100", store=store)
    first = SYS["as2"][0]
    _click_item(dialog, _grid_item(dialog, first))
    _close(dialog)
    again = picker.SelectionStore()
    again.load_state(json.loads(json.dumps(store.to_state())))               # through the session file
    dialog = _open(svc, start="vol:100", store=again)
    try:
        assert _chosen(dialog, svc) == {first}
        assert _grid_item(dialog, first).checkState() == Qt.CheckState.Checked
        assert again.load("composition_exclude").chips == []                 # purposes stay apart
    finally:
        _close(dialog)


def test_a_broken_kept_choice_is_an_empty_one():
    store = picker.SelectionStore()
    for raw in (None, [], "x", {"search_exclude": "x"}, {"search_exclude": {"format": "shelf-selection",
                                                                             "version": 1, "chips": 1}}):
        store.load_state(raw)
        assert store.load("search_exclude").chips == []
    store.load_state({"search_exclude": {"format": "shelf-selection", "version": 1,
                                         "chips": [{"kind": "leaf", "ref": "inv:1", "sys_id": "970000000000001"}]}})
    assert store.load("search_exclude").chips == []                           # never a LOCAL 97… id


# -- Exclude manuscripts ----------------------------------------------------------------------

def _exclude_dialog(svc, store, sources=None, **kw):
    from desktop.dialogs_filter import ExcludeDialog
    return ExcludeDialog(None, exclusion_sources=sources, purpose="search_exclude",
                         shelf_service=svc, shelf_store=store, **kw)


def _fake_picker(svc, monkeypatch, node="vol:100", seen=None):
    """The picker as the dialog uses it: the volume ticked in the kept choice, then the confirm."""
    def pick(_parent, service, *, purpose, on_confirm, store=None, **_kw):
        selection = store.load(purpose)
        selection.add_node(service.node(node), ALL, members_function(service))
        store.save(purpose, selection)
        answer = on_confirm(selection.resolve_entries(service), "T-S AS 2")
        if seen is not None:
            seen.append(answer)
        return answer is picker.STAGED or answer is None
    monkeypatch.setattr(picker, "pick_several_from_shelf", pick)


def test_the_real_picker_stages_a_source_and_apply_consumes_the_choice(svc):
    store = picker.SelectionStore()
    dlg = _exclude_dialog(svc, store)

    def drive():
        modal = QApplication.activeModalWidget()
        if not isinstance(modal, picker.ShelfPickerDialog) or not _quiet(modal) or not modal.leaf_panel.whole.isVisible():
            QTimer.singleShot(20, drive)
            return
        if not len(modal.chosen()):
            modal.leaf_panel.whole.click()
            QTimer.singleShot(20, drive)
            return
        modal.confirm_button.click()

    try:
        assert dlg.btn_shelf is not None
        # the real dialog, opened at T-S AS 2 by its own "Pick from the shelf" -> drive it
        real = picker.pick_several_from_shelf

        def opened_at_volume(*a, **k):
            k["start"] = "vol:100"
            return real(*a, **k)
        picker.pick_several_from_shelf = opened_at_volume
        try:
            QTimer.singleShot(20, drive)
            dlg._pick_from_shelf()
        finally:
            picker.pick_several_from_shelf = real
        members = svc.members_under("vol:100")
        assert [s.source_type for s in dlg.shelf_sources()] == ["shelf"]
        assert dlg.shelf_sources()[0].sys_ids == set(members)
        assert dlg.shelf_sources()[0].label == "From the shelf: CUL T-S AS 2"     # its library's code first
        assert store.load("search_exclude").count(members_function(svc)) == len(members)   # kept until Apply
        dlg.accept()
        assert [s.sys_ids for s in dlg.get_exclusion_sources()] == [set(members)]
        assert not len(store.load("search_exclude"))                         # Apply consumed it
    finally:
        dlg.deleteLater()


def test_cancel_applies_nothing_and_keeps_the_choice(svc, monkeypatch):
    store = picker.SelectionStore()
    _fake_picker(svc, monkeypatch)
    dlg = _exclude_dialog(svc, store)
    dlg._pick_from_shelf()
    assert dlg.shelf_sources()
    dlg.reject()
    assert len(store.load("search_exclude")) == 1                             # still chosen, for next time
    dlg.deleteLater()


def test_the_picked_ids_are_never_resolved_again_by_shelfmark(svc, monkeypatch):
    # the shelf map knows the shelfmark of a picked manuscript under ANOTHER record: the picked id
    # stands, and the other record is not added
    store = picker.SelectionStore()
    _fake_picker(svc, monkeypatch)
    picked = svc.sys_ids_under("vol:100")
    from genizah_core import normalize_shelfmark
    other = "990009999990205171"
    shelf_map = {normalize_shelfmark(svc.leaves("vol:100", 0, 1)[0][0].shelfmark): other}
    dlg = _exclude_dialog(svc, store, shelf_map=shelf_map)
    dlg._pick_from_shelf()
    dlg.accept()
    ids = set().union(*(s.sys_ids for s in dlg.get_exclusion_sources()))
    assert ids == set(picked) and other not in ids
    assert dlg.sys_text_area.toPlainText() == ""                              # the editor is left as it was
    dlg.deleteLater()


def test_a_shelf_source_applied_before_stays_apart_and_can_be_removed_without_the_tree():
    applied = shelf_exclusion_source({SYS["as2"][0]}, "From the shelf: T-S AS 2.1")
    typed = ExclusionSource(label="Manual entry", source_type="file", source_id="manual", sys_ids={SYS["or"][0]})
    from desktop.dialogs_filter import ExcludeDialog
    dlg = ExcludeDialog(None, existing_entries=sorted(applied.sys_ids | typed.sys_ids),
                        exclusion_sources=[applied, typed], purpose="search_exclude",
                        shelf_service=None, shelf_store=picker.SelectionStore())
    try:
        assert dlg.btn_shelf is None                                          # the file missing: no button
        assert dlg._shelf_box.isVisibleTo(dlg) or not dlg.isVisible()
        assert dlg.sys_text_area.toPlainText() == SYS["or"][0]                # the shelf's ids are not in the editor
        assert dlg.shelf_sources() == [applied]
        dlg._remove_shelf_source(applied.source_id)
        assert dlg.shelf_sources() == []
        assert [s.source_type for s in dlg.get_exclusion_sources()] == ["file"]
    finally:
        dlg.deleteLater()


def test_only_shelf_sources_do_not_bring_their_ids_into_the_editor():
    applied = shelf_exclusion_source({SYS["as2"][0]}, "From the shelf: T-S AS 2.1")
    from desktop.dialogs_filter import ExcludeDialog
    dlg = ExcludeDialog(None, existing_entries=sorted(applied.sys_ids), exclusion_sources=[applied])
    try:
        assert dlg.sys_text_area.toPlainText() == ""
        assert dlg.get_exclusion_sources() == [applied]
    finally:
        dlg.deleteLater()


def test_clear_all_clears_the_list_tab_and_the_shelf_too(svc, monkeypatch):
    class Lists:
        def get_all_lists(self, include_recent=False):
            return [{"id": "l1", "name": "Mine", "count": 1}]

        def get_items_in_list(self, list_id):
            return [{"sys_id": SYS["or"][1]}]

    _fake_picker(svc, monkeypatch)
    dlg = _exclude_dialog(svc, picker.SelectionStore(), lists_mgr=Lists())
    try:
        dlg._pick_from_shelf()
        dlg._tab_widget.setCurrentIndex(1)
        dlg._list_widget.item(0).setSelected(True)
        assert {s.source_type for s in dlg.get_exclusion_sources()} == {"list", "shelf"}
        dlg._clear_all()
        assert dlg.get_exclusion_sources() == []
    finally:
        dlg.deleteLater()


def test_an_edit_after_resolving_is_not_lost():
    # a resolution answers for the text it was made from: ids loaded afterwards still count
    from desktop.dialogs_filter import ExcludeDialog
    from genizah_core import normalize_shelfmark
    dlg = ExcludeDialog(None, shelf_map={normalize_shelfmark("T-S AS 2.1"): SYS["as2"][0]})
    try:
        dlg.shelf_text_area.setPlainText("T-S AS 2.1")
        dlg._show_report()
        assert dlg._resolved_ids == {SYS["as2"][0]}
        dlg._last_edited = "sys"
        dlg.sys_text_area.setPlainText(f"{SYS['as2'][0]}\n{SYS['or'][0]}")
        ids = set().union(*(s.sys_ids for s in dlg.get_exclusion_sources()))
        assert SYS["or"][0] in ids
    finally:
        dlg.deleteLater()


def test_local_ids_never_become_a_shelf_source(svc):
    dlg = _exclude_dialog(svc, picker.SelectionStore())
    try:
        assert dlg._take_from_shelf([{"sys_id": "970000000000001", "shelfmark": "x", "placement_id": "p"}], "x") == []
        assert dlg.shelf_sources() == []
    finally:
        dlg.deleteLater()


# -- the host: genizah_app.open_exclude_dialog and the session file ---------------------------

def _app_module(monkeypatch):
    import genizah_app
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en")
    return genizah_app


class _Host(QWidget):
    """The parts of GenizahGUI the Exclude dialog's host reads, with the real methods."""

    METHODS = ("open_exclude_dialog", "_excl_get", "_excl_set", "_update_exclusion_display")

    def __init__(self, app):
        super().__init__()
        gui = app.GenizahGUI
        for name in self.METHODS:
            setattr(self, name, getattr(gui, name).__get__(self))
        self._EXCLUSION_ATTRS, self._EXCLUSION_LABELS = gui._EXCLUSION_ATTRS, gui._EXCLUSION_LABELS
        self.lbl_main_exclude_status, self.lbl_exclude_status = QLabel(self), QLabel(self)
        for prefix in ("", "comp_"):
            setattr(self, f"{prefix}exclusion_sources", [])
            setattr(self, f"{prefix}excluded_sys_ids", set())
            setattr(self, f"{prefix}excluded_shelfmarks", set())
            setattr(self, f"{prefix}excluded_raw_entries", [])
        self._shelf_selections = picker.SelectionStore()
        self._shelf_to_sys = {}
        self.lists_mgr = None
        self.rerendered, self.saved = 0, 0

    def _ensure_shelf_map(self):
        pass

    def _rerender_with_exclusions(self):
        self.rerendered += 1

    def _schedule_session_save(self):
        self.saved += 1


@pytest.mark.parametrize("surface, tab", [("search", 0), ("search", 1), ("composition", 0)])
def test_a_shelf_choice_reaches_the_same_replace_all_handler_as_the_tabs(svc, monkeypatch, surface, tab):
    app = _app_module(monkeypatch)
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: svc)
    seen = []
    _fake_picker(svc, monkeypatch, seen=seen)
    host = _Host(app)
    typed = ExclusionSource(label="Manual entry", source_type="file", source_id="manual", sys_ids={SYS["or"][0]})
    host._excl_set(surface, "sources", [typed])
    host._excl_set(surface, "raw", [SYS["or"][0]])
    purposes = []

    class Driven(app.ExcludeDialog):
        def exec(self):
            purposes.append(self._purpose)
            self._pick_from_shelf()                          # "Pick from the shelf", then Apply
            self._tab_widget.setCurrentIndex(tab)
            self.accept()
            return True

    monkeypatch.setattr(app, "ExcludeDialog", Driven)
    host.open_exclude_dialog(surface)
    picked = set(svc.members_under("vol:100"))
    sources = host._excl_get(surface, "sources")
    assert seen == [picker.STAGED]
    assert purposes == ["search_exclude" if surface == "search" else "composition_exclude"]
    assert [s for s in sources if s.source_type == "shelf"][0].sys_ids == picked
    expected = picked | ({SYS["or"][0]} if tab == 0 else set())     # the List tab replaces the editor's
    assert host._excl_get(surface, "sys_ids") == expected
    other = "composition" if surface == "search" else "search"
    assert host._excl_get(other, "sources") == []                    # the other surface is untouched
    assert host.saved == 1
    label = host.lbl_main_exclude_status if surface == "search" else host.lbl_exclude_status
    assert "From the shelf: T-S AS 2" in label.text()
    assert not len(host._shelf_selections.load(purposes[0]))      # consumed by the Apply


def test_with_the_file_missing_the_exclude_dialog_has_no_shelf_button(monkeypatch):
    app = _app_module(monkeypatch)
    monkeypatch.setattr(picker, "shelf_tree_service", lambda: None)
    host = _Host(app)
    built = []

    class Seen(app.ExcludeDialog):
        def exec(self):
            built.append(self.btn_shelf)
            return False

    monkeypatch.setattr(app, "ExcludeDialog", Seen)
    host.open_exclude_dialog("search")
    assert built == [None]


def test_the_pending_choice_survives_a_restart_whatever_the_restore_mode(svc, monkeypatch):
    app = _app_module(monkeypatch)
    import shared.session_persistence as session_persistence
    saved = {}
    monkeypatch.setattr(app, "load_app_config", lambda: {"restore_mode": "never"})
    monkeypatch.setattr(session_persistence, "save_session_state", lambda state: saved.setdefault("state", state) or True)
    before = picker.SelectionStore()
    selection = Selection()
    selection.add_node(svc.node("vol:100"), ALL, members_function(svc))
    before.save("composition_exclude", selection)
    save_fake = types.SimpleNamespace(_shelf_selections=before, _is_browsing_local=lambda: False)
    app.GenizahGUI._save_session(save_fake)
    state = json.loads(json.dumps(saved["state"]))
    assert state["shelf_selections"]["composition_exclude"]["chips"][0]["ref"] == "vol:100"

    class _Quiet:
        def setVisible(self, _v):
            pass

        def notify_session_restored(self):
            pass

    restore_fake = types.SimpleNamespace(
        _shelf_selections=picker.SelectionStore(), search_progress=_Quiet(), my_library_tab=_Quiet(),
        _update_exclusion_display=lambda surface='search': None, _update_load_more_button=lambda: None)
    restore_fake._apply_persistent_session_preferences = types.MethodType(
        app.GenizahGUI._apply_persistent_session_preferences, restore_fake)
    monkeypatch.setattr(session_persistence, "load_session_state", lambda: state)
    app.GenizahGUI._restore_session(restore_fake)
    assert restore_fake._shelf_selections.load("composition_exclude").to_dict() == selection.to_dict()
    assert not len(restore_fake._shelf_selections.load("search_exclude"))
