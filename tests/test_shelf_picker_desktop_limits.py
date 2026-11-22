# -*- coding: utf-8 -*-
"""The desktop picker's limits when ticking and its chips' names (owner, 2026-11-21;
shared/shelfmark_tree_limits.py): a tick that would pass the limit is refused with it; a node too big
to tick whole has its box off (and "Select all" too), refused before its manuscripts are read; one
ticked whole from before can be unticked; removals always pass; chips carry the library's code
("CUL T-S AS 2"); and every host opens the picker with its limit (an exclusion with none).

The real dialog on the fixture tree. The four profile variables point at scratch folders.
"""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as shelf_picker
from shared.shelfmark_tree_limits import SelectionLimit
from shared.shelfmark_tree_selection import Selection, members_function
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
    yield


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


def _quiet(dialog, pred=lambda: True, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if (dialog._view is not None and not dialog._loads.busy() and not dialog._state_loads.busy()
                and dialog._paint_state is not None and not dialog._busy and pred()):
            return
    raise AssertionError("the picker did not settle")


def _open(svc, limit, store=None, on_confirm=lambda *_a: None, start="vol:100", node_boxes=True):
    # node boxes on: the exclusion pickers' kind, where a node's box exists (DX23)
    dialog = shelf_picker.ShelfPickerDialog(svc, start=start, mode="multi", purpose="test", store=store,
                                           on_confirm=on_confirm, limit=limit, node_boxes=node_boxes)
    dialog.show()
    _quiet(dialog, lambda: dialog.leaf_panel.leaves is not None)
    return dialog


def _leaves(dialog):
    return [row["leaf"] for row in dialog.leaf_panel.leaves["rows"]]


def _chosen(dialog):
    return dialog.chosen().chosen_set(dialog._members_of)


def test_a_tick_past_the_limit_is_refused_and_says_the_limit(svc):
    store = shelf_picker.SelectionStore()
    dialog = _open(svc, SelectionLimit(max_items=3), store=store)
    try:
        leaves = _leaves(dialog)
        for leaf in leaves[:4]:
            dialog._toggle_leaf(leaf)
        assert _chosen(dialog) == {lf.sys_id for lf in leaves[:3]}
        assert dialog.note.text() == "Up to 3 manuscripts here"
        assert store.load("test").count(members_function(svc)) == 3          # the refused one never kept
        dialog._toggle_leaf(leaves[0])                                       # removals always pass
        dialog._toggle_leaf(leaves[3])
        assert _chosen(dialog) == {lf.sys_id for lf in leaves[1:4]}
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_pages_count_with_what_the_host_holds(svc):
    leaves_of = [lf.sys_id for lf in svc.leaves_under("vol:100")]
    held = {"x": 0}
    limit = SelectionLimit(max_pages=1000, pages_of=lambda sid: 400, base_ids=lambda: ["held"] * held["x"])
    dialog = _open(svc, limit)
    try:
        leaves = _leaves(dialog)
        dialog._toggle_leaf(leaves[0])
        dialog._toggle_leaf(leaves[1])
        dialog._toggle_leaf(leaves[2])                                       # 1,200 pages
        assert len(_chosen(dialog)) == 2 and leaves_of
        assert dialog.note.text() == "The Reading Desk holds up to 1,000 pages; these would make 1,200"
        held["x"] = 1                                                        # the desk grew meanwhile
        dialog._toggle_leaf(leaves[0])
        dialog._toggle_leaf(leaves[0])                                       # 400 held + 800: refused
        assert len(_chosen(dialog)) == 1
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_a_node_too_big_has_its_box_off_and_is_refused_before_its_manuscripts_are_read(svc, monkeypatch):
    dialog = _open(svc, SelectionLimit(max_items=5))
    try:
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and "vol:100" in c.boxes)
        row = column.boxes["vol:100"]                                        # T-S AS 2: 12 manuscripts
        box = column.table.item(row, 0)
        assert not box.flags() & Qt.ItemFlag.ItemIsEnabled
        assert box.toolTip() == "Too many to choose here (up to 5)"
        whole = dialog.leaf_panel.whole
        assert whole.isVisible() and not whole.isEnabled() and whole.toolTip() == box.toolTip()
        read = []
        real = svc.members_under
        monkeypatch.setattr(svc, "members_under", lambda nid, *a, **k: (read.append(nid), real(nid, *a, **k))[1])
        dialog._toggle_row(column, row)                                      # a click that got through
        dialog._toggle_whole()
        assert not _chosen(dialog) and "vol:100" not in read
        assert dialog.note.text() == "Too many to choose here (up to 5)"
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_one_ticked_whole_from_before_stays_on_and_can_be_unticked(svc):
    store = shelf_picker.SelectionStore()
    kept = Selection()
    kept.add_node(svc.node("vol:100"), "all", members_function(svc))       # chosen before the limits
    store.save("test", kept)
    dialog = _open(svc, SelectionLimit(max_items=5), store=store)
    try:
        _quiet(dialog, lambda: dialog._paint_state["count"] == 12)
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and "vol:100" in c.boxes)
        box = column.table.item(column.boxes["vol:100"], 0)
        assert box.flags() & Qt.ItemFlag.ItemIsEnabled and box.checkState() == Qt.CheckState.Checked
        assert dialog.leaf_panel.whole.isEnabled()
        leaves = _leaves(dialog)
        dialog._toggle_leaf(leaves[0])                                       # a removal over the limit passes
        assert len(_chosen(dialog)) == 11
        dialog._toggle_leaf(leaves[0])                                       # adding it back does not
        assert len(_chosen(dialog)) == 11 and dialog.note.text() == "Up to 5 manuscripts here"
        dialog._change(lambda s, m: s.add_node(svc.node("vol:100"), "all", m))   # (refused: adds one)
        assert len(_chosen(dialog)) == 11
    finally:
        dialog.reject()
        dialog.deleteLater()
    dialog = _open(svc, SelectionLimit(max_items=5), store=store)
    try:
        _quiet(dialog, lambda: dialog._paint_state["count"] == 11)
        dialog._toggle_whole()                                               # partly ticked: refused
        assert len(_chosen(dialog)) == 11
    finally:
        dialog.reject()
        dialog.deleteLater()
    store.save("test", kept)
    dialog = _open(svc, SelectionLimit(max_items=5), store=store)
    try:
        _quiet(dialog, lambda: dialog._paint_state["count"] == 12)
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and "vol:100" in c.boxes)
        dialog._toggle_row(column, column.boxes["vol:100"])                  # ticked whole: its box unticks it
        _quiet(dialog)
        assert not _chosen(dialog)
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_without_a_limit_a_whole_node_is_ticked(svc):
    dialog = _open(svc, None)
    try:
        dialog._toggle_whole()
        assert len(_chosen(dialog)) == 12 and dialog.leaf_panel.whole.isEnabled()
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_chosen_during_a_confirm_stays_chosen_with_a_limit(svc):
    # the _since bookkeeping: what is ticked while the host works is not the host's to take
    answers = []

    def host(entries, _label):
        dialog._toggle_leaf(_leaves(dialog)[2])                              # ticked meanwhile
        answers.append([e["sys_id"] for e in entries])
        return None
    dialog = _open(svc, SelectionLimit(max_items=5), on_confirm=host)
    try:
        leaves = _leaves(dialog)
        dialog._toggle_leaf(leaves[0])
        dialog._toggle_leaf(leaves[1])
        dialog.confirm_button.click()
        _quiet(dialog)
        assert answers == [[leaves[0].sys_id, leaves[1].sys_id]]
        assert _chosen(dialog) == {leaves[2].sys_id} and dialog.isVisible()
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_chips_and_the_confirm_label_carry_the_library_s_code(svc):
    labels = []
    dialog = _open(svc, None, on_confirm=lambda _entries, label: labels.append(label))
    try:
        dialog._toggle_whole()
        _quiet(dialog, lambda: dialog._paint_state["chips"])
        assert [c["label"] for c in dialog._paint_state["chips"]] == ["CUL T-S AS 2"]
        dialog.confirm_button.click()
        _quiet(dialog, lambda: labels)
        assert labels == ["CUL T-S AS 2"]
    finally:
        dialog.reject()
        dialog.deleteLater()


# -- every host opens the picker with its limit ------------------------------------------------------

def test_the_hosts_open_the_picker_with_their_limits(svc, monkeypatch):
    import genizah_app
    from desktop import corrections_ui
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    monkeypatch.setattr(shelf_picker, "pick_from_shelf_or_several", lambda *a, **k: opened.append(k))
    gui = genizah_app.GenizahGUI
    desk = SimpleNamespace(browse_reading_desk_active=True, current_browse_sid=SYS["as2"][0],
                           browse_reading_desk_state=SimpleNamespace(entries=[SimpleNamespace(sys_id=SYS["as2"][1])]),
                           searcher=SimpleNamespace(page_count=lambda sid: 0), _shelf_selections=None, lists_mgr=None,
                           _lists_after_shelf_save=lambda: None, _browse_rd_shelf_start=lambda: None,
                           LIST_SHELF_MAX=gui.LIST_SHELF_MAX, lists_current_list_id="L1")
    for name in ("_browse_rd_limit", "_browse_rd_base_ids", "_browse_rd_pick_several", "open_shelf_picker",
                 "_browse_rd_take_from_shelf", "_browse_rd_images_drawn", "_shelf_choice_limit",
                 "_shelf_open_actions", "_shelf_picker_options", "_shelf_outcome", "_shelf_add_to_list", "_shelf_add_to_puzzle",
                 "_browse_open_several_from_shelf", "_lists_pick_from_shelf"):
        setattr(desk, name, getattr(gui, name).__get__(desk))
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: svc)
    desk._browse_rd_pick_several(svc)
    desk.open_shelf_picker()
    for k in opened:
        limit = k["limit"]
        assert (limit.max_items, limit.max_pages) == (30, 1000)
        assert limit._pages(SYS["as2"][1]) == 2                              # no pages in the index: 2 images
    assert opened[0]["limit"].base() == {SYS["as2"][1]}                      # the desk's button: what it holds
    # Browse's picker (it also adds to a list or the Puzzle): the choice alone; the desk's room is
    # applied at "Open them in the Reading Desk" (round 3, Codex amendment 1)
    assert opened[1]["limit"].base() == frozenset()
    assert [a.key for a in opened[1]["actions"]] == ["puzzle"] and opened[1]["act_label"] == "Open"
    desk.lists_mgr = SimpleNamespace(data={"lists": {"L1": {"name": "L"}}})
    desk._lists_pick_from_shelf(svc)
    assert opened[-1]["limit"].max_items == 2000 and opened[-1]["limit"].max_pages is None
    joins = corrections_ui.JoinsDialog.__new__(corrections_ui.JoinsDialog)
    joins._picked_b, joins.document_id = [], SYS["as2"][0]
    joins.frag_b_input = SimpleNamespace(text=lambda: "")
    corrections_ui.JoinsDialog._fragments_b_from_shelf(joins, svc)
    assert opened[-1]["limit"].max_items == 30 and opened[-1]["purpose"] == "join"
    assert opened[-1]["limit"].fragments                                     # "Up to 30 fragments here"
    from desktop.dialogs_filter import ExcludeDialog
    import inspect
    assert "limit=" not in inspect.getsource(ExcludeDialog)                 # an exclusion: no limit


def test_select_all_over_a_node_with_volumes_counts_its_own_shelfmarks(svc):
    # T-S AS (col:10): 17 manuscripts in all, two of its own beside its volumes (#80, Codex): its box is
    # off at a limit of 5, its "Select all" (its own two) is not
    dialog = _open(svc, SelectionLimit(max_items=5), start="col:10")
    try:
        _quiet(dialog, lambda: dialog.leaf_panel.leaves is not None and dialog.leaf_panel.whole.isVisible())
        assert dialog._paint_state["whole"]["scope"] == "direct" and dialog._paint_state["whole"]["total"] == 2
        assert dialog.leaf_panel.whole.isEnabled()
        dialog._toggle_whole()
        assert len(_chosen(dialog)) == 2
        column = next(c for c in dialog.node_columns if c.isVisibleTo(dialog) and "col:10" in c.boxes)
        assert not column.table.item(column.boxes["col:10"], 0).flags() & Qt.ItemFlag.ItemIsEnabled
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_chips_take_the_manuscript_s_own_code_when_the_window_gives_it(svc, monkeypatch):
    monkeypatch.setattr(shelf_picker, "library_code_of", lambda sid: "Katz")
    dialog = _open(svc, None)
    try:
        dialog._toggle_leaf(_leaves(dialog)[0])
        dialog._toggle_whole()
        _quiet(dialog, lambda: dialog._paint_state["chips"])
        assert [c["label"] for c in dialog._paint_state["chips"]] == ["Katz T-S AS 2"]
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_the_desk_counts_a_manuscript_at_least_as_the_images_it_draws(svc):
    import genizah_app
    gui = genizah_app.GenizahGUI
    images = [{"url": f"u{i}"} for i in range(40)]
    desk = SimpleNamespace(meta_mgr=SimpleNamespace(nli_cache={
        "a": {"images_nli": images}, "b": {"fl_ids": "FL123"}, "c": {}}))
    drawn = gui._browse_rd_images_drawn.__get__(desk)
    assert (drawn("a"), drawn("b"), drawn("c"), drawn("d")) == (40, 1, 2, 2)
    desk.browse_reading_desk_active, desk.current_browse_sid = False, ""
    desk.searcher = SimpleNamespace(page_count=lambda sid: 10 if sid == "a" else 0)
    for name in ("_browse_rd_limit", "_browse_rd_base_ids", "_browse_rd_images_drawn"):
        setattr(desk, name, getattr(gui, name).__get__(desk))
    limit = desk._browse_rd_limit()
    assert limit._pages("a") == 40 and limit._pages("b") == 1 and limit._pages("d") == 2   # never below the images
