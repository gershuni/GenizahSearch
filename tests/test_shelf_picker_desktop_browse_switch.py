# -*- coding: utf-8 -*-
"""Browse's one shelf button with a switch (DX16): the picker Browse opens has "Open more than one"
in its header; on, the same open dialog is the multiple-choice picker where it is (the Reading
Desk's kept choice), off again the single choice there. Its confirm: a desk open -> added to it; a
manuscript shown -> a desk started with it; nothing shown -> the first opens in Browse and the rest
join it on the desk (one: it just opens); at most 12 new, the rest stay chosen. Go to and the other
single-choice hosts have no switch, and the separate "Several to the Reading Desk" button is gone.

The call sites through the real dialog (its exec driven by a script) and the real GenizahGUI
methods on a stand-in window. The language is pinned and the four profile variables point at
scratch folders.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QDialog

import desktop.widgets.shelfmark_tree_picker as shelf_picker
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture
from tests.test_shelf_picker_desktop_multi_hosts import (
    _APP, _chosen, _Desk, _entries, _FakeWorker, _many, _store_with,
)

pytestmark = pytest.mark.gui

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    import genizah_core
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


@pytest.fixture
def app(monkeypatch):
    import genizah_app
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "ReadingDeskWorker", _FakeWorker)
    _FakeWorker.made.clear()
    return genizah_app


def _settle(dialog, pred=None, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if dialog._view is not None and not dialog._loads.busy() and not dialog._busy and (pred is None or pred()):
            return
    raise AssertionError("the picker did not settle")


def _scripted(monkeypatch, steps):
    """Each dialog's exec runs the next step on it, once it has drawn; answers the dialogs seen."""
    seen = []

    def exec_(dialog):
        dialog.show()
        _settle(dialog)
        seen.append(dialog)
        steps.pop(0)(dialog)
        _APP.processEvents()
        return dialog.result()
    monkeypatch.setattr(shelf_picker.ShelfPickerDialog, "exec", exec_)
    return seen


def _several_parts(dialog):
    """The multiple-choice parts, shown (True) or hidden (False), each."""
    return {name: getattr(dialog, name).isVisibleTo(dialog)
            for name in ("chips_bar", "count_label", "clear_button", "confirm_button")}


def _boxes(dialog):
    """The shelfmarks drawn with a box."""
    grid = dialog.leaf_panel.grid
    return [grid.item(i).data(Qt.ItemDataRole.CheckStateRole) is not None for i in range(grid.count())]


def _leaves(dialog):
    return [row["leaf"] for row in (dialog.leaf_panel.leaves or {}).get("rows", [])]


def _go(dialog, node_id, svc):
    path = tuple(n.id for n in svc.ancestors(node_id))
    dialog.go(path)
    _settle(dialog, lambda: dialog._drawn_path == path and dialog.leaf_panel.leaves is not None)
    return path


def _open(svc, store, start, on_confirm=lambda *_a: None):
    return shelf_picker.pick_from_shelf_or_several(
        None, svc, start=start, switch_label="Open more than one", purpose="reading_desk",
        on_confirm=on_confirm, store=store, confirm_label="Open them in the Reading Desk")


# -- the switch -----------------------------------------------------------------------------------

def test_the_switch_changes_the_mode_in_the_same_open_dialog_at_the_same_place(svc, monkeypatch):
    sid = SYS["as2"][2]
    store = shelf_picker.SelectionStore()
    state = {}

    def flips(dialog):
        assert not dialog.multi and dialog.switch is not None and not dialog.switch.isChecked()
        assert dialog.switch.text() == "Open more than one"
        assert not any(_several_parts(dialog).values()) and not any(_boxes(dialog))     # single choice
        assert {c.table.columnCount() for c in dialog.node_columns if c.isVisibleTo(dialog)} == {2}
        path, mark, size = dialog.path(), dialog._highlight, dialog.size()
        assert mark is not None                                              # opened at the manuscript
        dialog.switch.click()                                                # on
        _APP.processEvents()
        assert dialog.isVisible() and dialog.result() == 0 and dialog.multi  # the same dialog, still open
        assert all(_several_parts(dialog).values()) and all(_boxes(dialog)) # boxes, chips, count, confirm
        assert {c.table.columnCount() for c in dialog.node_columns if c.isVisibleTo(dialog)} == {3}
        assert dialog.confirm_button.text() == "Open them in the Reading Desk"
        assert dialog.path() == path and dialog._highlight == mark and dialog.size() == size
        first = _leaves(dialog)[0]
        dialog._pick(first)                                                  # a click ticks, never closes
        _settle(dialog, lambda: dialog._paint_state is not None and dialog._paint_state["count"] == 1)
        assert dialog.isVisible() and dialog.selection() is None
        state["ticked"] = first.sys_id
        state["moved"] = _go(dialog, "vol:110", svc)                          # elsewhere, then off
        _settle(dialog, lambda: dialog.leaf_panel.whole.isVisibleTo(dialog))  # "Select all in ..." shown
        dialog.switch.click()
        _APP.processEvents()
        assert dialog.isVisible() and not dialog.multi and not dialog.switch.isChecked()
        assert not any(_several_parts(dialog).values()) and not any(_boxes(dialog))
        assert {c.table.columnCount() for c in dialog.node_columns if c.isVisibleTo(dialog)} == {2}
        assert not dialog.leaf_panel.whole.isVisibleTo(dialog)               # "Select all" goes too
        assert dialog.path() == state["moved"]                               # where it was
        dialog.switch.click()                                                # on again: the choice is there
        _settle(dialog, lambda: dialog._paint_state is not None)
        assert dialog.multi and dialog.count_label.text() == "1 manuscripts chosen"
        dialog.switch.click()                                                # off: a click picks one
        _APP.processEvents()
        state["pick"] = _leaves(dialog)[1]
        dialog._pick(state["pick"])

    seen = _scripted(monkeypatch, [flips])
    assert _open(svc, store, sid) == ("open", state["pick"].as_selection())   # the single pick opens
    assert len(seen) == 1                                                    # one dialog throughout
    assert _chosen(store, "reading_desk", svc) == {state["ticked"]}         # the kept choice is intact


def test_the_switch_is_off_at_every_opening(svc, monkeypatch):
    store = shelf_picker.SelectionStore()

    def turn_on_and_close(dialog):
        dialog.switch.setChecked(True)
        assert dialog.multi
        dialog.reject()

    def close(dialog):
        assert not dialog.multi and not dialog.switch.isChecked()
        dialog.reject()

    seen = _scripted(monkeypatch, [turn_on_and_close, close])
    assert _open(svc, store, SYS["as2"][0]) is None
    assert _open(svc, store, SYS["as2"][0]) is None
    assert len(seen) == 2


def test_an_answer_asked_for_in_the_several_mode_is_not_drawn_after_the_switch_goes_off(svc):
    dialog = shelf_picker.ShelfPickerDialog(svc, start=SYS["as2"][0], purpose="reading_desk",
                                           on_confirm=lambda *_a: None, switch_label="Open more than one")
    try:
        dialog.show()
        _settle(dialog)
        dialog.switch.setChecked(True)
        dialog._toggle_whole()                                               # its answer is asked for ...
        dialog.switch.setChecked(False)                                      # ... and the mode leaves
        _settle(dialog, lambda: not dialog._state_loads.busy())
        _APP.processEvents()
        assert not dialog.leaf_panel.whole.isVisibleTo(dialog) and not any(_boxes(dialog))
        assert not dialog.chips_bar.isVisibleTo(dialog)
        dialog._request_state()                                              # a late one (a prune's) ...
        _settle(dialog, lambda: not dialog._state_loads.busy())
        _APP.processEvents()
        assert not dialog.leaf_panel.whole.isVisibleTo(dialog)               # ... is not drawn either
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_the_switch_waits_while_a_confirm_is_with_the_host(svc):
    dialog = shelf_picker.ShelfPickerDialog(svc, mode="multi", purpose="reading_desk", on_confirm=lambda *_a: None,
                                           switch_label="Open more than one")
    try:
        dialog.show()
        _settle(dialog)
        dialog._busy = True
        dialog._footer()
        assert not dialog.switch.isEnabled()
        dialog.switch.setChecked(False)                                     # (a click that got through)
        assert dialog.switch.isChecked() and dialog.multi and dialog.isVisible()
        assert all(_several_parts(dialog).values())
        dialog._busy = False
        dialog._footer()
        assert dialog.switch.isEnabled()
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_the_single_choice_hosts_have_no_switch(svc, monkeypatch):
    dialog = shelf_picker.ShelfPickerDialog(svc, start=SYS["as2"][0])
    try:
        assert dialog.switch is None
    finally:
        dialog.reject()
        dialog.deleteLater()
    # Go to has Browse's picker, switch and all (round 4): tests/test_shelf_picker_desktop_round4.py


# -- Browse's call site and its confirm -------------------------------------------------------------

class _Field:
    def __init__(self, text=""):
        self._text = text

    def text(self):
        return self._text

    def setText(self, text):
        self._text = text

    def clear(self):
        self._text = ""


class _Browse(_Desk):
    """The desk stand-in, with Browse's real open path: a pick goes through _browse_open_shelf_pick
    and _browse_open_field to browse_load, which here shows what the system number names."""

    BROWSE_METHODS = ("open_shelf_picker", "_browse_open_several_from_shelf", "_browse_open_shelf_pick",
                      "_shelf_choice_limit", "_shelf_open_actions", "_shelf_picker_options", "_shelf_outcome", "_shelf_add_to_list",
                      "_shelf_add_to_puzzle", "_shelf_place_sid", "_browse_open_field")

    def __init__(self, app, **kw):
        super().__init__(app, **kw)
        for name in self.BROWSE_METHODS:
            setattr(self, name, getattr(app.GenizahGUI, name).__get__(self))
        self.browse_sys_input, self.browse_fl_input, self.browse_shelf_input = _Field(), _Field(), _Field()
        self.browse_tab = object()
        self.shown_tabs = []                   # the sections brought to the front
        self.loads = []
        self.refuse = False                    # browse_load opens nothing (a manuscript that fails)

    def browse_load(self):
        self.loads.append(self.browse_sys_input.text())
        self.current_browse_sid = None if self.refuse else (self.browse_sys_input.text() or None)

    def _set_last_browse_field(self, _name):
        pass

    def _set_active_tab(self, tab):
        self.shown_tabs.append(tab)


def test_browse_opens_the_picker_with_the_switch_and_the_desk_s_kept_choice(app, svc, monkeypatch):
    seen = {}
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: svc)
    monkeypatch.setattr(shelf_picker, "pick_from_shelf_or_several", lambda *a, **k: seen.update(k) or None)
    host = _Browse(app, active=False, current="")
    assert host.open_shelf_picker("vol:100") is None
    assert seen["switch_label"] == "Open more than one" and seen["purpose"] == "reading_desk"
    assert seen["store"] is host._shelf_selections                          # the desk button's choice
    assert seen["on_confirm"] == host._browse_open_several_from_shelf       # the call site
    assert seen["confirm_label"] == "Open them in the Reading Desk" and seen["start"] == "vol:100"


def test_with_a_desk_open_the_choice_is_added_to_it(app, svc):
    sids = _many(svc, 4)
    host = _Browse(app, active=True, current=sids[0], on_desk=[sids[0]])
    taken = host._browse_open_several_from_shelf(_entries(svc, sids), "x")
    assert taken == sids                                                    # the one on the desk: taken
    assert [e.sys_id for e in host.browse_reading_desk_state.entries] == sids
    assert host.loads == [] and host.entered == []                          # Browse is not changed


@pytest.mark.parametrize("case", ["desk open", "manuscript shown", "nothing shown"])
def test_several_opened_from_another_section_bring_browse_to_the_front(app, svc, case):
    # Seen in the app: Ctrl+W from another section, several ticked and opened, and the window stayed
    # where it was; a single pick brings Browse to the front, and so does this
    sids = _many(svc, 3)
    if case == "desk open":
        host = _Browse(app, active=True, current=sids[0], on_desk=[sids[0]])
    elif case == "manuscript shown":
        host = _Browse(app, active=False, current=SYS["as2"][0])
    else:
        host = _Browse(app, active=False, current="")
    assert host._browse_open_several_from_shelf(_entries(svc, sids[1:]), "x")
    assert host.shown_tabs and host.shown_tabs[-1] is host.browse_tab


def test_nothing_opened_leaves_the_section_where_it_is(app, svc):
    sids = _many(svc, 2)
    host = _Browse(app, active=False, current=SYS["as2"][0])
    host.pages = {sid: 1200 for sid in sids}                                # neither fits the desk
    assert host._browse_open_several_from_shelf(_entries(svc, sids), "x") == []
    assert host.shown_tabs == []


def test_with_a_manuscript_shown_a_desk_is_started_with_it(app, svc):
    current = SYS["as2"][0]
    host = _Browse(app, active=False, current=current)
    taken = host._browse_open_several_from_shelf(_entries(svc, [SYS["as2"][1], SYS["as2"][2]]), "x")
    assert host.entered == [[current, SYS["as2"][1], SYS["as2"][2]]] and host.loads == []
    assert sorted(taken) == sorted([SYS["as2"][1], SYS["as2"][2]])


def test_with_nothing_shown_the_first_opens_and_the_rest_join_it_on_the_desk(app, svc):
    sids = _many(svc, 15)
    host = _Browse(app, active=False, current="")
    host.pages = {sid: 100 for sid in sids}                                 # ten make 1,000 pages
    taken = host._browse_open_several_from_shelf(_entries(svc, sids), "x")
    assert host.loads == [sids[0]]                                          # opened in Browse, as a pick
    assert host.entered == [sids[:10]]                                      # the desk's limit, the first first
    assert taken == sids[:10]                                               # the other 5 stay chosen
    assert "10 added to the Reading Desk" in host.status_label.text()       # the first counted too
    assert "The Reading Desk holds up to 1,000 pages" in host.status_label.text()


def test_with_nothing_shown_a_first_the_desk_could_not_hold_is_not_opened(app, svc):
    sids = _many(svc, 2)
    host = _Browse(app, active=False, current="")
    host.pages = {sids[0]: 1200, sids[1]: 1}                                # a restored choice, from before
    assert host._browse_open_several_from_shelf(_entries(svc, sids), "x") == []
    assert host.loads == [] and host.entered == []


def test_with_nothing_shown_one_ticked_just_opens(app, svc):
    sid = SYS["as2"][4]
    host = _Browse(app, active=False, current="")
    assert host._browse_open_several_from_shelf(_entries(svc, [sid, sid]), "x") == [sid]
    assert host.loads == [sid] and host.entered == [] and not host.browse_reading_desk_active


def test_a_first_that_does_not_open_takes_nothing(app, svc):
    sids = _many(svc, 3)
    host = _Browse(app, active=False, current="")
    host.refuse = True
    assert host._browse_open_several_from_shelf(_entries(svc, sids), "x") == []
    assert host.entered == [] and host.loads == [sids[0]]


def test_through_the_dialog_the_rest_stay_chosen_for_the_next_confirm(app, svc):
    sids = _many(svc, 14)
    store = _store_with(svc, "reading_desk", sids)
    host = _Browse(app, active=False, current="")
    host.pages = {sid: 100 for sid in sids}                                 # ten make 1,000 pages
    host._shelf_selections = store
    dialog = shelf_picker.ShelfPickerDialog(svc, mode="multi", purpose="reading_desk", store=store,
                                           on_confirm=host._browse_open_several_from_shelf,
                                           confirm_label="Open them in the Reading Desk",
                                           switch_label="Open more than one", limit=host._browse_rd_limit())
    try:
        dialog.show()
        _settle(dialog)
        dialog.confirm_button.click()
        _settle(dialog)
        assert host.loads == [sids[0]] and len(host.entered[0]) == 10
        assert _chosen(store, "reading_desk", svc) == set(sids[10:])
        assert dialog.isVisible() and dialog.note.text() == (
            "4 are still chosen · The Reading Desk holds up to 1,000 pages; these would make 1,400")
        host.pages.update({sid: 1 for sid in sids[10:]})                    # (smaller ones: they fit)
        host.pages.update({sid: 90 for sid in sids[:10]})
        dialog.confirm_button.click()                                       # the next confirm: to the desk
        _settle(dialog)
        assert dialog.result() == QDialog.DialogCode.Accepted and not dialog.isVisible()
        assert [e.sys_id for e in host.browse_reading_desk_state.entries][-4:] == sids[10:]
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_the_separate_several_button_is_gone_from_browse():
    src = (ROOT / "genizah_app.py").read_text(encoding="utf-8")
    assert "btn_b_rd_several" not in src
    assert src.count("on_several=self._browse_rd_pick_several") == 1       # the desk toolbar's only
    assert "pick_from_shelf_or_several(" in src and src.count("switch_label=") == 1
