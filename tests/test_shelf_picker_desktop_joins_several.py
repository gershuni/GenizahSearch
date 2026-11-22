# -*- coding: utf-8 -*-
"""Several fragments of one join from the shelf: the desktop Joins dialog's fragment B
(desktop/corrections_ui.py::JoinsDialog; shelf tree picker, handoff DX15 -- a join holds any number
of fragments, often ten, in rare cases twenty).

Pinned: the dialog's shelf button opens the picker's multiple-choice mode (purpose ``join``, kept
only while it is open, starting at fragment A); a confirm keeps the corpus manuscripts in the order
chosen, without fragment A itself or repeats; only fragment A, or more than JOIN_MAX, is a warning
that takes nothing (the picker stays open with the choice); one pick is the single path as before;
typing in the box forgets the several (and a list typed over is refused, never one join to "a; b");
Create Join makes one join from fragment A to each, with the same relationship and notes, says it
once, and keeps in the box only those not made, so the next Create tries only them; a join queued
offline counts as made and is said so.

The language is pinned and the four profile variables point at scratch folders.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QTimer
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as shelf_picker

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
SID_A = "990001234570205171"          # fragment A, the dialog's own document
SIDS = [f"9900012346{i:02d}0205171" for i in range(40)]
LOCAL = "970000000123"
SERVICE = object()


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: SERVICE)
    yield


def _sel(sid, shelfmark=None):
    return {"sys_id": sid, "shelfmark": shelfmark or f"T-S AS {sid[-9:-7]}.{sid[-11:-9]}",
            "placement_id": f"inv:1:{sid}"}


class _Client:
    """create_join refuses the records in ``refuse`` (with ``msg``), makes the others."""

    current_user = SimpleNamespace(role="user")

    def __init__(self):
        self.made = []
        self.refuse = set()
        self.msg = "refused"

    def is_logged_in(self):
        return True

    def is_server_available(self):
        return False

    def create_join(self, frag_a, frag_b, rel_type, notes, document_id_a=None, document_id_b=None):
        self.made.append({"frag_a": frag_a, "frag_b": frag_b, "rel": rel_type, "notes": notes,
                          "document_id_a": document_id_a, "document_id_b": document_id_b})
        if document_id_b in self.refuse:
            return None, self.msg
        return SimpleNamespace(id=f"j{len(self.made)}", fragment_a=frag_a, fragment_b=frag_b,
                               relationship_type=rel_type, notes=notes, source="user"), "ok"


class _JoinsMgr:
    def __init__(self):
        self.data = {"joins": {}}
        self.local = []

    def _index_join(self, join_data):
        pass

    def save(self):
        pass

    def create_join_local(self, frag_a, frag_b, rel_type, notes, document_id_a=None, document_id_b=None):
        self.local.append(document_id_b)


@pytest.fixture
def joins(monkeypatch):
    from desktop import corrections_ui
    said = []
    for kind in ("information", "warning", "critical"):
        monkeypatch.setattr(corrections_ui.QMessageBox, kind,
                            lambda _parent, title, text, *_a, k=kind: said.append((k, title, text)))

    class _Joins(corrections_ui.JoinsDialog):
        loads = 0

        def load_joins(self, force_fresh=False):
            type(self).loads += 1

    client = _Client()
    dialog = _Joins(None, client=client, document_id=SID_A, shelfmark="T-S NS 5.1")
    dialog.frag_a_input.setText("T-S NS 5.1")
    _Joins.loads = 0
    try:
        yield SimpleNamespace(dialog=dialog, client=client, said=said, cls=_Joins)
    finally:
        dialog.deleteLater()


def _confirming(monkeypatch, entries):
    """The picker stand-in: confirms ``entries`` at once; records how it was opened and the answer."""
    asked = []

    def fake(parent, service, *, purpose, on_confirm, store=None, start=None, title=None,
             confirm_label=None, **_kw):
        outcome = on_confirm(entries, "label")
        asked.append({"service": service, "purpose": purpose, "store": store, "start": start,
                      "confirm_label": confirm_label, "outcome": outcome})
        return outcome is None
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", fake)
    return asked


# -- the confirm ----------------------------------------------------------------------------------

def test_the_shelf_button_opens_the_several_mode_for_the_join(monkeypatch, joins):
    asked = _confirming(monkeypatch, [_sel(SIDS[0])])
    assert not joins.dialog.btn_frag_b_shelf.isHidden()
    joins.dialog.btn_frag_b_shelf.click()
    assert asked == [{"service": SERVICE, "purpose": "join", "store": None, "start": SID_A,
                      "confirm_label": "Add them to the join", "outcome": None}]


def test_a_confirm_keeps_corpus_picks_in_order_without_fragment_a_or_repeats(monkeypatch, joins):
    asked = _confirming(monkeypatch, [_sel(SIDS[2], "T-S AS 2.3"), _sel(SID_A, "T-S NS 5.1"),
                                      _sel(LOCAL, "My scan"), _sel(SIDS[1], "T-S AS 2.2"),
                                      _sel(SIDS[2], "T-S AS 2.3")])
    joins.dialog.btn_frag_b_shelf.click()
    assert asked[0]["outcome"] is None                                  # took them all
    assert joins.dialog.frag_b_input.text() == "T-S AS 2.3; T-S AS 2.2"
    assert joins.dialog._picked_b == [("T-S AS 2.3", SIDS[2]), ("T-S AS 2.2", SIDS[1])]
    assert joins.dialog._selected_doc_id_b is None
    assert joins.said == []
    # the next opening starts at the last one chosen, not at the box's "a; b"
    asked = _confirming(monkeypatch, [_sel(SIDS[3])])
    joins.dialog.btn_frag_b_shelf.click()
    assert asked[0]["start"] == SIDS[1]


def test_only_fragment_a_is_a_warning_that_takes_nothing(monkeypatch, joins):
    asked = _confirming(monkeypatch, [_sel(SID_A, "T-S NS 5.1")])
    joins.dialog.btn_frag_b_shelf.click()
    assert asked[0]["outcome"] == ()                                    # the picker stays open
    assert joins.said == [("warning", "Error", "Cannot join a fragment to itself")]
    assert joins.dialog.frag_b_input.text() == "" and joins.dialog._picked_b == []


def test_more_than_join_max_is_refused_whole(monkeypatch, joins):
    cap = joins.dialog.JOIN_MAX
    assert cap == 30
    asked = _confirming(monkeypatch, [_sel(sid) for sid in SIDS[:cap + 1]])
    joins.dialog.btn_frag_b_shelf.click()
    assert asked[0]["outcome"] == ()
    assert joins.said == [("warning", "Error", "A join takes at most 30 fragments at a time")]
    assert joins.dialog.frag_b_input.text() == "" and joins.dialog._picked_b == []
    joins.said.clear()
    asked = _confirming(monkeypatch, [_sel(sid) for sid in SIDS[:cap]] + [_sel(SID_A)])   # A not counted
    joins.dialog.btn_frag_b_shelf.click()
    assert asked[0]["outcome"] is None and len(joins.dialog._picked_b) == cap and joins.said == []


def test_one_pick_is_the_single_path_as_before(monkeypatch, joins):
    _confirming(monkeypatch, [_sel(SID_A), _sel(SIDS[4], "T-S AS 4.1")])
    joins.dialog.btn_frag_b_shelf.click()
    assert joins.dialog.frag_b_input.text() == "T-S AS 4.1"
    assert joins.dialog._selected_doc_id_b == SIDS[4] and joins.dialog._picked_b == []
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == [SIDS[4]]
    assert joins.said == [("information", "Success", "Join created successfully")]
    assert joins.dialog.frag_b_input.text() == "" and joins.cls.loads == 1


def test_typing_forgets_the_several_and_a_list_typed_over_is_refused(monkeypatch, joins):
    _confirming(monkeypatch, [_sel(SIDS[1], "T-S AS 2.2"), _sel(SIDS[2], "T-S AS 2.3")])
    joins.dialog.btn_frag_b_shelf.click()
    joins.dialog.frag_b_input.setText("T-S AS 2.2; T-S AS 2.9")          # the reader edits the list
    assert joins.dialog._picked_b == [] and joins.dialog._selected_doc_id_b is None
    joins.dialog.create_new_join()
    assert joins.client.made == []                                      # never one join to "a; b"
    assert joins.said == [("warning", "Error", "Please select a fragment")]
    # edited down to one shelfmark, it is typed as any other
    joins.said.clear()
    joins.dialog.frag_b_input.setText("T-S AS 2.9")
    joins.dialog.create_new_join()
    assert [m["frag_b"] for m in joins.client.made] == ["T-S AS 2.9"]
    # Codex on 6603ad4 (GitHub and CLI): a typed catalogue form holding "; " is not a list. Emptied,
    # the box forgets the several; a form typed afresh is resolved as before.
    joins.dialog.frag_b_input.clear()
    joins.dialog.frag_b_input.setText("Catalogue Halper, Philadelphia 138; 142")
    joins.said.clear()
    joins.dialog.create_new_join()
    assert joins.client.made[-1]["frag_b"] == "Catalogue Halper, Philadelphia 138; 142"
    assert joins.said == [("information", "Success", "Join created successfully")]


def test_a_catalogue_form_with_a_semicolon_is_typed_in_a_fresh_dialog(monkeypatch, joins):
    joins.dialog.frag_b_input.setText("Catalogue Halper, Philadelphia 138; 142")
    joins.dialog.create_new_join()
    assert [m["frag_b"] for m in joins.client.made] == ["Catalogue Halper, Philadelphia 138; 142"]
    # and after a single pick, as after a list's pick (no several were shown)
    joins.dialog._set_fragment_b("T-S AS 2.2", SIDS[1])
    joins.dialog.frag_b_input.setText("Catalogue Allony-Kupfer Belgium; Spain")
    joins.dialog.create_new_join()
    assert joins.client.made[-1]["frag_b"] == "Catalogue Allony-Kupfer Belgium; Spain"
    # a failed several left in the box, then typed over, is refused again
    _confirming(monkeypatch, [_sel(SIDS[1], "T-S AS 2.2"), _sel(SIDS[2], "T-S AS 2.3")])
    joins.dialog.btn_frag_b_shelf.click()
    joins.client.refuse = {SIDS[1], SIDS[2]}
    joins.dialog.create_new_join()
    joins.said.clear()
    made = len(joins.client.made)
    joins.dialog.frag_b_input.setText(joins.dialog.frag_b_input.text() + " ")
    joins.dialog.create_new_join()
    assert len(joins.client.made) == made and joins.said == [("warning", "Error", "Please select a fragment")]


def test_a_form_pasted_over_several_is_typed_and_an_undo_takes_the_several_back(monkeypatch, joins):
    # Codex CLI round 3 on b179725: pasting a catalogue form over several (no empty box between) was
    # refused; Delete then Undo brought "a; b" back without its records, to be joined as one.
    _confirming(monkeypatch, [_sel(SIDS[1], "T-S AS 2.2"), _sel(SIDS[2], "T-S AS 2.3")])
    joins.dialog.btn_frag_b_shelf.click()
    shown = joins.dialog.frag_b_input.text()
    joins.dialog.frag_b_input.setText("Catalogue Halper, Philadelphia 138; 142")      # pasted over
    joins.dialog.create_new_join()
    assert [m["frag_b"] for m in joins.client.made] == ["Catalogue Halper, Philadelphia 138; 142"]
    joins.client.made.clear()
    joins.dialog.frag_b_input.setText(shown)                            # Undo: the several are back
    assert joins.dialog._picked_b == [("T-S AS 2.2", SIDS[1]), ("T-S AS 2.3", SIDS[2])]
    joins.said.clear()
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == [SIDS[1], SIDS[2]]
    assert joins.said == [("information", "Success", "2 joins created")]
    # made: an Undo now brings back text only, and it is refused, never joined again or as one
    joins.client.made.clear()
    joins.said.clear()
    joins.dialog.frag_b_input.setText(shown)
    assert joins.dialog._picked_b == []
    joins.dialog.create_new_join()
    assert joins.client.made == [] and joins.said == [("warning", "Error", "Please select a fragment")]


# -- Create Join with several -----------------------------------------------------------------------

def test_create_makes_one_join_to_each_with_the_same_relationship_and_notes(monkeypatch, joins):
    picks = [_sel(SIDS[i], f"T-S AS 3.{i}") for i in range(3)]
    _confirming(monkeypatch, picks)
    joins.dialog.btn_frag_b_shelf.click()
    joins.dialog.type_combo.setCurrentIndex(joins.dialog.type_combo.findData("physical_join"))
    joins.dialog.notes_input.setText("one leaf torn in three")
    joins.dialog.create_new_join()
    assert joins.client.made == [
        {"frag_a": "T-S NS 5.1", "frag_b": f"T-S AS 3.{i}", "rel": "physical_join",
         "notes": "one leaf torn in three", "document_id_a": SID_A, "document_id_b": SIDS[i]}
        for i in range(3)]
    assert joins.said == [("information", "Success", "3 joins created")]   # one message, not three
    assert joins.dialog.frag_b_input.text() == "" and joins.dialog.notes_input.text() == ""
    assert joins.dialog._picked_b == [] and joins.cls.loads == 1
    assert joins.dialog.btn_create.isEnabled()


def test_those_not_made_stay_and_the_next_create_tries_only_them(monkeypatch, joins):
    picks = [_sel(SIDS[i], f"T-S AS 3.{i}") for i in range(4)]
    _confirming(monkeypatch, picks)
    joins.dialog.btn_frag_b_shelf.click()
    joins.dialog.notes_input.setText("kept")
    joins.client.refuse = {SIDS[1], SIDS[3]}
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == SIDS[:4]
    assert joins.said == [("warning", "Error",
                           "2 joins created\nNot created: T-S AS 3.1, T-S AS 3.3\nFailed to create join: refused")]
    assert joins.dialog._picked_b == [("T-S AS 3.1", SIDS[1]), ("T-S AS 3.3", SIDS[3])]
    assert joins.dialog.frag_b_input.text() == "T-S AS 3.1; T-S AS 3.3"
    assert joins.dialog.notes_input.text() == "kept" and joins.cls.loads == 1
    # again: only the two not made; one refused again is left as the single pick
    joins.client.made.clear()
    joins.said.clear()
    joins.client.refuse = {SIDS[3]}
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == [SIDS[1], SIDS[3]]
    assert joins.dialog._picked_b == [] and joins.dialog._selected_doc_id_b == SIDS[3]
    assert joins.dialog.frag_b_input.text() == "T-S AS 3.3"
    assert joins.said[0][0] == "warning" and joins.said[0][2].startswith("1 joins created\nNot created: T-S AS 3.3")


def test_a_host_failure_on_one_leaves_the_others_to_be_made(monkeypatch, joins):
    _confirming(monkeypatch, [_sel(SIDS[0], "T-S AS 3.0"), _sel(SIDS[1], "T-S AS 3.1")])
    joins.dialog.btn_frag_b_shelf.click()
    real = joins.client.create_join

    def flaky(frag_a, frag_b, *a, **k):
        if frag_b == "T-S AS 3.0":
            raise RuntimeError("network down")
        return real(frag_a, frag_b, *a, **k)
    joins.client.create_join = flaky
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == [SIDS[1]]
    assert joins.dialog._selected_doc_id_b == SIDS[0]                   # the failed one stays
    assert "Failed to create join: network down" in joins.said[0][2]


def test_cancel_during_a_request_stops_before_the_next(monkeypatch, joins):
    # Codex CLI round 1: Cancel clicked while a request waits is a queued click; checked before the
    # events were processed, one more join was made after it.
    from PyQt6.QtWidgets import QProgressDialog
    _confirming(monkeypatch, [_sel(SIDS[i], f"T-S AS 3.{i}") for i in range(3)])
    joins.dialog.btn_frag_b_shelf.click()
    real = joins.client.create_join

    def cancel_meanwhile(*a, **k):
        progress = joins.dialog.findChild(QProgressDialog)
        QTimer.singleShot(0, progress.cancel)                           # the click, queued
        return real(*a, **k)
    joins.client.create_join = cancel_meanwhile
    joins.dialog.create_new_join()
    assert [m["document_id_b"] for m in joins.client.made] == [SIDS[0]]
    assert joins.dialog._picked_b == [("T-S AS 3.1", SIDS[1]), ("T-S AS 3.2", SIDS[2])]   # not tried: kept
    assert joins.said == [("information", "Success", "1 joins created\nNot created: T-S AS 3.1, T-S AS 3.2")]


def test_a_join_queued_offline_counts_as_made_and_is_said_so(monkeypatch, joins):
    joins.dialog.joins_mgr = _JoinsMgr()
    _confirming(monkeypatch, [_sel(SIDS[0], "T-S AS 3.0"), _sel(SIDS[1], "T-S AS 3.1")])
    joins.dialog.btn_frag_b_shelf.click()
    joins.client.refuse = {SIDS[1]}
    joins.client.msg = "No connection to the server"
    joins.dialog.create_new_join()
    assert joins.dialog.joins_mgr.local == [SIDS[1]]
    assert list(joins.dialog.joins_mgr.data["joins"]) == ["j1"]          # the made one, in the cache
    assert joins.said == [("information", "Success",
                           "2 joins created\nJoin saved locally. Will sync when connection is restored.")]
    assert joins.dialog.frag_b_input.text() == "" and joins.dialog._picked_b == []


# -- the real picker ----------------------------------------------------------------------------------

def _quiet(dialog):
    """Drawn, and the boxes painted for what is chosen now (tests/test_shelf_picker_desktop_multi.py)."""
    return (dialog._view is not None and not dialog._loads.busy() and not dialog._state_loads.busy()
            and dialog._paint_state is not None)


def _drive_real_picker(monkeypatch, tmp_path, joins, after_confirm):
    """The Joins dialog's own button opens the real picker on the fixture tree, at T-S AS 2 (12
    shelfmarks); the whole volume is chosen and confirmed; ``after_confirm(modal)`` runs if the
    picker is still open once the confirm has been answered."""
    from shared.shelfmark_tree_service import ShelfmarkTreeService
    from tests.shelfmark_tree_fixture import build_fixture
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: svc)
    real = shelf_picker.pick_several_from_shelf
    opened = []

    def at_volume(*a, **k):
        opened.append(dict(k))
        k["start"] = "vol:100"
        return real(*a, **k)
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", at_volume)
    stage = {"confirmed": False, "done": False}

    def drive():
        if stage["done"]:              # a timer left over once this picker closed: never the next test's
            return
        modal = QApplication.activeModalWidget()
        if not isinstance(modal, shelf_picker.ShelfPickerDialog):
            if not stage["confirmed"]:
                QTimer.singleShot(20, drive)
            return
        if not _quiet(modal) or modal._busy or not modal.leaf_panel.whole.isVisible():
            QTimer.singleShot(20, drive)
            return
        if not stage["confirmed"]:
            if not len(modal.chosen()):
                modal.leaf_panel.whole.click()
            else:
                stage["confirmed"] = True
                modal.confirm_button.click()
            QTimer.singleShot(20, drive)
            return
        after_confirm(modal)

    try:
        QTimer.singleShot(20, drive)
        joins.dialog.btn_frag_b_shelf.click()
        return opened
    finally:
        stage["done"] = True
        svc.close()


def test_the_real_picker_s_whole_volume_fills_the_box(monkeypatch, tmp_path, joins):
    seen = []
    opened = _drive_real_picker(monkeypatch, tmp_path, joins, lambda modal: (seen.append("open"), modal.reject()))
    assert opened and opened[0]["purpose"] == "join" and opened[0].get("store") is None
    assert seen == []                                                   # took all: the picker closed
    assert len(joins.dialog._picked_b) == 12 and joins.said == []
    assert joins.dialog.frag_b_input.text() == "; ".join(shelf for shelf, _sid in joins.dialog._picked_b)


def test_the_real_picker_refuses_ticks_past_the_join_s_limit(monkeypatch, tmp_path, joins):
    # the limit is the join's (JOIN_MAX: 30, here 5): the volume's 12 cannot be ticked whole, its box
    # is off, and a sixth shelfmark is refused with the limit; the five go to the box
    from shared.shelfmark_tree_service import ShelfmarkTreeService
    from tests.shelfmark_tree_fixture import build_fixture
    import shared.shelfmark_tree_limits as limits
    monkeypatch.setitem(limits.PURPOSE_LIMITS, "join", (5, None))         # the picker's limit (limit_for)
    joins.dialog.JOIN_MAX = 5                                           # and the confirm's safety
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db), thread_safe=True)
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: svc)
    real = shelf_picker.pick_several_from_shelf

    def at_volume(*a, **k):
        k["start"] = "vol:100"
        return real(*a, **k)
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", at_volume)
    seen = {}

    def drive():
        if seen.get("done"):
            return
        modal = QApplication.activeModalWidget()
        if not isinstance(modal, shelf_picker.ShelfPickerDialog) or not _quiet(modal) or modal._busy \
                or not modal.leaf_panel.whole.isVisible():
            QTimer.singleShot(20, drive)
            return
        whole = modal.leaf_panel.whole
        seen["whole"] = (whole.isEnabled(), whole.toolTip())
        modal._toggle_whole()                                           # (a click that got through)
        seen["after_whole"] = (len(modal.chosen()), modal.note.text())
        leaves = [row["leaf"] for row in modal.leaf_panel.leaves["rows"]]
        for leaf in leaves[:6]:
            modal._toggle_leaf(leaf)
        seen["ticked"] = (modal.chosen().count(modal._members_of), modal.note.text())
        seen["done"] = True
        modal.confirm_button.click()
    try:
        QTimer.singleShot(20, drive)
        joins.dialog.btn_frag_b_shelf.click()
    finally:
        seen["done"] = True
        svc.close()
    assert seen["whole"] == (False, "Too many to choose here (up to 5)")
    assert seen["after_whole"] == (0, "Too many to choose here (up to 5)")
    assert seen["ticked"] == (5, "Up to 5 fragments here")
    assert len(joins.dialog._picked_b) == 5 and joins.said == []
