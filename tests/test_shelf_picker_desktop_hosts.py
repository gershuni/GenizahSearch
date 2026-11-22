# -*- coding: utf-8 -*-
"""The desktop hosts of "Pick from the shelf" (shelf tree picker PRs 5-7, desktop half): the
Reading Desk, the Puzzle, the Join Workbench's cold-start anchor, the Joins dialog's fragment B and
Share a Discovery's references, through desktop/widgets/shelf_pick_button.py.

Pinned (handoff "Every host PR"; plan §6, §10.4): each button is there only while the shelf tree is
served (a button built before the tree opened appears when it does); a pick reaches the method the
host's typed path ends in, with what the typed path gives it for that manuscript; a LOCAL ``97``
pick reaches nothing. And the three things the Codex pre-flight found on the way: an Oxford Part
folio picked in Browse while the Reading Desk is open goes to the desk; the Puzzle's cached folio
keeps its external image; the Joins dialog keeps the record chosen from a list (text, then id).

The language is pinned and the four profile variables point at scratch folders.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QLineEdit, QSpinBox, QWidget

import genizah_core
import desktop.widgets.shelf_pick_button as pick_button
import desktop.widgets.shelfmark_tree_picker as shelf_picker

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
ROOT = Path(__file__).resolve().parents[1]
SID = "990001234560205171"
SID2 = "990001234570205171"
LOCAL = "970000000123"


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    yield


SERVICE = object()


def _served(monkeypatch, service=SERVICE):
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: service)


def _picks(monkeypatch, selection):
    asked = []

    def fake(parent, service, start=None, title=None, **kw):
        asked.append({"service": service, "start": start})
        return selection
    monkeypatch.setattr(shelf_picker, "pick_from_shelf", fake)
    return asked


def _sel(sid=SID, shelfmark="T-S AS 2.1"):
    return {"sys_id": sid, "shelfmark": shelfmark, "placement_id": f"inv:1:{sid}"}


class _Meta:
    """A catalogue that knows one shelfmark per record."""

    def __init__(self, shelves):
        self.shelves = shelves

    def get_meta_for_id(self, sid):
        return (self.shelves.get(sid, "Unknown"), "")

    def resolve_system_by_shelfmark(self, text):
        found = [s for s, shelf in self.shelves.items() if shelf == text]
        return {"sys_id": found[0] if len(found) == 1 else None, "options": [], "selected_shelfmark": text}


def _bind(obj, cls, names):
    for name in names:
        member = cls.__dict__.get(name)
        if isinstance(member, staticmethod):
            setattr(obj, name, member.__func__)
        else:
            setattr(obj, name, getattr(cls, name).__get__(obj))


def _self_calls(path, function):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function:
            return ast.unparse(node)
    raise AssertionError(f"{function} not in {path}")


# -- the button ---------------------------------------------------------------------------------

def test_the_button_is_there_only_while_the_tree_is_served(monkeypatch):
    holder = QWidget()
    _served(monkeypatch, None)
    button = pick_button.ShelfPickButton(holder, lambda _s: None)
    assert button.isHidden()                                  # flag on, file missing: absent
    _served(monkeypatch)
    pick_button.tree_state().changed.emit()                   # GenizahGUI._on_shelf_tree_opened
    assert not button.isHidden()
    assert button.toolTip() == "Pick from the shelf" and not button.autoDefault()
    later = pick_button.ShelfPickButton(holder, lambda _s: None)
    assert not later.isHidden()                               # a dialog built after the tree opened
    holder.deleteLater()


def test_a_pick_goes_to_the_host_and_a_local_one_nowhere(monkeypatch):
    holder = QWidget()
    _served(monkeypatch)
    got = []
    button = pick_button.ShelfPickButton(holder, got.append, start=lambda: "T-S AS 2")
    asked = _picks(monkeypatch, _sel())
    button.click()
    assert got == [_sel()] and asked == [{"service": SERVICE, "start": "T-S AS 2"}]
    _picks(monkeypatch, _sel(LOCAL))
    button.click()
    _picks(monkeypatch, None)                                 # closed without a pick
    button.click()
    assert got == [_sel()]
    holder.deleteLater()


def test_the_window_tells_the_buttons_when_the_tree_opens():
    body = _self_calls("genizah_app.py", "_on_shelf_tree_opened")
    assert "tree_state().changed.emit()" in body


# -- the Reading Desk -----------------------------------------------------------------------------

class _Desk:
    def __init__(self, app, meta):
        _bind(self, app.GenizahGUI, ("_browse_rd_add_by_shelfmark", "_browse_rd_add_resolved",
                                      "_browse_rd_add_from_shelf"))
        self.meta_mgr = meta
        self.browse_rd_shelf_input = QLineEdit()
        self.added = []

    def _browse_rd_add_entry(self, sid, shelfmark, sequence_order=None):
        self.added.append((sid, shelfmark))


def test_the_reading_desk_adds_a_pick_as_it_adds_a_typed_shelfmark(monkeypatch):
    import genizah_app as app
    desk = _Desk(app, _Meta({SID: "T-S AS 2.1"}))
    desk.browse_rd_shelf_input.setText("T-S AS 2.1")
    desk._browse_rd_add_by_shelfmark()
    typed = list(desk.added)
    desk.added.clear()
    desk.browse_rd_shelf_input.setText("something typed")
    desk._browse_rd_add_from_shelf(_sel(shelfmark="FGP's T-S AS 2.1"))
    assert desk.added == typed == [(SID, "T-S AS 2.1")]       # the catalogue's shelfmark, both ways
    assert desk.browse_rd_shelf_input.text() == ""
    assert "ShelfPickButton" in _self_calls("genizah_app.py", "create_browse_tab")


class _Anything:
    """A widget the code under test only pokes."""

    def __getattr__(self, _name):
        return lambda *a, **k: None


class _Browse:
    """The parts of GenizahGUI.browse_load reads, with the real method."""

    def __init__(self, app, desk_open):
        _bind(self, app.GenizahGUI, ("browse_load",))
        self.searcher = object()
        self.browse_reading_desk_active = desk_open
        self.browse_sys_input = QLineEdit(SID)
        self.browse_shelf_input = QLineEdit()
        self.browse_fl_input = QLineEdit()
        self.last_browse_field = "sys"
        self.browse_text = self.browse_viewer = self.btn_b_catalog = self.btn_b_save = _Anything()
        self.btn_b_toggle_img = _Anything()
        self.lists_mgr = None
        self.calls = []
        meta = _Meta({SID: "MS. Heb. b. 10/43"})
        meta.parse_part_identifier = lambda _text: (None, False)
        meta.get_part_for_folio = lambda sid: "MS. Heb. b. 10/43"     # an Oxford Part's folio
        self.meta_mgr = meta

    def _show_local_browse_controls(self, _visible):
        pass

    def _sync_browse_cite_button(self):
        pass

    def _browse_load_part(self, part_id, target_folio=None, from_end=False):
        self.calls.append(("part", part_id, target_folio))

    def _start_browse_enrichment(self, sid, is_part=False):
        self.calls.append(("enrich", sid))

    def _browse_rd_add_entry(self, sid, shelfmark, sequence_order=None):
        self.calls.append(("desk", sid, shelfmark))

    def browse_load_page(self):
        self.calls.append(("page",))


def test_an_oxford_part_folio_picked_while_the_desk_is_open_goes_to_the_desk(monkeypatch):
    # Codex pre-flight: browse_load sent a Part's folio to the Part view before its desk branch.
    import genizah_app as app
    monkeypatch.setattr(app, "sync_marks_legend_button", lambda *_a: None)
    open_desk = _Browse(app, desk_open=True)
    open_desk.browse_load()
    assert ("desk", SID, "MS. Heb. b. 10/43") in open_desk.calls
    assert not any(c[0] == "part" for c in open_desk.calls)
    closed = _Browse(app, desk_open=False)
    closed.browse_load()
    assert closed.calls == [("part", "MS. Heb. b. 10/43", SID)]  # as before without the desk


# -- the Puzzle -----------------------------------------------------------------------------------

class _Puzzle:
    def __init__(self, folios):
        from desktop.puzzle import PuzzleCanvasWindow
        _bind(self, PuzzleCanvasWindow, ("_on_add_shelfmark", "_add_resolved_shelfmark", "_add_from_shelf",
                                         "_set_shelfmark_lookup_busy"))
        meta = _Meta({SID: "T-S AS 2.1"})
        self.app = SimpleNamespace(meta_mgr=meta, _shelf_to_sys={genizah_core.normalize_shelfmark("T-S AS 2.1"): SID})
        self._folio_lists = folios
        self.shelfmark_input = QLineEdit()
        self.btn_add = QLineEdit()                             # anything with setEnabled
        self.btn_shelf = QLineEdit()
        self.added = []

    def add_fragment(self, sys_id, shelfmark, folio_label, fl_id, image_url='', external_provider='',
                     page_index=-1):
        self.added.append((sys_id, shelfmark, folio_label, fl_id, image_url, external_provider, page_index))

    def statusBar(self):
        return _Anything()


def test_the_puzzle_adds_a_pick_as_it_adds_a_typed_shelfmark_external_image_kept():
    image = {"label": "1r", "fl_id": "", "image_url": "https://images.example/1.jpg",
             "external_provider": "cambridge", "page_index": 0}
    typed = _Puzzle({SID: [image]})
    typed.shelfmark_input.setText("T-S AS 2.1")
    typed._on_add_shelfmark()
    picked = _Puzzle({SID: [image]})
    picked._add_from_shelf(_sel(shelfmark="FGP's T-S AS 2.1"))
    expected = [(SID, "T-S AS 2.1", "1r", "", "https://images.example/1.jpg", "cambridge", 0)]
    assert typed.added == picked.added == expected
    picked._set_shelfmark_lookup_busy(True)
    assert not picked.btn_shelf.isEnabled()                   # no pick while a typed lookup runs
    source = _self_calls("desktop/puzzle.py", "_build_ui") if "def _build_ui" in (
        ROOT / "desktop/puzzle.py").read_text(encoding="utf-8") else (ROOT / "desktop/puzzle.py").read_text(
        encoding="utf-8")
    assert "ShelfPickButton(self, self._add_from_shelf" in source


# -- the Join Workbench ---------------------------------------------------------------------------

class _Workbench:
    def __init__(self):
        from desktop.join_workbench import JoinWorkbenchWindow
        _bind(self, JoinWorkbenchWindow, ("_cold_start_open", "_cold_start_from_shelf", "_anchor_from"))
        self.meta_mgr = _Meta({SID: "T-S AS 2.1"})
        self.coldstart_input = QLineEdit()
        self.anchors = []

    def set_anchor(self, res):
        self.anchors.append(res)


def test_the_join_workbench_anchors_a_pick_as_it_anchors_a_typed_shelfmark():
    typed = _Workbench()
    typed.coldstart_input.setText("T-S AS 2.1")
    typed._cold_start_open()
    picked = _Workbench()
    picked._cold_start_from_shelf(_sel(shelfmark="FGP's T-S AS 2.1"))
    expected = [{"display": {"id": SID, "shelfmark": "T-S AS 2.1", "img": 1}, "uid": f"{SID}_P001"}]
    assert typed.anchors == picked.anchors == expected
    pane = _self_calls("desktop/join_workbench.py", "_build_right_pane")
    assert "ShelfPickButton(" in pane and "self._cold_start_from_shelf" in pane


# -- the Joins dialog -----------------------------------------------------------------------------

def _joins_dialog(monkeypatch):
    from desktop import corrections_ui
    made = []

    class _Client:
        current_user = SimpleNamespace(role="user")

        def is_logged_in(self):
            return True

        def is_server_available(self):
            return False

        def create_join(self, frag_a, frag_b, rel_type, notes, document_id_a=None, document_id_b=None):
            made.append({"frag_b": frag_b, "document_id_b": document_id_b})
            return None, "refused"

    class _Joins(corrections_ui.JoinsDialog):
        def load_joins(self, force_fresh=False):
            pass

    monkeypatch.setattr(corrections_ui.QMessageBox, "critical", lambda *_a: None)
    monkeypatch.setattr(corrections_ui.QMessageBox, "warning", lambda *_a: None)
    dialog = _Joins(None, client=_Client(), document_id=SID2, shelfmark="T-S NS 5.1")
    dialog.frag_a_input.setText("T-S NS 5.1")
    return dialog, made


def test_the_joins_dialog_keeps_the_record_picked_for_fragment_b(monkeypatch):
    _served(monkeypatch)
    dialog, made = _joins_dialog(monkeypatch)
    try:
        assert not dialog.btn_frag_b_shelf.isHidden()
        # the multiple-choice mode since DX15 (a join holds any number of fragments); one ticked is
        # this single path (tests/test_shelf_picker_desktop_joins_several.py has the several)
        asked = []

        def several(parent, service, *, purpose, on_confirm, start=None, **_kw):
            asked.append({"service": service, "start": start, "outcome": on_confirm([_sel()], "label")})
        monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", several)
        dialog.btn_frag_b_shelf.click()
        assert asked[0]["start"] == SID2                      # fragment A's neighbours on the shelf
        assert asked[0]["outcome"] is None
        assert dialog.frag_b_input.text() == "T-S AS 2.1" and dialog._selected_doc_id_b == SID
        dialog.create_new_join()
        assert made == [{"frag_b": "T-S AS 2.1", "document_id_b": SID}]
        # typing in the box afterwards forgets the record, as before
        dialog.frag_b_input.setText("T-S AS 2.2")
        assert dialog._selected_doc_id_b is None
    finally:
        dialog.deleteLater()


def test_the_joins_dialog_list_pick_keeps_its_record_too(monkeypatch):
    # Codex pre-flight: the list's pick set the id, then the text, which forgot the id.
    _served(monkeypatch, None)
    dialog, _made = _joins_dialog(monkeypatch)
    try:
        assert dialog.btn_frag_b_shelf.isHidden()             # no tree: no button
        dialog._set_fragment_b("Ms. 1358", SID)
        assert dialog._selected_doc_id_b == SID
        body = _self_calls("desktop/corrections_ui.py", "_show_list_picker")
        assert "self._set_fragment_b(" in body and "_selected_doc_id_b =" not in body
    finally:
        dialog.deleteLater()


# -- Share a Discovery ----------------------------------------------------------------------------

def test_share_a_discovery_adds_a_pick_as_it_adds_a_typed_reference(monkeypatch):
    from desktop import corrections_ui
    _served(monkeypatch)
    meta = _Meta({SID: "T-S AS 2.1"})
    dialog = corrections_ui.CreateDiscoveryDialog(None, client=object(), document_id=SID2, meta_mgr=meta)
    try:
        assert isinstance(dialog.page_input, QSpinBox) and not dialog.btn_shelf_pick.isHidden()
        dialog.shelfmark_input.setText("T-S AS 2.1")
        dialog.page_input.setValue(3)
        dialog._add_shelfmark_to_list()
        typed = list(dialog.shelfmarks_list)
        dialog.shelfmarks_list.clear()
        dialog.shelfmarks_widget.clear()
        dialog.page_input.setValue(3)
        asked = _picks(monkeypatch, _sel())
        dialog.btn_shelf_pick.click()
        assert asked[0]["start"] == SID2
        assert dialog.shelfmarks_list == typed == [("T-S AS 2.1", 3, SID)]
        assert dialog.shelfmark_input.text() == "" and dialog.page_input.value() == 0
        _picks(monkeypatch, _sel(LOCAL))
        dialog.btn_shelf_pick.click()
        assert dialog.shelfmarks_list == typed                 # a LOCAL id is never a reference
    finally:
        dialog.deleteLater()


def test_the_desk_s_picker_opens_at_the_manuscript_shown_unless_something_was_typed(monkeypatch):
    # Seen in the app: the desk fills its box with the shown manuscript's shelfmark ("CUL T-S AS
    # 39.7"), which the picker cannot start from; it opened at the libraries.
    import genizah_app as app
    desk = _Desk(app, _Meta({}))
    _bind(desk, app.GenizahGUI, ("_browse_rd_shelf_start",))
    desk._browse_shelf_start = lambda typed=True: "inv:1:" + SID
    desk.browse_rd_shelf_input.setText("CUL T-S AS 39.7")        # the desk's own filling
    assert desk._browse_rd_shelf_start() == "inv:1:" + SID
    desk.browse_rd_shelf_input.setModified(True)                 # the reader typed
    desk.browse_rd_shelf_input.setText("T-S NS 5")
    desk.browse_rd_shelf_input.setModified(True)
    assert desk._browse_rd_shelf_start() == "T-S NS 5"


def test_a_picked_fragment_b_of_the_same_shelfmark_is_another_record(monkeypatch):
    # Codex on #72: the shelfmark-equality check refused two records sharing a shelfmark.
    _served(monkeypatch)
    dialog, made = _joins_dialog(monkeypatch)
    try:
        dialog.frag_a_input.setText("Ms. 1358")
        dialog._set_fragment_b("Ms. 1358", SID)               # another record than fragment A (SID2)
        dialog.create_new_join()
        assert made == [{"frag_b": "Ms. 1358", "document_id_b": SID}]
        dialog._set_fragment_b("Ms. 1358", SID2)              # fragment A's own record
        dialog.create_new_join()
        assert len(made) == 1                                 # refused
        dialog.frag_b_input.setText("ms. 1358")               # typed: the shelfmarks decide
        dialog.create_new_join()
        assert len(made) == 1
    finally:
        dialog.deleteLater()


def test_share_a_discovery_sends_its_first_reference_s_record(monkeypatch):
    # Codex on #72: the first reference's record id was kept in the list but never sent.
    from desktop import corrections_ui
    sent = []

    class _Client:
        def create_discovery(self, **kw):
            sent.append(kw)
            return object(), "ok"

    monkeypatch.setattr(corrections_ui.QMessageBox, "information", lambda *_a: None)
    _served(monkeypatch)
    dialog = corrections_ui.CreateDiscoveryDialog(None, client=_Client(), document_id=SID2,
                                                  meta_mgr=_Meta({SID: "T-S AS 2.1"}))
    try:
        _picks(monkeypatch, _sel())
        dialog.btn_shelf_pick.click()
        dialog.title_input.setText("A join")
        dialog.content_input.setPlainText("These two belong together.")
        dialog.submit()
        assert sent and sent[0]["document_id"] == SID and sent[0]["shelfmark"] == "T-S AS 2.1"
    finally:
        dialog.deleteLater()
