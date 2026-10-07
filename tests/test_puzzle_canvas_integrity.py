# -*- coding: utf-8 -*-
"""Fragment Puzzle: work is never replaced, dropped or overwritten silently.

Drives the REAL PuzzleCanvasWindow (see puzzle_window_harness.py for what is
faked and why). Each test's docstring names the failure it guards against.

Marked `gui`: builds a real offscreen QApplication and window. Runs in CI's
per-file gui-tests job.
"""
from __future__ import annotations

import ast
import inspect
import os
import sys
import types
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402

pytestmark = pytest.mark.gui

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtGui import QCloseEvent  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import (  # noqa: E402
    QApplication, QDialog, QGraphicsTextItem, QListWidgetItem, QMessageBox,
)

import puzzle_window_harness as pwh  # noqa: E402

SB = pwh.SB
ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _qt_callback_errors_fail_the_test(monkeypatch):
    """An exception inside a Qt callback (a timer's _auto_save, scene.changed)
    aborts the whole process unless sys.excepthook is Python code. Record them
    and fail the test instead."""
    caught = []
    monkeypatch.setattr(sys, "excepthook", lambda *exc: caught.append(exc))
    yield
    assert caught == [], f"raised inside a Qt callback: {caught[0][1]!r}"


@pytest.fixture
def env(tmp_path, monkeypatch):
    e = pwh.PuzzleEnv.create(tmp_path, monkeypatch)
    yield e
    e.close()


def _dp():
    import desktop.puzzle as dp
    return dp


def _tr(text):
    return _dp().tr(text)


def _titles(env):
    return [t for t, _text, _b in env.asks]


def _rotate(env, sys_id="990001", degrees=7):
    """Rotate one fragment the way the toolbar does; the scene change that
    starts the autosave debounce is delivered."""
    env.select_only(env.item(sys_id))
    env.win._rotate_selected(degrees)
    pwh.pump()
    pwh.pump()


def _rotate_first_fragment_by_key(env):
    """R on the canvas, the way a user rotates: the edit waits for the scene
    debounce + autosave timer."""
    env.select_only(env.item("990001"))
    env.win.canvas_view.setFocus()
    pwh.pump()
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_R)
    pwh.pump()                      # scene.changed arrives; the debounce starts
    assert env.item("990001").rotation() == 1.0


EMPTY_STORE_NOTE = "The canvas is empty, so the saved join keeps its fragments. Title and notes were saved."
# A folio step onto a key another fragment holds: on the canvas, loading, or
# in the join with its image not loaded.
REFUSED_STEP = "Folio {} of this manuscript is already in the puzzle."


# ------------------------------------------------------------------
# The canvas is cleared or replaced only after the work on it is safe.

def test_new_asks_before_clearing_a_scratch_pad_that_was_only_dragged(env):
    """Adding and dragging never marked a scratch pad dirty (only flip-folio,
    z-order and Delete did), so New wiped it with no question."""
    env.scratch_pad()
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before starting new?"),
                         SB.Save | SB.Discard | SB.Cancel)]
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]


def test_opening_a_saved_join_asks_before_replacing_a_scratch_pad(env):
    doc_b = env.save(pwh.fragments("99010"), title="B")
    env.win._refresh_docs_list()
    env.scratch_pad()
    env.answer = SB.Cancel
    env.click_join(doc_b)
    pwh.finish_loads()
    assert _titles(env) == [_tr("Save current work?")]
    assert env.win._current_doc_id is None
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]


@pytest.mark.parametrize("transition", ["new", "open_join"])
@pytest.mark.parametrize("save_outcome", ["dialog_cancelled", "blank_title", "write_refused"])
def test_a_save_that_did_not_happen_keeps_the_canvas(env, monkeypatch, transition, save_outcome):
    """Choosing Save and then cancelling the title dialog (or leaving the title
    blank, or a refused write) still cleared the canvas: the callers ignored
    _on_save_join's outcome."""
    doc_b = env.save(pwh.fragments("99010"), title="B")
    env.win._refresh_docs_list()
    env.scratch_pad()
    env.answer = SB.Save
    if save_outcome == "dialog_cancelled":
        env.save_dialog_result = QDialog.DialogCode.Rejected
    else:
        env.save_dialog_result = QDialog.DialogCode.Accepted
        if save_outcome == "blank_title":
            import shared.puzzle_export as pe
            monkeypatch.setattr(pe, "auto_suggest_title", lambda frags: "")
        else:
            env.refuse_writes()
    if transition == "new":
        env.win._on_new_puzzle()
    else:
        env.win._on_doc_list_clicked(env.list_item(doc_b))
        pwh.finish_loads()
    assert env.win._current_doc_id is None
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]
    if save_outcome == "write_refused":
        assert env.notices == [("warning", _tr("Error"),
                                _tr("The puzzle could not be saved. It is still on the canvas."))]


def test_save_reports_its_outcome(env):
    env.scratch_pad()
    env.save_dialog_result = QDialog.DialogCode.Rejected
    assert env.win._on_save_join() is False
    env.save_dialog_result = QDialog.DialogCode.Accepted
    assert env.win._on_save_join() is True
    assert env.stored(env.win._current_doc_id) is not None


# -- a fragment whose image has not arrived yet is work too --

def _add_loading(env, fr):
    """Add a fragment the ordinary way; its image has not arrived."""
    env.win.add_fragment(fr.sys_id, fr.shelfmark, fr.folio_label, fr.fl_id)
    pwh.pump()
    loads = pwh.take_started()
    assert [t.key for t in loads] == [(fr.sys_id, fr.folio_label)]
    return loads[0]


def _leave(env, transition, doc_b):
    if transition == "new":
        env.win._on_new_puzzle()
        return "Save current puzzle before starting new?"
    env.click_join(doc_b)
    pwh.finish_loads()
    return "Save current puzzle before loading?"


def _sys_ids(env, doc_id):
    return sorted(s for s, _r, _x in env.stored(doc_id))


@pytest.mark.parametrize("transition", ["new", "open_join"])
def test_leaving_asks_about_a_fragment_whose_image_has_not_arrived(env, transition):
    """A fragment added, then New (or another join) before its image came:
    nothing was on the canvas, so nothing was asked, and the clear dropped
    the request -- the fragment was lost without a word."""
    doc_b = _join_b(env)
    env.win._refresh_docs_list()
    load = _add_loading(env, pwh.fragments()[0])
    env.answer = SB.Cancel
    question = _leave(env, transition, doc_b)
    assert env.asks == [(_tr("Save current work?"), _tr(question),
                         SB.Save | SB.Discard | SB.Cancel)]
    assert env.win._current_doc_id is None
    load.deliver()                      # Cancel kept the request: the image still lands
    pwh.pump()
    assert sorted(env.win._fragment_items) == [("990001", "1r")]


def test_quit_asks_about_a_fragment_whose_image_has_not_arrived(env, quit_host):
    _add_loading(env, pwh.fragments()[0])
    env.answer = SB.Cancel
    ev = _close_event()
    escaped = _run_close(quit_host, ev)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert not ev.isAccepted()
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before quitting?"),
                         SB.Save | SB.Discard | SB.Cancel)]


@pytest.mark.parametrize("transition", ["new", "open_join", "quit"])
def test_save_at_the_leave_prompt_stores_a_fragment_whose_image_has_not_arrived(env, transition):
    """Save at that prompt refused ("Add fragments before saving") or wrote
    only the canvas; the fragment on its way must be in the saved join."""
    doc_b = _join_b(env)
    env.win._refresh_docs_list()
    load = _add_loading(env, pwh.fragments()[0])
    env.answer = SB.Save
    env.save_dialog_result = QDialog.DialogCode.Accepted
    if transition == "quit":
        assert env.win.confirm_quit() is True
    else:
        _leave(env, transition, doc_b)
    assert len(env.asks) == 1
    saved = [d["id"] for d in env.svc.list_documents() if d["id"] != doc_b]
    assert len(saved) == 1, "the scratch pad was not saved"
    assert _sys_ids(env, saved[0]) == ["990001"]
    if transition == "new":
        load.deliver()                  # the cleared canvas no longer waits for it
        pwh.pump()
        assert env.win._fragment_items == {}


@pytest.mark.parametrize("transition", ["new", "open_join"])
def test_discard_at_the_leave_prompt_drops_a_fragment_whose_image_has_not_arrived(
        env, transition):
    doc_b = _join_b(env)
    env.win._refresh_docs_list()
    load = _add_loading(env, pwh.fragments()[0])
    env.answer = SB.Discard
    _leave(env, transition, doc_b)
    assert len(env.asks) == 1
    assert [d["id"] for d in env.svc.list_documents()] == [doc_b]    # nothing saved
    load.deliver()                      # late: it lands on no canvas
    pwh.pump()
    assert ("990001", "1r") not in env.win._fragment_items
    assert env.win._pending_fragments == {}


def test_new_asks_only_when_a_fragment_is_placed_or_on_its_way(env):
    """An empty scratch pad, or one whose only addition failed to load, has
    nothing to lose and asks nothing; one with an addition on its way asks."""
    env.win._on_new_puzzle()
    assert env.win.confirm_quit() is True
    failed = _add_loading(env, pwh.fragments()[0])
    failed.fail()
    pwh.pump()
    env.win._on_new_puzzle()
    assert env.win.confirm_quit() is True
    assert env.asks == []
    _add_loading(env, pwh.fragments()[1])
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert _titles(env) == [_tr("Save current work?")]


def test_leaving_a_saved_join_asks_about_an_added_fragment_still_loading(env):
    """The same loss on an open saved join: a fragment added to it is in no
    write until its image is placed, so New dropped it."""
    doc_a = env.save(pwh.fragments(), title="A")
    env.open(doc_a)
    _add_loading(env, pwh.fragments("99030")[0])
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before starting new?"),
                         SB.Save | SB.Discard | SB.Cancel)]
    env.asks.clear()
    env.answer = SB.Save
    env.win._on_new_puzzle()
    assert len(env.asks) == 1
    assert _sys_ids(env, doc_a) == ["990001", "990002", "990301"]
    assert env.win._current_doc_id is None


def test_a_manual_save_keeps_an_added_fragment_whose_image_then_fails(env):
    """Saved while its image was on its way, the fragment is in the join; the
    image then failing must not drop it from the next write."""
    frs = pwh.fragments()
    env.add(frs[0])
    load = _add_loading(env, frs[1])
    env.save_dialog_result = QDialog.DialogCode.Accepted
    assert env.win._on_save_join() is True
    doc = env.win._current_doc_id
    assert _sys_ids(env, doc) == ["990001", "990002"]
    load.fail()
    _rotate(env, degrees=5)
    env.settle()
    assert _sys_ids(env, doc) == ["990001", "990002"]
    assert _tr("image not loaded") in env.win._fragments_label.text()


def _image_arrives_during_the_prompt(env, load, answer, written):
    """An answer to the leave prompt: the image lands while it is open and
    the user takes longer than the autosave delay. What the open join holds
    at that point is recorded in `written`."""
    doc = env.win._current_doc_id

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            load.deliver()
            pwh.pump(3 * (pwh.DEBOUNCE_MS + pwh.AUTOSAVE_MS))
            written.append(_sys_ids(env, doc))
        return answer
    return _answer


@pytest.mark.parametrize("transition", ["new", "open_join", "quit"])
@pytest.mark.parametrize("answer", [SB.Discard, SB.Cancel, SB.Save],
                         ids=["discard", "cancel", "save"])
def test_an_image_that_arrives_during_the_leave_prompt_is_not_autosaved_under_it(
        env, monkeypatch, transition, answer):
    """A saved join whose only unsaved work was a fragment still loading: its
    image arrived while the leave prompt was open, the prompt's event loop
    ran the autosave, and the join held the fragment before the user chose
    Discard -- the clear that followed could not take it out again."""
    doc_a = env.save(pwh.fragments(), title="A")
    doc_b = _join_b(env)
    env.open(doc_a)
    env.win._refresh_docs_list()
    load = _add_loading(env, pwh.fragments("99030")[0])
    saves = env.record_saves(monkeypatch)
    written = []
    env.answer = _image_arrives_during_the_prompt(env, load, answer, written)
    if transition == "quit":
        assert env.win.confirm_quit() is (answer != SB.Cancel)
    else:
        _leave(env, transition, doc_b)
    assert len(env.asks) == 1
    assert written == [["990001", "990002"]], "the join was written while the prompt was open"
    env.settle()
    # The flush before the question may write the join as it was; only a
    # write holding the new fragment is at issue.
    holding_it = [keys for doc_id, keys, _thumb in saves
                  if doc_id == doc_a and ("990301", "1r") in keys]
    if answer == SB.Discard:
        assert _sys_ids(env, doc_a) == ["990001", "990002"]
        assert holding_it == []
    else:
        # Cancel: the autosave held back runs once the prompt is closed.
        # Save: the Save writes it, once.
        assert _sys_ids(env, doc_a) == ["990001", "990002", "990301"]
        assert len(holding_it) == 1


def test_new_writes_the_pending_autosave_first(env):
    """New set _current_doc_id = None while the save was still pending, so
    when the timer fired it returned without writing (~2 s of edits lost)."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    _rotate_first_fragment_by_key(env)
    env.win._on_new_puzzle()
    assert env.stored(doc)[0][1] == 1.0
    assert env.asks == []           # nothing unsaved remained, so nothing to ask


def test_opening_another_join_writes_the_pending_autosave_first(env):
    """_load_document set _loading_document = True, so the pending save of
    the join being left returned without writing."""
    doc_a = env.save(pwh.fragments(), title="A")
    doc_b = env.save(pwh.fragments("99010"), title="B")
    env.open(doc_a)
    env.win._refresh_docs_list()
    _rotate_first_fragment_by_key(env)
    env.click_join(doc_b)
    assert env.stored(doc_a)[0][1] == 1.0
    assert env.win._current_doc_id == doc_b


def test_clicking_the_open_join_again_keeps_the_edit(env):
    """Reloading the join on screen read the stored copy BEFORE the pending
    save of that same join, so the canvas (and then the file) went back."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.win._refresh_docs_list()
    _rotate_first_fragment_by_key(env)
    env.win._on_doc_list_clicked(env.list_item(doc))
    pwh.finish_loads()
    pwh.pump()
    assert env.stored(doc)[0][1] == 1.0
    assert env.item("990001").rotation() == 1.0


def test_new_leaves_no_hidden_title_or_note_behind(env):
    """The Details panel is hidden on a scratch pad, but a note left in it
    from the join just closed counts as work: New on the empty scratch pad
    then asked to save it."""
    doc = env.save(pwh.fragments(), title="A", notes="a note on A")
    env.open(doc)
    env.win._on_new_puzzle()
    assert (env.win._title_edit.text(), env.win._notes_edit.toPlainText()) == ("", "")
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == []


def test_deleting_the_open_join_leaves_a_scratch_pad_of_what_is_on_the_canvas(env):
    """Deleting the open join from Saved Joins leaves its canvas as an unsaved
    scratch pad. The deleted join's fragment that was never shown is not part
    of it -- a join saved from that canvas must not get it -- and the
    deleted join's failed save is no longer reported."""
    doc = env.save(pwh.fragments(), title="A", notes="a note on A")
    env.open(doc, fail_ids={"99000FL2"})
    # 990002 is kept in the join, but not on the canvas
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]
    assert sorted(env.win._fragment_items) == [("990001", "1r")]
    _fail_an_autosave(env)
    assert env.win._save_failed_label.isVisible()
    env.static_answers[_tr("Delete join?")] = SB.Yes
    env.win._delete_document(doc)
    assert env.stored(doc) is None
    assert env.win._current_doc_id is None
    assert not env.win._save_failed_label.isVisible()
    assert (env.win._title_edit.text(), env.win._notes_edit.toPlainText()) == ("", "")
    env.answer = SB.Cancel
    env.win._on_new_puzzle()                            # the canvas stayed, as a scratch pad
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before starting new?"),
                         SB.Save | SB.Discard | SB.Cancel)]
    env.save_dialog_result = QDialog.DialogCode.Accepted
    assert env.win._on_save_join() is True
    new_doc = env.win._current_doc_id
    assert new_doc not in (None, doc)
    _rotate(env, degrees=6)
    env.settle()
    assert env.stored(new_doc) == [("990001", env.item("990001").rotation(),
                                    env.item("990001").pos().x())]


def test_closing_the_window_writes_the_pending_autosave(env):
    """closeEvent only waited for threads; the app can quit before the timer."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    _rotate_first_fragment_by_key(env)
    env.win.close()
    assert env.stored(doc)[0][1] == 1.0


def test_a_failed_autosave_is_reported_and_leaving_asks(env):
    """save_document returned None and the status bar still said
    'Auto-saved'; New then cleared the canvas without a word."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.refuse_writes()
    env.item("990001").setSelected(True)
    env.win._rotate_selected(9)
    assert env.win._auto_save() is False                 # the timer's slot
    label = env.win._save_failed_label
    assert label.isVisible()
    assert label.text() == _tr("Auto-save failed: the latest changes to this join are not saved.")
    env.win.statusBar().showMessage("x", 50)
    pwh.pump(150)                                       # the temporary message expired
    assert label.isVisible()
    assert env.stored(doc)[0][1] == 0.0
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert len(env.asks) == 1
    assert env.win._current_doc_id == doc and len(env.win._fragment_items) == 2


def test_autosave_does_not_bring_back_a_join_deleted_elsewhere(env):
    """The open join's row disappeared (deleted outside this window, or
    joins.db could not read it): the autosave wrote nothing and said
    nothing. It must say so -- and must not quietly re-create the join;
    only an explicit Save may."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.svc.delete_document(doc)
    _rotate(env, degrees=5)
    env.settle()
    assert [n[1] for n in env.notices] == [_tr("Auto-save failed")]
    assert env.stored(doc) is None
    assert env.win._last_save_failed is True
    assert env.win._on_save_join() is True               # Save re-creates it
    assert [(s, r) for s, r, _x in env.stored(doc)] == [("990001", 5.0), ("990002", 0.0)]


def test_autosave_never_empties_a_join_whose_images_all_failed(env):
    """Offline: every image fails, the canvas is empty, a note is typed, and
    the autosave wrote fragments=[] over the join (and the note with it)."""
    doc = env.save(pwh.fragments())
    env.win._load_document(doc)
    pwh.finish_loads(fail_ids={"99000FL1", "99000FL2"})
    pwh.pump()
    env.win._notes_edit.setPlainText("typed while offline")
    env.win._auto_save()
    assert [s for s, _r, _x in env.stored(doc)] == ["990001", "990002"]
    assert env.stored_doc(doc).notes == "typed while offline"


# -- a join opened while images cannot be downloaded (folded in) --

def test_open_with_one_image_failing_keeps_both_fragments(env):
    """The load's own autosave wrote only the fragments whose images arrived:
    one of two was dropped from the join with no user action."""
    doc = env.save(pwh.fragments())
    env.win._load_document(doc)
    pwh.finish_loads(fail_ids={"99000FL2"})
    env.settle()
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]
    assert env.win._loading_document is False
    # the user is told, and the autosave's own line does not replace it
    assert env.win.statusBar().currentMessage() == _tr(
        "Fragments whose images could not be loaded: {}. They stay in the join.").format(1)
    # ...and the Details list names the one that is not shown
    assert env.win._fragments_label.text().split("\n") == [
        "T-S A 1 (1r)", "T-S B 2 (1r) -- " + _tr("image not loaded")]


def test_new_right_after_an_offline_open_keeps_both_fragments(env):
    """Regression pin for the flush: writing the pending autosave before New
    must not write the truncated canvas."""
    doc = env.save(pwh.fragments())
    env.win._load_document(doc)
    pwh.finish_loads(fail_ids={"99000FL2"})
    pwh.pump(10)
    env.answer = SB.Discard
    env.win._on_new_puzzle()
    env.settle()
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]


def test_manual_save_after_a_partial_load_keeps_both_fragments(env):
    doc = env.save(pwh.fragments())
    env.win._load_document(doc)
    pwh.finish_loads(fail_ids={"99000FL2"})
    env.win._auto_save_timer.stop()
    env.win._scene_change_debounce.stop()
    saved = env.win._on_save_join()
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]
    assert saved is True


@pytest.mark.parametrize("failed", [("99000FL2",), ("99000FL1", "99000FL2")],
                         ids=["one_failed", "all_failed"])
def test_publish_sends_the_fragments_that_could_not_be_shown(env, monkeypatch, failed):
    """Publish sent only the fragments on the canvas, and refused a join
    whose images had all failed ("Add fragments before publishing")."""
    captured = []

    class Client:
        current_user = {"id": "u"}

        def check_is_published(self, _doc_id):
            return False

        def publish_puzzle_join(self, doc):
            captured.append(doc)
            return True, "ok"

    env.host.corrections_client = Client()
    dp = _dp()
    monkeypatch.setattr(dp.PuzzlePublishThread, "start", lambda self: self.run())
    env.static_answers[_tr("Publish to Community")] = SB.Yes
    env.static_answers[_tr("Published")] = None
    doc = env.save(pwh.fragments())
    env.open(doc, fail_ids=set(failed))
    env.win._on_publish()
    assert ("warning", _tr("No Fragments")) not in env.static_calls
    assert len(captured) == 1
    assert sorted(f.sys_id for f in captured[0].fragments) == ["990001", "990002"]


def _external_fragments():
    from shared.puzzle_model import PuzzleFragment
    return [PuzzleFragment(sys_id="990101", folio_label="1r", fl_id="", image_url="https://x/a.jpg",
                           shelfmark="Ext A", x=10.0, y=20.0),
            PuzzleFragment(sys_id="990102", folio_label="1r", fl_id="", image_url="https://x/b.jpg",
                           shelfmark="Ext B", x=300.0, y=40.0)]


def test_external_fragment_that_fails_stays_in_the_join(env):
    """External fragments have no fl_id and the loader reports their image
    URL; the failure was matched by fl_id, so it never matched: the fragment
    stayed 'pending' for ever and the next write dropped it."""
    doc = env.save(_external_fragments())
    env.win._load_document(doc)
    a, b = pwh.take_started()
    b.fail()
    a.deliver()
    env.settle()
    assert sorted(env.win._fragment_items) == [("990101", "1r")]
    assert env.win._pending_fragments == {}
    assert env.win._loading_document is False
    env.win._notes_edit.setPlainText("typed after the load")
    env.settle()
    stored = env.stored_doc(doc)
    assert stored.notes == "typed after the load"
    assert sorted(f.sys_id for f in stored.fragments) == ["990101", "990102"]


def test_an_added_external_fragment_that_fails_leaves_no_loading_placeholder(env):
    env.win.add_fragment("990103", "Ext C", "1r", "", image_url="https://x/c.jpg")
    (loader,) = pwh.take_started()
    loader.fail()
    pwh.pump()
    texts = [i for i in env.win.canvas_view.scene.items() if isinstance(i, QGraphicsTextItem)]
    assert texts == []
    assert env.win._placeholder_items == {}
    assert env.win._pending_fragments == {}


def test_edit_while_images_load_then_new_is_saved(env):
    """_auto_save returned while any image of the join was still loading, so
    an edit made then was dropped by New."""
    doc = env.save(pwh.fragments())
    env.win._load_document(doc)
    first, _second = pwh.take_started()
    first.deliver()                  # the other image is still loading
    _rotate(env, degrees=7)
    env.win._on_new_puzzle()
    stored = {s: r for s, r, _x in env.stored(doc)}
    assert stored == {"990001": 7.0, "990002": 0.0}
    assert env.asks == []


def test_new_during_a_load_leaves_autosave_working(env):
    """New pressed while a join's images loaded left the load guard armed on
    the next canvas, so a join saved from it never autosaved."""
    doc_a = env.save(pwh.fragments("99010"), title="A")
    env.win._load_document(doc_a)    # its images never arrive
    env.win._on_new_puzzle()
    env.add(pwh.fragments()[0])      # also delivers A's late images
    env.save_dialog_result = QDialog.DialogCode.Accepted
    env.win._on_save_join()
    doc_s = env.win._current_doc_id
    assert doc_s is not None
    env.settle()
    _rotate(env, degrees=5)
    env.settle()
    assert env.stored(doc_s)[0][1] == 5.0


def test_late_result_from_the_previous_canvas_is_ignored(env):
    """A late failure from join A's load, matched by fl_id, removed the same
    fragment's pending entry from fork B that replaced it: B lost it."""
    doc_a = env.save(pwh.fragments(), title="A")
    b_frags = pwh.fragments()
    b_frags[1].sys_id, b_frags[1].fl_id = "990003", "99000FL3"
    doc_b = env.save(b_frags, title="B")
    env.win._load_document(doc_a)
    a_loaders = pwh.take_started()
    env.win._load_document(doc_b)
    b_loaders = pwh.take_started()
    a_loaders[0].fail()              # A's result for the key B also holds
    for t in b_loaders:
        t.deliver()
    env.settle()
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990003", "1r")]
    assert sorted(s for s, _r, _x in env.stored(doc_b)) == ["990001", "990003"]
    # A's request for 990002 never answered; B's canvas does not wait for it.
    assert env.win._pending_req == {}


def test_a_join_opened_after_new_cut_a_load_short_finishes_its_own_load(env):
    """New during join A's load, then join B with one image failing: B's load
    must end (autosave and the fit depend on it) and say what it could not
    show, whatever A's load was still waiting for."""
    doc_a = env.save(pwh.fragments("99010"), title="A")
    doc_b = env.save(pwh.fragments(), title="B")
    env.win._load_document(doc_a)
    pwh.take_started()               # A's images never arrive
    env.win._on_new_puzzle()
    env.win._load_document(doc_b)
    assert env.asks == []
    first, second = pwh.take_started()
    second.fail()
    first.deliver()
    pwh.pump()
    assert env.win.statusBar().currentMessage() == _tr(
        "Fragments whose images could not be loaded: {}. They stay in the join.").format(1)
    assert env.win._loading_document is False
    env.settle()
    assert sorted(s for s, _r, _x in env.stored(doc_b)) == ["990001", "990002"]


def test_a_partial_canvas_keeps_the_stored_thumbnail(env, monkeypatch):
    """The autosave drew the thumbnail from the canvas alone, so a join with
    a failed image got a thumbnail without that fragment."""
    saves = env.record_saves(monkeypatch)
    doc = env.save(pwh.fragments(), thumbnail="OLD")
    saves.clear()
    env.open(doc, fail_ids={"99000FL2"})
    assert saves and saves[-1][2] is None
    assert env.stored_thumbnail(doc) == "OLD"
    assert all(("990002", "1r") not in call for call in env.thumb_calls)
    doc2 = env.save(pwh.fragments("99010"), thumbnail="OLD")
    env.answer = SB.Discard
    env.open(doc2)
    _rotate(env, sys_id="990101")
    env.settle()
    assert saves[-1][2] == "THUMB"
    assert env.stored_thumbnail(doc2) == "THUMB"
    assert env.thumb_calls[-1] == [("990101", "1r"), ("990102", "1r")]


def test_thumbnail_error_does_not_fail_the_save(env):
    """generate_thumbnail raising inside the autosave aborted the save (and,
    in a timer slot, escaped into Qt)."""
    doc = env.save(pwh.fragments(), thumbnail="OLD")
    env.thumb_error = OSError("disk cache unreadable")
    env.open(doc)
    _rotate(env, degrees=4)
    env.settle()
    assert env.stored(doc)[0][1] == 4.0
    assert env.stored_thumbnail(doc) == "OLD"
    assert env.win._last_save_failed is False


def test_failed_autosave_label_stays_until_the_next_save(env):
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.refuse_writes()
    _rotate(env, degrees=3)
    env.settle()
    label = env.win._save_failed_label
    assert label.isVisible()
    env.win.statusBar().showMessage(_tr("Auto-saved"), 50)
    pwh.pump(150)
    assert label.isVisible()
    env.allow_writes()
    _rotate(env, degrees=3)
    env.settle()
    assert not label.isVisible()
    assert env.stored(doc)[0][1] == 6.0


def test_first_autosave_failure_shows_one_box_per_join(env):
    failure_box = ("warning", _tr("Auto-save failed"), _tr(
        "The latest changes to this join could not be saved. They are still on the canvas. "
        "Saving is tried again after your next change, and you will be asked before you "
        "leave this join."))
    doc_a = env.save(pwh.fragments(), title="A")
    doc_b = env.save(pwh.fragments("99010"), title="B")
    doc_c = env.save(pwh.fragments("99020"), title="C")
    env.open(doc_a)
    env.refuse_writes()
    _rotate(env)
    env.settle()
    _rotate(env)
    env.settle()
    assert env.notices == [failure_box]                 # two failures, one box
    env.answer = SB.Discard
    env.open(doc_b)                                     # leaving A asks (Discard)
    _rotate(env, sys_id="990101")
    env.settle()
    assert env.notices == [failure_box, failure_box]    # B gets its own box
    env.allow_writes()
    env.open(doc_c)                                     # leaving B asks (Discard)
    env.refuse_writes()
    _rotate(env, sys_id="990201")                       # pending when New is pressed
    env.asks.clear()
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == [(_tr("Save current work?"),
                         _tr("The last changes to this join could not be saved."),
                         SB.Save | SB.Discard | SB.Cancel)]
    assert len(env.notices) == 2                        # the prompt said it; no box
    assert env.win._current_doc_id == doc_c


def test_leave_prompt_names_a_failed_save(env):
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.refuse_writes()
    _rotate(env, degrees=8)
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == [(_tr("Save current work?"),
                         _tr("The last changes to this join could not be saved."),
                         SB.Save | SB.Discard | SB.Cancel)]
    assert env.win._current_doc_id == doc and len(env.win._fragment_items) == 2
    env.answer = SB.Save                                # Save retries...
    env.win._on_new_puzzle()
    assert env.notices[-1] == ("warning", _tr("Error"),
                               _tr("The puzzle could not be saved. It is still on the canvas."))
    assert env.win._current_doc_id == doc and len(env.win._fragment_items) == 2
    env.allow_writes()                                  # ...and lands once it can
    env.win._on_new_puzzle()
    assert env.stored(doc)[0][1] == 8.0
    assert env.win._current_doc_id is None and env.win._fragment_items == {}


def _fail_an_autosave(env, sys_id="990001"):
    env.refuse_writes()
    _rotate(env, sys_id=sys_id, degrees=3)
    env.settle()
    env.allow_writes()


def test_the_failed_save_warning_goes_once_the_join_is_saved_or_left(env):
    """The failure belongs to the join and its unsaved changes: a toolbar
    Save that lands, or leaving the join with Discard, ends it. Otherwise
    the red line stays on screen and the next New asks about changes that
    were saved."""
    doc_a = env.save(pwh.fragments(), title="A")
    doc_b = env.save(pwh.fragments("99010"), title="B")
    env.open(doc_a)
    _fail_an_autosave(env)
    assert [n[1] for n in env.notices] == [_tr("Auto-save failed")]
    label = env.win._save_failed_label
    assert label.isVisible()
    # a toolbar Save that lands
    assert env.win._on_save_join() is True
    assert not label.isVisible()
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    assert env.asks == []                               # nothing was left to ask about
    assert env.win._current_doc_id is None
    # Discard, then New
    env.open(doc_a)
    _fail_an_autosave(env)
    assert label.isVisible()
    env.answer = SB.Discard
    env.win._on_new_puzzle()
    assert len(env.asks) == 1
    assert not label.isVisible() and env.win._last_save_failed is False
    # Discard, then another join: B has not failed (not even its load's
    # own write has run yet)
    env.open(doc_a)
    _fail_an_autosave(env)
    assert label.isVisible()
    env.win._load_document(doc_b)
    assert env.win._current_doc_id == doc_b
    assert not label.isVisible() and env.win._last_save_failed is False
    pwh.finish_loads()
    env.settle()
    assert not label.isVisible()


def test_prompts_label_their_buttons_in_the_interface_language(monkeypatch):
    """The static QMessageBox calls label Save/Discard/Cancel in Qt's own
    language, i.e. in English in the Hebrew interface."""
    dp = _dp()
    monkeypatch.setattr(dp, "tr", lambda text: "«" + text + "»")
    shown = []
    monkeypatch.setattr(QMessageBox, "exec",
                        lambda box: shown.append([b.text() for b in box.buttons()]) or 0)
    assert dp._ask(None, "t", "x", SB.Save | SB.Discard | SB.Cancel) == SB.Cancel
    assert dp._ask(None, "t", "x", SB.Yes | SB.No) == SB.No
    assert dp._ask(None, "t", "x", SB.Save | SB.Cancel) == SB.Cancel
    dp._notify(None, "warning", "t", "x")
    assert [len(texts) for texts in shown] == [3, 2, 2, 1]
    for texts in shown:
        assert all(t.startswith("«") and t.endswith("»") for t in texts), texts


# -- moving fragments between folios --

def _both_sides(env):
    from shared.puzzle_model import PuzzleFragment
    doc = env.save([PuzzleFragment(sys_id="990001", folio_label="1r", fl_id="FLA",
                                   shelfmark="T-S A 1", x=10.0, y=20.0),
                    PuzzleFragment(sys_id="990001", folio_label="1v", fl_id="FLB",
                                   shelfmark="T-S A 1", x=300.0, y=20.0)])
    env.open(doc)
    env.win._folio_lists["990001"] = [{"fl_id": "FLA", "label": "1r"},
                                      {"fl_id": "FLB", "label": "1v"}]
    return doc


def _keys_match_labels(env):
    return all(k == (it.puzzle_frag.sys_id, it.puzzle_frag.folio_label)
               for k, it in env.win._fragment_items.items())


@pytest.mark.parametrize("entry", ["navigate_next", "flip_recto_verso"])
def test_stepping_onto_a_folio_already_on_the_canvas_keeps_both(env, entry):
    """Stepping the recto onto the verso that was already on the canvas
    replaced the verso's entry: it stayed drawn but untracked, and every
    later write dropped it."""
    doc = _both_sides(env)
    env.select_only(env.item("990001", "1r"))
    if entry == "navigate_next":
        env.win._navigate_folio(+1)
    else:
        env.win._flip_recto_verso()
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990001", "1v")]
    assert _keys_match_labels(env)
    assert env.win.statusBar().currentMessage() == _tr(REFUSED_STEP).format("1v")
    env.settle()
    assert sorted(f.folio_label for f in env.stored_doc(doc).fragments) == ["1r", "1v"]


def test_a_step_onto_a_folio_whose_image_failed_is_refused(env):
    """The other side is in the join but not on the canvas (its image
    failed): the step is refused all the same, and the refusal must not
    say that folio is on the canvas."""
    from shared.puzzle_model import PuzzleFragment
    doc = env.save([PuzzleFragment(sys_id="990001", folio_label=label, fl_id="FL-" + label,
                                   shelfmark="T-S A 1", x=10.0 + 300 * i, y=20.0)
                    for i, label in enumerate(["1r", "1v"])])
    env.open(doc, fail_ids=("FL-1v",))
    env.win._folio_lists["990001"] = [{"fl_id": "FL-1r", "label": "1r"},
                                      {"fl_id": "FL-1v", "label": "1v"}]
    assert sorted(env.win._fragment_items) == [("990001", "1r")]
    env.select_only(env.item("990001", "1r"))
    env.win._navigate_folio(+1)
    assert sorted(env.win._fragment_items) == [("990001", "1r")]
    assert env.win.statusBar().currentMessage() == _tr(REFUSED_STEP).format("1v")
    from shared.genizah_translations import TRANSLATIONS
    assert TRANSLATIONS[REFUSED_STEP].count("{}") == 1     # the Hebrew line names the folio
    env.settle()
    assert sorted(f.folio_label for f in env.stored_doc(doc).fragments) == ["1r", "1v"]


@pytest.mark.parametrize("entry", ["flip_entire_puzzle", "flip_both_selected"])
def test_flipping_the_whole_puzzle_with_both_sides_on_the_canvas_swaps_them(env, entry):
    doc = _both_sides(env)
    recto, verso = env.item("990001", "1r"), env.item("990001", "1v")
    if entry == "flip_entire_puzzle":
        env.win._flip_entire_puzzle()
    else:
        env.select_only(recto, verso)
        env.win._flip_recto_verso()
    assert env.win._fragment_items.get(("990001", "1v")) is recto
    assert env.win._fragment_items.get(("990001", "1r")) is verso
    assert recto.puzzle_frag.folio_label == "1v" and verso.puzzle_frag.folio_label == "1r"
    pwh.finish_loads()
    env.settle()
    assert len(env.win._fragment_items) == 2
    assert sorted(f.folio_label for f in env.stored_doc(doc).fragments) == ["1r", "1v"]


def _one_manuscript(env, labels):
    """Pages of manuscript 990001 on a saved join's canvas, one per label,
    with the folio list 1r, 1v, 2r."""
    from shared.puzzle_model import PuzzleFragment
    doc = env.save([PuzzleFragment(sys_id="990001", folio_label=label, fl_id="FL-" + label,
                                   shelfmark="T-S A 1", x=10.0 + 300 * i, y=20.0)
                    for i, label in enumerate(labels)])
    env.open(doc)
    env.win._folio_lists["990001"] = [{"fl_id": "FL-" + label, "label": label}
                                      for label in ("1r", "1v", "2r")]
    return doc


def test_a_refused_folio_step_also_refuses_the_step_onto_it(env):
    """1r and 1v selected, 2r also on the canvas, next folio: 1v -> 2r is
    refused, so 1v stays -- and 1r -> 1v must then be refused too, or it
    overwrites 1v's entry and 1v drops out of every later write."""
    doc = _one_manuscript(env, ["1r", "1v", "2r"])
    before = dict(env.win._fragment_items)
    env.select_only(before[("990001", "1r")], before[("990001", "1v")])
    env.win._navigate_folio(+1)
    assert env.win._fragment_items == before
    assert _keys_match_labels(env)
    env.settle()
    assert sorted(f.folio_label for f in env.stored_doc(doc).fragments) == ["1r", "1v", "2r"]


def test_two_fragments_stepping_onto_the_same_folio_keeps_both(env):
    """Two selected pages that step onto the same folio in one move (the
    second's page is not in the folio list, so it counts from the first
    folio): one moves, the other stays. Both moving put two items on one
    key, and one of them dropped out of every later write."""
    doc = _one_manuscript(env, ["1r", "x9"])
    items = set(env.win._fragment_items.values())
    env.select_only(*items)
    env.win._navigate_folio(+1)
    assert len(env.win._fragment_items) == 2
    assert set(env.win._fragment_items.values()) == items
    assert _keys_match_labels(env)
    assert env.win.statusBar().currentMessage() == _tr(REFUSED_STEP).format("1v")
    pwh.finish_loads()
    env.settle()
    assert len(env.stored_doc(doc).fragments) == 2


@pytest.mark.parametrize("flip", ["_flip_recto_verso", "_flip_entire_puzzle"])
def test_flipping_an_external_fragment_loads_the_other_side_by_its_url(env, flip):
    """Regression pin (green on the base by design): the three folio-move
    sites now share one re-keying helper, and each must still point the
    fragment at the new page and pass the loader what it passed before. An
    external library's pages have no fl_id: the flip buttons fetch the other
    side by its image URL."""
    env.win.add_fragment("990103", "Ext C", "1r", "", image_url="https://x/c1.jpg", page_index=0)
    pwh.take_started()[0].deliver()
    pwh.pump()
    env.win._folio_lists["990103"] = [
        {"fl_id": "", "label": "1r", "image_url": "https://x/c1.jpg", "page_index": 0},
        {"fl_id": "", "label": "1v", "image_url": "https://x/c2.jpg", "page_index": 1}]
    item = env.item("990103")
    pf = item.puzzle_frag
    thr = pf.bg_removal_threshold
    env.select_only(item)
    getattr(env.win, flip)()
    (loader,) = pwh.take_started()
    assert (pf.folio_label, pf.fl_id, pf.image_url, pf.page_index) == (
        "1v", "", "https://x/c2.jpg", 1)
    assert loader.fl_id == ""
    assert loader.kwargs == dict(threshold=thr, processed=thr > 0,
                                 is_cul=env.win._has_blue_mat(pf), image_url="https://x/c2.jpg")
    loader.deliver(pwh.png_bytes(50, 20))
    assert env.win._fragment_items[("990103", "1v")] is item
    assert (item.pixmap().width(), item.pixmap().height()) == (50, 20)


def test_a_reload_that_arrives_after_the_fragment_moved_on_is_dropped(env):
    """A threshold reload still in flight when the fragment stepped to the
    next folio arrived under the old key and made a second item around the
    same fragment object."""
    env.add(pwh.fragments()[0])
    env.win._folio_lists["990001"] = [{"fl_id": "99000FL1", "label": "1r"},
                                      {"fl_id": "99000FL9", "label": "1v"}]
    env.select_only(env.item("990001"))
    env.win.slider_threshold.blockSignals(True)
    env.win.slider_threshold.setValue(60)
    env.win.slider_threshold.blockSignals(False)
    env.win._on_threshold_changed()
    (reload_a,) = pwh.take_started()
    env.win._navigate_folio(+1)
    (reload_b,) = pwh.take_started()
    # The superseded reload is no longer waited for, in either of the two
    # dicts that must always hold the same keys.
    assert set(env.win._pending_fragments) == {("990001", "1v")}
    assert set(env.win._pending_req) == {("990001", "1v")}
    reload_a.deliver(pwh.png_bytes(30, 40))
    reload_b.deliver(pwh.png_bytes(50, 20))
    pwh.pump()
    assert list(env.win._fragment_items) == [("990001", "1v")]
    item = env.win._fragment_items[("990001", "1v")]
    assert (item.pixmap().width(), item.pixmap().height()) == (50, 20)
    assert len(env.win._build_fragments_list()) == 1
    assert env.win._pending_req == {}


@pytest.mark.parametrize("flip", ["_flip_recto_verso", "_flip_entire_puzzle"])
def test_both_flip_buttons_load_through_the_request_seam(env, flip):
    """The two flip buttons start their own image loads; a callback change
    that missed them would raise on their first result."""
    env.add(pwh.fragments()[0])
    env.win._folio_lists["990001"] = [{"fl_id": "99000FL1", "label": "1r"},
                                      {"fl_id": "99000FL9", "label": "1v"}]
    env.select_only(env.item("990001"))
    getattr(env.win, flip)()
    (first,) = pwh.take_started()
    escaped = pwh.call_catching(first.deliver, pwh.png_bytes(50, 20))
    assert escaped is None, f"the flip's result raised {escaped!r}"
    item = env.win._fragment_items[("990001", "1v")]
    assert (item.pixmap().width(), item.pixmap().height()) == (50, 20)
    env.select_only(item)
    getattr(env.win, flip)()
    (second,) = pwh.take_started()
    env.answer = SB.Discard
    env.win._on_new_puzzle()
    assert _titles(env) == [_tr("Save current work?")]
    escaped = pwh.call_catching(second.deliver)
    assert escaped is None, f"a late flip result raised {escaped!r}"
    assert env.win._fragment_items == {}
    assert env.win.canvas_view.get_fragment_items() == []


def _functions(tree):
    return [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]


def _inside(node, fn):
    return fn.lineno <= node.lineno <= fn.end_lineno


def test_every_loader_goes_through_start_image_load():
    """Six places built and connected their own image loader; a stale-result
    check added to some of them could miss the others."""
    tree = ast.parse((ROOT / "desktop" / "puzzle.py").read_text(encoding="utf-8"))
    seams = [f for f in _functions(tree) if f.name == "_start_image_load"]
    assert len(seams) == 1, "no single _start_image_load seam"
    seam = seams[0]
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "PuzzleImageLoaderThread"]
    assert len(calls) == 1 and _inside(calls[0], seam)
    slots = [n for n in ast.walk(tree) if isinstance(n, ast.Attribute)
             and n.attr in ("_on_image_loaded", "_on_image_failed")]
    assert slots and all(_inside(n, seam) for n in slots)


def test_rename_keeps_an_autosave_made_while_the_dialog_was_open(env):
    """Rename read the join before its modal dialog and wrote that copy
    after it, over an autosave that landed while the dialog was open."""
    doc = env.save(pwh.fragments(), title="old")
    env.open(doc)

    def _dialog(*_args):
        _rotate(env, degrees=33)
        env.settle()
        assert env.stored(doc)[0][1] == 33.0
        return "renamed", True

    env.static_answers[_tr("Rename")] = _dialog
    env.win._rename_document(doc)
    stored = env.stored_doc(doc)
    assert stored.title == "renamed"
    assert stored.fragments[0].rotation == 33.0


def test_rename_that_cannot_be_written_says_so(env):
    doc = env.save(pwh.fragments(), title="old")
    env.open(doc)
    env.win._refresh_docs_list()
    list_text = env.list_item(doc).text()
    window_title = env.win.windowTitle()
    env.refuse_writes()
    env.static_answers[_tr("Rename")] = ("new title", True)
    env.win._rename_document(doc)
    assert env.notices == [("warning", _tr("Error"), _tr("The join could not be renamed."))]
    assert env.win._title_edit.text() == "old"
    assert env.list_item(doc).text() == list_text
    assert env.win.windowTitle() == window_title


def test_an_explicit_save_of_an_empty_saved_join_asks_first(env, monkeypatch):
    """Save on a saved join whose fragments were all removed wrote the empty
    canvas over the join without a question."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.answer = SB.Yes
    env.select_only(*env.win._fragment_items.values())
    env.win._delete_selected()
    assert env.win._fragment_items == {}
    env.settle()
    saves = env.record_saves(monkeypatch)
    env.asks.clear()
    env.win._notes_edit.blockSignals(True)
    env.win._notes_edit.setPlainText("note after emptying")
    env.win._notes_edit.blockSignals(False)
    env.answer = SB.Cancel
    assert env.win._on_save_join() is False
    assert [t for t in env.asks] == [(_tr("Save"), _tr(
        "The canvas is empty. Save only the title and notes? The saved join keeps its fragments."),
        SB.Save | SB.Cancel)]
    assert saves == []
    env.answer = SB.Save
    assert env.win._on_save_join() is True
    stored = env.stored_doc(doc)
    assert sorted(f.sys_id for f in stored.fragments) == ["990001", "990002"]
    assert stored.notes == "note after emptying"
    assert env.win.statusBar().currentMessage() == _tr(EMPTY_STORE_NOTE)
    env.asks.clear()
    env.win.statusBar().clearMessage()
    env.win._notes_edit.setPlainText("changed again")    # the autosave path
    env.settle()
    assert env.asks == []
    stored = env.stored_doc(doc)
    assert sorted(f.sys_id for f in stored.fragments) == ["990001", "990002"]
    assert stored.notes == "changed again"
    assert env.win.statusBar().currentMessage() == _tr(EMPTY_STORE_NOTE)


def test_closing_the_window_with_a_failing_pending_save_shows_the_first_failure_box(env):
    """Closing with X only hides the window and no prompt follows, so a
    failure of the write made on close must be reported there."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.refuse_writes()
    _rotate(env, degrees=2)
    assert env.win.close() is True
    assert not env.win.isVisible()
    assert [n[1] for n in env.notices] == [_tr("Auto-save failed")]
    assert env.win._last_save_failed is True
    env.win.show()
    _rotate(env, degrees=2)
    ev = QCloseEvent()
    ev.accept()
    escaped = pwh.call_catching(env.win.closeEvent, ev)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert ev.isAccepted()
    assert [n[1] for n in env.notices] == [_tr("Auto-save failed")]


def test_the_puzzle_close_event_never_ignores():
    """Qt also closes this window while the application quits; an ignored
    close there keeps the process running with no main window."""
    src = inspect.getsource(_dp().PuzzleCanvasWindow.closeEvent)
    body = src.split('"""')[-1]
    assert "ignore()" not in body
    assert "self._flush_auto_save(notify=True)" in body


# ------------------------------------------------------------------
# Delete acts only on the canvas, asks for more than one, and can be undone.

CTRL = Qt.KeyboardModifier.ControlModifier


def _focus(env, widget):
    env.win.activateWindow()
    widget.setFocus()
    pwh.pump()
    assert QApplication.focusWidget() is widget


def _undo(env):
    undo = getattr(env.win, "_undo_last_delete", None)
    assert undo is not None, "no undo for Delete"
    undo()


def test_delete_in_the_saved_joins_list_does_not_touch_the_canvas(env):
    """The list ignores Delete, so it reached the window's keyPressEvent and
    removed the selected canvas fragment; autosave then wrote that."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.win._refresh_docs_list()
    env.item("990001").setSelected(True)
    _focus(env, env.win._docs_list)
    QTest.keyClick(env.win._docs_list, Qt.Key.Key_Delete)
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]
    env.settle()                     # any autosave the key started has run
    assert len(env.stored(doc)) == 2


def test_select_all_delete_asks_and_never_autosaves_an_empty_join(env):
    """Ctrl+A, Delete removed everything without a question and the autosave
    saved the join with zero fragments."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.answer = SB.Yes
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_A, CTRL)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    assert env.asks == [(_tr("Delete fragments?"),
                         _tr("Remove {} fragments from the puzzle?").format(2), SB.Yes | SB.No)]
    assert env.win._fragment_items == {}
    assert env.win.statusBar().currentMessage() == _tr(
        "{} fragments deleted. Press Ctrl+Z to undo.").format(2)
    env.settle()                     # debounce + timer: the autosave runs for real
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]
    assert env.win.statusBar().currentMessage() == _tr(EMPTY_STORE_NOTE)


def test_declining_a_multi_delete_keeps_everything(env):
    env.scratch_pad()
    env.select_only(*env.win._fragment_items.values())
    env.answer = SB.No
    env.win._delete_selected()       # the toolbar button and the context menu
    assert len(env.asks) == 1
    assert len(env.win._fragment_items) == 2


def test_ctrl_z_restores_the_last_delete_but_never_into_the_next_puzzle(env):
    """No undo existed: a deleted fragment's alignment was gone for good."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    it = env.item("990001")
    it.setRotation(3.0)
    pos = (it.pos().x(), it.pos().y())
    env.select_only(it)
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    assert env.asks == []            # one fragment: no question, undo instead
    assert env.win.statusBar().currentMessage() == _tr("Fragment deleted. Press Ctrl+Z to undo.")
    assert ("990001", "1r") not in env.win._fragment_items
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Z, CTRL)
    back = env.win._fragment_items.get(("990001", "1r"))
    assert back is it
    assert (back.pos().x(), back.pos().y(), back.rotation()) == (*pos, 3.0)
    env.settle()
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]
    # ...and an undo never crosses into another puzzle.
    env.select_only(back)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    env.win._on_new_puzzle()
    env.add(pwh.fragments("99030")[0])
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Z, CTRL)
    assert sorted(env.win._fragment_items) == [("990301", "1r")]


def test_delete_with_the_fragment_combo_focused_deletes(env):
    """Regression pin: choosing a fragment in the fragment combo and pressing
    Delete must keep working (a canvas-only rule would break it)."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    combo = env.win.combo_fragments
    _focus(env, combo)
    combo.setCurrentIndex(-1)
    combo.setCurrentIndex(0)         # selects that fragment on the canvas
    chosen = combo.itemData(0)
    assert [i is env.win._fragment_items[chosen]
            for i in env.win.canvas_view.get_selected_fragments()] == [True]
    QTest.keyClick(combo, Qt.Key.Key_Delete)
    assert chosen not in env.win._fragment_items
    assert len(env.win._fragment_items) == 1


def test_delete_after_a_toolbar_click_deletes(env):
    """Regression pin: after a toolbar button took the focus, Delete still
    deletes the selected fragment."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.select_only(env.item("990002"))
    _focus(env, env.win.btn_bg_toggle)
    QTest.keyClick(env.win.btn_bg_toggle, Qt.Key.Key_Delete)
    assert sorted(env.win._fragment_items) == [("990001", "1r")]


def test_ctrl_z_with_the_fragment_combo_focused_undoes(env):
    """A non-editable combo swallows Ctrl+Z before the window's keyPressEvent
    sees it, so an undo there must come from a window shortcut."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    combo = env.win.combo_fragments
    _focus(env, combo)
    combo.setCurrentIndex(-1)
    combo.setCurrentIndex(0)
    chosen = combo.itemData(0)
    QTest.keyClick(combo, Qt.Key.Key_Delete)
    assert chosen not in env.win._fragment_items
    QTest.keyClick(combo, Qt.Key.Key_Z, CTRL)
    assert chosen in env.win._fragment_items
    assert len(env.win._fragment_items) == 2


def _ctrl_z_outside_the_canvas(env, where):
    """Ctrl+Z in the notes field is that field's own undo, and in the Saved
    Joins list it means nothing: neither may restore (or use up) the undo of
    a canvas Delete."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.win._refresh_docs_list()
    env.select_only(env.item("990001"))
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    assert ("990001", "1r") not in env.win._fragment_items
    widget = env.win._notes_edit if where == "notes" else env.win._docs_list
    _focus(env, widget)
    QTest.keyClick(widget, Qt.Key.Key_Z, CTRL)
    assert ("990001", "1r") not in env.win._fragment_items
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Z, CTRL)
    assert ("990001", "1r") in env.win._fragment_items


def test_ctrl_z_in_the_notes_field_does_not_touch_the_canvas(env):
    _ctrl_z_outside_the_canvas(env, "notes")


def test_ctrl_z_in_saved_joins_does_nothing(env):
    _ctrl_z_outside_the_canvas(env, "saved_joins")


def test_delete_readd_undo_keeps_one_copy(env):
    """Undo after the same page was added again must not put a second copy of
    it on the canvas, and still restores the rest of that Delete."""
    env.scratch_pad()
    env.select_only(*env.win._fragment_items.values())
    env.answer = SB.Yes
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    assert env.win._fragment_items == {}
    env.add(pwh.fragments()[0])      # the same page added again from the main window
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Z, CTRL)
    on_scene = sorted(i.puzzle_frag.sys_id for i in env.win.canvas_view.get_fragment_items())
    assert on_scene == ["990001", "990002"]
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]


def test_undo_of_a_crop_mode_delete_restores_a_normal_item(env):
    """A fragment deleted in crop mode came back still in crop mode, so every
    later edge drag cropped it."""
    doc = env.save(pwh.fragments())
    env.open(doc)
    env.select_only(env.item("990001"))
    env.win.btn_crop.setChecked(True)
    _focus(env, env.win.canvas_view)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Delete)
    env.win.btn_crop.setChecked(False)
    QTest.keyClick(env.win.canvas_view, Qt.Key.Key_Z, CTRL)
    back = env.win._fragment_items.get(("990001", "1r"))
    assert back is not None
    assert back._crop_mode is False


def _one_placed_fragment(env):
    from shared.puzzle_model import PuzzleFragment
    doc = env.save([PuzzleFragment(sys_id="990001", folio_label="1r", fl_id="FLA",
                                   shelfmark="T-S A 1", x=10.0, y=20.0)])
    env.win._load_document(doc)
    (loader,) = pwh.take_started()
    loader.deliver(pwh.png_bytes(30, 40))
    env.settle()
    env.win._folio_lists["990001"] = [{"fl_id": "FLA", "label": "1r"},
                                      {"fl_id": "FLB", "label": "1v"}]
    env.select_only(env.item("990001"))
    return doc


def _size(item):
    return item.pixmap().width(), item.pixmap().height()


@pytest.mark.parametrize("entry", ["navigate_next", "flip_recto_verso", "threshold"])
def test_undo_of_a_delete_made_while_a_reload_was_pending_loads_the_new_image(env, entry):
    """The Delete made the pending reload stale; an Undo that started nothing
    left the item showing the previous folio's image under the new label."""
    doc = _one_placed_fragment(env)
    if entry == "navigate_next":
        env.win._navigate_folio(+1)
        key = ("990001", "1v")
    elif entry == "flip_recto_verso":
        env.win._flip_recto_verso()
        key = ("990001", "1v")
    else:
        env.win.slider_threshold.blockSignals(True)
        env.win.slider_threshold.setValue(60)
        env.win.slider_threshold.blockSignals(False)
        env.win._on_threshold_changed()
        key = ("990001", "1r")
    (reload_b,) = pwh.take_started()
    item = env.win._fragment_items[key]
    env.win._delete_selected()
    assert env.asks == []
    _undo(env)
    assert env.win._fragment_items.get(key) is item
    restarted = pwh.take_started()
    assert len(restarted) == 1, "Undo must restart the reload the Delete made stale"
    reload_c = restarted[0]
    assert reload_c.kwargs == reload_b.kwargs
    assert env.win._pending_req[key][0] == reload_c.req
    reload_b.deliver(pwh.png_bytes(50, 20))
    assert _size(item) == (30, 40)   # stale
    reload_c.deliver(pwh.png_bytes(50, 20))
    assert _size(env.win._fragment_items[key]) == (50, 20)
    assert env.win._pending_req == {}
    env.settle()
    (stored,) = env.stored_doc(doc).fragments
    assert stored.folio_label == key[1]
    if entry == "threshold":
        assert stored.bg_removal_threshold == 60.0


def test_undo_with_no_reload_pending_starts_no_request(env):
    _one_placed_fragment(env)
    env.win._delete_selected()
    _undo(env)
    assert ("990001", "1r") in env.win._fragment_items
    assert pwh.take_started() == []
    assert env.win._pending_req == {}


# ------------------------------------------------------------------
# The four "Open in Puzzle" paths outside the window, and quitting.

def _external_fork(env, which):
    """Drive a REAL 'Open in Puzzle' path; its fork is already in joins.db."""
    fork_id = env.save(pwh.fragments("99020"), title="Fork of: X")
    client = types.SimpleNamespace(fork_puzzle_join=lambda join_id: fork_id,
                                   check_is_published=lambda doc_id: False)
    import genizah_app as ga
    host = env.host
    host._puzzle_window = env.win
    host._emit_feature_opened = lambda **k: None
    host._open_puzzle_window = types.MethodType(ga.GenizahGUI._open_puzzle_window, host)
    if which == "GenizahGUI._on_puzzle_clicked":
        host.corrections_client = client
        item = QListWidgetItem("x")
        item.setData(Qt.ItemDataRole.UserRole, {"id": "J1"})
        ga.GenizahGUI._on_puzzle_clicked(host, item)
    else:
        import desktop.corrections_ui as cu
        cls_name, meth = which.split(".")
        dialog = types.SimpleNamespace(client=client, parent=lambda: host,
                                       accept=lambda: None)
        getattr(getattr(cu, cls_name), meth)(dialog, "J1")
    pwh.pump(400)                   # two of the four defer the load by 200 ms
    pwh.finish_loads()
    return fork_id


EXTERNAL = ["GenizahGUI._on_puzzle_clicked",
            "DiscoveriesDialog._fork_and_open",
            "JoinsDialog._fork_and_open_puzzle",
            "JoinsFeedDialog._fork_and_open_puzzle"]


@pytest.mark.parametrize("which", EXTERNAL)
def test_open_in_puzzle_asks_before_replacing_a_scratch_pad(env, which):
    """The four fork-and-open paths called _load_document directly and
    replaced the canvas with no question at all."""
    env.scratch_pad()
    env.answer = SB.Cancel
    fork_id = _external_fork(env, which)
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before loading?"),
                         SB.Save | SB.Discard | SB.Cancel)]
    assert env.win._current_doc_id is None
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990002", "1r")]
    env.list_item(fork_id)          # the fork the user asked for is in Saved Joins


@pytest.mark.parametrize("which", EXTERNAL)
def test_open_in_puzzle_with_discard_opens_the_fork(env, which):
    env.scratch_pad()
    env.answer = SB.Discard
    fork_id = _external_fork(env, which)
    assert len(env.asks) == 1
    assert env.win._current_doc_id == fork_id


class _PastThePuzzleCheck(BaseException):
    """GenizahGUI.closeEvent went on past the puzzle question. A
    BaseException, so a try/except Exception around the next step of
    closeEvent cannot swallow it."""


_HOSTS: list = []                   # kept for the process: they hold Qt widgets


@pytest.fixture
def quit_host(env, monkeypatch):
    """The smallest host the REAL GenizahGUI.closeEvent can run on up to its
    first shutdown step, with the real _defer_close_for_puzzle bound."""
    import desktop.single_instance as si
    import genizah_app as ga
    from PyQt6.QtWidgets import QLabel, QPushButton
    monkeypatch.setattr(si, "_restart_requested", False)
    host = types.SimpleNamespace(_puzzle_window=env.win,
                                 _defer_close_for_passage=lambda e: False,
                                 status_label=QLabel(""), lang_btn=QPushButton(""),
                                 reached=[], raise_past_check=True)

    def _close_result_dialog():
        host.reached.append("result dialog")
        if host.raise_past_check:
            raise _PastThePuzzleCheck()

    host._close_result_dialog = _close_result_dialog
    for name in ("_defer_close_for_puzzle", "_defer_close_for_prompt",
                 "_close_waits_for_a_prompt"):
        method = getattr(ga.GenizahGUI, name, None)
        if method is not None:      # absent on a tree without that step
            setattr(host, name, types.MethodType(method, host))
    _HOSTS.append(host)
    return host


def _close_event():
    ev = QCloseEvent()
    ev.accept()
    return ev


def _run_close(host, ev):
    """GenizahGUI.closeEvent on the host; returns the Exception that escaped
    it, or None. The sentinel (past the puzzle check) is not caught."""
    import genizah_app as ga
    return pwh.call_catching(ga.GenizahGUI.closeEvent, host, ev)


HEBREW = "עברית"


def test_quit_cancel_keeps_the_app_open_and_forgets_the_restart(env, quit_host):
    """Quitting dropped an unsaved scratch pad with no question (closing the
    puzzle only hides it). Cancel must stop the close before any shutdown
    step, and must forget a pending language restart."""
    quit_host.raise_past_check = False   # a wrong close goes on and fails loudly
    import desktop.single_instance as si
    env.scratch_pad()
    si.request_restart()
    closing = "Closing once the current work finishes"
    quit_host.status_label.setText(closing)
    quit_host._closing_status_text = closing
    env.answer = SB.Cancel
    ev = _close_event()
    escaped = _run_close(quit_host, ev)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert quit_host.reached == []
    assert not ev.isAccepted()
    assert env.asks == [(_tr("Save current work?"), _tr("Save current puzzle before quitting?"),
                         SB.Save | SB.Discard | SB.Cancel)]
    launched = []
    assert si.relaunch_if_requested(popen=lambda *a, **k: launched.append(a)) is False
    assert launched == []
    assert quit_host.status_label.text() == _tr("Ready.")
    assert quit_host.lang_btn.text() == HEBREW          # the saved language is English


def test_quit_discard_carries_on_with_the_close(env, quit_host):
    env.scratch_pad()
    env.answer = SB.Discard
    with pytest.raises(_PastThePuzzleCheck):
        _run_close(quit_host, _close_event())
    assert len(env.asks) == 1


def test_a_failing_quit_check_does_not_keep_the_app_open(env, quit_host, monkeypatch):
    calls = []

    def _broken():
        calls.append(1)
        raise RuntimeError("broken check")

    monkeypatch.setattr(env.win, "confirm_quit", _broken, raising=False)
    with pytest.raises(_PastThePuzzleCheck):
        _run_close(quit_host, _close_event())
    assert calls == [1]


def test_quit_shows_a_hidden_puzzle_before_asking(env, quit_host):
    env.scratch_pad()
    env.win.close()                  # X only hides it; the work is still there
    assert not env.win.isVisible()
    seen = []

    def _answer(title, text, buttons):
        seen.append(env.win.isVisible())
        return SB.Discard

    env.answer = _answer
    with pytest.raises(_PastThePuzzleCheck):
        _run_close(quit_host, _close_event())
    assert seen == [True]


def test_quit_cancel_with_the_real_saved_language(env, quit_host):
    """load_language is not a module global of genizah_app (toggle_language
    imports it locally), so the Cancel path must import it itself."""
    quit_host.raise_past_check = False   # a wrong close goes on and fails loudly
    import desktop.single_instance as si
    import genizah_app as ga
    from genizah_core import save_language
    assert not hasattr(ga, "load_language")
    save_language("he")              # into the conftest-isolated language file
    si.request_restart()
    env.scratch_pad()
    env.answer = SB.Cancel
    ev = _close_event()
    escaped = _run_close(quit_host, ev)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert not ev.isAccepted()
    assert quit_host.lang_btn.text() == "English"


def test_quit_cancel_disarms_a_pending_passage_retry(env, quit_host):
    """A close deferred for a letter-level search, then a second close after
    the search ended but before the 400 ms retry: Cancel at the puzzle
    question must also stop the retry from closing (and asking) again."""
    quit_host.raise_past_check = False   # a wrong close goes on and fails loudly
    import genizah_app as ga
    closes = []
    quit_host._close_pending = True
    quit_host.close = lambda: closes.append(1)
    quit_host._passage_workers_busy = lambda: False
    env.scratch_pad()
    env.answer = SB.Cancel
    escaped = _run_close(quit_host, _close_event())
    assert escaped is None, f"closeEvent raised {escaped!r}"
    ga.GenizahGUI._retry_pending_close(quit_host)
    assert quit_host._close_pending is False
    assert closes == []


def test_quit_cancel_after_a_deferred_close_clears_the_closing_line(env, quit_host):
    """A close deferred for a letter-level search says 'Closing once the
    current work finishes'; work is then put on the puzzle, the close that
    follows is cancelled at the puzzle question, and the app stays open --
    that line must not stay."""
    quit_host.raise_past_check = False   # a wrong close goes on and fails loudly
    import genizah_app as ga
    busy = ["a letter-level search"]
    quit_host._passage_workers_busy = lambda: list(busy)
    quit_host._passage_batch_in_flight = lambda: False
    quit_host._retry_pending_close = lambda: None
    quit_host._defer_close_for_passage = types.MethodType(
        ga.GenizahGUI._defer_close_for_passage, quit_host)
    quit_host.status_label.setText(_tr("Ready."))
    ev = _close_event()
    assert _run_close(quit_host, ev) is None
    assert not ev.isAccepted() and env.asks == []       # nothing unsaved: deferred, not asked
    closing = quit_host.status_label.text()
    assert closing != _tr("Ready.") and busy[0] in closing
    env.scratch_pad()                                   # work put on the puzzle meanwhile
    busy.clear()                                        # the search ended
    env.answer = SB.Cancel
    ev = _close_event()
    escaped = _run_close(quit_host, ev)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert not ev.isAccepted()
    assert len(env.asks) == 1
    assert quit_host.status_label.text() == _tr("Ready.")


class _Host:
    """Holds the attributes the close methods read. Not a SimpleNamespace:
    QTimer.singleShot takes a weak reference to a bound method's object."""

    def __init__(self, **attrs):
        self.__dict__.update(attrs)


@pytest.fixture
def deferring_host(env, monkeypatch):
    """A host whose closes run the REAL closeEvent up to its first shutdown
    step, with the real puzzle question, passage deferral and 400 ms retry.
    `busy` stands in for passage work, `batch` makes it a multi-witness
    batch whose request_cancel is recorded in `cancels`. close() is what the
    retry calls: closeEvent with a fresh event; a close that reaches the
    shutdown steps is recorded in `shutdowns`."""
    import desktop.single_instance as si
    import genizah_app as ga
    from PyQt6.QtWidgets import QLabel, QPushButton
    monkeypatch.setattr(si, "_restart_requested", False)
    host = _Host(_puzzle_window=env.win, status_label=QLabel(""), lang_btn=QPushButton(""),
                 busy=[], batch=False, cancels=[], shutdowns=[])
    host._passage_workers_busy = lambda: list(host.busy)
    host._passage_batch_in_flight = lambda: host.batch and bool(host.busy)
    host.comp_thread = types.SimpleNamespace(request_cancel=lambda: host.cancels.append(1))
    for name in ("_defer_close_for_passage", "_retry_pending_close",
                 "_defer_close_for_puzzle", "_close_waits_for_a_prompt",
                 "_defer_close_for_prompt"):
        method = getattr(ga.GenizahGUI, name, None)
        if method is not None:          # absent on a tree without the prompt guard
            setattr(host, name, types.MethodType(method, host))

    def _close_result_dialog():
        raise _PastThePuzzleCheck()

    def _close(source="retry"):
        ev = _close_event()
        try:
            ga.GenizahGUI.closeEvent(host, ev)
        except _PastThePuzzleCheck:
            host.shutdowns.append(source)
        return ev

    host._close_result_dialog = _close_result_dialog
    host.close = _close
    _HOSTS.append(host)
    yield host
    host.busy.clear()
    host._close_pending = False         # a retry still queued stops at its next run
    host._close_waiting_for_prompt = False


def _quit_asks(env):
    return [text for _title, text, _b in env.asks
            if text == _tr("Save current puzzle before quitting?")]


def test_a_retry_during_the_quit_question_does_not_ask_it_again(env, deferring_host):
    """A close deferred for a letter-level search re-issues itself every
    400 ms. When a later close's quit question was still open at that
    moment, the retry ran inside the question's event loop and asked the
    same question again on top of it."""
    h = deferring_host
    h.busy[:] = ["a letter-level search"]
    ev = h.close("user")
    assert not ev.isAccepted() and env.asks == [] and h._close_pending   # nothing unsaved yet
    env.scratch_pad()                   # work put on the puzzle while the close waits
    h.busy.clear()                      # the search ended

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            pwh.pump(600)               # the user takes longer than the retry interval
        return SB.Cancel

    env.answer = _answer
    ev = h.close("user")
    assert len(env.asks) == 1, f"asked {len(env.asks)} times"
    assert not ev.isAccepted() and h.shutdowns == []
    pwh.pump(600)                       # Cancel ended the close attempt
    assert len(env.asks) == 1 and h.shutdowns == []
    assert not h._close_pending


@pytest.mark.parametrize("prompt", ["new", "save_dialog"])
def test_a_retry_waits_while_a_puzzle_prompt_is_open(env, deferring_host, monkeypatch, prompt):
    """The retry of a deferred close came round while the puzzle's New
    prompt or its Save dialog was open and asked the quit question on top of
    it. It waits for the answer, then asks."""
    h = deferring_host
    h.busy[:] = ["a letter-level search"]
    h.close("user")                     # deferred; nothing unsaved yet, nothing asked
    env.scratch_pad()
    during = []

    def _meanwhile():
        h.busy.clear()                  # the search ends while the prompt is open
        pwh.pump(600)
        during.append((len(env.asks), list(h.shutdowns)))

    if prompt == "new":
        def _answer(title, text, buttons):
            if len(env.asks) == 1:
                _meanwhile()
            return SB.Cancel

        env.answer = _answer
        env.win._on_new_puzzle()
        assert during == [(1, [])], "a quit question was asked over the New prompt"
    else:
        def _exec(dlg):
            _meanwhile()
            return QDialog.DialogCode.Rejected

        monkeypatch.setattr(QDialog, "exec", _exec)
        env.win._on_save_join()
        assert during == [(0, [])], "a quit question was asked over the Save dialog"
    env.answer = SB.Discard
    pwh.pump(600)                       # answered: the close goes on and asks now
    assert len(_quit_asks(env)) == 1
    assert h.shutdowns == ["retry"]


def test_a_retry_after_discard_does_not_close_the_app_under_a_puzzle_prompt(
        env, deferring_host):
    """Discard is kept for the close attempt, so the retry asks nothing; it
    must still wait while a puzzle prompt is open instead of shutting the app
    down underneath it."""
    h = deferring_host
    env.scratch_pad()
    h.busy[:] = ["a letter-level search"]
    env.answer = SB.Discard
    ev = h.close("user")
    assert not ev.isAccepted() and len(env.asks) == 1 and h._close_pending
    during = []

    def _answer(title, text, buttons):
        h.busy.clear()
        pwh.pump(600)
        during.append(list(h.shutdowns))
        return SB.Cancel

    env.answer = _answer
    env.win._on_new_puzzle()            # the scratch pad is still there: New asks
    assert during == [[]], "the app shut down under the puzzle's New prompt"
    pwh.pump(600)
    assert h.shutdowns == ["retry"]
    assert len(_quit_asks(env)) == 1


def test_a_retry_waits_while_any_modal_dialog_is_open(env, deferring_host):
    h = deferring_host
    h.busy[:] = ["a letter-level search"]
    h.close("user")
    h.busy.clear()
    box = QDialog(env.host)
    _HOSTS.append(box)
    box.setWindowModality(Qt.WindowModality.ApplicationModal)
    box.show()
    pwh.pump()
    assert QApplication.activeModalWidget() is box
    pwh.pump(600)
    assert h.shutdowns == [] and h._close_pending
    box.hide()
    pwh.pump(600)
    assert h.shutdowns == ["retry"]


def test_cancel_at_the_quit_question_does_not_stop_a_multi_witness_batch(env, deferring_host):
    """The close stopped a running (or paused) multi-witness batch before
    the puzzle question came; Cancel then kept the app open with its batch
    already stopped."""
    import desktop.single_instance as si
    h = deferring_host
    si.request_restart()
    env.scratch_pad()
    h.busy[:] = ["a letter-level search is running"]
    h.batch = True
    h.status_label.setText(_tr("Ready."))
    env.answer = SB.Cancel
    ev = h.close("user")
    assert not ev.isAccepted() and len(env.asks) == 1
    assert h.cancels == [], "the batch was stopped although the app stays open"
    assert not getattr(h, "_close_pending", False)
    assert h.status_label.text() == _tr("Ready.")
    assert si._restart_requested is False
    pwh.pump(600)
    assert h.shutdowns == [] and h.cancels == [] and len(env.asks) == 1


def test_discard_at_the_quit_question_closes_once_the_witness_finishes(env, deferring_host):
    import desktop.single_instance as si
    h = deferring_host
    si.request_restart()
    env.scratch_pad()
    h.busy[:] = ["a letter-level search is running"]
    h.batch = True
    env.answer = SB.Discard
    ev = h.close("user")
    assert not ev.isAccepted() and len(env.asks) == 1
    assert h.cancels == [1]             # asked to stop after the witness in flight
    pwh.pump(500)                       # that witness is still running
    assert h.shutdowns == [] and len(env.asks) == 1
    h.busy.clear()                      # it finished
    pwh.pump(600)
    assert h.shutdowns == ["retry"]
    assert len(env.asks) == 1, "the retry asked the quit question again"
    assert si._restart_requested is True


@pytest.mark.parametrize("answer", [SB.Cancel, SB.Discard], ids=["cancel", "discard"])
def test_the_language_restart_survives_only_when_the_app_closes(env, deferring_host, answer):
    """A retry that asked the quit question again on top of the open one
    shut the app down from the inner question while the outer one could
    still answer Cancel and forget the restart: the app exited and did not
    come back in the new language (or it shut down twice)."""
    import desktop.single_instance as si
    h = deferring_host
    si.request_restart()                # toggle_language: restart now
    h.busy[:] = ["a letter-level search"]
    h.close("user")                     # deferred; nothing unsaved yet
    env.scratch_pad()
    h.busy.clear()

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            pwh.pump(600)               # the retry comes round while this question is open
            return answer
        return SB.Discard               # a question asked on top of it

    env.answer = _answer
    h.close("user")
    pwh.pump(600)
    closed = answer == SB.Discard
    assert h.shutdowns == (["user"] if closed else [])
    assert si._restart_requested is closed


# -- the FIRST close, too, waits for an open question --

@pytest.mark.parametrize("prompt", ["new", "open_join", "save_dialog"])
def test_a_first_close_during_a_puzzle_prompt_waits_for_it(env, deferring_host, monkeypatch,
                                                           prompt):
    """Only the retry of a deferred close waited for an open question. A
    close arriving while the puzzle's New or open prompt, or its Save
    dialog, was open asked the quit question on top of it at once."""
    h = deferring_host
    doc_b = _join_b(env)
    env.win._refresh_docs_list()
    env.scratch_pad()
    during = []

    def _close_meanwhile():
        ev = h.close("user")
        pwh.pump(600)                   # longer than the retry interval
        during.append((len(env.asks), ev.isAccepted(), list(h.shutdowns)))

    if prompt == "save_dialog":
        def _exec(dlg):
            _close_meanwhile()
            return QDialog.DialogCode.Rejected

        monkeypatch.setattr(QDialog, "exec", _exec)
        env.win._on_save_join()
        assert during == [(0, False, [])], "a quit question was asked over the Save dialog"
    else:
        def _answer(title, text, buttons):
            if len(env.asks) == 1:
                _close_meanwhile()
            return SB.Cancel            # the scratch pad stays

        env.answer = _answer
        if prompt == "new":
            env.win._on_new_puzzle()
        else:
            env.click_join(doc_b)
        assert during == [(1, False, [])], "a quit question was asked over the puzzle prompt"
    env.answer = SB.Discard
    pwh.pump(600)                       # answered: the close comes round and asks once
    assert len(_quit_asks(env)) == 1
    assert h.shutdowns == ["retry"]


def test_a_first_close_during_a_puzzle_prompt_closes_once_nothing_is_unsaved(env, deferring_host):
    h = deferring_host
    h.close("user")                     # no question open, nothing unsaved: closes at once
    assert h.shutdowns == ["user"] and env.asks == [] and not h._close_pending
    h.shutdowns.clear()
    env.scratch_pad()
    during = []

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            ev = h.close("user")
            pwh.pump(600)
            during.append((len(env.asks), ev.isAccepted(), list(h.shutdowns)))
        return SB.Discard               # New clears the scratch pad

    env.answer = _answer
    env.win._on_new_puzzle()
    assert during == [(1, False, [])], "the close went on under the New prompt"
    pwh.pump(600)
    assert len(env.asks) == 1           # nothing left unsaved: no quit question
    assert h.shutdowns == ["retry"]


@pytest.mark.parametrize("answer", [SB.Cancel, SB.Discard], ids=["cancel", "discard"])
def test_a_second_close_during_the_quit_question_does_not_ask_it_again(env, deferring_host,
                                                                       answer):
    """A second close while the quit question was open asked it again on top
    of it; the inner answer then closed the app (or forgot the language
    restart) whatever the outer one said."""
    import desktop.single_instance as si
    h = deferring_host
    si.request_restart()
    env.scratch_pad()

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            h.close("second")
            pwh.pump(600)
            return answer
        return SB.Discard               # a question asked on top of it

    env.answer = _answer
    h.close("user")
    pwh.pump(600)
    assert len(env.asks) == 1, f"asked {len(env.asks)} times"
    closed = answer == SB.Discard
    assert h.shutdowns == (["user"] if closed else [])
    assert si._restart_requested is closed


def test_a_first_close_while_a_modal_dialog_is_open_waits_for_it(env, deferring_host):
    h = deferring_host
    env.scratch_pad()
    box = QDialog(env.host)
    _HOSTS.append(box)
    box.setWindowModality(Qt.WindowModality.ApplicationModal)
    box.show()
    pwh.pump()
    assert QApplication.activeModalWidget() is box
    env.answer = SB.Discard
    ev = h.close("user")
    assert not ev.isAccepted() and env.asks == [] and h._close_waiting_for_prompt
    assert not getattr(h, "_close_pending", False)      # nothing answered yet
    pwh.pump(600)
    assert env.asks == [] and h.shutdowns == []
    box.hide()
    pwh.pump(600)
    assert len(_quit_asks(env)) == 1
    assert h.shutdowns == ["retry"]


def test_a_close_that_waited_for_a_prompt_then_stops_the_batch_and_asks(env, deferring_host):
    """A close that waited only for an open question has not reached the quit
    question or the passage deferral. Once the question is answered it must
    come round and do both, not poll until the batch ends -- a paused batch
    never does."""
    h = deferring_host
    env.scratch_pad()
    h.busy[:] = ["a letter-level search is running"]
    h.batch = True
    during = []

    def _answer(title, text, buttons):
        if len(env.asks) == 1:
            h.close("user")
            pwh.pump(600)
            during.append((len(env.asks), list(h.cancels), list(h.shutdowns)))
        return SB.Cancel if len(env.asks) == 1 else SB.Discard

    env.answer = _answer
    env.win._on_new_puzzle()            # Cancel: the scratch pad stays
    assert during == [(1, [], [])], "a question was asked, or the batch stopped, under the prompt"
    pwh.pump(600)
    assert len(_quit_asks(env)) == 1
    assert h.cancels == [1]             # asked to stop after the witness in flight
    assert h.shutdowns == []
    h.busy.clear()
    pwh.pump(600)
    assert h.shutdowns == ["retry"]
    assert len(_quit_asks(env)) == 1


class _PastTheCloseCheck(BaseException):
    """Raised by a stand-in just past on_comp_scan_finished's close check:
    the scan's results are being shown."""


@pytest.mark.parametrize("answer", [SB.Cancel, SB.Discard], ids=["cancel", "discard"])
def test_work_that_finishes_while_a_first_close_waits_for_a_modal_is_kept(
        env, deferring_host, monkeypatch, answer):
    """A first close deferred only because a modal was open marked the close
    pending before the quit question was asked. A composition scan, a
    letter-level build or the index load finishing meanwhile took that for an
    app on its way out and dropped its result -- lost for good when the user
    then chose Cancel at the quit question."""
    import genizah_app as ga
    h = deferring_host
    env.scratch_pad()
    kept = []
    # What the three callbacks touch up to, and just past, their close check.
    h.is_comp_running = True
    h.reset_comp_ui = lambda: None
    h._stop_auto_expand = lambda _reason: kept.append("scan dropped")

    def _show_scan():
        kept.append("scan shown")
        raise _PastTheCloseCheck()

    h._refresh_comp_method_enabled = _show_scan
    # The first thing past the close check now: the chunk notice (#13).
    h._show_comp_chunk_notice = lambda _result: _show_scan()
    monkeypatch.setattr(ga.passage_lifecycle, "install_passage_state",
                        lambda state: kept.append(state.live_dir))
    for name in ("_finish_passage_build", "_honour_deferred_comp_method",
                 "_apply_default_comp_method", "_revalidate_comp_method",
                 "_maybe_offer_passage_build"):
        setattr(h, name, lambda: None)
    box = QDialog(env.host)
    _HOSTS.append(box)
    box.setWindowModality(Qt.WindowModality.ApplicationModal)
    box.show()
    pwh.pump()
    ev = h.close("user")
    assert not ev.isAccepted() and env.asks == []
    try:
        ga.GenizahGUI.on_comp_scan_finished(h, {"main": []})
    except _PastTheCloseCheck:
        pass
    ga.GenizahGUI._on_passage_build_finished(
        h, types.SimpleNamespace(index=object(), live_dir="built", status="installed"))
    ga.GenizahGUI._on_passage_loaded(
        h, types.SimpleNamespace(index=object(), live_dir="loaded", status="live_ok"))
    assert kept == ["scan shown", "built", "loaded"], "work was dropped under the modal"
    box.hide()
    env.answer = answer
    pwh.pump(600)                       # the close comes round and asks
    assert len(_quit_asks(env)) == 1
    assert h.shutdowns == ([] if answer == SB.Cancel else ["retry"])
    assert kept == ["scan shown", "built", "loaded"]


def test_quit_check_runs_before_any_shutdown_state():
    """The puzzle question comes first: the passage deferral stops a
    multi-witness batch, which a close the user then cancels must not do."""
    import genizah_app as ga
    src = inspect.getsource(ga.GenizahGUI.closeEvent)
    assert "self._defer_close_for_puzzle(event)" in src
    at = src.index("self._defer_close_for_puzzle(event)")
    assert at < src.index("self._defer_close_for_passage(event)")
    assert at < src.index("self._close_result_dialog()")
    assert at < src.index("self._app_shutting_down = True")


# -- a manuscript looked up for one canvas lands only on that canvas --

class _MetaMgr:
    def get_meta_for_id(self, sys_id):
        return ("T-S X 7", "")

    def get_library_for_id(self, sys_id):
        return ""


def _request_uncached_manuscript(env, entry, monkeypatch):
    """Ask for manuscript 990077, whose folio list is not cached, through the
    puzzle's shelfmark field or GenizahGUI.add_to_puzzle."""
    meta_mgr = _MetaMgr()
    env.host.meta_mgr = meta_mgr
    if entry == "add_shelfmark":
        env.host._ensure_shelf_map = lambda: None
        env.host._shelf_to_sys = {_dp().normalize_shelfmark("T-S X 7"): "990077"}
        env.win.shelfmark_input.setText("T-S X 7")
        env.win._on_add_shelfmark()
    else:
        import genizah_app as ga
        monkeypatch.setattr(ga, "PuzzleMetaLoaderThread", pwh.FakeMetaLoader, raising=False)
        host = types.SimpleNamespace(_puzzle_window=env.win, meta_mgr=meta_mgr,
                                     _emit_feature_opened=lambda **k: None)
        _HOSTS.append(host)
        ga.GenizahGUI.add_to_puzzle(host, "990077", "T-S X 7")
    assert len(pwh.FakeMetaLoader.made) == 1
    return pwh.FakeMetaLoader.made[0]


def _resolve(lookup):
    lookup.meta_ready.emit("990077", "T-S X 7", [{"label": "1r", "fl_id": "FL77"}])


def _join_b(env):
    from shared.puzzle_model import PuzzleFragment
    return env.save([PuzzleFragment(sys_id="990011", folio_label="1r", fl_id="FL11",
                                    shelfmark="T-S B 11", x=10.0, y=20.0)], title="B")


@pytest.mark.parametrize("replacement", ["open_join_b", "new"])
@pytest.mark.parametrize("entry", ["add_shelfmark", "add_to_puzzle"])
def test_a_fragment_resolved_after_the_canvas_was_replaced_is_not_added(
        env, monkeypatch, entry, replacement):
    """A late folio lookup added its manuscript to whatever canvas was open
    when it arrived -- a join opened meanwhile, which then autosaved it."""
    doc_b = _join_b(env)
    lookup = _request_uncached_manuscript(env, entry, monkeypatch)
    if replacement == "open_join_b":
        env.open(doc_b)
    else:
        env.win._on_new_puzzle()
    before = sorted(env.win._fragment_items)
    _resolve(lookup)
    assert [t for t in pwh.take_started() if t.fl_id == "FL77"] == []
    assert sorted(env.win._fragment_items) == before
    assert "990077" not in env.win._folio_lists
    assert "T-S X 7" not in env.win.statusBar().currentMessage()
    lookup.meta_failed.emit("990077", "lookup failed")
    assert "lookup failed" not in env.win.statusBar().currentMessage()
    env.settle()
    assert [s for s, _r, _x in env.stored(doc_b)] == ["990011"]


@pytest.mark.parametrize("entry", ["add_shelfmark", "add_to_puzzle"])
def test_a_fragment_resolved_on_the_same_canvas_is_added(env, monkeypatch, entry):
    """Control: with no replacement the looked-up manuscript is added."""
    lookup = _request_uncached_manuscript(env, entry, monkeypatch)
    _resolve(lookup)
    pwh.finish_loads()
    assert ("990077", "1r") in env.win._fragment_items


@pytest.mark.parametrize("entry", ["add_shelfmark", "add_to_puzzle"])
def test_a_fragment_resolved_after_a_cancelled_replacement_is_added(env, monkeypatch, entry):
    """Control: New answered Cancel keeps the canvas, and the lookup made on
    it still lands there."""
    env.add(pwh.fragments()[0])
    lookup = _request_uncached_manuscript(env, entry, monkeypatch)
    env.answer = SB.Cancel
    env.win._on_new_puzzle()
    _resolve(lookup)
    pwh.finish_loads()
    assert sorted(env.win._fragment_items) == [("990001", "1r"), ("990077", "1r")]


def test_every_metadata_request_goes_through_start_meta_resolve():
    """A lookup that adds a fragment was started, and its result connected,
    in two places (one in genizah_app.py); a canvas check added to one
    would miss the other."""
    tree = ast.parse((ROOT / "desktop" / "puzzle.py").read_text(encoding="utf-8"))
    fns = {f.name: f for f in _functions(tree)}
    assert "_start_meta_resolve" in fns, "no _start_meta_resolve seam"
    seam, rebuild = fns["_start_meta_resolve"], fns["_spawn_meta_loader"]
    slots = [n for n in ast.walk(tree) if isinstance(n, ast.Attribute)
             and n.attr in ("_on_meta_resolved", "_on_meta_failed")]
    assert slots and all(_inside(n, seam) for n in slots)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "PuzzleMetaLoaderThread"]
    assert len(calls) == 2
    assert sorted(int(_inside(c, seam)) + 2 * int(_inside(c, rebuild)) for c in calls) == [1, 2]
    app_tree = ast.parse((ROOT / "genizah_app.py").read_text(encoding="utf-8"))
    assert not [n for n in ast.walk(app_tree) if isinstance(n, ast.Attribute)
                and n.attr in ("_on_meta_resolved", "_on_meta_failed")]
    assert not [n for n in ast.walk(app_tree) if isinstance(n, ast.Name)
                and n.id == "PuzzleMetaLoaderThread"]
    assert not [a for n in ast.walk(app_tree) if isinstance(n, ast.ImportFrom)
                for a in n.names if a.name == "PuzzleMetaLoaderThread"], (
        "genizah_app.py still imports PuzzleMetaLoaderThread")
