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
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402

pytestmark = pytest.mark.gui

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtGui import QCloseEvent  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QDialog, QGraphicsTextItem, QMessageBox  # noqa: E402

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


# ------------------------------------------------------------------ #10
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
    assert env.win._on_save_join() is True
    assert sorted(s for s, _r, _x in env.stored(doc)) == ["990001", "990002"]


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
    assert env.win._on_save_join() is True
    doc_s = env.win._current_doc_id
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
    assert env.win.statusBar().currentMessage() == _tr(
        "Folio {} of this fragment is already on the canvas.").format("1v")
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

