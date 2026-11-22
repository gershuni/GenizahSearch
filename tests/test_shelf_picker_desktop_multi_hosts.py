# -*- coding: utf-8 -*-
"""Several at once from the shelf on the desktop (the desktop half of the web's #71): the Reading
Desk (the desk toolbar's shelf button; Browse reaches the same choice through its picker's switch,
tests/test_shelf_picker_desktop_browse_switch.py), the Fragment
Puzzle's shelf button and the Lists tab's "Add from the shelf" (D8), each through the one picker's
multiple-choice mode (desktop/widgets/shelfmark_tree_picker.py::pick_several_from_shelf).

The call sites; the cap with the rest kept; the desk's batch (one render, one source worker, no
earlier worker's results lost, the desk's exit retiring them); a Puzzle fragment taken only once its
image is on the canvas it was sent to (a failed or broken image, or a canvas replaced meanwhile,
stays chosen; those already on the canvas are taken; the fragments are placed side by side); the
list's one bulk add (a manuscript the list holds, even as one of its pages, is not added again; a
save that did not reach lists.pkl takes nothing, and a retry saves); and the buttons absent without
the tree. The language is pinned and the four profile variables point at scratch folders.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QApplication, QLabel, QWidget

import genizah_core
import desktop.widgets.shelfmark_tree_picker as shelf_picker
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


def _bind(obj, cls, names):
    for name in names:
        setattr(obj, name, getattr(cls, name).__get__(obj))


def _entries(svc, sids):
    by = {lf.sys_id: lf for node in ("vol:100", "vol:110", "col:10") for lf in svc.leaves_under(node)}
    out = []
    for sid in sids:
        leaf = by.get(sid)
        out.append({"sys_id": sid, "shelfmark": leaf.shelfmark if leaf else sid,
                    "placement_id": leaf.id if leaf else ""})
    return out


def _many(svc, n):
    seen, out = set(), []
    for library in svc.children(None):
        for node in svc.children(library.id):
            for leaf in svc.leaves_under(node.id):
                if leaf.sys_id not in seen:
                    seen.add(leaf.sys_id)
                    out.append(leaf.sys_id)
    assert len(out) >= n
    return out[:n]


def _store_with(svc, purpose, sids):
    store = shelf_picker.SelectionStore()
    selection = Selection()
    leaves = {lf.sys_id: lf for library in svc.children(None) for node in svc.children(library.id)
              for lf in svc.leaves_under(node.id)}
    for sid in sids:
        selection.add_leaf(leaves[sid], members_function(svc))
    store.save(purpose, selection)
    return store


def _chosen(store, purpose, svc):
    return store.load(purpose).chosen_set(members_function(svc))


# -- the buttons ----------------------------------------------------------------------------------

def test_the_several_buttons_open_the_host_s_multiple_choice_and_hide_without_the_tree(monkeypatch, svc):
    from desktop.widgets.shelf_pick_button import ShelfPickButton
    opened, picked = [], []
    monkeypatch.setattr(shelf_picker, "pick_from_shelf", lambda *a, **k: picked.append(1))
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: None)
    button = ShelfPickButton(None, text="Several", on_several=opened.append)
    assert button.isHidden()                                                   # absent without the tree
    button.pick()
    assert opened == []
    monkeypatch.setattr(shelf_picker, "shelf_tree_service", lambda: svc)
    button.refresh()
    assert not button.isHidden()
    button.click()
    assert opened == [svc] and picked == []                                    # the several mode, not one pick
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    app_src = (root / "genizah_app.py").read_text(encoding="utf-8")
    assert "on_several=self._browse_rd_pick_several" in app_src                 # the desk toolbar
    assert app_src.count("on_several=self._browse_rd_pick_several") == 1       # Browse: its picker's switch
    assert "on_several=self._lists_pick_from_shelf" in app_src
    assert "on_several=self._add_several_from_shelf" in (root / "desktop/puzzle.py").read_text(encoding="utf-8")


# -- the Reading Desk -----------------------------------------------------------------------------

class _FakeWorker(QObject):
    """ReadingDeskWorker without a thread: its results are emitted by the test."""

    sources_ready = pyqtSignal(list)
    error = pyqtSignal(str)
    finished = pyqtSignal()
    made: list = []

    def __init__(self, sys_ids, parent=None):
        super().__init__()
        self.sys_ids = list(sys_ids)
        self.done = False
        _FakeWorker.made.append(self)

    def start(self):
        pass

    def isRunning(self):
        return not self.done

    def isFinished(self):
        return self.done

    def end(self):
        self.done = True
        self.finished.emit()


class _Desk(QWidget):
    METHODS = ("_browse_rd_pick_several", "_browse_rd_take_from_shelf", "_browse_rd_add_entries",
               "_browse_rd_add_entry", "_browse_rd_load_sources", "_browse_rd_retire_sources",
               "_browse_rd_page_list", "_browse_rd_on_sources_loaded", "_browse_rd_limit",
               "_browse_rd_base_ids", "_browse_rd_preload", "_browse_rd_full_manuscript",
               "_browse_rd_images_drawn", "_browse_rd_images_known", "_browse_rd_fetch_images")

    def __init__(self, app, active=True, current="", on_desk=()):
        super().__init__()
        _bind(self, app.GenizahGUI, self.METHODS)
        self.browse_reading_desk_active = active
        self.browse_reading_desk_state = app.ReadingDeskState()
        for i, sid in enumerate(on_desk):
            self.browse_reading_desk_state.entries.append(app.ReadingDeskEntry(sys_id=sid, shelfmark=sid,
                                                                               pages=[], sequence_order=i))
        self.current_browse_sid = current
        self.meta_mgr = SimpleNamespace(get_meta_for_id=lambda sid: (f"SM {sid[-6:]}", ""))
        self.pages = {}                                                        # sys_id -> pages in the index
        self.read = []                                                         # get_full_manuscript, in order
        self.searcher = SimpleNamespace(get_full_manuscript=self._full, page_count=lambda sid: self.pages.get(sid, 1))
        self.status_label = QLabel()
        self._browse_rd_worker = None
        self._shelf_selections = shelf_picker.SelectionStore()
        self.lists_mgr = None
        self.renders, self.enriched, self.entered = 0, [], []

    def _full(self, sid):
        self.read.append((sid, self.browse_reading_desk_active, len(self.browse_reading_desk_state.entries)))
        return [{"p_num": 1, "text": f"text {sid}"}]

    def _browse_rd_render(self):
        self.renders += 1

    def _browse_rd_enrich_entry(self, sid, volume_ie=None):
        self.enriched.append(sid)

    def _browse_rd_shelf_start(self):
        return None

    def _lists_after_shelf_save(self):
        pass

    def _browse_enter_reading_desk(self, fragments_info, pgpid=None):
        self.entered.append([f["sys_id"] for f in fragments_info])
        self.browse_reading_desk_active = True
        for f in fragments_info:
            self.browse_reading_desk_state.entries.append(SimpleNamespace(sys_id=f["sys_id"], sources=None,
                                                                          pgp_doc=None, sequence_order=0))
        self._browse_rd_render()


@pytest.fixture
def app(monkeypatch):
    import genizah_app
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "ReadingDeskWorker", _FakeWorker)
    _FakeWorker.made.clear()
    return genizah_app


def test_no_manuscript_and_no_desk_opens_no_picker(app, svc, monkeypatch):
    told, opened = [], []
    monkeypatch.setattr(app.QMessageBox, "information", lambda _p, _t, text: told.append(text))
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    desk = _Desk(app, active=False, current="")
    desk._browse_rd_pick_several(svc)
    assert told == ["Please load a manuscript first"] and opened == []
    desk.current_browse_sid = SYS["as2"][0]
    desk._browse_rd_pick_several(svc)
    assert len(opened) == 1 and opened[0]["purpose"] == "reading_desk"
    assert opened[0]["on_confirm"] == desk._browse_rd_take_from_shelf          # the call site
    assert opened[0]["store"] is desk._shelf_selections


def test_without_a_desk_one_is_started_with_the_manuscript_shown_and_the_picks(app, svc):
    current = SYS["as2"][0]
    desk = _Desk(app, active=False, current=current)
    picks = _entries(svc, [current, SYS["as2"][1], SYS["as2"][2]])
    taken = desk._browse_rd_take_from_shelf(picks, "x")
    assert desk.entered == [[current, SYS["as2"][1], SYS["as2"][2]]]          # the shown one once, first
    assert sorted(taken) == sorted([current, SYS["as2"][1], SYS["as2"][2]])
    assert desk.renders == 1


def test_the_desk_takes_up_to_thirty_with_what_it_holds_in_one_batch_and_keeps_the_rest(app, svc):
    sids = _many(svc, 35)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    taken = desk._browse_rd_take_from_shelf(_entries(svc, sids), "x")
    added = sids[1:30]                                                         # 30 with the one it holds
    assert taken == [sids[0]] + added                                          # one on the desk: taken
    assert [e.sys_id for e in desk.browse_reading_desk_state.entries] == [sids[0]] + added
    assert desk.renders == 1                                                   # one render
    assert [w.sys_ids for w in _FakeWorker.made] == [added]                    # one source worker
    assert desk.enriched == added
    assert "29 added to the Reading Desk" in desk.status_label.text()
    assert "Up to 30 manuscripts here" in desk.status_label.text()           # why the rest stayed


def test_the_desk_takes_pages_up_to_a_thousand_counting_those_on_it(app, svc):
    sids = _many(svc, 5)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    desk.pages = {sids[0]: 300, sids[1]: 300, sids[2]: 300, sids[3]: 300, sids[4]: 0}   # sids[4]: images only
    taken = desk._browse_rd_take_from_shelf(_entries(svc, [sids[1], sids[2], sids[3], sids[4]]), "x")
    assert taken == [sids[1], sids[2]]                                         # 900 pages; a third: 1,200
    assert "The Reading Desk holds up to 1,000 pages; these would make 1,200" in desk.status_label.text()
    assert desk._browse_rd_limit().refusal((), [sids[4]]) is None             # 2 images: 902


def test_the_pages_are_read_before_the_desk_changes(app, svc):
    # a progress window pumps events while the pages load: nothing may find a desk half made
    sids = _many(svc, 4)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    desk._browse_rd_take_from_shelf(_entries(svc, sids[1:]), "x")
    assert [r[0] for r in desk.read] == sids[1:] and {r[2] for r in desk.read} == {1}   # read, then added
    new = _Desk(app, active=False, current=sids[0])
    new._browse_rd_take_from_shelf(_entries(svc, sids[1:3]), "x")
    assert all(not active for _sid, active, _n in new.read) and new.entered == [sids[:3]]


def test_the_rest_stay_chosen_through_the_picker(app, svc):
    # the real dialog's confirm: what the desk took leaves the kept choice, the rest stays
    sids = _many(svc, 14)
    store = _store_with(svc, "reading_desk", sids)
    desk = _Desk(app, active=True)
    desk._shelf_selections = store
    desk.pages = {sid: 100 for sid in sids}                                    # ten make 1,000 pages
    dialog = shelf_picker.ShelfPickerDialog(svc, mode="multi", purpose="reading_desk",
                                           on_confirm=desk._browse_rd_take_from_shelf, store=store,
                                           limit=desk._browse_rd_limit())
    try:
        dialog.show()
        import time
        end = time.monotonic() + 8
        while time.monotonic() < end and (dialog._view is None or dialog._loads.busy()):
            _APP.processEvents()
        dialog.confirm_button.click()
        while time.monotonic() < end and dialog._busy:
            _APP.processEvents()
        assert _chosen(store, "reading_desk", svc) == set(sids[10:])
        assert dialog.isVisible() and dialog.note.text() == (                 # the picker says why
            "4 are still chosen · The Reading Desk holds up to 1,000 pages; these would make 1,400")
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_no_source_result_is_lost_when_adds_overlap_and_the_exit_retires_them(app, svc):
    sids = _many(svc, 4)
    desk = _Desk(app, active=True)
    desk._browse_rd_add_entry(sids[0], "typed")                                # a typed add's worker ...
    desk._browse_rd_take_from_shelf(_entries(svc, sids[1:3]), "x")             # ... then a batch
    first, second = _FakeWorker.made
    assert first.sys_ids == [sids[0]] and second.sys_ids == sids[1:3]
    second.sources_ready.emit([(sids[1], ["src B"], {"d": 1})])
    first.sources_ready.emit([(sids[0], ["src A"], {})])                       # the earlier one still lands
    by = {e.sys_id: e for e in desk.browse_reading_desk_state.entries}
    assert by[sids[0]].sources == ["src A"] and by[sids[1]].sources == ["src B"]
    from desktop.gui_threads import _ORPHANED_WORKERS
    assert any(w is first for w in _ORPHANED_WORKERS)                           # held until its thread ends
    desk._browse_rd_retire_sources()                                           # the desk closes
    second.sources_ready.emit([(sids[2], ["late"], {})])
    assert by[sids[2]].sources != ["late"]                                     # no longer listened to
    for worker in (first, second):
        worker.end()
    _APP.processEvents()
    assert not any(w is first or w is second for w in _ORPHANED_WORKERS)


def test_the_desk_worker_s_results_are_not_its_thread_s_end():
    # Codex: ReadingDeskWorker.finished carried the results and shadowed QThread.finished, so
    # _keep_until_finished let the worker go before its thread had ended
    from PyQt6.QtCore import QThread
    from desktop.gui_threads import ReadingDeskWorker
    assert "finished" not in ReadingDeskWorker.__dict__ and "sources_ready" in ReadingDeskWorker.__dict__
    assert issubclass(ReadingDeskWorker, QThread)


# -- the Puzzle -----------------------------------------------------------------------------------

@pytest.fixture
def puzzle(tmp_path, monkeypatch):
    import puzzle_window_harness as pwh
    env = pwh.PuzzleEnv.create(tmp_path, monkeypatch)
    env.host.meta_mgr = SimpleNamespace(get_meta_for_id=lambda sid: (f"SM {sid[-6:]}", ""),
                                        get_library_for_id=lambda sid: "JTS")
    env.pwh = pwh
    yield env
    env.close()


def _cached_folios(win, sids):
    for i, sid in enumerate(sids):
        win._folio_lists[sid] = [{"label": "1r", "fl_id": f"FL{i}"}]


def test_a_fragment_is_taken_only_once_its_image_is_on_the_canvas(puzzle, svc):
    sids = _many(svc, 3)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    answer = win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset())
    assert answer is shelf_picker.STAGED
    assert _chosen(store, "puzzle", svc) == set(sids)                         # sent, not yet on the canvas
    loads = puzzle.pwh.take_started()
    loads[0].deliver()
    loads[1].fail()                                                            # a failed image stays chosen
    loads[2].deliver(image=b"not an image")                                    # and so does a broken one
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == {sids[1], sids[2]}
    assert win._shelf_waiting == {}


def test_bytes_that_are_not_an_image_put_nothing_on_the_canvas(puzzle, svc):
    # Codex #74 round 1: a "loaded" image that was not one left a null-pixmap item in the scene, and
    # the next confirm took the fragment as already on the canvas
    sids = _many(svc, 1)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    before = len(win.canvas_view.scene.items())
    win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset())
    puzzle.pwh.take_started()[0].deliver(image=b"not an image")
    puzzle.pwh.pump()
    assert not any(key[0] == sids[0] for key in win._fragment_items)
    assert len(win.canvas_view.scene.items()) == before                       # nor its placeholder
    assert _chosen(store, "puzzle", svc) == set(sids)
    win._take_from_shelf(_entries(svc, sids), svc, store)                     # the next confirm sends it again
    win._shelf_chosen_again(frozenset())
    again = puzzle.pwh.take_started()
    assert len(again) == 1
    again[0].deliver()
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == set()


def test_a_replaced_canvas_does_not_keep_the_picker_waiting(puzzle, svc, monkeypatch):
    # Codex #74 round 1: waits sent to the old canvas blocked the picker on the new one for 30 s
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    sids = _many(svc, 1)
    win = puzzle.win
    win.app._shelf_selections = _store_with(svc, "puzzle", sids)
    _cached_folios(win, sids)
    win._add_several_from_shelf(svc)
    opened[0]["on_confirm"](_entries(svc, sids), "x")
    win._shelf_chosen_again(frozenset())
    win._clear_canvas()                                                        # New: another canvas
    win._add_several_from_shelf(svc)
    assert len(opened) == 2


def test_a_stale_failure_of_a_replaced_canvas_leaves_the_new_canvas_s_wait(puzzle, svc):
    # Codex #74 round 2: the old canvas's folio lookup failed after the canvas was replaced and the
    # same manuscript sent again; the failure removed the new wait, so its landing took nothing
    sids = _many(svc, 1)
    sid = sids[0]
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    meta = puzzle.pwh.FakeMetaLoader
    win._take_from_shelf(_entries(svc, sids), svc, store)                     # old canvas: a folio lookup
    win._shelf_chosen_again(frozenset())
    old = meta.made[-1]
    win._clear_canvas()                                                        # New
    win._take_from_shelf(_entries(svc, sids), svc, store)                     # the same manuscript again
    win._shelf_chosen_again(frozenset())
    new = meta.made[-1]
    assert new is not old and sid in win._shelf_waiting
    old.meta_failed.emit(sid, "offline")                                       # the old failure arrives
    assert sid in win._shelf_waiting                                           # the new wait stays
    new.meta_ready.emit(sid, "SM", [{"label": "1r", "fl_id": "FL9"}])
    puzzle.pwh.finish_loads()
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == set()                              # and the new landing took it
    # a stale image result of a replaced canvas cannot touch the waits either: _clear_canvas emptied
    # the requests it would have to match (_take_request)
    assert all(w[0] == win._canvas_gen for w in win._shelf_waiting.values())


def test_the_puzzle_s_picker_reopens_at_the_last_fragment_added_from_the_shelf(puzzle, svc, monkeypatch):
    # the integration check: the box it started from is cleared after an add, so the next opening
    # started at the libraries; the web's shelf_pick memory reopens at the last pick
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    sids = _many(svc, 3)
    win = puzzle.win
    win.app._shelf_selections = _store_with(svc, "puzzle", sids)
    _cached_folios(win, sids)
    win._add_several_from_shelf(svc)
    assert opened[0]["start"] is None                                          # nothing added yet
    entries = _entries(svc, sids)
    opened[0]["on_confirm"](entries, "x")
    win._shelf_chosen_again(frozenset())
    puzzle.pwh.finish_loads()
    puzzle.pwh.pump()
    win._add_several_from_shelf(svc)
    assert opened[1]["start"] == entries[-1]["placement_id"] != ""             # the last one's placement
    one = _entries(svc, _many(svc, 4)[3:])[0]
    win._add_from_shelf(one)                                                   # a single pick does the same
    assert win.btn_shelf._start() == one["placement_id"]


def test_the_desk_s_picker_reopens_at_the_last_manuscript_added_from_the_shelf(app, svc, monkeypatch):
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    sids = _many(svc, 3)
    desk = _Desk(app, active=True)
    _bind(desk, app.GenizahGUI, ("_browse_rd_shelf_start",))
    from PyQt6.QtWidgets import QLineEdit
    desk.browse_rd_shelf_input = QLineEdit("CUL T-S AS 39.7")                  # filled when the desk opened
    desk._browse_shelf_start = lambda typed=False: "place-of-the-shown-manuscript"
    desk._browse_rd_pick_several(svc)
    assert opened[0]["start"] == "place-of-the-shown-manuscript"               # first: the shown manuscript
    entries = _entries(svc, sids)
    opened[0]["on_confirm"](entries, "x")
    desk._browse_rd_pick_several(svc)
    assert opened[1]["start"] == entries[-1]["placement_id"] != ""             # then the last one added


def test_a_canvas_replaced_meanwhile_takes_nothing(puzzle, svc):
    sids = _many(svc, 2)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset())
    win._canvas_gen += 1                                                       # New / another join opened
    for load in puzzle.pwh.take_started():
        load.deliver()
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == set(sids)


def test_up_to_thirty_with_the_canvas_are_sent_side_by_side_and_those_on_it_are_taken(puzzle, svc):
    sids = _many(svc, 33)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    win.add_fragment(sids[0], "SM", "1r", "FL0")                               # already on the canvas
    puzzle.pwh.finish_loads()
    puzzle.pwh.pump()
    win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset())
    loads = puzzle.pwh.take_started()
    assert len(loads) == 29                                                    # 30 with the one on the canvas
    assert "Up to 30 fragments here" in win.statusBar().currentMessage()
    assert win._shelf_limit().refusal((), [sids[31]]) is not None             # those loading count too
    for load in loads:
        load.deliver()
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == set(sids[30:])                     # the rest stay chosen
    xs = sorted(item.pos().x() for key, item in win._fragment_items.items() if key[0] in sids[1:30])
    assert len(xs) == 29 and all(b > a for a, b in zip(xs, xs[1:]))           # side by side, not stacked


def test_what_is_chosen_again_during_the_confirm_stays_chosen(puzzle, svc):
    sids = _many(svc, 2)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    win.add_fragment(sids[0], "SM", "1r", "FL0")
    puzzle.pwh.finish_loads()
    win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset(sids))                                   # both chosen again meanwhile
    puzzle.pwh.finish_loads()
    puzzle.pwh.pump()
    assert _chosen(store, "puzzle", svc) == set(sids)


def test_the_picker_does_not_reopen_while_fragments_it_sent_are_loading(puzzle, svc, monkeypatch):
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    sids = _many(svc, 1)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    win.app._shelf_selections = store
    _cached_folios(win, sids)
    win._add_several_from_shelf(svc)
    assert len(opened) == 1 and opened[0]["purpose"] == "puzzle"
    assert (opened[0]["limit"].max_items, opened[0]["limit"].max_pages) == (30, None)   # the Puzzle's 30
    win._fragment_items[(sids[0], "1r")] = win._fragment_items[(sids[0], "1v")] = None   # two folios of one
    assert opened[0]["limit"].totals(())[0] == 2                               # are two fragments
    win._fragment_items.clear()
    opened[0]["on_confirm"](_entries(svc, sids), "x")                          # the call site
    win._shelf_chosen_again(frozenset())
    win._add_several_from_shelf(svc)
    assert len(opened) == 1                                                    # not while it is loading
    puzzle.pwh.finish_loads()
    puzzle.pwh.pump()
    win._add_several_from_shelf(svc)
    assert len(opened) == 2


# -- the Lists tab --------------------------------------------------------------------------------

class _Lists(QWidget):
    def __init__(self, app, mgr, list_id):
        super().__init__()
        _bind(self, app.GenizahGUI, ("_lists_pick_from_shelf", "_lists_take_from_shelf"))
        self.LIST_SHELF_MAX = app.GenizahGUI.LIST_SHELF_MAX
        self.lists_mgr = mgr
        self.lists_current_list_id = list_id
        self.status_label = QLabel()
        self._shelf_selections = shelf_picker.SelectionStore()
        self.refreshed = 0
        self.uploads = 0
        self.refresh_fails = False

    def lists_refresh_all(self):
        self.refreshed += 1
        if self.refresh_fails:
            raise RuntimeError("panel gone")

    def _lists_auto_sync(self):
        self.uploads += 1


@pytest.fixture
def lists(tmp_path, monkeypatch):
    from shared import lists_manager as lm
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lm.Config, "INDEX_DIR", str(tmp_path))
    mgr = lm.ListsManager(None)
    return SimpleNamespace(lm=lm, mgr=mgr, list_id=mgr.create_list("Mine"))


def _in_list(mgr, list_id):
    return sorted(it["sys_id"] for it in mgr.data["items"].values() if list_id in it.get("lists", []))


def test_the_list_shown_gets_the_choice_in_one_add_and_a_held_manuscript_once(app, svc, lists, monkeypatch):
    sids = _many(svc, 3)
    lists.mgr.add_item(sids[0], lists.list_id, fl_id="FL-page")                # held as one of its pages
    host = _Lists(app, lists.mgr, lists.list_id)
    calls = []
    real = lists.mgr.add_items_bulk
    monkeypatch.setattr(lists.mgr, "add_items_bulk", lambda *a, **k: (calls.append(a[0]), real(*a, **k))[1])
    taken = host._lists_take_from_shelf(_entries(svc, sids), lists.list_id)
    assert taken == sids and calls == [sids[1:]]                               # one bulk add of what is new
    assert _in_list(lists.mgr, lists.list_id) == sorted(sids)                  # each once
    assert host.status_label.text() == "2 added to the list" and host.refreshed == 1 and host.uploads == 1


def test_a_list_save_that_failed_takes_nothing_and_a_retry_saves(app, svc, lists, monkeypatch):
    sids = _many(svc, 2)
    host = _Lists(app, lists.mgr, lists.list_id)
    real_write = lists.lm.write_bytes_atomic
    broken = {"on": True}
    monkeypatch.setattr(lists.lm, "write_bytes_atomic",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("full")) if broken["on"] else real_write(*a, **k))
    assert host._lists_take_from_shelf(_entries(svc, sids), lists.list_id) == []
    assert host.status_label.text() == "The list could not be saved." and host.refreshed == 0
    assert host.uploads == 0                                                   # nothing written: no upload (Codex #74)
    broken["on"] = False
    assert host._lists_take_from_shelf(_entries(svc, sids), lists.list_id) == sids       # all held in memory: saved now
    assert host.uploads == 1                                                   # the saved retry: exactly one
    assert not lists.mgr.saves_failing()
    reread = lists.lm.ListsManager(None)
    assert sorted(it["sys_id"] for it in reread.data["items"].values() if lists.list_id in it["lists"]) == sorted(sids)


def _breakable_writes(lists, monkeypatch):
    real_write = lists.lm.write_bytes_atomic
    broken = {"on": True}
    monkeypatch.setattr(lists.lm, "write_bytes_atomic",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("full")) if broken["on"] else real_write(*a, **k))
    return broken


def test_a_failed_add_is_rolled_back_so_the_retry_writes_the_choice_as_it_is_then(app, svc, lists, monkeypatch):
    # Codex on a65dfe5: the failed try's manuscripts stayed in memory, and a retry after the reader
    # changed the choice wrote them too
    a, b, c = _many(svc, 3)
    host = _Lists(app, lists.mgr, lists.list_id)
    broken = _breakable_writes(lists, monkeypatch)
    assert host._lists_take_from_shelf(_entries(svc, [a, b]), lists.list_id) == []
    assert _in_list(lists.mgr, lists.list_id) == []                            # rolled back in memory
    broken["on"] = False
    assert host._lists_take_from_shelf(_entries(svc, [a, c]), lists.list_id) == [a, c]   # b unticked, c ticked
    assert _in_list(lists.mgr, lists.list_id) == sorted([a, c])
    reread = lists.lm.ListsManager(None)
    assert sorted(it["sys_id"] for it in reread.data["items"].values() if lists.list_id in it["lists"]) == sorted([a, c])


def test_what_a_list_held_before_the_failed_add_survives_its_rollback(app, svc, lists, monkeypatch):
    a, b, d, x = _many(svc, 4)
    other = lists.mgr.create_list("Other")
    lists.mgr.add_item(x, lists.list_id)                                       # held before, as itself
    lists.mgr.add_item(d, lists.list_id, fl_id="FL-page")                      # and as a page
    lists.mgr.add_item(a, other)                                               # a: in another list before
    host = _Lists(app, lists.mgr, lists.list_id)
    _breakable_writes(lists, monkeypatch)
    assert host._lists_take_from_shelf(_entries(svc, [a, b, d, x]), lists.list_id) == []
    assert _in_list(lists.mgr, lists.list_id) == sorted([x, d])                # only what it held before
    assert lists.mgr.data["items"][a]["lists"] == [other]                      # a keeps its other list
    assert b not in lists.mgr.data["items"]                                    # made by the try: gone


def test_the_list_cap_a_vanished_list_and_a_refresh_that_fails(app, svc, lists, monkeypatch):
    host = _Lists(app, lists.mgr, lists.list_id)
    monkeypatch.setattr(host, "LIST_SHELF_MAX", 2)
    sids = _many(svc, 3)
    host.refresh_fails = True
    assert host._lists_take_from_shelf(_entries(svc, sids), lists.list_id) == sids[:2]   # a failed refresh still took them
    assert _in_list(lists.mgr, lists.list_id) == sorted(sids[:2])
    assert host.uploads == 1                                                   # the upload asked for all the same
    lists.mgr.delete_list(lists.list_id)
    assert host._lists_take_from_shelf(_entries(svc, sids), lists.list_id) == []


def test_the_lists_picker_opens_for_the_list_shown_and_never_for_recent(app, svc, lists, monkeypatch):
    opened = []
    monkeypatch.setattr(shelf_picker, "pick_several_from_shelf", lambda *a, **k: opened.append(k))
    host = _Lists(app, lists.mgr, "recent")
    host._lists_pick_from_shelf(svc)
    assert opened == []
    host.lists_current_list_id = lists.list_id
    host._lists_pick_from_shelf(svc)
    assert len(opened) == 1 and opened[0]["purpose"] == "lists"
    assert opened[0]["confirm_label"] == "Add them to this list"
    assert opened[0]["on_confirm"](_entries(svc, _many(svc, 1)), "x") == _many(svc, 1)   # the call site


def test_the_desk_checks_room_again_after_the_pages_load(app, svc):
    # what it holds may grow while the pages load (events are pumped): room is checked again right
    # before the desk changes; what no longer fits stays chosen
    sids = _many(svc, 5)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    desk.pages = {sid: 300 for sid in sids}
    real = desk._full

    def grew(sid):
        if not desk.read:                                                  # the first read: the desk grows
            desk.browse_reading_desk_state.entries.append(app.ReadingDeskEntry(
                sys_id=sids[4], shelfmark=sids[4], pages=[], sequence_order=9))
        return real(sid)
    desk.searcher.get_full_manuscript = grew
    taken = desk._browse_rd_take_from_shelf(_entries(svc, sids[1:4]), "x")
    assert taken == [sids[1]]                                              # 300 + 300 held + 300: 900
    assert [e.sys_id for e in desk.browse_reading_desk_state.entries] == [sids[0], sids[4], sids[1]]
    new = _Desk(app, active=False, current=sids[0])
    new.searcher.get_full_manuscript = lambda sid: (setattr(new, "browse_reading_desk_active", True), [])[1]
    assert new._browse_rd_take_from_shelf(_entries(svc, sids[1:3]), "x") == []   # a desk opened meanwhile


def test_the_puzzle_checks_room_again_before_placing_one_from_the_shelf(puzzle, svc):
    sids = _many(svc, 2)
    store = _store_with(svc, "puzzle", sids)
    win = puzzle.win
    _cached_folios(win, sids)
    win._take_from_shelf(_entries(svc, sids), svc, store)
    win._shelf_chosen_again(frozenset())
    loads = puzzle.pwh.take_started()
    for i in range(30):                                                    # filled by hand meanwhile
        win._fragment_items[("99x%d" % i, "1r")] = None
    loads[0].deliver()
    puzzle.pwh.pump()
    assert (sids[0], "1r") not in win._fragment_items and not any(k[0] == sids[0] for k in win._fragment_items)
    assert _chosen(store, "puzzle", svc) == set(sids)                      # it stays chosen
    assert "Up to 30 fragments here" in win.statusBar().currentMessage()
    for i in range(30):
        win._fragment_items.pop(("99x%d" % i, "1r"))


def _untick_in_dialog(svc, store, purpose, limit, sids):
    """The real picker with the host's limit and a kept choice: unticking one of it."""
    leaves = {lf.sys_id: lf for library in svc.children(None) for node in svc.children(library.id)
              for lf in svc.leaves_under(node.id)}
    dialog = shelf_picker.ShelfPickerDialog(svc, mode="multi", purpose=purpose, store=store,
                                           on_confirm=lambda *_a: None, limit=limit)
    try:
        dialog._toggle_leaf(leaves[sids[0]])
        return dialog.chosen().chosen_set(members_function(svc)), dialog.note.text()
    finally:
        dialog.reject()
        dialog.deleteLater()


def test_an_untick_always_passes_on_a_desk_over_its_limit(app, svc):
    # #80, Codex: an untick passes before anything is counted
    sids = _many(svc, 35)
    desk = _Desk(app, active=True, on_desk=sids[2:35])                       # 33 held: over 30
    store = _store_with(svc, "reading_desk", sids[:2])
    chosen, note = _untick_in_dialog(svc, store, "reading_desk", desk._browse_rd_limit(), sids)
    assert chosen == {sids[1]} and note == ""


def test_an_untick_always_passes_on_a_full_puzzle(puzzle, svc):
    sids = _many(svc, 2)
    win = puzzle.win
    for i in range(30):                                                    # a full canvas
        win._fragment_items[("99y%d" % i, "1r")] = None
    try:
        store = _store_with(svc, "puzzle", sids)
        chosen, note = _untick_in_dialog(svc, store, "puzzle", win._shelf_limit(), sids)
        assert chosen == {sids[1]} and note == ""
    finally:
        for i in range(30):
            win._fragment_items.pop(("99y%d" % i, "1r"))


def test_the_desk_fetches_unknown_image_lists_before_the_room_is_checked(app, svc):
    # Codex #84: a manuscript not fetched yet counts 2, then the desk draws every image it fetches;
    # the confirm fetches them first, so what has too many images stays chosen
    sids = _many(svc, 3)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    fetched = []
    nli_cache = {}

    def enrich(sid, suffix=1):
        fetched.append(sid)
        nli_cache[sid] = {"images_nli": [{"url": "u"}] * (998 if sid == sids[1] else 5)}
    desk.meta_mgr = SimpleNamespace(get_meta_for_id=lambda sid: (f"SM {sid[-6:]}", ""), nli_cache=nli_cache,
                                    enrich_metadata=enrich)
    taken = desk._browse_rd_take_from_shelf(_entries(svc, sids[1:]), "x")
    assert sorted(fetched) == sorted(sids[1:])                             # both fetched before the check
    assert taken == [sids[1]]                                              # 1 + 998 pages; 5 more: 1,004
    assert "The Reading Desk holds up to 1,000 pages" in desk.status_label.text()
    fetched.clear()
    desk._browse_rd_take_from_shelf(_entries(svc, [sids[2]]), "x")
    assert fetched == []                                                   # known now: not fetched again


_STALE_TESTS = ("shared.cudl_identity.entry_decision_stale",                # another item's Cambridge images
                "shared.nli_crossref_service.katz_cache_needs_refresh",     # a Katz entry due a refresh
                "shared.codicological.bodleian_cache_needs_refresh")        # an Oxford entry due a refresh


def _stale_when_marked(monkeypatch, predicate):
    """Each freshness test takes the cache entry last: here an entry is stale when marked so."""
    monkeypatch.setattr(predicate, lambda *a: bool((a[-1] or {}).get("stale")))


@pytest.mark.parametrize("predicate", _STALE_TESTS)
def test_a_stale_image_list_is_fetched_again_before_the_room_is_checked(app, svc, monkeypatch, predicate):
    # Codex #84: an entry holding images but due a refresh was taken as known, so its short list passed
    # the 1,000-page check and the desk, refreshing it right after the add, went past the limit
    _stale_when_marked(monkeypatch, predicate)
    sids = _many(svc, 3)
    desk = _Desk(app, active=True, on_desk=[sids[0]])
    fetched = []
    nli_cache = {sids[1]: {"images_nli": [{"url": "u"}] * 5, "stale": True},
                 sids[2]: {"images_nli": [{"url": "u"}] * 5}}

    def enrich(sid, suffix=1):
        fetched.append(sid)
        nli_cache[sid] = {"images_nli": [{"url": "u"}] * 998}
    desk.meta_mgr = SimpleNamespace(get_meta_for_id=lambda sid: (f"SM {sid[-6:]}", ""), nli_cache=nli_cache,
                                    enrich_metadata=enrich)
    taken = desk._browse_rd_take_from_shelf(_entries(svc, sids[1:]), "x")
    assert fetched == [sids[1]]                                            # the stale one only
    assert taken == [sids[1]]                                              # 1 + 998 pages; 5 more: 1,004
    assert "The Reading Desk holds up to 1,000 pages" in desk.status_label.text()


@pytest.mark.parametrize("predicate", _STALE_TESTS)
def test_the_desk_fetches_again_exactly_what_the_confirm_counts_as_not_known(app, monkeypatch, predicate):
    # the confirm's question and the desk's own fetch are one: a stale list is fetched by the desk, a
    # current one is not, and an empty one is
    _stale_when_marked(monkeypatch, predicate)
    started = []

    class _Thread:
        def __init__(self, _meta_mgr, sys_id, suffix=1):
            self.sys_id = sys_id
            self.finished_signal = SimpleNamespace(connect=lambda _slot: None)

        def isRunning(self):
            return False

        def start(self):
            started.append(self.sys_id)
    monkeypatch.setattr(app, "EnrichMetadataThread", _Thread)
    host = SimpleNamespace(current_browse_sid="", browse_reading_desk_active=False, _browse_rd_enrich_threads={},
                           meta_mgr=SimpleNamespace(nli_cache={"stale": {"images_nli": [1], "stale": True},
                                                               "current": {"images_nli": [1]}, "empty": {}}))
    _bind(host, app.GenizahGUI, ("_browse_rd_enrich_entry", "_browse_rd_images_known"))
    for sid in ("stale", "current", "empty"):
        host._browse_rd_enrich_entry(sid)
        assert (sid in started) is (not host._browse_rd_images_known(sid))
    assert started == ["stale", "empty"]


def test_a_batch_queued_for_the_puzzle_is_dropped_when_the_canvas_is_replaced(puzzle, svc, monkeypatch):
    # Codex #87: Add to Puzzle while an earlier batch loads queues the new one; a new puzzle (or another
    # join opened) meanwhile must not receive it, nor autosave it into an unrelated join
    sids = _many(svc, 2)
    win = puzzle.win
    _cached_folios(win, sids)
    assert win.take_entries_from_shelf(_entries(svc, sids[:1]), svc) is True     # loading
    assert win.take_entries_from_shelf(_entries(svc, sids[1:]), svc) is False    # queued behind it
    assert win._shelf_queued is not None
    win._clear_canvas()                                                    # a new puzzle
    assert win._shelf_queued is None
    taken = []
    monkeypatch.setattr(win, "_take_from_shelf", lambda *a: taken.append(a))
    win._take_queued()
    assert taken == []
