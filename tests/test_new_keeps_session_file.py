# -*- coding: utf-8 -*-
"""New must overwrite session.json with the cleared state, never delete it.

`_reset_search` and `_reset_composition` used to os.remove the whole file and
then schedule the ordinary 500 ms save. Every key of that save is rebuilt from
memory except one: `join_lab`, which `_save_session` carries forward FROM THE
FILE when the Joins Lab window was not built this session. With the file just
deleted there was nothing to carry, so the Lab's anchor, builders and triage
marks were lost for good. And until the timer fired there was no session file
at all, or (without the delete) the pre-New one: a crash in that window lost
the other tab, the LOCAL opt-outs and Browse, or brought the discarded search
back on the next launch.

These drive the real call sites: the method the New button is connected to,
the real `_save_session`, and whatever save New left pending on the timer.
"""
import ast
import inspect
import json
import os
import textwrap
import types

import pytest

import genizah_app
from shared.config import Config
from shared.lists_manager import ListsManager

APP = genizah_app.GenizahGUI

PRIOR_JOIN_LAB = {
    "open": False,  # closed before quitting, so startup does not rebuild it
    "anchor": {"sys_id": "990000000000000001", "shelfmark": "T-S 12.123",
               "img": 1, "uid": ""},
    "builder": {"lines": ["line one"]},
    "other_builder": {},
    "other_enabled": False,
    "other_mode_idx": 0,
    "triage": {"990000000000000002": "yes", "990000000000000003": "no"},
    "filter_text": "",
    "mat_filter_idx": 0,
    "tri_filter_idx": 0,
    "view_mode": "grid",
}

OLD_REGULAR_RESULTS = [{"display": {"id": "990000000000000011"}, "snippet": "a"}]
OLD_COMP_RESULTS = [{"display": {"id": "990000000000000022"}, "score": 3}]


class _Widget:
    """Takes every call a reset makes and reads back like an empty widget."""

    def __init__(self):
        self._text, self._idx = "", 0

    def setText(self, t):
        self._text = t

    def text(self):
        return self._text

    def clear(self):
        self._text = ""

    def toPlainText(self):
        return self._text

    def setCurrentIndex(self, i):
        self._idx = i

    def currentIndex(self):
        return self._idx

    def __getattr__(self, name):          # setEnabled, setVisible, setRowCount...
        return lambda *a, **k: None


class _Host:
    # Borrowed, not stubbed: the resets and the save run these on the way
    # to the file.
    _save_session = APP._save_session
    reset_ui = APP.reset_ui
    reset_comp_ui = APP.reset_comp_ui
    _set_local_scope_strip_visible = APP._set_local_scope_strip_visible
    _is_browsing_local = APP._is_browsing_local
    _comp_chunk_preference = APP._comp_chunk_preference
    _PASSAGE_FORCED_CONTROLS = APP._PASSAGE_FORCED_CONTROLS
    _discardable = APP._discardable
    _deliver_unless_discarded = APP._deliver_unless_discarded
    _reset_is_pending = APP._reset_is_pending

    def __init__(self):
        self._restoring_session = False
        self._join_workbench = None       # the Joins Lab was not opened this session
        self._local_file_optouts = {r"c:\scans\a.pdf"}
        self._pause_search = types.SimpleNamespace(state="idle")
        self._pause_comp = types.SimpleNamespace(state="idle")
        for name in ("query_input", "mode_combo", "results_table", "status_label",
                     "btn_domain_filter", "lbl_domain_filter", "btn_search",
                     "search_progress", "comp_text_area", "comp_title_input",
                     "comp_tree", "comp_mode_combo", "btn_comp_domain_filter",
                     "lbl_comp_domain_filter", "btn_comp_run", "comp_progress"):
            setattr(self, name, _Widget())
        self.query_input.setText("old query")
        self.comp_text_area.setText("old source text")
        self.last_results = list(OLD_REGULAR_RESULTS)
        self.comp_raw_items = list(OLD_COMP_RESULTS)
        self.comp_raw_filtered = list(OLD_COMP_RESULTS)
        self.export_buttons, self.comp_export_buttons = [], []
        self.pending_saves = 0

    def _schedule_session_save(self):
        # The real one arms a 500 ms QTimer whose timeout is _save_session.
        # It has not fired yet: a crash now finds whatever is on disk.
        self.pending_saves += 1

    def run_pending_saves(self):
        """What the debounce timer would do once it fires."""
        while self.pending_saves:
            self.pending_saves -= 1
            APP._save_session(self)

    def _apply_pause_state(self, *a): pass
    def _update_filter_chip_bar(self): pass
    def _clear_exclusions(self, surface): pass
    def _refuse_stop_during_passage_scan(self): return False
    def _passage_batch_in_flight(self): return False
    def _witness_notify(self, msg): pass
    def _refresh_witness_panel(self): pass
    def _update_load_more_button(self, *a): pass


@pytest.fixture
def session_file(tmp_path, monkeypatch):
    # Every personal-state file this code can reach points into tmp_path, so
    # no run of this file can touch the developer's own copies.
    path = tmp_path / "session.json"
    monkeypatch.setattr(Config, "SESSION_FILE", str(path))
    monkeypatch.setattr(Config, "CONFIG_FILE", str(tmp_path / "config.pkl"))
    monkeypatch.setattr(Config, "LANGUAGE_FILE", str(tmp_path / "lang.pkl"))
    monkeypatch.setattr(ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: {})
    path.write_text(json.dumps({
        "version": 1,
        "regular_search": {"query": "old query", "results": OLD_REGULAR_RESULTS},
        "composition_search": {"source_text": "old source text",
                               "results": OLD_COMP_RESULTS,
                               "filtered_results": OLD_COMP_RESULTS},
        # A Composition search was running when the user pressed New.
        "was_interrupted": True,
        "local_file_optouts": [r"c:\scans\a.pdf"],
        "join_lab": PRIOR_JOIN_LAB,
    }), encoding="utf-8")
    return path


def _on_disk(path, reset):
    assert os.path.exists(path), f"{reset} deleted session.json"
    return json.loads(path.read_text(encoding="utf-8"))


RESETS = ["_reset_search", "_reset_composition"]


@pytest.mark.parametrize("reset", RESETS)
def test_new_puts_the_cleared_state_on_disk_at_once(session_file, reset):
    """Before any timer fires, a crash must find a complete file holding the
    cleared tab, not no file and not the search New was pressed to discard."""
    host = _Host()
    if reset == "_reset_composition":
        host.is_comp_running = True   # reset_comp_ui is what clears it
    getattr(APP, reset)(host)
    saved = _on_disk(session_file, reset)
    assert saved["local_file_optouts"] == [r"c:\scans\a.pdf"]
    regular, comp = saved["regular_search"], saved["composition_search"]
    if reset == "_reset_search":
        assert regular["query"] == "" and regular["results"] == [], (
            "the discarded search is still the saved one")
        # The other tab is not New's to clear.
        assert comp["source_text"] == "old source text"
        assert comp["results"] == OLD_COMP_RESULTS
    else:
        assert comp["source_text"] == "" and comp["results"] == [], (
            "the discarded composition search is still the saved one")
        assert comp["filtered_results"] == []
        assert saved["was_interrupted"] is False, (
            "the next launch would offer to resume the discarded search")
        assert regular["query"] == "old query"
        assert regular["results"] == OLD_REGULAR_RESULTS


@pytest.mark.parametrize("reset", RESETS)
def test_new_keeps_the_joins_lab_state_when_the_lab_was_not_opened(session_file, reset):
    host = _Host()
    getattr(APP, reset)(host)
    host.run_pending_saves()
    saved = _on_disk(session_file, reset)
    assert saved.get("join_lab") == PRIOR_JOIN_LAB, (
        f"{reset} dropped the saved Joins Lab state")
    # And the save really ran: the tab New belongs to is written cleared.
    if reset == "_reset_search":
        assert saved["regular_search"]["query"] == ""
        assert saved["regular_search"]["results"] == []
    else:
        assert saved["composition_search"]["source_text"] == ""
        assert saved["composition_search"]["results"] == []


# --- New drops what the run it stopped still delivers ----------------------

class _StoppableCompThread:
    """A composition run in flight. Cancelled, it still hands back its
    partial rows, and Qt delivers them after New has returned."""

    def __init__(self):
        self.running = True

    def isRunning(self):
        return self.running

    def request_cancel(self):
        self.running = False

    def wait(self, *a):
        return True

    def terminate(self):
        self.running = False


def test_new_drops_what_the_composition_run_it_stopped_still_delivers(session_file):
    host = _Host()
    host.comp_thread = _StoppableCompThread()
    host._emit_comp_search_telemetry = lambda *a, **k: None
    rendered = []
    # What run_composition connects the scan's completion to, as the run starts.
    queued = host._discardable("_comp_new_generation", rendered.append)
    APP._reset_composition(host)                         # New
    assert not host.comp_thread.isRunning()
    queued({"main": OLD_COMP_RESULTS, "filtered": []})
    assert rendered == [], "the stopped run's rows reached the cleared tab"
    # The next run connects after New, and its rows arrive as ever.
    host._discardable("_comp_new_generation", rendered.append)({"main": []})
    assert rendered == [{"main": []}]


class _WitnessBatchThread(_StoppableCompThread):
    """A multi-witness batch: cancelling only asks it to stop at the next
    witness boundary, so it runs on until the witness in flight finishes."""

    def request_cancel(self):
        self.cancel_requested = True


def _batch_host(monkeypatch):
    """A host mid-way through a multi-witness batch, with the deferred-reset
    timer captured instead of armed. Returns (host, batch, timers, emitted)."""
    host = _Host()
    host.is_comp_running = True
    host.comp_thread = batch = _WitnessBatchThread()
    # The real check, with the batch class swapped: a run started later is
    # an ordinary scan, and must not count as a batch.
    host._passage_batch_in_flight = lambda: (
        host.is_comp_running and isinstance(host.comp_thread, _WitnessBatchThread))
    host._stop_auto_expand = lambda msg: None
    host._reset_composition = lambda: APP._reset_composition(host)
    host._retry_pending_reset = lambda *request: APP._retry_pending_reset(host, *request)
    host.lbl_comp_status = _Widget()
    emitted = []
    host._emit_comp_search_telemetry = lambda action, *a, **k: emitted.append(action)
    timers = []
    monkeypatch.setattr(genizah_app.QTimer, "singleShot",
                        staticmethod(lambda ms, fn: timers.append(fn)))
    return host, batch, timers, emitted


def _bind_batch_slots(host, delivered):
    """What run_composition connects as the batch starts: its completion,
    status line and error, each bound to the New generation current now."""
    return [host._discardable("_comp_new_generation",
                              lambda payload, kind=kind: delivered.append((kind, payload)))
            for kind in ("rows", "status", "error")]


def test_new_during_a_witness_batch_drops_what_it_delivers_before_the_reset(session_file, monkeypatch):
    """New waits for the witness in flight before clearing the tab. What the
    batch delivered in that wait -- its partial rows, which rendered and
    started grouping, an error, a status line -- still reached the tab,
    because the discarded run was only marked as such once the reset ran."""
    host, batch, timers, emitted = _batch_host(monkeypatch)
    delivered = []
    on_rows, on_status, on_error = _bind_batch_slots(host, delivered)

    APP._reset_composition(host)                          # New, mid-batch
    assert batch.cancel_requested and host._reset_pending
    assert host.comp_text_area.toPlainText() == "old source text", "cleared mid-batch"
    assert len(timers) == 1

    APP._reset_composition(host)                          # pressed again while it waits
    assert len(timers) == 1, "a second retry chain would run the reset twice"
    assert host.lbl_comp_status.text() == genizah_app.tr(
        "Clearing once the current witness finishes.")

    batch.running = False                                 # the witness in flight finishes
    on_status("Witness 3/17")
    on_error("the witness failed")
    on_rows({"main": OLD_COMP_RESULTS, "filtered": [], "partial": True})
    assert delivered == [], "the batch New discarded still wrote the tab"
    assert emitted == ["cancelled"]

    timers.pop()()                                        # the deferred reset
    assert not host._reset_pending and timers == [], "the reset waited on a batch that had ended"
    assert host.comp_text_area.toPlainText() == "" and host.is_comp_running is False
    saved = _on_disk(session_file, "_reset_composition")["composition_search"]
    assert saved["source_text"] == "" and saved["results"] == []

    # The next search starts after the reset, and everything it sends arrives.
    on_rows, on_status, on_error = _bind_batch_slots(host, delivered)
    on_status("Witness 1/2")
    on_rows({"main": []})
    assert delivered == [("status", "Witness 1/2"), ("rows", {"main": []})]


def test_a_refused_new_leaves_the_running_scan_to_deliver(session_file):
    """A single letter-level search cannot be stopped, so New is refused and
    does nothing: the scan's results are still the tab's to show."""
    host = _Host()
    host.is_comp_running = True
    host.comp_thread = _StoppableCompThread()
    host._refuse_stop_during_passage_scan = lambda: True
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)

    APP._reset_composition(host)                          # New, refused
    assert host.comp_thread.isRunning()
    on_rows({"main": OLD_COMP_RESULTS})
    assert delivered == [("rows", {"main": OLD_COMP_RESULTS})]


@pytest.mark.parametrize("stop", ["toggle_composition", "cancel_composition"])
def test_stop_during_a_witness_batch_still_shows_what_it_found(session_file, monkeypatch, stop):
    """Stop (the button, or Escape) keeps the partial results; only New drops them."""
    host, batch, _timers, _emitted = _batch_host(monkeypatch)
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)

    getattr(APP, stop)(host)
    assert batch.cancel_requested
    batch.running = False
    on_rows({"main": OLD_COMP_RESULTS, "partial": True})
    assert delivered == [("rows", {"main": OLD_COMP_RESULTS, "partial": True})]


# --- A New's retry never outlives its request ------------------------------

class _LaterRun(_StoppableCompThread):
    """A composition run started after New: an ordinary scan, which a reset
    would cancel and wait for."""

    def __init__(self):
        super().__init__()
        self.cancels = 0

    def request_cancel(self):
        self.cancels += 1
        super().request_cancel()


def _start_a_new_run(host):
    """What run_composition leaves as a run starts: a new worker in the
    slot, the run flag up, the search's text in the box."""
    host.comp_text_area.setText("the next search")
    host.comp_thread = run = _LaterRun()
    host.is_comp_running = True
    return run


def _assert_untouched(host, run, delivered, on_rows):
    assert run.cancels == 0 and run.isRunning(), "the old retry cancelled the new run"
    assert host.comp_text_area.toPlainText() == "the next search", "the old retry cleared the new run"
    assert host.is_comp_running is True
    on_rows({"main": OLD_COMP_RESULTS})
    assert delivered == [("rows", {"main": OLD_COMP_RESULTS})], "the new run's results were dropped"


def test_a_second_new_after_the_witness_finished_ends_the_first_ones_retry(session_file, monkeypatch):
    """The witness New waited for finished before the 400 ms retry fired, so
    New pressed again reset at once -- and left the first retry armed, which
    then cancelled and cleared the search started next."""
    host, batch, timers, emitted = _batch_host(monkeypatch)
    APP._reset_composition(host)                          # New, mid-batch
    batch.running = False                                 # the witness finishes
    APP._reset_composition(host)                          # New again: resets at once
    assert host.comp_text_area.toPlainText() == "" and host.is_comp_running is False
    assert len(timers) == 1, "the immediate reset armed a retry of its own"

    run = _start_a_new_run(host)
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)
    timers.pop()()                                        # the first New's retry fires
    assert timers == []
    _assert_untouched(host, run, delivered, on_rows)
    assert emitted == ["cancelled"]


def test_a_second_new_then_typing_the_next_search_keeps_the_text(session_file, monkeypatch):
    """The same retry, firing before the next search is started: it reset the
    tab a second time and wiped what the user had begun to type."""
    host, batch, timers, _emitted = _batch_host(monkeypatch)
    APP._reset_composition(host)                          # New, mid-batch
    batch.running = False
    APP._reset_composition(host)                          # New again: resets at once
    host.comp_text_area.setText("the next search")        # typed, not yet run
    timers.pop()()
    assert host.comp_text_area.toPlainText() == "the next search", "the old retry reset the tab again"
    assert timers == []


@pytest.mark.parametrize("stop", ["toggle_composition", "cancel_composition"])
def test_stop_after_new_finishes_that_new_and_ends_its_retry(session_file, monkeypatch, stop):
    """Stop (the button, or Escape) after the witness finished used to reset
    only the run controls. The New before it had already discarded the
    batch's results, so there was nothing to keep -- and its retry stayed
    armed and cleared the next search. Stop now completes the New."""
    host, batch, timers, _emitted = _batch_host(monkeypatch)
    APP._reset_composition(host)                          # New, mid-batch
    batch.running = False                                 # the witness finishes
    getattr(APP, stop)(host)
    cleared_by_stop = (host.comp_text_area.toPlainText() == "" and host.is_comp_running is False)

    run = _start_a_new_run(host)
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)
    timers.pop()()                                        # the New's retry fires
    _assert_untouched(host, run, delivered, on_rows)
    assert cleared_by_stop, "Stop dropped the New pressed before it"


@pytest.mark.parametrize("stop", ["toggle_composition", "cancel_composition"])
def test_stop_while_new_waits_keeps_waiting_for_the_witness(session_file, monkeypatch, stop):
    """Stop while the witness is still running changes nothing: New already
    asked the batch to stop, the tab still says it is being cleared (not that
    the results found so far are kept -- New dropped them), and the one retry
    does the reset once the witness finishes."""
    host, batch, timers, _emitted = _batch_host(monkeypatch)
    # The real stop guard: it is what says the results are kept.
    host._comp_last_result_method, host._comp_grouping_active = "passage", False
    host._passage_scan_in_flight = lambda: APP._passage_scan_in_flight(host)
    host._refuse_stop_during_passage_scan = lambda: APP._refuse_stop_during_passage_scan(host)
    APP._reset_composition(host)                          # New, mid-batch
    getattr(APP, stop)(host)
    assert host.comp_text_area.toPlainText() == "old source text" and len(timers) == 1
    assert host.lbl_comp_status.text() == genizah_app.tr(
        "Clearing once the current witness finishes.")

    batch.running = False
    timers.pop()()
    assert host.comp_text_area.toPlainText() == "" and host.is_comp_running is False
    assert timers == [] and not host._reset_is_pending()


def test_a_run_started_while_new_waits_is_not_cleared_by_its_retry(session_file, monkeypatch):
    """Any path that starts a composition run once the witness is done
    (run_composition only refuses while the old worker still runs) puts a
    new worker in the slot. The retry belongs to the batch it was armed for."""
    host, batch, timers, _emitted = _batch_host(monkeypatch)
    APP._reset_composition(host)                          # New, mid-batch
    batch.running = False
    run = _start_a_new_run(host)
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)
    timers.pop()()
    assert timers == []
    _assert_untouched(host, run, delivered, on_rows)
    assert not host._reset_is_pending()


def test_new_during_the_next_batch_arms_its_own_retry(session_file, monkeypatch):
    """A retry armed for an earlier batch, still in flight when New is pressed
    during the next one, must not stand in for it: that New discards the new
    batch at once and gets its own retry, and the old one stops."""
    host, first, timers, emitted = _batch_host(monkeypatch)
    APP._reset_composition(host)                          # New during the first batch
    first.running = False
    host.comp_text_area.setText("the next search")
    host.comp_thread = second = _WitnessBatchThread()     # the next batch starts
    delivered = []
    on_rows, _status, _error = _bind_batch_slots(host, delivered)

    APP._reset_composition(host)                          # New during it
    assert second.cancel_requested and len(timers) == 2
    on_rows({"main": OLD_COMP_RESULTS, "partial": True})
    assert delivered == [], "the batch New discarded still wrote the tab"
    assert emitted == ["cancelled", "cancelled"]

    stale, current = timers
    timers.clear()
    stale()                                               # the first batch's retry
    assert host.comp_text_area.toPlainText() == "the next search" and timers == []
    current()                                             # second batch still running
    assert len(timers) == 1 and host.comp_text_area.toPlainText() == "the next search"
    second.running = False
    timers.pop()()
    assert host.comp_text_area.toPlainText() == "" and host.is_comp_running is False
    assert timers == []


def test_the_deferred_reset_still_resets_exactly_once(session_file, monkeypatch):
    host, batch, timers, emitted = _batch_host(monkeypatch)
    resets = []
    host.reset_comp_ui = lambda: (resets.append(1), APP.reset_comp_ui(host))
    APP._reset_composition(host)                          # New, mid-batch
    timers.pop()()                                        # the witness is still running
    assert resets == [] and len(timers) == 1, "the retry did not keep waiting"
    batch.running = False
    timers.pop()()
    assert resets == [1] and timers == []
    assert host.comp_text_area.toPlainText() == "" and host.is_comp_running is False
    assert emitted == ["cancelled"]


def _connects(method):
    """(signal, slot source) of every `<obj>.<signal>.connect(<slot>)` in `method`."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(APP, method))))
    return [(node.func.value.attr, ast.unparse(node.args[0]))
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "connect"
            and isinstance(node.func.value, ast.Attribute)]


# Every signal whose slot writes the tab. Progress and pause acknowledgements
# are left out: the first only moves a bar New hid, and the second carries
# its own run id.
_WRITES_THE_TAB = {"results_signal", "scan_finished_signal", "finished_signal",
                   "error_signal", "status_signal", "phase_signal",
                   "witness_progress_signal"}


@pytest.mark.parametrize("method, guard", [
    ("start_search", "_live("),
    ("run_composition", "_comp_live("),
    ("start_grouping", "_comp_live("),
])
def test_every_worker_signal_that_writes_the_tab_is_dropped_after_new(method, guard):
    """run_composition and start_grouping cannot run in this harness, so their
    wiring is read: a slot connected bare still delivers a stopped run's rows
    after New. (The Search tab's, and the tag search's, are also driven, in
    test_exclusion_surfaces.)"""
    connects = [(sig, slot) for sig, slot in _connects(method) if sig in _WRITES_THE_TAB]
    assert connects, f"{method} connects none of these signals any more"
    bare = [(sig, slot) for sig, slot in connects if not slot.startswith(guard)]
    assert not bare, f"{method} connects these without the New guard: {bare}"
