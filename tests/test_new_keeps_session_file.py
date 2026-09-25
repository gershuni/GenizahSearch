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
import json
import os
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
