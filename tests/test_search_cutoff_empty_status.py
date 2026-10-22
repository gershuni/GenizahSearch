# -*- coding: utf-8 -*-
"""Search tab: a cut-off search that found nothing says "Partial results".

A search whose candidate list was cut at the limit (the engine's cut-off signal,
D8) and whose checked candidates held no match has not shown that nothing
matches: a Regex pattern with no prefilter reads only the first 50,000 of
~2.2M pages. The status line must then say what a stopped run says,
"No results found. (Partial results)", never a bare "No results found.".

The signal is in place when the results arrive: SearchThread emits
cutoff_signal just before results_signal (pinned in
tests/test_search_cutoff_signal.py), and start_search clears
_search_cutoff before the run.

Pattern: the unbound GenizahGUI.on_search_finished bound to a stub -- no
QApplication (as in tests/test_local_scope_zero_result_hint.py).
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import genizah_app   # noqa: E402
import genizah_core  # noqa: E402

APP = genizah_app.GenizahGUI


class _Widget:
    """Any widget the zero-result branch touches: every call is a no-op."""

    def __getattr__(self, name):
        return lambda *a, **k: None


class _Label:
    def __init__(self):
        self.text = None

    def setText(self, text):
        self.text = text


class _Host:
    on_search_finished = APP.on_search_finished
    _run_left_matches_out = APP._run_left_matches_out
    _forget_responsa_notes = APP._forget_responsa_notes
    _on_responsa_downgrade = APP._on_responsa_downgrade
    _downgrade_note = APP._downgrade_note

    def __init__(self, cutoff, cancelled=False):
        self._search_cutoff = cutoff
        self._search_was_cancelled = cancelled
        self._refine_mode = False
        self._pause_search = SimpleNamespace(elapsed=lambda now: 1.0)
        self.status_label = _Label()
        self.search_progress = _Widget()
        self.chk_search_header = _Widget()
        self.lbl_search_export = _Widget()
        self.results_table = _Widget()
        self.btn_domain_filter = _Widget()
        self.export_buttons = [_Widget()]
        self.telemetry = []

    def reset_ui(self):
        pass

    def _update_local_scope_strip(self, result_count, cancelled=False):
        return False

    def _update_load_more_button(self):
        pass

    def _update_search_within_btn(self):
        pass

    def _emit_search_telemetry(self, action, result_count=None):
        self.telemetry.append((action, result_count))


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    # tr() reads genizah_core.CURRENT_LANG; this machine may run the app in Hebrew.
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", request.param, raising=False)
    monkeypatch.setattr(genizah_app, "QApplication",
                        SimpleNamespace(processEvents=lambda: None))
    return request.param


def _partial():
    tr = genizah_app.tr
    return f"{tr('No results found.')} ({tr('Partial results')})"


def _finish(cutoff, cancelled=False):
    host = _Host(cutoff, cancelled)
    host.on_search_finished([])
    return host


def test_a_cut_off_search_with_no_match_says_partial(lang):
    host = _finish({"capped": True, "interrupted": False})
    assert host.status_label.text == _partial()
    assert host.status_label.text != genizah_app.tr("No results found.")
    if lang == "en":
        assert host.status_label.text == "No results found. (Partial results)"


def test_a_complete_search_with_no_match_says_no_results(lang):
    for cutoff in ({"capped": False, "interrupted": False}, None):
        host = _finish(cutoff)
        assert host.status_label.text == genizah_app.tr("No results found.")
        assert host.telemetry == [("completed", 0)]


def test_a_stopped_search_still_says_partial(lang):
    host = _finish({"capped": False, "interrupted": True}, cancelled=True)
    assert host.status_label.text == _partial()
    assert host.telemetry == [("cancelled", 0)]


def test_both_strings_are_translated():
    from shared.genizah_translations import TRANSLATIONS
    assert "No results found." in TRANSLATIONS and "Partial results" in TRANSLATIONS


def test_a_cut_off_search_within_results_with_no_match_says_partial(lang):
    """Inside a refinement the zero-result line is "0 results within current scope";
    a cut-off run must not lose its "(Partial results)" there either."""
    tr = genizah_app.tr
    host = _Host({"capped": True, "interrupted": False})
    host._refine_mode = True
    host.on_search_finished([])
    assert host.status_label.text == f"{tr('0 results within current scope')} ({tr('Partial results')})"
    complete = _Host({"capped": False, "interrupted": False})
    complete._refine_mode = True
    complete.on_search_finished([])
    assert complete.status_label.text == tr('0 results within current scope')


# --- A downgraded Responsa query that matched nothing (Codex review of PR #433) -----

NOTE = "Spelling variants disabled"


def test_a_downgraded_search_with_no_match_says_what_was_turned_off(lang):
    """With no rows there is no first row to carry the guard's downgrade: it comes on
    the thread's own signal, and the zero-result line keeps it."""
    tr = genizah_app.tr
    host = _Host({"capped": False, "interrupted": False})
    host._on_responsa_downgrade(NOTE)
    host.on_search_finished([])
    assert host.status_label.text == f"{tr('No results found.')} · {NOTE}"
    within = _Host({"capped": False, "interrupted": False})
    within._refine_mode = True
    within._on_responsa_downgrade(NOTE)
    within.on_search_finished([])
    assert within.status_label.text == f"{tr('0 results within current scope')} · {NOTE}"


def test_the_search_thread_sends_its_downgrade():
    """The engine records the downgrade on the thread that searched; the thread sends
    it on before the rows, with none for a search without one."""
    from desktop.gui_threads import SearchThread
    from shared.search_engine import _set_last_responsa_downgrade

    class Fake:
        def __init__(self, note):
            self.note = note

        def execute_search(self, *a, **kw):
            if self.note:
                _set_last_responsa_downgrade(self.note)
            return []

    for note in (NOTE, None):
        t = SearchThread(Fake(note), "q", "literal", 0)
        got = []
        t.downgrade_signal.connect(lambda n: got.append(('downgrade', n)))
        t.results_signal.connect(lambda rows: got.append(('rows', rows)))
        t.run()                   # same thread: the signals are delivered directly
        assert got == [('downgrade', note), ('rows', [])]
