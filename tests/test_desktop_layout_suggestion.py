# -*- coding: utf-8 -*-
"""The desktop Search tab's "Did you mean <Hebrew>?" (2026-11-04), as the website's
/search offers it (PR #469).

Pinned here, with the window's real methods over a real QLabel: after a text
search that found nothing, a query typed with the keyboard on the English layout
(``dcuk``) is offered back as Hebrew (גבול) -- only when the engine's
``words_occur`` says the corpus has the words, asked off the UI thread; not for a
search that was not a text search, nor once another search has started or the
search was reset; and the Hebrew, clicked, runs as if typed. In both languages:
the Hebrew sentence glues the preposition to the word.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QLabel, QLineEdit, QWidget

import genizah_core
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


class _Searcher:
    def __init__(self, known):
        self.known = known
        self.asked = []

    def words_occur(self, query):
        self.asked.append(query)
        return query in self.known


class _Window(QWidget):
    _offer_layout_suggestion = GenizahGUI._offer_layout_suggestion
    _on_layout_suggestion_checked = GenizahGUI._on_layout_suggestion_checked
    _show_layout_suggestion = GenizahGUI._show_layout_suggestion
    _hide_layout_suggestion = GenizahGUI._hide_layout_suggestion
    _take_layout_suggestion = GenizahGUI._take_layout_suggestion
    _layout_suggestion_worker_done = GenizahGUI._layout_suggestion_worker_done

    def __init__(self, known=("גבול",)):
        super().__init__()
        self.searcher = _Searcher(set(known))
        self.query_input = QLineEdit(self)
        self.layout_suggestion_label = QLabel(self)
        self.layout_suggestion_label.setTextFormat(Qt.TextFormat.RichText)
        self.layout_suggestion_label.setVisible(False)
        self._layout_suggestion = None
        self._run_seq = 0
        self.is_searching = False
        self.searches = []

    def finished_zero(self, query, text_search=True):
        """What start_search and on_search_finished do for a run that found nothing."""
        self._run_seq += 1
        self._layout_offer = (self._run_seq, query) if text_search else None
        self._offer_layout_suggestion()
        worker = getattr(self, '_layout_suggestion_worker', None)
        if worker is not None:
            assert worker.wait(5000)
        QApplication.processEvents()      # the worker's queued signals
        self.last_worker_gone = getattr(self, '_layout_suggestion_worker', None) is None

    def toggle_search(self):
        self.searches.append(self.query_input.text())


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    return request.param


def test_a_wrong_layout_query_is_offered_as_hebrew(lang):
    w = _Window()
    w.finished_zero("dcuk")
    assert w.searcher.asked == ["גבול"]
    assert not w.layout_suggestion_label.isHidden()
    text = w.layout_suggestion_label.text()
    assert '<a href="layout">גבול</a>' in text
    if lang == "he":
        assert text == 'האם התכוונתם ל<a href="layout">גבול</a>?'
    else:
        assert text == 'Did you mean <a href="layout">גבול</a>?'


def test_the_worker_is_an_index_reader_only_while_it_runs():
    """_index_readers_busy reads _layout_suggestion_worker: it is dropped when
    the thread ends, so the install never waits on, or touches, a deleted one."""
    w = _Window()
    w.finished_zero("dcuk")
    assert w.last_worker_gone


def test_the_hebrew_clicked_runs_it_as_if_typed(lang):
    w = _Window()
    w.finished_zero("dcuk")
    w.layout_suggestion_label.linkActivated.connect(w._take_layout_suggestion)
    w.layout_suggestion_label.linkActivated.emit("layout")
    assert w.query_input.text() == "גבול"
    assert w.searches == ["גבול"]
    assert w.layout_suggestion_label.isHidden()


def test_nothing_offered_when_the_corpus_lacks_the_words(lang):
    w = _Window(known=())
    w.finished_zero("dcuk")
    assert w.searcher.asked == ["גבול"]
    assert w.layout_suggestion_label.isHidden()


@pytest.mark.parametrize("query", ["ts 20.57", "hello123", "גבול", "q"])
def test_nothing_asked_when_the_query_spells_no_hebrew(query):
    w = _Window()
    w.finished_zero(query)
    assert w.searcher.asked == []
    assert w.layout_suggestion_label.isHidden()


def test_nothing_offered_after_a_search_that_was_not_a_text_search():
    w = _Window()
    w.finished_zero("dcuk", text_search=False)
    assert w.searcher.asked == []
    assert w.layout_suggestion_label.isHidden()


def test_an_answer_for_an_older_search_is_dropped():
    w = _Window()
    w._run_seq = 5
    w._on_layout_suggestion_checked(4, "גבול", True)       # a newer run started
    assert w.layout_suggestion_label.isHidden()
    w.is_searching = True
    w._on_layout_suggestion_checked(5, "גבול", True)       # running now
    assert w.layout_suggestion_label.isHidden()
    w.is_searching = False
    w._layout_cleared_run = 5                                # New Search since
    w._on_layout_suggestion_checked(5, "גבול", True)
    assert w.layout_suggestion_label.isHidden()


def test_no_check_without_the_engines_words_occur():
    w = _Window()
    w.searcher = object()                                    # an engine without the check
    w.finished_zero("dcuk")
    assert w.layout_suggestion_label.isHidden()


class _ZeroResultHost:
    """The real on_search_finished and reset_ui over stand-in widgets (as
    tests/test_search_cutoff_empty_status.py drives them): any other attribute
    is a no-op callable."""

    on_search_finished = GenizahGUI.on_search_finished
    reset_ui = GenizahGUI.reset_ui
    _run_left_matches_out = GenizahGUI._run_left_matches_out
    _forget_responsa_notes = GenizahGUI._forget_responsa_notes
    _downgrade_note = GenizahGUI._downgrade_note

    def __init__(self, cancelled=False):
        from types import SimpleNamespace
        self._layout_offer = (1, "dcuk")
        self._search_was_cancelled = cancelled
        self._search_cutoff = None
        self._refine_mode = False
        self._restoring_session = False
        self._pause_search = SimpleNamespace(elapsed=lambda now: 1.0, state='running')
        self.export_buttons = []
        self.offered = []

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return _NoOp()

    def _offer_layout_suggestion(self):
        self.offered.append(self._layout_offer)


class _NoOp:
    def __call__(self, *a, **k):
        return None

    def __getattr__(self, name):
        return _NoOp()


def test_a_completed_zero_result_search_reaches_the_offer():
    """reset_ui -- which every exit of a search passes through, the zero-result
    one first -- leaves the pending offer alone: only New (_reset_search) and
    the next start_search clear it (Codex on #479 read it the other way)."""
    host = _ZeroResultHost()
    host.on_search_finished([])
    assert host.offered == [(1, "dcuk")]


def test_a_stopped_search_with_no_results_offers_nothing():
    host = _ZeroResultHost(cancelled=True)
    host.on_search_finished([])
    assert host.offered == []


def test_the_line_sits_in_the_results_box():
    """Where the rows would be (owner, 2026-11-04): in the results table's
    viewport, under the column headers, not in the status row."""
    from PyQt6.QtWidgets import QTableWidget

    class _Builder(QWidget):
        _install_layout_suggestion = GenizahGUI._install_layout_suggestion
        _take_layout_suggestion = GenizahGUI._take_layout_suggestion

    host = _Builder()
    table = QTableWidget(0, 3, host)
    label = host._install_layout_suggestion(table)
    assert label.parentWidget() is table.viewport()
    assert label.isHidden()
    host.resize(800, 400)
    table.resize(800, 400)
    host.show()
    label.setText("Did you mean x?")
    label.setVisible(True)
    QApplication.processEvents()
    assert label.isVisible()
    assert label.geometry().top() == 0 and label.width() > 0


def test_the_window_builds_the_label_and_resets_it():
    """The real window wires the label (hidden until offered) and its reset."""
    import inspect
    src = inspect.getsource(GenizahGUI._reset_search)
    assert "_hide_layout_suggestion(self)" in src and "_layout_cleared_run" in src
    finished = inspect.getsource(GenizahGUI.on_search_finished)
    assert "self._offer_layout_suggestion()" in finished
