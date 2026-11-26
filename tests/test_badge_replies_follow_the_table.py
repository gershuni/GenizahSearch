# -*- coding: utf-8 -*-
"""GS-11: a badge worker's reply is shown only on the result set it was started for.

The PGP, printed and domain workers are started after a text search
(`_launch_enrichment_workers`, deferred 500 ms on a session restore). Before
this fix their replies went straight to the slots, so a reply that arrived
after a PGP-tag search had replaced the table overwrote the tag search's PGP
set and blanked its PGP column.

The table now carries `_results_table_generation`, advanced whenever its
result set is replaced. It is apart from `_search_new_generation`: a tag
search must not advance that one, or a text search still running would lose
its results and leave the tab "searching".

The tests drive the real GenizahGUI methods on a window built without __init__.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtWidgets import QApplication, QMainWindow, QTableWidget  # noqa: E402

import desktop.gui_threads as gui_threads  # noqa: E402
import genizah_app  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402

_APP = QApplication.instance() or QApplication([])

RESULTS = [{'display': {'id': sid}} for sid in ('990001', '990002')]


class _Signal:
    def __init__(self):
        self.slots = []

    def connect(self, slot):
        self.slots.append(slot)

    def emit(self, *args):
        for slot in list(self.slots):
            slot(*args)


class _Worker:
    """A badge worker that never runs: the test emits its reply."""
    made = []

    def __init__(self, *args, **kwargs):
        self.finished = _Signal()
        _Worker.made.append(self)

    def start(self):
        pass

    def isRunning(self):
        return False


@pytest.fixture
def workers(monkeypatch):
    _Worker.made = []
    monkeypatch.setattr(gui_threads, 'DomainEnrichmentWorker', _Worker)
    monkeypatch.setattr(genizah_app, 'PGPBadgeWorker', _Worker)
    monkeypatch.setattr(genizah_app, 'PrintedBadgeWorker', _Worker)
    return _Worker.made


def _window():
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    w._results_table_generation = 0
    w._search_new_generation = 0
    w._domain_worker = w._pgp_badge_worker = w._printed_badge_worker = None
    w._post_criteria = None
    w._recompute_post_criteria = lambda: None
    w._apply_results_table_filters = lambda: None
    w.btn_measurement_filter = MagicMock()
    w.delivered = []
    w._on_domain_enrichment_loaded = lambda *a: w.delivered.append(('domain', a))
    w._on_pgp_badges_loaded = lambda *a: w.delivered.append(('pgp', a))
    w._on_printed_badges_loaded = lambda *a: w.delivered.append(('printed', a))
    return w


def _replies(made):
    domain, pgp, printed = made
    domain.finished.emit({'990001': []})
    pgp.finished.emit({'990001'}, {'990001'}, {})
    printed.finished.emit({'990002'})


def test_a_reply_for_the_table_still_shown_is_delivered(workers):
    w = _window()
    w._launch_enrichment_workers(RESULTS, defer=False)
    assert len(workers) == 3
    _replies(workers)
    assert [kind for kind, _ in w.delivered] == ['domain', 'pgp', 'printed']


def test_a_reply_after_the_table_was_replaced_is_dropped(workers):
    w = _window()
    w._launch_enrichment_workers(RESULTS, defer=False)
    w._results_table_generation += 1          # e.g. a PGP-tag search's results
    _replies(workers)
    assert w.delivered == [], "a stale badge reply still wrote the table"


def test_a_deferred_start_after_the_table_was_replaced_starts_nothing(workers, monkeypatch):
    """The generation is taken when the launch is asked for, not when the
    deferred start runs (a session restore defers it 500 ms)."""
    pending = []
    monkeypatch.setattr(genizah_app, 'QTimer', SimpleNamespace(
        singleShot=lambda ms, fn: pending.append(fn)))
    w = _window()
    w._launch_enrichment_workers(RESULTS, defer=True)
    assert workers == [] and len(pending) == 1
    w._results_table_generation += 1
    pending.pop()()
    assert workers == [], "a deferred start ran for a table that was replaced"


def test_a_deferred_start_for_the_table_still_shown_runs(workers, monkeypatch):
    pending = []
    monkeypatch.setattr(genizah_app, 'QTimer', SimpleNamespace(
        singleShot=lambda ms, fn: pending.append(fn)))
    w = _window()
    w._launch_enrichment_workers(RESULTS, defer=True)
    pending.pop()()
    _replies(workers)
    assert [kind for kind, _ in w.delivered] == ['domain', 'pgp', 'printed']


# -- the call sites that replace the result set advance the generation ----------

def _table_window():
    w = _window()
    w.results_table = QTableWidget(3, 3)
    w.status_label = MagicMock()
    w.export_buttons = []
    return w


def test_tag_results_advance_the_table_generation_not_the_search_one(workers):
    w = _table_window()
    w._set_local_scope_strip_visible = lambda visible: None
    w._forget_responsa_notes = lambda: None
    w._emit_pgp_tag_search_telemetry = lambda *a, **k: None
    w._update_load_more_button = lambda: None
    w._launch_enrichment_workers(RESULTS, defer=False)   # a text search's badges
    w._on_tag_search_results('Bible', [], token=1)
    assert w._results_table_generation == 1
    assert w._search_new_generation == 0, (
        "a tag search advanced the text search's generation: a run still going would be dropped")
    _replies(workers)
    assert w.delivered == []


def test_an_error_advances_the_table_generation(workers, monkeypatch):
    shown = []
    # on_error ends with a modal box: a real one would wait for a click forever.
    monkeypatch.setattr(genizah_app, 'QMessageBox', SimpleNamespace(
        critical=lambda *a, **k: shown.append(a)))
    w = _table_window()
    for name in ('reset_ui', '_forget_responsa_notes', '_update_load_more_button',
                 '_update_local_scope_strip', '_update_search_within_btn'):
        setattr(w, name, lambda *a, **k: None)
    w._launch_enrichment_workers(RESULTS, defer=False)
    w.on_error('the index could not be read')
    assert len(shown) == 1
    assert w._results_table_generation == 1
    _replies(workers)
    assert w.delivered == []
