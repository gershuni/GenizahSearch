"""The desktop's index rebuild as the user sees it in the main window (offscreen Qt, one
file): the line with the progress and the time left, what it says about searching, the
Search and Analyze buttons during an in-place build, and the swap at the end of a build
beside the live index (GenizahGUI._start_index_build and its slots; the bar is
desktop/index_rebuild_bar.py).
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from unittest.mock import patch  # noqa: E402

import pytest  # noqa: E402
from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication, QLabel, QMessageBox, QProgressBar, QPushButton, QWidget  # noqa: E402

import genizah_app  # noqa: E402
import genizah_core  # noqa: E402
from desktop.index_rebuild_bar import IndexRebuildBar, progress_line  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402
from shared.index_swap import BuildPlan  # noqa: E402

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    """The asserted sentences are English: tr() must not read this machine's language."""
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


class _Thread(QObject):
    """Stands in for IndexerThread: the same signals, started by hand."""
    progress_signal = pyqtSignal(int, int)
    phase_signal = pyqtSignal(str, int, int)
    finished_signal = pyqtSignal(int)
    cancelled_signal = pyqtSignal()
    error_signal = pyqtSignal(str)
    made = []

    def __init__(self, meta_mgr, *, beside=False):
        super().__init__()
        self.beside = beside
        self.started = False
        self.cancelled = False
        _Thread.made.append(self)

    def start(self):
        self.started = True

    def isRunning(self):
        return self.started

    def request_cancel(self):
        self.cancelled = True


class _Engine:
    def __init__(self):
        self.index = object()
        self.calls = []
        self._browse_map_cache = {'x': 1}

    def close_index(self):
        self.calls.append('close')
        self.index = None

    def reload_index(self):
        self.calls.append('reload')
        self.index = object()
        return True

    def browse_map_replaced(self):
        self.calls.append('browse_map_replaced')

    def index_format_status(self):
        return {'current': True}


@pytest.fixture(autouse=True)
def _no_modal_boxes_and_no_real_index(tmp_path):
    """Nothing here may block on a real message box (a path a mutation opens would hang the
    run offscreen) or touch the real index folder: the boxes answer at once, the swap helpers
    are stand-ins unless a test patches them itself, and INDEX_DIR is a temp folder."""
    with patch.object(QMessageBox, 'information'), patch.object(QMessageBox, 'critical'),             patch.object(QMessageBox, 'question', return_value=QMessageBox.StandardButton.No),             patch.object(genizah_app.Config, 'INDEX_DIR', str(tmp_path)),             patch.object(genizah_app.Config, 'BROWSE_MAP', str(tmp_path / "browse_map.pkl")),             patch.object(genizah_app, 'swap_in_staged_index'),             patch.object(genizah_app, 'remove_old_index'),             patch.object(genizah_app, 'remove_old_index_in_background'), \
            patch.object(genizah_app, 'discard_staged_index'):
        yield


@pytest.fixture
def window():
    _Thread.made.clear()
    w = GenizahGUI.__new__(GenizahGUI)
    QWidget.__init__(w)
    w.btn_search = QPushButton("Search", w)
    w.btn_comp_run = QPushButton("Analyze", w)
    w.index_rebuild_bar = IndexRebuildBar(w)
    w.index_progress = QProgressBar(w)
    w.status_label = QLabel(w)
    w.lbl_index_format = QLabel(w)
    w.searcher = _Engine()
    w.indexer = object()
    w.meta_mgr = object()
    w.is_searching = False
    with patch.object(genizah_app, 'IndexerThread', _Thread):
        yield w
    w.deleteLater()


def _started(window, mode):
    window._start_index_build(mode)
    return _Thread.made[-1]


# --- the line and the buttons ---------------------------------------------------------------

def test_a_build_beside_the_live_index_shows_the_line_and_leaves_search_enabled(window):
    th = _started(window, 'beside')
    assert th.started and th.beside
    bar = window.index_rebuild_bar
    assert bar.isVisibleTo(window)
    assert bar.lbl_msg.text() == ("Rebuilding the search index: 0%, estimating the time left. "
                                  "Searching uses the current index meanwhile.")
    assert window.btn_search.isEnabled() and window.btn_comp_run.isEnabled()
    assert window.searcher.calls == []                     # the open index was not closed


def test_an_in_place_build_closes_the_index_and_disables_search_with_the_reason(window):
    th = _started(window, 'inplace')
    assert not th.beside
    assert window.searcher.calls == ['close']
    assert window.index_rebuild_bar.lbl_msg.text().endswith("Searching is unavailable until the build ends.")
    for btn in (window.btn_search, window.btn_comp_run):
        assert not btn.isEnabled()
        assert btn.toolTip() == "Searching is unavailable until the build ends."


def test_the_line_follows_the_lines_read_and_then_the_later_phases(window):
    _started(window, 'beside')
    clock = window._index_build['clock']
    clock.started -= 60                                     # a minute in
    window.on_index_progress(2500, 10000)
    text = window.index_rebuild_bar.lbl_msg.text()
    assert text.startswith("Rebuilding the search index: 13%, about ")
    assert "minutes left. Searching uses the current index meanwhile." in text
    assert window.index_rebuild_bar.bar.value() == 13
    assert window.index_progress.format() == "2500/10000 lines"
    window.on_index_phase('commit', 0, 0)
    assert window.index_rebuild_bar.lbl_msg.text().startswith("Rebuilding the search index: 63%, writing it to disk, ")
    assert window.index_progress.format() == "Finishing the index... %p%"
    assert window.index_progress.value() == 63
    window.on_index_phase('finish', 1, 1)
    assert window.index_rebuild_bar.bar.value() == 100


def test_the_time_left_reads_as_minutes_or_less_than_a_minute():
    assert progress_line(37, 14, 'beside') == ("Rebuilding the search index: 37%, about 14 minutes left. "
                                               "Searching uses the current index meanwhile.")
    assert progress_line(99, 0, 'inplace') == ("Rebuilding the search index: 99%, less than a minute left. "
                                               "Searching is unavailable until the build ends.")
    assert "90%, about a minute left. " in progress_line(90, 1, 'beside')


# --- the end of a build ---------------------------------------------------------------------

def test_a_finished_build_beside_the_live_index_is_swapped_in_and_the_window_returns_to_normal(window, tmp_path):
    th = _started(window, 'beside')
    swaps = []
    with patch.object(genizah_app, 'swap_in_staged_index', side_effect=lambda d, b: swaps.append((d, b)) or 'old') as swap, \
            patch.object(genizah_app, 'remove_old_index_in_background') as remove, \
            patch.object(QMessageBox, 'information') as info:
        th.finished_signal.emit(42)
    assert swap.call_count == 1 and swaps[0][0] == genizah_app.Config.INDEX_DIR
    # Closed before the swap, reopened after, and the browse map (with the FL-id lookup
    # built from it) rebuilt: Codex round 2 on PR #446.
    assert window.searcher.calls == ['close', 'reload', 'browse_map_replaced']
    remove.assert_called_once_with(genizah_app.Config.INDEX_DIR)
    assert not window.index_rebuild_bar.isVisibleTo(window)
    assert window.btn_search.isEnabled() and window._index_build is None
    assert window.lbl_index_format.text() == "Search index: current format."
    assert info.call_count == 1 and "42" in info.call_args[0][2]


class _Running:
    def isRunning(self):
        return True


def _reader(window, kind):
    """Something that reads the open index: the main search, Composition, grouping, or
    the Join Workbench's two searches (desktop/join_workbench.py shares app.searcher)."""
    import types
    if kind == 'search':
        window.is_searching = True
    elif kind == 'composition':
        window.comp_thread = _Running()
    elif kind == 'grouping':
        window.group_thread = _Running()
    elif kind == 'join candidates':
        window._join_workbench = types.SimpleNamespace(_search_thread=_Running(), _cross_worker=None)
    elif kind == 'join cross-side':
        window._join_workbench = types.SimpleNamespace(_search_thread=None, _cross_worker=_Running())


@pytest.mark.parametrize('kind', ['search', 'composition', 'grouping', 'join candidates', 'join cross-side'])
def test_the_swap_waits_while_a_search_holds_the_index(window, kind):
    th = _started(window, 'beside')
    _reader(window, kind)
    assert window._index_readers_busy() == [kind]
    with patch.object(genizah_app, 'swap_in_staged_index') as swap, \
            patch.object(genizah_app.QTimer, 'singleShot') as later:
        th.finished_signal.emit(42)
    assert swap.call_count == 0 and later.call_count == 1
    assert window.searcher.calls == []
    assert window.index_rebuild_bar.lbl_msg.text() == (
        "The new search index is ready; it takes over when the current search ends.")
    assert window._index_build is not None


def test_a_build_cancelled_while_its_install_waited_installs_nothing(window):
    # Codex round 2 on PR #446: the retry fired after the build had been cancelled.
    th = _started(window, 'beside')
    window.is_searching = True
    retries = []
    with patch.object(genizah_app.QTimer, 'singleShot', side_effect=lambda ms, fn: retries.append(fn)):
        th.finished_signal.emit(42)
    assert len(retries) == 1
    window.on_index_cancelled()
    window.is_searching = False
    with patch.object(genizah_app, 'swap_in_staged_index') as swap:
        retries[0]()
    assert swap.call_count == 0 and window.searcher.calls == []


def test_a_finished_build_marked_cancelling_is_discarded_not_installed(window):
    th = _started(window, 'beside')
    window._index_build['cancelling'] = True
    with patch.object(genizah_app, 'swap_in_staged_index') as swap, \
            patch.object(genizah_app, 'discard_staged_index') as discard:
        th.finished_signal.emit(42)
    assert swap.call_count == 0 and discard.call_count == 1
    assert window._index_build is None and window.searcher.calls == []


def test_a_swap_that_fails_reopens_the_index_the_user_had(window):
    th = _started(window, 'beside')
    with patch.object(genizah_app, 'swap_in_staged_index', side_effect=OSError("locked")), \
            patch.object(QMessageBox, 'critical') as crit:
        th.finished_signal.emit(42)
    assert window.searcher.calls == ['close', 'reload']
    assert crit.call_count == 1 and "still in use" in crit.call_args[0][2]
    assert window.btn_search.isEnabled() and window._index_build is None


def test_a_finished_in_place_build_reloads_and_re_enables_search(window):
    th = _started(window, 'inplace')
    with patch.object(QMessageBox, 'information'):
        th.finished_signal.emit(7)
    assert window.searcher.calls == ['close', 'reload', 'browse_map_replaced']
    assert window.btn_search.isEnabled() and window.btn_search.toolTip() == ""
    assert not window.index_rebuild_bar.isVisibleTo(window)


def test_a_cancelled_or_failed_build_returns_the_window_to_normal(window):
    th = _started(window, 'inplace')
    th.cancelled_signal.emit()
    assert window.btn_search.isEnabled() and window._index_build is None
    assert window.status_label.text() == "Index rebuild cancelled. The index you had is unchanged."
    th = _started(window, 'beside')
    with patch.object(QMessageBox, 'critical'):
        th.error_signal.emit("boom")
    assert not window.index_rebuild_bar.isVisibleTo(window) and window._index_build is None


# --- the confirmation, by the free-space plan -----------------------------------------------

def test_the_confirmation_says_how_the_build_will_run():
    beside = GenizahGUI._index_build_question(BuildPlan('beside', 10, 20, 8))
    assert beside.startswith("Start indexing?") and "searching keeps working" in beside
    inplace = GenizahGUI._index_build_question(BuildPlan('inplace', 16 * 1024 ** 3, 3 * 1024 ** 3, 14 * 1024 ** 3))
    assert "about 16.0 GB needed, 3.0 GB free" in inplace and "unavailable until the build ends" in inplace
    assert GenizahGUI._index_build_question(BuildPlan('fresh', 0, 20, 0)) == "Start indexing?"


def test_run_indexing_starts_the_build_the_plan_names(window, tmp_path):
    corpus = tmp_path / "Transcriptions.txt"
    corpus.write_text("x", encoding="utf-8")
    with patch.object(genizah_app.Config, 'FILE_V8', str(corpus)), \
            patch.object(genizah_app, 'plan_build', return_value=BuildPlan('inplace', 2, 1, 1)), \
            patch.object(QMessageBox, 'question', return_value=QMessageBox.StandardButton.Yes):
        window.run_indexing()
    assert _Thread.made[-1].beside is False and not window.btn_search.isEnabled()
    with patch.object(genizah_app.Config, 'FILE_V8', str(corpus)), \
            patch.object(QMessageBox, 'information') as info:
        window.run_indexing()                              # a second click while it runs
    assert len(_Thread.made) == 1 and "already being rebuilt" in info.call_args[0][2]


# --- the thread and the startup recovery, as the app wires them -----------------------------

def test_the_indexer_thread_builds_beside_the_live_index_and_reports_the_phases(tmp_path):
    """The real IndexerThread on the fixture corpus: finished_signal with the page count,
    the phases through phase_signal, and request_cancel ending in cancelled_signal."""
    import inspect
    from desktop.gui_threads import IndexerThread
    from shared import index_swap
    from shared.config import Config
    from tests.responsa_index_fixture import FakeMeta, build, write_corpus

    root = build(tmp_path, [("9900000001", 1, 1, "אחד"), ("9900000001", 1, 2, "שניים")])
    v8, v7 = write_corpus(root, [("9900000001", 1, 1, "אחד"), ("9900000002", 2, 1, "שלושה")])
    got = {'finished': [], 'phases': [], 'cancelled': 0, 'errors': []}

    def run(thread):
        thread.finished_signal.connect(got['finished'].append)
        thread.phase_signal.connect(lambda n, d, t: got['phases'].append(n))
        thread.cancelled_signal.connect(lambda: got.__setitem__('cancelled', got['cancelled'] + 1))
        thread.error_signal.connect(got['errors'].append)
        with patch.object(Config, "INDEX_DIR", root), patch.object(Config, "BROWSE_MAP", os.path.join(root, "browse_map.pkl")),                 patch.object(Config, "FILE_V8", v8), patch.object(Config, "FILE_V7", v7):
            thread.start()
            assert thread.wait(60_000)
        _APP.processEvents()

    run(IndexerThread(FakeMeta(), beside=True))
    assert got['finished'] == [2] and got['errors'] == []
    assert got['phases'][0] == 'aggregates' and got['phases'][-1] == 'finish'
    assert os.path.isfile(os.path.join(index_swap.staged_index_path(root), "meta.json"))
    assert os.path.isfile(os.path.join(index_swap.live_index_path(root), "meta.json"))   # untouched

    thread = IndexerThread(FakeMeta(), beside=True)
    thread.request_cancel()
    run(thread)
    assert got['cancelled'] == 1 and got['finished'] == [2]
    assert not os.path.exists(index_swap.staged_index_path(root))

    # The startup thread sets the index folders right before the engine opens the index.
    from desktop.gui_threads import StartupThread
    src = inspect.getsource(StartupThread.run)
    assert src.index("recover_index_dirs(Config.INDEX_DIR, Config.BROWSE_MAP)") < src.index("SearchEngine(meta_mgr, var_mgr)")
    assert "delete_leftovers_in_background(Config.INDEX_DIR)" in src

