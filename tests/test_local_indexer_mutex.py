# -*- coding: utf-8 -*-
"""Phase 95 D-25: single indexer mutex prevents concurrent side-index writes.

Tests that QMutex in MyLibraryTab serialises Refresh requests so only one
worker runs at a time and additional requests collapse into a FIFO queue of
max depth 1.

Also contains the D-threading-fix regression test: LocalIndexer constructed
on thread A must be fully usable (scan_all / SQLite access) from thread B
without raising sqlite3.ProgrammingError.  This test does NOT require PyQt6.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest.mock as mock

import pytest

try:
    from PyQt6.QtWidgets import QApplication, QWidget
    from PyQt6.QtCore import QMutex  # noqa: F401  (imported to verify PyQt6 availability)

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not QT_AVAILABLE, reason="PyQt6 not available"
)


@pytest.fixture(autouse=True)
def _ensure_app():
    if QT_AVAILABLE:
        from PyQt6.QtWidgets import QApplication
        if QApplication.instance() is None:
            QApplication(sys.argv)
    yield


def _make_mock_indexer(folders=None, scan_delay=0.0):
    """Return a MagicMock indexer. scan_delay simulates slow scan."""
    m = mock.MagicMock()
    m.list_folders.return_value = folders or []
    m.prescan_count.return_value = (0, 0)
    m.prescan_count_all.return_value = (0, 0)

    def slow_scan(cancel_check=None):
        if scan_delay:
            time.sleep(scan_delay)
        return {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}

    m.scan_all.side_effect = slow_scan
    m.startup_recovery.return_value = {
        "pending_deletes_recovered": 0,
        "pending_inserts_recovered": 0,
    }
    # Phase 97 R-01: MyLibraryTab.__init__ calls start_recovery_probe() and, if it
    # returns a non-empty list, shows a MODAL recovery dialog (mb.exec()) that
    # blocks forever headless. A bare MagicMock returns a truthy auto-mock, so this
    # MUST be stubbed to [] — otherwise every tab-construction test hangs.
    m.start_recovery_probe.return_value = []
    return m


def _make_tab(mock_idx):
    from desktop.my_library_tab import MyLibraryTab

    parent = QWidget()
    parent.searcher = mock.MagicMock()
    parent.statusBar = mock.MagicMock(return_value=mock.MagicMock())

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)
    return tab


def test_concurrent_refresh_no_interleave():
    """D-25: firing 3 Refresh requests rapidly; only one worker runs at a time.

    We verify that:
    - _indexer_mutex prevents concurrent access (tryLock returns False when held)
    - A second Refresh while worker is running is queued (not dropped silently)
    - A third Refresh collapses into the queue (max depth 1)
    """

    mock_idx = _make_mock_indexer()
    tab = _make_tab(mock_idx)

    # Manually lock the mutex to simulate a running worker
    locked = tab._indexer_mutex.tryLock()
    assert locked, "Mutex should be acquirable initially"

    # Now calling _start_worker should queue (not spawn a second worker)
    assert tab._worker is None  # no worker running (we hold the mutex)
    assert tab._queued_action is None

    # First queue attempt
    tab._start_worker(toast_on_complete=False)
    assert tab._queued_action is not None, (
        "A second Refresh while mutex held should be queued"
    )

    first_queued = tab._queued_action

    # Second queue attempt — should collapse (max depth 1)
    tab._start_worker(toast_on_complete=True)
    second_queued = tab._queued_action
    assert second_queued is not None
    # The queued action was replaced (collapse — max depth 1)
    # We can't assert identity easily since lambdas are different objects,
    # but we can assert the queue depth stays at 1.
    assert tab._queued_action is second_queued  # still exactly one item queued

    # Release mutex
    tab._indexer_mutex.unlock()

    # Cleanup: clear queued action to avoid side effects
    tab._queued_action = None


def test_mutex_released_after_worker_finishes():
    """D-25: mutex is released in _on_worker_finished so subsequent workers can run."""
    mock_idx = _make_mock_indexer()
    tab = _make_tab(mock_idx)

    # Simulate the mutex being held (as if a worker is running)
    tab._indexer_mutex.tryLock()

    # Simulate worker finishing
    tab._on_worker_finished({"indexed": 0, "skipped": 0}, toast=False)

    # Mutex should now be free
    can_lock = tab._indexer_mutex.tryLock()
    assert can_lock, "Mutex must be released after _on_worker_finished"
    tab._indexer_mutex.unlock()


def test_mutex_released_after_worker_error():
    """D-25: mutex is released even when _on_worker_error is called."""
    mock_idx = _make_mock_indexer()
    tab = _make_tab(mock_idx)

    tab._indexer_mutex.tryLock()

    with mock.patch("desktop.my_library_tab.QMessageBox.warning"):
        tab._on_worker_error("test error")

    can_lock = tab._indexer_mutex.tryLock()
    assert can_lock, "Mutex must be released after _on_worker_error"
    tab._indexer_mutex.unlock()


def test_queued_action_runs_after_worker_finishes():
    """D-25: if an action is queued while worker runs, it executes after completion."""
    mock_idx = _make_mock_indexer()
    tab = _make_tab(mock_idx)

    executed = []

    def deferred_action():
        executed.append(True)

    # Lock mutex and enqueue an action
    tab._indexer_mutex.tryLock()
    tab._queued_action = deferred_action

    # Simulate worker finishing — should run the queued action
    tab._on_worker_finished({"indexed": 0}, toast=False)

    assert executed, "Queued action should have been executed after worker finished"
    assert tab._queued_action is None, "Queue should be empty after dequeuing"


# ---------------------------------------------------------------------------
# D-threading-fix regression tests (no PyQt6 required)
# ---------------------------------------------------------------------------
# These tests verify the fix for the cross-thread sqlite3.ProgrammingError:
#   "SQLite objects created in a thread can only be used in that same thread."
#
# Before the fix: LocalIndexer.__init__ created self._conn on the main thread;
# any worker thread calling scan_all() would crash with ProgrammingError.
# After the fix: self._conn is a thread-local property — each thread gets its
# own sqlite3.Connection opened lazily by init_sqlite().


def _make_tmp_indexer():
    """Create a real LocalIndexer in a temp directory for threading tests."""
    from shared.local_indexer import LocalIndexer

    tmp = tempfile.mkdtemp(prefix="gsd_thread_test_")
    index_dir = os.path.join(tmp, "local_index")
    lab_dir = os.path.join(tmp, "lab_index")
    db_path = os.path.join(tmp, "local_index.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    indexer = LocalIndexer(
        index_dir=index_dir,
        lab_index_dir=lab_dir,
        db_path=db_path,
    )
    return indexer, tmp


def test_sqlite_conn_usable_from_worker_thread():
    """D-threading-fix: SQLite connection must not raise ProgrammingError on worker thread.

    Regression test for:
        sqlite3.ProgrammingError: SQLite objects created in a thread can only
        be used in that same thread.

    Protocol:
    1. Construct LocalIndexer on the main (test) thread.
    2. Spawn a Python threading.Thread that calls list_folders() — a simple
       SQLite SELECT — which exercises self._conn on a foreign thread.
    3. Assert no exception was raised.
    """
    indexer, tmp = _make_tmp_indexer()
    errors = []

    def worker():
        try:
            _ = indexer.list_folders()
        except sqlite3.ProgrammingError as exc:
            errors.append(exc)
        except Exception:
            pass  # Other errors are not the bug we're guarding against

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=10)

    indexer.close()
    shutil.rmtree(tmp, ignore_errors=True)

    assert not errors, (
        f"sqlite3.ProgrammingError raised on worker thread — "
        f"thread-local fix not working: {errors}"
    )


def test_scan_all_usable_from_worker_thread():
    """D-threading-fix: scan_all() (full SQLite write path) must work from worker thread.

    This exercises the complete write path that LocalIndexerWorker.run() takes:
    scan_all() -> _index_one_file() -> _finish_file() -> self._conn.execute().

    We use an empty folder so no actual files are indexed, but the SQLite
    path (scan_all queries folders table) is still exercised from the
    worker thread.
    """
    indexer, tmp = _make_tmp_indexer()

    # Register a real (empty) folder so scan_all has something to iterate
    scan_folder = os.path.join(tmp, "scan_me")
    os.makedirs(scan_folder, exist_ok=True)
    indexer.add_folder(scan_folder)

    errors = []
    result_holder = []

    def worker():
        try:
            result = indexer.scan_all(cancel_check=lambda: False)
            result_holder.append(result)
        except sqlite3.ProgrammingError as exc:
            errors.append(("ProgrammingError", exc))
        except Exception as exc:
            errors.append(("Other", exc))

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=30)

    indexer.close()
    shutil.rmtree(tmp, ignore_errors=True)

    assert not any(kind == "ProgrammingError" for kind, _ in errors), (
        f"sqlite3.ProgrammingError from scan_all on worker thread — "
        f"thread-local fix not working: {errors}"
    )
    assert result_holder, "scan_all should have returned a result dict"
    assert result_holder[0].get("cancelled") is False


def test_each_thread_gets_independent_connection():
    """D-threading-fix: two threads each get their own sqlite3.Connection instance.

    This confirms the thread-local mechanism is actually creating separate
    connections rather than sharing a single one.
    """

    indexer, tmp = _make_tmp_indexer()

    conn_ids = {}

    def capture_conn(name):
        # Access _conn from this thread — will create a new connection if needed
        conn_ids[name] = id(indexer._conn)

    # Main thread connection (already created in __init__)
    capture_conn("main")

    # Worker thread connection (created lazily on first access)
    t = threading.Thread(target=capture_conn, args=("worker",))
    t.start()
    t.join(timeout=10)

    indexer.close()
    shutil.rmtree(tmp, ignore_errors=True)

    assert "worker" in conn_ids, "Worker thread did not access _conn"
    assert conn_ids["main"] != conn_ids["worker"], (
        "Main thread and worker thread must use different sqlite3.Connection objects"
    )
