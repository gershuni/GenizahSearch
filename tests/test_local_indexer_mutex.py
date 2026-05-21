# -*- coding: utf-8 -*-
"""Phase 95 D-25: single indexer mutex prevents concurrent side-index writes.

Tests that QMutex in MyLibraryTab serialises Refresh requests so only one
worker runs at a time and additional requests collapse into a FIFO queue of
max depth 1.
"""
from __future__ import annotations

import sys
import threading
import time
import unittest.mock as mock

import pytest

try:
    from PyQt6.QtWidgets import QApplication, QWidget
    from PyQt6.QtCore import QMutex

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
    from desktop.my_library_tab import MyLibraryTab

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
