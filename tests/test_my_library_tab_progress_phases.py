# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-E (D-06 + D-07 + D-21) — Discovering files… status +
indeterminate progress bar phase transitions.

Pins the bilingual "Discovering files… / מאתר קבצים…" status, the
indeterminate→determinate progress-bar phase transition, and the
round-trip invariant that the bar returns to (0, 100) on finish/cancel/error.

  Test 1   test_status_updated_signal_exists
  Test 2   test_enumeration_status_emitted_before_first_progress
  Test 2b  test_enumeration_status_emitted_before_first_progress_with_500ms_delay
           (Codex Critique #3 R97.3-E coverage gap fix — ≥500ms wall-clock gap)
  Test 3   test_progress_bar_busy_during_enumeration
  Test 4   test_progress_bar_resets_to_100_on_finish
  Test 5   test_progress_bar_resets_to_100_on_error
  Test 6   test_progress_bar_resets_to_100_on_cancel
"""
from __future__ import annotations

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from PyQt6.QtCore import Qt
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


def _make_tab(tmp_path, monkeypatch):
    """Construct a real MyLibraryTab against an isolated tmp_path index."""
    from genizah_core import Config
    idx_dir = str(tmp_path / "local_index")
    lab_dir = str(tmp_path / "local_lab")
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", idx_dir, raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", lab_dir, raising=False)

    from desktop.my_library_tab import MyLibraryTab
    tab = MyLibraryTab(parent=None)
    return tab


def _close_tab(tab):
    try:
        if getattr(tab, "_indexer", None) is not None:
            try:
                tab._indexer._conn.close()
            except Exception:
                pass
        tab.deleteLater()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test 1 — signal exists
# ---------------------------------------------------------------------------

def test_status_updated_signal_exists(tmp_path, monkeypatch):
    """R97.3-E: LocalIndexerWorker exposes status_updated pyqtSignal(str)."""
    from desktop.my_library_tab import LocalIndexerWorker
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        worker = LocalIndexerWorker(tab._indexer)
        assert hasattr(worker, "status_updated"), (
            "R97.3-E: LocalIndexerWorker must define status_updated signal"
        )
        # pyqtBoundSignal instances expose .emit + .connect; checking attr names is enough.
        assert hasattr(worker.status_updated, "emit")
        assert hasattr(worker.status_updated, "connect")
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 2 — status fires before first progress
# ---------------------------------------------------------------------------

def test_enumeration_status_emitted_before_first_progress(tmp_path, monkeypatch):
    """D-07: status_updated('Discovering files… / מאתר קבצים…') fires BEFORE
    any progress_updated signal.
    """
    from desktop.my_library_tab import LocalIndexerWorker
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        (folder / "x.txt").write_text("x")
        tab._indexer.add_folder(str(folder))

        worker = LocalIndexerWorker(tab._indexer)
        order = []

        worker.status_updated.connect(
            lambda t: order.append(("status", t, time.monotonic())),
            Qt.ConnectionType.DirectConnection,
        )
        worker.progress_updated.connect(
            lambda c, t, f: order.append(("progress", c, t, f, time.monotonic())),
            Qt.ConnectionType.DirectConnection,
        )

        worker.start()
        # Wait for finished_signal
        finished = []
        worker.finished_signal.connect(
            lambda r: finished.append(r),
            Qt.ConnectionType.DirectConnection,
        )
        worker.wait(10_000)
        QApplication.processEvents()

        status_events = [e for e in order if e[0] == "status"]
        progress_events = [e for e in order if e[0] == "progress"]
        assert status_events, "status_updated must fire at least once"
        status_text = status_events[0][1]
        assert status_text == "Discovering files… / מאתר קבצים…", (
            f"D-07: status text must match verbatim; got {status_text!r}"
        )
        if progress_events:
            status_idx = order.index(status_events[0])
            progress_idx = order.index(progress_events[0])
            assert status_idx < progress_idx, (
                "D-07: status_updated must fire BEFORE first progress_updated"
            )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 2b — ≥500ms wall-clock gap (Codex Critique #3 R97.3-E coverage gap)
# ---------------------------------------------------------------------------

def test_enumeration_status_emitted_before_first_progress_with_500ms_delay(
    tmp_path, monkeypatch,
):
    """Codex Critique #3 R97.3-E coverage: status_updated must be visible to
    user during slow enumeration (≥500ms before first progress_updated).
    Monkeypatch scan_all to sleep BEFORE yielding first progress callback.
    """
    from desktop.my_library_tab import LocalIndexerWorker
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        (folder / "x.txt").write_text("x")
        tab._indexer.add_folder(str(folder))

        original_scan_all = tab._indexer.scan_all

        def _slow_scan(cancel_check=lambda: False):
            # Delay BEFORE yielding the first progress callback so the
            # "Discovering files…" message has time to render.
            time.sleep(0.6)
            # Manually invoke progress callback so the test can observe
            # ordering. Then return a minimal result dict.
            if tab._indexer._progress_cb is not None:
                tab._indexer._progress_cb(1, 1, "x.txt")
            return {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}

        monkeypatch.setattr(tab._indexer, "scan_all", _slow_scan)

        worker = LocalIndexerWorker(tab._indexer)
        status_ts = []
        progress_ts = []

        worker.status_updated.connect(
            lambda t: status_ts.append(time.monotonic()),
            Qt.ConnectionType.DirectConnection,
        )
        worker.progress_updated.connect(
            lambda c, t, f: progress_ts.append(time.monotonic()),
            Qt.ConnectionType.DirectConnection,
        )

        worker.start()
        worker.wait(10_000)
        QApplication.processEvents()

        assert status_ts, "status_updated must fire"
        assert progress_ts, "progress_updated must fire (slow scan triggered it)"
        gap = progress_ts[0] - status_ts[0]
        assert gap >= 0.5, (
            f"Codex Critique #3: gap between status_updated and first "
            f"progress_updated must be >= 0.5s; got {gap:.3f}s"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 3 — busy mode set in _start_worker
# ---------------------------------------------------------------------------

def test_progress_bar_busy_during_enumeration(tmp_path, monkeypatch):
    """D-06: _start_worker puts progress bar in busy (setRange(0,0)) mode."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        # Spy on setRange so we can capture all calls.
        calls = []
        original_setRange = tab._progress_bar.setRange

        def _spy(mn, mx):
            calls.append((mn, mx))
            return original_setRange(mn, mx)

        monkeypatch.setattr(tab._progress_bar, "setRange", _spy)

        # _start_worker with no folders — worker will exit fast.
        tab._start_worker(toast_on_complete=False)

        # Wait briefly for the worker to drain (or stop immediately because
        # no folders).
        if tab._worker is not None:
            tab._worker.wait(5_000)
            QApplication.processEvents()

        assert (0, 0) in calls, (
            f"D-06: _start_worker must call setRange(0, 0) for busy mode; got {calls}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 4 — finish resets to (0, 100)
# ---------------------------------------------------------------------------

def test_progress_bar_resets_to_100_on_finish(tmp_path, monkeypatch):
    """D-21: _on_worker_finished resets progress bar to (0, 100)."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        # Put the bar in busy mode first (simulates mid-scan).
        tab._progress_bar.setRange(0, 0)
        assert tab._progress_bar.maximum() == 0

        # Acquire mutex so _on_worker_finished's unlock balances.
        tab._indexer_mutex.tryLock()

        monkeypatch.setattr(tab, "_reload_all_local_indexes", lambda: None)
        monkeypatch.setattr(tab, "_maybe_rebuild_lab_if_stale", lambda: False)
        monkeypatch.setattr(tab, "_update_disk_indicator", lambda: None)

        tab._on_worker_finished(
            {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}, False,
        )
        assert tab._progress_bar.maximum() == 100, (
            f"D-21: after _on_worker_finished, maximum should be 100; "
            f"got {tab._progress_bar.maximum()}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 5 — error resets to (0, 100)
# ---------------------------------------------------------------------------

def test_progress_bar_resets_to_100_on_error(tmp_path, monkeypatch):
    """D-21: _on_worker_error resets progress bar to (0, 100)."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        tab._progress_bar.setRange(0, 0)
        assert tab._progress_bar.maximum() == 0

        # Acquire mutex so unlock balances.
        tab._indexer_mutex.tryLock()

        # Suppress the QMessageBox so the test doesn't block.
        from PyQt6 import QtWidgets
        monkeypatch.setattr(QtWidgets.QMessageBox, "warning", lambda *a, **kw: None)

        tab._on_worker_error("synthetic boom")
        assert tab._progress_bar.maximum() == 100, (
            f"D-21: after _on_worker_error, maximum should be 100; "
            f"got {tab._progress_bar.maximum()}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 6 — cancel-drain resets to (0, 100)
# ---------------------------------------------------------------------------

def test_progress_bar_resets_to_100_on_cancel(tmp_path, monkeypatch):
    """D-21: _on_cancel_finished_drain resets progress bar to (0, 100)."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        tab._progress_bar.setRange(0, 0)
        assert tab._progress_bar.maximum() == 0

        tab._on_cancel_finished_drain({})
        assert tab._progress_bar.maximum() == 100, (
            f"D-21: after _on_cancel_finished_drain, maximum should be 100; "
            f"got {tab._progress_bar.maximum()}"
        )
    finally:
        _close_tab(tab)
