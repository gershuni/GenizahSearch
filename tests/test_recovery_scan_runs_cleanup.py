# -*- coding: utf-8 -*-
"""Recovery-modal recurrence fix (2026-05-27).

The "התאוששות מאינדוקס שהופסק" / "Recover interrupted indexing" modal kept
reappearing on every launch because:

  1. `_show_recovery_modal` resolved only `running_runs[0]` — leftover orphan
     'running' rows survived and re-triggered the probe next launch.
  2. The LD-6 clean-shutdown sweep lived in `MyLibraryTab.closeEvent`, but a
     child widget never receives closeEvent on app exit, so it was dead code and
     orphan rows accumulated across hard kills.

Tests:
  - sweep_running_scan_runs marks ALL running rows completed (behavioral).
  - GenizahGUI.closeEvent wires sweep_running_scan_runs (source guard, Fix 2).
  - _show_recovery_modal resolves every running run, not just [0] (source guard,
    Fix 1).
"""
from __future__ import annotations

import inspect
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

try:
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


def _make_tab(tmp_path, monkeypatch):
    """Construct a real MyLibraryTab against an isolated tmp_path index.

    Clean tmp_path => no orphan scan_runs => recovery modal never shows
    (mb.exec() would block the test), mirroring test_my_library_tab_*.
    """
    from genizah_core import Config
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", str(tmp_path / "local_index"), raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "local_lab"), raising=False)
    from desktop.my_library_tab import MyLibraryTab
    return MyLibraryTab(parent=None)


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


def test_sweep_marks_all_running_runs_completed(tmp_path, monkeypatch):
    """sweep_running_scan_runs() must clear EVERY orphan 'running' row, so the
    recovery modal does not reappear on next launch."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert tab._indexer is not None
        conn = tab._indexer._conn
        for i in range(3):
            conn.execute(
                "INSERT INTO scan_runs (scan_run_id, started_at, status) "
                "VALUES (?, ?, 'running')",
                (f"orphan_{i}", time.time()),
            )
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM scan_runs WHERE status='running'"
        ).fetchone()[0] == 3

        tab.sweep_running_scan_runs()

        assert conn.execute(
            "SELECT COUNT(*) FROM scan_runs WHERE status='running'"
        ).fetchone()[0] == 0, "sweep must leave zero 'running' rows"
    finally:
        _close_tab(tab)


def test_parent_closeevent_wires_sweep():
    """GenizahGUI.closeEvent must call sweep_running_scan_runs — MyLibraryTab is a
    child widget and never receives its own closeEvent on app exit (Fix 2)."""
    src = inspect.getsource(
        __import__("genizah_app", fromlist=["GenizahGUI"]).GenizahGUI.closeEvent
    )
    assert "sweep_running_scan_runs" in src, (
        "GenizahGUI.closeEvent must call my_library_tab.sweep_running_scan_runs() "
        "so orphan 'running' scan_runs are cleared on clean shutdown."
    )


def test_recovery_modal_resolves_all_running_runs():
    """_show_recovery_modal must iterate running_runs, not index [0] (Fix 1)."""
    from desktop.my_library_tab import MyLibraryTab
    src = inspect.getsource(MyLibraryTab._show_recovery_modal)
    assert "for run_id in running_runs" in src, (
        "_show_recovery_modal must resolve EVERY running run (loop over "
        "running_runs), else orphan rows survive and the modal reappears."
    )
    assert "running_runs[0]" not in src, (
        "_show_recovery_modal must no longer resolve only running_runs[0]."
    )
