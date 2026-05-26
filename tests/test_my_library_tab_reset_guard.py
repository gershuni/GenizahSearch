# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-B (D-10) — Reset guard simplification tests.

Pins the new single-condition guard semantics on MyLibraryTab._update_reset_button_state:
  - With orphan `scan_runs.status='running'` rows + idle worker -> Reset is ENABLED.
  - With active worker -> Reset is DISABLED with bilingual tooltip.
  - `start_recovery_probe` is NOT called from the reset-guard code path.
  - Reassuring bilingual tooltip is preserved verbatim from Phase 97.2.

These tests MUST be RED against the current (pre-D-10) code where
`_update_reset_button_state` still consults `start_recovery_probe()` and disables
the button whenever orphan rows exist.

RED-before-fix protocol: Task 1 creates this file expecting failure; Task 2
implements D-10 in desktop/my_library_tab.py to flip these GREEN.
"""
from __future__ import annotations

import os
import sys

# Path bootstrap so `desktop.my_library_tab` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

# PyQt6-aware fixture pattern mirroring tests/test_folder_walk_worker.py.
try:
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


def _make_tab(tmp_path, monkeypatch):
    """Construct a real MyLibraryTab against an isolated tmp_path index.

    Patches genizah_core.Config.LOCAL_INDEX_DIR and LOCAL_LAB_INDEX_DIR BEFORE
    construction so MyLibraryTab._init_indexer wires the LocalIndexer at the
    test-owned paths. The DB lives inside LOCAL_INDEX_DIR (production layout).
    """
    from genizah_core import Config
    idx_dir = str(tmp_path / "local_index")
    lab_dir = str(tmp_path / "local_lab")
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", idx_dir, raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", lab_dir, raising=False)

    from desktop.my_library_tab import MyLibraryTab
    tab = MyLibraryTab(parent=None)
    return tab


def _close_tab(tab):
    """Best-effort cleanup of Qt resources + indexer SQLite handle."""
    try:
        if getattr(tab, "_indexer", None) is not None:
            try:
                tab._indexer._conn.close()
            except Exception:
                pass
        tab.deleteLater()
    except Exception:
        pass


def test_reset_enabled_with_orphan_rows_and_idle_worker(tmp_path, monkeypatch):
    """D-10: orphan scan_runs.status='running' rows must NOT disable the Reset button.

    Reset is precisely what cleans up orphan rows — locking the user out is the
    UX dead-end the Phase 97.2-02 REVIEWS Codex MEDIUM recommendation created.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert tab._indexer is not None, "fixture must produce a real LocalIndexer"
        # Inject an orphan scan_runs row mimicking the post-crash state.
        tab._indexer._conn.execute(
            "INSERT INTO scan_runs (scan_run_id, started_at, status) "
            "VALUES (?, ?, ?)",
            ("test-orphan-run", 1700000000.0, "running"),
        )
        tab._indexer._conn.commit()

        # Ensure worker is idle.
        tab._worker = None

        tab._update_reset_button_state()

        assert tab._btn_reset.isEnabled(), (
            "D-10: Reset must be ENABLED when worker is idle, even with orphan "
            "scan_runs.status='running' rows in the DB. reset_my_library's "
            "7-step protocol is the load-bearing safety; the UI guard does not "
            "duplicate it."
        )
    finally:
        _close_tab(tab)


def test_reset_disabled_while_worker_running(tmp_path, monkeypatch):
    """D-10: active worker MUST keep Reset disabled with the bilingual tooltip."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        class _FakeRunningWorker:
            def isRunning(self):  # noqa: N802 — Qt-style name to match real worker
                return True

        tab._worker = _FakeRunningWorker()

        tab._update_reset_button_state()

        assert not tab._btn_reset.isEnabled(), (
            "D-10: Reset must be DISABLED while a worker is actively running."
        )
        tip = tab._btn_reset.toolTip()
        assert "Stop or resolve the active scan first" in tip, (
            f"English half of disabled tooltip missing; got {tip!r}"
        )
        assert "עצור או פתור את הסריקה הפעילה תחילה" in tip, (
            f"Hebrew half of disabled tooltip missing; got {tip!r}"
        )
    finally:
        _close_tab(tab)


def test_reset_does_not_call_start_recovery_probe(tmp_path, monkeypatch):
    """D-10: start_recovery_probe MUST NOT be consulted from the reset-guard path.

    Monkeypatches start_recovery_probe to raise AssertionError. If the guard
    still calls it (pre-D-10 code), the AssertionError will surface and fail
    the test. After D-10 lands, the guard never reaches start_recovery_probe.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert tab._indexer is not None

        def _must_not_be_called():
            raise AssertionError(
                "D-10: _update_reset_button_state must not call start_recovery_probe"
            )

        monkeypatch.setattr(
            tab._indexer, "start_recovery_probe", _must_not_be_called
        )

        tab._worker = None
        # Must not raise — D-10 removed the start_recovery_probe call.
        tab._update_reset_button_state()
    finally:
        _close_tab(tab)


def test_reset_enabled_tooltip_reassuring(tmp_path, monkeypatch):
    """D-10: idle-worker tooltip must preserve the Phase 97.2 bilingual reassurance.

    Both halves (EN + HE) must be present so users know Reset only touches
    LOCAL/LAB index data, not source files or the Genizah corpus.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        tab._worker = None
        tab._update_reset_button_state()

        assert tab._btn_reset.isEnabled(), "Idle worker -> enabled"
        tip = tab._btn_reset.toolTip()
        assert "Reset deletes LOCAL/LAB index data only" in tip, (
            f"English half missing from idle tooltip; got {tip!r}"
        )
        assert "האיפוס מוחק רק את נתוני האינדקס המקומי" in tip, (
            f"Hebrew half missing from idle tooltip; got {tip!r}"
        )
    finally:
        _close_tab(tab)
