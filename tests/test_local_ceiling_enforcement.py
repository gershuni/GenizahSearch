# -*- coding: utf-8 -*-
"""Phase 95 REQ-10 + D-26 + D-41 + W8: pre-scan ceiling enforcement.

Covers:
  - Single-folder Add Folder ceiling (file_count > 5000 OR bytes > 2 GB)
  - W8 RESOLVED: multi-folder AGGREGATE ceiling for Refresh
  - Unavailable folders excluded from Refresh aggregate
"""
from __future__ import annotations

import sys
import unittest.mock as mock

import pytest

try:
    from PyQt6.QtWidgets import QApplication, QWidget, QMessageBox
    from PyQt6.QtCore import Qt  # noqa: F401  (imported to verify PyQt6 availability)

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_indexer(folders=None, prescan_single=None, prescan_all=None):
    m = mock.MagicMock()
    m.list_folders.return_value = folders or []
    m.prescan_count.return_value = prescan_single or (0, 0)
    m.prescan_count_all.return_value = prescan_all or (0, 0)
    m.scan_all.return_value = {
        "indexed": 0, "skipped": 0, "errors": 0, "cancelled": False
    }
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


# ---------------------------------------------------------------------------
# Single-folder ceiling (Add Folder path)
# ---------------------------------------------------------------------------

def test_prescan_warning_above_5000_files():
    """REQ-10 + D-26: Add Folder path shows dialog when prescan exceeds the file ceiling.

    Ceiling is _MAX_FILES_CEILING = 50,000 (raised from the original 5,000).
    """
    mock_idx = _make_mock_indexer(prescan_single=(50_001, 1_000_000_000))
    tab = _make_tab(mock_idx)

    called_with = {}

    def fake_exec(self):
        called_with["title"] = self.windowTitle()
        called_with["text"] = self.text()
        return QMessageBox.StandardButton.Cancel  # user cancels

    with mock.patch("desktop.my_library_tab.QMessageBox.exec", autospec=True, side_effect=fake_exec):
        result = tab._check_ceiling_single_folder("/some/folder")

    assert not result, "Should return False when user cancels"
    assert called_with, "confirm dialog must have been shown"
    assert "50,001" in called_with.get("text", ""), (
        f"Dialog text should contain formatted file count '50,001'; got: {called_with.get('text')}"
    )


def test_prescan_warning_above_2gb():
    """REQ-10 + D-41: Add Folder path shows dialog when prescan exceeds the byte ceiling.

    Ceiling is _MAX_BYTES_CEILING = 50 GiB (raised from the original 2 GB).
    """
    mock_idx = _make_mock_indexer(prescan_single=(100, 60_000_000_000))
    tab = _make_tab(mock_idx)

    called_with = {}

    def fake_exec(self):
        called_with["title"] = self.windowTitle()
        called_with["text"] = self.text()
        return QMessageBox.StandardButton.Cancel

    with mock.patch("desktop.my_library_tab.QMessageBox.exec", autospec=True, side_effect=fake_exec):
        result = tab._check_ceiling_single_folder("/some/folder")

    assert not result, "Should return False when user cancels"
    text = called_with.get("text", "")
    assert "60.0 GB" in text, (
        f"Dialog text should contain '60.0 GB'; got: {text}"
    )


def test_no_dialog_below_ceiling_single_folder():
    """D-26: no dialog shown when prescan is under both thresholds (single folder)."""
    mock_idx = _make_mock_indexer(prescan_single=(100, 100_000_000))
    tab = _make_tab(mock_idx)

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.question"
    ) as qmock:
        result = tab._check_ceiling_single_folder("/some/folder")

    assert result is True, "Should return True (proceed) when under threshold"
    qmock.assert_not_called()


def test_user_confirms_proceeds():
    """D-26: when user clicks Yes on ceiling dialog, scan proceeds (returns True).

    Values must exceed _MAX_FILES_CEILING (50,000) so the dialog actually fires.
    """
    mock_idx = _make_mock_indexer(prescan_single=(60_000, 3_000_000_000))
    tab = _make_tab(mock_idx)

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.exec",
        autospec=True,
        return_value=QMessageBox.StandardButton.Yes,
    ):
        result = tab._check_ceiling_single_folder("/huge/folder")

    assert result is True, "Should return True when user confirms"


# ---------------------------------------------------------------------------
# W8: multi-folder AGGREGATE ceiling (Refresh path)
# ---------------------------------------------------------------------------

def test_refresh_aggregates_prescan_across_all_folders():
    """W8: Refresh uses prescan_count_all() which aggregates across folders.

    Sub-test A: aggregate (3000 files, 1.5 GB) is under threshold → no dialog.
    Sub-test B: aggregate (60,000 files, 2.5 GB) is over threshold → dialog shown.

    Ceiling is _MAX_FILES_CEILING = 50,000 / _MAX_BYTES_CEILING = 50 GiB.
    """
    # --- Sub-test A: under threshold ---
    folders_3 = [
        {"folder_id": i, "path": f"/f{i}", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"}
        for i in range(3)
    ]
    mock_idx_a = _make_mock_indexer(
        folders=folders_3,
        prescan_all=(3000, 1_500_000_000),  # under 50,000 files and under 50 GiB
    )
    tab_a = _make_tab(mock_idx_a)

    with mock.patch("desktop.my_library_tab.QMessageBox.question") as qmock_a:
        result_a = tab_a._check_ceiling_refresh_aggregate()

    assert result_a is True, "Under threshold: should proceed without dialog"
    qmock_a.assert_not_called()

    # --- Sub-test B: over threshold ---
    mock_idx_b = _make_mock_indexer(
        folders=folders_3,
        prescan_all=(60_000, 2_500_000_000),  # over 50,000 files
    )
    tab_b = _make_tab(mock_idx_b)

    called_with = {}

    def fake_exec(self):
        called_with["title"] = self.windowTitle()
        called_with["text"] = self.text()
        return QMessageBox.StandardButton.Cancel

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.exec", autospec=True, side_effect=fake_exec
    ):
        result_b = tab_b._check_ceiling_refresh_aggregate()

    assert not result_b, "Over threshold: should show dialog (user cancels)"
    text = called_with.get("text", "")
    assert "60,000" in text or "60000" in text, (
        f"Dialog text should include aggregate file count; got: {text}"
    )
    assert "2.5 GB" in text or "2,500" in text, (
        f"Dialog text should include aggregate size; got: {text}"
    )


def test_refresh_aggregate_excludes_unavailable_folders():
    """W8: aggregate ceiling for Refresh excludes unavailable folders (D-40).

    3 folders: 2 available (status='active'), 1 unavailable.
    prescan_count() should only be called for the 2 available folders.
    The unavailable folder contributes 0 to the aggregate.
    """
    folders = [
        {"folder_id": 1, "path": "/f1", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
        {"folder_id": 2, "path": "/f2", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
        {"folder_id": 3, "path": "/f3_unavail", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "unavailable"},
    ]
    # prescan_count_all() returns sum for only the 2 available folders
    mock_idx = _make_mock_indexer(
        folders=folders,
        prescan_all=(2000, 1_000_000_000),  # 2 × 1000 files, 2 × 500 MB
    )
    # Also stub per-folder prescan so we can verify unavailable is not called
    def per_folder_prescan(path):
        if "unavail" in path:
            raise AssertionError(
                f"prescan_count should NOT be called for unavailable folder: {path}"
            )
        return (1000, 500_000_000)

    mock_idx.prescan_count.side_effect = per_folder_prescan
    # prescan_count_all() already set via prescan_all; override side_effect to use return_value
    mock_idx.prescan_count_all.side_effect = None
    mock_idx.prescan_count_all.return_value = (2000, 1_000_000_000)

    tab = _make_tab(mock_idx)

    with mock.patch("desktop.my_library_tab.QMessageBox.question") as qmock:
        result = tab._check_ceiling_refresh_aggregate()

    # 2000 files < 5000 and 1 GB < 2 GB → under threshold → no dialog
    assert result is True, "Under threshold (unavailable excluded): should proceed"
    qmock.assert_not_called()

    # Verify prescan_count_all was called (not per-folder prescan_count)
    mock_idx.prescan_count_all.assert_called()


def test_aggregate_both_thresholds_checked():
    """W8: aggregate ceiling triggers on file_count > 50,000 even if bytes are small."""
    folders_1 = [
        {"folder_id": 1, "path": "/f1", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
    ]
    # File count triggers (> 50,000), bytes does not (well under 50 GiB)
    mock_idx = _make_mock_indexer(
        folders=folders_1,
        prescan_all=(50_001, 500_000_000),
    )
    tab = _make_tab(mock_idx)

    called = []

    def fake_exec(self):
        called.append(self.text())
        return QMessageBox.StandardButton.Cancel

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.exec", autospec=True, side_effect=fake_exec
    ):
        result = tab._check_ceiling_refresh_aggregate()

    assert not result, "Should show dialog when file_count > 50,000"
    assert called, "the confirm dialog must have been shown"


def test_aggregate_bytes_threshold_triggers():
    """W8: aggregate ceiling triggers on total_bytes > 50 GiB even if count is small."""
    folders_1 = [
        {"folder_id": 1, "path": "/f1", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
    ]
    # Bytes triggers (> 50 GiB = 53,687,091,200), count does not (< 50,000)
    mock_idx = _make_mock_indexer(
        folders=folders_1,
        prescan_all=(100, 60_000_000_000),
    )
    tab = _make_tab(mock_idx)

    called = []

    def fake_exec(self):
        called.append(self.text())
        return QMessageBox.StandardButton.Cancel

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.exec", autospec=True, side_effect=fake_exec
    ):
        result = tab._check_ceiling_refresh_aggregate()

    assert not result, "Should show dialog when total_bytes > 2 GB"
    assert called, "the confirm dialog must have been shown"
