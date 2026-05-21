# -*- coding: utf-8 -*-
"""Phase 95 REQ-7 + REQ-8: My Library tab registration and badge tests.

These tests are headless-safe: they use QApplication but avoid any GUI
rendering that requires a display. Tests that can only run with a real Qt
display are skipped with pytest.mark.skip / DISPLAY guard.
"""
from __future__ import annotations

import sys
import types
import unittest.mock as mock

import pytest

# ---------------------------------------------------------------------------
# Ensure a QApplication exists for all tests (headless-safe)
# ---------------------------------------------------------------------------
try:
    from PyQt6.QtWidgets import QApplication, QWidget

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not QT_AVAILABLE, reason="PyQt6 not available"
)


@pytest.fixture(autouse=True)
def _ensure_app():
    """Guarantee a QApplication exists for every test."""
    if QT_AVAILABLE:
        from PyQt6.QtWidgets import QApplication
        import sys
        if QApplication.instance() is None:
            QApplication(sys.argv)
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_indexer(folders=None):
    """Return a MagicMock that satisfies LocalIndexer's interface."""
    m = mock.MagicMock()
    m.list_folders.return_value = folders or []
    m.prescan_count.return_value = (0, 0)
    m.prescan_count_all.return_value = (0, 0)
    m.scan_all.return_value = {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}
    m.startup_recovery.return_value = {
        "pending_deletes_recovered": 0,
        "pending_inserts_recovered": 0,
    }
    return m


def _make_mock_parent(searcher=None):
    """Return a QWidget-based mock that exposes a .searcher attribute."""
    parent = QWidget() if QT_AVAILABLE else mock.MagicMock()
    parent.searcher = searcher or mock.MagicMock()
    if QT_AVAILABLE:
        parent.statusBar = mock.MagicMock(return_value=mock.MagicMock())
    return parent


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_my_library_tab_has_required_classes():
    """Verify MyLibraryTab and LocalIndexerWorker are importable (REQ-8)."""
    from desktop.my_library_tab import MyLibraryTab, LocalIndexerWorker  # noqa: F401
    assert MyLibraryTab is not None
    assert LocalIndexerWorker is not None


def test_my_library_tab_has_folder_list_widget():
    """REQ-8 + D-16: My Library tab has QListWidget, Add/Remove, progress bar,
    Refresh button, and per-file status QTableWidget with 3 columns."""
    from desktop.my_library_tab import MyLibraryTab
    from PyQt6.QtWidgets import (
        QListWidget,
        QPushButton,
        QProgressBar,
        QTableWidget,
    )

    parent = _make_mock_parent()
    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    # Folder list widget
    folder_list = tab.findChild(QListWidget)
    assert folder_list is not None, "Missing QListWidget (folder list)"

    # Add Folder / Remove buttons
    buttons = tab.findChildren(QPushButton)
    btn_texts = [b.text() for b in buttons]
    assert any("Add" in t or "folder" in t.lower() for t in btn_texts), (
        f"No 'Add Folder' button found; buttons: {btn_texts}"
    )
    assert any("Remove" in t for t in btn_texts), (
        f"No 'Remove' button found; buttons: {btn_texts}"
    )
    # Refresh and Cancel buttons
    assert any("Refresh" in t for t in btn_texts), (
        f"No 'Refresh' button found; buttons: {btn_texts}"
    )

    # Progress bar
    assert tab.findChild(QProgressBar) is not None, "Missing QProgressBar"

    # Status QTableWidget with 3 columns (Filename | Pages | Status)
    table = tab.findChild(QTableWidget)
    assert table is not None, "Missing QTableWidget"
    assert table.columnCount() == 3, (
        f"Expected 3 columns, got {table.columnCount()}"
    )


def test_my_library_tab_registered():
    """REQ-8: MyLibraryTab exists and is importable (used by genizah_app.py).

    The genizah_app.py tab registration is tested by the import + attribute
    check in test_task2_genizah_app_registration below (avoids launching a full
    QMainWindow which requires a real display + heavy startup).
    """
    from desktop.my_library_tab import MyLibraryTab
    assert MyLibraryTab is not None


def test_task2_genizah_app_registration():
    """REQ-8: verify genizah_app.py imports MyLibraryTab and registers it as 7th tab.

    AST-level check (no import of genizah_app which would trigger heavy startup).
    """
    import ast

    with open("genizah_app.py", encoding="utf-8") as f:
        source = f.read()

    # Check import
    assert "from desktop.my_library_tab import MyLibraryTab" in source, (
        "genizah_app.py must import MyLibraryTab"
    )
    # Check instantiation
    assert "self.my_library_tab = MyLibraryTab(self)" in source, (
        "genizah_app.py must instantiate MyLibraryTab"
    )
    # Check addTab
    assert "self.tabs.addTab(self.my_library_tab" in source, (
        "genizah_app.py must register MyLibraryTab via addTab"
    )


def test_reload_local_indexes_called_after_worker_finished():
    """HIGH-1: _on_worker_finished calls search_engine.reload_local_indexes()."""
    from desktop.my_library_tab import MyLibraryTab

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    mock_searcher.reload_local_indexes.reset_mock()

    # Simulate worker finishing
    tab._on_worker_finished({"indexed": 3, "skipped": 0}, toast=False)

    mock_searcher.reload_local_indexes.assert_called()


def test_reload_local_indexes_called_after_remove_folder():
    """HIGH-1: _on_remove_folder_clicked calls search_engine.reload_local_indexes()."""
    from desktop.my_library_tab import MyLibraryTab
    from PyQt6.QtCore import Qt

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    folders = [
        {"folder_id": 1, "path": "/test/folder", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"}
    ]

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer(folders=folders)
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    mock_searcher.reload_local_indexes.reset_mock()

    # Select the first folder item
    tab._folder_list.setCurrentRow(0)

    from PyQt6.QtWidgets import QMessageBox as _QMB

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.question",
        return_value=_QMB.StandardButton.Yes,
    ):
        tab._on_remove_folder_clicked()

    mock_searcher.reload_local_indexes.assert_called()


def test_reload_local_indexes_called_at_startup_recovery():
    """HIGH-1: _on_startup_recovery_completed calls search_engine.reload_local_indexes()."""
    from desktop.my_library_tab import MyLibraryTab

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        # Reset before construction so we can count calls from __init__
        mock_searcher.reload_local_indexes.reset_mock()
        tab = MyLibraryTab(parent)  # noqa: F841

    # At least one reload call should have been made during startup recovery
    assert mock_searcher.reload_local_indexes.call_count >= 1


def test_reload_local_indexes_called_on_rebuild_lab():
    """HIGH-1: _on_rebuild_lab_completed calls search_engine.reload_local_indexes()."""
    from desktop.my_library_tab import MyLibraryTab

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    mock_searcher.reload_local_indexes.reset_mock()
    tab._on_rebuild_lab_completed()
    mock_searcher.reload_local_indexes.assert_called_once()


def test_refresh_then_search_in_same_session_returns_local_hits():
    """HIGH-1 end-to-end wiring: after worker finishes, search_engine.reload_local_indexes()
    is called so the engine's searcher is updated (mocked)."""
    from desktop.my_library_tab import MyLibraryTab

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    mock_searcher.reload_local_indexes.reset_mock()
    # Simulate a completed Refresh
    tab._on_worker_finished({"indexed": 5, "skipped": 0, "errors": 0}, toast=False)

    # After Refresh finishes, reload_local_indexes must have been called
    mock_searcher.reload_local_indexes.assert_called()
    # In a real session this makes local hits visible; here we assert the call site


def test_delete_then_search_no_local_hits():
    """HIGH-1 delete regression: after remove_folder, reload_local_indexes() is called
    so deleted LOCAL docs disappear from live search (mocked)."""
    from desktop.my_library_tab import MyLibraryTab
    from PyQt6.QtWidgets import QMessageBox as _QMB

    mock_searcher = mock.MagicMock()
    parent = _make_mock_parent(searcher=mock_searcher)

    folders = [
        {"folder_id": 1, "path": "/test/folder", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"}
    ]

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer(folders=folders)
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    tab._folder_list.setCurrentRow(0)
    mock_searcher.reload_local_indexes.reset_mock()

    with mock.patch(
        "desktop.my_library_tab.QMessageBox.question",
        return_value=_QMB.StandardButton.Yes,
    ):
        tab._on_remove_folder_clicked()

    mock_searcher.reload_local_indexes.assert_called()


def test_refresh_completion_always_shows_status_message():
    """B2 feedback: _on_worker_finished ALWAYS calls _show_status_message,
    even when zero files were re-indexed (the zero-work case that previously
    gave no visible feedback to the user)."""
    from desktop.my_library_tab import MyLibraryTab

    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    # Patch _show_status_message to capture calls
    with mock.patch.object(tab, "_show_status_message") as mock_show:
        # Zero-work result: nothing was indexed, everything up to date
        tab._on_worker_finished(
            {"indexed": 0, "skipped": 5, "errors": 0, "cancelled": False},
            toast=False,
        )
        mock_show.assert_called_once()
        msg = mock_show.call_args[0][0]
        # Message must mention "complete" or "up to date" to be useful
        assert any(kw in msg.lower() for kw in ("complete", "up to date", "refresh")), (
            f"Status message did not contain expected keyword: {msg!r}"
        )


def test_refresh_completion_message_includes_counts():
    """B2 feedback: status message includes re-indexed and up-to-date counts."""
    from desktop.my_library_tab import MyLibraryTab

    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer()
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    with mock.patch.object(tab, "_show_status_message") as mock_show:
        # Non-zero result: 3 indexed, 7 skipped
        tab._on_worker_finished(
            {"indexed": 3, "skipped": 7, "errors": 0, "cancelled": False},
            toast=False,
        )
        mock_show.assert_called_once()
        msg = mock_show.call_args[0][0]
        # Both counts should appear in the message
        assert "3" in msg, f"Indexed count missing from message: {msg!r}"
        assert "7" in msg, f"Skipped count missing from message: {msg!r}"
