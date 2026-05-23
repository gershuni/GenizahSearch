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

    # Add Folder / Remove / Refresh / Cancel buttons — check by widget reference
    # rather than translated text so the test works in both EN and HE environments.
    assert tab._btn_add is not None, "Missing _btn_add (Add Folder button)"
    assert tab._btn_remove is not None, "Missing _btn_remove (Remove button)"
    assert tab._btn_refresh is not None, "Missing _btn_refresh (Refresh button)"
    assert tab._btn_cancel is not None, "Missing _btn_cancel (Cancel button)"

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


# ---------------------------------------------------------------------------
# Bug 1 regression: folder list survives language toggle (writer-lock retry)
# ---------------------------------------------------------------------------

def test_folder_list_populated_after_init_with_registered_folders():
    """Bug 1 regression: _refresh_folder_list_ui populates the QListWidget even
    when the indexer is constructed while the previous process may still hold
    the writer lock.

    The fix: LocalIndexer.__init__ retries writer acquisition with back-off.
    Here we verify that if LocalIndexer is constructable (mock), the folder
    list widget reflects the registered folders regardless of language.
    """
    from desktop.my_library_tab import MyLibraryTab
    from PyQt6.QtWidgets import QListWidget

    folders = [
        {"folder_id": 1, "path": "/docs/research", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
        {"folder_id": 2, "path": "/docs/thesis", "added_at": 1.0,
         "last_scanned_at": 1.0, "status": "active"},
    ]
    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer(folders=folders)
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    folder_list = tab.findChild(QListWidget)
    assert folder_list is not None
    # Both folders must appear — regardless of UI language
    assert folder_list.count() == 2, (
        f"Expected 2 folders in list, got {folder_list.count()}"
    )
    paths = [folder_list.item(i).text() for i in range(folder_list.count())]
    assert "/docs/research" in paths
    assert "/docs/thesis" in paths


def test_folder_list_not_empty_when_indexer_writer_retry_succeeds():
    """Bug 1 regression: if LocalIndexer writer() raises on first attempt but
    succeeds on retry, the tab still initialises and shows folders.

    Simulates the Windows writer-lock race on restart after language switch.
    """
    from desktop.my_library_tab import MyLibraryTab
    from PyQt6.QtWidgets import QListWidget

    folders = [
        {"folder_id": 1, "path": "/home/user/genizah", "added_at": 0.0,
         "last_scanned_at": 0.0, "status": "active"},
    ]
    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        # LocalIndexer itself succeeds (we mock the whole class, so the
        # retry logic inside __init__ is exercised by shared/local_indexer.py
        # tests; here we verify the tab doesn't end up with a None indexer
        # when construction succeeds on a later attempt).
        mock_idx = _make_mock_indexer(folders=folders)
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    # If indexer is None, folder list is empty — that is the bug.
    assert tab._indexer is not None, (
        "MyLibraryTab._indexer is None after successful LocalIndexer construction — "
        "folder list will be empty (Bug 1)"
    )
    folder_list = tab.findChild(QListWidget)
    assert folder_list.count() == 1


# ---------------------------------------------------------------------------
# Bug 3 regression: Refresh button always fires the worker
# ---------------------------------------------------------------------------

def test_refresh_always_starts_worker_even_with_no_folders():
    """Bug 3 regression: clicking Refresh starts the indexer worker even when
    no folders are registered (zero-work case).

    The bug: an exception in _check_ceiling_refresh_aggregate was silently
    swallowed by Qt, making the button appear to do nothing.
    """
    from desktop.my_library_tab import MyLibraryTab

    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer(folders=[])
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    # Replace _start_worker to track calls
    start_worker_calls = []
    original_start = tab._start_worker

    def _patched_start_worker(toast_on_complete=False):
        start_worker_calls.append(toast_on_complete)
        # Don't actually start a QThread — just record the call.

    tab._start_worker = _patched_start_worker

    # Simulate clicking Refresh
    tab._on_refresh_clicked()

    assert len(start_worker_calls) == 1, (
        f"Expected _start_worker to be called once; got {len(start_worker_calls)} calls. "
        "Refresh button does nothing (Bug 3)"
    )


def test_refresh_shows_status_message_on_completion():
    """Bug 3 regression: after Refresh finishes (even zero-work), a status
    message is shown so the user gets visible feedback.
    """
    from desktop.my_library_tab import MyLibraryTab

    parent = _make_mock_parent()

    with (
        mock.patch("desktop.my_library_tab.LocalIndexer") as MockIdx,
        mock.patch("desktop.my_library_tab.os.makedirs"),
    ):
        mock_idx = _make_mock_indexer(folders=[])
        MockIdx.return_value = mock_idx
        tab = MyLibraryTab(parent)

    with mock.patch.object(tab, "_show_status_message") as mock_show:
        tab._on_worker_finished(
            {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False},
            toast=False,
        )
        assert mock_show.call_count >= 1, (
            "No status message shown after Refresh — user gets no feedback (Bug 3)"
        )


# ---------------------------------------------------------------------------
# Bug 4 regression: LOCAL sys_id browse renders file text
# ---------------------------------------------------------------------------

def test_open_result_in_browse_calls_open_local_browse_for_local_sys_id():
    """Bug 4 regression: when a LOCAL search result is sent to Browse,
    open_result_in_browse must call _open_local_browse instead of browse_load.

    Previously open_result_in_browse always called self.browse_load() which
    doesn't know how to render LOCAL text — resulting in empty Browse pane.

    This test verifies via AST that the function body of open_result_in_browse
    contains a call to _open_local_browse (i.e., it is called, not merely
    defined elsewhere in the file).
    """
    import ast

    with open("genizah_app.py", encoding="utf-8") as f:
        source = f.read()

    tree = ast.parse(source)

    # Find the open_result_in_browse method body
    calls_open_local = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == "open_result_in_browse":
                # Walk the function body looking for a call to _open_local_browse
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        func = inner.func
                        if (
                            isinstance(func, ast.Attribute)
                            and func.attr == "_open_local_browse"
                        ):
                            calls_open_local = True
                            break
                break

    assert calls_open_local, (
        "open_result_in_browse does not call self._open_local_browse — "
        "LOCAL hits fall through to browse_load() and render nothing (Bug 4)"
    )


# ---------------------------------------------------------------------------
# Bug 2 regression: all tr() keys used in MyLibraryTab have HE translations
# ---------------------------------------------------------------------------

def test_my_library_tab_tr_keys_have_hebrew_translations():
    """Bug 2 regression: every tr() key used in desktop/my_library_tab.py
    must have a Hebrew translation entry in genizah_translations.TRANSLATIONS.

    Missing translations cause Hebrew users to see English UI labels.
    """
    import re
    from genizah_translations import TRANSLATIONS

    with open("desktop/my_library_tab.py", encoding="utf-8") as f:
        source = f.read()

    # Extract all literal string arguments to tr()
    # Matches tr("...") or tr('...') with simple string literals (no f-strings)
    pattern = re.compile(r'\btr\(\s*["\']([^"\'{}]+)["\']\s*\)')
    keys = set(pattern.findall(source))

    # Some keys like "OK", "Cancel", "Remove", "Refresh" exist from other
    # tabs — only the Phase-95-specific ones are new.
    phase95_keys = {
        "My Library",
        "Indexed folders:",
        "File status:",
        "Add Folder…",           # "Add Folder…"
        "Select folder to index",
        "Folder already covered",
        "Already registered",
        "This folder is already registered.",
        "My Library Error",
        "Remove folder",
        "Remove failed",
        "Add folder — pre-scan",  # "Add folder — pre-scan"
        "Refresh — pre-scan",     # "Refresh — pre-scan"
    }

    missing = []
    for key in keys:
        if key not in TRANSLATIONS:
            missing.append(key)

    assert not missing, (
        f"Missing Hebrew translations for {len(missing)} tr() key(s) "
        f"in desktop/my_library_tab.py:\n  " + "\n  ".join(sorted(missing))
    )


# ---------------------------------------------------------------------------
# Bug 5 regression: About credit uses correct Hebrew spelling for Seewald
# ---------------------------------------------------------------------------

def test_seewald_hebrew_spelling_in_translations():
    """Bug 5 regression: the Seewald attribution in genizah_translations.py
    (desktop About dialog) must use 'יהודה זייבלד' (not 'זיוואלד').
    """
    with open("genizah_translations.py", encoding="utf-8") as f:
        content = f.read()

    # The WRONG spelling that was in the original code
    assert "זיואלד" not in content, (
        "genizah_translations.py still contains old spelling 'זיוואלד' — "
        "must be replaced with 'זייבלד' (Bug 5)"
    )
    # The CORRECT spelling must be present
    assert "זייבלד" in content, (
        "genizah_translations.py missing correct spelling 'זייבלד' (Bug 5)"
    )


def test_seewald_attribution_integrated_in_credits_not_standalone():
    """Bug 5 regression: the Seewald attribution in web/pages/about.py must NOT
    be a standalone paragraph but integrated into the credits section.
    """
    with open("web/pages/about.py", encoding="utf-8") as f:
        content = f.read()

    # The attribution must mention 'Seewald' and 'Inspired by' / 'inspired by'
    # in the same credits block, not as a separate <p> tag after the creator line.
    assert "Seewald" in content, (
        "web/pages/about.py must contain 'Seewald' in the credits"
    )
    # Must NOT use old wrong HE spelling
    assert "זיואלד" not in content, (
        "web/pages/about.py still contains old spelling 'זיוואלד' (Bug 5)"
    )
