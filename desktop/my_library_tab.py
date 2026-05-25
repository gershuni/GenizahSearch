# -*- coding: utf-8 -*-
"""Phase 95 My Library desktop tab (REQ-7, REQ-8, REQ-10).

Provides MyLibraryTab(QWidget) — the 7th tab in GenizahGUI — and
LocalIndexerWorker(QThread) that wraps shared/local_indexer.LocalIndexer.

Key architecture decisions enforced here:
  - D-24 Codex revision: cooperative cancel between files AND within files
    (between PDF pages / DOCX chunks); partial in-flight pages rolled back.
  - D-25 Codex revision: QMutex gates all side-index mutations; concurrent
    Refresh/Add/Remove requests queue FIFO max depth 1.
  - D-26 + D-41: pre-scan ceiling dialog shows BOTH file_count + total_bytes;
    triggers on file_count > 5000 OR total_bytes > 2 GB.
  - W8 RESOLVED: two distinct ceiling entry-points —
      _check_ceiling_single_folder()  — Add Folder (single folder only)
      _check_ceiling_refresh_aggregate() — Refresh (aggregate across ALL folders)
  - HIGH-1 review fix: after every commit (Refresh finish, Remove, LAB rebuild,
    startup-recovery), calls self.search_engine.reload_local_indexes() so the
    live SearchEngine session picks up newly indexed/deleted files without restart.
  - D-40: unavailable folders shown with warning colour (#f39c12) + tooltip;
    previously-indexed files remain searchable; excluded from Refresh aggregate.
  - D-15: folder list persisted in SQLite (portable); QSettings for UI prefs only.
"""
from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

from PyQt6.QtCore import (
    Qt,
    QMutex,
    QThread,
    QSettings,
    QTimer,
    pyqtSignal,
)
from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QListWidget,
    QListWidgetItem,
    QLabel,
    QProgressBar,
    QProgressDialog,
    QTreeWidget,
    QTreeWidgetItem,
    QFileDialog,
    QMessageBox,
    QHeaderView,
    QAbstractItemView,
)
from PyQt6.QtGui import QColor

from shared.local_indexer import LocalIndexer
from genizah_core import Config, tr, CURRENT_LANG

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (D-26 + D-41)
# ---------------------------------------------------------------------------
# Phase 97 C-01 — soft warning thresholds (was Phase 95 hard-stop at 5K/2GB).
# Codex P0 sequencing: constant only lifted AFTER Wave A (R-03 cache + R-02
# atomic rebuild + D-NEW-1 migration) and Wave B (C-05 oversized/zip-bomb) land.
_MAX_FILES_CEILING = 50_000
_MAX_BYTES_CEILING = 50 * 1024 ** 3   # 50 GB

# Column indices for the unified file tree widget
_COL_FILENAME = 0
_COL_PAGES = 1
_COL_STATUS = 2


# ---------------------------------------------------------------------------
# Phase 96 D-F1 (D-09): rescan-preserves-opt-out-state helper
# ---------------------------------------------------------------------------

def _prune_optouts_to_disk(optouts: set, on_disk: set) -> set:
    """Phase 96 D-F1 D-09: filter the opt-out set to entries still on disk.

    Called after a rescan completes — drops entries for files that have
    been removed (or renamed) since the last scan. Surviving entries are
    preserved untouched.

    Pure function (no I/O, no side effects) — keep it that way so it can
    be exercised by tests/test_local_optout_persistence.py without any
    Qt or filesystem fixtures.

    Examples:
        >>> _prune_optouts_to_disk(set(), set())
        set()
        >>> _prune_optouts_to_disk({'/a', '/b'}, {'/a'})
        {'/a'}
        >>> _prune_optouts_to_disk({'/a'}, set())
        set()
    """
    if not optouts:
        return set()
    if not on_disk:
        return set()
    return optouts & on_disk


# ---------------------------------------------------------------------------
# Phase 96 D-F1 + Phase 96 polish (96-09 bug #1): unified file tree widget
# ---------------------------------------------------------------------------

class _UnifiedFileTreeWidget(QTreeWidget):
    """Phase 96 D-F1 + 96-09 UX redesign: single tree widget combining
    opt-out checkboxes and per-file status/pages columns.

    Replaces the QSplitter[_OptoutTreeWidget | _status_table] layout shipped
    in plan 96-06. Three columns:
      - Col 0: Filename (with tri-state opt-out checkbox)
      - Col 1: Pages
      - Col 2: Status

    Tri-state folder checkboxes work identically to the old _OptoutTreeWidget.
    The opt-out SET-DIFFERENCE/UNION algebra (Codex HIGH #1) is preserved.

    Per-file status is updated via update_file_status(filename, pages, status)
    which is called from LocalIndexerWorker.file_finished signal handler
    (replacing the old _status_table.insertRow path).

    Bug #2 fix (96-09): flush_pending() force-fires the debounce timer so
    that pending opt-out changes are committed before app.closeEvent saves
    the session. Called from GenizahGUI.closeEvent.
    """

    # 150ms debounce avoids thrashing the session-save + re-filter pipeline
    # when the user rapidly toggles multiple checkboxes (e.g., "uncheck all").
    _DEBOUNCE_MS = 150

    def __init__(self, parent, app):
        super().__init__(parent)
        self._app = app
        self._suppress_signals = False
        # Track the set of paths CURRENTLY DISPLAYED in the tree (canonical
        # form). _commit_changes() uses this as the diff scope so paths
        # belonging to other indexed folders are left untouched.
        self._displayed_paths: set = set()
        # Map canonical filepath -> QTreeWidgetItem for O(1) status updates.
        self._leaf_by_path: dict = {}
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.setInterval(self._DEBOUNCE_MS)
        self._save_timer.timeout.connect(self._commit_changes)
        self.setColumnCount(3)
        self.setHeaderLabels([tr("Filename"), tr("Pages"), tr("Status")])
        self.header().setSectionResizeMode(
            _COL_FILENAME, QHeaderView.ResizeMode.Stretch
        )
        self.header().setSectionResizeMode(
            _COL_PAGES, QHeaderView.ResizeMode.ResizeToContents
        )
        self.header().setSectionResizeMode(
            _COL_STATUS, QHeaderView.ResizeMode.ResizeToContents
        )
        self.itemChanged.connect(self._on_item_changed)

    def populate_for_folder(self, folder_path: str, prior_status: dict = None):
        """Rebuild the tree for the given indexed folder. Walks the
        filesystem (so ignored files also appear, letting the user opt
        them out preemptively before indexer touches them).

        Phase 96 fix-2: prior_status is an optional dict mapping
        canonical_filepath -> {'pages': int, 'status': str} loaded from
        the LocalIndexer DB via get_file_status_for_folder(). When provided,
        Pages and Status columns are populated immediately so the user can
        see the results of the last scan without having to wait for a new one.
        """
        import os
        # If prior_status was not supplied, try to load it from the indexer.
        if prior_status is None:
            try:
                my_lib = None
                # Walk up from self._app to find MyLibraryTab (may be parent or app itself)
                for attr in ('my_library_tab',):
                    my_lib = getattr(self._app, attr, None)
                    if my_lib is not None:
                        break
                if my_lib is None and hasattr(self._app, '_indexer'):
                    my_lib = self._app
                indexer = getattr(my_lib, '_indexer', None) if my_lib else None
                if indexer is not None:
                    prior_status = indexer.get_file_status_for_folder(folder_path)
            except Exception:
                prior_status = {}
        if prior_status is None:
            prior_status = {}

        self._suppress_signals = True
        try:
            self.clear()
            self._displayed_paths = set()
            self._leaf_by_path = {}
            optouts = getattr(self._app, '_local_file_optouts', set())
            root_item = QTreeWidgetItem(self, [os.path.basename(folder_path) or folder_path])
            root_item.setFlags(
                root_item.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsAutoTristate
            )
            # Recurse into subdirectories, passing prior_status for column population.
            self._populate_node(root_item, folder_path, optouts, prior_status)
            self.expandAll()
        finally:
            self._suppress_signals = False

    def _populate_node(self, parent_item, dirpath: str, optouts: set, prior_status: dict = None):
        """Recursively add files and subdirs to parent_item.

        Phase 96 fix-2: prior_status (canonical_filepath -> {pages, status})
        is threaded down so leaves show scan results from the last scan
        immediately on tab open, not only after a new scan completes.
        """
        import os
        from shared.local_sys_id import _canonical_filepath
        SUPPORTED = {'.pdf', '.docx', '.txt'}
        if prior_status is None:
            prior_status = {}
        try:
            entries = sorted(os.listdir(dirpath))
        except (OSError, PermissionError):
            return
        # Add subdirs first
        for name in entries:
            full = os.path.join(dirpath, name)
            if os.path.isdir(full):
                sub = QTreeWidgetItem(parent_item, [name, '', ''])
                sub.setFlags(
                    sub.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsAutoTristate
                )
                self._populate_node(sub, full, optouts, prior_status)
        # Add files
        for name in entries:
            full = os.path.join(dirpath, name)
            if not os.path.isfile(full):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in SUPPORTED:
                continue
            # Phase 96 fix-2: pre-populate Pages + Status from last scan if known.
            canonical = _canonical_filepath(full)
            file_info = prior_status.get(canonical, {})
            prior_pages = file_info.get('pages', 0)
            prior_st = file_info.get('status', '')
            pages_str = str(prior_pages) if prior_pages and prior_pages > 0 else ''
            # Translate stored status codes to display strings (mirrors update_file_status).
            # Phase 97 C-05: reads from local_files.extraction_status (LD-9 UI source of truth).
            if prior_st == 'ok':
                status_str = tr("OK")
            elif prior_st == 'cancelled':
                status_str = tr("Cancelled")
            elif prior_st == 'no_text_layer':
                status_str = tr("No text layer")
            elif prior_st == 'encoding_error':
                status_str = tr("Encoding error")
            elif prior_st == 'unsupported':
                status_str = tr("Unsupported")
            elif prior_st == 'oversized':
                # Phase 97 C-05 — file > 100 MB hard skip (LD-9)
                # local_files.extraction_status is the UI source of truth per LD-9.
                status_str = "גדול מדי (>100 מ\"ב)" if CURRENT_LANG == 'he' else tr("Too large (>100 MB)")
            elif prior_st == 'zip_bomb_suspected':
                # Phase 97 C-05 — zip-container uncompressed size > 500 MB (LD-9)
                status_str = "ארכיון חשוד" if CURRENT_LANG == 'he' else tr("Suspicious archive")
            elif prior_st in ('error',):
                status_str = tr("Error")
            else:
                status_str = ''
            leaf = QTreeWidgetItem(parent_item, [name, pages_str, status_str])
            leaf.setFlags(leaf.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            # Apply error colour for error-state rows (mirrors update_file_status).
            if prior_st in ('error', 'encoding_error'):
                from PyQt6.QtGui import QColor
                for col in range(3):
                    leaf.setForeground(col, QColor('#e74c3c'))
            elif prior_st in ('oversized', 'zip_bomb_suspected'):
                # Phase 97 C-05 — orange warning colour (same as cancelled/warning states)
                from PyQt6.QtGui import QColor
                for col in range(3):
                    leaf.setForeground(col, QColor('#e67e22'))
            elif prior_st == 'cancelled':
                from PyQt6.QtGui import QColor
                for col in range(3):
                    leaf.setForeground(col, QColor('#e67e22'))
            # Codex MEDIUM #9 closure: canonicalize at populate time so the
            # value stashed in UserRole matches what _local_file_optouts holds.
            leaf.setData(0, Qt.ItemDataRole.UserRole, canonical)
            self._displayed_paths.add(canonical)
            self._leaf_by_path[canonical] = leaf
            is_opted_out = canonical in optouts
            leaf.setCheckState(
                0,
                Qt.CheckState.Unchecked if is_opted_out else Qt.CheckState.Checked,
            )

    def reset_for_scan(self):
        """Clear Pages/Status columns on all existing leaves before a new scan.

        Called by _start_worker so the tree reflects the upcoming scan state
        rather than stale data from a previous scan.
        """
        self._suppress_signals = True
        try:
            self._clear_status_columns_recursive(self.invisibleRootItem())
        finally:
            self._suppress_signals = False

    def _clear_status_columns_recursive(self, node):
        """Recursively blank Pages + Status columns on every node."""
        for i in range(node.childCount()):
            child = node.child(i)
            child.setText(_COL_PAGES, '')
            child.setText(_COL_STATUS, '')
            self._clear_status_columns_recursive(child)

    def update_file_status(self, filepath: str, pages: int, status: str,
                            err: str = '') -> None:
        """Update Pages + Status for the leaf matching `filepath`.

        Phase 96 fix-7 (Codex P1.2): `filepath` is now the CANONICAL filepath
        emitted by LocalIndexer._file_finished_cb (via _canonical_filepath),
        NOT a bare basename.  _leaf_by_path is keyed by canonical path, so the
        lookup is an O(1) dict hit — no linear basename scan.  Two folders
        containing a file with the same name (e.g. scan.pdf) are now updated
        independently without collision.

        Called from MyLibraryTab._on_file_finished (the
        LocalIndexerWorker.file_finished signal handler).
        """
        # Translate status code to display string (mirrors old _status_table logic).
        # Phase 97 C-05: reads from local_files.extraction_status (LD-9 UI source of truth).
        display_status = status
        if status == "ok":
            display_status = tr("OK")
        elif status == "cancelled":
            display_status = tr("Cancelled")
        elif status == "no_text_layer":
            display_status = tr("No text layer")
        elif status == "encoding_error":
            display_status = tr("Encoding error")
        elif status == "unsupported":
            display_status = tr("Unsupported")
        elif status == "oversized":
            # Phase 97 C-05 — file > 100 MB hard skip (LD-9)
            display_status = "גדול מדי (>100 מ\"ב)" if CURRENT_LANG == 'he' else tr("Too large (>100 MB)")
        elif status == "zip_bomb_suspected":
            # Phase 97 C-05 — zip-container uncompressed size > 500 MB (LD-9)
            display_status = "ארכיון חשוד" if CURRENT_LANG == 'he' else tr("Suspicious archive")

        pages_str = str(pages) if pages > 0 else '-'

        # Direct canonical-path lookup (O(1)).  Falls back to basename scan
        # only for legacy callers that may still pass bare filenames.
        leaf = self._leaf_by_path.get(filepath)
        if leaf is None:
            # Fallback: linear basename scan for backward compat with any
            # caller that still passes a bare filename.
            import os
            for canonical, item in self._leaf_by_path.items():
                if os.path.basename(canonical) == filepath:
                    leaf = item
                    break
        if leaf is None:
            # Not in the currently displayed tree — no-op (file may belong
            # to a different folder not currently selected in the UI).
            return

        self._suppress_signals = True
        try:
            leaf.setText(_COL_PAGES, pages_str)
            leaf.setText(_COL_STATUS, display_status)
            if status in ('error', 'encoding_error'):
                from PyQt6.QtGui import QColor
                for col in range(3):
                    leaf.setForeground(col, QColor('#e74c3c'))
            elif status in ('oversized', 'zip_bomb_suspected', 'cancelled'):
                # Phase 97 C-05 — orange warning colour for size/zip-bomb skips (LD-9)
                from PyQt6.QtGui import QColor
                for col in range(3):
                    leaf.setForeground(col, QColor('#e67e22'))
        finally:
            self._suppress_signals = False

    def flush_pending(self):
        """Phase 96 bug #2 fix: force-fire pending debounce if active.

        Called from GenizahGUI.closeEvent BEFORE _save_session() so that
        opt-out changes made in the last 150 ms are committed to
        app._local_file_optouts before the session is serialised to disk.
        Without this, a QTimer single-shot is silently abandoned when the
        Qt event loop shuts down, losing the user's pending changes.
        """
        if self._save_timer.isActive():
            self._save_timer.stop()
            self._commit_changes()

    def _on_item_changed(self, item, column):
        """User toggled a checkbox — debounce the commit."""
        if self._suppress_signals:
            return
        # Only respond to changes on the checkbox column (col 0).
        if column != _COL_FILENAME:
            return
        # Restart the debounce timer; multiple rapid changes coalesce.
        self._save_timer.start()

    def _commit_changes(self):
        """Debounce fired — perform SET-DIFFERENCE/UNION update, persist, re-filter.

        REVISION 2026-05-24 — Codex HIGH #1 closure (load-bearing):
        previously this method cleared the global set and rebuilt from the
        currently displayed tree only — that ERASED opt-outs in folders not
        currently visible. The new flow:

          1. Walk the currently displayed tree leaves and partition them into
             `currently_unchecked` (opted-out by the user, paths to ADD to
             the global set) and `currently_checked` (paths to REMOVE from
             the global set).
          2. Apply DIFFERENCE then UNION on the global set, scoped to the
             currently displayed paths only. Paths not in self._displayed_paths
             (i.e., from other indexed folders) are LEFT UNTOUCHED.

        This makes "toggle folder B erases folder A's opt-outs" structurally
        impossible. Regression test: tests/test_local_optout_persistence.py
        ::test_folder_a_optout_survives_folder_b_toggle (added by Plan 96-01).
        """
        currently_unchecked: set = set()
        currently_checked: set = set()
        for i in range(self.topLevelItemCount()):
            self._collect_leaves_by_state(
                self.topLevelItem(i),
                currently_unchecked,
                currently_checked,
            )

        app = self._app
        if not hasattr(app, '_local_file_optouts'):
            app._local_file_optouts = set()

        # SET-DIFFERENCE/UNION update — scoped to currently displayed paths.
        # Paths NOT in self._displayed_paths (other indexed folders) untouched.
        existing: set = app._local_file_optouts
        # Remove paths that are now checked (user re-enabled them) — scoped.
        existing.difference_update(currently_checked)
        # Add paths that are now unchecked (user opted them out) — scoped.
        existing.update(currently_unchecked)

        try:
            if hasattr(app, '_save_session'):
                app._save_session()
        except Exception:
            pass  # session-save is best-effort
        try:
            if hasattr(app, '_reapply_filters_for_optout_change'):
                app._reapply_filters_for_optout_change()
        except Exception:
            pass

    def _collect_leaves_by_state(self, node, unchecked_out, checked_out):
        """Recursively gather LEAF nodes' canonical paths by check state.

        Two output sets so _commit_changes() can perform diff/union scoped
        to the currently displayed tree.
        """
        if node.childCount() == 0:
            data = node.data(0, Qt.ItemDataRole.UserRole)
            if not data:
                return
            state = node.checkState(0)
            if state == Qt.CheckState.Unchecked:
                unchecked_out.add(data)
            elif state == Qt.CheckState.Checked:
                checked_out.add(data)
            # PartiallyChecked is a folder-level state, not seen on leaves;
            # we drop it defensively.
            return
        for i in range(node.childCount()):
            self._collect_leaves_by_state(node.child(i), unchecked_out, checked_out)


# Keep old name as alias so any external code or tests referencing
# _OptoutTreeWidget still resolve correctly.
_OptoutTreeWidget = _UnifiedFileTreeWidget


# ---------------------------------------------------------------------------
# LocalIndexerWorker  (D-23 / D-24 / D-25)
# ---------------------------------------------------------------------------

class LocalIndexerWorker(QThread):
    """Qt thread wrapper around LocalIndexer.scan_all().

    D-23 signals:
      progress_updated(current_index, total_files, current_filename)
      file_finished(filename, status, pages, error_msg)
      finished_signal(result_dict)
      error_signal(error_str)

    D-24 Codex revision: cancel flag is checked between files AND the indexer's
    cancel_check lambda threads the flag into PDF-page and DOCX-chunk loops so
    huge single files respond within one page/chunk, not only between files.
    """

    progress_updated = pyqtSignal(int, int, str)
    file_finished = pyqtSignal(str, str, int, str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, indexer: LocalIndexer) -> None:
        super().__init__()
        self._indexer = indexer
        self._cancel_requested = False

    def cancel(self) -> None:
        """Set the cooperative cancel flag (D-24)."""
        self._cancel_requested = True

    def run(self) -> None:  # noqa: PLR0912
        try:
            # Wire up per-file progress callbacks into the indexer
            def _on_progress(current: int, total: int, filename: str) -> None:
                self.progress_updated.emit(current, total, filename)

            def _on_file_done(filename: str, status: str, pages: int, err: str) -> None:
                self.file_finished.emit(filename, status, pages, err)

            self._indexer._progress_cb = _on_progress
            self._indexer._file_finished_cb = _on_file_done

            result = self._indexer.scan_all(
                cancel_check=lambda: self._cancel_requested
            )
            self.finished_signal.emit(result)
        except Exception as exc:  # noqa: BLE001
            logger.exception("LocalIndexerWorker: unhandled error")
            self.error_signal.emit(str(exc))


# ---------------------------------------------------------------------------
# PrescanWorker (Phase 97 C-03 — folder walk off the UI thread)
# ---------------------------------------------------------------------------

class PrescanWorker(QThread):
    """Phase 97 C-03 — runs LocalIndexer.prescan_count() off the UI thread.

    Emits finished_signal(file_count, total_bytes) on success, or
    error_signal(error_str) on failure.  The UI thread must NOT block waiting
    for this worker — wire finished_signal to a slot that continues the
    ceiling-check flow and shows the QProgressDialog with Cancel.
    """

    finished_signal = pyqtSignal(int, int)  # file_count, total_bytes
    error_signal = pyqtSignal(str)

    def __init__(self, indexer: LocalIndexer, folder_path: str) -> None:
        super().__init__()
        self._indexer = indexer
        self._folder_path = folder_path
        self._cancel = False

    def cancel(self) -> None:
        """Cooperative cancel — threads the flag into prescan_count's os.walk loop."""
        self._cancel = True

    def run(self) -> None:
        try:
            file_count, total_bytes = self._indexer.prescan_count(
                self._folder_path,
                cancel_check=lambda: self._cancel,
            )
            self.finished_signal.emit(file_count, total_bytes)
        except Exception as exc:  # noqa: BLE001
            logger.exception("PrescanWorker: unhandled error")
            self.error_signal.emit(str(exc))


# ---------------------------------------------------------------------------
# FolderWalkWorker (Phase 97 U-03 — filesystem walk off the UI thread)
# ---------------------------------------------------------------------------

class FolderWalkWorker(QThread):
    """Phase 97 U-03 — filesystem walk off the UI thread.

    Emits batched (filepath, mtime_ns, size) records throttled to
    BATCH_SIZE files OR BATCH_TIMEOUT seconds so the UI stays responsive.

    CRITICAL: NO QWidget mutation in this thread (T-97E-02 / AST guard
    test_folder_walk_worker.py::test_no_widget_mutation pins this invariant).
    Only pyqtSignal emissions — the UI-thread slot handles all widget updates.
    """

    batch_emitted = pyqtSignal(list)      # list of (filepath, mtime_ns, size) tuples
    finished_signal = pyqtSignal(int, int)  # total_files, total_bytes
    error_signal = pyqtSignal(str)

    BATCH_SIZE = 100    # emit after this many files
    BATCH_TIMEOUT = 0.5  # or after this many seconds

    def __init__(self, folder_paths: list) -> None:
        super().__init__()
        self._folder_paths = folder_paths
        self._cancel_requested = False

    def cancel(self) -> None:
        """Request cooperative cancellation."""
        self._cancel_requested = True

    def run(self) -> None:
        """Walk folders and emit batched file-metadata records.

        This method must NOT mutate any QWidget — it only emits signals.
        The UI-thread slot connected to batch_emitted handles all widget updates.
        AST guard: test_folder_walk_worker.py::test_no_widget_mutation.
        """
        import os as _os
        import time as _time
        batch: list = []
        last_emit = _time.monotonic()
        total_files = 0
        total_bytes = 0
        try:
            for folder in self._folder_paths:
                if self._cancel_requested:
                    break
                for root, dirs, files in _os.walk(folder, followlinks=False):
                    if self._cancel_requested:
                        break
                    for name in files:
                        if self._cancel_requested:
                            break
                        fp = _os.path.join(root, name)
                        try:
                            stat = _os.stat(fp)
                        except OSError:
                            continue
                        batch.append((fp, stat.st_mtime_ns, stat.st_size))
                        total_files += 1
                        total_bytes += stat.st_size
                        now = _time.monotonic()
                        if (
                            len(batch) >= self.BATCH_SIZE
                            or (now - last_emit) >= self.BATCH_TIMEOUT
                        ):
                            self.batch_emitted.emit(batch)
                            batch = []
                            last_emit = now
            if batch:
                self.batch_emitted.emit(batch)
            self.finished_signal.emit(total_files, total_bytes)
        except Exception as exc:  # noqa: BLE001
            logger.exception("FolderWalkWorker: unhandled error")
            self.error_signal.emit(str(exc))


# ---------------------------------------------------------------------------
# MyLibraryTab
# ---------------------------------------------------------------------------

class MyLibraryTab(QWidget):
    """My Library tab — 7th tab in GenizahGUI (Pitfall #4: NOT 6th).

    Manages multi-folder indexing of .docx / .pdf / .txt files into the LOCAL
    Tantivy side-index, with QMutex serialization (D-25), cooperative
    cancellation (D-24), ceiling dialogs (D-26/D-41 + W8), unavailable-folder
    UI (D-40), and automatic reload_local_indexes() after every mutation (HIGH-1).
    """

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        # Phase 97 R-01: LOCAL search is gated until recovery probe resolves.
        # Set False here; flipped True after _init_indexer + recovery probe below.
        self.is_searchable: bool = False

        # HIGH-1: stash reference to the SearchEngine so the four reload call
        # sites can call self.search_engine.reload_local_indexes().
        # The parent GenizahGUI exposes the searcher via self.searcher which is
        # assigned asynchronously in on_startup_finished().  We use a deferred
        # property pattern: at call time we read parent.searcher; if it is None
        # (startup not yet complete) we log a warning and skip (the index was
        # just built so no live session is querying it yet).
        self._parent_window = parent

        # D-25: QMutex serialises all side-index mutations
        self._indexer_mutex = QMutex()
        # FIFO queue max depth 1 (D-25 Codex revision): stores a callable
        self._queued_action: Optional[callable] = None

        # Active worker (None when idle)
        self._worker: Optional[LocalIndexerWorker] = None

        # QSettings for non-portable UI prefs only (D-15)
        self._settings = QSettings("Dicta", "GenizahSearchPro")

        # Build UI first (so widgets exist before _init_indexer may log)
        self._build_ui()

        # Initialise the indexer and run startup recovery
        self._init_indexer()

        # Phase 97 R-01: run recovery probe after indexer init.
        if self._indexer is not None:
            try:
                running_runs = self._indexer.start_recovery_probe()
                if not running_runs:
                    self.is_searchable = True
                else:
                    self._show_recovery_modal(running_runs)
            except Exception as exc:  # noqa: BLE001
                logger.warning("MyLibraryTab: start_recovery_probe failed: %s", exc)
                self.is_searchable = True  # fail-open so search still works
        else:
            self.is_searchable = True  # no indexer = no gate needed

        # D-25: silent background rescan at startup
        self._auto_rescan_on_startup()

    # ------------------------------------------------------------------
    # Property: search_engine (deferred — may be None before startup finishes)
    # ------------------------------------------------------------------

    @property
    def search_engine(self):
        """Return parent window's SearchEngine (self._parent_window.searcher).

        Returns None if parent is None or startup is not yet complete.
        """
        if self._parent_window is None:
            return None
        return getattr(self._parent_window, "searcher", None)

    @property
    def lab_engine(self):
        """Return parent window's LabEngine (self._parent_window.lab_engine).

        Returns None if parent is None or LAB engine not yet constructed.
        CR-02: needed so we can call lab_engine.reload_local_lab_index() in
        the same code paths that call search_engine.reload_local_indexes().
        """
        if self._parent_window is None:
            return None
        return getattr(self._parent_window, "lab_engine", None)

    def _reload_all_local_indexes(self) -> None:
        """CR-02: reload BOTH SearchEngine and LabEngine LOCAL LAB searchers.

        Wraps the previous self.search_engine.reload_local_indexes() pattern
        and adds the LAB-side reload so LAB-mode Composition Search also
        picks up newly indexed LOCAL files.  Each call is wrapped in its
        own try/except so a LAB failure does not block the main reload.
        """
        if self.search_engine is not None:
            try:
                self.search_engine.reload_local_indexes()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._reload_all_local_indexes: "
                    "search_engine.reload_local_indexes failed: %s", exc
                )
        if self.lab_engine is not None:
            try:
                self.lab_engine.reload_local_lab_index()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._reload_all_local_indexes: "
                    "lab_engine.reload_local_lab_index failed: %s", exc
                )

    def _maybe_rebuild_lab_if_stale(self) -> bool:
        """WR-08: D-38 weights-hash invalidation entry point.

        Inspects LabEngine._check_local_lab_freshness AFTER the LAB searcher
        is reopened. If the LAB index is present but stale (weights_hash
        mismatch, or .meta.json missing), call SearchEngine.rebuild_local_lab_index
        to rebuild it from the durable main LOCAL Tantivy index.

        Returns True if a rebuild was triggered. Defensive: any exception is
        logged + swallowed so a LAB rebuild failure never blocks normal
        Refresh/Add/Remove flow.

        Trigger points (mirror reload sites): startup recovery, worker
        finished, folder removed. No rebuild is attempted if (a) the LAB
        searcher could not be opened at all, or (b) the indexer is None.
        """
        if self._indexer is None:
            return False
        lab = self.lab_engine
        search = self.search_engine
        if lab is None or search is None:
            return False
        try:
            if lab.local_lab_searcher is None:
                # No LAB index present yet — nothing to rebuild (first build
                # is triggered explicitly by the user via "Rebuild LAB", not
                # by a freshness check on an absent index).
                return False
            # Freshness check raises only on serious misuse — guard anyway.
            try:
                fresh = lab._check_local_lab_freshness()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._maybe_rebuild_lab_if_stale: "
                    "lab._check_local_lab_freshness raised: %s", exc
                )
                return False
            if fresh:
                return False
            # Stale: rebuild via SearchEngine (Option C callback wiring).
            logger.info(
                "WR-08: LAB index stale (weights_hash mismatch) — triggering rebuild"
            )
            try:
                search.rebuild_local_lab_index(self._indexer)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._maybe_rebuild_lab_if_stale: "
                    "rebuild_local_lab_index failed: %s", exc
                )
                return False
            # Reload so the freshly-built LAB is visible in live session.
            self._reload_all_local_indexes()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "MyLibraryTab._maybe_rebuild_lab_if_stale: unexpected error: %s", exc
            )
            return False

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        # ---- Section 1: folder list ----
        root.addWidget(QLabel(tr("Indexed folders:")))

        self._folder_list = QListWidget()
        self._folder_list.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection
        )
        root.addWidget(self._folder_list, stretch=1)

        folder_btns = QHBoxLayout()
        self._btn_add = QPushButton(tr("Add Folder…"))
        self._btn_remove = QPushButton(tr("Remove"))
        self._btn_add.clicked.connect(self._on_add_folder_clicked)
        self._btn_remove.clicked.connect(self._on_remove_folder_clicked)
        folder_btns.addWidget(self._btn_add)
        folder_btns.addWidget(self._btn_remove)
        folder_btns.addStretch()
        root.addLayout(folder_btns)

        # ---- Section 2: refresh / cancel / progress bar ----
        refresh_row = QHBoxLayout()
        self._btn_refresh = QPushButton(tr("Refresh"))
        self._btn_cancel = QPushButton(tr("Cancel"))
        self._btn_cancel.setEnabled(False)
        self._btn_refresh.clicked.connect(self._on_refresh_clicked)
        self._btn_cancel.clicked.connect(self._on_cancel_clicked)
        refresh_row.addWidget(self._btn_refresh)
        refresh_row.addWidget(self._btn_cancel)
        refresh_row.addStretch()
        root.addLayout(refresh_row)

        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._progress_bar.setVisible(False)
        root.addWidget(self._progress_bar)

        # ---- Section 3: unified file tree (Phase 96 D-F1 + 96-09 UX redesign) ----
        # 96-09 bug #1: replace QSplitter[_OptoutTreeWidget | _status_table] with ONE
        # unified tree showing Filename (checkbox) | Pages | Status columns.
        root.addWidget(QLabel(tr("File status & opt-outs:")))

        # Single unified tree — replaces both the old opt-out tree and status table.
        self._unified_tree = _UnifiedFileTreeWidget(self, self._parent_window)
        # Keep _optout_tree as an alias so any external callers (e.g. _on_worker_finished
        # prune block) that reference self._optout_tree still work.
        self._optout_tree = self._unified_tree
        root.addWidget(self._unified_tree, stretch=2)

        # Phase 96 D-F1: when the user picks a folder, repopulate the unified tree.
        # PINNED: selected-path comes from Qt.ItemDataRole.UserRole on the QListWidgetItem
        # (Phase 95 stashes the folder path string here when the folder is added to the list).
        self._folder_list.currentItemChanged.connect(self._on_folder_selection_changed)

        # ---- Section 4: disk indicator (Phase 97 C-06) ----
        self._disk_label = QLabel(tr("Index size: — / — free"))
        self._disk_label.setStyleSheet("color: #888; font-size: 11px;")
        root.addWidget(self._disk_label)

        # Periodic disk-indicator refresh: every 30 s while the tab is visible.
        self._disk_timer = QTimer(self)
        self._disk_timer.setInterval(30_000)  # 30 seconds
        self._disk_timer.timeout.connect(self._update_disk_indicator)
        self._disk_timer.start()

    # ------------------------------------------------------------------
    # Phase 97 C-06: disk indicator with merge headroom
    # ------------------------------------------------------------------

    def _update_disk_indicator(self) -> None:
        """Phase 97 C-06 — live disk indicator with Tantivy merge-headroom warning.

        Warning fires when (free - 2 × index_size) < 1 GB.
        Wired to: showEvent, LocalIndexerWorker.finished_signal, QTimer (30s).
        """
        if self._indexer is None:
            return
        try:
            index_size = self._indexer.estimate_index_size()
            usage = shutil.disk_usage(Config.LOCAL_INDEX_DIR)
            # Reserve 2× current index size as Tantivy merge scratch.
            headroom = usage.free - 2 * index_size
            label_text = tr("Index size: {} / {} free").format(
                self._human_bytes(index_size), self._human_bytes(usage.free)
            )
            if headroom < 1024 ** 3:  # < 1 GB headroom
                label_text += "  " + tr("⚠ low merge headroom")
            self._disk_label.setText(label_text)
        except Exception as exc:  # noqa: BLE001
            logger.debug("_update_disk_indicator: %s", exc)

    def showEvent(self, event) -> None:  # noqa: N802
        """Update disk indicator when tab becomes visible (Phase 97 C-06)."""
        super().showEvent(event)
        self._update_disk_indicator()

    # ------------------------------------------------------------------
    # Phase 96 D-F1: folder selection handler (tree repopulation)
    # ------------------------------------------------------------------

    def _on_folder_selection_changed(self, current, previous):
        """Phase 96 D-F1: repopulate the unified tree when the user selects a folder.

        PINNED (96-08-WIRING-NOTES.md §Plan 96-06 wiring):
          - selected-path extraction: item.data(Qt.ItemDataRole.UserRole)
          - DO NOT use item.text() — that is the display label, not the fs path.
        """
        if not hasattr(self, '_unified_tree'):
            return
        if current is None:
            return
        selected_path = current.data(Qt.ItemDataRole.UserRole)
        if selected_path:
            self._unified_tree.populate_for_folder(selected_path)

    # ------------------------------------------------------------------
    # Indexer initialisation + startup recovery
    # ------------------------------------------------------------------

    def _init_indexer(self) -> None:
        """Instantiate LocalIndexer and run startup crash-recovery (D-21 + HIGH-3)."""
        db_path = os.path.join(Config.LOCAL_INDEX_DIR, "local_index.sqlite3")
        os.makedirs(Config.LOCAL_INDEX_DIR, exist_ok=True)
        try:
            self._indexer = LocalIndexer(
                index_dir=Config.LOCAL_INDEX_DIR,
                lab_index_dir=Config.LOCAL_LAB_INDEX_DIR,
                db_path=db_path,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("MyLibraryTab: LocalIndexer init failed: %s", exc)
            self._indexer = None
            return

        # Run startup recovery (two-pass: pending deletes + pending inserts)
        try:
            recovery = self._indexer.startup_recovery()
            if recovery.get("pending_deletes_recovered") or recovery.get(
                "pending_inserts_recovered"
            ):
                logger.info(
                    "MyLibraryTab startup_recovery: %s", recovery
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("MyLibraryTab: startup_recovery failed: %s", exc)

        # HIGH-1 review fix: reload after recovery so any pending-delete
        # recoveries or pending-insert re-extracts are visible to the live
        # search session (the LOCAL searcher was opened in SearchEngine.__init__
        # BEFORE startup_recovery ran, so it has a stale snapshot).
        self._on_startup_recovery_completed()

        # Populate folder list UI
        self._refresh_folder_list_ui()

    def _show_recovery_modal(self, running_runs: list) -> None:
        """Phase 97 R-01 — 3-button Resume/Restart/Skip recovery modal.

        Displayed when start_recovery_probe() returns non-empty (a previous
        scan was interrupted by crash/power-loss).  Flips is_searchable=True
        in all branches so LOCAL search is unblocked after user decision.

        Wave E (plan 97-05) will refine Resume/Restart to actually resume or
        discard the interrupted run; for Wave A the gate-flip is load-bearing.
        """
        mb = QMessageBox(self)
        mb.setIcon(QMessageBox.Icon.Warning)
        if CURRENT_LANG == "he":
            mb.setWindowTitle("התאוששות מאינדוקס שהופסק")
            mb.setText(
                "האינדוקס הקודם הופסק — להמשיך, להתחיל מחדש או לדלג?"
            )
            btn_resume = mb.addButton("המשך", QMessageBox.ButtonRole.AcceptRole)
            btn_restart = mb.addButton("התחל מחדש", QMessageBox.ButtonRole.DestructiveRole)
            btn_skip = mb.addButton("דלג", QMessageBox.ButtonRole.RejectRole)
        else:
            mb.setWindowTitle("Recover interrupted indexing")
            mb.setText(
                "Previous indexing was interrupted — Resume / Restart / Skip?"
            )
            btn_resume = mb.addButton("Resume", QMessageBox.ButtonRole.AcceptRole)
            btn_restart = mb.addButton("Restart", QMessageBox.ButtonRole.DestructiveRole)
            btn_skip = mb.addButton("Skip", QMessageBox.ButtonRole.RejectRole)
        mb.exec()
        clicked = mb.clickedButton()
        run_id = running_runs[0]  # most recent interrupted run
        if clicked is btn_restart:
            # Phase 97 Wave E: discard_run() removes all four row sources (LD-7).
            # Previously a Wave A stub that only called _end_scan_run("canceled").
            try:
                self._indexer.discard_run(run_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("_show_recovery_modal: discard_run(restart) failed: %s", exc)
                try:
                    self._indexer._end_scan_run(run_id, "canceled")
                except Exception:
                    pass
        elif clicked is btn_skip:
            # Skip: leave partial data in place but mark run as canceled so
            # recovery probe doesn't trigger again on next start.
            try:
                self._indexer._end_scan_run(run_id, "canceled")
            except Exception as exc:  # noqa: BLE001
                logger.warning("_show_recovery_modal: _end_scan_run(skip) failed: %s", exc)
        else:
            # Resume — full resume logic deferred; mark completed so gate lifts.
            try:
                self._indexer._end_scan_run(run_id, "completed")
            except Exception as exc:  # noqa: BLE001
                logger.warning("_show_recovery_modal: _end_scan_run(resume) failed: %s", exc)
        self.is_searchable = True

    def closeEvent(self, event) -> None:
        """Phase 97 LD-6: clean-shutdown sweep — mark any still-running scan_runs
        as completed so they don't trigger recovery modal on next startup.
        Called BEFORE the parent GenizahGUI.closeEvent flushes pending opt-outs.
        """
        try:
            if self._indexer is not None and self._indexer._conn is not None:
                import time as _time
                self._indexer._conn.execute(
                    "UPDATE scan_runs SET status = 'completed', ended_at = ? "
                    "WHERE status = 'running'",
                    (_time.time(),),
                )
                self._indexer._conn.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Phase 97 LD-6: scan_runs clean-shutdown sweep failed")
        super().closeEvent(event)

    def _auto_rescan_on_startup(self) -> None:
        """D-25: silent background rescan; non-modal toast on completion."""
        if self._indexer is None:
            return
        # Only rescan if there are registered folders
        if not self._indexer.list_folders():
            return
        self._start_worker(toast_on_complete=True)

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def _start_worker(self, toast_on_complete: bool = False) -> None:
        """Acquire mutex and start LocalIndexerWorker; queue if mutex held (D-25)."""
        if self._indexer is None:
            return

        if not self._indexer_mutex.tryLock():
            # Mutex held — queue this action (collapse if already queued)
            self._queued_action = lambda: self._start_worker(
                toast_on_complete=toast_on_complete
            )
            return

        # Mutex acquired — spawn worker
        self._btn_refresh.setEnabled(False)
        self._btn_add.setEnabled(False)
        self._btn_remove.setEnabled(False)
        self._btn_cancel.setEnabled(True)
        self._progress_bar.setVisible(True)
        self._progress_bar.setValue(0)
        # 96-09 bug #1: unified tree replaces _status_table; clear Pages/Status columns.
        if hasattr(self, '_unified_tree'):
            self._unified_tree.reset_for_scan()

        self._worker = LocalIndexerWorker(self._indexer)
        self._worker.progress_updated.connect(self._on_progress_updated)
        self._worker.file_finished.connect(self._on_file_finished)
        self._worker.finished_signal.connect(
            lambda result: self._on_worker_finished(result, toast_on_complete)
        )
        self._worker.error_signal.connect(self._on_worker_error)
        self._worker.start()

    def _on_worker_finished(self, result: dict, toast: bool) -> None:
        """Worker completed: unlock mutex, reload indexes, always show summary (HIGH-1 + D-25).

        B2 feedback fix: always show a summary status message after every Refresh
        (including the zero-work case where no files needed re-indexing).  The
        progress bar hits 100% momentarily before being hidden so the user can
        see completion even when the status table remains empty.
        """
        # B2: briefly show 100% so the bar visually completes before disappearing
        self._progress_bar.setValue(100)
        self._progress_bar.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._btn_add.setEnabled(True)
        self._btn_remove.setEnabled(True)
        self._btn_cancel.setEnabled(False)
        self._worker = None

        # HIGH-1 review fix: reload BEFORE toast so by the time the user
        # dismisses the toast, search already picks up the new content.
        # CR-02: also reload LabEngine LOCAL LAB searcher so LAB-mode
        # Composition Search sees newly indexed LOCAL files.
        self._reload_all_local_indexes()

        # Phase 97 C-06: refresh disk indicator after indexing batch completes.
        self._update_disk_indicator()

        # WR-08: if the LAB index is stale (D-38 weights_hash mismatch),
        # rebuild it now while we are already on the post-Refresh code
        # path. Defensive — never blocks normal Refresh flow.
        self._maybe_rebuild_lab_if_stale()

        # Release mutex AFTER reload
        self._indexer_mutex.unlock()

        # Refresh folder list (status may have changed, e.g. unavailable)
        self._refresh_folder_list_ui()

        # B2 feedback fix: always show a completion summary — status bar for
        # both the startup silent-rescan (toast=True) and manual Refresh
        # (toast=False, the case the user reported gave no feedback).
        indexed = result.get("indexed", 0)
        skipped = result.get("skipped", 0)
        errors = result.get("errors", 0)
        cancelled = result.get("cancelled", False)
        if cancelled:
            msg = tr(f"Refresh cancelled — {indexed} files indexed, {skipped} up to date")
        else:
            msg = tr(
                f"Refresh complete — {indexed} re-indexed, {skipped} up to date"
                + (f", {errors} errors" if errors else "")
            )
        self._show_status_message(msg)

        # For startup silent-rescan (toast=True) that had actual work: also
        # surface a QLabel summary so it is visible even if status bar is hidden.
        # (Manual Refresh already shows the status table rows as visual confirmation.)
        if toast and indexed > 0:
            self._show_refresh_summary_label(indexed, skipped, errors)

        # Phase 96 D-F1 D-09: drop opt-out entries for files no longer on disk.
        # PINNED (96-08-WIRING-NOTES.md §Plan 96-06 wiring):
        #   scan-complete callback = _on_worker_finished (Phase 95 actual name)
        #   indexer attribute on tab = self._indexer
        try:
            app = self._parent_window
            on_disk = set()
            indexer = getattr(self, '_indexer', None)
            if indexer is not None:
                if hasattr(indexer, 'list_all_filepaths'):
                    on_disk = set(indexer.list_all_filepaths())
                else:
                    # Fallback: query SQLite local_files table directly.
                    try:
                        conn = getattr(indexer, '_conn', None)
                        if conn is not None:
                            cur = conn.execute("SELECT filepath FROM local_files")
                            on_disk = {row[0] for row in cur.fetchall()}
                    except Exception as exc:
                        try:
                            logger.debug(
                                "Phase 96 prune fallback: SELECT filepath FROM local_files "
                                "failed: %s", exc,
                            )
                        except Exception:
                            pass
                        on_disk = set()
            if app is not None and hasattr(app, '_local_file_optouts'):
                before_count = len(app._local_file_optouts)
                app._local_file_optouts = _prune_optouts_to_disk(
                    app._local_file_optouts, on_disk
                )
                after_count = len(app._local_file_optouts)
                logger.info(
                    "MyLibrary opt-out prune after rescan: on_disk=%d "
                    "optouts_before=%d optouts_after=%d",
                    len(on_disk),
                    before_count,
                    after_count,
                )
                if before_count != after_count:
                    try:
                        logger.info(
                            "Phase 96 D-F1: pruned %d opt-out entries "
                            "(was %d, now %d) after rescan — likely due to "
                            "file removal or drive remap.",
                            before_count - after_count, before_count, after_count,
                        )
                    except Exception:
                        pass
                if hasattr(app, '_save_session'):
                    try:
                        app._save_session()
                    except Exception:
                        pass
        except Exception as exc:
            # Never let prune failure crash the scan-finished UI flow.
            try:
                logger.warning("Phase 96 D-F1 prune-on-rescan failed: %s", exc)
            except Exception:
                pass

        # Process any queued action
        if self._queued_action is not None:
            action = self._queued_action
            self._queued_action = None
            action()

    def _show_refresh_summary_label(
        self, indexed: int, skipped: int, errors: int
    ) -> None:
        """Show a non-modal summary label above the status table (B2 feedback).

        Only called for the startup background rescan when it found new work
        (toast=True and indexed > 0).  For manual Refresh the status table rows
        themselves serve as the visual confirmation.
        """
        msg = tr(
            f"Last scan: {indexed} new files indexed, {skipped} up to date"
            + (f", {errors} errors" if errors else "")
        )
        self._show_status_message(msg)

    def _on_worker_error(self, msg: str) -> None:
        """Worker raised an unhandled error."""
        self._progress_bar.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._btn_add.setEnabled(True)
        self._btn_remove.setEnabled(True)
        self._btn_cancel.setEnabled(False)
        self._worker = None
        self._indexer_mutex.unlock()
        QMessageBox.warning(
            self,
            tr("My Library Error"),
            tr(f"Indexing error: {msg}"),
        )

        if self._queued_action is not None:
            action = self._queued_action
            self._queued_action = None
            action()

    # ------------------------------------------------------------------
    # HIGH-1 reload call sites
    # ------------------------------------------------------------------

    def _on_startup_recovery_completed(self) -> None:
        """HIGH-1 call site 4: called after startup_recovery() completes.

        Reloads LOCAL indexes so any recovered pending-deletes or pending-inserts
        are visible to the live SearchEngine session.
        CR-02: also reloads LabEngine LOCAL LAB searcher (REQ-6).
        WR-08: also triggers a LAB rebuild if the on-disk index is stale
        (e.g. user changed LAB weights between sessions).
        """
        self._reload_all_local_indexes()
        self._maybe_rebuild_lab_if_stale()

    def _on_rebuild_lab_completed(self) -> None:
        """HIGH-1 call site 3: called after rebuild_local_lab_index() finishes.

        Reloads LOCAL indexes (includes the LAB side-index) so the live session
        uses the freshly-built LAB fingerprints for Composition/Parallels.
        CR-02: also reloads LabEngine LOCAL LAB searcher (REQ-6).
        """
        self._reload_all_local_indexes()

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------

    def _on_add_folder_clicked(self) -> None:
        """Open folder picker; run single-folder ceiling check; add + scan (D-16 + D-26)."""
        if self._indexer is None:
            return
        path = QFileDialog.getExistingDirectory(
            self, tr("Select folder to index")
        )
        if not path:
            return

        # W8 — single-folder ceiling check for Add Folder
        if not self._check_ceiling_single_folder(path):
            return

        try:
            added = self._indexer.add_folder(path)
        except ValueError as exc:
            QMessageBox.warning(
                self,
                tr("Folder already covered"),
                str(exc),
            )
            return

        if not added:
            QMessageBox.information(
                self,
                tr("Already registered"),
                tr("This folder is already registered."),
            )
            return

        self._refresh_folder_list_ui()
        # Kick off a scan for the newly added folder
        self._start_worker(toast_on_complete=False)

    def _on_remove_folder_clicked(self) -> None:
        """Synchronous delete (D-20); then reload indexes (HIGH-1 call site 2)."""
        if self._indexer is None:
            return
        items = self._folder_list.selectedItems()
        if not items:
            return

        folder_path = items[0].data(Qt.ItemDataRole.UserRole)
        if not folder_path:
            return

        reply = QMessageBox.question(
            self,
            tr("Remove folder"),
            tr(
                f"Remove '{folder_path}' from My Library?\n\n"
                "All indexed files from this folder will be removed from search results."
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            removed = self._indexer.remove_folder(folder_path)
            logger.info(
                "MyLibraryTab: removed folder '%s' (%d files)", folder_path, removed
            )
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(
                self,
                tr("Remove failed"),
                tr(f"Could not remove folder: {exc}"),
            )
            return

        # HIGH-1 review fix: deleted LOCAL docs must disappear from live search
        # immediately, not only after app restart.
        # CR-02: also reload LabEngine LOCAL LAB searcher (REQ-6).
        self._reload_all_local_indexes()

        self._refresh_folder_list_ui()

    def _on_refresh_clicked(self) -> None:
        """Aggregate ceiling check (W8) then start worker (D-25).

        Bug-3 fix: wrapped in try/except so Qt never silently swallows an
        exception from _check_ceiling_refresh_aggregate (e.g. SQLite or OS
        error from prescan_count_all), which would make the button appear to
        do nothing.  On error we log and fall through to start the worker so
        the user still gets a Refresh attempt.
        """
        if self._indexer is None:
            return
        # W8 — AGGREGATE ceiling check for Refresh
        try:
            if not self._check_ceiling_refresh_aggregate():
                return
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "MyLibraryTab._on_refresh_clicked: ceiling check failed (%s); "
                "proceeding with Refresh anyway", exc
            )
        self._start_worker(toast_on_complete=False)

    def _on_cancel_clicked(self) -> None:
        """Phase 97 U-02 — 3-button Cancel modal: Discard / Keep partial / Resume indexing.

        Replaces the old cooperative-cancel-only approach (D-24) with a discard/keep
        choice backed by discard_run() / keep_run() so the user can cleanly remove
        partial-run data (LD-7) or preserve it for future resume.
        """
        if self._worker is None or not self._worker.isRunning():
            self._btn_cancel.setEnabled(False)
            return

        run_id = self._indexer._current_scan_run_id if self._indexer is not None else None

        mb = QMessageBox(self)
        mb.setIcon(QMessageBox.Icon.Question)
        if CURRENT_LANG == "he":
            mb.setWindowTitle("ביטול אינדוקס")
            mb.setText(
                "האם לבטל את כל מה שאונדק בריצה הזאת, לשמור את הספרייה החלקית ולעצור, "
                "או להמשיך את האינדוקס?"
            )
            btn_discard = mb.addButton("בטל הכל", QMessageBox.ButtonRole.DestructiveRole)
            btn_keep = mb.addButton("שמור חלקי", QMessageBox.ButtonRole.AcceptRole)
            btn_resume = mb.addButton("המשך אינדוקס", QMessageBox.ButtonRole.RejectRole)
        else:
            mb.setWindowTitle("Cancel indexing")
            mb.setText(
                "Discard everything indexed in this run, or keep partial library and stop?"
            )
            btn_discard = mb.addButton("Discard", QMessageBox.ButtonRole.DestructiveRole)
            btn_keep = mb.addButton("Keep partial", QMessageBox.ButtonRole.AcceptRole)
            btn_resume = mb.addButton("Resume indexing", QMessageBox.ButtonRole.RejectRole)
        mb.setDefaultButton(btn_resume)
        mb.exec()
        clicked = mb.clickedButton()

        if clicked is btn_resume:
            # User cancelled the cancellation — leave worker running
            return

        # Stop the worker
        self._worker.cancel()
        self._worker.wait(5000)
        self._btn_cancel.setEnabled(False)

        if self._indexer is not None and run_id:
            if clicked is btn_discard:
                try:
                    self._indexer.discard_run(run_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("_on_cancel_clicked: discard_run failed: %s", exc)
            else:
                # Keep partial
                try:
                    self._indexer.keep_run(run_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("_on_cancel_clicked: keep_run failed: %s", exc)

        try:
            self._refresh_folder_list_ui()
        except Exception:
            pass

    def _on_progress_updated(
        self, current: int, total: int, filename: str
    ) -> None:
        """Update progress bar from worker signal."""
        if total > 0:
            pct = int(current * 100 / total)
            self._progress_bar.setValue(pct)
        else:
            self._progress_bar.setValue(0)

    def _on_file_finished(
        self, filepath: str, status: str, pages: int, err: str
    ) -> None:
        """Update Pages + Status columns in the unified tree (D-22 two-stage UX).

        Phase 96 fix-7 (Codex P1.2): `filepath` is now a canonical filepath
        (emitted by LocalIndexer._file_finished_cb via _canonical_filepath),
        NOT a bare basename.  update_file_status does an O(1) dict lookup so
        two files with the same basename in different folders are updated
        independently.

        D-22: the worker emits the final status directly after _finish_file
        returns, so each leaf is updated once (no two-stage transition needed
        at the UI level — the simplified model is unchanged).
        """
        if hasattr(self, '_unified_tree'):
            self._unified_tree.update_file_status(filepath, pages, status, err)

    # ------------------------------------------------------------------
    # W8 Ceiling checks — TWO entry points
    # ------------------------------------------------------------------

    def _check_ceiling_single_folder(self, folder_path: str) -> bool:
        """W8: SINGLE-folder ceiling check for Add Folder (D-26 + D-41).

        Phase 97 C-03: pre-scan walk runs in PrescanWorker QThread so the UI
        stays responsive during the os.walk on large trees.  A QProgressDialog
        with Cancel button is shown while the worker runs.

        Thresholds apply to the candidate folder alone (not aggregate).
        Returns True if scan should proceed, False if user cancelled.
        """
        if self._indexer is None:
            return True

        # Phase 97 C-03: run prescan in a worker thread; block UI event loop
        # via QProgressDialog (not a raw thread join) so the Cancel button works.
        result_holder: list = []   # [file_count, total_bytes] or [] on cancel/error
        error_holder: list = []    # [error_str] on worker error

        worker = PrescanWorker(self._indexer, folder_path)
        progress_dlg = QProgressDialog(
            tr("Scanning folder — please wait…"),
            tr("Cancel"),
            0, 0,   # indeterminate
            self,
        )
        progress_dlg.setWindowTitle(tr("Add folder — pre-scan"))
        progress_dlg.setMinimumDuration(0)
        progress_dlg.setValue(0)

        def _on_finished(fc: int, tb: int) -> None:
            result_holder.append((fc, tb))
            progress_dlg.accept()

        def _on_error(err: str) -> None:
            error_holder.append(err)
            progress_dlg.reject()

        def _on_cancelled() -> None:
            worker.cancel()

        worker.finished_signal.connect(_on_finished)
        worker.error_signal.connect(_on_error)
        progress_dlg.canceled.connect(_on_cancelled)
        worker.finished.connect(worker.deleteLater)

        worker.start()
        progress_dlg.exec()   # blocks UI event loop; Cancel fires _on_cancelled

        if not result_holder:
            # User cancelled or worker errored — abort add
            if error_holder:
                logger.warning("PrescanWorker error: %s", error_holder[0])
            return False

        file_count, total_bytes = result_holder[0]
        if file_count < 0:
            # Worker was cancelled mid-walk (cancel_check returned True)
            return False

        if file_count > _MAX_FILES_CEILING or total_bytes > _MAX_BYTES_CEILING:
            return self._show_ceiling_confirm_dialog(
                file_count,
                total_bytes,
                tr("Add folder — pre-scan"),
                tr("Adding folder '{}' will index {:,} files ({}).").format(
                    folder_path, file_count, self._human_bytes(total_bytes)
                ),
            )
        return True

    def _check_ceiling_refresh_aggregate(self) -> bool:
        """W8: MULTI-FOLDER aggregate ceiling check for Refresh (D-16 + D-26 + D-41).

        Iterates all registered + available folders; thresholds apply to the
        AGGREGATE sum per D-16 multi-folder support. Unavailable folders (D-40)
        are excluded from the sum.
        Returns True if scan should proceed, False if user cancelled.
        """
        if self._indexer is None:
            return True
        total_files, total_bytes = self._indexer.prescan_count_all()
        if total_files > _MAX_FILES_CEILING or total_bytes > _MAX_BYTES_CEILING:
            folder_count = sum(
                1
                for f in self._indexer.list_folders()
                if f.get("status") != "unavailable"
            )
            return self._show_ceiling_confirm_dialog(
                total_files,
                total_bytes,
                tr("Refresh — pre-scan"),
                tr("Refreshing {:d} folder(s) will index {:,} files total ({}).").format(
                    folder_count, total_files, self._human_bytes(total_bytes)
                ),
            )
        return True

    def _show_ceiling_confirm_dialog(
        self, file_count: int, total_bytes: int, title: str, body: str
    ) -> bool:
        """Shared modal dialog for ceiling confirmation (D-26 + D-41).

        Returns True if user confirmed Yes, False if Cancel.
        """
        formatted = "{}\n\n{}\n\n{}".format(
            body,
            tr("Files: {:,}").format(file_count),
            tr("Total size: {}").format(self._human_bytes(total_bytes)),
        )
        formatted += "\n\n" + tr(
            "Performance may degrade. Do you want to continue?"
        )
        reply = QMessageBox.question(
            self,
            title,
            formatted,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        return reply == QMessageBox.StandardButton.Yes

    # ------------------------------------------------------------------
    # Folder list UI (D-40)
    # ------------------------------------------------------------------

    def _refresh_folder_list_ui(self) -> None:
        """Clear and repopulate the folder QListWidget from SQLite state (D-40).

        D-40: unavailable folders shown with warning colour (#f39c12) + tooltip.

        Phase 96 fix-7 (Codex P1.1): auto-select is NO LONGER scheduled here.
        It is deferred until notify_session_restored() is called by GenizahGUI
        at the end of _restore_session(), guaranteeing opt-outs are loaded
        before populate_for_folder reads them.  Earlier fix-1/fix-2 relied on
        a 300ms timer which could still race when on_startup_finished was
        delayed (e.g. slow I/O on first launch).
        """
        if self._indexer is None:
            return
        current = self._folder_list.currentItem()
        previous_path = (
            current.data(Qt.ItemDataRole.UserRole)
            if current is not None else None
        )
        self._folder_list.clear()
        restore_row = -1
        for folder in self._indexer.list_folders():
            path = folder["path"]
            status = folder.get("status", "active")
            item = QListWidgetItem(path)
            item.setData(Qt.ItemDataRole.UserRole, path)
            if status == "unavailable":
                item.setForeground(QColor("#f39c12"))
                item.setToolTip(
                    tr(
                        "Folder not found at {} — files remain indexed from last scan."
                    ).format(path)
                )
            self._folder_list.addItem(item)
            if previous_path and path == previous_path:
                restore_row = self._folder_list.count() - 1
        # NOTE: do NOT auto-select here.  notify_session_restored() handles that
        # after opt-outs have been loaded from the session JSON.
        if restore_row >= 0:
            self._folder_list.blockSignals(True)
            self._folder_list.setCurrentRow(restore_row)
            self._folder_list.blockSignals(False)
            if hasattr(self, '_unified_tree'):
                self._unified_tree.populate_for_folder(previous_path)

    def notify_session_restored(self) -> None:
        """Called by GenizahGUI._restore_session() (in its finally block) when
        session state — including _local_file_optouts — has been loaded.

        Phase 96 fix-7 (Codex P1.1): replacing the fragile 300ms QTimer
        with an explicit post-restore callback.  Now _auto_select_first_folder
        is guaranteed to run AFTER opt-outs are populated, regardless of how
        long on_startup_finished takes.
        """
        current = self._folder_list.currentItem()
        if current is not None:
            selected_path = current.data(Qt.ItemDataRole.UserRole)
            if selected_path and hasattr(self, '_unified_tree'):
                self._unified_tree.populate_for_folder(selected_path)
            return
        self._auto_select_first_folder()

    def _auto_select_first_folder(self) -> None:
        """Select the first folder in the list (if none is selected).

        Selecting the first item triggers _on_folder_selection_changed which
        calls populate_for_folder.  At the point this is called (via
        notify_session_restored), opt-outs are already loaded, so checkboxes
        reflect the saved state correctly.

        Only auto-selects if no item is currently selected (avoids overriding
        a user click that happened before session restore completed).
        """
        if self._folder_list.count() > 0 and self._folder_list.currentRow() < 0:
            self._folder_list.setCurrentRow(0)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _human_bytes(n: int) -> str:
        """Format bytes as a human-readable string (GB / MB / KB)."""
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.1f} GB"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f} MB"
        if n >= 1_000:
            return f"{n / 1_000:.1f} KB"
        return f"{n} B"

    def _show_status_message(self, msg: str) -> None:
        """Display a non-modal toast using the parent status bar (D-25)."""
        if self._parent_window is not None:
            sb = getattr(self._parent_window, "statusBar", None)
            if callable(sb):
                try:
                    sb().showMessage(msg, 5000)
                    return
                except Exception:  # noqa: BLE001
                    pass
        # Fallback: log only
        logger.info("MyLibraryTab status: %s", msg)
