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
from typing import Optional

from PyQt6.QtCore import (
    Qt,
    QMutex,
    QThread,
    QSettings,
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
    QTableWidget,
    QTableWidgetItem,
    QFileDialog,
    QMessageBox,
    QHeaderView,
    QAbstractItemView,
)
from PyQt6.QtGui import QColor

from shared.local_indexer import LocalIndexer
from genizah_core import Config, tr

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (D-26 + D-41)
# ---------------------------------------------------------------------------
_MAX_FILES_CEILING = 5000
_MAX_BYTES_CEILING = 2 * 1024 ** 3   # 2 GB

# Column indices for the per-file status QTableWidget
_COL_FILENAME = 0
_COL_PAGES = 1
_COL_STATUS = 2


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

        # ---- Section 3: per-file status table ----
        root.addWidget(QLabel(tr("File status:")))
        self._status_table = QTableWidget(0, 3)
        self._status_table.setHorizontalHeaderLabels(
            [tr("Filename"), tr("Pages"), tr("Status")]
        )
        self._status_table.horizontalHeader().setSectionResizeMode(
            _COL_FILENAME, QHeaderView.ResizeMode.Stretch
        )
        self._status_table.horizontalHeader().setSectionResizeMode(
            _COL_PAGES, QHeaderView.ResizeMode.ResizeToContents
        )
        self._status_table.horizontalHeader().setSectionResizeMode(
            _COL_STATUS, QHeaderView.ResizeMode.ResizeToContents
        )
        self._status_table.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers
        )
        root.addWidget(self._status_table, stretch=2)

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
        self._status_table.setRowCount(0)

        self._worker = LocalIndexerWorker(self._indexer)
        self._worker.progress_updated.connect(self._on_progress_updated)
        self._worker.file_finished.connect(self._on_file_finished)
        self._worker.finished_signal.connect(
            lambda result: self._on_worker_finished(result, toast_on_complete)
        )
        self._worker.error_signal.connect(self._on_worker_error)
        self._worker.start()

    def _on_worker_finished(self, result: dict, toast: bool) -> None:
        """Worker completed: unlock mutex, reload indexes, show toast (HIGH-1 + D-25)."""
        self._progress_bar.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._btn_add.setEnabled(True)
        self._btn_remove.setEnabled(True)
        self._btn_cancel.setEnabled(False)
        self._worker = None

        # HIGH-1 review fix: reload BEFORE toast so by the time the user
        # dismisses the toast, search already picks up the new content.
        if self.search_engine is not None:
            try:
                self.search_engine.reload_local_indexes()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._on_worker_finished: reload_local_indexes failed: %s", exc
                )

        # Release mutex AFTER reload
        self._indexer_mutex.unlock()

        # Refresh folder list (status may have changed, e.g. unavailable)
        self._refresh_folder_list_ui()

        if toast:
            indexed = result.get("indexed", 0)
            msg = tr(f"My Library updated: {indexed} new files indexed")
            self._show_status_message(msg)

        # Process any queued action
        if self._queued_action is not None:
            action = self._queued_action
            self._queued_action = None
            action()

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
        """
        if self.search_engine is not None:
            try:
                self.search_engine.reload_local_indexes()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._on_startup_recovery_completed: "
                    "reload_local_indexes failed: %s", exc
                )

    def _on_rebuild_lab_completed(self) -> None:
        """HIGH-1 call site 3: called after rebuild_local_lab_index() finishes.

        Reloads LOCAL indexes (includes the LAB side-index) so the live session
        uses the freshly-built LAB fingerprints for Composition/Parallels.
        """
        if self.search_engine is not None:
            try:
                self.search_engine.reload_local_indexes()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._on_rebuild_lab_completed: "
                    "reload_local_indexes failed: %s", exc
                )

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
        if self.search_engine is not None:
            try:
                self.search_engine.reload_local_indexes()
            except Exception as exc2:  # noqa: BLE001
                logger.warning(
                    "MyLibraryTab._on_remove_folder_clicked: "
                    "reload_local_indexes failed: %s", exc2
                )

        self._refresh_folder_list_ui()

    def _on_refresh_clicked(self) -> None:
        """Aggregate ceiling check (W8) then start worker (D-25)."""
        if self._indexer is None:
            return
        # W8 — AGGREGATE ceiling check for Refresh
        if not self._check_ceiling_refresh_aggregate():
            return
        self._start_worker(toast_on_complete=False)

    def _on_cancel_clicked(self) -> None:
        """Cooperative cancel (D-24): signal the worker to stop."""
        if self._worker is not None:
            self._worker.cancel()
        self._btn_cancel.setEnabled(False)

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
        self, filename: str, status: str, pages: int, err: str
    ) -> None:
        """Append a row to the status QTableWidget (D-22 two-stage UX).

        D-22: initial status is 'Indexing...' which transitions to 'OK' once
        the batch commits. We use a simplified model where the worker emits
        the final status directly (the two-stage display is handled by the
        worker calling file_finished AFTER _finish_file returns).
        """
        row = self._status_table.rowCount()
        self._status_table.insertRow(row)

        fn_item = QTableWidgetItem(filename)
        pages_item = QTableWidgetItem(str(pages) if pages > 0 else "-")
        pages_item.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )

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

        status_item = QTableWidgetItem(display_status)

        if status in ("error", "encoding_error"):
            for item in (fn_item, pages_item, status_item):
                item.setForeground(QColor("#e74c3c"))  # red
        elif status == "cancelled":
            for item in (fn_item, pages_item, status_item):
                item.setForeground(QColor("#e67e22"))  # orange

        self._status_table.setItem(row, _COL_FILENAME, fn_item)
        self._status_table.setItem(row, _COL_PAGES, pages_item)
        self._status_table.setItem(row, _COL_STATUS, status_item)
        self._status_table.scrollToBottom()

    # ------------------------------------------------------------------
    # W8 Ceiling checks — TWO entry points
    # ------------------------------------------------------------------

    def _check_ceiling_single_folder(self, folder_path: str) -> bool:
        """W8: SINGLE-folder ceiling check for Add Folder (D-26 + D-41).

        Thresholds apply to the candidate folder alone (not aggregate).
        Returns True if scan should proceed, False if user cancelled.
        """
        if self._indexer is None:
            return True
        file_count, total_bytes = self._indexer.prescan_count(folder_path)
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
        """
        if self._indexer is None:
            return
        self._folder_list.clear()
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
