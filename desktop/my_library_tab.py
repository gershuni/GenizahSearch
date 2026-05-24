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
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTreeWidget,
    QTreeWidgetItem,
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
# Phase 96 D-F1: per-file opt-out tree widget (tri-state checkbox UI)
# ---------------------------------------------------------------------------

class _OptoutTreeWidget(QTreeWidget):
    """Phase 96 D-F1: tree of folders + files with tri-state checkboxes.

    Shows the contents of the selected indexed folder. Each file is a leaf
    with a tri-state checkbox (checked = included in search, unchecked =
    opted out). Folder nodes use Qt's native ItemIsAutoTristate so they
    show all/some/none state automatically.

    Toggling a checkbox mutates `app._local_file_optouts` (the set
    shipped by plan 96-04) using a SET-DIFFERENCE/UNION update — paths
    NOT in the currently displayed tree are LEFT UNTOUCHED (Codex HIGH #1
    closure 2026-05-24). The session-save + re-filter is debounced.
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
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.setInterval(self._DEBOUNCE_MS)
        self._save_timer.timeout.connect(self._commit_changes)
        self.setHeaderLabel(tr("Folder contents"))
        self.setColumnCount(1)
        self.itemChanged.connect(self._on_item_changed)

    def populate_for_folder(self, folder_path: str):
        """Rebuild the tree for the given indexed folder. Walks the
        filesystem (so ignored files also appear, letting the user opt
        them out preemptively before indexer touches them)."""
        import os
        self._suppress_signals = True
        try:
            self.clear()
            self._displayed_paths = set()
            optouts = getattr(self._app, '_local_file_optouts', set())
            root_item = QTreeWidgetItem(self, [os.path.basename(folder_path) or folder_path])
            root_item.setFlags(
                root_item.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsAutoTristate
            )
            # Recurse into subdirectories
            self._populate_node(root_item, folder_path, optouts)
            self.expandAll()
        finally:
            self._suppress_signals = False

    def _populate_node(self, parent_item, dirpath: str, optouts: set):
        """Recursively add files and subdirs to parent_item."""
        import os
        from shared.local_sys_id import _canonical_filepath
        SUPPORTED = {'.pdf', '.docx', '.txt'}
        try:
            entries = sorted(os.listdir(dirpath))
        except (OSError, PermissionError):
            return
        # Add subdirs first
        for name in entries:
            full = os.path.join(dirpath, name)
            if os.path.isdir(full):
                sub = QTreeWidgetItem(parent_item, [name])
                sub.setFlags(
                    sub.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsAutoTristate
                )
                self._populate_node(sub, full, optouts)
        # Add files
        for name in entries:
            full = os.path.join(dirpath, name)
            if not os.path.isfile(full):
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in SUPPORTED:
                continue
            leaf = QTreeWidgetItem(parent_item, [name])
            leaf.setFlags(leaf.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            # Codex MEDIUM #9 closure: canonicalize at populate time so the
            # value stashed in UserRole matches what _local_file_optouts holds.
            canonical = _canonical_filepath(full)
            leaf.setData(0, Qt.ItemDataRole.UserRole, canonical)
            self._displayed_paths.add(canonical)
            is_opted_out = canonical in optouts
            leaf.setCheckState(
                0,
                Qt.CheckState.Unchecked if is_opted_out else Qt.CheckState.Checked,
            )

    def _on_item_changed(self, item, column):
        """User toggled a checkbox — debounce the commit."""
        if self._suppress_signals:
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

        # ---- Section 3: per-file status table + opt-out tree (Phase 96 D-F1) ----
        # RESEARCH §3 Option 1 (minimal-blast-radius): replace the bare
        # _status_table with a QSplitter(Horizontal) containing
        # [_optout_tree, _status_table]. Outer QVBoxLayout stays unchanged.
        root.addWidget(QLabel(tr("File status & opt-outs:")))
        self._bottom_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left: opt-out tree (NEW in Phase 96)
        self._optout_tree = _OptoutTreeWidget(self._bottom_splitter, self._parent_window)
        self._bottom_splitter.addWidget(self._optout_tree)

        # Right: status table (PRESERVE all existing setup verbatim, only parent changed)
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
        self._bottom_splitter.addWidget(self._status_table)

        # 40/60 initial split — user can drag the handle.
        self._bottom_splitter.setSizes([400, 600])
        root.addWidget(self._bottom_splitter, stretch=2)

        # Phase 96 D-F1: when the user picks a folder, repopulate the opt-out tree.
        # PINNED: selected-path comes from Qt.ItemDataRole.UserRole on the QListWidgetItem
        # (Phase 95 stashes the folder path string here when the folder is added to the list).
        self._folder_list.currentItemChanged.connect(self._on_folder_selection_changed)

    # ------------------------------------------------------------------
    # Phase 96 D-F1: folder selection handler (tree repopulation)
    # ------------------------------------------------------------------

    def _on_folder_selection_changed(self, current, previous):
        """Phase 96 D-F1: repopulate the opt-out tree when the user selects a folder.

        PINNED (96-08-WIRING-NOTES.md §Plan 96-06 wiring):
          - selected-path extraction: item.data(Qt.ItemDataRole.UserRole)
          - DO NOT use item.text() — that is the display label, not the fs path.
        """
        if not hasattr(self, '_optout_tree'):
            return
        if current is None:
            return
        selected_path = current.data(Qt.ItemDataRole.UserRole)
        if selected_path:
            self._optout_tree.populate_for_folder(selected_path)

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
