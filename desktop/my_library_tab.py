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
    # Phase 97.2 R97.2-E — Reset My Library typed-confirm dialog
    QDialog,
    QLineEdit,
    QApplication,
)
from PyQt6.QtGui import QColor

from shared.local_indexer import LocalIndexer, _SUPPORTED_EXTENSIONS, is_office_temp_file
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
        # Phase 97.3 R97.3-A: async tree-worker state
        # Monotonic generation token — incremented on every populate_for_folder.
        # Workers stamp this at construction; stale batches/signals are dropped.
        self._tree_token: int = 0
        # Distinct from MyLibraryTab._worker (indexing). Lifetime: created on
        # populate_for_folder, finished.connect(deleteLater).
        self._tree_worker: Optional["FolderWalkWorker"] = None
        # Phase 97.3 R97.3-A (Codex Critique #3 HIGH fix): retention list for
        # canceled tree-workers. Setting self._tree_worker = None as the SOLE
        # Python reference to a still-running QThread is undefined behavior in
        # PyQt6 even with finished.connect(deleteLater). Retired workers stay
        # in this list until their finished_signal fires; the slot then pops
        # them out. Bounded by the cancel-rate; typically 0-2 entries.
        self._retired_tree_workers: list = []
        # Cache the folder path + parent items so batch slot can build subdir
        # nodes incrementally without re-walking.
        self._tree_folder_path: Optional[str] = None
        # canonical_dirpath -> QTreeWidgetItem (for incremental subdir node creation)
        self._dir_nodes: dict = {}
        # Prior-status dict for the current populate (resolved at start)
        self._current_prior_status: dict = {}
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
        """Phase 97.3 R97.3-A — async tree population off the UI thread.

        Replaces the Phase 96 synchronous _populate_node recursion. Walks
        the filesystem in a FolderWalkWorker (QThread), drops unsupported
        files BEFORE _canonical_filepath (D-02), and updates the tree
        incrementally via batch_emitted → _on_tree_batch slot.

        Tree starts collapsed (D-04 — never expandAll). User expands subtrees
        manually.

        prior_status (optional): canonical_filepath -> {'pages': int, 'status': str}
        from the cached LocalIndexer state. When omitted, the MyLibraryTab's
        _prior_status_cache (D-12) supplies values without a DB lookup on the
        click path.
        """
        import os
        # D-03 + D-17: increment token FIRST so any in-flight stale batches
        # from a previous worker are dropped on arrival.
        self._tree_token += 1
        current_token = self._tree_token

        # D-05: cancel any in-flight worker + clear the tree entirely (no
        # partial-results state machine).
        self._cancel_existing_tree_worker()

        self._suppress_signals = True
        try:
            self.clear()
            self._displayed_paths = set()
            self._leaf_by_path = {}
            self._dir_nodes = {}
            self._tree_folder_path = folder_path

            # Build the root node now so the user sees immediate feedback
            # (folder name shown collapsed; children fill in as batches arrive).
            root_label = os.path.basename(folder_path) or folder_path
            root_item = QTreeWidgetItem(self, [root_label, '', ''])
            root_item.setFlags(
                root_item.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsAutoTristate
            )
            root_item.setData(0, Qt.ItemDataRole.UserRole, folder_path)
            # Store the canonical folder path so subdir keys can be relative.
            self._dir_nodes[os.path.normcase(os.path.normpath(folder_path))] = root_item
        finally:
            self._suppress_signals = False

        # Resolve prior_status: explicit kwarg > tab cache > empty dict
        # (D-12 — never issue a fresh DB query on the click path).
        if prior_status is None:
            tab = getattr(self._app, "my_library_tab", None) or self._app
            cache = getattr(tab, "_prior_status_cache", None)
            prior_status = (cache or {}).get(folder_path, {}) if isinstance(cache, dict) else {}
        self._current_prior_status = prior_status or {}

        # Launch the worker (D-01).
        try:
            self._tree_worker = FolderWalkWorker([folder_path], token=current_token)
            self._tree_worker.batch_emitted.connect(self._on_tree_batch)
            self._tree_worker.finished_signal.connect(self._on_tree_finished)
            self._tree_worker.error_signal.connect(self._on_tree_error)
            # D-18: cleanup Qt resources when the thread exits.
            self._tree_worker.finished.connect(self._tree_worker.deleteLater)
            self._tree_worker.start()
            # Phase 97.3 R97.3-A (Codex Critique #3 HIGH fix) — notify the tab
            # so it enables _btn_cancel for tree-population cancel.
            try:
                tab = getattr(self._app, "my_library_tab", None) or self._app
                if hasattr(tab, "_on_tree_population_started"):
                    tab._on_tree_population_started()
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            logger.exception("populate_for_folder: launching FolderWalkWorker failed")
            self._tree_worker = None

    # ------------------------------------------------------------------
    # Phase 97.3 R97.3-A — async tree-worker helpers
    # ------------------------------------------------------------------

    def _cancel_existing_tree_worker(self) -> None:
        """D-05 + D-18 + Codex Critique #3 HIGH fix: cancel + RETAIN until finished.

        The prior draft set ``self._tree_worker = None`` immediately after
        ``.cancel()``, dropping the last Python reference to a still-running
        QThread. PyQt6's ``finished.connect(deleteLater)`` keeps Qt alive but
        losing the Python ref is undefined behavior. The retention pattern:
        move the prev worker into ``self._retired_tree_workers``; the worker's
        ``finished_signal`` slot (``_on_tree_finished`` / ``_on_tree_error``)
        removes it from the list AFTER the thread has exited. The current
        ``_tree_worker`` slot is freed for a new walker.
        """
        prev = self._tree_worker
        if prev is None:
            return
        try:
            prev.cancel()
        except Exception:
            pass
        # Disconnect the BATCH slot (so stale batches don't waste UI cycles)
        # but KEEP finished_signal / error_signal connected — they're how we
        # know to pop prev from _retired_tree_workers after it exits.
        try:
            prev.batch_emitted.disconnect(self._on_tree_batch)
        except (TypeError, RuntimeError):
            pass
        # finished_signal / error_signal STAY CONNECTED. Token guard inside
        # those slots will short-circuit any stale state mutation; their only
        # remaining job is to release `prev` from _retired_tree_workers.
        self._retired_tree_workers.append(prev)
        self._tree_worker = None

    def _ensure_dir_node(self, canonical_dirpath: str) -> "QTreeWidgetItem":
        """Walk up from canonical_dirpath creating intermediate folder nodes.

        Returns the QTreeWidgetItem for the directory. Uses self._dir_nodes
        as a memo so repeat lookups are O(1).
        """
        import os
        key = os.path.normcase(os.path.normpath(canonical_dirpath))
        cached = self._dir_nodes.get(key)
        if cached is not None:
            return cached
        # Recurse up. The root key was inserted in populate_for_folder.
        parent_path = os.path.dirname(canonical_dirpath)
        if parent_path == canonical_dirpath or not parent_path:
            # Reached filesystem root without hitting the known root node —
            # fall back to the invisible root.
            return self.invisibleRootItem()
        parent_node = self._ensure_dir_node(parent_path)
        name = os.path.basename(canonical_dirpath)
        node = QTreeWidgetItem(parent_node, [name, '', ''])
        node.setFlags(
            node.flags()
            | Qt.ItemFlag.ItemIsUserCheckable
            | Qt.ItemFlag.ItemIsAutoTristate
        )
        node.setData(0, Qt.ItemDataRole.UserRole, canonical_dirpath)
        self._dir_nodes[key] = node
        return node

    def _build_leaf_item_status(self, prior_st: str, prior_pages: int) -> tuple:
        """Translate stored status codes to display strings + color tag.

        Returns (pages_str, status_str, color_hex_or_None). Preserves the
        bilingual EN/HE mapping from the pre-Phase-97.3 _populate_node body.
        """
        pages_str = str(prior_pages) if prior_pages and prior_pages > 0 else ''
        if prior_st == 'ok':
            return pages_str, tr("OK"), None
        if prior_st == 'cancelled':
            return pages_str, tr("Cancelled"), '#e67e22'
        if prior_st == 'no_text_layer':
            return pages_str, tr("No text layer"), None
        if prior_st == 'encoding_error':
            return pages_str, tr("Encoding error"), '#e74c3c'
        if prior_st == 'unsupported':
            return pages_str, tr("Unsupported"), None
        if prior_st == 'oversized':
            label = "גדול מדי (>100 מ\"ב)" if CURRENT_LANG == 'he' else tr("Too large (>100 MB)")
            return pages_str, label, '#e67e22'
        if prior_st == 'zip_bomb_suspected':
            label = "ארכיון חשוד" if CURRENT_LANG == 'he' else tr("Suspicious archive")
            return pages_str, label, '#e67e22'
        if prior_st == 'error':
            return pages_str, tr("Error"), '#e74c3c'
        return pages_str, '', None

    def _on_tree_batch(self, batch: list, token: int) -> None:
        """Worker emitted a batch — apply to tree IF token matches current.

        D-13 + D-17: stale batches are dropped. Each item is a 4-tuple
        (filepath, canonical, mtime_ns, size). Builds intermediate dir nodes
        on demand via _ensure_dir_node so the tree shape mirrors the
        filesystem.
        """
        if token != self._tree_token:
            return
        import os
        optouts = getattr(self._app, '_local_file_optouts', set()) or set()
        prior_status = getattr(self, '_current_prior_status', {}) or {}
        self._suppress_signals = True
        try:
            for filepath, canonical, _mtime_ns, _size in batch:
                # Determine parent dir node.
                parent_dirpath = os.path.dirname(filepath)
                parent_node = self._ensure_dir_node(parent_dirpath)
                name = os.path.basename(filepath)
                file_info = prior_status.get(canonical, {}) if isinstance(prior_status, dict) else {}
                prior_pages = file_info.get('pages', 0)
                prior_st = file_info.get('status', '')
                pages_str, status_str, color = self._build_leaf_item_status(
                    prior_st, prior_pages,
                )
                leaf = QTreeWidgetItem(parent_node, [name, pages_str, status_str])
                leaf.setFlags(leaf.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                if color is not None:
                    qc = QColor(color)
                    for col in range(3):
                        leaf.setForeground(col, qc)
                leaf.setData(0, Qt.ItemDataRole.UserRole, canonical)
                self._displayed_paths.add(canonical)
                self._leaf_by_path[canonical] = leaf
                is_opted_out = canonical in optouts
                leaf.setCheckState(
                    0,
                    Qt.CheckState.Unchecked if is_opted_out else Qt.CheckState.Checked,
                )
        finally:
            self._suppress_signals = False

    def _release_finished_worker(self, sender) -> None:
        """Codex Critique #3 HIGH fix: pop the just-finished worker from the
        retention list AND clear ``_tree_worker`` if it's the current one.

        Called from ``_on_tree_finished`` / ``_on_tree_error`` for BOTH current
        and retired workers — the token guard short-circuits state mutation,
        but the worker reference still needs to be released so the retention
        list does not grow unboundedly.
        """
        try:
            if sender in self._retired_tree_workers:
                self._retired_tree_workers.remove(sender)
        except (ValueError, RuntimeError):
            pass
        if self._tree_worker is sender:
            self._tree_worker = None
        # Notify the tab so it disables _btn_cancel for tree-population (if no
        # scan worker is also running).
        try:
            tab = getattr(self._app, "my_library_tab", None) or self._app
            if hasattr(tab, "_on_tree_population_ended"):
                tab._on_tree_population_ended()
        except Exception:  # noqa: BLE001
            pass

    def _on_tree_finished(self, total_files: int, total_bytes: int, token: int) -> None:
        """Worker finished — release the handle.

        Token-guard: stale ``finished_signal`` from a superseded worker does NOT
        mutate widget state, but it STILL releases the worker reference (D-17 +
        Codex Critique #3 HIGH retention fix).
        """
        # D-04: do NOT call self.expandAll(). User expands manually.
        self._release_finished_worker(self.sender())

    def _on_tree_error(self, msg: str, token: int) -> None:
        """Worker errored — log and release handle."""
        logger.warning("FolderWalkWorker error: %s", msg)
        self._release_finished_worker(self.sender())

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
    # Phase 97.3 R97.3-E (D-07) — phase status text (bilingual).
    status_updated = pyqtSignal(str)

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

            # Phase 97.3 R97.3-E (D-07): emit "Discovering files…" BEFORE scan_all
            # enters its enumeration loop. Emitting at the QThread wrapper layer
            # (not via an indexer callback) keeps scan_all untouched and ensures
            # the message shows immediately on worker start.
            self.status_updated.emit("Discovering files… / מאתר קבצים…")

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

    # qlonglong (64-bit): total_bytes overflows a 32-bit C++ int for folders
    # larger than ~2 GB, which would truncate the byte count and break the
    # _MAX_BYTES_CEILING check across the queued cross-thread connection.
    finished_signal = pyqtSignal('qlonglong', 'qlonglong')  # file_count, total_bytes
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
    """Phase 97 U-03 + Phase 97.3 R97.3-A — off-thread filesystem walk.

    Phase 97.3 (D-01 + D-02 + D-17):
      - Emits 4-tuple (filepath, canonical, mtime_ns, size) + token int.
      - Pre-filters by _SUPPORTED_EXTENSIONS imported from shared.local_indexer
        (single source of truth — closes R97.3-N).
      - Drops unsupported files BEFORE os.stat() and BEFORE _canonical_filepath()
        so unsupported subtrees pay zero Path.resolve tax (D-14 acceptance).
      - All three signals (batch_emitted, finished_signal, error_signal) carry
        the token; UI slot drops payloads whose token != current widget token
        (stale-worker guard — D-13 / D-17).

    CRITICAL: NO QWidget mutation in this thread (test_no_widget_mutation
    AST guard pins this invariant). Only pyqtSignal emissions.

    Per-file failure policy (D-16): supported-file os.stat / canonicalization
    errors and inaccessible subdirs are caught and logged at WARNING; only
    _cancel_requested aborts the walk.
    """

    # 4-tuple list + token int (D-01 + D-17)
    batch_emitted = pyqtSignal(list, int)        # list of (filepath, canonical, mtime_ns, size); token
    finished_signal = pyqtSignal(int, int, int)  # total_files, total_bytes, token
    error_signal = pyqtSignal(str, int)          # message; token

    BATCH_SIZE = 100    # emit after this many files
    BATCH_TIMEOUT = 0.5  # or after this many seconds

    def __init__(self, folder_paths: list, token: int = 0) -> None:
        super().__init__()
        self._folder_paths = folder_paths
        self._cancel_requested = False
        self._token = int(token)

    @property
    def token(self) -> int:
        """Generation token captured at construction (D-03 + D-17)."""
        return self._token

    def cancel(self) -> None:
        """Request cooperative cancellation."""
        self._cancel_requested = True

    def run(self) -> None:
        """Walk folders and emit batched supported-file records.

        This method must NOT mutate any QWidget — it only emits signals.
        AST guard: test_folder_walk_worker.py::test_no_widget_mutation.
        """
        import os as _os
        import time as _time
        # D-02: import canonicalizer lazily to keep test monkeypatching simple.
        from shared.local_sys_id import _canonical_filepath

        batch: list = []
        last_emit = _time.monotonic()
        total_files = 0
        total_bytes = 0
        token = self._token

        # Codex Critique #3 MEDIUM fix — D-16 skip+log+continue policy must
        # cover scandir errors during os.walk too, not just per-file stat /
        # canonicalize errors. Without onerror=, os.walk silently swallows
        # PermissionError / OSError on inaccessible subdirs.
        def _walk_onerror(exc: OSError) -> None:
            logger.warning(
                "FolderWalkWorker: os.walk failed for %s: %s",
                getattr(exc, "filename", "?"), exc,
            )

        # D-15 Rule 2 deviation — Windows junctions (FILE_ATTRIBUTE_REPARSE_POINT)
        # are NOT detected by os.path.islink() on Python 3.11, so the literal
        # `followlinks=False` keyword does not stop os.walk from recursing into
        # them. We must explicitly prune reparse-point dirs from `dirs` inside
        # the walk loop. POSIX symlinks ARE caught by followlinks=False as
        # documented. This closes the must_haves invariant "Clicking a folder
        # containing a Windows junction does NOT recurse into the junction
        # target" which the plan's literal `followlinks=False` alone cannot
        # satisfy on Windows.
        _FILE_ATTRIBUTE_REPARSE_POINT = 0x400

        def _is_reparse_point(path: str) -> bool:
            try:
                st = _os.stat(path, follow_symlinks=False)
            except OSError:
                return False
            attrs = getattr(st, "st_file_attributes", 0)
            return bool(attrs & _FILE_ATTRIBUTE_REPARSE_POINT)

        try:
            for folder in self._folder_paths:
                if self._cancel_requested:
                    break
                # D-15: followlinks=False prevents recursion into POSIX symlinks.
                # D-16: onerror logs inaccessible subdirs at WARNING (don't abort).
                for root, dirs, files in _os.walk(
                    folder, followlinks=False, onerror=_walk_onerror,
                ):
                    if self._cancel_requested:
                        break
                    # D-15 — Prune Windows reparse-point (junction) dirs in place
                    # so os.walk does not descend into them on the next iteration.
                    # In-place mutation of `dirs` is the documented os.walk hook.
                    pruned = [
                        d for d in dirs
                        if not _is_reparse_point(_os.path.join(root, d))
                    ]
                    if len(pruned) != len(dirs):
                        for d in dirs:
                            if d not in pruned:
                                logger.warning(
                                    "FolderWalkWorker: pruning reparse-point "
                                    "(junction/symlink) dir %s",
                                    _os.path.join(root, d),
                                )
                        dirs[:] = pruned
                    for name in files:
                        if self._cancel_requested:
                            break
                        # Office/LibreOffice lock files (~$foo.docx) are transient
                        # non-documents — drop before any stat/extension work so
                        # they never appear in the opt-out tree.
                        if is_office_temp_file(name):
                            continue
                        # D-02: extension pre-filter — DROP before stat/canonical.
                        # _SUPPORTED_EXTENSIONS is the DELIBERATE R97.3-N opt-out
                        # surface (single source of truth) and intentionally
                        # broader than the indexer's extraction set: it also lists
                        # .html/.xlsx/.csv. NOT all of these have an extraction
                        # codepath in LocalIndexer.scan_all yet (.pdf/.docx/.txt
                        # do); rows for the others may show an empty/unsupported
                        # Status until extraction lands. This is expected — do NOT
                        # narrow this filter back to the old 3-extension literal
                        # (that would revert R97.3-N). See WR-02 (Phase 97.3 review).
                        ext = _os.path.splitext(name)[1].lower()
                        if ext not in _SUPPORTED_EXTENSIONS:
                            continue
                        fp = _os.path.join(root, name)
                        try:
                            stat = _os.stat(fp)
                        except OSError as exc:
                            # D-16: skip + log + continue
                            logger.warning(
                                "FolderWalkWorker: stat failed for %s: %s", fp, exc
                            )
                            continue
                        try:
                            canonical = _canonical_filepath(fp)
                        except (OSError, ValueError) as exc:
                            # D-16: skip + log + continue
                            logger.warning(
                                "FolderWalkWorker: canonicalize failed for %s: %s",
                                fp, exc,
                            )
                            continue
                        batch.append((fp, canonical, stat.st_mtime_ns, stat.st_size))
                        total_files += 1
                        total_bytes += stat.st_size
                        now = _time.monotonic()
                        if (
                            len(batch) >= self.BATCH_SIZE
                            or (now - last_emit) >= self.BATCH_TIMEOUT
                        ):
                            self.batch_emitted.emit(batch, token)
                            batch = []
                            last_emit = now
            if batch:
                self.batch_emitted.emit(batch, token)
            self.finished_signal.emit(total_files, total_bytes, token)
        except Exception as exc:  # noqa: BLE001
            logger.exception("FolderWalkWorker: unhandled error")
            self.error_signal.emit(str(exc), token)


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

        # Phase 97.3 R97.3-D (D-09): one-shot flag — set by _show_recovery_modal
        # Skip branch, read-and-cleared by _auto_rescan_on_startup on first call.
        # Initialised here BEFORE _build_ui / _init_indexer / recovery probe so
        # the attribute always exists even on init failure paths.
        self._skip_startup_rescan_once: bool = False

        # Phase 97.3 R97.3-A (D-12) — in-memory prior_status cache so the
        # folder-click code path never issues a fresh local_files DB query.
        # Structure: {folder_path: {canonical_filepath: {"pages": int, "status": str}}}
        # Invalidated + reloaded BEFORE _refresh_folder_list_ui in worker
        # finish/error/reset/folder-add/folder-remove paths.
        self._prior_status_cache: dict = {}

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

        # Phase 97.2 R97.2-E — initial Reset button state after recovery probe.
        # Constructor-end call site #1 of 3 (REVIEWS Codex MEDIUM proactive).
        self._update_reset_button_state()

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
                # LAB weights live on the LabEngine — pass it so the rebuilt
                # index's weights_hash matches the freshness check (else stale loop).
                search.rebuild_local_lab_index(self._indexer, lab_engine=lab)
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
        # Phase 97.2 R97.2-E — Reset My Library destructive action (right end).
        # Button starts DISABLED (REVIEWS Codex MEDIUM proactive button state);
        # _update_reset_button_state() enables/disables based on
        # (a) self._worker.isRunning() AND (b) start_recovery_probe() result.
        self._btn_reset = QPushButton(tr("Reset My Library"))
        self._btn_reset.setStyleSheet(
            "QPushButton { color: #c0392b; font-weight: bold; } "
            "QPushButton:disabled { color: #888; font-weight: normal; }"
        )
        self._btn_reset.setEnabled(False)
        self._btn_reset.setToolTip(tr("Stop or resolve the active scan first"))
        self._btn_reset.clicked.connect(self._on_reset_clicked)
        refresh_row.addWidget(self._btn_reset)
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

        # Phase 97.3 R97.3-A (D-12): populate prior_status cache at startup so
        # the FIRST folder click already hits the cache, not the DB.
        self._invalidate_prior_status_cache()

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
        # Apply the decision to EVERY interrupted run, not just the most recent.
        # start_recovery_probe() returns all status='running' rows; resolving only
        # the first one left the rest 'running', so the modal reappeared on the
        # next launch (orphans accumulate from hard kills / crashes — e.g. the
        # 2026-05-25 NLI-hang SIGKILL — and the clean-shutdown sweep had been dead
        # code). Clearing them all here makes one decision a clean slate. The probe
        # runs before _auto_rescan_on_startup, so no live scan is in flight.
        if clicked is btn_restart:
            # Phase 97 Wave E: discard_run() removes all four row sources (LD-7).
            for run_id in running_runs:
                try:
                    self._indexer.discard_run(run_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("_show_recovery_modal: discard_run(restart) failed for %s: %s", run_id, exc)
                    try:
                        self._indexer._end_scan_run(run_id, "canceled")
                    except Exception:
                        pass
        elif clicked is btn_skip:
            # Skip: leave partial data in place but mark runs as canceled so the
            # recovery probe doesn't trigger again on next start.
            for run_id in running_runs:
                try:
                    self._indexer._end_scan_run(run_id, "canceled")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("_show_recovery_modal: _end_scan_run(skip) failed for %s: %s", run_id, exc)
            # Phase 97.3 R97.3-D (D-09): suppress same-launch auto-rescan.
            self._skip_startup_rescan_once = True
            # Phase 97.3 R97.3-D (D-08): bilingual auto-fading status-bar message (5s).
            try:
                if self._parent_window is not None and hasattr(self._parent_window, "statusBar"):
                    self._parent_window.statusBar().showMessage(
                        "Recovery skipped. Use Refresh to rescan. / "
                        "ההתאוששות דולגה. לחץ Refresh לסריקה מחדש.",
                        5000,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Phase 97.3 D-08 status message failed: %s", exc)
        else:
            # Resume — full resume logic deferred; mark completed so gate lifts.
            for run_id in running_runs:
                try:
                    self._indexer._end_scan_run(run_id, "completed")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("_show_recovery_modal: _end_scan_run(resume) failed for %s: %s", run_id, exc)
        self.is_searchable = True

    def sweep_running_scan_runs(self) -> None:
        """Phase 97 LD-6: clean-shutdown sweep — mark any still-running scan_runs
        as completed so they don't trigger the recovery modal on next startup.

        MUST be called from GenizahGUI.closeEvent: MyLibraryTab is a child widget,
        not a top-level window, so Qt never delivers closeEvent to it on app exit
        — relying on self.closeEvent left the sweep as dead code and let orphan
        'running' rows accumulate across hard exits.
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

    def closeEvent(self, event) -> None:
        """Defensive: if this tab is ever shown as a top-level window, still sweep.
        On normal app exit GenizahGUI.closeEvent calls sweep_running_scan_runs()
        directly because Qt does not deliver closeEvent to child widgets.
        """
        self.sweep_running_scan_runs()
        super().closeEvent(event)

    def _auto_rescan_on_startup(self) -> None:
        """D-25: silent background rescan; non-modal toast on completion.

        Phase 97.3 R97.3-D (D-09): if the user clicked Skip on the recovery
        modal earlier in this constructor sequence, the one-shot
        _skip_startup_rescan_once flag will be True. Consume the flag (so a
        subsequent manual Refresh works normally) and return early. The D-08
        status message was already surfaced by the Skip branch — we do not
        repeat it here.
        """
        # Phase 97.3 R97.3-D (D-09): consume one-shot flag.
        if getattr(self, "_skip_startup_rescan_once", False):
            self._skip_startup_rescan_once = False
            return
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
        # Phase 97.3 R97.3-E (D-06): indeterminate "busy" mode during enumeration.
        # Reset to determinate (0, 100) on first progress_updated signal.
        self._progress_bar.setVisible(True)
        self._progress_bar.setRange(0, 0)   # busy mode
        self._progress_bar.setValue(0)
        # Phase 97.3 R97.3-E (D-07): bilingual status via parent statusBar.
        try:
            if self._parent_window is not None and hasattr(self._parent_window, "statusBar"):
                self._parent_window.statusBar().showMessage(
                    "Discovering files… / מאתר קבצים…", 0,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Phase 97.3 D-07 status message failed: %s", exc)
        # 96-09 bug #1: unified tree replaces _status_table; clear Pages/Status columns.
        if hasattr(self, '_unified_tree'):
            self._unified_tree.reset_for_scan()

        self._worker = LocalIndexerWorker(self._indexer)
        self._worker.progress_updated.connect(self._on_progress_updated)
        self._worker.file_finished.connect(self._on_file_finished)
        # Phase 97.3 R97.3-E (D-07): forward worker phase text to status bar.
        self._worker.status_updated.connect(self._on_worker_status_updated)
        self._worker.finished_signal.connect(
            lambda result: self._on_worker_finished(result, toast_on_complete)
        )
        self._worker.error_signal.connect(self._on_worker_error)
        self._worker.start()
        # Phase 97.2 R97.2-E — Reset must be disabled while a scan runs.
        self._update_reset_button_state()

    def _on_worker_finished(self, result: dict, toast: bool) -> None:
        """Worker completed: unlock mutex, reload indexes, always show summary (HIGH-1 + D-25).

        B2 feedback fix: always show a summary status message after every Refresh
        (including the zero-work case where no files needed re-indexing).  The
        progress bar hits 100% momentarily before being hidden so the user can
        see completion even when the status table remains empty.
        """
        # Phase 97.3 R97.3-E (D-21): reset range so a future scan does not
        # inherit busy state.
        self._progress_bar.setRange(0, 100)
        # B2: briefly show 100% so the bar visually completes before disappearing
        self._progress_bar.setValue(100)
        self._progress_bar.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._btn_add.setEnabled(True)
        self._btn_remove.setEnabled(True)
        self._btn_cancel.setEnabled(False)
        self._worker = None
        # Phase 97.2 R97.2-E — re-enable Reset now that scan is done (subject
        # to the start_recovery_probe re-check inside _update_reset_button_state).
        self._update_reset_button_state()

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

        # Phase 97.3 R97.3-A (D-12, D-19): cache must be reloaded BEFORE
        # _refresh_folder_list_ui (which calls populate_for_folder).
        self._invalidate_prior_status_cache()

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
        # Phase 97.3 R97.3-E (D-21): reset range so a future scan starts clean.
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setVisible(False)
        self._btn_refresh.setEnabled(True)
        self._btn_add.setEnabled(True)
        self._btn_remove.setEnabled(True)
        self._btn_cancel.setEnabled(False)
        self._worker = None
        self._indexer_mutex.unlock()
        # Phase 97.2 R97.2-E — re-enable Reset after worker errored
        # (start_recovery_probe will gate this if orphan rows persist).
        self._update_reset_button_state()
        # Phase 97.3 R97.3-A (D-12): error path also invalidates the cache so
        # any subsequent folder click reflects the latest DB state.
        self._invalidate_prior_status_cache()
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
    # Phase 97.3 R97.3-A (Codex Critique #3 HIGH fix) — Cancel-button wiring
    # for tree-population.
    # ------------------------------------------------------------------

    def _on_tree_population_started(self) -> None:
        """Enable Cancel button so user can abort tree-population.

        Called by ``_UnifiedFileTreeWidget.populate_for_folder`` when a worker
        is launched. Idempotent: if ``_btn_cancel`` is already enabled (e.g.
        scan running), the call is a no-op.
        """
        try:
            if hasattr(self, "_btn_cancel") and self._btn_cancel is not None:
                self._btn_cancel.setEnabled(True)
        except Exception:  # noqa: BLE001
            pass

    def _on_tree_population_ended(self) -> None:
        """Disable Cancel button when tree-population finishes AND no scan
        worker is running AND no NEWER tree-worker is still active.

        Called by ``_UnifiedFileTreeWidget._release_finished_worker``.

        WR-01 (Phase 97.3 review) — a stale ``finished_signal`` from a
        superseded tree-worker must NOT disable Cancel while a newer
        tree-worker (created during rapid folder-switching) is still walking
        the filesystem. Short-circuit on BOTH the scan worker (``self._worker``)
        AND the current tree worker (``self._unified_tree._tree_worker``).
        """
        try:
            # Don't disable while a NEWER tree worker is still active.
            tree_worker = getattr(self._unified_tree, "_tree_worker", None)
            tree_still_running = (
                tree_worker is not None
                and hasattr(tree_worker, "isRunning")
                and tree_worker.isRunning()
            )
            scan_running = self._worker is not None and self._worker.isRunning()
            if not scan_running and not tree_still_running:
                if hasattr(self, "_btn_cancel") and self._btn_cancel is not None:
                    self._btn_cancel.setEnabled(False)
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Phase 97.2 R97.2-E — Reset My Library destructive recovery action
    # ------------------------------------------------------------------

    def _update_reset_button_state(self) -> None:
        """Phase 97.3 R97.3-B (D-10) — single-condition guard.

        Enabled WHEN no worker is actively running. Disabled WHILE a worker
        runs. The start_recovery_probe() check was REMOVED — orphan
        scan_runs rows are precisely what Reset is supposed to clean up, and
        LocalIndexer.reset_my_library()'s own 7-step protocol (path-safety
        pre-check + handle-close + retry-rename + LAB-rollback + fail-loud +
        deferred-GC) is the load-bearing safety. The UI guard does not
        duplicate it.

        Called from:
          - constructor end (after recovery-probe runs)
          - _start_worker (scan begins)
          - _on_worker_finished / _on_worker_error (scan ends)
        """
        if not hasattr(self, "_btn_reset") or self._btn_reset is None:
            return
        worker_running = self._worker is not None and self._worker.isRunning()
        if worker_running:
            self._btn_reset.setEnabled(False)
            # Always-bilingual tooltip (matches the enabled-tooltip pattern below):
            # do NOT wrap the English half in tr(), or on a Hebrew-locale machine
            # it collapses to "Hebrew / Hebrew" instead of showing both languages.
            self._btn_reset.setToolTip(
                "Stop or resolve the active scan first"
                + " / "
                + "עצור או פתור את הסריקה הפעילה תחילה"
            )
            return
        # Worker idle — enable with reassuring bilingual tooltip.
        # Phase 97.2 invariant: source files and Genizah corpus are preserved.
        self._btn_reset.setEnabled(True)
        en_tip = (
            "Reset deletes LOCAL/LAB index data only. "
            "Source files and Genizah corpus are preserved."
        )
        he_tip = (
            "האיפוס מוחק רק את נתוני האינדקס המקומי. "
            "קבצי המקור וקורפוס הגניזה נשמרים."
        )
        self._btn_reset.setToolTip(en_tip + " / " + he_tip)

    def _on_reset_clicked(self) -> None:
        """Phase 97.2 R97.2-E — re-check guards then open two-step confirm dialog.

        The button SHOULD already be disabled when guards fail (see
        _update_reset_button_state) — this on-click check is defense-in-depth
        in case the user clicks during a state transition where the helper has
        not been called yet.
        """
        # Defense-in-depth re-check (the helper should have disabled the button
        # already, but state transitions may race a click).
        self._update_reset_button_state()
        if not self._btn_reset.isEnabled():
            # Helper already updated tooltip — surface the disabled reason via
            # a brief informational popup ONLY if the user explicitly clicked
            # the disabled button (Qt does not normally fire clicked on a
            # disabled button, so this branch is mostly unreachable; kept for
            # safety).
            QMessageBox.information(
                self,
                tr("Reset My Library"),
                tr("Stop or resolve the active scan first"),
            )
            return
        self._show_reset_confirm_dialog()

    def _show_reset_confirm_dialog(self) -> None:
        """Phase 97.2 D-04 — two-step typed confirm.

        Reset button enabled only when QLineEdit text is 'RESET' (EN) or
        'אפס' (HE). Both accepted regardless of CURRENT_LANG so a HE-locale
        user can type RESET if their keyboard is in English at that moment,
        and vice versa.
        """
        dlg = QDialog(self)
        if CURRENT_LANG == "he":
            dlg.setWindowTitle("אפס ספריה שלי")
            header = "אזהרה: פעולה זו תמחק את כל המידע של 'ספריה שלי'"
            explain = (
                "ההגדרה תתאפס לחלוטין. קבצי המקור שלך לא יושפעו. "
                "הקלד אפס כדי להפעיל את כפתור האיפוס."
            )
            placeholder = "הקלד אפס"
            btn_cancel_text = "ביטול"
            btn_reset_text = "אפס"
        else:
            dlg.setWindowTitle("Reset My Library")
            header = "WARNING: this will erase all 'My Library' index data"
            explain = (
                "Your source files will not be affected. "
                "Type RESET to enable the reset button."
            )
            placeholder = "Type RESET"
            btn_cancel_text = "Cancel"
            btn_reset_text = "Reset"

        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel(header))
        explain_lbl = QLabel(explain)
        explain_lbl.setWordWrap(True)
        layout.addWidget(explain_lbl)

        line = QLineEdit()
        line.setPlaceholderText(placeholder)
        layout.addWidget(line)

        btn_row = QHBoxLayout()
        btn_cancel = QPushButton(btn_cancel_text)
        btn_reset = QPushButton(btn_reset_text)
        btn_reset.setEnabled(False)
        btn_reset.setStyleSheet(
            "QPushButton:enabled { color: #c0392b; font-weight: bold; }"
        )
        btn_row.addWidget(btn_cancel)
        btn_row.addStretch()
        btn_row.addWidget(btn_reset)
        layout.addLayout(btn_row)

        def _on_text_changed(txt: str) -> None:
            v = txt.strip().upper()
            # Accept both EN ("RESET") and HE ("אפס") regardless of CURRENT_LANG.
            btn_reset.setEnabled(v == "RESET" or v == "אפס")

        line.textChanged.connect(_on_text_changed)
        btn_cancel.clicked.connect(dlg.reject)
        btn_reset.clicked.connect(dlg.accept)
        # Cancel is the safe default — user MUST explicitly tab+click Reset.
        btn_cancel.setDefault(True)
        btn_cancel.setAutoDefault(True)
        btn_reset.setAutoDefault(False)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._perform_reset()

    def _perform_reset(self) -> None:
        """Phase 97.2 R97.2-E — invoke reset_my_library with progress dialog.

        Runs on UI thread per CONTEXT D-05 cost table (sub-second total: the
        rename-aside is atomic at the FS level; reinit __init__ takes
        10-100ms; deferred-GC INSERT is <1ms). The slow rmtree fallback
        path only fires if pending_dir_cleanup INSERT fails — at that point
        UX considerations matter less than the recovery itself.
        """
        from shared.local_indexer import LocalIndexerError
        if self._indexer is None:
            return
        label = "מאפס…" if CURRENT_LANG == "he" else "Resetting…"
        title = "אפס ספריה שלי" if CURRENT_LANG == "he" else "Reset My Library"
        dlg = QProgressDialog(label, "", 0, 0, self)
        dlg.setWindowTitle(title)
        dlg.setCancelButton(None)
        dlg.setWindowModality(Qt.WindowModality.NonModal)
        dlg.setMinimumDuration(0)
        dlg.show()
        QApplication.processEvents()
        try:
            engine = self._parent_window.engine if self._parent_window and hasattr(self._parent_window, "engine") else None
            if engine is None:
                # Fall back to the search_engine property (Phase 97 R-01 path).
                engine = self.search_engine
            close_cb = engine.close_local_searcher if engine is not None else (lambda: None)
            reload_cb = engine.reload_local_indexes if engine is not None else (lambda: None)
            self._indexer.reset_my_library(close_cb, reload_cb)
            dlg.close()
            ok_msg = "איפוס ספריה הושלם" if CURRENT_LANG == "he" else "Library reset complete"
            # Status bar surface (mirror existing _show_status_message pattern).
            if hasattr(self, "_show_status_message"):
                self._show_status_message(ok_msg)
            else:
                QMessageBox.information(self, title, ok_msg)
            # Phase 97.3 R97.3-A (D-12): cache cleared on full reset.
            try:
                self._invalidate_prior_status_cache()
            except Exception:
                logger.exception("_perform_reset: _invalidate_prior_status_cache failed (continuing)")
            # Repopulate UI — these helpers may or may not exist; tolerate either.
            if hasattr(self, "_refresh_folder_list_ui"):
                try:
                    self._refresh_folder_list_ui()
                except Exception:
                    logger.exception("_perform_reset: _refresh_folder_list_ui failed (continuing)")
            if hasattr(self, "_unified_tree") and hasattr(self._unified_tree, "reset_for_scan"):
                try:
                    self._unified_tree.reset_for_scan()
                except Exception:
                    logger.exception("_perform_reset: _unified_tree.reset_for_scan failed (continuing)")
            # Refresh disk indicator + Reset button state now that the index
            # is empty.
            try:
                self._update_disk_indicator()
            except Exception:
                pass
            self._update_reset_button_state()
        except LocalIndexerError as exc:
            dlg.close()
            err_msg = (
                f"האיפוס נכשל: {exc}\nאנא הפעל מחדש את האפליקציה."
                if CURRENT_LANG == "he"
                else f"Reset failed: {exc}\nPlease restart the app."
            )
            QMessageBox.critical(self, title, err_msg)
            self._update_reset_button_state()
        except Exception as exc:  # noqa: BLE001
            dlg.close()
            err_msg = (
                f"האיפוס נכשל: {exc}\nאנא הפעל מחדש את האפליקציה."
                if CURRENT_LANG == "he"
                else f"Reset failed: {exc}\nPlease restart the app."
            )
            QMessageBox.critical(self, title, err_msg)
            self._update_reset_button_state()

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

        # Phase 97.3 R97.3-A (D-12): new folder added — reload cache so the
        # next folder selection reads correct prior_status.
        self._invalidate_prior_status_cache()
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
            tr("Remove '{}' from My Library?\n\nAll indexed files from this folder will be removed from search results.").format(folder_path),
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

        # Phase 97.3 R97.3-A (D-12): folder removed — drop its cache entry.
        self._invalidate_prior_status_cache()
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

        Phase 97.1: non-blocking cancel. The prior implementation called
        ``self._worker.wait(5000)`` on the UI thread, freezing the window for up
        to 5 s when the scan was mid-``os.walk`` of a 100K-file tree. Worse, it
        then called ``discard_run`` unconditionally — racing the Tantivy writer
        against the still-running scan thread. The new flow defers
        discard/keep to the worker's ``finished_signal``, so by the time we
        touch the writer the scan thread has exited.
        Debug session: `.planning/debug/phase-97-freeze-winerror-3.md`.
        """
        # Phase 97.3 R97.3-A (Codex Critique #3 HIGH fix) — context-aware cancel.
        # Priority: scan worker > tree-population worker > nothing.
        scan_running = self._worker is not None and self._worker.isRunning()
        tree_worker = getattr(self._unified_tree, "_tree_worker", None)
        tree_running = (
            tree_worker is not None
            and hasattr(tree_worker, "isRunning")
            and tree_worker.isRunning()
        )
        if not scan_running and tree_running:
            # Tree-population only — simple cancel, no scan Discard/Keep modal.
            try:
                self._unified_tree._cancel_existing_tree_worker()
                # Clear the tree so user sees the cancel took effect (D-05 invariant).
                self._unified_tree.clear()
                self._unified_tree._displayed_paths = set()
                self._unified_tree._leaf_by_path = {}
                self._unified_tree._dir_nodes = {}
            except Exception as exc:  # noqa: BLE001
                logger.warning("_on_cancel_clicked: tree cancel failed: %s", exc)
            # _on_tree_population_ended will disable _btn_cancel via the
            # finished_signal slot once the worker actually exits.
            return
        if not scan_running:
            self._btn_cancel.setEnabled(False)
            return
        # Else: scan running — fall through to existing 3-button modal.

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

        # Remember which post-cancel action the user picked; the deferred slot
        # below will execute it after the worker emits finished_signal.
        action = "discard" if clicked is btn_discard else "keep"
        self._pending_cancel_action = (action, run_id)
        self._btn_cancel.setEnabled(False)

        # Signal cooperative cancel into the scan loop and surface a
        # non-modal "Stopping…" progress dialog so the user sees the app is
        # still alive while the worker drains.
        self._worker.cancel()
        if CURRENT_LANG == "he":
            label = "מבטל…"
            title = "ביטול אינדוקס"
        else:
            label = "Stopping…"
            title = "Cancel indexing"
        stop_dlg = QProgressDialog(label, "", 0, 0, self)
        stop_dlg.setWindowTitle(title)
        stop_dlg.setCancelButton(None)  # user can't cancel the cancel
        stop_dlg.setWindowModality(Qt.WindowModality.NonModal)
        stop_dlg.setMinimumDuration(0)
        stop_dlg.show()
        self._stopping_dialog = stop_dlg

        # Hook the worker's existing finished_signal — the deferred slot
        # closes the progress dialog and runs discard_run/keep_run on the UI
        # thread AFTER the scan thread has exited (FIX-3 invariant: no
        # cross-thread Tantivy writer race).
        self._worker.finished_signal.connect(self._on_cancel_finished_drain)
        self._worker.error_signal.connect(self._on_cancel_finished_drain_error)

    def _on_cancel_finished_drain(self, _result: dict) -> None:
        """Phase 97.1 — runs after worker.finished_signal post-cancel.

        Tantivy writer is only safe to touch from the UI thread once the scan
        thread has exited; this slot is the rendezvous point. Debug session:
        `.planning/debug/phase-97-freeze-winerror-3.md`.
        """
        # Phase 97.3 R97.3-E (D-21): reset progress bar range so a future
        # scan starts clean (otherwise the next scan inherits busy state if
        # cancel fired during enumeration phase).
        try:
            self._progress_bar.setRange(0, 100)
        except Exception:
            pass
        self._close_stopping_dialog()
        action_run = getattr(self, "_pending_cancel_action", None)
        self._pending_cancel_action = None
        if action_run is not None and self._indexer is not None:
            action, run_id = action_run
            if run_id:
                try:
                    if action == "discard":
                        self._indexer.discard_run(run_id)
                    else:
                        self._indexer.keep_run(run_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "_on_cancel_finished_drain: %s_run failed: %s", action, exc
                    )
        # Phase 97.3 R97.3-A (D-12, Codex Critique #3 MEDIUM site 6):
        # cancel-drain also repopulates the tree via _refresh_folder_list_ui,
        # so prior_status cache must be reloaded first or the tree shows
        # pre-cancel status (now-stale).
        self._invalidate_prior_status_cache()
        try:
            self._refresh_folder_list_ui()
        except Exception:
            pass
        # Phase 97.2 R97.2-E — discard_run/keep_run flips scan_runs.status off
        # 'running', so the Reset button can now potentially re-enable.
        self._update_reset_button_state()

    def _on_cancel_finished_drain_error(self, _err: str) -> None:
        """Worker errored while draining the cancel — still run discard/keep."""
        self._on_cancel_finished_drain({})

    def _close_stopping_dialog(self) -> None:
        dlg = getattr(self, "_stopping_dialog", None)
        if dlg is not None:
            try:
                dlg.reset()
                dlg.close()
            except Exception:
                pass
            self._stopping_dialog = None

    def _on_progress_updated(
        self, current: int, total: int, filename: str
    ) -> None:
        """Update progress bar from worker signal.

        Phase 97.3 R97.3-E (D-21): first progress signal flips the bar
        from busy (0,0) to determinate (0,100).
        """
        # D-21: ensure determinate mode on first progress update.
        if self._progress_bar.maximum() == 0:
            self._progress_bar.setRange(0, 100)
            # Clear the "Discovering files…" status now that real progress runs.
            try:
                if self._parent_window is not None and hasattr(self._parent_window, "statusBar"):
                    self._parent_window.statusBar().clearMessage()
            except Exception:
                pass
        if total > 0:
            pct = int(current * 100 / total)
            self._progress_bar.setValue(pct)
        else:
            self._progress_bar.setValue(0)

    def _on_worker_status_updated(self, text: str) -> None:
        """Phase 97.3 R97.3-E (D-07): route worker phase text to status bar."""
        try:
            if self._parent_window is not None and hasattr(self._parent_window, "statusBar"):
                self._parent_window.statusBar().showMessage(text, 0)
        except Exception:
            pass

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
            # Phase 97 D-NEW-2 / LD-9: skip-set includes Phase 95 'unavailable' + new 'unreachable'/'timeout'.
            folder_count = sum(
                1
                for f in self._indexer.list_folders()
                if f.get("status") not in ("unavailable", "unreachable", "timeout")
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
            # Phase 97 D-NEW-2 / LD-9: skip-set includes Phase 95 'unavailable' + new 'unreachable'/'timeout'.
            if status in ("unavailable", "unreachable", "timeout"):
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

    def _invalidate_prior_status_cache(self) -> None:
        """Phase 97.3 R97.3-A (D-12, D-19) — clear cache, then reload from DB.

        MUST be called BEFORE _refresh_folder_list_ui in the worker
        finish/error/reset/folder-add/folder-remove code paths. Codex
        Critique #2 v7.14 blocker: late clearing leaves stale prior_status
        visible in the post-scan tree because _refresh_folder_list_ui calls
        populate_for_folder() which reads the cache.
        """
        self._prior_status_cache = {}
        if self._indexer is None:
            return
        try:
            folders = self._indexer.list_folders()
        except Exception as exc:  # noqa: BLE001
            logger.warning("_invalidate_prior_status_cache: list_folders failed: %s", exc)
            return
        for folder in folders:
            path = folder.get("path")
            if not path:
                continue
            try:
                self._prior_status_cache[path] = self._indexer.get_file_status_for_folder(path) or {}
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "_invalidate_prior_status_cache: get_file_status_for_folder(%s) failed: %s",
                    path, exc,
                )
                self._prior_status_cache[path] = {}

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
