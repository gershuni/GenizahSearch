"""Filter dialogs extracted from genizah_app.py (v7.9 decomposition)."""

import re

from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QFileDialog,
    QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QListWidget, QListWidgetItem, QPlainTextEdit, QPushButton, QToolTip,
    QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget,
)
from PyQt6.QtCore import QEvent, Qt
from PyQt6.QtGui import QColor, QFontMetrics

from genizah_core import CURRENT_LANG, normalize_shelfmark, tr
from desktop.gui_threads import FilterCountWorker
from shared.exclusion_service import (
    ExclusionSource, parse_csv_shelfmarks, resolve_shelfmarks,
)


class ExcludeDialog(QDialog):
    """Collect system IDs or shelfmarks that should be excluded from searches."""
    def __init__(self, parent, existing_entries=None, lists_mgr=None, shelf_map=None, exclusion_sources=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Exclude Manuscripts"))
        self.resize(600, 500)
        layout = QVBoxLayout()

        self._syncing = False
        self._shelf_to_sys = shelf_map
        # Catalogue-form answers for lines the shelf map misses ({line: sys_id or None}),
        # fetched off the UI thread by CatalogFormsThread before Resolve or Apply uses them.
        self._catalog_cache = {}
        self._catalog_thread = None
        self._catalog_abandoned = False
        self._last_edited = None
        self._full_titles = []
        self._display_titles = []
        self.meta_mgr = getattr(parent, "meta_mgr", None)
        self._lists_mgr = lists_mgr
        self._resolved_entries: list = []  # ResolvedEntry list from file resolution
        self._resolved_ids: set = set()
        self._resolved_unresolved: list = []
        self._loaded_filename: str = ''

        # Tabbed interface (Phase 56)
        from PyQt6.QtWidgets import QTabWidget, QTableWidget, QHeaderView, QListWidget, QListWidgetItem, QAbstractItemView
        self._tab_widget = QTabWidget()
        layout.addWidget(self._tab_widget)

        # === Tab 1: From File / Manual ===
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)

        help_lbl = QLabel(tr("Enter system IDs or shelfmarks to exclude (one per line). Matching values are filled automatically."))
        help_lbl.setWordWrap(True)
        tab1_layout.addWidget(help_lbl)

        grid = QGridLayout()
        grid.addWidget(QLabel(tr("System IDs")), 0, 0)
        grid.addWidget(QLabel(tr("Shelfmarks")), 0, 1)
        grid.addWidget(QLabel(tr("Title")), 0, 2)

        self.sys_text_area = QPlainTextEdit()
        self.sys_text_area.setPlaceholderText("990051564290205171\n990053963680205171")
        self.sys_text_area.textChanged.connect(self._on_sys_text_changed)

        self.shelf_text_area = QPlainTextEdit()
        self.shelf_text_area.setPlaceholderText("T-S NS 192.21\nMS heb. e.34/30\nMs. EVR II B 1011\nMs. Kaufmann GEN 227/A")
        self.shelf_text_area.textChanged.connect(self._on_shelf_text_changed)

        self.title_text_area = QPlainTextEdit()
        self.title_text_area.setPlaceholderText(tr("Title"))
        self.title_text_area.setReadOnly(True)
        self.title_text_area.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)

        self.sys_text_area.installEventFilter(self)
        self.shelf_text_area.installEventFilter(self)
        self.title_text_area.installEventFilter(self)

        grid.addWidget(self.sys_text_area, 1, 0)
        grid.addWidget(self.shelf_text_area, 1, 1)
        grid.addWidget(self.title_text_area, 1, 2)
        tab1_layout.addLayout(grid)

        # Populate editor from existing exclusion sources (remembers what was chosen)
        _initial_sys = []
        _initial_shelf = []
        if exclusion_sources:
            for src in exclusion_sources:
                _initial_sys.extend(sorted(src.sys_ids))
        elif existing_entries:
            _init_sys, _init_shelf = self._split_existing_entries(existing_entries)
            _initial_sys = _init_sys
            _initial_shelf = _init_shelf

        if _initial_sys:
            self.sys_text_area.setPlainText("\n".join(_initial_sys))
        if _initial_shelf:
            self.shelf_text_area.setPlainText("\n".join(_initial_shelf))
        if _initial_sys and not _initial_shelf:
            self._last_edited = "sys"
            self._sync_from_sys()
        elif _initial_shelf and not _initial_sys:
            self._last_edited = "shelf"
            self._sync_from_shelf()
        elif _initial_sys:
            self._set_titles(self._resolve_titles_from_sys(_initial_sys))

        # Resolution report table (D-04)
        self._report_label = QLabel("")
        tab1_layout.addWidget(self._report_label)
        self._report_table = QTableWidget(0, 4)
        self._report_table.setHorizontalHeaderLabels([tr("Shelfmark"), tr("Normalized"), "sys_id", tr("Status")])
        self._report_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._report_table.setVisible(False)
        tab1_layout.addWidget(self._report_table)

        file_row = QHBoxLayout()
        self.btn_load = QPushButton(tr("Load from File"))
        self.btn_load.clicked.connect(self.load_file)
        file_row.addWidget(self.btn_load)

        btn_resolve = QPushButton(tr("Resolve Shelfmarks"))
        btn_resolve.setToolTip(tr("Resolve entered shelfmarks and show per-row status report"))
        btn_resolve.clicked.connect(self._resolve_and_show_report)
        self._btn_resolve = btn_resolve
        file_row.addWidget(btn_resolve)
        file_row.addStretch()
        tab1_layout.addLayout(file_row)

        self._tab_widget.addTab(tab1, tr("From File / Manual"))

        # === Tab 2: From List ===
        tab2 = QWidget()
        tab2_layout = QVBoxLayout(tab2)

        if self._lists_mgr:
            tab2_layout.addWidget(QLabel(tr("Select a list and click 'Load to Editor' to add its manuscripts to the exclusion list.")))
            self._list_widget = QListWidget()
            self._list_widget.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
            all_lists = self._lists_mgr.get_all_lists(include_recent=False)
            self._list_data = {}
            for lst in all_lists:
                list_id = lst.get('id', '')
                list_name = lst.get('name', list_id)
                count = lst.get('count', 0)
                item = QListWidgetItem(f"{list_name} ({count} {tr('items')})")
                item.setData(Qt.ItemDataRole.UserRole, list_id)
                self._list_widget.addItem(item)
                self._list_data[list_id] = lst
            tab2_layout.addWidget(self._list_widget)

            btn_load_to_editor = QPushButton(tr("Load to Editor"))
            btn_load_to_editor.setToolTip(tr("Load selected list items into the editor tab so you can review and modify them"))
            btn_load_to_editor.clicked.connect(self._load_list_to_editor)
            tab2_layout.addWidget(btn_load_to_editor)
        else:
            tab2_layout.addWidget(QLabel(tr("Lists not available (not logged in)")))
            self._list_widget = None

        self._tab_widget.addTab(tab2, tr("From List"))

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_clear_all = QPushButton(tr("Clear All"))
        btn_clear_all.setToolTip(tr("Remove all exclusions"))
        btn_clear_all.clicked.connect(self._clear_all)
        btn_row.addWidget(btn_clear_all)
        btn_row.addStretch()
        btn_apply = QPushButton(tr("Apply"))
        btn_apply.clicked.connect(self.accept)
        self._btn_apply = btn_apply
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_apply)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def _clear_all(self):
        """Clear all entries from the editor and accept (removes all exclusions)."""
        self._syncing = True
        self.sys_text_area.clear()
        self.shelf_text_area.clear()
        self.title_text_area.clear()
        self._syncing = False
        self._full_titles = []
        self._display_titles = []
        self._resolved_entries = []
        self._resolved_ids = set()
        self._resolved_unresolved = []
        self._report_table.setVisible(False)
        self._report_label.setText("")
        self.accept()

    def _load_list_to_editor(self):
        """Load selected list items into the editor tab (sys_ids + shelfmarks)."""
        if not self._list_widget or not self._lists_mgr:
            return
        selected = self._list_widget.selectedItems()
        if not selected:
            return
        new_sys_ids = []
        for sel_item in selected:
            list_id = sel_item.data(Qt.ItemDataRole.UserRole)
            try:
                items = self._lists_mgr.get_items_in_list(list_id)
                for it in items:
                    sid = it.get('sys_id')
                    if sid and sid not in new_sys_ids:
                        new_sys_ids.append(sid)
            except Exception:
                pass  # Shelfmark lookup failed; use fallback identifier
        if not new_sys_ids:
            return
        # Append to existing sys_id text area
        existing = self.sys_text_area.toPlainText().strip()
        existing_lines = set(existing.splitlines()) if existing else set()
        to_add = [sid for sid in new_sys_ids if sid not in existing_lines]
        if to_add:
            combined = (existing + "\n" if existing else "") + "\n".join(to_add)
            self._syncing = True
            self.sys_text_area.setPlainText(combined)
            self._syncing = False
            self._last_edited = "sys"
            self._sync_from_sys()
        # Switch to editor tab
        self._tab_widget.setCurrentIndex(0)

    def _resolve_and_show_report(self):
        """Resolve shelfmarks from the text areas and show the resolution report.

        Lines the shelf map misses are looked up as catalogue forms first, in a worker
        (a cold first lookup builds the shelfmark index); the report follows."""
        shelf_lines = [l.strip() for l in self.shelf_text_area.toPlainText().splitlines() if l.strip()]
        if not shelf_lines:
            self._report_label.setText(tr("No shelfmarks to resolve"))
            return
        self._with_catalog_lookup(shelf_lines, self._show_report, skip_sys_ids=False)

    def _show_report(self):
        from PyQt6.QtWidgets import QTableWidgetItem
        shelf_lines = [l.strip() for l in self.shelf_text_area.toPlainText().splitlines() if l.strip()]
        if not shelf_lines:
            self._report_label.setText(tr("No shelfmarks to resolve"))
            return
        self._ensure_shelf_map()
        if not self._shelf_to_sys:
            self._report_label.setText(tr("Shelf map not available"))
            return
        ids, unresolved, entries = resolve_shelfmarks(
            shelf_lines, self._shelf_to_sys, fallback=self._catalog_fallback())
        self._resolved_entries = entries
        self._resolved_ids = ids
        self._resolved_unresolved = unresolved

        n_found = sum(1 for e in entries if e.status == 'found')
        n_notfound = sum(1 for e in entries if e.status == 'not_found')
        n_dup = sum(1 for e in entries if e.status == 'duplicate')
        self._report_label.setText(f"{tr('Resolved')} {n_found}/{len(entries)} | {n_notfound} {tr('not found')} | {n_dup} {tr('duplicates')}")

        self._report_table.setRowCount(min(len(entries), 200))
        self._report_table.setVisible(True)
        for i, e in enumerate(entries[:200]):
            self._report_table.setItem(i, 0, QTableWidgetItem(e.original))
            self._report_table.setItem(i, 1, QTableWidgetItem(e.normalized))
            self._report_table.setItem(i, 2, QTableWidgetItem(e.sys_id or '--'))
            status_item = QTableWidgetItem(e.status)
            if e.status == 'found':
                status_item.setBackground(QColor(200, 255, 200))
            elif e.status == 'not_found':
                status_item.setBackground(QColor(255, 200, 200))
            elif e.status == 'duplicate':
                status_item.setBackground(QColor(255, 255, 200))
            self._report_table.setItem(i, 3, status_item)

    def get_exclusion_sources(self) -> list:
        """Return ExclusionSource objects from the active tab."""
        sources = []
        current_tab = self._tab_widget.currentIndex()

        if current_tab == 0:
            # Tab 1: From File / Manual
            # If resolution was done, use resolved entries
            if self._resolved_ids:
                label = self._loaded_filename or tr('Manual entry')
                sources.append(ExclusionSource(
                    label=label,
                    source_type='file',
                    source_id=self._loaded_filename or 'manual',
                    sys_ids=set(self._resolved_ids),
                    unresolved=list(self._resolved_unresolved),
                    resolved_entries=list(self._resolved_entries),
                ))
            else:
                # Fall back to legacy text-based extraction
                entries_text = self.get_entries_text()
                if entries_text.strip():
                    entries = [e.strip() for e in entries_text.splitlines() if e.strip()]
                    sys_ids = set()
                    misses = []
                    for e in entries:
                        cleaned = re.sub(r"\s+", "", e)
                        digits_only = re.sub(r"\D", "", cleaned)
                        if digits_only and digits_only == cleaned:
                            sys_ids.add(cleaned)
                        else:
                            self._ensure_shelf_map()
                            norm = normalize_shelfmark(e)
                            sid = self._shelf_to_sys.get(norm) if self._shelf_to_sys else None
                            if sid:
                                sys_ids.add(sid)
                            else:
                                misses.append(e)
                    # Lines the map missed: one catalogue-form batch (single records only).
                    fallback = self._catalog_fallback()
                    if misses and fallback is not None:
                        sys_ids.update(fallback(list(dict.fromkeys(misses))).values())
                    if sys_ids:
                        sources.append(ExclusionSource(
                            label=self._loaded_filename or tr('Manual entry'),
                            source_type='file',
                            source_id=self._loaded_filename or 'manual',
                            sys_ids=sys_ids,
                            unresolved=[],
                        ))

        elif current_tab == 1 and self._list_widget:
            # Tab 2: From List
            for item in self._list_widget.selectedItems():
                list_id = item.data(Qt.ItemDataRole.UserRole)
                list_info = self._list_data.get(list_id, {})
                list_name = list_info.get('name', list_id)
                try:
                    items = self._lists_mgr.get_items_in_list(list_id)
                    sys_ids = {it.get('sys_id') for it in items if it.get('sys_id')}
                except Exception:
                    sys_ids = set()  # Batch lookup failed; use empty set for this batch
                if sys_ids:
                    sources.append(ExclusionSource(
                        label=list_name,
                        source_type='list',
                        source_id=list_id,
                        sys_ids=sys_ids,
                        unresolved=[],
                    ))

        return sources

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.FocusIn:
            if obj is self.sys_text_area:
                self._last_edited = "sys"
            elif obj is self.shelf_text_area:
                self._last_edited = "shelf"
        if obj is self.title_text_area and event.type() == QEvent.Type.ToolTip:
            cursor = self.title_text_area.cursorForPosition(event.pos())
            line_idx = cursor.blockNumber()
            if 0 <= line_idx < len(self._full_titles):
                full_title = self._full_titles[line_idx]
                display_title = self._display_titles[line_idx]
                if full_title and full_title != display_title:
                    QToolTip.showText(event.globalPos(), full_title, self.title_text_area)
                    return True
            QToolTip.hideText()
            return True
        return super().eventFilter(obj, event)

    def _split_existing_entries(self, entries):
        sys_entries = []
        shelf_entries = []
        for entry in entries:
            cleaned = re.sub(r"\s+", "", entry or "")
            digits_only = re.sub(r"\D", "", cleaned)
            if digits_only and digits_only == cleaned:
                sys_entries.append(cleaned)
            else:
                stripped = (entry or "").strip()
                if stripped:
                    shelf_entries.append(stripped)
        return sys_entries, shelf_entries

    def _on_sys_text_changed(self):
        if self._syncing or self._last_edited != "sys":
            return
        self._sync_from_sys()

    def _on_shelf_text_changed(self):
        if self._syncing or self._last_edited != "shelf":
            return
        self._sync_from_shelf()

    def _sync_from_sys(self):
        self._syncing = True
        sys_lines = self._get_lines(self.sys_text_area.toPlainText())
        shelves = self._resolve_shelves_from_sys(sys_lines)
        titles = self._resolve_titles_from_sys(sys_lines)
        self.shelf_text_area.setPlainText("\n".join(shelves))
        self._set_titles(titles)
        self._syncing = False

    def _sync_from_shelf(self):
        self._syncing = True
        shelf_lines = self._get_lines(self.shelf_text_area.toPlainText())
        sys_ids = self._resolve_sys_from_shelves(shelf_lines)
        self.sys_text_area.setPlainText("\n".join(sys_ids))
        titles = self._resolve_titles_from_sys(sys_ids)
        self._set_titles(titles)
        self._syncing = False

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._full_titles:
            self._refresh_title_display()

    def _get_lines(self, text):
        return text.splitlines()

    def _set_titles(self, titles):
        self._full_titles = titles
        self._refresh_title_display()

    def _refresh_title_display(self):
        metrics = QFontMetrics(self.title_text_area.font())
        width = max(self.title_text_area.viewport().width() - 6, 20)
        self._display_titles = [
            metrics.elidedText(title, Qt.TextElideMode.ElideRight, width) if title else ""
            for title in self._full_titles
        ]
        self.title_text_area.setPlainText("\n".join(self._display_titles))

    def _resolve_shelves_from_sys(self, sys_lines):
        shelves = []
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if not cleaned or not self.meta_mgr:
                shelves.append("")
                continue
            shelf, _ = self.meta_mgr.get_meta_for_id(cleaned)
            if shelf == "Unknown" and cleaned not in self.meta_mgr.nli_cache:
                self.meta_mgr.fetch_nli_data(cleaned)
                shelf, _ = self.meta_mgr.get_meta_for_id(cleaned)
            shelves.append("" if shelf == "Unknown" else shelf)
        return shelves

    def _resolve_titles_from_sys(self, sys_lines):
        titles = []
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if not cleaned or not self.meta_mgr:
                titles.append("")
                continue
            _, title = self.meta_mgr.get_meta_for_id(cleaned)
            if not title and cleaned not in self.meta_mgr.nli_cache:
                self.meta_mgr.fetch_nli_data(cleaned)
                _, title = self.meta_mgr.get_meta_for_id(cleaned)
            titles.append(title or "")
        return titles

    def _ensure_shelf_map(self):
        if self._shelf_to_sys is not None:
            return
        self._shelf_to_sys = {}
        if not self.meta_mgr:
            return
        for sys_id, meta in self.meta_mgr.csv_bank.items():
            self._add_shelf_map(meta.get("shelfmark"), sys_id)
        for sys_id, meta in self.meta_mgr.nli_cache.items():
            self._add_shelf_map(meta.get("shelfmark"), sys_id)

    def _add_shelf_map(self, shelf, sys_id):
        norm = self._normalize_shelfmark(shelf)
        if norm and norm not in self._shelf_to_sys:
            self._shelf_to_sys[norm] = sys_id

    def _catalog_fallback(self):
        """Catalogue-form lookup for the lines the shelf map misses (exclusion_service.catalog_fallback).

        Used on Resolve and on Apply, never by the per-keystroke sync below. CACHE ONLY: the
        answers are fetched off the UI thread by _with_catalog_lookup first, and a line that was
        not fetched counts as not found here -- this never looks anything up on the UI thread.
        (The shared helper exclusion_service.catalog_fallback is the non-UI version.)"""
        if self.meta_mgr is None or not hasattr(self.meta_mgr, 'resolve_catalog_forms'):
            return None

        def _cached(lines):
            return {line: self._catalog_cache[line] for line in lines if self._catalog_cache.get(line)}
        return _cached

    def _catalog_misses(self, lines, skip_sys_ids=True):
        """Lines that need a catalogue-form lookup: not in the shelf map, not cached, and (for
        Apply, which reads digit-only lines as sys_ids) not digit-only. Resolve sends every
        line the map misses to the fallback, digit-only ones included, so it passes False."""
        self._ensure_shelf_map()
        out = []
        for line in lines:
            text = (line or '').strip()
            if not text or text in self._catalog_cache:
                continue
            if skip_sys_ids and re.sub(r"\s+", "", text).isdigit():
                continue
            if self._shelf_to_sys and self._shelf_to_sys.get(normalize_shelfmark(text)):
                continue
            out.append(text)
        return list(dict.fromkeys(out))

    def _with_catalog_lookup(self, lines, then, skip_sys_ids=True):
        """Run `then()` once every catalogue-form line among `lines` has its answer cached.

        Without lookups to make it runs at once; otherwise a CatalogFormsThread fetches them
        and `then()` runs when it answers -- unless the dialog was cancelled meanwhile. The
        text boxes are read-only while it runs, so `then()` sees the lines that were fetched."""
        misses = (self._catalog_misses(lines, skip_sys_ids)
                  if self._catalog_fallback() is not None else [])
        if not misses:
            then()
            return
        if self._catalog_thread is not None:
            return  # a lookup is running; Resolve/Apply are disabled until it answers
        from desktop.gui_threads import CatalogFormsThread, _keep_until_finished
        thread = CatalogFormsThread(self.meta_mgr, misses)
        thread.done.connect(lambda answers, t=then: self._on_catalog_done(answers, t))
        self._catalog_thread = thread        # held until it answers (cleared in the slot)
        self._set_catalog_busy(True)
        thread.start()
        _keep_until_finished(thread)         # and past a dialog that closes meanwhile

    def _on_catalog_done(self, answers, then):
        try:
            from PyQt6 import sip
            if sip.isdeleted(self):
                return
        except ImportError:
            pass
        self._catalog_thread = None
        self._catalog_cache.update(answers)
        self._set_catalog_busy(False)
        if not self._catalog_abandoned:
            then()

    def _set_catalog_busy(self, busy):
        self._btn_apply.setEnabled(not busy)
        self._btn_resolve.setEnabled(not busy)
        self.shelf_text_area.setReadOnly(busy)
        self.sys_text_area.setReadOnly(busy)
        if busy:
            self._report_label.setText(tr("Searching..."))

    def accept(self):
        """Apply. Lines the shelf map misses are looked up as catalogue forms first, off the
        UI thread; the dialog closes when the answers are in (get_exclusion_sources reads them)."""
        if self._tab_widget.currentIndex() == 0 and not self._resolved_ids:
            lines = [l.strip() for l in self.get_entries_text().splitlines() if l.strip()]
            if self._catalog_fallback() is not None and self._catalog_misses(lines):
                self._with_catalog_lookup(lines, lambda: super(ExcludeDialog, self).accept())
                return
        super().accept()

    def reject(self):
        self._catalog_abandoned = True   # a lookup still running must not apply anything
        super().reject()

    def _resolve_sys_from_shelves(self, shelf_lines):
        self._ensure_shelf_map()
        sys_ids = []
        for line in shelf_lines:
            norm = self._normalize_shelfmark(line)
            sys_ids.append(self._shelf_to_sys.get(norm, "") if norm else "")
        return sys_ids

    def _normalize_shelfmark(self, shelf):
        if not shelf:
            return ""
        without_prefix = re.sub(r"^\s*m[\.\s]*s[\.\s]*\.?\s*", "", shelf, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^\w]", "", without_prefix).lower()
        if cleaned.startswith("ms"):
            cleaned = cleaned[2:]
        return cleaned

    def load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load", "", "Text/CSV (*.txt *.csv)")
        if path:
            import os
            self._loaded_filename = os.path.basename(path)
            with open(path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            # Use shared parsing for CSV files
            if path.lower().endswith('.csv'):
                lines = parse_csv_shelfmarks(content)
                self._syncing = True
                self.shelf_text_area.setPlainText("\n".join(lines))
                self._syncing = False
                self._last_edited = "shelf"
                self._sync_from_shelf()
                # Auto-resolve
                self._resolve_and_show_report()
                return

            entries = [line for line in content.splitlines() if line.strip()]
            sys_entries, shelf_entries = self._split_existing_entries(entries)
            self._syncing = True
            self.sys_text_area.setPlainText("\n".join(sys_entries))
            self.shelf_text_area.setPlainText("\n".join(shelf_entries))
            self._syncing = False
            if sys_entries and not shelf_entries:
                self._last_edited = "sys"
                self._sync_from_sys()
            elif shelf_entries and not sys_entries:
                self._last_edited = "shelf"
                self._sync_from_shelf()
            elif sys_entries:
                self._set_titles(self._resolve_titles_from_sys(sys_entries))
            # Auto-resolve for file imports
            if shelf_entries:
                self._resolve_and_show_report()

    def get_entries_text(self):
        entries = []
        seen = set()

        sys_lines = self._get_lines(self.sys_text_area.toPlainText())
        for line in sys_lines:
            cleaned = re.sub(r"\D", "", line or "")
            if cleaned and cleaned not in seen:
                entries.append(cleaned)
                seen.add(cleaned)

        shelf_lines = self._get_lines(self.shelf_text_area.toPlainText())
        for line in shelf_lines:
            stripped = (line or "").strip()
            if stripped and stripped not in seen:
                entries.append(stripped)
                seen.add(stripped)

        return "\n".join(entries)


# ── Puzzle Canvas Building Blocks (Phase 48) ────────────────────────────




class DomainFilterDialog(QDialog):
    """Hierarchical domain filter dialog with checkboxes and type-ahead search.

    Post-search dynamic filter: Shows only domains from current results,
    all checked by default. Unchecking excludes domains.
    """

    def __init__(self, parent=None, result_domains: dict = None, excluded_domains: set = None, uncategorized_count: int = 0):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter by Subject Domain"))
        self.setMinimumSize(550, 650)
        self.result_domains = result_domains or {}  # domain_name -> count
        self.excluded_domains = excluded_domains or set()
        self.uncategorized_count = uncategorized_count
        self._updating_checks = False  # Guard for programmatic checkbox changes

        layout = QVBoxLayout(self)

        # Search input for type-ahead filtering
        search_label = QLabel(tr("Search domains:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Type to filter..."))
        self.search_input.textChanged.connect(self._filter_tree)
        layout.addWidget(search_label)
        layout.addWidget(self.search_input)

        # Tree widget with checkboxes
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels([tr("Domain"), tr("Manuscripts")])
        self.tree.setColumnWidth(0, 380)
        self.tree.itemChanged.connect(self._handle_item_changed)
        layout.addWidget(self.tree)

        # Selection summary
        self.summary_label = QLabel(tr("Showing all domains"))
        layout.addWidget(self.summary_label)

        # Buttons
        btn_layout = QHBoxLayout()
        check_all_btn = QPushButton(tr("Select All"))
        check_all_btn.clicked.connect(self._check_all)
        btn_layout.addWidget(check_all_btn)
        uncheck_all_btn = QPushButton(tr("Select None"))
        uncheck_all_btn.clicked.connect(self._uncheck_all)
        btn_layout.addWidget(uncheck_all_btn)
        btn_layout.addStretch()

        ok_btn = QPushButton(tr("OK"))
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton(tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        self._populate_tree()
        self._restore_exclusions()
        self._update_summary()

    def _populate_tree(self):
        """Populate tree with domains from current search results only."""
        from shared.fjms_service import get_fjms_service, qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS
        fjms = get_fjms_service()
        if not fjms.is_available() or not self.result_domains:
            return

        # Get full hierarchy to maintain parent/child structure
        hierarchy = fjms.get_domain_hierarchy()
        self.tree.blockSignals(True)

        # Only show domains that appear in result_domains
        for parent_name, info in hierarchy.items():
            # Show parent if it or any of its children are in result_domains
            parent_in_results = parent_name in self.result_domains
            # Check both qualified and bare names for ambiguous domains
            children_in_results = []
            for child in info.get('children', []):
                qname = qualify_domain_name(child['domain'], parent_name)
                if qname in self.result_domains:
                    children_in_results.append((child, qname))
                elif child['domain'] in self.result_domains and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                    children_in_results.append((child, child['domain']))

            if not parent_in_results and not children_in_results:
                continue  # Skip this parent entirely

            # Add parent item (display Hebrew name when available, store English as UserRole key)
            parent_count = self.result_domains.get(parent_name, 0)
            # If parent count is 0 but has children in results, sum their counts
            if children_in_results and parent_count == 0:
                parent_count = sum(self.result_domains.get(domain_key, 0) for _, domain_key in children_in_results)
            parent_display = info.get('parent_domain_heb', parent_name) if CURRENT_LANG == 'he' else parent_name
            parent_item = QTreeWidgetItem([parent_display, f"{parent_count:,}"])
            parent_item.setFlags(parent_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            parent_item.setCheckState(0, Qt.CheckState.Checked)  # All checked by default
            parent_item.setData(0, Qt.ItemDataRole.UserRole, parent_name)
            self.tree.addTopLevelItem(parent_item)

            # Add children that are in results (each entry is (child_dict, domain_key))
            for child, domain_key in children_in_results:
                child_count = self.result_domains.get(domain_key, 0)
                child_display = child.get('domain_heb', child['domain']) if CURRENT_LANG == 'he' else domain_key
                child_item = QTreeWidgetItem([child_display, f"{child_count:,}"])
                child_item.setFlags(child_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child_item.setCheckState(0, Qt.CheckState.Checked)  # All checked by default
                child_item.setData(0, Qt.ItemDataRole.UserRole, domain_key)
                parent_item.addChild(child_item)

                # Third level: sub-sub-domains in results
                for subchild in child.get('children', []):
                    sc_qname = qualify_domain_name(subchild['domain'], child['domain'])
                    sc_key = None
                    if sc_qname in self.result_domains:
                        sc_key = sc_qname
                    elif subchild['domain'] in self.result_domains and subchild['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                        sc_key = subchild['domain']
                    if sc_key:
                        sc_count = self.result_domains.get(sc_key, 0)
                        sc_display = subchild.get('domain_heb', subchild['domain']) if CURRENT_LANG == 'he' else sc_key
                        sc_item = QTreeWidgetItem([sc_display, f"{sc_count:,}"])
                        sc_item.setFlags(sc_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                        sc_item.setCheckState(0, Qt.CheckState.Checked)
                        sc_item.setData(0, Qt.ItemDataRole.UserRole, sc_key)
                        child_item.addChild(sc_item)

        # Add "Uncategorized" node for results without domain data
        if self.uncategorized_count > 0:
            uncat_display = tr("Uncategorized")
            uncat_item = QTreeWidgetItem([uncat_display, f"{self.uncategorized_count:,}"])
            uncat_item.setFlags(uncat_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            uncat_item.setCheckState(0, Qt.CheckState.Checked)
            uncat_item.setData(0, Qt.ItemDataRole.UserRole, "Uncategorized")
            self.tree.addTopLevelItem(uncat_item)

        self.tree.blockSignals(False)
        self.tree.expandAll()

    def _filter_tree(self):
        """Filter tree items by search text."""
        search_text = self.search_input.text().lower()
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            parent_text = parent_item.text(0).lower()
            parent_match = search_text in parent_text
            any_child_match = False

            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                child_text = child_item.text(0).lower()
                child_match = search_text in child_text
                any_grandchild_match = False

                for k in range(child_item.childCount()):
                    sc_item = child_item.child(k)
                    sc_text = sc_item.text(0).lower()
                    sc_match = search_text in sc_text
                    sc_item.setHidden(bool(search_text and not sc_match))
                    if sc_match:
                        any_grandchild_match = True

                child_item.setHidden(bool(search_text and not (child_match or any_grandchild_match)))
                if child_match or any_grandchild_match:
                    any_child_match = True

            parent_item.setHidden(bool(search_text and not (parent_match or any_child_match)))

    def _handle_item_changed(self, item, column):
        """Handle checkbox state changes with parent-child propagation."""
        if self._updating_checks or column != 0:
            return

        self._updating_checks = True
        check_state = item.checkState(0)

        # Propagate to all visible descendants (children + grandchildren)
        def propagate_down(parent_item, state):
            for i in range(parent_item.childCount()):
                child = parent_item.child(i)
                if not child.isHidden():
                    child.setCheckState(0, state)
                    propagate_down(child, state)

        propagate_down(item, check_state)

        self._updating_checks = False
        self._update_summary()

    def _check_all(self):
        """Check all items (no filtering)."""
        self.tree.blockSignals(True)
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            parent_item.setCheckState(0, Qt.CheckState.Checked)
            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                child_item.setCheckState(0, Qt.CheckState.Checked)
                for k in range(child_item.childCount()):
                    child_item.child(k).setCheckState(0, Qt.CheckState.Checked)
        self.tree.blockSignals(False)
        self._update_summary()

    def _uncheck_all(self):
        """Uncheck all items (exclude all domains)."""
        self.tree.blockSignals(True)
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            parent_item.setCheckState(0, Qt.CheckState.Unchecked)
            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                child_item.setCheckState(0, Qt.CheckState.Unchecked)
                for k in range(child_item.childCount()):
                    child_item.child(k).setCheckState(0, Qt.CheckState.Unchecked)
        self.tree.blockSignals(False)
        self._update_summary()

    def _restore_exclusions(self):
        """Restore previously excluded domains by unchecking them."""
        if not self.excluded_domains:
            return

        self.tree.blockSignals(True)
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            parent_domain = parent_item.data(0, Qt.ItemDataRole.UserRole)
            if parent_domain in self.excluded_domains:
                parent_item.setCheckState(0, Qt.CheckState.Unchecked)

            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                child_domain = child_item.data(0, Qt.ItemDataRole.UserRole)
                if child_domain in self.excluded_domains:
                    child_item.setCheckState(0, Qt.CheckState.Unchecked)

                for k in range(child_item.childCount()):
                    sc_item = child_item.child(k)
                    sc_domain = sc_item.data(0, Qt.ItemDataRole.UserRole)
                    if sc_domain in self.excluded_domains:
                        sc_item.setCheckState(0, Qt.CheckState.Unchecked)

        self.tree.blockSignals(False)

    def get_excluded_domains(self):
        """Return set of excluded (unchecked) domain names."""
        excluded = set()
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            if parent_item.checkState(0) == Qt.CheckState.Unchecked:
                parent_domain = parent_item.data(0, Qt.ItemDataRole.UserRole)
                excluded.add(parent_domain)

            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                if child_item.checkState(0) == Qt.CheckState.Unchecked:
                    child_domain = child_item.data(0, Qt.ItemDataRole.UserRole)
                    excluded.add(child_domain)

                for k in range(child_item.childCount()):
                    sc_item = child_item.child(k)
                    if sc_item.checkState(0) == Qt.CheckState.Unchecked:
                        sc_domain = sc_item.data(0, Qt.ItemDataRole.UserRole)
                        excluded.add(sc_domain)

        return excluded

    def _update_summary(self):
        """Update exclusion summary label."""
        excluded = self.get_excluded_domains()
        count = len(excluded)
        if count == 0:
            self.summary_label.setText(tr("Showing all domains"))
        elif count == 1:
            domain_name = next(iter(excluded))
            self.summary_label.setText(f"{tr('Excluding')}: {domain_name}")
        else:
            self.summary_label.setText(f"{tr('Excluding')} {count} {tr('domains')}")




# Count-label styles (#17: an error shows red; a later answer resets it).
_COUNT_STYLE_OK = "font-size: 12px; font-weight: bold; color: #2980b9; padding: 4px;"
_COUNT_STYLE_ERROR = "font-size: 12px; font-weight: bold; color: #e74c3c; padding: 4px;"


# ── Manuscript features: the catalogue criteria (docs/plans/CATALOGUE_CRITERIA_PLAN.md) ──

# Saved "Search only in..." keys from before the catalogue criteria.
_OLD_DATE_KEYS = ('date_from', 'date_to', 'include_undated')
_UNSHIPPED_KEYS = ('script_styles', 'script_places')  # PR 2.1's: never released


def criteria_unavailable_keys(service=None) -> frozenset:
    """The criteria the open catalogue file is KNOWN unable to answer: on a
    5.0.0 file, every one that needs the 5.1.0 tables -- period of copy,
    columns, lines (in any source mode), vocalization extent, attributes
    (rule 9). A file that could not be read is not known to lack anything
    (K-16): its criteria stay, and the lookup reports the error."""
    from shared.catalog_criteria import NEW_TABLE_KEYS
    from shared.fjms_service import FILTER_UNSUPPORTED, get_fjms_service
    service = service if service is not None else get_fjms_service()
    if service.criteria_support() != FILTER_UNSUPPORTED:
        return frozenset()
    return frozenset(NEW_TABLE_KEYS)


def drop_unavailable_criteria(criteria, service=None):
    """(criteria, dropped keys): ``criteria`` without what the open file is
    known unable to answer -- such a criterion could only make every lookup
    fail (#17)."""
    criteria = dict(criteria or {})
    if not criteria:
        return criteria, []
    gone = sorted(k for k in criteria if k in criteria_unavailable_keys(service))
    for key in gone:
        criteria.pop(key)
    return criteria, gone


def migrate_saved_filters(filters):
    """Saved pre-search filters in today's form (plan §6.4).

    A date range and "include undated" become Period of copy (D18). The old
    measurement bounds are not restored (R2, owner: "the least work"); a
    search saved with them must not show its stored rows, which were found
    with a filter that is no longer shown -- hence the second value. The
    criteria are cleaned (a malformed one is dropped). Returns (a deep copy:
    a chip removed later must not edit a saved history entry,
    had_old_measurements)."""
    import copy
    from shared.catalog_criteria import clean_criteria, period_from_dates
    from shared.fjms_service import MEASUREMENT_FILTER_KEYS
    filters = copy.deepcopy(dict(filters or {}))
    had_measurements = any(filters.get(k) not in (None, '', [], ()) for k in MEASUREMENT_FILTER_KEYS)
    for key in MEASUREMENT_FILTER_KEYS + _UNSHIPPED_KEYS:
        filters.pop(key, None)
    criteria = clean_criteria(filters.get('criteria'), strict=False)[0]
    date_from, date_to, undated = (filters.pop(k, None) for k in _OLD_DATE_KEYS)
    if 'period' not in criteria:
        criteria = clean_criteria(dict(criteria, **period_from_dates(
            date_from or None, date_to or None, bool(undated))), strict=False)[0]
    if criteria:
        filters['criteria'] = criteria
    else:
        filters.pop('criteria', None)
    return filters, had_measurements


def criteria_chip_texts(criteria) -> list:
    """(key, text) for each criterion, one chip per list or range, values in
    the catalogue's own Hebrew (FjmsService.criteria_chips: never fails for
    want of a label)."""
    from shared.fjms_service import get_fjms_service
    return get_fjms_service().criteria_chips(criteria, _lang())


def _lang() -> str:
    return 'he' if CURRENT_LANG == 'he' else 'en'


def _label(entry) -> str:
    """A section's or a criterion's name in the interface language."""
    return entry.label_he if CURRENT_LANG == 'he' else entry.label_en


def _value_label(key, value: dict) -> str:
    """A list value as the chips show it (shared.catalog_criteria.value_label);
    "Not recorded" as a list entry, capitalised."""
    from shared.catalog_criteria import NOT_RECORDED, value_label
    if value['value'] == NOT_RECORDED:
        return value['label_he'] if _lang() == 'he' else value['label_en']
    label = value_label(key, value['value'], _lang(), {value['value']: value.get('label_he')})
    # A value the catalogue gives no Hebrew for here (measured Paper, under
    # "Measured only"): the app's own translation.
    return tr(label) if label == value['value'] else label


def _range_number(text: str):
    try:
        return float(text) if text.strip() else None
    except ValueError:
        return None  # '-' or '.' while typing


_LONG_LIST = 12  # a value list longer than this gets a filter box
_TAIL_VALUES = ("Unknown", "Cannot be determined", "_none")  # listed last, in this order
_SOURCE_CHOICES = (('either', "Catalogue or measured"), ('catalogue', "Catalogue only"),
                   ('measured', "Measured only"))
_NOTE_STYLE = "font-size: 11px; color: #7f8c8d;"


class CriteriaDialog(QDialog):
    """Manuscript features ("מאפייני כתב היד", D9): the catalogue criteria of
    shared/catalog_criteria.py, laid out as the web panel -- the sections in a
    list, each section's controls beside it, the live total below (plan §6.3,
    D1/D2). Lists show their values by count, then Unknown, Cannot be
    determined, Not recorded; a long list has a filter box.

    Opened from the Catalog tab and "Search only in..." over the corpus
    (``universe``: a set or MetadataManager.corpus_sys_ids; by default the
    parent's), and for the post-search filters with ``sections`` (the ones to
    show) and ``within`` (the result sys_ids the total counts, and against
    which "not recorded" and "all except" are read).

    get_criteria() returns clean criteria; a criterion outside ``sections`` is
    returned unchanged. Criteria the open file is KNOWN unable to answer are
    shown disabled with a note, and saved values for them are dropped
    (``dropped_saved_criteria``)."""

    def __init__(self, parent=None, criteria=None, *, universe=None, sections=None, within=None):
        from PyQt6.QtCore import QRegularExpression, QTimer
        from PyQt6.QtGui import QRegularExpressionValidator
        from PyQt6.QtWidgets import QScrollArea, QStackedWidget
        from shared.catalog_criteria import BY_KEY, CRITERIA, SECTIONS, clean_criteria
        super().__init__(parent)
        self.setWindowTitle(tr("Manuscript features"))
        self.setMinimumSize(840, 620)
        self._validators = {
            True: QRegularExpressionValidator(QRegularExpression(r"^\d{0,5}$"), self),
            False: QRegularExpressionValidator(QRegularExpression(r"^\d{0,4}(\.\d{1,2})?$"), self),
        }
        saved = clean_criteria(criteria, strict=False)[0]
        self._sections = [s for s in SECTIONS if sections is None or s.key in sections]
        shown = {s.key for s in self._sections}
        self._criteria = [c for c in CRITERIA if c.section in shown]
        self._unavailable = criteria_unavailable_keys() if self._criteria else frozenset()
        self.dropped_saved_criteria = sorted(k for k in saved if k in self._unavailable)
        saved = {k: v for k, v in saved.items() if k not in self._unavailable}
        self._outside = {k: v for k, v in saved.items() if BY_KEY[k].section not in shown}
        if universe is None:
            universe = getattr(getattr(parent, 'meta_mgr', None), 'corpus_sys_ids', None)
        self._corpus = universe
        self._within = within
        self._controls = {}
        self._pending = {}      # list key -> saved values not yet shown (values still loading)
        self._requested = set()  # (key, source) asked of a values worker
        self._answers = {}       # (key, source) -> {'values': [...], 'coverage': (n, of)}
        self._count_tag = 0
        self._count_timer = QTimer(self)
        self._count_timer.setSingleShot(True)
        self._count_timer.setInterval(300)
        self._count_timer.timeout.connect(self._update_count)

        layout = QVBoxLayout(self)
        body = QHBoxLayout()
        self.section_list = QListWidget()
        self.section_list.setFixedWidth(200)
        self.pages = QStackedWidget()
        for section in self._sections:
            item = QListWidgetItem(_label(section))
            item.setData(Qt.ItemDataRole.UserRole, section.key)
            self.section_list.addItem(item)
            page = QWidget()
            page_layout = QVBoxLayout(page)
            members = [c for c in self._criteria if c.section == section.key]
            for criterion in members:
                build = self._build_list if criterion.kind == 'list' else self._build_range
                page_layout.addWidget(build(criterion, saved.get(criterion.key, {})))
            for help_text in self._section_help(members):
                hint = QLabel(help_text)
                hint.setWordWrap(True)
                hint.setStyleSheet(_NOTE_STYLE)
                page_layout.addWidget(hint)
            page_layout.addStretch()
            scroll = QScrollArea()
            scroll.setWidget(page)
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(scroll.Shape.NoFrame)
            self.pages.addWidget(scroll)
        body.addWidget(self.section_list)
        body.addWidget(self.pages, 1)
        layout.addLayout(body, 1)

        self.dropped_notice_label = QLabel(
            tr('Some saved filters were removed: this catalog data is not available.'))
        self.dropped_notice_label.setWordWrap(True)
        self.dropped_notice_label.setStyleSheet("color: #e67e22; font-size: 11px; padding: 2px 4px;")
        self.dropped_notice_label.setVisible(bool(self.dropped_saved_criteria))
        layout.addWidget(self.dropped_notice_label)
        self.total_label = QLabel("")
        self.total_label.setWordWrap(True)
        self.total_label.setStyleSheet(_COUNT_STYLE_OK)
        layout.addWidget(self.total_label)
        self.note_label = QLabel("")
        self.note_label.setWordWrap(True)
        self.note_label.setStyleSheet(_NOTE_STYLE)
        self.note_label.setVisible(False)
        layout.addWidget(self.note_label)

        buttons = QHBoxLayout()
        clear_section = QPushButton(tr("Clear section"))
        clear_section.clicked.connect(self._clear_section)
        buttons.addWidget(clear_section)
        clear_all = QPushButton(tr("Clear All"))
        clear_all.clicked.connect(self._clear_all)
        buttons.addWidget(clear_all)
        buttons.addStretch()
        self.ok_btn = QPushButton(tr("Apply"))
        self.ok_btn.setDefault(True)
        self.ok_btn.clicked.connect(self.accept)
        cancel = QPushButton(tr("Cancel"))
        cancel.clicked.connect(self.reject)
        buttons.addWidget(self.ok_btn)
        buttons.addWidget(cancel)
        layout.addLayout(buttons)

        self.section_list.currentRowChanged.connect(self._on_section_changed)
        first = next((i for i, s in enumerate(self._sections)
                      if any(BY_KEY[k].section == s.key for k in saved)), 0)
        if self._sections:
            self.section_list.setCurrentRow(first)
            self._on_section_changed(first)  # also when row 0 was already current
        self._refresh_badges()
        self._update_count()

    # -- building ---------------------------------------------------------------
    @staticmethod
    def _section_help(members):
        """The panel's help lines (rules 3 and 4) for a section's criteria."""
        lines = []
        if any(c.category for c in members):
            lines.append(tr("A manuscript matches when any of its catalogue records gives a ticked value."))
        if any(c.page_column for c in members):
            lines.append(tr("Measured values: every range set here holds on one measured page."))
        return lines

    def _source_combo(self, criterion, spec):
        combo = QComboBox()
        for value, text in _SOURCE_CHOICES:
            combo.addItem(tr(text), value)
        combo.setCurrentIndex(max(0, combo.findData(spec.get('source', 'either'))))
        combo.currentIndexChanged.connect(lambda _i, k=criterion.key: self._on_source_changed(k))
        return combo

    def _unavailable_note(self, group_layout):
        note = QLabel(tr("This catalogue data file is too old for this criterion: "
                         "it needs version 5.1.0 or later."))
        note.setWordWrap(True)
        note.setStyleSheet("color: #e67e22; font-size: 11px;")
        group_layout.addWidget(note)

    def _build_list(self, criterion, spec):
        key = criterion.key
        group = QGroupBox(_label(criterion))
        box = QVBoxLayout(group)
        top = QHBoxLayout()
        mode = QComboBox()
        mode.addItem(tr("Only these"), 'only')
        mode.addItem(tr("All except these"), 'except')
        mode.setCurrentIndex(1 if spec.get('mode') == 'except' else 0)
        mode.currentIndexChanged.connect(self._changed)
        top.addWidget(mode)
        source = self._source_combo(criterion, spec) if criterion.sources else None
        if source is not None:
            top.addWidget(source)
        top.addStretch()
        coverage = QLabel("")
        coverage.setStyleSheet(_NOTE_STYLE)
        top.addWidget(coverage)
        box.addLayout(top)
        search = QLineEdit()
        search.setPlaceholderText(tr("Filter values"))
        search.setClearButtonEnabled(True)
        search.setVisible(False)
        search.textChanged.connect(lambda text, k=key: self._filter_values(k, text))
        box.addWidget(search)
        values = QListWidget()
        values.setMinimumHeight(120)
        values.addItem(QListWidgetItem(tr("Loading...")))
        values.itemChanged.connect(self._changed)
        box.addWidget(values)
        if key == 'attributes':
            hint = QLabel(tr("Any of the ticked attributes."))
            hint.setStyleSheet(_NOTE_STYLE)
            box.addWidget(hint)
        self._pending[key] = list(spec.get('values', []))
        self._controls[key] = {'kind': 'list', 'mode': mode, 'source': source, 'search': search,
                               'values': values, 'coverage': coverage, 'loaded': None}
        if key in self._unavailable:
            group.setEnabled(False)
            values.clear()
            self._unavailable_note(box)
        return group

    def _build_range(self, criterion, spec):
        key = criterion.key
        group = QGroupBox(_label(criterion) + (f" ({tr(criterion.unit)})" if criterion.unit else ""))
        box = QVBoxLayout(group)
        row = QHBoxLayout()
        bounds = []
        for bound, text in (('min', "From"), ('max', "To")):
            edit = QLineEdit()
            edit.setPlaceholderText(tr(text))
            edit.setMaximumWidth(90)
            edit.setValidator(self._validators[criterion.integer])
            if bound in spec:
                edit.setText(f"{spec[bound]:g}")
            edit.textChanged.connect(self._changed)
            row.addWidget(edit)
            bounds.append(edit)
        not_recorded = QCheckBox(tr("or: not recorded"))
        not_recorded.setChecked(bool(spec.get('not_recorded')))
        not_recorded.toggled.connect(self._changed)
        row.addWidget(not_recorded)
        source = self._source_combo(criterion, spec) if criterion.sources else None
        if source is not None:
            row.addWidget(source)
        row.addStretch()
        coverage = QLabel("")
        coverage.setStyleSheet(_NOTE_STYLE)
        row.addWidget(coverage)
        box.addLayout(row)
        if key == 'period':
            centuries = QHBoxLayout()
            centuries.setSpacing(2)
            # The web's presets (Catalog Browse), so both list the same manuscripts.
            for lo, hi, text in [(c, c, f"{c + 1}th") for c in range(9, 15)] + [(16, 19, "16-19")]:
                btn = QPushButton(tr(text))
                btn.setStyleSheet("QPushButton { font-size: 10px; padding: 1px 4px; }")
                btn.clicked.connect(lambda _c=False, a=lo * 100, b=hi * 100 + 99: self._set_range('period', a, b))
                centuries.addWidget(btn)
            centuries.addStretch()
            box.addLayout(centuries)
        self._controls[key] = {'kind': 'range', 'min': bounds[0], 'max': bounds[1],
                               'not_recorded': not_recorded, 'source': source, 'coverage': coverage}
        if key in self._unavailable:
            group.setEnabled(False)
            self._unavailable_note(box)
        return group

    # -- reading the controls ---------------------------------------------------
    def _source_of(self, key) -> str:
        combo = self._controls[key]['source']
        return combo.currentData() if combo is not None else 'either'

    def _checked(self, key) -> list:
        values = self._controls[key]['values']
        out = []
        for i in range(values.count()):
            item = values.item(i)
            data = item.data(Qt.ItemDataRole.UserRole)
            if data is not None and item.checkState() == Qt.CheckState.Checked:
                out.append(data)
        return out

    def _spec(self, key) -> dict:
        from shared.catalog_criteria import BY_KEY
        controls = self._controls[key]
        spec = {}
        if BY_KEY[key].sources:
            spec['source'] = self._source_of(key)
        if controls['kind'] == 'list':
            spec['values'] = (self._checked(key) if controls['loaded'] is not None
                              else list(self._pending.get(key, [])))
            spec['mode'] = controls['mode'].currentData()
            return spec
        for bound in ('min', 'max'):
            number = _range_number(controls[bound].text())
            if number is not None:
                spec[bound] = number
        spec['not_recorded'] = controls['not_recorded'].isChecked()
        return spec

    def get_criteria(self) -> dict:
        """The chosen criteria, clean (shared.catalog_criteria.clean_criteria)."""
        from shared.catalog_criteria import clean_criteria
        raw = dict(self._outside)
        for criterion in self._criteria:
            if criterion.key not in self._unavailable:
                raw[criterion.key] = self._spec(criterion.key)
        return clean_criteria(raw, strict=False)[0]

    # -- values and coverage, off the UI thread -----------------------------------
    def _on_section_changed(self, row):
        if row < 0:
            return
        self.pages.setCurrentIndex(row)
        section = self._sections[row].key
        self._request([(c.key, self._source_of(c.key)) for c in self._criteria
                       if c.section == section and c.key not in self._unavailable])

    def _request(self, needs):
        """Ask for each (key, source) once; one already answered is shown again
        from its answer (a source switched back must not wait for a worker)."""
        from desktop.gui_threads import CriterionValuesWorker, _keep_until_finished
        for need in needs:
            for kind, answer in self._answers.get(need, {}).items():
                if kind == 'values':
                    self._on_values_loaded(*need, answer)
                else:
                    self._on_coverage(*need, *answer)
        needs = [n for n in needs if n not in self._requested]
        if not needs:
            return
        self._requested.update(needs)
        worker = CriterionValuesWorker(needs, universe=self._corpus)
        worker.loaded.connect(self._on_values_loaded)
        worker.coverage.connect(self._on_coverage)
        worker.failed.connect(self._on_values_failed)
        worker.start()
        _keep_until_finished(worker)  # after start(): it lets go of a worker not running

    def _on_values_loaded(self, key, source, values):
        self._answers.setdefault((key, source), {})['values'] = values
        controls = self._controls.get(key)
        if controls is None or source != self._source_of(key):
            return
        chosen = self._checked(key) if controls['loaded'] is not None else self._pending.get(key, [])
        widget = controls['values']
        widget.blockSignals(True)
        widget.clear()
        offered = {value['value']: value for value in values}
        # A saved value this file does not offer stays chosen and shown --
        # dropping it silently would widen the search -- after the offered
        # values; Unknown, Cannot be determined and Not recorded stay last,
        # in that order, whether offered or kept.
        kept = [value for value in chosen if value not in offered]
        for value in values:
            if value['value'] not in _TAIL_VALUES:
                self._add_offered_item(widget, key, value, chosen)
        for saved in kept:
            if saved not in _TAIL_VALUES:
                self._add_value_item(widget, saved, tr(saved), True)
        for name in _TAIL_VALUES:
            if name in offered:
                self._add_offered_item(widget, key, offered[name], chosen)
            elif name in kept:
                self._add_value_item(widget, name, tr(name) if name != "_none" else _value_label(key, {
                    'value': name, 'label_en': 'Not recorded', 'label_he': 'לא נרשם'}), True)
        widget.blockSignals(False)
        controls['loaded'] = source
        self._pending.pop(key, None)
        controls['search'].setVisible(widget.count() > _LONG_LIST)
        self._filter_values(key, controls['search'].text())

    def _add_offered_item(self, widget, key, value, chosen):
        self._add_value_item(widget, value['value'], f"{_value_label(key, value)} ({value['count']:,})",
                             value['value'] in chosen)

    @staticmethod
    def _add_value_item(widget, value, text, checked):
        item = QListWidgetItem(text)
        item.setData(Qt.ItemDataRole.UserRole, value)
        item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
        widget.addItem(item)

    def _on_coverage(self, key, source, recorded, of):
        self._answers.setdefault((key, source), {})['coverage'] = (recorded, of)
        controls = self._controls.get(key)
        if controls is None or source != self._source_of(key):
            return
        controls['coverage'].setText(tr("recorded for {n} of {total}").format(
            n=f"{recorded:,}", total=f"{of:,}"))

    def _on_values_failed(self, key, source, reason):
        self._requested.discard((key, source))  # a later visit retries
        controls = self._controls.get(key)
        if controls is None or controls['kind'] != 'list' or controls['loaded'] is not None:
            return
        widget = controls['values']
        widget.blockSignals(True)
        widget.clear()
        widget.addItem(QListWidgetItem(tr("These values could not be read.")))
        widget.blockSignals(False)

    def _filter_values(self, key, text):
        widget = self._controls[key]['values']
        needle = (text or '').strip().lower()
        for i in range(widget.count()):
            item = widget.item(i)
            widget.setRowHidden(i, bool(needle) and needle not in item.text().lower()
                                and item.checkState() != Qt.CheckState.Checked)

    def _on_source_changed(self, key):
        controls = self._controls[key]
        controls['coverage'].setText("")
        if controls['kind'] == 'list':
            # Values differ by source (measured Mix, Wood): reload, keeping what is ticked.
            if controls['loaded'] is not None:
                self._pending[key] = self._checked(key)
            controls['loaded'] = None
            controls['values'].blockSignals(True)
            controls['values'].clear()
            controls['values'].addItem(QListWidgetItem(tr("Loading...")))
            controls['values'].blockSignals(False)
        self._request([(key, self._source_of(key))])
        self._changed()

    # -- editing ----------------------------------------------------------------
    def _set_range(self, key, low, high):
        controls = self._controls[key]
        controls['min'].setText(str(low))
        controls['max'].setText(str(high))

    def _reset(self, key):
        controls = self._controls[key]
        if controls['source'] is not None:
            controls['source'].setCurrentIndex(0)
        if controls['kind'] == 'list':
            controls['mode'].setCurrentIndex(0)
            if controls['loaded'] is None:
                self._pending[key] = []
            widget = controls['values']
            widget.blockSignals(True)
            for i in range(widget.count()):
                if widget.item(i).data(Qt.ItemDataRole.UserRole) is not None:
                    widget.item(i).setCheckState(Qt.CheckState.Unchecked)
            widget.blockSignals(False)
        else:
            controls['min'].clear()
            controls['max'].clear()
            controls['not_recorded'].setChecked(False)

    def _clear_section(self):
        row = self.section_list.currentRow()
        if row < 0:
            return
        for criterion in self._criteria:
            if criterion.section == self._sections[row].key and criterion.key not in self._unavailable:
                self._reset(criterion.key)
        self._changed()

    def _clear_all(self):
        self._outside = {}
        for criterion in self._criteria:
            if criterion.key not in self._unavailable:
                self._reset(criterion.key)
        self._changed()

    def _changed(self, *_args):
        self._refresh_badges()
        self._check_bounds()
        self._count_timer.start()

    def _check_bounds(self):
        """A bound being typed that is not a number yet (".", "12.") would be
        read as no bound: Apply waits for it, and the field says so."""
        unfinished = False
        for controls in self._controls.values():
            if controls['kind'] != 'range':
                continue
            for bound in ('min', 'max'):
                edit = controls[bound]
                bad = bool(edit.text().strip()) and not edit.hasAcceptableInput()
                edit.setStyleSheet("border: 1px solid #e74c3c;" if bad else "")
                unfinished = unfinished or bad
        self.ok_btn.setEnabled(not unfinished)

    def _refresh_badges(self):
        """Each section's name with the number of its active criteria."""
        from shared.catalog_criteria import BY_KEY
        active = self.get_criteria()
        for i, section in enumerate(self._sections):
            n = sum(1 for k in active if BY_KEY[k].section == section.key)
            item = self.section_list.item(i)
            item.setText(_label(section) + (f"  ({n})" if n else ""))
            font = item.font()
            font.setBold(bool(n))
            item.setFont(font)

    # -- the live total -----------------------------------------------------------
    def _update_count(self):
        from desktop.gui_threads import CriteriaCountWorker, _keep_until_finished
        self._count_tag += 1
        criteria = self.get_criteria()
        if not criteria:
            self.total_label.setText(tr("No features chosen."))
            self.total_label.setStyleSheet(_COUNT_STYLE_OK)
            self.note_label.setVisible(False)
            return
        self.total_label.setText(tr("Counting..."))
        self.total_label.setStyleSheet(_COUNT_STYLE_OK)
        worker = CriteriaCountWorker(
            criteria, universe=self._within if self._within is not None else self._corpus,
            tag=self._count_tag, note=self._within is None and not self.note_label.text())
        worker.counted.connect(self._on_counted)
        worker.failed.connect(self._on_count_failed)
        worker.start()
        _keep_until_finished(worker)  # after start(): it lets go of a worker not running

    def _on_counted(self, tag, total, info):
        from shared.catalog_criteria import BY_KEY
        if tag != self._count_tag:
            return
        if self._within is not None:
            text = tr("{n} of the {total} manuscripts in the results match").format(
                n=f"{total:,}", total=f"{len(self._within):,}")
        elif total == 0:
            text = tr("No manuscripts match")
        else:
            text = tr("{n} manuscripts have these features").format(n=f"{total:,}")
        for key, n in (info.get('unrecorded') or {}).items():
            if n:
                text += tr(", of which {m} have no {label} recorded").format(
                    m=f"{n:,}", label=_label(BY_KEY[key]))
        self.total_label.setText(text)
        self.total_label.setStyleSheet(_COUNT_STYLE_OK if total else _COUNT_STYLE_ERROR)
        note = info.get('note')
        if note:  # read once per window
            self.note_label.setText(tr(
                "These criteria cover all {corpus} manuscripts. {uncatalogued} have no FJMS catalogue "
                "record: they match 'Not recorded', 'all except' and measured data ({sized} also have a "
                "catalogue size).").format(corpus=f"{note['corpus']:,}",
                                           uncatalogued=f"{note['uncatalogued']:,}",
                                           sized=f"{note['uncatalogued_sized']:,}"))
        self.note_label.setVisible(self._within is None and bool(self.note_label.text()))

    def _on_count_failed(self, tag, reason):
        if tag != self._count_tag:
            return
        self.total_label.setText(
            tr("The catalog data these features need is not available.")
            if reason in ('column_missing', 'sidecar_unavailable')
            else tr("The count could not be computed."))
        self.total_label.setStyleSheet(_COUNT_STYLE_ERROR)


class PreSearchFilterDialog(QDialog):
    """Pre-search filter dialog with multi-select domain, author, work, date range,
    include/exclude mode, text filters, and material controls.

    Allows researchers to constrain the search scope BEFORE executing a search,
    resulting in genuinely faster searches by restricting the candidate set.
    """

    def __init__(self, parent=None, current_filters: dict = None):
        from PyQt6.QtWidgets import QRadioButton, QButtonGroup
        super().__init__(parent)
        self.setWindowTitle(tr("Search only in..."))
        self.setMinimumSize(780, 640)
        # Saved filters in today's form: dates read into Period of copy, the
        # old measurement bounds gone (plan §6.4); and no criterion the open
        # catalogue file is known unable to answer (#17), with a notice below.
        self._current_filters, _ = migrate_saved_filters(current_filters)
        self._criteria, self.dropped_saved_filters = drop_unavailable_criteria(
            self._current_filters.pop('criteria', None))
        self._count_worker = None
        self._count_generation = 0
        self._result_set = None  # computed restrict_sys_ids
        # Store all domain/author/work data for display name lookup
        self._domain_data = {}  # data_key -> display_text
        self._author_data = {}
        self._work_data = {}

        layout = QVBoxLayout(self)

        # --- Include/Exclude mode ---
        mode_layout = QHBoxLayout()
        self._mode_group = QButtonGroup(self)
        self._rb_include = QRadioButton(tr("Include"))
        self._rb_exclude = QRadioButton(tr("Exclude"))
        self._mode_group.addButton(self._rb_include, 1)
        self._mode_group.addButton(self._rb_exclude, 2)
        include_mode = self._current_filters.get('include_mode', True)
        self._rb_include.setChecked(include_mode)
        self._rb_exclude.setChecked(not include_mode)
        self._mode_group.buttonToggled.connect(self._on_filter_changed)
        mode_layout.addWidget(self._rb_include)
        mode_layout.addWidget(self._rb_exclude)
        mode_layout.addStretch()
        layout.addLayout(mode_layout)

        # --- 2-column body layout ---
        from PyQt6.QtWidgets import QTreeWidget, QScrollArea
        self._updating_domain_checks = False

        body_layout = QHBoxLayout()

        # LEFT column: Domain tree
        domain_group = QGroupBox(tr("Subject Domain"))
        domain_layout = QVBoxLayout(domain_group)
        self.domain_filter_edit = QLineEdit()
        self.domain_filter_edit.setPlaceholderText(tr("Filter domains..."))
        self.domain_filter_edit.textChanged.connect(self._filter_domain_list)
        domain_layout.addWidget(self.domain_filter_edit)
        self.domain_tree = QTreeWidget()
        self.domain_tree.setHeaderLabels([tr("Domain"), tr("Manuscripts")])
        self.domain_tree.setColumnWidth(0, 250)
        self._populate_domains()
        # Restore selections
        for d in self._current_filters.get('domains', []):
            self._check_tree_item(self.domain_tree, d, True)
        if not self._current_filters.get('domains') and self._current_filters.get('domain'):
            self._check_tree_item(self.domain_tree, self._current_filters['domain'], True)
        domain_layout.addWidget(self.domain_tree)
        body_layout.addWidget(domain_group, 1)  # stretch factor 1

        # RIGHT column: Author, Work, Date, Material, Text
        right_layout = QVBoxLayout()

        # Author filter (type-ahead dropdown)
        author_group = QGroupBox(tr("Author"))
        author_layout = QVBoxLayout(author_group)
        self._selected_authors = []
        self.author_combo = QComboBox()
        self.author_combo.setEditable(True)
        self.author_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.author_combo.setPlaceholderText(tr("Type to select author..."))
        self.author_combo.addItem("", None)
        _ac = self.author_combo.completer()
        if _ac:
            _ac.setFilterMode(Qt.MatchFlag.MatchContains)
        self._populate_authors()
        for a in self._current_filters.get('authors', []):
            if a not in self._selected_authors:
                self._selected_authors.append(a)
        if not self._current_filters.get('authors') and self._current_filters.get('author'):
            a = self._current_filters['author']
            if a not in self._selected_authors:
                self._selected_authors.append(a)
        self.author_combo.activated.connect(self._on_author_selected)
        author_layout.addWidget(self.author_combo)
        right_layout.addWidget(author_group)

        # Work filter (type-ahead dropdown)
        work_group = QGroupBox(tr("Work"))
        work_layout = QVBoxLayout(work_group)
        self._selected_works = []
        self.work_combo = QComboBox()
        self.work_combo.setEditable(True)
        self.work_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.work_combo.setPlaceholderText(tr("Type to select work..."))
        self.work_combo.addItem("", None)
        _wc = self.work_combo.completer()
        if _wc:
            _wc.setFilterMode(Qt.MatchFlag.MatchContains)
        self._populate_works()
        for w in self._current_filters.get('works', []):
            if w not in self._selected_works:
                self._selected_works.append(w)
        if not self._current_filters.get('works') and self._current_filters.get('work'):
            w = self._current_filters['work']
            if w not in self._selected_works:
                self._selected_works.append(w)
        self.work_combo.activated.connect(self._on_work_selected)
        work_layout.addWidget(self.work_combo)
        right_layout.addWidget(work_group)

        # Manuscript features: period of copy, script, measurements ... in
        # one window (CriteriaDialog). Not turned around by Include/Exclude.
        self.criteria_btn = QPushButton()
        self.criteria_btn.clicked.connect(self._open_criteria_dialog)
        right_layout.addWidget(self.criteria_btn)
        self._update_criteria_button()

        # Type filter (3-way dropdown)
        material_group = QGroupBox(tr("Type"))
        material_layout = QVBoxLayout(material_group)
        self.material_combo = QComboBox()
        self.material_combo.addItem(tr("Show all"), "all")
        self.material_combo.addItem(tr("Exclude printed"), "mss_only")
        self.material_combo.addItem(tr("Printed only"), "printed_only")
        # Restore from current filters
        material_exclude = self._current_filters.get('material_exclude', [])
        material_include = self._current_filters.get('material_include', [])
        if 'Printed' in (material_exclude or []):
            self.material_combo.setCurrentIndex(1)  # Manuscripts only
        elif 'Printed' in (material_include or []):
            self.material_combo.setCurrentIndex(2)  # Printed only
        self.material_combo.currentIndexChanged.connect(self._on_filter_changed)
        material_layout.addWidget(self.material_combo)
        right_layout.addWidget(material_group)

        # Text filter (catalog data)
        text_group = QGroupBox(tr("Catalog Data Text"))
        text_layout = QVBoxLayout(text_group)
        text_input_layout = QHBoxLayout()
        self.text_mode_combo = QComboBox()
        self.text_mode_combo.addItem(tr("All words"), "all")
        self.text_mode_combo.addItem(tr("Any word"), "any")
        self.text_mode_combo.addItem(tr("Not these words"), "not")
        text_input_layout.addWidget(self.text_mode_combo)
        self.text_term_edit = QLineEdit()
        self.text_term_edit.setPlaceholderText(tr("Add term"))
        self.text_term_edit.returnPressed.connect(self._add_text_term)
        text_input_layout.addWidget(self.text_term_edit)
        add_term_btn = QPushButton("+")
        add_term_btn.setFixedWidth(30)
        add_term_btn.clicked.connect(self._add_text_term)
        text_input_layout.addWidget(add_term_btn)
        text_layout.addLayout(text_input_layout)
        right_layout.addWidget(text_group)


        body_layout.addLayout(right_layout, 1)  # stretch factor 1
        layout.addLayout(body_layout)

        # Initialize text filter state
        self._text_all = list(self._current_filters.get('text_all', []))
        self._text_any = list(self._current_filters.get('text_any', []))
        self._text_not = list(self._current_filters.get('text_not', []))

        # --- Unified chip bar (all active filters) ---
        chip_scroll = QScrollArea()
        chip_scroll.setWidgetResizable(True)
        chip_scroll.setMaximumHeight(50)
        chip_scroll.setFrameShape(chip_scroll.Shape.NoFrame)
        chip_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.chip_bar_widget = QWidget()
        self.chip_bar_layout = QHBoxLayout(self.chip_bar_widget)
        self.chip_bar_layout.setContentsMargins(0, 2, 0, 2)
        self.chip_bar_layout.setSpacing(4)
        self.chip_bar_layout.addStretch()
        chip_scroll.setWidget(self.chip_bar_widget)
        layout.addWidget(chip_scroll)
        self.domain_tree.itemChanged.connect(self._on_domain_tree_changed)
        self._rebuild_dialog_chips()

        # --- Saved filters this catalog cannot answer (#17) ---
        self.dropped_notice_label = QLabel(
            tr('Some saved filters were removed: this catalog data is not available.'))
        self.dropped_notice_label.setWordWrap(True)
        self.dropped_notice_label.setStyleSheet("color: #e67e22; font-size: 11px; padding: 2px 4px;")
        self.dropped_notice_label.setVisible(bool(self.dropped_saved_filters))
        layout.addWidget(self.dropped_notice_label)

        # --- Manuscript count ---
        self.count_label = QLabel("")
        self.count_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #2980b9; padding: 4px;")
        layout.addWidget(self.count_label)

        # --- Buttons ---
        btn_layout = QHBoxLayout()
        clear_btn = QPushButton(tr("Clear All"))
        clear_btn.clicked.connect(self._clear_all)
        btn_layout.addWidget(clear_btn)
        btn_layout.addStretch()

        self.ok_btn = QPushButton(tr("OK"))
        self.ok_btn.setDefault(True)
        self.ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton(tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        # Initial count update
        self._update_count()

    def _open_criteria_dialog(self):
        """Manuscript features: choose criteria in their own window, then recount."""
        dlg = CriteriaDialog(self, self._criteria, universe=self._corpus())
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        self._criteria = dlg.get_criteria()
        self._update_criteria_button()
        self._on_filter_changed()

    def _update_criteria_button(self):
        n = len(self._criteria)
        self.criteria_btn.setText(tr("Manuscript features") + (f" ({n})" if n else "") + "…")
        font = self.criteria_btn.font()
        font.setBold(bool(n))
        self.criteria_btn.setFont(font)

    def _remove_criterion(self, key):
        self._criteria.pop(key, None)
        self._update_criteria_button()
        self._on_filter_changed()

    def _corpus(self):
        """The corpus list for D17 and the criteria (MetadataManager.corpus_sys_ids
        of the main window; None in tests without one)."""
        try:
            owner = self.parent()
        except RuntimeError:  # a dialog whose Qt side was never built (tests)
            owner = None
        return getattr(getattr(owner, 'meta_mgr', None), 'corpus_sys_ids', None)

    @staticmethod
    def _check_list_item(list_widget, data_value, checked):
        """Check/uncheck a QListWidget item by its data value."""
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            if item.data(Qt.ItemDataRole.UserRole) == data_value:
                item.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
                break

    @staticmethod
    def _check_tree_item(tree_widget, data_value, checked):
        """Check/uncheck a QTreeWidget item by its data value (searches all levels)."""
        state = Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked
        root = tree_widget.invisibleRootItem()
        for i in range(root.childCount()):
            parent = root.child(i)
            if parent.data(0, Qt.ItemDataRole.UserRole) == data_value:
                parent.setCheckState(0, state)
                return
            for j in range(parent.childCount()):
                child = parent.child(j)
                if child.data(0, Qt.ItemDataRole.UserRole) == data_value:
                    child.setCheckState(0, state)
                    return

    @staticmethod
    def _get_checked_items(list_widget):
        """Return list of data values for all checked items."""
        result = []
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                data = item.data(Qt.ItemDataRole.UserRole)
                if data is not None:
                    result.append(data)
        return result

    @staticmethod
    def _get_checked_tree_items(tree_widget):
        """Return list of data values for all checked leaf/child items in a QTreeWidget (up to 3 levels)."""
        result = []
        root = tree_widget.invisibleRootItem()
        for i in range(root.childCount()):
            parent = root.child(i)
            if parent.childCount() == 0:
                # Leaf parent (no children)
                if parent.checkState(0) == Qt.CheckState.Checked:
                    data = parent.data(0, Qt.ItemDataRole.UserRole)
                    if data is not None:
                        result.append(data)
            else:
                for j in range(parent.childCount()):
                    child = parent.child(j)
                    if child.childCount() == 0:
                        if child.checkState(0) == Qt.CheckState.Checked:
                            data = child.data(0, Qt.ItemDataRole.UserRole)
                            if data is not None:
                                result.append(data)
                    else:
                        # Third level: grandchildren
                        for k in range(child.childCount()):
                            grandchild = child.child(k)
                            if grandchild.checkState(0) == Qt.CheckState.Checked:
                                data = grandchild.data(0, Qt.ItemDataRole.UserRole)
                                if data is not None:
                                    result.append(data)
        return result

    def _populate_domains(self):
        """Populate domain tree with hierarchy from FJMS."""
        from PyQt6.QtWidgets import QTreeWidgetItem
        try:
            from shared.fjms_service import get_fjms_service, qualify_domain_name
            fjms = get_fjms_service()
            if not fjms.is_available():
                return
            self.domain_tree.blockSignals(True)
            hierarchy = fjms.get_domain_hierarchy()
            for parent_name, info in hierarchy.items():
                parent_heb = info.get('parent_domain_heb', '')
                display = parent_heb if CURRENT_LANG == 'he' and parent_heb else parent_name
                count = info.get('count', 0)
                display_text = f"{display} ({count:,})"
                parent_item = QTreeWidgetItem([display, f"{count:,}"])
                parent_item.setFlags(parent_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                parent_item.setCheckState(0, Qt.CheckState.Unchecked)
                parent_item.setData(0, Qt.ItemDataRole.UserRole, parent_name)
                self.domain_tree.addTopLevelItem(parent_item)
                self._domain_data[parent_name] = display_text
                for child in info.get('children', []):
                    child_heb = child.get('domain_heb', '')
                    child_count = child.get('count', 0)
                    qname = qualify_domain_name(child['domain'], parent_name)
                    if CURRENT_LANG == 'he' and child_heb:
                        child_display = f"{child_heb} ({parent_heb})" if qname != child['domain'] else child_heb
                    else:
                        child_display = qname
                    child_text = f"{child_display} ({child_count:,})"
                    child_item = QTreeWidgetItem([child_display, f"{child_count:,}"])
                    child_item.setFlags(child_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    child_item.setCheckState(0, Qt.CheckState.Unchecked)
                    child_item.setData(0, Qt.ItemDataRole.UserRole, qname)
                    parent_item.addChild(child_item)
                    self._domain_data[qname] = child_text
                    # Third level: sub-sub-domains
                    for sc in child.get('children', []):
                        sc_heb = sc.get('domain_heb', '')
                        sc_count = sc.get('count', 0)
                        sc_qname = qualify_domain_name(sc['domain'], child['domain'])
                        if CURRENT_LANG == 'he' and sc_heb:
                            sc_display = f"{sc_heb} ({child_heb})" if sc_qname != sc['domain'] else sc_heb
                        else:
                            sc_display = sc_qname
                        sc_text = f"{sc_display} ({sc_count:,})"
                        sc_item = QTreeWidgetItem([sc_display, f"{sc_count:,}"])
                        sc_item.setFlags(sc_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                        sc_item.setCheckState(0, Qt.CheckState.Unchecked)
                        sc_item.setData(0, Qt.ItemDataRole.UserRole, sc_qname)
                        child_item.addChild(sc_item)
                        self._domain_data[sc_qname] = sc_text
            self.domain_tree.blockSignals(False)
            self.domain_tree.expandAll()
        except Exception:
            pass  # UI population failed; continue with available data

    def _populate_authors(self, domain=None):
        """Populate author dropdown, optionally filtered by first selected domain."""
        self.author_combo.blockSignals(True)
        self.author_combo.clear()
        self._author_data = {}
        self.author_combo.addItem("", None)  # placeholder
        try:
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service()
            if not fjms.is_available():
                return
            _first = domain[0] if isinstance(domain, list) and domain else (None if isinstance(domain, list) else domain)
            authors = fjms.get_browse_authors(domain=_first)
            for author in authors:
                heb = author.get('heb_desc', '')
                eng = author.get('eng_desc', '')
                name = heb or eng or author.get('author_name', '')
                if eng and eng != name:
                    name = f"{name} / {eng}"
                count = author.get('count', 0)
                author_id = str(author.get('person_id') or author.get('author_id') or name)
                display_text = f"{name} ({count:,})"
                self.author_combo.addItem(display_text, author_id)
                self._author_data[author_id] = display_text
        except Exception:
            pass  # Catalog/FJMS operation failed; continue with available data
        finally:
            self.author_combo.setCurrentIndex(0)
            self.author_combo.blockSignals(False)

    def _populate_works(self, domain=None, author=None):
        """Populate work dropdown, optionally filtered by domain and author."""
        self.work_combo.blockSignals(True)
        self.work_combo.clear()
        self._work_data = {}
        self.work_combo.addItem("", None)  # placeholder
        try:
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service()
            if not fjms.is_available():
                return
            _first_d = domain[0] if isinstance(domain, list) and domain else (None if isinstance(domain, list) else domain)
            _first_a = author[0] if isinstance(author, list) and author else (None if isinstance(author, list) else author)
            works = fjms.get_browse_works(domain=_first_d, author=_first_a)
            for work in works:
                org = work.get('org_title', '')
                eng = work.get('eng_title', '')
                name = org or eng
                if eng and eng != org:
                    name = f"{org} / {eng}"
                count = work.get('count', 0)
                work_id = str(work.get('title_id') or name)
                display_text = f"{name} ({count:,})"
                self.work_combo.addItem(display_text, work_id)
                self._work_data[work_id] = display_text
        except Exception:
            pass  # Catalog/FJMS operation failed; continue with available data
        finally:
            self.work_combo.setCurrentIndex(0)
            self.work_combo.blockSignals(False)

    def _filter_domain_list(self, text):
        """Filter domain tree items by search text."""
        search_text = text.lower() if text else ''
        root = self.domain_tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent_item = root.child(i)
            parent_match = search_text in parent_item.text(0).lower() if search_text else True
            any_child_match = False
            for j in range(parent_item.childCount()):
                child_item = parent_item.child(j)
                child_match = search_text in child_item.text(0).lower() if search_text else True
                child_item.setHidden(not child_match and not parent_match)
                if child_match:
                    any_child_match = True
            parent_item.setHidden(not parent_match and not any_child_match)

    def _on_domain_tree_changed(self, item, column):
        """Handle domain tree checkbox with parent-child propagation."""
        if self._updating_domain_checks or column != 0:
            return
        self._updating_domain_checks = True
        state = item.checkState(0)
        # Propagate to all visible descendants (children + grandchildren)
        def propagate_down(parent_item, st):
            for i in range(parent_item.childCount()):
                child = parent_item.child(i)
                if not child.isHidden():
                    child.setCheckState(0, st)
                    propagate_down(child, st)
        propagate_down(item, state)
        self._updating_domain_checks = False
        self._on_domain_changed()

    def _on_domain_changed(self, item=None):
        """When domain selection changes, re-populate authors and works."""
        domains = self._get_checked_tree_items(self.domain_tree)
        self._populate_authors(domain=domains)
        self._populate_works(domain=domains)
        self._on_filter_changed()

    def _on_author_selected(self, index):
        """Handle author dropdown selection — add to selected list."""
        if index <= 0:
            return
        author_id = self.author_combo.itemData(index)
        if author_id and author_id not in self._selected_authors:
            self._selected_authors.append(author_id)
        self.author_combo.setCurrentIndex(0)
        self._on_author_changed()

    def _on_work_selected(self, index):
        """Handle work dropdown selection — add to selected list."""
        if index <= 0:
            return
        work_id = self.work_combo.itemData(index)
        if work_id and work_id not in self._selected_works:
            self._selected_works.append(work_id)
        self.work_combo.setCurrentIndex(0)
        self._on_filter_changed()

    def _on_author_changed(self, item=None):
        """When author selection changes, re-populate works."""
        domains = self._get_checked_tree_items(self.domain_tree)
        self._populate_works(domain=domains, author=self._selected_authors)
        self._on_filter_changed()

    def _on_filter_changed(self, *args):
        """Any filter changed -- update count and chip bar."""
        self._rebuild_dialog_chips()
        self._update_count()

    def _add_text_term(self):
        """Add a text filter term from the input."""
        term = self.text_term_edit.text().strip()
        if not term:
            return
        mode = self.text_mode_combo.currentData()
        target = {'all': self._text_all, 'any': self._text_any, 'not': self._text_not}.get(mode, self._text_all)
        if term not in target:
            target.append(term)
        self.text_term_edit.clear()
        self._on_filter_changed()

    def _remove_text_term(self, mode, term):
        """Remove a text filter term."""
        target = {'all': self._text_all, 'any': self._text_any, 'not': self._text_not}.get(mode)
        if target and term in target:
            target.remove(term)
        self._on_filter_changed()

    def _make_chip(self, text, bg_color, on_click):
        """Create a removable chip button (dark-mode aware)."""
        from desktop.widgets.filter_chip import make_filter_chip
        return make_filter_chip(text, bg_color, on_click)

    def _rebuild_dialog_chips(self):
        """Rebuild unified chip bar showing all active filters."""
        if not hasattr(self, 'chip_bar_layout'):
            return
        while self.chip_bar_layout.count() > 0:
            item = self.chip_bar_layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

        # Domain chips
        for d in self._get_checked_tree_items(self.domain_tree):
            name = self._get_display_name(d, self._domain_data)
            self.chip_bar_layout.addWidget(self._make_chip(
                name, '#e8f4fd',
                lambda checked=False, key=d: (self._check_tree_item(self.domain_tree, key, False), self._on_domain_changed())))

        # Author chips
        for a in self._selected_authors:
            name = self._get_display_name(a, self._author_data)
            self.chip_bar_layout.addWidget(self._make_chip(
                name, '#f3e5f5',
                lambda checked=False, key=a: (self._selected_authors.remove(key), self._on_author_changed())))

        # Work chips
        for w in self._selected_works:
            name = self._get_display_name(w, self._work_data)
            self.chip_bar_layout.addWidget(self._make_chip(
                name, '#fff3e0',
                lambda checked=False, key=w: (self._selected_works.remove(key), self._on_filter_changed())))

        # Material chip
        mat_mode = self.material_combo.currentData()
        if mat_mode == 'mss_only':
            self.chip_bar_layout.addWidget(self._make_chip(
                tr("Exclude printed"), '#ffebee',
                lambda: self.material_combo.setCurrentIndex(0)))
        elif mat_mode == 'printed_only':
            self.chip_bar_layout.addWidget(self._make_chip(
                tr("Printed only"), '#ffebee',
                lambda: self.material_combo.setCurrentIndex(0)))

        # Manuscript features: one chip per criterion
        for key, text in criteria_chip_texts(self._criteria):
            self.chip_bar_layout.addWidget(self._make_chip(
                text, '#efebe9', lambda checked=False, k=key: self._remove_criterion(k)))

        # Text term chips
        for mode, prefix, terms in [('all', '+', self._text_all), ('any', '~', self._text_any), ('not', '-', self._text_not)]:
            for t in terms:
                color = '#e8f5e9' if mode == 'all' else '#e3f2fd' if mode == 'any' else '#ffebee'
                self.chip_bar_layout.addWidget(self._make_chip(
                    f"{prefix} {t}", color,
                    lambda checked=False, m=mode, term=t: self._remove_text_term(m, term)))

        self.chip_bar_layout.addStretch()

    def _get_current_filter_dict(self) -> dict:
        """Build filter dict from current dialog state."""
        filters = {}
        domains = self._get_checked_tree_items(self.domain_tree)
        if domains:
            filters['domains'] = domains
        if self._selected_authors:
            filters['authors'] = list(self._selected_authors)
        if self._selected_works:
            filters['works'] = list(self._selected_works)
        filters['include_mode'] = self._rb_include.isChecked()
        mat_mode = self.material_combo.currentData()
        if mat_mode == 'mss_only':
            filters['material_exclude'] = ['Printed']
        elif mat_mode == 'printed_only':
            filters['material_include'] = ['Printed']
        if self._criteria:
            filters['criteria'] = dict(self._criteria)
        if self._text_all:
            filters['text_all'] = list(self._text_all)
        if self._text_any:
            filters['text_any'] = list(self._text_any)
        if self._text_not:
            filters['text_not'] = list(self._text_not)
        return filters

    def _get_display_name(self, key, data_map):
        """Get display name (without count) from data map."""
        text = data_map.get(key, key)
        return text.rsplit(' (', 1)[0].strip().lstrip().strip() if isinstance(text, str) else str(key)

    def _update_count(self):
        """Recompute manuscript count in background thread."""
        self._count_generation += 1
        generation = self._count_generation
        filters = self._get_current_filter_dict()
        # Check if any actual filter is set (not just include_mode)
        has_filter = any(k != 'include_mode' for k in filters)
        if not has_filter:
            self.count_label.setText(tr("All manuscripts (no filters)"))
            self.count_label.setStyleSheet(_COUNT_STYLE_OK)
            self._result_set = None
            if hasattr(self, 'ok_btn'):
                self.ok_btn.setEnabled(True)
            return
        self.count_label.setText(tr("Counting..."))
        if hasattr(self, 'ok_btn'):
            self.ok_btn.setEnabled(False)
        # D17: Exclude-only filters keep the manuscripts with no catalogue
        # record, so the count needs the corpus list (read only if it applies).
        try:
            owner = self.parent()
        except RuntimeError:  # a dialog whose Qt side was never built (tests)
            owner = None
        corpus = getattr(getattr(owner, 'meta_mgr', None), 'corpus_sys_ids', None)
        self._count_worker = FilterCountWorker(filters, self, universe=corpus)
        self._count_worker.finished.connect(
            lambda result_set, gen=generation: self._on_count_finished(result_set, gen)
        )
        self._count_worker.failed.connect(
            lambda reason, gen=generation: self._on_count_failed(reason, gen)
        )
        self._count_worker.start()

    def _on_count_failed(self, reason, generation=None):
        """The filters could not be evaluated (#17): say so, and keep OK
        disabled -- accepting would store None, which means an unrestricted
        search. The user can change the filters, Clear All, or Cancel."""
        if generation is not None and generation != self._count_generation:
            return
        self._result_set = None
        self.count_label.setText(tr("Could not update filter count"))
        self.count_label.setStyleSheet(_COUNT_STYLE_ERROR)
        if hasattr(self, 'ok_btn'):
            self.ok_btn.setEnabled(False)

    def _on_count_finished(self, result_set, generation=None):
        """Handle count worker result."""
        if generation is not None and generation != self._count_generation:
            return
        self._result_set = result_set
        if result_set is None:
            self.count_label.setText(tr("All manuscripts (no filters)"))
            self.count_label.setStyleSheet(_COUNT_STYLE_OK)
        elif len(result_set) == 0:
            self.count_label.setText(tr("No manuscripts match"))
            self.count_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #e74c3c; padding: 4px;")
        else:
            count_str = f"{len(result_set):,}"
            self.count_label.setText(f"{count_str} {tr('manuscripts')}")
            self.count_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #2980b9; padding: 4px;")
        if hasattr(self, 'ok_btn'):
            self.ok_btn.setEnabled(True)

    def _clear_all(self):
        """Reset all filter controls to default."""
        # Clear domain tree
        self.domain_tree.blockSignals(True)
        root = self.domain_tree.invisibleRootItem()
        for i in range(root.childCount()):
            parent = root.child(i)
            parent.setCheckState(0, Qt.CheckState.Unchecked)
            for j in range(parent.childCount()):
                parent.child(j).setCheckState(0, Qt.CheckState.Unchecked)
        self.domain_tree.blockSignals(False)
        # Clear author/work selections
        self._selected_authors.clear()
        self._selected_works.clear()
        self.author_combo.setCurrentIndex(0)
        self.work_combo.setCurrentIndex(0)
        self._rb_include.setChecked(True)
        self.material_combo.setCurrentIndex(0)
        self._criteria = {}
        self._update_criteria_button()
        self._text_all.clear()
        self._text_any.clear()
        self._text_not.clear()
        self._rebuild_dialog_chips()
        self._update_count()

    def get_filters(self) -> dict:
        """Return the current filter state dict."""
        return self._get_current_filter_dict()

    def get_restrict_sys_ids(self):
        """Return the computed restrict_sys_ids set (or None)."""
        return self._result_set


# ── Library Filter (GAP-G / 129-07) ────────────────────────────────────────

def library_apply_selection(checked: list, all_codes: list) -> list:
    """Map the dialog's checked codes to the _catalog_library_filter sentinel value.

    Inclusion model:
      * all checked (== all_codes) -> [] (show-all sentinel; no filter active)
      * strict subset             -> that subset list (ordered by checked position)

    This function is ONLY called when the OK guard allows accept (i.e. checked is
    non-empty), so the all-unchecked -> [] -> show-all collision is structurally
    prevented by the dialog.
    """
    if set(checked) == set(all_codes):
        return []
    return list(checked)


class LibraryFilterDialog(QDialog):
    """Flat checkable library filter dialog mirroring DomainFilterDialog.

    Dual-mode (DMF-07 — Phase 131 Plan 03):
      * mode='show_only': Show only selected libraries (allowlist). All checked by
        default when ``selected_codes`` is empty (= show all).
      * mode='hide': Hide selected libraries (denylist). Empty hide-set = show all.
        OK is enabled even with zero checked (D-05/D-08).
      * D-04: toggling mode resets the checked set (prevents silent inversion of intent).
      * ``get_mode()`` returns the active mode string.
      * Universe is ``library_codes_with_manuscripts()`` minus ``'LOCAL'`` (DMF-13).
      * ``get_checked_codes()`` returns the list of currently-checked codes
        in stable (list order) sequence.
      * ``facets`` is an optional ``{library_code: count}`` dict (dynamic full-set counts
        honoring the catalog's other active filters); rows render ``Name (count)`` when a
        count is present, name-only otherwise (DMF-07/DMF-12 desktop facet counts).
      * Type-to-find search box (placeholder ``tr('Search libraries...')``) hides every row
        whose display label does not contain the query (case-insensitive substring); checkbox
        state is preserved across hide/show; hidden rows still count as checked in
        ``get_checked_codes()`` and the OK guard — at parity with web /catalog
        ``catLibFilterSearch``.
      * A-Z / By-count sort toggle: By count orders by ``self._facets`` descending (0/missing
        last), falls back to A-Z when facets empty; A-Z is the default (ascending by display
        name) — at parity with web /catalog ``catLibFilterSort``.
    """

    def __init__(self, parent=None, *, mode: str = 'hide', selected_codes: list | None = None,
                 facets: dict | None = None):
        super().__init__(parent)
        from genizah_core import get_library_display  # local import to avoid circular
        from shared.browse_map_utils import library_codes_with_manuscripts
        self.setWindowTitle(tr("Filter by Library"))
        self.setMinimumSize(360, 500)
        self._facets = facets if isinstance(facets, dict) else {}
        self._all_codes = [c for c in library_codes_with_manuscripts() if c != 'LOCAL']
        self._get_library_display = get_library_display  # keep a reference for _repopulate
        # GAP-131-09: always append the code in parens (both EN and HE UI) so users can
        # type 'CUL' / 'JTS' in the type-to-find box regardless of the active language.
        self._with_code = True

        layout = QVBoxLayout(self)

        # Mode toggle — Show only selected / Hide selected (D-03)
        from PyQt6.QtWidgets import QRadioButton, QButtonGroup
        mode_layout = QHBoxLayout()
        self._mode_group = QButtonGroup(self)
        self._rb_show_only = QRadioButton(tr("Show only selected"))
        self._rb_hide = QRadioButton(tr("Hide selected"))
        self._mode_group.addButton(self._rb_show_only, 1)
        self._mode_group.addButton(self._rb_hide, 2)
        if mode == 'show_only':
            self._rb_show_only.setChecked(True)
        else:
            self._rb_hide.setChecked(True)
        self._mode_group.buttonToggled.connect(self._on_mode_changed)
        mode_layout.addWidget(self._rb_show_only)
        mode_layout.addWidget(self._rb_hide)
        mode_layout.addStretch()
        layout.addLayout(mode_layout)

        # BUG-D fix: sort by display name alphabetically for a clear, predictable order.
        # get_library_display uses the current CURRENT_LANG so Hebrew UI gets Hebrew names.
        self._all_codes = sorted(
            self._all_codes,
            key=lambda c: get_library_display(c, short=False),
        )

        # Search box — type-to-find (catLibFilterSearch parity)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Search libraries..."))
        self.search_input.setClearButtonEnabled(True)
        layout.addWidget(self.search_input)
        self.search_input.textChanged.connect(self._apply_search_filter)

        # Sort toggle — A-Z / By count (catLibFilterSort parity)
        sort_layout = QHBoxLayout()
        sort_label = QLabel(tr("Sort:"))
        sort_label.setStyleSheet("color: #888; font-size: 11px;")
        sort_layout.addWidget(sort_label)
        self._sort_group = QButtonGroup(self)
        self._rb_sort_az = QRadioButton(tr("A–Z"))      # EN-DASH U+2013 matches web tr('A–Z')
        self._rb_sort_count = QRadioButton(tr("By count"))
        self._rb_sort_az.setChecked(True)  # A-Z is the default
        self._sort_group.addButton(self._rb_sort_az, 1)
        self._sort_group.addButton(self._rb_sort_count, 2)
        sort_layout.addWidget(self._rb_sort_az)
        sort_layout.addWidget(self._rb_sort_count)
        sort_layout.addStretch()
        layout.addLayout(sort_layout)

        # List widget with per-item checkboxes (flat, one row per library code)
        self.list_widget = QListWidget()
        self.list_widget.setSpacing(2)
        layout.addWidget(self.list_widget)

        # Populate list — initial checked state is mode-aware (DMF-07)
        active_set = set(selected_codes) if selected_codes else set()
        # Show-only: empty selected_codes = show all (all-checked); Hide: start unchecked
        if mode == 'show_only':
            all_checked = len(active_set) == 0
        else:  # hide
            all_checked = False
        initial_checked = set(self._all_codes) if all_checked else active_set
        self._populate_rows(self._all_codes, initial_checked)

        # Connect sort toggle AFTER initial population so it doesn't fire during __init__
        self._sort_group.buttonToggled.connect(self._on_sort_changed)

        # Status hint label (shown when OK guard fires)
        self._hint_label = QLabel("")
        self._hint_label.setStyleSheet("color: #c0392b; font-size: 11px;")
        self._hint_label.setVisible(False)
        layout.addWidget(self._hint_label)

        # Buttons row
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton(tr("Select All"))
        select_all_btn.clicked.connect(self._select_all)
        btn_layout.addWidget(select_all_btn)
        # BUG-C: "Select none" unchecks all checkboxes (convenience), but does NOT
        # commit — OK remains disabled at zero checked (_update_ok_button guard).
        select_none_btn = QPushButton(tr("Select None"))
        select_none_btn.clicked.connect(self._select_none)
        btn_layout.addWidget(select_none_btn)
        btn_layout.addStretch()

        self.ok_button = QPushButton(tr("OK"))
        self.ok_button.setDefault(True)
        self.ok_button.clicked.connect(self._on_accept)
        cancel_btn = QPushButton(tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.ok_button)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        # Connect guard
        self.list_widget.itemChanged.connect(self._on_item_changed)
        self._update_ok_button()

    def _populate_rows(self, codes, checked_set):
        """Build list rows for the given code order with check state from ``checked_set``.

        This helper is called both from ``__init__`` (initial population with mode-aware
        ``checked_set``) and from ``_repopulate`` (re-sort preserving existing check state).
        ``checked_set`` must be a ``set`` of library codes to render as Checked.
        """
        get_library_display = self._get_library_display
        for code in codes:
            # GAP-131-08: pass with_code in Hebrew UI so the code appears in the label
            # and becomes searchable via _apply_search_filter (which matches item.text()).
            # Sort sites (in __init__ and _repopulate) do NOT pass with_code so order
            # is keyed on the bare name and remains unchanged.
            base_label = get_library_display(code, short=False, with_code=self._with_code)
            count = self._facets.get(code)
            if isinstance(count, int) and count >= 0:
                label = f"{base_label}  ({count:,})"
            else:
                label = base_label
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, code)
            if code in checked_set:
                item.setCheckState(Qt.CheckState.Checked)
            else:
                item.setCheckState(Qt.CheckState.Unchecked)
            self.list_widget.addItem(item)

    def _apply_search_filter(self, *args):
        """Hide rows whose display label does not contain the active query string.

        Case-insensitive substring match on ``item.text()`` (the display label including
        the count suffix).  Empty query shows all rows.  Does NOT change check state —
        hidden-but-checked rows still count in ``get_checked_codes()`` (web parity).
        """
        q = self.search_input.text().strip().lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setHidden(bool(q) and q not in item.text().lower())

    def _on_sort_changed(self, *args):
        """Re-sort the list when the sort radio changes.

        ``QButtonGroup.buttonToggled`` emits ``(button, checked)`` — ignore the
        deactivation signal (``checked=False``) of the previously-active button.
        """
        if len(args) >= 2 and not args[1]:
            return  # deactivation of the old button — nothing to do
        self._repopulate()

    def _repopulate(self):
        """Re-sort the list while preserving check state and the active search filter.

        Sort order:
          * By count (``self._rb_sort_count`` checked AND ``self._facets`` non-empty):
            sort by ``self._facets.get(code, 0)`` DESCENDING, tie-break A-Z.
            Codes with 0 / missing count cluster at the end (web parity).
          * A-Z (default or By-count fallback when ``self._facets`` is empty):
            sort by ``get_library_display`` ascending.
        After sorting the rows, re-applies the active search filter so a re-sort
        never loses the typed query.
        """
        # Capture current check state before clearing
        checked_set = set(self.get_checked_codes())

        get_library_display = self._get_library_display
        if self._rb_sort_count.isChecked() and self._facets:
            # By count descending, tie-break by display name ascending
            ordered = sorted(
                self._all_codes,
                key=lambda c: (-self._facets.get(c, 0), get_library_display(c, short=False)),
            )
        else:
            # A-Z (default) or By-count fallback when facets empty
            ordered = sorted(
                self._all_codes,
                key=lambda c: get_library_display(c, short=False),
            )

        self.list_widget.blockSignals(True)
        self.list_widget.clear()
        self._populate_rows(ordered, checked_set)
        self.list_widget.blockSignals(False)

        # Re-apply the active search query so a re-sort does not lose the filter
        self._apply_search_filter()
        self._update_ok_button()

    def get_mode(self) -> str:
        """Return the active mode: 'show_only' or 'hide'."""
        return 'show_only' if self._rb_show_only.isChecked() else 'hide'

    def _on_mode_changed(self, *args):
        """D-04: mode flip resets the checked set (prevents silent inversion of intent).

        NOTE: QButtonGroup.buttonToggled emits (button, checked) — slot MUST accept *args
        (mirror the live analog _on_filter_changed(self, *args) at dialogs_filter.py:1368).
        Only act on the activation signal (checked=True); ignore the deactivation signal
        from the previously-active button so clicking the already-active radio does not
        unexpectedly clear all checkboxes.
        """
        # args[1] is the `checked` bool from buttonToggled(button, checked)
        if len(args) >= 2 and not args[1]:
            return  # deactivation of the old button — nothing to do
        self.list_widget.blockSignals(True)
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
        self.list_widget.blockSignals(False)
        self._update_ok_button()

    def _on_item_changed(self, item):
        """Re-evaluate the OK guard whenever a checkbox changes."""
        self._update_ok_button()

    def _update_ok_button(self):
        """Mode-aware OK guard (DMF-07).

        Hide mode: enable OK unconditionally (empty hide-set = show all, D-05/D-08).
        Show-only mode: enable OK only when at least one library is checked.
        """
        mode = self.get_mode()
        checked_count = sum(
            1 for i in range(self.list_widget.count())
            if self.list_widget.item(i).checkState() == Qt.CheckState.Checked
        )
        if mode == 'hide':
            self.ok_button.setEnabled(True)       # empty hide-set = show all (D-05/D-08)
            self._hint_label.setVisible(False)
        else:  # show_only
            self.ok_button.setEnabled(checked_count > 0)
            if checked_count == 0:
                self._hint_label.setText(
                    tr("Select at least one library, or check all to clear the filter")
                )
                self._hint_label.setVisible(True)
            else:
                self._hint_label.setVisible(False)

    def _select_all(self):
        """Re-check all items (= clear the filter / show all)."""
        self.list_widget.blockSignals(True)
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.CheckState.Checked)
        self.list_widget.blockSignals(False)
        self._update_ok_button()

    def _select_none(self):
        """Uncheck all items (convenience only — does NOT apply).

        BUG-C fix: unchecking all is a convenience action that does NOT commit.
        OK stays disabled while zero are checked (_update_ok_button guard).
        The all-unchecked state cannot be committed via OK because _on_accept
        also checks get_checked_codes() before calling self.accept().
        """
        self.list_widget.blockSignals(True)
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.CheckState.Unchecked)
        self.list_widget.blockSignals(False)
        self._update_ok_button()

    def _on_accept(self):
        """Mode-aware guard: reject only when Show-only mode has zero checked.

        Hide mode: empty checked is valid (= hide nothing = show all, D-05/D-08).
        """
        mode = self.get_mode()
        checked = self.get_checked_codes()
        if mode == 'show_only' and not checked:
            self._hint_label.setText(
                tr("Select at least one library, or check all to clear the filter")
            )
            self._hint_label.setVisible(True)
            return
        # Hide mode: empty checked is valid (= hide nothing = show all)
        self.accept()

    def get_checked_codes(self) -> list:
        """Return the ordered list of currently-checked library codes."""
        codes = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                codes.append(item.data(Qt.ItemDataRole.UserRole))
        return codes

