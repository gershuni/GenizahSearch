"""Filter dialogs extracted from genizah_app.py (v7.9 decomposition)."""

import re

from PyQt6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDoubleSpinBox, QFileDialog,
    QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QPlainTextEdit, QPushButton, QSpinBox, QToolTip,
    QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget,
)
from PyQt6.QtCore import QEvent, Qt
from PyQt6.QtGui import QColor, QCursor, QFontMetrics

from genizah_core import CURRENT_LANG, normalize_shelfmark, tr
from gui_threads import FilterCountWorker
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
        """Resolve shelfmarks from the text areas and show resolution report table."""
        from PyQt6.QtWidgets import QTableWidgetItem
        shelf_lines = [l.strip() for l in self.shelf_text_area.toPlainText().splitlines() if l.strip()]
        if not shelf_lines:
            self._report_label.setText(tr("No shelfmarks to resolve"))
            return
        self._ensure_shelf_map()
        if not self._shelf_to_sys:
            self._report_label.setText(tr("Shelf map not available"))
            return
        ids, unresolved, entries = resolve_shelfmarks(shelf_lines, self._shelf_to_sys)
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
        self.setMinimumSize(780, 720)
        self._current_filters = current_filters.copy() if current_filters else {}
        self._count_worker = None
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

        # Date range
        date_group = QGroupBox(tr("Date Range"))
        date_layout = QHBoxLayout(date_group)
        self.spin_date_from = QSpinBox()
        self.spin_date_from.setRange(0, 2000)
        self.spin_date_from.setSpecialValueText(tr("Any"))
        self.spin_date_from.setValue(self._current_filters.get('date_from') or 0)
        self.spin_date_from.setPrefix(tr("From") + ": ")
        self.spin_date_from.valueChanged.connect(self._on_filter_changed)
        date_layout.addWidget(self.spin_date_from)
        self.spin_date_to = QSpinBox()
        self.spin_date_to.setRange(0, 2000)
        self.spin_date_to.setSpecialValueText(tr("Any"))
        self.spin_date_to.setValue(self._current_filters.get('date_to') or 0)
        self.spin_date_to.setPrefix(tr("To") + ": ")
        self.spin_date_to.valueChanged.connect(self._on_filter_changed)
        date_layout.addWidget(self.spin_date_to)
        self.chk_include_undated = QCheckBox(tr("Include undated"))
        self.chk_include_undated.setChecked(self._current_filters.get('include_undated', False))
        self.chk_include_undated.stateChanged.connect(self._on_filter_changed)
        date_layout.addWidget(self.chk_include_undated)
        right_layout.addWidget(date_group)

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

        # --- Measurement filters (Phase 54, DIM-02) ---
        meas_group = QGroupBox(tr("Measurements"))
        meas_layout = QGridLayout(meas_group)
        MEASUREMENT_MATERIALS = ['Paper', 'Vellum', 'Papyrus', 'Mix', 'Wood']

        def _make_meas_spin(row, label, max_val, decimals, suffix):
            meas_layout.addWidget(QLabel(label), row, 0)
            spin_min = QDoubleSpinBox()
            spin_min.setRange(0, max_val)
            spin_min.setDecimals(decimals)
            spin_min.setSpecialValueText("")
            spin_min.setValue(0)
            if suffix:
                spin_min.setSuffix(f" {suffix}")
            spin_min.setPrefix(tr("Min") + ": ")
            spin_min.editingFinished.connect(self._on_filter_changed)
            meas_layout.addWidget(spin_min, row, 1)
            spin_max = QDoubleSpinBox()
            spin_max.setRange(0, max_val)
            spin_max.setDecimals(decimals)
            spin_max.setSpecialValueText("")
            spin_max.setValue(0)
            if suffix:
                spin_max.setSuffix(f" {suffix}")
            spin_max.setPrefix(tr("Max") + ": ")
            spin_max.editingFinished.connect(self._on_filter_changed)
            meas_layout.addWidget(spin_max, row, 2)
            return spin_min, spin_max

        self.meas_width_min, self.meas_width_max = _make_meas_spin(0, tr("Width (cm)"), 100, 1, "cm")
        self.meas_height_min, self.meas_height_max = _make_meas_spin(1, tr("Height (cm)"), 100, 1, "cm")
        self.meas_lines_min, self.meas_lines_max = _make_meas_spin(2, tr("Lines"), 200, 0, "")
        self.meas_line_height_min, self.meas_line_height_max = _make_meas_spin(3, tr("Line Height (mm)"), 20, 1, "mm")
        self.meas_density_min, self.meas_density_max = _make_meas_spin(4, tr("Text Density"), 100, 1, "")

        # Material multi-select via checkboxes -- "Material (measured)" per review concern #7
        meas_layout.addWidget(QLabel(tr("Material (measured)")), 5, 0)
        self._meas_material_checks = {}
        mat_widget = QWidget()
        mat_layout_inner = QHBoxLayout(mat_widget)
        mat_layout_inner.setContentsMargins(0, 0, 0, 0)
        for mat in MEASUREMENT_MATERIALS:
            cb = QCheckBox(mat)
            cb.stateChanged.connect(self._on_filter_changed)
            mat_layout_inner.addWidget(cb)
            self._meas_material_checks[mat] = cb
        meas_layout.addWidget(mat_widget, 5, 1, 1, 2)

        # Restore measurement values from current_filters
        if self._current_filters.get('width_min'):
            self.meas_width_min.setValue(self._current_filters['width_min'])
        if self._current_filters.get('width_max'):
            self.meas_width_max.setValue(self._current_filters['width_max'])
        if self._current_filters.get('height_min'):
            self.meas_height_min.setValue(self._current_filters['height_min'])
        if self._current_filters.get('height_max'):
            self.meas_height_max.setValue(self._current_filters['height_max'])
        if self._current_filters.get('line_count_min'):
            self.meas_lines_min.setValue(self._current_filters['line_count_min'])
        if self._current_filters.get('line_count_max'):
            self.meas_lines_max.setValue(self._current_filters['line_count_max'])
        if self._current_filters.get('line_height_min'):
            self.meas_line_height_min.setValue(self._current_filters['line_height_min'])
        if self._current_filters.get('line_height_max'):
            self.meas_line_height_max.setValue(self._current_filters['line_height_max'])
        if self._current_filters.get('text_density_min'):
            self.meas_density_min.setValue(self._current_filters['text_density_min'])
        if self._current_filters.get('text_density_max'):
            self.meas_density_max.setValue(self._current_filters['text_density_max'])
        for mat, cb in self._meas_material_checks.items():
            cb.setChecked(mat in (self._current_filters.get('measurement_material') or []))

        right_layout.addWidget(meas_group)

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

        ok_btn = QPushButton(tr("OK"))
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton(tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        # Initial count update
        self._update_count()

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
        chip = QPushButton(f"{text}  \u00d7")
        chip.setFlat(True)
        chip.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        # Detect dark mode via palette brightness
        palette = chip.palette()
        is_dark = palette.color(palette.ColorRole.Window).lightness() < 128
        if is_dark:
            # Darken the background for dark mode, use light text
            from PyQt6.QtGui import QColor as _QC
            c = _QC(bg_color)
            dark_bg = c.darker(250).name()
            hover_bg = c.darker(180).name()
            chip.setStyleSheet(
                f"QPushButton {{ background: {dark_bg}; color: #e0e0e0; border: 1px solid #555; "
                "border-radius: 10px; padding: 2px 8px; font-size: 11px; } "
                f"QPushButton:hover {{ background: {hover_bg}; }}"
            )
        else:
            chip.setStyleSheet(
                f"QPushButton {{ background: {bg_color}; border: 1px solid #ccc; "
                "border-radius: 10px; padding: 2px 8px; font-size: 11px; } "
                "QPushButton:hover { background: #ddd; }"
            )
        chip.clicked.connect(on_click)
        return chip

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

        # Date chip
        date_from = self.spin_date_from.value()
        date_to = self.spin_date_to.value()
        if date_from > 0 or date_to > 0:
            f_str = str(date_from) if date_from > 0 else '...'
            t_str = str(date_to) if date_to > 0 else '...'
            self.chip_bar_layout.addWidget(self._make_chip(
                f"{f_str}-{t_str}", '#e8f5e9',
                lambda: (self.spin_date_from.setValue(0), self.spin_date_to.setValue(0))))

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

        # Text term chips
        for mode, prefix, terms in [('all', '+', self._text_all), ('any', '~', self._text_any), ('not', '-', self._text_not)]:
            for t in terms:
                color = '#e8f5e9' if mode == 'all' else '#e3f2fd' if mode == 'any' else '#ffebee'
                self.chip_bar_layout.addWidget(self._make_chip(
                    f"{prefix} {t}", color,
                    lambda checked=False, m=mode, term=t: self._remove_text_term(m, term)))

        # Measurement chips (Phase 54, teal #e0f2f1)
        def _fmt_range_chip(prefix, vmin, vmax, unit=''):
            u = f' {unit}' if unit else ''
            if vmin and vmax:
                return f"{prefix}: {vmin}-{vmax}{u}"
            elif vmin:
                return f"{prefix}: \u2265{vmin}{u}"
            elif vmax:
                return f"{prefix}: \u2264{vmax}{u}"
            return None
        _mf = self._get_measurement_filters()
        _meas_fields = [
            (tr('Width'), 'width_min', 'width_max', 'cm', [self.meas_width_min, self.meas_width_max]),
            (tr('Height'), 'height_min', 'height_max', 'cm', [self.meas_height_min, self.meas_height_max]),
            (tr('Lines'), 'line_count_min', 'line_count_max', '', [self.meas_lines_min, self.meas_lines_max]),
            (tr('Line Height'), 'line_height_min', 'line_height_max', 'mm', [self.meas_line_height_min, self.meas_line_height_max]),
            (tr('Text Density'), 'text_density_min', 'text_density_max', '', [self.meas_density_min, self.meas_density_max]),
        ]
        for _label, _kmin, _kmax, _unit, _widgets in _meas_fields:
            _chip_text = _fmt_range_chip(_label, _mf.get(_kmin), _mf.get(_kmax), _unit)
            if _chip_text:
                def _clear_meas(kmin=_kmin, kmax=_kmax, widgets=_widgets):
                    for w in widgets:
                        w.setValue(w.minimum())
                    self._on_filter_changed()
                self.chip_bar_layout.addWidget(self._make_chip(_chip_text, '#e0f2f1', _clear_meas))
        for _mat in _mf.get('measurement_material', []):
            def _clear_mat(m=_mat):
                cb = self._meas_material_checks.get(m)
                if cb:
                    cb.setChecked(False)
                self._on_filter_changed()
            self.chip_bar_layout.addWidget(self._make_chip(tr(_mat), '#e0f2f1', _clear_mat))

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
        date_from = self.spin_date_from.value()
        if date_from > 0:
            filters['date_from'] = date_from
        date_to = self.spin_date_to.value()
        if date_to > 0:
            filters['date_to'] = date_to
        if self.chk_include_undated.isChecked():
            filters['include_undated'] = True
        mat_mode = self.material_combo.currentData()
        if mat_mode == 'mss_only':
            filters['material_exclude'] = ['Printed']
        elif mat_mode == 'printed_only':
            filters['material_include'] = ['Printed']
        if self._text_all:
            filters['text_all'] = list(self._text_all)
        if self._text_any:
            filters['text_any'] = list(self._text_any)
        if self._text_not:
            filters['text_not'] = list(self._text_not)
        # Measurement filters (Phase 54)
        filters.update(self._get_measurement_filters())
        return filters

    def _get_measurement_filters(self) -> dict:
        """Extract measurement filter values from dialog spin boxes and checkboxes."""
        result = {}
        if self.meas_width_min.value() > 0:
            result['width_min'] = self.meas_width_min.value()
        if self.meas_width_max.value() > 0:
            result['width_max'] = self.meas_width_max.value()
        if self.meas_height_min.value() > 0:
            result['height_min'] = self.meas_height_min.value()
        if self.meas_height_max.value() > 0:
            result['height_max'] = self.meas_height_max.value()
        if self.meas_lines_min.value() > 0:
            result['line_count_min'] = int(self.meas_lines_min.value())
        if self.meas_lines_max.value() > 0:
            result['line_count_max'] = int(self.meas_lines_max.value())
        if self.meas_line_height_min.value() > 0:
            result['line_height_min'] = self.meas_line_height_min.value()
        if self.meas_line_height_max.value() > 0:
            result['line_height_max'] = self.meas_line_height_max.value()
        if self.meas_density_min.value() > 0:
            result['text_density_min'] = self.meas_density_min.value()
        if self.meas_density_max.value() > 0:
            result['text_density_max'] = self.meas_density_max.value()
        checked = [m for m, cb in self._meas_material_checks.items() if cb.isChecked()]
        if checked:
            result['measurement_material'] = checked
        return result

    def _get_display_name(self, key, data_map):
        """Get display name (without count) from data map."""
        text = data_map.get(key, key)
        return text.rsplit(' (', 1)[0].strip().lstrip().strip() if isinstance(text, str) else str(key)

    def _update_count(self):
        """Recompute manuscript count in background thread."""
        filters = self._get_current_filter_dict()
        # Check if any actual filter is set (not just include_mode)
        has_filter = any(k != 'include_mode' for k in filters)
        if not has_filter:
            self.count_label.setText(tr("All manuscripts (no filters)"))
            self._result_set = None
            return
        self.count_label.setText(tr("Counting..."))
        self._count_worker = FilterCountWorker(filters, self)
        self._count_worker.finished.connect(self._on_count_finished)
        self._count_worker.start()

    def _on_count_finished(self, result_set):
        """Handle count worker result."""
        self._result_set = result_set
        if result_set is None:
            self.count_label.setText(tr("All manuscripts (no filters)"))
        elif len(result_set) == 0:
            self.count_label.setText(tr("No manuscripts match"))
            self.count_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #e74c3c; padding: 4px;")
        else:
            count_str = f"{len(result_set):,}"
            self.count_label.setText(f"{count_str} {tr('manuscripts')}")
            self.count_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #2980b9; padding: 4px;")

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
        self.spin_date_from.setValue(0)
        self.spin_date_to.setValue(0)
        self.chk_include_undated.setChecked(False)
        self.material_combo.setCurrentIndex(0)
        self._text_all.clear()
        self._text_any.clear()
        self._text_not.clear()
        # Clear measurement filters (Phase 54)
        self.meas_width_min.setValue(0)
        self.meas_width_max.setValue(0)
        self.meas_height_min.setValue(0)
        self.meas_height_max.setValue(0)
        self.meas_lines_min.setValue(0)
        self.meas_lines_max.setValue(0)
        self.meas_line_height_min.setValue(0)
        self.meas_line_height_max.setValue(0)
        self.meas_density_min.setValue(0)
        self.meas_density_max.setValue(0)
        for cb in self._meas_material_checks.values():
            cb.setChecked(False)
        self._rebuild_dialog_chips()
        self._update_count()

    def get_filters(self) -> dict:
        """Return the current filter state dict."""
        return self._get_current_filter_dict()

    def get_restrict_sys_ids(self):
        """Return the computed restrict_sys_ids set (or None)."""
        return self._result_set


