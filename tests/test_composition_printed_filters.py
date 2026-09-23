"""Regression tests for Composition Search printed filtering."""

import os
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QTreeWidget, QTreeWidgetItem

import desktop.dialogs_filter as dialogs_filter
from genizah_app import GenizahGUI

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed non-GUI run
# segfaults after thousands of NiceGUI/asyncio tests share the process (2026-08-21).


_APP = QApplication.instance() or QApplication([])


class _CompositionFilterHarness:
    """GenizahGUI's composition filter methods bound to a stub with a real QTreeWidget.

    Since the shared rule (desktop/comp_view_filter.py) the tree is filtered from row
    DATA through the same display resolver the row builders use, so each manuscript
    here is stored the real way: a record with a ``pages`` list, and one child node
    per page holding that page dict. ``_comp_ms_display`` is stubbed to read the
    display fields the harness stored, standing in for the metadata services.
    """

    comp_col_library = 1
    comp_col_shelfmark = 2
    comp_col_title = 3
    comp_col_context = 5
    comp_col_ms_context = 6
    comp_col_printed = 7
    comp_col_sysid = 4

    def __init__(self):
        self.comp_tree = QTreeWidget()
        self.comp_tree.setColumnCount(9)
        self.comp_filters = {}
        self._comp_printed_sys_ids = {"printed"}
        self._comp_printed_filter_state = "all"
        self._local_filter_state_composition = "all"
        self._local_filter_inactive_chip_visible = False
        self._local_file_optouts = set()
        self.comp_raw_items = []
        self._comp_view_groups = []
        self._comp_results_from_parallels = False
        self._comp_domain_exclusions = set()
        self._comp_result_domain_map = {}
        self._uid = 0

        for name in ("_apply_comp_tree_filters", "_comp_filter_state",
                     "_comp_filter_population", "_comp_ms_sys_id",
                     "_text_matches_filter", "_comp_shelf_cells",
                     "_comp_shelf_cells_uncached"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))
        self._comp_item_is_local = GenizahGUI._comp_item_is_local
        self._comp_preview_source_text = GenizahGUI._comp_preview_source_text

    def _comp_ms_display(self, ms_item):
        return {
            "sys_id": ms_item.get("sys_id", ""),
            "shelf": ms_item.get("_shelf", ""),
            "library_code": ms_item.get("_library", ""),
            "title": ms_item.get("_title", ""),
        }

    def _get_meta_for_header(self, raw_header):
        return ("", raw_header, "", "")

    def _apply_local_filter(self, results, state):
        return results

    def _apply_local_optout_filter(self, results):
        return results

    def _show_local_filter_chip(self, surface, inactive):
        return None

    def add_manuscript(self, sys_id, *, library="", shelfmark="", title="", pages=("source",)):
        """A manuscript row and one child row per page; returns (ms_node, [page_nodes])."""
        page_dicts = []
        for ctx in pages:
            self._uid += 1
            page_dicts.append({"uid": f"u{self._uid}", "raw_header": f"h{self._uid}",
                               "source_ctx": ctx, "text": "manuscript"})
        record = {"type": "manuscript", "sys_id": sys_id, "pages": page_dicts,
                  "_library": library, "_shelf": shelfmark, "_title": title}
        ms_node = QTreeWidgetItem(self.comp_tree)
        ms_node.setText(self.comp_col_library, library)
        ms_node.setText(self.comp_col_shelfmark, shelfmark)
        ms_node.setText(self.comp_col_title, title)
        ms_node.setData(0, Qt.ItemDataRole.UserRole, record)
        page_nodes = []
        if len(page_dicts) > 1:
            for pd in page_dicts:
                node = QTreeWidgetItem(ms_node)
                node.setData(0, Qt.ItemDataRole.UserRole, pd)
                page_nodes.append(node)
        return ms_node, page_nodes


def test_composition_printed_filter_uses_record_data_not_preview_data():
    harness = _CompositionFilterHarness()
    printed, _ = harness.add_manuscript("printed")
    manuscript, _ = harness.add_manuscript("manuscript")

    harness._comp_printed_filter_state = "hide_printed"
    harness._apply_comp_tree_filters()
    assert printed.isHidden()
    assert not manuscript.isHidden()

    harness._comp_printed_filter_state = "only_printed"
    harness._apply_comp_tree_filters()
    assert not printed.isHidden()
    assert manuscript.isHidden()


def test_composition_printed_filter_applies_to_entire_manuscript_subtree():
    harness = _CompositionFilterHarness()
    printed, pages = harness.add_manuscript("printed", pages=("a", "b"))

    harness._comp_printed_filter_state = "hide_printed"
    harness._apply_comp_tree_filters()
    assert printed.isHidden()
    assert all(p.isHidden() for p in pages)

    harness._comp_printed_filter_state = "only_printed"
    harness._apply_comp_tree_filters()
    assert not printed.isHidden()
    assert not any(p.isHidden() for p in pages)


def test_composition_library_filter_uses_displayed_library_column():
    harness = _CompositionFilterHarness()
    local, _ = harness.add_manuscript(
        "local-id", library="Local Folder", shelfmark="local-file.txt")
    harness.comp_filters = {
        harness.comp_col_library: {"text": "Local Folder", "exclude": False}
    }

    harness._apply_comp_tree_filters()
    assert not local.isHidden()

    harness.comp_filters = {
        harness.comp_col_library: {"text": "local-file.txt", "exclude": False}
    }
    harness._apply_comp_tree_filters()
    assert local.isHidden()


def test_composition_title_filter_is_inherited_by_matching_page_context():
    """Manuscript-level (title) keeps the manuscript; page-level (context) keeps only
    the matching page -- the owner's split-by-filter-type contract."""
    harness = _CompositionFilterHarness()
    manuscript, (first_page, matching_page) = harness.add_manuscript(
        "manuscript", title="Matching composition",
        pages=("first page", "target context"))
    harness.comp_filters = {
        harness.comp_col_title: {"text": "Matching composition", "exclude": False},
        harness.comp_col_context: {"text": "target context", "exclude": False},
    }

    harness._apply_comp_tree_filters()

    assert not manuscript.isHidden()
    assert first_page.isHidden()
    assert not matching_page.isHidden()


class _Signal:
    def __init__(self):
        self.callback = None

    def connect(self, callback):
        self.callback = callback

    def emit(self, value):
        self.callback(value)


class _FilterWorker:
    instances = []

    def __init__(self, filters, parent):
        self.filters = filters
        self.parent = parent
        self.finished = _Signal()
        self.started = False
        self.__class__.instances.append(self)

    def start(self):
        self.started = True


class _FilterDialogHarness:
    def __init__(self):
        self._count_generation = 0
        self._count_worker = None
        self._result_set = None
        self.count_label = QLabel()
        self.ok_btn = QPushButton()
        self.filters = {"material_exclude": ["Printed"]}
        self._on_count_finished = MethodType(
            dialogs_filter.PreSearchFilterDialog._on_count_finished, self
        )

    def _get_current_filter_dict(self):
        return dict(self.filters)


def test_focus_search_waits_for_latest_filter_result(monkeypatch):
    monkeypatch.setattr(dialogs_filter, "FilterCountWorker", _FilterWorker)
    _FilterWorker.instances.clear()
    dialog = _FilterDialogHarness()

    dialogs_filter.PreSearchFilterDialog._update_count(dialog)
    first = _FilterWorker.instances[-1]
    assert first.started
    assert not dialog.ok_btn.isEnabled()

    dialog.filters = {"material_include": ["Printed"]}
    dialogs_filter.PreSearchFilterDialog._update_count(dialog)
    second = _FilterWorker.instances[-1]

    second.finished.emit({"printed"})
    assert dialog.ok_btn.isEnabled()
    assert dialog._result_set == {"printed"}

    first.finished.emit({"stale-unprinted"})
    assert dialog._result_set == {"printed"}
