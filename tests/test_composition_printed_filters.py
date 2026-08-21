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
        self._comp_results_from_parallels = False

        self._apply_comp_tree_filters = MethodType(
            GenizahGUI._apply_comp_tree_filters, self
        )
        self._comp_data_matches_filters = MethodType(
            GenizahGUI._comp_data_matches_filters, self
        )
        self._text_matches_filter = MethodType(GenizahGUI._text_matches_filter, self)

    def _apply_local_filter(self, results, state):
        return results

    def _apply_local_optout_filter(self, results):
        return results

    def _show_local_filter_chip(self, surface, inactive):
        return None

    def add_result(
        self,
        sys_id=None,
        *,
        parent=None,
        library="",
        shelfmark="",
        title="",
        source_ctx="source",
        ms_ctx="manuscript",
    ):
        node = QTreeWidgetItem(parent if parent is not None else self.comp_tree)
        node.setText(self.comp_col_library, library)
        node.setText(self.comp_col_shelfmark, shelfmark)
        node.setText(self.comp_col_title, title)
        record = {"sys_id": sys_id} if sys_id else {}
        node.setData(0, Qt.ItemDataRole.UserRole, record)
        node.setData(
            0,
            Qt.ItemDataRole.UserRole + 1,
            {"source_ctx": source_ctx, "ms_ctx": ms_ctx, "anchor": None},
        )
        return node


def test_composition_printed_filter_uses_record_data_not_preview_data():
    harness = _CompositionFilterHarness()
    printed = harness.add_result("printed")
    manuscript = harness.add_result("manuscript")

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
    printed = harness.add_result("printed")
    page = harness.add_result(parent=printed, shelfmark="Image 1")

    harness._comp_printed_filter_state = "hide_printed"
    harness._apply_comp_tree_filters()
    assert printed.isHidden()
    assert page.isHidden()

    harness._comp_printed_filter_state = "only_printed"
    harness._apply_comp_tree_filters()
    assert not printed.isHidden()
    assert not page.isHidden()


def test_composition_library_filter_uses_displayed_library_column():
    harness = _CompositionFilterHarness()
    local = harness.add_result(
        "local-id",
        library="Local Folder",
        shelfmark="local-file.txt",
    )
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
    harness = _CompositionFilterHarness()
    manuscript = harness.add_result(
        "manuscript", title="Matching composition", source_ctx="first page"
    )
    first_page = harness.add_result(parent=manuscript, source_ctx="first page")
    matching_page = harness.add_result(parent=manuscript, source_ctx="target context")
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
