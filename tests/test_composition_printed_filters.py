"""Regression tests for Composition Search printed filtering."""

import os
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QTreeWidget, QTreeWidgetItem

import desktop.dialogs_filter as dialogs_filter
from genizah_app import GenizahGUI


_APP = QApplication.instance() or QApplication([])


class _CompositionFilterHarness:
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

    def add_result(self, sys_id):
        node = QTreeWidgetItem(self.comp_tree)
        node.setData(0, Qt.ItemDataRole.UserRole, {"sys_id": sys_id})
        node.setData(
            0,
            Qt.ItemDataRole.UserRole + 1,
            {"source_ctx": "source", "ms_ctx": "manuscript", "anchor": None},
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
