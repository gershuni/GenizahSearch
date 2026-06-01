# -*- coding: utf-8 -*-
"""v7.16 BUG-4: folder-uncheck must cascade to leaf opt-out state.

ItemIsAutoTristate propagates child->parent state but NOT reliably parent->child,
so unchecking a FOLDER never reached its leaves and the opt-out set (built from
leaf states) stayed empty — folder unchecks did nothing to search results. These
tests pin the explicit parent->child cascade in
_UnifiedFileTreeWidget._on_item_changed + _set_descendant_leaves_check_state.

This file is collect-ignored on CI (test_my_library_tab*.py, D-F15) because it
needs a real QApplication; run locally with `pytest tests/test_my_library_tab_optout_cascade.py`.
"""
import pytest

pytest.importorskip("PyQt6.QtWidgets")
from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QApplication, QTreeWidgetItem  # noqa: E402


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


class _FakeApp:
    def __init__(self):
        self._local_file_optouts = set()
        self.reapplied = 0

    def _save_session(self):
        pass

    def _reapply_filters_for_optout_change(self):
        self.reapplied += 1


def _build_tree(qapp):
    from desktop.my_library_tab import _UnifiedFileTreeWidget

    app = _FakeApp()
    tree = _UnifiedFileTreeWidget(None, app)
    tree._suppress_signals = True
    folder = QTreeWidgetItem(tree, ["folder", "", ""])
    folder.setFlags(
        folder.flags()
        | Qt.ItemFlag.ItemIsUserCheckable
        | Qt.ItemFlag.ItemIsAutoTristate
    )
    folder.setData(0, Qt.ItemDataRole.UserRole, "c:/folder")
    leaf_paths = []
    for name in ["a.pdf", "b.pdf", "c.pdf"]:
        leaf = QTreeWidgetItem(folder, [name, "1", "OK"])
        leaf.setFlags(leaf.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        path = "c:/folder/" + name
        leaf.setData(0, Qt.ItemDataRole.UserRole, path)
        leaf.setCheckState(0, Qt.CheckState.Checked)
        tree._displayed_paths.add(path)
        leaf_paths.append(path)
    tree._suppress_signals = False
    return tree, folder, leaf_paths, app


def _leaf_states(folder):
    return [folder.child(i).checkState(0) for i in range(folder.childCount())]


def test_unchecking_folder_cascades_to_leaves(qapp):
    tree, folder, leaf_paths, app = _build_tree(qapp)

    # Simulate the user unchecking the FOLDER checkbox.
    folder.setCheckState(0, Qt.CheckState.Unchecked)  # fires itemChanged -> cascade

    assert _leaf_states(folder) == [Qt.CheckState.Unchecked] * 3, (
        "folder uncheck did not cascade to leaves"
    )

    # The debounce timer would fire _commit_changes in the event loop; call it
    # directly to verify the opt-out set is populated from the cascaded leaves.
    tree._commit_changes()
    assert app._local_file_optouts == set(leaf_paths)
    assert app.reapplied >= 1, "opt-out change must trigger a result re-filter"


def test_rechecking_folder_clears_optouts(qapp):
    tree, folder, leaf_paths, app = _build_tree(qapp)

    folder.setCheckState(0, Qt.CheckState.Unchecked)
    tree._commit_changes()
    assert app._local_file_optouts == set(leaf_paths)

    # Re-include: check the folder again -> leaves checked -> optouts cleared.
    folder.setCheckState(0, Qt.CheckState.Checked)
    assert _leaf_states(folder) == [Qt.CheckState.Checked] * 3
    tree._commit_changes()
    assert app._local_file_optouts == set()


def test_set_descendant_leaves_helper_only_touches_leaves(qapp):
    tree, folder, leaf_paths, app = _build_tree(qapp)
    tree._suppress_signals = True
    try:
        tree._set_descendant_leaves_check_state(folder, Qt.CheckState.Unchecked)
    finally:
        tree._suppress_signals = False
    assert _leaf_states(folder) == [Qt.CheckState.Unchecked] * 3
