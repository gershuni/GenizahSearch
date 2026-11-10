"""The batched composition-tree loader must survive the tree being cleared.

Owner UAT 2026-09-03, four identical tracebacks in one session:

    File "genizah_app.py", line 26756, in _process_tree_batch
      self._add_manuscript_node(self._batch_parent, self._batch_queue[i])
    File "genizah_app.py", line 26612, in _add_manuscript_node
      ms_node = QTreeWidgetItem(parent)
  RuntimeError: wrapped C/C++ object of type QTreeWidgetItem has been deleted

`_process_tree_batch` chains itself across event-loop turns with
`QTimer.singleShot(0, ...)`. `comp_tree.clear()` -- a new search, a re-sort, a
filter re-render -- deletes `_batch_parent`'s C++ object while the Python
wrapper survives, so the next scheduled batch built a child under a dead
parent. Four tracebacks means four chains were in flight at once.

Two consequences, both covered here: the crash itself, and the fact that a
chain dying mid-flight never reached its cleanup, leaving the tree with
painting disabled and `comp_tree_updating` stuck True (which suppresses
itemChanged, i.e. the check boxes stop working).
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem

from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui


_APP = QApplication.instance() or QApplication([])


class _Harness:
    """Real batch-loader methods over a real QTreeWidget."""

    _start_batched_tree_load = GenizahGUI._start_batched_tree_load
    _process_tree_batch = GenizahGUI._process_tree_batch
    _batch_parent_is_alive = GenizahGUI._batch_parent_is_alive
    _finish_batched_tree_load = GenizahGUI._finish_batched_tree_load

    def __init__(self):
        self.comp_tree = QTreeWidget()
        self.comp_tree.setColumnCount(2)
        self.comp_tree_updating = True
        self.added = []
        self.filters_applied = 0

    def _add_manuscript_node(self, parent, item):
        # The first thing the real _add_manuscript_node does, and the exact
        # line that raised: build a child under the batch parent.
        node = QTreeWidgetItem(parent)
        node.setText(0, str(item))
        self.added.append(item)
        return node

    def _update_comp_filter_indicators(self):
        pass

    def _apply_comp_tree_filters(self):
        self.filters_applied += 1


def _pump():
    """Let the queued singleShot(0) callbacks run."""
    for _ in range(50):
        _APP.processEvents()


def _root(h):
    parent = QTreeWidgetItem(h.comp_tree)
    parent.setText(0, "group")
    return parent


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------

def test_all_items_are_loaded_across_batches():
    h = _Harness()
    h._start_batched_tree_load(_root(h), list(range(120)), batch_size=25)
    _pump()
    assert h.added == list(range(120))
    assert h.comp_tree_updating is False
    assert h.comp_tree.updatesEnabled() is True
    assert h.filters_applied == 1


def test_empty_item_list_still_runs_cleanup():
    h = _Harness()
    h._start_batched_tree_load(_root(h), [], batch_size=25)
    _pump()
    assert h.added == []
    assert h.comp_tree_updating is False
    assert h.filters_applied == 1


# --------------------------------------------------------------------------
# The reported crash
# --------------------------------------------------------------------------

def test_clearing_the_tree_mid_chain_does_not_raise():
    h = _Harness()
    h._start_batched_tree_load(_root(h), list(range(500)), batch_size=25)
    assert h.added, "the first batch runs synchronously"
    h.comp_tree.clear()          # deletes _batch_parent's C++ object
    _pump()                      # the queued batches now fire
    assert len(h.added) < 500, "the chain must abandon, not keep building"


def test_clearing_the_tree_mid_chain_hands_the_tree_back():
    """A chain that dies must not leave painting off and checkboxes inert."""
    h = _Harness()
    h._start_batched_tree_load(_root(h), list(range(500)), batch_size=25)
    h.comp_tree.clear()
    _pump()
    assert h.comp_tree.updatesEnabled() is True
    assert h.comp_tree_updating is False


def test_batch_parent_is_alive_reports_deletion():
    h = _Harness()
    parent = _root(h)
    h._batch_parent = parent
    assert h._batch_parent_is_alive() is True
    h.comp_tree.clear()
    assert h._batch_parent_is_alive() is False


def test_batch_parent_is_alive_with_no_parent():
    h = _Harness()
    assert h._batch_parent_is_alive() is False
    h._batch_parent = None
    assert h._batch_parent_is_alive() is False


# --------------------------------------------------------------------------
# Overlapping renders
# --------------------------------------------------------------------------

def test_a_superseded_chain_stops_and_the_newer_one_completes():
    h = _Harness()
    h._start_batched_tree_load(_root(h), ["old"] * 300, batch_size=25)
    first_generation_added = len(h.added)

    # A second render starts before the first chain drained.
    h.comp_tree.clear()
    h._start_batched_tree_load(_root(h), ["new"] * 60, batch_size=25)
    _pump()

    assert h.added[first_generation_added:].count("new") == 60
    assert "old" not in h.added[first_generation_added:], (
        "the superseded chain must not keep appending"
    )
    assert h.comp_tree_updating is False


def test_a_stale_chain_does_not_run_cleanup_for_the_live_one():
    """Cleanup from a dead chain would re-enable painting mid-render."""
    h = _Harness()
    h._start_batched_tree_load(_root(h), ["a"] * 300, batch_size=25)
    stale_generation = h._batch_generation
    h._start_batched_tree_load(_root(h), ["b"] * 300, batch_size=25)
    h.comp_tree_updating = True
    before = h.filters_applied

    h._process_tree_batch(stale_generation)  # the old timer finally fires

    assert h.filters_applied == before, "stale chain must not finish the live one"
    assert h.comp_tree_updating is True, "stale chain must not clear the guard"


def test_generation_increments_per_render():
    h = _Harness()
    h._start_batched_tree_load(_root(h), [], batch_size=25)
    g1 = h._batch_generation
    h._start_batched_tree_load(_root(h), [], batch_size=25)
    assert h._batch_generation == g1 + 1
