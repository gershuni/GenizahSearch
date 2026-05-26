# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-A — _UnifiedFileTreeWidget async tree population tests.

Pins D-04 / D-05 / D-13 / D-20 / D-22 of the Wave 2 tree refactor:

  test_tree_starts_collapsed_after_populate
      D-04: After async populate, top-level item is NOT expanded.

  test_cancel_clears_tree
      D-05: Selecting a different folder mid-populate clears the tree (no
      partial-results state machine).

  test_stale_batch_dropped_by_token
      D-13 UI side: manually invoking _on_tree_batch with a stale token
      leaves _displayed_paths unchanged.

  test_optout_tristate_preserved_across_async
      D-20: pre-existing _local_file_optouts entries cause the matching
      leaves to be Unchecked and the parent folder PartiallyChecked.

  test_populate_returns_within_100ms
      D-22: 10K-file fixture; time.perf_counter() bracket; QTimer marker;
      QApplication.processEvents(); elapsed < 100ms.

  test_cancel_button_click_stops_tree_worker
      Codex Critique #3 HIGH: _btn_cancel click while tree-worker is
      running stops the walk cleanly (no scan Discard/Keep modal).

  test_all_six_supported_extensions_appear_as_tree_leaves
      Codex Critique #3 R97.3-N coverage: each supported extension
      (case-insensitive) reaches the UI as a leaf.

All 7 tests are RED against the synchronous populate_for_folder + expandAll()
in the pre-Phase-97.3 code; Task 4 GREEN flips them by wiring an async
FolderWalkWorker into _UnifiedFileTreeWidget.populate_for_folder.
"""
from __future__ import annotations

import os
import sys
import time

import pytest

# Path bootstrap so `desktop.my_library_tab` and `shared.*` are importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# PyQt6-aware fixture pattern (mirror tests/test_folder_walk_worker.py).
try:
    from PyQt6.QtCore import Qt, QTimer
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _MockApp:
    """Minimal stand-in for the app/parent that _UnifiedFileTreeWidget reads.

    Exposes _local_file_optouts (the set the tree consults for checkbox
    state) and my_library_tab=None so the populate_for_folder prior_status
    cache resolution path falls through gracefully.
    """

    def __init__(self):
        self._local_file_optouts: set = set()
        self.my_library_tab = None


def _make_tree(parent_app=None):
    from desktop.my_library_tab import _UnifiedFileTreeWidget
    app = parent_app if parent_app is not None else _MockApp()
    tree = _UnifiedFileTreeWidget(None, app)
    return tree, app


def _wait_for_tree_worker(tree, timeout_ms=10_000):
    """Wait for tree._tree_worker.finished_signal via QSignalSpy."""
    from PyQt6.QtTest import QSignalSpy
    worker = getattr(tree, "_tree_worker", None)
    if worker is None:
        return False
    spy = QSignalSpy(worker.finished_signal)
    # Process events while we wait so signals can drain
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline and len(spy) == 0:
        QApplication.processEvents()
        time.sleep(0.01)
    return len(spy) > 0


def _walk_leaves(tree):
    """Return list of (leaf_item, basename, canonical) for all leaves."""
    result = []

    def _rec(node):
        if node.childCount() == 0:
            data = node.data(0, Qt.ItemDataRole.UserRole)
            result.append((node, node.text(0), data))
            return
        for i in range(node.childCount()):
            _rec(node.child(i))

    inv = tree.invisibleRootItem()
    for i in range(inv.childCount()):
        _rec(inv.child(i))
    return result


# ---------------------------------------------------------------------------
# D-04 — tree starts collapsed
# ---------------------------------------------------------------------------

def test_tree_starts_collapsed_after_populate(tmp_path):
    """D-04: After async populate completes, top-level item is NOT expanded."""
    folder = tmp_path / "small"
    folder.mkdir()
    (folder / "a.pdf").write_text("x")
    (folder / "b.txt").write_text("y")

    tree, app = _make_tree()
    tree.populate_for_folder(str(folder))
    assert _wait_for_tree_worker(tree), "tree worker should finish"
    # Drain queued slot calls
    QApplication.processEvents()

    top = tree.topLevelItem(0)
    assert top is not None, "Top-level item should exist after populate"
    assert top.isExpanded() is False, (
        "D-04: tree must NOT call expandAll(); top-level item should start collapsed."
    )


# ---------------------------------------------------------------------------
# D-05 — cancel-by-folder-switch clears tree
# ---------------------------------------------------------------------------

def test_cancel_clears_tree(tmp_path):
    """D-05: Selecting a different folder mid-populate clears the tree.

    Strategy: build a small first folder, populate, then switch to a tiny
    second folder; after the second worker finishes, assert the tree shows
    ONLY content from the second folder.
    """
    folder_a = tmp_path / "folder_a"
    folder_a.mkdir()
    for i in range(20):
        (folder_a / f"a_{i:02d}.pdf").write_text("a")
    folder_b = tmp_path / "folder_b"
    folder_b.mkdir()
    (folder_b / "only_b.txt").write_text("b")

    tree, app = _make_tree()
    tree.populate_for_folder(str(folder_a))
    # Immediately switch — this should cancel + clear.
    tree.populate_for_folder(str(folder_b))
    assert _wait_for_tree_worker(tree), "second worker should finish"
    QApplication.processEvents()

    leaves = _walk_leaves(tree)
    basenames = [b for _, b, _ in leaves]
    assert "only_b.txt" in basenames, f"folder_b's file should appear; got {basenames}"
    for b in basenames:
        assert not b.startswith("a_"), (
            f"D-05: folder_a's files must NOT remain in the tree; got {basenames}"
        )


# ---------------------------------------------------------------------------
# D-13 UI — stale-token batch dropped
# ---------------------------------------------------------------------------

def test_stale_batch_dropped_by_token(tmp_path):
    """D-13 UI: _on_tree_batch with stale token does NOT mutate _displayed_paths."""
    folder = tmp_path / "empty"
    folder.mkdir()

    tree, app = _make_tree()
    tree.populate_for_folder(str(folder))
    assert _wait_for_tree_worker(tree)
    QApplication.processEvents()

    before = set(tree._displayed_paths)
    current_token = tree._tree_token
    stale_token = current_token - 1
    # Simulate a queued stale batch arrival
    tree._on_tree_batch([("/tmp/ghost.pdf", "/tmp/ghost.pdf", 0, 0)], stale_token)

    assert tree._displayed_paths == before, (
        "D-13: stale-token batches must be dropped — _displayed_paths must not change"
    )
    assert "/tmp/ghost.pdf" not in tree._displayed_paths
    assert "/tmp/ghost.pdf" not in tree._leaf_by_path


# ---------------------------------------------------------------------------
# D-20 — opt-out tri-state preserved across async populate
# ---------------------------------------------------------------------------

def test_optout_tristate_preserved_across_async(tmp_path):
    """D-20: pre-populated _local_file_optouts → leaves Unchecked; parent PartiallyChecked."""
    folder = tmp_path / "mixed"
    folder.mkdir()
    a = folder / "opted_out.pdf"
    b = folder / "also_opted_out.pdf"
    c = folder / "kept_in.pdf"
    for p in (a, b, c):
        p.write_text("x")

    from shared.local_sys_id import _canonical_filepath
    canon_a = _canonical_filepath(str(a))
    canon_b = _canonical_filepath(str(b))
    canon_c = _canonical_filepath(str(c))

    app = _MockApp()
    app._local_file_optouts = {canon_a, canon_b}

    tree, _ = _make_tree(parent_app=app)
    tree.populate_for_folder(str(folder))
    assert _wait_for_tree_worker(tree)
    QApplication.processEvents()

    leaves = {canon: item for item, _, canon in _walk_leaves(tree)}
    assert canon_a in leaves, f"canon_a should be a leaf; tree paths={list(leaves)}"
    assert canon_b in leaves
    assert canon_c in leaves
    assert leaves[canon_a].checkState(0) == Qt.CheckState.Unchecked
    assert leaves[canon_b].checkState(0) == Qt.CheckState.Unchecked
    assert leaves[canon_c].checkState(0) == Qt.CheckState.Checked

    # Parent folder node should be in PartiallyChecked state (ItemIsAutoTristate).
    top = tree.topLevelItem(0)
    assert top.checkState(0) == Qt.CheckState.PartiallyChecked, (
        f"D-20: parent folder should be PartiallyChecked when some leaves are "
        f"opted out; got {top.checkState(0)}"
    )


# ---------------------------------------------------------------------------
# D-22 — 100ms responsiveness
# ---------------------------------------------------------------------------

def test_populate_returns_within_100ms(tmp_path):
    """D-22: populate_for_folder returns < 100ms even for ~10K files.

    Codex Critique #2 measurement contract:
      - time.perf_counter() brackets the call
      - QTimer.singleShot(0, marker) detects event-loop re-entry
      - QApplication.processEvents() drains pending events
    """
    big = tmp_path / "mega"
    big.mkdir()
    # 50 subdirs × 200 files = 10000 supported files (all .txt).
    for d in range(50):
        sub = big / f"sub_{d:02d}"
        sub.mkdir()
        for f in range(200):
            (sub / f"file_{f:04d}.txt").write_text("x")

    marker_fired = [False]

    def _mark():
        marker_fired[0] = True

    tree, app = _make_tree()
    QTimer.singleShot(0, _mark)

    start = time.perf_counter()
    tree.populate_for_folder(str(big))
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    QApplication.processEvents()

    assert elapsed_ms < 100.0, (
        f"D-22: populate_for_folder took {elapsed_ms:.1f}ms; target <100ms. "
        "The synchronous _populate_node walk on the UI thread is the freeze "
        "root cause (R97.3-A)."
    )
    assert marker_fired[0], (
        "D-22: QTimer.singleShot(0) did NOT fire — event loop was blocked "
        "during populate_for_folder."
    )

    # Drain so the worker doesn't leak between tests.
    _wait_for_tree_worker(tree, timeout_ms=15_000)
    QApplication.processEvents()


# ---------------------------------------------------------------------------
# Codex Critique #3 HIGH — cancel button stops tree-worker
# ---------------------------------------------------------------------------

def test_cancel_button_click_stops_tree_worker(tmp_path, monkeypatch):
    """Codex Critique #3 HIGH: Cancel button must stop tree-worker without
    showing the scan Discard/Keep/Resume modal.

    Build a real MyLibraryTab. Start a tree-worker on a 5K-file folder.
    While the worker is running, click _btn_cancel programmatically. Expect:
      - tree-worker exits cleanly
      - tree._tree_worker eventually becomes None (released)
      - tree._displayed_paths is empty (D-05: cancel clears tree)
      - no Discard/Keep/Resume modal shown
    """
    from genizah_core import Config
    idx_dir = str(tmp_path / "local_index")
    lab_dir = str(tmp_path / "local_lab")
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", idx_dir, raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", lab_dir, raising=False)

    from desktop.my_library_tab import MyLibraryTab
    tab = MyLibraryTab(parent=None)
    try:
        # Build a 5K-file fixture
        big = tmp_path / "mega"
        big.mkdir()
        for d in range(25):
            sub = big / f"sub_{d:02d}"
            sub.mkdir()
            for f in range(200):
                (sub / f"file_{f:04d}.txt").write_text("x")

        # Ensure no scan worker is masquerading — _on_cancel_clicked must
        # take the tree-only branch and NOT show a QMessageBox modal.
        msgbox_calls = []
        from PyQt6 import QtWidgets

        def _fail_msgbox(*a, **kw):
            msgbox_calls.append((a, kw))
            return QtWidgets.QMessageBox.StandardButton.Cancel

        monkeypatch.setattr(QtWidgets.QMessageBox, "exec", _fail_msgbox)

        tab._unified_tree.populate_for_folder(str(big))
        # Worker should be running.
        worker = tab._unified_tree._tree_worker
        assert worker is not None, "tree-worker must be set"
        assert worker.isRunning(), "tree-worker must be running"

        # Click cancel programmatically — this should route through
        # _on_cancel_clicked's tree-only branch.
        tab._btn_cancel.clicked.emit()

        # Wait for the worker to actually exit.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            QApplication.processEvents()
            still_running = worker.isRunning()
            if not still_running:
                break
            time.sleep(0.01)

        assert not worker.isRunning(), (
            "Cancel button must stop the tree-worker"
        )
        # Drain finished_signal slots
        worker.wait(2000)
        QApplication.processEvents()

        # D-05: tree cleared on cancel
        assert tab._unified_tree._displayed_paths == set(), (
            "D-05: cancel must clear _displayed_paths"
        )
        # No Discard/Keep modal should have been shown (the patched .exec was
        # never called because the tree-only branch returns before mb.exec()).
        assert msgbox_calls == [], (
            f"Cancel must NOT show the scan Discard/Keep/Resume modal when "
            f"only tree-population is running; got msgbox calls={msgbox_calls!r}"
        )
    finally:
        try:
            if getattr(tab, "_indexer", None) is not None:
                try:
                    tab._indexer._conn.close()
                except Exception:
                    pass
            tab.deleteLater()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Codex Critique #3 R97.3-N coverage — 6 extensions reach UI as leaves
# ---------------------------------------------------------------------------

def test_all_six_supported_extensions_appear_as_tree_leaves(tmp_path):
    """Codex Critique #3 R97.3-N: each supported extension (case-insensitive)
    must appear as a leaf in the opt-out tree, including mixed-case .PDF/.Pdf.
    """
    folder = tmp_path / "all_exts"
    folder.mkdir()
    files = [
        "a.pdf", "b.docx", "c.txt", "d.html", "e.xlsx", "f.csv",
        "g.PDF", "h.Pdf",
    ]
    for f in files:
        (folder / f).write_text("x")

    tree, app = _make_tree()
    tree.populate_for_folder(str(folder))
    assert _wait_for_tree_worker(tree)
    QApplication.processEvents()

    leaves = _walk_leaves(tree)
    basenames = sorted(b for _, b, _ in leaves)
    expected = sorted(files)
    assert basenames == expected, (
        f"R97.3-N: all 8 supported-extension files should appear as leaves; "
        f"got {basenames}, expected {expected}"
    )
