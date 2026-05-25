# -*- coding: utf-8 -*-
"""Phase 97 U-03 — FolderWalkWorker: batched throttled pyqtSignal(list).

Tests:
  T-E-4  test_batched_signal — FolderWalkWorker emits batches of <= BATCH_SIZE
         files; 250-file folder yields at least 3 batches.
  T-E-5  test_no_widget_mutation — AST scan of FolderWalkWorker.run asserts
         zero QWidget mutation method calls (addItem/setText/setHtml/etc.).
"""
import ast
import os
import pathlib


# ---------------------------------------------------------------------------
# T-E-4: batched signals
# ---------------------------------------------------------------------------

def test_batched_signal(tmp_path):
    """FolderWalkWorker emits batches; 250 files → >= 3 batches; each <= BATCH_SIZE."""
    try:
        from PyQt6.QtWidgets import QApplication
        import sys
        app = QApplication.instance() or QApplication(sys.argv[:1])
    except ImportError:
        import pytest
        pytest.skip("PyQt6 not available in this environment")

    from desktop.my_library_tab import FolderWalkWorker

    # Create 250 tiny files
    folder = str(tmp_path / "files_250")
    os.makedirs(folder)
    for i in range(250):
        p = os.path.join(folder, f"file_{i:04d}.txt")
        with open(p, "w") as f:
            f.write(f"content {i}")

    received_batches = []
    finished_counts = []

    worker = FolderWalkWorker([folder])
    # Use DirectConnection so signals are delivered immediately in the worker thread
    # rather than being queued to the main thread's event loop (which isn't running
    # in a test context).
    from PyQt6.QtCore import Qt
    worker.batch_emitted.connect(
        lambda batch: received_batches.append(batch),
        Qt.ConnectionType.DirectConnection,
    )
    worker.finished_signal.connect(
        lambda fc, fb: finished_counts.append((fc, fb)),
        Qt.ConnectionType.DirectConnection,
    )
    worker.start()
    worker.wait(10_000)  # 10s timeout

    assert len(received_batches) >= 3, (
        f"Expected >= 3 batches for 250 files (BATCH_SIZE={worker.BATCH_SIZE}), "
        f"got {len(received_batches)}"
    )
    for idx, batch in enumerate(received_batches):
        assert len(batch) <= worker.BATCH_SIZE, (
            f"Batch {idx} has {len(batch)} items but BATCH_SIZE={worker.BATCH_SIZE}"
        )

    # Total files across all batches should equal 250
    total = sum(len(b) for b in received_batches)
    assert total == 250, f"Expected 250 total files in batches, got {total}"

    # finished_signal should have been emitted with correct count
    assert finished_counts, "finished_signal should have been emitted"
    fc, _ = finished_counts[0]
    assert fc == 250, f"finished_signal total_files expected 250, got {fc}"


# ---------------------------------------------------------------------------
# T-E-5: no QWidget mutation in worker thread
# ---------------------------------------------------------------------------

_MUTATION_METHODS = frozenset([
    "addItem", "addItems", "addWidget", "insertItem", "insertWidget",
    "setText", "setHtml", "setPlainText", "clear", "setCurrentIndex",
    "setEnabled", "setVisible", "show", "hide", "update", "repaint",
    "setValue", "setRange", "setMaximum", "setMinimum",
    "setStyleSheet", "setToolTip", "setWindowTitle",
])


def _find_folder_walk_worker_run_ast():
    """Parse desktop/my_library_tab.py and return the FolderWalkWorker.run FunctionDef node."""
    src_path = pathlib.Path(__file__).resolve().parent.parent / "desktop" / "my_library_tab.py"
    src = src_path.read_text(encoding="utf-8")
    tree = ast.parse(src)

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "FolderWalkWorker":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "run":
                    return item
    return None


def test_no_widget_mutation():
    """AST scan: FolderWalkWorker.run must NOT call QWidget mutation methods.

    Calls to addItem, setText, setHtml, etc. from a QThread worker cause
    cross-thread QWidget access — undefined behavior (T-97E-02).
    """
    run_node = _find_folder_walk_worker_run_ast()
    assert run_node is not None, (
        "FolderWalkWorker.run method not found in desktop/my_library_tab.py. "
        "Has FolderWalkWorker been added? (Task 3 is the GREEN implementation)"
    )

    violations = []
    for node in ast.walk(run_node):
        if isinstance(node, ast.Attribute) and node.attr in _MUTATION_METHODS:
            violations.append(node.attr)

    assert len(violations) == 0, (
        f"FolderWalkWorker.run contains QWidget mutation calls: {violations}. "
        "Worker thread must NOT mutate QWidgets — emit pyqtSignal(list) instead "
        "and let the UI-thread slot handle mutations (T-97E-02)."
    )
