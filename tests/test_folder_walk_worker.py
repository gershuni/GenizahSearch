# -*- coding: utf-8 -*-
"""Phase 97 U-03 + Phase 97.3 R97.3-A — FolderWalkWorker tests.

Original Phase 97 tests (preserved):
  T-E-4  test_batched_signal — FolderWalkWorker emits batches of <= BATCH_SIZE
         files; 250-file folder yields at least 3 batches.
  T-E-5  test_no_widget_mutation — AST scan of FolderWalkWorker.run asserts
         zero QWidget mutation method calls (addItem/setText/setHtml/etc.).

Phase 97.3 R97.3-A additions (D-13/D-14/D-15/D-17):
  test_stale_batch_token_dropped              — D-13a token guard contract
  test_stale_finished_signal_ignored          — D-13b/D-17 finished_signal token
  test_unsupported_files_not_canonicalized    — D-14a no Path.resolve tax on .ds_store
  test_mixed_case_extensions_normalized       — D-14b .PDF / .Pdf accepted
  test_junction_not_followed                  — D-15 followlinks=False; AST fallback
  test_error_signal_carries_token             — D-17 error_signal carries token

After Task 2 GREEN, batch_emitted = pyqtSignal(list, int) carries a list of
4-tuples (filepath, canonical, mtime_ns, size) AS THE FIRST ARG and the
generation token AS THE SECOND ARG (the token is NOT embedded inside the
tuple itself — Codex Critique #3 wording fix).
"""
import ast
import os
import pathlib
import subprocess
import sys

import pytest


# ---------------------------------------------------------------------------
# T-E-4: batched signals (Phase 97 U-03 original)
# ---------------------------------------------------------------------------

def test_batched_signal(tmp_path):
    """FolderWalkWorker emits batches; 250 files → >= 3 batches; each <= BATCH_SIZE."""
    try:
        from PyQt6.QtWidgets import QApplication
        app = QApplication.instance() or QApplication(sys.argv[:1])
    except ImportError:
        pytest.skip("PyQt6 not available in this environment")

    from desktop.my_library_tab import FolderWalkWorker

    # Create 250 tiny files (.txt — passes the new _SUPPORTED_EXTENSIONS filter)
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
    # Phase 97.3: batch_emitted now carries (list, int) — token added.
    worker.batch_emitted.connect(
        lambda batch, tok: received_batches.append(batch),
        Qt.ConnectionType.DirectConnection,
    )
    # Phase 97.3: finished_signal now carries (int, int, int) — token added.
    worker.finished_signal.connect(
        lambda fc, fb, tok: finished_counts.append((fc, fb)),
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
# T-E-5: no QWidget mutation in worker thread (Phase 97 U-03 original)
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


# ---------------------------------------------------------------------------
# Phase 97.3 helpers
# ---------------------------------------------------------------------------

def _require_pyqt():
    try:
        from PyQt6.QtWidgets import QApplication  # noqa: F401
    except ImportError:
        pytest.skip("PyQt6 not available in this environment")
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication(sys.argv[:1])


# ---------------------------------------------------------------------------
# D-13a / D-17 — token guard on batch_emitted
# ---------------------------------------------------------------------------

def test_stale_batch_token_dropped(tmp_path):
    """D-13a + D-17: batch_emitted carries (list, int) — the int is the token.

    Constructs a FolderWalkWorker with a known token, manually emits a batch
    payload with a STALE token, and verifies the receiving lambda observes
    that the token argument does NOT match the worker's current token. The
    real UI slot (_on_tree_batch in _UnifiedFileTreeWidget) implements the
    drop-on-stale-token behaviour; this test pins the signal SHAPE so the
    UI slot can read the token.

    Signal shape (Codex Critique #3 wording fix):
      batch_emitted = pyqtSignal(list, int)
      - list = list of 4-tuples (filepath, canonical, mtime_ns, size)
      - int  = generation token (NOT embedded inside the tuple)
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    # Worker constructed with token=42
    worker = FolderWalkWorker([str(tmp_path)], token=42)
    assert worker.token == 42, "Worker must expose its token via .token property"

    received = []
    worker.batch_emitted.connect(
        lambda batch, tok: received.append((batch, tok)),
        Qt.ConnectionType.DirectConnection,
    )

    # Emit a stale batch (token=0 — pretend a previous-generation worker is firing)
    fake_batch = [("/tmp/a.pdf", "/tmp/a.pdf", 0, 0)]
    worker.batch_emitted.emit(fake_batch, 0)

    assert received, "batch_emitted should have delivered the synthetic emit"
    batch, tok = received[0]
    assert tok == 0, (
        f"Token in delivered signal should be 0 (stale); got {tok}. "
        "Signal arity must be (list, int) — see D-13/D-17 wording."
    )
    assert tok != worker.token, (
        "Stale token must differ from worker's current token so UI slot can drop it."
    )


# ---------------------------------------------------------------------------
# D-13b / D-17 — token propagated through finished_signal
# ---------------------------------------------------------------------------

def test_stale_finished_signal_ignored(tmp_path):
    """D-13b + D-17: finished_signal carries (total_files, total_bytes, token).

    Constructs a worker with token=7, runs it on an empty folder, and
    confirms the finished_signal delivers the token matching the worker's
    construction-time argument. The UI slot can then compare against its
    current _tree_token to drop stale finishes from superseded workers.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    empty = tmp_path / "empty"
    empty.mkdir()
    worker = FolderWalkWorker([str(empty)], token=7)

    finishes = []
    worker.finished_signal.connect(
        lambda fc, fb, tok: finishes.append((fc, fb, tok)),
        Qt.ConnectionType.DirectConnection,
    )
    worker.start()
    worker.wait(5_000)

    assert finishes, "finished_signal must fire even on empty folder"
    fc, fb, tok = finishes[0]
    assert fc == 0, f"Empty folder should report 0 files; got {fc}"
    assert tok == 7, (
        f"finished_signal must carry the construction-time token; got {tok}, expected 7. "
        "D-17 invariant — all 3 signals (batch/finished/error) token-guarded."
    )


# ---------------------------------------------------------------------------
# D-14a — unsupported files are NOT canonicalized
# ---------------------------------------------------------------------------

def test_unsupported_files_not_canonicalized(tmp_path, monkeypatch):
    """D-14a: extension pre-filter drops unsupported files BEFORE _canonical_filepath.

    Creates a folder with 1 .pdf + 1 .ds_store + 1 .git_index, monkeypatches
    _canonical_filepath to count calls, runs the worker, and asserts the
    count == 1 (only the supported .pdf paid the Path.resolve tax). The 95%
    of typical noisy folders that are mostly unsupported files must pay zero
    canonicalize tax — this is the load-bearing perf claim for D-02.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    # Create one supported + two unsupported files
    (tmp_path / "real.pdf").write_text("pdf content")
    (tmp_path / ".ds_store").write_text("ds_store noise")
    (tmp_path / ".git_index").write_text("git index noise")

    # Count calls to _canonical_filepath at BOTH the import site (shared.local_sys_id)
    # AND any in-module binding in desktop.my_library_tab (the worker imports lazily,
    # so we must patch the canonical module name to catch the lazy reference).
    import shared.local_sys_id as _sysid
    call_count = [0]
    real_canonical = _sysid._canonical_filepath

    def _counting(p):
        call_count[0] += 1
        return real_canonical(p)

    monkeypatch.setattr(_sysid, "_canonical_filepath", _counting)

    # Also patch any module-level binding in desktop.my_library_tab (Codex Critique #2
    # warning — if the worker imports the symbol at module top, monkeypatching the
    # source module alone won't intercept; both bindings must be patched).
    import desktop.my_library_tab as _mlt
    if hasattr(_mlt, "_canonical_filepath"):
        monkeypatch.setattr(_mlt, "_canonical_filepath", _counting)

    received = []
    worker = FolderWalkWorker([str(tmp_path)], token=1)
    worker.batch_emitted.connect(
        lambda batch, tok: received.extend(batch),
        Qt.ConnectionType.DirectConnection,
    )
    worker.start()
    worker.wait(5_000)

    assert call_count[0] == 1, (
        f"D-14a: _canonical_filepath should be called exactly once for the .pdf; "
        f"got {call_count[0]} calls. Worker must pre-filter by _SUPPORTED_EXTENSIONS "
        "BEFORE stat/canonical/emit."
    )
    # The single emitted record should be the .pdf
    paths_emitted = [r[0] for r in received]
    assert any(p.lower().endswith("real.pdf") for p in paths_emitted), (
        f"D-14a: real.pdf should be in emitted batch; got {paths_emitted}"
    )
    assert not any(p.lower().endswith(".ds_store") for p in paths_emitted), (
        ".ds_store must NOT be emitted by the worker"
    )
    assert not any(p.lower().endswith(".git_index") for p in paths_emitted), (
        ".git_index must NOT be emitted by the worker"
    )


# ---------------------------------------------------------------------------
# D-14b — mixed-case extensions
# ---------------------------------------------------------------------------

def test_mixed_case_extensions_normalized(tmp_path):
    """D-14b: .PDF, .Pdf, .pdf all accepted (case-insensitive ext match).

    R97.3-N requires extensions matched case-insensitively so users with
    .PDF or .Pdf filenames (common on Windows) see their files in the tree.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    (tmp_path / "lower.pdf").write_text("a")
    (tmp_path / "upper.PDF").write_text("b")
    (tmp_path / "mixed.Pdf").write_text("c")

    received = []
    worker = FolderWalkWorker([str(tmp_path)], token=1)
    worker.batch_emitted.connect(
        lambda batch, tok: received.extend(batch),
        Qt.ConnectionType.DirectConnection,
    )
    worker.start()
    worker.wait(5_000)

    basenames = sorted(os.path.basename(r[0]) for r in received)
    expected = sorted(["lower.pdf", "upper.PDF", "mixed.Pdf"])
    assert basenames == expected, (
        f"D-14b: all 3 mixed-case .PDF/.Pdf/.pdf files must be emitted; "
        f"got {basenames}, expected {expected}"
    )


# ---------------------------------------------------------------------------
# D-15 — junctions / symlinks not followed
# ---------------------------------------------------------------------------

def _ast_walk_has_followlinks_false_and_onerror():
    """Codex Critique #3 round-4 tightened AST fallback for CI.

    Parses desktop/my_library_tab.py, finds FolderWalkWorker.run, walks its
    body for the os.walk Call node, and returns (followlinks_ok, onerror_ok).
    """
    run_node = _find_folder_walk_worker_run_ast()
    if run_node is None:
        return False, False
    followlinks_ok = False
    onerror_ok = False
    for node in ast.walk(run_node):
        if isinstance(node, ast.Call):
            # Match `os.walk(...)` or `_os.walk(...)`
            fn = node.func
            is_walk = (
                isinstance(fn, ast.Attribute) and fn.attr == "walk"
            )
            if not is_walk:
                continue
            for kw in node.keywords:
                if kw.arg == "followlinks":
                    if isinstance(kw.value, ast.Constant) and kw.value.value is False:
                        followlinks_ok = True
                if kw.arg == "onerror":
                    # Any non-None value satisfies the contract
                    if not (isinstance(kw.value, ast.Constant) and kw.value.value is None):
                        onerror_ok = True
    return followlinks_ok, onerror_ok


def test_junction_not_followed(tmp_path):
    """D-15: os.walk(followlinks=False) — junction/symlink contents NOT recursed.

    Creates a junction (Windows mklink /J) or symlink (POSIX os.symlink)
    pointing OUT of the test tree. Runs the worker on the outer folder.
    Asserts NO file from the junction target appears in the emitted batches.

    Codex Critique #3 round-4 MEDIUM CI-fallback: if neither mklink /J (needs
    admin/dev-mode) nor os.symlink (needs privilege) is available, fall back
    to an AST-level static check that proves the `os.walk` call uses BOTH
    `followlinks=False` AND `onerror=...`. A grep fallback is NOT sufficient.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    # Build the target (outside the outer tree)
    junction_target = tmp_path / "target"
    junction_target.mkdir()
    (junction_target / "hidden.pdf").write_text("hidden via junction")
    outer = tmp_path / "outer"
    outer.mkdir()
    junction_path = outer / "inner_junction"

    used_static_fallback = False
    if sys.platform == "win32":
        rc = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction_path), str(junction_target)],
            check=False, capture_output=True,
        ).returncode
        if rc != 0:
            # Fall back to AST-level static check (Codex Critique #3 tightened).
            followlinks_ok, onerror_ok = _ast_walk_has_followlinks_false_and_onerror()
            assert followlinks_ok and onerror_ok, (
                "D-15: mklink /J unavailable (dev-mode/admin required) AND "
                "FolderWalkWorker.run AST does NOT contain os.walk(..., "
                "followlinks=False, onerror=...). Cannot prove junction "
                f"non-following invariant. followlinks_ok={followlinks_ok}, "
                f"onerror_ok={onerror_ok}"
            )
            used_static_fallback = True
    else:
        try:
            os.symlink(str(junction_target), str(junction_path))
        except (OSError, NotImplementedError):
            followlinks_ok, onerror_ok = _ast_walk_has_followlinks_false_and_onerror()
            assert followlinks_ok and onerror_ok, (
                "D-15: os.symlink unavailable AND FolderWalkWorker.run AST does NOT "
                "contain os.walk(..., followlinks=False, onerror=...). "
                f"followlinks_ok={followlinks_ok}, onerror_ok={onerror_ok}"
            )
            used_static_fallback = True

    if used_static_fallback:
        return  # static check already passed

    received = []
    worker = FolderWalkWorker([str(outer)], token=1)
    worker.batch_emitted.connect(
        lambda batch, tok: received.extend(batch),
        Qt.ConnectionType.DirectConnection,
    )
    worker.start()
    worker.wait(5_000)

    paths_emitted = [r[0] for r in received]
    for p in paths_emitted:
        assert "hidden.pdf" not in p.lower(), (
            f"D-15: junction target file should NOT appear; got {paths_emitted}. "
            "FolderWalkWorker.run must call os.walk(folder, followlinks=False)."
        )


# ---------------------------------------------------------------------------
# D-17 — error_signal carries token
# ---------------------------------------------------------------------------

def test_error_signal_carries_token(tmp_path):
    """D-17: error_signal carries (msg, token).

    Manually emits an error_signal with a known token and verifies the
    receiver gets both args. Real worker errors are race-prone to capture in
    a unit test, so we use a manual emit (mirroring the D-13a pattern) —
    this pins the SIGNAL ARITY which the UI slot relies on for token
    comparison.
    """
    _require_pyqt()
    from PyQt6.QtCore import Qt
    from desktop.my_library_tab import FolderWalkWorker

    worker = FolderWalkWorker([str(tmp_path)], token=99)

    received = []
    worker.error_signal.connect(
        lambda msg, tok: received.append((msg, tok)),
        Qt.ConnectionType.DirectConnection,
    )

    worker.error_signal.emit("synthetic boom", 99)

    assert received, "error_signal should have delivered"
    msg, tok = received[0]
    assert msg == "synthetic boom"
    assert tok == 99, (
        f"D-17: error_signal must carry the token; got {tok}, expected 99."
    )
