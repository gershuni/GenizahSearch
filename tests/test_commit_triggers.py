# -*- coding: utf-8 -*-
"""Phase 97 Wave B — C-02 _CommitTriggers tests.

Tests:
- test_commit_fires_on_bytes_threshold: 200 MB source bytes triggers commit
- test_commit_fires_on_file_count: 100 files triggers commit
- test_commit_fires_on_seconds: 60s elapsed triggers commit
- test_reset_zeros_all_counters: reset clears all three predicates
- test_heap_sampling_path_absent: AST guard — no writer.get_memory_usage call (RESEARCH Issue #1)
"""
from __future__ import annotations

import ast
import pathlib


from shared.local_indexer import _CommitTriggers


def test_commit_fires_on_bytes_threshold():
    """record_file(210MB) once -> should_commit() True (bytes threshold = 200 MB)."""
    ct = _CommitTriggers()
    ct.record_file(210 * 1024 * 1024)
    assert ct.should_commit(), "Expected should_commit() True after 210 MB source bytes"


def test_commit_fires_on_file_count():
    """record_file 100 times with 1 KB each -> should_commit() True (file count = 100)."""
    ct = _CommitTriggers()
    for _ in range(100):
        ct.record_file(1024)
    assert ct.should_commit(), "Expected should_commit() True after 100 files"


def test_commit_fires_on_seconds(monkeypatch):
    """time.monotonic patched to advance 61s -> should_commit() True (seconds = 60)."""
    # Capture start time (returned during __init__)
    start_time = 1_000_000.0
    call_count = [0]

    def fake_monotonic():
        call_count[0] += 1
        # First call is during __init__ (sets _batch_start)
        if call_count[0] == 1:
            return start_time
        # Subsequent calls (should_commit) return start + 61s
        return start_time + 61.0

    monkeypatch.setattr("shared.local_indexer.time.monotonic", fake_monotonic)
    ct = _CommitTriggers()
    ct.record_file(1)
    assert ct.should_commit(), "Expected should_commit() True after 61s elapsed"


def test_reset_zeros_all_counters(monkeypatch):
    """After reset(), all three predicates revert to False."""
    start_time = 1_000_000.0
    call_count = [0]

    def fake_monotonic():
        call_count[0] += 1
        # First call is during __init__; after reset() one more call is made
        # All calls return the same start_time so elapsed = 0
        return start_time

    monkeypatch.setattr("shared.local_indexer.time.monotonic", fake_monotonic)
    ct = _CommitTriggers()

    # Trigger all three conditions
    ct._batch_bytes = 300 * 1024 * 1024
    ct._batch_files = 200
    # Time-based already 0 via fake_monotonic

    assert ct._batch_bytes >= ct.BYTES_THRESHOLD
    assert ct._batch_files >= ct.FILES_THRESHOLD

    ct.reset()

    # After reset, bytes + files should be zero
    assert ct._batch_bytes == 0
    assert ct._batch_files == 0
    # should_commit() must be False on all three predicates
    assert not ct.should_commit(), (
        "Expected should_commit() False after reset() — all three predicates should be clear"
    )


def test_heap_sampling_path_absent():
    """AST guard: shared/local_indexer.py must NOT contain writer.get_memory_usage (RESEARCH Issue #1).

    tantivy-py 0.25.1 has no get_memory_usage() method. The heap-sampling branch is
    DROPPED per RESEARCH Issue #1. Only byte/count/time triggers exist.
    """
    src = pathlib.Path("shared/local_indexer.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    attr_names = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }
    assert "get_memory_usage" not in attr_names, (
        "Found writer.get_memory_usage in shared/local_indexer.py — "
        "heap-sampling path must be ABSENT per RESEARCH Issue #1 (tantivy-py 0.25.1 has no such method). "
        "See TODO(tantivy >= 0.26) comment for the deferred re-add path."
    )
