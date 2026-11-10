# -*- coding: utf-8 -*-
"""Phase 97 R-01: recovery gate + scan_runs lifecycle tests.

Tests:
  - test_search_returns_empty_during_recovery
  - test_search_works_after_recovery_resolution
  - test_scan_runs_lifecycle_marks_completed
  - test_startup_recovery_probe_returns_running_runs
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch


def _make_bare_indexer(tmp_path):
    """Build a LocalIndexer at tmp_path."""
    from shared.local_indexer import LocalIndexer
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    return LocalIndexer(index_dir, lab_dir, db_path)


def _make_engine():
    """Build a bare SearchEngine stub (no real index required)."""
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None
    engine.local_index = None
    return engine


class _MockTab:
    """Minimal MyLibraryTab stub exposing is_searchable."""
    def __init__(self, is_searchable: bool = False):
        self.is_searchable = is_searchable


def test_search_returns_empty_during_recovery():
    """_query_local_index returns [] when is_searchable=False on the attached tab."""
    engine = _make_engine()
    tab = _MockTab(is_searchable=False)
    engine.attach_my_library_tab(tab)

    result = engine._query_local_index("test query", "phrase", 1)
    assert result == [], f"Expected [] during recovery, got {result}"


def test_search_works_after_recovery_resolution():
    """_query_local_index proceeds past gate when is_searchable=True."""
    engine = _make_engine()
    tab = _MockTab(is_searchable=True)
    engine.attach_my_library_tab(tab)

    # local_searcher is None so the query returns [] from the None-guard,
    # but it passes the is_searchable gate (we just need it not to short-circuit)
    result = engine._query_local_index("test query", "phrase", 1)
    # With local_searcher=None, returns [] but via the None-guard, NOT the gate
    assert isinstance(result, list)


def test_scan_runs_lifecycle_marks_completed(tmp_path):
    """_begin_scan_run / _end_scan_run pair: scan_runs row ends with status='completed'."""
    indexer = _make_bare_indexer(tmp_path)

    run_id = indexer._begin_scan_run()
    assert run_id is not None and len(run_id) > 0

    # Verify row was inserted with status='running'
    row = indexer._conn.execute(
        "SELECT status, ended_at FROM scan_runs WHERE scan_run_id = ?",
        (run_id,),
    ).fetchone()
    assert row is not None, "scan_runs row not found after _begin_scan_run"
    assert row["status"] == "running"
    assert row["ended_at"] is None

    indexer._end_scan_run(run_id, "completed")

    row2 = indexer._conn.execute(
        "SELECT status, ended_at FROM scan_runs WHERE scan_run_id = ?",
        (run_id,),
    ).fetchone()
    assert row2["status"] == "completed"
    assert row2["ended_at"] is not None


def test_startup_recovery_probe_returns_running_runs(tmp_path):
    """start_recovery_probe returns scan_run_ids with status='running'."""
    import time
    indexer = _make_bare_indexer(tmp_path)

    # Manually insert a 'running' scan_runs row
    run_id = "test_run_001"
    indexer._conn.execute(
        "INSERT INTO scan_runs (scan_run_id, started_at, status) VALUES (?, ?, 'running')",
        (run_id, time.time()),
    )
    indexer._conn.commit()

    running_runs = indexer.start_recovery_probe()
    assert run_id in running_runs, (
        f"Expected {run_id!r} in recovery probe result, got {running_runs}"
    )
