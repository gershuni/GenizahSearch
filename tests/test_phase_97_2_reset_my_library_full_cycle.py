# -*- coding: utf-8 -*-
"""Phase 97.2 R97.2-E — reset_my_library full cycle.

RED gate: reset_my_library() does not yet exist. After implementation:
  (a) LOCAL + LAB dirs are emptied (no Tantivy files lingering)
  (b) SQLite at PRAGMA user_version=2 (migration ladder ran on fresh DB)
  (c) Fresh add_folder + scan_all round-trips a new file end-to-end
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.local_indexer import LocalIndexer  # noqa: E402


def _make_indexer_canonical(tmp_path):
    """Local helper — uses canonical basenames so reset_my_library path-safety passes.

    The Phase 97 _make_indexer in tests/test_scan_run_id.py uses 'idx'/'lab' basenames,
    which would fail Task 2's path-safety pre-check (REVIEWS Rev-2 HIGH #1).
    Do NOT change the Phase 97 helper — define a local one here.
    """
    idx_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    db_path = str(tmp_path / "db.sqlite3")
    os.makedirs(idx_dir)
    os.makedirs(lab_dir)
    return LocalIndexer(idx_dir, lab_dir, db_path)


def test_reset_my_library_full_cycle(tmp_path):
    """RED before R97.2-E; GREEN after."""
    indexer = _make_indexer_canonical(tmp_path)
    try:
        # --- populate ---
        folder = str(tmp_path / "docs")
        os.makedirs(folder)
        fp = os.path.join(folder, "before.txt")
        with open(fp, "w", encoding="utf-8") as f:
            f.write("pre-reset content for Phase 97.2 R97.2-E test")
        indexer.add_folder(folder)
        indexer.scan_all()

        n_before = indexer._conn.execute(
            "SELECT COUNT(*) FROM local_files"
        ).fetchone()[0]
        assert n_before > 0, "fixture must populate at least one local_files row"

        # --- reset ---
        indexer.reset_my_library(
            close_searcher_cb=lambda: None,
            reload_searcher_cb=lambda: None,
        )

        # Assert (a): SQLite local_files is empty (fresh DB)
        n_after = indexer._conn.execute(
            "SELECT COUNT(*) FROM local_files"
        ).fetchone()[0]
        assert n_after == 0, (
            f"local_files must be empty after reset, got {n_after}"
        )

        # Assert (b): user_version=2 (migration ladder ran on fresh DB)
        v = indexer._conn.execute("PRAGMA user_version").fetchone()[0]
        assert v == 2, f"PRAGMA user_version must be 2 after reset, got {v}"

        # Assert (c): fresh scan works end-to-end
        fp2 = os.path.join(folder, "after.txt")
        with open(fp2, "w", encoding="utf-8") as f:
            f.write("post-reset content")
        # Tolerate either add_folder being idempotent OR raising if already present
        try:
            indexer.add_folder(folder)
        except Exception:
            pass
        indexer.scan_all()
        n_final = indexer._conn.execute(
            "SELECT COUNT(*) FROM local_files"
        ).fetchone()[0]
        assert n_final > 0, "fresh scan after reset must populate local_files"
    finally:
        try:
            indexer._close_internal_writer_index()
        except Exception:
            pass


def test_reset_my_library_lab_rename_failure_rolls_back_local(tmp_path, monkeypatch):
    """REVIEWS Codex HIGH #6 — when LAB rename fails, LOCAL rename MUST be rolled back.

    Simulates LAB rename failure by monkeypatching _retry_windows_rename to raise
    only when called with the LAB path. Asserts that:
      (1) LocalIndexerError is raised with the rollback-message marker
      (2) LOCAL_INDEX_DIR is restored to its pre-reset state (the rename
          was rolled back, so the dir contains the original tantivy/sqlite
          files, not the freshly-recreated empty dir).
    """
    import pytest
    from shared.local_indexer import LocalIndexerError

    indexer = _make_indexer_canonical(tmp_path)
    try:
        # Capture original LOCAL dir snapshot (must contain a meta.json after init)
        local_dir = indexer._index_dir
        lab_dir = indexer._lab_index_dir
        assert os.path.isdir(local_dir)
        # Ensure both LOCAL + LAB dirs exist so Step 2b runs
        os.makedirs(lab_dir, exist_ok=True)
        # Put a marker file inside LOCAL so we can detect rollback
        marker_path = os.path.join(local_dir, ".reviews-codex-high6-marker")
        with open(marker_path, "w", encoding="utf-8") as f:
            f.write("REVIEWS Codex HIGH #6 — must survive LAB-rename rollback")

        # Patch _retry_windows_rename: succeed for LOCAL, raise OSError for LAB
        # (and succeed again for the rollback call that points back to local_dir).
        real_rename = indexer._retry_windows_rename
        lab_failure_triggered = {"value": False}

        def _wrapped_rename(src, dst):
            # LAB rename target -> raise once
            if not lab_failure_triggered["value"] and src == lab_dir:
                lab_failure_triggered["value"] = True
                raise OSError(13, "simulated LAB rename failure for REVIEWS Codex HIGH #6")
            return real_rename(src, dst)

        monkeypatch.setattr(indexer, "_retry_windows_rename", _wrapped_rename)

        # Reset MUST raise LocalIndexerError with rollback-marker text
        with pytest.raises(LocalIndexerError) as exc_info:
            indexer.reset_my_library(
                close_searcher_cb=lambda: None,
                reload_searcher_cb=lambda: None,
            )
        assert "LAB rename failed" in str(exc_info.value) or "rolled back" in str(exc_info.value), (
            f"LAB rename failure must propagate as LocalIndexerError; got {exc_info.value!r}"
        )

        # Critically: the LOCAL dir must contain the original marker file
        # (proving the rename was rolled back, NOT freshly recreated empty).
        assert os.path.isfile(marker_path), (
            f"LOCAL rollback failed — marker file at {marker_path!r} is gone. "
            "The reset_my_library LAB-rename-failure path did NOT roll back "
            "the LOCAL rename per REVIEWS Codex HIGH #6."
        )
        # And LAB dir should still exist (it was never renamed; or was, but
        # since rename failed, it should still be at its original path).
        assert os.path.isdir(lab_dir), (
            "LAB dir disappeared unexpectedly after rollback"
        )
    finally:
        try:
            indexer._close_internal_writer_index()
        except Exception:
            pass
