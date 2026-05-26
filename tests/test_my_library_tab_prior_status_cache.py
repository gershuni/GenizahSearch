# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-A (D-12 + D-19) — prior_status cache + invalidation ordering.

Pins the new ``_prior_status_cache`` on ``MyLibraryTab`` and the
"invalidate-then-refresh" ordering required to make the post-scan tree
reflect the latest DB state (Codex Critique #2 v7.14 blocker).

  Test 1  test_prior_status_cache_initialised_in_init
  Test 2  test_cache_cleared_before_refresh_folder_list_ui_on_finished
  Test 3  test_cache_invalidated_on_reset
  Test 4  test_populate_does_not_issue_db_query
  Test 5  test_cache_cleared_before_refresh_in_cancel_finished_drain
          (Codex Critique #3 MEDIUM site 6 — _on_cancel_finished_drain)
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


def _make_tab(tmp_path, monkeypatch):
    """Construct a real MyLibraryTab against an isolated tmp_path index."""
    from genizah_core import Config
    idx_dir = str(tmp_path / "local_index")
    lab_dir = str(tmp_path / "local_lab")
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", idx_dir, raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", lab_dir, raising=False)

    from desktop.my_library_tab import MyLibraryTab
    tab = MyLibraryTab(parent=None)
    return tab


def _close_tab(tab):
    try:
        if getattr(tab, "_indexer", None) is not None:
            try:
                tab._indexer._conn.close()
            except Exception:
                pass
        tab.deleteLater()
    except Exception:
        pass


def _seed_local_file(tab, folder_path, canonical, status="ok", pages=1, sys_id_suffix="001"):
    """Insert a row in the indexer's local_files table for testing."""
    import os
    from shared.local_sys_id import _canonical_filepath
    folder_canon = _canonical_filepath(folder_path)
    # add_folder if not present
    row = tab._indexer._conn.execute(
        "SELECT folder_id FROM folders WHERE path = ?", (folder_canon,)
    ).fetchone()
    if row is None:
        tab._indexer.add_folder(folder_path)
        row = tab._indexer._conn.execute(
            "SELECT folder_id FROM folders WHERE path = ?", (folder_canon,)
        ).fetchone()
    folder_id = row["folder_id"]
    name = os.path.basename(canonical)
    ext = os.path.splitext(name)[1].lower()
    sys_id = f"LOCAL{sys_id_suffix:<15}"[:20]
    tab._indexer._conn.execute(
        "INSERT OR REPLACE INTO local_files "
        "(sys_id, filepath, folder_id, display_title, original_filename, "
        "file_extension, page_count, file_size_bytes, extraction_status, "
        "last_indexed_at, pending_delete) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 0)",
        (sys_id, canonical, folder_id, name, name, ext, pages, status),
    )
    tab._indexer._conn.commit()


# ---------------------------------------------------------------------------
# Test 1 — cache initialised in __init__
# ---------------------------------------------------------------------------

def test_prior_status_cache_initialised_in_init(tmp_path, monkeypatch):
    """D-12: MyLibraryTab.__init__ initialises _prior_status_cache to a dict."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert hasattr(tab, "_prior_status_cache")
        assert isinstance(tab._prior_status_cache, dict), (
            "D-12: _prior_status_cache must be a dict"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 2 — cache reloaded BEFORE _refresh_folder_list_ui on worker finish
# ---------------------------------------------------------------------------

def test_cache_cleared_before_refresh_folder_list_ui_on_finished(tmp_path, monkeypatch):
    """Codex Critique #2 v7.14 blocker: cache must be reloaded BEFORE
    _refresh_folder_list_ui (which calls populate_for_folder, which reads
    the cache). Late clearing leaves stale prior_status visible.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        fp = str(folder / "scan.pdf")
        (folder / "scan.pdf").write_text("x")
        from shared.local_sys_id import _canonical_filepath
        canonical = _canonical_filepath(fp)

        # Seed local_files with status='ok'
        _seed_local_file(tab, str(folder), canonical, status="ok", pages=1)
        # Force the cache to start with stale 'ok'
        tab._invalidate_prior_status_cache()

        # Sanity: the cache has the 'ok' status now.
        from shared.local_sys_id import _canonical_filepath as _canon
        folder_canon = _canon(str(folder))
        # Note: get_file_status_for_folder normalises folder via _canonical_filepath,
        # but list_folders returns canonical paths from the DB. The cache key is
        # whatever list_folders returns (canonical). Use that.
        cache_key = folder_canon
        # Spy on _refresh_folder_list_ui to capture cache at runtime
        captured = {}
        real_refresh = tab._refresh_folder_list_ui

        def _spy():
            # Deep-snapshot the cache so subsequent mutation doesn't change it
            captured["cache"] = {
                k: dict(v) if isinstance(v, dict) else v
                for k, v in tab._prior_status_cache.items()
            }
            real_refresh()

        monkeypatch.setattr(tab, "_refresh_folder_list_ui", _spy)

        # Flip DB row to status='cancelled' BEFORE _on_worker_finished
        tab._indexer._conn.execute(
            "UPDATE local_files SET extraction_status = 'cancelled' WHERE filepath = ?",
            (canonical,),
        )
        tab._indexer._conn.commit()

        # Avoid touching real search_engine / lab_engine subsystems.
        monkeypatch.setattr(tab, "_reload_all_local_indexes", lambda: None)
        monkeypatch.setattr(tab, "_maybe_rebuild_lab_if_stale", lambda: False)
        monkeypatch.setattr(tab, "_update_disk_indicator", lambda: None)
        # Acquire the mutex so _on_worker_finished's unlock matches (it expects
        # the worker has locked it inside _start_worker).
        tab._indexer_mutex.tryLock()

        tab._on_worker_finished(
            {"indexed": 0, "skipped": 0, "errors": 0, "cancelled": False}, False,
        )

        # Cache observed at the moment _refresh_folder_list_ui ran must contain
        # the NEW status ('cancelled').
        cache_at_refresh = captured.get("cache", {})
        folder_cache = cache_at_refresh.get(cache_key, {})
        file_entry = folder_cache.get(canonical, {})
        assert file_entry.get("status") == "cancelled", (
            f"D-12/D-19 v7.14 blocker: cache was NOT reloaded BEFORE "
            f"_refresh_folder_list_ui. captured cache={folder_cache!r}; "
            f"expected status='cancelled' for canonical={canonical}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 3 — cache invalidated on reset
# ---------------------------------------------------------------------------

def test_cache_invalidated_on_reset(tmp_path, monkeypatch):
    """D-12: _perform_reset clears the cache (and reloads empty since DB is empty)."""
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        fp = str(folder / "scan.pdf")
        (folder / "scan.pdf").write_text("x")
        from shared.local_sys_id import _canonical_filepath
        canonical = _canonical_filepath(fp)
        _seed_local_file(tab, str(folder), canonical, status="ok")
        tab._invalidate_prior_status_cache()
        assert tab._prior_status_cache, "cache should be populated pre-reset"

        # Monkeypatch reset_my_library to a no-op (we just want to verify the
        # invalidation hook runs).
        def _fake_reset(close_cb, reload_cb):
            close_cb()
            reload_cb()
            # Simulate "the DB is now empty" by deleting the row
            tab._indexer._conn.execute("DELETE FROM local_files")
            tab._indexer._conn.execute("DELETE FROM folders")
            tab._indexer._conn.commit()

        monkeypatch.setattr(tab._indexer, "reset_my_library", _fake_reset)
        # Bypass the typed-confirm dialog
        tab._perform_reset()

        # After reset, the cache should be empty (no folders → no entries).
        assert tab._prior_status_cache == {}, (
            f"D-12: _perform_reset must clear the cache; "
            f"got {tab._prior_status_cache!r}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 4 — populate_for_folder does not query the DB on click path
# ---------------------------------------------------------------------------

def test_populate_does_not_issue_db_query(tmp_path, monkeypatch):
    """D-12: with cache populated, populate_for_folder uses the cache; the
    indexer's get_file_status_for_folder is NOT called.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        (folder / "x.pdf").write_text("x")
        tab._indexer.add_folder(str(folder))
        tab._invalidate_prior_status_cache()

        # Wire MyLibraryTab onto the tree's app so cache lookup finds it.
        tab._unified_tree._app = tab

        calls = []
        original_gffs = tab._indexer.get_file_status_for_folder

        def _spy_get(fp):
            calls.append(fp)
            return original_gffs(fp)

        monkeypatch.setattr(
            tab._indexer, "get_file_status_for_folder", _spy_get,
        )

        # Trigger the click path
        tab._unified_tree.populate_for_folder(str(folder))

        # Drain any worker so the test cleans up nicely.
        import time as _t
        deadline = _t.monotonic() + 5.0
        while _t.monotonic() < deadline:
            QApplication.processEvents()
            if getattr(tab._unified_tree, "_tree_worker", None) is None:
                break
            _t.sleep(0.01)

        assert calls == [], (
            f"D-12: populate_for_folder must use the cache; "
            f"get_file_status_for_folder was called {len(calls)} times: {calls}"
        )
    finally:
        _close_tab(tab)


# ---------------------------------------------------------------------------
# Test 5 — Codex Critique #3 MEDIUM site 6: cancel-drain ordering
# ---------------------------------------------------------------------------

def test_cache_cleared_before_refresh_in_cancel_finished_drain(tmp_path, monkeypatch):
    """Codex Critique #3 MEDIUM site 6: _on_cancel_finished_drain calls
    _refresh_folder_list_ui at the end of its body — the cache invalidation
    must happen BEFORE that call or the post-cancel tree shows stale status.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        folder = tmp_path / "corpus"
        folder.mkdir()
        fp = str(folder / "scan.pdf")
        (folder / "scan.pdf").write_text("x")
        from shared.local_sys_id import _canonical_filepath
        canonical = _canonical_filepath(fp)
        folder_canon = _canonical_filepath(str(folder))
        _seed_local_file(tab, str(folder), canonical, status="ok")
        tab._invalidate_prior_status_cache()

        captured = {}
        real_refresh = tab._refresh_folder_list_ui

        def _spy():
            captured["cache"] = {
                k: dict(v) if isinstance(v, dict) else v
                for k, v in tab._prior_status_cache.items()
            }
            real_refresh()

        monkeypatch.setattr(tab, "_refresh_folder_list_ui", _spy)

        # Flip DB row to status='cancelled' BEFORE the cancel-drain call
        tab._indexer._conn.execute(
            "UPDATE local_files SET extraction_status = 'cancelled' WHERE filepath = ?",
            (canonical,),
        )
        tab._indexer._conn.commit()

        tab._on_cancel_finished_drain({})

        cache_at_refresh = captured.get("cache", {})
        folder_cache = cache_at_refresh.get(folder_canon, {})
        file_entry = folder_cache.get(canonical, {})
        assert file_entry.get("status") == "cancelled", (
            f"Codex Critique #3 site 6: _on_cancel_finished_drain must "
            f"invalidate the cache BEFORE _refresh_folder_list_ui; "
            f"captured cache={folder_cache!r}; expected status='cancelled'"
        )
    finally:
        _close_tab(tab)
