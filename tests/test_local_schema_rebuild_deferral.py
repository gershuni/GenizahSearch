"""SEED-006 HIGH-5 — deferred LOCAL schema-rebuild (off the UI thread).

A schema-marker change (e.g. the SEED-006 `content` -> hebword + `content_search`
migration) makes `LocalIndexer.__init__` rebuild from cached_text. On the desktop
that constructor runs on the UI thread, so a synchronous rebuild freezes launch.
`defer_schema_rebuild=True` makes __init__ flag `needs_schema_rebuild` instead and
leaves the indexer not-ready; the caller runs `run_deferred_schema_rebuild()` on a
background thread (its own per-thread SQLite connection) and gates search until it
returns.

These tests exercise the SHARED mechanism only (no Qt / UI thread). The desktop
wiring in MyLibraryTab that calls these is validated by a real desktop smoke test
(can't run headless).
"""

import os

import tantivy

import sqlite3

import pytest

from shared.local_indexer import (
    LocalIndexer,
    LocalIndexerError,
    local_db_path_for,
    migrate_legacy_local_db,
    _compute_schema_marker,
    _read_schema_marker,
    _write_schema_marker,
    build_local_schema,
)


def _make_dirs(tmp_path):
    index_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    # SEED-006 P1: DB lives in the PARENT of the swapped index dir, never inside.
    db_path = str(tmp_path / "local_index.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    return index_dir, lab_dir, db_path


def _new_indexer(index_dir, lab_dir, db_path, **kw):
    return LocalIndexer(index_dir=index_dir, lab_index_dir=lab_dir, db_path=db_path, **kw)


def _force_marker_mismatch(index_dir):
    """Write a bogus .schema_version so the next open sees a schema mismatch."""
    _write_schema_marker(index_dir, "0000deadbeef0000")


def test_fresh_construction_is_ready_and_not_flagged(tmp_path):
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        # No prior index → fresh create, no mismatch → ready immediately.
        assert idx.needs_schema_rebuild is False
        assert idx._index is not None
    finally:
        idx.close()


def test_default_rebuilds_synchronously_on_mismatch(tmp_path):
    """defer_schema_rebuild=False (the default) preserves the old behaviour."""
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()  # create + write marker + meta.json
    _force_marker_mismatch(index_dir)

    idx = _new_indexer(index_dir, lab_dir, db_path)  # default: no defer
    try:
        assert idx.needs_schema_rebuild is False        # rebuilt inline
        assert idx._index is not None
        assert _read_schema_marker(index_dir) == _compute_schema_marker(build_local_schema)
    finally:
        idx.close()


def test_defer_flags_and_does_not_rebuild_in_init(tmp_path):
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()
    _force_marker_mismatch(index_dir)

    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        # __init__ must NOT have rebuilt — flagged + not-ready, no exception.
        assert idx.needs_schema_rebuild is True
        assert idx._index is None
        assert idx._writer is None
        # The stale marker is untouched until the deferred rebuild runs.
        assert _read_schema_marker(index_dir) == "0000deadbeef0000"
    finally:
        idx.close()


def test_run_deferred_rebuild_makes_it_ready(tmp_path):
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()
    _force_marker_mismatch(index_dir)

    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        assert idx.needs_schema_rebuild is True
        ready = idx.run_deferred_schema_rebuild()
        assert ready is True
        assert idx.needs_schema_rebuild is False
        assert idx._index is not None
        # Marker now current; rebuilt index carries the content_search field.
        assert _read_schema_marker(index_dir) == _compute_schema_marker(build_local_schema)
        from shared.search_tokenizer import register_search_tokenizers
        probe = tantivy.Index(build_local_schema(), path=index_dir)
        register_search_tokenizers(probe)
        # parse on content_search must not raise (field exists in the rebuilt index)
        probe.parse_query('content_search:"x"', ["content_search"])
    finally:
        idx.close()


def test_run_deferred_is_noop_when_not_flagged(tmp_path):
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        assert idx.needs_schema_rebuild is False
        # No-op path: returns readiness without touching the index.
        assert idx.run_deferred_schema_rebuild() is True
        assert idx._index is not None
    finally:
        idx.close()


def test_close_internal_writer_index_forces_gc(tmp_path, monkeypatch):
    """P1: the live-index handle must be GC'd before the atomic-rebuild rename.

    On Windows, nulling self._index/_writer is not enough — Python GC may delay
    the Rust-side drop, so os.rename(live_dir -> .old) in rebuild_main_index_atomic
    fails with PermissionError(13). _close_internal_writer_index must call
    gc.collect() after dropping the handles (mirroring the rebuild validation
    block). Cannot reproduce the rename failure on POSIX, so guard the mechanism.
    """
    import shared.local_indexer as li

    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    idx = _new_indexer(index_dir, lab_dir, db_path)
    try:
        calls = {"n": 0}
        real_collect = li.gc.collect
        monkeypatch.setattr(li.gc, "collect", lambda *a, **k: (calls.__setitem__("n", calls["n"] + 1), real_collect())[1])

        idx._close_internal_writer_index()

        assert idx._index is None
        assert idx._writer is None
        assert calls["n"] >= 1, "expected gc.collect() after dropping handles"
    finally:
        idx.close()


# ---------------------------------------------------------------------------
# SEED-006 P1 — the SQLite sidecar must live OUTSIDE the atomically-swapped
# Tantivy index dir (else os.rename locks on Windows / orphans the DB on POSIX).
# ---------------------------------------------------------------------------

def test_local_db_path_is_outside_index_dir(tmp_path):
    index_dir = str(tmp_path / "LocalIndex")
    db = local_db_path_for(index_dir)
    # The resolved DB path must NOT be under the (swapped) index dir.
    assert os.path.commonpath([os.path.abspath(db), os.path.abspath(index_dir)]) != os.path.abspath(index_dir)
    assert os.path.dirname(os.path.abspath(db)) == os.path.dirname(os.path.abspath(index_dir))


def test_migrate_legacy_local_db_moves_db_out(tmp_path):
    """A legacy in-dir DB (+ -wal/-shm) is moved to the parent; new path returned."""
    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir, exist_ok=True)
    legacy = os.path.join(index_dir, "local_index.sqlite3")
    # Seed a legacy DB with a marker row + sidecar files.
    conn = sqlite3.connect(legacy)
    conn.execute("CREATE TABLE marker (v TEXT)")
    conn.execute("INSERT INTO marker VALUES ('legacy-data')")
    conn.commit()
    conn.close()
    for suffix in ("-wal", "-shm"):
        with open(legacy + suffix, "w") as f:
            f.write("x")

    new_path = migrate_legacy_local_db(index_dir)

    assert new_path == local_db_path_for(index_dir)
    assert not os.path.exists(legacy), "legacy DB should have been moved out"
    assert os.path.exists(new_path), "DB should now be at the external path"
    assert os.path.exists(new_path + "-wal") and os.path.exists(new_path + "-shm")
    # Data survived the move.
    conn2 = sqlite3.connect(new_path)
    assert conn2.execute("SELECT v FROM marker").fetchone()[0] == "legacy-data"
    conn2.close()


def test_migrate_is_noop_when_external_db_exists(tmp_path):
    """Never clobber a live external DB with a stale legacy one (newer wins)."""
    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir, exist_ok=True)
    new_path = local_db_path_for(index_dir)
    with open(new_path, "w") as f:
        f.write("current")
    legacy = os.path.join(index_dir, "local_index.sqlite3")
    with open(legacy, "w") as f:
        f.write("stale")

    assert migrate_legacy_local_db(index_dir) == new_path
    with open(new_path) as f:
        assert f.read() == "current"          # untouched
    assert os.path.exists(legacy)             # legacy left in place


def test_migrate_fatal_on_move_failure_keeps_legacy(tmp_path, monkeypatch):
    """Codex P1: a move failure must raise (not strand the legacy DB silently)."""
    import shutil as _shutil
    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir, exist_ok=True)
    legacy = os.path.join(index_dir, "local_index.sqlite3")
    with open(legacy, "w") as f:
        f.write("legacy-data")

    def _boom(src, dst):
        raise OSError(13, "simulated move failure")

    monkeypatch.setattr(_shutil, "move", _boom)
    with pytest.raises(LocalIndexerError, match="migrate legacy LOCAL DB"):
        migrate_legacy_local_db(index_dir)

    # Legacy DB left intact for a retry; no empty external DB was created.
    assert os.path.exists(legacy)
    assert not os.path.exists(local_db_path_for(index_dir))


def test_reset_fatal_when_external_db_delete_fails(tmp_path, monkeypatch):
    """Codex P2: reset must abort if the old external DB can't be deleted."""
    index_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    db_path = str(tmp_path / "local_index.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    idx = _new_indexer(index_dir, lab_dir, db_path)
    try:
        real_remove = os.remove

        def _no_remove(path, *a, **k):
            if os.path.abspath(path).startswith(os.path.abspath(db_path)):
                raise OSError(13, "simulated delete failure")
            return real_remove(path, *a, **k)

        monkeypatch.setattr(os, "remove", _no_remove)
        with pytest.raises(LocalIndexerError, match="could not delete the LOCAL DB"):
            idx.reset_my_library(
                close_searcher_cb=lambda: None, reload_searcher_cb=lambda: None,
            )
    finally:
        idx.close()


def test_rebuild_raises_when_db_inside_index_dir(tmp_path):
    """The dir-swap guard fails loud if the DB is wired inside the swapped dir."""
    index_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    os.makedirs(index_dir, exist_ok=True)
    bad_db = os.path.join(index_dir, "local_index.sqlite3")  # INSIDE — the bug
    idx = _new_indexer(index_dir, lab_dir, bad_db)
    try:
        with pytest.raises(LocalIndexerError, match="must not live inside"):
            idx.rebuild_main_index_atomic(
                "run", close_searcher_cb=lambda: None, reload_searcher_cb=lambda: None,
            )
    finally:
        idx.close()


def test_run_deferred_tolerates_marker_already_current(tmp_path):
    """Race: another path rebuilt + rewrote the marker before we ran."""
    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()
    _force_marker_mismatch(index_dir)
    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        assert idx.needs_schema_rebuild is True
        # Simulate a concurrent rebuild restoring the current marker.
        _write_schema_marker(index_dir, _compute_schema_marker(build_local_schema))
        assert idx.run_deferred_schema_rebuild() is True
        assert idx.needs_schema_rebuild is False
        assert idx._index is not None
    finally:
        idx.close()
