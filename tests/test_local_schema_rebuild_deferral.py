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


def test_reset_db_quarantine_failure_rolls_back_live_dirs(tmp_path, monkeypatch):
    """Codex REQUEST CHANGES: a DB-quarantine failure must roll back the LIVE
    LocalIndex (restored with its marker), not leave it recreated-empty next to
    a stale DB."""
    index_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    db_path = str(tmp_path / "local_index.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    idx = _new_indexer(index_dir, lab_dir, db_path)
    try:
        # Marker inside the live LocalIndex — must survive the rollback.
        marker = os.path.join(index_dir, ".reset-rollback-marker")
        with open(marker, "w") as f:
            f.write("must survive DB-quarantine rollback")
        assert os.path.exists(db_path)  # __init__ created the DB

        # Fail the DB rename only; dir renames (basename LocalIndex/LocalLabIndex)
        # succeed, including the rollback that restores them.
        real_rename = idx._retry_windows_rename

        def _wrapped(src, dst):
            if os.path.basename(src).startswith("local_index.sqlite3"):
                raise OSError(13, "simulated DB quarantine failure")
            return real_rename(src, dst)

        monkeypatch.setattr(idx, "_retry_windows_rename", _wrapped)

        with pytest.raises(LocalIndexerError, match="could not move the LOCAL DB aside"):
            idx.reset_my_library(
                close_searcher_cb=lambda: None, reload_searcher_cb=lambda: None,
            )

        # Live state fully restored: LocalIndex back with its marker (NOT empty),
        # DB still at its original path, nothing left in a quarantine.
        assert os.path.isfile(marker), "LOCAL dir was not rolled back (marker gone)"
        assert os.path.exists(db_path), "DB should remain at its original path"
        leftover = [p for p in os.listdir(tmp_path) if "reset-quarantine" in p]
        assert leftover == [], f"quarantines should have been rolled back, found {leftover}"
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


# ---------------------------------------------------------------------------
# SEED-006 regression — a REAL on-disk schema mismatch (old index lacks the
# content_search field) must rebuild from cached_text, not crash. The other
# tests force a MARKER-only mismatch, which leaves the on-disk schema identical
# to build_local_schema() — so the buggy seed `tantivy.Index(new_schema,
# path=index_dir)` succeeded there and never reproduced the production failure
# ("Schema error: 'An index exists but the schema does not match.'").
# ---------------------------------------------------------------------------

def _build_old_schema_index(index_dir):
    """Replace the on-disk index with a genuine pre-SEED-006 schema: `content`
    tokenized whitespace and NO `content_search` field, + a stale marker."""
    import gc as _gc
    import shutil as _sh

    _sh.rmtree(index_dir, ignore_errors=True)
    os.makedirs(index_dir, exist_ok=True)
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True, tokenizer_name="raw")
    b.add_text_field("content", stored=True, tokenizer_name="whitespace")  # OLD: no hebword, no content_search
    b.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
    b.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
    b.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
    b.add_text_field("source", stored=True)
    b.add_text_field("full_header", stored=True)
    b.add_text_field("shelfmark", stored=True)
    b.add_text_field("scope", stored=True)
    b.add_text_field("boundaries", stored=True)
    b.add_text_field("scan_run_id", stored=True, tokenizer_name="raw")
    b.add_text_field("chunk_locator", stored=True, tokenizer_name="raw")
    old = tantivy.Index(b.build(), path=index_dir)  # creates meta.json with the OLD schema
    old = None  # drop the handle before any rename (Windows lock)
    _gc.collect()
    _write_schema_marker(index_dir, "0000oldschema0000")


def _seed_committed_page(db_path, text):
    """Seed one committed page with cached_text so the rebuild has a doc to carry."""
    import sqlite3
    c = sqlite3.connect(db_path)
    try:
        c.execute("INSERT INTO folders (folder_id, path, added_at) VALUES (1, '/fake/folder', 0)")
        c.execute(
            "INSERT INTO local_files (sys_id, filepath, folder_id, original_filename, "
            "file_extension, page_count, file_size_bytes, extraction_status, last_indexed_at) "
            "VALUES ('sysX', '/fake/folder/doc.txt', 1, 'doc.txt', '.txt', 1, 10, 'ok', 0)"
        )
        c.execute(
            "INSERT INTO processed_files (filepath, mtime, size, sys_id, status, scan_run_id, mtime_ns) "
            "VALUES ('/fake/folder/doc.txt', 0, 10, 'sysX', 'committed', 'run1', 0)"
        )
        c.execute(
            "INSERT INTO local_pages (sys_id, uid, page_num, cached_text, cached_text_codec, "
            "extraction_format_version, chunk_locator) VALUES ('sysX', 'uidX', 1, ?, 'raw', 3, '')",
            (text.encode("utf-8"),),
        )
        c.commit()
    finally:
        c.close()


def test_real_schema_mismatch_rebuilds_inline_and_preserves_docs(tmp_path):
    """Default (inline) path: old-schema on-disk index + committed cached_text →
    migrate without raising, new index has content_search, doc preserved."""
    from genizah_core import _index_has_field

    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()       # create SQLite tables
    _seed_committed_page(db_path, "מצותה בסגן, ועוד בסגן כאן")
    _build_old_schema_index(index_dir)                       # genuine schema mismatch

    idx = _new_indexer(index_dir, lab_dir, db_path)          # default: inline rebuild (line 1882 site)
    try:
        assert idx.needs_schema_rebuild is False
        assert idx._index is not None
        assert _index_has_field(idx._index, "content_search")
        assert _read_schema_marker(index_dir) == _compute_schema_marker(build_local_schema)
        assert idx._index.searcher().num_docs == 1           # cached_text doc carried across migration
    finally:
        idx.close()


def test_real_schema_mismatch_rebuilds_deferred_and_preserves_docs(tmp_path):
    """Deferred path (run_deferred_schema_rebuild, line 1978 site): same real
    schema mismatch must rebuild from cached_text once the deferred call runs."""
    from genizah_core import _index_has_field

    index_dir, lab_dir, db_path = _make_dirs(tmp_path)
    _new_indexer(index_dir, lab_dir, db_path).close()
    _seed_committed_page(db_path, "מצותה בסגן, ועוד בסגן כאן")
    _build_old_schema_index(index_dir)

    idx = _new_indexer(index_dir, lab_dir, db_path, defer_schema_rebuild=True)
    try:
        assert idx.needs_schema_rebuild is True
        assert idx._index is None
        assert idx.run_deferred_schema_rebuild() is True     # must NOT raise on the real mismatch
        assert idx.needs_schema_rebuild is False
        assert _index_has_field(idx._index, "content_search")
        assert idx._index.searcher().num_docs == 1
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
