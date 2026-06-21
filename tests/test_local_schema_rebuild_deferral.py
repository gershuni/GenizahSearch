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

from shared.local_indexer import (
    LocalIndexer,
    _compute_schema_marker,
    _read_schema_marker,
    _write_schema_marker,
    build_local_schema,
)


def _make_dirs(tmp_path):
    index_dir = str(tmp_path / "LocalIndex")
    lab_dir = str(tmp_path / "LocalLabIndex")
    db_path = os.path.join(index_dir, "local_index.sqlite3")
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
