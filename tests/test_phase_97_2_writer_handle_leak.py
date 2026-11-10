# -*- coding: utf-8 -*-
"""Phase 97.2 R97.2-A + R97.2-B — rebuild_main_index_atomic must NOT leak writer lock.

REVISED per REVIEWS.md Codex HIGH #1: original test asserted only
`_writer is not None` which passes for the wrong reason. New version forces a
contended-writer scenario by explicitly closing _writer and re-acquiring a fresh
writer — this is the ONLY check that proves the rebuild released the lock.
Plus a static AST/substring scan that the deprecated `del fresh_writer` /
`del fresh_index` patterns are gone from rebuild_main_index_atomic.
"""
import gc
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_scan_run_id import (  # noqa: E402
    _make_indexer,
    _add_doc_via_writer,
    _populate_sql_for_run,
)


def test_rebuild_does_not_leak_writer_lock(tmp_path):
    """RED before R97.2-A + R97.2-B; GREEN after (deterministic contended check)."""
    indexer = _make_indexer(tmp_path)
    try:
        _add_doc_via_writer(indexer, "run_x", "LOCAL_X_P1", "hello")
        indexer._writer.commit()
        _populate_sql_for_run(indexer._conn, "run_x", ["970000000099999999"])
        indexer._conn.execute(
            "UPDATE processed_files SET status='committed' WHERE scan_run_id=?",
            ("run_x",),
        )
        # Populate cached_text so rebuild_main_index_atomic can write a doc
        # without needing to re-extract from source (the synthetic /fake path
        # doesn't exist on disk). Use cached_text_codec='plain' so the bytes
        # are decoded as UTF-8 (NOT decompressed via zstd). The uid in SQLite
        # is "LOCAL_<sys_id>_P1" per _populate_sql_for_run, NOT "LOCAL_X_P1"
        # which was the Tantivy-side uid we wrote via _add_doc_via_writer.
        indexer._conn.execute(
            "UPDATE local_pages SET cached_text=?, cached_text_codec='plain' "
            "WHERE sys_id=?",
            (b"hello", "970000000099999999"),
        )
        indexer._conn.commit()

        indexer.rebuild_main_index_atomic(
            "rebuild_run", lambda: None, lambda: None,
        )

        # Step 1: close the indexer-owned writer so it does not hold the lock.
        indexer._close_internal_writer_index()
        gc.collect()

        # Step 2: REOPEN the index (since _close_internal_writer_index sets
        # self._index = None) and attempt a FRESH writer acquisition. If the
        # rebuild leaked a Rust-side writer handle, this call raises LockBusy.
        import tantivy
        from shared.local_indexer import build_local_schema
        fresh_index = tantivy.Index(build_local_schema(), path=indexer._index_dir)
        fresh_writer = fresh_index.writer(heap_size=15_000_000)  # MUST NOT raise LockBusy

        # Step 3: assert fresh acquisition succeeded (no LockBusy was raised)
        assert fresh_writer is not None, (
            "fresh writer acquisition must succeed after rebuild (proves lock released)"
        )

        # Cleanup our test-owned writer
        fresh_writer = None
        fresh_index = None
        gc.collect()
    finally:
        # Already closed above, but guard against double-close
        try:
            indexer._close_internal_writer_index()
        except Exception:
            pass


def test_rebuild_main_index_atomic_has_no_del_fresh_handles():
    """Static AST/substring guard (REVIEWS.md Codex HIGH #1): the deprecated
    `del fresh_writer` / `del fresh_index` patterns are absent from
    shared/local_indexer.py. Mirrors the Phase 87 static AST guard pattern in
    `tests/test_no_raw_storage_access.py`.
    """
    import shared.local_indexer as li_module
    src_path = li_module.__file__
    with open(src_path, "r", encoding="utf-8") as fh:
        source = fh.read()
    # Substring check is sufficient — these patterns only ever appeared in
    # the rebuild_main_index_atomic swap region per CONTEXT.md §canonical_refs.
    assert "del fresh_writer" not in source, (
        "del fresh_writer must be removed (R97.2-B uses null + gc.collect instead)"
    )
    assert "del fresh_index" not in source, (
        "del fresh_index must be removed (R97.2-B uses null + gc.collect instead)"
    )
