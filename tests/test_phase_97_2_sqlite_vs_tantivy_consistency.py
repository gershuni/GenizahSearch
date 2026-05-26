# -*- coding: utf-8 -*-
"""Phase 97.2 R97.2-H — discard_run short-circuits SQLite delete on Tantivy failure.

RED gate: when Tantivy delete_documents raises, the current code logs and proceeds
to SQLite deletion + scan_runs flip — leaving orphaned Tantivy docs without
matching SQLite rows. Post-fix: raise LocalIndexerError BEFORE the SQLite
transaction; SQLite rows + scan_runs.status preserved.

REVISED per REVIEWS.md Codex MEDIUM (readonly Rust extension): uses a wrapper-
seam `_FailingIndex` proxy instead of `monkeypatch.setattr` on the Rust Index
object, because newer tantivy-py versions may make `writer` a readonly
attribute. The proxy exposes a valid `.schema` so the post-Task-7 schema-
introspection / probe branch chooses the standard `delete_documents` path;
the proxy's writer then makes that call raise.

Post-Task-7 implementation note: the production code uses a PROBE-based
fallback (try delete_documents("scan_run_id", ...), catch ValueError whose
message contains "scan_run_id"). To trigger the OUTER `except Exception:`
branch (not the per-uid LocalIndexerError path), our _FailingWriter raises
a RuntimeError whose message does NOT contain "scan_run_id" — that way the
inner probe re-raises it, the outer Exception handler logs + sets
_tantivy_delete_ok = False, and the post-finally short-circuit raises
LocalIndexerError.
"""
import gc
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from shared.local_indexer import LocalIndexerError  # noqa: E402 (added in commit 4250c731)
from tests.test_scan_run_id import (  # noqa: E402
    _make_indexer,
    _add_doc_via_writer,
    _populate_sql_for_run,
)


class _FailingWriter:
    """Shim writer whose delete_documents always raises a generic RuntimeError
    (the message does NOT contain "scan_run_id" so the probe-based Phase 95
    fallback does NOT trigger — the outer except catches and sets
    _tantivy_delete_ok = False). REVIEWS Codex MEDIUM — wrapper-seam fallback
    for readonly Rust extension attributes.
    """

    def delete_documents(self, *a, **k):
        raise RuntimeError("simulated Tantivy delete failure (generic, not Phase 95)")

    def commit(self):
        pass

    def rollback(self):
        pass

    def wait_merging_threads(self):
        pass


class _FailingIndex:
    """Shim around a real tantivy.Index. Exposes the real .schema (in case the
    implementation grows a schema-introspection branch later), but returns a
    _FailingWriter from .writer() so discard_run's delete_documents call
    raises into the outer Exception handler.
    """

    def __init__(self, real):
        self._real = real

    @property
    def schema(self):
        return self._real.schema

    def writer(self, **kwargs):
        return _FailingWriter()

    def searcher(self):
        return self._real.searcher()

    def reload(self):
        if hasattr(self._real, "reload"):
            return self._real.reload()


def test_discard_run_short_circuits_sqlite_on_tantivy_failure(tmp_path):
    """RED before R97.2-H; GREEN after.

    Force Tantivy delete to fail (generic — not the Phase 95 signal). Assert
    LocalIndexerError raised AND SQLite rows preserved AND scan_runs.status
    NOT flipped to 'discarded'.
    """
    indexer = _make_indexer(tmp_path)
    try:
        run_b = "run_b_xxx"
        sys_ids = ["970000000099999991", "970000000099999992"]
        for i, sid in enumerate(sys_ids):
            _add_doc_via_writer(indexer, run_b, f"LOCAL_{sid}_P1", f"content {i}")
        indexer._writer.commit()
        _populate_sql_for_run(indexer._conn, run_b, sys_ids)
        indexer._conn.execute(
            "UPDATE processed_files SET status='committed' WHERE scan_run_id=?",
            (run_b,),
        )
        indexer._conn.commit()

        # Close the real writer so the shim can own the lock cleanly.
        indexer._close_internal_writer_index()
        gc.collect()

        # Re-open through _FailingIndex wrapper (REVIEWS Codex MEDIUM —
        # wrapper-seam, not setattr on Rust extension). Leave indexer._writer
        # = None so discard_run step 2 is the first writer acquisition.
        import tantivy
        from shared.local_indexer import build_local_schema
        real_index = tantivy.Index(build_local_schema(), path=indexer._index_dir)
        indexer._index = _FailingIndex(real_index)
        # NOTE: do NOT pre-acquire indexer._writer — leave it None so
        # discard_run's step 1 is skipped and step 2 calls _index.writer()
        # which returns the _FailingWriter that raises on delete_documents.

        with pytest.raises(LocalIndexerError):
            indexer.discard_run(run_b)

        # SQLite rows must STILL exist (the short-circuit prevented step 3)
        pf_count = indexer._conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE scan_run_id=?",
            (run_b,),
        ).fetchone()[0]
        assert pf_count > 0, "SQLite rows must NOT be deleted on Tantivy failure"

        # scan_runs row must NOT be 'discarded'
        row = indexer._conn.execute(
            "SELECT status FROM scan_runs WHERE scan_run_id=?",
            (run_b,),
        ).fetchone()
        if row is not None:
            assert row[0] != "discarded", (
                "scan_runs.status must NOT be flipped on Tantivy failure"
            )
    finally:
        try:
            indexer._close_internal_writer_index()
        except Exception:
            pass
