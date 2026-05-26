# -*- coding: utf-8 -*-
"""Phase 97.2 R97.2-C + R97.2-G — discard_run writer lifecycle.

(a) On Phase 95 schema (no scan_run_id field) discard_run falls back to
    per-uid deletion via SELECT uid FROM local_pages JOIN processed_files.
    PATTERNS.md authorizes a try/except ValueError PROBE pattern because
    tantivy-py 0.25.1's Schema does NOT expose field_names() —
    delete_documents("scan_run_id", run_id) raises ValueError("Field
    'scan_run_id' is not defined") on Phase 95, and the implementation
    catches that to trigger the per-uid loop.
(b) After step 2 commit/except, _del_writer is explicitly nulled + gc.collect()
    before step 5 reopens self._writer.

REVISED per REVIEWS.md Codex HIGH #2 + MEDIUM: the Phase 95 fallback test must
exercise the probe-fallback branch deterministically AND must spy on the
per-uid loop to confirm it executes (originally drafted as schema-introspection
but switched to probe-based when tantivy-py 0.25.1's Schema was confirmed to
lack field_names()).
"""
import gc
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: F401
from tests.test_scan_run_id import (  # noqa: E402
    _make_indexer,
    _add_doc_via_writer,
    _populate_sql_for_run,
)


# ----- Wrapper-seam shims (REVIEWS Codex MEDIUM — readonly Rust extension) -----

class _DelWriterShim:
    """Proxy around a real IndexWriter. Simulates the Phase 95 schema signal —
    delete_documents("scan_run_id", ...) raises a ValueError that contains
    "scan_run_id" in its message (matching the actual tantivy "Field
    'scan_run_id' is not defined" error). All other field deletes are
    delegated to the real writer (and counted for the per-uid spy).

    NOTE (post-Task-7 implementation reality): tantivy-py 0.25.1's
    tantivy.Schema does NOT expose field_names() / get_field_names(), so the
    Phase 95 schema-detection in discard_run uses the PATTERNS.md-authorized
    try/except ValueError PROBE pattern (try delete_documents on the field
    name first; if it raises with "scan_run_id" in the message, fall back to
    per-uid). This shim makes the probe deterministic: scan_run_id → raise,
    unique_id → delegate-and-count.
    """

    def __init__(self, real):
        self._real = real
        self.delete_calls = []  # list of (field, value) tuples

    def delete_documents(self, field, value):
        self.delete_calls.append((field, value))
        if field == "scan_run_id":
            # Simulate Phase 95 schema signal — matches PATTERNS.md fallback
            # contract (any ValueError whose message contains "scan_run_id"
            # triggers the per-uid path).
            raise ValueError("Field 'scan_run_id' is not defined")
        return self._real.delete_documents(field, value)

    def commit(self):
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()

    def wait_merging_threads(self):
        return self._real.wait_merging_threads()


class _IndexShim:
    """Proxy around a real Index. Returns a _DelWriterShim from .writer() so
    that the post-Task-7 probe-based Phase-95 fallback in discard_run
    deterministically takes the per-uid branch.

    REVIEWS Rev-2 HIGH #2: discard_run's step-5 reopen calls .writer() AGAIN
    after step-2's per-uid loop. A naive `self.last_del_writer = ...` would
    OVERWRITE the step-2 writer's delete_calls log with the step-5 (empty)
    writer, making the per-uid spy assertion read the wrong writer.

    Fix: accumulate ALL spawned writers in `all_writers` and expose
    `all_delete_calls` which aggregates delete_calls across every writer
    ever returned by this shim. The step-2 calls survive step-5 reopen.
    """

    def __init__(self, real):
        self._real = real
        self.all_writers = []  # every _DelWriterShim ever returned (REVIEWS Rev-2 HIGH #2)
        self.last_del_writer = None  # most recent; kept for back-compat only — DO NOT use for spy

    @property
    def schema(self):
        # Defer to the real schema. The probe-based fallback in discard_run
        # does NOT call schema.field_names() (which tantivy-py 0.25.1 lacks);
        # it relies on _DelWriterShim raising a ValueError instead.
        return self._real.schema

    def writer(self, **kwargs):
        real_writer = self._real.writer(**kwargs)
        shim_writer = _DelWriterShim(real_writer)
        self.all_writers.append(shim_writer)  # aggregate (REVIEWS Rev-2 HIGH #2)
        self.last_del_writer = shim_writer
        return shim_writer

    @property
    def all_delete_calls(self):
        """Aggregate delete_calls across every writer this shim has returned.
        Use this — NOT last_del_writer.delete_calls — for the per-uid spy
        assertion, because discard_run step 5 reopens the writer.
        """
        calls = []
        for w in self.all_writers:
            calls.extend(w.delete_calls)
        return calls

    def searcher(self):
        return self._real.searcher()

    def reload(self):
        if hasattr(self._real, "reload"):
            return self._real.reload()


# ----- Tests -----

def test_discard_run_fallback_on_phase_95_schema(tmp_path):
    """Case (a) — REVIEWS.md Codex HIGH #2: deterministic Phase 95 fallback exercised.

    Patches the index's .writer() to return a _DelWriterShim whose
    delete_documents("scan_run_id", ...) raises a ValueError matching the
    actual tantivy "Field 'scan_run_id' is not defined" signal. This forces
    the post-Task-7 probe-based fallback (PATTERNS.md try/except ValueError
    path — used because tantivy-py 0.25.1's tantivy.Schema lacks
    field_names() / get_field_names()) to enter the per-uid loop
    deterministically. Spies on the per-uid loop via all_delete_calls to
    confirm it executed.
    """
    indexer = _make_indexer(tmp_path)
    try:
        # Populate run_b with 3 docs + matching SQL rows
        sys_ids = ["970000000099999991", "970000000099999992", "970000000099999993"]
        for i, sid in enumerate(sys_ids):
            _add_doc_via_writer(indexer, "run_b", f"LOCAL_{sid}_P1", f"content {i}")
        indexer._writer.commit()
        _populate_sql_for_run(indexer._conn, "run_b", sys_ids)
        indexer._conn.execute(
            "UPDATE processed_files SET status='committed' WHERE scan_run_id=?",
            ("run_b",),
        )
        indexer._conn.commit()

        # Close the real writer so the shim can own the lock cleanly.
        indexer._close_internal_writer_index()
        gc.collect()

        # Re-open via shim so .writer() returns a _DelWriterShim that simulates
        # the Phase 95 "scan_run_id field not defined" signal. Leave
        # indexer._writer = None so discard_run's step 2 is the FIRST writer
        # acquisition on this shim (otherwise pre-acquiring here would hold
        # the lock and discard_run's step-2 writer() call would LockBusy).
        import tantivy
        from shared.local_indexer import build_local_schema
        real_index = tantivy.Index(build_local_schema(), path=indexer._index_dir)
        shim = _IndexShim(real_index)
        indexer._index = shim
        # NOTE: discard_run step 1 calls self._writer.rollback() iff
        # self._writer is not None. Leaving it None skips that path.

        # Call discard_run — POST-FIX (R97.2-C): try delete_documents on
        # "scan_run_id" -> _DelWriterShim raises ValueError("Field
        # 'scan_run_id' is not defined") -> probe-based fallback enters the
        # per-uid loop.
        indexer.discard_run("run_b")

        # Per-uid spy assertion (REVIEWS Codex HIGH #2 + Rev-2 HIGH #2):
        # the fallback loop must have called delete_documents("unique_id", uid)
        # at least once. Use shim.all_delete_calls (aggregated across all
        # writers) NOT shim.last_del_writer.delete_calls — because
        # discard_run step 5 reopens the writer and would overwrite the
        # step-2 spy (Rev-2 HIGH #2).
        per_uid_calls = [
            c for c in shim.all_delete_calls if c[0] == "unique_id"
        ]
        assert len(per_uid_calls) > 0, (
            f"per-uid fallback loop did not execute "
            f"(all_delete_calls={shim.all_delete_calls})"
        )

        # Assert SQLite cleanup happened
        pf_count = indexer._conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE scan_run_id=?",
            ("run_b",),
        ).fetchone()[0]
        assert pf_count == 0, "processed_files rows must be cleared after discard"
    finally:
        try:
            indexer._close_internal_writer_index()
        except Exception:
            pass


def test_discard_run_closes_del_writer_before_reopen(tmp_path):
    """Case (b): _del_writer is None + gc.collect() before step 5 reopen."""
    indexer = _make_indexer(tmp_path)
    try:
        sys_ids = ["970000000099999991", "970000000099999992"]
        for i, sid in enumerate(sys_ids):
            _add_doc_via_writer(indexer, "run_b", f"LOCAL_{sid}_P1", f"content {i}")
        indexer._writer.commit()
        _populate_sql_for_run(indexer._conn, "run_b", sys_ids)
        indexer._conn.execute(
            "UPDATE processed_files SET status='committed' WHERE scan_run_id=?",
            ("run_b",),
        )
        indexer._conn.commit()

        indexer.discard_run("run_b")

        # POST-FIX (R97.2-G): step 5 reopened self._writer successfully because
        # _del_writer was explicitly nulled + gc.collect()-ed between step 2 and
        # step 5. Pre-fix, step 5 logged LockBusy via except and self._writer
        # would be None (or stale).
        assert indexer._writer is not None, (
            "step 5 must successfully reopen self._writer (R97.2-G)"
        )
    finally:
        indexer._close_internal_writer_index()
