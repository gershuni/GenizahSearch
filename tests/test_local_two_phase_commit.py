# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-21: two-phase commit protocol for Tantivy + SQLite atomicity.

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-03).
"""


def test_crash_between_tantivy_and_sqlite_recovers():
    """D-21 Codex P1: simulate crash between Tantivy writer.commit() and SQLite
    UPDATE (status='committed'). On app-restart, pending rows are re-extracted
    (idempotent via delete-by-uid + re-insert). No doubled or missing docs.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-21 two-phase commit recovery — implemented in Wave 1 plan 95-03"
    )
