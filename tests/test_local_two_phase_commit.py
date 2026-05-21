# -*- coding: utf-8 -*-
"""Phase 95 D-21 + HIGH-3: two-phase commit protocol + crash-safe delete recovery.

Tests:
- test_crash_between_tantivy_and_sqlite_recovers: D-21 fault injection
- test_crash_between_pending_delete_and_tantivy_commit_recovers: HIGH-3 new test
- test_crash_between_tantivy_commit_and_sqlite_final_delete_recovers: HIGH-3 new test
"""
import os
import sqlite3

import tantivy

from shared.local_indexer import LocalIndexer, build_local_schema


def _make_indexer_with_file(tmp_path):
    """Helper: create an indexer, add one folder, index one file.

    Returns (indexer, folder, filepath, db_path, index_dir, lab_dir).
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    filepath = os.path.join(folder, "doc.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("Two-phase commit test document content. שלום עולם.")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    indexer.add_folder(folder)
    return indexer, folder, filepath, db_path, index_dir, lab_dir


def test_crash_between_tantivy_and_sqlite_recovers(tmp_path):
    """D-21 Codex P1: simulate crash between Tantivy writer.commit() and SQLite
    UPDATE (status='committed'). On app-restart, startup_recovery() re-extracts
    the pending files. No doubled or missing docs.

    Fault injection: after a successful scan+commit, we directly flip the
    processed_files.status back to 'pending' to simulate the state that would
    exist if the process crashed after Tantivy commit but before SQLite UPDATE.
    This is the canonical crash state for D-21 recovery to handle.
    """
    indexer, folder, filepath, db_path, index_dir, lab_dir = _make_indexer_with_file(tmp_path)

    # Perform a normal scan (Tantivy docs ARE committed)
    indexer.scan_all()
    indexer.close()

    # Verify 'committed' rows exist after normal scan
    conn = sqlite3.connect(db_path)
    committed_before = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE status = 'committed'"
    ).fetchone()[0]
    assert committed_before >= 1, f"Expected >= 1 committed row after scan, got {committed_before}"

    # FAULT INJECTION: flip status back to 'pending' to simulate the crash state
    # (Tantivy docs exist on disk but SQLite update never happened)
    conn.execute("UPDATE processed_files SET status = 'pending'")
    conn.commit()
    conn.close()

    # Verify fault injection worked
    conn2 = sqlite3.connect(db_path)
    pending = conn2.execute(
        "SELECT COUNT(*) FROM processed_files WHERE status = 'pending'"
    ).fetchone()[0]
    conn2.close()
    assert pending >= 1, f"Expected >= 1 pending row after fault injection, got {pending}"

    # Restart indexer and call startup_recovery()
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        recovery_result = indexer2.startup_recovery()
    finally:
        indexer2.close()

    # After recovery, pending rows should be committed (or re-extracted)
    conn3 = sqlite3.connect(db_path)
    still_pending = conn3.execute(
        "SELECT COUNT(*) FROM processed_files WHERE status = 'pending'"
    ).fetchone()[0]
    total_rows = conn3.execute(
        "SELECT COUNT(*) FROM processed_files"
    ).fetchone()[0]
    conn3.close()

    assert still_pending == 0, f"Expected 0 pending after recovery, got {still_pending}"
    assert total_rows >= 1, f"Expected >= 1 processed_files row after recovery, got {total_rows}"


def test_crash_between_pending_delete_and_tantivy_commit_recovers(tmp_path):
    """HIGH-3 review fix — NEW test: simulate crash after step 1 (pending_delete=1
    marked in SQLite) but BEFORE step 2 (Tantivy delete).

    Recovery at next startup: _recover_pending_deletes() must:
    (a) issue Tantivy delete + commit for the pending sys_id's UIDs
    (b) remove local_files / local_pages / processed_files rows for that sys_id
    """
    indexer, folder, filepath, db_path, index_dir, lab_dir = _make_indexer_with_file(tmp_path)

    # Index the file first
    indexer.scan_all()
    indexer.close()

    # Get the sys_id that was indexed
    conn = sqlite3.connect(db_path)
    lf_row = conn.execute("SELECT sys_id FROM local_files").fetchone()
    assert lf_row is not None, "Expected a local_files row after indexing"
    sys_id = lf_row[0]

    # Verify pages exist in local_pages
    page_count = conn.execute(
        "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    assert page_count >= 1, f"Expected >= 1 local_pages row, got {page_count}"

    # Simulate STEP 1 ONLY: mark pending_delete=1 (as if crash happened immediately after)
    conn.execute(
        "UPDATE local_files SET pending_delete = 1 WHERE sys_id = ?", (sys_id,)
    )
    conn.commit()
    conn.close()

    # Do NOT proceed to step 2 (Tantivy delete) — simulate crash here

    # Reopen LocalIndexer — __init__ calls _recover_pending_deletes()
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    indexer2.close()

    # (a) Searcher should return ZERO hits for that sys_id's UIDs
    schema = build_local_schema()
    idx = tantivy.Index(schema, path=index_dir)
    searcher = idx.searcher()
    assert searcher.num_docs == 0, (
        f"Expected 0 Tantivy docs after recovery, got {searcher.num_docs}"
    )

    # (b) local_files / local_pages / processed_files rows should be GONE
    conn3 = sqlite3.connect(db_path)
    lf_after = conn3.execute(
        "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    lp_after = conn3.execute(
        "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    pf_after = conn3.execute(
        "SELECT COUNT(*) FROM processed_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    conn3.close()

    assert lf_after == 0, f"Expected 0 local_files rows after recovery, got {lf_after}"
    assert lp_after == 0, f"Expected 0 local_pages rows after recovery, got {lp_after}"
    assert pf_after == 0, f"Expected 0 processed_files rows after recovery, got {pf_after}"


def test_crash_between_tantivy_commit_and_sqlite_final_delete_recovers(tmp_path):
    """HIGH-3 review fix — NEW test: simulate crash after step 1 (pending_delete=1)
    AND step 2 (Tantivy delete committed), but BEFORE step 3 (SQLite final cleanup).

    Recovery at next startup: _recover_pending_deletes() re-runs step 2 (idempotent —
    Tantivy ignores already-deleted UIDs) and completes step 3.
    Final state: no Tantivy hits, no SQLite rows.
    """
    indexer, folder, filepath, db_path, index_dir, lab_dir = _make_indexer_with_file(tmp_path)

    # Index the file first
    indexer.scan_all()
    indexer.close()

    # Get the sys_id and UIDs
    conn = sqlite3.connect(db_path)
    lf_row = conn.execute("SELECT sys_id FROM local_files").fetchone()
    assert lf_row is not None, "Expected a local_files row after indexing"
    sys_id = lf_row[0]

    uid_rows = conn.execute(
        "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchall()
    uids = [r[0] for r in uid_rows]
    assert len(uids) >= 1, "Expected at least 1 UID in local_pages"

    # Simulate STEPS 1+2: mark pending_delete=1 AND do Tantivy delete + commit
    conn.execute(
        "UPDATE local_files SET pending_delete = 1 WHERE sys_id = ?", (sys_id,)
    )
    conn.commit()
    conn.close()

    # Do Tantivy delete manually (simulating step 2 completed)
    schema = build_local_schema()
    idx = tantivy.Index(schema, path=index_dir)
    writer = idx.writer(heap_size=15_000_000)
    for uid in uids:
        writer.delete_documents("unique_id", uid)
    writer.commit()
    del writer
    del idx

    # Verify Tantivy docs are already gone (step 2 done)
    idx_check = tantivy.Index(schema, path=index_dir)
    assert idx_check.searcher().num_docs == 0, "Expected 0 docs after manual step 2"
    del idx_check

    # SQLite rows still exist (step 3 NOT done — simulating crash)
    conn2 = sqlite3.connect(db_path)
    lf_count = conn2.execute(
        "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    assert lf_count == 1, f"Expected local_files row still present (step 3 not done), got {lf_count}"
    conn2.close()

    # Reopen LocalIndexer — _recover_pending_deletes() should run step 2 (idempotent) + step 3
    indexer3 = LocalIndexer(index_dir, lab_dir, db_path)
    indexer3.close()

    # Final state: no Tantivy docs
    idx_final = tantivy.Index(schema, path=index_dir)
    assert idx_final.searcher().num_docs == 0, (
        f"Expected 0 Tantivy docs after full recovery, got {idx_final.searcher().num_docs}"
    )

    # Final state: no SQLite rows
    conn4 = sqlite3.connect(db_path)
    lf_final = conn4.execute(
        "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    lp_final = conn4.execute(
        "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    conn4.close()

    assert lf_final == 0, f"Expected 0 local_files rows, got {lf_final}"
    assert lp_final == 0, f"Expected 0 local_pages rows, got {lp_final}"
