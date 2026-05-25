# -*- coding: utf-8 -*-
"""Phase 97 U-02 — scan_run_id infrastructure: discard_run / keep_run + cache-hit lock.

Tests:
  T-E-2  test_discard_removes_all_four_row_sources — discard_run(run_B) removes
         only run_B from Tantivy + local_pages + local_files + processed_files
         while run_A rows survive.
  T-E-3  test_no_run_id_on_skipped — cache-hit skip must NOT overwrite scan_run_id
         (RESEARCH Issue #4 lock).
  T-E-extra  test_keep_run_commits_and_preserves_audit — keep_run marks
             scan_runs.status='completed'; SQL rows untouched.
  T-E-extra  test_discard_handles_uncommitted_writer_state — writer.rollback()
             is invoked (or fallback) when there are uncommitted docs.
"""
import os
import sqlite3
import time
import uuid

import tantivy

from shared.local_indexer import LocalIndexer, build_local_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_indexer(tmp_path):
    idx_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "db.sqlite3")
    os.makedirs(idx_dir)
    os.makedirs(lab_dir)
    return LocalIndexer(idx_dir, lab_dir, db_path)


def _add_doc_via_writer(indexer, run_id: str, uid: str, content: str) -> None:
    """Add a single doc to the indexer's own writer (no external writer opened)."""
    doc = tantivy.Document()
    doc.add_text("unique_id", uid)
    doc.add_text("scan_run_id", run_id)
    doc.add_text("content", content)
    for field_name in ("content_head", "content_tail", "line_starts", "line_ends"):
        try:
            doc.add_text(field_name, "")
        except Exception:
            pass
    for field_name in ("source", "full_header", "shelfmark", "scope", "boundaries", "chunk_locator"):
        try:
            doc.add_text(field_name, "")
        except Exception:
            pass
    indexer._writer.add_document(doc)


def _populate_sql_for_run(conn: sqlite3.Connection, run_id: str, sys_ids: list) -> None:
    """Insert test rows into processed_files + local_files + local_pages for given run."""
    for sys_id in sys_ids:
        fp = f"/fake/{sys_id}.txt"
        conn.execute(
            "INSERT OR REPLACE INTO processed_files "
            "(filepath, mtime, mtime_ns, size, sys_id, status, scan_run_id) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (fp, time.time(), 0, 100, sys_id, run_id),
        )
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(sys_id, filepath, folder_id, original_filename, file_extension, "
            " file_size_bytes, extraction_status, last_indexed_at) "
            "VALUES (?, ?, 0, 'fake.txt', '.txt', 100, 'ok', ?)",
            (sys_id, fp, time.time()),
        )
        conn.execute(
            "INSERT OR REPLACE INTO local_pages "
            "(uid, sys_id, page_num) "
            "VALUES (?, ?, 1)",
            (f"LOCAL_{sys_id}_P1", sys_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# T-E-2: discard_run removes only run_B sources; run_A survives
# ---------------------------------------------------------------------------

def test_discard_removes_all_four_row_sources(tmp_path):
    """LD-7 four-source delete: Tantivy + local_pages + local_files + processed_files.

    Setup:
      - 2 docs from run_A (committed to Tantivy + SQLite)
      - 3 docs from run_B (mid-run: in Tantivy + SQLite but not committed)

    After discard_run(run_B):
      a) Tantivy searcher finds only run_A docs (2 docs)
      b) processed_files has 0 rows for run_B
      c) local_files has 0 rows for run_B's sys_ids
      d) local_pages has 0 rows for run_B's sys_ids
      e) scan_runs row for run_B has status='discarded'
    """
    indexer = _make_indexer(tmp_path)
    try:
        # --- Populate run_A (committed to Tantivy via indexer's own writer) ---
        run_a = "run_id_A_" + uuid.uuid4().hex
        sys_ids_a = ["970000000011111111", "970000000022222222"]

        for sid in sys_ids_a:
            _add_doc_via_writer(indexer, run_a, f"LOCAL_{sid}_P1", f"content A {sid}")
        indexer._writer.commit()  # commit run_A docs

        # Insert scan_runs row for run_A (completed)
        indexer._conn.execute(
            "INSERT INTO scan_runs (scan_run_id, started_at, status) VALUES (?, ?, 'completed')",
            (run_a, time.time()),
        )
        # Populate SQL rows for run_A
        _populate_sql_for_run(indexer._conn, run_a, sys_ids_a)

        # --- Populate run_B (mid-run: written and committed to Tantivy + SQLite) ---
        run_b = "run_id_B_" + uuid.uuid4().hex
        sys_ids_b = ["970000000033333333", "970000000044444444", "970000000055555555"]

        for sid in sys_ids_b:
            _add_doc_via_writer(indexer, run_b, f"LOCAL_{sid}_P1", f"content B {sid}")
        indexer._writer.commit()  # commit run_B docs (simulating a mid-run batch commit)

        # Insert scan_runs row for run_B (still running)
        indexer._conn.execute(
            "INSERT INTO scan_runs (scan_run_id, started_at, status) VALUES (?, ?, 'running')",
            (run_b, time.time()),
        )
        # Populate SQL rows for run_B
        _populate_sql_for_run(indexer._conn, run_b, sys_ids_b)

        # Store run_b as current
        indexer._current_scan_run_id = run_b

        # --- Execute discard_run(run_B) ---
        counts = indexer.discard_run(run_b)

        # Assert (a): Tantivy has 0 run_B docs
        # Use the indexer's own index (writer was reopened by discard_run).
        # Use Index.parse_query (tantivy-py 0.25.1 API — no QueryParser class).
        schema = build_local_schema()
        reload_idx = tantivy.Index(schema, path=str(tmp_path / "idx"))
        searcher = reload_idx.searcher()
        query_b = reload_idx.parse_query(run_b, ["scan_run_id"])
        hits_b = searcher.search(query_b, 100).hits
        assert len(hits_b) == 0, (
            f"Expected 0 run_B Tantivy docs after discard, got {len(hits_b)}"
        )

        # Assert (b): processed_files has 0 rows for run_B
        pf_count = indexer._conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE scan_run_id = ?", (run_b,)
        ).fetchone()[0]
        assert pf_count == 0, f"Expected 0 processed_files for run_B, got {pf_count}"

        # Assert (c): local_files has 0 rows for run_B sys_ids
        for sid in sys_ids_b:
            lf_count = indexer._conn.execute(
                "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sid,)
            ).fetchone()[0]
            assert lf_count == 0, f"Expected 0 local_files for sys_id {sid}, got {lf_count}"

        # Assert (d): local_pages has 0 rows for run_B sys_ids
        for sid in sys_ids_b:
            lp_count = indexer._conn.execute(
                "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sid,)
            ).fetchone()[0]
            assert lp_count == 0, f"Expected 0 local_pages for sys_id {sid}, got {lp_count}"

        # Assert (e): scan_runs row for run_B is 'discarded'
        status = indexer._conn.execute(
            "SELECT status FROM scan_runs WHERE scan_run_id = ?", (run_b,)
        ).fetchone()
        assert status is not None, "scan_runs row for run_B not found"
        assert status[0] == "discarded", f"Expected status='discarded', got '{status[0]}'"

        # Bonus: run_A SQL rows must still be intact
        pf_a_count = indexer._conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE scan_run_id = ?", (run_a,)
        ).fetchone()[0]
        assert pf_a_count == len(sys_ids_a), (
            f"run_A processed_files must be intact: expected {len(sys_ids_a)}, got {pf_a_count}"
        )

    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# T-E-3: cache-hit skipped rows must NOT have scan_run_id overwritten
# ---------------------------------------------------------------------------

def test_no_run_id_on_skipped(tmp_path):
    """RESEARCH Issue #4 lock: scan_all cache-hit skip does NOT overwrite scan_run_id.

    Setup: processed_files row with scan_run_id='PRIOR' + known mtime_ns + committed.
    Action: scan_all (sees file as cache-hit, same mtime_ns).
    Assert: processed_files.scan_run_id is still 'PRIOR' (not the new run UUID).
    """
    indexer = _make_indexer(tmp_path)
    try:
        # Create a real file and index it first so processed_files has a committed row
        folder = str(tmp_path / "docs")
        os.makedirs(folder)
        fp = os.path.join(folder, "cached_doc.txt")
        with open(fp, "w", encoding="utf-8") as f:
            f.write("Hello world — this file is already indexed.")

        indexer.add_folder(folder)
        indexer.scan_all()  # indexes the file, sets scan_run_id = <run1>

        # Find the processed_files row and overwrite scan_run_id with 'PRIOR'
        prior_run_id = "PRIOR_RUN_ID_SENTINEL"
        # Get actual mtime_ns from the file on disk (same as what indexer stored)
        actual_mtime_ns = os.stat(fp).st_mtime_ns

        from shared.local_sys_id import _canonical_filepath
        canonical_fp = _canonical_filepath(fp)

        indexer._conn.execute(
            "UPDATE processed_files SET scan_run_id = ?, mtime_ns = ?, status = 'committed' "
            "WHERE filepath = ?",
            (prior_run_id, actual_mtime_ns, canonical_fp),
        )
        indexer._conn.commit()

        # Verify we set it correctly
        stored = indexer._conn.execute(
            "SELECT scan_run_id, mtime_ns FROM processed_files WHERE filepath = ?",
            (canonical_fp,),
        ).fetchone()
        assert stored is not None, "processed_files row not found after UPDATE"
        assert stored[0] == prior_run_id, f"Setup failed: expected PRIOR, got {stored[0]}"

        # Now run scan_all again — the file has same mtime_ns → should be a cache-hit skip
        indexer.scan_all()  # second scan

        # The scan_run_id must still be 'PRIOR' (not overwritten with new run UUID)
        row = indexer._conn.execute(
            "SELECT scan_run_id FROM processed_files WHERE filepath = ?",
            (canonical_fp,),
        ).fetchone()
        assert row is not None, "processed_files row gone after second scan"
        assert row[0] == prior_run_id, (
            f"RESEARCH Issue #4 VIOLATED: scan_run_id was overwritten on cache-hit skip. "
            f"Expected '{prior_run_id}', got '{row[0]}'. "
            "The cache-hit branch must NOT write scan_run_id."
        )

    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# T-E-extra: keep_run commits and preserves SQL rows
# ---------------------------------------------------------------------------

def test_keep_run_commits_and_preserves_audit(tmp_path):
    """keep_run(run_id) calls writer.commit() and marks scan_runs.status='completed'.

    SQL rows for the run must be preserved (not deleted).
    """
    indexer = _make_indexer(tmp_path)
    try:
        # Create a scan_runs row in 'running' state
        run_id = "keep_run_test_" + uuid.uuid4().hex
        indexer._conn.execute(
            "INSERT INTO scan_runs (scan_run_id, started_at, status) VALUES (?, ?, 'running')",
            (run_id, time.time()),
        )
        # Create a processed_files row for this run
        sys_id = "970000000099999999"
        indexer._conn.execute(
            "INSERT OR REPLACE INTO processed_files "
            "(filepath, mtime, mtime_ns, size, sys_id, status, scan_run_id) "
            "VALUES ('/fake/keep.txt', 1.0, 1000, 100, ?, 'pending', ?)",
            (sys_id, run_id),
        )
        indexer._conn.commit()

        indexer._current_scan_run_id = run_id

        # Call keep_run
        indexer.keep_run(run_id)

        # Assert: scan_runs.status = 'completed'
        status = indexer._conn.execute(
            "SELECT status FROM scan_runs WHERE scan_run_id = ?", (run_id,)
        ).fetchone()
        assert status is not None
        assert status[0] == "completed", f"Expected 'completed', got '{status[0]}'"

        # Assert: SQL rows are preserved
        pf_count = indexer._conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE scan_run_id = ?", (run_id,)
        ).fetchone()[0]
        assert pf_count == 1, f"processed_files should be preserved by keep_run, got {pf_count}"

    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# T-E-extra: discard handles uncommitted writer state
# ---------------------------------------------------------------------------

def test_discard_handles_uncommitted_writer_state(tmp_path):
    """discard_run invokes writer.rollback() (or fallback) on uncommitted docs.

    Adds docs to the writer WITHOUT committing, then calls discard_run.
    Asserts either:
      - counts['tantivy_rolled_back'] is True (rollback() succeeded), OR
      - the docs are nonetheless absent from Tantivy (commit-then-delete fallback worked).
    """
    indexer = _make_indexer(tmp_path)
    try:
        run_id = "uncommitted_test_" + uuid.uuid4().hex

        # Register the run in scan_runs
        indexer._conn.execute(
            "INSERT INTO scan_runs (scan_run_id, started_at, status) VALUES (?, ?, 'running')",
            (run_id, time.time()),
        )
        indexer._conn.commit()

        # Add some uncommitted docs via the indexer's writer (NO commit before discard)
        indexer._current_scan_run_id = run_id
        for i in range(3):
            sid = f"9700000000{i:08d}"
            _add_doc_via_writer(indexer, run_id, f"LOCAL_{sid}_P1", f"uncommitted content {i}")
        # Deliberately do NOT call writer.commit() here

        # Call discard_run (docs are in writer buffer, not committed)
        counts = indexer.discard_run(run_id)

        # Assert: either rollback was invoked OR fallback worked
        assert isinstance(counts, dict), "discard_run must return a dict of counts"
        rollback_attempted = counts.get("tantivy_rolled_back", False)

        # We accept either rollback=True OR the docs being absent (commit-then-delete)
        if not rollback_attempted:
            # Fallback path: commit-then-delete — verify docs are gone
            schema = build_local_schema()
            reload_idx = tantivy.Index(schema, path=str(tmp_path / "idx"))
            searcher = reload_idx.searcher()
            qp = tantivy.QueryParser.for_index(reload_idx, ["scan_run_id"])
            query = qp.parse_query(run_id)
            hits = searcher.search(query, 100).hits
            assert len(hits) == 0, (
                f"After discard_run (fallback path), expected 0 docs with run_id, got {len(hits)}"
            )

        # scan_runs.status must be 'discarded'
        status = indexer._conn.execute(
            "SELECT status FROM scan_runs WHERE scan_run_id = ?", (run_id,)
        ).fetchone()
        assert status is not None
        assert status[0] == "discarded", f"Expected 'discarded', got '{status[0]}'"

    finally:
        indexer.close()
