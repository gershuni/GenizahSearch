# -*- coding: utf-8 -*-
"""Phase 95 D-20: delete Tantivy docs by uid (via local_pages sidecar).

Tests that Tantivy delete_documents works correctly with raw tokenizer on unique_id.
Without tokenizer_name='raw', delete_documents silently fails (Pitfall #2 / tantivy-py #297).
"""
import os
import sqlite3

import tantivy

from shared.local_indexer import (
    LocalIndexer,
    build_local_schema,
)


def test_delete_by_uid_with_raw_tokenizer(tmp_path):
    """D-20 + Pitfall #2: Tantivy unique_id with tokenizer_name='raw' allows
    delete_documents(Term('unique_id', uid)) to correctly remove a doc.

    Without raw tokenizer the delete silently fails because the default tokenizer
    splits 'LOCAL_970012345601234567_P1' into multiple tokens, none of which
    match the full UID — tantivy-py issue #297.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    # Create a folder with one text file
    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    filepath = os.path.join(folder, "test_doc.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("Test content for delete-by-uid verification. שלום עולם.")

    # Index the file
    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    # Verify the file was indexed: check local_pages for the uid
    conn = sqlite3.connect(db_path)
    pages = conn.execute("SELECT uid, sys_id FROM local_pages").fetchall()
    conn.close()

    assert len(pages) >= 1, f"Expected at least 1 local_pages row, got {len(pages)}"
    uid = pages[0][0]
    sys_id = pages[0][1]

    # Confirm uid format matches D-34 pattern: LOCAL_{sys_id}_P{page_num}
    assert uid.startswith("LOCAL_"), f"UID should start with 'LOCAL_', got: {uid}"
    assert f"_{sys_id}_" in uid, f"UID should contain sys_id, got: {uid}"

    # Reopen the index and verify the doc is searchable
    schema = build_local_schema()
    idx = tantivy.Index(schema, path=index_dir)
    searcher = idx.searcher()
    total_before = searcher.num_docs

    assert total_before >= 1, f"Expected >= 1 doc before delete, got {total_before}"

    # Now use LocalIndexer._delete_file to delete via the uid
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer2._delete_file(sys_id, filepath)
    finally:
        indexer2.close()

    # Reopen and verify doc is gone
    idx2 = tantivy.Index(schema, path=index_dir)
    searcher2 = idx2.searcher()
    total_after = searcher2.num_docs

    assert total_after == 0, (
        f"Expected 0 docs after delete, got {total_after}. "
        f"This would indicate the raw tokenizer is NOT working correctly "
        f"(Pitfall #2 / tantivy-py #297)."
    )

    # Also verify local_pages + local_files rows are gone
    conn2 = sqlite3.connect(db_path)
    pages_after = conn2.execute(
        "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    files_after = conn2.execute(
        "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()[0]
    conn2.close()

    assert pages_after == 0, f"Expected 0 local_pages rows after delete, got {pages_after}"
    assert files_after == 0, f"Expected 0 local_files rows after delete, got {files_after}"
