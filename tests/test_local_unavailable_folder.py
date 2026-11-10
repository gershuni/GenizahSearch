# -*- coding: utf-8 -*-
"""Phase 95 D-40 / Phase 97 D-NEW-2: unreachable folder behavior at app startup.

When os.path.isdir(folder.path) is False at auto-rescan:
- folders.status updated to 'unreachable' (Phase 97 D-NEW-2 errno-discriminated
  reachability check; superseded the Phase 95 'unavailable' label for a missing
  folder — ENOENT now maps to 'unreachable')
- Existing local_files rows are PRESERVED (not deleted)
- Existing Tantivy docs remain searchable
"""
import os
import shutil
import sqlite3


from shared.local_indexer import LocalIndexer


def test_unavailable_folder_marked_status_unavailable(tmp_path):
    """D-40: when os.path.isdir(folder.path) is False at auto-rescan, the folder
    row is updated to status='unavailable'. Existing Tantivy docs are NOT deleted.
    Previously-indexed files remain as local_files rows (not purged).
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    # Create a folder with 2 text files
    folder = str(tmp_path / "my_docs")
    os.makedirs(folder)
    for i in range(2):
        with open(os.path.join(folder, f"doc{i}.txt"), "w", encoding="utf-8") as f:
            f.write(f"Document {i} content: שלום עולם test text for searching.")

    # First scan: index 2 files
    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        result = indexer.scan_all()
    finally:
        indexer.close()

    assert result["indexed"] == 2, f"Expected 2 indexed, got {result}"

    # Verify files are in local_files
    conn = sqlite3.connect(db_path)
    file_rows_before = conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    assert file_rows_before == 2, f"Expected 2 local_files rows before, got {file_rows_before}"
    conn.close()

    # Make the folder unavailable by deleting it
    shutil.rmtree(folder)
    assert not os.path.isdir(folder), "Folder should not exist now"

    # Second scan: folder is unavailable
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        result2 = indexer2.scan_all()
    finally:
        indexer2.close()

    # Check folders.status = 'unreachable' (Phase 97 D-NEW-2: ENOENT -> 'unreachable')
    conn = sqlite3.connect(db_path)
    folder_row = conn.execute("SELECT status FROM folders WHERE path LIKE ?", (f"%my_docs%",)).fetchone()
    assert folder_row is not None, "Folder row should exist in folders table"
    assert folder_row[0] == "unreachable", (
        f"Expected folder status='unreachable', got '{folder_row[0]}'"
    )

    # local_files rows must be PRESERVED (D-40: do not purge on unavailability)
    file_rows_after = conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    assert file_rows_after == 2, (
        f"Expected 2 local_files rows PRESERVED after unavailability, got {file_rows_after}"
    )

    conn.close()
