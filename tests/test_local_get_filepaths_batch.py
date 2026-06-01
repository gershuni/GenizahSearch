# -*- coding: utf-8 -*-
"""v7.16 BUG-6: LocalIndexer.get_filepaths() batch lookup.

Replaces N per-row get_filepath() SQLite round-trips on the UI thread (the cause
of the ~10s LOCAL-search freeze) with ONE IN(...) query. Pure (no Qt) so it runs
on CI.
"""
import os

from shared.local_indexer import LocalIndexer


def _make_indexer_with_files(tmp_path, names):
    folder = str(tmp_path / "lib")
    os.makedirs(folder)
    for n in names:
        with open(os.path.join(folder, n), "w", encoding="utf-8") as f:
            f.write("רוזנצווייג " + n)
    idx = str(tmp_path / "idx")
    lab = str(tmp_path / "lab")
    db = str(tmp_path / "m.sqlite3")
    os.makedirs(idx)
    os.makedirs(lab)
    ix = LocalIndexer(idx, lab, db)
    ix.add_folder(folder)
    ix.scan_all()
    return ix


def test_get_filepaths_matches_single_lookup(tmp_path):
    ix = _make_indexer_with_files(tmp_path, ["a.txt", "b.txt", "c.txt"])
    try:
        ids = [r["sys_id"] for r in ix._conn.execute("SELECT sys_id FROM local_files")]
        assert len(ids) == 3
        batch = ix.get_filepaths(ids)
        assert len(batch) == 3
        for sid in ids:
            assert batch[sid] == ix.get_filepath(sid)
    finally:
        ix.close()


def test_get_filepaths_edge_cases(tmp_path):
    ix = _make_indexer_with_files(tmp_path, ["a.txt"])
    try:
        assert ix.get_filepaths([]) == {}
        assert ix.get_filepaths(["does-not-exist"]) == {}
        assert ix.get_filepaths([None, ""]) == {}
        # Duplicate ids collapse; missing ids simply absent.
        ids = [r["sys_id"] for r in ix._conn.execute("SELECT sys_id FROM local_files")]
        out = ix.get_filepaths(ids + ids + ["nope"])
        assert set(out) == set(ids)
    finally:
        ix.close()


def test_get_filepaths_handles_large_id_list(tmp_path):
    """>900 ids must chunk under SQLITE_MAX_VARIABLE_NUMBER without error."""
    ix = _make_indexer_with_files(tmp_path, ["a.txt"])
    try:
        real = [r["sys_id"] for r in ix._conn.execute("SELECT sys_id FROM local_files")]
        synthetic = [f"missing{i:05d}" for i in range(2500)]
        out = ix.get_filepaths(real + synthetic)
        assert set(out) == set(real)  # only the real one resolves; no crash on 2500+ ids
    finally:
        ix.close()


def test_get_folder_filepaths(tmp_path):
    """v7.16 BUG-5: per-folder file enumeration for the folder opt-out checkbox."""
    ix = _make_indexer_with_files(tmp_path, ["a.txt", "b.txt", "c.txt"])
    try:
        folder = str(tmp_path / "lib")
        files = ix.get_folder_filepaths(folder)
        assert len(files) == 3
        # Every returned path matches a stored canonical local_files.filepath.
        stored = {r["filepath"] for r in ix._conn.execute("SELECT filepath FROM local_files")}
        assert set(files) == stored
        # Unknown folder → empty list (not an error).
        assert ix.get_folder_filepaths(str(tmp_path / "nope")) == []
    finally:
        ix.close()
