# -*- coding: utf-8 -*-
"""Phase 97.1 — verify _iterate_supported_files honors cancel_check per file.

Regression for: prior implementation checked cancel_check only at directory
boundaries. For a single flat directory with hundreds of files, the cancel
flag went unread until that directory was fully iterated — cancel latency
exceeded the UI thread's 5 s `wait()` budget on real-world Dropbox trees.
Debug session: `.planning/debug/phase-97-freeze-winerror-3.md`.
"""
import os

import pytest

# Skip the entire module on platforms where local_indexer can't import
# (mostly to short-circuit on CI containers missing tantivy native libs).
tantivy = pytest.importorskip("tantivy")  # noqa: F841


def _make_files(tmp_dir: str, n: int) -> None:
    for i in range(n):
        with open(os.path.join(tmp_dir, f"f{i:04d}.pdf"), "wb") as f:
            f.write(b"x")


def _make_indexer(db_dir: str):
    """Build a LocalIndexer rooted at db_dir; caller must close()."""
    from shared.local_indexer import LocalIndexer

    db_path = os.path.join(db_dir, "test.db")
    index_dir = os.path.join(db_dir, "index")
    lab_index_dir = os.path.join(db_dir, "lab")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_index_dir, exist_ok=True)
    return LocalIndexer(
        index_dir=index_dir,
        lab_index_dir=lab_index_dir,
        db_path=db_path,
    )


def test_iterate_supported_files_cancels_within_5_yields(tmp_path):
    """200 files; cancel_check returns True after 5 yields. Expect early stop."""
    folder = tmp_path / "src"
    folder.mkdir()
    _make_files(str(folder), 200)

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    indexer = _make_indexer(str(db_dir))
    try:
        seen = 0
        cancel_after = 5

        def cancel_check() -> bool:
            return seen >= cancel_after

        for _canonical, _size in indexer._iterate_supported_files(
            str(folder), cancel_check
        ):
            seen += 1
            # If the inner cancel check is missing, this loop runs to 200.
            # With FIX-2b, it must stop at cancel_after + a small margin.
            if seen > cancel_after + 2:
                break

        assert seen <= cancel_after + 1, (
            f"_iterate_supported_files yielded {seen} files after "
            f"cancel_check became True at {cancel_after}; per-file cancel "
            f"check is missing"
        )
    finally:
        try:
            indexer.close()
        except Exception:
            pass


def test_iterate_supported_files_runs_to_completion_without_cancel(tmp_path):
    """Baseline: same fixture, cancel_check never fires → all files yielded."""
    folder = tmp_path / "src"
    folder.mkdir()
    _make_files(str(folder), 30)

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    indexer = _make_indexer(str(db_dir))
    try:
        files = list(
            indexer._iterate_supported_files(str(folder), lambda: False)
        )
        assert len(files) == 30
    finally:
        try:
            indexer.close()
        except Exception:
            pass
