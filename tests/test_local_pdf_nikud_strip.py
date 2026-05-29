# -*- coding: utf-8 -*-
"""Phase 102 Plan 03 Task 1 — D-06 FINAL all-format nikud-strip guard.

Tests that _write_page_doc strips nikud ONCE for ALL LOCAL formats
(PDF, DOCX, TXT, HTML, XLSX, CSV) via a function-local lazy strip_nikud import.

Key invariants pinned here:
  (a) All-format strip: a vocalized string passed to _write_page_doc (any format)
      has nikud removed from BOTH content AND cached_text (content == cached_text
      == consonantal). This is the D-06 FINAL guard (reverts round-2 PDF-only M4).
  (b) No nikud codepoints U+0591-U+05CF in content or cached_text.
  (c) extraction_format_version stored == 2 (bumped from 1).
  (d) An un-vocalized (consonantal) query string matches a page indexed from a
      vocalized source (the search-side win — strip at _write_page_doc makes
      vocalized source searchable by consonantal query).
  (e) The lazy import is FUNCTION-LOCAL (module top of shared/local_indexer.py
      must NOT have 'from genizah_core import strip_nikud').

These tests are designed to FAIL until _write_page_doc is updated (TDD RED).
"""
import ast
import inspect
import os
import re
import sqlite3


from shared.local_indexer import LocalIndexer, decompress_cached_text
from shared.local_sys_id import _canonical_filepath

# Unicode range for nikud (vowel points) + cantillation marks — same as NIKUD_PATTERN
_NIKUD_RE = re.compile(r"[֑-׏]")

# A vocalized Hebrew string with nikud and cantillation
_VOCALIZED = "בְּרֵאשִׁית בָּרָא אֱלֹהִים אֵת הַשָּׁמַיִם וְאֵת הָאָרֶץ"
# Consonantal (stripped) version — expected after strip_nikud
_CONSONANTAL = "בראשית ברא אלהים את השמים ואת הארץ"


def _make_indexer(tmp_path):
    """Create a LocalIndexer instance in a fresh tmp directory."""
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)
    return LocalIndexer(index_dir, lab_dir, db_path), db_path


def _setup_folder_and_file(indexer, db_path, sys_id, folder_path, filename="test.txt"):
    """Add folder to indexer and pre-insert processed_files + local_files rows.

    Returns (folder_id, fpath) so _write_page_doc has its FK lookups satisfied.
    """
    import time as _time
    fpath = os.path.join(folder_path, filename)
    if not os.path.exists(fpath):
        with open(fpath, "w", encoding="utf-8") as f:
            f.write("dummy")

    indexer.add_folder(folder_path)

    # Use canonical path for the folder lookup (matches what add_folder stores)
    canonical_folder = _canonical_filepath(folder_path)
    row = indexer._conn.execute(
        "SELECT folder_id FROM folders WHERE path = ?", (canonical_folder,)
    ).fetchone()
    assert row is not None, f"Folder not found after add_folder: {canonical_folder}"
    folder_id = row["folder_id"]

    canonical_fpath = _canonical_filepath(fpath)
    now = _time.time()
    ext = os.path.splitext(filename)[1].lower()
    # processed_files schema: filepath PK, mtime, size, sys_id, status, scan_run_id, mtime_ns
    fsize = os.path.getsize(canonical_fpath) if os.path.exists(canonical_fpath) else 10
    fmtime = os.path.getmtime(canonical_fpath) if os.path.exists(canonical_fpath) else now
    indexer._conn.execute(
        "INSERT OR REPLACE INTO processed_files "
        "(filepath, mtime, size, sys_id, status) VALUES (?, ?, ?, ?, 'pending')",
        (canonical_fpath, fmtime, fsize, sys_id),
    )
    indexer._conn.commit()
    indexer._conn.execute(
        "INSERT OR REPLACE INTO local_files "
        "(sys_id, filepath, folder_id, display_title, original_filename, file_extension, "
        " page_count, file_size_bytes, extraction_status, last_indexed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?, 'pending', ?)",
        (sys_id, canonical_fpath, folder_id, "Test", filename, ext, fsize, now),
    )
    indexer._conn.commit()
    return folder_id, canonical_fpath


# ---------------------------------------------------------------------------
# (a) All-format strip: _write_page_doc strips vocalized text for ALL formats
# ---------------------------------------------------------------------------

def test_write_page_doc_strips_nikud_all_formats(tmp_path):
    """D-06 FINAL — _write_page_doc strips nikud for ALL formats.

    Pass a vocalized Hebrew string directly to _write_page_doc (simulating any
    format: DOCX, TXT, HTML, XLSX, CSV, PDF).  Assert:
      - cached_text decompresses to the consonantal form (no nikud)
      - content == cached_text == consonantal (no divergence)

    This is the inverted M4 guard: the strip must NOT be PDF-only.
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-NIKUD-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, _ = _setup_folder_and_file(indexer, db_path, sys_id, folder, "test.txt")
        indexer._write_page_doc(sys_id, 1, _VOCALIZED, "Test Title", folder_id)

        # Read back from SQLite
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT cached_text, cached_text_codec FROM local_pages WHERE sys_id = ?",
            (sys_id,),
        ).fetchone()
        conn.close()

        assert row is not None, "No local_pages row written"
        cached_bytes = bytes(row["cached_text"])
        codec = row["cached_text_codec"]
        assert codec == "zstd", f"Expected zstd codec, got '{codec}'"

        # Decompress cached_text
        cached_str = decompress_cached_text(cached_bytes)

        # Assert no nikud in cached_text
        assert not _NIKUD_RE.search(cached_str), (
            f"Nikud found in cached_text: {cached_str!r} (D-06 FINAL — all-format strip "
            f"must remove nikud from cached_text)"
        )

        # Assert cached_text == consonantal expected
        assert cached_str == _CONSONANTAL, (
            f"cached_text mismatch: got {cached_str!r}, expected {_CONSONANTAL!r}"
        )
    finally:
        indexer.close()


def test_write_page_doc_docx_simulated_strip(tmp_path):
    """D-06 FINAL inverted M4 guard — DOCX-simulated write is stripped.

    Passes a vocalized string through _write_page_doc (as a DOCX extractor would)
    and asserts cached_text contains no nikud.  Before D-06 FINAL, only PDF had
    strip logic; this proves strip is at the shared write site, not in extractors.
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-DOCX-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, _ = _setup_folder_and_file(indexer, db_path, sys_id, folder, "test.docx")
        # Simulate a DOCX extractor calling _write_page_doc with vocalized text
        indexer._write_page_doc(sys_id, 1, _VOCALIZED, "Docx Title", folder_id)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT cached_text FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchone()
        conn.close()

        assert row is not None, "No local_pages row for DOCX-simulated write"
        cached_str = decompress_cached_text(bytes(row["cached_text"]))
        assert not _NIKUD_RE.search(cached_str), (
            f"Nikud found in DOCX-simulated cached_text: {cached_str!r} — "
            f"inverted M4 guard: _write_page_doc must strip ALL formats, NOT PDF-only"
        )
        assert cached_str == _CONSONANTAL, (
            f"DOCX cached_text not consonantal: {cached_str!r}"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# (c) extraction_format_version stored == 2
# ---------------------------------------------------------------------------

def test_write_page_doc_extraction_format_version_is_2(tmp_path):
    """extraction_format_version stored in local_pages must be 2 (bumped from 1)."""
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-VER-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, _ = _setup_folder_and_file(indexer, db_path, sys_id, folder, "test.txt")
        indexer._write_page_doc(sys_id, 1, "שלום עולם", "Title", folder_id)

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT extraction_format_version FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchone()
        conn.close()

        assert row is not None, "No local_pages row"
        assert row[0] == 2, (
            f"extraction_format_version should be 2 (D-06 FINAL bump), got {row[0]}"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# (d) Un-vocalized query matches vocalized source (search-side win)
# ---------------------------------------------------------------------------

def test_unvocalized_query_matches_vocalized_source(tmp_path):
    """D-06 FINAL search-side win: un-vocalized query matches vocalized-source page.

    strip_nikud at _write_page_doc makes every LOCAL format searchable by
    consonantal queries (no read-path change needed, no diacritic folding needed).
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-SEARCH-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, _ = _setup_folder_and_file(indexer, db_path, sys_id, folder, "test.txt")
        # Index a vocalized page
        indexer._write_page_doc(sys_id, 1, _VOCALIZED, "Search Test", folder_id)
        indexer._writer.commit()

        # Reload the searcher (required after commit for tantivy to see new doc)
        indexer._index.reload()

        # Now search with a consonantal (un-vocalized) query term from the text
        # "בראשית" is the first word of _CONSONANTAL
        search_term = "בראשית"
        query = indexer._index.parse_query(search_term, ["content"])
        searcher = indexer._index.searcher()
        results = searcher.search(query, 10)
        count = results.count
        assert count > 0, (
            f"Un-vocalized query '{search_term}' found 0 results — strip_nikud must be "
            f"applied in _write_page_doc so vocalized source is searchable consonantally"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# (e) Module-level import guard (L1 — lazy import only inside _write_page_doc)
# ---------------------------------------------------------------------------

def test_no_module_top_strip_nikud_import():
    """L1: 'from genizah_core import strip_nikud' must NOT appear at module-top of
    shared/local_indexer.py. The import must be function-local (lazy) inside _write_page_doc.
    """
    import shared.local_indexer as mod

    source_path = inspect.getfile(mod)
    with open(source_path, "r", encoding="utf-8") as f:
        source = f.read()

    tree = ast.parse(source)
    # Find module-level ImportFrom nodes (direct children of the module)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == "genizah_core":
                names = [alias.name for alias in node.names]
                assert "strip_nikud" not in names, (
                    f"L1 violation: 'from genizah_core import strip_nikud' found at module "
                    f"top (line {node.lineno}). It must be a function-local lazy import "
                    f"inside _write_page_doc."
                )
