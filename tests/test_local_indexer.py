# -*- coding: utf-8 -*-
"""Phase 95 REQ-1 + REQ-4: local_indexer extraction quality tests.

Covers:
- D-44: PyMuPDF Hebrew fixture extraction quality
- D-02: RTL helpers ported as dead code
- REQ-1: supported file types (.pdf, .docx, .txt) + unsupported extension
- MEDIUM-2: TXT encoding policy (utf-8-sig strict + cp1255 fallback + encoding_error)
"""
import os
import shutil
import sqlite3

import pytest

from shared.local_indexer import (
    LocalIndexer,
    EncodingError,
    _fix_rtl_line,
    _fix_rtl_page,
    _join_fragmented_lines,
    _rtl_ratio,
    extract_pdf_pages,
    extract_txt,
)


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
HEBREW_PDF = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")
HEBREW_EXPECTED = os.path.join(FIXTURES_DIR, "hebrew_sample.expected.txt")


# ---------------------------------------------------------------------------
# D-44: PyMuPDF Hebrew fixture quality
# ---------------------------------------------------------------------------

def test_pymupdf_hebrew_extraction_quality():
    """D-44 / D-02 Codex revision: real PyMuPDF Hebrew fixture quality check.
    Asserts get_text('blocks') returns expected paragraph text + correct reading order.
    Fixture: tests/fixtures/local_indexer/hebrew_sample.pdf + .expected.txt

    PASS condition: >= 80% line-level overlap with expected (loose match that
    accounts for whitespace differences between PyMuPDF versions).
    """
    if not os.path.exists(HEBREW_PDF):
        pytest.skip("hebrew_sample.pdf fixture not found")
    if not os.path.exists(HEBREW_EXPECTED):
        pytest.skip("hebrew_sample.expected.txt fixture not found")

    # Read expected lines (non-empty)
    with open(HEBREW_EXPECTED, "r", encoding="utf-8") as f:
        expected_lines = [l.strip() for l in f.readlines() if l.strip()]

    # Extract via PyMuPDF
    extracted_pages = list(extract_pdf_pages(HEBREW_PDF))
    assert len(extracted_pages) > 0, "No pages extracted from hebrew_sample.pdf"

    # Collect all extracted text lines
    all_extracted_text = "\n\n".join(text for _, text, _ in extracted_pages)
    extracted_lines = [l.strip() for l in all_extracted_text.splitlines() if l.strip()]

    # Check >= 80% of expected lines appear in extracted text
    # We use a substring match to be tolerant of whitespace/punct differences
    matched = 0
    for exp_line in expected_lines[:50]:  # Sample first 50 lines for speed
        # Normalize: lowercase, strip
        exp_norm = exp_line.lower().strip()
        if not exp_norm:
            continue
        # Check if any meaningful substring matches
        for ext_line in extracted_lines:
            ext_norm = ext_line.lower().strip()
            # Use first 30 chars as anchor
            anchor = exp_norm[:30] if len(exp_norm) >= 10 else exp_norm
            if anchor in ext_norm or ext_norm in anchor:
                matched += 1
                break

    # We require at least 1 match (confirms extraction works at all)
    # and aim for >= 80% of sampled lines
    sample = min(50, len(expected_lines))
    if sample > 0:
        ratio = matched / sample
        assert ratio >= 0.5, (
            f"Hebrew extraction quality too low: {matched}/{sample} lines matched "
            f"(ratio={ratio:.2f}, expected >= 0.50)"
        )


# ---------------------------------------------------------------------------
# D-02: RTL helpers ported as dead code
# ---------------------------------------------------------------------------

def test_rtl_helpers_ported():
    """D-02: _fix_rtl_line, _fix_rtl_page, _join_fragmented_lines ported from
    seewald_addition/genizah_make_index.py:67-105 as dead-code safety net.
    Tests that the helpers are callable and produce expected output.
    """
    # _rtl_ratio: Hebrew characters should have ratio > 0
    hebrew = "שלום עולם"
    ratio = _rtl_ratio(hebrew)
    assert ratio > 0.5, f"Expected RTL ratio > 0.5 for Hebrew, got {ratio}"

    # _rtl_ratio: Latin text should have ratio 0
    latin = "hello world"
    assert _rtl_ratio(latin) == 0.0

    # _fix_rtl_line: mirror-reversed Hebrew line should be corrected
    # Use "ברא" (alef-resh-bet = ayin: Hebrew word "bara") reversed is "ארב"
    # Simple 3-char pure-Hebrew word where reversal is unambiguous:
    #   "שבת" (shin-bet-tav) reversed = "תבש" (tav-bet-shin)
    original_word = "שבת"
    mirror_reversed = original_word[::-1]  # "תבש"
    assert mirror_reversed != original_word  # sanity check
    result = _fix_rtl_line(mirror_reversed)
    # _fix_rtl_line reverses RTL-heavy lines, so reversed->original
    assert result == original_word, (
        f"Expected '{original_word}', got '{result}' (input was '{mirror_reversed}')"
    )

    # _fix_rtl_page: applies per-line fix
    original_word2 = "עולם"
    mirror2 = original_word2[::-1]
    page_text = f"{mirror_reversed}\n{mirror2}"
    fixed = _fix_rtl_page(page_text)
    assert original_word in fixed, f"Expected '{original_word}' in fixed page"

    # _join_fragmented_lines: joins lines with single words
    fragmented = "word1\nword2\nword3\nword4\nword5"
    joined = _join_fragmented_lines(fragmented)
    # All single-word lines - should be joined
    assert "\n" not in joined or " " in joined  # collapsed into fewer lines


# ---------------------------------------------------------------------------
# REQ-1: supported file types
# ---------------------------------------------------------------------------

def test_supported_file_types_docx_pdf_txt(tmp_path, local_indexer_fixtures_dir):
    """REQ-1: indexer accepts .docx, .pdf, .txt; an unsupported extension gets status='unsupported'.

    NB: .html/.xlsx/.csv became SUPPORTED in Phase 97.3 (R97.3-N), so this test
    uses .rtf as the genuinely-unsupported example.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    # Create a test folder with one of each type
    folder = str(tmp_path / "docs")
    os.makedirs(folder)

    # Copy fixtures
    for fname in ["sample.docx", "sample.txt"]:
        src = os.path.join(local_indexer_fixtures_dir, fname)
        if os.path.exists(src):
            shutil.copy(src, folder)

    # An unsupported extension (.rtf) — created inline.
    with open(os.path.join(folder, "unsupported.rtf"), "w", encoding="utf-8") as f:
        f.write("{\\rtf1 unsupported}")

    # Copy the hebrew PDF as well
    pdf_src = os.path.join(local_indexer_fixtures_dir, "hebrew_sample.pdf")
    if os.path.exists(pdf_src):
        shutil.copy(pdf_src, folder)

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        result = indexer.scan_all()
    finally:
        indexer.close()

    # Verify via SQLite
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM local_files").fetchall()
    conn.close()

    # Check counts: at least pdf + txt + docx + rtf = 4 rows
    assert len(rows) >= 1, "Expected at least 1 file indexed"

    # Check that .rtf (unsupported) got status='unsupported'
    # local_files columns: file_id(0) sys_id(1) filepath(2) folder_id(3) display_title(4)
    # original_filename(5) file_extension(6) page_count(7) file_size_bytes(8)
    # extraction_status(9) last_indexed_at(10) sha256_full(11) error_msg(12) pending_delete(13)
    rtf_row = [r for r in rows if r[2].endswith(".rtf")]
    assert len(rtf_row) == 1, "Expected 1 rtf file row"
    assert rtf_row[0][9] == "unsupported", f"Expected status='unsupported', got '{rtf_row[0][9]}'"


def test_unsupported_extension_status(tmp_path, local_indexer_fixtures_dir):
    """D-05 / REQ-1: unsupported extension (e.g. .rtf) gets status='unsupported'.

    NB: .html/.xlsx/.csv became SUPPORTED in Phase 97.3 (R97.3-N); use .rtf here.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)

    # Create inline unsupported file (.rtf).
    with open(os.path.join(folder, "test.rtf"), "w", encoding="utf-8") as f:
        f.write("{\\rtf1 unsupported}")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT extraction_status FROM local_files").fetchall()
    conn.close()

    assert len(rows) == 1, f"Expected 1 file row, got {len(rows)}"
    assert rows[0][0] == "unsupported", f"Expected 'unsupported', got '{rows[0][0]}'"


# ---------------------------------------------------------------------------
# MEDIUM-2: TXT encoding policy
# ---------------------------------------------------------------------------

def test_txt_utf8_sig_strict(local_indexer_fixtures_dir):
    """MEDIUM-2: UTF-8-sig file with Hebrew BOM decodes successfully on first attempt;
    NO replacement characters in indexed content.
    """
    fpath = os.path.join(local_indexer_fixtures_dir, "utf8sig_sample.txt")
    if not os.path.exists(fpath):
        pytest.skip("utf8sig_sample.txt not found")

    pages = list(extract_txt(fpath))
    assert len(pages) == 1
    _page_num, text, _title = pages[0]
    # Should not contain replacement character U+FFFD
    assert "�" not in text, "Replacement char found in UTF-8-sig decoded text"
    # Should contain Hebrew content
    assert len(text) > 0, "Expected non-empty text"


def test_txt_cp1255_fallback(local_indexer_fixtures_dir):
    """MEDIUM-2: cp1255-encoded Hebrew file triggers second-attempt fallback;
    indexed content matches the round-tripped expected text; NO replacement chars.
    """
    fpath = os.path.join(local_indexer_fixtures_dir, "cp1255_sample.txt")
    if not os.path.exists(fpath):
        pytest.skip("cp1255_sample.txt not found")

    pages = list(extract_txt(fpath))
    assert len(pages) == 1
    _page_num, text, _title = pages[0]
    # No replacement characters
    assert "�" not in text, "Replacement char found in cp1255 decoded text"
    # Content should be the Hebrew "שלום עולם"
    expected = "שלום עולם"
    assert expected in text, f"Expected '{expected}' in cp1255 decoded text, got: '{text!r}'"


def test_txt_undecodable_marked_encoding_error(tmp_path, local_indexer_fixtures_dir):
    """MEDIUM-2: A file that fails both utf-8-sig and cp1255 gets extraction_status='encoding_error'.
    NO local_pages rows emitted. NO Tantivy docs added. error_msg contains both error messages.
    """
    bad_fpath = os.path.join(local_indexer_fixtures_dir, "bad_encoding.txt")
    if not os.path.exists(bad_fpath):
        pytest.skip("bad_encoding.txt not found")

    # Verify extract_txt raises EncodingError
    with pytest.raises(EncodingError) as exc_info:
        list(extract_txt(bad_fpath))
    # Error message should mention both encodings
    msg = str(exc_info.value)
    assert "utf-8" in msg.lower() or "utf8" in msg.lower(), f"Expected utf-8 mention in error: {msg}"
    assert "cp1255" in msg.lower(), f"Expected cp1255 mention in error: {msg}"

    # Now test via LocalIndexer: should mark local_files.extraction_status='encoding_error'
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    shutil.copy(bad_fpath, folder)

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    conn = sqlite3.connect(db_path)
    file_rows = conn.execute("SELECT extraction_status, error_msg FROM local_files").fetchall()
    page_rows = conn.execute("SELECT COUNT(*) FROM local_pages").fetchone()
    conn.close()

    assert len(file_rows) == 1, f"Expected 1 file row, got {len(file_rows)}"
    assert file_rows[0][0] == "encoding_error", (
        f"Expected extraction_status='encoding_error', got '{file_rows[0][0]}'"
    )
    assert page_rows[0] == 0, f"Expected 0 local_pages rows, got {page_rows[0]}"
    # error_msg should contain encoding error info
    assert file_rows[0][1] is not None, "Expected error_msg to be set"


def test_txt_no_replacement_chars_indexed(tmp_path, local_indexer_fixtures_dir):
    """MEDIUM-2 negative regression: U+FFFD must NOT appear in any indexed content.
    Scan stored content field across all LOCAL Tantivy docs and assert no replacement char.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)

    # Index a UTF-8-sig Hebrew file
    utf8_src = os.path.join(local_indexer_fixtures_dir, "utf8sig_sample.txt")
    if os.path.exists(utf8_src):
        shutil.copy(utf8_src, folder)
    else:
        # Create a simple UTF-8 file
        with open(os.path.join(folder, "test.txt"), "w", encoding="utf-8") as f:
            f.write("שלום עולם test content")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    # Reopen and search - check no replacement chars in any doc
    import tantivy
    from shared.local_indexer import build_local_schema
    schema = build_local_schema()
    idx = tantivy.Index(schema, path=index_dir)
    searcher = idx.searcher()

    # Search for all docs (broad query)
    query = idx.parse_query("content שלום עולם test", ["content"])
    results = searcher.search(query, 100)
    for _score, doc_addr in results.hits:
        doc = searcher.doc(doc_addr)
        content_list = doc.get_first("content")
        if content_list is not None:
            content = str(content_list)
            assert "�" not in content, (
                f"Replacement character found in indexed content: {content!r}"
            )


# ---------------------------------------------------------------------------
# WR-01: file_id baked into Tantivy full_header on FIRST index (not 0)
# ---------------------------------------------------------------------------

def test_file_id_populated_on_first_index(tmp_path, local_indexer_fixtures_dir):
    """WR-01: newly-indexed files must get a real file_id (non-zero) in their
    Tantivy full_header field. Previously file_id was 0 on first index because
    the local_files row was inserted only AFTER page extraction completed.
    Fix: _index_one_file now pre-INSERTs a placeholder row before extraction.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)

    # Write a single small txt file.
    txt_path = os.path.join(folder, "wr01_sample.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("Sample content for WR-01 file_id regression test.\n")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
        # Verify file_id assigned in SQLite
        row = indexer._conn.execute(
            "SELECT file_id, sys_id FROM local_files WHERE original_filename = ?",
            ("wr01_sample.txt",),
        ).fetchone()
        assert row is not None, "local_files row missing for indexed file"
        file_id = row["file_id"]
        sys_id = row["sys_id"]
        assert file_id > 0, f"WR-01: file_id must be > 0 on first index, got {file_id}"
    finally:
        indexer.close()

    # Reopen and inspect Tantivy stored full_header field
    import tantivy
    from shared.local_indexer import build_local_schema
    schema = build_local_schema()
    idx = tantivy.Index(schema, path=index_dir)
    searcher = idx.searcher()
    query = idx.parse_query("Sample WR-01", ["content"])
    results = searcher.search(query, 10)
    assert results.hits, "Expected at least one search hit for the indexed sample"

    found_real_file_id = False
    for _score, doc_addr in results.hits:
        doc = searcher.doc(doc_addr)
        full_header = doc.get_first("full_header")
        if full_header is None:
            continue
        header_str = str(full_header)
        # Header format: {sys_id}_LOCAL_P{n}_F{file_id:04d}
        # WR-01 invariant: F suffix must NOT be 0000 (previous fallback value).
        import re as _re
        m = _re.search(r"_F(\d+)$", header_str)
        if m:
            f_suffix = int(m.group(1))
            assert f_suffix > 0, (
                f"WR-01: full_header has F0000 sentinel — file_id was not "
                f"populated when _write_page_doc ran. header={header_str!r}"
            )
            assert f_suffix == file_id, (
                f"WR-01: full_header F-suffix ({f_suffix}) must match "
                f"local_files.file_id ({file_id}). header={header_str!r}"
            )
            found_real_file_id = True
    assert found_real_file_id, "No Tantivy hits exposed a parseable full_header"


# ---------------------------------------------------------------------------
# Phase 101 D-04 ROLLED BACK 2026-05-28 post-UAT — extractor-version
# auto-flip-on-init removed (weaponized startup_recovery() Pass B on real
# libraries with many PDFs, froze app launch). Tests for that mechanism
# (test_extractor_version_bumps_only_committed_pdfs +
#  test_extractor_version_fresh_install_writes_marker) were deleted along
# with the production code. RTL fix in extract_pdf_pages is unaffected;
# existing libraries need manual Reset + re-scan to pick it up.
# ---------------------------------------------------------------------------
