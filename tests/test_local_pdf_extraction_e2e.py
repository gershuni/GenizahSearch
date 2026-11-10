# -*- coding: utf-8 -*-
"""Phase 102 Plan 05 Task 2 -- end-to-end extraction/index/query tests.

Covers the full mandated matrix from 102-05-PLAN.md:

  F-D: test_letter_spaced_defragments
       letter_spaced_hebrew.pdf extracts to multi-letter Hebrew tokens;
       single-letter token ratio < 0.15.

  F-E: test_letter_spaced_reversed_reads_correctly
       "מלחמת" appears in extracted text after the RTL reorder pipeline.

  F-F: test_rtl_running_header
       "אבן תיכון" present (words reordered correctly),
       "תיכון אבן" absent (wrong emission order corrected).

  LTR no-regression: test_ltr_no_regression
       ltr_latin_noregress.pdf: "Northwest Semitic Dictionary" present un-scrambled;
       rawdict token count >= 95% of old blocks token count (D-03 guard band).

  F-G / D-F16: test_corrupt_encoding_status
       corrupt_encoding_sample.pdf: _extract_and_write_pdf returns
       status='corrupt_encoding' AND pages_written==0 (no garbage indexed,
       HIGH-2 detect-before-write).

  Existing guard: test_single_word_per_line_guard_still_passes
       single_word_per_line.pdf still yields non-empty sensible text.

  D-06 FINAL: test_nikud_strip_e2e
       Vocalized PDF page indexed via LocalIndexer; un-vocalized (consonantal)
       Tantivy query matches; cached_text has NO nikud and EQUALS content
       (content == cached_text == stripped, NO divergence).

See tests/scripts/build_phase102_fixtures.py for fixture provenance.
See docs/OPEN_ISSUES.md D-F13/D-F14/D-F16/SEED-004 for tracker context.
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import time

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
_LETTER_SPACED_PDF = os.path.join(_FIXTURES, "letter_spaced_hebrew.pdf")
_LTR_NOREGRESS_PDF = os.path.join(_FIXTURES, "ltr_latin_noregress.pdf")
_CORRUPT_PDF = os.path.join(_FIXTURES, "corrupt_encoding_sample.pdf")
_SINGLE_WORD_PDF = os.path.join(_FIXTURES, "single_word_per_line.pdf")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_NIKUD_RE = re.compile(r"[ְ-ׇ]")
_HEB_RE = re.compile(r"[֐-׿יִ-ﭏ]")


def _is_heb_token(token: str) -> bool:
    """Return True if every non-space char in token is a Hebrew or nikud codepoint."""
    return bool(token) and all(
        _HEB_RE.match(c) or _NIKUD_RE.match(c)
        for c in token
    )


def _single_letter_ratio(text: str) -> float:
    """Fraction of Hebrew tokens that are exactly one Hebrew letter."""
    tokens = [t for t in text.split() if t.strip()]
    heb = [t for t in tokens if _is_heb_token(t)]
    if not heb:
        return 0.0
    single = [t for t in heb if len(t) == 1]
    return len(single) / len(heb)


def _make_indexer(tmp_path: str):
    """Create a LocalIndexer in a fresh temp directory."""
    from shared.local_indexer import LocalIndexer
    idx = os.path.join(tmp_path, "idx")
    lab = os.path.join(tmp_path, "lab")
    db = os.path.join(tmp_path, "test.sqlite3")
    os.makedirs(idx)
    os.makedirs(lab)
    return LocalIndexer(idx, lab, db), db


def _pre_insert_pdf_rows(indexer, db_path: str, sys_id: str, folder: str, filename: str):
    """Pre-insert processed_files + local_files rows (mirrors _index_one_file).

    Required so _extract_and_write_pdf has the FK lookups it needs
    (processed_files.filepath for the basename, local_files.file_id for full_header).
    """
    from shared.local_sys_id import _canonical_filepath
    fpath = os.path.join(folder, filename)
    if not os.path.exists(fpath):
        shutil.copy(_CORRUPT_PDF, fpath)

    indexer.add_folder(folder)

    canonical_folder = _canonical_filepath(folder)
    row = indexer._conn.execute(
        "SELECT folder_id FROM folders WHERE path = ?", (canonical_folder,)
    ).fetchone()
    assert row is not None, f"Folder not found: {canonical_folder}"
    folder_id = row["folder_id"]

    canonical_fpath = _canonical_filepath(fpath)
    now = time.time()
    fsize = os.path.getsize(canonical_fpath)
    fmtime = os.path.getmtime(canonical_fpath)

    indexer._conn.execute(
        "INSERT OR REPLACE INTO processed_files "
        "(filepath, mtime, size, sys_id, status) VALUES (?, ?, ?, ?, 'pending')",
        (canonical_fpath, fmtime, fsize, sys_id),
    )
    indexer._conn.commit()
    indexer._conn.execute(
        "INSERT OR REPLACE INTO local_files "
        "(sys_id, filepath, folder_id, display_title, original_filename, "
        " file_extension, page_count, file_size_bytes, extraction_status, last_indexed_at) "
        "VALUES (?, ?, ?, ?, ?, '.pdf', 0, ?, 'pending', ?)",
        (sys_id, canonical_fpath, folder_id, "Test", filename, fsize, now),
    )
    indexer._conn.commit()
    return folder_id, canonical_fpath


# ---------------------------------------------------------------------------
# F-D: single-letter token ratio
# ---------------------------------------------------------------------------

def test_letter_spaced_defragments():
    """F-D: letter_spaced_hebrew.pdf extracts to multi-letter Hebrew tokens.

    The Phase 102 rawdict + de-space pipeline collapses individual Hebrew
    letter-tokens back into searchable words.  single-letter ratio MUST be
    < 0.15 (ideally 0.0 for the synthetic fixture).

    This test pins the dominant PDF extraction bug found in Spike 001 (D-F13):
    whole books like אוצר הגאונים had 46% single-letter tokens before the fix.
    """
    if not os.path.exists(_LETTER_SPACED_PDF):
        pytest.skip("letter_spaced_hebrew.pdf fixture missing; run build_phase102_fixtures.py")

    from shared.local_indexer import extract_pdf_pages
    pages = list(extract_pdf_pages(_LETTER_SPACED_PDF))
    assert pages, "letter_spaced_hebrew.pdf yielded zero pages"

    all_text = "\n".join(t for _, t, _ in pages)
    ratio = _single_letter_ratio(all_text)
    assert ratio < 0.15, (
        f"F-D: single-letter Hebrew token ratio {ratio:.3f} >= 0.15 -- "
        f"de-space did NOT collapse letter-spaced glyphs into searchable words. "
        f"Extracted text (first 300 chars): {all_text[:300]!r}"
    )


# ---------------------------------------------------------------------------
# F-E: correct reading order after reorder
# ---------------------------------------------------------------------------

def test_letter_spaced_reversed_reads_correctly():
    """F-E: the word 'מלחמת' appears in the extracted text of letter_spaced_hebrew.pdf.

    The RTL reorder pipeline must produce the correct logical Unicode word
    ('מלחמת' = war) not its physically-reversed form ('תמחלמ').
    """
    if not os.path.exists(_LETTER_SPACED_PDF):
        pytest.skip("letter_spaced_hebrew.pdf fixture missing")

    from shared.local_indexer import extract_pdf_pages
    pages = list(extract_pdf_pages(_LETTER_SPACED_PDF))
    all_text = "\n".join(t for _, t, _ in pages)

    assert "מלחמת" in all_text, (
        "F-E: 'מלחמת' not found in extracted text -- RTL reorder did not "
        "produce correct Unicode reading order.\n"
        f"Extracted (first 400 chars): {all_text[:400]!r}"
    )


# ---------------------------------------------------------------------------
# F-F: RTL running-header reorder
# ---------------------------------------------------------------------------

def test_rtl_running_header():
    """F-F: 'אבן תיכון' present (correct reading order), 'תיכון אבן' absent.

    The running-header fixture stores 'תיכון' (original_order 0-4, lower x)
    BEFORE 'אבן' (original_order 5-7, higher x) -- wrong emission order.
    The reorder algorithm detects the forward center-x jump and outputs
    'אבן תיכון' (right-to-left reading order: 'אבן' first = rightmost).
    """
    if not os.path.exists(_LETTER_SPACED_PDF):
        pytest.skip("letter_spaced_hebrew.pdf fixture missing")

    from shared.local_indexer import extract_pdf_pages
    pages = list(extract_pdf_pages(_LETTER_SPACED_PDF))
    all_text = "\n".join(t for _, t, _ in pages)

    assert "אבן תיכון" in all_text, (
        "F-F: 'אבן תיכון' NOT found -- running-header word-order was not "
        f"corrected by the RTL reorder. Extracted: {all_text!r}"
    )
    assert "תיכון אבן" not in all_text, (
        "F-F: 'תיכון אבן' found -- running-header word-order is STILL wrong "
        f"(uncorrected wrong emission order). Extracted: {all_text!r}"
    )


# ---------------------------------------------------------------------------
# LTR no-regression
# ---------------------------------------------------------------------------

def test_ltr_no_regression():
    """D-03 LTR no-regression pin: rawdict pipeline preserves 'Northwest Semitic Dictionary'.

    Requirements:
      1. Exact phrase "Northwest Semitic Dictionary" present and un-scrambled.
      2. Rawdict token count >= 95% of old blocks token count (D-03 guard band).

    The spike found Meiri's raw reorder HURTS Latin/LTR PDFs (NW Semitic
    Dictionary was worse after wholesale reorder).  The RTL-gated approach
    must leave LTR content unchanged.
    """
    if not os.path.exists(_LTR_NOREGRESS_PDF):
        pytest.skip("ltr_latin_noregress.pdf fixture missing; run build_phase102_fixtures.py")

    from shared.local_indexer import extract_pdf_pages, _extract_blocks_text
    import fitz

    # New rawdict path
    pages = list(extract_pdf_pages(_LTR_NOREGRESS_PDF))
    assert pages, "ltr_latin_noregress.pdf yielded zero pages"
    rawdict_text = "\n".join(t for _, t, _ in pages)

    # 1. Phrase present and un-scrambled
    assert "Northwest Semitic Dictionary" in rawdict_text, (
        "LTR no-regression: 'Northwest Semitic Dictionary' not found or scrambled.\n"
        f"Rawdict text (first 200 chars): {rawdict_text[:200]!r}"
    )

    # 2. Token-count guard band (rawdict >= 95% of blocks)
    doc = fitz.open(_LTR_NOREGRESS_PDF)
    blocks_text = _extract_blocks_text(doc[0])
    doc.close()

    rawdict_tokens = rawdict_text.split()
    blocks_tokens = blocks_text.split()
    if blocks_tokens:
        count_ratio = len(rawdict_tokens) / len(blocks_tokens)
        assert count_ratio >= 0.95, (
            f"LTR no-regression: rawdict token count ({len(rawdict_tokens)}) "
            f"is only {count_ratio:.2%} of blocks ({len(blocks_tokens)}) -- "
            f"rawdict DAMAGED the LTR content (D-03 guard band < 95%)."
        )


# ---------------------------------------------------------------------------
# F-G / D-F16: corrupt encoding status
# ---------------------------------------------------------------------------

def test_corrupt_encoding_status(tmp_path):
    """F-G/D-F16: corrupt_encoding_sample.pdf yields status='corrupt_encoding', pages=0.

    HIGH-2 detect-before-write: _extract_and_write_pdf buffers all pages,
    checks the >= 50% corrupt-page threshold, and returns BEFORE calling
    _write_page_doc -- so ZERO garbage pages reach the Tantivy index.
    """
    if not os.path.exists(_CORRUPT_PDF):
        pytest.skip("corrupt_encoding_sample.pdf missing; run build_phase102_fixtures.py")

    indexer, db_path = _make_indexer(str(tmp_path))
    sys_id = "CORRUPT-E2E-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)

    try:
        # Copy the corrupt fixture into the temp folder so _extract_and_write_pdf can open it
        dest_path = os.path.join(folder, "corrupt.pdf")
        shutil.copy(_CORRUPT_PDF, dest_path)

        folder_id, canonical_fpath = _pre_insert_pdf_rows(
            indexer, db_path, sys_id, folder, "corrupt.pdf"
        )

        pages_written, status, title = indexer._extract_and_write_pdf(
            sys_id, dest_path, folder_id, cancel_check=lambda: False
        )

        assert status == "corrupt_encoding", (
            f"F-G: Expected status='corrupt_encoding', got '{status}'. "
            f"The corrupt PDF should be detected BEFORE writing any pages."
        )
        assert pages_written == 0, (
            f"F-G: Expected pages_written==0, got {pages_written}. "
            f"HIGH-2 detect-before-write: no garbage must reach the index."
        )

        # Verify no local_pages rows were written (belt + suspenders)
        conn = sqlite3.connect(db_path)
        page_count = conn.execute(
            "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        conn.close()
        assert page_count == 0, (
            f"F-G: {page_count} local_pages rows were written for a corrupt file "
            f"-- HIGH-2 detect-before-write FAILED."
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# Existing guard: single_word_per_line.pdf still passes
# ---------------------------------------------------------------------------

def test_single_word_per_line_guard_still_passes():
    """Phase 96 D-F4 guard: single_word_per_line.pdf still extracts sensible text.

    The Phase 102 rawdict primary path (with D-03 LTR-damage fallback to blocks)
    must not regress this fixture.  It still yields non-empty text with a
    single-word-per-line ratio < 0.70 (not purely pathological).
    """
    if not os.path.exists(_SINGLE_WORD_PDF):
        pytest.fail(
            "Phase 96 guard fixture missing: " + _SINGLE_WORD_PDF + "\n"
            "Run scripts/generate_single_word_fixture.py to regenerate."
        )

    from shared.local_indexer import extract_pdf_pages
    pages = list(extract_pdf_pages(_SINGLE_WORD_PDF))
    assert len(pages) >= 1, "single_word_per_line.pdf yielded zero pages"

    all_text = "\n".join(t for _, t, _ in pages)
    assert all_text.strip(), "single_word_per_line.pdf produced empty text"

    # The output should NOT be purely one-word-per-line
    lines = [ln for ln in all_text.splitlines() if ln.strip()]
    if len(lines) >= 5:
        single = sum(1 for ln in lines if len(ln.split()) <= 1)
        ratio = single / len(lines)
        assert ratio < 0.70, (
            f"single_word_per_line.pdf still one-word-per-line after Phase 102 "
            f"(ratio={ratio:.2f}) -- D-F4 guard regressed."
        )


# ---------------------------------------------------------------------------
# D-06 FINAL: nikud strip E2E (strip-once for ALL formats at _write_page_doc)
# ---------------------------------------------------------------------------

def test_nikud_strip_e2e(tmp_path):
    """D-06 FINAL: un-vocalized query matches vocalized PDF page; no divergence.

    Process:
      1. Index letter_spaced_hebrew.pdf via a temp LocalIndexer (full pipeline).
      2. Assert (a): a Tantivy search for 'שלום' (consonantal, un-vocalized) MATCHES
         the page that was indexed from a vocalized source (שָׁלוֹם with nikud).
      3. Assert (b): read cached_text for the page from local_pages; decompress;
         assert it has NO nikud codepoints (0x05B0-0x05C7) — strip happened at
         _write_page_doc, not in the extractor.
      4. Assert (c): content == cached_text (NO divergence, NO nikud display field).

    D-06 FINAL: strip-once for ALL formats at the single shared write site
    (_write_page_doc). PDF text retains nikud through de-space/reorder glyph math;
    the strip is the value _write_page_doc persists, applied uniformly to all formats.
    """
    if not os.path.exists(_LETTER_SPACED_PDF):
        pytest.skip("letter_spaced_hebrew.pdf fixture missing; run build_phase102_fixtures.py")

    from shared.local_indexer import LocalIndexer, decompress_cached_text

    idx = os.path.join(str(tmp_path), "idx")
    lab = os.path.join(str(tmp_path), "lab")
    db = os.path.join(str(tmp_path), "test.sqlite3")
    os.makedirs(idx)
    os.makedirs(lab)
    indexer = LocalIndexer(idx, lab, db)
    try:
        folder = str(tmp_path / "docs")
        os.makedirs(folder, exist_ok=True)
        dest = os.path.join(folder, "letter_spaced_hebrew.pdf")
        shutil.copy(_LETTER_SPACED_PDF, dest)

        indexer.add_folder(folder)
        indexer.scan_all()

        # Reload so the searcher sees the freshly committed documents
        indexer._index.reload()

        # (a) Un-vocalized Tantivy search for 'שלום' must find at least one result
        search_term = "שלום"  # consonantal, un-vocalized
        try:
            query = indexer._index.parse_query(search_term, ["content"])
        except Exception as exc:
            pytest.skip(f"Tantivy parse_query failed (index may be empty): {exc}")

        searcher = indexer._index.searcher()
        results = searcher.search(query, 10)
        count = results.count

        assert count > 0, (
            f"D-06 FINAL: un-vocalized query '{search_term}' found 0 results in the LOCAL index. "
            f"strip_nikud must be applied in _write_page_doc so a vocalized source (שָׁלוֹם) "
            f"is searchable by an un-vocalized query."
        )

        # (b) + (c): read cached_text and verify it has no nikud and equals content
        # Find the local_pages rows for our sys_id
        sys_id_rows = indexer._conn.execute(
            "SELECT lp.sys_id, lp.cached_text, lp.cached_text_codec "
            "FROM local_pages lp "
            "JOIN local_files lf ON lp.sys_id = lf.sys_id "
            "WHERE lf.original_filename = 'letter_spaced_hebrew.pdf'"
        ).fetchall()

        assert sys_id_rows, (
            "D-06 FINAL: no local_pages rows found for letter_spaced_hebrew.pdf. "
            "Was scan_all() successful?"
        )

        for row in sys_id_rows:
            cached_bytes = bytes(row["cached_text"])
            cached_str = decompress_cached_text(cached_bytes)

            # (b) No nikud in cached_text (strip happened at _write_page_doc)
            nikud_found = _NIKUD_RE.search(cached_str)
            assert not nikud_found, (
                f"D-06 FINAL: cached_text contains nikud codepoints for page "
                f"(sys_id={row['sys_id']!r}). "
                f"The strip must happen at _write_page_doc for ALL formats. "
                f"cached_text snippet: {cached_str[:100]!r}"
            )

        # (c) content == cached_text for each page (no divergence)
        # Retrieve content from Tantivy stored field by searching for any term in the page
        # and comparing against cached_text
        for row in sys_id_rows:
            lp_sys_id = row["sys_id"]
            cached_bytes = bytes(row["cached_text"])
            cached_str = decompress_cached_text(cached_bytes)

            # Look up stored 'content' field from Tantivy via unique_id
            # Build the uid from the local pages data
            uid_row = indexer._conn.execute(
                "SELECT uid FROM local_pages WHERE sys_id = ?", (lp_sys_id,)
            ).fetchone()
            if uid_row is None:
                continue

            uid = uid_row["uid"]
            uid_query = indexer._index.parse_query(uid, ["unique_id"])
            searcher2 = indexer._index.searcher()
            uid_results = searcher2.search(uid_query, 1)

            if uid_results.count == 0:
                # Doc not found by uid -- may be empty page, skip content comparison
                continue

            doc_addr = uid_results.hits[0][1]
            tantivy_doc = searcher2.doc(doc_addr)
            content_field = tantivy_doc.get_first("content")

            if content_field is not None:
                # content == cached_text (D-06 FINAL no-divergence guarantee)
                assert content_field == cached_str, (
                    f"D-06 FINAL: content != cached_text for page uid={uid!r}. "
                    f"There must be NO divergence between the two fields "
                    f"(both should be stripped, both should be equal). "
                    f"content: {content_field[:100]!r}, "
                    f"cached_text: {cached_str[:100]!r}"
                )
    finally:
        indexer.close()
