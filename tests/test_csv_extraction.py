# -*- coding: utf-8 -*-
"""Phase 97 Wave C — F-03 + F-05 CSV extraction tests.

Tests:
- test_per_200_row_chunking: 500-row CSV -> 3 chunks with 'rows N-M' locators
- test_encoding_chain: cp1255 CSV -> recovered Hebrew text via 2nd encoding
- test_delimiter_detection: semicolon-delimited CSV -> Sniffer picks ';'
- test_uniform_row_extraction: row ["שלום", "hello", "123"] -> text "שלום | hello | 123"
- test_encoding_total_failure_raises: non-decodable bytes -> EncodingError
"""
from __future__ import annotations

import csv
import io
import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "local_indexer"


def test_per_200_row_chunking(tmp_path):
    """500-row CSV should yield 3 chunks with locators 'rows 1-200', 'rows 201-400', 'rows 401-500'."""
    from shared.local_indexer import extract_csv_pages

    # Create exactly 500 rows (no header) with UTF-8-BOM.
    # F-04: no header-row assumption, so a header would count as row 1.
    # To get clean locators 1-200/201-400/401-500 we use exactly 500 data rows.
    lines = []
    for i in range(1, 501):
        lines.append(f"val{i}a,val{i}b,val{i}c")
    content = "\n".join(lines) + "\n"
    p = tmp_path / "large.csv"
    p.write_text(content, encoding="utf-8-sig")

    chunks = list(extract_csv_pages(str(p)))
    assert len(chunks) == 3, f"Expected 3 chunks, got {len(chunks)}"

    locators = [loc for _, _, _, loc in chunks]
    assert locators[0] == "rows 1-200", f"Expected 'rows 1-200', got {locators[0]!r}"
    assert locators[1] == "rows 201-400", f"Expected 'rows 201-400', got {locators[1]!r}"
    assert locators[2] == "rows 401-500", f"Expected 'rows 401-500', got {locators[2]!r}"


def test_encoding_chain(tmp_path):
    """CSV in cp1255 (no BOM) should be recovered correctly via 2nd encoding in chain."""
    from shared.local_indexer import extract_csv_pages

    # Build a cp1255 CSV with Hebrew text
    rows = [["שם", "ערך"], ["שלום", "עולם"], ["בדיקה", "טקסט"]]
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow(row)
    content_str = buf.getvalue()
    p = tmp_path / "cp1255.csv"
    p.write_bytes(content_str.encode("cp1255"))

    chunks = list(extract_csv_pages(str(p)))
    assert len(chunks) > 0, "Expected at least one chunk from cp1255 CSV"
    all_text = " ".join(text for _, text, _, _ in chunks)
    assert "שלום" in all_text, f"Expected 'שלום' in extracted text, got: {all_text!r}"


def test_delimiter_detection(tmp_path):
    """Semicolon-delimited CSV -> Sniffer picks ';' and rows split correctly."""
    from shared.local_indexer import extract_csv_pages

    rows_data = [["שם", "ערך", "תיאור"]]
    for i in range(1, 11):
        rows_data.append([f"שם{i}", f"ערך{i}", f"תיאור{i}"])
    # Write with semicolon delimiter, UTF-8-BOM
    lines = [";".join(row) for row in rows_data]
    content = "\n".join(lines) + "\n"
    p = tmp_path / "semicolon.csv"
    p.write_text(content, encoding="utf-8-sig")

    chunks = list(extract_csv_pages(str(p)))
    assert len(chunks) > 0, "Expected at least one chunk"
    all_text = " ".join(text for _, text, _, _ in chunks)
    # Row text should contain " | " separators (F-04 uniform extraction)
    assert " | " in all_text, f"Expected ' | ' separator in text, got: {all_text!r}"
    # Should NOT contain raw semicolons (delimiter was consumed by reader)
    assert ";" not in all_text, f"Unexpected semicolons in text — delimiter not consumed: {all_text!r}"


def test_uniform_row_extraction(tmp_path):
    """Row [שלום, hello, 123] -> chunk text contains 'שלום | hello | 123'."""
    from shared.local_indexer import extract_csv_pages

    content = "שלום,hello,123\n"
    p = tmp_path / "uniform.csv"
    p.write_text(content, encoding="utf-8-sig")

    chunks = list(extract_csv_pages(str(p)))
    assert len(chunks) > 0, "Expected at least one chunk"
    all_text = " ".join(text for _, text, _, _ in chunks)
    assert "שלום | hello | 123" in all_text, (
        f"Expected 'שלום | hello | 123' in text, got: {all_text!r}"
    )


def test_encoding_total_failure_raises(tmp_path):
    """CSV that is neither utf-8-sig, cp1255, nor utf-16-le raises EncodingError."""
    from shared.local_indexer import extract_csv_pages, EncodingError

    # Write bytes that are invalid in all three encodings:
    # UTF-16-LE BOM-less but random high bytes that cp1255 would silently accept...
    # Better approach: use bytes that are invalid in utf-8-sig and cp1255 but
    # that when decoded as utf-16-le produce garbage that triggers a decode error.
    # Actually cp1255 accepts almost any byte sequence (it's a single-byte encoding).
    # The only way to truly fail all three is to trigger a UnicodeDecodeError in each.
    # Use bytes with invalid UTF-16-LE sequence AND invalid cp1255 doesn't exist since
    # cp1255 maps all 256 byte values.
    #
    # Per the spec: "CSV that is neither utf-8-sig nor cp1255 nor utf-16-le -> raises EncodingError"
    # In practice, cp1255 maps all bytes, so we need a file that makes the sniffer/reader fail
    # OR we test that a file with clearly broken encoding raises the error.
    #
    # The real test: patch _CSV_ENCODINGS to a set of encodings that all fail.
    import unittest.mock as mock

    # Write bytes that cause UnicodeDecodeError with utf-8-sig (invalid continuation)
    # We need bytes that fail ALL encodings in _CSV_ENCODINGS.
    # Since cp1255 accepts all bytes, we mock _CSV_ENCODINGS to use strict encodings that fail.
    invalid_bytes = bytes([0xFF, 0xFE, 0x00])  # invalid in ascii/utf-8

    p = tmp_path / "bad.csv"
    p.write_bytes(invalid_bytes)

    # Test with a patched encoding list that will all fail
    with mock.patch("shared.local_indexer._CSV_ENCODINGS", ("utf-8",)):
        with pytest.raises(EncodingError):
            list(extract_csv_pages(str(p)))
