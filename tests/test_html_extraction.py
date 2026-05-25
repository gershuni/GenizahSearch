# -*- coding: utf-8 -*-
"""Phase 97 Wave C — F-01 HTML extraction tests.

Tests:
- test_semantic_chunking_at_h1_h2: 5 h2 + 30 paragraphs -> 5 chunks with '§' locators
- test_fallback_to_20_paragraph_chunks: 1 h1 + 60 paragraphs (sparse) -> 3 chunks '¶ N-M'
- test_rtl_logical_order_preserved: Hebrew logical-order text round-trips unchanged
- test_encoding_chain: cp1255 bytes with no meta charset -> recovered Hebrew text
"""
from __future__ import annotations

import pathlib

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "local_indexer"


def test_semantic_chunking_at_h1_h2(tmp_path):
    """HTML with 5 h2 + 30 paragraphs (6 per section) should yield 5 chunks,
    each with a '§ ' chunk_locator.
    """
    from shared.local_indexer import extract_html_pages

    # Build HTML: 5 h2 headings, 6 paragraphs each = 30 paragraphs total.
    body = ""
    for i in range(1, 6):
        body += f"<h2>פרק {i}</h2>\n"
        for j in range(1, 7):
            body += f"<p>פסקה {i}.{j}: טקסט לדוגמה לצורכי בדיקה.</p>\n"
    html = (
        "<!DOCTYPE html><html lang='he' dir='rtl'><head><meta charset='utf-8'>"
        f"<title>מבחן</title></head><body>{body}</body></html>"
    )
    p = tmp_path / "test_semantic.html"
    p.write_bytes(html.encode("utf-8"))

    chunks = list(extract_html_pages(str(p)))
    # Should yield 5 chunks, one per h2
    assert len(chunks) == 5, f"Expected 5 chunks, got {len(chunks)}"
    # Each chunk is a 5-tuple (chunk_num, text, title, locator, is_rtl)
    for chunk_num, text, title, locator, is_rtl in chunks:
        assert locator.startswith("§ "), f"Expected '§ ' locator, got {locator!r}"
        assert text.strip(), "Chunk text should not be empty"


def test_fallback_to_20_paragraph_chunks(tmp_path):
    """HTML with 1 h1 + 60 paragraphs (sparse: avg_inter = 60 >> 5 but only 1 heading)
    should fall back to 20-paragraph windows -> 3 chunks with '¶ N-M' locators.
    """
    from shared.local_indexer import extract_html_pages

    # 1 h1 (sparse = fewer than 3 headings) + 60 p -> fallback to 20-para windows
    body = "<h1>כותרת ראשית</h1>\n"
    for i in range(1, 61):
        body += f"<p>פסקה {i}: תוכן לבדיקת חלוקה לקטעים.</p>\n"
    html = (
        "<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>"
        f"<title>בדיקה</title></head><body>{body}</body></html>"
    )
    p = tmp_path / "test_fallback.html"
    p.write_bytes(html.encode("utf-8"))

    chunks = list(extract_html_pages(str(p)))
    # 60 paragraphs / 20 per window = 3 chunks (last one has 20 too)
    assert len(chunks) == 3, f"Expected 3 chunks, got {len(chunks)}"
    for chunk_num, text, title, locator, is_rtl in chunks:
        assert locator.startswith("¶ "), f"Expected '¶ ' locator, got {locator!r}"


def test_rtl_logical_order_preserved(tmp_path):
    """Hebrew logical-order text must survive extraction unchanged (F-06).

    The string 'שלום עולם' in the HTML must appear character-for-character
    in the extracted text. The reversed string must NOT appear.
    """
    from shared.local_indexer import extract_html_pages

    html = (
        "<!DOCTYPE html><html lang='he' dir='rtl'><head><meta charset='utf-8'>"
        "<title>RTL test</title></head><body>"
        "<h2>ברכה</h2><p>שלום עולם</p>"
        "<h2>שלום</h2><p>עוד טקסט לבדיקה כאן.</p>"
        "<h2>שלישי</h2><p>פסקה אחרונה.</p>"
        "</body></html>"
    )
    p = tmp_path / "rtl_test.html"
    p.write_bytes(html.encode("utf-8"))

    chunks = list(extract_html_pages(str(p)))
    all_text = " ".join(text for _, text, _, _, _ in chunks)

    assert "שלום עולם" in all_text, (
        f"Expected logical-order Hebrew 'שלום עולם' in extracted text, got: {all_text!r}"
    )
    # Reversed string must NOT appear (that would indicate _fix_rtl_* corruption)
    reversed_str = "שלום עולם"[::-1]
    assert reversed_str not in all_text, (
        f"Reversed string {reversed_str!r} found — RTL corruption detected!"
    )
    # is_rtl flag should be True for html dir=rtl
    for _, _, _, _, is_rtl in chunks:
        assert is_rtl is True, "Expected is_rtl=True for <html dir='rtl'>"


def test_encoding_chain(tmp_path):
    """HTML with cp1255 bytes and no meta charset: extractor recovers Hebrew text."""
    from shared.local_indexer import extract_html_pages

    # Write a minimal HTML in cp1255 encoding (no charset meta)
    # 'שלום' in cp1255: \xf9\xec\xe5\xed
    html_body = b"<html><head><title>test</title></head><body>"
    html_body += b"<h2>section A</h2><p>"
    html_body += "שלום".encode("cp1255")
    html_body += b"</p>"
    html_body += b"<h2>section B</h2><p>hello</p>"
    html_body += b"<h2>section C</h2><p>test</p>"
    html_body += b"</body></html>"
    p = tmp_path / "cp1255_sample.html"
    p.write_bytes(html_body)

    chunks = list(extract_html_pages(str(p)))
    assert len(chunks) > 0, "Expected at least one chunk from cp1255 HTML"
    all_text = " ".join(text for _, text, _, _, _ in chunks)
    # Should contain the recovered Hebrew word
    assert "שלום" in all_text, f"Expected 'שלום' in extracted text, got: {all_text!r}"
