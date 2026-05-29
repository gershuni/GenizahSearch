#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase 102 -- reproducible fixture builder for PDF extraction E2E tests.

Generates three fixture PDFs into tests/fixtures/local_indexer/:

  1. letter_spaced_hebrew.pdf  (F-D/F-E/F-F/D-06 fixture)
     Hebrew text for end-to-end RTL extraction quality testing.

     **RTL layout convention:** each Hebrew word is inserted as the REVERSED
     NFC string so PyMuPDF places the first logical letter at the HIGHEST x
     (rightmost), matching real Hebrew PDF layout.  ``_order_unit_text_rtl``
     (descending center-x) then recovers the correct logical Unicode order.

     Each word is a single ``insert_text`` call (one PDF text object / span)
     so the LTR-damage guard (D-03, 0.70 threshold) sees the SAME token count
     from both the rawdict and blocks extraction paths (ratio ~1.0).

     Failure modes exercised:
       - F-D: single-letter Hebrew token ratio = 0.0 after full pipeline.
       - F-E: "מלחמת" present in extracted text (correct RTL word order).
       - F-F: "אבן תיכון" present (words in wrong emission order, reorder fixes).
       - D-06 FINAL: "שָׁלוֹם" (vocalized) stripped to consonantal "שלום";
              un-vocalized query "שלום" must match the indexed page.

  2. ltr_latin_noregress.pdf  (LTR/Latin no-regression pin)
     Clean Latin text ("Northwest Semitic Dictionary" + paragraph) that the
     pre-Phase-102 blocks path handled well.  The Phase 102 rawdict pipeline
     must not regress: phrase present un-scrambled, token count >= 95%.

  3. corrupt_encoding_sample.pdf  (F-G / D-F16 fixture)
     A PDF whose content stream contains C1 control bytes (0x81-0x9F);
     PyMuPDF extracts them verbatim (no ToUnicode mapping in Helvetica).
     _detect_corrupt_encoding() returns True (garbage_ratio > 5%) and the
     file-level >=50% corrupt-page decision yields status='corrupt_encoding'.

Provenance (Codex LOW-11): committing the builder alongside the binary PDFs
allows regeneration after a PyMuPDF upgrade changes output.

Usage::

    python tests/scripts/build_phase102_fixtures.py

Idempotent: regenerates fixtures in place.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unicodedata

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import fitz  # PyMuPDF  # noqa: E402

_FIXTURES_DIR = os.path.join(_REPO_ROOT, "tests", "fixtures", "local_indexer")

_HEBREW_FONT_CANDIDATES = [
    "C:/Windows/Fonts/arial.ttf",
    "C:/Windows/Fonts/arialuni.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
]
_HEBREW_FONT_PATH: str | None = None
for _fp in _HEBREW_FONT_CANDIDATES:
    if os.path.isfile(_fp):
        _HEBREW_FONT_PATH = _fp
        break


def _require_hebrew_font() -> str:
    if _HEBREW_FONT_PATH is None:
        raise RuntimeError(
            "No Hebrew-capable font found (Arial/FreeFont). "
            "Install the font and re-run."
        )
    return _HEBREW_FONT_PATH


def _insert_rtl_word(
    page,
    word: str,
    x: float,
    y: float,
    font_path: str,
    fontsize: float = 14.0,
    font_alias: str = "heb",
) -> None:
    """Insert a Hebrew word with correct RTL physical layout.

    Inserts the REVERSED NFC-normalised string so the physical x positions are:
      - first logical char at HIGHEST x (rightmost visual position)
      - last logical char at LOWEST x (leftmost visual position)

    ``_order_unit_text_rtl`` (descending center-x) then recovers the correct
    logical Unicode order (matching what a real Hebrew PDF viewer produces).

    The ``x`` parameter is the LEFT edge of the REVERSED string as PyMuPDF
    places it; the rightmost glyph ends up at approximately ``x + len(word)*0.6*fs``.
    """
    reversed_nfc = unicodedata.normalize("NFC", word)[::-1]
    page.insert_text(
        fitz.Point(x, y),
        reversed_nfc,
        fontsize=fontsize,
        fontfile=font_path,
        fontname=font_alias,
        color=(0, 0, 0),
    )


# ---------------------------------------------------------------------------
# 1. letter_spaced_hebrew.pdf
# ---------------------------------------------------------------------------
def build_letter_spaced_hebrew(dest: str) -> None:
    """Build the letter-spaced Hebrew fixture (F-D/F-E/F-F/D-06).

    See module docstring for the design rationale.
    """
    font_path = _require_hebrew_font()
    fa = "heb"
    fs = 14.0

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)

    # -------------------------------------------------------------------------
    # Line 1 (F-D/F-E): four Hebrew words at widely spaced x positions.
    # "מלחמת" is at rightmost (x=390), "משה" at leftmost (x=50).
    # All four words in the CORRECT RTL visual layout (first letter rightmost).
    # Single-letter token ratio = 0.0 (each word is a multi-letter span).
    # F-E: "מלחמת" must appear in the extracted text after pipeline.
    # -------------------------------------------------------------------------
    _insert_rtl_word(page, "מלחמת", 390, 100, font_path, fs, fa)
    _insert_rtl_word(page, "כוש", 280, 100, font_path, fs, fa)
    _insert_rtl_word(page, "של", 200, 100, font_path, fs, fa)
    _insert_rtl_word(page, "משה", 110, 100, font_path, fs, fa)

    # -------------------------------------------------------------------------
    # Line 2: Latin anchor text (LTR -- not de-spaced by RTL pipeline).
    # Keeps rawdict/blocks token ratio close to 1.0 so the D-03 guard does
    # not fall back to blocks (which would undo any RTL reorder benefit).
    # -------------------------------------------------------------------------
    page.insert_text(
        fitz.Point(50, 140),
        "Hebrew scholarship text reference",
        fontsize=12,
        color=(0, 0, 0),
    )

    # -------------------------------------------------------------------------
    # Line 3 (F-F): running-header "תיכון אבן" in WRONG emission order.
    #
    # "תיכון" emitted FIRST at lower x (x=70, leftmost physical position).
    # "אבן"   emitted SECOND at higher x (x=270, rightmost physical position).
    #
    # RTL reading (right->left): "אבן" (rightmost) is read FIRST.
    # Correct reading order: "אבן תיכון".
    #
    # reorder_word_units_rtl detects the FORWARD center-x jump from
    # unit-0 (תיכון, original_order 0-4, low x) to unit-1 (אבן, high x),
    # creates two segments, and sorts descending by max center-x ->
    # [אבן, תיכון] = "אבן תיכון" (correct).
    # -------------------------------------------------------------------------
    y3 = 175.0
    _insert_rtl_word(page, "תיכון", 70, y3, font_path, fs, fa)   # low x (wrong visual)
    _insert_rtl_word(page, "אבן", 270, y3, font_path, fs, fa)    # high x (wrong visual)

    # -------------------------------------------------------------------------
    # Line 4 (D-06 FINAL): vocalized "שָׁלוֹם" for nikud-strip E2E test.
    # Inserted reversed (as NFC[::-1]) for correct RTL physical layout.
    # _order_unit_text_rtl recovers "שָׁלוֹם" (logical order with nikud).
    # _write_page_doc applies strip_nikud -> "שלום" stored in Tantivy content.
    # An un-vocalized query "שלום" must match the indexed page.
    # -------------------------------------------------------------------------
    _insert_rtl_word(page, "שָׁלוֹם", 50, 210, font_path, fs, fa)

    doc.save(dest)
    doc.close()
    print(f"Built: {dest}")


# ---------------------------------------------------------------------------
# 2. ltr_latin_noregress.pdf
# ---------------------------------------------------------------------------
def build_ltr_latin_noregress(dest: str) -> None:
    """Generate a clean Latin-text PDF for the LTR no-regression pin."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)

    text_lines = [
        "Northwest Semitic Dictionary",
        "",
        "The Northwest Semitic languages include Ugaritic, Phoenician,",
        "Hebrew, Aramaic, and their related dialects. This dictionary",
        "provides comprehensive lexical coverage of epigraphic sources",
        "dating from the second millennium BCE through the Hellenistic period.",
        "",
        "Key features:",
        "  - Extensive cross-referencing across dialects",
        "  - Etymology from Proto-Semitic roots",
        "  - Cuneiform and alphabetic script attestations",
        "  - Bibliography of primary sources and modern scholarship",
        "",
        "This reference work is essential for students of ancient Near",
        "Eastern languages, biblical scholars, and comparative linguists",
        "who require a reliable resource for lexical analysis.",
    ]

    y = 80.0
    for line in text_lines:
        page.insert_text(fitz.Point(50, y), line, fontsize=12, color=(0, 0, 0))
        y += 18.0

    doc.save(dest)
    doc.close()
    print(f"Built: {dest}")


# ---------------------------------------------------------------------------
# 3. corrupt_encoding_sample.pdf
# ---------------------------------------------------------------------------
def build_corrupt_encoding_sample(dest: str) -> None:
    """Generate a PDF with C1-control garbage text (F-G / D-F16 fixture).

    Technique:
      1. Create a PDF with a real content stream xref (via insert_text anchor).
      2. Replace the stream with raw bytes in the C1 control range (0x81-0x9F);
         PyMuPDF extracts these verbatim when Helvetica has no ToUnicode entry.

    Result: garbage_ratio > 0.05 -> _detect_corrupt_encoding returns True;
    >=50% pages trigger -> _extract_and_write_pdf returns status='corrupt_encoding'.
    """
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        doc = fitz.open()
        page = doc.new_page(width=595, height=842)
        page.insert_text(fitz.Point(50, 400), "placeholder", fontsize=10)
        doc.save(tmp_path)
        doc.close()

        doc = fitz.open(tmp_path)
        page = doc[0]
        contents_xrefs = page.get_contents()

        c1_bytes = bytes(range(0x81, 0xA0)) * 12  # 300 C1 control chars
        raw_stream = (
            b"BT\n"
            b"/Helvetica 10 Tf\n"
            b"50 700 Td\n"
            b"(" + c1_bytes[:100] + b") Tj\n"
            b"50 -20 Td\n"
            b"(" + c1_bytes[100:200] + b") Tj\n"
            b"50 -20 Td\n"
            b"(" + c1_bytes[200:300] + b") Tj\n"
            b"ET\n"
        )
        if contents_xrefs:
            doc.update_stream(contents_xrefs[0], raw_stream)
        else:
            xref = doc.get_new_xref()
            doc.update_stream(xref, raw_stream)
            page._set_contents([xref])  # type: ignore[attr-defined]

        doc.save(dest)
        doc.close()
        print(f"Built: {dest}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _verify_corrupt_produces_garbage(dest: str) -> bool:
    try:
        from shared.local_indexer import _detect_corrupt_encoding, extract_pdf_pages
        pages = list(extract_pdf_pages(dest))
        if not pages:
            return False
        return _detect_corrupt_encoding("".join(t for _, t, _ in pages))
    except Exception as e:
        print(f"  [verify] exception: {e}")
        return False


def _verify_letter_spaced(dest: str) -> bool:
    try:
        from shared.local_indexer import extract_pdf_pages
        pages = list(extract_pdf_pages(dest))
        if not pages:
            print("  [verify] no pages extracted")
            return False
        all_text = "".join(t for _, t, _ in pages)
        has_word = "מלחמת" in all_text
        tokens = [t for t in all_text.split() if t.strip()]
        heb_tokens = [
            t for t in tokens
            if t and all(
                0x0590 <= ord(c) <= 0x05FF or 0xFB1D <= ord(c) <= 0xFB4F
                or 0x05B0 <= ord(c) <= 0x05C7
                for c in t
            )
        ]
        single = [t for t in heb_tokens if len(t) == 1]
        ratio = len(single) / len(heb_tokens) if heb_tokens else 0
        print(f"  [verify] מלחמת in text: {has_word}; single-letter ratio: {ratio:.3f}")
        return has_word and ratio < 0.50
    except Exception as e:
        print(f"  [verify] exception: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    os.makedirs(_FIXTURES_DIR, exist_ok=True)

    dest_letter = os.path.join(_FIXTURES_DIR, "letter_spaced_hebrew.pdf")
    dest_ltr = os.path.join(_FIXTURES_DIR, "ltr_latin_noregress.pdf")
    dest_corrupt = os.path.join(_FIXTURES_DIR, "corrupt_encoding_sample.pdf")

    print("Building Phase 102 fixture PDFs...")
    print(f"Output directory: {_FIXTURES_DIR}")
    print(f"Hebrew font: {_HEBREW_FONT_PATH or 'NOT FOUND'}")
    print()

    build_letter_spaced_hebrew(dest_letter)
    ok = _verify_letter_spaced(dest_letter)
    print(f"  [{'OK' if ok else 'WARN'}] letter_spaced_hebrew.pdf")

    build_ltr_latin_noregress(dest_ltr)
    print(f"  [OK] ltr_latin_noregress.pdf")

    build_corrupt_encoding_sample(dest_corrupt)
    ok = _verify_corrupt_produces_garbage(dest_corrupt)
    print(f"  [{'OK' if ok else 'WARN'}] corrupt_encoding_sample.pdf "
          f"{'triggers detector' if ok else 'DOES NOT trigger detector'}")

    print()
    print("Done. Fixtures built:")
    for path in [dest_letter, dest_ltr, dest_corrupt]:
        size = os.path.getsize(path) if os.path.exists(path) else 0
        print(f"  {os.path.basename(path)}: {size} bytes")


if __name__ == "__main__":
    main()
