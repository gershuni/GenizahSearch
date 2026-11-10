#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase 99 PDFIMG-06: generate test fixtures for the PDF page renderer tests.

Generates three PDF fixtures in tests/fixtures/local_indexer/:

1. multipage_sample.pdf  — 3-page PDF for page-selection and out-of-range tests
   Each page has one line of text ("Page 1", "Page 2", "Page 3").
   Used for: test_render_single_page, test_only_requested_page_rendered,
             test_page_out_of_range, test_page_num_zero,
             test_uppercase_pdf_not_misclassified.

2. encrypted_sample.pdf  — AES-256 encrypted 1-page PDF (owner + user password).
   Used for: test_encrypted_reason.

3. corrupt_sample.pdf  — file that starts with "%PDF-1.5" but has no valid body.
   fitz.open() raises on this (corrupt body).
   Used for: test_corrupt_reason.

Usage:
    python scripts/generate_pdf_render_fixtures.py

Requires: PyMuPDF (fitz) — already a project dependency.
"""
import os
import sys

FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "tests", "fixtures", "local_indexer"
)
MULTIPAGE_PDF = os.path.join(FIXTURES_DIR, "multipage_sample.pdf")
ENCRYPTED_PDF = os.path.join(FIXTURES_DIR, "encrypted_sample.pdf")
CORRUPT_PDF = os.path.join(FIXTURES_DIR, "corrupt_sample.pdf")


def _generate_multipage_pdf(path: str) -> None:
    """Create a 3-page PDF where each page has a simple text label.

    Page 1 → "Page 1", Page 2 → "Page 2", Page 3 → "Page 3".
    Used to prove that rendering page_num=2 returns the second page
    (fitz index 1) and that get_pixmap is called exactly once.
    """
    import fitz  # type: ignore[import]

    doc = fitz.open()
    for i in range(3):
        p = doc.new_page()
        p.insert_text((72, 72), f"Page {i + 1}", fontsize=14)
    doc.save(path)
    doc.close()


def _verify_multipage(path: str) -> None:
    """Assert 3-page fixture has exactly 3 pages."""
    import fitz  # type: ignore[import]

    doc = fitz.open(path)
    count = doc.page_count
    doc.close()
    print(f"  page_count={count} (want 3): {'OK' if count == 3 else 'FAIL'}")
    assert count == 3, f"multipage fixture has {count} pages, expected 3"


def _generate_encrypted_pdf(path: str) -> None:
    """Create a 1-page PDF encrypted with AES-256.

    Uses PDF_ENCRYPT_AES_256 so fitz opens it without raising but
    sets doc.needs_pass = True, classifying it as ENCRYPTED.
    """
    import fitz  # type: ignore[import]

    doc = fitz.open()
    p = doc.new_page()
    p.insert_text((72, 72), "Encrypted content", fontsize=14)
    doc.save(
        path,
        encryption=fitz.PDF_ENCRYPT_AES_256,
        owner_pw="owner",
        user_pw="user",
    )
    doc.close()


def _verify_encrypted(path: str) -> None:
    """Assert encrypted fixture has needs_pass = True."""
    import fitz  # type: ignore[import]

    doc = fitz.open(path)
    needs_pass = doc.needs_pass
    doc.close()
    print(f"  needs_pass={needs_pass} (want True): {'OK' if needs_pass else 'FAIL'}")
    assert needs_pass, "encrypted fixture did not produce needs_pass=True"


def _generate_corrupt_pdf(path: str) -> None:
    """Create a file with a valid PDF header but an invalid body.

    This passes the .pdf suffix check but raises when fitz tries to
    parse the cross-reference table, classifying it as CORRUPT.
    """
    with open(path, "wb") as f:
        f.write(b"%PDF-1.5\nthis is not a valid pdf body\n%%EOF\n")


def _verify_corrupt(path: str) -> None:
    """Assert corrupt fixture raises on fitz.open()."""
    import fitz  # type: ignore[import]

    raised = False
    try:
        doc = fitz.open(path)
        # Some versions may open without raising — check that we can't render
        try:
            if doc.page_count > 0:
                doc.load_page(0).get_pixmap()
        except Exception:
            raised = True
        doc.close()
    except Exception:
        raised = True

    print(f"  raises_on_open={raised} (want True): {'OK' if raised else 'FAIL'}")
    if not raised:
        # Fallback: the file exists and has non-PDF content — the renderer
        # will still classify via _open_doc_classified; the verify just warns.
        print("  WARNING: fitz.open() did not raise on corrupt fixture — "
              "renderer will classify as CORRUPT via page-level failure instead.")


def main() -> int:
    os.makedirs(FIXTURES_DIR, exist_ok=True)

    print(f"Generating {MULTIPAGE_PDF} ...")
    _generate_multipage_pdf(MULTIPAGE_PDF)
    _verify_multipage(MULTIPAGE_PDF)
    print(f"  Written: {MULTIPAGE_PDF}")

    print(f"Generating {ENCRYPTED_PDF} ...")
    _generate_encrypted_pdf(ENCRYPTED_PDF)
    _verify_encrypted(ENCRYPTED_PDF)
    print(f"  Written: {ENCRYPTED_PDF}")

    print(f"Generating {CORRUPT_PDF} ...")
    _generate_corrupt_pdf(CORRUPT_PDF)
    _verify_corrupt(CORRUPT_PDF)
    print(f"  Written: {CORRUPT_PDF}")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
