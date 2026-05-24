#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Phase 96 D-F4: generate test fixtures for the detect-then-fallback PDF tests.

Generates two PDF fixtures in tests/fixtures/local_indexer/:

1. single_word_per_line.pdf  — PATHOLOGICAL fixture
   Each word is placed at a unique x-position on visually the same line,
   but written in scrambled content-stream order (column 2 before column 1,
   right-to-left within each line group). Result:
     - get_text("blocks") follows content-stream order → one-word-per-block
       (ratio = 1.0, pathological)
     - get_text("text", sort=True) sorts by (y, x) → multi-word lines
       (ratio = 0.0, recovered)

2. clean_sample.pdf  — CLEAN control fixture
   Normal paragraph text written via page.insert_text() in sequential order.
   Each insert_text() call writes a full sentence in one operation.
   Result:
     - get_text("blocks") → multi-word blocks (ratio = 0.0, clean)
     - The detect-then-fallback heuristic does NOT fire
     - Used for test_good_pdf_does_not_invoke_fallback_mode (Codex MEDIUM #8)

Usage:
    python scripts/generate_single_word_fixture.py

Requires: PyMuPDF (fitz) — already a project dependency.
"""
import os
import sys

FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "tests", "fixtures", "local_indexer"
)
SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")
CLEAN_PDF = os.path.join(FIXTURES_DIR, "clean_sample.pdf")


def _generate_pathological_pdf(path: str) -> None:
    """Create a PDF where blocks-mode is pathological (ratio 1.0) but
    sort=True is clean (ratio 0.0).

    Technique: place 5 words per logical line at distinct x-positions but
    same y-coordinate. Write them in reverse (right-to-left) content-stream
    order within each line group. PyMuPDF's blocks mode respects content-stream
    order when words don't share a bounding box → one word per block. sort=True
    reorders by (y, x) → all words on the same y group into one visual line.
    """
    import fitz  # type: ignore[import]

    # 5 lines of 5 words each; x-positions are well-separated (no bbox overlap)
    lines = [
        [("The",      50), ("quick",   100), ("brown",   155), ("fox",    205), ("jumps",  240)],
        [("over",     50), ("the",      95), ("lazy",    130), ("dog",    168), ("today",  200)],
        [("Lorem",    50), ("ipsum",   110), ("dolor",   160), ("sit",    200), ("amet",   240)],
        [("consectetur", 50), ("adipiscing", 165), ("elit", 255), ("sed", 286), ("do", 318)],
        [("eiusmod",  50), ("tempor",  125), ("incididunt", 185), ("ut",  275), ("labore", 298)],
    ]
    y_base = 100
    y_step = 30

    doc = fitz.open()
    page = doc.new_page()

    # Write in scrambled content-stream order:
    # reverse each line's word order (right-to-left), then emit lines in
    # reverse sequence (bottom-to-top).  This maximises the chance that
    # blocks mode sees words in a non-reading order.
    for line_idx in reversed(range(len(lines))):
        y = y_base + line_idx * y_step
        for word, x in reversed(lines[line_idx]):
            page.insert_text((x, y), word, fontsize=11)

    doc.save(path)
    doc.close()


def _verify_pathological(path: str) -> None:
    """Assert fixture behaves as expected: blocks ratio >= 0.70, sort ratio < 0.50."""
    import fitz  # type: ignore[import]

    doc = fitz.open(path)
    page = doc[0]

    blocks = page.get_text("blocks")
    text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
    blocks_text = "\n\n".join(text_parts)
    blocks_lines = [ln for ln in blocks_text.splitlines() if ln.strip()]
    blocks_single = sum(1 for ln in blocks_lines if len(ln.split()) <= 1)
    blocks_ratio = blocks_single / max(1, len(blocks_lines))

    sort_text = page.get_text("text", sort=True)
    sort_lines = [ln for ln in sort_text.splitlines() if ln.strip()]
    sort_single = sum(1 for ln in sort_lines if len(ln.split()) <= 1)
    sort_ratio = sort_single / max(1, len(sort_lines))

    doc.close()

    print(f"  blocks_ratio={blocks_ratio:.3f} (want >= 0.70): {'OK' if blocks_ratio >= 0.70 else 'FAIL'}")
    print(f"  sort_ratio  ={sort_ratio:.3f}   (want < 0.50):  {'OK' if sort_ratio < 0.50 else 'FAIL'}")

    assert blocks_ratio >= 0.70, (
        f"Pathological fixture failed: blocks ratio {blocks_ratio:.3f} < 0.70. "
        "The scrambling technique did not produce one-word-per-block output."
    )
    assert sort_ratio < 0.50, (
        f"Pathological fixture failed: sort=True ratio {sort_ratio:.3f} >= 0.50. "
        "sort=True did not recover multi-word lines."
    )


def _generate_clean_pdf(path: str) -> None:
    """Create a clean synthetic PDF where blocks mode gives ratio 0.0.

    Each insert_text() call writes a full sentence at a distinct y-position,
    in sequential top-to-bottom order. No scrambling. PyMuPDF merges sequential
    text into proper paragraph blocks; all lines have multiple words.
    """
    import fitz  # type: ignore[import]

    sentences = [
        "The quick brown fox jumps over the lazy dog.",
        "Lorem ipsum dolor sit amet consectetur adipiscing elit.",
        "Second paragraph begins here with more text content.",
        "Another line of normal paragraph text is written here.",
        "And finally a fifth sentence to pass the sample guard.",
        "Sixth sentence adds more context for thorough testing.",
        "Seventh line ensures the page has substantial content.",
    ]

    doc = fitz.open()
    page = doc.new_page()
    y = 80
    for sentence in sentences:
        page.insert_text((50, y), sentence, fontsize=11)
        y += 22

    doc.save(path)
    doc.close()


def _verify_clean(path: str) -> None:
    """Assert clean fixture has blocks ratio < 0.30 (well below 0.70 threshold)."""
    import fitz  # type: ignore[import]

    doc = fitz.open(path)
    page = doc[0]

    blocks = page.get_text("blocks")
    text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
    blocks_text = "\n\n".join(text_parts)
    blocks_lines = [ln for ln in blocks_text.splitlines() if ln.strip()]
    blocks_single = sum(1 for ln in blocks_lines if len(ln.split()) <= 1)
    blocks_ratio = blocks_single / max(1, len(blocks_lines))

    doc.close()

    print(f"  blocks_ratio={blocks_ratio:.3f} (want < 0.30): {'OK' if blocks_ratio < 0.30 else 'FAIL'}")

    assert blocks_ratio < 0.30, (
        f"Clean fixture failed: blocks ratio {blocks_ratio:.3f} >= 0.30. "
        "The clean PDF is unexpectedly triggering the pathological heuristic."
    )


def main() -> int:
    os.makedirs(FIXTURES_DIR, exist_ok=True)

    print(f"Generating {SINGLE_WORD_PDF} ...")
    _generate_pathological_pdf(SINGLE_WORD_PDF)
    _verify_pathological(SINGLE_WORD_PDF)
    print(f"  Written: {SINGLE_WORD_PDF}")

    print(f"Generating {CLEAN_PDF} ...")
    _generate_clean_pdf(CLEAN_PDF)
    _verify_clean(CLEAN_PDF)
    print(f"  Written: {CLEAN_PDF}")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
