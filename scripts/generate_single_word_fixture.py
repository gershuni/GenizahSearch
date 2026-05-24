"""Phase 96 D-F4: generate the pathological one-word-per-line regression fixture.

Run once: `python scripts/generate_single_word_fixture.py`
Output: tests/fixtures/local_indexer/single_word_per_line.pdf

Strategy: emit one word per line at distinct y-coordinates. Validated AGAINST
the production detection heuristic (single_word_ratio >= 0.70 over >= 5
non-empty lines) — NOT against PyMuPDF block-count internals (REVISION
2026-05-24: checker BLOCKER 1).

We try TextWriter first (independent content streams per word — most robust);
fall back to insert_text at distinct y-coords (also produces separate blocks
on PyMuPDF 1.27.x).
"""
import os
import sys
import fitz  # pymupdf

WORDS = [
    "The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
    "and", "the", "lazy", "cat", "watches", "from", "the", "window", "above",
    "while", "the", "rain", "falls", "softly", "on", "the", "garden", "below",
    "filling", "every", "leaf", "with", "tiny", "droplets", "of", "water",
    "above", "the", "rooftops", "of", "the", "neighboring", "houses", "below",
]

OUT = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..",
    "tests", "fixtures", "local_indexer", "single_word_per_line.pdf",
))


def _build_with_text_writer(doc) -> bool:
    """Try TextWriter first — produces independent content streams per append()."""
    try:
        page = doc.new_page(width=612, height=792)
        try:
            tw_factory = fitz.TextWriter  # noqa: F841 (existence check)
        except AttributeError:
            return False
        y = 72.0
        for word in WORDS:
            tw = fitz.TextWriter(page.rect)
            tw.append(fitz.Point(72.0, y), word, fontsize=12)
            tw.write_text(page)
            y += 28.0
        return True
    except Exception:
        return False


def _build_with_insert_text(doc) -> None:
    """Fallback for older PyMuPDF: per-word insert_text at distinct y-coords."""
    page = doc.new_page(width=612, height=792)
    y = 72.0
    for word in WORDS:
        page.insert_text(fitz.Point(72.0, y), word, fontsize=12)
        y += 28.0


def main() -> int:
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    doc = fitz.open()  # new empty doc
    if not _build_with_text_writer(doc):
        # Reset the doc since _build_with_text_writer may have partially populated.
        doc.close()
        doc = fitz.open()
        _build_with_insert_text(doc)
    doc.save(OUT)
    doc.close()
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
