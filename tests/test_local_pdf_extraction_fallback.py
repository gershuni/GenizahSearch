# -*- coding: utf-8 -*-
"""Phase 96 D-F4: detect-then-fallback PDF extraction regression tests.

Implementation plan: 96-02-PLAN.md
"""
import os
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")
HEBREW_PDF = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")


def _import_indexer_helpers():
    """Try to import the Phase 96 detection helper. Skips until 96-02 lands."""
    try:
        from shared.local_indexer import (
            extract_pdf_pages,
            _detect_single_word_per_line,  # NEW in 96-02
        )
        return extract_pdf_pages, _detect_single_word_per_line
    except ImportError:
        pytest.skip("Phase 96 D-F4 not yet implemented (waiting for plan 96-02)")


def test_pathological_pdf_uses_fallback():
    """D-F4: extract_pdf_pages on single_word_per_line.pdf must trigger the
    `get_text('text', sort=True)` fallback and return paragraph-shaped text
    (NOT one-word-per-line)."""
    extract_pdf_pages, _ = _import_indexer_helpers()
    if not os.path.exists(SINGLE_WORD_PDF):
        pytest.fail("Phase 96 Wave 0 fixture missing: " + SINGLE_WORD_PDF)
    pages = list(extract_pdf_pages(SINGLE_WORD_PDF))
    assert len(pages) >= 1
    page_num, text, title = pages[0]
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 5:
        pytest.skip("Sample too small for ratio check")
    single = sum(1 for ln in lines if len(ln.split()) <= 1)
    ratio = single / len(lines)
    # After fallback, ratio should drop well below the 0.70 trigger threshold.
    assert ratio < 0.50, (
        f"Fallback did NOT produce paragraph-shaped text "
        f"(single_word_ratio={ratio:.2f}, expected < 0.50)"
    )


def test_good_pdf_stays_blocks():
    """D-F4 regression-direction-two: hebrew_sample.pdf (a known-good PDF
    from Phase 95) must still use the blocks-mode primary path, not the
    fallback. We can't observe the mode directly, so we assert the
    detection helper returns False on its extracted output.

    Acceptance note (REVISION 2026-05-24): Phase 95 fixture
    `tests/fixtures/local_indexer/hebrew_sample.pdf` must exist (W13).
    """
    extract_pdf_pages, detect = _import_indexer_helpers()
    if not os.path.exists(HEBREW_PDF):
        pytest.skip("hebrew_sample.pdf fixture not found")
    pages = list(extract_pdf_pages(HEBREW_PDF))
    assert len(pages) > 0
    # For the existing-good fixture, the blocks-mode output should NOT trip
    # the single-word-ratio detector. We test detect() directly on the text
    # to confirm the heuristic doesn't flag healthy PDFs.
    for _p, text, _t in pages:
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) >= 5:
            assert not detect(text), (
                "False positive: hebrew_sample.pdf should NOT trip "
                "single-word-per-line detection"
            )


def test_small_sample_skipped():
    """D-F4: detection heuristic returns False when < 5 non-empty lines
    (small-sample guard per RESEARCH §2)."""
    _, detect = _import_indexer_helpers()
    # 4 single-word lines: below the threshold sample size — must NOT trip.
    text = "one\ntwo\nthree\nfour\n"
    assert detect(text) is False, (
        "Small-sample guard failed: < 5 lines should skip detection"
    )
