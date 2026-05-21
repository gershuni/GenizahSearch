# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-1 + REQ-4: local_indexer extraction quality.

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-03).
All tests raise NotImplementedError until Plan 95-03 ships.
"""
import pytest

try:
    from shared.local_indexer import LocalIndexer  # noqa: F401
except ImportError:
    pytest.skip(
        "Wave 0 stub — shared.local_indexer not yet implemented (Plan 95-03 Wave 1)",
        allow_module_level=True,
    )


def test_pymupdf_hebrew_extraction_quality():
    """D-44 / D-02 Codex revision: real PyMuPDF Hebrew fixture quality check.
    Asserts get_text('blocks') returns expected paragraph text + correct reading order.
    Fixture: tests/fixtures/local_indexer/hebrew_sample.pdf + .expected.txt
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-1 / D-44 Hebrew extraction quality — implemented in Wave 1 plan 95-03"
    )


def test_rtl_helpers_ported():
    """D-02: _fix_rtl_line, _fix_rtl_page, _join_fragmented_lines ported from
    seewald_addition/genizah_make_index.py:67-105 as dead-code safety net.
    Fixtures: tests/fixtures/local_indexer/mirror_reversed.pdf + single_word_per_line.pdf
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-4 RTL helpers dead-code port — implemented in Wave 1 plan 95-03"
    )


def test_supported_file_types_docx_pdf_txt():
    """REQ-1: indexer accepts .docx, .pdf, .txt; returns extracted text per page."""
    raise NotImplementedError(
        "Wave 0 stub for REQ-1 supported file types — implemented in Wave 1 plan 95-03"
    )


def test_unsupported_extension_status():
    """D-05 / REQ-1: unsupported extension (e.g. .html) gets status='unsupported'."""
    raise NotImplementedError(
        "Wave 0 stub for unsupported extension status — implemented in Wave 1 plan 95-03"
    )
