# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-43: PyInstaller smoke test for packaged EXE.

Gated @pytest.mark.packaging — runs in release CI pipeline only, not on every commit.
Real implementation: Plan 95-09 Wave 4 + this plan's .spec update.
"""
import pytest


@pytest.mark.packaging
def test_packaged_exe_extracts_hebrew_pdf():
    """D-43: after build, run packaged GenizahSearchPro.exe; import fitz; open
    hebrew_sample.pdf fixture; assert get_text('blocks') returns expected string
    from tests/fixtures/local_indexer/hebrew_sample.expected.txt.
    Validates that collect_all('pymupdf') in GenizahSearchPro.spec correctly
    bundles fitz._fitz C-extension.
    """
    pytest.skip(
        "Wave 0 stub — @pytest.mark.packaging; requires built EXE; implemented in Wave 4 plan 95-09"
    )
