# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-46: static AST guard — no web page builds library-filter
dropdown from LIBRARY_CODES without filtering out code == 'LOCAL'.

Mirrors tests/test_pgp_filter_cascade.py pattern.
Real implementation: web/pages/search.py + browse.py (Wave 4, Plan 95-09).
"""
import pytest


def test_no_web_page_iterates_library_codes_without_local_guard():
    """D-46: static AST scan over web/pages/ — every library-filter dropdown builder
    that iterates LIBRARY_CODES must have a guard filtering out code == 'LOCAL'.
    Prevents future regressions when new library-list consumers are added.
    """
    pytest.skip(
        "Wave 0 stub — D-46 web library options LOCAL guard static AST; implemented in Wave 4 plan 95-09"
    )
