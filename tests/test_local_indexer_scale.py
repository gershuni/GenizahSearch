# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-10: scale test (5000 files under 10 min).

Marked @pytest.mark.slow — excluded from the default test run.
Real implementation: shared/local_indexer.py (Wave 3, Plan 95-07).
"""
import pytest


@pytest.mark.slow
def test_5000_files_under_10_min():
    """REQ-10: indexing 5000 files completes in under 10 minutes."""
    pytest.skip(
        "Wave 0 stub — @pytest.mark.slow scale test; implemented in Wave 3 plan 95-07"
    )
