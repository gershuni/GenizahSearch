# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-6: LOCAL filter cascade discipline (static AST guard).

Mirrors tests/test_pgp_filter_cascade.py pattern.
Real implementation: genizah_app.py _apply_results_table_filters + _apply_comp_tree_filters
(Wave 4, Plan 95-08).
"""
import pytest


def test_local_filter_applied_after_pgp_filter():
    """REQ-6: static AST confirms LOCAL filter is applied after PGP filter and
    printed filter in _apply_results_table_filters and _apply_comp_tree_filters.
    """
    pytest.skip(
        "Wave 0 stub — LOCAL filter cascade static AST guard; implemented in Wave 4 plan 95-08"
    )


def test_no_op_when_no_local_hits():
    """D-10 Codex P1: when result set has zero LOCAL hits AND state is Only-Local
    or No-Local, the filter is a no-op — all hits show, inline chip appears.
    """
    pytest.skip(
        "Wave 0 stub — no-op filter when no LOCAL hits; implemented in Wave 4 plan 95-08"
    )
