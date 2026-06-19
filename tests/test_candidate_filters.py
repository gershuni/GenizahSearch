# -*- coding: utf-8 -*-
"""RED test scaffold for CND-06: Candidate filters (material / has-dims / size-mismatch / triage-state).

Requirement: CND-06
Wave that turns this green: Wave 1 (Plan 119-02)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (filter predicates
and apply_filters) are not yet implemented.  They form the failing seams that Wave 1 will
make green.

Design intent (per CONTEXT.md D-14/D-15 / RESEARCH.md Pattern):
  - Filters live in a ui.dialog popover, opened by a "Filters" button (D-14)
  - Filter dimensions: material / has-dimensions / size-mismatch / triage-state
  - Size-mismatch formula: ratio = max(w, anchor_w) / min(w, anchor_w) > 1.4 (D-15, parity :1687-1695)
  - None width → not flagged (no data → pass-through)
  - Filter predicates are pure functions (testable headlessly)
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — size-mismatch predicate not yet implemented",
    strict=False,
)
def test_size_mismatch_ratio_above_threshold():
    """CND-06 D-15: ratio = max/min > 1.4 → mismatch flagged.

    Parity: desktop join_workbench.py:1687-1695.
    """
    from web.components.candidate_grid import is_size_mismatch
    # ratio = max(10, 20) / min(10, 20) = 20/10 = 2.0 > 1.4 → mismatch
    assert is_size_mismatch(candidate_width_cm=20.0, anchor_width_cm=10.0) is True


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — size-mismatch predicate not yet implemented",
    strict=False,
)
def test_size_mismatch_ratio_at_threshold_not_flagged():
    """CND-06 D-15: ratio exactly = 1.4 is NOT flagged (strict >)."""
    from web.components.candidate_grid import is_size_mismatch
    # ratio = 14/10 = 1.4 → exactly threshold → NOT flagged
    assert is_size_mismatch(candidate_width_cm=14.0, anchor_width_cm=10.0) is False


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — size-mismatch predicate not yet implemented",
    strict=False,
)
def test_size_mismatch_none_width_not_flagged():
    """CND-06 D-15: None width (no data) → NOT flagged (no data = pass-through)."""
    from web.components.candidate_grid import is_size_mismatch
    assert is_size_mismatch(candidate_width_cm=None, anchor_width_cm=10.0) is False
    assert is_size_mismatch(candidate_width_cm=10.0, anchor_width_cm=None) is False
    assert is_size_mismatch(candidate_width_cm=None, anchor_width_cm=None) is False


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — size-mismatch predicate not yet implemented",
    strict=False,
)
def test_size_mismatch_zero_width_not_flagged():
    """CND-06 D-15: zero width → NOT flagged (guards division by zero)."""
    from web.components.candidate_grid import is_size_mismatch
    assert is_size_mismatch(candidate_width_cm=0.0, anchor_width_cm=10.0) is False
    assert is_size_mismatch(candidate_width_cm=10.0, anchor_width_cm=0.0) is False


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — material filter predicate not yet implemented",
    strict=False,
)
def test_material_filter_excludes_non_matching():
    """CND-06: material filter applied to enrichment dict excludes non-matching candidates."""
    from shared.joins_lab import Candidate
    from web.components.candidate_grid import compute_filtered

    enrichment = {
        "990001": {"material": "parchment", "width_cm": 10.0, "height_cm": 12.0},
        "990002": {"material": "paper", "width_cm": 11.0, "height_cm": 13.0},
    }
    candidates = [
        Candidate(sys_id="990001", page=1, via_text=True),
        Candidate(sys_id="990002", page=1, via_text=True),
    ]
    filter_state = {
        "materials": ["parchment"],
        "has_dims": False,
        "exclude_mismatch": False,
        "triage_states": [],
        "text_q": "",
    }
    result = compute_filtered(candidates, filter_state, enrichment, triage={}, anchor_sys_id="999")
    assert len(result) == 1
    assert result[0].sys_id == "990001"


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — triage-state filter predicate not yet implemented",
    strict=False,
)
def test_triage_filter_not_triaged():
    """CND-06: triage-state filter 'Not triaged' keeps only candidates with no verdict."""
    from shared.joins_lab import Candidate
    from web.components.candidate_grid import compute_filtered

    candidates = [
        Candidate(sys_id="990001", page=1, via_text=True),
        Candidate(sys_id="990002", page=1, via_text=True),
    ]
    triage = {"990001": "yes"}  # 990001 triaged; 990002 not
    filter_state = {
        "materials": [],
        "has_dims": False,
        "exclude_mismatch": False,
        "triage_states": ["Not triaged"],
        "text_q": "",
    }
    result = compute_filtered(candidates, filter_state, {}, triage=triage, anchor_sys_id="999")
    assert len(result) == 1
    assert result[0].sys_id == "990002"
