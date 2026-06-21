# -*- coding: utf-8 -*-
"""RED test scaffold for VSM-01: VS adapter (field mapping, conditional intersection/union model).

Requirements: VSM-01
Wave that turns this green: Wave 2 (Plan 119-04)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (VS adapter
and conditional merge in joins_lab.py) are not yet implemented.  They form the failing
seams that Wave 2 will make green.

Design intent (per CONTEXT.md D-04/D-05 / RESEARCH.md Pitfall 4):
  - VS adapter maps get_suggestions result {alma_id, svm_score, rank} → Candidate
  - CRITICAL field mapping: svm_score → vs_score (NOT vs_rank), rank → vs_rank (NOT swapped!)
  - Conditional model:
      ON + builder has query → INTERSECTION (c.via_text AND c.via_vs)
      ON + empty builder   → UNION (pure VS browse: merge_candidates([], vs))
      OFF                  → text-only; tier0+tier1 only (via_text required)
  - None vs_score means "no VS data", NOT dissimilar (Pitfall 4)
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — VS adapter not yet implemented",
    strict=False,
)
def test_vs_adapter_maps_svm_score_to_vs_score_not_rank():
    """VSM-01 Pitfall-4 guard: svm_score MUST map to vs_score, rank MUST map to vs_rank.

    This is the critical field-mapping invariant — a transposition would cause vs_score
    to receive an integer rank (0-200) and vs_rank to receive a float score (0.0-1.0),
    breaking tier ordering and the None=='no-data' sentinel contract.
    """
    from web.pages.joins_lab import _map_vs_suggestions_to_candidates

    raw = [
        {"alma_id": "990001", "svm_score": 0.91, "rank": 3},
        {"alma_id": "990002", "svm_score": 0.85, "rank": 7},
    ]
    candidates = _map_vs_suggestions_to_candidates(raw)

    assert len(candidates) == 2
    c1 = next(c for c in candidates if c.sys_id == "990001")
    # svm_score=0.91 → vs_score (NOT vs_rank)
    assert c1.vs_score == pytest.approx(0.91), (
        f"svm_score must map to vs_score, got vs_score={c1.vs_score!r}"
    )
    # rank=3 → vs_rank (NOT vs_score)
    assert c1.vs_rank == 3, (
        f"rank must map to vs_rank, got vs_rank={c1.vs_rank!r}"
    )
    # via_vs must be True
    assert c1.via_vs is True


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — VS adapter alma_id mapping not yet implemented",
    strict=False,
)
def test_vs_adapter_alma_id_maps_to_sys_id():
    """VSM-01 D-05: returned alma_id (the PARTNER sys_id = alma_id_b) must map to Candidate.sys_id.

    Verified in visual_similarity_service.py:111-124: 'SELECT alma_id_b ... WHERE alma_id_a = ?'
    So the returned alma_id IS the candidate, not the anchor.
    """
    from web.pages.joins_lab import _map_vs_suggestions_to_candidates

    raw = [{"alma_id": "990099", "svm_score": 0.75, "rank": 1}]
    candidates = _map_vs_suggestions_to_candidates(raw)
    assert candidates[0].sys_id == "990099"


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — VS conditional intersection model not yet implemented",
    strict=False,
)
def test_vs_conditional_model_intersection_when_query():
    """VSM-01 D-04: ON + builder has query → INTERSECTION (keep only via_text AND via_vs).

    Desktop parity: join_workbench.py:2788-2802.
    """
    from shared.joins_lab import Candidate
    from web.pages.joins_lab import _apply_vs_merge

    text_candidates = [
        Candidate(sys_id="A", page=1, via_text=True),   # text-only
        Candidate(sys_id="B", page=2, via_text=True),   # text + vs → tier0 (intersection)
    ]
    vs_candidates = [
        Candidate(sys_id="B", page=None, via_vs=True, vs_rank=1, vs_score=0.9),
        Candidate(sys_id="C", page=None, via_vs=True, vs_rank=2, vs_score=0.8),  # vs-only
    ]
    result = _apply_vs_merge(
        text_candidates=text_candidates,
        vs_candidates=vs_candidates,
        vs_on=True,
        builder_has_query=True,
    )
    # Intersection: only B (both via_text AND via_vs)
    sys_ids = [c.sys_id for c in result]
    assert "B" in sys_ids, "B (text+VS) must be in intersection"
    assert "A" not in sys_ids, "A (text-only) must be excluded from intersection"
    assert "C" not in sys_ids, "C (VS-only) must be excluded from intersection"


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — VS conditional union model not yet implemented",
    strict=False,
)
def test_vs_conditional_model_union_when_empty_builder():
    """VSM-01 D-04: ON + empty builder → UNION (pure VS browse: all VS candidates).

    Desktop parity: join_workbench.py:2788-2802.
    """
    from shared.joins_lab import Candidate
    from web.pages.joins_lab import _apply_vs_merge

    vs_candidates = [
        Candidate(sys_id="X", page=None, via_vs=True, vs_rank=1, vs_score=0.95),
        Candidate(sys_id="Y", page=None, via_vs=True, vs_rank=2, vs_score=0.88),
    ]
    result = _apply_vs_merge(
        text_candidates=[],        # empty builder → no text candidates
        vs_candidates=vs_candidates,
        vs_on=True,
        builder_has_query=False,   # empty builder
    )
    sys_ids = [c.sys_id for c in result]
    assert "X" in sys_ids
    assert "Y" in sys_ids


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — VS OFF model not yet implemented",
    strict=False,
)
def test_vs_off_keeps_only_text_candidates():
    """VSM-01 D-04: OFF → text-only; VS-only (tier2) candidates excluded.

    D-04 specifies: 'text-only but look-alikes among text hits still carry the 👁 badge'
    → text candidates that also appear in VS get via_vs=True badge; pure VS-only are excluded.
    """
    from shared.joins_lab import Candidate
    from web.pages.joins_lab import _apply_vs_merge

    text_candidates = [
        Candidate(sys_id="A", page=1, via_text=True),
        Candidate(sys_id="B", page=2, via_text=True),
    ]
    vs_candidates = [
        Candidate(sys_id="B", page=None, via_vs=True, vs_rank=1),  # also in text
        Candidate(sys_id="C", page=None, via_vs=True, vs_rank=2),  # VS-only
    ]
    result = _apply_vs_merge(
        text_candidates=text_candidates,
        vs_candidates=vs_candidates,
        vs_on=False,
        builder_has_query=True,
    )
    sys_ids = [c.sys_id for c in result]
    assert "A" in sys_ids, "Text-only A must appear when VS is OFF"
    assert "B" in sys_ids, "Text+VS B must appear when VS is OFF"
    assert "C" not in sys_ids, "VS-only C must be excluded when VS is OFF"
    # B should have via_vs=True (badge) since it appears in both
    b_cand = next(c for c in result if c.sys_id == "B")
    assert b_cand.via_vs is True, "B must carry 👁 badge (via_vs=True) even when VS toggle is OFF"


def test_vs_score_none_is_no_data_not_dissimilar():
    """VSM-01 Pitfall-4: vs_score=None means 'no VS data', NOT dissimilar.

    A Candidate with vs_score=None but via_vs=True still carries the 👁 badge.
    Only vs_score=0.0 would indicate dissimilar; None must not be treated as falsy
    VS indicator.
    """
    from shared.joins_lab import Candidate, badge_and_tooltip

    # A VS candidate with no score data (rank-only)
    c = Candidate(sys_id="990001", page=1, via_vs=True, vs_rank=5, vs_score=None)
    assert c.via_vs is True, "via_vs must be True regardless of vs_score"
    assert c.vs_score is None, "vs_score=None means no-data sentinel"
    # badge_and_tooltip must still return 'visibility' for this candidate
    icon, tip = badge_and_tooltip(c)
    assert icon == "visibility", (
        "badge_and_tooltip must return 'visibility' for via_vs=True even when vs_score=None"
    )


# ---------------------------------------------------------------------------
# Plan 119-07: G2 — raw baseline + _compute_display_candidates helper
# ---------------------------------------------------------------------------


def test_toggle_on_vs_off_produces_different_set_from_raw_baseline():
    """G2: _apply_vs_merge(raw, vs, on=True, query=True) INTERSECTS; on=False returns full text.

    With a raw text baseline where some candidates also appear in the VS list,
    toggling VS ON (intersection) must produce a STRICT SUBSET of toggling VS OFF
    (text-only). This proves that the baseline is the RAW text set (not the
    pre-merged display set), since a pre-merged set would already contain both
    text+VS candidates AND lose the ability to distinguish them.
    """
    from shared.joins_lab import Candidate
    from web.pages.joins_lab import _apply_vs_merge

    raw_text = [
        Candidate(sys_id="A", page=1, via_text=True),   # text-only
        Candidate(sys_id="B", page=2, via_text=True),   # in BOTH text and VS
        Candidate(sys_id="D", page=3, via_text=True),   # text-only
    ]
    vs_cands = [
        Candidate(sys_id="B", page=None, via_vs=True, vs_rank=1, vs_score=0.9),
        Candidate(sys_id="C", page=None, via_vs=True, vs_rank=2, vs_score=0.8),  # VS-only
    ]

    # ON: intersection — only candidates that are in BOTH text AND VS
    on_result = _apply_vs_merge(raw_text, vs_cands, vs_on=True, builder_has_query=True)
    on_ids = [c.sys_id for c in on_result]

    # OFF: text-only (look-alikes carry badge but VS-only excluded)
    off_result = _apply_vs_merge(raw_text, vs_cands, vs_on=False, builder_has_query=True)
    off_ids = [c.sys_id for c in off_result]

    # Intersection must differ from text-only (the core G2 correctness criterion)
    assert on_ids != off_ids, (
        "ON (intersection) and OFF (text-only) must produce different sets from a RAW baseline; "
        f"got ON={on_ids!r} == OFF={off_ids!r}"
    )
    # ON is a subset of OFF
    assert set(on_ids) < set(off_ids), (
        f"Intersection set {on_ids!r} must be a STRICT SUBSET of text-only set {off_ids!r}"
    )
    # Only B survives the intersection (A and D are text-only, C is VS-only)
    assert on_ids == ["B"], f"Only B (text+VS) must survive intersection, got {on_ids!r}"
    # OFF includes A, B, D (text candidates); C is VS-only and excluded
    assert set(off_ids) == {"A", "B", "D"}, f"Text-only set must be {{A,B,D}}, got {set(off_ids)!r}"


def test_compute_display_candidates_symbol_exists_in_joins_lab():
    """G2 source assertion: joins_lab.py must define _compute_display_candidates.

    This test is RED until Plan 07 Task 1 implements the helper.
    """
    from pathlib import Path

    joins_lab_path = Path(__file__).parent.parent / "web" / "pages" / "joins_lab.py"
    assert joins_lab_path.exists(), "web/pages/joins_lab.py must exist"
    source = joins_lab_path.read_text(encoding="utf-8")

    # Helper must be defined (a page-local closure)
    assert "_compute_display_candidates" in source, (
        "joins_lab.py must define _compute_display_candidates (G2 fix — Task 1 RED)"
    )
    # Raw baseline must exist
    assert "_raw_text_candidates" in source, (
        "joins_lab.py must define _raw_text_candidates (G2 fix — raw baseline)"
    )


def test_re_render_calls_compute_display_candidates_not_all_candidates_directly():
    """G2 source assertion: _re_render_candidates_surface must call _compute_display_candidates.

    The pre-fix code passed _all_candidates directly to compute_filtered, skipping the
    VS recompute. The fix must route through _compute_display_candidates() first.
    """
    from pathlib import Path

    joins_lab_path = Path(__file__).parent.parent / "web" / "pages" / "joins_lab.py"
    assert joins_lab_path.exists(), "web/pages/joins_lab.py must exist"
    source = joins_lab_path.read_text(encoding="utf-8")

    # Find _re_render_candidates_surface function body
    lines = source.splitlines()
    fn_start = None
    fn_end = None
    for i, line in enumerate(lines):
        if "def _re_render_candidates_surface(" in line:
            fn_start = i
        if fn_start is not None and i > fn_start:
            # Next function definition ends the body
            stripped = line.strip()
            if stripped.startswith("def ") or stripped.startswith("async def "):
                fn_end = i
                break
    assert fn_start is not None, "_re_render_candidates_surface must exist in joins_lab.py"
    fn_body = "\n".join(lines[fn_start: fn_end or fn_start + 30])

    assert "_compute_display_candidates" in fn_body, (
        "_re_render_candidates_surface must call _compute_display_candidates() to recompute "
        "the display list from the RAW baseline (not pass _all_candidates directly — G2 fix)"
    )
    # Must NOT directly pass _all_candidates to compute_filtered in this function
    assert "_apply_vs_merge(_all_candidates" not in fn_body, (
        "_re_render_candidates_surface must NOT call _apply_vs_merge(_all_candidates, ...) "
        "— it must go through _compute_display_candidates() (G2 fix)"
    )
