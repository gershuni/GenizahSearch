# -*- coding: utf-8 -*-
"""RED test scaffold for CMP-01/02/03: Compare modal (open, folio independence, verdict+auto-advance).

Requirements: CMP-01, CMP-02, CMP-03
Wave that turns this green: Wave 2 (Plan 119-03)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (CompareModal
factory and associated state helpers in web/components/compare_modal.py) are not yet
implemented.  They form the failing seams that Wave 2 will make green.

Design intent (per CONTEXT.md D-01/D-02/D-03 / RESEARCH.md Pattern 3):
  - Full-screen modal overlay (ui.dialog maximized), anchor|candidate panes
  - Each pane reuses the extracted AnchorViewer (Phase 117 D-10), per-pane independent
    folio navigation (Pitfall 3: separate AnchorViewer instances, NOT shared state)
  - Flip-through navigation inside Compare: ‹ Prev / Next › steps through candidates in
    current sort/filter order (parity step(delta) over wb.filtered :3741/:3753)
  - Verdict Y/?/N records to sys_id-keyed triage AND auto-advances (D-03)
  - Compare open/lookup keyed by full candidate (sys_id, page) — NOT sys_id alone (D-02)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Literal

import pytest


# Minimal Candidate stand-in for headless tests
@dataclass(frozen=True)
class _Cand:
    sys_id: str
    page: Optional[int]
    uid: str = ""
    shelfmark: str = "?"
    volume_ie: Optional[str] = None
    via_vs: bool = False
    via_other_side: bool = False
    is_anchor_self: bool = False

    @property
    def key(self):
        return (self.sys_id, self.page)


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — CompareModal not yet implemented",
    strict=False,
)
def test_compare_modal_opens_with_anchor_and_candidate_sys_ids():
    """CMP-01: Compare modal must be openable with an anchor sys_id + candidate sys_id.

    Desktop parity: _fill_anchor:4051 / _fill_candidate:4086 (join_workbench.py).
    """
    from web.components.compare_modal import create_compare_state

    anchor = _Cand(sys_id="990001", page=1, shelfmark="T-S 12.001")
    candidate = _Cand(sys_id="990002", page=2, shelfmark="T-S 12.002")
    filtered = [candidate, _Cand(sys_id="990003", page=1, shelfmark="T-S 12.003")]

    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=candidate,
        filtered_candidates=filtered,
    )
    assert state["anchor_sys_id"] == "990001"
    assert state["current_candidate"].sys_id == "990002"
    assert state["current_candidate"].page == 2


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — per-pane folio independence not yet implemented",
    strict=False,
)
def test_compare_pane_folio_navigation_is_independent():
    """CMP-02: anchor pane and candidate pane must track their own folio independently.

    Pitfall 3: AnchorViewer instantiated separately for each pane; navigating one
    must NOT move the other.  Desktop parity: per-pane zoom dict :3823 + _pane_page nav :4243.
    """
    from web.components.compare_modal import create_compare_state, step_pane_page

    anchor = _Cand(sys_id="990001", page=1)
    candidate = _Cand(sys_id="990002", page=1)
    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=candidate,
        filtered_candidates=[candidate],
    )
    # Navigate anchor pane to page 2
    step_pane_page(state, pane="anchor", delta=1)
    assert state["anchor_page"] == 2
    # Candidate pane must still be on page 1
    assert state["candidate_page"] == 1

    # Navigate candidate pane to page 3
    step_pane_page(state, pane="candidate", delta=2)
    assert state["candidate_page"] == 3
    # Anchor pane must still be on page 2
    assert state["anchor_page"] == 2


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — verdict triage sync not yet implemented",
    strict=False,
)
def test_verdict_updates_triage_dict():
    """CMP-03: recording a verdict from Compare must update the shared sys_id-keyed triage dict.

    Desktop parity: _mark → wb.mark(sys_id, val) → triage[sys_id] → restyle (:4202).
    """
    from web.components.compare_modal import create_compare_state, record_verdict

    anchor = _Cand(sys_id="990001", page=1)
    candidate = _Cand(sys_id="990002", page=2)
    filtered = [candidate]
    triage: dict = {}

    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=candidate,
        filtered_candidates=filtered,
    )
    record_verdict(state, verdict="yes", triage=triage)
    assert triage.get("990002") == "yes"


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — verdict auto-advance not yet implemented",
    strict=False,
)
def test_verdict_auto_advances_to_next_candidate():
    """CMP-03: recording a verdict must AUTO-ADVANCE to the next candidate in filtered order.

    Desktop parity: _record_verdict → _step(1) auto-advance (:4202).
    """
    from web.components.compare_modal import create_compare_state, record_verdict

    anchor = _Cand(sys_id="990001", page=1)
    cand_a = _Cand(sys_id="990002", page=2)
    cand_b = _Cand(sys_id="990003", page=3)
    filtered = [cand_a, cand_b]
    triage: dict = {}

    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=cand_a,
        filtered_candidates=filtered,
    )
    assert state["current_candidate"].sys_id == "990002"

    record_verdict(state, verdict="no", triage=triage)
    # After recording verdict on cand_a, must advance to cand_b
    assert state["current_candidate"].sys_id == "990003"
    assert triage.get("990002") == "no"


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — flip-through navigation not yet implemented",
    strict=False,
)
def test_flip_through_wraps_around():
    """CMP-03 D-02: flip-through Prev/Next navigates through filtered_candidates in order,
    wrapping around at boundaries (parity desktop step(delta) :3741/:3753).
    """
    from web.components.compare_modal import create_compare_state, step_candidate

    anchor = _Cand(sys_id="990001", page=1)
    cands = [_Cand(sys_id=f"99000{i}", page=i) for i in range(2, 5)]
    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=cands[0],
        filtered_candidates=cands,
    )
    # Step forward through all candidates
    step_candidate(state, delta=1)
    assert state["current_candidate"].sys_id == "990003"
    step_candidate(state, delta=1)
    assert state["current_candidate"].sys_id == "990004"
    # Wrap around: next after last → first
    step_candidate(state, delta=1)
    assert state["current_candidate"].sys_id == "990002"
