# -*- coding: utf-8 -*-
"""Tests for CMP-01/02/03: Compare modal (open, folio independence, verdict+auto-advance).

Requirements: CMP-01, CMP-02, CMP-03 / VSM-02
Phase: 119-candidates-compare-visual-similarity
Plan: 119-03 (Wave 1 — turns this scaffold green)

All tests exercise the headless-testable pure helpers (create_compare_state,
step_pane_page, step_candidate, record_verdict) and _find_candidate_idx.
No NiceGUI render harness is needed — mirrors AnchorViewer's headless-test pattern.

Seams covered:
  CMP-01 — open with anchor + candidate sys_ids; current_candidate is the correct folio
  CMP-02 — per-pane folio navigation is independent (anchor page ≠ candidate page)
  CMP-03 — verdict updates shared triage dict; auto-advance increments idx (mod len)
  F2     — _find_candidate_idx is per-image: given two candidates sharing a sys_id on
            different pages, lookup by page-6 candidate returns page-6 index (Pitfall 6)
  WR-05  — wrap-around: Next after last → first; Prev before first → last
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pytest


# ---------------------------------------------------------------------------
# Minimal Candidate stand-in for headless tests
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# CMP-01: open with anchor + candidate sys_ids
# ---------------------------------------------------------------------------

def test_compare_modal_opens_with_anchor_and_candidate_sys_ids():
    """CMP-01: create_compare_state must capture anchor sys_id and the correct candidate.

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


# ---------------------------------------------------------------------------
# CMP-02: per-pane folio navigation independence
# ---------------------------------------------------------------------------

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

    # Navigate candidate pane forward by 2
    step_pane_page(state, pane="candidate", delta=2)
    assert state["candidate_page"] == 3
    # Anchor pane must still be on page 2
    assert state["anchor_page"] == 2


# ---------------------------------------------------------------------------
# CMP-03: verdict updates triage dict
# ---------------------------------------------------------------------------

def test_verdict_updates_triage_dict():
    """CMP-03: recording a verdict from Compare must update the shared sys_id-keyed triage dict.

    Desktop parity: _mark → wb.mark(sys_id, val) → triage[sys_id] → restyle (:4202).
    Verdict is reported by sys_id — triage is per-sys_id (D-11).
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


def test_verdict_calls_on_verdict_callback():
    """CMP-03: record_verdict must invoke the on_verdict callback with (sys_id, verdict).

    The on_verdict callback is the page-level triage update hook (D-03).
    """
    from web.components.compare_modal import create_compare_state, record_verdict

    anchor = _Cand(sys_id="990001", page=1)
    candidate = _Cand(sys_id="990002", page=2)
    filtered = [candidate]
    triage: dict = {}
    calls: list = []

    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=candidate,
        filtered_candidates=filtered,
    )

    def on_verdict_stub(sys_id: str, verdict: str) -> None:
        calls.append((sys_id, verdict))

    record_verdict(state, verdict="maybe", triage=triage, on_verdict=on_verdict_stub)
    assert len(calls) == 1
    assert calls[0] == ("990002", "maybe")


# ---------------------------------------------------------------------------
# CMP-03: verdict auto-advance
# ---------------------------------------------------------------------------

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


def test_verdict_auto_advance_wraps_at_last_candidate():
    """CMP-03: auto-advance from the last candidate must wrap to the first (D-03 UI-SPEC)."""
    from web.components.compare_modal import create_compare_state, record_verdict

    anchor = _Cand(sys_id="990001", page=1)
    cand_a = _Cand(sys_id="990002", page=2)
    cand_b = _Cand(sys_id="990003", page=3)
    filtered = [cand_a, cand_b]
    triage: dict = {}

    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=cand_b,  # start at last
        filtered_candidates=filtered,
    )
    assert state["current_candidate"].sys_id == "990003"

    # Verdict on last → wraps to first
    record_verdict(state, verdict="yes", triage=triage)
    assert state["current_candidate"].sys_id == "990002"


# ---------------------------------------------------------------------------
# WR-05: flip-through wrap-around
# ---------------------------------------------------------------------------

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


def test_flip_through_prev_wraps_to_last():
    """WR-05: step(-1) from the first candidate must wrap to the last."""
    from web.components.compare_modal import create_compare_state, step_candidate

    anchor = _Cand(sys_id="990001", page=1)
    cands = [_Cand(sys_id=f"99000{i}", page=i) for i in range(2, 5)]
    state = create_compare_state(
        anchor_cand=anchor,
        initial_candidate=cands[0],  # start at first (sys_id=990002)
        filtered_candidates=cands,
    )
    assert state["current_candidate"].sys_id == "990002"
    # Prev before first → last
    step_candidate(state, delta=-1)
    assert state["current_candidate"].sys_id == "990004"


# ---------------------------------------------------------------------------
# F2 (Pitfall 6): per-image candidate lookup — same sys_id on multiple pages
# ---------------------------------------------------------------------------

def test_find_candidate_idx_locates_by_per_image_identity():
    """F2 (Pitfall 6): _find_candidate_idx must locate by per-image identity.

    Given two candidates sharing a sys_id on pages 5 and 6, looking up the
    page-6 candidate must return the page-6 index — NOT a sys_id collision
    into the page-5 row.

    Proven by: tests/test_joins_lab.py:425-433 (dedup preserves both).
    """
    from web.components.compare_modal import _find_candidate_idx

    # Both candidates share sys_id "990042" but differ by page
    cand_p5 = _Cand(sys_id="990042", page=5, uid="990042|p5")
    cand_p6 = _Cand(sys_id="990042", page=6, uid="990042|p6")
    cand_other = _Cand(sys_id="990099", page=1, uid="990099|p1")

    candidates = [cand_other, cand_p5, cand_p6]  # page-5 at index 1, page-6 at index 2

    # Looking up cand_p6 must return index 2 (the page-6 entry), not index 1 (page-5)
    idx = _find_candidate_idx(cand_p6, candidates)
    assert idx == 2, (
        f"Expected index 2 (page-6 candidate) but got {idx} — "
        "sys_id-only lookup would have returned the wrong (page-5) entry (Pitfall 6)"
    )

    # Looking up cand_p5 must return index 1
    idx2 = _find_candidate_idx(cand_p5, candidates)
    assert idx2 == 1

    # Looking up cand_other returns index 0
    idx3 = _find_candidate_idx(cand_other, candidates)
    assert idx3 == 0


def test_find_candidate_idx_uid_wins_over_key():
    """Per-image identity: uid match takes precedence over (sys_id, page) match."""
    from web.components.compare_modal import _find_candidate_idx

    # Two candidates with different sys_ids but uid match targets the right one
    cand_a = _Cand(sys_id="AAA", page=1, uid="unique-uid-xyz")
    cand_b = _Cand(sys_id="BBB", page=2, uid="unique-uid-xyz")  # same uid, different sys_id

    # cand_b matches cand_a by uid since uid is checked first
    idx = _find_candidate_idx(_Cand(sys_id="AAA", page=1, uid="unique-uid-xyz"), [cand_a, cand_b])
    assert idx == 0  # cand_a is at index 0 (first uid match)


def test_find_candidate_idx_fallback_when_not_found():
    """_find_candidate_idx returns 0 when candidate is not in the list (graceful fallback)."""
    from web.components.compare_modal import _find_candidate_idx

    cand_a = _Cand(sys_id="AAA", page=1, uid="uid-a")
    cand_b = _Cand(sys_id="BBB", page=2, uid="uid-b")
    missing = _Cand(sys_id="ZZZ", page=99, uid="uid-z")

    # Not found → fallback to 0
    idx = _find_candidate_idx(missing, [cand_a, cand_b])
    assert idx == 0


# ---------------------------------------------------------------------------
# Source integrity checks (headless — no NiceGUI runtime)
# ---------------------------------------------------------------------------

def test_compare_modal_source_has_two_anchor_viewer_instantiations():
    """Source assertion: the file contains exactly 2 AnchorViewer( instantiations."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    count = source.count("AnchorViewer(")
    assert count >= 2, (
        f"Expected at least 2 AnchorViewer( instantiations (one per pane) but found {count}"
    )


def test_compare_modal_source_no_inject_viewer_assets():
    """Source assertion: compare_modal.py must NOT call inject_viewer_assets.

    Plan 04 owns the single call site; the window._msViewerLoaded guard covers
    the dynamically-created Compare viewers.
    """
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "inject_viewer_assets" not in source, (
        "compare_modal.py must NOT call inject_viewer_assets() — "
        "Plan 04 calls it once at page-build time (idempotency guard covers Compare viewers)"
    )


def test_compare_modal_source_no_raw_storage_access():
    """Source assertion: no app.storage.user in compare_modal.py (Phase-87 invariant)."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "app.storage.user" not in source, (
        "compare_modal.py must never access app.storage.user directly (Phase-87 invariant)"
    )


def test_compare_modal_source_no_forbidden_spacing():
    """Source assertion: no p-3/gap-3 (12px) spacing — UI-SPEC uses only declared tokens."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "p-3" not in source, "compare_modal.py must not use p-3 (12px) — use p-2 or p-4"
    assert "gap-3" not in source, "compare_modal.py must not use gap-3 (12px) — use gap-2 or gap-4"


def test_compare_modal_source_no_server_side_stop_propagation():
    """Source assertion: no server-side .stop_propagation() calls (AST guard pattern)."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert ".stop_propagation()" not in source, (
        "compare_modal.py must use js_handler='(e) => e.stopPropagation()' "
        "not server-side e.stop_propagation() (see 2026-06-12 hotfix pattern)"
    )


def test_compare_modal_source_uses_size_mismatch_from_candidate_grid():
    """Source assertion: size-mismatch formula imported from candidate_grid (single formula).

    There must be exactly ONE size-mismatch formula in use (D-15).
    compare_modal.py imports is_size_mismatch from candidate_grid.py — not reimplemented.
    """
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "from web.components.candidate_grid import" in source and "is_size_mismatch" in source, (
        "compare_modal.py must import is_size_mismatch from web.components.candidate_grid "
        "to reuse ONE shared formula (D-15)"
    )


def test_compare_modal_source_size_mismatch_badge_uses_tr():
    """Source assertion: size-mismatch badge uses tr('Size mismatch')."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "Size mismatch" in source, (
        "compare_modal.py must use tr('Size mismatch') for the warning badge (UI-SPEC)"
    )


def test_compare_modal_imports_headlessly():
    """CMP-01: `import web.components.compare_modal` must succeed without a live AppState."""
    import web.components.compare_modal as m  # noqa: F401
    assert hasattr(m, "create_compare_modal")
    assert hasattr(m, "create_compare_state")
    assert hasattr(m, "_find_candidate_idx")
    assert hasattr(m, "step_candidate")
    assert hasattr(m, "step_pane_page")
    assert hasattr(m, "record_verdict")
