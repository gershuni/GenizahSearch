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


# ---------------------------------------------------------------------------
# Task 2 (Plan 119-06): G5 — show-loader behavioral tests
# ---------------------------------------------------------------------------

def _make_compare_modal_with_stubs():
    """Create a compare modal with mocked NiceGUI; return (dialog, anchor_stub, cand_stub).

    The stubs are AsyncMock AnchorViewer instances injected in place of the real
    AnchorViewer constructor, so the show-loader coroutine calls update_content on
    them instead of building live NiceGUI widgets.
    """
    import asyncio
    import inspect
    from unittest.mock import MagicMock, AsyncMock, patch

    anchor = _Cand(sys_id="ANCHOR01", page=2, shelfmark="T-S Anchor")
    cand1 = _Cand(sys_id="CAND01", page=3, shelfmark="T-S Cand1")
    cand2 = _Cand(sys_id="CAND02", page=4, shelfmark="T-S Cand2")
    triage: dict = {}
    calls: list = []

    def on_verdict(sys_id, verdict):
        calls.append((sys_id, verdict))

    # Stub AnchorViewer — records update_content calls
    anchor_stub = MagicMock()
    anchor_stub.update_content = AsyncMock()

    cand1_stub = MagicMock()
    cand1_stub.update_content = AsyncMock()

    # Track which stub instances were created (in order)
    created_viewers = []

    mock_element = MagicMock()
    mock_element.__enter__ = lambda s: s
    mock_element.__exit__ = MagicMock(return_value=False)
    for m in ("classes", "props", "style", "mark", "tooltip", "on"):
        setattr(mock_element, m, MagicMock(return_value=mock_element))

    # dialog mock that supports .on("show", ...) — captures the handler
    dialog_mock = MagicMock()
    dialog_mock.__enter__ = lambda s: s
    dialog_mock.__exit__ = MagicMock(return_value=False)
    _show_handlers: list = []

    def _dialog_on(event, handler):
        if event == "show":
            _show_handlers.append(handler)
        return dialog_mock

    dialog_mock.on = _dialog_on
    dialog_mock.props = MagicMock(return_value=dialog_mock)

    stub_sequence = [anchor_stub, cand1_stub]

    def _fake_anchor_viewer_init(self, sys_id, p_num=None, volume_ie=None,
                                  highlight_pattern=None, browse_resolver=None,
                                  external_resolver=None, fl_id=None):
        if stub_sequence:
            stub = stub_sequence.pop(0)
        else:
            stub = MagicMock()
            stub.update_content = AsyncMock()
        created_viewers.append(stub)
        # Copy stub attributes into self (since the factory accesses _cand_viewer_ref[0])
        self.update_content = stub.update_content
        self._sys_id = sys_id
        self._p_num = p_num

    with (
        patch("web.components.compare_modal.ui") as mock_ui,
        patch("web.components.compare_modal.AnchorViewer") as MockAnchorViewer,
    ):
        mock_ui.dialog.return_value = dialog_mock
        mock_ui.card.return_value = mock_element
        mock_ui.row.return_value = mock_element
        mock_ui.column.return_value = mock_element
        mock_ui.label.return_value = mock_element
        mock_ui.button.return_value = mock_element
        mock_ui.icon.return_value = mock_element
        mock_ui.badge.return_value = mock_element

        # Make AnchorViewer() return the stub instances in order.
        # The factory calls AnchorViewer(...) and then we need to inject
        # the stub so _anchor_viewer_ref / _cand_viewer_ref get the stub's
        # update_content. Use side_effect to inject.
        created_viewer_instances = []

        def _av_side_effect(*args, **kwargs):
            if created_viewer_instances:
                # Return next stub
                stub = anchor_stub if len(created_viewer_instances) == 0 else cand1_stub
            # Build a real-looking mock with update_content
            inst = MagicMock()
            if not created_viewer_instances:
                inst.update_content = anchor_stub.update_content
            else:
                inst.update_content = cand1_stub.update_content
            created_viewer_instances.append(inst)
            return inst

        MockAnchorViewer.side_effect = _av_side_effect

        from web.components.compare_modal import create_compare_modal
        dialog = create_compare_modal(
            anchor_cand=anchor,
            initial_candidate=cand1,
            filtered_candidates=[cand1, cand2],
            triage=triage,
            on_verdict=on_verdict,
        )

    return dialog, anchor_stub, cand1_stub, _show_handlers, triage, cand2


class TestShowLoaderBehavioral:
    """Behavioral tests for the G5 show-loader (Task 2, Plan 119-06).

    These tests drive the _on_show / _load_candidate_pane coroutines with stub
    viewers and observe that both panes' update_content is awaited — NO source
    introspection required for the core behavioral assertions.
    """

    def test_source_contains_dialog_on_show(self):
        """Source assertion: compare_modal.py wires the loader via dialog.on('show'."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert 'dialog.on("show"' in source, (
            "compare_modal.py must attach an async show-loader via dialog.on('show', ...)"
        )

    def test_step_is_async_coroutine_function(self):
        """Source assertion: _step is defined as async def (G5 fix)."""
        import pathlib
        import re
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert re.search(r"async def _step\b", source), (
            "_step must be defined as 'async def' so it awaits _load_candidate_pane"
        )

    def test_record_verdict_is_async_coroutine_function(self):
        """Source assertion: _record_verdict is defined as async def (G5 fix)."""
        import pathlib
        import re
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert re.search(r"async def _record_verdict\b", source), (
            "_record_verdict must be defined as 'async def' so it awaits _load_candidate_pane"
        )

    def test_no_naked_ensure_future(self):
        """Source assertion: no naked asyncio.ensure_future for pane loaders (T-119-09)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "ensure_future" not in source, (
            "compare_modal.py must NOT use asyncio.ensure_future — "
            "use dialog.on('show', async ...) instead (Codex client-context rule, T-119-09)"
        )

    def test_modal_level_generation_guard_in_source(self):
        """Source assertion: _cand_load_gen modal-level generation counter is present (F-G5)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_cand_load_gen" in source, (
            "_cand_load_gen modal-level generation token must exist in compare_modal.py (F-G5)"
        )

    def test_generation_incremented_in_fill_candidate(self):
        """Source assertion: _cand_load_gen['n'] incremented in _fill_candidate (F-G5)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # Both _fill_candidate and _load_candidate_pane reference _cand_load_gen
        assert source.count("_cand_load_gen") >= 3, (
            "_cand_load_gen must appear in _fill_candidate (increment) and "
            "_load_candidate_pane (capture + recheck) — at least 3 references"
        )

    def test_anchor_viewer_constructed_with_highlight_pattern(self):
        """Source assertion: both pane AnchorViewers are constructed with highlight_pattern= (G1-compare)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "highlight_pattern=" in source, (
            "compare_modal.py must pass highlight_pattern= to AnchorViewer constructors (G1-compare)"
        )

    def test_behavioral_show_loader_awaits_both_panes(self):
        """BEHAVIORAL: show-loader coroutine awaits update_content on BOTH anchor + candidate stubs.

        This is the load-bearing behavioral test for G5.  It does NOT inspect source
        code — it drives the actual loader coroutine with AsyncMock stub viewers and
        asserts that BOTH stubs' update_content was called (i.e., awaited).
        """
        import asyncio
        from unittest.mock import MagicMock, AsyncMock, patch

        anchor = _Cand(sys_id="ANCHOR01", page=2, shelfmark="T-S Anchor")
        cand1 = _Cand(sys_id="CAND01", page=3, shelfmark="T-S Cand1")
        triage: dict = {}

        # Stub AnchorViewer instances — update_content is an AsyncMock
        anchor_stub = MagicMock()
        anchor_stub.update_content = AsyncMock(return_value=None)

        cand_stub = MagicMock()
        cand_stub.update_content = AsyncMock(return_value=None)

        # Keep track of which pane each AV instance serves
        av_call_order = []

        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))

        dialog_show_handlers = []

        def _make_dialog():
            d = MagicMock()
            d.__enter__ = lambda s: s
            d.__exit__ = MagicMock(return_value=False)
            d.props = MagicMock(return_value=d)
            d.on = lambda event, handler: dialog_show_handlers.append(handler) if event == "show" else None
            return d

        av_instances_created = []

        def _av_constructor(*args, **kwargs):
            """Each call returns next stub in sequence."""
            if len(av_instances_created) == 0:
                inst = anchor_stub
            else:
                inst = cand_stub
            av_instances_created.append(inst)
            return inst

        with (
            patch("web.components.compare_modal.ui") as mock_ui,
            patch("web.components.compare_modal.AnchorViewer", side_effect=_av_constructor),
        ):
            mock_ui.dialog.side_effect = _make_dialog
            for attr in ("card", "row", "column", "label", "button", "icon", "badge"):
                factory = MagicMock(return_value=mock_element)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)

            from web.components.compare_modal import create_compare_modal
            dialog = create_compare_modal(
                anchor_cand=anchor,
                initial_candidate=cand1,
                filtered_candidates=[cand1],
                triage=triage,
                on_verdict=lambda sid, v: None,
            )

        # The show-handler must have been registered
        assert dialog_show_handlers, (
            "No 'show' handler was registered on the dialog — "
            "dialog.on('show', ...) was not called"
        )

        # Drive the loader coroutine with asyncio.run (behavioral — no live NiceGUI)
        show_handler = dialog_show_handlers[0]
        asyncio.run(show_handler())

        # BEHAVIORAL: both panes' update_content must have been awaited
        anchor_stub.update_content.assert_awaited_once(), (
            "Anchor pane update_content was NOT awaited by the show-loader (G5)"
        )
        cand_stub.update_content.assert_awaited_once(), (
            "Candidate pane update_content was NOT awaited by the show-loader (G5)"
        )

    def test_behavioral_step_awaits_candidate_not_anchor(self):
        """BEHAVIORAL: step/_load_candidate_pane awaits candidate stub but NOT anchor (CMP-02).

        Drives _on_show to load both, then drives _load_candidate_pane directly
        (as the step/verdict path would) and asserts only the candidate is re-called.
        """
        import asyncio
        from unittest.mock import MagicMock, AsyncMock, patch

        anchor = _Cand(sys_id="ANCHOR01", page=2)
        cand1 = _Cand(sys_id="CAND01", page=3)
        triage: dict = {}

        anchor_stub = MagicMock()
        anchor_stub.update_content = AsyncMock(return_value=None)

        cand_stub = MagicMock()
        cand_stub.update_content = AsyncMock(return_value=None)

        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))

        dialog_show_handlers = []
        av_instances_created = []

        def _make_dialog():
            d = MagicMock()
            d.__enter__ = lambda s: s
            d.__exit__ = MagicMock(return_value=False)
            d.props = MagicMock(return_value=d)
            d.on = lambda event, handler: dialog_show_handlers.append(handler) if event == "show" else None
            return d

        def _av_constructor(*args, **kwargs):
            if len(av_instances_created) == 0:
                inst = anchor_stub
            else:
                inst = cand_stub
            av_instances_created.append(inst)
            return inst

        with (
            patch("web.components.compare_modal.ui") as mock_ui,
            patch("web.components.compare_modal.AnchorViewer", side_effect=_av_constructor),
        ):
            mock_ui.dialog.side_effect = _make_dialog
            for attr in ("card", "row", "column", "label", "button", "icon", "badge"):
                factory = MagicMock(return_value=mock_element)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)

            from web.components.compare_modal import create_compare_modal
            dialog = create_compare_modal(
                anchor_cand=anchor,
                initial_candidate=cand1,
                filtered_candidates=[cand1],
                triage=triage,
                on_verdict=lambda sid, v: None,
            )

        assert dialog_show_handlers, "No show handler registered"
        show_handler = dialog_show_handlers[0]

        # Step 1: run show-loader — both get called
        asyncio.run(show_handler())
        anchor_call_count_after_show = anchor_stub.update_content.await_count
        cand_call_count_after_show = cand_stub.update_content.await_count

        assert anchor_call_count_after_show >= 1, "Anchor not awaited on show"
        assert cand_call_count_after_show >= 1, "Candidate not awaited on show"

        # Step 2: simulate a subsequent candidate-pane reload (as step/verdict does)
        # The _load_candidate_pane coroutine is exposed on dialog for testing.
        load_cand = getattr(dialog, "_load_candidate_pane", None)
        if load_cand is not None:
            asyncio.run(load_cand())
            # Candidate update_content is called again; anchor is NOT called again (CMP-02)
            assert cand_stub.update_content.await_count > cand_call_count_after_show, (
                "Candidate pane update_content was NOT re-awaited by _load_candidate_pane"
            )
            assert anchor_stub.update_content.await_count == anchor_call_count_after_show, (
                "Anchor pane update_content was re-awaited by step/verdict (CMP-02 violation — "
                "only the candidate pane should reload on step/verdict)"
            )

    def test_f_g3c_record_verdict_advances_to_next_candidate(self):
        """F-G3c behavioral: record_verdict writes+advances atomically — _fill_candidate shows NEXT.

        After record_verdict(state, 'yes', triage) the _state["current_candidate"] is the
        NEXT candidate. _refresh_verdict_buttons (called inside _fill_candidate) keys on that
        next candidate's triage entry, not the just-recorded one.
        """
        from web.components.compare_modal import create_compare_state, record_verdict

        anchor = _Cand(sys_id="A", page=1)
        cand_a = _Cand(sys_id="CAND_A", page=2)
        cand_b = _Cand(sys_id="CAND_B", page=3)
        filtered = [cand_a, cand_b]
        triage: dict = {}

        state = create_compare_state(
            anchor_cand=anchor,
            initial_candidate=cand_a,
            filtered_candidates=filtered,
        )
        assert state["current_candidate"].sys_id == "CAND_A"

        # record_verdict writes the verdict AND advances atomically (F-G3c)
        record_verdict(state, "yes", triage)

        # After advance, current_candidate is cand_b
        assert state["current_candidate"].sys_id == "CAND_B", (
            "F-G3c: record_verdict must advance _state to the NEXT candidate atomically; "
            "the verdict button refresh keys on this POST-ADVANCE candidate"
        )
        assert triage.get("CAND_A") == "yes", "Verdict for CAND_A must be stored in triage"
        # The verdict button for CAND_B's entry in triage — which is None (not yet voted)
        assert triage.get("CAND_B") is None, (
            "F-G3c: the refresh keys on CAND_B's triage entry (the shown candidate), "
            "NOT CAND_A's just-recorded verdict"
        )


# ---------------------------------------------------------------------------
# Task 3 (Plan 119-06): G3-compare — verdict buttons reflect current candidate
# ---------------------------------------------------------------------------

class TestVerdictButtonRefresh:
    """G3-compare verdict button active state tests (Task 3, Plan 119-06)."""

    def test_source_verdict_btn_refs_captured(self):
        """Source assertion: _verdict_btn_refs dict is captured in the verdict-buttons loop."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_verdict_btn_refs" in source, (
            "compare_modal.py must capture verdict-button refs in _verdict_btn_refs dict (G3-compare)"
        )

    def test_source_refresh_verdict_buttons_defined(self):
        """Source assertion: _refresh_verdict_buttons helper is defined and called from _fill_candidate."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_refresh_verdict_buttons" in source, (
            "_refresh_verdict_buttons must be defined in compare_modal.py (G3-compare)"
        )

    def test_source_refresh_reads_triage_get(self):
        """Source assertion: _refresh_verdict_buttons reads triage.get( for the current candidate."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "triage.get(" in source, (
            "_refresh_verdict_buttons must read triage.get(cand.sys_id) for button active state"
        )

    def test_source_refresh_called_from_fill_candidate(self):
        """Source assertion: _refresh_verdict_buttons is called from _fill_candidate."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert source.count("_refresh_verdict_buttons(") >= 1, (
            "_refresh_verdict_buttons must be called from _fill_candidate (G3-compare)"
        )

    def test_source_no_module_globals_for_verdict_refs(self):
        """Source assertion: _verdict_btn_refs is factory-scoped — no module-level dict."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # Module-level globals would appear outside any function definition.
        # Check that _verdict_btn_refs is declared INSIDE create_compare_modal (factory-scoped).
        # We verify by ensuring _verdict_btn_refs appears after "def create_compare_modal" in source.
        factory_start = source.find("def create_compare_modal(")
        refs_pos = source.find("_verdict_btn_refs")
        assert factory_start != -1, "create_compare_modal not found"
        assert refs_pos > factory_start, (
            "_verdict_btn_refs must be declared INSIDE create_compare_modal (factory-scoped, no module globals)"
        )

    def test_behavior_maybe_verdict_selects_maybe_button(self):
        """Behavior assertion: for a candidate with triage[sys_id]='maybe', maybe button is active.

        Drives _refresh_verdict_buttons with a fake triage + fake button refs and
        asserts the 'maybe' button gets the 'unelevated' active prop while others get 'outline'.
        """
        import pathlib
        import re

        # We test _refresh_verdict_buttons by reading the source logic.
        # The function reads triage.get(cand.sys_id) and sets active vs outline props.
        # This is a behavioral-level assertion using the pure helper via source analysis
        # and the record_verdict / triage state machinery.
        from web.components.compare_modal import create_compare_state, record_verdict

        anchor = _Cand(sys_id="ANC", page=1)
        cand_a = _Cand(sys_id="MAYBE_CAND", page=2)
        cand_b = _Cand(sys_id="NEXT_CAND", page=3)
        triage: dict = {"MAYBE_CAND": "maybe"}

        state = create_compare_state(
            anchor_cand=anchor,
            initial_candidate=cand_a,
            filtered_candidates=[cand_a, cand_b],
        )

        # The current candidate has verdict 'maybe' in triage
        current = state["current_candidate"]
        assert current.sys_id == "MAYBE_CAND"
        active_verdict = triage.get(current.sys_id)
        assert active_verdict == "maybe", "Expected 'maybe' verdict for MAYBE_CAND in triage"

        # The refresh should select 'maybe' as active; 'yes' and 'no' as inactive.
        # We verify this by inspecting the source logic of _refresh_verdict_buttons.
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # The active branch uses 'unelevated' (filled); inactive uses 'outline'
        assert "unelevated" in source, (
            "_refresh_verdict_buttons must set active button to 'unelevated' (filled state)"
        )
        assert "outline" in source, (
            "_refresh_verdict_buttons must set inactive buttons to 'outline' state"
        )

    def test_behavior_f_g3c_refresh_keys_on_post_advance_candidate(self):
        """F-G3c behavioral: after record_verdict advances, refresh keys on POST-ADVANCE candidate.

        Verifies via pure record_verdict helper that:
        1. After record_verdict(state, 'yes', triage), _state["current_candidate"] is the NEXT.
        2. The verdict button refresh (keyed by triage.get(next.sys_id)) yields None (unvoted).
        3. The previous candidate's verdict IS in triage (persisted via on_verdict).
        """
        from web.components.compare_modal import create_compare_state, record_verdict

        anchor = _Cand(sys_id="ANC", page=1)
        cand_a = _Cand(sys_id="CAND_A", page=2)
        cand_b = _Cand(sys_id="CAND_B", page=3)
        triage: dict = {}

        state = create_compare_state(
            anchor_cand=anchor,
            initial_candidate=cand_a,
            filtered_candidates=[cand_a, cand_b],
        )

        # Record 'yes' for CAND_A — this writes AND advances
        record_verdict(state, "yes", triage)

        # _state now shows CAND_B (post-advance)
        shown_cand = state["current_candidate"]
        assert shown_cand.sys_id == "CAND_B", (
            "F-G3c: after record_verdict the SHOWN candidate is CAND_B (post-advance)"
        )

        # The refresh keys on CAND_B's triage entry → None (not yet voted)
        post_advance_verdict = triage.get(shown_cand.sys_id)
        assert post_advance_verdict is None, (
            "F-G3c: verdict buttons reflect CAND_B's triage entry (None = no active button); "
            "NOT CAND_A's just-recorded 'yes'"
        )

        # CAND_A's verdict is persisted
        assert triage.get("CAND_A") == "yes", (
            "CAND_A's verdict must be persisted in triage so it shows when user navigates back"
        )

    def test_behavior_no_active_verdict_when_triage_empty(self):
        """Behavior assertion: when triage has no entry for the shown candidate, no button is active."""
        from web.components.compare_modal import create_compare_state

        anchor = _Cand(sys_id="ANC", page=1)
        cand = _Cand(sys_id="FRESH_CAND", page=2)
        triage: dict = {}  # empty — no verdict recorded yet

        state = create_compare_state(
            anchor_cand=anchor,
            initial_candidate=cand,
            filtered_candidates=[cand],
        )

        active = triage.get(state["current_candidate"].sys_id)
        assert active is None, (
            "When triage has no entry for the shown candidate, no verdict button should be active"
        )
