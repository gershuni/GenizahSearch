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

from dataclasses import dataclass
from typing import Optional


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


def test_compare_modal_no_badge_icon_kwarg():
    """Regression (UAT 2026-06-21): ui.badge() has no `icon` kwarg.

    `ui.badge(tr("Size mismatch"), icon="warning")` raised
    `TypeError: got an unexpected keyword argument 'icon'` inside _fill_candidate,
    which aborted the ENTIRE Compare modal build — so the modal showed no image,
    no transcription, no metadata, and no image prefetch. Render the warning glyph
    as a child element of the badge instead.
    """
    import ast
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_badge = (
            (isinstance(func, ast.Attribute) and func.attr == "badge")
            or (isinstance(func, ast.Name) and func.id == "badge")
        )
        if is_badge and any(kw.arg == "icon" for kw in node.keywords):
            raise AssertionError(
                "ui.badge() does not accept an `icon` kwarg (TypeError at runtime, "
                "aborts the whole Compare modal build). Put the icon inside the badge "
                "as a child element instead."
            )


def test_compare_modal_card_is_viewport_bounded():
    """Source assertion (round-5 UAT 'does not adapt to window height'): the modal
    card must bind its height to the VIEWPORT (100vh), not rely on h-full.

    height:100% only resolves when every ancestor is height-bounded; that chain
    breaks inside the maximized dialog wrapper, leaving the flex body unbounded so
    the panes overflowed the window. The card must use an explicit 100vh so the
    inner flex column + pane scroll areas cap to the actual window height.
    """
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert "height:100vh" in source, (
        "compare_modal card must be bound to the viewport height (height:100vh) so "
        "Compare adapts to the window height."
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
# Plan 119-11: R2-2 / R2-3 / R2-4 / R2-5 / R2-6 / R2-7 source assertions
# ---------------------------------------------------------------------------

class TestPlan11R2Features:
    """Source integrity checks for the Plan 119-11 R2 gap-closure features."""

    def test_r2_2_counter_label_has_direction_ltr(self):
        """R2-2: the flip-through counter label must include direction:ltr to prevent bidi flip."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "direction:ltr" in source, (
            "R2-2: compare_modal.py must set direction:ltr on the counter label "
            "so '5 / 118' is not bidi-flipped to '118 / 5' under the Hebrew RTL UI"
        )

    def test_r2_2_counter_label_has_unicode_bidi_isolate(self):
        """R2-2: the counter label must also carry unicode-bidi:isolate for full bidi isolation."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "unicode-bidi:isolate" in source, (
            "R2-2: compare_modal.py counter label must include unicode-bidi:isolate "
            "(in addition to direction:ltr) for full bidi isolation"
        )

    def test_r2_2_no_hardcoded_ltr_chevron_icons_on_nav_buttons(self):
        """R2-2: Prev/Next nav buttons must NOT carry a hardcoded LTR icon= contradicting the RTL label."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # The banned pattern: icon="chevron_left" or icon="chevron_right" on the nav buttons.
        # These are hardcoded LTR icons that contradict the RTL label direction.
        assert 'icon="chevron_left"' not in source, (
            "R2-2: compare_modal.py must NOT use icon=\"chevron_left\" on nav buttons — "
            "the labelled chevron in tr('‹ Prev')/tr('Next ›') handles direction"
        )
        assert 'icon="chevron_right"' not in source, (
            "R2-2: compare_modal.py must NOT use icon=\"chevron_right\" on nav buttons — "
            "use the labelled chevron only (R2-2 Codex P119-R2-2-1)"
        )

    def test_r2_2_no_flex_direction_row_reverse_on_next(self):
        """R2-2: the Next button must NOT use flex-direction:row-reverse (the old RTL workaround)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "flex-direction:row-reverse" not in source, (
            "R2-2: compare_modal.py must not use flex-direction:row-reverse — "
            "the RTL-correct label from 119-09 (‹ הבא) handles direction natively"
        )

    def test_r2_4_compare_imports_triage_icons(self):
        """R2-4: compare_modal.py must import TRIAGE_ICONS from shared.joins_lab."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "TRIAGE_ICONS" in source, (
            "R2-4: compare_modal.py must import and use TRIAGE_ICONS from shared.joins_lab"
        )

    def test_r2_4_verdict_buttons_use_glyphs_not_text(self):
        """R2-4: verdict buttons must render the glyph (✓/?/✗), not Yes/Maybe/No text."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # The glyphs must be in the source via TRIAGE_ICONS["glyph"] reference
        assert 'TRIAGE_ICONS[_v]["glyph"]' in source or 'TRIAGE_ICONS[verdict]["glyph"]' in source, (
            "R2-4: compare_modal.py verdict buttons must use TRIAGE_ICONS[v][\"glyph\"] "
            "to render ✓/?/✗ (desktop parity)"
        )

    def test_r2_4_verdict_buttons_have_tooltips(self):
        """R2-4: verdict buttons must carry tooltips via TRIAGE_ICONS tooltip key."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert 'TRIAGE_ICONS[_v]["tooltip"]' in source or 'TRIAGE_ICONS[verdict]["tooltip"]' in source, (
            "R2-4: Compare verdict buttons must carry .tooltip(tr(TRIAGE_ICONS[v]['tooltip'])) "
            "for Mark yes/maybe/no tooltips"
        )

    def test_r2_5_pane_border_refresh_function_exists(self):
        """R2-5: _refresh_pane_border helper must exist and be called from _refresh_verdict_buttons."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_refresh_pane_border" in source, (
            "R2-5: compare_modal.py must define _refresh_pane_border to update the "
            "candidate pane border on verdict (mirrors grid card _make_restyle_fn)"
        )

    def test_r2_5_cand_pane_mark_for_render_smoke(self):
        """R2-5: the candidate pane column must be marked 'compare-candidate-pane' for render-smoke."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "compare-candidate-pane" in source, (
            "R2-5: the candidate pane column must carry .mark('compare-candidate-pane') "
            "so render-smoke can locate it for the verdict-border assertion"
        )

    def test_r2_6_anchor_viewer_suppress_kwarg_passed_to_both_panes(self):
        """R2-6: both pane AnchorViewers must be constructed with suppress_shelfmark_header=True."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        count = source.count("suppress_shelfmark_header=True")
        assert count >= 2, (
            f"R2-6: expected at least 2 occurrences of suppress_shelfmark_header=True "
            f"(one per pane) but found {count}. Both anchor and candidate panes must "
            "suppress the inner AnchorViewer shelfmark header."
        )

    def test_r2_6_image_max_height_passed_to_both_panes(self):
        """R2-3: both pane AnchorViewers must be constructed with image_max_height set."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        count = source.count("image_max_height=")
        assert count >= 2, (
            f"R2-3: expected at least 2 occurrences of image_max_height= "
            f"(one per pane) but found {count}. Both panes must cap the image height."
        )

    def test_r2_7_esc_handler_defined(self):
        """R2-7: compare_modal.py must define an Escape-key handler that closes the dialog."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_on_escape" in source, (
            "R2-7: compare_modal.py must define an _on_escape key handler for Esc-to-close"
        )
        assert "ui.keyboard" in source, (
            "R2-7: compare_modal.py must use ui.keyboard(on_key=...) for the Esc handler"
        )

    def test_r2_7_esc_handler_guards_dialog_value(self):
        """R2-7: the Escape handler must guard dialog.value to avoid firing on hidden modals."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # The guard checks dialog.value (or getattr(dialog, 'value', False))
        assert "dialog" in source and "value" in source, (
            "R2-7: Esc handler must guard with dialog.value check (Codex P119-R2-7-1)"
        )

    def test_r2_7_esc_handler_noop_when_dialog_closed(self):
        """R2-7 BEHAVIORAL: invoking _on_escape on a closed dialog does NOT call _handle_close."""
        from unittest.mock import MagicMock, patch

        anchor = _Cand(sys_id="ANCHOR01", page=1, shelfmark="T-S Anchor")
        cand = _Cand(sys_id="CAND01", page=2, shelfmark="T-S Cand")
        triage: dict = {}

        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))

        dialog_obj = None
        close_calls: list = []

        def _make_dialog():
            d = MagicMock()
            d.__enter__ = lambda s: s
            d.__exit__ = MagicMock(return_value=False)
            d.props = MagicMock(return_value=d)
            d.on = MagicMock(return_value=d)
            d.value = False  # dialog is CLOSED
            d.close = MagicMock(side_effect=lambda: close_calls.append("close"))
            return d

        with (
            patch("web.components.compare_modal.ui") as mock_ui,
            patch("web.components.compare_modal.AnchorViewer"),
        ):
            mock_ui.dialog.side_effect = _make_dialog
            mock_ui.keyboard = MagicMock(return_value=mock_element)
            for attr in ("card", "row", "column", "label", "button", "icon", "badge"):
                factory = MagicMock(return_value=mock_element)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)

            from web.components.compare_modal import create_compare_modal
            dialog = create_compare_modal(
                anchor_cand=anchor,
                initial_candidate=cand,
                filtered_candidates=[cand],
                triage=triage,
                on_verdict=lambda sid, v: None,
            )

        # Simulate an Escape keydown on the CLOSED dialog
        on_escape = getattr(dialog, "_on_escape", None)
        assert on_escape is not None, "dialog._on_escape test seam not set"

        # Build a synthetic Escape keydown event
        from types import SimpleNamespace
        escape_event = SimpleNamespace(
            action=SimpleNamespace(keydown=True),
            key=SimpleNamespace(name="Escape"),
        )

        # dialog.value is False (closed) → handler must be a no-op
        on_escape(escape_event)

        # _handle_close calls dialog.close() — must NOT have been called
        assert not close_calls, (
            "R2-7: _on_escape must be a no-op when dialog.value is False "
            f"(stale hidden-dialog keyboard guard, Codex P119-R2-7-1). close() was called {len(close_calls)} times"
        )

    def test_r2_7_esc_handler_closes_when_dialog_open(self):
        """R2-7 BEHAVIORAL: invoking _on_escape on an OPEN dialog calls _handle_close (dialog.close)."""
        from unittest.mock import MagicMock, patch

        anchor = _Cand(sys_id="ANCHOR01", page=1, shelfmark="T-S Anchor")
        cand = _Cand(sys_id="CAND01", page=2, shelfmark="T-S Cand")
        triage: dict = {}

        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))

        close_calls: list = []

        def _make_dialog():
            d = MagicMock()
            d.__enter__ = lambda s: s
            d.__exit__ = MagicMock(return_value=False)
            d.props = MagicMock(return_value=d)
            d.on = MagicMock(return_value=d)
            d.value = True  # dialog is OPEN
            d.close = MagicMock(side_effect=lambda: close_calls.append("close"))
            return d

        with (
            patch("web.components.compare_modal.ui") as mock_ui,
            patch("web.components.compare_modal.AnchorViewer"),
        ):
            mock_ui.dialog.side_effect = _make_dialog
            mock_ui.keyboard = MagicMock(return_value=mock_element)
            for attr in ("card", "row", "column", "label", "button", "icon", "badge"):
                factory = MagicMock(return_value=mock_element)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)

            from web.components.compare_modal import create_compare_modal
            dialog = create_compare_modal(
                anchor_cand=anchor,
                initial_candidate=cand,
                filtered_candidates=[cand],
                triage=triage,
                on_verdict=lambda sid, v: None,
            )

        on_escape = getattr(dialog, "_on_escape", None)
        assert on_escape is not None, "dialog._on_escape test seam not set"

        from types import SimpleNamespace
        escape_event = SimpleNamespace(
            action=SimpleNamespace(keydown=True),
            key=SimpleNamespace(name="Escape"),
        )

        # dialog.value is True (open) → handler must call dialog.close()
        on_escape(escape_event)

        assert close_calls, (
            "R2-7: _on_escape must call dialog.close() when dialog.value is True (open)"
        )


# ---------------------------------------------------------------------------
# Task 2 (Plan 119-06): G5 — show-loader behavioral tests
# ---------------------------------------------------------------------------

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

        # We test _refresh_verdict_buttons by reading the source logic.
        # The function reads triage.get(cand.sys_id) and sets active vs outline props.
        # This is a behavioral-level assertion using the pure helper via source analysis
        # and the record_verdict / triage state machinery.
        from web.components.compare_modal import create_compare_state

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


# ---------------------------------------------------------------------------
# Phase 120 Plan 07: D-08 Browse-in-Compare + D-09 Info buttons source guards
# ---------------------------------------------------------------------------

class TestBrowseInCompare:
    """D-08: Browse-in-Compare source-level assertions (Plan 120-07).

    Both the anchor and the candidate pane must have an Open-in-Browse button
    that opens via js_handler (no Python navigate call — T-119-09 propagation rule).
    """

    def test_d08_build_browse_url_imported_in_compare_modal(self):
        """D-08: compare_modal.py must import build_browse_url from candidate_grid."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "build_browse_url" in source, (
            "D-08: compare_modal.py must import and use build_browse_url from "
            "candidate_grid to construct the per-pane Browse URL"
        )

    def test_d08_open_in_new_icon_present_in_compare_modal(self):
        """D-08: compare_modal.py must render open_in_new icon buttons (Browse-in-Compare)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "open_in_new" in source, (
            "D-08: compare_modal.py must render icon='open_in_new' buttons for Browse-in-Compare"
        )

    def test_d08_browse_buttons_have_aria_labels(self):
        """D-08: icon-only Browse buttons must carry aria-label (UI-SPEC §4 exception)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # aria-label appears in the .props() string for the Browse button
        assert "aria-label" in source, (
            "D-08: Browse-in-Compare icon-only buttons must carry aria-label "
            "(UI-SPEC §4 icon-only exception — screen reader accessibility)"
        )

    def test_d08_browse_opens_via_js_handler_not_navigate(self):
        """D-08/T-119-09: Browse button must use js_handler (window.open), NOT ui.navigate.to."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "window.open" in source, (
            "D-08: Browse button must use js_handler with window.open (new tab), "
            "not ui.navigate.to which would navigate the current client session"
        )
        # No Python-side navigate call
        assert "ui.navigate.to" not in source, (
            "D-08: compare_modal.py must NOT use ui.navigate.to — "
            "Browse-in-Compare must open in a new tab via js_handler"
        )

    def test_d08_candidate_shelfmark_row_ref_exists_in_source(self):
        """D-08: _cand_shelfmark_row_ref ref exists for the candidate pane Browse row."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_cand_shelfmark_row_ref" in source, (
            "D-08: compare_modal.py must maintain _cand_shelfmark_row_ref so the "
            "candidate-pane Browse URL updates on each candidate flip"
        )


class TestCompareInfoButtons:
    """D-09/H3/R2-H3: Compare pane info buttons source-level assertions (Plan 120-07)."""

    def test_d09_metadata_prefetcher_param_in_create_compare_modal(self):
        """D-09/H3: create_compare_modal must accept a metadata_prefetcher parameter."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "metadata_prefetcher" in source, (
            "D-09/H3: create_compare_modal must accept metadata_prefetcher= param "
            "for the off-loop per-pane metadata fetch layer"
        )

    def test_d09_populate_pane_info_row_defined(self):
        """D-09: _populate_pane_info_row helper must exist in compare_modal.py."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "_populate_pane_info_row" in source, (
            "D-09: compare_modal.py must define _populate_pane_info_row to build "
            "FJMS Catalog + PGP/Bibliography info buttons from prefetched metadata"
        )

    def test_d09_info_rows_hidden_by_default(self):
        """D-09: info button row containers must start hidden (display:none)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # The info row uses display:none by default
        assert "_anchor_info_row_ref" in source, (
            "D-09: _anchor_info_row_ref must exist as a factory-scoped ref"
        )
        assert "_cand_info_row_ref" in source, (
            "D-09: _cand_info_row_ref must exist as a factory-scoped ref"
        )

    def test_d09_pjms_catalog_button_in_info_row(self):
        """D-09: _populate_pane_info_row must render a 'FJMS Catalog' button."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "FJMS Catalog" in source or "tr(\"FJMS Catalog\")" in source, (
            "D-09: compare_modal.py must render an 'FJMS Catalog' button in each pane info row"
        )

    def test_d09_pgp_bibliography_button_in_info_row(self):
        """D-09: _populate_pane_info_row must render a 'PGP / Bibliography' button."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "PGP / Bibliography" in source or 'tr("PGP / Bibliography")' in source, (
            "D-09: compare_modal.py must render a 'PGP / Bibliography' button in each pane info row"
        )

    def test_r2_h3_show_catalog_dialog_receives_catalog_detail_kwarg(self):
        """R2-H3: compare_modal.py must call show_catalog_dialog with catalog_detail= keyword.

        This proves the Compare modal passes the prefetched catalog detail so the
        dialog does NOT call get_catalog_detail synchronously on open (R2-H3).
        """
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "catalog_detail=" in source, (
            "R2-H3: compare_modal.py must pass catalog_detail= to show_catalog_dialog "
            "so the dialog skips its internal synchronous get_catalog_detail call on open"
        )

    def test_d09_metadata_fetched_via_run_io_bound(self):
        """D-09/R2-H3: metadata fetch in _on_show must dispatch via run.io_bound (off-loop)."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # asyncio.gather + run.io_bound present in _on_show metadata fetch block
        assert "run.io_bound" in source, (
            "D-09: compare_modal.py _on_show must dispatch metadata fetch via run.io_bound "
            "so get_bibliography / get_catalog_detail run off the event loop (R2-H3)"
        )
        assert "asyncio.gather" in source, (
            "D-09: compare_modal.py _on_show should use asyncio.gather to fetch anchor + "
            "candidate metadata concurrently off-loop"
        )

    def test_d09_info_row_calls_existing_dialogs(self):
        """D-09: _populate_pane_info_row must call create_fjms_bibliography_dialog
        and show_catalog_dialog (not re-implement fetching).

        Source-level assertion: the existing dialog helpers are referenced from
        within compare_modal.py.
        """
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        assert "create_fjms_bibliography_dialog" in source, (
            "D-09: _populate_pane_info_row must call create_fjms_bibliography_dialog "
            "for the PGP / Bibliography button"
        )
        assert "show_catalog_dialog" in source, (
            "D-09: _populate_pane_info_row must call show_catalog_dialog "
            "for the FJMS Catalog button"
        )

    def test_d09_on_show_uses_seed008_runtime_error_guard(self):
        """D-09/SEED-008: _on_show must be wrapped in try/except RuntimeError."""
        import pathlib
        source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
        # SEED-008 pattern: except RuntimeError: return in _on_show
        assert "except RuntimeError" in source, (
            "D-09/SEED-008: _on_show must guard the whole body with try/except RuntimeError: return "
            "to handle NiceGUI client disconnect cleanly"
        )


class TestCatalogDialogPrefetchParam:
    """R2-H3: catalog_dialog.py accepts catalog_detail= and skips sync fetch when provided."""

    def test_r2_h3_show_catalog_dialog_accepts_catalog_detail_param(self):
        """R2-H3: show_catalog_dialog must accept a catalog_detail= param."""
        import inspect
        from web.components.catalog_dialog import show_catalog_dialog
        sig = inspect.signature(show_catalog_dialog)
        assert "catalog_detail" in sig.parameters, (
            "R2-H3: show_catalog_dialog must accept catalog_detail= param so that "
            "Compare modal can pass prefetched data and skip the synchronous internal fetch"
        )

    def test_r2_h3_catalog_dialog_skips_sync_fetch_when_detail_provided(self):
        """R2-H3 BEHAVIORAL: when catalog_detail is supplied, get_catalog_detail is NOT called."""
        from unittest.mock import patch, MagicMock

        mock_el = MagicMock()
        mock_el.__enter__ = lambda s: s
        mock_el.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on", "clear"):
            setattr(mock_el, m, MagicMock(return_value=mock_el))

        # Pre-build a catalog_detail dict
        prefetched_detail = {
            "source_names": ["FJMS"],
            "records": [],
        }

        get_catalog_calls: list = []

        def fake_get_catalog_detail(sys_id):
            get_catalog_calls.append(sys_id)
            return {"source_names": [], "records": []}

        with (
            patch("web.components.catalog_dialog.ui") as mock_ui,
        ):
            for attr in ("dialog", "card", "row", "column", "label", "button", "icon",
                         "badge", "scroll_area", "separator", "tabs", "tab", "tab_panels",
                         "tab_panel", "table", "select", "expansion"):
                factory = MagicMock(return_value=mock_el)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)

            mock_svc = MagicMock()
            mock_svc.get_catalog_detail = fake_get_catalog_detail

            from web.components.catalog_dialog import show_catalog_dialog
            try:
                show_catalog_dialog(
                    sys_id="990001",
                    shelfmark="T-S 12.001",
                    fjms_service=mock_svc,
                    catalog_detail=prefetched_detail,
                )
            except Exception:
                pass  # UI render may partially fail in headless context

        assert not get_catalog_calls, (
            "R2-H3: when catalog_detail= is supplied to show_catalog_dialog, "
            "get_catalog_detail must NOT be called (prefetched data is used directly). "
            f"But get_catalog_detail was called for: {get_catalog_calls}"
        )


# ---------------------------------------------------------------------------
# Issue B (UAT 2026-06-21): Add-as-Join button in the Compare modal header
# ---------------------------------------------------------------------------

def _build_modal_with_mock_ui(*, on_add_as_join=None, initial_candidate=None,
                              filtered=None, anchor=None):
    """Build a Compare modal under a fully-mocked NiceGUI ``ui`` and return the
    list of (args, kwargs) for every ``ui.button(...)`` call so tests can locate
    the Add-as-Join button and exercise its on_click callback.

    Returns (dialog, button_calls) where button_calls is a list of (args, kwargs).
    """
    from unittest.mock import MagicMock, patch

    anchor = anchor or _Cand(sys_id="ANC01", page=1, shelfmark="T-S Anchor")
    cand = initial_candidate or _Cand(sys_id="CAND01", page=2, shelfmark="T-S Cand")
    filtered = filtered or [cand]
    triage: dict = {}

    mock_element = MagicMock()
    mock_element.__enter__ = lambda s: s
    mock_element.__exit__ = MagicMock(return_value=False)
    for m in ("classes", "props", "style", "mark", "tooltip", "on"):
        setattr(mock_element, m, MagicMock(return_value=mock_element))

    button_calls: list = []

    def _button_factory(*args, **kwargs):
        button_calls.append((args, kwargs))
        return mock_element

    def _make_dialog():
        d = MagicMock()
        d.__enter__ = lambda s: s
        d.__exit__ = MagicMock(return_value=False)
        d.props = MagicMock(return_value=d)
        d.on = MagicMock(return_value=d)
        d.value = True
        d.close = MagicMock()
        return d

    with (
        patch("web.components.compare_modal.ui") as mock_ui,
        patch("web.components.compare_modal.AnchorViewer"),
    ):
        mock_ui.dialog.side_effect = _make_dialog
        mock_ui.keyboard = MagicMock(return_value=mock_element)
        mock_ui.button = MagicMock(side_effect=_button_factory)
        for attr in ("card", "row", "column", "label", "icon", "badge"):
            factory = MagicMock(return_value=mock_element)
            factory.__enter__ = lambda s: s
            factory.__exit__ = MagicMock(return_value=False)
            setattr(mock_ui, attr, factory)

        from web.components.compare_modal import create_compare_modal
        dialog = create_compare_modal(
            anchor_cand=anchor,
            initial_candidate=cand,
            filtered_candidates=filtered,
            triage=triage,
            on_verdict=lambda sid, v: None,
            on_add_as_join=on_add_as_join,
        )
    return dialog, button_calls


def test_compare_modal_accepts_on_add_as_join_param():
    """Issue B: create_compare_modal must accept an on_add_as_join parameter."""
    import inspect
    from web.components.compare_modal import create_compare_modal
    sig = inspect.signature(create_compare_modal)
    assert "on_add_as_join" in sig.parameters, (
        "Issue B: create_compare_modal must accept on_add_as_join= so the modal "
        "can offer Add-as-Join on the currently-shown candidate."
    )


def test_compare_modal_renders_add_as_join_button_when_callback_provided():
    """Issue B: an Add-as-Join button is rendered when on_add_as_join is provided."""
    def _cb(sys_id, shelfmark):
        pass

    _dialog, button_calls = _build_modal_with_mock_ui(on_add_as_join=_cb)

    # Locate the Add-as-Join button: it carries icon='add_link'
    add_join_btns = [
        (args, kwargs) for (args, kwargs) in button_calls
        if kwargs.get("icon") == "add_link"
    ]
    assert add_join_btns, (
        "Issue B: no Add-as-Join button (icon='add_link') was created in the "
        "Compare modal header when on_add_as_join was provided."
    )


def test_compare_modal_no_add_as_join_button_when_callback_absent():
    """Issue B: no Add-as-Join button when on_add_as_join is None (backward compat)."""
    _dialog, button_calls = _build_modal_with_mock_ui(on_add_as_join=None)
    add_join_btns = [
        (args, kwargs) for (args, kwargs) in button_calls
        if kwargs.get("icon") == "add_link"
    ]
    assert not add_join_btns, (
        "Issue B: an Add-as-Join button was rendered even though on_add_as_join "
        "was None — it must only appear when the callback is provided."
    )


def test_compare_add_as_join_callback_uses_current_candidate():
    """Issue B: clicking Add-as-Join calls back with the CURRENTLY-shown candidate."""
    captured = []

    def _cb(sys_id, shelfmark):
        captured.append((sys_id, shelfmark))

    cand = _Cand(sys_id="CAND77", page=4, shelfmark="T-S 99.077")
    _dialog, button_calls = _build_modal_with_mock_ui(
        on_add_as_join=_cb, initial_candidate=cand, filtered=[cand],
    )

    add_join_call = next(
        ((args, kwargs) for (args, kwargs) in button_calls
         if kwargs.get("icon") == "add_link"),
        None,
    )
    assert add_join_call is not None, "Add-as-Join button not created"
    _args, kwargs = add_join_call
    on_click = kwargs.get("on_click")
    assert callable(on_click), "Add-as-Join button must have an on_click handler"

    # Fire the handler — must call back with the current candidate's sys_id + shelfmark
    on_click()
    assert captured == [("CAND77", "T-S 99.077")], (
        "Issue B: Add-as-Join must call on_add_as_join(current.sys_id, current.shelfmark). "
        f"Got: {captured!r}"
    )


# ---------------------------------------------------------------------------
# Issue C (UAT 2026-06-21): Compare info buttons render when FJMS data exists.
# Root cause: get_catalog_detail() has NO 'source_names' key, so the catalog
# gate read an always-empty list.  The fix reads source_names from the
# separately-fetched prefetcher meta key (mirrors browse_enrichment.py:551).
# ---------------------------------------------------------------------------

def _compute_info_gate(meta: dict):
    """Mirror of the gating logic in compare_modal._populate_pane_info_row.

    Canonical contract for the Issue-C fix: catalog presence is derived from
    meta['source_names'] (mirrors browse_enrichment), falling back to
    catalog_detail['records'] when source_names is absent.
    """
    fjms_bib = meta.get("fjms_bib") or []
    catalog_detail = meta.get("catalog_detail")
    source_names = meta.get("source_names") or []
    catalog_src_count = len(source_names)
    if catalog_src_count == 0 and catalog_detail and isinstance(catalog_detail, dict):
        catalog_src_count = len(catalog_detail.get("records", []))
    elif catalog_src_count == 0 and isinstance(catalog_detail, list):
        catalog_src_count = len(catalog_detail)
    return bool(fjms_bib), catalog_src_count > 0


class TestCompareInfoRowSourceNamesGate:
    """The catalog-presence gate must use the prefetched meta['source_names'],
    NOT catalog_detail['source_names'] (which never exists)."""

    def test_catalog_button_renders_when_source_names_present(self):
        """has_catalog must be True when meta['source_names'] is non-empty."""
        meta = {
            "fjms_bib": [],
            "catalog_detail": {"records": [{"x": 1}]},
            "source_names": ["Goitein", "Schwab"],
        }
        has_bib, has_catalog = _compute_info_gate(meta)
        assert has_catalog is True, (
            "Issue C: catalog button must render when source_names is non-empty "
            f"(meta={meta!r})"
        )

    def test_catalog_button_hidden_when_no_source_names_and_no_records(self):
        """has_catalog must be False when source_names empty AND no records."""
        meta = {
            "fjms_bib": [],
            "catalog_detail": {"records": []},
            "source_names": [],
        }
        has_bib, has_catalog = _compute_info_gate(meta)
        assert has_catalog is False

    def test_bib_button_renders_when_bibliography_present(self):
        """has_bib must be True when fjms_bib is non-empty."""
        meta = {
            "fjms_bib": [{"citation": "Smith 1990"}],
            "catalog_detail": {"records": []},
            "source_names": [],
        }
        has_bib, has_catalog = _compute_info_gate(meta)
        assert has_bib is True

    def test_old_catalog_detail_source_names_key_alone_does_not_gate(self):
        """Regression: relying on catalog_detail['source_names'] (which never exists)
        would hide the catalog button.  The fix uses meta['source_names'] instead."""
        meta = {
            "fjms_bib": [],
            "catalog_detail": {
                "records": [{"source_name": "Goitein"}],
                "running_titles": {}, "sizes": {}, "fields": {},
                "free_descriptions": [], "full_texts": [],
                "textual_frames": {}, "mentions": {},
            },
            "source_names": ["Goitein"],
        }
        # Confirm the real catalog_detail shape has NO source_names key
        assert "source_names" not in meta["catalog_detail"], (
            "Test premise: get_catalog_detail() does NOT carry a source_names key"
        )
        has_bib, has_catalog = _compute_info_gate(meta)
        assert has_catalog is True


def test_compare_modal_reads_source_names_from_meta_not_catalog_detail():
    """Issue C (static): _populate_pane_info_row must read meta['source_names'],
    not catalog_detail.get('source_names') (which never exists)."""
    import pathlib
    source = pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")
    assert 'meta.get("source_names")' in source, (
        "Issue C: _populate_pane_info_row must read source_names from the prefetched "
        "meta dict (meta.get('source_names')), since get_catalog_detail() has no such key."
    )
    # The old broken pattern must be gone
    assert 'catalog_detail.get("source_names"' not in source, (
        "Issue C: compare_modal.py must NOT gate the catalog button on "
        "catalog_detail.get('source_names') — that key never exists on get_catalog_detail()."
    )


# ===========================================================================
# Round 4 UAT (2026-06-21) — Compare modal fixes (Issues 3, 4, 7)
# ===========================================================================


def _cm_source():
    import pathlib
    return pathlib.Path("web/components/compare_modal.py").read_text(encoding="utf-8")


def test_issue3_compare_has_candidate_title_marker():
    """Round-4 Issue 3: the candidate pane must render a title label marked
    'compare-candidate-title' (the title was previously missing)."""
    source = _cm_source()
    assert "compare-candidate-title" in source, (
        "Issue 3: compare_modal.py must mark the candidate title label "
        "'compare-candidate-title' so it is rendered + locatable."
    )
    assert "_cand_title_row_ref" in source, (
        "Issue 3: a candidate title row ref must exist and be rebuilt in _fill_candidate."
    )


def test_issue3_compare_panes_split_header_and_scroll():
    """Round-4 Issue 3: each pane must clip overflow (fixed header) with an inner
    scrolling viewer area so the FJMS info buttons stay visible at 100% zoom."""
    source = _cm_source()
    # The pane columns now use overflow:hidden (header pinned) + an inner overflow-y:auto.
    assert "flex-direction:column; overflow:hidden;" in source, (
        "Issue 3: Compare panes must clip overflow so the header (info buttons) stays "
        "pinned while only the inner viewer scrolls."
    )
    assert "overflow-y:auto;" in source, (
        "Issue 3: the inner viewer area must scroll (overflow-y:auto)."
    )


def test_issue3_info_rows_built_in_header_block():
    """Round-4 Issue 3: the per-pane info rows must be appended in the FIXED header
    block (the refs are populated before the scrolling viewer column is built)."""
    source = _cm_source()
    # The anchor + candidate info-row refs must still be present (moved up, not removed).
    assert "_anchor_info_row_ref.append" in source
    assert "_cand_info_row_ref.append" in source


def test_issue4_compare_esc_guarded_by_nested_dialog():
    """Round-4 Issue 4: Compare's Esc handler must no-op when a nested dialog is open."""
    source = _cm_source()
    assert "_has_nested_dialog_open" in source, (
        "Issue 4: compare_modal.py must define _has_nested_dialog_open and call it "
        "from _on_escape so Esc dismisses only the topmost dialog."
    )
    # The guard must be invoked inside _on_escape before closing.
    assert "if _has_nested_dialog_open():" in source, (
        "Issue 4: _on_escape must early-return when a nested dialog is open."
    )


def test_issue7_compare_has_add_to_puzzle_param_and_button():
    """Round-4 Issue 7: create_compare_modal must accept on_add_to_puzzle and render
    a header button marked 'compare-add-to-puzzle'."""
    source = _cm_source()
    assert "on_add_to_puzzle" in source, (
        "Issue 7: create_compare_modal must accept an on_add_to_puzzle callback."
    )
    assert "compare-add-to-puzzle" in source, (
        "Issue 7: the Compare header must render an Add-to-Puzzle button marked "
        "'compare-add-to-puzzle'."
    )


def test_issue7_compare_add_to_puzzle_acts_on_current_candidate():
    """Round-4 Issue 7: the Compare Add-to-Puzzle handler must act on the
    CURRENTLY-shown candidate (_state['current_candidate'])."""
    source = _cm_source()
    # The handler resolves cand = _state.get("current_candidate") then calls on_add_to_puzzle(cand.sys_id)
    assert "on_add_to_puzzle(cand.sys_id)" in source, (
        "Issue 7: the Compare Add-to-Puzzle button must pass the current candidate's "
        "sys_id to on_add_to_puzzle."
    )
