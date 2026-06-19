# -*- coding: utf-8 -*-
"""RED test scaffold for CND-08: Candidate metadata enrichment (off-loop, batched).

Requirement: CND-08
Wave that turns this green: Wave 2 (Plan 119-04)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (enrichment
dispatch helpers in joins_lab.py) are not yet implemented.  They form the failing
seams that Wave 2 will make green.

Design intent (per CONTEXT.md D-16 / RESEARCH.md Pattern):
  - Off-loop batched enrichment via get_measurement_summaries_batch(sys_ids)
  - LOCAL fjms_enrichment.db SQLite read — no NLI circuit breaker needed
  - Batch covers the FULL filtered set (not just current page)
  - Degrades gracefully: returns {} if fjms service unavailable
  - Feeds filter predicates (material / dimensions / size-mismatch) + table columns
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — enrichment batch helper not yet implemented",
    strict=False,
)
def test_enrichment_batch_covers_full_filtered_set():
    """CND-08: enrichment batch sys_ids covers ALL candidates in the filtered set, not just current page.

    The enrichment must be dispatched with the full filtered set's sys_ids so that filters
    (material/size-mismatch) evaluate correctly even for candidates on later pages.
    """
    from web.pages.joins_lab import _get_enrichment_sys_ids
    from shared.joins_lab import Candidate

    all_filtered = [
        Candidate(sys_id=f"99000{i}", page=1, via_text=True) for i in range(30)
    ]
    sys_ids = _get_enrichment_sys_ids(all_filtered)
    assert len(sys_ids) == 30, (
        "Enrichment must cover all 30 filtered candidates, not just the current page"
    )
    assert all(f"99000{i}" in sys_ids for i in range(30))


@pytest.mark.xfail(
    reason="Phase 119 Wave 2 — enrichment returns {} on missing db",
    strict=False,
)
def test_enrichment_returns_empty_dict_when_service_unavailable():
    """CND-08: enrichment degrades gracefully to {} when the FJMS service is unavailable.

    This is the graceful-degradation case: fjms_enrichment.db absent or unavailable.
    The surface must still render; material/dims columns show '—'.
    """
    from unittest.mock import patch, MagicMock
    import asyncio
    from web.pages.joins_lab import _enrich_candidates

    mock_service = MagicMock()
    mock_service.is_available.return_value = False

    with patch("shared.fjms_service.get_fjms_service", return_value=mock_service):
        result = asyncio.get_event_loop().run_until_complete(
            _enrich_candidates(["990001", "990002"])
        )
    assert result == {}, (
        "Enrichment must return {} when the FJMS service is unavailable"
    )


def test_enrichment_call_site_is_covered_by_off_loop_guard():
    """CND-08: the joins_lab.py enrichment call site must be covered by the off-loop guard.

    This test verifies that test_enrichment_batch_not_on_event_loop (in
    test_joins_lab_off_loop.py) will fire once Wave 2 adds the call site — i.e. the
    guard is live (not skipped) when get_measurement_summaries_batch is present.

    Here we check structurally that the off-loop guard test exists and would not skip
    if the call site were present.
    """
    from pathlib import Path
    import ast

    guard_path = Path(__file__).parent / "test_joins_lab_off_loop.py"
    assert guard_path.exists(), "Off-loop guard test file must exist"
    source = guard_path.read_text(encoding="utf-8")
    assert "get_measurement_summaries_batch" in source, (
        "Off-loop guard must reference get_measurement_summaries_batch"
    )
    assert "_find_blocking_call_violations" in source, (
        "Off-loop guard must have the generic _find_blocking_call_violations detector"
    )


# ---------------------------------------------------------------------------
# Plan 119-07: A3 anchor in enrichment batch + A4 VS-only candidate metadata
# ---------------------------------------------------------------------------


def test_enrichment_batch_includes_anchor_sys_id():
    """A3: the enrichment batch must include the anchor sys_id so is_size_mismatch
    has the anchor's width/height.

    _get_enrichment_sys_ids extracts candidate sys_ids; _do_enrich_and_update
    adds the anchor sys_id to that list (deduped) before calling _enrich_candidates.

    Here we test the structural source assertion that the enrichment batch path
    includes the anchor sys_id.
    """
    from pathlib import Path

    joins_lab_path = Path(__file__).parent.parent / "web" / "pages" / "joins_lab.py"
    assert joins_lab_path.exists(), "web/pages/joins_lab.py must exist"
    source = joins_lab_path.read_text(encoding="utf-8")

    # _do_enrich_and_update must reference anchor sys_id from _anchor_state
    assert "_anchor_state.get('sys_id')" in source or '_anchor_state["sys_id"]' in source, (
        "_do_enrich_and_update must read the anchor sys_id from _anchor_state (A3)"
    )
    # The enrichment section must explicitly include the anchor
    assert "anchor_sid" in source, (
        "A variable named 'anchor_sid' must appear in the enrichment batch logic (A3)"
    )


def test_enrichment_batch_dedup_with_anchor():
    """A3: _get_enrichment_sys_ids + anchor dedup produces a list containing the anchor."""
    from web.pages.joins_lab import _get_enrichment_sys_ids
    from shared.joins_lab import Candidate

    candidates = [
        Candidate(sys_id="C1", page=1, via_text=True),
        Candidate(sys_id="C2", page=1, via_text=True),
        Candidate(sys_id="C1", page=2, via_text=True),  # duplicate sys_id, different page
    ]
    anchor_sid = "ANCHOR_SID"

    # Simulate what _do_enrich_and_update does: get candidate sys_ids + add anchor
    sys_ids = list(_get_enrichment_sys_ids(candidates))
    if anchor_sid not in sys_ids:
        sys_ids.append(anchor_sid)

    # Anchor must be present exactly once
    assert sys_ids.count(anchor_sid) == 1, (
        f"Anchor sys_id must appear exactly once in the enrichment batch, "
        f"got count={sys_ids.count(anchor_sid)!r}"
    )
    # All candidate sys_ids must also be present (deduped)
    assert "C1" in sys_ids and "C2" in sys_ids, (
        "All candidate sys_ids must be in the batch"
    )
    # C1 appears only once (deduped by _get_enrichment_sys_ids)
    assert sys_ids.count("C1") == 1, "C1 must be deduped in the enrichment batch"


def test_vs_candidate_metadata_populated_when_supplied():
    """A4: VS Candidate built with metadata resolvers carries shelfmark/title/library_code.

    This tests the _map_vs_suggestions_to_candidates base output + the run_vs_meta_core
    enrichment model: when metadata is supplied (as would happen after the off-loop
    executor.get_meta_for_id / get_library_for_id calls), the resulting Candidate has
    real shelfmark/title/library_code (not '?'/None/'').

    Uses dataclasses.replace (the actual implementation mechanism) to apply metadata.
    """
    import dataclasses
    from web.pages.joins_lab import _map_vs_suggestions_to_candidates

    raw = [{"alma_id": "990001", "svm_score": 0.9, "rank": 1}]
    vs_cands = _map_vs_suggestions_to_candidates(raw)
    assert len(vs_cands) == 1

    base = vs_cands[0]
    # By default, shelfmark is '?' (Candidate default), page is None (VS-agnostic)
    assert base.page is None, "VS candidates must have page=None (page-agnostic, F-A4-api)"
    assert base.shelfmark == "?", "VS candidates start with default '?' shelfmark"

    # Simulate what run_vs_meta_core would produce
    meta = {'shelfmark': 'T-S 12.100', 'title': 'Some manuscript', 'library_code': 'CUL'}
    enriched = dataclasses.replace(base, **meta)

    assert enriched.shelfmark == 'T-S 12.100', "Enriched shelfmark must match supplied value"
    assert enriched.title == 'Some manuscript', "Enriched title must match supplied value"
    assert enriched.library_code == 'CUL', "Enriched library_code must match supplied value"
    # page stays None — VS suggestions are page-agnostic (F-A4-api)
    assert enriched.page is None, "page must remain None after A4 metadata enrichment"


def test_off_loop_guard_names_meta_for_id_and_library_for_id():
    """A4 F-A4-guard: the NEW off-loop guard for VS metadata must NAME get_meta_for_id
    and get_library_for_id.

    This is distinct from the existing guards for get_suggestions and
    get_measurement_summaries_batch. The dedicated test ensures that the guard
    names the actual blocking calls (not just that run_vs_core/run_enrich_core stay green).
    """
    from pathlib import Path

    guard_path = Path(__file__).parent / "test_joins_lab_off_loop.py"
    assert guard_path.exists(), "Off-loop guard test file must exist"
    source = guard_path.read_text(encoding="utf-8")
    assert "get_meta_for_id" in source, (
        "The off-loop guard must reference get_meta_for_id (A4 F-A4-guard)"
    )
    assert "get_library_for_id" in source, (
        "The off-loop guard must reference get_library_for_id (A4 F-A4-guard)"
    )
