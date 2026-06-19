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
