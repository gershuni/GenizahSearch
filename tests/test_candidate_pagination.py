# -*- coding: utf-8 -*-
"""RED test scaffold for CND-07: Candidate pagination (24/page, filter-before-paginate).

Requirement: CND-07
Wave that turns this green: Wave 1 (Plan 119-02)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (pagination
helpers and the paginate() function) are not yet implemented.  They form the failing
seams that Wave 1 will make green.

Design intent (per CONTEXT.md D-08 / RESEARCH.md Pattern 5):
  - ~24 candidates per page (replaces Phase-117's _MAX_RENDERED_CANDIDATES=200 silent cap)
  - Filters apply BEFORE pagination (filter the full set, then slice)
  - Triage persists across page changes (triage dict is not reset by pagination)
  - Enrichment batch covers the FULL filtered set, not just the current page
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest


# Minimal Candidate stand-in (mirrors shared.joins_lab.Candidate fields needed here)
@dataclass(frozen=True)
class _Cand:
    sys_id: str
    page: Optional[int]
    via_text: bool = False


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — pagination helper not yet implemented",
    strict=False,
)
def test_paginate_first_page_slice():
    """CND-07: page 0, page_size=24 returns the first 24 candidates."""
    from web.components.candidate_grid import paginate
    all_cands = [_Cand(sys_id=str(i), page=1) for i in range(50)]
    page_slice, current_page, total_pages = paginate(all_cands, page=0, page_size=24)
    assert len(page_slice) == 24
    assert current_page == 0
    assert total_pages == 3  # ceil(50/24) = 3


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — pagination helper not yet implemented",
    strict=False,
)
def test_paginate_last_page_has_remainder():
    """CND-07: the last page has the remainder (50 % 24 = 2 candidates)."""
    from web.components.candidate_grid import paginate
    all_cands = [_Cand(sys_id=str(i), page=1) for i in range(50)]
    page_slice, current_page, total_pages = paginate(all_cands, page=2, page_size=24)
    assert len(page_slice) == 2   # 50 - 48 = 2
    assert current_page == 2
    assert total_pages == 3


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — pagination helper not yet implemented",
    strict=False,
)
def test_paginate_empty_list_yields_one_page():
    """CND-07: empty candidate list → total_pages=1, page_slice=[]."""
    from web.components.candidate_grid import paginate
    page_slice, current_page, total_pages = paginate([], page=0, page_size=24)
    assert page_slice == []
    assert total_pages == 1


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — filter-before-paginate contract not yet enforced",
    strict=False,
)
def test_filter_applies_before_pagination():
    """CND-07: filters are applied to the full candidate set BEFORE pagination.

    A filter that keeps only via_text=True candidates applied to 30 candidates
    (15 with via_text=True, 15 without) should yield 15 filtered candidates; the
    paginated slice of page 0 at page_size=10 returns the first 10 of those 15.
    """
    from web.components.candidate_grid import paginate, compute_filtered
    all_cands = (
        [_Cand(sys_id=str(i), page=1, via_text=True) for i in range(15)] +
        [_Cand(sys_id=str(i + 15), page=1, via_text=False) for i in range(15)]
    )
    # Filter: only via_text candidates (using text_q to approximate)
    filtered = [c for c in all_cands if c.via_text]
    page_slice, current_page, total_pages = paginate(filtered, page=0, page_size=10)
    assert len(page_slice) == 10
    assert total_pages == 2  # ceil(15/10)
    assert all(c.via_text for c in page_slice)


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — triage persists across page changes",
    strict=False,
)
def test_triage_survives_page_change():
    """CND-07: triage dict is NOT reset when the page changes.

    Setting a verdict on page 0 then navigating to page 1 and back must preserve it.
    """
    from web.components.candidate_grid import make_triage_state, paginate
    all_cands = [_Cand(sys_id=str(i), page=1) for i in range(50)]
    triage = make_triage_state()

    # Set verdict on page 0
    triage.set("0", "yes")

    # Navigate to page 1 (simulate — pagination does not reset triage)
    page_slice, _, _ = paginate(all_cands, page=1, page_size=24)
    assert triage.get("0") == "yes", "Triage must persist when navigating to page 1"

    # Navigate back to page 0
    page_slice, _, _ = paginate(all_cands, page=0, page_size=24)
    assert triage.get("0") == "yes", "Triage must persist when navigating back to page 0"
