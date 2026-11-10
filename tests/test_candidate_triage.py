# -*- coding: utf-8 -*-
"""RED test scaffold for CND-04: Candidate triage (Yes/Maybe/No keyed by sys_id).

Requirement: CND-04
Wave that turns this green: Wave 1 (Plan 119-02)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (triage state
management helpers) are not yet implemented.  They form the failing seams that Wave 1
will make green.

Design intent (per CONTEXT.md D-11 / RESEARCH.md Pattern 2):
  - Triage dict: dict[str, Literal['yes','maybe','no']] keyed by sys_id
  - In-memory page state this phase (no safe_storage writes — Phase 120 adds persistence)
  - Resets on re-anchor / re-search
  - Consistent across grid, table, Compare — single source of truth
  - Bulk triage supported (D-12)
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — triage state management not yet implemented",
    strict=False,
)
def test_triage_set_and_get():
    """CND-04: setting triage for a sys_id stores the verdict and is retrievable."""
    from web.components.candidate_grid import make_triage_state
    triage = make_triage_state()
    triage.set("990001", "yes")
    assert triage.get("990001") == "yes"
    triage.set("990001", "no")
    assert triage.get("990001") == "no"


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — triage reset on re-anchor not yet implemented",
    strict=False,
)
def test_triage_resets_on_reanchor():
    """CND-04: triage must clear entirely on re-anchor / new search (D-11)."""
    from web.components.candidate_grid import make_triage_state
    triage = make_triage_state()
    triage.set("990001", "yes")
    triage.set("990002", "maybe")
    assert len(triage) == 2
    triage.reset()
    assert len(triage) == 0
    assert triage.get("990001") is None


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — bulk triage not yet implemented",
    strict=False,
)
def test_bulk_triage():
    """CND-04 + D-12: setting triage for multiple sys_ids in one call (bulk triage)."""
    from web.components.candidate_grid import make_triage_state
    triage = make_triage_state()
    triage.set_bulk(["990001", "990002", "990003"], "maybe")
    assert triage.get("990001") == "maybe"
    assert triage.get("990002") == "maybe"
    assert triage.get("990003") == "maybe"


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — triage values restricted to yes/maybe/no",
    strict=False,
)
def test_triage_values_are_yes_maybe_no():
    """CND-04: only 'yes', 'maybe', 'no' are valid triage values."""
    from web.components.candidate_grid import make_triage_state
    triage = make_triage_state()
    for valid in ("yes", "maybe", "no"):
        triage.set("990001", valid)
        assert triage.get("990001") == valid
    # Setting an invalid value should raise or be refused
    with pytest.raises(Exception):
        triage.set("990001", "invalid")
