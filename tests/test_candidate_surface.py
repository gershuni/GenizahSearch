# -*- coding: utf-8 -*-
"""RED test scaffold for CND-03: Candidate table surface (sortable columns, multi-select).

Requirement: CND-03
Wave that turns this green: Wave 1 (Plan 119-02)
Phase: 119-candidates-compare-visual-similarity

These tests are marked xfail/skip because the target production symbols (the table-view
rendering logic and CandidateSurface factory) are not yet implemented.  They form the
failing seams that Wave 1 will make green.

Design intent (per CONTEXT.md D-10 / RESEARCH.md table pattern):
  - 8-column table shape: Checkbox | Shelfmark | Score | Snippet | Material | Dimensions | Page | Triage
  - Web ADDS sortable columns + multi-select (desktop has setSortingEnabled(False) — web divergence)
  - Default sort: score descending; VS-rank ascending when 👁 ON
  - Both grid and table share the SAME sys_id-keyed triage + 👁 badge state
"""
from __future__ import annotations

import pytest


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — table surface not yet implemented",
    strict=False,
)
def test_table_has_8_column_shape():
    """CND-03: the table view must expose exactly 8 columns in the correct order.

    Expected columns: Checkbox | Shelfmark | Score | Snippet | Material | Dimensions | Page | Triage
    (parity desktop join_workbench.py:2449-2454; web ADDS sorting + multi-select per D-10)
    """
    from web.components.candidate_grid import get_table_columns
    cols = get_table_columns()
    names = [c["name"] for c in cols]
    assert names == ["select", "shelfmark", "score", "snippet", "material", "dimensions", "page", "triage"]


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — table sortable columns not yet implemented",
    strict=False,
)
def test_table_columns_sortable_except_checkbox_and_triage():
    """CND-03: all columns except Checkbox and Triage must be marked sortable=True.

    Web ADDS sortable columns — divergence from desktop setSortingEnabled(False) (D-10).
    """
    from web.components.candidate_grid import get_table_columns
    cols = get_table_columns()
    col_map = {c["name"]: c for c in cols}
    assert col_map["select"]["sortable"] is False
    assert col_map["triage"]["sortable"] is False
    for col_name in ("shelfmark", "score", "snippet", "material", "dimensions", "page"):
        assert col_map[col_name]["sortable"] is True, (
            f"Column '{col_name}' should be sortable=True"
        )


@pytest.mark.xfail(
    reason="Phase 119 Wave 1 — multi-select on table not yet implemented",
    strict=False,
)
def test_table_supports_multi_select():
    """CND-03: table must support multi-selection (selection='multiple' in NiceGUI/Quasar).

    Multi-select is the substrate for bulk triage (D-12) and Phase 120 bulk actions.
    """
    from web.components.candidate_grid import get_table_config
    config = get_table_config()
    assert config.get("selection") == "multiple", (
        "Table must have selection='multiple' for bulk triage (D-12)"
    )
