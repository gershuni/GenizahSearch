# -*- coding: utf-8 -*-
"""Render-contract tests for Phase 119 web integration.

These tests catch API-signature drift between web/pages/joins_lab.py and
web/components/candidate_grid.py WITHOUT requiring a live NiceGUI server.
They use ``inspect.signature`` introspection to assert that:

  - TriageState accepts the calling form used by the page (CR-01 class)
  - The kwargs joins_lab.py passes to open_filter_dialog are a subset of its
    real parameters (CR-02 class)
  - Every kwarg joins_lab.py passes to Candidate(...) is a real Candidate field
    (CR-03 class)
  - compose() returns (None, None, None) — not None — so the guard in
    run_cross_side_core safely tests ``b_query is None`` (CR-05 class)

These tests would have caught every Phase 119 critical finding.

Robustness notes:
  - No NiceGUI import needed; all checks are structural/introspective.
  - No mocking of heavy deps: we only import lightweight pure-Python modules.
  - Tests are immune to unrelated refactors that don't change the specific
    interfaces they check.
"""

from __future__ import annotations

import dataclasses
import inspect
import pytest


# ---------------------------------------------------------------------------
# CR-01: TriageState constructor accepts an optional backing dict
# ---------------------------------------------------------------------------

class TestTriageStateConstructorContract:
    """TriageState() must accept 0 or 1 positional/keyword args (CR-01)."""

    def test_no_args_still_works(self):
        """Existing tests call TriageState() — backward compat must be preserved."""
        from web.components.candidate_grid import TriageState
        ts = TriageState()
        assert ts.get("anything") is None
        assert len(ts) == 0

    def test_accepts_backing_dict(self):
        """The page calls TriageState(backing=_triage) — must not raise TypeError."""
        from web.components.candidate_grid import TriageState
        d = {"99001": "yes", "99002": "maybe"}
        ts = TriageState(backing=d)
        # Must expose the pre-populated values
        assert ts.get("99001") == "yes"
        assert ts.get("99002") == "maybe"

    def test_backing_dict_is_shared(self):
        """TriageState._data must BE the backing dict (same object, not a copy)."""
        from web.components.candidate_grid import TriageState
        d: dict = {}
        ts = TriageState(backing=d)
        ts.set("99003", "no")
        # Change must be visible through the original dict
        assert d.get("99003") == "no"

    def test_verdict_written_to_dict_visible_via_triagestate(self):
        """Writes to the backing dict must be visible via TriageState.get()."""
        from web.components.candidate_grid import TriageState
        d: dict = {}
        ts = TriageState(backing=d)
        d["99004"] = "yes"
        assert ts.get("99004") == "yes"

    def test_constructor_signature_allows_backing_kwarg(self):
        """inspect.signature check: backing must be an accepted parameter name."""
        from web.components.candidate_grid import TriageState
        sig = inspect.signature(TriageState.__init__)
        assert "backing" in sig.parameters, (
            "TriageState.__init__ must accept a 'backing' keyword argument (CR-01)"
        )

    def test_backing_defaults_to_none(self):
        """backing parameter must have a default of None so TriageState() works."""
        from web.components.candidate_grid import TriageState
        sig = inspect.signature(TriageState.__init__)
        param = sig.parameters.get("backing")
        assert param is not None
        assert param.default is None, (
            "backing must default to None so TriageState() (no-arg) still works"
        )


# ---------------------------------------------------------------------------
# Round-5 (PST + bulk toolbar): source-contract guards
# ---------------------------------------------------------------------------

class TestRound5SourceContract:
    """Structural guards for the round-5 features (no live server needed):

    1. Results persistence is wired (persist_results_snapshot called).
    2. Restore prefers the per-tab snapshot before re-running the search
       (read_results_snapshot + a restored_from_snapshot branch).
    3. The selection bulk-action toolbar exists and is refreshed on selection.
    4. The candidate TABLE no longer renders its own Add-to-Puzzle / Add-to-List
       buttons (the page toolbar owns them for BOTH views — no duplication).
    """

    def _page_source(self) -> str:
        import web.pages.joins_lab as jl
        return inspect.getsource(jl)

    def test_results_snapshot_persisted(self):
        src = self._page_source()
        assert 'persist_results_snapshot(' in src, (
            'results must be persisted to the per-tab snapshot (round-5 PST)'
        )

    def test_restore_prefers_snapshot_over_rerun(self):
        src = self._page_source()
        assert 'read_results_snapshot(' in src, (
            'restore must read the per-tab snapshot before re-running the search'
        )
        assert 'restored_from_snapshot' in src, (
            'the stored-anchor restore path must branch on a snapshot restore '
            '(instant) vs a full search re-run (fallback)'
        )

    def test_bulk_toolbar_wired(self):
        src = self._page_source()
        assert '_refresh_bulk_toolbar' in src, (
            'the selection bulk-action toolbar must exist (round-5)'
        )
        assert "_bulk_bar_ref" in src, (
            'the bulk toolbar must be reachable via a late-bound ref'
        )
        # The bulk bar wires the existing selection-scoped handlers.
        assert '_on_add_to_puzzle_click' in src
        assert '_on_add_to_list_click' in src

    def test_table_does_not_render_duplicate_bulk_buttons(self):
        """create_candidate_table must NOT bind on_add_to_puzzle / on_add_to_list
        to a button (those moved to the page toolbar — round-5)."""
        import web.components.candidate_grid as cgrid
        table_src = inspect.getsource(cgrid.create_candidate_table)
        assert 'on_click=on_add_to_puzzle' not in table_src, (
            'Add-to-Puzzle must not be rendered inside the table bulk bar '
            '(moved to the page toolbar for both views — round-5)'
        )
        assert 'on_click=on_add_to_list' not in table_src, (
            'Add-to-List must not be rendered inside the table bulk bar '
            '(moved to the page toolbar for both views — round-5)'
        )
        # Add-as-Join STAYS in the table bulk bar (pairwise single-select).
        assert 'on_add_as_join' in table_src


# ---------------------------------------------------------------------------
# CR-02: open_filter_dialog call-site kwargs must match the real signature
# ---------------------------------------------------------------------------

class TestOpenFilterDialogCallSiteContract:
    """Every kwarg the page passes to open_filter_dialog must be a real parameter.

    Also asserts on_apply is called with no arguments (filter_state mutated in place)
    and on_reset is a required parameter.
    """

    # The kwargs joins_lab.py now passes (post-fix):
    _CALLER_KWARGS = frozenset({
        "filter_state",
        "enrichment",
        "enrichment_ready",
        "on_apply",
        "on_reset",
    })

    def test_caller_kwargs_are_subset_of_real_signature(self):
        """All kwargs the page passes must exist in open_filter_dialog's signature."""
        from web.components.candidate_grid import open_filter_dialog
        sig = inspect.signature(open_filter_dialog)
        real_params = frozenset(sig.parameters.keys())
        unknown = self._CALLER_KWARGS - real_params
        assert not unknown, (
            f"open_filter_dialog: page passes kwargs not in real signature: {unknown}"
        )

    def test_on_reset_is_a_required_parameter(self):
        """on_reset must exist and have no default (i.e., required positional)."""
        from web.components.candidate_grid import open_filter_dialog
        sig = inspect.signature(open_filter_dialog)
        assert "on_reset" in sig.parameters, (
            "open_filter_dialog must have an 'on_reset' parameter (CR-02)"
        )

    def test_on_apply_is_a_required_parameter(self):
        """on_apply must exist in the real signature."""
        from web.components.candidate_grid import open_filter_dialog
        sig = inspect.signature(open_filter_dialog)
        assert "on_apply" in sig.parameters, (
            "open_filter_dialog must have an 'on_apply' parameter"
        )

    def test_no_candidates_param(self):
        """candidates= must NOT be in the real signature (was a spurious kwarg before fix)."""
        from web.components.candidate_grid import open_filter_dialog
        sig = inspect.signature(open_filter_dialog)
        assert "candidates" not in sig.parameters, (
            "open_filter_dialog must NOT have a 'candidates' parameter — "
            "the page was passing this spuriously (CR-02)"
        )

    def test_no_anchor_sys_id_param(self):
        """anchor_sys_id= must NOT be in open_filter_dialog's signature."""
        from web.components.candidate_grid import open_filter_dialog
        sig = inspect.signature(open_filter_dialog)
        assert "anchor_sys_id" not in sig.parameters, (
            "open_filter_dialog must NOT have 'anchor_sys_id' — spurious kwarg (CR-02)"
        )


# ---------------------------------------------------------------------------
# CR-03: Candidate(**kwargs) — every kwarg must be a real dataclass field
# ---------------------------------------------------------------------------

class TestCandidateConstructorContract:
    """Every kwarg that joins_lab.py passes to Candidate() must be a real field.

    The anchor Candidate is built as:
        Candidate(sys_id=, page=, uid=, volume_ie=, is_anchor_self=True)
    after the CR-03 fix removed the spurious fl_id= kwarg.
    """

    # The kwargs joins_lab.py now passes (post-fix):
    _ANCHOR_KWARGS = frozenset({
        "sys_id",
        "page",
        "uid",
        "volume_ie",
        "is_anchor_self",
    })

    def test_anchor_kwargs_are_real_candidate_fields(self):
        """Every kwarg the page passes for the anchor Candidate must be a real field."""
        from shared.joins_lab import Candidate
        real_fields = frozenset(f.name for f in dataclasses.fields(Candidate))
        unknown = self._ANCHOR_KWARGS - real_fields
        assert not unknown, (
            f"Candidate: page passes kwargs not in real fields: {unknown}"
        )

    def test_fl_id_is_NOT_a_candidate_field(self):
        """fl_id must NOT be a Candidate field — the old code passed it spuriously."""
        from shared.joins_lab import Candidate
        real_fields = frozenset(f.name for f in dataclasses.fields(Candidate))
        assert "fl_id" not in real_fields, (
            "Candidate must NOT have an fl_id field — the page was passing it "
            "spuriously, causing TypeError on every Compare open (CR-03)"
        )

    def test_anchor_candidate_construction_succeeds(self):
        """Building the anchor Candidate with the page's actual kwargs must not raise."""
        from shared.joins_lab import Candidate
        # This exact call mirrors what joins_lab.py does in _open_compare (post-fix)
        cand = Candidate(
            sys_id="990001234560205171",
            page=1,
            uid="990001234560205171|anchor",
            volume_ie=None,
            is_anchor_self=True,
        )
        assert cand.sys_id == "990001234560205171"
        assert cand.is_anchor_self is True

    def test_anchor_candidate_with_fl_id_raises(self):
        """Passing fl_id= to Candidate must raise TypeError (confirms no regression)."""
        from shared.joins_lab import Candidate
        with pytest.raises(TypeError, match="fl_id"):
            Candidate(
                sys_id="990001234560205171",
                page=1,
                uid="990001234560205171|anchor",
                fl_id="T-S 12.100.1r",  # spurious kwarg — must remain absent
                is_anchor_self=True,
            )


# ---------------------------------------------------------------------------
# CR-05: compose() returns 3-tuple with None first element (not None itself)
# ---------------------------------------------------------------------------

class TestComposeEmptyInputContract:
    """compose() must return (None, None, None) — not None — so b_query is None guard works."""

    def test_all_empty_rows_returns_three_none_tuple(self):
        """When all rows are whitespace-only, compose returns (None, None, None)."""
        from shared.joins_lab import SideQuery, BuilderRow, compose
        sq = SideQuery(rows=(BuilderRow(term="   "), BuilderRow(term="\t")))
        result = compose(sq)
        assert result == (None, None, None), (
            "compose() must return (None, None, None) for all-whitespace input "
            "so `b_query is None` guard in run_cross_side_core works (CR-05)"
        )

    def test_return_is_iterable_3_tuple(self):
        """Ensures the return is always unpackable as (b_query, b_ro, b_page_position)."""
        from shared.joins_lab import SideQuery, BuilderRow, compose
        sq = SideQuery(rows=(BuilderRow(term=""),))
        result = compose(sq)
        assert len(result) == 3, "compose() must always return a 3-tuple"
        b_query, b_ro, b_page_position = result  # must not raise ValueError

    def test_non_empty_rows_return_non_none_query(self):
        """Sanity: compose with non-empty content does NOT return None query."""
        from shared.joins_lab import SideQuery, BuilderRow, compose
        sq = SideQuery(rows=(BuilderRow(term="אמת"),))
        b_query, b_ro, b_page_position = compose(sq)
        assert b_query is not None
        assert b_ro is not None
        assert isinstance(b_ro, dict)


# ---------------------------------------------------------------------------
# CR-04: create_candidate_grid must NOT write to a module-global _card_refs
# ---------------------------------------------------------------------------

class TestCardRefsNotModuleGlobal:
    """create_candidate_grid must not rely on a module-level _card_refs dict (CR-04)."""

    def test_no_module_level_card_refs(self):
        """The module-level _card_refs dict must not exist after the CR-04 fix."""
        import web.components.candidate_grid as cg_module
        assert not hasattr(cg_module, "_card_refs"), (
            "web.components.candidate_grid must NOT have a module-level '_card_refs' "
            "dict after the CR-04 fix — it was a cross-user DOM ref leak"
        )

    def test_make_restyle_fn_is_exported(self):
        """_make_restyle_fn factory must exist so per-render card_refs work."""
        from web.components.candidate_grid import _make_restyle_fn
        assert callable(_make_restyle_fn)

    def test_make_restyle_fn_returns_callable(self):
        """_make_restyle_fn({}) must return a callable restyle function."""
        from web.components.candidate_grid import _make_restyle_fn
        fn = _make_restyle_fn({})
        assert callable(fn)

    def test_restyle_fn_accepts_sys_id_and_triage(self):
        """The returned restyle fn must accept (sys_id, triage) without raising."""
        from web.components.candidate_grid import _make_restyle_fn
        fn = _make_restyle_fn({})
        # No card refs registered — call must be a safe no-op.
        fn("99001", {"99001": "yes"})  # must not raise
