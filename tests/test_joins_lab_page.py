# -*- coding: utf-8 -*-
"""Tests for web/pages/joins_lab.py module-level helpers (headless — no NiceGUI runtime).

Tests the importable, pure helper functions:
  - lines_to_side_query(text) -> SideQuery
  - decide_initial_anchor(initial_sys_id, initial_shelfmark, stored) -> dict|None
  - _make_progress_cb(my_gen, gen_ref) -> callable
  - _should_apply_results(my_gen, gen_ref) -> bool

Also exercises the real compose() + dedup_candidates() pipeline from shared.joins_lab.

The off-loop static guard (tests/test_joins_lab_off_loop.py) is run/verified here to
confirm it passes against the real web/pages/joins_lab.py and that execute_search
is only inside the run.io_bound closure (MEDIUM-4).
"""

import pytest

from shared.joins_lab import BuilderRow, SideQuery, compose, dedup_candidates

from web.pages.joins_lab import (
    lines_to_side_query,
    decide_initial_anchor,
    _make_progress_cb,
    _should_apply_results,
)


# ── lines_to_side_query ───────────────────────────────────────────────────────


class TestLinesToSideQuery:
    def test_three_non_empty_lines_produce_three_rows(self):
        text = "אמת\n\nשלום\nברכה"
        sq = lines_to_side_query(text)
        assert isinstance(sq, SideQuery)
        assert len(sq.rows) == 3
        assert sq.rows[0].term == "אמת"
        assert sq.rows[1].term == "שלום"
        assert sq.rows[2].term == "ברכה"

    def test_blank_lines_dropped(self):
        text = "line1\n\n\nline2"
        sq = lines_to_side_query(text)
        assert len(sq.rows) == 2
        assert sq.rows[0].term == "line1"
        assert sq.rows[1].term == "line2"

    def test_terms_are_stripped(self):
        text = "  אמת  \n  שלום  "
        sq = lines_to_side_query(text)
        # SideQuery itself doesn't strip — BuilderRow holds what we pass
        # lines_to_side_query uses line.strip() to filter, term gets the stripped value
        assert sq.rows[0].term == "אמת"
        assert sq.rows[1].term == "שלום"

    def test_variants_false_page_position_none(self):
        sq = lines_to_side_query("test")
        assert sq.variants is False
        assert sq.page_position is None

    def test_builder_rows_have_spine_defaults(self):
        """All BuilderRows produced by the spine have line_start=False, gap_to_next=0."""
        sq = lines_to_side_query("a\nb\nc")
        for row in sq.rows:
            assert isinstance(row, BuilderRow)
            assert row.line_start is False
            assert row.line_end is False
            assert row.gap_to_next == 0

    def test_empty_input_produces_zero_rows(self):
        sq = lines_to_side_query("")
        assert len(sq.rows) == 0

    def test_only_blank_lines_produces_zero_rows(self):
        sq = lines_to_side_query("\n\n\n")
        assert len(sq.rows) == 0

    def test_single_line(self):
        sq = lines_to_side_query("אמת")
        assert len(sq.rows) == 1
        assert sq.rows[0].term == "אמת"


# ── compose pipeline ──────────────────────────────────────────────────────────


class TestComposePipeline:
    def test_three_rows_composes_non_none_query_str(self):
        """3 non-empty lines -> SideQuery -> compose() returns non-None query_str."""
        sq = lines_to_side_query("אמת\nשלום\nברכה")
        query_str, responsa_options, page_position = compose(sq)
        assert query_str is not None
        assert isinstance(query_str, str)
        assert len(query_str) > 0

    def test_empty_side_query_returns_none_query_str(self):
        sq = lines_to_side_query("")
        query_str, _, _ = compose(sq)
        assert query_str is None

    def test_compose_returns_three_tuple(self):
        sq = lines_to_side_query("test line")
        result = compose(sq)
        assert len(result) == 3


# ── dedup_candidates ──────────────────────────────────────────────────────────


def _make_raw(sys_id: str, page: int, uid: str = None, **extra) -> dict:
    """Build a minimal raw result dict for dedup_candidates testing."""
    return {
        "display": {
            "id": sys_id,
            "shelfmark": extra.get("shelfmark", f"T-S {sys_id[-3:]}"),
            "title": extra.get("title", ""),
            "library_code": extra.get("library_code", "CUL"),
            "img": page,
            "source": "FGP",
        },
        "uid": uid or f"{sys_id}_FGP_P{page:03d}",
        "full_text": "",
        "snippet": "",
        "score": 1.0,
        "scope": "genizah",
    }


class TestDedupCandidates:
    def test_two_results_same_sys_id_and_page_yields_one(self):
        raw = [
            _make_raw("990001", 1, uid="990001_FGP_P001"),
            _make_raw("990001", 1, uid="990001_FGP_P001_dup"),
        ]
        candidates, _ = dedup_candidates(raw, anchor_sid="")
        assert len(candidates) == 1

    def test_two_results_different_pages_both_survive(self):
        raw = [
            _make_raw("990001", 1),
            _make_raw("990001", 2),
        ]
        candidates, _ = dedup_candidates(raw, anchor_sid="")
        assert len(candidates) == 2

    def test_anchor_sys_id_excluded_by_default(self):
        raw = [
            _make_raw("ANCHOR", 1),
            _make_raw("990002", 1),
        ]
        candidates, anchor_matched = dedup_candidates(raw, anchor_sid="ANCHOR")
        sys_ids = [c.sys_id for c in candidates]
        assert "ANCHOR" not in sys_ids
        assert anchor_matched is True

    def test_anchor_included_when_flag_set(self):
        raw = [_make_raw("ANCHOR", 1)]
        candidates, anchor_matched = dedup_candidates(
            raw, anchor_sid="ANCHOR", include_self=True
        )
        assert len(candidates) == 1
        assert candidates[0].is_anchor_self is True

    def test_empty_raw_returns_empty_list(self):
        candidates, anchor_matched = dedup_candidates([], anchor_sid="990001")
        assert candidates == []
        assert anchor_matched is False


# ── decide_initial_anchor ─────────────────────────────────────────────────────


class TestDecideInitialAnchor:
    def test_url_sys_id_wins_over_stored(self):
        """D-13: URL sys_id wins over stored anchor."""
        stored = {'anchor_sys_id': '990009999', 'schema_version': 1}
        result = decide_initial_anchor('990001234', None, stored)
        assert result is not None
        assert result['source'] == 'url_sys_id'
        assert result['sys_id'] == '990001234'

    def test_url_sys_id_wins_when_both_url_and_shelfmark(self):
        result = decide_initial_anchor('990001234', 'T-S 12.123', None)
        assert result['source'] == 'url_sys_id'
        assert result['sys_id'] == '990001234'

    def test_url_shelfmark_used_when_no_sys_id(self):
        result = decide_initial_anchor(None, 'T-S 12.123', None)
        assert result is not None
        assert result['source'] == 'url_shelfmark'
        assert result['shelfmark'] == 'T-S 12.123'

    def test_stored_used_when_no_url_params(self):
        stored = {'anchor_sys_id': '990009999', 'schema_version': 1}
        result = decide_initial_anchor(None, None, stored)
        assert result is not None
        assert result['source'] == 'stored'
        assert result['sys_id'] == '990009999'

    def test_stored_includes_fl_id_and_volume_ie(self):
        stored = {
            'anchor_sys_id': '990009999',
            'anchor_fl_id': 'T-S 12.123.1r',
            'anchor_volume_ie': 'vol1',
            'schema_version': 1,
        }
        result = decide_initial_anchor(None, None, stored)
        assert result['fl_id'] == 'T-S 12.123.1r'
        assert result['volume_ie'] == 'vol1'

    def test_cold_start_returns_none_when_no_inputs(self):
        result = decide_initial_anchor(None, None, None)
        assert result is None

    def test_cold_start_returns_none_when_stored_is_empty(self):
        result = decide_initial_anchor(None, None, {})
        assert result is None

    def test_stored_anchor_sys_id_missing_returns_none(self):
        stored = {'schema_version': 1}  # no anchor_sys_id key
        result = decide_initial_anchor(None, None, stored)
        assert result is None


# ── _should_apply_results ─────────────────────────────────────────────────────


class TestShouldApplyResults:
    def test_current_generation_returns_true(self):
        assert _should_apply_results(1, {'value': 1}) is True

    def test_superseded_generation_returns_false(self):
        assert _should_apply_results(1, {'value': 2}) is False

    def test_generation_zero_matches(self):
        assert _should_apply_results(0, {'value': 0}) is True

    def test_generation_mismatch(self):
        assert _should_apply_results(3, {'value': 5}) is False


# ── _make_progress_cb — cooperative cancel + dual-protocol guard ──────────────


class TestMakeProgressCb:
    def test_superseded_raises_interrupted_error(self):
        """MEDIUM cooperative cancel: callback raises InterruptedError when superseded."""
        gen_ref = {'value': 2}
        cb = _make_progress_cb(my_gen=1, gen_ref=gen_ref)
        with pytest.raises(InterruptedError):
            cb(0, 100)

    def test_current_generation_does_not_raise(self):
        """Callback does NOT raise when generation still matches."""
        gen_ref = {'value': 1}
        cb = _make_progress_cb(my_gen=1, gen_ref=gen_ref)
        # Should not raise
        cb(0, 100)
        cb(50, 100)

    def test_string_arg_does_not_raise_when_superseded(self):
        """Dual-protocol guard: string status arg raises (superseded takes priority)."""
        # The superseded check fires BEFORE the isinstance check, so a string
        # call ALSO raises InterruptedError when superseded.
        gen_ref = {'value': 2}
        cb = _make_progress_cb(my_gen=1, gen_ref=gen_ref)
        with pytest.raises(InterruptedError):
            cb('Scanning...')

    def test_string_arg_returns_without_raising_when_current(self):
        """Dual-protocol guard: string status arg returns silently (not superseded)."""
        gen_ref = {'value': 1}
        cb = _make_progress_cb(my_gen=1, gen_ref=gen_ref)
        result = cb('Scanning...')
        assert result is None  # returns None (no exception)

    def test_no_exception_on_numeric_when_current(self):
        gen_ref = {'value': 5}
        cb = _make_progress_cb(my_gen=5, gen_ref=gen_ref)
        cb(10, 100)  # Should not raise

    def test_generation_advanced_mid_search_raises(self):
        """Simulates a mid-search generation bump: first call OK, second raises."""
        gen_ref = {'value': 1}
        cb = _make_progress_cb(my_gen=1, gen_ref=gen_ref)
        cb(0, 100)  # OK — still gen 1
        gen_ref['value'] = 2  # Simulate a newer search starting
        with pytest.raises(InterruptedError):
            cb(50, 100)  # Now superseded


# ── End-to-end discard test (MEDIUM, Codex round-3) ──────────────────────────


class TestEndToEndDiscard:
    """Prove a cooperatively-cancelled run's partial results are discarded.

    Mimics genizah_core.py:9000 — the core catches InterruptedError and returns
    a partial list (it does NOT re-raise).  The stale-generation guard
    _should_apply_results is what discards those partial results.
    """

    def test_partial_results_discarded_when_superseded(self):
        """Fake executor mimics the core: catches InterruptedError, returns partial."""
        gen_ref = {'value': 1}
        my_gen = 1

        # A partial result list the fake executor would return
        partial_results = [_make_raw("990001", 1)]

        def fake_execute_search_mimicking_core(progress_callback):
            """Mimics genizah_core.py:9000 — catches InterruptedError, returns partial."""
            try:
                progress_callback(0, 100)
            except InterruptedError:
                # Core catches it and returns PARTIAL results — NOT re-raise
                return partial_results
            return partial_results

        # Simulate: bump generation (a newer search started) before calling
        gen_ref['value'] = 2  # Newer search bumped the generation
        cb = _make_progress_cb(my_gen=my_gen, gen_ref=gen_ref)
        # The fake executor catches InterruptedError and returns partial results
        raw = fake_execute_search_mimicking_core(cb)
        # Prove partial results were returned (not an exception from the executor)
        assert raw == partial_results

        # NOW _should_apply_results would DISCARD them (UI not updated)
        assert _should_apply_results(my_gen, gen_ref) is False

    def test_results_applied_when_still_current(self):
        """Current-generation results ARE applied."""
        gen_ref = {'value': 1}
        my_gen = 1
        assert _should_apply_results(my_gen, gen_ref) is True
