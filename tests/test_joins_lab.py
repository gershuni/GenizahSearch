# -*- coding: utf-8 -*-
"""Tests for shared/joins_lab.py — Joins Lab domain model, normalizer, compose(), and static guards."""

import ast
import importlib
import pathlib

import pytest

from shared.joins_lab import (
    BuilderRow,
    SideQuery,
    Candidate,
    MergeResult,
    SearchExecutor,
    normalize_candidate,
    page_of,
    compose,
    resolve_other_side_pages,
    cross_side_membership,
    apply_cross_side,
    dedup_candidates,
    merge_candidates,
)


# ── Module-level helpers ──────────────────────────────────────────────────────


def _make_result(sys_id: str, page: int, **extra) -> dict:
    """Build a realistic result dict matching the verified engine result-dict shape."""
    d = {
        "display": {
            "id": sys_id,
            "shelfmark": extra.pop("shelfmark", f"T-S 12.{sys_id[-3:]}"),
            "title": extra.pop("title", ""),
            "library_code": extra.pop("library_code", "CUL"),
            "img": page,
            "source": extra.pop("source", "FGP"),
        },
        "uid": extra.pop("uid", f"{sys_id}_FGP_P{page:03d}"),
        "full_text": extra.pop("full_text", ""),
        "snippet": extra.pop("snippet", ""),
        "highlight_pattern": extra.pop("highlight_pattern", None),
        "score": extra.pop("score", 1.0),
        "scope": extra.pop("scope", "genizah"),
    }
    d.update(extra)
    return d


class FakeSearchExecutor:
    """Test double for SearchExecutor Protocol — canned results, call recording."""

    def __init__(self, results=None, browse_pages=None, meta=None, library=None):
        self._results = results or []
        self._browse_pages = browse_pages or {}  # (sys_id, p_num) -> dict
        self._meta = meta or {}                  # sys_id -> (shelfmark, title)
        self._library = library or {}            # sys_id -> library_code
        self.calls = []                          # list for assertions

    def execute_search(self, query_str, mode, gap, **kwargs) -> list:
        self.calls.append(("execute_search", query_str, kwargs))
        return self._results

    def get_browse_page(self, sys_id, p_num=None, **kwargs):
        self.calls.append(("get_browse_page", sys_id, p_num))
        return self._browse_pages.get((sys_id, p_num))

    def get_meta_for_id(self, sys_id) -> tuple:
        return self._meta.get(sys_id, ("Unknown", ""))

    def get_library_for_id(self, sys_id) -> str:
        return self._library.get(sys_id, "")


# ── Task 1 tests ──────────────────────────────────────────────────────────────


class TestPageOf:
    def test_display_img_path(self):
        assert page_of({"display": {"img": 5}}) == 5

    def test_uid_p_fallback(self):
        assert page_of({"uid": "990001_FGP_P012"}) == 12

    def test_vs_uid_returns_none(self):
        assert page_of({"uid": "990001|vs"}) is None

    def test_str_img_coerced(self):
        assert page_of({"display": {"img": "7"}}) == 7


class TestNormalize:
    def test_flat_fields(self):
        res = {
            "display": {
                "id": "990001",
                "shelfmark": "T-S 12.100",
                "title": "כותרת",
                "library_code": "CUL",
                "img": 3,
            },
            "uid": "990001_FGP_P003",
            "full_text": "...",
            "highlight_pattern": "שלום",
            "score": 1.2,
        }
        c = normalize_candidate(res)
        assert c.sys_id == "990001"
        assert c.page == 3
        assert c.shelfmark == "T-S 12.100"
        assert c.library_code == "CUL"
        assert c.key == ("990001", 3)

    def test_vs_only_key_none(self):
        res = {"display": {"id": "990002"}, "uid": "990002|vs"}
        c = normalize_candidate(res)
        assert c.key == ("990002", None)

    def test_missing_score_is_none(self):
        """Line-break results carry no 'score' key — normalizer must return None."""
        res = {
            "display": {"id": "990003", "img": 4},
            "uid": "990003_FGP_P004",
            "full_text": "...",
            "highlight_pattern": "שלום",
            "scope": "genizah",
        }
        c = normalize_candidate(res)
        assert c.score is None

    def test_provenance_flags_carried(self):
        res = {
            "display": {"id": "990004", "img": 1},
            "uid": "990004_FGP_P001",
            "_via_vs": True,
            "vs_rank": 4,
        }
        c = normalize_candidate(res)
        assert c.via_vs is True
        assert c.vs_rank == 4


class TestDataclasses:
    def test_candidate_key_property(self):
        c = Candidate(sys_id="990001", page=3)
        assert c.key == ("990001", 3)

    def test_merge_result_construction(self):
        c = Candidate(sys_id="990002", page=1)
        mr = MergeResult(candidates=(c,), note="test")
        assert len(mr.candidates) == 1
        assert mr.note == "test"


class TestProtocol:
    def test_fake_is_searchexecutor(self):
        assert isinstance(FakeSearchExecutor(), SearchExecutor)


# ── Task 2 tests ──────────────────────────────────────────────────────────────


class TestCompose:
    def test_single_row_no_anchor(self):
        sq = SideQuery((BuilderRow("שלום"),), variants=False)
        qs, ro, pp = compose(sq)
        assert qs == "שלום"
        assert ro["responsa_mode"] is True
        assert ro["variants"] is False
        assert ro["variant_mode"] == "exact"
        assert pp is None

    def test_multiline_round_trip(self):
        from genizah_core import _parse_line_break_query
        rows = (
            BuilderRow("שהדותא ממשלה", line_start=True, gap_to_next=1),
            BuilderRow("ועתה", line_end=True),
        )
        sq = SideQuery(rows)
        qs, ro, pp = compose(sq)
        assert qs is not None
        line_groups, line_gaps = _parse_line_break_query(qs)
        assert line_groups is not None
        assert line_groups[0].line_start is True
        assert line_groups[1].line_end is True
        assert line_gaps[0] == 1

    def test_line_start_leading_pipe(self):
        sq = SideQuery((BuilderRow("שהדותא", line_start=True),))
        qs, ro, pp = compose(sq)
        assert "|שהדותא" in qs

    def test_line_end_trailing_pipe(self):
        sq = SideQuery((BuilderRow("ממשלה", line_end=True),))
        qs, ro, pp = compose(sq)
        assert "ממשלה|" in qs

    def test_variants_toggle(self):
        sq = SideQuery((BuilderRow("שלום"),), variants=True)
        qs, ro, pp = compose(sq)
        assert ro["variants"] is True
        assert ro["variant_mode"] == "variants"

    def test_page_position_start(self):
        sq = SideQuery((BuilderRow("שלום"),), page_position="start")
        qs, ro, pp = compose(sq)
        assert pp == "start"

    def test_page_position_end(self):
        sq = SideQuery((BuilderRow("ועתה"),), page_position="end")
        qs, ro, pp = compose(sq)
        assert pp == "end"

    def test_page_position_independent_of_line_start(self):
        sq = SideQuery(
            (BuilderRow("שהדותא", line_start=True),),
            page_position="start",
        )
        qs, ro, pp = compose(sq)
        assert "|שהדותא" in qs
        assert pp == "start"

    def test_page_position_start_empty_first_row_raises(self):
        sq = SideQuery((BuilderRow("   "), BuilderRow("שלום")), page_position="start")
        with pytest.raises(ValueError):
            compose(sq)

    def test_page_position_end_empty_last_row_raises(self):
        sq = SideQuery((BuilderRow("שלום"), BuilderRow("")), page_position="end")
        with pytest.raises(ValueError):
            compose(sq)

    def test_page_position_all_empty_raises(self):
        sq = SideQuery((BuilderRow("  "),), page_position="start")
        with pytest.raises(ValueError):
            compose(sq)

    def test_all_empty_returns_none(self):
        sq = SideQuery((BuilderRow("   "),))
        qs, ro, pp = compose(sq)
        assert qs is None
        assert ro is None
        assert pp is None


# ── Task 3 tests ──────────────────────────────────────────────────────────────


class TestStaticImport:
    def test_no_pyqt_import(self):
        """shared/joins_lab.py must not import any Qt binding."""
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = (
                    [a.name for a in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module or ""]
                )
                for name in names:
                    assert not (name or "").startswith(
                        ("PyQt6", "PyQt5", "PySide6")
                    ), f"Qt import found: {name}"

    def test_no_pyside_or_qt_substring(self):
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        assert "PyQt" not in src
        assert "PySide" not in src

    def test_no_fist_data_direct(self):
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        assert "fist_data" not in src

    def test_no_sqlite3_connect(self):
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        assert "sqlite3.connect" not in src

    def test_importable_without_engine_init(self):
        mod = importlib.import_module("shared.joins_lab")
        assert mod is not None


# ── Plan 02 Task 1 tests ──────────────────────────────────────────────────────


class TestResolveOtherSide:
    def test_first_page(self):
        assert resolve_other_side_pages(1, total_pages=10) == frozenset({2})

    def test_last_page(self):
        assert resolve_other_side_pages(10, total_pages=10) == frozenset({9})

    def test_middle_page(self):
        assert resolve_other_side_pages(5, total_pages=10) == frozenset({4, 6})

    def test_single_page_doc(self):
        assert resolve_other_side_pages(1, total_pages=1) == frozenset()

    def test_total_unknown(self):
        # total_pages=None: no upper clamp; lower clamp still drops <1
        result = resolve_other_side_pages(5, total_pages=None)
        assert result == frozenset({4, 6})


class TestCrossSide:
    # ── cross_side_membership (pure set logic) ────────────────────────────────

    def test_and_narrows(self):
        # base: {('A',3), ('A',7), ('B',2)}, b_set: {('A',2), ('B',9)}
        # AND keeps only base entries with a neighbor in b_set:
        #   ('A',3): (A,2) in b_set → kept
        #   ('A',7): neither (A,6) nor (A,8) in b_set → dropped
        #   ('B',2): neither (B,1) nor (B,3) in b_set → dropped
        base_keys = {("A", 3), ("A", 7), ("B", 2)}
        b_set = {("A", 2), ("B", 9)}
        result = cross_side_membership(base_keys, b_set, "AND", totals={})
        assert result == {("A", 3)}

    def test_or_widens(self):
        # OR returns the base set UNION neighbor pages of b_set entries
        # b_set has ("A", 2): neighbors are page 1 (if not in base) and page 3
        # base has ("A", 3) already, so only ("A", 1) added; page 3 already in base
        base_keys = {("A", 3)}
        b_set = {("A", 2)}
        totals = {"A": 10}
        result = cross_side_membership(base_keys, b_set, "OR", totals=totals)
        # Should contain original base AND neighbor(s) of b_set not already in base
        assert ("A", 3) in result
        assert ("A", 1) in result   # neighbor of (A,2) not in base → added

    def test_or_no_upper_overflow(self):
        # b_set page at last page should not synthesize a page > total_pages
        base_keys = {("A", 5)}
        b_set = {("A", 10)}
        totals = {"A": 10}
        result = cross_side_membership(base_keys, b_set, "OR", totals=totals)
        # (A, 11) would overflow → not added
        assert ("A", 11) not in result
        # (A, 9) is valid neighbor
        assert ("A", 9) in result

    # ── apply_cross_side (I/O via FakeSearchExecutor) ─────────────────────────

    def test_and_filters_base(self):
        # FakeSearchExecutor returns B-side results for ('A', 2)
        # Base has text Candidates for ('A', 3) and ('A', 7)
        # AND: ('A', 3) neighbor ('A', 2) is in b_set → kept
        # AND: ('A', 7) neither ('A', 6) nor ('A', 8) in b_set → dropped
        b_result = _make_result("A", 2)
        executor = FakeSearchExecutor(results=[b_result])
        base = [
            normalize_candidate(_make_result("A", 3)),
            normalize_candidate(_make_result("A", 7)),
        ]
        mr = apply_cross_side(executor, base, "b_query", {}, "AND")
        assert len(mr.candidates) == 1
        assert mr.candidates[0].sys_id == "A"
        assert mr.candidates[0].page == 3
        # Executor recorded an execute_search call with corpus_scope 'genizah'
        assert any(
            call[0] == "execute_search" and call[2].get("corpus_scope") == "genizah"
            for call in executor.calls
        )

    def test_or_synthesizes_neighbor(self):
        # B-side has result at ('B', 3); OR should synthesize neighbor ('B', 2)
        # Base has no 'B' candidates at all
        b_result = _make_result("B", 3)
        executor = FakeSearchExecutor(
            results=[b_result],
            browse_pages={("B", 2): {"text": "שלום עולם", "total_pages": 5}},
            meta={"B": ("T-S 12.001", "כותרת")},
            library={"B": "CUL"},
        )
        base = []
        mr = apply_cross_side(executor, base, "b_q", {}, "OR", anchor_pattern="שלום")
        # A neighbor candidate should have been synthesized for ('B', 2)
        neighbor_cands = [c for c in mr.candidates if c.sys_id == "B" and c.page == 2]
        assert len(neighbor_cands) == 1
        cand = neighbor_cands[0]
        assert cand.via_other_side is True

    def test_total_pages_clamps(self):
        # B-side page at the document's last page (total_pages=3, b_page=3)
        # OR: neighbor (B, 4) should NOT be synthesized (> total_pages)
        b_result = _make_result("B", 3)
        executor = FakeSearchExecutor(
            results=[b_result],
            browse_pages={
                ("B", 1): {"text": "", "total_pages": 3},
                ("B", 2): {"text": "neighbor text", "total_pages": 3},
            },
            meta={"B": ("T-S 12.002", "")},
            library={"B": "CUL"},
        )
        base = []
        mr = apply_cross_side(executor, base, "b_q", {}, "OR")
        pages = {c.page for c in mr.candidates if c.sys_id == "B"}
        assert 4 not in pages   # page 4 > total_pages=3 must not appear


# ── Plan 02 Task 2 tests ──────────────────────────────────────────────────────


class TestDedup:
    def test_dedup_same_page(self):
        """Two raw dicts for the same (sys_id, page) collapse to ONE Candidate."""
        raw = [
            _make_result("990001", 5),
            _make_result("990001", 5, snippet="dup"),
        ]
        out, anchor_matched = dedup_candidates(raw, anchor_sid="ANCHOR")
        assert len(out) == 1
        assert anchor_matched is False

    def test_distinct_pages_kept(self):
        """(sys_id, 5) and (sys_id, 6) are distinct and both survive dedup."""
        raw = [
            _make_result("990001", 5),
            _make_result("990001", 6),
        ]
        out, _ = dedup_candidates(raw, anchor_sid="ANCHOR")
        assert len(out) == 2
        pages = {c.page for c in out}
        assert pages == {5, 6}

    def test_vs_uid_key(self):
        """A VS-only raw dict (uid='{sid}|vs') yields key (sys_id, None);
        two such dicts for the same sid collapse to one Candidate."""
        raw = [
            {"display": {"id": "990002"}, "uid": "990002|vs"},
            {"display": {"id": "990002"}, "uid": "990002|vs", "snippet": "dup"},
        ]
        out, _ = dedup_candidates(raw, anchor_sid="ANCHOR")
        assert len(out) == 1
        assert out[0].key == ("990002", None)

    def test_via_text_marked(self):
        """Every deduped Candidate from dedup_candidates has via_text True."""
        raw = [_make_result("990001", 5)]
        out, _ = dedup_candidates(raw, anchor_sid="ANCHOR")
        assert all(c.via_text is True for c in out)

    def test_anchor_self_excluded_by_default(self):
        """A raw dict whose sys_id == anchor_sid is dropped when include_self=False,
        and the returned anchor_matched flag is True."""
        raw = [_make_result("ANCHOR_SID", 1)]
        out, anchor_matched = dedup_candidates(raw, anchor_sid="ANCHOR_SID")
        assert len(out) == 0
        assert anchor_matched is True

    def test_anchor_self_included_when_flag(self):
        """Same dict with include_self=True is kept and its Candidate.is_anchor_self is True."""
        raw = [_make_result("ANCHOR_SID", 1)]
        out, anchor_matched = dedup_candidates(raw, anchor_sid="ANCHOR_SID", include_self=True)
        assert len(out) == 1
        assert out[0].is_anchor_self is True
        assert anchor_matched is True


class TestMerge:
    def test_text_only_passthrough(self):
        """merge_candidates(text=[3 cands], vs=[]) returns those 3, order preserved."""
        text_cands = [
            Candidate(sys_id="A", page=1, via_text=True),
            Candidate(sys_id="B", page=2, via_text=True),
            Candidate(sys_id="C", page=3, via_text=True),
        ]
        result = merge_candidates(text_cands, [])
        assert len(result) == 3
        assert [c.sys_id for c in result] == ["A", "B", "C"]

    def test_vs_only(self):
        """merge_candidates(text=[], vs=[2 cands]) returns the 2 vs cands."""
        vs_cands = [
            Candidate(sys_id="X", page=None, via_vs=True, vs_rank=1),
            Candidate(sys_id="Y", page=None, via_vs=True, vs_rank=2),
        ]
        result = merge_candidates([], vs_cands)
        assert len(result) == 2

    def test_overlap_annotated(self):
        """A text cand for sid 'X' and a vs cand for sid 'X' (vs_rank 4):
        merged 'X' Candidate has via_text True AND via_vs True AND vs_rank 4."""
        text_cands = [Candidate(sys_id="X", page=5, via_text=True)]
        vs_cands = [Candidate(sys_id="X", page=None, via_vs=True, vs_rank=4)]
        result = merge_candidates(text_cands, vs_cands)
        x_cand = next(c for c in result if c.sys_id == "X" and c.page == 5)
        assert x_cand.via_text is True
        assert x_cand.via_vs is True
        assert x_cand.vs_rank == 4

    def test_ordering(self):
        """Given text cands for X(also-vs rank2), Y(text-only), and a VS-only Z(rank5):
        merged order is [X (both, tier0), Y (text, tier1), Z (vs-only, tier2)];
        within tier-2 sorted by vs_rank."""
        text_cands = [
            Candidate(sys_id="X", page=1, via_text=True),
            Candidate(sys_id="Y", page=2, via_text=True),
        ]
        vs_cands = [
            Candidate(sys_id="X", page=None, via_vs=True, vs_rank=2),
            Candidate(sys_id="Z", page=None, via_vs=True, vs_rank=5),
        ]
        result = merge_candidates(text_cands, vs_cands)
        sys_ids = [c.sys_id for c in result]
        # X (both) must come before Y (text-only) must come before Z (vs-only)
        assert sys_ids.index("X") < sys_ids.index("Y") < sys_ids.index("Z")

    def test_both_tier_sorts_before_text(self):
        """A both-cand always precedes a text-only cand regardless of input order."""
        # Y is text-only, X is both — even if Y is listed first in input
        text_cands = [
            Candidate(sys_id="Y", page=2, via_text=True),
            Candidate(sys_id="X", page=1, via_text=True),
        ]
        vs_cands = [
            Candidate(sys_id="X", page=None, via_vs=True, vs_rank=1),
        ]
        result = merge_candidates(text_cands, vs_cands)
        sys_ids = [c.sys_id for c in result]
        assert sys_ids.index("X") < sys_ids.index("Y")
