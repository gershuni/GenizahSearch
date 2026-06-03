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
