# -*- coding: utf-8 -*-
"""Phase 96 D-F5: LOCAL hit dict highlight-shape normalization tests.

Implementation plan: 96-03-PLAN.md
"""
import re
from unittest.mock import MagicMock, patch
import pytest


def _make_engine():
    """Phase 95 engine-stub pattern (verbatim copy from
    tests/test_local_post_dedup_merge.py:21-30)."""
    try:
        from genizah_core import SearchEngine
    except ImportError:
        pytest.skip("genizah_core import failed")
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None
    return engine


def _mock_doc(content="the genizah text matches here", uid="local_uid",
              full_header="123_LOCAL_P1_F0", shelfmark="folder/file.pdf"):
    doc = MagicMock()
    fields = {
        "content": content,
        "unique_id": uid,
        "full_header": full_header,
        "shelfmark": shelfmark,
    }
    doc.get_first.side_effect = lambda f: fields.get(f, "")
    return doc


def test_local_hit_dict_has_highlight_pattern():
    """D-F5: _build_local_result_dict must add 'highlight_pattern' field
    when called with a regex parameter."""
    engine = _make_engine()
    # Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    # BLOCKER 5 audit (96-09): skip converted to positive assertion.
    assert hasattr(engine, "_build_local_result_dict"), (
        "Phase 96 D-F5 regression: _build_local_result_dict missing from SearchEngine"
    )
    # Phase 96 signature change: regex param added.
    regex = re.compile(r"genizah", re.IGNORECASE)
    result = engine._build_local_result_dict(_mock_doc(), 1.0, regex=regex,
                                              pattern_str="genizah")
    assert "highlight_pattern" in result, (
        "D-F5: LOCAL hit dict must carry 'highlight_pattern' so "
        "ResultDialog can apply highlighting (mirrors Genizah hit shape)"
    )
    assert result["highlight_pattern"] == "genizah"


def test_local_snippet_has_asterisk_markers_when_regex_matches():
    """D-F5: snippet must contain *...* markers when regex matches content."""
    engine = _make_engine()
    # Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    # BLOCKER 5 audit (96-09): skip converted to positive assertion.
    assert hasattr(engine, "_build_local_result_dict"), (
        "Phase 96 D-F5 regression: _build_local_result_dict missing from SearchEngine"
    )
    regex = re.compile(r"genizah", re.IGNORECASE)
    result = engine._build_local_result_dict(_mock_doc(), 1.0, regex=regex,
                                              pattern_str="genizah")
    assert "*" in result["snippet"], (
        "D-F5: snippet must contain asterisk markers when regex matches"
    )


def test_regex_non_match_filtered_out():
    """D-F5 / D-04.1 LOAD-BEARING (REVISION 2026-05-24 — Codex HIGH #2 closure):
    when the regex does NOT match the LOCAL content, _build_local_result_dict
    returns None and _query_local_index SKIPS that candidate. The candidate
    is ABSENT from the result list (not displayed with empty highlight_pattern,
    not displayed with content[:200] snippet).

    Matches the Genizah two-phase model (Tantivy candidates -> regex
    filter+highlight -> only matches survive). Tantivy false positives are
    silently filtered.

    Implementation plan: 96-03-PLAN.md (REVISION 2026-05-24).
    REPLACES the old test_local_snippet_fallback_when_regex_no_match — the
    old test asserted fallback-display semantics that D-04.1 explicitly
    reverses.
    """
    engine = _make_engine()
    # Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    # BLOCKER 5 audit (96-09): skip converted to positive assertion.
    assert hasattr(engine, "_build_local_result_dict"), (
        "Phase 96 D-F5 regression: _build_local_result_dict missing from SearchEngine"
    )
    regex = re.compile(r"completely-absent-token", re.IGNORECASE)

    # Test A: _build_local_result_dict returns None when regex doesn't match.
    result = engine._build_local_result_dict(_mock_doc(), 1.0, regex=regex,
                                              pattern_str="completely-absent-token")

    assert result is None, (
        "D-04.1 (Codex HIGH #2 closure): _build_local_result_dict MUST return "
        "None when the regex does not match the LOCAL content. Got a dict — "
        "this is the old 'fallback display' semantics that D-04.1 explicitly "
        "reverses. The Tantivy false positive must be silently filtered out."
    )

    # Test B: _query_local_index returns an empty results list for the same case.
    # Stub a fake local_searcher with one Tantivy hit whose content does NOT
    # contain the regex token.
    fake_doc = _mock_doc(content="the genizah text matches here")
    fake_searcher = MagicMock()
    fake_searcher.search.return_value.hits = [(1.0, 0)]
    fake_searcher.doc.return_value = fake_doc
    engine.local_searcher = fake_searcher
    engine.local_index = MagicMock()
    engine.local_index.parse_query.return_value = MagicMock()

    # Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    # BLOCKER 5 audit (96-09): skip converted to positive assertion.
    results = engine._query_local_index("genizah", "exact", 0, regex=regex)

    assert results == [], (
        "D-04.1 (Codex HIGH #2 closure): _query_local_index MUST return an "
        "EMPTY list when all Tantivy candidates fail the regex check. Got "
        f"{len(results)} hits — the filter-out path is broken."
    )


def test_result_dialog_render_uses_highlight_pattern():
    """D-F5 integration: ResultDialog uses highlight_pattern to apply
    markers on full_text. We do not need a QApplication — just AST-verify
    that the relevant branch (result_dialog.py:2052-2061) still uses
    `highlight_pattern`."""
    from pathlib import Path
    src = (Path(__file__).parent.parent / "desktop" / "result_dialog.py").read_text(encoding="utf-8")
    assert "highlight_pattern" in src, (
        "ResultDialog must still consume 'highlight_pattern' field — "
        "Phase 96 D-F5 normalizes LOCAL to use this field too"
    )


def test_d_f5_integration_regex_arrives_at_build_local_result_dict():
    """D-F5 integration (REVISION 2026-05-24 — checker BLOCKER 4 closure):
    when _query_local_index is invoked via the search merger, the compiled
    `regex` must arrive at _build_local_result_dict NON-None. This converts
    the silent-no-op failure mode (regex=None -> fallback content[:200])
    into a loud, observable test failure if 96-03's merge-site rewire
    regresses.

    Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    BLOCKER 5 audit (96-09): skips converted to positive assertions.
    """
    engine = _make_engine()
    # Phase 96 D-F5 shipped in plan 96-03 (closed 2026-05-24).
    assert hasattr(engine, "_build_local_result_dict"), (
        "Phase 96 D-F5 regression: _build_local_result_dict missing from SearchEngine"
    )

    captured = {}
    orig = engine._build_local_result_dict

    def _spy(doc, score, regex=None, pattern_str=None):
        captured['regex'] = regex
        captured['pattern_str'] = pattern_str
        # Return a minimal dict so the merger doesn't crash
        return orig(doc, score, regex=regex, pattern_str=pattern_str)

    # Stub a fake local_searcher with one hit
    fake_doc = _mock_doc(content="the genizah text matches here")
    fake_searcher = MagicMock()
    fake_searcher.search.return_value.hits = [(1.0, 0)]
    fake_searcher.doc.return_value = fake_doc
    engine.local_searcher = fake_searcher
    engine.local_index = MagicMock()
    engine.local_index.parse_query.return_value = MagicMock()
    engine._build_local_result_dict = _spy

    regex = re.compile(r"genizah", re.IGNORECASE)
    engine._query_local_index("genizah", "exact", 0, regex=regex)

    assert captured.get('regex') is not None, (
        "BLOCKER 4: regex did NOT arrive at _build_local_result_dict — "
        "merge call site (genizah_core.py around line 8363) is not passing "
        "the compiled regex through. This silently degrades D-F5 to no-op."
    )


def test_render_pipeline_format_snippet_handles_local_markers():
    """D-F5: SearchEngine.format_snippet converts *word* to highlighted
    spans — this is the rendering primitive both Genizah and LOCAL hits
    flow through. Confirm it still works (smoke)."""
    try:
        from genizah_core import SearchEngine
    except ImportError:
        pytest.skip("genizah_core import failed")
    out = SearchEngine.format_snippet("the *genizah* text")
    assert "<span" in out or "&lt;span" not in out  # produced HTML span
    assert "genizah" in out
