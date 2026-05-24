# -*- coding: utf-8 -*-
"""Phase 96 NEW-2: LOCAL navigation primitive + View-All separator tests.

Implementation plan: 96-03-PLAN.md (engine primitive), 96-08-PLAN.md (UI wiring).
"""
import pytest
from unittest.mock import MagicMock, patch


def _make_engine_with_local_pages(sys_id="97000000010001", pages=None):
    """Build a SearchEngine whose LOCAL index is mocked to return `pages`
    when queried by sys_id prefix."""
    try:
        from genizah_core import SearchEngine
    except ImportError:
        pytest.skip("genizah_core import failed")
    pages = pages or [
        (1, "first page text"),
        (2, "second page text"),
        (3, "third page text"),
    ]
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())

    fake_docs = []
    for p, txt in pages:
        d = MagicMock()
        fh = f"{sys_id}_LOCAL_P{p}_F0"
        fields = {"full_header": fh, "content": txt, "unique_id": f"{fh}_uid"}
        d.get_first.side_effect = lambda f, _fields=fields: _fields.get(f, "")
        fake_docs.append(d)

    fake_searcher = MagicMock()
    fake_searcher.search.return_value.hits = [(1.0, i) for i in range(len(fake_docs))]
    fake_searcher.doc.side_effect = lambda i: fake_docs[i]
    engine.local_searcher = fake_searcher
    engine.local_index = MagicMock()
    engine.local_index.parse_query.return_value = MagicMock()
    return engine, sys_id


def test_next_page():
    """NEW-2: get_local_browse_page(sys_id, p_num=1, next_prev=1) returns page 2."""
    engine, sid = _make_engine_with_local_pages()
    if not hasattr(engine, "get_local_browse_page"):
        pytest.skip("Phase 96 NEW-2 primitive not yet implemented (waiting for 96-03)")
    res = engine.get_local_browse_page(sid, p_num=1, next_prev=1)
    assert res is not None
    assert res["p_num"] == 2
    assert "second" in res["text"]


def test_no_wrap_at_boundary():
    """NEW-2 D-12: get_local_browse_page returns None at end-of-file
    (no wrap, buttons disabled)."""
    engine, sid = _make_engine_with_local_pages()
    if not hasattr(engine, "get_local_browse_page"):
        pytest.skip("Phase 96 NEW-2 primitive not yet implemented (waiting for 96-03)")
    res_end = engine.get_local_browse_page(sid, p_num=3, next_prev=1)
    res_start = engine.get_local_browse_page(sid, p_num=1, next_prev=-1)
    assert res_end is None, "no wrap at end"
    assert res_start is None, "no wrap at start"


def test_view_all_separators():
    """NEW-2 D-14: View-All aggregates all pages with '-- page N --' separators."""
    try:
        # 96-08 introduces this helper either on engine or in genizah_app.
        from genizah_app import _aggregate_local_pages_with_separators as agg
    except ImportError:
        pytest.skip("Phase 96 NEW-2 View-All helper not yet implemented (96-08)")
    out = agg([(1, "alpha"), (2, "beta"), (3, "gamma")], is_pdf=True)
    assert "alpha" in out and "beta" in out and "gamma" in out
    # Separator format from PATTERNS / RESEARCH §4
    assert "page 2" in out or "page 2 " in out or "— page 2 —" in out


def test_format_aware_label():
    """NEW-2 D-12: PDF gets 'page' label, DOCX/TXT gets 'chunk' label."""
    try:
        from genizah_app import _aggregate_local_pages_with_separators as agg
    except ImportError:
        pytest.skip("Phase 96 NEW-2 View-All helper not yet implemented (96-08)")
    pdf_out = agg([(1, "a"), (2, "b")], is_pdf=True)
    chunk_out = agg([(1, "a"), (2, "b")], is_pdf=False)
    assert "page" in pdf_out.lower()
    assert "chunk" in chunk_out.lower()
