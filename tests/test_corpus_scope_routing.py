# -*- coding: utf-8 -*-
"""Phase 95 smoke-fix (item 2): corpus_scope routing regression tests.

Asserts:
  - corpus_scope='genizah' → LOCAL _query_local_index is NOT called.
  - corpus_scope='local'   → Genizah Tantivy searcher.search is NOT called;
                             only LOCAL hits returned.
  - corpus_scope='all'     → both are called (existing behaviour, regression guard).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper — bare SearchEngine instance with mocked indexes
# ---------------------------------------------------------------------------

def _make_engine_with_mocks():
    """Return (engine, mock_searcher, mock_local_searcher)."""
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())

    # Minimal Genizah searcher mock — returns 0 hits so we can inspect call counts.
    mock_searcher = MagicMock()
    mock_hits_obj = MagicMock()
    mock_hits_obj.hits = []
    mock_searcher.search.return_value = mock_hits_obj
    engine.searcher = mock_searcher

    # Minimal LOCAL searcher mock
    mock_local_searcher = MagicMock()
    engine.local_searcher = mock_local_searcher

    return engine, mock_searcher, mock_local_searcher


# ---------------------------------------------------------------------------
# corpus_scope='genizah' — LOCAL query must be SKIPPED
# ---------------------------------------------------------------------------

def test_corpus_scope_genizah_skips_local_query():
    """corpus_scope='genizah': _query_local_index must NOT be called."""
    engine, mock_searcher, _mock_local = _make_engine_with_mocks()

    with patch.object(engine, "_query_local_index") as mock_local_query:
        engine.execute_search("test", "literal", 0, corpus_scope="genizah")

    mock_local_query.assert_not_called(), (
        "corpus_scope='genizah' must skip LOCAL index query"
    )


# ---------------------------------------------------------------------------
# corpus_scope='local' — Genizah Tantivy search must be SKIPPED
# ---------------------------------------------------------------------------

def test_corpus_scope_local_skips_genizah_search():
    """corpus_scope='local': Genizah searcher.search must NOT be called."""
    engine, mock_searcher, _mock_local = _make_engine_with_mocks()

    # _query_local_index returns empty list for simplicity
    with patch.object(engine, "_query_local_index", return_value=[]) as mock_local_query:
        engine.execute_search("test", "literal", 0, corpus_scope="local")

    mock_searcher.search.assert_not_called(), (
        "corpus_scope='local' must skip Genizah Tantivy search"
    )
    mock_local_query.assert_called_once(), (
        "corpus_scope='local' must call _query_local_index"
    )


def test_corpus_scope_local_returns_local_hits_only():
    """corpus_scope='local': result list contains only LOCAL-sourced hits."""
    engine, _mock_searcher, _mock_local = _make_engine_with_mocks()

    local_hit = {
        "uid": "LOCAL_abc_P1_F1",
        "full_text": "local content",
        "snippet": "local content",
        "display": {"id": "LOCAL_abc", "source": "LOCAL"},
    }
    with patch.object(engine, "_query_local_index", return_value=[local_hit]):
        results = engine.execute_search("test", "literal", 0, corpus_scope="local")

    assert results == [local_hit], (
        "corpus_scope='local' must return exactly the LOCAL hits"
    )


# ---------------------------------------------------------------------------
# corpus_scope='all' — LOCAL index IS consulted (regression guard)
# ---------------------------------------------------------------------------

def test_corpus_scope_all_calls_local_query():
    """corpus_scope='all' (default): _query_local_index IS called when Genizah
    query succeeds. We mock index.parse_query so the Genizah path completes
    and reaches the LOCAL merge block.
    """
    engine, mock_searcher, _mock_local = _make_engine_with_mocks()

    # Make the Genizah parse_query + search succeed (returns 0 hits).
    mock_index = MagicMock()
    mock_hits_obj = MagicMock()
    mock_hits_obj.hits = []
    mock_index.parse_query.return_value = MagicMock()
    mock_searcher.search.return_value = mock_hits_obj
    engine.index = mock_index

    with patch.object(engine, "_query_local_index", return_value=[]) as mock_local_query:
        engine.execute_search("test", "literal", 0, corpus_scope="all")

    mock_local_query.assert_called(), (
        "corpus_scope='all' must call _query_local_index after Genizah search"
    )
