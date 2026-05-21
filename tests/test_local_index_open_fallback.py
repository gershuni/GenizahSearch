# -*- coding: utf-8 -*-
"""Phase 95 D-37: fallback when LOCAL side-index is missing/corrupt/locked.

Tests that SearchEngine.local_searcher is None on open failure, and that
main search returns Genizah-only results without exception.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_search_engine():
    """Construct a SearchEngine with mocked meta/variants managers (no real index needed)."""
    from genizah_core import SearchEngine
    meta = MagicMock()
    meta.parse_full_id_components.return_value = {}
    var_mgr = MagicMock()
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        engine = SearchEngine(meta, var_mgr)
    engine.searcher = None  # ensure no real Tantivy searcher
    return engine


# ---------------------------------------------------------------------------
# Test: corrupt LOCAL index directory falls back
# ---------------------------------------------------------------------------

def test_corrupt_local_index_falls_back_to_genizah_only(tmp_path, caplog):
    """D-37: when LOCAL_INDEX_DIR contains a corrupt meta.json (not a valid Tantivy index),
    local_searcher is None, no exception propagates, and a warning is logged.
    """
    import genizah_core

    # Write a corrupt meta.json to simulate a corrupted Tantivy index.
    # An invalid meta.json causes tantivy.Index.open() to raise ValueError.
    corrupt_dir = tmp_path / "corrupt_local"
    corrupt_dir.mkdir()
    (corrupt_dir / "meta.json").write_text("{invalid json!!!}")

    warnings_logged = []

    def _patched_warning(msg, *args, **kwargs):
        warnings_logged.append(msg % args if args else msg)

    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", str(corrupt_dir)):
        with patch("genizah_core.SearchEngine.reload_index", return_value=False):
            with patch.object(genizah_core.LOGGER, "warning", side_effect=_patched_warning):
                meta = MagicMock()
                meta.parse_full_id_components.return_value = {}
                engine = genizah_core.SearchEngine(meta, MagicMock())

    assert engine.local_searcher is None, "local_searcher must be None on corrupt index"
    assert any(
        "LOCAL index unavailable" in msg or "LOCAL" in msg
        for msg in warnings_logged
    ), f"Expected a warning about LOCAL index. Got: {warnings_logged}"


def test_missing_local_index_dir_falls_back(tmp_path, caplog):
    """D-37: when LOCAL_INDEX_DIR does not exist, local_searcher is None, no exception."""
    import genizah_core

    nonexistent = str(tmp_path / "does_not_exist")
    assert not os.path.exists(nonexistent)

    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", nonexistent):
        with patch("genizah_core.SearchEngine.reload_index", return_value=False):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = genizah_core.SearchEngine(meta, MagicMock())

    assert engine.local_searcher is None, "local_searcher must be None when dir is absent"


def test_local_searcher_attr_exists_on_engine():
    """SearchEngine must expose a local_searcher attribute (None or searcher)."""
    import genizah_core

    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", "/nonexistent_path"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = genizah_core.SearchEngine(meta, MagicMock())

    assert hasattr(engine, "local_searcher"), "SearchEngine must have local_searcher attr"


def test_main_search_returns_genizah_only_when_local_unavailable():
    """D-37: when local_searcher is None, execute_search returns results without LOCAL rows."""
    from genizah_core import SearchEngine

    meta = MagicMock()
    meta.parse_full_id_components.return_value = {}
    var_mgr = MagicMock()

    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_query_local_index", return_value=[]) as mock_local:
            engine = SearchEngine(meta, var_mgr)
            engine.local_searcher = None
            engine.searcher = None  # no main index either

    # With no searchers, execute_search returns [] immediately
    result = engine.execute_search("שלום", "Phrase", 0)
    assert isinstance(result, list), "execute_search must return a list"
    assert not any(
        r.get("display", {}).get("source") == "LOCAL" for r in result
    ), "No LOCAL rows when local_searcher is None"


def test_query_local_index_returns_empty_when_local_searcher_none():
    """_query_local_index must return [] when local_searcher is None (D-37)."""
    from genizah_core import SearchEngine

    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
            engine.local_searcher = None

    result = engine._query_local_index("שלום", "Phrase", 0)
    assert result == [], "_query_local_index must return [] when local_searcher is None"


def test_rrf_merge_method_exists():
    """SearchEngine must have a _rrf_merge method (D-08 / RESEARCH Pattern 1)."""
    from genizah_core import SearchEngine

    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())

    assert callable(getattr(engine, "_rrf_merge", None)), (
        "SearchEngine must have a callable _rrf_merge method"
    )
