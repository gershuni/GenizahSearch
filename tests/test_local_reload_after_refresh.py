# -*- coding: utf-8 -*-
"""Phase 95 HIGH-1 review fix: SearchEngine.reload_local_indexes() picks up
newly committed docs without app restart.

Also covers:
  - reload_local_lab_index() for LAB side-index
  - No-op / graceful fallback when dir is missing or index fails to open
  - MEDIUM-1 (Option B deferred): query semantics parity tests marked xfail
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
import tantivy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine_no_local():
    """Construct a SearchEngine with no real index and local_searcher=None."""
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None
    engine.local_lab_searcher = None
    engine._lab_local_meta = None
    return engine


def _write_marker(index_dir: str) -> None:
    """Write the .schema_version marker so SearchEngine opens the index without
    triggering the Phase 97.2 schema-mismatch rebuild path (which, on Windows,
    fails to rename a still-open index dir). Production always writes this marker
    via LocalIndexer; the manual test builds must do the same."""
    from shared.local_indexer import (
        build_local_schema, _compute_schema_marker, _write_schema_marker,
    )
    _write_schema_marker(index_dir, _compute_schema_marker(build_local_schema))


def _build_local_index(index_dir: str, docs: list[dict]) -> None:
    """Build a minimal LOCAL Tantivy index with the given docs for testing."""
    from shared.local_indexer import build_local_schema
    schema = build_local_schema()
    index = tantivy.Index(schema, path=index_dir)
    writer = index.writer(heap_size=15_000_000)
    for doc in docs:
        writer.add_document(tantivy.Document(
            unique_id=[doc["uid"]],
            content=[doc["content"]],
            content_head=[doc["content"][:50]],
            content_tail=[doc["content"][-50:]],
            line_starts=[""],
            line_ends=[""],
            source=["LOCAL"],
            full_header=[doc.get("full_header", f"970000000100000001_LOCAL_P1_F0001")],
            shelfmark=[doc.get("shelfmark", "test.txt")],
            scope=["page"],
            boundaries=[""],
        ))
    writer.commit()
    del writer
    del index
    _write_marker(index_dir)


# ---------------------------------------------------------------------------
# Test: reload picks up newly committed docs (HIGH-1 load-bearing)
# ---------------------------------------------------------------------------

def test_reload_local_indexes_picks_up_new_docs_without_restart(tmp_path):
    """HIGH-1: after calling reload_local_indexes(), a newly committed doc
    becomes visible in the search session without app restart.
    """
    import genizah_core

    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir)

    # Build an EMPTY index first
    from shared.local_indexer import build_local_schema
    schema = build_local_schema()
    empty_index = tantivy.Index(schema, path=index_dir)
    writer = empty_index.writer(heap_size=15_000_000)
    writer.commit()
    del writer
    del empty_index
    _write_marker(index_dir)

    # Open engine against the empty index
    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", index_dir):
        with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "LocalLabIndex")):
            with patch("genizah_core.SearchEngine.reload_index", return_value=False):
                meta = MagicMock()
                meta.parse_full_id_components.return_value = {}
                engine = genizah_core.SearchEngine(meta, MagicMock())

    # Verify initial state: local_searcher opened (empty index)
    assert engine.local_searcher is not None, "local_searcher should be opened on valid (empty) index"
    initial_query_result = engine._query_local_index("unique_test_token_xyz", "Phrase", 0)
    assert initial_query_result == [], "Empty index should return 0 LOCAL hits"

    # Simulate a background worker adding a doc and committing
    _build_local_index(index_dir, [{
        "uid": "LOCAL_970000000100000001_P1",
        "content": "unique_test_token_xyz this is a test document",
        "full_header": "970000000100000001_LOCAL_P1_F0001",
        "shelfmark": "test.txt",
    }])

    # WITHOUT reload, still returns 0 (Tantivy searcher is snapshotted at open time)
    # (This is the bug HIGH-1 flags — can't easily test "no reload → no result" due to
    # Tantivy's internal caching, but we test that AFTER reload, the new doc IS found.)

    # Call reload_local_indexes() to pick up new commit
    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", index_dir):
        with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "LocalLabIndex")):
            engine.reload_local_indexes()

    # Now the new doc should be visible
    assert engine.local_searcher is not None, "local_searcher should be non-None after reload"
    results = engine._query_local_index("unique_test_token_xyz", "Phrase", 0)
    assert len(results) >= 1, (
        "After reload_local_indexes(), the newly committed doc must be visible. "
        f"Got {len(results)} results."
    )
    assert results[0].get("display", {}).get("source") == "LOCAL"


# ---------------------------------------------------------------------------
# Test: reload is a no-op when LOCAL_INDEX_DIR does not exist
# ---------------------------------------------------------------------------

def test_reload_local_indexes_no_op_when_dir_missing(tmp_path):
    """HIGH-1 + D-37: when LOCAL_INDEX_DIR does not exist, reload_local_indexes()
    leaves local_searcher = None and does NOT raise.
    """
    import genizah_core

    nonexistent = str(tmp_path / "does_not_exist")
    engine = _make_engine_no_local()

    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", nonexistent):
        with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "lab_nope")):
            engine.reload_local_indexes()

    assert engine.local_searcher is None, (
        "reload_local_indexes() must leave local_searcher=None when dir is absent"
    )


# ---------------------------------------------------------------------------
# Test: reload recovers from a transient open failure
# ---------------------------------------------------------------------------

def test_reload_local_indexes_recovers_from_transient_lock_error(tmp_path):
    """HIGH-1 + D-37: when tantivy.Index raises on reload, local_searcher is set
    to None (defensive — same D-37 fallback semantics on reload as at init).
    """
    import genizah_core

    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir)

    engine = _make_engine_no_local()

    # Patch tantivy.Index to raise IOError
    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", index_dir):
        with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "lab")):
            with patch("genizah_core.tantivy.Index", side_effect=IOError("simulated lock")):
                engine.reload_local_indexes()

    assert engine.local_searcher is None, (
        "On open failure during reload, local_searcher must be None (D-37 fallback)"
    )


# ---------------------------------------------------------------------------
# Test: reload_local_lab_index works independently
# ---------------------------------------------------------------------------

def test_reload_local_lab_index_no_op_when_dir_missing(tmp_path):
    """reload_local_lab_index() leaves local_lab_searcher=None when dir absent."""
    import genizah_core

    engine = _make_engine_no_local()

    with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "nope")):
        engine.reload_local_lab_index()

    assert engine.local_lab_searcher is None


# ---------------------------------------------------------------------------
# MEDIUM-1 deferred (Option B): query semantics parity tests (xfail)
# ---------------------------------------------------------------------------
# The main execute_search query builder uses Responsa expansion, spelling variants,
# grammatical prefix/suffix expansion, Judeo-Arabic expansion, line constraints, and
# flex spacing — over 200 LOC. Extracting a shared _build_tantivy_query() helper
# is too invasive for this revision.
#
# These tests are marked xfail to document the known divergence and serve as a
# follow-up trigger. When the shared helper lands, remove the xfail markers.
# See plan 95-05 <deferred> block.

@pytest.mark.xfail(
    reason=(
        "MEDIUM-1 deferred: shared query builder not yet extracted from execute_search. "
        "_query_local_index uses a simplified parse_query (content/content_head/content_tail) "
        "without Responsa expansion, spelling variants, or Hebrew morphological expansion. "
        "Follow-up: extract _build_tantivy_query() in a future v7.14.x patch plan."
    ),
    strict=False,
)
def test_query_semantics_phrase_mode_parity_with_main(tmp_path):
    """MEDIUM-1 Option B (xfail): phrase mode search should produce identical
    hit-sets on main-index and LOCAL-index fixtures for the same document.
    This test documents the known divergence until the shared builder is extracted.
    """
    # This test intentionally fails to document the deferred work.
    # When the shared builder is extracted, this should pass.
    pytest.xfail("MEDIUM-1 deferred — shared query builder not yet extracted")


@pytest.mark.xfail(
    reason=(
        "MEDIUM-1 deferred: gap mode parity not tested until shared builder extracted."
    ),
    strict=False,
)
def test_query_semantics_gap_mode_parity_with_main(tmp_path):
    """MEDIUM-1 Option B (xfail): gap mode search parity between main and LOCAL index."""
    pytest.xfail("MEDIUM-1 deferred — shared query builder not yet extracted")
