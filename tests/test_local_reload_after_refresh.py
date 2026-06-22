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
    from shared.search_tokenizer import register_search_tokenizers
    schema = build_local_schema()
    index = tantivy.Index(schema, path=index_dir)
    register_search_tokenizers(index)  # SEED-006: content uses the hebword tokenizer
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
# Phase 95-05 MEDIUM-1 RESOLVED: Responsa operators now work over the LOCAL index.
# ---------------------------------------------------------------------------
# The LOCAL path previously used a simplified parse_query that stripped operator
# metacharacters (#, *, %, |, (a/b)) — so Responsa queries returned nothing in
# LOCAL. _query_local_index now accepts a pre-expanded candidate query built by
# the shared, index-agnostic build_tantivy_query / build_regex_pattern (via
# _build_local_responsa_query_and_regex), so LOCAL applies the SAME grammatical
# prefix/suffix, plene/defective, JA, variant, and (a/b) alternation expansion as
# the main index. (The line-break '|' operator still routes through the separate
# main-index-only _execute_line_break_search — LOCAL falls back gracefully.)

def _make_engine_with_local(tmp_path, docs):
    """Build a LOCAL index with `docs` and a SearchEngine pointed at it (no main index)."""
    import genizah_core
    from unittest.mock import MagicMock as _MM

    index_dir = str(tmp_path / "LocalIndex")
    os.makedirs(index_dir)
    _build_local_index(index_dir, docs)
    with patch.object(genizah_core.Config, "LOCAL_INDEX_DIR", index_dir):
        with patch.object(genizah_core.Config, "LOCAL_LAB_INDEX_DIR", str(tmp_path / "LocalLabIndex")):
            with patch("genizah_core.SearchEngine.reload_index", return_value=False):
                meta = _MM()
                meta.parse_full_id_components.return_value = {}
                engine = genizah_core.SearchEngine(meta, _MM())
    assert engine.local_searcher is not None, "LOCAL searcher should open on the built index"
    # We exercise the non-variant operators (#, *, (a/b)); stub get_variants so any
    # incidental call returns an empty list rather than a non-iterable MagicMock.
    engine.var_mgr.get_variants = _MM(return_value=[])
    return engine


def test_responsa_grammatical_prefix_finds_prefixed_form_in_local(tmp_path):
    """`#word` (grammatical-prefix expansion) finds a prefixed form in LOCAL that a
    bare term cannot (hebword tokenizes `לשלום` as one token, so `שלום` ≠ `לשלום`)."""
    from genizah_core import expand_grammatical_prefixes

    prefixed = [w for w in expand_grammatical_prefixes("שלום") if w != "שלום"]
    assert prefixed, "expand_grammatical_prefixes should yield prefixed forms"
    target = prefixed[0]
    engine = _make_engine_with_local(tmp_path, [{
        "uid": "LOCAL_970000000100000001_P1",
        "content": f"כתב {target} העם בכל מקום",
        "full_header": "970000000100000001_LOCAL_P1_F0001",
    }])
    opts = {"responsa_mode": True}
    # Bare term does NOT match the prefixed token...
    bare = engine.execute_search("שלום", "Phrase", 0, responsa_options=opts, corpus_scope="local")
    assert bare == [], f"bare שלום should not match the prefixed token {target!r}"
    # ...but #-prefix expansion DOES (this returned nothing before the fix).
    hits = engine.execute_search("#שלום", "Phrase", 0, responsa_options=opts, corpus_scope="local")
    assert len(hits) >= 1, f"#שלום must find the prefixed form {target!r} in LOCAL"


def test_responsa_inline_alternation_finds_either_in_local(tmp_path):
    """`(a/b)` inline alternation matches a LOCAL doc containing either alternative."""
    engine = _make_engine_with_local(tmp_path, [{
        "uid": "LOCAL_970000000100000002_P1",
        "content": "ויהי בימי המלך הגדול",
        "full_header": "970000000100000002_LOCAL_P1_F0001",
    }])
    opts = {"responsa_mode": True}
    hits = engine.execute_search("(שלום/המלך)", "Phrase", 0, responsa_options=opts, corpus_scope="local")
    assert len(hits) >= 1, "(שלום/המלך) alternation must find the doc containing המלך in LOCAL"


def test_responsa_line_break_falls_back_without_crashing_in_local(tmp_path):
    """Line-break (`|`) is main-index only; in LOCAL the helper returns None and the
    simplified fallback runs — it must not raise (graceful, documented gap)."""
    engine = _make_engine_with_local(tmp_path, [{
        "uid": "LOCAL_970000000100000003_P1",
        "content": "שורה ראשונה\nשורה שנייה",
        "full_header": "970000000100000003_LOCAL_P1_F0001",
    }])
    opts = {"responsa_mode": True}
    # Must not raise (returns whatever the simplified fallback yields).
    engine.execute_search("ראשונה | שנייה", "Phrase", 0, responsa_options=opts, corpus_scope="local")


def test_local_responsa_helper_bails_on_line_break_and_empty(tmp_path):
    """The helper returns (None, None) for line-break and empty queries so callers
    fall back instead of mis-building a query."""
    engine = _make_engine_with_local(tmp_path, [{
        "uid": "LOCAL_970000000100000004_P1",
        "content": "טקסט כלשהו",
        "full_header": "970000000100000004_LOCAL_P1_F0001",
    }])
    opts = {"responsa_mode": True}
    assert engine._build_local_responsa_query_and_regex("א | ב", "Phrase", 0, opts) == (None, None)
    assert engine._build_local_responsa_query_and_regex("", "Phrase", 0, opts) == (None, None)
    # A plain operator query yields a usable (query, regex) pair.
    q, rx = engine._build_local_responsa_query_and_regex("#שלום", "Phrase", 0, opts)
    assert q and rx is not None
