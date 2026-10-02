"""A single Exact, Variants or Fuzzy word is searched in page docs only, not in whole-manuscript / part docs.

A single word cannot span a page break, so an aggregate doc only repeated a page hit
or, through its first-match-in-the-manuscript row, added a match INSIDE a longer word
on some page (לשלום) -- found only when the same manuscript also had the plain word
elsewhere. Owner decision 2026-09-30: Literal matches exact words. On the real index
the aggregates were most of the load time (שלום 10.1 s -> 1.7 s uncapped); the
real-index gate is described in docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md.

The fast path must stay off for multi-word queries (cross-page matches), for text
positions, for other modes, and for an index whose docs do not all carry a scope.
Variants got the same path on 2026-10-01 (owner: Variants match whole words too), and Fuzzy
on 2026-10-02 (near spellings, whole words: לשלום is one edit from שלום, so it is a match,
found on its own page).
"""
import gc
import json
import os
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
import shared.search_engine as se  # noqa: E402

W = "שלום"                       # שלום
PAGE_A = f"ל{W} רב"                   # לשלום רב   (only inside a longer word)
PAGE_B = f"{W} עליכם"       # שלום עליכם


def _schema():
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True)
    b.add_text_field("content", stored=True, tokenizer_name="hebword")
    for f in ("content_head", "content_tail", "line_starts", "line_ends"):
        b.add_text_field(f, stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_search", stored=False, tokenizer_name="hebword")
    for f in ("source", "full_header", "shelfmark", "scope", "boundaries"):
        b.add_text_field(f, stored=True)
    return b.build()


class _Meta:
    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


def _build(root, *, extra_scopeless_doc=False):
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, scope, bounds="", **extra):
        fields = dict(unique_id=uid, content=text, content_search=strip_search_diacritics(text),
                      source="V0.8", full_header=f"{uid} 990001", shelfmark=uid, boundaries=bounds,
                      **Indexer._extract_position_fields(text))
        if scope is not None:
            fields["scope"] = scope
        w.add_document(tantivy.Document(**fields))

    add("A", PAGE_A, "page")
    add("B", PAGE_B, "page")
    text = PAGE_A + "\n" + PAGE_B
    bounds = json.dumps([
        {"uid": "A", "p_num": 1, "full_header": "A 990001", "source": "V0.8", "sys_id": "990001",
         "start": 0, "end": len(PAGE_A) + 1},
        {"uid": "B", "p_num": 2, "full_header": "B 990001", "source": "V0.8", "sys_id": "990001",
         "start": len(PAGE_A) + 1, "end": len(text)}], ensure_ascii=False)
    add("sys:990001", text, "system", bounds)
    if extra_scopeless_doc:
        add("C", "עוד", None)   # עוד -- a doc with no scope at all
    w.commit()
    w.wait_merging_threads()


@pytest.fixture(scope="module")
def engines(tmp_path_factory):
    made = {}
    patchers = []
    for name, scopeless in (("complete", False), ("scopeless", True)):
        root = tmp_path_factory.mktemp(f"single_{name}")
        _build(root, extra_scopeless_doc=scopeless)
        p = patch.object(Config, "INDEX_DIR", str(root))
        p.start()
        made[name] = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
        p.stop()
        patchers.append(p)
    yield made
    made.clear()
    gc.collect()


def _uids(engine, query, mode="literal", **kw):
    return {r["uid"] for r in engine.execute_search(query, mode, 0, corpus_scope="genizah", **kw)}


def test_single_word_is_found_on_its_page_only(engines):
    # A (לשלום) came only through the manuscript doc's first match; it is not a Literal match.
    assert _uids(engines["complete"], W) == {"B"}


def test_the_manuscript_doc_is_still_used_when_a_doc_has_no_scope(engines):
    eng = engines["scopeless"]
    assert eng._every_doc_has_scope() is False
    assert not any("scope:page" in q for q in _queries(eng, W))   # no page-only clause
    # The manuscript doc is read, but its first match (לשלום) is inside a longer
    # word: the whole-word check moves on to page B's שלום.
    got = _uids(eng, W)
    assert "A" not in got and "B" in got


def test_single_word_variants_is_found_on_its_page_only(engines):
    assert _uids(engines["complete"], W, mode="variants") == {"B"}


def test_single_word_fuzzy_is_found_on_its_page_only(engines):
    rows = engines["complete"].execute_search(W, "fuzzy", 0, corpus_scope="genizah")
    # לשלום is a near spelling of שלום (one added letter) -- a whole word on page A.
    assert {r["uid"]: r["scope"] for r in rows} == {"A": "page", "B": "page"}


class _RecordingIndex:
    """Forwards to the real tantivy Index (whose methods cannot be patched) and
    records every query string it parses."""

    def __init__(self, real):
        self._real, self.parsed = real, []

    def parse_query(self, qs, fields):
        self.parsed.append(qs)
        return self._real.parse_query(qs, fields)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _queries(eng, query, mode="literal", **kw):
    real = eng.index
    eng.index = rec = _RecordingIndex(real)
    try:
        eng.execute_search(query, mode, 0, corpus_scope="genizah", **kw)
    finally:
        eng.index = real
    return rec.parsed


AGG = "(scope:system OR scope:part)"


@pytest.mark.parametrize("mode", ["literal", "variants", "fuzzy"])
def test_single_word_never_queries_the_aggregates(engines, mode):
    assert not any(AGG in q for q in _queries(engines["complete"], W, mode=mode))


@pytest.mark.parametrize("query, kw", [
    (W, {"text_position": "start"}),
    (f"{W} עליכם", {}),
    (f"{W} עליכם", {"mode": "variants"}),
    (f"{W} עליכם", {"mode": "fuzzy"}),
    (W, {"mode": "fuzzy", "text_position": "start"}),
], ids=["position", "phrase", "variants-phrase", "fuzzy-phrase", "fuzzy-position"])
def test_other_searches_still_query_the_aggregates(engines, query, kw):
    assert any(AGG in q for q in _queries(engines["complete"], query, **kw))


class _NoSearch:
    num_docs = 0

    def search(self, *a, **k):
        raise AssertionError("re-counted")


def test_scope_check_is_cached_and_true_for_a_complete_index(engines):
    eng = engines["complete"]
    assert eng._every_doc_has_scope() is True
    real, eng.searcher = eng.searcher, _NoSearch()
    try:
        assert eng._every_doc_has_scope() is True     # cached: no second count
    finally:
        eng.searcher = real
