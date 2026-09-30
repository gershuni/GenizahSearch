"""A single Literal word is searched in page docs only, not in whole-manuscript / part docs.

A single word cannot span a page break, so an aggregate doc only repeated a page hit
or, through its first-match-in-the-manuscript row, added a match INSIDE a longer word
on some page (לשלום) -- found only when the same manuscript also had the plain word
elsewhere. Owner decision 2026-09-30: Literal matches exact words. On the real index
the aggregates were most of the load time (שלום 10.1 s -> 1.7 s uncapped); the
real-index gate is described in docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md.

The fast path must stay off for multi-word queries (cross-page matches), for text
positions, for other modes, and for an index whose docs do not all carry a scope.
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
    assert "A" in _uids(eng, W)                     # today's behaviour, unchanged


def test_other_modes_keep_the_manuscript_docs(engines):
    assert "A" in _uids(engines["complete"], W, mode="variants")


def test_page_only_path_not_consulted_for_positions_phrases_or_other_modes(engines):
    eng = engines["complete"]
    with patch.object(eng, "_every_doc_has_scope", side_effect=AssertionError("fast path consulted")):
        eng.execute_search(W, "literal", 0, text_position="start", corpus_scope="genizah")
        eng.execute_search(f"{W} עליכם", "literal", 0, corpus_scope="genizah")
        eng.execute_search(W, "variants", 0, corpus_scope="genizah")
        eng.execute_search(W, "fuzzy", 0, corpus_scope="genizah")


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
