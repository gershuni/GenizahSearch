"""The engine says when a result list leaves matches out (owner decision D8, 2026-10-04).

The desktop keeps today's 50,000-candidate cut-off for the display but must say so:
the count shows "N+", and a combination built on such a step completes it first.
execute_search records {'capped', 'interrupted'} per thread (consume_last_search_cutoff),
from every query that can hit the limit -- page docs, whole-manuscript docs, the
line-break search, My Library -- and from Stop. The return value is unchanged, so the
web and the API are not affected.
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

W, C = "שלום", "ברכה"
B1, B2 = "ברוך", "הבא"


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
                "id": header.split()[-1], "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


def _build(path, docs):
    os.makedirs(path)
    idx = tantivy.Index(_schema(), path=path)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text, scope, bounds in docs:
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} 99{len(uid)}", shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    idx.reload()
    return idx


PAGES = [(f"p{n}", f"אבג {W} דהו\n{C} זחט", "page", "") for n in range(4)]
# Two whole-manuscript docs whose page break holds B1 | B2.
AGG_TEXT = f"אבג {B1}\n{B2} דהו"
AGGS = [(f"sys:{n}", AGG_TEXT, "system", json.dumps([
    {"uid": f"a{n}", "p_num": 1, "full_header": f"a{n}", "source": "V0.8", "sys_id": "1",
     "start": 0, "end": len(f"אבג {B1}") + 1},
    {"uid": f"b{n}", "p_num": 2, "full_header": f"b{n}", "source": "V0.8", "sys_id": "1",
     "start": len(f"אבג {B1}") + 1, "end": len(AGG_TEXT)}])) for n in range(2)]


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("cutoff")
    _build(os.path.join(str(root), "tantivy_db"), PAGES + AGGS)
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


def _cutoff(engine, query, limit, mode="literal", **kw):
    se.consume_last_search_cutoff()
    with patch.object(Config, "SEARCH_LIMIT", limit):
        engine.execute_search(query, mode, 0, corpus_scope="genizah", **kw)
    return se.consume_last_search_cutoff()


@pytest.mark.parametrize("mode", ["literal", "variants", "fuzzy"])
def test_a_query_that_reaches_the_limit_is_capped(engine, mode):
    assert _cutoff(engine, W, 2, mode) == {"capped": True, "interrupted": False}
    assert _cutoff(engine, W, 50, mode) == {"capped": False, "interrupted": False}


def test_a_phrase_whose_page_docs_reach_the_limit_is_capped(engine):
    # The page query fills its limit; the aggregate query after it does not, and must
    # not clear what the page query said.
    assert _cutoff(engine, f"אבג {W}", 2)["capped"]
    assert not _cutoff(engine, f"אבג {W}", 50)["capped"]


def test_whole_manuscript_docs_that_reach_their_limit_are_capped(engine):
    # The phrase's page docs are few; its two aggregates fill an aggregate limit of 1.
    assert _cutoff(engine, f"{B1} {B2}", 1)["capped"]
    assert not _cutoff(engine, f"{B1} {B2}", 50)["capped"]


def test_the_line_break_search_reports_its_limit(engine):
    opts = {"responsa_mode": True}
    assert _cutoff(engine, f"{W} | {C}", 2, responsa_options=opts)["capped"]
    assert not _cutoff(engine, f"{W} | {C}", 50, responsa_options=opts)["capped"]


def test_stop_is_reported(engine):
    se.consume_last_search_cutoff()

    def stop(i, total):
        if i:
            raise InterruptedError
    with patch.object(se, "_PROGRESS_TICK_EVERY", 1):
        engine.execute_search(W, "literal", 0, corpus_scope="genizah", progress_callback=stop)
    assert se.consume_last_search_cutoff()["interrupted"]


def test_the_signal_belongs_to_one_search(engine):
    se.consume_last_search_cutoff()
    with patch.object(Config, "SEARCH_LIMIT", 2):
        engine.execute_search(W, "literal", 0, corpus_scope="genizah")   # capped, not read
    with patch.object(Config, "SEARCH_LIMIT", 50):
        engine.execute_search(W, "literal", 0, corpus_scope="genizah")
    assert not se.consume_last_search_cutoff()["capped"], "a search must start from a clean signal"
    assert se.consume_last_search_cutoff() == {"capped": False, "interrupted": False}   # read once


def test_my_library_reports_its_limit(engine, tmp_path):
    local = _build(str(tmp_path / "local"), [(f"l{n}", f"אבג {W}", "page", "") for n in range(3)])
    saved = (getattr(engine, "local_index", None), getattr(engine, "local_searcher", None))
    engine.local_index, engine.local_searcher = local, local.searcher()
    try:
        se.consume_last_search_cutoff()
        with patch.object(Config, "SEARCH_LIMIT", 2):
            engine.execute_search(W, "literal", 0, corpus_scope="local")
        assert se.consume_last_search_cutoff()["capped"]
    finally:
        engine.local_index, engine.local_searcher = saved


def test_the_desktop_thread_hands_the_signal_over_before_the_results(engine):
    from desktop.gui_threads import SearchThread
    got = []
    t = SearchThread(engine, W, "literal", 0, corpus_scope="genizah")
    t.cutoff_signal.connect(lambda c: got.append(("cutoff", c)))
    t.results_signal.connect(lambda r: got.append(("results", len(r))))
    with patch.object(Config, "SEARCH_LIMIT", 2):
        t.run()                                 # same thread: signals are delivered directly
    assert [k for k, _v in got] == ["cutoff", "results"] and got[0][1]["capped"]
