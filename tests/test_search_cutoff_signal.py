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


def test_a_stopped_composition_search_is_reported(engine):
    """The website's time limit stops a composition search (a background /api/parallels
    job) through its progress callback: the payload says 'partial', and the cut-off
    signal says so too, as for every other search (2026-10-07)."""
    text = " ".join([f"אבג {W} דהו {C} זחט"] * 3)

    def stop(i, total):
        if i >= 2:
            raise InterruptedError
    se.consume_last_search_cutoff()
    result = engine.search_composition_logic(text, 2, float("inf"), "literal", progress_callback=stop)
    assert result["partial"] is True
    assert result["main"], "the matches of the chunks searched before the stop are kept"
    assert se.consume_last_search_cutoff()["interrupted"]
    whole = engine.search_composition_logic(text, 2, float("inf"), "literal")
    assert whole["partial"] is False
    assert not se.consume_last_search_cutoff()["interrupted"], "a finished scan is not"


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


# --- Codex review of PR #375 (round 5) ------------------------------------------------

@pytest.fixture
def my_library(engine, tmp_path):
    local = _build(str(tmp_path / "local"), [(f"l{n}", f"אבג {W} {C}", "page", "") for n in range(5)])
    saved = (getattr(engine, "local_index", None), getattr(engine, "local_searcher", None))
    engine.local_index, engine.local_searcher = local, local.searcher()
    yield engine
    engine.local_index, engine.local_searcher = saved


def _local_rows(rows):
    return [r for r in rows if r.get("display", {}).get("source") == "LOCAL"]


@pytest.mark.parametrize("scope, responsa", [("local", False), ("local", True), ("all", True), ("all", False)])
def test_ids_only_reads_every_my_library_candidate(my_library, scope, responsa):
    # complete_chain completing a My Library step: the LOCAL-only paths (and the 'all'
    # merge's Responsa path) kept the display limit, so the "complete" set was the cut one.
    opts = {"responsa_mode": True} if responsa else None
    se.consume_last_search_cutoff()
    with patch.object(Config, "SEARCH_LIMIT", 2):
        rows = my_library.execute_search(W, "literal", 0, corpus_scope=scope, ids_only=True,
                                         responsa_options=opts)
        cutoff = se.consume_last_search_cutoff()
    assert len(_local_rows(rows)) == 5
    assert not cutoff["capped"]


class _TitleMeta:
    ids = [f"99{n:06d}" for n in range(50)]

    def search_by_meta(self, query_str, target_field):
        return list(self.ids)

    def get_meta_for_id(self, sid):
        return {"shelfmark": f"SM {sid}", "title": "T", "library_code": "CUL"}

    def get_library_for_id(self, sid):
        return "CUL"


@pytest.mark.parametrize("mode", ["Title", "Shelfmark"])
def test_a_stopped_title_or_shelfmark_search_is_reported(mode):
    eng = se.SearchEngine.__new__(se.SearchEngine)       # metadata rows need no index
    eng.meta_mgr, eng.searcher = _TitleMeta(), None

    def stop(i, total):
        if i >= 10:
            raise InterruptedError
    se.consume_last_search_cutoff()
    rows = eng.execute_search("*", mode, 0, progress_callback=stop, restrict_sys_ids=set(_TitleMeta.ids))
    assert 0 < len(rows) < 50, "partial rows come back"
    assert se.consume_last_search_cutoff()["interrupted"]
    eng.execute_search("*", mode, 0, restrict_sys_ids=set(_TitleMeta.ids))
    assert not se.consume_last_search_cutoff()["interrupted"], "a finished scan is not"


# --- Exactly the limit is not a cut (Codex review of the Regex prefilter PR) --------
# A query with exactly SEARCH_LIMIT candidates left nothing out, yet showed "N+": the
# flag was len(hits) >= limit. Now one more hit is asked for and only `limit` are read.

def _run(engine, query, limit, mode="literal", **kw):
    se.consume_last_search_cutoff()
    with patch.object(Config, "SEARCH_LIMIT", limit):
        rows = engine.execute_search(query, mode, 0, **kw)
        return rows, se.consume_last_search_cutoff()


@pytest.mark.parametrize("mode", ["literal", "variants", "fuzzy", "Regex"])
def test_exactly_the_limit_is_not_a_cut(engine, mode):
    n = len(PAGES)                     # the page docs holding W; every one matches
    rows, cut = _run(engine, W, n, mode, corpus_scope="genizah")
    assert len(rows) == n and cut == {"capped": False, "interrupted": False}
    rows, cut = _run(engine, W, n - 1, mode, corpus_scope="genizah")
    assert len(rows) == n - 1, "only `limit` candidates are read"
    assert cut["capped"]


def test_exactly_the_limit_of_whole_manuscript_docs_is_not_a_cut(engine):
    n = len(AGGS)                      # the phrase is found only in the aggregates
    rows, cut = _run(engine, f"{B1} {B2}", n, corpus_scope="genizah")
    assert len(rows) == n and not cut["capped"]
    rows, cut = _run(engine, f"{B1} {B2}", n - 1, corpus_scope="genizah")
    assert len(rows) == n - 1 and cut["capped"]


def test_exactly_the_limit_of_the_line_break_search_is_not_a_cut(engine):
    opts = {"responsa_mode": True}
    n = len(PAGES)
    rows, cut = _run(engine, f"{W} | {C}", n, corpus_scope="genizah", responsa_options=opts)
    assert len(rows) == n and not cut["capped"]
    rows, cut = _run(engine, f"{W} | {C}", n - 1, corpus_scope="genizah", responsa_options=opts)
    assert len(rows) == n - 1 and cut["capped"]


@pytest.mark.parametrize("mode", ["literal", "Regex"])
def test_exactly_the_limit_of_my_library_is_not_a_cut(engine, tmp_path, mode):
    n = 3
    local = _build(str(tmp_path / "local"), [(f"l{i}", f"אבג {W}", "page", "") for i in range(n)])
    saved = (getattr(engine, "local_index", None), getattr(engine, "local_searcher", None))
    engine.local_index, engine.local_searcher = local, local.searcher()
    try:
        rows, cut = _run(engine, W, n, mode, corpus_scope="local")
        assert len(rows) == n and not cut["capped"]
        rows, cut = _run(engine, W, n - 1, mode, corpus_scope="local")
        assert len(rows) == n - 1 and cut["capped"]
    finally:
        engine.local_index, engine.local_searcher = saved


def test_the_extra_hit_changes_no_candidate(tmp_path):
    # _top_hits keeps exactly what a search for `limit` hits kept, in the same order,
    # ties included (equal texts score equally).
    docs = [(f"t{i}", f"{W} " * (1 + i % 3) + "אבג", "page", "") for i in range(12)]
    idx = _build(str(tmp_path / "ties"), docs)
    searcher = idx.searcher()
    q = idx.parse_query(W, ["content"])

    def key(hits):
        return [(score, addr.segment_ord, addr.doc) for score, addr in hits]
    for limit in range(1, len(docs) + 2):
        hits, capped = se._top_hits(searcher, q, limit)
        assert key(hits) == key(searcher.search(q, limit).hits)
        assert capped is (limit < len(docs))


def test_ids_only_is_not_cut_off_when_every_doc_matches(tmp_path):
    # Its limit was the doc count: a query every doc matched filled it and read as cut off.
    root = tmp_path / "all_match"
    root.mkdir()
    _build(str(root / "tantivy_db"), [(f"m{n}", f"אבג {W}", "page", "") for n in range(3)])
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    se.consume_last_search_cutoff()
    with patch.object(Config, "SEARCH_LIMIT", 1):
        rows = eng.execute_search(W, "literal", 0, corpus_scope="genizah", ids_only=True)
        cutoff = se.consume_last_search_cutoff()
    assert len(rows) == 3 and not cutoff["capped"]
    eng = None
    gc.collect()
