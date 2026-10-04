"""execute_search(ids_only=True): the complete set of matches, without building rows (D8).

When search-within or the all-terms filter relies on a step whose list was cut off at
the 50,000-candidate limit, the desktop completes it: every candidate read, only page
ids and manuscripts kept. Its membership must be a normal search's, case by case --
whole words, Variants/Fuzzy, page-break crossings, the page dropped when its original
text does not hold the match, NOT-words decided by the winning copy of a page (V0.8
over V0.7) -- and it must not build what it skips (display metadata, snippets).
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

W, ON, BAD = "שלום", "עליכם", "רע"
NEAR = "שלוס"


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
    """Counts get_display_data: ids_only reads the manuscript from the header."""
    calls = 0

    def get_display_data(self, header, source):
        _Meta.calls += 1
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": self.parse_header_smart(header)[0], "library_code": ""}

    def parse_header_smart(self, header):
        return header.split()[-1], "1"

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


# uid, manuscript, text, source
PAGES = [
    ("whole", "991", f"אבג {W} {ON} דהו", "V0.8"),
    ("inside", "992", f"אבג ו{W}ים דהו", "V0.8"),               # only inside a longer word
    ("near", "993", f"אבג {NEAR} דהו", "V0.8"),
    ("bracket", "994", f"אבג ש[לום דהו", "V0.8"),               # the original text breaks the word
    ("bad", "995", f"אבג {W} {BAD}", "V0.8"),                  # V0.8 holds the NOT-word...
    ("bad", "995", f"אבג {W} טוב", "V0.7"),                    # ...its V0.7 copy does not
    ("plain", "996", f"אבג {W} זחט", "V0.8"),
    # Found for the phrase (W ... ON within the phrase query's slop), matched only once the
    # brackets are stripped (ש[לום ON): the original text holds no match, so no row.
    ("bracketed", "990", f"{W} אבג גדה {ON} ש[לום {ON}", "V0.8"),
    ("extra1", "997", f"{W} {W} {W}", "V0.8"),
    ("extra2", "998", f"{W} {W} {W}", "V0.8"),
]
CROSS = [("c1", "999", f"אבג {W}"), ("c2", "999", f"{ON} דהו")]


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("idsonly")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, sid, text, source, scope="page", bounds=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source=source,
            full_header=f"IE_{uid} {sid}", shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))

    for uid, sid, text, source in PAGES:
        add(uid, sid, text, source)
    for uid, sid, text in CROSS:
        add(uid, sid, text, "V0.8")
    text = CROSS[0][2] + "\n" + CROSS[1][2]
    cut = len(CROSS[0][2]) + 1
    add("sys:999", "999", text, "V0.8", "system", json.dumps([
        {"uid": "c1", "p_num": 1, "full_header": "IE_c1 999", "source": "V0.8", "sys_id": "999",
         "start": 0, "end": cut},
        {"uid": "c2", "p_num": 2, "full_header": "IE_c2 999", "source": "V0.8", "sys_id": "999",
         "start": cut, "end": len(text)}]))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    # Its own browse map: the real one would be loaded from disk into a class-level
    # cache and leak into later tests in the process.
    browse = {}
    for uid, sid, *_rest in PAGES + CROSS:
        browse.setdefault(sid, []).append({"uid": uid})
    eng._load_browse_map = lambda: browse
    yield eng
    eng = None
    gc.collect()


def _members(rows):
    return {(r["uid"], r["display"]["id"], r["display"]["source"]) for r in rows}


CASES = [
    (W, "literal", {}), (W, "variants", {}), (W, "fuzzy", {}),
    (f"{W} {ON}", "literal", {}), (f"{W} {ON}", "fuzzy", {}),
    (W, "literal", {"exclude_words": [BAD]}), (W, "fuzzy", {"exclude_words": [BAD]}),
    (W, "literal", {"text_position": "start"}),
    (W, "literal", {"restrict_sys_ids": {"991", "995", "999"}}),
    (f"{W} {ON}", "literal", {"responsa_options": {"responsa_mode": True}}),
]


@pytest.mark.parametrize("query, mode, kw", CASES)
def test_ids_only_holds_exactly_what_a_search_returns(engine, query, mode, kw):
    with patch.object(Config, "SEARCH_LIMIT", 1000):
        full = engine.execute_search(query, mode, 0, corpus_scope="genizah", **kw)
        ids = engine.execute_search(query, mode, 0, corpus_scope="genizah", ids_only=True, **kw)
    assert _members(ids) == _members(full)
    assert not any(k in r for r in ids for k in ("snippet", "full_text", "highlight_pattern", "_excluded"))


def test_the_cases_cover_what_they_claim(engine):
    with patch.object(Config, "SEARCH_LIMIT", 1000):
        got = {u for u, _s, _src in _members(engine.execute_search(W, "literal", 0, corpus_scope="genizah",
                                                                      ids_only=True))}
        no_bad = {u for u, _s, _src in _members(engine.execute_search(
            W, "literal", 0, corpus_scope="genizah", ids_only=True, exclude_words=[BAD]))}
        cross = engine.execute_search(f"{W} {ON}", "literal", 0, corpus_scope="genizah", ids_only=True)
    assert "inside" not in got and "bracket" not in got and {"whole", "bad", "plain"} <= got
    assert "bad" not in no_bad, "the winning V0.8 copy holds the NOT-word"
    assert "c1" in {r["uid"] for r in cross}, "the crossing comes from the whole-manuscript doc"
    assert "bracketed" not in {r["uid"] for r in cross}


def test_ids_only_reads_every_candidate(engine):
    with patch.object(Config, "SEARCH_LIMIT", 2):
        capped = engine.execute_search(W, "literal", 0, corpus_scope="genizah")
        complete = engine.execute_search(W, "literal", 0, corpus_scope="genizah", ids_only=True)
        assert not se.consume_last_search_cutoff()["capped"]
    assert len(capped) < len(complete) and {"extra1", "extra2", "plain"} <= {r["uid"] for r in complete}


def test_ids_only_builds_no_display_rows(engine):
    _Meta.calls = 0
    with patch.object(Config, "SEARCH_LIMIT", 1000), \
            patch.object(se.SearchEngine, "_highlight_pair", side_effect=AssertionError("a snippet")):
        rows = engine.execute_search(f"{W} {ON}", "literal", 0, corpus_scope="genizah", ids_only=True)
        rows += engine.execute_search(W, "fuzzy", 0, corpus_scope="genizah", ids_only=True)
    assert rows and _Meta.calls == 0


def test_ids_only_offers_no_preview(engine):
    calls = []
    with patch.object(se, "_PREVIEW_AFTER_S", 0.0):
        engine.execute_search(W, "literal", 0, corpus_scope="genizah", ids_only=True,
                              preview_callback=calls.append)
    assert calls == []


# --- Completing a cut-off chain with the real engine (D8 commit 6/7) -------------------

def _chain():
    from shared.refinement import RefinementStep
    steps = [RefinementStep(W, "literal", corpus_scope="genizah", result_count_capped=True),
             RefinementStep(ON, "literal", corpus_scope="genizah", result_count_capped=True)]
    steps[0]._result_sys_ids, steps[1]._result_sys_ids = {"991"}, {"991"}
    return steps


def test_complete_chain_reads_every_match_from_the_engine(engine):
    from shared.refinement import complete_chain
    with patch.object(Config, "SEARCH_LIMIT", 1000):
        first = {r["display"]["id"] for r in engine.execute_search(W, "literal", 0, corpus_scope="genizah")}
        within = {r["display"]["id"] for r in engine.execute_search(
            ON, "literal", 0, corpus_scope="genizah", restrict_sys_ids=first)}
    chain = _chain()
    with patch.object(Config, "SEARCH_LIMIT", 2):          # the display's cut-off is tiny
        result = complete_chain(chain, engine, None)
    assert len(first) > 2 and chain[0]._result_sys_ids == first
    assert result == {"restrict": within, "interrupted": False}
    assert [s.result_count_capped for s in chain] == [False, False]


def test_the_completion_thread_hands_back_the_completed_set(engine):
    from desktop.gui_threads import ChainCompletionThread
    chain, got = _chain(), []
    t = ChainCompletionThread(chain, engine, None, upto=1)
    t.finished_signal.connect(got.append)
    with patch.object(Config, "SEARCH_LIMIT", 2):
        t.run()                                 # same thread: delivered directly
    assert got == [{"restrict": chain[0]._result_sys_ids, "interrupted": False}]
    assert chain[1]._result_sys_ids == {"991"}, "upto=1 leaves the shown step alone"


def test_a_stopped_completion_leaves_the_chain_as_it_was(engine):
    from desktop.gui_threads import ChainCompletionThread
    chain, got = _chain(), []
    t = ChainCompletionThread(chain, engine, None)
    t.finished_signal.connect(got.append)
    t.request_cancel()
    with patch.object(se, "_PROGRESS_TICK_EVERY", 1):
        t.run()
    assert got == [{"restrict": None, "interrupted": True}]
    assert [(s._result_sys_ids, s.result_count_capped) for s in chain] == [({"991"}, True)] * 2
