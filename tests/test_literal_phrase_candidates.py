"""Multi-word Literal search retrieves candidates with adjacent-pair PHRASE queries.

Before 2026-09-30 the candidate query was AND-of-terms: every document holding the
words anywhere, whole manuscripts included; the regex then discarded most of them
(real index, אהרן כהן: 8,897 candidates, 252 verified, 8.7 s -> 0.7 s with phrases).
Owner decision 2026-09-30: Literal matches exact words, in phrases too, so a pair
found only inside a longer word (לאהרן כהן) is not a match.

The real-index gate for this change is the script described in
docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md (phrase path OFF vs ON, uncapped).
"""
import gc
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

AH, KO = "אהרן", "כהן"          # אהרן כהן
BR, AT, YY = "ברוך", "אתה", "יי"  # ברוך אתה יי
F1, F2, F3 = "הגדול", "הזה", "עוד"  # הגדול הזה עוד

PAGES = {
    "adjacent": f"{AH} {KO} {F1}",
    "lone_bracket": f"{F1} {AH} [\n{KO} {F2}",           # a lone lacuna bracket between the words
    # לאהרן כהן, and the bare words exist only FAR apart (beyond the slop in either order):
    # the AND query made it a candidate and the substring regex accepted it by accident.
    "inside_far": f"ל{AH} {KO} " + " ".join([F3] * 7) + f" {AH} " + " ".join([F1] * 7),
    # The same accident with the bare words NEAR each other stays a candidate; only the
    # whole-word verifier (plan stage 0a) removes it. Pinned as a known gap.
    "inside_near": f"ל{AH} {KO} {F3} {AH}",
    "gap_two": f"{AH} {F1} {F2} {KO}",
    "three": f"{BR} {AT} {YY} {F1}",
    "three_triple_yod": f"{BR} {AT} ייי " + " ".join([F1] * 7) + f" {YY}",  # ברוך אתה ייי ... יי
}


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


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("phraseidx")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text in PAGES.items():
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=uid, shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    patcher = patch.object(Config, "INDEX_DIR", str(root))
    patcher.start()
    eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    patcher.stop()
    eng = None
    gc.collect()


def _uids(engine, query, gap=0, **kw):
    return {r["uid"] for r in engine.execute_search(query, "literal", gap, corpus_scope="genizah", **kw)}


# --- the builder ---------------------------------------------------------------

def test_builder_declines_single_terms_and_non_token_terms():
    eng = se.SearchEngine.__new__(se.SearchEngine)
    assert eng.build_phrase_candidate_query([AH], 0) is None
    assert eng.build_phrase_candidate_query([AH, "a/b"], 0) is None
    assert eng.build_phrase_candidate_query([AH, KO + "*"], 0) is None


def test_builder_pairs_slop_and_bracket_forms():
    eng = se.SearchEngine.__new__(se.SearchEngine)
    q = eng.build_phrase_candidate_query([BR, AT, YY], 2, content_search_field="content_search")
    assert q.count(") AND (") == 1                       # two adjacent pairs
    assert f'content:"{BR} {AT}"~{2 + eng._PHRASE_EXTRA_SLOP}' in q
    assert f'content:"]{BR}[ [{AT}]"~' in q              # bracket forms on both sides
    assert f'content_search:"{AT} {YY}"~' in q
    assert "content_search" not in eng.build_phrase_candidate_query([BR, AT], 0)


# --- end to end ----------------------------------------------------------------

def test_adjacent_and_lone_bracket_found(engine):
    assert {"adjacent", "lone_bracket"} <= _uids(engine, f"{AH} {KO}")


def test_pair_only_inside_a_longer_word_is_not_a_match(engine):
    assert "inside_far" not in _uids(engine, f"{AH} {KO}")


def test_inside_word_near_bare_words_is_not_a_match(engine):
    # The pair phrase still returns the page (Tantivy's slop also allows reversed
    # order), but the only phrase on it is inside a longer word: the whole-word
    # check rejects it (2026-10-01; was a known gap pinned here).
    assert "inside_near" not in _uids(engine, f"{AH} {KO}")


def test_gap_is_respected(engine):
    assert "gap_two" not in _uids(engine, f"{AH} {KO}", gap=0)
    assert "gap_two" in _uids(engine, f"{AH} {KO}", gap=2)


def test_three_words_exact(engine):
    got = _uids(engine, f"{BR} {AT} {YY}")
    assert "three" in got
    assert "three_triple_yod" not in got                 # ייי is not יי (owner, 2026-09-30)


def test_phrase_path_not_used_for_variants_or_positions(engine):
    calls = []
    real = engine.build_phrase_candidate_query

    def spy(*a, **k):
        calls.append(a)
        return real(*a, **k)

    with patch.object(engine, "build_phrase_candidate_query", spy):
        engine.execute_search(f"{AH} {KO}", "variants", 0, corpus_scope="genizah")
        engine.execute_search(f"{AH} {KO}", "literal", 0, text_position="start", corpus_scope="genizah")
        assert calls == []
        engine.execute_search(f"{AH} {KO}", "literal", 0, corpus_scope="genizah")
        assert len(calls) == 1
