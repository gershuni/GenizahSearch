"""A Latin letter or a digit continues a word (Codex review of PR #375).

The whole-word rule of Exact, Variants and Fuzzy took only Hebrew letters as part
of a word, so `abc def` matched inside `xabc defz` and `שלום עולם` matched
`שלום2 עולם`. The hebword tokenizer -- what Exact's tokens are -- keeps letters and
digits of every script in one token; the rule now agrees with it.
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

W, ON = "שלום", "עולם"
PAGES = {
    # Each word also stands alone elsewhere, so the candidate query finds the page;
    # the only adjacent pair is inside longer tokens.
    "lat_in": "xabc defz אבג abc זחט def",
    "lat_whole": "אבג abc def דהו",
    "dig_in": f"אבג {W}2 {ON} זחט {W} דהו {ON}",
    "dig_whole": f"אבג {W} {ON} דהו",
}


class _Meta:
    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header.split()[-1], "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


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


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("latin_digits")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for n, (uid, text) in enumerate(PAGES.items()):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} 99{n}", shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


def _uids(engine, query, mode):
    return {r["uid"] for r in engine.execute_search(query, mode, 0, corpus_scope="genizah")}


@pytest.mark.parametrize("mode", ["literal", "variants"])
def test_a_latin_phrase_inside_longer_tokens_is_not_a_match(engine, mode):
    assert _uids(engine, "abc def", mode) & {"lat_in", "lat_whole"} == {"lat_whole"}


@pytest.mark.parametrize("mode", ["literal", "variants", "fuzzy"])
def test_a_word_with_a_digit_after_it_is_another_word(engine, mode):
    assert _uids(engine, f"{W} {ON}", mode) & {"dig_in", "dig_whole"} == {"dig_whole"}


def test_the_boundary_rule_on_its_own():
    assert not se._whole_word_span("xabc", 1, 4) and not se._whole_word_span("abc1", 0, 3)
    assert se._whole_word_span("(abc)", 1, 4) and se._whole_word_span(f"{W}־{ON}", 0, len(W))
