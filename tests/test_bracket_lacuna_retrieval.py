"""A bare query reaches a word written between two lacuna brackets (``]word[``).

End-to-end through ``SearchEngine.execute_search`` on a tiny index with the main
schema and the real ``hebword`` tokenizer. The tokenizer keeps ``]שלום[`` as one
token, so the page is reachable only if the Tantivy query carries that form
(``_add_bracket_variants``). Before 2026-09-30 it did not: 10 V0.8 pages for
שלום were unreachable on the real index.

A bracket INSIDE the word (``ש[לום``) is still out of reach; that page is
pinned as not-returned so the gap stays visible until it is fixed (plan
docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md, stage 0b/0a).
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

WORD = "שלום"  # שלום

PAGES = [
    ("plain", f"{WORD} עליכם"),                 # שלום עליכם
    ("lacunae", f"ישראל ]{WORD}[ רב"),  # ישראל ]שלום[ רב
    ("opening", f"[{WORD} רב"),                                 # [שלום רב (already reached)
    ("inside", "ש[לום רב"),                  # ש[לום רב (not reached yet)
]


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


@pytest.fixture
def uids_for_bare_word(tmp_path):
    db = os.path.join(str(tmp_path), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    writer = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text in PAGES:
        writer.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=uid, shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))
    writer.commit()
    writer.wait_merging_threads()
    del writer, idx

    with patch.object(Config, "INDEX_DIR", str(tmp_path)):
        engine = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
        results = engine.execute_search(WORD, "literal", 0, corpus_scope="genizah")
        uids = {r["uid"] for r in results}
    # Release the memory-mapped index before tmp_path cleanup (Windows).
    del engine, results
    gc.collect()
    return uids


def test_word_between_two_lacunae_is_found(uids_for_bare_word):
    assert "lacunae" in uids_for_bare_word


def test_plain_and_single_bracket_forms_still_found(uids_for_bare_word):
    assert {"plain", "opening"} <= uids_for_bare_word


def test_bracket_inside_word_is_a_known_gap(uids_for_bare_word):
    # Pinned so the gap is visible; flip this when stage 0a/0b reaches it.
    assert "inside" not in uids_for_bare_word
