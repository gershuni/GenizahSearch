"""A row whose accepted occurrence holds a bracket marks that occurrence (Codex review of PR #375).

Membership is decided on the text with its brackets out; the snippet then looks for the
match again in the original text, where של[ו]ם is not שלום. When the original's only
occurrences were ones the whole-word rule rejects (inside בשלום), the row fell back to
the first of them and marked it. Now the accepted occurrence is mapped back onto the
original text, brackets included.
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
    # The embedded pair comes first; the whole-word one needs its bracket out.
    "rejected_then_bracketed": f"{W} אבג ב{W} {ON} דהו של[ו]ם {ON} זחט",
    "plain": f"אבג {W} {ON} דהו",
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
                "id": header.split()[-1], "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("bracket_hl")
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


def test_the_snippet_marks_the_bracketed_occurrence_not_the_rejected_one(engine):
    rows = {r["uid"]: r for r in engine.execute_search(f"{W} {ON}", "variants", 0, corpus_scope="genizah")}
    row = rows["rejected_then_bracketed"]
    for text in (row["snippet"], row["raw_file_hl"]):
        assert f"*של[ו]ם {ON}*" in text and f"ב*{W}" not in text, text


def test_an_original_whole_word_occurrence_is_still_the_one_marked(engine):
    rows = {r["uid"]: r for r in engine.execute_search(f"{W} {ON}", "variants", 0, corpus_scope="genizah")}
    assert f"אבג *{W} {ON}*" in rows["plain"]["snippet"]


@pytest.mark.parametrize("text, start, end, want", [
    ("אב[ג]ד", 1, 3, (1, 5)),        # ב[ג]: the pair kept whole
    ("א[בג]ד", 0, 2, (0, 3)),        # א[ב: a pair the match splits stays split (no ג)
    ("[א]בג", 0, 1, (1, 2)),         # a leading bracket stays outside the first letter
    ("אבג", 0, 3, (0, 3)),           # no brackets: the same offsets
    ("ש[ל]ום עולם", 0, 4, (0, 6)),
    ("א[בג]ד", 2, 4, (3, 6)),        # ג]ד: likewise (no ב)
    ("[אב]ג", 0, 3, (0, 5)),         # [אב]ג: the pair it closes, opened right at its edge
])
def test_unstripped_span(text, start, end, want):
    assert se._unstripped_span(text, start, end) == want
    # The same letters, never one outside the match.
    assert se._strip_brackets(text[want[0]:want[1]]) == se._strip_brackets(text)[start:end]
