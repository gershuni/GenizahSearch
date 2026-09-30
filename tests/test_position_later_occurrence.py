"""Position search accepts a page whose FIRST occurrence misses the position but a later one meets it.

End-to-end through ``SearchEngine.execute_search`` on a tiny main-schema index.
Before 2026-09-30 only the first regex occurrence was validated, so e.g. a page
"שלום עליכם ואמר שלום" was dropped by an end-of-text search for שלום. The
snippet must also mark the occurrence that met the position, not the first one.
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

W = "שלום"            # שלום
A = "עליכם"      # עליכם
B = "ואמר"            # ואמר
C = "ברכה"            # ברכה

PAGES = {
    "end_later": f"{W} {A} {B} {W}",               # first at start, last at end
    "line_end_later": f"{W} {A}\n{C} {W}",         # line 1: mid/start, line 2: ends line
    "line_start_later": f"{A} {W}\n{W} {C}",       # line 1: ends line, line 2: starts line
    "nowhere": f"{A} {W} {C}\n{B} {W} {A}",        # never at start/end of text or of a line
    # Brackets make the engine match on bracket-stripped text and re-search the
    # original for the highlight -- that re-search must also pick the valid one.
    "end_later_bracketed": f"{W} {A} [ו]אמר {W}",  # שלום עליכם [ו]אמר שלום
    # Line-break search (Responsa "W | C": W on a line, C on the next), position=end.
    "lb_end_later": f"{W}\n{C} {A}\n{B}\n{W}\n{C}",  # first W|C mid-text, the last one ends it
    "lb_end_never": f"{W}\n{C} {A}\n{B} {A}",          # W|C only mid-text
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
def results_by_position(tmp_path_factory):
    root = tmp_path_factory.mktemp("posidx")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    writer = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text in PAGES.items():
        writer.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=uid, shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))
    writer.commit()
    writer.wait_merging_threads()
    del writer, idx

    out = {}
    with patch.object(Config, "INDEX_DIR", str(root)):
        engine = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
        for pos in ("start", "end", "line_start", "line_end"):
            rows = engine.execute_search(W, "literal", 0, text_position=pos, corpus_scope="genizah")
            out[pos] = {r["uid"]: r["snippet"] for r in rows}
        rows = engine.execute_search(f"{W} | {C}", "literal", 0, text_position="end",
                                     responsa_options={"responsa_mode": True}, corpus_scope="genizah")
        out["line_break_end"] = {r["uid"]: r["snippet"] for r in rows}
    del engine
    gc.collect()
    return out


def test_end_accepts_a_later_occurrence(results_by_position):
    rows = results_by_position["end"]
    assert "end_later" in rows
    assert rows["end_later"].rstrip().endswith(f"*{W}*")  # the LAST occurrence is marked


def test_end_highlight_on_bracketed_page_marks_the_valid_occurrence(results_by_position):
    rows = results_by_position["end"]
    assert "end_later_bracketed" in rows
    snippet = rows["end_later_bracketed"].rstrip()
    assert snippet.endswith(f"*{W}*")
    assert not snippet.startswith(f"*{W}*")


def test_line_end_accepts_a_later_occurrence(results_by_position):
    rows = results_by_position["line_end"]
    assert "line_end_later" in rows
    assert f"{C} *{W}*" in rows["line_end_later"]


def test_line_start_accepts_a_later_occurrence(results_by_position):
    rows = results_by_position["line_start"]
    assert "line_start_later" in rows
    assert f"*{W}* {C}" in rows["line_start_later"]


def test_start_unchanged(results_by_position):
    rows = results_by_position["start"]
    assert {"end_later", "line_end_later"} <= set(rows)
    assert "line_start_later" not in rows and "nowhere" not in rows


def test_line_break_end_accepts_a_later_occurrence(results_by_position):
    rows = results_by_position["line_break_end"]
    assert "lb_end_later" in rows
    assert "lb_end_never" not in rows


def test_no_position_valid_occurrence_still_rejected(results_by_position):
    for pos in ("start", "end", "line_start", "line_end"):
        assert "nowhere" not in results_by_position[pos], pos
