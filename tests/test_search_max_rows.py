"""execute_search(max_rows=N): the first N rows built, every other match only counted.

The API shows at most `limit` rows (web/search_api.py), and a Responsa search for a
common word built all 50,000 of them first (docs/open_issues.d/2026-11-24-responsa-
common-word-searches-slow.md). With max_rows the engine builds full rows until the
first N rows of the final list are settled, checks the rest as ids_only checks them,
and puts the whole list's length on the first row (`result_total`).

The contract tested here, on a small real index with V0.8 pages, V0.7 copies of some
of them (some scoring above their V0.8 copy), V0.7-only pages, pages a NOT-word
excludes, and a whole-manuscript document with a match across a page break:
- the rows are exactly the whole list's first N, and `result_total` its length;
- the rows past N are not built (no display metadata, no snippet);
- where the order is not settled as rows are found (an all-star Responsa query,
  ids_only), max_rows is ignored: every row, no `result_total`.
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

W = "שלום"     # the word searched
F = "עוד"      # filler
X = "גלות"     # the excluded word
N_V8 = 40


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
    """Display metadata, counted: a row the engine builds asks for it once."""

    def __init__(self):
        self.display_calls = 0

    def get_display_data(self, header, source):
        self.display_calls += 1
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}

    def parse_header_smart(self, header):
        return header, None


def _build(root):
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, source, scope="page", boundaries=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source=source, full_header=uid, shelfmark=uid, scope=scope, boundaries=boundaries,
            **Indexer._extract_position_fields(text)))

    for n in range(N_V8):
        # Different lengths give different scores, so hit order is not doc order; every
        # seventh page holds the excluded word; brackets in some, so the bracket paths run.
        words = [W] + [F] * (n % 11) + ([X] if n % 7 == 3 else [])
        if n % 5 == 2:
            words.insert(1, f"[{F}]")
        add(f"u{n:03d}", " ".join(words), "V0.8")
    for n in range(0, 20, 3):
        # V0.7 copies: the short ones score above their V0.8 copy and come first.
        add(f"u{n:03d}", W if n % 2 else f"{W} " + " ".join([F] * 12), "V0.7")
    add("v7only-a", f"{W} {F}", "V0.7")                   # V0.7-only pages: the list's end
    add("v7only-b", f"{F} {W} {X}", "V0.7")               # ... one of them excluded
    add("u039", f"{F} {F}", "V0.7")                       # a V0.7 copy with no match
    # A whole manuscript whose two pages hold "W | F" across their break: a phrase
    # finds it only through this document.
    p1, p2 = f"{F} {F} {W}", f"{F} {F}"
    text = p1 + "\n" + p2
    add("sys:990009", text, "V0.8", scope="system", boundaries=json.dumps([
        {"uid": "brk-1", "p_num": 1, "full_header": "brk-1", "source": "V0.8", "sys_id": "990009",
         "start": 0, "end": len(p1)},
        {"uid": "brk-2", "p_num": 2, "full_header": "brk-2", "source": "V0.8", "sys_id": "990009",
         "start": len(p1) + 1, "end": len(text)}]))
    w.commit()
    w.wait_merging_threads()


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("max_rows")
    _build(root)
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    del eng
    gc.collect()


RESPONSA = {"responsa_mode": True, "variants": False, "ja": False, "flex_spacing": False,
            "bidirectional": False, "variant_mode": "exact"}

# (query, mode, keyword arguments): the searches the API runs.
SEARCHES = {
    "exact": (W, "literal", {}),
    "exact-not-word": (W, "literal", {"exclude_words": [X]}),
    "phrase-across-a-break": (f"{W} {F}", "literal", {}),
    "variants": (W, "variants", {}),
    "fuzzy": (W, "fuzzy", {}),
    "responsa": (W, "responsa", {"responsa_options": RESPONSA}),
    "responsa-minus-word": (f"{W} -{X}", "responsa", {"responsa_options": RESPONSA}),
    "position": (W, "variants", {"text_position": "start"}),
    "all-corpora": (W, "literal", {"corpus_scope": "all"}),
}
_ROW_MARKERS = ("result_total", "responsa_warning", "responsa_expanded_count", "responsa_pattern_count")


def _plain(rows):
    return [{k: v for k, v in r.items() if k not in _ROW_MARKERS} for r in rows]


def _search(engine, name, **extra):
    query, mode, kw = SEARCHES[name]
    kw = dict(kw)
    kw.setdefault("corpus_scope", "genizah")
    return engine.execute_search(query, mode, 0, **kw, **extra)


@pytest.mark.parametrize("name", list(SEARCHES))
def test_the_rows_are_the_whole_lists_first_and_the_total_its_length(engine, name):
    whole = _search(engine, name)
    assert len(whole) >= 3, f"{name}: the index must give this search several rows"
    assert "result_total" not in whole[0], "without max_rows nothing is counted apart"
    for n in sorted({1, 2, len(whole) // 2, len(whole) - 1, len(whole), len(whole) + 7}):
        part = _search(engine, name, max_rows=n)
        assert _plain(part) == _plain(whole[:n]), f"{name}, max_rows={n}: not the whole list's first rows"
        assert part[0]["result_total"] == len(whole), f"{name}, max_rows={n}: the count"


def test_the_list_holds_what_the_contract_is_about(engine):
    """The fixture gives the cases above their point: V0.7 rows at the end of the list, an
    excluded page, a row found only across a page break."""
    exact = [r["uid"] for r in _search(engine, "exact")]
    assert exact[-2:] == ["v7only-a", "v7only-b"]
    assert "u003" in exact and "u003" not in [r["uid"] for r in _search(engine, "exact-not-word")]
    assert "brk-1" in [r["uid"] for r in _search(engine, "phrase-across-a-break")]


@pytest.mark.parametrize("name", ["exact", "responsa", "variants"])
def test_the_rows_past_max_rows_are_not_built(engine, name):
    meta = engine.meta_mgr
    meta.display_calls = 0
    whole = _search(engine, name)
    built_whole = meta.display_calls
    assert built_whole >= len(whole)
    meta.display_calls = 0
    with patch.object(se.SearchEngine, "_highlight_pair", autospec=True,
                      side_effect=se.SearchEngine._highlight_pair) as highlight:
        part = _search(engine, name, max_rows=2)
    assert len(part) == 2 and part[0]["result_total"] == len(whole)
    # Two rows shown; a V0.7 copy found before the second V0.8 row may be built as well.
    assert meta.display_calls <= 4, f"{name}: {meta.display_calls} rows built for 2 shown"
    assert highlight.call_count <= 4


def test_an_all_star_query_builds_every_row_and_counts_nothing_apart(engine):
    # Its rows are ordered by their matches once all are found (decision 2): nothing
    # is settled while the search runs.
    whole = engine.execute_search("של*", "responsa", 0, responsa_options=RESPONSA,
                                  corpus_scope="genizah")
    assert len(whole) >= 3
    part = engine.execute_search("של*", "responsa", 0, responsa_options=RESPONSA,
                                 corpus_scope="genizah", max_rows=2)
    assert _plain(part) == _plain(whole)
    assert "result_total" not in part[0]


def test_ids_only_ignores_max_rows(engine):
    whole = _search(engine, "exact", ids_only=True)
    assert _search(engine, "exact", ids_only=True, max_rows=2) == whole


def test_unstripped_span_walks_the_brackets_as_the_letter_walk_did():
    """The fast mapping (brackets, not letters) gives the old answer on every input."""
    import random

    def letter_walk(text, start, end):
        kept, s0, e0 = 0, None, None
        for i, ch in enumerate(text):
            if ch in '[]':
                continue
            if kept == start:
                s0 = i
            kept += 1
            if kept == end:
                e0 = i + 1
                break
        s0 = len(text) if s0 is None else s0
        e0 = len(text) if e0 is None else e0
        seg = text[s0:e0]
        if seg.count('[') > seg.count(']') and text[e0:e0 + 1] == ']':
            e0 += 1
        if seg.count(']') > seg.count('[') and s0 and text[s0 - 1] == '[':
            s0 -= 1
        return s0, e0

    rnd = random.Random(20261125)
    for _ in range(20000):
        text = ''.join(rnd.choice('אב[] ') for _ in range(rnd.randint(0, 14)))
        kept = len(se._strip_brackets(text))
        start, end = rnd.randint(-1, kept + 2), rnd.randint(-1, kept + 3)
        assert se._unstripped_span(text, start, end) == letter_walk(text, start, end), (text, start, end)
