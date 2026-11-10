"""AND / OR / NOT in query text are words, never bare Tantivy operators.

``build_tantivy_query`` passed a term spelled AND, OR or NOT (any case) or a lone
parenthesis into the query bare. Its parts are joined with AND, so the result was
never an operator, only a parse error ("x AND OR AND y", a trailing OR, AND AND):
a Composition / Parallels chunk holding Latin text with one of them (a header, a
shelfmark, apparatus) was dropped with a ``[LAB]`` warning while the search reported
success, and a plain Exact search for it returned nothing. Each keyword is now quoted
like any other term, which is what the verification regex already required.

Tiny throwaway indexes with the real ``hebword`` tokenizer; no corpus needed.
"""
import gc
import logging
import os
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.local_indexer import build_local_schema  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
import shared.search_engine as se  # noqa: E402

KEYWORDS = ["AND", "OR", "NOT", "and", "or", "not", "Or", "(", ")"]
UPPER = ["AND", "OR", "NOT"]
A, B = "אבג", "דהו"


def _stub_engine():
    """What build_tantivy_query touches: a VariantManager giving the word itself."""

    class _Var:
        def get_variants(self, term, mode, limit=200):
            return [term]

    return SimpleNamespace(var_mgr=_Var(), _has_content_search=True)


@pytest.fixture(scope="module")
def small_index(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("bool_kw_parse"))
    idx = tantivy.Index(build_local_schema(), path=path)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=15_000_000)
    docs = {
        "or_doc": f"Ms 12 OR 13 {A}",
        "and_not_doc": f"T-S AND NOT {B}",
        "plain_doc": f"Ms 12 13 {A}",
    }
    for uid, text in docs.items():
        w.add_document(tantivy.Document(
            unique_id=[uid], content=[text], content_search=[strip_search_diacritics(text)],
            **{k: [v] for k, v in Indexer._extract_position_fields(text).items()}))
    w.commit()
    idx.reload()
    return idx


def _hits(idx, query_str):
    s = idx.searcher()
    return {s.doc(a)["unique_id"][0] for _sc, a in s.search(idx.parse_query(query_str, ["content"]), 50).hits}


@pytest.mark.parametrize("mode", ["exact", "variants", "fuzzy"])
@pytest.mark.parametrize("cs_field", [None, "content_search"])
@pytest.mark.parametrize("kw", KEYWORDS)
@pytest.mark.parametrize("shape", ["middle", "trailing", "leading", "doubled", "alone"])
def test_every_keyword_builds_a_query_that_parses(small_index, kw, mode, cs_field, shape):
    terms = {"middle": [A, kw, B], "trailing": [A, kw], "leading": [kw, B],
             "doubled": [A, kw, kw, B], "alone": [kw]}[shape]
    q = se.SearchEngine.build_tantivy_query(_stub_engine(), terms, mode, content_search_field=cs_field)
    small_index.parse_query(q, ["content"])          # raised "Syntax Error" before
    assert f'"{kw}"' in q, q                         # quoted as a word


@pytest.mark.parametrize("kw", UPPER)
def test_an_uppercase_keyword_is_never_left_bare(kw):
    q = se.SearchEngine.build_tantivy_query(_stub_engine(), [A, kw, B], "exact")
    assert f" AND {kw} AND " not in q, q
    assert f'("{kw}"^5' in q, q


def test_the_keyword_is_a_required_word_not_an_operator(small_index):
    stub = _stub_engine()
    q = se.SearchEngine.build_tantivy_query(stub, ["12", "OR", "13"], "exact")
    assert _hits(small_index, q) == {"or_doc"}       # not "12 or 13": OR itself is required
    q = se.SearchEngine.build_tantivy_query(stub, ["AND", "NOT"], "exact")
    assert _hits(small_index, q) == {"and_not_doc"}


# --- Through the engine: Composition (= /api/parallels) and a plain Exact search ---

PAGES = {f"p_{kw}": f"זחט {A} {kw} {B} טיכ" for kw in ("AND", "OR", "NOT", "or")}


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
    root = tmp_path_factory.mktemp("bool_kw_engine")
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


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@contextmanager
def _warnings():
    """Every WARNING the search engine's two loggers emit (attached directly:
    genizah_core may turn their propagation off)."""
    handler = _Records()
    loggers = (se.LAB_LOGGER, se.LOGGER)
    levels = [lg.level for lg in loggers]
    for lg in loggers:
        lg.addHandler(handler)
        lg.setLevel(logging.DEBUG)
    try:
        yield handler.records
    finally:
        for lg, level in zip(loggers, levels):
            lg.removeHandler(handler)
            lg.setLevel(level)


@pytest.mark.parametrize("mode", ["literal", "variants"])
@pytest.mark.parametrize("kw", ["AND", "OR", "NOT", "or"])
def test_composition_keeps_a_chunk_holding_a_keyword(engine, kw, mode):
    with _warnings() as warned:
        res = engine.search_composition_logic(f"{A} {kw} {B}", 3, 10 ** 9, mode, corpus_scope="genizah")
    assert not [r.getMessage() for r in warned]      # the chunk was dropped with a [LAB] warning
    assert f"p_{kw}" in {i["uid"] for i in res["main"] + res["filtered"]}


@pytest.mark.parametrize("kw", ["AND", "OR", "NOT"])
def test_an_exact_search_for_a_keyword_finds_it(engine, kw):
    with _warnings() as warned:
        rows = engine.execute_search(f"{A} {kw} {B}", "exact", 0, corpus_scope="genizah")
    assert not [r.getMessage() for r in warned]      # "Search query failed to parse" before
    assert f"p_{kw}" in {r["uid"] for r in rows}
