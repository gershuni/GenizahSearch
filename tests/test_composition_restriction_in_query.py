"""Composition (Parallels) within manuscripts: the restriction is in the query at any size.

Each chunk of the source text takes its 50 best hits. Above 500 manuscripts the
restriction was left out of the query (an OR of phrases was too slow) and the 50 hits
were filtered afterwards, so pages of the chosen manuscripts that ranked after 50 pages
of other manuscripts were never seen -- with a narrow filter, almost all of them
(website Parallels page and /api/parallels; desktop Composition passes no restriction).
Now the term-set restriction main search uses (D8 commit 1), on page docs only:
Composition keeps nothing else, and a whole-manuscript doc or an Oxford part naming a
chosen manuscript would take one of the 50 slots and be dropped afterwards.
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

PASSAGE = "ברוך אתה יי אלהינו מלך העולם"
OUTSIDE, INSIDE, AGGREGATES = 60, 2, 60   # each kind alone fills 50 slots


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


def _inside_sid(n):
    return f"9911{n:04d}"


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("comp_restrict")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    text = f"אבג {PASSAGE} דהו"

    def add(uid, sid, scope="page"):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} {sid}", shelfmark=uid, scope=scope, boundaries="",
            **Indexer._extract_position_fields(text)))
    # Docs added first rank first among equal scores: the outside pages, then Oxford
    # parts and whole-manuscript docs whose headers name an inside manuscript, come
    # before the inside pages.
    for n in range(OUTSIDE):
        add(f"out{n}", f"9922{n:04d}")
    for n in range(AGGREGATES):
        add(f"part:{n}", _inside_sid(0), scope="part")
        add(f"sys:{n}", _inside_sid(1), scope="system")
    for n in range(INSIDE):
        add(f"in{n}", _inside_sid(n))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    browse = {_inside_sid(n): [{"uid": f"in{n}"}] for n in range(INSIDE)}
    eng._load_browse_map = lambda: browse            # never the class-level real map
    yield eng
    eng = None
    gc.collect()


def _found(engine, restrict):
    res = engine.search_composition_logic(PASSAGE, 3, 10 ** 9, "literal", restrict_sys_ids=restrict,
                                          corpus_scope="genizah")
    return {i["uid"] for i in res["main"] + res["filtered"]}


@pytest.mark.parametrize("padding", [0, 600])          # at most 500 ids, and above
def test_composition_finds_the_chosen_manuscripts_pages_at_any_size(engine, padding):
    restrict = {_inside_sid(n) for n in range(INSIDE)} | {f"9933{n:04d}" for n in range(padding)}
    assert _found(engine, restrict) == {f"in{n}" for n in range(INSIDE)}


def test_unrestricted_the_fifty_slots_go_to_the_first_pages(engine):
    # What the old filter-afterwards path saw: no insider among a chunk's 50 hits.
    found = _found(engine, None)
    assert found and not (found & {f"in{n}" for n in range(INSIDE)})
