"""Composition (Parallels): a whole-manuscript / part row names the page its match starts on.

The index holds, beside each page, the manuscript's text whole (scope 'system', uid
``sys:<id>``) and each Oxford part's (scope 'part', uid ``part:<id>``); a composition
chunk that matches only there -- across a page break, or a page doc outside a chunk's
50 hits -- gives a row with that uid, which /api/browse refuses (400 locator_conflict),
and a raw_header naming the document's FIRST page. The row now also carries
``match_page_uid`` / ``match_page_header``: the page, from the document's boundaries,
where its first match starts -- None when the boundaries do not place it. Its uid and
raw_header stay as they were: the web page and the desktop tell these rows apart by the
uid (search_engine.group_pages_by_manuscript, retrieval_adapters.is_page_row).
shared/search_serializer.py gives a client that page.
"""
import gc
import json
import os
import re
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
SID = "990000000000205171"


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
                "id": SID, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


def _page_uid(ie, p):
    return f"IE{ie}_P{p:06d}_FL{ie}{p}"


def _whole(ie, pages):
    """(content, boundaries) of a document made of *pages*, as the indexer joins them."""
    content, bounds = "", []
    for p, text in enumerate(pages, 1):
        start = len(content)
        content += text + "\n"
        bounds.append({"uid": _page_uid(ie, p), "p_num": p, "source": "V0.8", "sys_id": SID,
                       "full_header": f"{SID}_{_page_uid(ie, p)}", "start": start, "end": len(content)})
    return content, bounds


# Page 1 is not the passage; it starts on page 2 and runs on to page 3.
CROSSING = ["אבג דהו זחט טיכ", "למנ ברוך אתה יי", "אלהינו מלך העולם סעפ"]
# The passage whole on page 2 and again on page 3: the first one is page 2's.
TWICE = ["אבג דהו זחט טיכ", f"למנ {PASSAGE}", f"{PASSAGE} סעפ"]
# The passage's end on page 2, its beginning on page 3: the first chunk matches on
# page 3, a later one on page 2 -- the first match in the text is page 2's.
REVERSED = ["אבג דהו זחט טיכ", "אלהינו מלך העולם סעפ", "למנ ברוך אתה יי"]
# Page 2 has the passage with letters in brackets (in a word of every chunk), page 3 has
# it plain: the bracket-free query finds page 2's first in the stripped text, while
# the original text's own first match is page 3's.
BRACKETED = ["אבג דהו זחט טיכ", "למנ ברוך אתה י[י] אלהינו מל[ך] העולם", f"{PASSAGE} סעפ"]


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("comp_match_page")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, content, scope, boundaries, header):
        w.add_document(tantivy.Document(
            unique_id=uid, content=content, content_search=strip_search_diacritics(content),
            source="V0.8", full_header=header, shelfmark=uid, scope=scope,
            boundaries=json.dumps(boundaries) if boundaries is not None else "",
            **Indexer._extract_position_fields(content)))

    for uid, ie, pages, scope in (("part:MS. Test 1/2", 11, CROSSING, "part"),
                                  ("sys:twice", 22, TWICE, "system"),
                                  ("sys:reversed", 55, REVERSED, "system"),
                                  ("sys:bracketed", 66, BRACKETED, "system")):
        content, bounds = _whole(ie, pages)
        add(uid, content, scope, bounds, bounds[0]["full_header"])
    # A whole-manuscript doc of an index without boundaries.
    content, bounds = _whole(33, TWICE)
    add("sys:no-boundaries", content, "system", None, bounds[0]["full_header"])
    # A page's own doc.
    add(_page_uid(44, 1), f"אבג {PASSAGE} דהו", "page", None, f"{SID}_{_page_uid(44, 1)}")
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


@pytest.fixture(scope="module")
def rows(engine):
    res = engine.search_composition_logic(PASSAGE, 3, 10 ** 9, "literal", corpus_scope="genizah")
    return {r["uid"]: r for r in res["main"] + res["filtered"]}


def test_a_match_across_a_page_break_names_the_page_it_starts_on(rows):
    row = rows["part:MS. Test 1/2"]
    assert row["match_page_uid"] == _page_uid(11, 2)
    assert row["match_page_header"] == f"{SID}_{_page_uid(11, 2)}"
    # uid and raw_header (the document's first page) are unchanged.
    assert row["raw_header"] == f"{SID}_{_page_uid(11, 1)}"


def test_of_several_matches_the_first_one_s_page(rows):
    row = rows["sys:twice"]
    assert row["match_page_uid"] == _page_uid(22, 2)
    assert row["match_page_header"] == f"{SID}_{_page_uid(22, 2)}"


def test_the_first_match_in_the_text_not_the_first_chunk_s(rows):
    row = rows["sys:reversed"]
    assert row["match_page_uid"] == _page_uid(55, 2)
    assert row["match_page_header"] == f"{SID}_{_page_uid(55, 2)}"


def test_a_bracketed_first_match_is_the_page_not_a_later_plain_one(rows):
    row = rows["sys:bracketed"]
    assert row["match_page_uid"] == _page_uid(66, 2)
    assert row["match_page_header"] == f"{SID}_{_page_uid(66, 2)}"
    # The row shows the occurrence it is placed by: every chunk's marked match is
    # page 2's, with its brackets, not page 3's plain one (Codex review of PR #125).
    marked = [re.search(r"\*([^*]+)\*", hit[3]).group(1) for hit in row["chunk_hits"]]
    assert marked and all("[" in m for m in marked), marked


def test_without_boundaries_no_page_is_named(rows):
    row = rows["sys:no-boundaries"]
    assert row["match_page_uid"] is None and row["match_page_header"] is None
    assert row["raw_header"] == f"{SID}_{_page_uid(33, 1)}"


def test_a_page_row_carries_no_match_page(rows):
    row = rows[_page_uid(44, 1)]
    assert "match_page_uid" not in row and "match_page_header" not in row


def test_the_export_snapshot_keeps_the_match_page(rows):
    """The website's parallels export compacts its rows before serializing them:
    the match page must survive, or the export names no page where the API does."""
    from web.export_state import compact_parallels_result_rows
    (kept,) = compact_parallels_result_rows([rows["part:MS. Test 1/2"]])
    assert kept["match_page_uid"] == _page_uid(11, 2)
    assert kept["match_page_header"] == f"{SID}_{_page_uid(11, 2)}"
