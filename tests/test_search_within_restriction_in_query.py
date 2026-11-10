"""Search-within applies its manuscript restriction INSIDE the query, at any size.

Above 500 manuscripts the restriction used to be left out of the query: the search
ran over the whole corpus, kept its first Config.SEARCH_LIMIT hits by score, and only
then dropped the hits outside the restriction -- so a match in a restricted
manuscript that did not make that corpus-wide cut was lost (real index, ישראל then
משה within it: about three quarters of the manuscripts). Owner decision D8
(2026-10-04): combinations must not lose results to the cap.

Here SEARCH_LIMIT is 3 and three louder pages outside the restriction outrank the
target, so only an in-query restriction can reach it. Also: an Oxford part's match
across its own page break counts when EITHER page is inside the restriction.
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

W, C = "שלום", "ברכה"
NEAR = "שלוס"                                   # a near spelling of W (Fuzzy)
B1, B2 = "ברוך", "הבא"
TARGET = "990001"                               # the restricted manuscript
MANY = {f"99{n:07d}" for n in range(1000, 1501)}  # 501 unrelated ids: above the old 500 line


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


# Loud pages (the word many times: higher scores) in three other manuscripts.
LOUD = [(f"loud{n}", f"99000{n + 5}", " ".join([W] * 12) + f"\n{C}") for n in range(3)]
PAGES = LOUD + [
    ("target", TARGET, f"אבג {W} דהו\n{C} זחט"),      # W, and W | C across a line break
    ("near", TARGET, f"אבג {NEAR} דהו"),               # only a near spelling
    ("upper", "ABC1", f"אבג {W} דהו"),                 # an id the tokenizer lowercases
    ("odd", "99-12", f"אבג {W} דהו"),                  # an id the tokenizer splits
]
# An Oxford part spanning two manuscripts: B1 B2 runs across its page break, from
# 990002's page into 990003's. Its own header names only the first.
PART = [("pa", "990002", f"אבג {B1}"), ("pb", "990003", f"{B2} דהו")]
BROWSE_MAP = {}
for uid, sid, _t in PAGES + PART:
    BROWSE_MAP.setdefault(sid, []).append({"uid": uid})


def _aggregate(pages):
    text, bounds, cursor = [], [], 0
    for i, (uid, sid, t) in enumerate(pages):
        start = cursor
        text.append(t)
        cursor += len(t)
        if i != len(pages) - 1:
            text.append("\n")
            cursor += 1
        bounds.append({"uid": uid, "p_num": i + 1, "full_header": f"IE_{uid} {sid}",
                       "source": "V0.8", "sys_id": sid, "start": start, "end": cursor})
    return "".join(text), json.dumps(bounds, ensure_ascii=False)


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("restrict")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, header, scope, bounds=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=header, shelfmark=header, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))

    for uid, sid, text in PAGES + PART:
        add(uid, text, f"IE_{uid} {sid}", "page")
    text, bounds = _aggregate(PART)
    add("part:MS. Heb. y. 1/1", text, f"IE_pa {PART[0][1]}", "part", bounds)
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    eng._load_browse_map = lambda: BROWSE_MAP
    yield eng
    eng = None
    gc.collect()


def _uids(engine, query, restrict, mode="literal", **kw):
    with patch.object(Config, "SEARCH_LIMIT", 3):
        rows = engine.execute_search(query, mode, 0, restrict_sys_ids=restrict,
                                     corpus_scope="genizah", **kw)
    return {r["uid"] for r in rows}


@pytest.mark.parametrize("restrict", [{TARGET}, {TARGET} | MANY], ids=["few", "over 500"])
@pytest.mark.parametrize("mode", ["literal", "variants", "fuzzy"])
def test_a_restricted_match_outside_the_corpus_wide_top_hits_is_found(engine, mode, restrict):
    # Unrestricted, the three loud pages fill the limit and the target is not among them.
    with patch.object(Config, "SEARCH_LIMIT", 3):
        assert "target" not in {r["uid"] for r in engine.execute_search(W, mode, 0, corpus_scope="genizah")}
    got = _uids(engine, W, restrict, mode)
    assert "target" in got and not got & {"loud0", "loud1", "loud2"}
    if mode == "fuzzy":
        assert "near" in got


@pytest.mark.parametrize("restrict", [{TARGET}, {TARGET} | MANY], ids=["few", "over 500"])
def test_a_line_break_search_within_finds_a_restricted_match(engine, restrict):
    got = _uids(engine, f"{W} | {C}", restrict, responsa_options={"responsa_mode": True})
    assert "target" in got


@pytest.mark.parametrize("sid, uid", [("ABC1", "upper"), ("99-12", "odd")])
def test_manuscript_ids_of_any_shape_restrict_as_their_header_tokens(engine, sid, uid):
    assert uid in _uids(engine, W, {sid} | MANY)


@pytest.mark.parametrize("responsa", [False, True], ids=["crossing path", "aggregate path"])
@pytest.mark.parametrize("inside", ["990002", "990003"])
def test_a_part_crossing_counts_when_either_page_is_inside(engine, responsa, inside):
    # The match starts on 990002's page and ends on 990003's. It used to count only
    # when the FIRST page was inside (the crossing path and _first_match_in_pages
    # both tested the primary page alone).
    kw = {"responsa_options": {"responsa_mode": True}} if responsa else {}
    with patch.object(Config, "SEARCH_LIMIT", 50):
        rows = engine.execute_search(f"{B1} {B2}", "literal", 0, restrict_sys_ids={inside},
                                     corpus_scope="genizah", **kw)
    assert any(r.get("cross_page") for r in rows), rows


def test_outside_the_restriction_nothing_is_found(engine):
    assert not _uids(engine, W, {"990099"} | MANY)
    with patch.object(Config, "SEARCH_LIMIT", 50):
        assert not engine.execute_search(f"{B1} {B2}", "literal", 0, restrict_sys_ids={"990099"},
                                         corpus_scope="genizah")


def test_a_part_page_missing_from_a_stale_browse_map_still_counts(engine, monkeypatch):
    # The browse map is a cached file (repaired at startup when stale); the part's own
    # page list carries each page's manuscript, so the restriction does not depend on it.
    monkeypatch.setattr(engine, "_load_browse_map", lambda: {k: [p for p in v if p["uid"] != "pb"]
                                                             for k, v in BROWSE_MAP.items()})
    with patch.object(Config, "SEARCH_LIMIT", 50):
        rows = engine.execute_search(f"{B1} {B2}", "literal", 0, restrict_sys_ids={"990003"},
                                     corpus_scope="genizah")
    assert any(r.get("cross_page") for r in rows), rows
