# -*- coding: utf-8 -*-
"""The shelf picker's shared part of round 3 (2026-11-21): the
breadcrumb's arrow, the library codes in the library filter, the title in a shelfmark's tooltip and
the details pane's contents. On the fixture tree of tests/shelfmark_tree_fixture.py."""
from __future__ import annotations

import dataclasses

import pytest

from shared import shelfmark_tree_columns as cols
from shared.shelfmark_tree_details import add_joins, item_details, other_shelfmarks, read_pages, readable, thumbnail_url
from shared.shelfmark_tree_service import ShelfmarkTreeService
from shared.shelfmark_tree_view import CRUMB_SEP
from tests.shelfmark_tree_fixture import SYS, build_fixture

LABELS = {"library": "Library", "others": "Others", "in": "in {place}", "part": "Part: {part}"}


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db))
    assert service.is_ready()
    yield service
    service.close()


def _libraries(svc, text, lang):
    view = cols.load_view(svc, (), {"n:None": text}, {}, lang, LABELS)
    return [r["id"] for r in view["columns"][0]["rows"] if "id" in r]


def test_the_breadcrumb_arrow_is_one_character_that_bidi_mirrors():
    # In Hebrew the arrow pointed the wrong way: "‹" written for Hebrew was
    # mirrored back by the right-to-left text; one mirrored character, in both languages.
    import unicodedata
    assert CRUMB_SEP == "›" and unicodedata.mirrored(CRUMB_SEP)


def test_the_library_filter_finds_a_library_by_its_code_and_both_names(svc):
    # A library is found by its code too ("RNL", "CUL"), and by its name in either language. The
    # Kiev library's Hebrew name is "קייב" and its short name "Kiev": only its code finds it by "harkavy".
    assert svc.library_codes()["lib:3"] == ("Harkavy",)
    assert _libraries(svc, "harkavy", "he") == ["lib:3"]
    assert _libraries(svc, "harkavy", "en") == ["lib:3"]
    assert _libraries(svc, "vernadsky", "he") == ["lib:3"]          # the English name, in Hebrew
    assert _libraries(svc, "קייב", "en") == ["lib:3"]  # the Hebrew name, in English
    assert "lib:1" in _libraries(svc, "cul", "he")
    assert _libraries(svc, "zzz", "he") == []


def test_a_shelfmark_row_carries_its_title_for_the_tooltip(svc):
    titles = {SYS["as2"][0]: "Letter of the Gaon " + "word " * 40}
    path = tuple(n.id for n in svc.path_of(SYS["as2"][0])[0].path)
    view = cols.load_view(svc, path, {}, {}, "en", LABELS, title_of=titles.get)
    rows = {r["leaf"].sys_id: r for r in view["leaves"]["rows"]}
    first = rows[SYS["as2"][0]]
    assert first["title"].startswith("Letter of the Gaon word")
    assert len(first["title"]) <= cols.TITLE_CHARS + 1 and first["title"].endswith("…")
    assert rows[SYS["as2"][1]]["title"] == ""
    assert cols.leaf_tooltip(first) == "\n".join(x for x in (first["hint"], first["title"]) if x)
    assert cols.leaf_tooltip(first).endswith(first["title"])
    # a title_of that fails leaves the title out, never the row
    def broken(_sid):
        raise KeyError("x")
    assert all(r["title"] == "" for r in
               cols.load_view(svc, path, {}, {}, "en", LABELS, title_of=broken)["leaves"]["rows"])
    # without title_of, the rows are as before
    plain = cols.load_view(svc, path, {}, {}, "en", LABELS)
    assert all(r["title"] == "" for r in plain["leaves"]["rows"])


def test_the_tooltip_and_the_title_cut():
    assert cols.leaf_tooltip({"hint": "Or. 6 · Part: 2", "title": "A letter"}) == "Or. 6 · Part: 2\nA letter"
    assert cols.leaf_tooltip({"hint": "", "title": "A letter"}) == "A letter"
    assert cols.leaf_tooltip({"hint": "", "title": ""}) == ""
    assert cols.short_title("  a   b  ") == "a b"
    assert cols.short_title("x" * 200, 120) == "x" * 120 + "…"
    assert cols.short_title("word " * 50, 120).endswith("word…")


def test_the_details_pane_of_a_shelfmark(svc, monkeypatch):
    # The pane says what the manuscript has, not where it is (the reader is in the picker).
    from shared import document_service, fgp_service, fjms_service
    sid = SYS["as2"][0]
    partner, other = SYS["as2"][1], SYS["as10"][0]
    leaf = next(lf for lf in svc.leaves_under("lib:1") if lf.sys_id == sid)

    class Meta:
        csv_bank = {sid: {"title": "A letter", "library_code": "CUL"},
                    other: {"shelfmark": "T-S AS 10.1"}, "999": {"shelfmark": "T-S AS 2.3"}}

    class Fjms:
        def is_available(self):
            return True

        def get_join_group(self, sys_id):
            return [{"alma_id": partner}, {"alma_id": other}]

        def get_catalog_detail(self, sys_id):
            return {"records": [{"id": 1}, {"id": 2}]}

        def get_bibliography(self, sys_id):
            return [{"t": "a"}, {"t": "b"}, {"t": "c"}]

        def get_domains_for_sys_ids(self, ids):
            return {}

        def get_catalog(self, sys_id):
            return {}

    monkeypatch.setattr(document_service, "get_sys_ids_with_editions", lambda ids: {sid})
    # two PGP documents, one for each side: the joins of both are read, not of the one chosen
    monkeypatch.setattr(document_service, "get_document_ids_for_fragment", lambda s: [7, 9])
    docs = {7: [{"sys_id": sid, "shelfmark": "T-S AS 2.1"}, {"sys_id": partner, "shelfmark": "T-S AS 2.2"}],
            9: [{"sys_id": "999", "shelfmark": "CUL T-S AS2.3"}, {"sys_id": sid, "shelfmark": "T-S AS 2.1"}]}
    monkeypatch.setattr(document_service, "get_fragments_for_document", lambda pgpid: docs.get(pgpid, []))
    monkeypatch.setattr(fgp_service, "get_sys_ids_with_fgp_editions", lambda ids: set())
    monkeypatch.setattr(fjms_service, "get_fjms_service", lambda thread_safe=True: Fjms())
    text = "ויהי בימי השופטים " * 12

    got = item_details(svc, leaf, "he", meta_mgr=Meta(), page_count=lambda s: 3,
                       page_texts=lambda s: iter([text, "x"]))
    assert got["sys_id"] == sid and got["shelfmark"] == "T-S AS 2.1" and got["title"] == "A letter"
    assert "library" not in got and "place" not in got
    assert got["pages"] == 3 and got["text"][0]["text"].startswith("ויהי")
    assert got["text_pages"] is None                    # two of three pages came: not counted
    assert [p["page"] for p in got["text"]] == [1, 2]
    assert got["transcriptions"] == ["pgp"]
    # one partner from two sources is one join; the manuscript itself is never its own join
    assert [(j["sys_id"], j["shelfmark"], j["sources"]) for j in got["joins"]] == [
        (partner, "T-S AS 2.2", ["pgp", "fjms"]), ("999", "T-S AS 2.3", ["pgp"]), (other, "T-S AS 10.1", ["fjms"])]
    assert got["catalogue"] == 2 and got["bibliography"] == 3 and got["comments"] is None
    assert any(link["key"] == "ktiv" for link in got["links"])

    # the community's joins and comments, added by each app
    add_joins(got, [(other, ""), ("", "ENA 1.1"), (sid, "T-S AS 2.1")], "community")
    assert [j["sources"] for j in got["joins"]] == [["pgp", "fjms"], ["pgp"], ["fjms", "community"], ["community"]]
    got["comments"] = 4

    def tr(s):
        return s
    from shared.shelfmark_tree_details import field_lines
    lines = dict(field_lines(got, tr))
    assert lines["transcriptions"] == [("Scholarly transcriptions", "PGP")]
    assert lines["comments"] == [("Comments", "4")]
    assert lines["catalogue"] == [("Catalog entries", "2"), ("Bibliography entries", "3")]

    # nothing known: never raises, the parts are empty, and the fields say nothing
    monkeypatch.setattr(fjms_service, "get_fjms_service", lambda thread_safe=True: None)
    monkeypatch.setattr(document_service, "get_sys_ids_with_editions", lambda ids: set())
    monkeypatch.setattr(document_service, "get_document_ids_for_fragment", lambda s: [])
    bare = item_details(svc, leaf, "en")
    assert bare["title"] == "" and bare["pages"] is None and bare["text"] == []
    assert bare["joins"] == [] and bare["catalogue"] is None and field_lines(bare, tr) == []

    def broken(_sid):
        raise RuntimeError("no index")
    got = item_details(svc, leaf, "en", page_count=broken, page_texts=broken)
    assert got["pages"] is None and got["text"] == []
    # no pages with text: the index is not read
    assert item_details(svc, leaf, "en", page_count=lambda s: 0, page_texts=broken)["text"] == []


def _first_page(texts, pages=None):
    shown = read_pages(texts, pages)["text"]
    return shown[0]["page"] if shown else None


def test_the_text_starts_past_a_title_page_and_what_does_not_read():
    good = "ויהי בימי השופטים\n" * 20
    garbled = "| ' . : ; I l 1 | ~ ^ " * 30 + "א ב ג " * 30
    assert readable(good) and not readable(garbled) and not readable("אבג")
    # the first page that reads
    assert _first_page([garbled, good]) == 2
    # a long manuscript (a microfilm) does not start at its first page, even one that reads
    title = "Rossiiskaya natsionalnaya biblioteka manuscript collection " * 4
    assert readable(title)
    assert _first_page([title, good] + [""] * 298, pages=300) == 2
    assert _first_page([title, good], pages=2) == 1


def test_a_small_fragments_few_lines_read():
    # a fragment of two pages whose first text is four short lines (38 letters), then stray marks
    few = "].ניס [\n].י. ברכם ואלוק\nזכתואת חמיה נוס אלה\nכל פנימיכל"
    marks = "| ' . : ; I l 1 | ~ ^ " * 4
    assert _first_page([marks, few], pages=2) == 2
    # held to the full rule when the manuscript is long: the marks, the first page with text, start it
    assert _first_page([""] + [marks, few] + [""] * 297, pages=300) == 2
    # a short manuscript's stray marks still do not read
    assert not readable(marks, 30)
    assert not readable("א ב ג ד ה ו ז ח ט י כ ל מ נ ס ע פ צ ק ר ש ת " * 2, 30)


def test_other_shelfmarks_lists_every_other_call_number_once():
    from shared.shelfmark_tree_service import ShelfLeaf
    leaf = ShelfLeaf(id="p", node_id="n", library_id="lib:1", sys_id="s", shelfmark="Or. 6",
                     shelf_label="", label="Or. 6", part="", inventory_id=None, how="exact", pos=1,
                     aliases="Or. 6 | Or. 1080 J 6 | Cambridge University Library, Ms. Or. 1080 J 6 | Or. 1080 J 6")
    assert other_shelfmarks(leaf) == ["Or. 1080 J 6"]
    fgp = ShelfLeaf(id="p", node_id="n", library_id="lib:1", sys_id="s", shelfmark="T-S 10J1.1",
                    shelf_label="T-S 10J1.1", label="T-S 10 J 1.1", part="", inventory_id=None,
                    how="fist", pos=1, aliases="Ms. T-S 10J1.1 | Ms. T-S 10 J 1.1")
    assert other_shelfmarks(fgp) == []          # the app's own shelfmark is FGP's but for its spaces
    assert other_shelfmarks(dataclasses.replace(fgp, label="T-S 10J1 f. 1")) == ["T-S 10J1.1"]


def test_a_call_number_that_only_restates_the_shelfmark_is_not_shown_again():
    from types import SimpleNamespace

    from shared.shelfmark_tree_columns import hint, restates
    from shared.shelfmark_tree_service import ShelfLeaf
    assert restates("Cambridge University Library Ms. T-S AS 39.7", "T-S AS 39.7")
    assert restates("Ms. T-S AS 39.7", "T-S AS 39.7") and restates("t-s  as 39.7", "T-S AS 39.7")
    assert restates("Cambridge University Library, MSS T-S AS 39.7", "T-S AS 39.7")
    assert restates("Cambridge University Library Ms. T-S 13 K 4.6", "T-S 13K4.6")
    assert restates("The Bodleian Libraries MS Heb c 57", "MS Heb c 57")
    # a different call number, or the shelfmark inside a longer one, is not a restatement
    assert not restates("Ms. heb. a. 1.5", "MS heb. a.1/5")
    assert not restates("Or. 1080 T-S AS 39.7", "T-S AS 39.7")
    assert not restates("T-S AS 39.7", "AS 39.7") and not restates("", "T-S AS 39.7")
    leaf = ShelfLeaf(id="p", node_id="n", library_id="lib:1", sys_id="s", shelfmark="T-S AS 39.7",
                     shelf_label="", label="T-S AS 39.7", part="", inventory_id=None, how="fist", pos=1,
                     aliases="Cambridge University Library Ms. T-S AS 39.7 | Ms. T-S AS 39.7")
    assert other_shelfmarks(leaf) == []
    tip = SimpleNamespace(aliases="Ms. T-S AS 39.7 | T-S Ar. 5.1", label="T-S AS 39.7",
                          shelfmark="T-S AS 39.7", part="")
    assert hint(tip, {"part": "Part: {part}"}) == "T-S Ar. 5.1"


def test_each_call_number_once_whichever_form_comes_first():
    from types import SimpleNamespace

    from shared.shelfmark_tree_columns import call_numbers_once, hint
    from shared.shelfmark_tree_service import ShelfLeaf
    # the long form before the short one, and after it: the short one, in the first one's place
    assert call_numbers_once(["Cambridge University Library, Ms. Or. 1080 J 6", "Or. 1080 J 6"]) == ["Or. 1080 J 6"]
    assert call_numbers_once(["Or. 1080 J 6", "Cambridge University Library, Ms. Or. 1080 J 6"]) == ["Or. 1080 J 6"]
    # two libraries' long forms of one call number, then the call number: once
    assert call_numbers_once(["A Library Ms. ENA 1", "T-S 1.1", "B Library Ms. ENA 1", "ENA 1"]) == ["ENA 1", "T-S 1.1"]
    leaf = ShelfLeaf(id="p", node_id="n", library_id="lib:1", sys_id="s", shelfmark="Or. 6",
                     shelf_label="", label="Or. 6", part="", inventory_id=None, how="exact", pos=1,
                     aliases="Cambridge University Library, Ms. Or. 1080 J 6 | Or. 1080 J 6")
    assert other_shelfmarks(leaf) == ["Or. 1080 J 6"]
    tip = SimpleNamespace(aliases="Ms. Or. 5 | Or. 5", label="T-S 1.1", shelfmark="T-S 1.1", part="")
    assert hint(tip, {"part": "Part: {part}"}) == "Or. 5"


def test_the_thumbnail():
    class Meta:
        def get_thumbnail(self, sys_id, size=320):
            return f"https://img/{sys_id}/{size}"

    class Broken:
        def get_thumbnail(self, sys_id, size=320):
            raise OSError("offline")
    assert thumbnail_url("99", Meta()) == "https://img/99/320"
    assert thumbnail_url("99", Broken()) == "" and thumbnail_url("99", None) == "" and thumbnail_url("", Meta()) == ""


def test_the_new_strings_have_hebrew():
    from shared.genizah_translations import TRANSLATIONS
    for key in ("Tick shelfmarks to choose them.", "Choose a shelfmark to see it here", "Other shelfmarks",
                "Open", "Open {name}"):
        assert TRANSLATIONS.get(key), key
    assert TRANSLATIONS["Open"] == "פתח"
    assert "{name}" in TRANSLATIONS["Open {name}"]


def test_the_index_hands_the_first_pages_one_at_a_time():
    from shared.search_engine import SearchEngine
    engine = SearchEngine.__new__(SearchEngine)
    read = []
    # two volumes, each numbered from 1, in the browse map's order (Browse's): kept, not sorted together
    engine._load_browse_map = lambda: {"s": [{"uid": "a1", "p_num": 1}, {"uid": "a2", "p_num": "2"},
                                             {"uid": "a3", "p_num": 3}, {"uid": "b1", "p_num": 1}]}
    engine.get_full_text_by_id = lambda uid: read.append(uid) or f"text {uid}"
    pages = engine.page_texts("s", limit=3)
    assert read == []                                   # nothing read before it is asked for
    assert next(pages) == "text a1" and read == ["a1"]
    assert list(pages) == ["text a2", "text a3"] and read == ["a1", "a2", "a3"]
    assert list(engine.page_texts("missing")) == [] and list(engine.page_texts("")) == []
    # with no limit, every page (the details count the pages with text)
    assert list(engine.page_texts("s")) == ["text a1", "text a2", "text a3", "text b1"]


def test_a_shelfmark_only_join_is_the_same_partner_as_one_with_its_id():
    # the community's rows may carry only the shelfmark; a partner known by its id gains the source
    details = {"sys_id": "own", "shelfmark": "T-S AS 2.1", "joins": []}
    add_joins(details, [("p1", "T-S AS 2.2")], "pgp")
    add_joins(details, [("", "t-s  as 2.2"), ("", "T-S AS 2.1"), ("", "ENA 1.1")], "community")
    add_joins(details, [("p3", "ENA 1.1"), ("p4", "T-S AS 2.2")], "fjms")
    assert [(j["sys_id"], j["shelfmark"], j["sources"]) for j in details["joins"]] == [
        ("p1", "T-S AS 2.2", ["pgp", "community"]),     # by its shelfmark, case and spaces aside
        ("p3", "ENA 1.1", ["community", "fjms"]),        # the id arriving later fills it in
        ("p4", "T-S AS 2.2", ["fjms"]),                  # two different ids are two partners
    ]                                                    # and the manuscript's own shelfmark is left out
