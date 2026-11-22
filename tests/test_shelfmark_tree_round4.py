# -*- coding: utf-8 -*-
"""The shelf picker's shared part of round 4 (2026-11-22): the details pane's text box and its
labelled fields, the scholarly transcriptions by the editions rule, and the signs on the rows. On
the fixture tree of tests/shelfmark_tree_fixture.py."""
from __future__ import annotations

import threading

import pytest

from shared import shelfmark_tree_signs as signs
from shared.shelfmark_tree_details import (TEXT_BOX_CHARS, TEXT_BOX_PAGES, field_lines, item_details,
                                           read_pages)
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture

GOOD = "ויהי בימי השופטים ויהי רעב בארץ\n" * 8
GARBLED = "| ' . : ; I l 1 | ~ ^ " * 30


def tr(text):
    return text


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db))
    assert service.is_ready()
    yield service
    service.close()


def test_a_page_that_does_not_read_is_shown_as_it_is():
    # The owner: "the text can be shown as is, fully (which will solve ... text not showing at all
    # when no 'normal' text)". A small fragment's two short lines, nothing that "reads": shown.
    got = read_pages(["", "  שורה\r\nשנייה  \n"], pages=2)
    assert got["text_pages"] == 1
    assert got["text"] == [{"page": 2, "text": "שורה\nשנייה"}]


def test_the_box_starts_at_the_first_page_that_reads_and_goes_on():
    # a long microfilm: its first page, a title page, is passed over; three pages at most
    texts = [GARBLED, "", "x", GOOD, GOOD + "ב", GOOD + "ג", GOOD + "ד"] + [""] * 20
    got = read_pages(texts, pages=len(texts))
    assert got["text_pages"] == 6
    assert [p["page"] for p in got["text"]] == [4, 5, 6] and len(got["text"]) == TEXT_BOX_PAGES
    assert got["text"][0]["text"] == GOOD.strip()
    # none reads: the first page with text, the title page passed over while another has text
    got = read_pages([GARBLED, "", "x y"] + [""] * 30, pages=33)
    assert [p["page"] for p in got["text"]] == [3]
    # only the title page has text: it is shown
    assert [p["page"] for p in read_pages([GARBLED] + [""] * 30, pages=31)["text"]] == [1]
    # nothing at all
    assert read_pages(["", " \n"], pages=2) == {"text_pages": 0, "text": []}


def test_the_box_holds_a_long_page_cut_and_no_more():
    long = "מילה " * 2000
    got = read_pages([long, GOOD], pages=2)
    assert len(got["text"]) == 1 and got["text"][0]["text"].endswith("…")
    assert len(got["text"][0]["text"]) <= TEXT_BOX_CHARS + 1
    # pages share the characters: the second is cut to what is left
    half = "א" * (TEXT_BOX_CHARS - 100)
    got = read_pages([half, "ב" * 500, GOOD], pages=3)
    assert [len(p["text"]) for p in got["text"]] == [TEXT_BOX_CHARS - 100, 101]


def test_the_text_is_never_changed_within():
    # niqqud, te'amim, and the spaces inside a line stay exactly as stored
    pointed = "בְּרֵאשִׁ֖ית  בָּרָ֣א\tאֱלֹהִ֑ים"
    assert read_pages([pointed], pages=1)["text"][0]["text"] == pointed


def test_the_pages_with_text_are_counted_only_when_all_came():
    assert read_pages([GOOD, ""], pages=5)["text_pages"] is None
    assert read_pages([GOOD, ""])["text_pages"] == 1


def test_item_details_reads_every_page_once(svc):
    leaf = next(lf for lf in svc.leaves_under("lib:1") if lf.sys_id == SYS["as2"][0])
    asked = []

    def texts(sid):
        asked.append(sid)
        return iter([GARBLED] + [""] * 23 + [GOOD])
    got = item_details(svc, leaf, "en", page_count=lambda s: 25, page_texts=texts)
    assert asked == [leaf.sys_id]
    assert got["pages"] == 25 and got["text_pages"] == 2
    assert [p["page"] for p in got["text"]] == [25]

    def broken(_sid):
        raise RuntimeError("no index")
    got = item_details(svc, leaf, "en", page_count=lambda s: 0, page_texts=broken)
    assert got["text_pages"] == 0 and got["text"] == []
    got = item_details(svc, leaf, "en", page_count=lambda s: 3, page_texts=broken)
    assert got["text_pages"] is None and got["text"] == []


def test_the_fields_in_the_owners_order():
    details = {"pages": 12, "text_pages": 10, "catalogue": 0, "bibliography": 5,
               "transcriptions": ["fgp", "pgp"], "comments": 2, "date": "1150", "subjects": ["Letters", "Law"]}
    assert field_lines(details, tr) == [
        ("pages", [("Pages", "12"), ("With text", "10")]),
        ("catalogue", [("Catalog entries", "0"), ("Bibliography entries", "5")]),
        ("transcriptions", [("Scholarly transcriptions", "PGP, FGP")]),
        ("comments", [("Comments", "2")]),
        ("date", [("Date", "1150")]),
        ("subjects", [("Subjects", "Letters, Law")]),
    ]
    assert field_lines({"pages": 1_234, "text_pages": None}, tr) == [("pages", [("Pages", "1,234")])]
    assert field_lines({"pages": 0}, tr) == [("pages", [("Pages with text", "0")])]
    assert field_lines({"pages": None, "catalogue": None, "comments": 0}, tr) == []


def test_a_scholarly_transcription_is_an_edition_not_a_translation(svc, monkeypatch):
    # The pane's transcriptions follow the editions rule, the rows' sign's: a PGP translation alone
    # is not a transcription.
    from shared import document_service, fgp_service
    leaf = next(lf for lf in svc.leaves_under("lib:1") if lf.sys_id == SYS["as2"][0])
    sid = leaf.sys_id
    monkeypatch.setattr(document_service, "get_sys_ids_with_pgp_text", lambda ids: {sid})
    monkeypatch.setattr(document_service, "get_sys_ids_with_editions", lambda ids: set())
    monkeypatch.setattr(fgp_service, "get_sys_ids_with_fgp_editions", lambda ids: set())
    assert item_details(svc, leaf, "en")["transcriptions"] == []
    monkeypatch.setattr(document_service, "get_sys_ids_with_editions", lambda ids: {sid})
    monkeypatch.setattr(fgp_service, "get_sys_ids_with_fgp_editions", lambda ids: {sid})
    assert item_details(svc, leaf, "en")["transcriptions"] == ["pgp", "fgp"]


def test_the_signs_of_a_manuscript():
    calls = []

    def loader(key, ids):
        def load():
            calls.append(key)
            return ids
        return load
    shelf = signs.ShelfSigns({"pgp": loader("pgp", ["1", "2"]), "edition": loader("edition", {"2"}),
                              "join": loader("join", ["2", "3", ""])})
    assert not shelf.ready and shelf.of("2") == ()
    assert shelf.load() is shelf and shelf.ready
    assert shelf.of("2") == ("pgp", "edition", "join")
    assert shelf.of("1") == ("pgp",) and shelf.of("3") == ("join",)
    assert shelf.of("4") == () and shelf.of("") == () and shelf.of(None) == ()
    shelf.load()
    assert calls == ["pgp", "edition", "join"]          # read once
    assert signs.signs_text(shelf.of("2"), tr) == "PGP · Scholarly transcription · Joins"
    assert signs.signs_text((), tr) == ""


def test_a_sign_that_cannot_be_read_is_left_off():
    def broken():
        raise RuntimeError("no sidecar")
    shelf = signs.ShelfSigns({"pgp": broken, "edition": lambda: None, "join": lambda: ["7"]}).load()
    assert shelf.ready and shelf.of("7") == ("join",)


def test_the_signs_load_once_across_threads():
    calls, gate = [], threading.Event()

    def slow():
        gate.wait(2)
        calls.append(1)
        return ["1"]
    shelf = signs.ShelfSigns({"pgp": slow, "edition": lambda: (), "join": lambda: ()})
    workers = [threading.Thread(target=shelf.load) for _ in range(4)]
    for w in workers:
        w.start()
    gate.set()
    for w in workers:
        w.join(5)
    assert calls == [1] and shelf.of("1") == ("pgp",)


def test_the_signs_read_the_details_rules(monkeypatch):
    # each sign's set comes from the sources the details pane reads, by the same rules
    from shared import document_service, fgp_service, fjms_service

    class Fjms:
        def is_available(self):
            return True

        def get_all_joined_sys_ids(self):
            return {"f1"}
    monkeypatch.setattr(document_service, "get_all_pgp_link_sys_ids", lambda: {"p1"})
    monkeypatch.setattr(document_service, "get_sys_ids_with_editions", lambda ids: {"e1"} if ids is None else set())
    monkeypatch.setattr(fgp_service, "get_sys_ids_with_fgp_editions", lambda ids: {"e2"} if ids is None else set())
    monkeypatch.setattr(document_service, "get_all_joined_sys_ids", lambda: {"j1"})
    monkeypatch.setattr(fjms_service, "get_fjms_service", lambda thread_safe=True: Fjms())
    shelf = signs.ShelfSigns().load()
    assert shelf.of("p1") == ("pgp",)
    assert shelf.of("e1") == ("edition",) and shelf.of("e2") == ("edition",)
    assert shelf.of("j1") == ("join",) and shelf.of("f1") == ("join",)


def test_one_signs_object_a_process():
    assert signs.shelf_signs() is signs.shelf_signs()
    assert set(signs.SIGN_LABELS) == set(signs.SIGNS)


def test_the_new_strings_have_hebrew():
    from shared.genizah_translations import TRANSLATIONS
    for key in ("With text", "Pages with text", "Bibliography entries", "Page {n}", "Links",
                "Scholarly transcription", "Catalog entries", "Scholarly transcriptions", "Pages", "Date",
                "Subjects", "Comments", "Joins"):
        assert TRANSLATIONS.get(key), key
    assert "{n}" in TRANSLATIONS["Page {n}"]


def test_a_join_partner_is_named_as_its_row(svc, monkeypatch):
    # A partner the shelf holds is named by the tree's label, so the join reads as the row its link
    # moves to; one off the shelf keeps the catalogue's (or PGP's) name.
    from shared import document_service, fjms_service
    leaf = next(lf for lf in svc.leaves_under("lib:1") if lf.sys_id == SYS["as2"][0])
    partner = SYS["as10"][0]
    label = svc.path_of(partner)[0].leaf.label

    class Meta:
        csv_bank = {partner: {"shelfmark": "Catalogue name of the partner"}, "999": {"shelfmark": "Off 9"}}

    class Fjms:
        def is_available(self):
            return True

        def get_join_group(self, sys_id):
            return [{"alma_id": partner}, {"alma_id": "999"}]

        def get_catalog_detail(self, sys_id):
            return {}

        def get_bibliography(self, sys_id):
            return []

        def get_domains_for_sys_ids(self, ids):
            return {}

        def get_catalog(self, sys_id):
            return {}
    monkeypatch.setattr(document_service, "get_document_ids_for_fragment", lambda s: [])
    monkeypatch.setattr(fjms_service, "get_fjms_service", lambda thread_safe=True: Fjms())
    got = item_details(svc, leaf, "en", meta_mgr=Meta())
    assert label and label != "Catalogue name of the partner"
    assert [(j["sys_id"], j["shelfmark"]) for j in got["joins"]] == [(partner, label), ("999", "Off 9")]

    class Broken:
        def path_of(self, sys_id):
            raise RuntimeError("closed")
    got = item_details(Broken(), leaf, "en", meta_mgr=Meta())
    assert [j["shelfmark"] for j in got["joins"]] == ["Catalogue name of the partner", "Off 9"]


def test_the_crossref_service_is_one_for_every_thread(monkeypatch):
    # The desktop window asked for a one-thread instance first, and the picker's details, on a
    # worker thread, then had no library link (CUDL, Manchester): the one instance is thread-safe.
    from shared import nli_crossref_service as crossref
    made = []

    class Service:
        def __init__(self, thread_safe=False):
            made.append(thread_safe)
    monkeypatch.setattr(crossref, "NliCrossrefService", Service)
    monkeypatch.setattr(crossref, "_default_service", None)
    first = crossref.get_nli_crossref_service(thread_safe=False)
    assert crossref.get_nli_crossref_service(thread_safe=True) is first
    assert crossref.get_nli_crossref_service() is first
    assert made == [True]
