"""Safety/provenance gates and separated-word behavior of the offline preparation.

Synthetic text only: no copyrighted dictionary text is committed as a fixture.
"""
from __future__ import annotations

import itertools
import json
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from scripts import prepare_friedman_dictionary as prep


def brute_match(tokens, query, gap):
    for positions in itertools.combinations(range(len(tokens)), len(query)):
        if [tokens[i] for i in positions] == list(query) and all(
                b - a - 1 <= gap for a, b in zip(positions, positions[1:])):
            return positions
    return None


def test_gapped_match_agrees_with_exhaustive_oracle():
    for length in range(7):
        for tokens in itertools.product("אב", repeat=length):
            for query in ("א", "אב", "אא", "אבא", "אאב"):
                for gap in range(3):
                    result = prep.ordered_match(list(tokens), list(query), gap)
                    assert (result is None) == (brute_match(tokens, query, gap) is None)
                    if result is not None:
                        assert [tokens[i] for i in result] == list(query)
                        assert all(0 < b - a <= gap + 1 for a, b in zip(result, result[1:]))


def test_word_order_and_repeated_words():
    assert prep.ordered_match("לא עוד אבד".split(), ["לא", "אבד"], 0) is None
    assert prep.ordered_match("לא עוד אבד".split(), ["לא", "אבד"], 1) == (0, 2)
    assert prep.ordered_match(["אבד", "לא"], ["לא", "אבד"], 5) is None
    assert prep.ordered_match(["מן"], ["מן", "מן"], 5) is None
    assert prep.ordered_match(["מן", "מן"], ["מן", "מן"], 0) == (0, 1)


def test_source_pagination_reports_overlaps_gaps_and_unknown_labels():
    records = [{"source_id": "a", "pdf_page": i, "printed_page": label}
               for i, label in enumerate(["10", "11", "11", "13", None], 1)]
    result = prep.pagination_summary(records)
    assert result["distinct_printed_pages"] == 3
    assert result["missing_within_range"] == [12]
    assert result["unlabelled_pdf_pages"] == 1
    assert result["overlaps"] == [{"printed_page": 11, "locations": [
        {"source_id": "a", "pdf_page": 2}, {"source_id": "a", "pdf_page": 3}]}]


def test_fetch_rejects_changed_inventory_before_pdf_download(tmp_path, monkeypatch):
    monkeypatch.setattr(prep, "download", lambda url: b"LoadPDF('../outside')")
    with pytest.raises(ValueError, match="inventory changed"):
        prep.fetch(tmp_path)
    assert not (tmp_path / "source_manifest.json").exists()


def test_source_snapshot_cannot_be_overwritten(tmp_path):
    path = tmp_path / "source.pdf"
    prep.store_source(path, b"original")
    prep.store_source(path, b"original")
    with pytest.raises(ValueError, match="Source changed"):
        prep.store_source(path, b"replacement")
    assert path.read_bytes() == b"original"


class Page:
    rect = SimpleNamespace(height=700)

    def __init__(self, raw, blocks=()):
        self.raw = raw
        self.blocks = list(blocks)

    def get_text(self, kind, sort=False):
        return self.raw if kind == "text" else {"blocks": self.blocks}


def span(text, font, x, y=140, size=13.5):
    return {"text": text, "font": font, "size": size, "bbox": [x, y, x + 15, y + 15]}


def test_broken_encoding_is_not_indexable_but_raw_data_is_retained():
    raw = "\x02ÆØº\x03" * 70
    record = prep.inspect_page(Page(raw))
    assert record["raw_text"] == raw
    assert not record["text_searchable"]
    assert record["heading_candidates"] == []


def test_index_recovery_is_bound_to_the_inspected_pdf():
    assert prep.recover_index_letters(None, "different-source") == {"status": "not_attempted_unreviewed_source"}
    assert "".join(prep.INDEX_LETTERS.values()) == "אבגדהוזחטיךכלםמןנסעףפץצקרשת"


def test_index_recovery_changes_only_letter_mappings(monkeypatch):
    class Document:
        def __iter__(self):
            return iter([SimpleNamespace(get_fonts=lambda: [(35, "cff", "Type1", "PREFIX+Advhadasa-n")])])

        def xref_get_key(self, xref, key):
            return "xref", "36 0 R" if key == "Encoding" else "39 0 R"

        def xref_object(self, xref):
            return "<< /Differences [ 11 /C224 31 /tilde 198 /AE ] >>"

        def xref_stream(self, xref):
            return (b"1 begincodespacerange <1f> <f8> endcodespacerange "
                    b"2 beginbfchar <1f> <02DC> <c6> <00C6> endbfchar "
                    b"1 beginbfrange <28> <29> <0028> endbfrange endcmap")

        def update_stream(self, xref, data):
            self.new_cmap = data

        def tobytes(self):
            return b"in-memory-only"

    class Corrected:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            return iter([Page("אב")])

    monkeypatch.setitem(sys.modules, "fitz", SimpleNamespace(open=lambda **kwargs: Corrected()))
    doc = Document()
    result = prep.recover_index_letters(doc, prep.INDEX_RECOVERY_SHA256)
    assert b"<0B> <05D0>" in doc.new_cmap and b"<C6> <05D1>" in doc.new_cmap
    assert b"<00C6>" not in doc.new_cmap
    assert b"<1F> <02DC>" in doc.new_cmap, "unreviewed diacritic glyph stays unchanged"
    assert b"<28> <29> <0028>" in doc.new_cmap, "parenthesis mapping stays unchanged"
    assert b"<00> <FF>" in doc.new_cmap, "the low source codes must be inside the codespace"
    assert result["pages"][0]["text"] == "אב"


@pytest.mark.parametrize("page_number", ["[12]", "]12["])
def test_heading_candidates_keep_separate_provenance_and_join_vowel_split_letters(page_number):
    spans = [span("ד", "Hadassah-Bold", 100), span("َ ", "ArialUnicodeMS", 118),
             span("ב", "Hadassah-Bold", 120), span("א", "Hadassah-Bold", 140)]
    block = {"type": 0, "lines": [{"bbox": [100, 140, 160, 155], "spans": spans}]}
    footer = {"type": 0, "lines": [{"bbox": [100, 640, 160, 655],
                                    "spans": [span(page_number, "FrankRuehl", 100, y=640)]}]}
    raw = "א בד " + "מילים בעברית לצורך בדיקת חילוץ הטקסט " * 2
    record = prep.inspect_page(Page(raw, [block, footer]))
    assert record["raw_text"] == raw
    assert record["blocks"][0]["lines"][0]["spans"] == spans
    assert record["heading_candidates"][0]["key"] == "אבד"
    assert record["heading_candidates"][0]["status"] == "typographic_candidate"
    assert record["printed_page"] == "12"
    assert record["text_searchable"]


@pytest.fixture
def archive(tmp_path, monkeypatch):
    html = " ".join(f"LoadPDF('{name}'" for name in prep.SOURCE_IDS).encode()
    monkeypatch.setattr(prep, "download", lambda url: html if url == prep.SITE_URL else b"%PDF-fake-source")
    prep.fetch(tmp_path)
    return tmp_path


@pytest.fixture
def built(archive, monkeypatch):
    class Document:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            return iter([Page("לא עוד אבד " + "מילים בעברית לצורך בדיקת חילוץ הטקסט " * 2),
                         Page("\x02ÆØº" * 100)])

    monkeypatch.setitem(sys.modules, "fitz", SimpleNamespace(open=lambda path: Document(), VersionBind="test"))
    prep.build(archive)
    return archive


def test_full_archive_build_verify_and_gapped_lookup(built):
    result = prep.verify(built)
    assert result["verified"] and result["sources"] == 38 and result["indexed_body_pages"] == 28
    assert result["status"] == "offline_candidate_not_for_production"
    assert prep.search(built, "לא אבד")["hits"] == []
    found = prep.search(built, "לא אבד", gap=1, limit=2)
    assert len(found["hits"]) == 2 and found["more"]
    assert found["hits"][0]["url"].endswith("1.pdf#page=1")
    assert found["hits"][0]["match_type"] == "extracted_page_text"
    assert found["credit_he"] == prep.CREDIT_HE
    audit = prep.read_json(built / "candidate" / "audit.json")
    assert audit["total_pages"] == 76
    assert all(row["unreadable_pages"] == [2] for row in audit["sources"])


@pytest.mark.parametrize("part", ["pdf", "extraction", "database"])
def test_changed_artifact_fails_verification(built, part):
    paths = {"pdf": built / "download" / "1.pdf", "extraction": built / "candidate" / "1.json",
             "database": built / "candidate" / "pages.sqlite"}
    with paths[part].open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="checksum mismatch"):
        prep.verify(built)


def test_token_corruption_fails_even_if_database_hash_is_updated(built):
    path = built / "candidate" / "pages.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE positions SET token='שגיאה' WHERE page_id=1 AND position=0")
    audit_path = built / "candidate" / "audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    audit["database_sha256"] = prep.sha256(path.read_bytes())
    prep.write_json(audit_path, audit)
    with pytest.raises(ValueError, match="tokens differ"):
        prep.verify(built)


def test_failed_build_does_not_replace_previous_database(built, monkeypatch):
    path = built / "candidate" / "pages.sqlite"
    original = path.read_bytes()

    def broken(page):
        raise ValueError("broken extraction")

    monkeypatch.setattr(prep, "inspect_page", broken)
    with pytest.raises(ValueError, match="broken extraction"):
        prep.build(built)
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.building"))


@pytest.mark.parametrize("query,gap,limit", [("", 0, 10), ("abc", 0, 10), ("לא", -1, 10),
                                            ("לא", 21, 10), ("לא", 0, 51), ("לא " * 13, 0, 10)])
def test_invalid_search_cannot_open_or_create_a_database(tmp_path, query, gap, limit):
    with pytest.raises(ValueError):
        prep.search(tmp_path, query, gap, limit)
    assert not (tmp_path / "candidate").exists()
