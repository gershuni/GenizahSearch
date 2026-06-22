# -*- coding: utf-8 -*-
"""
Tests for FgpService (shared/fgp_service.py) using real SQLite fixtures.

The real fgp_transcriptions.db is gitignored (~387 MB) and absent in CI, so these
tests build small in-memory-style temp SQLite databases matching the ASSUMED FGP
schema (mirrors PGP document_sources, flattened with sys_id/page_info/fgp_c_number
on each row). They pin the chooser contract: source-dict shape, section
normalization, recto/verso splitting (never both sides), the source-kind/namespace
helpers, the feature flag, graceful DB-absent degradation, and schema discovery.

Test classes:
- TestSchemaDiscovery: finds the source table under different names; no-sys_id table
- TestGetFgpSourcesForFragment: source-dict shape + ordering + page_info
- TestSectionNormalization: page_num -> canvas_num
- TestGetFgpSectionForPage: recto/verso split, no-both-sides invariant (FGP-02)
- TestSourceKindHelpers: provider / relation-kind / namespaced id (FGP-03)
- TestFeatureFlag: flag off -> [] (FGP-04)
- TestServiceUnavailable: missing DB degrades gracefully
- TestBatchLookup: get_sys_ids_with_fgp_sources
"""

import os
import sqlite3

import pytest

from shared import fgp_service
from shared.fgp_service import (
    FGP_ATTRIBUTION,
    FgpService,
    _content_similarity,
    _fgp_match_image_number,
    _heb_token_set,
    _select_fgp_editions_by_similarity,
    _select_fgp_sources_for_page,
    dedupe_fgp_sources,
    fgp_image_number_for_displayed_page,
    fgp_source_for_folio,
    filter_sources_for_page,
    folio_label_for_displayed_page,
    get_fgp_section_for_page,
    group_transcription_sources,
    namespaced_source_id,
    reset_fgp_service,
    source_provider,
    source_relation_kind,
)


# ── Schema + fixture helpers ──────────────────────────────────────

# Real FGP schema (fgp_data/README.md): table fgp_transcriptions; sys_id/page_info/
# c_number/sections live on the row; no sequence_order column.
FGP_TRANSCRIPTIONS_DDL = """
CREATE TABLE fgp_transcriptions (
    id INTEGER PRIMARY KEY,
    collection TEXT,
    shelfmark TEXT,
    c_number TEXT,
    image_id TEXT,
    source_scholar TEXT,
    doc_relation TEXT,
    language TEXT,
    content TEXT,
    content_length INTEGER,
    n_pages INTEGER,
    sections TEXT,
    sys_id TEXT,
    page_info TEXT,
    folio_num INTEGER,
    image_side TEXT,
    source_credit TEXT
)
"""


def _create_fgp_db(tmp_path_str: str, table_ddl: str = FGP_TRANSCRIPTIONS_DDL) -> str:
    """Create a temp FGP SQLite database with schema. Returns path."""
    db_path = os.path.join(tmp_path_str, "test_fgp.db")
    conn = sqlite3.connect(db_path)
    conn.execute(table_ddl)
    conn.commit()
    conn.close()
    return db_path


def _insert_sample_data(db_path: str):
    """Insert standard FGP sample rows for most tests."""
    conn = sqlite3.connect(db_path)
    cols = ("id, collection, shelfmark, c_number, source_scholar, doc_relation, "
            "language, content, sections, sys_id, page_info")
    q = f"INSERT INTO fgp_transcriptions ({cols}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    # Row 1: edition with structured page_num sections (recto + verso), sys_id A
    conn.execute(q, (1, "CUL", "T-S 8J5.11", "C12345", "FGP", "Digital Edition",
                     None, "recto side text\nverso side text",
                     '[{"page_num": 1, "text": "recto side text"}, '
                     '{"page_num": 2, "text": "verso side text"}]',
                     "003072766", None))
    # Row 2: edition, verso-only via page_info, NO sections, sys_id A
    conn.execute(q, (2, "CUL", "T-S 8J5.11", "C12346", "FGP", "Digital Edition",
                     None, "only verso content here", None, "003072766", "verso"))
    # Row 3: edition, NO sections, NO page_info, sys_id B
    conn.execute(q, (3, "JTS", "ENA 1234.5", "C20000", "FGP", "Digital Edition",
                     None, "unsided fragment text", None, "003099001", None))
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _enable_flag_and_reset_singleton(monkeypatch):
    """Enable the FGP flag by default and reset the module singleton per test."""
    monkeypatch.setenv("FGP_TRANSCRIPTIONS_ENABLED", "1")
    reset_fgp_service()
    yield
    reset_fgp_service()


@pytest.fixture
def fgp_svc(tmp_path):
    """An FgpService over a temp DB populated with sample data."""
    db_path = _create_fgp_db(str(tmp_path))
    _insert_sample_data(db_path)
    svc = FgpService(db_path=db_path)
    yield svc
    svc.close()


# ── TestSchemaDiscovery ───────────────────────────────────────────


class TestSchemaDiscovery:
    def test_discovers_fgp_transcriptions_table(self, fgp_svc):
        assert fgp_svc.is_available()
        assert fgp_svc._table == "fgp_transcriptions"
        assert "sys_id" in fgp_svc._columns

    def test_discovers_alternate_table_name(self, tmp_path):
        # Robustness: a rebuilt DB under a different (candidate) name still resolves.
        ddl = FGP_TRANSCRIPTIONS_DDL.replace("fgp_transcriptions", "fgp_sources")
        db_path = _create_fgp_db(str(tmp_path), table_ddl=ddl)
        svc = FgpService(db_path=db_path)
        try:
            assert svc.is_available()
            assert svc._table == "fgp_sources"
        finally:
            svc.close()

    def test_table_without_sys_id_is_not_available(self, tmp_path):
        ddl = "CREATE TABLE document_sources (id INTEGER PRIMARY KEY, content TEXT)"
        db_path = _create_fgp_db(str(tmp_path), table_ddl=ddl)
        svc = FgpService(db_path=db_path)
        try:
            # Connection opened but no usable source table -> not available.
            assert svc.is_available() is False
            assert svc.get_fgp_sources_for_fragment("003072766") == []
        finally:
            svc.close()


# ── TestGetFgpSourcesForFragment ──────────────────────────────────


class TestGetFgpSourcesForFragment:
    def test_returns_chooser_shaped_dicts(self, fgp_svc):
        sources = fgp_svc.get_fgp_sources_for_fragment("003072766")
        assert len(sources) == 2
        s = sources[0]
        # Discriminator + attribution (FGP-01/03)
        assert s["source"] == "fgp"
        assert s["is_fgp"] is True
        assert s["source_scholar"] == "FGP"
        assert s["attribution"] == FGP_ATTRIBUTION
        assert s["fgp_c_number"] == "C12345"
        # Chooser-consumed keys present
        for key in ("doc_relation", "content", "language", "id", "sections", "page_info"):
            assert key in s

    def test_ordered_by_id(self, fgp_svc):
        # No sequence_order column -> stable order by id.
        sources = fgp_svc.get_fgp_sources_for_fragment("003072766")
        assert [s["id"] for s in sources] == [1, 2]

    def test_namespaced_uid(self, fgp_svc):
        sources = fgp_svc.get_fgp_sources_for_fragment("003072766")
        assert sources[0]["uid"] == "fgp:1"
        assert sources[1]["uid"] == "fgp:2"

    def test_unknown_sys_id_returns_empty(self, fgp_svc):
        assert fgp_svc.get_fgp_sources_for_fragment("000000000") == []

    def test_empty_sys_id_returns_empty(self, fgp_svc):
        assert fgp_svc.get_fgp_sources_for_fragment("") == []


# ── TestSectionNormalization ──────────────────────────────────────


class TestSectionNormalization:
    def test_page_num_copied_to_canvas_num(self, fgp_svc):
        sources = fgp_svc.get_fgp_sources_for_fragment("003072766")
        sections = sources[0]["sections"]
        assert sections is not None
        assert sections[0]["canvas_num"] == 1
        assert sections[1]["canvas_num"] == 2
        # original page_num preserved
        assert sections[0]["page_num"] == 1

    def test_no_sections_stays_none(self, fgp_svc):
        sources = fgp_svc.get_fgp_sources_for_fragment("003072766")
        assert sources[1]["sections"] is None


# ── TestGetFgpSectionForPage (FGP-02) ─────────────────────────────


class TestGetFgpSectionForPage:
    def test_structured_sections_split_recto_verso(self, fgp_svc):
        src = fgp_svc.get_fgp_sources_for_fragment("003072766")[0]
        assert get_fgp_section_for_page(src, 1) == "recto side text"
        assert get_fgp_section_for_page(src, 2) == "verso side text"

    def test_structured_sections_uncovered_page_is_none(self):
        # Only recto section present -> verso must be None (NOT full text).
        src = {
            "content": "recto only",
            "sections": [{"canvas_num": 1, "text": "recto only"}],
        }
        assert get_fgp_section_for_page(src, 1) == "recto only"
        assert get_fgp_section_for_page(src, 2) is None

    def test_no_sections_verso_page_info(self, fgp_svc):
        # Row 2: page_info='verso', no sections.
        src = fgp_svc.get_fgp_sources_for_fragment("003072766")[1]
        assert get_fgp_section_for_page(src, 1) is None
        assert get_fgp_section_for_page(src, 2) == "only verso content here"

    def test_no_sections_no_page_info_defaults_recto_only(self, fgp_svc):
        # Row 3 (sys_id B): no sections, no page_info -> recto only, NEVER both.
        src = fgp_svc.get_fgp_sources_for_fragment("003099001")[0]
        assert get_fgp_section_for_page(src, 1) == "unsided fragment text"
        assert get_fgp_section_for_page(src, 2) is None

    def test_page_info_takes_precedence_over_sections(self):
        # page_info is authoritative for the side: a 'recto' row shows its FULL
        # content on recto and nothing on verso, even if it has multi-page sections.
        src = {
            "content": "full recto pdf text",
            "page_info": "recto",
            "sections": [{"canvas_num": 1, "text": "pdf page 1"},
                         {"canvas_num": 2, "text": "pdf page 2"}],
        }
        assert get_fgp_section_for_page(src, 1) == "full recto pdf text"
        assert get_fgp_section_for_page(src, 2) is None

    def test_empty_content_is_none(self):
        assert get_fgp_section_for_page({"content": ""}, 1) is None
        assert get_fgp_section_for_page({"content": "   "}, 1) is None


# ── TestSourceKindHelpers (FGP-03) ────────────────────────────────


class TestSourceKindHelpers:
    def test_source_provider(self):
        assert source_provider({"source": "fgp"}) == "fgp"
        assert source_provider({"is_fgp": True}) == "fgp"
        assert source_provider({"source": "pgp"}) == "pgp"
        assert source_provider({}) == "pgp"

    def test_source_relation_kind(self):
        assert source_relation_kind({"doc_relation": "Digital Edition"}) == "edition"
        assert source_relation_kind({"doc_relation": "Digital Translation"}) == "translation"
        assert source_relation_kind({"doc_relation": ""}) == "other"
        assert source_relation_kind({}) == "other"

    def test_namespaced_source_id_no_collision(self):
        # PGP and FGP share integer ids; namespacing must disambiguate.
        pgp = {"source": "pgp", "id": 123}
        fgp = {"source": "fgp", "id": 123}
        assert namespaced_source_id(pgp) == "pgp:123"
        assert namespaced_source_id(fgp) == "fgp:123"
        assert namespaced_source_id(pgp) != namespaced_source_id(fgp)

    def test_namespaced_source_id_none(self):
        assert namespaced_source_id({"source": "fgp"}) is None


# ── TestGroupTranscriptionSources (FGP-03/07) ─────────────────────


class TestGroupTranscriptionSources:
    def test_fgp_edition_not_folded_into_pgp(self):
        # Both share 'Digital Edition'; provider must keep them in separate groups.
        sources = [
            {"source": "pgp", "doc_relation": "Digital Edition", "content": "p", "id": 1},
            {"source": "fgp", "doc_relation": "Digital Edition", "content": "f", "id": 1},
        ]
        groups = group_transcription_sources(sources)
        assert [s["content"] for s in groups["pgp_editions"]] == ["p"]
        assert [s["content"] for s in groups["fgp_editions"]] == ["f"]

    def test_translations_split_by_provider(self):
        sources = [
            {"source": "pgp", "doc_relation": "Digital Translation", "content": "pt", "id": 1},
            {"is_fgp": True, "doc_relation": "Digital Translation", "content": "ft", "id": 2},
        ]
        groups = group_transcription_sources(sources)
        assert groups["pgp_translations"][0]["content"] == "pt"
        assert groups["fgp_translations"][0]["content"] == "ft"

    def test_contentless_skipped(self):
        sources = [{"source": "fgp", "doc_relation": "Digital Edition", "content": ""}]
        groups = group_transcription_sources(sources)
        assert groups["fgp_editions"] == []

    def test_order_preserved(self):
        sources = [
            {"source": "fgp", "doc_relation": "Digital Edition", "content": "a", "id": 5},
            {"source": "fgp", "doc_relation": "Digital Edition", "content": "b", "id": 2},
        ]
        groups = group_transcription_sources(sources)
        assert [s["content"] for s in groups["fgp_editions"]] == ["a", "b"]

    def test_empty_and_none(self):
        empty = {"pgp_editions": [], "fgp_editions": [], "pgp_translations": [], "fgp_translations": []}
        assert group_transcription_sources(None) == empty
        assert group_transcription_sources([]) == empty


# ── TestFilterSourcesForPage (FGP-04.4) ───────────────────────────


class TestFilterSourcesForPage:
    def test_pgp_page_info_match_kept(self):
        sources = [{"source": "pgp", "doc_relation": "Digital Edition",
                    "content": "x", "page_info": "verso"}]
        assert filter_sources_for_page(sources, 2)  # verso page keeps it
        assert filter_sources_for_page(sources, 1) == []  # recto page drops it

    def test_pgp_no_page_info_narrowed(self):
        # Lowercase content lines so the recto/verso marker regex (which treats a
        # Capitalized following word as a qualifier) splits as intended. Fresh
        # dict per call — the PGP path narrows content in place (original behavior).
        def _src():
            return [{"source": "pgp", "doc_relation": "Digital Edition",
                     "content": "Recto\nrecto body\nVerso\nverso body", "page_info": None}]
        assert filter_sources_for_page(_src(), 1)[0]["content"] == "recto body"
        assert filter_sources_for_page(_src(), 2)[0]["content"] == "verso body"

    def test_pgp_translation_not_narrowed(self):
        sources = [{"source": "pgp", "doc_relation": "Digital Translation",
                    "content": "full translation", "page_info": None}]
        kept = filter_sources_for_page(sources, 1)
        assert kept[0]["content"] == "full translation"

    def test_fgp_kept_full_on_every_page(self):
        # Codex HIGH-1: FGP is per-image and NOT side-filtered — kept with full
        # content on BOTH recto and verso (its numbering is independent of the
        # displayed page model, so side-filtering wrongly hid rows).
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "verso text", "page_info": "verso", "sections": None}
        assert filter_sources_for_page([fgp], 2)[0]["content"] == "verso text"
        assert filter_sources_for_page([fgp], 1)[0]["content"] == "verso text"

    def test_mixed_list_order_and_split(self):
        pgp = {"source": "pgp", "doc_relation": "Digital Edition",
               "content": "p-recto", "page_info": "recto"}
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "f-unsided", "page_info": None, "sections": None}
        out = filter_sources_for_page([pgp, fgp], 1)
        # both present on recto, original order
        assert [s["content"] for s in out] == ["p-recto", "f-unsided"]
        # verso: pgp(recto) dropped; fgp kept full (no side-filtering) — Codex HIGH-1
        assert [s["content"] for s in filter_sources_for_page([pgp, fgp], 2)] == ["f-unsided"]

    def test_fgp_dict_not_mutated(self):
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "orig", "page_info": "recto", "sections": None}
        filter_sources_for_page([fgp], 1)
        assert fgp["content"] == "orig"  # copied, not clobbered


# ── TestFgpFolioMapping — align FGP to the displayed image by folio ───
#
# FGP is one row per manuscript image. When the caller knows the displayed
# image's folio label (resolved from the local NLI crossref folio_images, which
# shares the same ImageName origin as the FGP folio — verified 56/56 on
# Add.3207), the chooser shows only that image's transcription, matched by folio
# label (robust to gaps — a manuscript starting at 1v aligns to the 1v image,
# not image position 1).


class TestFgpFolioMapping:
    @staticmethod
    def _fgp(side=None, folio_num=None, page_info=None, content="x", c=None):
        return {
            "source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
            "content": content, "image_side": side, "folio_num": folio_num,
            "page_info": page_info, "fgp_c_number": c, "sections": None,
        }

    def test_matches_own_folio(self):
        assert fgp_source_for_folio(self._fgp(side="1r"), "1r") is True
        assert fgp_source_for_folio(self._fgp(side="1r"), "1v") is False
        assert fgp_source_for_folio(self._fgp(side="2v"), "2v") is True

    def test_match_is_case_insensitive(self):
        assert fgp_source_for_folio(self._fgp(side="1R"), "1r") is True

    def test_compose_from_folio_num_and_page_info_when_no_side(self):
        assert fgp_source_for_folio(
            self._fgp(folio_num=3, page_info="recto"), "3r") is True
        assert fgp_source_for_folio(
            self._fgp(folio_num=3, page_info="verso"), "3r") is False

    def test_whole_doc_shows_on_every_folio(self):
        # No image_side / folio_num -> whole-document transcription.
        wd = self._fgp(content="whole")
        assert fgp_source_for_folio(wd, "1r") is True
        assert fgp_source_for_folio(wd, "9v") is True

    def test_unknown_displayed_folio_keeps_row(self):
        # Caller could not resolve the displayed folio (e.g. non-NLI MS) -> never hide.
        assert fgp_source_for_folio(self._fgp(side="1r"), None) is True
        assert fgp_source_for_folio(self._fgp(side="1r"), "") is True

    def test_c_number_is_not_a_match_key(self):
        # A row with only a c_number (no real folio) is whole-doc, NOT folio 'C123';
        # it must never be hidden by failing to match a displayed folio.
        row = self._fgp(c="C362967")
        assert fgp_source_for_folio(row, "1r") is True
        assert fgp_source_for_folio(row, "5v") is True

    def test_filter_keeps_only_matching_folio(self):
        srcs = [self._fgp(side="1r", content="r1"),
                self._fgp(side="1v", content="v1"),
                self._fgp(side="2r", content="r2")]
        out = filter_sources_for_page(srcs, 1, "1v")
        assert [s["content"] for s in out] == ["v1"]

    def test_filter_whole_doc_plus_foliated(self):
        srcs = [self._fgp(content="whole"),            # whole-doc -> every page
                self._fgp(side="1r", content="r1"),
                self._fgp(side="1v", content="v1")]
        out = filter_sources_for_page(srcs, 1, "1r")
        assert [s["content"] for s in out] == ["whole", "r1"]

    def test_filter_no_folio_label_keeps_all_fgp(self):
        # Backward-compatible 2-arg call: folio unknown -> keep every FGP row.
        srcs = [self._fgp(side="1r", content="r1"),
                self._fgp(side="2v", content="v2")]
        out = filter_sources_for_page(srcs, 1)
        assert {s["content"] for s in out} == {"r1", "v2"}

    def test_pgp_unaffected_by_folio_label(self):
        # folio_label only governs FGP; PGP side-filtering is unchanged.
        pgp = {"source": "pgp", "doc_relation": "Digital Edition",
               "content": "p", "page_info": "verso"}
        assert filter_sources_for_page([pgp], 2, "9r")  # verso page keeps it
        assert filter_sources_for_page([pgp], 1, "9r") == []  # recto drops it


# ── TestFgpImageNumberMapping — exact c_number ↔ fgp_image_number_id key ─


class TestFgpImageNumberMapping:
    """The robust per-image FGP key. Verified against real data: the folio
    LABEL is only coincidentally equal to FGP's image_side and breaks on
    bare-sequence / NULL / duplicate values and multi-volume manuscripts, while
    c_number == fgp_image_number_id is exact (100%) and volume-aware."""

    @staticmethod
    def _fgp(side=None, folio_num=None, page_info=None, content="x", c=None):
        return {
            "source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
            "content": content, "image_side": side, "folio_num": folio_num,
            "page_info": page_info, "fgp_c_number": c, "sections": None,
        }

    # -- _fgp_match_image_number --
    def test_strips_leading_c(self):
        assert _fgp_match_image_number({"fgp_c_number": "C62553"}) == "62553"
        assert _fgp_match_image_number({"fgp_c_number": "c62553"}) == "62553"
        assert _fgp_match_image_number({"c_number": "C421559"}) == "421559"

    def test_no_c_number_is_blank(self):
        assert _fgp_match_image_number({"fgp_c_number": None}) == ""
        assert _fgp_match_image_number({}) == ""

    # -- fgp_image_number_for_displayed_page --
    def test_resolver_reads_fgp_image_number_id(self):
        imgs = [{"folio_label": "1r", "fgp_image_number_id": "62553"},
                {"folio_label": "1v", "fgp_image_number_id": "62554"}]
        assert fgp_image_number_for_displayed_page(imgs, 1, 2) == "62553"
        assert fgp_image_number_for_displayed_page(imgs, 2, 2) == "62554"

    def test_resolver_multi_ie_and_out_of_range(self):
        imgs = [{"fgp_image_number_id": "10"}, {"fgp_image_number_id": "11"},
                {"fgp_image_number_id": "12"}, {"fgp_image_number_id": "13"}]
        assert fgp_image_number_for_displayed_page(imgs, 5, 8) == "10"  # 2nd IE -> img0
        assert fgp_image_number_for_displayed_page(imgs, 9, 8) == ""    # past total
        assert fgp_image_number_for_displayed_page(imgs, 1, 4) == "10"

    def test_resolver_missing_id_is_blank(self):
        assert fgp_image_number_for_displayed_page([{"folio_label": "1r"}], 1, 1) == ""

    # -- fgp_source_for_folio with image_number (preferred key) --
    def test_image_number_preferred_over_label(self):
        # The displayed image's number matches this row's c_number -> show it,
        # even though its (bare-sequence) image_side would NOT match the label.
        row = self._fgp(side="1", c="C69878")           # Geneva: bare-sequence side
        assert fgp_source_for_folio(row, "1r", "69878") is True
        # A different displayed image number -> hidden, despite any label.
        assert fgp_source_for_folio(row, "1r", "99999") is False

    def test_multi_volume_separated_by_number(self):
        # Manchester: two volumes both parse to label '1r'; distinct numbers.
        vol_a = self._fgp(side="1r", c="C421559")
        vol_b = self._fgp(side="1r", c="C421512")
        assert fgp_source_for_folio(vol_a, "1r", "421559") is True
        assert fgp_source_for_folio(vol_b, "1r", "421559") is False   # other volume
        assert fgp_source_for_folio(vol_b, "1r", "421512") is True

    def test_null_side_row_pinned_by_number_not_whole_doc(self):
        # NLI Heb 577: a foliated row whose image_side is NULL. Under the label
        # path it looks whole-doc (every page); the c_number pins it to its image.
        row = self._fgp(side=None, c="C62555")
        assert fgp_source_for_folio(row, "2r", "62555") is True
        assert fgp_source_for_folio(row, "1r", "62553") is False  # NOT on other images

    def test_falls_back_to_label_when_number_unknown(self):
        row = self._fgp(side="1r", c="C62553")
        # Displayed image number unknown ('' / None) -> use the folio label.
        assert fgp_source_for_folio(row, "1r", "") is True
        assert fgp_source_for_folio(row, "1v", None) is False

    def test_no_c_number_uses_label_path(self):
        # Row without c_number can't match by number -> label path (whole-doc here).
        row = self._fgp(content="whole")
        assert fgp_source_for_folio(row, "1r", "62553") is True

    def test_filter_uses_image_number(self):
        srcs = [self._fgp(side="1", c="C69878", content="A"),
                self._fgp(side="2", c="C69879", content="B")]
        # Geneva-style: bare-sequence sides, gallery label '1r' — number selects A.
        out = filter_sources_for_page(srcs, 1, "1r", "69878")
        assert [s["content"] for s in out] == ["A"]


# ── TestFgpSimilarityAlignment — align FGP editions to V0.8 by text ─


class TestFgpSimilarityAlignment:
    """FGP editions align to the displayed V0.8 page by word-overlap (same folio
    shares most words). Safe-by-design: a confident single folio match is never
    overridden; similarity only chooses when the folio result is ambiguous
    (keep-all / unalignable) or a single weak/wrong pick."""

    # Three distinct "folios" with disjoint vocabularies + a shared page text
    # that matches FOLIO B (so B is the correct alignment).
    A = "אלף בית גימל דלת הא וו זין חית טית יוד"
    B = "כף למד מם נון סמך עין פא צדי קוף ריש"
    C = "שין תיו אבן גזר משנה תלמוד גמרא הלכה אגדה פירוש"
    PAGE_B = "כף למד מם נון סמך עין פא צדי קוף ריש שונה"  # ≈ B + 1 word

    @staticmethod
    def _ed(content, c=None, side=None):
        return {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
                "content": content, "fgp_c_number": c, "image_side": side,
                "folio_num": None, "page_info": None, "sections": None}

    def test_token_set_strips_nikud_and_punctuation(self):
        assert _heb_token_set("בְּרֵאשִׁית, בָּרָא") == {"בראשית", "ברא"}
        assert _heb_token_set("") == set()
        assert _heb_token_set("hello 123") == set()

    def test_similarity_high_for_same_folio_low_for_other(self):
        pt = _heb_token_set(self.PAGE_B)
        assert _content_similarity(pt, self.B) > 0.8     # same folio
        assert _content_similarity(pt, self.A) < 0.2     # different folio
        assert _content_similarity(set(), self.B) == 0.0

    def test_confident_single_folio_match_not_overridden(self):
        # Folio filter pinned ONE edition that matches the page well -> trust it
        # (no similarity override -> zero regression), even though another
        # edition exists. Here folio_eds is the CORRECT one.
        eds = [self._ed(self.B), self._ed(self.A)]
        out = _select_fgp_editions_by_similarity(eds, [eds[0]], self.PAGE_B)
        assert out == [eds[0]]

    def test_keep_all_narrowed_to_best_by_similarity(self):
        # Unalignable -> folio filter kept ALL editions; similarity narrows to the
        # one matching the page (the fix for the structural cases).
        eds = [self._ed(self.A), self._ed(self.B), self._ed(self.C)]
        out = _select_fgp_editions_by_similarity(eds, eds, self.PAGE_B)
        assert out == [eds[1]]   # B

    def test_single_wrong_pick_corrected(self):
        # Folio filter pinned the WRONG single edition (A); its similarity to the
        # page is low -> similarity corrects to B (the multi-volume failure mode).
        eds = [self._ed(self.A), self._ed(self.B)]
        out = _select_fgp_editions_by_similarity(eds, [eds[0]], self.PAGE_B)
        assert out == [eds[1]]

    def test_keep_all_picks_single_best_even_when_close(self):
        # Unalignable (folio kept all). Per "one transcription per page", narrow
        # to the SINGLE best match (argmax) rather than show all — even when a
        # sibling is close (continuous-work folios share vocabulary).
        eds = [self._ed(self.B + " מלה אחרת"), self._ed(self.B)]
        out = _select_fgp_editions_by_similarity(eds, eds, self.PAGE_B)
        assert len(out) == 1

    def test_short_page_text_does_not_override(self):
        eds = [self._ed(self.A), self._ed(self.B)]
        out = _select_fgp_editions_by_similarity(eds, [eds[0]], "כף למד")  # < min tokens
        assert out == [eds[0]]

    def test_no_page_text_keeps_folio(self):
        eds = [self._ed(self.A), self._ed(self.B)]
        out = _select_fgp_sources_for_page(eds, folio_label="1r", image_number="", page_text="")
        # No page text -> pure folio match; neither has a folio so both kept.
        assert len(out) == 2

    def test_filter_uses_similarity_when_page_text_given(self):
        srcs = [self._ed(self.A, c="C1"), self._ed(self.B, c="C2"), self._ed(self.C, c="C3")]
        # No folio/image -> folio filter keeps all 3; similarity narrows to B.
        out = filter_sources_for_page(srcs, 1, folio_label="", image_number="",
                                      page_text=self.PAGE_B)
        assert [s["fgp_c_number"] for s in out] == ["C2"]


# ── TestFolioForDisplayedPage — multi-edition (IE) page→folio resolver ─


class TestFolioForDisplayedPage:
    IMGS = [{"folio_label": "1r"}, {"folio_label": "1v"},
            {"folio_label": "2r"}, {"folio_label": "2v"}]

    def test_single_ie_positional(self):
        f = folio_label_for_displayed_page
        assert f(self.IMGS, 1, 4) == "1r"
        assert f(self.IMGS, 3, 4) == "2r"
        assert f(self.IMGS, 4, 4) == "2v"

    def test_multi_ie_modulo_maps_back_onto_folios(self):
        # Two text editions -> 8 pages, same 4 folios repeating. Page 5 is the
        # 2nd edition's 1r; page 7 is its 2r (this is the Add.3207 failure mode).
        f = folio_label_for_displayed_page
        assert f(self.IMGS, 5, 8) == "1r"
        assert f(self.IMGS, 7, 8) == "2r"
        assert f(self.IMGS, 8, 8) == "2v"
        # First edition still correct.
        assert f(self.IMGS, 3, 8) == "2r"

    def test_three_editions(self):
        assert folio_label_for_displayed_page(self.IMGS, 11, 12) == "2r"  # (10 % 4)=2

    def test_page_past_total_is_blank_not_modulo(self):
        # Codex MEDIUM: a page index beyond total_pages is stale/bad — must NOT
        # be folded back onto a folio via the modulo (would show a wrong image).
        f = folio_label_for_displayed_page
        assert f(self.IMGS, 9, 8) == ""    # 9 > total 8 even though 8 % 4 == 0
        assert f(self.IMGS, 13, 12) == ""  # 13 > total 12
        assert f(self.IMGS, 8, 8) == "2v"  # boundary still valid

    def test_unknown_total_falls_back_to_positional(self):
        f = folio_label_for_displayed_page
        assert f(self.IMGS, 3, 0) == "2r"      # in range
        assert f(self.IMGS, 5, 0) == ""        # out of range, no total -> blank

    def test_irregular_total_keeps_all_failsafe(self):
        # total not a clean multiple of the image count -> structurally
        # unalignable (uneven editions / fewer pages than images). FAIL-SAFE:
        # return '' for every page so the caller keeps ALL FGP (chooser shows
        # every transcription) rather than confidently placing the WRONG folio.
        # See docs/OPEN_ISSUES.md "FGP per-folio alignment".
        f = folio_label_for_displayed_page
        assert f(self.IMGS, 2, 5) == ""        # 5 % 4 != 0 -> keep all
        assert f(self.IMGS, 1, 7) == ""        # 7 % 4 != 0 -> keep all
        assert f(self.IMGS, 5, 5) == ""        # out of range
        # Clean multiples still resolve (unchanged).
        assert f(self.IMGS, 2, 4) == "1v"      # single edition
        assert f(self.IMGS, 6, 8) == "1v"      # two editions (6th page -> 2nd ed 1v)

    def test_empty_or_bad_input(self):
        f = folio_label_for_displayed_page
        assert f([], 1, 4) == ""
        assert f(self.IMGS, 0, 4) == ""
        assert f(None, 1, 4) == ""


# ── TestFeatureFlag (FGP-04) ──────────────────────────────────────


class TestFeatureFlag:
    def test_flag_off_returns_empty(self, fgp_svc, monkeypatch):
        monkeypatch.setenv("FGP_TRANSCRIPTIONS_ENABLED", "0")
        assert fgp_svc.get_fgp_sources_for_fragment("003072766") == []
        assert fgp_svc.get_sys_ids_with_fgp_sources(["003072766"]) == set()

    def test_flag_unset_defaults_on(self, fgp_svc, monkeypatch):
        # Default is ON (2026-06-22 go-live): with the env unset, FGP surfaces
        # wherever the sidecar DB is present. Kill-switch is FGP_TRANSCRIPTIONS_ENABLED=0.
        monkeypatch.delenv("FGP_TRANSCRIPTIONS_ENABLED", raising=False)
        assert len(fgp_svc.get_fgp_sources_for_fragment("003072766")) == 2

    def test_flag_truthy_variants(self, fgp_svc, monkeypatch):
        for val in ("1", "true", "TRUE", "yes", "on"):
            monkeypatch.setenv("FGP_TRANSCRIPTIONS_ENABLED", val)
            assert len(fgp_svc.get_fgp_sources_for_fragment("003072766")) == 2


# ── TestServiceUnavailable ────────────────────────────────────────


class TestServiceUnavailable:
    def test_missing_db_degrades(self, tmp_path):
        missing = os.path.join(str(tmp_path), "does_not_exist.db")
        svc = FgpService(db_path=missing)
        assert svc.is_available() is False
        assert svc.get_fgp_sources_for_fragment("003072766") == []
        assert svc.get_sys_ids_with_fgp_sources(["003072766"]) == set()
        svc.close()

    def test_module_wrapper_when_db_absent(self):
        # No fgp_data/ in the repo -> singleton degrades, wrappers return empty.
        assert fgp_service.get_fgp_sources_for_fragment("003072766") == []
        assert fgp_service.get_sys_ids_with_fgp_sources(["003072766"]) == set()


# ── TestBatchLookup ───────────────────────────────────────────────


class TestBatchLookup:
    def test_returns_only_present_sys_ids(self, fgp_svc):
        got = fgp_svc.get_sys_ids_with_fgp_sources(
            ["003072766", "003099001", "000000000"]
        )
        assert got == {"003072766", "003099001"}

    def test_empty_input(self, fgp_svc):
        assert fgp_svc.get_sys_ids_with_fgp_sources([]) == set()


# ── TestDedupeWholeDocAndCredit (FGP-A/B) ─────────────────────────


def _insert_dedupe_data(db_path: str):
    """Rows exercising whole-doc + duplicate-scan + credit + image_side (FGP-B)."""
    conn = sqlite3.connect(db_path)
    cols = ("id, collection, shelfmark, c_number, source_scholar, doc_relation, "
            "language, content, sys_id, page_info, folio_num, image_side, source_credit")
    q = (f"INSERT INTO fgp_transcriptions ({cols}) "
         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    sid = "990000000000000001"
    credit = "יעקב זוסמן, ראש צוות FGP לספרות תלמודית"
    # Whole-document row (no c_number) — should be dropped (page rows exist).
    conn.execute(q, (1, "BL", "OR 1", None, "FGP", "Digital Edition", None,
                     "WHOLE DOC recto+verso", sid, None, None, None, credit))
    # Recto, scan A (shorter) + scan B (longer) — same c_number -> dedup, keep longest.
    conn.execute(q, (2, "BL", "OR 1", "C100", "FGP", "Digital Edition", None,
                     "recto short", sid, "recto", 1, "1r", credit))
    conn.execute(q, (3, "BL", "OR 1", "C100", "FGP", "Digital Edition", None,
                     "recto LONGER content here", sid, "recto", 1, "1r", credit))
    # Verso, single row.
    conn.execute(q, (4, "BL", "OR 1", "C101", "FGP", "Digital Edition", None,
                     "verso content", sid, "verso", 1, "1v", credit))
    # A second sys_id with ONLY a whole-doc row -> kept (sole source).
    conn.execute(q, (5, "CUL", "Add.1", None, "FGP", "Digital Edition", None,
                     "cul whole doc only", "990000000000000002", None, None, None, None))
    # A multi-folio sys_id inserted OUT of folio order -> chooser must sort to
    # FGP file order 1r, 1v, 2r, 2v (FGP-B per-image navigation).
    sid3 = "990000000000000003"
    conn.execute(q, (10, "BL", "OR 9", "C200", "FGP", "Digital Edition", None,
                     "f2 verso", sid3, "verso", 2, "2v", None))
    conn.execute(q, (11, "BL", "OR 9", "C201", "FGP", "Digital Edition", None,
                     "f1 recto", sid3, "recto", 1, "1r", None))
    conn.execute(q, (12, "BL", "OR 9", "C202", "FGP", "Digital Edition", None,
                     "f2 recto", sid3, "recto", 2, "2r", None))
    conn.execute(q, (13, "BL", "OR 9", "C203", "FGP", "Digital Edition", None,
                     "f1 verso", sid3, "verso", 1, "1v", None))
    conn.commit()
    conn.close()


@pytest.fixture
def dedupe_svc(tmp_path):
    db_path = _create_fgp_db(str(tmp_path))
    _insert_dedupe_data(db_path)
    svc = FgpService(db_path=db_path)
    yield svc
    svc.close()


class TestDedupeWholeDocAndCredit:
    def test_whole_doc_row_dropped_when_page_rows_exist(self, dedupe_svc):
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000001")
        # whole-doc id=1 dropped; C100 deduped to one; C101 kept -> 2 sources.
        assert len(srcs) == 2
        assert all(s["fgp_c_number"] for s in srcs)
        assert "WHOLE DOC recto+verso" not in [s["content"] for s in srcs]

    def test_duplicate_scan_keeps_longest(self, dedupe_svc):
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000001")
        c100 = [s for s in srcs if s["fgp_c_number"] == "C100"]
        assert len(c100) == 1
        assert c100[0]["content"] == "recto LONGER content here"

    def test_whole_doc_only_sys_id_is_kept(self, dedupe_svc):
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000002")
        assert len(srcs) == 1
        assert srcs[0]["content"] == "cul whole doc only"

    def test_folio_label_from_image_side(self, dedupe_svc):
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000001")
        labels = {s["folio_label"] for s in srcs}
        assert labels == {"1r", "1v"}

    def test_source_credit_exposed(self, dedupe_svc):
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000001")
        assert all(s["source_credit"] == "יעקב זוסמן, ראש צוות FGP לספרות תלמודית"
                   for s in srcs)

    def test_multi_folio_sorted_to_fgp_file_order(self, dedupe_svc):
        # Rows inserted as 2v,1r,2r,1v must surface as 1r,1v,2r,2v (FGP-B).
        srcs = dedupe_svc.get_fgp_sources_for_fragment("990000000000000003")
        assert [s["folio_label"] for s in srcs] == ["1r", "1v", "2r", "2v"]

    def test_relation_kind_compound_is_edition(self):
        # Codex LOW-1: compound "Edition ; Translation" -> edition (desktop parity)
        assert source_relation_kind(
            {"doc_relation": "Digital Edition ; Digital Translation"}) == "edition"
        assert source_relation_kind({"doc_relation": "Digital Translation"}) == "translation"

    def test_folio_label_falls_back_to_cnumber(self):
        # Codex MEDIUM-1: no side/folio -> use the FGP c_number, not "" (identical labels)
        from shared.fgp_service import _fgp_folio_label
        assert _fgp_folio_label({"c_number": "C520386"}) == "C520386"
        assert _fgp_folio_label({"image_side": "1r", "c_number": "C1"}) == "1r"

    def test_whole_doc_translation_kept_across_language(self):
        # Codex MEDIUM-3: a c-numbered Hebrew translation must NOT drop a whole-doc
        # English translation (both are 'Digital Translation').
        srcs = [
            {"fgp_c_number": "C1", "doc_relation": "Digital Translation",
             "language": "Hebrew", "content": "he"},
            {"fgp_c_number": None, "doc_relation": "Digital Translation",
             "language": "English", "content": "en whole"},
        ]
        out = dedupe_fgp_sources(srcs)
        assert {s["language"] for s in out} == {"Hebrew", "English"}

    def test_filter_keeps_all_fgp_rows_full(self):
        # Codex HIGH-1: FGP is per-image; NOT side-filtered. Both rows survive on
        # page 2 (verso) with full content (no recto/verso narrowing).
        fgp = [
            {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
             "page_info": "recto", "content": "R", "fgp_c_number": "C1"},
            {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
             "page_info": "verso", "content": "V", "fgp_c_number": "C2"},
        ]
        out = filter_sources_for_page(list(fgp), 2)
        assert {s["content"] for s in out} == {"R", "V"}

    def test_parse_datasource_order_independent_and_multi(self):
        # Codex LOW-2: parse eng/heb in either order; aggregate multi-source.
        from scripts.fgp_rebuild_text_and_credit import _parse_datasource
        assert _parse_datasource("{heb: שלום, eng: hello}") == "שלום"
        assert _parse_datasource("{eng: hello, heb: שלום}") == "שלום"
        assert _parse_datasource("{eng: A, heb: A}, {eng: B, heb: B}") == "A; B"

    def test_dedupe_pure_function_preserves_order(self):
        rows = [
            {"fgp_c_number": "C2", "doc_relation": "Digital Edition", "content": "b"},
            {"fgp_c_number": "C1", "doc_relation": "Digital Edition", "content": "a"},
            {"fgp_c_number": "C1", "doc_relation": "Digital Edition", "content": "aaa"},
        ]
        out = dedupe_fgp_sources(rows)
        assert [s["fgp_c_number"] for s in out] == ["C2", "C1"]
        assert out[1]["content"] == "aaa"  # longest kept
