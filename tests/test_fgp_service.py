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
    filter_sources_for_page,
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
    folio_num INTEGER
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

    def test_fgp_kept_only_on_its_side(self):
        # FGP verso-only (no sections, page_info=verso)
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "verso text", "page_info": "verso", "sections": None}
        assert filter_sources_for_page([fgp], 2)[0]["content"] == "verso text"
        assert filter_sources_for_page([fgp], 1) == []  # not duplicated on recto

    def test_mixed_list_order_and_split(self):
        pgp = {"source": "pgp", "doc_relation": "Digital Edition",
               "content": "p-recto", "page_info": "recto"}
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "f-unsided", "page_info": None, "sections": None}
        out = filter_sources_for_page([pgp, fgp], 1)
        # both present on recto, original order
        assert [s["content"] for s in out] == ["p-recto", "f-unsided"]
        # verso: pgp(recto) dropped, fgp(no page_info) defaults recto-only -> dropped
        assert filter_sources_for_page([pgp, fgp], 2) == []

    def test_fgp_dict_not_mutated(self):
        fgp = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition",
               "content": "orig", "page_info": "recto", "sections": None}
        filter_sources_for_page([fgp], 1)
        assert fgp["content"] == "orig"  # copied, not clobbered


# ── TestFeatureFlag (FGP-04) ──────────────────────────────────────


class TestFeatureFlag:
    def test_flag_off_returns_empty(self, fgp_svc, monkeypatch):
        monkeypatch.setenv("FGP_TRANSCRIPTIONS_ENABLED", "0")
        assert fgp_svc.get_fgp_sources_for_fragment("003072766") == []
        assert fgp_svc.get_sys_ids_with_fgp_sources(["003072766"]) == set()

    def test_flag_unset_defaults_off(self, fgp_svc, monkeypatch):
        monkeypatch.delenv("FGP_TRANSCRIPTIONS_ENABLED", raising=False)
        assert fgp_svc.get_fgp_sources_for_fragment("003072766") == []

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
