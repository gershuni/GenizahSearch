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
    get_fgp_section_for_page,
    namespaced_source_id,
    reset_fgp_service,
    source_provider,
    source_relation_kind,
)


# ── Schema + fixture helpers ──────────────────────────────────────

# Assumed FGP schema: mirrors PGP document_sources, flattened (sys_id/page_info/
# fgp_c_number live on the row; no document_fragments join).
FGP_SOURCES_DDL = """
CREATE TABLE document_sources (
    id INTEGER PRIMARY KEY,
    sys_id TEXT,
    fgp_c_number TEXT,
    page_info TEXT,
    source_scholar TEXT,
    doc_relation TEXT,
    language TEXT,
    content TEXT,
    content_length INTEGER,
    sequence_order INTEGER DEFAULT 1,
    sections TEXT
)
"""


def _create_fgp_db(tmp_path_str: str, table_ddl: str = FGP_SOURCES_DDL) -> str:
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

    # Row 1: edition with structured page_num sections (recto + verso), sys_id A
    conn.execute(
        "INSERT INTO document_sources (id, sys_id, fgp_c_number, page_info, "
        "source_scholar, doc_relation, language, content, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, "003072766", "C12345", None, "FGP", "Digital Edition", "he",
         "recto side text\nverso side text", 1,
         '[{"page_num": 1, "text": "recto side text"}, '
         '{"page_num": 2, "text": "verso side text"}]'),
    )
    # Row 2: edition, verso-only via page_info, NO sections, sys_id A
    conn.execute(
        "INSERT INTO document_sources (id, sys_id, fgp_c_number, page_info, "
        "source_scholar, doc_relation, language, content, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (2, "003072766", "C12346", "verso", "FGP", "Digital Edition", "he",
         "only verso content here", 2, None),
    )
    # Row 3: edition, NO sections, NO page_info, sys_id B
    conn.execute(
        "INSERT INTO document_sources (id, sys_id, fgp_c_number, page_info, "
        "source_scholar, doc_relation, language, content, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (3, "003099001", "C20000", None, "FGP", "Digital Edition", "jrb",
         "unsided fragment text", 1, None),
    )
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
    def test_discovers_document_sources_table(self, fgp_svc):
        assert fgp_svc.is_available()
        assert fgp_svc._table == "document_sources"
        assert "sys_id" in fgp_svc._columns

    def test_discovers_alternate_table_name(self, tmp_path):
        ddl = FGP_SOURCES_DDL.replace("document_sources", "fgp_sources")
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

    def test_ordered_by_sequence(self, fgp_svc):
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
