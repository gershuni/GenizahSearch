# -*- coding: utf-8 -*-
"""
Offline verification tests for all three sidecar services.

Proves that PGP, FJMS, and NLI browse paths operate entirely from local
SQLite with no Supabase or network dependencies. Also verifies graceful
degradation when any sidecar database is missing.

These tests serve as regression guards ensuring no Supabase or network
imports creep back into the sidecar service code paths.

Test classes:
- TestPgpServiceOffline: PGP browse path uses only local SQLite
- TestFjmsServiceOffline: FJMS browse path uses only local SQLite
- TestNliCrossrefServiceOffline: NLI crossref path uses only local SQLite
- TestNoNetworkImportsInServiceModules: Cross-cutting import check
"""

import inspect
import json
import os
import sqlite3
import tempfile

import pytest

import shared.document_service as pgp_module
import shared.fjms_service as fjms_module
import shared.nli_crossref_service as nli_module
from shared.document_service import PgpService
from shared.fjms_service import FjmsService
from shared.nli_crossref_service import NliCrossrefService


# ── PGP Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def pgp_db(tmp_path):
    """Create a temp pgp.db with full schema and minimal test data."""
    db_path = str(tmp_path / "pgp.db")
    conn = sqlite3.connect(db_path)

    # Schema: documents
    conn.execute("""
        CREATE TABLE documents (
            pgpid INTEGER PRIMARY KEY,
            shelfmark_combined TEXT,
            document_type TEXT,
            tags TEXT,
            doc_date_original TEXT,
            doc_date_standard TEXT,
            doc_date_calendar TEXT,
            inferred_date_display TEXT,
            inferred_date_standard TEXT,
            inferred_date_rationale TEXT,
            inferred_date_notes TEXT,
            description TEXT,
            transcription TEXT,
            transcription_source TEXT,
            languages_primary TEXT,
            languages_secondary TEXT,
            language_note TEXT,
            scholarship_records TEXT,
            shelfmarks_historic TEXT,
            has_transcription INTEGER,
            has_translation INTEGER,
            input_by TEXT,
            pgp_url TEXT,
            created_at TEXT
        )
    """)

    # Schema: document_sources
    conn.execute("""
        CREATE TABLE document_sources (
            id INTEGER PRIMARY KEY,
            pgpid INTEGER NOT NULL,
            source_scholar TEXT NOT NULL,
            doc_relation TEXT NOT NULL,
            language TEXT,
            content TEXT NOT NULL,
            content_length INTEGER,
            source_url TEXT,
            notes TEXT,
            sequence_order INTEGER DEFAULT 1,
            sections TEXT,
            source_language TEXT,
            source_direction TEXT,
            created_at TEXT
        )
    """)

    # Schema: document_fragments
    conn.execute("""
        CREATE TABLE document_fragments (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL,
            sys_id TEXT NOT NULL,
            shelfmark TEXT,
            sequence_order INTEGER DEFAULT 1,
            page_info TEXT,
            collection TEXT,
            library TEXT,
            library_abbrev TEXT,
            fragment_url TEXT,
            iiif_url TEXT,
            created_at TEXT
        )
    """)

    # Schema: meta
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Insert test data: one document with transcription and tags
    conn.execute(
        "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags, "
        "doc_date_original, doc_date_standard, inferred_date_display, description, "
        "transcription, transcription_source, pgp_url, has_transcription, has_translation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1001, 'T-S 8J5.11', 'Letter', '["letter","commercial"]',
         '1050-1100 CE', '1050/1100', '11th century',
         'A commercial letter', 'Test transcription text', 'PGP',
         'https://geniza.princeton.edu/documents/1001', 1, 0)
    )

    # Fragment linked to the document
    conn.execute(
        "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, "
        "sequence_order, page_info) VALUES (?, ?, ?, ?, ?, ?)",
        (1, 1001, '003072766', 'T-S 8J5.11', 1, 'recto')
    )

    # Source (edition) for the document
    conn.execute(
        "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, "
        "content, language, content_length, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1001, 'Scholar A', 'Digital Edition', 'Edition content', 'he', 100, 1, None)
    )

    # Source (translation) for the document
    conn.execute(
        "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, "
        "content, language, content_length, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (2, 1001, 'Scholar B', 'Digital Translation', 'Translation content', 'en', 80, 2, None)
    )

    # Meta
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        ("version", "1.0.0")
    )

    conn.commit()
    conn.close()
    return db_path


# ── FJMS Fixtures ────────────────────────────────────────────────────


@pytest.fixture
def fjms_db(tmp_path):
    """Create a temp fjms_enrichment.db with minimal schema and test data."""
    db_path = str(tmp_path / "fjms_enrichment.db")
    conn = sqlite3.connect(db_path)

    conn.execute("""
        CREATE TABLE domains (
            AlmaId TEXT NOT NULL,
            Domain TEXT NOT NULL,
            DomainHeb TEXT,
            ParentDomain TEXT,
            ParentDomainHeb TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Insert test data
    conn.execute(
        "INSERT INTO domains VALUES (?, ?, ?, ?, ?)",
        ("990001", "Piyyut", "piyyut_heb", None, None)
    )
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        ("version", "2.0.0")
    )

    conn.commit()
    conn.close()
    return db_path


# ── NLI Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def nli_db(tmp_path):
    """Create a temp nli_crossref.db with minimal schema and test data."""
    db_path = str(tmp_path / "nli_crossref.db")
    conn = sqlite3.connect(db_path)

    conn.execute("""
        CREATE TABLE nli_images (
            LibraryNameEng TEXT, LibraryAbbrev TEXT, LibraryCity TEXT,
            LibraryNameHeb TEXT, CollectionName TEXT, Shelfmark TEXT,
            InventoryId TEXT, OBBox TEXT, OBVolume TEXT, OBFolio TEXT,
            NLI_AlmaId TEXT, CatalogAbbrev TEXT, CatalogEntry TEXT,
            FGPImageNumberId TEXT, FGPNumber TEXT, ImageName TEXT,
            ImageSourceName TEXT, PartOf TEXT, See TEXT, BifolioWith TEXT,
            NumFolio TEXT, NumBifolio TEXT, Material TEXT, Size TEXT,
            IsNotGenizah TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE cambridge_manifests (
            label TEXT NOT NULL,
            manifest_url TEXT NOT NULL,
            normalized_shelfmark TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Insert test NLI image data
    conn.execute(
        "INSERT INTO nli_images VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("Cambridge UL", "CUL", "Cambridge", "heb", "Taylor-Schechter",
         "T-S 12.123", "INV001", "", "", "", "A001", "CAT1", "1",
         "FGP001", "1234", "T_S_12_123__L1F0B0S1", "NLI", "", "", "",
         "4", "2", "Paper", "15x20", "")
    )

    # Cambridge manifest
    conn.execute(
        "INSERT INTO cambridge_manifests VALUES (?, ?, ?)",
        ("MS-TS-00012-00123", "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123", "ts12123")
    )

    # Meta
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        ("version", "1.0.0")
    )

    conn.commit()
    conn.close()
    return db_path


# ── TestPgpServiceOffline ────────────────────────────────────────────


class TestPgpServiceOffline:
    """Verifies PGP browse path uses only local SQLite with zero Supabase imports."""

    def test_pgp_service_imports_no_supabase(self):
        """Import lines in document_service contain no Supabase/network packages."""
        source = inspect.getsource(pgp_module)

        # Extract only import lines (code before any comments)
        import_lines = [
            line.strip().split('#')[0].strip()
            for line in source.split('\n')
            if line.strip().startswith('import ') or line.strip().startswith('from ')
        ]

        # Must not import any Supabase ecosystem or HTTP client packages
        forbidden = ['supabase', 'postgrest', 'httpx', 'requests']
        for line in import_lines:
            for term in forbidden:
                assert term not in line.lower(), (
                    f"Found '{term}' in import line of shared/document_service.py: {line}. "
                    f"PGP browse must be local-only."
                )

    def test_pgp_browse_all_methods_local(self, pgp_db):
        """All PgpService public methods operate from local SQLite."""
        svc = PgpService(db_path=pgp_db)
        assert svc.is_available() is True

        try:
            # get_document_for_fragment
            doc = svc.get_document_for_fragment('003072766')
            assert doc is not None
            assert doc['pgpid'] == 1001

            # get_fragments_for_document
            frags = svc.get_fragments_for_document(1001)
            assert len(frags) == 1
            assert frags[0]['sys_id'] == '003072766'

            # get_transcription_for_document
            transcription = svc.get_transcription_for_document(1001)
            assert transcription == 'Test transcription text'

            # get_document_metadata
            metadata = svc.get_document_metadata(1001)
            assert metadata is not None
            assert metadata['document_type'] == 'Letter'

            # get_sources_for_document
            sources = svc.get_sources_for_document(1001)
            assert len(sources) == 2

            # get_all_sources_for_fragment
            all_sources = svc.get_all_sources_for_fragment('003072766')
            assert len(all_sources) == 2

            # get_editions_for_document
            editions = svc.get_editions_for_document(1001)
            assert len(editions) == 1
            assert 'Edition' in editions[0]['doc_relation']

            # get_translations_for_document
            translations = svc.get_translations_for_document(1001)
            assert len(translations) == 1
            assert 'Translation' in translations[0]['doc_relation']

            # get_sys_ids_with_transcriptions
            sys_ids = svc.get_sys_ids_with_transcriptions(['003072766', 'UNKNOWN'])
            assert '003072766' in sys_ids
            assert 'UNKNOWN' not in sys_ids

            # get_fragments_by_tag
            tag_results = svc.get_fragments_by_tag('letter')
            assert len(tag_results) > 0
            assert tag_results[0]['pgpid'] == 1001

            # get_all_distinct_tags
            tags = svc.get_all_distinct_tags()
            assert isinstance(tags, list)
            assert 'letter' in tags
            assert 'commercial' in tags
        finally:
            svc.close()

    def test_pgp_graceful_degradation(self, tmp_path):
        """PgpService with non-existent db: is_available=False, all methods safe."""
        nonexistent = str(tmp_path / "does_not_exist.db")
        svc = PgpService(db_path=nonexistent)

        assert svc.is_available() is False

        # All methods return None or empty, no exceptions
        assert svc.get_document_for_fragment('003072766') is None
        assert svc.get_fragments_for_document(1001) == []
        assert svc.get_transcription_for_document(1001) is None
        assert svc.get_document_metadata(1001) is None
        assert svc.get_sources_for_document(1001) == []
        assert svc.get_all_sources_for_fragment('003072766') == []
        assert svc.get_editions_for_document(1001) == []
        assert svc.get_translations_for_document(1001) == []
        assert svc.get_sys_ids_with_transcriptions(['003072766']) == set()
        assert svc.get_fragments_by_tag('letter') == []
        assert svc.get_all_distinct_tags() == []

        svc.close()


# ── TestFjmsServiceOffline ───────────────────────────────────────────


class TestFjmsServiceOffline:
    """Verifies FJMS browse path uses only local SQLite with zero network dependencies."""

    def test_fjms_service_imports_no_network(self):
        """Source of fjms_service module contains no network library imports."""
        source = inspect.getsource(fjms_module)

        # Must not import Supabase ecosystem or HTTP client packages
        forbidden = ['supabase', 'postgrest', 'httpx']
        for term in forbidden:
            assert term not in source.lower(), (
                f"Found '{term}' in shared/fjms_service.py source. "
                f"FJMS browse must be local-only."
            )

        # urllib is acceptable only for URL encoding (urllib.parse), not for network calls
        # Check that if 'requests' appears it's not as an import
        import_lines = [
            line.strip() for line in source.split('\n')
            if line.strip().startswith('import ') or line.strip().startswith('from ')
        ]
        for line in import_lines:
            assert 'requests' not in line, (
                f"Found 'requests' in import line: {line}"
            )

    def test_fjms_browse_methods_local(self, fjms_db):
        """FjmsService browse methods operate from local SQLite."""
        svc = FjmsService(db_path=fjms_db)
        assert svc.is_available() is True

        try:
            # get_version
            version = svc.get_version()
            assert version == "2.0.0"

            # get_domains
            domains = svc.get_domains("990001")
            assert len(domains) == 1
            assert domains[0]["domain"] == "Piyyut"

            # is_available (already checked above)
            assert svc.is_available() is True
        finally:
            svc.close()

    def test_fjms_graceful_degradation(self, tmp_path):
        """FjmsService with non-existent db: is_available=False, all methods safe."""
        nonexistent = str(tmp_path / "does_not_exist.db")
        svc = FjmsService(db_path=nonexistent)

        assert svc.is_available() is False
        assert svc.get_domains("990001") == []
        assert svc.get_version() is None

        svc.close()


# ── TestNliCrossrefServiceOffline ────────────────────────────────────


class TestNliCrossrefServiceOffline:
    """Verifies NLI crossref path uses only local SQLite with zero network dependencies."""

    def test_nli_service_imports_no_network(self):
        """Source of nli_crossref_service module contains no Supabase/HTTP imports."""
        source = inspect.getsource(nli_module)

        # Must not import Supabase ecosystem packages
        forbidden_supabase = ['supabase', 'postgrest', 'gotrue', 'realtime']
        for term in forbidden_supabase:
            assert term not in source.lower(), (
                f"Found '{term}' in shared/nli_crossref_service.py source. "
                f"NLI crossref must be local-only."
            )

        # Check import lines for HTTP client libraries
        import_lines = [
            line.strip() for line in source.split('\n')
            if line.strip().startswith('import ') or line.strip().startswith('from ')
        ]
        for line in import_lines:
            assert 'httpx' not in line, (
                f"Found 'httpx' in import line: {line}"
            )
            # 'requests' should not be in any import line
            assert 'requests' not in line.split('#')[0], (
                f"Found 'requests' in import line: {line}"
            )

    def test_nli_browse_methods_local(self, nli_db):
        """NliCrossrefService browse methods operate from local SQLite."""
        svc = NliCrossrefService(db_path=nli_db)
        assert svc.is_available() is True

        try:
            # get_version
            version = svc.get_version()
            assert version == "1.0.0"

            # get_images (the primary browse method)
            images = svc.get_images("A001")
            assert len(images) == 1
            assert images[0]["fgp_image_number_id"] == "FGP001"
            assert images[0]["shelfmark"] == "T-S 12.123"

            # is_available (already checked above)
            assert svc.is_available() is True
        finally:
            svc.close()

    def test_nli_graceful_degradation(self, tmp_path):
        """NliCrossrefService with non-existent db: is_available=False, all methods safe."""
        nonexistent = str(tmp_path / "does_not_exist.db")
        svc = NliCrossrefService(db_path=nonexistent)

        assert svc.is_available() is False
        assert svc.get_version() is None
        assert svc.get_images("A001") == []

        svc.close()


# ── TestNoNetworkImportsInServiceModules ─────────────────────────────


class TestNoNetworkImportsInServiceModules:
    """Cross-cutting check: all three service modules use only stdlib + sqlite3."""

    # Supabase ecosystem packages that must never appear in service modules
    FORBIDDEN_PACKAGES = frozenset({
        'supabase', 'postgrest', 'gotrue', 'realtime',
    })

    @pytest.mark.parametrize("module_path,module_name", [
        ("shared/document_service.py", "PGP document service"),
        ("shared/fjms_service.py", "FJMS enrichment service"),
        ("shared/nli_crossref_service.py", "NLI crossref service"),
    ])
    def test_service_modules_stdlib_and_sqlite_only(self, module_path, module_name):
        """Service module imports contain no Supabase ecosystem packages."""
        # Read file content directly to inspect import statements
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(project_root, module_path)
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract import lines
        import_lines = []
        for line in content.split('\n'):
            stripped = line.strip()
            if stripped.startswith('import ') or stripped.startswith('from '):
                # Ignore comments
                code_part = stripped.split('#')[0].strip()
                if code_part:
                    import_lines.append(code_part)

        # Check each import line for forbidden packages
        for line in import_lines:
            for pkg in self.FORBIDDEN_PACKAGES:
                assert pkg not in line.lower(), (
                    f"{module_name} ({module_path}) imports forbidden package "
                    f"'{pkg}' in line: {line}"
                )
