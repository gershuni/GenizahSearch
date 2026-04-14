# -*- coding: utf-8 -*-
"""
Tests for PgpService (document_service) using real SQLite fixtures.

Tests cover all PgpService methods using in-memory SQLite databases --
no Supabase mocks. Each test class gets a fresh temp database with
controlled test data.

Test classes:
- TestGetDocumentForFragment: Fragment->document lookup + page selection
- TestGetFragmentsForDocument: Document->fragments ordering
- TestGetTranscriptionForDocument: Transcription retrieval + edge cases
- TestGetDocumentMetadata: Metadata fields + tags deserialization
- TestJsonDeserialization: SQLite TEXT -> Python list/dict round-trip
- TestTagSearch: json_each tag search + distinct tags
- TestBatchLookup: Batch sys_id transcription check
- TestSourceQueries: Sources, editions, translations, all-for-fragment
- TestServiceUnavailable: Graceful degradation with missing db
"""

import os
import sqlite3

import pytest

from shared.document_service import PgpService


# ── Schema + fixture helpers ──────────────────────────────────────


DOCUMENTS_DDL = """
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
"""

DOCUMENT_SOURCES_DDL = """
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
"""

DOCUMENT_FRAGMENTS_DDL = """
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
"""


def _create_test_db(tmp_path_str: str) -> str:
    """Create a test SQLite database with schema at given path. Returns path."""
    db_path = os.path.join(tmp_path_str, "test_pgp.db")
    conn = sqlite3.connect(db_path)
    conn.execute(DOCUMENTS_DDL)
    conn.execute(DOCUMENT_SOURCES_DDL)
    conn.execute(DOCUMENT_FRAGMENTS_DDL)
    conn.commit()
    conn.close()
    return db_path


def _insert_sample_data(db_path: str):
    """Insert standard sample data for most tests."""
    conn = sqlite3.connect(db_path)

    # Document 1234: commercial letter with tags
    conn.execute(
        "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags, "
        "doc_date_original, doc_date_standard, inferred_date_display, description, "
        "transcription, transcription_source, pgp_url, has_transcription, has_translation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1234, 'T-S 8J5.11', 'Letter', '["letter","commercial"]',
         '1050-1100 CE', '1050/1100', '11th century',
         'A commercial letter', 'Test transcription text', 'PGP',
         'https://geniza.princeton.edu/documents/1234', 1, 0)
    )

    # Document 5678: legal document with different tags
    conn.execute(
        "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags, "
        "doc_date_original, doc_date_standard, inferred_date_display, description, "
        "transcription, transcription_source, pgp_url, has_transcription, has_translation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (5678, 'T-S 10J6.3', 'Legal', '["legal","marriage"]',
         '1100-1150 CE', '1100/1150', '12th century',
         'A marriage contract', 'Legal transcription text', 'PGP',
         'https://geniza.princeton.edu/documents/5678', 1, 0)
    )

    # Fragments for document 1234
    conn.execute(
        "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (1, 1234, '003072766', 'T-S 8J5.11', 1, 'recto')
    )
    conn.execute(
        "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (2, 1234, '003072767', 'T-S 8J5.12', 2, 'verso')
    )
    conn.execute(
        "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (3, 1234, '003072768', 'T-S 8J5.13', 3, None)
    )

    # Fragment for document 5678
    conn.execute(
        "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (4, 5678, '003099001', 'T-S 10J6.3', 1, 'recto')
    )

    # Sources for document 1234
    conn.execute(
        "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, "
        "content, language, content_length, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1234, 'Scholar A', 'Digital Edition', 'Edition content here', 'he', 100, 1,
         '[{"canvas_num": 1, "text": "recto text"}]')
    )
    conn.execute(
        "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, "
        "content, language, content_length, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (2, 1234, 'Scholar B', 'Digital Translation', 'Translation content', 'en', 80, 2, None)
    )

    # Source for document 5678
    conn.execute(
        "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, "
        "content, language, content_length, sequence_order, sections) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (3, 5678, 'Scholar C', 'Digital Edition', 'Legal edition', 'he', 60, 1, None)
    )

    conn.commit()
    conn.close()


@pytest.fixture
def pgp_service(tmp_path):
    """Create a PgpService with a temp SQLite database populated with sample data."""
    db_path = _create_test_db(str(tmp_path))
    _insert_sample_data(db_path)
    svc = PgpService(db_path=db_path)
    yield svc
    svc.close()


@pytest.fixture
def empty_pgp_service(tmp_path):
    """Create a PgpService with an empty temp SQLite database (schema only)."""
    db_path = _create_test_db(str(tmp_path))
    svc = PgpService(db_path=db_path)
    yield svc
    svc.close()


# ── TestGetDocumentForFragment ────────────────────────────────────


class TestGetDocumentForFragment:
    """Tests for get_document_for_fragment method."""

    def test_get_document_for_fragment_found(self, pgp_service):
        """Should return document when fragment is linked."""
        result = pgp_service.get_document_for_fragment('003072766')

        assert result is not None
        assert result['pgpid'] == 1234
        assert result['shelfmark_combined'] == 'T-S 8J5.11'
        assert result['document_type'] == 'Letter'

    def test_get_document_for_fragment_not_found(self, pgp_service):
        """Should return None when fragment is not linked."""
        result = pgp_service.get_document_for_fragment('999999999')
        assert result is None

    def test_get_document_for_fragment_empty_id(self, pgp_service):
        """Should return None for empty sys_id."""
        assert pgp_service.get_document_for_fragment('') is None
        assert pgp_service.get_document_for_fragment(None) is None

    def test_get_document_for_fragment_page_selection(self, tmp_path):
        """Page_num selects correct document when multiple PGP docs share a sys_id."""
        db_path = _create_test_db(str(tmp_path))
        conn = sqlite3.connect(db_path)

        # Two documents
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags, "
            "transcription, has_transcription) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (100, 'T-S 1.1', 'Letter', '[]', 'Recto text', 1)
        )
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags, "
            "transcription, has_transcription) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (200, 'T-S 1.1', 'Letter', '[]', 'Verso text', 1)
        )

        # Same sys_id, different page_info, different document_id
        conn.execute(
            "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (10, 100, 'SYS001', 'T-S 1.1', 1, 'recto')
        )
        conn.execute(
            "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (11, 200, 'SYS001', 'T-S 1.1', 1, 'verso')
        )
        conn.commit()
        conn.close()

        svc = PgpService(db_path=db_path)
        try:
            # page_num=1 should select recto -> pgpid 100
            result = svc.get_document_for_fragment('SYS001', page_num=1)
            assert result is not None
            assert result['pgpid'] == 100

            # page_num=2 should select verso -> pgpid 200
            result = svc.get_document_for_fragment('SYS001', page_num=2)
            assert result is not None
            assert result['pgpid'] == 200
        finally:
            svc.close()


# ── TestGetFragmentsForDocument ───────────────────────────────────


class TestGetFragmentsForDocument:
    """Tests for get_fragments_for_document method."""

    def test_get_fragments_for_document_found(self, pgp_service):
        """Should return ordered list of fragments."""
        result = pgp_service.get_fragments_for_document(1234)

        assert len(result) == 3
        assert result[0]['sequence_order'] == 1
        assert result[1]['sequence_order'] == 2
        assert result[2]['sequence_order'] == 3
        assert result[0]['shelfmark'] == 'T-S 8J5.11'

    def test_get_fragments_for_document_empty(self, pgp_service):
        """Should return empty list when no fragments found."""
        result = pgp_service.get_fragments_for_document(9999)
        assert result == []

    def test_get_fragments_for_document_empty_pgpid(self, pgp_service):
        """Should return empty list for empty pgpid."""
        assert pgp_service.get_fragments_for_document(0) == []
        assert pgp_service.get_fragments_for_document(None) == []


# ── TestGetTranscriptionForDocument ───────────────────────────────


class TestGetTranscriptionForDocument:
    """Tests for get_transcription_for_document method."""

    def test_get_transcription_found(self, pgp_service):
        """Should return transcription string when found."""
        result = pgp_service.get_transcription_for_document(1234)
        assert result == 'Test transcription text'

    def test_get_transcription_not_found(self, pgp_service):
        """Should return None when document not found."""
        result = pgp_service.get_transcription_for_document(9999)
        assert result is None

    def test_get_transcription_empty_string(self, tmp_path):
        """Should return None when transcription is empty string."""
        db_path = _create_test_db(str(tmp_path))
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, transcription) "
            "VALUES (?, ?, ?)",
            (999, 'T-S test', '')
        )
        conn.commit()
        conn.close()

        svc = PgpService(db_path=db_path)
        try:
            result = svc.get_transcription_for_document(999)
            assert result is None
        finally:
            svc.close()

    def test_get_transcription_empty_pgpid(self, pgp_service):
        """Should return None for empty pgpid."""
        assert pgp_service.get_transcription_for_document(0) is None
        assert pgp_service.get_transcription_for_document(None) is None


# ── TestGetDocumentMetadata ───────────────────────────────────────


class TestGetDocumentMetadata:
    """Tests for get_document_metadata method."""

    def test_get_document_metadata_found(self, pgp_service):
        """Should return metadata dict when document found."""
        result = pgp_service.get_document_metadata(1234)

        assert result is not None
        assert 'document_type' in result
        assert 'pgp_url' in result
        assert result['document_type'] == 'Letter'
        assert isinstance(result['tags'], list)
        assert 'letter' in result['tags']

    def test_get_document_metadata_not_found(self, pgp_service):
        """Should return None when document not found."""
        result = pgp_service.get_document_metadata(9999)
        assert result is None

    def test_get_document_metadata_empty_pgpid(self, pgp_service):
        """Should return None for empty pgpid."""
        assert pgp_service.get_document_metadata(0) is None
        assert pgp_service.get_document_metadata(None) is None


# ── TestJsonDeserialization ───────────────────────────────────────


class TestJsonDeserialization:
    """Tests for SQLite TEXT -> Python list/dict JSON round-trip."""

    def test_tags_returned_as_list(self, pgp_service):
        """Tags stored as TEXT JSON should be returned as a Python list."""
        doc = pgp_service.get_document_metadata(1234)
        assert doc is not None
        assert isinstance(doc['tags'], list)
        assert doc['tags'] == ['letter', 'commercial']

    def test_tags_null_returns_none(self, tmp_path):
        """Document with NULL tags should return None for tags."""
        db_path = _create_test_db(str(tmp_path))
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags) "
            "VALUES (?, ?, ?, ?)",
            (999, 'T-S test', 'Letter', None)
        )
        conn.commit()
        conn.close()

        svc = PgpService(db_path=db_path)
        try:
            doc = svc.get_document_metadata(999)
            assert doc is not None
            assert doc['tags'] is None
        finally:
            svc.close()

    def test_tags_empty_list_returns_empty_list(self, tmp_path):
        """Document with '[]' tags TEXT should return empty Python list."""
        db_path = _create_test_db(str(tmp_path))
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, document_type, tags) "
            "VALUES (?, ?, ?, ?)",
            (999, 'T-S test', 'Letter', '[]')
        )
        conn.commit()
        conn.close()

        svc = PgpService(db_path=db_path)
        try:
            doc = svc.get_document_metadata(999)
            assert doc is not None
            assert doc['tags'] == []
        finally:
            svc.close()

    def test_sections_returned_as_list(self, pgp_service):
        """Source sections stored as TEXT JSON should be returned as list of dicts."""
        sources = pgp_service.get_sources_for_document(1234)
        # First source (Scholar A edition) has sections
        edition = [s for s in sources if 'Edition' in s['doc_relation']][0]
        assert isinstance(edition['sections'], list)
        assert len(edition['sections']) == 1
        assert edition['sections'][0]['canvas_num'] == 1

    def test_sections_null_returns_none(self, pgp_service):
        """Source with NULL sections should return None for sections."""
        sources = pgp_service.get_sources_for_document(1234)
        # Second source (Scholar B translation) has NULL sections
        translation = [s for s in sources if 'Translation' in s['doc_relation']][0]
        assert translation['sections'] is None


# ── TestTagSearch ─────────────────────────────────────────────────


class TestTagSearch:
    """Tests for json_each tag search and distinct tags."""

    def test_get_fragments_by_tag_found(self, pgp_service):
        """Should return fragments for documents matching tag."""
        results = pgp_service.get_fragments_by_tag('letter')

        assert len(results) > 0
        # Verify result structure
        frag = results[0]
        assert 'sys_id' in frag
        assert 'shelfmark' in frag
        assert 'document_type' in frag
        assert 'description' in frag
        assert 'pgpid' in frag
        assert 'transcription' in frag

        # Should find doc 1234 which has tag "letter"
        pgpids = {r['pgpid'] for r in results}
        assert 1234 in pgpids

    def test_get_fragments_by_tag_not_found(self, pgp_service):
        """Should return empty list for nonexistent tag."""
        results = pgp_service.get_fragments_by_tag('nonexistent_tag_xyz')
        assert results == []

    def test_get_all_distinct_tags(self, pgp_service):
        """Should return sorted unique list of all tags."""
        tags = pgp_service.get_all_distinct_tags()

        assert isinstance(tags, list)
        # Our test data has: letter, commercial (doc 1234) + legal, marriage (doc 5678)
        assert 'letter' in tags
        assert 'commercial' in tags
        assert 'legal' in tags
        assert 'marriage' in tags
        # Should be sorted alphabetically
        assert tags == sorted(tags)
        # No duplicates
        assert len(tags) == len(set(tags))


# ── TestBatchLookup ───────────────────────────────────────────────


class TestBatchLookup:
    """Tests for batch sys_id transcription check."""

    def test_get_sys_ids_with_transcriptions(self, pgp_service):
        """Should return only sys_ids that have linked fragments."""
        result = pgp_service.get_sys_ids_with_transcriptions(
            ['003072766', '003072767', 'UNKNOWN_SYS_ID']
        )

        assert isinstance(result, set)
        assert '003072766' in result
        assert '003072767' in result
        assert 'UNKNOWN_SYS_ID' not in result

    def test_get_sys_ids_with_transcriptions_empty(self, pgp_service):
        """Empty list input should return empty set."""
        result = pgp_service.get_sys_ids_with_transcriptions([])
        assert result == set()


# ── TestSourceQueries ─────────────────────────────────────────────


class TestSourceQueries:
    """Tests for source retrieval methods."""

    def test_get_sources_for_document(self, pgp_service):
        """Should return sources ordered by doc_relation then sequence_order."""
        sources = pgp_service.get_sources_for_document(1234)

        assert len(sources) == 2
        # Edition should come before Translation (alphabetical sort on doc_relation)
        assert 'Edition' in sources[0]['doc_relation']
        assert 'Translation' in sources[1]['doc_relation']

    def test_get_editions_for_document(self, pgp_service):
        """Should return only editions."""
        editions = pgp_service.get_editions_for_document(1234)

        assert len(editions) == 1
        assert 'Edition' in editions[0]['doc_relation']
        assert editions[0]['source_scholar'] == 'Scholar A'

    def test_get_translations_for_document(self, pgp_service):
        """Should return only translations."""
        translations = pgp_service.get_translations_for_document(1234)

        assert len(translations) == 1
        assert 'Translation' in translations[0]['doc_relation']
        assert translations[0]['source_scholar'] == 'Scholar B'

    def test_get_all_sources_for_fragment(self, pgp_service):
        """Should return sources from all linked documents with page_info."""
        sources = pgp_service.get_all_sources_for_fragment('003072766')

        assert len(sources) > 0
        # Should include page_info from fragment link
        for source in sources:
            assert 'page_info' in source
        # All sources for doc 1234 (which 003072766 links to)
        assert len(sources) == 2

    def test_get_all_sources_for_fragment_multi_doc(self, tmp_path):
        """Fragment linked to two documents should return sources from both."""
        db_path = _create_test_db(str(tmp_path))
        conn = sqlite3.connect(db_path)

        # Two documents
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, transcription) "
            "VALUES (?, ?, ?)",
            (100, 'T-S 1.1', 'Recto text')
        )
        conn.execute(
            "INSERT INTO documents (pgpid, shelfmark_combined, transcription) "
            "VALUES (?, ?, ?)",
            (200, 'T-S 1.1', 'Verso text')
        )

        # Same sys_id links to both
        conn.execute(
            "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (10, 100, 'SYS001', 'T-S 1.1', 1, 'recto')
        )
        conn.execute(
            "INSERT INTO document_fragments (id, document_id, sys_id, shelfmark, sequence_order, page_info) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (11, 200, 'SYS001', 'T-S 1.1', 1, 'verso')
        )

        # One source per document
        conn.execute(
            "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, content, sequence_order) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (10, 100, 'Scholar X', 'Digital Edition', 'Recto edition', 1)
        )
        conn.execute(
            "INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, content, sequence_order) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (11, 200, 'Scholar Y', 'Digital Edition', 'Verso edition', 1)
        )
        conn.commit()
        conn.close()

        svc = PgpService(db_path=db_path)
        try:
            sources = svc.get_all_sources_for_fragment('SYS001')
            assert len(sources) == 2
            # Each source should have page_info
            page_infos = {s['page_info'] for s in sources}
            assert 'recto' in page_infos
            assert 'verso' in page_infos
        finally:
            svc.close()

    def test_get_sources_for_document_empty(self, pgp_service):
        """Should return empty list for document with no sources."""
        sources = pgp_service.get_sources_for_document(9999)
        assert sources == []

    def test_get_editions_for_document_empty(self, pgp_service):
        """Should return empty list for nonexistent document."""
        editions = pgp_service.get_editions_for_document(9999)
        assert editions == []

    def test_get_translations_for_document_empty(self, pgp_service):
        """Should return empty list for nonexistent document."""
        translations = pgp_service.get_translations_for_document(9999)
        assert translations == []


# ── TestServiceUnavailable ────────────────────────────────────────


class TestServiceUnavailable:
    """Tests for graceful degradation when database is unavailable."""

    def test_service_unavailable_returns_none_or_empty(self, tmp_path):
        """All functions should return None or [] with nonexistent db, no exceptions."""
        nonexistent = os.path.join(str(tmp_path), "does_not_exist.db")
        svc = PgpService(db_path=nonexistent)

        assert svc.is_available() is False

        # All methods should degrade gracefully
        assert svc.get_document_for_fragment('003072766') is None
        assert svc.get_fragments_for_document(1234) == []
        assert svc.get_transcription_for_document(1234) is None
        assert svc.get_document_metadata(1234) is None
        assert svc.get_sources_for_document(1234) == []
        assert svc.get_all_sources_for_fragment('003072766') == []
        assert svc.get_editions_for_document(1234) == []
        assert svc.get_translations_for_document(1234) == []
        assert svc.get_sys_ids_with_transcriptions(['003072766']) == set()
        assert svc.get_fragments_by_tag('letter') == []
        assert svc.get_all_distinct_tags() == []

        svc.close()
