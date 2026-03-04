# -*- coding: utf-8 -*-
"""
Tests for Dicta Translation API client and TranslationService.

Tests are organized in two groups:
1. Dicta API client tests (shared/dicta_client.py) - mocked HTTP, no live calls
2. TranslationService tests (shared/translation_service.py) - in-memory SQLite
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================================
# Task 1: Dicta API Client Tests
# =============================================================================


class TestBuildFewShotPrompt:
    """Tests for build_few_shot_prompt function."""

    def test_build_few_shot_prompt_en2he(self):
        """build_few_shot_prompt with en2he direction produces 'English: ...\nHebrew: ...' pairs."""
        from shared.dicta_client import build_few_shot_prompt

        template = {
            "prompts": [
                {"English": "Letter from a merchant", "Hebrew": "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8"},
                {"English": "Legal document", "Hebrew": "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9"},
            ],
            "en_category": "English",
            "he_category": "Hebrew",
        }

        result = build_few_shot_prompt(template, direction="en2he")

        # Each pair should have English first, then Hebrew
        assert "English: Letter from a merchant" in result
        assert "Hebrew: \u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8" in result
        assert "English: Legal document" in result
        assert "Hebrew: \u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9" in result

        # Pairs should be separated by double newline
        pairs = result.split("\n\n")
        assert len(pairs) == 2

    def test_build_few_shot_prompt_he2en(self):
        """build_few_shot_prompt with he2en direction produces 'Hebrew: ...\nEnglish: ...' pairs."""
        from shared.dicta_client import build_few_shot_prompt

        template = {
            "prompts": [
                {"English": "Letter from a merchant", "Hebrew": "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8"},
                {"English": "Legal document", "Hebrew": "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9"},
            ],
            "en_category": "English",
            "he_category": "Hebrew",
        }

        result = build_few_shot_prompt(template, direction="he2en")

        # Each pair should have Hebrew first, then English
        lines = result.split("\n")
        # First line of first pair should be Hebrew
        assert lines[0].startswith("Hebrew:")
        # Second line of first pair should be English
        assert lines[1].startswith("English:")


class TestTranslateText:
    """Tests for translate_text function."""

    @patch("shared.dicta_client.requests.post")
    def test_translate_text_constructs_correct_payload(self, mock_post):
        """translate_text builds prompt with few-shot prefix + source/target categories."""
        from shared.dicta_client import translate_text

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"text": " \u05e8\u05d9\u05e9\u05d5\u05dd \u05d1\u05d9\u05ea \u05d3\u05d9\u05df"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        few_shot = "English: Hello\nHebrew: \u05e9\u05dc\u05d5\u05dd"
        result = translate_text("Court record", few_shot, direction="en2he")

        # Should have been called
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs[0][1]

        # Prompt should end with "English: Court record\nHebrew:"
        assert "English: Court record" in payload["prompt"]
        assert payload["prompt"].endswith("Hebrew:")

        # Check other payload fields
        assert payload["model"] == "dicta-il/dictalm2.0"
        assert payload["temperature"] == 0
        assert payload["stop"] == ["\n\n"]
        assert payload["max_tokens"] == 1024

    @patch("shared.dicta_client.requests.post")
    def test_translate_text_handles_api_error(self, mock_post):
        """Returns None on HTTP error, logs warning."""
        from shared.dicta_client import translate_text

        mock_post.side_effect = Exception("Connection error")

        few_shot = "English: Hello\nHebrew: \u05e9\u05dc\u05d5\u05dd"
        result = translate_text("Test text", few_shot, direction="en2he")

        assert result is None

    @patch("shared.dicta_client.requests.post")
    def test_translate_text_strips_whitespace(self, mock_post):
        """Returned translation is stripped of leading/trailing whitespace."""
        from shared.dicta_client import translate_text

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"text": "  \u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8  "}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        few_shot = "English: Hello\nHebrew: \u05e9\u05dc\u05d5\u05dd"
        result = translate_text("Letter from a merchant", few_shot, direction="en2he")

        assert result == "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8"


class TestPgpDocumentTypeHe:
    """Tests for PGP_DOCUMENT_TYPE_HE dictionary."""

    def test_pgp_document_type_he_covers_all_9(self):
        """PGP_DOCUMENT_TYPE_HE dict has exactly 9 entries matching known values."""
        from shared.dicta_client import PGP_DOCUMENT_TYPE_HE

        expected_keys = {
            "Letter",
            "Legal document",
            "List or table",
            "Literary text",
            "State document",
            "Paraliterary text",
            "Credit instrument or private receipt",
            "Legal query or responsum",
            "Inscription",
        }

        assert set(PGP_DOCUMENT_TYPE_HE.keys()) == expected_keys
        assert len(PGP_DOCUMENT_TYPE_HE) == 9

        # All values should be non-empty Hebrew strings
        for key, value in PGP_DOCUMENT_TYPE_HE.items():
            assert isinstance(value, str)
            assert len(value) > 0


class TestLoadFewShotTemplate:
    """Tests for load_few_shot_template function."""

    def test_load_few_shot_template(self, tmp_path):
        """load_few_shot_template reads JSON and returns dict with prompts, en_category, he_category."""
        from shared.dicta_client import load_few_shot_template

        template_data = {
            "prompts": [
                {"English": "Letter", "Hebrew": "\u05de\u05db\u05ea\u05d1"},
            ],
            "en_category": "English",
            "he_category": "Hebrew",
        }

        template_file = tmp_path / "test_template.json"
        template_file.write_text(json.dumps(template_data), encoding="utf-8")

        result = load_few_shot_template(str(template_file))

        assert "prompts" in result
        assert "en_category" in result
        assert "he_category" in result
        assert len(result["prompts"]) == 1
        assert result["en_category"] == "English"
        assert result["he_category"] == "Hebrew"


# =============================================================================
# Task 2: TranslationService Tests (in-memory SQLite)
# =============================================================================

import sqlite3


class TestTranslationServiceAvailability:
    """Tests for TranslationService availability checks."""

    def test_service_not_available_without_tables(self, tmp_path):
        """TranslationService.is_available() returns False when translation tables don't exist."""
        from shared.translation_service import TranslationService

        # Create empty databases without translation tables
        pgp_db = tmp_path / "pgp.db"
        fjms_db = tmp_path / "fjms.db"
        sqlite3.connect(str(pgp_db)).close()
        sqlite3.connect(str(fjms_db)).close()

        svc = TranslationService(
            pgp_db_path=str(pgp_db), fjms_db_path=str(fjms_db)
        )
        try:
            assert svc.is_available() is False
            assert svc.pgp_available() is False
            assert svc.fjms_available() is False
        finally:
            svc.close()


class TestTranslationServicePgp:
    """Tests for PGP translation queries."""

    @pytest.fixture
    def pgp_service(self, tmp_path):
        """Create a TranslationService with a PGP database containing test data."""
        from shared.translation_service import (
            TranslationService,
            ensure_pgp_translations_table,
        )

        pgp_db = tmp_path / "pgp.db"
        conn = sqlite3.connect(str(pgp_db))
        ensure_pgp_translations_table(conn)

        # Insert test data
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, document_type_he, translated_at) "
            "VALUES (?, ?, ?, ?)",
            (1001, "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8", "\u05de\u05db\u05ea\u05d1", "2026-03-04"),
        )
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, document_type_he, translated_at) "
            "VALUES (?, ?, ?, ?)",
            (1002, "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9", "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9", "2026-03-04"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(pgp_db_path=str(pgp_db))
        yield svc
        svc.close()

    def test_get_pgp_description_he(self, pgp_service):
        """Returns Hebrew translation for known pgpid from pgp_translations table."""
        result = pgp_service.get_pgp_description_he(1001)
        assert result == "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8"

    def test_get_pgp_description_he_missing(self, pgp_service):
        """Returns None for unknown pgpid."""
        result = pgp_service.get_pgp_description_he(9999)
        assert result is None

    def test_get_translations_batch(self, pgp_service):
        """Batch lookup returns dict of pgpid -> translation dict for multiple pgpids."""
        result = pgp_service.get_pgp_translations_batch([1001, 1002, 9999])
        assert 1001 in result
        assert 1002 in result
        assert 9999 not in result
        assert result[1001]["description_he"] == "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8"
        assert result[1002]["document_type_he"] == "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9"


class TestTranslationServiceFjms:
    """Tests for FJMS translation queries."""

    @pytest.fixture
    def fjms_service(self, tmp_path):
        """Create a TranslationService with an FJMS database containing test data."""
        from shared.translation_service import (
            TranslationService,
            ensure_fjms_translations_table,
        )

        fjms_db = tmp_path / "fjms.db"
        conn = sqlite3.connect(str(fjms_db))
        ensure_fjms_translations_table(conn)

        # Insert test data
        conn.execute(
            "INSERT INTO fjms_translations (alma_id, field_name, signature_id, original_text, translated_text, direction, translated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ALMA001", "Title", None, "\u05ea\u05d5\u05e8\u05d4 \u05e4\u05e8\u05e9\u05ea \u05d1\u05e8\u05d0\u05e9\u05d9\u05ea", "Torah Parashat Bereshit", "he2en", "2026-03-04"),
        )
        conn.execute(
            "INSERT INTO fjms_translations (alma_id, field_name, signature_id, original_text, translated_text, direction, translated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ALMA001", "FreeDesc", 42, "\u05e7\u05d8\u05e2 \u05de\u05ea\u05d5\u05e8\u05d4", "Fragment from the Torah", "he2en", "2026-03-04"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(fjms_db_path=str(fjms_db))
        yield svc
        svc.close()

    def test_get_fjms_translation(self, fjms_service):
        """Returns translated text for known alma_id + field_name."""
        result = fjms_service.get_fjms_translation("ALMA001", "Title")
        assert result == "Torah Parashat Bereshit"

    def test_get_fjms_free_desc_en(self, fjms_service):
        """Returns English translation for known free description."""
        result = fjms_service.get_fjms_free_desc_en("ALMA001", 42)
        assert result == "Fragment from the Torah"

    def test_get_fjms_translation_missing(self, fjms_service):
        """Returns None for unknown alma_id."""
        result = fjms_service.get_fjms_translation("UNKNOWN", "Title")
        assert result is None


class TestNoOverwriteCheck:
    """Tests for the no-overwrite safety check."""

    def test_has_existing_translation(self, tmp_path):
        """has_existing_translation returns True when target field already has content."""
        from shared.translation_service import (
            TranslationService,
            ensure_pgp_translations_table,
        )

        pgp_db = tmp_path / "pgp.db"
        conn = sqlite3.connect(str(pgp_db))
        ensure_pgp_translations_table(conn)
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he) VALUES (?, ?)",
            (1001, "\u05de\u05db\u05ea\u05d1 \u05e7\u05d9\u05d9\u05dd"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(pgp_db_path=str(pgp_db))
        try:
            assert svc.has_existing_translation(1001, "description_he") is True
            assert svc.has_existing_translation(9999, "description_he") is False
        finally:
            svc.close()


class TestSchemaCreation:
    """Tests for schema creation helpers."""

    def test_create_pgp_translations_schema(self):
        """DDL creates correct pgp_translations table."""
        from shared.translation_service import ensure_pgp_translations_table

        conn = sqlite3.connect(":memory:")
        ensure_pgp_translations_table(conn)

        # Verify table exists with correct columns
        cursor = conn.execute("PRAGMA table_info(pgp_translations)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert "pgpid" in columns
        assert "description_he" in columns
        assert "document_type_he" in columns
        assert "translated_at" in columns
        assert "model_version" in columns

        conn.close()

    def test_create_fjms_translations_schema(self):
        """DDL creates correct fjms_translations table with indexes."""
        from shared.translation_service import ensure_fjms_translations_table

        conn = sqlite3.connect(":memory:")
        ensure_fjms_translations_table(conn)

        # Verify table exists with correct columns
        cursor = conn.execute("PRAGMA table_info(fjms_translations)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert "id" in columns
        assert "alma_id" in columns
        assert "field_name" in columns
        assert "signature_id" in columns
        assert "original_text" in columns
        assert "translated_text" in columns
        assert "direction" in columns
        assert "translated_at" in columns
        assert "model_version" in columns

        # Verify indexes exist
        cursor = conn.execute("PRAGMA index_list(fjms_translations)")
        index_names = [row[1] for row in cursor.fetchall()]
        assert "idx_fjms_trans_alma" in index_names
        assert "idx_fjms_trans_field" in index_names

        conn.close()


# =============================================================================
# Plan 46-02: PGP Batch Translation Script Tests
# =============================================================================


class TestPgpBatchFlowE2E:
    """End-to-end test for PGP batch translation flow with mocked API."""

    def test_pgp_batch_flow_e2e(self, tmp_path):
        """Full batch flow: reads docs, filters by length, translates, writes pgp_translations."""
        from shared.dicta_client import PGP_DOCUMENT_TYPE_HE
        from shared.translation_service import ensure_pgp_translations_table

        # Create a minimal documents table in a test db
        db_path = str(tmp_path / "pgp.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE documents ("
            "  pgpid INTEGER PRIMARY KEY,"
            "  description TEXT,"
            "  document_type TEXT"
            ")"
        )
        # Row 1: normal description (>= 20 chars, should be translated)
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?)",
            (1001, "Letter from a merchant requesting payment for goods shipped from Fustat to Alexandria", "Letter"),
        )
        # Row 2: short description (< 20 chars, should be skipped)
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?)",
            (1002, "Short desc", "Letter"),
        )
        # Row 3: NULL description (should be skipped)
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?)",
            (1003, None, "Legal document"),
        )
        # Row 4: normal description, different type
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?)",
            (1004, "Legal document concerning a debt between two parties witnessed by judges", "Legal document"),
        )
        conn.commit()
        conn.close()

        # Create pgp_translations table
        write_conn = sqlite3.connect(db_path)
        ensure_pgp_translations_table(write_conn)

        # Import batch script functions
        from scripts.translate_pgp_descriptions import flush_batch, get_candidates

        # Get candidates with min_length=20
        candidates = get_candidates(db_path, min_length=20)

        # Should only include rows with description >= 20 chars
        pgpids = [c[0] for c in candidates]
        assert 1001 in pgpids, "Normal description should be a candidate"
        assert 1002 not in pgpids, "Short description should be filtered out"
        assert 1003 not in pgpids, "NULL description should be filtered out"
        assert 1004 in pgpids, "Second normal description should be a candidate"

        # Simulate translation: mock API for descriptions, manual mapping for types
        mock_translations = {
            1001: "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8 \u05d4\u05de\u05d1\u05e7\u05e9 \u05ea\u05e9\u05dc\u05d5\u05dd",
            1004: "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9 \u05d1\u05e0\u05d5\u05e9\u05d0 \u05d7\u05d5\u05d1",
        }

        batch_results = []
        for pgpid, desc, dtype in candidates:
            desc_he = mock_translations.get(pgpid)
            dtype_he = PGP_DOCUMENT_TYPE_HE.get(dtype) if dtype else None
            if desc_he is not None:
                batch_results.append((pgpid, desc_he, dtype_he))

        # Flush to database
        flush_batch(write_conn, batch_results)

        # Verify results in pgp_translations table
        rows = write_conn.execute(
            "SELECT pgpid, description_he, document_type_he FROM pgp_translations ORDER BY pgpid"
        ).fetchall()

        assert len(rows) == 2, f"Expected 2 translated rows, got {len(rows)}"

        # Row 1001: Letter
        assert rows[0][0] == 1001
        assert "\u05de\u05db\u05ea\u05d1" in rows[0][1]  # description contains "michtav"
        assert rows[0][2] == "\u05de\u05db\u05ea\u05d1"  # document_type_he = Letter in Hebrew

        # Row 1004: Legal document
        assert rows[1][0] == 1004
        assert "\u05de\u05e1\u05de\u05da" in rows[1][1]  # description contains "mismakh"
        assert rows[1][2] == "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9"  # Legal document in Hebrew

        write_conn.close()

    def test_candidates_exclude_empty_strings(self, tmp_path):
        """get_candidates excludes rows with empty string descriptions."""
        db_path = str(tmp_path / "pgp.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE documents ("
            "  pgpid INTEGER PRIMARY KEY,"
            "  description TEXT,"
            "  document_type TEXT"
            ")"
        )
        conn.execute("INSERT INTO documents VALUES (?, ?, ?)", (1, "", "Letter"))
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?)",
            (2, "A valid description of sufficient length for translation", "Letter"),
        )
        conn.commit()
        conn.close()

        from scripts.translate_pgp_descriptions import get_candidates

        candidates = get_candidates(db_path, min_length=20)
        pgpids = [c[0] for c in candidates]
        assert 1 not in pgpids, "Empty string description should be excluded"
        assert 2 in pgpids

    def test_document_type_manual_mapping(self):
        """All 9 PGP document types map correctly via PGP_DOCUMENT_TYPE_HE without API."""
        from shared.dicta_client import PGP_DOCUMENT_TYPE_HE

        # Every known type should have a non-empty Hebrew translation
        test_types = [
            "Letter", "Legal document", "List or table", "Literary text",
            "State document", "Paraliterary text",
            "Credit instrument or private receipt",
            "Legal query or responsum", "Inscription",
        ]
        for dt in test_types:
            he = PGP_DOCUMENT_TYPE_HE.get(dt)
            assert he is not None, f"Missing HE translation for '{dt}'"
            assert len(he) > 0, f"Empty HE translation for '{dt}'"

        # Unknown type should return None
        assert PGP_DOCUMENT_TYPE_HE.get("Unknown type") is None


class TestCheckpointSaveLoad:
    """Tests for checkpoint save and load functions."""

    def test_checkpoint_round_trip(self, tmp_path):
        """Save and load checkpoint preserves the set of completed IDs."""
        from scripts.translate_pgp_descriptions import load_checkpoint, save_checkpoint

        checkpoint_path = str(tmp_path / "checkpoint.json")

        # Initially empty
        ids = load_checkpoint(checkpoint_path)
        assert ids == set()

        # Save some IDs
        test_ids = {100, 200, 300, 42, 9999}
        save_checkpoint(checkpoint_path, test_ids)

        # Load back
        loaded = load_checkpoint(checkpoint_path)
        assert loaded == test_ids

    def test_checkpoint_atomic_write(self, tmp_path):
        """Checkpoint write is atomic -- no .tmp files left behind."""
        from scripts.translate_pgp_descriptions import save_checkpoint

        checkpoint_path = str(tmp_path / "checkpoint.json")
        save_checkpoint(checkpoint_path, {1, 2, 3})

        # Verify no .tmp files in directory
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert len(tmp_files) == 0, f"Leftover .tmp files: {tmp_files}"

        # Verify the checkpoint file exists and is valid JSON
        assert os.path.isfile(checkpoint_path)
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["count"] == 3
        assert set(data["completed_ids"]) == {1, 2, 3}
        assert "saved_at" in data

    def test_checkpoint_incremental_update(self, tmp_path):
        """Checkpoint can be incrementally updated with more IDs."""
        from scripts.translate_pgp_descriptions import load_checkpoint, save_checkpoint

        checkpoint_path = str(tmp_path / "checkpoint.json")

        # First batch
        ids = {1, 2, 3}
        save_checkpoint(checkpoint_path, ids)

        # Load, add more, save again
        loaded = load_checkpoint(checkpoint_path)
        loaded.update({4, 5, 6})
        save_checkpoint(checkpoint_path, loaded)

        # Final load
        final = load_checkpoint(checkpoint_path)
        assert final == {1, 2, 3, 4, 5, 6}

    def test_checkpoint_load_missing_file(self, tmp_path):
        """Loading from non-existent file returns empty set."""
        from scripts.translate_pgp_descriptions import load_checkpoint

        result = load_checkpoint(str(tmp_path / "nonexistent.json"))
        assert result == set()

    def test_checkpoint_load_corrupt_file(self, tmp_path):
        """Loading from corrupt JSON file returns empty set (graceful degradation)."""
        from scripts.translate_pgp_descriptions import load_checkpoint

        corrupt_path = str(tmp_path / "corrupt.json")
        with open(corrupt_path, "w") as f:
            f.write("not valid json {{{")

        result = load_checkpoint(corrupt_path)
        assert result == set()


# =============================================================================
# Plan 46-04: Search Integration Tests (search methods + sys_id mapping)
# =============================================================================


class TestSearchPgpByTranslation:
    """Tests for search_pgp_by_translation method."""

    @pytest.fixture
    def pgp_service(self, tmp_path):
        from shared.translation_service import (
            TranslationService,
            ensure_pgp_translations_table,
        )

        pgp_db = tmp_path / "pgp.db"
        conn = sqlite3.connect(str(pgp_db))
        ensure_pgp_translations_table(conn)

        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, translated_at) VALUES (?, ?, ?)",
            (1001, "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8 \u05de\u05e4\u05d5\u05e1\u05d8\u05d0\u05d8", "2026-03-04"),
        )
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, translated_at) VALUES (?, ?, ?)",
            (1002, "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9 \u05d1\u05d9\u05df \u05e9\u05e0\u05d9 \u05e6\u05d3\u05d3\u05d9\u05dd", "2026-03-04"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(pgp_db_path=str(pgp_db))
        yield svc
        svc.close()

    def test_search_he_finds_matching_descriptions(self, pgp_service):
        """search_pgp_by_translation for Hebrew query finds matching pgpids."""
        result = pgp_service.search_pgp_by_translation("\u05de\u05e1\u05d5\u05d7\u05e8", "he")
        assert 1001 in result
        assert 1002 not in result

    def test_search_en_returns_empty(self, pgp_service):
        """search_pgp_by_translation for English language returns empty set (descriptions already in EN)."""
        result = pgp_service.search_pgp_by_translation("merchant", "en")
        assert result == set()

    def test_search_empty_query_returns_empty(self, pgp_service):
        """search_pgp_by_translation with empty query returns empty set."""
        result = pgp_service.search_pgp_by_translation("", "he")
        assert result == set()


class TestSearchFjmsByTranslation:
    """Tests for search_fjms_by_translation method."""

    @pytest.fixture
    def fjms_service(self, tmp_path):
        from shared.translation_service import (
            TranslationService,
            ensure_fjms_translations_table,
        )

        fjms_db = tmp_path / "fjms.db"
        conn = sqlite3.connect(str(fjms_db))
        ensure_fjms_translations_table(conn)

        conn.execute(
            "INSERT INTO fjms_translations (alma_id, field_name, original_text, translated_text, direction, translated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("ALMA001", "Title", "\u05ea\u05d5\u05e8\u05d4 \u05e4\u05e8\u05e9\u05ea \u05d1\u05e8\u05d0\u05e9\u05d9\u05ea", "Torah Parashat Bereshit", "he2en", "2026-03-04"),
        )
        conn.execute(
            "INSERT INTO fjms_translations (alma_id, field_name, original_text, translated_text, direction, translated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("ALMA002", "FreeDesc", "\u05e7\u05d8\u05e2 \u05de\u05ea\u05dc\u05de\u05d5\u05d3", "Talmud Fragment", "he2en", "2026-03-04"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(fjms_db_path=str(fjms_db))
        yield svc
        svc.close()

    def test_search_finds_matching_alma_ids(self, fjms_service):
        """search_fjms_by_translation finds alma_ids with matching translated text."""
        result = fjms_service.search_fjms_by_translation("Torah", "en")
        assert "ALMA001" in result
        assert "ALMA002" not in result

    def test_search_empty_returns_empty(self, fjms_service):
        """search_fjms_by_translation with empty query returns empty set."""
        result = fjms_service.search_fjms_by_translation("", "en")
        assert result == set()


class TestGetPgpTranslationsBySysIds:
    """Tests for get_pgp_translations_by_sys_ids method."""

    @pytest.fixture
    def svc_with_fragments(self, tmp_path):
        from shared.translation_service import (
            TranslationService,
            ensure_pgp_translations_table,
        )

        pgp_db = tmp_path / "pgp.db"
        conn = sqlite3.connect(str(pgp_db))

        # Create document_fragments table (exists in pgp.db)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS document_fragments ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  document_id INTEGER,"
            "  sys_id TEXT,"
            "  sequence_order INTEGER"
            ")"
        )
        conn.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", (1001, "SYS_A"))
        conn.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", (1002, "SYS_B"))
        conn.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", (1003, "SYS_C"))

        ensure_pgp_translations_table(conn)
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, document_type_he) VALUES (?, ?, ?)",
            (1001, "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8", "\u05de\u05db\u05ea\u05d1"),
        )
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he, document_type_he) VALUES (?, ?, ?)",
            (1002, "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9", "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(pgp_db_path=str(pgp_db))
        yield svc
        svc.close()

    def test_maps_sys_ids_to_translations(self, svc_with_fragments):
        """get_pgp_translations_by_sys_ids maps sys_ids through document_fragments."""
        result = svc_with_fragments.get_pgp_translations_by_sys_ids(["SYS_A", "SYS_B", "SYS_C"])
        assert "SYS_A" in result
        assert "SYS_B" in result
        assert "SYS_C" not in result  # pgpid 1003 has no translation
        assert "\u05de\u05db\u05ea\u05d1" in result["SYS_A"]["description_he"]

    def test_empty_sys_ids_returns_empty(self, svc_with_fragments):
        """get_pgp_translations_by_sys_ids with empty list returns empty dict."""
        result = svc_with_fragments.get_pgp_translations_by_sys_ids([])
        assert result == {}


class TestGetTranslatedMatchSysIds:
    """Tests for get_translated_match_sys_ids method."""

    @pytest.fixture
    def svc_with_fragments(self, tmp_path):
        from shared.translation_service import (
            TranslationService,
            ensure_pgp_translations_table,
        )

        pgp_db = tmp_path / "pgp.db"
        conn = sqlite3.connect(str(pgp_db))

        conn.execute(
            "CREATE TABLE IF NOT EXISTS document_fragments ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  document_id INTEGER,"
            "  sys_id TEXT"
            ")"
        )
        conn.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", (1001, "SYS_A"))
        conn.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (?, ?)", (1002, "SYS_B"))

        ensure_pgp_translations_table(conn)
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he) VALUES (?, ?)",
            (1001, "\u05de\u05db\u05ea\u05d1 \u05de\u05e1\u05d5\u05d7\u05e8 \u05de\u05e4\u05d5\u05e1\u05d8\u05d0\u05d8"),
        )
        conn.execute(
            "INSERT INTO pgp_translations (pgpid, description_he) VALUES (?, ?)",
            (1002, "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9 \u05d1\u05d9\u05df \u05e9\u05e0\u05d9"),
        )
        conn.commit()
        conn.close()

        svc = TranslationService(pgp_db_path=str(pgp_db))
        yield svc
        svc.close()

    def test_finds_matching_sys_ids(self, svc_with_fragments):
        """get_translated_match_sys_ids returns sys_ids with matching translations."""
        result = svc_with_fragments.get_translated_match_sys_ids("\u05de\u05e1\u05d5\u05d7\u05e8", ["SYS_A", "SYS_B"])
        assert "SYS_A" in result
        assert "SYS_B" not in result

    def test_empty_query_returns_empty(self, svc_with_fragments):
        """get_translated_match_sys_ids with empty query returns empty set."""
        result = svc_with_fragments.get_translated_match_sys_ids("", ["SYS_A"])
        assert result == set()

    def test_no_sys_ids_returns_empty(self, svc_with_fragments):
        """get_translated_match_sys_ids with no sys_ids returns empty set."""
        result = svc_with_fragments.get_translated_match_sys_ids("\u05de\u05e1\u05d5\u05d7\u05e8", [])
        assert result == set()
