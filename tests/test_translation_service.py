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
