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
