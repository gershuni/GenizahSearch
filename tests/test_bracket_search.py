"""Tests for bracket-aware search matching.

Genizah transcriptions use square brackets for uncertain/reconstructed text
(e.g., [d]l[k]). This module tests that bracket-free queries find documents
containing bracketed words, while bracket-containing queries match literally.
"""

import re

from genizah_core import (
    _add_bracket_variants,
    _query_has_brackets,
    _strip_brackets,
)


class TestAddBracketVariants:
    """Test _add_bracket_variants helper."""

    def test_returns_bracket_adorned_variants(self):
        variants = _add_bracket_variants("word")
        assert "[word" in variants
        assert "word]" in variants
        assert "[word]" in variants
        assert "]word" in variants
        assert "word[" in variants

    def test_includes_original(self):
        variants = _add_bracket_variants("word")
        assert "word" in variants

    def test_hebrew_term(self):
        variants = _add_bracket_variants("\u05d4\u05e0\u05ea\u05e9\u05e0")
        assert "[\u05d4\u05e0\u05ea\u05e9\u05e0" in variants
        assert "\u05d4\u05e0\u05ea\u05e9\u05e0]" in variants
        assert "]\u05d4\u05e0\u05ea\u05e9\u05e0" in variants

    def test_empty_string(self):
        variants = _add_bracket_variants("")
        # Should return at least the original
        assert "" in variants


class TestQueryHasBrackets:
    """Test _query_has_brackets helper."""

    def test_no_brackets(self):
        assert _query_has_brackets("word") is False

    def test_opening_bracket(self):
        assert _query_has_brackets("[word") is True

    def test_closing_bracket(self):
        assert _query_has_brackets("word]") is True

    def test_both_brackets(self):
        assert _query_has_brackets("[word]") is True

    def test_hebrew_with_bracket(self):
        assert _query_has_brackets("]\u05d4\u05e0\u05ea\u05e9\u05e0") is True

    def test_hebrew_without_bracket(self):
        assert _query_has_brackets("\u05d4\u05e0\u05ea\u05e9\u05e0") is False

    def test_responsa_gap_operator_ignored(self):
        """Responsa [N] gap syntax should NOT be treated as literal brackets."""
        assert _query_has_brackets("\u05e9\u05dc\u05d5\u05dd [3] \u05d3\u05dc\u05da") is False

    def test_responsa_line_gap_operator_ignored(self):
        """Responsa [|N] line-gap syntax should NOT be treated as literal brackets."""
        assert _query_has_brackets("\u05e9\u05dc\u05d5\u05dd [|2] \u05d3\u05dc\u05da") is False

    def test_responsa_gap_with_real_bracket(self):
        """If query has BOTH a gap operator and a literal bracket, detect it."""
        assert _query_has_brackets("]\u05e9\u05dc\u05d5\u05dd [3] \u05d3\u05dc\u05da") is True

    def test_multiple_gap_operators(self):
        assert _query_has_brackets("a [3] b [|5] c") is False


class TestStripBrackets:
    """Test _strip_brackets helper."""

    def test_removes_opening_brackets(self):
        assert _strip_brackets("[word") == "word"

    def test_removes_closing_brackets(self):
        assert _strip_brackets("word]") == "word"

    def test_removes_all_brackets(self):
        assert _strip_brackets("[word] test ]other") == "word test other"

    def test_no_brackets(self):
        assert _strip_brackets("hello world") == "hello world"

    def test_empty_string(self):
        assert _strip_brackets("") == ""

    def test_hebrew_with_brackets(self):
        result = _strip_brackets("]\u05d4\u05e0\u05ea\u05e9\u05e0 [\u05d3]\u05dc[\u05da]")
        assert "[" not in result
        assert "]" not in result
        assert "\u05d4\u05e0\u05ea\u05e9\u05e0" in result


class TestRegexMatchingWithBrackets:
    """Test that regex matching works correctly with bracket stripping."""

    def test_bracket_free_query_matches_bracketed_content(self):
        """A bracket-free query should match content with brackets after stripping."""
        query_term = "\u05d4\u05e0\u05ea\u05e9\u05e0"  # הנתשנ
        content = "text ]\u05d4\u05e0\u05ea\u05e9\u05e0 more"  # text ]הנתשנ more
        pattern = re.compile(re.escape(query_term))

        # Without stripping, regex won't match (] is between words)
        # Actually re.escape(query_term) matches substring, but the content
        # has ]הנתשנ which contains הנתשנ as substring -- regex DOES match.
        # The real issue is Tantivy not returning the candidate.
        # But for bracket-in-middle cases like [ד]ל[ך]:
        content2 = "[\u05d3]\u05dc[\u05da]"  # [ד]ל[ך]
        query2 = "\u05d3\u05dc\u05da"  # דלך
        pattern2 = re.compile(re.escape(query2))

        # On original content, regex won't match because brackets break the word
        assert pattern2.search(content2) is None

        # On stripped content, regex matches
        stripped = _strip_brackets(content2)
        assert pattern2.search(stripped) is not None

    def test_bracket_containing_query_matches_literally(self):
        """A bracket-containing query should match only the literal bracketed form."""
        query_term = "]\u05d4\u05e0\u05ea\u05e9\u05e0"  # ]הנתשנ
        content = "text ]\u05d4\u05e0\u05ea\u05e9\u05e0 more"
        pattern = re.compile(re.escape(query_term))

        # Should match on original content (has brackets)
        assert _query_has_brackets(query_term) is True
        assert pattern.search(content) is not None

    def test_highlighting_preserves_brackets(self):
        """Highlighted snippets should preserve original brackets."""
        content = "some text ]\u05d4\u05e0\u05ea\u05e9\u05e0 more text"
        # Bracket-free query
        query_term = "\u05d4\u05e0\u05ea\u05e9\u05e0"
        pattern = re.compile(re.escape(query_term))

        # Re-search on original content for highlighting should still find it
        # (because הנתשנ is a substring of ]הנתשנ)
        match = pattern.search(content)
        assert match is not None
        # The match in original content preserves surrounding brackets
        assert "]" in content[max(0, match.start()-1):match.end()+1]
