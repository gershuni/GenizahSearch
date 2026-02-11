"""
Tests for search normalization functions: diacritics stripping and mark-tolerant patterns.

Tests cover:
- strip_search_diacritics() combining diacritical mark removal
- make_mark_tolerant_pattern() mark-tolerant regex generation
"""

import re
import pytest

from genizah_core import (
    strip_search_diacritics,
    make_mark_tolerant_pattern,
)


# ============================================================================
# strip_search_diacritics
# ============================================================================

class TestStripSearchDiacritics:
    """Tests for strip_search_diacritics(text) -> str."""

    def test_removes_combining_acute_accent(self):
        """Combining acute accent (U+0301) is stripped."""
        assert strip_search_diacritics("te\u0301st") == "test"

    def test_removes_combining_grave_accent(self):
        """Combining grave accent (U+0300) is stripped."""
        assert strip_search_diacritics("cafe\u0300") == "cafe"

    def test_preserves_hebrew_base_letters(self):
        """Hebrew base letters are not modified."""
        assert strip_search_diacritics("\u05e9\u05dc\u05d5\u05dd") == "\u05e9\u05dc\u05d5\u05dd"

    def test_preserves_ascii_unchanged(self):
        """Plain ASCII text is returned unchanged."""
        assert strip_search_diacritics("word") == "word"

    def test_removes_geresh(self):
        """Hebrew geresh (U+05F3) is stripped."""
        assert strip_search_diacritics("\u05e8\u05f3 \u05d9\u05d4\u05d5\u05d3\u05d4") == "\u05e8 \u05d9\u05d4\u05d5\u05d3\u05d4"

    def test_removes_gershayim(self):
        """Hebrew gershayim (U+05F4) is stripped."""
        assert strip_search_diacritics("\u05d7\u05d6\u05f4\u05dc") == "\u05d7\u05d6\u05dc"

    def test_removes_both_geresh_and_gershayim(self):
        """Both geresh and gershayim are stripped in the same string."""
        assert strip_search_diacritics("\u05e8\u05f3 \u05e9\u05de\u05d5\u05d0\u05dc \u05d1\u05e8 \u05d7\u05d6\u05f4\u05dc") == "\u05e8 \u05e9\u05de\u05d5\u05d0\u05dc \u05d1\u05e8 \u05d7\u05d6\u05dc"

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert strip_search_diacritics("") == ""

    def test_multiple_stacked_combining_marks(self):
        """Multiple combining marks on the same base character are all stripped."""
        assert strip_search_diacritics("a\u0300\u0301b\u0302c") == "abc"

    def test_combining_mark_on_hebrew_letter(self):
        """Combining mark between Hebrew letters is stripped."""
        assert strip_search_diacritics("\u05e9\u05dc\u05d5\u0308\u05dd") == "\u05e9\u05dc\u05d5\u05dd"

    def test_preserves_hebrew_nikud(self):
        """Hebrew nikud/vowel points (U+05B0-U+05BD, U+05BF, U+05C1-U+05C2) are NOT stripped."""
        # sheva (U+05B0), patach (U+05B7), kamatz (U+05B8)
        text_with_nikud = "\u05e9\u05b0\u05dc\u05b7\u05d5\u05b8\u05dd"
        assert strip_search_diacritics(text_with_nikud) == text_with_nikud

    def test_preserves_digits_and_punctuation(self):
        """Digits, spaces, and punctuation are preserved."""
        assert strip_search_diacritics("abc 123 !@#") == "abc 123 !@#"

    def test_removes_ascii_apostrophe(self):
        """ASCII apostrophe (U+0027) is stripped."""
        # Example: הקב'ה → הקבה
        assert strip_search_diacritics("הקב'ה") == "הקבה"

    def test_removes_left_curly_quote(self):
        """Left curly single quote (U+2018) is stripped."""
        assert strip_search_diacritics("הקב\u2018ה") == "הקבה"

    def test_removes_right_curly_quote(self):
        """Right curly single quote (U+2019) is stripped."""
        assert strip_search_diacritics("הקב\u2019ה") == "הקבה"

    def test_all_apostrophe_variants_normalize_identically(self):
        """ASCII apostrophe, curly quotes, geresh, and gershayim all normalize to same base form."""
        base = "הקבה"
        assert strip_search_diacritics("הקב'ה") == base       # ASCII apostrophe
        assert strip_search_diacritics("הקב\u05F3ה") == base   # Hebrew geresh
        assert strip_search_diacritics("הקב\u05F4ה") == base   # Hebrew gershayim
        assert strip_search_diacritics("הקב\u2018ה") == base   # Left curly quote
        assert strip_search_diacritics("הקב\u2019ה") == base   # Right curly quote


# ============================================================================
# make_mark_tolerant_pattern
# ============================================================================

class TestMakeMarkTolerantPattern:
    """Tests for make_mark_tolerant_pattern(escaped_term) -> str."""

    def test_matches_text_without_marks(self):
        """Pattern matches plain text without combining marks."""
        pattern = make_mark_tolerant_pattern(re.escape("\u05e9\u05dc\u05d5\u05dd"))
        assert re.search(pattern, "\u05e9\u05dc\u05d5\u05dd")

    def test_matches_text_with_marks_between_letters(self):
        """Pattern matches text that has combining marks inserted between base letters."""
        pattern = make_mark_tolerant_pattern(re.escape("\u05e9\u05dc\u05d5\u05dd"))
        assert re.search(pattern, "\u05e9\u0300\u05dc\u0301\u05d5\u0302\u05dd")

    def test_matches_latin_with_mark(self):
        """Pattern matches Latin text with a combining mark inserted."""
        pattern = make_mark_tolerant_pattern(re.escape("test"))
        assert re.search(pattern, "te\u0308st")

    def test_matches_abc_with_and_without_marks(self):
        """Pattern matches both plain 'abc' and 'abc' with marks."""
        pattern = make_mark_tolerant_pattern(re.escape("abc"))
        assert re.search(pattern, "abc")
        assert re.search(pattern, "a\u0300b\u0301c")

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert make_mark_tolerant_pattern("") == ""

    def test_handles_escaped_special_chars(self):
        """Escape sequences like \\. are treated as single tokens, not split."""
        escaped = re.escape("a.b")  # produces "a\\.b"
        pattern = make_mark_tolerant_pattern(escaped)
        # The mark inserter should go between "a", "\\.", and "b"
        # It should still match "a.b" literally
        assert re.search(pattern, "a.b")
        # It should NOT match "axb" (dot should be literal)
        assert not re.search(pattern, "axb")

    def test_mark_tolerant_pattern_is_valid_regex(self):
        """The returned pattern compiles as a valid regex."""
        pattern = make_mark_tolerant_pattern(re.escape("hello"))
        compiled = re.compile(pattern)
        assert compiled.search("hello")

    def test_matches_text_with_apostrophe_variants(self):
        """Pattern matches text containing ASCII apostrophe, curly quotes, or Hebrew marks."""
        # Pattern built from base form should match all variants
        pattern = make_mark_tolerant_pattern(re.escape("הקבה"))
        assert re.search(pattern, "הקבה")        # base form
        assert re.search(pattern, "הקב'ה")       # ASCII apostrophe
        assert re.search(pattern, "הקב\u05F3ה")  # Hebrew geresh
        assert re.search(pattern, "הקב\u2019ה")  # Right curly quote

    def test_mark_tolerant_with_latin_apostrophe(self):
        """Pattern matches Latin text with apostrophe variants inserted."""
        pattern = make_mark_tolerant_pattern(re.escape("dont"))
        assert re.search(pattern, "dont")
        assert re.search(pattern, "don't")        # ASCII apostrophe
        assert re.search(pattern, "don\u2019t")   # Curly quote
