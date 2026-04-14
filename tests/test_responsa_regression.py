"""
Regression tests for non-Responsa search modes.

Verifies that all existing search modes (exact, variants, fuzzy, regex,
shelfmark, title) produce correct Tantivy queries, valid regex patterns
with verified match/reject behavior, and correct parse_query_syntax results
when Responsa mode is OFF.

These tests ensure that the Responsa additions did not break any existing
search functionality.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

import genizah_core as _gc_module

from genizah_core import SearchEngine


@pytest.fixture(autouse=True)
def _ensure_genizah_core_module():
    """Ensure genizah_core is properly loaded in sys.modules.

    test_missing_tantivy.py pops genizah_core from sys.modules which can
    break module-level patching in subsequent test files. This fixture
    ensures the correct module object is in sys.modules.
    """
    if 'genizah_core' not in sys.modules or sys.modules['genizah_core'] is not _gc_module:
        sys.modules['genizah_core'] = _gc_module
    yield


# ============================================================================
# Helper: Create a SearchEngine with mocked index/searcher
# ============================================================================

def _make_search_engine(variant_side_effect=None):
    """Create a SearchEngine with mocked dependencies for testing query building.

    Args:
        variant_side_effect: Optional side_effect for var_mgr.get_variants.
            Default returns just the term itself (no extra variants).
    """
    meta_mgr = MagicMock()
    var_mgr = MagicMock()

    if variant_side_effect is None:
        var_mgr.get_variants = MagicMock(side_effect=lambda term, mode, limit=200: [term])
    else:
        var_mgr.get_variants = MagicMock(side_effect=variant_side_effect)

    with patch.object(SearchEngine, 'reload_index', return_value=False):
        engine = SearchEngine(meta_mgr, var_mgr)

    return engine


# ============================================================================
# Class 1: TestExistingModesUnchanged
# ============================================================================

class TestExistingModesUnchanged:
    """Regression tests verifying all non-Responsa search modes produce correct
    Tantivy queries and regex patterns with behavioral match/reject assertions.

    For each mode, we test both structural validity (Tantivy query format) and
    behavioral correctness (regex pattern matches expected text, rejects non-matching).
    Calling with responsa_components=None (default) ensures the existing code path
    is exercised -- the same path used before Responsa was added.
    """

    # --- Exact mode ---

    def test_exact_tantivy_query_structure(self):
        """Exact mode: Tantivy query contains exact term with ^5 boost."""
        engine = _make_search_engine()
        result = engine.build_tantivy_query(['test'], 'exact')
        assert result is not None
        assert len(result) > 0
        assert '"test"^5' in result

    def test_exact_tantivy_with_responsa_none_same_as_default(self):
        """Exact mode: Passing responsa_components=None is identical to default call."""
        engine = _make_search_engine()
        result_default = engine.build_tantivy_query(['test'], 'exact')
        result_explicit = engine.build_tantivy_query(['test'], 'exact', responsa_components=None)
        assert result_default == result_explicit

    def test_exact_regex_matches_target_text(self):
        """Exact mode: Regex pattern matches text containing the search term."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['test'], 'exact', 0)
        assert pattern is not None
        assert pattern.search("this document has test word in it") is not None

    def test_exact_regex_rejects_non_matching_text(self):
        """Exact mode: Regex pattern does NOT match text without the search term."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['test'], 'exact', 0)
        assert pattern is not None
        assert pattern.search("no match here at all") is None

    # --- Variants mode ---

    def test_variants_tantivy_query_contains_all_variants(self):
        """Variants mode: Tantivy query includes the original term and all variants."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tst']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        result = engine.build_tantivy_query(['test'], 'variants')
        assert '"test"^5' in result
        assert '"tst"' in result

    def test_variants_regex_matches_original_term(self):
        """Variants mode: Regex matches text containing the original term."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tst']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'variants', 0)
        assert pattern is not None
        assert pattern.search("a document with test inside") is not None

    def test_variants_regex_matches_variant_term(self):
        """Variants mode: Regex matches text containing a variant (not original)."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tst']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'variants', 0)
        assert pattern is not None
        assert pattern.search("a document with tst inside") is not None

    def test_variants_regex_rejects_non_matching_text(self):
        """Variants mode: Regex rejects text containing neither original nor variants."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tst']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'variants', 0)
        assert pattern is not None
        assert pattern.search("completely unrelated xyz") is None

    # --- Fuzzy mode ---

    def test_fuzzy_tantivy_query_structure(self):
        """Fuzzy mode: Tantivy query uses fuzzy syntax (~1 or ~2 depending on length)."""
        engine = _make_search_engine()
        # 'test' is 4 chars -> ~1
        result = engine.build_tantivy_query(['test'], 'fuzzy')
        assert '"test"~1' in result

    def test_fuzzy_tantivy_long_word(self):
        """Fuzzy mode: Words of 5+ chars get ~2 distance."""
        engine = _make_search_engine()
        result = engine.build_tantivy_query(['hello'], 'fuzzy')
        assert '"hello"~2' in result

    def test_fuzzy_tantivy_short_word(self):
        """Fuzzy mode: Words shorter than 3 chars get exact match (no ~)."""
        engine = _make_search_engine()
        result = engine.build_tantivy_query(['ab'], 'fuzzy')
        assert '"ab"' in result
        # Should NOT have fuzzy distance
        assert '"ab"~' not in result

    def test_fuzzy_regex_matches_exact_term(self):
        """Fuzzy mode: Regex matches the exact search term."""
        # Fuzzy mode uses variants_maximum for regex, so mock returns near-matches
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tset', 'tost']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'fuzzy', 0)
        assert pattern is not None
        assert pattern.search("a document with test") is not None

    def test_fuzzy_regex_matches_near_match(self):
        """Fuzzy mode: Regex matches a near-match variant like 'tset' or 'tost'."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tset', 'tost']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'fuzzy', 0)
        assert pattern is not None
        assert pattern.search("a document with tset") is not None
        assert pattern.search("a document with tost") is not None

    def test_fuzzy_regex_rejects_unrelated_text(self):
        """Fuzzy mode: Regex rejects completely unrelated text."""
        def variants_side_effect(term, mode, limit=200):
            if term == 'test':
                return ['test', 'tset', 'tost']
            return [term]

        engine = _make_search_engine(variant_side_effect=variants_side_effect)
        pattern = engine.build_regex_pattern(['test'], 'fuzzy', 0)
        assert pattern is not None
        assert pattern.search("xyz absolutely unrelated") is None

    # --- Regex mode ---

    def test_regex_tantivy_query_extracts_hebrew_candidates(self):
        """Regex mode: Tantivy query extracts Hebrew word candidates from regex string."""
        engine = _make_search_engine()
        # Hebrew word of 2+ chars should be extracted
        result = engine.build_tantivy_query(['\u05e9\u05dc\u05d5\u05dd.*'], 'Regex')
        assert '\u05e9\u05dc\u05d5\u05dd' in result

    def test_regex_tantivy_query_fallback_to_wildcard(self):
        """Regex mode: When no Hebrew candidates found, falls back to wildcard."""
        engine = _make_search_engine()
        result = engine.build_tantivy_query(['test.*'], 'Regex')
        assert result == '*'

    def test_regex_pattern_matches_expected_text(self):
        """Regex mode: Pattern from 'test.*' matches 'testing' and 'testable'."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['test.*'], 'Regex', 0)
        assert pattern is not None
        assert pattern.search("testing") is not None
        assert pattern.search("testable") is not None

    def test_regex_pattern_rejects_non_matching_text(self):
        """Regex mode: Pattern from 'test.*' rejects text without 'test'."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['test.*'], 'Regex', 0)
        assert pattern is not None
        assert pattern.search("xyz") is None

    # --- Gap pattern behavior ---

    def test_gap_pattern_matches_within_gap(self):
        """Gap=3: Pattern matches 'hello' and 'world' with up to 3 intervening words."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['hello', 'world'], 'exact', 3)
        assert pattern is not None
        # 2 words between -> within gap of 3
        assert pattern.search("hello one two world") is not None
        # 3 words between -> within gap of 3
        assert pattern.search("hello one two three world") is not None

    def test_gap_pattern_rejects_beyond_gap(self):
        """Gap=3: Pattern rejects 'hello' and 'world' with 4 intervening words."""
        engine = _make_search_engine()
        pattern = engine.build_regex_pattern(['hello', 'world'], 'exact', 3)
        assert pattern is not None
        assert pattern.search("hello one two three four world") is None


# ============================================================================
# Class 2: TestParseQuerySyntaxRegression
# ============================================================================

class TestParseQuerySyntaxRegression:
    """Verify prefix shortcuts still work when NOT in Responsa mode.

    This defends against accidentally breaking prefix detection when
    Responsa mode is OFF, which is the default for non-Responsa users.
    """

    def test_hash_shelfmark_prefix(self):
        """'#T-S 12.1' with responsa_mode=False -> mode='Shelfmark', clean='T-S 12.1'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#T-S 12.1', responsa_mode=False)
        assert mode == 'Shelfmark'
        assert clean == 'T-S 12.1'

    def test_question_mark_variants_prefix(self):
        """'?test' with responsa_mode=False -> mode='variants', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('?test', responsa_mode=False)
        assert mode == 'variants'
        assert clean == 'test'

    def test_equals_exact_prefix(self):
        """'=test' with responsa_mode=False -> mode='exact', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('=test', responsa_mode=False)
        assert mode == 'exact'
        assert clean == 'test'

    def test_slash_regex_prefix(self):
        """'/test.*/' with responsa_mode=False -> mode='Regex', clean='test.*/'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('/test.*/', responsa_mode=False)
        assert mode == 'Regex'
        # The slash is consumed as the prefix, remainder is the regex
        assert 'test' in clean

    def test_dollar_title_prefix(self):
        """'$Genesis' with responsa_mode=False -> mode='Title', clean='Genesis'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('$Genesis', responsa_mode=False)
        assert mode == 'Title'
        assert clean == 'Genesis'

    def test_tilde_fuzzy_prefix(self):
        """'~test' with responsa_mode=False -> mode='fuzzy', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('~test', responsa_mode=False)
        assert mode == 'fuzzy'
        assert clean == 'test'

    def test_double_question_variants_extended(self):
        """'??test' with responsa_mode=False -> mode='variants_extended', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('??test', responsa_mode=False)
        assert mode == 'variants_extended'
        assert clean == 'test'

    def test_triple_question_variants_maximum(self):
        """'???test' with responsa_mode=False -> mode='variants_maximum', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('???test', responsa_mode=False)
        assert mode == 'variants_maximum'
        assert clean == 'test'

    def test_plain_query_returns_none_mode(self):
        """Plain query without prefix returns (None, query)."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('plain query', responsa_mode=False)
        assert mode is None
        assert clean == 'plain query'

    def test_R_prefix_responsa_mode(self):
        """'R test' with responsa_mode=False -> mode='responsa', clean='test'."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('R test', responsa_mode=False)
        # Note: 'R' prefix triggers only if followed by non-empty content after stripping
        assert mode == 'responsa'
        assert clean == 'test'
