"""
Tests for Responsa search edge cases: empty queries, flex spacing minimum length,
hash symbol mode conflicts, and explosion guard cascade with realistic expansion.

Tests cover:
- Empty and whitespace queries (no crash, returns empty)
- Flex spacing minimum length enforcement (>= 3 chars for Tantivy splits)
- Hash symbol (#) conflict between Shelfmark and Responsa modes
- Explosion guard cascade with realistic Hebrew expansion counts
"""

import re
import sys
import pytest
from unittest.mock import MagicMock, patch

import genizah_core as _gc_module

from genizah_core import (
    ResponsaComponent,
    parse_responsa_query,
    extract_per_pair_gaps,
    expand_grammatical_prefixes,
    expand_grammatical_suffixes,
    expand_plene_defective,
    expand_judeo_arabic,
    _apply_explosion_guard,
    _make_flex_spacing_pattern,
    SearchEngine,
    Config,
    GRAMMATICAL_PREFIXES,
    GRAMMATICAL_SUFFIXES,
)


@pytest.fixture(autouse=True)
def _ensure_genizah_core_module():
    """Ensure genizah_core is properly loaded in sys.modules."""
    if 'genizah_core' not in sys.modules or sys.modules['genizah_core'] is not _gc_module:
        sys.modules['genizah_core'] = _gc_module
    yield


# ============================================================================
# Helper: reuse _make_search_engine from test_responsa_integration.py pattern
# ============================================================================

def _make_search_engine():
    """Create a SearchEngine with mocked dependencies for testing."""
    meta_mgr = MagicMock()
    var_mgr = MagicMock()
    var_mgr.get_variants = MagicMock(side_effect=lambda term, mode, limit=200: [term])

    with patch.object(SearchEngine, 'reload_index', return_value=False):
        engine = SearchEngine(meta_mgr, var_mgr)

    return engine


def _default_responsa_options(**overrides):
    """Build a minimal responsa_options dict with defaults."""
    opts = {
        'responsa_mode': True,
        'variants': False,
        'ja': False,
        'flex_spacing': False,
        'bidirectional': False,
        'variant_mode': 'exact',
    }
    opts.update(overrides)
    return opts


# ============================================================================
# Class 1: Empty and whitespace queries
# ============================================================================

class TestEmptyAndWhitespaceQueries:
    """Edge cases for empty, whitespace-only, and modifier-only queries."""

    def test_empty_string_returns_empty_list(self):
        """Empty string query with responsa_mode=True returns empty list, no crash."""
        engine = _make_search_engine()
        # With searcher=None (default from _make_search_engine), returns early
        result = engine.execute_search('', 'exact', 0,
                                        responsa_options=_default_responsa_options())
        assert result == []

    def test_whitespace_only_returns_empty_list(self):
        """Whitespace-only query with responsa_mode=True returns empty list, no crash."""
        engine = _make_search_engine()
        result = engine.execute_search('   ', 'exact', 0,
                                        responsa_options=_default_responsa_options())
        assert result == []

    def test_modifier_only_query_no_crash(self):
        """Query with only modifiers (e.g., '#', '%', '*') and no actual word
        returns empty list or handles gracefully (no crash)."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_result = MagicMock()
        mock_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_result)

        # Try various modifier-only queries -- none should crash
        for query in ['#', '%', '*', '#%', '%#', '##']:
            result = engine.execute_search(query, 'exact', 0,
                                            responsa_options=_default_responsa_options())
            assert isinstance(result, list), f"Query '{query}' did not return a list"


# ============================================================================
# Class 2: Flex spacing minimum length enforcement
# ============================================================================

class TestFlexSpacingMinLength:
    """Tests for flex spacing behavior with short terms.

    The Tantivy branch has a guard: `if len(w) >= 3` for flex splits.
    The regex pattern (_make_flex_spacing_pattern) works on any length.
    """

    def test_flex_pattern_single_char(self):
        """_make_flex_spacing_pattern('a') for 1 char -- no crash, returns
        the single escaped character (no \\s* joins possible)."""
        result = _make_flex_spacing_pattern('a')
        assert result is not None
        assert len(result) > 0
        # For a single character, there are no joins -- just the escaped char
        assert r'\s*' not in result

    def test_flex_pattern_two_chars(self):
        """_make_flex_spacing_pattern('ab') for 2 chars -- produces a\\s*b pattern."""
        result = _make_flex_spacing_pattern('ab')
        assert result is not None
        assert r'\s*' in result
        # Should be like a\s*b
        assert re.match(r'a\\s\*b', result) or r'a\s*b' == result

    def test_flex_pattern_three_chars(self):
        """_make_flex_spacing_pattern('abc') for 3 chars -- produces a\\s*b\\s*c."""
        result = _make_flex_spacing_pattern('abc')
        assert result == r'a\s*b\s*c'

    def test_tantivy_flex_split_skips_short_words(self):
        """Via the full build_tantivy_query pipeline: a 2-char term with
        flex_spacing=True should NOT produce flex split clauses in the
        Tantivy query (the >= 3 guard works end-to-end)."""
        engine = _make_search_engine()

        # Component with a 2-char word and flex_spacing enabled
        comp_dicts = [{
            'tantivy_terms': ['ab'],
            'regex_terms': ['ab'],
            'original_words': ['ab'],
            'wildcard': None,
            'wildcard_pattern': None,
            'flex_patterns': [_make_flex_spacing_pattern('ab')],
            'inline_pattern': None,
        }]

        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=comp_dicts,
            responsa_options={'flex_spacing': True}
        )

        # The Tantivy query should NOT contain flex split AND clauses
        # because 'ab' is only 2 chars (< 3)
        assert 'AND' not in result, (
            "2-char term should not generate flex split AND clauses in Tantivy query"
        )

    def test_tantivy_flex_split_works_for_three_char_word(self):
        """A 3-char word with flex_spacing=True SHOULD produce flex split
        clauses in the Tantivy query."""
        engine = _make_search_engine()

        comp_dicts = [{
            'tantivy_terms': ['abc'],
            'regex_terms': ['abc'],
            'original_words': ['abc'],
            'wildcard': None,
            'wildcard_pattern': None,
            'flex_patterns': [_make_flex_spacing_pattern('abc')],
            'inline_pattern': None,
        }]

        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=comp_dicts,
            responsa_options={'flex_spacing': True}
        )

        # The 3-char word should generate flex split AND clauses
        assert 'AND' in result, (
            "3-char term should generate flex split AND clauses in Tantivy query"
        )

    def test_flex_pattern_empty_string(self):
        """_make_flex_spacing_pattern('') -- empty string returns empty string, no crash."""
        result = _make_flex_spacing_pattern('')
        assert result == ''


# ============================================================================
# Class 3: Hash symbol mode conflicts
# ============================================================================

class TestHashSymbolConflict:
    """Tests for # symbol meaning in different search modes.

    - In Responsa mode: # = grammatical prefix expansion operator
    - In non-Responsa mode: # = Shelfmark search prefix shortcut
    """

    def test_hash_in_responsa_mode_bypasses_shelfmark(self):
        """#word in Responsa mode -> parse_query_syntax returns (None, '#word')
        -- the prefix shortcut is bypassed."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#shalom', responsa_mode=True)
        assert mode is None
        assert clean == '#shalom'

    def test_hash_in_responsa_via_execute_search(self):
        """#word in Responsa mode via execute_search -> calls parse_responsa_query
        (not shelfmark search)."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_result = MagicMock()
        mock_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_result)

        with patch('genizah_core.parse_responsa_query') as mock_parse:
            mock_parse.return_value = [ResponsaComponent(words=["shalom"])]

            engine.execute_search(
                '#shalom', 'exact', 0,
                responsa_options=_default_responsa_options()
            )

            # parse_responsa_query should have been called (not shelfmark)
            mock_parse.assert_called_once()
            # Verify the query was passed to it
            call_args = mock_parse.call_args
            assert '#shalom' in call_args[0]

    def test_hash_in_shelfmark_mode_triggers_shelfmark(self):
        """#T-S 12.1 in Shelfmark mode (NOT Responsa) -> triggers Shelfmark search."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#T-S 12.1', responsa_mode=False)
        assert mode == 'Shelfmark'
        assert clean == 'T-S 12.1'

    def test_switching_from_responsa_to_non_responsa(self):
        """Switching from Responsa mode to non-Responsa -> #word triggers
        Shelfmark again (no stale state)."""
        engine = _make_search_engine()

        # First call: Responsa mode (# bypassed)
        mode1, clean1 = engine.parse_query_syntax('#test', responsa_mode=True)
        assert mode1 is None
        assert clean1 == '#test'

        # Second call: non-Responsa mode (# = Shelfmark)
        mode2, clean2 = engine.parse_query_syntax('#test', responsa_mode=False)
        assert mode2 == 'Shelfmark'
        assert clean2 == 'test'

    def test_hash_alone_in_responsa_mode_no_crash(self):
        """# alone with Responsa mode -> graceful handling (no crash)."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_result = MagicMock()
        mock_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_result)

        # Should not crash
        result = engine.execute_search('#', 'exact', 0,
                                        responsa_options=_default_responsa_options())
        assert isinstance(result, list)


# ============================================================================
# Class 4: Explosion guard end-to-end with realistic expansions
# ============================================================================

class TestExplosionGuardEndToEnd:
    """Tests for the explosion guard cascade using real expansion functions
    (not mocked) to verify realistic expansion counts trigger the guard.
    """

    def _make_mock_var_mgr(self, variants_per_term=30):
        """Create a mock VariantManager returning fixed variants per term."""
        mock = MagicMock()
        mock.get_variants = MagicMock(
            side_effect=lambda term, mode, limit=200: [f"{term}_v{i}" for i in range(variants_per_term)]
        )
        return mock

    def test_three_words_prefix_variants_ja_triggers_cascade(self):
        """3 words with #prefix + variants_maximum + JA -> should trigger
        cascade (warning returned).

        Expansion: 3 words * 24 prefixes * 30 variants * 8 JA = ~17,280 >> 500
        """
        components = [
            ResponsaComponent(words=["shalom"], grammatical_prefixes=True),
            ResponsaComponent(words=["olam"], grammatical_prefixes=True),
            ResponsaComponent(words=["torah"], grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=30)

        expanded, warning, actual_opts = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_maximum'
        )
        assert warning is not None, "Should have triggered cascade warning"

    def test_prefix_plus_suffix_cascades_down_instead_of_error(self):
        """1 word with #prefix + #suffix (both) -> 24 prefixes * 25 suffixes = 600
        which exceeds 500 even without variants. With expanded cascade, the guard
        should disable suffixes (bringing it to 24*5=120) instead of raising ValueError."""
        components = [
            ResponsaComponent(
                words=["shalom"],
                grammatical_prefixes=True,
                grammatical_suffixes=True,
            ),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)

        expanded, warning, actual_opts = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=False,
            var_mgr=var_mgr,
            variant_mode='variants'
        )
        # Cascade should have disabled suffixes (and possibly variants) instead of erroring
        assert warning is not None, "Should have triggered cascade warning"
        assert "suffix" in warning.lower() or "Grammatical suffix" in warning, (
            f"Warning should mention suffix disabling, got: {warning}"
        )
        # The component's grammatical_suffixes should be False after cascade
        assert expanded[0].grammatical_suffixes is False

    def test_two_words_prefix_only_no_variants_under_limit(self):
        """2 words with #prefix only, variants=False, ja=False ->
        2 * 24 = 48 total, well under 500. No warning."""
        components = [
            ResponsaComponent(words=["shalom"], grammatical_prefixes=True),
            ResponsaComponent(words=["olam"], grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)

        expanded, warning, actual_opts = _apply_explosion_guard(
            components,
            variants_on=False,
            ja_on=False,
            var_mgr=var_mgr,
            variant_mode='exact'
        )
        assert warning is None, "Should be under limit with no warning"
        assert actual_opts['variants_on'] is False
        assert actual_opts['ja_on'] is False

    def test_real_expansion_counts_verify_prefix_count(self):
        """Verify that actual expand_grammatical_prefixes returns 24 forms,
        confirming the explosion guard's estimation is realistic."""
        forms = expand_grammatical_prefixes("test")
        assert len(forms) == len(GRAMMATICAL_PREFIXES), (
            f"Expected {len(GRAMMATICAL_PREFIXES)} prefix forms, got {len(forms)}"
        )

    def test_real_expansion_counts_verify_suffix_count(self):
        """Verify that actual expand_grammatical_suffixes returns 25 forms,
        confirming the explosion guard's estimation is realistic."""
        forms = expand_grammatical_suffixes("test")
        assert len(forms) == len(GRAMMATICAL_SUFFIXES), (
            f"Expected {len(GRAMMATICAL_SUFFIXES)} suffix forms, got {len(forms)}"
        )

    def test_cascade_downgrades_preserve_search_ability(self):
        """After cascade downgrade, the returned components still have enough
        data to perform a search (words list is not empty)."""
        components = [
            ResponsaComponent(words=["word1"], grammatical_prefixes=True),
            ResponsaComponent(words=["word2"], grammatical_prefixes=True),
            ResponsaComponent(words=["word3"], grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=100)

        expanded, warning, actual_opts = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_maximum'
        )
        # Even after downgrade, components should still be valid
        assert len(expanded) == 3
        for comp in expanded:
            assert len(comp.words) >= 1
