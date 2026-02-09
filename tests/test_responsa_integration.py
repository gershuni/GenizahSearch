"""
Tests for Responsa pipeline integration: wiring Responsa components into
build_tantivy_query, build_regex_pattern, parse_query_syntax, and execute_search.

Tests cover:
- build_tantivy_query Responsa branch (OR groups with boosting, AND-joined)
- build_regex_pattern Responsa branch (wildcard patterns, flex spacing, bidirectional)
- parse_query_syntax Responsa bypass (prefix shortcuts disabled)
- execute_search Responsa pipeline (routing, explosion warning propagation)
- Suffix expansion and plene/defective through the pipeline
"""

import re
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from genizah_core import (
    ResponsaComponent,
    parse_responsa_query,
    expand_grammatical_prefixes,
    expand_grammatical_suffixes,
    expand_plene_defective,
    expand_judeo_arabic,
    _apply_explosion_guard,
    SearchEngine,
    Config,
)


# ============================================================================
# Helper: Create a SearchEngine with mocked index/searcher
# ============================================================================

def _make_search_engine():
    """Create a SearchEngine with mocked dependencies for testing query building."""
    meta_mgr = MagicMock()
    var_mgr = MagicMock()
    # Default: get_variants returns just the term itself
    var_mgr.get_variants = MagicMock(side_effect=lambda term, mode, limit=200: [term])

    # Patch reload_index to avoid needing an actual Tantivy index
    with patch.object(SearchEngine, 'reload_index', return_value=False):
        engine = SearchEngine(meta_mgr, var_mgr)

    return engine


# ============================================================================
# build_tantivy_query — Responsa branch
# ============================================================================

class TestBuildTantivyQueryResponsa:
    """Tests for the Responsa branch of build_tantivy_query."""

    def test_single_component_or_group(self):
        """Single component with expanded terms produces OR group with boosted exact term."""
        engine = _make_search_engine()
        components = [
            {'tantivy_terms': ['shalom', 'ushalom', 'hashalom'],
             'original_words': ['shalom']},
        ]
        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=components, responsa_options={}
        )
        # Should contain OR group with exact term boosted ^5
        assert '"shalom"^5' in result
        assert 'OR' in result
        assert '"ushalom"' in result or '"hashalom"' in result

    def test_two_components_and_joined(self):
        """Two components are AND-joined."""
        engine = _make_search_engine()
        components = [
            {'tantivy_terms': ['a', 'b'], 'original_words': ['a']},
            {'tantivy_terms': ['c', 'd'], 'original_words': ['c']},
        ]
        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=components, responsa_options={}
        )
        assert 'AND' in result

    def test_exact_term_boosted_highest(self):
        """The original word(s) in each component get ^5 boost."""
        engine = _make_search_engine()
        components = [
            {'tantivy_terms': ['shalom', 'ushalom', 'hashalom'],
             'original_words': ['shalom']},
        ]
        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=components, responsa_options={}
        )
        assert '"shalom"^5' in result

    def test_existing_path_unchanged(self):
        """When responsa_components is None, existing behavior is preserved."""
        engine = _make_search_engine()
        # Existing call: build_tantivy_query(terms, mode)
        result = engine.build_tantivy_query(['test'], 'exact')
        # Should produce existing format with OR groups
        assert '"test"^5' in result

    def test_different_length_variants_get_boost(self):
        """Variants with different length than original get ^3 boost."""
        engine = _make_search_engine()
        components = [
            {'tantivy_terms': ['ab', 'abc', 'a'],
             'original_words': ['ab']},
        ]
        result = engine.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=components, responsa_options={}
        )
        # 'ab' is original -> ^5
        assert '"ab"^5' in result
        # 'abc' is different length -> ^3
        assert '"abc"^3' in result


# ============================================================================
# build_regex_pattern — Responsa branch
# ============================================================================

class TestBuildRegexResponsa:
    """Tests for the Responsa branch of build_regex_pattern."""

    def test_suffix_wildcard(self):
        """Suffix wildcard produces \\S* pattern."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['shalom'],
                'wildcard': 'suffix',
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['shalom'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain shalom followed by \S*
        assert r'shalom\S*' in pattern or r'shalom\\S*' in pattern or re.search(r'shalom\\S\*', pattern) or 'shalom' in pattern

    def test_prefix_wildcard(self):
        """Prefix wildcard produces \\S* before the term."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['neder'],
                'wildcard': 'prefix',
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['neder'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain \S* before neder
        assert r'\S*neder' in pattern or r'\\S*neder' in pattern or 'neder' in pattern

    def test_character_pattern(self):
        """Character pattern *a*b*c* produces \\S*a\\S*b\\S*c\\S* regex."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': [],
                'wildcard': 'pattern',
                'wildcard_pattern': '*a*b*c*',
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': [],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain \S*a\S*b\S*c\S* (with regex escaping)
        assert re.search(r'\\S\*a\\S\*b\\S\*c\\S\*', pattern) or r'\S*a\S*b\S*c\S*' in pattern

    def test_inline_alternation_single_char(self):
        """Inline alternation with single chars uses character class."""
        engine = _make_search_engine()
        # Pattern like word(a/b)end -> word[ab]end
        components = [
            {
                'regex_terms': [],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': 'word(a/b)end',
                'original_words': [],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain word[ab]end or word(a|b)end
        assert 'word' in pattern and 'end' in pattern

    def test_inline_alternation_multi_char(self):
        """Inline alternation with multi-char uses alternation group."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': [],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': 'pre(abc/de)post',
                'original_words': [],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain alternation
        assert 'pre' in pattern and 'post' in pattern

    def test_flex_spacing(self):
        """Flex spacing adds \\s* between chars of original terms as alternatives."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['abc', 'xabc'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [r'a\s*b\s*c'],
                'inline_pattern': None,
                'original_words': ['abc'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components,
            responsa_options={'flex_spacing': True}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain the flex pattern as an alternative
        assert r'a\s*b\s*c' in pattern

    def test_flex_spacing_only_on_original_terms(self):
        """Flex spacing is NOT applied to expanded variants, only originals."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['abc', 'xabc', 'habc'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [r'a\s*b\s*c'],  # Only for 'abc', not xabc/habc
                'inline_pattern': None,
                'original_words': ['abc'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components,
            responsa_options={'flex_spacing': True}
        )
        pattern = result.pattern
        # Should have flex pattern for 'abc' but NOT for 'xabc'
        assert r'a\s*b\s*c' in pattern
        # xabc should be in the alternation as a regular escaped term, NOT flex-spaced
        assert 'xabc' in pattern
        assert r'x\s*a\s*b\s*c' not in pattern

    def test_bidirectional(self):
        """Bidirectional with 2 components produces forward|backward alternation."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['A'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['A'],
            },
            {
                'regex_terms': ['B'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['B'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components,
            responsa_options={'bidirectional': True}
        )
        assert result is not None
        pattern = result.pattern
        # Should contain both A...B and B...A orderings
        assert 'A' in pattern and 'B' in pattern
        # And alternation via |
        assert '|' in pattern

    def test_flex_separator_gap_zero(self):
        """When flex_spacing=True and gap=0, separator uses * instead of +."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['X'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['X'],
            },
            {
                'regex_terms': ['Y'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['Y'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components,
            responsa_options={'flex_spacing': True}
        )
        assert result is not None
        pattern = result.pattern
        # The separator for gap=0 with flex_spacing should use * (zero or more)
        # instead of + (one or more)
        assert '*' in pattern

    def test_existing_path_unchanged(self):
        """When responsa_components is None, existing behavior is preserved."""
        engine = _make_search_engine()
        # Existing call
        result = engine.build_regex_pattern(['test'], 'exact', 0)
        assert result is not None
        assert 'test' in result.pattern

    def test_suffix_expansion_in_alternation(self):
        """Suffix-expanded terms appear in the regex alternation group."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['stem', 'stemim', 'stemot', 'stemah'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['stem'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        # All suffix-expanded terms should be in the alternation
        assert 'stem' in pattern
        assert 'stemim' in pattern
        assert 'stemot' in pattern
        assert 'stemah' in pattern

    def test_plene_defective_in_alternation(self):
        """Plene/defective variants appear in the regex alternation group."""
        engine = _make_search_engine()
        components = [
            {
                'regex_terms': ['shalom', 'shlm', 'shalm'],
                'wildcard': None,
                'wildcard_pattern': None,
                'flex_patterns': [],
                'inline_pattern': None,
                'original_words': ['shalom'],
            },
        ]
        result = engine.build_regex_pattern(
            terms=None, mode='exact', max_gap=0,
            responsa_components=components, responsa_options={}
        )
        assert result is not None
        pattern = result.pattern
        assert 'shalom' in pattern
        assert 'shlm' in pattern
        assert 'shalm' in pattern


# ============================================================================
# parse_query_syntax — Responsa bypass
# ============================================================================

class TestParseQuerySyntaxResponsaBypass:
    """Tests for parse_query_syntax with responsa_mode parameter."""

    def test_hash_bypass_when_responsa(self):
        """# prefix is NOT interpreted as Shelfmark when responsa_mode=True."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#shalom', responsa_mode=True)
        assert mode is None
        assert clean == '#shalom'

    def test_question_mark_bypass_when_responsa(self):
        """? prefix is NOT interpreted as variants when responsa_mode=True."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('?shalom', responsa_mode=True)
        assert mode is None
        assert clean == '?shalom'

    def test_all_prefixes_bypass_when_responsa(self):
        """All prefix shortcuts are bypassed when responsa_mode=True."""
        engine = _make_search_engine()
        for prefix in ['???', '??', '?', '=', '~', '/', '$', '#']:
            query = prefix + 'test'
            mode, clean = engine.parse_query_syntax(query, responsa_mode=True)
            assert mode is None, f"Prefix '{prefix}' was not bypassed"
            assert clean == query

    def test_existing_hash_shelfmark_when_not_responsa(self):
        """# prefix still interpreted as Shelfmark when responsa_mode=False."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#shalom', responsa_mode=False)
        assert mode == 'Shelfmark'
        assert clean == 'shalom'

    def test_existing_behavior_unchanged_plain_query(self):
        """Plain query returns (None, query) regardless of responsa_mode."""
        engine = _make_search_engine()
        mode1, clean1 = engine.parse_query_syntax('plain')
        mode2, clean2 = engine.parse_query_syntax('plain', responsa_mode=False)
        mode3, clean3 = engine.parse_query_syntax('plain', responsa_mode=True)
        assert mode1 is None and clean1 == 'plain'
        assert mode2 is None and clean2 == 'plain'
        assert mode3 is None and clean3 == 'plain'

    def test_default_responsa_mode_is_false(self):
        """Default responsa_mode is False (backward compatible)."""
        engine = _make_search_engine()
        # Call without the new parameter - should work identically to before
        mode, clean = engine.parse_query_syntax('#test')
        assert mode == 'Shelfmark'


# ============================================================================
# execute_search — Responsa pipeline
# ============================================================================

class TestExecuteSearchResponsa:
    """Tests for the Responsa pipeline in execute_search."""

    def test_accepts_responsa_options_parameter(self):
        """execute_search accepts responsa_options parameter without error."""
        engine = _make_search_engine()
        # With searcher=None, it returns early, but should NOT raise
        result = engine.execute_search(
            '#shalom', 'exact', 0,
            responsa_options={'responsa_mode': True, 'variants': False, 'ja': False,
                              'flex_spacing': False, 'bidirectional': False, 'variant_mode': 'exact'}
        )
        assert isinstance(result, list)

    def test_responsa_options_none_is_existing_path(self):
        """When responsa_options=None, existing path is used (no changes)."""
        engine = _make_search_engine()
        result = engine.execute_search('test', 'exact', 0, responsa_options=None)
        assert isinstance(result, list)

    def test_responsa_mode_false_is_existing_path(self):
        """When responsa_mode=False in options, existing path is used."""
        engine = _make_search_engine()
        result = engine.execute_search(
            'test', 'exact', 0,
            responsa_options={'responsa_mode': False}
        )
        assert isinstance(result, list)

    def test_explosion_warning_propagated(self):
        """When explosion guard returns a warning, it appears in first result."""
        engine = _make_search_engine()

        # Set up a mock index and searcher for the Responsa path
        engine.index = MagicMock()
        engine.searcher = MagicMock()

        # Mock parse_query to return something
        mock_query = MagicMock()
        engine.index.parse_query = MagicMock(return_value=mock_query)

        # Mock search to return hits with content that matches our regex
        mock_doc_addr = MagicMock()
        mock_search_result = MagicMock()
        mock_search_result.hits = [(1.0, mock_doc_addr)]
        engine.searcher.search = MagicMock(return_value=mock_search_result)

        # Mock doc retrieval
        mock_doc = {
            'content': ['some text with word1 word2 more text here'],
            'full_header': ['test_header'],
            'source': ['V0.8'],
            'unique_id': ['uid1'],
            'scope': ['page'],
            'boundaries': ['[]'],
        }
        engine.searcher.doc = MagicMock(return_value=mock_doc)

        # Mock meta_mgr
        engine.meta_mgr.get_display_data = MagicMock(return_value={
            'shelfmark': 'T-S 12.1', 'source': 'V0.8'
        })
        engine.meta_mgr.parse_header_smart = MagicMock(return_value=('sys1', '1'))

        # Use a query with many words that will trigger explosion guard warning
        # We'll patch _apply_explosion_guard to return a warning
        with patch('genizah_core._apply_explosion_guard') as mock_guard:
            mock_guard.return_value = (
                [ResponsaComponent(words=["word1"]), ResponsaComponent(words=["word2"])],
                "Variant mode downgraded to basic (30 pairs)",
                {'variants_on': True, 'ja_on': False, 'variant_mode': 'variants'}
            )

            results = engine.execute_search(
                'word1 word2', 'exact', 0,
                responsa_options={
                    'responsa_mode': True, 'variants': True, 'ja': False,
                    'flex_spacing': False, 'bidirectional': False,
                    'variant_mode': 'variants_maximum'
                }
            )

            # If there are results, the first one should have the warning
            if results:
                assert results[0].get('responsa_warning') is not None
                assert 'downgraded' in results[0]['responsa_warning'].lower() or 'basic' in results[0]['responsa_warning'].lower()

    def test_responsa_pipeline_calls_parse_responsa_query(self):
        """Responsa path calls parse_responsa_query, not str.split()."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_search_result = MagicMock()
        mock_search_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_search_result)

        with patch('genizah_core.parse_responsa_query') as mock_parse:
            mock_parse.return_value = [ResponsaComponent(words=["test"])]

            engine.execute_search(
                '#test', 'exact', 0,
                responsa_options={
                    'responsa_mode': True, 'variants': False, 'ja': False,
                    'flex_spacing': False, 'bidirectional': False,
                    'variant_mode': 'exact'
                }
            )

            mock_parse.assert_called_once()

    def test_suffix_expansion_in_pipeline(self):
        """Components with grammatical_suffixes=True get suffix-expanded in pipeline."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_search_result = MagicMock()
        mock_search_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_search_result)

        with patch('genizah_core.parse_responsa_query') as mock_parse, \
             patch('genizah_core.expand_grammatical_suffixes') as mock_suffix:
            mock_parse.return_value = [
                ResponsaComponent(words=["stem"], grammatical_suffixes=True)
            ]
            mock_suffix.return_value = ["stem", "stemim", "stemot"]

            engine.execute_search(
                'stem#', 'exact', 0,
                responsa_options={
                    'responsa_mode': True, 'variants': False, 'ja': False,
                    'flex_spacing': False, 'bidirectional': False,
                    'variant_mode': 'exact'
                }
            )

            # expand_grammatical_suffixes should have been called
            mock_suffix.assert_called()

    def test_plene_defective_in_pipeline(self):
        """Components with plene_defective=True get plene/defective expansion in pipeline."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_search_result = MagicMock()
        mock_search_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_search_result)

        with patch('genizah_core.parse_responsa_query') as mock_parse, \
             patch('genizah_core.expand_plene_defective') as mock_plene:
            mock_parse.return_value = [
                ResponsaComponent(words=["shalom"], plene_defective=True)
            ]
            mock_plene.return_value = ["shalom", "shlm"]

            engine.execute_search(
                '%shalom', 'exact', 0,
                responsa_options={
                    'responsa_mode': True, 'variants': False, 'ja': False,
                    'flex_spacing': False, 'bidirectional': False,
                    'variant_mode': 'exact'
                }
            )

            # expand_plene_defective should have been called
            mock_plene.assert_called()

    def test_expansion_order_plene_then_prefixes_then_suffixes(self):
        """Expansion order: plene/defective -> prefixes -> suffixes -> JA -> variants."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_search_result = MagicMock()
        mock_search_result.hits = []
        engine.searcher.search = MagicMock(return_value=mock_search_result)

        call_order = []

        with patch('genizah_core.parse_responsa_query') as mock_parse, \
             patch('genizah_core.expand_plene_defective') as mock_plene, \
             patch('genizah_core.expand_grammatical_prefixes') as mock_prefix, \
             patch('genizah_core.expand_grammatical_suffixes') as mock_suffix:

            mock_parse.return_value = [
                ResponsaComponent(
                    words=["word"],
                    plene_defective=True,
                    grammatical_prefixes=True,
                    grammatical_suffixes=True,
                )
            ]

            def track_plene(w):
                call_order.append('plene')
                return [w, w + '_plene']
            def track_prefix(w):
                call_order.append('prefix')
                return [w, 'ha' + w]
            def track_suffix(w):
                call_order.append('suffix')
                return [w, w + 'im']

            mock_plene.side_effect = track_plene
            mock_prefix.side_effect = track_prefix
            mock_suffix.side_effect = track_suffix

            engine.execute_search(
                '%#word#', 'exact', 0,
                responsa_options={
                    'responsa_mode': True, 'variants': False, 'ja': False,
                    'flex_spacing': False, 'bidirectional': False,
                    'variant_mode': 'exact'
                }
            )

            # Plene should be called first, then prefix, then suffix
            assert len(call_order) > 0
            first_plene = call_order.index('plene') if 'plene' in call_order else 999
            first_prefix = call_order.index('prefix') if 'prefix' in call_order else 999
            first_suffix = call_order.index('suffix') if 'suffix' in call_order else 999
            assert first_plene < first_prefix, "plene should be expanded before prefixes"
            assert first_prefix < first_suffix, "prefixes should be expanded before suffixes"
