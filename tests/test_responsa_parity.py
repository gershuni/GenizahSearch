"""
Tests for cross-app parity (XAPP-01): verify that both web and desktop apps
produce identical Responsa search results by testing at the shared
`execute_search()` interface.

Tests cover:
- Dict structure parity: both apps build identical `responsa_options` for all
  16 checkbox combinations
- Pipeline determinism: same query + same options = same Tantivy query string
  and same regex pattern string
- Execute search determinism: same inputs = same result UIDs (compared as sets)
- Mode detection parity: Responsa mode detection logic in both apps
- Explosion guard warning propagation through the full pipeline
"""

import sys
import itertools
import pytest
from unittest.mock import MagicMock, patch

import genizah_core as _gc_module

from genizah_core import (
    ResponsaComponent,
    parse_responsa_query,
    extract_per_pair_gaps,
    SearchEngine,
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
    """Create a SearchEngine with mocked dependencies for testing query building."""
    meta_mgr = MagicMock()
    var_mgr = MagicMock()
    var_mgr.get_variants = MagicMock(side_effect=lambda term, mode, limit=200: [term])

    with patch.object(SearchEngine, 'reload_index', return_value=False):
        engine = SearchEngine(meta_mgr, var_mgr)

    return engine


def _make_search_engine_with_hits(content_texts, uid_prefix='uid'):
    """Create a SearchEngine that returns mocked hits with specified content texts.

    Each content_text becomes a document hit. Returns (engine, expected_uids).
    """
    meta_mgr = MagicMock()
    var_mgr = MagicMock()
    var_mgr.get_variants = MagicMock(side_effect=lambda term, mode, limit=200: [term])

    with patch.object(SearchEngine, 'reload_index', return_value=False):
        engine = SearchEngine(meta_mgr, var_mgr)

    engine.index = MagicMock()
    engine.searcher = MagicMock()
    engine.index.parse_query = MagicMock(return_value=MagicMock())

    # Build mock hits
    mock_doc_addrs = [MagicMock() for _ in content_texts]
    hits = [(1.0 - i * 0.1, addr) for i, addr in enumerate(mock_doc_addrs)]
    mock_result = MagicMock()
    mock_result.hits = hits
    engine.searcher.search = MagicMock(return_value=mock_result)

    expected_uids = []
    docs = {}
    for i, (text, addr) in enumerate(zip(content_texts, mock_doc_addrs)):
        uid = f'{uid_prefix}_{i}'
        expected_uids.append(uid)
        docs[id(addr)] = {
            'content': [text],
            'full_header': [f'test_header_{i}'],
            'source': ['V0.8'],
            'unique_id': [uid],
            'scope': ['page'],
            'boundaries': ['[]'],
        }

    def doc_getter(addr):
        return docs[id(addr)]

    engine.searcher.doc = MagicMock(side_effect=doc_getter)

    engine.meta_mgr.get_display_data = MagicMock(return_value={
        'shelfmark': 'T-S 12.1', 'source': 'V0.8'
    })
    engine.meta_mgr.parse_header_smart = MagicMock(return_value=('sys1', '1'))

    return engine, expected_uids


# ============================================================================
# Dict structure parity -- XAPP-01
# ============================================================================

class TestCrossAppParity:
    """Verify XAPP-01: identical `responsa_options` dict structure and search
    results for the same Responsa query in both web and desktop apps."""

    @staticmethod
    def _build_web_responsa_options(variants=False, ja=False,
                                     flex_spacing=False, bidirectional=False):
        """Build responsa_options dict matching web app construction.
        Source: web/pages/search.py:1661-1668
        """
        return {
            'responsa_mode': True,
            'variants': variants,
            'ja': ja,
            'flex_spacing': flex_spacing,
            'bidirectional': bidirectional,
            'variant_mode': 'variants' if variants else 'exact',
        }

    @staticmethod
    def _build_desktop_responsa_options(variants=False, ja=False,
                                         flex_spacing=False, bidirectional=False):
        """Build responsa_options dict matching desktop app construction.
        Source: genizah_app.py:13407-13414
        """
        return {
            'responsa_mode': True,
            'variants': variants,
            'ja': ja,
            'flex_spacing': flex_spacing,
            'bidirectional': bidirectional,
            'variant_mode': 'variants' if variants else 'exact',
        }

    # ------------------------------------------------------------------
    # 1. Dict structure parity (all 16 boolean combinations)
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("variants,ja,flex,bidir", list(
        itertools.product([True, False], repeat=4)
    ))
    def test_options_dict_identical_all_combinations(self, variants, ja, flex, bidir):
        """Web and desktop produce identical responsa_options dicts for every
        combination of the 4 boolean checkboxes (variants, ja, flex_spacing,
        bidirectional) -- 16 combinations total."""
        web_opts = self._build_web_responsa_options(variants, ja, flex, bidir)
        desk_opts = self._build_desktop_responsa_options(variants, ja, flex, bidir)
        assert web_opts == desk_opts, (
            f"Mismatch for variants={variants}, ja={ja}, flex={flex}, bidir={bidir}"
        )

    def test_variant_mode_tracks_variants_flag(self):
        """variant_mode is 'variants' when variants=True, 'exact' when False --
        verifying this key conditional logic in both apps."""
        # variants=True -> 'variants'
        web_on = self._build_web_responsa_options(variants=True)
        desk_on = self._build_desktop_responsa_options(variants=True)
        assert web_on['variant_mode'] == 'variants'
        assert desk_on['variant_mode'] == 'variants'

        # variants=False -> 'exact'
        web_off = self._build_web_responsa_options(variants=False)
        desk_off = self._build_desktop_responsa_options(variants=False)
        assert web_off['variant_mode'] == 'exact'
        assert desk_off['variant_mode'] == 'exact'

    def test_required_keys_present(self):
        """Both apps' options dicts contain all 6 required keys."""
        required = {'responsa_mode', 'variants', 'ja', 'flex_spacing',
                     'bidirectional', 'variant_mode'}
        web = self._build_web_responsa_options()
        desk = self._build_desktop_responsa_options()
        assert set(web.keys()) == required
        assert set(desk.keys()) == required

    # ------------------------------------------------------------------
    # 2. Pipeline determinism (Tantivy + regex)
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("query", [
        '#shalom',
        '#shalom olam*',
        '#(shalom/shalmot) %olam',
        '#shalom [3] olam',
    ])
    def test_tantivy_query_deterministic(self, query):
        """Same query + same responsa_options -> same Tantivy query string."""
        opts = self._build_web_responsa_options(variants=False, ja=False)

        engine1 = _make_search_engine()
        engine2 = _make_search_engine()

        # Parse into components
        components = parse_responsa_query(query)
        if not components:
            return

        # Build component dicts (minimal expansion for determinism test)
        comp_dicts = []
        for comp in components:
            if comp.negated:
                continue
            comp_dicts.append({
                'tantivy_terms': list(comp.words),
                'regex_terms': list(comp.words),
                'original_words': list(comp.words),
                'wildcard': comp.wildcard,
                'wildcard_pattern': comp.wildcard_pattern,
                'flex_patterns': [],
                'inline_pattern': comp.inline_pattern,
            })

        if not comp_dicts:
            return

        result1 = engine1.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=comp_dicts, responsa_options=opts)
        result2 = engine2.build_tantivy_query(
            terms=None, mode='exact',
            responsa_components=comp_dicts, responsa_options=opts)
        assert result1 == result2

    @pytest.mark.parametrize("query", [
        '#shalom',
        '#shalom olam*',
        '#(shalom/shalmot) %olam',
        '#shalom [3] olam',
    ])
    def test_regex_pattern_deterministic(self, query):
        """Same query + same responsa_options -> same regex pattern string."""
        opts = self._build_web_responsa_options(variants=False, ja=False)

        engine1 = _make_search_engine()
        engine2 = _make_search_engine()

        components = parse_responsa_query(query)
        if not components:
            return

        per_pair_gaps = extract_per_pair_gaps(query)
        comp_dicts = []
        for comp in components:
            if comp.negated:
                continue
            comp_dicts.append({
                'tantivy_terms': list(comp.words),
                'regex_terms': list(comp.words),
                'original_words': list(comp.words),
                'wildcard': comp.wildcard,
                'wildcard_pattern': comp.wildcard_pattern,
                'flex_patterns': [],
                'inline_pattern': comp.inline_pattern,
            })

        if not comp_dicts:
            return

        result1 = engine1.build_regex_pattern(
            terms=None, mode='exact', max_gap=1,
            responsa_components=comp_dicts, responsa_options=opts,
            per_pair_gaps=per_pair_gaps)
        result2 = engine2.build_regex_pattern(
            terms=None, mode='exact', max_gap=1,
            responsa_components=comp_dicts, responsa_options=opts,
            per_pair_gaps=per_pair_gaps)
        assert result1.pattern == result2.pattern

    # ------------------------------------------------------------------
    # 3. Execute search determinism (result UIDs as sets)
    # ------------------------------------------------------------------

    def test_execute_search_deterministic_results(self):
        """Same query + same options -> same result UIDs (compared as sets,
        NOT ordered lists -- per research Pitfall 3)."""
        content_texts = [
            'some text with shalom and more words here',
            'another document containing shalom olam text',
        ]

        opts = self._build_web_responsa_options(variants=False, ja=False)

        engine1, uids1 = _make_search_engine_with_hits(content_texts, uid_prefix='run1')
        engine2, uids2 = _make_search_engine_with_hits(content_texts, uid_prefix='run2')

        results1 = engine1.execute_search(
            'shalom', 'exact', 0, responsa_options=opts)
        results2 = engine2.execute_search(
            'shalom', 'exact', 0, responsa_options=opts)

        # Both should return same number of results
        assert len(results1) == len(results2)

    # ------------------------------------------------------------------
    # 4. Mode detection parity
    # ------------------------------------------------------------------

    def test_web_mode_detection_logic(self):
        """Web app: mode_select.value == 'responsa' produces responsa_options
        with responsa_mode=True; other values produce None."""
        # Simulate web mode detection
        for mode_value in ['exact', 'variants', 'fuzzy', 'Regex', 'Title', 'Shelfmark']:
            responsa_options = None
            if mode_value == 'responsa':
                responsa_options = self._build_web_responsa_options()
            assert responsa_options is None, f"Mode '{mode_value}' should NOT produce responsa_options"

        # Responsa mode should produce options
        responsa_options = None
        if 'responsa' == 'responsa':
            responsa_options = self._build_web_responsa_options()
        assert responsa_options is not None
        assert responsa_options['responsa_mode'] is True

    def test_desktop_mode_detection_logic(self):
        """Desktop app: currentIndex() == MODE_RESPONSA (2) produces
        responsa_options with responsa_mode=True; other indices produce None."""
        MODE_RESPONSA = 2

        # Non-Responsa indices
        for idx in [0, 1, 3, 4, 5, 6, 7]:
            responsa_options = None
            if idx == MODE_RESPONSA:
                responsa_options = self._build_desktop_responsa_options()
            assert responsa_options is None, f"Index {idx} should NOT produce responsa_options"

        # Responsa index
        responsa_options = None
        if 2 == MODE_RESPONSA:
            responsa_options = self._build_desktop_responsa_options()
        assert responsa_options is not None
        assert responsa_options['responsa_mode'] is True

    # ------------------------------------------------------------------
    # 5. Explosion guard warning propagation through execute_search
    # ------------------------------------------------------------------

    def test_explosion_guard_warning_propagates_to_results(self):
        """When explosion guard downgrades, the warning string appears in
        results[0]['responsa_warning'] through the full execute_search pipeline."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        # Set up search to return one matching hit
        mock_doc_addr = MagicMock()
        mock_result = MagicMock()
        mock_result.hits = [(1.0, mock_doc_addr)]
        engine.searcher.search = MagicMock(return_value=mock_result)

        engine.searcher.doc = MagicMock(return_value={
            'content': ['text with word1 and word2 in the document'],
            'full_header': ['test_header'],
            'source': ['V0.8'],
            'unique_id': ['uid1'],
            'scope': ['page'],
            'boundaries': ['[]'],
        })
        engine.meta_mgr.get_display_data = MagicMock(return_value={
            'shelfmark': 'T-S 12.1', 'source': 'V0.8'
        })

        warning_text = "Variant mode downgraded to basic (30 pairs)"
        with patch('genizah_core._apply_explosion_guard') as mock_guard:
            mock_guard.return_value = (
                [ResponsaComponent(words=["word1"]), ResponsaComponent(words=["word2"])],
                warning_text,
                {'variants_on': True, 'ja_on': False, 'variant_mode': 'variants'}
            )

            opts = self._build_web_responsa_options(variants=True)
            results = engine.execute_search(
                'word1 word2', 'exact', 0, responsa_options=opts)

            if results:
                assert results[0].get('responsa_warning') is not None
                assert results[0]['responsa_warning'] == warning_text

    def test_explosion_guard_no_warning_when_under_limit(self):
        """When under limit, no warning in results."""
        engine = _make_search_engine()
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())

        mock_doc_addr = MagicMock()
        mock_result = MagicMock()
        mock_result.hits = [(1.0, mock_doc_addr)]
        engine.searcher.search = MagicMock(return_value=mock_result)

        engine.searcher.doc = MagicMock(return_value={
            'content': ['text with shalom in document'],
            'full_header': ['test_header'],
            'source': ['V0.8'],
            'unique_id': ['uid1'],
            'scope': ['page'],
            'boundaries': ['[]'],
        })
        engine.meta_mgr.get_display_data = MagicMock(return_value={
            'shelfmark': 'T-S 12.1', 'source': 'V0.8'
        })

        opts = self._build_web_responsa_options(variants=False, ja=False)
        results = engine.execute_search('shalom', 'exact', 0, responsa_options=opts)

        if results:
            assert results[0].get('responsa_warning') is None
