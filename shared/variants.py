# -*- coding: utf-8 -*-
"""Spelling variant generation for Hebrew search terms.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import VariantManager`` callers continue working.
"""

import itertools
from collections import defaultdict
from typing import Mapping

from shared.config import Config

try:
    from unified_variants import UNIFIED_VARIANT_PAIRS
except ImportError:
    UNIFIED_VARIANT_PAIRS = []


class VariantManager:
    """
    Generate spelling variants for Hebrew search terms using unified frequency-based pairs.

    Features:
    1. Unified variant pairs: both 1<->1 and 2<->1 substitutions sorted by frequency
    2. Slider-based selection: use top N pairs based on user setting
    3. Dynamic max_changes based on term length to prevent combinatorial explosion
    4. LRU caching for frequently searched terms
    5. Early termination with smarter limit handling
    """

    # Mode-to-pairs mapping: how many pairs to use for each search mode
    # Matches preset buttons: Basic (30), Extended (70), Maximum (150)
    # User slider overrides these defaults when enabled
    _MODE_PAIRS_COUNT = {
        'variants': 30,           # Basic (?): top 30 most frequent pairs
        'variants_extended': 70,  # Extended (??): top 70 pairs
        'variants_maximum': 150,  # Maximum (???): top 150 pairs
    }

    # Tier configuration for balanced flexibility vs explosion prevention
    _TIER_CONFIG = {
        'variants': {'max_changes': 1, 'per_term_limit': 50},
        'variants_extended': {'max_changes': 2, 'per_term_limit': 100},
        'variants_maximum': {'max_changes': 2, 'per_term_limit': 200},
    }

    @staticmethod
    def make_multimap(pairs):
        """Create bidirectional mapping from character pairs."""
        m = defaultdict(set)
        for a, b in pairs:
            m[a].add(b)
            m[b].add(a)
        return m

    def __init__(self, settings=None):
        # Settings reference (can be updated later via set_settings)
        self._settings = settings

        # Cache for frequently searched terms
        self._cache = {}
        self._cache_max_size = 5000

        # Build maps (will include custom variants if settings has them)
        self._rebuild_maps()

    def _get_custom_pairs(self) -> tuple:
        """
        Parse custom variants from settings.
        Format: dict of 'a=b' style strings, e.g. {'q=a': True, 'kv=m': True}
        Returns (single_char_pairs, multi_char_pairs) tuple.
        Single-char pairs: both sides are 1 character (for regular variant maps)
        Multi-char pairs: at least one side has >1 character (for string substitution)
        """
        if not self._settings:
            return [], []

        custom = getattr(self._settings, 'custom_variants', {})
        if not custom:
            return [], []

        single_pairs = []
        multi_pairs = []
        for key in custom:
            if '=' in key:
                parts = key.split('=', 1)
                if len(parts) == 2:
                    a, b = parts[0].strip(), parts[1].strip()
                    if a and b:
                        if len(a) == 1 and len(b) == 1:
                            single_pairs.append((a, b))
                        else:
                            multi_pairs.append((a, b))
        return single_pairs, multi_pairs

    # Maximum multichar variants per term to prevent explosion
    MAX_MULTICHAR_VARIANTS = 8

    def _get_pairs_count(self, mode: str = None) -> int:
        """
        Get the number of variant pairs to use.
        Settings slider value takes precedence over mode defaults.
        """
        # If settings has explicit pairs count, use it
        if self._settings:
            count = getattr(self._settings, 'variant_pairs_count', None)
            if count is not None:
                return count

        # Fall back to mode-based defaults
        if mode:
            return self._MODE_PAIRS_COUNT.get(mode, 50)
        return 50  # Default

    def _get_unified_pairs(self, n: int) -> tuple:
        """
        Get top N pairs from unified variant list, split into single-char and multi-char.
        Returns (single_char_pairs, multi_char_pairs) tuple.
        """
        if not UNIFIED_VARIANT_PAIRS:
            return [], []

        # Get top N pairs (without frequency)
        top_pairs = [(s, t) for s, t, _ in UNIFIED_VARIANT_PAIRS[:n]]

        single_pairs = []
        multi_pairs = []
        for a, b in top_pairs:
            if len(a) == 1 and len(b) == 1:
                single_pairs.append((a, b))
            else:
                multi_pairs.append((a, b))

        return single_pairs, multi_pairs

    def _get_multichar_pairs_for_mode(self, mode: str) -> list:
        """
        Get multi-character pairs based on search mode and settings.
        Uses unified frequency-sorted pairs list.
        """
        # Get custom pairs from settings
        _, custom_multi = self._get_custom_pairs()

        # Get count based on mode and settings
        n = self._get_pairs_count(mode)

        # Get multi-char pairs from unified list
        _, unified_multi = self._get_unified_pairs(n)

        return unified_multi + custom_multi

    def _generate_multichar_variants(self, term: str, mode: str = 'variants') -> set:
        """
        Generate variants using multi-character substitution pairs.
        Each pair is applied as simple string replacement (bidirectional).
        Returns set of variant terms (may have different lengths than original).

        Limited to MAX_MULTICHAR_VARIANTS to prevent explosion.
        """
        multi_pairs = self._get_multichar_pairs_for_mode(mode)
        if not multi_pairs:
            return set()

        variants = set()
        for a, b in multi_pairs:
            # a -> b substitution
            if a in term:
                variants.add(term.replace(a, b))
                if len(variants) >= self.MAX_MULTICHAR_VARIANTS:
                    break
            # b -> a substitution
            if b in term:
                variants.add(term.replace(b, a))
                if len(variants) >= self.MAX_MULTICHAR_VARIANTS:
                    break

        # Remove original term if present
        variants.discard(term)
        return variants

    def _rebuild_maps(self):
        """Build variant maps from unified frequency-sorted pairs list."""
        custom_single, _ = self._get_custom_pairs()

        # Get pairs count from settings (or use default for maximum coverage)
        n = self._get_pairs_count()

        # Build maps for each mode using unified pairs
        # Basic: top 30 pairs
        basic_single, _ = self._get_unified_pairs(self._MODE_PAIRS_COUNT['variants'])
        self.basic_map = self.make_multimap(basic_single + custom_single)

        # Extended: top 100 pairs
        extended_single, _ = self._get_unified_pairs(self._MODE_PAIRS_COUNT['variants_extended'])
        self.extended_map = self.make_multimap(extended_single + custom_single)

        # Maximum: uses settings slider value (default 500)
        max_single, _ = self._get_unified_pairs(max(n, self._MODE_PAIRS_COUNT['variants_maximum']))
        self.maximum_map = self.make_multimap(max_single + custom_single)

        # Also store a dynamic map based on current slider value
        slider_single, _ = self._get_unified_pairs(n)
        self.slider_map = self.make_multimap(slider_single + custom_single)

    def set_settings(self, settings):
        """Update settings reference, rebuild maps, and clear cache."""
        self._settings = settings
        self._rebuild_maps()
        self._cache.clear()

    def set_variant_level(self, n: int):
        """
        Update variant pairs count (slider value) and rebuild slider map.
        Call this when user adjusts the slider to avoid full rebuild.
        """
        if self._settings:
            self._settings.variant_pairs_count = n

        # Rebuild only the slider map
        custom_single, _ = self._get_custom_pairs()
        slider_single, _ = self._get_unified_pairs(n)
        self.slider_map = self.make_multimap(slider_single + custom_single)

        # Clear cache since pairs changed
        self._cache.clear()

    def get_variant_level(self) -> int:
        """Get current variant pairs count."""
        return self._get_pairs_count()

    def get_max_variant_pairs(self) -> int:
        """Get total number of available variant pairs."""
        return len(UNIFIED_VARIANT_PAIRS)

    def _get_max_changes_for_length(self, term_len: int, base_max: int) -> int:
        """
        Dynamic max_changes based on term length to prevent combinatorial explosion.
        Respects settings if available (variant_min_word_len, variant_aggressive).
        """
        # Check for aggressive mode (old behavior - no limits based on length)
        if self._settings and getattr(self._settings, 'variant_aggressive', False):
            return min(base_max, getattr(self._settings, 'variant_max_changes', 2))

        # Get threshold from settings or use default
        min_len = 2
        if self._settings:
            min_len = getattr(self._settings, 'variant_min_word_len', 2)

        if term_len <= min_len:
            # Short words: only 1 change
            return 1
        else:
            # Longer words: allow full base_max (capped by settings or 2)
            max_cap = 2
            if self._settings:
                max_cap = getattr(self._settings, 'variant_max_changes', 2)
            return min(base_max, max_cap)

    def hamming_distance(self, term: str, variant: str) -> int:
        """Calculate character difference count between term and variant."""
        if len(term) != len(variant):
            return len(term) + len(variant)
        return sum(1 for a, b in zip(term, variant) if a != b)

    def generate_variants(self, term: str, mapping: Mapping[str, set[str]],
                          max_changes: int, limit: int) -> set[str]:
        """
        Generate variants with early termination and smart position filtering.
        Only considers positions that actually have replacements in the mapping.
        """
        term_len = len(term)
        limit = min(limit, Config.VARIANT_GEN_LIMIT)
        result = set()

        # Pre-filter: find positions that have possible replacements
        replaceable_positions = []
        for i, char in enumerate(term):
            if char in mapping and mapping[char] - {char}:
                replaceable_positions.append(i)

        if not replaceable_positions:
            return result

        # Generate variants by number of changes (1 change first, then 2, etc.)
        for num_changes in range(1, max_changes + 1):
            if num_changes > len(replaceable_positions):
                break

            for positions in itertools.combinations(replaceable_positions, num_changes):
                # Build character options for each position
                char_options = []
                valid = True

                for i in range(term_len):
                    if i in positions:
                        repls = mapping[term[i]] - {term[i]}
                        if not repls:
                            valid = False
                            break
                        char_options.append(repls)
                    else:
                        char_options.append((term[i],))

                if not valid:
                    continue

                # Generate all combinations for these positions
                for combo in itertools.product(*char_options):
                    result.add("".join(combo))
                    if len(result) >= limit:
                        return result

        return result

    def get_variants(self, term: str, mode: str, limit: int = None) -> list[str]:
        """
        Generate spelling variants for Hebrew search terms.

        Uses unified frequency-sorted pairs with slider-based selection.
        The number of pairs used is determined by settings.variant_pairs_count.

        Also applies multi-character substitutions for pairs where one side
        has more than one character (2<->1 substitutions).
        """
        if len(term) < 2:
            return [term]

        # Get tier configuration
        tier = self._TIER_CONFIG.get(mode)
        if not tier:
            return [term]

        # Apply limit from tier config if not specified
        if limit is None:
            limit = tier['per_term_limit']
        else:
            limit = min(limit, Config.VARIANT_GEN_LIMIT)

        # Get current pairs count for cache key
        pairs_count = self._get_pairs_count(mode)

        # Check cache (include pairs_count for proper invalidation)
        cache_key = (term, mode, limit, pairs_count)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Check if a larger-limit result exists that we can slice from
        for cached_key, cached_value in self._cache.items():
            if (cached_key[0] == term and cached_key[1] == mode
                    and cached_key[3] == pairs_count and cached_key[2] >= limit):
                # Larger result exists; slice to our limit
                sliced = cached_value[:limit]
                self._cache[cache_key] = sliced
                return sliced

        # Select the appropriate map based on mode
        # Use slider_map when settings has custom pairs count
        if self._settings and hasattr(self._settings, 'variant_pairs_count'):
            # Rebuild slider map with current value if needed
            mapping = self.slider_map
        elif mode == 'variants':
            mapping = self.basic_map
        elif mode == 'variants_extended':
            mapping = self.extended_map
        elif mode == 'variants_maximum':
            mapping = self.maximum_map
        else:
            return [term]

        # Dynamic max_changes based on term length
        base_max = tier['max_changes']
        max_changes = self._get_max_changes_for_length(len(term), base_max)

        # Step 1: Generate multi-char substitution variants (e.g., kv=m)
        multichar_variants = self._generate_multichar_variants(term, mode)

        # Step 2: Generate single-char variants for original term
        variants = self.generate_variants(term, mapping, max_changes, limit)
        variants.add(term)  # Always include original

        # Step 3: Generate single-char variants for each multi-char variant
        for mc_variant in multichar_variants:
            variants.add(mc_variant)
            if len(variants) < limit and len(mc_variant) >= 2:
                mc_max_changes = self._get_max_changes_for_length(len(mc_variant), base_max)
                mc_single_variants = self.generate_variants(
                    mc_variant, mapping, mc_max_changes,
                    limit - len(variants)  # Remaining budget
                )
                variants.update(mc_single_variants)

        # Sort: original term first, then by similarity
        def sort_key(v):
            if v == term:
                return (0, 0, v)
            elif v in multichar_variants:
                return (1, 0, v)  # Multi-char variants second
            else:
                return (2, self.hamming_distance(term, v) if len(v) == len(term) else 100, v)

        sorted_variants = sorted(variants, key=sort_key)[:limit]

        # Cache result (with size limit)
        if len(self._cache) >= self._cache_max_size:
            # Simple eviction: clear half the cache
            keys_to_remove = list(self._cache.keys())[:self._cache_max_size // 2]
            for k in keys_to_remove:
                del self._cache[k]

        self._cache[cache_key] = sorted_variants
        return sorted_variants

    def clear_cache(self):
        """Clear the variant cache."""
        self._cache.clear()
