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
    from shared.unified_variants import UNIFIED_VARIANT_PAIRS
except ImportError:
    UNIFIED_VARIANT_PAIRS = []



# x1-x3 (single-letter changes per word) is kept per variant level. Defaults: x1 for
# Basic, x2 for Extended and Maximum (owner ruling 2026-09-28) -- what the website
# has always given; the desktop's Extended and Maximum gave x1 (their tier was Basic).
DEFAULT_MAX_CHANGES_BY_PRESET = {'basic': 1, 'extended': 2, 'maximum': 2}


def variant_preset_of(pairs_count) -> str:
    """The level a pair count belongs to: Basic (under 70), Extended (under 150),
    Maximum. The slider's values map onto the nearest level at or below them."""
    try:
        n = int(pairs_count)
    except (TypeError, ValueError):
        return 'basic'
    return 'basic' if n < 70 else ('extended' if n < 150 else 'maximum')


def max_changes_by_preset(stored=None, legacy=None) -> dict:
    """A checked per-level x1-x3 table. *stored*: a saved table (any missing or
    unreadable level gets its default). *legacy*: the single value saved before the
    table existed -- it seeds Extended and Maximum only; Basic starts at x1 (owner
    ruling: the old global x2 is not carried into Basic)."""
    table = dict(DEFAULT_MAX_CHANGES_BY_PRESET)
    if stored is None and legacy is not None:
        stored = {'extended': legacy, 'maximum': legacy}
    for level in table:
        try:
            value = int((stored or {}).get(level, table[level]))
        except (TypeError, ValueError, AttributeError):
            continue
        table[level] = max(1, min(3, value))
    return table


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

    # Tier configuration for balanced flexibility vs explosion prevention.
    # max_changes is the limit used only when there are no settings (tests,
    # callers without LabSettings); with settings, variant_max_changes (x1-x3,
    # kept per level: max_changes_for) is the per-word limit in every tier.
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
        # Per cache key: the list was cut at its limit (variants_overflowed)
        self._overflow = {}

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

    def _max_changes_setting(self, base_max: int) -> int:
        """The per-word limit of single-letter changes: the x1-x3 setting, in every
        tier (owner ruling 2026-09-28, option A). Without settings, the tier's own."""
        if not self._settings:
            return min(base_max, 2)
        try:
            value = int(getattr(self._settings, 'variant_max_changes', 2))
        except (TypeError, ValueError):
            value = 2
        return max(1, min(3, value))

    def _get_max_changes_for_length(self, term_len: int, base_max: int) -> int:
        """
        Dynamic max_changes based on term length to prevent combinatorial explosion.
        Respects settings if available (variant_min_word_len, variant_aggressive).
        """
        cap = self._max_changes_setting(base_max)
        # Check for aggressive mode (old behavior - no limits based on length)
        if self._settings and getattr(self._settings, 'variant_aggressive', False):
            return cap

        # Get threshold from settings or use default
        min_len = 2
        if self._settings:
            min_len = getattr(self._settings, 'variant_min_word_len', 2)

        if term_len <= min_len:
            # Short words: only 1 change
            return 1
        # Longer words: the per-word limit
        return cap

    def _change_settings(self):
        """The settings that decide which variants a word gets besides the pairs:
        part of the cache key, because they change between calls without a reset."""
        if not self._settings:
            return None
        return (getattr(self._settings, 'variant_max_changes', 2),
                getattr(self._settings, 'variant_min_word_len', 2),
                bool(getattr(self._settings, 'variant_aggressive', False)))

    def variants_overflowed(self, term: str, mode: str, limit: int = None) -> bool:
        """Whether *term*'s variants at *limit* (default: the regex budget) were cut:
        more spellings within the settings exist than were returned, so a search
        over them can miss pages."""
        limit = Config.REGEX_VARIANTS_LIMIT if limit is None else limit
        self.get_variants(term, mode, limit=limit)
        key = (term, mode, min(limit, Config.VARIANT_GEN_LIMIT), self._get_pairs_count(mode),
               self._change_settings())
        return bool(self._overflow.get(key, False))

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
        change_settings = self._change_settings()

        # Check cache (pairs count and change settings for proper invalidation)
        cache_key = (term, mode, limit, pairs_count, change_settings)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Check if a larger-limit result exists that we can slice from
        for cached_key, cached_value in list(self._cache.items()):
            if (cached_key[0] == term and cached_key[1] == mode
                    and cached_key[3] == pairs_count and cached_key[4] == change_settings
                    and cached_key[2] >= limit):
                # Larger result exists; slice to our limit
                sliced = cached_value[:limit]
                self._cache[cache_key] = sliced
                self._overflow[cache_key] = (len(cached_value) > limit
                                             or self._overflow.get(cached_key, False))
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
        # Up to two changes first, for the word and its multi-letter spellings; a
        # third change only fills what budget is left, so raising the limit to x3
        # never pushes out a spelling x2 finds -- the two-letters-for-one ones
        # above all (measured: ישראל at 150 pairs kept 1 of 2,717 when x3 ran first).
        near = min(max_changes, 2)

        # Step 1: Generate multi-char substitution variants (e.g., kv=m)
        multichar_variants = self._generate_multichar_variants(term, mode)

        # Step 2: Generate single-char variants for original term
        own = self.generate_variants(term, mapping, near, limit)
        variants = set(own)
        variants.add(term)  # Always include original

        # Step 3: Generate single-char variants for each multi-char variant
        mc_derived = set()
        for mc_variant in multichar_variants:
            variants.add(mc_variant)
            if len(variants) < limit and len(mc_variant) >= 2:
                mc_max_changes = min(self._get_max_changes_for_length(len(mc_variant), base_max), 2)
                mc_single_variants = self.generate_variants(
                    mc_variant, mapping, mc_max_changes,
                    limit - len(variants)  # Remaining budget
                )
                mc_derived.update(mc_single_variants - variants)
                variants.update(mc_single_variants)

        # Step 4 (x3): the third change, into what budget is left.
        far = set()
        if max_changes > 2:
            for base in (term, *sorted(multichar_variants)):
                if len(variants) + len(far) >= limit:
                    break
                if len(base) < 2 or self._get_max_changes_for_length(len(base), base_max) <= 2:
                    continue
                for v in self.generate_variants(base, mapping, 3, limit):
                    if v not in variants and v not in far:
                        far.add(v)
                        if len(variants) + len(far) >= limit:
                            break
        overflowed = len(variants) + len(far) >= limit
        variants |= far

        # Sort: original term first, then the multi-letter spellings, then by
        # similarity; spellings two letters for one before any third change.
        def sort_key(v):
            if v == term:
                return (0, 0, v)
            elif v in multichar_variants:
                return (1, 0, v)  # Multi-char variants second
            elif v in far:
                return (4, self.hamming_distance(term, v), v)
            elif v in mc_derived and v not in own:
                return (3, 0, v)
            else:
                return (2, self.hamming_distance(term, v) if len(v) == len(term) else 100, v)

        ordered = sorted(variants, key=sort_key)
        overflowed = overflowed or len(ordered) > limit
        sorted_variants = ordered[:limit]

        # Cache result (with size limit)
        if len(self._cache) >= self._cache_max_size:
            # Simple eviction: clear half the cache
            keys_to_remove = list(self._cache.keys())[:self._cache_max_size // 2]
            for k in keys_to_remove:
                del self._cache[k]
                self._overflow.pop(k, None)

        self._cache[cache_key] = sorted_variants
        self._overflow[cache_key] = overflowed
        return sorted_variants

    def clear_cache(self):
        """Clear the variant cache."""
        self._cache.clear()
        self._overflow.clear()
