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

        # Per (term, mode, pairs, change settings): the word's spellings in their one
        # canonical order, up to Config.VARIANT_GEN_LIMIT; every limit is a cut of it.
        self._cache = {}
        self._cache_max_size = 5000
        # ...and at most this many spellings in all (about 150 MB of strings): a
        # Responsa query asks for many prefixed forms, each list up to 8,000 long.
        self._cache_max_spellings = 2_000_000
        self._cached_spellings = 0
        # Per cache key: more spellings exist within the settings than the list holds
        # (it stopped at VARIANT_GEN_LIMIT, or the multi-letter cap left some out).
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

    def _multichar_spellings(self, term: str, mode: str = 'variants') -> tuple[list[str], bool]:
        """The word's multi-letter ("two letters for one") spellings: each multi-
        character pair applied as a whole-string replacement, both ways, in the order
        of the pairs (most frequent first). At most MAX_MULTICHAR_VARIANTS of them, to
        prevent explosion; the second value says whether that cap left any out."""
        found = []
        seen = {term}
        for a, b in self._get_multichar_pairs_for_mode(mode):
            for old, new in ((a, b), (b, a)):
                if old in term:
                    spelling = term.replace(old, new)
                    if spelling not in seen:
                        seen.add(spelling)
                        found.append(spelling)
        return found[:self.MAX_MULTICHAR_VARIANTS], len(found) > self.MAX_MULTICHAR_VARIANTS

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
        self.clear_cache()

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
        self.clear_cache()

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
        """Whether get_variants(term, mode, limit) (default limit: the regex budget)
        leaves spellings out: more exist within the settings than it returns, so a
        search over them can miss pages. A list that holds them all exactly is not cut."""
        limit = Config.REGEX_VARIANTS_LIMIT if limit is None else limit
        spellings = self._spellings(term, mode)
        if spellings is None:
            return False
        sequence, more = spellings
        return more or len(sequence) > min(limit, Config.VARIANT_GEN_LIMIT)

    def hamming_distance(self, term: str, variant: str) -> int:
        """Calculate character difference count between term and variant."""
        if len(term) != len(variant):
            return len(term) + len(variant)
        return sum(1 for a, b in zip(term, variant) if a != b)

    @staticmethod
    def _replaceable(base: str, mapping: Mapping[str, set[str]]) -> list:
        """(position, its replacement letters in sorted order) for each letter of
        *base* the pairs can change. Sorted, so the order never depends on
        PYTHONHASHSEED (the mapping holds sets)."""
        out = []
        for i, char in enumerate(base):
            repls = mapping.get(char)       # .get: the defaultdict gains no keys
            others = sorted(repls - {char}) if repls else ()
            if others:
                out.append((i, tuple(others)))
        return out

    @staticmethod
    def _changes(base: str, replaceable: list, k: int):
        """*base*'s spellings with exactly *k* letters changed, in a fixed order: the
        changed positions left to right (itertools.combinations), then the letters in
        sorted order. Lazy, so a caller can stop at its budget."""
        chars = list(base)
        for combo in itertools.combinations(replaceable, k):
            positions = [p for p, _ in combo]
            for letters in itertools.product(*(r for _, r in combo)):
                for p, c in zip(positions, letters):
                    chars[p] = c
                yield ''.join(chars)
            for p in positions:
                chars[p] = base[p]

    def generate_variants(self, term: str, mapping: Mapping[str, set[str]],
                          max_changes: int, limit: int) -> set[str]:
        """Up to *limit* of *term*'s spellings with 1..max_changes single-letter
        changes, fewest changes first (only positions the mapping can change)."""
        limit = min(limit, Config.VARIANT_GEN_LIMIT)
        result = set()
        replaceable = self._replaceable(term, mapping)
        for k in range(1, max_changes + 1):
            for spelling in self._changes(term, replaceable, k):
                if len(result) >= limit:
                    return result
                result.add(spelling)
        return result

    def _spelling_sequence(self, term: str, mode: str, mapping: Mapping[str, set[str]],
                           base_max: int) -> tuple[list[str], bool]:
        """The word's spellings in their one canonical order, up to
        Config.VARIANT_GEN_LIMIT, and whether more exist within the settings.

        The order: the word; its multi-letter spellings; then by the number of
        single-letter changes k = 1, 2, 3 -- at each k the word's own k-change
        spellings, then each multi-letter spelling's (each base within its own
        length rule, _get_max_changes_for_length). So the list is the same whatever
        limit a caller asks for and whatever was asked before (every limit is a cut
        of it), and x1's list is the start of x2's and x2's of x3's: raising Num
        Changes never drops a spelling a lower one finds. Before, a word's own
        2-change spellings filled the budget ahead of its multi-letter spellings'
        1-change ones (והמשפטים at 150 pairs: 104 of x1's 316 were missing at x2).
        """
        cap = Config.VARIANT_GEN_LIMIT
        multi, more = self._multichar_spellings(term, mode)
        out = [term, *multi]
        if len(out) > cap:
            return out[:cap], True
        seen = set(out)
        bases = [(base, self._get_max_changes_for_length(len(base), base_max),
                  self._replaceable(base, mapping))
                 for base in out if len(base) >= 2]
        top = max((most for _, most, _ in bases), default=0)
        for k in range(1, top + 1):
            for base, most, replaceable in bases:
                if k > most or k > len(replaceable):
                    continue
                for spelling in self._changes(base, replaceable, k):
                    if spelling in seen:
                        continue
                    if len(out) >= cap:
                        return out, True        # one more exists than the budget holds
                    seen.add(spelling)
                    out.append(spelling)
        return out, more

    def _spellings(self, term: str, mode: str):
        """(the canonical sequence, more exist) for *term* in *mode*, cached; None
        when the word gets no variants (one letter, or a mode with no tier)."""
        if len(term) < 2:
            return None
        tier = self._TIER_CONFIG.get(mode)
        if not tier:
            return None

        # Select the appropriate map based on mode
        # Use slider_map when settings has custom pairs count
        if self._settings and hasattr(self._settings, 'variant_pairs_count'):
            mapping = self.slider_map
        elif mode == 'variants':
            mapping = self.basic_map
        elif mode == 'variants_extended':
            mapping = self.extended_map
        else:
            mapping = self.maximum_map

        # Pairs count and change settings: they change between calls without a reset.
        key = (term, mode, self._get_pairs_count(mode), self._change_settings(),
               Config.VARIANT_GEN_LIMIT)
        sequence = self._cache.get(key)
        more = self._overflow.get(key)
        if sequence is not None and more is not None:
            return sequence, more

        sequence, more = self._spelling_sequence(term, mode, mapping, tier['max_changes'])

        if (len(self._cache) >= self._cache_max_size
                or self._cached_spellings + len(sequence) > self._cache_max_spellings):
            # Simple eviction: drop the older half
            for k in list(self._cache)[:max(1, len(self._cache) // 2)]:
                dropped = self._cache.pop(k, None)
                self._overflow.pop(k, None)
                if dropped is not None:
                    self._cached_spellings -= len(dropped)
        self._cache[key] = sequence
        self._overflow[key] = more
        self._cached_spellings += len(sequence)
        return sequence, more

    def get_variants(self, term: str, mode: str, limit: int = None) -> list[str]:
        """
        Spelling variants of a Hebrew search term, closest first: the word, its
        multi-letter (2<->1) spellings, then by the number of single-letter changes.

        Uses unified frequency-sorted pairs with slider-based selection; the number
        of pairs is settings.variant_pairs_count, the changes per word x1-x3 its
        variant_max_changes. *limit* (default: the tier's per-term limit) cuts the
        word's one canonical list (_spelling_sequence), so the answer never depends
        on the calls made before. variants_overflowed says whether it left any out.
        """
        spellings = self._spellings(term, mode)
        if spellings is None:
            return [term]
        if limit is None:
            limit = self._TIER_CONFIG[mode]['per_term_limit']
        return spellings[0][:min(limit, Config.VARIANT_GEN_LIMIT)]

    def clear_cache(self):
        """Clear the variant cache (and the overflow flags kept beside it)."""
        self._cache.clear()
        self._overflow.clear()
        self._cached_spellings = 0
