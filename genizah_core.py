"""Core search, indexing, metadata, and AI helpers for the Genizah project."""

# -*- coding: utf-8 -*-
# genizah_core.py
import logging
import os
import re
import shutil
import pickle
import threading
import time
import csv
from collections import defaultdict
from logging.handlers import RotatingFileHandler
import platform


class SafeRotatingFileHandler(RotatingFileHandler):
    """
    A RotatingFileHandler that handles Windows file locking gracefully.
    On Windows, if the log file can't be rotated (due to being in use),
    it continues logging to the current file without raising an error.
    """
    def doRollover(self):
        try:
            super().doRollover()
        except (PermissionError, OSError) as e:
            # On Windows, file might be locked by another process
            # Just continue logging to current file
            if platform.system() == 'Windows':
                pass  # Silently continue without rotation
            else:
                raise
from typing import Optional
from functools import lru_cache
import json
import html
import weakref  # Phase 97 R-01: weakref for MyLibraryTab gate

# Phase 123: LIBRARY_CODES_HE retained on the compat facade (GUARD-04). It is now
# consumed only inside shared/ (browse_map_utils), but was importable from
# genizah_core at v8.2.x, so the re-export is preserved for zero behavior change.
from genizah_translations import TRANSLATIONS, LIBRARY_CODES_HE  # noqa: F401

# Phase 98 D-03 + D-22 + D-23: shared NLI circuit breaker (replaces the
# class-attribute breaker that used to live on MetadataManager). Module-level
# import (parallel to the stdlib/requests block above) so the migration is
# grep-visible and so the aliased names match the original class-method
# call sites verbatim.
from shared.nli_circuit_breaker import (  # noqa: F401
    is_open as _nli_circuit_is_open,
    record_failure as _nli_record_failure,
    record_success as _nli_record_success,
    NLI_CONNECT_TIMEOUT,
    NLI_IIIF_READ_TIMEOUT,
    NLI_MARC_READ_TIMEOUT,
)
# SEED-006: dependency-light hebword tokenizer registration (only imports
# tantivy — safe at module top, no PyMuPDF/circular-import concern).
from shared.search_tokenizer import register_search_tokenizers
# Phase 122: Config extracted to shared/config.py — permanent compat facade (v8.3.0)
from shared.config import Config  # noqa: F401
# Phase 123: browse_map_utils extracted — permanent compat facade (v8.3.0)
from shared.browse_map_utils import (  # noqa: F401
    LIBRARY_CODES, normalize_shelfmark, natural_sort_key,
    get_library_display, dedupe_browse_map,
    _get_library_prefix_aliases, _strip_library_prefix,
    _load_ie_volume_map, _extract_ie_from_header, _repair_missing_ie_pages,
)
# Phase 123: text_normalize extracted — permanent compat facade (v8.3.0)
from shared.text_normalize import NIKUD_PATTERN, strip_nikud  # noqa: F401
from shared.text_normalize import COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics  # noqa: F401
# Phase 123: variants extracted — permanent compat facade (v8.3.0)
from shared.variants import VariantManager  # noqa: F401
# Phase 123: unified_variants pairs retained on the compat facade (GUARD-04).
# VariantManager moved to shared/variants.py (which imports UNIFIED_VARIANT_PAIRS
# itself); these names were importable from genizah_core at v8.2.x, so the
# try/except re-export is preserved verbatim for zero behavior change.
try:
    from unified_variants import UNIFIED_VARIANT_PAIRS, get_top_pairs  # noqa: F401
except ImportError:
    UNIFIED_VARIANT_PAIRS = []
    def get_top_pairs(n): return []
# Phase 123: responsa extracted — permanent compat facade (v8.3.0)
from shared.responsa import (  # noqa: F401
    GRAMMATICAL_PREFIXES, GRAMMATICAL_SUFFIXES,
    _SOFIT_TO_NORMAL, _GAP_TOKEN_RE, _LINE_GAP_TOKEN_RE,
    ResponsaComponent, parse_responsa_query, _tokenize_responsa_query,
    _parse_single_token, extract_per_pair_gaps, generate_tabular_syntax,
    expand_grammatical_prefixes, expand_grammatical_suffixes,
    expand_judeo_arabic, expand_plene_defective,
    _expand_inline_alternation, _count_expanded_terms, _apply_explosion_guard,
    _has_line_break_syntax, LineGroup, _parse_line_break_query,
)
# Phase 123: codicological extracted — permanent compat facade (v8.3.0)
from shared.codicological import CodicologicalManager  # noqa: F401
# Phase 123: joins_manager extracted — permanent compat facade (v8.3.0)
from shared.joins_manager import JoinsManager  # noqa: F401
# Phase 123: lists_manager extracted — permanent compat facade (v8.3.0)
from shared.lists_manager import ListsManager  # noqa: F401
# Phase 124: metadata_manager extracted — permanent compat facade (v8.3.0)
from shared.metadata_manager import (  # noqa: F401
    _NLI_CACHE_MAX_ENTRIES,
    _BoundedLRUCache,
    MARC_FUTURE_TIMEOUT,
    NLI_IIIF_FUTURE_TIMEOUT,
    EXTERNAL_IIIF_HTTP_TIMEOUT,
    MetadataManager,
    _get_crossref_service,
    _get_fjms_service,
    _parse_cudl_label,
)
# Phase 124: indexer extracted — permanent compat facade (v8.3.0)
from shared.indexer import Indexer  # noqa: F401


# --- Phase 78 (Concern #6): thread-local cascade-downgrade signal ---
# When a Responsa query triggers MAX_EXPANDED_TERMS downgrade, the cascade
# code sets this AND ALSO attaches responsa_warning to deduped[0] (legacy
# path, preserved for callers that read result rows). The /api/search handler
# reads this thread-local instead so warnings survive empty result sets.
#
# Naming: prefixed with _LAST_ to make obvious it's a one-shot signal that the
# next consumer should consume (read-and-clear). Per-thread because the search
# engine may be invoked concurrently from FastAPI threads.
_LAST_RESPONSA_DOWNGRADE = threading.local()

# Phase 81A — structured per-flag cascade outcome carried alongside the
# legacy string message. Skill consumer (81B) reads this to populate
# responsa_options_effective in the /api/search envelope echo.
_LAST_RESPONSA_DOWNGRADE_META = threading.local()


def _set_last_responsa_downgrade(message: str) -> None:
    """Record a Responsa downgrade signal on the current thread.

    Phase 78 Concern #6: called from the cascade decision site so the
    /api/search handler can surface the warning even when results == [].
    """
    _LAST_RESPONSA_DOWNGRADE.value = message


def _consume_last_responsa_downgrade() -> Optional[str]:
    """Read-and-clear the per-thread downgrade signal. Returns None if unset.

    Phase 78 Concern #6: called once per /api/search invocation in the finally
    branch of the response builder. Read-and-clear semantics ensure the signal
    is attributed to exactly one request.
    """
    msg = getattr(_LAST_RESPONSA_DOWNGRADE, 'value', None)
    if msg is not None:
        # Clear by deleting the attribute so subsequent calls return None.
        try:
            del _LAST_RESPONSA_DOWNGRADE.value
        except AttributeError:
            pass
    return msg


def _set_last_responsa_downgrade_meta(meta: dict) -> None:
    """Phase 81A — record a structured per-flag cascade outcome.

    `meta` is a dict with the four ResponsaOptions field names as keys and
    booleans indicating whether each was applied (True) or cascade-disabled
    (False). The skill consumer compares this to the request's
    responsa_options to detect server-side downgrades.
    """
    _LAST_RESPONSA_DOWNGRADE_META.value = meta


def _consume_last_responsa_downgrade_meta() -> Optional[dict]:
    """Phase 81A — read-and-clear the structured cascade outcome.

    Returns None when no downgrade occurred OR when already consumed
    on the current thread.
    """
    meta = getattr(_LAST_RESPONSA_DOWNGRADE_META, 'value', None)
    if meta is not None:
        try:
            del _LAST_RESPONSA_DOWNGRADE_META.value
        except AttributeError:
            pass
    return meta


# --- Shmidman Rare-Letter Helpers ---
HEBREW_FREQ = {
    'י': 1, 'ו': 2, 'ה': 3, 'ל': 4, 'א': 5, 'ר': 6, 'מ': 7, 'ת': 8, 
    'ב': 9, 'ש': 10, 'נ': 11, 'ד': 12, 'כ': 13, 'ע': 14, 'ח': 15, 
    'ק': 16, 'פ': 17, 'ס': 18, 'ג': 19, 'ט': 20, 'ז': 21, 'צ': 22,
    # Final letters
    'ך': 13, 'ם': 7, 'ן': 11, 'ף': 17, 'ץ': 22
}

STANDARD_HEBREW_DIST = {
    'י': 11.5, 'ו': 10.2, 'ה': 8.5, 'א': 8.2, 'ל': 7.2, 'מ': 6.5, 'ת': 5.5,
    'ב': 5.2, 'ר': 5.1, 'ש': 4.3, 'נ': 4.0, 'ד': 2.8, 'כ': 2.5, 'ע': 2.4,
    'ח': 2.3, 'ק': 2.0, 'פ': 1.8, 'ס': 1.5, 'ט': 1.1, 'ז': 0.9, 'ג': 0.8,
    'צ': 0.8, 'ץ': 0.4, 'ף': 0.3, 'ך': 0.3, 'ם': 2.5, 'ן': 1.0
}

# NIKUD_PATTERN, strip_nikud moved to shared/text_normalize.py (Phase 123). Re-exported above.

# --- Responsa Search Constants ---
# Hebrew grammatical prefixes for # expansion (~25 entries)

# ── Mosseri CUDL label construction ─────────────────────────────

# Valid Mosseri CUDL series (Roman numeral collections digitized at Cambridge)
_MOSSERI_CUDL_SERIES = {"I", "IA", "II", "III", "IIIA", "IV", "V", "VI", "VII", "VIII", "IX", "X"}

# Regex to extract series, number, optional sub-fragment, and optional letter suffix
# from Mosseri shelfmark variants like "Ms. VI 108", "Moss. III 145.3C",
# or "Mosseri, Jacques Ms. VII 173.3"
_MOSSERI_CUDL_RE = re.compile(
    r'(?:Mosseri.*?)?(?:Ms\.?|Moss\.?)\s*([IVXL]+[a-z]?)\s*,?\s*(\d+)(?:\.(\d+))?([A-Z])?$',
    re.IGNORECASE,
)


def construct_mosseri_cudl_label(shelfmark: str) -> str | None:
    """
    Convert a Mosseri shelfmark variant to a CUDL manifest label.

    Returns the CUDL label (e.g., 'MS-MOSSERI-VI-00108') or None if the
    shelfmark is not a recognized Mosseri Roman-numeral-series pattern.

    Examples:
        'Ms. VI 108'            -> 'MS-MOSSERI-VI-00108'
        'Moss. VI,129.3'        -> 'MS-MOSSERI-VI-00129-00003'
        'Ms. III 27O'           -> 'MS-MOSSERI-III-00027-O'
        'Ms. III 145.3C'        -> 'MS-MOSSERI-III-00145-00003-C'
        'Ms. IIIa 15'           -> 'MS-MOSSERI-IIIA-00015'
        'T-S 12.123'            -> None  (not Mosseri)
        'Ms. L 241'             -> None  (2nd-series, not Roman numeral)
    """
    if not shelfmark:
        return None

    m = _MOSSERI_CUDL_RE.search(shelfmark)
    if not m:
        return None

    series = m.group(1).upper()
    if series not in _MOSSERI_CUDL_SERIES:
        return None

    number = m.group(2).zfill(5)

    parts = [f"MS-MOSSERI-{series}-{number}"]

    sub = m.group(3)
    if sub:
        parts.append(sub.zfill(5))

    letter = m.group(4)
    if letter:
        parts.append(letter.upper())

    return "-".join(parts)


def encode_word_shmidman(word: str, freq_map=None) -> str:
    """Encode a single word by selecting its two rarest Hebrew characters."""
    if freq_map is None:
        freq_map = HEBREW_FREQ
    # Strip nikud (vowel marks) before encoding
    word = strip_nikud(word)
    letters = []
    for idx, ch in enumerate(word):
        if ch in freq_map:
            letters.append((idx, ch, freq_map[ch]))

    if not letters:
        return ""

    rarest = sorted(letters, key=lambda item: (-item[2], item[0]))[:3]
    rarest_sorted = sorted(rarest, key=lambda item: item[0])
    return "".join(ch for _, ch, _ in rarest_sorted)


def text_to_fingerprint(text: str, freq_map=None) -> str:
    """Convert free text into a fingerprint representation."""
    tokens = re.findall(Config.WORD_TOKEN_PATTERN, text or "")
    encoded_tokens = []
    for tok in tokens:
        encoded = encode_word_shmidman(tok, freq_map=freq_map)
        if encoded:
            encoded_tokens.append(encoded)
    return " ".join(encoded_tokens)


# ==============================================================================
#  BOUNDARY SEARCH HELPERS
# ==============================================================================

def parse_boundaries(text: str, delimiter: str, min_distance: int = 3) -> list:
    """
    Find word indices where boundaries occur.

    Args:
        text: Source text
        delimiter: Boundary marker (e.g., '\n\n', '.', ':')
        min_distance: Minimum words between boundaries (ignore closer ones)

    Returns:
        List of word indices where boundaries occur (boundary is AFTER this index)
    """
    if not text or not delimiter:
        return []

    # Split by delimiter
    parts = text.split(delimiter)

    if len(parts) <= 1:
        return []

    boundaries = []
    word_count = 0
    last_boundary_pos = -min_distance  # Allow first boundary

    for i, part in enumerate(parts[:-1]):  # Skip last part (no boundary after it)
        words_in_part = len(re.findall(r"[\w\u0590-\u05FF\']+", part))
        word_count += words_in_part

        # Only add boundary if far enough from previous
        if word_count - last_boundary_pos >= min_distance and word_count > 0:
            boundaries.append(word_count - 1)  # Boundary after last word of this part
            last_boundary_pos = word_count

    return boundaries


def chunk_crosses_boundary(chunk_start: int, chunk_end: int, boundaries: list) -> bool:
    """
    Check if a chunk spans any boundary with words on BOTH sides.

    A chunk truly crosses a boundary if it includes at least one word
    before/at the boundary AND at least one word after the boundary.

    Args:
        chunk_start: Starting word index of chunk
        chunk_end: Ending word index of chunk (exclusive)
        boundaries: List of boundary word indices (last word before delimiter)

    Returns:
        True if chunk spans at least one boundary with words on both sides
    """
    for b in boundaries:
        # chunk_start <= b: chunk includes word at or before boundary
        # b < chunk_end - 1: chunk includes at least one word after boundary
        if chunk_start <= b < chunk_end - 1:
            return True
    return False


def get_crossed_boundaries(chunk_start: int, chunk_end: int, boundaries: list) -> set:
    """
    Get the set of boundary indices that a chunk crosses.

    Args:
        chunk_start: Starting word index of chunk
        chunk_end: Ending word index of chunk (exclusive)
        boundaries: List of boundary word indices

    Returns:
        Set of boundary indices that this chunk crosses
    """
    crossed = set()
    for b in boundaries:
        if chunk_start <= b < chunk_end - 1:
            crossed.add(b)
    return crossed


def calculate_boundary_quality(boundary_chunk_scores: list) -> float:
    """
    Calculate boundary match quality as average of match strengths.

    Args:
        boundary_chunk_scores: List of scores from chunks that crossed boundaries

    Returns:
        Average score (0 if no boundary matches)
    """
    if not boundary_chunk_scores:
        return 0.0
    return sum(boundary_chunk_scores) / len(boundary_chunk_scores)


def calculate_final_score_with_boost(base_score: float,
                                     boundary_quality: float,
                                     has_boundary_matches: bool,
                                     boundary_boost: float = 1.5) -> float:
    """
    Calculate final score with boundary boost.

    Formula: base_score * (1 + (boost - 1) * normalized_quality)

    Where normalized_quality = boundary_quality / base_score
    This ensures the boost is proportional to how good the boundary matches are
    relative to overall match quality.

    Args:
        base_score: Original score without boost
        boundary_quality: Average score of boundary-crossing chunks
        has_boundary_matches: Whether any boundary matches exist
        boundary_boost: Multiplier for boundary matches (default 1.5)

    Returns:
        Boosted score (or original score if no boundary matches)
    """
    if not has_boundary_matches or base_score == 0:
        return base_score

    # Normalize boundary quality relative to base score
    normalized_quality = min(boundary_quality / base_score, 1.0)

    multiplier = 1 + (boundary_boost - 1) * normalized_quality
    return base_score * multiplier


def get_boundary_stats(text: str, delimiter: str, chunk_size: int, min_distance: int = 3) -> dict:
    """
    Get pre-search statistics about boundaries.

    Args:
        text: Source text
        delimiter: Boundary marker
        chunk_size: Size of search chunks
        min_distance: Minimum words between boundaries

    Returns:
        Dictionary with boundary_count, crossing_chunk_count, total_chunks, and boundaries list
    """
    boundaries = parse_boundaries(text, delimiter, min_distance)
    tokens = re.findall(r"[\w\u0590-\u05FF\']+", text or "")
    total_words = len(tokens)

    if total_words < chunk_size:
        return {
            'boundary_count': len(boundaries),
            'crossing_chunk_count': 1 if boundaries else 0,
            'total_chunks': 1,
            'boundaries': boundaries  # Include parsed boundaries for reuse
        }

    # Count chunks that cross boundaries
    crossing_chunks = 0
    step = max(1, chunk_size // 2)
    total_chunks = 0

    for i in range(0, max(1, total_words - chunk_size + 1), step):
        total_chunks += 1
        if chunk_crosses_boundary(i, i + chunk_size, boundaries):
            crossing_chunks += 1

    return {
        'boundary_count': len(boundaries),
        'crossing_chunk_count': crossing_chunks,
        'total_chunks': total_chunks,
        'boundaries': boundaries  # Include parsed boundaries for reuse
    }


def _count_unique_chunks(chunk_hits):
    """Count distinct source-chunk contents from a chunk_hits list.

    Used by both search_composition_logic and lab_composition_search to derive
    the user-facing `chunk_count` (the basis for the `min chunks` filter).
    Counting by chunk_text dedupes (a) sliding-window indices with identical
    content from repeated source phrases and (b) cross-Tantivy-segment hits
    that already share (i, ms_snip) after the per-rec dedup step in each
    caller. Defensive against malformed inputs.
    """
    return len({
        hit[1]
        for hit in (chunk_hits or ())
        if isinstance(hit, (tuple, list)) and len(hit) > 1 and hit[1]
    })


try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

# ==============================================================================
#  LAB SETTINGS
# ==============================================================================
class LabSettings:
    """Manages configuration for the Lab Mode, including scoring weights."""
    def __init__(self):
        self.custom_variants = {} 
        self.candidate_limit = 5000
        self.min_should_match = 75
        self.gap_penalty = 2
        
        # Scoring Weights
        self.length_bonus_factor = 1.5
        self.common_penalty_factor = 0.1
        self.unique_bonus_base = 100
        self.density_penalty = 0.2
        self.coverage_power = 2.0
        self.order_bonus = 10.0
        
        # --- New Settings: Noise Suppression (Stop Words) ---
        self.stop_word_score = 1.0       # Score for short words (<3 chars)
        self.common_3char_score = 2.0    # Score for common 3-letter words
        
        # Composition Settings
        self.comp_chunk_limit = 500
        self.comp_min_score = 70
        self.comp_max_final_results = 200
        
        # Deep Scan Settings
        self.lab_scan_limit = 50000

        # Display Limit
        self.lab_display_limit = 500

        self.use_dynamic_weights = False

        # Variant Search Settings (affects standard search when using variants mode)
        self.variant_min_word_len = 2      # Words <= this length get only 1 change
        self.variant_max_changes = 2       # Max character changes per word
        self.variant_aggressive = False    # If True, ignore length limits (like old behavior)
        self.variant_pairs_count = 50      # Number of top variant pairs to use (slider value)
        self.variant_use_slider = False    # If True, show slider instead of preset buttons

        # Boundary Search Settings
        self.boundary_mode = 'full'           # 'full', 'boundary', 'combined'
        self.boundary_delimiter = '\n'        # What marks a paragraph boundary
        self.boundary_boost = 1.5             # Score multiplier for boundary matches (1.0-3.0)
        self.min_boundary_matches = 0         # Filter results with fewer matches (0-10)
        self.min_delimiter_distance = 3       # Min words between delimiters

        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.use_dynamic_weights = data.get('use_dynamic_weights', False)
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.min_should_match = data.get('min_should_match', 60)
                    self.gap_penalty = data.get('gap_penalty', 2)
                    
                    self.length_bonus_factor = data.get('length_bonus_factor', 1.5)
                    self.common_penalty_factor = data.get('common_penalty_factor', 0.1)
                    self.unique_bonus_base = data.get('unique_bonus_base', 100)
                    self.density_penalty = data.get('density_penalty', 0.2)
                    self.coverage_power = data.get('coverage_power', 2.0)
                    self.order_bonus = data.get('order_bonus', 10.0)

                    # Load noise settings
                    self.stop_word_score = data.get('stop_word_score', 1.0)
                    self.common_3char_score = data.get('common_3char_score', 2.0)

                    self.comp_chunk_limit = data.get('comp_chunk_limit', 200)
                    self.comp_min_score = data.get('comp_min_score', 70)
                    self.comp_max_final_results = data.get('comp_max_final_results', 100)

                    self.lab_scan_limit = data.get('lab_scan_limit', 50000)
                    self.lab_display_limit = data.get('lab_display_limit', 500)

                    # Load variant settings
                    self.variant_min_word_len = data.get('variant_min_word_len', 2)
                    self.variant_max_changes = data.get('variant_max_changes', 2)
                    self.variant_aggressive = data.get('variant_aggressive', False)
                    self.variant_pairs_count = data.get('variant_pairs_count', 50)
                    self.variant_use_slider = data.get('variant_use_slider', False)

                    # Load boundary search settings
                    self.boundary_mode = data.get('boundary_mode', 'full')
                    self.boundary_delimiter = data.get('boundary_delimiter', '\n')
                    self.boundary_boost = data.get('boundary_boost', 1.5)
                    self.min_boundary_matches = data.get('min_boundary_matches', 0)
                    self.min_delimiter_distance = data.get('min_delimiter_distance', 3)
            except Exception as e:
                logging.getLogger(__name__).warning('Failed to load lab config from %s: %s', Config.LAB_CONFIG_FILE, e)

    def save(self):
        try:
            os.makedirs(Config.LAB_DIR, exist_ok=True)
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'use_dynamic_weights': self.use_dynamic_weights,
                    'candidate_limit': self.candidate_limit,
                    'min_should_match': self.min_should_match,
                    'gap_penalty': self.gap_penalty,
                    
                    'length_bonus_factor': self.length_bonus_factor,
                    'common_penalty_factor': self.common_penalty_factor,
                    'unique_bonus_base': self.unique_bonus_base,
                    'density_penalty': self.density_penalty,
                    'coverage_power': self.coverage_power,
                    'order_bonus': self.order_bonus,
                    
                    # Save noise settings
                    'stop_word_score': self.stop_word_score,
                    'common_3char_score': self.common_3char_score,

                    'comp_chunk_limit': self.comp_chunk_limit,
                    'comp_min_score': self.comp_min_score,
                    'comp_max_final_results': self.comp_max_final_results,

                    'lab_scan_limit': self.lab_scan_limit,
                    'lab_display_limit': self.lab_display_limit,

                    # Variant settings
                    'variant_min_word_len': self.variant_min_word_len,
                    'variant_max_changes': self.variant_max_changes,
                    'variant_aggressive': self.variant_aggressive,
                    'variant_pairs_count': self.variant_pairs_count,
                    'variant_use_slider': self.variant_use_slider,

                    # Boundary search settings
                    'boundary_mode': self.boundary_mode,
                    'boundary_delimiter': self.boundary_delimiter,
                    'boundary_boost': self.boundary_boost,
                    'min_boundary_matches': self.min_boundary_matches,
                    'min_delimiter_distance': self.min_delimiter_distance
                }, f, indent=4)
        except Exception as e:
            logging.getLogger(__name__).warning('Failed to save lab config to %s: %s', Config.LAB_CONFIG_FILE, e)

# ==============================================================================
#  LAB ENGINE 
# ==============================================================================
class LabEngine:
    LAB_FINGERPRINT_FIELD = "fingerprint"
    # NGRAM_SIZE kept for compatibility if other parts of code ref it
    NGRAM_SIZE = 3 

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.settings = LabSettings()
        self.lab_index = None
        self.lab_searcher = None
        self.lab_index_needs_rebuild = False
        self.dynamic_rank_map = None

        # CR-02 FIX: LOCAL LAB side-index attributes — mirror SearchEngine so
        # LabEngine.lab_composition_search can query LOCAL LAB hits in LAB mode.
        # Previously these attributes only existed on SearchEngine, so the
        # `getattr(self, "_check_local_lab_freshness", None)` guard in
        # lab_composition_search returned None and the entire LOCAL LAB hook
        # was silently skipped — REQ-6 (three-surface coverage) was broken.
        self.local_lab_searcher = None
        self._local_lab_index = None
        self.local_lab_searcher_stale = False
        self._lab_local_meta = None

        # Try load dynamic weights
        if os.path.exists(Config.LAB_WEIGHTS_FILE):
            try:
                with open(Config.LAB_WEIGHTS_FILE, 'r', encoding='utf-8') as f:
                    self.dynamic_rank_map = json.load(f)
            except Exception:
                # Dynamic weights file corrupt or unreadable; keep defaults.
                logging.getLogger(__name__).warning(
                    'Failed to load dynamic weights from %s; using defaults',
                    Config.LAB_WEIGHTS_FILE, exc_info=True,
                )

        self._reload_lab_index()
        # CR-02 FIX: open LOCAL LAB side-index at startup so LAB-mode
        # Composition Search sees LOCAL hits without waiting for a refresh.
        self.reload_local_lab_index()

    def _close_index(self):
        self.lab_searcher = None
        self.lab_index = None
        import gc
        gc.collect() 

    def _ensure_lab_tokenizers(self, index):
        """Register analyzers safely."""
        try:
            index.register_tokenizer("whitespace", tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.whitespace()).build())
        except Exception:
            pass  # Tokenizer registration may fail on reopen; non-fatal, search still works
        try:
            index.register_tokenizer("simple", tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple()).build())
        except Exception:
            pass  # Tokenizer registration may fail on reopen; non-fatal, search still works

    def _reload_lab_index(self):
        """Loads index with heavy debug logging."""
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                LAB_LOGGER.info("Reloading Lab Index...")
                self.lab_index = tantivy.Index.open(Config.LAB_INDEX_DIR)
                self._ensure_lab_tokenizers(self.lab_index)
                self.lab_searcher = self.lab_index.searcher()

                # Simplified robust check
                self.lab_index_needs_rebuild = False
                return True
            except Exception as e:
                LAB_LOGGER.error(f"Failed to load Lab Index: {e}")
                self._close_index()

        self.lab_index_needs_rebuild = True
        return False

    # ------------------------------------------------------------------
    # CR-02 FIX: LOCAL LAB side-index handling on LabEngine
    # ------------------------------------------------------------------
    # These mirror the SearchEngine.reload_local_lab_index /
    # _check_local_lab_freshness methods so LabEngine.lab_composition_search
    # actually surfaces LOCAL hits in LAB mode (REQ-6).  Wired by
    # MyLibraryTab on startup + after every Refresh / Add / Remove.
    def reload_local_lab_index(self) -> None:
        """Reopen the LOCAL LAB side-index against the current Config.LOCAL_LAB_INDEX_DIR.

        Idempotent + defensive: D-37 semantics — on any open failure the
        searcher falls back to None and the LAB-mode composition path
        cleanly skips LOCAL.
        """
        self.local_lab_searcher = None
        self._local_lab_index = None
        self._lab_local_meta = None
        try:
            if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
                from shared.local_indexer import build_local_lab_schema, LocalIndexer
                schema = build_local_lab_schema()
                local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
                # Phase 110 UAT BLOCKER: the LOCAL LAB schema declares the
                # fingerprint / fingerprint_dyn / content fields with
                # tokenizer_name="simple" (and text_ngram with "whitespace").
                # A freshly-opened tantivy.Index does NOT know those custom
                # tokenizers, so EVERY parse_query against the fingerprint field
                # raised ValueError('The tokenizer "simple" ... is unknown'),
                # which lab_composition_search's `except (ValueError, RuntimeError):
                # continue` swallowed — silently skipping every chunk and returning
                # ZERO LOCAL LAB hits. The Genizah lab index never hit this because
                # _reload_lab_index() calls _ensure_lab_tokenizers(); the LOCAL
                # reload simply forgot to. Register them here, before any searcher
                # query, exactly as _reload_lab_index does.
                self._ensure_lab_tokenizers(local_lab_index)
                self._local_lab_index = local_lab_index
                self.local_lab_searcher = local_lab_index.searcher()
                self._lab_local_meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
                LAB_LOGGER.info(
                    "CR-02: LabEngine LOCAL LAB side-index reopened: %s",
                    Config.LOCAL_LAB_INDEX_DIR,
                )
            else:
                LAB_LOGGER.info(
                    "CR-02: LabEngine LOCAL LAB side-index dir absent; searcher=None"
                )
        except Exception as e:
            LAB_LOGGER.warning(
                "CR-02: LabEngine LOCAL LAB side-index unavailable: %r", e
            )
            self.local_lab_searcher = None
            self._local_lab_index = None
            self._lab_local_meta = None

    def _current_lab_weights_hash(self) -> str:
        """Compute hash of current LAB weights for D-38 staleness check.

        Mirrors SearchEngine._current_lab_weights_hash; uses the real
        dynamic_rank_map / settings that live on LabEngine.
        """
        import hashlib as _hashlib
        import json as _json
        weights_dict = {
            "dynamic_rank_map": self.dynamic_rank_map if self.dynamic_rank_map else None,
            "use_dynamic_weights": getattr(self.settings, "use_dynamic_weights", False),
        }
        return _hashlib.sha256(
            _json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _check_local_lab_freshness(self) -> bool:
        """Return True if LOCAL LAB index is fresh; False if stale or missing.

        D-38 mirror on LabEngine: compares current LAB weights_hash to value
        stored in .meta.json by build_lab_side_index. Side effect: sets
        self.local_lab_searcher_stale.
        """
        if getattr(self, "local_lab_searcher", None) is None:
            return False
        meta = getattr(self, "_lab_local_meta", None)
        if not meta:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info("CR-02: LabEngine LOCAL LAB has no .meta.json — stale")
            return False
        current_hash = self._current_lab_weights_hash()
        if meta.get("weights_hash") != current_hash:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info(
                "CR-02: LabEngine LOCAL LAB index stale (weights changed)"
            )
            return False
        self.local_lab_searcher_stale = False
        return True

    @staticmethod
    def lab_index_normalize(text):
        return re.sub(r"[^\w\u0590-\u05FF\s\*\~]", "", text).replace('_', ' ').lower()

    def rebuild_lab_index(self, progress_callback=None):
        LAB_LOGGER.info(f"Starting REBUILD at: {Config.LAB_INDEX_DIR}")

        # 1. Always Calculate Dynamic Weights First
        LAB_LOGGER.info("Calculating dynamic corpus statistics...")
        self.dynamic_rank_map = calculate_smart_weights(Config.FILE_V8, sample_size=None)

        self._close_index()
        time.sleep(0.5)

        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError("Input file not found")

        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                shutil.rmtree(Config.LAB_INDEX_DIR, ignore_errors=True)
            except Exception as e:
                LAB_LOGGER.error(f"Delete failed: {e}")

        os.makedirs(Config.LAB_INDEX_DIR, exist_ok=True)

        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True)
        builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
        builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace") # Legacy
        
        # The critical fields
        builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple") # Static
        builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")          # Dynamic
        
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("source", stored=True)
        builder.add_text_field("content", stored=True, tokenizer_name="simple")

        schema = builder.build()
        index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
        self._ensure_lab_tokenizers(index)
        writer = index.writer(heap_size=50_000_000)

        # --- Pre-calculation for progress percentage ---
        def count_documents(fname, label):
            if not os.path.exists(fname): return 0
            count = 0
            try:
                with open(fname, 'r', encoding='utf-8-sig') as f:
                    for line in f:
                        if label == "V0.8" and line.startswith("==>"): count += 1
                        elif label == "V0.7" and line.startswith("###"): count += 1
            except Exception as e:
                logging.getLogger(__name__).debug('Could not count documents in %s: %s', fname, e)
            return count

        estimated_total = count_documents(Config.FILE_V8, "V0.8") + count_documents(Config.FILE_V7, "V0.7")
        LAB_LOGGER.info(f"Estimated total docs: {estimated_total}")

        total_docs = 0
        
        def process_file(fpath, label):
            nonlocal total_docs
            if not os.path.exists(fpath): return
            LAB_LOGGER.info(f"Indexing {label}...")
            
            with open(fpath, 'r', encoding='utf-8-sig') as f:
                cid, chead, ctext = None, None, []
                for line in f:
                    line = line.strip()
                    is_sep = (label == "V0.8" and line.startswith("==>")) or (label == "V0.7" and line.startswith("###"))

                    if is_sep:
                        if cid and ctext:
                            original = "\n".join(ctext)
                            norm = self.lab_index_normalize(original)

                            fp_static = text_to_fingerprint(original, freq_map=HEBREW_FREQ)
                            fp_dyn = text_to_fingerprint(original, freq_map=self.dynamic_rank_map)

                            sm = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"

                            writer.add_document(tantivy.Document(
                                unique_id=str(cid),
                                text_normalized=norm,
                                fingerprint=fp_static,
                                fingerprint_dyn=fp_dyn,
                                content=original,
                                full_header=str(chead),
                                shelfmark=str(sm),
                                source=str(label)
                            ))
                            total_docs += 1
                            if progress_callback and total_docs % 1000 == 0:
                                progress_callback(total_docs, estimated_total)
                        
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = [] 
                    else:
                        ctext.append(line)
                
                # Last doc
                if cid and ctext:
                    original = "\n".join(ctext)
                    fp_static = text_to_fingerprint(original, freq_map=HEBREW_FREQ)
                    fp_dyn = text_to_fingerprint(original, freq_map=self.dynamic_rank_map)

                    writer.add_document(tantivy.Document(
                        unique_id=str(cid),
                        text_normalized=self.lab_index_normalize(original),
                        fingerprint=fp_static,
                        fingerprint_dyn=fp_dyn,
                        content=original,
                        full_header=str(chead),
                        shelfmark=str("Unknown"),
                        source=str(label)
                    ))
                    total_docs += 1

        process_file(Config.FILE_V8, "V0.8")
        process_file(Config.FILE_V7, "V0.7")

        writer.commit()
        LAB_LOGGER.info(f"Rebuild done. {total_docs} docs committed.")
        self._reload_lab_index()
        return total_docs

    def _create_lab_query(self, query_str, slop=0, field_name=None):
        """
        Helper to construct the Tantivy query object based on settings.
        """
        if field_name is None:
            field_name = self.LAB_FINGERPRINT_FIELD

        tokens = query_str.split()
        if not tokens:
            return None

        # If 100% match required, use Phrase Query
        if self.settings.min_should_match >= 100:
            final_query_str = f'{field_name}:"{query_str}"~{slop}'
        else:
            # OR query
            clauses = [f'{field_name}:{t}' for t in tokens]
            final_query_str = " OR ".join(clauses)

        # Try parsing strategies
        strategies = [
            lambda: self.lab_index.parse_query(final_query_str),
            lambda: self.lab_index.parse_query(final_query_str, [field_name]),
            lambda: self.lab_index.parse_query(final_query_str, [self.lab_index.schema.get_field(field_name)])
        ]

        for strategy in strategies:
            try:
                return strategy()
            except Exception:
                continue  # Try next query strategy; all-fail logged after loop by LAB_LOGGER.error

        LAB_LOGGER.error("All query strategies failed.")
        return None

    def _execute_batched_search(self, query_obj, progress_callback=None, limit_override=None):
        """
        Executes a Tantivy search in memory-safe batches.
        Yields (score, doc_address) tuples.
        """
        if not query_obj or not self.lab_searcher:
            return

        BATCH_SIZE = 5000
        MAX_SCAN_LIMIT = limit_override if limit_override else 50000

        # Determine strict limit
        limit = MAX_SCAN_LIMIT

        # 1. Fetch all candidate pointers (lightweight tuples)
        # Note: tantivy-py search() returns all hits at once, but they are just (score, addr).
        # This is memory-safe even for 50k items. The heavy lifting (doc loading) happens in the loop.
        try:
            res = self.lab_searcher.search(query_obj, limit)
        except Exception as e:
            LAB_LOGGER.warning(f"Search execution failed: {e}")
            return

        hits = res.hits
        total_hits = len(hits)

        # 2. Iterate in batches to allow for progress updates / UI breathing
        for i in range(0, total_hits, BATCH_SIZE):
            batch = hits[i : i + BATCH_SIZE]

            if progress_callback:
                # Send numeric progress for ProgressBar (i, total)
                try:
                    progress_callback(i, total_hits)
                except (InterruptedError, KeyboardInterrupt):
                    raise
                except Exception:
                    pass  # Score extraction optional — result still usable without score
                # Send text status for Label.
                # Same guard as the numeric call above: cancellation must propagate,
                # but a callback that can't handle the single-string protocol must
                # degrade to "no status text" — never abort a long deep-scan search
                # (prod 2026-06-12: web two-arg callback raised TypeError here).
                try:
                    progress_callback(f"Scanning items {i}-{min(i+BATCH_SIZE, total_hits)} / {total_hits}...")
                except (InterruptedError, KeyboardInterrupt):
                    raise
                except Exception:
                    pass  # Status text optional — search proceeds without it

            for hit in batch:
                yield hit

    def _get_term_weight(self, fp):
        """
        Calculates importance using User Configurable Stop-Word scores.
        """
        raw_weight = 0
        for char in fp:
            raw_weight += HEBREW_FREQ.get(char, 0)
        
        # 1. Words too short (<3 chars)
        if len(fp) < 3:
            return self.settings.stop_word_score 
        
        # 2. Common 3-letter words (low weight)
        if len(fp) == 3 and raw_weight < 18:
            return self.settings.common_3char_score

        # 3. Regular/Rare words
        final_weight = raw_weight
        
        # Length bonus only for significant words
        if len(fp) > 3:
            final_weight *= self.settings.length_bonus_factor
            
        return final_weight

    def _calculate_match_metrics(self, text, query_fingerprints_list, original_query_str, freq_map=None):
        """
        Calculates score with STRICT FREQUENCY CAP & SEQUENTIAL ORDER.
        1. Words appearing more times in text than in query yield ZERO score.
        2. Sequence matches get huge bonuses.
        """
        if not text:
            return 0, [], (0, 0)

        # 1. Exact Match Check
        def safe_norm(s): return re.sub(r"[^\w\u0590-\u05FF]", "", s).lower()
        norm_text = safe_norm(text)
        norm_query = safe_norm(original_query_str)
        exact_bonus = 0
        if norm_query and norm_query in norm_text:
            exact_bonus = 1000000

        # 2. Weights & Mapping
        fp_to_query_indices = defaultdict(list)
        term_weights = {}
        
        for idx, fp in enumerate(query_fingerprints_list):
            fp_to_query_indices[fp].append(idx)
            term_weights[fp] = self._get_term_weight(fp)

        max_possible_unique_weight = sum(term_weights.values()) 
        
        # 3. Collect Matches
        matches = []
        q_fp_set = set(query_fingerprints_list)
        
        for m in re.finditer(r"[\w\u0590-\u05FF\']+", text):
            word = m.group()
            fp = encode_word_shmidman(word, freq_map=freq_map)
            if fp in q_fp_set:
                matches.append({
                    'start': m.start(),
                    'end': m.end(),
                    'word': word,
                    'fp': fp,
                    'weight': term_weights[fp],
                    'q_indices': fp_to_query_indices[fp]
                })

        if not matches:
            return 0, [], (0, 0)

        # 4. Find Best Cluster
        max_score = 0
        best_window = (0, 0)
        total_matches = len(matches)
        
        unique_bonus = self.settings.unique_bonus_base
        common_factor = self.settings.common_penalty_factor
        density_pen = self.settings.density_penalty
        order_bonus_factor = self.settings.order_bonus
        
        lookahead_limit = len(query_fingerprints_list) * 5
        
        for i in range(total_matches):
            current_window_score = 0
            
            # Track quantities: how many times have we seen each word in the current window?
            seen_counts = defaultdict(int)
            
            # Track order
            last_valid_query_idx = -1
            sequential_chain_length = 0
            
            # Initialize by start word
            if matches[i]['q_indices']:
                last_valid_query_idx = matches[i]['q_indices'][0]

            for j in range(i, min(total_matches, i + lookahead_limit)):
                m = matches[j]
                
                # Check physical distance
                dist = m['end'] - matches[i]['start']
                if dist > 450: break 
                
                fp = m['fp']
                w = m['weight']
                
                # How many times does this word appear in the original query?
                allowed_count = len(fp_to_query_indices[fp])
                
                # How many times have we seen it in this window so far?
                seen_counts[fp] += 1
                
                # Calculate score for this specific word
                word_score = 0
                
                if seen_counts[fp] <= allowed_count:
                    # Valid occurrence (first or second allowed)
                    # Full score
                    word_score = (w * unique_bonus)
                else:
                    # Redundant occurrence (garbage). Word found enough times.
                    # Drastically reduced score (or 0 if user set 0)
                    word_score = (w * common_factor) 
                
                current_window_score += word_score

                # --- Order Bonus Logic ---
                found_sequence = False
                best_q_idx_for_match = -1
                
                for q_idx in m['q_indices']:
                    if q_idx > last_valid_query_idx:
                        best_q_idx_for_match = q_idx
                        found_sequence = True
                        break 
                
                if found_sequence:
                    sequential_chain_length += 1
                    current_window_score += (w * order_bonus_factor * sequential_chain_length)
                    last_valid_query_idx = best_q_idx_for_match
                
                # --- Density Penalty ---
                penalty = dist * density_pen
                final_window_score = current_window_score - penalty
                
                if final_window_score > max_score:
                    max_score = final_window_score
                    best_window = (i, j)

        # 5. Coverage Calculation
        start_idx, end_idx = best_window
        window_matches = matches[start_idx : end_idx + 1]
        
        found_unique_fps = set(m['fp'] for m in window_matches)
        found_unique_weight = sum(term_weights[fp] for fp in found_unique_fps)
        
        coverage_ratio = 0
        if max_possible_unique_weight > 0:
            coverage_ratio = found_unique_weight / max_possible_unique_weight
        
        final_score = (max_score * (coverage_ratio ** self.settings.coverage_power)) + exact_bonus

        return final_score, matches, best_window    

    def _generate_highlighted_snippet(self, text, matches, best_window):
        """
        Generates a snippet with asterisk markers (*text*) for highlighting.
        """
        if not text: return ""
        if not matches: return text[:300]

        start_m_idx, end_m_idx = best_window
        
        # Guard indices
        start_m_idx = max(0, start_m_idx)
        end_m_idx = min(len(matches) - 1, end_m_idx)

        # 1. Determine snippet bounds (100 chars context)
        padding = 100
        snippet_start_char = max(0, matches[start_m_idx]['start'] - padding)
        snippet_end_char = min(len(text), matches[end_m_idx]['end'] + padding)
        
        # Cosmetic: Don't cut in middle of word
        if snippet_start_char > 0:
            next_space = text.find(' ', snippet_start_char)
            if next_space != -1 and next_space < matches[start_m_idx]['start']:
                snippet_start_char = next_space + 1

        # 2. Collect relevant matches
        relevant_matches = matches[start_m_idx : end_m_idx + 1]
        
        # 3. Build text
        out_parts = []
        
        if snippet_start_char > 0: out_parts.append("... ")
        
        current_idx = snippet_start_char
        
        for m in relevant_matches:
            if m['start'] < snippet_start_char: continue
            if m['end'] > snippet_end_char: break
            
            # Plain text
            if m['start'] > current_idx:
                plain = text[current_idx : m['start']]
                out_parts.append(plain.replace('*', ''))
            
            # Highlighted word (Asterisks)
            word = text[m['start'] : m['end']]
            out_parts.append(f"*{word.replace('*', '')}*")
            
            current_idx = m['end']
        
        # Remainder
        if current_idx < snippet_end_char:
            out_parts.append(text[current_idx : snippet_end_char].replace('*', ''))
            
        if snippet_end_char < len(text): out_parts.append(" ...")
        
        final_text = "".join(out_parts)
        # Flatten for table display
        return final_text.replace("\n", " ").replace("\r", " ")

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0, deep_scan=False, scan_limit=50000,
                   corpus_scope: str = 'genizah'):
        """Lab Mode (fingerprint) word search.

        Phase 110 (UAT bug #2): honor the corpus selector — ``corpus_scope`` is
        'genizah' (Genizah LAB index only, legacy default), 'local' (LOCAL LAB
        side-index only), or 'all' (both, merged). Previously lab_search ignored
        the selector entirely and always queried the Genizah LAB index, so a
        regular Search-tab "Lab Mode + Local" run returned Genizah hits. Corpus
        is orthogonal to mode (Lab Mode is NOT hardwired to LOCAL), mirroring the
        composition path (lab_composition_search).
        """
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'

        # Strip combining diacritical marks and geresh/gershayim from query
        query_str = strip_search_diacritics(query_str)

        # Determine strategy: Static or Dynamic
        use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None

        target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
        target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

        # 1. Prepare Fingerprints
        fp_str = text_to_fingerprint(query_str, freq_map=target_map)
        if not fp_str: return []

        query_fp_list = fp_str.split()

        # 2. Fetch Candidates
        slop = max(50, int(self.settings.gap_penalty) * 10)

        results = []
        min_match_pct = self.settings.min_should_match

        # --- Shared per-doc processing (Genizah + LOCAL) ---
        def _process_lab_doc(doc, is_local):
            content = doc['content'][0]
            uid = doc['unique_id'][0]

            # --- Core: Calculate Score & Find Matches ---
            custom_score, matches, best_window = self._calculate_match_metrics(content, query_fp_list, query_str, freq_map=target_map)

            if custom_score < 15:
                return

            # Filter by Percentage (Approximate)
            if min_match_pct < 100:
                found_unique = set(m['fp'] for m in matches)
                needed_unique = set(query_fp_list)
                common = found_unique.intersection(needed_unique)
                if len(needed_unique) > 0 and (len(common) / len(needed_unique) * 100 < min_match_pct):
                    return

            # --- Highlight Snippet ---
            smart_snippet = self._generate_highlighted_snippet(content, matches, best_window)
            html_snippet = smart_snippet  # No HTML conversion needed, pure markers

            start_idx, end_idx = best_window
            relevant_matches = matches[start_idx : end_idx + 1]
            found_words = list(set(m['word'] for m in relevant_matches))
            found_words.sort(key=len, reverse=True)
            highlight_regex_str = "|".join(make_mark_tolerant_pattern(re.escape(w)) for w in found_words) if found_words else ""

            full_header = doc['full_header'][0]
            if is_local:
                # Phase 110 bug #2: build the LOCAL hit shape the search-results
                # renderer expects (load_next_batch reads display.source=='LOCAL'
                # and resolves the filename/parent-folder from the canonical
                # filepath). Parse sys_id + page from the LOCAL full_header
                # ({sys_id}_LOCAL_P{page}_F{file_id}) — same as _build_local_result_dict.
                sys_id = ""
                p_num = "1"
                _parts = full_header.split("_LOCAL_P")
                if len(_parts) == 2:
                    sys_id = _parts[0]
                    p_num = _parts[1].split("_F")[0]
                try:
                    _shelf = doc['shelfmark'][0] if doc['shelfmark'] else sys_id
                except Exception:
                    _shelf = sys_id
                display_meta = {
                    "id": sys_id,
                    "source": "LOCAL",
                    "library_code": "LOCAL",
                    "shelfmark": _shelf,
                    "img": p_num,
                }
                results.append({
                    'sort_score': custom_score,
                    'display': display_meta,
                    'snippet': html_snippet,
                    'full_text': content,
                    'uid': uid,
                    'raw_header': full_header,
                    'raw_file_hl': smart_snippet,
                    'highlight_pattern': highlight_regex_str,
                    # LOCAL extras for ResultDialog / Browse / file-open actions
                    'sys_id': sys_id,
                    'p_num': p_num,
                    'img': p_num,
                    'full_header': full_header,
                    'score': float(custom_score),
                })
            else:
                # Populate display metadata correctly
                display_meta = self.meta_mgr.get_display_data(full_header, doc['source'][0])
                results.append({
                    'sort_score': custom_score,
                    'display': display_meta,
                    'snippet': html_snippet,
                    'full_text': content,
                    'uid': uid,
                    'raw_header': full_header,
                    'raw_file_hl': smart_snippet,
                    # This is the magic key for the Viewer:
                    'highlight_pattern': highlight_regex_str
                })

        # 3. Process — Genizah LAB loop (skipped for corpus_scope='local', and
        # gracefully skipped if the Genizah LAB index was never built).
        if corpus_scope != 'local' and self.lab_searcher is not None and self.lab_index is not None:
            query_obj = self._create_lab_query(fp_str, slop, field_name=target_field)
            if query_obj:
                if deep_scan:
                    # Use Deep Scan batched iterator
                    def batch_cb(*args):
                        if progress_callback:
                            try:
                                progress_callback(*args)
                            except Exception:
                                pass  # Progress callback optional — search proceeds without progress updates

                    iterator = self._execute_batched_search(query_obj, progress_callback=batch_cb, limit_override=scan_limit)
                else:
                    # Standard Fast Method
                    try:
                        # Limit 5000 for standard scan
                        res = self.lab_searcher.search(query_obj, 5000)
                        iterator = res.hits
                    except Exception as e:
                        LOGGER.debug('Batched search query failed, falling back to empty: %s', e)
                        iterator = []

                for score, doc_addr in iterator:
                    try:
                        _process_lab_doc(self.lab_searcher.doc(doc_addr), is_local=False)
                    except Exception as e:
                        LAB_LOGGER.error(f"Error processing doc: {e}")

        # 3b. LOCAL LAB loop (corpus_scope 'local' or 'all'). Mirrors the LOCAL
        # extension in lab_composition_search: query the LOCAL LAB side-index with
        # the same fingerprint field, build LOCAL-shaped hits. The simple/whitespace
        # tokenizers are registered by reload_local_lab_index (Phase 110 UAT fix) so
        # parse_query on the fingerprint field no longer raises.
        if (corpus_scope != 'genizah'
                and getattr(self, 'local_lab_searcher', None) is not None
                and getattr(self, '_local_lab_index', None) is not None):
            _tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
            _searchable = getattr(_tab, "is_searchable", True) if _tab is not None else True
            if _searchable:
                try:
                    _clauses = [f'{target_field}:{t}' for t in fp_str.split()]
                    _core_query = " OR ".join(_clauses)
                    _q_obj = self._local_lab_index.parse_query(_core_query)
                    _res = self.local_lab_searcher.search(_q_obj, 5000)
                    for _score, _doc_addr in _res.hits:
                        try:
                            _process_lab_doc(self.local_lab_searcher.doc(_doc_addr), is_local=True)
                        except Exception as _e:
                            LAB_LOGGER.error(f"Error processing LOCAL LAB doc: {_e}")
                except (ValueError, RuntimeError):
                    pass  # tokenizer/parse issue — skip LOCAL contribution gracefully
                except Exception as _local_exc:
                    LAB_LOGGER.warning("lab_search LOCAL LAB scan failed: %r", _local_exc)

        # 4. Sort & Dedup (Logic Fixed: Prioritize V0.8 over V0.7)
        v8_map = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
        
        final_list = []
        
        # Add all V0.8 results
        final_list.extend(v8_map.values())
        
        # Add V0.7 results *only* if UID not in V0.8
        for r in results:
            if r['display']['source'] != "V0.8": # V0.7 or others
                if r['uid'] not in v8_map:
                    final_list.append(r)

        # Finally, sort unified list by highest score
        final_list.sort(key=lambda x: x['sort_score'], reverse=True)

        return final_list

    def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None,
                                excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000,
                                boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                                min_boundary_matches=0, min_delimiter_distance=3,
                                corpus_scope: str = 'genizah'):
        """
        Scans a composition using Lab Mode.
        UPGRADES:
        1. Filters common phrases.
        2. Boosts V0.8.
        3. FIX: Separates excluded/known manuscripts.
        4. Supports Filter Text and Batching.
        5. Returns partial results if interrupted/cancelled.
        6. Supports boundary-crossing search modes.

        Boundary Search Modes:
        - 'full': Regular search, track boundary matches for display
        - 'boundary': Only return results with boundary-crossing matches
        - 'combined': Full search with score boost for boundary matches

        Phase 110 (COMP-LOC-01/02): corpus_scope selects which index loop runs —
        'genizah' (Genizah lab loop only), 'local' (LOCAL LAB loop only), or 'all'
        (both, merged into results_map). Corpus is orthogonal to mode (Lab Mode is
        NOT hardwired to LOCAL).
        """
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'
        _local_lab_stale = False  # Phase 110 Round-2 #4: A2 default so EVERY return path carries it

        if not full_text:
            return {'main': [], 'filtered': [], 'known': [], 'partial': False, 'boundary_stats': None,
                    'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

        # Strip combining diacritical marks and geresh/gershayim from queries
        full_text = strip_search_diacritics(full_text)
        if filter_text:
            filter_text = strip_search_diacritics(filter_text)

        # Reset debug counter for this search (prevents state leak between searches)
        self._filter_match_count = 0

        # Determine strategy: Static or Dynamic
        use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
        target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
        target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

        # Normalize exclusion list for fast lookup
        excluded_set = set(str(x) for x in (excluded_ids or []))

        # User settings
        PER_CHUNK_LIMIT = self.settings.comp_chunk_limit
        MIN_SCORE_THRESHOLD = self.settings.comp_min_score
        MAX_FINAL = self.settings.comp_max_final_results
        min_pct_ratio = self.settings.min_should_match / 100.0

        # (Part 1: Tokenization) - track positions for preserving formatting
        token_matches = list(re.finditer(r"[\w\u0590-\u05FF\']+", full_text))
        tokens = [strip_nikud(m.group()) for m in token_matches]  # Strip nikud from tokens
        token_positions = [(m.start(), m.end()) for m in token_matches]  # Store positions
        c_size = chunk_size if chunk_size else 15
        step = max(1, int(c_size * 0.5))

        # Strip nikud from filter text for consistent matching
        if filter_text:
            filter_text = strip_nikud(filter_text)

        # Get boundary stats (includes parsed boundaries to avoid double parsing)
        boundary_stats = get_boundary_stats(full_text, boundary_delimiter, c_size, min_delimiter_distance)
        boundaries = boundary_stats.get('boundaries', [])

        # Build chunks - handle short texts first to avoid wasteful iteration
        chunks_data = []
        if len(tokens) < c_size:
            # Short text: single chunk with all tokens
            crossed_bounds = get_crossed_boundaries(0, len(tokens), boundaries)
            chunks_data = [(0, tokens, crossed_bounds)]
        else:
            # Normal text: create overlapping chunks
            for i in range(0, max(1, len(tokens) - c_size + 1), step):
                chunk_end = i + c_size
                crossed_bounds = get_crossed_boundaries(i, chunk_end, boundaries)
                chunks_data.append((i, tokens[i : i + c_size], crossed_bounds))

        total_chunks = len(chunks_data)
        results_map = {}
        was_interrupted = False
        chunks_processed = 0

        # (Part 2: Scanning) - wrapped in try/except to support partial results on cancel
        try:
          # Phase 110: gate the Genizah lab loop — skipped on a LOCAL-only run.
          # Phase 110 UAT (Issue 3): guard against an UNBUILT Genizah fingerprint
          # LAB index. When Config.LAB_INDEX_DIR has never been built,
          # self.lab_index / self.lab_searcher are None and a Lab-Mode + Genizah
          # run (chunk_size>3) crashed with
          # "'NoneType' object has no attribute 'parse_query'". Mirror the LOCAL
          # LAB loop's existing None-guard below: skip the Genizah lab contribution
          # gracefully (no Genizah-lab hits) — do NOT crash, do NOT build anything.
          if corpus_scope != 'local' and (self.lab_index is None or self.lab_searcher is None):
            LAB_LOGGER.info(
                "lab_composition_search: Genizah LAB index not built — skipping Genizah lab loop"
            )
          if corpus_scope != 'local' and self.lab_index is not None and self.lab_searcher is not None:
            for i, (token_start_idx, chunk_tokens, chunk_crossed_bounds) in enumerate(chunks_data):
                chunks_processed = i
                if progress_callback: progress_callback(i, total_chunks)
                chunk_text = " ".join(chunk_tokens)

                if self._is_phrase_statistically_weak(chunk_text): continue

                fp_str = text_to_fingerprint(chunk_text, freq_map=target_map)
                if not fp_str or len(chunk_tokens) < 4: continue

                fp_list = fp_str.split()
                needed_unique_fps = set(fp_list)

                # Query with Boost
                query_tokens = fp_str.split()
                clauses = [f'{target_field}:{t}' for t in query_tokens]
                core_query = " OR ".join(clauses)
                final_query_str = f'({core_query}) AND (source:"V0.8"^10 OR source:"V0.7")'

                q_obj = None
                try:
                    q_obj = self.lab_index.parse_query(final_query_str)
                except (ValueError, RuntimeError):
                    try:
                        q_obj = self.lab_index.parse_query(core_query)
                    except (ValueError, RuntimeError): continue

                if not q_obj: continue

                iterator = []
                if deep_scan:
                    batch_cb = None
                    if progress_callback:
                        batch_cb = lambda *args: progress_callback(*args) if callable(progress_callback) else None
                    iterator = self._execute_batched_search(q_obj, progress_callback=batch_cb, limit_override=scan_limit)
                else:
                    try:
                        res = self.lab_searcher.search(q_obj, 5000)
                        iterator = res.hits
                    except Exception as e:
                        LOGGER.debug('Batched search query failed, falling back to empty: %s', e)
                        iterator = []

                for score, doc_addr in iterator:
                    try:
                        doc = self.lab_searcher.doc(doc_addr)
                        content = doc['content'][0]
                        uid = doc['unique_id'][0]

                        # --- Filter Text Logic ---
                        # Check if the search chunk's words appear in sequence in the filter text
                        is_filtered_match = False
                        if filter_text and len(chunk_tokens) >= 3:
                            # Normalize: keep only Hebrew letters, join with single space
                            clean_chunk = ' '.join(re.findall(r'[\u05D0-\u05EA]+', chunk_text))
                            if clean_chunk and clean_chunk in filter_text:
                                is_filtered_match = True

                        match_score, matches, best_window = self._calculate_match_metrics(content, fp_list, chunk_text, freq_map=target_map)

                        found_unique_fps = set(m['fp'] for m in matches[best_window[0]:best_window[1]+1])
                        common_fps = found_unique_fps.intersection(needed_unique_fps)
                        if len(needed_unique_fps) > 0:
                            if (len(common_fps) / len(needed_unique_fps)) < min_pct_ratio: continue

                        if match_score < MIN_SCORE_THRESHOLD: continue

                        if uid not in results_map:
                            results_map[uid] = {
                                'uid': uid, 'total_score': 0, 'hits_count': 0,
                                'raw_header': doc['full_header'][0], 'source': doc['source'][0],
                                'content': content, 'best_chunk_score': -1,
                                'all_found_words': set(), 'src_indices': set(), 'ms_matches': [],
                                'is_text_filtered': False,
                                # Boundary tracking - use set to count each boundary only once
                                'boundary_chunk_scores': [],
                                'crossed_boundaries': set(),
                                # Phase 77 D-13: per-chunk attribution for parallels JSON
                                # matches[]. Tuples are (chunk_index_0_based, source_chunk_text,
                                # match_score, manuscript_snippet). Used by
                                # shared/search_serializer.serialize_parallels_payload.
                                'chunk_hits': [],
                            }
                        rec = results_map[uid]

                        if is_filtered_match:
                            rec['is_text_filtered'] = True

                        # Track boundary-crossing matches - each boundary counted once
                        if chunk_crossed_bounds:
                            rec['boundary_chunk_scores'].append(match_score)
                            rec['crossed_boundaries'].update(chunk_crossed_bounds)

                        rec['total_score'] += match_score
                        rec['hits_count'] += 1
                        token_end_idx = token_start_idx + len(chunk_tokens)
                        rec['src_indices'].update(range(token_start_idx, token_end_idx))
                        start_m, end_m = best_window
                        if matches:
                            rec['ms_matches'].append((matches[start_m]['start'], matches[end_m]['end']))
                            for m in matches[start_m : end_m + 1]: rec['all_found_words'].add(m['word'])
                            # Phase 77 D-13: per-chunk attribution for parallels matches[].
                            # i is the 0-based chunk index from the outer enumerate(chunks_data) loop.
                            # The manuscript snippet is the same substring used for ms_matches.
                            ms_snip = content[matches[start_m]['start']:matches[end_m]['end']]
                            # Dedup: same (chunk_index, ms_snip) can arise from
                            # multiple Tantivy segments returning the same uid.
                            # Keep the highest-scoring entry per key.
                            _seen = rec.setdefault('_chunk_hit_keys', {})
                            _key = (i, ms_snip)
                            _existing_idx = _seen.get(_key)
                            if _existing_idx is None:
                                _seen[_key] = len(rec['chunk_hits'])
                                rec['chunk_hits'].append((i, chunk_text, match_score, ms_snip))
                            elif match_score > rec['chunk_hits'][_existing_idx][2]:
                                rec['chunk_hits'][_existing_idx] = (i, chunk_text, match_score, ms_snip)
                    except (KeyError, IndexError, TypeError) as _dedup_exc:
                        logging.getLogger(__name__).debug(
                            "lab_composition_search: skipped chunk-hit dedup entry: %r", _dedup_exc
                        )
        except InterruptedError:
            was_interrupted = True

        # Phase 95 D-09: LOCAL LAB extension — query LOCAL LAB side-index with same
        # fingerprint scoring path (NOT RRF, NOT BM25 — custom scoring per D-09).
        # Results merged into results_map so Part 3 handles them uniformly.
        # Guard: _check_local_lab_freshness is defined on SearchEngine (not LabEngine).
        _freshness_fn = getattr(self, "_check_local_lab_freshness", None)
        # Phase 97 R-01: skip LOCAL LAB search if is_searchable gate is closed.
        _lab_tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
        _lab_is_searchable = getattr(_lab_tab, "is_searchable", True) if _lab_tab is not None else True
        # Phase 110: compute freshness ONCE (preserve D-37 try/except — never raise on
        # the worker thread; never trigger a rebuild here).
        _lab_fresh_lab = False
        if callable(_freshness_fn):
            try:
                _lab_fresh_lab = bool(_freshness_fn())
            except Exception as _lab_fresh_exc:
                LOGGER.warning(
                    "lab_composition_search: _check_local_lab_freshness raised %r — "
                    "skipping LOCAL LAB extension (D-37 fallback).",
                    _lab_fresh_exc,
                )
                _lab_fresh_lab = False
        # Phase 110 A2 + M2: per-run stale verdict — stale ONLY when an index is present
        # but not fresh (stale != no-index). Set the back-compat engine flag only then.
        _local_index_present_lab = getattr(self, 'local_lab_searcher', None) is not None
        _local_lab_stale = bool(
            corpus_scope in ('local', 'all') and _local_index_present_lab and not _lab_fresh_lab
        )
        if _local_lab_stale:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info(
                "lab_composition_search: LOCAL LAB weights-hash reports stale, but "
                "searching the present index anyway (freshness is advisory, not a hard "
                "gate — the static fingerprint field is weights-independent; an empty "
                "result is worse than a slightly-stale one)."
            )
        # Phase 110 (UAT fix): gate the LOCAL LAB loop on corpus_scope + the index being
        # PRESENT — NOT on `_lab_fresh_lab`. The weights-hash freshness check is perpetually
        # false-stale when the LabEngine's dynamic_rank_map differs build-vs-search (see
        # rebuild_local_lab_index docstring), which silently suppressed ALL Lab+LOCAL results.
        # Freshness stays advisory (per-run `local_lab_stale` payload + the log above); the
        # inner None-guard below keeps it crash-safe.
        if (not was_interrupted and corpus_scope != 'genizah' and _lab_is_searchable
                and getattr(self, 'local_lab_searcher', None) is not None
                and getattr(self, '_local_lab_index', None) is not None):
            try:
                local_lab_index = getattr(self, "_local_lab_index", None)
                local_lab_searcher = self.local_lab_searcher
                if local_lab_index is not None and local_lab_searcher is not None:
                    for _i, (_token_start_idx, _chunk_tokens, _chunk_crossed_bounds) in enumerate(chunks_data):
                        _chunk_text = " ".join(_chunk_tokens)
                        if self._is_phrase_statistically_weak(_chunk_text):
                            continue
                        _fp_str = text_to_fingerprint(_chunk_text, freq_map=target_map)
                        if not _fp_str or len(_chunk_tokens) < 4:
                            continue
                        _fp_list = _fp_str.split()
                        _needed_unique_fps = set(_fp_list)
                        _clauses = [f'{target_field}:{t}' for t in _fp_str.split()]
                        _core_query = " OR ".join(_clauses)
                        try:
                            _q_obj = local_lab_index.parse_query(_core_query)
                        except (ValueError, RuntimeError):
                            continue
                        if not _q_obj:
                            continue
                        try:
                            _res = local_lab_searcher.search(_q_obj, 5000)
                            _local_iter = _res.hits
                        except Exception:
                            continue
                        for _score, _doc_addr in _local_iter:
                            try:
                                _doc = local_lab_searcher.doc(_doc_addr)
                                _content = _doc['content'][0]
                                _uid = _doc['unique_id'][0]
                                if filter_text and len(_chunk_tokens) >= 3:
                                    _clean_chunk = ' '.join(re.findall(r'[א-ת]+', _chunk_text))
                                    if _clean_chunk and _clean_chunk in filter_text:
                                        pass  # LOCAL LAB hits not filter-excluded by source_text
                                _match_score, _matches, _best_window = self._calculate_match_metrics(
                                    _content, _fp_list, _chunk_text, freq_map=target_map
                                )
                                _found_unique_fps = set(
                                    m['fp'] for m in _matches[_best_window[0]:_best_window[1] + 1]
                                )
                                _common_fps = _found_unique_fps.intersection(_needed_unique_fps)
                                if len(_needed_unique_fps) > 0:
                                    if (len(_common_fps) / len(_needed_unique_fps)) < min_pct_ratio:
                                        continue
                                if _match_score < MIN_SCORE_THRESHOLD:
                                    continue
                                if _uid not in results_map:
                                    results_map[_uid] = {
                                        'uid': _uid, 'total_score': 0, 'hits_count': 0,
                                        'raw_header': _doc['full_header'][0],
                                        'source': _doc['source'][0],
                                        'content': _content, 'best_chunk_score': -1,
                                        'all_found_words': set(), 'src_indices': set(),
                                        'ms_matches': [], 'is_text_filtered': False,
                                        'boundary_chunk_scores': [],
                                        'crossed_boundaries': set(),
                                        'chunk_hits': [],
                                    }
                                _rec = results_map[_uid]
                                if _chunk_crossed_bounds:
                                    _rec['boundary_chunk_scores'].append(_match_score)
                                    _rec['crossed_boundaries'].update(_chunk_crossed_bounds)
                                _rec['total_score'] += _match_score
                                _rec['hits_count'] += 1
                                _token_end_idx = _token_start_idx + len(_chunk_tokens)
                                _rec['src_indices'].update(range(_token_start_idx, _token_end_idx))
                                _start_m, _end_m = _best_window
                                if _matches:
                                    _rec['ms_matches'].append(
                                        (_matches[_start_m]['start'], _matches[_end_m]['end'])
                                    )
                                    for _m in _matches[_start_m:_end_m + 1]:
                                        _rec['all_found_words'].add(_m['word'])
                                    _ms_snip = _content[
                                        _matches[_start_m]['start']:_matches[_end_m]['end']
                                    ]
                                    _seen_llb = _rec.setdefault('_chunk_hit_keys', {})
                                    _key_llb = (_i, _ms_snip)
                                    _existing_llb = _seen_llb.get(_key_llb)
                                    if _existing_llb is None:
                                        _seen_llb[_key_llb] = len(_rec['chunk_hits'])
                                        _rec['chunk_hits'].append(
                                            (_i, _chunk_text, _match_score, _ms_snip)
                                        )
                                    elif _match_score > _rec['chunk_hits'][_existing_llb][2]:
                                        _rec['chunk_hits'][_existing_llb] = (
                                            _i, _chunk_text, _match_score, _ms_snip
                                        )
                            except (KeyError, IndexError, TypeError) as _dedup_llb_exc:
                                logging.getLogger(__name__).debug(
                                    "lab_composition_search: skipped LOCAL-LAB chunk-hit dedup entry: %r",
                                    _dedup_llb_exc,
                                )
            except Exception as _local_lab_exc:
                logging.getLogger(__name__).warning(
                    "lab_composition_search: LOCAL LAB scan failed: %r", _local_lab_exc,
                    exc_info=True,
                )

        # (Part 3: Result Processing) - runs even if interrupted to return partial results
        raw_final_items = []
        is_short_search = (total_chunks <= 3)

        for uid, data in results_map.items():
            if not is_short_search:
                if data['hits_count'] < 2 and data['total_score'] < 1000: continue 
            else:
                if data['total_score'] < 250: continue

            # Generate snippets
            src_snippets = []
            src_indices = sorted(list(data['src_indices']))
            if src_indices:
                clusters = []
                curr_cluster = [src_indices[0]]
                for idx in src_indices[1:]:
                    if idx - curr_cluster[-1] < 60: curr_cluster.append(idx)
                    else: clusters.append(curr_cluster); curr_cluster = [idx]
                clusters.append(curr_cluster)
                for cl in clusters:
                    start_ctx = max(0, cl[0] - 50); end_ctx = min(len(tokens), cl[-1] + 51)
                    cl_set = set(cl)

                    # Get character positions from token_positions - preserve original formatting
                    char_start = token_positions[start_ctx][0]
                    char_end = token_positions[end_ctx - 1][1]
                    original_snippet = full_text[char_start:char_end]

                    # Build highlights for matched words
                    highlights = []
                    for k in range(start_ctx, end_ctx):
                        if k in cl_set:
                            word_char_start = token_positions[k][0] - char_start
                            word_char_end = token_positions[k][1] - char_start
                            highlights.append((word_char_start, word_char_end))

                    # Apply highlights in reverse order to preserve positions
                    result = original_snippet
                    for word_start, word_end in reversed(highlights):
                        result = result[:word_start] + '*' + result[word_start:word_end] + '*' + result[word_end:]

                    src_snippets.append(f"... {result} ...")

            ms_snips = []
            spans = sorted(data['ms_matches'], key=lambda x: x[0])
            merged = []
            if spans:
                curr_s, curr_e = spans[0]
                for s, e in spans[1:]:
                    if s <= curr_e + 20: curr_e = max(curr_e, e)
                    else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
                merged.append((curr_s, curr_e))
            
            content = data['content']
            for s, e in merged:
                start = max(0, s - 60); end = min(len(content), e + 60)
                snip = content[start:end]
                rs = max(0, s - start); re_ = min(len(snip), e - start)
                if re_ > rs:
                    ms_snips.append(snip[:rs] + f"*{snip[rs:re_]}*" + snip[re_:])

            found_words = sorted(list(data['all_found_words']), key=len, reverse=True)[:50]
            hl_pattern = "|".join(re.escape(w) for w in found_words) if found_words else ""

            # Calculate boundary match quality and final score with boost
            base_score = data['total_score']
            boundary_chunk_scores = data.get('boundary_chunk_scores', [])
            has_boundary_matches = len(boundary_chunk_scores) > 0
            boundary_quality = calculate_boundary_quality(boundary_chunk_scores)

            # Apply score boost in combined mode
            if boundary_mode == 'combined' and has_boundary_matches:
                final_score = calculate_final_score_with_boost(
                    base_score, boundary_quality, has_boundary_matches, boundary_boost
                )
            else:
                final_score = base_score

            # Calculate normalized boundary quality (0-1 range)
            boundary_quality_normalized = 0.0
            if has_boundary_matches and base_score > 0:
                boundary_quality_normalized = min(boundary_quality / base_score, 1.0)

            item = {
                'score': base_score,
                'final_score': final_score,
                'uid': uid,
                'raw_header': data['raw_header'],
                'src_lbl': data['source'],
                'source_ctx': "\n\n".join(src_snippets),
                'text': "\n...\n".join(ms_snips),
                'highlight_pattern': hl_pattern,
                'full_text': data['content'],
                'is_text_filtered': data.get('is_text_filtered', False),
                'filter_reason': 'source_text' if data.get('is_text_filtered', False) else '',
                # Boundary metadata
                'has_boundary_matches': has_boundary_matches,
                'boundary_match_count': len(data.get('crossed_boundaries', set())),
                'boundary_quality': boundary_quality_normalized,
                # Phase 77 D-13: surface per-chunk attribution to consumers
                # (serialize_parallels_payload, /api/parallels). Each tuple is
                # (chunk_index_0_based, source_chunk_text, match_score,
                # manuscript_snippet). May be empty if no chunks matched
                # (defensive default for forward compatibility).
                'chunk_hits': data.get('chunk_hits', []),
                # User-facing chunk_count: unique source-chunk contents. The
                # internal `hits_count` counter (line 1448) drives the noise
                # gate at line 1480 and is left alone. The full-mode
                # min_boundary_matches filter below reads this derived field
                # so repeated source phrases don't inflate the result.
                'chunk_count': _count_unique_chunks(data.get('chunk_hits', [])),
            }
            raw_final_items.append(item)

        # --- Sorting & Splitting Logic ---
        # In combined mode, sort by final_score; otherwise by base score
        if boundary_mode == 'combined':
            raw_final_items.sort(key=lambda x: x.get('final_score', x['score']), reverse=True)
        else:
            raw_final_items.sort(key=lambda x: x['score'], reverse=True)

        main_list = []
        known_list = []
        filtered_list = []

        for item in raw_final_items:
            # In boundary-only mode, skip items without boundary matches
            if boundary_mode == 'boundary' and not item.get('has_boundary_matches', False):
                continue

            # Apply min_boundary_matches filter
            if min_boundary_matches > 0:
                if boundary_mode == 'full':
                    # Use derived chunk_count (unique source chunks), not
                    # the internal hits_count which was never surfaced on
                    # the item dict (latent always-zero bug pre-fix).
                    if item.get('chunk_count', 0) < min_boundary_matches:
                        continue
                else:
                    if item.get('boundary_match_count', 0) < min_boundary_matches:
                        continue

            # Check if manuscript is excluded
            is_excluded = False

            # 1. Check by UID (e.g. IE...)
            if str(item['uid']) in excluded_set:
                is_excluded = True

            # 2. Check by System ID (99...) found in header
            if not is_excluded:
                m = re.search(r'(99\d+)', str(item['raw_header']))
                if m and m.group(1) in excluded_set:
                    is_excluded = True

            if is_excluded:
                known_list.append(item)
            elif item.get('is_text_filtered'):
                filtered_list.append(item)
            else:
                main_list.append(item)

        # Truncate limit only on main list
        if len(main_list) > MAX_FINAL:
            main_list = main_list[:MAX_FINAL]

        # Split return so GUI builds tree correctly
        return {
            'main': main_list,
            'known': known_list,
            'filtered': filtered_list,
            'partial': was_interrupted,
            'boundary_stats': boundary_stats,
            # Phase 110 A2 + Round-2 #4: per-run scope + staleness verdict.
            'corpus_scope': corpus_scope,
            'local_lab_stale': _local_lab_stale,
        }
    
    @lru_cache(maxsize=10000)
    def _is_word_too_common(self, word, threshold=5000):
        """
        Check existing index stats to see if a word is essentially a stop-word.
        Uses LRU Cache to avoid hitting the index repeatedly for 'אמר' or 'על'.
        """
        try:
            # Tantivy allows checking document frequency for a term
            # Note: Create a Term object for the specific field
            # In some tantivy-py versions command is doc_freq
            # We check how many documents contain the word

            # Determine field based on setting
            use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
            target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD

            count = self.lab_searcher.doc_freq(self.lab_index.schema.get_field(target_field), word)
            return count > threshold
        except Exception:
            # If error/unsupported, assume word is not too common to avoid missing it
            return False

    def _is_phrase_statistically_weak(self, phrase_text):
        """
        Returns True if the phrase consists ONLY of extremely common words.
        If it has at least one 'rare' anchor word, it returns False (keep it).
        """
        # Clean punctuation and split to words
        words = re.findall(r"[\w\u0590-\u05FF]+", phrase_text)
        if not words:
            return True # Empty phrase is weak
            
        rare_anchors = 0
        
        for w in words:
            # We use Shmidman encoding as stored in index,
            # but could check raw word if stored,
            # or its Fingerprint.
            # Assuming Fingerprint check as it's our indexed field:

            use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
            target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

            fp_word = encode_word_shmidman(w, freq_map=target_map)
            if not fp_word: continue
            
            # If word is *not* too common, anchor found!
            if not self._is_word_too_common(fp_word):
                rare_anchors += 1
        
        # If no rare word found, phrase is weak
        return rare_anchors == 0
    
# LIBRARY_CODES, normalize_shelfmark, natural_sort_key, get_library_display,
# _get_library_prefix_aliases, _strip_library_prefix, _load_ie_volume_map,
# _extract_ie_from_header, _repair_missing_ie_pages, dedupe_browse_map
# — all moved to shared/browse_map_utils.py (Phase 123). Re-exported above.




def get_volume_pages(pages, ie_id):
    """Filter browse_map pages to a specific IE's pages only.

    Args:
        pages: List of page dicts from browse_map[sys_id]
        ie_id: IE identifier to filter for

    Returns:
        List of page dicts belonging to the specified IE, sorted by p_num.
    """
    return [p for p in pages if p.get('ie_id') == ie_id]


def get_volumes_for_sys_id(sys_id):
    """Get volume information for a sys_id from ie_volume_map.

    Returns:
        List of {"ie_id", "suffix", "page_count"} dicts, or empty list for single-IE.
    """
    ie_volume_map = _load_ie_volume_map()
    entry = ie_volume_map.get(sys_id, {})
    return entry.get("volumes", [])


def resolve_volume_suffix(sys_id, ie_id):
    """Map an IE identifier to its IIIF manifest suffix for a given sys_id.

    Args:
        sys_id: NLI system ID
        ie_id: IE identifier (e.g. 'IE89040977')

    Returns:
        int: The suffix (1-based) for the IIIF manifest URL, or 1 if not found.
    """
    if not ie_id:
        return 1
    volumes = get_volumes_for_sys_id(sys_id)
    for v in volumes:
        if v['ie_id'] == ie_id:
            return v['suffix']
    return 1


# ==============================================================================
#  METADATA MANAGER (moved to shared/metadata_manager.py — Phase 124)
# ==============================================================================
# _CUDL_LABEL_RE, _parse_cudl_label, _BRIDGE_IMPORT_WARNED, _warn_bridge_import_failed,
# _nli_crossref_svc, _get_crossref_service, _fjms_svc, _get_fjms_service,
# _NLI_CACHE_MAX_ENTRIES, _BoundedLRUCache, MARC_FUTURE_TIMEOUT,
# NLI_IIIF_FUTURE_TIMEOUT, EXTERNAL_IIIF_HTTP_TIMEOUT, MetadataManager
# all re-exported from genizah_core via the Phase 124 compat shim above.

# ==============================================================================
#  LOGGING
# ==============================================================================


def configure_logger():
    """Configure a rotating file logger for the app (quiet for users, verbose for devs)."""
    logger = logging.getLogger("genizah")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    os.makedirs(Config.INDEX_DIR, exist_ok=True)

    file_handler = SafeRotatingFileHandler(Config.LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    logger.propagate = False
    return logger


def get_logger(name=None):
    base_logger = configure_logger()
    return base_logger.getChild(name) if name else base_logger


LOGGER = get_logger(__name__)

# Phase 84: WARNING-once flag for shelfmark_bridge import failures (Gemini LOW).
_BRIDGE_IMPORT_WARNED = False


def _warn_bridge_import_failed(exc):
    """Log shelfmark_bridge import failure at WARNING once per process (Gemini LOW)."""
    global _BRIDGE_IMPORT_WARNED
    if not _BRIDGE_IMPORT_WARNED:
        LOGGER.warning("shelfmark_bridge unavailable (degrading to v7.10 behavior): %s", exc)
        _BRIDGE_IMPORT_WARNED = True


def configure_lab_logger():
    """Configure a separate logger for Lab Mode operations."""
    lab_logger = logging.getLogger("GenizahLab")
    if lab_logger.handlers:
        # Check if it only has NullHandler (length 1 and is NullHandler)
        # If so, we still want to add the real handlers.
        # But for simplicity in this specific task context:
        # The user instruction says: "If using a global logger, use NullHandler as default".
        # When this runs, we want to ADD file/stream handlers.
        # However, `logging.getLogger` returns the same instance.
        # So we should just check if we have "real" handlers or just clear and re-add.
        # Let's follow the standard pattern:
        # If it has handlers other than NullHandler, return.
        has_real = any(not isinstance(h, logging.NullHandler) for h in lab_logger.handlers)
        if has_real:
            return lab_logger

    lab_logger.setLevel(logging.DEBUG)

    # Ensure lab directory exists
    os.makedirs(Config.LAB_DIR, exist_ok=True)

    file_handler = SafeRotatingFileHandler(Config.LAB_LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s"))
    file_handler.setLevel(logging.DEBUG)
    lab_logger.addHandler(file_handler)

    # Optional: Log to console as well if debugging
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("[LAB] %(levelname)s: %(message)s"))
    console.setLevel(logging.INFO)
    lab_logger.addHandler(console)

    lab_logger.propagate = False
    return lab_logger

LAB_LOGGER = configure_lab_logger()

# Paths resolved through PyInstaller-friendly helper
Config.HELP_FILE = Config.resource_path("Help.html")

def load_language():
    """Load language preference. Returns 'en' or 'he'."""
    try:
        if os.path.exists(Config.LANGUAGE_FILE):
            with open(Config.LANGUAGE_FILE, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        LOGGER.warning("Failed to load language preference from %s: %s", Config.LANGUAGE_FILE, e)
    return 'en'

def save_language(lang):
    """Save language preference."""
    try:
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.LANGUAGE_FILE, 'wb') as f:
            pickle.dump(lang, f)
    except Exception as e:
        LOGGER.error("Failed to save language preference to %s: %s", Config.LANGUAGE_FILE, e)

def load_app_config():
    """Load general app configuration."""
    cfg = {}
    if os.path.exists(Config.CONFIG_FILE):
        try:
            with open(Config.CONFIG_FILE, 'rb') as f:
                cfg = pickle.load(f)
        except Exception:
            pass  # Config key missing or malformed; default value used
    return cfg

def save_app_config(new_data):
    """Update general app configuration with new keys."""
    try:
        cfg = load_app_config()
        cfg.update(new_data)
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump(cfg, f)
    except Exception as e:
        LOGGER.error("Failed to save config: %s", e)

# Global language state
CURRENT_LANG = load_language()

def tr(text):
    """Translate text if current language is Hebrew."""
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text

try:
    import tantivy
except ImportError:
    raise ImportError(tr("Tantivy library missing. Please install it."))


# ==============================================================================
#  RESPONSA REGEX HELPERS
# ==============================================================================

def _make_flex_spacing_pattern(term: str) -> str:
    """Create a flex-spacing regex pattern for a term.

    Inserts \\s* between each character of the term, allowing flexible
    whitespace between characters (handles OCR/HTR word boundary errors).

    Example: "abc" -> "a\\s*b\\s*c"
    """
    if not term:
        return term
    chars = [re.escape(ch) for ch in term]
    return r'\s*'.join(chars)


def _build_wildcard_regex(component: dict) -> str:
    """Build a regex pattern for a component with wildcard type.

    Returns the regex string (NOT compiled) for the wildcard pattern.
    """
    wildcard = component.get('wildcard')
    wildcard_pattern = component.get('wildcard_pattern')

    if wildcard == 'pattern' and wildcard_pattern:
        # Character pattern: *a*b*c* -> \S*a\S*b\S*c\S*
        # Split on '*', escape each part, join with \S*
        parts = wildcard_pattern.split('*')
        escaped_parts = [re.escape(p) for p in parts]
        return r'\S*'.join(escaped_parts)

    if wildcard == 'suffix':
        # Suffix wildcard: term\S*
        # For Hebrew suffix wildcards, the trailing letter may be a sofit (final-form)
        # letter. When followed by \S*, the sofit form will never match text where
        # the stem continues with more letters (Hebrew uses normal form mid-word).
        # Replace trailing sofit with a character class matching BOTH forms.
        # Example: שלום -> שלו[םמ]\S* (matches both standalone שלום and continuation שלומ-)
        regex_terms = component.get('regex_terms', [])
        if regex_terms:
            # Sort by length descending, escape
            sorted_terms = sorted(set(regex_terms), key=len, reverse=True)
            escaped = []
            for t in sorted_terms:
                if t and t[-1] in _SOFIT_TO_NORMAL:
                    # Replace trailing sofit with char class matching both forms
                    normal = _SOFIT_TO_NORMAL[t[-1]]
                    sofit = t[-1]
                    base = re.escape(t[:-1])
                    escaped.append(f'{base}[{sofit}{normal}]' + r'\S*')
                else:
                    escaped.append(re.escape(t) + r'\S*')
            return f"({'|'.join(escaped)})"
        return ''

    if wildcard == 'prefix':
        # Prefix wildcard: \S*term
        regex_terms = component.get('regex_terms', [])
        if regex_terms:
            sorted_terms = sorted(set(regex_terms), key=len, reverse=True)
            escaped = [r'\S*' + re.escape(t) for t in sorted_terms]
            return f"({'|'.join(escaped)})"
        return ''

    return ''


def _add_bracket_variants(term: str) -> list:
    """Return bracket-adorned variants of *term* for Tantivy OR expansion.

    The hebword tokenizer keeps bracketed words as single tokens
    (e.g., ``]הנתשנ``), so a bare query needs the plausible bracket positions
    so Tantivy returns them as candidates.

    SEED-006 (invariants 1 & 2): only expand a term that has NO bracket of its
    own. A query that already contains ``[`` / ``]`` is an *exact* bracket
    search (``[סגן`` must return only ``[סגן``), so it is returned unchanged
    — never broadened to the bare/other-bracket forms. A bare ``סגן`` still
    expands to ``[סגן`` etc. so it reaches bracketed tokens on pages that
    contain it.
    """
    variants = [term]
    if not term:
        return variants
    if '[' in term or ']' in term:
        return variants  # exact bracket query — do not expand
    for v in (f'[{term}', f'{term}]', f'[{term}]', f']{term}', f'{term}['):
        if v not in variants:
            variants.append(v)
    return variants


def _query_has_brackets(query_str: str) -> bool:
    """Return True if *query_str* contains literal square brackets.

    Responsa gap operators like ``[3]`` and ``[|2]`` are NOT literal
    bracket searches — strip them before checking.
    """
    stripped = re.sub(r'\[\|?\d+\]', '', query_str)
    return '[' in stripped or ']' in stripped


def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*."""
    return text.replace('[', '').replace(']', '')


def _index_has_field(index, field_name: str) -> bool:
    """SEED-006 compat gate: True if *index*'s schema defines *field_name*.

    tantivy-py 0.25 ``Schema`` exposes no field introspection, so we probe via
    ``parse_query`` — querying a missing field raises ``ValueError`` mentioning
    'does not exist' / 'not defined in the schema'. Any other outcome (parse OK,
    or an unrelated error such as an unregistered tokenizer) is treated as
    "field present" so we never disable a working field by accident. Register
    tokenizers BEFORE calling this so a hebword field does not look absent.
    """
    if index is None:
        return False
    try:
        index.parse_query(f'{field_name}:probe', [field_name])
        return True
    except ValueError as exc:
        msg = str(exc).lower()
        if 'does not exist' in msg or 'not defined' in msg:
            return False
        return True
    except Exception:
        return True


def content_search_staleness_messages(genizah_present: bool,
                                      local_present):
    """SEED-019 #28: human-readable staleness diagnostics for the SEED-006
    ``content_search`` compat gate.

    The compat gate (:func:`_index_has_field`) lets search keep working against an
    index built before the ``content_search`` field existed — but it does so by
    *silently* degrading Hebrew punctuation/diacritic retrieval to the old
    whitespace-tokenized ``content`` field. This helper turns that silent state
    into one actionable remediation string per degraded index so the condition is
    visible (logged at open + queryable via
    :meth:`SearchEngine.index_staleness_report`).

    Args:
        genizah_present: the main GENIZAH index defines ``content_search``.
        local_present: the LOCAL side-index defines it, or ``None`` when no LOCAL
            index is open (so it contributes no message).

    Returns:
        list[str]: one message per degraded index; empty when nothing is stale.
    """
    messages = []
    if not genizah_present:
        messages.append(
            "GENIZAH index predates the content_search field (SEED-006): Hebrew "
            "punctuation/diacritic retrieval is degraded to whitespace-tokenized "
            "content-only. Rebuild the index via create_index (web: rebuild + "
            "redeploy; desktop: Settings -> Build / Rebuild Index)."
        )
    if local_present is False:
        messages.append(
            "LOCAL (My Library) index predates the content_search field "
            "(SEED-006): Hebrew punctuation/diacritic retrieval is degraded for "
            "local documents. Re-index via My Library -> Re-index All."
        )
    return messages


# Inserted between regex tokens to allow optional combining marks and apostrophe/quote variants.
# SEED-006: U+0022 (ASCII double quote) is included so the second-phase regex filter stays
# symmetric with strip_search_diacritics / COMBINING_DIACRITICALS_PATTERN (which fold U+0022).
# Without it, a clean query retrieves the stored quoted abbreviation via content_search but the
# filter then drops it (no tolerated quote between letters).
MARK_TOLERANT_INSERTER = '[\u0300-\u036F\u0022\u0027\u05F3\u05F4\u2018\u2019]*'


def make_mark_tolerant_pattern(escaped_term: str) -> str:
    """Insert optional combining mark matchers between characters of an escaped regex term.

    Takes an already-escaped regex term (from re.escape) and inserts optional
    combining mark matchers between each token. Escape sequences like \\. are
    treated as single tokens.

    Example: re.escape("abc") -> "a[\\u0300-\\u036F]*b[\\u0300-\\u036F]*c"
    """
    if not escaped_term:
        return escaped_term
    # Split escaped string into tokens: \\X (escape sequences) or single chars
    tokens = re.findall(r'\\.|.', escaped_term)
    return MARK_TOLERANT_INSERTER.join(tokens)


# ==============================================================================
#  SEARCH ENGINE
# ==============================================================================
# Reciprocal Rank Fusion constant (D-08 Codex P0): BM25 scores from two
# independent indexes are not comparable, so LOCAL + Genizah hits are fused by
# rank. k=60 is the Cormack/Clarke (2009) default.
RRF_K = 60


class SearchEngine:
    """Run searches, build queries, and provide browsing utilities."""
    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.index = None
        self.searcher = None
        # SEED-006 compat gate: whether the on-disk main / LOCAL indexes carry
        # the additive content_search field. Set at open time; gates the Stage 2
        # OR-fallback so the NEW query code never crashes against an OLD index
        # built before content_search existed (graceful degradation to content).
        self._has_content_search = False
        # FL ID index for O(1) browse lookup (built in background)
        self._fl_id_index = None  # dict: fl_digits_str -> list of (sys_id, page_idx)
        self._fl_id_index_building = False
        self._fl_id_index_lock = threading.Lock()
        self.reload_index()
        self.start_fl_id_index_build()
        # Phase 95 — open LOCAL side-index alongside main (D-14 + D-37 fallback).
        self.local_index = None            # tantivy.Index for LOCAL side-index
        self.local_searcher = None         # tantivy.Searcher snapshot
        self._local_has_content_search = False  # SEED-006 compat gate (LOCAL)
        self.local_lab_searcher = None     # tantivy.Searcher for LOCAL LAB side-index
        self._local_lab_index = None       # tantivy.Index for LOCAL LAB side-index (parse_query)
        self.local_lab_searcher_stale = False  # D-38: True when weights_hash mismatch
        self._lab_local_meta = None        # dict from .meta.json, or None
        # Phase 96 D-F5 (Codex HIGH #3): instrumentation for test spies.
        self._last_local_query_regex = None
        # Phase 96 NEW-2: per-sys_id page-list cache for get_local_browse_page.
        self._local_pages_cache = {}
        # Phase 97 R-01: weakref to MyLibraryTab for is_searchable gate.
        # Default True when no tab attached (e.g. standalone CLI / tests).
        self._my_library_tab_ref: weakref.ref | None = None
        # Phase 97 R-02: error from last atomic rebuild attempt; surfaced in UI banner.
        self._local_open_error: str | None = None
        self._open_local_searcher()

    def attach_my_library_tab(self, tab) -> None:
        """Phase 97 R-01: attach a weakref to the MyLibraryTab for is_searchable gate.

        Called from MyLibraryTab.__init__ after engine reference is available.
        The gate at _query_local_index checks is_searchable via this weakref.
        Default-True when weakref is dead (engine running standalone / tests).
        """
        self._my_library_tab_ref = weakref.ref(tab)

    def close_local_searcher(self) -> None:
        """Phase 97 R-02 LD-5: close BOTH main + LAB searcher + index handles.

        Called BEFORE os.rename in rebuild_main_index_atomic to prevent
        Windows os error 5 (Access denied) from an open reader handle.
        Closes the FULL handle graph:
          local_searcher, local_index, local_lab_searcher, _local_lab_index
        (note underscore prefix on _local_lab_index — verified at genizah_core.py:6745).
        """
        self.local_searcher = None
        self.local_index = None
        self.local_lab_searcher = None
        self._local_lab_index = None  # exact attribute name (line 6745)

    def close_local_lab_searcher(self) -> None:
        """Phase 97 R-02 LD-5 (LAB-only variant used by rebuild_lab_index_atomic)."""
        self.local_lab_searcher = None
        self._local_lab_index = None

    def _open_local_searcher(self) -> None:
        """Phase 97 R-02: open LOCAL side-index with atomic-rebuild recovery.

        Replaces D-37 silent None fallback per Codex HIGH #3 and ADVICE LD-4.
        On Index.open exception OR schema-mismatch, attempts atomic rebuild from
        cached_text via LocalIndexer. On rebuild failure, logs + sets both to None
        AND records error in self._local_open_error for UI banner consumption.
        Does NOT silently create a fresh empty index on corruption.
        """
        self.local_index = None
        self.local_searcher = None
        self._local_open_error = None

        if not os.path.isdir(Config.LOCAL_INDEX_DIR):
            LOGGER.info(
                "LOCAL side-index dir not present (no scan yet?): %s",
                Config.LOCAL_INDEX_DIR,
            )
            return

        from shared.local_indexer import (
            build_local_schema,
            LocalIndexer,
            migrate_legacy_local_db,
            _compute_schema_marker,
            _read_schema_marker,
        )

        # Check schema marker
        expected_marker = _compute_schema_marker(build_local_schema)
        actual_marker = _read_schema_marker(Config.LOCAL_INDEX_DIR)
        schema_mismatch = (actual_marker != expected_marker)

        try:
            schema = build_local_schema()
            local_index = tantivy.Index(schema, path=Config.LOCAL_INDEX_DIR)
            register_search_tokenizers(local_index)  # SEED-006: hebword + builtins
            if schema_mismatch:
                raise RuntimeError(
                    f"Schema marker mismatch (actual={actual_marker!r}, "
                    f"expected={expected_marker!r})"
                )
            self.local_index = local_index
            self.local_searcher = local_index.searcher()
            self._local_has_content_search = _index_has_field(local_index, "content_search")
            self._warn_if_local_index_stale()
            LOGGER.info("LOCAL side-index opened: %s", Config.LOCAL_INDEX_DIR)
        except Exception as open_exc:
            LOGGER.warning(
                "LOCAL index open/schema-check failed: %r — attempting atomic rebuild",
                open_exc,
            )
            try:
                import gc as _gc
                # Codex MED #3: drop the probe Tantivy handle opened above BEFORE
                # constructing the indexer. The LocalIndexer constructor performs the
                # atomic rebuild from cached_text on the mismatch/open-failure it
                # re-detects (which renames LOCAL_INDEX_DIR); a live read handle here
                # would block os.rename on Windows.
                local_index = None
                _gc.collect()
                # SEED-006 P1: DB must live OUTSIDE the atomically-swapped LocalIndex
                # dir; migrate any legacy in-dir DB out before constructing.
                db_path = migrate_legacy_local_db(Config.LOCAL_INDEX_DIR)
                # Codex MED #3: the constructor already does the SINGLE atomic rebuild
                # on the same mismatch — do NOT call rebuild_main_index_atomic() again
                # (the prior code rebuilt twice: once in __init__, once explicitly).
                indexer = LocalIndexer(
                    index_dir=Config.LOCAL_INDEX_DIR,
                    lab_index_dir=Config.LOCAL_LAB_INDEX_DIR,
                    db_path=db_path,
                )
                # D-01: close the temp indexer's writer + index handles before
                # opening the live searcher below, so its writer lock is released
                # (a still-live writer would block writer acquisition on the dir).
                try:
                    indexer._close_internal_writer_index()
                except Exception:
                    LOGGER.exception("temp indexer close failed (continuing)")
                schema2 = build_local_schema()
                local_index2 = tantivy.Index(schema2, path=Config.LOCAL_INDEX_DIR)
                register_search_tokenizers(local_index2)  # SEED-006
                self.local_index = local_index2
                self.local_searcher = local_index2.searcher()
                self._local_has_content_search = _index_has_field(local_index2, "content_search")
                self._warn_if_local_index_stale()
                LOGGER.info(
                    "LOCAL side-index: atomic rebuild succeeded, reopened: %s",
                    Config.LOCAL_INDEX_DIR,
                )
            except Exception as rebuild_exc:
                LOGGER.error("LOCAL atomic rebuild failed: %r", rebuild_exc)
                self._local_open_error = str(rebuild_exc)
                self.local_index = None
                self.local_searcher = None

    def reload_local_indexes(self) -> None:
        """HIGH-1 review fix: reopen LOCAL Tantivy searchers (main + LAB) so newly
        committed docs become visible in the live session.

        Called by MyLibraryTab (Plan 07) AFTER every refresh / delete / rebuild /
        recovery commit. Idempotent + defensive: on any open failure, the searcher
        falls back to None (D-37 semantics).
        """
        # Phase 96 NEW-2: invalidate page-list cache so get_local_browse_page
        # sees fresh docs after a rebuild/refresh.
        self._local_pages_cache = {}
        self._open_local_searcher()
        self.reload_local_lab_index()

    def reload_local_lab_index(self) -> None:
        """HIGH-1 review fix (LAB-only narrow reload). Reopens self.local_lab_searcher
        and re-reads the .meta.json staleness sentinel.

        Plan 06: also reads .meta.json for D-38 weights_hash freshness check.
        """
        self.local_lab_searcher = None
        self._lab_local_meta = None
        try:
            if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
                from shared.local_indexer import build_local_lab_schema, LocalIndexer
                schema = build_local_lab_schema()
                local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
                # Phase 110 UAT BLOCKER (mirror of LabEngine.reload_local_lab_index):
                # the LOCAL LAB schema's fingerprint / fingerprint_dyn / content
                # fields use tokenizer_name="simple" (text_ngram uses "whitespace").
                # Without registering them on this freshly-opened Index, any
                # parse_query against those fields raises
                # ValueError('The tokenizer "simple" ... is unknown'). Register
                # before creating the searcher / running any query.
                for _tk_name, _tk in (
                    ("simple", tantivy.Tokenizer.simple()),
                    ("whitespace", tantivy.Tokenizer.whitespace()),
                ):
                    try:
                        local_lab_index.register_tokenizer(
                            _tk_name, tantivy.TextAnalyzerBuilder(_tk).build()
                        )
                    except Exception:
                        pass  # May fail on reopen — non-fatal, like _ensure_lab_tokenizers
                self.local_lab_searcher = local_lab_index.searcher()
                # Phase 95 D-38: read .meta.json for weights_hash freshness check
                self._lab_local_meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
                self._local_lab_index = local_lab_index  # kept for parse_query
                LOGGER.info(
                    "HIGH-1 reload: LOCAL LAB side-index reopened: %s",
                    Config.LOCAL_LAB_INDEX_DIR,
                )
            else:
                LOGGER.info(
                    "HIGH-1 reload: LOCAL LAB side-index dir absent; searcher=None"
                )
        except Exception as e:
            LOGGER.warning(
                "HIGH-1 reload: LOCAL LAB side-index unavailable: %r", e
            )
            self.local_lab_searcher = None
            self._lab_local_meta = None

    def _current_lab_weights_hash(self) -> str:
        """Compute hash of current LAB weights for D-38 staleness check.

        CR-01 FIX: ``dynamic_rank_map`` and ``settings`` are NOT defined on
        :class:`SearchEngine` — they live on :class:`LabEngine`.  Until the
        two classes share a registry, ``getattr`` with safe defaults is the
        smallest correctness fix:

          * On a real :class:`SearchEngine` (no LAB attrs) the function returns
            a deterministic hash of "no weights" → never crashes.
          * On a :class:`LabEngine` instance (or a SearchEngine that has been
            wired to a LabEngine and forwards attribute access), the real LAB
            weights are picked up via ``getattr`` and the hash matches what
            ``build_lab_side_index`` writes into ``.meta.json``.
        """
        # Phase 110 RF-4: standard composition uses the LabEngine's hash;
        # SearchEngine.dynamic_rank_map is always None → would never match
        # .meta.json otherwise. The app injects
        # searcher._lab_weights_hash_override = lab_engine._current_lab_weights_hash()
        # (Plan 03 — at GenizahGUI init + after every LOCAL LAB rebuild) so the
        # standard-composition freshness check compares against the SAME hash the
        # index was built with, letting 'all'-scope LOCAL LAB hits through.
        _override = getattr(self, '_lab_weights_hash_override', None)
        if _override:
            return _override
        import hashlib as _hashlib
        import json as _json
        # CR-01: use getattr defaults so missing attrs don't raise.
        dyn_map = getattr(self, "dynamic_rank_map", None)
        settings = getattr(self, "settings", None)
        weights_dict = {
            "dynamic_rank_map": dyn_map if dyn_map else None,
            "use_dynamic_weights": (
                getattr(settings, "use_dynamic_weights", False)
                if settings is not None
                else False
            ),
        }
        return _hashlib.sha256(
            _json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _check_local_lab_freshness(self) -> bool:
        """Return True if LOCAL LAB index is fresh; False if stale or missing.

        Side effect: sets self.local_lab_searcher_stale.
        D-38: compares current LAB weights_hash to value stored in .meta.json.
        """
        if self.local_lab_searcher is None:
            return False
        meta = self._lab_local_meta
        if not meta:
            self.local_lab_searcher_stale = True
            LOGGER.info("LOCAL LAB index has no .meta.json — treating as stale")
            return False
        current_hash = self._current_lab_weights_hash()
        if meta.get("weights_hash") != current_hash:
            self.local_lab_searcher_stale = True
            LOGGER.info(
                "LOCAL LAB index stale (weights changed); banner surface required"
            )
            return False
        self.local_lab_searcher_stale = False
        return True

    def rebuild_local_lab_index(self, local_indexer, lab_engine=None) -> None:
        """Trigger LOCAL LAB rebuild via LocalIndexer, passing fingerprint helpers
        as callbacks (W5 — Option C LOCKED). Called from MyLibraryTab Refresh
        (Plan 07) and Tools→Rebuild LAB (per D-38).

        The three bound-method callbacks (_compute_fingerprint_dyn,
        _compute_fingerprint_static, _normalize_text) are thin wrappers around
        the existing text_to_fingerprint / lab_index_normalize helpers in this class.

        The LAB weights (``dynamic_rank_map`` + ``settings``) live on
        :class:`LabEngine`, NOT on :class:`SearchEngine` (same split CR-01 fixed
        for ``_current_lab_weights_hash``). The caller passes the live LabEngine
        as ``lab_engine`` so the weights — and therefore the build-time
        fingerprints and the ``weights_hash`` written into ``.meta.json`` —
        match what the LabEngine freshness check recomputes. Falling back to
        ``self`` with ``getattr`` defaults keeps a plain SearchEngine from
        raising ``AttributeError``; sourcing weights from ``self`` (None map)
        would make the freshness check see a different hash and judge the index
        perpetually stale.
        """
        weights_source = lab_engine if lab_engine is not None else self
        dyn_map = getattr(weights_source, "dynamic_rank_map", None)
        settings = getattr(weights_source, "settings", None)
        # Normalize identically to LabEngine._current_lab_weights_hash so the
        # weights_hash written here matches the freshness check exactly.
        lab_weights = {
            "dynamic_rank_map": dyn_map if dyn_map else None,
            "use_dynamic_weights": (
                getattr(settings, "use_dynamic_weights", False)
                if settings is not None else False
            ),
        }
        local_indexer.build_lab_side_index(
            lab_weights=lab_weights,
            fingerprint_dyn_fn=self._compute_fingerprint_dyn,
            fingerprint_static_fn=self._compute_fingerprint_static,
            normalize_text_fn=self._normalize_text,
            lab_schema_version=1,
            dynamic_rank_map=dyn_map,
        )
        # Reload so newly built index is visible in live session
        self.reload_local_lab_index()

    def _compute_fingerprint_dyn(self, content: str, dynamic_rank_map) -> str:
        """Compute fingerprint_dyn for a content string using the given rank map.
        W5 Option C callback — wraps text_to_fingerprint with dynamic weights."""
        return text_to_fingerprint(content, freq_map=dynamic_rank_map)

    def _compute_fingerprint_static(self, content: str) -> str:
        """Compute static fingerprint for a content string using HEBREW_FREQ.
        W5 Option C callback — wraps text_to_fingerprint with static weights."""
        return text_to_fingerprint(content, freq_map=HEBREW_FREQ)

    def _normalize_text(self, content: str) -> str:
        """Normalize content for the text_normalized field (W5 Option C callback).

        Wraps ``LabEngine.lab_index_normalize`` — a pure ``@staticmethod`` that
        lives on :class:`LabEngine`, NOT on :class:`SearchEngine`. The previous
        ``self.lab_index_normalize`` raised ``AttributeError`` whenever this
        callback ran on a SearchEngine, which aborted every LOCAL LAB rebuild at
        ``build_lab_side_index``'s pre-flight probe — so ``.meta.json`` was never
        written, the index stayed perpetually "stale", and a doomed rebuild was
        re-attempted on every startup/refresh (console churn + wasted reloads)."""
        return LabEngine.lab_index_normalize(content)

    def _query_local_index(self, query_str: str, mode: str, gap: int,
                           limit=None, regex=None, tantivy_query_str=None):
        """Query the LOCAL side-index. Returns [] if local_searcher is None (D-37).

        MEDIUM-1 note: this uses a simplified parse_query (not the full Responsa
        expansion pipeline used by the main searcher). The divergence is documented
        as a deferred follow-up — see plan 95-05 <deferred> block.

        Phase 96 D-F5: accept an optional `regex` parameter so each hit dict
        can carry `highlight_pattern` + asterisk-marker `snippet`, matching
        the Genizah hit shape. When `regex` is None (legacy callers), hits
        are returned with raw snippets (back-compat).

        REVISION 2026-05-24 — D-04.1 LOAD-BEARING (Codex HIGH #2): when
        `regex` is provided AND `_build_local_result_dict` returns None (regex
        didn't match), the candidate is SKIPPED. Visible LOCAL hits all satisfy
        the regex — same algebra as Genizah hits (genizah_core.py:8335-8345
        `if hl_c:` filter-out pattern).

        REVISION 2026-05-24 — Codex HIGH #3: records the last-passed regex
        object on `self._last_local_query_regex` so test spies can assert the
        merge call site genuinely threaded regex (rather than passing None
        silently). Cleared on each call entry to avoid stale state.
        """
        # Codex HIGH #3 instrumentation — record what we got passed.
        self._last_local_query_regex = regex

        # Phase 97 R-01 is_searchable gate — FIRST executable check.
        # Returns [] while MyLibraryTab recovery modal is unresolved.
        # Default-True if weakref is dead (engine running standalone / CLI / tests).
        tab = self._my_library_tab_ref() if self._my_library_tab_ref is not None else None
        if tab is not None and not getattr(tab, "is_searchable", True):
            return []

        if self.local_searcher is None or self.local_index is None:
            return []
        try:
            # Use self.local_index (kept alongside local_searcher) for parse_query.
            # tantivy.Searcher has no .index attribute — Index must be stored separately.
            # MEDIUM-1 deferred: full query builder (variants/Responsa) not extracted yet.
            # SEED-006 Stage 2: fan field-less terms across content_search too (the
            # diacritic-folded field) so צמאן / צ'מאן reach צ̇מאן. query_str is
            # already diacritic-stripped by both callers. Gated on the compat flag
            # so an OLD LOCAL index (no content_search) keeps working.
            _fields = ["content", "content_head", "content_tail"]
            if getattr(self, "_local_has_content_search", False):
                _fields.append("content_search")
            # Phase 95-05 follow-up (Responsa-over-LOCAL): when the caller supplies
            # a pre-expanded candidate query (the SAME operator-expanded query the
            # main index uses for Responsa #/*/%/(a/b)), parse THAT — the simplified
            # parse_query below strips operator metacharacters and returns nothing,
            # which is why LOCAL Responsa came back empty. Defensive fall-back to the
            # simplified path if the pre-built query somehow fails to parse.
            tantivy_q = None
            if tantivy_query_str:
                try:
                    tantivy_q = self.local_index.parse_query(tantivy_query_str, _fields)
                except (ValueError, Exception):
                    LOGGER.warning(
                        "LOCAL pre-built query failed to parse; falling back to simplified: %.200s",
                        tantivy_query_str,
                    )
                    tantivy_q = None
            if tantivy_q is None:
                try:
                    tantivy_q = self.local_index.parse_query(query_str, _fields)
                except (ValueError, Exception):
                    # v7.16: Tantivy's query parser chokes on syntax metacharacters,
                    # which appear naturally in Hebrew abbreviations — the geresh in
                    # אמ' and the gershayim in רמב"ם both raise "Syntax Error" and the
                    # whole LOCAL search returned 0 results. Strip the metacharacters
                    # (`'"` + structural chars) so the term query parses; the precise
                    # match is still enforced by the regex filter below.
                    _safe = re.sub(r"[+\-&|!(){}\[\]^\"~*?:\\/']", " ", query_str).strip()
                    if not _safe:
                        return []
                    tantivy_q = self.local_index.parse_query(_safe, _fields)
            search_limit = limit or Config.SEARCH_LIMIT
            res_obj = self.local_searcher.search(tantivy_q, search_limit)
            hits = res_obj.hits if hasattr(res_obj, "hits") else res_obj
            pattern_str = regex.pattern if regex is not None else ""
            results = []
            for score, doc_address in hits:
                doc = self.local_searcher.doc(doc_address)
                hit = self._build_local_result_dict(
                    doc, score, regex=regex, pattern_str=pattern_str
                )
                # D-04.1 filter-out: skip candidates whose regex didn't match.
                # _build_local_result_dict returns None for those.
                if hit is None:
                    continue
                results.append(hit)
            return results
        except Exception as e:
            LOGGER.warning("LOCAL index query failed: %r", e)
            return []

    def _build_local_result_dict(self, doc, score, regex=None, pattern_str=None):
        """Construct a result row from a LOCAL Tantivy doc per D-34 shape.

        Phase 96 D-F5: when `regex` is provided, populate snippet + raw_file_hl
        + highlight_pattern so the LOCAL hit shape matches Genizah hits and
        downstream UI (format_snippet, ResultDialog highlight branch) works
        without per-source branching. Mirrors genizah_core.py:8335-8345.

        REVISION 2026-05-24 — D-04.1 LOAD-BEARING (per user CONTEXT update +
        Codex HIGH #2): when `regex` is provided AND the regex does NOT match
        `content`, this function returns **None** to signal the caller to FILTER
        OUT this candidate. This matches the Genizah two-phase model — Tantivy
        candidates that fail the regex are silently dropped from the result list.
        No fallback display of unhighlighted content.

        Back-compat: when `regex` is None (legacy callers), the function ALWAYS
        returns a dict (old shape — snippet = content[:200]).

        Returns:
            - dict (the hit) when regex matches OR regex is None
            - None when regex is provided AND regex does NOT match content
              → caller MUST skip this candidate (D-04.1)
        """
        unique_id = doc.get_first("unique_id") or ""
        full_header = doc.get_first("full_header") or ""
        content = doc.get_first("content") or ""
        shelfmark = doc.get_first("shelfmark") or ""
        # Phase 97 D-NEW-5: chunk_locator — location string set by extractor
        # (e.g. "p. 3" for PDF, "paragraphs 1-20" for DOCX, "§ Introduction" for HTML).
        chunk_locator = doc.get_first("chunk_locator") or ""
        # Parse sys_id + p_num from full_header (format: {sys_id}_LOCAL_P{page}_F{file_id})
        sys_id = ""
        p_num = "1"
        if full_header:
            parts = full_header.split("_LOCAL_P")
            if len(parts) == 2:
                sys_id = parts[0]
                # p_num is before the _F suffix
                p_part = parts[1].split("_F")[0]
                p_num = p_part

        # Phase 96 D-F5: compute snippet via self.highlight when regex provided.
        # D-04.1: filter-out signal when regex doesn't match (return None).
        if regex is not None:
            hl_c = self.highlight(content, regex, for_file=False)
            if not hl_c:
                # D-04.1: Tantivy matched but regex didn't.
                # SILENTLY DROP this candidate by returning None.
                # _query_local_index will skip it.
                return None
            hl_f = self.highlight(content, regex, for_file=True)
            snippet = hl_c
            raw_file_hl = hl_f or ""
            effective_pattern = pattern_str or regex.pattern
        else:
            # Back-compat path (no regex passed by caller — old behaviour).
            # Always returns a dict; no filter-out.
            snippet = content[:200] if content else ""
            raw_file_hl = ""
            effective_pattern = ""

        return {
            "uid": unique_id,
            "full_text": content,
            "snippet": snippet,
            "raw_file_hl": raw_file_hl,
            "highlight_pattern": effective_pattern,
            "sys_id": sys_id,
            "p_num": p_num,
            # Phase 96 fix-4: populate 'img' with p_num (page/chunk number).
            # Genizah hits use 'img' for the folio page number; LOCAL hits must
            # mirror this so ResultDialog.load_result and _open_local_browse can
            # both open at the correct page (not always page 1).
            "img": p_num,
            "score": float(score),
            # Phase 97 D-NEW-5: chunk_locator for human-readable position in source file.
            "chunk_locator": chunk_locator,
            "display": {
                "id": sys_id,
                "source": "LOCAL",
                "library_code": "LOCAL",
                "shelfmark": shelfmark,
                # Phase 96 fix-3 (Img column): search results render the Img
                # column from meta.get('img') where meta = res['display'].
                # Genizah hits populate 'img' inside the display dict via
                # get_display_data().  LOCAL hits must mirror this so the Img
                # column shows the page/chunk number instead of a blank cell.
                "img": p_num,
            },
            "full_header": full_header,
        }

    def _rrf_merge(self, genizah_hits, local_hits, k: int = RRF_K, limit=None):
        """Reciprocal Rank Fusion merger (D-08 Codex P0). BM25 scores from two
        independent indexes are NOT comparable; RRF fuses by rank (Cormack/Clarke 2009).

        Tie-break: Genizah first when LOCAL and Genizah hits have identical scores.
        The tie-break is content-driven (display.source != 'LOCAL' → Genizah) so it
        is ORDER-INDEPENDENT — passing (local, genizah) vs (genizah, local) gives the
        same result (W7 requirement).
        """
        rrf: dict = {}
        for rank, hit in enumerate(genizah_hits, start=1):
            uid = hit["uid"]
            rrf.setdefault(uid, {"hit": hit, "score": 0.0})
            rrf[uid]["score"] += 1.0 / (k + rank)
        for rank, hit in enumerate(local_hits, start=1):
            uid = hit["uid"]
            rrf.setdefault(uid, {"hit": hit, "score": 0.0})
            rrf[uid]["score"] += 1.0 / (k + rank)
        # Tie-break: Genizah (non-LOCAL) first at equal score.
        # display.source == 'LOCAL' → local (lower priority on tie).
        # Any other source (V0.8, V0.7) → Genizah (higher priority on tie).
        # True > False → non-LOCAL sorts higher at equal score (reverse=True).
        fused = sorted(
            rrf.values(),
            key=lambda r: (r["score"], r["hit"].get("display", {}).get("source") != "LOCAL"),
            reverse=True,
        )
        out = [r["hit"] for r in fused]
        return out[:limit] if limit else out

    # ------------------------------------------------------------------
    #  FL ID Index (background build for O(1) browse-by-FL lookup)
    # ------------------------------------------------------------------
    def _build_fl_id_index(self):
        """Build FL ID -> (sys_id, page_idx) index from browse_map. Called in background thread."""
        browse_map = self._load_browse_map()
        if not browse_map:
            return
        index = {}
        for sys_id, pages in browse_map.items():
            for idx, page in enumerate(pages):
                parsed = self.meta_mgr.parse_full_id_components(page.get('full_header', ''))
                fl_id = parsed.get('fl_id')
                if fl_id:
                    fl_digits = re.sub(r"\D", "", str(fl_id))
                    if fl_digits:
                        if fl_digits not in index:
                            index[fl_digits] = []
                        index[fl_digits].append((sys_id, idx))
        with self._fl_id_index_lock:
            self._fl_id_index = index
        LOGGER.info("FL ID index built: %d entries", len(index))

    def start_fl_id_index_build(self):
        """Start building FL ID index in background. Non-blocking."""
        if self._fl_id_index is not None or self._fl_id_index_building:
            return
        self._fl_id_index_building = True
        t = threading.Thread(target=self._build_fl_id_index_thread, daemon=True)
        t.start()

    def _build_fl_id_index_thread(self):
        try:
            self._build_fl_id_index()
        except Exception as e:
            LOGGER.warning("Failed to build FL ID index: %s", e)
        finally:
            self._fl_id_index_building = False

    @staticmethod
    def format_snippet(text, style='html_class'):
        """
        Format snippet with highlighted matches, safely escaping HTML.
        style: 'html_class' (Web: span class="highlight-match") or 'html_inline' (Desktop: span style="color:...")
        """
        if not text:
            return ""

        # First escape HTML to prevent XSS
        escaped = html.escape(text)

        # Style ‖ line-break indicators
        escaped = escaped.replace('\u2016', '<span class="line-break-sep">\u2016</span>' if style == 'html_class'
                                  else '<span style="color:#888; font-weight:bold;">\u2016</span>')

        # Convert *word* to highlighted span (after escaping, markers are safe)
        if style == 'html_class':
            return re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', escaped)
        else:
            # Desktop style (inline) - Red/Bold
            return re.sub(r'\*(.*?)\*', r'<span style="color:#ff0000; font-weight:bold;">\1</span>', escaped)

    def close_index(self):
        """Release Tantivy index and searcher to unlock files (required before rebuild on Windows)."""
        self.searcher = None
        self.index = None
        import gc
        gc.collect()

    def reload_index(self):
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        if os.path.exists(db_path):
            try:
                self.index = tantivy.Index.open(db_path)
                # SEED-006: register hebword (content field) BEFORE searcher use,
                # then detect content_search for the Stage 2 compat gate.
                register_search_tokenizers(self.index)
                self._has_content_search = _index_has_field(self.index, "content_search")
                if not self._has_content_search:
                    # SEED-006 M3 + SEED-019 #28: the GENIZAH main index has no schema
                    # marker, so nothing else detects that it predates this fix.
                    # Surface it loudly — the punctuation/diacritic retrieval fix is
                    # INERT (degrades to whitespace-tokenized content-only) until the
                    # index is rebuilt. Message centralized in
                    # content_search_staleness_messages so this log and the queryable
                    # index_staleness_report() never drift apart.
                    for _msg in content_search_staleness_messages(False, None):
                        LOGGER.warning("Stale index [%s]: %s", db_path, _msg)
                self.searcher = self.index.searcher()
                return True
            except Exception as e:
                LOGGER.error("Failed to reload Tantivy index from %s: %s", db_path, e)
        return False

    def index_staleness_report(self) -> dict:
        """SEED-019 #28: queryable verdict on the SEED-006 ``content_search`` compat
        gate, so a degraded (stale) index is visible beyond the one-shot WARNING
        emitted at index open.

        The gate keeps search working against an index that predates the
        ``content_search`` field, but silently degrades Hebrew
        punctuation/diacritic retrieval. This report exposes that state for ops /
        health surfaces (and the desktop rebuild prompt) instead of leaving it to a
        single startup log line.

        Returns:
            dict: ``{'genizah_content_search': bool,
                     'local_content_search': bool | None,   # None = no LOCAL index
                     'stale': bool,                          # any index degraded
                     'messages': list[str]}``               # remediation per index
        """
        genizah_present = bool(getattr(self, "_has_content_search", False))
        local_present = (
            bool(getattr(self, "_local_has_content_search", False))
            if getattr(self, "local_index", None) is not None
            else None
        )
        messages = content_search_staleness_messages(genizah_present, local_present)
        return {
            "genizah_content_search": genizah_present,
            "local_content_search": local_present,
            "stale": bool(messages),
            "messages": messages,
        }

    def _warn_if_local_index_stale(self) -> None:
        """SEED-019 #28: log a remediation warning when a freshly-opened LOCAL
        side-index predates the SEED-006 ``content_search`` field. LOCAL normally
        self-heals via its schema marker (a mismatch triggers an atomic rebuild),
        so this is a defensive catch for the rare case where it opened degraded —
        parity with the GENIZAH warning in :meth:`reload_index`.
        """
        if getattr(self, "_local_has_content_search", True) is False:
            for _msg in content_search_staleness_messages(True, False):
                LOGGER.warning("Stale LOCAL index: %s", _msg)

    # Class-level browse_map shared across all instances (loaded once)
    _shared_browse_map = None
    _browse_map_lock = threading.Lock()

    def _load_browse_map(self):
        """Load the browse map, deduplicate it, and persist corrections if needed.

        Uses class-level cache shared across all SearchEngine instances to avoid
        redundant file I/O, repair scans, and pickle write race conditions.
        """
        if SearchEngine._shared_browse_map is not None:
            return SearchEngine._shared_browse_map

        if hasattr(self, '_browse_map_cache') and self._browse_map_cache is not None:
            return self._browse_map_cache

        if not os.path.exists(Config.BROWSE_MAP):
            return {}

        with SearchEngine._browse_map_lock:
            # Double-check after acquiring lock (another thread may have loaded)
            if SearchEngine._shared_browse_map is not None:
                return SearchEngine._shared_browse_map

            with open(Config.BROWSE_MAP, 'rb') as f:
                raw_map = pickle.load(f)

            cleaned_map, changed = dedupe_browse_map(raw_map)
            if changed:
                try:
                    with open(Config.BROWSE_MAP, 'wb') as f:
                        pickle.dump(cleaned_map, f)
                except Exception as e:
                    LOGGER.warning("Failed to write deduplicated browse map to %s: %s", Config.BROWSE_MAP, e)

            SearchEngine._shared_browse_map = cleaned_map
            self._browse_map_cache = cleaned_map
            return cleaned_map

    def _get_or_compute_variants(self, terms, mode):
        """Pre-compute variants at the larger limit for each search term.

        This ensures that when build_tantivy_query requests variants with
        limit=200 and build_regex_pattern later requests limit=8000 for the
        same term+mode, the second call is served from the superset cache
        (via slicing) instead of recomputing from scratch.
        """
        if not self.var_mgr or not terms:
            return
        max_limit = Config.REGEX_VARIANTS_LIMIT  # 8000
        regex_mode = 'variants_maximum' if mode == 'fuzzy' else mode
        for term in terms:
            if term.upper() in ['AND', 'OR', 'NOT', '(', ')']:
                continue
            # Pre-compute at the larger limit; Tantivy phase will slice from cache
            self.var_mgr.get_variants(term, mode, limit=max_limit)
            if regex_mode != mode:
                self.var_mgr.get_variants(term, regex_mode, limit=max_limit)

    def _build_local_responsa_query_and_regex(self, query_str, mode, gap, responsa_options):
        """Build a Tantivy query string + filter regex for a Responsa query, for
        use against the LOCAL My-Library index.

        Mirrors the component expansion the main (genizah) path runs inline in
        execute_search — grammatical ``#prefix``/``suffix#``, ``*`` wildcards,
        ``%`` plene/defective, ``(a/b)`` alternation, Judeo-Arabic + spelling-
        variant expansion — but is self-contained so it never disturbs the
        load-bearing main search path. It reuses the SAME index-agnostic
        ``build_tantivy_query`` / ``build_regex_pattern`` the main path uses, so
        the returned query string drops straight onto ``local_index.parse_query``.

        Closes the Phase-95 MEDIUM-1 deferral (95-05): LOCAL search used a
        simplified ``parse_query`` that stripped operator metacharacters, so
        Responsa queries returned nothing. The line-break (``|``) operator is NOT
        handled here — it runs through the separate ``_execute_line_break_search``
        (main index only); callers get ``(None, None)`` and fall back.

        Parity with the main path is pinned by
        ``tests/test_local_reload_after_refresh.py::test_query_semantics_*``.

        Returns ``(t_query_str, regex)``, or ``(None, None)`` when the query can't
        be expressed for LOCAL (no components, line-break query, or no positive
        components) so the caller falls back to the simplified path.
        """
        components = parse_responsa_query(query_str)
        if not components:
            return None, None
        # Line-break ('|') is a separate, main-index-only path. Bail to fallback.
        line_groups, _ = _parse_line_break_query(query_str)
        if line_groups is not None:
            return None, None
        per_pair_gaps = extract_per_pair_gaps(query_str)

        variants_on = responsa_options.get('variants', False)
        ja_on = responsa_options.get('ja', False)
        flex_spacing = responsa_options.get('flex_spacing', False)
        variant_mode = responsa_options.get('variant_mode', 'exact')

        # Rewrite *word* -> #word# (both-side wildcard = prefix+suffix expansion;
        # true substring search isn't expressible in Tantivy). Mirrors main path.
        for comp in components:
            if (comp.wildcard == 'pattern' and comp.wildcard_pattern
                    and comp.wildcard_pattern.startswith('*')
                    and comp.wildcard_pattern.endswith('*')
                    and comp.wildcard_pattern.count('*') == 2):
                stem = comp.wildcard_pattern.strip('*')
                if stem:
                    comp.words = [stem]
                    comp.wildcard = None
                    comp.wildcard_pattern = None
                    comp.grammatical_prefixes = True
                    comp.grammatical_suffixes = True

        # Explosion guard (same as main path) so a huge LOCAL expansion can't hang.
        _components, _guard_warning, actual_opts = _apply_explosion_guard(
            components, variants_on=variants_on, ja_on=ja_on,
            var_mgr=self.var_mgr, variant_mode=variant_mode,
        )
        if _guard_warning:
            variants_on = actual_opts['variants_on']
            ja_on = actual_opts['ja_on']
            variant_mode = actual_opts['variant_mode']

        positive_components = [c for c in components if not c.negated]
        if not positive_components:
            return None, None

        component_dicts = []
        for comp in positive_components:
            expanded_words = list(comp.words)
            if comp.plene_defective:
                pe = []
                for w in expanded_words:
                    pe.extend(expand_plene_defective(w))
                expanded_words = list(dict.fromkeys(pe))
            if comp.grammatical_prefixes:
                pe = []
                for w in expanded_words:
                    pe.extend(expand_grammatical_prefixes(w))
                expanded_words = list(dict.fromkeys(pe))
            if comp.grammatical_suffixes:
                se = []
                for w in expanded_words:
                    se.extend(expand_grammatical_suffixes(w))
                expanded_words = list(dict.fromkeys(se))
            if ja_on:
                je = []
                for w in expanded_words:
                    je.extend(expand_judeo_arabic(w))
                expanded_words = list(dict.fromkeys(je))
            if variants_on and self.var_mgr:
                ve = []
                for w in expanded_words:
                    try:
                        ve.extend(self.var_mgr.get_variants(w, variant_mode, limit=200))
                    except Exception:
                        ve.append(w)
                expanded_words = list(dict.fromkeys(ve))
            flex_patterns = []
            if flex_spacing:
                for w in comp.words:
                    flex_patterns.append(_make_flex_spacing_pattern(w))
            component_dicts.append({
                'tantivy_terms': expanded_words,
                'regex_terms': expanded_words,
                'original_words': comp.words,
                'wildcard': comp.wildcard,
                'wildcard_pattern': comp.wildcard_pattern,
                'flex_patterns': flex_patterns,
                'inline_pattern': comp.inline_pattern,
            })

        if not component_dicts:
            return None, None
        t_query_str = self.build_tantivy_query(
            terms=None, mode=mode,
            responsa_components=component_dicts, responsa_options=responsa_options,
        )
        regex = self.build_regex_pattern(
            terms=None, mode=mode, max_gap=gap,
            responsa_components=component_dicts, responsa_options=responsa_options,
            per_pair_gaps=per_pair_gaps,
        )
        return t_query_str, regex

    def build_tantivy_query(self, terms, mode, responsa_components=None, responsa_options=None,
                            content_search_field=None):
        # SEED-006 Stage 2: when *content_search_field* is supplied (only by the
        # plain word-search + composition retrieval sites, NOT position /
        # line-break / responsa), each term gets an extra lower-weighted
        # ``content_search:"<diacritic-folded>"`` OR-clause so צמאן / צ'מאן reach
        # the corpus form צ̇מאן. The exact-content ^5 boost is preserved, so a
        # doc carrying the original form still ranks above a fold-only match.
        # --- Responsa branch ---
        if responsa_components is not None:
            flex_spacing = responsa_options.get('flex_spacing', False) if responsa_options else False
            parts = []
            for comp in responsa_components:
                tantivy_terms = comp.get('tantivy_terms', [])
                original_words = set(comp.get('original_words', []))
                if not tantivy_terms:
                    continue

                clean_vars = []
                seen = set()
                for t in tantivy_terms:
                    t_clean = t.replace('"', '')
                    if not t_clean or t_clean in seen:
                        continue
                    seen.add(t_clean)

                    if t_clean in original_words:
                        # Original / exact term gets highest boost
                        clean_vars.append(f'"{t_clean}"^5')
                    elif any(len(t_clean) != len(ow) for ow in original_words):
                        # Different length from any original -> medium boost
                        clean_vars.append(f'"{t_clean}"^3')
                    else:
                        clean_vars.append(f'"{t_clean}"')

                # For suffix wildcards, expand with grammatical suffixes so Tantivy
                # finds documents containing derived forms (e.g., שלומו for שלום*).
                # Also add sofit-converted stem as fallback.
                wildcard = comp.get('wildcard')
                if wildcard == 'suffix':
                    for w in comp.get('original_words', []):
                        for sfx in expand_grammatical_suffixes(w):
                            if sfx not in seen:
                                seen.add(sfx)
                                clean_vars.append(f'"{sfx}"')
                        if w and w[-1] in _SOFIT_TO_NORMAL:
                            converted = w[:-1] + _SOFIT_TO_NORMAL[w[-1]]
                            if converted not in seen:
                                seen.add(converted)
                                clean_vars.append(f'"{converted}"')
                elif wildcard == 'prefix':
                    for w in comp.get('original_words', []):
                        for pfx in expand_grammatical_prefixes(w):
                            if pfx not in seen:
                                seen.add(pfx)
                                clean_vars.append(f'"{pfx}"')

                # Bracket variants: add bracket-adorned forms of each
                # original word so Tantivy returns bracketed tokens.
                for w in comp.get('original_words', []):
                    for bv in _add_bracket_variants(w):
                        if bv != w and bv not in seen:
                            seen.add(bv)
                            clean_vars.append(f'"{bv}"')

                # Flex spacing: add split alternatives so Tantivy finds
                # documents where a word appears with spaces (e.g., "בן דוד"
                # for query "בןדוד"). Each split produces an AND pair;
                # regex phase then verifies adjacency.
                flex_split_clauses = []
                if flex_spacing:
                    for w in comp.get('original_words', []):
                        if len(w) >= 3:  # only split words with 3+ chars
                            for i in range(1, len(w)):
                                left, right = w[:i], w[i:]
                                if len(left) >= 1 and len(right) >= 1:
                                    flex_split_clauses.append(f'("{left}" AND "{right}")')

                if clean_vars or flex_split_clauses:
                    all_alternatives = clean_vars + flex_split_clauses
                    parts.append(f'({" OR ".join(all_alternatives)})')

            return " AND ".join(parts)

        # --- Existing path (unchanged) ---
        if mode == 'Regex':
            regex_str = terms[0]
            candidates = re.findall(r'[\u0590-\u05FF]{2,}', regex_str)
            if candidates: return " AND ".join(candidates)
            else: return "*"

        parts = []
        for term in terms:
            if term.upper() in ['AND', 'OR', 'NOT', '(', ')']:
                parts.append(term)
                continue

            # SEED-006 P2: a raw ASCII double-quote inside a term (the typed
            # gershayim substitute, e.g. רמב"ם) would break the quoted Tantivy
            # clauses below (f'"{term}"' -> "רמב"ם" = a parse error). The normal
            # entry points already fold it via strip_search_diacritics, but
            # sanitize defensively here too so direct callers (tests/API) are
            # safe and the exact clause matches the diacritic-folded variants.
            term = term.replace('"', '')
            if not term:
                continue

            if mode == 'fuzzy':
                if len(term) < 3: parts.append(f'"{term}"')
                elif len(term) < 5: parts.append(f'"{term}"~1')
                else: parts.append(f'"{term}"~2')
            else:
                # 1. Get variants (limit 200 is usually enough if quality is good)
                all_vars = self.var_mgr.get_variants(term, mode, limit=200)

                # 2. Prepare list
                clean_vars = []
                seen_vars = set()

                # Add EXACT term with BOOST (^5)
                # This tells Tantivy: "If you find the exact word, it's 5x more important"
                clean_vars.append(f'"{term}"^5')

                # Add variants
                for v in all_vars:
                    if v == term: continue # Skip exact (already added)

                    if len(term) > 1 and len(v) < 2:
                        continue

                    # Clean quotes
                    v_clean = v.replace('"', '')
                    if v_clean:
                        # Multi-char variants (different length) get medium boost
                        # This ensures they rank higher and don't get cut off at search limit
                        if len(v_clean) != len(term):
                            clean_vars.append(f'"{v_clean}"^3')
                        else:
                            clean_vars.append(f'"{v_clean}"')

                # Bracket variants: add bracket-adorned forms so Tantivy
                # returns documents where the term appears with scholarly
                # brackets (e.g., ]הנתשנ for query הנתשנ).
                for bv in _add_bracket_variants(term):
                    if bv != term and bv not in seen_vars:
                        clean_vars.append(f'"{bv}"')
                        seen_vars.add(bv)

                # SEED-006 Stage 2: lower-weighted diacritic-folded fallback so a
                # query like צמאן / צ'מאן reaches the corpus form צ̇מאן (U+0307).
                # Explicit field clause (works without content_search being in the
                # parse_query default list — verified on tantivy 0.25); strip
                # quotes/diacritics defensively. ^0.5 keeps it below the ^5 exact.
                if content_search_field:
                    cs_term = strip_search_diacritics(term).replace('"', '')
                    if cs_term:
                        clean_vars.append(f'{content_search_field}:"{cs_term}"^0.5')

                parts.append(f'({" OR ".join(clean_vars)})')

        return " AND ".join(parts)

    def build_regex_pattern(self, terms, mode, max_gap, responsa_components=None, responsa_options=None, per_pair_gaps=None):
        # --- Responsa branch ---
        if responsa_components is not None:
            opts = responsa_options or {}
            flex_spacing = opts.get('flex_spacing', False)
            bidirectional = opts.get('bidirectional', False)

            parts = []
            for comp in responsa_components:
                wildcard = comp.get('wildcard')
                inline_pattern = comp.get('inline_pattern')

                if inline_pattern:
                    # Inline alternation: word(a/b)end
                    part = _expand_inline_alternation(inline_pattern)
                    parts.append(f"({part})")
                    continue

                if wildcard == 'pattern':
                    # Character pattern: *a*b*c*
                    part = _build_wildcard_regex(comp)
                    parts.append(f"({part})")
                    continue

                if wildcard in ('suffix', 'prefix'):
                    # Wildcard suffix/prefix
                    part = _build_wildcard_regex(comp)
                    if part:
                        parts.append(part)
                    continue

                # Regular terms: build alternation group
                regex_terms = comp.get('regex_terms', [])
                flex_patterns = comp.get('flex_patterns', [])

                all_alternatives = []

                # Add regex terms (sorted by length descending, escaped)
                unique_terms = sorted(set(regex_terms), key=len, reverse=True)
                for t in unique_terms:
                    all_alternatives.append(make_mark_tolerant_pattern(re.escape(t)))

                # Add flex spacing patterns (already regex, NOT escaped)
                if flex_spacing and flex_patterns:
                    for fp in flex_patterns:
                        if fp not in all_alternatives:
                            all_alternatives.append(fp)

                if all_alternatives:
                    parts.append(f"({'|'.join(all_alternatives)})")

            if not parts:
                return None

            def _make_sep_for_gap(gap_val, flex):
                """Build regex separator for a specific gap value."""
                if gap_val == 0:
                    if flex:
                        return r'[^\w\u0590-\u05FF\']*'
                    else:
                        return r'[^\w\u0590-\u05FF\']+'
                else:
                    return rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{gap_val}}}[^\w\u0590-\u05FF\']+'

            def _join_parts_with_gaps(parts_list, gaps_list, default_gap, flex):
                """Join regex parts with per-pair or uniform gap separators."""
                if len(parts_list) == 1:
                    return parts_list[0]
                result = parts_list[0]
                for i in range(1, len(parts_list)):
                    gap = gaps_list[i-1] if gaps_list and i-1 < len(gaps_list) and gaps_list[i-1] is not None else default_gap
                    result += _make_sep_for_gap(gap, flex) + parts_list[i]
                return result

            forward = _join_parts_with_gaps(parts, per_pair_gaps, max_gap, flex_spacing)

            if bidirectional and len(parts) >= 2:
                reversed_gaps = list(reversed(per_pair_gaps)) if per_pair_gaps else per_pair_gaps
                backward = _join_parts_with_gaps(list(reversed(parts)), reversed_gaps, max_gap, flex_spacing)
                pattern_str = f"({forward})|({backward})"
            else:
                pattern_str = forward

            try:
                return re.compile(pattern_str, re.IGNORECASE)
            except Exception:
                return None  # Boundary data unavailable for this document

        # --- Existing path (unchanged) ---
        if mode == 'Regex':
            try: return re.compile(" ".join(terms), re.IGNORECASE)
            except re.error: return None

        parts = []
        for term in terms:
            regex_mode = 'variants_maximum' if mode == 'fuzzy' else mode

            # 1. Get variants
            vars_list = self.var_mgr.get_variants(term, regex_mode, limit=Config.REGEX_VARIANTS_LIMIT)

            # 2. Ensure exact term
            if term not in vars_list:
                vars_list.append(term)

            # 3. Sort by LENGTH (Descending)
            # This is the correct fix for the visual glitch.
            # Favor longer matches before short variants
            unique_vars = sorted(list(set(vars_list)), key=len, reverse=True)

            # 4. Escape special chars
            escaped = [make_mark_tolerant_pattern(re.escape(v)) for v in unique_vars]

            # 5. Simple Group (Removed strict Lookbehind/Lookahead)
            # Allow prefix matches when search term appears inside a word
            parts.append(f"({'|'.join(escaped)})")

        if max_gap == 0:
            # Flexible separator (any non-word char)
            sep = r'[^\w\u0590-\u05FF\']+'
        else:
            # Gap logic
            sep = rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{max_gap}}}[^\w\u0590-\u05FF\']+'

        try:
            return re.compile(sep.join(parts), re.IGNORECASE)
        except re.error:
            return None

    def highlight(self, text, regex, for_file=False):
        m = regex.search(text)
        if not m: return None
        s, e = m.span()
        start = max(0, s - 60)
        end = min(len(text), e + 60)
        
        # Calculate indices relative to snippet
        rel_s = s - start
        rel_e = e - start

        # Grab raw snippet
        snippet = text[start:end]
        
        # Sanitize snippet to prevent interference with markers (replace with space to keep indices)
        snippet_safe = snippet.replace('*', ' ')

        # Insert Asterisks for Unified Highlighting
        hl_snippet = snippet_safe[:rel_s] + f"*{snippet_safe[rel_s:rel_e]}*" + snippet_safe[rel_e:]

        if not for_file:
            # For UI Table: Replace newlines with ‖ line-break indicator
            return hl_snippet.replace('\n', ' \u2016 ')

        # For File/Export: Keep newlines
        return hl_snippet

    def _highlight_by_span(self, text, span, for_file=False):
        """Return a highlighted snippet around a specific span."""
        if not span:
            return None
        s, e = span
        start = max(0, s - 60)
        end = min(len(text), e + 60)

        rel_s = s - start
        rel_e = e - start

        snippet = text[start:end]
        snippet_safe = snippet.replace('*', ' ')
        hl_snippet = snippet_safe[:rel_s] + f"*{snippet_safe[rel_s:rel_e]}*" + snippet_safe[rel_e:]

        if not for_file:
            return hl_snippet.replace('\n', ' \u2016 ')
        return hl_snippet

    def _parse_boundaries(self, doc):
        raw = self._get_field(doc, 'boundaries', [""])
        if not raw or not raw[0]:
            return []
        try:
            return json.loads(raw[0])
        except Exception as e:
            uid_val = None
            try:
                uid_val = doc['unique_id'][0]
            except Exception:
                uid_val = '?'  # UID extraction failed; use placeholder for warning message
            LOGGER.warning("Failed to parse boundaries for doc %s: %s", uid_val, e)
            return []

    def _map_span_to_pages(self, span, boundaries):
        """Return page overlaps and primary page for a match span."""
        overlaps = []
        primary = None
        if not span:
            return {'primary': primary, 'overlaps': overlaps, 'cross_page': False}

        s, e = span
        for b in boundaries:
            b_start = b.get('start', 0)
            b_end = b.get('end', 0)
            if e <= b_start or s >= b_end:
                continue
            overlap_start = max(s, b_start)
            overlap_end = min(e, b_end)
            if overlap_start >= overlap_end:
                continue
            rel_start = overlap_start - b_start
            rel_end = overlap_end - b_start
            overlaps.append({
                'uid': b.get('uid'),
                'p_num': b.get('p_num'),
                'full_header': b.get('full_header', ''),
                'source': b.get('source', ''),
                'sys_id': b.get('sys_id', ''),
                'span': (rel_start, rel_end)
            })
            if not primary:
                primary = b
        cross_page = len({o.get('uid') for o in overlaps if o.get('uid')}) > 1
        return {'primary': primary or (boundaries[0] if boundaries else None), 'overlaps': overlaps, 'cross_page': cross_page}

    def _get_field(self, doc, field, default=None):
        try:
            return doc[field]
        except Exception:
            return default  # Key missing or type mismatch; caller gets default value

    def _get_best_text_for_id(self, sys_id):
        """Find the first page with meaningful text for a given System ID."""
        if not self.searcher: return "", "", "", ""

        # Query index for all pages of this manuscript
        try:
            q = self.index.parse_query(f'full_header:"{sys_id}"', ["full_header"])
            # Fetch enough docs to cover a manuscript
            res = self.searcher.search(q, 2000)
        except (ValueError, RuntimeError):
            return "", "", "", ""

        pages = []
        for score, doc_addr in res.hits:
            doc = self.searcher.doc(doc_addr)
            full_header = doc['full_header'][0]

            # Verify this doc really belongs to the sys_id (strict check)
            parsed = self.meta_mgr.parse_header_smart(full_header)
            if parsed[0] != sys_id:
                continue

            p_num_str = parsed[1]
            try: p_num = int(p_num_str)
            except (ValueError, TypeError): p_num = 999999

            content = doc['content'][0]
            uid = doc['unique_id'][0]
            src = doc['source'][0]
            pages.append({'p': p_num, 'text': content, 'head': full_header, 'uid': uid, 'src': src})

        if not pages:
            return "", "", "", ""

        # Sort by page number
        pages.sort(key=lambda x: x['p'])

        # Heuristic: Find first page with sequence of 3 words, each > 3 chars
        best_page = pages[0] # Default to first page

        pattern = re.compile(r'[\w\u0590-\u05FF]{4,}\s+[\w\u0590-\u05FF]{4,}\s+[\w\u0590-\u05FF]{4,}')

        for p in pages:
            if pattern.search(p['text']):
                best_page = p
                break

        return best_page['text'], best_page['head'], best_page['src'], best_page['uid']

    def parse_query_syntax(self, query, responsa_mode=False):
        """
        Parses search syntax prefix from query string.
        Returns (mode, clean_query). If no prefix, returns (None, query).

        When responsa_mode=True, all prefix shortcuts are bypassed and the raw
        query is returned unchanged. This prevents '#' from being interpreted as
        Shelfmark when Responsa mode is active.

        Prefixes:
        - ??? = variants_maximum (top 150 pairs)
        - ?? = variants_extended (top 70 pairs)
        - ? = variants (top 30 pairs, basic)
        - = = exact
        - ~ = fuzzy
        - / = Regex
        - $ = Title
        - # = Shelfmark
        - R = responsa
        """
        if responsa_mode:
            return None, query
        if not query: return None, ""

        # Check multi-character prefixes first (order matters: longer prefixes first)
        prefix_map = [
            ('???', 'variants_maximum'),
            ('??', 'variants_extended'),
            ('?', 'variants'),
            ('=', 'exact'),
            ('~', 'fuzzy'),
            ('/', 'Regex'),
            ('$', 'Title'),
            ('#', 'Shelfmark'),
            ('R', 'responsa'),
        ]

        for prefix, mode in prefix_map:
            if query.startswith(prefix):
                clean = query[len(prefix):].lstrip()
                if clean:
                    return mode, clean

        return None, query

    def _expand_responsa_component(self, comp, responsa_options):
        """Expand a single ResponsaComponent through the full Responsa expansion pipeline.

        Returns list of expanded words (lowercase strings).
        """
        variants_on = responsa_options.get('variants', False)
        ja_on = responsa_options.get('ja', False)
        variant_mode = responsa_options.get('variant_mode', 'exact')

        expanded = list(comp.words)

        if comp.plene_defective:
            plene = []
            for w in expanded:
                plene.extend(expand_plene_defective(w))
            expanded = list(dict.fromkeys(plene))

        if comp.grammatical_prefixes:
            pfx = []
            for w in expanded:
                pfx.extend(expand_grammatical_prefixes(w))
            expanded = list(dict.fromkeys(pfx))

        if comp.grammatical_suffixes:
            sfx = []
            for w in expanded:
                sfx.extend(expand_grammatical_suffixes(w))
            expanded = list(dict.fromkeys(sfx))

        if ja_on:
            ja = []
            for w in expanded:
                ja.extend(expand_judeo_arabic(w))
            expanded = list(dict.fromkeys(ja))

        if variants_on and self.var_mgr:
            var = []
            for w in expanded:
                try:
                    var.extend(self.var_mgr.get_variants(w, variant_mode, limit=200))
                except Exception:
                    var.append(w)  # Variant expansion failed for this word; use original
            expanded = list(dict.fromkeys(var))

        return expanded

    @staticmethod
    def _build_line_break_regex(line_groups, line_gaps, expanded_groups):
        """Build a regex pattern for line-break search.

        Each group becomes a line pattern, joined by newline separators with gap support.
        Returns compiled regex or None.
        """
        def _line_word_sep(gap):
            """Separator between two words on the SAME line (never crosses \\n).

            gap None/0 -> adjacent words (whitespace only). gap N>0 -> up to N
            intervening words on the line (CR HIGH-6 — [N] word gaps in line mode).
            """
            if not gap:
                return r'[^\S\n]+'
            return r'(?:[^\S\n]+\S+){0,' + str(int(gap)) + r'}[^\S\n]+'

        line_patterns = []
        for gi, group in enumerate(line_groups):
            # Build per-component alternatives, tracking the [N] word gap that
            # precedes each part so the separators below can honor it.
            comp_parts = []  # list of (pattern, gap_before)
            grp_word_gaps = getattr(group, 'word_gaps', None) or []
            for ci, word_set in enumerate(expanded_groups[gi]):
                gap_before = (
                    grp_word_gaps[ci - 1]
                    if ci > 0 and (ci - 1) < len(grp_word_gaps)
                    else None
                )
                # Check if component has a wildcard — dispatch to wildcard regex builder
                comp = group.components[ci] if ci < len(group.components) else None
                pat = None
                if comp and comp.wildcard:
                    comp_dict = {
                        'wildcard': comp.wildcard,
                        'wildcard_pattern': comp.wildcard_pattern,
                        'regex_terms': sorted(word_set, key=len, reverse=True),
                        'original_words': comp.words,
                    }
                    pat = _build_wildcard_regex(comp_dict)
                # Plain word path (non-wildcard or wildcard fallback)
                if pat is None:
                    sorted_words = sorted(word_set, key=len, reverse=True)
                    escaped = [make_mark_tolerant_pattern(re.escape(w)) for w in sorted_words]
                    if not escaped:
                        continue
                    pat = f"({'|'.join(escaped)})"
                comp_parts.append((pat, gap_before))

            if not comp_parts:
                continue

            # Build line pattern based on position constraints
            is_first_start = group.line_start
            is_last_end = group.line_end

            if len(comp_parts) == 1:
                word_pat = comp_parts[0][0]
            else:
                # Multiple words on same line: join with per-pair gap separators
                word_pat = comp_parts[0][0]
                for pat, gap_before in comp_parts[1:]:
                    word_pat += _line_word_sep(gap_before) + pat

            if is_first_start and is_last_end:
                # Entire line must be just these words
                line_pat = r'^\s*' + word_pat + r'\s*$'
            elif is_first_start:
                line_pat = r'^\s*' + word_pat + r'.*$'
            elif is_last_end:
                line_pat = r'^.*' + word_pat + r'\s*$'
            else:
                # Word anywhere on line
                line_pat = r'^.*' + word_pat + r'.*$'

            line_patterns.append(line_pat)

        if not line_patterns:
            return None

        # Join line patterns with newline separators respecting gaps
        parts = [line_patterns[0]]
        for i in range(1, len(line_patterns)):
            gap = line_gaps[i - 1] if i - 1 < len(line_gaps) and line_gaps[i - 1] is not None else 0
            if gap == 0:
                parts.append(r'\n')
            else:
                # Skip exactly `gap` lines
                parts.append(r'\n(?:.*\n){' + str(gap) + r'}')
            parts.append(line_patterns[i])

        pattern_str = ''.join(parts)
        try:
            return re.compile(pattern_str, re.IGNORECASE | re.MULTILINE)
        except re.error as e:
            LOGGER.warning("Line-break regex failed to compile: %s", e)
            return None

    def _execute_line_break_search(self, line_groups, line_gaps, query_str,
                                    responsa_options=None, progress_callback=None,
                                    exclude_words=None, restrict_sys_ids=None,
                                    text_position=None):
        """Execute a line-break search using | syntax.

        Uses regex for both matching and highlighting (fast + correct highlights).
        """
        if not self.searcher:
            return []
        opts = responsa_options or {}

        # CR HIGH-5: the whole-query Text Position 'line_start'/'line_end' applies
        # to the FIRST / LAST line group in a line-break query. The line-break path
        # previously ignored text_position entirely, so the dropdown silently did
        # nothing once a query became multi-line.
        if text_position == 'line_start' and line_groups:
            line_groups[0].line_start = True
        elif text_position == 'line_end' and line_groups:
            line_groups[-1].line_end = True

        # Expand each component in each group
        expanded_groups = []
        tantivy_parts = []
        for group in line_groups:
            group_expanded = []
            for comp in group.components:
                expanded = self._expand_responsa_component(comp, opts)
                word_set = {w.lower() for w in expanded}
                group_expanded.append(word_set)

                is_first = (comp == group.components[0])
                is_last = (comp == group.components[-1])

                if is_first and group.line_start:
                    field = 'line_starts'
                elif is_last and group.line_end:
                    field = 'line_ends'
                else:
                    field = 'content'

                # Wildcard components: use content field (not positional)
                # since the matched word may differ from the base stem.
                # Expand with grammatical suffixes/prefixes for Tantivy recall.
                if comp.wildcard in ('suffix', 'prefix', 'pattern'):
                    wc_terms = []
                    for w in comp.words:
                        wc_terms.append(f'content:"{w}"')
                        if comp.wildcard == 'suffix':
                            for sfx in expand_grammatical_suffixes(w):
                                wc_terms.append(f'content:"{sfx}"')
                            if w and w[-1] in _SOFIT_TO_NORMAL:
                                converted = w[:-1] + _SOFIT_TO_NORMAL[w[-1]]
                                wc_terms.append(f'content:"{converted}"')
                        elif comp.wildcard == 'prefix':
                            for pfx in expand_grammatical_prefixes(w):
                                wc_terms.append(f'content:"{pfx}"')
                    tantivy_parts.append(f'({" OR ".join(wc_terms)})')
                else:
                    # Use expanded terms (plene, grammatical, JA, variants)
                    # for better Tantivy recall, matching the normal path strategy.
                    # Boost original words higher for scoring.
                    original_set = set(comp.words)
                    tantivy_clause_terms = []
                    seen = set()
                    for w in expanded:
                        w_clean = w.replace('"', '')
                        if not w_clean or w_clean in seen:
                            continue
                        seen.add(w_clean)
                        if w_clean in original_set:
                            tantivy_clause_terms.append(f'{field}:"{w_clean}"^5')
                        else:
                            tantivy_clause_terms.append(f'{field}:"{w_clean}"')
                    if tantivy_clause_terms:
                        tantivy_parts.append(f'({" OR ".join(tantivy_clause_terms)})')

            expanded_groups.append(group_expanded)

        if not tantivy_parts:
            return []

        # Build regex for matching + highlighting
        regex = SearchEngine._build_line_break_regex(line_groups, line_gaps, expanded_groups)
        if not regex:
            return []

        t_query_str = " AND ".join(tantivy_parts)
        pattern_str = regex.pattern
        LOGGER.debug(f"Line-break search, Tantivy: {t_query_str[:500]}")
        LOGGER.debug(f"Line-break regex: {pattern_str[:500]}")

        if restrict_sys_ids is not None and len(restrict_sys_ids) <= 500:
            sid_clauses = ' OR '.join(f'full_header:"{sid}"' for sid in restrict_sys_ids)
            t_query_str = f'({t_query_str}) AND ({sid_clauses})'

        try:
            query = self.index.parse_query(t_query_str, ['content'])
            res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
        except Exception as e:
            LOGGER.warning("Line-break search query failed: %s", e)
            return []

        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        LOGGER.debug(f"Line-break Tantivy returned {total_hits} hits")

        restrict_uids = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])

        results = []
        regex_filtered = 0
        was_interrupted = False

        try:
            for i, (score, doc_addr) in enumerate(hits):
                if progress_callback and i % 5 == 0:
                    progress_callback(i, total_hits)
                try:
                    doc = self.searcher.doc(doc_addr)

                    if restrict_uids is not None:
                        if doc['unique_id'][0] not in restrict_uids:
                            continue

                    content = self._get_field(doc, 'content', [""])[0]

                    # Bracket handling: strip brackets for bracket-free queries
                    match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)

                    # Regex match — fast filter + provides highlight span
                    match_obj = regex.search(match_content)
                    if not match_obj:
                        regex_filtered += 1
                        continue

                    # Re-search on original content for highlighting
                    if match_content is not content:
                        orig_match = regex.search(content)
                        if orig_match:
                            match_obj = orig_match

                    # Text position filter — strip brackets from
                    # prefix/suffix only for bracket-free queries
                    _brackets_in_query = _query_has_brackets(query_str)
                    if text_position == 'start' and match_obj.start() > 0:
                        prefix = content[:match_obj.start()]
                        cleaned = prefix.strip() if _brackets_in_query else _strip_brackets(prefix).strip()
                        if cleaned:
                            regex_filtered += 1
                            continue
                    elif text_position == 'end' and match_obj.end() < len(content):
                        suffix = content[match_obj.end():]
                        cleaned = suffix.strip() if _brackets_in_query else _strip_brackets(suffix).strip()
                        if cleaned:
                            regex_filtered += 1
                            continue

                    # Use standard highlight helpers with the match span
                    span = match_obj.span()
                    scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
                    scope = scope_list[0]
                    boundaries = self._parse_boundaries(doc) if scope != 'page' else []

                    hl_c = self.highlight(content, regex, False)
                    hl_f = self.highlight(content, regex, True)

                    if boundaries:
                        span_map = self._map_span_to_pages(span, boundaries)
                        primary = span_map.get('primary') or {}
                        display_header = primary.get('full_header', doc['full_header'][0])
                        source_label = primary.get('source', doc['source'][0])
                        meta = self.meta_mgr.get_display_data(display_header, source_label)
                        page_highlights = []
                        for ov in span_map.get('overlaps', []):
                            if 'span' in ov and ov.get('uid'):
                                page_highlights.append({
                                    'uid': ov.get('uid'),
                                    'p_num': ov.get('p_num'),
                                    'span': ov.get('span'),
                                    'full_header': ov.get('full_header', ''),
                                    'source': ov.get('source', '')
                                })
                        results.append({
                            'display': meta,
                            'snippet': hl_c or "",
                            'full_text': content,
                            'uid': primary.get('uid') or doc['unique_id'][0],
                            'raw_header': display_header,
                            'raw_file_hl': hl_f or "",
                            'highlight_pattern': pattern_str,
                            'page_highlights': page_highlights,
                            'cross_page': span_map.get('cross_page', False),
                            'scope': scope
                        })
                    else:
                        if hl_c:
                            meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])
                            results.append({
                                'display': meta, 'snippet': hl_c, 'full_text': content,
                                'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
                                'raw_file_hl': hl_f, 'highlight_pattern': pattern_str,
                                'scope': scope
                            })

                except Exception as e:
                    LOGGER.warning("Line-break search: failed to process hit %s: %s", i, e)
        except InterruptedError:
            was_interrupted = True

        LOGGER.debug(f"Line-break search: {len(results)} results, filtered: {regex_filtered}, interrupted: {was_interrupted}")
        deduped = self._deduplicate(results)

        if exclude_words and deduped:
            filtered = []
            for r in deduped:
                text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()
                should_exclude = any(w.lower() in text_content for w in exclude_words)
                if not should_exclude:
                    filtered.append(r)
            deduped = filtered

        return deduped

    def _execute_metadata_search(self, query_str, mode, progress_callback=None, restrict_sys_ids=None):
        """Search by title or shelfmark via csv_bank. Returns results even for metadata-only records."""
        if mode != 'Regex':
            query_str = strip_search_diacritics(query_str)

        field_map = {'Title': 'title', 'Shelfmark': 'shelfmark'}
        target_field = field_map.get(mode)

        # Wildcard or empty query with restrict set: return all restricted IDs
        stripped = query_str.strip()
        if (stripped == '*' or stripped == '') and restrict_sys_ids:
            sys_ids = sorted(restrict_sys_ids)
        else:
            sys_ids = self.meta_mgr.search_by_meta(query_str, target_field)
            if restrict_sys_ids is not None:
                sys_ids = [s for s in sys_ids if s in restrict_sys_ids]
        results = []
        total_ids = len(sys_ids)

        for i, sid in enumerate(sys_ids):
            if progress_callback and i % 5 == 0:
                progress_callback(i, total_ids)

            # Try to get transcription text from Tantivy
            text, head, src, uid = '', '', '', ''
            if self.searcher:
                text, head, src, uid = self._get_best_text_for_id(sid)

            metadata_only = not text

            if metadata_only:
                # Build display from csv_bank metadata (no Tantivy needed)
                meta_info = self.meta_mgr.get_meta_for_id(sid)
                if isinstance(meta_info, tuple):
                    # get_meta_for_id returns (shelfmark, title) tuple
                    shelfmark, title = meta_info
                    meta_info = {'shelfmark': shelfmark, 'title': title}
                display = {
                    'shelfmark': meta_info.get('shelfmark', f'ID: {sid}'),
                    'title': meta_info.get('title', ''),
                    'img': '',
                    'source': '',
                    'id': sid,
                    'library_code': meta_info.get('library_code', '') if isinstance(meta_info, dict) else '',
                }
                # For tuple returns, get library_code from csv_bank directly
                if isinstance(meta_info, dict) and not display['library_code']:
                    display['library_code'] = self.meta_mgr.get_library_for_id(sid)
                elif not display['library_code']:
                    display['library_code'] = self.meta_mgr.get_library_for_id(sid)
                results.append({
                    'display': display,
                    'snippet': '',
                    'full_text': '',
                    'uid': '',
                    'raw_header': '',
                    'raw_file_hl': '',
                    'highlight_pattern': None,
                    'metadata_only': True,
                })
            else:
                meta = self.meta_mgr.get_display_data(head, src or "V0.8")
                snippet = text[:300] + "..." if len(text) > 300 else text
                results.append({
                    'display': meta,
                    'snippet': snippet,
                    'full_text': text,
                    'uid': uid,
                    'raw_header': head,
                    'raw_file_hl': text,
                    'highlight_pattern': None,
                    'metadata_only': False,
                })

        results.sort(key=lambda r: natural_sort_key(r.get('display', {}).get('shelfmark', '')))
        return results

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None, responsa_options=None, restrict_sys_ids: set = None, text_position: str = None, corpus_scope: str = "all"):
        # R2-#1: discard any stale per-thread downgrade signal from a prior
        # invocation (e.g., a prior request that crashed before consuming).
        # Keeps the signal one-shot per execute_search call, so it cannot
        # leak across requests on the same worker thread.
        _consume_last_responsa_downgrade()
        # Phase 81A (Codex MEDIUM-2) — drain the structured-meta channel
        # symmetrically so a direct-core caller cannot leave a stale meta
        # dict that a later web request would read as "the cascade fired."
        _consume_last_responsa_downgrade_meta()
        # --- Metadata Search Modes (csv_bank-backed, no Tantivy needed) ---
        if mode in ['Title', 'Shelfmark']:
            return self._execute_metadata_search(query_str, mode, progress_callback, restrict_sys_ids)

        # Phase 95 smoke-fix (item 2): corpus_scope gates which index is queried.
        # 'local' → query LOCAL side-index only (skip Genizah Tantivy).
        # 'genizah' → query Genizah only (LOCAL merge block below is skipped).
        # 'all' → current behavior: Genizah + LOCAL merged via RRF.
        if corpus_scope == "local":
            if getattr(self, "local_searcher", None) is None:
                return []
            try:
                # SEED-006: strip diacritics here (the 'all' path strips at the
                # shared site below) so the LOCAL regex is built mark-tolerant
                # AND content_search retrieval folds צ'מאן -> צמאן. Mirrors the
                # main path; Regex mode is left untouched (user owns the pattern).
                if mode != 'Regex':
                    query_str = strip_search_diacritics(query_str)
                # Phase 95-05 follow-up: Responsa operators (#/*/%/(a/b)) need the
                # full component expansion; the simplified path below strips them and
                # returns nothing. Build the same candidate query + components-aware
                # regex the main index uses, then run it against LOCAL. Line-break (|)
                # is main-index only -> helper returns None -> simplified fallback.
                if responsa_options and responsa_options.get('responsa_mode'):
                    _resp_q, _resp_regex = self._build_local_responsa_query_and_regex(
                        query_str, mode, gap, responsa_options
                    )
                    if _resp_q is not None and _resp_regex is not None:
                        return self._query_local_index(
                            query_str, mode, gap,
                            regex=_resp_regex, tantivy_query_str=_resp_q,
                        )
                # Phase 96 D-F5: build regex here so LOCAL-only path also gets
                # D-04.1 filter-out + highlight_pattern, same as the RRF merge path.
                if mode == 'Regex':
                    _local_terms = [query_str]
                else:
                    _local_terms = query_str.split()
                _local_regex = self.build_regex_pattern(_local_terms, mode, gap)
                return self._query_local_index(query_str, mode, gap, regex=_local_regex or None)
            except Exception as _le:
                LOGGER.warning("LOCAL-only search failed: %r", _le)
                return []

        if not self.searcher: return []
        _line_constraints = {}  # Per-line position constraints (L3:word syntax)
        _has_wildcard_component = False  # Set True when any Responsa component has a wildcard

        # Strip combining diacritical marks and geresh/gershayim from query
        # Skip for Regex mode -- user controls the pattern directly
        if mode != 'Regex':
            query_str = strip_search_diacritics(query_str)

        # --- Responsa Pipeline ---
        responsa_warning = None
        # Phase 95-05: the clean (pre-restrict) Responsa candidate query, captured
        # so the LOCAL RRF merge can reuse it without re-parsing/expanding (keeps the
        # single parse_responsa_query/build_regex_pattern contract the tests pin).
        # Stays None for non-Responsa searches.
        _local_responsa_query = None
        if responsa_options and responsa_options.get('responsa_mode'):
            # a. Bypass prefix shortcuts
            self.parse_query_syntax(query_str, responsa_mode=True)

            # b. Parse Responsa query into components
            components = parse_responsa_query(query_str)
            if not components:
                return []

            # b2. Extract per-pair gap values from [N] tokens
            per_pair_gaps = extract_per_pair_gaps(query_str)

            # b3. Check for line-break syntax (| separators)
            line_groups, line_gaps = _parse_line_break_query(query_str)
            if line_groups is not None:
                return self._execute_line_break_search(
                    line_groups, line_gaps, query_str,
                    responsa_options=responsa_options,
                    progress_callback=progress_callback,
                    exclude_words=exclude_words,
                    restrict_sys_ids=restrict_sys_ids,
                    text_position=text_position,
                )

            # c. Expand each component
            variants_on = responsa_options.get('variants', False)
            ja_on = responsa_options.get('ja', False)
            flex_spacing = responsa_options.get('flex_spacing', False)
            bidirectional = responsa_options.get('bidirectional', False)
            variant_mode = responsa_options.get('variant_mode', 'exact')

            # Rewrite simple *word* patterns to #word# (prefix+suffix expansion).
            # Both-side wildcards can't be executed as true substring searches in
            # Tantivy, so we convert to grammatical expansion which is more useful.
            rewritten_patterns = []
            for comp in components:
                if (comp.wildcard == 'pattern' and comp.wildcard_pattern
                        and comp.wildcard_pattern.startswith('*')
                        and comp.wildcard_pattern.endswith('*')
                        and comp.wildcard_pattern.count('*') == 2):
                    stem = comp.wildcard_pattern.strip('*')
                    if stem:
                        rewritten_patterns.append(f'*{stem}*')
                        comp.words = [stem]
                        comp.wildcard = None
                        comp.wildcard_pattern = None
                        comp.grammatical_prefixes = True
                        comp.grammatical_suffixes = True
            if rewritten_patterns:
                rewrite_msg = tr("*word* rewritten as #word# (prefix + suffix expansion)")
                responsa_warning = rewrite_msg

            # Apply explosion guard (estimates before materializing)
            _components, guard_warning, actual_opts = _apply_explosion_guard(
                components,
                variants_on=variants_on,
                ja_on=ja_on,
                var_mgr=self.var_mgr,
                variant_mode=variant_mode,
            )
            if guard_warning:
                responsa_warning = f"{responsa_warning}; {guard_warning}" if responsa_warning else guard_warning
                variants_on = actual_opts['variants_on']
                ja_on = actual_opts['ja_on']
                variant_mode = actual_opts['variant_mode']

            # d. Separate negated components from positive ones
            positive_components = [c for c in components if not c.negated]
            negated_components = [c for c in components if c.negated]

            # Extract negated words and add to exclude_words
            if negated_components:
                negated_word_list = []
                for nc in negated_components:
                    for w in nc.words:
                        negated_word_list.append(w)
                        # Also expand plene/defective for negated words
                        if nc.plene_defective:
                            negated_word_list.extend(expand_plene_defective(w))
                if negated_word_list:
                    if exclude_words is None:
                        exclude_words = []
                    exclude_words = list(exclude_words) + negated_word_list

            if not positive_components:
                return []

            # Build per-component dicts with expanded terms
            component_dicts = []
            for comp in positive_components:
                # Start with component.words
                expanded_words = list(comp.words)

                # 1. Plene/defective expansion (before prefix/suffix to generate base variants)
                if comp.plene_defective:
                    plene_expanded = []
                    for w in expanded_words:
                        plene_expanded.extend(expand_plene_defective(w))
                    expanded_words = list(dict.fromkeys(plene_expanded))  # dedupe preserving order

                # 2. Grammatical prefix expansion
                if comp.grammatical_prefixes:
                    prefix_expanded = []
                    for w in expanded_words:
                        prefix_expanded.extend(expand_grammatical_prefixes(w))
                    expanded_words = list(dict.fromkeys(prefix_expanded))

                # 3. Grammatical suffix expansion
                if comp.grammatical_suffixes:
                    suffix_expanded = []
                    for w in expanded_words:
                        suffix_expanded.extend(expand_grammatical_suffixes(w))
                    expanded_words = list(dict.fromkeys(suffix_expanded))

                # 4. Judeo-Arabic expansion
                if ja_on:
                    ja_expanded = []
                    for w in expanded_words:
                        ja_expanded.extend(expand_judeo_arabic(w))
                    expanded_words = list(dict.fromkeys(ja_expanded))

                # 5. Spelling variants expansion
                if variants_on and self.var_mgr:
                    var_expanded = []
                    for w in expanded_words:
                        try:
                            variants = self.var_mgr.get_variants(w, variant_mode, limit=200)
                            var_expanded.extend(variants)
                        except Exception:
                            var_expanded.append(w)  # Variant expansion failed for this word; use original
                    expanded_words = list(dict.fromkeys(var_expanded))

                # Build flex spacing patterns for original words only
                flex_patterns = []
                if flex_spacing:
                    for w in comp.words:
                        flex_patterns.append(_make_flex_spacing_pattern(w))

                component_dicts.append({
                    'tantivy_terms': expanded_words,
                    'regex_terms': expanded_words,
                    'original_words': comp.words,
                    'wildcard': comp.wildcard,
                    'wildcard_pattern': comp.wildcard_pattern,
                    'flex_patterns': flex_patterns,
                    'inline_pattern': comp.inline_pattern,
                })
                if comp.wildcard:
                    _has_wildcard_component = True

            # Calculate total expanded terms for UI display
            total_expanded = sum(len(cd['tantivy_terms']) for cd in component_dicts)

            # e. Build Tantivy query
            t_query_str = self.build_tantivy_query(
                terms=None, mode=mode,
                responsa_components=component_dicts,
                responsa_options=responsa_options,
            )
            # f. Build regex pattern
            regex = self.build_regex_pattern(
                terms=None, mode=mode, max_gap=gap,
                responsa_components=component_dicts,
                responsa_options=responsa_options,
                per_pair_gaps=per_pair_gaps,
            )
            # Phase 95-05: capture the clean Responsa candidate query for the LOCAL
            # RRF merge below — BEFORE any restrict_sys_ids augmentation (which adds
            # genizah-only full_header clauses that don't apply to LOCAL docs).
            _local_responsa_query = t_query_str
        else:
            # --- Existing path (unchanged) ---
            if mode == 'Regex': terms = [query_str]
            else: terms = query_str.split()

            # Per-line position search: parse L{n}:word constraints
            # e.g. "L1:שלום L3:עליכם" → Tantivy searches positional tokens,
            # regex matches the plain words, post-filter validates line positions
            _line_constraints = {}  # {line_num: word} for post-filter
            if text_position in ('line_start', 'line_end') and mode != 'Regex':
                _lc_pattern = re.compile(r'^L(\d+):(.+)$')
                has_line_prefixes = any(_lc_pattern.match(t) for t in terms)
                if has_line_prefixes:
                    tantivy_parts = []
                    regex_terms = []
                    for t in terms:
                        m = _lc_pattern.match(t)
                        if m:
                            line_num, word = int(m.group(1)), m.group(2)
                            _line_constraints[line_num] = word
                            tantivy_parts.append(f'"{t}"')  # L{n}:word token
                            regex_terms.append(word)         # plain word for regex
                        else:
                            tantivy_parts.append(f'"{t}"')
                            regex_terms.append(t)
                    t_query_str = " AND ".join(tantivy_parts)
                    regex = self.build_regex_pattern(regex_terms, mode, gap)
                    if not regex: return []
                    # Skip the normal build path below
                    terms = None

            if terms is not None:
                # Pre-compute variants at max limit so Tantivy (limit=200) can
                # slice from cache instead of recomputing when regex (limit=8000) runs
                if mode != 'Regex':
                    self._get_or_compute_variants(terms, mode)

                # SEED-006 Stage 2: only fold-fallback for a plain content search.
                # When text_position is set the query is reused against
                # content_head/tail/line_starts/ends, where a full-content
                # content_search clause would defeat the position filter.
                _cs_field = ('content_search'
                             if (not text_position and getattr(self, '_has_content_search', False))
                             else None)
                t_query_str = self.build_tantivy_query(terms, mode, content_search_field=_cs_field)
                regex = self.build_regex_pattern(terms, mode, gap)
        if not regex: return []

        LOGGER.debug(f"Mode: {mode}, Query: {query_str[:200]}")
        LOGGER.debug(f"Tantivy query: {t_query_str[:500]}")
        LOGGER.debug(f"Regex pattern: {regex.pattern[:500]}")

        # Save pattern string for passing to results
        pattern_str = regex.pattern

        # Augment Tantivy query with sys_id filter so the index only returns
        # hits from the restricted manuscripts (avoids iterating 50K hits).
        if restrict_sys_ids is not None and len(restrict_sys_ids) <= 500:
            sid_clauses = ' OR '.join(f'full_header:"{sid}"' for sid in restrict_sys_ids)
            t_query_str = f'({t_query_str}) AND ({sid_clauses})'

        # Choose search field based on text_position filter
        position_field_map = {
            'start': 'content_head',
            'end': 'content_tail',
            'line_start': 'line_starts',
            'line_end': 'line_ends',
        }
        search_field = position_field_map.get(text_position, 'content')

        # Wildcard components with positional fields: fall back to content field.
        # Positional fields only contain exact tokens (first/last words or head/tail),
        # so suffix/prefix wildcards won't match extended forms. The regex +
        # _validate_position_match post-filter handles exact position validation.
        if search_field != 'content' and _has_wildcard_component:
            search_field = 'content'

        try:
            query = self.index.parse_query(t_query_str, [search_field])
            res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
        except Exception as e:
            if text_position and search_field != 'content':
                raise RuntimeError(
                    tr("Line/position search requires a rebuilt index. Please rebuild the index from Settings to use this feature.")
                ) from e
            LOGGER.warning("Search query failed to parse/execute for pattern %s: %s", t_query_str, e)
            return []

        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        LOGGER.debug(f"Tantivy returned {total_hits} hits")
        results = []
        regex_filtered_count = 0
        was_interrupted = False

        # Pre-compute allowed unique_ids for fast O(1) filtering
        # instead of running parse_header_smart regex on every hit
        restrict_uids = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])

        try:
            for i, (score, doc_addr) in enumerate(hits):
                if progress_callback and i % 5 == 0:
                    progress_callback(i, total_hits)
                try:
                    doc = self.searcher.doc(doc_addr)

                    # Pre-search filter: skip manuscripts outside the restrict set
                    if restrict_uids is not None:
                        if doc['unique_id'][0] not in restrict_uids:
                            continue

                    content = self._get_field(doc, 'content', [""])[0]
                    scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
                    scope = scope_list[0]

                    # Bracket handling: strip brackets from content for
                    # bracket-free queries so e.g. הנתשנ matches ]הנתשנ
                    match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)

                    # Check for match before any heavy parsing
                    match_obj = regex.search(match_content)
                    if not match_obj:
                        regex_filtered_count += 1
                        continue

                    # Position post-filter: Tantivy uses broad fields (10-word head/tail),
                    # validate exact position (first word, last word, line boundary)
                    if text_position and not Indexer._validate_position_match(match_content, match_obj, text_position, _line_constraints or None, strip_brackets=not _query_has_brackets(query_str)):
                        regex_filtered_count += 1
                        continue

                    # For highlighting, re-search on original content to
                    # preserve scholarly bracket notation in snippets.
                    if match_content is not content:
                        orig_match = regex.search(content)
                        if orig_match:
                            match_obj = orig_match
                        # else: keep match_obj from stripped content; highlight
                        # may be slightly offset but still useful

                    boundaries = self._parse_boundaries(doc) if scope != 'page' else []
                    span = match_obj.span()
                    if boundaries:
                        span_map = self._map_span_to_pages(span, boundaries)
                        primary = span_map.get('primary') or {}
                        display_header = primary.get('full_header', doc['full_header'][0])
                        source_label = primary.get('source', doc['source'][0])
                        hl_c = self._highlight_by_span(content, span, False)
                        hl_f = self._highlight_by_span(content, span, True)
                        meta = self.meta_mgr.get_display_data(display_header, source_label)
                        page_highlights = []
                        for ov in span_map.get('overlaps', []):
                            if 'span' in ov and ov.get('uid'):
                                page_highlights.append({
                                    'uid': ov.get('uid'),
                                    'p_num': ov.get('p_num'),
                                    'span': ov.get('span'),
                                    'full_header': ov.get('full_header', ''),
                                    'source': ov.get('source', '')
                                })
                        results.append({
                            'display': meta,
                            'snippet': hl_c or "",
                            'full_text': content,
                            'uid': primary.get('uid') or doc['unique_id'][0],
                            'raw_header': display_header,
                            'raw_file_hl': hl_f or "",
                            'highlight_pattern': pattern_str,
                            'page_highlights': page_highlights,
                            'cross_page': span_map.get('cross_page', False),
                            'scope': scope,
                            # Phase 77 D-01: surface Tantivy relevance score so
                            # serialize_search_payload emits non-zero scores.
                            'score': float(score),
                        })
                    else:
                        hl_c = self.highlight(content, regex, False)
                        hl_f = self.highlight(content, regex, True)
                        if hl_c:
                            meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])
                            results.append({
                                'display': meta, 'snippet': hl_c, 'full_text': content,
                                'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
                                'raw_file_hl': hl_f, 'highlight_pattern': pattern_str,
                                'scope': scope,
                                # Phase 77 D-01: Tantivy relevance score for JSON.
                                'score': float(score),
                            })
                except Exception as e:
                    LOGGER.warning("Failed to materialize search hit at position %s: %s", i, e)
        except InterruptedError:
            was_interrupted = True
            LOGGER.debug(f"Search interrupted at hit {i}/{total_hits}, found {len(results)} results so far")

        LOGGER.debug(f"Regex filtered out: {regex_filtered_count}, Results before dedup: {len(results)}, interrupted: {was_interrupted}")
        deduped = self._deduplicate(results)

        # Phase 95 D-08 (Codex P0): LOCAL hits merge AFTER _deduplicate.
        # The dedup body at _deduplicate() whitelists V0.8/V0.7 only and would
        # otherwise DROP LOCAL hits. RRF k=60 used (BM25 IDF from two independent
        # indexes is not comparable; raw score sort would mis-rank — Codex revision).
        # Phase 95 smoke-fix (item 2): skip LOCAL merge when corpus_scope='genizah'.
        if corpus_scope != "genizah" and getattr(self, "local_searcher", None) is not None:
            try:
                # Phase 95-05 follow-up: reuse the main path's already-built,
                # operator-expanded Responsa candidate query (captured above, pre-
                # restrict) so merged LOCAL hits aren't dropped by the simplified
                # path — WITHOUT re-running parse/expand (preserves the single
                # parse_responsa_query/build_regex_pattern call the tests pin). The
                # main `regex` is already components-aware. Non-Responsa: stays None
                # -> simplified path, unchanged. Line-break (|) returned early above.
                if _local_responsa_query is not None:
                    local_hits = self._query_local_index(
                        query_str, mode, gap, regex=regex,
                        tantivy_query_str=_local_responsa_query,
                    )
                else:
                    local_hits = self._query_local_index(query_str, mode, gap, regex=regex)
            except Exception as _e:
                LOGGER.warning(
                    "LOCAL side-index query failed; main results unaffected: %r", _e
                )
                local_hits = []
            if local_hits:
                deduped = self._rrf_merge(deduped, local_hits, k=RRF_K)
        # End Phase 95 D-08 LOCAL merge.

        # --- Apply Exclusion Filter (NOT Filter) ---
        if exclude_words and deduped:
            filtered = []
            for r in deduped:
                # Combine text fields for checking
                # We check snippet and full_text to be safe
                text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()

                # Check if ANY excluded word is present
                should_exclude = False
                for w in exclude_words:
                    if w.lower() in text_content:
                        should_exclude = True
                        break

                if not should_exclude:
                    filtered.append(r)
            deduped = filtered

        LOGGER.debug(f"Results after dedup & filtering: {len(deduped)}")

        # --- Attach Responsa explosion guard warning to first result ---
        # Phase 78 Concern #6: ALSO record on the thread-local so the
        # /api/search handler can surface the warning even when deduped is
        # empty (the legacy results[0] attachment is preserved as a fallback).
        if responsa_warning:
            _set_last_responsa_downgrade(responsa_warning)
            # Phase 81A — structured per-flag effective state alongside the
            # legacy string channel. The local variables variants_on / ja_on /
            # flex_spacing / bidirectional are bound at lines ~7295-7298 from
            # the input responsa_options dict and are mutated by the cascade
            # at lines ~7332-7334 (variants_on, ja_on; variant_mode is
            # internal-only). flex_spacing and bidirectional are pass-through
            # in 81A scope (cascade tiers 4-6 are deferred).
            _set_last_responsa_downgrade_meta({
                'variants':      bool(variants_on),
                'ja':            bool(ja_on),
                'flex_spacing':  bool(flex_spacing),
                'bidirectional': bool(bidirectional),
            })
        if responsa_warning and deduped:
            deduped[0]['responsa_warning'] = responsa_warning

        # --- Attach Responsa expanded term count to first result ---
        if responsa_options and responsa_options.get('responsa_mode') and deduped:
            deduped[0]['responsa_expanded_count'] = total_expanded

        return deduped

    def _deduplicate(self, results):
        v8 = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
        final = list(v8.values())
        for r in results:
            if r['display']['source'] == "V0.7" and r['uid'] not in v8: final.append(r)
        return final

    def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None,
                                   boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                                   min_boundary_matches=0, min_delimiter_distance=3,
                                   restrict_sys_ids: set = None,
                                   corpus_scope: str = 'genizah'):
        """
        Scans composition chunks against the index.
        Returns aggregated results with WIDE source context.

        Boundary Search Modes:
        - 'full': Regular search, track boundary matches for display
        - 'boundary': Only return results with boundary-crossing matches
        - 'combined': Full search with score boost for boundary matches

        Phase 110 (COMP-LOC-01/02): corpus_scope selects which index loop runs —
        'genizah' (Genizah Tantivy loop only), 'local' (regular My-Library index
        only), or 'all' (both, merged into the same doc_hits accumulator — NOT RRF).

        Phase 110 DESIGN CORRECTION (2026-06-08): standard composition queries the
        REGULAR My-Library index (self.local_searcher), the same index regular
        search scope=Local uses — NOT the LAB side-index. The LAB side-index is
        opt-in via Lab Mode (LabEngine.lab_composition_search). The default path
        has no weights-hash / no staleness; an empty LOCAL result is just "no
        results" (no staleness banner).
        """
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'
        _local_lab_stale = False  # Phase 110 Round-2 #4: A2 default so EVERY return path carries it

        # 1. Tokenize original text - track positions for preserving formatting
        token_matches = list(re.finditer(Config.WORD_TOKEN_PATTERN, full_text))
        tokens = [strip_nikud(m.group()) for m in token_matches]  # Strip nikud from tokens
        token_positions = [(m.start(), m.end()) for m in token_matches]  # Store positions

        # Strip nikud from filter text for consistent matching
        if filter_text:
            filter_text = strip_nikud(filter_text)

        if len(tokens) < chunk_size:
            return {'main': [], 'filtered': [], 'boundary_stats': None,
                    'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

        # Get boundary stats (includes parsed boundaries to avoid double parsing)
        boundary_stats = get_boundary_stats(full_text, boundary_delimiter, chunk_size, min_delimiter_distance)
        boundaries = boundary_stats.get('boundaries', [])

        # Build chunks with boundary tracking
        chunks_data = []
        for i in range(len(tokens) - chunk_size + 1):
            crossed_bounds = get_crossed_boundaries(i, i + chunk_size, boundaries)
            chunks_data.append((i, tokens[i:i + chunk_size], crossed_bounds))

        # Single map for all results - track filtering status per uid
        doc_hits = defaultdict(lambda: {
            'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(),
            # Phase 110 UAT (Issue 1): carry the LOCAL doc's shelfmark (filename)
            # so build_items can emit it for the comp display. Empty for Genizah hits.
            'shelfmark': '',
            'patterns': set(), 'boundary_chunk_scores': [], 'crossed_boundaries': set(),
            # Phase 77 D-13: per-chunk attribution mirrors lab_composition_search
            # at line 1366. Same tuple shape (chunk_index, chunk_text, score, snippet)
            # so shared.search_serializer consumes both producers uniformly.
            # chunk_count is derived from chunk_hits at build_items time via
            # _count_unique_chunks() so it reflects unique source-chunk contents,
            # not raw Tantivy hits (matters when source repeats a phrase).
            'chunk_hits': [],
            'is_filtered': False  # True if ANY match came from filtered chunk
        })

        total_chunks = len(chunks_data)
        was_cancelled = False

        # Pre-compute allowed unique_ids for fast O(1) filtering
        restrict_uids = None
        _sid_filter_clause = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])
            # Pre-build Tantivy filter clause for small restrict sets
            if len(restrict_sys_ids) <= 500:
                _sid_filter_clause = '(' + ' OR '.join(
                    f'full_header:"{sid}"' for sid in restrict_sys_ids
                ) + ')'

        # 2. Scan chunks (wrapped in try/except to support partial results on cancel)
        try:
          # Phase 110: gate the Genizah Tantivy loop — skipped on a LOCAL-only run.
          # doc_hits/was_cancelled/total_chunks are initialized ABOVE this branch (M1),
          # so a corpus_scope='local' run never NameErrors on a loop-local variable.
          if corpus_scope != 'local':
            for i, (token_idx, chunk, chunk_crossed_bounds) in enumerate(chunks_data):
                if progress_callback: progress_callback(i, total_chunks)

                # Build query (SEED-006 Stage 2: composition is a plain
                # content word-search → enable the diacritic-fold fallback).
                _cs_field = 'content_search' if getattr(self, '_has_content_search', False) else None
                t_query = self.build_tantivy_query(chunk, mode, content_search_field=_cs_field)
                regex = self.build_regex_pattern(chunk, mode, 0)
                if not regex: continue

                # Augment chunk query with sys_id filter
                if _sid_filter_clause:
                    t_query = f'({t_query}) AND {_sid_filter_clause}'

                # Check: Is phrase in "Filter Text"?
                is_text_filtered = False
                if filter_text:
                    if regex.search(filter_text):
                        is_text_filtered = True

                try:
                    # Search index
                    query = self.index.parse_query(t_query, ["content"])
                    hits = self.searcher.search(query, 50).hits

                    is_freq_filtered = len(hits) > max_freq

                    for score, doc_addr in hits:
                        doc = self.searcher.doc(doc_addr)

                        # Pre-search filter: skip manuscripts outside the restrict set
                        if restrict_uids is not None:
                            if doc['unique_id'][0] not in restrict_uids:
                                continue

                        content = doc['content'][0]

                        # Bracket handling: strip brackets for bracket-free
                        # queries; preserve for bracket-containing queries
                        # (user pasted text with literal brackets).
                        match_content = content if _query_has_brackets(' '.join(chunk)) else _strip_brackets(content)

                        # Verify exact Regex match
                        if regex.search(match_content):
                            uid = doc['unique_id'][0]

                            # Always use single map - accumulate all matches for same uid
                            rec = doc_hits[uid]

                            # Mark as filtered if ANY chunk match is filtered
                            if is_text_filtered or is_freq_filtered:
                                rec['is_filtered'] = True
                                # Annotate filter reason for UI display
                                if is_text_filtered:
                                    rec['filter_reason'] = 'source_text'
                                elif is_freq_filtered:
                                    rec['filter_reason'] = 'high_frequency'

                            rec['head'] = doc['full_header'][0]
                            rec['src'] = doc['source'][0]
                            rec['content'] = content
                            # Use original content span if possible, fall back to stripped
                            _orig_m = regex.search(content)
                            _ms_match = _orig_m or regex.search(match_content)
                            rec['matches'].append(_ms_match.span())
                            # Save indices of found words in *source* text
                            rec['src_indices'].update(range(token_idx, token_idx + chunk_size))
                            rec['patterns'].add(regex.pattern)
                            # chunk_count is derived post-hoc from unique
                            # chunk_hits contents in build_items below;
                            # incrementing here would inflate the count
                            # whenever the source repeats a phrase or Tantivy
                            # returns the same uid from multiple segments.

                            # Phase 77 D-13: per-chunk attribution mirrors
                            # lab_composition_search at line 1390. ms_snip is a
                            # 60-char window around the match for parallels JSON
                            # matches[*].manuscript_snippet (chunk_index is the
                            # 0-based outer-loop variable i).
                            try:
                                _ms_s, _ms_e = _ms_match.span()
                                _snip_s = max(0, _ms_s - 60)
                                _snip_e = min(len(content), _ms_e + 60)
                                ms_snip = content[_snip_s:_ms_s] + f"*{content[_ms_s:_ms_e]}*" + content[_ms_e:_snip_e]
                            except Exception:
                                ms_snip = ''
                            # Dedup: Tantivy can return the same uid twice from
                            # different segments — keep the highest-scoring entry
                            # per (chunk_index, ms_snip) instead of appending duplicates.
                            _seen = rec.setdefault('_chunk_hit_keys', {})
                            _key = (i, ms_snip)
                            _existing_idx = _seen.get(_key)
                            _new_score = float(score)
                            if _existing_idx is None:
                                _seen[_key] = len(rec['chunk_hits'])
                                rec['chunk_hits'].append(
                                    (i, ' '.join(chunk), _new_score, ms_snip)
                                )
                            elif _new_score > rec['chunk_hits'][_existing_idx][2]:
                                rec['chunk_hits'][_existing_idx] = (
                                    i, ' '.join(chunk), _new_score, ms_snip
                                )

                            # Track boundary-crossing matches - each boundary counted once
                            if chunk_crossed_bounds:
                                rec['boundary_chunk_scores'].append(score)
                                rec['crossed_boundaries'].update(chunk_crossed_bounds)
                except Exception as e:
                    LAB_LOGGER.warning(f"Failed composition chunk processing at token {token_idx}: {e}")
        except InterruptedError:
            was_cancelled = True

        # Phase 110 DESIGN CORRECTION (2026-06-08, Plan 110-03 UAT checkpoint):
        # Standard (Lab-Mode-OFF) composition now queries the REGULAR My-Library
        # index (self.local_searcher / self.local_index) — the SAME index that
        # regular search scope=Local uses — NOT the LOCAL LAB side-index. The LAB
        # side-index is opt-in ("Lab Mode") only; routing the default path through
        # it returned nothing when the user had never built a LAB index. The regular
        # LOCAL schema (shared/local_indexer.build_local_schema) carries
        # content/unique_id/full_header/source/content_head/content_tail — every
        # field this hook reads — so the doc-field reads are unchanged.
        #
        # Lab Mode composition (LabEngine.lab_composition_search) is UNCHANGED and
        # keeps using the LAB side-index + its own freshness/staleness. The default
        # path has NO weights-hash and NO staleness concept: an empty LOCAL result
        # is treated exactly like an empty Genizah result ("no results"), with no
        # staleness banner.
        #
        # CR-01 fallback semantics preserved: the outer try/except below guarantees
        # LOCAL never breaks standard Composition Search.
        # Phase 97 R-01: skip LOCAL composition search if is_searchable gate closed.
        _scl_tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
        _scl_is_searchable = getattr(_scl_tab, "is_searchable", True) if _scl_tab is not None else True
        # Phase 110 correction: the regular index has no staleness — the standard
        # path never reports stale. (Lab-Mode staleness is handled in
        # lab_composition_search only.) Keep the per-run key for the result payload.
        _local_lab_stale = False
        # Phase 110: gate the LOCAL hook on corpus_scope (skipped on a Genizah-only run).
        if (not was_cancelled and corpus_scope != 'genizah'
                and getattr(self, 'local_searcher', None) is not None
                and getattr(self, 'local_index', None) is not None
                and _scl_is_searchable):
            try:
                _local_index_scl = self.local_index
                _local_searcher_scl = self.local_searcher
                # SEED-006 M1: parity with the regular LOCAL path (_query_local_index)
                # — when the diacritic-folded content_search field exists, fan
                # field-less chunk terms across it too so צמאן/צ'מאן reach צ̇מאן in
                # LOCAL composition/parallels, not just regular LOCAL search.
                _local_has_cs_scl = getattr(self, "_local_has_content_search", False)
                _local_fields_scl = ["content", "content_head", "content_tail"]
                if _local_has_cs_scl:
                    _local_fields_scl.append("content_search")
                if _local_index_scl is not None and _local_searcher_scl is not None:
                    for _i_scl, (_token_idx_scl, _chunk_scl, _chunk_crossed_scl) in enumerate(chunks_data):
                        # SEED-006 M1: fold diacritics off the chunk for BOTH the
                        # Tantivy query AND the regex backstop (a fold-only hit
                        # retrieved via content_search would otherwise be dropped
                        # by the regex at the `_regex_scl.search` filter below).
                        if _local_has_cs_scl and mode != 'Regex':
                            _chunk_q_scl = [strip_search_diacritics(_w) for _w in _chunk_scl]
                        else:
                            _chunk_q_scl = _chunk_scl
                        _t_query_scl = self.build_tantivy_query(_chunk_q_scl, mode)
                        _regex_scl = self.build_regex_pattern(_chunk_q_scl, mode, 0)
                        if not _regex_scl:
                            continue
                        _is_freq_filtered_scl = False
                        _is_text_filtered_scl = False
                        if filter_text and _regex_scl.search(filter_text):
                            _is_text_filtered_scl = True
                        try:
                            # Mirror _query_local_index's metacharacter-strip
                            # fallback (v7.16 load-bearing fix): Tantivy's parser
                            # chokes on the geresh in אמ' / gershayim in רמב"ם and
                            # the whole LOCAL search returned 0. Strip the syntax
                            # metacharacters and re-parse; the regex filter below
                            # still enforces the precise match.
                            try:
                                _query_scl = _local_index_scl.parse_query(
                                    _t_query_scl, _local_fields_scl
                                )
                            except (ValueError, Exception):
                                _safe_scl = re.sub(
                                    r"[+\-&|!(){}\[\]^\"~*?:\\/']", " ", _t_query_scl
                                ).strip()
                                if not _safe_scl:
                                    continue
                                _query_scl = _local_index_scl.parse_query(
                                    _safe_scl, _local_fields_scl
                                )
                            _hits_scl = _local_searcher_scl.search(_query_scl, 50).hits
                            _is_freq_filtered_scl = len(_hits_scl) > max_freq
                            for _score_scl, _doc_addr_scl in _hits_scl:
                                _doc_scl = _local_searcher_scl.doc(_doc_addr_scl)
                                _content_scl = _doc_scl['content'][0]
                                _match_content_scl = (
                                    _content_scl
                                    if _query_has_brackets(' '.join(_chunk_scl))
                                    else _strip_brackets(_content_scl)
                                )
                                if _regex_scl.search(_match_content_scl):
                                    _uid_scl = _doc_scl['unique_id'][0]
                                    _rec_scl = doc_hits[_uid_scl]
                                    if _is_text_filtered_scl or _is_freq_filtered_scl:
                                        _rec_scl['is_filtered'] = True
                                        if _is_text_filtered_scl:
                                            _rec_scl['filter_reason'] = 'source_text'
                                        elif _is_freq_filtered_scl:
                                            _rec_scl['filter_reason'] = 'high_frequency'
                                    _rec_scl['head'] = _doc_scl['full_header'][0]
                                    _rec_scl['src'] = _doc_scl['source'][0]
                                    # Phase 110 UAT (Issue 1): carry the LOCAL
                                    # doc's shelfmark (filename) through to the
                                    # comp display so the manuscript row shows the
                                    # filename, not "unknown". Tolerate a missing
                                    # field (only the regular LOCAL schema has it).
                                    try:
                                        _shelf_scl = _doc_scl['shelfmark']
                                        _rec_scl['shelfmark'] = _shelf_scl[0] if _shelf_scl else ''
                                    except (KeyError, IndexError, TypeError):
                                        _rec_scl['shelfmark'] = ''
                                    _rec_scl['content'] = _content_scl
                                    _orig_m_scl = _regex_scl.search(_content_scl)
                                    _ms_match_scl = _orig_m_scl or _regex_scl.search(_match_content_scl)
                                    _rec_scl['matches'].append(_ms_match_scl.span())
                                    _rec_scl['src_indices'].update(
                                        range(_token_idx_scl, _token_idx_scl + chunk_size)
                                    )
                                    _rec_scl['patterns'].add(_regex_scl.pattern)
                                    if _chunk_crossed_scl:
                                        _rec_scl['boundary_chunk_scores'].append(_score_scl)
                                        _rec_scl['crossed_boundaries'].update(_chunk_crossed_scl)
                                    try:
                                        _ms_s_scl, _ms_e_scl = _ms_match_scl.span()
                                        _snip_s_scl = max(0, _ms_s_scl - 60)
                                        _snip_e_scl = min(len(_content_scl), _ms_e_scl + 60)
                                        _ms_snip_scl = (
                                            _content_scl[_snip_s_scl:_ms_s_scl]
                                            + f"*{_content_scl[_ms_s_scl:_ms_e_scl]}*"
                                            + _content_scl[_ms_e_scl:_snip_e_scl]
                                        )
                                    except Exception:
                                        _ms_snip_scl = ''
                                    _seen_scl = _rec_scl.setdefault('_chunk_hit_keys', {})
                                    _key_scl = (_i_scl, _ms_snip_scl)
                                    _existing_scl = _seen_scl.get(_key_scl)
                                    _new_score_scl = float(_score_scl)
                                    if _existing_scl is None:
                                        _seen_scl[_key_scl] = len(_rec_scl['chunk_hits'])
                                        _rec_scl['chunk_hits'].append(
                                            (_i_scl, ' '.join(_chunk_scl), _new_score_scl, _ms_snip_scl)
                                        )
                                    elif _new_score_scl > _rec_scl['chunk_hits'][_existing_scl][2]:
                                        _rec_scl['chunk_hits'][_existing_scl] = (
                                            _i_scl, ' '.join(_chunk_scl), _new_score_scl, _ms_snip_scl
                                        )
                        except Exception:
                            pass
            except Exception as _scl_exc:
                LAB_LOGGER.warning(
                    "search_composition_logic: LOCAL (regular-index) scan failed: %r", _scl_exc
                )

        # 3. Build results with Wide Context
        def build_items(hits_dict):
            final_items = []

            for uid, data in hits_dict.items():
                src_indices = sorted(list(data['src_indices']))
                src_snippets = []

                if src_indices:
                    # A. Group nearby indices
                    clusters = []
                    if src_indices:
                        curr_cluster = [src_indices[0]]
                        for idx in src_indices[1:]:
                            if idx - curr_cluster[-1] < 60:
                                curr_cluster.append(idx)
                            else:
                                clusters.append(curr_cluster)
                                curr_cluster = [idx]
                        clusters.append(curr_cluster)

                    # B. Build text for each cluster - preserve original formatting
                    for cl in clusters:
                        start_ctx = max(0, cl[0] - 200)
                        end_ctx = min(len(tokens), cl[-1] + 201)

                        cl_set = set(cl)

                        # Get character positions from token_positions
                        char_start = token_positions[start_ctx][0]
                        char_end = token_positions[end_ctx - 1][1]

                        # Extract original text with formatting preserved
                        original_snippet = full_text[char_start:char_end]

                        # Insert highlight markers for matched words (work backwards to preserve positions)
                        # Build list of (offset_in_snippet, word_start, word_end) for matched words
                        highlights = []
                        for k in range(start_ctx, end_ctx):
                            if k in cl_set:
                                word_char_start = token_positions[k][0] - char_start
                                word_char_end = token_positions[k][1] - char_start
                                highlights.append((word_char_start, word_char_end))

                        # Apply highlights in reverse order to preserve positions
                        result = original_snippet
                        for word_start, word_end in reversed(highlights):
                            result = result[:word_start] + '*' + result[word_start:word_end] + '*' + result[word_end:]

                        src_snippets.append(result)

                spans = sorted(data['matches'], key=lambda x: x[0])
                merged = []
                if spans:
                    curr_s, curr_e = spans[0]
                    for s, e in spans[1:]:
                        if s <= curr_e + 20: curr_e = max(curr_e, e)
                        else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
                    merged.append((curr_s, curr_e))

                base_score = sum(e-s for s,e in merged)

                # Calculate boundary match quality and final score
                boundary_chunk_scores = data.get('boundary_chunk_scores', [])
                has_boundary_matches = len(boundary_chunk_scores) > 0
                boundary_quality = calculate_boundary_quality(boundary_chunk_scores)

                # Apply score boost in combined mode
                if boundary_mode == 'combined' and has_boundary_matches:
                    final_score = calculate_final_score_with_boost(
                        base_score, boundary_quality, has_boundary_matches, boundary_boost
                    )
                else:
                    final_score = base_score

                # Calculate normalized boundary quality
                boundary_quality_normalized = 0.0
                if has_boundary_matches and base_score > 0:
                    boundary_quality_normalized = min(boundary_quality / base_score, 1.0)

                ms_snips = []
                for s, e in merged:
                    start = max(0, s - 60); end = min(len(data['content']), e + 60)
                    fragment = data['content'][start:s] + \
                               f"*{data['content'][s:e]}*" + \
                               data['content'][e:end]
                    ms_snips.append(fragment)

                combined_pattern = "|".join(list(data['patterns'])) if data.get('patterns') else ""

                final_items.append({
                    'score': base_score,
                    'final_score': final_score,
                    'uid': uid,
                    'raw_header': data['head'],
                    'src_lbl': data['src'],
                    # Phase 110 UAT (Issue 1): per-item shelfmark (filename) for
                    # LOCAL comp hits; empty for Genizah (UI computes Genizah
                    # shelfmark from raw_header via the Genizah meta path).
                    'shelfmark': data.get('shelfmark', ''),
                    'source_ctx': "\n\n".join(src_snippets),
                    'text': "\n...\n".join(ms_snips),
                    'highlight_pattern': combined_pattern,
                    # Boundary metadata
                    'has_boundary_matches': has_boundary_matches,
                    'boundary_match_count': len(data.get('crossed_boundaries', set())),
                    'boundary_quality': boundary_quality_normalized,
                    # chunk_count is derived from unique chunk_hits contents,
                    # not raw Tantivy hits, so repeated source phrases and
                    # cross-segment duplicates don't inflate the user-facing
                    # `min chunks` filter. chunk_hits remains the list of
                    # (chunk_index, chunk_text, score, ms_snip) tuples for
                    # serialize_parallels_payload matches[].
                    'chunk_count': _count_unique_chunks(data.get('chunk_hits', [])),
                    'chunk_hits': data.get('chunk_hits', []),
                    # Filtering flag and reason
                    'is_filtered': data.get('is_filtered', False),
                    'filter_reason': data.get('filter_reason', '')
                })

            # Sort by final_score in combined mode, otherwise by base score
            if boundary_mode == 'combined':
                final_items.sort(key=lambda x: x.get('final_score', x['score']), reverse=True)
            else:
                final_items.sort(key=lambda x: x['score'], reverse=True)

            return final_items

        # Build all items from single map, then separate by filter status
        all_items = build_items(doc_hits)

        # Apply boundary mode filtering first
        if boundary_mode == 'boundary':
            all_items = [item for item in all_items if item.get('has_boundary_matches', False)]

        # Apply min_boundary_matches filter
        # In 'full' mode, this acts as min chunk hits (total matching chunks per document)
        # In 'boundary'/'combined' modes, this filters on actual boundary crossings
        if min_boundary_matches > 0:
            if boundary_mode == 'full':
                all_items = [item for item in all_items if item.get('chunk_count', 0) >= min_boundary_matches]
            else:
                all_items = [item for item in all_items if item.get('boundary_match_count', 0) >= min_boundary_matches]

        # Separate into main and filtered lists
        main_list = [item for item in all_items if not item.get('is_filtered', False)]
        filtered_list = [item for item in all_items if item.get('is_filtered', False)]

        return {'main': main_list, 'filtered': filtered_list, 'partial': was_cancelled,
                'boundary_stats': boundary_stats,
                # Phase 110 A2 + Round-2 #4: per-run scope + staleness verdict.
                'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

    def group_pages_by_manuscript(self, pages_list):
        """Aggregate individual page results into manuscript-level items.

        Groups by Codicological Part (Neubauer) when available, otherwise by System ID.
        """
        grouped = defaultdict(list)
        part_info = {}  # Track Part metadata for grouped items

        # 1. Bucket pages by Part ID (if available) or System ID
        for p in pages_list:
            sid, _ = self.meta_mgr.parse_header_smart(p['raw_header'])
            if sid:
                # Check if this folio belongs to a Part
                part_id = self.meta_mgr.get_part_for_folio(sid)
                if part_id:
                    # Group by Part ID
                    grouped[f"PART:{part_id}"].append(p)
                    if part_id not in part_info:
                        part_info[part_id] = {
                            'part_id': part_id,
                            'folios': set()
                        }
                    part_info[part_id]['folios'].add(sid)
                else:
                    grouped[sid].append(p)
            else:
                # Fallback for pages without valid ID (should be rare)
                grouped["UNKNOWN"].append(p)

        manuscripts = []

        for group_key, pages in grouped.items():
            if not pages: continue

            # Filter out continuous document results (sys:/part:) - they use wrong
            # raw_header (first page's header instead of actual match location).
            # Individual page results are more accurate.
            page_results = [p for p in pages if not str(p.get('uid', '')).startswith(('sys:', 'part:'))]
            continuous_results = [p for p in pages if str(p.get('uid', '')).startswith(('sys:', 'part:'))]

            # Use page results if available, otherwise fall back to continuous
            pages = page_results if page_results else continuous_results

            # Deduplicate pages by p_num within this group.
            # Same page can appear multiple times from V0.7 and V0.8.
            p_num_best = {}
            for p in pages:
                _, p_num = self.meta_mgr.parse_header_smart(p['raw_header'])
                if p_num and p_num != "Unknown":
                    if p_num not in p_num_best or p['score'] > p_num_best[p_num]['score']:
                        p_num_best[p_num] = p
                else:
                    # Pages without valid p_num: use uid as fallback key
                    uid_key = f"_uid_{p.get('uid', id(p))}"
                    if uid_key not in p_num_best or p['score'] > p_num_best[uid_key]['score']:
                        p_num_best[uid_key] = p
            pages = list(p_num_best.values())

            # Aggregate Score
            total_score = sum(p['score'] for p in pages)

            # Use the highest-scoring page as representative
            pages.sort(key=lambda x: x['score'], reverse=True)
            rep_page = pages[0]

            # Check if this is a Part grouping
            is_part = group_key.startswith("PART:")
            if is_part:
                part_id = group_key[5:]  # Remove "PART:" prefix
                part_meta = self.meta_mgr.get_part_metadata(part_id)
                folios = self.meta_mgr.get_folios_for_part(part_id) or []

                # Get Part display name
                part_display = self.meta_mgr.codico_mgr.get_part_display_name(part_id)

                manuscript_item = {
                    'type': 'part',
                    'part_id': part_id,
                    'part_display': part_display,
                    'sys_id': folios[0] if folios else None,  # First folio as representative
                    'folios': folios,
                    'score': total_score,
                    'pages': pages,
                    'raw_header': rep_page['raw_header'],
                    'text': rep_page['text'],
                    'source_ctx': rep_page.get('source_ctx', ''),
                    'highlight_pattern': rep_page.get('highlight_pattern', ''),
                    'oxford_title': part_meta.get('title', '') if part_meta else '',
                    'oxford_contents': part_meta.get('contents', '') if part_meta else '',
                }
            else:
                manuscript_item = {
                    'type': 'manuscript',
                    'sys_id': group_key,
                    'score': total_score,
                    'pages': pages,
                    'raw_header': rep_page['raw_header'],
                    'text': rep_page['text'],
                    'source_ctx': rep_page.get('source_ctx', ''),
                    'highlight_pattern': rep_page.get('highlight_pattern', ''),
                    # Phase 110 UAT (Issue 1): carry the representative page's source
                    # label + shelfmark (filename) so the comp UI can render LOCAL
                    # manuscript rows as filename / parent-folder (mirroring regular
                    # search) instead of the Genizah-meta "unknown".
                    'source': rep_page.get('src_lbl', ''),
                    'shelfmark': rep_page.get('shelfmark', ''),
                }
            manuscripts.append(manuscript_item)

        # Sort manuscripts by aggregated score
        manuscripts.sort(key=lambda x: x['score'], reverse=True)
        return manuscripts

    def group_composition_results(self, items, threshold=5, progress_callback=None, status_callback=None, check_cancel=None):
        # 1. Collect IDs for metadata
        ids = []
        for i in items:
            if check_cancel and check_cancel(): return None, None, None
            if i.get('type') == 'manuscript' and i.get('sys_id'):
                ids.append(i['sys_id'])
            else:
                parsed = self.meta_mgr.parse_header_smart(i['raw_header'])
                if parsed and parsed[0]: ids.append(parsed[0])

        if status_callback:
            status_callback(tr("Fetching metadata..."))

        # Phase 110 (D-12 / Round-2 #1): filter LOCAL `97…` sys_ids OUT of the
        # NLI/FJMS metadata fetch. A private LOCAL id is not in csv_bank, so a
        # grouped LOCAL composition run would otherwise reach the NLI network
        # path. LOCAL display data comes only from the primed filepath cache.
        from shared.local_sys_id import is_local_sys_id
        genizah_ids = [sid for sid in ids if sid and not is_local_sys_id(sid)]

        # Load metadata (fast due to previous fix)
        self.meta_mgr.batch_fetch_shelfmarks(genizah_ids, progress_callback=progress_callback)

        if status_callback:
            status_callback(tr("Grouping results..."))

        # 2. Prepare data for sorting
        IGNORE_PREFIXES = {'קטע', 'קטעי', 'גניזה', 'לא', 'מזוהה', 'חיבור', 'פילוסופיה', 'הלכה', 'שירה', 'פיוט', 'מסמך', 'מכתב', 'ספרות', 'סיפורת', 'יפה', 'דרשות', 'פרשנות', 'מקרא', 'בפילוסופיה', 'קטעים', 'וספרות', 'מוסר', 'הגות', 'וחכמת', 'הלשון', 'פירוש', 'תפסיר', 'שרח', 'על', 'ספר', 'כתאב', 'משנה', 'תלמוד'}

        def _get_clean_words(t):
            if not t: return []
            clean = re.sub(r'[^\w]', ' ', t)
            return [w for w in clean.split() if len(w) > 1]

        def _get_signature(title_str):
            words = _get_clean_words(title_str)
            while words and words[0] in IGNORE_PREFIXES: words.pop(0)
            if not words: return None
            # Signature: First two significant words
            return f"{words[0]} {words[1]}" if len(words) >= 2 else words[0]

        # 3. New Grouping Algorithm (Dictionary Based - O(N))
        # Instead of double loop, map all items by signature
        
        groups_map = defaultdict(list)
        wrapped_items = []
        total_items = len(items)

        for idx, item in enumerate(items):
            # Update GUI infrequently to prevent freezing
            if progress_callback and idx % 100 == 0:
                progress_callback(idx, total_items)
            
            if check_cancel and check_cancel(): return None, None, None

            # Extract title
            if item.get('type') == 'manuscript' and item.get('sys_id'):
                sid = item['sys_id']
            else:
                sid, _ = self.meta_mgr.parse_header_smart(item['raw_header'])

            meta = self.meta_mgr.nli_cache.get(sid, {})
            t = meta.get('title', '').strip()
            shelfmark = self.meta_mgr.get_shelfmark_from_header(item['raw_header']) or meta.get('shelfmark', 'Unknown')
            
            sig = _get_signature(t)
            
            w_item = {
                'item': item, 
                'title': t, 
                'signature': sig,
                'shelfmark': shelfmark,
                'grouped': False
            }
            wrapped_items.append(w_item)
            
            if sig:
                groups_map[sig].append(w_item)

        # 4. Filter groups by Threshold
        appendix = defaultdict(list)
        summary = defaultdict(list)

        for sig, group_items in groups_map.items():
            if len(group_items) > threshold:
                # Group large enough - move to appendix
                for w in group_items:
                    w['grouped'] = True
                    appendix[sig].append(w['item'])
                    summary[sig].append(w['shelfmark'])

        # 5. Create Main List (ungrouped)
        main_list = [w['item'] for w in wrapped_items if not w['grouped']]
        
        # Sort by score descending
        main_list.sort(key=lambda x: x['score'], reverse=True)
        
        # Final GUI update
        if progress_callback:
            progress_callback(total_items, total_items)

        return main_list, appendix, summary

    def get_full_text_by_id(self, uid):
        try:
            q = self.index.parse_query(f'unique_id:"{uid}"', ["unique_id"])
            res = self.searcher.search(q, 1)
            if res.hits: return self.searcher.doc(res.hits[0][1])['content'][0]
        except Exception as e:
            LOGGER.warning("Failed to retrieve full text for uid %s: %s", uid, e)
        return None

    def get_full_manuscript(self, sys_id):
        """Fetch ALL pages for a system ID, sorted by page number."""
        browse_map = self._load_browse_map()
        if not browse_map: return []
        
        pages_meta = browse_map.get(sys_id, [])
        if not pages_meta: return []

        full_content = []
        for p in pages_meta:
            text = self.get_full_text_by_id(p['uid'])
            if text:
                parsed = self.meta_mgr.parse_full_id_components(p.get('full_header', ''))
                full_content.append({
                    'p_num': p['p_num'],
                    'text': text,
                    'uid': p['uid'],
                    'full_header': p.get('full_header', ''),
                    'fl_id': parsed.get('fl_id')
                })
        return full_content
        
    def _get_metadata_only_browse_page(self, sys_id, p_num=None, absolute_index=None, next_prev=0):
        """Build a minimal browse result from csv_bank for records with no Tantivy text.

        Phase 86: synthetic (and other metadata-only) records have no Tantivy
        pages, but enriched images can drive pagination at the UI layer. Thread
        the caller's p_num / absolute_index / direction through so Next/Prev
        and combo selection produce a moving target page that the renderer can
        clamp against the image count.
        """
        if not hasattr(self, 'meta_mgr') or not self.meta_mgr:
            return None
        shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark == 'Unknown':
            return None

        if p_num is not None:
            try:
                base_p = max(1, int(p_num))
            except (ValueError, TypeError):
                base_p = 1
        elif absolute_index is not None:
            try:
                base_p = max(1, int(absolute_index) + 1)
            except (ValueError, TypeError):
                base_p = 1
        else:
            base_p = 1

        try:
            bump = int(next_prev or 0)
        except (ValueError, TypeError):
            bump = 0

        target_p = max(1, base_p + bump)

        return {
            'uid': '',
            'p_num': target_p,
            'full_header': '',
            'text': '',
            'total_pages': 0,
            'current_idx': target_p,
            'internal_index': target_p - 1,
            'sys_id': sys_id,
            'metadata_only': True,
        }

    def get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None):
        browse_map = self._load_browse_map()
        if not browse_map:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)

        # Prepare ordered list for cross-manuscript navigation
        if allow_cross and (not hasattr(self, '_ordered_sys_ids') or not self._ordered_sys_ids):
            self._ordered_sys_ids = list(browse_map.keys())

        if sys_id not in browse_map:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)
        all_pages = browse_map[sys_id]
        if not all_pages:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)

        # Filter to specific IE for multi-IE manuscripts
        active_ie = volume_ie
        if volume_ie:
            pages = get_volume_pages(all_pages, volume_ie)
            if not pages:
                # Requested IE not found — fall back to all pages
                LOGGER.warning("get_browse_page: volume_ie=%s not found in %d pages for sys_id=%s. ie_ids in pages: %s",
                               volume_ie, len(all_pages), sys_id,
                               list(set(p.get('ie_id') for p in all_pages[:20])))
                pages = all_pages
                active_ie = None
            else:
                LOGGER.info("get_browse_page: volume_ie=%s filtered to %d/%d pages for sys_id=%s",
                            volume_ie, len(pages), len(all_pages), sys_id)
        else:
            pages = all_pages
        
        target_idx = -1

        # PRIORITY 1: Use Absolute Index if provided (Fixes duplicate page loop)
        if absolute_index is not None:
            if 0 <= absolute_index < len(pages):
                target_idx = absolute_index
            else:
                # If index is invalid, fallback to p_num logic? No, just fail or reset.
                pass 
        
        # PRIORITY 2: Search by p_num (Fallback / Initial Load)
        if target_idx == -1 and p_num is not None:
            # Robust casting
            try: p_val = int(p_num)
            except (ValueError, TypeError): p_val = -999
            
            for i, p in enumerate(pages):
                if p['p_num'] == p_val: 
                    target_idx = i; break
            
            # Smart Fallback: Find closest insertion point
            if target_idx == -1:
                for i, p in enumerate(pages):
                    if p['p_num'] > p_val:
                        target_idx = max(0, i - 1)
                        break
                if target_idx == -1: target_idx = len(pages) - 1

        # PRIORITY 3: Default to start
        if target_idx == -1: target_idx = 0
        
        # Calculate New Index
        new_idx = target_idx + next_prev
        
        # Handle crossing to adjacent manuscripts when requested
        if (new_idx < 0 or new_idx >= len(pages)) and allow_cross and next_prev != 0:
            direction = 1 if next_prev > 0 else -1
            adjacent_id = self.get_adjacent_sys_id_by_file_order(sys_id, direction)
            while adjacent_id:
                if adjacent_id in browse_map and browse_map[adjacent_id]:
                    pages = browse_map[adjacent_id]
                    sys_id = adjacent_id
                    new_idx = 0 if direction > 0 else len(pages) - 1
                    break
                adjacent_id = self.get_adjacent_sys_id_by_file_order(adjacent_id, direction)
            else:
                return None

        if new_idx < 0 or new_idx >= len(pages): return None
        
        target_page = pages[new_idx]
        text = self.get_full_text_by_id(target_page['uid'])
        
        return {
            'uid': target_page['uid'],
            'p_num': target_page['p_num'],
            'full_header': target_page['full_header'],
            'text': text,
            'total_pages': len(pages),
            'current_idx': new_idx + 1, # Display is 1-based
            'internal_index': new_idx,  # 0-based for logic (NEW)
            'sys_id': sys_id,
            'volume_ie': active_ie or target_page.get('ie_id'),
        }

    def get_local_browse_page(self, sys_id, p_num=None, next_prev=0,
                              absolute_index=None, allow_cross=False, volume_ie=None):
        """Phase 96 NEW-2: LOCAL analog to `get_browse_page` (folio nav for LOCAL files).

        Page identity contract (Codex Item 4 / tech-debt D):
          p_num       — physical page number in the source file.  For PDFs this is
                        the real PDF page number; blank/empty pages are skipped by
                        the indexer so the set of p_nums is SPARSE (not contiguous).
                        Example: a 1,600-page PDF with 71 blank pages has indexed
                        p_nums like {1, 2, …, 1529, 1552, …} — NOT 1..1529.
          current_idx — 0-based ordinal in the SORTED indexed page list (dense).
                        Used for prev/next boundary detection only.  The UI spinbox
                        must display p_num, not current_idx.

        Returns `{uid, p_num, full_header, text, total_pages, current_idx,
        internal_index, max_p_num, sys_id}` — same shape as `get_browse_page`
        plus `max_p_num` so the caller can set a meaningful spinbox upper bound
        for sparse files.

        Canonical img read order (tech-debt D): prefer hit['display']['img']
        over hit['img'] for Genizah hits; for LOCAL hits use p_num directly
        (see _build_local_result_dict which writes p_num into display['img']).

        Parameters mirror get_browse_page for drop-in dispatch, but LOCAL
        semantics differ:
          - allow_cross is IGNORED (D-12: no wrap, no cross-file nav)
          - volume_ie is IGNORED (LOCAL files have no volume concept)
          - absolute_index is IGNORED (p_num + current position in sorted page
            list is the authoritative navigation state for LOCAL files)

        Returns None when:
          - sys_id has no LOCAL pages in the index
          - target p_num is not found in the sorted page list (Item 5: no
            silent fallback to page 1 — let the caller preserve its current state)
          - next_prev would land outside [0, total_pages) (D-12: no wrap)

        Note: this method does NOT use Task 1's D-04.1 filter-out semantics.
        Browse navigation must return ALL pages of the file (user navigated
        into a specific file and wants to see ALL its pages, regardless of
        whether the search regex matches each page). Filter-out is search-only.
        """
        if self.local_searcher is None or self.local_index is None:
            return None

        # Cache the sorted page list per sys_id to avoid repeat Tantivy queries
        # on every nav click. Invalidated by reload_local_indexes().
        cache = getattr(self, "_local_pages_cache", None)
        if cache is None:
            cache = {}
            self._local_pages_cache = cache

        pages = cache.get(sys_id)
        if pages is None:
            try:
                q = self.local_index.parse_query(sys_id, ["full_header"])
                res = self.local_searcher.search(q, 5000)
            except Exception as e:
                LOGGER.warning(
                    "get_local_browse_page: parse_query failed for %s: %s", sys_id, e
                )
                return None
            collected = []
            hits = res.hits if hasattr(res, "hits") else res
            for _score, doc_addr in hits:
                try:
                    doc = self.local_searcher.doc(doc_addr)
                    full_header = doc.get_first("full_header") or ""
                    if not full_header.startswith(f"{sys_id}_LOCAL_P"):
                        continue
                    content = doc.get_first("content") or ""
                    uid = doc.get_first("unique_id") or ""
                    try:
                        p_str = full_header.split("_LOCAL_P")[1].split("_F")[0]
                        pn = int(p_str)
                    except (ValueError, IndexError):
                        continue
                    collected.append({
                        "p_num": pn,
                        "full_header": full_header,
                        "text": content,
                        "uid": uid,
                    })
                except (KeyError, IndexError, TypeError):
                    continue
            collected.sort(key=lambda x: x["p_num"])
            pages = collected
            cache[sys_id] = pages

        if not pages:
            return None

        # max_p_num: the highest physical page number in the sorted list.
        # For sparse PDFs this is the real last-page number (e.g. 1600), NOT
        # len(pages) (e.g. 1529). Used by the caller to set spinbox maximum.
        max_p_num = pages[-1]["p_num"]

        # Determine target index.
        if p_num is None and next_prev == 0:
            target_idx = 0
        elif p_num is not None:
            # Find the current p_num in the sorted page list, then apply offset.
            # Item 5: if p_num is not in the list, return None — do NOT silently
            # fall back to page 1.  The UI caller keeps its existing spinner value.
            found_idx = next(
                (i for i, pg in enumerate(pages) if pg["p_num"] == p_num), None
            )
            if found_idx is None:
                LOGGER.debug(
                    "get_local_browse_page: p_num=%s not in index for %s — returning None",
                    p_num, sys_id,
                )
                return None
            target_idx = found_idx + next_prev
        else:
            target_idx = 0 + next_prev

        if target_idx < 0 or target_idx >= len(pages):
            return None  # D-12: no wrap at boundaries

        target = pages[target_idx]
        return {
            "uid": target["uid"],
            "p_num": target["p_num"],
            "full_header": target["full_header"],
            "text": target["text"],
            "total_pages": len(pages),
            "current_idx": target_idx + 1,    # 1-based ordinal (UI display)
            "internal_index": target_idx,      # 0-based ordinal (boundary logic)
            "max_p_num": max_p_num,            # highest physical page number
            "sys_id": sys_id,
        }

    def get_browse_page_by_fl(self, fl_id, sys_id=None):
        browse_map = self._load_browse_map()
        if not browse_map: return None

        if not fl_id:
            return None

        fl_digits = re.sub(r"\D", "", str(fl_id))
        if not fl_digits:
            return None

        def _build_fl_result(sid, all_pages, page, idx):
            """Build result dict with volume-aware page count and IE info."""
            text = self.get_full_text_by_id(page['uid'])
            ie_id = page.get('ie_id') or _extract_ie_from_header(page.get('full_header', ''))
            # For multi-IE manuscripts, filter to the page's IE for correct page count
            if ie_id:
                volume_pages = get_volume_pages(all_pages, ie_id)
                if volume_pages:
                    # Recompute index within the volume's pages
                    vol_idx = next((i for i, p in enumerate(volume_pages) if p['uid'] == page['uid']), 0)
                    return {
                        'uid': page['uid'],
                        'p_num': page['p_num'],
                        'full_header': page['full_header'],
                        'text': text,
                        'total_pages': len(volume_pages),
                        'current_idx': vol_idx + 1,
                        'sys_id': sid,
                        'fl_id': fl_digits,
                        'volume_ie': ie_id,
                    }
            return {
                'uid': page['uid'],
                'p_num': page['p_num'],
                'full_header': page['full_header'],
                'text': text,
                'total_pages': len(all_pages),
                'current_idx': idx + 1,
                'sys_id': sid,
                'fl_id': fl_digits,
                'volume_ie': ie_id,
            }

        # Try O(1) index lookup first
        if self._fl_id_index is not None:
            candidates = self._fl_id_index.get(fl_digits, [])
            # If sys_id is known, filter to that sys_id
            if sys_id and candidates:
                candidates = [(sid, idx) for sid, idx in candidates if sid == sys_id]
            if candidates:
                sid, idx = candidates[0]
                if sid in browse_map and idx < len(browse_map[sid]):
                    page = browse_map[sid][idx]
                    return _build_fl_result(sid, browse_map[sid], page, idx)

        # Fallback: linear scan (index not yet ready or FL ID not found in index)
        sys_candidates = [sys_id] if sys_id else list(browse_map.keys())

        for sid in sys_candidates:
            if sid not in browse_map:
                continue
            pages = browse_map[sid]
            for idx, page in enumerate(pages):
                parsed = self.meta_mgr.parse_full_id_components(page.get('full_header', ''))
                page_fl = re.sub(r"\D", "", str(parsed.get('fl_id') or ""))
                if page_fl and page_fl == fl_digits:
                    return _build_fl_result(sid, pages, page, idx)
        return None

    def get_adjacent_sys_id_by_file_order(self, current_sys_id, offset):
        """
        Returns the next/prev system ID based on the order in Transcriptions.txt.
        This relies on browse_map preserving insertion order.
        """
        # Load map if not already cached in memory for navigation
        if not hasattr(self, '_ordered_sys_ids') or not self._ordered_sys_ids:
            if not os.path.exists(Config.BROWSE_MAP):
                return None
            with open(Config.BROWSE_MAP, 'rb') as f:
                b_map = pickle.load(f)
                # list(dict.keys()) returns items in insertion order (File Order)
                self._ordered_sys_ids = list(b_map.keys())

        if not current_sys_id:
            return self._ordered_sys_ids[0] if self._ordered_sys_ids else None

        try:
            # Find current index
            curr_idx = self._ordered_sys_ids.index(current_sys_id)
            new_idx = curr_idx + offset
            
            # Check bounds
            if 0 <= new_idx < len(self._ordered_sys_ids):
                return self._ordered_sys_ids[new_idx]
        except ValueError:
            pass # Current ID not found in list
            
        return None

def calculate_smart_weights(file_path, sample_size=None):
    """
    Analyzes corpus to generate HTR-aware letter frequency weights.
    Robust version: Tries multiple encodings to ensure file reading.
    """
    size_desc = "ALL lines" if sample_size is None else f"{sample_size} lines"
    LAB_LOGGER.info(f"Calculating smart weights from {file_path} (Sample: {sample_size})...")
    
    total_letters = 0
    counts = defaultdict(int)
    
    encodings_to_try = ['utf-8-sig', 'utf-8', 'windows-1255', 'iso-8859-8', 'latin-1']
    
    file_read_success = False
    
    for enc in encodings_to_try:
        try:
            temp_counts = defaultdict(int)
            temp_total = 0
            
            with open(file_path, 'r', encoding=enc) as f:
                for i, line in enumerate(f):
                    if sample_size is not None and i >= sample_size: 
                        break
                    if line.startswith("==>") or line.startswith("###"): continue
                    
                    text = re.sub(r"[^\u0590-\u05FF]", "", line)
                    if not text: continue
                    
                    for char in text:
                        temp_counts[char] += 1
                        temp_total += 1
            
            if temp_total > 0:
                counts = temp_counts
                total_letters = temp_total
                file_read_success = True
                LAB_LOGGER.info(f"Successfully read corpus using encoding: {enc}")
                break
                
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            LAB_LOGGER.error(f"Error reading file with {enc}: {e}")
            break

    if not file_read_success or total_letters == 0:
        LAB_LOGGER.error("Failed to read corpus with any encoding or file is empty. Using static weights.")
        return HEBREW_FREQ
    
    # 2. Analyze & Score
    analysis_rows = []
    final_scores = {}

    for char, count in counts.items():
        if char not in STANDARD_HEBREW_DIST: continue
        
        corpus_pct = (count / total_letters) * 100
        standard_pct = STANDARD_HEBREW_DIST.get(char, 0.1)
        
        ratio = corpus_pct / standard_pct
        score = (1 / corpus_pct) if corpus_pct > 0 else 100.0
        
        original_score = score
        if ratio > 1.5:
            score = score / (ratio ** 2)
            
        final_scores[char] = score
        
        analysis_rows.append({
            'Letter': char,
            'Standard_Pct': round(standard_pct, 4),
            'Corpus_Pct': round(corpus_pct, 4),
            'Ratio_Suspicion': round(ratio, 2),
            'Original_Score': round(original_score, 4),
            'Penalized_Score': round(score, 4)
        })

    # 3. Save Report
    LAB_LOGGER.info(f"DEBUG: Preparing to save HTR report with {len(analysis_rows)} rows...") # <--- שורה חדשה
    try:
        os.makedirs(Config.REPORTS_DIR, exist_ok=True)
        report_path = os.path.join(Config.REPORTS_DIR, "HTR_Frequency_Analysis.csv")
        
        with open(report_path, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['Letter', 'Standard_Pct', 'Corpus_Pct', 'Ratio_Suspicion', 'Original_Score', 'Penalized_Score']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            sorted_rows = sorted(analysis_rows, key=lambda x: x['Ratio_Suspicion'], reverse=True)
            writer.writerows(sorted_rows)
            
        LAB_LOGGER.info(f"SUCCESS: HTR Report saved to: {report_path}") # <--- שורה חדשה לאישור הצלחה
        
    except Exception as e:
        LAB_LOGGER.warning(f"Failed to save HTR report: {e}")

    # 4. Normalize to Ranks
    LAB_LOGGER.info("DEBUG: Normalizing scores to integer ranks...") # <--- שורה חדשה
    sorted_chars = sorted(final_scores.keys(), key=lambda x: final_scores[x], reverse=True)
    rank_map = {}
    max_rank = len(sorted_chars)
    for i, char in enumerate(sorted_chars):
        rank_map[char] = max_rank - i
        
    # 5. Save JSON
    try:
        os.makedirs(Config.LAB_DIR, exist_ok=True)
        LAB_LOGGER.info(f"DEBUG: Saving JSON weights to {Config.LAB_WEIGHTS_FILE}...") # <--- שורה חדשה
        
        with open(Config.LAB_WEIGHTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(rank_map, f, ensure_ascii=False, indent=2)
            
        LAB_LOGGER.info(f"SUCCESS: Lab weights JSON saved successfully.") # <--- שורה חדשה לאישור הצלחה
        
    except Exception as e:
        LAB_LOGGER.error(f"Failed to save lab weights JSON: {e}")

    return rank_map
