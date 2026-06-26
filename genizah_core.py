"""Core search, indexing, metadata, and AI helpers for the Genizah project."""

# -*- coding: utf-8 -*-
# genizah_core.py
import logging
import os
import re
import pickle
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
import json

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




try:
    import tantivy  # noqa: F401
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

# Phase 125: lab_settings extracted — permanent compat facade (v8.3.0)
from shared.lab_settings import LabSettings  # noqa: F401


# Phase 125: lab_engine extracted — permanent compat facade (v8.3.0)
from shared.lab_engine import LabEngine  # noqa: F401

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
    import tantivy  # noqa: F401
except ImportError:
    raise ImportError(tr("Tantivy library missing. Please install it."))




# Phase 125: search_engine extracted — permanent compat facade (v8.3.0)
from shared.search_engine import (  # noqa: F401
    # SearchEngine class
    SearchEngine,
    # Pre-cluster: RRF + Responsa regex helpers
    RRF_K,
    _make_flex_spacing_pattern,
    _build_wildcard_regex,
    _add_bracket_variants,
    _query_has_brackets,
    _strip_brackets,
    # SEED-006 compat gate helpers
    _index_has_field,
    content_search_staleness_messages,
    MARK_TOLERANT_INSERTER,
    make_mark_tolerant_pattern,
    # Composition helper
    _count_unique_chunks,
    # Chunk plan dataclasses
    _ChunkPlan,
    _LabChunkPlan,
    # Responsa downgrade thread-local channel (6 names)
    _LAST_RESPONSA_DOWNGRADE,
    _LAST_RESPONSA_DOWNGRADE_META,
    _set_last_responsa_downgrade,
    _consume_last_responsa_downgrade,
    _set_last_responsa_downgrade_meta,
    _consume_last_responsa_downgrade_meta,
)

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
