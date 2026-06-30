# -*- coding: utf-8 -*-
"""Browse-map utilities, shelfmark normalisation, and library-code helpers.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains permanent same-object re-export shims so all
existing ``from genizah_core import …`` callers continue working unchanged.
"""

import json
import logging
import os
import re

from genizah_translations import LIBRARY_CODES_HE
from shared.config import Config

LOGGER = logging.getLogger("genizah." + __name__)


# ==============================================================================
#  LIBRARY CODE MAPPINGS
# ==============================================================================
LIBRARY_CODES = {
    'CUL': 'Cambridge University Library',
    'JTS': 'The Jewish Theological Seminary of America',
    'RNL': 'The National Library of Russia',
    'Oxford': 'The Bodleian Libraries, University of Oxford',
    'Manchester': 'The University of Manchester Library',
    'BL': 'The British Library',
    'AIU': 'Alliance Israélite Universelle',
    'Westminster': 'Westminster College',
    'Freer': 'Freer Gallery of Art',
    'Mosseri': 'Mosseri Collection',
    'Gaster': 'Gaster Collection',
    'Katz': 'Katz Center',
    'Halper': 'Halper Catalogue',
    'HUC': 'Hebrew Union College Library',
    'HAS': 'Library of the Hungarian Academy of Sciences',
    'Vienna': 'Austrian National Library',
    'Strasbourg': 'National and University Library of Strasbourg',
    'InstFrance': 'Library of the Institute of France',
    'Warsaw': 'Jewish Community of Warsaw',
    'ASL': 'Academy of Sciences and Literature',
    'Schoeyen': 'Schoeyen Collection',
    'Harkavy': 'Harkavy Collection',
    'Combs': 'Combs Collection',
    'Lehnardt': 'Lehnardt Collection',
    'Allony': 'Allony Collection',
    'Boesky': 'Boesky Collection',
    'Bisno': 'Bisno Collection',
    'UPenn': 'University of Pennsylvania',
    'BnF': 'Bibliothèque nationale de France',
    # Additional libraries
    'Toronto': 'University of Toronto Library',
    'Dropsie': 'Dropsie College',
    'Princeton': 'Princeton University Library',
    'Columbia': 'Columbia University Library',
    'Harvard': 'Harvard University Library',
    'Yale': 'Yale University Library',
    'Rylands': 'John Rylands Library',
    # European libraries
    'Senckenberg': 'University Library Johann Christian Senckenberg (Frankfurt)',
    'Geneva': 'Library of Geneva',
    'Munich': 'Bavarian State Library (Munich)',
    'BNF': 'National Library of France',
    'RSL': 'Russian State Library',
    'SBB': 'State Library of Berlin',
    'Birmingham': 'University of Birmingham Library',
    'Heidelberg': 'Heidelberg University Library',
    'Turin': 'Oriental Studies Library, Turin',
    'Basel': 'Basel University Library',
    'IOM': 'Institute of Oriental Manuscripts (St. Petersburg)',
    'Leeds': 'Leeds University Library',
    'Chetham': "Chetham's Library (Manchester)",
    'Wellcome': 'Wellcome Library',
    'TCD': 'Trinity College Dublin',
    # Israeli institutions
    'TAU': 'Tel Aviv University Library',
    'Haifa': 'University of Haifa Library',
    'BenZvi': 'Ben Zvi Institute',
    'Schocken': 'Schocken Institute for Jewish Research',
    'BarIlan': 'Bar-Ilan University Library',
    'NLI': 'National Library of Israel',
    # North American libraries
    'UChicago': 'University of Chicago Library',
    'McGill': 'McGill University Library',
    'Duke': 'Duke University Libraries',
    'YU': 'Yeshiva University Library',
    'UMich': 'University of Michigan Library',
    # Collections and foundations
    'Sassoon': 'Sassoon Collection',
    'Wallach': 'Wallach Collection',
    'Lutzki': 'Lutzki Collection',
    'Adler': 'Adler Collection',
    'Lehmann': 'Manfred and Anne Lehmann Foundation',
    'JCBerlin': 'Jewish Community of Berlin',
    'JCErfurt': 'Jewish Community of Erfurt',
    'AllonyLoew': 'Allony-Loewinger Catalogue',
    'AllonyKupf': 'Allony-Kupfer Catalogue',
    'Benayahu': 'Benayahu Collection',
    'Nahum': 'Yehuda Nahum Collection',
    'Salmon': 'Chava Salmon Collection',
    'Sofer': 'David Sofer Collection',
    'Shapira': 'Bernard Shapira Collection',
    'Weiss': 'Steve Weiss Collection',
    'Karp': 'Abraham Karp Collection',
    'Goldsmith': 'Goldsmith Museum',
    'SOS': 'Separated Orthodox Society',
    'MotB': 'Museum of the Bible',
    # Phase 53: New codes for FIST gap records
    'Solomon': 'Solomon Halberstam Collection',
    'Reinach': 'Reinach Collection',
    'Vatican': 'Vatican Library',
    'Mehlman': 'Mehlman Collection',
    'CentralArch': 'Central Archives for the History of the Jewish People',
    'JCMainz': 'Jewish Community of Mainz',
    'Corwin': 'Corwin Collection',
    # Phase 95 D-13 — My Library namespace (LOCAL sys_ids start with 97).
    'LOCAL': 'My Library',
}


def library_codes_with_manuscripts() -> frozenset:
    """Return the set of library_code values that have at least one manuscript record.

    Reads ``libraries.csv`` (column 3 per CLAUDE.md), caches the result at module level
    so repeated calls are free.  Fail-open: if the CSV is unavailable or unreadable,
    returns the full ``frozenset(LIBRARY_CODES)`` so the filter never silently empties.

    Returns a ``frozenset`` so callers cannot mutate the cached result (Codex LOW fix).
    ``c in <frozenset>`` membership tests work identically to a regular set.

    Phase 131 note: reusable by catalog/parallels/desktop — keep import-safe (no web/
    or desktop/ imports here).
    """
    if library_codes_with_manuscripts._cache is not None:
        return library_codes_with_manuscripts._cache

    csv_path = Config.LIBRARIES_CSV
    if not os.path.exists(csv_path):
        LOGGER.warning(
            "library_codes_with_manuscripts: %s not found — fail-open (returning full set)",
            csv_path,
        )
        library_codes_with_manuscripts._cache = frozenset(LIBRARY_CODES)
        return library_codes_with_manuscripts._cache

    import csv as _csv
    found: set = set()
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as _f:
            reader = _csv.reader(_f)
            next(reader, None)  # skip header
            for row in reader:
                if not row or row[0].startswith('#'):
                    continue
                if len(row) > 3:
                    code = row[3].strip()
                    if code:
                        found.add(code)
    except Exception as exc:
        LOGGER.warning(
            "library_codes_with_manuscripts: failed to read %s (%s) — fail-open",
            csv_path, exc,
        )
        library_codes_with_manuscripts._cache = frozenset(LIBRARY_CODES)
        return library_codes_with_manuscripts._cache

    # Intersect with known canonical codes so stale/typo values from the CSV don't leak.
    result = found & set(LIBRARY_CODES)
    if not result:
        LOGGER.warning(
            "library_codes_with_manuscripts: intersection is empty — fail-open (returning full set)"
        )
        result = set(LIBRARY_CODES)
    library_codes_with_manuscripts._cache = frozenset(result)
    LOGGER.debug("library_codes_with_manuscripts: %d codes with manuscripts", len(result))
    return library_codes_with_manuscripts._cache


library_codes_with_manuscripts._cache = None  # type: ignore[attr-defined]


def sanitize_library_codes(raw) -> list:
    """Return a clean list of canonical library codes from an untrusted value.

    Accepts any value; non-list input returns [].
    Keeps only str items that are in LIBRARY_CODES and != 'LOCAL'.
    Order-preserving; deduplication not required by the caller.

    Used at every restore / Apply-handler entry point in web/pages/search.py
    so malformed persisted values (int, dict-items, stray strings) never reach
    the filter logic and cause a TypeError.  The ``c != 'LOCAL'`` guard is kept
    literal here (not hidden behind a variable) so the AST guard in
    tests/test_web_library_options_no_local.py can detect it.
    """
    if not isinstance(raw, list):
        return []
    return [c for c in raw if isinstance(c, str) and c in LIBRARY_CODES and c != 'LOCAL']


def normalize_shelfmark(shelfmark: str) -> str:
    """
    Normalize shelfmarks for consistent matching across the codebase.

    This is the CANONICAL implementation - all other normalizations should use this.

    Rules:
    - Convert to lowercase
    - Treat "/" as "." for consistency (192/23 -> 192.23)
    - Preserve dots between digits (e.g., "12.123" stays as "12.123")
    - Remove all other non-alphanumeric characters
    - Remove "MS" or "Ms." prefix (common in Oxford shelfmarks)

    Examples:
        "T-S 12.123" -> "ts12.123"
        "MS. Heb. a.1" -> "heba1"
        "T-S  K  25/2" -> "tsk25.2"
        "120.2" -> "120.2"
    """
    if not shelfmark:
        return ""

    # Treat "/" as "." for consistency (192/23 -> 192.23)
    temp = shelfmark.replace('/', '.')

    # Preserve dots that appear between digits (like 120.2) by replacing with a marker
    temp = re.sub(r'(\d)\.(\d)', r'\1DOTMARKER\2', temp)

    # Remove all other non-alphanumeric characters
    cleaned = re.sub(r'\W+', '', temp).casefold()

    # Restore the preserved dots
    cleaned = cleaned.replace('dotmarker', '.')

    # Remove "ms" prefix (common in Oxford: "MS. Heb. a.1")
    if cleaned.startswith("ms"):
        cleaned = cleaned[2:]

    # Normalize RNL "Yevr." prefix to "EVR" (FIST uses Yevr., CSV uses EVR)
    if cleaned.startswith("yevr"):
        cleaned = "evr" + cleaned[4:]
    # Normalize CAJS "Halper" to "Genizah" (FIST uses Halper, CSV uses Genizah prefix)
    if cleaned.startswith("halper") and not cleaned.startswith("halpern"):
        cleaned = "genizah" + cleaned[6:]
    # Normalize JTS "ENA-MS" / "ENA MS" to "ENA" (CSV uses "ENA 2956", users type "ENA-MS 2956")
    if cleaned.startswith("enams"):
        cleaned = "ena" + cleaned[5:]

    return cleaned


def natural_sort_key(text):
    """Sort strings containing numbers naturally (e.g. 'Item 2' < 'Item 10')."""
    normalized = re.sub(r'^\s*ms\.?\s*', '', text or "", flags=re.IGNORECASE)
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', normalized)]


def get_library_display(code: str, short: bool = True, lang: str = None,
                        with_code: bool = False) -> str:
    """Return library name for display.

    Args:
        code: Library code (e.g., 'CUL', 'JTS')
        short: If True, return code; if False, return full name
        lang: Language code ('he', 'en', or None for auto-detect from CURRENT_LANG)
        with_code: If True and short=False, append ' ({code})' after the resolved name
            (e.g. 'ספריית האוניברסיטה של קיימברידג' (CUL)').  Default False so ALL
            existing callers are byte-identical — only catalog filter-dialog row builders
            pass True. No-op when short=True (short returns the bare code already).

    Returns:
        Display string for the library
    """
    if not code:
        return ''
    if short:
        return code
    effective_lang = lang
    if not effective_lang:        # falsy: None OR "" — matches live `lang if lang else CURRENT_LANG`
        from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
        effective_lang = CURRENT_LANG
    if effective_lang == 'he':
        name = LIBRARY_CODES_HE.get(code, LIBRARY_CODES.get(code, code))
    else:
        name = LIBRARY_CODES.get(code, code)
    if with_code:
        return f"{name} ({code})"
    return name


# ── Library prefix stripping for shelfmark lookup ────────────────────────────

# Common short names / abbreviations that users type before a shelfmark.
# Ordered longest-first so "Cambridge University Library" matches before "Cambridge".
_LIBRARY_PREFIX_ALIASES = None  # Built lazily


def _get_library_prefix_aliases():
    """Build sorted list of (lowercase_prefix, ) for library name stripping."""
    global _LIBRARY_PREFIX_ALIASES
    if _LIBRARY_PREFIX_ALIASES is not None:
        return _LIBRARY_PREFIX_ALIASES
    prefixes = set()
    # Library codes (CUL, JTS, BL, etc.)
    for code in LIBRARY_CODES:
        prefixes.add(code.lower())
    # Full library names from LIBRARY_CODES values
    for name in LIBRARY_CODES.values():
        prefixes.add(name.lower())
    # Common short aliases users are likely to type
    _extra = [
        'cambridge', 'oxford', 'manchester', 'british library', 'bodleian',
        'rylands', 'john rylands', 'national library of russia',
        'jewish theological seminary', 'jts library',
        'alliance israélite', 'alliance israelite',
        'hebrew union college', 'university of pennsylvania',
        'princeton', 'columbia', 'harvard', 'yale',
        'national library of israel', 'bnf', 'bibliothèque nationale',
        'state library of berlin', 'bavarian state library',
        'ben zvi', 'bar ilan', 'tel aviv university',
        'schocken', 'sassoon', 'adler',
    ]
    for a in _extra:
        prefixes.add(a.lower())
    # Sort longest-first so "Cambridge University Library" matches before "Cambridge"
    _LIBRARY_PREFIX_ALIASES = sorted(prefixes, key=len, reverse=True)
    return _LIBRARY_PREFIX_ALIASES


def _strip_library_prefix(query: str) -> str:
    """Strip a leading library name/code prefix from a shelfmark query.

    Examples:
        "Cambridge T-S 12.123"  -> "T-S 12.123"
        "British Library Or 5557B" -> "Or 5557B"
        "CUL T-S AS 31.1" -> "T-S AS 31.1"
    """
    if not query:
        return query
    q_lower = query.lower()
    for prefix in _get_library_prefix_aliases():
        if q_lower.startswith(prefix):
            rest = query[len(prefix):]
            # Must be followed by whitespace or punctuation (not part of shelfmark)
            if rest and rest[0] in (' ', ',', ':', '-', '|', '\t'):
                return rest.lstrip(' ,:|:\t-')
            elif rest == '':
                return ''  # query was just the library name
    return query


def _load_ie_volume_map():
    """Load IE volume map from JSON file (cached after first call).

    Returns: {sys_id: {"primary_ie": str, "volumes": [{"ie_id", "suffix", "page_count"}, ...]}}
    Falls back to primary_ie_map.json for backward compatibility.
    """
    if not hasattr(_load_ie_volume_map, '_cache'):
        # Try ie_volume_map.json first (new format with full IE→suffix mapping)
        vol_path = os.path.join(Config.INTERNAL_DIR, "ie_volume_map.json")
        if os.path.exists(vol_path):
            try:
                with open(vol_path, 'r', encoding='utf-8') as f:
                    _load_ie_volume_map._cache = json.load(f)
                return _load_ie_volume_map._cache
            except Exception as e:
                LOGGER.debug('Could not load ie_volume_map.json: %s', e)

        # Fallback: primary_ie_map.json (old format — primary IE only)
        map_path = os.path.join(Config.INTERNAL_DIR, "primary_ie_map.json")
        if os.path.exists(map_path):
            try:
                with open(map_path, 'r', encoding='utf-8') as f:
                    old_map = json.load(f)
                # Convert old format to new format
                converted = {}
                for sid, entry in old_map.items():
                    volumes = []
                    for idx, (ie_id, count) in enumerate(
                        sorted(entry.get("all_ies", {}).items(),
                               key=lambda x: (0 if x[0] == entry.get("primary_ie") else 1, -x[1]))
                    ):
                        volumes.append({"ie_id": ie_id, "suffix": idx + 1, "page_count": count})
                    converted[sid] = {
                        "primary_ie": entry.get("primary_ie", ""),
                        "volumes": volumes,
                    }
                _load_ie_volume_map._cache = converted
            except Exception as e:
                LOGGER.debug('ie_volume_map.json parse failed, using empty cache: %s', e)
                _load_ie_volume_map._cache = {}
        else:
            _load_ie_volume_map._cache = {}
    return _load_ie_volume_map._cache


def _extract_ie_from_header(full_header):
    """Extract IE identifier from a browse_map entry's full_header."""
    m = re.search(r'(IE\d+)', full_header or '')
    return m.group(1) if m else None


def _repair_missing_ie_pages(browse_map):
    """Repair browse_map for multi-IE manuscripts where pages from non-primary IEs
    were lost by pre-Phase-58 dedup (which deduped by p_num across IEs).

    Also fixes uid format for pages that were repaired with full-header uids
    (should be IE_P_FL format to match Tantivy index keys).

    Reads Transcriptions.txt headers to find missing IE pages and re-adds them.
    Returns (repaired_map, repair_count) where repair_count is the number of
    manuscripts that had pages restored or uid-fixed.
    """
    ie_volume_map = _load_ie_volume_map()
    if not ie_volume_map:
        return browse_map, 0

    # Phase 1: Fix uid format for already-repaired pages (full-header → IE_P_FL)
    uid_fix_count = 0
    uid_re = re.compile(r'(IE\d+_P\d+_FL\d+)')
    for sid in ie_volume_map:
        if sid not in browse_map:
            continue
        for page in browse_map[sid]:
            uid = page.get('uid', '')
            # If uid starts with sys_id prefix, it was repaired with wrong format
            if uid.startswith(sid + '_'):
                m = uid_re.search(uid)
                if m:
                    page['uid'] = m.group(1)
                    uid_fix_count += 1

    if uid_fix_count:
        LOGGER.info("Fixed %d browse_map page uids (removed sys_id prefix)", uid_fix_count)

    transcriptions_path = Config.FILE_V8
    if not os.path.exists(transcriptions_path):
        return browse_map, uid_fix_count

    # Phase 2: Find multi-IE manuscripts with missing IEs in browse_map
    needs_repair = {}  # {sid: set of expected ie_ids not in browse_map}
    for sid, vol_info in ie_volume_map.items():
        if sid not in browse_map:
            continue
        expected_ies = {v['ie_id'] for v in vol_info.get('volumes', [])}
        existing_ies = {_extract_ie_from_header(p.get('full_header', '')) for p in browse_map[sid]}
        missing = expected_ies - existing_ies
        if missing:
            needs_repair[sid] = missing

    if not needs_repair:
        return browse_map, uid_fix_count

    LOGGER.debug("browse_map repair: %d multi-IE manuscripts have IEs without transcription text", len(needs_repair))

    # Scan Transcriptions.txt for missing pages
    # Parse headers: ==> {sys_id}_{IE_id}_{P_num}_{FL_id} <==
    header_re = re.compile(r'^==> (.+?) <==\s*$')
    restored = {}  # {sid: [page_dicts]}
    current_header = None
    current_text_lines = []

    def _flush_page():
        nonlocal current_header, current_text_lines
        if not current_header:
            return
        parts = current_header.split('_')
        if len(parts) < 4:
            current_header = None
            current_text_lines = []
            return
        sid = parts[0]
        if sid not in needs_repair:
            current_header = None
            current_text_lines = []
            return
        ie_id = _extract_ie_from_header(current_header)
        if ie_id not in needs_repair[sid]:
            current_header = None
            current_text_lines = []
            return
        # This page needs to be restored
        try:
            p_num = int(parts[2].replace('P', '').lstrip('0') or '0')
        except ValueError:
            p_num = 0
        # Extract uid in the same format as MetadataManager.extract_unique_id
        # (IE_P_FL without sys_id prefix) to match Tantivy index keys
        uid_match = re.search(r'(IE\d+_P\d+_FL\d+)', current_header)
        uid = uid_match.group(1) if uid_match else current_header
        page = {
            'uid': uid,
            'p_num': p_num,
            'full_header': current_header,
            'ie_id': ie_id,
            'seq_index': p_num,
        }
        if sid not in restored:
            restored[sid] = []
        restored[sid].append(page)
        current_header = None
        current_text_lines = []

    try:
        with open(transcriptions_path, 'r', encoding='utf-8') as f:
            for line in f:
                m = header_re.match(line)
                if m:
                    _flush_page()
                    current_header = m.group(1)
                    current_text_lines = []
                else:
                    current_text_lines.append(line)
            _flush_page()
    except Exception as e:
        LOGGER.warning("Failed to read Transcriptions.txt for browse_map repair: %s", e)
        return browse_map, 0

    # Merge restored pages into browse_map
    repair_count = 0
    for sid, new_pages in restored.items():
        if sid in browse_map:
            browse_map[sid].extend(new_pages)
            repair_count += 1
            LOGGER.info("Restored %d pages for %s (IEs: %s)",
                         len(new_pages), sid,
                         ', '.join(sorted(set(p['ie_id'] for p in new_pages))))

    total_restored = sum(len(v) for v in restored.values())
    LOGGER.info("browse_map repair complete: %d manuscripts repaired, %d pages restored, %d uids fixed",
                repair_count, total_restored, uid_fix_count)
    return browse_map, repair_count + uid_fix_count


def dedupe_browse_map(browse_map):
    """
    Deduplicate browse_map pages and tag each page with its IE.

    For multi-IE manuscripts, keeps ALL pages from ALL IEs (no cross-IE dedup).
    Each page gets an 'ie_id' field extracted from its full_header.
    Only removes true duplicates: same IE + same p_num within one sys_id.

    For single-IE manuscripts, behavior is unchanged — pages are kept as-is
    with ie_id tagged.

    Also repairs browse_maps where non-primary IE pages were lost by pre-Phase-58
    dedup that incorrectly deduped by p_num across IEs.
    """
    # First repair any missing IE pages from Transcriptions.txt
    browse_map, repair_count = _repair_missing_ie_pages(browse_map)

    ie_volume_map = _load_ie_volume_map()
    cleaned = {}
    changed = repair_count > 0

    for sid, pages in browse_map.items():
        is_multi_ie = sid in ie_volume_map

        if not is_multi_ie:
            # Single-IE: tag ie_id but no dedup needed (p_nums are unique)
            for page in pages:
                if 'ie_id' not in page:
                    page['ie_id'] = _extract_ie_from_header(page.get('full_header', ''))
            cleaned[sid] = pages
            continue

        # Multi-IE: tag ie_id, dedup within each IE (same ie+p_num), keep all IEs
        ie_pages = {}  # {ie_id: {p_num: page}}
        for page in pages:
            ie_id = _extract_ie_from_header(page.get('full_header', ''))
            page['ie_id'] = ie_id
            p_num = page.get('p_num')

            if ie_id not in ie_pages:
                ie_pages[ie_id] = {}

            if p_num in ie_pages[ie_id]:
                # True duplicate (same IE + same p_num) — skip
                changed = True
                continue

            ie_pages[ie_id][p_num] = page

        # Flatten: order IEs by volume map order, then pages by p_num within each IE
        volume_info = ie_volume_map.get(sid, {})
        volume_order = [v["ie_id"] for v in volume_info.get("volumes", [])]

        # Add any IEs found in data but not in volume map
        all_ie_ids = list(ie_pages.keys())
        for ie_id in all_ie_ids:
            if ie_id not in volume_order:
                volume_order.append(ie_id)

        all_pages = []
        for ie_id in volume_order:
            if ie_id in ie_pages:
                ie_sorted = sorted(ie_pages[ie_id].values(), key=lambda p: p.get('p_num', 0))
                all_pages.extend(ie_sorted)

        # Include pages with unknown IE (ie_id=None) at the end
        if None in ie_pages:
            none_sorted = sorted(ie_pages[None].values(), key=lambda p: p.get('p_num', 0))
            all_pages.extend(none_sorted)

        cleaned[sid] = all_pages

    return cleaned, changed
