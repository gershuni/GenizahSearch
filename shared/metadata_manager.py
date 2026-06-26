# -*- coding: utf-8 -*-
"""NLI metadata fetch, IIIF/MARC enrichment, and persistent caching.

Phase 124: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import MetadataManager`` callers continue working.
"""

import logging
import os
import re
import threading
import pickle
import requests
import xml.etree.ElementTree as ET
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared.config import Config
from shared.nli_circuit_breaker import (
    is_open as _nli_circuit_is_open,
    record_failure as _nli_record_failure,
    record_success as _nli_record_success,
    NLI_CONNECT_TIMEOUT,
    NLI_IIIF_READ_TIMEOUT,
    NLI_MARC_READ_TIMEOUT,
)
from shared.codicological import CodicologicalManager
from shared.browse_map_utils import normalize_shelfmark, natural_sort_key, _strip_library_prefix

LOGGER = logging.getLogger("genizah." + __name__)

# Phase 84: WARNING-once flag for shelfmark_bridge import failures (Gemini LOW).
_BRIDGE_IMPORT_WARNED = False


def _warn_bridge_import_failed(exc):
    """Log shelfmark_bridge import failure at WARNING once per process (Gemini LOW)."""
    global _BRIDGE_IMPORT_WARNED
    if not _BRIDGE_IMPORT_WARNED:
        LOGGER.warning("shelfmark_bridge unavailable (degrading to v7.10 behavior): %s", exc)
        _BRIDGE_IMPORT_WARNED = True


_CUDL_LABEL_RE = re.compile(r'^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b', re.IGNORECASE)


def _parse_cudl_label(lbl):
    """Parse a CUDL canvas label → (folio_num:int|None, folio_side:'r'|'v'|None).

    Convention: a bare numeric label (no 'r'/'v' suffix) is treated as recto
    ('r'); non-numeric labels ('Binding', 'Cover', etc.) return (None, None).
    Uppercase side letters ('1R', '1V') are normalized to lowercase.

    Examples:
        '1'        → (1, 'r')   # bare numeric = recto by convention
        '1r'       → (1, 'r')
        '1v'       → (1, 'v')
        '6R'       → (6, 'r')   # uppercase normalized
        'f.2v'     → (2, 'v')
        'f. 2v'    → (2, 'v')
        'Binding'  → (None, None)
        ''         → (None, None)
        None       → (None, None)
    """
    if not lbl:
        return (None, None)
    m = _CUDL_LABEL_RE.match(str(lbl).strip())
    if not m:
        return (None, None)
    try:
        folio_num = int(m.group(1))
    except (TypeError, ValueError):
        return (None, None)
    side_raw = m.group(2)
    side = side_raw.lower() if side_raw else 'r'  # bare numeric → recto
    return (folio_num, side)


# ==============================================================================
#  NLI CROSSREF SIDECAR (lazy accessor for local image resolution)
# ==============================================================================
_nli_crossref_svc = None

def _get_crossref_service():
    """Lazy accessor for the NLI crossref sidecar service (desktop use)."""
    global _nli_crossref_svc
    if _nli_crossref_svc is None:
        try:
            from shared.nli_crossref_service import NliCrossrefService
            _nli_crossref_svc = NliCrossrefService(thread_safe=True)
        except Exception as e:
            LOGGER.warning('Failed to initialize NLI crossref service: %s', e)
    return _nli_crossref_svc


# ==============================================================================
#  FJMS SIDECAR (lazy accessor for bibliography/catalog enrichment)
# ==============================================================================
_fjms_svc = None

def _get_fjms_service():
    """Lazy accessor for the FJMS enrichment sidecar service."""
    global _fjms_svc
    if _fjms_svc is None:
        try:
            from shared.fjms_service import FjmsService
            _fjms_svc = FjmsService(thread_safe=True)
        except Exception as e:
            LOGGER.warning('Failed to initialize FJMS service: %s', e)
    return _fjms_svc


# ------------------------------------------------------------------------------
#  Bounded, thread-safe LRU for MetadataManager.nli_cache
# ------------------------------------------------------------------------------
# Origin: 2026-06-06 production heap re-attribution. ``self.nli_cache`` was a
# plain ``dict`` with NO eviction; on the long-running web service it accrued
# one metadata entry per unique manuscript ever browsed/enriched and never
# released them (50K+ entries and still climbing on a 3-day-old process). This
# bounds it to ``NLI_CACHE_MAX_ENTRIES`` with LRU eviction.
#
# Why a *locked* wrapper rather than the lock-free ``_bounded_cache_*`` helpers
# used for the manifest caches: nli_cache is read/written from MULTIPLE threads
# (the 2-worker ``nli_executor`` pool AND Starlette's sync-route threadpool) and
# is iterated at genizah_core.py:~5062/~5097. A plain dict's get/set are
# GIL-atomic, but an OrderedDict's ``move_to_end``/``popitem`` are not, so
# eviction under concurrency could corrupt ordering or raise. The RLock makes
# each operation atomic and ``items()``/``keys()``/``values()``/iteration return
# snapshots so concurrent writes can't raise "mutated during iteration".
#
# Pickles as a plain ``dict`` (``__reduce__``) so ``nli_cache.pkl`` keeps its
# existing on-disk format and stays loadable by the desktop app / older builds.
_NLI_CACHE_MAX_ENTRIES = max(0, int(os.environ.get('NLI_CACHE_MAX_ENTRIES', '75000')))


class _BoundedLRUCache:
    """Thread-safe, bounded, dict-like LRU.

    Implements only the ``nli_cache`` API surface actually used across both
    apps: ``in``, ``[]`` (get/set), ``.get()``, ``.items()``, ``.keys()``,
    ``.values()``, ``len()`` and iteration. ``maxsize <= 0`` disables eviction
    (unbounded — restores legacy behavior for an escape hatch).
    """

    __slots__ = ('_data', '_maxsize', '_lock')

    def __init__(self, maxsize=_NLI_CACHE_MAX_ENTRIES, data=None):
        self._data = OrderedDict()
        self._maxsize = int(maxsize)
        self._lock = threading.RLock()
        if data:
            with self._lock:
                for key, value in data.items():
                    self._data[key] = value
                self._evict_locked()

    def _evict_locked(self):
        # Caller must hold self._lock.
        if self._maxsize > 0:
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)

    def __contains__(self, key):
        with self._lock:
            return key in self._data

    def __getitem__(self, key):
        with self._lock:
            value = self._data[key]  # raises KeyError like dict
            self._data.move_to_end(key)
            return value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            self._evict_locked()

    def get(self, key, default=None):
        with self._lock:
            if key not in self._data:
                return default
            self._data.move_to_end(key)
            return self._data[key]

    def items(self):
        with self._lock:
            return list(self._data.items())

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def values(self):
        with self._lock:
            return list(self._data.values())

    def __len__(self):
        with self._lock:
            return len(self._data)

    def __iter__(self):
        with self._lock:
            return iter(list(self._data.keys()))

    def __reduce__(self):
        # Serialize as a plain dict: keeps nli_cache.pkl format unchanged and
        # loadable by older builds / the desktop app.
        with self._lock:
            return (dict, (dict(self._data),))

    @property
    def maxsize(self):
        return self._maxsize


# ==============================================================================
#  METADATA MANAGER
# ==============================================================================
# Per-operation network timeouts (seconds). Named so each remote dependency can
# be tuned independently rather than sharing one opaque literal.
MARC_FUTURE_TIMEOUT = 15          # await fetch_marc_data() future result
NLI_IIIF_FUTURE_TIMEOUT = 15      # await fetch_iiif_manifest() future result
EXTERNAL_IIIF_HTTP_TIMEOUT = 5    # external IIIF manifest GET (Figgy/CUDL etc.)


class MetadataManager:
    def _make_session(self):
        return requests.Session()

    """Handle metadata parsing, remote retrieval, and persistent caching."""
    def __init__(self):
        self.meta_map = {}
        # Bounded LRU (was an unbounded dict — see _BoundedLRUCache above).
        self.nli_cache = _BoundedLRUCache()
        self.csv_bank = {}
        self._shelfmark_index = None
        self.nli_executor = ThreadPoolExecutor(max_workers=2)
        self.ns = {'marc': 'http://www.loc.gov/MARC21/slim'}

        # Codicological Parts manager (Oxford Neubauer)
        self.codico_mgr = CodicologicalManager()

        # Ensure index dir exists for caches
        if not os.path.exists(Config.INDEX_DIR):
            try:
                os.makedirs(Config.INDEX_DIR)
            except Exception as e:
                LOGGER.error("Failed to create index directory for metadata at %s: %s", Config.INDEX_DIR, e)

        # Load small caches immediately
        self._load_small_caches()

    def start_background_loading(self):
        """Start loading heavy metadata resources (CSV, Maps) in background."""
        threading.Thread(target=self._load_heavy_caches_bg, daemon=True).start()
        threading.Thread(target=self._build_file_map_background, daemon=True).start()

    def _load_small_caches(self):
        if os.path.exists(Config.CACHE_NLI):
            try:
                with open(Config.CACHE_NLI, 'rb') as f:
                    _loaded = pickle.load(f)
                # On-disk format is a plain dict; wrap into the bounded LRU.
                # (Defensive: tolerate a legacy pickled _BoundedLRUCache too.)
                if isinstance(_loaded, _BoundedLRUCache):
                    self.nli_cache = _loaded
                else:
                    self.nli_cache = _BoundedLRUCache(data=dict(_loaded))
            except Exception as e:
                LOGGER.warning("Failed to load NLI cache from %s: %s", Config.CACHE_NLI, e)
        if os.path.exists(Config.CACHE_META):
            try:
                with open(Config.CACHE_META, 'rb') as f: self.meta_map = pickle.load(f)
            except Exception as e:
                LOGGER.warning("Failed to load metadata cache from %s: %s", Config.CACHE_META, e)

    def _load_heavy_caches_bg(self):
        self._load_csv_bank()
        # Load codicological parts after CSV is ready
        self.codico_mgr.load(csv_bank=self.csv_bank)

    def _load_csv_bank(self):
        """Load the massive CSV file into memory for instant lookup."""
        if not os.path.exists(Config.LIBRARIES_CSV):
            LOGGER.warning("libraries.csv not found at %s; csv_bank will remain empty", Config.LIBRARIES_CSV)
            return

        LOGGER.info("Loading libraries.csv from %s", Config.LIBRARIES_CSV)

        import csv
        try:
            with open(Config.LIBRARIES_CSV, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, delimiter=',')
                next(reader, None) # Skip header

                for row in reader:

                    if not row or len(row) < 3:
                        continue
                    # Format: system_number, oxford_part_id, call_numbers, library_code, ..., titles_non_placeholder
                    raw_sys_id = row[0]
                    # Phase 85 D-04a: tolerate marker-block lines from scripts/generate_synthetic_rows.py
                    # (e.g. '# BEGIN SYNTHETIC', '# END SYNTHETIC'). Without this guard, the loader's
                    # digit-normalization would produce sys_id='' and overwrite previous '' entries.
                    if raw_sys_id.startswith('#'):
                        continue
                    sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())

                    # Oxford Part ID (column 1) - for Neubauer catalog
                    oxford_part_id = row[1].strip() if len(row) > 1 else ""

                    # Call numbers (column 2) can be multiple separated by '|'
                    # We take the shortest one that looks like a shelfmark, or just the first
                    raw_shelves = row[2].split('|') if len(row) > 2 else []
                    shelf = raw_shelves[0].strip() if raw_shelves else ""
                    # Try to find a nice short shelfmark
                    for s in raw_shelves:
                        s = s.strip()
                        if s and len(s) < len(shelf):
                            shelf = s

                    # Library code (column 3) - backward compatible with old CSV
                    library_code = row[3].strip() if len(row) > 3 else ""

                    # Title is column index 7 (0-based) - titles_non_placeholder
                    # (shifted by 1 due to library_code column insertion)
                    title = ""
                    if len(row) > 7:
                        title = row[7].strip()

                    # Keep all call_number variants for shelfmark resolution and special-library fallbacks.
                    call_numbers_raw = [s.strip() for s in raw_shelves if s.strip()] or None

                    self.csv_bank[sys_id] = {
                        'shelfmark': shelf,
                        'title': title,
                        'oxford_part_id': oxford_part_id,
                        'library_code': library_code,
                        'call_numbers_raw': call_numbers_raw,
                    }
            # CSV data is authoritative for shelfmark browsing; force the normalized index
            # to rebuild after background loading completes.
            self._shelfmark_index = None
            LOGGER.info("Loaded %d records into csv_bank from libraries.csv", len(self.csv_bank))

            # Phase 84: build CUDL alias index for cross-system shelfmark lookups (D-03).
            try:
                from shared.shelfmark_bridge import build_alias_index as _build_cudl_alias_index
                _build_cudl_alias_index(self.csv_bank)
            except ImportError as e:
                _warn_bridge_import_failed(e)
            except Exception as e:
                LOGGER.warning("CUDL alias index build failed (continuing without bridge): %s", e)

            # Stamp has_vs from vs_manifest.txt (lightweight, ~2.5MB, 129K sys_ids)
            vs_manifest_path = os.path.join(os.path.dirname(Config.LIBRARIES_CSV), 'fist_data', 'vs_manifest.txt')
            if not os.path.exists(vs_manifest_path):
                # Try PyInstaller _internal path
                vs_manifest_path = os.path.join(os.path.dirname(os.path.abspath(Config.LIBRARIES_CSV)), 'fist_data', 'vs_manifest.txt')
            if os.path.exists(vs_manifest_path):
                try:
                    with open(vs_manifest_path, 'r') as vf:
                        vs_ids = set(line.strip() for line in vf if line.strip())
                    stamped = 0
                    for vid in vs_ids:
                        if vid in self.csv_bank:
                            self.csv_bank[vid]['has_vs'] = True
                            stamped += 1
                    LOGGER.info("Stamped has_vs on %d/%d manuscripts from vs_manifest.txt", stamped, len(vs_ids))
                except Exception as e:
                    LOGGER.warning("Failed to load vs_manifest.txt: %s", e)
        except Exception as e:
            LOGGER.error("Failed to load CSV library bank from %s: %s", Config.LIBRARIES_CSV, e)

    def get_meta_for_id(self, sys_id):
        # Normalize sys_id to digits only (handles BOM/RTL marks/stray chars)
        if sys_id is None:
            return "Unknown", ""
        raw_input = str(sys_id) if sys_id is not None else ""
        sys_id = "".join(ch for ch in raw_input if ch.isdigit())

        if raw_input != sys_id and raw_input:
             LOGGER.debug("Normalized sys_id: raw=%r -> %r", raw_input, sys_id)

        """Get shelfmark and title from ANY source (CSV > Cache > Bank)."""
        shelf = "Unknown"
        title = ""

        # 1. Check CSV (Fastest & Most reliable for basic info)
        if sys_id in self.csv_bank:
            shelf = self.csv_bank[sys_id]['shelfmark']
            title = self.csv_bank[sys_id]['title']

        # 2. Check NLI Cache (Fallback/Enrichment)
        if sys_id in self.nli_cache:
            m = self.nli_cache[sys_id]
            cached_shelf = m.get('shelfmark')
            cached_title = m.get('title')

            # If CSV missed shelfmark, try cache
            if shelf == "Unknown" or not shelf:
                if cached_shelf and cached_shelf != "Unknown":
                    shelf = cached_shelf

            # If CSV missed title, try cache (crucial fix for missing titles)
            if not title and cached_title:
                title = cached_title

        return shelf, title

    def get_library_for_id(self, sys_id: str) -> str:
        """Get library code for a system ID.

        Args:
            sys_id: The system ID to look up

        Returns:
            Library code string (e.g., 'CUL', 'JTS') or empty string if not found
        """
        if sys_id is None:
            return ''
        # Normalize sys_id
        sys_id = "".join(ch for ch in str(sys_id) if ch.isdigit())
        entry = self.csv_bank.get(sys_id, {})
        return entry.get('library_code', '')

    def get_shelfmark_from_header(self, full_header):
        parsed = self.parse_full_id_components(full_header)

        sys_id = parsed.get('sys_id')
        if sys_id:
            shelf, _ = self.get_meta_for_id(sys_id)
            if shelf and shelf != "Unknown":
                return shelf

        if sys_id and sys_id in self.nli_cache:
            return self.nli_cache[sys_id].get('shelfmark', '')
        return ''

    def save_caches(self):
        try:
            # Persist as a plain dict (snapshot) so the on-disk format is
            # unchanged and remains loadable by older builds / the desktop app.
            with open(Config.CACHE_NLI, 'wb') as f: pickle.dump(dict(self.nli_cache.items()), f)
        except Exception as e:
            LOGGER.error("Failed to persist NLI cache to %s: %s", Config.CACHE_NLI, e)

    # --- Codicological Parts API (delegates to codico_mgr) ---

    def get_part_for_folio(self, sys_id):
        """Get the Part ID for a given system ID."""
        return self.codico_mgr.get_part_for_folio(sys_id)

    def get_folios_for_part(self, part_id):
        """Get all system IDs (folios) belonging to a Part."""
        return self.codico_mgr.get_folios_for_part(part_id)

    def get_part_metadata(self, part_id):
        """Get full metadata for a Part (Oxford Neubauer)."""
        return self.codico_mgr.get_part_metadata(part_id)

    def get_part_images(self, part_id):
        """Get all images for a Part."""
        return self.codico_mgr.get_part_images(part_id)

    def is_part_id(self, identifier):
        """Check if an identifier is a Part ID."""
        return self.codico_mgr.is_part_id(identifier)

    def parse_part_identifier(self, identifier):
        """Parse an identifier that might be a Part. Returns (part_id, is_part)."""
        return self.codico_mgr.parse_part_identifier(identifier)

    def get_part_autocomplete_list(self):
        """Get list of Parts for autocomplete."""
        return self.codico_mgr.part_autocomplete

    def get_meta_with_part(self, sys_id):
        """
        Get shelfmark, title, and Part info for a system ID.
        Returns dict with: shelfmark, title, oxford_part_id, part_metadata
        """
        shelf, title = self.get_meta_for_id(sys_id)

        result = {
            'shelfmark': shelf,
            'title': title,
            'oxford_part_id': None,
            'part_metadata': None,
        }

        # Get Part info if available
        part_id = self.get_part_for_folio(sys_id)
        if part_id:
            result['oxford_part_id'] = part_id
            result['part_metadata'] = self.get_part_metadata(part_id)

            # If our title is empty but Part has a title, use it
            if not title and result['part_metadata']:
                result['title'] = result['part_metadata'].get('title', '')

        return result

    def _build_file_map_background(self):
        if self.meta_map: return
        if not os.path.exists(Config.FILE_V7): return
        temp_map = {}
        try:
            with open(Config.FILE_V7, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith("###"):
                        uid = self.extract_unique_id(line)
                        if "xml -" in line:
                            parts = line.split("xml -")
                            if len(parts) > 1: temp_map[uid] = parts[1].strip()
            self.meta_map = temp_map
            with open(Config.CACHE_META, 'wb') as f: pickle.dump(self.meta_map, f)
        except Exception as e:
            LOGGER.warning("Failed to build or save file map cache from %s: %s", Config.FILE_V7, e)

    def extract_unique_id(self, text):
        """
        Robust extraction of Unique ID.
        Instead of expecting a fixed string 'IE_P_FL', we scan for components anywhere.
        This fixes issues with file paths containing backslashes (e.g. Russia Library).
        """
        # First attempt: classic continuous structure
        match = re.search(r'(IE\d+_P\d+_FL\d+)', text)
        if match:
            return match.group(1)

        # Second attempt: components assembly (robust to path breaks)
        ie = re.search(r'(IE\d+)', text)
        p = re.search(r'(P\d+)', text)
        fl = re.search(r'(FL\d+)', text)

        if ie and p and fl:
            return f"{ie.group(1)}_{p.group(1)}_{fl.group(1)}"

        # Default: System ID (only if all else fails)
        sys = re.search(r'(99\d+)', text)
        return sys.group(1) if sys else "UNKNOWN"

    def parse_header_smart(self, full_header):
        # Phase 95 D-13 (Codex P0) — broaden to recognize LOCAL 97-prefix in addition to 99.
        sys_match = re.search(r'((?:99|97)\d{8,})', full_header)
        sys_id = sys_match.group(1) if sys_match else None
        p_num = "Unknown"
        p_match = re.search(r'_P(\d+)_', full_header)
        if p_match:
            p_num = str(int(p_match.group(1)))
        else:
            tif_match = re.search(r'[ -_](\d{3,4})\.tif', full_header, re.IGNORECASE)
            if tif_match: p_num = str(int(tif_match.group(1)))
        return sys_id, p_num

    def parse_full_id_components(self, full_header):
        """
        Parse header into components regardless of order or separators.
        Fixes display issues for V0.7 paths.
        """
        result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}

        # 1. System ID (99... or 97... for LOCAL — Phase 95 D-13 Codex P0)
        # Phase 95 D-13 (Codex P0) — broaden for LOCAL 97-prefix.
        sys_match = re.search(r'((?:99|97)\d{8,})', full_header)
        if sys_match:
            result['sys_id'] = sys_match.group(1)

        # 2. IE ID
        ie_match = re.search(r'(IE\d+)', full_header)
        if ie_match:
            result['ie_id'] = ie_match.group(1)
        # Phase 95 D-34 — LOCAL full_header has no IE\d+ component; instead uses F\d{4}.
        if not result.get('ie_id'):
            f_match = re.search(r'_F(\d{3,5})', full_header)
            if f_match:
                result['ie_id'] = f"F{f_match.group(1)}"

        # 3. Page Number (P...)
        p_match = re.search(r'_?(P\d+)', full_header)
        if p_match:
            # Remove P to get clean number
            raw_p = p_match.group(1) # P0001
            result['p_num'] = str(int(raw_p[1:]))

        # 4. FL ID
        fl_match = re.search(r'(FL\d+)', full_header)
        if fl_match:
            result['fl_id'] = fl_match.group(1).replace("FL", "")

        return result

    def fetch_nli_data(self, system_id):
        # 1. Check existing cache
        if system_id in self.nli_cache:
            return self.nli_cache[system_id]

        # 2. Check CSV Bank (local fetch)
        if system_id in self.csv_bank:
            row = self.csv_bank[system_id]
            meta = {
                'shelfmark': row['shelfmark'],
                'title': row['title'],
                'desc': '',
                'fl_ids': [],
                'thumb_url': None,
                'thumb_checked': True # Mark as checked to prevent repeated image download attempts
            }
            self.nli_cache[system_id] = meta
            return meta

        # 3. Only if necessary (not in cache/CSV) - Network request
        _, meta = self._fetch_single_worker(system_id)
        self.nli_cache[system_id] = meta
        return meta

    # Separate manifest cache keyed by (sys_id, suffix) — does NOT touch nli_cache.
    # Bounded LRU: crawl traffic can otherwise leave one manifest dict per
    # requested manuscript in process memory for the lifetime of the service.
    _iiif_manifest_cache = OrderedDict()
    _IIIF_MANIFEST_CACHE_MAX = max(0, int(os.environ.get('IIIF_MANIFEST_CACHE_MAX_ENTRIES', '5000')))
    # 260421 follow-up (L81 lag) — Phase 98 migration: per-sys_id negative
    # cache lives HERE (per-(sys_id, suffix) for IIIF, per-sys_id for MARC),
    # but the global "NLI is down" circuit breaker has MOVED to
    # shared/nli_circuit_breaker.py (a module-level singleton — single source
    # of truth across web/api.py, shared/puzzle_image_service.py, and
    # genizah_core.py). The 404 / parse-error paths populate the per-sys_id
    # caches here; the timeout / 5xx / 429 / connection-error paths call
    # _nli_record_failure from the shared module.
    _iiif_manifest_fail_cache: dict = OrderedDict()  # {(sys_id, suffix): timestamp}
    _marc_fail_cache: dict = OrderedDict()  # {sys_id: timestamp}
    _NLI_FAIL_TTL = 60  # seconds
    _NLI_FAIL_CACHE_MAX = max(0, int(os.environ.get('NLI_FAIL_CACHE_MAX_ENTRIES', '20000')))

    @staticmethod
    def _bounded_cache_get(cache: dict, key):
        if key not in cache:
            return None
        if isinstance(cache, OrderedDict):
            cache.move_to_end(key)
        return cache[key]

    @staticmethod
    def _bounded_cache_set(cache: dict, key, value, max_entries: int) -> None:
        if max_entries == 0:
            return
        cache[key] = value
        if isinstance(cache, OrderedDict):
            cache.move_to_end(key)
        while max_entries and len(cache) > max_entries:
            if isinstance(cache, OrderedDict):
                cache.popitem(last=False)
            else:
                cache.pop(next(iter(cache)), None)

    def _timestamp_cache_recent(self, cache: dict, key, ttl: float, now: float):
        ts = cache.get(key)
        if ts is None:
            return None
        if now - ts < ttl:
            if isinstance(cache, OrderedDict):
                cache.move_to_end(key)
            return ts
        cache.pop(key, None)
        return None

    def _timestamp_cache_set(self, cache: dict, key, now: float) -> None:
        self._bounded_cache_set(cache, key, now, self._NLI_FAIL_CACHE_MAX)
        stale = [
            cache_key for cache_key, ts in list(cache.items())
            if now - ts >= self._NLI_FAIL_TTL
        ]
        for cache_key in stale:
            cache.pop(cache_key, None)

    def get_runtime_cache_stats(self) -> dict:
        """Return lightweight cache sizes for the memstat diagnostic endpoint."""
        return {
            'nli_cache_entries': len(self.nli_cache),
            'iiif_manifest_cache_entries': len(self._iiif_manifest_cache),
            'iiif_manifest_cache_max_entries': self._IIIF_MANIFEST_CACHE_MAX,
            'iiif_manifest_fail_cache_entries': len(self._iiif_manifest_fail_cache),
            'marc_fail_cache_entries': len(self._marc_fail_cache),
            'nli_fail_cache_max_entries': self._NLI_FAIL_CACHE_MAX,
        }

    def fetch_iiif_manifest(self, system_id, suffix=1):
        """Fetch and parse IIIF manifest for physical description, attribution, and image labels.

        Args:
            system_id: NLI system ID
            suffix: IE suffix for multi-volume manuscripts (1=primary, 2=second IE, etc.)
        """
        import time
        # Phase 85 D-14: synthetic sys_ids have no NLI Alma record — skip the
        # network call BEFORE issuing it. Saves ~93-2K external requests per
        # cold cache cycle and prevents NLI 404 access-log pollution.
        from shared.synthetic_sys_id import is_synthetic_sys_id
        if is_synthetic_sys_id(system_id):
            return {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}

        # Check manifest-only cache first
        cache_key = (system_id, suffix)
        cached_manifest = self._bounded_cache_get(self._iiif_manifest_cache, cache_key)
        if cached_manifest is not None:
            return cached_manifest

        # Phase 98 D-03 + D-13: shared NLI circuit breaker. If NLI has been
        # failing consistently across ANY call site, skip entirely without
        # burning another timeout on this sys_id.
        if _nli_circuit_is_open():
            return {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}

        # Negative cache: if this sys_id recently timed out, return empty
        # without re-trying. TTL lets the cache age out so recovery works.
        now = time.time()
        fail_ts = self._timestamp_cache_recent(
            self._iiif_manifest_fail_cache, cache_key, self._NLI_FAIL_TTL, now
        )
        if fail_ts is not None:
            return {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}

        url = f"{Config.NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"
        headers = Config.HTTP_HEADERS

        result = {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}
        try:
            session = self._make_session()
            resp = session.get(
                url,
                headers=headers,
                timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT),
                verify=True,
            )
            if resp.status_code == 200:
                data = resp.json()

                # 1. Physical Description
                result['physical_desc'] = data.get('attribution', '')
                attr_val = data.get('attribution')
                if isinstance(attr_val, str):
                    result['attribution'] = attr_val
                elif isinstance(attr_val, list) and attr_val:
                    result['attribution'] = str(attr_val[0])
                elif data.get('label'):
                    result['attribution'] = str(data.get('label'))

                # 2. Canvas Map (FL -> Label)
                if 'sequences' in data and data['sequences']:
                    for canvas in data['sequences'][0].get('canvases', []):
                        label = canvas.get('label', '')
                        # Extract FL ID from image service ID
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            service_id = service.get('@id', '')
                            # Extract FL number (e.g. .../FL7734473/...)
                            fl_match = re.search(r'FL(\d+)', service_id)
                            if fl_match:
                                fl_digits = fl_match.group(1)
                                result['canvas_map'][fl_digits] = label

                self._bounded_cache_set(
                    self._iiif_manifest_cache,
                    cache_key,
                    result,
                    self._IIIF_MANIFEST_CACHE_MAX,
                )
                self._iiif_manifest_fail_cache.pop(cache_key, None)
                _nli_record_success(path='fetch_iiif_manifest')
                return result
            elif resp.status_code == 429:
                _nli_record_failure(failure_type='429', path='fetch_iiif_manifest')
                LOGGER.warning(f"IIIF 429 for {system_id} (suffix={suffix})")
            elif 500 <= resp.status_code < 600:
                _nli_record_failure(failure_type='5xx', path='fetch_iiif_manifest')
                LOGGER.warning(f"IIIF {resp.status_code} for {system_id} (suffix={suffix})")
            # 404 / other 4xx -> per-sys_id negative cache only (D-07);
            # do NOT increment the breaker.
            self._timestamp_cache_set(self._iiif_manifest_fail_cache, cache_key, time.time())
            return result
        except requests.exceptions.Timeout as e:
            LOGGER.warning(f"IIIF fetch timeout for {system_id} (suffix={suffix}): {e}")
            self._timestamp_cache_set(self._iiif_manifest_fail_cache, cache_key, time.time())
            _nli_record_failure(failure_type='timeout', path='fetch_iiif_manifest')
            return result
        except requests.exceptions.ConnectionError as e:
            LOGGER.warning(f"IIIF fetch connection error for {system_id} (suffix={suffix}): {e}")
            self._timestamp_cache_set(self._iiif_manifest_fail_cache, cache_key, time.time())
            _nli_record_failure(failure_type='connection_error', path='fetch_iiif_manifest')
            return result
        except Exception as e:
            # Parse errors / data structure issues — code-level, not upstream.
            # Per-sys_id negative cache only; do NOT trip the breaker (D-07).
            LOGGER.warning(f"IIIF parse failed for {system_id} (suffix={suffix}): {e}")
            self._timestamp_cache_set(self._iiif_manifest_fail_cache, cache_key, time.time())
            return result

    def fetch_marc_data(self, system_id):
        """Fetch and parse MARC XML for bibliography, notes, and extended metadata."""
        import time
        # Phase 85 D-14: synthetic sys_ids have no NLI MARC record — skip the
        # network call BEFORE issuing it (parallel to fetch_iiif_manifest guard).
        from shared.synthetic_sys_id import is_synthetic_sys_id

        result = {
            'bibliography': [],
            'notes': [],
            'english_title': '',
            'dimensions': '',
            'people': [],
            'current_owner': '',
            'shelfmark_alt': '',
            'date': '',
            'subjects': [],
            'physical_medium': '',
            'attribution': '',
            'online_link': None,
            'external_iiif_link': None
        }
        if is_synthetic_sys_id(system_id):
            return result

        # Use the specific IIIF/MARC endpoint which is more reliable
        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"
        headers = Config.HTTP_HEADERS

        # Phase 98 D-03 + D-13: shared NLI circuit breaker + per-sys_id
        # negative cache (complementary layers). Breaker trips across ALL
        # sys_ids after THRESHOLD consecutive failures so a dead NLI host
        # doesn't gate navigation; per-sys_id cache prevents re-trying the
        # SAME failed sys_id within TTL even when the breaker is closed.
        if _nli_circuit_is_open():
            return result
        fail_ts = self._timestamp_cache_recent(
            self._marc_fail_cache, system_id, self._NLI_FAIL_TTL, time.time()
        )
        if fail_ts is not None:
            return result

        try:
            session = self._make_session()
            resp = session.get(
                url,
                headers=headers,
                timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT),
            )
            if resp.status_code == 200:
                # Remove namespaces to simplify parsing
                xml_content = re.sub(r'\sxmlns="[^"]+"', '', resp.text, count=1)
                xml_content = re.sub(r'\sxmlns:marc="[^"]+"', '', xml_content, count=1)
                xml_content = xml_content.replace('marc:', '') # Bruteforce namespace removal

                root = ET.fromstring(xml_content)

                for df in root.findall(".//datafield"):
                    tag = df.get('tag')

                    def get_sub(code):
                        sf = df.find(f"subfield[@code='{code}']")
                        return sf.text.strip() if sf is not None and sf.text else ""

                    if tag == '581': # Bibliography
                        val = get_sub('a')
                        if val: result['bibliography'].append(val)

                    elif tag == '500': # Notes
                        val = get_sub('a')
                        if val: result['notes'].append(val)

                    elif tag == '246': # English Title
                        val_a = get_sub('a')
                        val_i = get_sub('i')
                        if "English" in val_i:
                            result['english_title'] = val_a

                    elif tag == '260' or tag == '264': # Date
                        val = get_sub('c')
                        if val: result['date'] = val

                    elif tag == '300': # Dimensions
                        val_a = get_sub('a') # Extent (pages)
                        val_c = get_sub('c') # Dimensions
                        parts = [p for p in [val_a, val_c] if p]
                        result['dimensions'] = " | ".join(parts)

                    elif tag == '340': # Physical Medium / Condition
                        val = get_sub('a')
                        if val: result['physical_medium'] = val

                    elif tag == '650': # Subjects
                        val = get_sub('a')
                        if val: result['subjects'].append(val)

                    elif tag == '700': # People / Owners
                        name = get_sub('a')
                        role = get_sub('e')
                        if name:
                            full = f"{name} ({role})" if role else name
                            result['people'].append(full)

                    elif tag == '710': # Current Owner (Library Name)
                        val = get_sub('a')
                        if val: result['current_owner'] = val

                    elif tag == '856': # Online Link
                        url = get_sub('u')
                        label = get_sub('z') or "Online Version"
                        if url:
                            result['online_link'] = {'url': url, 'label': label}
                            # Detect CUDL for External Viewer
                            if "cudl.lib.cam.ac.uk" in url:
                                result['external_iiif_link'] = url

                    elif tag == '942': # Alt Shelfmark
                        val = get_sub('z')
                        # NLI MARC has multiple 942 datafields: one for the
                        # owning library (holds the real shelfmark, e.g.
                        # "Ms. ENA 1052.1") and one for FGP (holds a
                        # numeric photo ID like "4677401"). Prefer the
                        # first non-numeric $z so we do not clobber the
                        # shelfmark with the FGP ID.
                        if val and not result.get('shelfmark_alt'):
                            result['shelfmark_alt'] = val
                        elif val and val.isdigit():
                            # Numeric $z is an FGP/ARK id, not a shelfmark.
                            # Skip when we already have a non-numeric one.
                            prior = result.get('shelfmark_alt', '')
                            if prior and not prior.isdigit():
                                pass  # keep the real shelfmark
                            else:
                                result['shelfmark_alt'] = val
                        elif val:
                            # Non-numeric $z and we already had something.
                            # Prefer the most-recent non-numeric (unlikely
                            # to have two real shelfmarks, but if we do,
                            # the later one usually wins for consistency
                            # with older behavior).
                            result['shelfmark_alt'] = val

                    elif tag == '597': # Image credit / attribution
                        val = get_sub('a')
                        if val: result['attribution'] = val

                self._marc_fail_cache.pop(system_id, None)
                _nli_record_success(path='fetch_marc_data')
                return result
            elif resp.status_code == 429:
                _nli_record_failure(failure_type='429', path='fetch_marc_data')
                LOGGER.warning(f"MARC 429 for {system_id}")
            elif 500 <= resp.status_code < 600:
                _nli_record_failure(failure_type='5xx', path='fetch_marc_data')
                LOGGER.warning(f"MARC {resp.status_code} for {system_id}")
            # 404 / other 4xx -> per-sys_id negative cache only (D-07);
            # do NOT increment the breaker.
            self._timestamp_cache_set(self._marc_fail_cache, system_id, time.time())
            return result
        except requests.exceptions.Timeout as e:
            LOGGER.warning(f"MARC fetch timeout for {system_id}: {e}")
            self._timestamp_cache_set(self._marc_fail_cache, system_id, time.time())
            _nli_record_failure(failure_type='timeout', path='fetch_marc_data')
            return result
        except requests.exceptions.ConnectionError as e:
            LOGGER.warning(f"MARC fetch connection error for {system_id}: {e}")
            self._timestamp_cache_set(self._marc_fail_cache, system_id, time.time())
            _nli_record_failure(failure_type='connection_error', path='fetch_marc_data')
            return result
        except Exception as e:
            # XML parse errors / data structure issues — code-level, not upstream.
            # Per-sys_id negative cache only; do NOT trip the breaker (D-07).
            LOGGER.warning(f"MARC parse failed for {system_id}: {e}")
            self._timestamp_cache_set(self._marc_fail_cache, system_id, time.time())
            return result

    def enrich_metadata(self, system_id, suffix=1):
        """Fetch extended metadata (IIIF/MARC), build Image List, and merge into cache.

        Args:
            system_id: NLI system ID
            suffix: IIIF manifest suffix (1=primary IE, 2+=secondary). When suffix != 1,
                    only the NLI images change; MARC, external images, and metadata stay
                    the same. The nli_cache is only updated for suffix=1 (primary).
        """
        if not system_id: return {}

        # Ensure basic meta exists
        if system_id not in self.nli_cache:
            self.fetch_nli_data(system_id)

        current_meta = self.nli_cache.get(system_id, {})
        current_meta['sys_id'] = system_id  # Store for downstream consumers

        # 1. Submit both NLI network calls concurrently (fetch_marc_data + fetch_iiif_manifest)
        # These are independent calls; running in parallel halves metadata fetch time.
        # We avoid 'with' context manager here because its __exit__ calls shutdown(wait=True),
        # which would block until both futures complete before we can process MARC results.
        # Instead, we submit both, process MARC first (for external IIIF dependency), then
        # await IIIF results later — allowing IIIF fetch to overlap with external IIIF logic.
        _executor = ThreadPoolExecutor(max_workers=2)
        marc_future = _executor.submit(self.fetch_marc_data, system_id)
        iiif_future = _executor.submit(self.fetch_iiif_manifest, system_id, suffix)
        _executor.shutdown(wait=False)  # Don't block; futures continue in background threads

        # Await MARC result first (needed for external IIIF logic below)
        try:
            marc_data = marc_future.result(timeout=MARC_FUTURE_TIMEOUT)
        except Exception:
            marc_data = {}  # Network/parse failure; proceed with empty metadata
        current_meta['marc'] = marc_data
        marc_attribution = marc_data.get('attribution')

        # 2. Determine Image Source (External CUDL vs Fallback NLI)
        image_list = []
        external_meta = {}

        # Check for External Link from MARC (e.g. CUDL)
        ext_link = marc_data.get('external_iiif_link')
        if ext_link:
            # Phase 84 follow-up: store the viewer URL form (cudl.lib.cam.ac.uk/view/...)
            # rather than the raw IIIF manifest JSON URL (cudl.lib.cam.ac.uk/iiif/...).
            # The "View on CUDL" external link in browse opens external_url in a new tab;
            # /iiif/ returns JSON ("unavailable page" to a human), /view/ is the gallery
            # page. Web-side browse_enrichment.py:199 already does this transform for
            # MARC; centralizing it here so the bridge supplement (3a) and the Mosseri
            # variant loop (3b) below get the same correct form.
            if 'cudl.lib.cam.ac.uk' in ext_link.lower():
                ext_link = ext_link.replace("/iiif/", "/view/")
                current_meta['external_provider'] = 'cambridge'
            current_meta['external_url'] = ext_link

        # Lists for multiple sources
        images_nli = []
        images_ext = []

        # Initialize crossref service once (used for both Cambridge supplement and NLI FL IDs)
        crossref_svc = _get_crossref_service()

        # 2a-supplement: if MARC didn't provide a CUDL link, try crossref sidecar
        # Phase 84: migrated from get_cambridge_manifest(norm_sm) to get_cambridge_manifest_with_bridge(shelfmark)
        # The wrapper owns normalization internally; pass raw shelfmark (Codex MEDIUM #6).
        if not ext_link and crossref_svc and crossref_svc.is_available():
            shelfmark = current_meta.get('shelfmark', '')
            if shelfmark:
                cam_manifest_url = crossref_svc.get_cambridge_manifest_with_bridge(shelfmark)
                if cam_manifest_url:
                    ext_link = cam_manifest_url
                    # Bridge returns the IIIF manifest URL form (/iiif/MS-...). Convert
                    # to the CUDL viewer URL (/view/MS-...) for the user-facing
                    # "View on CUDL" link. /iiif/ returns JSON; /view/ is the gallery.
                    current_meta['external_url'] = ext_link.replace("/iiif/", "/view/")
                    current_meta['external_provider'] = 'cambridge'
                    LOGGER.info(f"Using local Cambridge manifest for {system_id} from crossref sidecar")

        # 2a-mosseri: if crossref didn't find Cambridge manifest and this is Mosseri, try CUDL label construction
        # Phase 84: migrated from get_cambridge_manifest_by_label(label) to get_cambridge_manifest_with_bridge(variant)
        # Round 3 Codex HIGH #4: variant loop PRESERVED — wrapper takes one shelfmark; only the loop
        # knows which alternates to try for Mosseri rows whose primary shelfmark is not the resolving form.
        if not ext_link and crossref_svc and crossref_svc.is_available():
            lib_code = current_meta.get('lib_code') or self.csv_bank.get(system_id, {}).get('library_code', '')
            if lib_code == 'Mosseri':
                # Try all call_number variants for best CUDL match
                variants = self.csv_bank.get(system_id, {}).get('call_numbers_raw') or [current_meta.get('shelfmark', '')]
                for variant in variants:
                    cam_url = crossref_svc.get_cambridge_manifest_with_bridge(variant)
                    if cam_url:
                        ext_link = cam_url
                        # iiif manifest URL → viewer page URL (see 2a-supplement above).
                        current_meta['external_url'] = ext_link.replace("/iiif/", "/view/")
                        current_meta['external_provider'] = 'cambridge'
                        LOGGER.info(f"Using Mosseri CUDL manifest for {system_id} via variant {variant!r}")
                        break

        # 2a-manchester: build canvas entries directly from ALL crossref images (each has its own luna_id)
        if not ext_link and crossref_svc and crossref_svc.is_available():
            manchester_canvases = crossref_svc.get_manchester_canvases(system_id)
            if manchester_canvases:
                images_ext = manchester_canvases
                current_meta['external_provider'] = 'manchester'
                current_meta['attribution'] = 'The University of Manchester Library · CC BY-NC-SA 4.0'
                # Set a synthetic ext_link to prevent JTS/other fallback from running,
                # but skip fetch_external_iiif_data since we already have canvas entries
                ext_link = '__manchester_direct__'
                LOGGER.info(f"Using {len(manchester_canvases)} Manchester LUNA canvases for {system_id}")

        # 2a-jts: if no external link yet, try JTS Figgy manifest via crossref sidecar.
        # 260421 follow-up (L81): look up by sys_id (single JOIN on
        # nli_images ↔ jts_dpul) rather than iterating user-facing
        # call_number variants. The nli_images.Shelfmark column already
        # stores the canonical bare form ("ENA 1052.1") that jts_dpul
        # is keyed on, so one query replaces up to 16 variant lookups.
        if not ext_link and crossref_svc and crossref_svc.is_available():
            jts_urls = crossref_svc.get_jts_urls_for_sys_id(system_id)
            if jts_urls and jts_urls.get('manifest_url'):
                ext_link = jts_urls['manifest_url']
                current_meta['external_url'] = jts_urls.get('dpul_url') or ext_link
                current_meta['external_provider'] = 'jts'
                LOGGER.info(
                    f"Using JTS Figgy manifest for {system_id} "
                    f"(nli_images.Shelfmark={jts_urls.get('shelfmark')!r})"
                )

        # 2a. Fetch External IIIF (Cambridge / JTS — Manchester already resolved above)
        if ext_link and ext_link != '__manchester_direct__':
            ext_data = self.fetch_external_iiif_data(ext_link)
            if ext_data.get('canvases'):
                images_ext = ext_data['canvases'] # Format: [{'label': '...', 'url': '...'}]
                external_meta = ext_data.get('metadata', {})
                if not marc_attribution:
                    current_meta['attribution'] = ext_data.get('attribution')

        # 2a2. Check for Oxford Part images (if no Cambridge images)
        if not images_ext:
            part_id = self.get_part_for_folio(system_id)
            if part_id:
                current_meta['oxford_part_id'] = part_id
                part_meta = self.get_part_metadata(part_id)
                if part_meta:
                    current_meta['oxford_part_metadata'] = part_meta
                    if not current_meta.get('title') and part_meta.get('title'):
                        current_meta['title'] = part_meta['title']
                    if part_meta.get('direct_link'):
                        current_meta['external_url'] = part_meta['direct_link']

                part_images = self.get_part_images(part_id)
                if part_images:
                    # Convert Oxford Part images to the expected format (include thumb_url)
                    images_ext = [{
                        'label': img.get('label', ''),
                        'url': img.get('full_url', ''),
                        'thumb_url': img.get('thumb_url', ''),
                        'folio_num': img.get('folio_num')
                    } for img in part_images]
                    current_meta['attribution'] = "From the collections of the Bodleian Libraries, Oxford"
                    current_meta['thumb_url'] = part_images[0].get('thumb_url') or current_meta.get('thumb_url')

        # 2b. Fetch NLI IIIF manifest for image FL IDs (crossref FGPImageNumberId != IIIF FL number)
        # Crossref is used for metadata (folio labels, image count) but NOT for image URLs.
        crossref_labels = {}
        if crossref_svc and crossref_svc.is_available():
            crossref_images = crossref_svc.get_images(system_id)
            if crossref_images:
                for img in crossref_images:
                    name = img.get('image_name', '')
                    if name:
                        crossref_labels[name] = name

        # Await NLI IIIF manifest (submitted concurrently with MARC above)
        try:
            nli_iiif_data = iiif_future.result(timeout=NLI_IIIF_FUTURE_TIMEOUT)
        except Exception:
            nli_iiif_data = {}  # Network/parse failure; proceed with empty metadata
        if nli_iiif_data.get('canvas_map'):
            sorted_map = sorted(nli_iiif_data['canvas_map'].items(), key=lambda x: x[0])
            for fl_id, label in sorted_map:
                url = f"{Config.NLI_IIIF_BASE}/FL{fl_id}"
                images_nli.append({'label': label, 'url': url, 'fl_id': fl_id})

        if not current_meta.get('physical_desc'):
            current_meta['physical_desc'] = nli_iiif_data.get('physical_desc', '')

        if marc_attribution:
            current_meta['attribution'] = marc_attribution
        elif not current_meta.get('attribution'):
            current_meta['attribution'] = nli_iiif_data.get('attribution', '')

        if nli_iiif_data.get('canvas_map'):
            current_meta['canvas_map'] = nli_iiif_data['canvas_map']

        # Prioritize External if available, but keep both sets.
        # 260419-cfx / 260421-aln: when both lists are populated, classify CUDL
        # alignment per (folio_num, side) rather than just length. Misaligned
        # CUL/Cambridge manuscripts (count or position mismatch) default to
        # NLI; CUDL stays in images_ext so UI can still offer it as a manual
        # switch. Non-CUL Cambridge (Mosseri, Gaster, private collections) is
        # intentionally exempt — those have NLI stubs that 503, so images_ext
        # must remain primary there (see gui_threads.py / web/pages/puzzle.py).
        ext_provider = current_meta.get('external_provider', '')
        lib_code = self.csv_bank.get(system_id, {}).get('library_code', '')
        cambridge_alignment = None
        if (
            images_ext and images_nli
            and ext_provider == 'cambridge' and lib_code == 'CUL'
        ):
            from shared.nli_crossref_service import classify_cambridge_alignment
            cambridge_alignment = classify_cambridge_alignment(
                system_id, images_ext, svc=crossref_svc,
            )
            current_meta['cambridge_alignment'] = cambridge_alignment

        misaligned = bool(
            cambridge_alignment and cambridge_alignment.get('verdict') == 'misaligned'
        )
        if misaligned:
            current_meta['images'] = images_nli
            current_meta['external_count_mismatch'] = True
        else:
            current_meta['images'] = images_ext if images_ext else images_nli
        current_meta['images_nli'] = images_nli
        current_meta['images_ext'] = images_ext
        current_meta['external_meta'] = external_meta

        # 3. Enrich with image source info and folio labels (Phase 31: IMG-04)
        if crossref_svc and crossref_svc.is_available():
            try:
                shelfmark = current_meta.get('shelfmark', '')
                norm_sm = normalize_shelfmark(shelfmark) if shelfmark else ''
                current_meta['image_source_info'] = crossref_svc.get_image_sources(
                    system_id, normalized_shelfmark=norm_sm, shelfmark=shelfmark
                )
                current_meta['folio_images'] = crossref_svc.get_folio_images(system_id)

                # Physical metadata (Phase 32: META-01, META-02)
                phys_meta = crossref_svc.get_physical_metadata(system_id)
                if phys_meta:
                    current_meta['physical_metadata'] = phys_meta

                # Library viewer URL (Phase 32: META-04)
                lib_url = crossref_svc.get_library_viewer_url(system_id)
                if lib_url:
                    current_meta['library_viewer_url'] = lib_url

                # Phase 33: Metadata enrichment from NLI crossref
                # Neubauer-Cowley catalog entry (Oxford manuscripts)
                catalog_entry = crossref_svc.get_catalog_entry(system_id)
                if catalog_entry:
                    current_meta['catalog_entry'] = catalog_entry

                # Collection and storage references
                coll_storage = crossref_svc.get_collection_storage(system_id)
                if coll_storage:
                    current_meta['collection_storage'] = coll_storage
            except Exception as e:
                LOGGER.debug(f"Folio enrichment error for {system_id}: {e}")

        # Phase 33: FJMS bibliography and catalog references
        fjms_svc = _get_fjms_service()
        if fjms_svc and fjms_svc.is_available():
            try:
                bib_entries = fjms_svc.get_bibliography(system_id)
                if bib_entries:
                    current_meta['bibliography'] = bib_entries

                cat_refs = fjms_svc.get_catalog_refs(system_id)
                if cat_refs:
                    current_meta['catalog_refs'] = cat_refs

                source_names = fjms_svc.get_source_names(system_id)
                if source_names:
                    current_meta['source_names'] = source_names
            except Exception as e:
                LOGGER.debug(f"FJMS metadata enrichment error for {system_id}: {e}")

        # Update cache precedence (Enrichment overrides basic placeholders)
        if marc_data.get('english_title') and not current_meta.get('title'):
            current_meta['title'] = marc_data['english_title']

        if marc_data.get('shelfmark_alt') and (not current_meta.get('shelfmark') or current_meta.get('shelfmark') == 'Unknown'):
            current_meta['shelfmark'] = marc_data['shelfmark_alt']

        self.nli_cache[system_id] = current_meta
        return current_meta

    def fetch_volume_manifest(self, system_id, suffix):
        """Lightweight manifest-only fetch for volume switches (no MARC, no FJMS, no crossref).

        Returns a dict with 'images_nli' list for the requested suffix, suitable for
        updating ManuscriptViewerWidget without re-running full enrichment.
        """
        iiif_data = self.fetch_iiif_manifest(system_id, suffix=suffix)
        images_nli = []
        if iiif_data.get('canvas_map'):
            sorted_map = sorted(iiif_data['canvas_map'].items(), key=lambda x: x[0])
            for fl_id, label in sorted_map:
                url = f"{Config.NLI_IIIF_BASE}/FL{fl_id}"
                images_nli.append({'label': label, 'url': url, 'fl_id': fl_id})
        return {'images_nli': images_nli, 'suffix': suffix}

    def fetch_external_iiif_data(self, view_url):
        """
        Generic handler to fetch external IIIF data.
        Currently supports CUDL logic: /view/ -> /iiif/ manifest.
        Returns: {'attribution': str, 'metadata': dict, 'canvases': [{'label': str, 'url': str}]}
        """
        if not view_url: return {}

        # CUDL Conversion Logic
        manifest_url = view_url
        if "cudl.lib.cam.ac.uk/view/" in view_url:
            base = view_url.replace("/view/", "/iiif/")
            manifest_url = re.sub(r'/\d+$', '', base)

        if manifest_url.startswith("http://"):
            manifest_url = manifest_url.replace("http://", "https://")

        result = {
            'attribution': 'External Library',
            'metadata': {},
            'canvases': []
        }

        try:
            session = self._make_session()
            # 260421 follow-up (L81 lag): 10s was overkill — Figgy/CUDL
            # normally respond in <2s. Shorten so a slow external host
            # does not gate the whole browse navigation.
            resp = session.get(manifest_url, timeout=EXTERNAL_IIIF_HTTP_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()

                # Attribution
                attr = data.get('attribution')
                if isinstance(attr, str):
                    result['attribution'] = attr
                elif isinstance(attr, list) and attr:
                    result['attribution'] = str(attr[0])
                elif data.get('label'):
                    result['attribution'] = str(data.get('label'))

                # Metadata
                if 'metadata' in data:
                    import re as _re_meta
                    for item in data['metadata']:
                        label = str(item.get('label', '')).lower()
                        val = str(item.get('value', ''))
                        if label in ['abstract', 'condition', 'provenance', 'physical description']:
                            # Strip HTML tags from IIIF manifest values (e.g. <p style='...'> wrappers)
                            val = _re_meta.sub(r'<[^>]+>', '', val).strip()
                            result['metadata'][label.title()] = val

                # Canvases (Images with Labels)
                if 'sequences' in data and data['sequences']:
                    for idx, canvas in enumerate(data['sequences'][0].get('canvases', [])):
                        lbl = canvas.get('label', f"Img {idx + 1}")
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            # Try to get the service ID (base URL) for flexible resizing
                            img_id = service.get('@id') if service else resource.get('@id')

                            if img_id:
                                # Extract folio_num + folio_side from label for
                                # proper page indexing. Labels like "1", "1r",
                                # "1v", "f.2v" → (1,'r'|'v'); "Binding"/"Cover"
                                # → (None, None). See _parse_cudl_label below.
                                folio_num, folio_side = _parse_cudl_label(lbl)
                                result['canvases'].append({
                                    'label': lbl, 'url': img_id,
                                    'folio_num': folio_num,
                                    'folio_side': folio_side,
                                })

            return result
        except Exception as e:
            LOGGER.warning(f"External IIIF fetch failed for {view_url}: {e}")
            return result

    def _fetch_single_worker(self, system_id):
        # Phase 85 D-14: synthetic sys_ids skip the NLI MARC network call
        # (no Alma record exists). Returns the default empty meta structure.
        from shared.synthetic_sys_id import is_synthetic_sys_id
        meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': [], 'thumb_url': None, 'thumb_checked': False}
        if is_synthetic_sys_id(system_id):
            # Must return the (system_id, meta) 2-tuple like every other path —
            # callers unpack it (`_, meta = ...` at fetch_nli_data; `sid, meta =
            # future.result()` in batch_fetch_shelfmarks). Returning a bare dict
            # raised "ValueError: too many values to unpack" for any synthetic
            # sys_id absent from both nli_cache and csv_bank (audit 2026-05-29).
            return system_id, meta

        # Phase 98 D-22: shared NLI circuit breaker guard. Without this, the
        # retry loop below (2 attempts * (10s + 1s sleep) = up to 22s of
        # blocking) re-burns on every navigation when NLI is degraded.
        # `is_synthetic_sys_id` check is intentionally BEFORE this so synthetic
        # sys_ids do not consume breaker logic (Phase 85 D-14 invariant).
        if _nli_circuit_is_open():
            return system_id, meta

        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"

        headers = Config.HTTP_HEADERS

        import time

        for attempt in range(2):
            # Phase 98 Codex REVIEW Issue 3: per-iteration breaker recheck.
            # If the first attempt tripped the breaker, the second retry
            # must short-circuit immediately rather than burn another timeout.
            if _nli_circuit_is_open():
                break
            try:
                time.sleep(0.3)
                session = self._make_session()
                resp = session.get(
                    url,
                    headers=headers,
                    timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT),
                )

                if resp.status_code == 200:
                    try:
                        root = ET.fromstring(resp.content)

                        # --- 1. Extract Representative FL (907 $d) ---
                        # This is the "Cover Image" or main representative FL
                        rep_fl = None
                        for df in root.findall("marc:datafield[@tag='907']", self.ns):
                            sf = df.find("marc:subfield[@code='d']", self.ns)
                            if sf is not None and sf.text:
                                clean_fl = sf.text.strip()
                                if clean_fl.startswith("FL"):
                                    rep_fl = clean_fl
                                    break

                        # --- 2. Extract Standard Metadata ---
                        c_942 = None; c_907 = None; c_090 = None; c_avd = None
                        fl_ids = self._extract_fl_ids(root) # Backup list

                        for df in root.findall('marc:datafield', self.ns):
                            tag = df.get('tag')
                            def get_val(code):
                                sf = df.find(f"marc:subfield[@code='{code}']", self.ns)
                                return sf.text if sf is not None else None

                            if tag == '942':
                                val = get_val('z')
                                if val:
                                    if not c_942: c_942 = val
                                    elif val.isdigit(): pass
                                    else: c_942 = val
                            elif tag == '907':
                                val = get_val('e')
                                if val: c_907 = val
                            elif tag == '090':
                                val = get_val('a')
                                if val and "MSS" not in val: c_090 = val
                            elif tag == 'AVD':
                                val = get_val('e')
                                if val: c_avd = val
                            elif tag == '245':
                                val = get_val('a')
                                if val: meta['title'] = val.rstrip('./,:;')

                        final = c_942 or c_907 or c_090 or c_avd
                        if final: meta['shelfmark'] = final

                        meta['fl_ids'] = fl_ids

                        # --- 3. Set Thumbnail URL ---
                        # PRIORITIZE the Representative FL found in 907 $d
                        if rep_fl:
                             meta['thumb_url'] = self._resolve_thumbnail([rep_fl])
                        else:
                             # Only if missing, fallback to the list
                             meta['thumb_url'] = self._resolve_thumbnail(fl_ids)

                        meta['thumb_checked'] = True
                        _nli_record_success(path='_fetch_single_worker')
                        return system_id, meta

                    except ET.ParseError:
                        # Parse error — code-level, not upstream. Bail out of
                        # the retry loop without incrementing the breaker (D-07).
                        break
                elif resp.status_code == 429:
                    _nli_record_failure(failure_type='429', path='_fetch_single_worker')
                    time.sleep(1)
                elif 500 <= resp.status_code < 600:
                    _nli_record_failure(failure_type='5xx', path='_fetch_single_worker')
                    time.sleep(1)
                else:
                    # 404 / other 4xx — break out, do not touch the breaker.
                    break
            except requests.exceptions.Timeout:
                _nli_record_failure(failure_type='timeout', path='_fetch_single_worker')
                time.sleep(1)
            except requests.exceptions.ConnectionError:
                _nli_record_failure(failure_type='connection_error', path='_fetch_single_worker')
                time.sleep(1)
            except Exception:
                # Non-network error (DNS lookup quirks, etc.) — preserve
                # existing retry-with-sleep behavior but do NOT touch the
                # breaker (D-07).
                time.sleep(1)

        return system_id, meta

    def _extract_fl_ids(self, root):
        fl_ids = []
        for df in root.findall("marc:datafield[@tag='907']", self.ns):
            for sf in df.findall("marc:subfield[@code='d']", self.ns):
                val = (sf.text or "").strip()
                if val.startswith("FL"):
                    fl_ids.append(val)
        return fl_ids

    def _resolve_thumbnail(self, fl_ids, size=320, session=None):
        if not fl_ids: return None

        # Ensure it's iterable but treat string as single item list
        if isinstance(fl_ids, str): fl_ids = [fl_ids]

        for fl_id in fl_ids:
            if not fl_id: continue

            # Robust extraction of digits
            raw_str = str(fl_id)
            digits = re.sub(r"\D", "", raw_str)

            # Basic validation: FL IDs are usually long (e.g. 7+ digits)
            if not digits or len(digits) < 4: continue

            # Return the URL that worked in debug
            return f"{Config.NLI_IIIF_BASE}/FL{digits}/full/400,/0/default.jpg"

        return None

    @staticmethod
    def get_rosetta_fallback_url(fl_id):
        """Construct a fallback URL for Rosetta stream if IIIF fails.
        Returns full-resolution image (TIFF) from Rosetta delivery."""
        if not fl_id: return None
        raw_str = str(fl_id)
        digits = re.sub(r"\D", "", raw_str)
        if not digits: return None
        return f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=stream&dps_pid=FL{digits}"

    def _fetch_fl_ids(self, system_id):
        # Phase 85 D-14: synthetic sys_ids have no NLI manifest — skip the network call.
        from shared.synthetic_sys_id import is_synthetic_sys_id
        if is_synthetic_sys_id(system_id):
            return []

        # Phase 98 D-23: shared NLI circuit breaker guard. `is_synthetic_sys_id`
        # check is intentionally BEFORE this so synthetic sys_ids do not consume
        # breaker logic (Phase 85 D-14 invariant).
        if _nli_circuit_is_open():
            return []

        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"
        headers = Config.HTTP_HEADERS
        try:
            session = self._make_session()
            resp = session.get(
                url,
                headers=headers,
                timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT),
                allow_redirects=True,
            )
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                _nli_record_success(path='_fetch_fl_ids')
                return self._extract_fl_ids(root)
            elif resp.status_code == 429:
                _nli_record_failure(failure_type='429', path='_fetch_fl_ids')
            elif 500 <= resp.status_code < 600:
                _nli_record_failure(failure_type='5xx', path='_fetch_fl_ids')
            # 404 / other 4xx -> just return [] without touching the breaker (D-07).
        except requests.exceptions.Timeout as e:
            LOGGER.debug('Metadata batch fetch timeout: %s', e)
            _nli_record_failure(failure_type='timeout', path='_fetch_fl_ids')
            return []
        except requests.exceptions.ConnectionError as e:
            LOGGER.debug('Metadata batch fetch connection error: %s', e)
            _nli_record_failure(failure_type='connection_error', path='_fetch_fl_ids')
            return []
        except Exception as e:
            LOGGER.debug('Metadata batch fetch failed (non-network): %s', e)
            return []
        return []

    def get_thumbnail(self, system_id, size=320):
        meta = self.nli_cache.get(system_id)
        if meta and meta.get('thumb_checked') and meta.get('thumb_url'):
            return meta.get('thumb_url')

        fl_ids = []
        if meta:
            fl_ids = meta.get('fl_ids', [])
        if not fl_ids:
            fl_ids = self._fetch_fl_ids(system_id)

        thumb_url = self._resolve_thumbnail(fl_ids, size=size)

        if meta is None:
            meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': fl_ids}
        meta['fl_ids'] = fl_ids
        meta['thumb_url'] = thumb_url
        meta['thumb_checked'] = True
        self.nli_cache[system_id] = meta
        return thumb_url

    def batch_fetch_shelfmarks(self, system_ids, progress_callback=None, use_network=True, check_cancel=None):
        """
        Populate metadata cache.
        use_network=False -> Only loads from local CSV/Cache (Instant).
        use_network=True  -> Fetches missing items from NLI.
        """
        # Step A: Fast fetch from CSV (no network)
        for sid in system_ids:
            if check_cancel and check_cancel(): return
            if sid not in self.nli_cache and sid in self.csv_bank:
                self.fetch_nli_data(sid) # This fetches from CSV automatically now

        # If only local work requested, stop here
        if not use_network:
            return

        # Step B: Identify what is *really* missing
        to_fetch = [sid for sid in system_ids if sid not in self.nli_cache]

        if not to_fetch:
            if progress_callback:
                for i, sid in enumerate(system_ids):
                     progress_callback(i + 1, len(system_ids), sid)
            return

        # Step C: Network download (only if use_network=True)
        futures = {self.nli_executor.submit(self._fetch_single_worker, sid): sid for sid in to_fetch}
        current_progress = len(system_ids) - len(to_fetch)

        for future in as_completed(futures):
            if check_cancel and check_cancel(): break
            sid, meta = future.result()
            self.nli_cache[sid] = meta
            current_progress += 1
            if progress_callback:
                progress_callback(current_progress, len(system_ids), sid)

        self.save_caches()

    def search_by_meta(self, query, field):
        """Search for system IDs where the specified field matches the query.

        Uses precise matching to avoid false positives like 120.2 matching 120.25.
        For shelfmarks, uses normalization to handle format variations like:
        - "ts12.123" or "T-S 12 123" matching "T-S 12.123"
        - Missing/extra spaces, dashes, dots
        - Case insensitivity
        """
        results = set()
        q_norm = query.lower().strip()

        # For shelfmark searches, also compute fully normalized version
        q_normalized = self._normalize_shelfmark(query) if field == 'shelfmark' else None

        # Helper function for smart matching
        def matches(value, query_norm):
            val_norm = value.lower().strip()

            # 1. Exact match (case-insensitive)
            if val_norm == query_norm:
                return True

            # 2. For shelfmarks, use normalized matching to handle format variations
            if field == 'shelfmark':
                # Normalize the value and compare with normalized query
                val_normalized = self._normalize_shelfmark(value)

                # Exact normalized match: "ts12.123" matches "T-S 12.123"
                if val_normalized == q_normalized:
                    return True

                # Dot-agnostic match: "ts12123" matches "ts12.123"
                # This allows users to type without dots and still find results
                if val_normalized.replace('.', '') == q_normalized.replace('.', ''):
                    return True

                # Normalized prefix match: "ts12" matches "T-S 12.123"
                # But be careful with numeric boundaries
                if q_normalized and val_normalized.startswith(q_normalized):
                    next_pos = len(q_normalized)
                    if next_pos < len(val_normalized):
                        next_char = val_normalized[next_pos]
                        # Allow if next char is a dot (e.g., "ts12" -> "ts12.123")
                        # or if it's not a digit (e.g., "ts12" -> "ts12a")
                        if next_char == '.' or not next_char.isdigit():
                            return True
                        # If query ends with digit and value continues with digit,
                        # don't immediately return - fall through to token matching
                        # This allows "T-S NS 12" to match "T-S NS 120.2" via tokens
                    else:
                        return True

                # Also try token-based matching for partial searches
                # Tokenize both query and value
                val_tokens = [t for t in re.split(r'[\s\.\-]+', val_norm) if t]
                query_tokens = [t for t in re.split(r'[\s\.\-]+', query_norm) if t]

                if not query_tokens:
                    return False

                # Match tokens sequentially: "t-s ns 12.4" must match tokens in order
                query_idx = 0
                val_idx = 0

                while query_idx < len(query_tokens) and val_idx < len(val_tokens):
                    qt = query_tokens[query_idx]
                    vt = val_tokens[val_idx]

                    # Check if tokens match
                    if qt == vt:
                        # Exact match - advance both
                        query_idx += 1
                        val_idx += 1
                    elif vt.startswith(qt):
                        # Prefix match
                        # For the LAST query token, allow numeric prefix (e.g., "12" matches "120")
                        # For earlier tokens, be strict to avoid false positives
                        if query_idx == len(query_tokens) - 1:
                            # Last token - allow prefix match even for digits
                            query_idx += 1
                            val_idx += 1
                        elif qt.isdigit() and vt.isdigit():
                            # Not last token and both numeric - be strict
                            if len(vt) > len(qt) and vt[len(qt)].isdigit():
                                val_idx += 1
                                continue
                            query_idx += 1
                            val_idx += 1
                        else:
                            query_idx += 1
                            val_idx += 1
                    else:
                        val_idx += 1

                return query_idx == len(query_tokens)

            # 3. For other fields (title), use substring match
            else:
                return query_norm in val_norm

            return False

        # 1. Search in CSV Bank (Fastest) — snapshot to avoid concurrent modification
        for sys_id, data in list(self.csv_bank.items()):
            val = data.get(field, '')
            if val and matches(val, q_norm):
                results.add(sys_id)

        # 2. Search in NLI Cache (for items not in CSV or updated)
        for sys_id, data in self.nli_cache.items():
            val = data.get(field, '')
            if val and matches(val, q_norm):
                results.add(sys_id)

        # Phase 84: CUDL classmark fallback (NORM-01/02). Zero-regression by construction:
        # only runs when canonical matching returned no hits.
        if field == 'shelfmark' and not results:
            try:
                from shared.shelfmark_bridge import lookup_cudl
                hit = lookup_cudl(query)
                if hit and hit.get('sys_id'):
                    results.add(hit['sys_id'])
            except ImportError as e:
                _warn_bridge_import_failed(e)
            except Exception as e:
                LOGGER.debug("Bridge fallback failed for query %r: %s", query, e)

        return list(results)

    # ---------------- Shelfmark Resolution Helpers ----------------
    def _normalize_shelfmark(self, shelfmark: str) -> str:
        """Normalize shelfmarks using the canonical module-level function."""
        return normalize_shelfmark(shelfmark)

    def _iter_shelfmark_sources(self):
        """Yield shelfmark candidates from CSV bank and cached metadata."""
        # CSV bank
        for sys_id, data in self.csv_bank.items():
            title = data.get('title', '')
            variants = data.get('call_numbers_raw') or [data.get('shelfmark', '')]
            for shelf in variants:
                if shelf:
                    yield sys_id, shelf, title
        # NLI cache (may contain enriched shelfmarks)
        for sys_id, data in self.nli_cache.items():
            shelf = data.get('shelfmark', '')
            alt = data.get('shelfmark_alt', '')
            title = data.get('title', '')
            for candidate in [shelf, alt]:
                if candidate:
                    yield sys_id, candidate, title

    def _get_shelfmark_index(self):
        """Build or return cached pre-normalized shelfmark index.

        Returns list of (sys_id, shelfmark, title, norm_shelf, norm_shelf_no_dots) tuples,
        already deduplicated by (sys_id, norm_shelf).
        """
        if hasattr(self, '_shelfmark_index') and self._shelfmark_index is not None:
            # The web app loads libraries.csv in a background thread. If shelfmark search runs
            # before that finishes, we can cache an empty index and then keep reusing it forever.
            # Rebuild once csv_bank has data.
            if self._shelfmark_index or not self.csv_bank:
                return self._shelfmark_index
            self._shelfmark_index = None

        if not self.csv_bank:
            # Fall back to a synchronous load when browse shelfmark search beats the background loader.
            self._load_csv_bank()

        if self._shelfmark_index is not None:
            return self._shelfmark_index

        index = []
        seen = set()
        for sys_id, shelf, title in self._iter_shelfmark_sources():
            norm_shelf = normalize_shelfmark(shelf)
            if not norm_shelf or (sys_id, norm_shelf) in seen:
                continue
            seen.add((sys_id, norm_shelf))
            index.append((sys_id, shelf, title, norm_shelf, norm_shelf.replace('.', '')))

        self._shelfmark_index = index
        return index

    def resolve_system_by_shelfmark(self, query, limit=100):
        """
        Resolve a system ID by shelfmark, ignoring dots/slashes/spaces.
        Returns a dict: {'sys_id': ..., 'options': [...], 'selected_shelfmark': ...}
        """
        result = {'sys_id': None, 'options': [], 'selected_shelfmark': None}

        # Strip known library prefix (code, full name, or common abbreviation)
        if query:
            q = query.strip()
            q = _strip_library_prefix(q)
            query = q

        norm_query = self._normalize_shelfmark(query)
        if not norm_query:
            return result

        exact_matches = []
        partial_matches = []

        def shelf_sort_key(entry):
            shelf = entry.get('shelfmark', '')
            title = entry.get('title', '')
            sid_val = entry.get('sys_id', '')
            return (natural_sort_key(shelf), natural_sort_key(title), natural_sort_key(sid_val))

        # Dot-agnostic matching only when query has NO dots (e.g. "19234" → "19.234").
        # When query has dots (e.g. "31.1"), dots are semantically significant — don't strip.
        query_has_dots = '.' in norm_query
        norm_query_no_dots = norm_query.replace('.', '') if not query_has_dots else None

        for sys_id, shelf, title, norm_shelf, norm_shelf_no_dots in self._get_shelfmark_index():
            entry = {'sys_id': sys_id, 'shelfmark': shelf, 'title': title}

            # Standard matching (preserves dots — "tsas31.1" != "tsas3.11")
            if norm_shelf == norm_query:
                exact_matches.append(entry)
            elif norm_query in norm_shelf:
                partial_matches.append(entry)
            elif norm_query_no_dots and norm_shelf_no_dots == norm_query_no_dots:
                # Dot-agnostic match only for dotless queries (e.g. "19234" == "19.234")
                exact_matches.append(entry)

        # Phase 84 follow-up: bridge resolution as exact match. The legacy
        # normalize_shelfmark() does NOT collapse CUDL leading zeros or merge
        # slash/comma/dot variants, so user queries in the canonical/NLI form
        # (e.g. 'T-S F 8.2') never match libraries.csv rows stored only in CUDL
        # classmark form (e.g. 'Ms. T-S F 8/002' → norm 'tsf8.002'). When the
        # bridge resolves the query to a sys_id that's absent from exact_matches,
        # promote it to exact-match status — that row is the user's intended
        # target, and ranking it ahead of substring prefix-noise (8.20/8.21/8.22)
        # gives a single-exact-match path back to the picker-free direct browse.
        # UAT Test 1 sub-issue 1b: 'T-S F 8.2' resolves to row 990026242400205171.
        try:
            from shared.shelfmark_bridge import lookup_cudl
            bridge_hit = lookup_cudl(query)
        except ImportError as _e:
            _warn_bridge_import_failed(_e)
            bridge_hit = None
        except Exception:
            bridge_hit = None

        if bridge_hit and bridge_hit.get('sys_id'):
            _bsid = bridge_hit['sys_id']
            if not any(e.get('sys_id') == _bsid for e in exact_matches):
                _bdata = self.csv_bank.get(_bsid, {})
                _bshelf = _bdata.get('shelfmark') or bridge_hit.get('shelfmark', '') or ''
                _btitle = _bdata.get('title', '')
                # Insert at front so the dedup-then-single-exact path returns
                # this row as the resolved sys_id when no canonical exact match
                # exists.
                exact_matches.insert(0, {
                    'sys_id': _bsid,
                    'shelfmark': _bshelf,
                    'title': _btitle,
                })

        # Deduplicate exact matches by sys_id (e.g. "T-S AS 31.1" and "Ms. T-S AS 31.1"
        # are the same manuscript). Keep the shortest shelfmark as most user-friendly.
        if exact_matches:
            by_sid = {}
            for e in exact_matches:
                sid = e['sys_id']
                if sid not in by_sid or len(e['shelfmark']) < len(by_sid[sid]['shelfmark']):
                    by_sid[sid] = e
            exact_matches = list(by_sid.values())

        if len(exact_matches) == 1:
            result['sys_id'] = exact_matches[0]['sys_id']
            result['selected_shelfmark'] = exact_matches[0]['shelfmark']
            return result

        # If we have exact matches, prefer them over partials
        if exact_matches:
            exact_matches.sort(key=shelf_sort_key)
            result['options'] = exact_matches[:limit]
            return result

        partial_matches.sort(key=shelf_sort_key)
        result['options'] = partial_matches[:limit]
        return result

    def get_display_data(self, full_header, src_label):
        sys_id, p_num = self.parse_header_smart(full_header)

        # Use get_meta_for_id which has proper fallback logic (CSV > NLI Cache)
        shelfmark, title = self.get_meta_for_id(sys_id)

        # Get library code
        library_code = self.get_library_for_id(sys_id)

        return {
            'shelfmark': shelfmark or f"ID: {sys_id}",
            'title': title,
            'img': p_num,
            'source': src_label,
            'id': sys_id,
            'library_code': library_code,
        }
