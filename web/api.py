import logging
import json

from nicegui import app
from fastapi import Response
from fastapi.responses import RedirectResponse
from starlette.requests import Request
from web.state import state
from web.export_service import get_export_service, encode_filename_for_header
import requests
import requests.adapters
import re
import os
import threading
import time as _module_time
from collections import OrderedDict
from genizah_core import Config
from shared.synthetic_sys_id import is_synthetic_sys_id
from urllib.parse import urlparse
from web.crawler_visibility import ARCHIVE_DISALLOWED_PATHS, ARCHIVE_USER_AGENT_TOKENS

logger = logging.getLogger(__name__)

# NOTE: Backend API routes removed - now using Supabase directly
# Auth, users, corrections, comments, discoveries all go through Supabase

# Cache TTL values (configurable via environment variables)
NLI_CACHE_TTL = int(os.environ.get('NLI_CACHE_TTL', '300'))  # 5 minutes default
NLI_FAIL_CACHE_TTL = 60  # negative-cache failures for 60s to avoid hammering NLI
NLI_DISK_CACHE_TTL = int(os.environ.get('NLI_DISK_CACHE_TTL', str(30 * 24 * 60 * 60)))  # 30 days
IMAGE_CACHE_TTL = int(os.environ.get('IMAGE_CACHE_TTL', '600'))  # 10 minutes default
NLI_MAX_CONCURRENT_FETCHES = max(1, int(os.environ.get('NLI_MAX_CONCURRENT_FETCHES', '8')))
NLI_SEMAPHORE_TIMEOUT = int(os.environ.get('NLI_SEMAPHORE_TIMEOUT', '20'))
NLI_PERSISTENT_CACHE_FILE = os.path.join(Config.INDEX_DIR, 'nli_fl_ids_cache.json')
IMAGE_CACHE_MAX_BYTES = max(0, int(os.environ.get('IMAGE_CACHE_MAX_BYTES', str(256 * 1024 * 1024))))
IMAGE_CACHE_MAX_ENTRIES = max(0, int(os.environ.get('IMAGE_CACHE_MAX_ENTRIES', '512')))
NLI_MEMORY_CACHE_MAX_ENTRIES = max(0, int(os.environ.get('NLI_MEMORY_CACHE_MAX_ENTRIES', '50000')))
PUZZLE_RATE_LIMIT_BUCKET_TTL = max(60, int(os.environ.get('PUZZLE_RATE_LIMIT_BUCKET_TTL', '3600')))

# Concurrency cap for NLI IIIF fetches (prevents burst-flooding the upstream)
_nli_semaphore = threading.Semaphore(NLI_MAX_CONCURRENT_FETCHES)

# Phase 92.2 D-NLI-01: serialize os.replace across concurrent background threads
# to avoid [WinError 5] access-denied races on Windows. Lock is acquired BEFORE
# writing the temp file so concurrent callers queue rather than race on the rename.
_nli_persist_lock = threading.Lock()

# Persistent session for NLI requests (connection reuse via keep-alive)
_nli_session = requests.Session()
_nli_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
})
_nli_adapter = requests.adapters.HTTPAdapter(
    pool_connections=NLI_MAX_CONCURRENT_FETCHES,
    pool_maxsize=max(8, NLI_MAX_CONCURRENT_FETCHES * 2),
)
_nli_session.mount('https://iiif.nli.org.il', _nli_adapter)


def _estimate_cache_value_bytes(value) -> int:
    """Approximate the heap footprint of cache values dominated by image bytes."""
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value.encode('utf-8', errors='ignore'))
    if isinstance(value, dict):
        return sum(
            _estimate_cache_value_bytes(k) + _estimate_cache_value_bytes(v)
            for k, v in value.items()
        )
    if isinstance(value, (tuple, list, set)):
        return sum(_estimate_cache_value_bytes(v) for v in value)
    return 64


class _TTLMemoryCache:
    """Small thread-safe LRU+TTL cache for response bytes.

    The previous route-local dict caches stored full image responses forever
    when every request used a new key. This cache evicts on age, entry count,
    and approximate byte budget so crawls or gallery browsing cannot ratchet
    process RSS upward indefinitely.
    """

    def __init__(self, name: str, *, ttl_seconds: int, max_entries: int, max_bytes: int):
        self.name = name
        self.ttl_seconds = max(1, int(ttl_seconds))
        self.max_entries = max(0, int(max_entries))
        self.max_bytes = max(0, int(max_bytes))
        self._items: OrderedDict[object, tuple[object, int, float]] = OrderedDict()
        self._total_bytes = 0
        self._lock = threading.Lock()

    def get(self, key):
        now = _module_time.time()
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            value, size, cached_at = item
            if now - cached_at >= self.ttl_seconds:
                self._remove_locked(key)
                return None
            self._items.move_to_end(key)
            return value

    def set(self, key, value) -> None:
        if self.max_entries == 0 or self.max_bytes == 0:
            return
        now = _module_time.time()
        size = _estimate_cache_value_bytes(value)
        if size > self.max_bytes:
            return
        with self._lock:
            self._remove_locked(key)
            self._items[key] = (value, size, now)
            self._total_bytes += size
            self._evict_locked(now)

    def _remove_locked(self, key) -> None:
        item = self._items.pop(key, None)
        if item is not None:
            self._total_bytes -= item[1]

    def _evict_locked(self, now: float) -> None:
        expired = [
            key for key, (_, _, cached_at) in self._items.items()
            if now - cached_at >= self.ttl_seconds
        ]
        for key in expired:
            self._remove_locked(key)
        while self.max_entries and len(self._items) > self.max_entries:
            _key, (_value, size, _cached_at) = self._items.popitem(last=False)
            self._total_bytes -= size
        while self.max_bytes and self._total_bytes > self.max_bytes and self._items:
            _key, (_value, size, _cached_at) = self._items.popitem(last=False)
            self._total_bytes -= size

    def stats(self) -> dict:
        now = _module_time.time()
        with self._lock:
            self._evict_locked(now)
            return {
                'entries': len(self._items),
                'bytes_estimate': max(0, self._total_bytes),
                'max_entries': self.max_entries,
                'max_bytes': self.max_bytes,
                'ttl_seconds': self.ttl_seconds,
            }


_RUNTIME_CACHE_STAT_PROVIDERS = {}


def _register_runtime_cache_stats(name: str, provider) -> None:
    _RUNTIME_CACHE_STAT_PROVIDERS[name] = provider


def get_runtime_cache_stats() -> dict:
    """Return lightweight diagnostics for route-local runtime caches."""
    stats = {}
    for name, provider in list(_RUNTIME_CACHE_STAT_PROVIDERS.items()):
        try:
            stats[name] = provider()
        except Exception:
            stats[name] = {'error': True}
    return stats


def _make_image_cache(name: str) -> _TTLMemoryCache:
    shard_count = 5
    max_bytes = IMAGE_CACHE_MAX_BYTES // shard_count if IMAGE_CACHE_MAX_BYTES else 0
    max_entries = IMAGE_CACHE_MAX_ENTRIES // shard_count if IMAGE_CACHE_MAX_ENTRIES else 0
    cache = _TTLMemoryCache(
        name,
        ttl_seconds=IMAGE_CACHE_TTL,
        max_entries=max(1, max_entries) if IMAGE_CACHE_MAX_ENTRIES else 0,
        max_bytes=max(1, max_bytes) if IMAGE_CACHE_MAX_BYTES else 0,
    )
    _register_runtime_cache_stats(name, cache.stats)
    return cache

# Allowed domains for image proxy (prevents SSRF attacks)
ALLOWED_IMAGE_DOMAINS = [
    'rosetta.nli.org.il',
    'iiif.nli.org.il',
    'www.nli.org.il',
    'nli.org.il',
    'hebrew.bodleian.ox.ac.uk',
    'luna.manchester.ac.uk',
    'iiif-cloud.princeton.edu',
    'figgy.princeton.edu',
]


def _normalize_persisted_fl_ids(fl_ids) -> list[str]:
    """Normalize persisted FL IDs to a clean list of digit strings."""
    normalized = []
    for fl_id in fl_ids or []:
        digits = re.sub(r'\D', '', str(fl_id))
        if digits:
            normalized.append(digits)
    return normalized


def _load_nli_persistent_cache(
    cache_path: str = NLI_PERSISTENT_CACHE_FILE,
    *,
    now: float | None = None,
) -> tuple[dict[str, list[str]], dict[str, float]]:
    """Load persisted positive NLI FL-ID cache entries from disk.

    Persisted entries are rehydrated with a fresh in-memory timestamp so they
    can absorb the immediate post-restart burst even if they were last fetched
    hours or days earlier.
    """
    import time as _time

    now = _time.time() if now is None else now
    if NLI_DISK_CACHE_TTL <= 0:
        return {}, {}

    try:
        with open(cache_path, 'r', encoding='utf-8') as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return {}, {}
    except Exception as e:
        logger.warning(f"Failed to load persisted NLI FL-ID cache from {cache_path}: {e}")
        return {}, {}

    entries = payload.get('entries', {}) if isinstance(payload, dict) else {}
    cache: dict[str, list[str]] = {}
    cache_time: dict[str, float] = {}
    changed = False

    for system_id, entry in entries.items():
        if not isinstance(entry, dict):
            changed = True
            continue
        cached_at = entry.get('cached_at')
        if not isinstance(cached_at, (int, float)):
            changed = True
            continue
        if now - float(cached_at) >= NLI_DISK_CACHE_TTL:
            changed = True
            continue
        fl_ids = _normalize_persisted_fl_ids(entry.get('fl_ids', []))
        if not fl_ids:
            changed = True
            continue
        cache[str(system_id)] = fl_ids
        cache_time[str(system_id)] = now

    if changed:
        _save_nli_persistent_cache(cache, cache_time, cache_path=cache_path, now=now)

    if cache:
        logger.info(f"Loaded {len(cache)} persisted NLI FL-ID cache entries from {cache_path}")
    return cache, cache_time


def _save_nli_persistent_cache(
    cache: dict[str, object],
    cache_time: dict[str, float],
    cache_path: str = NLI_PERSISTENT_CACHE_FILE,
    *,
    now: float | None = None,
) -> bool:
    """Persist positive NLI FL-ID cache entries with an atomic file replace.

    Phase 92.2 D-NLI-01: serializes writes via _nli_persist_lock to prevent
    [WinError 5] access-denied races when multiple background threads call
    os.replace() on the same target simultaneously. Retries os.replace at
    100/250/500ms before logging WARNING once and returning False.

    Returns:
        True on success, False on final failure (all retries exhausted).
    """
    import time as _time

    now = _time.time() if now is None else now
    if NLI_DISK_CACHE_TTL <= 0:
        return True  # disabled; not an error

    entries = {}
    for system_id, fl_ids in cache.items():
        if not isinstance(fl_ids, list):
            continue
        cached_at = cache_time.get(system_id)
        if not isinstance(cached_at, (int, float)):
            continue
        if now - float(cached_at) >= NLI_DISK_CACHE_TTL:
            continue
        normalized_fl_ids = _normalize_persisted_fl_ids(fl_ids)
        if not normalized_fl_ids:
            continue
        entries[str(system_id)] = {
            'fl_ids': normalized_fl_ids,
            'cached_at': float(cached_at),
        }

    payload = {'version': 1, 'entries': entries}
    temp_path = f"{cache_path}.tmp.{os.getpid()}.{threading.get_ident()}"

    # D-NLI-01: acquire lock BEFORE writing the temp file so concurrent callers
    # queue rather than race on the atomic rename (Reviews Codex-LOW-1: barrier
    # belongs outside the lock in tests; the lock itself is the serializer here).
    with _nli_persist_lock:
        try:
            cache_dir = os.path.dirname(cache_path)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
            with open(temp_path, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle, ensure_ascii=True, sort_keys=True)
        except Exception as e:
            logger.warning("Failed to write NLI FL-ID cache temp file %s: %s", temp_path, e)
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            return False

        # Retry os.replace at 100/250/500ms on EACCES / WinError 5
        _replace_delays = (0.1, 0.25, 0.5)
        last_exc: Exception | None = None
        for attempt, delay in enumerate((*_replace_delays, None), start=1):
            try:
                os.replace(temp_path, cache_path)
                return True
            except Exception as exc:
                last_exc = exc
                if delay is None:
                    break
                logger.debug(
                    "os.replace NLI cache attempt %d failed (%s); retrying in %.0fms",
                    attempt, exc, delay * 1000,
                )
                _time.sleep(delay)

        # All retries exhausted — warn ONCE and clean up temp file
        logger.warning(
            "Failed to persist NLI FL-ID cache to %s after %d attempts: %s",
            cache_path, len(_replace_delays) + 1, last_exc,
        )
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        return False

def init_api_routes(app_override=None):
    """Register API routes for image proxy and exports.

    Args:
        app_override: Optional FastAPI/Starlette app to register routes onto.
                      When None (default, production), registers onto the
                      module-level NiceGUI singleton ``app``. When a bare app
                      is passed (test fixtures), registers onto that instead -
                      keeps the NiceGUI singleton clean across multiple
                      TestClient setups. See 77-REVIEWS.md HIGH-08.
    """
    # NOTE: Backend database and routers removed - using Supabase now
    target_app = app_override if app_override is not None else app
    logger.info("API routes initialized (Supabase mode)")

    # ── SEO: robots.txt and sitemap.xml ──────────────────────────────
    @target_app.get('/robots.txt')
    def robots_txt():
        # /search and /parallels are crawlable (not disallowed) so bots can
        # see their <meta name="robots" content="noindex"> tags.  Blocking
        # them here would prevent Google from ever reading the noindex directive.
        archive_user_agents = ''.join(
            f"User-agent: {agent}\n"
            for agent in ARCHIVE_USER_AGENT_TOKENS
        )
        archive_disallows = ''.join(
            f"Disallow: {path}\n"
            for path in ARCHIVE_DISALLOWED_PATHS
        )
        content = (
            "User-agent: *\n"
            "Allow: /\n"
            "\n"
            "Disallow: /admin\n"
            "Disallow: /auth/\n"
            "Disallow: /profile\n"
            "Disallow: /settings\n"
            "Disallow: /corrections\n"
            "Disallow: /lists\n"
            "Disallow: /reset-hints\n"
            "Disallow: /api/\n"
            "\n"
            "# Keep Wayback captures focused on user-facing pages, not app internals.\n"
            f"{archive_user_agents}"
            "Allow: /\n"
            f"{archive_disallows}"
            "\n"
            "Sitemap: https://genizahsearch.com/sitemap.xml\n"
        )
        return Response(content=content, media_type="text/plain")

    # -- Sitemap index (split into static pages + manuscript chunks) --
    # Cache sorted sys_ids to avoid re-sorting on every request
    _sitemap_sys_ids_cache = {'ids': None}

    def _get_sitemap_sys_ids():
        if _sitemap_sys_ids_cache['ids'] is not None:
            return _sitemap_sys_ids_cache['ids']
        try:
            mm = state.meta_mgr
            if mm and hasattr(mm, 'csv_bank') and mm.csv_bank:
                _sitemap_sys_ids_cache['ids'] = sorted(mm.csv_bank.keys())
                return _sitemap_sys_ids_cache['ids']
        except Exception:
            pass  # Cache operation failed; continue without cached data
        # csv_bank not ready yet — return empty without caching so we retry next request
        return []

    @target_app.get('/sitemap.xml')
    def sitemap_index():
        """Sitemap index pointing to sub-sitemaps."""
        sitemaps = [
            'https://genizahsearch.com/sitemap-static.xml',
        ]
        total = len(_get_sitemap_sys_ids())
        if total > 0:
            chunk_size = 40000
            num_chunks = (total + chunk_size - 1) // chunk_size
            for i in range(num_chunks):
                sitemaps.append(f'https://genizahsearch.com/sitemap-manuscripts-{i}.xml')
        entries = '\n'.join(
            f'  <sitemap>\n    <loc>{s}</loc>\n  </sitemap>'
            for s in sitemaps
        )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            f'{entries}\n'
            '</sitemapindex>\n'
        )
        return Response(content=xml, media_type="application/xml")

    @target_app.get('/sitemap-static.xml')
    def sitemap_static():
        """Static/editorial pages sitemap."""
        pages = [
            ('/', '1.0', 'weekly'),
            ('/catalog-browse', '0.8', 'weekly'),
            ('/discoveries', '0.7', 'weekly'),
            ('/browse', '0.6', 'daily'),
            ('/about', '0.6', 'monthly'),
            ('/help', '0.5', 'monthly'),
            ('/download', '0.5', 'monthly'),
            ('/accessibility', '0.3', 'yearly'),
        ]
        urls = []
        for path, priority, freq in pages:
            urls.append(
                f'  <url>\n'
                f'    <loc>https://genizahsearch.com{path}</loc>\n'
                f'    <changefreq>{freq}</changefreq>\n'
                f'    <priority>{priority}</priority>\n'
                f'  </url>'
            )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            + '\n'.join(urls) + '\n'
            '</urlset>\n'
        )
        return Response(content=xml, media_type="application/xml")

    @target_app.get('/sitemap-manuscripts-{chunk}.xml')
    def sitemap_manuscripts(chunk: int):
        """Dynamic manuscript sitemap chunk (up to 40K URLs per file)."""
        chunk_size = 40000
        all_sys_ids = _get_sitemap_sys_ids()
        start = chunk * chunk_size
        end = start + chunk_size
        page_ids = all_sys_ids[start:end]
        if not page_ids:
            return Response(
                content='<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n</urlset>\n',
                media_type="application/xml",
            )
        urls = []
        for sys_id in page_ids:
            urls.append(
                f'  <url>\n'
                f'    <loc>https://genizahsearch.com/browse?sys_id={sys_id}</loc>\n'
                f'    <changefreq>monthly</changefreq>\n'
                f'    <priority>0.5</priority>\n'
                f'  </url>'
            )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            + '\n'.join(urls) + '\n'
            '</urlset>\n'
        )
        return Response(content=xml, media_type="application/xml")

    # Initialize NLI crossref service for local FL ID resolution (Phase 30)
    from shared.nli_crossref_service import get_nli_crossref_service
    nli_svc = get_nli_crossref_service(thread_safe=True)
    if nli_svc.is_available():
        logger.info("NLI crossref sidecar loaded (local FL ID resolution enabled)")
    else:
        logger.info("NLI crossref sidecar not available (will use network manifest fetch)")

    # Sentinel for negative cache entries (distinguishes "cached empty" from "not cached")
    _NLI_FAIL_SENTINEL = object()
    _nli_cache, _nli_cache_time = _load_nli_persistent_cache()
    _nli_cache = OrderedDict(_nli_cache)
    _nli_cache_time = OrderedDict(_nli_cache_time)
    _nli_cache_lock = threading.Lock()

    def _prune_nli_memory_cache_locked(now: float) -> None:
        """Drop expired/oldest FL-ID cache entries from the in-process cache."""
        stale_keys = []
        for key, cached_at in list(_nli_cache_time.items()):
            cached_val = _nli_cache.get(key)
            ttl = NLI_FAIL_CACHE_TTL if cached_val is _NLI_FAIL_SENTINEL else NLI_CACHE_TTL
            if now - cached_at >= ttl:
                stale_keys.append(key)
        for key in stale_keys:
            _nli_cache.pop(key, None)
            _nli_cache_time.pop(key, None)
        while NLI_MEMORY_CACHE_MAX_ENTRIES and len(_nli_cache) > NLI_MEMORY_CACHE_MAX_ENTRIES:
            key, _value = _nli_cache.popitem(last=False)
            _nli_cache_time.pop(key, None)

    def _nli_memory_cache_stats() -> dict:
        import time as _time
        now = _time.time()
        with _nli_cache_lock:
            _prune_nli_memory_cache_locked(now)
            negative_count = sum(1 for value in _nli_cache.values() if value is _NLI_FAIL_SENTINEL)
            positive_count = len(_nli_cache) - negative_count
            return {
                'entries': len(_nli_cache),
                'positive_entries': positive_count,
                'negative_entries': negative_count,
                'max_entries': NLI_MEMORY_CACHE_MAX_ENTRIES,
                'ttl_seconds': NLI_CACHE_TTL,
                'negative_ttl_seconds': NLI_FAIL_CACHE_TTL,
            }

    _register_runtime_cache_stats('nli_fl_ids', _nli_memory_cache_stats)

    def _persist_positive_cache_snapshot() -> None:
        with _nli_cache_lock:
            cache_snapshot = dict(_nli_cache)
            cache_time_snapshot = dict(_nli_cache_time)
        _save_nli_persistent_cache(cache_snapshot, cache_time_snapshot)

    def fetch_fl_ids_from_nli(system_id: str, suffix: int = 1) -> list:
        """Fetch ALL FL IDs from NLI IIIF manifest (contains all pages). Results are cached.

        Args:
            system_id: NLI system ID
            suffix: IIIF manifest suffix (1=primary IE, 2=second IE, etc.)
                    Maps to MARC 907 field order. Default 1 for single-IE manuscripts.

        Resolution order:
        1. In-memory cache — includes negative entries to avoid retrying failures (fastest)
        2. Restart-persistent positive cache file under Config.INDEX_DIR
        3. NLI IIIF manifest network fetch (all pages)
        4. NLI MARC API fallback (typically 1 FL ID)
        """
        import time as _time

        cache_key = f"{system_id}:{suffix}" if suffix != 1 else system_id

        # Check cache first (both positive and negative entries)
        with _nli_cache_lock:
            _prune_nli_memory_cache_locked(_time.time())
            cached_present = cache_key in _nli_cache
            cached_val = _nli_cache.get(cache_key)
            cache_age = _time.time() - _nli_cache_time.get(cache_key, 0)
        if cached_present:
            if cached_val is _NLI_FAIL_SENTINEL:
                # Negative cache: return empty if still within cooldown
                if cache_age < NLI_FAIL_CACHE_TTL:
                    return []
                # Cooldown expired — fall through to retry
            elif cache_age < NLI_CACHE_TTL:
                return cached_val

        # Acquire semaphore to cap concurrent NLI requests
        acquired = _nli_semaphore.acquire(timeout=NLI_SEMAPHORE_TIMEOUT)
        if not acquired:
            logger.warning(f"NLI semaphore timeout for {cache_key} — too many concurrent fetches")
            return []
        try:
            return _fetch_fl_ids_network(system_id, suffix)
        finally:
            _nli_semaphore.release()

    def _fetch_fl_ids_network(system_id: str, suffix: int = 1) -> list:
        """Network fetch for FL IDs (called under semaphore)."""
        import time as _time

        cache_key = f"{system_id}:{suffix}" if suffix != 1 else system_id

        # Re-check cache after acquiring semaphore (another thread may have resolved it
        # or stored a negative entry while we were waiting)
        with _nli_cache_lock:
            _prune_nli_memory_cache_locked(_time.time())
            cached_present = cache_key in _nli_cache
            cached_val = _nli_cache.get(cache_key)
            cache_age = _time.time() - _nli_cache_time.get(cache_key, 0)
        if cached_present:
            if cached_val is _NLI_FAIL_SENTINEL:
                if cache_age < NLI_FAIL_CACHE_TTL:
                    return []
            elif cache_age < NLI_CACHE_TTL:
                return cached_val

        # Use IIIF manifest endpoint - this has ALL page images, unlike MARC which only has 1
        url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"
        try:
            resp = _nli_session.get(url, timeout=15, verify=True)
            if resp.status_code == 200:
                data = resp.json()
                fl_ids = []

                # Extract FL IDs from canvas images in order
                if 'sequences' in data and data['sequences']:
                    for canvas in data['sequences'][0].get('canvases', []):
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            service_id = service.get('@id', '')
                            # Extract FL number (e.g. .../FL7734473/...)
                            fl_match = re.search(r'FL(\d+)', service_id)
                            if fl_match:
                                fl_ids.append(fl_match.group(1))

                if fl_ids:
                    with _nli_cache_lock:
                        _nli_cache[cache_key] = fl_ids
                        _nli_cache_time[cache_key] = _time.time()
                        _prune_nli_memory_cache_locked(_time.time())
                    _persist_positive_cache_snapshot()
                    logger.info(f"Resolved {len(fl_ids)} FL IDs for {cache_key} from network IIIF manifest")
                    return fl_ids
        except Exception as e:
            logger.error(f"Failed to fetch FL IDs from IIIF manifest for {cache_key}: {e}")

        # Fallback to MARC API (only has 1 FL ID typically) — only for suffix 1
        if suffix == 1:
            try:
                marc_url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
                resp = _nli_session.get(marc_url, timeout=10, verify=True)
                if resp.status_code == 200:
                    fl_ids = re.findall(r'FL(\d+)', resp.text)
                    seen = set()
                    unique_fl_ids = []
                    for fl_id in fl_ids:
                        if fl_id not in seen:
                            seen.add(fl_id)
                            unique_fl_ids.append(fl_id)
                    if unique_fl_ids:
                        with _nli_cache_lock:
                            _nli_cache[cache_key] = unique_fl_ids
                            _nli_cache_time[cache_key] = _time.time()
                            _prune_nli_memory_cache_locked(_time.time())
                        _persist_positive_cache_snapshot()
                        logger.info(f"Cached {len(unique_fl_ids)} FL IDs from MARC for {system_id}")
                        return unique_fl_ids
            except Exception as e:
                logger.error(f"MARC fallback also failed for {system_id}: {e}")

        # Both attempts failed — negative-cache to avoid immediate retry
        with _nli_cache_lock:
            _nli_cache[cache_key] = _NLI_FAIL_SENTINEL
            _nli_cache_time[cache_key] = _time.time()
            _prune_nli_memory_cache_locked(_time.time())
        return []

    @target_app.get('/api/fl_ids/{sys_id}')
    def get_fl_ids(sys_id: str):
        """Return manifest FL IDs for a sys_id (cached). Used by NLI viewer deep links.

        Phase 85 D-14: synthetic sys_ids skip NLI resolution. Returns 200 + JSON
        empty list dict (NOT 204) — JSON-expecting clients call .json() and would
        break on 204 No Content per REVIEWS-MODE Codex MEDIUM closure.
        """
        if is_synthetic_sys_id(sys_id):
            return {"fl_ids": []}
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        return fl_ids

    @target_app.get('/api/nli_image/{fl_id}')
    def nli_image(fl_id: str):
        """
        Fetch NLI image by FL ID. Tries IIIF first (for valid IDs), then Rosetta.
        """
        # Clean the FL ID
        digits = re.sub(r"\D", "", str(fl_id))
        if not digits:
            return Response(content="Invalid FL ID", status_code=400)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        # Try IIIF first (works for valid FL IDs, returns real images)
        iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/2000,/0/default.jpg"
        try:
            resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                # Verify it's not a tiny placeholder (real images are > 5KB)
                if len(resp.content) > 5000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/jpeg')
                    )
        except Exception as e:
            logger.error(f"IIIF failed for FL{digits}: {e}")

        # Fallback to Rosetta - but filter out the "no image" placeholder
        rosetta_url = f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{digits}"
        try:
            resp = requests.get(rosetta_url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                # The "no image" placeholder is ~1615 bytes, real images are larger
                if len(resp.content) > 2000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/png')
                    )
        except Exception as e:
            logger.error(f"Rosetta failed for FL{digits}: {e}")

        return Response(content="Image not found", status_code=404)

    # Image cache: (sys_id, page, width, suffix) -> (content, content_type, fl_id)
    _image_cache = _make_image_cache('image_nli')

    def _fetch_nli_image_bytes(sys_id: str, page: int, width: int = 2000, suffix: int = 1):
        """Internal helper: fetch NLI image bytes for (sys_id, page, width, suffix).

        Returns (content_bytes, content_type, fl_id) on success, or None when
        nothing was retrievable. The fl_id is the FL digit string that
        actually succeeded (for logging / observability).

        FL ids are sourced from fetch_fl_ids_from_nli, which reads the NLI
        IIIF manifest's canvas_map. This is the AUTHORITATIVE source. NEVER
        construct NLI IIIF URLs from nli_crossref FGPImageNumberId — that
        column holds Friedberg photo numbers, not NLI FL ids (see
        .planning/research/PITFALLS.md Pitfall 6).
        """
        width = max(100, min(width, 2000))
        cache_key = (sys_id, page, width, suffix)

        entry = _image_cache.get(cache_key)
        if entry is not None:
            content, content_type, fl_id = entry
            return (content, content_type, fl_id)

        fl_ids = fetch_fl_ids_from_nli(sys_id, suffix=suffix)
        if not fl_ids:
            return None

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        def _try_fl(fl_id):
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
                min_size = 1000 if width < 500 else 5000  # Thumbnails are smaller
                ct = resp.headers.get('Content-Type', '')
                if resp.status_code == 200 and 'image' in ct and len(resp.content) > min_size:
                    return (resp.content, ct or 'image/jpeg')
            except Exception:
                return None
            return None

        if 0 <= page < len(fl_ids):
            got = _try_fl(fl_ids[page])
            if got is not None:
                _image_cache.set(cache_key, (got[0], got[1], fl_ids[page]))
                return (got[0], got[1], fl_ids[page])

        # Fallback: try each FL id until one works.
        for fl_id in fl_ids:
            got = _try_fl(fl_id)
            if got is not None:
                _image_cache.set(cache_key, (got[0], got[1], fl_id))
                return (got[0], got[1], fl_id)

        return None

    @target_app.get('/api/nli_image_by_sysid/{sys_id}')
    def nli_image_by_sysid(sys_id: str, page: int = 0, width: int = 2000, suffix: int = 1):
        """
        Fetch NLI image by System ID. Dynamically gets FL IDs from NLI IIIF manifest.

        Args:
            suffix: IIIF manifest suffix (1=primary IE, 2=second IE, etc.).
                    For multi-IE manuscripts, each IE has its own manifest with
                    different images. Default 1 for single-IE manuscripts.

        Phase 85 D-14: synthetic sys_ids return 204 No Content. Image endpoint;
        <img> consumers handle 204 as "image not available" without breaking.
        """
        if is_synthetic_sys_id(sys_id):
            return Response(content="", status_code=204)
        got = _fetch_nli_image_bytes(sys_id, page, width=width, suffix=suffix)
        if got is None:
            return Response(content="Image not found", status_code=404)
        content, content_type, _fl_id = got
        return Response(
            content=content,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=600"},
        )

    # Oxford image cache: (sys_id, page, width) -> (content, content_type)
    _oxford_image_cache = _make_image_cache('image_oxford')

    # Cambridge image cache (260419-cfx shape):
    #   key   = (_CAMBRIDGE_CACHE_VERSION, sys_id, page)
    #   value = (content, content_type, extra_headers_dict)
    # Bumping _CAMBRIDGE_CACHE_VERSION invalidates all server-side entries.
    # _CAMBRIDGE_ETAG_VERSION is the matching ETag suffix exposed to clients;
    # bump BOTH in lockstep when the resolver contract changes so browsers
    # and CDNs revalidate via ETag too.
    _CAMBRIDGE_CACHE_VERSION = 2
    _CAMBRIDGE_ETAG_VERSION = "v2"
    _cambridge_image_cache = _make_image_cache('image_cambridge')
    # sys_ids for which we have already logged a "degraded: legacy positional"
    # WARNING (once per process lifetime, per sys_id — not per request).
    _cambridge_degraded_warned = set()

    @target_app.get('/api/cambridge_image/{sys_id}')
    def cambridge_image(sys_id: str, page: int = 0, width: int = 2000):
        """
        Fetch Cambridge IIIF image by System ID with folio+side matching.

        Resolution:
          1. Consult resolve_cambridge_canvas_for_page(sys_id, page, images_ext)
             — maps page N → CUDL canvas whose (folio_num, side) matches the
             N-th NLI nli_images row for this sys_id.
          2. On match: serve the CUDL IIIF tile.
          3. No match (resolver returns None): serve the NLI image for this
             page via _fetch_nli_image_bytes (X-Image-Fallback-Source: nli).
          4. Degraded (resolver returns {'degraded': True}): fall back to
             legacy positional images_ext[page] behavior and log WARN once
             per sys_id.

        All responses carry X-Image-Resolver-Version and ETag headers so
        downstream caches (browser + CDN) can revalidate after a deploy.
        """
        # Imported locally to avoid adding a hot-path import at module load.
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page

        width = max(100, min(int(width or 2000), 2000))
        cache_key = (_CAMBRIDGE_CACHE_VERSION, sys_id, page, width)

        def _base_headers():
            """Resolver-version metadata on every response (success, fallback,
            legacy). ETag lets clients revalidate after a deploy."""
            return {
                "Cache-Control": "public, max-age=600",
                "ETag": f'"{sys_id}-p{page}-w{width}-{_CAMBRIDGE_ETAG_VERSION}"',
                "X-Image-Resolver-Version": str(_CAMBRIDGE_CACHE_VERSION),
            }

        cached_entry = _cambridge_image_cache.get(cache_key)
        if cached_entry is not None:
            content, content_type, headers_extra = cached_entry
            resp_headers = _base_headers()
            if headers_extra:
                resp_headers.update(headers_extra)
            return Response(content=content, media_type=content_type, headers=resp_headers)

        # Look up Cambridge images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No Cambridge images available", status_code=404)

        # Resolve page → canvas or NLI fallback. Sentinel check via
        # `.get('degraded')` — never identity-compare the _DEGRADED dict.
        try:
            resolved = resolve_cambridge_canvas_for_page(sys_id, page, images_ext, svc=nli_svc)
        except Exception as e:  # defensive: never 500 on resolver error
            logger.warning(
                "cambridge_image: resolver raised for sys_id=%s page=%s: %s — "
                "falling back to legacy positional", sys_id, page, e,
            )
            resolved = {'degraded': True}

        canvas_entry = None
        fallback_to_nli = False
        matched_folio_side = None  # e.g. '8r' — when resolver produced (folio, side)

        if resolved is None:
            # Resolver identified a target (folio, side) but no CUDL canvas
            # matches → serve NLI fallback. Attempt to record the folio
            # label for the X-Folio-Matched response header.
            fallback_to_nli = True
            try:
                folio_rows = nli_svc.get_folio_images(sys_id) if nli_svc else []
                if 0 <= page < len(folio_rows):
                    matched_folio_side = folio_rows[page].get('folio_label') or None
            except Exception:
                matched_folio_side = None
        elif resolved.get('degraded'):
            # Sidecar unavailable OR sys_id has no nli_images rows → legacy
            # positional behavior. Warn once per sys_id per process.
            if sys_id not in _cambridge_degraded_warned:
                logger.warning(
                    "cambridge_image: nli_crossref unavailable or empty for "
                    "sys_id=%s — using legacy positional canvas lookup", sys_id,
                )
                _cambridge_degraded_warned.add(sys_id)
            if 0 <= page < len(images_ext):
                canvas_entry = images_ext[page]
            else:
                return Response(content="Page out of range", status_code=404)
        else:
            # Exact (folio, side) match.
            idx = resolved.get('canvas_index')
            matched_folio_side = f"{resolved['folio_num']}{resolved['side']}"
            if idx is not None and 0 <= idx < len(images_ext):
                canvas_entry = images_ext[idx]
            else:
                # Resolver produced an index but it was out of range — treat
                # as NLI fallback. Shouldn't happen with well-formed data.
                fallback_to_nli = True

        if fallback_to_nli:
            # KNOWN LIMITATION (documented in SUMMARY.md): suffix=1 is
            # hardcoded here because the /api/cambridge_image endpoint
            # contract has no `suffix` query param (adding one is out of
            # scope per CONTEXT.md). Multi-IE CUL shelfmarks (rare) may
            # therefore receive the wrong volume's NLI image on the web
            # fallback path. Desktop does NOT have this limitation.
            got = _fetch_nli_image_bytes(sys_id, page, width=width, suffix=1)
            if got is None:
                return Response(content="Image not found", status_code=404)
            content, ct, resolved_fl_id = got
            logger.info(
                "cambridge_image NLI fallback: sys_id=%s page=%s folio=%s fl_id=%s",
                sys_id, page, matched_folio_side or "?", resolved_fl_id or "?",
            )
            extra_headers = {"X-Image-Fallback-Source": "nli"}
            if matched_folio_side:
                extra_headers["X-Folio-Matched"] = matched_folio_side
            _cambridge_image_cache.set(cache_key, (content, ct, extra_headers))
            resp_headers = _base_headers()
            resp_headers.update(extra_headers)
            return Response(content=content, media_type=ct, headers=resp_headers)

        # Normal CUDL fetch path.
        canvas_url = (canvas_entry or {}).get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        img_url = f"{canvas_url}/full/{width},/0/default.jpg"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://cudl.lib.cam.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                extra_headers = {}
                if matched_folio_side:
                    extra_headers["X-Folio-Matched"] = matched_folio_side
                _cambridge_image_cache.set(cache_key, (resp.content, content_type, extra_headers))
                resp_headers = _base_headers()
                resp_headers.update(extra_headers)
                return Response(
                    content=resp.content,
                    media_type=content_type,
                    headers=resp_headers,
                )
            else:
                return Response(
                    content=f"Failed to fetch Cambridge image: {resp.status_code}",
                    status_code=resp.status_code,
                )
        except Exception as e:
            return Response(content=f"Error fetching Cambridge image: {e}", status_code=500)

    # Manchester image cache: (sys_id, page, width) -> (content, content_type)
    _manchester_image_cache = _make_image_cache('image_manchester')

    @target_app.get('/api/manchester_image/{sys_id}')
    def manchester_image(sys_id: str, page: int = 0, width: int = 2000):
        """
        Fetch Manchester IIIF image by System ID.
        Uses images_ext from nli_cache populated by enrich_metadata via LUNA IIIF manifest.
        """
        width = max(100, min(int(width or 2000), 2000))
        cache_key = (sys_id, page, width)

        # Check cache
        cached_entry = _manchester_image_cache.get(cache_key)
        if cached_entry is not None:
            content, content_type = cached_entry
            return Response(content=content, media_type=content_type,
                          headers={"Cache-Control": "public, max-age=600"})

        # Look up Manchester images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No Manchester images available", status_code=404)

        if page < 0 or page >= len(images_ext):
            return Response(content="Page out of range", status_code=404)

        canvas_entry = images_ext[page]
        canvas_url = canvas_entry.get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        # Build IIIF Image API URL from canvas base URL
        img_url = f"{canvas_url}/full/{width},/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://luna.manchester.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                _manchester_image_cache.set(cache_key, (resp.content, content_type))
                return Response(content=resp.content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})
            else:
                return Response(content=f"Failed to fetch Manchester image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching Manchester image: {e}", status_code=500)

    # JTS image cache: (sys_id, page, width) -> (content, content_type)
    _jts_image_cache = _make_image_cache('image_jts')

    @target_app.get('/api/jts_image/{sys_id}')
    def jts_image(sys_id: str, page: int = 0, width: int = 2000):
        """
        Fetch JTS/Princeton IIIF image by System ID.
        Uses images_ext from nli_cache populated by enrich_metadata via Figgy IIIF manifest.
        """
        width = max(100, min(int(width or 2000), 2000))
        cache_key = (sys_id, page, width)

        # Check cache
        cached_entry = _jts_image_cache.get(cache_key)
        if cached_entry is not None:
            content, content_type = cached_entry
            return Response(content=content, media_type=content_type,
                          headers={"Cache-Control": "public, max-age=600"})

        # Look up JTS images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No JTS images available", status_code=404)

        if page < 0 or page >= len(images_ext):
            return Response(content="Page out of range", status_code=404)

        canvas_entry = images_ext[page]
        canvas_url = canvas_entry.get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        # Build IIIF Image API URL from canvas base URL
        img_url = f"{canvas_url}/full/{width},/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://dpul.princeton.edu/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                _jts_image_cache.set(cache_key, (resp.content, content_type))
                return Response(content=resp.content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})
            else:
                return Response(content=f"Failed to fetch JTS image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching JTS image: {e}", status_code=500)

    def _extract_folio_number(shelfmark: str) -> int:
        """Extract folio number from Oxford shelfmark like 'MS heb. f.21/21' -> 21"""
        # Pattern: MS heb. X.YY/ZZ where ZZ is the folio number
        match = re.search(r'/(\d+)', shelfmark)
        if match:
            return int(match.group(1))
        return 0

    @target_app.get('/api/oxford_image/{sys_id}')
    def oxford_image(sys_id: str, page: int = 0, width: int = 2000):
        """
        Fetch Oxford image by System ID using CodicologicalManager.
        Automatically finds the correct folio image based on shelfmark.
        """
        width = max(100, min(int(width or 2000), 2000))
        cache_key = (sys_id, page, width)

        # Check image cache first
        cached_entry = _oxford_image_cache.get(cache_key)
        if cached_entry is not None:
            content, content_type = cached_entry
            return Response(
                content=content,
                media_type=content_type,
                headers={"Cache-Control": "public, max-age=600"}
            )

        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return Response(content="Oxford manager not initialized", status_code=503)

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return Response(content="Oxford database still loading", status_code=503)

        # Get the Part ID for this system ID
        part_id = codico.get_part_for_folio(sys_id)

        # Get shelfmark for folio number extraction
        shelfmark = ''
        shelfmark_tuple = state.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark_tuple and shelfmark_tuple[0]:
            shelfmark = shelfmark_tuple[0]

        if not part_id and shelfmark:
            # Try to find part by shelfmark
            part_id, is_part = codico.parse_part_identifier(shelfmark)
            if not is_part:
                part_id = None

        if not part_id:
            return Response(content="No Oxford Part found for this document", status_code=404)

        # Get images for this part
        images = codico.get_part_images(part_id)
        if not images:
            return Response(content="No images available for this Part", status_code=404)

        # Extract folio number from shelfmark and find the matching image
        folio_num = _extract_folio_number(shelfmark)
        img_data = None
        img_url = ''

        if folio_num > 0:
            # Find all images matching this folio number (both 'a' and 'b' sides)
            folio_images = [img for img in images if img.get('folio_num') == folio_num]

            if folio_images:
                # Use page param to select: 0 = first (recto/a), 1 = second (verso/b)
                idx = min(page, len(folio_images) - 1)
                img_data = folio_images[idx]
            else:
                # If image not in database but folio is in range, generate URL dynamically
                metadata = codico.part_metadata.get(part_id, {})
                folio_range = metadata.get('folio_range', [])
                if len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
                    # Generate URL based on part_id pattern
                    # MS. Heb. f. 21/1 -> MS_HEB_f_21_18a.jpg or MS_HEB_f_21_18b.jpg
                    match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)', part_id, re.IGNORECASE)
                    if match:
                        letter, volume = match.groups()
                        # page 0 = recto (a), page 1 = verso (b)
                        side = 'b' if page == 1 else 'a'
                        img_url = f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"

        # Fallback to page index if folio not found and no dynamic URL
        if not img_data and not img_url:
            if page < 0 or page >= len(images):
                page = 0
            img_data = images[page]

        if not img_url and img_data:
            thumb_url = img_data.get('thumb_url', '')
            if thumb_url and width <= 600:
                img_url = thumb_url
            else:
                img_url = img_data.get('full_url', '')

        if not img_url:
            return Response(content="No image URL available", status_code=404)

        # Fetch the image from Oxford
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://hebrew.bodleian.ox.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                # Cache the image
                _oxford_image_cache.set(cache_key, (resp.content, content_type))
                return Response(
                    content=resp.content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )
            else:
                # Production fallback: if server-side fetch fails, let browser fetch directly.
                logger.info(f"Oxford proxy fetch failed ({resp.status_code}) for {sys_id} page={page}; redirecting to direct URL")
                return RedirectResponse(url=img_url, status_code=307)
        except Exception as e:
            # Production fallback: network/SSL differences can break server fetches while
            # direct browser access still works.
            logger.error(f"Oxford proxy exception for {sys_id} page={page}: {e}; redirecting to direct URL")
            return RedirectResponse(url=img_url, status_code=307)

    @target_app.get('/api/oxford_image_url/{sys_id}')
    def oxford_image_url(sys_id: str, page: int = 0):
        """
        Get the direct Oxford URL for an image (no proxy).
        Returns JSON with the URL that the browser can fetch directly.
        """
        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return {"error": "Oxford manager not initialized", "url": None}

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return {"error": "Oxford database still loading", "url": None}

        # Get shelfmark for folio number extraction
        shelfmark = ''
        shelfmark_tuple = state.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark_tuple and shelfmark_tuple[0]:
            shelfmark = shelfmark_tuple[0]

        part_id = codico.get_part_for_folio(sys_id)
        if not part_id and shelfmark:
            part_id, is_part = codico.parse_part_identifier(shelfmark)
            if not is_part:
                part_id = None

        if not part_id:
            return {"error": "No Oxford Part found", "url": None}

        images = codico.get_part_images(part_id)
        folio_num = _extract_folio_number(shelfmark)
        img_url = None

        if folio_num > 0:
            # Find images for this folio
            folio_images = [img for img in images if img.get('folio_num') == folio_num]
            if folio_images:
                idx = min(page, len(folio_images) - 1)
                img_url = folio_images[idx].get('full_url', '')
            else:
                # Generate dynamically if not in database
                metadata = codico.part_metadata.get(part_id, {})
                folio_range = metadata.get('folio_range', [])
                if len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
                    match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)', part_id, re.IGNORECASE)
                    if match:
                        letter, volume = match.groups()
                        side = 'b' if page == 1 else 'a'
                        img_url = f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"

        if not img_url and images:
            idx = min(page, len(images) - 1)
            img_url = images[idx].get('full_url', '')

        return {"url": img_url, "folio": folio_num, "page": page, "part_id": part_id}

    @target_app.get('/api/oxford_images/{sys_id}')
    def oxford_images_list(sys_id: str):
        """
        Get list of available Oxford images for a system ID.
        Returns JSON with image metadata.
        """
        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return {"error": "Oxford manager not initialized", "images": []}

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return {"error": "Oxford database still loading", "images": []}

        # Get the Part ID for this system ID
        part_id = codico.get_part_for_folio(sys_id)
        if not part_id:
            return {"error": "No Oxford Part found", "images": [], "part_id": None}

        # Get images for this part
        images = codico.get_part_images(part_id)

        return {
            "part_id": part_id,
            "images": [
                {
                    "index": i,
                    "label": img.get('label', ''),
                    "folio_num": img.get('folio_num'),
                    "url": f"/api/oxford_image/{sys_id}?page={i}"
                }
                for i, img in enumerate(images)
            ]
        }

    @target_app.get('/api/oxford_debug')
    def oxford_debug():
        """
        Debug endpoint to check Oxford mapping status.
        """
        if not state.meta_mgr:
            return {"error": "MetadataManager not initialized"}

        codico = state.meta_mgr.codico_mgr
        if not codico:
            return {"error": "CodicologicalManager not initialized"}

        csv_bank_count = len(state.meta_mgr.csv_bank)
        oxford_part_count = sum(1 for v in state.meta_mgr.csv_bank.values() if v.get('oxford_part_id'))

        return {
            "codico_loaded": getattr(codico, '_loaded', False),
            "csv_bank_entries": csv_bank_count,
            "entries_with_oxford_part_id": oxford_part_count,
            "folio_to_part_mappings": len(codico.folio_to_part),
            "part_metadata_count": len(codico.part_metadata),
            "sample_mappings": list(codico.folio_to_part.items())[:5] if codico.folio_to_part else [],
        }

    @target_app.get('/api/browse_debug/{sys_id}')
    def browse_debug(sys_id: str):
        """
        Debug endpoint to check browse data for a specific sys_id.
        """
        if not state.meta_mgr:
            return {"error": "MetadataManager not initialized"}
        if not state.searcher:
            return {"error": "SearchEngine not initialized"}

        # Get shelfmark from csv_bank
        csv_entry = state.meta_mgr.csv_bank.get(sys_id, {})
        shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)

        # Get browse_map data
        browse_map = state.searcher._load_browse_map() if hasattr(state.searcher, '_load_browse_map') else {}
        browse_entries = browse_map.get(sys_id, [])

        # Extract FL IDs from browse entries
        import re
        fl_ids = []
        for entry in browse_entries:
            full_header = entry.get('full_header', '')
            match = re.search(r'FL(\d+)', full_header)
            if match:
                fl_ids.append(match.group(1))

        # Check Oxford mapping
        part_id = None
        if state.meta_mgr.codico_mgr:
            part_id = state.meta_mgr.codico_mgr.get_part_for_folio(sys_id)

        return {
            "sys_id": sys_id,
            "csv_entry": csv_entry,
            "shelfmark": shelfmark,
            "title": title,
            "browse_entries_count": len(browse_entries),
            "fl_ids_found": fl_ids[:5],  # First 5
            "oxford_part_id": part_id,
            "is_oxford_shelfmark": shelfmark.lower().startswith('ms heb') or shelfmark.lower().startswith('ms. heb') if shelfmark else False,
        }

    # === Puzzle Canvas API (Phase 49) ===

    @target_app.get('/api/puzzle_image')
    def puzzle_image(fl_id: str, threshold: float = 30.0, size: int = 800,
                     processed: bool = True, is_cul: bool = False):
        """Serve processed/original fragment image for puzzle canvas.
        Tries server-side IIIF fetch (works on desktop/local dev).
        Returns 404 if NLI blocks the server IP — the browser JS then
        falls back to the localhost helper service for bg removal.
        """
        from shared.puzzle_image_service import get_puzzle_image_service
        service = get_puzzle_image_service()
        image_bytes = service.resolve_fragment_image(
            fl_id=fl_id, size=size, threshold=threshold, processed=processed,
            is_cul=is_cul
        )
        if image_bytes is None:
            # Generate upload token so the browser extension can fetch + upload
            from web.puzzle_tokens import generate_upload_token
            token = generate_upload_token(fl_id, threshold, is_cul)
            return Response(
                content="Image not found", status_code=404,
                headers={
                    "X-Puzzle-Upload-Token": token,
                    "Access-Control-Expose-Headers": "X-Puzzle-Upload-Token"
                }
            )
        content_type = 'image/png' if image_bytes[:4] == b'\x89PNG' else 'image/jpeg'
        return Response(
            content=image_bytes,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=3600"}
        )

    # In-memory rate limiter for puzzle upload endpoints
    _puzzle_rate_limits = {}  # IP -> (count, window_start_epoch)

    def _puzzle_rate_limit_stats() -> dict:
        now = _module_time.time()
        stale = [
            ip for ip, (_count, window_start) in _puzzle_rate_limits.items()
            if now - window_start > PUZZLE_RATE_LIMIT_BUCKET_TTL
        ]
        for ip in stale:
            _puzzle_rate_limits.pop(ip, None)
        return {
            'entries': len(_puzzle_rate_limits),
            'bucket_ttl_seconds': PUZZLE_RATE_LIMIT_BUCKET_TTL,
        }

    _register_runtime_cache_stats('puzzle_rate_limits', _puzzle_rate_limit_stats)

    def _check_puzzle_rate_limit(request: Request, max_per_min: int = 60):
        """Check per-IP rate limit. Returns Response if exceeded, else None."""
        import time as _time
        client_ip = request.client.host if request.client else 'unknown'
        now = _time.time()
        stale = [
            ip for ip, (_count, window_start) in _puzzle_rate_limits.items()
            if now - window_start > PUZZLE_RATE_LIMIT_BUCKET_TTL
        ]
        for ip in stale:
            _puzzle_rate_limits.pop(ip, None)
        entry = _puzzle_rate_limits.get(client_ip)
        if entry:
            count, window_start = entry
            if now - window_start < 60:
                if count >= max_per_min:
                    return Response(content="Rate limit exceeded", status_code=429)
                _puzzle_rate_limits[client_ip] = (count + 1, window_start)
            else:
                _puzzle_rate_limits[client_ip] = (1, now)
        else:
            _puzzle_rate_limits[client_ip] = (1, now)
        return None

    @target_app.post('/api/puzzle_process')
    async def puzzle_process(request: Request):
        """Process client-fetched image bytes with background removal.
        Fallback endpoint: client fetches IIIF image in the browser, POSTs
        raw bytes here for server-side bg removal + caching.
        Requires a valid upload token from a prior GET /api/puzzle_image 404.
        """
        import re as _re
        from shared.puzzle_image_service import get_puzzle_image_service
        from shared.background_removal import remove_background
        from web.puzzle_tokens import verify_upload_token
        from PIL import Image as _PILImage
        import io as _io

        MAX_BODY_SIZE = 10 * 1024 * 1024  # 10 MB

        # Rate limiting
        rate_resp = _check_puzzle_rate_limit(request)
        if rate_resp:
            return rate_resp

        fl_id = request.query_params.get('fl_id', '')
        # Validate fl_id: must be digits only (NLI FL IDs are numeric)
        if not fl_id or not _re.match(r'^[\d]+$', _re.sub(r'\D', '', fl_id)):
            return Response(content="Invalid fl_id", status_code=400)

        # Verify upload token
        upload_token = request.headers.get('X-Puzzle-Upload-Token', '')
        if not verify_upload_token(upload_token, fl_id):
            return Response(content="Invalid or expired upload token", status_code=403)

        threshold = float(request.query_params.get('threshold', 30))
        is_cul = request.query_params.get('is_cul', 'false').lower() == 'true'
        processed = request.query_params.get('processed', 'true').lower() == 'true'
        size = int(request.query_params.get('size', 800))

        # Clamp parameters to valid ranges
        threshold = max(0, min(255, threshold))
        # Validate size against known presets (400, 800, 1200, 2000)
        valid_sizes = {400, 800, 1200, 2000}
        if size not in valid_sizes:
            size = min(valid_sizes, key=lambda s: abs(s - size))
        size = max(100, min(2000, size))

        # Check cache first
        service = get_puzzle_image_service()
        cache_path = service.get_cache_path(fl_id, size, threshold, processed, is_cul)
        if cache_path.exists():
            try:
                cached = cache_path.read_bytes()
                content_type = 'image/png' if cached[:4] == b'\x89PNG' else 'image/jpeg'
                return Response(content=cached, media_type=content_type,
                                headers={"Cache-Control": "public, max-age=3600"})
            except Exception:
                pass  # Cache operation failed; continue without cached data

        # Read and validate client-uploaded image bytes
        content_length = int(request.headers.get('content-length', 0))
        if content_length > MAX_BODY_SIZE:
            return Response(content="Image too large", status_code=413)

        raw_bytes = await request.body()
        if not raw_bytes or len(raw_bytes) > MAX_BODY_SIZE:
            return Response(content="No image data or too large", status_code=400)

        # Validate it's actually an image (JPEG or PNG magic bytes)
        is_jpeg = raw_bytes[:2] == b'\xff\xd8'
        is_png = raw_bytes[:4] == b'\x89PNG'
        if not is_jpeg and not is_png:
            return Response(content="Invalid image format", status_code=400)

        # Verify Pillow can open it (prevents crafted payloads)
        try:
            img = _PILImage.open(_io.BytesIO(raw_bytes))
            img.verify()
        except Exception:
            return Response(content="Corrupt image data", status_code=400)  # Request processing failed; return error response

        if not processed:
            # Cache original and return (validated as real image)
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_bytes(raw_bytes)
            except OSError:
                pass
            return Response(content=raw_bytes, media_type='image/jpeg',
                            headers={"Cache-Control": "public, max-age=3600"})

        # Apply background removal
        try:
            result_bytes = remove_background(raw_bytes, threshold=threshold, is_cul=is_cul)
        except Exception:
            result_bytes = raw_bytes  # Image processing failed; serve original bytes

        # Cache result via versioned cache path
        service.save_derivative_to_cache(fl_id, size, threshold, is_cul, result_bytes)

        content_type = 'image/png' if result_bytes[:4] == b'\x89PNG' else 'image/jpeg'
        return Response(content=result_bytes, media_type=content_type,
                        headers={"Cache-Control": "public, max-age=3600"})

    @target_app.post('/api/puzzle_upload_derivative')
    async def puzzle_upload_derivative(request: Request):
        """Accept pre-processed PNG bytes from desktop app or extension.
        Saves directly to server cache without re-processing.
        Requires valid upload token for cache poisoning prevention.
        """
        import re as _re
        from shared.puzzle_image_service import get_puzzle_image_service
        from web.puzzle_tokens import verify_upload_token
        from PIL import Image as _PILImage
        import io as _io

        MAX_BODY_SIZE = 10 * 1024 * 1024  # 10 MB

        # Rate limiting
        rate_resp = _check_puzzle_rate_limit(request)
        if rate_resp:
            return rate_resp

        fl_id = request.query_params.get('fl_id', '')
        if not fl_id or not _re.match(r'^[\d]+$', _re.sub(r'\D', '', fl_id)):
            return Response(content="Invalid fl_id", status_code=400)

        # Verify upload token
        upload_token = request.headers.get('X-Puzzle-Upload-Token', '')
        if not verify_upload_token(upload_token, fl_id):
            return Response(content="Invalid or expired upload token", status_code=403)

        threshold = float(request.query_params.get('threshold', 30.0))
        is_cul = request.query_params.get('is_cul', 'false').lower() == 'true'
        size = int(request.query_params.get('size', 800))

        # Read body
        content_length = int(request.headers.get('content-length', 0))
        if content_length > MAX_BODY_SIZE:
            return Response(content="Image too large", status_code=413)

        png_bytes = await request.body()
        if not png_bytes or len(png_bytes) > MAX_BODY_SIZE:
            return Response(content="No image data or too large", status_code=400)

        # Validate PNG header
        if png_bytes[:4] != b'\x89PNG':
            return Response(content="Invalid PNG format", status_code=400)

        # Verify Pillow can open it
        try:
            img = _PILImage.open(_io.BytesIO(png_bytes))
            img.verify()
        except Exception:
            return Response(content="Corrupt image data", status_code=400)  # Request processing failed; return error response

        # Save to cache
        service = get_puzzle_image_service()
        success = service.save_derivative_to_cache(fl_id, size, threshold, is_cul, png_bytes)
        if success:
            from starlette.responses import JSONResponse
            return JSONResponse({"cached": True})
        else:
            return Response(content="Cache write failed", status_code=500)

    def _fetch_provider_image(provider: str, sys_id: str, page: int):
        """Fetch raw image bytes from a library-specific IIIF source.

        Delegates to the existing per-provider proxy endpoint functions.
        Returns the Response object from the proxy, or a 404 Response if provider unknown.
        """
        if provider == 'cambridge':
            return cambridge_image(sys_id=sys_id, page=page)
        elif provider == 'manchester':
            return manchester_image(sys_id=sys_id, page=page)
        elif provider == 'jts':
            return jts_image(sys_id=sys_id, page=page)
        elif provider == 'oxford':
            return oxford_image(sys_id=sys_id, page=page)
        else:
            return Response(content=f"Unknown provider: {provider}", status_code=400)

    @target_app.get('/api/puzzle_ext_image')
    def puzzle_ext_image(sys_id: str, page: int = 0, provider: str = '',
                         threshold: float = 30.0, size: int = 800,
                         processed: bool = True):
        """Serve processed external library image for puzzle canvas.

        Fetches raw image from the library-specific proxy (cambridge_image,
        manchester_image, jts_image, oxford_image), applies BG removal server-side,
        caches the processed result, and returns processed PNG.

        No HMAC token needed (same-origin only). BG removal uses the same HSV
        pipeline as NLI images (default threshold=30.0 for external libraries).
        """
        from shared.puzzle_image_service import get_puzzle_image_service
        from shared.background_removal import remove_background

        if not provider or not sys_id:
            return Response(content="Missing provider or sys_id", status_code=400)

        valid_providers = ('cambridge', 'manchester', 'jts', 'oxford')
        if provider not in valid_providers:
            return Response(content=f"Unknown provider: {provider}", status_code=400)

        # Cache key: includes provider to avoid collisions between libraries sharing sys_ids
        cache_id = f"{provider}_{sys_id}_page{page}"
        service = get_puzzle_image_service()
        cache_path = service.get_cache_path(cache_id, size, threshold, processed, is_cul=False)

        # Return cached if exists
        if cache_path.exists():
            try:
                cached = cache_path.read_bytes()
                content_type = 'image/png' if cached[:4] == b'\x89PNG' else 'image/jpeg'
                return Response(content=cached, media_type=content_type,
                                headers={"Cache-Control": "public, max-age=3600"})
            except (FileNotFoundError, OSError):
                pass

        # Delegate to existing provider-specific proxy to fetch raw image bytes
        proxy_resp = _fetch_provider_image(provider, sys_id, page)
        if proxy_resp.status_code != 200:
            return proxy_resp  # Pass through error

        raw_bytes = proxy_resp.body
        if not raw_bytes or len(raw_bytes) < 100:
            return Response(content="Empty image from proxy", status_code=502)

        # Apply BG removal if requested
        if processed:
            try:
                result_bytes = remove_background(raw_bytes, threshold=threshold)
            except Exception:
                result_bytes = raw_bytes  # fallback to original
        else:
            result_bytes = raw_bytes

        # Cache result
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(result_bytes)
        except OSError:
            pass

        content_type = 'image/png' if result_bytes[:4] == b'\x89PNG' else 'image/jpeg'
        return Response(content=result_bytes, media_type=content_type,
                        headers={"Cache-Control": "public, max-age=3600"})

    @target_app.get('/api/puzzle_folios/{sys_id}')
    def puzzle_folios(sys_id: str):
        """Get ordered folio list for a manuscript (for prev/next navigation).

        For NLI-hosted manuscripts: uses IIIF manifest FL IDs.
        For external libraries (Manchester, Oxford, JTS, Cambridge): uses enrich_metadata images_ext.

        Manchester/Oxford/JTS NLI FL IDs are catalog stubs (return 503) — prefer images_ext.
        Cambridge external_provider = CUL at CUDL; NLI FL IDs are real images — use NLI.
        """
        # Check enrich_metadata for external provider
        ext_data = None
        if state.meta_mgr:
            try:
                ext_data = state.meta_mgr.enrich_metadata(sys_id)
                images_ext = (ext_data or {}).get('images_ext', [])
                external_provider = (ext_data or {}).get('external_provider', '')
                # Oxford: enrich_metadata populates images_ext but may leave external_provider empty
                if not external_provider and images_ext:
                    lib_code = state.meta_mgr.get_library_for_id(sys_id) or ''
                    if lib_code == 'Oxford':
                        external_provider = 'oxford'
                if images_ext and external_provider and external_provider != 'cambridge':
                    result = []
                    for i, img in enumerate(images_ext):
                        label = img.get('label', '') or str(i + 1)
                        result.append({
                            'fl_id': '',
                            'label': label,
                            'image_url': img.get('url', ''),
                            'page_index': i,
                            'external_provider': external_provider,
                        })
                    return result
            except Exception as e:
                logger.warning(f"puzzle_folios enrich_metadata failed for {sys_id}: {e}")

        # NLI path: use IIIF manifest FL IDs
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        if fl_ids:
            result = []
            for i, fid in enumerate(fl_ids):
                leaf = (i // 2) + 1
                side = 'r' if i % 2 == 0 else 'v'
                result.append({'fl_id': fid, 'label': f'{leaf}{side}'})
            return result

        # Last fallback: images_ext without external_provider
        if ext_data:
            images_ext = ext_data.get('images_ext', [])
            if images_ext:
                return [
                    {'fl_id': '', 'label': img.get('label', '') or str(i + 1),
                     'image_url': img.get('url', ''), 'page_index': i,
                     'external_provider': ext_data.get('external_provider', '')}
                    for i, img in enumerate(images_ext)
                ]
        return []

    # === Puzzle Document CRUD + Export (Phase 50) ===

    @target_app.get('/api/puzzle_documents')
    def puzzle_documents_list():
        """List all saved puzzle documents."""
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service(thread_safe=True)
        return svc.list_documents()

    @target_app.get('/api/puzzle_document/{doc_id}')
    def puzzle_document_get(doc_id: str):
        """Load a specific puzzle document."""
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service(thread_safe=True)
        doc = svc.load_document(doc_id)
        if doc is None:
            from starlette.responses import JSONResponse
            return JSONResponse({'error': 'not found'}, status_code=404)
        return {
            'id': doc.id, 'title': doc.title, 'notes': doc.notes,
            'join_type': doc.join_type,
            'fragments': [
                {'sys_id': f.sys_id, 'folio_label': f.folio_label, 'fl_id': f.fl_id,
                 'shelfmark': f.shelfmark, 'x': f.x, 'y': f.y,
                 'rotation': f.rotation, 'scale': f.scale,
                 'flip_h': f.flip_h, 'flip_v': f.flip_v,
                 'bg_removal_threshold': f.bg_removal_threshold,
                 'crop_top': f.crop_top, 'crop_bottom': f.crop_bottom,
                 'crop_left': f.crop_left, 'crop_right': f.crop_right,
                 'processed': f.processed}
                for f in doc.fragments
            ],
            'created_at': doc.created_at, 'updated_at': doc.updated_at
        }

    @target_app.post('/api/puzzle_document')
    async def puzzle_document_save(request: Request):
        """Save or update a puzzle document."""
        from shared.puzzle_service import get_puzzle_service
        from shared.puzzle_model import PuzzleDocument, PuzzleFragment
        from shared.puzzle_export import generate_thumbnail
        from shared.puzzle_image_service import get_puzzle_image_service
        import uuid

        body = await request.json()
        fragments = [PuzzleFragment(**f) for f in body.get('fragments', [])]
        doc = PuzzleDocument(
            id=body.get('id', ''),
            title=body.get('title', ''),
            notes=body.get('notes', ''),
            fragments=fragments,
        )
        if not doc.id:
            doc.id = str(uuid.uuid4())

        # Generate thumbnail
        img_svc = get_puzzle_image_service()
        thumb = generate_thumbnail(fragments, img_svc, thumb_size=150)

        svc = get_puzzle_service(thread_safe=True)
        doc_id = svc.save_document(doc, thumbnail_b64=thumb)
        if doc_id:
            return {'id': doc_id, 'status': 'ok'}
        from starlette.responses import JSONResponse
        return JSONResponse({'error': 'save failed'}, status_code=500)

    @target_app.delete('/api/puzzle_document/{doc_id}')
    def puzzle_document_delete(doc_id: str):
        """Delete a puzzle document."""
        from shared.puzzle_service import get_puzzle_service
        svc = get_puzzle_service(thread_safe=True)
        ok = svc.delete_document(doc_id)
        if ok:
            return {'status': 'ok'}
        from starlette.responses import JSONResponse
        return JSONResponse({'error': 'not found'}, status_code=404)

    @target_app.post('/api/puzzle_export')
    async def puzzle_export(request: Request):
        """Export composite PNG from fragment data."""
        from shared.puzzle_model import PuzzleFragment
        from shared.puzzle_export import compose_puzzle_export
        from shared.puzzle_image_service import get_puzzle_image_service
        import io

        body = await request.json()
        fragments = [PuzzleFragment(**f) for f in body.get('fragments', [])]
        if not fragments:
            from starlette.responses import JSONResponse
            return JSONResponse({'error': 'no fragments'}, status_code=400)

        img_svc = get_puzzle_image_service()
        result = compose_puzzle_export(fragments, img_svc, export_size=3000, margin=20)
        if result is None:
            from starlette.responses import JSONResponse
            return JSONResponse({'error': 'export failed'}, status_code=500)

        buf = io.BytesIO()
        result.save(buf, 'PNG')
        buf.seek(0)
        return Response(
            content=buf.getvalue(),
            media_type='image/png',
            headers={'Content-Disposition': 'attachment; filename="puzzle_export.png"'}
        )

    @target_app.get('/api/puzzle_thumbnail/{doc_id}')
    def puzzle_thumbnail(doc_id: str):
        """Serve thumbnail image for a document."""
        from shared.puzzle_service import get_puzzle_service
        import base64

        svc = get_puzzle_service(thread_safe=True)
        docs = svc.list_documents()
        for d in docs:
            if d['id'] == doc_id:
                thumb_b64 = d.get('thumbnail_b64', '')
                if thumb_b64:
                    return Response(
                        content=base64.b64decode(thumb_b64),
                        media_type='image/png'
                    )
        # Return 1x1 transparent pixel as fallback
        return Response(
            content=b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            media_type='image/png'
        )

    @target_app.get('/api/proxy_image')
    def proxy_image(url: str):
        """
        Proxy image requests to NLI to bypass Referer checks.
        Spoofs the Referer header to look like it's coming from nli.org.il.
        """
        if not url:
            return Response(status_code=400)

        # Validate URL format and domain to prevent SSRF attacks
        try:
            parsed = urlparse(url)
            if parsed.scheme not in ('http', 'https'):
                return Response(content="Invalid URL scheme", status_code=400)
            if not parsed.netloc:
                return Response(content="Invalid URL", status_code=400)
            if parsed.netloc not in ALLOWED_IMAGE_DOMAINS:
                return Response(content="Domain not allowed", status_code=403)
        except Exception:
            return Response(content="Invalid URL format", status_code=400)  # Request processing failed; return error response

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.nli.org.il/',
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        }

        try:
            # Fetch the image with timeout
            resp = requests.get(url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type=resp.headers.get('Content-Type', 'image/jpeg')
                )
            else:
                logger.info(f"Proxy got status {resp.status_code} for URL: {url}")
                return Response(status_code=resp.status_code)
        except requests.Timeout:
            logger.error(f"Proxy timeout for URL: {url}")
            return Response(content="Request timeout", status_code=504)
        except Exception as e:
            logger.error(f"Proxy error for {url}: {e}")
            return Response(status_code=500)

    # ── Visual Similarity Suggestions ─────────────────────────────
    # NOTE: /version route MUST come before /{sys_id} to avoid wildcard capture

    @target_app.get('/api/visual_suggestions/version')
    def visual_suggestions_version():
        """Return VS database version metadata for cache staleness detection.

        FROZEN CONTRACT (Wave 2 depends on this):
        Returns {version: str, import_date: str, pair_count: str, manuscript_count: str}
        """
        from shared.visual_similarity_service import get_vs_service
        svc = get_vs_service(thread_safe=True)
        if not svc.is_available():
            return {'version': '', 'import_date': '', 'pair_count': '0', 'manuscript_count': '0'}
        return svc.get_db_version()

    @target_app.get('/api/visual_similarity_db')
    def visual_similarity_db_download():
        """Serve the full visual_similarity.db for optional download (D-03).
        Includes Content-Length and X-Checksum-SHA256 headers for client validation."""
        import hashlib
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'fist_data', 'visual_similarity.db')
        if not os.path.exists(db_path):
            from starlette.responses import JSONResponse
            return JSONResponse({'error': 'Database not available'}, status_code=404)

        # Compute SHA256 checksum
        sha256 = hashlib.sha256()
        with open(db_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        checksum = sha256.hexdigest()
        file_size = os.path.getsize(db_path)

        from starlette.responses import FileResponse
        return FileResponse(
            db_path,
            media_type='application/octet-stream',
            filename='visual_similarity.db',
            headers={
                'Content-Length': str(file_size),
                'X-Checksum-SHA256': checksum,
                'X-File-Size-MB': str(round(file_size / 1024 / 1024, 1)),
            }
        )

    @target_app.post('/api/visual_suggestions/batch_check')
    async def visual_suggestions_batch_check(request: Request):
        """Check which sys_ids have visual suggestions.
        Body: {"sys_ids": [...]} -- max 500 IDs per request.

        FROZEN CONTRACT: Returns {sys_id: bool, ...}
        """
        from shared.visual_similarity_service import get_vs_service
        body = await request.body()
        data = json.loads(body)
        sys_ids = data.get('sys_ids', [])[:500]  # Enforce limit per review feedback
        svc = get_vs_service(thread_safe=True)
        if not svc.is_available():
            return {sid: False for sid in sys_ids}
        return svc.batch_has_suggestions(sys_ids)

    @target_app.get('/api/visual_suggestions/{sys_id}')
    def visual_suggestions_api(sys_id: str, limit: int = 200):
        """Return ranked visual similarity suggestions for a manuscript.

        FROZEN CONTRACT (Wave 2 depends on this):
        Returns list of dicts: {alma_id: str, rank: int, svm_score: float,
                                shelfmark: str, library_code: str, domain: str}
        """
        from shared.visual_similarity_service import get_vs_service
        svc = get_vs_service(thread_safe=True)
        if not svc.is_available():
            return []
        suggestions = svc.get_suggestions(sys_id, limit=limit)
        if not suggestions:
            return []

        # Enrich with shelfmark and library_code from csv_bank
        csv_bank = state.meta_mgr.csv_bank if state.meta_mgr else None
        if csv_bank:
            for s in suggestions:
                meta = csv_bank.get(s['alma_id'])
                if meta:
                    s['shelfmark'] = meta.get('shelfmark', '')
                    s['library_code'] = meta.get('library_code', '')
                else:
                    s['shelfmark'] = ''
                    s['library_code'] = ''
        else:
            for s in suggestions:
                s['shelfmark'] = ''
                s['library_code'] = ''

        # Enrich with domain from FJMS
        try:
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            if fjms.is_available():
                for s in suggestions:
                    domains = fjms.get_domains(s['alma_id'])
                    s['domain'] = domains[0]['name'] if domains else ''
            else:
                for s in suggestions:
                    s['domain'] = ''
        except Exception:
            for s in suggestions:  # Enrichment failed; populate empty defaults
                s.setdefault('domain', '')

        return suggestions

    @target_app.get('/api/export/excel')
    def export_excel():
        """Export search results to Excel format using unified export service."""
        # Reads per-session payload via web.export_state (Phase 88, 2026-05-13).
        # Historical context: the pre-Phase-88 AppState mirror fields (deleted
        # in STATE-01) leaked User A's query name into User B's xlsx filename
        # across separate devices (2026-05-12 incident). Phase 88 removed the
        # singleton mirrors entirely; export state is now per-session via
        # app.storage.user routed through web.safe_storage.
        from web.export_state import get_search_export
        payload = get_search_export()
        if not payload or not payload.get('results'):
            return Response("No results to export", status_code=400)

        all_results = payload['results']
        query = payload.get('query') or ''
        _sel = payload.get('selected_uids')
        if _sel:
            _sel_set = set(_sel)
            _results = [r for r in all_results if r.get('uid', '') in _sel_set]
        else:
            _results = all_results

        # Phase 94 EXPORT-META-06: enrichment signals from session payload.
        # Wave 2 plumbed them into scope here; Wave 3 closes the kwarg pass
        # below via the restructured export_search_results_excel signature.
        _transcription_sys_ids = set(payload.get('transcription_sys_ids') or [])
        _printed_ids = set(payload.get('printed_ids') or [])
        _result_domains = payload.get('result_domains') or {}
        # Smoke verification round 2 (2026-05-21): credits-info-sheet
        # metadata + Hebrew domain substitution. ``mode`` / ``gap`` came
        # from set_search_export's existing kwargs; ``domain_name_map`` is
        # the new kwarg added in this commit.
        _search_mode = payload.get('mode')
        _search_gap = payload.get('gap')
        _domain_name_map = payload.get('domain_name_map') or {}

        # Phase 94 EXPORT-META-05 / D-04: UI lang controls sheet view direction
        # (content stays English regardless).
        # MUST-FIX 94-03-C: read the canonical per-user UI lang from the
        # safe_storage chokepoint (web/main.py:832-834, 1001-1002) — NOT
        # from the module-global get_language() in the translations module,
        # which may not reflect the current per-request user state under
        # the multitenant architecture (Phase 87-92 invariants).
        from web.safe_storage import safe_user_get
        _ui_lang = safe_user_get('ui_language', 'he')
        # Normalize: anything other than 'en' is treated as 'he' (the default).
        _ui_lang = 'en' if _ui_lang == 'en' else 'he'

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_search_results_excel(
                _results, query,
                # Phase 94 EXPORT-META Wave 3 kwarg activation:
                transcription_sys_ids=_transcription_sys_ids,
                printed_ids=_printed_ids,
                result_domains=_result_domains,
                lang=_ui_lang,
                # Smoke verification round 2 (2026-05-21):
                search_mode=_search_mode,
                search_gap=_search_gap,
                domain_name_map=_domain_name_map,
            )
            if _sel and len(_results) < len(all_results):
                root, _, ext = filename.rpartition('.')
                filename = f"{root}-selected-{len(_results)}.{ext}" if root else filename
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export Excel error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/word')
    def export_word():
        """Export search results to Word format using unified export service."""
        from web.export_state import get_search_export
        payload = get_search_export()
        if not payload or not payload.get('results'):
            return Response("No results to export", status_code=400)

        all_results = payload['results']
        query = payload.get('query') or ''
        _sel = payload.get('selected_uids')
        if _sel:
            _sel_set = set(_sel)
            _results = [r for r in all_results if r.get('uid', '') in _sel_set]
        else:
            _results = all_results

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_search_results_word(
                _results, query
            )
            if _sel and len(_results) < len(all_results):
                root, _, ext = filename.rpartition('.')
                filename = f"{root}-selected-{len(_results)}.{ext}" if root else filename
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export Word error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/parallels/excel')
    def export_parallels_excel():
        """Export parallels results to Excel using unified export service."""
        from web.export_state import get_parallels_export

        session_payload = get_parallels_export() or {}
        parallels_results = session_payload.get('results') or []
        filtered_results = session_payload.get('filtered') or []
        meta = session_payload.get('meta') or {}
        # Phase 88 D-14: source_text reads exclusively from per-session meta.
        # Legacy app.storage.user key fallback removed; writer at parallels.py
        # populates meta['source_text'] on every completion path.
        source_text = meta.get('source_text') or ''

        if not parallels_results and not filtered_results:
            return Response("No parallels results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_parallels_excel(
                parallels_results, filtered_results, source_text=source_text
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export Parallels Excel error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/parallels/word')
    def export_parallels_word():
        """Export parallels results to Word using unified export service."""
        from web.export_state import get_parallels_export

        session_payload = get_parallels_export() or {}
        parallels_results = session_payload.get('results') or []
        filtered_results = session_payload.get('filtered') or []
        meta = session_payload.get('meta') or {}
        # Phase 88 D-14: source_text reads exclusively from per-session meta.
        # Legacy app.storage.user key fallback removed.
        source_text = meta.get('source_text') or ''

        if not parallels_results and not filtered_results:
            return Response("No parallels results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_parallels_word(
                parallels_results, filtered_results, source_text=source_text
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export Parallels Word error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/json')
    def export_json():
        """Phase 77 EXPORT-01/03/04: search results as Claude-friendly JSON.

        Stateful download (mirrors existing /api/export/excel pattern). Reads
        per-session payload from web.export_state (formerly state.* singleton,
        which leaked across users — 2026-05-12). Phase 78 /api/search is the
        stateless POST counterpart.
        """
        from starlette.responses import JSONResponse
        from shared.search_serializer import (
            serialize_search_payload, build_search_filename,
        )
        from web.export_state import get_search_export

        session_payload = get_search_export()
        if not session_payload or not session_payload.get('results'):
            return Response("No results to export", status_code=400)

        all_results = session_payload['results']
        _sel = session_payload.get('selected_uids')
        if _sel:
            _sel_set = set(_sel)
            _results = [r for r in all_results if r.get('uid', '') in _sel_set]
        else:
            _results = all_results

        try:
            payload = serialize_search_payload(
                _results,
                meta_mgr=state.meta_mgr,
                query=session_payload.get('query') or '',
                mode=session_payload.get('mode') or 'text',
                gap=session_payload.get('gap'),
                filters=session_payload.get('filters'),
                warnings=session_payload.get('warnings') or [],
                # Phase 94 EXPORT-META-07: pass enrichment sets so per-item
                # has_pgp / is_printed are populated correctly (opt-in path
                # per MUST-FIX 94-02-B -- public /api/search at search_api.py
                # does NOT pass these, preserving its prior response shape).
                transcription_sys_ids=set(session_payload.get('transcription_sys_ids') or []),
                printed_sys_ids=set(session_payload.get('printed_ids') or []),
                # MUST-FIX 94-02-A: short-circuit FJMS round-trip -- the
                # session payload already has up-to-date domains.
                result_domains=session_payload.get('result_domains') or {},
            )
            filename = build_search_filename()
            if _sel and len(_results) < len(all_results):
                root, _, ext = filename.rpartition('.')
                filename = f"{root}-selected-{len(_results)}.{ext}" if root else filename
            return JSONResponse(
                payload,
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.exception(f"Export JSON error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/parallels/json')
    def export_parallels_json():
        """Phase 77 EXPORT-02/03/04: parallels results as Claude-friendly JSON.

        Stateful download (mirrors existing /api/export/parallels/excel pattern).
        Reads per-session payload from web.export_state (formerly state.* singleton,
        which leaked across users — 2026-05-12). Phase 80 /api/parallels is the
        stateless POST counterpart.
        """
        from starlette.responses import JSONResponse
        from shared.search_serializer import (
            serialize_parallels_payload, build_parallels_filename,
        )
        from web.export_state import get_parallels_export

        session_payload = get_parallels_export() or {}
        parallels_results = session_payload.get('results') or []
        filtered_results = session_payload.get('filtered') or []

        # Empty-state check first - avoids touching storage when there's nothing
        # to export. Per Phase 88 D-14, source_text reads exclusively from meta.
        if not parallels_results and not filtered_results:
            return Response("No parallels results to export", status_code=400)

        meta = session_payload.get('meta') or {}
        source_text = meta.get('source_text') or ''

        try:
            payload = serialize_parallels_payload(
                parallels_results,
                filtered_results,
                meta_mgr=state.meta_mgr,
                source_text=source_text,
                chunk_size=meta.get('chunk_size', 5),
                mode=meta.get('mode', 'exact') or 'exact',
                max_freq=meta.get('max_freq'),
                boundary_options=meta.get('boundary_options'),
                warnings=meta.get('warnings') or [],
            )
            filename = build_parallels_filename()
            return JSONResponse(
                payload,
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.exception(f"Export Parallels JSON error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/browse/word')
    def export_browse_word():
        """Export current browse page to Word using unified export service."""
        # 2026-05-12: routed through safe_user_get so prune_user_storage races
        # don't surface as 500 (Codex review HIGH finding on the v7.11.0 fixes).
        from web.safe_storage import safe_user_get
        browse_data = safe_user_get('browse_export_data')
        if not browse_data:
            return Response("No browse data to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_browse_word(browse_data)
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export Browse Word error: {e}")
            return Response("Export failed", status_code=500)

    @target_app.get('/api/export/list/{list_id}/excel')
    def export_list_excel(list_id: str):
        """Export a specific list to Excel using unified export service."""
        if not state.lists_mgr:
            return Response("Lists manager not available", status_code=400)

        list_data = state.lists_mgr.data.get('lists', {}).get(list_id)
        if not list_data:
            return Response("List not found", status_code=404)

        # Use get_items_in_list() - items are stored in data['items'] with list membership
        items = state.lists_mgr.get_items_in_list_sync(list_id)
        if not items:
            return Response("List is empty", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_list_excel(
                list_id, list_data.get('name', 'list'), items
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            logger.error(f"Export List Excel error: {e}")
            return Response("Export failed", status_code=500)

