# -*- coding: utf-8 -*-
"""
Shared Image Resolver/Cache for Fragment Puzzle.

Fetches IIIF fragment images, applies background removal, and caches
processed results to disk. Used by both web and desktop apps.

Cache key format: {fl_id}_{size}_{threshold}.png (processed) or {fl_id}_{size}_original.jpg (raw)
Files are only ever created, never replaced: a write to a name that already
exists keeps the existing file.
Cache location:
  - Windows installed: {LOCALAPPDATA}/GenizahSearchPro/cache/puzzle/
  - Development/other: {project_root}/cache/puzzle/

Images a web browser uploads (``store_upload``):
  - from a signed-in account: into the shared cache above, and one line is
    appended to ``_uploads.jsonl`` in the cache directory recording the file,
    the account id and the UTC time;
  - from a signed-out browser: into ``_browser/<hash of the browser key>/``,
    and only that browser is given them back (``browser_key=`` lookups).
A lookup always tries the shared file first, then the requester's own.
The desktop app never passes a browser key, so it sees only the shared cache.
"""

import hashlib
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests

from shared.background_removal import remove_background, DEFAULT_THRESHOLD

# Phase 98 D-19 + D-20: shared NLI circuit breaker for puzzle IIIF fetches.
from shared.nli_circuit_breaker import (
    is_open as _nli_circuit_is_open,
    record_failure as _nli_record_failure,
    record_success as _nli_record_success,
    NLI_CONNECT_TIMEOUT,
    NLI_IMAGE_READ_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Processing algorithm version. Included in cache keys for processed images
# so that cache entries are automatically invalidated when the background
# removal algorithm changes.
PROCESSING_VERSION = 'v4'

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"

# Browser-scoped uploads live under this subdirectory of the cache, one
# directory per browser; the shared-upload record is this file.
BROWSER_UPLOADS_DIR = '_browser'
UPLOAD_MANIFEST_NAME = '_uploads.jsonl'

# Where store_upload put an image.
STORED_SHARED = 'shared'
STORED_BROWSER = 'browser'
STORED_NOWHERE = ''

_manifest_lock = threading.Lock()

# Size presets (width in pixels)
SIZE_PRESETS = {
    'small': 400,
    'medium': 800,
    'large': 1200,
    'full': 2000,
}


def _get_default_cache_dir() -> Path:
    """Determine cache directory based on platform.

    Windows: {LOCALAPPDATA}/GenizahSearchPro/cache/puzzle/
    Other: {project_root}/cache/puzzle/
    """
    local_app_data = os.environ.get('LOCALAPPDATA')
    if local_app_data:
        return Path(local_app_data) / 'GenizahSearchPro' / 'cache' / 'puzzle'

    # Fallback: project root
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current / 'cache' / 'puzzle'
        current = current.parent
    return Path.cwd() / 'cache' / 'puzzle'


def _safe_filename(fl_id: str) -> str:
    """Create a filesystem-safe version of an FL ID."""
    return re.sub(r'[^a-zA-Z0-9_-]', '_', str(fl_id))


# Width presets a web request may ask for, and the threshold range.
REQUEST_SIZES = (400, 800, 1200, 2000)


def normalize_request_params(size, threshold):
    """Snap a web request's size to a preset and clamp its threshold.

    Used by the web routes so that a lookup, the upload token it hands out,
    and the upload that follows all name the same cache file.
    """
    try:
        size = int(size)
    except (TypeError, ValueError):
        size = 800
    if size not in REQUEST_SIZES:
        size = min(REQUEST_SIZES, key=lambda s: abs(s - size))
    try:
        threshold = float(threshold)
    except (TypeError, ValueError):
        threshold = DEFAULT_THRESHOLD
    if threshold != threshold:  # NaN
        threshold = DEFAULT_THRESHOLD
    threshold = max(0.0, min(255.0, threshold))
    return size, threshold


def _write_new_file(path: Path, data: bytes) -> bool:
    """Create ``path`` with ``data``. Never replaces an existing file.

    Returns True if this call created the file, False if a file of that name
    already existed. Other OS errors propagate.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, 'xb') as fh:
            fh.write(data)
    except FileExistsError:
        return False
    return True


def _browser_dir_name(browser_key: str) -> str:
    """Directory name for one browser's uploads: a hash, never the key itself."""
    return hashlib.sha256(str(browser_key).encode('utf-8')).hexdigest()[:24]


class PuzzleImageService:
    """Resolves fragment images: IIIF fetch -> background removal -> disk cache."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self._cache_dir = cache_dir or _get_default_cache_dir()
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _cache_filename(fl_id: str, size: int, threshold: float,
                        processed: bool, is_cul: bool) -> str:
        """File name of a (current-version) cache entry."""
        safe_id = _safe_filename(fl_id)
        if processed:
            suffix = '_cul' if is_cul else ''
            return f"{safe_id}_{size}_{threshold:.1f}{suffix}_{PROCESSING_VERSION}.png"
        return f"{safe_id}_{size}_original.jpg"

    def get_cache_path(self, fl_id: str, size: int = 800,
                       threshold: float = DEFAULT_THRESHOLD,
                       processed: bool = True,
                       is_cul: bool = False) -> Path:
        """Deterministic shared-cache path for a specific (fl_id, size, threshold) combination.
        Falls back to legacy (unversioned) path if it exists, for backward compat."""
        versioned = self._cache_dir / self._cache_filename(fl_id, size, threshold, processed, is_cul)
        if processed:
            if versioned.exists():
                return versioned
            # Fall back to legacy path (no version suffix) if it exists
            safe_id = _safe_filename(fl_id)
            suffix = '_cul' if is_cul else ''
            legacy = self._cache_dir / f"{safe_id}_{size}_{threshold:.1f}{suffix}.png"
            if legacy.exists():
                return legacy
        # New files use versioned path
        return versioned

    def get_browser_cache_path(self, browser_key: Optional[str], fl_id: str, size: int = 800,
                               threshold: float = DEFAULT_THRESHOLD,
                               processed: bool = True,
                               is_cul: bool = False) -> Optional[Path]:
        """Path of one browser's own copy of a cache entry, or None without a key."""
        if not browser_key:
            return None
        return (self._cache_dir / BROWSER_UPLOADS_DIR / _browser_dir_name(browser_key)
                / self._cache_filename(fl_id, size, threshold, processed, is_cul))

    def read_cached(self, fl_id: str, size: int = 800,
                    threshold: float = DEFAULT_THRESHOLD,
                    processed: bool = True,
                    is_cul: bool = False,
                    browser_key: Optional[str] = None) -> Optional[Tuple[bytes, bool]]:
        """Return ``(bytes, is_browser_copy)`` for a cached entry, or None.

        The shared file is tried first, then (with ``browser_key``) that
        browser's own copy. ``is_browser_copy`` is True only for the latter,
        which must be served to that browser alone.
        """
        candidates = [(self.get_cache_path(fl_id, size, threshold, processed, is_cul), False)]
        own = self.get_browser_cache_path(browser_key, fl_id, size, threshold, processed, is_cul)
        if own is not None:
            candidates.append((own, True))
        for path, is_browser_copy in candidates:
            try:
                if path.is_file():
                    return path.read_bytes(), is_browser_copy
            except OSError:
                continue  # TOCTOU race or read error: try the next one
        return None

    def store_upload(self, fl_id: str, size: int, threshold: float,
                     processed: bool, is_cul: bool, data: bytes, *,
                     user_id: Optional[str] = None,
                     browser_key: Optional[str] = None) -> str:
        """Keep image bytes a web browser uploaded. Never replaces a file.

        With ``user_id`` (a signed-in account) the bytes go to the shared cache
        and the account id and UTC time are appended to the upload record.
        Otherwise, with ``browser_key``, they go to that browser's own
        directory. With neither, nothing is kept.

        Returns STORED_SHARED, STORED_BROWSER or STORED_NOWHERE. A name that
        already exists counts as stored (the existing file is kept as is).
        """
        if not data:
            return STORED_NOWHERE
        user_id = str(user_id).strip() if user_id else ''
        try:
            if user_id:
                path = self.get_cache_path(fl_id, size, threshold, processed, is_cul)
                if _write_new_file(path, data):
                    self._record_shared_upload(path.name, user_id)
                    logger.info("Stored uploaded image in shared cache: %s (%d bytes)",
                                path.name, len(data))
                return STORED_SHARED
            path = self.get_browser_cache_path(browser_key, fl_id, size, threshold, processed, is_cul)
            if path is None:
                return STORED_NOWHERE
            _write_new_file(path, data)
            return STORED_BROWSER
        except OSError as e:
            logger.warning("Failed to store uploaded image for %s: %s", fl_id, e)
            return STORED_NOWHERE

    def _record_shared_upload(self, file_name: str, user_id: str) -> None:
        """Append one line to the shared-upload record (JSON lines)."""
        line = json.dumps({
            'file': file_name,
            'user_id': user_id,
            'uploaded_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        }, ensure_ascii=True, sort_keys=True)
        manifest = self._cache_dir / UPLOAD_MANIFEST_NAME
        try:
            with _manifest_lock:
                with open(manifest, 'a', encoding='utf-8', newline='\n') as fh:
                    fh.write(line + '\n')
        except OSError as e:
            logger.warning("Failed to record shared upload %s: %s", file_name, e)

    def for_browser(self, browser_key: Optional[str]) -> 'BrowserImageView':
        """A view of this service that also finds ``browser_key``'s own images.

        Hand it to shared code that takes an image service (exports,
        thumbnails, publishing) so that a browser's uploads are used for that
        browser's own work.
        """
        return BrowserImageView(self, browser_key)

    def resolve_fragment_image(self, fl_id: str, size: int = 800,
                                threshold: float = DEFAULT_THRESHOLD,
                                processed: bool = True,
                                is_cul: bool = False,
                                image_url: str = '',
                                browser_key: Optional[str] = None) -> Optional[bytes]:
        """Fetch IIIF image, apply background removal, cache result.

        Args:
            fl_id: NLI FL ID for the fragment image (empty for non-NLI)
            size: Image width in pixels (default 800)
            threshold: Background removal threshold (default 30.0)
            processed: If True, apply background removal. If False, return original.
            is_cul: If True, also remove CUL blue conservation mat.
            image_url: Direct IIIF canvas URL for non-NLI libraries. When non-empty,
                       fetched directly instead of constructing NLI URL from fl_id.
            browser_key: When given, that browser's own uploaded copy is used
                       if there is no shared one.

        Returns:
            Image bytes (RGBA PNG if processed, JPEG if original), or None on failure.
        """
        # Determine cache key — use fl_id for NLI, safe filename of URL for external
        cache_id = fl_id if fl_id else _safe_filename(image_url[:120])
        if not cache_id:
            return None

        # Return cached if exists (shared first, then this browser's own)
        cached = self.read_cached(cache_id, size, threshold, processed, is_cul,
                                  browser_key=browser_key)
        if cached is not None:
            return cached[0]
        cache_path = self.get_cache_path(cache_id, size, threshold, processed, is_cul)

        # Fetch image
        if image_url:
            raw_bytes = self._fetch_direct_url(image_url, size)
        else:
            raw_bytes = self._fetch_iiif_image(fl_id, size)
        if raw_bytes is None:
            return None

        if not processed:
            # Cache and return original
            try:
                _write_new_file(cache_path, raw_bytes)
            except OSError as e:
                logger.warning(f"Failed to cache image for {cache_id}: {e}")
            return raw_bytes

        # Apply background removal
        try:
            result_bytes = remove_background(raw_bytes, threshold=threshold, is_cul=is_cul)
        except Exception as e:
            logger.error(f"Background removal failed for {cache_id}: {e}")
            return raw_bytes  # fallback to original on error

        # Cache processed result
        try:
            _write_new_file(cache_path, result_bytes)
        except OSError as e:
            logger.warning(f"Failed to cache processed image for {cache_id}: {e}")
        return result_bytes

    def _fetch_direct_url(self, image_url: str, size: int) -> Optional[bytes]:
        """Fetch image from a direct IIIF canvas URL (non-NLI libraries).

        Constructs the full IIIF Image API URL if the given URL is a canvas base URL,
        or uses it directly if it already contains '/full/'.

        Phase 98 D-20: NLI / Rosetta URLs are guarded by the shared circuit breaker
        and use a bounded timeout. Non-NLI hosts (Cambridge, Manchester, Oxford, etc.)
        retain the existing 30s timeout — they have legitimate slower response times
        for large IIIF tiles and have not exhibited the threadpool-saturation pattern.
        """
        if '/full/' in image_url:
            url = image_url  # Already a complete image URL
        else:
            url = f"{image_url}/full/{size},/0/default.jpg"

        # Phase 98 D-20: host-conditional breaker scoping. urlparse is evaluated
        # AFTER URL construction so a caller cannot smuggle a different host past
        # the check via the '/full/' short-circuit (T-98-04-03).
        parsed = urlparse(url)
        is_nli_host = parsed.netloc in ('iiif.nli.org.il', 'rosetta.nli.org.il')

        if is_nli_host and _nli_circuit_is_open():
            return None

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        try:
            # NLI: bounded env-driven timeout. Non-NLI: existing 30s preserved.
            timeout = (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT) if is_nli_host else 30
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and len(resp.content) > 100:
                logger.info(f"Direct IIIF fetch OK for {image_url[:80]}")
                if is_nli_host:
                    _nli_record_success(path='puzzle_fetch_direct_url')
                return resp.content
            else:
                logger.warning(f"Direct IIIF fetch non-200 for {image_url[:80]}: status={resp.status_code}")
                if is_nli_host:
                    if resp.status_code == 429:
                        _nli_record_failure(failure_type='429', path='puzzle_fetch_direct_url')
                    elif 500 <= resp.status_code < 600:
                        _nli_record_failure(failure_type='5xx', path='puzzle_fetch_direct_url')
        except requests.exceptions.Timeout as e:
            logger.warning(f"Direct IIIF timeout for {image_url[:80]}: {e}")
            if is_nli_host:
                _nli_record_failure(failure_type='timeout', path='puzzle_fetch_direct_url')
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Direct IIIF connection error for {image_url[:80]}: {e}")
            if is_nli_host:
                _nli_record_failure(failure_type='connection_error', path='puzzle_fetch_direct_url')
        except Exception as e:
            # Catch-all for malformed URLs / unexpected errors — preserve existing
            # observability without affecting the breaker (non-NLI hosts may emit
            # provider-specific exceptions that should not trip the NLI breaker).
            logger.warning(f"Direct IIIF fetch failed for {image_url[:80]}: {e}")
        return None

    def save_derivative_to_cache(self, fl_id: str, size: int, threshold: float,
                                 is_cul: bool, png_bytes: bytes) -> bool:
        """Save externally-processed image bytes to the cache.

        Used when the browser extension or desktop app provides already-processed
        image data that should be persisted to the server cache.

        Args:
            fl_id: NLI FL ID for the fragment.
            size: Image width in pixels.
            threshold: Background removal threshold used.
            is_cul: Whether CUL blue mat removal was applied.
            png_bytes: Processed PNG image bytes.

        Returns:
            True if the entry is now cached (saved, or already present),
            False otherwise.
        """
        if not png_bytes or png_bytes[:4] != b'\x89PNG':
            logger.warning(f"save_derivative_to_cache: invalid PNG header for {fl_id}")
            return False
        cache_path = self.get_cache_path(fl_id, size, threshold, True, is_cul)
        try:
            if _write_new_file(cache_path, png_bytes):
                logger.info(f"Saved derivative to cache: {cache_path.name} ({len(png_bytes)} bytes)")
            return True
        except OSError as e:
            logger.warning(f"Failed to save derivative for {fl_id}: {e}")
            return False

    def invalidate_cache(self, fl_id: str, threshold: Optional[float] = None):
        """Clear cached images for a specific fl_id.

        Args:
            fl_id: The fragment to invalidate
            threshold: If provided, only clear entries for this threshold.
                       If None, clear all entries for this fl_id.
        """
        safe_id = _safe_filename(fl_id)
        if threshold is not None:
            # Remove specific threshold file(s) — match both versioned and legacy
            pattern = f"{safe_id}_*_{threshold:.1f}*.png"
        else:
            # Remove all files for this fl_id (processed and original)
            pattern = f"{safe_id}_*"
        for f in self._cache_dir.glob(pattern):
            f.unlink(missing_ok=True)

    def _fetch_iiif_image(self, fl_id: str, size: int) -> Optional[bytes]:
        """Fetch image from NLI IIIF (direct).

        Works from desktop/local dev where NLI is reachable. On production
        servers where NLI blocks datacenter IPs, this returns None and the
        web client falls back to the localhost helper service.

        NOTE: Does NOT include Rosetta thumbnail fallback. Rosetta returns tiny
        low-quality thumbnails that look bad in the puzzle. Better to return None
        and let the caller's fallback chain (extension, helper, proxy) handle it.

        Phase 98 D-19: guarded by shared NLI circuit breaker; bounded timeout
        replaces hard-coded 30s.
        """
        digits = re.sub(r"\D", "", str(fl_id))
        if not digits:
            return None

        # Phase 98 D-19: short-circuit if NLI is known-degraded.
        if _nli_circuit_is_open():
            return None

        url = f"{NLI_IIIF_BASE}/FL{digits}/full/{size},/0/default.jpg"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        try:
            # Phase 98 D-19: env-driven (connect, read) tuple replaces hard-coded timeout=30.
            resp = requests.get(
                url,
                headers=headers,
                timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT),
            )
            if resp.status_code == 200 and len(resp.content) > 100:
                logger.info(f"IIIF fetch OK for {fl_id}")
                _nli_record_success(path='puzzle_fetch_iiif_image')
                return resp.content
            elif resp.status_code == 429:
                _nli_record_failure(failure_type='429', path='puzzle_fetch_iiif_image')
                logger.warning(f"IIIF 429 for {fl_id}")
            elif 500 <= resp.status_code < 600:
                _nli_record_failure(failure_type='5xx', path='puzzle_fetch_iiif_image')
                logger.warning(f"IIIF {resp.status_code} for {fl_id}")
            else:
                logger.warning(
                    f"IIIF fetch non-200 or empty for {fl_id}: "
                    f"status={resp.status_code}, size={len(resp.content)}"
                )
        except requests.exceptions.Timeout as e:
            logger.warning(f"IIIF timeout for {fl_id}: {e}")
            _nli_record_failure(failure_type='timeout', path='puzzle_fetch_iiif_image')
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"IIIF connection error for {fl_id}: {e}")
            _nli_record_failure(failure_type='connection_error', path='puzzle_fetch_iiif_image')

        return None


class BrowserImageView:
    """A PuzzleImageService seen by one browser (see ``for_browser``).

    ``resolve_fragment_image`` also finds that browser's own uploads; every
    other attribute is the underlying service's.
    """

    def __init__(self, service: PuzzleImageService, browser_key: Optional[str]):
        self._service = service
        self._browser_key = browser_key or None

    def resolve_fragment_image(self, fl_id: str, size: int = 800,
                               threshold: float = DEFAULT_THRESHOLD,
                               processed: bool = True,
                               is_cul: bool = False,
                               image_url: str = '') -> Optional[bytes]:
        return self._service.resolve_fragment_image(
            fl_id, size, threshold, processed, is_cul,
            image_url=image_url, browser_key=self._browser_key,
        )

    def __getattr__(self, name):
        return getattr(self._service, name)


# ── Singleton ──

_service_instance: Optional[PuzzleImageService] = None


def get_puzzle_image_service(cache_dir: Optional[Path] = None) -> PuzzleImageService:
    """Get or create singleton PuzzleImageService instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = PuzzleImageService(cache_dir=cache_dir)
    return _service_instance


def reset_puzzle_image_service():
    """Reset singleton instance (for testing)."""
    global _service_instance
    _service_instance = None


# ── Convenience functions ──

def resolve_fragment_image(fl_id: str, size: int = 800,
                            threshold: float = DEFAULT_THRESHOLD,
                            processed: bool = True,
                            is_cul: bool = False,
                            image_url: str = '') -> Optional[bytes]:
    """Module-level convenience for resolve_fragment_image."""
    return get_puzzle_image_service().resolve_fragment_image(
        fl_id, size, threshold, processed, is_cul, image_url=image_url
    )


def get_cache_path(fl_id: str, size: int = 800,
                   threshold: float = DEFAULT_THRESHOLD,
                   processed: bool = True,
                   is_cul: bool = False) -> Path:
    """Module-level convenience for get_cache_path."""
    return get_puzzle_image_service().get_cache_path(fl_id, size, threshold, processed, is_cul)


def invalidate_cache(fl_id: str, threshold: Optional[float] = None):
    """Module-level convenience for invalidate_cache."""
    get_puzzle_image_service().invalidate_cache(fl_id, threshold)


def save_derivative_to_cache(fl_id: str, size: int, threshold: float,
                              is_cul: bool, png_bytes: bytes) -> bool:
    """Module-level convenience for save_derivative_to_cache."""
    return get_puzzle_image_service().save_derivative_to_cache(
        fl_id, size, threshold, is_cul, png_bytes
    )
