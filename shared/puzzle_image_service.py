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
  - from a signed-in account: one line recording the file name, the account
    id, the UTC time and the SHA-256 of the bytes is appended to
    ``_uploads.jsonl`` in the cache directory and flushed to disk; only then
    is the file given its name in the shared cache above. If the line cannot
    be written the image is not shared (it is kept for the uploading browser
    only, when there is one). If the line was written but the file then did
    not get its name (another copy got it first, or the link step failed), a
    second line with the same file, account and SHA-256 and
    ``"published": false`` follows it: that upload was not shared;
  - from a signed-out browser: into ``_browser/<hash of the browser key>/``,
    and only that browser is given them back (``browser_key=`` lookups).
A lookup always tries the shared file first, then the requester's own.

Web and desktop rules. The web app asks with ``web=True`` (or through
``for_browser``): an image fetched from a direct URL is filed under that URL's
name, only known library image hosts are fetched, every redirect is followed
one hop at a time and each hop is checked against the same host list, and a
file is written only by an atomic create-only step (never a direct write to
the final name; where that step is not available the result is not cached).
A call without ``web=True`` -- the desktop app, the local helper, scripts --
keeps the behaviour the desktop had at v9.4.0: the fragment's fl_id names the
file when there is one, any URL is fetched and redirects are followed as
before, and the result is still cached when hard links are not available.
"""

import hashlib
import io
import json
import logging
import os
import re
import tempfile
import threading
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

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

# True while a web request is being resolved (see resolve_fragment_image's
# ``web``). The fetch helpers read it, so their signatures stay as they were.
_web_rules: ContextVar[bool] = ContextVar('puzzle_image_web_rules', default=False)

# Redirects a web fetch follows, each hop checked (see get_with_checked_redirects).
MAX_REDIRECT_HOPS = 5
REDIRECT_STATUSES = (301, 302, 303, 307, 308)

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


class CacheLinkUnavailable(OSError):
    """The cache directory cannot give a file its name atomically (no hard links)."""


class UploadNotRecorded(OSError):
    """The shared-upload record line could not be written."""


def _write_new_file(path: Path, data: bytes, *, before_publish=None,
                    allow_direct: bool = False) -> bool:
    """Create ``path`` with ``data``. Never replaces an existing file.

    The bytes are written to a temporary file in the same directory and
    flushed to disk; the final name is then added with a hard link, which is
    atomic and fails if the name exists. A reader therefore sees either no
    file or the whole file, and a failed write leaves nothing under ``path``.

    ``before_publish``, if given, is called after the bytes are on disk and
    before the final name exists; if it raises, nothing is published and the
    exception propagates.

    Returns True if this call created the file, False if a file of that name
    already existed (it is kept as is). When the file system has no hard
    links: with ``allow_direct`` (the desktop's own cache) the file is created
    directly under its name, as the desktop always did; otherwise
    ``CacheLinkUnavailable`` is raised and the caller leaves the result
    uncached. Other OS errors propagate.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return False
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix='.partial-')
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        with io.open(tmp, 'wb') as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        if before_publish is not None:
            before_publish()
        try:
            os.link(tmp, path)
        except FileExistsError:
            return False
        except (OSError, NotImplementedError, AttributeError) as e:
            if path.exists():
                return False
            if allow_direct:
                return _create_directly(path, data)
            raise CacheLinkUnavailable(f"no hard link for {path.name}: {e}") from e
        return True
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def _create_directly(path: Path, data: bytes) -> bool:
    """Create ``path`` in place (exclusive create); the desktop's fallback."""
    created = False
    try:
        with io.open(path, 'xb') as fh:
            created = True
            fh.write(data)
    except FileExistsError:
        if not created:
            return False
        raise
    except BaseException:
        if created:
            try:
                path.unlink()
            except OSError:
                pass
        raise
    return True


# Image hosts a direct image URL may point at: the libraries whose IIIF images
# the puzzle shows (NLI, Cambridge, Manchester, Oxford, Princeton/JTS).
DIRECT_IMAGE_HOST_SUFFIXES = (
    'nli.org.il',
    'cam.ac.uk',
    'manchester.ac.uk',
    'ox.ac.uk',
    'princeton.edu',
    'jtsa.edu',
)


def is_allowed_image_url(url: str) -> bool:
    """True if ``url`` is an http(s) URL on one of the known library image hosts."""
    try:
        parsed = urlparse(str(url))
        host = (parsed.hostname or '').lower().rstrip('.')
        port = parsed.port
    except ValueError:
        return False
    if parsed.scheme not in ('http', 'https') or not host:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    if port is not None and port not in (80, 443):
        return False
    return any(host == suffix or host.endswith('.' + suffix)
               for suffix in DIRECT_IMAGE_HOST_SUFFIXES)


class RedirectNotFollowed(requests.exceptions.RequestException):
    """A redirect pointed off the allowed hosts, or there were too many."""


def _redirect_location(resp) -> Optional[str]:
    """The Location of a redirect response, or None if it is not a redirect."""
    if getattr(resp, 'status_code', None) not in REDIRECT_STATUSES:
        return None
    headers = getattr(resp, 'headers', None) or {}
    try:
        location = headers.get('Location') or headers.get('location')
    except Exception:
        return None
    return location if isinstance(location, str) and location else None


def get_with_checked_redirects(url: str, *, allowed=None,
                               max_hops: int = MAX_REDIRECT_HOPS, **kwargs):
    """``requests.get`` that follows redirects one hop at a time.

    Every URL -- the first one and each redirect target -- must pass
    ``allowed`` (default: ``is_allowed_image_url``) before it is requested;
    each request is made with ``allow_redirects=False``. Raises
    ``RedirectNotFollowed`` for a URL that does not pass, or after
    ``max_hops`` redirects. Other keyword arguments go to ``requests.get``.
    """
    allowed = allowed or is_allowed_image_url
    kwargs.pop('allow_redirects', None)
    current = str(url)
    for _hop in range(max_hops + 1):
        if not allowed(current):
            raise RedirectNotFollowed(f"not an allowed image host: {current[:80]}")
        resp = requests.get(current, allow_redirects=False, **kwargs)
        location = _redirect_location(resp)
        if location is None:
            return resp
        try:
            resp.close()
        except Exception:
            pass
        current = urljoin(current, location)
    raise RedirectNotFollowed(f"more than {max_hops} redirects from {str(url)[:80]}")


def _image_get(url: str, **kwargs):
    """The HTTP GET of the fetch helpers: checked hops for the web, plain otherwise."""
    if _web_rules.get():
        return get_with_checked_redirects(url, **kwargs)
    return requests.get(url, **kwargs)


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

        With ``user_id`` (a signed-in account) the bytes go to the shared
        cache: the record line (file, account id, UTC time, SHA-256) is
        written and flushed to disk first, and only then is the file given its
        name. If the line cannot be written the image is not shared; it is
        kept for ``browser_key`` instead when there is one. Without
        ``user_id``, with ``browser_key``, the bytes go to that browser's own
        directory. With neither, nothing is kept.

        Returns STORED_SHARED, STORED_BROWSER or STORED_NOWHERE -- where the
        bytes can now be found. A name that already exists counts as stored
        (the existing file is kept as is).
        """
        if not data:
            return STORED_NOWHERE
        user_id = str(user_id).strip() if user_id else ''
        if user_id:
            path = self.get_cache_path(fl_id, size, threshold, processed, is_cul)
            recorded = []
            try:
                if path.is_file():
                    return STORED_SHARED

                def _record():
                    self._record_shared_upload(path.name, user_id, data)
                    recorded.append(True)

                created = _write_new_file(path, data, before_publish=_record)
                if created:
                    logger.info("Stored uploaded image in shared cache: %s (%d bytes)",
                                path.name, len(data))
                elif recorded:
                    # Another copy got the name between our record line and our link.
                    self._record_upload_not_published(path.name, user_id, data)
                return STORED_SHARED
            except UploadNotRecorded as e:
                logger.warning("Uploaded image for %s not shared (record not written): %s",
                               fl_id, e)
            except OSError as e:
                if recorded:
                    self._record_upload_not_published(path.name, user_id, data)
                logger.warning("Uploaded image for %s not shared: %s", fl_id, e)
        try:
            path = self.get_browser_cache_path(browser_key, fl_id, size, threshold, processed, is_cul)
            if path is None:
                return STORED_NOWHERE
            _write_new_file(path, data)
            return STORED_BROWSER
        except OSError as e:
            logger.warning("Failed to store uploaded image for %s: %s", fl_id, e)
            return STORED_NOWHERE

    def _record_shared_upload(self, file_name: str, user_id: str, data: bytes) -> None:
        """Append one line to the shared-upload record (JSON lines) and flush it to disk.

        Raises ``UploadNotRecorded`` if the whole line cannot be written.
        """
        self._append_upload_record(file_name, user_id, data)

    def _record_upload_not_published(self, file_name: str, user_id: str, data: bytes) -> None:
        """Append the line that withdraws an upload's record: its file was not published.

        Written when the record line is on disk but the file then did not get
        its name (another copy got it first, or the link step failed). Best
        effort: a failure is logged, since nothing was shared either way.
        """
        try:
            self._append_upload_record(file_name, user_id, data, published=False)
        except UploadNotRecorded as e:
            logger.warning("Could not record that %s was not published: %s", file_name, e)

    def _append_upload_record(self, file_name: str, user_id: str, data: bytes,
                              published: Optional[bool] = None) -> None:
        """Write one JSON line to the upload record with one append and fsync it."""
        fields = {
            'file': file_name,
            'user_id': user_id,
            'uploaded_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'sha256': hashlib.sha256(data).hexdigest(),
        }
        if published is not None:
            fields['published'] = published
        line = json.dumps(fields, ensure_ascii=True, sort_keys=True)
        encoded = (line + '\n').encode('ascii')
        manifest = self._cache_dir / UPLOAD_MANIFEST_NAME
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, 'O_BINARY', 0)
        try:
            with _manifest_lock:
                fd = os.open(str(manifest), flags, 0o644)
                try:
                    written = os.write(fd, encoded)
                    if written != len(encoded):
                        raise OSError(f"short write ({written} of {len(encoded)} bytes)")
                    os.fsync(fd)
                finally:
                    os.close(fd)
        except OSError as e:
            raise UploadNotRecorded(f"{UPLOAD_MANIFEST_NAME}: {e}") from e

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
                                browser_key: Optional[str] = None,
                                web: bool = False) -> Optional[bytes]:
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
                       if there is no shared one. Implies ``web``.
            web: The web app's rules (see the module docstring). Without it
                       the desktop's behaviour is kept.

        Returns:
            Image bytes (RGBA PNG if processed, JPEG if original), or None on failure.
        """
        if not (web or browser_key):
            # The desktop's lookup: the fl_id names the file when there is one.
            cache_id = fl_id if fl_id else _safe_filename(image_url[:120])
            if not cache_id:
                return None
            cached = self.read_cached(cache_id, size, threshold, processed, is_cul)
            if cached is not None:
                return cached[0]
            return self._fetch_process_and_cache(fl_id, image_url, cache_id, size, threshold,
                                                 processed, is_cul, allow_direct=True)

        # The web's lookup: an image fetched from a direct URL is named after
        # that URL, an NLI image after its fl_id. A URL-fetched image is never
        # stored under an fl_id name.
        if image_url:
            if not is_allowed_image_url(image_url):
                logger.warning("Direct image URL on an unknown host refused: %s", image_url[:80])
                image_url = ''
                if not fl_id:
                    return None
        cache_id = _safe_filename(image_url[:120]) if image_url else fl_id
        if not cache_id:
            return None

        # Return cached if exists (shared first, then this browser's own)
        cached = self.read_cached(cache_id, size, threshold, processed, is_cul,
                                  browser_key=browser_key)
        if cached is not None:
            return cached[0]
        token = _web_rules.set(True)
        try:
            return self._fetch_process_and_cache(fl_id, image_url, cache_id, size, threshold,
                                                 processed, is_cul, allow_direct=False)
        finally:
            _web_rules.reset(token)

    def _fetch_process_and_cache(self, fl_id: str, image_url: str, cache_id: str,
                                 size: int, threshold: float, processed: bool,
                                 is_cul: bool, *, allow_direct: bool) -> Optional[bytes]:
        """Fetch, remove the background if asked, and cache under ``cache_id``."""
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
                _write_new_file(cache_path, raw_bytes, allow_direct=allow_direct)
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
            _write_new_file(cache_path, result_bytes, allow_direct=allow_direct)
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

        Under the web's rules only known library image hosts are fetched and
        each redirect hop is checked (``get_with_checked_redirects``); the
        desktop fetches as it always did.
        """
        if '/full/' in image_url:
            url = image_url  # Already a complete image URL
        else:
            url = f"{image_url}/full/{size},/0/default.jpg"
        if _web_rules.get() and not is_allowed_image_url(url):
            logger.warning(f"Direct IIIF fetch refused (unknown host) for {image_url[:80]}")
            return None

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
            resp = _image_get(url, headers=headers, timeout=timeout)
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
            # Under the web's rules each redirect hop is checked (_image_get).
            resp = _image_get(
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
        except RedirectNotFollowed as e:
            # Web only: a redirect off the allowed hosts. Not an NLI outage.
            logger.warning(f"IIIF redirect not followed for {fl_id}: {e}")

        return None


class BrowserImageView:
    """A PuzzleImageService seen by one browser (see ``for_browser``).

    ``resolve_fragment_image`` applies the web's rules and also finds that
    browser's own uploads; every other attribute is the underlying service's.
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
            image_url=image_url, browser_key=self._browser_key, web=True,
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
