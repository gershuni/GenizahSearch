# -*- coding: utf-8 -*-
"""
Shared Image Resolver/Cache for Fragment Puzzle.

Fetches IIIF fragment images, applies background removal, and caches
processed results to disk. Used by both web and desktop apps.

Cache key format: {fl_id}_{size}_{threshold}.png (processed) or {fl_id}_{size}_original.jpg (raw)
Cache location:
  - Windows installed: {LOCALAPPDATA}/GenizahSearchPro/cache/puzzle/
  - Development/other: {project_root}/cache/puzzle/
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

import requests

from shared.background_removal import remove_background, DEFAULT_THRESHOLD

logger = logging.getLogger(__name__)

# Processing algorithm version. Included in cache keys for processed images
# so that cache entries are automatically invalidated when the background
# removal algorithm changes.
PROCESSING_VERSION = 'v4'

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"

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


class PuzzleImageService:
    """Resolves fragment images: IIIF fetch -> background removal -> disk cache."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self._cache_dir = cache_dir or _get_default_cache_dir()
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_path(self, fl_id: str, size: int = 800,
                       threshold: float = DEFAULT_THRESHOLD,
                       processed: bool = True,
                       is_cul: bool = False) -> Path:
        """Deterministic cache path for a specific (fl_id, size, threshold) combination.
        Falls back to legacy (unversioned) path if it exists, for backward compat."""
        safe_id = _safe_filename(fl_id)
        if processed:
            suffix = '_cul' if is_cul else ''
            versioned = self._cache_dir / f"{safe_id}_{size}_{threshold:.1f}{suffix}_{PROCESSING_VERSION}.png"
            if versioned.exists():
                return versioned
            # Fall back to legacy path (no version suffix) if it exists
            legacy = self._cache_dir / f"{safe_id}_{size}_{threshold:.1f}{suffix}.png"
            if legacy.exists():
                return legacy
            # New files use versioned path
            return versioned
        else:
            return self._cache_dir / f"{safe_id}_{size}_original.jpg"

    def resolve_fragment_image(self, fl_id: str, size: int = 800,
                                threshold: float = DEFAULT_THRESHOLD,
                                processed: bool = True,
                                is_cul: bool = False,
                                image_url: str = '') -> Optional[bytes]:
        """Fetch IIIF image, apply background removal, cache result.

        Args:
            fl_id: NLI FL ID for the fragment image (empty for non-NLI)
            size: Image width in pixels (default 800)
            threshold: Background removal threshold (default 30.0)
            processed: If True, apply background removal. If False, return original.
            is_cul: If True, also remove CUL blue conservation mat.
            image_url: Direct IIIF canvas URL for non-NLI libraries. When non-empty,
                       fetched directly instead of constructing NLI URL from fl_id.

        Returns:
            Image bytes (RGBA PNG if processed, JPEG if original), or None on failure.
        """
        # Determine cache key — use fl_id for NLI, safe filename of URL for external
        cache_id = fl_id if fl_id else _safe_filename(image_url[:120])
        if not cache_id:
            return None

        cache_path = self.get_cache_path(cache_id, size, threshold, processed, is_cul)

        # Return cached if exists
        if cache_path.exists():
            try:
                return cache_path.read_bytes()
            except (FileNotFoundError, OSError):
                pass  # TOCTOU race or read error — treat as cache miss

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
                cache_path.write_bytes(raw_bytes)
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
            cache_path.write_bytes(result_bytes)
        except OSError as e:
            logger.warning(f"Failed to cache processed image for {cache_id}: {e}")
        return result_bytes

    def _fetch_direct_url(self, image_url: str, size: int) -> Optional[bytes]:
        """Fetch image from a direct IIIF canvas URL (non-NLI libraries).

        Constructs the full IIIF Image API URL if the given URL is a canvas base URL,
        or uses it directly if it already contains '/full/'.
        """
        if '/full/' in image_url:
            url = image_url  # Already a complete image URL
        else:
            url = f"{image_url}/full/{size},/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 100:
                logger.info(f"Direct IIIF fetch OK for {image_url[:80]}")
                return resp.content
            else:
                logger.warning(f"Direct IIIF fetch non-200 for {image_url[:80]}: status={resp.status_code}")
        except Exception as e:
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
            True if saved successfully, False otherwise.
        """
        if not png_bytes or png_bytes[:4] != b'\x89PNG':
            logger.warning(f"save_derivative_to_cache: invalid PNG header for {fl_id}")
            return False
        cache_path = self.get_cache_path(fl_id, size, threshold, True, is_cul)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(png_bytes)
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
        """
        digits = re.sub(r"\D", "", str(fl_id))
        if not digits:
            return None

        url = f"{NLI_IIIF_BASE}/FL{digits}/full/{size},/0/default.jpg"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 100:
                logger.info(f"IIIF fetch OK for {fl_id}")
                return resp.content
            else:
                logger.warning(f"IIIF fetch non-200 or empty for {fl_id}: "
                               f"status={resp.status_code}, size={len(resp.content)}")
        except Exception as e:
            logger.warning(f"IIIF fetch failed for {fl_id}: {e}")

        return None


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
