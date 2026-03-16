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
                       processed: bool = True) -> Path:
        """Deterministic cache path for a specific (fl_id, size, threshold) combination."""
        safe_id = _safe_filename(fl_id)
        if processed:
            # Include threshold in filename (rounded to 1 decimal to avoid float noise)
            return self._cache_dir / f"{safe_id}_{size}_{threshold:.1f}.png"
        else:
            return self._cache_dir / f"{safe_id}_{size}_original.jpg"

    def resolve_fragment_image(self, fl_id: str, size: int = 800,
                                threshold: float = DEFAULT_THRESHOLD,
                                processed: bool = True) -> Optional[bytes]:
        """Fetch IIIF image, apply background removal, cache result.

        Args:
            fl_id: NLI FL ID for the fragment image
            size: Image width in pixels (default 800)
            threshold: Background removal threshold (default 30.0)
            processed: If True, apply background removal. If False, return original.

        Returns:
            Image bytes (RGBA PNG if processed, JPEG if original), or None on failure.
        """
        cache_path = self.get_cache_path(fl_id, size, threshold, processed)

        # Return cached if exists
        if cache_path.exists():
            return cache_path.read_bytes()

        # Fetch from IIIF
        raw_bytes = self._fetch_iiif_image(fl_id, size)
        if raw_bytes is None:
            return None

        if not processed:
            # Cache and return original
            cache_path.write_bytes(raw_bytes)
            return raw_bytes

        # Apply background removal
        try:
            result_bytes = remove_background(raw_bytes, threshold=threshold)
        except Exception as e:
            logger.error(f"Background removal failed for {fl_id}: {e}")
            return raw_bytes  # fallback to original on error

        # Cache processed result
        cache_path.write_bytes(result_bytes)
        return result_bytes

    def invalidate_cache(self, fl_id: str, threshold: Optional[float] = None):
        """Clear cached images for a specific fl_id.

        Args:
            fl_id: The fragment to invalidate
            threshold: If provided, only clear entries for this threshold.
                       If None, clear all entries for this fl_id.
        """
        safe_id = _safe_filename(fl_id)
        if threshold is not None:
            # Remove specific threshold file(s)
            pattern = f"{safe_id}_*_{threshold:.1f}.png"
        else:
            # Remove all files for this fl_id (processed and original)
            pattern = f"{safe_id}_*"
        for f in self._cache_dir.glob(pattern):
            f.unlink(missing_ok=True)

    def _fetch_iiif_image(self, fl_id: str, size: int) -> Optional[bytes]:
        """Fetch image from NLI IIIF endpoint."""
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
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"IIIF fetch failed for {fl_id}: {e}")
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
                            processed: bool = True) -> Optional[bytes]:
    """Module-level convenience for resolve_fragment_image."""
    return get_puzzle_image_service().resolve_fragment_image(
        fl_id, size, threshold, processed
    )


def get_cache_path(fl_id: str, size: int = 800,
                   threshold: float = DEFAULT_THRESHOLD,
                   processed: bool = True) -> Path:
    """Module-level convenience for get_cache_path."""
    return get_puzzle_image_service().get_cache_path(fl_id, size, threshold, processed)


def invalidate_cache(fl_id: str, threshold: Optional[float] = None):
    """Module-level convenience for invalidate_cache."""
    get_puzzle_image_service().invalidate_cache(fl_id, threshold)
