# -*- coding: utf-8 -*-
"""Tests for shared.puzzle_image_service -- IIIF fetch, background removal, disk cache."""

import io
import pytest
from pathlib import Path
from PIL import Image

from shared.puzzle_image_service import PuzzleImageService, _safe_filename


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def mock_jpeg_bytes():
    """Create minimal valid JPEG bytes for testing."""
    img = Image.new('RGB', (100, 100), color=(0, 0, 255))  # blue background
    # Add a white square in center for foreground
    for x in range(40, 60):
        for y in range(40, 60):
            img.putpixel((x, y), (255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format='JPEG')
    return buf.getvalue()


@pytest.fixture
def service(tmp_path):
    return PuzzleImageService(cache_dir=tmp_path)


@pytest.fixture
def mock_fetch(monkeypatch, mock_jpeg_bytes):
    """Mock _fetch_iiif_image to return synthetic JPEG without network."""
    call_count = [0]

    def fake_fetch(self, fl_id, size):
        call_count[0] += 1
        return mock_jpeg_bytes

    monkeypatch.setattr(PuzzleImageService, '_fetch_iiif_image', fake_fetch)
    return call_count


# ── Cache path tests ──────────────────────────────────────────────

class TestGetCachePath:

    def test_get_cache_path_deterministic(self, service):
        """Same (fl_id, size, threshold) returns same Path. Different values return different."""
        p1 = service.get_cache_path("FL12345", 800, 30.0)
        p2 = service.get_cache_path("FL12345", 800, 30.0)
        assert p1 == p2

        # Different fl_id
        p3 = service.get_cache_path("FL99999", 800, 30.0)
        assert p3 != p1

        # Different size
        p4 = service.get_cache_path("FL12345", 1200, 30.0)
        assert p4 != p1

        # Different threshold
        p5 = service.get_cache_path("FL12345", 800, 50.0)
        assert p5 != p1

    def test_cache_path_includes_all_components(self, service):
        """Cache path filename contains fl_id, size, and threshold."""
        p = service.get_cache_path("FL12345", 800, 30.0)
        name = p.name
        assert "FL12345" in name
        assert "800" in name
        assert "30.0" in name

    def test_cache_path_platform_default(self, monkeypatch, tmp_path):
        """On Windows, default cache dir starts with LOCALAPPDATA."""
        fake_appdata = str(tmp_path / "AppData" / "Local")
        monkeypatch.setenv('LOCALAPPDATA', fake_appdata)
        from shared.puzzle_image_service import _get_default_cache_dir
        cache_dir = _get_default_cache_dir()
        assert str(cache_dir).startswith(fake_appdata)
        assert "GenizahSearchPro" in str(cache_dir)
        assert "puzzle" in str(cache_dir)


# ── Resolve tests ─────────────────────────────────────────────────

class TestResolveFragmentImage:

    def test_resolve_returns_processed_bytes(self, service, mock_fetch):
        """resolve_fragment_image returns RGBA PNG bytes (starts with PNG signature)."""
        result = service.resolve_fragment_image("FL12345", size=800)
        assert result is not None
        assert result[:4] == b'\x89PNG'
        # Verify it's a valid RGBA image
        img = Image.open(io.BytesIO(result))
        assert img.mode == 'RGBA'

    def test_resolve_caches_to_disk(self, service, mock_fetch):
        """After resolve, cache file exists. Second call doesn't re-fetch."""
        result1 = service.resolve_fragment_image("FL12345", size=800)
        cache_path = service.get_cache_path("FL12345", 800, 30.0)
        assert cache_path.exists()
        assert mock_fetch[0] == 1

        result2 = service.resolve_fragment_image("FL12345", size=800)
        assert result2 == result1
        assert mock_fetch[0] == 1  # NOT called again

    def test_resolve_different_threshold_different_cache(self, service, mock_fetch):
        """resolve with threshold=30 then threshold=50 produces two distinct cache files."""
        service.resolve_fragment_image("FL12345", size=800, threshold=30.0)
        service.resolve_fragment_image("FL12345", size=800, threshold=50.0)

        p1 = service.get_cache_path("FL12345", 800, 30.0)
        p2 = service.get_cache_path("FL12345", 800, 50.0)
        assert p1.exists()
        assert p2.exists()
        assert p1 != p2
        assert mock_fetch[0] == 2  # fetched twice (different threshold = different entry)

    def test_resolve_different_size_different_cache(self, service, mock_fetch):
        """resolve with size=800 then size=1200 produces two distinct cache files."""
        service.resolve_fragment_image("FL12345", size=800)
        service.resolve_fragment_image("FL12345", size=1200)

        p1 = service.get_cache_path("FL12345", 800, 30.0)
        p2 = service.get_cache_path("FL12345", 1200, 30.0)
        assert p1.exists()
        assert p2.exists()
        assert p1 != p2
        assert mock_fetch[0] == 2

    def test_resolve_original_mode(self, service, mock_fetch, mock_jpeg_bytes):
        """resolve_fragment_image(processed=False) returns raw JPEG bytes without bg removal."""
        result = service.resolve_fragment_image("FL12345", size=800, processed=False)
        assert result is not None
        # JPEG starts with FF D8
        assert result[:2] == b'\xff\xd8'
        # Should be the same bytes as the mock (no processing)
        assert result == mock_jpeg_bytes


# ── Invalidation tests ────────────────────────────────────────────

class TestInvalidateCache:

    def test_invalidate_cache_specific_threshold(self, service, mock_fetch):
        """invalidate_cache(fl_id, threshold=30) removes only threshold=30 file."""
        service.resolve_fragment_image("FL12345", size=800, threshold=30.0)
        service.resolve_fragment_image("FL12345", size=800, threshold=50.0)

        p30 = service.get_cache_path("FL12345", 800, 30.0)
        p50 = service.get_cache_path("FL12345", 800, 50.0)
        assert p30.exists()
        assert p50.exists()

        service.invalidate_cache("FL12345", threshold=30.0)
        assert not p30.exists()
        assert p50.exists()  # still there

    def test_invalidate_cache_all_thresholds(self, service, mock_fetch):
        """invalidate_cache(fl_id) with no threshold removes ALL cached files for that fl_id."""
        service.resolve_fragment_image("FL12345", size=800, threshold=30.0)
        service.resolve_fragment_image("FL12345", size=800, threshold=50.0)
        service.resolve_fragment_image("FL12345", size=1200, threshold=30.0)
        service.resolve_fragment_image("FL12345", size=800, processed=False)

        service.invalidate_cache("FL12345")

        p1 = service.get_cache_path("FL12345", 800, 30.0)
        p2 = service.get_cache_path("FL12345", 800, 50.0)
        p3 = service.get_cache_path("FL12345", 1200, 30.0)
        p4 = service.get_cache_path("FL12345", 800, processed=False)
        assert not p1.exists()
        assert not p2.exists()
        assert not p3.exists()
        assert not p4.exists()
