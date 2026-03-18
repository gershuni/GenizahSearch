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


class TestImageUrlParameter:
    """Tests for resolve_fragment_image with image_url parameter (non-NLI libraries)."""

    def test_resolve_fragment_image_with_image_url(self, tmp_path, monkeypatch):
        """resolve_fragment_image with image_url fetches from direct URL instead of NLI."""
        from shared.puzzle_image_service import PuzzleImageService, reset_puzzle_image_service
        reset_puzzle_image_service()
        svc = PuzzleImageService(cache_dir=tmp_path)

        # Mock requests.get to return fake image bytes
        class FakeResp:
            status_code = 200
            content = b'\x89PNG\r\n\x1a\n' + b'\x00' * 200
        def mock_get(url, headers=None, timeout=None):
            return FakeResp()
        monkeypatch.setattr('shared.puzzle_image_service.requests.get', mock_get)

        result = svc.resolve_fragment_image(
            fl_id='', size=800, threshold=30.0, processed=False,
            image_url='https://luna.manchester.ac.uk/luna/servlet/iiif/test123'
        )
        assert result is not None
        assert len(result) > 100

    def test_resolve_fragment_image_image_url_cached(self, tmp_path, monkeypatch):
        """image_url fetch result is cached to disk; second call uses cache."""
        from shared.puzzle_image_service import PuzzleImageService, reset_puzzle_image_service
        reset_puzzle_image_service()
        svc = PuzzleImageService(cache_dir=tmp_path)

        call_count = [0]
        class FakeResp:
            status_code = 200
            content = b'\xff\xd8\xff' + b'\x00' * 200
        def mock_get(url, headers=None, timeout=None):
            call_count[0] += 1
            return FakeResp()
        monkeypatch.setattr('shared.puzzle_image_service.requests.get', mock_get)

        image_url = 'https://luna.manchester.ac.uk/luna/servlet/iiif/test456'
        result1 = svc.resolve_fragment_image(fl_id='', size=800, processed=False, image_url=image_url)
        result2 = svc.resolve_fragment_image(fl_id='', size=800, processed=False, image_url=image_url)
        assert result1 == result2
        assert call_count[0] == 1  # Second call should use disk cache

    def test_resolve_fragment_image_no_fl_id_no_url_returns_none(self, tmp_path):
        """resolve_fragment_image with both fl_id and image_url empty returns None."""
        from shared.puzzle_image_service import PuzzleImageService, reset_puzzle_image_service
        reset_puzzle_image_service()
        svc = PuzzleImageService(cache_dir=tmp_path)
        result = svc.resolve_fragment_image(fl_id='', size=800, image_url='')
        assert result is None

    def test_fetch_direct_url_builds_iiif_url(self, tmp_path, monkeypatch):
        """_fetch_direct_url appends /full/{size},/0/default.jpg to canvas base URL."""
        from shared.puzzle_image_service import PuzzleImageService, reset_puzzle_image_service
        reset_puzzle_image_service()
        svc = PuzzleImageService(cache_dir=tmp_path)

        fetched_urls = []
        class FakeResp:
            status_code = 200
            content = b'\x89PNG\r\n\x1a\n' + b'\x00' * 200
        def mock_get(url, headers=None, timeout=None):
            fetched_urls.append(url)
            return FakeResp()
        monkeypatch.setattr('shared.puzzle_image_service.requests.get', mock_get)

        canvas_url = 'https://luna.manchester.ac.uk/luna/servlet/iiif/UoMimg~1~1~123'
        svc._fetch_direct_url(canvas_url, 800)
        assert len(fetched_urls) == 1
        assert fetched_urls[0] == f"{canvas_url}/full/800,/0/default.jpg"

    def test_fetch_direct_url_uses_full_url_as_is(self, tmp_path, monkeypatch):
        """_fetch_direct_url uses URL as-is when it already contains /full/."""
        from shared.puzzle_image_service import PuzzleImageService, reset_puzzle_image_service
        reset_puzzle_image_service()
        svc = PuzzleImageService(cache_dir=tmp_path)

        fetched_urls = []
        class FakeResp:
            status_code = 200
            content = b'\x89PNG\r\n\x1a\n' + b'\x00' * 200
        def mock_get(url, headers=None, timeout=None):
            fetched_urls.append(url)
            return FakeResp()
        monkeypatch.setattr('shared.puzzle_image_service.requests.get', mock_get)

        full_url = 'https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_21_21a.jpg'
        svc._fetch_direct_url(full_url, 800)
        assert fetched_urls[0] == full_url  # Used as-is
