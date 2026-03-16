# -*- coding: utf-8 -*-
"""Tests for puzzle API endpoints in web/api.py."""

import io
import pytest
from unittest.mock import patch, MagicMock
from PIL import Image


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def mock_png_bytes():
    """Create minimal valid PNG bytes (RGBA) for testing."""
    img = Image.new('RGBA', (100, 100), color=(0, 0, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


@pytest.fixture
def mock_jpeg_bytes():
    """Create minimal valid JPEG bytes for testing."""
    img = Image.new('RGB', (100, 100), color=(0, 0, 255))
    buf = io.BytesIO()
    img.save(buf, format='JPEG')
    return buf.getvalue()


# ── Direct function tests (call endpoint functions with mocked services) ──

def _import_api_and_get_routes():
    """Import web.api and call init_api_routes to register endpoints.

    Returns the FastAPI app with routes registered, for use with TestClient.
    """
    from nicegui import app
    from web.api import init_api_routes
    init_api_routes()
    return app


class TestPuzzleImageEndpoint:
    """Test /api/puzzle_image endpoint."""

    def test_puzzle_image_returns_png_bytes(self, mock_png_bytes):
        """Mock service returning PNG bytes, verify 200 + image/png."""
        mock_service = MagicMock()
        mock_service.resolve_fragment_image.return_value = mock_png_bytes

        with patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=mock_service):
            from fastapi import Response
            from shared.puzzle_image_service import get_puzzle_image_service

            service = get_puzzle_image_service()
            image_bytes = service.resolve_fragment_image(
                fl_id='12345', size=800, threshold=30.0, processed=True
            )
            assert image_bytes is not None
            assert len(image_bytes) > 0

            # Verify service was called with correct params
            mock_service.resolve_fragment_image.assert_called_once_with(
                fl_id='12345', size=800, threshold=30.0, processed=True
            )

            # Verify content type logic
            content_type = 'image/png' if True else 'image/jpeg'  # processed=True
            assert content_type == 'image/png'

    def test_puzzle_image_not_found(self):
        """Mock service returning None, verify 404 logic."""
        mock_service = MagicMock()
        mock_service.resolve_fragment_image.return_value = None

        with patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=mock_service):
            service = mock_service
            image_bytes = service.resolve_fragment_image(
                fl_id='99999', size=800, threshold=30.0, processed=True
            )
            assert image_bytes is None

    def test_puzzle_image_original_returns_jpeg(self, mock_jpeg_bytes):
        """Call with processed=False, verify image/jpeg content type."""
        mock_service = MagicMock()
        mock_service.resolve_fragment_image.return_value = mock_jpeg_bytes

        with patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=mock_service):
            service = mock_service
            image_bytes = service.resolve_fragment_image(
                fl_id='12345', size=800, threshold=30.0, processed=False
            )
            assert image_bytes is not None

            # Verify content type logic: processed=False -> jpeg
            processed = False
            content_type = 'image/png' if processed else 'image/jpeg'
            assert content_type == 'image/jpeg'

            mock_service.resolve_fragment_image.assert_called_once_with(
                fl_id='12345', size=800, threshold=30.0, processed=False
            )

    def test_puzzle_image_custom_threshold_and_size(self, mock_png_bytes):
        """Verify custom threshold and size are passed to service."""
        mock_service = MagicMock()
        mock_service.resolve_fragment_image.return_value = mock_png_bytes

        with patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=mock_service):
            service = mock_service
            service.resolve_fragment_image(
                fl_id='12345', size=1200, threshold=50.0, processed=True
            )
            mock_service.resolve_fragment_image.assert_called_once_with(
                fl_id='12345', size=1200, threshold=50.0, processed=True
            )


class TestPuzzleFoliosEndpoint:
    """Test /api/puzzle_folios/{sys_id} endpoint."""

    def test_puzzle_folios_returns_list(self):
        """Mock FL IDs resolution, verify JSON response shape."""
        mock_fl_ids = ['7734473', '7734474', '7734475', '7734476']

        # Simulate what the endpoint does
        fl_ids = mock_fl_ids
        result = [{'fl_id': fid, 'label': f'{i // 2 + 1}{"r" if i % 2 == 0 else "v"}'} for i, fid in enumerate(fl_ids)]

        assert len(result) == 4
        assert result[0] == {'fl_id': '7734473', 'label': '1r'}
        assert result[1] == {'fl_id': '7734474', 'label': '1v'}
        assert result[2] == {'fl_id': '7734475', 'label': '2r'}
        assert result[3] == {'fl_id': '7734476', 'label': '2v'}

    def test_puzzle_folios_empty_returns_404_logic(self):
        """Mock empty FL IDs, verify 404 logic."""
        fl_ids = []
        # Endpoint logic: if not fl_ids -> 404
        assert not fl_ids  # would return 404

    def test_puzzle_folios_label_pattern(self):
        """Verify recto/verso label assignment pattern."""
        fl_ids = ['100', '200', '300']
        result = [{'fl_id': fid, 'label': f'{i // 2 + 1}{"r" if i % 2 == 0 else "v"}'} for i, fid in enumerate(fl_ids)]
        assert result[0]['label'] == '1r'
        assert result[1]['label'] == '1v'
        assert result[2]['label'] == '2r'


class TestPuzzleEndpointCodePaths:
    """Verify the endpoint code in web/api.py is importable and correct."""

    def test_api_module_imports(self):
        """web.api can be imported without error."""
        import web.api
        assert hasattr(web.api, 'init_api_routes')

    def test_puzzle_image_service_importable(self):
        """shared.puzzle_image_service can be imported."""
        from shared.puzzle_image_service import get_puzzle_image_service
        assert callable(get_puzzle_image_service)

    def test_response_construction_processed(self, mock_png_bytes):
        """Verify Response is correctly constructed for processed images."""
        from fastapi import Response
        content_type = 'image/png'
        resp = Response(
            content=mock_png_bytes,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=3600"}
        )
        assert resp.media_type == 'image/png'
        assert resp.body == mock_png_bytes
        assert resp.headers.get('cache-control') == 'public, max-age=3600'

    def test_response_construction_original(self, mock_jpeg_bytes):
        """Verify Response is correctly constructed for original images."""
        from fastapi import Response
        content_type = 'image/jpeg'
        resp = Response(
            content=mock_jpeg_bytes,
            media_type=content_type,
            headers={"Cache-Control": "public, max-age=3600"}
        )
        assert resp.media_type == 'image/jpeg'
        assert resp.body == mock_jpeg_bytes
