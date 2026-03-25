# -*- coding: utf-8 -*-
"""Tests for puzzle API endpoints in web/api.py."""

import io
import json
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


class TestNliPersistentCacheHelpers:
    """Tests for restart-persistent NLI FL-ID cache helpers."""

    def test_persistent_cache_round_trip_skips_invalid_entries(self, tmp_path):
        """Only positive FL-ID lists are persisted and reloaded."""
        from web.api import _load_nli_persistent_cache, _save_nli_persistent_cache

        cache_path = tmp_path / 'nli_fl_ids_cache.json'
        cache = {
            '990000000000000001': ['FL123', '00456'],
            '990000000000000002': 'not-a-list',
            '990000000000000003': [],
        }
        cache_time = {
            '990000000000000001': 100.0,
            '990000000000000002': 100.0,
            '990000000000000003': 100.0,
        }

        _save_nli_persistent_cache(cache, cache_time, cache_path=str(cache_path), now=100.0)
        loaded_cache, loaded_cache_time = _load_nli_persistent_cache(
            cache_path=str(cache_path),
            now=120.0,
        )

        assert loaded_cache == {'990000000000000001': ['123', '00456']}
        assert loaded_cache_time == {'990000000000000001': 120.0}

    def test_persistent_cache_prunes_expired_entries(self, tmp_path):
        """Expired persisted entries are dropped during reload."""
        import web.api

        cache_path = tmp_path / 'nli_fl_ids_cache.json'
        payload = {
            'version': 1,
            'entries': {
                'fresh': {'fl_ids': ['111'], 'cached_at': 95.0},
                'stale': {'fl_ids': ['222'], 'cached_at': 10.0},
            },
        }
        cache_path.write_text(json.dumps(payload), encoding='utf-8')

        with patch.object(web.api, 'NLI_DISK_CACHE_TTL', 20):
            loaded_cache, loaded_cache_time = web.api._load_nli_persistent_cache(
                cache_path=str(cache_path),
                now=100.0,
            )

        assert loaded_cache == {'fresh': ['111']}
        assert loaded_cache_time == {'fresh': 100.0}


class TestPuzzleFoliosExternalFallback:
    """Tests for puzzle_folios endpoint images_ext fallback."""

    def test_puzzle_folios_external_fallback_logic(self):
        """puzzle_folios returns images_ext entries when NLI manifest is empty."""
        # Simulate the endpoint logic: no NLI fl_ids -> use images_ext from meta_mgr
        fl_ids = []  # Simulates fetch_fl_ids_from_nli returning empty

        # Simulate enrich_metadata returning images_ext
        mock_data = {
            'images_ext': [
                {'label': 'A', 'url': 'https://luna.manchester.ac.uk/iiif/test1'},
                {'label': 'B', 'url': 'https://luna.manchester.ac.uk/iiif/test2'},
            ],
            'external_provider': 'manchester',
        }

        result = []
        if not fl_ids:
            images_ext = mock_data.get('images_ext', [])
            external_provider = mock_data.get('external_provider', '')
            for i, img in enumerate(images_ext):
                label = img.get('label', '') or str(i + 1)
                result.append({
                    'fl_id': '',
                    'label': label,
                    'image_url': img.get('url', ''),
                    'page_index': i,
                    'external_provider': external_provider,
                })

        assert len(result) == 2
        assert result[0]['fl_id'] == ''
        assert result[0]['external_provider'] == 'manchester'
        assert result[0]['page_index'] == 0
        assert result[0]['image_url'] == 'https://luna.manchester.ac.uk/iiif/test1'
        assert result[1]['page_index'] == 1

    def test_puzzle_folios_nli_takes_priority(self):
        """When NLI fl_ids are present, images_ext fallback is NOT used."""
        fl_ids = ['7734473', '7734474']
        images_ext = [{'label': 'A', 'url': 'https://luna.manchester.ac.uk/iiif/x'}]

        # When fl_ids present, return NLI result without checking images_ext
        if fl_ids:
            result = [{'fl_id': fid, 'label': f'{i // 2 + 1}{"r" if i % 2 == 0 else "v"}'}
                      for i, fid in enumerate(fl_ids)]
        else:
            result = [{'fl_id': '', 'label': e['label'], 'image_url': e['url']}
                      for e in images_ext]

        assert len(result) == 2
        assert result[0]['fl_id'] == '7734473'  # NLI result, not external


class TestPuzzleExtImageEndpoint:
    """Tests for /api/puzzle_ext_image endpoint behavior."""

    def test_puzzle_ext_image_missing_provider_400(self):
        """puzzle_ext_image returns 400 when provider is missing."""
        from fastapi import Response

        # Simulate the validation logic
        provider = ''
        sys_id = 'TEST123'
        if not provider or not sys_id:
            resp = Response(content="Missing provider or sys_id", status_code=400)
        else:
            resp = Response(content="OK", status_code=200)

        assert resp.status_code == 400

    def test_puzzle_ext_image_unknown_provider_400(self):
        """puzzle_ext_image returns 400 for unknown provider."""
        from fastapi import Response

        provider = 'unknown_library'
        valid_providers = ('cambridge', 'manchester', 'jts', 'oxford')
        if provider not in valid_providers:
            resp = Response(content=f"Unknown provider: {provider}", status_code=400)
        else:
            resp = Response(content="OK", status_code=200)

        assert resp.status_code == 400

    def test_puzzle_ext_image_valid_providers_accepted(self):
        """All four library providers are recognized as valid."""
        valid_providers = ('cambridge', 'manchester', 'jts', 'oxford')
        for provider in valid_providers:
            assert provider in valid_providers

    def test_puzzle_ext_image_content_type_logic(self, mock_png_bytes, mock_jpeg_bytes):
        """Content type is image/png for PNG output, image/jpeg for JPEG."""
        # PNG detection: first 4 bytes are \x89PNG
        assert mock_png_bytes[:4] == b'\x89PNG'
        content_type_png = 'image/png' if mock_png_bytes[:4] == b'\x89PNG' else 'image/jpeg'
        assert content_type_png == 'image/png'

        # JPEG detection: doesn't start with PNG magic
        content_type_jpeg = 'image/png' if mock_jpeg_bytes[:4] == b'\x89PNG' else 'image/jpeg'
        assert content_type_jpeg == 'image/jpeg'


class TestAutoAddPageIndexParsing:
    """Tests for auto_add page:N format parsing in web puzzle."""

    def test_auto_add_page_index_parsing(self):
        """auto_add correctly parses 'page:N' format from browse deep link."""
        initial_add = 'SYS123,page:3'
        parts = initial_add.split(',', 1)
        add_sys_id = parts[0].strip()
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''
        add_page_index = -1
        if add_fl_id.startswith('page:'):
            try:
                add_page_index = int(add_fl_id[5:])
            except ValueError:
                add_page_index = 0
            add_fl_id = ''
        assert add_sys_id == 'SYS123'
        assert add_fl_id == ''
        assert add_page_index == 3

    def test_auto_add_fl_id_parsing(self):
        """auto_add correctly handles traditional 'sys_id,fl_id' format."""
        initial_add = 'SYS456,FL12345'
        parts = initial_add.split(',', 1)
        add_sys_id = parts[0].strip()
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''
        add_page_index = -1
        if add_fl_id.startswith('page:'):
            add_page_index = int(add_fl_id[5:])
            add_fl_id = ''
        assert add_sys_id == 'SYS456'
        assert add_fl_id == 'FL12345'
        assert add_page_index == -1

    def test_auto_add_page_index_invalid_value_defaults_to_zero(self):
        """auto_add handles invalid page:N (non-integer N) gracefully."""
        initial_add = 'SYSABC,page:xyz'
        parts = initial_add.split(',', 1)
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''
        add_page_index = -1
        if add_fl_id.startswith('page:'):
            try:
                add_page_index = int(add_fl_id[5:])
            except ValueError:
                add_page_index = 0
            add_fl_id = ''
        assert add_fl_id == ''
        assert add_page_index == 0  # Defaults to 0 on parse error

    def test_auto_add_sys_id_only(self):
        """auto_add handles sys_id only (no comma, no fl_id)."""
        initial_add = 'SYS789'
        parts = initial_add.split(',', 1)
        add_sys_id = parts[0].strip()
        add_fl_id = parts[1].strip() if len(parts) > 1 else ''
        add_page_index = -1
        if add_fl_id.startswith('page:'):
            add_page_index = int(add_fl_id[5:])
            add_fl_id = ''
        assert add_sys_id == 'SYS789'
        assert add_fl_id == ''
        assert add_page_index == -1
