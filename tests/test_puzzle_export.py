# -*- coding: utf-8 -*-
"""
Tests for shared/puzzle_export.py

Composite export and thumbnail generation. All image_service interactions
are mocked with small 50x50 RGBA PIL Images.
"""

import base64
import io
from unittest.mock import MagicMock

from PIL import Image

from shared.puzzle_model import PuzzleFragment
from shared.puzzle_export import (
    compose_puzzle_export,
    generate_thumbnail,
    add_metadata_banner,
    auto_suggest_title,
)


# ── Helpers ──────────────────────────────────────────────────────────

def _make_fragment(**overrides) -> PuzzleFragment:
    """Create a PuzzleFragment with sensible defaults and optional overrides."""
    defaults = dict(
        sys_id='sys_0',
        folio_label='1r',
        fl_id='FL_0',
        shelfmark='T-S 10.1',
        x=0.0,
        y=0.0,
        rotation=0.0,
        scale=1.0,
        flip_h=False,
        flip_v=False,
        crop_top=0,
        crop_bottom=0,
        crop_left=0,
        crop_right=0,
        processed=False,
    )
    defaults.update(overrides)
    return PuzzleFragment(**defaults)


def _png_bytes(width: int = 50, height: int = 50,
               color=(180, 120, 80, 255)) -> bytes:
    """Return a small RGBA PNG as bytes."""
    img = Image.new('RGBA', (width, height), color)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


def _make_image_service(return_bytes=None):
    """Mock image_service whose resolve_fragment_image returns given bytes."""
    svc = MagicMock()
    svc.resolve_fragment_image.return_value = (
        return_bytes if return_bytes is not None else _png_bytes()
    )
    return svc


def _make_null_image_service():
    """Mock image_service that always returns None (image load failure)."""
    svc = MagicMock()
    svc.resolve_fragment_image.return_value = None
    return svc


# ── Test 1: empty fragments list → returns None ─────────────────────

def test_compose_empty_fragments_returns_none():
    """compose_puzzle_export with an empty list returns None immediately."""
    result = compose_puzzle_export([], _make_image_service(), export_size=200)
    assert result is None


# ── Test 2: all images fail to load → returns None ───────────────────

def test_compose_all_images_none_returns_none():
    """When image_service returns None for every fragment, result is None."""
    frags = [_make_fragment(fl_id=f'FL_{i}') for i in range(3)]
    result = compose_puzzle_export(frags, _make_null_image_service(), export_size=200)
    assert result is None


# ── Test 3: valid fragments produce RGBA image with reasonable size ──

def test_compose_valid_fragments_returns_rgba():
    """Valid fragments produce an RGBA PIL Image with non-zero dimensions."""
    frags = [
        _make_fragment(fl_id='FL_0', x=0.0, y=0.0),
        _make_fragment(fl_id='FL_1', x=100.0, y=0.0, sys_id='sys_1',
                       shelfmark='T-S 10.2'),
    ]
    svc = _make_image_service()
    result = compose_puzzle_export(frags, svc, export_size=200)

    assert result is not None
    assert isinstance(result, Image.Image)
    assert result.mode == 'RGBA'
    assert result.width > 0
    assert result.height > 0


# ── Test 4: rotation does not crash ──────────────────────────────────

def test_compose_with_rotation():
    """A fragment with 90-degree rotation produces an image without errors."""
    frags = [_make_fragment(rotation=90.0)]
    result = compose_puzzle_export(frags, _make_image_service(), export_size=200)

    assert result is not None
    assert result.mode == 'RGBA'


# ── Test 5: flip_h does not crash ────────────────────────────────────

def test_compose_with_flip_h():
    """A fragment with flip_h=True produces an image without errors."""
    frags = [_make_fragment(flip_h=True)]
    result = compose_puzzle_export(frags, _make_image_service(), export_size=200)

    assert result is not None
    assert result.mode == 'RGBA'


# ── Test 6: non-unity scale produces output ──────────────────────────

def test_compose_with_scale():
    """A fragment with scale != 1.0 produces an image without errors."""
    frags = [_make_fragment(scale=0.5)]
    result = compose_puzzle_export(frags, _make_image_service(), export_size=200)

    assert result is not None
    assert result.mode == 'RGBA'
    assert result.width > 0
    assert result.height > 0


# ── Test 7: generate_thumbnail with valid image → non-empty b64 ─────

def test_generate_thumbnail_returns_base64():
    """generate_thumbnail with renderable fragments returns non-empty base64."""
    frags = [_make_fragment()]
    svc = _make_image_service()
    b64 = generate_thumbnail(frags, svc, thumb_size=80)

    assert isinstance(b64, str)
    assert len(b64) > 0
    # Verify it decodes to valid PNG bytes
    decoded = base64.b64decode(b64)
    img = Image.open(io.BytesIO(decoded))
    assert img.mode in ('RGBA', 'RGB')
    assert img.width <= 80
    assert img.height <= 80


# ── Test 8: generate_thumbnail with no renderable images → '' ────────

def test_generate_thumbnail_no_images_returns_empty():
    """generate_thumbnail returns empty string when no images can be loaded."""
    frags = [_make_fragment()]
    b64 = generate_thumbnail(frags, _make_null_image_service(), thumb_size=80)
    assert b64 == ''


# ── Test 9: metadata banner adds height below the composite ──────────

def test_metadata_banner_adds_height():
    """add_metadata_banner increases image height beyond the original composite."""
    frags = [
        _make_fragment(shelfmark='T-S 10.1'),
        _make_fragment(shelfmark='T-S 20.2', fl_id='FL_1', sys_id='sys_1'),
    ]
    svc = _make_image_service()
    export_size = 200
    composite = compose_puzzle_export(frags, svc, export_size=export_size)
    assert composite is not None

    with_banner = add_metadata_banner(composite, frags)
    assert with_banner.height > composite.height
    assert with_banner.width == composite.width
    assert with_banner.mode == 'RGBA'


# ── Test 10 (bonus): auto_suggest_title ──────────────────────────────

def test_auto_suggest_title_deduplicates():
    """auto_suggest_title joins unique shelfmarks with ' + '."""
    frags = [
        _make_fragment(shelfmark='T-S 10.1'),
        _make_fragment(shelfmark='T-S 10.1'),  # duplicate
        _make_fragment(shelfmark='T-S 20.2'),
    ]
    assert auto_suggest_title(frags) == 'T-S 10.1 + T-S 20.2'


def test_auto_suggest_title_no_shelfmarks():
    """auto_suggest_title returns 'Untitled Join' when no shelfmarks present."""
    frags = [_make_fragment(shelfmark=''), _make_fragment(shelfmark='')]
    assert auto_suggest_title(frags) == 'Untitled Join'


# ── Test 11: external fragment (image_url) export ────────────────────

def test_export_external_fragment_rendered():
    """compose_puzzle_export handles external fragments with image_url.

    Previously, external fragments (fl_id='') were silently skipped because
    resolve_fragment_image didn't accept image_url. After the fix, the mock
    receives image_url= keyword arg and returns bytes, so the fragment renders.
    """
    from shared.puzzle_model import PuzzleFragment

    ext_frag = PuzzleFragment(
        sys_id="M1", folio_label="A", fl_id="",
        shelfmark="Rylands 123",
        image_url="https://luna.manchester.ac.uk/iiif/test",
        external_provider="manchester", page_index=0,
        x=0, y=0, scale=1.0, processed=False
    )

    def mock_resolve(fl_id, size=800, threshold=30.0, processed=True, is_cul=False, image_url=''):
        # Ensure image_url is passed through correctly
        assert image_url == "https://luna.manchester.ac.uk/iiif/test"
        return _png_bytes()

    svc = MagicMock()
    svc.resolve_fragment_image.side_effect = mock_resolve

    result = compose_puzzle_export([ext_frag], svc, export_size=200)
    assert result is not None  # Previously would have been None (fragment skipped)


def test_export_external_fragment_image_url_passed_to_service():
    """resolve_fragment_image is called with the fragment's image_url."""
    from shared.puzzle_model import PuzzleFragment

    ext_frag = PuzzleFragment(
        sys_id="M2", folio_label="B", fl_id="",
        image_url="https://luna.manchester.ac.uk/iiif/canvas_xyz",
        external_provider="manchester", page_index=1,
        x=50, y=50, scale=1.0, processed=False
    )
    svc = _make_image_service(_png_bytes())
    compose_puzzle_export([ext_frag], svc, export_size=200)

    # Check the call was made with image_url keyword arg
    call_kwargs = svc.resolve_fragment_image.call_args
    assert call_kwargs is not None
    kwargs = call_kwargs[1] if call_kwargs[1] else {}
    args = call_kwargs[0] if call_kwargs[0] else ()
    # image_url should be in kwargs
    assert kwargs.get('image_url') == "https://luna.manchester.ac.uk/iiif/canvas_xyz"
