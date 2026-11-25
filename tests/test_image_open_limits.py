# -*- coding: utf-8 -*-
"""Images from outside are opened by Pillow's JPEG and PNG decoders only, and an
upload is refused by its declared size before Pillow allocates anything.

POST /api/puzzle_process takes an image from a signed-out visitor, so what
reaches Pillow there is a visitor's choice.
"""
from __future__ import annotations

import io
import struct
import zlib

import pytest
from PIL import Image
from starlette.testclient import TestClient

from shared import background_removal
from shared.background_removal import MAX_UPLOAD_PIXELS, open_jpeg_or_png, remove_background
from tests.test_puzzle_upload_token_binding import (  # noqa: F401 -- puzzle_app is a fixture
    _miss_token, _upload, puzzle_app,
)


def _encoded(fmt, size=(64, 48)):
    buf = io.BytesIO()
    Image.new('RGB', size, (200, 180, 150)).save(buf, format=fmt)
    return buf.getvalue()


def _chunk(kind, data):
    return struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data))


def _png_declaring(width, height):
    """A tiny PNG whose header declares ``width`` x ``height``."""
    return (b'\x89PNG\r\n\x1a\n'
            + _chunk(b'IHDR', struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0))
            + _chunk(b'IDAT', zlib.compress(b'\x00' * 10))
            + _chunk(b'IEND', b''))


def _jpeg_declaring(width, height):
    """A small JPEG whose frame header declares ``width`` x ``height``."""
    data = bytearray(_encoded('JPEG'))
    at = data.index(b'\xff\xc0')
    data[at + 5:at + 9] = struct.pack('>HH', height, width)
    return bytes(data)


@pytest.fixture
def pillow_opens(monkeypatch):
    """Every call Pillow's Image.open gets, as (args, kwargs)."""
    calls = []
    real = Image.open

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(background_removal.Image, 'open', spy)
    return calls


@pytest.mark.parametrize('fmt', ['JPEG', 'PNG'])
def test_jpeg_and_png_open(fmt):
    assert open_jpeg_or_png(_encoded(fmt), MAX_UPLOAD_PIXELS).size == (64, 48)


@pytest.mark.parametrize('fmt', ['GIF', 'TIFF', 'BMP', 'EPS', 'TGA', 'PPM'])
def test_no_other_format_opens(fmt):
    with pytest.raises(Exception):
        open_jpeg_or_png(_encoded(fmt))
    with pytest.raises(Exception):
        remove_background(_encoded(fmt))


def test_a_large_png_never_reaches_pillow(pillow_opens):
    with pytest.raises(ValueError, match='too large'):
        open_jpeg_or_png(_png_declaring(22000, 22000), MAX_UPLOAD_PIXELS)
    assert pillow_opens == []


def test_a_large_jpeg_is_refused():
    # 8000 x 7000: past the limit here, below the size Pillow itself warns about.
    with pytest.raises(ValueError, match='too large'):
        open_jpeg_or_png(_jpeg_declaring(8000, 7000), MAX_UPLOAD_PIXELS)


def test_a_png_that_hides_its_header_is_refused(pillow_opens):
    data = _png_declaring(22000, 22000)
    shifted = data[:8] + _chunk(b'tEXt', b'a\x00b') + data[8:]
    with pytest.raises(ValueError, match='IHDR'):
        open_jpeg_or_png(shifted, MAX_UPLOAD_PIXELS)
    assert pillow_opens == []


@pytest.mark.parametrize('body', [_png_declaring(22000, 22000), _jpeg_declaring(8000, 7000)],
                         ids=['png', 'jpeg'])
@pytest.mark.parametrize('processed', [True, False])
def test_the_upload_route_refuses_a_large_image(puzzle_app, body, processed):  # noqa: F811
    app, service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, processed=processed)

    resp = _upload(client, token, body, processed=processed)

    assert resp.status_code == 400
    assert not service.get_cache_path('12345678', 800, 30.0, processed, False).exists()


def test_the_upload_route_still_takes_a_long_scroll(puzzle_app):  # noqa: F811
    app, _service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, size=2000, processed=False)
    body = _encoded('PNG', (2000, 6000))

    resp = _upload(client, token, body, size=2000, processed=False)

    assert resp.status_code == 200
    assert resp.content == body
