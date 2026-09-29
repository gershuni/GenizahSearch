# -*- coding: utf-8 -*-
"""Fragment Puzzle image-cache uploads: one token, one image, one time.

An upload token handed out with a cache miss from GET /api/puzzle_image is
accepted by POST /api/puzzle_process only for exactly the image it was issued
for (fl_id, size, threshold, processed, CUL flag), only once, and a cached
file is never replaced. Drives the real routes (``init_api_routes``) on a bare
FastAPI app with a session cookie, and a temp cache directory.
"""
from __future__ import annotations

import io
import uuid

import pytest
from fastapi import FastAPI
from PIL import Image
from starlette.middleware.sessions import SessionMiddleware
from starlette.testclient import TestClient

FL = '12345678'


def _jpeg(color=(200, 180, 150)) -> bytes:
    buf = io.BytesIO()
    Image.new('RGB', (64, 48), color=color).save(buf, format='JPEG')
    return buf.getvalue()


def _png(color=(10, 20, 30, 255)) -> bytes:
    buf = io.BytesIO()
    Image.new('RGBA', (64, 48), color=color).save(buf, format='PNG')
    return buf.getvalue()


class _SessionIdMiddleware:
    """Gives every browser a session id, as NiceGUI's own middleware does."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':
            session = scope.get('session')
            if session is not None and 'id' not in session:
                session['id'] = str(uuid.uuid4())
        await self.app(scope, receive, send)


@pytest.fixture
def puzzle_app(tmp_path, monkeypatch):
    import shared.puzzle_image_service as pis
    pis.reset_puzzle_image_service()
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    # The server cannot fetch NLI itself (as in production): every GET misses.
    monkeypatch.setattr(service, '_fetch_iiif_image', lambda fl_id, size: None)
    from web.api import init_api_routes
    app = FastAPI()
    init_api_routes(app_override=app)
    app.add_middleware(_SessionIdMiddleware)
    app.add_middleware(SessionMiddleware, secret_key='test-session-secret-' + 'x' * 24)
    yield app, service
    pis.reset_puzzle_image_service()


def _miss_token(client, *, fl_id=FL, size=800, threshold=30, processed=True, is_cul=False):
    resp = client.get('/api/puzzle_image', params={
        'fl_id': fl_id, 'size': size, 'threshold': threshold,
        'processed': str(processed).lower(), 'is_cul': str(is_cul).lower(),
    })
    assert resp.status_code == 404
    token = resp.headers.get('X-Puzzle-Upload-Token')
    assert token
    return token


def _upload(client, token, body, *, fl_id=FL, size=800, threshold=30, processed=True, is_cul=False):
    return client.post(
        '/api/puzzle_process',
        params={'fl_id': fl_id, 'size': size, 'threshold': threshold,
                'processed': str(processed).lower(), 'is_cul': str(is_cul).lower()},
        headers={'X-Puzzle-Upload-Token': token, 'Content-Type': 'image/jpeg'},
        content=body,
    )


@pytest.mark.parametrize('issued, used', [
    ({'fl_id': '87654321'}, {'fl_id': FL}),
    ({'threshold': 254}, {'threshold': 30}),
    ({'size': 2000}, {'size': 400}),
    ({'processed': True}, {'processed': False}),
    ({'is_cul': True}, {'is_cul': False}),
])
def test_a_token_is_accepted_only_for_the_image_it_was_issued_for(puzzle_app, issued, used):
    app, service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, **issued)

    resp = _upload(client, token, _jpeg(), **used)

    assert resp.status_code == 403
    params = {'size': 800, 'threshold': 30, 'processed': True, 'is_cul': False}
    params.update(used)
    assert not service.get_cache_path(FL, params['size'], float(params['threshold']),
                                      params['processed'], params['is_cul']).exists()


def test_a_matching_token_is_accepted_once(puzzle_app):
    app, _service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, processed=False)

    first = _upload(client, token, _jpeg(), processed=False)
    second = _upload(client, token, _jpeg((1, 2, 3)), processed=False)

    assert first.status_code == 200
    assert second.status_code == 403


def test_the_derivative_upload_route_is_gone_and_cached_bytes_stay(puzzle_app):
    app, service = puzzle_app
    client = TestClient(app)
    kept = _png((0, 128, 0, 255))
    path = service.get_cache_path(FL, 800, 30.0, True, False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(kept)
    token = _miss_token(client, threshold=254)  # same fragment, another image

    resp = client.post(
        '/api/puzzle_upload_derivative',
        params={'fl_id': FL, 'size': 800, 'threshold': 30, 'is_cul': 'false'},
        headers={'X-Puzzle-Upload-Token': token, 'Content-Type': 'image/png'},
        content=_png((255, 0, 0, 255)),
    )

    assert resp.status_code in (404, 405)
    assert path.read_bytes() == kept
    other = TestClient(app)
    got = other.get('/api/puzzle_image', params={'fl_id': FL, 'size': 800, 'threshold': 30})
    assert got.status_code == 200
    assert got.content == kept


def test_saving_a_derivative_never_replaces_a_cached_file(tmp_path):
    from shared.puzzle_image_service import PuzzleImageService
    service = PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    first, second = _png((1, 1, 1, 255)), _png((2, 2, 2, 255))

    assert service.save_derivative_to_cache(FL, 800, 30.0, False, first) is True
    assert service.save_derivative_to_cache(FL, 800, 30.0, False, second) is True

    assert service.get_cache_path(FL, 800, 30.0, True, False).read_bytes() == first


def test_a_token_is_refused_once_it_has_expired(puzzle_app, monkeypatch):
    import web.puzzle_tokens as tokens
    app, service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, processed=False)
    later = tokens.time.time() + tokens.TOKEN_TTL_SECONDS + 1
    monkeypatch.setattr(tokens.time, 'time', lambda: later)

    resp = _upload(client, token, _jpeg(), processed=False)

    assert resp.status_code == 403
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()


def test_a_token_for_an_in_between_size_is_accepted_for_the_same_request(puzzle_app):
    app, _service = puzzle_app
    client = TestClient(app)
    token = _miss_token(client, size=600, threshold=30.04, processed=False)

    resp = _upload(client, token, _jpeg(), size=600, threshold=30.04, processed=False)

    assert resp.status_code == 200


def test_a_failed_write_leaves_no_cached_file(tmp_path, monkeypatch):
    import io as _io
    from shared.puzzle_image_service import PuzzleImageService
    service = PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    path = service.get_cache_path(FL, 800, 30.0, True, False)
    real_open = _io.open

    class _HalfWriter:
        def __init__(self, fh):
            self._fh = fh

        def write(self, data):
            data = bytes(data)
            self._fh.write(data[:len(data) // 2])
            self._fh.flush()
            raise OSError('no space left on device')

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._fh.close()
            return False

    def _failing_open(file, mode='r', *args, **kwargs):
        fh = real_open(file, mode, *args, **kwargs)
        writing = 'b' in mode and any(m in mode for m in 'wxa')
        if writing and str(tmp_path) in str(file):
            return _HalfWriter(fh)
        return fh

    monkeypatch.setattr(_io, 'open', _failing_open)
    try:
        service.save_derivative_to_cache(FL, 800, 30.0, False, _png((3, 3, 3, 255)))
    except OSError:
        pass
    monkeypatch.setattr(_io, 'open', real_open)

    assert not path.exists()
    assert [p.name for p in path.parent.iterdir()] == []
    good = _png((4, 4, 4, 255))
    assert service.save_derivative_to_cache(FL, 800, 30.0, False, good) is True
    assert path.read_bytes() == good
