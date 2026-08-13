"""Session/caching boundary tests for public website assets."""
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from web.framework_patches import (
    _CacheSafeRequestTrackingMiddleware,
    _CacheSafeSessionMiddleware,
    _CONTENT_HASHED_ATLAS_RE,
    _is_public_cacheable_asset_path,
    _patch_static_asset_session_middleware,
)


def test_only_known_public_assets_are_cacheable():
    allowed = [
        '/favicon.ico',
        '/static/common.css',
        '/_nicegui/3.8.0/static/vue.esm-browser.prod.js',
        '/_nicegui/3.8.0/libraries/lib.js',
        '/_nicegui/3.8.0/components/hash/component.js',
        '/_nicegui/3.8.0/esm/key/module.js',
        '/atlas-data/atlas-v1-0123456789ab.bin',
    ]
    blocked = [
        '/', '/search', '/browse', '/api/search', '/auth/callback',
        '/_nicegui_ws/socket.io',
        '/_nicegui/3.8.0/dynamic_resources/user-content',
        '/_nicegui/3.8.0/resources/key/file',
        '/atlas-data/manifest.json',
        '/atlas-data/atlas-v1-not-a-hash.bin',
    ]
    assert all(_is_public_cacheable_asset_path(path) for path in allowed)
    assert not any(_is_public_cacheable_asset_path(path) for path in blocked)
    assert _CONTENT_HASHED_ATLAS_RE.fullmatch('/atlas-data/atlas-v1-0123456789ab.bin')


def test_static_asset_response_never_sets_or_reissues_session_cookie():
    async def app(scope, receive, send):
        # The cache-safe path must not even expose a session mapping downstream.
        assert 'session' not in scope
        await PlainTextResponse('asset')(scope, receive, send)

    stack = _CacheSafeSessionMiddleware(
        _CacheSafeRequestTrackingMiddleware(app),
        secret_key='test-secret',
    )
    client = TestClient(stack)
    response = client.get(
        '/static/common.css',
        headers={'Cookie': 'session=even-an-existing-cookie-must-not-be-reissued'},
    )
    assert response.status_code == 200
    assert 'set-cookie' not in response.headers


def test_dynamic_response_keeps_normal_session_cookie_behavior():
    async def app(scope, receive, send):
        scope['session']['id'] = 'per-user-session'
        await PlainTextResponse('page')(scope, receive, send)

    client = TestClient(_CacheSafeSessionMiddleware(app, secret_key='test-secret'))
    response = client.get('/search')
    assert response.status_code == 200
    assert response.headers['set-cookie'].startswith('session=')


def test_patch_replaces_only_nicegui_storage_middleware_references():
    import nicegui.storage as storage

    original_session = storage.SessionMiddleware
    original_tracking = storage.RequestTrackingMiddleware
    try:
        _patch_static_asset_session_middleware()

        assert storage.SessionMiddleware is _CacheSafeSessionMiddleware
        assert storage.RequestTrackingMiddleware is _CacheSafeRequestTrackingMiddleware
        # Starlette's class itself remains untouched; only NiceGUI's construction
        # references are replaced before ui.run installs them.
        from starlette.middleware.sessions import SessionMiddleware
        assert SessionMiddleware is not _CacheSafeSessionMiddleware
    finally:
        storage.SessionMiddleware = original_session
        storage.RequestTrackingMiddleware = original_tracking
