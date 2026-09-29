# -*- coding: utf-8 -*-
"""Publishing a puzzle sends the signed-in user's token to Storage.

Storage allows an upload into ``<user id>/...`` of the ``puzzle-images`` bucket
only when the request carries that user's token (SUPABASE_GUIDE.md, storage
policies). ``get_user_client`` used to set the token only on the storage
session's default headers, but storage3 sends its own header set with every
request, which carries the anon key and overrides the session default. Every
web publish then failed with a row-level policy error from Storage, and every
unpublish left its images behind.

These tests build the client through the real ``get_user_client`` and run the
real ``publish_join`` / ``unpublish_join``; only the network is replaced, and
the assertion is on the Authorization header of each request that would have
left the process.
"""
from __future__ import annotations

import base64
import json
import os

import httpx
import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'publish-storage-auth-test-secret-0123456789')


def _b64(obj):
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b'=').decode()


ANON_KEY = f"{_b64({'alg': 'HS256', 'typ': 'JWT'})}.{_b64({'role': 'anon'})}.c2ln"
USER_TOKEN = f"{_b64({'alg': 'HS256', 'typ': 'JWT'})}.{_b64({'sub': 'user-1', 'exp': 4102444800})}.dXNy"
USER_ID = '00000000-0000-4000-8000-000000000001'


@pytest.fixture
def wire(monkeypatch):
    """A signed-in get_user_client() whose storage and PostgREST requests are recorded."""
    import web.supabase_client as sc
    import web.safe_storage as ss

    monkeypatch.setattr(sc, 'SUPABASE_URL', 'https://example.supabase.co')
    monkeypatch.setattr(sc, 'SUPABASE_ANON_KEY', ANON_KEY)
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: (
        {'access_token': USER_TOKEN, 'refresh_token': 'refresh-1'} if key == 'auth_session' else default))
    monkeypatch.setattr(ss, 'get_persisted_session_uuid', lambda: None)

    sent = []

    def handler(request):
        sent.append((request.method, request.url.path, request.headers.get('authorization')))
        if request.url.path.startswith('/storage/'):
            return httpx.Response(200, json={'Key': 'puzzle-images/x', 'Id': 'x'})
        return httpx.Response(201, json=[])

    client = sc.get_user_client()
    assert client is not sc.get_client(), 'expected the signed-in client, not the anonymous one'
    client.storage.session._transport = httpx.MockTransport(handler)
    client.postgrest.session._transport = httpx.MockTransport(handler)
    return client, sent


def _doc():
    from shared.puzzle_model import PuzzleDocument, PuzzleFragment
    return PuzzleDocument(
        id='doc-publish-1', title='A join', notes='',
        fragments=[PuzzleFragment(sys_id='990001', folio_label='1r', fl_id='FL1',
                                  shelfmark='T-S 1.1')],
    )


def _storage_requests(sent):
    return [s for s in sent if s[1].startswith('/storage/')]


def test_publish_uploads_with_the_users_token(wire, monkeypatch):
    from PIL import Image
    import shared.puzzle_publish_service as pps

    client, sent = wire
    monkeypatch.setattr(pps, 'compose_puzzle_export',
                        lambda fragments, image_service, export_size=None: Image.new('RGBA', (16, 16)))

    assert pps.publish_join(client, USER_ID, _doc(), image_service=None) == 'doc-publish-1'

    uploads = [s for s in _storage_requests(sent) if s[0] == 'POST']
    assert [path for _, path, _ in uploads] == [
        f'/storage/v1/object/puzzle-images/{USER_ID}/doc-publish-1.png',
        f'/storage/v1/object/puzzle-images/{USER_ID}/doc-publish-1_thumb.png',
    ]
    assert sent, 'no request reached the network layer -- the test would prove nothing'
    assert all(auth == f'Bearer {USER_TOKEN}' for _, _, auth in sent), sent


def test_unpublish_removes_images_with_the_users_token(wire):
    import shared.puzzle_publish_service as pps

    client, sent = wire
    pps.unpublish_join(client, USER_ID, 'doc-publish-1')

    removals = _storage_requests(sent)
    assert removals and all(method == 'DELETE' for method, _, _ in removals), removals
    assert all(auth == f'Bearer {USER_TOKEN}' for _, _, auth in sent), sent
