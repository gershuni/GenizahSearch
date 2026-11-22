"""/api/oxford_image waits seconds, not 30, for the Bodleian (2026-11-22).

The Bodleian host has sat behind a bot-check page since 2026-09 (GS-255) that no server
fetch passes. The route's fetch had a 30 s timeout, and the website waited 31 s for it
before showing "Image not available" with the Bodleian link (measured on the live site).
"""
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web import api

SYS = '990053464220205171'
BODLEIAN = 'https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_21_21a.jpg'


class _Codico:
    _loaded = True
    part_metadata = {}

    def get_part_for_folio(self, sys_id):
        return 'MS. Heb. f. 21/1'

    def parse_part_identifier(self, shelfmark):
        return None, False

    def get_part_images(self, part_id):
        return [{'folio_num': 21, 'full_url': BODLEIAN, 'thumb_url': ''}]


@pytest.fixture
def client(monkeypatch):
    meta = SimpleNamespace(codico_mgr=_Codico(), get_meta_for_id=lambda sys_id: ('MS heb. f.21/21', ''))
    monkeypatch.setattr(api.state, 'meta_mgr', meta)
    app = FastAPI()
    api.init_api_routes(app_override=app)
    with TestClient(app, follow_redirects=False) as test_client:
        yield test_client


def test_the_bodleian_fetch_has_a_short_timeout(client, monkeypatch):
    seen = {}

    def fetch(url, **kwargs):
        seen['url'] = url
        seen['timeout'] = kwargs.get('timeout')
        return SimpleNamespace(status_code=200, headers={'Content-Type': 'text/html'}, content=b'<html>')

    monkeypatch.setattr('shared.puzzle_image_service.get_with_checked_redirects', fetch)
    response = client.get(f'/api/oxford_image/{SYS}?page=0')
    assert seen['url'] == BODLEIAN
    assert seen['timeout'] == api.OXFORD_IMAGE_TIMEOUT
    connect, read = api.OXFORD_IMAGE_TIMEOUT
    assert connect + read <= 10, 'the reader waits this long for the Bodleian link'
    # The bot-check page is not an image: the browser is sent to the library itself.
    assert response.status_code == 307
    assert response.headers['location'] == BODLEIAN


def test_a_timeout_still_sends_the_browser_to_the_library(client, monkeypatch):
    import requests

    def fetch(url, **kwargs):
        raise requests.exceptions.ReadTimeout('no answer')

    monkeypatch.setattr('shared.puzzle_image_service.get_with_checked_redirects', fetch)
    response = client.get(f'/api/oxford_image/{SYS}?page=0')
    assert response.status_code == 307
    assert response.headers['location'] == BODLEIAN
    assert response.headers['cache-control'] == 'no-store'
