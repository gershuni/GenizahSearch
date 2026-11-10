"""HTTP failures keep internal diagnostics out of response bodies."""
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from web import api


@pytest.fixture
def client():
    app = FastAPI()
    api.init_api_routes(app_override=app)
    with TestClient(app) as client:
        yield client


@pytest.mark.parametrize('message, expected', [
    ('private diagnostic from the serializer', 'Invalid export request'),
    ('No results to export', 'No results to export'),
    ('List is empty', 'List is empty'),
    ('No parallels results to export', 'No parallels results to export'),
    ('No browse data to export', 'No browse data to export'),
])
def test_json_export_uses_public_error_messages(client, monkeypatch, message, expected):
    monkeypatch.setattr('web.export_state.get_search_export',
                        lambda: {'results': [{'uid': 'example'}]})

    def fail(*args, **kwargs):
        raise ValueError(message)

    monkeypatch.setattr('shared.search_serializer.serialize_search_payload', fail)
    response = client.get('/api/export/json')
    assert response.status_code == 400
    assert response.text == expected


@pytest.mark.parametrize('provider', ['cambridge', 'manchester', 'jts'])
def test_image_fetch_error_is_not_returned(client, monkeypatch, provider):
    monkeypatch.setattr(api.state, 'meta_mgr', SimpleNamespace(nli_cache={}))
    monkeypatch.setattr('shared.cudl_identity.fresh_external_entry',
                        lambda *args: {'images_ext': [{'url': 'https://cudl.lib.cam.ac.uk/iiif/test'}]})
    monkeypatch.setattr('shared.cudl_identity.entry_decision_stale', lambda *args: False)
    monkeypatch.setattr('shared.nli_crossref_service.resolve_cambridge_canvas_for_page',
                        lambda *args, **kwargs: {'degraded': True})

    def fail(*args, **kwargs):
        raise RuntimeError('private diagnostic from the image source')

    monkeypatch.setattr('shared.puzzle_image_service.get_with_checked_redirects', fail)
    response = client.get(f'/api/{provider}_image/990012345670205171')
    assert response.status_code == 500
    assert 'private diagnostic' not in response.text
