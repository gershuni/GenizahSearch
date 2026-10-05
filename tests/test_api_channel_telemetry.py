"""search_api_request says which surface a call came through.

GPT Actions reuse the public API's handlers, so without a `channel` property
GPT traffic and direct API traffic are the same event. These tests drive the
production wiring (init_search_api on the /api sub-app, the GPT facade on top,
both job runners) and read the real event off the capture queue.
"""
import time
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api_hardening import API_CHANNEL_SCOPE_KEY, api_channel
from web.chatgpt_api import register_chatgpt_api
from web.search_api import init_search_api

EMPTY_QUERY = {'query': '   ', 'search_mode': 'exact'}  # 400 query_required, no search runs


@pytest.fixture
def events(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '1000')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '1')
    from web.state import state
    monkeypatch.setattr(state, 'searcher', MagicMock())
    monkeypatch.setattr(state, 'meta_mgr', MagicMock())
    captured = []

    class FakeQueue:
        def put_nowait(self, item):
            captured.append(item)

    monkeypatch.setattr('web.api_hardening._event_queue', FakeQueue())
    monkeypatch.setattr('web.api_hardening._start_drain_thread_once', lambda: None)
    return captured


@pytest.fixture
def client(events):
    # Same shape as web/main.py: routes at '' on a sub-app mounted at /api.
    child = FastAPI()
    init_search_api(app_override=child, path_prefix='')
    register_chatgpt_api(child)
    parent = FastAPI()
    parent.mount('/api', child)
    with TestClient(parent) as c:
        yield c


def _only_event(events):
    assert len(events) == 1, events
    event = events.pop()
    assert event['event'] == 'search_api_request'
    return event['properties']


def _wait(get, done):
    for _ in range(200):
        response = get()
        if done(response):
            return response
        time.sleep(0.02)
    raise AssertionError('background job never finished')


@pytest.mark.parametrize('scope,expected', [
    ({}, 'api'),
    ({'research_job': object()}, 'api_job'),
    ({API_CHANNEL_SCOPE_KEY: 'chatgpt'}, 'chatgpt'),
    ({API_CHANNEL_SCOPE_KEY: 'chatgpt', 'research_job': object()}, 'chatgpt_job'),
    ({API_CHANNEL_SCOPE_KEY: 'something-else'}, 'api'),
])
def test_channel_values(scope, expected):
    assert api_channel(MagicMock(scope=scope)) == expected


@pytest.mark.parametrize('path,channel', [('/api/search', 'api'), ('/api/chatgpt/search', 'chatgpt')])
def test_search_endpoint_reports_channel(client, events, path, channel):
    assert client.post(path, json=EMPTY_QUERY).status_code == 400
    props = _only_event(events)
    assert (props['endpoint'], props['error_code'], props['channel']) == ('search', 'query_required', channel)


@pytest.mark.parametrize('path,channel', [('/api/browse', 'api'), ('/api/chatgpt/browse', 'chatgpt')])
def test_wrapped_endpoint_reports_channel(client, events, path, channel):
    assert client.get(path).status_code == 400  # no sys_id
    props = _only_event(events)
    assert (props['endpoint'], props['channel']) == ('browse', channel)


def test_api_background_job_reports_api_job(client, events):
    submitted = client.post('/api/search/jobs', json=EMPTY_QUERY)
    assert submitted.status_code == 202, submitted.text
    url = '/api/research/jobs/' + submitted.json()['job_id']
    _wait(lambda: client.get(url), lambda r: r.json()['state'] not in ('queued', 'running'))
    props = _only_event(events)
    assert (props['endpoint'], props['error_code'], props['channel']) == ('search', 'query_required', 'api_job')


def test_gpt_background_job_reports_chatgpt_job(client, events):
    submitted = client.post('/api/chatgpt/search/jobs', json=EMPTY_QUERY)
    assert submitted.status_code == 202, submitted.text
    url = '/api/chatgpt/jobs/' + submitted.json()['job_id']
    _wait(lambda: client.get(url), lambda r: r.status_code != 202)
    props = _only_event(events)
    assert (props['endpoint'], props['error_code'], props['channel']) == ('search', 'query_required', 'chatgpt_job')


def test_gpt_marking_does_not_leak_into_later_direct_calls(client, events):
    client.post('/api/chatgpt/search', json=EMPTY_QUERY)
    client.post('/api/search', json=EMPTY_QUERY)
    assert [e['properties']['channel'] for e in events] == ['chatgpt', 'api']
