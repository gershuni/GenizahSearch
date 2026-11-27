"""The manuscript-details route's own daily budget (owner decision D3, 2026-11-27).

The route already had a 30-a-minute limiter, so fast calls get 429 from that limiter alone.
These tests replace the minute limiter with a pass-through, so each 429 asserted here can only
come from the daily budget, and they check the daily budget's own code and message.
"""
import asyncio
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web import api_hardening
from web import chatgpt_details as details

ROWS = ([{'citation': 'Author, title'}], 'local_records')
PATH = '/chatgpt/manuscript-details'
PARAMS = {'sys_id': '99001', 'section': 'fjms_bibliography'}


def _app(minute_limiter=None, owner=lambda request: request.client.host):
    app = FastAPI()
    details.register_manuscript_details(
        app, owner, lambda *args: ROWS,
        minute_limiter or SimpleNamespace(check=lambda caller: None))
    return app


def test_default_is_2000_and_separate_from_the_search_budget():
    budget = details.make_details_daily_budget()
    assert budget._env_var == 'MANUSCRIPT_DETAILS_DAILY_LIMIT'
    assert budget._default_limit == 2000
    # The search budget's variable and message are untouched.
    search = api_hardening.DailyBudget()
    assert search._env_var == 'SEARCH_API_DAILY_LIMIT'
    assert 'daily_requests_per_address' in search._message


def test_the_limit_plus_one_call_gets_the_daily_error(monkeypatch):
    monkeypatch.setenv('MANUSCRIPT_DETAILS_DAILY_LIMIT', '3')
    monkeypatch.setenv('SEARCH_API_DAILY_LIMIT', '1')  # must not apply to this route
    with TestClient(_app()) as client:
        for _ in range(3):
            assert client.get(PATH, params=PARAMS).status_code == 200
        refused = client.get(PATH, params=PARAMS)
    assert refused.status_code == 429
    body = refused.json()['error']
    assert body['code'] == 'daily_limit_reached'
    assert 'genizahsearch.com/support' in body['message']
    assert 'manuscript-details' in body['message']
    assert 1 <= int(refused.headers['Retry-After']) <= api_hardening.DAILY_BUDGET_WINDOW_SECONDS


def test_admitted_again_after_the_window(monkeypatch):
    monkeypatch.setenv('MANUSCRIPT_DETAILS_DAILY_LIMIT', '2')
    clock = [1_000_000.0]
    monkeypatch.setattr(api_hardening.time, 'time', lambda: clock[0])
    with TestClient(_app()) as client:
        assert client.get(PATH, params=PARAMS).status_code == 200
        clock[0] += 3600
        assert client.get(PATH, params=PARAMS).status_code == 200
        clock[0] += 3600
        refused = client.get(PATH, params=PARAMS)
        assert refused.status_code == 429
        # Retry-After is when the oldest call leaves the window.
        assert int(refused.headers['Retry-After']) == 86400 - 7200
        clock[0] += 86400 - 7200
        assert client.get(PATH, params=PARAMS).status_code == 200


def test_a_call_the_minute_limiter_refuses_spends_no_daily_budget(monkeypatch):
    from shared.api_errors import APIError
    monkeypatch.setenv('MANUSCRIPT_DETAILS_DAILY_LIMIT', '1')
    refuse = [True]

    def check(caller):
        if refuse[0]:
            raise APIError('rate_limited', 'slow down', http_status=429, headers={'Retry-After': '1'})

    with TestClient(_app(minute_limiter=SimpleNamespace(check=check))) as client:
        assert client.get(PATH, params=PARAMS).json()['error']['code'] == 'rate_limited'
        refuse[0] = False
        assert client.get(PATH, params=PARAMS).status_code == 200


def test_mcp_calls_share_the_address_budget(monkeypatch):
    """MCP reaches this handler in-process as the visitor (web/mcp_server.py::_facade), so an
    address cannot get round the limit by switching to MCP."""
    from web.mcp_server import _facade
    monkeypatch.setenv('MANUSCRIPT_DETAILS_DAILY_LIMIT', '2')
    app = _app()

    async def run():
        first = await _facade(app, '203.0.113.7', 'GET', PATH, params=PARAMS)
        second = await _facade(app, '203.0.113.7', 'GET', PATH, params=PARAMS)
        other = await _facade(app, '198.51.100.9', 'GET', PATH, params=PARAMS)
        third = await _facade(app, '203.0.113.7', 'GET', PATH, params=PARAMS)
        return first, second, other, third

    first, second, other, third = asyncio.run(run())
    assert (first.status_code, second.status_code, other.status_code) == (200, 200, 200)
    assert third.status_code == 429
    assert third.json()['error']['code'] == 'daily_limit_reached'
