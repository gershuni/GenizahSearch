"""integrations/mcp/smoke_test.py's own behaviour (Codex, build 7-8 review).

A pending job is polled to the facade's deadline and reported as a timeout, never as
"no result"; details_pending is retried; a missing page number is reported, never
replaced by page 1.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location('mcp_smoke', ROOT / 'integrations' / 'mcp' / 'smoke_test.py')
smoke = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(smoke)


def result(payload, is_error=False):
    return SimpleNamespace(is_error=is_error, content=[SimpleNamespace(text=json.dumps(payload))])


class FakeClient:
    def __init__(self, replies):
        self.replies, self.calls = replies, []

    async def call_tool(self, name, arguments):
        self.calls.append(name)
        reply = self.replies[name]
        return reply(len(self.calls)) if callable(reply) else reply


def test_a_job_that_never_finishes_is_a_timeout_not_an_empty_result(monkeypatch):
    monkeypatch.setattr(smoke, 'JOB_DEADLINE', 0.05)
    pending = {'job_id': 'J' * 43, 'state': 'running', 'poll_after_seconds': 0.01}
    client, failures = FakeClient({'get_research_job': result(pending)}), []
    # wait_for: without its deadline the poll would loop forever; this test must then fail.
    assert asyncio.run(asyncio.wait_for(smoke.wait_for_job(client, failures, pending), timeout=5)) is None
    assert len(failures) == 1 and 'timed out, which is not the same as no results' in failures[0]


def test_a_job_that_finishes_returns_its_page():
    pending = {'job_id': 'J' * 43, 'state': 'queued', 'poll_after_seconds': 0}
    page = {'results': [{'uid': '1'}], 'pagination': {'page': 0}}
    client, failures = FakeClient({'get_research_job': result(page)}), []
    assert asyncio.run(smoke.wait_for_job(client, failures, pending)) == page and failures == []


def test_details_pending_is_retried_not_failed():
    async def go():
        client, failures = FakeClient({'get_manuscript_details': result(
            {'error': {'code': 'details_pending', 'retry_after_seconds': 0}}, is_error=True)}), []
        reply = await smoke.call(client, failures, 'get_manuscript_details', {}, retry_codes=('details_pending',))
        return reply, failures
    reply, failures = asyncio.run(go())
    assert reply == {'error': {'code': 'details_pending', 'retry_after_seconds': 0}} and failures == []


def test_a_result_without_a_page_number_is_reported_not_read_as_page_one(monkeypatch):
    page0 = {'results': [{'uid': '1', 'locator': {'sys_id': '990001', 'p_num': None}}]}
    replies = {'get_capabilities': result({}), 'search_manuscripts': result(page0)}
    tools = SimpleNamespace(tools=[SimpleNamespace(name=n) for n in smoke.EXPECTED_TOOLS])

    class Fake(FakeClient):
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def list_tools(self):
            return tools
    fake = Fake(replies)
    monkeypatch.setattr(smoke, 'Client', lambda url, mode: fake)
    failures = []
    asyncio.run(smoke.run('http://x', 'legacy', failures))
    assert 'browse_manuscript_page' not in fake.calls
    assert any('no usable locator' in failure for failure in failures)
