"""The reply cap in front of the MCP SDK (web/mcp_server.py::_ReplyCap).

Codex fix verification (2026-10-09): JSON replies are held and capped, with their
representation headers rewritten; anything else is never held -- a streamed reply
passes through as it comes and is ended at the bound, and a non-body message first
releases the held start so ASGI ordering holds; a start without headers is fine.
"""
from __future__ import annotations

import asyncio
import json
import queue

import pytest

import web.api_hardening as hardening
import web.mcp_server as mcp_server
from web.chatgpt_pagination import MCP_RESPONSE_BYTES

SCOPE = {'type': 'http', 'method': 'POST', 'path': '/mcp', 'headers': [], 'client': ('203.0.113.5', 1)}
JSON = [(b'content-type', b'application/json')]


@pytest.fixture(autouse=True)
def quiet_events(monkeypatch):
    monkeypatch.setattr(hardening, '_event_queue', queue.Queue())
    monkeypatch.setattr(hardening, '_start_drain_thread_once', lambda: None)


def through_cap(messages, request_id=7):
    sent = []

    async def send(message):
        sent.append(message)

    async def go():
        cap = mcp_server._ReplyCap(send, SCOPE, request_id)
        for message in messages:
            await cap(message)
    asyncio.run(go())
    return sent


def start(headers=None, status=200):
    message = {'type': 'http.response.start', 'status': status}
    if headers is not None:
        message['headers'] = headers
    return message


def body(data, more=False):
    return {'type': 'http.response.body', 'body': data, 'more_body': more}


def test_a_small_json_reply_passes_unchanged():
    payload = b'{"jsonrpc":"2.0","id":7,"result":{}}'
    headers = JSON + [(b'content-length', str(len(payload)).encode()), (b'x-extra', b'1')]
    sent = through_cap([start(headers), body(payload[:10], more=True), body(payload[10:])])
    assert sent == [start(headers), body(payload)]


def test_an_oversized_json_reply_is_replaced_and_its_headers_rewritten():
    huge = b'{"x":"' + b'a' * MCP_RESPONSE_BYTES + b'"}'
    headers = JSON + [(b'content-encoding', b'gzip'), (b'transfer-encoding', b'chunked'),
                      (b'content-length', str(len(huge)).encode()), (b'x-extra', b'1')]
    first, second = through_cap([start(headers), body(huge)])
    kept = dict(first['headers'])
    assert b'content-encoding' not in kept and b'transfer-encoding' not in kept and kept[b'x-extra'] == b'1'
    assert kept[b'content-type'] == b'application/json' and int(kept[b'content-length']) == len(second['body'])
    reply = json.loads(second['body'])
    assert reply['id'] == 7 and 'mcp_response_too_large' in reply['error']['message']


def test_a_start_without_headers_is_handled():
    # No declared content type: not held as JSON, so passed through and cut at the bound.
    assert through_cap([start(), body(b'{}')]) == [start(), body(b'{}')]
    sent = through_cap([start(), body(b'"' + b'a' * MCP_RESPONSE_BYTES + b'"')])
    assert sent == [start(), body(b'')]


def test_a_streamed_reply_is_never_held_and_ends_at_the_bound():
    sent = []
    released = asyncio.Event()

    async def send(message):
        sent.append(message)
        if message.get('body') == b'event: one\n\n':
            released.set()

    async def go():
        cap = mcp_server._ReplyCap(send, SCOPE, 7)
        await cap(start([(b'content-type', b'text/event-stream')]))
        await cap(body(b'event: one\n\n', more=True))
        # Were the reply held, the client would not have the first event yet.
        await asyncio.wait_for(released.wait(), timeout=1)
        await cap(body(b'x' * MCP_RESPONSE_BYTES, more=True))
        await cap(body(b'event: after the end\n\n', more=True))
    asyncio.run(go())
    assert [m['type'] for m in sent] == ['http.response.start', 'http.response.body', 'http.response.body']
    assert sent[-1] == body(b'')
    assert sum(len(m.get('body', b'')) for m in sent) <= MCP_RESPONSE_BYTES


def test_a_non_body_message_releases_the_held_start_first():
    pathsend = {'type': 'http.response.pathsend', 'path': '/tmp/x'}
    sent = through_cap([start(JSON), pathsend])
    assert sent == [start(JSON), pathsend]
