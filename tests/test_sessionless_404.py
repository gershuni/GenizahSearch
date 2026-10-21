# -*- coding: utf-8 -*-
"""A 404 on a session-free path must not leave a session-less NiceGUI client behind.

The session middleware skips public assets and session-free endpoints
(``web.framework_patches._skips_session``), so their requests carry no session.
When such a path matches no route (a stale ``/_nicegui/<old version>/...`` URL
after an upgrade, a missing ``/static`` file on 3.8), NiceGUI's 404 handler built
its HTML error page in a new ``Client``. That client lives for at least 60 s, and
while it does every ``prune_user_storage`` run raises on its first line
(``client.request.session`` asserts the middleware), so no visitor's user storage
is pruned and a traceback is logged every 10 s. Reproduced on NiceGUI 3.8.0 and
3.18.0 (2026-10-20 review).
"""

from __future__ import annotations

import asyncio
import uuid

from nicegui import app, core
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation
from starlette.exceptions import HTTPException
from starlette.requests import Request

import web.framework_patches as fp

_SESSION_FREE_MISS = '/_nicegui/3.8.0/static/no-such-file.js'


def _request(path: str, session: dict | None = None) -> Request:
    scope = {'type': 'http', 'method': 'GET', 'path': path, 'headers': [],
             'query_string': b'', 'app': app}
    if session is not None:
        scope['session'] = session
    return Request(scope)


def _handle(handler, request):
    """Run a 404 handler; return (response, clients it created)."""
    prepare_simulation()

    async def _main():
        core.loop = asyncio.get_running_loop()
        before = set(Client.instances)
        response = await handler(request, HTTPException(404))
        created = [Client.instances[i] for i in set(Client.instances) - before]
        for client in created:
            client.delete()
        return response, created

    return asyncio.run(_main())


def test_niceguis_own_handler_makes_a_sessionless_client():
    """The hazard this patch exists for (fails if NiceGUI ever stops doing it)."""
    handler = fp._nicegui_404_handler or app.exception_handlers[404]
    response, created = _handle(handler, _request(_SESSION_FREE_MISS))
    assert response.status_code == 404
    assert [c for c in created if 'session' not in c.request.scope]


def test_a_404_without_a_session_creates_no_client():
    fp._patch_sessionless_404()
    response, created = _handle(app.exception_handlers[404], _request(_SESSION_FREE_MISS))
    assert response.status_code == 404
    assert created == [], 'a session-less 404 still built a NiceGUI client'


def test_a_page_404_with_a_session_keeps_the_html_error_page():
    fp._patch_sessionless_404()
    request = _request('/no/such/page', session={'id': str(uuid.uuid4())})
    response, created = _handle(app.exception_handlers[404], request)
    assert response.status_code == 404
    assert 'text/html' in response.headers.get('content-type', '')
    assert len(created) == 1


def test_the_patch_is_idempotent():
    fp._patch_sessionless_404()
    first = app.exception_handlers[404]
    fp._patch_sessionless_404()
    assert app.exception_handlers[404] is first
    assert fp._nicegui_404_handler is not first
