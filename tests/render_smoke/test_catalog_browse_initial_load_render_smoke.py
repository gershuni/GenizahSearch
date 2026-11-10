"""/catalog-browse loads its first page of results with no filter picked.

The first load runs in a task started with ``asyncio.ensure_future``. NiceGUI
keeps the current page per asyncio task, so that task starts with none, and
until 2026-10-20 its first ``ui.run_javascript`` raised "slot stack ... empty"
-- swallowed by a bare ``except Exception: pass`` -- leaving the page with no
results until a filter was picked (confirmed on the live site 2026-10-07).
"""

from __future__ import annotations

import asyncio
import os
import re
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
import web.main as web_main  # noqa: F401  (registers /catalog-browse)
import web.pages.catalog_browse as catalog_browse
from nicegui import core, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret
from tests.render_smoke._bounded_run import run_user_driver

_SYS_ID = '990000000000017'
_TITLE = 'Catalog Smoke Initial Load Title'


class _FakeFjms:
    def __init__(self):
        self.browse_calls = 0

    def is_available(self):
        return True

    def get_domain_hierarchy(self):
        return {}

    def get_unclassified_count(self):
        return 0

    def get_browse_authors(self, domain=None):
        return []

    def get_browse_works(self, domain=None, author=None):
        return []

    def get_browse_library_facets(self, *args, **kwargs):
        return {}

    def get_browse_results(self, *args, **kwargs):
        self.browse_calls += 1
        return {
            'results': [{'sys_id': _SYS_ID, 'author': 'Smoke Author', 'title': _TITLE}],
            'total': 1,
        }

    def get_printed_sys_ids(self, sys_ids):
        return set()

    def get_catalog_records(self, sys_id):
        return []


@asynccontextmanager
async def _catalog_user():
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('catalog-browse-render-smoke-secret', {})
        os.environ['NICEGUI_USER_SIMULATION'] = 'true'
        try:
            async with core.app.router.lifespan_context(core.app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app),
                    base_url='http://test',
                ) as client:
                    yield User(client)
        finally:
            os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver):
    saved_slots = list(nicegui_context.slot_stack)
    try:
        run_user_driver(_catalog_user, driver)
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _page_text(user) -> str:
    with user._client:
        parts = []
        for element in user._client.elements.values():
            if isinstance(element, ui.table):
                parts.extend(str(row) for row in element.rows)
            text = getattr(element, 'text', None)
            if isinstance(text, str):
                parts.append(text)
        return '\n'.join(parts)


def _assert_first_page_loads(answer_session_storage: bool, wait_s: float):
    fake = _FakeFjms()

    async def driver(user: User) -> None:
        if answer_session_storage:
            user.javascript_rules[re.compile(r".*sessionStorage\.getItem\('catalog_browse_filters'\)")] = (
                lambda _: None
            )
        await user.open('/catalog-browse')
        deadline = asyncio.get_running_loop().time() + wait_s
        while _TITLE not in _page_text(user) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert fake.browse_calls >= 1, 'the initial load never asked for results'
        assert _TITLE in _page_text(user), 'the first page of results was never drawn'

    with patch.object(catalog_browse, 'get_fjms_service', return_value=fake):
        _run(driver)


def test_first_page_of_results_loads_without_a_filter():
    _assert_first_page_loads(answer_session_storage=True, wait_s=5.0)


def test_first_page_loads_when_the_saved_filter_read_times_out():
    # The restore waits 2 s for sessionStorage; a browser that does not answer
    # gets the unfiltered first page, not an empty one.
    _assert_first_page_loads(answer_session_storage=False, wait_s=8.0)
