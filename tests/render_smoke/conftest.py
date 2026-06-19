# -*- coding: utf-8 -*-
"""Render-smoke conftest for /joins-lab NiceGUI User simulation.

TASK 1 RESOLUTION: "manual" (no pytest-asyncio installed — gate decision 2026-06-19).
Tests in this package are SYNCHRONOUS functions.  Each test drives a NiceGUI
``User`` via ``asyncio.run(driver_coroutine)``, where the driver coroutine uses
``await user.open()``, ``user.find(...).click()``, and ``await user.should_see()``.

This conftest declares ``pytest_plugins = ['nicegui.testing.user_plugin']`` so the
NiceGUI test-time marker infrastructure is available (even though we bypass the
``user`` fixture itself — the plugin registers the ``nicegui_main_file`` marker).

F-A1 (route-root): ``import web.main`` at module load time registers the
``@ui.page('/joins-lab')`` decorator on ``core.app`` — so the route is present
in ``core.app.routes`` before we enter the lifespan.  We do NOT call
``nicegui_reset_globals()`` (it removes non-/_nicegui routes, including /joins-lab).

F-A2 (startup mock): ``web.main`` registers ``initialize_engine`` and
``compact_export_storage_on_startup`` as on_startup handlers in
``core.app._startup_handlers``.  Before entering the lifespan we clear that list
and replace both with trivial async no-ops so the real SearchEngine/MetadataManager
are never constructed.

Usage pattern in tests::

    def test_foo(joins_lab_user_runner):
        def driver(user):
            async def _run():
                await user.open('/joins-lab')
                await user.should_see(...)
                ...
            return _run()
        joins_lab_user_runner(driver)
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Callable
from unittest.mock import AsyncMock, patch

import httpx
import pytest

# NOTE: We do NOT declare pytest_plugins = ['nicegui.testing.user_plugin'] here.
# Declaring pytest_plugins in a non-top-level conftest is disallowed as of pytest 8+.
# The NiceGUI user plugin's marker/addoption hooks are not needed because we
# bypass the official 'user' fixture entirely (Task 1 "manual" decision) and drive
# the simulation via our own run_joins_lab_smoke() helper using asyncio.run().


# ---------------------------------------------------------------------------
# Import web.main at module load — registers /joins-lab on core.app (F-A1).
# This must happen exactly ONCE, before any nicegui_reset_globals() call.
# ---------------------------------------------------------------------------
import web.main as _web_main  # noqa: E402, F401  (side-effect import: registers /joins-lab route)
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret
from web.state import state as _web_state


# ---------------------------------------------------------------------------
# Deterministic stub search result dicts
# ---------------------------------------------------------------------------
# NOTE: execute_search returns RAW SEARCH RESULT DICTS (not Candidate objects).
# Candidate objects are built later by candidate_from_result() in shared/joins_lab.py.
# The dict format must match what _r_sid / _r_shelf / candidate_from_result expect.
# ---------------------------------------------------------------------------
from shared.joins_lab import Candidate

STUB_ANCHOR_SID = '990001111111111'
STUB_ANCHOR_SHELFMARK = 'T-S 12.001'

# Raw search result dict for Candidate 1: text-hit with highlight (G1)
STUB_RAW_TEXT = {
    'uid': '990002222222222|p3',
    'sys_id': '990002222222222',
    'display': {
        'id': '990002222222222',
        'shelfmark': 'T-S 12.123',
        'title': 'Test Fragment',
        'library_code': 'CUL',
    },
    'full_text': 'This is a test text with highlighted word here in Hebrew context',
    'snippet': 'test text with highlighted word here',
    'highlight_pattern': 'highlighted',
    'score': 0.9,
    '_via_text': True,
    '_via_vs': False,
}

# Raw search result dict for Candidate 2: text-hit + VS match (via_vs=True)
STUB_RAW_VS = {
    'uid': '990003333333333|vs',
    'sys_id': '990003333333333',
    'display': {
        'id': '990003333333333',
        'shelfmark': 'T-S 13.001',
        'title': 'VS Fragment',
        'library_code': 'CUL',
    },
    'full_text': 'Visual similarity candidate with matching word highlighted',
    'snippet': 'Visual similarity candidate',
    'highlight_pattern': 'highlighted',
    'score': 0.7,
    '_via_text': True,
    '_via_vs': True,
    'vs_rank': 1,
    'svm_score': 0.85,
}

# Convenience Candidate objects for test assertions (G1 highlight check etc.)
# Built from the raw dicts so tests can compare rendered output.
STUB_CAND_TEXT = Candidate(
    sys_id='990002222222222',
    page=3,
    uid='990002222222222|p3',
    shelfmark='T-S 12.123',
    title='Test Fragment',
    library_code='CUL',
    full_text='This is a test text with highlighted word here in Hebrew context',
    snippet='test text with highlighted word here',
    highlight_pattern='highlighted',
    score=0.9,
    via_text=True,
    via_vs=False,
)

STUB_CAND_VS = Candidate(
    sys_id='990003333333333',
    page=1,
    uid='990003333333333|vs',
    shelfmark='T-S 13.001',
    title='VS Fragment',
    library_code='CUL',
    full_text='Visual similarity candidate with matching word highlighted',
    snippet='Visual similarity candidate',
    highlight_pattern='highlighted',
    score=0.7,
    via_text=True,
    via_vs=True,
    vs_rank=1,
    vs_score=0.85,
)

# VS-only candidates (pure look-alike, no text hit)
STUB_VS_ONLY_CANDIDATES = [
    {'alma_id': '990004444444444', 'rank': 1, 'svm_score': 0.92},
]

# Stub browse page returned by mocked AnchorViewer resolver
STUB_BROWSE_PAGE = {
    'sys_id': STUB_ANCHOR_SID,
    'folios': [{'p_num': 1, 'fl_id': f'{STUB_ANCHOR_SHELFMARK}.1r'}],
    'text': 'Anchor fragment text',
    'p_num': 1,
    'shelfmark': STUB_ANCHOR_SHELFMARK,
    'title': 'Anchor Title',
    'library_code': 'CUL',
}


# ---------------------------------------------------------------------------
# async context manager: full joins-lab user driver (one call per test)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _joins_lab_user_context(
    mock_search_results: list | None = None,
    mock_vs_raw: list | None = None,
):
    """Async context manager that yields a ready NiceGUI User for /joins-lab.

    F-A1: /joins-lab is registered by the web.main import above.
    F-A2: core.app._startup_handlers is cleared before entering lifespan so
          initialize_engine and compact_export_storage_on_startup never run.

    Patches applied before the lifespan:
      - core.app._startup_handlers cleared (no real engine init)
      - WebSearchExecutor.execute_search → returns mock_search_results (raw dicts)
      - WebSearchExecutor.get_meta_for_id → returns (shelfmark, {})
      - WebSearchExecutor.get_browse_page → returns STUB_BROWSE_PAGE
      - AnchorViewer.update_content → async no-op (no network)
      - fetch_connected_fragments → returns dict with empty joins list (no known joins)
      - _enrich_candidates → returns {} (no enrichment)
      - _do_vs_fetch_and_update → async no-op initially (VS is off by default)
      - VisualSimilarityService.get_suggestions → returns mock_vs_raw
      - state.is_ready() → returns True (engine guard bypass — F-A2b)
    """
    if mock_search_results is None:
        # Raw dicts — execute_search returns dicts, not Candidate objects
        mock_search_results = [STUB_RAW_TEXT, STUB_RAW_VS]
    if mock_vs_raw is None:
        mock_vs_raw = STUB_VS_ONLY_CANDIDATES

    # F-A2: clear on_startup handlers so the real engine is never built
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    # F-A2b: mock state.is_ready() so execute_joins_search passes the engine guard
    # (state.searcher and state.meta_mgr are None because initialize_engine is cleared)
    saved_is_ready = _web_state.is_ready
    _web_state.is_ready = lambda: True

    try:
        prepare_simulation()
        set_storage_secret('render-smoke-test-secret', {})

        with patch(
            'web.pages.joins_lab.WebSearchExecutor.execute_search',
            return_value=mock_search_results,
        ):
            with patch(
                'web.pages.joins_lab.WebSearchExecutor.get_meta_for_id',
                return_value=(STUB_ANCHOR_SHELFMARK, {}),
            ):
                with patch(
                    'web.pages.joins_lab.WebSearchExecutor.get_browse_page',
                    return_value=STUB_BROWSE_PAGE,
                ):
                    with patch(
                        'web.components.anchor_viewer.AnchorViewer.update_content',
                        new_callable=AsyncMock,
                    ):
                        with patch(
                            'web.pages.joins_lab.fetch_connected_fragments',
                            return_value={'joins': [], 'fragment_details': [], 'total_joins': 0},
                        ):
                            with patch(
                                'web.pages.joins_lab._enrich_candidates',
                                new_callable=AsyncMock,
                                return_value={},
                            ):
                                with patch(
                                    'shared.visual_similarity_service.'
                                    'VisualSimilarityService.get_suggestions',
                                    return_value=mock_vs_raw,
                                ):
                                    with patch(
                                        'shared.visual_similarity_service.'
                                        'VisualSimilarityService.is_available',
                                        return_value=True,
                                    ):
                                        os.environ['NICEGUI_USER_SIMULATION'] = 'true'
                                        try:
                                            async with core.app.router.lifespan_context(
                                                core.app
                                            ):
                                                async with httpx.AsyncClient(
                                                    transport=httpx.ASGITransport(core.app),
                                                    base_url='http://test',
                                                ) as client:
                                                    user = User(client)
                                                    yield user
                                        finally:
                                            os.environ.pop(
                                                'NICEGUI_USER_SIMULATION', None
                                            )
    finally:
        # Restore original startup handlers and state for subsequent tests
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _web_state.is_ready = saved_is_ready


def run_joins_lab_smoke(
    driver: Callable[[User], 'asyncio.Future | asyncio.coroutine'],
    mock_search_results: list | None = None,
    mock_vs_raw: list | None = None,
) -> None:
    """Synchronous helper: run an async driver coroutine against the /joins-lab page.

    Usage::

        def test_something():
            async def driver(user: User) -> None:
                await user.open('/joins-lab')
                await user.should_see(...)

            run_joins_lab_smoke(driver)

    The driver receives a NiceGUI User with all heavy seams (engine init, search,
    AnchorViewer image fetch, enrichment, VS service) mocked.  It must return a
    coroutine (async def).

    Test isolation: NiceGUI's global ``context.slot_stack`` is saved before entering
    ``asyncio.run()`` and restored after, so subsequent non-simulation NiceGUI tests
    (those using ``with ui.column():`` context) are not contaminated.  The save/restore
    must happen at THIS synchronous level — inside asyncio.run() it's too late because
    the simulation has already modified the stack before our context-manager runs.
    """
    # Save NiceGUI global slot_stack BEFORE entering asyncio.run() — the simulation
    # can modify it, and we must restore it so subsequent tests see clean state.
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _run():
        async with _joins_lab_user_context(
            mock_search_results=mock_search_results,
            mock_vs_raw=mock_vs_raw,
        ) as user:
            coro = driver(user)
            await coro

    try:
        asyncio.run(_run())
    finally:
        # Restore NiceGUI global slot_stack to prevent test contamination.
        # The User simulation may leave it empty (all slots popped) or dirty;
        # restoring ensures subsequent NiceGUI-based tests see their expected
        # slot context (confirmed fix for "expected an active NiceGUI slot" failure).
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)


# ---------------------------------------------------------------------------
# pytest fixture: exposes run_joins_lab_smoke to tests as a fixture argument
# ---------------------------------------------------------------------------

@pytest.fixture
def joins_lab_smoke_runner():
    """Pytest fixture yielding the run_joins_lab_smoke() helper.

    Tests use this as::

        def test_foo(joins_lab_smoke_runner):
            async def driver(user: User) -> None:
                await user.open('/joins-lab')
                await user.should_see(...)
            joins_lab_smoke_runner(driver)
    """
    return run_joins_lab_smoke
