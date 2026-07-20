# -*- coding: utf-8 -*-
"""Render-smoke test: NiceGUI User drives the live server-render path for /atlas.

Phase 133, Plan 04 (ATLAS-01).

Scope (deliberately narrow — MEDIUM-2)
--------------------------------------
This harness only asserts what a SERVER-SIDE render can prove:

  * with ``web.main.atlas_preview_available()`` mocked True (no real 2.9 GB DB
    or scp'd asset needed), ``/atlas`` renders the beta chrome (Beta badge +
    the standing honesty banner) and a ``#atlas-canvas`` element with RESERVED
    dimensions (fixed 720px height, not just a max-height — CLS-safe, D-10);
  * EN and HE both render, with the correct text direction (RTL under HE);
  * the client-side decoder module is injected (``/static/js/atlas_decode.js``
    + the ``AtlasDecode.init`` bootstrap that fetches ``/atlas-data/manifest.json``).

It does NOT (and must not) claim to exercise fetch/decode or any Canvas
interaction — those run only in a real browser and are covered by the Node
golden/DOM-XSS tests in ``tests/atlas_bake/test_atlas_golden_js.py`` plus the
live UAT in plan 133-06.

Harness shape mirrors ``test_joins_lab_render_smoke.py``: a NiceGUI ``User`` over
``httpx.ASGITransport(core.app)``, auto-tagged ``render_smoke`` by tests/conftest.py.
The package ``conftest.py`` imports ``web.main`` at collection time, registering
the ``@ui.page('/atlas')`` route on ``core.app``; we clear ``_startup_handlers``
so the real SearchEngine is never built.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Callable
from unittest.mock import patch

import httpx

# Import web.main at module load — registers /atlas on core.app (also done by the
# package conftest; the import is idempotent).
import web.main as _web_main  # noqa: E402, F401
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


# ---------------------------------------------------------------------------
# Atlas-specific user context: flag+asset mocked available, engine init skipped.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _atlas_user_context(lang: str = 'he'):
    """Yield a ready NiceGUI User for /atlas.

    - ``core.app._startup_handlers`` cleared so ``initialize_engine`` never runs.
    - ``web.main.atlas_preview_available`` -> True so the route delegates to
      ``create_atlas_page`` instead of the clean-hide "unavailable" card.
    - ``web.main._resolve_ui_language`` -> ``lang`` so create_layout sets the UI
      language (HE default; EN when requested), driving direction + tr().
    """
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('atlas-render-smoke-secret', {})
        with patch('web.main.atlas_preview_available', return_value=True):
            with patch('web.main._resolve_ui_language', return_value=lang):
                os.environ['NICEGUI_USER_SIMULATION'] = 'true'
                try:
                    async with core.app.router.lifespan_context(core.app):
                        async with httpx.AsyncClient(
                            transport=httpx.ASGITransport(core.app),
                            base_url='http://test',
                        ) as client:
                            user = User(client)
                            yield user
                finally:
                    os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run_atlas_smoke(driver: Callable[[User], 'asyncio.Future'], lang: str = 'he') -> None:
    """Synchronous helper: run an async driver against /atlas, restoring the
    NiceGUI global slot_stack afterward (test isolation)."""
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _run():
        async with _atlas_user_context(lang=lang) as user:
            await driver(user)

    try:
        asyncio.run(_run())
    finally:
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)


# ---------------------------------------------------------------------------
# Render-tree helpers
# ---------------------------------------------------------------------------

def _label_texts(user) -> list[str]:
    from nicegui import ElementFilter, ui
    with user._client:
        return [e.text for e in ElementFilter(kind=ui.label) if e.text]


def _html_contents(user) -> list[str]:
    from nicegui import ElementFilter, ui
    with user._client:
        return [(e.content or '') for e in ElementFilter(kind=ui.html)]


def _column_directions(user) -> list[str]:
    """Collect the 'direction' inline-style value of every rendered element."""
    from nicegui import ElementFilter, ui
    dirs = []
    with user._client:
        for e in ElementFilter(kind=ui.column):
            style = getattr(e, '_style', None) or {}
            if 'direction' in style:
                dirs.append(style['direction'])
    return dirs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_atlas_renders_chrome_canvas_and_decoder_injection():
    """HE (default): the beta chrome + honesty banner render, a CLS-reserved
    canvas exists, and the decoder module + manifest bootstrap are injected."""
    async def driver(user):
        await user.open('/atlas')

        labels = _label_texts(user)
        # Beta badge (HE value from genizah_translations.py).
        assert any('בטא' == t for t in labels), (
            "Atlas render FAIL: Beta badge label ('בטא') not found. "
            f"Labels: {labels[:20]}"
        )
        # Standing honesty banner (D-15) — algorithmic-not-provenance disclaimer.
        assert any('אלגוריתמית' in t for t in labels), (
            "Atlas render FAIL: honesty banner text not found in any label."
        )

        # CLS-reserved canvas: a ui.html carrying #atlas-canvas with a FIXED
        # 720px height (not just max-height).
        htmls = _html_contents(user)
        canvas_html = next((c for c in htmls if 'atlas-canvas' in c), None)
        assert canvas_html is not None, (
            "Atlas render FAIL: no #atlas-canvas element rendered."
        )
        assert '720px' in canvas_html, (
            "Atlas render FAIL: canvas is not CLS-reserved with a fixed 720px "
            f"height. Canvas html: {canvas_html!r}"
        )

        # Decoder module + bootstrap injected into the page body.
        body_html = user._client.body_html
        assert '/static/js/atlas_decode.js' in body_html, (
            "Atlas render FAIL: decoder module <script> not injected into body_html."
        )
        assert 'AtlasDecode.init' in body_html, (
            "Atlas render FAIL: AtlasDecode.init bootstrap not injected."
        )
        assert '/atlas-data/manifest.json' in body_html, (
            "Atlas render FAIL: manifest fetch URL not present in the injected config."
        )

        # HE is RTL.
        dirs = _column_directions(user)
        assert 'rtl' in dirs, (
            f"Atlas render FAIL: no RTL column under HE. directions={dirs}"
        )

    _run_atlas_smoke(driver, lang='he')


def test_atlas_renders_en_ltr():
    """EN: chrome renders in English with LTR direction."""
    async def driver(user):
        await user.open('/atlas')

        labels = _label_texts(user)
        assert any(t == 'Beta' for t in labels), (
            f"Atlas EN render FAIL: 'Beta' badge label not found. Labels: {labels[:20]}"
        )
        assert any('algorithmically' in t for t in labels), (
            "Atlas EN render FAIL: English honesty banner text not found."
        )

        dirs = _column_directions(user)
        assert 'ltr' in dirs and 'rtl' not in dirs, (
            f"Atlas EN render FAIL: expected LTR direction only. directions={dirs}"
        )

        # Decoder injection is language-independent.
        assert '/static/js/atlas_decode.js' in user._client.body_html

    _run_atlas_smoke(driver, lang='en')
