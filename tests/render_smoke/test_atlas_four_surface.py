# -*- coding: utf-8 -*-
"""Four-surface availability integration test for the Connections Atlas (Phase 133, ATLAS-01).

Plan 133-06, Task 2. Closes Codex MEDIUM-6 (T-133-20): the FOUR availability
surfaces the atlas exposes must ALL gate on the single
``web.atlas_assets.atlas_preview_available()`` predicate (flag ON *and* the
baked asset loaded), so a flag-ON/asset-missing window never advertises a
broken beta from any surface. The surfaces are:

  1. PAGE   -- the ``/atlas`` page route (renders beta chrome, or clean-hides).
  2. DATA   -- the ``/atlas-data/manifest.json`` + ``/atlas-data/{asset_name}``
               routes (200 when ready, 404 otherwise).
  3. NAV    -- the "Connections Atlas / Beta" nav item appended in
               ``create_layout()`` (present or absent).
  4. TEASER -- the homepage teaser card on ``/`` (present + links to /atlas, or
               absent).

Each surface is parametrized across THREE availability states:

  * ``off``           -- ``ATLAS_PREVIEW_ENABLED`` False (asset irrelevant).
  * ``asset_missing`` -- flag ON but the asset never loaded (``ready=False``);
                         this is the MEDIUM-6 case the plan calls out by name.
  * ``ready``         -- flag ON AND the asset loaded.

Every assertion is BEHAVIORAL (renders / 200 / present vs clean-hidden / 404 /
absent) -- the nav and teaser are proven by driving the live render tree, NOT
by a source-string reference (the gap Codex MEDIUM-6 flagged in the 133-03
tests). The states are driven through the REAL predicate by monkeypatching the
underlying ``web.atlas_assets`` module state (``ATLAS_PREVIEW_ENABLED`` +
``_state``), not a ``return_value`` mock of the predicate -- so the test proves
all four surfaces read the SAME predicate.

Harness shape mirrors ``test_atlas_render_smoke.py`` / ``test_home_teaser_render_smoke.py``:
a NiceGUI ``User`` over ``httpx.ASGITransport(core.app)`` (never a launched web
server -- project memory ``feedback_no_background_webserver.md``), auto-tagged
``render_smoke`` by ``tests/conftest.py``. The ``ready``-state DATA-route bytes
are a small self-contained in-memory ``_AtlasState`` (no 2.9 GB research DB, no
bake-time deps, no committed ``atlas_data/``); the data route serves those bytes
verbatim (it does not re-validate the binary -- that is ``load_atlas_state``'s
job), so a tiny fabricated payload is sufficient to prove 200-vs-404 gating.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from typing import Callable
from unittest.mock import patch

import httpx
import pytest

# Import web.main at module load -- registers /atlas, /, and the /atlas-data/*
# routes on core.app (also done by the package conftest; idempotent).
import web.main as _web_main  # noqa: E402, F401
import web.atlas_assets as aa
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


_TEASER_MARKER = "atlas-teaser-card"
_NAV_BADGE_CLASS = "nav-item-badge"  # unique to the atlas nav item (only badged nav item)
_STATES = ["off", "asset_missing", "ready"]


# ---------------------------------------------------------------------------
# State driver: the REAL predicate via the underlying module state (MEDIUM-6)
# ---------------------------------------------------------------------------

def _ready_state():
    """A small, self-contained, fully-populated ``_AtlasState`` for the READY
    case. The data route serves ``plain_bytes``/``manifest_bytes`` verbatim (it
    does not re-validate the binary), so a tiny fabricated payload proves the
    200-vs-404 gating without the real bake. ``br_bytes=None`` so the asset
    route always negotiates identity (no httpx Brotli auto-decode surprise)."""
    plain = b"ATLAS-FOUR-SURFACE-TEST-PAYLOAD"
    content_hash = hashlib.sha256(plain).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    bin_name = f"{basename}.bin"
    manifest = {"asset_basename": basename, "content_hash": content_hash, "schema_version": 1}
    manifest_bytes = json.dumps(manifest).encode("utf-8")
    state = aa._AtlasState(
        ready=True,
        manifest_bytes=manifest_bytes,
        manifest=manifest,
        bin_name=bin_name,
        plain_bytes=plain,
        br_bytes=None,
        etag=f'"{content_hash}"',
    )
    return state, bin_name


def _apply_state(monkeypatch, state: str):
    """Drive ``atlas_preview_available()`` through the underlying module state.
    Returns the content-hashed bin_name for the READY case (else None)."""
    if state == "off":
        monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", False)
        monkeypatch.setattr(aa, "_state", aa._AtlasState(ready=True))  # ready but flag off
        assert aa.atlas_preview_available() is False
        return None
    if state == "asset_missing":
        monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", True)
        monkeypatch.setattr(aa, "_state", aa._AtlasState(ready=False))
        assert aa.atlas_preview_available() is False
        return None
    if state == "ready":
        st, bin_name = _ready_state()
        monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", True)
        monkeypatch.setattr(aa, "_state", st)
        assert aa.atlas_preview_available() is True
        return bin_name
    raise AssertionError(f"unknown state {state!r}")


# ---------------------------------------------------------------------------
# Render harness: yields (user, client) so page/nav/teaser use the User and the
# data routes use a direct httpx GET over the same ASGITransport.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _ctx(lang: str = "en"):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()  # no real SearchEngine build
    try:
        prepare_simulation()
        set_storage_secret("atlas-four-surface-secret", {})
        with patch("web.main._resolve_ui_language", return_value=lang):
            os.environ["NICEGUI_USER_SIMULATION"] = "true"
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app),
                        base_url="http://test",
                    ) as client:
                        yield User(client), client
            finally:
                os.environ.pop("NICEGUI_USER_SIMULATION", None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver: Callable, lang: str = "en") -> None:
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _main():
        async with _ctx(lang=lang) as (user, client):
            await driver(user, client)

    try:
        asyncio.run(_main())
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
        return [(e.content or "") for e in ElementFilter(kind=ui.html)]


def _has_atlas_canvas(user) -> bool:
    """True iff a native <canvas id="atlas-canvas"> element rendered.

    The canvas is a ui.element('canvas') (NOT ui.html — whose default client-side
    sanitize strips the id), so it carries no .content string; inspect the element
    tree for the id prop instead.
    """
    with user._client:
        for el in user._client.elements.values():
            if (getattr(el, "_props", {}) or {}).get("id") == "atlas-canvas":
                return True
    return False


def _nav_badge_present(user) -> bool:
    """True iff a label carrying the unique atlas nav badge class is rendered.
    The atlas nav item is the ONLY badged nav item, and the page-chrome 'Beta'
    badge does NOT carry this class -- so this isolates the NAV surface."""
    from nicegui import ElementFilter, ui
    with user._client:
        return any(
            _NAV_BADGE_CLASS in (getattr(e, "_classes", None) or [])
            for e in ElementFilter(kind=ui.label)
        )


def _marked_element(user, marker: str):
    with user._client:
        for el in user._client.elements.values():
            if marker in getattr(el, "_markers", []):
                return el
    return None


def _click_element(user, element) -> None:
    from nicegui import events
    with user._client:
        for listener in element._event_listeners.values():
            if listener.element_id != element.id:
                continue
            ea = events.GenericEventArguments(sender=element, client=user._client, args=None)
            events.handle_event(listener.handler, ea)


# ---------------------------------------------------------------------------
# Surface 1: PAGE route /atlas
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", _STATES)
def test_page_surface(state, monkeypatch):
    """/atlas renders the beta chrome (canvas) only when ready; otherwise it
    clean-hides with a 'temporarily unavailable' card and NEVER renders chrome."""
    _apply_state(monkeypatch, state)

    async def driver(user, client):
        await user.open("/atlas")
        has_canvas = _has_atlas_canvas(user)
        labels = _label_texts(user)
        has_unavailable = any("temporarily unavailable" in t.lower() for t in labels)

        if state == "ready":
            assert has_canvas, (
                f"PAGE/ready FAIL: /atlas did not render the #atlas-canvas chrome. "
                f"Labels: {labels[:15]}"
            )
            assert not has_unavailable, (
                "PAGE/ready FAIL: the clean-hide 'temporarily unavailable' card "
                "rendered even though the atlas is ready."
            )
        else:
            assert not has_canvas, (
                f"PAGE/{state} FAIL: /atlas rendered the #atlas-canvas chrome while "
                f"unavailable -- it must clean-hide (create_atlas_page unreachable)."
            )
            assert has_unavailable, (
                f"PAGE/{state} FAIL: /atlas did not show the 'temporarily "
                f"unavailable' clean-hide card. Labels: {labels[:15]}"
            )

    _run(driver, lang="en")


# ---------------------------------------------------------------------------
# Surface 2: DATA routes /atlas-data/*
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", _STATES)
def test_data_surface(state, monkeypatch):
    """The manifest + content-hashed asset routes serve 200 only when ready;
    they 404 while the flag is OFF or the asset is not loaded (HIGH-1)."""
    bin_name = _apply_state(monkeypatch, state)

    async def driver(user, client):
        manifest_resp = await client.get("/atlas-data/manifest.json")
        if state == "ready":
            assert manifest_resp.status_code == 200, (
                f"DATA/ready FAIL: manifest route returned {manifest_resp.status_code}, "
                "expected 200."
            )
            asset_resp = await client.get(
                f"/atlas-data/{bin_name}", headers={"Accept-Encoding": "identity"}
            )
            assert asset_resp.status_code == 200, (
                f"DATA/ready FAIL: content-hashed asset route returned "
                f"{asset_resp.status_code} for {bin_name!r}, expected 200."
            )
            # Whitelist: a non-matching asset name still 404s even when ready.
            bad_resp = await client.get("/atlas-data/atlas-v1-deadbeefdead.bin")
            assert bad_resp.status_code == 404, (
                f"DATA/ready FAIL: a non-whitelisted asset name returned "
                f"{bad_resp.status_code}, expected 404 (T-133-04)."
            )
        else:
            assert manifest_resp.status_code == 404, (
                f"DATA/{state} FAIL: manifest route returned "
                f"{manifest_resp.status_code} while unavailable, expected 404."
            )
            asset_resp = await client.get(
                "/atlas-data/atlas-v1-anyname.bin", headers={"Accept-Encoding": "identity"}
            )
            assert asset_resp.status_code == 404, (
                f"DATA/{state} FAIL: asset route returned {asset_resp.status_code} "
                f"while unavailable, expected 404."
            )

    _run(driver, lang="en")


# ---------------------------------------------------------------------------
# Surface 3: NAV link in create_layout()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", _STATES)
def test_nav_surface(state, monkeypatch):
    """The 'Connections Atlas / Beta' nav item is appended (present) only when
    ready; absent while unavailable. create_layout() runs on every page (incl.
    the clean-hidden /atlas), so the nav is asserted on the /atlas page in all
    three states, isolated by the unique nav-item-badge class."""
    _apply_state(monkeypatch, state)

    async def driver(user, client):
        await user.open("/atlas")
        present = _nav_badge_present(user)
        if state == "ready":
            assert present, (
                "NAV/ready FAIL: the atlas nav item (unique 'nav-item-badge' "
                "label) is absent even though the atlas is ready."
            )
        else:
            assert not present, (
                f"NAV/{state} FAIL: the atlas nav item is present while "
                f"unavailable -- it must gate on atlas_preview_available()."
            )

    _run(driver, lang="en")


# ---------------------------------------------------------------------------
# Surface 4: homepage teaser card on /
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("state", _STATES)
def test_teaser_surface(state, monkeypatch):
    """The homepage teaser card is present (and links to /atlas) only when
    ready; absent while unavailable (the fourth MEDIUM-6 surface)."""
    _apply_state(monkeypatch, state)

    async def driver(user, client):
        await user.open("/")
        card = _marked_element(user, _TEASER_MARKER)
        if state == "ready":
            assert card is not None, (
                "TEASER/ready FAIL: no element marked 'atlas-teaser-card' on '/' "
                "even though the atlas is ready."
            )
            # Links to /atlas: clicking the card navigates to '/atlas'.
            with patch("nicegui.ui.navigate.to") as nav_mock:
                _click_element(user, card)
                assert nav_mock.called, (
                    "TEASER/ready FAIL: clicking the teaser did not call ui.navigate.to."
                )
                assert all(c.args == ("/atlas",) for c in nav_mock.call_args_list), (
                    f"TEASER/ready FAIL: teaser navigated somewhere other than "
                    f"'/atlas'. Calls: {nav_mock.call_args_list!r}"
                )
        else:
            assert card is None, (
                f"TEASER/{state} FAIL: the teaser card is present while "
                f"unavailable -- it must gate on atlas_preview_available()."
            )

    _run(driver, lang="en")
