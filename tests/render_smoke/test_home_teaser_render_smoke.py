# -*- coding: utf-8 -*-
"""Render-smoke test: NiceGUI User drives the live server-render path for `/`
(the homepage Connections Atlas teaser card).

Phase 133, Plan 05 (ATLAS-01 SC#6). This is the fourth-surface half of the
MEDIUM-6 predicate coverage — the /atlas page route, the nav append, and both
data routes are covered by plan 133-03's ``tests/test_atlas_flag_gating.py``
(``test_all_three_surfaces_gate_on_the_single_predicate``); this file proves
the homepage teaser gates on the SAME ``atlas_preview_available()`` predicate
(flag AND loaded asset), so a flag-ON/asset-missing window never advertises a
broken link from the homepage either.

Scope (deliberately narrow, mirrors ``test_atlas_render_smoke.py``)
--------------------------------------------------------------------
This harness only asserts what a SERVER-SIDE render can prove:

  * with the real ``web.atlas_assets`` predicate monkeypatched to True (flag ON
    AND ``_state.ready`` True — no real baked asset needed), ``/`` renders a
    small teaser card marked ``atlas-teaser-card`` that navigates to ``/atlas``
    on click/keydown.enter/keydown.space, carries a Beta badge, and whose
    rendered text contains no claim-level wording (no digits, no
    "identification", no "discoveries found");
  * with the predicate False — BOTH the flag-OFF case AND the flag-ON-but-
    asset-not-loaded case — the teaser card is absent and ``/`` still renders;
  * EN and HE both render, with the HE teaser text carrying REAL Hebrew values
    (not the English translation keys leaking through).

It does not exercise the /atlas page itself (covered by 133-03/133-04's render
smokes) or Search/Composition/anything beyond the homepage grid.

Harness shape mirrors ``test_atlas_render_smoke.py``: a NiceGUI ``User`` over
``httpx.ASGITransport(core.app)``, auto-tagged ``render_smoke`` by
``tests/conftest.py``. The package ``conftest.py`` imports ``web.main`` at
collection time, registering the ``@ui.page('/')`` route on ``core.app``; we
clear ``_startup_handlers`` so the real SearchEngine/MetadataManager are never
built (the homepage degrades gracefully to `state.searcher is None` etc. --
see ``web/state.py::AppState.is_ready``).
"""

from __future__ import annotations

import asyncio
import os
import re
from contextlib import asynccontextmanager
from typing import Callable
from unittest.mock import patch

import httpx

# Import web.main at module load — registers '/' on core.app (also done by the
# package conftest; the import is idempotent).
import web.main as _web_main  # noqa: E402, F401
import web.atlas_assets as aa
import web.discovery_assets as da
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


_TEASER_MARKER = 'atlas-teaser-card'

# Claim-level wording the teaser must NEVER carry (T-133-06 / D-16). Digits are
# checked separately (any ASCII digit anywhere in the teaser's own text).
_FORBIDDEN_SUBSTRINGS_EN = ('identification', 'discoveries found', 'discovery')
# HE: "תגליות" (discoveries) is the Pitfall #8 name-collision to avoid (never
# call this "Discoveries"). NOTE: the approved 133-03 HE description itself
# reads "...סקירה אלגוריתמית, ללא טענות זיהוי" ("...an algorithmic overview,
# WITHOUT identification claims") -- "זיהוי" appears there as part of the
# NEGATION (honesty framing), not as a claim, so it is intentionally NOT
# forbidden; the positive assertion below checks the negation phrase
# ("ללא טענות" / "without claims") is present instead.
_FORBIDDEN_SUBSTRINGS_HE = ('תגליות',)
_CLAIM_FREE_NEGATION_HE = 'ללא טענות'


def _set_atlas_predicate(monkeypatch, *, flag_on: bool, ready: bool) -> None:
    """Drive the REAL atlas_preview_available() predicate (flag AND loaded
    asset) via the underlying module state -- not a return_value mock -- so
    this test proves the teaser reads the SAME predicate the page/nav/data
    routes use (MEDIUM-6), not a hand-wired stand-in."""
    monkeypatch.setattr(aa, 'ATLAS_PREVIEW_ENABLED', flag_on)
    monkeypatch.setattr(aa, '_state', aa._AtlasState(ready=ready))


def _set_discovery_predicate(monkeypatch, *, flag_on: bool, ready: bool) -> None:
    """Drive the shared discovery flag+loaded-asset predicate."""
    monkeypatch.setattr(da, 'DISCOVERY_ENABLED', flag_on)
    monkeypatch.setattr(da, '_state', da._DiscoveryState(ready=ready))


# ---------------------------------------------------------------------------
# Homepage-specific user context: engine init skipped, UI language forced.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _home_user_context(lang: str = 'he'):
    """Yield a ready NiceGUI User for '/'.

    - ``core.app._startup_handlers`` cleared so ``initialize_engine`` never
      runs (the homepage degrades gracefully with state.searcher/meta_mgr/
      lists_mgr/lab_engine all None -- see web/state.py).
    - ``web.main._resolve_ui_language`` -> ``lang`` so create_layout sets the
      UI language (HE default; EN when requested), driving direction + tr().
    """
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('home-teaser-render-smoke-secret', {})
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


def _run_home_smoke(driver: Callable[[User], 'asyncio.Future'], lang: str = 'he') -> None:
    """Synchronous helper: run an async driver against '/', restoring the
    NiceGUI global slot_stack afterward (test isolation)."""
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _run():
        async with _home_user_context(lang=lang) as user:
            await driver(user)

    try:
        asyncio.run(_run())
    finally:
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)


# ---------------------------------------------------------------------------
# Render-tree helpers
# ---------------------------------------------------------------------------

def _all_elements(user):
    with user._client:
        return list(user._client.elements.values())


def _marked_element(user, marker: str):
    for el in _all_elements(user):
        if marker in getattr(el, '_markers', []):
            return el
    return None


_TAG_RE = re.compile(r'<[^>]+>')


def _descendant_texts(root) -> list[str]:
    """Collect .text / .content strings from root and every nested slot child
    (NiceGUI containers expose children via element.slots[name].children).

    ``.content`` on a ui.html-based element (e.g. the h3 SemanticHeading
    wrapper -- web/components/typography.py) is raw markup ("<h3 ...>...
    </h3>"); tags are stripped so a claim-free/digit scan of the visible text
    doesn't false-positive on a digit inside a TAG NAME (e.g. the '3' in
    '<h3>') rather than the actual displayed content.
    """
    out: list[str] = []
    stack = [root]
    seen = set()
    while stack:
        el = stack.pop()
        if id(el) in seen:
            continue
        seen.add(id(el))
        text = getattr(el, 'text', None)
        if isinstance(text, str) and text:
            out.append(text)
        content = getattr(el, 'content', None)
        if isinstance(content, str) and content:
            stripped = _TAG_RE.sub('', content).strip()
            if stripped:
                out.append(stripped)
        for slot in getattr(el, 'slots', {}).values():
            stack.extend(slot.children)
    return out


def _click_element(user, element) -> None:
    """Fire the click event on a specific element (bypasses UserInteraction find)."""
    from nicegui import events
    with user._client:
        for listener in element._event_listeners.values():
            if listener.element_id != element.id:
                continue
            ea = events.GenericEventArguments(sender=element, client=user._client, args=None)
            events.handle_event(listener.handler, ea)


# ---------------------------------------------------------------------------
# Tests: available -> teaser present, claim-free, /atlas-linking, bilingual
# ---------------------------------------------------------------------------

def test_teaser_present_when_available_en(monkeypatch):
    """flag ON + asset loaded (EN): the teaser card renders, is claim-free, and
    navigates to /atlas on click."""
    _set_atlas_predicate(monkeypatch, flag_on=True, ready=True)
    assert aa.atlas_preview_available() is True

    async def driver(user):
        await user.open('/')

        card = _marked_element(user, _TEASER_MARKER)
        assert card is not None, (
            "Home teaser FAIL: no element marked 'atlas-teaser-card' found on '/' "
            "with atlas_preview_available() True. Check web/pages/home.py gates "
            "the card under `if atlas_preview_available():` and calls .mark(...)."
        )

        texts = _descendant_texts(card)
        blob = ' '.join(texts)
        assert texts, "Home teaser FAIL: the marked card rendered no text content."

        # Beta badge present.
        assert any(t == 'Beta' for t in texts), (
            f"Home teaser FAIL: 'Beta' badge text not found in teaser card. Texts: {texts}"
        )

        # Claim-free: no digits, no claim-level substrings, in the CARD's own text.
        assert not any(ch.isdigit() for ch in blob), (
            f"Home teaser FAIL: teaser card text contains a digit (claim-level "
            f"count/number wording is forbidden). Text: {blob!r}"
        )
        lowered = blob.lower()
        offenders = [s for s in _FORBIDDEN_SUBSTRINGS_EN if s in lowered]
        assert not offenders, (
            f"Home teaser FAIL: teaser card text contains forbidden claim-level "
            f"wording {offenders!r}. Text: {blob!r}"
        )

        # Click -> ui.navigate.to('/atlas'). Patch ui.navigate.to to record calls
        # (clicking would otherwise attempt a real client-side navigation).
        # The card has THREE identical-target listeners (click, keydown.enter,
        # keydown.space -- matching every other homepage card's pattern), and
        # _click_element fires all listeners on the element, so assert every
        # recorded call targeted '/atlas' rather than call-count-once.
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _click_element(user, card)
            assert nav_mock.called, (
                "Home teaser FAIL: clicking the teaser card did not call "
                "ui.navigate.to at all."
            )
            assert all(c.args == ('/atlas',) for c in nav_mock.call_args_list), (
                f"Home teaser FAIL: teaser card navigated somewhere other than "
                f"'/atlas'. Calls: {nav_mock.call_args_list!r}"
            )

    _run_home_smoke(driver, lang='en')


def test_teaser_present_when_available_he_real_hebrew_values(monkeypatch):
    """flag ON + asset loaded (HE): the teaser renders with REAL Hebrew values
    (not the English tr() keys leaking through under the Hebrew UI)."""
    _set_atlas_predicate(monkeypatch, flag_on=True, ready=True)

    async def driver(user):
        await user.open('/')

        card = _marked_element(user, _TEASER_MARKER)
        assert card is not None, (
            "Home teaser (HE) FAIL: no element marked 'atlas-teaser-card' found."
        )
        texts = _descendant_texts(card)
        blob = ' '.join(texts)

        # Hebrew Beta badge value ('בטא'), never the raw English key.
        assert any(t == 'בטא' for t in texts), (
            f"Home teaser (HE) FAIL: Hebrew Beta badge value 'בטא' not found "
            f"(English 'Beta' would leak under HE if the tr() key were missing "
            f"a Hebrew value). Texts: {texts}"
        )
        assert not any(t == 'Beta' for t in texts), (
            "Home teaser (HE) FAIL: raw English 'Beta' leaked under the "
            "Hebrew UI -- the tr() key must resolve to the Hebrew value."
        )
        # The Hebrew title value (renamed 2026-07-21: the card uses the short
        # sidebar name 'The Genizah Atlas' -> 'אטלס הגניזה').
        assert any('אטלס הגניזה' in t for t in texts), (
            f"Home teaser (HE) FAIL: Hebrew 'אטלס הגניזה' title not found. Texts: {texts}"
        )

        # Claim-free in Hebrew too: no digits, no HE claim-level substrings
        # (never named "Discoveries" -- Pitfall #8), and the honesty-negation
        # phrasing ("without claims") is present.
        assert not any(ch.isdigit() for ch in blob), (
            f"Home teaser (HE) FAIL: teaser text contains a digit. Text: {blob!r}"
        )
        offenders = [s for s in _FORBIDDEN_SUBSTRINGS_HE if s in blob]
        assert not offenders, (
            f"Home teaser (HE) FAIL: forbidden HE claim-level wording "
            f"{offenders!r} found. Text: {blob!r}"
        )
        assert _CLAIM_FREE_NEGATION_HE in blob, (
            f"Home teaser (HE) FAIL: expected the claim-free negation phrase "
            f"{_CLAIM_FREE_NEGATION_HE!r} ('without claims') in the Hebrew "
            f"description. Text: {blob!r}"
        )

    _run_home_smoke(driver, lang='he')


# ---------------------------------------------------------------------------
# Tests: unavailable (flag OFF, and flag-ON/asset-not-loaded) -> teaser absent
# ---------------------------------------------------------------------------

def test_teaser_absent_when_flag_off(monkeypatch):
    """flag OFF (asset irrelevant): the teaser card is absent; '/' still renders."""
    _set_atlas_predicate(monkeypatch, flag_on=False, ready=True)
    assert aa.atlas_preview_available() is False

    async def driver(user):
        await user.open('/')

        card = _marked_element(user, _TEASER_MARKER)
        assert card is None, (
            "Home teaser FAIL: teaser card is present with the flag OFF. "
            "It must gate on atlas_preview_available(), not render "
            "unconditionally."
        )
        # The homepage must still render normally (other cards present).
        texts = [t for el in _all_elements(user) for t in _descendant_texts(el)]
        assert any('Text Search' in t for t in texts), (
            "Home teaser FAIL (sanity): '/' did not render its other homepage "
            "cards -- the page itself may have failed to render."
        )

    _run_home_smoke(driver, lang='en')


def test_teaser_absent_when_flag_on_but_asset_not_loaded(monkeypatch):
    """flag ON but the asset never loaded (ready=False): the teaser card is
    absent -- a flag-ON/asset-missing window must never advertise a broken
    /atlas link from the homepage (MEDIUM-6, the fourth surface)."""
    _set_atlas_predicate(monkeypatch, flag_on=True, ready=False)
    assert aa.atlas_preview_available() is False

    async def driver(user):
        await user.open('/')

        card = _marked_element(user, _TEASER_MARKER)
        assert card is None, (
            "Home teaser FAIL: teaser card is present while the flag is ON "
            "but the asset is not loaded (ready=False). This would advertise "
            "a broken /atlas link -- the card must gate on "
            "atlas_preview_available() (flag AND loaded asset), not the bare "
            "flag."
        )
        texts = [t for el in _all_elements(user) for t in _descendant_texts(el)]
        assert any('Text Search' in t for t in texts), (
            "Home teaser FAIL (sanity): '/' did not render its other homepage "
            "cards -- the page itself may have failed to render."
        )

    _run_home_smoke(driver, lang='en')


def test_requested_discovery_tools_appear_across_homepage_surfaces(monkeypatch):
    """Computed IDs and Atlas reach chips, carousel, and tool cards; Joins Lab
    gets its requested tool card. Every marked entry navigates to its own route."""
    _set_atlas_predicate(monkeypatch, flag_on=True, ready=True)
    _set_discovery_predicate(monkeypatch, flag_on=True, ready=True)

    expected = {
        'home-chip-computed': '/computed-identifications',
        'home-chip-atlas': '/atlas',
        'home-carousel-computed': '/computed-identifications',
        'home-carousel-atlas': '/atlas',
        'computed-tool-card': '/computed-identifications',
        'joins-lab-tool-card': '/joins-lab',
        'atlas-teaser-card': '/atlas',
    }

    async def driver(user):
        await user.open('/')
        for marker, route in expected.items():
            element = _marked_element(user, marker)
            assert element is not None, f'missing homepage entry point: {marker}'
            with patch('nicegui.ui.navigate.to') as nav_mock:
                _click_element(user, element)
                assert nav_mock.called
                assert all(call.args == (route,) for call in nav_mock.call_args_list)

    _run_home_smoke(driver, lang='en')


def test_gated_homepage_entries_hide_cleanly_and_joins_card_remains_he(monkeypatch):
    """Unavailable asset-backed tools vanish from all new entry points, while
    the always-public Joins Lab card remains useful and translated in Hebrew."""
    _set_atlas_predicate(monkeypatch, flag_on=True, ready=False)
    _set_discovery_predicate(monkeypatch, flag_on=True, ready=False)

    async def driver(user):
        await user.open('/')
        for marker in (
            'home-chip-computed',
            'home-chip-atlas',
            'home-carousel-computed',
            'home-carousel-atlas',
            'computed-tool-card',
            'atlas-teaser-card',
        ):
            assert _marked_element(user, marker) is None

        joins_card = _marked_element(user, 'joins-lab-tool-card')
        assert joins_card is not None
        blob = ' '.join(_descendant_texts(joins_card))
        assert 'מעבדת צירופים' in blob
        assert 'מצאו והשוו קטעים מצטרפים' in blob
        assert 'Find and compare joining fragments' not in blob

    _run_home_smoke(driver, lang='he')


def test_the_hero_text_column_keeps_a_width_floor_so_the_row_wraps_on_a_phone():
    """A REGRESSION GUARD on a defect that looks idiomatic in review.

    The hero is a `flex-wrap` row: the title/subtitle column beside the inline
    stats. While that column carried `flex-1 min-w-0` it was allowed to shrink
    to NOTHING, so the row's min-content width was the stats' width alone, the
    line never overflowed, `flex-wrap` never fired on a phone, and the stats
    kept their intrinsic width while the Hebrew title wrapped one word per line
    down a tall, mostly-empty card (reported 2026-08-13, on mobile).

    `min-w-0` is the right default nearly everywhere in a flex layout, which is
    exactly why this needs a guard rather than a comment: the floor reads like
    something to tidy away. Asserted as a DECLARATION rather than a computed
    layout because a headless render exposes no geometry -- so this cannot prove
    the hero looks right, only that the property whose absence broke it is still
    there.
    """
    async def driver(user):
        await user.open('/')
        column = _marked_element(user, 'home-hero-text')
        assert column is not None, (
            'the hero text column lost its marker; if the hero was '
            'restructured, re-point this guard rather than deleting it'
        )
        style_items = (getattr(column, '_style', None) or {}).items()
        style = '; '.join(f'{prop}: {value}' for prop, value in style_items)
        classes = getattr(column, '_classes', None) or []
        assert 'min-w-0' not in classes, (
            'the hero text column is back to `min-w-0`: it may now shrink to '
            'zero, so flex-wrap will not fire and the title collapses to one '
            'word per line on a phone'
        )
        assert 'min-width' in style and 'min(' in style, (
            'the hero text column lost its min-width floor. Without it the row '
            'cannot wrap on a phone; with a BARE floor (no `min()`) it '
            f'overflows a 320px viewport instead. Style was: {style!r}'
        )
        assert 'flex' in style, (
            f'the hero text column lost its flex basis. Style was: {style!r}'
        )

    _run_home_smoke(driver, lang='he')
