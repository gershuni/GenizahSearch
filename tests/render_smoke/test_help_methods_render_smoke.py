# -*- coding: utf-8 -*-
"""Render-smoke test: NiceGUI User drives the live server-render path for /help.

Phase 135, Plan 02 (BAND-05).

Scope
-----
Proves what a SERVER-SIDE render of the async ``/help`` route can prove for the
flag-gated "Confidence Bands & Methods" methods section:

  * with ``web.pages.help.discovery_available`` mocked True (the ACTUAL body/TOC
    gate call-site — Codex #11) and the band-precision + per-band claim-count
    wrappers the route awaits patched to fakes (no real sidecar needed), the
    section renders in EN and HE with the section heading, all 7 per-band deep-
    link anchors (``help-confidence-<band>``), and the BAND-05 field set;
  * the ``population`` field renders from the RUNTIME DISPLAY-DEDUPLICATED
    shipped-claim count (the fake ``band_counts``), NOT the denominator / raw
    evidence rows / the Wave-4 frame doc;
  * the propagated **0.926** collection estimate renders ONLY at collection
    scope (exactly once), never on the corroborated/weak per-band rows;
  * the four registry fields render placeholder-safe (never fabricated) —
    tier_a shows "not yet measured" / "independent audit pending";
  * the Phase-139 noindex transition (Codex #18): pre-release → noindex,
    released → indexed, flag-off → section absent AND indexed;
  * the D-06 / WARNING-4 word gate: "certified" (EN) and its HE equivalents
    (מאומת / מאושר / מוסמך) appear NOWHERE in the confidence section (scoped to
    the section — מאושר legitimately appears in the Joins-Lab Help copy);
  * HE renders RTL.

It does NOT exercise the real sidecar or the DiscoveryService — those are unit-
tested in tests/test_discovery_band_labels.py + the discovery-service suites.

Harness shape mirrors ``test_atlas_render_smoke.py``: a NiceGUI ``User`` over
``httpx.ASGITransport(core.app)``, auto-tagged ``render_smoke`` by
tests/conftest.py. The package conftest imports ``web.main`` at collection time,
registering ``@ui.page('/help')`` on ``core.app``; we clear
``_startup_handlers`` so the real SearchEngine is never built.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import ExitStack, asynccontextmanager
from typing import Callable
from unittest.mock import AsyncMock, patch

import httpx

# Import web.main at module load — registers /help on core.app (also done by the
# package conftest; the import is idempotent).
import web.main as _web_main  # noqa: E402, F401
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


# ---------------------------------------------------------------------------
# Deterministic fakes — the runtime DISPLAY-DEDUPLICATED shipped-claim counts
# (band_counts) + band_precision rows the async route threads into the section.
# Counts are opaque test fixtures; a distinctive tier_a population lets the test
# assert the population field is driven from band_counts (not the denominator).
# ---------------------------------------------------------------------------

_FAKE_TIER_A_POP = 654321  # distinctive → "654,321" in the rendered population

FAKE_COUNTS = {
    ('track1_direct', 'high_confidence_algorithmic'): 1188,
    ('track1_direct', 'tier_a'): _FAKE_TIER_A_POP,
    ('track1_direct', 'screening_rb'): 4210,
    ('track1_direct', 'screening_canon'): 3300,
    ('propagated', 'corroborated'): 900,
    ('propagated', 'weak'): 1109,
    ('propagated', 'not_evaluated'): 60156,
}

FAKE_PRECISION = {
    ('track1_direct', 'high_confidence_algorithmic'): {
        'scope': 'band',
        'evidence_source': 'track1_direct',
        'confidence_band': 'high_confidence_algorithmic',
        'precision': 0.889, 'ci_low': None, 'ci_high': None,
        'numerator': 160, 'denominator': 180, 'draw_size': 200,
        'sampling_frame': None, 'weighting': None,
        'measurement_status': None, 'measurement_date': None,
        'grader': None, 'audit_status': None, 'report_id': None,
    },
    ('track1_direct', 'tier_a'): {
        'scope': 'band',
        'evidence_source': 'track1_direct',
        'confidence_band': 'tier_a',
        'precision': None, 'ci_low': None, 'ci_high': None,
        'numerator': None, 'denominator': None,
        'sampling_frame': None, 'weighting': None,
        'measurement_status': None, 'measurement_date': None,
        'grader': None, 'audit_status': None, 'report_id': None,
    },
    # The 0.926 propagated-witness collection estimate — collection scope ONLY.
    'collection': {
        'scope': 'collection',
        'collection_id': 'propagated_witness_collection_v1',
        'evidence_source': None, 'confidence_band': None,
        'precision': 0.926, 'ci_low': 0.875, 'ci_high': 0.968,
    },
}

# The 7 canonical (v2) band keys whose deep-link anchors must render.
_EXPECTED_BAND_ANCHORS = [
    'help-confidence-high_confidence_algorithmic',
    'help-confidence-tier_a',
    'help-confidence-screening_rb',
    'help-confidence-screening_canon',
    'help-confidence-corroborated',
    'help-confidence-weak',
    'help-confidence-not_evaluated',
]


# ---------------------------------------------------------------------------
# Help-specific user context: discovery flag + noindex + wrappers all patched.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _help_user_context(
    lang: str = 'en',
    discovery_on: bool = True,
    noindex: bool = True,
):
    """Yield a ready NiceGUI User for /help with the discovery seams mocked.

    - ``core.app._startup_handlers`` cleared so ``initialize_engine`` never runs.
    - ``web.pages.help.discovery_available`` -> ``discovery_on`` (the REAL body +
      TOC gate — Codex #11).
    - ``web.main.discovery_methods_noindex`` -> ``noindex`` (drives the robots
      meta; the three-state test flips this + ``discovery_on``).
    - ``web.main.get_all_band_precision`` / ``get_band_claim_counts`` -> fakes so
      the population/precision fields render without a real sidecar.
    - ``web.main._resolve_ui_language`` -> ``lang`` (drives direction + tr()).
    """
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-methods-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(
                patch('web.pages.help.discovery_available', return_value=discovery_on)
            )
            stack.enter_context(
                patch('web.main.discovery_methods_noindex', return_value=noindex)
            )
            stack.enter_context(
                patch('web.main.get_all_band_precision',
                      new=AsyncMock(return_value=FAKE_PRECISION))
            )
            stack.enter_context(
                patch('web.main.get_band_claim_counts',
                      new=AsyncMock(return_value=FAKE_COUNTS))
            )
            stack.enter_context(
                patch('web.main._resolve_ui_language', return_value=lang)
            )
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


def _run_help_smoke(
    driver: Callable[[User], 'asyncio.Future'],
    lang: str = 'en',
    discovery_on: bool = True,
    noindex: bool = True,
) -> None:
    """Synchronous helper: run an async driver against /help, restoring the
    NiceGUI global slot_stack afterward (test isolation)."""
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _run():
        async with _help_user_context(
            lang=lang, discovery_on=discovery_on, noindex=noindex
        ) as user:
            await driver(user)

    try:
        asyncio.run(_run())
    finally:
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)


# ---------------------------------------------------------------------------
# Render-tree helpers
# ---------------------------------------------------------------------------

def _anchor_names(user) -> set:
    """Every rendered element's ``name`` prop (the ui.element('a') anchors)."""
    names = set()
    with user._client:
        for e in user._client.elements.values():
            nm = (getattr(e, '_props', {}) or {}).get('name')
            if nm:
                names.add(nm)
    return names


def _confidence_section_text(user) -> str:
    """All rendered text INSIDE the confidence section card only (scoped via its
    unique marker class), so the word gate is not tripped by legitimate copy
    elsewhere on the Help page (e.g. the Joins-Lab 'confirmed join' מאושר)."""
    from web.pages.help import _CONFIDENCE_SECTION_CLASS
    parts = []
    with user._client:
        for e in user._client.elements.values():
            classes = getattr(e, '_classes', None) or []
            if _CONFIDENCE_SECTION_CLASS in classes:
                for d in e.descendants(include_self=True):
                    for attr in ('text', 'content'):
                        v = getattr(d, attr, None)
                        if isinstance(v, str) and v:
                            parts.append(v)
    return '\n'.join(parts)


def _column_directions(user) -> list:
    """The 'direction' inline-style value of every rendered ui.column."""
    from nicegui import ElementFilter, ui
    dirs = []
    with user._client:
        for e in ElementFilter(kind=ui.column):
            style = getattr(e, '_style', None) or {}
            if 'direction' in style:
                dirs.append(style['direction'])
    return dirs


# ---------------------------------------------------------------------------
# Tests: section render (EN + HE)
# ---------------------------------------------------------------------------

def test_help_methods_section_renders_en():
    """EN: the flag-gated methods section renders with the heading, all 7 band
    anchors, the runtime-count population, placeholder-safe registry fields, and
    the 0.926 at collection scope only. No 'certified' anywhere in the section."""
    async def driver(user):
        await user.open('/help')

        section = _confidence_section_text(user)
        assert 'Confidence Bands and Methods' in section, (
            "Help methods EN FAIL: section heading missing.\n"
            f"Section text (first 400): {section[:400]!r}"
        )

        anchors = _anchor_names(user)
        assert 'help-confidence' in anchors, "Help methods EN FAIL: section anchor missing."
        for a in _EXPECTED_BAND_ANCHORS:
            assert a in anchors, f"Help methods EN FAIL: band anchor {a!r} missing. Got: {sorted(anchors)}"

        # population from the RUNTIME display-deduplicated count (not denominator).
        assert '654,321' in section, (
            "Help methods EN FAIL: tier_a population not rendered from band_counts."
        )

        # registry fields placeholder-safe (never fabricated) for tier_a.
        assert 'Report identifier' in section, "Help methods EN FAIL: report-id field label missing."
        assert 'not yet measured' in section, "Help methods EN FAIL: 'not yet measured' placeholder missing."
        assert 'independent audit pending' in section, (
            "Help methods EN FAIL: 'independent audit pending' audit placeholder missing."
        )
        # tier_a precision copy from the values module (never a bare percentage).
        assert 'precision not yet measured' in section, (
            "Help methods EN FAIL: tier_a should read 'precision not yet measured'."
        )

        # 0.926 collection estimate — rendered ONCE, at collection scope only.
        assert section.count('92.6%') == 1, (
            f"Help methods EN FAIL: 0.926 must appear exactly once (collection scope). "
            f"count={section.count('92.6%')}"
        )
        assert '[87.5%, 96.8%]' in section, "Help methods EN FAIL: collection CI missing."

        # WARNING-4 / D-06 word gate: never 'certified'.
        assert 'certified' not in section.lower(), (
            "Help methods EN FAIL: prohibited word 'certified' present in section."
        )

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


def test_help_methods_section_renders_he_rtl():
    """HE: the section renders in Hebrew (RTL) with the 7 anchors + the runtime
    population + placeholder registry fields, and NONE of the prohibited HE
    'verified/approved/certified' words appear in the section."""
    async def driver(user):
        await user.open('/help')

        section = _confidence_section_text(user)
        assert 'דרגות ודאות ושיטות' in section, (
            "Help methods HE FAIL: HE section heading missing.\n"
            f"Section text (first 400): {section[:400]!r}"
        )

        anchors = _anchor_names(user)
        for a in _EXPECTED_BAND_ANCHORS:
            assert a in anchors, f"Help methods HE FAIL: band anchor {a!r} missing."

        assert '654,321' in section, "Help methods HE FAIL: population not rendered from band_counts."
        assert 'מזהה דוח' in section, "Help methods HE FAIL: HE report-id field label missing."
        assert 'טרם נמדד' in section, "Help methods HE FAIL: HE 'not yet measured' placeholder missing."
        assert section.count('92.6%') == 1, (
            f"Help methods HE FAIL: 0.926 must appear once (collection scope). count={section.count('92.6%')}"
        )

        # HE word gate — none of the prohibited review/certification words.
        for forbidden in ('מאומת', 'מאושר', 'מוסמך'):
            assert forbidden not in section, (
                f"Help methods HE FAIL: prohibited HE word {forbidden!r} present in section."
            )
        assert 'certified' not in section.lower(), "Help methods HE FAIL: 'certified' present in section."

        # HE renders RTL.
        dirs = _column_directions(user)
        assert 'rtl' in dirs, f"Help methods HE FAIL: no RTL column under HE. directions={dirs}"

    _run_help_smoke(driver, lang='he', discovery_on=True, noindex=True)


# ---------------------------------------------------------------------------
# Tests: the Phase-139 three-state noindex transition (Codex #18)
# ---------------------------------------------------------------------------

def _robots_noindex_present(user) -> bool:
    return 'name="robots" content="noindex' in user._client.head_html


def test_help_noindex_pre_release():
    """State (i) — discovery available, NOT publicly released: /help is
    noindexed (the pre-release methods copy is hidden from crawlers)."""
    async def driver(user):
        await user.open('/help')
        assert _robots_noindex_present(user), (
            "Help noindex FAIL: /help must be noindexed pre-release "
            "(discovery_methods_noindex() True)."
        )
        # Section is present pre-release.
        assert 'help-confidence' in _anchor_names(user)

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


def test_help_indexed_after_rel01():
    """State (ii) — discovery available AND publicly released (REL-01 flipped):
    /help is INDEXED (no robots noindex). Proves the noindex is a bounded
    pre-release window, never forever."""
    async def driver(user):
        await user.open('/help')
        assert not _robots_noindex_present(user), (
            "Help noindex FAIL: /help must be INDEXED once released (REL-01 flip)."
        )
        # Section still renders when released.
        assert 'help-confidence' in _anchor_names(user)

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=False)


def test_help_flag_off_section_absent_and_indexed():
    """State (iii) — discovery unavailable: the section, its TOC entry, and the
    noindex are all absent; the rest of Help is untouched."""
    async def driver(user):
        await user.open('/help')
        anchors = _anchor_names(user)
        assert 'help-confidence' not in anchors, (
            "Help flag-off FAIL: confidence section anchor present with discovery OFF."
        )
        for a in _EXPECTED_BAND_ANCHORS:
            assert a not in anchors, f"Help flag-off FAIL: band anchor {a!r} present with discovery OFF."
        assert _confidence_section_text(user) == '', (
            "Help flag-off FAIL: confidence section rendered with discovery OFF."
        )
        assert not _robots_noindex_present(user), (
            "Help flag-off FAIL: /help must NOT be noindexed when discovery is unavailable."
        )
        # The rest of Help is untouched — a core section still renders.
        assert 'help-api' in anchors, "Help flag-off FAIL: the rest of Help did not render."

    _run_help_smoke(driver, lang='en', discovery_on=False, noindex=False)


# ---------------------------------------------------------------------------
# Test: the discovery_methods_noindex() predicate LOGIC (the REL-01 truth table)
# ---------------------------------------------------------------------------

def test_discovery_methods_noindex_predicate_truth_table():
    """Exercise the REAL predicate (not the render patch): noindex ONLY while
    available AND NOT released — the Phase-139 flip logic itself."""
    import web.discovery as wd

    cases = [
        (True, False, True),    # available, pre-release  -> noindex
        (True, True, False),    # available, released     -> indexed (REL-01 flip)
        (False, False, False),  # unavailable             -> indexed (nothing to hide)
        (False, True, False),   # unavailable + released  -> indexed
    ]
    for available, released, expected in cases:
        with patch.object(wd, 'discovery_available', return_value=available):
            with patch.object(wd, 'DISCOVERY_PUBLIC_RELEASED', released):
                got = wd.discovery_methods_noindex()
                assert got is expected, (
                    f"discovery_methods_noindex() truth-table FAIL: "
                    f"available={available} released={released} -> {got} (expected {expected})"
                )
