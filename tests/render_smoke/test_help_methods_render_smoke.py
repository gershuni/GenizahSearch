# -*- coding: utf-8 -*-
"""Render-smoke test: NiceGUI User drives the live server-render path for /help.

Phase 135, Plan 02 (BAND-05) — rewritten QUALITATIVELY in Phase 136, Plan 02
(D-06a): every precision percentage, confidence interval, weighted estimate
and strata table is STRUCK from the methods section; each band's measurement
is explained in words, and three new qualitative subsections (the two-bucket
rule, known limitations, the novelty check) are added. This suite is also the
FIRST caller of the shared ``tests/render_smoke/discovery_honesty_gate.py``
no-numbers gate that every later Phase-136 surface suite will import.

Scope
-----
Proves what a SERVER-SIDE render of the async ``/help`` route can prove for
the flag-gated "Confidence Bands & Methods" methods section:

  * with ``web.pages.help.discovery_available`` mocked True (the ACTUAL
    body/TOC gate call-site — Codex #11) and the band-precision + per-band
    claim-count wrappers the route awaits patched to fakes (no real sidecar
    needed), the section renders in EN and HE with the section heading, all
    7 per-band deep-link anchors (``help-confidence-<band>``), and the
    BAND-05 field set (population/unit/sample/measurement status/registry);
  * the ``population`` field renders from the RUNTIME DISPLAY-DEDUPLICATED
    shipped-claim count (the fake ``band_counts``), NOT the denominator /
    raw evidence rows / the Wave-4 frame doc;
  * each band's MEASUREMENT STATUS renders qualitatively (never a bare
    percentage or interval) via ``band_measurement_status()``;
  * the four registry fields render placeholder-safe (never fabricated) —
    both the populated case (tier_a) and the not-yet-measured case
    (screening/corroborated/weak/not_evaluated);
  * the three new qualitative subsections (bucket rule / known limitations /
    novelty check) render in both languages, with ``MAIN_POOL_SENTENCE`` as
    the SINGLE source of the bucket-rule wording;
  * the confidence-section anchor set is EXACTLY the section anchor plus the
    7 band anchors — no more, no fewer;
  * both language paths render the SAME field-label set (field parity);
  * the Phase-139 noindex transition (Codex #18): pre-release → noindex,
    released → indexed, flag-off → section absent AND indexed;
  * the D-06/D-06a word gate, run via the SHARED
    ``tests/render_smoke/discovery_honesty_gate.py::assert_discovery_honesty``
    — proven able to fail via two positive controls (a seeded
    precision+interval, and a seeded raw stored vocabulary key);
  * HE renders RTL.

It does NOT exercise the real sidecar or the DiscoveryService — those are
unit-tested in tests/test_discovery_band_labels.py + the discovery-service
suites.

Harness shape mirrors ``test_atlas_render_smoke.py``: a NiceGUI ``User`` over
``httpx.ASGITransport(core.app)``, auto-tagged ``render_smoke`` by
tests/conftest.py. The package conftest imports ``web.main`` at collection
time, registering ``@ui.page('/help')`` on ``core.app``; we clear
``_startup_handlers`` so the real SearchEngine is never built.
"""

from __future__ import annotations

import asyncio
import html
import os
from contextlib import ExitStack, asynccontextmanager
from typing import Callable
from unittest.mock import AsyncMock, patch

import httpx
import pytest

# Import web.main at module load — registers /help on core.app (also done by the
# package conftest; the import is idempotent).
import web.main as _web_main  # noqa: E402, F401
from nicegui import core
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

from tests.render_smoke.discovery_honesty_gate import (
    DiscoveryHonestyScopeError,
    DiscoveryHonestyViolation,
    assert_discovery_honesty,
)


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

# Deliberately covers BOTH the "measured_audit_pending" derivation
# (high_confidence_algorithmic: precision present, no stored status) and the
# "measured_pass" derivation (tier_a: stored status + ci_low above
# STRICT_FLOOR) — the other five bands are absent here, so they exercise the
# {} → band_measurement_status → "not_measured" placeholder path. NOTE:
# `precision`/`ci_low`/`ci_high` are read ONLY to derive the qualitative
# status (band_measurement_status) — no surface renders them as a number.
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
        'precision': None, 'ci_low': 0.9084, 'ci_high': None,
        'numerator': None, 'denominator': None, 'draw_size': None,
        'sampling_frame': None, 'weighting': None,
        'measurement_status': 'measured_pass',
        'measurement_date': '2026-07-28',
        'grader': 'Owner (catalogue-blind)',
        'audit_status': None,
        'report_id': 'CERT-01-2026-07-28',
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


def _scoped_html_fragment(user) -> str:
    """Wrap the confidence section's rendered TEXT in a class-scoped div so
    the SHARED discovery_honesty_gate (which extracts by class name over
    real markup — mandatory scope, per its own module docstring) can run
    over it. The render-smoke harness reads NiceGUI's server-side element
    tree directly (no literal serialized HTML is available from the test
    client for a Vue-reactive page); this produces a faithful
    text-equivalent scoped fragment, which is sufficient because every gate
    check operates on TEXT content, never on markup structure."""
    from web.pages.help import _CONFIDENCE_SECTION_CLASS
    text = _confidence_section_text(user)
    return f'<div class="{_CONFIDENCE_SECTION_CLASS}">{html.escape(text)}</div>'


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
# Tests: section render (EN + HE) — D-06a qualitative content
# ---------------------------------------------------------------------------

def test_help_methods_section_renders_en():
    """EN: the flag-gated methods section renders with the heading, all 7 band
    anchors, the runtime-count population, the qualitative measurement-status
    copy (both the populated and not-yet-measured cases), placeholder-safe
    registry fields, and the three new qualitative subsections. No 'certified'
    anywhere in the section, and no percentage/interval anywhere."""
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

        # measurement status — qualitative, both derivations exercised.
        assert 'graded to completion and passed its pre-registered floor' in section, (
            "Help methods EN FAIL: tier_a measured_pass status copy missing."
        )
        assert 'graded; independent audit pending' in section, (
            "Help methods EN FAIL: high_confidence_algorithmic measured_audit_pending "
            "status copy missing."
        )
        assert 'not yet measured' in section, (
            "Help methods EN FAIL: 'not yet measured' status/placeholder missing "
            "(screening/corroborated/weak/not_evaluated bands have no fixture row)."
        )

        # registry fields — populated case (tier_a) + placeholder case (others).
        assert 'Report identifier' in section, "Help methods EN FAIL: report-id field label missing."
        assert 'CERT-01-2026-07-28' in section, "Help methods EN FAIL: populated report_id missing."
        assert '2026-07-28' in section, "Help methods EN FAIL: populated measurement_date missing."
        assert 'independent audit pending' in section, (
            "Help methods EN FAIL: audit-status placeholder missing."
        )

        # the three new qualitative subsections.
        from web.pages.help import MAIN_POOL_SENTENCE
        assert MAIN_POOL_SENTENCE['en'] in section, (
            "Help methods EN FAIL: the bucket-rule sentence (MAIN_POOL_SENTENCE) is missing."
        )
        assert 'The two-bucket rule' in section, "Help methods EN FAIL: bucket-rule heading missing."
        assert 'Known limitations, stated plainly' in section, (
            "Help methods EN FAIL: known-limitations heading missing."
        )
        assert 'The novelty check' in section, "Help methods EN FAIL: novelty-check heading missing."
        assert 'not a confirmed find' in section, (
            "Help methods EN FAIL: candidate-is-not-a-confirmed-find framing missing."
        )
        assert 'not evidence that a match is correct' in section, (
            "Help methods EN FAIL: absence-is-not-evidence framing missing."
        )

        # the second bucket = insufficient evidence, never "probably wrong".
        assert 'does not mean the match is wrong' in section, (
            "Help methods EN FAIL: second-bucket-meaning sentence missing."
        )
        assert 'not enough evidence for the rule above' in section, (
            "Help methods EN FAIL: second-bucket insufficient-evidence framing missing."
        )

        # known limitations: containment, two-sides-of-one-leaf, dating caveat.
        assert 'can absorb matches that really belong to the' in section, (
            "Help methods EN FAIL: containment limitation missing."
        )
        assert 'a low single-digit share' in section, (
            "Help methods EN FAIL: containment share must be stated in WORDS, not a percentage."
        )
        assert 'two sides of one physical leaf' in section, (
            "Help methods EN FAIL: two-sides-of-one-leaf caveat missing."
        )
        assert 'cannot settle identity by itself' in section, (
            "Help methods EN FAIL: composition-date caveat missing."
        )

        # D-06/WARNING-4 word gate: never 'certified'.
        assert 'certified' not in section.lower(), (
            "Help methods EN FAIL: prohibited word 'certified' present in section."
        )

        # No percentage/interval reachable anywhere in the section (D-06a).
        import re
        assert not re.search(r'\d+(?:\.\d+)?%', section), (
            f"Help methods EN FAIL: a percentage figure is still reachable. Section: {section!r}"
        )
        assert not re.search(r'\[\s*0\.\d+\s*,\s*0\.\d+\s*\]', section), (
            "Help methods EN FAIL: a bracketed confidence interval is still reachable."
        )

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


def test_help_methods_section_renders_he_rtl():
    """HE: the section renders in Hebrew (RTL) with the 7 anchors + the runtime
    population + placeholder registry fields + the three new subsections, and
    NONE of the prohibited HE 'verified/approved/certified' words appear."""
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
        assert 'סווג עד תום ועבר את הסף שנקבע מראש' in section, (
            "Help methods HE FAIL: HE measured_pass status copy missing."
        )

        from web.pages.help import MAIN_POOL_SENTENCE
        assert MAIN_POOL_SENTENCE['he'] in section, (
            "Help methods HE FAIL: the HE bucket-rule sentence is missing."
        )
        assert 'כלל שתי הקבוצות' in section, "Help methods HE FAIL: HE bucket-rule heading missing."
        assert 'מגבלות ידועות' in section, "Help methods HE FAIL: HE known-limitations heading missing."
        assert 'בדיקת החידוש' in section, "Help methods HE FAIL: HE novelty-check heading missing."

        # HE word gate — none of the prohibited review/certification words.
        for forbidden in ('מאומת', 'מאושר', 'מוסמך'):
            assert forbidden not in section, (
                f"Help methods HE FAIL: prohibited HE word {forbidden!r} present in section."
            )
        assert 'certified' not in section.lower(), "Help methods HE FAIL: 'certified' present in section."

        # No percentage/interval reachable in the HE section either.
        import re
        assert not re.search(r'\d+(?:\.\d+)?%', section), (
            f"Help methods HE FAIL: a percentage figure is still reachable. Section: {section!r}"
        )

        # HE renders RTL.
        dirs = _column_directions(user)
        assert 'rtl' in dirs, f"Help methods HE FAIL: no RTL column under HE. directions={dirs}"

    _run_help_smoke(driver, lang='he', discovery_on=True, noindex=True)


def test_help_methods_field_labels_render_in_both_languages():
    """Field parity: both language paths render through the SAME
    _render_confidence_section function, so every field label defined in
    _CONFIDENCE_FIELD_LABELS must appear in BOTH rendered sections, in its
    own language."""
    from web.pages.help import _CONFIDENCE_FIELD_LABELS

    captured = {}

    def _make_driver(lang_key):
        async def driver(user):
            await user.open('/help')
            captured[lang_key] = _confidence_section_text(user)
        return driver

    _run_help_smoke(_make_driver('en'), lang='en', discovery_on=True, noindex=True)
    _run_help_smoke(_make_driver('he'), lang='he', discovery_on=True, noindex=True)

    for field_key, labels in _CONFIDENCE_FIELD_LABELS.items():
        assert labels['en'] in captured['en'], (
            f"Field parity FAIL: EN label for {field_key!r} ({labels['en']!r}) missing "
            "from the EN section."
        )
        assert labels['he'] in captured['he'], (
            f"Field parity FAIL: HE label for {field_key!r} ({labels['he']!r}) missing "
            "from the HE section."
        )


def test_help_methods_confidence_anchor_set_exact():
    """The confidence-section anchor set is EXACTLY the section anchor plus
    the 7 per-band anchors — no more, no fewer. Filtered to the
    'help-confidence' prefix so this does not need to enumerate every other
    anchor on the rest of the Help page."""
    async def driver(user):
        await user.open('/help')
        anchors = _anchor_names(user)
        confidence_anchors = {
            a for a in anchors if a == 'help-confidence' or a.startswith('help-confidence-')
        }
        expected = {'help-confidence', *_EXPECTED_BAND_ANCHORS}
        assert confidence_anchors == expected, (
            f"Confidence anchor set FAIL: got {sorted(confidence_anchors)}, "
            f"expected {sorted(expected)}"
        )

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


# ---------------------------------------------------------------------------
# Tests: the shared discovery_honesty_gate wired against the REAL rendered
# section, in both languages, plus its positive controls (Task 3).
# ---------------------------------------------------------------------------

def test_help_methods_honesty_gate_passes_on_real_render_en():
    """The shared gate does NOT raise over the real EN rendered section — the
    methods page is honest by construction."""
    async def driver(user):
        await user.open('/help')
        fragment = _scoped_html_fragment(user)
        from web.pages.help import _CONFIDENCE_SECTION_CLASS
        assert_discovery_honesty(fragment, scope_selector=_CONFIDENCE_SECTION_CLASS, lang='en')

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


def test_help_methods_honesty_gate_passes_on_real_render_he():
    """The shared gate does NOT raise over the real HE rendered section."""
    async def driver(user):
        await user.open('/help')
        fragment = _scoped_html_fragment(user)
        from web.pages.help import _CONFIDENCE_SECTION_CLASS
        assert_discovery_honesty(fragment, scope_selector=_CONFIDENCE_SECTION_CLASS, lang='he')

    _run_help_smoke(driver, lang='he', discovery_on=True, noindex=True)


def test_help_methods_honesty_gate_positive_control_precision_and_interval():
    """Positive control 1: seed a precision figure PLUS a confidence interval
    into the REAL rendered section and confirm the shared gate raises — a
    green check that cannot fail is worthless. Turns 2 assertions red: the
    unqualified-percentage check AND the bracketed-interval check."""
    async def driver(user):
        await user.open('/help')
        fragment = _scoped_html_fragment(user)
        from web.pages.help import _CONFIDENCE_SECTION_CLASS
        poisoned = fragment.replace(
            '</div>',
            ' estimated band precision 93.8% [0.9084, 0.9644]</div>',
            1,
        )
        with pytest.raises(DiscoveryHonestyViolation) as exc_info:
            assert_discovery_honesty(poisoned, scope_selector=_CONFIDENCE_SECTION_CLASS, lang='en')
        message = str(exc_info.value)
        assert 'unqualified percentage' in message, (
            f"Positive control FAIL: percentage violation not reported. Got: {message}"
        )
        assert 'bracketed interval' in message, (
            f"Positive control FAIL: interval violation not reported. Got: {message}"
        )

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


def test_help_methods_honesty_gate_positive_control_stored_vocab_key():
    """Positive control 2: seed a stored vocabulary key (`direct_witness`)
    into the REAL rendered section and confirm the shared gate catches it.
    Turns 1 assertion red: the raw-vocab-key check."""
    async def driver(user):
        await user.open('/help')
        fragment = _scoped_html_fragment(user)
        from web.pages.help import _CONFIDENCE_SECTION_CLASS
        poisoned = fragment.replace('</div>', ' evidence family: direct_witness</div>', 1)
        with pytest.raises(DiscoveryHonestyViolation) as exc_info:
            assert_discovery_honesty(poisoned, scope_selector=_CONFIDENCE_SECTION_CLASS, lang='en')
        message = str(exc_info.value)
        assert "raw stored vocabulary key 'direct_witness'" in message, (
            f"Positive control FAIL: direct_witness violation not reported. Got: {message}"
        )

    _run_help_smoke(driver, lang='en', discovery_on=True, noindex=True)


# ---------------------------------------------------------------------------
# Tests: the discovery_honesty_gate module's own contract (unit-level, no
# NiceGUI needed) — the mandatory-scope guard, the qualified-coverage
# exception, and the negation-proof word gate.
# ---------------------------------------------------------------------------

def test_discovery_honesty_gate_requires_scope_selector():
    """Calling the gate without a scope_selector, or with one matching
    nothing, raises rather than passing vacuously."""
    with pytest.raises(DiscoveryHonestyScopeError):
        assert_discovery_honesty(
            '<div class="discovery-methods-section">99%</div>', scope_selector='', lang='en'
        )
    with pytest.raises(DiscoveryHonestyScopeError):
        assert_discovery_honesty(
            '<div class="unrelated-class">hello</div>',
            scope_selector='discovery-methods-section',
            lang='en',
        )


def test_discovery_honesty_gate_permits_qualified_coverage_percentage_only():
    """A percentage adjacent to the matched-letter coverage qualifier PASSES;
    a bare percentage (no qualifier) FAILS — the exception cannot become a
    loophole."""
    qualified = (
        '<div class="discovery-methods-section">Matches the work &middot; 68% of page</div>'
    )
    assert_discovery_honesty(qualified, scope_selector='discovery-methods-section', lang='en')

    bare = '<div class="discovery-methods-section">estimated band precision 92.6%</div>'
    with pytest.raises(DiscoveryHonestyViolation):
        assert_discovery_honesty(bare, scope_selector='discovery-methods-section', lang='en')


def test_discovery_honesty_gate_catches_negated_prohibited_wording():
    """A NEGATED use of a prohibited relation word still fails — exactly the
    trap the findings-page sketch fell into ('a match is not proof that a
    folio is a copy of the work')."""
    negated = (
        '<div class="discovery-methods-section">a match is not proof that a folio is a '
        'copy of the work</div>'
    )
    with pytest.raises(DiscoveryHonestyViolation):
        assert_discovery_honesty(negated, scope_selector='discovery-methods-section', lang='en')


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
