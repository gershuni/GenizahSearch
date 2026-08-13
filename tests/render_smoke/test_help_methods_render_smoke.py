"""Render-smoke coverage for the practical Atlas and computed-ID help."""

from __future__ import annotations

import asyncio
import html
import os
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import web.main as _web_main  # noqa: F401  -- registers /help on core.app
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

from tests.render_smoke.discovery_honesty_gate import (
    DiscoveryHonestyScopeError,
    DiscoveryHonestyViolation,
    assert_discovery_honesty,
)


@asynccontextmanager
async def _help_user(
    lang: str,
    *,
    discovery_on: bool,
    atlas_on: bool,
):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-tools-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(
                patch('web.pages.help.discovery_available', return_value=discovery_on)
            )
            stack.enter_context(
                patch('web.pages.help.atlas_preview_available', return_value=atlas_on)
            )
            # The route still accepts the legacy report inputs while the old
            # public confidence-band report is retired.
            stack.enter_context(
                patch('web.main.discovery_methods_noindex', return_value=False)
            )
            stack.enter_context(
                patch('web.main.get_all_band_precision', new=AsyncMock(return_value={}))
            )
            stack.enter_context(
                patch('web.main.get_band_claim_counts', new=AsyncMock(return_value={}))
            )
            stack.enter_context(patch('web.main._resolve_ui_language', return_value=lang))
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


def _run(lang, driver, *, discovery_on=True, atlas_on=True):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _help_user(
            lang,
            discovery_on=discovery_on,
            atlas_on=atlas_on,
        ) as user:
            await driver(user)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _marked(elements, marker):
    return [element for element in elements if marker in getattr(element, '_markers', [])]


def _hrefs(elements):
    return {
        str((getattr(element, '_props', None) or {}).get('href'))
        for element in elements
        if (getattr(element, '_props', None) or {}).get('href')
    }


def _anchor_names(elements):
    return {
        str((getattr(element, '_props', None) or {}).get('name'))
        for element in elements
        if (getattr(element, '_props', None) or {}).get('name')
    }


def _computed_id_section_text(user) -> str:
    """All rendered text INSIDE the computed-identifications card only,
    scoped via its unique marker class (``_COMPUTED_ID_SECTION_CLASS``), so
    the honesty gate is not tripped by legitimate copy elsewhere on the Help
    page. Mirrors the pre-merge ``_confidence_section_text`` helper, re-pointed
    at the card that now carries the discovery copy (the BAND-05 methods
    card it used to scope was retired and no longer renders at all)."""
    from web.pages.help import _COMPUTED_ID_SECTION_CLASS
    parts = []
    with user._client:
        for e in user._client.elements.values():
            classes = getattr(e, '_classes', None) or []
            if _COMPUTED_ID_SECTION_CLASS in classes:
                for d in e.descendants(include_self=True):
                    for attr in ('text', 'content'):
                        v = getattr(d, attr, None)
                        if isinstance(v, str) and v:
                            parts.append(v)
    return '\n'.join(parts)


def _computed_id_scoped_html_fragment(user) -> str:
    """Wrap the computed-identifications card's rendered TEXT in a
    class-scoped div so the shared ``discovery_honesty_gate`` (which extracts
    by class name over real markup -- mandatory scope, per its own module
    docstring) can run over it.

    Obtained the same way the pre-merge version of this suite obtained it for
    the BAND-05 card: the NiceGUI ``User`` test client exposes no literal
    serialized HTML for a Vue-reactive page, so this reads the server-side
    element tree directly (``_computed_id_section_text`` above) and wraps the
    concatenated text in a synthetic class-scoped ``<div>``. That is faithful
    for this gate's purposes because every one of its checks operates on TEXT
    content, never on markup structure."""
    from web.pages.help import _COMPUTED_ID_SECTION_CLASS
    text = _computed_id_section_text(user)
    return f'<div class="{_COMPUTED_ID_SECTION_CLASS}">{html.escape(text)}</div>'


def test_help_explains_current_computed_matches_pane_in_english():
    async def driver(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        hrefs = _hrefs(elements)

        assert 'help-computed-identifications' in anchors
        assert len(_marked(elements, 'help-computed-identifications')) == 1
        assert '/computed-identifications' in hrefs
        await user.should_see('Computed Identifications (Beta)')
        await user.should_see('On this page')
        await user.should_see('Show more possible matches')
        await user.should_see('In this manuscript')
        await user.should_see('includes this page')
        await user.should_see('View text match')
        await user.should_see('Does not correspond to the catalogue — not adjudicated')
        await user.should_see('algorithmic suggestions, not identifications reviewed by a scholar')
        await user.should_not_see('Confidence Bands and Methods')

    _run('en', driver)


def test_help_explains_computed_matches_in_hebrew_rtl():
    async def driver(user):
        await user.open('/help')
        elements = _elements(user)
        assert 'help-computed-identifications' in _anchor_names(elements)
        await user.should_see('זיהויים מחושבים (בטא)')
        await user.should_see('בדף זה')
        await user.should_see('הצג התאמות אפשריות נוספות')
        await user.should_see('בכתב־יד זה')
        await user.should_see('כולל דף זה')
        await user.should_see('הצגת התאמת הטקסט')
        await user.should_see('אינו מתאים לקטלוג — לא הוכרע')
        await user.should_not_see('דרגות ודאות ושיטות')

    _run('he', driver)


def test_help_explains_the_visual_atlas_in_both_languages():
    async def english_driver(user):
        await user.open('/help')
        elements = _elements(user)
        assert 'help-visual-atlas' in _anchor_names(elements)
        assert len(_marked(elements, 'help-visual-atlas')) == 1
        assert '/atlas' in _hrefs(elements)
        await user.should_see('The Visual Genizah Atlas')
        await user.should_see('proximity reflects algorithmically calculated textual similarity')
        await user.should_see('not shared physical provenance')

    _run('en', english_driver)

    async def hebrew_driver(user):
        await user.open('/help')
        await user.should_see('אטלס הגניזה החזותי')
        await user.should_see('הקרבה במפה משקפת דמיון טקסטואלי')
        await user.should_see('לא מוצא פיזי משותף')

    _run('he', hebrew_driver)


def test_help_tools_follow_their_independent_readiness_gates():
    async def only_atlas(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        assert 'help-computed-identifications' not in anchors
        assert 'help-visual-atlas' in anchors
        assert '/computed-identifications' not in _hrefs(elements)
        assert '/atlas' in _hrefs(elements)

    _run('en', only_atlas, discovery_on=False, atlas_on=True)

    async def only_computed(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        assert 'help-computed-identifications' in anchors
        assert 'help-visual-atlas' not in anchors
        assert '/computed-identifications' in _hrefs(elements)
        assert '/atlas' not in _hrefs(elements)

    _run('en', only_computed, discovery_on=True, atlas_on=False)

    async def neither(user):
        await user.open('/help')
        anchors = _anchor_names(_elements(user))
        assert 'help-computed-identifications' not in anchors
        assert 'help-visual-atlas' not in anchors
        await user.should_see('Public API & AI Tools')

    _run('en', neither, discovery_on=False, atlas_on=False)


# ---------------------------------------------------------------------------
# Restored (2026-08-13): the retired BAND-05 section (and the ~700-line
# pre-merge version of this suite that exercised it) took seven tests down
# with it that were never about that section -- they are the only coverage
# of the shared discovery_honesty_gate. Group A below is verbatim (the gate's
# own unit contract, no page render needed). Groups B and C re-point the
# pre-merge tests' ``_CONFIDENCE_SECTION_CLASS`` scope -- which no longer
# renders at all -- at ``_COMPUTED_ID_SECTION_CLASS``, the card that now
# carries the discovery copy (``help-computed-identifications``). They use
# ``assert_discovery_honesty`` (the five-detector entry point), never
# ``assert_surface_honesty``: that card encloses the owner-approved D-06a
# limitations paragraph, whose qualitative rate wording legitimately trips the
# sixth (accuracy) detector. That wording is DELIBERATELY not quoted here --
# ``web/pages/help.py::_LIMITATIONS_TEXT`` is its only authority, and a comment
# holding a stale copy of an owner-approved sentence is how the next reader is
# misled about what the exemption covers. The D-06a exemption in
# ``discovery_honesty_gate.D06A_QUALITATIVE_SCOPES`` is bound to the
# PARAGRAPH's class (``discovery-methods-limitations``), not the card's --
# see that module's docstring and its "D-06a's ONE named exception" section.
# ---------------------------------------------------------------------------

def test_help_methods_honesty_gate_passes_on_real_render_en():
    """The shared gate does NOT raise over the real EN rendered
    computed-identifications card -- the practical help copy is honest by
    construction."""
    async def driver(user):
        await user.open('/help')
        fragment = _computed_id_scoped_html_fragment(user)
        from web.pages.help import _COMPUTED_ID_SECTION_CLASS
        assert_discovery_honesty(fragment, scope_selector=_COMPUTED_ID_SECTION_CLASS, lang='en')

    _run('en', driver)


def test_help_methods_honesty_gate_passes_on_real_render_he():
    """The shared gate does NOT raise over the real HE rendered
    computed-identifications card."""
    async def driver(user):
        await user.open('/help')
        fragment = _computed_id_scoped_html_fragment(user)
        from web.pages.help import _COMPUTED_ID_SECTION_CLASS
        assert_discovery_honesty(fragment, scope_selector=_COMPUTED_ID_SECTION_CLASS, lang='he')

    _run('he', driver)


def test_help_methods_honesty_gate_positive_control_precision_and_interval():
    """Positive control 1: seed a precision figure PLUS a confidence interval
    into the REAL rendered computed-identifications card and confirm the
    shared gate raises -- a green check that cannot fail is worthless. Turns
    2 assertions red: the unqualified-percentage check AND the
    bracketed-interval check."""
    async def driver(user):
        await user.open('/help')
        fragment = _computed_id_scoped_html_fragment(user)
        from web.pages.help import _COMPUTED_ID_SECTION_CLASS
        poisoned = fragment.replace(
            '</div>',
            ' estimated band precision 93.8% [0.9084, 0.9644]</div>',
            1,
        )
        with pytest.raises(DiscoveryHonestyViolation) as exc_info:
            assert_discovery_honesty(
                poisoned, scope_selector=_COMPUTED_ID_SECTION_CLASS, lang='en')
        message = str(exc_info.value)
        assert 'unqualified percentage' in message, (
            f"Positive control FAIL: percentage violation not reported. Got: {message}"
        )
        assert 'bracketed interval' in message, (
            f"Positive control FAIL: interval violation not reported. Got: {message}"
        )

    _run('en', driver)


def test_help_methods_honesty_gate_positive_control_stored_vocab_key():
    """Positive control 2: seed a stored vocabulary key (`direct_witness`)
    into the REAL rendered computed-identifications card and confirm the
    shared gate catches it. Turns 1 assertion red: the raw-vocab-key check."""
    async def driver(user):
        await user.open('/help')
        fragment = _computed_id_scoped_html_fragment(user)
        from web.pages.help import _COMPUTED_ID_SECTION_CLASS
        poisoned = fragment.replace('</div>', ' evidence family: direct_witness</div>', 1)
        with pytest.raises(DiscoveryHonestyViolation) as exc_info:
            assert_discovery_honesty(
                poisoned, scope_selector=_COMPUTED_ID_SECTION_CLASS, lang='en')
        message = str(exc_info.value)
        assert "raw stored vocabulary key 'direct_witness'" in message, (
            f"Positive control FAIL: direct_witness violation not reported. Got: {message}"
        )

    _run('en', driver)


# ---------------------------------------------------------------------------
# The discovery_honesty_gate module's own contract (unit-level, no NiceGUI
# needed) -- the mandatory-scope guard, the qualified-coverage exception, and
# the negation-proof word gate. Restored verbatim from the pre-merge suite:
# these never depended on the retired section at all.
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
    a bare percentage (no qualifier) FAILS -- the exception cannot become a
    loophole."""
    qualified = (
        '<div class="discovery-methods-section">Matches the work &middot; 68% of page</div>'
    )
    assert_discovery_honesty(qualified, scope_selector='discovery-methods-section', lang='en')

    bare = '<div class="discovery-methods-section">estimated band precision 92.6%</div>'
    with pytest.raises(DiscoveryHonestyViolation):
        assert_discovery_honesty(bare, scope_selector='discovery-methods-section', lang='en')


def test_discovery_honesty_gate_catches_negated_prohibited_wording():
    """A NEGATED use of a prohibited relation word still fails -- exactly the
    trap the findings-page sketch fell into ('a match is not proof that a
    folio is a copy of the work')."""
    negated = (
        '<div class="discovery-methods-section">a match is not proof that a folio is a '
        'copy of the work</div>'
    )
    with pytest.raises(DiscoveryHonestyViolation):
        assert_discovery_honesty(negated, scope_selector='discovery-methods-section', lang='en')


# ---------------------------------------------------------------------------
# The /computed-identifications PAGE subsection (owner, 2026-08-13). The card
# documented the connections PANE and then linked to the page without saying
# what is on it, so the corpus-wide surface -- the one that carries all the
# controls -- was undocumented.
# ---------------------------------------------------------------------------

def test_help_documents_the_computed_identifications_page_controls_in_english():
    """A DRIFT GUARD, not a content test.

    Every control name asserted here is read from the findings page's OWN
    authority (`copy_text`, `_SORT_LABEL_KEYS`, the rows module's facet header)
    rather than retyped, so renaming a control THERE fails HERE instead of
    leaving the Help page describing a screen that no longer exists. `copy_text`
    raises on an unknown key by design, so a deleted key cannot pass either.

    `Show as` is the one literal: it is an inline `tr("Show as")` argument at the
    `ui.select` call site with no exported constant to pin to.
    """
    from web.components import findings_rows as rows
    from web.pages import findings as fp

    async def driver(user):
        await user.open('/help')
        await user.should_see('The Computed Identifications page')
        for expected in (
            fp.copy_text('novelty_view_label', 'en'),      # "Which findings"
            fp.copy_text('pool_card_header', 'en'),        # "Which pool"
            fp.copy_text('needs_tag', 'en'),               # "not available yet"
            fp._SORT_LABEL_KEYS['band_rank'],              # "Strongest first"
            rows.copy_text('facet_domain_header', 'en'),   # the identified work's domain
        ):
            await user.should_see(expected)
        await user.should_see('Show as')
        await user.should_see('Report a problem')

    _run('en', driver)


def test_help_documents_the_computed_identifications_page_controls_in_hebrew():
    """The Hebrew half asserts the rendered literals rather than pinning to an
    authority: these labels reach the page through `tr()` and the shared
    translation table, so a Hebrew rename is a translation-table edit that this
    file cannot see. Asserting the strings a Hebrew reader actually gets is the
    coverage available here."""
    async def driver(user):
        await user.open('/help')
        await user.should_see('דף הזיהויים המחושבים')
        await user.should_see('אילו ממצאים')       # Which findings
        await user.should_see('באיזה מאגר')        # Which pool
        await user.should_see('הצג כ')             # Show as
        await user.should_see('החזקים תחילה')      # Strongest first
        await user.should_see('תחום החיבור המזוהה')  # Domain of the identified work
        await user.should_see('עדיין לא זמין')      # not available yet
        await user.should_see('דיווח על בעיה')     # Report a problem

    _run('he', driver)
