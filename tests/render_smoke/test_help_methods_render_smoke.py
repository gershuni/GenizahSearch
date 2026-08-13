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
    assert_surface_honesty,
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
        # ⟨REVERSED 2026-08-13, owner⟩ This asserted the BAND-05 methods section
        # was ABSENT. It is back: REL-01 gates the public launch on that report
        # being published and CERT-02 names it as where each tier's unit and
        # status are recorded, so retiring it was not a free choice. Now pinned
        # PRESENT, below this card.
        await user.should_see('Confidence Bands and Methods')

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
        # ⟨REVERSED 2026-08-13, owner⟩ see the English half.
        await user.should_see('דרגות ודאות ושיטות')

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

def _class_scoped_text(user, class_name: str) -> str:
    """All rendered text inside elements carrying ``class_name``. Generic
    counterpart of ``_computed_id_section_text``, for scopes other than the
    computed-identifications card."""
    parts = []
    with user._client:
        for element in user._client.elements.values():
            if class_name in (getattr(element, '_classes', None) or []):
                for node in element.descendants(include_self=True):
                    for attr in ('text', 'content'):
                        value = getattr(node, attr, None)
                        if isinstance(value, str) and value:
                            parts.append(value)
    return '\n'.join(parts)


def _elements_with_class(user, class_name: str):
    with user._client:
        return [e for e in user._client.elements.values()
                if class_name in (getattr(e, '_classes', None) or [])]


# ---------------------------------------------------------------------------
# The BAND-05 methods section, RESTORED 2026-08-13 (owner) after a merge
# stopped it rendering. REL-01 gates the public launch on this report being
# published and CERT-02 names it as where each tier's unit and status are
# recorded, so its absence was a release-gate problem, not a tidy-up.
# ---------------------------------------------------------------------------

def test_the_restored_band_section_renders_in_both_languages_and_stays_gated():
    """Present under both languages, absent when discovery is unavailable.

    The gate matters as much as the presence: the section reports on an
    unreleased artifact, so a flag-off/sidecar-missing environment must not
    advertise it.
    """
    from web.pages.help import _CONFIDENCE_SECTION_CLASS, _CONFIDENCE_TOC_TITLE

    async def english(user):
        await user.open('/help')
        await user.should_see(_CONFIDENCE_TOC_TITLE['en'])
        anchors = _anchor_names(_elements(user))
        assert 'help-confidence' in anchors
        # The per-tier deep-link targets BAND-03's tooltips are specified to
        # link to. They went away with the section; assert they are back.
        assert 'help-confidence-tier_a' in anchors
        assert len(_elements_with_class(user, _CONFIDENCE_SECTION_CLASS)) == 1

    _run('en', english)

    async def hebrew(user):
        await user.open('/help')
        await user.should_see(_CONFIDENCE_TOC_TITLE['he'])
        assert 'help-confidence' in _anchor_names(_elements(user))

    _run('he', hebrew)

    async def gated_off(user):
        await user.open('/help')
        anchors = _anchor_names(_elements(user))
        assert 'help-confidence' not in anchors
        assert 'help-confidence-tier_a' not in anchors
        await user.should_not_see(_CONFIDENCE_TOC_TITLE['en'])

    _run('en', gated_off, discovery_on=False, atlas_on=True)


def test_the_limitations_paragraph_renders_exactly_once_so_its_digest_pin_holds():
    """A SECOND copy of the D-06a paragraph would break the wording pin.

    `LIMITATIONS_TEXT_SHA256` digests the text of EVERY element carrying
    `_LIMITATIONS_PARAGRAPH_CLASS`, so re-rendering the paragraph inside the
    restored methods section -- where it used to live -- silently doubles the
    digested string and fails the pin on wording nobody edited. That is a
    plausible future edit, which is why this is asserted rather than commented.
    """
    from web.pages.help import _LIMITATIONS_PARAGRAPH_CLASS

    async def driver(user):
        await user.open('/help')
        carriers = _elements_with_class(user, _LIMITATIONS_PARAGRAPH_CLASS)
        assert len(carriers) == 1, (
            f'the D-06a limitations paragraph renders {len(carriers)} times; the '
            'digest pin covers all of them concatenated, so more than one breaks '
            'it. It belongs in the practical card only.'
        )

    _run('en', driver)
    _run('he', driver)


def test_an_ungraded_tier_states_that_once_instead_of_four_placeholder_lines():
    """The owner asked for concise. With no measurement in the artifact every
    tier is ungraded, and what used to render was the status line plus FOUR more
    lines each reading "not yet measured" -- repetition that was most of the
    section's length. The status line is unconditional and says it once; the
    sample/date/grader/report/audit fields appear only where a measurement
    exists, so nothing publishable is withheld.
    """
    from web.pages.help import _CONFIDENCE_FIELD_LABELS, _CONFIDENCE_SECTION_CLASS

    async def driver(user):
        await user.open('/help')
        text = _class_scoped_text(user, _CONFIDENCE_SECTION_CLASS)
        fl = _CONFIDENCE_FIELD_LABELS
        assert fl['population']['en'] in text
        assert fl['status']['en'] in text
        assert 'not yet measured' in text
        for withheld in ('sample', 'measurement_date', 'grader', 'report_id',
                         'audit_status'):
            assert fl[withheld]['en'] not in text, (
                f"{fl[withheld]['en']!r} rendered for an UNGRADED tier -- it has "
                'no value, so this is a placeholder line the status already made'
            )
        # The estimand prose is section-level now, so it appears ONCE rather
        # than once per tier.
        assert text.count(fl['unit']['en']) == 1

    _run('en', driver)


def test_the_restored_band_section_passes_the_strict_honesty_gate():
    """SIX detectors, accuracy included -- the strictest scan available, and it
    is available here precisely because the D-06a limitations paragraph is no
    longer inside this section. This is the surface D-06a rewrote, so a
    percentage, an interval or a raw band key leaking back in is the exact
    regression to catch."""
    from web.pages.help import _CONFIDENCE_SECTION_CLASS

    def _drive(lang):
        async def driver(user):
            await user.open('/help')
            text = _class_scoped_text(user, _CONFIDENCE_SECTION_CLASS)
            fragment = (f'<div class="{_CONFIDENCE_SECTION_CLASS}">'
                        f'{html.escape(text)}</div>')
            assert_surface_honesty(
                fragment, scope_selector=_CONFIDENCE_SECTION_CLASS, lang=lang)

        return driver

    _run('en', _drive('en'))
    _run('he', _drive('he'))


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
