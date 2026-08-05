# -*- coding: utf-8 -*-
"""The discovery connections panel, RENDERED (Phase 136, plan 136-17).

This module draws what `shared/discovery_panel_model.py` decided, and takes no
display decision of its own. That split is the whole point: every honesty
invariant on this surface is a property of a pure model plus a renderer that
adds nothing, which is what makes the invariants testable without a browser.

WHAT THIS MODULE MAY NOT DO, stated as prohibitions because each one has a test:

* No bucket, collapse, gating or ordering decision. `shared.discovery_grouping`
  and `shared.discovery_main_pool` already took them and the model already
  called them.
* No band comparison and no band string built here. 136-21 resolves the WEAKER
  band of an expansion pair server-side and supplies its `band_label`; a second
  comparator is exactly how a displayed band drifts from the filtered one.
* No raw `neutral_title` read. Ruling R routes every title through
  `display_work_title`; on `w000176` a raw read prints a halakhic work's name
  over pages the owner ruled are mostly liturgy. The model routes what it
  emits; the ONE title that could arrive here uncurated is on a 136-21
  expansion row, and `_expansion_work_title` routes that through the same
  function rather than formatting it.
* No relation-keyed colour on any chip. Colour-coding by relation kind
  reintroduces per-tier confidence styling through the back door, which is what
  D-24 prohibits.
* No catalogue description anywhere in the panel or its vicinity (D-13i). On a
  composite shelfmark the browse header's catalogue line describes OTHER
  leaves, and read beside a page-level claim the two produce a false alarm: a
  manuscript catalogued as court records carried a verifiably correct
  commentary identification on one folio. This panel therefore renders NO
  catalogue-derived text at all, and `_MANUSCRIPT_PANE_SCOPE_NOTE` says in
  words that the pane describes THIS manuscript's computed identifications.
* No CSS. Every class here is one plan 136-10 landed in
  `web/static/common.css`, scoped under `.gs-discovery`; the root element
  carries that class or none of it applies.

Off-loop discipline: the two LAZY reads (`get_related_pages_enveloped` and
`get_work_expansion_enveloped`) are DIRECT awaits on the `web.discovery` async
wrappers, which dispatch off the loop internally. No `run.io_bound` and no
`web.discovery._service` access appears in this module -- an AST guard pins
both.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from nicegui import ui

import shared.discovery_display_strings as ds
from shared.discovery_panel_model import (
    PANE_EMPTY,
    PANE_OUTAGE,
    PANE_UNRESOLVED,
    ROWS_NOT_REQUESTED,
    ROWS_OUTAGE,
    ROWS_POPULATED,
    PanelModel,
    related_page_row,
)
from shared.discovery_surface_projection import STATUS_OK, is_outage
from web.translations import tr

logger = logging.getLogger(__name__)

#: The panel's own marker classes. Render-smoke assertions scope to these; an
#: assertion that searches the whole page can pass for the wrong reason.
PANEL_ROOT_CLASS = 'discovery-panel'
PANEL_ENTRY_CLASS = 'discovery-panel-entry'
PANEL_ROW_CLASS = 'discovery-panel-row'
PANEL_MANUSCRIPT_PANE_CLASS = 'discovery-panel-manuscript'
PANEL_EXPANSION_CLASS = 'discovery-panel-expansion'
PANEL_RELATED_CLASS = 'discovery-panel-related'
PANEL_SERVICE_STATE_CLASS = 'discovery-panel-service-state'

#: D-13i, implemented as OMISSION plus an explicit scope note. See the module
#: docstring for the false alarm this prevents.
_MANUSCRIPT_PANE_SCOPE_NOTE = {
    'en': 'Computed for this manuscript only. No catalogue description is shown here.',
    'he': 'מחושב עבור כתב יד זה בלבד. לא מוצג כאן תיאור קטלוגי.',
}


def _lang_of(model: PanelModel) -> str:
    return 'he' if model.lang == 'he' else 'en'


def _attr(value: Any) -> str:
    """A value safe to place inside a double-quoted NiceGUI prop."""
    return str(value or '').replace('"', "'")


def _neutral_chip(text: str, tooltip: Optional[str] = None):
    """The relation chip. VISUALLY NEUTRAL, always: the frozen band label rides
    on `title` and never appears as visible text, and no per-kind class is ever
    added (hard rule 2 of the discovery CSS block)."""
    chip = ui.label(text).classes('rel')
    if tooltip:
        chip.props(f'title="{_attr(tooltip)}"')
    return chip


def _service_state_block(state: Mapping[str, Any], on_retry=None) -> None:
    """A named temporary condition plus a retry -- never an empty section.

    An empty section reads as an authoritative zero, which is the exact
    false-zero class the envelope exists to prevent.

    EVERY call site passes `on_retry` -- pinned as an AST guard by
    `test_no_service_state_block_can_be_rendered_without_a_retry_handler`, so a
    new outage branch cannot silently take the default. The round-12 defect was
    exactly that: three of the four outage paths took it and left the reader an
    outage message with no way out.

    Whether a retry is OFFERED is still a decision, but it belongs to the MODEL,
    which supplies a `service_state` only where re-running the reads can help.
    This function's job is to forward the handler it was given.
    """
    with ui.row().classes(f'{PANEL_SERVICE_STATE_CLASS} items-center gap-2 dnote'):
        ui.label(state.get('message') or '')
        if on_retry is not None:
            ui.button(state.get('retry') or '', on_click=on_retry).props(
                'flat dense size=sm no-caps')


# ---------------------------------------------------------------------------
# The entry control (D-13). Visibility is a FIELD on the model, never a
# render-time expression: hidden ONLY on a status of `ok` with a total of zero.
# ---------------------------------------------------------------------------


def render_discovery_entry_control(model: PanelModel, *, on_toggle=None) -> None:
    entry = model.entry_control
    if entry.get('hidden'):
        return
    lang = _lang_of(model)
    count = entry.get('count')
    if entry.get('manuscript_elsewhere_only'):
        # A TRUE zero on this folio while the whole-manuscript read came back
        # `ok` and non-empty. The two facts -- "nothing on this page" and "this
        # page has N" -- are different facts, so they never share a rendering:
        # this state gets its own wording, naming the scope, and no number at
        # all. The MODEL decided the state; the renderer only speaks it.
        label = tr('Computed identifications elsewhere in this manuscript')
    else:
        label = tr('Computed identifications')
        if isinstance(count, int):
            label = f'{label} ({count})'

    with ui.element('span').classes(f'gs-discovery {PANEL_ENTRY_CLASS}'):
        button = ui.button(label, icon='hub', on_click=on_toggle).props(
            'flat dense size=sm no-caps')
        degraded_status = entry.get('degraded_status')
        if degraded_status:
            # An outage ANYWHERE behind this control keeps it VISIBLE and says
            # so. Hiding it would tell the reader this manuscript has nothing on
            # the strength of a query that failed; saying nothing would leave a
            # bare "(0)" standing beside an unknown until the panel is opened.
            # The MODEL names which read failed and how -- the renderer never
            # recombines the claims status with the scope state to decide this,
            # and never substitutes a status of its own.
            button.props('disable=false')
            _neutral_chip(ds.service_state_message(degraded_status, lang))


# ---------------------------------------------------------------------------
# One identification row, in the documented order.
# ---------------------------------------------------------------------------


def _render_vote_placeholders(lang: str) -> None:
    """Inert. Wired in a later phase; they must not imply a vote was recorded,
    so they carry no handler at all and are marked disabled."""
    del lang  # the label is page chrome; the model supplies no vote wording
    with ui.row().classes('items-center gap-1 dnote').style('margin-inline-start: auto;'):
        for icon in ('thumb_up_off_alt', 'thumb_down_off_alt'):
            ui.button(icon=icon).props('flat dense size=sm disable').tooltip(
                tr('Coming soon'))


def _render_expansion(row: Mapping[str, Any], lang: str) -> None:
    """"Other manuscripts matching <work>" -- a DESCRIPTOR until the reader asks.

    The read is issued on open and never with the panel: the heaviest work has
    thousands of claim rows while the median manuscript carries one work, so
    eager loading pays the worst case to serve the common one.
    """
    descriptor = row.get('expansion') or {}
    body = ui.element('div').classes(f'{PANEL_EXPANSION_CLASS} dbody')
    state: Dict[str, Any] = {'open': False, 'page': 1, 'loaded': False}

    async def _load_page() -> None:
        from web import discovery as _discovery
        body.clear()
        try:
            envelope = await _discovery.get_work_expansion_enveloped(
                descriptor.get('work_id'),
                page=state['page'],
                page_size=descriptor.get('page_size'),
                anchor_sys_id=descriptor.get('anchor_sys_id'),
                anchor_claim_type=descriptor.get('anchor_claim_type'),
                anchor_evidence_source=descriptor.get('anchor_evidence_source'),
                anchor_confidence_band=descriptor.get('anchor_confidence_band'),
                lang=lang,
            )
        except Exception as e:  # never let an expansion take the panel down
            logger.error('discovery expansion failed: %s', type(e).__name__)
            return
        state['loaded'] = True
        with body:
            _render_expansion_envelope(
                envelope, lang, state, _load_page,
                page_size=int(descriptor.get('page_size') or 0))

    async def _toggle() -> None:
        state['open'] = not state['open']
        body.style('display: block;' if state['open'] else 'display: none;')
        if state['open'] and not state['loaded']:
            await _load_page()

    ui.button(descriptor.get('heading') or '', on_click=_toggle).props(
        'flat dense size=sm no-caps')
    body.style('display: none;')


def _expansion_work_title(item: Mapping[str, Any], lang: str) -> Optional[str]:
    """Ruling R for a row the MODEL did not curate.

    `SURFACE_EXPANSION_FIELDS` carries no title today, so this normally returns
    None. If one is ever added, it goes through `display_work_title` here rather
    than being formatted -- which is the difference between honouring the
    curation and silently opting out of it.
    """
    raw = item.get('neutral_title')
    if not raw:
        return None
    return ds.display_work_title(
        item.get('display_work_id') or item.get('work_id'), raw, lang) or None


def _render_expansion_envelope(
    envelope: Mapping[str, Any], lang: str, state: Dict[str, Any], reload_cb,
    *, page_size: int = 0,
) -> None:
    if is_outage(envelope):
        _service_state_block({
            'message': ds.service_state_message(envelope.get('status'), lang),
            'retry': ds.retry_label(lang),
        }, on_retry=reload_cb)
        return

    items = list(envelope.get('items') or ())
    # The REAL total, from 136-21's count query. Never `len(items)`: a page
    # length where a count belongs is a number nobody measured. Rendered as a
    # bare figure beside the match-framed heading rather than wrapped in a
    # borrowed sentence -- no display string in the shared vocabulary says
    # "N other manuscripts", and inventing one here would put claim wording in
    # a renderer.
    total = int(envelope.get('total') or 0)
    ui.label(str(total)).classes('chip')

    for item in items:
        with ui.row().classes('items-center gap-2 row'):
            if item.get('display_missing'):
                # An absent manuscript_display row is FLAGGED, never blanked --
                # a blank cell reads as a manuscript with no name rather than as
                # a name we could not resolve.
                ui.label(ds.missing_title(lang)).classes('dnote')
            else:
                ui.label(str(item.get('library_code') or '')).classes('chip')
                ui.label(str(item.get('shelfmark_display') or ''))
            title = _expansion_work_title(item, lang)
            if title:
                ui.label(title)
            band_label = item.get('band_label')
            # BOTH sides' relation kinds when they DIFFER, using the service's
            # own `relations_differ` marker -- never a comparison made here.
            _neutral_chip(ds.relation_chip(item.get('claim_type'), lang), band_label)
            if item.get('relations_differ'):
                _neutral_chip(
                    ds.relation_chip(item.get('anchor_claim_type'), lang), band_label)

    if page_size > 0 and total > page_size:
        with ui.row().classes('items-center gap-2'):
            async def _next() -> None:
                state['page'] = int(state['page']) + 1
                await reload_cb()

            async def _prev() -> None:
                state['page'] = max(1, int(state['page']) - 1)
                await reload_cb()

            ui.button(icon='chevron_left', on_click=_prev).props('flat dense size=sm')
            ui.button(icon='chevron_right', on_click=_next).props('flat dense size=sm')


def _render_identification_row(row: Mapping[str, Any], lang: str) -> None:
    with ui.element('div').classes(f'{PANEL_ROW_CLASS} row'):
        # 1. verb + work title, as PLAIN TEXT. `/work/{id}` does not exist until
        #    Phase 136.1, and a dead link is worse than plain text.
        ui.label(row.get('headline') or '').classes('font-semibold')

        # 2. the meta line, beginning with the relation chip; the band label is
        #    the chip's tooltip and never visible text.
        with ui.row().classes('items-center gap-2 side'):
            _neutral_chip(row.get('relation_chip') or '', row.get('band_tooltip'))
            if row.get('coverage_label'):
                ui.label(row['coverage_label']).classes('dnote')
            ui.label(str(row.get('bucket') or '')).classes('dnote')

        # 3. the optional granularity sub-line (D-13d).
        if row.get('granularity_subline'):
            ui.label(row['granularity_subline']).classes('dnote')

        # 4. the optional low-coverage note. It is about COVERAGE, never review.
        if row.get('low_coverage_note'):
            ui.label(row['low_coverage_note']).classes('dnote')

        # 5. the actions, with the inert vote placeholders at the inline end.
        with ui.row().classes('items-center gap-2 side'):
            _render_expansion(row, lang)
            _render_vote_placeholders(lang)


# ---------------------------------------------------------------------------
# The related-pages section (D-11/D-11a). Header and count by default; the ROWS
# are the LAZY fifth read and are issued by this toggle, never with the panel.
# ---------------------------------------------------------------------------


#: The related-page row's own bilingual copy. TWO strings, and both are about
#: what the row could not resolve rather than about the match itself -- the
#: match framing ("unevaluated candidate alignments") stays on the section
#: label, unchanged.
_RELATED_ROW_COPY = {
    # A manuscript with no `manuscript_display` row. Named, never blanked, and
    # never the composite id: printing the internal identifier as a fallback is
    # exactly how this defect would come back.
    'display_missing': {
        'en': 'Manuscript not in the display index',
        'he': 'כתב היד אינו במפתח התצוגה',
    },
    # The folio, when the id carried one. A bare number beside a shelfmark
    # reads as part of the shelfmark, so the word is not optional.
    'page_number': {
        'en': 'page {number}',
        'he': 'דף {number}',
    },
}


def _render_related_page_row(row: Mapping[str, Any], lang: str) -> None:
    """ONE candidate alignment, in the findings page's own row anatomy: the
    library chip, then the shelfmark as a LINK, then quiet metadata.

    Reusing that anatomy is the point -- it is the vocabulary the corpus-wide
    surface already established (`web/components/findings_rows.py::
    _render_shelfmark`), the `chip` class IS the badge, and a second anatomy for
    the same object is how two surfaces stop looking like one product.

    THE COMPOSITE PAGE ID CANNOT REACH THIS FUNCTION. `related_page_row` does
    not emit it, on either path, so there is nothing here to fall back to and
    nothing to forget: an unresolvable manuscript renders a NAMED state.

    The shelfmark is Latin script inside a Hebrew line. It is its own element in
    a flex row rather than concatenated into a sentence, which is how the
    findings rows already solve the boundary reorder -- not re-solved here.
    """
    with ui.row().classes('items-center gap-2 row flex-wrap'):
        if row.get('display_missing'):
            ui.label(_RELATED_ROW_COPY['display_missing'][lang]).classes('dnote')
        else:
            ui.label(str(row.get('library_code') or '')).classes('chip')
            shelfmark = str(row.get('shelfmark_display') or '')
            sys_id = row.get('sys_id')
            page_number = row.get('page_number')
            # The SAME target the findings row links to, built the same way
            # (`web/components/findings_rows.py::_render_shelfmark`; there is no
            # shared builder to call, and this is not the place to invent a
            # second one). `page` is added when the id carried a folio number --
            # `/browse` takes it as `page: int` (`web/main.py::
            # browse_page_route`), so the link lands on the FOLIO the alignment
            # is about rather than on the manuscript's first page.
            target = f'/browse?sys_id={sys_id}'
            if isinstance(page_number, int):
                target = f'{target}&page={page_number}'
            ui.link(shelfmark, target)
        page_number = row.get('page_number')
        if isinstance(page_number, int):
            ui.label(
                _RELATED_ROW_COPY['page_number'][lang].format(number=page_number)
            ).classes('dnote')


def _render_related_pages(section: Mapping[str, Any], lang: str, page_id: Optional[str],
                          on_retry=None) -> None:
    with ui.element('div').classes(f'{PANEL_RELATED_CLASS} dbody'):
        ui.label(section.get('header') or '').classes('font-semibold')
        # Labelled as UNEVALUATED candidate alignments, always.
        ui.label(section.get('label') or '').classes('dnote')

        if section.get('count_state') == ROWS_OUTAGE:
            # The count is an EAGER read, so its recovery is the seam's retry --
            # the same handler the claims-level outage gets. An outage with no
            # retry is a dead end the reader cannot leave.
            _service_state_block(section.get('count_service_state') or {},
                                 on_retry=on_retry)
        elif section.get('count_line'):
            ui.label(section['count_line']).classes('dnote')

        rows_body = ui.element('div')
        rows_state = section.get('rows_state')
        state: Dict[str, Any] = {'open': False, 'loaded': rows_state != ROWS_NOT_REQUESTED}

        def _paint(envelope: Optional[Mapping[str, Any]]) -> None:
            rows_body.clear()
            with rows_body:
                if envelope is not None and is_outage(envelope):
                    _service_state_block({
                        'message': ds.service_state_message(envelope.get('status'), lang),
                        'retry': ds.retry_label(lang),
                    }, on_retry=_load)
                    return
                # BOTH paths project through the model's own row function: the
                # section's rows were built by it, and a freshly-loaded envelope
                # goes through it here. That is what keeps the eager and the
                # lazy path from drifting -- and what means neither of them ever
                # holds the composite page id.
                if envelope is not None:
                    rows = [related_page_row(item)
                            for item in (envelope.get('items') or ())]
                else:
                    rows = list(section.get('rows') or ())
                for row in rows:
                    _render_related_page_row(row, lang)

        async def _load() -> None:
            from web import discovery as _discovery
            if not page_id:
                return
            try:
                envelope = await _discovery.get_related_pages_enveloped(page_id)
            except Exception as e:
                logger.error('discovery related-pages read failed: %s', type(e).__name__)
                return
            state['loaded'] = True
            _paint(envelope)

        async def _toggle() -> None:
            state['open'] = not state['open']
            rows_body.style('display: block;' if state['open'] else 'display: none;')
            if state['open'] and not state['loaded']:
                await _load()

        ui.button(ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, lang),
                  on_click=_toggle).props('flat dense size=sm no-caps')
        rows_body.style('display: none;')
        if rows_state == ROWS_POPULATED:
            _paint(None)


# ---------------------------------------------------------------------------
# The manuscript pane (D-13h). NAMED works with their page counts -- a bare
# count is what makes a single claim unjudgeable.
# ---------------------------------------------------------------------------


def _render_manuscript_pane(pane: Mapping[str, Any], lang: str, on_retry=None) -> None:
    with ui.element('div').classes(PANEL_MANUSCRIPT_PANE_CLASS):
        ui.label(pane.get('header') or '').classes('font-semibold')
        ui.label(_MANUSCRIPT_PANE_SCOPE_NOTE['he' if lang == 'he' else 'en']).classes('dnote')

        state = pane.get('state')
        if state == PANE_UNRESOLVED:
            # OUR plumbing failed. The pane reports nothing about the manuscript
            # -- not a total, not an empty marker, not a zero. The MODEL decides
            # whether that failure is recoverable: it supplies a `service_state`
            # only for a page-scope OUTAGE, never for a manuscript that has no
            # resolvable page scope, so a retry is offered exactly where it can
            # work.
            unresolved_state = pane.get('service_state')
            if unresolved_state:
                _service_state_block(unresolved_state, on_retry=on_retry)
            else:
                ui.label(ds.service_state_message('unavailable', lang)).classes('dnote')
            return
        if state == PANE_OUTAGE:
            # An EAGER read, so the seam's retry re-issues it.
            _service_state_block(pane.get('service_state') or {}, on_retry=on_retry)
            return
        if pane.get('partial_scope'):
            ui.label(ds.related_pages_label(lang)).classes('dnote')
        if state == PANE_EMPTY:
            return

        works = list(pane.get('works') or ())
        threshold = int(pane.get('page_threshold') or len(works) or 1)
        shown = works if not pane.get('paginated') else works[:threshold]
        with ui.row().classes('items-center gap-1 flex-wrap'):
            for chip in shown:
                classes = 'chip here' if not chip.get('gated') else 'chip gated'
                label = chip.get('work_title') or ''
                count = chip.get('page_count')
                if isinstance(count, int):
                    label = f'{label} ({count})'
                ui.label(label).classes(classes)
        if pane.get('paginated'):
            rest = ui.element('div')
            rest.style('display: none;')
            with rest:
                with ui.row().classes('items-center gap-1 flex-wrap'):
                    for chip in works[threshold:]:
                        classes = 'chip here' if not chip.get('gated') else 'chip gated'
                        label = chip.get('work_title') or ''
                        count = chip.get('page_count')
                        if isinstance(count, int):
                            label = f'{label} ({count})'
                        ui.label(label).classes(classes)
            hidden = {'value': True}

            def _show_rest() -> None:
                hidden['value'] = not hidden['value']
                rest.style('display: none;' if hidden['value'] else 'display: block;')

            ui.button(ds.disclosure_toggle(ds.TOGGLE_MORE_MATCHES, lang),
                      on_click=_show_rest).props('flat dense size=sm no-caps')


# ---------------------------------------------------------------------------
# The panel body.
# ---------------------------------------------------------------------------


def render_discovery_panel_body(
    model: PanelModel, *, on_retry=None, page_id: Optional[str] = None
) -> None:
    lang = _lang_of(model)
    root = ui.element('div').classes(f'gs-discovery {PANEL_ROOT_CLASS} w-full')
    if lang == 'he':
        root.props('dir=rtl')

    with root:
        # --- header + the PERMANENT caveat slot (a designed element between
        # --- header and body, never fine print and never a warning banner).
        with ui.element('div').classes('phead'):
            with ui.row().classes('items-center gap-2'):
                ui.label(tr('Computed identifications')).classes('text-lg font-semibold')
                count = model.entry_control.get('count')
                if isinstance(count, int):
                    ui.label(str(count)).classes('chip')
            ui.label(model.caveat).classes('caveat')
            ui.label(model.bucket_rule_sentence).classes('dnote')

        if model.panel_status != STATUS_OK:
            _service_state_block(model.service_state, on_retry=on_retry)

        # --- the two EVEN panes. Block stack on mobile (page pane FIRST), a
        # --- `1fr 1fr` grid at 900px and above. Both carry equal weight and the
        # --- manuscript pane is NOT collapsed.
        # ---
        # --- ORDER is the model's decision, not one taken here: on the
        # --- `manuscript_elsewhere_only` state the page pane is an empty list
        # --- and the manuscript pane is the whole reason the reader opened the
        # --- panel, so the model leads with it and the stack order follows.
        def _page_pane() -> None:
            with ui.element('div'):
                for level in model.disclosure_levels:
                    _render_level(level, lang, page_id, on_retry=on_retry)

        with ui.element('div').classes('dpanes'):
            if model.lead_with_manuscript_pane:
                _render_manuscript_pane(model.manuscript_pane, lang, on_retry=on_retry)
                _page_pane()
            else:
                _page_pane()
                _render_manuscript_pane(model.manuscript_pane, lang, on_retry=on_retry)


def _render_level(level: Mapping[str, Any], lang: str, page_id: Optional[str],
                  on_retry=None) -> None:
    """Exactly the disclosure levels the model emits -- no more, no fewer."""
    if level.get('default_visible'):
        ui.label(level.get('label') or '').classes('font-semibold')
        for row in level.get('rows') or ():
            _render_identification_row(row, lang)
        return

    # A collapsed level. `notid` is applied only where the model says the level
    # is NOT identifications (D-13e), so the marking follows the model rather
    # than a guess made here.
    classes = 'disc' if level.get('is_identifications') else 'disc notid'
    details = ui.element('details').classes(classes)
    if level.get('visible'):
        details.props('open')
    with details:
        with ui.element('summary').classes('font-semibold'):
            ui.label(level.get('label') or '')
        with ui.element('div').classes('dbody'):
            if level.get('note'):
                ui.label(level['note']).classes('dnote')
            for group in level.get('generic_groups') or ():
                _render_generic_group(group, lang)
            for row in level.get('rows') or ():
                _render_identification_row(row, lang)
            related = level.get('related_pages')
            if related is not None:
                _render_related_pages(related, lang, page_id, on_retry=on_retry)


def _render_generic_group(group: Mapping[str, Any], lang: str) -> None:
    with ui.element('div').classes('row'):
        ui.label(group.get('note') or '').classes('dnote')
        with ui.row().classes('items-center gap-2 flex-wrap'):
            for work in group.get('works') or ():
                ui.label(work.get('work_title') or '').classes('chip')
                _neutral_chip(work.get('relation_chip') or '')
