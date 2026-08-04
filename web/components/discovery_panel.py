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
    label = tr('Computed identifications')
    if isinstance(count, int):
        label = f'{label} ({count})'

    with ui.element('span').classes(f'gs-discovery {PANEL_ENTRY_CLASS}'):
        button = ui.button(label, icon='hub', on_click=on_toggle).props(
            'flat dense size=sm no-caps')
        if entry.get('status') != STATUS_OK:
            # An outage keeps the control VISIBLE and says so. Hiding it here
            # would tell the reader this manuscript has nothing, on the strength
            # of a query that failed.
            button.props('disable=false')
            _neutral_chip(ds.service_state_message(entry.get('status'), lang))


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
            _render_expansion_envelope(envelope, lang, state, _load_page)

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
    envelope: Mapping[str, Any], lang: str, state: Dict[str, Any], reload_cb
) -> None:
    if is_outage(envelope):
        _service_state_block({
            'message': ds.service_state_message(envelope.get('status'), lang),
            'retry': ds.retry_label(lang),
        }, on_retry=reload_cb)
        return

    items = list(envelope.get('items') or ())
    # The REAL total, from 136-21's count query. Never `len(items)`: a page
    # length where a count belongs is a number nobody measured.
    total = envelope.get('total')
    ui.label(ds.related_pages_count_line(int(total or 0), lang)).classes('dnote')

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

    page_size = int(envelope.get('meta', {}).get('page_size') or 0) or len(items) or 1
    if int(total or 0) > page_size:
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


def _render_related_pages(section: Mapping[str, Any], lang: str, page_id: Optional[str]) -> None:
    with ui.element('div').classes(f'{PANEL_RELATED_CLASS} dbody'):
        ui.label(section.get('header') or '').classes('font-semibold')
        # Labelled as UNEVALUATED candidate alignments, always.
        ui.label(section.get('label') or '').classes('dnote')

        if section.get('count_state') == ROWS_OUTAGE:
            _service_state_block(section.get('count_service_state') or {})
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
                items = list((envelope or {}).get('items') or section.get('rows') or ())
                for item in items:
                    ui.label(str(item.get('related_page_id') or '')).classes('dnote')

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


def _render_manuscript_pane(pane: Mapping[str, Any], lang: str) -> None:
    with ui.element('div').classes(PANEL_MANUSCRIPT_PANE_CLASS):
        ui.label(pane.get('header') or '').classes('font-semibold')
        ui.label(_MANUSCRIPT_PANE_SCOPE_NOTE['he' if lang == 'he' else 'en']).classes('dnote')

        state = pane.get('state')
        if state == PANE_UNRESOLVED:
            # OUR plumbing failed. The pane reports nothing about the manuscript
            # -- not a total, not an empty marker, not a zero.
            ui.label(ds.service_state_message('unavailable', lang)).classes('dnote')
            return
        if state == PANE_OUTAGE:
            _service_state_block(pane.get('service_state') or {})
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
        with ui.element('div').classes('dpanes'):
            with ui.element('div'):
                for level in model.disclosure_levels:
                    _render_level(level, lang, page_id)
            _render_manuscript_pane(model.manuscript_pane, lang)


def _render_level(level: Mapping[str, Any], lang: str, page_id: Optional[str]) -> None:
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
                _render_related_pages(related, lang, page_id)


def _render_generic_group(group: Mapping[str, Any], lang: str) -> None:
    with ui.element('div').classes('row'):
        ui.label(group.get('note') or '').classes('dnote')
        with ui.row().classes('items-center gap-2 flex-wrap'):
            for work in group.get('works') or ():
                ui.label(work.get('work_title') or '').classes('chip')
                _neutral_chip(work.get('relation_chip') or '')
