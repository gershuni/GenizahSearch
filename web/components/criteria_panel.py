# -*- coding: utf-8 -*-
"""The "Manuscript features" control: a button with a badge, the chips under
it, and the panel it opens (docs/plans/CATALOGUE_CRITERIA_PLAN.md §6.3, D2).

The same control serves Catalog Browse (it lists manuscripts) and "Search
only in..." on /search and /parallels (it restricts a text search). The
page owns the criteria value (``shared/catalog_criteria.py`` shape); the
control reads it with ``get_criteria`` and hands a new one to ``on_apply``,
then redraws its badge and chips with ``refresh()``.

The panel is a window over the page (full screen on a phone): the sections
beside their controls, a live total at the foot with Clear section / Clear
all / Apply. Everything that reads the sidecar runs in ``run.io_bound``.
"""

from __future__ import annotations

import asyncio
import copy
import logging

from nicegui import run, ui

from shared.catalog_criteria import (
    BY_KEY, CRITERIA, NEW_TABLE_KEYS, SECTIONS, clean_criteria,
    criterion_label, section_label, value_label,
)
from web.translations import get_language, is_rtl, tr

logger = logging.getLogger(__name__)

# A list longer than this gets a box to filter its values.
_FILTER_BOX_FROM = 12

_PANEL_CSS = '''
.criteria-panel-card { width: min(1040px, 96vw); max-width: 96vw; height: min(760px, 92vh);
  display: flex; flex-direction: column; padding: 0; }
.criteria-panel-body { flex: 1 1 auto; min-height: 0; display: flex; flex-wrap: nowrap; }
.criteria-panel-nav { width: 220px; flex: 0 0 220px; overflow-y: auto;
  border-inline-end: 1px solid var(--border-color, #e5e7eb); padding: 8px; }
.criteria-panel-controls { flex: 1 1 auto; min-width: 0; overflow-y: auto; padding: 12px 16px; }
.criteria-panel-foot { border-top: 1px solid var(--border-color, #e5e7eb); padding: 8px 16px; }
.criteria-values { max-height: 320px; overflow-y: auto; }
@media (max-width: 639px) {
  .q-dialog__inner:has(.criteria-panel-card) { padding: 0 !important; }
  .criteria-panel-card { width: 100vw; max-width: 100vw; height: 100dvh; max-height: 100dvh;
    border-radius: 0; }
  .criteria-panel-body { flex-direction: column; }
  .criteria-panel-nav { width: 100%; flex: 0 0 auto; display: flex; overflow-x: auto;
    overflow-y: hidden; border-inline-end: none;
    border-bottom: 1px solid var(--border-color, #e5e7eb); }
  .criteria-panel-nav .q-btn { flex: 0 0 auto; }
}
'''

_SOURCE_LABELS = {
    'either': 'Catalogue or measured',
    'catalogue': 'Catalogue only',
    'measured': 'Measured only',
}


def active_criteria_count(criteria) -> int:
    """The badge: how many criteria are set."""
    return len(clean_criteria(criteria, strict=False)[0])


def unsupported_criteria(fjms) -> frozenset:
    """The criteria the open catalogue file is KNOWN not to answer (a 5.0.0
    file: rule 9). A check that cannot run answers nothing is unsupported
    (K-16): the search then fails closed if it must."""
    from shared.fjms_service import FILTER_UNSUPPORTED
    try:
        support = fjms.criteria_support()
    except Exception:
        logger.debug("criteria_support failed", exc_info=True)
        return frozenset()
    return frozenset(NEW_TABLE_KEYS) if support == FILTER_UNSUPPORTED else frozenset()


def drop_unsupported(criteria, fjms) -> tuple[dict, list]:
    """``criteria`` without the ones the open file is known not to answer,
    and the keys dropped (a saved session or an address, rule 9)."""
    bad = unsupported_criteria(fjms)
    kept = {k: v for k, v in (criteria or {}).items() if k not in bad}
    return kept, [k for k in (criteria or {}) if k in bad]


def prewarm_criteria(fjms, meta_mgr, corpus_timeout: float = 300.0) -> bool:
    """Compute the panel's values and "recorded for N" counts once, at server
    start, in a background thread (web/main.py): ~20-30 s over the whole corpus,
    which the first visitor after a restart otherwise waited for section by
    section, 1-8 s each (v9.13.0). They are cached per process with the corpus
    the panel passes (MetadataManager.corpus_sys_ids, one frozenset), and
    GET /api/criteria reads the same cache. Waits for libraries.csv, which
    corpus_sys_ids never gives in part. False with no metadata manager yet;
    raises FilterUnavailable as FjmsService.criteria_vocabulary does."""
    if meta_mgr is None:
        return False
    fjms.criteria_vocabulary(lambda: meta_mgr.corpus_sys_ids(timeout=corpus_timeout))
    return True


def _default_spec(key: str) -> dict:
    criterion = BY_KEY[key]
    spec = {'source': 'either'} if criterion.sources else {}
    if criterion.kind == 'list':
        spec.update(values=[], mode='only')
    else:
        spec.update({'min': None, 'max': None, 'not_recorded': False})
    return spec


def _draft_of(criteria) -> dict:
    """Every criterion as an editable spec (defaults where none is set)."""
    clean, _ = clean_criteria(criteria, strict=False)
    draft = {}
    for criterion in CRITERIA:
        spec = _default_spec(criterion.key)
        spec.update(copy.deepcopy(clean.get(criterion.key, {})))
        draft[criterion.key] = spec
    return draft


def _fmt(n) -> str:
    return f"{n:,}"


class CriteriaControl:
    """The button, its badge and chips, and the panel.

    Args:
        get_criteria: returns the page's current criteria.
        on_apply: async callable taking the new criteria (clean); the page
            stores them, refreshes, and calls ``refresh()`` (or lets this
            control do it: it calls ``refresh()`` after ``on_apply`` returns).
        fjms: returns the FJMS service the page uses.
        universe: returns the corpus (a set or a callable giving it, as
            ``filter_panel.corpus_universe``), or None before it exists.
        mark: prefix for the elements' test marks.
        show_chips: draw the chips under the button. A page with its own
            chip bar passes False and ``on_chips``: it is called with the
            chips (``self.chips``, ``[(key, text)]``) once their text is read,
            and the page draws them, removing one with ``remove(key)``.
    """

    def __init__(self, *, get_criteria, on_apply, fjms, universe, mark: str = 'criteria',
                 show_chips: bool = True, on_chips=None):
        self._get_criteria = get_criteria
        self._on_apply = on_apply
        self._fjms = fjms
        self._universe = universe
        self._mark = mark
        self._show_chips = show_chips
        self._on_chips = on_chips
        self.chips = []
        self._lang = get_language()
        self._rtl = is_rtl()
        self._chip_gen = 0
        self._client = None
        self._root = None
        self.button = None
        self.badge = None
        self.chips_row = None
        # The open panel (None when closed).
        self._dialog = None
        self._opening = False
        self._draft = None
        self._section = SECTIONS[0].key
        self._nav = None
        self._controls = None
        self._total_label = None
        self._note_label = None
        self._total_gen = 0
        self._values = {}       # (key, source) -> list of value dicts
        self._unsupported = frozenset()

    # ── Button, badge, chips ────────────────────────────────────────
    def build(self):
        """Draw the button and the chip row in the current container."""
        ui.add_css(_PANEL_CSS)
        self._client = ui.context.client
        self._root = ui.column().classes('w-full gap-1').mark(f'{self._mark}-control')
        with self._root:
            with ui.row().classes('items-center gap-2'):
                self.button = ui.button(
                    tr('Manuscript features'), icon='tune', on_click=self.open,
                ).props('outline dense no-caps color=primary').classes('text-sm').mark(
                    f'{self._mark}-button')
                with self.button:
                    self.badge = ui.badge('', color='primary').props('floating').mark(
                        f'{self._mark}-badge')
            if self._show_chips:
                self.chips_row = ui.row().classes('w-full gap-1 flex-wrap items-center').mark(
                    f'{self._mark}-chips')
        self.refresh()
        return self

    def refresh(self):
        """Redraw the badge now and the chips as soon as their text is read
        (Hebrew value labels come from the sidecar: off the event loop)."""
        criteria = clean_criteria(self._get_criteria(), strict=False)[0]
        if self.badge is not None:
            n = len(criteria)
            self.badge.set_text(str(n))
            self.badge.set_visibility(n > 0)
        self._chip_gen += 1
        gen = self._chip_gen
        if not criteria:
            self._set_chips([])
            return
        asyncio.ensure_future(self._draw_chips(criteria, gen))

    def _set_chips(self, chips):
        self.chips = list(chips)
        if self._on_chips is not None:
            try:
                self._on_chips(self.chips)
            except Exception:
                logger.warning("criteria on_chips failed", exc_info=True)
        if self.chips_row is not None and not self.chips_row.is_deleted:
            self.chips_row.clear()
            with self.chips_row:
                for key, text in self.chips:
                    with ui.row().classes(
                        'bg-blue-600 text-white rounded-full px-3 py-0.5 items-center gap-1 text-sm'
                    ).mark(f'{self._mark}-chip'):
                        ui.label(text)
                        ui.button(icon='close', on_click=lambda _=None, k=key: self.remove(k)).props(
                            'flat round dense size=xs color=white'
                        ).classes('ml-1').mark(f'{self._mark}-chip-remove')

    async def _draw_chips(self, criteria, gen):
        try:
            chips = await run.io_bound(lambda: self._fjms().criteria_chips(criteria, self._lang))
        except Exception:
            logger.warning("criteria chips failed", exc_info=True)
            chips = None
        if chips is None:  # cancelled or failed: the registry's text, no Hebrew values
            from shared.catalog_criteria import criteria_chips
            chips = criteria_chips(criteria, self._lang)
        if gen != self._chip_gen:
            return
        self._set_chips(chips)

    async def remove(self, key: str):
        """A chip's close button: drop that criterion."""
        criteria = clean_criteria(self._get_criteria(), strict=False)[0]
        criteria.pop(key, None)
        await self._apply(criteria)

    async def clear(self):
        await self._apply({})

    async def _apply(self, criteria):
        # In the control's own column: the Apply button's dialog is deleted
        # when it hides (and a chip when the chips are redrawn) while the
        # page's callback is still awaiting -- a ui call it makes then in
        # the deleted element's slot would raise (Codex round 1 on PR 1b).
        with self._root:
            result = self._on_apply(criteria)
            if asyncio.iscoroutine(result):
                await result
            self.refresh()

    # ── The panel ───────────────────────────────────────────────────
    async def open(self):
        """Open the panel on a copy of the current criteria. A second click
        while it opens, or while it is open, does nothing."""
        if self._opening or self._dialog is not None:
            return
        self._opening = True
        try:
            await self._open()
        finally:
            self._opening = False

    async def _open(self):
        self._draft = _draft_of(self._get_criteria())
        self._values = {}
        try:
            self._unsupported = await run.io_bound(lambda: unsupported_criteria(self._fjms()))
        except Exception:
            self._unsupported = frozenset()
        self._unsupported = self._unsupported or frozenset()
        active = [c.section for c in CRITERIA if c.key in clean_criteria(
            self._draft, strict=False)[0]]
        self._section = active[0] if active else SECTIONS[0].key
        with self._root:
            dialog = self._dialog = ui.dialog().mark(f'{self._mark}-dialog')
        dialog.on('hide', lambda _: self._closed(dialog))
        with self._dialog, ui.card().classes('criteria-panel-card').style(
                f"direction: {'rtl' if self._rtl else 'ltr'}").mark(f'{self._mark}-panel'):
            with ui.row().classes('w-full items-center justify-between px-4 pt-3 pb-2'):
                ui.label(tr('Manuscript features')).classes('text-lg font-bold')
                ui.button(icon='close', on_click=self._dialog.close).props(
                    f'flat round dense aria-label="{tr("Close")}"')
            with ui.element('div').classes('criteria-panel-body'):
                self._nav = ui.element('div').classes('criteria-panel-nav').mark(
                    f'{self._mark}-sections')
                self._controls = ui.element('div').classes('criteria-panel-controls').mark(
                    f'{self._mark}-section-controls')
            with ui.column().classes('criteria-panel-foot w-full gap-1'):
                self._total_label = ui.label('').classes('text-sm font-medium').mark(
                    f'{self._mark}-total')
                self._note_label = ui.label('').classes('text-xs').style(
                    'color: var(--text-muted)').mark(f'{self._mark}-note')
                with ui.row().classes('w-full items-center gap-2 justify-end flex-wrap'):
                    ui.button(tr('Clear section'), on_click=self._clear_section).props(
                        'flat dense no-caps').mark(f'{self._mark}-clear-section')
                    ui.button(tr('Clear all'), on_click=self._clear_draft).props(
                        'flat dense no-caps color=red').mark(f'{self._mark}-clear-all')
                    ui.button(tr('Cancel'), on_click=self._dialog.close).props(
                        'flat dense no-caps')
                    ui.button(tr('Apply'), icon='check', on_click=self._apply_draft).props(
                        'dense no-caps color=primary').mark(f'{self._mark}-apply')
        self._draw_nav()
        self._draw_section()
        self._dialog.open()
        self._schedule_total()
        asyncio.ensure_future(self._load_note())

    def _closed(self, dialog):
        if dialog is self._dialog:
            self._dialog = None
            self._nav = self._controls = self._total_label = self._note_label = None
        if not dialog.is_deleted:
            dialog.delete()

    def _clean_draft(self) -> dict:
        return clean_criteria(self._draft or {}, strict=False)[0]

    def _draw_nav(self):
        if self._nav is None:
            return
        self._nav.clear()
        clean = self._clean_draft()
        with self._nav:
            for section in SECTIONS:
                n = sum(1 for c in CRITERIA if c.section == section.key and c.key in clean)
                text = section_label(section.key, self._lang) + (f" ({n})" if n else '')
                selected = section.key == self._section
                ui.button(text, on_click=lambda _=None, s=section.key: self._pick_section(s)).props(
                    f"flat dense no-caps align={'right' if self._rtl else 'left'} "
                    f"{'color=primary' if selected else 'color=grey-8'}"
                ).classes('w-full text-sm' + (' font-bold' if selected or n else '')).mark(
                    f'{self._mark}-section-{section.key}')

    def _pick_section(self, key: str):
        self._section = key
        self._draw_nav()
        self._draw_section()

    def _draw_section(self):
        if self._controls is None:
            return
        self._controls.clear()
        with self._controls:
            ui.label(section_label(self._section, self._lang)).classes('text-base font-bold mb-2')
            criteria = [c for c in CRITERIA if c.section == self._section]
            if any(c.kind == 'list' and c.key != 'attributes' for c in criteria):
                ui.label(tr('A manuscript matches when any of its catalogue records gives a '
                            'ticked value.')).classes('text-xs mb-2').style('color: var(--text-muted)')
            if self._section == 'measured':
                ui.label(tr('Measured values: every range set here holds on one measured page.')
                         ).classes('text-xs mb-2').style('color: var(--text-muted)')
            for criterion in criteria:
                with ui.column().classes('w-full gap-1 mb-4').mark(f'{self._mark}-crit-{criterion.key}'):
                    if criterion.kind == 'list':
                        self._list_control(criterion)
                    else:
                        self._range_control(criterion)

    # -- shared pieces -------------------------------------------------
    def _header(self, criterion):
        with ui.row().classes('w-full items-baseline gap-2'):
            ui.label(criterion_label(criterion.key, self._lang)).classes('text-sm font-semibold')
            coverage = ui.label('').classes('text-xs').style('color: var(--text-muted)').mark(
                f'{self._mark}-coverage-{criterion.key}')
        if criterion.key in self._unsupported:
            ui.label(tr('This catalogue data is not available on this site yet.')).classes(
                'text-xs').style('color: var(--text-muted)').mark(f'{self._mark}-unavailable')
            return coverage, False
        asyncio.ensure_future(self._load_coverage(criterion.key, coverage))
        return coverage, True

    def _source_select(self, criterion, enabled: bool):
        if not criterion.sources:
            return
        spec = self._draft[criterion.key]

        async def on_source(e):
            spec['source'] = e.value or 'either'
            self._draw_section()  # values and coverage follow the source
            self._changed()
        sel = ui.select({k: tr(v) for k, v in _SOURCE_LABELS.items()}, value=spec.get('source', 'either'),
                        label=tr('Source'), on_change=on_source).props('dense outlined').classes(
            'min-w-48').mark(f'{self._mark}-source-{criterion.key}')
        if not enabled:
            sel.disable()

    async def _load_coverage(self, key, label):
        source = self._draft[key].get('source', 'either') if self._draft else 'either'
        try:
            n = await run.io_bound(
                lambda: self._fjms().get_criterion_coverage(key, self._universe(), source))
            note = await run.io_bound(lambda: self._fjms().criteria_corpus_note(self._universe()))
        except Exception:
            logger.debug("coverage of %s failed", key, exc_info=True)
            return
        if n is None or note is None or label.is_deleted:
            return
        label.set_text(tr('recorded for {n} of {total}').format(
            n=_fmt(n), total=_fmt(note['corpus'])))

    # -- list criteria -------------------------------------------------
    def _list_control(self, criterion):
        key = criterion.key
        spec = self._draft[key]
        _coverage, enabled = self._header(criterion)
        with ui.row().classes('w-full items-center gap-3 flex-wrap'):
            def on_mode(e):
                spec['mode'] = e.value or 'only'
                self._changed()
            mode = ui.toggle({'only': tr('Only these'), 'except': tr('All except these')},
                             value=spec.get('mode', 'only'), on_change=on_mode).props(
                'dense no-caps').mark(f'{self._mark}-mode-{key}')
            if not enabled:
                mode.disable()
            self._source_select(criterion, enabled)
        if key == 'attributes':
            ui.label(tr('Any of the ticked attributes.')).classes('text-xs').style(
                'color: var(--text-muted)')
        if not enabled:
            return
        holder = ui.column().classes('w-full gap-0')
        with holder:
            ui.spinner('dots', size='sm')
        asyncio.ensure_future(self._fill_values(criterion, holder))

    async def _fill_values(self, criterion, holder):
        key = criterion.key
        spec = self._draft[key]
        source = spec.get('source', 'either')
        values = self._values.get((key, source))
        error = None
        if values is None:
            try:
                values = await run.io_bound(
                    lambda: self._fjms().get_criterion_values(key, self._universe(), source))
            except Exception as e:
                logger.warning("criterion values of %s failed", key, exc_info=True)
                error = e
                values = None
            if values is not None:
                self._values[(key, source)] = values
        if values is not None:
            listed = {v['value'] for v in values}
            values = values + [{'value': v, 'label_en': v, 'label_he': '', 'count': 0}
                               for v in spec.get('values', []) if v not in listed]
        if holder.is_deleted:
            return
        holder.clear()
        with holder:
            if values is None:
                reason = getattr(error, 'reason', '')
                ui.label(tr('The list of all manuscripts is still loading. Try again in a moment.')
                         if reason == 'corpus_unavailable'
                         else tr('These values could not be read.')).classes('text-xs').style(
                    'color: var(--text-muted)').mark(f'{self._mark}-values-error')
                return
            rows = []
            if len(values) > _FILTER_BOX_FROM:
                def on_filter(e):
                    q = (e.value or '').strip().lower()
                    for row, text in rows:
                        row.set_visibility(not q or q in text)
                ui.input(placeholder=tr('Filter values'), on_change=on_filter).props(
                    'dense outlined clearable').classes('w-full max-w-sm').mark(
                    f'{self._mark}-filter-{key}')
            with ui.column().classes('w-full gap-0 criteria-values').mark(f'{self._mark}-values-{key}'):
                for entry in values:
                    value = entry['value']
                    shown = (entry.get('label_he') if self._lang == 'he' else entry.get('label_en')
                             ) or value_label(key, value, self._lang)
                    text = f"{shown} ({_fmt(entry['count'])})"

                    def on_tick(e, v=value):
                        chosen = spec.setdefault('values', [])
                        if e.value and v not in chosen:
                            chosen.append(v)
                        elif not e.value and v in chosen:
                            chosen.remove(v)
                        self._changed()
                    box = ui.checkbox(text, value=value in spec.get('values', []),
                                      on_change=on_tick).props('dense').mark(
                        f'{self._mark}-value-{key}')
                    rows.append((box, f"{shown} {value}".lower()))

    # -- range criteria ------------------------------------------------
    def _range_control(self, criterion):
        key = criterion.key
        spec = self._draft[key]
        _coverage, enabled = self._header(criterion)
        self._source_select(criterion, enabled)
        with ui.row().classes('w-full items-center gap-2 flex-wrap'):
            def bound(name):
                def on_change(e):
                    spec[name] = e.value
                    self._changed()
                return on_change
            low = ui.number(label=tr('From'), value=spec.get('min'),
                            precision=0 if criterion.integer else None,
                            on_change=bound('min')).props('dense outlined clearable').classes(
                'w-32').mark(f'{self._mark}-min-{key}')
            ui.label('–')
            high = ui.number(label=tr('To'), value=spec.get('max'),
                             precision=0 if criterion.integer else None,
                             on_change=bound('max')).props('dense outlined clearable').classes(
                'w-32').mark(f'{self._mark}-max-{key}')
            if criterion.unit:
                ui.label(tr(criterion.unit)).classes('text-xs')

            def on_missing(e):
                spec['not_recorded'] = bool(e.value)
                self._changed()
            missing = ui.checkbox(tr('or: not recorded'), value=bool(spec.get('not_recorded')),
                                  on_change=on_missing).props('dense').mark(
                f'{self._mark}-not-recorded-{key}')
        if not enabled:
            for element in (low, high, missing):
                element.disable()
            return
        if key == 'period':
            with ui.row().classes('w-full flex-wrap gap-1'):
                def century(lo, hi):
                    def on_click():
                        low.set_value(lo * 100)
                        high.set_value(hi * 100 + 99)
                    return on_click
                for c in range(9, 15):
                    ui.button(tr(f"{c + 1}th"), on_click=century(c, c)).props(
                        'flat dense size=xs no-caps').classes('text-xs')
                # The 16th-19th centuries: 1500-1899, as "16th" would be 1500-1599
                # (it said 1600-1999 before; GitHub Codex on PR 1b).
                ui.button(tr('16-19'), on_click=century(15, 18)).props(
                    'flat dense size=xs no-caps').classes('text-xs')

    # -- live total ----------------------------------------------------
    def _changed(self):
        self._draw_nav()
        self._schedule_total()

    def _schedule_total(self):
        self._total_gen += 1
        asyncio.ensure_future(self._update_total(self._total_gen))

    async def _update_total(self, gen):
        await asyncio.sleep(0.3)  # typing in a number box: count once it settles
        if gen != self._total_gen or self._total_label is None:
            return
        criteria = self._clean_draft()
        if not criteria:
            self._total_label.set_text(tr('No features chosen.'))
            return
        self._total_label.set_text(tr('Counting...'))
        try:
            total = await run.io_bound(
                lambda: self._fjms().criteria_live_total(criteria, self._universe()))
            error = None
        except Exception as e:
            logger.info("criteria live total failed: %s", e)
            total, error = None, e
        if gen != self._total_gen or self._total_label is None:
            return
        if total is None:
            reason = getattr(error, 'reason', '')
            if reason == 'corpus_unavailable':
                text = tr('The list of all manuscripts is still loading. Try again in a moment.')
            elif reason in ('column_missing', 'sidecar_unavailable'):
                text = tr('This catalogue data is not available on this site yet.')
            else:
                text = tr('The count could not be computed.')
            self._total_label.set_text(text)
            return
        text = tr('{n} manuscripts have these features').format(n=_fmt(total['total'] or 0))
        for key, m in total.get('not_recorded', {}).items():
            text += tr(', of which {m} have no {label} recorded').format(
                m=_fmt(m), label=criterion_label(key, self._lang))
        self._total_label.set_text(text)

    async def _load_note(self):
        try:
            note = await run.io_bound(lambda: self._fjms().criteria_corpus_note(self._universe()))
        except Exception:
            logger.debug("criteria corpus note failed", exc_info=True)
            return
        if note is None or self._note_label is None:
            return
        self._note_label.set_text(tr(
            "These criteria cover all {corpus} manuscripts. {uncatalogued} have no FJMS "
            "catalogue record: they match 'Not recorded', 'all except' and measured data "
            "({sized} also have a catalogue size)."
        ).format(corpus=_fmt(note['corpus']), uncatalogued=_fmt(note['uncatalogued']),
                 sized=_fmt(note['uncatalogued_sized'])))

    # -- foot buttons --------------------------------------------------
    def _clear_section(self):
        for criterion in CRITERIA:
            if criterion.section == self._section:
                self._draft[criterion.key] = _default_spec(criterion.key)
        self._draw_section()
        self._changed()

    def _clear_draft(self):
        self._draft = _draft_of({})
        self._draw_section()
        self._changed()

    async def _apply_draft(self):
        criteria = self._clean_draft()
        if self._dialog is not None:
            self._dialog.close()
        await self._apply(criteria)
