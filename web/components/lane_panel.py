# -*- coding: utf-8 -*-
"""Lane's *Arabic-English Lexicon* on /browse, RENDERED (phase B of
docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md; D12: in its own tab of the dictionary panel).

web/components/blau_panel.py calls ``render_lane`` in Lane's tab of the dictionary popover
(``render_lane_empty`` when Lane has no reading for the word), with what ``shared.arabic_service``
answered for the same word. This module draws that
answer and decides nothing itself: the readings, their order, the root and the entries are the
service's (the plan's method v4, measured by its gate).

What is drawn: the word's Arabic readings as chips, the commonest in the word count first (a
click looks that reading up in the box, so the panel then shows it in Arabic letters); for the
first reading, the root card with Lane's entries (the text cut short, each linked to the entry on
Ephraim Meiri's site, D5); and on every Lane section the Perseus credit line verbatim (D3), then
where the entries and the ranking come from. A section without the credit line does not render.
A word typed in Arabic letters has no readings row (it is its own reading).

Every class here is styled in web/static/common.css under ``.gs-blau`` (the popover's scope).
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from nicegui import ui

from shared.arabic_service import SITE_URL, LaneEntry, LaneLookup, LaneRoot
from web.components import expression_panel, show_more
from web.translations import tr

logger = logging.getLogger(__name__)

#: Marker classes, scoped so a render-smoke check cannot pass on another element's text.
SECTION_CLASS = 'lane-section'
READINGS_CLASS = 'lane-readings'
CHIP_CLASS = 'lane-chip'
ROOT_CLASS = 'lane-root'
ENTRY_CLASS = 'lane-entry'
NO_ROOT_CLASS = 'lane-no-root'
CREDIT_CLASS = 'lane-credit'
#: Lane's tab when Lane has no reading for the word.
EMPTY_SECTION_CLASS = 'lane-none'
#: The entries a root card shows before "Show N more entries", and the button that shows a cut
#: entry's whole text (web/components/show_more.py).
ENTRIES_SHOWN = 3
FULL_CLASS = 'lane-full'
FULL_ENTRY = 'Show the full entry'

TITLE = "Lane's Arabic-English Lexicon"
#: The same words as Blau's tab when it has nothing (one string, one translation).
NOTHING = 'No match was retrieved for this spelling. Try another reading or the root.'
READINGS_LABEL = 'Arabic readings, commonest first'
NO_ROOT = 'No root of this reading was found in Lane.'
#: Where the entries come from (D5) and what orders the readings (D7); the Perseus line is
#: the sidecar's ``meta.lane_credit``, shown verbatim above these.
SOURCE_NOTE = "Lane's entries from semitic.lang.emeiri.org (Ephraim Meiri)."
RANKING_NOTE = 'Readings ranked by a word count of the OpenITI corpus.'
PERSEUS_URL = 'http://www.perseus.tufts.edu/'
OPENITI_URL = 'https://openiti.org/'


def _attr(value: Any) -> str:
    return str(value or '').replace('"', "'")


def _arabic(text: str):
    return ui.label(text).props('dir="rtl" lang="ar"')


def _render_readings(result: LaneLookup, on_search: Optional[Callable[[str], Any]]) -> None:
    with ui.element('div').classes(f'{READINGS_CLASS} readings'):
        ui.label(tr(READINGS_LABEL)).classes('readings-label')
        with ui.element('div').classes('chips'):
            for index, reading in enumerate(result.readings):
                chip = ui.button(reading.spelling).props(
                    'flat dense no-caps dir="rtl" lang="ar"').classes(
                    f'{CHIP_CLASS} chip' + (' active' if index == 0 else ''))
                if index == 0:
                    chip.props('aria-current="true"')
                elif on_search is not None:
                    chip.on_click(lambda _e=None, text=reading.spelling: on_search(text))
                    chip.tooltip(tr('Look up this reading'))


def _render_entry(entry: LaneEntry) -> None:
    with ui.element('div').classes(f'{ENTRY_CLASS} sense' + (' matched' if entry.matched else '')):
        with ui.element('div').classes('head'):
            _arabic(entry.headword).classes('ar headword')
            if entry.page is not None:
                ui.label(f'{tr("p.")} {entry.page}').classes('page')
        labels = {'plural': tr('Plural printed in this entry'), 'verbal_noun': tr('Verbal noun printed in this entry'),
                  'verb_form': tr('Possible grammatical analysis')}
        if entry.match_kind in labels:
            ui.label(f'{labels[entry.match_kind]}: {entry.matched_form}').classes('where dict-evidence')
        text = ui.label(entry.text).props('dir="ltr" lang="en"').classes('gloss lane-text')
        if entry.cut and entry.full:
            # The whole text in place on a click (nothing only on the site).
            def show_full(_e: Any = None, label: ui.label = text, whole: str = entry.full) -> None:
                label.set_text(whole)
                button.delete()

            button = ui.button(tr(FULL_ENTRY), on_click=show_full).props('flat dense no-caps').classes(
                f'{show_more.BUTTON_CLASS} {FULL_CLASS} more')
        elif entry.cut:
            ui.link(tr('full entry'), entry.url, new_tab=True).classes('more')


def _render_root(root: LaneRoot) -> None:
    with ui.element('div').classes(f'{ROOT_CLASS} card'):
        if root.borrowed:
            # A root Blau or Friedman gave for the word (ArabicService.borrow): the line their own
            # tabs put on such a match, with the dictionary and its form on the info mark.
            expression_panel.render_match_details(root)
        elif root.inferred:
            ui.label(tr('Possible grammatical analysis')).classes('where dict-evidence')
        elif not any(entry.matched for entry in root.entries):
            ui.label(tr('Possible root; the word itself was not matched')).classes('where dict-evidence')
        with ui.element('div').classes('entry'):
            ui.label(tr('root')).classes('root-label')
            _arabic(root.name).classes('ar name root-name')
            if root.page is not None:
                ui.label(f'{tr("p.")} {root.page}').classes('page')
            link = ui.link(tr('All entries of this root'), root.url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr("All entries of this root"))}"')
        # The first entries (the matched ones are first); the rest on a click, in place.
        for entry in root.entries[:ENTRIES_SHOWN]:
            _render_entry(entry)
        show_more.render_more(root.entries[ENTRIES_SHOWN:], _render_entry, show_more.MORE_ENTRIES)
        if root.more_entries:   # beyond what a root carries (MAX_ENTRIES): none in Lane's largest root
            ui.link(f'+{root.more_entries} {tr("more entries under this root")}', root.url,
                    new_tab=True).classes('more')


def _render_credit(credit: str, lang: str) -> None:
    """The Perseus line verbatim (D3; English in both UIs, as Perseus words it), then the
    entries' source and the ranking's."""
    with ui.element('div').classes(f'{CREDIT_CLASS} credit'):
        ui.link(credit, PERSEUS_URL, new_tab=True).props('dir="ltr" lang="en"').classes('credit-perseus')
        direction = 'dir="rtl"' if lang == 'he' else 'dir="ltr"'
        ui.link(tr(SOURCE_NOTE), SITE_URL, new_tab=True).props(direction).classes('credit-source')
        ui.link(tr(RANKING_NOTE), OPENITI_URL, new_tab=True).props(direction).classes('credit-ranking')


def render_lane_empty(word: str, *, titled: bool = True) -> None:
    """Lane's tab when Lane has no reading for ``word`` (or no word was typed yet): its title,
    and for a word, that nothing was found."""
    with ui.element('div').classes(f'{EMPTY_SECTION_CLASS} lane'):
        if titled:
            ui.label(tr(TITLE)).classes('lane-title')
        if word:
            ui.label(tr(NOTHING)).classes('empty')


def render_lane(result: Optional[LaneLookup], *, credit: str, lang: str = 'he',
                on_search: Optional[Callable[[str], Any]] = None, titled: bool = True) -> bool:
    """Draw ``result`` into the current container. Returns False, drawing nothing, when there is
    no result, neither a reading nor a root (a root alone is one another dictionary gave for the
    word), or no credit line (D3: no Lane text without it). ``titled=False`` leaves out the
    section's title (the panel's head already names Lane)."""
    if result is None or not (result.readings or result.roots):
        return False
    credit = (credit or '').strip()
    if not credit:
        logger.warning('lane_panel: no credit line; the Lane lookup of %r is not shown', result.word)
        return False
    with ui.element('div').classes(f'{SECTION_CLASS} lane'):
        if titled:
            ui.label(tr(TITLE)).classes('lane-title')
        expression_panel.render_automatic_note()
        if result.script == 'ja' and result.readings:
            _render_readings(result, on_search)
        if result.roots:
            for root in result.roots:
                _render_root(root)
        else:
            ui.label(tr(NO_ROOT)).classes(f'{NO_ROOT_CLASS} empty')
        _render_credit(credit, lang)
    return True
