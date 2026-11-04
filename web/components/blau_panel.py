# -*- coding: utf-8 -*-
"""The Blau dictionary companion on /browse, RENDERED (plan §6.2, phase 2).

A reader clicks a word of the transcription; web/static/js/blau_lookup.js finds
the word under the pointer, marks it, places the popover beside it (a sheet at
the foot of a phone) and dispatches ``blau-lookup`` on the popover element;
web/pages/browse.py asks ``shared.blau_service`` and draws the answer here.
This module draws what the service decided and decides nothing itself: no
ranking, no merging, no cap -- those are the service's, and are tested there.

What is always on the panel: the credit line of the owner's ruling (a) --
the site's own Hebrew line on every surface, with the English rendering
beside it in the English UI -- and under it the digital edition's credit to
the MiDRASH project (``MIDRASH_CREDIT``). A result without a credit does not
render.

The reader decides where the dictionary applies (owner, 2026-11-04: the page
does not guess the text's language). A switch in the version row turns the
word lookup on or off, remembered per visitor (``PREF_KEY``, default OFF);
while it is off the pane is not marked, so a click on a word does nothing.
While it is on, a button beside the hint opens the panel with an empty box
(``OPEN_CLASS``: the script handles that click, ``OPEN_EVENT``), and every
panel has the box at its top, holding the word it shows, so a misread word
can be corrected and looked up again.

The panel holds every dictionary the site serves, one tab each (owner,
2026-11-04): Blau's, and Lane's Lexicon (web/components/lane_panel.py, plan
docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md D12). It opens on the tab the
reader last chose while that dictionary has something for the word, else on
the first that has (``pick_tab``); a tab with nothing is greyed, not hidden,
and says so when opened. With one dictionary served there is no tab row, and
the head is titled with its name.

Every class here lives in web/static/common.css under ``.gs-blau``; the root
element carries that class or none of it applies.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from nicegui import ui

from shared.arabic_service import LaneLookup
from shared.blau_service import SITE_URL, BlauCard, BlauLookup, BlauSense
from web.components import lane_panel
from web.safe_storage import safe_user_get, safe_user_set
from web.translations import tr

logger = logging.getLogger(__name__)

#: The visitor's choice: is the word lookup on? (default off).
PREF_KEY = 'ui.blau_lookup'
#: The switch in the version row; ``active`` while the lookup is on.
SWITCH_CLASS = 'blau-switch'
#: The button that opens the panel with an empty box (the script handles it).
OPEN_CLASS = 'blau-open'
OPEN_EVENT = 'blau-open'
#: The hint beside the switch (shown only while the lookup is on).
HINT_CLASS = 'blau-hint'
#: The box at the top of the panel and its search button.
INPUT_CLASS = 'blau-input'
SEARCH_CLASS = 'blau-search'
#: The disclaimer at the top of EVERY panel (owner, 2026-11-04, the Hebrew in the
#: owner's words): a word with no entry is usually an ordinary Arabic word, which
#: Blau does not record, or a verb or inflected form whose base was not found.
DISCLAIMER = ("Blau's dictionary records only meanings particular to Judeo-Arabic. "
              "To look up verbs and inflected forms, type the root or the base form.")
DISCLAIMER_CLASS = 'blau-disclaimer'
#: The digital edition's credit, under the print credit on EVERY panel (owner,
#: 2026-11-04, at Ephraim Meiri's request: the data is the MiDRASH project's
#: digitization). It links to the site's about page, which names the team and
#: the funder in full.
MIDRASH_CREDIT = ("Digital edition: the MiDRASH project, funded by the European Union "
                  "(ERC, MiDRASH, 101071829).")
MIDRASH_CREDIT_CLASS = 'blau-credit-midrash'
ABOUT_URL = SITE_URL + 'about.html'

#: The scope class of the CSS block, and the popover the page creates.
PANEL_CLASS = 'gs-blau'
POPOVER_CLASS = 'gs-blau-popover'
#: The attribute that marks the transcription pane whose words are looked up.
PANE_ATTR = 'data-blau-lookup'
#: The span the script wraps the clicked word in (keeps it highlighted).
HIT_CLASS = 'blau-word-hit'
#: The events the script dispatches on the popover element.
LOOKUP_EVENT = 'blau-lookup'
CLOSE_EVENT = 'blau-close'
#: Marker classes for render-smoke assertions, scoped so a check cannot pass
#: on some other element's text.
CARD_CLASS = 'blau-card'
SENSE_CLASS = 'blau-sense'
EXAMPLES_CLASS = 'blau-examples'
EMPTY_CLASS = 'blau-empty'
CREDIT_CLASS = 'blau-credit'
CLOSE_CLASS = 'blau-close'

#: One tab per dictionary (owner, 2026-11-04: Lane in a tab, not under Blau's
#: cards, so a long entry in one never pushes the other out of sight, and more
#: dictionaries can be added). The tab row sits between the head and the
#: scrolling body; a panel with one dictionary has no tab row.
BLAU_TAB = 'blau'
LANE_TAB = 'lane'
TABS_CLASS = 'dict-tabs'
TAB_CLASS = 'dict-tab'
TAB_PANELS_CLASS = 'dict-tab-panels'
#: The head's title when the panel has tabs; with one dictionary, its name.
TITLE = 'Dictionaries'
BLAU_TITLE = "Blau's dictionary"
TAB_LABELS = {BLAU_TAB: 'Blau', LANE_TAB: 'Lane'}
#: Inside a dictionary's tab: it has nothing for the word.
NOTHING = 'Nothing in the dictionary for this word.'

_JS_PATH = Path(__file__).resolve().parents[1] / 'static' / 'js' / 'blau_lookup.js'

_FLAG_LABELS = (
    ('is_classical', 'classical'),
    ('is_reconstructed', 'reconstructed'),
    ('is_erroneous', 'erroneous'),
)


def lookup_on() -> bool:
    """The visitor's choice (default off: the page never assumes Judeo-Arabic)."""
    return bool(safe_user_get(PREF_KEY, False))


def set_lookup_on(value: bool) -> None:
    safe_user_set(PREF_KEY, bool(value))


def switch_tooltip(on: bool) -> str:
    """What the switch will do when pressed."""
    return tr('Turn off the dictionaries') if on else tr('Turn on the dictionaries (Judeo-Arabic)')


def pick_tab(tabs: Sequence[str], found: Mapping[str, bool], preferred: Optional[str] = None) -> str:
    """The tab a lookup opens on: the one the reader last chose while it has
    something for the word; else the first that has; else the reader's (or
    the first). ``tabs`` is in display order and never empty."""
    if preferred in tabs and found.get(preferred):
        return preferred
    for name in tabs:
        if found.get(name):
            return name
    return preferred if preferred in tabs else tabs[0]


def head_html() -> str:
    """The ``<head>`` line that loads the click script, versioned by its
    content (as web/ux_timing.py does); '' when the file is missing, so a page
    never references a script that 404s."""
    try:
        version = hashlib.sha256(_JS_PATH.read_bytes()).hexdigest()[:10]
    except OSError:
        logger.warning('blau_panel: %s is missing; words are not looked up', _JS_PATH)
        return ''
    return f'<script src="/static/js/blau_lookup.js?v={version}" defer></script>\n'


def lookup_word(args: Any) -> str:
    """The clicked word from the ``blau-lookup`` event's arguments, or ''.

    The script puts a JSON string in ``CustomEvent.detail`` (the shape the
    puzzle page uses: NiceGUI forwards ``detail`` as it is); anything else --
    no detail, a detail that is not JSON, a payload without a string word --
    is ''. Never raises.
    """
    payload = args
    if isinstance(payload, dict) and 'detail' in payload:
        payload = payload['detail']
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return ''
    if not isinstance(payload, dict):
        return ''
    word = payload.get('word')
    return word.strip() if isinstance(word, str) else ''


def _attr(value: Any) -> str:
    return str(value or '').replace('"', "'")


def _rtl(text: str):
    """A Hebrew-letter text, right-to-left whatever the UI direction."""
    return ui.label(text).props('dir="rtl"')


def _gloss_pair(sense: BlauSense, lang: str) -> list[tuple[str, str]]:
    """(text, class) of the glosses to show, the UI language's first."""
    pairs = [(sense.gloss_he, 'gloss he'), (sense.gloss_en, 'gloss en')]
    if lang != 'he':
        pairs.reverse()
    return [(text, cls) for text, cls in pairs if text]


def _render_sense(sense: BlauSense, lang: str) -> None:
    with ui.element('div').classes(f'{SENSE_CLASS} sense' + (' matched' if sense.matched else '')):
        with ui.element('div').classes('head'):
            if sense.ja_text:
                _rtl(sense.ja_text).classes('ja')
            if sense.arabic:
                ui.label(sense.arabic).props('dir="rtl" lang="ar"').classes('ar')
            if sense.binyan:
                ui.label(sense.binyan).classes('binyan').props(f'title="{_attr(tr("binyan"))}"')
            for attr, label in _FLAG_LABELS:
                if getattr(sense, attr):
                    ui.label(tr(label)).classes('flag')
        for text, cls in _gloss_pair(sense, lang):
            ui.label(text).classes(cls).props('dir="auto"')
        if sense.cross_reference or sense.see:
            with ui.element('div').classes('see'):
                ui.label(tr('see')).classes('see-label')
                for name, url in sense.see:
                    ui.link(name, url, new_tab=True).props('dir="rtl"').classes('see-link')
                if not sense.see and sense.cross_reference:
                    ui.label(sense.cross_reference).props('dir="rtl" lang="ar"').classes('ar')


def _render_card(card: BlauCard, lang: str) -> None:
    with ui.element('div').classes(f'{CARD_CLASS} card'):
        with ui.element('div').classes('entry'):
            if card.name:
                _rtl(card.name).classes('name')
            if card.sub_entry and card.sub_entry != card.name:
                ui.label('›').classes('sep')
                _rtl(card.sub_entry).classes('name sub')
            if card.arabic:
                ui.label(card.arabic).props('dir="rtl" lang="ar"').classes('ar')
            if card.page is not None:
                ui.label(f'{tr("p.")} {card.page}').classes('page')
            if card.dot_mismatch:
                ui.label(tr('spelled without the dot')).classes('flag dot-mismatch')
            link = ui.link(tr('Open on the dictionary site'), card.url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr("Open on the dictionary site"))}"')
        if card.via:
            with ui.element('div').classes('via'):
                ui.label(tr('via')).classes('via-label')
                _rtl(card.via)
        if card.matched_key and card.matched_key not in (card.name, card.sub_entry):
            with ui.element('div').classes('matched-key'):
                ui.label(tr('matched')).classes('matched-label')
                _rtl(card.matched_key)
        for sense in card.senses:
            _render_sense(sense, lang)
        if card.more_senses:
            ui.link(f'+{card.more_senses} {tr("more senses on the site")}', card.url,
                    new_tab=True).classes('more')


def _render_examples(result: BlauLookup, lang: str) -> None:
    with ui.element('div').classes(f'{EXAMPLES_CLASS} examples'):
        ui.label(tr('Also in the examples under')).classes('examples-label')
        with ui.element('div').classes('example-links'):
            for example in result.examples:
                link = ui.link(example.name or example.root_id, example.url, new_tab=True).props('dir="rtl"')
                if example.phrase:
                    link.props(f'title="{_attr(example.phrase)}"')
            if result.more_examples:
                ui.label(f'+{result.more_examples}').classes('more')


def _render_credit(credit_he: str, credit_en: str, lang: str) -> None:
    """The credit, as a link to the dictionary site (ruling (a): the credit
    with the site linked, on every panel -- an empty answer has no other
    link to it), then the digital edition's (``MIDRASH_CREDIT``), linked to
    the site's about page."""
    with ui.element('div').classes(f'{CREDIT_CLASS} credit'):
        ui.link(credit_he, SITE_URL, new_tab=True).props('dir="rtl"').classes('credit-he')
        if lang != 'he' and credit_en:
            ui.link(credit_en, SITE_URL, new_tab=True).props('dir="ltr"').classes('credit-en')
        ui.link(tr(MIDRASH_CREDIT), ABOUT_URL, new_tab=True).props(
            'dir="rtl"' if lang == 'he' else 'dir="ltr"').classes(f'{MIDRASH_CREDIT_CLASS} credit-midrash')


def _render_box(word: str, on_search: Callable[[str], Any], focus: bool) -> None:
    """The box at the top of the panel: the word shown, editable; Enter or the
    search button looks up what it holds."""
    box = ui.input(value=word, placeholder=tr("A word in Hebrew or Arabic letters (ד' for ד̇)")).props(
        'dense outlined autocomplete=off spellcheck=false' + (' autofocus' if focus else '')
    ).classes(f'{INPUT_CLASS} word-input')
    box.props(f'aria-label="{_attr(tr("Type a word to look up"))}"')
    box.on('keydown.enter', lambda: on_search(box.value or ''))
    search = ui.button(icon='search', on_click=lambda: on_search(box.value or '')).props(
        f'flat round dense size=sm aria-label="{_attr(tr("Look up"))}"').classes(SEARCH_CLASS)
    search.tooltip(tr('Look up'))


def _render_blau(result: BlauLookup, credit_he: str, credit_en: str, lang: str) -> None:
    """Blau's tab: the disclaimer, the cards and the examples, or what was
    looked for, then the credit (ruling (a): on every panel)."""
    ui.label(tr(DISCLAIMER)).classes(f'{DISCLAIMER_CLASS} disclaimer')
    if result.cards:
        for card in result.cards:
            _render_card(card, lang)
        if result.more_cards:
            ui.label(f'+{result.more_cards} {tr("more entries")}').classes('more')
    if result.examples:
        _render_examples(result, lang)
    if result.word and not result.found:
        with ui.element('div').classes(f'{EMPTY_CLASS} empty'):
            ui.label(tr(NOTHING))
            if result.candidates:
                with ui.element('div').classes('looked-for'):
                    ui.label(tr('Looked for')).classes('looked-label')
                    for candidate in result.candidates:
                        _rtl(candidate).classes('candidate')
    _render_credit(credit_he, credit_en, lang)


def _render_lane(lane: LaneLookup, credit: str, lang: str, on_search: Optional[Callable[[str], Any]],
                 titled: bool) -> None:
    """Lane's tab: its section (web/components/lane_panel.py), or its title and,
    for a word, that nothing was found."""
    if lane_panel.render_lane(lane, credit=credit, lang=lang, on_search=on_search, titled=titled):
        return
    lane_panel.render_lane_empty(lane.word, titled=titled)


def render_lookup(result: Optional[BlauLookup], *, credit_he: str = '', credit_en: str = '', lang: str = 'he',
                  on_close: Optional[Callable[..., Any]] = None,
                  on_search: Optional[Callable[[str], Any]] = None, focus_input: bool = False,
                  lane: Optional[LaneLookup] = None, lane_credit: str = '',
                  tab: Optional[str] = None, on_tab: Optional[Callable[[str], Any]] = None) -> bool:
    """Draw the dictionaries' answers for one word into the current container:
    ``result`` is Blau's (None when Blau is not served), ``lane`` Lane's (None
    when Lane is not served). Each dictionary with its credit line gets a tab
    (ruling (a), D3: no dictionary's text without its credit); without any,
    nothing is drawn and the answer is False. With two, the tab row opens on
    ``pick_tab(..., tab)`` and a tab the reader chooses is reported to
    ``on_tab``. With ``on_search`` the head holds the box (the word in it); a
    lookup with no word is the panel opened to type one."""
    credit_he = (credit_he or '').strip()
    lane_credit = (lane_credit or '').strip()
    word = result.word if result is not None else (lane.word if lane is not None else '')
    tabs: list[str] = []
    if result is not None:
        if credit_he:
            tabs.append(BLAU_TAB)
        else:
            logger.warning('blau_panel: no credit line; the lookup of %r in Blau is not shown', word)
    if lane is not None:
        if lane_credit:
            tabs.append(LANE_TAB)
        else:
            logger.warning('blau_panel: no Perseus line; the lookup of %r in Lane is not shown', word)
    if not tabs:
        return False
    found = {BLAU_TAB: bool(result is not None and result.found),
             LANE_TAB: bool(lane is not None and lane.readings)}
    titles = {BLAU_TAB: BLAU_TITLE, LANE_TAB: lane_panel.TITLE}
    several = len(tabs) > 1

    def draw(name: str) -> None:
        if name == BLAU_TAB:
            _render_blau(result, credit_he, credit_en, lang)
        else:
            _render_lane(lane, lane_credit, lang, on_search, titled=several)

    with ui.element('div').classes('panel-head'):
        ui.label(tr(TITLE if several else titles[tabs[0]])).classes('title')
        if on_search is not None:
            _render_box(word, on_search, focus_input)
        elif word:
            _rtl(word).classes('word')
        close = ui.button(icon='close', on_click=on_close).props(
            f'flat round dense size=sm aria-label="{_attr(tr("Close"))}"').classes(CLOSE_CLASS)
        close.tooltip(tr('Close'))
    if several:
        current = pick_tab(tabs, found, tab)

        def chosen(e: Any) -> None:
            if on_tab is not None and e.value in tabs:
                on_tab(e.value)

        with ui.tabs(value=current, on_change=chosen).props(
                'dense no-caps inline-label narrow-indicator').classes(TABS_CLASS) as tab_row:
            for name in tabs:
                ui.tab(name, label=tr(TAB_LABELS[name])).classes(
                    f'{TAB_CLASS} ' + ('found' if found[name] else 'not-found'))
    with ui.element('div').classes('panel-body'):
        if not word:
            ui.label(tr('Type a Judeo-Arabic word in Hebrew letters, or an Arabic word.')).classes(
                'type-note')
        if not several:
            draw(tabs[0])
        else:
            with ui.tab_panels(tab_row, value=current, animated=False).classes(TAB_PANELS_CLASS):
                for name in tabs:
                    with ui.tab_panel(name):
                        draw(name)
    return True
