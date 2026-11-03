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
beside it in the English UI. A result without a credit does not render.

Every class here lives in web/static/common.css under ``.gs-blau``; the root
element carries that class or none of it applies.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Optional

from nicegui import ui

from shared.blau_service import BlauCard, BlauLookup, BlauSense
from web.translations import tr

logger = logging.getLogger(__name__)

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

_JS_PATH = Path(__file__).resolve().parents[1] / 'static' / 'js' / 'blau_lookup.js'

_FLAG_LABELS = (
    ('is_classical', 'classical'),
    ('is_reconstructed', 'reconstructed'),
    ('is_erroneous', 'erroneous'),
)


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
    with ui.element('div').classes(f'{CREDIT_CLASS} credit'):
        ui.label(credit_he).props('dir="rtl"').classes('credit-he')
        if lang != 'he' and credit_en:
            ui.label(credit_en).props('dir="ltr"').classes('credit-en')


def render_lookup(result: BlauLookup, *, credit_he: str, credit_en: str = '', lang: str = 'he',
                  on_close: Optional[Callable[..., Any]] = None) -> bool:
    """Draw ``result`` into the current container. Returns False, drawing
    nothing, when there is no credit line to show (ruling (a): no panel
    without it)."""
    credit_he = (credit_he or '').strip()
    if not credit_he:
        logger.warning('blau_panel: no credit line; the lookup of %r is not shown', result.word)
        return False
    with ui.element('div').classes('panel-head'):
        ui.label(tr("Blau's dictionary")).classes('title')
        if result.word:
            _rtl(result.word).classes('word')
        close = ui.button(icon='close', on_click=on_close).props(
            f'flat round dense size=sm aria-label="{_attr(tr("Close"))}"').classes(CLOSE_CLASS)
        close.tooltip(tr('Close'))
    with ui.element('div').classes('panel-body'):
        if result.cards:
            for card in result.cards:
                _render_card(card, lang)
            if result.more_cards:
                ui.label(f'+{result.more_cards} {tr("more entries")}').classes('more')
        if result.examples:
            _render_examples(result, lang)
        if not result.found:
            with ui.element('div').classes(f'{EMPTY_CLASS} empty'):
                ui.label(tr('Nothing in the dictionary for this word.'))
                if result.candidates:
                    with ui.element('div').classes('looked-for'):
                        ui.label(tr('Looked for')).classes('looked-label')
                        for candidate in result.candidates:
                            _rtl(candidate).classes('candidate')
        _render_credit(credit_he, credit_en, lang)
    return True
