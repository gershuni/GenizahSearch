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
The switch is the letters עع (``GLYPH_SVG``) and, from 640px, a label
(owner, 2026-11-04: the feature was too hidden behind a grey icon). Until the
visitor first turns it on it is highlighted and says it is new (``NEW_LABEL``;
``SEEN_KEY``); on a phone it is the highlighted letters alone.
While it is on, a button beside the hint opens the panel with an empty box
(``OPEN_CLASS``: the script handles that click, ``OPEN_EVENT``), and every
panel has the box at its top, holding the word it shows, so a misread word
can be corrected and looked up again.

The panel holds every dictionary the site serves, one tab each (owner,
2026-11-04): Blau's, and Lane's Lexicon (web/components/lane_panel.py, plan
docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md D12). A new word opens on the
first dictionary that has something for it (``pick_tab``; no
tab is remembered); an action inside a tab (a stem button, a gap control, a
Lane reading) stays on that tab while it has an answer. A tab chosen opens at
its top. A tab with nothing is greyed, not hidden, and says so when opened.
With one dictionary served there is no tab row, and the head is titled with
its name.

Every class here lives in web/static/common.css under ``.gs-blau``; the root
element carries that class or none of it applies.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from nicegui import ui

from shared.arabic_service import LaneLookup
from shared.blau_service import (
    SCOPE_CITATION, SITE_URL, BlauAnswer, BlauCard, BlauExpression, BlauExpressionHit, BlauFormDetails, BlauLookup, BlauPlace, BlauSense,
)
from shared.friedman_service import FriedmanAnswer, FriedmanExpression, FriedmanLookup
from shared.blau_normalize import evidence_cost
from shared.blau_normalize import normalize_ja
from shared.ja_expression import MAX_CONTEXT_WORDS
from shared.ja_roots import STEM_NUMERALS, root_and_stem, stem_label, stem_query
from web.components import expression_panel, friedman_panel, lane_panel, show_more, stem_buttons
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
#: Has this visitor ever turned the lookup on? Until then the switch is highlighted (``new``)
#: and its label says it is new (owner, 2026-11-04). Remembered per visitor, like ``PREF_KEY``.
SEEN_KEY = 'ui.dictionaries_seen'
NEW_CLASS = 'new'
#: The switch's label (the owner's wording, 2026-11-04), shown from 640px: on a phone the
#: switch is the letters alone (common.css).
NEW_LABEL = 'New! Judeo-Arabic dictionaries!'
LABEL = 'Judeo-Arabic dictionaries'
LABEL_CLASS = 'blau-switch-label'
#: The switch's letters: Hebrew ʿayin beside Arabic ʿayn, "Arabic in Hebrew letters" (owner's
#: choice, 2026-11-04). Drawn as outlines from Noto Sans Hebrew Bold (ע) and Noto Naskh Arabic
#: Bold (ع), both SIL Open Font License, so the switch looks the same on every device and needs
#: no Arabic font. ``currentColor``: the switch's state colours it.
GLYPH_CLASS = 'blau-switch-glyph'
GLYPH_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="-2 -52.3 107.5 84.4" fill="currentColor" '
    'aria-hidden="true" focusable="false"><path d="'
    'M28.4 30.1Q20.6 30.1 14.9 27.3Q9.2 24.5 6.1 19.3Q3 14.1 3 7.1Q3 -3.7 10.5 -11.8'
    'Q7.3 -13.3 4.9 -15.8Q2.4 -18.2 1.2 -20.9Q0 -23.6 0 -26.1Q0 -29.1 1.2 -32.5Q2.3 -36 4.4 -39.2'
    'Q6.4 -42.3 8.9 -44.4Q12.6 -47.5 16.7 -47.5Q20.2 -47.5 24 -45.2Q27.8 -42.9 31.9 -38.2L29.8 -34.2'
    'Q26.8 -35.4 24.2 -36Q21.6 -36.5 19.3 -36.5Q15.8 -36.5 13.1 -35.8Q10.3 -35 7.1 -33.3'
    'Q7.4 -27.9 10.8 -24.3Q14.1 -20.7 19.8 -19.3Q24.2 -21.9 29 -24.1Q33.8 -26.2 39.2 -28'
    'Q40.3 -26.4 40.3 -22.9Q40.3 -20.8 39.5 -19.1Q38.6 -17.5 36.7 -16.2Q35.9 -15.6 34.1 -14.8'
    'Q32.3 -13.9 29.6 -12.8Q20.9 -9.4 16.6 -6.6Q12.3 -3.8 10.2 -0.5Q9.2 1.1 8.7 2.9Q8.1 4.7 8.1 6.5'
    'Q8.1 12.2 13.4 15.4Q18.6 18.6 28.6 18.6Q30.6 18.6 32.7 18.5Q34.8 18.3 37 18Q39.1 17.7 42.8 16.9'
    'Q46.5 16.2 51.7 15L53 19.2Q44.4 24.5 38 27.3Q31.5 30.1 28.4 30.1ZM57 3.1V-7.2L76.7 -11.1'
    'Q79.6 -11.8 81.9 -13.1Q84.2 -14.3 85.8 -16.9Q87.5 -19.4 88.5 -23.8Q89.5 -28.1 90 -34.9'
    'L91 -49.7H103.5L102.5 -34.5Q102.1 -29.5 101.3 -25Q100.6 -20.5 98.9 -16.6Q97.2 -12.8 94.4 -9.6'
    'Q91.6 -6.5 87.2 -4.3Q82.9 -2.1 76.7 -0.8ZM68.8 -4.7 59.5 -48.8 71.7 -50.3 77.8 -20.1'
    'Q78.4 -17.5 78.1 -14.7Q77.8 -11.9 76.6 -9.3Q75.4 -6.8 73.5 -4.7Z'
    '"/></svg>')
#: The box at the top of the panel and its search button.
INPUT_CLASS = 'blau-input'
SEARCH_CLASS = 'blau-search'
#: Beside the box, where Blau's or Friedman's dictionary is served: the verb stem to open with the
#: root in the box. Empty: the box is looked up as typed. Labelled with the
#: panel's existing word for a stem.
STEM_SELECT_CLASS = 'blau-stem-select'
#: The field's list, which Quasar draws outside the popover: the click script must know a choice
#: there is the panel's own (web/static/js/blau_lookup.js, ``STEM_MENU``).
STEM_MENU_CLASS = 'blau-stem-menu'
#: The disclaimer at the top of EVERY panel (owner, 2026-11-04, the Hebrew in the
#: owner's words): a word with no entry is usually an ordinary Arabic word, which
#: Blau does not record, or a verb or inflected form whose base was not found.
#: "mainly", not "only" (owner, 2026-11-05, with the Help text).
DISCLAIMER = ("Blau's dictionary records mainly meanings particular to Judeo-Arabic. "
              "To look up verbs and inflected forms, type the root or the base form.")
DISCLAIMER_CLASS = 'blau-disclaimer'
#: The digital edition's credit, under the print credit on EVERY panel (owner,
#: 2026-11-04): the data is the MiDRASH project's digitization. It links to the
#: site's about page, which names the team and the funder in full.
MIDRASH_CREDIT = ("Digital edition: the MiDRASH project, funded by the European Union "
                  "(ERC, MiDRASH, 101071829).")
MIDRASH_CREDIT_CLASS = 'blau-credit-midrash'
ABOUT_URL = SITE_URL + 'about.html'

#: The scope class of the CSS block, and the popover the page creates.
PANEL_CLASS = 'gs-blau'
POPOVER_CLASS = 'gs-blau-popover'
#: The attribute that marks the transcription pane whose words are looked up.
PANE_ATTR = 'data-blau-lookup'
#: The attribute on the same pane wherever a dictionary is served, the switch on or off: the
#: look-up key works there at any time (owner, 2026-11-05; blau_lookup.js): D or M tapped arms
#: the next click, Alt + a click, selected words and D or M, and on a phone a chip.
KEY_PANE_ATTR = 'data-blau-pane'
#: The phone's chip under selected words: its label, set on the pane in the page's language.
CHIP_LABEL_ATTR = 'data-blau-chip'
CHIP_LABEL = 'Look up in the dictionary'
CHIP_CLASS = 'blau-chip'
#: On <html> while a tapped key waits for its click (the pointer says so over the text).
ARMED_CLASS = 'blau-armed'
#: The switch's tooltip: what pressing it does, and under it the key (owner, 2026-11-05).
TIP_CLASS = 'blau-switch-tip'
KEY_HINT = 'Press D or M, then click a word (or click it with Alt held); or select words and press D or M'
KEY_HINT_CLASS = 'blau-key-hint'
#: How a lookup started (``lookup_how``): a click with the switch on, a click with D or M
#: armed, a click with Alt held, selected words and the key, or the phone's chip.
LOOKUP_HOWS = ('word', 'key', 'alt', 'selection', 'chip')
#: A stem button pressed on a root's card (web/components/stem_buttons.py): the page's own lookup.
STEM_HOW = 'stem'
#: A chip of the other possible roots pressed (stem_buttons.render_suggestions): the page keeps the
#: word's chips while the reader checks them.
SUGGEST_HOW = 'suggestion'
#: The span the script wraps the clicked word in (keeps it highlighted).
HIT_CLASS = 'blau-word-hit'
#: The events the script dispatches on the popover element.
LOOKUP_EVENT = 'blau-lookup'
CLOSE_EVENT = 'blau-close'
#: PostHog events (owner, 2026-11-04: do readers find the switch, and use it?), beside
#: browse_manuscript. The switch: {on, was_new}; a lookup: {how: 'word' (clicked in the text),
#: 'key' (clicked with D or M armed), 'alt' (clicked with Alt), 'selection' (selected words and
#: the key), 'chip' (the phone's chip) or 'box' (typed, or a reading chip), blau / lane: found
#: something, or None when not served};
#: a tab the reader chose: {tab}. The word itself is never sent.
POSTHOG_SWITCH = 'dictionary_switch'
POSTHOG_LOOKUP = 'dictionary_lookup'
POSTHOG_TAB = 'dictionary_tab'
#: Marker classes for render-smoke assertions, scoped so a check cannot pass
#: on some other element's text.
CARD_CLASS = 'blau-card'
SENSE_CLASS = 'blau-sense'
#: The head of a run of senses under a sub-entry (``BlauSense.sub_entry``).
SUB_ENTRY_HEAD_CLASS = 'blau-sub-entry-head'
#: The senses a card shows before "Show N more senses" (web/components/show_more.py).
SENSES_SHOWN = 4
#: The expression search's section in Blau's tab (several words selected or typed; the gap
#: control, the marks and the matched-word pairs are web/components/expression_panel.py's).
EXPRESSION_CLASS = 'blau-expression'
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
#: Friedman's dictionary (the private Friedman plan step 3; web/components/friedman_panel.py):
#: a tab only while its service is available, which it is not by default (a candidate sidecar).
FRIEDMAN_TAB = 'friedman'
TABS_CLASS = 'dict-tabs'
TAB_CLASS = 'dict-tab'
TAB_PANELS_CLASS = 'dict-tab-panels'
#: The scrolling body under the head and the tab row (one for all the tabs).
BODY_CLASS = 'panel-body'
#: The head's title when the panel has tabs; with one dictionary, its name.
TITLE = 'Dictionaries'
BLAU_TITLE = "Blau's dictionary"
TAB_LABELS = {BLAU_TAB: 'Blau', LANE_TAB: 'Lane', FRIEDMAN_TAB: 'Friedman'}
#: Over the entries that hold the word in a citation, a name or an example phrase (Friedman's
#: tab uses the same words over its quotations).
EXAMPLES_LABEL = 'Also in the examples under'
#: A card found by the generated forms of a stem its entry lists (BlauCard.verb_form, PR 3),
#: before the form, the root and the stems. friedman_panel.VERB_FORM (equal by test).
VERB_FORM_CLASS = 'blau-verb-form'
VERB_FORM = 'found by its verb form'
#: Inside Blau's and Friedman's tabs: the dictionary has nothing for the word (step B of the
#: dictionary lookup plan). Lane's tab keeps its own line.
NOTHING = 'No match was retrieved for this spelling. Try another reading or the root.'

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
    """The reader's choice; turning it on also marks the switch as no longer new."""
    safe_user_set(PREF_KEY, bool(value))
    if value:
        safe_user_set(SEEN_KEY, True)


def switch_is_new(on: bool) -> bool:
    """Is the switch still new to this visitor (highlighted, its label saying so)? Yes until
    they first turn it on. A visitor whose lookup is already on has used it, and is marked so
    here: they may have turned it on before the switch could say it was new."""
    if safe_user_get(SEEN_KEY, False):
        return False
    if on:
        safe_user_set(SEEN_KEY, True)
        return False
    return True


def switch_label(new: bool) -> str:
    """The switch's label (shown from 640px)."""
    return tr(NEW_LABEL) if new else tr(LABEL)


def switch_tooltip(on: bool) -> str:
    """What the switch will do when pressed."""
    return tr('Turn off the dictionaries') if on else tr('Turn on the dictionaries (Judeo-Arabic)')


def lookup_event(typed: bool, result: Optional[BlauLookup], lane: Optional[LaneLookup],
                 how: str = 'word', friedman: Optional[FriedmanAnswer] = None) -> dict:
    """The ``POSTHOG_LOOKUP`` properties of one answered lookup (never the word; an expression
    search says how many words, never which; a root with a stem says that a stem was asked)."""
    event = {'how': 'box' if typed else (how if how in (*LOOKUP_HOWS, STEM_HOW, SUGGEST_HOW) else 'word'),
             'blau': None if result is None else bool(result.found),
             'lane': None if lane is None else bool(lane.readings or lane.roots)}
    if lane is not None and any(root.borrowed for root in lane.roots):
        event['lane_borrowed'] = sum(1 for root in lane.roots if root.borrowed)
    if isinstance(result, BlauExpression):
        event['expression'] = len(result.words)
    if isinstance(result, BlauLookup) and any(card.borrowed for card in result.cards):
        event['borrowed'] = sum(1 for card in result.cards if card.borrowed)
    if isinstance(result, BlauLookup) and any(card.verb_form for card in result.cards):
        event['verb_forms'] = sum(1 for card in result.cards if card.verb_form)
    if isinstance(friedman, FriedmanLookup) and any(m.kind for m in friedman.matches):
        event['friedman_verb_forms'] = sum(1 for m in friedman.matches if m.kind)
    suggested = (len(result.suggestions) if isinstance(result, BlauLookup) else 0) + (
        len(friedman.suggestions) if isinstance(friedman, FriedmanLookup) else 0)
    if suggested:
        event['suggestions'] = suggested
    if (isinstance(result, BlauLookup) and result.stem) or (isinstance(friedman, FriedmanLookup) and friedman.stem):
        event['stem'] = True
    if friedman is not None:
        event['friedman'] = bool(friedman.found)
        if isinstance(friedman, FriedmanExpression):
            event['expression'] = len(friedman.words)
    return event


def keep_chips(state: dict, word: str, how: str, result: Optional[BlauAnswer],
               friedman: Optional[FriedmanAnswer]) -> tuple[Optional[BlauAnswer], Optional[FriedmanAnswer], tuple[str, str]]:
    """The chips of the other possible roots, kept while the reader checks them (as
    they were first drawn). ``state`` is the page's memory of them: the word they belong
    to, Blau's and Friedman's chips, and the one shown now. A chip pressed (``SUGGEST_HOW``) or a stem
    button pressed (``STEM_HOW``) while there are chips keeps them: the answers get the word's chips
    again, and what is shown now is marked ('' when it is the word itself). Any other lookup is a new
    word: its own chips, if any, are remembered. Returns the answers to draw and (word, shown now)."""
    if how in (SUGGEST_HOW, STEM_HOW) and state.get('word'):
        state['chosen'] = '' if word == state['word'] else word
        if isinstance(result, BlauLookup):
            result = replace(result, suggestions=state['blau'])
        if isinstance(friedman, FriedmanLookup):
            friedman = replace(friedman, suggestions=state['friedman'])
    else:
        blau = result.suggestions if isinstance(result, BlauLookup) else ()
        own = friedman.suggestions if isinstance(friedman, FriedmanLookup) else ()
        state.update(word=word if blau or own else '', blau=blau, friedman=own, chosen='')
    return result, friedman, (state['word'], state['chosen'])


#: How close a dictionary's answer comes to the clicked word, closest first (the
#: panel opens on the closest answer): an expression the word makes with the words beside it; the
#: word itself (an entry, a form or a head that is the word, Friedman's index listing it, an
#: expression searched as typed); its base form (a clitic peeled); a guessed root (another
#: dictionary's, or by verb form; Lane's reading of it); only the quotations that hold it. Spelled
#: without the word's dot: half a step further. An expression with the words beside it that only
#: quotations hold (``context_quoted``) follows lexical results and remains available separately.
CLOSE_CONTEXT, CLOSE_WORD, CLOSE_BASE, CLOSE_GUESS, CLOSE_QUOTES = range(5)
CLOSE_QUOTED = CLOSE_GUESS + 0.25
_FRIEDMAN_CLOSENESS = {'head': CLOSE_WORD, 'chosen': CLOSE_WORD, 'stem': CLOSE_BASE, 'index': CLOSE_BASE}


def closeness(answer: Any) -> Optional[float]:
    """How close one dictionary's answer comes to the clicked word (``CLOSE_*``), None for an answer
    with nothing. A Blau lookup by its closest card; a Friedman lookup by its first match (Friedman
    orders them by closeness: the word's own head, its base form, then a unit of another root); Lane
    by whether a reading names an entry."""
    if answer is None or not getattr(answer, 'found', False):
        return None
    if isinstance(answer, (BlauExpression, FriedmanExpression)):
        if answer.related and not answer.hits:
            return 0.5 + min(closeness(item) for item in answer.related)
        scopes = {place.scope for hit in answer.hits for place in hit.places}
        if scopes - {'quotation', 'arabic_quotation', 'citation', 'construction'}:
            return CLOSE_WORD - 0.5
        return CLOSE_WORD - 0.25 if 'construction' in scopes else CLOSE_WORD
    if not getattr(answer, 'context', ()):
        return _own_closeness(answer)
    if not getattr(answer, 'context_quoted', False):
        return CLOSE_CONTEXT
    own = _own_closeness(answer)
    contextual = CLOSE_QUOTED
    return contextual if own is None else min(own, contextual)


def _own_closeness(answer: Any) -> Optional[float]:
    """``closeness`` of what the word itself reached, leaving out the words beside it: None for a
    lookup that reached nothing but them."""
    if isinstance(answer, BlauLookup):
        if not (answer.cards or answer.examples or answer.quotes):
            return None
        levels = [(CLOSE_GUESS if card.verb_form or card.borrowed else (CLOSE_WORD + evidence_cost(card.analyses) if card.analyses else
                        CLOSE_WORD if card.candidate == answer.key else CLOSE_BASE))
                  + (0.5 if card.dot_mismatch else 0) for card in answer.cards]
        return min(levels) if levels else CLOSE_QUOTES
    if isinstance(answer, FriedmanLookup):
        if not answer.matches:
            return CLOSE_QUOTES if answer.quotes else None
        first = answer.matches[0]
        return (CLOSE_WORD + evidence_cost(first.analyses) if first.via in {'head', 'stem'} and first.analyses else
                _FRIEDMAN_CLOSENESS.get(first.via, CLOSE_GUESS)) + (0.5 if first.dot_mismatch else 0)
    if isinstance(answer, LaneLookup):
        own = [root for root in answer.roots if not root.borrowed]   # a borrowed root is a guess
        matched = any(entry.matched for root in own for entry in root.entries)
        if not matched:
            return CLOSE_GUESS + 0.5
        inferred = all(root.inferred for root in own)
        return CLOSE_WORD + (1.25 if inferred else .75)
    return CLOSE_QUOTES


def pick_tab(tabs: Sequence[str], found: Mapping[str, bool], preferred: Optional[str] = None,
             close: Optional[Mapping[str, Optional[float]]] = None) -> str:
    """The tab a lookup opens on: ``preferred`` -- the tab an action inside the
    panel came from (a stem button, a gap control, a Lane reading), never one
    remembered from an earlier word -- while it has something
    for the word; else the one whose answer comes closest to the word (``close``: each tab's
    ``closeness``; Friedman's "אטאל אללה בקאה" for אללה in "ואטאל אללה בקאך",
    where Blau has only אללה), the first of equals; else the first that has something; else
    ``preferred`` (or the first). ``tabs`` is in display order and never empty."""
    if preferred in tabs and found.get(preferred):
        return preferred
    ranked = [name for name in tabs if found.get(name) and close and close.get(name) is not None]
    if ranked:
        return min(ranked, key=lambda name: (close[name], tabs.index(name)))
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


def lookup_how(args: Any) -> str:
    """How the script's lookup started (``LOOKUP_HOWS``), from the same payload as
    ``lookup_word``; 'word' for anything else. Never raises."""
    payload = args
    if isinstance(payload, dict) and 'detail' in payload:
        payload = payload['detail']
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return 'word'
    how = payload.get('how') if isinstance(payload, dict) else None
    return how if how in LOOKUP_HOWS else 'word'


#: The longest word the page takes from the script among the words beside a clicked one.
_CONTEXT_WORD_CHARS = 40


def lookup_context(args: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The words beside the clicked word in the text, (before, after), from the same payload as
    ``lookup_word``: at most ``MAX_CONTEXT_WORDS`` a side (the nearest kept), each one word with a
    Hebrew letter (one lookup key: not "מ].נ") and at most ``_CONTEXT_WORD_CHARS`` characters; anything
    else is left out. Never raises."""
    payload = args
    if isinstance(payload, dict) and 'detail' in payload:
        payload = payload['detail']
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return (), ()
    if not isinstance(payload, dict):
        return (), ()

    def words(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [w.strip() for w in value if isinstance(w, str) and w.strip() and ' ' not in w.strip()
                and len(w.strip()) <= _CONTEXT_WORD_CHARS and len(normalize_ja(w).split()) == 1]

    before, after = words(payload.get('before')), words(payload.get('after'))
    return tuple(before[len(before) - MAX_CONTEXT_WORDS:] if len(before) > MAX_CONTEXT_WORDS else before), \
        tuple(after[:MAX_CONTEXT_WORDS])


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


def _render_sense(sense: BlauSense, lang: str, on_details=None) -> None:
    # Where the senses of a sub-entry begin, its name, as the site heads it (קריץ's
    # 'nettle' read as a sense of the verb קרץ); the entry's name where its own senses come back.
    if sense.sub_entry:
        _rtl(sense.sub_entry).classes(SUB_ENTRY_HEAD_CLASS)
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
        if sense.details_available and on_details is not None:
            show_more.render_details(lambda: on_details(sense.form_id), _render_form_details)


def _render_card(card: BlauCard, lang: str, on_stem: Optional[Callable[[str, str], Any]] = None, on_details=None) -> None:
    with ui.element('div').classes(f'{CARD_CLASS} card' + (' stem-chosen' if card.stem else '')):
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
            expression_panel.render_match_details(card)
            if card.dot_mismatch:
                ui.label(tr('spelled without the dot')).classes('flag dot-mismatch')
            link = ui.link(tr('Open on the dictionary site'), card.url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr("Open on the dictionary site"))}"')
        # A card for a root another dictionary gave (BlauCard.borrowed) has no line naming that
        # dictionary: it was redundant. Found by verb form (PR 3): the word's form, the root and the stems it fits.
        if card.verb_form:
            with ui.element('div').classes(f'{VERB_FORM_CLASS} via'):
                ui.label(tr(VERB_FORM)).classes('via-label')
                _rtl(card.verb_form)
        # The stems the entry lists: a press opens that stem's senses.
        stem_buttons.render_stems(card.stems, card.stem, card.root_label, BLAU_TAB, on_stem)
        if card.via:
            with ui.element('div').classes('via'):
                ui.label(tr('via')).classes('via-label')
                _rtl(card.via)
        if card.matched_key and card.matched_key not in (card.name, card.sub_entry):
            with ui.element('div').classes('matched-key'):
                ui.label(tr('matched')).classes('matched-label')
                _rtl(card.matched_key)
        # The first senses; the rest on a click, in place (never only on the site).
        for sense in card.senses[:SENSES_SHOWN]:
            _render_sense(sense, lang, on_details)
        show_more.render_more(card.senses[SENSES_SHOWN:], lambda sense: _render_sense(sense, lang, on_details),
                              show_more.MORE_SENSES)
        if card.more_senses:   # beyond what a card carries (MAX_SENSES): none in release 1.1
            ui.link(f'+{card.more_senses} {tr("more senses on the site")}', card.url,
                    new_tab=True).classes('more')


def _render_examples(result: BlauLookup, lang: str, on_details=None) -> None:
    """The entries that hold the word without being about it: first those whose citations or
    names print it (``quotes``: the place shown, the word marked), then the entries whose
    example phrases are the word (links)."""
    with ui.element('div').classes(f'{EXAMPLES_CLASS} examples'):
        ui.label(tr(EXAMPLES_LABEL)).classes('examples-label')
        for hit in result.quotes:
            _render_expression_hit(hit, lang, on_details)
        if result.more_quotes:
            ui.label(f'+{result.more_quotes} {tr("more entries")}').classes('more')
        if result.examples:
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


def box_query(text: str, stem: Optional[int]) -> str:
    """What the box and the stem beside it look up: the root in the box with the stem chosen after
    it ("דלל" and X: "דלל X"; another root typed later keeps the stem). The text as typed when no
    stem is chosen, when the box is empty, when the text already carries a typed stem ("דלל IV"
    wins over the selector), or when it is several words: an expression search, which a stem
    chosen for an earlier root must not turn into a root lookup ("כתב כטה" after "דלל X")."""
    text = ' '.join((text or '').split())
    if not text or not stem or ' ' in text or root_and_stem(text)[1]:
        return text
    return stem_query(text, int(stem))


def _render_box(word: str, on_search: Callable[[str], Any], focus: bool, stems: bool = False) -> None:
    """The box at the top of the panel: the word shown, editable; Enter or the
    search button looks up what it holds. With ``stems`` (Blau's or Friedman's
    dictionary served), a selector beside it chooses a verb stem: a root with a
    stem shows the root in the box and the stem in the selector, and choosing a
    stem, or clearing it, looks the box up again with it. Several words in the box
    are looked up as typed, whatever the stem (``box_query``)."""
    root, stem = root_and_stem(word) if stems else ('', 0)
    box = ui.input(value=root if stem else word,
                   placeholder=tr("A word in Hebrew or Arabic letters (ד' for ד̇)")).props(
        'dense outlined autocomplete=off spellcheck=false' + (' autofocus' if focus else '')
    ).classes(f'{INPUT_CLASS} word-input')
    box.props(f'aria-label="{_attr(tr("Type a word to look up"))}"')
    chosen: dict[str, Optional[int]] = {'stem': stem or None}

    def look_up() -> None:
        query = box_query(box.value or '', chosen['stem'])
        if query:
            on_search(query)

    box.on('keydown.enter', look_up)
    if stems:
        def choose(e: Any) -> None:
            chosen['stem'] = e.value or None
            look_up()

        select = ui.select({n: stem_label(n) for n in range(1, len(STEM_NUMERALS) + 1)}, value=chosen['stem'],
                           label=tr(stem_buttons.STEMS_LABEL), on_change=choose).props(
            f'dense outlined clearable hide-dropdown-icon options-dense behavior=menu popup-content-class={STEM_MENU_CLASS} '
            f'aria-label="{_attr(tr(stem_buttons.STEMS_LABEL))}"'
        ).classes(f'{STEM_SELECT_CLASS} stem-select')
        select.tooltip(tr(stem_buttons.STEMS_LABEL))
    search = ui.button(icon='search', on_click=look_up).props(
        f'flat round dense size=sm aria-label="{_attr(tr("Look up"))}"').classes(SEARCH_CLASS)
    search.tooltip(tr('Look up'))


def _place_glosses(place: BlauPlace, lang: str) -> list[tuple[str, str]]:
    pairs = [(place.gloss_he, 'gloss he'), (place.gloss_en, 'gloss en')]
    if lang != 'he':
        pairs.reverse()
    return [(text, cls) for text, cls in pairs if text]


def _citation_locator(quotation: Any) -> str:
    """The citation as the dictionary prints it, with its source work in front when the
    citation does not name it (an "ibid." citation is a bare page)."""
    locator, source = quotation.locator, quotation.source
    if source and source not in locator:
        return f'{source}, {locator}' if locator else source
    return locator


def _render_form_details(details: BlauFormDetails, shown: frozenset[str] = frozenset()) -> None:
    citations = tuple(c for c in details.citations if c.id not in shown or c.commentary or c.bibliography)
    if not details.notes and not details.classical_note and not citations:
        ui.label(tr('No additional examples or notes for this sense.')).classes('more')
        return
    for text, cls in ((details.notes, 'notes'), (details.classical_note, 'classical-note')):
        if text:
            ui.label(text).props('dir="auto"').classes(cls)
    for citation in citations:
        with ui.element('div').classes('quotation'):
            # A matching quotation is already visible above; its commentary and full
            # bibliography may still supply information the excerpt did not carry.
            if citation.id not in shown:
                if citation.quote:
                    _rtl(citation.quote).classes('ja')
                if citation.translation:
                    ui.label(citation.translation).props('dir="auto"').classes('gloss he')
                locator = _citation_locator(citation)
                if locator:
                    _rtl(locator).classes('locator')
            if citation.commentary:
                ui.label(citation.commentary).props('dir="auto"').classes('commentary')
            if citation.bibliography:
                ui.label(citation.bibliography).props('dir="auto"').classes('references')


def _render_expression_place(place: BlauPlace, lang: str, on_details=None,
                             shown: frozenset[str] = frozenset()) -> None:
    """One citation (its words, the matched ones marked, its translation, the form it
    illustrates with its gloss, the locator) or one name of the entry (marked, with the form's
    gloss); then, when a word was reached by a clitic alternative, which printed word carried it."""
    with ui.element('div').classes(f'{expression_panel.PLACE_CLASS} place'):
        if place.scope == SCOPE_CITATION and place.quotation is not None:
            with ui.element('div').classes('quotation'):
                expression_panel.render_marked(place.words, place.positions, 'ja')
                if place.quotation.translation:
                    ui.label(place.quotation.translation).props('dir="auto"').classes('gloss he')
                if place.form_text or place.gloss_he or place.gloss_en:
                    with ui.element('div').classes('under'):
                        ui.label(tr(friedman_panel.UNDER)).classes('matched-label')
                        if place.form_text:
                            _rtl(place.form_text).classes('form-text')
                        for text, cls in _place_glosses(place, lang):
                            ui.label(text).classes(cls).props('dir="auto"')
                locator = _citation_locator(place.quotation)
                if locator:
                    _rtl(locator).classes('locator')
        else:
            ui.label(tr('Construction with inherited verb') if place.scope == 'construction' else tr(friedman_panel.IN_HEAD)).classes('where')
            if place.binyan:
                ui.label(place.binyan).classes('binyan')
            expression_panel.render_marked(place.words, place.positions, 'ja form-text')
            for text, cls in _place_glosses(place, lang):
                ui.label(text).classes(cls).props('dir="auto"')
        expression_panel.render_components(place)
        if place.details_available and on_details is not None:
            show_more.render_details(lambda: on_details(place.form_id), lambda details: _render_form_details(details, shown))


def _render_expression_hit(hit: BlauExpressionHit, lang: str, on_details=None) -> None:
    with ui.element('div').classes(f'{CARD_CLASS} card'):
        with ui.element('div').classes('entry'):
            if hit.name:
                _rtl(hit.name).classes('name')
            if hit.sub_entry and hit.sub_entry != hit.name:
                ui.label('›').classes('sep')
                _rtl(hit.sub_entry).classes('name sub')
            if hit.arabic:
                ui.label(hit.arabic).props('dir="rtl" lang="ar"').classes('ar')
            if hit.page is not None:
                ui.label(f'{tr("p.")} {hit.page}').classes('page')
            link = ui.link(tr('Open on the dictionary site'), hit.url, new_tab=True).classes('site')
            link.props(f'title="{_attr(tr("Open on the dictionary site"))}"')
        if getattr(hit, 'possible_spelling', False):
            ui.label(tr('Possible spelling')).classes('where dict-evidence')
        offered = set()
        shown = frozenset(p.quotation.id for p in hit.places if p.quotation is not None and p.quotation.id)
        for place in hit.places:
            _render_expression_place(place, lang, on_details if place.form_id not in offered else None, shown)
            if place.details_available:
                offered.add(place.form_id)
        if hit.places_total > len(hit.places):
            ui.label(f'+{hit.places_total - len(hit.places)} {tr(expression_panel.MORE_PLACES)}').classes('more')


def _render_blau_expression(result: BlauExpression, credit_he: str, credit_en: str, lang: str,
                            on_gap: Optional[Callable[[int, str], Any]], on_more_entries=None, on_details=None) -> None:
    """Blau's tab for several words: the disclaimer, the gap control, the entries holding the
    expression (the service's tiers) with their places, or that the words were not found
    together (the gap control stays, so the reader can widen it), then the credit."""
    with ui.element('div').classes(f'{EXPRESSION_CLASS} expression'):
        expression_panel.render_automatic_note()
        ui.label(tr(DISCLAIMER)).classes(f'{DISCLAIMER_CLASS} disclaimer')
        expression_panel.render_gap(result.gap, result.text, on_gap)
        for hit in result.hits:
            _render_expression_hit(hit, lang, on_details)
        expression_panel.render_related(result, lambda hit: _render_expression_hit(hit, lang, on_details))
        expression_panel.render_more_entries(result.hits, result.truncated, on_more_entries,
            lambda hit: _render_expression_hit(hit, lang, on_details), lambda hit: (hit.root_id, hit.sub_entry), 20)
        if result.text and not result.found:
            with ui.element('div').classes(f'{EMPTY_CLASS} empty'):
                ui.label(tr(expression_panel.NOTHING_TOGETHER))
        _render_credit(credit_he, credit_en, lang)


def _render_blau(result: BlauAnswer, credit_he: str, credit_en: str, lang: str,
                 on_gap: Optional[Callable[[int, str], Any]] = None,
                 on_stem: Optional[Callable[[str, str], Any]] = None,
                 on_suggest: Optional[Callable[[str, str], Any]] = None, chips: tuple[str, str] = ('', ''), on_more_entries=None, on_details=None) -> None:
    """Blau's tab: the disclaimer, the cards and the examples, or what was
    looked for, then the credit (ruling (a): on every panel). Several words are
    the expression search's section. A root typed with a stem it does not list
    says so above the root's cards."""
    if isinstance(result, BlauExpression):
        _render_blau_expression(result, credit_he, credit_en, lang, on_gap, on_more_entries, on_details)
        return
    expression_panel.render_automatic_note()
    ui.label(tr(DISCLAIMER)).classes(f'{DISCLAIMER_CLASS} disclaimer')
    if result.stem_missing:
        stem_buttons.render_not_listed(result.stem)
    # The other roots the word's verb forms give, beside the entries the word itself reached:
    # chips at the top, a press opens that root on that stem and the chips stay.
    stem_buttons.render_suggestions(result.suggestions, BLAU_TAB, on_suggest or on_stem, *chips)
    # The entries that print the word with the words beside it in the text, first.
    if not result.context_quoted:
        for found in result.context:
            expression_panel.render_context(found, lambda hit: _render_expression_hit(hit, lang, on_details))
    if result.cards:
        for card in result.cards:
            _render_card(card, lang, on_stem, on_details)
        expression_panel.render_more_entries(result.cards, result.more_cards, on_more_entries,
            lambda card: _render_card(card, lang, on_stem, on_details),
            lambda card: (card.candidate, card.root_id, card.sub_entry), 10)
    if result.context_quoted:
        for found in result.context:
            expression_panel.render_context(found, lambda hit: _render_expression_hit(hit, lang, on_details))
    if result.examples or result.quotes:
        _render_examples(result, lang, on_details)
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


def _render_friedman(friedman: FriedmanAnswer, credit: str, lang: str, on_search: Optional[Callable[[str], Any]],
                     titled: bool, on_gap: Optional[Callable[[int, str], Any]] = None,
                     on_stem: Optional[Callable[[str, str], Any]] = None,
                     on_suggest: Optional[Callable[[str, str], Any]] = None, chips: tuple[str, str] = ('', ''), on_more_entries=None) -> None:
    """Friedman's tab: its section (web/components/friedman_panel.py), or its title and,
    for a word, that nothing was found. Several words are an expression search (plan step 4):
    its hits with the gap control, or that the words were not found together."""
    if isinstance(friedman, FriedmanExpression):
        if not friedman_panel.render_friedman_expression(friedman, credit=credit, lang=lang, on_gap=on_gap,
                                                         titled=titled, on_more_entries=on_more_entries):
            friedman_panel.render_friedman_expression_empty(friedman, on_gap=on_gap, titled=titled)
        return
    if friedman_panel.render_friedman(friedman, credit=credit, lang=lang, on_search=on_search, titled=titled,
                                      on_stem=on_stem, tab=FRIEDMAN_TAB, on_suggest=on_suggest, chips=chips,
                                      on_more_entries=on_more_entries):
        return
    friedman_panel.render_friedman_empty(friedman.word, titled=titled)


def render_lookup(result: Optional[BlauAnswer], *, credit_he: str = '', credit_en: str = '', lang: str = 'he',
                  on_close: Optional[Callable[..., Any]] = None,
                  on_search: Optional[Callable[[str], Any]] = None, focus_input: bool = False,
                  lane: Optional[LaneLookup] = None, lane_credit: str = '',
                  tab: Optional[str] = None, on_tab: Optional[Callable[[str], Any]] = None,
                  friedman: Optional[FriedmanAnswer] = None, friedman_credit: str = '',
                  on_gap: Optional[Callable[[int, str], Any]] = None,
                  on_stem: Optional[Callable[[str, str], Any]] = None,
                  on_suggest: Optional[Callable[[str, str], Any]] = None, chips: tuple[str, str] = ('', ''),
                  on_order: Optional[Callable] = None, on_more_quotes: Optional[Callable] = None,
                  on_more_entries: Optional[Callable] = None, on_blau_details: Optional[Callable] = None) -> bool:
    """Draw the dictionaries' answers for one word into the current container:
    ``result`` is Blau's (None when Blau is not served; for several words its
    expression search), ``lane`` Lane's (None when Lane is not served), ``friedman``
    Friedman's (None when not served; for several words its expression search). The
    expression searches' gap controls report to ``on_gap``: one gap for both.
    Each dictionary with its credit line gets a tab (ruling (a), D3: no
    dictionary's text without its credit); without any, nothing is drawn and
    the answer is False. With several, the tab row opens on
    ``pick_tab(..., tab)`` (``tab``: the tab an action inside the panel came from),
    a tab the reader chooses is reported to ``on_tab`` and opens at its top. An
    action inside a tab calls ``on_search(text, tab)`` / ``on_gap(gap, text, tab)``
    with its tab. With ``on_search`` the head holds the box (the word in it); a
    lookup with no word is the panel opened to type one. A stem button on a
    root's card reports its tab and what to look up ("דלל X") to ``on_stem``; a chip of the
    other possible roots to ``on_suggest`` (else ``on_stem``). ``chips``: (the word the chips
    belong to, the chip shown now; '' while the word's own answer is shown)."""
    credit_he = (credit_he or '').strip()
    lane_credit = (lane_credit or '').strip()
    friedman_credit = (friedman_credit or '').strip()
    word = next((r.word for r in (result, lane, friedman) if r is not None), '')
    tabs: list[str] = []
    if result is not None:
        if credit_he:
            tabs.append(BLAU_TAB)
        else:
            logger.warning('blau_panel: no credit line; the lookup of %r in Blau is not shown', word)
    # Friedman's tab beside Blau's: the two Judeo-Arabic dictionaries first, Lane's Arabic
    # lexicon after them (owner, 2026-11-08).
    if friedman is not None:
        if friedman_credit:
            tabs.append(FRIEDMAN_TAB)
        else:
            logger.warning('blau_panel: no credit line; the lookup of %r in Friedman is not shown', word)
    if lane is not None:
        if lane_credit:
            tabs.append(LANE_TAB)
        else:
            logger.warning('blau_panel: no Perseus line; the lookup of %r in Lane is not shown', word)
    if not tabs:
        return False
    found = {BLAU_TAB: bool(result is not None and result.found),
             LANE_TAB: bool(lane is not None and (lane.readings or lane.roots)),
             FRIEDMAN_TAB: bool(friedman is not None and friedman.found)}
    # The panel opens on the answer closest to the word.
    nearness = {BLAU_TAB: closeness(result), LANE_TAB: closeness(lane), FRIEDMAN_TAB: closeness(friedman)}
    titles = {BLAU_TAB: BLAU_TITLE, LANE_TAB: lane_panel.TITLE, FRIEDMAN_TAB: friedman_panel.TITLE}
    several = len(tabs) > 1

    def draw(name: str) -> None:
        # An action inside a tab (a Lane reading chip, a gap control) looks up again on that tab:
        # ``on_search`` and ``on_gap`` are told which (no tab is remembered).
        search = (lambda text, _tab=name: on_search(text, _tab)) if on_search is not None else None
        gap = (lambda value, text, _tab=name: on_gap(value, text, _tab)) if on_gap is not None else None
        answer = {BLAU_TAB: result, LANE_TAB: lane, FRIEDMAN_TAB: friedman}[name]
        if expression_panel.render_problem(answer):
            ui.label({BLAU_TAB: credit_he, LANE_TAB: lane_credit, FRIEDMAN_TAB: friedman_credit}[name])
            return
        if isinstance(answer, (BlauExpression, FriedmanExpression)) and on_order is not None:
            ui.checkbox(tr('Any order'), value=not answer.ordered,
                        on_change=lambda e: on_order(not e.value, answer.text, name)).classes('expr-any-order')
        async def fetch_entries(limit):
            expanded = await on_more_entries(name, answer, limit)
            if getattr(expanded, 'problem', ''):
                raise RuntimeError(expanded.problem)
            if isinstance(expanded, (BlauExpression, FriedmanExpression)):
                return expanded.hits, expanded.truncated
            if name == BLAU_TAB:
                shown = {(hit.root_id, hit.sub_entry) for found in answer.context for hit in found.hits}
                return tuple(c for c in expanded.cards if (c.root_id, c.sub_entry) not in shown), expanded.more_cards
            shown = {hit.unit.id for found in answer.context for hit in found.hits}
            return tuple(m for m in expanded.matches if m.unit.id not in shown), expanded.truncated
        more_entries = fetch_entries if on_more_entries is not None else None
        displayed = replace(answer, more_quotes=0) if on_more_quotes is not None and hasattr(answer, "more_quotes") else answer
        if name == BLAU_TAB:
            _render_blau(displayed, credit_he, credit_en, lang, on_gap=gap, on_stem=on_stem, on_suggest=on_suggest,
                         chips=chips, on_more_entries=more_entries, on_details=on_blau_details)
        elif name == LANE_TAB:
            _render_lane(lane, lane_credit, lang, search, titled=several)
        else:
            _render_friedman(displayed, friedman_credit, lang, search, titled=several, on_gap=gap,
                             on_stem=on_stem, on_suggest=on_suggest, chips=chips, on_more_entries=more_entries)
        if on_more_quotes is not None and getattr(answer, 'more_quotes', 0):
            extra = ui.column().classes('dict-more-quotations')
            offset = answer.quote_offset or len(answer.quotes)
            async def more():
                nonlocal offset
                button.disable()
                try:
                    hits, remaining = await on_more_quotes(name, answer, offset)
                    if extra.is_deleted:
                        return
                    with extra:
                        for hit in hits:
                            if name == BLAU_TAB:
                                _render_expression_hit(hit, lang, on_blau_details)
                            else:
                                friedman_panel._render_hit(hit)
                    offset += 5
                    button.set_visibility(bool(remaining and hits))
                    button.set_text(f'+{remaining} {tr("more entries")} · {tr("Show more examples")}')
                except Exception:
                    logger.exception('Additional dictionary examples could not be read')
                    with extra:
                        ui.label(tr('The dictionary lookup failed. Please try again.'))
                finally:
                    if not button.is_deleted:
                        button.enable()
            button = ui.button(f'+{answer.more_quotes} {tr("more entries")} · {tr("Show more examples")}',
                               on_click=more).props('flat dense').classes('dict-more-quotes-button')

    with ui.element('div').classes('panel-head'):
        ui.label(tr(TITLE if several else titles[tabs[0]])).classes('title')
        if on_search is not None:
            _render_box(word, on_search, focus_input, stems=BLAU_TAB in tabs or FRIEDMAN_TAB in tabs)
        elif word:
            _rtl(word).classes('word')
        close = ui.button(icon='close', on_click=on_close).props(
            f'flat round dense size=sm aria-label="{_attr(tr("Close"))}"').classes(CLOSE_CLASS)
        close.tooltip(tr('Close'))
    if several:
        current = pick_tab(tabs, found, tab, nearness)

        def chosen(e: Any) -> None:
            if on_tab is not None and e.value in tabs:
                on_tab(e.value)

        with ui.tabs(value=current, on_change=chosen).props(
                'dense no-caps inline-label narrow-indicator').classes(TABS_CLASS) as tab_row:
            for name in tabs:
                ui.tab(name, label=tr(TAB_LABELS[name])).classes(
                    f'{TAB_CLASS} ' + ('found' if found[name] else 'not-found'))
    with ui.element('div').classes(BODY_CLASS) as body:
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
    if several:
        # A tab chosen opens at its top: the body that scrolls is one for all
        # the tabs, so it is put back to the top in the browser, at once.
        tab_row.on('update:model-value', js_handler=scroll_to_top_js(body.id))
    return True


def scroll_to_top_js(element_id: int) -> str:
    """The browser-side handler that puts the element with this NiceGUI id back to its top."""
    return f'() => {{ const body = getHtmlElement({int(element_id)}); if (body) body.scrollTop = 0; }}'
