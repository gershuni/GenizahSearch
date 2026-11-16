# -*- coding: utf-8 -*-
"""The dictionary lookup on a page: which dictionaries are served, the popover and its handlers, the
reader's switch (docs/plans/BLAU_DICTIONARY_PLAN.md §6.2; the Quick View plan in the private repository).

``/browse`` and the search page's Quick View both use it. ``web/components/blau_panel.py`` draws the
answer; ``web/static/js/blau_lookup.js`` is the click side, which finds the ONE popover in the page and
dispatches ``blau-lookup`` / ``blau-close`` / ``blau-open`` on it.

The page passes its own services and its own PostHog capture (each looked up when called), so a page's
tests patch them where they always did.
"""
from __future__ import annotations

from typing import Callable

from nicegui import background_tasks, run, ui

from shared.arabic_service import empty_lane
from shared.blau_service import empty_lookup as blau_empty_lookup
from shared.friedman_service import DEFAULT_GAP as FRIEDMAN_DEFAULT_GAP
from shared.friedman_service import empty_lookup as friedman_empty_lookup
from shared.ja_roots import borrowed_roots, root_and_stem
from web.components import blau_panel
from web.translations import get_language, tr, using_language


class Popover:
    """One popover and its lookups. ``hide()`` hides it and makes every lookup in flight stale."""

    def __init__(self, element, hide: Callable[..., None]):
        self.element = element
        self.hide = hide


class DictionaryLookup:
    """One page build's dictionaries: decided ONCE, fail-closed. A service is served when the sidecar
    passed every check at open and its switch is on, or (switch "editors") to a signed-in editor or
    admin (shared/dictionary_switch.py). Without any, the page carries no script, no popover and no
    marked pane, and is what it was before."""

    def __init__(self, blau, lane, friedman, *, editor: bool, capture: Callable[..., None]):
        self.blau = blau
        self.lane = lane
        self.friedman = friedman
        # Asked once per page, as the reader is known here and not in the lookups' worker threads.
        self.editor = editor
        self.capture = capture
        self.on = self.served(blau) or self.served(lane) or self.served(friedman)
        # No tab is remembered: a new word opens on the first dictionary that has something for it
        # (blau_panel.pick_tab); only an action inside a tab (a stem button, a gap control, a Lane
        # reading) looks up again on that tab. The printed words Friedman's expression search allows
        # between two of the typed words (friedman_panel's gap control); the reader's choice holds for
        # the visit.
        self.gap = {'value': FRIEDMAN_DEFAULT_GAP}
        self.ordered = {'value': True}

    def served(self, service) -> bool:
        return service.is_available() or (self.editor and service.served_to(True))

    def head(self) -> None:
        """The click script, from the page's build: a script added to a live page does not run."""
        if self.on:
            ui.add_head_html(blau_panel.head_html())

    def popover(self) -> Popover:
        """The popover, here in the tree. Fixed-positioned by the click script beside the word (a
        sheet at the foot of a phone). Where the page rebuilds it, a lookup in flight for what the
        reader has left finds a deleted element and stops. The reader's language is captured now: the
        handlers run outside any render, where the process global may be another visitor's."""
        blau_service, lane_service, friedman_service = self.blau, self.lane, self.friedman
        _dict_served = self.served
        dict_gap, dict_ordered = self.gap, self.ordered
        capture = self.capture
        blau_popover = ui.element('div').classes(
            f'{blau_panel.POPOVER_CLASS} {blau_panel.PANEL_CLASS} print-hide'
        ).style('display: none;').props(
            # data-no-loading-bar: Enter in the panel's box looks a word up in
            # the page; the site's top bar (web/main.py) must not start.
            'role="dialog" aria-live="polite" data-no-loading-bar')
        _blau_lang = get_language()
        # PostHog (blau_panel.POSTHOG_*: is the switch found, and used?) goes through
        # the page's client: a lookup answers from a background task, with no page.
        _blau_client = ui.context.client
        # The request counter: a lookup that finishes after a later
        # click or a close is stale and draws nothing (Codex on
        # #473: a slow earlier lookup overwrote the newer word, or
        # reopened a panel the reader had closed).
        _blau_request = {'value': 0}

        def _blau_hide(_e=None):
            _blau_request['value'] += 1
            if not blau_popover.is_deleted:
                blau_popover.style('display: none;')

        def _choose_tab(name):
            # Counted only: the next word opens on the first tab with an answer.
            capture(blau_panel.POSTHOG_TAB, {'tab': name}, client=_blau_client)

        def _choose_order(ordered, text, tab=None):
            dict_ordered['value'] = ordered
            _blau_start(text, True, tab=tab, stems=False)

        async def _more_quotes(tab, answer, offset):
            service = friedman_service if tab == blau_panel.FRIEDMAN_TAB else blau_service
            return await run.io_bound(service.quotation_page, answer, offset)

        async def _blau_form_details(form_id):
            return await run.io_bound(blau_service.form_details, form_id)

        def _choose_gap(gap, text, tab=None):
            # The gap control in an expression search: the same words again with
            # the new gap, on the tab it is in, the focus staying in the box. The
            # words were an expression: they stay one ("פרק 2" is no typed stem).
            dict_gap['value'] = gap
            _blau_start(text, True, tab=tab, stems=False)

        def _choose_stem(tab, text):
            # A stem button on a root's card (or the chosen one, to go back to
            # the root): the lookup of "root stem", staying on the tab pressed.
            _blau_start(text, False, how=blau_panel.STEM_HOW, tab=tab)

        # The word's chips of other possible roots, kept while the reader presses
        # them one by one (blau_panel.keep_chips).
        _blau_chips = {'word': '', 'blau': (), 'friedman': (), 'chosen': ''}

        def _choose_suggestion(tab, text):
            # A chip of the other possible roots (or the word, to go back): the
            # lookup of "root stem" on the tab pressed, the word's chips kept.
            _blau_start(text, False, how=blau_panel.SUGGEST_HOW, tab=tab)

        def _blau_draw(result, focus, lane=None, friedman=None, tab=None, chips=('', '')):
            async def more_entries(name, answer, limit):
                service = friedman_service if name == blau_panel.FRIEDMAN_TAB else blau_service
                def load():
                    if hasattr(answer, 'hits'):
                        return service.expression(answer.text, answer.gap, limit=limit, ordered=answer.ordered)
                    extra = service.lookup(answer.word, limit=limit)
                    roots = (borrowed_roots(lane=lane, blau=result) if name == blau_panel.FRIEDMAN_TAB
                             else borrowed_roots(friedman, lane))
                    return service.borrow(extra, roots, limit=limit)
                return await run.io_bound(load)

            with using_language(_blau_lang):
                blau_popover.clear()
                with blau_popover:
                    drawn = blau_panel.render_lookup(
                        result, credit_he=blau_service.credit_he,
                        credit_en=blau_service.credit_en, lang=_blau_lang,
                        on_close=_blau_hide, on_search=_blau_search,
                        focus_input=focus, lane=lane, lane_credit=lane_service.credit,
                        tab=tab, on_tab=_choose_tab,
                        friedman=friedman, friedman_credit=friedman_service.credit_he,
                        on_gap=_choose_gap, on_order=_choose_order, on_more_quotes=_more_quotes,
                        on_more_entries=more_entries, on_stem=_choose_stem, on_blau_details=_blau_form_details,
                        on_suggest=_choose_suggestion, chips=chips)
            blau_popover.style('display: flex;' if drawn else 'display: none;')

        def _both_lookups(word, stems=True, beside=((), ())):
            # One trip off the loop for every dictionary; None for a
            # dictionary not served now (it gets no tab). Several words are
            # an expression search in Blau and Friedman (one gap for both); a
            # root with a stem ("דלל X") is that stem's paragraph in both, and
            # Lane, which has no stem paragraphs here, reads the root alone. Blau's
            # and Friedman's tabs then show, after their own, their entries of the
            # roots the other dictionaries give for the word, each from the others'
            # OWN answers (BlauService.borrow, FriedmanService.borrow):
            # Friedman's from Friedman's own matches, then Blau's own cards in the
            # order Blau's borrow gave them (borrowed_roots skips Blau's borrowed cards).
            # ``stems`` False: words clicked or selected in the manuscript, where a
            # number after a word ("פרק 2") is no typed stem. ``beside``: the words
            # before and after a clicked word in the text, for the expressions it
            # makes with them, shown first in both tabs.
            before, after = beside
            lane_word = (root_and_stem(word)[0] if stems else '') or word
            blau = (blau_service.answer(word, gap=dict_gap['value'], stems=stems, before=before, after=after, ordered=dict_ordered['value'])
                    if _dict_served(blau_service) else None)
            lane = lane_service.lookup(lane_word) if _dict_served(lane_service) else None
            friedman = (friedman_service.answer(word, gap=dict_gap['value'], stems=stems, before=before, after=after, ordered=dict_ordered['value'])
                        if _dict_served(friedman_service) else None)
            if blau is not None:
                blau = blau_service.borrow(blau, borrowed_roots(friedman, lane))
            if friedman is not None:
                friedman = friedman_service.borrow(friedman, borrowed_roots(lane=lane, blau=blau))
            if lane is not None:
                # Last, so the other two borrowed from Lane's own answer: Lane's tab adds
                # its entries of the roots they give, those reached by verb form too.
                lane = lane_service.borrow(lane, borrowed_roots(friedman, blau=blau, verb_forms=True))
            return blau, lane, friedman

        async def _blau_answer(word, request, focus=False, how='word', tab=None, stems=True,
                               beside=((), ())):
            answers = await run.io_bound(_both_lookups, word, stems, beside)
            if answers is None or blau_popover.is_deleted or request != _blau_request['value']:
                return
            result, lane, friedman = answers
            result, friedman, chips = blau_panel.keep_chips(_blau_chips, word, how, result, friedman)
            _blau_draw(result, focus, lane, friedman, tab, chips)
            capture(blau_panel.POSTHOG_LOOKUP,
                    blau_panel.lookup_event(focus, result, lane, how, friedman=friedman),
                    client=_blau_client)

        def _blau_start(word, focus, how='word', tab=None, stems=True, beside=((), ())):
            # Synchronous on purpose: the counter must move in the
            # order the clicks arrived, before any of their lookups
            # runs. An async handler is a task that starts later,
            # and a close handled in between would be counted
            # before the lookup it was meant to cancel. ``tab``: the
            # tab an action inside the panel came from, else None.
            # ``stems`` False: the words came from the manuscript; ``beside``: the
            # words before and after a clicked one there.
            _blau_request['value'] += 1
            background_tasks.create(_blau_answer(word, _blau_request['value'], focus, how, tab, stems,
                                                 beside),
                                    name='blau-lookup')

        def _blau_lookup(e):
            # Words clicked or selected in the manuscript: never a typed stem. A clicked
            # word comes with the words beside it (blau_panel.lookup_context).
            word = blau_panel.lookup_word(e.args)
            if word:
                _blau_start(word, False, blau_panel.lookup_how(e.args), stems=False,
                            beside=blau_panel.lookup_context(e.args))

        def _blau_search(word, tab=None):
            # The box at the top of the panel (typed, or a clicked word
            # corrected), or a Lane reading chip (on Lane's tab): the focus
            # stays in the box.
            word = (word or '').strip()
            if word:
                _blau_start(word, True, tab=tab)

        def _blau_open(_e=None):
            # The "type a word" button: the panel with an empty box.
            _blau_request['value'] += 1
            _blau_chips.update(word='', blau=(), friedman=(), chosen='')
            if not blau_popover.is_deleted:
                _blau_draw(blau_empty_lookup('') if _dict_served(blau_service) else None, True,
                           empty_lane('') if _dict_served(lane_service) else None,
                           friedman_empty_lookup('') if _dict_served(friedman_service) else None)

        blau_popover.on(blau_panel.LOOKUP_EVENT, _blau_lookup)
        blau_popover.on(blau_panel.CLOSE_EVENT, _blau_hide)
        blau_popover.on(blau_panel.OPEN_EVENT, _blau_open)
        return Popover(blau_popover, _blau_hide)

    def mark_pane(self, container) -> bool:
        """Mark a text pane; True when the reader's switch is on. The reader's switch (owner,
        2026-11-04: the page never guesses the text's language): marked, the pane's words are looked
        up; unmarked, a click on a word does nothing. The look-up key, Alt + a click and the phone's
        chip work here with the switch off too (owner, 2026-11-05; blau_lookup.js); the chip's label
        is in the page's language."""
        lookup_on = self.on and blau_panel.lookup_on()
        if lookup_on:
            container.props(blau_panel.PANE_ATTR)
        if self.on:
            container.props(blau_panel.KEY_PANE_ATTR)
            container.props[blau_panel.CHIP_LABEL_ATTR] = tr(blau_panel.CHIP_LABEL)
        return lookup_on

    def switch(self, container, popover: Popover, lookup_on: bool) -> None:
        """The switch, then (while it is on) the hint and the "type a word" button -- whose click the
        script handles: a server handler would race its close. The switch is the letters עع and, from
        640px, its label; highlighted and saying "New!" until this visitor first turns it on (owner,
        2026-11-04). ``container``: the pane it marks; ``lookup_on``: what ``mark_pane`` said."""
        if not self.on:
            return
        capture = self.capture
        _blau_lang = get_language()
        _blau_client = ui.context.client
        blau_lookup_on = lookup_on
        blau_new = blau_panel.switch_is_new(blau_lookup_on)
        # color=None: Quasar's `text-primary` is
        # !important and would beat the state's
        # colour (common.css, .blau-switch).
        blau_switch = ui.button(color=None).props(
            'flat dense no-caps').classes(
            f'{blau_panel.SWITCH_CLASS} print-hide'
            + (' active' if blau_lookup_on else '')
            + (f' {blau_panel.NEW_CLASS}' if blau_new else ''))
        blau_switch.props(
            f'aria-pressed={"true" if blau_lookup_on else "false"} '
            f'aria-label="{blau_panel.switch_label(blau_new)}"')
        with blau_switch:
            ui.html(blau_panel.GLYPH_SVG, sanitize=False).classes(
                blau_panel.GLYPH_CLASS)
            blau_label = ui.label(blau_panel.switch_label(blau_new)).classes(
                blau_panel.LABEL_CLASS)
            # What pressing it does, and under it the look-up
            # key, which works either way (owner, 2026-11-05).
            with ui.tooltip().classes(blau_panel.TIP_CLASS):
                blau_switch_tip = ui.label(
                    blau_panel.switch_tooltip(blau_lookup_on))
                ui.label(tr(blau_panel.KEY_HINT)).classes(
                    blau_panel.KEY_HINT_CLASS)
        blau_hint = ui.label(tr('Click a word to look it up in the dictionaries')).classes(
            f'text-xs {blau_panel.HINT_CLASS} print-hide')
        blau_type = ui.button(icon='keyboard').props(
            'flat round dense size=sm').classes(
            f'{blau_panel.OPEN_CLASS} print-hide')
        blau_type.props(f'aria-label="{tr("Type a word to look up")}"')
        blau_type.tooltip(tr('Type a word to look up'))
        blau_hint.set_visibility(blau_lookup_on)
        blau_type.set_visibility(blau_lookup_on)

        def _toggle_blau():
            on = not blau_panel.lookup_on()
            was_new = blau_panel.NEW_CLASS in blau_switch.classes
            blau_panel.set_lookup_on(on)
            if on:
                container.props(blau_panel.PANE_ATTR)
                blau_switch.classes(add='active', remove=blau_panel.NEW_CLASS)
            else:
                container.props(remove=blau_panel.PANE_ATTR)
                blau_switch.classes(remove='active')
                popover.hide()
            blau_switch.props(f'aria-pressed={"true" if on else "false"}')
            with using_language(_blau_lang):
                blau_switch_tip.text = blau_panel.switch_tooltip(on)
                if was_new and on:
                    blau_label.text = blau_panel.switch_label(False)
                    blau_switch.props(f'aria-label="{blau_label.text}"')
            blau_hint.set_visibility(on)
            blau_type.set_visibility(on)
            capture(blau_panel.POSTHOG_SWITCH,
                    {'on': on, 'was_new': was_new}, client=_blau_client)

        blau_switch.on_click(_toggle_blau)
