# -*- coding: utf-8 -*-
"""A "Pick from the shelf" button for the web pages that name one manuscript (shelf tree picker
plan §3.5, §10.4; handoff PRs 5-6): the Fragment Puzzle, the Reading Desk, the Joins Lab
anchor, fragment B of a new join, and the home page.

Each host keeps its own resolved-selection path: the button opens the shared picker
(``web.components.shelfmark_tree_picker.open_shelf_picker``) and hands the host's handler the
picker's ``{sys_id, shelfmark, placement_id}``, which the host passes to the same function its
typed shelfmark or its list already reaches. The picker itself is not copied here.

- The button exists only while the shelf tree is served (``SHELFMARK_TREE_ENABLED`` and the file
  checked: ``web.shelfmark_tree_assets.shelf_tree_service()``); with the file missing it is absent.
- Single choice, repeated: the next opening starts where the last pick was, so a reader adding
  several fragments walks along the shelf. A click selects; the foot's button (``confirm_label``)
  picks (handoff DX21).
- ``on_open()``, when given, runs at the click and returns the handler for that opening, so a
  host can bind the pick to what was on screen when it was asked for (the Puzzle's canvas).
- Only a corpus ``sys_id`` reaches a handler: the tree never holds LOCAL ``97…`` ids, and a pick
  that did would not be the tree's.
- ``multi=True`` (plan §10.13; handoff DX5, DX13): the picker's multiple-choice mode for a host that
  takes several at once (the Reading Desk, the Puzzle, a list). The handler is the host's
  ``on_confirm(entries, label)``: it answers None when it took them all, else the ids it took; what
  it did not take stays chosen, kept per reader and ``purpose`` (``web/shelf_selection_store.py``).
"""
from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Optional, Union
from urllib.parse import quote

from nicegui import ui

from shared.sys_id_patterns import CORPUS_SYS_ID_RE
from web.client_guard import client_gone
from web.components.shelfmark_tree_picker import open_shelf_picker
from web.shelfmark_tree_assets import shelf_tree_service
from web.translations import tr

#: A placement id as the tree writes it (``inv:1101:9900…``, ``cat:…``, ``sys:…``); anything else
#: in a link is not one.
PLACEMENT_ID_RE = re.compile(r'[a-z]{2,8}:[\w:.-]{1,120}')

Handler = Callable[[dict], Any]
ConfirmHandler = Callable[[list, str], Any]


def is_placement_id(text: Optional[str]) -> bool:
    """Whether ``text`` has the form of a placement id (it is still checked against the tree)."""
    return bool(text) and PLACEMENT_ID_RE.fullmatch(text) is not None


def corpus_selection(selection: Any) -> Optional[dict]:
    """The picker's output when it names a corpus manuscript, else None."""
    if not isinstance(selection, dict):
        return None
    sys_id = str(selection.get('sys_id') or '').strip()
    if not CORPUS_SYS_ID_RE.fullmatch(sys_id):
        return None
    return {'sys_id': sys_id, 'shelfmark': selection.get('shelfmark') or sys_id,
            'placement_id': selection.get('placement_id') or None}


def browse_url_for_pick(selection: dict) -> str:
    """The Browse address of a pick: the manuscript, and the place it was picked at (``place``),
    which Browse keeps for its place on the shelf and previous / next."""
    url = f"/browse?sys_id={quote(selection['sys_id'], safe='')}"
    placement = selection.get('placement_id')
    if is_placement_id(placement):
        url += f"&place={quote(placement, safe='')}"
    return url


def shelf_pick_button(on_pick: Optional[Handler] = None, *, on_open: Optional[Callable[[], Optional[Handler]]] = None,
                      start: Union[None, str, Callable[[], Optional[str]]] = None, host=None,
                      memory: Optional[dict] = None, title: Optional[str] = None, label: Optional[str] = None,
                      icon: str = 'shelves', props: str = 'flat dense no-caps', testid: str = 'shelf-pick',
                      multi: bool = False, purpose: str = '', confirm_label: Optional[str] = None,
                      lists_mgr=None, hint: Optional[str] = None, limit=None, add_to: bool = False,
                      switch_label: Optional[str] = None, on_several: Optional[ConfirmHandler] = None,
                      several_label: Optional[str] = None):
    """Add the button where the host builds its typed box; None when the tree is not served.

    ``on_pick(selection)`` (awaited if async) receives ``{sys_id, shelfmark, placement_id}``;
    ``on_open()`` instead returns the handler for one opening (None: nothing to pick for).
    ``start``: where the first opening starts (a ``sys_id``, a placement id, a node id or a typed
    prefix, or a callable answering one at the click). ``host``: the element the dialog is built
    in; by default the page's layout, which no redraw clears. ``memory``: a dict that keeps the
    last pick (``placement``) across redraws that build the button again (the Reading Desk's
    toolbar); by default the button's own.

    ``multi``: the multiple-choice mode; the handler (``on_pick`` or what ``on_open()`` returns) is
    ``on_confirm(entries, label)`` for ``purpose``, with ``confirm_label`` on the confirm button and
    "Save as a list" offered through ``lists_mgr`` (or a callable answering it at the click: the
    site's lists manager is made per access). ``hint``: the button's tooltip and accessible name, where
    it says more than "Pick from the shelf".

    ``confirm_label`` is also the single choice's button (handoff DX21: a click selects, the button
    picks): "Open" where the pick opens the manuscript, the host's own words elsewhere, "Select" by
    default. ``add_to`` (DX22, the hosts whose pick opens a manuscript): "Add to List" (through
    ``lists_mgr``) and "Add to Puzzle" beside it.

    ``switch_label`` with ``on_several`` (the header and the home page, as Browse's own picker): the
    single choice with a switch to the multiple choice in the same dialog (DX16); several ticked go to
    ``on_several(entries, label)`` for ``purpose``, its button ``several_label``, each tick limited by
    ``limit`` (or a callable answering one at the click).
    """
    if shelf_tree_service() is None:
        return None
    if host is None:
        host = ui.context.client.layout
    label = tr('Pick from the shelf') if label is None else label
    last = memory if memory is not None else {}

    async def _picked(handler: Handler, selection: dict):
        chosen = corpus_selection(selection)
        if chosen is None or client_gone(host):     # the tab was closed while the picker was open
            return
        if chosen['placement_id']:
            last['placement'] = chosen['placement_id']
        outcome = handler(chosen)
        if inspect.isawaitable(outcome):
            await outcome

    async def _confirmed(handler: ConfirmHandler, entries: list, chip_label: str):
        if client_gone(host):                   # the tab was closed while the picker was open
            return []
        chosen = [c for c in (corpus_selection(e) for e in entries) if c is not None]
        if not chosen:
            return []
        outcome = handler(chosen, chip_label)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        return outcome

    def _open():
        tree = shelf_tree_service()
        if tree is None or client_gone(host):   # the tree was closed meanwhile, or the tab
            return
        handler = on_open() if on_open is not None else on_pick
        if handler is None:
            return
        where = last.get('placement') or (start() if callable(start) else start)
        if switch_label and on_several is not None:
            open_shelf_picker(host, tree, lambda selection: _picked(handler, selection), start=where, title=title,
                              purpose=purpose,
                              on_confirm=lambda entries, chip_label: _confirmed(on_several, entries, chip_label),
                              confirm_label=several_label, switch_label=switch_label, pick_label=confirm_label,
                              add_to=add_to, lists_mgr=lists_mgr() if callable(lists_mgr) else lists_mgr,
                              limit=limit() if callable(limit) else limit)
            return
        if multi:
            open_shelf_picker(host, tree, start=where, title=title, mode='multi', purpose=purpose,
                              on_confirm=lambda entries, chip_label: _confirmed(handler, entries, chip_label),
                              confirm_label=confirm_label,
                              lists_mgr=lists_mgr() if callable(lists_mgr) else lists_mgr,
                              limit=limit() if callable(limit) else limit)
            return
        open_shelf_picker(host, tree, lambda selection: _picked(handler, selection),
                          start=where, title=title, pick_label=confirm_label, add_to=add_to,
                          lists_mgr=lists_mgr() if callable(lists_mgr) else lists_mgr)

    said = hint or tr('Pick from the shelf')
    button = ui.button(label or '', icon=icon, on_click=_open).props(
        f'{props} aria-label="{said}" data-testid="{testid}"')
    if not label or hint:
        button.tooltip(said)
    return button
