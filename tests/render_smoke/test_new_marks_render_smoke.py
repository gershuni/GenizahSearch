# -*- coding: utf-8 -*-
"""Render-smoke: the one-time "New" on /search (owner, 2026-11-18; web/components/new_mark.py),
on the real page in a NiceGUI User simulation. As letter-level search was announced: the control
highlighted and its label saying it is new, no badge; shown ONE time.

* With the catalogue search served, a reader who has not seen it sees the mode list highlighted,
  with the tooltip "Search the catalogue information", its Catalog item reading "New! Catalog (%)"
  -- and it is spent for later visits at once. Choosing a mode (the reader's own pick) makes it
  plain on the page; the Catalog item keeps its tooltip. Where it was seen, plain; where the
  catalogue is not served, nothing.
* In Title mode the Fuzzy search switch is highlighted and reads "New! Fuzzy search", until the
  reader works the switch or leaves Title mode.
"""
from __future__ import annotations

from nicegui import events, ui
from nicegui.context import context as nicegui_context

from tests.render_smoke._bounded_run import run_bounded
from tests.render_smoke.test_search_measurement_filters_render_smoke import _user

import web.pages.search as search_page
from shared import fjms_service
from shared.fjms_service import FjmsService
from tests.criteria_sidecar import build_criteria_sidecar
from web.components.new_mark import (CATALOG_MODE_TIP, CATALOG_SEARCH_USED_KEY, KEYS, NEW_CLASS,
                                     NEW_OPTION_CLASS, TITLE_FUZZY_USED_KEY)


def _mode_list(user):
    with user._client:
        return next(e for e in user._client.elements.values()
                    if isinstance(e, ui.select) and 'Title' in (e.options or {}))


def _fuzzy_switch(user):
    with user._client:
        return next(e for e in user._client.elements.values()
                    if 'catalog-near-switch' in (getattr(e, '_markers', None) or []))


def _reader_sets(user, element, args):
    """What the reader's own pick sends: update:model-value, to every listener."""
    ran = 0
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.type == 'update:modelValue':          # NiceGUI keeps the camelCase name
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args=args))
                ran += 1
    assert ran, element


def _choose(user, mode):
    select = _mode_list(user)
    _reader_sets(user, select, {'value': list(select.options).index(mode)})


def _state(user):
    mode, near = _mode_list(user), _fuzzy_switch(user)
    slot = mode.slots.get('option')
    template = (slot.template or '') if slot else ''
    with user._client:
        tips = [c.text for c in mode.default_slot.children if isinstance(c, ui.tooltip)]
    return {'list': NEW_CLASS in mode.classes,
            'catalog': mode.options.get('Catalog'),
            'item tip': NEW_OPTION_CLASS in template and CATALOG_MODE_TIP in template,
            'list tip': tips,
            'switch': (near.text, NEW_CLASS in near.classes)}


def _open(monkeypatch, tmp_path, saved, *, served=True, steps=()):
    monkeypatch.setattr(fjms_service, '_default_service',
                        FjmsService(db_path=build_criteria_sidecar(tmp_path / 's.db')))
    monkeypatch.setattr(search_page, 'catalog_available', lambda: served)
    facts, written = {}, {}
    saved_slots = list(nicegui_context.slot_stack)

    async def run():
        async with _user(saved, written) as user:
            await user.open('/search')
            facts['shown'] = _state(user)
            facts['spent'] = {k: v for k, v in written.items() if k in KEYS}
            for name, step in steps:
                step(user)
                facts[name] = _state(user)

    try:
        run_bounded(run())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)
    return facts, written


PLAIN = ('Fuzzy search', False)


def test_a_new_reader_sees_it_one_time_until_choosing_a_mode(monkeypatch, tmp_path):
    facts, _ = _open(monkeypatch, tmp_path, {'search_mode': 'exact'},
                     steps=[('title', lambda u: _choose(u, 'Title')), ('exact', lambda u: _choose(u, 'exact'))])
    assert facts['shown'] == {'list': True, 'catalog': 'New! Catalog (%)', 'item tip': True,
                              'list tip': [CATALOG_MODE_TIP], 'switch': PLAIN}, facts
    assert facts['spent'] == {CATALOG_SEARCH_USED_KEY: True}, facts       # seen: spent for later visits
    # the reader's own pick: the list plain, its item's tooltip stays; the switch's "New" in Title
    assert facts['title'] == {'list': False, 'catalog': 'Catalog (%)', 'item tip': True, 'list tip': [],
                              'switch': ('New! Fuzzy search', True)}, facts
    assert facts['exact']['switch'] == PLAIN, facts                       # gone on leaving Title


def test_a_reader_who_saw_it_sees_it_plain(monkeypatch, tmp_path):
    facts, _ = _open(monkeypatch, tmp_path, {'search_mode': 'exact', CATALOG_SEARCH_USED_KEY: True})
    assert facts['shown'] == {'list': False, 'catalog': 'Catalog (%)', 'item tip': True, 'list tip': [],
                              'switch': PLAIN}, facts


def test_nothing_where_the_catalogue_is_not_served(monkeypatch, tmp_path):
    facts, written = _open(monkeypatch, tmp_path, {'search_mode': 'exact'}, served=False)
    assert facts['shown'] == {'list': False, 'catalog': None, 'item tip': False, 'list tip': [],
                              'switch': PLAIN}, facts
    assert CATALOG_SEARCH_USED_KEY not in written


def test_the_fuzzy_switch_says_new_in_title_mode_until_worked(monkeypatch, tmp_path):
    facts, written = _open(monkeypatch, tmp_path, {'search_mode': 'Title', CATALOG_SEARCH_USED_KEY: True},
                           steps=[('worked', lambda u: _reader_sets(u, _fuzzy_switch(u), True))])
    assert facts['shown']['switch'] == ('New! Fuzzy search', True), facts
    assert facts['spent'] == {TITLE_FUZZY_USED_KEY: True}, facts
    assert facts['worked']['switch'] == PLAIN, facts


def _new_search(user):
    with user._client:
        button = next(e for e in user._client.elements.values()
                      if isinstance(e, ui.button) and 'restart_alt' in str(e.props.get('icon')))
    _reader_sets_click(user, button)


def _reader_sets_click(user, element):
    ran = 0
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.type == 'click':
                events.handle_event(listener.handler, events.ClickEventArguments(
                    sender=element, client=user._client))
                ran += 1
    assert ran, element


def test_new_search_hides_the_title_and_catalog_switches(monkeypatch, tmp_path):
    """New Search puts the mode back to Exact in code, which fires no change event: the
    Fuzzy search / Automatic translations switches must go with Title or Catalog mode."""
    for mode in ('Title', 'Catalog'):
        seen = {}
        (tmp_path / mode).mkdir()

        def look(user, key):
            near = _fuzzy_switch(user)
            seen[key] = (_mode_list(user).value, near.parent_slot.parent.visible)

        _open(monkeypatch, tmp_path / mode, {'search_mode': mode, CATALOG_SEARCH_USED_KEY: True},
              steps=[('before', lambda u: look(u, 'before')),
                     ('reset', lambda u: (_new_search(u), look(u, 'after')))])
        assert seen == {'before': (mode, True), 'after': ('exact', False)}, (mode, seen)
