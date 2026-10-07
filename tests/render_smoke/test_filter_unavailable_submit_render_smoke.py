# -*- coding: utf-8 -*-
"""Render-smoke (C-9, K-18): web submit handlers when the filters cannot be applied (#17).

Drives the REAL /search and /parallels pages in a NiceGUI User simulation:
a saved date filter is active (seeded through the storage reader the pages
use), the FJMS singleton is either absent (no sidecar) or a real sidecar
whose filter query fails, and the visitor presses the page's own Search /
Find Parallels control. Each case must:

* never call the engine (no search without the filters, and no "No
  manuscripts match" short-circuit either);
* show the notice chosen by the failure's reason;
* put the page back to idle (buttons, progress, results area).

Also K-18: /parallels shows the "saved filters were removed" notice when a
saved measurement filter is one the open catalog is known to lack.

And a search-history entry never shares a list with the live filters, in
either direction: a chip removal edits the live list IN PLACE, so a shared
list rewrote the saved entry (restore domains=['Halakha'], remove the chip,
and the history said domains=[]).
"""
from __future__ import annotations

import asyncio
import os
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from nicegui import core, events, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.testing.user_interaction import UserInteraction
from nicegui.ui_run import set_storage_secret

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fjms_filter_sidecar import FailingFinalExecute, build_sidecar  # noqa: E402

import web.main  # noqa: E402,F401  (registers /search and /parallels)
import web.safe_storage as _safe_storage  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402
from web import research_jobs  # noqa: E402
from web.state import state as _web_state  # noqa: E402

MSG_FAILED = ('The filters could not be applied, so the search was not run. '
              'Try again, or remove the filters.')
MSG_MISSING = ('The catalog data these filters need is not available, so the search '
               'was not run. Remove the filters to search.')
NO_MATCH = 'No manuscripts match the current filters.'
DROPPED_NOTICE = 'Some saved filters were removed: this catalog data is not available.'


class _Engine:
    """Records every engine call made after the visitor presses the button."""

    def __init__(self):
        self.calls = []

    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, *args, **kwargs):
        self.calls.append(('execute_search', kwargs))
        return []

    def search_composition_logic(self, *args, **kwargs):
        self.calls.append(('search_composition_logic', kwargs))
        return {'main': [], 'filtered': [], 'partial': False, 'boundary_stats': None,
                'corpus_scope': 'genizah', 'effective_chunk_size': 5,
                'composition_notices': []}


@asynccontextmanager
async def _user(engine, saved: dict):
    real_get = _safe_storage.safe_user_get

    def _get(key, default=None):
        if key in saved:
            return saved[key]
        return real_get(key, default)

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    kept = (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine,
            _web_state.meta_mgr)
    _web_state.is_ready = lambda: True
    _web_state.searcher = engine
    _web_state.lab_engine = SimpleNamespace(settings=None)
    _web_state.meta_mgr = SimpleNamespace(csv_bank={})
    saved_executor = research_jobs._wait_executor
    research_jobs._wait_executor = None
    try:
        prepare_simulation()
        set_storage_secret('filter-unavailable-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value='en'))
            stack.enter_context(patch('web.pages.parallels.passage_available', return_value=False))
            stack.enter_context(patch.object(_safe_storage, 'safe_user_get', _get))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine,
         _web_state.meta_mgr) = kept
        research_jobs._wait_executor = saved_executor


def _run(coro_fn):
    saved_slots = list(nicegui_context.slot_stack)
    try:
        asyncio.run(coro_fn())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user, kind):
    with user.client:
        return [e for e in user.client.elements.values() if isinstance(e, kind)]


def _button(user, label, cls):
    found = [b for b in _elements(user, ui.button)
             if b.props.get('label') == label and cls in b.classes]
    assert len(found) == 1, f"{label!r} button not found uniquely ({len(found)})"
    return found[0]


async def _settle(user, engine, rounds=60):
    """Wait until a notice appears or the engine is called."""
    for _ in range(rounds):
        if user.notify.messages or engine.calls:
            return
        await asyncio.sleep(0.05)


@pytest.fixture
def absent_catalog(monkeypatch, tmp_path):
    monkeypatch.setattr(fjms_service, '_default_service',
                        FjmsService(db_path=str(tmp_path / 'absent.db')))
    return MSG_MISSING


@pytest.fixture
def failing_catalog(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / 's.db'))
    svc._conn = FailingFinalExecute(svc._conn)
    monkeypatch.setattr(fjms_service, '_default_service', svc)
    return MSG_FAILED


CATALOGS = ['absent_catalog', 'failing_catalog']


@pytest.mark.parametrize('catalog', CATALOGS)
def test_search_submit_with_unappliable_filters_runs_nothing_and_says_why(request, catalog):
    expected = request.getfixturevalue(catalog)
    engine = _Engine()
    seen = {}

    async def go():
        async with _user(engine, {'search_filter_date_from': 1000}) as user:
            await user.open('/search')
            query = next(e for e in _elements(user, ui.input)
                         if e.props.get('placeholder') == 'Enter Hebrew text to search')
            with user.client:
                query.value = 'word'
            engine.calls.clear()
            user.notify.messages.clear()
            search_btn = _button(user, 'Search', 'px-8')
            UserInteraction(user, {search_btn}, None).click()
            await _settle(user, engine)
            await asyncio.sleep(0.2)
            stop_btn = _button(user, 'Stop', 'px-4')
            bars = _elements(user, ui.linear_progress)
            seen.update(
                calls=list(engine.calls), notices=list(user.notify.messages),
                search_display=search_btn._style.get('display'),
                stop_display=stop_btn._style.get('display'),
                bar_hidden=all('opacity-0' in b.classes for b in bars),
            )

    _run(go)
    assert seen['calls'] == [], f"the search ran although its filters could not be applied: {seen['calls']}"
    assert expected in seen['notices'], seen['notices']
    assert NO_MATCH not in seen['notices'], "a lookup that could not run was shown as no match"
    assert seen['search_display'] == 'inline-flex' and seen['stop_display'] == 'none', seen
    assert seen['bar_hidden'], "the progress bar was left showing"


@pytest.mark.parametrize('catalog', CATALOGS)
def test_parallels_submit_with_unappliable_filters_runs_nothing_and_says_why(request, catalog):
    expected = request.getfixturevalue(catalog)
    engine = _Engine()
    seen = {}

    async def go():
        async with _user(engine, {'parallels_filter_date_from': 1000}) as user:
            await user.open('/parallels')
            text_box = next(e for e in _elements(user, ui.textarea)
                            if e.props.get('placeholder') == 'Paste your Hebrew text here...')
            with user.client:
                text_box.value = 'ברוך אתה יי אלהינו מלך'
            engine.calls.clear()
            user.notify.messages.clear()
            run_btn = next(b for b in _elements(user, ui.button)
                           if b.props.get('label') == 'Find Parallels')
            UserInteraction(user, {run_btn}, None).click()
            for _ in range(60):
                if expected in user.notify.messages or engine.calls:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)
            headers = [e.text for e in _elements(user, ui.element)
                       if getattr(e, 'text', None) in ('Results', 'Searching...')]
            seen.update(calls=list(engine.calls), notices=list(user.notify.messages),
                        run_enabled=run_btn.enabled, headers=headers)

    _run(go)
    assert seen['calls'] == [], f"the search ran although its filters could not be applied: {seen['calls']}"
    assert expected in seen['notices'], seen['notices']
    assert NO_MATCH not in seen['notices']
    assert seen['run_enabled'], "Find Parallels was left disabled"
    assert 'Results' in seen['headers'] and 'Searching...' not in seen['headers'], (
        f"the results header still says it is searching: {seen['headers']}")


def test_parallels_drops_a_saved_filter_the_catalog_lacks_with_a_notice(monkeypatch, tmp_path):
    """K-18: /parallels consumes load_filter_state's dropped keys."""
    monkeypatch.setattr(fjms_service, '_default_service', FjmsService(
        db_path=build_sidecar(tmp_path / 'no_lh.db', with_line_height=False)))
    engine = _Engine()
    notices = []

    async def go():
        async with _user(engine, {'parallels_filter_line_height_min': 3.0}) as user:
            await user.open('/parallels')
            notices.extend(user.notify.messages)

    _run(go)
    assert DROPPED_NOTICE in notices, notices


# -- search history never shares a list with the live filters --

def _found_lookup(monkeypatch, tmp_path):
    """A real catalog whose filter lookup answers, so a search can run. The
    small file has no domain tables, so the author/work option lists a chip
    removal refreshes are stubbed to empty."""
    svc = FjmsService(db_path=build_sidecar(tmp_path / 'ok.db'))
    monkeypatch.setattr(svc, 'get_filter_sys_ids', lambda **kwargs: {'990001'})
    monkeypatch.setattr(fjms_service, '_default_service', svc)
    for page in ('search', 'parallels'):
        for name in ('build_author_options', 'build_work_options'):
            monkeypatch.setattr(f'web.pages.{page}.{name}', lambda *a, **k: {})


def _chips(user, text):
    return [c for c in _elements(user, ui.chip) if c.text == text]


def _remove_chip(user, text):
    """Press the x on the chip showing ``text``; False when there is none.

    A text term has two chips (the panel's own row and the chip bar) that
    remove the same term, so the first one serves. The listeners are read
    from a snapshot: /parallels removes synchronously and rebuilds the chip
    bar inside the handler, which UserInteraction.trigger's live loop over
    the listener dict does not survive.
    """
    found = _chips(user, text)
    if not found:
        return False
    chip = found[0]
    with user.client:
        for listener in list(chip._event_listeners.values()):
            if listener.type == 'remove':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=chip, client=user.client, args={}))
    return True


def _saved_history_entry():
    return {'query': 'word', 'result_count': 0, 'mode': 'exact',
            'timestamp': '2026-10-06T10:00:00',
            'params': {'mode': 'exact', 'filters': {
                'domains': ['Halakha', 'Piyyut'], 'authors': [], 'works': [],
                'include_mode': True, 'date_from': None, 'date_to': None,
                'material_exclude': [], 'text_all': ['term'], 'text_any': [],
                'text_not': []}},
            'state': {}}


def test_search_history_restore_shares_no_list_with_the_saved_entry(monkeypatch, tmp_path):
    """Restore an entry from the history menu, then remove two chips."""
    _found_lookup(monkeypatch, tmp_path)
    history = [_saved_history_entry()]
    monkeypatch.setattr('web.pages.search.get_search_history', lambda: history)
    monkeypatch.setattr('web.pages.search.add_to_search_history', lambda **kwargs: None)
    engine = _Engine()
    seen = {}

    async def go():
        async with _user(engine, {}) as user:
            await user.open('/search')
            history_btn = next(b for b in _elements(user, ui.button)
                               if b.props.get('icon') == 'history')
            UserInteraction(user, {history_btn}, None).click()
            item = next(m for m in _elements(user, ui.menu_item)
                        if any(str(getattr(d, 'text', '')).startswith('word')
                               for d in m.descendants()))
            UserInteraction(user, {item}, None).click()
            for _ in range(60):
                if 'Re-running search from history' in user.notify.messages:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)
            seen['removed'] = (_remove_chip(user, 'Halakha'), _remove_chip(user, '+ term'))
            await asyncio.sleep(0.2)
            seen['left'] = len(_chips(user, 'Halakha'))

    _run(go)
    assert seen == {'removed': (True, True), 'left': 0}, (
        f"the restored chips were not shown and removed; nothing was exercised: {seen}")
    assert history == [_saved_history_entry()], (
        f"removing a chip changed the saved history entry: {history[0]['params']['filters']}")


def test_search_history_records_a_copy_of_the_live_filters(monkeypatch, tmp_path):
    """Run a search with saved filters, then remove a chip: the entry handed to
    the history must not change with it. (A live list loaded from storage is
    stored as-is by NiceGUI, so an entry holding it is the live list.)"""
    _found_lookup(monkeypatch, tmp_path)
    recorded = []
    monkeypatch.setattr('web.pages.search.add_to_search_history',
                        lambda **kwargs: recorded.append(kwargs['params']))
    engine = _Engine()
    seen = {}

    async def go():
        async with _user(engine, {'search_filter_domains': ['Halakha', 'Piyyut'],
                                  'search_filter_text_all': ['term']}) as user:
            await user.open('/search')
            query = next(e for e in _elements(user, ui.input)
                         if e.props.get('placeholder') == 'Enter Hebrew text to search')
            with user.client:
                query.value = 'word'
            UserInteraction(user, {_button(user, 'Search', 'px-8')}, None).click()
            for _ in range(60):
                if recorded:
                    break
                await asyncio.sleep(0.05)
            seen['notices'] = list(user.notify.messages)
            seen['removed'] = (_remove_chip(user, 'Halakha'), _remove_chip(user, '+ term'))
            await asyncio.sleep(0.2)
            seen['left'] = len(_chips(user, 'Halakha'))

    _run(go)
    assert recorded, f"the search never reached the history: {seen}"
    assert (seen['removed'], seen['left']) == ((True, True), 0), (
        f"the chips were not shown and removed; nothing was exercised: {seen}")
    filters = recorded[0]['filters']
    assert (filters['domains'], filters['text_all']) == (['Halakha', 'Piyyut'], ['term']), (
        f"removing a chip changed the recorded history entry: {filters}")


def _saved_comp_history_entry():
    return {'title': 'ברוך אתה', 'result_count': 1,
            'timestamp': '2026-10-06T10:00:00',
            'params': {'chunk_size': 5, 'mode': 'exact', 'filters': {
                'domains': ['Halakha', 'Piyyut'], 'authors': [], 'works': [],
                'include_mode': True, 'date_from': None, 'date_to': None,
                'material_exclude': [], 'text_all': ['term'], 'text_any': [],
                'text_not': []}},
            'state': {'source_text': 'ברוך אתה יי אלהינו מלך העולם'}}


def _storage_hook(monkeypatch, module, history_key, history=None, recorded=None):
    """Serve ``history`` for the page's history key and/or record what the
    page writes there; every other key goes to the real storage."""
    real_get = getattr(module, 'safe_user_get')
    real_set = getattr(module, 'safe_user_set')

    def _get(key, default=None):
        if history is not None and key == history_key:
            return history
        return real_get(key, default)

    def _set(key, value):
        if recorded is not None and key == history_key:
            recorded.append(value)
            return True
        return real_set(key, value)

    monkeypatch.setattr(module, 'safe_user_get', _get)
    monkeypatch.setattr(module, 'safe_user_set', _set)


def test_parallels_history_restore_shares_no_list_with_the_saved_entry(monkeypatch, tmp_path):
    """Restore an entry from the Composition History menu, then remove two chips."""
    import web.pages.parallels as parallels_page
    _found_lookup(monkeypatch, tmp_path)
    history = [_saved_comp_history_entry()]
    _storage_hook(monkeypatch, parallels_page, 'composition_history',
                  history=history, recorded=[])
    engine = _Engine()
    seen = {}

    async def go():
        async with _user(engine, {}) as user:
            await user.open('/parallels')
            history_btn = next(b for b in _elements(user, ui.button)
                               if b.props.get('label') == 'Composition History')
            UserInteraction(user, {history_btn}, None).click()
            item = next(m for m in _elements(user, ui.menu_item)
                        if any(str(getattr(d, 'text', '')).startswith('ברוך אתה')
                               for d in m.descendants()))
            UserInteraction(user, {item}, None).click()
            for _ in range(60):
                if 'Re-running composition from history' in user.notify.messages:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)
            seen['removed'] = (_remove_chip(user, 'Halakha'), _remove_chip(user, '+ term'))
            await asyncio.sleep(0.2)
            seen['left'] = len(_chips(user, 'Halakha'))

    _run(go)
    assert seen == {'removed': (True, True), 'left': 0}, (
        f"the restored chips were not shown and removed; nothing was exercised: {seen}")
    assert history == [_saved_comp_history_entry()], (
        f"removing a chip changed the saved history entry: {history[0]['params']['filters']}")


class _OneRowEngine(_Engine):
    """A composition run with one result, so /parallels records it in history."""

    def search_composition_logic(self, *args, **kwargs):
        result = super().search_composition_logic(*args, **kwargs)
        result['main'] = [{'uid': '990001_1', 'raw_header': '990001', 'score': 1,
                           'display': {'id': '990001', 'shelfmark': 'T-S 1.1', 'title': ''},
                           'text': '', 'source_text': '', 'matches': []}]
        return result


def test_parallels_history_records_a_copy_of_the_live_filters(monkeypatch, tmp_path):
    """Run a composition search with saved filters, then remove a chip: the
    entry written to the history must not change with it."""
    import web.pages.parallels as parallels_page
    _found_lookup(monkeypatch, tmp_path)
    recorded = []
    _storage_hook(monkeypatch, parallels_page, 'composition_history', recorded=recorded)
    engine = _OneRowEngine()
    seen = {}

    async def go():
        async with _user(engine, {'parallels_filter_domains': ['Halakha', 'Piyyut'],
                                  'parallels_filter_text_all': ['term']}) as user:
            await user.open('/parallels')
            text_box = next(e for e in _elements(user, ui.textarea)
                            if e.props.get('placeholder') == 'Paste your Hebrew text here...')
            with user.client:
                text_box.value = 'ברוך אתה יי אלהינו מלך'
            run_btn = next(b for b in _elements(user, ui.button)
                           if b.props.get('label') == 'Find Parallels')
            UserInteraction(user, {run_btn}, None).click()
            for _ in range(60):
                if recorded:
                    break
                await asyncio.sleep(0.05)
            seen['notices'] = list(user.notify.messages)
            seen['removed'] = (_remove_chip(user, 'Halakha'), _remove_chip(user, '+ term'))
            await asyncio.sleep(0.2)
            seen['left'] = len(_chips(user, 'Halakha'))

    _run(go)
    assert recorded, f"the search never reached the history: {seen}"
    assert (seen['removed'], seen['left']) == ((True, True), 0), (
        f"the chips were not shown and removed; nothing was exercised: {seen}")
    filters = recorded[0][0]['params']['filters']
    assert (filters['domains'], filters['text_all']) == (['Halakha', 'Piyyut'], ['term']), (
        f"removing a chip changed the recorded history entry: {filters}")
