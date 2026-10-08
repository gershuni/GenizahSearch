# -*- coding: utf-8 -*-
"""Interrupted ``run.io_bound`` calls are told apart from a callback's own None.

NiceGUI 3.8's ``run.io_bound`` returns None when its task is cancelled or the
app is stopping (tests/test_web_io_bound_none.py). Where a callback can itself
answer None (or nothing), the website now reads the call through
``web.io_bound_result``, which returns ``INTERRUPTED`` for "no answer". These
tests drive the real page handlers (NiceGUI User simulation, no index, no
network) through the review findings of 2026-10-07:

* a filter removed while its lookup waits for a worker does not abandon the
  search (/search, /parallels): the filters are read when Search is pressed;
* an interrupted filter lookup leaves no "Searching..." spinner on /search;
* an interrupted Show-only library lookup never runs /parallels over every
  library;
* an interrupted results fetch on /catalog-browse stops its spinner;
* the Joins Lab never says "Fragment not found" for a lookup that gave no answer;
* one gathered enrichment lookup with no answer never reaches the page's state;
* the Fragment Puzzle never saves an interrupted save again as a new document,
  and never says "Deleted" for a deletion that gave no answer;
* the identification review never says "sent" for a submission with no answer;
* (round 2, 2026-10-08) a refinement replay answers only for the chain it
  replayed: a chain cleared while its replay waits for a worker is not brought
  back as an invisible restriction that blocks the next search.
"""
from __future__ import annotations

import asyncio
import gc
import os
import sys
import threading

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'io-bound-none-findings-secret-0123456789abcdef')

from tests.test_web_io_bound_none import (  # noqa: E402
    _capture_errors, _card_shown, _io_bound_returning_none_for, _none_errors, _release,
)
from tests.test_web_search_within_cutoff import (  # noqa: E402,F401  (page is a fixture)
    WORD, _label_texts, _row, _wait_for, page,
)
from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    WORD2, WORD3, _element, run, server, store, submit, wait_for_payloads,
)


def test_the_helper_tells_no_answer_from_the_callbacks_own_none():
    """The contract every site below relies on, on the installed NiceGUI."""
    from web import io_bound_result

    async def main():
        own_none = await io_bound_result.io_bound(lambda: None)
        value = await io_bound_result.io_bound(lambda x, y=0: x + y, 1, y=2)
        gate = threading.Event()
        try:
            task = asyncio.ensure_future(io_bound_result.io_bound(gate.wait, 5))
            await asyncio.sleep(0.05)
            task.cancel()
            cancelled = await task
            timed_out = await asyncio.wait_for(io_bound_result.io_bound(gate.wait, 5), timeout=0.05)
            # One child of a gather cancelled on its own, the parent still waiting.
            child = asyncio.ensure_future(io_bound_result.io_bound(gate.wait, 5))
            gathered = asyncio.gather(child, io_bound_result.io_bound(lambda: 'peer'))
            await asyncio.sleep(0.05)
            child.cancel()
            gathered = await gathered
        finally:
            gate.set()
        return own_none, value, cancelled, timed_out, gathered

    own_none, value, cancelled, timed_out, gathered = asyncio.run(main())
    assert own_none is None and value == 3
    assert cancelled is io_bound_result.INTERRUPTED and timed_out is io_bound_result.INTERRUPTED
    assert not io_bound_result.INTERRUPTED
    assert gathered == [io_bound_result.INTERRUPTED, 'peer']
    assert io_bound_result.interrupted(None, cancelled) and not io_bound_result.interrupted(None, 0)


# --- /search ---------------------------------------------------------------------

def _record_search_states(monkeypatch):
    from web.pages import search as search_page
    states = []

    class Recording(search_page.SearchUIState):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            states.append(self)

    monkeypatch.setattr(search_page, 'SearchUIState', Recording)
    return states


class _HeldFilterLookup:
    """get_fjms_service whose call from the page's ``_compute_restrict`` waits until
    released -- a lookup queued behind busy workers. Everything else is the real
    service; get_filter_sys_ids keeps the real "nothing active -> None" contract."""

    def __init__(self, monkeypatch, answer):
        from shared import fjms_service
        self.real = fjms_service.get_fjms_service
        self.answer = answer
        self.entered, self.release = threading.Event(), threading.Event()
        self.kwargs = []
        monkeypatch.setattr(fjms_service, 'get_fjms_service', self)

    def __call__(self, *args, **kwargs):
        real = self.real(*args, **kwargs)
        if sys._getframe(1).f_code.co_name != '_compute_restrict':
            return real
        self.entered.set()
        self.release.wait(10)
        held = self

        class Service:
            def __getattr__(self, name):
                return getattr(real, name)

            def get_filter_sys_ids(self, **kwargs):
                held.kwargs.append(kwargs)
                active = any(v is not None and v != [] for v in kwargs.values())
                return set(held.answer) if active else None
        return Service()


def _restrict_sent(queue):
    return [set(p['arguments'].get('restrict_sys_ids') or ()) for p in queue.payloads]


def test_a_filter_removed_while_its_lookup_waits_still_runs_the_search_pressed(page, monkeypatch):  # noqa: F811
    """Finding 1 (/search). The visitor presses Search with one domain filter on;
    the lookup waits for a worker; meanwhile they remove that filter. The lookup
    read the filters inside the worker, found none, answered None ("no
    restriction") -- and the None guard took that for an interruption and silently
    dropped the search. The search pressed (with its filter) must run."""
    queue = page({(WORD, False): ([_row('M1')], None)})
    states = _record_search_states(monkeypatch)
    held = _HeldFilterLookup(monkeypatch, answer={'M1'})

    async def driver(a, b):
        try:
            await a.open('/search')
            search_state = states[-1]
            search_state.filter_domains = ['Liturgy']
            submit(a, WORD)
            await _wait_for(held.entered.is_set)
            search_state.filter_domains.clear()        # the chip's x, while the lookup waits
            held.release.set()
            await wait_for_payloads(queue, 1)
        finally:
            held.release.set()

    run(driver)
    assert held.kwargs and held.kwargs[0].get('domains') == ['Liturgy'], held.kwargs
    assert _restrict_sent(queue) == [{'M1'}]


def _results_spinner_shown(user):
    from nicegui import ui
    with user._client:
        return any(isinstance(e, ui.spinner) and e.tag == 'q-spinner-bars'
                   for e in user._client.elements.values())


def test_an_interrupted_filter_lookup_leaves_no_searching_spinner(page, monkeypatch):  # noqa: F811
    """Finding 2. The lookup gave no answer: no search runs (#17), and the results
    area must not keep "Searching..." with its spinner for a search that never ran."""
    from web.pages import search as search_page
    queue = page({(WORD, False): ([_row('M1')], None)})
    monkeypatch.setattr(search_page, 'has_active_filters', lambda state: True)
    seen = _io_bound_returning_none_for(monkeypatch, '_compute_restrict')
    shown = {}

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await _wait_for(lambda: seen == ['_compute_restrict'])
        await asyncio.sleep(0.5)
        shown['texts'] = _label_texts(a)
        shown['spinner'] = _results_spinner_shown(a)

    run(driver)
    assert queue.payloads == []
    assert 'Searching...' not in shown['texts'], shown['texts']
    assert not shown['spinner']
    assert 'Ready to search.' in shown['texts'], shown['texts']


def test_a_gathered_enrichment_lookup_with_no_answer_never_reaches_the_page(page, monkeypatch):  # noqa: F811
    """Finding 6. The first page's enrichment gathers six lookups; one of them gave
    no answer while the others did. Its None was committed as the page's
    transcription set, and the result cards then failed on it."""
    queue = page({(WORD, False): ([_row('M1')], None)})
    seen = _io_bound_returning_none_for(monkeypatch, 'get_sys_ids_with_transcriptions')
    errors = _capture_errors()
    states = _record_search_states(monkeypatch)

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: _card_shown(a, 'S-M1'), user=a)
        await _wait_for(lambda: 'get_sys_ids_with_transcriptions' in seen)
        await asyncio.sleep(0.5)                    # the enrichment's render, if any

    try:
        run(driver)
    finally:
        _release(errors)
    assert states[-1].transcription_sys_ids is not None
    assert _none_errors(errors) == [], errors.messages


# --- /search: a replay answers only for the chain it replayed (round 2) -------------

def _held_io_bound(monkeypatch, name, module='web.pages.search', fail=None):
    """run.io_bound whose call to the callback *name* waits, before it starts,
    until released -- a call queued behind busy workers -- and then runs (or, given
    *fail*, raises it, as a failed worker does). Every other call runs at once.
    ``done`` holds what the held call answered (or raised), once it has."""
    import importlib
    from types import SimpleNamespace

    from nicegui import run as nicegui_run
    held = SimpleNamespace(entered=threading.Event(), release=threading.Event(), done=[])

    async def io_bound(fn, *args, **kwargs):
        if getattr(fn, '__name__', '') != name:
            return await nicegui_run.io_bound(fn, *args, **kwargs)
        held.entered.set()
        while not held.release.is_set():
            await asyncio.sleep(0.02)
        if fail is not None:
            held.done.append(fail)
            raise fail
        value = await nicegui_run.io_bound(fn, *args, **kwargs)
        held.done.append(value)
        return value
    monkeypatch.setattr(importlib.import_module(module), 'run',
                        SimpleNamespace(io_bound=io_bound, cpu_bound=nicegui_run.cpu_bound))
    return held


def _sent(queue):
    return [(p['arguments'].get('query_str'), p['arguments'].get('restrict_sys_ids'))
            for p in queue.payloads]


async def _settle(predicate, timeout=5.0):
    """Wait until *predicate* holds or *timeout* passes, without raising: what the
    page did is asserted after run(). (A driver that raised mid-way, with a page
    handler still running, has left a test here hanging in NiceGUI's teardown -- an
    outbox loop that never finished its cancel -- instead of failing.)"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate() and loop.time() < deadline:
        await asyncio.sleep(0.05)


def test_a_chain_cleared_while_its_restore_replay_waits_is_not_brought_back(page, monkeypatch):  # noqa: F811
    """A reload replays the saved chain; the replay waits for a worker, and the
    visitor runs a new search meanwhile, which clears the chain. The replay read
    its chain before waiting: it then replayed the old chain (its step now matches
    nothing) and stored that empty set as the page's restriction -- with no chain
    left to show it, and an empty set is not cleared by the next search, which
    then said "No manuscripts match the current filters." and never ran."""
    queue = page({(WORD, False): ([], None),               # the old step, replayed: nothing
                  (WORD2, False): ([_row('M2')], None),
                  (WORD3, False): ([_row('M3')], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay')
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            store(a, search_refinement_chain=[{'query': WORD, 'mode': 'literal', 'gap': 0}])
            await a.open('/search')
            await _wait_for(held.entered.is_set)        # the restore's replay waits for a worker
            submit(a, WORD2)                            # a new search: the chain is cleared
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: _card_shown(a, 'S-M2'), user=a)
            held.release.set()
            await _wait_for(lambda: held.done)          # the old chain replayed: an empty set
            await asyncio.sleep(0.3)
            shown['chain'] = list(states[-1].refinement_chain)
            shown['restrict'] = states[-1].refinement_restrict_sys_ids
            submit(a, WORD3)
            await _settle(lambda: len(queue.payloads) >= 3)
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert held.done == [set()]
    assert shown == {'chain': [], 'restrict': None}, shown
    assert _sent(queue) == [(WORD2, None), (WORD, None), (WORD3, None)], _sent(queue)


def test_a_chain_cleared_while_a_chip_removal_replays_is_not_brought_back(page, monkeypatch):  # noqa: F811
    """Removing a chip replays the shorter chain; while that replay waits for a
    worker the visitor presses Clear all (still shown until the replay ends). The
    replay's answer for the old chain must not become the page's restriction."""
    from nicegui import ui

    from tests.test_web_search_within_cutoff import _click_search_within
    from tests.test_web_variant_settings_per_visitor import _fire
    queue = page({(WORD, False): ([_row('M1')], None),
                  (WORD2, False): ([_row('M1', page=2)], None),
                  (WORD3, False): ([_row('M3')], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay')
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            submit(a, WORD)
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD2)
            await wait_for_payloads(queue, 2)
            await _wait_for(lambda: [s.query for s in states[-1].refinement_chain] == [WORD, WORD2])
            await asyncio.sleep(0.3)                    # the strip shows both chips
            queue.answers[(WORD, False)] = ([], None)   # replayed now, WORD matches nothing
            _fire(a, _element(a, ui.chip, lambda e: e.text == WORD2), 'remove')
            await _wait_for(held.entered.is_set)        # the chain is [WORD]; its replay waits
            _click(a, _element(a, ui.button, lambda e: e.text == 'Clear all'))
            await _wait_for(lambda: not states[-1].refinement_chain)
            held.release.set()
            await _wait_for(lambda: held.done)          # [WORD] replayed: an empty set
            await asyncio.sleep(0.3)
            shown['chain'] = list(states[-1].refinement_chain)
            shown['restrict'] = states[-1].refinement_restrict_sys_ids
            submit(a, WORD3)
            await _settle(lambda: len(queue.payloads) >= 4)
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert held.done == [set()]
    assert shown == {'chain': [], 'restrict': None}, shown
    assert [q for q, _ in _sent(queue)] == [WORD, WORD2, WORD, WORD3], _sent(queue)
    assert _sent(queue)[3] == (WORD3, None)


def test_a_failed_restore_replay_never_clears_a_chain_built_since(page, monkeypatch):  # noqa: F811
    """The restore's replay waits; the visitor runs a new search and searches
    within its results. The old replay then fails: its failure path cleared "the"
    chain -- by then the visitor's new one -- while the page still said
    "Searching within 1 manuscripts", and the next search ran over everything."""
    from tests.test_web_search_within_cutoff import _click_search_within
    queue = page({(WORD2, False): ([_row('M2')], None),
                  (WORD3, False): ([_row('M2', page=3)], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay', fail=RuntimeError('the worker failed'))
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            store(a, search_refinement_chain=[{'query': WORD, 'mode': 'literal', 'gap': 0}])
            await a.open('/search')
            await _wait_for(held.entered.is_set)        # the restore's replay waits for a worker
            submit(a, WORD2)                            # a new search clears the restored chain
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
            _click_search_within(a)                     # ... and the visitor searches within it
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            held.release.set()                          # the old replay fails
            await _wait_for(lambda: held.done)
            await asyncio.sleep(0.3)
            shown['chain'] = [s.query for s in states[-1].refinement_chain]
            shown['restrict'] = states[-1].refinement_restrict_sys_ids
            submit(a, WORD3)
            await _settle(lambda: len(queue.payloads) >= 2)
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert shown == {'chain': [WORD2], 'restrict': {'M2'}}, shown
    assert [(q, set(r or ())) for q, r in _sent(queue)] == [(WORD2, set()), (WORD3, {'M2'})], _sent(queue)


def test_back_to_previous_step_runs_nothing_once_the_chain_is_cleared(page, monkeypatch):  # noqa: F811
    """After a search within found nothing, "Back to previous step" replays the
    chain without its last step, then runs that step's search within it. When the
    visitor presses Clear all while the replay waits, the page must not then run
    a search within the cleared chain's manuscripts."""
    from nicegui import ui

    from tests.test_web_search_within_cutoff import _click_search_within
    queue = page({(WORD, False): ([_row('M1')], None),
                  (WORD2, False): ([_row('M1', page=2)], None),
                  (WORD3, False): ([], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay')
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            submit(a, WORD)
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD2)
            await wait_for_payloads(queue, 2)
            await _wait_for(lambda: [s.query for s in states[-1].refinement_chain] == [WORD, WORD2])
            await asyncio.sleep(0.3)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD3)                            # nothing within: "Back to previous step"
            await wait_for_payloads(queue, 3)
            await _wait_for(lambda: any(t == 'Back to previous step' for t in _label_texts(a)), user=a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Back to previous step'))
            await _wait_for(held.entered.is_set)        # [WORD] replays; it waits for a worker
            _click(a, _element(a, ui.button, lambda e: e.text == 'Clear all'))
            await _wait_for(lambda: not states[-1].refinement_chain)
            held.release.set()
            await _wait_for(lambda: held.done)
            await _settle(lambda: len(queue.payloads) >= 5, timeout=2.0)
            shown['chain'] = list(states[-1].refinement_chain)
            shown['restrict'] = states[-1].refinement_restrict_sys_ids
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert held.done == [{'M1'}]
    assert [q for q, _ in _sent(queue)] == [WORD, WORD2, WORD3, WORD], _sent(queue)
    assert shown == {'chain': [], 'restrict': None}, shown


def test_a_new_search_clears_an_empty_refinement_restriction(page, monkeypatch):  # noqa: F811
    """A search that is not a search-within drops the refinement. An EMPTY
    restriction ("within no manuscripts") left with no chain was kept, being
    falsy, and blocked the search: it is dropped like any other."""
    queue = page({(WORD, False): ([_row('M1')], None)})
    states = _record_search_states(monkeypatch)

    async def driver(a, b):
        await a.open('/search')
        states[-1].refinement_restrict_sys_ids = set()
        submit(a, WORD)
        await _settle(lambda: queue.payloads)

    run(driver)
    assert _sent(queue) == [(WORD, None)], 'the search never ran'
    assert states[-1].refinement_restrict_sys_ids is None


# --- /parallels --------------------------------------------------------------------

def _parallels_ready(server):  # noqa: F811
    from shared.search_engine import SearchEngine
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine
    from web.state import state
    server.monkeypatch.setattr(FakeSearchEngine, 'search_composition_logic',
                               SearchEngine.search_composition_logic, raising=False)
    server.monkeypatch.setattr(state, 'is_ready', lambda: True)


def _find_parallels(user):
    from nicegui import events, ui
    text = _element(user, ui.textarea, lambda e: e.props.get('placeholder') == 'Paste your Hebrew text here...')
    with user._client:
        text.value = ' '.join([WORD, WORD2, WORD3] * 2)
    button = _element(user, ui.button, lambda e: e.text == 'Find Parallels')
    with user._client:
        for listener in button._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=button, client=user._client, args={}))
    return button


def _parallels_state():
    """The newest /parallels page state (a class local to the page factory)."""
    found = [o for o in gc.get_objects() if type(o).__name__ == 'ParallelsState']
    return found[-1]


def test_parallels_a_filter_removed_while_its_lookup_waits_still_runs_the_search_pressed(server):  # noqa: F811
    """Finding 1 (/parallels): as on /search, with the date filter -- read inside
    the worker until now (domains/authors/works were already read before it)."""
    _parallels_ready(server)
    held = _HeldFilterLookup(server.monkeypatch, answer={'M1'})

    async def driver(a, b):
        try:
            await a.open('/parallels')
            p_state = _parallels_state()
            p_state.filter_date_from = 900
            _find_parallels(a)
            await _wait_for(held.entered.is_set)
            p_state.filter_date_from = None             # the chip's x, while the lookup waits
            held.release.set()
            await wait_for_payloads(server.queue, 1)
        finally:
            held.release.set()

    run(driver)
    assert held.kwargs and held.kwargs[0].get('date_from') == 900, held.kwargs
    assert _restrict_sent(server.queue) == [{'M1'}]


def _show_only_lookup(server, answer):  # noqa: F811
    from shared import fjms_service

    def resolve_library_sys_ids(library_codes, meta_mgr):
        return set(answer)
    server.monkeypatch.setattr(fjms_service, 'resolve_library_sys_ids', resolve_library_sys_ids)


def test_parallels_show_only_runs_within_the_selected_libraries(server):  # noqa: F811
    """The control for the next test: answered, Show-only scopes the search."""
    _parallels_ready(server)
    _show_only_lookup(server, {'M7'})

    async def driver(a, b):
        await a.open('/parallels')
        store(a, parallels_library_filter={'mode': 'show_only', 'codes': ['CUL']})
        await a.open('/parallels')
        _find_parallels(a)
        await wait_for_payloads(server.queue, 1)

    run(driver)
    assert _restrict_sent(server.queue) == [{'M7'}]


def test_parallels_an_interrupted_show_only_lookup_never_searches_every_library(server):  # noqa: F811
    """Finding 3. Show-only is applied ONLY before the search (no later pass); a
    lookup with no answer skipped it, and the search ran over every library."""
    from nicegui import ui
    _parallels_ready(server)
    _show_only_lookup(server, {'M7'})
    seen = _io_bound_returning_none_for(server.monkeypatch, 'resolve_library_sys_ids',
                                        pages=('web.pages.parallels',))
    shown = {}

    async def driver(a, b):
        await a.open('/parallels')
        store(a, parallels_library_filter={'mode': 'show_only', 'codes': ['CUL']})
        await a.open('/parallels')
        _find_parallels(a)
        await _wait_for(lambda: seen == ['resolve_library_sys_ids'])
        await asyncio.sleep(0.5)                    # time for a search to reach the queue
        button = _element(a, ui.button, lambda e: e.text == 'Find Parallels')
        shown['enabled'] = button.enabled
        shown['running'] = _parallels_state().is_running

    run(driver)
    assert server.queue.payloads == [], _restrict_sent(server.queue)
    assert shown == {'enabled': True, 'running': False}


# --- /catalog-browse -------------------------------------------------------------

def test_catalog_an_interrupted_results_fetch_stops_the_spinner(server):  # noqa: F811
    """Finding 4. The results fetch gave no answer: the spinner (and the top
    loading bar) must stop, not turn forever. Driven from the transcriptions
    filter button: the page's first load runs in a task without the page's
    slot, where its first ui.run_javascript raises before any fetch."""
    from nicegui import events, ui
    seen = _io_bound_returning_none_for(server.monkeypatch, '_fetch_results_blocking',
                                        pages=('web.pages.catalog_browse',))
    shown = {}

    async def driver(a, b):
        await a.open('/catalog-browse?domain=Liturgy')
        button = _element(a, ui.button, lambda e: e.text == 'Filter Scholarly Transcriptions')
        with a._client:
            for listener in button._event_listeners.values():
                if listener.type == 'click':
                    events.handle_event(listener.handler, events.GenericEventArguments(
                        sender=button, client=a._client, args={}))
        await _wait_for(lambda: '_fetch_results_blocking' in seen)
        await asyncio.sleep(0.5)
        with a._client:
            shown['spinners'] = [e.visible for e in a._client.elements.values()
                                 if isinstance(e, ui.spinner) and e.tag == 'q-spinner-dots']

    run(driver)
    assert shown['spinners'] and not any(shown['spinners']), shown


# --- /joins-lab ----------------------------------------------------------------------

def _click(user, element):
    from nicegui import events
    with user._client:
        # A copy: a handler may delete its own button (Clear all rebuilds its strip).
        for listener in list(element._event_listeners.values()):
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args={}))


def _joins_lab_io_bound(monkeypatch, decide):
    """run.io_bound for web.pages.joins_lab and web.io_bound_result: ``decide(fn)``
    returns 'none' (no answer, as NiceGUI gives for a cancel), 'run', or an
    awaitable to await first and then answer None."""
    from types import SimpleNamespace

    from nicegui import run as nicegui_run

    from web import io_bound_result
    from web.pages import joins_lab

    async def io_bound(fn, *args, **kwargs):
        verdict = decide(getattr(fn, '__wrapped__', fn))
        if verdict == 'run':
            return await nicegui_run.io_bound(fn, *args, **kwargs)
        if verdict != 'none':
            await verdict
        return None

    fake = SimpleNamespace(io_bound=io_bound, cpu_bound=nicegui_run.cpu_bound)
    for module in (joins_lab, io_bound_result):
        monkeypatch.setattr(module, 'run', fake)


def _names_in(fn):
    code = getattr(fn, '__code__', None)
    return set(code.co_names) | set(code.co_freevars) if code else set()


_ANCHOR_BOX = 'Shelfmark or fragment ID (e.g. T-S 12.123)'


def test_joins_lab_a_shelfmark_lookup_with_no_answer_is_not_fragment_not_found(server, monkeypatch):  # noqa: F811
    """Finding 5. "Fragment not found" is a claim about the catalogue; a lookup
    that gave no answer (cancelled, or the app is stopping) must not make it."""
    from nicegui import ui
    asked = []

    def decide(fn):
        if 'search_by_shelfmark' in _names_in(fn):
            asked.append(fn)
            return 'none'
        return 'run'
    _joins_lab_io_bound(monkeypatch, decide)
    shown = {}

    async def driver(a, b):
        await a.open('/joins-lab')
        box = _element(a, ui.input, lambda e: e.props.get('placeholder') == _ANCHOR_BOX)
        with a._client:
            box.value = 'T-S 1.1'
        _click(a, _element(a, ui.button, lambda e: e.text == 'Load Anchor'))
        await _wait_for(lambda: asked)
        await asyncio.sleep(0.3)
        shown['texts'] = _label_texts(a)
        shown['loading'] = _element(a, ui.button, lambda e: e.text == 'Load Anchor').props.get('loading')

    run(driver)
    assert not any('Fragment not found' in t for t in shown['texts']), shown['texts']
    assert not shown['loading']


def test_joins_lab_an_answered_lookup_with_no_match_still_says_fragment_not_found(server, monkeypatch):  # noqa: F811
    """The control: a lookup that answered "no match" keeps its message."""
    from types import SimpleNamespace

    from nicegui import ui

    from web.pages import joins_lab
    monkeypatch.setattr(joins_lab, 'get_service',
                        lambda: SimpleNamespace(search_by_shelfmark=lambda q, limit=20: ([], 0)))
    shown = {}

    async def driver(a, b):
        await a.open('/joins-lab')
        box = _element(a, ui.input, lambda e: e.props.get('placeholder') == _ANCHOR_BOX)
        with a._client:
            box.value = 'T-S 1.1'
        _click(a, _element(a, ui.button, lambda e: e.text == 'Load Anchor'))
        await _wait_for(lambda: any('Fragment not found' in t for t in _label_texts(a)))
        shown['ok'] = True

    run(driver)
    assert shown == {'ok': True}


async def _type_joins_query(user):
    """A word in the builders' query boxes: the word box of the Responsa-style
    lines and the single-line box, whichever search type is shown (the page has
    two builders; the other side is off, so it is the same search). After the
    page's start-up restore, which rebuilds the builder."""
    from nicegui import ui

    from tests.test_web_variant_settings_per_visitor import _fire
    await asyncio.sleep(1.5)
    with user._client:
        boxes = [e for e in user._client.elements.values()
                 if isinstance(e, ui.input) and e.props.get('placeholder') in (
                     'Search in Responsa syntax', 'Enter Hebrew text to search')]
    assert boxes, 'no builder word box'
    for box in boxes:
        _fire(user, box, 'update:modelValue', WORD)


def _status(user):
    return [t for t in _label_texts(user) if t.startswith(('Search timed out', 'Search failed', 'Searching'))]


def test_joins_lab_a_search_past_its_time_limit_says_so(server, monkeypatch):  # noqa: F811
    """The production failure the earlier guard fixed, end to end with NiceGUI's own
    run.io_bound: asyncio.wait_for's time limit cancels the call, run.io_bound
    swallows that and returns None, and the search must say it timed out (and give
    the Run button back) instead of failing on None."""
    from nicegui import ui

    from web.joins_executor import WebSearchExecutor
    from web.pages import joins_lab
    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    monkeypatch.setattr(joins_lab, '_SEARCH_TIMEOUT_SECONDS', 0.3)
    release = threading.Event()

    def slow_search(self, *args, **kwargs):
        release.wait(5)
        return []
    monkeypatch.setattr(WebSearchExecutor, 'execute_search', slow_search)
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            await _type_joins_query(a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _wait_for(lambda: any(t.startswith(('Search timed out', 'Search failed')) for t in _status(a)))
            await asyncio.sleep(0.1)
            shown['status'] = _status(a)
            shown['run_visible'] = _element(a, ui.button, lambda e: e.text == 'Run Search').visible
        finally:
            release.set()

    run(driver)
    assert shown['status'] == ['Search timed out. Try fewer or shorter lines.'], shown
    assert shown['run_visible']


def test_joins_lab_an_other_side_search_past_its_time_limit_says_so(server, monkeypatch):  # noqa: F811
    """As above, for the other side of the leaf (round-2 advice): its own time limit
    makes run.io_bound return None too, and the page must say that leg timed out --
    not "Could not resolve the other side", which is what reading None as a result
    gives."""
    from nicegui import ui

    from web.joins_executor import WebSearchExecutor
    from web.pages import joins_lab
    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    monkeypatch.setattr(joins_lab, '_SEARCH_TIMEOUT_SECONDS', 0.5)
    release = threading.Event()
    calls = []

    def search(self, *args, **kwargs):
        calls.append(args[:1])
        if len(calls) > 1:              # the other side's search: past its time limit
            release.wait(5)
        return []
    monkeypatch.setattr(WebSearchExecutor, 'execute_search', search)
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            await _type_joins_query(a)              # both builders, this side and the other
            other_side = _element(a, ui.checkbox, lambda e: e.text == 'Search the other side of the leaf')
            with a._client:
                other_side.value = True
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _settle(lambda: any('timed out' in m or 'Could not resolve' in m
                                      for m in a.notify.messages))
            shown['calls'] = len(calls)
            shown['messages'] = [m for m in a.notify.messages
                                 if m.startswith(('Other-side search', 'Could not resolve'))]
        except Exception as exc:                    # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            release.set()
            await asyncio.sleep(0.2)

    run(driver)
    assert shown == {'calls': 2,
                     'messages': ['Other-side search timed out — showing this-side results only.']}, shown


def test_joins_lab_a_visual_similarity_service_that_answers_unavailable_says_so(server, monkeypatch):  # noqa: F811
    """The availability probe answering False (a real answer, not an interruption)
    must still disable the switch and say "unavailable" (round-2 advice: reading
    the probe with ``if not available`` instead of ``is None`` passed every test)."""
    from nicegui import ui

    from web.pages import joins_lab
    monkeypatch.setattr(joins_lab, '_check_vs_service_available', lambda: False)
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            switch = _element(a, ui.switch, lambda e: e.text == 'Visual Similarity')
            with a._client:
                switch.value = True
            await _settle(lambda: 'Visual similarity unavailable' in _label_texts(a))
            shown['said'] = 'Visual similarity unavailable' in _label_texts(a)
            shown['switch_enabled'] = switch.enabled
        except Exception as exc:                    # asserted below (see _settle)
            shown['error'] = repr(exc)

    run(driver)
    assert shown == {'said': True, 'switch_enabled': False}, shown


def test_joins_lab_a_superseded_search_with_no_answer_stays_quiet(server, monkeypatch):  # noqa: F811
    """A search that ends with no answer just as a newer search starts must leave
    the status line to the newer one: no "timed out" over a search that runs."""
    from nicegui import ui

    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    holder = {}
    calls = []

    def decide(fn):
        if getattr(fn, '__name__', '') != 'run_search_core':
            return 'run'
        calls.append(fn)
        gate = asyncio.Event()
        holder['gate' if len(calls) == 1 else 'second'] = gate
        return gate.wait()          # 1st: the old search, no answer; 2nd: still running
    _joins_lab_io_bound(monkeypatch, decide)
    shown = {}

    async def driver(a, b):
        await a.open('/joins-lab')
        await _type_joins_query(a)
        run_button = _element(a, ui.button, lambda e: e.text == 'Run Search')
        _click(a, run_button)
        await _wait_for(lambda: 'gate' in holder)
        # The old call gives no answer at the moment the newer search is started.
        holder['gate'].set()
        _click(a, run_button)
        await _wait_for(lambda: len(calls) == 2)
        await asyncio.sleep(0.3)
        shown['status'] = _status(a)
        holder['second'].set()
        await asyncio.sleep(0.2)

    run(driver)
    assert shown['status'] == ['Searching...'], shown


# --- /puzzle -------------------------------------------------------------------------

def _puzzle_io_bound(monkeypatch, decide):
    """run.io_bound for web.pages.puzzle and web.io_bound_result: ``decide(fn)``
    returns 'none' (no answer: the callback does not run) or 'run'."""
    from types import SimpleNamespace

    from nicegui import run as nicegui_run

    from web import io_bound_result
    from web.pages import puzzle

    async def io_bound(fn, *args, **kwargs):
        if decide(getattr(fn, '__wrapped__', fn)) == 'none':
            return None
        return await nicegui_run.io_bound(fn, *args, **kwargs)

    fake = SimpleNamespace(io_bound=io_bound, cpu_bound=nicegui_run.cpu_bound)
    for module in (puzzle, io_bound_result):
        monkeypatch.setattr(module, 'run', fake)


def _puzzle_patches(**published):
    from unittest.mock import MagicMock, patch
    detail = published.get('detail')
    return [
        patch('shared.puzzle_publish_service.get_published_join_detail',
              side_effect=lambda client, join_id: dict(detail) if detail else None),
        patch('web.supabase_client.get_client', return_value=MagicMock()),
        patch('web.supabase_client.get_user_client', return_value=MagicMock()),
        patch('shared.puzzle_export.generate_thumbnail', return_value=''),
        patch('shared.puzzle_image_service.get_puzzle_image_service', return_value=MagicMock()),
    ]


def _run_puzzle(driver, patches):
    from contextlib import ExitStack

    from tests.test_web_saved_joins_isolation import _run as run_puzzle
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        run_puzzle(driver)


def _own_doc(doc_id='doc-own'):
    from shared.puzzle_model import PuzzleDocument, PuzzleFragment
    from tests.test_web_saved_joins_isolation import _fragment_dict
    return PuzzleDocument(id=doc_id, title='Mine', notes='',
                          fragments=[PuzzleFragment(**_fragment_dict())])


def test_puzzle_an_interrupted_save_is_never_saved_again_as_a_new_document(monkeypatch, tmp_path):
    """Finding 7. Saving the visitor's open document gave no answer. The page took
    that for "not this visitor's document" and saved the canvas again under a new
    id -- a second draft whenever the first save had in fact completed."""
    import shared.puzzle_service as ps
    from tests.test_web_saved_joins_isolation import _save_canvas, _seed, _visitor_key
    svc = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, '_service_instance', svc)
    attempts = []

    def decide(fn):
        if '_save_doc_with_thumbnail' in _names_in(fn):
            attempts.append(fn)
            return 'none' if len(attempts) == 1 else 'run'
        return 'run'
    _puzzle_io_bound(monkeypatch, decide)
    shown = {}

    async def driver(a, b):
        await a.open('/puzzle')
        assert _seed(svc, _own_doc(), _visitor_key(a)) == 'doc-own'
        await a.open('/puzzle?doc=doc-own')
        await asyncio.sleep(3.2)        # the page loads ?doc= after its start-up delay
        shown['dialog'] = await _save_canvas(a)
        shown['messages'] = list(a.notify.messages)

    try:
        _run_puzzle(driver, _puzzle_patches())
        ids = {d['id'] for d in svc.list_documents()}
    finally:
        if svc._conn is not None:
            svc._conn.close()
    assert shown['dialog'], 'the save dialog did not open'
    assert len(attempts) == 1, f'saved {len(attempts)} times'
    assert ids == {'doc-own'}, ids
    assert 'Could not save this join' in shown['messages'], shown['messages']


def test_puzzle_a_deletion_with_no_answer_never_says_deleted(monkeypatch, tmp_path):
    """Finding 8 (deletion). The page announced "Deleted" whatever the deletion's
    call returned -- also when it gave no answer at all."""
    from nicegui import ui
    from nicegui.testing.user_interaction import UserInteraction

    import shared.puzzle_service as ps
    from tests.test_web_saved_joins_isolation import _open_drawer, _seed, _visitor_key
    svc = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, '_service_instance', svc)
    deletes = []

    def decide(fn):
        if getattr(fn, '__name__', '') == 'delete_document':
            deletes.append(fn)
            return 'none'
        return 'run'
    _puzzle_io_bound(monkeypatch, decide)
    shown = {}

    def _buttons(user, pred):
        return [el for el in user._client.elements.values() if isinstance(el, ui.button) and pred(el)]

    async def driver(a, b):
        await a.open('/puzzle')
        assert _seed(svc, _own_doc(), _visitor_key(a)) == 'doc-own'
        before = set(map(id, _buttons(a, lambda el: el.props.get('icon') == 'delete')))
        await _open_drawer(a)
        row_delete = [el for el in _buttons(a, lambda el: el.props.get('icon') == 'delete') if id(el) not in before]
        assert row_delete, 'no delete button on the saved join'
        UserInteraction(a, {row_delete[0]}, None).click()
        confirm = []
        for _ in range(40):
            await asyncio.sleep(0.05)
            confirm = _buttons(a, lambda el: el.text == 'Delete')
            if confirm:
                break
        assert confirm, 'no confirmation dialog'
        UserInteraction(a, {confirm[-1]}, None).click()
        await _wait_for(lambda: deletes)
        await asyncio.sleep(0.5)
        shown['messages'] = list(a.notify.messages)

    try:
        _run_puzzle(driver, _puzzle_patches())
    finally:
        if svc._conn is not None:
            svc._conn.close()
    assert 'Deleted' not in shown['messages'], shown['messages']
    assert 'The change could not be saved. Check your connection and try again.' in shown['messages']


def test_puzzle_an_unpublish_with_no_answer_never_says_unpublished(monkeypatch, tmp_path):
    """Finding 8. unpublish_join returns nothing when it is done, so the page could
    not tell done from no answer, and announced "Unpublished" for both."""
    from nicegui import ui
    from nicegui.testing.user_interaction import UserInteraction

    import shared.puzzle_publish_service as pps
    import shared.puzzle_service as ps
    from tests.test_web_saved_joins_isolation import _seed
    from web.auth_state import GlobalAuthState
    svc = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, '_service_instance', svc)
    monkeypatch.setattr(GlobalAuthState, 'get_user', classmethod(lambda cls: {'id': 'u-1'}))
    unpublished = []

    def unpublish_join(client, user_id, join_id):
        unpublished.append(join_id)
    monkeypatch.setattr(pps, 'unpublish_join', unpublish_join)
    asked = []

    def decide(fn):
        if getattr(fn, '__name__', '') == 'unpublish_join':
            asked.append(fn)
            return 'none'
        return 'run'
    _puzzle_io_bound(monkeypatch, decide)
    detail = {'id': 'doc-own', 'title': 'Mine', 'notes': '', 'is_published': True,
              'fragments_json': {'fragments': [], 'join_type': 'physical'}}
    shown = {}

    async def driver(a, b):
        assert _seed(svc, _own_doc(), 'u:u-1') == 'doc-own'
        await a.open('/puzzle?doc=doc-own')
        await asyncio.sleep(3.2)        # the page loads ?doc= after its start-up delay
        publish = [el for el in a._client.elements.values()
                   if isinstance(el, ui.button) and el.props.get('icon') == 'publish']
        assert publish and publish[0].props.get('color') == 'green', 'the join is not shown as published'
        UserInteraction(a, {publish[0]}, None).click()
        await _wait_for(lambda: asked)
        await asyncio.sleep(0.3)
        shown['messages'] = list(a.notify.messages)
        shown['color'] = publish[0].props.get('color')

    try:
        _run_puzzle(driver, _puzzle_patches(detail=detail))
    finally:
        if svc._conn is not None:
            svc._conn.close()
    assert 'Unpublished' not in shown['messages'], shown['messages']
    assert shown['color'] == 'green', 'the page shows the join unpublished'
    assert unpublished == []


# --- the identification review dialog -------------------------------------------------

_REVIEW_PAGE = '/__io-bound-none-review'


def _register_review_page():
    """A bare page with one review action (registered once per process)."""
    from nicegui import core, ui
    if any(getattr(route, 'path', None) == _REVIEW_PAGE for route in core.app.routes):
        return

    @ui.page(_REVIEW_PAGE)
    def _review_page():
        from web.components import identification_review as review
        review.render_identification_review_action(
            {'identification_id': 'idf-1', 'sys_id': '990001', 'page_id': 'p-1'},
            'en', sidecar_version='v-test')


def test_review_a_submission_with_no_answer_never_says_sent(server, monkeypatch):  # noqa: F811
    """Finding 8 (review submission). The dialog closed and thanked the reader
    whatever the submission's call returned -- also when it gave no answer."""
    from unittest.mock import MagicMock

    from nicegui import ui

    from web.components import identification_review as review
    monkeypatch.setattr(review, 'reviews_enabled', lambda: True)
    monkeypatch.setattr(review, 'get_user_client', lambda: MagicMock())
    monkeypatch.setattr(review, 'get_session_uuid', lambda: '0123456789abcdef' * 2)

    def submit_review(submission, *, client):
        return {'status': 'pending'}
    monkeypatch.setattr(review, 'submit_review', submit_review)
    seen = _io_bound_returning_none_for(monkeypatch, 'submit_review',
                                        pages=('web.components.identification_review',))
    _register_review_page()
    shown = {}

    async def driver(a, b):
        await a.open(_REVIEW_PAGE)
        _click(a, _element(a, ui.button, lambda e: e.text == review.review_text('action', 'en')))
        await asyncio.sleep(0.2)
        radio = _element(a, ui.radio, lambda e: review.RELATION_NOT_MEANINGFUL in (e.options or {}))
        with a._client:
            radio.value = review.RELATION_NOT_MEANINGFUL
        _click(a, _element(a, ui.button, lambda e: e.text == review.review_text('submit', 'en')))
        await _wait_for(lambda: seen)
        await asyncio.sleep(0.2)
        shown['messages'] = list(a.notify.messages)

    run(driver)
    assert review.review_text('success', 'en') not in shown['messages'], shown['messages']
    assert review.review_text('unavailable', 'en') in shown['messages'], shown['messages']


# --- the filter selects (Codex question 3) -----------------------------------------------

def test_an_author_list_refresh_with_no_answer_stops_saying_loading(page, monkeypatch):  # noqa: F811
    """A domain change reloads the author list with the select marked 'loading'.
    When that load gave no answer, the select kept its spinner for good."""
    from nicegui import ui

    from tests.test_web_variant_settings_per_visitor import _fire
    page({})
    seen = _io_bound_returning_none_for(monkeypatch, 'build_author_options')
    shown = {}

    async def driver(a, b):
        await a.open('/search')
        await _wait_for(lambda: seen)               # the page's own first load (quiet)
        with a._client:
            # Domain, Author, Work: the first three chip selects, in that order.
            selects = [e for e in a._client.elements.values()
                       if isinstance(e, ui.select) and e.props.get('use-chips')]
        assert len(selects) >= 3
        domain, author = selects[0], selects[1]
        _fire(a, domain, 'update:modelValue')       # the visitor changes the domains
        await _wait_for(lambda: len(seen) >= 2)
        await asyncio.sleep(0.3)
        shown['loading'] = author.props.get('loading')

    run(driver)
    assert not shown['loading']


@pytest.mark.parametrize('disrupt', ['clear all', 'new search'])
def test_back_to_previous_step_whose_replay_fails_runs_nothing_for_a_changed_chain(
        page, monkeypatch, disrupt):  # noqa: F811
    """Codex round 3: the replay behind "Back to previous step" can fail (a time
    limit, a worker error). When the visitor has meanwhile pressed Clear all or
    started another search, the failure path must not switch refinement back on
    and search within the old chain's manuscripts."""
    from nicegui import ui

    from shared.search_regex import SearchBudgetExceeded
    from tests.test_web_search_within_cutoff import _click_search_within
    queue = page({(WORD, False): ([_row('M1')], None),
                  (WORD2, False): ([_row('M1', page=2)], None),
                  (WORD3, False): ([], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay', fail=SearchBudgetExceeded())
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            submit(a, WORD)
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD2)
            await wait_for_payloads(queue, 2)
            await _wait_for(lambda: [s.query for s in states[-1].refinement_chain] == [WORD, WORD2])
            await asyncio.sleep(0.3)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD3)                            # nothing within: "Back to previous step"
            await wait_for_payloads(queue, 3)
            await _wait_for(lambda: any(t == 'Back to previous step' for t in _label_texts(a)), user=a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Back to previous step'))
            await _wait_for(held.entered.is_set)        # [WORD] replays; it waits for a worker
            if disrupt == 'clear all':
                _click(a, _element(a, ui.button, lambda e: e.text == 'Clear all'))
                await _wait_for(lambda: not states[-1].refinement_chain)
                expected = 3
            else:
                submit(a, WORD)                         # a fresh search replaces the chain
                await wait_for_payloads(queue, 4)
                expected = 4
            held.release.set()                          # ... and the replay now fails
            await _wait_for(lambda: held.done)
            await _settle(lambda: len(queue.payloads) > expected, timeout=2.0)
            shown['sent'] = len(queue.payloads) - expected
            shown['refine_mode'] = bool(states[-1]._refine_mode)
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert shown == {'sent': 0, 'refine_mode': False}, (shown, _sent(queue))


def test_back_to_previous_step_with_its_chain_unchanged_searches_once_within_it(page, monkeypatch):  # noqa: F811
    """The positive case next to the ones above (Codex round 4): with nothing
    changed meanwhile, "Back to previous step" replays the remaining chain and then
    runs the last step's search once, within the replayed manuscripts."""
    from nicegui import ui

    from tests.test_web_search_within_cutoff import _click_search_within
    queue = page({(WORD, False): ([_row('M1')], None),
                  (WORD2, False): ([_row('M1', page=2)], None),
                  (WORD3, False): ([], None)})
    states = _record_search_states(monkeypatch)
    held = _held_io_bound(monkeypatch, '_do_replay')
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/search')
            submit(a, WORD)
            await wait_for_payloads(queue, 1)
            await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD2)
            await wait_for_payloads(queue, 2)
            await _wait_for(lambda: [s.query for s in states[-1].refinement_chain] == [WORD, WORD2])
            await asyncio.sleep(0.3)
            _click_search_within(a)
            await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
            submit(a, WORD3)                            # nothing within: "Back to previous step"
            await wait_for_payloads(queue, 3)
            await _wait_for(lambda: any(t == 'Back to previous step' for t in _label_texts(a)), user=a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Back to previous step'))
            await _wait_for(held.entered.is_set)
            held.release.set()                          # nothing changed meanwhile
            await _wait_for(lambda: held.done)
            await _settle(lambda: len(queue.payloads) > 5, timeout=2.0)
            shown['sent'] = [(q, None if r is None else set(r)) for q, r in _sent(queue)[3:]]
        except Exception as exc:                        # asserted below (see _settle)
            shown['error'] = repr(exc)
        finally:
            held.release.set()
            await asyncio.sleep(0.3)

    run(driver)
    assert held.done == [{'M1'}]
    assert shown == {'sent': [(WORD, None), (WORD2, {'M1'})]}, (shown, _sent(queue))
