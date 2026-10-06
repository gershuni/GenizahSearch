# -*- coding: utf-8 -*-
"""The website says when a result list was cut off, and search-within completes it.

Every search reads at most Config.SEARCH_LIMIT candidates per query. The desktop
has shown such a count as "N+" and completed a cut-off step before searching
within it since v9.5.0 (D8); these tests hold the website to the same:

* the worker's result carries the engine's cut-off signal, and IsolatedEngine
  hands it to the calling thread (a failed or stopped call leaves none);
* /search shows "N+" for a cut-off list and keeps it in the session snapshot;
* "Search within" over a cut-off list first reads every match of that step
  (ids_only), so a manuscript the shown rows did not reach is still searched.

The page tests drive the real /search handlers (NiceGUI User simulation) with a
recording job queue in place of the worker processes. No index, no network.
"""
from __future__ import annotations

import asyncio
import gzip
import os
import pickle
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'search-within-cutoff-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402
    FakeSearchEngine, run, submit, wait_for_payloads,
)

WORD = 'אבגד'
WORD2 = 'הוזח'


def _row(sys_id, page=1):
    return {'uid': f'{sys_id}_{page}', 'raw_header': f'{sys_id}_{page}',
            'display': {'id': sys_id, 'shelfmark': f'S-{sys_id}', 'title': '', 'source': 'V0.8',
                        'library': ''},
            'snippet': 'אבגד', 'full_text': 'אבגד', 'source': 'V0.8'}


class AnsweringQueue:
    """Stands in for ResearchQueue: answers each job by its query, records them all."""

    def __init__(self, answers):
        self.answers = answers          # (query, ids_only) -> (rows, cutoff)
        self.payloads = []
        self.lock = threading.Lock()

    def submit(self, payload):
        from web.research_jobs import Job
        with self.lock:
            self.payloads.append(payload)
        args = payload['arguments']
        rows, cutoff = self.answers.get((args.get('query_str'), bool(args.get('ids_only'))), ([], None))
        job = Job(payload)
        job.future.set_result({'value': [dict(r) for r in rows],
                               'cutoff': cutoff or {'capped': False, 'interrupted': False}})
        return job

    def position(self, job):
        return 0

    def cancel(self, job):
        pass


@pytest.fixture
def page(monkeypatch):
    from shared.lab_settings import LabSettings
    from shared.variants import VariantManager
    from web import research_jobs
    from web.research_jobs import IsolatedEngine
    from web.state import state

    monkeypatch.setattr(LabSettings, 'load', lambda self: None)
    monkeypatch.setattr(LabSettings, 'save', lambda self: None)
    settings = LabSettings()
    var_mgr = VariantManager(settings)
    holder = SimpleNamespace(queue=None)

    def use(answers):
        holder.queue = AnsweringQueue(answers)
        monkeypatch.setattr(research_jobs, 'get_queue', lambda: holder.queue)
        return holder.queue

    monkeypatch.setattr(research_jobs, '_wait_executor', None)
    monkeypatch.setattr(state, 'var_mgr', var_mgr)
    monkeypatch.setattr(state, 'searcher', IsolatedEngine(FakeSearchEngine(var_mgr), 'search'))
    monkeypatch.setattr(state, 'lab_engine',
                        IsolatedEngine(SimpleNamespace(settings=settings, var_mgr=var_mgr), 'lab'))
    monkeypatch.setattr(state, 'meta_mgr', SimpleNamespace(csv_bank={}))
    return use


# --- The worker and the transport -------------------------------------------

def test_worker_result_carries_the_engines_cut_off(tmp_path, monkeypatch):
    from shared import research_worker
    from shared.search_engine import _note_search_cutoff

    class Engine:
        def __init__(self, meta, variants, *, worker_mode=False, open_local=True):
            self.searcher = object()

        def execute_search(self, query_str, mode, gap, progress_callback=None, corpus_scope='all'):
            _note_search_cutoff(capped=True)
            return [{'uid': 'p1'}]
    monkeypatch.setattr('shared.search_engine.SearchEngine', Engine)
    monkeypatch.setenv('GENIZAH_RESEARCH_MEMORY_MB', '512')
    payload = {'kind': 'search', 'method': 'execute_search', 'settings': None,
               'arguments': {'query_str': WORD, 'mode': 'literal', 'gap': 0, 'corpus_scope': 'genizah'}}
    research_worker.run_query(tmp_path, payload, lambda *a: None, meta=SimpleNamespace())
    with gzip.open(tmp_path / 'output.pkl', 'rb') as stream:
        result = pickle.load(stream)
    assert result['cutoff'] == {'capped': True, 'interrupted': False}


def test_isolated_engine_hands_the_cut_off_to_the_calling_thread(page):
    from shared.search_engine import _note_search_cutoff, consume_last_search_cutoff
    from web.state import state
    page({(WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
          (WORD2, False): ([_row('M2')], None)})
    _note_search_cutoff(capped=True, interrupted=True)        # stale, from an earlier search
    state.searcher.execute_search(WORD2, 'literal', 0)
    assert consume_last_search_cutoff() == {'capped': False, 'interrupted': False}
    state.searcher.execute_search(WORD, 'literal', 0)
    assert consume_last_search_cutoff() == {'capped': True, 'interrupted': False}


def test_a_failed_call_leaves_no_cut_off(page, monkeypatch):
    from shared.search_engine import _note_search_cutoff, consume_last_search_cutoff
    from web import research_jobs
    from web.state import state

    class Failing:
        def submit(self, payload):
            from web.research_jobs import Job
            job = Job(payload)
            job.future.set_exception(InterruptedError('Search cancelled'))
            return job

        def position(self, job):
            return 0

        def cancel(self, job):
            pass
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: Failing())
    _note_search_cutoff(capped=True)
    with pytest.raises(InterruptedError):
        state.searcher.execute_search(WORD, 'literal', 0)
    assert consume_last_search_cutoff() == {'capped': False, 'interrupted': False}


# --- The page -----------------------------------------------------------------

def _label_texts(user):
    """Every visible text on the page (labels, the refine chip, buttons)."""
    with user._client:
        return [e.text for e in user._client.elements.values()
                if isinstance(getattr(e, 'text', None), str) and e.text]


async def _wait_for(predicate, timeout=15.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, 'timed out'
        await asyncio.sleep(0.05)


def _click_search_within(user):
    from nicegui import events, ui
    with user._client:
        button = next(e for e in user._client.elements.values()
                      if isinstance(e, ui.button) and str(e.text).startswith('Search within'))
        for listener in button._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=button, client=user._client, args={}))
        return button.text


def test_a_cut_off_list_shows_n_plus_and_search_within_completes_it_first(page):
    """WORD's shown rows reach manuscript M1 only (the search stopped at its
    candidate limit); its complete set also holds M2. Searching WORD2 within
    WORD's results must search M2 as well."""
    queue = page({
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD, True): ([_row('M1'), _row('M2')], None),
        (WORD2, False): ([_row('M2')], None),
    })
    seen = {}

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        seen['button'] = _click_search_within(a)
        await wait_for_payloads(queue, 2)          # the completion job
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 3)

    run(driver)
    assert seen['button'] == 'Search within 1+ manuscripts'
    completion, within = queue.payloads[1], queue.payloads[2]
    assert completion['arguments']['query_str'] == WORD and completion['arguments']['ids_only'] is True
    assert within['arguments']['query_str'] == WORD2
    assert set(within['arguments']['restrict_sys_ids']) == {'M1', 'M2'}


def test_a_complete_list_is_searched_within_as_shown(page):
    queue = page({
        (WORD, False): ([_row('M1')], None),
        (WORD2, False): ([_row('M1')], None),
    })

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)))
        _click_search_within(a)
        await _wait_for(lambda: any('Searching within 1 manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 2)

    run(driver)
    assert [bool(p['arguments'].get('ids_only')) for p in queue.payloads] == [False, False]
    assert set(queue.payloads[1]['arguments']['restrict_sys_ids']) == {'M1'}


def test_the_n_plus_survives_a_reload(page):
    queue = page({(WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False})})

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        await a.open('/search')
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))

    run(driver)


def test_a_snapshot_that_kept_part_of_the_list_restores_as_cut_off(monkeypatch):
    from web.pages import search_state as ss
    stored = {}
    monkeypatch.setattr(ss, 'safe_user_set', lambda k, v: stored.__setitem__(k, v) or True)
    monkeypatch.setattr(ss, 'safe_user_get', lambda k, d=None: stored.get(k, d))
    monkeypatch.setattr(ss, '_get_tab_storage', lambda: None)
    monkeypatch.setattr(ss, '_SEARCH_ACTIVE_USER_FALLBACK_LIMIT', 2)
    state = ss.SearchUIState()
    state.results = [_row(f'M{i}') for i in range(3)]
    ss.persist_search_snapshot(state)
    restored = ss.SearchUIState()
    ss.restore_search_snapshot(restored)
    assert len(restored.results) == 2 and restored.result_count_capped is True


def test_completion_runs_the_search_that_ran_not_the_search_box(page):
    """Found in the browser check: the first step of a chain was built from the
    search box, which still held the "=" prefix, and with scope 'all', so the
    completion searched "= <word>" and found nothing. It must re-run the search
    that ran: the query without its prefix, its mode, the website's scope."""
    queue = page({
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD, True): ([_row('M1'), _row('M2')], None),
        (WORD2, False): ([_row('M2')], None),
    })

    async def driver(a, b):
        await a.open('/search')
        submit(a, f'= {WORD}')
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        _click_search_within(a)
        await wait_for_payloads(queue, 2)
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))

    run(driver)
    completion = queue.payloads[1]['arguments']
    assert (completion['query_str'], completion['mode'], completion['corpus_scope']) == (WORD, 'literal', 'genizah')
    assert completion['ids_only'] is True
