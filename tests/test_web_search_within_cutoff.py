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
import re
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'search-within-cutoff-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402
    FakeSearchEngine, run, store, submit, wait_for_payloads,
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
        # (query, ids_only) -> (rows, cutoff) or (rows, cutoff, stopped by the time limit)
        self.answers = answers
        self.payloads = []
        self.lock = threading.Lock()

    def submit(self, payload):
        from web.research_jobs import Job
        with self.lock:
            self.payloads.append(payload)
        args = payload['arguments']
        rows, cutoff, *time_limit = self.answers.get((args.get('query_str'), bool(args.get('ids_only'))),
                                                     ([], None))
        job = Job(payload)
        job.future.set_result({'value': [dict(r) for r in rows],
                               'cutoff': cutoff or {'capped': False, 'interrupted': False},
                               'time_limit': bool(time_limit and time_limit[0])})
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


_COUNTS = re.compile(r'Results|Search within|Searching within')


async def _wait_for(predicate, timeout=15.0, user=None):
    """Wait until *predicate* holds; on a timeout, say what *user*'s page shows."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, 'timed out' + (
            f'; the page shows {[t for t in _label_texts(user) if _COUNTS.search(t)]}' if user else '')
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


# --- Review round 1 (Codex, 2026-10-06) ---------------------------------------

def _watched_job_class():
    from web.research_jobs import Job

    class WatchedJob(Job):
        """A job whose early rows record when the waiting caller took them."""
        rows = None
        handed = False

        @property
        def preview(self):
            if self.rows is not None:
                self.handed = True
            return self.rows

        @preview.setter
        def preview(self, value):
            self.rows = value
    return WatchedJob


class _LazyJob:
    def __call__(self, payload):
        return _watched_job_class()(payload)


_WatchedJob = _LazyJob()


class StoppableQueue(AnsweringQueue):
    """WORD's ordinary search hands over early rows, then waits until Stop cancels
    it; every other job is answered as AnsweringQueue answers it."""

    def __init__(self, answers, early_rows):
        super().__init__(answers)
        self.early_rows = early_rows
        self.held = None

    def submit(self, payload):
        args = payload['arguments']
        if args.get('query_str') == WORD and not args.get('ids_only') and self.held is None:
            with self.lock:
                self.payloads.append(payload)
            job = _WatchedJob(payload)
            job.rows = [dict(r) for r in self.early_rows]
            job.preview_sequence = 1
            self.held = job
            return job
        return super().submit(payload)

    def cancel(self, job):
        if not job.future.done():
            job.future.set_exception(InterruptedError('Search cancelled'))


def _click(user, kind, test):
    from nicegui import events
    with user._client:
        element = next(e for e in user._client.elements.values() if isinstance(e, kind) and test(e))
        for listener in element._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args={}))


def _input(user, placeholder):
    from nicegui import ui
    with user._client:
        return next(e for e in user._client.elements.values()
                    if isinstance(e, ui.input) and e.props.get('placeholder') == placeholder)


def test_stopped_rows_are_cut_off_and_search_within_completes_them(page, monkeypatch):
    """Stop keeps the early rows; they are the start of the list, never all of it:
    the count and Search within say "+", and the search within completes the step
    (M2, beyond the rows shown, is searched too)."""
    from nicegui import ui
    from web import research_jobs
    queue = StoppableQueue({
        (WORD, True): ([_row('M1'), _row('M2')], None),
        (WORD2, False): ([_row('M2')], None),
    }, early_rows=[_row('M1')])
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    seen = {}

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: queue.held is not None and queue.held.handed)
        await asyncio.sleep(0.3)                   # the caller hands them to the page
        _click(a, ui.button, lambda e: e.text == 'Stop')
        await _wait_for(lambda: any('(partial)' in t for t in _label_texts(a)))
        seen['button'] = _click_search_within(a)
        await wait_for_payloads(queue, 2)          # the completion job
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 3)

    run(driver)
    assert seen['button'] == 'Search within 1+ manuscripts'
    completion, within = queue.payloads[1], queue.payloads[2]
    assert completion['arguments']['query_str'] == WORD and completion['arguments']['ids_only'] is True
    assert set(within['arguments']['restrict_sys_ids']) == {'M1', 'M2'}


def test_search_within_again_keeps_the_completed_set(page):
    """Complete {M1} to {M1, M2}, cancel the refinement, click Search within again:
    the next search is still restricted to both manuscripts (the shown rows only
    reach M1), and nothing is completed twice."""
    from nicegui import ui
    queue = page({
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD, True): ([_row('M1'), _row('M2')], None),
        (WORD2, False): ([_row('M2')], None),
    })

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        _click_search_within(a)
        await wait_for_payloads(queue, 2)
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))
        _click(a, ui.button, lambda e: e.text == 'Cancel' and e.props.get('icon') == 'close')
        await _wait_for(lambda: not any('Searching within' in t for t in _label_texts(a)
                                        if t.startswith('Searching within')) or True)
        _click_search_within(a)
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 3)

    run(driver)
    assert [bool(p['arguments'].get('ids_only')) for p in queue.payloads] == [False, True, False]
    assert set(queue.payloads[2]['arguments']['restrict_sys_ids']) == {'M1', 'M2'}


def test_a_reload_completes_the_search_that_ran_with_its_not_words(page):
    """The search the shown results came from is saved with them: after a reload a
    completion runs it again exactly, NOT-words included -- whatever the controls
    hold by then."""
    queue = page({
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD, True): ([_row('M1'), _row('M2')], None),
    })
    not_words = 'אאא'

    async def driver(a, b):
        await a.open('/search')
        with a._client:
            _input(a, 'Words to exclude (space separated)').value = not_words
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        await a.open('/search')
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        with a._client:
            _input(a, 'Words to exclude (space separated)').value = 'בבב'
        _click_search_within(a)
        await wait_for_payloads(queue, 2)

    run(driver)
    first, completion = queue.payloads[0]['arguments'], queue.payloads[1]['arguments']
    assert first['exclude_words'] == [not_words]
    assert completion['ids_only'] is True
    assert completion['exclude_words'] == [not_words], completion['exclude_words']


class FailingCompletionQueue(AnsweringQueue):
    def submit(self, payload):
        if payload['arguments'].get('ids_only'):
            from web.research_jobs import Job
            with self.lock:
                self.payloads.append(payload)
            job = Job(payload)
            job.future.set_exception(RuntimeError('worker died'))
            return job
        return super().submit(payload)


def test_a_failed_all_terms_completion_leaves_the_filter_off(page, monkeypatch):
    """The all-terms filter needs every earlier step's complete pages; when completing
    them fails it is not applied (it would hide shown rows that have every term):
    the box comes back unchecked and stays off after a reload."""
    from nicegui import ui
    from web import research_jobs
    from tests.test_web_variant_settings_per_visitor import _fire, stored
    queue = FailingCompletionQueue({
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD2, False): ([_row('M1', page=2)], None),
    })
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    seen = {}

    def all_terms_box(user):
        with user._client:
            return next(e for e in user._client.elements.values()
                        if isinstance(e, ui.checkbox) and e.text == 'Only results with all terms')

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        _click_search_within(a)
        await wait_for_payloads(queue, 2)          # completion fails: within the shown rows
        await _wait_for(lambda: any('Searching within 1+ manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 3)
        await _wait_for(lambda: any(t == 'Only results with all terms' for t in _label_texts(a)))
        _fire(a, all_terms_box(a), 'update:modelValue', True)
        await wait_for_payloads(queue, 4)          # the second completion, failing too
        await _wait_for(lambda: all_terms_box(a).value is False)
        seen['stored'] = stored(a, 'search_all_terms_filter')

    run(driver)
    assert seen['stored'] is False


def test_a_line_break_step_is_named_as_one_that_cannot_be_completed():
    from shared.refinement import RefinementStep, steps_that_cannot_complete
    line_break = RefinementStep(query='אבג | דהו', mode='exact', result_count_capped=True,
                                responsa_options={'responsa_mode': True})
    plain = RefinementStep(query='אבג', mode='exact', result_count_capped=True)
    complete = RefinementStep(query='אבג | דהו', mode='exact',
                              responsa_options={'responsa_mode': True})
    assert steps_that_cannot_complete([plain, line_break, complete]) == [line_break]
    assert steps_that_cannot_complete([plain, line_break], upto=1) == []


def test_a_step_saves_without_copying_its_runtime_sets(monkeypatch):
    """to_dict never copies a step's id sets (a completed step can hold hundreds of
    thousands): only the fields that are saved."""
    import copy
    from shared.refinement import RefinementStep
    step = RefinementStep(query=WORD, mode='literal', variant_settings={'variant_pairs_count': 30})
    step._result_sys_ids = {'M1'}
    step._result_uids = {'p1'}
    copied = []
    real = copy.deepcopy

    def watching(value, *args, **kwargs):
        copied.append(value)
        return real(value, *args, **kwargs)
    monkeypatch.setattr(copy, 'deepcopy', watching)
    saved = step.to_dict()
    assert '_result_sys_ids' not in saved and '_result_uids' not in saved
    assert not any(value is step._result_sys_ids or value is step._result_uids for value in copied)
    assert saved['variant_settings'] == {'variant_pairs_count': 30}
    assert saved['variant_settings'] is not step.variant_settings


# --- The website's time limit (owner ruling 2026-09-28) ---------------------------

def _slow_engine(seen, escape=False):
    """A search engine that checks two rows per progress call, for up to 5 seconds
    (without a time limit it then finishes, so a broken limit fails the test rather
    than hanging it), and returns what it had checked when a progress call stops it
    -- as the real engine does."""
    import time as _time

    from shared.search_engine import _note_search_cutoff

    class Engine:
        def __init__(self, meta, variants, *, worker_mode=False, open_local=True):
            self.searcher = object()

        def execute_search(self, query_str, mode, gap, progress_callback=None, corpus_scope='all'):
            rows = []
            if escape:
                for _ in range(500):
                    progress_callback(len(rows), 0)
                    _time.sleep(0.01)
                return rows
            try:
                for _ in range(500):
                    progress_callback(len(rows), 0)
                    rows += [{'uid': f'p{len(rows)}'}, {'uid': f'p{len(rows) + 1}'}]
                    seen['rows'] = len(rows)
                    _time.sleep(0.01)
            except InterruptedError:
                _note_search_cutoff(interrupted=True)
            return rows
    return Engine


def _run_worker(tmp_path, monkeypatch, time_limit, escape=False, **extra):
    from shared import research_worker
    seen = {}
    monkeypatch.setattr('shared.search_engine.SearchEngine', _slow_engine(seen, escape))
    monkeypatch.setenv('GENIZAH_RESEARCH_MEMORY_MB', '512')
    payload = {'kind': 'search', 'method': 'execute_search', 'settings': None, 'time_limit': time_limit,
               'arguments': {'query_str': WORD, 'mode': 'literal', 'gap': 0, 'corpus_scope': 'genizah'},
               **extra}
    research_worker.run_query(tmp_path, payload, lambda *a: None, meta=SimpleNamespace())
    with gzip.open(tmp_path / 'output.pkl', 'rb') as stream:
        return pickle.load(stream), seen


def test_the_worker_stops_at_its_time_limit_and_returns_what_it_checked(tmp_path, monkeypatch):
    result, seen = _run_worker(tmp_path, monkeypatch, time_limit=0.3)
    assert result['time_limit'] is True
    assert result['cutoff'] == {'capped': False, 'interrupted': True}
    assert len(result['value']) == seen['rows'] > 0


def test_the_workers_limit_counts_from_when_the_job_left_the_queue(tmp_path, monkeypatch):
    """GitHub review (Codex on #385): the worker's clock started just before the
    engine, so loading and the index-lease wait were not counted. A job that left
    the queue 10 s ago with a 3 s limit stops at its first check."""
    import time
    result, seen = _run_worker(tmp_path, monkeypatch, time_limit=3, started_at=time.time() - 10)
    assert result['time_limit'] is True
    assert result['value'] == [] and 'rows' not in seen


def test_the_queue_stamps_when_a_job_leaves_it():
    import time
    from web.research_jobs import Job, ResearchQueue

    class Handed(Exception):
        pass
    handed = {}

    def start(slot, payload):
        handed.update(payload)
        raise Handed()
    queue = ResearchQueue.__new__(ResearchQueue)
    queue._take_spare = lambda slot: None
    queue._memory_for_new_worker = lambda: True
    queue._start_worker = start
    before = time.time()
    with pytest.raises(Handed):
        queue._execute(Job({'kind': 'search', 'time_limit': 180}), 0)
    assert before <= handed['started_at'] <= time.time()


def test_a_part_that_cannot_return_rows_reports_the_limit(tmp_path, monkeypatch):
    result, _ = _run_worker(tmp_path, monkeypatch, time_limit=0.2, escape=True)
    assert result.get('time_limit') is True and 'error' in result


def test_the_web_side_says_the_time_limit_stopped_the_call(page):
    from web.research_jobs import consume_time_limit_stop
    from web.state import state
    from shared.config import Config
    queue = page({(WORD, False): ([_row('M1')], {'capped': False, 'interrupted': True})})
    real_submit = queue.submit

    def submit_stopped(payload):
        job = real_submit(payload)
        result = job.future.result()
        result['time_limit'] = True
        return job
    queue.submit = submit_stopped
    state.searcher.execute_search(WORD, 'literal', 0)
    assert queue.payloads[0]['time_limit'] == Config.WEB_SEARCH_TIME_LIMIT == 180
    assert consume_time_limit_stop() is True
    assert consume_time_limit_stop() is False


def test_a_worker_that_does_not_stop_is_stopped_after_the_grace(page, monkeypatch):
    """A job still running TIME_LIMIT_GRACE_SECONDS after its limit (stuck where the
    engine never checks) is cancelled, which frees the queue."""
    from shared.config import Config
    from shared.search_regex import SearchBudgetExceeded
    from web import research_jobs
    from web.state import state
    cancelled = []

    class Stuck:
        def submit(self, payload):
            return research_jobs.Job(payload)

        def position(self, job):
            return 0

        def cancel(self, job):
            cancelled.append(job)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: Stuck())
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 0.2)
    monkeypatch.setattr(research_jobs, 'TIME_LIMIT_GRACE_SECONDS', 0.2)
    with pytest.raises(SearchBudgetExceeded):
        state.searcher.execute_search(WORD, 'literal', 0)
    assert len(cancelled) == 1


def test_a_search_stopped_by_the_time_limit_shows_n_plus(page):
    """The rows a time-limited search returns are the start of its list: "N+"."""
    queue = page({(WORD, False): ([_row('M1')], {'capped': False, 'interrupted': True})})

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))

    run(driver)


# --- Review round 2 (2026-10-07): every time-limited search says so ----------------

TIME_LIMIT_NOTE = 'The search stopped after 3 minutes; showing the results found so far.'


class FakeLabEngine:
    """The real lab_search signature; never runs in process."""

    def __init__(self, settings, var_mgr):
        self.settings, self.var_mgr = settings, var_mgr

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0, deep_scan=False,
                   scan_limit=50000, corpus_scope='genizah'):
        raise AssertionError('must run through the worker queue, not in process')


def _search_within_text(user):
    from nicegui import ui
    with user._client:
        return next(e.text for e in user._client.elements.values()
                    if isinstance(e, ui.button) and str(e.text).startswith('Search within'))


def test_a_search_the_time_limit_stopped_shows_n_plus_and_says_why(page):
    """The worker stopped the search at the time limit: its rows are the start of the
    list ("N+"), and the page says that the time limit stopped it."""
    queue = page({(WORD, False): ([_row('M1')], {'capped': False, 'interrupted': True}, True)})
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)), user=a)
        seen['notes'] = list(a.notify.messages)

    run(driver, count=1)
    assert TIME_LIMIT_NOTE in seen['notes'], seen['notes']


def test_a_lab_search_the_time_limit_stopped_is_cut_off(page, monkeypatch):
    """Review round 2, finding 1: a Lab search the time limit stopped returned its rows
    with no cut-off signal, so the page showed "1 Results", "Search within 1
    manuscripts" and searched within the shown rows as if they were all. The time
    limit alone makes the list incomplete."""
    from nicegui import ui
    from web.research_jobs import IsolatedEngine
    from web.state import state
    monkeypatch.setattr(state, 'lab_engine', IsolatedEngine(
        FakeLabEngine(state.lab_engine._engine.settings, state.var_mgr), 'lab'))
    queue = page({(WORD, False): ([_row('M1')], {'capped': False, 'interrupted': False}, True)})
    seen = {}

    async def driver(a):
        await a.open('/search')
        with a._client:
            switch = next(e for e in a._client.elements.values()
                          if isinstance(e, ui.switch) and e.text == 'Enable Lab Mode algorithms')
            switch.value = True
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)), user=a)
        seen['button'] = _search_within_text(a)
        seen['notes'] = list(a.notify.messages)

    run(driver, count=1)
    assert (queue.payloads[0]['kind'], queue.payloads[0]['method']) == ('lab', 'lab_search')
    assert seen['button'] == 'Search within 1+ manuscripts'
    assert TIME_LIMIT_NOTE in seen['notes'], seen['notes']


def test_searching_within_a_cut_off_line_break_search_says_it_cannot_be_completed(page):
    """Review round 2, test gap (b): a cut-off Responsa line-break step cannot be
    completed (that search has no ids-only path), so Search within says it searches
    within the shown results -- on the page, not only in the helper."""
    query = f'{WORD} | {WORD2}'
    queue = page({(query, False): ([_row('M1')], {'capped': True, 'interrupted': False})})
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_mode='responsa')
        await a.open('/search')
        submit(a, query)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)), user=a)
        _click_search_within(a)
        await _wait_for(lambda: any('Searching within 1+ manuscripts' in t for t in _label_texts(a)), user=a)
        seen['notes'] = list(a.notify.messages)

    run(driver, count=1)
    assert queue.payloads[0]['arguments']['responsa_options']['responsa_mode'] is True
    assert len(queue.payloads) == 1, 'a line-break step is not run again to complete it'
    assert ('A search with a line break cannot be completed; searching within the shown results.'
            in seen['notes']), seen['notes']


def _forced_stop(page, monkeypatch, early_rows):
    """WORD's search hands over *early_rows*, then never finishes: the web side stops
    it TIME_LIMIT_GRACE_SECONDS after its time limit (both made short here)."""
    from shared.config import Config
    from web import research_jobs
    queue = StoppableQueue({}, early_rows=early_rows)
    # The job counts as queued until the caller has taken its early rows, so the
    # web side's clock -- which starts when the job runs -- starts only after the
    # rows are on the page: the kill can never come first, however slow the machine.
    queue.position = lambda job: (0 if job is not queue.held or not early_rows or job.handed else 1)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 0.5)
    monkeypatch.setattr(research_jobs, 'TIME_LIMIT_GRACE_SECONDS', 0.5)
    return queue


def test_a_forced_stop_keeps_the_rows_already_shown(page, monkeypatch):
    """Review round 2, finding 4: when the web side has to stop a worker that did not
    stop itself, the rows it had already handed over are verified rows (the start of
    its list): they are kept, as Stop keeps them -- "+", partial, the time-limit
    notice -- instead of "0 Results" and an error."""
    from web import export_state
    queue = _forced_stop(page, monkeypatch, early_rows=[_row('M1')])
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any('(partial)' in t for t in _label_texts(a)), user=a)
        with a._client:     # result cards show the shelfmark as html
            seen['html'] = [e.content for e in a._client.elements.values()
                            if isinstance(getattr(e, 'content', None), str)]
        seen['button'] = _search_within_text(a)
        seen['notes'] = list(a.notify.messages)
        with a._client:
            seen['export'] = export_state.get_search_export()

    run(driver, count=1)
    assert any('S-M1' in h for h in seen['html']), 'the row shown is still shown'
    assert seen['button'] == 'Search within 1+ manuscripts'
    assert any(n.startswith('The search stopped after') for n in seen['notes']), seen['notes']
    assert not any('exceeded its time limit' in n for n in seen['notes']), seen['notes']
    assert 'results-cut-off' in seen['export']['warnings']
    assert [r['uid'] for r in seen['export']['results']] == ['M1_1']


def test_a_forced_stop_with_nothing_shown_says_the_search_was_stopped(page, monkeypatch):
    """Without rows to keep, the page says what it said before."""
    queue = _forced_stop(page, monkeypatch, early_rows=[])
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t == '0 Results' for t in _label_texts(a)), user=a)
        seen['notes'] = list(a.notify.messages)

    run(driver, count=1)
    assert any('exceeded its time limit' in n for n in seen['notes']), seen['notes']


# --- The worker: a letter-level (passage) search checks the time limit -------------

def test_the_worker_stops_a_letter_level_search_at_its_time_limit(tmp_path, monkeypatch):
    """Review round 2, finding 3: the passage engine never called anything the worker's
    time limit could stop, so the job ran on until the web side killed it 60 s past the
    limit, returning nothing. The worker hands it the limit as `checkpoint`; stopped,
    the real PassageSearcher returns what it had completed, marked partial."""
    from shared import research_worker
    from shared.passage_builder import build_index
    from tests.test_passage_multi_witness import _aperiodic, _rid
    motif = _aperiodic(90, salt=11)
    texts = {_rid(1): _aperiodic(150, salt=101) + ' ' + motif + ' ' + _aperiodic(150, salt=102)}
    for r in range(10, 16):
        texts[_rid(r)] = _aperiodic(300, salt=900 + r)
    build_index(list(texts.items()), str(tmp_path / 'passage'), partitions=2, apply_hygiene=False)

    class TextFetcher:
        def __init__(self, meta, variants, *, worker_mode=False, open_local=True):
            self.searcher = None

        def get_full_text_by_header(self, header):
            return texts.get(header)
    monkeypatch.setattr('shared.search_engine.SearchEngine', TextFetcher)
    monkeypatch.setenv('GENIZAH_RESEARCH_MEMORY_MB', '512')

    def run_with(time_limit, root):
        root.mkdir()
        payload = {'kind': 'passage', 'method': 'search_composition_logic', 'settings': None,
                   'time_limit': time_limit, 'arguments': {'full_text': motif},
                   'options': {'path': str(tmp_path / 'passage'), 'preset': 'standard-40',
                               'length': 'normal', 'depth': 'normal', 'render_cap': None}}
        research_worker.run_query(root, payload, lambda *a: None, meta=SimpleNamespace())
        with gzip.open(root / 'output.pkl', 'rb') as stream:
            return pickle.load(stream)

    whole = run_with(60, tmp_path / 'whole')
    assert whole['time_limit'] is False and 'partial' not in whole['value']
    assert [r['raw_header'] for r in whole['value']['main']] == [_rid(1)]

    # A limit already past (time.monotonic() ticks every ~16 ms on Windows, so a
    # tiny positive limit may not have passed at the first check).
    stopped = run_with(-1, tmp_path / 'stopped')
    assert stopped['time_limit'] is True, stopped
    assert stopped['value']['partial'] is True
    assert stopped['value']['main'] == []
    assert stopped['cutoff']['interrupted'] is True


# --- GitHub review (Codex on #385, 2026-10-07): one time limit for a chain -------

class _TimedQueue(AnsweringQueue):
    """Answers as AnsweringQueue does, each job after *seconds* of running."""

    def __init__(self, answers, seconds):
        super().__init__(answers)
        self.seconds = seconds

    def submit(self, payload):
        from web.research_jobs import Job
        result = super().submit(payload).future.result()
        timed = Job(payload)
        threading.Timer(self.seconds, timed.future.set_result, (result,)).start()
        return timed


def test_searches_inside_shared_time_limit_share_one_limit(page, monkeypatch):
    """Each search inside shared_time_limit() gets what the ones before it left of
    the one limit; once it is spent the next stops at once. Outside, a search gets
    the whole limit again."""
    from shared.config import Config
    from web import research_jobs
    from web.research_jobs import shared_time_limit
    from web.state import state
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 1.0)
    queue = _TimedQueue({}, 0.6)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    with shared_time_limit():
        for _ in range(3):
            state.searcher.execute_search(WORD, 'literal', 0)
    state.searcher.execute_search(WORD, 'literal', 0)
    limits = [p['time_limit'] for p in queue.payloads]
    assert limits[0] == 1.0
    assert 0.001 < limits[1] < 0.5, limits       # what the first search left
    assert limits[2] == 0.001, limits            # spent: stops at its first check
    assert limits[3] == 1.0, limits


def test_a_restored_chain_and_its_completion_each_run_under_one_limit(page, monkeypatch):
    """A reload replays a two-step chain, and Search within then completes both
    steps: the second job of each runs with what the first left of one limit, not
    with a fresh one (an N-step chain could otherwise hold the queue N x 3 min)."""
    from shared.config import Config
    from web import research_jobs
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 30)
    answers = {
        (WORD, False): ([_row('M1')], {'capped': True, 'interrupted': False}),
        (WORD, True): ([_row('M1'), _row('M2')], None),
        (WORD2, False): ([_row('M2')], None),
        (WORD2, True): ([_row('M2')], None),
    }
    queue = _TimedQueue(answers, 0.3)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any(t.startswith('1+ Results') for t in _label_texts(a)))
        _click_search_within(a)
        await wait_for_payloads(queue, 2)          # completes WORD
        await _wait_for(lambda: any('Searching within 2 manuscripts' in t for t in _label_texts(a)))
        submit(a, WORD2)
        await wait_for_payloads(queue, 3)          # the chain is WORD > WORD2
        await _wait_for(lambda: any(t.startswith('1 Results') for t in _label_texts(a)), user=a)
        await a.open('/search')
        await wait_for_payloads(queue, 5)          # the reload replays both steps
        await _wait_for(lambda: not any('Restoring refinement chain' in t for t in _label_texts(a)))
        _click_search_within(a)
        await wait_for_payloads(queue, 7)          # completes both steps

    run(driver)
    jobs = [(p['arguments']['query_str'], bool(p['arguments'].get('ids_only')), p['time_limit'])
            for p in queue.payloads]
    assert [j[:2] for j in jobs[3:7]] == [(WORD, False), (WORD2, False), (WORD, True), (WORD2, True)], jobs
    assert jobs[3][2] == 30 and jobs[5][2] == 30, jobs
    assert jobs[4][2] < 29.8 and jobs[6][2] < 29.8, jobs
