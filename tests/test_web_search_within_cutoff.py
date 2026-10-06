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


def _run_worker(tmp_path, monkeypatch, time_limit, escape=False):
    from shared import research_worker
    seen = {}
    monkeypatch.setattr('shared.search_engine.SearchEngine', _slow_engine(seen, escape))
    monkeypatch.setenv('GENIZAH_RESEARCH_MEMORY_MB', '512')
    payload = {'kind': 'search', 'method': 'execute_search', 'settings': None, 'time_limit': time_limit,
               'arguments': {'query_str': WORD, 'mode': 'literal', 'gap': 0, 'corpus_scope': 'genizah'}}
    research_worker.run_query(tmp_path, payload, lambda *a: None, meta=SimpleNamespace())
    with gzip.open(tmp_path / 'output.pkl', 'rb') as stream:
        return pickle.load(stream), seen


def test_the_worker_stops_at_its_time_limit_and_returns_what_it_checked(tmp_path, monkeypatch):
    result, seen = _run_worker(tmp_path, monkeypatch, time_limit=0.3)
    assert result['time_limit'] is True
    assert result['cutoff'] == {'capped': False, 'interrupted': True}
    assert len(result['value']) == seen['rows'] > 0


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
