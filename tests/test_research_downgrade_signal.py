"""VS-2: one search's Responsa downgrade signal must not reach another caller.

The signal is per thread, and the website's searches and /api/search wait on the
same research-wait threads. A website search reads its warning from results[0]
and never consumes the signal, so a later API request on that thread must not
report it. The worker process is faked (canned results); IsolatedEngine,
run_research_call, wait_executor and the /api/search route are the real ones.
"""
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import web.research_jobs as research_jobs
from web.research_jobs import IsolatedEngine, run_research_call, wait_executor
from shared.search_engine import (_consume_last_responsa_downgrade,
                                  _consume_last_responsa_downgrade_meta)

# What a real cascade leaves (shared/responsa.py labels; four booleans).
WEBSITE_DOWNGRADE = 'Spelling variants disabled'
WEBSITE_META = {'variants': False, 'ja': True, 'flex_spacing': False, 'bidirectional': False}


class _Job:
    def __init__(self, result):
        self.future = Future()
        self.future.set_result(result)
        self.status = 'done'
        self.progress = (1, 1)
        self.preview_sequence = 0
        self.preview = []


class _Queue:
    """Answers each job with the next canned worker result."""

    def __init__(self):
        self.results = []

    def submit(self, payload):
        return _Job(self.results.pop(0))

    def position(self, job):
        return 0

    def cancel(self, job):
        pass


class _Engine:
    """IsolatedEngine binds each call's arguments to this signature (the real one's)."""
    settings = None

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False):
        raise AssertionError('runs in the (faked) worker, never in this process')


def _drain():
    _consume_last_responsa_downgrade()
    _consume_last_responsa_downgrade_meta()


@pytest.fixture
def queue(monkeypatch):
    fake = _Queue()
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: fake)
    # One wait thread, so both searches use it: the real pool reuses an idle
    # thread the same way.
    pinned = ThreadPoolExecutor(max_workers=1, thread_name_prefix='research-wait')
    monkeypatch.setattr(research_jobs, '_wait_executor', pinned)
    pinned.submit(_drain).result()
    yield fake
    pinned.submit(_drain).result()
    pinned.shutdown(wait=True)


@pytest.fixture
def api(monkeypatch, queue):
    from web.search_api import init_search_api, _rate_limiter
    from web.state import state
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234', 'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    monkeypatch.setattr(state, 'meta_mgr', mgr)
    monkeypatch.setattr(state, 'searcher', IsolatedEngine(_Engine(), 'search'))
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as client:
        yield client
    _rate_limiter.reset_for_tests()


def _website_responsa_search(engine):
    """A Responsa search made the way the website's run_core_search makes it."""
    def run_core_search():
        return engine.execute_search('visitor query', mode='Responsa', gap=0,
                                     progress_callback=None, exclude_words=None,
                                     responsa_options={'variants': True, 'ja': True},
                                     restrict_sys_ids=None, text_position=None,
                                     corpus_scope='genizah', preview_callback=None)

    async def run():
        return await run_research_call(run_core_search)
    return asyncio.run(run())


def _website_result():
    return {'value': [], 'downgrade': WEBSITE_DOWNGRADE, 'cascade': dict(WEBSITE_META)}


def test_a_call_without_a_downgrade_reports_none_left_by_an_earlier_call(queue):
    engine = IsolatedEngine(_Engine(), 'search')
    queue.results = [_website_result(), {'value': []}]
    _website_responsa_search(engine)

    def next_search():
        engine.execute_search('other query', 'literal', 0)
        return _consume_last_responsa_downgrade(), _consume_last_responsa_downgrade_meta()

    assert wait_executor().submit(next_search).result() == (None, None)


def test_a_call_still_reports_its_own_downgrade(queue):
    engine = IsolatedEngine(_Engine(), 'search')
    queue.results = [_website_result()]

    def search():
        engine.execute_search('query', 'Responsa', 0)
        return _consume_last_responsa_downgrade(), _consume_last_responsa_downgrade_meta()

    assert wait_executor().submit(search).result() == (WEBSITE_DOWNGRADE, WEBSITE_META)


def test_api_exact_search_after_a_website_downgrade_has_no_downgrade_warning(api, queue):
    from web.state import state
    queue.results = [_website_result(), {'value': []}]
    _website_responsa_search(state.searcher)

    r = api.post('/api/search', json={'query': 'unrelated', 'search_mode': 'exact'})
    assert r.status_code == 200, r.json()
    warnings = r.json().get('warnings') or []
    assert not [w for w in warnings if 'query_downgraded' in str(w)], warnings


def test_api_responsa_search_after_a_website_downgrade_echoes_its_own_options(api, queue):
    from web.state import state
    queue.results = [_website_result(), {'value': []}]
    _website_responsa_search(state.searcher)

    r = api.post('/api/search', json={'query': 'unrelated', 'search_mode': 'responsa',
                                      'responsa_options': {'variants': True, 'ja': True}})
    assert r.status_code == 200, r.json()
    body = r.json()
    assert not [w for w in body.get('warnings') or [] if 'query_downgraded' in str(w)], body['warnings']
    echo = body['request']
    assert echo['responsa_options_effective'] != WEBSITE_META
    # No cascade fired for this request: effective is what it asked for.
    assert echo['responsa_options_effective'] == echo['responsa_options']
