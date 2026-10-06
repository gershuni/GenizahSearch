# -*- coding: utf-8 -*-
"""Website variant and Lab settings are kept per visitor and sent per search.

The web process holds one LabSettings object for every visitor. These tests
drive the real /search and /settings handlers (NiceGUI ``User`` simulation, two
browsers), the real /api/search route and the real ``IsolatedEngine`` adapter,
with the research queue replaced by a recorder: what is asserted is the exact
settings snapshot a worker process would have received.

The server's settings object starts with values that differ from every website
default, so a leftover write to it, or a job that reads it, fails an assertion.

No index, no worker subprocess, no network.
"""
from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'variant-settings-test-secret-0123456789abcdef')

WORD = 'אבגד'
WORD2 = 'הוזח'
PLACEHOLDER = 'Enter Hebrew text to search'

# Server values chosen to differ from every website default.
SERVER = {
    'variant_pairs_count': 50,
    'variant_max_changes': 3,
    'variant_min_word_len': 4,
    'variant_aggressive': True,
    'custom_variants': {'ק=א': True},
    'comp_min_score': 33,
}


class RecordingQueue:
    """Stands in for ResearchQueue: records each payload, answers with no rows."""

    def __init__(self):
        self.payloads = []
        self.lock = threading.Lock()

    def submit(self, payload):
        from web.research_jobs import Job
        with self.lock:
            self.payloads.append(payload)
        job = Job(payload)
        job.future.set_result({'value': []})
        return job

    def position(self, job):
        return 0

    def cancel(self, job):
        pass

    def query_of(self, payload):
        return payload['arguments'].get('query_str')


class FakeSearchEngine:
    """The real engine's search signature; never runs in process."""
    index = None

    def __init__(self, var_mgr):
        self.var_mgr = var_mgr

    def parse_query_syntax(self, query, responsa_mode=False):
        from shared.search_engine import SearchEngine
        return SearchEngine.parse_query_syntax(self, query, responsa_mode=responsa_mode)

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None,
                       ids_only=False):
        raise AssertionError('must run through the worker queue, not in process')


@pytest.fixture
def server(monkeypatch):
    from shared.lab_settings import LabSettings
    from shared.variants import VariantManager
    from web import research_jobs
    from web.research_jobs import IsolatedEngine
    from web.state import state

    monkeypatch.setattr(LabSettings, 'load', lambda self: None)
    saves = []
    monkeypatch.setattr(LabSettings, 'save', lambda self: saves.append(dict(vars(self))))
    settings = LabSettings()
    for name, value in SERVER.items():
        setattr(settings, name, dict(value) if isinstance(value, dict) else value)
    var_mgr = VariantManager(settings)
    queue = RecordingQueue()
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    # The app lifespan shuts the wait pool down on exit; give each test its own.
    monkeypatch.setattr(research_jobs, '_wait_executor', None)
    monkeypatch.setattr(state, 'var_mgr', var_mgr)
    monkeypatch.setattr(state, 'searcher', IsolatedEngine(FakeSearchEngine(var_mgr), 'search'))
    monkeypatch.setattr(state, 'lab_engine',
                        IsolatedEngine(SimpleNamespace(settings=settings, var_mgr=var_mgr), 'lab'))
    monkeypatch.setattr(state, 'meta_mgr', SimpleNamespace(csv_bank={}))
    return SimpleNamespace(settings=settings, queue=queue, monkeypatch=monkeypatch, saves=saves)


def server_values(settings):
    return {name: getattr(settings, name) for name in SERVER}


@asynccontextmanager
async def _browsers(count=2):
    import httpx
    from nicegui import core
    from nicegui import storage as nicegui_storage
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    import web.main  # noqa: F401  -- registers the pages on core.app

    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    # An earlier test in the same process may have built Starlette's middleware
    # stack (see tests/test_web_saved_joins_isolation.py); start unbuilt.
    saved_user_middleware = list(core.app.user_middleware)
    saved_stack = core.app.middleware_stack
    saved_secret = nicegui_storage.Storage.secret
    try:
        prepare_simulation()
        core.app.middleware_stack = None
        set_storage_secret('variant-settings-render-secret', {})
        os.environ['NICEGUI_USER_SIMULATION'] = 'true'
        try:
            with patch('web.main._resolve_ui_language', return_value='en'):
                async with core.app.router.lifespan_context(core.app):
                    clients = [httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                                 base_url='http://test') for _ in range(count)]
                    try:
                        yield [User(c) for c in clients]
                    finally:
                        for c in clients:
                            await c.aclose()
        finally:
            os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        core.app.user_middleware = saved_user_middleware
        core.app.middleware_stack = saved_stack
        nicegui_storage.Storage.secret = saved_secret


def run(driver, count=2):
    from nicegui.context import context as nicegui_context
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _browsers(count) as made:
            await driver(*made)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def store(user, **values):
    """Write this visitor's own storage, as earlier visits would have."""
    from nicegui import app
    with user._client:
        for key, value in values.items():
            app.storage.user[key] = value


def stored(user, key):
    from nicegui import app
    with user._client:
        return app.storage.user.get(key)


def _element(user, kind, test):
    with user._client:
        for element in user._client.elements.values():
            if isinstance(element, kind) and test(element):
                return element
    raise AssertionError(f'{kind.__name__} not found')


def _fire(user, element, event_type, value=None):
    from nicegui import events
    with user._client:
        if value is not None:
            element.value = value
        for listener in element._event_listeners.values():
            if listener.type == event_type:
                # A value element's own listener reads the new value from args.
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args={} if value is None else value))


def submit(user, text):
    """Type into the search box and press Enter, exactly as the page wires it."""
    from nicegui import ui
    box = _element(user, ui.input, lambda e: e.props.get('placeholder') == PLACEHOLDER)
    _fire(user, box, 'keydown.enter', text)


async def wait_for_payloads(queue, n, timeout=15.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while len(queue.payloads) < n:
        assert loop.time() < deadline, f'only {len(queue.payloads)} of {n} searches reached the queue'
        await asyncio.sleep(0.02)


def test_prefix_level_and_changes_reach_the_worker_and_leave_server_settings_alone(server):
    async def driver(a, b):
        await a.open('/search')
        store(a, search_max_changes=1)          # this visitor chose x1 earlier
        await a.open('/search')
        submit(a, f'??? {WORD}')
        await wait_for_payloads(server.queue, 1)

    run(driver)
    sent = server.queue.payloads[0]['settings']
    assert sent['variant_pairs_count'] == 150, sent['variant_pairs_count']
    assert sent['variant_max_changes'] == 1, sent['variant_max_changes']
    assert server_values(server.settings) == SERVER, server_values(server.settings)


def test_two_visitors_interleaved_each_get_their_own_level(server):
    """Visitor A asks for Maximum, visitor B for Extended; B's search lands while
    A's search is between 'read the controls' and 'hand the job to a worker'."""
    from web import research_jobs
    b_reached_queue = threading.Event()
    calls = {'n': 0}
    queue = server.queue

    def gated_get_queue():
        calls['n'] += 1
        if calls['n'] == 1:
            # A's wait thread: hold here until B has dispatched.
            assert b_reached_queue.wait(10), 'visitor B never dispatched'
        else:
            b_reached_queue.set()
        return queue

    server.monkeypatch.setattr(research_jobs, 'get_queue', gated_get_queue)

    async def driver(a, b):
        await a.open('/search')
        await b.open('/search')
        submit(a, f'??? {WORD}')
        for _ in range(500):
            if calls['n'] >= 1:
                break
            await asyncio.sleep(0.02)
        assert calls['n'] == 1, 'visitor A never dispatched'
        submit(b, f'?? {WORD2}')
        await wait_for_payloads(queue, 2)

    run(driver)
    by_query = {queue.query_of(p): p['settings']['variant_pairs_count'] for p in queue.payloads}
    assert by_query == {WORD: 150, WORD2: 70}, by_query


def test_api_variants_request_uses_the_website_defaults_after_a_web_visitor(server):
    """An API search never inherits what a website visitor or the server file holds:
    it runs with the website defaults, which a visitor who changed nothing also gets."""
    import httpx
    from nicegui import core
    from web.variant_preferences import WEBSITE_DEFAULTS

    async def driver(a, b):
        await a.open('/search')
        store(a, variant_pref_custom_variants={'ש=ס': True}, variant_pref_variant_aggressive=True)
        submit(a, f'??? {WORD}')
        await wait_for_payloads(server.queue, 1)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                     base_url='http://test') as api:
            response = await api.post('/api/search', json={'query': WORD, 'search_mode': 'variants'})
            assert response.status_code == 200, response.text
        await wait_for_payloads(server.queue, 2)
        await b.open('/search')
        submit(b, f'? {WORD2}')
        await wait_for_payloads(server.queue, 3)

    run(driver)
    web_a, api, web_b = server.queue.payloads
    assert api['arguments']['mode'] == 'variants'
    assert {k: api['settings'][k] for k in WEBSITE_DEFAULTS} == WEBSITE_DEFAULTS
    assert web_a['settings']['custom_variants'] == {'ש=ס': True}
    assert web_a['settings']['variant_aggressive'] is True
    # A visitor who changed nothing searches exactly as the API does.
    assert {k: web_b['settings'][k] for k in WEBSITE_DEFAULTS} == WEBSITE_DEFAULTS


def test_restored_refinement_steps_replay_with_their_own_settings(server):
    """A refinement chain restored from the visitor's session is replayed step by
    step, each with the variant settings it was first searched with, never with the
    server-wide object's values. A step saved before settings were recorded runs
    with the website defaults at its level's preset -- not with this visitor's
    current preferences, which say nothing about how it first ran (design 4.3)."""
    from web.variant_preferences import WEBSITE_DEFAULTS
    chain = [
        {'query': WORD, 'mode': 'variants_maximum', 'gap': 0,
         'variant_settings': {'variant_pairs_count': 120, 'variant_max_changes': 1}},
        {'query': WORD2, 'mode': 'variants_extended', 'gap': 0},
    ]

    async def driver(a, b):
        await a.open('/search')
        store(a, search_refinement_chain=chain, search_mode='variants_maximum', search_query=WORD,
              variant_pref_custom_variants={'ש=ס': True}, variant_pref_variant_aggressive=True,
              search_max_changes=3)
        await a.open('/search')
        await wait_for_payloads(server.queue, 2)

    run(driver)
    sent = {server.queue.query_of(p): p['settings'] for p in server.queue.payloads[:2]}
    assert (sent[WORD]['variant_pairs_count'], sent[WORD]['variant_max_changes']) == (120, 1), sent[WORD]
    legacy = sent[WORD2]
    assert legacy['variant_pairs_count'] == 70, legacy['variant_pairs_count']
    assert {k: legacy[k] for k in WEBSITE_DEFAULTS if k != 'variant_pairs_count'} == {
        k: v for k, v in WEBSITE_DEFAULTS.items() if k != 'variant_pairs_count'}, legacy
    assert server_values(server.settings) == SERVER


def test_the_shown_results_settings_survive_a_reload():
    """The settings the shown results were searched with are saved with them, so a
    refinement started after a reload records step 0 with them (not None, which
    would replay it with other settings)."""
    from web.pages import search_state as ss

    saved = {}
    sent = {'variant_pairs_count': 120, 'variant_max_changes': 1, 'custom_variants': {'ש=ס': True}}
    state = ss.SearchUIState()
    state.last_variant_settings = dict(sent)
    with patch.object(ss, 'safe_user_get', lambda key, default=None: saved.get(key, default)), \
            patch.object(ss, 'safe_user_set', lambda key, value: saved.__setitem__(key, value) or True), \
            patch.object(ss, '_get_tab_storage', lambda: None):
        ss.persist_search_snapshot(state)
        restored = ss.SearchUIState()
        ss.restore_search_snapshot(restored)
    assert restored.last_variant_settings == sent

    tab = {}
    with patch.object(ss, '_get_tab_storage', lambda: tab):
        ss.persist_search_active_snapshot(state)
        restored = ss.SearchUIState()
        assert ss.restore_search_active_snapshot(restored)
    assert restored.last_variant_settings == sent


def test_slider_mode_prefix_moves_the_slider(server):
    """In slider mode (a per-visitor choice) ??? sends the Maximum level, as the desktop does."""
    from nicegui import ui

    async def driver(a, b):
        await a.open('/search')
        store(a, variant_pref_variant_use_slider=True)
        await a.open('/search')
        slider = _element(a, ui.slider, lambda e: True)
        submit(a, f'??? {WORD}')
        await wait_for_payloads(server.queue, 1)
        assert slider.value == 150
        # The other visitor still has preset buttons.
        await b.open('/search')
        with b._client:
            assert not [e for e in b._client.elements.values() if isinstance(e, ui.slider)]

    run(driver)
    assert server.queue.payloads[0]['settings']['variant_pairs_count'] == 150
    assert server.settings.variant_use_slider is False


def test_settings_page_choices_belong_to_one_visitor(server):
    """Every control on /settings writes the visitor's own storage, never the
    server's settings object or its file, and reaches only that visitor's searches."""
    from nicegui import ui
    pair = 'ש=ס'

    async def driver(a, b):
        await a.open('/settings')
        box = _element(a, ui.textarea, lambda e: str(e.props.get('placeholder', '')).startswith('ק=א'))
        _fire(a, box, 'blur', pair)
        aggressive = _element(a, ui.switch, lambda e: 'Aggressive' in str(e.text))
        _fire(a, aggressive, 'update:modelValue', True)
        numbers = [e for e in a._client.elements.values() if isinstance(e, ui.number)]
        by_max = {int(e._props.get('max')): e for e in numbers
                  if (e._props.get('min'), e._props.get('max')) != (5, 100)}  # not History entries
        _fire(a, by_max[5], 'update:modelValue', 4)     # Limit Short Words
        _fire(a, by_max[3], 'update:modelValue', 1)     # Max Changes per Word
        _fire(a, by_max[100], 'update:modelValue', 55)  # Lab Min Score
        await asyncio.sleep(0.05)
        assert stored(a, 'search_max_changes') == 1
        await a.open('/search')
        submit(a, f'?? {WORD}')
        await wait_for_payloads(server.queue, 1)
        await b.open('/search')
        submit(b, f'?? {WORD2}')
        await wait_for_payloads(server.queue, 2)

    run(driver)
    assert server_values(server.settings) == SERVER, server_values(server.settings)
    assert server.saves == [], 'a website visitor wrote the server settings file'
    sent = {server.queue.query_of(p): p['settings'] for p in server.queue.payloads}
    assert sent[WORD]['custom_variants'] == {pair: True}
    assert sent[WORD]['variant_aggressive'] is True
    assert sent[WORD]['variant_min_word_len'] == 4
    assert sent[WORD]['variant_max_changes'] == 1
    assert sent[WORD]['comp_min_score'] == 55
    from web.variant_preferences import WEBSITE_DEFAULTS
    for name in ('custom_variants', 'variant_aggressive', 'variant_min_word_len', 'comp_min_score'):
        assert sent[WORD2][name] == WEBSITE_DEFAULTS[name], name


def test_settings_page_has_only_the_lab_control_the_website_reads():
    """Candidate limit, display limit and chunk size were only ever saved to the
    server file; the website's Lab searches read the minimum score alone."""
    import pathlib
    source = (pathlib.Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'settings.py').read_text(encoding='utf-8')
    for gone in ("tr('Candidate Limit')", "tr('Display Limit')", "tr('Default Chunk Size')", '.save()'):
        assert gone not in source, gone
    assert "tr('Min Score')" in source


def test_joins_executor_sends_the_visitor_settings_it_was_built_with(server):
    from web.joins_executor import WebSearchExecutor
    from web.variant_preferences import WEBSITE_DEFAULTS
    WebSearchExecutor(variant_settings={'variant_pairs_count': 70, 'custom_variants': {'ש=ס': True}}).execute_search(
        WORD, 'variants', 0)
    WebSearchExecutor().execute_search(WORD2, 'variants', 0)
    sent = {server.queue.query_of(p): p['settings'] for p in server.queue.payloads}
    assert sent[WORD]['variant_pairs_count'] == 70
    assert sent[WORD]['custom_variants'] == {'ש=ס': True}
    assert {k: sent[WORD2][k] for k in WEBSITE_DEFAULTS} == WEBSITE_DEFAULTS
    assert server_values(server.settings) == SERVER


def test_request_settings_reject_unknown_names_and_bound_values():
    from web.research_jobs import with_request_settings
    from web.variant_preferences import request_settings
    with pytest.raises(ValueError):
        request_settings(candidate_limit=5)
    with pytest.raises(ValueError):
        with_request_settings(object(), lab_display_limit=5)
    checked = request_settings(variant_pairs_count=10_000, variant_max_changes=0,
                               custom_variants={'ש=ס': True, 'too long=x': True, 'noequals': True},
                               comp_min_score='junk')
    assert checked == {'variant_pairs_count': 300, 'variant_max_changes': 1,
                       'custom_variants': {'ש=ס': True}, 'comp_min_score': 70}


def test_website_defaults_are_the_eleven_server_pairs_with_one_change_for_short_words():
    """The website defaults the owner ruled (2026-09-28): the server file's eleven
    custom pairs, Aggressive off (short words get one change), Basic = 30 pairs."""
    from web.variant_preferences import WEBSITE_DEFAULTS
    pairs = WEBSITE_DEFAULTS['custom_variants']
    assert len(pairs) == 11
    assert {'ב=כ', 'ה=ח', 'ד=ר', 'ס=ם'} <= set(pairs)
    assert 'ה' + chr(0x0308) + '=ה' in pairs
    assert {letter + chr(0x0307) + '=' + letter for letter in 'תדטכצץ'} <= set(pairs)
    assert WEBSITE_DEFAULTS['variant_aggressive'] is False
    assert WEBSITE_DEFAULTS['variant_pairs_count'] == 30


def test_parallels_fingerprint_keeps_old_identities_and_tells_preferences_apart():
    """An old fingerprint was made with the server's settings then: the website
    defaults with Aggressive Mode on. It keeps matching a search with exactly those
    settings, and no other -- the new defaults (Aggressive off) included, or an old
    tab could show a newer search's rows as its own."""
    from web.export_state import compute_parallels_search_fingerprint
    from web.variant_preferences import website_defaults
    defaults = {k: v for k, v in website_defaults().items()
                if k in ('variant_min_word_len', 'variant_aggressive', 'custom_variants', 'comp_min_score')}
    base = dict(text=WORD, engine='chunk', mode='variants', variant_level=30, variant_max_changes=2)
    old = compute_parallels_search_fingerprint(**base)
    as_before = {**defaults, 'variant_aggressive': True}
    assert compute_parallels_search_fingerprint(**base, variant_preferences=as_before) == old
    assert compute_parallels_search_fingerprint(**base, variant_preferences=defaults) != old
    changed = {**as_before, 'custom_variants': {'ש=ס': True}}
    assert compute_parallels_search_fingerprint(**base, variant_preferences=changed) != old
