# -*- coding: utf-8 -*-
"""Measurement probe (not a suite test: the file name keeps it out of collection).

/search?q=<query> opened by URL versus the same query typed into the box, in
ONE process, on the real page in a NiceGUI User simulation with a stub engine
whose search costs the same fixed time every run. What differs between the
two runs is therefore the page's own work around the search.

Run from the repo root:
    python -m pytest tests/render_smoke/_probe_search_nav_latency.py -s -p no:cacheprovider

Env:
    PROBE_RUNS=5                 runs per variant
    PROBE_HANDSHAKE_DELAY=0.0    seconds between the page response and the socket
                                 handshake on a URL open (a browser needs ~0.5-1.5 s)
    PROBE_SIDECARS=1             point FJMS / PGP / titles at real sidecars (read-only)
                                 so the deferred loaders do their real work
    PROBE_SIDECAR_ROOT=<dir>     the checkout holding fist_data/, pgp_data/ and
                                 libraries_translations.db (default: this repo's root;
                                 a worktree has none, so name the main checkout)
"""
from __future__ import annotations

import asyncio
import collections
import gc
import os
import re
import statistics
import sys
import threading
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

MAIN = Path(os.environ.get('PROBE_SIDECAR_ROOT') or Path(__file__).resolve().parents[2])
if os.environ.get('PROBE_SIDECARS') == '1':
    for _sidecar in ('fist_data/fjms_enrichment.db', 'pgp_data/pgp.db', 'libraries_translations.db'):
        if not (MAIN / _sidecar).is_file():
            sys.exit(f'PROBE_SIDECARS=1 but {MAIN / _sidecar} is missing: set PROBE_SIDECAR_ROOT')
    os.environ.setdefault('GENIZAH_FJMS_DB_PATH', str(MAIN / 'fist_data' / 'fjms_enrichment.db'))

import httpx  # noqa: E402
from nicegui import core, ui  # noqa: E402
from nicegui.client import Client  # noqa: E402
from nicegui.context import context as nicegui_context  # noqa: E402
from nicegui.nicegui import _on_handshake  # noqa: E402
from nicegui.testing.general import prepare_simulation  # noqa: E402
from nicegui.testing.user import User  # noqa: E402
from nicegui.testing.user_interaction import UserInteraction  # noqa: E402
from nicegui.ui_run import set_storage_secret  # noqa: E402
from tests.render_smoke._bounded_run import run_bounded  # noqa: E402

import web.main  # noqa: E402,F401
import web.pages.search as _search_page  # noqa: E402
import web.safe_storage as _safe_storage  # noqa: E402
from web import research_jobs, search_runs  # noqa: E402
from web.state import state as _web_state  # noqa: E402

QUERY = 'שבת קודש'
RUNS = int(os.environ.get('PROBE_RUNS', '5'))
HANDSHAKE_DELAY = float(os.environ.get('PROBE_HANDSHAKE_DELAY', '0'))


def _rows(n=20):
    return [{
        'sys_id': f'990{i:09d}',
        'display': {'id': f'990{i:09d}', 'shelfmark': f'T-S {i}.{i}', 'title': 'סידור',
                    'library_code': 'CUL', 'source': 'V8'},
        'snippet': f'... ברוך אתה שבת קודש {i} ...',
        'full_text': 'שבת קודש ' * 30,
    } for i in range(1, n + 1)]


class _Engine:
    """A search that costs the same every time: early rows after 50 ms, the
    result after 300 ms, on the wait thread like the real worker's."""

    PREVIEW_AFTER = 0.05
    TOTAL = 0.30

    def __init__(self):
        self.calls = 0

    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, query, **kwargs):
        self.calls += 1
        rows = _rows()
        preview = kwargs.get('preview_callback')
        progress = kwargs.get('progress_callback')
        time.sleep(self.PREVIEW_AFTER)
        if preview is not None:
            preview(rows[:5])
        if progress is not None:
            progress(5, 10)
        time.sleep(self.TOTAL - self.PREVIEW_AFTER)
        return rows


class _LagProbe:
    """How late a 5 ms sleep wakes: the event loop's own blocking. A sampling
    thread records the loop thread's stack whenever a tick is >50 ms late."""

    def __init__(self):
        self.max = 0.0
        self.total = 0.0
        self.task = None
        self.last_tick = time.perf_counter()
        self.stacks = collections.Counter()
        self._sampling = False
        self._loop_tid = None

    async def _run(self):
        loop = asyncio.get_running_loop()
        self._loop_tid = threading.get_ident()
        while True:
            t = loop.time()
            self.last_tick = time.perf_counter()
            await asyncio.sleep(0.005)
            late = loop.time() - t - 0.005
            if late > 0:
                self.total += late
                self.max = max(self.max, late)

    def _sample(self):
        while self._sampling:
            time.sleep(0.01)
            if self._loop_tid is None or time.perf_counter() - self.last_tick < 0.05:
                continue
            frame = sys._current_frames().get(self._loop_tid)
            if frame is None:
                continue
            parts = []
            f = frame
            while f is not None and len(parts) < 14:
                name = f.f_code.co_filename.replace('\\', '/')
                for cut in ('site-packages/', 'search-nav-latency-85bf15/'):
                    if cut in name:
                        name = name.split(cut, 1)[1]
                parts.append(f'{name}:{f.f_lineno} {f.f_code.co_name}')
                f = f.f_back
            self.stacks[' <- '.join(parts)] += 10

    def start(self):
        self.max = self.total = 0.0
        self.gc = collections.Counter()
        self.gc_ms = collections.Counter()
        self._gc_t0 = {}
        if self._on_gc not in gc.callbacks:
            gc.callbacks.append(self._on_gc)
        self.task = asyncio.ensure_future(self._run())
        self._sampling = True
        threading.Thread(target=self._sample, daemon=True).start()

    def _on_gc(self, phase, info):
        gen = info['generation']
        if phase == 'start':
            self._gc_t0[gen] = time.perf_counter()
        else:
            self.gc[gen] += 1
            self.gc_ms[gen] += (time.perf_counter() - self._gc_t0.get(gen, time.perf_counter())) * 1000

    def stop(self):
        self._sampling = False
        if self._on_gc in gc.callbacks:
            gc.callbacks.remove(self._on_gc)
        self.task.cancel()


@asynccontextmanager
async def _app(engine):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    kept = (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine, _web_state.meta_mgr)
    _web_state.is_ready = lambda: True
    _web_state.searcher = engine
    _web_state.lab_engine = SimpleNamespace(settings=None)
    _web_state.meta_mgr = SimpleNamespace(csv_bank={})
    saved_executor = research_jobs._wait_executor
    research_jobs._wait_executor = None
    try:
        prepare_simulation()
        set_storage_secret('search-nav-latency-probe-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value='en'))
            if os.environ.get('PROBE_SIDECARS') == '1':
                from shared.translation_service import TranslationService
                stack.enter_context(patch.object(TranslationService, '_find_pgp_db',
                                                 staticmethod(lambda: str(MAIN / 'pgp_data' / 'pgp.db'))))
                stack.enter_context(patch.object(TranslationService, '_find_titles_db',
                                                 staticmethod(lambda: str(MAIN / 'libraries_translations.db'))))
                stack.enter_context(patch.object(TranslationService, '_find_fjms_db',
                                                 staticmethod(lambda: str(MAIN / 'fist_data' / 'fjms_enrichment.db'))))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                yield stack
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine, _web_state.meta_mgr) = kept
        research_jobs._wait_executor = saved_executor


@asynccontextmanager
async def _session():
    """A fresh browser: its own cookie, its own user storage."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app), base_url='http://test') as client:
        yield User(client)


async def _open(user, path, handshake_delay=0.0):
    """User.open, with the socket handshake *handshake_delay* s after the page
    response (a real browser has to load the page before it can connect)."""
    t0 = time.perf_counter()
    response = await user.http_client.get(path, follow_redirects=True)
    build_ms = (time.perf_counter() - t0) * 1000
    assert response.status_code == 200, response.status_code
    client_id = re.search(r"'client_id': '([0-9a-f-]+)'", response.text).group(1)
    user.client = Client.instances[client_id]
    if handshake_delay:
        await asyncio.sleep(handshake_delay)
    user.sio.on('connect')
    t1 = time.perf_counter()
    await _on_handshake(f'test-{uuid4()}', {
        'client_id': user.client.id, 'tab_id': user.tab_id, 'document_id': str(uuid4()),
    })
    HANDSHAKE_MS.append((time.perf_counter() - t1) * 1000)
    user._patch_outbox_emit_function()
    return build_ms, len(response.text)


HANDSHAKE_MS = []


def _elements(user, kind):
    with user.client:
        return [e for e in user.client.elements.values() if isinstance(e, kind)]


async def _wait_for(records, n, timeout=30.0):
    deadline = time.perf_counter() + timeout
    while len(records) < n:
        if time.perf_counter() > deadline:
            raise TimeoutError(f'no ux_done after {timeout} s; records={records}')
        await asyncio.sleep(0.002)


def test_probe():
    engine = _Engine()
    records = []          # (outcome, nav, server_ms, at)
    gets = {'n': 0}
    real_get = _safe_storage.safe_user_get

    def _counting_get(key, default=None):
        gets['n'] += 1
        return real_get(key, default)

    def _record(client, workflow, outcome, *, server_ms=None, nav=False, **_kw):
        records.append((outcome, nav, server_ms, time.perf_counter()))

    results = {'nav': [], 'typed': []}
    lag = _LagProbe()

    async def nav_run(user):
        search_runs.clear()
        gets['n'] = 0
        records.clear()
        lag.start()
        t0 = time.perf_counter()
        build_ms, html_len = await _open(user, f'/search?q={QUERY}', HANDSHAKE_DELAY)
        gets_at_build = gets['n']
        await _wait_for(records, 1)
        first = records[0]
        lag.stop()
        results['nav'].append({
            'server_ms': first[2], 'outcome': first[0], 'nav': first[1],
            'wall_ms': (first[3] - t0) * 1000, 'build_ms': build_ms, 'html_kb': html_len // 1024,
            'gets_build': gets_at_build, 'gets_total': gets['n'],
            'lag_max_ms': lag.max * 1000, 'lag_total_ms': lag.total * 1000,
            'gc': dict(lag.gc), 'gc_ms': {k: round(v) for k, v in lag.gc_ms.items()},
        })

    async def typed_run(user):
        search_runs.clear()
        await _open(user, '/search', 0.0)
        await asyncio.sleep(1.5)   # the page's deferred loaders settle, as on a reader's open page
        query = next(e for e in _elements(user, ui.input)
                     if e.props.get('placeholder') == 'Enter Hebrew text to search')
        with user.client:
            query.value = QUERY
        btn = next(b for b in _elements(user, ui.button)
                   if b.props.get('label') == 'Search' and 'px-8' in b.classes)
        gets['n'] = 0
        records.clear()
        lag.start()
        t0 = time.perf_counter()
        UserInteraction(user, {btn}, None).click()
        await _wait_for(records, 1)
        first = records[0]
        lag.stop()
        results['typed'].append({
            'server_ms': first[2], 'outcome': first[0], 'nav': first[1],
            'wall_ms': (first[3] - t0) * 1000, 'gets_total': gets['n'],
            'lag_max_ms': lag.max * 1000, 'lag_total_ms': lag.total * 1000,
            'gc': dict(lag.gc), 'gc_ms': {k: round(v) for k, v in lag.gc_ms.items()},
        })

    async def main():
        async with _app(engine) as stack:
            stack.enter_context(patch.object(_search_page, 'ux_done', _record))
            stack.enter_context(patch.object(_safe_storage, 'safe_user_get', _counting_get))
            # One warm-up of each, discarded: imports and first-use caches.
            async with _session() as user:
                await nav_run(user)
            async with _session() as user:
                await typed_run(user)
            results['nav'].clear()
            results['typed'].clear()
            for _ in range(RUNS):
                async with _session() as user:
                    await nav_run(user)
                async with _session() as user:
                    await typed_run(user)

    saved_slots = list(nicegui_context.slot_stack)
    try:
        run_bounded(main())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)

    def med(rows, key):
        return statistics.median(r[key] for r in rows)

    print()
    print(f'engine: preview after {int(_Engine.PREVIEW_AFTER*1000)} ms, result after {int(_Engine.TOTAL*1000)} ms;'
          f' runs={RUNS}; handshake delay={HANDSHAKE_DELAY} s; sidecars={os.environ.get("PROBE_SIDECARS")=="1"}')
    for name in ('nav', 'typed'):
        rows = results[name]
        print(f'{name:5} server_ms median {med(rows, "server_ms"):7.0f}  wall_ms {med(rows, "wall_ms"):7.0f}'
              f'  lag_max {med(rows, "lag_max_ms"):6.0f}  lag_total {med(rows, "lag_total_ms"):6.0f}'
              f'  storage_gets {med(rows, "gets_total"):4.0f}'
              f'  outcomes {[r["outcome"] for r in rows]} nav={rows[0]["nav"]}')
        for r in rows:
            print('      ', {k: (round(v) if isinstance(v, float) else v) for k, v in r.items()})
    print('handshake ms:', [round(h) for h in HANDSHAKE_MS])
    print('loop-blocking stacks (sampled every 10 ms while a tick was >50 ms late), top 8:')
    for stack, ms in lag.stacks.most_common(8):
        print(f'  ~{ms} ms  {stack}')
    sys.stdout.flush()
