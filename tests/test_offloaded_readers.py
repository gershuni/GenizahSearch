# -*- coding: utf-8 -*-
"""Regression tests for the 2026-07-30 Codex code-review findings.

Context: uvicorn runs a SINGLE worker (`ui.run` in web/main.py, no `workers=`) and
NiceGUI runs sync page builders/callbacks directly on the event loop, so ONE
blocking Supabase call stalls every concurrent request -- including unrelated
static assets -- while burning no CPU (hence prod showing `load average: 0.03`
during multi-second responses).

The first fix round removed the invalid `await <sync fn>()` but left the blocking
reads on the loop. Codex flagged four issues; these tests pin the corrections:

  P1  blocking reads now run in a worker AND keep their auth context, via an
      explicit `client=` threaded from the event loop (a bare `run.io_bound`
      would degrade to anonymous and silently drop user-scoped rows).
  P2  the leaderboard fan-out is bounded (shared thread pool) and deferred until
      its tab is actually opened.
  P2  post-I/O rendering is guarded against client teardown.
  P2  slow-request timing is genuinely outermost, so a short-circuited crawler
      403 is still counted.
"""

import asyncio
import inspect

import pytest


# ---------------------------------------------------------------------------
# P1 -- readers accept an explicit client, and default behaviour is unchanged
# ---------------------------------------------------------------------------

READERS_WITH_EXPLICIT_CLIENT = [
    ('web.supabase_client', 'get_corrections'),
    ('web.supabase_client', 'get_comments'),
    ('web.supabase_client', 'get_discovery_responses'),
    ('web.supabase_client', 'get_recent_items'),
    ('web.supabase_client', 'get_list_items'),
]


@pytest.mark.parametrize('module_name,func_name', READERS_WITH_EXPLICIT_CLIENT)
def test_reader_accepts_keyword_only_client_defaulting_to_none(module_name, func_name):
    """Each offloadable reader must take a keyword-only `client=None`.

    Keyword-only so it can never be captured by an existing positional caller;
    default None so every current call site keeps its exact behaviour.
    """
    module = pytest.importorskip(module_name)
    sig = inspect.signature(getattr(module, func_name))
    assert 'client' in sig.parameters, f"{func_name} lost its `client` parameter"
    param = sig.parameters['client']
    assert param.kind == inspect.Parameter.KEYWORD_ONLY, f"{func_name}: client must be keyword-only"
    assert param.default is None, f"{func_name}: client must default to None"


def test_lists_manager_threads_client_through():
    """`get_items_in_list_sync` must forward the client to BOTH backing readers."""
    user_lists = pytest.importorskip('web.user_lists')
    sig = inspect.signature(user_lists.UserListsManager.get_items_in_list_sync)
    assert sig.parameters['client'].kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters['client'].default is None

    src = inspect.getsource(user_lists.UserListsManager.get_items_in_list_sync)
    assert 'get_recent_items(self.user_id, client=client)' in src, 'recent path drops the client'
    assert 'get_list_items(list_id_int, client=client)' in src, 'list-items path drops the client'


def test_explicit_client_is_used_instead_of_get_user_client(monkeypatch):
    """The whole point: when a client is passed, get_user_client must NOT be called.

    If it were called inside a worker thread it would find no NiceGUI request
    context and degrade to the anonymous client, silently dropping the
    `TO authenticated` rows (a user's own pending corrections).
    """
    sc = pytest.importorskip('web.supabase_client')
    called = []
    monkeypatch.setattr(sc, 'get_user_client', lambda: called.append('leaked') or _FakeClient())

    fake = _FakeClient()
    sc.get_corrections(author_id='u1', client=fake)

    assert called == [], 'get_user_client was called despite an explicit client'
    assert fake.tables, 'the explicit client was never used'


def test_omitting_client_still_uses_get_user_client(monkeypatch):
    """Backwards compatibility: existing callers must be unaffected."""
    sc = pytest.importorskip('web.supabase_client')
    used = _FakeClient()
    monkeypatch.setattr(sc, 'get_user_client', lambda: used)

    sc.get_corrections(author_id='u1')

    assert used.tables, 'default path no longer builds a client via get_user_client'


class _FakeQuery:
    def __init__(self, recorder):
        self._recorder = recorder
        self.data = []

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def or_(self, *a, **k):
        return self

    def in_(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        return self


class _FakeClient:
    def __init__(self):
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        return _FakeQuery(self.tables)


# ---------------------------------------------------------------------------
# P1 -- a slow read must not stop the event loop
# ---------------------------------------------------------------------------

def test_offloaded_read_does_not_stall_an_event_loop_ticker():
    """The behavioural contract: a blocking read in a worker leaves the loop free.

    A ticker counts while a 0.4s synchronous read runs. On the loop it would get
    ~0 ticks; in a worker it keeps ticking. This is what the P1 fix buys.
    """
    import time as real_time
    from nicegui import run as nicegui_run

    ticks = 0

    async def ticker(stop):
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    def blocking_read():
        real_time.sleep(0.4)
        return ['row']

    async def main():
        stop = asyncio.Event()
        t = asyncio.create_task(ticker(stop))
        rows = await nicegui_run.io_bound(blocking_read)
        stop.set()
        await t
        return rows

    rows = asyncio.run(main())
    assert rows == ['row']
    # ~40 ticks are theoretically possible; assert a floor well above the
    # on-loop case (which would be 0-1) without being timing-flaky.
    assert ticks >= 5, f'event loop was starved during the offloaded read (ticks={ticks})'


# ---------------------------------------------------------------------------
# P2 -- leaderboard: bounded concurrency + lazy start + guarded render
# ---------------------------------------------------------------------------

def test_leaderboard_concurrency_is_bounded(monkeypatch):
    """No more than _LEADERBOARD_COUNT_CONCURRENCY counts may run at once.

    `run.io_bound` shares ONE process-wide thread pool with the rest of the app,
    so an unbounded ~20-wide fan-out could starve unrelated offloaded work.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    limit = corrections._LEADERBOARD_COUNT_CONCURRENCY
    assert limit <= 8, 'bound should stay small relative to the shared pool'

    profiles = [{'id': f'u{i}', 'reputation': i} for i in range(20)]
    monkeypatch.setattr(corrections, '_fetch_top_profiles', lambda lim: list(profiles))

    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    async def fake_io_bound(fn, *args, **kwargs):
        nonlocal in_flight, peak
        if fn is corrections._fetch_top_profiles:
            return fn(*args, **kwargs)
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        return 1

    monkeypatch.setattr(corrections.run, 'io_bound', fake_io_bound)

    users = asyncio.run(corrections.fetch_leaderboard_users(limit=20))
    assert len(users) == 20
    assert peak <= limit, f'peak concurrency {peak} exceeded the bound {limit}'


def test_leaderboard_view_returns_loader_and_does_not_fetch_eagerly():
    """create_leaderboard_view must hand back a loader, not start the fetch itself.

    The Leaderboard tab is not the default, so building the panel must not spend
    up to `limit` Supabase queries for a tab most visitors never open.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    src = inspect.getsource(corrections.create_corrections_page)

    assert 'return load_leaderboard' in src, 'view no longer returns its loader'
    assert 'load_leaderboard = create_leaderboard_view()' in src, 'caller does not capture the loader'
    assert 'tabs.on_value_change(_on_tab_change)' in src, 'loader is not wired to tab activation'
    assert 'asyncio.ensure_future(_deferred_load_leaderboard' not in src, \
        'leaderboard is still started eagerly at page build'


def test_leaderboard_tab_gate_compares_against_the_tab_name():
    """`ui.tabs`'s value is the tab NAME (str), not the Tab element.

    `ui.tab(name)` stores it in `_props['name']`, so comparing the event value to
    the Tab object would never match and the leaderboard would never load.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    src = inspect.getsource(corrections.create_corrections_page)
    assert 'event.value != leaderboard_tab_name' in src, \
        'tab gate must compare the event value to the tab NAME string'


def test_leaderboard_render_is_inside_the_teardown_guard():
    """Rendering happens after lengthy I/O, so it needs the SEED-008 guard.

    Without it, a user closing the tab mid-fetch produces an unhandled NiceGUI
    RuntimeError from mutating a torn-down client.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    src = inspect.getsource(corrections.create_corrections_page)
    loader = src.split('async def load_leaderboard()', 1)[1].split('return load_leaderboard', 1)[0]

    assert 'has_been_deleted' in loader, 'no teardown guard around leaderboard render'
    assert 'except RuntimeError' in loader, 'teardown RuntimeError is not handled'
    # The error branch mutates the UI too and must be guarded as well.
    error_branch = loader.split('except Exception', 1)[1]
    assert 'has_been_deleted' in error_branch, 'error-render path lacks the teardown guard'


# ---------------------------------------------------------------------------
# P1/P2 -- every deferred loader guards its render and threads a client
# ---------------------------------------------------------------------------

DEFERRED_LOADERS = [
    ('web.pages.corrections', '_deferred_load_edits'),
    ('web.pages.corrections', '_deferred_load_comments'),
    ('web.pages.home', '_deferred_load_recent'),
    ('web.pages.discoveries', '_deferred_responses'),
]


@pytest.mark.parametrize('module_name,loader', DEFERRED_LOADERS)
def test_deferred_loader_offloads_and_guards(module_name, loader):
    """Each deferred loader must: offload the read, pass a client, guard the render."""
    module = pytest.importorskip(module_name)
    import pathlib
    src = pathlib.Path(module.__file__).read_text(encoding='utf-8')
    block = src.split(f'async def {loader}', 1)
    assert len(block) == 2, f'{loader} not found in {module_name}'
    # Take a generous slice; these are short functions.
    body = block[1][:2600]

    assert 'run.io_bound' in body, f'{loader} still performs its read on the event loop'
    assert 'client=reader_client' in body or 'client=reader_client' in body, \
        f'{loader} does not pass an explicitly-built client into the worker'
    assert 'get_user_client()' in body, f'{loader} does not build the client on the loop'
    assert 'has_been_deleted' in body, f'{loader} renders without a teardown guard'


# ---------------------------------------------------------------------------
# P2 -- timing middleware must be outermost
# ---------------------------------------------------------------------------

def test_timing_middleware_is_registered_after_all_http_decorators():
    """Starlette wraps in reverse, so the LAST registration is OUTERMOST.

    Registered before the `@app.middleware('http')` decorators, the timing
    middleware sat inside them -- and `_mark_non_document_paths_noindex` can
    return 403/404 without calling through, so blocked crawler/archive requests
    were never timed at all.
    """
    import pathlib
    main_src = pathlib.Path('web/main.py').read_text(encoding='utf-8')
    timing_at = main_src.index('app.add_middleware(SlowRequestTimingMiddleware)')
    last_decorator_at = main_src.rindex("@app.middleware('http')")
    assert timing_at > last_decorator_at, (
        'SlowRequestTimingMiddleware must be registered AFTER every '
        "@app.middleware('http') decorator so it wraps outermost"
    )


def test_short_circuited_403_is_still_timed():
    """A middleware that returns early without calling through is still counted.

    Proves the `finally`-based accounting survives a short-circuit, which is the
    shape of the crawler 403 the ordering bug was hiding.
    """
    perf_watch = pytest.importorskip('web.perf_watch')
    perf_watch.reset_stats()

    async def blocking_app(scope, receive, send):
        # Mimics _mark_non_document_paths_noindex's 403: responds, never delegates.
        await send({'type': 'http.response.start', 'status': 403})
        await send({'type': 'http.response.body', 'body': b'Forbidden', 'more_body': False})

    async def send(message):
        pass

    mw = perf_watch.SlowRequestTimingMiddleware(blocking_app)
    asyncio.run(mw({'type': 'http', 'path': '/wp-login.php', 'method': 'GET'}, None, send))

    assert perf_watch.get_stats_snapshot()['requests'] == 1
    perf_watch.reset_stats()
