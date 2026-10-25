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

import ast
import asyncio
import inspect
import pathlib

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
    ('web.supabase_client', 'get_user_lists'),
]

# Writers take the same keyword-only client (2026-10-20, RLS-06): an
# owner-only UPDATE/DELETE run anonymously changes 0 rows and says nothing.
WRITERS_WITH_EXPLICIT_CLIENT = [
    ('web.supabase_client', 'update_profile'),
    ('web.supabase_client', 'add_recent_item'),
    # 2026-10-21: the list, project, correction and comment writers run in a
    # worker now (UserListsManager, the comment dialog, the browse editor).
    ('web.supabase_client', 'create_list'),
    ('web.supabase_client', 'update_list'),
    ('web.supabase_client', 'delete_list'),
    ('web.supabase_client', 'restore_list'),
    ('web.supabase_client', 'empty_trash'),
    ('web.supabase_client', 'add_list_item'),
    ('web.supabase_client', 'add_list_items'),
    ('web.supabase_client', 'update_list_item'),
    ('web.supabase_client', 'delete_list_item'),
    ('web.supabase_client', 'get_projects'),
    ('web.supabase_client', 'create_project'),
    ('web.supabase_client', 'update_project'),
    ('web.supabase_client', 'delete_project'),
    ('web.supabase_client', 'create_correction'),
    ('web.supabase_client', 'update_correction'),
    ('web.supabase_client', 'create_comment'),
]


@pytest.mark.parametrize('module_name,func_name',
                         READERS_WITH_EXPLICIT_CLIENT + WRITERS_WITH_EXPLICIT_CLIENT)
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
    assert 'get_recent_items(uid, client=client)' in src, 'recent path drops the client'
    assert 'get_list_items(list_id_int, client=client)' in src, 'list-items path drops the client'
    # The auth decision must also be overridable, or the worker re-derives it
    # from storage it cannot read -- see the P1 test further down.
    for name in ('is_authenticated', 'user_id'):
        assert sig.parameters[name].kind == inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters[name].default is None


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

def _instrument_leaderboard(monkeypatch, corrections, n_profiles=20):
    """Wire fakes and return a live peak-concurrency counter.

    Counts concurrency INSIDE the blocking functions, on the worker threads, so
    the real `bounded_io_bound` + real semaphore + real thread pool are all
    exercised. (An earlier version faked `run.io_bound` and skipped the profiles
    call -- which is precisely why it could not see the unbounded profile read.)

    Injects a FRESH module-level semaphore: `_LoopBoundMixin._get_loop` caches the
    loop on first contended use and raises if reused from another loop, and each
    test drives its own `asyncio.run`.
    """
    import threading
    import time as real_time

    profiles = [{'id': f'u{i}', 'reputation': i} for i in range(n_profiles)]
    monkeypatch.setattr(
        corrections, '_LEADERBOARD_COUNT_SEMAPHORE',
        asyncio.Semaphore(corrections._LEADERBOARD_COUNT_CONCURRENCY),
    )

    stats = {'in_flight': 0, 'peak': 0}
    lock = threading.Lock()

    def _enter():
        with lock:
            stats['in_flight'] += 1
            stats['peak'] = max(stats['peak'], stats['in_flight'])

    def _exit():
        with lock:
            stats['in_flight'] -= 1

    def fake_profiles(limit):
        _enter()
        try:
            real_time.sleep(0.02)
            return list(profiles)
        finally:
            _exit()

    def fake_count(uid):
        _enter()
        try:
            real_time.sleep(0.02)
            return 1
        finally:
            _exit()

    monkeypatch.setattr(corrections, '_fetch_top_profiles', fake_profiles)
    monkeypatch.setattr(corrections, 'get_user_corrections_count', fake_count)
    return stats


def test_leaderboard_concurrency_is_bounded(monkeypatch):
    """No more than _LEADERBOARD_COUNT_CONCURRENCY counts may run at once.

    `run.io_bound` shares ONE process-wide thread pool with the rest of the app,
    so an unbounded ~20-wide fan-out could starve unrelated offloaded work.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    limit = corrections._LEADERBOARD_COUNT_CONCURRENCY
    assert limit <= 8, 'bound should stay small relative to the shared pool'

    stats = _instrument_leaderboard(monkeypatch, corrections)
    users = asyncio.run(corrections.fetch_leaderboard_users(limit=20))

    assert len(users) == 20
    assert stats['peak'] <= limit, f"peak {stats['peak']} exceeded the bound {limit}"


def test_leaderboard_bound_holds_across_concurrent_visitors(monkeypatch):
    """The bound must be PROCESS-wide, not per-invocation.

    Regression for the PR-review P2: the semaphore was built inside
    `fetch_leaderboard_users`, so every concurrent visitor got their own 4 slots
    and the real ceiling was 4 x visitors -- enough to fill NiceGUI's shared
    thread pool and delay search and image work. Three simultaneous visitors on
    ONE event loop (which is all production has) must still total <= 4.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    limit = corrections._LEADERBOARD_COUNT_CONCURRENCY
    stats = _instrument_leaderboard(monkeypatch, corrections)

    async def three_visitors():
        return await asyncio.gather(*[
            corrections.fetch_leaderboard_users(limit=20) for _ in range(3)
        ])

    results = asyncio.run(three_visitors())

    assert [len(r) for r in results] == [20, 20, 20]
    assert stats['peak'] <= limit, (
        f"combined peak {stats['peak']} exceeded the process-wide bound {limit} -- "
        'the semaphore is per-invocation again'
    )


def test_leaderboard_profile_reads_are_also_bounded(monkeypatch):
    """The initial profiles query must sit under the bound too.

    Regression for the follow-up PR-review P2: `_fetch_top_profiles` was offloaded
    BEFORE acquiring the semaphore, so the true ceiling was "one unbounded profile
    read per visitor, plus 4 counts".

    Uses 8 simultaneous visitors deliberately: their profile reads all start at
    once, so an unbounded version peaks at 8 while the bounded one cannot exceed
    4. Three visitors would NOT discriminate -- 3 unbounded profile reads stay
    under the bound by luck.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    limit = corrections._LEADERBOARD_COUNT_CONCURRENCY
    stats = _instrument_leaderboard(monkeypatch, corrections, n_profiles=2)

    async def many_visitors():
        return await asyncio.gather(*[
            corrections.fetch_leaderboard_users(limit=2) for _ in range(8)
        ])

    results = asyncio.run(many_visitors())

    assert len(results) == 8
    assert stats['peak'] <= limit, (
        f"combined peak {stats['peak']} exceeded {limit} -- the profiles read is "
        'outside the shared bound'
    )


def test_every_leaderboard_offload_goes_through_the_bounded_helper():
    """Both offloaded calls must use `bounded_io_bound` with the shared semaphore.

    A bare `async with semaphore: await run.io_bound(...)` releases the permit
    when the CALLER unwinds, and `run.io_bound` swallows CancelledError and
    returns while its thread keeps working -- so a teardown mid-fetch frees the
    slot with the request still in flight.
    """
    corrections = pytest.importorskip('web.pages.corrections')
    src = inspect.getsource(corrections.fetch_leaderboard_users)

    assert 'async with _LEADERBOARD_COUNT_SEMAPHORE' not in src, \
        'raw `async with` on the semaphore releases the permit too early'
    assert src.count('bounded_io_bound(_LEADERBOARD_COUNT_SEMAPHORE') == 2, \
        'expected exactly two bounded offloads (profiles + counts)'
    # Look at CODE only -- the docstring legitimately discusses `run.io_bound`.
    code = '\n'.join(
        line for line in src.splitlines() if not line.lstrip().startswith('#')
    )
    assert 'await run.io_bound(' not in code, 'an unbounded run.io_bound call remains'


def test_discovery_responses_use_the_bounded_helper():
    import pathlib
    src = pathlib.Path('web/pages/discoveries.py').read_text(encoding='utf-8')
    assert 'bounded_io_bound(\n' in src or 'bounded_io_bound(' in src
    assert 'async with _RESPONSES_FETCH_SEMAPHORE' not in src, \
        'raw `async with` releases the permit before the worker finishes'


def test_leaderboard_semaphore_is_module_level():
    """Pins the fix structurally as well as behaviourally."""
    corrections = pytest.importorskip('web.pages.corrections')
    assert isinstance(corrections._LEADERBOARD_COUNT_SEMAPHORE, asyncio.Semaphore)
    src = inspect.getsource(corrections.fetch_leaderboard_users)
    assert 'asyncio.Semaphore(' not in src, \
        'fetch_leaderboard_users constructs its own semaphore -- the bound must be shared'
    assert '_LEADERBOARD_COUNT_SEMAPHORE' in src, 'the shared semaphore is not acquired'


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

    assert 'client_gone(' in loader, 'no teardown guard around leaderboard render'
    assert 'except RuntimeError' in loader, 'teardown RuntimeError is not handled'
    # The error branch mutates the UI too and must be guarded as well.
    error_branch = loader.split('except Exception', 1)[1]
    assert 'client_gone(' in error_branch, 'error-render path lacks the teardown guard'


# ---------------------------------------------------------------------------
# P1/P2 -- every deferred loader guards its render and threads a client
# ---------------------------------------------------------------------------

# NOTE (v8.5.2): these are source-TEXT assertions -- they prove a guard is
# *present*, not that it *works*. An earlier version of this file asserted
# `'has_been_deleted' in body` and passed for weeks while that guard raised
# AttributeError on every call in production. Behavioural coverage of the guard
# itself lives in tests/test_client_guard.py, which drives it with real
# nicegui.Client objects; keep both.

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
    assert 'client_gone(' in body, f'{loader} renders without a teardown guard'


# ---------------------------------------------------------------------------
# PR #319 review, P1 -- the worker must not re-derive auth for itself
# ---------------------------------------------------------------------------

def test_get_items_in_list_sync_honours_explicit_auth_when_context_is_lost(monkeypatch):
    """THE regression test for the PR-review P1.

    Passing `client` alone was not enough. `get_items_in_list_sync` also consulted
    `self.is_authenticated` -> `GlobalAuthState.is_logged_in()` -> `safe_user_get`,
    which in a worker thread has no NiceGUI UI context and reads as logged-OUT.
    Execution then fell through to the `local_mgr` branch and a signed-in user got
    local-or-empty recent activity, while the authenticated client sat unused.

    Here `is_logged_in` returns False (exactly what the worker sees) but the
    caller passes the auth decision it resolved on the loop, so the Supabase path
    must still be taken.
    """
    user_lists = pytest.importorskip('web.user_lists')

    monkeypatch.setattr(user_lists.GlobalAuthState, 'is_logged_in', classmethod(lambda cls: False))
    monkeypatch.setattr(user_lists.GlobalAuthState, 'get_user_id', classmethod(lambda cls: None))

    recent_calls = []
    monkeypatch.setattr(
        user_lists, 'get_recent_items',
        lambda uid, client=None: recent_calls.append((uid, client)) or [{'sys_id': '42'}],
    )

    class _LocalMgr:
        def __init__(self):
            self.used = False

        def get_items_in_list(self, list_id):
            self.used = True
            return [{'sys_id': 'LOCAL'}]

    local = _LocalMgr()
    mgr = user_lists.UserListsManager(local_mgr=local)
    sentinel_client = object()

    items = mgr.get_items_in_list_sync(
        'recent', client=sentinel_client, is_authenticated=True, user_id='u1'
    )

    assert not local.used, 'fell through to local_mgr despite explicit is_authenticated=True'
    assert recent_calls == [('u1', sentinel_client)], \
        f'Supabase reader not called with the captured user id + client: {recent_calls}'
    assert [it['sys_id'] for it in items] == ['42']


def test_get_items_in_list_sync_still_defers_to_ambient_auth_by_default(monkeypatch):
    """Omitting the overrides still defers to the ambient auth decision.

    Anonymous (ambient logged-out) now yields [] WITHOUT reading the shared
    local store: the old `[{'sys_id': 'LOCAL'}]` expectation pinned the
    server-wide anonymous store leak (improvement sweep C2).
    """
    user_lists = pytest.importorskip('web.user_lists')
    monkeypatch.setattr(user_lists.GlobalAuthState, 'is_logged_in', classmethod(lambda cls: False))

    class _LocalMgr:
        def __init__(self):
            self.used = False

        def get_items_in_list(self, list_id):
            self.used = True
            return [{'sys_id': 'LOCAL'}]

    local = _LocalMgr()
    mgr = user_lists.UserListsManager(local_mgr=local)
    assert mgr.get_items_in_list_sync('recent') == []
    assert not local.used, 'anonymous read fell through to the shared local store'


def test_home_passes_the_auth_decision_into_the_worker():
    """home.py must hand over is_authenticated AND user_id, not just the client."""
    import pathlib
    src = pathlib.Path('web/pages/home.py').read_text(encoding='utf-8')
    block = src.split('async def _deferred_load_recent', 1)[1][:3400]
    # Strip comment lines: these docs mention `run.io_bound` in prose, and an
    # ordering assertion must look at CODE, not at explanatory text.
    code = '\n'.join(
        line for line in block.splitlines() if not line.lstrip().startswith('#')
    )

    assert 'is_authenticated=is_authed' in code, 'auth decision not passed to the worker'
    assert 'user_id=reader_user_id' in code, 'user id not passed to the worker'
    # The decision itself must be taken before the offload, not inside it.
    assert code.index('is_authed = lists_mgr.is_authenticated') < code.index('await run.io_bound('), \
        'auth must be resolved on the event loop, before the offload'


# ---------------------------------------------------------------------------
# PR #319 review, P2 -- discovery responses: gated + bounded
# ---------------------------------------------------------------------------

def test_discovery_responses_are_gated_on_expansion_open():
    """A 50-card feed must not fire 50 Supabase reads on load.

    NiceGUI builds expansion content eagerly, so an unconditional
    `ensure_future` per card ran one read per card regardless of whether the
    visitor ever opened it.
    """
    import pathlib
    src = pathlib.Path('web/pages/discoveries.py').read_text(encoding='utf-8')
    assert 'as content_expansion' in src, 'expansion is not captured'
    assert 'content_expansion.on_value_change(_on_expansion_change)' in src, \
        'responses loader is not wired to expansion activation'
    assert 'asyncio.ensure_future(_deferred_responses())' not in src, \
        'responses are still loaded eagerly for every card'


def test_discovery_responses_fetch_is_bounded():
    discoveries = pytest.importorskip('web.pages.discoveries')
    sem = discoveries._RESPONSES_FETCH_SEMAPHORE
    assert isinstance(sem, asyncio.Semaphore)
    # Small relative to NiceGUI's shared pool.
    assert sem._value <= 8, f'bound {sem._value} is too loose for the shared executor'

    import pathlib
    src = pathlib.Path('web/pages/discoveries.py').read_text(encoding='utf-8')
    assert 'bounded_io_bound(' in src and '_RESPONSES_FETCH_SEMAPHORE,' in src, \
        'the responses read does not go through the bounded helper'


def test_expansion_gate_only_fires_on_open_and_only_once():
    """Mirrors the gate's logic: ignore close events, run at most once."""
    started = []
    responses_started = False

    async def fake_load():
        started.append(1)

    async def on_change(value):
        nonlocal responses_started
        if responses_started or not value:
            return
        responses_started = True
        await fake_load()

    async def main():
        await on_change(False)  # collapse event before any open
        assert started == []
        await on_change(True)   # first open -> loads
        await on_change(True)   # re-open -> must not reload
        await on_change(False)
        return started

    assert asyncio.run(main()) == [1]


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


# ---------------------------------------------------------------------------
# 2026-08-19 -- /admin was still doing every Supabase round trip on the loop
# ---------------------------------------------------------------------------
#
# perf-watch logged /admin at 2.0-4.5 s with the loop blocked for most of it.
# The page build called SEVEN synchronous Supabase round trips inline: pending
# corrections (x2 queries), the identification-review queue, all users, the
# corrections count -- and then the Statistics tab re-fetched the users and the
# pending corrections the other two tabs had already loaded. On a single uvicorn
# worker each of those stalled every concurrent request, static assets included.

# Every name that performs a Supabase round trip. Calling any of them directly
# means calling it ON the event loop; the only place that is correct is inside
# the worker payload itself.
ADMIN_BLOCKING_CALLS = [
    'get_pending_corrections',
    'get_all_users',
    'get_user_accounts',
    'get_all_corrections_count',
    'get_pending_identification_reviews',
    'update_correction_status',
    'update_user_role',
    'delete_user',
    # The aggregate payload itself: calling it directly is calling all four on
    # the loop at once, which is precisely the shape being removed here.
    '_load_admin_data',
]
ADMIN_WORKER_PAYLOADS = {'_load_admin_data'}


def test_no_admin_supabase_call_is_made_on_the_event_loop():
    """AST guard: these names may be PASSED to run.io_bound, never CALLED.

    Structural rather than source-text because the failure mode is a call
    reappearing anywhere in a 900-line file, not a missing keyword in one known
    function. `run.io_bound(get_all_users)` references the name; it does not
    call it, so this stays quiet for the correct shape and fires on the wrong one.
    """
    import ast
    import pathlib

    admin = pytest.importorskip('web.pages.admin')
    tree = ast.parse(pathlib.Path(admin.__file__).read_text(encoding='utf-8'))

    offenders = []

    def walk(node, enclosing):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, child.name)
                continue
            if (isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id in ADMIN_BLOCKING_CALLS
                    and enclosing not in ADMIN_WORKER_PAYLOADS):
                offenders.append(f'{child.func.id}() called in {enclosing} '
                                 f'at line {child.lineno}')
            walk(child, enclosing)

    walk(tree, '<module>')
    assert not offenders, (
        'these Supabase round trips run on the event loop and stall every '
        'concurrent request: ' + '; '.join(offenders))


def test_admin_page_builds_the_client_on_the_loop_then_offloads():
    """The auth half of the fix, which a bare run.io_bound would silently break."""
    import pathlib

    admin = pytest.importorskip('web.pages.admin')
    src = pathlib.Path(admin.__file__).read_text(encoding='utf-8')
    body = src.split('async def create_admin_page', 1)[1][:3000]

    assert 'user_client = get_user_client()' in body, \
        'create_admin_page must build the authenticated client ON the loop'
    assert 'run.io_bound(_load_admin_data, user_client)' in body, \
        'create_admin_page must do its Supabase reads in a worker'


def test_admin_payload_fetches_each_source_once_and_threads_the_client(monkeypatch):
    """Behavioural: no duplicate round trips, and the queue keeps its auth.

    The client assertion is the load-bearing one. get_user_client() reads the
    auth session through safe_user_get, which returns {} outside the NiceGUI
    context -- so building it inside the worker would hand the moderation queue
    an ANONYMOUS client and return an empty queue that reads as "nothing pending".
    """
    admin = pytest.importorskip('web.pages.admin')

    calls = []
    seen = {}

    def _reviews(client=None):
        calls.append('reviews')
        seen['client'] = client
        return ('review',)

    def _emails(client):
        calls.append('emails')
        seen['emails_client'] = client
        return {'u1': {'email': 'a@x.org', 'sign_in_name': 'Ada'}}, None

    monkeypatch.setattr(admin, 'get_pending_corrections',
                        lambda: calls.append('pending') or ['correction'])
    monkeypatch.setattr(admin, 'get_all_users',
                        lambda: calls.append('users') or [{'id': 'u1'}, {'id': 'u2'}])
    monkeypatch.setattr(admin, 'get_user_accounts', _emails)
    monkeypatch.setattr(admin, 'get_all_corrections_count',
                        lambda: calls.append('count') or 7)
    monkeypatch.setattr(admin, 'get_pending_identification_reviews', _reviews)

    sentinel = object()
    data = admin._load_admin_data(sentinel)

    assert seen['client'] is sentinel, \
        'the worker re-derived its own client instead of using the caller\'s'
    # The account emails come from an admin-only RPC: an anonymous client
    # would be refused and the Users tab would show no emails at all.
    assert seen['emails_client'] is sentinel, \
        'the email lookup did not get the admin client'
    assert calls.count('users') == 1, f'users fetched {calls.count("users")}x: {calls}'
    assert calls.count('emails') == 1, f'emails fetched {calls.count("emails")}x: {calls}'
    assert calls.count('pending') == 1, f'pending fetched {calls.count("pending")}x: {calls}'
    assert data == {
        'pending_corrections': ['correction'],
        'identification_reviews': ('review',),
        'users': [{'id': 'u1', 'email': 'a@x.org', 'sign_in_name': 'Ada'}, {'id': 'u2'}],
        'emails_problem': None,
        'total_corrections': 7,
    }


def test_admin_payload_offloaded_leaves_the_loop_free(monkeypatch):
    """The whole point: four serial round trips must not stop the world."""
    import time as real_time
    from nicegui import run as nicegui_run

    admin = pytest.importorskip('web.pages.admin')

    def _slow(value):
        def _call(*_a, **_kw):
            real_time.sleep(0.1)
            return value
        return _call

    monkeypatch.setattr(admin, 'get_pending_corrections', _slow([]))
    monkeypatch.setattr(admin, 'get_all_users', _slow([]))
    monkeypatch.setattr(admin, 'get_all_corrections_count', _slow(0))
    monkeypatch.setattr(admin, 'get_pending_identification_reviews', _slow(()))

    ticks = 0

    async def ticker(stop):
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    async def main():
        stop = asyncio.Event()
        t = asyncio.create_task(ticker(stop))
        result = await nicegui_run.io_bound(admin._load_admin_data, None)
        stop.set()
        await t
        return result

    result = asyncio.run(main())
    assert result['total_corrections'] == 0
    assert ticks >= 5, f'event loop was starved while /admin loaded (ticks={ticks})'


# ---------------------------------------------------------------------------
# 2026-10-20 (RLS-06) -- no user-scoped helper is offloaded without client=
# ---------------------------------------------------------------------------
#
# The P1 fix above threaded client= through the readers it knew about; three
# other sites kept the bare shape and ran ANONYMOUS for a signed-in user. The
# /puzzle "Add from List" picker (get_user_lists, get_list_items) always said
# "No lists found", and web sign-up's update_profile changed 0 rows, so the
# chosen username was never saved. NiceGUI's run.io_bound, and bounded_io_bound
# and io_bound_result.io_bound built on it, submit with loop.run_in_executor,
# which does not copy contextvars: in the worker app.storage.user raises,
# safe_user_get returns {}, and get_user_client() returns the anonymous
# singleton. (asyncio.to_thread copies the context and is not affected.)
#
# Scope: a web/supabase_client.py function that calls get_user_client(), passed
# STRAIGHT to an offload -- bare, inside a lambda, or through partial. Blind
# spot: a wrapper defined elsewhere that calls such a helper and is offloaded
# itself (admin.py's _load_admin_data takes its client positionally and has
# its own test above).

_REPO = pathlib.Path(__file__).resolve().parent.parent

# Offload call name -> position of the callable among its arguments.
_OFFLOADERS = {'io_bound': 0, 'bounded_io_bound': 1}


def _call_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _user_scoped_helpers():
    """Top-level functions of web/supabase_client.py that call get_user_client()."""
    tree = ast.parse((_REPO / 'web' / 'supabase_client.py').read_text(encoding='utf-8'))
    return {
        node.name for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name != 'get_user_client'
        and any(isinstance(n, ast.Call) and _call_name(n) == 'get_user_client'
                for n in ast.walk(node))
    }


def _helper_refs(tree, helpers, own_module=False):
    """How a file names the helpers: {local name: helper}, plus module aliases.

    Import statements anywhere in the file count (several pages import inside
    the handler). Only names imported from web.supabase_client are helpers, so
    an unrelated local function of the same name is not.
    """
    names = {h: h for h in helpers} if own_module else {}
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'web.supabase_client' or (node.level and node.module == 'supabase_client'):
                for a in node.names:
                    if a.name in helpers:
                        names[a.asname or a.name] = a.name
            elif node.module == 'web' or (node.level and node.module is None):
                for a in node.names:
                    if a.name == 'supabase_client':
                        modules.add(a.asname or a.name)
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == 'web.supabase_client':
                    modules.add(a.asname or 'web.supabase_client')
    return names, modules


def _helper_named(expr, helpers, names, modules):
    if isinstance(expr, ast.Name):
        return names.get(expr.id)
    if isinstance(expr, ast.Attribute) and expr.attr in helpers \
            and ast.unparse(expr.value) in modules:
        return expr.attr
    return None


def _passes_client(call):
    # **kwargs cannot be read statically; give it the benefit of the doubt.
    return any(kw.arg == 'client' or kw.arg is None for kw in call.keywords)


def _offload_violations(source, helpers, own_module=False):
    """Return ([(line, helper)], offload sites seen) for one file's source."""
    tree = ast.parse(source)
    names, modules = _helper_refs(tree, helpers, own_module)
    violations, sites = [], 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        pos = _OFFLOADERS.get(_call_name(node))
        if pos is None or len(node.args) <= pos:
            continue
        sites += 1
        target = node.args[pos]
        if isinstance(target, ast.Call) and _call_name(target) == 'partial' and target.args:
            helper = _helper_named(target.args[0], helpers, names, modules)
            if helper and not (_passes_client(target) or _passes_client(node)):
                violations.append((node.lineno, helper))
        elif isinstance(target, ast.Lambda):
            for inner in ast.walk(target.body):
                if isinstance(inner, ast.Call):
                    helper = _helper_named(inner.func, helpers, names, modules)
                    if helper and not _passes_client(inner):
                        violations.append((node.lineno, helper))
        else:
            helper = _helper_named(target, helpers, names, modules)
            if helper and not _passes_client(node):
                violations.append((node.lineno, helper))
    return violations, sites


def test_user_scoped_helper_set_is_not_empty():
    """The guard is only as good as this set: an empty one passes everything."""
    helpers = _user_scoped_helpers()
    for name in ('get_user_lists', 'get_list_items', 'update_profile',
                 'add_recent_item', 'get_recent_items', 'get_corrections'):
        assert name in helpers, f'{name} no longer recognised as user-scoped'
    # get_profile reads a TO-public table through get_client() on purpose.
    assert 'get_profile' not in helpers


def test_no_user_scoped_helper_is_offloaded_without_a_client():
    """Every offload in web/ of a user-scoped helper passes client=."""
    helpers = _user_scoped_helpers()
    offenders, sites = [], 0
    for path in sorted((_REPO / 'web').rglob('*.py')):
        rel = path.relative_to(_REPO).as_posix()
        found, n = _offload_violations(path.read_text(encoding='utf-8'), helpers,
                                       own_module=(rel == 'web/supabase_client.py'))
        sites += n
        offenders += [f'{rel}:{line} {helper}' for line, helper in found]
    # web/ has well over a hundred offloads; a handful means the scan broke.
    assert sites > 50, f'only {sites} offload sites seen -- the scan is not finding them'
    assert not offenders, (
        'these offloads run a user-scoped helper in a worker with no client=, so it '
        'silently uses the ANONYMOUS client (owner-only rows come back empty, '
        'owner-only writes change nothing). Build `client = get_user_client()` on the '
        'event loop and pass client=client: ' + '; '.join(offenders))


_SEEDED_HEADER = (
    'import functools\n'
    'from functools import partial\n'
    'from nicegui import run\n'
    'from web import io_bound_result\n'
    'from web import supabase_client as sc\n'
    'from web.bounded_io import bounded_io_bound\n'
    'from web.supabase_client import get_user_lists, get_list_items, update_profile, get_profile\n'
    'from web.supabase_client import get_user_lists as lists_alias\n'
)

SEEDED_VIOLATIONS = [
    ('bare', 'async def f(uid):\n    await run.io_bound(get_user_lists, uid)\n'),
    ('lambda', 'async def f(uid):\n    await run.io_bound(lambda: get_user_lists(uid))\n'),
    ('partial', 'async def f():\n    await run.io_bound(functools.partial(get_list_items, 7))\n'),
    ('bare_partial', 'async def f():\n    await run.io_bound(partial(get_list_items, 7))\n'),
    ('bounded', 'async def f(sem):\n    await bounded_io_bound(sem, get_list_items, 7)\n'),
    ('io_bound_result', 'async def f(uid, d):\n    await io_bound_result.io_bound(update_profile, uid, d)\n'),
    ('module_alias', 'async def f(uid):\n    await run.io_bound(sc.add_recent_item, uid, "990001")\n'),
    ('import_alias', 'async def f(uid):\n    await run.io_bound(lists_alias, uid)\n'),
    ('import_in_handler', 'async def f(uid):\n'
                          '    from web.supabase_client import get_recent_items\n'
                          '    await run.io_bound(get_recent_items, uid)\n'),
]


@pytest.mark.parametrize('trap_id,snippet', SEEDED_VIOLATIONS, ids=[t for t, _ in SEEDED_VIOLATIONS])
def test_seeded_offload_without_client_is_flagged(trap_id, snippet):
    found, _ = _offload_violations(_SEEDED_HEADER + snippet, _user_scoped_helpers())
    assert len(found) == 1, f'{trap_id}: the guard missed a seeded violation ({found})'


NEGATIVE_CONTROLS = [
    ('bare_with_client', 'async def f(uid, c):\n    await run.io_bound(get_user_lists, uid, client=c)\n'),
    ('lambda_with_client', 'async def f(uid, c):\n    await run.io_bound(lambda: get_user_lists(uid, client=c))\n'),
    ('partial_with_client', 'async def f(c):\n    await run.io_bound(partial(get_list_items, 7, client=c))\n'),
    ('bounded_with_client', 'async def f(sem, c):\n    await bounded_io_bound(sem, get_list_items, 7, client=c)\n'),
    ('public_reader', 'async def f(uid):\n    await run.io_bound(get_profile, uid)\n'),
    ('not_imported', 'def get_comments(x):\n    return x\n'
                     'async def f():\n    await run.io_bound(get_comments, 1)\n'),
]


@pytest.mark.parametrize('ctrl_id,snippet', NEGATIVE_CONTROLS, ids=[c for c, _ in NEGATIVE_CONTROLS])
def test_offload_with_client_or_public_helper_is_not_flagged(ctrl_id, snippet):
    found, _ = _offload_violations(_SEEDED_HEADER + snippet, _user_scoped_helpers())
    assert found == [], f'{ctrl_id}: false positive {found}'
