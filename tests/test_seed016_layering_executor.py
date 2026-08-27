# -*- coding: utf-8 -*-
"""SEED-016 -- shared/ -> web/ layering decoupling + bounded browse executor.

Two findings, two test groups:

#3 (layering): shared/browse_service.py and shared/parallels_service.py must not
   import from web/ at runtime. Proven by
   (a) a SOURCE guard (AST) that no `from web.` / `import web.` appears outside
       a TYPE_CHECKING block, and
   (b) BEHAVIOR tests that call the shared functions with INJECTED FAKES while
       web.state / web.services are POISONED in sys.modules -- so any latent
       runtime reach into web/ would raise. A plain "imports without web" test
       is too weak here because the imports were already late; poisoning proves
       the call path itself is decoupled.

#29 (bounded executor): browse enrichment fan-out must run on a module-level
   NAMED, size-bounded executor with a source-concurrency cap, and one slow
   source must time out WITHOUT starving the others.
"""

from __future__ import annotations

import ast
import asyncio
import os
import pathlib
import sys
import threading
import time

import pytest

import shared.browse_service as bs
import shared.parallels_service as ps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _web_imports_outside_type_checking(module_path: pathlib.Path) -> list[str]:
    """Return offending `web.*` import statements that are NOT under a
    `if TYPE_CHECKING:` block. Empty list == clean."""
    src = module_path.read_text(encoding='utf-8')
    tree = ast.parse(src)

    # Collect line ranges of all `if TYPE_CHECKING:` bodies so we can exempt
    # imports inside them (those never execute at runtime).
    type_checking_ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            test = node.test
            is_tc = (
                (isinstance(test, ast.Name) and test.id == 'TYPE_CHECKING')
                or (isinstance(test, ast.Attribute) and test.attr == 'TYPE_CHECKING')
            )
            if is_tc:
                start = node.body[0].lineno
                end = getattr(node, 'end_lineno', node.body[-1].lineno)
                type_checking_ranges.append((start, end))

    def _in_tc(lineno: int) -> bool:
        return any(s <= lineno <= e for s, e in type_checking_ranges)

    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ''
            if (mod == 'web' or mod.startswith('web.')) and not _in_tc(node.lineno):
                offenders.append(f'line {node.lineno}: from {mod} import ...')
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if (alias.name == 'web' or alias.name.startswith('web.')) and not _in_tc(node.lineno):
                    offenders.append(f'line {node.lineno}: import {alias.name}')
    return offenders


class _FakeBrowsePage:
    """Structural stand-in for web.services.BrowsePage (attribute access only)."""

    def __init__(self, sys_id='99001', p_num=3, fl_id='FL1'):
        self.sys_id = sys_id
        self.p_num = p_num
        self.fl_id = fl_id


class _FakeProvider:
    """Satisfies shared.browse_service.BrowsePageProvider structurally."""

    def __init__(self, page=None):
        self._page = page if page is not None else _FakeBrowsePage()
        self.calls: list[tuple] = []

    def get_browse_page(self, sys_id, *, p_num=None, volume_ie=None):
        self.calls.append(('get_browse_page', sys_id, p_num, volume_ie))
        return self._page

    def get_browse_page_by_fl(self, fl_id, *, sys_id=None):
        self.calls.append(('get_browse_page_by_fl', fl_id, sys_id))
        return self._page


class _FakeSearcher:
    def __init__(self, result):
        self._result = result
        self.calls = 0

    def search_composition_logic(self, *args, **kwargs):
        self.calls += 1
        return self._result


class _FakeMeta:
    def parse_full_id_components(self, uid_or_header):
        # 'h_<sys>_IE..' -> sys_id second token; else stable id.
        if uid_or_header and uid_or_header.startswith('h_'):
            parts = uid_or_header.split('_')
            return {'sys_id': parts[1] if len(parts) > 1 else 'unknown'}
        return {'sys_id': 'sysA'}


class _PoisonWeb:
    """Context manager: replace web.state / web.services in sys.modules with
    objects that raise on ANY attribute access, proving the code under test
    never reaches into web/ at runtime."""

    _TARGETS = ('web.state', 'web.services')

    def __init__(self):
        self._saved: dict[str, object] = {}

    def __enter__(self):
        class _Boom:
            def __getattr__(self, name):
                raise AssertionError(
                    f'shared/ reached into web.* at runtime (attr {name!r}) '
                    '-- SEED-016 #3 layering regression'
                )

        for name in self._TARGETS:
            self._saved[name] = sys.modules.get(name)
            sys.modules[name] = _Boom()  # type: ignore[assignment]
        return self

    def __exit__(self, *exc):
        for name, mod in self._saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod
        return False


@pytest.fixture(autouse=True)
def _reset_browse_executor():
    """Each asyncio.run() builds a fresh loop; drop the loop-bound semaphore +
    pool before/after so a stale loop binding is never reused."""
    bs.reset_browse_executor_state()
    yield
    bs.reset_browse_executor_state()


# ---------------------------------------------------------------------------
# #3 -- SOURCE guard (AST)
# ---------------------------------------------------------------------------

def test_browse_service_has_no_runtime_web_import():
    offenders = _web_imports_outside_type_checking(
        _REPO_ROOT / 'shared' / 'browse_service.py'
    )
    assert offenders == [], (
        'shared/browse_service.py imports web.* outside TYPE_CHECKING '
        f'(SEED-016 #3 regression): {offenders!r}'
    )


@pytest.mark.parametrize(
    'module_path',
    sorted((_REPO_ROOT / 'shared').glob('*.py'), key=lambda p: p.name),
    ids=lambda p: p.name,
)
def test_no_shared_module_has_a_runtime_web_import(module_path):
    """The whole of `shared/`, not a hand-maintained list of two.

    The two named guards below came first and stay, because they record WHY
    this layering exists. But a list of names only protects the modules
    somebody remembered to add: `shared/passage_witness_source.py` was moved
    out of `web/pages/parallels.py` precisely so the desktop could import it,
    and it would have landed here with no coverage at all. Every module in
    `shared/` was already clean when this sweep was written, so it costs
    nothing today and catches the next one for free.
    """
    offenders = _web_imports_outside_type_checking(module_path)
    assert offenders == [], (
        f'shared/{module_path.name} imports web.* outside TYPE_CHECKING '
        f'-- shared/ must not depend on the web surface: {offenders!r}'
    )


def test_parallels_service_has_no_runtime_web_import():
    offenders = _web_imports_outside_type_checking(
        _REPO_ROOT / 'shared' / 'parallels_service.py'
    )
    assert offenders == [], (
        'shared/parallels_service.py imports web.* outside TYPE_CHECKING '
        f'(SEED-016 #3 regression): {offenders!r}'
    )


# ---------------------------------------------------------------------------
# #3 -- BEHAVIOR: call with injected fakes while web.* is poisoned
# ---------------------------------------------------------------------------

def test_browse_bundle_runs_with_web_poisoned(monkeypatch):
    """fetch_browse_bundle resolves the page through the INJECTED provider while
    web.state / web.services are poisoned -- proving real decoupling."""
    # Keep sidecars silent so the test is offline + fast.
    monkeypatch.setattr(bs, '_pgp_sync', lambda *a, **k: None)
    monkeypatch.setattr(bs, '_fjms_sync', lambda *a, **k: None)
    monkeypatch.setattr(bs, '_nli_sync', lambda *a, **k: None)

    provider = _FakeProvider()

    async def _run():
        with _PoisonWeb():
            bundle, warnings = await bs.fetch_browse_bundle(
                service=provider, sys_id='99001', p_num=3,
            )
        return bundle, warnings

    bundle, warnings = asyncio.run(_run())
    assert bundle.page is provider._page
    assert provider.calls and provider.calls[0][0] == 'get_browse_page'
    assert warnings == []


def test_browse_bundle_fl_id_routes_to_by_fl(monkeypatch):
    monkeypatch.setattr(bs, '_pgp_sync', lambda *a, **k: None)
    monkeypatch.setattr(bs, '_fjms_sync', lambda *a, **k: None)
    monkeypatch.setattr(bs, '_nli_sync', lambda *a, **k: None)
    provider = _FakeProvider()

    async def _run():
        with _PoisonWeb():
            return await bs.fetch_browse_bundle(
                service=provider, sys_id='99001', fl_id='FLZ',
            )

    bundle, _ = asyncio.run(_run())
    assert provider.calls[0][0] == 'get_browse_page_by_fl'
    assert provider.calls[0][1] == 'FLZ'


def test_parallels_runs_with_web_poisoned():
    """fetch_parallels_results uses INJECTED searcher + meta_mgr while web.* is
    poisoned -- proving real decoupling."""
    main_rows = [
        {'uid': 'IE1_P1_FL1', 'raw_header': 'h_sysA_IE1_P1_FL1', 'score': 5.0,
         'final_score': 5.0},
    ]
    searcher = _FakeSearcher({'main': main_rows, 'filtered': [], 'boundary_stats': None})
    meta = _FakeMeta()

    async def _run():
        with _PoisonWeb():
            return await ps.fetch_parallels_results(
                searcher=searcher, meta_mgr=meta,
                text='hello world', chunk_size=3, mode='exact',
            )

    bundle = asyncio.run(_run())
    assert searcher.calls == 1
    assert bundle.main_results == main_rows
    assert bundle.truncated_to_200 is False


def test_parallels_group_cap_with_injected_meta():
    """Group cap path consumes the injected meta_mgr (not web.state)."""
    rows = []
    for i in range(250):
        rows.append({
            'uid': f'IE{i}_P1_FL{i}',
            'raw_header': f'h_sys{i}_IE{i}_P1_FL{i}',
            'score': float(250 - i), 'final_score': float(250 - i),
        })
    searcher = _FakeSearcher({'main': rows, 'filtered': [], 'boundary_stats': None})
    meta = _FakeMeta()

    async def _run():
        with _PoisonWeb():
            return await ps.fetch_parallels_results(
                searcher=searcher, meta_mgr=meta,
                text='hello', chunk_size=3, mode='exact',
            )

    bundle = asyncio.run(_run())
    assert bundle.truncated_to_200 is True
    assert len(bundle.main_results) == ps.PARALLELS_GROUP_CAP


# ---------------------------------------------------------------------------
# #29 -- bounded NAMED executor
# ---------------------------------------------------------------------------

def test_browse_executor_is_named_and_bounded():
    ex = bs._get_browse_executor()
    assert ex is bs._get_browse_executor(), 'executor must be a process singleton'
    assert ex._max_workers == bs.BROWSE_EXECUTOR_MAX_WORKERS
    # Threads carry the descriptive prefix.
    assert ex._thread_name_prefix == 'browse-enrich'
    # NOT the event loop's default executor.
    assert bs.BROWSE_EXECUTOR_MAX_WORKERS >= 1


def test_browse_executor_workers_env_override(monkeypatch):
    """The pool size honors SEARCH_API_BROWSE_EXECUTOR_WORKERS at import time.

    We can't re-import cheaply mid-suite, so assert the parsing contract on a
    re-derived value instead (the module read os.environ at import)."""
    # Sanity: the constant matches the parse rule for the current env (or default 8).
    raw = os.environ.get('SEARCH_API_BROWSE_EXECUTOR_WORKERS')
    expected = max(1, int(raw)) if raw else 8
    assert bs.BROWSE_EXECUTOR_MAX_WORKERS == expected
    # The source-concurrency cap is strictly below the pool (headroom invariant)
    # unless the pool is size 1.
    if bs.BROWSE_EXECUTOR_MAX_WORKERS > 1:
        assert bs.BROWSE_SOURCE_CONCURRENCY < bs.BROWSE_EXECUTOR_MAX_WORKERS


def test_browse_source_concurrency_is_capped(monkeypatch):
    """At most BROWSE_SOURCE_CONCURRENCY sync fetches run at once across the
    fan-out. Instrument the sync helpers to record peak concurrency."""
    peak = 0
    current = 0
    lock = threading.Lock()
    barrier_release = threading.Event()

    def _make_tracked(_name):
        def _fn(*a, **k):
            nonlocal peak, current
            with lock:
                current += 1
                peak = max(peak, current)
            # Hold long enough that all dispatched sources overlap.
            barrier_release.wait(timeout=2.0)
            with lock:
                current -= 1
            return None
        return _fn

    monkeypatch.setattr(bs, '_pgp_sync', _make_tracked('pgp'))
    monkeypatch.setattr(bs, '_fjms_sync', _make_tracked('fjms'))
    monkeypatch.setattr(bs, '_nli_sync', _make_tracked('nli'))
    # Force the cap down to 1 for a crisp assertion.
    monkeypatch.setattr(bs, 'BROWSE_SOURCE_CONCURRENCY', 1)
    # Generous per-source timeout so the cap (not the timeout) governs.
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '5.0')

    provider = _FakeProvider()

    async def _run():
        # Release the barrier shortly after dispatch so serialized sources finish.
        async def _releaser():
            await asyncio.sleep(0.3)
            barrier_release.set()
        rel = asyncio.ensure_future(_releaser())
        bundle, warnings = await bs.fetch_browse_bundle(
            service=provider, sys_id='99001', p_num=3,
        )
        await rel
        return bundle, warnings

    asyncio.run(_run())
    assert peak == 1, f'source concurrency cap=1 violated; peak was {peak}'


def test_browse_slow_source_times_out_without_starving_others(monkeypatch):
    """One slow source hits enrichment_timeout; the fast sources still resolve.

    Cap is the default (>1) so the three sources fan out concurrently."""
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '0.2')

    def _slow_pgp(*a, **k):
        time.sleep(1.5)  # exceeds the 0.2s timeout
        return {'page_section_text': 'late'}

    def _fast_fjms(*a, **k):
        return {'source_names': ['CUL'], 'has_measurements': False,
                'has_visual_suggestions': False}

    def _fast_nli(*a, **k):
        return {'physical_metadata': None, 'folio': None}

    monkeypatch.setattr(bs, '_pgp_sync', _slow_pgp)
    monkeypatch.setattr(bs, '_fjms_sync', _fast_fjms)
    monkeypatch.setattr(bs, '_nli_sync', _fast_nli)

    provider = _FakeProvider()

    async def _run():
        t0 = time.monotonic()
        bundle, warnings = await bs.fetch_browse_bundle(
            service=provider, sys_id='99001', p_num=3,
        )
        return bundle, warnings, time.monotonic() - t0

    bundle, warnings, elapsed = asyncio.run(_run())

    # Fast sources resolved.
    assert bundle.fjms is not None and bundle.fjms['source_names'] == ['CUL']
    assert bundle.nli is not None
    # Slow source timed out -> null slot + structured warning.
    assert bundle.pgp is None
    assert any(
        w.get('code') == 'enrichment_timeout' and w.get('source') == 'pgp'
        for w in warnings
    ), f'expected enrichment_timeout/pgp; got {warnings!r}'
    # gather returned at ~the timeout, NOT at the slow source's 1.5s -- the fast
    # sources were not starved by the slow one.
    assert elapsed < 1.0, (
        f'fan-out blocked on the slow source ({elapsed:.2f}s) -- starvation'
    )


def test_timed_out_source_keeps_slot_and_does_not_hang_contenders(monkeypatch):
    """SEED-016 #29 (in-session Codex REQUEST-CHANGES) + GitHub Codex PR #297 P1.

    Two properties that must coexist with BROWSE_SOURCE_CONCURRENCY = 1:

      (A) Anti-early-re-admit: a source whose coroutine TIMES OUT keeps its slot
          until the underlying blocking function ACTUALLY returns -- not merely
          until asyncio.wait_for cancels the await. So a contender must NOT begin
          executing its blocking body while the timed-out holder is still running.

      (B) No-hang (PR #297 P1 FIX): a contender waiting for that held slot is
          bounded -- once it cannot get a slot within the per-source timeout it is
          SKIPPED with an enrichment_timeout warning, rather than blocking outside
          any timeout until the hung thread returns. Browse stays responsive even
          when every slot is occupied by a slow/hung sidecar call.

    The slow holder blocks past its per-source timeout; the contender therefore
    both (A) never runs its body while the slot is held AND (B) returns promptly
    with an enrichment_timeout instead of waiting out the 5s hold.
    """
    # Cap concurrency to a single slot and rebuild the semaphore on this loop.
    monkeypatch.setattr(bs, 'BROWSE_SOURCE_CONCURRENCY', 1)
    bs.reset_browse_executor_state()
    # Slow source's per-source timeout is short; the slow body runs much longer.
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '0.2')

    slow_started = threading.Event()
    slow_may_finish = threading.Event()
    second_started = threading.Event()

    def _slow_pgp(*a, **k):
        slow_started.set()
        # Block well past the 0.2s coroutine timeout; only finish on command.
        slow_may_finish.wait(timeout=5.0)
        return {'page_section_text': 'late'}

    def _second_fjms(*a, **k):
        # Should NEVER run: the contender times out waiting for the held slot.
        second_started.set()
        return {'source_names': ['CUL'], 'has_measurements': False,
                'has_visual_suggestions': False}

    def _noop_nli(*a, **k):
        return {'physical_metadata': None, 'folio': None}

    monkeypatch.setattr(bs, '_pgp_sync', _slow_pgp)
    monkeypatch.setattr(bs, '_fjms_sync', _second_fjms)
    monkeypatch.setattr(bs, '_nli_sync', _noop_nli)

    provider = _FakeProvider()

    async def _run():
        task = asyncio.ensure_future(
            bs.fetch_browse_bundle(service=provider, sys_id='99001', p_num=3)
        )
        # Wait until the slow source is running (it holds the only slot).
        for _ in range(200):
            if slow_started.is_set():
                break
            await asyncio.sleep(0.01)
        assert slow_started.is_set(), 'slow source never started'

        # (B) The whole gather must resolve PROMPTLY (~per-source timeout), NOT
        # wait out the slow holder's 5s block. If the contender's slot-acquire
        # were unbounded (the PR #297 P1 bug) this await would hang until we set
        # slow_may_finish below.
        bundle, warnings = await asyncio.wait_for(task, timeout=2.0)

        # (A) The contender never executed its blocking body -- the slot was held
        # by the timed-out holder for the holder's true lifetime, never re-admitted
        # early on coroutine timeout.
        assert not second_started.is_set(), (
            'contender ran its blocking body while the timed-out holder still held '
            'the slot -- the slot was freed on coroutine timeout instead of on real '
            'completion (SEED-016 #29 regression)'
        )
        return bundle, warnings

    try:
        bundle, warnings = asyncio.run(_run())
    finally:
        # Release the still-blocked holder thread so it does not linger.
        slow_may_finish.set()

    # The slow holder timed out on its WORK; the contender timed out on its SLOT
    # ACQUIRE -- both surface enrichment_timeout, neither hangs browse.
    assert bundle.pgp is None and bundle.fjms is None
    timed_out = {w.get('source') for w in warnings if w.get('code') == 'enrichment_timeout'}
    assert {'pgp', 'fjms'} <= timed_out, (
        f'expected enrichment_timeout for pgp (work) AND fjms (slot wait); got {warnings!r}'
    )


def test_slot_released_on_true_completion_lets_contender_proceed(monkeypatch):
    """Companion to the no-hang test: when the holder finishes WITHIN the
    contender's slot-acquire budget, the slot is released (on true completion) and
    the contender proceeds -- and it only runs AFTER the holder's blocking call
    actually returned (positive proof of release-on-completion, not on timeout)."""
    monkeypatch.setattr(bs, 'BROWSE_SOURCE_CONCURRENCY', 1)
    bs.reset_browse_executor_state()
    # Generous per-source timeout so the slot wait (not the timeout) governs; the
    # holder finishes quickly, well inside that budget.
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '2.0')

    holder_returned = threading.Event()
    observed = {'contender_started_before_holder_returned': None}

    def _holder_pgp(*a, **k):
        time.sleep(0.3)  # finishes well within the 2.0s contender budget
        holder_returned.set()
        return {'page_section_text': 'on-time'}

    def _contender_fjms(*a, **k):
        observed['contender_started_before_holder_returned'] = not holder_returned.is_set()
        return {'source_names': ['CUL'], 'has_measurements': False,
                'has_visual_suggestions': False}

    def _noop_nli(*a, **k):
        return {'physical_metadata': None, 'folio': None}

    monkeypatch.setattr(bs, '_pgp_sync', _holder_pgp)
    monkeypatch.setattr(bs, '_fjms_sync', _contender_fjms)
    monkeypatch.setattr(bs, '_nli_sync', _noop_nli)

    provider = _FakeProvider()

    async def _run():
        return await asyncio.wait_for(
            bs.fetch_browse_bundle(service=provider, sys_id='99001', p_num=3),
            timeout=5.0,
        )

    bundle, warnings = asyncio.run(_run())

    # Both sources resolved (no timeout) and the contender ran only AFTER the
    # holder's blocking call returned -- the slot was freed on true completion.
    assert bundle.pgp is not None and bundle.pgp['page_section_text'] == 'on-time'
    assert bundle.fjms is not None and bundle.fjms['source_names'] == ['CUL']
    assert observed['contender_started_before_holder_returned'] is False, (
        'contender ran before the holder blocking call returned'
    )


def test_browse_executor_workers_env_parse_does_not_raise_on_garbage():
    """_read_int_env falls back to the default on malformed input instead of
    raising at import (secondary nit)."""
    assert bs._read_int_env('SEARCH_API_BROWSE_EXECUTOR_WORKERS_NONEXISTENT', 8) == 8
    import os as _os
    _os.environ['_BS_TEST_GARBAGE_INT'] = 'not-a-number'
    try:
        assert bs._read_int_env('_BS_TEST_GARBAGE_INT', 8) == 8
        _os.environ['_BS_TEST_GARBAGE_INT'] = '3'
        assert bs._read_int_env('_BS_TEST_GARBAGE_INT', 8) == 3
        _os.environ['_BS_TEST_GARBAGE_INT'] = '0'
        # Clamped to the minimum (1).
        assert bs._read_int_env('_BS_TEST_GARBAGE_INT', 8) == 1
    finally:
        _os.environ.pop('_BS_TEST_GARBAGE_INT', None)


def test_shutdown_browse_executor_is_idempotent_and_rebuilds():
    ex1 = bs._get_browse_executor()
    bs.shutdown_browse_executor(wait=True)
    bs.shutdown_browse_executor(wait=True)  # idempotent, no raise
    ex2 = bs._get_browse_executor()
    assert ex2 is not ex1, 'a fresh pool must be built after shutdown'
