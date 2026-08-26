# -*- coding: utf-8 -*-
"""Phase 146 Tasks 3+4: the desktop passage-index lifecycle state machine.

Every test drives the module's PUBLIC functions directly -- no QApplication,
no Qt import anywhere in this file. `desktop/passage_lifecycle.py`'s own
docstring explains why: the whole point of factoring it as pure functions
plus injected filesystem seams is that the state machine is provable without
a GUI event loop.
"""
from __future__ import annotations

import inspect
import itertools
import logging
import os
import shutil
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.passage_lifecycle as pl  # noqa: E402
import shared.passage_builder as passage_builder  # noqa: E402
from shared.passage_index import BuildCancelled, open_index  # noqa: E402

ALEF = 0x05D0
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _reset_module_globals():
    """`_state` and `_freshness` are module globals -- every test starts and
    ends with a clean slate so results never leak across tests."""
    pl.close_passage_state()
    yield
    pl.close_passage_state()


# ---------------------------------------------------------------------------
# Fixtures: a tiny synthetic corpus, on disk in the transcriptions format
# `shared/passage_corpus.py::iter_records` expects.
# ---------------------------------------------------------------------------

def _letters(seq) -> str:
    return ''.join(chr(ALEF + (v % 22)) for v in seq)


def synthetic_records(n_records: int = 20, base_len: int = 200) -> list:
    out = []
    for r in range(n_records):
        text = _letters(((r + 1) * (i + 1) for i in range(base_len)))
        out.append((f'rec{r:04d}', text))
    return out


def write_corpus(path: str, records: list) -> None:
    with open(path, 'w', encoding='utf-8') as fh:
        for rid, text in records:
            fh.write(f'==> {rid} <==\n{text}\n')


def _cancel_after(n):
    """False for the first n-1 calls, True from the n-th call on."""
    calls = {'i': 0}

    def cancel():
        calls['i'] += 1
        return calls['i'] >= n

    cancel.calls = calls
    return cancel


class _Corpus:
    """A ready-to-use (root, corpus_path, records) triple under tmp_path."""

    def __init__(self, tmp_path, n_records=20):
        self.root = str(tmp_path / 'passage_index')
        self.corpus_path = str(tmp_path / 'corpus.txt')
        self.records = synthetic_records(n_records)
        write_corpus(self.corpus_path, self.records)

    @property
    def source_paths(self):
        return [self.corpus_path]

    @property
    def live_dir(self):
        return os.path.join(self.root, pl.LIVE_DIRNAME)

    def build(self, **kwargs):
        """Mirrors the real caller contract: `run_build_and_swap` never
        installs on its own (see its docstring), so a test that wants a
        SECOND build to be able to rename `live` away must install -- and
        therefore later release, via `close_passage_state` -- whatever the
        first one returned. Skipping this is exactly the bug that produced
        the very first version of these rebuild tests: an un-installed
        `res.index` kept a live memmap open for the rest of the test
        function, and the next rebuild's rename blocked on it."""
        kwargs.setdefault('release_live_state', pl.close_passage_state)
        res = pl.run_build_and_swap(
            self.root, self.records, self.source_paths, self.corpus_path,
            partitions=2, **kwargs)
        if res.index is not None:
            pl.install_passage_state(
                pl.PassageState(index=res.index, live_dir=res.live_dir))
        return res


def _join_background_threads(result, extra_seconds=0.5):
    """Background generation/quarantine deletes are fire-and-forget; give
    them a moment before a test asserts on the directory listing."""
    time.sleep(extra_seconds)


# ---------------------------------------------------------------------------
# 0. install_passage_state must release the state it REPLACES, not just the
#    one close_passage_state releases at teardown.
# ---------------------------------------------------------------------------

def test_install_passage_state_releases_the_state_it_replaces(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()  # undo the helper's own install; start clean

    # Two INDEPENDENT copies of the same artifact, each opened separately --
    # isolates "did the OUTGOING state's handles get released" from "is the
    # new state's own directory still open", which a single shared directory
    # could not distinguish.
    copy1 = str(tmp_path / 'copy1')
    copy2 = str(tmp_path / 'copy2')
    shutil.copytree(c.live_dir, copy1)
    shutil.copytree(c.live_dir, copy2)

    idx1 = open_index(copy1)
    assert idx1 is not None
    pl.install_passage_state(pl.PassageState(index=idx1, live_dir=copy1))

    idx2 = open_index(copy2)
    assert idx2 is not None
    pl.install_passage_state(pl.PassageState(index=idx2, live_dir=copy2))

    # idx1 is no longer installed -- its handles must be gone even though
    # idx2 (the currently-installed state) is still open on copy2.
    renamed = copy1 + '-renamed'
    os.rename(copy1, renamed)  # raises PermissionError if idx1 leaked
    shutil.rmtree(renamed)

    pl.close_passage_state()  # release idx2 too


def test_install_passage_state_same_object_is_a_noop(tmp_path):
    """Installing the SAME state object it already holds must not release
    its own handles out from under it."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    state = pl._state
    pl.install_passage_state(state)
    assert open_index(c.live_dir) is not None, (
        're-installing the same state must not close its own handles')


def test_install_passage_state_from_none_releases_nothing():
    """No outgoing state at all -- must not raise."""
    pl.close_passage_state()
    pl.install_passage_state(None)  # no-op, must not raise
    assert not pl.passage_available()


# ---------------------------------------------------------------------------
# 0b. load_passage_state must not downgrade a valid `current/` on one flake.
# ---------------------------------------------------------------------------

def test_load_passage_state_retries_a_transient_open_failure(
        tmp_path, monkeypatch):
    """`open_index` swallows a transient access error into None exactly like
    a genuinely missing index -- `load_passage_state` calling it directly,
    instead of going through `_open_with_retry` like every other reload
    check in this module, means one flake at startup reports "no index" for
    a `current/` that is perfectly valid, and the caller offers to rebuild a
    multi-GB artifact that never needed rebuilding."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()  # undo the helper's own install; start clean

    stub, calls = _flaky_open_index(c.live_dir, fail_times=1)
    monkeypatch.setattr(pl, 'open_index', stub)

    state = pl.load_passage_state(c.root)

    assert state is not None, (
        'a single transient open failure must not be reported as a missing '
        'index')
    assert state.index.n_records == len(c.records)
    assert calls['n'] >= 2
    pl.install_passage_state(state)  # let the autouse fixture release it


# ---------------------------------------------------------------------------
# 0c. Reader leases are now CALL-SCOPED: a search still touching the index
#     must block a force-close, not lose the race to one, but what proves
#     "still touching" is a search actually IN FLIGHT inside
#     `search_composition_logic`, not an object a caller is holding onto.
# ---------------------------------------------------------------------------

class _NullTextFetcher:
    def get_full_text_by_header(self, full_header):
        return None


def _shrink_lease_window(monkeypatch, timeout=0.2, poll=0.02):
    """Every negative (never-drains) test needs a short window, or it pays
    the full production timeout; every positive (drains-in-time) test needs
    the window wide enough that its background release always lands inside
    it. Centralised so both kinds of test tune the same two knobs the same
    way."""
    monkeypatch.setattr(pl, 'LEASE_DRAIN_TIMEOUT_SECONDS', timeout)
    monkeypatch.setattr(pl, 'LEASE_DRAIN_POLL_SECONDS', poll)


def _patch_blocking_search(monkeypatch, entered_event, release_event):
    """Patches `PassageSearcher.search_composition_logic` (the REAL search,
    not the adapter) to signal `entered_event` the moment it starts -- proof
    the lease is now held -- then block until `release_event` is set. Models
    a query genuinely still touching the index, the exact window the lease
    exists to protect; the adapter's own acquire/release wrapping is left
    completely real."""
    import shared.passage_parallels as passage_parallels

    def _blocked(self, *a, **k):
        entered_event.set()
        assert release_event.wait(timeout=5), 'test setup stalled'
        return {'main': [], 'filtered': []}

    monkeypatch.setattr(passage_parallels.PassageSearcher,
                        'search_composition_logic', _blocked)


def test_close_refuses_while_a_search_is_in_flight_then_succeeds_after_it_returns(
        tmp_path, monkeypatch):
    _shrink_lease_window(monkeypatch)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    entered = threading.Event()
    release = threading.Event()
    _patch_blocking_search(monkeypatch, entered, release)

    adapter = pl.get_passage_searcher(_NullTextFetcher())
    outcome = {}
    t = threading.Thread(
        target=lambda: outcome.__setitem__(
            'result', adapter.search_composition_logic(c.records[0][1][:10])))
    t.start()
    assert entered.wait(timeout=5), 'search never reached the blocking point'

    # "mid-query": the call above is still inside search_composition_logic,
    # holding the lease it acquired at the top of that call.
    assert pl.close_passage_state() is False, (
        'a search still touching the index must block a force-close, not '
        'lose the race to it')
    assert pl.passage_available(), (
        'a refused close must leave the state installed and usable')

    # Let the search return -- its `finally` releases the lease.
    release.set()
    t.join(timeout=5)
    assert outcome['result'] == {'main': [], 'filtered': []}

    # A close AFTER the search has genuinely finished must succeed -- the
    # lease was released inside that call, not left dangling on some object
    # nobody closed.
    assert pl.close_passage_state() is True
    assert not pl.passage_available()


def test_close_succeeds_after_a_search_completes_normally(tmp_path):
    """The un-blocked, happy-path twin of the test above: no threads, no
    monkeypatching -- just proof that an ordinary completed search leaves
    nothing outstanding for close to trip over."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    result = pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(
        c.records[0][1][:10])
    assert isinstance(result, dict)

    assert pl.close_passage_state() is True
    assert not pl.passage_available()


def test_search_finishing_inside_the_wait_window_lets_the_same_close_through(
        tmp_path, monkeypatch):
    """A search that finishes WHILE `close_passage_state` is still polling
    must let THAT SAME call succeed -- proving the window is a real wait,
    not an instant refusal dressed up as one."""
    _shrink_lease_window(monkeypatch, timeout=2.0, poll=0.02)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    entered = threading.Event()
    release = threading.Event()
    _patch_blocking_search(monkeypatch, entered, release)

    adapter = pl.get_passage_searcher(_NullTextFetcher())
    t = threading.Thread(
        target=lambda: adapter.search_composition_logic(c.records[0][1][:10]))
    t.start()
    assert entered.wait(timeout=5), 'search never reached the blocking point'

    def _release_soon():
        time.sleep(0.15)
        release.set()

    releaser = threading.Thread(target=_release_soon)
    releaser.start()
    try:
        assert pl.close_passage_state() is True, (
            'a search that finishes inside the wait window must let the '
            'close through on the same call, not force the caller to retry')
    finally:
        releaser.join()
        t.join(timeout=5)


def test_run_build_and_swap_refuses_while_a_search_is_in_flight_and_cleans_staging(
        tmp_path, monkeypatch):
    _shrink_lease_window(monkeypatch)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    staging = os.path.join(c.root, pl.STAGING_DIRNAME)

    entered = threading.Event()
    release = threading.Event()
    _patch_blocking_search(monkeypatch, entered, release)

    adapter = pl.get_passage_searcher(_NullTextFetcher())
    t = threading.Thread(
        target=lambda: adapter.search_composition_logic(c.records[0][1][:10]))
    t.start()
    assert entered.wait(timeout=5), 'search never reached the blocking point'

    try:
        res = pl.run_build_and_swap(
            c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
            release_live_state=pl.close_passage_state)

        assert res.status == 'readers_active'
        assert pl.passage_available(), (
            'a refused swap must leave the old, still-live index installed')
        assert not os.path.isdir(staging), (
            'a swap abandoned before promotion must still clean its staging '
            '-- an unswapped build is a build this run will never use')
    finally:
        release.set()
        t.join(timeout=5)

    # Search released -- a fresh build+swap now proceeds normally.
    res2 = pl.run_build_and_swap(
        c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
        release_live_state=pl.close_passage_state)
    assert res2.status == 'installed'
    pl.install_passage_state(
        pl.PassageState(index=res2.index, live_dir=res2.live_dir))


def test_install_passage_state_refuses_to_replace_a_state_with_a_search_in_flight(
        tmp_path, monkeypatch):
    _shrink_lease_window(monkeypatch)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    outgoing_index = pl._state.index

    other = str(tmp_path / 'other')
    shutil.copytree(c.live_dir, other)
    idx2 = open_index(other)
    assert idx2 is not None

    entered = threading.Event()
    release = threading.Event()
    _patch_blocking_search(monkeypatch, entered, release)

    adapter = pl.get_passage_searcher(_NullTextFetcher())
    t = threading.Thread(
        target=lambda: adapter.search_composition_logic(c.records[0][1][:10]))
    t.start()
    assert entered.wait(timeout=5), 'search never reached the blocking point'

    try:
        assert pl.install_passage_state(
            pl.PassageState(index=idx2, live_dir=other)) is False
        assert pl._state.index is outgoing_index, (
            'a refused install must leave the ORIGINAL state installed, '
            'not the new one half-swapped in')
    finally:
        release.set()
        t.join(timeout=5)
        pl._release_index_handles(idx2)  # never installed -- release by hand

    assert pl.close_passage_state() is True


# ---------------------------------------------------------------------------
# 0d. G1: what `get_passage_searcher()` hands out holds no index and no
#     searcher, so there is nothing to escape with. Capturing the bound
#     method and calling it long after the call that produced it is exactly
#     as safe as calling it immediately -- each call is its own lease.
# ---------------------------------------------------------------------------

def test_captured_bound_method_is_safe_to_call_after_the_search_completes(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    # Captured OUTSIDE any `with` block -- there is no block to be inside of.
    bound = pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic
    first = bound(text)
    assert isinstance(first, dict)

    # The call that produced `bound` is long finished. Calling the SAME
    # captured method again must take a FRESH lease, not dereference
    # anything left over from the first call.
    second = bound(text)
    # `query_report['seconds']` is a real wall-clock measurement, so two
    # independently timed calls differ at the rounding under load; every
    # other field must match exactly.
    first['query_report'].pop('seconds', None)
    second['query_report'].pop('seconds', None)
    assert second == first

    assert pl.close_passage_state() is True


def test_captured_bound_method_raises_cleanly_once_the_index_is_gone(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    bound = pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic
    pl.close_passage_state()

    with pytest.raises(pl.PassageSearchUnavailableError):
        bound(c.records[0][1][:30])


def test_captured_bound_method_picks_up_a_replaced_index_later(tmp_path):
    """The bound method resolves the CURRENT state at call time -- a rebuild
    that swaps in an entirely different `PassageIndex` object between two
    calls must not leave the second call reaching for the first one."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    first_index = pl._state.index
    text = c.records[0][1][:30]

    bound = pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic
    bound(text)

    res2 = c.build()  # rebuild + swap -- installs a DIFFERENT index object
    assert res2.status == 'installed'
    assert pl._state.index is not first_index

    result = bound(text)  # same captured method, called again
    assert isinstance(result, dict)


def test_no_adapter_attribute_reaches_the_index_or_searcher(tmp_path):
    from shared.passage_index import PassageIndex
    from shared.passage_parallels import PassageSearcher

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    adapter = pl.get_passage_searcher(_NullTextFetcher())

    with pytest.raises(AttributeError):
        adapter.__dict__  # __slots__: no instance dict to smuggle a reference into

    for name in dir(adapter):
        if name.startswith('__'):
            continue
        value = getattr(adapter, name)
        assert not isinstance(value, PassageIndex), (
            f'adapter.{name} reaches a PassageIndex')
        assert not isinstance(value, PassageSearcher), (
            f'adapter.{name} reaches a PassageSearcher')


def test_adapter_search_result_matches_calling_the_searcher_directly(
        tmp_path):
    """The adapter must not change WHAT is searched or HOW -- only who owns
    the index reference while it happens."""
    from shared.passage_parallels import PassageSearcher

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:40]

    via_adapter = pl.get_passage_searcher(
        _NullTextFetcher()).search_composition_logic(text)

    direct_searcher = PassageSearcher(
        index=pl._state.index, text_fetcher=_NullTextFetcher(),
        policy=pl.compose(pl._DEFAULT_WIDTH, pl.DEFAULT_LENGTH, pl.DEFAULT_DEPTH))
    direct = direct_searcher.search_composition_logic(text)

    # `query_report['seconds']` is a real wall-clock measurement taken
    # independently by each call -- it will never match bit-for-bit between
    # two separate runs and asserting on it would be timing noise, not a
    # behavioural difference. Every other field, including every OTHER key
    # in `query_report` itself, must match exactly.
    via_adapter['query_report'].pop('seconds', None)
    direct['query_report'].pop('seconds', None)
    assert via_adapter == direct


# ---------------------------------------------------------------------------
# 0e. G2: acquisition and teardown share ONE mutual-exclusion point -- a
#     search requested while a close/swap is mid-teardown must be refused,
#     never handed a soon-to-be-closed index. Proven with REAL threads, not
#     by inspecting the lock.
# ---------------------------------------------------------------------------

def test_close_refuses_a_search_requested_mid_teardown_real_threads(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    real_release_handles = pl._release_index_handles
    entered_teardown = threading.Event()
    let_teardown_finish = threading.Event()

    def slow_release_handles(idx):
        # Mid-teardown: leases have already drained to zero and
        # `close_passage_state` has committed to tearing this index down,
        # but the mappings are not closed yet -- exactly the window a new
        # acquisition must never be able to slip into.
        entered_teardown.set()
        assert let_teardown_finish.wait(timeout=5), 'test setup stalled'
        real_release_handles(idx)

    monkeypatch.setattr(pl, '_release_index_handles', slow_release_handles)

    outcome = {}

    def closer():
        outcome['close'] = pl.close_passage_state()

    def acquirer():
        assert entered_teardown.wait(timeout=5), 'close never reached teardown'
        try:
            pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(
                c.records[0][1][:10])
            outcome['acquire'] = 'succeeded'
        except pl.PassageSearchUnavailableError:
            outcome['acquire'] = 'refused'
        let_teardown_finish.set()

    t_close = threading.Thread(target=closer)
    t_acquire = threading.Thread(target=acquirer)
    t_close.start()
    t_acquire.start()
    t_close.join(timeout=10)
    t_acquire.join(timeout=10)

    assert outcome['close'] is True
    assert outcome['acquire'] == 'refused', (
        'a search requested while a close is mid-teardown must be refused '
        'outright, never handed an index whose mappings are being closed '
        'concurrently on another thread')


def test_a_refused_close_leaves_no_new_leases_flag_cleared_for_next_attempt(
        tmp_path, monkeypatch):
    _shrink_lease_window(monkeypatch)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    entered = threading.Event()
    release = threading.Event()
    _patch_blocking_search(monkeypatch, entered, release)

    adapter = pl.get_passage_searcher(_NullTextFetcher())
    t = threading.Thread(
        target=lambda: adapter.search_composition_logic(c.records[0][1][:10]))
    t.start()
    assert entered.wait(timeout=5), 'search never reached the blocking point'

    assert pl.close_passage_state() is False, (
        'refused: a search is still in flight')

    release.set()
    t.join(timeout=5)

    # The refused close must have restored the no-new-leases flag exactly
    # as it found it -- a fresh search right after must behave normally,
    # not be refused by a flag the failed close forgot to clear.
    result = pl.get_passage_searcher(
        _NullTextFetcher()).search_composition_logic(c.records[0][1][:10])
    assert isinstance(result, dict), (
        'a refused close must not leave the drain flag stuck on, refusing '
        'every later search')
    assert pl.close_passage_state() is True


class _PausingLock:
    """Wraps a real `threading.Lock` and, on the FIRST release only, blocks
    the releasing thread (after the real lock has already been let go, so
    other threads may freely take it) until `resume` is set. This is what
    lets the test park an acquiring thread at the exact instant it exits
    `_try_acquire_lease`'s critical section -- after whatever that section
    does, before the function returns -- regardless of how much or how
    little work that section actually contains. It never blocks
    `acquire()`, so it cannot itself deadlock anything: the underlying
    mutual exclusion is untouched, only the return from ONE release is
    delayed."""

    def __init__(self, real_lock, released_event, resume_event):
        self._real = real_lock
        self._released_event = released_event
        self._resume_event = resume_event
        self._fired = False

    def acquire(self, *args, **kwargs):
        return self._real.acquire(*args, **kwargs)

    def release(self, *args, **kwargs):
        self._real.release(*args, **kwargs)
        if not self._fired:
            self._fired = True
            self._released_event.set()
            if not self._resume_event.wait(timeout=5):
                raise AssertionError(
                    'test harness never resumed the paused acquirer thread '
                    'within the bounded wait -- synchronisation failure, '
                    'not the invariant under test')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


def test_search_state_check_and_reader_increment_are_atomic_real_threads(
        tmp_path, monkeypatch):
    """Forces the exact interleaving that a mutation surviving code review
    could open: the reader-count increment happening OUTSIDE the same
    critical section as the `_state`/`_draining` check inside
    `_try_acquire_lease`. `_try_acquire_lease` is the acquisition point in
    BOTH the old and the new design -- unchanged by the call-scoping
    redesign -- so `_PausingLock` still parks an acquiring thread at the
    instant it releases `_lease_lock`, exactly as before; only the CALLER of
    `_try_acquire_lease` is now `PassageSearchAdapter.search_composition_
    logic` instead of `get_passage_searcher` itself.

    With the increment INSIDE the critical section (current code), the
    count already reflects the in-flight acquisition when the close checks
    it, so the close must find leases outstanding and refuse. With the
    increment moved OUTSIDE (the mutation this test exists to kill), the
    close's check lands in the gap where the count is still zero, so the
    close wrongly declares the index drained, tears its mappings down, and
    THEN the acquirer resumes and runs its search over the now-closed
    index. The single assertion below (`close must have been refused`)
    fails cleanly on that outcome instead of silently letting the search
    proceed over a closed mapping.
    """
    # A short, EXPLICIT drain timeout on the real function object -- not the
    # `LEASE_DRAIN_TIMEOUT_SECONDS` module global, which `_wait_for_leases_
    # to_drain`'s default argument already captured at import time and a
    # module-attribute monkeypatch cannot reach after the fact.
    monkeypatch.setattr(pl._wait_for_leases_to_drain, '__defaults__', (0.3,))
    monkeypatch.setattr(pl, 'LEASE_DRAIN_POLL_SECONDS', 0.02)

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:10]

    original_lock = pl._lease_lock
    released = threading.Event()
    resume = threading.Event()
    pl._lease_lock = _PausingLock(original_lock, released, resume)

    outcome = {}
    adapter = pl.get_passage_searcher(_NullTextFetcher())

    def _acquire():
        try:
            outcome['search'] = adapter.search_composition_logic(text)
        except pl.PassageSearchUnavailableError:
            outcome['search'] = None

    acquirer = threading.Thread(target=_acquire)
    closer = None

    try:
        acquirer.start()
        assert released.wait(timeout=5), (
            'the acquiring thread never reached the lock-release point -- '
            'test synchronisation failure, not the invariant under test')

        # The acquirer is now parked immediately after its critical section
        # exited `_lease_lock` -- exactly the boundary the increment's
        # placement decides the meaning of. Race a real close against it.
        closer = threading.Thread(
            target=lambda: outcome.__setitem__(
                'close', pl.close_passage_state()))
        closer.start()
        closer.join(timeout=5)
        assert not closer.is_alive(), (
            'close_passage_state() did not return within the bounded '
            'drain window -- possible deadlock, not the invariant under '
            'test')

        resume.set()
        acquirer.join(timeout=5)
        assert not acquirer.is_alive(), (
            'the paused acquirer thread never resumed within the bounded '
            'wait -- test synchronisation failure')

        assert outcome.get('search') is not None, (
            'sanity check on the test itself: the acquisition\'s state '
            'check passed (state was installed, not draining) before the '
            'pause, so resuming it must produce a real result either way '
            '-- if this is None the harness above is not exercising the '
            'intended interleaving at all')
        assert outcome.get('close') is False, (
            'close_passage_state() succeeded while a concurrent search '
            'had already passed its state check and was paused before its '
            'reader-count increment -- the state check and the increment '
            'are no longer atomic (the increment likely moved outside '
            '`_lease_lock`), so the close observed zero outstanding '
            'readers and tore the index down while a lease for it was '
            'still being issued')
    finally:
        resume.set()
        for t in (acquirer, closer):
            if t is not None and t.is_alive():
                t.join(timeout=5)
        pl._lease_lock = original_lock
        pl._outstanding_leases = 0
        pl._draining = False
        pl.close_passage_state()  # no-op if already closed; else real cleanup


# ---------------------------------------------------------------------------
# 0f. Exception boundary: `search_composition_logic` must not let an
#     exception object carrying a frame from inside the lease cross its own
#     boundary -- see the module's PassageSearchAdapter docstring
#     ("EXCEPTION BOUNDARY"). Every test here proves this with a REAL built
#     index, never a mock, because the failure mode under test is a Windows
#     access violation on a real memmap, not a Python-level assertion.
# ---------------------------------------------------------------------------

def _collect_chain_locals(exc, seen_exc=None):
    """Every `f_locals` value reachable from `exc`'s own traceback
    (`tb_next` chain), PLUS the same walk repeated for `__context__` and
    `__cause__`, recursively. That is the full definition of "the exception
    chain" -- a caller doing `logging.exception(...)`, storing the object,
    or walking `__context__` by hand all reach exactly this set of frames
    and nothing more. Returns every local VALUE seen (not pre-filtered) so
    a caller can isinstance-check them itself."""
    if seen_exc is None:
        seen_exc = set()
    if exc is None or id(exc) in seen_exc:
        return []
    seen_exc.add(id(exc))
    values = []
    tb = exc.__traceback__
    while tb is not None:
        values.extend(tb.tb_frame.f_locals.values())
        tb = tb.tb_next
    values.extend(_collect_chain_locals(exc.__context__, seen_exc))
    values.extend(_collect_chain_locals(exc.__cause__, seen_exc))
    return values


def _assert_chain_holds_no_index_or_searcher(exc):
    from shared.passage_index import PassageIndex
    from shared.passage_parallels import PassageSearcher

    for value in _collect_chain_locals(exc):
        assert not isinstance(value, PassageIndex), (
            f'exception chain still reaches a PassageIndex via {value!r}')
        assert not isinstance(value, PassageSearcher), (
            f'exception chain still reaches a PassageSearcher via {value!r}')


def test_boundary_mode_partial_raises_passage_search_error_and_severs_the_chain(
        tmp_path):
    """The natural, already-documented path: `boundary_mode='partial'` makes
    the real unmodified engine raise `ValueError` from deep inside the
    lease. Must surface as `PassageSearchError` (never the bare `ValueError`,
    and never `PassageSearchUnavailableError` -- an index IS installed
    here), and nothing in its ENTIRE chain may reach the index or searcher."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    with pytest.raises(pl.PassageSearchError) as excinfo:
        pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(
            text, boundary_mode='partial')

    assert not isinstance(excinfo.value, pl.PassageSearchUnavailableError)
    assert 'ValueError' in str(excinfo.value)
    _assert_chain_holds_no_index_or_searcher(excinfo.value)

    # close_passage_state() must succeed cleanly right here -- proof the
    # index really is unreachable, not merely unexamined.
    assert pl.close_passage_state() is True
    assert not pl.passage_available()


def test_boundary_holds_for_a_deep_engine_exception_not_just_the_known_valueerror(
        tmp_path, monkeypatch):
    """Same guarantee, forced from far deeper in the call graph than the
    boundary_mode check at the top of PassageSearcher.search_composition_
    logic -- proves this is a general boundary, not a special case carved
    out for the one ValueError the module already documents."""
    import shared.passage_parallels as passage_parallels

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    def _boom(*a, **k):
        raise RuntimeError('synthetic deep engine failure')

    # `search_passage` is imported BY NAME into passage_parallels's own
    # namespace (`from shared.passage_search import search_passage`) --
    # patching shared.passage_search.search_passage instead would leave
    # this already-bound reference untouched and the stub silently unused.
    monkeypatch.setattr(passage_parallels, 'search_passage', _boom)

    with pytest.raises(pl.PassageSearchError) as excinfo:
        pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(text)

    assert 'RuntimeError' in str(excinfo.value)
    assert 'synthetic deep engine failure' in str(excinfo.value)
    _assert_chain_holds_no_index_or_searcher(excinfo.value)

    assert pl.close_passage_state() is True


def test_boundary_raised_error_has_no_context_or_cause(tmp_path):
    """Raising INSIDE the `except` block would set `__context__` to the
    original exception (and `raise ... from None` only sets
    `__suppress_context__` -- it does NOT clear `__context__`), dragging the
    whole severed traceback back in through the back door. The fix raises
    only after the try/except/finally has fully completed; this asserts
    that empirically rather than trusting the sketch's description of it."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    with pytest.raises(pl.PassageSearchError) as excinfo:
        pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(
            text, boundary_mode='partial')

    # Exact type, not `isinstance` via the `pytest.raises` catch above --
    # `PassageSearchUnavailableError` is ALSO a `PassageSearchError` (see
    # its own docstring), so a regression that always reports "unavailable"
    # regardless of what actually failed would satisfy the catch above and
    # still pass every assertion below it.
    assert type(excinfo.value) is pl.PassageSearchError
    assert excinfo.value.__context__ is None
    assert excinfo.value.__cause__ is None

    assert pl.close_passage_state() is True


def test_retained_excinfo_value_across_close_is_harmless(tmp_path):
    """`pytest.raises(...) as excinfo` then touching `excinfo.value` is
    named explicitly as an escape route this boundary must close: `excinfo`
    keeps the raised `PassageSearchError` (and, transitively, whatever it
    can reach) alive for the rest of this test function -- including across
    the `close_passage_state()` call below. That close must SUCCEED, not
    defer or hang, and touching the retained exception afterward must not
    fault."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    with pytest.raises(pl.PassageSearchError) as excinfo:
        pl.get_passage_searcher(_NullTextFetcher()).search_composition_logic(
            text, boundary_mode='partial')

    assert pl.close_passage_state() is True
    assert not pl.passage_available()

    # excinfo/excinfo.value is STILL held by this frame's locals, well past
    # the close above -- exactly the shape of the access violation this
    # boundary exists to prevent. Touching it now must be inert.
    _ = str(excinfo.value)
    _ = repr(excinfo.value)
    # Exact type: `isinstance` alone also accepts the `PassageSearchUnavailableError`
    # subclass, which would let a regression that always reports
    # "unavailable" pass this unchanged.
    assert type(excinfo.value) is pl.PassageSearchError


def test_non_exception_baseexception_keeps_its_class_and_is_not_caught_by_except_exception(
        tmp_path, monkeypatch):
    """A propagating KeyboardInterrupt carries the same frames an ordinary
    exception would, so it must be caught by the boundary too -- but NOT
    converted to `PassageSearchError`, which would make it catchable by a
    plain `except Exception` and swallow shutdown semantics. A FRESH
    instance of the SAME class must cross instead: same type, no frames."""
    import shared.passage_parallels as passage_parallels

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:30]

    def _interrupt(self, *a, **k):
        raise KeyboardInterrupt()

    monkeypatch.setattr(passage_parallels.PassageSearcher,
                        'search_composition_logic', _interrupt)

    adapter = pl.get_passage_searcher(_NullTextFetcher())

    caught_as_exception = False
    try:
        adapter.search_composition_logic(text)
    except Exception:
        caught_as_exception = True
    except KeyboardInterrupt as exc:
        assert type(exc) is KeyboardInterrupt
        _assert_chain_holds_no_index_or_searcher(exc)
    assert not caught_as_exception, (
        'a non-Exception BaseException must NOT be catchable by an '
        'ordinary except Exception -- converting it to PassageSearchError '
        'would swallow shutdown semantics')

    # The lease's `finally` must have released regardless of which
    # exception class crossed the boundary.
    assert pl.close_passage_state() is True


def test_success_path_result_survives_close_no_memmap_backed_values(tmp_path):
    """Task 2: nobody had checked whether the RETURNED dict holds anything
    whose lifetime depends on the index's memmaps -- a numpy VIEW over a
    memmap keeps the mapping alive and faults on access after
    `close_passage_state()`. Runs a REAL search against a REAL built index
    that returns REAL rows (a real text fetcher, not `_NullTextFetcher`,
    which drops every row via `_render_highlights`'s failed-lookup path),
    closes the index, then deep-walks the returned structure -- touching
    every value and separately sweeping for `numpy.ndarray`/`np.memmap`
    instances or any `.base` chain that reaches one."""
    import numpy as np

    from shared.passage_index import PassageIndex
    from shared.passage_parallels import PassageSearcher

    class _EchoTextFetcher:
        """Returns the record's OWN corpus text keyed by its exact record
        id (`iter_records`' header, e.g. 'rec0000') -- a real, working
        text-fetch path with no dependency on the passage index itself."""

        def __init__(self, records):
            self._by_id = dict(records)

        def get_full_text_by_header(self, full_header):
            return self._by_id.get(full_header)

    def _walk_and_touch(obj, seen=None):
        """Visits every value reachable from `obj` and 'touches' it --
        `str()` forces materialization, so a numpy view over an already-
        closed memmap would fault right here, not silently later."""
        if seen is None:
            seen = set()
        oid = id(obj)
        if oid in seen:
            return
        seen.add(oid)
        if isinstance(obj, dict):
            for v in obj.values():
                _walk_and_touch(v, seen)
        elif isinstance(obj, (list, tuple, set)):
            for v in obj:
                _walk_and_touch(v, seen)
        else:
            str(obj)

    def _find_memmap_backed(obj, seen=None):
        if seen is None:
            seen = set()
        oid = id(obj)
        if oid in seen:
            return []
        seen.add(oid)
        found = []
        if isinstance(obj, np.ndarray):
            found.append(obj)
            base = obj.base
            while base is not None:
                if isinstance(base, np.memmap):
                    found.append(base)
                base = getattr(base, 'base', None)
        elif isinstance(obj, dict):
            for v in obj.values():
                found.extend(_find_memmap_backed(v, seen))
        elif isinstance(obj, (list, tuple, set)):
            for v in obj:
                found.extend(_find_memmap_backed(v, seen))
        return found

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    text = c.records[0][1][:60]

    fetcher = _EchoTextFetcher(c.records)
    result = pl.get_passage_searcher(fetcher).search_composition_logic(text)

    assert result['main'], (
        'test setup: need at least one REAL rendered hit to prove '
        'anything -- an empty result proves nothing about memmap safety')
    assert result['main'][0]['chunk_hits'], (
        'test setup: need real span data (chunk_hits), not an empty row')
    assert not isinstance(result['main'][0]['raw_header'], PassageIndex)
    assert not isinstance(result['main'][0]['raw_header'], PassageSearcher)

    assert pl.close_passage_state() is True
    assert not pl.passage_available()

    _walk_and_touch(result)  # would fault right here if anything leaked
    offenders = _find_memmap_backed(result)
    assert offenders == [], (
        f'result holds {len(offenders)} memmap-backed numpy object(s) '
        'after close -- these must be materialised (e.g. .copy()) inside '
        'the exception boundary before the adapter returns them')


# ---------------------------------------------------------------------------
# 0g. PassageIndex.close() (shared/passage_index.py) is the structural fix
#     the boundary above is defense-in-depth FOR: every one of the five
#     section attributes is REPLACED with a poison stand-in that raises
#     PassageIndexClosed on every access route, not merely left pointing at
#     a dangling memmap for the next reader to fault on. These tests drive
#     PassageIndex.close() directly -- a real built index, never a mock --
#     independent of the desktop lifecycle module that now delegates to it
#     (_release_index_handles).
# ---------------------------------------------------------------------------

def _open_a_free_index(tmp_path):
    """A standalone, real PassageIndex nothing else owns: built and opened
    via `execute_build`, which -- unlike `_Corpus.build()` -- never installs
    into `pl._state` (see its own docstring: "does NOT call
    install_passage_state on success"). These tests want a PassageIndex the
    autouse `_reset_module_globals` fixture's own close_passage_state()
    calls never touch, so PassageIndex.close() itself is what each
    assertion is exercising."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built', built.status
    idx = open_index(built.staging_dir)
    assert idx is not None
    return idx, c


def test_close_makes_every_read_accessor_raise_passage_index_closed(
        tmp_path):
    from shared.passage_index import PassageIndexClosed

    idx, c = _open_a_free_index(tmp_path)

    # Sanity, BEFORE close: every accessor must actually work -- otherwise
    # the raises below could be catching a pre-existing failure, not
    # close()'s effect.
    import numpy as np
    assert isinstance(idx.stream(0), str)
    assert isinstance(idx.record_id(0), str)
    assert idx.n_records == len(c.records)
    assert idx.n_postings >= 0
    idx.df(0)
    idx.dfs(np.array([0, 1], dtype=np.int64))
    idx.postings_for(0)

    idx.close()

    with pytest.raises(PassageIndexClosed):
        idx.stream(0)
    with pytest.raises(PassageIndexClosed):
        idx.record_id(0)
    with pytest.raises(PassageIndexClosed):
        idx.postings_for(0)
    with pytest.raises(PassageIndexClosed):
        idx.df(0)
    with pytest.raises(PassageIndexClosed):
        idx.dfs(np.array([0, 1], dtype=np.int64))
    with pytest.raises(PassageIndexClosed):
        _ = idx.n_records
    with pytest.raises(PassageIndexClosed):
        _ = idx.n_postings


def test_every_poison_dunder_raises_on_its_own(tmp_path):
    """Each dunder must raise BY ITSELF, exercised directly on a closed
    section rather than through an accessor.

    Going through `idx.stream(0)` and friends proves only that SOME guard
    fired: `__getitem__` runs first on every real call site, so it alone
    satisfies every accessor test above while `__array__`, `__len__` and
    `__iter__` could be silently broken. A section reached by a future
    caller doing `np.asarray(idx.records)` after close would then coerce to
    a plausible empty array instead of raising -- silent wrong data, which
    is worse than the crash the poison replaced.
    """
    import numpy as np
    from shared.passage_index import PassageIndexClosed

    idx, _ = _open_a_free_index(tmp_path)
    idx.close()
    section = idx.records

    with pytest.raises(PassageIndexClosed):
        section.__array__()
    with pytest.raises(PassageIndexClosed):
        section[0]
    with pytest.raises(PassageIndexClosed):
        section[0:2]
    with pytest.raises(PassageIndexClosed):
        len(section)
    with pytest.raises(PassageIndexClosed):
        iter(section)
    with pytest.raises(PassageIndexClosed):
        _ = section.shape
    # numpy's own coercion must not find a way through either.
    with pytest.raises(PassageIndexClosed):
        np.asarray(section)


def test_close_survives_a_section_that_fails_to_unmap(tmp_path):
    """A section whose `mm.close()` raises must still be poisoned, and must
    not strand the sections after it.

    `_closed` is set before the loop, so an exception escaping mid-loop
    would leave the remaining sections LIVE and still returning correct
    data, with the idempotence guard blocking any retry -- the precise
    dangling state close() exists to remove. The leaked mapping is logged
    rather than raised: `close_passage_state()` documents a True/False
    contract, and on Windows a leak already surfaces where it is actionable,
    as the swap's rename retry.
    """
    from shared.passage_index import PassageIndexClosed

    idx, _ = _open_a_free_index(tmp_path)

    class _Unclosable:
        def close(self):
            raise OSError('mapping busy')

    # gram_offsets is FIRST in close()'s fixed order, so a naive loop would
    # strand all four sections after it.
    object.__setattr__(idx.gram_offsets, '_mmap', _Unclosable())

    idx.close()  # must not raise

    for name in ('gram_offsets', 'postings', 'streams', 'records',
                 'record_ids'):
        with pytest.raises(PassageIndexClosed):
            getattr(idx, name)[0]


def test_close_twice_is_a_noop(tmp_path):
    """The swap path and a finalizer can both reach close() on the same
    object -- a second call must not raise (a double mmap.close(), or a
    redundant attribute swap that itself somehow failed)."""
    idx, _ = _open_a_free_index(tmp_path)
    idx.close()
    idx.close()  # must not raise

    from shared.passage_index import PassageIndexClosed
    with pytest.raises(PassageIndexClosed):
        idx.stream(0)


# ---------------------------------------------------------------------------
# 0h. The LogRecord route (Change 2): _render_highlights used to log a
#     swallowed text-fetch failure with exc_info=True, which stores the raw
#     (type, value, traceback) tuple on the LogRecord -- and that
#     traceback's outermost frame holds `self` (the searcher, hence its
#     index). Any handler that RETAINS LogRecords (logging.handlers.
#     MemoryHandler, a Qt recent-errors panel, pytest's own caplog -- every
#     test in this suite runs under caplog) would keep the index reachable
#     past close(). Fixed by logging traceback.format_exc() -- a STRING,
#     holding no frames -- instead.
# ---------------------------------------------------------------------------

class _RaisingTextFetcher:
    """Always raises -- the ONLY way to drive _render_highlights's
    exc_info-logging branch; _NullTextFetcher (returning None) instead
    takes the separate, always-exc_info-free "text lookup failed" branch."""

    def get_full_text_by_header(self, full_header):
        raise RuntimeError('synthetic text-fetch failure')


class _RetainingHandler(logging.Handler):
    """Models exactly what the docstring above warns about: a handler that
    keeps every LogRecord it sees for later inspection, the way
    MemoryHandler and a desktop "recent errors" panel both do."""

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _collect_record_reachable_values(record):
    """Everything reachable from one retained LogRecord: its own
    __dict__ (covers .args, .msg, and any extra=... fields), plus -- only
    if exc_info was ever attached -- the full frame-local walk
    _collect_chain_locals already does for a raised exception. That
    combination is exactly what a handler retaining LogRecords keeps
    reachable long after the call that logged them returns."""
    values = list(record.__dict__.values())
    exc_info = record.exc_info
    if exc_info:
        _, exc_value, _ = exc_info
        values.extend(_collect_chain_locals(exc_value))
    return values


def _assert_no_record_reaches_index_or_searcher(records):
    from shared.passage_index import PassageIndex
    from shared.passage_parallels import PassageSearcher

    for record in records:
        for value in _collect_record_reachable_values(record):
            assert not isinstance(value, PassageIndex), (
                f'retained LogRecord {record.getMessage()!r} still reaches '
                f'a PassageIndex via {value!r}')
            assert not isinstance(value, PassageSearcher), (
                f'retained LogRecord {record.getMessage()!r} still reaches '
                f'a PassageSearcher via {value!r}')


def test_retaining_log_handler_never_reaches_the_index_after_close(
        tmp_path):
    """Attaches a REAL handler that retains LogRecords directly to
    shared.passage_parallels's logger, runs a search whose text fetcher
    ALWAYS raises (so _render_highlights's exc_info branch actually fires),
    closes the state, then walks every retained record for a reachable
    PassageIndex/PassageSearcher -- asserting none. Then, belt and braces,
    smuggles out the raw index reference deliberately (bypassing the fix
    entirely) and asserts even THAT raises PassageIndexClosed rather than
    faulting -- proof this is defended in depth, not merely that this one
    route happens to carry no reference today.
    """
    import shared.passage_parallels as passage_parallels
    from shared.passage_index import PassageIndexClosed

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    # A 60-letter self-match, same length proven to produce a real rendered
    # hit in test_success_path_result_survives_close_no_memmap_backed_values
    # -- _render_highlights's exc_info branch only fires for an ACTUAL hit.
    text = c.records[0][1][:60]
    smuggled_idx = pl._state.index  # deliberately smuggled, BEFORE close

    handler = _RetainingHandler()
    passage_parallels.logger.addHandler(handler)
    passage_parallels.logger.setLevel(logging.WARNING)
    try:
        pl.get_passage_searcher(
            _RaisingTextFetcher()).search_composition_logic(text)
    finally:
        passage_parallels.logger.removeHandler(handler)

    assert handler.records, (
        'test setup: the raising text fetcher must have produced at least '
        'one warning -- an empty capture proves nothing about the '
        'exc_info fix')
    assert not any(r.exc_info for r in handler.records), (
        'a retained LogRecord still carries exc_info -- exc_info=True is '
        'back on some call site, or a new one was added')

    assert pl.close_passage_state() is True
    _assert_no_record_reaches_index_or_searcher(handler.records)

    with pytest.raises(PassageIndexClosed):
        smuggled_idx.stream(0)


# ---------------------------------------------------------------------------
# 0i. End-to-end proof, in an ISOLATED subprocess: dereferencing a smuggled
#     PassageIndex reference after close() raises a catchable
#     PassageIndexClosed instead of faulting the process. Never run
#     in-process -- the whole point is that the UNPOISONED version of this
#     scenario really does crash (Windows 0xC0000005 / SIGSEGV), and doing
#     that in the pytest process itself would take the whole run down
#     rather than prove anything about it.
# ---------------------------------------------------------------------------

# Exit codes observed for a genuine access violation. Windows reports
# 0xC0000005 (STATUS_ACCESS_VIOLATION) as the unsigned 32-bit returncode
# 3221225477 from a native `subprocess.run` (confirmed empirically on this
# machine); some Python/OS combinations instead surface it sign-extended as
# -1073741819. POSIX SIGSEGV shows up as -11 (Python's own signal
# convention) or 139 (128 + SIGSEGV, a shell's convention) depending on how
# the code is read back. All four are the SAME fault, not four different
# ones -- checked as a set so this test is not machine-specific.
_ACCESS_VIOLATION_RETURN_CODES = {3221225477, -1073741819, -11, 139}


def _crash_conversion_script() -> str:
    """argv: [index_dir, mode]. mode='poison' closes via the real fix
    (PassageIndex.close()); mode='raw' reproduces the PRE-FIX teardown
    _release_index_handles used to do -- close each section's `._mmap`
    directly, leaving the attribute itself still pointing at the now-
    dangling array -- so the 'raw' run is the control this test needs: if
    IT does not crash, this test proves nothing about what close() fixes.

    SetErrorMode suppresses the Windows "python.exe has stopped working"
    crash dialog for the 'raw' branch -- without it, an access violation
    here would pop a UI dialog and hang the subprocess (and this test)
    waiting for a user who is not present to dismiss it.
    """
    return (
        "import sys, os\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "if os.name == 'nt':\n"
        "    import ctypes\n"
        "    ctypes.windll.kernel32.SetErrorMode(0x0003)\n"
        "from shared.passage_index import open_index, PassageIndexClosed\n"
        "idx = open_index(sys.argv[1])\n"
        "assert idx is not None, 'FAILED_TO_OPEN'\n"
        "smuggled = idx\n"
        "mode = sys.argv[2]\n"
        "if mode == 'poison':\n"
        "    idx.close()\n"
        "else:\n"
        "    for name in ('gram_offsets', 'postings', 'streams', 'records',"
        " 'record_ids'):\n"
        "        arr = getattr(idx, name, None)\n"
        "        mm = getattr(arr, '_mmap', None)\n"
        "        if mm is not None:\n"
        "            mm.close()\n"
        "try:\n"
        "    smuggled.stream(0)\n"
        "except PassageIndexClosed:\n"
        "    print('CAUGHT_CLOSED', flush=True)\n"
        "    sys.exit(0)\n"
        "except Exception as e:\n"
        "    print('OTHER_EXCEPTION:' + type(e).__name__, flush=True)\n"
        "    sys.exit(2)\n"
        "print('NO_EXCEPTION', flush=True)\n"
        "sys.exit(3)\n"
    )


def test_crash_converts_to_passage_index_closed_in_a_real_subprocess(
        tmp_path):
    # Built ONCE and shared, read-only, between both subprocess runs below
    # -- only the in-CHILD teardown mode ('poison' vs 'raw') differs.
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths,
                             partitions=2)
    assert built.status == 'built', built.status
    index_dir = built.staging_dir
    script = _crash_conversion_script()

    poisoned = subprocess.run(
        [sys.executable, '-c', script, index_dir, 'poison'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        timeout=60)
    assert poisoned.returncode == 0, (
        f'poisoned dereference did not exit 0 -- returncode='
        f'{poisoned.returncode!r} stdout={poisoned.stdout!r} '
        f'stderr={poisoned.stderr[-2000:]!r}')
    assert 'CAUGHT_CLOSED' in poisoned.stdout, (
        f'poisoned dereference exited 0 but never caught PassageIndexClosed'
        f' -- stdout={poisoned.stdout!r}')

    raw = subprocess.run(
        [sys.executable, '-c', script, index_dir, 'raw'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        timeout=60)
    assert raw.returncode in _ACCESS_VIOLATION_RETURN_CODES, (
        'control run: the PRE-FIX teardown (close the mmap, leave the '
        'attribute dangling) must genuinely fault this process -- '
        f'returncode={raw.returncode!r} stdout={raw.stdout!r} -- '
        'otherwise this test proves nothing about what PassageIndex.close()'
        ' fixes')
    assert 'CAUGHT_CLOSED' not in raw.stdout, (
        'the control run caught PassageIndexClosed -- it should not even '
        'define that path, only reproduce the pre-fix crash')


def _pre_close_view_crash_script() -> str:
    """argv: [index_dir]. The DOCUMENTED contract violation from
    `PassageIndex`'s own docstring, not a bug `close()` failed to catch: a
    raw section VIEW taken BEFORE `close()` is a reference `close()` has no
    way to reach -- it poisons the five section ATTRIBUTES, not whatever a
    caller already extracted from one -- so the view keeps pointing at the
    mapping after `close()` unmaps it, and dereferencing it faults the
    process exactly like the 'raw' control above. There is no 'poison'
    counterpart to run here: unlike the smuggled-`PassageIndex` case, no
    fix is possible for this one (see the docstring), so this script only
    ever reproduces the fault."""
    return (
        "import sys, os\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "if os.name == 'nt':\n"
        "    import ctypes\n"
        "    ctypes.windll.kernel32.SetErrorMode(0x0003)\n"
        "import numpy as np\n"
        "from shared.passage_index import open_index\n"
        "idx = open_index(sys.argv[1])\n"
        "assert idx is not None, 'FAILED_TO_OPEN'\n"
        "view = idx.records[:1].view(np.ndarray)  # taken BEFORE close()\n"
        "idx.close()\n"
        # `view[0]` alone returns a numpy.void scalar that WRAPS the
        # buffer without necessarily reading it -- str() is what forces
        # the actual bytes to be materialised, which is where this faults.
        "s = str(view[0])\n"
        "print('NO_CRASH', s, flush=True)\n"
        "sys.exit(3)\n"
    )


def test_view_taken_before_close_faults_the_process_documented_violation(
        tmp_path):
    """HIGH-2, pinned as the trap it is: a raw section view captured BEFORE
    `close()` is NOT made safe by anything in this module, and cannot be --
    `close()`'s poisoning replaces the five ATTRIBUTES, which is unreachable
    from a reference a caller already holds off one of them, and deferring
    the unmap to protect such a reference would just bring back the
    Windows rename/rmtree hazard `close()` exists to fix. This test does
    not prove the crash is caught (it is not, and must not be) -- it proves
    the documented contract in `PassageIndex`'s docstring is the true
    behaviour of this interpreter, not an untested claim. Run in a
    subprocess, like the crash-conversion test above: the whole point is
    that this really does fault the process, and doing that in the pytest
    process itself would take the whole run down instead of proving
    anything about it."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths,
                             partitions=2)
    assert built.status == 'built', built.status
    script = _pre_close_view_crash_script()

    result = subprocess.run(
        [sys.executable, '-c', script, built.staging_dir],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        timeout=60)
    assert result.returncode in _ACCESS_VIOLATION_RETURN_CODES, (
        'a raw section view taken BEFORE close() must fault the process '
        'once the mapping is unmapped -- returncode='
        f'{result.returncode!r} stdout={result.stdout!r} -- otherwise the '
        "documented contract in PassageIndex's docstring is wrong")
    assert 'NO_CRASH' not in result.stdout


# ---------------------------------------------------------------------------
# 1. Cancel propagation: pass 1 / spooling / scatter / partition / hash.
# ---------------------------------------------------------------------------

def test_hash_cancel_reports_cancelled_before_any_build_work(tmp_path):
    """cancel on the very first hash call: build_index never even starts, and
    staging must never have been created at all."""
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_check_none_is_a_noop_for_execute_build(tmp_path):
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=None)
    assert outcome.status == 'built'
    assert outcome.stats.n_records_indexed == len(c.records)


def test_cancel_inside_pass1_propagates_as_cancelled(tmp_path, monkeypatch):
    """The hash step is bypassed here (a stub with no cancel_check calls of
    its own) so every observed call is pass 1's -- proving execute_build
    plumbs cancel_check all the way into build_index, not just to the hash."""
    monkeypatch.setattr(passage_builder, 'CANCEL_CHECK_RECORDS', 3)
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=20)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


_HUGE_BATCH = 1 << 40  # forces one batch per partition; see test_passage_builder.py


def test_cancel_inside_spool_spooling_loop_propagates(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction='spool', partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_inside_scatter_partition_loop_propagates(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction='scatter', partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_in_begin_swap_hash_propagates_and_cleans_staging(tmp_path):
    """begin_swap's own corpus re-fingerprint is cancellable too, and a
    cancel there must PROPAGATE (not be swallowed into a SwapValidation)."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths)
    assert built.status == 'built'
    with pytest.raises(BuildCancelled):
        pl.begin_swap(built.staging_dir, c.source_paths,
                      built.pre_build_manifest, cancel_check=_cancel_after(1))
    assert not os.path.isdir(built.staging_dir)


# ---------------------------------------------------------------------------
# 2. Staging is deletable immediately after a cancel (Windows handle case).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('construction', ['scatter', 'spool'])
def test_staging_is_immediately_deletable_after_a_mid_build_cancel(
        tmp_path, construction):
    """No ignore_errors=True anywhere on this path: if a memmap handle
    leaked, shutil.rmtree would raise PermissionError right here."""
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction=construction, partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(2))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)
    # Cleanup already happened; a SECOND attempt on the same (now-absent)
    # path must not be needed, but proves nothing is still pinning it.
    if os.path.isdir(outcome.staging_dir):
        shutil.rmtree(outcome.staging_dir)  # would raise if anything leaked


# ---------------------------------------------------------------------------
# 3. Any unswapped failure cleans staging; the ownership flag prevents the
#    finally/swap race.
# ---------------------------------------------------------------------------

def test_build_error_cleans_staging(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('synthetic build failure')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        outcome = pl.execute_build(c.root, c.records, c.source_paths)
    finally:
        pl.build_index = orig
    assert outcome.status == 'error'
    assert not os.path.isdir(outcome.staging_dir)


def test_ownership_flag_prevents_the_finally_swap_race(tmp_path):
    """`owns_staging` flips to False the instant `build_index` returns, BEFORE
    `execute_build` does anything else -- so by the time a caller receives a
    'built' outcome and starts consuming staging (the real swap step,
    simulated here by just reading the artifact out of it), nothing left
    inside `execute_build`'s own `finally` can still delete it out from
    under that consumer. Contrast with the cancel/error paths, which still
    own staging throughout and must clean it up themselves."""
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths)
    assert outcome.status == 'built'
    # execute_build has already returned; staging must STILL be there, fully
    # readable, exactly as a swap step consuming it would need.
    assert os.path.isdir(outcome.staging_dir)
    assert os.path.exists(os.path.join(outcome.staging_dir, 'manifest.json'))
    shutil.rmtree(outcome.staging_dir)  # test cleanup, not part of the assertion


# ---------------------------------------------------------------------------
# 4. First build: staging renames directly to live; live untouched by a
#    cancelled or failed build.
# ---------------------------------------------------------------------------

def test_first_build_renames_staging_directly_to_live_no_generation(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build()
    assert res.status == 'installed'
    assert os.path.isdir(c.live_dir)
    entries = os.listdir(c.root)
    assert not any(n.startswith(pl.PREV_PREFIX) for n in entries), entries


def test_live_untouched_by_a_cancelled_first_build(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build(cancel_check=lambda: True)
    assert res.status == 'cancelled'
    assert not os.path.isdir(c.live_dir)


def test_live_untouched_by_a_failed_first_build(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('boom')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        res = c.build()
    finally:
        pl.build_index = orig
    assert res.status == 'error'
    assert not os.path.isdir(c.live_dir)


# ---------------------------------------------------------------------------
# 5. Rebuild: unique generation naming; back-to-back rebuilds don't collide;
#    a lagging background delete cannot hit the next generation.
# ---------------------------------------------------------------------------

def test_rebuild_creates_a_unique_prev_generation(tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    res2 = c.build()
    assert res2.status == 'installed'
    _join_background_threads(res2)
    # The prev generation from res2's rebuild is background-deleted; by now
    # it should be gone, and `current` must hold the SECOND build.
    assert os.path.isdir(c.live_dir)


def test_back_to_back_rebuilds_never_collide_on_generation_name(tmp_path,
                                                                 monkeypatch):
    """Freeze the deletion of every generation so all of them coexist on
    disk at once, proving the NAMES themselves never collide even when nanosecond
    resolution alone would risk it."""
    seen_names = []
    orig_token = pl._gen_token

    def _tracking_token():
        tok = orig_token()
        seen_names.append(tok)
        return tok

    monkeypatch.setattr(pl, '_gen_token', _tracking_token)
    monkeypatch.setattr(pl, '_delete_generation_background',
                        lambda path: None)  # freeze: never actually delete

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    for _ in range(4):
        assert c.build().status == 'installed'

    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert len(prevs) == 4, prevs  # one per rebuild, none collided/overwrote
    assert len(set(prevs)) == 4


def test_a_lagging_background_delete_cannot_hit_the_next_generation(
        tmp_path, monkeypatch):
    """Hold the FIRST rebuild's background delete thread before it runs, do
    a SECOND rebuild in the meantime, then release the first delete -- it
    must remove only its own (now-stale) generation, never the second
    rebuild's live directory or its own new generation."""
    release = {'go': False}
    started = []

    def _held_delete(path):
        def _do():
            while not release['go']:
                time.sleep(0.01)
            shutil.rmtree(path, ignore_errors=True)
        import threading
        t = threading.Thread(target=_do, daemon=True)
        started.append((path, t))
        t.start()
        return t

    monkeypatch.setattr(pl, '_delete_generation_background', _held_delete)

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    res2 = c.build()
    assert res2.status == 'installed'
    first_prev = started[0][0]
    # The held delete has not run yet, so res2's own prev generation must
    # still be sitting on disk under the EXACT name it was queued with.
    current_prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert current_prevs == [os.path.basename(first_prev)], current_prevs

    res3 = c.build()
    assert res3.status == 'installed'

    release['go'] = True
    for _path, t in started:
        t.join(timeout=5)

    assert os.path.isdir(c.live_dir), 'a lagging delete removed live itself'
    idx = open_index(c.live_dir)
    assert idx is not None, 'live must still open after the lagging deletes settle'


# ---------------------------------------------------------------------------
# 6. A None reload after the swap: retry, then rollback (old index works
#    again); the first-install variant quarantines and reports.
# ---------------------------------------------------------------------------

def _flaky_open_index(live_dir, fail_times):
    """Fails exactly `fail_times` calls made against `live_dir`, then defers
    to the real open_index -- models a genuinely bad reload (both retry
    attempts in `_open_with_retry` exhausted) without touching the fixture
    on disk."""
    real = pl.open_index
    calls = {'n': 0}

    def stub(path):
        if os.path.normpath(path) == os.path.normpath(live_dir):
            calls['n'] += 1
            if calls['n'] <= fail_times:
                return None
        return real(path)
    return stub, calls


def test_none_reload_on_rebuild_retries_then_rolls_back(tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(live, fail_times=2)  # both attempts fail
    monkeypatch.setattr(pl, 'open_index', stub)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'reload_failed_rolled_back'
    assert swap.index is not None
    assert calls['n'] >= 2
    # The rollback quarantine is a proven-good-live rollback -- deleted
    # immediately, never surfaced.
    assert not any(n.startswith(pl.FAILED_PREFIX) for n in os.listdir(c.root))
    assert open_index(live) is not None, 'old index must work again'


def test_none_reload_on_first_install_quarantines_and_reports(tmp_path,
                                                               monkeypatch):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(c.live_dir, fail_times=2)
    monkeypatch.setattr(pl, 'open_index', stub)
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'reload_failed_first_install'
    assert swap.quarantine_dir
    assert os.path.isdir(swap.quarantine_dir)
    assert not os.path.isdir(c.live_dir)


def test_a_transient_open_failure_does_not_downgrade_a_valid_reload(
        tmp_path, monkeypatch):
    """One flaky failure, then success -- must NOT be treated as a genuine
    None reload."""
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(live, fail_times=1)  # first attempt only
    monkeypatch.setattr(pl, 'open_index', stub)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'installed', swap.status
    assert swap.index is not None


# ---------------------------------------------------------------------------
# 6b. The two catastrophic double-failure branches: the new build's reload
#     fails AND the rollback itself cannot be completed cleanly.
# ---------------------------------------------------------------------------

# A marker file travels with a directory through every os.rename (rename is
# metadata-only -- it never touches contents), so tying "this build is
# broken" to the marker's presence, rather than to whatever path the content
# currently sits under, lets the SAME simulated-broken build follow staging
# -> live -> quarantine while a never-marked previous generation opens fine
# wherever IT ends up, including after the terminal recovery walk promotes
# it back into `current/`. A stub keyed on the "live" path instead would
# also block the recovery walk's own post-promotion reopen of that exact
# path, which cannot be told apart from a genuinely unrecoverable machine.
_BROKEN_BUILD_MARKER = '.simulated_broken'


def test_rollback_failed_terminal_recovery_still_finds_the_previous_generation(
        tmp_path, monkeypatch):
    """New live renamed in, fails to open, gets quarantined -- then the
    restore of the known-good previous generation is blocked long enough to
    exhaust `_swap_rebuild`'s own bounded retry (`rollback_failed`). Nothing
    may be deleted: not the quarantine (the only copy of the broken build)
    and not the previous generation. And -- the point of this test --
    `run_build_and_swap`'s terminal path must not stop at a bare open of
    `current/`: it must route through the same recovery walk startup uses,
    which gets its OWN separate bounded retry at the very same restore
    rename and (here, modelling a transient block that clears a moment
    later, not a permanently cursed path) succeeds where the swap's narrower
    retry gave up -- promoting the previous generation into `current/` and
    returning it as a working index, not leaving the caller with nothing."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    real_open = pl.open_index

    def _wont_open_if_marked_broken(path):
        if os.path.isfile(os.path.join(path, _BROKEN_BUILD_MARKER)):
            return None
        return real_open(path)

    monkeypatch.setattr(pl, 'open_index', _wont_open_if_marked_broken)

    # Mark the NEW build broken only once `begin_swap`'s own validation of
    # staging has already passed -- otherwise the marker would fail that
    # earlier, unrelated check instead of the swap's post-promotion reload.
    real_begin_swap = pl.begin_swap

    def _mark_new_build_broken(staging_dir, source_paths, pre_build_manifest,
                               cancel_check=None):
        result = real_begin_swap(staging_dir, source_paths, pre_build_manifest,
                                 cancel_check=cancel_check)
        if result.ok:
            open(os.path.join(staging_dir, _BROKEN_BUILD_MARKER), 'w').close()
        return result

    monkeypatch.setattr(pl, 'begin_swap', _mark_new_build_broken)
    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename
    blocked_calls = {'n': 0}

    def _restore_blocked_then_clears(src, dst):
        # Only the prev->live restore rename is blocked -- live->prev and
        # staging->live (both earlier in the same swap) must still succeed.
        # Blocked for exactly RENAME_RETRY_ATTEMPTS calls -- enough to
        # exhaust the swap's own retry loop -- then it clears, exactly like
        # a lagging antivirus scan rather than a rename that can never land.
        if os.path.basename(src).startswith(pl.PREV_PREFIX) \
                and os.path.normpath(dst) == os.path.normpath(live):
            blocked_calls['n'] += 1
            if blocked_calls['n'] <= 2:
                raise PermissionError('synthetic: prev->live restore blocked')
        return real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _restore_blocked_then_clears)

    res = c.build()

    assert res.status == 'rollback_failed'
    assert res.quarantine_dir
    assert os.path.isdir(res.quarantine_dir), (
        'the unopenable build must be preserved, not deleted')
    assert res.index is not None, (
        'the terminal recovery walk must still return the previous '
        'generation as a working index, not leave the caller with nothing')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == c.live_dir
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the recovered generation was promoted into current/ by the '
        'recovery walk, not deleted and not left behind under its own name')
    assert open_index(c.live_dir) is not None


def test_reload_failed_no_recovery_terminal_recovery_still_finds_live(
        tmp_path, monkeypatch):
    """The rollback rename itself succeeds, but re-opening the restored
    directory ALSO fails within `_swap_rebuild`'s own bounded retry
    (`reload_failed_no_recovery`) -- modelling a transient inability to open
    that specific location that clears a moment later, not a directory that
    is genuinely, permanently unopenable. Both copies (the quarantined
    broken build and the restored live) must survive; nothing is deleted.
    And -- the point of this test -- `run_build_and_swap`'s terminal path
    must route through the SAME recovery walk startup uses, whose own
    separate bounded retry succeeds where the swap's narrower one gave up,
    returning the restored generation as a working index rather than
    leaving the caller with nothing despite `current/` holding a perfectly
    good artifact."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    real_open = pl.open_index

    real_begin_swap = pl.begin_swap

    def _mark_new_build_broken(staging_dir, source_paths, pre_build_manifest,
                               cancel_check=None):
        result = real_begin_swap(staging_dir, source_paths, pre_build_manifest,
                                 cancel_check=cancel_check)
        if result.ok:
            open(os.path.join(staging_dir, _BROKEN_BUILD_MARKER), 'w').close()
        return result

    monkeypatch.setattr(pl, 'begin_swap', _mark_new_build_broken)

    live_open_calls = {'n': 0}

    def _stub_open(path):
        if os.path.isfile(os.path.join(path, _BROKEN_BUILD_MARKER)):
            return None  # the new build's own data, wherever it currently sits
        if os.path.normpath(path) == os.path.normpath(live):
            live_open_calls['n'] += 1
            if live_open_calls['n'] <= 2:
                # Transient: fails exactly as many times as `_swap_rebuild`'s
                # own reload check attempts, then clears before the terminal
                # recovery walk's SEPARATE retry gets to it.
                return None
        return real_open(path)

    monkeypatch.setattr(pl, 'open_index', _stub_open)

    res = c.build()

    assert res.status == 'reload_failed_no_recovery'
    assert res.quarantine_dir
    assert os.path.isdir(res.quarantine_dir), (
        'the unopenable new build must be preserved, not deleted')
    assert res.index is not None, (
        'the terminal recovery walk must still return the restored '
        'generation as a working index')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == c.live_dir
    assert open_index(c.live_dir) is not None
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the previous generation was RENAMED into live, not left behind '
        'under its own name')


# ---------------------------------------------------------------------------
# 6c. A blocked RESTORE (prev -> live) must never raise past the swap --
#     both call sites that lacked a guard around that specific rename.
# ---------------------------------------------------------------------------

def test_cancel_restore_failure_still_routes_through_terminal_recovery(
        tmp_path, monkeypatch):
    """Cancel lands right after `current -> _prev`, so `_swap_rebuild` tries
    to restore `_prev -> current` -- and that restore is blocked long enough
    to exhaust the swap's own bounded retry. Unlike every other destructive
    rename in this module, this one call site had NO `except PermissionError`
    at all, so the FINAL attempt's raise used to propagate straight out of
    `perform_swap`, past the terminal recovery walk that exists to promote
    the surviving `_prev-*` generation -- leaving `current/` absent, a valid
    `_prev-*` still on disk, and the caller told there is no index. Blocked
    only long enough to exhaust `_swap_rebuild`'s own retry, then clears --
    modelling a lagging antivirus scan, not a path that can never be renamed
    -- so the terminal walk's SEPARATE retry at the same rename succeeds
    where the swap's narrower one gave up."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename
    state = {'prev_created': False}
    blocked_calls = {'n': 0}

    def _rename_hook(src, dst):
        norm_src = os.path.normpath(src)
        norm_dst = os.path.normpath(dst)
        if norm_src == os.path.normpath(live) \
                and os.path.basename(norm_dst).startswith(pl.PREV_PREFIX):
            real_rename(src, dst)
            state['prev_created'] = True  # only NOW may cancel_check fire
            return
        if os.path.basename(norm_src).startswith(pl.PREV_PREFIX) \
                and norm_dst == os.path.normpath(live):
            blocked_calls['n'] += 1
            if blocked_calls['n'] <= 2:
                raise PermissionError('synthetic: prev->live restore blocked')
        real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _rename_hook)

    def cancel():
        # False throughout the build and the hash re-check -- only becomes
        # True once `current -> _prev` has actually happened, matching the
        # exact race this test targets.
        return state['prev_created']

    res = c.build(cancel_check=cancel)

    assert res.status == 'rollback_failed'
    # Assert on the function's OWN returned index, not a side-channel
    # open_index(live) -- a blocked restore that raises past the terminal
    # walk would leave res.index None here.
    assert res.index is not None, (
        'a blocked restore must not raise past the terminal recovery walk '
        '-- the caller must still end up with a working index')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == live
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the surviving generation was promoted into current/ by the '
        'recovery walk, not left behind under its own name and not deleted')


def test_failed_promotion_restore_failure_still_routes_through_terminal_recovery(
        tmp_path, monkeypatch):
    """The NEW build can never be promoted (`staging -> current` is blocked
    for good), so `_swap_rebuild` tries to restore the OLD generation --
    and that restore is also blocked long enough to exhaust the swap's own
    bounded retry. This call site had no guard around the restore's FINAL
    attempt either, so it used to raise past `perform_swap` exactly like the
    cancel-path sibling above, instead of reporting a status the terminal
    recovery walk can act on."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir
    staging_dir = os.path.join(c.root, pl.STAGING_DIRNAME)

    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename
    blocked_calls = {'n': 0}

    def _rename_hook(src, dst):
        norm_src = os.path.normpath(src)
        norm_dst = os.path.normpath(dst)
        if norm_src == os.path.normpath(staging_dir) \
                and norm_dst == os.path.normpath(live):
            # The promotion itself never clears -- a genuinely cursed path,
            # not a transient flake.
            raise PermissionError('synthetic: staging->live permanently blocked')
        if os.path.basename(norm_src).startswith(pl.PREV_PREFIX) \
                and norm_dst == os.path.normpath(live):
            blocked_calls['n'] += 1
            if blocked_calls['n'] <= 2:
                raise PermissionError('synthetic: prev->live restore blocked')
        real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _rename_hook)

    res = c.build()

    assert res.status == 'rollback_failed'
    assert res.index is not None, (
        'a blocked restore must not raise past the terminal recovery walk '
        '-- the caller must still end up with a working index')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == live
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the surviving generation was promoted into current/ by the '
        'recovery walk, not left behind under its own name and not deleted')


# ---------------------------------------------------------------------------
# 7. Every pre-promotion failure ends with the old state reinstalled.
# ---------------------------------------------------------------------------

def test_invalid_staging_leaves_old_live_reopenable(tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    fake_staging = os.path.join(c.root, pl.STAGING_DIRNAME)
    os.makedirs(fake_staging, exist_ok=True)  # empty: not a real artifact
    validation = pl.begin_swap(fake_staging, c.source_paths, [])
    assert not validation.ok
    assert validation.reason == pl.VALIDATION_INVALID_STAGING
    assert not os.path.isdir(fake_staging)
    assert open_index(c.live_dir) is not None, 'old live must still open'


def test_a_transient_staging_open_failure_does_not_discard_a_finished_build(
        tmp_path, monkeypatch):
    """A single flaky open of a genuinely valid staging artifact must not be
    judged invalid -- and must not be deleted -- before the same bounded
    retry every other reload check in this module gets."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'

    stub, calls = _flaky_open_index(built.staging_dir, fail_times=1)
    monkeypatch.setattr(pl, 'open_index', stub)

    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)

    assert validation.ok, validation.reason
    assert calls['n'] >= 2
    assert os.path.isdir(built.staging_dir), (
        'a candidate must not be deleted before the retry gives up on it')


def test_corpus_changed_mismatch_discards_staging_leaves_live_untouched(
        tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    # Corpus changes on disk AFTER the build read it.
    with open(c.corpus_path, 'a', encoding='utf-8') as fh:
        fh.write('==> extra0000 <==\nאבגדהוזחטיכלמנסעפצקרשת\n')

    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert not validation.ok
    assert validation.reason == pl.VALIDATION_CORPUS_CHANGED
    assert not os.path.isdir(built.staging_dir)
    assert open_index(c.live_dir) is not None


def test_rename_blocked_on_rebuild_rolls_back_to_old_live(tmp_path,
                                                           monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename

    def _blocked_rename(src, dst):
        # Allow live->prev (the first rename), block staging->live (the
        # second) -- this is the mid-swap block scenario.
        if os.path.normpath(src) == os.path.normpath(built.staging_dir):
            raise PermissionError('synthetic: staging->live blocked')
        return real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _blocked_rename)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'rename_blocked'
    assert open_index(live) is not None, 'rollback must restore the old live'
    assert not os.path.isdir(built.staging_dir), (
        'a blocked promotion whose restore SUCCEEDED must still clean its '
        'staging -- nothing downstream (the terminal recovery walk never '
        'looks inside passage_index.building) will ever do it otherwise')


def test_blocked_promotion_with_successful_restore_cleans_staging_end_to_end(
        tmp_path, monkeypatch):
    """Same scenario as the test above, driven through the full
    `run_build_and_swap` entry point: the caller must get back a WORKING
    index (the terminal recovery walk reopens the restored `live`) and the
    validated staging artifact must not be left stranded on disk."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    staging = os.path.join(c.root, pl.STAGING_DIRNAME)

    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename

    def _blocked_rename(src, dst):
        if os.path.normpath(src) == os.path.normpath(staging):
            raise PermissionError('synthetic: staging->live blocked')
        return real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _blocked_rename)

    res = pl.run_build_and_swap(
        c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
        release_live_state=pl.close_passage_state)

    assert res.status == 'rename_blocked'
    assert res.index is not None, (
        'a promotion block with a successful restore must still hand the '
        'caller a working index')
    assert not os.path.isdir(staging), (
        'a validated staging artifact must not be stranded on disk once '
        'the swap has definitively failed and the old generation was '
        'restored')
    pl.install_passage_state(
        pl.PassageState(index=res.index, live_dir=res.live_dir))


def test_cancelled_latch_before_any_rename_leaves_live_untouched(
        tmp_path, monkeypatch):
    """The NAME of this test is a claim about RENAMES, so it has to watch
    renames. It used to assert only that `live` was openable at the end --
    which cannot tell "never renamed" apart from "renamed aside and rolled
    back", because the rollback path leaves `live` openable too and passed
    this test just as happily (Codex review round 7, finding 2)."""
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    pl.close_passage_state()

    renames = []
    _real_rename = pl._retry_rename

    def _spy(src, dst):
        renames.append((src, dst))
        return _real_rename(src, dst)

    monkeypatch.setattr(pl, '_retry_rename', _spy)

    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=lambda: True)

    assert swap.status == 'cancelled'
    assert renames == [], (
        'a swap that was already cancelled renamed something anyway: %r'
        % (renames,))
    assert not os.path.isdir(built.staging_dir)
    assert open_index(c.live_dir) is not None


def test_cancelled_latch_on_a_first_build_leaves_no_live_promoted(tmp_path):
    """FIRST build: no `live/` exists, so `perform_swap` routes to
    `_swap_first_build`. A latch already set before `perform_swap` is even
    entered is caught by `perform_swap`'s own entry check, before
    `_swap_first_build` is reached at all."""
    c = _Corpus(tmp_path)
    assert not os.path.isdir(c.live_dir)

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=lambda: True)

    assert swap.status == 'cancelled'
    assert not os.path.isdir(built.staging_dir), 'staging must be discarded'
    assert not os.path.isdir(c.live_dir), (
        'a cancelled FIRST build must never promote staging to live')


def test_cancel_race_before_first_build_promotion_rename_is_still_caught(
        tmp_path):
    """The latch is NOT set when `perform_swap`'s entry check runs, but IS
    set by the time `_swap_first_build` reaches its own destructive rename --
    proving the re-check immediately before THAT rename, not merely the entry
    check, is what stands between a race and a promoted first build."""
    c = _Corpus(tmp_path)
    assert not os.path.isdir(c.live_dir)

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    cancel = _cancel_after(2)  # False on perform_swap's entry check, True after
    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=cancel)

    assert swap.status == 'cancelled'
    assert not os.path.isdir(built.staging_dir), 'staging must be discarded'
    assert not os.path.isdir(c.live_dir), (
        'a cancel racing the promotion rename must never let it land')


# ---------------------------------------------------------------------------
# 7b. `_reopen_live` directly -- every pre-promotion failure above routes
#     through it, but nothing exercised its own three outcomes in isolation.
# ---------------------------------------------------------------------------

def test_reopen_live_returns_a_usable_index_for_a_healthy_live(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()  # release before _reopen_live re-opens the same dir

    result = pl._reopen_live(c.root)
    assert result.status == 'reinstalled'
    assert result.index is not None
    assert result.index.n_records == len(c.records)
    assert result.live_dir == c.live_dir


def test_reopen_live_retries_a_transient_failure_rather_than_giving_up(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()

    stub, calls = _flaky_open_index(c.live_dir, fail_times=1)  # one flake, then real
    monkeypatch.setattr(pl, 'open_index', stub)

    result = pl._reopen_live(c.root)
    assert result.status == 'reinstalled'
    assert result.index is not None
    assert calls['n'] >= 2, 'a single flaky open must not be treated as unopenable'


def test_reopen_live_reports_no_prior_state_when_genuinely_unopenable(
        tmp_path):
    c = _Corpus(tmp_path)
    os.makedirs(c.live_dir)  # present but not a real artifact -- open_index fails
    result = pl._reopen_live(c.root)
    assert result.status == 'no_prior_state'
    assert result.index is None


# ---------------------------------------------------------------------------
# 8. Corpus manifest differs pre/post build -> staging discarded, live
#    untouched (same as one of the tests above, kept here as its own item
#    per the acceptance list, using run_build_and_swap end to end).
# ---------------------------------------------------------------------------

def test_end_to_end_corpus_drift_during_build_discards_staging(tmp_path,
                                                                monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    # Simulate the corpus changing WHILE build_index runs: monkeypatch
    # source_manifest to return the pre-build fingerprint on its first call
    # (inside execute_build) and a DIFFERENT one on the second (inside
    # begin_swap) without touching the file at all.
    calls = {'n': 0}
    real = pl.source_manifest

    def _drifting(paths, cancel_check=None):
        calls['n'] += 1
        out = real(paths, cancel_check=cancel_check)
        if calls['n'] >= 2:
            out = [dict(out[0], sha256='deadbeef' * 8)]
        return out

    monkeypatch.setattr(pl, 'source_manifest', _drifting)
    res2 = c.build()
    assert res2.status == pl.VALIDATION_CORPUS_CHANGED
    # Assert on the function's OWN returned index/live_dir, not a side-channel
    # open_index(c.live_dir) -- a broken `_reopen_live` wiring that always
    # returns a state-less result must fail HERE.
    assert res2.index is not None, (
        'run_build_and_swap must itself report a usable reopened index')
    assert res2.index.n_records == len(c.records)
    assert res2.live_dir == c.live_dir


# ---------------------------------------------------------------------------
# 9. Recovery at startup.
# ---------------------------------------------------------------------------

def test_recovery_live_unopenable_falls_back_to_valid_prev_generation(
        tmp_path, monkeypatch):
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'  # creates a _prev-* generation
    pl.close_passage_state()  # release live's handles before mutating it

    # Corrupt live for real.
    os.remove(os.path.join(c.live_dir, 'manifest.json'))
    # A leftover quarantine must survive being tried and NOT be deleted by
    # this promotion.
    quarantine = os.path.join(c.root, f'{pl.FAILED_PREFIX}sentinel')
    os.makedirs(quarantine)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_prev'
    assert result.index is not None
    assert open_index(c.live_dir) is not None
    assert os.path.isdir(quarantine), 'an untried quarantine must survive'


def test_recovery_newest_generation_corrupt_older_one_valid(tmp_path,
                                                             monkeypatch):
    c = _Corpus(tmp_path)
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'
    prevs = sorted(n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX))
    assert len(prevs) == 2, prevs
    # Corrupt the NEWER one (higher gen token -> sorts later ascending).
    newest = sorted(prevs, key=lambda n: pl._gen_sort_key(n, pl.PREV_PREFIX))[-1]
    os.remove(os.path.join(c.root, newest, 'manifest.json'))
    pl.close_passage_state()  # release live's handles before removing it
    shutil.rmtree(c.live_dir)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_prev'
    assert open_index(c.live_dir) is not None
    # The corrupt newest generation is swept away with the other _prev-*.
    remaining = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert remaining == [], remaining


def test_recovery_no_live_no_generation_only_quarantine_is_tried(tmp_path):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    quarantine = os.path.join(c.root, f'{pl.FAILED_PREFIX}{pl._gen_token()}')
    os.rename(built.staging_dir, quarantine)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_quarantine'
    assert open_index(c.live_dir) is not None


def test_recovery_transient_open_failure_does_not_downgrade_valid_live(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    stub, calls = _flaky_open_index(c.live_dir, fail_times=1)
    monkeypatch.setattr(pl, 'open_index', stub)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'live_ok'
    assert result.index is not None
    assert calls['n'] >= 2


def test_recovery_two_transient_open_failures_does_not_downgrade_valid_current(
        tmp_path, monkeypatch):
    """MEDIUM-2: `fail_times=1` above clears within `_open_with_retry`'s
    own two immediate attempts at the top of the loop, so it never reaches
    the destroy branch at all -- it proves nothing about what happens once
    `current/` has ALREADY failed both of those. This is the two-failure
    case the reviewer's repro actually hit: a valid `_prev-*` generation
    sits right behind a `current/` that is genuinely fine but hit two
    consecutive access-denied errors (an ordinary virus-scanner event) --
    old code treated that as proof of corruption, moved `current/` to
    `.dead-*`, and promoted the STALE `_prev-*` over it. The fix re-opens
    `current/` again -- more attempts, real backoff -- before ever
    destroying it."""
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    monkeypatch.setattr(pl, 'CURRENT_DESTROY_RECONFIRM_BACKOFF_SECONDS', 0.01)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'  # creates one _prev-* generation
    # `fail_times` is DERIVED, not the literal 2 it used to be. With 2,
    # `_open_with_retry`'s own default attempts absorb both failures and
    # the re-confirmation's very FIRST try succeeds -- so the test passed
    # for any margin >= 1 and only caught the step being deleted outright.
    # Weakening 5 -> 1 was invisible (adversarial audit 2026-08-26).
    # Failing every attempt but the last one the shipped margin buys makes
    # the WHOLE margin load-bearing, and tracks the constants if either is
    # ever legitimately changed.
    stub, calls = _flaky_open_index(c.live_dir,
                                    fail_times=_RECONFIRM_TOTAL() - 1)
    monkeypatch.setattr(pl, 'open_index', stub)

    # `current/` must never be moved aside -- the claim this test is named
    # for. Watching for a leftover `.dead-*` directory could not check it:
    # that directory is rmtree'd two lines after it is created, so the
    # assertion was true whatever the code did.
    renames = []
    _real_rename = pl._retry_rename

    def _spy(src, dst):
        renames.append((src, dst))
        return _real_rename(src, dst)

    monkeypatch.setattr(pl, '_retry_rename', _spy)

    result = pl.recover_at_startup(c.root)

    assert result.status == 'live_ok', (
        f'a valid current/ was downgraded to {result.status!r} after '
        'transient open failures -- the re-confirmation before destroying '
        'it either did not run or gave up too soon')
    assert result.index is not None
    assert calls['n'] == _RECONFIRM_TOTAL(), (
        'the re-confirmation must have spent its FULL margin and succeeded '
        'on the last attempt it buys (%d calls); %d means the margin is '
        'wider than anything this test actually requires, and could be cut '
        'without failing it' % (_RECONFIRM_TOTAL(), calls['n']))
    assert not any(os.path.normpath(src) == os.path.normpath(c.live_dir)
                   for src, _ in renames), (
        'current/ was moved aside; for this to count as a fix rather than a '
        'lucky recovery from one, it must never have been renamed at all: '
        '%r' % (renames,))
    # The still-genuinely-valid _prev-* generation must be swept away, same
    # as the ordinary all-first-try-open live_ok path -- proving this is
    # not merely surviving by accident (e.g. falling through to a
    # different, un-cleaned branch).
    remaining_prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert remaining_prevs == [], remaining_prevs


def _RECONFIRM_TOTAL():
    """Every open_index call a genuinely-valid `current/` gets before the
    recovery walk is allowed to destroy it: `_open_with_retry`'s own default
    attempts at the top of the loop, plus the wider re-confirmation. Read
    from the signature and the constant rather than hardcoded, so a test
    built on it cannot silently drift away from the code it guards."""
    default = inspect.signature(
        pl._open_with_retry).parameters['attempts'].default
    return default + pl.CURRENT_DESTROY_RECONFIRM_ATTEMPTS


def test_a_weakened_reconfirm_margin_does_downgrade_a_valid_current(
        tmp_path, monkeypatch):
    """The companion that proves the margin is LOAD-BEARING rather than
    merely present. `CURRENT_DESTROY_RECONFIRM_ATTEMPTS` is cut to 1 -- the
    weakening that used to pass the whole suite -- against the same corpus
    the test above recovers cleanly, and the valid `current/` is destroyed
    and replaced by the stale generation behind it. Without this, "the
    constant is 5" is an assertion no test makes."""
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    monkeypatch.setattr(pl, 'CURRENT_DESTROY_RECONFIRM_BACKOFF_SECONDS', 0.01)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'  # creates one _prev-* generation

    shipped_total = _RECONFIRM_TOTAL()
    stub, calls = _flaky_open_index(c.live_dir, fail_times=shipped_total - 1)
    monkeypatch.setattr(pl, 'open_index', stub)
    monkeypatch.setattr(pl, 'CURRENT_DESTROY_RECONFIRM_ATTEMPTS', 1)

    renames = []
    _real_rename = pl._retry_rename

    def _spy(src, dst):
        renames.append((src, dst))
        return _real_rename(src, dst)

    monkeypatch.setattr(pl, '_retry_rename', _spy)

    result = pl.recover_at_startup(c.root)

    assert result.status != 'live_ok', (
        'a re-confirmation margin of 1 still rode out %d consecutive '
        'transient failures -- then the margin the shipped constant buys is '
        'not what makes the sibling test pass, and cutting it would go '
        'unnoticed' % (shipped_total - 1))
    # The DIRECT claim, and the one that does not depend on what the walk
    # manages afterwards: the still-valid `current/` was moved aside. (What
    # it ends up as varies -- with the stub's remaining failures the
    # promoted generation cannot be opened either, so the walk runs out of
    # candidates entirely rather than settling on `recovered_from_prev`.
    # Either way `current/` is gone, which is the regression.)
    assert any(os.path.normpath(src) == os.path.normpath(c.live_dir)
               for src, _ in renames), (
        'the weakened margin did not actually destroy current/, so this '
        'test is not exercising the downgrade it claims: %r' % (renames,))


def test_recovery_reconsiders_a_promoted_candidate_after_a_transient_reopen_failure(
        tmp_path, monkeypatch):
    """A `_prev-*` candidate that validates in place and gets promoted into
    `current/` must not be abandoned just because its immediate post
    -promotion reopen hits a transient error -- recovery must retry THAT
    directory before ever reporting nothing_recoverable, since the content
    sitting there is already proven to work."""
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'  # creates the one _prev-* generation
    pl.close_passage_state()  # release live's handles before removing it
    shutil.rmtree(c.live_dir)  # no 'live' candidate -- forces the _prev-* path

    # Fails the first two calls against `live` (both attempts inside the
    # post-promotion `_open_with_retry`), then defers to the real opener --
    # models a flake that has cleared by the time recovery checks again.
    stub, calls = _flaky_open_index(c.live_dir, fail_times=2)
    monkeypatch.setattr(pl, 'open_index', stub)

    result = pl.recover_at_startup(c.root)

    assert result.status == 'recovered_from_prev', (
        'the promoted candidate must be reconsidered, not abandoned, once '
        'its transient reopen failure clears')
    assert result.index is not None
    assert calls['n'] >= 3, (
        'the retry must actually have happened -- otherwise this test '
        'proves nothing')
    # Independent of the mock: current/ genuinely opens now -- the exact
    # fact the reviewer's repro showed recovery denying.
    assert open_index(c.live_dir) is not None


def test_recovery_nothing_recoverable_reports_cleanly(tmp_path):
    c = _Corpus(tmp_path)
    os.makedirs(c.root, exist_ok=True)
    result = pl.recover_at_startup(c.root)
    assert result.status == 'nothing_recoverable'
    assert result.index is None


# ---------------------------------------------------------------------------
# 10. Quarantine matrix (rollback quarantine deleted; no-proven-copy
#     quarantine kept until a later proven build).
# ---------------------------------------------------------------------------

def test_kept_quarantine_survives_until_a_later_proven_build(tmp_path,
                                                              monkeypatch):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok
    stub, _calls = _flaky_open_index(c.live_dir, fail_times=2)
    monkeypatch.setattr(pl, 'open_index', stub)
    swap = pl.perform_swap(c.root, built.staging_dir)
    assert swap.status == 'reload_failed_first_install'
    quarantine = swap.quarantine_dir
    assert os.path.isdir(quarantine)

    # An UNRELATED successful build later must sweep it.
    monkeypatch.undo()
    res = c.build()
    assert res.status == 'installed'
    _join_background_threads(res, extra_seconds=0.8)
    assert not os.path.isdir(quarantine), 'a later proven build must sweep it'


# ---------------------------------------------------------------------------
# 11. Lock.
# ---------------------------------------------------------------------------

def test_a_second_in_process_acquire_is_refused(tmp_path):
    lock1 = pl.acquire_lock(str(tmp_path))
    assert lock1 is not None
    try:
        lock2 = pl.acquire_lock(str(tmp_path))
        assert lock2 is None
    finally:
        pl.release_lock(lock1)


def test_lock_released_after_a_release_lock_call(tmp_path):
    lock1 = pl.acquire_lock(str(tmp_path))
    pl.release_lock(lock1)
    lock2 = pl.acquire_lock(str(tmp_path))
    assert lock2 is not None
    pl.release_lock(lock2)


def test_lock_released_on_a_build_error(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('boom')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        res = c.build()
    finally:
        pl.build_index = orig
    assert res.status == 'error'
    lock = pl.acquire_lock(c.root)
    assert lock is not None, 'lock must be released after a build error'
    pl.release_lock(lock)


def test_lock_released_on_a_thread_start_style_failure(tmp_path, monkeypatch):
    """Any exception raised before the worker's own body even starts must
    still release the lock -- `run_build_and_swap`'s `finally` covers the
    WHOLE run, not only the paths this module explicitly names."""
    c = _Corpus(tmp_path)

    def _explode(root, corpus_path):
        raise RuntimeError('synthetic thread-start failure')

    monkeypatch.setattr(pl, 'prepare_staging', _explode)
    with pytest.raises(RuntimeError):
        c.build()
    lock = pl.acquire_lock(c.root)
    assert lock is not None, 'lock must be released even on an early raise'
    pl.release_lock(lock)


def test_startup_recovery_skips_when_lock_unavailable(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    held = pl.acquire_lock(c.root)
    try:
        before = sorted(os.listdir(c.root))
        result = pl.recover_at_startup(c.root)
        assert result.status == 'locked'
        assert result.index is None
        after = sorted(os.listdir(c.root))
        assert before == after, 'a skipped recovery must touch nothing'
    finally:
        pl.release_lock(held)


def _lock_holder_script() -> str:
    return (
        "import sys, time\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "import desktop.passage_lifecycle as pl\n"
        "lock = pl.acquire_lock(sys.argv[1])\n"
        "print('LOCKED' if lock is not None else 'FAILED', flush=True)\n"
        "time.sleep(30)\n"
    )


def test_lock_refuses_a_real_second_process_and_os_releases_it_on_kill(
        tmp_path):
    root = str(tmp_path)
    proc = subprocess.Popen([sys.executable, '-c', _lock_holder_script(), root],
                            stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'LOCKED', line
        # Our own process must now be refused.
        ours = pl.acquire_lock(root)
        assert ours is None
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # The OS must release the lock the instant the holder dies -- poll
    # briefly rather than assuming instantaneous propagation.
    deadline = time.time() + 5
    got = None
    while time.time() < deadline:
        got = pl.acquire_lock(root)
        if got is not None:
            break
        time.sleep(0.1)
    assert got is not None, 'lock was not released after the holder was killed'
    pl.release_lock(got)


def _mmap_holder_script() -> str:
    return (
        "import sys, time\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "from shared.passage_index import open_index\n"
        "idx = open_index(sys.argv[1])\n"
        "print('MAPPED' if idx is not None else 'FAILED', flush=True)\n"
        "time.sleep(30)\n"
    )


@pytest.mark.skipif(
    sys.platform != 'win32',
    reason='os.rename over a directory another process has memory-mapped '
           'only raises PermissionError on Windows -- POSIX rename() is a '
           'metadata-only operation unaffected by open file handles inside '
           'the directory, so this is a Windows filesystem property, not a '
           'code property, and the sibling test below pins the POSIX side')
def test_a_second_process_mapped_reader_causes_a_persistent_rename_block(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    proc = subprocess.Popen(
        [sys.executable, '-c', _mmap_holder_script(), live],
        stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'MAPPED', line
        time.sleep(0.3)

        built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
        assert built.status == 'built'
        validation = pl.begin_swap(built.staging_dir, c.source_paths,
                                   built.pre_build_manifest)
        assert validation.ok

        monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 3)
        monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0.1)
        pl.close_passage_state()
        swap = pl.perform_swap(c.root, built.staging_dir)
        assert swap.status == 'rename_blocked', swap.status
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # Once the reader is gone, the SAME live directory must still open fine
    # -- the block cost nothing permanent.
    assert open_index(live) is not None


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='pins the POSIX side of the scenario above: renaming a directory '
           "out from under another process's open mmap is NOT blocked on "
           'POSIX, so the rebuild must complete rather than roll back')
def test_a_second_process_mapped_reader_does_not_block_rename_on_posix(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    proc = subprocess.Popen(
        [sys.executable, '-c', _mmap_holder_script(), live],
        stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'MAPPED', line
        time.sleep(0.3)

        built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
        assert built.status == 'built'
        validation = pl.begin_swap(built.staging_dir, c.source_paths,
                                   built.pre_build_manifest)
        assert validation.ok

        pl.close_passage_state()
        swap = pl.perform_swap(c.root, built.staging_dir)
        assert swap.status == 'installed', swap.status
    finally:
        proc.kill()
        proc.wait(timeout=10)

    assert open_index(live) is not None


# ---------------------------------------------------------------------------
# 12. Cancelled latch: a cancel landing after the build but before the swap
#     still prevents the swap (covered above as
#     test_cancelled_latch_before_any_rename_leaves_live_untouched; this adds
#     the run_build_and_swap end-to-end variant).
# ---------------------------------------------------------------------------

def test_end_to_end_cancel_between_build_and_swap_prevents_the_swap(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live_before = open_index(c.live_dir).manifest

    # cancel_check that is False during the build and True from then on --
    # models "cancel requested right after the build finished".
    state = {'building_done': False}

    def cancel():
        return state['building_done']

    orig_execute_build = pl.execute_build

    def _tracking_execute_build(*a, **k):
        result = orig_execute_build(*a, **k)
        state['building_done'] = True
        return result

    import unittest.mock as mock
    with mock.patch.object(pl, 'execute_build', _tracking_execute_build):
        res = c.build(cancel_check=cancel)

    assert res.status == 'cancelled'
    # Assert on the function's OWN returned index, not a side-channel
    # open_index(c.live_dir) -- a broken `_reopen_live` wiring that always
    # returns a state-less result must fail HERE.
    assert res.index is not None, (
        'run_build_and_swap must itself report a usable reopened index')
    assert res.index.manifest == live_before, (
        'live must be byte-for-byte the pre-rebuild artifact')
    assert res.live_dir == c.live_dir


# ---------------------------------------------------------------------------
# 12b. Two-phase swap handoff: `run_build_and_swap` must not mutate `_state`
#      itself -- it only ever requests the release through the injected seam.
#      The Windows test below is the actual safety gate on that: the OS
#      itself refuses the rename while the seam left old handles open. Its
#      POSIX sibling is NOT a gate -- POSIX rename() has no such refusal to
#      exercise, so that test only documents the accepted platform gap; see
#      its own docstring before mistaking it for coverage of the same
#      property.
# ---------------------------------------------------------------------------

def test_run_build_and_swap_uses_the_injected_seam_not_close_passage_state(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    orig_close = pl.close_passage_state
    close_calls = []

    def _tracking_close():
        close_calls.append(1)
        return orig_close()

    monkeypatch.setattr(pl, 'close_passage_state', _tracking_close)

    seam_calls = []

    def _seam(expect_generation=None):
        seam_calls.append(1)
        orig_close()  # the real release, driven by the seam -- not by the
                      # module reaching for close_passage_state on its own

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=_seam)
    assert res2.status == 'installed'
    if res2.index is not None:
        pl.install_passage_state(
            pl.PassageState(index=res2.index, live_dir=res2.live_dir))
    assert seam_calls == [1]
    assert close_calls == [], (
        'run_build_and_swap must never call close_passage_state directly '
        '-- only the injected seam may release _state')


def test_run_build_and_swap_seam_that_never_returns_times_out_and_frees_the_lock(
        tmp_path, monkeypatch):
    """MEDIUM-1: `release_live_state` runs with the cross-process lock
    already held (see `run_build_and_swap`'s docstring) -- a seam whose
    event loop never answers (a wedged or gone UI thread) must not be
    allowed to hold that lock forever, or every future build and startup
    recovery in every process is locked out behind a worker thread that is
    never coming back. Modelled with a seam that blocks on an `Event` this
    test never sets -- the timeout must fire, staging must still be
    cleaned, the OLD live index must stay installed (nothing was proven
    released, so nothing may be renamed), and the status must be distinct
    from `readers_active`, which means something else (a lease was SEEN
    and refused to drain -- here, nothing about the outcome is known at
    all)."""
    monkeypatch.setattr(pl, 'UI_RELEASE_SEAM_TIMEOUT_SECONDS', 0.2)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    staging = os.path.join(c.root, pl.STAGING_DIRNAME)

    never = threading.Event()

    def _wedged_seam(expect_generation=None):
        never.wait()  # never set within this test -- models a UI thread
        return True   # that never answers; unreachable here

    res = pl.run_build_and_swap(
        c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
        release_live_state=_wedged_seam)

    assert res.status == 'release_timed_out', res.status
    assert not os.path.isdir(staging), (
        'a swap abandoned on a seam timeout must still clean its staging')
    assert pl.passage_available(), (
        'a timed-out release must leave the OLD, still-live index '
        'installed -- nothing was proven released, so nothing may be '
        'renamed')

    # The cross-process lock itself must be free again -- proof the timeout
    # does not strand it -- checked by actually running a second build+swap
    # through to completion, not by poking at lock internals.
    never.set()  # let the abandoned thread finish; it is a daemon either way
    res2 = pl.run_build_and_swap(
        c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
        release_live_state=pl.close_passage_state)
    assert res2.status == 'installed', res2.status
    pl.install_passage_state(
        pl.PassageState(index=res2.index, live_dir=res2.live_dir))


@pytest.mark.skipif(
    sys.platform != 'win32',
    reason='the seam-skipped rename is only caught by the OS on Windows -- '
           'POSIX rename() does not care that this process still holds an '
           'open mmap on the directory being renamed, so a broken seam is '
           'NOT stopped there (a filesystem property, not a code property); '
           'the sibling test below pins the POSIX side')
def test_run_build_and_swap_relies_on_the_seam_actually_releasing(tmp_path):
    """A seam that does nothing must leave the OLD live's handles open --
    proving the rename that follows depends on the seam's own effect, not on
    some direct release the worker performs behind its back."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert pl.passage_available()

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=lambda _gen=None: None)
    assert res2.status == 'rename_blocked', res2.status
    assert pl.passage_available(), (
        'a no-op seam must leave the old state exactly as installed')
    pl.close_passage_state()


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='pins the POSIX side of the scenario above: a broken seam is NOT '
           'caught by the OS here, so the rebuild goes through regardless '
           'of the still-open old mmap')
def test_run_build_and_swap_posix_gap_broken_seam_is_not_caught_no_safety_gate(
        tmp_path):
    """NOT a safety gate, and cannot be made into one -- read it as the
    inverse of `test_run_build_and_swap_relies_on_the_seam_actually_
    releasing` above, not as its POSIX equivalent. That test passes BECAUSE
    Windows' own os.rename() refuses to rename a directory under an open
    mmap, which is what makes a broken seam observable there at all. POSIX
    rename() carries no such refusal -- it doesn't care what file handles
    are open under the target -- so there is nothing in this codebase for
    this test to catch a broken seam WITH on this platform: `res2.status
    == 'installed'` below is the rebuild succeeding regardless of whether
    the seam did anything, which is exactly the gap, not a property being
    verified. Kept only so that gap is a documented, asserted fact instead
    of a surprise discovered later -- if this assertion ever starts
    failing, POSIX rename() has changed underneath this test, not this
    module's code."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert pl.passage_available()

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=lambda _gen=None: None)
    assert res2.status == 'installed', res2.status
    pl.close_passage_state()


# ---------------------------------------------------------------------------
# 13. Freshness.
# ---------------------------------------------------------------------------

def test_freshness_missing_corpus_sources_yields_unknown():
    assert pl.compute_freshness([], ['/does/not/matter']) == pl.UNKNOWN


def test_freshness_fresh_and_stale(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build()
    assert res.status == 'installed'
    sources = res.index.manifest['corpus']['sources']
    assert sources, 'fixture must actually carry a corpus fingerprint'

    assert pl.compute_freshness(sources, c.source_paths) == pl.FRESH
    with open(c.corpus_path, 'a', encoding='utf-8') as fh:
        fh.write('==> extra0000 <==\nאבגדהוזחטיכלמנסעפצקרשת\n')
    assert pl.compute_freshness(sources, c.source_paths) == pl.STALE


def test_freshness_result_computed_against_a_since_changed_artifact_is_discarded():
    token = pl.start_freshness_check()
    # The artifact was replaced (e.g. install_passage_state ran) WHILE this
    # check was in flight -- its token is now stale.
    pl.install_passage_state(None)
    pl.finish_freshness_check(token, pl.FRESH)
    snap = pl.freshness_snapshot()
    assert snap.result is None, 'a stale-token result must be discarded'


def test_freshness_banner_clears_after_a_successful_rebuild(tmp_path):
    token = pl.start_freshness_check()
    pl.finish_freshness_check(token, pl.STALE)
    assert pl.freshness_snapshot().result == pl.STALE

    c = _Corpus(tmp_path)
    res = c.build()
    pl.install_passage_state(pl.PassageState(index=res.index,
                                             live_dir=res.live_dir))
    snap = pl.freshness_snapshot()
    assert snap.result is None
    assert snap.checking is False


def test_freshness_cancelled_check_leaves_nothing_stuck(tmp_path):
    real_file = tmp_path / 'corpus.txt'
    real_file.write_text('placeholder', encoding='utf-8')
    token = pl.start_freshness_check()
    assert pl.freshness_snapshot().checking is True
    result = pl.compute_freshness([{'path': 'x', 'bytes': 1, 'sha256': 'x'}],
                                  [str(real_file)], cancel_check=lambda: True)
    assert result is None
    pl.finish_freshness_check(token, result)
    snap = pl.freshness_snapshot()
    assert snap.checking is False
    assert snap.result is None


# ---------------------------------------------------------------------------
# 14. Free-space preflight: the letters upper bound, never raw bytes.
# ---------------------------------------------------------------------------

def test_estimate_build_bytes_uses_letters_upper_bound_not_raw_bytes(
        tmp_path):
    corpus = tmp_path / 'corpus.txt'
    corpus.write_bytes(b'x' * 10_000)
    n_bytes = 10_000
    letters_ub = n_bytes // 2

    from shared.passage_builder import estimate_artifact_bytes
    expected = estimate_artifact_bytes(letters_ub) + 8 * letters_ub
    raw_bytes_formula = estimate_artifact_bytes(n_bytes) + 8 * n_bytes

    got = pl.estimate_build_bytes(str(corpus))
    assert got == expected
    assert got < raw_bytes_formula, (
        'using raw file bytes as n_letters roughly doubles the demand')


def test_prepare_staging_points_free_space_check_at_the_parent(tmp_path,
                                                                monkeypatch):
    c = _Corpus(tmp_path)
    seen = {}

    def _capture(index_dir, needed_bytes):
        seen['dir'] = index_dir
        seen['needed'] = needed_bytes

    monkeypatch.setattr(pl, 'check_free_space', _capture)
    pl.prepare_staging(c.root, c.corpus_path)
    assert seen['dir'] == c.root
    assert not os.path.isdir(c.live_dir), (
        'preflight must never create the live directory'
    )
    assert seen['needed'] == pl.estimate_build_bytes(c.corpus_path)


def test_prepare_staging_clears_a_stale_tree_before_preflighting(tmp_path,
                                                                  monkeypatch):
    """Preflight-after-clear, not before: a crashed staging tree eating the
    headroom must not permanently refuse every retry."""
    c = _Corpus(tmp_path)
    stale = os.path.join(c.root, pl.STAGING_DIRNAME)
    os.makedirs(stale)
    with open(os.path.join(stale, 'junk.bin'), 'wb') as fh:
        fh.write(b'0' * 1000)

    order = []
    orig_check = pl.check_free_space

    def _tracking_check(index_dir, needed_bytes):
        order.append(('preflight', os.path.isdir(stale)))
        return orig_check(index_dir, needed_bytes)

    monkeypatch.setattr(pl, 'check_free_space', _tracking_check)
    pl.prepare_staging(c.root, c.corpus_path)
    assert order == [('preflight', False)], (
        'the stale tree must be gone BEFORE the preflight check runs')


# ---------------------------------------------------------------------------
# Task 4: the pure readiness gate.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    'passage_ready,scope,lab_on,build_in_flight,main_ready,expected', [
        # Scope wins over every other condition.
        (True, 'local', False, False, True, pl.REASON_SCOPE),
        (True, 'all', False, False, True, pl.REASON_SCOPE),
        (False, 'local', True, True, False, pl.REASON_SCOPE),
        # Genizah scope, one blocker at a time.
        (False, 'genizah', False, False, True, pl.REASON_NOT_BUILT),
        (True, 'genizah', True, False, True, pl.REASON_LAB_ACTIVE),
        (True, 'genizah', False, True, True, pl.REASON_BUILD_IN_FLIGHT),
        (True, 'genizah', False, False, False, pl.REASON_MAIN_INDEX_MISSING),
        # Fully ready.
        (True, 'genizah', False, False, True, None),
        # Precedence boundaries: not-built beats everything after it.
        (False, 'genizah', True, True, False, pl.REASON_NOT_BUILT),
        # lab-active beats build-in-flight and main-index-missing.
        (True, 'genizah', True, True, False, pl.REASON_LAB_ACTIVE),
        # build-in-flight beats main-index-missing.
        (True, 'genizah', False, True, False, pl.REASON_BUILD_IN_FLIGHT),
    ])
def test_passage_disabled_reason_precedence(passage_ready, scope, lab_on,
                                            build_in_flight, main_ready,
                                            expected):
    got = pl.passage_disabled_reason(passage_ready, scope, lab_on,
                                     build_in_flight, main_ready)
    assert got == expected


def test_passage_disabled_reason_full_truth_table_is_deterministic():
    """Every combination of the five inputs must return SOME value from the
    known reason set (or None) -- no crash, no unexpected key, for all 32
    combinations including every main-index-missing pairing."""
    known = {pl.REASON_SCOPE, pl.REASON_NOT_BUILT, pl.REASON_LAB_ACTIVE,
            pl.REASON_BUILD_IN_FLIGHT, pl.REASON_MAIN_INDEX_MISSING, None}
    for passage_ready, scope, lab_on, build_in_flight, main_ready in \
            itertools.product([True, False], ['genizah', 'local'],
                              [True, False], [True, False], [True, False]):
        got = pl.passage_disabled_reason(passage_ready, scope, lab_on,
                                         build_in_flight, main_ready)
        assert got in known


def test_passage_disabled_reason_reads_no_module_state(monkeypatch):
    """Every input is explicit -- poisoning the module's OWN globals must not
    change the answer.

    `monkeypatch.undo()` is called explicitly, right after `_state` is no
    longer needed, rather than left to the fixture's teardown ordering:
    `_reset_module_globals`'s post-yield `close_passage_state()` runs
    BEFORE monkeypatch's own restore on this pytest version, so leaving
    `_state` poisoned would hand `close_passage_state()` a bare string --
    `PassageIndex.close()` delegation (`_release_index_handles`) expects a
    real `PassageIndex` or None, not whatever a test left behind."""
    monkeypatch.setattr(pl, '_state', 'poisoned', raising=False)
    got = pl.passage_disabled_reason(True, 'genizah', False, False, True)
    monkeypatch.undo()
    assert got is None


# =========================================================================
# Codex review round 7. Three defects the wave-1 suite passed straight
# over, each pinned by the state it actually produces rather than by the
# state it happens to end up in.
# =========================================================================


def test_a_late_release_from_a_timed_out_seam_cannot_close_a_newer_index(
        tmp_path, monkeypatch):
    """Finding 1. `_call_release_seam` abandons a seam that does not answer
    in time -- but abandoning a thread does not stop it. Build A's wedged
    seam wakes up minutes later and runs its close against whatever `_state`
    holds BY THEN, which can be the index a SECOND build installed in the
    meantime. The feature then goes offline until restart, with nothing in
    the logs tying it to a build that failed long ago.

    The existing timeout test cannot see this: its wedged seam only returns
    `True` and never performs a close at all, and it is released BEFORE the
    second build rather than after it."""
    monkeypatch.setattr(pl, 'UI_RELEASE_SEAM_TIMEOUT_SECONDS', 0.2)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    gate = threading.Event()
    late = []

    def _wedged_seam(expect_generation=None):
        # Models a UI thread that is wedged past the timeout and then
        # finally drains its queue and performs the release for real.
        gate.wait(30)
        late.append(pl.close_passage_state(expect_generation))
        return True

    res_a = pl.run_build_and_swap(
        c.root, c.records, c.source_paths, c.corpus_path, partitions=2,
        release_live_state=_wedged_seam)
    assert res_a.status == 'release_timed_out', res_a.status
    assert pl.passage_available()

    # Build B succeeds while A's seam is still parked, and installs a
    # DIFFERENT index object.
    res_b = c.build()
    assert res_b.status == 'installed', res_b.status
    assert pl.passage_available()
    installed = pl._state.index

    # Only now does A's abandoned seam get to run its close.
    gate.set()
    deadline = time.time() + 30
    while not late and time.time() < deadline:
        time.sleep(0.02)
    assert late, 'the abandoned seam never ran -- the test proved nothing'

    assert late == [False], (
        'the late close was PERFORMED; it must be refused, because the '
        'generation it was authorised against is long gone')
    assert pl.passage_available(), (
        "build A's abandoned seam closed build B's index")
    assert pl._state.index is installed, 'the installed index was replaced'


def test_a_cancel_racing_the_rebuild_never_renames_the_live_index_aside(
        tmp_path, monkeypatch):
    """Finding 2. `perform_swap`'s entry check can pass and the latch be set
    a moment later, before `_swap_rebuild`'s first destructive rename. The
    rollback that used to be the only answer is not free: it is a SECOND
    rename, and on Windows it can itself fail with PermissionError -- which
    leaves `current/` missing entirely. Not renaming at all cannot fail.

    `_swap_first_build` already re-checked before ITS rename; the rebuild
    path did not, though `perform_swap`'s docstring claimed both did."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok
    pl.close_passage_state()

    renames = []
    _real_rename = pl._retry_rename

    def _spy(src, dst):
        renames.append((src, dst))
        return _real_rename(src, dst)

    monkeypatch.setattr(pl, '_retry_rename', _spy)

    # False for `perform_swap`'s entry check, True by the time the rebuild
    # path is about to rename -- exactly the race, made deterministic.
    calls = []

    def _cancel():
        calls.append(1)
        return len(calls) > 1

    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=_cancel)

    assert swap.status == 'cancelled', swap.status
    assert not any(src == c.live_dir for src, _ in renames), (
        'the live index was renamed aside by a swap that had already been '
        'cancelled, and only put back by a rollback that can itself fail: '
        '%r' % (renames,))
    assert not os.path.isdir(built.staging_dir)
    assert open_index(c.live_dir) is not None


def test_a_failed_post_build_fingerprint_still_cleans_staging(
        tmp_path, monkeypatch):
    """Finding 3. Only `BuildCancelled` cleaned staging on the way out of
    `begin_swap`. Re-reading the corpus can fail for entirely ordinary
    reasons -- a source file removed, a drive unmounted, a permission
    change -- and every one of those escaped to the worker leaving a
    multi-GB staging artifact on disk. Nothing downstream collects it: the
    startup recovery walk never looks inside `passage_index.building`."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built', built.status
    assert os.path.isdir(built.staging_dir)

    def _boom(paths, cancel_check=None):
        raise OSError(5, 'the corpus went away mid-swap')

    monkeypatch.setattr(pl, 'source_manifest', _boom)

    with pytest.raises(OSError):
        pl.begin_swap(built.staging_dir, c.source_paths,
                      built.pre_build_manifest)

    assert not os.path.isdir(built.staging_dir), (
        'a validated multi-GB staging artifact was stranded on disk by an '
        'ordinary failure of the post-build fingerprint')
