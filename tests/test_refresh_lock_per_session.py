"""Behavioral test for Phase 90 _refresh_user_session lock semantics (D-17).

Three deterministic tests verifying refresh-lock serialization,
parallelism across distinct sessions, and stale-snapshot short-circuit.
Uses threading.Barrier for deterministic ordering; monkeypatches
`web.safe_storage.app` to an instance-isolated stub (Phase 87 B3
pattern from tests/test_browse_state.py) for Tests A and C, OR to a
threading.local-routed _ThreadRoutedApp proxy for Test B (per-thread
storage isolation -- plan-checker round catch replacing the 5ms stagger
workaround). Avoids real NiceGUI storage contexts in worker threads.

See CONTEXT.md D-17 + D-06.
"""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


class _ThreadRoutedApp:
    """Per-thread storage routing for D-17 Test B (distinct-uuid parallelism).

    Each thread sees its own storage dict via threading.local. Mimics
    `nicegui.app.storage.user` interface used by safe_storage helpers
    (safe_user_get / safe_user_set / get_session_uuid all read from
    `web.safe_storage.app.storage.user`).

    Plan-checker round catch: replaces the 5ms time.sleep stagger
    workaround that was insufficient to prove real parallelism. With
    a real per-thread proxy + threading.Barrier(2), the two worker
    threads can actually overlap, and the ConcurrencyRecorder's
    max_concurrent reaches 2 ONLY when the per-uuid locks really
    serialize each uuid independently (not the global refresh path).
    """

    def __init__(self):
        self._local = threading.local()

    def bind(self, user_store):
        """Bind a per-thread user-storage dict. Call this inside each worker
        before any safe_storage-routed operation runs in that thread."""
        self._local.user = user_store

    @property
    def storage(self):
        # Return a fresh SimpleNamespace per access whose `.user` attribute
        # points at the calling thread's bound store. Threads that haven't
        # called bind() see an empty dict (defensive -- should never happen
        # in tests because workers bind() before crossing the barrier).
        if not hasattr(self._local, 'user'):
            self._local.user = {}
        return SimpleNamespace(user=self._local.user)


class _ConcurrencyRecorder:
    """Tracks max concurrent invocations of a mocked refresh_session.

    enter() and exit() are called by the mocked refresh_session via the
    side_effect; max_concurrent records the peak observed _active count.
    Test B uses max_concurrent == 2 to prove real parallelism (which
    call_count == 2 alone cannot prove -- call_count passes even when
    calls are perfectly serialized).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._active = 0
        self.max_concurrent = 0
        self.call_count = 0

    def enter(self):
        with self._lock:
            self._active += 1
            if self._active > self.max_concurrent:
                self.max_concurrent = self._active
            self.call_count += 1

    def exit(self):
        with self._lock:
            self._active -= 1


def _make_mock_throwaway(recorder, new_access_token, new_refresh_token, hold_ms=20):
    """Build a mock for create_client(...) whose .auth.refresh_session(...)
    records concurrency, sleeps briefly, then returns a session with the
    new tokens. The hold_ms ensures concurrent threads actually overlap
    if not serialized -- Test B uses 50ms to maximize the parallel window."""
    def fake_create_client(url, key):
        client = MagicMock()

        def fake_refresh_session(refresh_token):
            recorder.enter()
            try:
                time.sleep(hold_ms / 1000.0)
                resp = MagicMock()
                resp.session = SimpleNamespace(
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                )
                return resp
            finally:
                recorder.exit()

        client.auth.refresh_session.side_effect = fake_refresh_session
        return client

    return fake_create_client


def _seed_storage_with_expiring_token(store, session_uuid):
    """Seed an auth_session whose access_token has exp in the past
    (forces _access_token_near_expiry -> True). Sets _session_uuid too."""
    import base64
    import json
    # JWT with exp = 0 (1970). Header + payload + bogus signature.
    header = base64.urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').rstrip(b'=').decode()
    payload = base64.urlsafe_b64encode(json.dumps({'exp': 0}).encode()).rstrip(b'=').decode()
    access_token = f"{header}.{payload}.sig"
    store['auth_session'] = {
        'access_token': access_token,
        'refresh_token': 'rt-original',
    }
    store['_session_uuid'] = session_uuid


@pytest.fixture(autouse=True)
def _reset_refresh_locks():
    """Reset _refresh_locks between tests to avoid lock state bleeding across cases."""
    import web.supabase_client as mod
    mod._refresh_locks.clear()
    yield
    mod._refresh_locks.clear()


def test_a_same_uuid_serialization(monkeypatch):
    """Two threads of same _session_uuid -> only ONE refresh fires; max_concurrent == 1."""
    import web.supabase_client as mod

    uuid_a = 'a' * 32
    store = {}
    _seed_storage_with_expiring_token(store, uuid_a)

    # Stub the safe_storage app handle so safe_user_get/set/get_session_uuid
    # all route to our isolated dict (shared by both threads -- same uuid).
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=store)),
    )

    recorder = _ConcurrencyRecorder()
    monkeypatch.setattr(
        mod, 'create_client',
        _make_mock_throwaway(recorder, 'at-new', 'rt-new', hold_ms=50),
    )

    barrier = threading.Barrier(2)
    results = []
    results_lock = threading.Lock()

    def worker():
        barrier.wait()
        res = mod._refresh_user_session(stale_refresh_token='rt-original')
        with results_lock:
            results.append(res)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert results == [True, True], results
    assert recorder.max_concurrent == 1, (
        f"refresh_session was invoked concurrently -- per-uuid lock failed "
        f"(max_concurrent={recorder.max_concurrent}, expected 1)"
    )
    assert recorder.call_count == 1, f"expected exactly 1 refresh, got {recorder.call_count}"
    assert store['auth_session']['refresh_token'] == 'rt-new'


def test_b_distinct_uuid_parallelism(monkeypatch):
    """Two threads of distinct _session_uuids -> both refreshes fire IN PARALLEL.

    Plan-checker round catch: uses _ThreadRoutedApp + ConcurrencyRecorder
    to assert max_concurrent == 2 (proves real parallelism). Replaces the
    prior 5ms-stagger workaround which only asserted call_count == 2
    (trivially passes even under serialization).
    """
    import web.supabase_client as mod
    import web.safe_storage as ss

    uuid_a = 'a' * 32
    uuid_b = 'b' * 32
    store_a = {}
    store_b = {}
    _seed_storage_with_expiring_token(store_a, uuid_a)
    _seed_storage_with_expiring_token(store_b, uuid_b)

    # Per-thread routed app stub: each worker binds its own store before
    # crossing the barrier, then safe_storage reads route to that store.
    routed_app = _ThreadRoutedApp()
    original_app = ss.app
    monkeypatch.setattr('web.safe_storage.app', routed_app)

    recorder = _ConcurrencyRecorder()
    monkeypatch.setattr(
        mod, 'create_client',
        _make_mock_throwaway(recorder, 'at-new', 'rt-new', hold_ms=50),
    )

    barrier = threading.Barrier(2)
    results = {}
    results_lock = threading.Lock()

    def worker(store, key):
        # Bind THIS thread's user store via threading.local BEFORE the barrier.
        routed_app.bind(store)
        barrier.wait()
        res = mod._refresh_user_session(stale_refresh_token='rt-original')
        with results_lock:
            results[key] = res

    try:
        t1 = threading.Thread(target=worker, args=(store_a, 'a'))
        t2 = threading.Thread(target=worker, args=(store_b, 'b'))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        # Defensive: monkeypatch handles teardown, but be explicit.
        ss.app = original_app

    # Both refreshes succeeded.
    assert results == {'a': True, 'b': True}, results
    # call_count == 2: both refreshes fired (no cross-session lock).
    assert recorder.call_count == 2, (
        f"expected 2 refreshes (one per uuid), got {recorder.call_count}"
    )
    # PRIMARY INVARIANT (plan-checker round catch): max_concurrent == 2
    # proves the two refreshes actually OVERLAPPED in time -- they were
    # not serialized by some unexpected global lock. With hold_ms=50 and
    # threading.Barrier(2), both threads enter recorder.enter() before
    # either exits -- so max_concurrent reaches 2 iff the per-uuid locks
    # don't cross-serialize.
    assert recorder.max_concurrent == 2, (
        f"distinct-uuid refreshes did not run in parallel "
        f"(max_concurrent={recorder.max_concurrent}, expected 2). "
        "Either the per-uuid lock is keyed wrong (global instead of "
        "per-uuid), or some other serialization point exists. The "
        "5ms-stagger workaround would have missed this -- that's why "
        "the plan-checker round required completing the _ThreadRoutedApp "
        "proxy."
    )
    # And the two locks are distinct module-level entries:
    assert uuid_a in mod._refresh_locks
    assert uuid_b in mod._refresh_locks
    assert mod._refresh_locks[uuid_a] is not mod._refresh_locks[uuid_b]


def test_c_stale_snapshot_short_circuits(monkeypatch):
    """Thread 2 with stale_refresh_token != current refresh_token returns True
    without calling refresh_session."""
    import web.supabase_client as mod

    uuid_a = 'a' * 32
    store = {}
    _seed_storage_with_expiring_token(store, uuid_a)

    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user=store)),
    )

    recorder = _ConcurrencyRecorder()
    monkeypatch.setattr(
        mod, 'create_client',
        _make_mock_throwaway(recorder, 'at-rotated', 'rt-rotated'),
    )

    # Thread 1: refreshes normally with the original stale token.
    result_1 = mod._refresh_user_session(stale_refresh_token='rt-original')
    assert result_1 is True
    assert recorder.call_count == 1
    assert store['auth_session']['refresh_token'] == 'rt-rotated'

    # Thread 2: now calls with the PRE-rotation token. Inside lock,
    # sees refresh_token != 'rt-original' -> returns True without
    # invoking refresh_session.
    result_2 = mod._refresh_user_session(stale_refresh_token='rt-original')
    assert result_2 is True
    # CRITICAL: call_count MUST stay at 1 -- no second refresh.
    assert recorder.call_count == 1, (
        f"stale-snapshot short-circuit failed: {recorder.call_count} refreshes "
        "instead of 1. Second thread should have detected its snapshot was "
        "obsolete (refresh_token rotated) and returned True without "
        "burning another refresh (D-06)."
    )
