# -*- coding: utf-8 -*-
"""Phase 92.2 D-NLI-01 behavioral tests for _save_nli_persistent_cache.

3 tests verifying the threading.Lock + retry semantics:
(a) 2-fail-then-succeed returns True after at most 3 attempts
(b) all-fail returns False, warns ONCE, cleans tmp file
(c) serialization under concurrent calls via threading.Barrier placed
    BEFORE with _nli_persist_lock: acquisition (Reviews Codex-LOW-1)
    and _ConcurrencyRecorder.max_concurrent == 1

Uses importlib to reload the module shim and directly patches internal
helpers. Does NOT require real filesystem access -- os.replace is
monkeypatched.
"""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import os
import json
import tempfile


# ---------------------------------------------------------------------------
# Helper: _ConcurrencyRecorder (mirrors test_refresh_lock_per_session.py pattern)
# ---------------------------------------------------------------------------

class _ConcurrencyRecorder:
    """Tracks max concurrent invocations of a critical section."""

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


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _make_minimal_cache():
    """Build a minimal cache + cache_time suitable for _save_nli_persistent_cache."""
    fl_ids = ['T-S_12.123.1r', 'T-S_12.123.1v']
    cache = {'12345': fl_ids}
    cache_time = {'12345': time.time()}
    return cache, cache_time


# ---------------------------------------------------------------------------
# Test (a): 2-fail-then-succeed returns True after retry
# ---------------------------------------------------------------------------

def test_save_retries_and_returns_true(tmp_path):
    """os.replace fails twice then succeeds; function returns True.

    Verifies the retry loop at 100/250/500ms does not prematurely give up.
    """
    from web.api import _save_nli_persistent_cache

    cache, cache_time = _make_minimal_cache()
    cache_path = str(tmp_path / 'nli_cache.json')

    replace_call_count = {'n': 0}
    real_replace = os.replace

    def fake_replace(src, dst):
        replace_call_count['n'] += 1
        if replace_call_count['n'] <= 2:
            raise OSError(5, 'Access is denied', dst)  # WinError 5 equivalent
        # 3rd attempt: succeed by doing the real replace
        real_replace(src, dst)

    with patch('os.replace', side_effect=fake_replace), \
         patch('time.sleep'):  # skip actual sleep delays
        result = _save_nli_persistent_cache(cache, cache_time, cache_path=cache_path)

    assert result is True, "should return True after retry succeeds"
    assert replace_call_count['n'] == 3, f"expected 3 replace attempts, got {replace_call_count['n']}"


# ---------------------------------------------------------------------------
# Test (b): all-fail returns False, warns ONCE, cleans up tmp file
# ---------------------------------------------------------------------------

def test_save_returns_false_after_exhaustion(tmp_path, caplog):
    """All os.replace attempts fail; function returns False and warns exactly once."""
    import logging
    from web.api import _save_nli_persistent_cache

    cache, cache_time = _make_minimal_cache()
    cache_path = str(tmp_path / 'nli_cache.json')

    def always_fail(src, dst):
        raise OSError(5, 'Access is denied', dst)

    with patch('os.replace', side_effect=always_fail), \
         patch('time.sleep'), \
         caplog.at_level(logging.WARNING, logger='web.api'):
        result = _save_nli_persistent_cache(cache, cache_time, cache_path=cache_path)

    assert result is False, "should return False when all retries exhausted"

    # Warning logged exactly once (not once per retry attempt)
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING and 'persist' in r.message.lower()]
    assert len(warning_records) == 1, (
        f"expected exactly 1 WARNING about persist failure, got {len(warning_records)}: "
        + str([r.message for r in warning_records])
    )

    # Temp file cleaned up (no .tmp.* files left in tmp_path)
    tmp_files = list(tmp_path.glob('*.tmp.*'))
    assert len(tmp_files) == 0, f"expected no leftover tmp files, found: {tmp_files}"


# ---------------------------------------------------------------------------
# Test (c): serialization under concurrent calls (Reviews Codex-LOW-1)
# ---------------------------------------------------------------------------

def test_save_serialized_under_concurrent_calls(tmp_path):
    """Two concurrent threads cannot hold _nli_persist_lock simultaneously.

    Reviews Codex-LOW-1: threading.Barrier is placed BEFORE the
    `with _nli_persist_lock:` acquisition so both threads are guaranteed
    to be alive and contending on the lock simultaneously. If the barrier
    were placed INSIDE the locked region, it would deadlock because the
    first thread to acquire would hold the lock while waiting for the second
    thread at the barrier, but the second thread can't reach the barrier
    without first acquiring the lock.

    _ConcurrencyRecorder.max_concurrent == 1 proves serialization: if both
    threads ever held the lock simultaneously, max_concurrent would reach 2.
    """
    from web import api as api_mod

    cache, cache_time = _make_minimal_cache()
    cache_path = str(tmp_path / 'nli_cache.json')

    recorder = _ConcurrencyRecorder()
    barrier = threading.Barrier(2)  # placed BEFORE lock acquisition (Reviews Codex-LOW-1)

    results = []
    errors = []

    real_replace = os.replace

    def instrumented_replace(src, dst):
        """Record entry into the critical os.replace region for concurrency tracking."""
        recorder.enter()
        try:
            real_replace(src, dst)
        finally:
            recorder.exit()

    def worker():
        try:
            # Barrier BEFORE the lock: both threads are alive and contending.
            barrier.wait(timeout=5.0)
            result = api_mod._save_nli_persistent_cache(cache, cache_time, cache_path=cache_path)
            results.append(result)
        except Exception as exc:
            errors.append(exc)

    with patch.object(api_mod, '_nli_persist_lock', api_mod._nli_persist_lock), \
         patch('os.replace', side_effect=instrumented_replace):
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

    assert not errors, f"worker threads raised exceptions: {errors}"
    assert len(results) == 2, f"expected 2 results, got {len(results)}"
    assert all(r is True for r in results), f"both saves should succeed, got {results}"
    assert recorder.max_concurrent == 1, (
        f"max_concurrent={recorder.max_concurrent} — lock did not serialize: "
        "two threads entered os.replace simultaneously"
    )
