"""Tests for web/malloc_trim.py (2026-07-08 web-memory Tier-2).

The trim loop is exercised through injected trim/rss callables — no real
libc dependency, so the suite runs identically on Windows CI.
"""

import threading


from web.malloc_trim import MallocTrimmer, _env_int


class TestEnvParsing:
    def test_default_when_unset(self, monkeypatch):
        monkeypatch.delenv('GENIZAH_MALLOC_TRIM_SECONDS', raising=False)
        assert _env_int('GENIZAH_MALLOC_TRIM_SECONDS', 300) == 300

    def test_explicit_value(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', '120')
        assert _env_int('GENIZAH_MALLOC_TRIM_SECONDS', 300) == 120

    def test_zero_allowed(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', '0')
        assert _env_int('GENIZAH_MALLOC_TRIM_SECONDS', 300) == 0

    def test_garbage_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', 'soon')
        assert _env_int('GENIZAH_MALLOC_TRIM_SECONDS', 300) == 300

    def test_negative_clamped_to_zero(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', '-5')
        assert _env_int('GENIZAH_MALLOC_TRIM_SECONDS', 300) == 0


class TestStartGuards:
    def test_interval_zero_disables(self):
        trimmer = MallocTrimmer(interval_seconds=0, min_growth_mb=0, trim_fn=lambda: 1)
        assert trimmer.start() is False
        assert trimmer.disabled_reason == 'interval_zero'
        assert trimmer.stats()['enabled'] is False

    def test_non_linux_disables(self, monkeypatch):
        monkeypatch.setattr('web.malloc_trim.sys.platform', 'win32')
        trimmer = MallocTrimmer(interval_seconds=300, min_growth_mb=0, trim_fn=lambda: 1)
        assert trimmer.start() is False
        assert trimmer.disabled_reason == 'not_linux'

    def test_libc_unavailable_disables(self, monkeypatch):
        monkeypatch.setattr('web.malloc_trim.sys.platform', 'linux')
        monkeypatch.setattr('web.malloc_trim._load_libc_trim', lambda: None)
        trimmer = MallocTrimmer(interval_seconds=300, min_growth_mb=0)
        assert trimmer.start() is False
        assert trimmer.disabled_reason == 'libc_unavailable'

    def test_start_is_idempotent(self, monkeypatch):
        monkeypatch.setattr('web.malloc_trim.sys.platform', 'linux')
        trimmer = MallocTrimmer(
            interval_seconds=3600, min_growth_mb=0,
            trim_fn=lambda: 1, rss_reader=lambda: 1000,
        )
        try:
            assert trimmer.start() is True
            first_thread = trimmer._thread
            assert trimmer.start() is True
            assert trimmer._thread is first_thread
        finally:
            trimmer.stop()

    def test_restart_after_stop_spawns_fresh_thread(self, monkeypatch):
        # Codex code-review MEDIUM: start() must not report success on a dead
        # thread — after stop(), a new start() spawns a fresh live loop.
        monkeypatch.setattr('web.malloc_trim.sys.platform', 'linux')
        trimmer = MallocTrimmer(
            interval_seconds=3600, min_growth_mb=0,
            trim_fn=lambda: 1, rss_reader=lambda: 1000,
        )
        try:
            assert trimmer.start() is True
            first_thread = trimmer._thread
            trimmer.stop(join_timeout=5.0)
            assert not first_thread.is_alive()
            assert trimmer.is_running() is False
            assert trimmer.stats()['enabled'] is False  # liveness-aware
            assert trimmer.start() is True
            assert trimmer._thread is not first_thread
            assert trimmer.is_running() is True
            assert trimmer.stats()['enabled'] is True
        finally:
            trimmer.stop(join_timeout=5.0)


class TestAdaptiveRunOnce:
    def _trimmer(self, rss_values, min_growth_mb):
        calls = []
        rss_iter = iter(rss_values)

        def rss_reader():
            return next(rss_iter)

        trimmer = MallocTrimmer(
            interval_seconds=300,
            min_growth_mb=min_growth_mb,
            trim_fn=lambda: calls.append(1) or 1,
            rss_reader=rss_reader,
        )
        return trimmer, calls

    def test_trims_when_growth_exceeds_threshold(self):
        # baseline 1_000_000 kB, now 1_100_000 kB (+~98 MB) >= 64 MB threshold.
        trimmer, calls = self._trimmer([1_100_000, 1_050_000], min_growth_mb=64)
        trimmer._last_baseline_kb = 1_000_000
        assert trimmer.run_once() is True
        assert calls == [1]
        stats = trimmer.stats()
        assert stats['runs'] == 1
        assert stats['last_rc'] == 1
        assert stats['last_rss_anon_before_kb'] == 1_100_000
        assert stats['last_rss_anon_after_kb'] == 1_050_000
        assert stats['last_freed_kb'] == 50_000
        assert stats['total_freed_kb'] == 50_000
        assert trimmer._last_baseline_kb == 1_050_000  # post-trim baseline

    def test_skips_when_growth_below_threshold(self):
        trimmer, calls = self._trimmer([1_010_000], min_growth_mb=64)
        trimmer._last_baseline_kb = 1_000_000
        assert trimmer.run_once() is False
        assert calls == []
        assert trimmer.stats()['skips'] == 1

    def test_threshold_zero_always_trims(self):
        trimmer, calls = self._trimmer([1_000_100, 1_000_000], min_growth_mb=0)
        trimmer._last_baseline_kb = 1_000_000
        assert trimmer.run_once() is True
        assert calls == [1]

    def test_unreadable_rss_still_trims(self):
        # rss_reader returning None (non-procfs platform) must not block trimming.
        trimmer = MallocTrimmer(
            interval_seconds=300, min_growth_mb=64,
            trim_fn=lambda: 1, rss_reader=lambda: None,
        )
        assert trimmer.run_once() is True
        assert trimmer.stats()['last_freed_kb'] is None


class TestLoop:
    def test_loop_invokes_trim_and_stop_terminates(self, monkeypatch):
        monkeypatch.setattr('web.malloc_trim.sys.platform', 'linux')
        trimmed = threading.Event()
        trimmer = MallocTrimmer(
            interval_seconds=1, min_growth_mb=0,
            trim_fn=lambda: trimmed.set() or 1,
            rss_reader=lambda: 1000,
        )
        # Shrink the wait so the test is fast: patch the stop event's wait.
        trimmer.interval_seconds = 0.01  # type: ignore[assignment]
        assert trimmer.start() is True
        assert trimmed.wait(timeout=5.0), 'trim_fn was never called by the loop'
        trimmer.stop()
        trimmer._thread.join(timeout=5.0)
        assert not trimmer._thread.is_alive()


class TestStartupHelper:
    def test_helper_is_idempotent_and_registers_stats(self, monkeypatch):
        import web.malloc_trim as mt

        monkeypatch.setattr(mt, '_trimmer', None)
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', '0')  # disabled: no thread
        registered = {}
        monkeypatch.setattr(
            'web.api.register_runtime_cache_stats',
            lambda name, provider: registered.update({name: provider}),
        )
        first = mt.start_malloc_trim_from_env()
        second = mt.start_malloc_trim_from_env()
        assert first is second
        assert 'malloc_trim' in registered
        stats = registered['malloc_trim']()
        assert stats['enabled'] is False
        assert stats['disabled_reason'] == 'interval_zero'

    def test_helper_singleton_survives_concurrent_callers(self, monkeypatch):
        # Codex code-review MEDIUM: unlocked check-then-set raced — N threads
        # hitting the helper simultaneously must all get the SAME instance.
        import web.malloc_trim as mt

        monkeypatch.setattr(mt, '_trimmer', None)
        monkeypatch.setenv('GENIZAH_MALLOC_TRIM_SECONDS', '0')  # disabled: no thread
        monkeypatch.setattr(
            'web.api.register_runtime_cache_stats', lambda name, provider: None
        )
        n = 8
        barrier = threading.Barrier(n)
        results = []

        def call():
            barrier.wait()
            results.append(mt.start_malloc_trim_from_env())

        threads = [threading.Thread(target=call) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)
        assert len(results) == n
        assert all(r is results[0] for r in results)


class TestRssReader:
    def test_read_rss_anon_kb_never_raises(self):
        # Returns an int on Linux, None on Windows/macOS — never raises.
        from web.malloc_trim import read_rss_anon_kb
        value = read_rss_anon_kb()
        assert value is None or (isinstance(value, int) and value >= 0)
