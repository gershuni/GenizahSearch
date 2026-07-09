"""Periodic glibc ``malloc_trim(0)`` — return dead allocator high-water to the OS.

Why this exists (2026-07-08 attribution, see docs/OPEN_ISSUES.md web-memory
addendum): the web process's RSS ratchets up under traffic because per-request
transient churn (browse renders, IIIF manifest JSON, search full_text
hydration) is freed back to the allocator but never returned to the kernel —
smaps showed ~12 GB across ~300 arena-class anon regions while live Python
objects grew only single-digit MB (tracemalloc-under-load). ``malloc_trim(0)``
walks every glibc arena and MADV_DONTNEEDs free pages, directly attacking that
retention. It does NOT reclaim partially-occupied pymalloc arenas or non-glibc
native allocations, so the daily restart timer remains the backstop.

Design (Codex pre-flight + code review, _tmp/codex-tier2-critique-2026-07-08.md):
- Started ONLY from ``app.on_startup`` (never at import) to avoid duplicate
  threads under dev reload / test collection; the module-level singleton is
  guarded by a lock (code-review MEDIUM: unlocked check-then-set raced).
- Plain daemon thread, not an asyncio task: the trim call can take tens of ms
  walking arenas; ctypes releases the GIL during the call, and a thread is
  immune to event-loop stalls.
- Adaptive trigger: wakes every ``GENIZAH_MALLOC_TRIM_SECONDS`` (default 300,
  0 disables) but only trims when RssAnon grew by at least
  ``GENIZAH_MALLOC_TRIM_MIN_GROWTH_MB`` (default 64, 0 = trim every wake)
  since the previous trim — idle periods cost one /proc read.
- Observability: registers a stats provider in web.api's runtime-cache
  registry, so /_internal/memstat shows runs/skips/durations/freed bytes and
  explicit RssAnon before/after readings; stats reads/writes share a lock.
"""

import ctypes
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

_DEFAULT_INTERVAL_SECONDS = 300
_DEFAULT_MIN_GROWTH_MB = 64


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, '')
    try:
        value = int(raw) if raw.strip() else default
    except ValueError:
        value = default
    return max(0, value)


def read_rss_anon_kb() -> Optional[int]:
    """Read RssAnon from /proc/self/status. None if unavailable."""
    try:
        with open('/proc/self/status', encoding='ascii', errors='replace') as handle:
            for line in handle:
                if line.startswith('RssAnon:'):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def _load_libc_trim():
    """Return a callable ``trim() -> int`` bound to glibc, or None."""
    try:
        libc = ctypes.CDLL('libc.so.6', use_errno=True)
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        return lambda: trim(0)
    except Exception:
        return None


class MallocTrimmer:
    """Daemon-thread loop calling glibc malloc_trim(0) on an adaptive schedule."""

    def __init__(
        self,
        interval_seconds: int,
        min_growth_mb: int,
        trim_fn=None,
        rss_reader=read_rss_anon_kb,
    ):
        self.interval_seconds = interval_seconds
        self.min_growth_mb = min_growth_mb
        self._trim_fn = trim_fn
        self._rss_reader = rss_reader
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()        # thread create/replace
        self._stats_lock = threading.Lock()  # stats mutation vs. stats() reads
        self.disabled_reason: Optional[str] = None
        # Stats (read by /_internal/memstat via the runtime-cache registry).
        self.runs = 0
        self.skips = 0
        self.errors = 0
        self.last_rc: Optional[int] = None
        self.last_duration_ms: Optional[float] = None
        self.last_rss_anon_before_kb: Optional[int] = None
        self.last_rss_anon_after_kb: Optional[int] = None
        self.last_freed_kb: Optional[int] = None
        self.total_freed_kb = 0
        self.last_run_utc: Optional[str] = None
        self._last_baseline_kb: Optional[int] = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> bool:
        """Start the daemon thread. Returns False when disabled.

        Restartable: after stop(), a new call starts a fresh thread. A live
        thread is never duplicated (checked under the lock via is_alive()).
        """
        if self.interval_seconds <= 0:
            self.disabled_reason = 'interval_zero'
            return False
        if not sys.platform.startswith('linux'):
            self.disabled_reason = 'not_linux'
            return False
        if self._trim_fn is None:
            self._trim_fn = _load_libc_trim()
        if self._trim_fn is None:
            self.disabled_reason = 'libc_unavailable'
            return False
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return True  # already running; never double-start
            # Fresh event so a stopped trimmer can be restarted (tests).
            self._stop = threading.Event()
            self._thread = threading.Thread(
                target=self._loop, name='malloc-trim', daemon=True
            )
            self._thread.start()
        return True

    def stop(self, join_timeout: Optional[float] = 1.0) -> None:
        """Signal the loop to exit; setting the event wakes the wait immediately."""
        self._stop.set()
        thread = self._thread
        if (
            thread is not None
            and join_timeout
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=join_timeout)

    def is_running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    # -- loop --------------------------------------------------------------

    def _loop(self) -> None:
        try:
            self._last_baseline_kb = self._rss_reader()
        except Exception:
            with self._stats_lock:
                self.errors += 1
            self._last_baseline_kb = None
        while not self._stop.wait(self.interval_seconds):
            try:
                self.run_once()
            except Exception:
                with self._stats_lock:
                    self.errors += 1

    def run_once(self) -> bool:
        """One adaptive tick: trim if RssAnon grew enough. Returns True if trimmed."""
        try:
            before_kb = self._rss_reader()
        except Exception:
            before_kb = None
        if (
            self.min_growth_mb > 0
            and before_kb is not None
            and self._last_baseline_kb is not None
            and (before_kb - self._last_baseline_kb) < self.min_growth_mb * 1024
        ):
            with self._stats_lock:
                self.skips += 1
            return False
        started = time.perf_counter()
        rc = self._trim_fn()
        duration_ms = (time.perf_counter() - started) * 1000.0
        try:
            after_kb = self._rss_reader()
        except Exception:
            after_kb = None
        with self._stats_lock:
            self.runs += 1
            self.last_rc = rc
            self.last_duration_ms = round(duration_ms, 1)
            self.last_rss_anon_before_kb = before_kb
            self.last_rss_anon_after_kb = after_kb
            self.last_run_utc = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            if before_kb is not None and after_kb is not None:
                freed = max(0, before_kb - after_kb)
                self.last_freed_kb = freed
                self.total_freed_kb += freed
        self._last_baseline_kb = after_kb
        return True

    # -- observability -----------------------------------------------------

    def stats(self) -> dict:
        running = self.is_running()
        with self._stats_lock:
            return {
                'enabled': running and self.disabled_reason is None,
                'disabled_reason': self.disabled_reason,
                'interval_seconds': self.interval_seconds,
                'min_growth_mb': self.min_growth_mb,
                'runs': self.runs,
                'skips': self.skips,
                'errors': self.errors,
                'last_rc': self.last_rc,
                'last_duration_ms': self.last_duration_ms,
                'last_rss_anon_before_kb': self.last_rss_anon_before_kb,
                'last_rss_anon_after_kb': self.last_rss_anon_after_kb,
                'last_freed_kb': self.last_freed_kb,
                'total_freed_kb': self.total_freed_kb,
                'last_run_utc': self.last_run_utc,
            }


_trimmer: Optional[MallocTrimmer] = None
_trimmer_lock = threading.Lock()


def start_malloc_trim_from_env() -> Optional[MallocTrimmer]:
    """app.on_startup hook: create, start, and register the singleton trimmer.

    Idempotent AND thread-safe (module lock guards the check-then-set — Codex
    code-review MEDIUM). Never raises — a failure to start degrades to a log
    line, the app runs as before.
    """
    global _trimmer
    with _trimmer_lock:
        if _trimmer is not None:
            return _trimmer
        trimmer = MallocTrimmer(
            interval_seconds=_env_int('GENIZAH_MALLOC_TRIM_SECONDS', _DEFAULT_INTERVAL_SECONDS),
            min_growth_mb=_env_int('GENIZAH_MALLOC_TRIM_MIN_GROWTH_MB', _DEFAULT_MIN_GROWTH_MB),
        )
        try:
            started = trimmer.start()
            # Registry import is lazy to keep this module import-light and avoid
            # any web.api import-order coupling at module load.
            from web.api import register_runtime_cache_stats
            register_runtime_cache_stats('malloc_trim', trimmer.stats)
            if started:
                print(
                    f"[init] malloc_trim loop started (interval={trimmer.interval_seconds}s, "
                    f"min_growth={trimmer.min_growth_mb}MB)",
                    flush=True,
                )
            else:
                print(f"[init] malloc_trim disabled ({trimmer.disabled_reason})", flush=True)
        except Exception as e:
            print(f"[init] malloc_trim setup failed (non-fatal): {e}", flush=True)
        _trimmer = trimmer
        return _trimmer
