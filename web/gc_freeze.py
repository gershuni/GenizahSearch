# -*- coding: utf-8 -*-
"""One ``gc.collect(); gc.freeze()`` after the startup loaders finish (2026-10-21).

Why this exists
---------------
Startup leaves 0.6-0.84 M tracked objects in the web process (the csv bank,
the engines, the browse map), and every full (generation 2) collection walks
all of them on whichever thread trips the threshold -- mostly the event loop:
465 and 534 ms in a real /browse run (measured 2026-10-20), 92-133 ms after
``gc.collect(); gc.freeze()``. ``gc.freeze()`` moves everything tracked at that
moment into the permanent generation, which later collections ignore.

Design
------
- It runs ONCE, after the loaders that keep allocating after ``_init_sync``
  returns are done, so what they build is frozen too: the two MetadataManager
  daemon threads (``background_loading_finished()``), the FL-ID/browse-map
  build thread (``SearchEngine._fl_id_index_building``, reached through the
  ``IsolatedEngine`` wrapper's ``_engine``) and the FJMS prewarm task, handed
  in as ``extra_tasks``. It is NOT an ``app.on_startup`` handler: NiceGUI 3.18
  runs those concurrently, so no handler can run "after" ``initialize_engine``;
  it is scheduled from inside it. ``AppState.is_ready()`` flips True before the
  loaders finish and is not a done signal.
- Bounded wait: after ``GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS`` it freezes anyway
  and names the loader still running (what that loader builds afterwards stays
  unfrozen, which only costs what it was costing before).
- Never raises; a failure degrades to one ``[init]`` log line.
- Frozen objects are invisible to ``gc.get_objects()`` AND ``gc.get_referrers()``,
  so /_internal/objgraph reports ``gc.get_freeze_count()`` and can call
  ``unfreeze_for_diagnostics()`` / ``refreeze_for_diagnostics()``.
- Observability: stats are registered lazily as ``gc_freeze`` in web.api's
  runtime-cache registry, so /_internal/memstat shows them under
  ``runtime_caches`` with no change to that route (the web/malloc_trim.py shape).

Env knobs
---------
``GENIZAH_GC_FREEZE``                   1/true (default) enables; 0/false/no/off disables.
``GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS``  freeze anyway after waiting this long (default 180).
``GENIZAH_GC_FREEZE_POLL_SECONDS``      how often the wait re-checks the loaders (default 1.0).
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_TRUTHY = {'1', 'true', 'yes', 'on'}
_DEFAULT_MAX_WAIT_SECONDS = 180.0
_DEFAULT_POLL_SECONDS = 1.0


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    return raw.strip().lower() in _TRUTHY


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    """Read a float env var, clamped to `minimum`; fall back on any bad value."""
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning("gc_freeze: %s=%r is not a number; using %s", name, raw, default)
        return default
    return max(minimum, value)


def gc_freeze_enabled() -> bool:
    """True unless explicitly disabled. Read live so it can be flipped per restart."""
    return _env_flag('GENIZAH_GC_FREEZE', True)


def max_wait_seconds() -> float:
    return _env_float('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', _DEFAULT_MAX_WAIT_SECONDS, minimum=0.0)


def poll_seconds() -> float:
    # Floor of 10 ms: a tighter poll would itself be measurable loop work.
    return _env_float('GENIZAH_GC_FREEZE_POLL_SECONDS', _DEFAULT_POLL_SECONDS, minimum=0.01)


# ---------------------------------------------------------------------------
# The loaders that keep allocating after initialize_engine's _init_sync returns
# ---------------------------------------------------------------------------

def pending_loaders(state) -> list:
    """Names of the startup loaders still running; empty once all are done.

    Reads only what the loaders already expose: MetadataManager's thread
    handles and SearchEngine's ``_fl_id_index_building`` flag (set
    synchronously in ``__init__``, cleared in the build thread's ``finally``).
    """
    pending = []
    meta_mgr = getattr(state, 'meta_mgr', None)
    if meta_mgr is None:
        pending.append('MetadataManager')
    else:
        finished = getattr(meta_mgr, 'background_loading_finished', None)
        try:
            if finished is not None and not finished():
                pending.append('MetadataManager.start_background_loading')
        except Exception:
            pass
    searcher = getattr(state, 'searcher', None)
    engine = getattr(searcher, '_engine', searcher)
    if getattr(engine, '_fl_id_index_building', False):
        pending.append('SearchEngine FL-ID index build')
    return pending


def loaders_finished(state) -> bool:
    return not pending_loaders(state)


# ---------------------------------------------------------------------------
# State and stats (read by /_internal/memstat via the runtime-cache registry)
# ---------------------------------------------------------------------------

class _FreezeState:
    def __init__(self):
        self.lock = threading.Lock()
        self.scheduled = False
        self.task: Optional[asyncio.Task] = None
        self.stats = {
            'enabled': None,
            'frozen_objects': None,
            'waited_seconds': None,
            'loaders_finished': None,
            'pending_loaders': [],
            'collect_ms': None,
            'frozen_at_utc': None,
            'unfrozen_for_diagnostics': False,
            'unfrozen_at_utc': None,
            'refrozen_at_utc': None,
        }


_state = _FreezeState()


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def stats() -> dict:
    with _state.lock:
        out = dict(_state.stats)
    out['current_freeze_count'] = gc.get_freeze_count()
    return out


def reset_stats() -> None:
    """Tests only: forget the singleton so freeze_after_loaders() can run again."""
    global _state
    _state = _FreezeState()


def _register_stats() -> None:
    # Registry import is lazy to keep this module import-light and avoid any
    # web.api import-order coupling at module load (the malloc_trim shape).
    try:
        from web.api import register_runtime_cache_stats
        register_runtime_cache_stats('gc_freeze', stats)
    except Exception as e:
        print(f"[init] gc.freeze stats registration failed (non-fatal): {e}", flush=True)


# ---------------------------------------------------------------------------
# The one-time freeze
# ---------------------------------------------------------------------------

async def freeze_after_loaders(
    state,
    *,
    extra_tasks=(),
    collect=gc.collect,
    freeze=gc.freeze,
    sleep=asyncio.sleep,
) -> bool:
    """Wait for the startup loaders, then ``collect(); freeze()`` exactly once.

    Scheduled from web/main.py::initialize_engine once ``_init_sync`` has
    succeeded. Returns True when it froze, False when disabled, already run,
    or failed. Never raises.
    """
    with _state.lock:
        if _state.scheduled:
            return False
        _state.scheduled = True
        # Anchor the task: asyncio keeps only weak references to tasks.
        try:
            _state.task = asyncio.current_task()
        except RuntimeError:
            _state.task = None
    try:
        _register_stats()
        if not gc_freeze_enabled():
            with _state.lock:
                _state.stats['enabled'] = False
            print(
                "[init] gc.freeze disabled (GENIZAH_GC_FREEZE=0): full collections keep "
                "walking the startup objects",
                flush=True,
            )
            return False
        with _state.lock:
            _state.stats['enabled'] = True
        max_wait = max_wait_seconds()
        poll = poll_seconds()
        started = time.monotonic()
        pending = pending_loaders(state)
        while pending and (time.monotonic() - started) < max_wait:
            await sleep(poll)
            pending = pending_loaders(state)
        if extra_tasks:
            # A failed prewarm is its own log line; it must not stop the freeze.
            await asyncio.gather(*extra_tasks, return_exceptions=True)
            pending = pending_loaders(state)
        waited = time.monotonic() - started
        t0 = time.perf_counter()
        collect()
        collect_ms = (time.perf_counter() - t0) * 1000.0
        freeze()
        frozen = gc.get_freeze_count()
        with _state.lock:
            _state.stats.update({
                'frozen_objects': frozen,
                'waited_seconds': round(waited, 1),
                'loaders_finished': not pending,
                'pending_loaders': list(pending),
                'collect_ms': round(collect_ms, 1),
                'frozen_at_utc': _now_utc(),
            })
        if pending:
            print(
                f"[init] gc.freeze: {frozen:,} objects moved to the permanent generation; "
                f"STILL RUNNING after {waited:.0f} s: {', '.join(pending)} "
                f"(gc.collect took {collect_ms:.0f} ms; perf_watch may report this one stall; "
                f"what those loaders build from now on stays unfrozen)",
                flush=True,
            )
        else:
            print(
                f"[init] gc.freeze: {frozen:,} objects moved to the permanent generation after "
                f"the loaders finished (waited {waited:.1f} s; gc.collect took {collect_ms:.0f} ms; "
                f"perf_watch may report this one stall)",
                flush=True,
            )
        return True
    except Exception as e:
        print(f"[init] gc.freeze failed (non-fatal): {e}", flush=True)
        return False
    finally:
        with _state.lock:
            _state.task = None


# ---------------------------------------------------------------------------
# Diagnostics (/_internal/objgraph?action=unfreeze|refreeze)
# ---------------------------------------------------------------------------

def unfreeze_for_diagnostics() -> int:
    """Move the permanent generation back into the oldest generation.

    Until ``refreeze_for_diagnostics()``, ``gc.get_objects()`` and
    ``gc.get_referrers()`` see the startup objects again -- and so does every
    full collection, at the pre-freeze cost. Returns how many objects moved.
    """
    count = gc.get_freeze_count()
    gc.unfreeze()
    with _state.lock:
        _state.stats['unfrozen_for_diagnostics'] = True
        _state.stats['unfrozen_at_utc'] = _now_utc()
    return count


def refreeze_for_diagnostics() -> int:
    """``gc.collect(); gc.freeze()`` everything alive now. Returns the new freeze count."""
    gc.collect()
    gc.freeze()
    count = gc.get_freeze_count()
    with _state.lock:
        _state.stats['unfrozen_for_diagnostics'] = False
        _state.stats['refrozen_at_utc'] = _now_utc()
    return count
