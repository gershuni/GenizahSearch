"""Background builds that give way to the user's own work (the desktop's startup).

At start the desktop builds several lookup tables in Python: the catalogue, the FL-id lookup,
the CUDL alias index, the Shelfmark search index (``desktop/startup_work.py``). Python runs one
thread at a time (the GIL). A build running beside a search slows the search, and on Windows
every Qt callback waits for the build's time slice (measured 2026-11-23: 0.08 ms idle, 42-67 ms
beside one busy thread). This module lets a build give way:

* ``user_work_started(alive=None)`` / ``user_work_ended()``: a search worker of the user's is
  running, not parked. ``desktop/gui_threads.py::_prevent_sleep`` / ``_allow_sleep`` call them,
  which every search worker already calls at exactly those moments. Kept per thread, so a
  repeated call changes nothing. ``alive()`` (the worker's ``QThread.isRunning``) lets a waiting
  build drop the mark of a worker stopped by ``terminate()``, which never reaches its end.
* ``yielding()``: the builds run in the calling thread give way (the desktop's startup queue).
  Only such a thread ever waits. A build that runs on the UI thread, in a search worker, or
  on the web never does.
* ``checkpoint(wanted=None)``: called inside a build's loop every few thousand rows. In a
  yielding thread, while user work runs, it waits, polling every ``POLL`` seconds, unless
  ``wanted()`` says someone is waiting for this very build. After ``MAX_WAIT`` seconds of one
  wait, the thread stops giving way for good: a worker that never said it ended cannot hold
  the builds back forever.
* ``OnDemandBuild``: the lock of a build that a caller may need before the queue gets to it.
  ``held()`` takes it and counts the threads waiting for it; ``wanted()`` says whether any are.
* ``fast_switching()``: while the startup queue runs, Python hands the GIL over every 0.5 ms
  instead of every 5 ms, so the window answers between a build's slices. With one busy
  thread, a UI callback measured 0.03 ms. The interval returns to its old value while user
  work runs (two busy threads at 0.5 ms lose much of their time handing it over) and when the
  queue ends.

When nothing marks user work (the web, the tests), ``checkpoint`` is one set lookup and
nothing else changes.
"""
from __future__ import annotations

import contextlib
import logging
import sys
import threading
import time
from typing import Callable, Optional

LOGGER = logging.getLogger(__name__)

FAST_SWITCH_INTERVAL = 0.0005
POLL = 0.05
MAX_WAIT = 30.0

_lock = threading.Lock()
_user_threads: dict = {}          # ident of a user's search worker now running -> alive() or None
_yielding_threads: set = set()    # idents of the threads whose builds give way
_stopped_yielding: set = set()    # yielding threads that waited MAX_WAIT once
_fast_depth = 0
_saved_interval: Optional[float] = None


def _apply_interval_locked() -> None:
    if not _fast_depth:
        return
    sys.setswitchinterval(_saved_interval if _user_threads else FAST_SWITCH_INTERVAL)


def user_work_started(alive: Optional[Callable[[], bool]] = None) -> None:
    """The calling thread is a search worker of the user's, running (not parked). *alive*: False
    once the worker is gone without ``user_work_ended`` (``QThread.terminate``)."""
    ident = threading.get_ident()
    with _lock:
        if ident in _user_threads:
            return
        _user_threads[ident] = alive
        _apply_interval_locked()


def user_work_ended() -> None:
    """The calling thread's search is over, or parked by Pause."""
    ident = threading.get_ident()
    with _lock:
        if ident not in _user_threads:
            return
        del _user_threads[ident]
        _apply_interval_locked()


def _still_alive(alive) -> bool:
    if alive is None:
        return True
    try:
        return bool(alive())
    except Exception:          # a deleted Qt object: gone
        return False


def _drop_gone_workers() -> None:
    """Forget the marks of workers that ended without saying so (``alive()`` False)."""
    with _lock:
        gone = [ident for ident, alive in _user_threads.items() if not _still_alive(alive)]
        for ident in gone:
            del _user_threads[ident]
        if gone:
            LOGGER.info("Startup builds: %d search worker(s) ended without saying so", len(gone))
            _apply_interval_locked()


def user_work_running() -> bool:
    return bool(_user_threads)


@contextlib.contextmanager
def yielding():
    """The builds run in the calling thread inside this block give way to user work."""
    ident = threading.get_ident()
    with _lock:
        _yielding_threads.add(ident)
    try:
        yield
    finally:
        with _lock:
            _yielding_threads.discard(ident)
            _stopped_yielding.discard(ident)


def checkpoint(wanted: Optional[Callable[[], bool]] = None) -> None:
    """Inside a build's loop: in a yielding thread, wait while user work runs (see the module)."""
    if not _user_threads:
        return
    ident = threading.get_ident()
    if ident not in _yielding_threads or ident in _stopped_yielding:
        return
    deadline = time.monotonic() + MAX_WAIT
    _drop_gone_workers()
    while _user_threads and not (wanted is not None and wanted()):
        if time.monotonic() >= deadline:
            LOGGER.info("A background build waited %.0f s for user work; it no longer gives way",
                        MAX_WAIT)
            with _lock:
                _stopped_yielding.add(ident)
            return
        time.sleep(POLL)
        _drop_gone_workers()


class OnDemandBuild:
    """The lock of a build that the startup queue runs in the background and that a caller
    may need sooner. A caller blocked in ``held()`` makes ``wanted()`` true, so the build
    goes on at its checkpoints instead of giving way to the very work waiting for it."""

    def __init__(self):
        self._lock = threading.Lock()
        self._count_lock = threading.Lock()
        self._waiting = 0

    def wanted(self) -> bool:
        return self._waiting > 0

    @contextlib.contextmanager
    def held(self):
        if not self._lock.acquire(blocking=False):
            with self._count_lock:
                self._waiting += 1
            try:
                self._lock.acquire()
            finally:
                with self._count_lock:
                    self._waiting -= 1
        try:
            yield
        finally:
            self._lock.release()


@contextlib.contextmanager
def fast_switching():
    """GIL hand-over every 0.5 ms inside this block, except while user work runs."""
    global _fast_depth, _saved_interval
    with _lock:
        if _fast_depth == 0:
            _saved_interval = sys.getswitchinterval()
        _fast_depth += 1
        _apply_interval_locked()
    try:
        yield
    finally:
        with _lock:
            _fast_depth -= 1
            if _fast_depth == 0:
                if _saved_interval is not None:
                    sys.setswitchinterval(_saved_interval)
                _saved_interval = None
            else:
                _apply_interval_locked()
