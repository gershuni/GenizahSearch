# -*- coding: utf-8 -*-
"""Leave no running QThread to be destroyed on the way out of the desktop app.

Destroying a QThread whose thread is still running aborts the process ("QThread:
Destroyed while thread '' is still running"; Windows 0xC0000409). At exit the
interpreter drops Python objects in two waves, and only the first one destroys:

1. PyQt's own exit handler destroys the remaining windows, which destroys every child
   widget (a closed Joins Lab is a hidden child of the main window). A child's Python
   attributes go with it, and a QThread whose last Python reference was one of them is
   destroyed then, running or not. So is a QThread whose Qt parent is destroyed,
   whatever Python still holds. A thread inside its Python run() is also held by that
   frame's ``self``, which hides the problem most of the time; a thread still waiting
   for the GIL to enter run() or just leaving it, or a worker thread running its event
   loop, has no such frame. Measured 2026-09-29: event-loop threads held only by a
   hidden dialog aborted 20 runs of 20; ten busy Python threads, 32 of 200.
2. Then the interpreter tears down modules, and a C++ object whose wrapper goes then is
   not destroyed. A QThread still referenced from a module-level list reaches this wave
   and is left running; the process exits with code 0 (0 aborts in 620 runs).

settle_running_threads() runs once, after the event loop has returned: every running
QThread created from Python is asked to stop (requestInterruption() and quit()), all of
them get one shared, bounded wait, and those still running are taken off any Qt parent
and held here for the rest of the process, so they reach wave 2. The wait happens after
the last window has closed, so it freezes nothing on screen. There is no terminate(): it
can kill a thread inside a lock and hang the exit.

PyQt6 only, no repository imports: tests/test_kept_qthreads_exit_cleanly.py runs this
module in a child process without touching the app's data folder.
"""
import gc
import logging
import time

from PyQt6 import sip
from PyQt6.QtCore import QThread

LOGGER = logging.getLogger("genizah." + __name__)

# One wait shared by all running threads, not one per thread.
EXIT_WAIT_MS = 1500

# Threads still running after the wait. Never pruned: dropping one from a finished()
# slot would release it while its thread is still finishing.
_KEPT_AT_EXIT: list = []


def running_qthreads() -> list:
    """Every QThread created from Python whose thread is still running, except this one.

    Found through the garbage collector, so a thread counts wherever it is referenced:
    a keeper list, a window attribute, a Qt parent. Threads Qt created itself, and the
    main thread, are not created from Python and are left out.
    """
    current = QThread.currentThread()
    found = []
    for obj in gc.get_objects():
        if not isinstance(obj, QThread):
            continue
        try:
            if (obj is not current and sip.ispycreated(obj) and not sip.isdeleted(obj)
                    and obj.isRunning()):
                found.append(obj)
        except RuntimeError:
            continue
    return found


def settle_running_threads(wait_ms: int = EXIT_WAIT_MS) -> list:
    """Stop what stops within ``wait_ms``; keep the rest from being destroyed at exit.

    Call once, after QApplication.exec() has returned and before the interpreter exits.
    Returns the threads still running, which are now held here and have no Qt parent.
    """
    started = time.monotonic()
    threads = running_qthreads()
    if not threads:
        return []
    for t in threads:
        try:
            t.requestInterruption()
            t.quit()
        except RuntimeError:
            pass
    deadline = started + wait_ms / 1000
    for t in threads:
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            break
        try:
            t.wait(remaining_ms)
        except RuntimeError:
            pass
    still_running = []
    for t in threads:
        try:
            if not t.isRunning():
                continue
            if t.parent() is not None:
                t.setParent(None)
        except RuntimeError:
            continue
        _KEPT_AT_EXIT.append(t)
        still_running.append(t)
    if still_running:
        LOGGER.warning(
            "exit: %d of %d QThreads still running after %d ms, left running: %s",
            len(still_running), len(threads), wait_ms,
            ", ".join(sorted({type(t).__name__ for t in still_running})))
    else:
        LOGGER.info("exit: %d running QThreads finished within %.0f ms",
                    len(threads), (time.monotonic() - started) * 1000)
    return still_running
