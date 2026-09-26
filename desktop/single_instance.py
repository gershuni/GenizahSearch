# -*- coding: utf-8 -*-
"""One running copy of the desktop app per data folder.

Two running copies each keep their own lists, settings and session in memory,
and every save writes one copy's whole state over the shared files in
Config.INDEX_DIR, so the last save silently discards the other window's edits.
A QLockFile there makes a second copy step aside.

Stale locks: a copy that crashed or was killed leaves the lock file behind, and
QLockFile.tryLock removes it by itself once the process named in it is no longer
running. Qt's default stale time (30 s) is kept: an empty or damaged lock file
older than that is recovered, and a live holder is still safe past it because
Windows refuses to delete a file its holder keeps open.

The language-change restart relaunches only after this process has left its
event loop -- after closeEvent and its session save -- and names this process
on the new command line (``--restarted-from=<pid>``). The new copy waits up to
PARENT_WAIT_MS (ten minutes) for that process to exit -- an upload still in
flight can keep it alive for a while -- instead of calling it "already open"
after the usual LAUNCH_WAIT_MS. If it is still running after that, the new copy
reports it as the copy already open. Because nothing can keep this copy open
by then, the restart is only requested when relaunch_looks_possible() says the
command can start, and a launch that fails anyway is reported to the user.
"""
import logging
import os
import subprocess
import sys
import time

from PyQt6.QtCore import QLockFile

LOGGER = logging.getLogger("genizah." + __name__)

LOCK_FILE_NAME = "app.lock"
# A copy the user has just closed can still be in closeEvent when a new launch
# reaches the lock, so every launch waits this long before calling the other
# copy "running".
LAUNCH_WAIT_MS = 3000
# How long a relaunched copy waits for the copy that relaunched it to exit.
PARENT_WAIT_MS = 10 * 60 * 1000
RESTART_FLAG = "--restarted-from"
_TRY_STEP_MS = 1000

_restart_requested = False


def restarted_from(argv):
    """The pid named by ``--restarted-from=<pid>`` in argv, or None."""
    prefix = RESTART_FLAG + "="
    for arg in argv:
        if arg.startswith(prefix):
            try:
                return int(arg[len(prefix):])
            except ValueError:
                return None
    return None


def restart_argv(executable, argv, pid):
    """The command line that relaunches this app, naming ``pid`` as the copy to wait for."""
    prefix = RESTART_FLAG + "="
    return [executable] + [a for a in argv if not a.startswith(prefix)] + [f"{prefix}{pid}"]


def acquire_instance_lock(index_dir, wait_ms=LAUNCH_WAIT_MS, parent_pid=None,
                          parent_wait_ms=PARENT_WAIT_MS):
    """Return (lock, other_copy_running).

    Waits up to ``wait_ms`` for another copy to finish closing; while the holder
    is ``parent_pid`` (the copy that relaunched this one), up to
    ``parent_wait_ms``. Keep the returned lock referenced for the life of the
    process: it is released when unlocked or garbage-collected. If the lock file
    cannot be created at all (permissions, disk), the app runs unguarded rather
    than refusing to start.
    """
    lock = QLockFile(os.path.join(index_dir, LOCK_FILE_NAME))
    start = time.monotonic()
    deadline_ms = wait_ms
    while True:
        left_ms = deadline_ms - (time.monotonic() - start) * 1000
        if lock.tryLock(int(max(0, min(_TRY_STEP_MS, left_ms)))):
            return lock, False
        if lock.error() != QLockFile.LockError.LockFailedError:
            LOGGER.warning("Single-instance lock unavailable in %s (%s); running unguarded",
                           index_dir, lock.error().name)
            return None, False
        # getLockInfo() -> (ok, pid, hostname, appname); the pid means nothing
        # unless ok is True.
        ok, holder_pid, _host, _app = lock.getLockInfo()
        waiting_for_parent = parent_pid is not None and ok and holder_pid == parent_pid
        deadline_ms = parent_wait_ms if waiting_for_parent else wait_ms
        if (time.monotonic() - start) * 1000 >= deadline_ms:
            return None, True


def relaunch_looks_possible(executable=None, argv=None, frozen=None):
    """True when what the relaunch would run is there: the interpreter (or the
    frozen EXE), the script for a script launch, and the working folder.

    Asked before the window closes for a restart. The relaunch itself runs
    only after the event loop has ended, so a command that cannot start then
    leaves nothing running.
    """
    executable = sys.executable if executable is None else executable
    argv = sys.argv if argv is None else argv
    frozen = getattr(sys, "frozen", False) if frozen is None else frozen
    try:
        cwd = os.getcwd()
    except OSError:
        return False
    if not executable or not os.path.isfile(executable) or not os.path.isdir(cwd):
        return False
    if not frozen:
        script = argv[0] if argv else ""
        if not script or not os.path.isfile(os.path.join(cwd, script)):
            return False
    return True


def request_restart():
    """Relaunch the app once its event loop has ended (see relaunch_if_requested)."""
    global _restart_requested
    _restart_requested = True


def relaunch_if_requested(popen=None, on_failure=None):
    """Start the new copy if request_restart() was called. Returns True if one was started.

    Called after ``app.exec()`` returns, so the new copy starts only once this
    one's closeEvent -- and its session save -- has finished. If it cannot be
    started, ``on_failure(error)`` is called -- __main__ passes one that tells
    the user to start the app again, since this copy is about to exit -- and
    False is returned.
    """
    global _restart_requested
    if not _restart_requested:
        return False
    _restart_requested = False
    popen = popen or subprocess.Popen
    try:
        popen(restart_argv(sys.executable, sys.argv, os.getpid()), cwd=os.getcwd())
    except Exception as e:
        LOGGER.error("Could not relaunch the app: %s", e)
        if on_failure is not None:
            try:
                on_failure(e)
            except Exception:
                LOGGER.exception("Could not report the failed relaunch")
        return False
    return True
