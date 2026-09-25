# -*- coding: utf-8 -*-
"""Crash-safe reads and writes for the desktop's small personal-state files.

config.pkl, lang.pkl and lists.pkl used to be rewritten in place, so a kill, a
crash or a full disk in the middle of a save left a truncated file, which the
next start read as "no settings" or "no lists". ``write_bytes_atomic`` writes
the new bytes to a temporary file in the same folder, flushes them to disk and
renames that file over the target -- the pattern shared/session_persistence.py
already uses for session.json -- so the target is always either the old file or
the new one.

Stdlib-only leaf module: it imports nothing from this project.
"""
import logging
import os
import tempfile
import time

LOGGER = logging.getLogger("genizah." + __name__)

# Windows refuses an open, a delete or a rename while another program holds the
# file (antivirus, the search indexer, a backup tool), and Python raises
# PermissionError (WinError 5 or 32). Such handles are usually brief, so every
# call here retries after these pauses (0.75 s in all), then makes one last
# attempt whose error is raised. A longer ``budget`` continues in 0.5 s steps.
RETRY_DELAYS = (0.05, 0.2, 0.5)
DEFAULT_BUSY_BUDGET = sum(RETRY_DELAYS)


def _pauses(budget):
    """RETRY_DELAYS, then 0.5 s steps, adding up to ``budget`` seconds."""
    pauses, left, i = [], budget, 0
    while left > 1e-6:
        pause = min(RETRY_DELAYS[min(i, len(RETRY_DELAYS) - 1)], left)
        pauses.append(pause)
        left -= pause
        i += 1
    return pauses


def _retry_while_busy(fn, *args, budget=DEFAULT_BUSY_BUDGET):
    for pause in _pauses(budget):
        try:
            return fn(*args)
        except PermissionError:
            time.sleep(pause)
    return fn(*args)


def _read_all(path):
    with open(path, 'rb') as fh:
        return fh.read()


def read_bytes(path, budget=DEFAULT_BUSY_BUDGET):
    """Return the whole file, retrying for up to ``budget`` seconds while Windows reports it busy."""
    return _retry_while_busy(_read_all, path, budget=budget)


def replace_file(src, dst):
    """``os.replace(src, dst)``, retried while Windows reports either file busy."""
    _retry_while_busy(os.replace, src, dst)


def _write_in_place(path, data):
    with open(path, 'wb') as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _discard(path):
    try:
        os.remove(path)
    except OSError:
        pass


def write_bytes_atomic(path, data):
    """Replace the contents of ``path`` with ``data``, all or nothing.

    If Windows still refuses the rename after the retries (a program keeps
    ``path`` open without allowing it to be replaced), the bytes are written in
    place instead, which is what every save did before this module existed: the
    save still lands, and only this fallback can leave a torn file. The
    temporary file never survives an error, and any other error is raised.
    """
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + '.', suffix='.tmp', dir=folder)
    try:
        with os.fdopen(fd, 'wb') as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            replace_file(tmp_path, path)
            return
        except PermissionError as exc:
            blocked = exc
    except BaseException:
        _discard(tmp_path)
        raise
    _discard(tmp_path)
    LOGGER.warning("Could not replace %s (%s); writing it in place instead", path, blocked)
    _retry_while_busy(_write_in_place, path, data)
