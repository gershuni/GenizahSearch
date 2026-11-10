"""Cross-process reader/swap leases, stored outside the renamed index folders."""
from contextlib import contextmanager, ExitStack
from functools import wraps
import os
from pathlib import Path
import sqlite3
import threading

_held = threading.local()


@contextmanager
def index_leases(paths, *, exclusive=False):
    # A dedicated rollback-mode SQLite file supplies OS-backed shared/exclusive
    # locks on both Windows and POSIX, released automatically after a crash.
    held = getattr(_held, 'paths', None)
    if held is None:
        held = _held.paths = {}
    with ExitStack() as stack:
        for path in sorted({os.path.normcase(os.path.abspath(p)) for p in paths if p}):
            if path in held:
                if exclusive and not held[path]:
                    raise RuntimeError('Cannot upgrade an active local-index read lease')
                continue
            lease = Path(path + '.research-lease.sqlite3')
            lease.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(lease, timeout=1, isolation_level=None)
            stack.callback(connection.close)
            while True:
                try:
                    connection.execute('BEGIN EXCLUSIVE' if exclusive else 'BEGIN')
                    if not exclusive:
                        connection.execute('SELECT name FROM sqlite_master').fetchall()
                    break
                except sqlite3.OperationalError as exc:
                    connection.rollback()
                    if getattr(exc, 'sqlite_errorcode', None) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                        raise
            held[path] = exclusive
            stack.callback(held.pop, path)
        yield


def index_swap(function):
    """Keep readers out until a rebuild/reset has closed, swapped and reloaded."""
    @wraps(function)
    def wrapped(self, *args, **kwargs):
        with index_leases([self._index_dir, self._lab_index_dir], exclusive=True):
            return function(self, *args, **kwargs)
    return wrapped
