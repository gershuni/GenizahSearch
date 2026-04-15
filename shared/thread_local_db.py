# -*- coding: utf-8 -*-
"""
Thread-local SQLite connection pool for read-only sidecar databases.

Provides ThreadLocalConnection — a drop-in replacement for sqlite3.Connection
that creates one real connection per thread via threading.local(). This makes
concurrent access from NiceGUI's run.io_bound() thread pool safe without
requiring external locking.

Usage:
    # Instead of:
    #   conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10)
    #   conn.row_factory = sqlite3.Row
    # Use:
    conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row)

    # All standard methods work identically:
    cursor = conn.execute("SELECT * FROM t WHERE id = ?", (some_id,))
    rows = cursor.fetchall()
"""

import logging
import sqlite3
import threading

logger = logging.getLogger(__name__)


class ThreadLocalConnection:
    """Thread-safe SQLite connection pool using one connection per thread.

    Each thread lazily gets its own sqlite3.Connection. This avoids the
    SQLITE_MISUSE ("bad parameter or other API misuse") errors caused by
    concurrent cursor operations on a single shared connection — even with
    check_same_thread=False, which only disables Python's thread-identity
    assertion but does NOT make concurrent operations safe.

    All per-thread connections share the same URI, row_factory, and timeout.
    Designed for read-only sidecar databases (?mode=ro in the URI).

    Connections from dead threads are automatically pruned when new threads
    create connections, preventing unbounded growth in long-lived processes.
    """

    def __init__(
        self,
        uri: str,
        *,
        row_factory=None,
        timeout: float = 10.0,
        is_uri: bool = True,
    ):
        """
        Args:
            uri: SQLite connection string (e.g. "file:/path/to/db?mode=ro").
            row_factory: Optional row factory (e.g. sqlite3.Row).
            timeout: Connection timeout in seconds.
            is_uri: Whether to pass uri=True to sqlite3.connect().
        """
        self._uri = uri
        self._row_factory = row_factory
        self._timeout = timeout
        self._is_uri = is_uri
        self._local = threading.local()
        # Map thread ident -> connection; pruned on new-thread creation
        self._conns: dict[int, sqlite3.Connection] = {}
        self._lock = threading.Lock()

    def _prune_dead(self):
        """Close and remove connections belonging to threads that no longer exist.

        Called under self._lock whenever a new thread registers a connection.
        """
        alive = {t.ident for t in threading.enumerate()}
        dead_idents = [tid for tid in self._conns if tid not in alive]
        for tid in dead_idents:
            try:
                self._conns[tid].close()
            except Exception:
                pass  # conn.close() can raise ProgrammingError; safe to ignore during cleanup
            del self._conns[tid]

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create the connection for the current thread."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self._uri,
                uri=self._is_uri,
                check_same_thread=False,
                timeout=self._timeout,
            )
            if self._row_factory is not None:
                conn.row_factory = self._row_factory
            self._local.conn = conn
            with self._lock:
                # Prune first so a reused thread ident doesn't silently
                # overwrite (and leak) a stale connection from a dead thread.
                self._prune_dead()
                self._conns[threading.current_thread().ident] = conn
        else:
            # Existing thread — still prune so dead-thread connections
            # don't linger when no new threads arrive.
            with self._lock:
                self._prune_dead()
        return conn

    # ── sqlite3.Connection interface (read-only subset) ──────────

    def execute(self, sql: str, parameters=()) -> sqlite3.Cursor:
        """Execute SQL on the current thread's connection."""
        return self._get_conn().execute(sql, parameters)

    @property
    def row_factory(self):
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._row_factory = value
        # Update current thread's connection if it exists
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.row_factory = value

    def close(self):
        """Close all per-thread connections and reset state."""
        with self._lock:
            for conn in self._conns.values():
                try:
                    conn.close()
                except Exception:
                    pass  # conn.close() can raise ProgrammingError; safe to ignore during shutdown
            self._conns.clear()
        # Reset thread-local storage so new connections can be created
        self._local = threading.local()

    def __bool__(self):
        """Truthy — allows `if self._conn:` availability checks to pass."""
        return True
