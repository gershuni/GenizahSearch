# -*- coding: utf-8 -*-
"""The parsed NLI catalogue answers, kept on disk so an NLI outage does not empty a page.

``MetadataManager.fetch_marc_data`` and ``fetch_iiif_manifest`` (shared/metadata_manager.py)
ask the National Library of Israel for a manuscript's MARC record and IIIF manifest on
every browse whose in-memory caches miss. When NLI fails -- a 5xx streak, a timeout, or
the shared circuit breaker open after one (shared/nli_circuit_breaker.py) -- they used
to return an empty answer, and /browse lost its catalogue details, its image labels and,
for a manuscript with no other image source, its images. PostHog counted 4,338 breaker
opens in the 30 days to 2026-11-03.

This store keeps the PARSED result of each successful fetch (JSON, one row per kind,
sys_id and manifest suffix) in one SQLite file beside the NLI image proxy's FL-id store
(web/nli_image_store.py):

* a row younger than ``NLI_METADATA_STORE_TTL`` (30 days) answers without a network
  call;
* an older row is refreshed, and when NLI cannot answer -- breaker open, negative cache,
  a failed or unparseable fetch -- the old row is served instead, marked
  ``stale: True`` with its ``fetched_at``, so the page can say what it is showing;
* only a successful parse with content is written: an error page, a placeholder, or a
  record with nothing in it is never stored, so the fallback can never be an error.

Synthetic sys_ids (Phase 85 D-14) never reach the store: the callers return before it,
and the store refuses them too. The file is a cache: deleting it is safe, it refills.
The location is read from the environment on every call (a dict lookup), so tests point
it at a temporary file per test (``tests/conftest.py``). Every error here is logged and
treated as a miss; the store makes a page fuller, it must never make one fail. One
short-lived connection per call: safe from the worker threads ``enrich_metadata`` uses.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import NamedTuple

from shared.config import Config
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)

KINDS = frozenset({'marc', 'manifest'})

# How long a stored answer is served WITHOUT asking NLI again, seconds. 0 means every
# call asks NLI and the row only serves an outage; the file keeps rows whatever the TTL.
NLI_METADATA_STORE_TTL = max(0, int(os.environ.get('NLI_METADATA_STORE_TTL', str(30 * 24 * 60 * 60))))
# The row cap. A write past it drops the oldest-fetched rows (about 2 KB a row: a MARC
# record's lists, or a manifest's page labels). 0 = no cap.
NLI_METADATA_STORE_MAX_ROWS = max(0, int(os.environ.get('NLI_METADATA_STORE_MAX_ROWS', '200000')))

_stats_lock = threading.Lock()
_stats = {
    'fresh_hits': 0,     # a row young enough to answer in NLI's place
    'stale_hits': 0,     # an old row read because NLI may be asked (served only if NLI fails)
    'misses': 0,
    'writes': 0,
    'pruned': 0,
    'errors': 0,
}


class StoredAnswer(NamedTuple):
    """One stored answer: the parsed result, when it was fetched, and whether it is within the TTL."""
    payload: dict
    fetched_at: float
    fresh: bool


def _count(name: str, n: int = 1) -> None:
    with _stats_lock:
        _stats[name] += n


def metadata_store_path() -> str | None:
    """The store's SQLite file (``NLI_METADATA_STORE_FILE``), or None when it is off.

    Unset means ``<INDEX_DIR>/nli_metadata.db``; an empty value turns the store off.
    """
    value = os.environ.get('NLI_METADATA_STORE_FILE')
    if value is None:
        return os.path.join(Config.INDEX_DIR, 'nli_metadata.db')
    value = value.strip()
    return value or None


def _usable_key(kind: str, sys_id) -> bool:
    return (kind in KINDS and isinstance(sys_id, str) and bool(sys_id)
            and not is_synthetic_sys_id(sys_id))


def _connect(path: str) -> sqlite3.Connection:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5)
    conn.execute(
        'CREATE TABLE IF NOT EXISTS nli_metadata ('
        'kind TEXT NOT NULL, sys_id TEXT NOT NULL, suffix INTEGER NOT NULL, '
        'payload TEXT NOT NULL, fetched_at REAL NOT NULL, '
        'PRIMARY KEY (kind, sys_id, suffix))'
    )
    conn.execute('CREATE INDEX IF NOT EXISTS nli_metadata_fetched_at ON nli_metadata (fetched_at)')
    return conn


def metadata_store_get(kind: str, sys_id: str, suffix: int = 1, *,
                       ttl_seconds: int | None = None, path: str | None = None,
                       now: float | None = None) -> StoredAnswer | None:
    """The stored answer for (kind, sys_id, suffix), fresh or not, or None when there is none.

    The caller decides what a stale answer is worth: ``fresh`` is False once the row is
    ``ttl_seconds`` old (``NLI_METADATA_STORE_TTL`` by default).
    """
    path = path or metadata_store_path()
    if not path or not _usable_key(kind, sys_id):
        return None
    ttl = NLI_METADATA_STORE_TTL if ttl_seconds is None else max(0, int(ttl_seconds))
    now = time.time() if now is None else now
    try:
        conn = _connect(path)
        try:
            row = conn.execute(
                'SELECT payload, fetched_at FROM nli_metadata WHERE kind = ? AND sys_id = ? AND suffix = ?',
                (kind, sys_id, int(suffix)),
            ).fetchone()
        finally:
            conn.close()
        payload = json.loads(row[0]) if row else None
        if not isinstance(payload, dict):
            payload = None
    except (sqlite3.Error, OSError, ValueError, TypeError) as e:
        _count('errors')
        logger.warning("NLI metadata store read failed for %s %s/%s (%s): %s", kind, sys_id, suffix, path, e)
        return None
    if payload is None:
        _count('misses')
        return None
    fetched_at = float(row[1])
    fresh = now - fetched_at < ttl
    _count('fresh_hits' if fresh else 'stale_hits')
    return StoredAnswer(payload, fetched_at, fresh)


def metadata_store_put(kind: str, sys_id: str, suffix: int, payload: dict, *,
                       path: str | None = None, now: float | None = None,
                       max_rows: int | None = None) -> bool:
    """Keep a parsed answer. Only a non-empty dict is written; a cap past is pruned oldest first.

    True when written (or the store is off); False when nothing was kept.
    """
    path = path or metadata_store_path()
    if not path:
        return True
    if not _usable_key(kind, sys_id) or not has_content(payload):
        return False
    cap = NLI_METADATA_STORE_MAX_ROWS if max_rows is None else max(0, int(max_rows))
    now = time.time() if now is None else now
    try:
        body = json.dumps(_without_serving_marks(payload), ensure_ascii=False)
        conn = _connect(path)
        try:
            with conn:
                conn.execute(
                    'INSERT OR REPLACE INTO nli_metadata (kind, sys_id, suffix, payload, fetched_at) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (kind, sys_id, int(suffix), body, float(now)),
                )
                pruned = _prune_locked(conn, cap)
        finally:
            conn.close()
    except (sqlite3.Error, OSError, TypeError, ValueError) as e:
        _count('errors')
        logger.warning("NLI metadata store write failed for %s %s/%s (%s): %s", kind, sys_id, suffix, path, e)
        return False
    _count('writes')
    if pruned:
        _count('pruned', pruned)
    return True


def _prune_locked(conn: sqlite3.Connection, cap: int) -> int:
    """Inside a transaction: drop the oldest-fetched rows past ``cap``. Returns how many."""
    if cap <= 0:
        return 0
    (count,) = conn.execute('SELECT COUNT(*) FROM nli_metadata').fetchone()
    excess = int(count) - cap
    if excess <= 0:
        return 0
    conn.execute(
        'DELETE FROM nli_metadata WHERE rowid IN ('
        'SELECT rowid FROM nli_metadata ORDER BY fetched_at ASC, rowid ASC LIMIT ?)',
        (excess,),
    )
    return excess


def metadata_store_prepare() -> None:
    """At startup: bring the file under the row cap (a lowered cap, or a cap added to a full file).

    Old rows are NOT dropped by age: an old row is what an outage is served from.
    """
    path = metadata_store_path()
    if not path or NLI_METADATA_STORE_MAX_ROWS <= 0:
        return
    try:
        conn = _connect(path)
        try:
            with conn:
                pruned = _prune_locked(conn, NLI_METADATA_STORE_MAX_ROWS)
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as e:
        _count('errors')
        logger.warning("NLI metadata store cleanup failed (%s): %s", path, e)
        return
    if pruned:
        _count('pruned', pruned)


# ── What a stored answer looks like to a caller ───────────────────────────────

# Keys the store adds to an answer it serves in NLI's place. Never written back.
STALE_KEY = 'stale'
FETCHED_AT_KEY = 'fetched_at'


def has_content(payload) -> bool:
    """A parsed answer worth keeping: a dict with at least one non-empty field.

    An NLI error page that happens to parse (an XML record with no datafields, a JSON
    object without sequences) parses to the empty shape and is not kept, so the
    fallback can never be an error page.
    """
    if not isinstance(payload, dict):
        return False
    return any(value for key, value in payload.items() if key not in (STALE_KEY, FETCHED_AT_KEY))


def _without_serving_marks(payload: dict) -> dict:
    return {key: value for key, value in payload.items() if key not in (STALE_KEY, FETCHED_AT_KEY)}


def served_stale(stored: StoredAnswer) -> dict:
    """The stored answer as the caller returns it when NLI could not answer now.

    A copy of the payload with ``stale: True`` and ``fetched_at`` (epoch seconds), so
    the page can say "catalogue details from <date>".
    """
    answer = dict(stored.payload)
    answer[STALE_KEY] = True
    answer[FETCHED_AT_KEY] = stored.fetched_at
    return answer


def stale_from(*answers) -> float | None:
    """The oldest ``fetched_at`` among the answers served stale, or None when none was."""
    times = [a.get(FETCHED_AT_KEY) for a in answers
             if isinstance(a, dict) and a.get(STALE_KEY) and isinstance(a.get(FETCHED_AT_KEY), (int, float))]
    return min(times) if times else None


# ── Diagnostics ───────────────────────────────────────────────────────────────

def stats() -> dict:
    """Counters since the process started plus the file's row count and size, for ``/_internal/memstat``."""
    with _stats_lock:
        snapshot = dict(_stats)
    path = metadata_store_path()
    snapshot['file'] = path
    snapshot['ttl_seconds'] = NLI_METADATA_STORE_TTL
    snapshot['max_rows'] = NLI_METADATA_STORE_MAX_ROWS
    snapshot['rows'] = None
    snapshot['bytes'] = None
    if path and os.path.exists(path):
        try:
            snapshot['bytes'] = os.path.getsize(path)
            conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=5)
            try:
                snapshot['rows'] = conn.execute('SELECT COUNT(*) FROM nli_metadata').fetchone()[0]
            finally:
                conn.close()
        except (sqlite3.Error, OSError) as e:
            logger.debug("NLI metadata store stats failed (%s): %s", path, e)
    return snapshot


def _reset_for_tests() -> None:
    with _stats_lock:
        for key in _stats:
            _stats[key] = 0


__all__ = [
    'KINDS',
    'NLI_METADATA_STORE_TTL',
    'NLI_METADATA_STORE_MAX_ROWS',
    'StoredAnswer',
    'metadata_store_path',
    'metadata_store_get',
    'metadata_store_put',
    'metadata_store_prepare',
    'has_content',
    'served_stale',
    'stale_from',
    'stats',
]
