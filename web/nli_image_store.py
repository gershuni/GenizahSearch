# -*- coding: utf-8 -*-
"""On-disk stores behind the NLI image proxy (``web/api.py``).

Two stores, both safe to delete (they refill from NLI), both under
``Config.INDEX_DIR`` unless an environment variable moves them:

* **FL-id store** (``nli_fl_ids.db``, SQLite): a manuscript's NLI image ids as
  read from its IIIF manifest, kept ``NLI_DISK_CACHE_TTL`` seconds (30 days). The
  proxy reads it when its 5-minute memory cache misses, so a manuscript resolved
  last week needs no manifest download today. It replaces
  ``nli_fl_ids_cache.json``, which was rewritten from the memory cache on every
  save and so never held more than the last 5 minutes (2026-10-21); that file is
  imported once and renamed ``.imported``.

* **Thumbnail cache** (``nli_thumbs/``): the JPEG of one page at a grid width
  (``NLI_THUMB_CACHE_WIDTHS``, 300), written the first time the proxy gets it from
  NLI's IIIF server and served from disk after that -- before the NLI circuit
  breaker is consulted, so a cached thumbnail still shows during an NLI outage.
  Capped by size (``NLI_THUMB_CACHE_MAX_MB``) and by the disk's free space
  (``NLI_THUMB_CACHE_MIN_FREE_MB``). A grid of thumbnails nobody had opened used to lose cards when more
  than 8 needed a manifest at once (``NLI_MAX_CONCURRENT_FETCHES``, 1-second
  wait). Filling it ahead of time: docs/plans/NLI_THUMBNAIL_PREFETCH_PLAN.md.

The locations are read from the environment on every call (a dict lookup), so
tests point them at a temporary directory per test (``tests/conftest.py``).
Every error here is logged and treated as a miss: these stores make images
faster, they must never make one fail.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time

from shared.config import Config
from shared.synthetic_sys_id import is_synthetic_sys_id
from shared.sys_id_patterns import CORPUS_SYS_ID_RE

logger = logging.getLogger(__name__)

# Widths kept on disk, px: the ones the grids ask for (Visual Similarity, search
# results and Joins Lab Small/Medium all fetch 300). Every other width stays in
# memory only, so asking for many widths cannot store many copies of one image.
NLI_THUMB_CACHE_WIDTHS = frozenset(
    int(w) for w in os.environ.get('NLI_THUMB_CACHE_WIDTHS', '300').split(',') if w.strip().isdigit()
)
# A write that would take the folder past this size, or the disk below this much
# free space, is skipped. In MB; 0 turns that limit off.
NLI_THUMB_CACHE_MAX_MB = max(0, int(os.environ.get('NLI_THUMB_CACHE_MAX_MB', '10240')))
NLI_THUMB_CACHE_MIN_FREE_MB = max(0, int(os.environ.get('NLI_THUMB_CACHE_MIN_FREE_MB', '2048')))

_JPEG_MAGIC = b'\xff\xd8'
_LEGACY_JSON_NAME = 'nli_fl_ids_cache.json'

_stats_lock = threading.Lock()
_stats = {
    'thumb_hits': 0,
    'thumb_misses': 0,
    'thumb_writes': 0,
    'thumb_skipped_low_disk': 0,
    'thumb_skipped_full': 0,
    'thumb_skipped_counting': 0,
    'fl_id_hits': 0,
    'fl_id_misses': 0,
    'fl_id_writes': 0,
    'errors': 0,
}


def _count(name: str, n: int = 1) -> None:
    with _stats_lock:
        _stats[name] += n


def _location(env_name: str, default_name: str) -> str | None:
    """The path an environment variable names; unset means under INDEX_DIR, empty means off."""
    value = os.environ.get(env_name)
    if value is None:
        return os.path.join(Config.INDEX_DIR, default_name)
    value = value.strip()
    return value or None


def fl_id_store_path() -> str | None:
    """The FL-id store's SQLite file (``NLI_FL_ID_STORE_FILE``), or None when it is off."""
    return _location('NLI_FL_ID_STORE_FILE', 'nli_fl_ids.db')


def thumb_cache_dir() -> str | None:
    """The thumbnail cache's directory (``NLI_THUMB_CACHE_DIR``), or None when it is off."""
    return _location('NLI_THUMB_CACHE_DIR', 'nli_thumbs')


def stats() -> dict:
    """Counters since the process started, for ``/_internal/memstat``."""
    with _stats_lock:
        snapshot = dict(_stats)
    snapshot['fl_id_store'] = fl_id_store_path()
    snapshot['thumb_dir'] = thumb_cache_dir()
    snapshot['thumb_widths'] = sorted(NLI_THUMB_CACHE_WIDTHS)
    with _size_lock:
        snapshot['thumb_bytes'] = _size['bytes'] if _size['root'] == snapshot['thumb_dir'] else None
    snapshot['thumb_max_mb'] = NLI_THUMB_CACHE_MAX_MB
    return snapshot


# ── FL-id store ───────────────────────────────────────────────────────────────

def normalize_fl_ids(fl_ids) -> list[str]:
    """FL ids as a clean list of digit strings ('FL123' -> '123'); anything else is dropped."""
    normalized = []
    for fl_id in fl_ids or []:
        digits = re.sub(r'\D', '', str(fl_id))
        if digits:
            normalized.append(digits)
    return normalized


def _connect(path: str) -> sqlite3.Connection:
    # One short-lived connection per call: safe from any threadpool thread, and a
    # test that deletes the file between calls gets a fresh store.
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5)
    conn.execute(
        'CREATE TABLE IF NOT EXISTS fl_ids ('
        'cache_key TEXT PRIMARY KEY, fl_ids TEXT NOT NULL, cached_at REAL NOT NULL)'
    )
    return conn


def fl_id_store_get(cache_key: str, ttl_seconds: int, *, path: str | None = None,
                    now: float | None = None) -> list[str] | None:
    """The stored FL ids for ``cache_key`` if younger than ``ttl_seconds``, else None."""
    path = path or fl_id_store_path()
    if not path or ttl_seconds <= 0:
        return None
    now = time.time() if now is None else now
    try:
        conn = _connect(path)
        try:
            row = conn.execute(
                'SELECT fl_ids, cached_at FROM fl_ids WHERE cache_key = ?', (cache_key,)
            ).fetchone()
        finally:
            conn.close()
        fl_ids = normalize_fl_ids(json.loads(row[0])) if row and now - row[1] < ttl_seconds else []
    except (sqlite3.Error, OSError, ValueError) as e:
        _count('errors')
        logger.warning("NLI FL-id store read failed for %s (%s): %s", cache_key, path, e)
        return None
    _count('fl_id_hits' if fl_ids else 'fl_id_misses')
    return fl_ids or None


def fl_id_store_put(cache: dict, cache_time: dict, ttl_seconds: int, *,
                    path: str | None = None, now: float | None = None,
                    replace: bool = True) -> bool:
    """Write positive entries (``key -> FL ids``, each with its time in ``cache_time``).

    Entries that are not a non-empty FL-id list, or already older than
    ``ttl_seconds``, are skipped. ``replace=False`` keeps a row that is already
    there (the one-time import of the old JSON file). True when written or
    nothing to write.
    """
    path = path or fl_id_store_path()
    if not path or ttl_seconds <= 0:
        return True
    now = time.time() if now is None else now
    rows = []
    for key, fl_ids in cache.items():
        cached_at = cache_time.get(key)
        if not isinstance(fl_ids, list) or not isinstance(cached_at, (int, float)):
            continue
        if now - float(cached_at) >= ttl_seconds:
            continue
        normalized = normalize_fl_ids(fl_ids)
        if normalized:
            rows.append((str(key), json.dumps(normalized), float(cached_at)))
    if not rows:
        return True
    verb = 'INSERT OR REPLACE' if replace else 'INSERT OR IGNORE'
    try:
        conn = _connect(path)
        try:
            with conn:
                conn.executemany(f'{verb} INTO fl_ids (cache_key, fl_ids, cached_at) VALUES (?, ?, ?)', rows)
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as e:
        _count('errors')
        logger.warning("NLI FL-id store write failed (%s): %s", path, e)
        return False
    _count('fl_id_writes', len(rows))
    return True


def fl_id_store_prepare(ttl_seconds: int, *, now: float | None = None) -> int:
    """At startup: import the old JSON cache once, then drop expired rows.

    The JSON file sits beside the store (``nli_fl_ids_cache.json``) and is renamed
    ``.imported`` after a successful import, so it is read once; a row the store
    already has is kept. Returns how many entries the old file held.
    """
    path = fl_id_store_path()
    if not path or ttl_seconds <= 0:
        return 0
    now = time.time() if now is None else now
    imported = 0
    legacy = os.path.join(os.path.dirname(path), _LEGACY_JSON_NAME)
    if os.path.exists(legacy):
        try:
            with open(legacy, 'r', encoding='utf-8') as handle:
                payload = json.load(handle)
            entries = payload.get('entries', {}) if isinstance(payload, dict) else {}
            cache, cache_time = {}, {}
            for key, entry in entries.items():
                if isinstance(entry, dict):
                    cache[str(key)] = entry.get('fl_ids')
                    cache_time[str(key)] = entry.get('cached_at')
            if fl_id_store_put(cache, cache_time, ttl_seconds, path=path, now=now, replace=False):
                imported = len(cache)
                os.replace(legacy, legacy + '.imported')
                logger.info("Read %d NLI FL-id entries from %s into the store", imported, legacy)
        except (OSError, ValueError) as e:
            logger.warning("Could not import the old NLI FL-id cache %s: %s", legacy, e)
    try:
        conn = _connect(path)
        try:
            with conn:
                conn.execute('DELETE FROM fl_ids WHERE cached_at <= ?', (now - ttl_seconds,))
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as e:
        logger.warning("NLI FL-id store cleanup failed (%s): %s", path, e)
    return imported


# ── Thumbnail cache ───────────────────────────────────────────────────────────

def thumb_path(sys_id: str, page: int, width: int, suffix: int = 1,
               root: str | None = None) -> str | None:
    """Where the thumbnail of (sys_id, page, width, suffix) lives, or None if it is not kept.

    Layout: ``<root>/w<width>/<first 2 hex of md5(sys_id)>/<sys_id>_s<suffix>_p<page>.jpg``.
    Only corpus sys_ids, pages from 0, suffixes from 1 and the widths in
    ``NLI_THUMB_CACHE_WIDTHS``; the sys_id check is also what keeps a request's
    path out of the file name. The prefetch script is to use this same function.
    """
    root = root or thumb_cache_dir()
    if not root or not isinstance(sys_id, str):
        return None
    if width not in NLI_THUMB_CACHE_WIDTHS or page < 0 or suffix < 1:
        return None
    if not CORPUS_SYS_ID_RE.fullmatch(sys_id) or is_synthetic_sys_id(sys_id):
        return None
    shard = hashlib.md5(sys_id.encode('ascii')).hexdigest()[:2]
    return os.path.join(root, f'w{int(width)}', shard, f'{sys_id}_s{int(suffix)}_p{int(page)}.jpg')


# The folder's size, for NLI_THUMB_CACHE_MAX_MB: counted once per process -- in a
# background thread when the folder already exists, started at startup by
# ``thumb_cache_prepare`` -- then kept up to date by the writes. Nothing is written
# until the count is in, because the folder may already be full (Codex on #427).
_size_lock = threading.Lock()
_size = {'root': None, 'bytes': None}
# Thumbnails being written now. A second request for the same thumbnail does not
# write it again, so one file is never counted twice (Codex on #427).
_writing: set = set()


def _folder_bytes(root: str) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if name.endswith('.jpg'):
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    pass
    return total


def _count_folder(root: str) -> None:
    try:
        total = _folder_bytes(root)
    except Exception as e:  # unknown size: keep writes off rather than guess
        logger.warning("Could not size the NLI thumbnail cache %s; none are kept until a restart: %s", root, e)
        return
    with _size_lock:
        if _size['root'] == root:
            _size['bytes'] = total
    logger.info("NLI thumbnail cache %s holds %.1f MB", root, total / (1024 * 1024))


def _start_count_locked(root: str) -> None:
    exists = os.path.isdir(root)
    _size.update(root=root, bytes=None if exists else 0)
    if exists:
        threading.Thread(target=_count_folder, args=(root,), daemon=True, name='nli-thumb-size').start()


def thumb_cache_prepare() -> None:
    """At startup: start counting the thumbnail folder, so thumbnails are kept again soon."""
    root = thumb_cache_dir()
    if root and NLI_THUMB_CACHE_MAX_MB:
        with _size_lock:
            if _size['root'] != root:
                _start_count_locked(root)


def _reserve(root: str, n: int) -> str:
    """Count ``n`` more bytes against NLI_THUMB_CACHE_MAX_MB: 'ok', 'counting' or 'full'."""
    cap = NLI_THUMB_CACHE_MAX_MB * 1024 * 1024
    if not cap:
        return 'ok'
    with _size_lock:
        if _size['root'] != root:
            _start_count_locked(root)
        if _size['bytes'] is None:
            return 'counting'
        if _size['bytes'] + n > cap:
            return 'full'
        _size['bytes'] += n
        return 'ok'


def _release(root: str, n: int) -> None:
    with _size_lock:
        if _size['root'] == root and _size['bytes'] is not None:
            _size['bytes'] -= n


def thumb_get(sys_id: str, page: int, width: int, suffix: int = 1) -> bytes | None:
    """The stored JPEG bytes, or None."""
    path = thumb_path(sys_id, page, width, suffix)
    if path is None:
        return None
    try:
        with open(path, 'rb') as handle:
            data = handle.read()
    except FileNotFoundError:
        _count('thumb_misses')
        return None
    except OSError as e:
        _count('errors')
        logger.warning("NLI thumbnail read failed (%s): %s", path, e)
        return None
    if not data.startswith(_JPEG_MAGIC):
        _count('errors')
        logger.warning("NLI thumbnail %s is not a JPEG; ignored", path)
        return None
    _count('thumb_hits')
    return data


def thumb_put(sys_id: str, page: int, width: int, suffix: int, content: bytes) -> bool:
    """Keep a JPEG fetched from NLI's IIIF server. True when written.

    Not written: anything that is not a JPEG, a key ``thumb_path`` refuses, a
    thumbnail already kept or being written by another request, a write past
    ``NLI_THUMB_CACHE_MAX_MB`` or before the folder is counted, or when the disk
    is below ``NLI_THUMB_CACHE_MIN_FREE_MB``. Written atomically (temporary file,
    then rename), so a reader never sees half a file.
    """
    root = thumb_cache_dir()
    path = thumb_path(sys_id, page, width, suffix, root=root)
    if path is None or not content or not content.startswith(_JPEG_MAGIC):
        return False
    with _size_lock:
        if path in _writing or os.path.exists(path):
            return False
        _writing.add(path)
    try:
        return _write_new_thumb(root, path, content)
    finally:
        with _size_lock:
            _writing.discard(path)


def _write_new_thumb(root: str, path: str, content: bytes) -> bool:
    verdict = _reserve(root, len(content))
    if verdict != 'ok':
        _count('thumb_skipped_counting' if verdict == 'counting' else 'thumb_skipped_full')
        return False
    directory = os.path.dirname(path)
    temp_path = f'{path}.tmp.{os.getpid()}.{threading.get_ident()}'
    try:
        os.makedirs(directory, exist_ok=True)
        if shutil.disk_usage(directory).free < NLI_THUMB_CACHE_MIN_FREE_MB * 1024 * 1024:
            _count('thumb_skipped_low_disk')
            _release(root, len(content))
            return False
        with open(temp_path, 'wb') as handle:
            handle.write(content)
        os.replace(temp_path, path)
    except OSError as e:
        _count('errors')
        _release(root, len(content))
        logger.warning("NLI thumbnail write failed (%s): %s", path, e)
        try:
            os.remove(temp_path)
        except OSError:
            pass
        return False
    _count('thumb_writes')
    return True
