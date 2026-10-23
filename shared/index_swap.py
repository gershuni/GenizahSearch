# -*- coding: utf-8 -*-
"""Building the main index beside the live one, and swapping it in.

The desktop rebuild used to delete ``tantivy_db`` first and build in its place, so for the
twenty-odd minutes of a build there was no index to search, and a crash left none at all.
Now ``Indexer.create_index(beside=True)`` builds into ``tantivy_db.new`` (its own seams and
marker; the browse map into ``browse_map.pkl.new``) while the open index keeps serving.
When the build has returned, the app closes its index and calls :func:`swap_in_staged_index`:

    tantivy_db  ->  tantivy_db.old        (one rename)
    tantivy_db.new  ->  tantivy_db        (one rename)
    browse_map.pkl.new  ->  browse_map.pkl

then reopens the index and removes ``tantivy_db.old`` (:func:`remove_old_index`: on
Windows the old memory-mapped files refuse deletion until every handle is gone, so it
retries, and what it cannot remove the next start does).

:func:`recover_index_dirs` runs at every start, before the index is opened. The states a
crash or a shutdown can leave are told apart by what exists:

* ``tantivy_db`` missing, ``.old`` and ``.new`` both present -- a swap interrupted between
  its two renames; ``.new`` is a finished build (a swap starts only after one), so it is
  moved into place.
* ``tantivy_db`` missing, only ``.old`` -- restored.
* otherwise every ``.new`` (a build that did not finish, or was never installed) and every
  ``.old`` (a swap whose cleanup did not finish) is junk: renamed aside at once and deleted
  in the background (``tantivy_db.discard-*``), so a user is never left without an index
  and never waits on a 14 GB deletion.

:func:`plan_build` decides whether a beside-build fits on the disk: it needs about the
live index's size plus :data:`BESIDE_BUILD_HEADROOM` (format 2 is about 1.4 GB larger than
format 1 on the full corpus). Without that room the build runs in place, as before, and
the app says so.
"""
from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from collections import namedtuple

from shared.config import Config

LOGGER = logging.getLogger("genizah." + __name__)

LIVE_INDEX_DIRNAME = 'tantivy_db'
STAGED_SUFFIX = '.new'
OLD_SUFFIX = '.old'
DISCARD_INFIX = '.discard-'
BESIDE_BUILD_HEADROOM = int(1.5 * 1024 ** 3)
GIB = 1024 ** 3

#: ``mode``: ``'beside'`` (a live index, and room for a second one), ``'inplace'`` (a live
#: index, no room) or ``'fresh'`` (no live index: nothing to keep serving, so the build
#: goes straight to ``tantivy_db``). Sizes in bytes.
BuildPlan = namedtuple('BuildPlan', 'mode needed free live')


def live_index_path(index_dir=None):
    return os.path.join(index_dir or Config.INDEX_DIR, LIVE_INDEX_DIRNAME)


def staged_index_path(index_dir=None):
    return live_index_path(index_dir) + STAGED_SUFFIX


def old_index_path(index_dir=None):
    return live_index_path(index_dir) + OLD_SUFFIX


def staged_browse_map_path(browse_map=None):
    return (browse_map or Config.BROWSE_MAP) + STAGED_SUFFIX


def dir_size(path):
    """Bytes under *path* (0 when it does not exist); follows no links."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def free_bytes(path):
    """The free space of the volume holding *path* (its nearest existing parent)."""
    probe = os.path.abspath(path)
    while probe and not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return shutil.disk_usage(probe).free


def plan_build(index_dir=None, headroom=BESIDE_BUILD_HEADROOM):
    """How the next build should run (see :data:`BuildPlan`)."""
    index_dir = index_dir or Config.INDEX_DIR
    live = live_index_path(index_dir)
    live_bytes = dir_size(live) if os.path.isdir(live) else 0
    free = free_bytes(index_dir)
    if not live_bytes:
        return BuildPlan('fresh', 0, free, 0)
    needed = live_bytes + headroom
    return BuildPlan('beside' if free >= needed else 'inplace', needed, free, live_bytes)


def gib(n_bytes):
    """*n_bytes* as a GB figure for a sentence: one decimal."""
    return f"{n_bytes / GIB:.1f}"


def _rename_retry(src, dst, attempts=10, delay=0.3):
    """``os.rename`` that waits out Windows' lazily released handles (the closed index's
    memory maps, an antivirus scan): a PermissionError is retried, anything else raised."""
    for attempt in range(attempts):
        try:
            os.rename(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay)


def _discard_name(index_dir):
    base = live_index_path(index_dir) + DISCARD_INFIX
    name = f"{base}{int(time.time())}"
    n = 0
    while os.path.exists(name):
        n += 1
        name = f"{base}{int(time.time())}-{n}"
    return name


def discard_staged_index(index_dir=None, browse_map=None):
    """Remove a staged build (after a cancel or a failure). Every handle on it must be
    closed first. Nothing of the live index is touched; errors are logged, not raised."""
    staged = staged_index_path(index_dir)
    if os.path.isdir(staged):
        shutil.rmtree(staged, ignore_errors=True)
        if os.path.isdir(staged):
            # Still held (Windows): out of the way now, deleted at the next start.
            try:
                os.rename(staged, _discard_name(index_dir or Config.INDEX_DIR))
            except OSError:
                LOGGER.warning("Could not discard the staged index %s", staged, exc_info=True)
    try:
        os.remove(staged_browse_map_path(browse_map))
    except FileNotFoundError:
        pass
    except OSError:
        LOGGER.warning("Could not remove the staged browse map", exc_info=True)


def swap_in_staged_index(index_dir=None, browse_map=None):
    """Put the staged build in place: ``tantivy_db`` -> ``.old``, ``.new`` -> ``tantivy_db``,
    then the browse map. Call with every handle on the live index closed. Returns the old
    index's path (to remove once the new one is open) or None when there was none.
    When the second rename fails the first is undone, so the live index stays."""
    index_dir = index_dir or Config.INDEX_DIR
    live, staged, old = live_index_path(index_dir), staged_index_path(index_dir), old_index_path(index_dir)
    if not os.path.isdir(staged):
        raise FileNotFoundError(f"No staged index at {staged}")
    if os.path.exists(old):
        # A previous swap's leftover that the start did not get to: out of the way.
        _rename_retry(old, _discard_name(index_dir))
    had_live = os.path.isdir(live)
    if had_live:
        _rename_retry(live, old)
    try:
        _rename_retry(staged, live)
    except OSError:
        if had_live:
            try:
                os.rename(old, live)
            except OSError:
                LOGGER.critical("The live index could not be put back from %s", old, exc_info=True)
        raise
    target = browse_map or Config.BROWSE_MAP
    source = staged_browse_map_path(target)
    if os.path.isfile(source):
        os.replace(source, target)
    return old if had_live else None


def remove_old_index(index_dir=None, attempts=20, delay=0.5):
    """Delete ``tantivy_db.old``; True when it is gone. Retried: on Windows the files stay
    locked until the closed index's maps are released. What stays is renamed aside and the
    next start deletes it."""
    old = old_index_path(index_dir)
    for attempt in range(attempts):
        if not os.path.exists(old):
            return True
        shutil.rmtree(old, ignore_errors=True)
        if not os.path.exists(old):
            return True
        if attempt < attempts - 1:
            time.sleep(delay)
    try:
        os.rename(old, _discard_name(index_dir or Config.INDEX_DIR))
    except OSError:
        LOGGER.warning("The old index %s could not be removed; the next start retries", old, exc_info=True)
    return False


def remove_old_index_in_background(index_dir=None):
    """:func:`remove_old_index` on a daemon thread (it may wait several seconds)."""
    index_dir = index_dir or Config.INDEX_DIR
    t = threading.Thread(target=remove_old_index, args=(index_dir,), name='remove-old-index', daemon=True)
    t.start()
    return t


def discarded_dirs(index_dir=None):
    index_dir = index_dir or Config.INDEX_DIR
    prefix = LIVE_INDEX_DIRNAME + DISCARD_INFIX
    try:
        names = os.listdir(index_dir)
    except OSError:
        return []
    return [os.path.join(index_dir, n) for n in names if n.startswith(prefix)]


def remove_discarded(index_dir=None):
    """Delete every ``tantivy_db.discard-*``; returns the paths that are gone."""
    gone = []
    for path in discarded_dirs(index_dir):
        shutil.rmtree(path, ignore_errors=True)
        if not os.path.exists(path):
            gone.append(path)
    return gone


def recover_index_dirs(index_dir=None, browse_map=None):
    """Before the index is opened at a start: finish an interrupted swap, and move every
    leftover build aside (deleted by :func:`delete_leftovers_in_background`). Returns the
    actions taken, for the log and the tests. Never raises."""
    index_dir = index_dir or Config.INDEX_DIR
    live, staged, old = live_index_path(index_dir), staged_index_path(index_dir), old_index_path(index_dir)
    target = browse_map or Config.BROWSE_MAP
    staged_map = staged_browse_map_path(target)
    actions = []
    try:
        if not os.path.isdir(live) and os.path.isdir(old):
            if os.path.isdir(staged):
                _rename_retry(staged, live)
                if os.path.isfile(staged_map):
                    os.replace(staged_map, target)
                actions.append('finished-swap')
            else:
                _rename_retry(old, live)
                actions.append('restored-old')
        if os.path.isdir(staged):
            _rename_retry(staged, _discard_name(index_dir))
            actions.append('discarded-staged')
        if os.path.isdir(old):
            _rename_retry(old, _discard_name(index_dir))
            actions.append('discarded-old')
        if os.path.isfile(staged_map):
            os.remove(staged_map)
            actions.append('discarded-staged-browse-map')
    except OSError:
        LOGGER.exception("Recovering the index folders under %s failed", index_dir)
    if actions:
        LOGGER.warning("Index folders recovered under %s: %s", index_dir, ', '.join(actions))
    return actions


def delete_leftovers_in_background(index_dir=None):
    """Delete the ``tantivy_db.discard-*`` folders on a daemon thread, when there are any."""
    index_dir = index_dir or Config.INDEX_DIR
    if not discarded_dirs(index_dir):
        return None
    t = threading.Thread(target=remove_discarded, args=(index_dir,), name='remove-discarded-index', daemon=True)
    t.start()
    return t
