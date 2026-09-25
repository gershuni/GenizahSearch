# -*- coding: utf-8 -*-
"""lists.pkl, config.pkl and lang.pkl survive interrupted writes, unreadable
files, busy files and concurrent saves, and the cloud sync keeps a snapshot of
the local lists from before each direction.

Every test drives the real call sites -- ListsManager() / add_to_recent /
create_list / add_item, genizah_core.save_app_config / save_language, the
desktop's startup notice and its sync dialog handler -- against files in
tmp_path. ``ListsManager.LISTS_FILE`` is bound at import to the real data
folder and the Config paths are read per call, so the fixture below redirects
all of them.
"""
import ast
import builtins
import errno
import glob
import hashlib
import logging
import os
import pickle
import shutil
import threading
import time
import types
from pathlib import Path

import pytest

import genizah_core
from genizah_core import Config
from shared import atomic_io
from shared import lists_manager as lm
from shared import lists_sync

USER_LIST = "My research"
NOTE = "important note"


@pytest.fixture(autouse=True)
def personal_state(tmp_path, monkeypatch):
    """Point every personal-state path at tmp_path; returns the lists.pkl path."""
    pkl = tmp_path / "lists.pkl"
    monkeypatch.setattr(Config, "INDEX_DIR", str(tmp_path))
    monkeypatch.setattr(Config, "SESSION_FILE", str(tmp_path / "session.json"))
    monkeypatch.setattr(Config, "CONFIG_FILE", str(tmp_path / "config.pkl"))
    monkeypatch.setattr(Config, "LANGUAGE_FILE", str(tmp_path / "lang.pkl"))
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(pkl))
    return pkl


@pytest.fixture
def store(personal_state):
    return personal_state


@pytest.fixture
def cfg_file(tmp_path):
    return tmp_path / "config.pkl"


@pytest.fixture
def no_sleep(monkeypatch):
    """Record the retry pauses instead of sleeping through them."""
    pauses = []
    monkeypatch.setattr(time, "sleep", pauses.append)
    return pauses


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _list_names(path):
    """The list names stored in one copy of the store, or None if unreadable."""
    try:
        with open(path, "rb") as fh:
            data = pickle.load(fh)
        return {v.get("name") for v in data["lists"].values()}
    except Exception:
        return None


def _names(mgr):
    return {v.get("name") for v in mgr.data["lists"].values()}


def _fingerprint(directory):
    return sorted((p.name, _sha(p), p.stat().st_mtime_ns) for p in directory.iterdir())


def _two_sessions(store):
    """Session 1 builds a list; session 2 only browses. Returns the list id."""
    m = lm.ListsManager(None)
    list_id = m.create_list(USER_LIST)
    assert m.add_item("990001", list_id, note=NOTE)
    m = lm.ListsManager(None)
    m.add_to_recent("990002")
    assert USER_LIST in _list_names(store)
    assert USER_LIST in _list_names(f"{store}.bak1")
    return list_id


def _truncate(path):
    """Cut the file in half: what a kill or power loss mid-write leaves."""
    half = Path(path).read_bytes()[: Path(path).stat().st_size // 2]
    Path(path).write_bytes(half)
    return half


class _Records(logging.Handler):
    """The 'genizah' loggers do not propagate, so caplog never sees them."""

    def __init__(self, level):
        super().__init__(level)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@pytest.fixture
def lists_errors():
    handler = _Records(logging.ERROR)
    lm.LOGGER.addHandler(handler)
    yield handler.messages
    lm.LOGGER.removeHandler(handler)


def _busy_reads(monkeypatch, path, times, error=None):
    """Opening `path` for reading fails `times` times: with the Windows sharing
    error, or with what ``error()`` returns."""
    real_open = builtins.open
    left = {"n": times}

    def fake_open(file, mode="r", *args, **kwargs):
        if (isinstance(file, (str, os.PathLike)) and os.path.abspath(file) == os.path.abspath(path)
                and "r" in mode and left["n"] > 0):
            left["n"] -= 1
            if error is not None:
                raise error()
            raise PermissionError(13, "The process cannot access the file because "
                                      "it is being used by another process", str(path))
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    return left


# ---------------------------------------------------------------------------
# lists.pkl: recovery from the backups
# ---------------------------------------------------------------------------

def test_truncated_lists_pkl_recovers_from_backup_and_keeps_the_backups(store):
    list_id = _two_sessions(store)
    truncated = _truncate(store)

    m = lm.ListsManager(None)

    assert USER_LIST in _names(m), "a truncated lists.pkl came back as empty lists although .bak1 was good"
    item = m.data["items"]["990001"]
    assert item["note"] == NOTE and list_id in item["lists"]
    assert m.load_status == "recovered"
    assert m.recovered_from == f"{store}.bak1"

    backups = {p: _sha(p) for p in sorted(glob.glob(f"{store}.bak*"))}
    for sys_id in ("990010", "990011", "990012"):  # ordinary browsing saves
        m.add_to_recent(sys_id)

    assert {p: _sha(p) for p in sorted(glob.glob(f"{store}.bak*"))} == backups, \
        "saves after a recovery rotated the backups"
    assert USER_LIST in _list_names(store)
    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == truncated


def test_recovery_skips_an_unreadable_bak1_and_loads_bak2(store):
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    Path(f"{store}.bak2").write_bytes(store.read_bytes())
    Path(f"{store}.bak1").write_bytes(b"torn backup")
    store.write_bytes(b"torn store")

    m = lm.ListsManager(None)

    assert USER_LIST in _names(m)
    assert m.load_status == "recovered"
    assert m.recovered_from == f"{store}.bak2"


def test_recovery_reaches_bak3_when_it_is_the_only_readable_copy(store):
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    Path(f"{store}.bak3").write_bytes(store.read_bytes())
    Path(f"{store}.bak1").write_bytes(b"torn backup 1")
    Path(f"{store}.bak2").write_bytes(b"torn backup 2")
    store.write_bytes(b"torn store")

    m = lm.ListsManager(None)

    assert USER_LIST in _names(m)
    assert m.load_status == "recovered"
    assert m.recovered_from == f"{store}.bak3"


def test_load_never_writes_even_when_it_recovers(store):
    # The web server builds a ListsManager at startup (web/main.py) and must
    # leave the file alone.
    _two_sessions(store)
    _truncate(store)
    before = _fingerprint(store.parent)

    m = lm.ListsManager(None)

    assert USER_LIST in _names(m)
    assert _fingerprint(store.parent) == before


def test_unreadable_store_without_a_readable_backup_is_kept_not_rotated(store):
    store.write_bytes(b"not a pickle")
    for i in (1, 2, 3):
        (store.parent / f"lists.pkl.bak{i}").write_bytes(b"also broken %d" % i)
    backups = {p: _sha(p) for p in sorted(glob.glob(f"{store}.bak*"))}

    m = lm.ListsManager(None)
    m.add_to_recent("990010")

    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == b"not a pickle"
    assert m.load_status == "failed"
    assert {p: _sha(p) for p in sorted(glob.glob(f"{store}.bak*"))} == backups
    assert _list_names(store) is not None


def test_a_busy_lists_file_at_startup_is_waited_for_not_replaced(store, monkeypatch, no_sleep):
    """Antivirus or the indexer holding lists.pkl for a few seconds at startup
    must not make it look unreadable (that would load an older backup and let
    the first save replace the newer file)."""
    _two_sessions(store)
    with monkeypatch.context() as mp:
        _busy_reads(mp, store, times=6)  # about 2.25 s of retries
        m = lm.ListsManager(None)

    assert USER_LIST in _names(m), "a busy lists.pkl was read as unreadable"
    assert m.load_status == "ok"
    assert 0 < sum(no_sleep) <= lm.ListsManager.LOAD_BUSY_BUDGET
    m.add_to_recent("990010")
    assert USER_LIST in _list_names(store), "the first save after a busy load dropped the user's list"


def test_a_read_that_fails_for_a_moment_at_startup_is_retried_too(store, monkeypatch, no_sleep):
    """Not only the busy-file error: a network or removable drive can fail a
    read for a moment. Taking that for a damaged file would load .bak1 -- the
    store as the last session found it -- and drop that session's changes."""
    _two_sessions(store)  # session 2 viewed 990002; .bak1 is from before it
    with monkeypatch.context() as mp:
        _busy_reads(mp, store, times=3, error=lambda: OSError(
            errno.EIO, "The request could not be performed because of an I/O device error", str(store)))
        m = lm.ListsManager(None)

    assert "990002" in m.data["recent_items"], "a passing read error at startup loaded an older backup"
    assert m.load_status == "ok"
    assert 0 < sum(no_sleep) <= lm.ListsManager.LOAD_BUSY_BUDGET


def test_a_save_that_cannot_keep_the_unreadable_file_leaves_it_alone(store, monkeypatch):
    """After a recovery, lists.pkl can be newer than the backup that was loaded
    (it stayed busy past the startup budget, say). The first save copies it
    aside, and if that copy fails the save must not replace the only copy."""
    _two_sessions(store)
    truncated = _truncate(store)
    m = lm.ListsManager(None)
    assert m.load_status == "recovered"
    real_copy2 = shutil.copy2

    def copy2(src, dst, *args, **kwargs):
        if ".unreadable-" in os.path.basename(str(dst)):
            raise PermissionError(13, "Access is denied", str(dst))
        return real_copy2(src, dst, *args, **kwargs)

    with monkeypatch.context() as mp:
        mp.setattr(shutil, "copy2", copy2)
        m.add_to_recent("990010")
        saved = m.save()

    assert store.read_bytes() == truncated, "lists.pkl was replaced although it could not be kept first"
    assert saved is False
    assert not glob.glob(f"{store}.unreadable-*")

    assert m.save() is True  # the copy can be made now: kept first, then written
    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == truncated
    assert USER_LIST in _list_names(store)


# ---------------------------------------------------------------------------
# lists.pkl: how saves write
# ---------------------------------------------------------------------------

def test_backups_rotate_once_per_session_not_on_every_save(store):
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    at_start = _sha(store)

    m = lm.ListsManager(None)
    for i in range(5):
        m.add_to_recent(f"99000{i}")

    assert _sha(f"{store}.bak1") == at_start
    assert not os.path.exists(f"{store}.bak2")


def test_the_backups_hold_the_start_of_each_of_the_last_three_sessions(store):
    """.bak1 is the store as the latest session found it, .bak2 as the one
    before found it, .bak3 the one before that: the ladder load() climbs down."""
    for k in range(1, 6):
        m = lm.ListsManager(None)
        m.create_list(f"Session {k}")  # the session's first save
        m.add_to_recent(f"99000{k}")  # and a second one

    def sessions(path):
        return sorted(n for n in _list_names(path) if n.startswith("Session "))

    assert sessions(store) == [f"Session {k}" for k in range(1, 6)]
    assert sessions(f"{store}.bak1") == [f"Session {k}" for k in range(1, 5)]
    assert sessions(f"{store}.bak2") == [f"Session {k}" for k in range(1, 4)]
    assert sessions(f"{store}.bak3") == [f"Session {k}" for k in range(1, 3)]


def test_a_save_that_fails_midway_leaves_the_previous_file(store):
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    good = _sha(store)
    # Anything that makes pickling fail part-way (a concurrent mutation:
    # "dictionary changed size during iteration"; MemoryError) used to leave
    # the truncated file behind, because it was opened 'wb' first.
    m.data["items"]["broken"] = {"sys_id": "broken", "lists": [], "fn": lambda: None}

    m.add_to_recent("990010")

    assert _sha(store) == good
    assert not glob.glob(str(store.parent / "*.tmp"))


def test_a_save_that_cannot_be_serialised_leaves_the_backups_alone(store):
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    at_start = _sha(store)
    m = lm.ListsManager(None)  # a new session: its first save rotates the backups
    before = _fingerprint(store.parent)
    m.data["items"]["broken"] = {"sys_id": "broken", "lists": [], "fn": lambda: None}

    m.add_to_recent("990010")

    assert _fingerprint(store.parent) == before, "a save that wrote nothing still rotated the backups"
    del m.data["items"]["broken"]
    m.add_to_recent("990011")
    assert _sha(f"{store}.bak1") == at_start
    assert not os.path.exists(f"{store}.bak2")


def test_a_save_still_lands_while_another_program_holds_the_file(store, monkeypatch, no_sleep):
    """A program keeping lists.pkl open without delete sharing makes Windows
    refuse the rename. The save must still reach the disk (written in place,
    as before) instead of failing silently, and the log must say so."""
    _two_sessions(store)
    m = lm.ListsManager(None)
    real_replace = os.replace

    def refuse_lists_pkl(src, dst):
        if os.path.abspath(dst) == os.path.abspath(store):
            raise PermissionError(13, "Access is denied", str(dst))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", refuse_lists_pkl)
    handler = _Records(logging.WARNING)
    atomic_log = logging.getLogger("genizah.shared.atomic_io")
    atomic_log.addHandler(handler)
    try:
        for sys_id in ("990010", "990011", "990012"):
            m.add_to_recent(sys_id)
            with open(store, "rb") as fh:
                assert pickle.load(fh)["recent_items"][0] == sys_id
    finally:
        atomic_log.removeHandler(handler)

    assert USER_LIST in _list_names(store)
    assert any("in place" in w for w in handler.messages), \
        f"the save did not go through the atomic write and its logged fallback: {handler.messages}"
    assert not glob.glob(str(store.parent / "*.tmp"))


@pytest.mark.parametrize("target", ["lists.pkl", "config.pkl"])
def test_a_briefly_refused_rename_is_retried_not_written_in_place(
        store, cfg_file, monkeypatch, no_sleep, target):
    """A reader holding the file for a moment (antivirus, the indexer, this
    app's own config read on another thread) makes Windows refuse the rename.
    The save waits and renames; the in-place write, which a kill can tear, is
    only for a file that stays blocked."""
    if target == "lists.pkl":
        m = lm.ListsManager(None)
        m.create_list(USER_LIST)
        path, save = store, lambda: m.add_to_recent("990010")
    else:
        _write_cfg(cfg_file)
        path, save = cfg_file, lambda: genizah_core.save_app_config({"line_numbers": True})
    real_replace = os.replace
    refused, renamed, in_place = [], [], []

    def replace_refused_once(src, dst):
        if os.path.abspath(dst) == os.path.abspath(path):
            if not refused:
                refused.append(dst)
                raise PermissionError(13, "Access is denied", str(dst))
            renamed.append(dst)
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", replace_refused_once)
    monkeypatch.setattr(atomic_io, "_write_in_place", lambda p, data: in_place.append(p))
    handler = _Records(logging.WARNING)
    atomic_log = logging.getLogger("genizah.shared.atomic_io")
    atomic_log.addHandler(handler)
    try:
        save()
    finally:
        atomic_log.removeHandler(handler)

    assert refused and len(renamed) == 1, "the save did not retry the refused rename"
    assert in_place == [] and not any("in place" in w for w in handler.messages)
    if target == "lists.pkl":
        with open(store, "rb") as fh:
            assert pickle.load(fh)["recent_items"][:1] == ["990010"]
    else:
        assert _read_cfg(cfg_file) == {**GOOD_CFG, "line_numbers": True}
    assert not glob.glob(str(store.parent / "*.tmp"))


def test_the_in_place_fallback_also_waits_out_a_busy_file(store, monkeypatch, no_sleep):
    """The rename stays refused, and the in-place write meets the same busy
    file once: the save still lands."""
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    real_replace, real_open = os.replace, builtins.open
    busy_writes = {"n": 1}

    def refuse_lists_pkl(src, dst):
        if os.path.abspath(dst) == os.path.abspath(store):
            raise PermissionError(13, "Access is denied", str(dst))
        return real_replace(src, dst)

    def busy_open(file, mode="r", *args, **kwargs):
        if (isinstance(file, (str, os.PathLike)) and os.path.abspath(file) == os.path.abspath(store)
                and "w" in mode and busy_writes["n"] > 0):
            busy_writes["n"] -= 1
            raise PermissionError(13, "The process cannot access the file because "
                                      "it is being used by another process", str(file))
        return real_open(file, mode, *args, **kwargs)

    with monkeypatch.context() as mp:
        mp.setattr(os, "replace", refuse_lists_pkl)
        mp.setattr(builtins, "open", busy_open)
        m.add_to_recent("990010")

    assert busy_writes["n"] == 0
    with open(store, "rb") as fh:
        assert pickle.load(fh)["recent_items"][:1] == ["990010"], "the save was lost to one busy write"
    assert USER_LIST in _list_names(store)


def test_the_new_bytes_reach_the_disk_before_they_replace_the_file(store, monkeypatch):
    """Without the flush to disk before the rename, a power cut just after it
    can leave a zero-length lists.pkl on NTFS."""
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    real_fsync, real_replace = os.fsync, os.replace
    events = []

    def fsync(fd):
        events.append(("fsync", os.fstat(fd)))
        return real_fsync(fd)

    def replace(src, dst):
        events.append(("replace", os.stat(src), os.path.abspath(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)
    m.add_to_recent("990010")

    renames = [i for i, e in enumerate(events) if e[0] == "replace" and e[2] == os.path.abspath(store)]
    assert len(renames) == 1, "the save did not rename a finished temporary file over lists.pkl"
    moved = events[renames[0]][1]
    assert any(e[0] == "fsync" and os.path.samestat(e[1], moved) for e in events[:renames[0]]), \
        "the temporary file was renamed over lists.pkl before its bytes were flushed to disk"


def test_concurrent_saves_from_the_sync_thread_do_not_fail(store, lists_errors):
    m = lm.ListsManager(None)
    list_id = m.create_list(USER_LIST)
    for i in range(1000):
        m.data["items"][f"99{i:07d}"] = {
            "sys_id": f"99{i:07d}", "lists": [list_id], "tags": [], "note": "n" * 60,
            "source": "", "added": 0.0, "modified": 0.0,
            "shelfmark_override": None, "fl_id": None, "img": None}
    m.save()

    def sync_thread():  # sync_to_cloud saves before and after its requests
        for _ in range(100):
            m.save()

    worker = threading.Thread(target=sync_thread)
    worker.start()
    for i in range(100):  # the UI thread: a manuscript view each
        m.add_to_recent(f"98{i:07d}")
    worker.join(60)

    assert not worker.is_alive()
    assert lists_errors == []
    with open(store, "rb") as fh:
        assert pickle.load(fh)["recent_items"] == m.data["recent_items"]


def test_a_stalled_save_on_the_sync_thread_cannot_land_over_a_newer_one(store, monkeypatch):
    """The save lock: the auto-sync worker takes its snapshot, then stalls; the
    UI saves a newer state meanwhile. Without the lock the worker's older
    snapshot lands last and the newer edit is gone from disk."""
    m = lm.ListsManager(None)
    m.create_list(USER_LIST)
    real = pickle
    stalled, release = threading.Event(), threading.Event()

    class GatedPickle:
        def __getattr__(self, name):
            return getattr(real, name)

        @staticmethod
        def dumps(obj, *args, **kwargs):
            payload = real.dumps(obj, *args, **kwargs)
            if threading.current_thread().name == "sync-worker":
                stalled.set()
                release.wait(10)
            return payload

        @staticmethod
        def dump(obj, fh, *args, **kwargs):  # the old save() pickled into the open file
            fh.write(GatedPickle.dumps(obj, *args, **kwargs))

    monkeypatch.setitem(lm.ListsManager.save.__globals__, "pickle", GatedPickle())
    worker = threading.Thread(target=m.save, name="sync-worker")
    worker.start()
    assert stalled.wait(10)
    ui = threading.Thread(target=m.add_to_recent, args=("990077",))
    ui.start()
    ui.join(0.5)  # with the lock the UI save waits here; without it, it lands now
    release.set()
    worker.join(10)
    ui.join(10)

    with open(store, "rb") as fh:
        assert real.load(fh)["recent_items"] == m.data["recent_items"] == ["990077"]


# ---------------------------------------------------------------------------
# config.pkl and lang.pkl
# ---------------------------------------------------------------------------

GOOD_CFG = {"telemetry_enabled": True, "viewer_text_pt": 14, "last_save_folder": "D:/x"}


def _write_cfg(path, cfg=GOOD_CFG):
    with open(path, "wb") as fh:
        pickle.dump(cfg, fh)


def _read_cfg(path):
    with open(path, "rb") as fh:
        return pickle.load(fh)


def test_save_app_config_keeps_an_unreadable_file(cfg_file):
    _write_cfg(cfg_file)
    truncated = _truncate(cfg_file)

    genizah_core.save_app_config({"line_numbers": True})

    kept = glob.glob(f"{cfg_file}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == truncated
    assert _read_cfg(cfg_file) == {"line_numbers": True}


def test_a_config_file_that_is_not_a_dict_counts_as_unreadable(cfg_file):
    """Every preference save updates the dict it reads, so a config.pkl
    holding anything else would make every later save fail."""
    _write_cfg(cfg_file, ["not", "a", "dict"])
    before = cfg_file.read_bytes()

    assert genizah_core.load_app_config() == {}
    genizah_core.save_app_config({"line_numbers": True})

    kept = glob.glob(f"{cfg_file}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == before
    assert _read_cfg(cfg_file) == {"line_numbers": True}


def test_save_app_config_does_not_overwrite_a_file_it_cannot_read(cfg_file, monkeypatch, no_sleep):
    _write_cfg(cfg_file)
    good = _sha(cfg_file)
    with monkeypatch.context() as mp:
        _busy_reads(mp, cfg_file, times=100)
        genizah_core.save_app_config({"line_numbers": True})

    assert _sha(cfg_file) == good


def test_save_app_config_rides_out_one_busy_read(cfg_file, monkeypatch, no_sleep):
    _write_cfg(cfg_file)
    with monkeypatch.context() as mp:
        _busy_reads(mp, cfg_file, times=1)
        genizah_core.save_app_config({"line_numbers": True})

    assert _read_cfg(cfg_file) == {**GOOD_CFG, "line_numbers": True}


def test_a_config_save_that_fails_midway_leaves_the_previous_file(cfg_file):
    _write_cfg(cfg_file)
    good = _sha(cfg_file)

    genizah_core.save_app_config({"callback": lambda: None})  # cannot be pickled

    assert _sha(cfg_file) == good
    assert not glob.glob(str(cfg_file.parent / "*.tmp"))


def test_concurrent_config_saves_keep_every_key(cfg_file):
    """The UI thread and a worker thread saving different preferences at the
    same time: each save is a read-modify-write, so without the lock one reads
    the file while the other is rewriting it, or both read the same old state,
    and keys are lost."""
    _write_cfg(cfg_file)

    def saver(prefix):
        for i in range(60):
            genizah_core.save_app_config({f"{prefix}{i}": i})

    threads = [threading.Thread(target=saver, args=(p,)) for p in ("ui_", "worker_")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)

    cfg = _read_cfg(cfg_file)
    missing = [k for p in ("ui_", "worker_") for k in (f"{p}{i}" for i in range(60)) if k not in cfg]
    assert missing == []
    assert all(cfg[k] == v for k, v in GOOD_CFG.items())


class _DiskFullWrites:
    """A write handle that stores half of what it is given, then fails:
    what a full disk (or a kill) in the middle of a save leaves behind."""

    def __init__(self, fh):
        self._fh = fh

    def write(self, data):
        data = bytes(data)
        self._fh.write(data[: max(1, len(data) // 2)])
        self._fh.flush()
        raise OSError(errno.ENOSPC, "No space left on device")

    def __getattr__(self, name):
        return getattr(self._fh, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._fh.close()
        return False


def test_a_language_save_that_dies_midway_keeps_the_previous_choice(tmp_path, monkeypatch):
    genizah_core.save_language("he")
    assert genizah_core.load_language() == "he"
    real_open, real_fdopen = builtins.open, os.fdopen

    def failing_open(file, mode="r", *args, **kwargs):
        fh = real_open(file, mode, *args, **kwargs)
        if "w" in mode and str(tmp_path) in os.path.abspath(str(file)):
            return _DiskFullWrites(fh)
        return fh

    def failing_fdopen(fd, mode="r", *args, **kwargs):
        fh = real_fdopen(fd, mode, *args, **kwargs)
        return _DiskFullWrites(fh) if "w" in mode else fh

    with monkeypatch.context() as mp:
        mp.setattr(builtins, "open", failing_open)
        mp.setattr(os, "fdopen", failing_fdopen)
        genizah_core.save_language("en")

    assert genizah_core.load_language() == "he", "a torn lang.pkl reset the interface to English"
    assert not glob.glob(str(tmp_path / "*.tmp"))


# ---------------------------------------------------------------------------
# The desktop tells the user
# ---------------------------------------------------------------------------

def _is_hebrew(ch):
    return "\u0590" <= ch <= "\u05ff"


@pytest.fixture
def genizah_app_module():
    import genizah_app
    return genizah_app


class _DistinctStamps:
    """lists_manager's clock, with a new timestamp for every copy it names, so
    a second unreadable-<time> copy cannot hide under the first one's name."""

    def __init__(self):
        self.n = 0

    def strftime(self, fmt, *args):
        self.n += 1
        return f"20260101-0000{self.n:02d}"

    def __getattr__(self, name):
        return getattr(time, name)


@pytest.mark.parametrize("lang", ["en", "he"])
def test_startup_reports_a_recovered_lists_file(store, monkeypatch, genizah_app_module, lang):
    _two_sessions(store)
    three_days_ago = time.time() - 3 * 24 * 3600
    os.utime(f"{store}.bak1", (three_days_ago, three_days_ago))
    truncated = _truncate(store)
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    monkeypatch.setattr(lm, "time", _DistinctStamps())
    shown = []
    monkeypatch.setattr(genizah_app_module, "QMessageBox", types.SimpleNamespace(
        warning=lambda parent, title, text: shown.append((title, text))))
    gui = genizah_app_module.GenizahGUI.__new__(genizah_app_module.GenizahGUI)
    gui.lists_mgr = lm.ListsManager(None)

    gui._report_lists_load_problem()

    assert len(shown) == 1
    title, text = shown[0]
    assert title == genizah_core.tr("Lists restored from a backup")
    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == truncated, \
        "the notice names a kept copy that does not exist yet"
    assert os.path.basename(kept[0]) in text and str(store.parent) in text
    # The date tells the user which changes may be missing: the backup's, not lists.pkl's.
    stamp = "%Y-%m-%d %H:%M"
    assert time.strftime(stamp, time.localtime(three_days_ago)) in text
    assert time.strftime(stamp, time.localtime(os.path.getmtime(store))) not in text
    if lang == "he":
        assert _is_hebrew(text[0]) and _is_hebrew(title[0])

    for sys_id in ("990010", "990011"):  # the session goes on
        gui.lists_mgr.add_to_recent(sys_id)
    assert glob.glob(f"{store}.unreadable-*") == kept, "the saves after the notice kept another copy"
    assert USER_LIST in _list_names(store)


def test_the_notice_names_the_copy_an_earlier_save_already_kept(store, monkeypatch, genizah_app_module):
    _two_sessions(store)
    truncated = _truncate(store)
    shown = []
    monkeypatch.setattr(genizah_app_module, "QMessageBox", types.SimpleNamespace(
        warning=lambda parent, title, text: shown.append((title, text))))
    gui = genizah_app_module.GenizahGUI.__new__(genizah_app_module.GenizahGUI)
    gui.lists_mgr = lm.ListsManager(None)
    gui.lists_mgr.add_to_recent("990010")  # a save that ran before the notice

    gui._report_lists_load_problem()

    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and Path(kept[0]).read_bytes() == truncated
    assert len(shown) == 1 and os.path.basename(kept[0]) in shown[0][1], \
        "the notice does not name the copy that holds the unreadable file"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_startup_reports_lists_that_could_not_be_loaded_and_says_nothing_otherwise(
        store, monkeypatch, genizah_app_module, lang):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    shown = []
    monkeypatch.setattr(genizah_app_module, "QMessageBox", types.SimpleNamespace(
        warning=lambda parent, title, text: shown.append((title, text))))
    gui = genizah_app_module.GenizahGUI.__new__(genizah_app_module.GenizahGUI)

    gui.lists_mgr = lm.ListsManager(None)  # a fresh install
    gui._report_lists_load_problem()
    gui.lists_mgr.create_list(USER_LIST)
    gui.lists_mgr = lm.ListsManager(None)  # an ordinary start
    gui._report_lists_load_problem()
    assert shown == []

    store.write_bytes(b"not a pickle")
    for path in glob.glob(f"{store}.bak*"):
        os.remove(path)
    gui.lists_mgr = lm.ListsManager(None)
    gui._report_lists_load_problem()

    assert [t for t, _ in shown] == [genizah_core.tr("Lists could not be loaded")]
    kept = glob.glob(f"{store}.unreadable-*")
    assert len(kept) == 1 and os.path.basename(kept[0]) in shown[0][1]
    if lang == "he":
        title, text = shown[0]
        assert _is_hebrew(title[0]) and _is_hebrew(text[0])


@pytest.mark.parametrize("lang", ["en", "he"])
@pytest.mark.parametrize("backup", ["a readable backup", "no backup"])
def test_startup_says_the_lists_cannot_be_saved_while_the_file_stays_busy(
        store, monkeypatch, no_sleep, genizah_app_module, lang, backup):
    """lists.pkl stays busy past the startup budget: load() falls back, and the
    copy of lists.pkl fails for the same reason. The notice said the file "is
    kept as lists.pkl", and every save after it failed with only a log line."""
    _two_sessions(store)
    if backup == "no backup":
        for path in glob.glob(f"{store}.bak*"):
            os.remove(path)
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    shown = []
    monkeypatch.setattr(genizah_app_module, "QMessageBox", types.SimpleNamespace(
        warning=lambda parent, title, text: shown.append((title, text))))
    gui = genizah_app_module.GenizahGUI.__new__(genizah_app_module.GenizahGUI)
    before = store.read_bytes()

    with monkeypatch.context() as mp:
        _busy_reads(mp, store, times=10 ** 6)       # busy for the whole session
        gui.lists_mgr = lm.ListsManager(None)
        gui._report_lists_load_problem()
        gui.lists_mgr.add_to_recent("990010")
        assert gui.lists_mgr.save() is False        # what the notice warns of

    assert gui.lists_mgr.load_status == (
        "recovered" if backup == "a readable backup" else "failed")
    assert store.read_bytes() == before and not glob.glob(f"{store}.unreadable-*")
    assert len(shown) == 1
    title, text = shown[0]
    tr = genizah_core.tr
    assert title == tr("Lists cannot be saved")
    tail = ("Another program appears to be using the lists file, and until it can be "
            "read your lists cannot be saved: changes you make now may be lost. Close any "
            "other program that may be using the file and restart the application. The "
            "file is in:\n{}")
    if backup == "a readable backup":
        when = time.strftime("%Y-%m-%d %H:%M",
                             time.localtime(os.path.getmtime(f"{store}.bak1")))
        assert text == tr("Your saved lists could not be read, so they were restored "
                          "from a backup saved on {}. " + tail).format(when, store.parent)
    else:
        assert text == tr("Your saved lists could not be read and no readable backup "
                          "was found, so your lists are empty. " + tail).format(store.parent)
    if lang == "he":
        assert _is_hebrew(title[0]) and _is_hebrew(text[0])

    assert gui.lists_mgr.save() is True             # once it can be read: kept, then saved
    assert len(glob.glob(f"{store}.unreadable-*")) == 1


def test_on_startup_finished_reports_right_after_building_the_lists():
    source = (Path(__file__).resolve().parents[1] / "genizah_app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    gui_cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "GenizahGUI")
    method = next(n for n in gui_cls.body
                  if isinstance(n, ast.FunctionDef) and n.name == "on_startup_finished")
    wanted = ("ListsManager(self.meta_mgr)", "self.lists_refresh_all()", "self._report_lists_load_problem()")
    calls = sorted((n for n in ast.walk(method) if isinstance(n, ast.Call) and ast.unparse(n) in wanted),
                   key=lambda n: (n.lineno, n.col_offset))
    assert [ast.unparse(n) for n in calls] == list(wanted)


# ---------------------------------------------------------------------------
# Cloud sync: a snapshot before each direction, and Merge stops on a failed download
# ---------------------------------------------------------------------------

TODAY = "today's reading of the colophon"
OLD = "an older note, saved in the cloud"


class _FakeCloud:
    """Enough of supabase-py's table API for lists_sync, over in-memory rows."""

    def __init__(self):
        self.tables = {
            "projects": [],
            "user_lists": [{"id": "cl-1", "user_id": "u1", "name": USER_LIST, "color": "#4CAF50"}],
            "list_items": [{"id": "ci-1", "list_id": "cl-1", "sys_id": "990001", "fl_id": None,
                            "note": OLD, "tags": []}],
        }
        self.writes = []
        self.fail_next_read = False
        self._n = 0

    def table(self, name):
        return _FakeQuery(self, name)


class _FakeQuery:
    def __init__(self, cloud, name):
        self.cloud, self.name, self.op, self.payload, self.filters = cloud, name, "select", None, []

    def select(self, *args):
        self.op = "select"
        return self

    def insert(self, payload):
        self.op, self.payload = "insert", payload
        return self

    def update(self, payload):
        self.op, self.payload = "update", payload
        return self

    def eq(self, column, value):
        self.filters.append((column, value))
        return self

    def execute(self):
        rows = self.cloud.tables[self.name]
        matched = [r for r in rows if all(r.get(c) == v for c, v in self.filters)]
        if self.op == "select":
            if self.cloud.fail_next_read:
                self.cloud.fail_next_read = False
                raise ConnectionError("the network went away")
            return types.SimpleNamespace(data=[dict(r) for r in matched])
        self.cloud.writes.append((self.name, self.op, self.payload))
        if self.op == "insert":
            out = []
            for p in self.payload if isinstance(self.payload, list) else [self.payload]:
                self.cloud._n += 1
                row = dict(p, id=f"{self.name}-{self.cloud._n}")
                rows.append(row)
                out.append(dict(row))
            return types.SimpleNamespace(data=out)
        for r in matched:
            r.update(self.payload)
        return types.SimpleNamespace(data=[dict(r) for r in matched])


@pytest.fixture
def synced(store, monkeypatch):
    """A local store holding today's note and a fake cloud holding an older one."""
    m = lm.ListsManager(None)
    list_id = m.create_list(USER_LIST)
    assert m.add_item("990001", list_id, note=TODAY)
    cloud = _FakeCloud()
    monkeypatch.setattr(lists_sync, "SUPABASE_AVAILABLE", True)
    monkeypatch.setattr(lists_sync, "SUPABASE_ANON_KEY", "test-key")
    sync = lists_sync.ListsCloudSync(m)
    sync.set_user("u1")
    sync.set_client(cloud)
    monkeypatch.setattr(lists_sync, "_sync_instance", sync)
    return types.SimpleNamespace(mgr=m, cloud=cloud, store=store)


def _note_in(path):
    with open(path, "rb") as fh:
        return pickle.load(fh)["items"]["990001"]["note"]


def _run_sync_dialog_action(genizah_app, monkeypatch, mgr, action):
    """Drive GenizahGUI._do_sync_action -- the sync dialog's buttons -- with Qt faked out."""
    shown = []

    class _Progress:
        def __init__(self, *args):
            pass

        def __getattr__(self, name):
            return lambda *args: None

    monkeypatch.setattr(genizah_app, "QProgressDialog", _Progress)
    monkeypatch.setattr(genizah_app, "QApplication", types.SimpleNamespace(processEvents=lambda: None))
    monkeypatch.setattr(genizah_app, "QMessageBox", types.SimpleNamespace(
        information=lambda parent, title, text: shown.append(("information", text)),
        warning=lambda parent, title, text: shown.append(("warning", text)),
        critical=lambda parent, title, text: shown.append(("critical", text))))
    host = types.SimpleNamespace(lists_mgr=mgr,
                                 _sync_error_text=genizah_app.GenizahGUI._sync_error_text)
    genizah_app.GenizahGUI._do_sync_action(host, types.SimpleNamespace(accept=lambda: None), action)
    return shown


def test_a_merge_keeps_todays_note_in_the_pre_download_snapshot(synced, monkeypatch, genizah_app_module):
    shown = _run_sync_dialog_action(genizah_app_module, monkeypatch, synced.mgr, "merge")

    assert [kind for kind, _ in shown] == ["information"], shown
    assert _note_in(synced.store) == OLD  # the download took the cloud's note
    assert _note_in(f"{synced.store}.pre-download") == TODAY, \
        "the upload half of the Merge replaced the snapshot taken before the download"
    assert _note_in(f"{synced.store}.pre-upload") == OLD


def test_an_upload_alone_snapshots_before_it_and_leaves_the_download_snapshot(synced):
    """Auto-sync (after every list change) and the logout sync only upload."""
    assert synced.mgr.sync_to_cloud()["success"]
    assert _note_in(f"{synced.store}.pre-upload") == TODAY
    assert not os.path.exists(f"{synced.store}.pre-download")

    Path(f"{synced.store}.pre-download").write_bytes(b"an earlier download's snapshot")
    synced.mgr.add_to_recent("990010")
    assert synced.mgr.sync_to_cloud()["success"]
    assert Path(f"{synced.store}.pre-download").read_bytes() == b"an earlier download's snapshot"


def _block_the_download_snapshot(store):
    # A folder where the snapshot file should go: neither the rename nor the
    # in-place write can put a file there.
    os.mkdir(f"{store}.pre-download")


def test_a_download_without_its_snapshot_changes_nothing(synced, no_sleep):
    _block_the_download_snapshot(synced.store)
    before_disk = _sha(synced.store)
    before_memory = pickle.dumps(synced.mgr.data)

    result = synced.mgr.sync_from_cloud()

    assert result["success"] is False
    assert result["error"] == lists_sync.DOWNLOAD_BACKUP_FAILED
    assert result["error"] in genizah_core.TRANSLATIONS  # the dialog shows it translated
    assert pickle.dumps(synced.mgr.data) == before_memory
    assert _sha(synced.store) == before_disk
    assert synced.cloud.writes == []


@pytest.mark.parametrize("cause", ["network", "snapshot"])
def test_a_merge_whose_download_fails_never_uploads(synced, monkeypatch, genizah_app_module, no_sleep, cause):
    if cause == "network":
        synced.cloud.fail_next_read = True
    else:
        _block_the_download_snapshot(synced.store)
    cloud_before = {name: [dict(r) for r in rows] for name, rows in synced.cloud.tables.items()}

    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")

    shown = _run_sync_dialog_action(genizah_app_module, monkeypatch, synced.mgr, "merge")

    assert synced.cloud.writes == [], "the Merge uploaded after its download failed"
    assert synced.cloud.tables == cloud_before
    assert [kind for kind, _ in shown] == ["warning"]
    assert _note_in(synced.store) == TODAY
    if cause == "snapshot":
        assert shown[0][1] == genizah_core.TRANSLATIONS[lists_sync.DOWNLOAD_BACKUP_FAILED]


def test_a_merge_whose_upload_fails_says_so_in_the_interface_language(
        synced, monkeypatch, genizah_app_module):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    monkeypatch.setattr(synced.mgr, "sync_to_cloud",
                        lambda: {"success": False, "error": "Sync already in progress"})

    shown = _run_sync_dialog_action(genizah_app_module, monkeypatch, synced.mgr, "merge")

    assert [kind for kind, _ in shown] == ["warning"]
    text = shown[0][1]
    assert _is_hebrew(text[0]), f"the upload error is not in the interface language: {text!r}"
    assert text == genizah_core.tr("The cloud lists were downloaded, but the upload failed: {}").format(
        genizah_core.TRANSLATIONS["Sync already in progress"]), (
        f"the reason inside the message is not in the interface language: {text!r}")
    assert _note_in(synced.store) == OLD  # the download half did land


@pytest.mark.parametrize("lang", ["en", "he"])
def test_a_partial_upload_is_reported_in_the_interface_language_with_its_counts(
        synced, monkeypatch, genizah_app_module, lang):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    monkeypatch.setattr(synced.mgr, "sync_to_cloud", lambda: {
        "success": False, "items_pushed": 3, "items_failed": 2,
        "error": lists_sync.UPLOAD_PARTLY_FAILED.format(3, 2)})

    shown = _run_sync_dialog_action(genizah_app_module, monkeypatch, synced.mgr, "upload")

    assert shown == [("warning", genizah_core.tr(lists_sync.UPLOAD_PARTLY_FAILED).format(3, 2))]
    if lang == "he":
        assert _is_hebrew(shown[0][1][0]), shown


def _sync_error_literals():
    """Every fixed message the sync layer puts in a result's 'error'."""
    root = Path(__file__).resolve().parents[1] / "shared"
    found = set()
    for name in ("lists_sync.py", "lists_manager.py"):
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                values = [v for k, v in zip(node.keys, node.values)
                          if isinstance(k, ast.Constant) and k.value == "error"]
            elif isinstance(node, ast.Assign):
                values = [node.value for t in node.targets
                          if isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant)
                          and t.slice.value == "error"]
            else:
                continue
            found |= {v.value for v in values
                      if isinstance(v, ast.Constant) and isinstance(v.value, str)}
    return found


def test_every_sync_error_the_dialog_can_show_has_a_hebrew_entry():
    """The dialog shows tr(error); only DOWNLOAD_BACKUP_FAILED had an entry, so
    the rest reached the Hebrew interface in English."""
    literals = _sync_error_literals()
    assert {"Sync already in progress", "Sync not available", "No Supabase client",
            "Cloud sync not available"} <= literals, literals   # the scan still sees them
    shown = literals | {"Unknown error",                        # the dialog's own fallback
                        lists_sync.DOWNLOAD_BACKUP_FAILED, lists_sync.UPLOAD_PARTLY_FAILED}
    missing = sorted(m for m in shown if not genizah_core.TRANSLATIONS.get(m))
    assert missing == [], f"shown in English in the Hebrew interface: {missing}"
