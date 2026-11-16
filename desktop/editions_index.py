# -*- coding: utf-8 -*-
"""The scholarly transcriptions' index on the desktop: built here, kept current, opened.

Editions search plan (private repository), section 3.7 and decision D3: the desktop builds
the editions index itself, from the FGP and PGP sidecars it ships with, the NLI crossref and
the main index's browse map (shared/editions_indexer.py), instead of downloading it. A
generation is about 270 MB and takes a minute or two to build.

**One pass at a time, in a background thread** (:class:`EditionsIndexWorker`; never from
``__init__`` or the UI thread, which only starts passes and installs what they opened):

* *check*: the live generation (``<INDEX_DIR>/Editions``) is compared with the installed
  inputs (``changed_inputs``: the sidecars' files, the browse map, the NLI crossref, the
  cleaner's and builder's versions; a file whose size and time are unchanged is not read
  again, :class:`FingerprintCache`, ``Editions.inputs.json``). Current: it is opened. Missing or stale: a finished
  ``Editions.new`` built from today's inputs is handed on as it is; otherwise ``Editions.new``
  is built (replacing any unfinished one), checked (``check_generation``) and handed on.
* *promote*: the built generation goes in place of the live one under the readers' lease
  (``promote_with_lease``), the one it replaced is deleted, and the live one is opened.

**The lifecycle** (:class:`EditionsIndex`, on the UI thread) promotes the way the main index
is swapped in (genizah_app.py, ``_install_staged_index``): it waits until nothing reads the
index (``readers_busy``), closes the engine's editions child (``close_editions``), and lets a
*promote* pass rename and reopen; the opened leaf is installed on the UI thread
(``SearchEngine.install_editions``). Until a generation is installed, a search asked to read
the editions runs without them and says so (``consume_editions_unavailable``).

A rebuild of the main index replaces the browse map the editions are placed by, and a sidecar
update replaces the sidecars (which a running build holds open): the main window suspends
this lifecycle for each (a running pass is stopped; a built generation is dropped) and
resumes it afterwards: the generation built from the old inputs is closed once no search
reads it, and the editions are checked again and rebuilt.
"""
from __future__ import annotations

import gc
import json
import logging
import os
import shutil
import time
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from PyQt6.QtCore import QObject, QThread, QTimer

from shared.config import Config
from shared.editions_indexer import (BuildCancelled, build_generation, changed_inputs, default_generation_dir,
                                     file_fingerprint, input_stamp, read_generation)
from shared.editions_search import check_generation, open_editions_engine, promote_with_lease

LOGGER = logging.getLogger("genizah." + __name__)

STAGED_SUFFIX = ".new"
PREVIOUS_SUFFIX = ".prev"
#: Free space a build asks for (a generation is about 270 MB, 2026-11-16).
BUILD_HEADROOM = 1024 ** 3
#: The rename of a just-closed index can fail while Windows still releases its files.
PROMOTE_ATTEMPTS = 10
PROMOTE_RETRY_S = 0.3

# What a pass ends with (EditionsIndexWorker.result['kind']).
CURRENT = "current"          # the live generation is current and already the one installed
OPENED = "opened"            # a leaf on the live generation ('leaf'), to install
BUILT = "built"              # a checked generation at 'path', to promote
UNAVAILABLE = "unavailable"  # an input is missing, or no room: nothing built ('reason')
FAILED = "failed"            # the build, the check or the promotion failed ('reason')
CANCELLED = "cancelled"      # stopped (exit, a main-index rebuild); what it wrote is removed
INPUTS_CHANGED = "inputs_changed"  # an input was replaced during the build: removed, checked again

#: What a resumed lifecycle waits for before it checks: the generation built from the
#: replaced inputs, closed (EditionsIndex._retire_when_idle).
RETIRING = "closing the previous generation"


class EditionsInputs(NamedTuple):
    """The files a generation is built from."""
    fgp_db: Optional[str]
    pgp_db: Optional[str]
    browse_map: Optional[str]
    nli_db: Optional[str]

    def missing(self) -> List[str]:
        """The inputs that are not there (an empty file counts as missing: a placeholder)."""
        out = []
        for name, path in self._asdict().items():
            try:
                if not path or os.path.getsize(path) == 0:
                    out.append(name)
            except OSError:
                out.append(name)
        return out


def installed_inputs() -> EditionsInputs:
    """The sidecars the app reads (each service's own lookup: the user's data folder first,
    then the app's) and the main index's browse map."""
    from shared.editions_search import EditionResolver
    from shared.nli_crossref_service import NliCrossrefService
    resolver = EditionResolver()
    nli = NliCrossrefService()
    nli_db = getattr(nli, "_db_path", None)
    nli.close()
    return EditionsInputs(getattr(resolver, "_fgp_db", None), getattr(resolver, "_pgp_db", None),
                          Config.BROWSE_MAP, nli_db)


def staged_dir(live_dir: str) -> str:
    return live_dir + STAGED_SUFFIX


class FingerprintCache:
    """``file_fingerprint`` that does not read a file again while its size and modification
    time are the ones it was read at: the check at each start would otherwise read about
    0.9 GB of sidecars and browse map (2026-11-16). Kept as JSON at *path* (beside the
    generation); a missing or unreadable record only means reading the files."""

    def __init__(self, path: str):
        self.path = path
        self.files_read = 0
        self._dirty = False
        try:
            with open(path, encoding="utf-8") as f:
                entries = json.load(f)
            self._entries = entries if isinstance(entries, dict) else {}
        except (OSError, ValueError):
            self._entries = {}

    def __call__(self, path: Optional[str]) -> Optional[Dict[str, Any]]:
        if not path or not os.path.isfile(path):
            return None
        st = os.stat(path)
        key = os.path.normcase(os.path.abspath(path))
        entry = self._entries.get(key)
        if (isinstance(entry, dict) and entry.get("size") == st.st_size
                and entry.get("mtime_ns") == st.st_mtime_ns and isinstance(entry.get("fp"), dict)):
            return dict(entry["fp"])
        # The size and time from before the read: a file changed while it was read is read
        # again next time.
        fp = file_fingerprint(path)
        self.files_read += 1
        self._entries[key] = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "fp": fp}
        self._dirty = True
        return fp

    def save(self) -> None:
        if not self._dirty:
            return
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                json.dump(self._entries, f, indent=1)
            os.replace(tmp, self.path)
            self._dirty = False
        except OSError:
            LOGGER.warning("Could not keep the inputs' fingerprints at %s", self.path, exc_info=True)


def inputs_cache_path(live_dir: str) -> str:
    return live_dir + ".inputs.json"


def generation_changes(gen_dir: str, inputs: EditionsInputs,
                       fingerprint: Optional[Callable[[Optional[str]], Any]] = None) -> Optional[List[str]]:
    """What changed since the generation at *gen_dir* was built (``changed_inputs``; empty:
    it is current), or None when it is not a ready generation. *fingerprint*: a
    FingerprintCache, or None to read every file."""
    ready, _reason, meta, _payload = check_generation(gen_dir)
    if not ready:
        return None
    installed = input_stamp(inputs.fgp_db, inputs.pgp_db, inputs.browse_map, inputs.nli_db, fingerprint=fingerprint)
    return changed_inputs((meta or {}).get("inputs"), installed)


def remove_dir(path: str) -> bool:
    """Delete *path* if it is there; False when some of it could not be deleted (Windows
    keeps a just-closed index's files a moment): what is left goes at the next build."""
    if not os.path.lexists(path):
        return True
    shutil.rmtree(path, ignore_errors=True)
    if os.path.exists(path):
        LOGGER.warning("Could not delete all of %s; it is deleted at the next build", path)
        return False
    return True


def _free_bytes(path: str) -> int:
    from shared.index_swap import free_bytes
    return free_bytes(path)


class EditionsIndexWorker(QThread):
    """One pass of the lifecycle (module docstring): *task* ``'check'`` or ``'promote'``.

    Its outcome is ``self.result`` (``{'kind', 'leaf', 'path', 'reason'}``), read on the UI
    thread once the thread has finished. No Qt parent: it is kept until it finishes
    (``_keep_until_finished``), and the exit asks it to stop (``isInterruptionRequested``)."""

    def __init__(self, task: str, *, live_dir: str, meta_mgr, var_mgr, inputs: Optional[EditionsInputs] = None,
                 before_stamp: Optional[Callable[[], Any]] = None, installed_build: Optional[str] = None):
        super().__init__()
        if task not in ("check", "promote"):
            raise ValueError(f"unknown task {task!r}")
        self.task = task
        self.live_dir = live_dir
        self.meta_mgr = meta_mgr
        self.var_mgr = var_mgr
        self.inputs = inputs
        self.before_stamp = before_stamp
        self.installed_build = installed_build   # the build the engine has open, if any
        self.result: Dict[str, Any] = {"kind": FAILED, "leaf": None, "path": None, "reason": "not run"}
        self._stop = False

    def request_stop(self) -> None:
        """Stop a check's build at its next document (BuildCancelled). A promotion is a few
        renames and is never stopped halfway."""
        self._stop = True

    def should_stop(self) -> bool:
        return self._stop or self.isInterruptionRequested()

    def run(self):
        started = time.perf_counter()
        try:
            self.result = self._check() if self.task == "check" else self._promote()
        except Exception as e:                                   # noqa: BLE001
            LOGGER.exception("The scholarly transcriptions' index: the %s pass failed", self.task)
            self.result = {"kind": FAILED, "leaf": None, "path": None, "reason": repr(e)}
        self.result["seconds"] = time.perf_counter() - started

    # --- check ---------------------------------------------------------------------------

    def _check(self) -> Dict[str, Any]:
        inputs = self.inputs or installed_inputs()
        missing = inputs.missing()
        if missing:
            return {"kind": UNAVAILABLE, "leaf": None, "path": None,
                    "reason": "missing input(s): " + ", ".join(missing)}
        if self.before_stamp is not None:
            # The main engine's browse map, loaded: its one-time repair may rewrite the file,
            # and the generation must be stamped with the file as it stays.
            self.before_stamp()
        live, new = self.live_dir, staged_dir(self.live_dir)
        cache = FingerprintCache(inputs_cache_path(live))
        try:
            return self._check_with(inputs, live, new, cache)
        finally:
            cache.save()

    def _check_with(self, inputs: EditionsInputs, live: str, new: str, cache: "FingerprintCache") -> Dict[str, Any]:
        changes = generation_changes(live, inputs, cache)
        if changes == []:
            remove_dir(new)                   # an unfinished or superseded build: junk
            meta, _payload = read_generation(live)
            if self.installed_build and (meta or {}).get("build_id") == self.installed_build:
                return {"kind": CURRENT, "leaf": None, "path": live, "reason": ""}
            return self._opened(live)
        if self.should_stop():
            return {"kind": CANCELLED, "leaf": None, "path": None, "reason": "stopped"}
        if generation_changes(new, inputs, cache) == []:
            # Built from today's inputs and never put in place (the app closed first).
            return {"kind": BUILT, "leaf": None, "path": new, "reason": "built earlier"}
        why = "no generation" if changes is None else "changed: " + ", ".join(changes)
        parent = os.path.dirname(os.path.abspath(live)) or "."
        os.makedirs(parent, exist_ok=True)
        # A superseded or unfinished Editions.new, and a .prev a promotion could not delete,
        # are never read again: their space counts as free (Codex review of PR #25).
        remove_dir(new)
        remove_dir(live + PREVIOUS_SUFFIX)
        if _free_bytes(parent) < BUILD_HEADROOM:
            return {"kind": UNAVAILABLE, "leaf": None, "path": None,
                    "reason": f"less than {BUILD_HEADROOM // 1024 ** 2} MB free in {parent}"}
        LOGGER.info("Building the scholarly transcriptions' index at %s (%s)", new, why)
        started = time.perf_counter()
        try:
            build_generation(new, fgp_db=inputs.fgp_db, pgp_db=inputs.pgp_db,
                             browse_map_path=inputs.browse_map, nli_db=inputs.nli_db, force=True,
                             progress=lambda msg: LOGGER.info("editions build: %s", msg),
                             should_stop=self.should_stop)
        except BuildCancelled:
            gc.collect()                      # the index writer lets go of its files
            remove_dir(new)
            return {"kind": CANCELLED, "leaf": None, "path": None, "reason": "stopped"}
        except Exception:
            gc.collect()
            remove_dir(new)
            raise
        ready, reason, meta, _payload = check_generation(new)
        if not ready:
            remove_dir(new)
            return {"kind": FAILED, "leaf": None, "path": None, "reason": f"the new build is not ready: {reason}"}
        # An input replaced while the build read it (a sidecar update): the build stamped the
        # inputs as they were when it started, so it is stale already. Not put in place; the
        # lifecycle checks again and builds from the new files (Codex review of PR #25).
        changed = generation_changes(new, inputs, cache)
        if changed:
            remove_dir(new)
            return {"kind": INPUTS_CHANGED, "leaf": None, "path": None, "again": True,
                    "reason": "changed during the build: " + ", ".join(changed)}
        LOGGER.info("Built the scholarly transcriptions' index: %s documents in %.0f s",
                    ((meta or {}).get("counts") or {}).get("documents"), time.perf_counter() - started)
        return {"kind": BUILT, "leaf": None, "path": new, "reason": why}

    # --- promote -------------------------------------------------------------------------

    def _promote(self) -> Dict[str, Any]:
        live, new = self.live_dir, staged_dir(self.live_dir)
        error = None
        for attempt in range(PROMOTE_ATTEMPTS):
            gc.collect()                      # the closed child's maps, released
            try:
                promote_with_lease(new, live)
                error = None
                break
            except OSError as e:              # Windows: a just-closed index's files
                error = e
                time.sleep(PROMOTE_RETRY_S)
        if error is not None:
            # The generation still at *live* was built from inputs since replaced (a promotion
            # follows a build, and a build follows a change): never reopened. The checked one
            # stays at Editions.new for a later try (Codex review of PR #25).
            LOGGER.error("The new scholarly transcriptions' index could not be put in place: %r", error)
            return {"kind": FAILED, "leaf": None, "path": new, "retry_promotion": True,
                    "reason": f"not put in place: {error!r}"}
        remove_dir(live + PREVIOUS_SUFFIX)    # the desktop keeps no rollback copy
        return self._opened(live)

    def _opened(self, gen_dir: str) -> Dict[str, Any]:
        leaf = open_editions_engine(self.meta_mgr, self.var_mgr, gen_dir)
        if leaf is None:
            return {"kind": FAILED, "leaf": None, "path": gen_dir, "reason": "the generation did not open"}
        meta, _payload = read_generation(gen_dir)
        return {"kind": OPENED, "leaf": leaf, "path": gen_dir, "reason": "",
                "build_id": (meta or {}).get("build_id")}


class EditionsIndex(QObject):
    """The desktop's editions index lifecycle (module docstring), on the UI thread.

    *engine*: the main SearchEngine, or a callable returning it (the main window's
    ``searcher`` at the time of each step). *readers_busy*: what reads the index now (the
    main window's ``_index_readers_busy``); a promotion waits while it names anything.
    *live_dir* (default ``<INDEX_DIR>/Editions`` at each pass) and *inputs* (default the
    installed ones, found in the worker): tests name their own.

    Nothing happens at construction: :meth:`check` starts the first pass."""

    PROMOTE_RETRY_MS = 1000
    #: A promotion whose renames all failed is tried again this much later, this many times;
    #: then the editions stay unavailable and the next start puts the kept build in place.
    PROMOTE_LATER_MS = 10_000
    PROMOTE_LATER_TRIES = 3

    def __init__(self, engine, *, readers_busy: Optional[Callable[[], list]] = None,
                 live_dir: Optional[str] = None, inputs: Optional[EditionsInputs] = None, parent=None):
        super().__init__(parent)
        self._engine = engine if callable(engine) else (lambda: engine)
        self._readers_busy = readers_busy or (lambda: [])
        self._live_dir = live_dir
        self._inputs = inputs
        self._worker: Optional[EditionsIndexWorker] = None
        self._pending: Optional[str] = None      # a built generation waiting to be promoted
        self._again = False                       # check again when the running pass ends
        self._waiting_for: set = set()          # why passes wait (suspend's reasons)
        self._closed = False
        self.status = "idle"                      # what the last step left (logs, tests)
        self.reason = ""
        self.build_id = None                      # the generation installed last
        self._promotion_tries = 0                 # failed promotion passes of this build
        self._later: Optional[str] = None         # a failed promotion's build, tried again later

    # --- what the main window calls ------------------------------------------------------

    def live_dir(self) -> str:
        return self._live_dir or default_generation_dir()

    def busy(self) -> bool:
        """A pass is running (the main index's install waits for it)."""
        return self._worker is not None

    def check(self) -> None:
        """Start a check pass, or one more after the running pass or the waiting promotion."""
        if self._closed:
            return
        if self._suspended or self._worker is not None or self._pending is not None:
            self._again = True
            return
        engine = self._engine()
        if engine is None or not hasattr(engine, "install_editions"):
            return
        self._again = False
        self._start("check", engine)

    @property
    def _suspended(self) -> bool:
        return bool(self._waiting_for)

    def suspend(self, reason: str = "main index") -> None:
        """Something is about to replace an input, *reason*: a main-index rebuild (its new
        browse map) or a sidecar update (the files themselves, which a running build holds
        open). A running check stops; a built generation waiting for its promotion is
        dropped (the next check finds whether it is still current). Nothing starts until
        :meth:`resume` has been called for every reason given."""
        self._waiting_for.add(reason)
        self._later = None                    # resume() checks, which finds the kept build
        # The main index's reopen at the end of its build must not reopen, on the UI thread,
        # the generation being made stale: the child open now keeps serving until then, and
        # resume() installs one checked in the background (Codex review of PR #25).
        engine = self._engine()
        hold = getattr(engine, "set_editions_reopen", None)
        if callable(hold):
            hold(False)
        if self._pending is not None:
            self._pending = None
            self._again = True
        if self._worker is not None:
            self._again = True
            self._worker.request_stop()

    def resume(self, reason: str = "main index") -> None:
        """What *reason* named has ended: once nothing else is waited for, the generation
        open now -- built from the inputs just replaced -- is closed (:meth:`_retire_when_idle`),
        and the editions are checked against what is there now. Also when the start was
        missed (a lifecycle made after the build began). Codex review of PR #25."""
        self._waiting_for.discard(reason)
        if self._waiting_for - {RETIRING}:
            return
        self._waiting_for.add(RETIRING)        # nothing starts until the old one is closed
        self._retire_when_idle()

    def _retire_when_idle(self) -> None:
        """Close the engine's editions child once nothing reads the index (a search reading
        it keeps it until it ends; a second at a time): until a generation built from the
        new inputs is installed, a search asked for the editions runs without them and says
        so, never on old text or the old browse map's pages. Then check."""
        if self._closed or RETIRING not in self._waiting_for:
            return                             # closed, or a second timer of one retirement
        if self._waiting_for - {RETIRING}:
            return                             # suspended again: its resume retires
        engine = self._engine()
        ready = getattr(engine, "editions_ready", None)
        if callable(ready) and ready():
            if self._worker is not None or self._readers_busy():
                QTimer.singleShot(self.PROMOTE_RETRY_MS, self._retire_when_idle)
                return
            engine.detach_editions()
            self.build_id = None
        self._waiting_for.discard(RETIRING)
        self._again = False
        self.check()

    def close(self) -> None:
        """At exit: stop the running pass; install nothing more."""
        self._closed = True
        self._pending = None
        self._again = False
        if self._worker is not None:
            self._worker.request_stop()

    # --- passes --------------------------------------------------------------------------

    def _start(self, task: str, engine) -> None:
        from desktop.gui_threads import _keep_until_finished
        # A later try of a failed promotion is superseded by any pass (a check finds the kept
        # build itself): its timer must not promote a build this pass may put in place or
        # remove (Codex review of PR #25).
        self._later = None
        before = getattr(engine, "_load_browse_map", None) if task == "check" else None
        ready = getattr(engine, "editions_ready", None)
        installed = self.build_id if (callable(ready) and ready()) else None
        worker = EditionsIndexWorker(task, live_dir=self.live_dir(), meta_mgr=engine.meta_mgr,
                                     var_mgr=engine.var_mgr, inputs=self._inputs, before_stamp=before,
                                     installed_build=installed)
        worker.finished.connect(self._on_finished)
        self._worker = worker
        self.status = "checking" if task == "check" else "promoting"
        _keep_until_finished(worker)
        worker.start()

    def _on_finished(self) -> None:
        worker, self._worker = self._worker, None
        if worker is None:
            return
        result = worker.result
        kind = result.get("kind")
        self.reason = result.get("reason") or ""
        leaf = result.get("leaf")
        if self._closed:
            if leaf is not None:
                leaf.close_index()
            return
        if kind == CURRENT:
            self.status = "ready"
        elif kind == OPENED:
            engine = self._engine()
            if engine is None or not hasattr(engine, "install_editions"):
                leaf.close_index()
            elif self._suspended:
                # Opened by a pass that began before an input was being replaced: it is
                # about to be stale. Not installed; resume() checks again.
                leaf.close_index()
                self.status = "suspended"
                self._again = True
            else:
                engine.install_editions(leaf, result.get("path"))
                self.build_id = result.get("build_id")
                self.status = "ready"
                self._promotion_tries = 0
                LOGGER.info("Scholarly transcriptions' index open (%s)", result.get("path"))
        elif kind == BUILT:
            self._pending = result.get("path")
            self._promotion_tries = 0
            self.status = "built"
            if self._suspended:
                self._pending = None
                self._again = True
        else:
            self.status = kind or FAILED
            log = LOGGER.info if kind in (UNAVAILABLE, CANCELLED, INPUTS_CHANGED) else LOGGER.warning
            log("Scholarly transcriptions' index: %s (%s)", self.status, self.reason)
            if result.get("again"):
                self._again = True
            if result.get("retry_promotion") and not self._suspended:
                self._promotion_tries += 1
                if self._promotion_tries < self.PROMOTE_LATER_TRIES:
                    self._later = result.get("path")
                    self.status = "retrying"
                    QTimer.singleShot(self.PROMOTE_LATER_MS, self._promote_later)
        if self._pending is not None:
            self._promote_when_idle()
        elif self._again and not self._suspended:
            self.check()

    def _promote_later(self) -> None:
        """Try a promotion that failed again (PROMOTE_LATER_MS on), unless something else
        has started since (a check, a suspend: they find the kept build themselves)."""
        path, self._later = self._later, None
        if path is None or self._closed or self._suspended:
            return
        if self._worker is not None or self._pending is not None or not os.path.isdir(path):
            return                            # another pass took over, or the build is gone
        self._pending = path
        self._promote_when_idle()

    def _promote_when_idle(self) -> None:
        """Promote the built generation once nothing reads the index; a second at a time."""
        if self._closed or self._pending is None:
            return
        if self._suspended:
            self._pending = None
            self._again = True
            return
        engine = self._engine()
        if engine is None or not hasattr(engine, "close_editions"):
            self._pending = None
            return
        if self._worker is not None or self._readers_busy():
            self.status = "waiting"
            QTimer.singleShot(self.PROMOTE_RETRY_MS, self._promote_when_idle)
            return
        self._pending = None
        engine.close_editions()               # its files, released before the renames
        self._start("promote", engine)
