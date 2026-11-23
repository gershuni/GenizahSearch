"""The desktop's startup builds: one thread, one build at a time, in the order the reader needs them.

Measured 2026-11-23 with a timing driver on a Cambridge and an Oxford manuscript: about 33 s of
Python (the browse map, libraries.csv, the Oxford parts, the FL-id lookup, the CUDL alias index,
the Shelfmark search index, the completer) started in five or six threads within the first 6 s.
Python runs one thread at a time (the GIL), so every Qt callback waited for a build's time slice:
the window froze for 13-21 s, and the restored manuscript showed after 22-30 s.

Here one thread runs them in turn (``StartupWork``):

* Phase 1, at once: the browse map (the restored page's text), libraries.csv (shelfmarks and
  titles, the catalogue), the Oxford parts (an Oxford manuscript's images).
* Phase 2, once the restored manuscript is shown (``open_gate``), or ``GATE_TIMEOUT`` seconds
  after phase 1: the completer's rows, Browse's shelfmark resolver index, the CUDL alias index,
  the Shelfmark search index, the FL-id lookup, the file map. Ordered by first need (desktop usage, 2026-11-23): going to a
  shelfmark in Browse comes early, going to an FL id rarely. Each of them is also built on
  demand, so nothing waits for its turn here: a lookup before the alias index is built builds
  it (``shelfmark_bridge.ensure_alias_index``), the Shelfmark search builds its index, an FL id
  is found by a scan.
* Phase 2's builds give way to the user's searches, and the whole queue runs with fast GIL
  switching (``shared/background_work.py``).
* After each build, ``gc.freeze()``: the objects it made (a million browse-map pages, 255,000
  catalogue rows, the lookups) move to the permanent generation, so Python's full collections
  stop walking them. Before this, each full collection held the GIL 0.2-0.66 s, growing with
  every build, and the window stalled 0.4-0.7 s every few seconds (measured 2026-11-23); the
  web does the same once its loaders end (web/gc_freeze.py). Cyclic garbage alive at a freeze
  stays: a one-time, bounded cost.

The window hears of it through Qt signals (queued to the UI thread): ``browse_map_ready``,
``phase1_ready``, ``completer_rows_ready(rows)``, ``browse_resolver_ready``, ``finished``.
"""
from __future__ import annotations

import gc
import logging
import threading

from PyQt6.QtCore import QObject, pyqtSignal

from shared import background_work

LOGGER = logging.getLogger(__name__)

GATE_TIMEOUT = 8.0


def shelfmark_completer_rows(meta_mgr):
    """The shelfmark completer's rows, ``(text, key, part_id)``: each catalogue shelfmark
    once, sorted, keyed by ``ShelfmarkCompleter.normalize`` (part_id None), then the Oxford
    parts with their own keys. None when libraries.csv changed while it was read."""
    from desktop.widgets import ShelfmarkCompleter
    try:
        shelfmarks = sorted({v['shelfmark'] for v in list(meta_mgr.csv_bank.values())
                             if v.get('shelfmark')})
    except RuntimeError:
        return None
    normalize = ShelfmarkCompleter.normalize
    rows = []
    for n, text in enumerate(shelfmarks):
        if not n & 4095:
            background_work.checkpoint()
        rows.append((text, normalize(text), None))
    try:
        for part in meta_mgr.get_part_autocomplete_list():
            rows.append((part['display'], part['normalized'], part['part_id']))
    except Exception:
        LOGGER.debug("Oxford parts not in the shelfmark completer", exc_info=True)
    return rows


class StartupWork(QObject):
    """Runs the startup builds (see the module). ``start()`` once; ``open_gate()`` from the
    UI thread when the restored manuscript is shown, or when there is none."""

    browse_map_ready = pyqtSignal()
    phase1_ready = pyqtSignal()
    browse_resolver_ready = pyqtSignal()
    completer_rows_ready = pyqtSignal(object)
    finished = pyqtSignal()

    def __init__(self, meta_mgr, searcher, parent=None, *, gate_timeout=GATE_TIMEOUT,
                 gc_freeze=gc.freeze):
        super().__init__(parent)
        self.meta_mgr = meta_mgr
        self.searcher = searcher
        self.gate_timeout = gate_timeout
        self._gc_freeze = gc_freeze
        self._gate = threading.Event()
        self._thread = None
        self.browse_map_loaded = False
        self.phase1_done = False      # the browse map, libraries.csv and the Oxford parts
        self.browse_resolver_built = False
        self.done = False
        self.steps_done = []          # names, in the order they ended (tests, the timing driver)

    def start(self):
        if self._thread is not None:
            return
        # An Oxford manuscript's images come from the parts read in phase 1: an enrichment
        # that begins before they are in waits for them (MetadataManager.wait_for_oxford_parts).
        expect = getattr(self.meta_mgr, 'expect_oxford_parts', None)
        if expect is not None:
            expect()
        self._thread = threading.Thread(target=self._run, name="startup-work", daemon=True)
        self._thread.start()

    def open_gate(self):
        self._gate.set()

    def gate_open(self):
        return self._gate.is_set()

    def phase1_steps(self):
        return [
            ('browse map', self._load_browse_map),
            ('catalogue', self.meta_mgr.load_catalogue_now),
            ('oxford parts', self.meta_mgr.load_oxford_parts_now),
        ]

    def phase2_steps(self):
        from shared import shelfmark_bridge
        return [
            ('completer rows', self._completer_rows),
            ('browse resolver index', self._browse_resolver),
            ('alias index', shelfmark_bridge.ensure_alias_index),
            ('shelfmark search index', lambda: self.meta_mgr.prewarm_shelfmark_search(timeout=0)),
            ('fl-id lookup', self.searcher.build_fl_id_index_now),
            ('file map', self.meta_mgr._build_file_map_background),
        ]

    def _emit(self, signal, *args):
        # At the app's end the window and this object may be gone before the thread is.
        try:
            signal.emit(*args)
        except RuntimeError:
            LOGGER.debug("Startup work: the window is gone", exc_info=True)

    def _load_browse_map(self):
        try:
            self.searcher._load_browse_map()
        finally:
            self.browse_map_loaded = True
            self._emit(self.browse_map_ready)

    def _browse_resolver(self):
        try:
            self.meta_mgr.prewarm_browse_resolver()
        finally:
            self._resolver_settled()

    def _resolver_settled(self):
        if not self.browse_resolver_built:
            self.browse_resolver_built = True
            self._emit(self.browse_resolver_ready)

    def _completer_rows(self):
        rows = shelfmark_completer_rows(self.meta_mgr)
        if rows:
            self._emit(self.completer_rows_ready, rows)

    def _step(self, name, fn):
        try:
            fn()
        except MemoryError:
            raise
        except Exception:
            LOGGER.exception("Startup build %r failed; it is built on first use instead", name)
        self._gc_freeze()
        self.steps_done.append(name)

    def _run(self):
        try:
            with background_work.fast_switching():
                try:
                    for name, fn in self.phase1_steps():
                        self._step(name, fn)
                finally:
                    self.phase1_done = True
                    self._emit(self.phase1_ready)
                self._gate.wait(self.gate_timeout)
                with background_work.yielding():
                    for name, fn in self.phase2_steps():
                        self._step(name, fn)
        finally:
            self._resolver_settled()      # a waiter never waits for a step that did not run
            self.done = True
            self._emit(self.finished)
