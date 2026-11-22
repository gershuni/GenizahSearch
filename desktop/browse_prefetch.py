"""Browse's neighbours on the shelf, prepared while the reader looks at a manuscript.

Measured on 2026-11-22 in the desktop window (from Israel): moving to the next manuscript on
a Cambridge shelf took 5.2 s to its sharp image -- about 2 s of metadata refresh (NLI's
catalogue record and image list, then Cambridge's image list), then about 3 s for the
2000-px Cambridge image, with no preview. This module prepares the manuscripts beside the
one on screen, so a move to one of them finds everything ready:

- its metadata (``enrich_metadata``, unless the cache holds a whole answer from this
  session), then its first page's preview, then its sharp image, into the image cache
  (desktop/image_loader.py, disk and memory);
- the manuscripts the window names (``neighbours``): the next two the reader's way and the
  one behind, one manuscript at a time, starting ``DELAY_MS`` after the manuscript on
  screen has been drawn -- a modest load on the libraries' servers;
- dropped when the reader moves elsewhere (``cancel``). A move that keeps the manuscript
  being prepared among the new neighbours, or onto it, keeps its download running: the
  viewer's loader for the same image waits for it instead of downloading it again.

Every thread is kept referenced until it has finished (a QThread destroyed while running
aborts the app; desktop/qthread_exit.py). Nothing here runs on the UI thread but the
bookkeeping and the callables the window passes (``neighbours``, ``image_urls``,
``fresh_meta``), which read memory only.
"""
from __future__ import annotations

from typing import Callable, Optional

from PyQt6.QtCore import QObject, QTimer

from desktop.image_loader import ImageLoaderThread

#: After the manuscript on screen is drawn, before its neighbours are prepared.
DELAY_MS = 400


class BrowsePrefetcher(QObject):
    """Prepares the manuscripts beside the one Browse shows.

    ``meta_mgr``: a callable returning the metadata manager. ``neighbours(sid)``: the
    manuscripts to prepare, in order. ``fresh_meta(sid)``: the cached metadata when it
    needs no refresh, else None. ``image_urls(sid, meta)``: the URLs to fetch, in order
    (the first page's preview, then its sharp image). ``enrich_thread`` and ``loader``
    are the thread classes (tests replace them).
    """

    def __init__(self, *, meta_mgr: Callable, neighbours: Callable, fresh_meta: Callable,
                 image_urls: Callable, parent=None, enrich_thread=None, loader=None,
                 delay_ms: int = DELAY_MS):
        super().__init__(parent)
        if enrich_thread is None:
            from desktop.gui_threads import EnrichMetadataThread as enrich_thread
        self._meta_mgr = meta_mgr
        self._neighbours = neighbours
        self._fresh_meta = fresh_meta
        self._image_urls = image_urls
        self._enrich_thread = enrich_thread
        self._loader = loader or ImageLoaderThread
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(delay_ms)
        self._timer.timeout.connect(self._start)
        self._anchor: Optional[str] = None
        self._queue: list = []
        self._job: Optional[dict] = None
        self._serial = 0
        self._kept: list = []      # threads of dropped jobs, kept until they finish
        self.prepared: set = set()  # manuscripts whose first page is in the cache
        self._stopped = False

    # --- what the window calls ------------------------------------------------------

    def schedule(self, sid: str) -> None:
        """``sid`` is on screen: prepare its neighbours after ``DELAY_MS``."""
        if self._stopped or not sid:
            return
        self._anchor = sid
        self._timer.start()

    def cancel(self, keep=None) -> None:
        """The reader moved: forget the queue, drop the running job unless it prepares one of
        ``keep`` (a sys_id or several: the manuscript now being opened, whose downloads the
        viewer waits for, and its own neighbours). A loader dropped before its answer came
        throws the download away, and a reader walking the shelf found the manuscript after
        next fetched again (2026-11-22)."""
        self._timer.stop()
        self._queue = []
        self._anchor = None
        kept = {keep} if isinstance(keep, str) else set(keep or ())
        job = self._job
        if job is not None and job['sid'] not in kept:
            self._drop_job()

    def stop(self) -> None:
        """The window closes: nothing more starts; running threads are asked to stop and
        stay referenced until they finish."""
        self._stopped = True
        self.cancel()
        self._drop_job()

    def job_sid(self) -> Optional[str]:
        return self._job['sid'] if self._job else None

    def pending_enrichment(self, sid: str):
        """The metadata thread still preparing ``sid``, or None. Browse opening ``sid`` meanwhile
        listens to it (its ``finished_signal``) instead of starting a second one, which would
        race it for the cache entry (Codex on #98)."""
        job = self._job
        if job is None or job['sid'] != sid or job['urls'] is not None:
            return None
        for thread in job['threads']:
            if hasattr(thread, 'finished_signal') and thread.isRunning():
                return thread
        return None

    # --- the work ---------------------------------------------------------------------

    def _start(self) -> None:
        if self._stopped or not self._anchor:
            return
        try:
            wanted = [s for s in (self._neighbours(self._anchor) or []) if s and s != self._anchor]
        except Exception:  # an aid: never disturb the page on screen
            wanted = []
        job = self._job
        # A job preparing the manuscript now on screen finishes: the viewer waits on its download.
        if job is not None and job['sid'] not in wanted and job['sid'] != self._anchor:
            self._drop_job()
        running = self._job['sid'] if self._job else None
        self._queue = [s for s in wanted if s not in self.prepared and s != running]
        self._next()

    def _next(self) -> None:
        if self._stopped or self._job is not None:
            return
        while self._queue:
            sid = self._queue.pop(0)
            self._serial += 1
            job = {'sid': sid, 'serial': self._serial, 'threads': [], 'urls': None}
            self._job = job
            try:
                cached = self._fresh_meta(sid)
            except Exception:
                cached = None
            if cached is not None:
                self._on_meta(job['serial'], sid, cached)
                return
            meta_mgr = self._meta_mgr()
            if meta_mgr is None:
                self._job = None
                return
            thread = self._enrich_thread(meta_mgr, sid)
            thread.finished_signal.connect(lambda s, m, n=job['serial']: self._on_meta(n, s, m))
            self._track(job, thread)
            thread.start()
            return

    def _on_meta(self, serial: int, sid: str, meta) -> None:
        job = self._job
        if job is None or job['serial'] != serial:
            return
        try:
            urls = list(self._image_urls(sid, meta) or []) if meta else []
        except Exception:
            urls = []
        job['urls'] = [u for u in urls if u]
        self._load_next_url(serial)

    def _load_next_url(self, serial: int) -> None:
        job = self._job
        if job is None or job['serial'] != serial:
            return
        if not job['urls']:
            self.prepared.add(job['sid'])
            self._job = None
            self._next()   # the queue is empty after a cancel, until the next schedule fills it
            return
        url = job['urls'].pop(0)
        thread = self._loader(url)
        thread.image_loaded.connect(lambda _img, n=serial: self._load_next_url(n))
        thread.load_failed.connect(lambda n=serial: self._abandon(n))
        self._track(job, thread)
        thread.start()

    def _abandon(self, serial: int) -> None:
        """An image of this manuscript failed: leave the rest of it to the reader's visit."""
        job = self._job
        if job is None or job['serial'] != serial:
            return
        job['urls'] = []
        self._load_next_url(serial)

    # --- threads ----------------------------------------------------------------------

    def _track(self, job: dict, thread) -> None:
        job['threads'].append(thread)
        thread.finished.connect(lambda t=thread, j=job: self._finished(j, t))

    def _finished(self, job: dict, thread) -> None:
        if thread in job['threads']:
            job['threads'].remove(thread)
        if thread in self._kept:
            self._kept.remove(thread)
        try:
            thread.deleteLater()
        except RuntimeError:
            pass

    def _drop_job(self) -> None:
        job, self._job = self._job, None
        if job is None:
            return
        for thread in list(job['threads']):
            for signal_name in ('image_loaded', 'load_failed', 'finished_signal'):
                signal = getattr(thread, signal_name, None)
                if signal is not None:
                    try:
                        signal.disconnect()
                    except (TypeError, RuntimeError):
                        pass
            cancel = getattr(thread, 'cancel', None)
            if cancel is not None:
                cancel()
            if thread.isRunning():
                self._kept.append(thread)
            # finished -> _finished removes it from _kept and deletes it
