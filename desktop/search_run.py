# -*- coding: utf-8 -*-
"""The search workers of the Search and Composition tabs: Stop, New, a new run and Close
never wait (GS-14).

Stop used to wait on the UI thread up to five seconds (New three, Close two) for
the worker to reach a cancel checkpoint, then ``QThread.terminate()`` it, which can
kill a thread inside the Tantivy searcher or an SQLite handle. Cancellation has no
time bound (a native regex, a catalogue query, the editions' grouping run without
a checkpoint), so no wait is long enough and no terminate is safe.

Now (owner ruling D1, "detach"):

- **Stop** asks the worker to stop and lets it finish in the background. It still
  delivers what it found so far, shown as partial results, as before.
- **New**, **Close** and **a new search** over a running one also stop listening
  to it: whatever it delivers later is dropped.
- A stopped worker is held until its thread ends (``_keep_until_finished``), so it
  is never destroyed while running; at exit, ``desktop.qthread_exit`` settles any
  still running.
- At most one stopped worker may still be finishing when a new run starts: a
  third run waits (``can_start``), so a run of slow stops cannot pile up workers
  on the index.
- The index install waits for the stopped ones too (``running``).

The window keeps its worker in ``search_thread`` (many callers and tests read it);
this object only holds the stopped ones. ``search_run_of(window)`` makes it on
first use, so windows built without ``__init__`` and test hosts have one.

The Composition tab has its own holder (``comp_run_of``), for its scan
(``comp_thread``) and its grouping (``group_thread``), under the same rules (PR 5).
Its deliveries are also dropped by the tab's New generation; the holder cuts the
signals every scan and grouping worker has, progress included, so a late progress
from a discarded run cannot move the next run's bar. A worker discarded during a
letter-level passage scan is tagged ``'passage'``: a close still waits for it
(``running(tag='passage')``), as it waits for a running one.
"""
from desktop.gui_threads import _keep_until_finished

# Every signal a search worker (SearchThread, LabSearchThread, ChainCompletionThread)
# may connect to the window. QThread.finished stays: it releases the held worker.
RESULT_SIGNALS = (
    'results_signal', 'finished_signal', 'error_signal', 'progress_signal', 'preview_signal',
    'phase_signal', 'status_signal', 'cutoff_signal', 'downgrade_signal', 'editions_signal',
    'perf_signal', 'pause_ack_signal',
)
# The Composition scan's and GroupingThread's signals that reach the window without the
# tab's generation gate, or that a Stop of grouping must cut (its results are dropped).
COMP_SIGNALS = ('progress_signal', 'status_signal', 'finished_signal', 'error_signal')


def _ask_to_stop(worker):
    """``request_cancel`` (the flag, and a paused worker un-parked); GroupingThread has
    none and reads ``isInterruptionRequested()``."""
    cancel = getattr(worker, 'request_cancel', None)
    if cancel is not None:
        cancel()
    else:
        worker.requestInterruption()


def _running(worker):
    try:
        return worker is not None and worker.isRunning()
    except RuntimeError:          # the C++ object is gone
        return False


class SearchRun:
    """A tab's stopped workers, still finishing in the background."""

    def __init__(self, signals=RESULT_SIGNALS):
        self.signals = signals
        self.stopping = []
        self.tags = []            # (worker, tag) for the held workers that have one

    def running(self, current=None, tag=None):
        """Every worker of the tab still running: ``current`` and the stopped ones. With
        ``tag``, only the stopped ones discarded with that tag."""
        self.stopping = [w for w in self.stopping if _running(w)]
        self.tags = [(w, t) for w, t in self.tags if _running(w)]
        if tag is not None:
            return [w for w, t in self.tags if t == tag]
        found = list(self.stopping)
        if _running(current) and not any(w is current for w in found):
            found.append(current)
        return found

    def can_start(self, current):
        """A new run may start while at most one older worker is still finishing."""
        return len(self.running(current)) <= 1

    def stop(self, current):
        """Stop: ask ``current`` to stop and keep it until it ends. Its partial results
        still arrive. Never waits, never terminates. True if it was running."""
        if not _running(current):
            return False
        _ask_to_stop(current)
        self._hold(current)
        return True

    def discard(self, current, tag=None):
        """New, Close or a new run: stop ``current`` and drop whatever it delivers
        from now on. True if it was running and not already held (a second New, or New
        after Stop, still cuts its signals but stops nothing twice)."""
        if not _running(current):
            return False
        held = any(w is current for w in self.stopping)
        if not held:
            _ask_to_stop(current)
            if tag is not None:
                self.tags.append((current, tag))
        for name in self.signals:
            try:
                getattr(current, name).disconnect()
            except (TypeError, RuntimeError, AttributeError):
                pass              # not this worker's, or nothing connected
        self._hold(current)
        return not held

    def _hold(self, worker):
        if not any(w is worker for w in self.stopping):
            self.stopping.append(worker)
        try:
            _keep_until_finished(worker)
        except AttributeError:    # a stand-in without QThread.finished (tests); held above
            pass


def search_run_of(window):
    """The window's SearchRun, made on first use."""
    run = window.__dict__.get('_search_run')
    if run is None:
        run = window.__dict__['_search_run'] = SearchRun()
    return run


def comp_run_of(window):
    """The Composition tab's holder (scan and grouping), made on first use."""
    run = window.__dict__.get('_comp_run')
    if run is None:
        run = window.__dict__['_comp_run'] = SearchRun(COMP_SIGNALS)
    return run
