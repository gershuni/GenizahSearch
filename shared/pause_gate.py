"""Cooperative pause/resume for a single search worker thread.

Deliberately plain Python — no Qt, no project imports. ``gui_threads`` imports
PyQt6 and ``genizah_core`` at module scope, and this is the one piece of the
Pause feature that is pure timing-sensitive logic: keeping it Qt-free is what
lets it be unit-tested exhaustively and fast.

Design notes worth keeping in view when editing:

* **The gate blocks; it never raises.** ``wait_if_paused`` returns a bool and
  the caller decides what cancellation means. The ``InterruptedError`` raise
  lives in ``gui_threads`` next to every other cancel in this codebase, so the
  gate stays free of both Qt and policy.
* **The gate knows nothing about runs.** It publishes only the pause *epoch* it
  parked under. The worker mixin stamps the run identity on top. Two levels,
  one owner each.
* **The poll is a fallback, not a guarantee.** Every stop path in the app calls
  ``request_cancel()`` (flag + ``abort()``), and the mixin overrides
  ``requestInterruption()`` to route into it, so a parked worker is woken
  immediately. ``POLL_INTERVAL_S`` bounds the damage if some future call site
  does neither. It cannot help a worker stuck *between* checkpoints — that one
  is not parked, so nothing can reach it.

Threading contract (violating it is a bug, not a race to survive):

* constructed on the UI thread;
* ``pause()`` / ``resume()`` / ``abort()`` called ONLY from the UI thread;
* ``wait_if_paused()`` called ONLY from the worker thread, and only from inside
  that worker's progress callback;
* ``finish()`` called from the worker's ``finally``;
* exactly one worker per gate.

The ``on_ack`` / ``on_park`` / ``on_unpark`` hooks fire on the WORKER thread and
must never call back into the gate.
"""

import threading
import time


class PauseGate:
    """Park a worker between checkpoints until the UI resumes or cancels it."""

    #: Ceiling on how long a parked worker can stay parked after a cancel that
    #: bypassed ``abort()`` (a bare ``cancel_flag = True`` write, say). Small
    #: enough to be invisible against the app's 2000-5000 ms stop budgets.
    POLL_INTERVAL_S = 0.1

    RUNNING = 'running'
    PAUSING = 'pausing'
    PAUSED = 'paused'

    def __init__(self, on_ack=None):
        self._resume_ev = threading.Event()
        self._resume_ev.set()               # set == running
        # Guards _state / _epoch / _closed ONLY. NEVER held across a wait, and
        # never held while an on_* hook runs.
        self._lock = threading.Lock()
        self._state = self.RUNNING
        self._epoch = 0
        self._closed = False
        #: Total seconds spent parked. Written by the worker on unpark, read by
        #: the worker when it reports timings. NOT suitable for driving a live
        #: display: it only advances when a pause *ends*.
        self.total_paused_s = 0.0
        self.on_ack = on_ack                # (epoch:int) -> None, worker thread
        self.on_park = None                 # () -> None, worker thread
        self.on_unpark = None               # () -> None, worker thread

    # ---------------------------------------------------------------- UI side

    @property
    def state(self):
        with self._lock:
            return self._state

    @property
    def epoch(self):
        with self._lock:
            return self._epoch

    @property
    def closed(self):
        with self._lock:
            return self._closed

    def is_pause_pending(self):
        """True once pause() has been requested and until resume/abort/finish."""
        with self._lock:
            return (not self._closed) and self._state != self.RUNNING

    def pause(self):
        """Request a pause. Returns False if it was a no-op."""
        with self._lock:
            if self._closed or self._state in (self.PAUSING, self.PAUSED):
                return False
            self._epoch += 1
            self._state = self.PAUSING
            self._resume_ev.clear()
            return True

    def resume(self):
        """Release a parked (or about-to-park) worker. No-op if already running.

        Safe to call unconditionally — the cancel paths do exactly that rather
        than branching on state.
        """
        with self._lock:
            if self._closed or self._state == self.RUNNING:
                return False
            self._state = self.RUNNING
            self._resume_ev.set()
            return True

    def abort(self):
        """Terminally release the worker. Idempotent, and MUST never raise.

        Called from cancel paths that may be running during teardown, where an
        exception would propagate into ``closeEvent``.
        """
        try:
            with self._lock:
                self._closed = True
                self._state = self.RUNNING
            self._resume_ev.set()
        except Exception:
            pass

    # ------------------------------------------------------------ worker side

    def finish(self):
        """Worker-side terminal release, called from ``run()``'s ``finally``.

        Without this, a run that completes while a pause is still pending leaves
        the gate closed over a pause nothing will ever answer.
        """
        self.abort()

    def wait_if_paused(self, should_abort=None):
        """Park while paused. Returns True if the caller must abort NOW.

        Never raises. ``should_abort`` is injected so the gate does not need to
        know what cancellation means for this worker (``cancel_flag``, Qt's
        ``isInterruptionRequested()``, or both).
        """
        # Hot path: not paused, no lock, no allocation. This runs on every
        # progress tick of every search, so it stays cheap.
        if self._resume_ev.is_set():
            return self._closed or bool(should_abort and should_abort())

        # A cancel can arrive as a bare `cancel_flag = True` from a call site
        # that never touched the gate; that is invisible to _resume_ev. Check it
        # BEFORE publishing anything, or the UI paints "Paused" for a worker
        # that is about to abort and we park for one pointless poll interval.
        if should_abort and should_abort():
            return True

        # Atomically re-verify "still paused, not closed" and snapshot the epoch
        # before publishing. A resume() or abort() landing between the is_set()
        # check above and here would otherwise produce a phantom "paused"
        # acknowledgement and a pointless on_park/on_unpark cycle.
        with self._lock:
            if self._closed or self._resume_ev.is_set():
                return self._closed or bool(should_abort and should_abort())
            self._state = self.PAUSED
            ack_epoch = self._epoch

        if self.on_ack:
            try:
                self.on_ack(ack_epoch)
            except Exception:
                pass  # a broken listener must not strand a parked worker

        parked_at = time.monotonic()
        try:
            if self.on_park:
                try:
                    self.on_park()
                except Exception:
                    pass
            while not self._resume_ev.wait(self.POLL_INTERVAL_S):
                if self._closed or (should_abort and should_abort()):
                    return True
            return self._closed or bool(should_abort and should_abort())
        finally:
            self.total_paused_s += time.monotonic() - parked_at
            if self.on_unpark:
                try:
                    self.on_unpark()
                except Exception:
                    pass
