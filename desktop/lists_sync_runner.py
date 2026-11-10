# -*- coding: utf-8 -*-
"""The desktop's list syncs: one at a time, off the UI thread.

A worker thread only talks to Supabase. It never touches Qt, never writes a
file, and never reads or writes the live store:

- a download is fetched on the worker (ListsManager.fetch_cloud_state, with the
  rows to confirm taken on the UI thread first) and applied on the UI thread
  (apply_cloud_state: its snapshot, the merge, the save);
- an upload runs on the worker against a copy of the store (begin_upload, on
  the UI thread, also writes lists.pkl.pre-upload) and is installed on the UI
  thread when it ends, however it ends (finish_upload). Edits made meanwhile go
  to the live store at once; their removal and move bookkeeping is replayed on
  top of the upload's result, so each counts as made after it.

Everything the worker hands back -- progress, every record and cloud-id write
the upload makes, the final result -- goes through a queue that a 50 ms timer
drains on the UI thread. A close (shutdown) drains it once more and installs
what the upload already established; a worker still inside a request is a
daemon thread and is left to finish on its own, unread.

Jobs run strictly one after another: explicit ones in order, and automatic
upload requests folded into one follow-up upload taken with a fresh copy.

inline=True runs every stage on the calling thread (tests that drive the
window's handlers without an event loop).
"""
import logging
import queue
import threading
import time

logger = logging.getLogger(__name__)

POLL_MS = 50
# What an upload that succeeded can still leave for the next one to send: memberships it
# could not check (a failed confirmation, a short read), entries waiting for a Download to
# fold them, and lists it could not reach -- the counters the engine's 'complete' is made of.
# After such an upload its changes stay unsent. What an upload leaves as it is on purpose --
# a note too long for the account, notes that differ and wait for Merge Both, a move into a
# list in the Trash -- does not keep them unsent: uploading again would not send it, and
# each has its own line. (A failed item or removal makes the upload fail: unsent too.)
RETRYABLE_LEFT = ('unchecked', 'waiting', 'lists_not_uploaded')
# A sign-out uploads when something asked for an upload since the last one, or
# when nothing was synced in the last minute.
RECENT_SYNC_S = 60


def sync_dialog_allowed(visible, restoring, modal_open, logout_pending, closing):
    """Whether a list-sync question (the sync choice, the website-removal prompt) may open now.

    Only on the shown main window, with no session restore running, no modal dialog
    open, no sign-out pending, and no close pending or under way.
    """
    return bool(visible) and not restoring and not modal_open and not logout_pending and not closing


class SyncJob:
    """One queued or running sync. on_done(outcome) is called once, whatever its end.

    outcome: {'kind', 'cancelled', and then some of 'preview', 'download', 'upload',
    'skipped', 'stopped', 'stale', 'deadline', 'shutdown', 'error'}.
    """

    KINDS = ('preview', 'download', 'upload', 'merge', 'auto', 'logout')

    def __init__(self, kind, on_done=None, on_progress=None, epoch=0, user_id=None, deadline=None):
        if kind not in self.KINDS:
            raise ValueError(f"unknown list sync job {kind!r}")
        self.kind = kind
        self.on_done = on_done
        self.on_progress = on_progress
        self.epoch = epoch
        self.user_id = user_id
        self.deadline = deadline        # time.monotonic() value, or None
        self.cancelled = False
        self.started = False
        self.done = False
        self.outcome = {'kind': kind, 'cancelled': False}
        self.copy = self.base = None    # the upload stage: the store copy the worker writes, and what it began from
        self.recorded = []              # every report the upload made, whichever drain read it
        self.took_changes = False       # its upload's copy held changes not yet in the account
        self.backfill = kind != 'logout'

    def past_deadline(self):
        return self.deadline is not None and time.monotonic() > self.deadline


class ListsSyncRunner:
    """The one owner of every list sync of the desktop window (see the module docstring)."""

    def __init__(self, lists_mgr, parent=None, on_auto_done=None, inline=False):
        self._mgr = lists_mgr
        self._inline = inline
        self._results = queue.Queue()
        self._job = None
        self._waiting = []
        self._auto_wanted = False
        self._auto_allowed = True
        self._dirty = False
        self._last_upload_ok = True
        self._closed = False
        self.auth_epoch = 0
        self.on_auto_done = on_auto_done
        self._timer = None
        if not inline:
            from PyQt6.QtCore import QTimer  # noqa: PLC0415 - the drain needs Qt; the gate above does not
            self._timer = QTimer(parent)
            self._timer.setInterval(POLL_MS)
            self._timer.timeout.connect(self._poll)
        self.seed_unsent()

    # --- what the window calls (UI thread) -----------------------------------------

    @property
    def busy(self):
        return self._job is not None or bool(self._waiting)

    @property
    def unsent(self):
        """Changes not known to be in the account: edits made since the last upload's copy,
        the last upload did not succeed, or the upload running now took changes with it
        (they count as sent once it succeeds)."""
        job = self._job
        return (self._dirty or not self._last_upload_ok
                or bool(job is not None and job.copy is not None and job.took_changes))

    @property
    def closed(self):
        return self._closed

    def invalidate_auth(self):
        """A sign-out or a new sign-in: what runs or waits now is stale (it stops, and is not shown)."""
        self.auth_epoch += 1

    def allow_auto(self):
        if not self._closed:
            self._auto_allowed = True

    def mark_dirty(self):
        """Edits were made that no upload has taken yet (while list sync was off)."""
        if not self._closed:
            self._dirty = True

    def seed_unsent(self):
        """Changes saved in lists.pkl that no upload of this session took -- made while offline,
        or left by an upload that did not finish before the program closed -- count as unsent
        (ListsManager.has_unsent_changes). Asked when the runner is made and at each log-in,
        never per edit. When the store cannot be read for it, they count as unsent."""
        if self._closed:
            return
        try:
            user = self._mgr.cloud_user()
            if self._mgr.has_unsent_changes(user):
                self._dirty = True
        except Exception:
            logger.warning("Could not tell whether the lists hold unsent changes; taking them as unsent",
                           exc_info=True)
            self._dirty = True

    def request_auto(self):
        """A list changed: upload it now, or once after what runs or waits."""
        if self._closed:
            return
        self._dirty = True
        if not self._auto_allowed:
            return
        if self.busy:
            self._auto_wanted = True
            return
        self._start(self._new_job('auto'))

    def run(self, kind, on_done=None, on_progress=None):
        """Queue a sync ('preview', 'download', 'upload', 'merge'). Returns its job, or None once closed."""
        if self._closed:
            return None
        job = self._new_job(kind, on_done, on_progress)
        if self.busy:
            self._waiting.append(job)
            self._tell_progress(job, 'waiting', 0, 0)
        else:
            self._start(job)
        return job

    def begin_logout(self, on_done, budget_s):
        """No more automatic uploads; one last upload if one is needed, within budget_s.

        The job running now is held to the same deadline. With nothing running, a sign-out
        that needs no upload completes at once ({'skipped': True}); behind a running job
        it is decided when its turn comes, so an upload that fails meanwhile is retried.
        Returns the logout job, or None once closed.
        """
        if self._closed:
            return None
        self._auto_allowed = False
        if self._auto_wanted:
            self._auto_wanted = False
            self._dirty = True
        deadline = time.monotonic() + budget_s
        if self._job is not None:
            self._job.deadline = deadline if self._job.deadline is None else min(self._job.deadline, deadline)
        job = self._new_job('logout', on_done, deadline=deadline)
        if self.busy:
            self._waiting.append(job)
        else:
            self._start(job)
        return job

    def cancel(self, job):
        """Stop a job: a waiting one completes at once; a running one stops before its next request
        and completes when its worker returns. A finished job is left alone."""
        if job is None or job.done or job.cancelled:
            return
        job.cancelled = True
        job.outcome['cancelled'] = True
        if job in self._waiting:
            self._waiting.remove(job)
            self._complete(job)

    def shutdown(self):
        """The window closes: install what the upload already did, complete every job, stop. Never blocks."""
        if self._closed:
            return
        self._closed = True
        self._auto_wanted = False
        job = self._job
        final = None
        while True:
            try:
                msg = self._results.get_nowait()
            except queue.Empty:
                break
            if msg[1] is not job:
                continue
            if msg[0] == 'recorded':
                job.recorded.append(msg[2])
            elif msg[0] == 'done':
                final = msg
        if job is not None and job.copy is not None:
            try:
                if final is not None and final[2] == 'upload':
                    self._mgr.finish_upload(job.copy, job.base, self._value(final[3], final[4]))
                else:
                    self._mgr.abandon_upload(job.base, list(job.recorded), job.user_id)
            except Exception:
                logger.exception("Installing the upload at close failed")
            job.copy = job.base = None
        # a running fetch's state is dropped unapplied
        for j in [job] + list(self._waiting):
            if j is None or j.done:
                continue
            j.cancelled = True
            j.outcome.update(cancelled=True, shutdown=True)
            self._complete(j, start_next=False, direct=True)
        self._waiting.clear()
        self._job = None
        if self._timer is not None:
            try:
                self._timer.stop()
            except Exception:
                pass

    # --- stages ----------------------------------------------------------------------

    def _new_job(self, kind, on_done=None, on_progress=None, deadline=None):
        user = None
        try:
            user = self._mgr.cloud_user()
        except Exception:
            logger.debug("Could not read the list sync's account", exc_info=True)
        return SyncJob(kind, on_done, on_progress, epoch=self.auth_epoch, user_id=user, deadline=deadline)

    def _should_stop(self, job):
        return job.cancelled or job.past_deadline() or job.epoch != self.auth_epoch

    def _stale(self, job):
        """Checked on the UI thread before a result is used: an old sign-in, or past the sign-out's deadline."""
        if job.epoch != self.auth_epoch or job.past_deadline():
            return True
        try:
            return self._mgr.cloud_user() != job.user_id
        except Exception:
            return True

    def _logout_needs_upload(self):
        last = getattr(self._mgr, '_last_sync', 0) or 0
        return self._dirty or not self._last_upload_ok or (time.time() - last) >= RECENT_SYNC_S

    def _start(self, job):
        self._job = job
        job.started = True
        self._tell_progress(job, 'start', 0, 0)
        if self._timer is not None and not self._timer.isActive():
            self._timer.start()
        try:
            if self._should_stop(job):
                job.outcome['cancelled'] = True
                if job.past_deadline():
                    job.outcome['deadline'] = True
                self._complete(job)
                return
            if job.kind == 'preview':
                self._spawn(job, 'preview', lambda: self._mgr.get_cloud_lists_preview(
                    should_stop=lambda: self._should_stop(job)))
            elif job.kind in ('download', 'merge'):
                ids = self._mgr.remembered_row_ids(job.user_id)
                self._spawn(job, 'fetch', lambda: self._mgr.fetch_cloud_state(
                    ids, should_stop=lambda: self._should_stop(job), progress=self._progress_cb(job, 'download')))
            elif job.kind == 'logout' and not self._logout_needs_upload():
                job.outcome['skipped'] = True
                self._complete(job)
            else:
                self._start_upload(job)
        except Exception as e:
            logger.exception("Starting a list sync failed")
            job.outcome['error'] = str(e)
            self._complete(job)

    def _start_upload(self, job):
        if self._should_stop(job):
            job.outcome['cancelled'] = True
            if job.past_deadline():
                job.outcome['deadline'] = True
            self._complete(job)
            return
        job.took_changes = self.unsent
        job.copy, job.base = self._mgr.begin_upload()
        job.recorded = []
        self._dirty = False        # edits from now on belong to the next upload
        mgr, copy_ = self._mgr, job.copy

        def report(rep, job=job):
            self._results.put(('recorded', job, rep))
        try:
            self._spawn(job, 'upload', lambda: mgr.sync_to_cloud(
                data=copy_, should_stop=lambda: self._should_stop(job), progress=self._progress_cb(job, 'upload'),
                backfill_pages=job.backfill, withdrawn=mgr.withdrawn_now, on_recorded=report))
        except Exception:
            if job.copy is not None:     # the worker never started: close the journal the upload opened
                mgr.finish_upload(job.copy, job.base, {'success': False, 'error': 'not started'})
                job.copy = job.base = None
            raise

    def _spawn(self, job, stage, fn):
        def work():
            try:
                value, ok = fn(), True
            except BaseException as e:   # reported on the UI thread, never raised here
                value, ok = e, False
            self._results.put(('done', job, stage, ok, value))

        if self._inline:
            work()
            self._drain()
            return
        threading.Thread(target=work, name=f"lists-sync-{job.kind}", daemon=True).start()

    def _progress_cb(self, job, stage):
        # the engine's progress(done, total, what): what 'deletes' is the upload's step
        # that sends the removals made here, told as its own stage
        def progress(done, total, what='lists'):
            self._results.put(('progress', job, 'deletes' if what == 'deletes' else stage, done, total))
        return progress

    def _poll(self):
        try:
            self._drain()
        except Exception:
            logger.exception("Draining the list sync results failed")

    def _drain(self):
        while not self._closed:
            try:
                msg = self._results.get_nowait()
            except queue.Empty:
                return
            kind, job = msg[0], msg[1]
            if job is not self._job or job.done:
                continue
            if kind == 'recorded':
                job.recorded.append(msg[2])
            elif kind == 'progress':
                if not job.cancelled:
                    self._tell_progress(job, msg[2], msg[3], msg[4])
            else:
                try:
                    self._after(job, msg[2], msg[3], msg[4])
                except Exception as e:
                    logger.exception("A list sync step failed")
                    if job.copy is not None:
                        try:
                            self._mgr.finish_upload(job.copy, job.base, {'success': False, 'error': str(e)})
                        except Exception:
                            logger.exception("Installing the upload after a failure failed")
                        job.copy = job.base = None
                    job.outcome['error'] = str(e)
                    self._complete(job)

    @staticmethod
    def _value(ok, value):
        if ok:
            return value if isinstance(value, dict) else {'success': False, 'error': 'Unknown error'}
        logger.error("A list sync stage failed: %s", value)
        return {'success': False, 'error': str(value) or type(value).__name__}

    def _after(self, job, stage, ok, value):
        value = self._value(ok, value)
        if stage == 'preview':
            if self._stale(job):
                job.outcome.update(cancelled=True, stale=True)
            else:
                job.outcome['preview'] = value
            self._complete(job)
        elif stage == 'fetch':
            if job.cancelled or value.get('stopped'):
                job.outcome['cancelled'] = True     # nothing local was read or changed
                self._complete(job)
            elif self._stale(job):
                job.outcome.update(cancelled=True, stale=True)
                self._complete(job)
            elif not value.get('success'):
                job.outcome['download'] = {k: v for k, v in value.items() if k != 'pass'}
                self._upload_after_download(job)
                self._complete(job)
            else:
                applied = self._mgr.apply_cloud_state(value)
                job.outcome['download'] = applied
                if job.kind != 'merge' or not applied.get('success'):
                    self._upload_after_download(job)
                    self._complete(job)       # a Merge whose download failed never uploads
                elif job.cancelled:
                    job.outcome['cancelled'] = True
                    self._complete(job)
                elif self._stale(job):
                    job.outcome.update(cancelled=True, stale=True)
                    self._complete(job)
                else:
                    self._start_upload(job)
        elif stage == 'upload':
            # its facts are the store's now, however it ended (a stopped or failed upload
            # recorded what it did send)
            try:
                self._mgr.finish_upload(job.copy, job.base, value)
            finally:
                job.copy = job.base = None
                job.recorded = []
            # an upload that left something for the next one to send did not send all it took
            ok_now = bool(value.get('success')) and not any(value.get(k) for k in RETRYABLE_LEFT)
            self._last_upload_ok = ok_now
            if not ok_now:
                self._dirty = True        # the rest still has to go up
            job.outcome['upload'] = value
            if value.get('stopped'):
                job.outcome['stopped'] = True
                job.outcome['cancelled'] = job.cancelled
                if job.past_deadline():
                    job.outcome['deadline'] = True
            self._complete(job)

    def _upload_after_download(self, job):
        """A Download sends nothing: when changes made here are still not in the account,
        one automatic upload follows it (what the log-out texts promise after the next log-in)."""
        if job.kind == 'download' and self.unsent and self._auto_allowed and not self._closed:
            self._auto_wanted = True

    def _complete(self, job, start_next=True, direct=False):
        """The one end of every job: once, whatever the exit."""
        if job.done:
            return
        job.done = True
        if job in self._waiting:
            self._waiting.remove(job)
        if job is self._job:
            self._job = None
        auto_done = None
        if job.kind == 'auto' and self.on_auto_done is not None:
            stale = job.outcome.get('shutdown') or job.outcome.get('cancelled') or self._stale(job)
            if not stale:
                auto_done = self.on_auto_done
        if start_next and not self._closed and self._job is None:
            nxt = None
            if self._waiting:
                nxt = self._waiting.pop(0)
            elif self._auto_wanted and self._auto_allowed:
                self._auto_wanted = False
                nxt = self._new_job('auto')
            if nxt is not None:
                self._start(nxt)
        if self._job is None and not self._waiting and self._timer is not None:
            try:
                self._timer.stop()
            except Exception:
                pass
        outcome = dict(job.outcome)
        for fn in (job.on_done, auto_done):
            if fn is not None:
                self._deliver(fn, outcome, direct)

    def _deliver(self, fn, outcome, direct):
        if direct or self._inline or self._timer is None:
            self._call(fn, outcome)
            return
        from PyQt6.QtCore import QTimer  # noqa: PLC0415
        # after this drain returns: a handler may open a dialog
        QTimer.singleShot(0, lambda: self._call(fn, outcome))

    def _tell_progress(self, job, stage, done, total):
        if job.on_progress is not None:
            self._call(job.on_progress, stage, done, total)

    @staticmethod
    def _call(fn, *args):
        try:
            fn(*args)
        except Exception:
            logger.exception("A list sync callback failed")
