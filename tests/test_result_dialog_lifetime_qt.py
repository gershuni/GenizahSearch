# -*- coding: utf-8 -*-
"""Real-Qt lifetime tests for the unparented Manuscript Viewer (PR #343).

Since 2026-09-17 ResultDialog has no Qt parent and its host deleteLater()s it
right after `finished`. Esc, reject(), accept() and done() emit `finished`
WITHOUT a closeEvent, so the worker teardown must run from `finished` too, and
before the host's deletion handler. Codex's review probe showed the failure
mode with the old closeEvent-only teardown: a late manuscript-image callback
raised "wrapped C/C++ object of type QSlider has been deleted".

These tests borrow the PRODUCTION methods (ResultDialog._teardown_workers,
_on_dialog_finished_teardown, closeEvent, cancel_image_thread,
_wait_or_terminate_thread, _on_pdf_dialog_finished, _scroll_to_first_highlight)
onto a minimal QDialog subclass, wire `finished` in the production order, and
drive real cooperative QThreads through every termination path.

Marked `gui`: constructs a real (offscreen) QApplication. Runs in CI's
dedicated fresh-process gui-tests job.
"""
from __future__ import annotations

import os
import sys
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402

pytestmark = pytest.mark.gui

from PyQt6 import sip  # noqa: E402
from PyQt6.QtCore import QCoreApplication, QEvent, QThread, Qt  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QDialog, QTextBrowser  # noqa: E402

import desktop.result_dialog as result_dialog  # noqa: E402

RD = result_dialog.ResultDialog
_APP = QApplication.instance() or QApplication(["pytest"])


class _Cooperative(QThread):
    """Runs until interrupted; records that it was asked to stop."""

    def __init__(self):
        super().__init__()
        self.interruptions = 0

    def requestInterruption(self):
        self.interruptions += 1
        super().requestInterruption()

    def cancel(self):                 # ImageLoaderThread's contract
        self.requestInterruption()

    def run(self):
        while not self.isInterruptionRequested():
            self.msleep(5)


class _Stubborn(QThread):
    """Ignores interruption and sleeps for `seconds`; only terminate() stops it early."""

    def __init__(self, seconds):
        super().__init__()
        self.seconds = seconds

    def run(self):
        end = time.monotonic() + self.seconds
        while time.monotonic() < end:
            self.msleep(20)


class _MsViewer:
    def __init__(self):
        self._closing = False
        self.stopped = 0

    def stop_threads(self):
        self._closing = True
        self.stopped += 1


class _Viewer(RD):
    """A real ResultDialog minus its heavy constructor: QDialog.__init__ plus the
    handful of attributes the production teardown, close and finish handlers
    read. Every method under test is INHERITED, so `super()` inside them binds
    correctly (borrowing them onto a plain QDialog aborts the interpreter when
    closeEvent's super() call meets a non-ResultDialog self)."""

    def __init__(self, stubborn_seconds=None):
        QDialog.__init__(self, None)          # skip ResultDialog.__init__ on purpose
        self._app = None
        self._pdf_scope = id(self)
        # production order (ResultDialog.__init__): PDF scope first, then teardown
        self.finished.connect(self._on_pdf_dialog_finished)
        self.finished.connect(self._on_dialog_finished_teardown)
        self.events = []
        self.finished.connect(lambda _c: self.events.append('teardown-done'))
        self.enrich_worker = _Cooperative()
        self._rd_pgp_worker = _Cooperative()
        self.preload_meta_worker = (_Stubborn(stubborn_seconds) if stubborn_seconds
                                    else _Cooperative())
        self.img_thread = _Cooperative()
        self.ms_viewer = _MsViewer()
        for w in self.workers():
            w.start()
        deadline = time.monotonic() + 2.0
        while not all(w.isRunning() for w in self.workers()):
            assert time.monotonic() < deadline, "workers did not start"
            time.sleep(0.01)

    def workers(self):
        return (self.enrich_worker, self._rd_pgp_worker,
                self.preload_meta_worker, self.img_thread)


class _Host:
    """What GenizahGUI._on_result_dialog_finished does, plus a recording."""

    def __init__(self, viewer):
        self.viewer = viewer
        self.saw_running_at_release = None
        viewer.finished.connect(self.release)

    def release(self, _code):
        self.saw_running_at_release = [w.isRunning() for w in self.viewer.workers()]
        self.viewer.events.append('host')
        self.viewer.deleteLater()


def _flush_deferred_deletes():
    for _ in range(5):
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        QCoreApplication.processEvents()


def _terminate(viewer, how):
    if how == 'reject':
        viewer.reject()
    elif how == 'accept':
        viewer.accept()
    elif how == 'done':
        viewer.done(7)
    elif how == 'esc':
        QTest.keyClick(viewer, Qt.Key.Key_Escape)
    elif how == 'close':
        assert viewer.close()
    else:
        raise AssertionError(how)


@pytest.mark.parametrize('how', ['reject', 'accept', 'done', 'esc', 'close'])
def test_every_termination_path_stops_the_workers_before_the_host_deletes(how):
    viewer = _Viewer()
    viewer.show()
    host = _Host(viewer)
    assert all(w.isRunning() for w in viewer.workers())

    _terminate(viewer, how)

    # the host's handler ran, and by then every worker had stopped
    assert 'host' in viewer.events
    assert host.saw_running_at_release == [False, False, False, False]
    assert viewer.enrich_worker.interruptions == 1        # once, not twice (close -> reject)
    assert viewer.ms_viewer.stopped == 1 and viewer.ms_viewer._closing
    assert viewer._workers_torn_down is True

    _flush_deferred_deletes()
    assert sip.isdeleted(viewer)


def test_teardown_wait_is_bounded_and_falls_back_to_terminate():
    """A worker that ignores interruption must not block the GUI thread for
    longer than the bounded wait; terminate() is the last resort."""
    viewer = _Viewer(stubborn_seconds=12.0)
    viewer.show()
    _Host(viewer)
    t0 = time.monotonic()
    viewer.reject()
    elapsed = time.monotonic() - t0
    assert not viewer.preload_meta_worker.isRunning()
    # bounded wait is 2 s, then terminate(); 8 s leaves a loaded runner 6 s of
    # slack while an unbounded wait would take the full 12 s
    assert elapsed < 8.0, f"teardown blocked for {elapsed:.1f}s; the wait is not bounded"
    _flush_deferred_deletes()


def test_deferred_highlight_scroll_survives_deletion_of_the_browser():
    """The QTimer.singleShot(0) closure may fire after the host's deleteLater()
    has destroyed the text browser; it must return, not raise."""
    browser = QTextBrowser()
    browser.setPlainText("alpha beta gamma")
    RD._scroll_to_first_highlight(object(), browser, "beta")   # schedules _do_scroll
    browser.deleteLater()

    # The hook goes on BEFORE any event processing: flushing the deferred
    # delete also fires the 0 ms timer, and PyQt aborts the process on an
    # exception that escapes a Qt callback unless a Python hook is installed.
    caught = []
    previous_hook = sys.excepthook
    sys.excepthook = lambda *exc: caught.append(exc)
    try:
        _flush_deferred_deletes()
        assert sip.isdeleted(browser)
        for _ in range(5):
            QCoreApplication.processEvents()          # in case the timer is still pending
    finally:
        sys.excepthook = previous_hook
    assert caught == [], f"deferred scroll raised on a deleted browser: {caught[0][1]!r}"


def test_deferred_highlight_scroll_still_scrolls_a_live_browser():
    browser = QTextBrowser()
    browser.setPlainText("first line\n" * 200 + "needle here")
    browser.resize(200, 60)
    browser.show()
    RD._scroll_to_first_highlight(object(), browser, "needle")
    for _ in range(5):
        QCoreApplication.processEvents()
    assert browser.textCursor().position() == browser.toPlainText().index("needle")
    browser.close()
