# -*- coding: utf-8 -*-
"""Quitting does not wait without limit for a metadata loader stuck in a request.

closeEvent used to call ``meta_loader.wait()`` with no timeout. The loader sees a
cancel only when its current NLI request returns, so a hung request held the quit
-- and the app.lock the process keeps -- and the app could not be opened again
until the process was killed. The wait is now bounded and logged, with no
terminate() (desktop/qthread_exit.py keeps a thread still running from being
destroyed at exit).
"""
from __future__ import annotations

import inspect
import threading
import time
from types import SimpleNamespace

from PyQt6.QtCore import QThread
from PyQt6.QtWidgets import QApplication

import genizah_app

_APP = QApplication.instance() or QApplication([])   # kept for the process


class _StuckLoader(QThread):
    """Ignores the cancel until released, as a loader inside a hung request does."""

    def __init__(self):
        super().__init__()
        self.release = threading.Event()
        self.cancel_requested = False
        self.terminated = False

    def request_cancel(self):
        self.cancel_requested = True

    def terminate(self):            # must never be called
        self.terminated = True
        super().terminate()

    def run(self):
        self.release.wait(30)


def test_a_stuck_loader_is_waited_for_a_bounded_time_and_left_running(monkeypatch):
    monkeypatch.setattr(genizah_app, '_META_LOADER_CLOSE_WAIT_MS', 200)
    loader = _StuckLoader()
    loader.start()
    try:
        assert loader.wait(50) is False          # it is really running
        started = time.monotonic()
        genizah_app.GenizahGUI._stop_meta_loader_for_close(SimpleNamespace(meta_loader=loader))
        took = time.monotonic() - started
        assert loader.cancel_requested
        assert took < 2.0, f"close waited {took:.1f}s"
        assert loader.isRunning()
        assert not loader.terminated
    finally:
        loader.release.set()
        assert loader.wait(5000)


def test_a_loader_that_stops_is_waited_for():
    loader = _StuckLoader()
    loader.start()
    loader.release.set()
    genizah_app.GenizahGUI._stop_meta_loader_for_close(SimpleNamespace(meta_loader=loader))
    assert not loader.isRunning()
    assert not loader.terminated


def test_no_loader_is_a_no_op():
    genizah_app.GenizahGUI._stop_meta_loader_for_close(SimpleNamespace(meta_loader=None))
    genizah_app.GenizahGUI._stop_meta_loader_for_close(SimpleNamespace())


def test_close_event_uses_the_bounded_stop():
    source = inspect.getsource(genizah_app.GenizahGUI.closeEvent)
    assert 'self._stop_meta_loader_for_close()' in source
    assert 'meta_loader.wait()' not in source
