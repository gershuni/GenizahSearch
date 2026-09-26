# -*- coding: utf-8 -*-
"""The update checker and the data download: a running check is never dropped, and
closing the app silences them without waiting.

Real GenizahGUI methods (check_updates_auto / check_updates_manual /
_download_next_sidecar / closeEvent) on a host that skips GenizahGUI.__init__, with the
threads' run() bodies replaced by a gate. Every thread a test creates is also held in
KEEP until it has finished, so a thread the code under test drops makes an assertion
fail instead of aborting pytest (0xC0000409).
"""
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.gui  # real QThreads + real closeEvent: gui bucket only

from PyQt6.QtCore import QCoreApplication  # noqa: E402
from PyQt6.QtGui import QCloseEvent  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QMainWindow, QMessageBox, QPushButton  # noqa: E402

APP = QApplication.instance() or QApplication(sys.argv)

import desktop.gui_threads as gt  # noqa: E402
import genizah_app as ga  # noqa: E402

KEEP = []            # every thread a test created, held until it has finished
GATE = threading.Event()
_LEFTOVER = []       # threads still running after the teardown wait (never cleared)
_ORIG_CHECK = ga.UpdateCheckerThread
_ORIG_SIDECAR = ga.SidecarUpdateThread
_ORIG_DOWNLOAD = ga.SidecarDownloadThread


def call_catching(fn, *args):
    """Run fn; return the Exception it raised, or None (a BaseException still escapes)."""
    try:
        fn(*args)
    except Exception as exc:
        return exc
    return None


def _alive(t):
    try:
        return t.isRunning()
    except RuntimeError:
        return False


def _kept(t):
    return any(x is t for x in gt._ORPHANED_WORKERS)


class _Box:
    """genizah_app's QMessageBox: never shows anything, records what it was asked."""
    StandardButton = QMessageBox.StandardButton
    shown = []

    @classmethod
    def information(cls, *a, **k):
        cls.shown.append(("information",) + a[1:])
        return QMessageBox.StandardButton.Ok

    @classmethod
    def warning(cls, *a, **k):
        cls.shown.append(("warning",) + a[1:])
        return QMessageBox.StandardButton.Ok

    @classmethod
    def question(cls, *a, **k):
        cls.shown.append(("question",) + a[1:])
        return QMessageBox.StandardButton.No


class GatedCheck(_ORIG_CHECK):
    constructed = []

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        KEEP.append(self)
        GatedCheck.constructed.append(self)

    def run(self):
        GATE.wait(10)
        self.finished_signal.emit(False, "v0", "", "", self.is_manual)


class GatedSidecar(_ORIG_SIDECAR):
    constructed = []

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        KEEP.append(self)
        GatedSidecar.constructed.append(self)

    def run(self):
        GATE.wait(10)
        self.update_available.emit([{"name": "pgp.db"}])


class GatedDownload(_ORIG_DOWNLOAD):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        KEEP.append(self)

    def run(self):
        GATE.wait(10)


@pytest.fixture(autouse=True)
def _private_state(monkeypatch, tmp_path):
    """No test reads or writes the owner's data folder or lists file."""
    from shared.config import Config
    import shared.lists_manager as lm

    state = str(tmp_path / "state")
    root = Config.INDEX_DIR
    for name, val in list(vars(Config).items()):
        if isinstance(val, str) and val.startswith(root):
            monkeypatch.setattr(Config, name, state + val[len(root):])
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))


@pytest.fixture(autouse=True)
def fresh_orphans(monkeypatch, _private_state):
    """A private keeper list per test; at teardown the gate opens and this test's own
    threads are waited for (bounded)."""
    monkeypatch.setattr(gt, "_ORPHANED_WORKERS", [], raising=False)
    errors = []
    monkeypatch.setattr(sys, "excepthook", lambda *a: errors.append(a))
    monkeypatch.setattr(ga, "UpdateCheckerThread", GatedCheck)
    monkeypatch.setattr(ga, "SidecarUpdateThread", GatedSidecar)
    monkeypatch.setattr(ga, "SidecarDownloadThread", GatedDownload)
    monkeypatch.setattr(ga, "QMessageBox", _Box)
    GATE.clear()
    GatedCheck.constructed.clear()
    GatedSidecar.constructed.clear()
    _Box.shown.clear()
    yield
    GATE.set()
    end = time.monotonic() + 10
    while any(_alive(t) for t in KEEP) and time.monotonic() < end:
        QTest.qWait(10)
    for _ in range(5):
        QCoreApplication.processEvents()
    stuck = [t for t in KEEP if _alive(t)]
    _LEFTOVER.extend(stuck)
    KEEP.clear()
    assert not stuck, f"{len(stuck)} thread(s) still running after the teardown wait"
    assert not errors, errors


class Host(ga.GenizahGUI):
    """GenizahGUI without __init__: just what the update and close paths touch."""

    def __init__(self):
        QMainWindow.__init__(self)          # skip GenizahGUI.__init__ on purpose
        self.btn_check_updates = QPushButton()
        self._puzzle_window = None
        self.results = []
        self.calls = []

    def on_update_result(self, *a):
        self.results.append(a)
        ga.GenizahGUI.on_update_result(self, *a)

    def on_update_error(self, *a):
        self.results.append(("error",) + a)

    def _on_sidecar_updates(self, *a):
        self.results.append(("sidecar",) + a)

    def _defer_close_for_passage(self, event):
        return False

    def _close_result_dialog(self):
        self.calls.append("close_result_dialog")

    def _telemetry_ready(self):
        return False

    def _save_session(self):
        self.calls.append("save_session")

    def cancel_browse_image_thread(self):
        self.calls.append("cancel_browse_image_thread")


def _settle():
    GATE.set()
    end = time.monotonic() + 5
    while any(_alive(t) for t in KEEP) and time.monotonic() < end:
        QTest.qWait(10)
    for _ in range(10):
        QTest.qWait(10)


def test_second_manual_check_keeps_the_first():
    h = Host()
    h.check_updates_manual()
    first = h.update_thread
    h.check_updates_manual()                  # double-click on the corner version label
    assert _alive(first)
    assert h.update_thread is first, "the running manual check was replaced"
    assert len(GatedCheck.constructed) == 1
    _settle()
    assert [r for r in h.results if r[0] != "sidecar"] == [(False, "v0", "", "", True)]


def test_manual_check_during_the_startup_check_keeps_and_silences_it():
    h = Host()
    h.check_updates_auto()
    auto = h.update_thread
    h.check_updates_manual()
    assert _alive(auto) and _kept(auto), "the running startup check was dropped"
    _settle()
    assert [r[-1] for r in h.results if r[0] != "sidecar"] == [True]   # only the manual result


def test_startup_check_during_a_manual_check_keeps_the_manual_one():
    h = Host()
    h.check_updates_manual()
    manual = h.update_thread
    h.check_updates_auto()                    # startup finished while the manual check runs
    assert h.update_thread is manual, "the startup check replaced the running manual check"
    assert len(GatedCheck.constructed) == 1
    assert len(GatedSidecar.constructed) == 1, "the data-update check must still start"
    assert not h.btn_check_updates.isEnabled()
    _settle()
    assert [r for r in h.results if r[0] != "sidecar"] == [(False, "v0", "", "", True)]
    assert h.btn_check_updates.isEnabled()


def _host_with_running_update_threads(tmp_path):
    h = Host()
    h.check_updates_auto()
    delivered = []
    h._current_sidecar_download = GatedDownload(
        "https://github.com/gershuni/GenizahSearch/x", str(tmp_path / "x.db"), "x.db")
    h._current_sidecar_download.finished_signal.connect(lambda *a: delivered.append(a))
    h._current_sidecar_download.start()
    threads = (h.update_thread, h.sidecar_update_thread, h._current_sidecar_download)
    return h, threads, delivered


def test_close_silences_running_update_checks(tmp_path):
    h, threads, delivered = _host_with_running_update_threads(tmp_path)
    escaped = call_catching(h.closeEvent, QCloseEvent())
    assert escaped is None, f"closeEvent raised {escaped!r}"
    for t in threads:
        assert _alive(t) and _kept(t), f"{type(t).__name__} was not kept"
    assert h._current_sidecar_download._cancelled
    _settle()
    assert h.results == [] and delivered == [], "a result reached the closing app"


def test_close_silences_update_checks_even_when_closing_the_viewer_fails(tmp_path):
    h, threads, delivered = _host_with_running_update_threads(tmp_path)

    def failing_close_result_dialog():
        raise ValueError("viewer close failed")

    h._close_result_dialog = failing_close_result_dialog
    escaped = call_catching(h.closeEvent, QCloseEvent())
    assert escaped is None, f"closeEvent raised {escaped!r}"
    for t in threads:
        assert _alive(t) and _kept(t), f"{type(t).__name__} was not kept"
    assert "save_session" in h.calls
    _settle()
    assert h.results == [] and delivered == []


def test_close_still_stops_the_workers_when_saving_the_session_fails(tmp_path):
    h, _threads, _delivered = _host_with_running_update_threads(tmp_path)
    stopped = []

    class RunningMetaLoader:
        def isRunning(self):
            return True

        def request_cancel(self):
            stopped.append("request_cancel")

        def wait(self, ms=None):
            return True

    def failing_save_session():
        raise ValueError("session save failed")

    h.meta_loader = RunningMetaLoader()
    h._save_session = failing_save_session
    event = QCloseEvent()
    event.ignore()
    escaped = call_catching(h.closeEvent, event)
    assert escaped is None, f"closeEvent raised {escaped!r}"
    assert stopped == ["request_cancel"], "the worker stops after the session save were skipped"
    assert event.isAccepted(), "QMainWindow.closeEvent was never reached"


def test_next_sidecar_download_keeps_the_previous_thread(tmp_path):
    h = Host()
    h._sidecar_data_dir = str(tmp_path)
    h._sidecar_download_queue = [
        {"url": "https://github.com/gershuni/GenizahSearch/a", "subdir": "data", "name": "a.db"},
        {"url": "https://github.com/gershuni/GenizahSearch/b", "subdir": "data", "name": "b.db"},
    ]
    h._download_next_sidecar()
    first = h._current_sidecar_download
    h._download_next_sidecar()                # as first's finished slot does, while it still runs
    assert h._current_sidecar_download is not first
    assert _alive(first) and _kept(first), "the running download was dropped"
