# -*- coding: utf-8 -*-
"""ResultDialog is an INDEPENDENT, non-modal top-level window (2026-09-17).

Debug session resultdialog-minimize-app. Report: "minimizing ResultDialog
minimizes the app itself." Measured on the live desktop (PyQt6 6.10, nine
parent/modality combinations, WM_SYSCOMMAND/SC_MINIMIZE sent to the dialog):
the main window NEVER goes iconic. What happened instead: the dialog was an
application-modal (.exec()) Win32 OWNED window of the main window, so
minimizing it hid it with no taskbar button of its own, the disabled main
window could not take focus, and Windows activated the next application over
it -- the whole app appeared to minimize. A user wanted to see the search
panel while reading results, so the viewer became a window of its own.

The contract pinned here:
  1. ResultDialog.__init__ passes NO Qt parent to QDialog (the host is kept
     only as `self._app`) and is explicitly non-modal.
  2. Every construction site in genizah_app.py routes through ONE helper,
     GenizahGUI._show_result_dialog, which shows (never exec()s) the dialog,
     holds the reference that keeps an unparented dialog alive, closes the
     previously open viewer first, and releases the reference on `finished`.

Pattern: the unbound host methods are bound to a stub, with ResultDialog
swapped for a recording fake -- no QApplication, no event loop, no segfault
risk (same approach as tests/test_desktop_passage_gate.py).
"""
from __future__ import annotations

import ast
import inspect
import io
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import genizah_app                            # noqa: E402
import desktop.result_dialog as result_dialog  # noqa: E402

APP = genizah_app.GenizahGUI
RD = result_dialog.ResultDialog
RD_PY = os.path.join(ROOT, 'desktop', 'result_dialog.py')
APP_PY = os.path.join(ROOT, 'genizah_app.py')


def _source(path):
    return io.open(path, encoding='utf-8').read()


def _init_source():
    src = _source(RD_PY)
    tree = ast.parse(src)
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == 'ResultDialog')
    fn = next(n for n in cls.body
              if isinstance(n, ast.FunctionDef) and n.name == '__init__')
    lines = src.splitlines()
    return '\n'.join(lines[fn.lineno - 1:fn.end_lineno])


# ---------------------------------------------------------------------------
# 1. The dialog itself: no Qt parent, non-modal.
# ---------------------------------------------------------------------------

def test_init_passes_no_qt_parent_and_keeps_the_host_as_app():
    src = _init_source()
    assert 'super().__init__(None)' in src, (
        "ResultDialog must be an unparented top-level window; a Qt parent "
        "makes it a Win32 owned window with no taskbar button of its own")
    assert 'super().__init__(parent)' not in src
    assert 'self._app = parent' in src
    assert 'self.setModal(False)' in src


def test_init_adds_min_max_buttons_without_changing_the_window_type():
    src = _init_source()
    assert 'Qt.WindowType.WindowMinMaxButtonsHint' in src
    assert 'self.windowFlags()' in src, "OR onto the defaults, never replace"
    # Qt.Window shown modally is the documented cause of the same symptom in
    # other apps; the type stays Qt::Dialog.
    assert not re.search(r'Qt\.WindowType\.Window\b', src)


# ---------------------------------------------------------------------------
# 2. One construction site, one helper, no exec().
# ---------------------------------------------------------------------------

def test_every_construction_site_routes_through_the_helper():
    src = _source(APP_PY)
    tree = ast.parse(src)
    sites = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for call in ast.walk(node):
                if (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Name)
                        and call.func.id == 'ResultDialog'):
                    sites.append(node.name)
    assert sites == ['_show_result_dialog'], sites
    assert not re.search(r'ResultDialog\([^)]*\)\s*\.exec\(\)', src)
    assert src.count('self._show_result_dialog(') >= 6, (
        "the six former exec() sites must all call the helper")


# ---------------------------------------------------------------------------
# 3. The helper's behaviour, on a stub host with a recording fake dialog.
# ---------------------------------------------------------------------------

class _Signal:
    def __init__(self):
        self.slots = []

    def connect(self, fn):
        self.slots.append(fn)

    def emit(self, *args):
        for fn in list(self.slots):
            fn(*args)


class _Rect:
    def __init__(self, x, y, w, h):
        self._c = (x + w // 2, y + h // 2)

    def center(self):
        return _Point(*self._c)


class _Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __sub__(self, other):
        return _Point(self.x - other.x, self.y - other.y)


class _FakeDialog:
    made = []

    def __init__(self, host, results, index, meta_mgr, searcher):
        self.host, self.results, self.index = host, results, index
        self.meta_mgr, self.searcher = meta_mgr, searcher
        self.finished = _Signal()
        self.shown = self.raised = self.activated = False
        self.closed = self.deleted = False
        self.execd = False
        self.moved_to = None
        _FakeDialog.made.append(self)

    def close(self):
        self.closed = True
        self.finished.emit(0)   # QDialog::closeEvent -> reject() -> finished
        return True

    def show(self):
        self.shown = True

    def raise_(self):
        self.raised = True

    def activateWindow(self):
        self.activated = True

    def rect(self):
        return _Rect(0, 0, 1300, 850)

    def move(self, pos):
        self.moved_to = (pos.x, pos.y)

    def deleteLater(self):
        self.deleted = True

    def exec(self):
        self.execd = True
        return 0


class _FakeSip:
    @staticmethod
    def isdeleted(obj):
        return getattr(obj, 'deleted', False)


class _FakeQApp:
    active_modal = None

    @classmethod
    def activeModalWidget(cls):
        return cls.active_modal


class _Host:
    _show_result_dialog = APP._show_result_dialog
    _on_result_dialog_finished = APP._on_result_dialog_finished
    _close_result_dialog = APP._close_result_dialog

    def __init__(self):
        self.meta_mgr = object()
        self.searcher = object()
        self._result_dialog = None

    def frameGeometry(self):
        return _Rect(100, 100, 1000, 800)


@pytest.fixture
def host(monkeypatch):
    _FakeDialog.made = []
    monkeypatch.setattr(genizah_app, 'ResultDialog', _FakeDialog)
    monkeypatch.setattr(genizah_app, 'sip', _FakeSip)
    _FakeQApp.active_modal = None
    monkeypatch.setattr(genizah_app, 'QApplication', _FakeQApp)
    return _Host()


def test_helper_shows_the_dialog_and_holds_the_reference(host):
    dlg = host._show_result_dialog(['r1', 'r2'], 1)
    assert host._result_dialog is dlg
    assert dlg.host is host and dlg.results == ['r1', 'r2'] and dlg.index == 1
    assert dlg.meta_mgr is host.meta_mgr and dlg.searcher is host.searcher
    assert dlg.shown and dlg.raised and dlg.activated
    # centred over the host: host centre (600, 500) minus dialog centre (650, 425)
    assert dlg.moved_to == (-50, 75)


def test_opening_a_second_result_closes_the_first(host):
    first = host._show_result_dialog(['a'], 0)
    second = host._show_result_dialog(['b'], 0)
    assert first.closed and first.deleted
    assert not second.closed
    assert host._result_dialog is second


def test_finished_releases_the_reference_and_schedules_deletion(host):
    dlg = host._show_result_dialog(['a'], 0)
    dlg.finished.emit(0)
    assert host._result_dialog is None
    assert dlg.deleted


def test_a_stale_finished_does_not_drop_the_current_viewer(host):
    first = host._show_result_dialog(['a'], 0)
    second = host._show_result_dialog(['b'], 0)
    first.finished.emit(0)          # late/second signal from the old one
    assert host._result_dialog is second


def test_shutdown_closes_the_open_viewer(host):
    """Codex (PR #343): an unparented viewer does not close with the main
    window; left open it keeps the process alive and points at a torn-down
    host."""
    dlg = host._show_result_dialog(['a'], 0)
    host._close_result_dialog()
    assert dlg.closed and dlg.deleted
    assert host._result_dialog is None
    host._close_result_dialog()          # nothing open: no error


def test_main_window_close_event_closes_the_viewer_before_shutdown_state():
    src = inspect.getsource(APP.closeEvent)
    assert 'self._close_result_dialog()' in src
    assert (src.index('self._close_result_dialog()')
            < src.index('self._app_shutting_down = True'))
    # ...but only once the passage-index deferral has let the close proceed.
    assert (src.index('_defer_close_for_passage(event)')
            < src.index('self._close_result_dialog()'))


def test_opened_from_inside_a_modal_dialog_the_viewer_runs_modally(host):
    """Codex (PR #343): the corrections / discoveries / my-comments dialogs
    call on_view_result from inside their own exec(). A non-modal viewer
    would be blocked behind that loop, so there it runs modally, as the
    old nested exec() did, and control returns to the source dialog."""
    _FakeQApp.active_modal = object()
    dlg = host._show_result_dialog(['a'], 0)
    assert dlg.execd and not dlg.shown
    assert dlg.moved_to is not None       # still centred over the host
    # nested viewers never take the free-standing slot
    assert host._result_dialog is None


def test_nesting_leaves_the_open_viewer_alone(host):
    """Codex (PR #343, round 3): ResultDialog.view_corrections parents its
    corrections dialog to the viewer. Closing that viewer from inside the
    corrections dialog's own callback would delete the caller mid-stack, so
    from a modal source the open viewer is kept and the new one nests."""
    first = host._show_result_dialog(['a'], 0)
    _FakeQApp.active_modal = object()     # e.g. first's corrections dialog
    nested = host._show_result_dialog(['b'], 0)
    assert not first.closed and not first.deleted
    assert host._result_dialog is first
    assert nested.execd and not nested.shown
    nested.finished.emit(0)                # closing the nested one...
    assert nested.deleted
    assert host._result_dialog is first    # ...never disturbs the first


def test_opened_from_the_main_window_the_viewer_is_free_standing(host):
    dlg = host._show_result_dialog(['a'], 0)
    assert dlg.shown and not dlg.execd


# ---------------------------------------------------------------------------
# 4. Worker teardown on EVERY termination path (Codex CLI review, PR #343).
# ---------------------------------------------------------------------------

class _Worker:
    def __init__(self, finishes=True):
        self.running = True
        self.interrupted = self.terminated = False
        self.finishes = finishes

    def isRunning(self):
        return self.running

    def requestInterruption(self):
        self.interrupted = True

    def wait(self, _ms=None):
        if self.finishes or self.terminated:
            self.running = False
            return True
        return False

    def terminate(self):
        self.terminated = True


class _MsViewer:
    def __init__(self):
        self.stopped = 0

    def stop_threads(self):
        self.stopped += 1


class _Viewer:
    _teardown_workers = RD._teardown_workers
    _on_dialog_finished_teardown = RD._on_dialog_finished_teardown

    def __init__(self):
        self.enrich_worker = _Worker()
        self._rd_pgp_worker = _Worker(finishes=False)
        self.preload_meta_worker = None
        self.ms_viewer = _MsViewer()
        self.images_cancelled = 0
        self._pdf_scope = 1

    def cancel_image_thread(self):
        self.images_cancelled += 1

    def _pdf_controller(self):
        return None


def test_teardown_stops_every_worker_once_and_is_idempotent():
    v = _Viewer()
    v._on_dialog_finished_teardown(0)         # the Esc / reject / accept / done path
    assert v.enrich_worker.interrupted and not v.enrich_worker.running
    assert v._rd_pgp_worker.interrupted and v._rd_pgp_worker.terminated
    assert v.images_cancelled == 1 and v.ms_viewer.stopped == 1
    v._teardown_workers()                     # closeEvent afterwards: a no-op
    assert v.images_cancelled == 1 and v.ms_viewer.stopped == 1


def test_teardown_is_wired_to_finished_and_to_close_event():
    init = _init_source()
    assert 'self.finished.connect(self._on_dialog_finished_teardown)' in init
    # the PDF-scope handler was connected first, so it still runs first
    assert (init.index('self._on_pdf_dialog_finished')
            < init.index('self._on_dialog_finished_teardown'))
    close_src = inspect.getsource(RD.closeEvent)
    assert 'self._teardown_workers()' in close_src
    assert 'requestInterruption' not in close_src   # one implementation, not two


def test_deferred_highlight_scroll_checks_the_browser_still_exists():
    src = inspect.getsource(RD._scroll_to_first_highlight)
    assert 'sip.isdeleted(text_browser)' in src
    assert (src.index('sip.isdeleted(text_browser)')
            < src.index('plain = text_browser.toPlainText()'))


def test_view_corrections_parents_its_dialog_to_the_viewer():
    """The nesting rule in _show_result_dialog exists because of this parentage;
    if it changes, that rule needs re-examining."""
    src = inspect.getsource(RD.view_corrections)
    assert re.search(r'CorrectionsViewerDialog\(\s*self,', src)


def test_every_on_view_result_source_dialog_runs_modally():
    """_show_result_dialog decides nesting by QApplication.activeModalWidget();
    that only works while these dialogs are shown with exec()."""
    src = _source(APP_PY)
    sites = [m.end() for m in re.finditer(
        r'on_view_result=lambda s: self\._open_document_result_dialog\(shelfmark=s\)', src)]
    assert len(sites) >= 4, len(sites)
    for end in sites:
        assert 'dialog.exec()' in src[end:end + 200], src[end:end + 200]


def test_host_initialises_the_reference_slot():
    src = _source(APP_PY)
    assert 'self._result_dialog = None' in src
