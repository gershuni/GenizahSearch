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
import io
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import genizah_app                            # noqa: E402

APP = genizah_app.GenizahGUI
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


class _FakeSip:
    @staticmethod
    def isdeleted(obj):
        return getattr(obj, 'deleted', False)


class _Host:
    _show_result_dialog = APP._show_result_dialog
    _on_result_dialog_finished = APP._on_result_dialog_finished

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


def test_host_initialises_the_reference_slot():
    src = _source(APP_PY)
    assert 'self._result_dialog = None' in src
