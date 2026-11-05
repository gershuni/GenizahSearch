# -*- coding: utf-8 -*-
"""The desktop What's New in every version dialog and the update notes (2026-11-05).

Pinned: the history lists the bundled releases; in update mode the changes come from
``ReleaseNotesThread`` (stubbed here: no network), a failure says so and offers the
GitHub release page, Update Now accepts; the update bar's "What's new?" asks for the
notes; the What's New dialog has "All versions".
"""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QApplication

from desktop import version_history as vh
from shared import whats_new_history as wnh

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _pin_english_labels(monkeypatch):
    """The labels asserted here are English; tr() follows the machine's saved language."""
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


class _FakeThread(QObject):
    """Stands in for ReleaseNotesThread: no network; the test emits the answer."""
    finished_signal = pyqtSignal(object)
    made = []

    def __init__(self, since, upto, parent=None):
        super().__init__(parent)
        self.since, self.upto = since, upto
        self.started = False
        _FakeThread.made.append(self)

    def start(self):
        self.started = True

    def isRunning(self):  # noqa: N802 - Qt API
        return False


@pytest.fixture
def fake_thread(monkeypatch):
    _FakeThread.made.clear()
    monkeypatch.setattr(vh, 'ReleaseNotesThread', _FakeThread)
    return _FakeThread


@pytest.mark.parametrize('w, h', [(640, 360), (1092, 580), (1920, 1040), (400, 250)])
def test_the_dialog_fits_the_screen(w, h):
    """Codex, PR #482: 1920x1080 at 300% leaves about 640x360; the dialog (and with it
    Update Now / Later) must fit inside the frame allowance, the minimum never above it."""
    from PyQt6.QtCore import QSize
    minimum, size = vh.fit_size(QSize(w, h))
    room = QSize(w, h) - vh.FRAME_ALLOWANCE
    assert size.width() <= room.width() and size.height() <= room.height()
    assert minimum.width() <= size.width() and minimum.height() <= size.height()
    assert size.width() <= vh.PREFERRED_SIZE.width() and size.height() <= vh.PREFERRED_SIZE.height()


def test_the_update_dialog_keeps_its_buttons_on_a_short_screen(fake_thread, monkeypatch):
    from PyQt6.QtCore import QRect

    class _Screen:
        def availableGeometry(self):
            return QRect(0, 0, 640, 360)
    monkeypatch.setattr(vh.QApplication, 'primaryScreen', staticmethod(lambda: _Screen()))
    dlg = vh.VersionHistoryDialog(None, current_version='9.13.1', update_to='9.14.0')
    assert dlg.minimumHeight() <= 360 - vh.FRAME_ALLOWANCE.height()
    assert dlg.height() <= 360 - vh.FRAME_ALLOWANCE.height()
    dlg.deleteLater()


def test_the_history_dialog_lists_every_bundled_release():
    dlg = vh.VersionHistoryDialog(None, releases=wnh.releases())
    text = dlg.browser.toPlainText()
    for release in wnh.releases():
        assert f"Version {release['version']}" in text
    assert not dlg.update_mode and not dlg.link_btn.isVisible()
    dlg.deleteLater()


def test_update_mode_asks_the_website_for_the_changes_and_shows_them(fake_thread):
    dlg = vh.VersionHistoryDialog(None, current_version='v9.13.1', update_to='v9.14.0',
                                  html_url='https://github.com/x/releases/tag/v9.14.0')
    assert dlg.update_mode
    [thread] = fake_thread.made
    assert (thread.since, thread.upto, thread.started) == ('9.13.1', '9.14.0', True)
    assert 'Loading' in dlg.browser.toPlainText()
    assert '9.13.1' in dlg.windowTitle() and 'v9.14.0' in dlg.subtitle_label.text()
    # What the website would send (its own version may be ahead of this copy's).
    newer = [r for r in wnh.HISTORY if r['version'] == '9.14.0']
    thread.finished_signal.emit(newer)
    assert 'Version 9.14.0' in dlg.browser.toPlainText()
    dlg.update_btn.click()
    assert dlg.result() == dlg.DialogCode.Accepted
    dlg.deleteLater()


@pytest.mark.parametrize('answer', [None, []])
def test_update_mode_says_when_the_changes_cannot_be_loaded(fake_thread, answer):
    dlg = vh.VersionHistoryDialog(None, current_version='9.13.1', update_to='9.14.0',
                                  html_url='https://github.com/x/releases/tag/v9.14.0')
    dlg.show()
    fake_thread.made[0].finished_signal.emit(answer)
    assert 'could not be loaded' in dlg.browser.toPlainText()
    assert dlg.link_btn.isVisible()
    dlg.reject()
    dlg.deleteLater()


def test_the_thread_turns_any_failure_into_none(monkeypatch):
    import requests

    def boom(*a, **k):
        raise requests.ConnectionError('offline')
    monkeypatch.setattr(requests, 'get', boom)
    got = []
    t = vh.ReleaseNotesThread('9.13.1', '9.14.0')
    t.finished_signal.connect(got.append)
    t.run()  # in this thread: no event loop needed
    assert got == [None]


def test_the_thread_reads_the_api_answer(monkeypatch):
    import requests

    class _Resp:
        status_code = 200

        def json(self):
            return {'latest': '9.14.0', 'releases': [wnh.to_public(r) for r in wnh.releases()[:1]]}
    seen = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.update(url=url, params=params, ua=headers.get('User-Agent'))
        return _Resp()
    monkeypatch.setattr(requests, 'get', fake_get)
    got = []
    t = vh.ReleaseNotesThread('9.13.1', '9.14.0')
    t.finished_signal.connect(got.append)
    t.run()
    assert seen['url'] == vh.WHATS_NEW_API_URL
    assert seen['params'] == {'since': '9.13.1', 'upto': '9.14.0', 'app': 'desktop'}
    assert seen['ua'].startswith('GenizahSearchPro/')
    assert [r['version'] for r in got[0]] == [wnh.releases()[0]['version']]


def test_the_update_bar_asks_for_the_notes():
    from desktop.update_ui import UpdateNotificationBar
    bar = UpdateNotificationBar()
    bar.show_update('v9.14.0', 'https://h', 'https://i')
    got = []
    bar.notes_requested.connect(lambda *a: got.append(a))
    bar.btn_notes.click()
    assert got == [('v9.14.0', 'https://h', 'https://i')]
    bar.deleteLater()


def test_a_manual_check_that_finds_a_version_shows_the_changes():
    """The call site: the manual check opens the notes (Update Now / Later), not a bare
    yes/no box; the automatic check still shows the bar."""
    from genizah_app import GenizahGUI

    class _Btn:
        def setEnabled(self, *_):
            pass

        def setText(self, *_):
            pass

    class _Bar:
        shown = []

        def show_update(self, *a):
            self.shown.append(a)

    calls = []
    fake = type('W', (), {})()
    fake.btn_check_updates = _Btn()
    fake.update_bar = _Bar()
    fake.show_update_notes = lambda *a: calls.append(a)
    GenizahGUI.on_update_result(fake, True, 'v9.14.0', 'https://h', 'https://i', True)
    assert calls == [('v9.14.0', 'https://h', 'https://i')]


def test_the_window_wires_the_menu_entry_and_the_bar():
    import inspect
    from genizah_app import GenizahGUI
    src = inspect.getsource(GenizahGUI)
    assert 'self.nav_history_btn.clicked.connect(self.show_version_history)' in src
    assert 'self.update_bar.notes_requested.connect(self.show_update_notes)' in src


def test_the_whats_new_dialog_offers_all_versions(monkeypatch):
    from desktop import update_ui
    opened = []
    monkeypatch.setattr(vh.VersionHistoryDialog, 'exec', lambda self: opened.append(self) or 0)
    dlg = update_ui.WhatsNewDialog(None)
    dlg.btn_all_versions.click()
    assert len(opened) == 1 and not opened[0].update_mode
    dlg.deleteLater()
