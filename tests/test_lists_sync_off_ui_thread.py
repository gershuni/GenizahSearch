# -*- coding: utf-8 -*-
"""The desktop window's list syncs run on the list-sync runner, off the UI thread.

Every test drives the window's real entry points -- the sync dialog's buttons
(_do_sync_action) and where their jobs end (_on_lists_sync_done), the sign-in
preview (_enable_lists_cloud_sync / _on_lists_preview), the automatic upload after a
list change (_lists_auto_sync and every list mutator), the automatic upload's end
(_on_lists_auto_done) -- on a GenizahGUI that has only its QMainWindow half built.
The runner is a recording one that follows ListsSyncRunner's contract as the window
sees it: run() returns a job, and the test ends that job when it chooses, the way the
runner delivers a result on the UI thread. QProgressDialog and the message box are
faked; the QTimers the window holds a question with are real, so those tests pump the
event loop. Personal state (lists.pkl, the index folder) is under tmp_path.

GUI-marked (tests/conftest.py): run it on its own, with QT_QPA_PLATFORM=offscreen.
"""
import ast
import json
import sys
import threading
import time
import types
from pathlib import Path

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QDialog, QLabel, QMainWindow, QPushButton

import genizah_app
import genizah_core
from genizah_core import Config
from shared import lists_manager as lm
from shared import lists_sync
from shared.genizah_translations import TRANSLATIONS

from lists_sync_contract import add_missing_manager_reads, runner_module

APP = QApplication.instance() or QApplication([])
ROOT = Path(__file__).resolve().parents[1]
USER = "u1"


def tr(text):
    return genizah_core.tr(text)


def _is_hebrew(ch):
    return "֐" <= ch <= "׿"


def _first_letter(text):
    for ch in text:
        if ch.isalpha():
            return ch
    return ""


@pytest.fixture(autouse=True)
def no_exception_in_a_qt_callback(monkeypatch):
    """An exception inside a Qt callback reaches sys.excepthook, not the test."""
    raised = []
    monkeypatch.setattr(sys, "excepthook", lambda *exc: raised.append(exc))
    yield
    assert raised == [], [str(e[1]) for e in raised]


@pytest.fixture(autouse=True)
def personal_state(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "INDEX_DIR", str(tmp_path))
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))
    monkeypatch.setattr(lists_sync, "SUPABASE_AVAILABLE", True)
    monkeypatch.setattr(lists_sync, "SUPABASE_ANON_KEY", "test-key")
    monkeypatch.setattr(lists_sync, "_sync_instance", None)
    runner_module(monkeypatch)
    add_missing_manager_reads(monkeypatch)
    return tmp_path


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    return request.param


# ---------------------------------------------------------------------------
# The runner, the progress dialog and the window
# ---------------------------------------------------------------------------

class RecordingRunner:
    """ListsSyncRunner as the window sees it; each job ends when the test calls finish()."""

    def __init__(self):
        self.calls = []
        self.jobs = []
        self.auth_epoch = 0
        self.unsent = False
        self.busy = False
        self.closed = False

    def names(self):
        return [c[0] if c[0] != "run" else f"run:{c[1]}" for c in self.calls]

    def run(self, kind, on_done=None, on_progress=None):
        self.calls.append(("run", kind))
        if self.closed:
            return None
        job = types.SimpleNamespace(kind=kind, on_done=on_done, on_progress=on_progress,
                                    epoch=self.auth_epoch, deadline=None, done=False)
        self.jobs.append(job)
        return job

    def finish(self, job, outcome):
        assert not job.done, "a job ended twice"
        job.done = True
        job.on_done(outcome)

    def last(self, kind):
        return [j for j in self.jobs if j.kind == kind][-1]

    def cancel(self, job):
        self.calls.append(("cancel", job))

    def request_auto(self):
        self.calls.append(("request_auto",))

    def allow_auto(self):
        self.calls.append(("allow_auto",))

    def mark_dirty(self):
        self.calls.append(("mark_dirty",))
        self.unsent = True

    def invalidate_auth(self):
        self.calls.append(("invalidate_auth",))
        self.auth_epoch += 1

    def begin_logout(self, on_done, budget_s):
        self.calls.append(("begin_logout", budget_s))
        if self.closed:
            return None
        job = types.SimpleNamespace(kind="logout", on_done=on_done, on_progress=None,
                                    epoch=self.auth_epoch, deadline=time.monotonic() + budget_s,
                                    done=False)
        self.jobs.append(job)
        return job

    def shutdown(self):
        self.calls.append(("shutdown",))
        self.closed = True


class _Signal:
    def __init__(self):
        self.slots = []

    def connect(self, slot):
        self.slots.append(slot)

    def disconnect(self, *a):
        if not self.slots:
            raise TypeError("disconnect() failed between 'canceled' and all its connections")
        self.slots.clear()

    def emit(self):
        for slot in list(self.slots):
            slot()


class FakeProgress:
    """QProgressDialog; like Qt's, closing it emits canceled."""
    made = []

    def __init__(self, label, cancel_text, minimum, maximum, parent=None):
        self.label, self.cancel_text = label, cancel_text
        self.canceled = _Signal()
        self.labels = [label]
        self.closed = self.shown = False
        self.modality = self.min_duration = None
        FakeProgress.made.append(self)

    def setWindowTitle(self, title):
        self.title = title

    def setWindowModality(self, modality):
        self.modality = modality

    def setMinimumDuration(self, ms):
        self.min_duration = ms

    def show(self):
        self.shown = True

    def setLabelText(self, text):
        self.labels.append(text)

    def reset(self):
        pass

    def close(self):
        self.closed = True
        self.canceled.emit()

    def deleteLater(self):
        pass


class Host(genizah_app.GenizahGUI):
    def __init__(self):  # the real window without its UI: only QMainWindow's own
        QMainWindow.__init__(self)


class FakeAccount:
    """The corrections client as the window uses it at sign-in and sign-out."""

    def __init__(self):
        self.sign_in()
        self._client = object()
        self.logouts = []

    def sign_in(self):
        self.current_user = types.SimpleNamespace(_uuid=USER, username="reader")
        self.signed_in = True

    def logout(self, revoke="wait"):
        self.logouts.append((revoke, threading.current_thread()))
        self.current_user = None
        self.signed_in = False

    def is_logged_in(self):
        return self.signed_in


@pytest.fixture
def gui(monkeypatch):
    notices = []
    FakeProgress.made = []
    monkeypatch.setattr(genizah_app, "QProgressDialog", FakeProgress)
    monkeypatch.setattr(genizah_app, "_show_ok_notice",
                        lambda parent, kind, title, text: notices.append((kind, title, text)))
    monkeypatch.setattr(genizah_app, "QMessageBox", types.SimpleNamespace(   # never a real modal box
        information=lambda parent, title, text, *a: notices.append(("information", title, text)),
        warning=lambda parent, title, text, *a: notices.append(("warning", title, text)),
        critical=lambda parent, title, text, *a: notices.append(("critical", title, text))))
    screen = {"visible": True, "modal": None}
    monkeypatch.setattr(genizah_app, "QApplication", types.SimpleNamespace(
        activeModalWidget=lambda: screen["modal"], instance=QApplication.instance,
        processEvents=QApplication.processEvents))
    from desktop import telemetry
    monkeypatch.setattr(telemetry, "reset_identity", lambda *a, **k: None)
    hosts = []

    def make(list_names=("Local list",)):
        host = Host()
        host.isVisible = lambda: screen["visible"]
        host._restoring_session = False
        host.LISTS_DIALOG_RECHECK_MS = 50
        host.lists_mgr = lm.ListsManager(None)
        host.list_ids = [host.lists_mgr.create_list(n) for n in list_names]
        host.lists_mgr.add_item("990000001", host.list_ids[0], note="a note made on this computer")
        host.refreshed = []
        host.lists_tree = None
        host.lists_refresh_all = lambda: host.refreshed.append(True)
        host.lists_refresh_sidebar = lambda: None
        host.lists_refresh_items = lambda: None
        host.corrections_client = FakeAccount()
        host.corner_login_btn = QPushButton()
        host.panel_refreshes = []
        host._refresh_community_panels = lambda **kw: host.panel_refreshes.append(kw)
        host._lists_sync = RecordingRunner()
        host.notices = notices
        host.screen = screen
        host.dialogs = []

        def show_dialog(*args):
            host.dialogs.append(args)
            return host.dialog_result
        host.dialog_result = 0            # Skip
        host._show_lists_sync_dialog = show_dialog
        hosts.append(host)
        return host

    yield make
    # No timer of one test may fire in the next: no event loop runs here, so
    # deleteLater never deletes a host, and its timers would live on.
    for host in hosts:
        for stop in ("_drop_held_lists_dialog", "_stop_web_removal_offer"):
            getattr(host, stop, lambda: None)()
        timer = getattr(host, "_logout_timer", None)
        if timer is not None:
            timer.stop()
        host.deleteLater()


def pump(seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        APP.processEvents()
        time.sleep(0.01)


def status(host):
    return host.statusBar().currentMessage()


DIALOG = types.SimpleNamespace(accept=lambda: None)


def _no_direct_sync(monkeypatch, host):
    """The window never runs a pass itself: every one goes through the runner."""
    direct = []
    for name in ("sync_from_cloud", "sync_to_cloud", "get_cloud_lists_preview"):
        monkeypatch.setattr(host.lists_mgr, name,
                            lambda *a, _n=name, **k: direct.append(_n) or {"success": False})
    return direct


def _signed_in(host):
    host.lists_mgr.enable_cloud_sync(USER, supabase_client=host.corrections_client._client)
    host._lists_sync_user_id = USER


# ---------------------------------------------------------------------------
# The sync dialog's buttons
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("action", ["download", "upload", "merge"])
def test_a_sync_dialog_action_runs_on_the_runner_behind_a_cancellable_progress(gui, monkeypatch, action):
    host = gui()
    direct = _no_direct_sync(monkeypatch, host)
    runner = host._lists_sync

    host._do_sync_action(DIALOG, action)

    assert direct == [], f"the window ran {direct} itself, on the UI thread"
    assert runner.names() == [f"run:{action}"]
    progress = FakeProgress.made[-1]
    assert progress.shown and progress.cancel_text == tr("Cancel")
    assert progress.modality == Qt.WindowModality.WindowModal
    assert progress.label == tr("Syncing lists...")
    job = runner.jobs[-1]
    progress.canceled.emit()                      # the user presses Cancel
    assert runner.calls[-1] == ("cancel", job)

    runner.finish(job, {"cancelled": True})
    assert progress.closed
    assert [c for c in runner.calls if c[0] == "cancel"] == [("cancel", job)], \
        "closing the finished job's dialog cancelled it again"


def test_a_sync_asked_for_while_another_runs_says_it_waits(gui):
    host = gui()
    host._lists_sync.busy = True
    host._do_sync_action(DIALOG, "merge")
    assert FakeProgress.made[-1].label == tr("Waiting for the list sync that is already running...")


def test_the_progress_dialog_counts_the_lists(gui, lang):
    host = gui()
    host._do_sync_action(DIALOG, "merge")
    job = host._lists_sync.jobs[-1]
    job.on_progress("download", 0, 3)
    job.on_progress("upload", 2, 3)
    assert FakeProgress.made[-1].labels[1:] == [tr("Downloading list {} of {}...").format(1, 3),
                                               tr("Uploading list {} of {}...").format(3, 3)]
    if lang == "he":
        assert all(_is_hebrew(_first_letter(t)) for t in FakeProgress.made[-1].labels)


CANCELLED = tr("List sync cancelled. Nothing was changed on this computer.")
UPLOAD_STOPPED = ("Upload stopped. The rest of your changes are saved on this computer but have "
                  "not reached your account yet.")
MERGE_STOPPED = ("The cloud lists were downloaded, but the upload was stopped. The rest of your "
                 "changes are saved on this computer but have not reached your account yet.")
STOPPED_UPLOAD = {"success": False, "stopped": True, "error": "Sync stopped"}


@pytest.mark.parametrize("action,outcome,expected", [
    ("download", {"cancelled": True}, "List sync cancelled. Nothing was changed on this computer."),
    ("download", {"cancelled": True, "download": {"success": False, "stopped": True}},
     "List sync cancelled. Nothing was changed on this computer."),
    ("merge", {"cancelled": True}, "List sync cancelled. Nothing was changed on this computer."),
    ("merge", {"cancelled": True, "download": {"success": True}, "upload": STOPPED_UPLOAD}, MERGE_STOPPED),
    ("upload", {"cancelled": True, "upload": STOPPED_UPLOAD}, UPLOAD_STOPPED),
    ("upload", {"cancelled": True}, UPLOAD_STOPPED),
], ids=["download-queued", "download-running", "merge-download-half", "merge-upload-half",
        "upload-running", "upload-queued"])
def test_a_cancelled_sync_says_on_the_status_bar_what_happened(gui, lang, action, outcome, expected):
    host = gui()
    host._do_sync_action(DIALOG, action)
    host._lists_sync.finish(host._lists_sync.jobs[-1], outcome)
    assert status(host) == tr(expected)
    assert host.notices == [], "a cancelled sync opened a message box"
    assert FakeProgress.made[-1].closed
    if lang == "he":
        assert _is_hebrew(_first_letter(status(host)))
    # the download half of a Merge did change the lists: they are shown again
    assert bool(host.refreshed) == bool((outcome.get("download") or {}).get("success"))


def test_nothing_is_shown_for_a_job_ended_by_the_window_closing(gui):
    host = gui()
    host._do_sync_action(DIALOG, "merge")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"cancelled": True, "shutdown": True})
    assert host.notices == [] and status(host) == ""
    assert FakeProgress.made[-1].closed


def test_a_finished_action_reports_through_the_ok_notice(gui, lang):
    host = gui()
    host._do_sync_action(DIALOG, "upload")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"upload": {
        "success": True, "lists_pushed": 2, "items_pushed": 5}})
    assert host.notices == [("information", tr("Sync Complete"),
                             tr("Uploaded {lists} lists and {items} items to cloud.").format(lists=2, items=5))]
    assert host.refreshed == [True]


# ---------------------------------------------------------------------------
# While lists.pkl cannot be saved, no sync text says a change is saved here
# ---------------------------------------------------------------------------

def _saves_failing(monkeypatch, host):
    monkeypatch.setattr(host.lists_mgr, "saves_failing", lambda: True)
    return host._lists_not_saved_line()


@pytest.mark.parametrize("action,outcome", [
    ("upload", {"cancelled": True, "upload": STOPPED_UPLOAD}),
    ("merge", {"cancelled": True, "download": {"success": True}, "upload": STOPPED_UPLOAD}),
], ids=["upload-stopped", "merge-upload-stopped"])
def test_no_sync_text_says_saved_while_lists_cannot_be_saved_stopped(gui, monkeypatch, lang, action, outcome):
    host = gui()
    line = _saves_failing(monkeypatch, host)
    host._do_sync_action(DIALOG, action)
    host._lists_sync.finish(host._lists_sync.jobs[-1], outcome)
    assert status(host) == line
    assert tr(UPLOAD_STOPPED) != line and tr(MERGE_STOPPED) != line
    if lang == "he":
        assert _is_hebrew(_first_letter(line))


TOO_LONG_KEPT = ("Notes too long to update safely in your account: {}. They were not changed "
                 "there and are kept on this computer.")
TOO_LONG = "Notes too long to update safely in your account: {}. They were not changed there."


def test_no_sync_text_says_saved_while_lists_cannot_be_saved_manual_result_too_long(gui, monkeypatch, lang):
    host = gui()
    line = _saves_failing(monkeypatch, host)
    host._do_sync_action(DIALOG, "upload")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"upload": {
        "success": True, "lists_pushed": 1, "items_pushed": 1, "notes_too_long": 2}})
    (kind, title, text), = host.notices
    paragraphs = text.split("\n\n")
    assert tr(TOO_LONG).format(2) in paragraphs
    assert tr(TOO_LONG_KEPT).format(2) not in text
    assert paragraphs[-1] == line


def test_no_sync_text_says_saved_while_lists_cannot_be_saved_manual_download_merged(gui, monkeypatch, lang):
    host = gui()
    line = _saves_failing(monkeypatch, host)
    host._do_sync_action(DIALOG, "download")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"download": {
        "success": True, "lists_added": 0, "items_added": 0, "notes_merged": 1}})
    (kind, title, text), = host.notices
    assert text.split("\n\n")[-1] == line
    assert len(text.split("\n\n")) == 3   # the result, the kept-both line, then the line


def test_no_sync_text_says_saved_while_lists_cannot_be_saved_auto_too_long(gui, monkeypatch, lang):
    host = gui()
    _saves_failing(monkeypatch, host)
    host._on_lists_auto_done({"upload": {"success": True, "notes_too_long": 3}})
    assert status(host) == tr(TOO_LONG).format(3)


def test_saved_results_say_nothing_about_saving(gui):
    host = gui()
    host._do_sync_action(DIALOG, "upload")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"upload": {
        "success": True, "lists_pushed": 1, "items_pushed": 1, "notes_too_long": 2}})
    (kind, title, text), = host.notices
    assert text.split("\n\n")[-1] == tr(TOO_LONG_KEPT).format(2)
    assert host._lists_not_saved_line() not in text


# ---------------------------------------------------------------------------
# The automatic upload
# ---------------------------------------------------------------------------

def test_a_list_change_asks_the_runner_for_an_upload(gui, monkeypatch):
    host = gui()
    _signed_in(host)
    direct = _no_direct_sync(monkeypatch, host)
    host._lists_auto_sync()
    host._lists_auto_sync()
    assert host._lists_sync.names() == ["request_auto", "request_auto"]
    assert direct == []
    assert not host._lists_changed_while_sync_off


def test_a_list_change_while_sync_is_off_is_remembered_not_sent(gui):
    host = gui()
    host._lists_auto_sync()
    assert host._lists_sync.calls == []
    assert host._lists_changed_while_sync_off is True


def test_the_automatic_upload_hints_at_differing_and_too_long_notes_once_a_session(gui, lang):
    host = gui()
    host._on_lists_auto_done({"upload": {"success": True, "notes_differing": 2}})
    hint = tr("Some notes differ from your account and were not uploaded. To keep both versions, "
              "use Sync lists now, then Merge Both.")
    assert status(host) == hint
    host.statusBar().clearMessage()
    host._on_lists_auto_done({"upload": {"success": True, "notes_differing": 2}})
    assert status(host) == "", "the hint came back after every upload"
    host._on_lists_auto_done({"upload": {"success": True, "notes_too_long": 1}})
    assert status(host) == tr(TOO_LONG_KEPT).format(1)
    if lang == "he":
        assert _is_hebrew(_first_letter(hint))


@pytest.mark.parametrize("outcome", [{"cancelled": True}, {"cancelled": True, "stale": True},
                                     {"cancelled": True, "shutdown": True}],
                         ids=["cancelled", "stale", "shutdown"])
def test_an_automatic_upload_that_did_not_end_normally_shows_nothing(gui, outcome):
    host = gui()
    host._on_lists_auto_done(dict(outcome, upload={"success": True, "notes_differing": 2}))
    assert status(host) == "" and host.notices == []


LIST_MUTATORS = {
    "create_list", "update_list", "update_list_project", "create_project", "update_project",
    "delete_project", "apply_list_layout", "delete_list", "restore_list", "permanently_delete_list",
    "empty_trash", "duplicate_list", "merge_lists", "merge_duplicate_group",
    "auto_merge_duplicate_group", "restore_project_hierarchy", "add_item", "add_items_bulk",
    "update_item", "remove_item_from_list", "move_items_to_list", "add_tag_to_items", "import_list",
    "clear_all", "resolve_web_removals",
}


def _window_methods():
    tree = ast.parse((ROOT / "genizah_app.py").read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "GenizahGUI")
    return [n for n in cls.body if isinstance(n, ast.FunctionDef)]


def test_every_list_mutator_asks_for_an_upload_ast():
    """Every window method that changes the lists asks for an upload after it (the
    Recently Viewed list never syncs, so add_to_recent is not a change here)."""
    missing, found = [], 0
    for fn in _window_methods():
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
        mutates = any(c.func.attr in LIST_MUTATORS and ast.unparse(c.func.value) == "self.lists_mgr"
                      for c in calls)
        if mutates:
            found += 1
            if not any(c.func.attr == "_lists_auto_sync" for c in calls):
                missing.append(fn.name)
    assert found >= 20, "the scan no longer finds the window's list edits"
    assert missing == [], f"these change the lists and never ask for an upload: {missing}"


def test_every_list_mutator_asks_for_an_upload_by_calling_them(gui, monkeypatch):
    host = gui(list_names=("Local list", "Other list"))
    _signed_in(host)
    runner = host._lists_sync
    item_id = "990000001"

    host.lists_current_item_id = item_id
    host.lists_detail_note = types.SimpleNamespace(text=lambda: "an edited note")
    host.lists_save_item_details()
    assert runner.names() == ["request_auto"]
    assert host.lists_mgr.get_item(item_id)["note"] == "an edited note"

    host.lists_current_list_id = host.list_ids[0]
    host.lists_get_selected_item_ids = lambda: [item_id]
    other = host.lists_mgr.data["lists"][host.list_ids[1]]
    monkeypatch.setattr(genizah_app, "QInputDialog", types.SimpleNamespace(
        getItem=lambda *a, **k: (host._get_list_display_name(dict(other, id=host.list_ids[1])), True)))
    host.lists_move_selected_items()
    assert runner.names() == ["request_auto"] * 2
    assert host.lists_mgr.get_item(item_id)["lists"] == [host.list_ids[1]]

    class Action:
        def __init__(self, text):
            self.text, self._data = text, None

        def setData(self, value):
            self._data = value

        def data(self):
            return self._data

    class Menu:
        def __init__(self, *a):
            self.actions = []

        def addAction(self, text):
            self.actions.append(Action(text))
            return self.actions[-1]

        def addMenu(self, text):
            return Menu()

        def addSeparator(self):
            pass

        def exec(self, pos):
            return next(a for a in self.actions if a.data() == host.list_ids[0])

    monkeypatch.setattr(genizah_app, "QMenu", Menu)
    host.status_label = types.SimpleNamespace(setText=lambda text: None)
    host.show_add_to_list_menu(["990000002"], source="search")
    assert runner.names() == ["request_auto"] * 3
    assert host.list_ids[0] in host.lists_mgr.get_item("990000002")["lists"]


class _MenuAction:
    def __init__(self, text):
        self.text, self._data = text, None

    def setData(self, value):
        self._data = value

    def data(self):
        return self._data


def _menu_choosing(label):
    """QMenu whose exec() returns the action labelled `label`."""
    class Menu:
        def __init__(self, *a):
            self.actions = []

        def addAction(self, text):
            self.actions.append(_MenuAction(text))
            return self.actions[-1]

        def addMenu(self, text):
            return Menu()

        def addSeparator(self):
            pass

        def exec(self, pos):
            return next(a for a in self.actions if a.text == label)
    return Menu


@pytest.mark.parametrize("branch", ["add-to-a-new-list", "project-rename", "project-delete-keep-lists",
                                    "project-delete-with-lists"])
def test_the_other_branches_of_the_list_menus_ask_for_an_upload(gui, monkeypatch, branch):
    """The branches the test above does not take: 'New List...' in the add-to-list menu, and each
    entry of a project's menu in the Lists tab."""
    host = gui(list_names=("Local list", "In a project"))
    _signed_in(host)
    runner = host._lists_sync
    lists = host.lists_mgr.data["lists"]
    monkeypatch.setattr(genizah_app, "QInputDialog", types.SimpleNamespace(
        getText=lambda *a, **k: ("Made from the menu", True)))
    if branch == "add-to-a-new-list":
        monkeypatch.setattr(genizah_app, "QMenu", _menu_choosing(tr("New List...")))
        host.status_label = types.SimpleNamespace(setText=lambda text: None)
        host.show_add_to_list_menu(["990000002"], source="search")
        made, = [lid for lid, ld in lists.items() if ld.get("name") == "Made from the menu"]
        assert made in host.lists_mgr.get_item("990000002")["lists"]
    else:
        project = host.lists_mgr.create_project("A project")
        lists[host.list_ids[1]]["project_id"] = project
        label = {"project-rename": "Rename Project", "project-delete-keep-lists": "Delete Project (Keep Lists)",
                 "project-delete-with-lists": "Delete Project and Lists"}[branch]
        monkeypatch.setattr(genizah_app, "QMenu", _menu_choosing(tr(label)))
        node = types.SimpleNamespace(data=lambda col, role: None if role == Qt.ItemDataRole.UserRole else project)
        host.lists_tree = types.SimpleNamespace(itemAt=lambda pos: node, mapToGlobal=lambda pos: pos)
        host.lists_current_list_id = host.list_ids[0]
        host.lists_show_list_context_menu(object())
        projects = host.lists_mgr.data.get("projects", {})
        if branch == "project-rename":
            assert projects[project]["name"] == "Made from the menu"
        elif branch == "project-delete-keep-lists":
            assert project not in projects and lists[host.list_ids[1]].get("project_id") is None
        else:
            assert project not in projects and lists[host.list_ids[1]].get("deleted_at")   # in the Trash
    assert runner.names() == ["request_auto"]


# ---------------------------------------------------------------------------
# Sign-in: the preview runs on the runner; the choice waits for a free screen
# ---------------------------------------------------------------------------

def _preview(n_cloud=1, names=None, success=True, error=None, **extra):
    lists = [{"id": f"cl-{i}", "name": (names or [f"Cloud list {i}" for i in range(n_cloud)])[i],
              "color": "#4CAF50", "item_count": 3} for i in range(n_cloud)]
    return dict({"success": success, "lists": lists if success else [], "error": error}, **extra)


def test_a_sign_in_previews_on_the_runner_and_offers_the_choice(gui, monkeypatch):
    host = gui()
    direct = _no_direct_sync(monkeypatch, host)
    runner = host._lists_sync

    host._enable_lists_cloud_sync()

    assert direct == [], "the preview ran on the UI thread"
    assert runner.names() == ["invalidate_auth", "allow_auto", "run:preview"]
    assert host.lists_mgr.is_sync_available() and host._lists_sync_user_id == USER
    busy = FakeProgress.made[-1]
    assert busy.label == tr("Checking the lists in your account...")
    assert busy.min_duration == 500 and not busy.shown, "the busy dialog did not wait half a second"
    busy.canceled.emit()
    assert runner.calls[-1] == ("cancel", runner.last("preview"))

    runner.finish(runner.last("preview"), {"preview": _preview(2)})
    assert busy.closed
    (local_lists, cloud_lists, cloud_error), = host.dialogs
    assert [c["name"] for c in cloud_lists] == ["Cloud list 0", "Cloud list 1"]
    assert cloud_error is None


def test_changes_made_while_signed_out_count_as_unsent_at_the_next_sign_in(gui):
    host = gui()
    host._lists_auto_sync()                           # sync is off
    host._enable_lists_cloud_sync()
    assert "mark_dirty" in host._lists_sync.names()
    assert host._lists_changed_while_sync_off is False


def test_sync_lists_now_in_a_session_already_syncing_keeps_the_running_upload(gui):
    host = gui()
    _signed_in(host)
    host._enable_lists_cloud_sync(always_offer=True)
    assert "invalidate_auth" not in host._lists_sync.names()


@pytest.mark.parametrize("case", ["both-empty", "already-in-sync", "skip", "action-chosen"])
def test_an_explicit_sign_in_uploads_once_when_no_dialog_is_needed_and_after_skip(gui, case):
    host = gui()
    runner = host._lists_sync
    if case == "both-empty":
        host.lists_mgr.get_local_lists_summary = lambda: []
        preview = _preview(0)
    elif case == "already-in-sync":
        host.lists_mgr.data["lists"][host.list_ids[0]]["cloud_id"] = "cl-0"
        host.lists_mgr.data["lists"]["default"]["cloud_id"] = "cl-1"
        preview = _preview(2, names=["General", "Local list"])
    else:
        preview = _preview(2)
        host.dialog_result = 0 if case == "skip" else 1

    host._enable_lists_cloud_sync()
    runner.finish(runner.last("preview"), {"preview": preview})

    wanted = 0 if case == "action-chosen" else 1
    assert runner.names().count("request_auto") == wanted
    assert len(host.dialogs) == (1 if case in ("skip", "action-chosen") else 0)


def test_sync_lists_now_offers_the_choice_even_when_in_sync(gui):
    host = gui()
    host.lists_mgr.data["lists"][host.list_ids[0]]["cloud_id"] = "cl-0"
    host.lists_mgr.data["lists"]["default"]["cloud_id"] = "cl-1"
    host._enable_lists_cloud_sync(always_offer=True)
    host._lists_sync.finish(host._lists_sync.last("preview"),
                            {"preview": _preview(2, names=["General", "Local list"])})
    assert len(host.dialogs) == 1


def test_a_failed_preview_after_a_sign_in_offers_the_upload_with_the_error_translated(gui, lang):
    host = gui()
    host._enable_lists_cloud_sync()
    host._lists_sync.finish(host._lists_sync.last("preview"),
                            {"preview": _preview(success=False, error="Sync not available")})
    (local_lists, cloud_lists, cloud_error), = host.dialogs
    assert cloud_lists == [] and cloud_error == tr("Sync not available")


@pytest.mark.parametrize("outcome", [
    {"cancelled": True},
    {"cancelled": True, "stale": True},
    {"preview": _preview(success=False, error="Sync stopped", stopped=True)},
], ids=["cancelled", "preview-after-new-sign-in", "stopped-by-the-sign-out-deadline"])
def test_a_preview_that_did_not_end_normally_offers_nothing(gui, outcome):
    host = gui()
    host._enable_lists_cloud_sync()
    host._lists_sync.finish(host._lists_sync.last("preview"), outcome)
    assert host.dialogs == [] and host.notices == []
    assert "request_auto" not in host._lists_sync.names()


def test_a_preview_returning_after_a_sign_out_began_offers_nothing(gui):
    host = gui()
    host._enable_lists_cloud_sync()
    host._logout_pending = True
    host._lists_sync.finish(host._lists_sync.last("preview"), {"preview": _preview(2)})
    assert host.dialogs == []
    assert "request_auto" not in host._lists_sync.names()


@pytest.mark.parametrize("blocked", ["hidden", "restoring", "modal-open", "close-pending",
                                     "close-waiting-for-prompt"])
def test_the_sync_choice_waits_for_a_free_screen(gui, blocked):
    host = gui()
    set_blocked = {
        "hidden": lambda on: host.screen.__setitem__("visible", not on),
        "restoring": lambda on: setattr(host, "_restoring_session", on),
        "modal-open": lambda on: host.screen.__setitem__("modal", object() if on else None),
        "close-pending": lambda on: setattr(host, "_close_pending", on),
        "close-waiting-for-prompt": lambda on: setattr(host, "_close_waiting_for_prompt", on),
    }[blocked]
    set_blocked(True)
    host._enable_lists_cloud_sync()
    host._lists_sync.finish(host._lists_sync.last("preview"), {"preview": _preview(2)})
    pump(0.3)
    assert host.dialogs == [], f"the sync choice opened while {blocked}"

    set_blocked(False)
    pump(0.3)
    assert len(host.dialogs) == 1, "the held choice did not open once the screen was free"
    pump(0.2)
    assert len(host.dialogs) == 1, "the held choice opened twice"
    assert host._lists_sync.names().count("request_auto") == 1   # Skip, once


@pytest.mark.parametrize("case", ["held-preview-after-sign-out", "held-preview-after-new-sign-in",
                                  "held-preview-past-deadline", "held-preview-after-a-newer-preview"])
def test_a_held_choice_from_before_a_sign_out_or_a_new_sign_in_is_dropped(gui, case):
    host = gui()
    host.screen["visible"] = False
    host._enable_lists_cloud_sync()
    job = host._lists_sync.last("preview")
    if case == "held-preview-past-deadline":
        job.deadline = time.monotonic() + 0.05
    host._lists_sync.finish(job, {"preview": _preview(2)})
    pump(0.1)
    if case == "held-preview-after-sign-out":
        host._logout_pending = True
    elif case == "held-preview-after-new-sign-in":
        host._lists_sync.invalidate_auth()
    elif case == "held-preview-after-a-newer-preview":
        host._enable_lists_cloud_sync()
    pump(0.2)
    host._logout_pending = False
    host.screen["visible"] = True
    pump(0.3)
    assert host.dialogs == [], f"{case}: the old account's choice was offered"
    assert host._held_lists_dialog is None and host._held_lists_dialog_timer is None


def test_the_preview_dialog_is_fully_translated(gui, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    captured = []

    class Dialog(QDialog):
        def exec(self):
            captured.append(self)
            return 0

    monkeypatch.setattr(genizah_app, "QDialog", Dialog)
    host = gui(list_names=tuple(f"רשימה {i}" for i in range(11)))   # + General = 12
    del host._show_lists_sync_dialog                                   # the real one
    def check_hebrew(dialog):
        for text in [w.text() for w in dialog.findChildren(QLabel)]:
            first = text.strip()[:1]
            assert first == "•" or _is_hebrew(_first_letter(text)), f"not in Hebrew: {text!r}"
            assert "items" not in text and "more" not in text, text
        tips = [b.toolTip() for b in dialog.findChildren(QPushButton) if b.toolTip()]
        assert tips and all(_is_hebrew(_first_letter(t)) for t in tips), tips

    host._show_lists_sync_dialog(host.lists_mgr.get_local_lists_summary(),
                                 _preview(12, names=[f"ענן {i}" for i in range(12)])["lists"], None)
    assert len(captured) == 1
    check_hebrew(captured[0])
    rows = [w.text() for w in captured[0].findChildren(QLabel)]
    assert rows.count("• " + tr("{} ({} items)").format("ענן 0", 3)) == 1
    assert rows.count(tr("... and {} more").format(2)) == 2

    host._enable_lists_cloud_sync()                                    # the error comes from the preview
    host._lists_sync.finish(host._lists_sync.last("preview"),
                            {"preview": _preview(success=False, error="Sync not available")})
    assert len(captured) == 2
    check_hebrew(captured[1])
    error_line = tr("Error: {}").format(host._sync_error_text({"error": "Sync not available"}))
    assert error_line in [w.text() for w in captured[1].findChildren(QLabel)]
    assert _is_hebrew(_first_letter(error_line))


def test_the_skip_tooltip_says_what_skip_does(gui, monkeypatch, lang):
    captured = []

    class Dialog(QDialog):
        def exec(self):
            captured.append(self)
            return 0

    monkeypatch.setattr(genizah_app, "QDialog", Dialog)
    host = gui()
    genizah_app.GenizahGUI._show_lists_sync_dialog(host, [{"name": "L", "item_count": 1}], [], None)
    skip, = [b for b in captured[0].findChildren(QPushButton) if b.text() == tr("Skip")]
    assert skip.toolTip() == tr(
        "Don't download from your account now. Uploads continue: until you sign out or close "
        "the program, your lists are uploaded to your account after each change.")
    assert "Don't sync now - you can sync later from Settings" not in TRANSLATIONS


@pytest.mark.parametrize("press", ["Skip", "close", "Download from Cloud", "Upload to Cloud", "Merge Both"])
def test_the_sync_choice_uploads_once_unless_an_action_was_chosen(gui, monkeypatch, press):
    """The real dialog: Skip or its close button returns falsy (one upload follows), an action
    button accepts it (the action runs; no extra upload)."""
    class Dialog(QDialog):
        def exec(self):
            if press == "close":
                self.reject()                     # what the title bar's close button does
            else:
                button, = [b for b in self.findChildren(QPushButton) if b.text() == tr(press)]
                button.click()
            return self.result()

    monkeypatch.setattr(genizah_app, "QDialog", Dialog)
    host = gui()
    del host._show_lists_sync_dialog                                   # the real one
    runner = host._lists_sync
    host._show_lists_sync_choice((host.lists_mgr.get_local_lists_summary(), _preview(2)["lists"], None))
    action = {"Download from Cloud": "download", "Upload to Cloud": "upload", "Merge Both": "merge"}.get(press)
    assert runner.names() == ([f"run:{action}"] if action else ["request_auto"])


# ---------------------------------------------------------------------------
# A sign-in restored at startup, and an offline start
# ---------------------------------------------------------------------------

# The restore either returns (whichever of its paths: no saved state, 'never', declined, restored --
# all one return to this code) or raises. Its own paths are the session-restore tests' to pin.
@pytest.mark.parametrize("restore", ["returns", "raises"])
def test_a_restored_sign_in_turns_sync_on_after_the_session_restore(gui, restore):
    host = gui()
    runner = host._lists_sync
    seen = []

    def restore_session():  # the restore's questions are modal: they are answered in here
        host._restoring_session = True
        try:
            seen.append(list(runner.names()))
            if restore == "raises":
                raise RuntimeError("the saved session could not be read")
        finally:
            host._restoring_session = False

    host._restore_session = restore_session
    if restore == "raises":
        with pytest.raises(RuntimeError):
            host._restore_session_then_lists_sync()
    else:
        host._restore_session_then_lists_sync()

    assert seen == [[]], "list sync started before the session restore returned"
    assert runner.names().count("run:preview") == 1
    assert host.lists_mgr.is_sync_available()
    assert FakeProgress.made == [], "a restored start showed the busy dialog"


def test_a_restored_start_without_a_saved_sign_in_does_nothing(gui):
    host = gui()
    host.corrections_client.current_user = None
    host._restore_session = lambda: None
    host._restore_session_then_lists_sync()
    assert host._lists_sync.calls == []
    assert not host.lists_mgr.is_sync_available()


def test_startup_schedules_the_restore_then_the_list_sync():
    src = ast.unparse(next(f for f in _window_methods() if f.name == "on_startup_finished"))
    assert "QTimer.singleShot(200, self._restore_session_then_lists_sync)" in src
    assert "QTimer.singleShot(200, self._restore_session)" not in src


def test_a_failed_preview_at_startup_shows_no_dialog(gui, lang):
    host = gui()
    host._restore_session = lambda: None
    host._restore_session_then_lists_sync()
    host._lists_sync.finish(host._lists_sync.last("preview"),
                            {"preview": _preview(success=False, error="No Supabase client")})
    expected = tr("Could not check the lists in your account: {}. List sync stays on; Sync lists "
                  "now tries again.").format(tr("No Supabase client"))
    assert status(host) == expected
    assert host.dialogs == [] and host.notices == []
    assert "request_auto" not in host._lists_sync.names()
    assert host.lists_mgr.is_sync_available(), "an offline start turned list sync off"
    if lang == "he":
        assert _is_hebrew(_first_letter(expected))


def test_a_restored_sign_in_offers_the_choice_only_once_the_window_is_shown(gui):
    host = gui()
    host.screen["visible"] = False
    host._restore_session = lambda: None
    host._restore_session_then_lists_sync()
    host._lists_sync.finish(host._lists_sync.last("preview"), {"preview": _preview(2)})
    pump(0.3)
    assert host.dialogs == []
    host.screen["visible"] = True
    pump(0.3)
    assert len(host.dialogs) == 1


# ---------------------------------------------------------------------------
# Sign-out: at once, this computer's session only, bounded, and truthful
# ---------------------------------------------------------------------------

LOGGED_OUT = "You have been logged out."
P16 = ("These lists were not uploaded before you signed out: {}. Your changes to them are saved on "
       "this computer and are uploaded after you next sign in.")
P17 = ("Your latest list changes were not uploaded before you signed out. They are saved on this "
       "computer and are uploaded after you next sign in.")
P18 = ("List sync was not on in this session, so your list changes were not uploaded. They are saved "
       "on this computer and are uploaded after you next sign in.")
P32 = ("Some notes differ from your account and were not uploaded. They are kept on this computer; "
       "after you next sign in, use Sync lists now, then Merge Both, to keep both versions.")
P35 = ("Your lists cannot be saved on this computer at the moment ({} cannot be written). List "
       "changes that did not reach your account before you signed out are lost when you close the program.")


def _notice(host):
    (kind, title, text), = host.notices
    assert title == tr("Logged Out")
    first, *paragraphs = text.split("\n\n")
    assert first == tr(LOGGED_OUT)
    return kind, paragraphs


def test_sign_out_returns_at_once_and_ends_when_the_last_upload_does(gui):
    host = gui()
    _signed_in(host)
    runner = host._lists_sync

    host._do_logout()

    assert host._logout_pending and host.corrections_client.logouts == []
    assert host.corner_login_btn.text() == tr("Signing out...") and not host.corner_login_btn.isEnabled()
    assert runner.calls[-1] == ("begin_logout", host.LOGOUT_SYNC_BUDGET_S) == ("begin_logout", 10)
    host._do_logout()                               # a second click while it runs
    assert runner.names().count("begin_logout") == 1

    runner.finish(runner.last("logout"), {"upload": {"success": True}})
    assert not host._logout_pending
    assert host.corrections_client.logouts == [("background", threading.main_thread())]
    assert "invalidate_auth" in runner.names() and not host.lists_mgr.is_sync_available()
    assert host.corner_login_btn.isEnabled() and host.corner_login_btn.text() == tr("Login")
    assert host.panel_refreshes == [{"cached_only": True}]
    assert _notice(host) == ("information", [])


def test_a_sign_out_that_cuts_a_running_upload_says_so(gui):
    host = gui()
    _signed_in(host)
    host.LOGOUT_SYNC_BUDGET_S = 0.2
    runner = host._lists_sync
    host._do_logout()
    job = runner.last("logout")
    pump(1.2)                                   # the upload is stuck; the budget ends the sign-out
    assert not host._logout_pending
    assert ("cancel", job) in runner.calls
    assert _notice(host) == ("warning", [tr(P17)])
    runner.finish(job, {"cancelled": True, "upload": STOPPED_UPLOAD})   # it ends later
    assert len(host.notices) == 1, "the late end of the cut upload showed a second notice"


def test_an_earlier_sign_outs_timer_does_not_cut_a_later_one(gui):
    host = gui()
    _signed_in(host)
    runner = host._lists_sync
    host.LOGOUT_SYNC_BUDGET_S = 0.2              # its timer fires 0.7 s from now
    host._do_logout()
    runner.finish(runner.last("logout"), {"upload": {"success": True}})
    host.corrections_client.sign_in()
    _signed_in(host)
    host.LOGOUT_SYNC_BUDGET_S = 5
    host._do_logout()
    pump(1.2)
    assert host._logout_pending, "the first sign-out's timer ended the second one"
    runner.finish(runner.last("logout"), {"upload": {"success": True}})
    assert not host._logout_pending and len(host.notices) == 2


def test_a_cut_sign_outs_job_that_ends_during_a_later_sign_out_leaves_it_alone(gui):
    """The budget cuts a sign-out whose upload hangs in a request; its job ends only when that
    request returns -- which can be during the next sign-out. Its end is the first sign-out's."""
    host = gui()
    _signed_in(host)
    runner = host._lists_sync
    host._do_logout()
    first = runner.last("logout")
    host._finish_logout(None, token=host._logout_generation)    # what the budget timer calls
    assert not host._logout_pending and ("cancel", first) in runner.calls and len(host.notices) == 1

    host.corrections_client.sign_in()
    _signed_in(host)
    host._do_logout()
    second = runner.last("logout")
    assert second is not first and host._logout_pending

    runner.finish(first, {"cancelled": True, "upload": STOPPED_UPLOAD})   # the cut job ends only now
    assert host._logout_pending, "the first sign-out's late job ended the second one"
    assert ("cancel", second) not in runner.calls and len(host.notices) == 1

    runner.finish(second, {"upload": {"success": True}})
    assert not host._logout_pending and len(host.notices) == 2
    assert host.notices[-1][0] == "information"


@pytest.mark.parametrize("unsent", [False, True], ids=["nothing-unsent", "changes-unsent"])
def test_a_sign_out_whose_last_upload_cannot_start_still_signs_out(gui, monkeypatch, unsent):
    host = gui()
    _signed_in(host)
    runner = host._lists_sync
    runner.unsent = unsent
    logged = []
    monkeypatch.setattr(genizah_app.logger, "exception", lambda msg, *a, **k: logged.append(msg % a if a else msg))

    def begin_logout(on_done, budget_s):
        runner.calls.append(("begin_logout", budget_s))
        raise RuntimeError("the runner could not take the sign-out")
    runner.begin_logout = begin_logout

    host._do_logout()                                  # nothing escapes the button's handler

    assert "Sign-out: the last list upload could not be started" in logged
    assert not host._logout_pending and host._logout_timer is None
    assert host.corner_login_btn.isEnabled() and host.corner_login_btn.text() == tr("Login")
    assert host.corrections_client.logouts == [("background", threading.main_thread())]
    assert not host.lists_mgr.is_sync_available()
    assert _notice(host) == (("warning", [tr(P17)]) if unsent else ("information", []))
    host.corrections_client.sign_in()                  # and the next sign-out is not blocked
    _signed_in(host)
    del runner.begin_logout
    host._do_logout()
    assert runner.names().count("begin_logout") == 2 and host._logout_pending


def test_sign_out_resets_the_telemetry_identity(gui, monkeypatch):
    from desktop import telemetry
    resets = []
    monkeypatch.setattr(telemetry, "reset_identity", lambda *a, **k: resets.append(a))
    host = gui()
    host._do_logout()                                  # list sync off: the sign-out ends at once
    assert resets == [()]


def _logout_cell(host, monkeypatch, cell):
    """Run a sign-out as `cell` describes; return the notice's kind and paragraphs."""
    state = {"differing": 0, "failing": False}
    monkeypatch.setattr(host.lists_mgr, "differing_notes_count", lambda: state["differing"])
    monkeypatch.setattr(host.lists_mgr, "saves_failing", lambda: state["failing"])
    runner = host._lists_sync
    failed = {"success": False, "error": "Sync not available", "lists_not_uploaded": ["Local list"]}
    outcome = {"upload": {"success": True}}
    if cell in ("b-earlier-conflict-upload-fails", "c-earlier-conflict-budget-cut",
                "f-earlier-conflict-membership-unchecked", "h-orphan-with-a-website-edit"):
        state["differing"] = 1
    if cell == "a-logout-upload-keeps-differing-notes":
        state["differing"] = 2
        outcome = {"upload": {"success": True, "notes_differing": 2}}
    elif cell == "b-earlier-conflict-upload-fails":
        outcome = {"upload": failed}
    elif cell == "e-too-long":
        outcome = {"upload": {"success": True, "notes_too_long": 1}}
    elif cell == "f-earlier-conflict-membership-unchecked":
        outcome = {"upload": {"success": True, "unchecked": 1, "complete": False}}
    elif cell == "h-orphan-with-a-website-edit":
        host._on_lists_auto_done({"upload": {"success": True, "notes_differing": 1}})
        assert status(host) == tr("Some notes differ from your account and were not uploaded. To keep "
                                  "both versions, use Sync lists now, then Merge Both.")
    elif cell == "i-a-removal-left-unsent":
        outcome = {"upload": {"success": False, "items_failed": 1, "removals_failed": 1,
                              "lists_not_uploaded": [], "error": "Sync not available"}}
    elif cell == "i2-a-removal-left-unsent-with-lists":
        outcome = {"upload": dict(failed, removals_failed=1, items_failed=1)}
    elif cell == "cancelled":
        outcome = {"cancelled": True, "upload": STOPPED_UPLOAD}
    elif cell == "unsent-after-an-earlier-failed-upload":
        runner.unsent = True
    elif cell in ("j-saves-failing", "j-too-long"):
        state["failing"] = True
        state["differing"] = 1
        outcome = {"upload": dict(failed, notes_too_long=1 if cell == "j-too-long" else 0)}
    if cell in ("g-sync-never-on", "g2-sync-never-on-nothing-changed"):
        if cell == "g-sync-never-on":
            host._lists_auto_sync()                  # a change while sync is off
        host._do_logout()
        return _notice(host)
    _signed_in(host)
    host._do_logout()
    if cell == "c-earlier-conflict-budget-cut":
        host._finish_logout(None, token=host._logout_generation)   # what the budget timer calls
    else:
        runner.finish(runner.last("logout"), outcome)
    return _notice(host)


LOGOUT_CELLS = {
    "a-logout-upload-keeps-differing-notes": [P32],
    "b-earlier-conflict-upload-fails": [(P16, "Local list"), P32],
    "c-earlier-conflict-budget-cut": [P17, P32],
    "d-conflict-resolved-by-a-complete-upload": [],
    "e-too-long": [(TOO_LONG_KEPT, 1)],
    "f-earlier-conflict-membership-unchecked": [P32],
    "g-sync-never-on": [P18],
    "g2-sync-never-on-nothing-changed": [],
    "h-orphan-with-a-website-edit": [P32],
    "i-a-removal-left-unsent": [P17],
    "i2-a-removal-left-unsent-with-lists": [P17],
    "cancelled": [P17],
    "unsent-after-an-earlier-failed-upload": [P17],
    "j-saves-failing": [(P35, "lists.pkl")],
    "j-too-long": [(P35, "lists.pkl"), (TOO_LONG, 1)],
}


@pytest.mark.parametrize("cell", list(LOGOUT_CELLS))
def test_sign_out_says_what_did_not_upload(gui, monkeypatch, lang, cell):
    host = gui()
    kind, paragraphs = _logout_cell(host, monkeypatch, cell)
    expected = [tr(p[0]).format(p[1]) if isinstance(p, tuple) else tr(p) for p in LOGOUT_CELLS[cell]]
    assert paragraphs == expected
    assert kind == ("warning" if expected else "information")
    if lang == "he":
        assert all(_is_hebrew(_first_letter(p)) for p in paragraphs), paragraphs
    if cell.startswith("j"):
        joined = " ".join(paragraphs)
        for kept in (P16, P17, P18, P32):
            assert tr(kept).split("{}")[0] not in joined
        assert tr(TOO_LONG_KEPT).format(1) not in joined


# ---- the account's own client: nothing waits on the network --------------

# Long enough to show that something waited on it; short enough that code which does
# wait (the code before this change) still finishes the run.
HANG_S = 3


class _Auth:
    """A supabase auth client whose sign_out and get_user can hang."""

    def __init__(self, gate=None):
        self.gate = gate
        self.calls = []

    def sign_out(self, options=None):
        self.calls.append(("sign_out", options, threading.current_thread()))
        if self.gate is not None:
            self.gate.wait(HANG_S)

    def get_user(self, jwt=None):
        self.calls.append(("get_user", jwt, threading.current_thread()))
        if self.gate is not None:
            self.gate.wait(HANG_S)
        return None

    def get_session(self):
        return None


class _Supabase:
    def __init__(self, auth):
        self.auth = auth


@pytest.fixture
def account(tmp_path, monkeypatch):
    """The real SupabaseCorrectionsClient, signed in on a client object whose requests hang,
    with every community request a recorded hang too."""
    from desktop import supabase_corrections_client as scc
    gate = threading.Event()
    made = []
    monkeypatch.setattr(scc, "SUPABASE_AVAILABLE", True)
    monkeypatch.setattr(scc, "SUPABASE_ANON_KEY", "test-key")
    monkeypatch.setattr(scc, "create_client", lambda *a, **k: made.append(_Supabase(_Auth())) or made[-1],
                        raising=False)
    client = scc.SupabaseCorrectionsClient(config_path=tmp_path / "corrections")
    old = _Supabase(_Auth(gate))
    client._client = old
    client.current_user = scc.User(id=1, email="r@example.org", username="reader", _uuid=USER)
    client.credentials_file.write_text(json.dumps({"access_token": "a", "refresh_token": "r"}))
    network = []
    for name in ("is_server_available", "get_discoveries", "get_all_corrections", "get_all_comments",
                 "search_joins", "get_published_puzzle_joins"):
        def hang(*a, _n=name, **k):
            network.append(_n)
            gate.wait(HANG_S)
            return ([], 0)
        monkeypatch.setattr(client, name, hang)
    yield types.SimpleNamespace(client=client, old=old, made=made, network=network, gate=gate)
    gate.set()


@pytest.mark.parametrize("sync", ["sync-off", "sync-on"])
def test_an_ordinary_sign_out_never_waits_for_the_network(gui, account, sync):
    host = gui()
    host.corrections_client = account.client
    del host._refresh_community_panels                      # the real, cache-only one
    host.community_tab = host.create_community_tab()
    timings = [0.0]
    finish = getattr(host, "_finish_logout", None)

    def timed_finish(*a, **k):
        t0 = time.monotonic()
        try:
            return finish(*a, **k)
        finally:
            timings.append(time.monotonic() - t0)

    if finish is not None:
        host._finish_logout = timed_finish
    if sync == "sync-on":
        _signed_in(host)
        host.LOGOUT_SYNC_BUDGET_S = 0.2
    t0 = time.monotonic()
    host._do_logout()
    returned_after = time.monotonic() - t0
    assert returned_after < 1.0, f"sign-out held the UI thread for {returned_after:.1f}s"
    if sync == "sync-on":
        assert host._logout_pending
        pump(1.0)                                       # the budget timer ends it
        assert len(timings) == 2

    assert max(timings) < 1.0, f"the sign-out's second half took {max(timings):.1f}s"
    assert not host._logout_pending
    assert account.network == [], f"the sign-out made requests on the UI thread: {account.network}"
    assert [c[0] for c in account.old.auth.calls if c[2] is threading.main_thread()] == [], \
        "the old session was revoked (or read) on the UI thread"
    assert not account.client.credentials_file.exists()
    assert account.client.current_user is None
    assert len(host.notices) == 1


def test_a_background_revoke_cannot_touch_a_later_sign_in(account):
    client = account.client
    before = set(threading.enumerate())      # an earlier test's revoke thread may still be exiting
    client.logout(revoke="background")
    assert client._client is None and client.current_user is None
    assert not client.credentials_file.exists()
    revoke, = [t for t in threading.enumerate() if t.name == "supabase-sign-out" and t not in before]
    assert revoke.daemon

    new = _Supabase(_Auth())                           # signed in again, as the same user
    user = types.SimpleNamespace(_uuid=USER, username="reader")
    client._client, client.current_user = new, user
    account.gate.set()
    revoke.join(5)

    assert [(c[0], c[1]) for c in account.old.auth.calls] == [("sign_out", {"scope": "local"})]
    assert account.old.auth.calls[0][2] is revoke
    assert new.auth.calls == [], "the revoke reached the later sign-in's client"
    assert client._client is new and client.current_user is user


def test_the_other_sign_out_path_also_ends_this_session_only(account):
    account.gate.set()
    account.client.logout()
    assert [(c[0], c[1]) for c in account.old.auth.calls] == [("sign_out", {"scope": "local"})]
    assert account.client.current_user is None and not account.client.credentials_file.exists()


def test_the_rest_client_accepts_the_same_sign_out():
    from desktop.corrections_client import CorrectionsClient
    import inspect
    assert "revoke" in inspect.signature(CorrectionsClient.logout).parameters


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------

class _Loader:
    def __init__(self):
        self.calls = []

    def isRunning(self):
        return True

    def request_cancel(self):
        self.calls.append("request_cancel")

    def wait(self, *a):
        self.calls.append("wait")
        return True


def _closing_host(host):
    host._defer_close_for_passage = lambda event: False
    host._close_result_dialog = lambda: None
    host._save_session = lambda: None
    host.cancel_browse_image_thread = lambda: None
    host.meta_loader = _Loader()
    return host


def _close(host):
    from PyQt6.QtGui import QCloseEvent
    event = QCloseEvent()
    event.ignore()
    t0 = time.monotonic()
    host.closeEvent(event)
    return time.monotonic() - t0, event


def test_closing_during_a_sign_out_whose_revoke_hangs_returns_promptly(gui, account):
    host = _closing_host(gui())
    host.corrections_client = account.client
    _signed_in(host)
    host._do_logout()
    assert host._logout_pending

    took, event = _close(host)

    assert took < 1.0, f"closeEvent took {took:.1f}s"
    assert not host._logout_pending
    assert not account.client.credentials_file.exists(), "the saved sign-in survived the close"
    assert host._lists_sync.names()[-1] == "shutdown"
    assert host.notices == [], "the close showed the sign-out notice"
    assert host.meta_loader.calls == ["request_cancel", "wait"] and event.isAccepted()


def test_a_failing_sign_out_at_close_still_stops_the_list_sync(gui):
    host = _closing_host(gui())
    _signed_in(host)
    host._do_logout()

    def broken(*a, **kw):
        raise ValueError("the sign-out broke")

    host._finish_logout = broken
    took, event = _close(host)
    assert "shutdown" in host._lists_sync.names()
    assert host._lists_sync.run("upload") is None, "the runner took a job after the close"
    assert host.meta_loader.calls == ["request_cancel", "wait"] and event.isAccepted()


def test_a_failing_list_sync_shutdown_skips_nothing_after_it(gui):
    host = _closing_host(gui())

    def broken():
        host._lists_sync.calls.append(("shutdown",))
        raise ValueError("the runner broke")

    host._lists_sync.shutdown = broken
    took, event = _close(host)                      # nothing escapes
    assert host._lists_sync.names() == ["shutdown"]
    assert host.meta_loader.calls == ["request_cancel", "wait"], "the worker stops were skipped"
    assert event.isAccepted(), "the window's own closeEvent was not reached"


# ---------------------------------------------------------------------------
# No text promises what does not happen
# ---------------------------------------------------------------------------

def test_no_false_promise_text_remains():
    app = (ROOT / "genizah_app.py").read_text(encoding="utf-8")
    for phrase in ("sync later from Settings", "uploaded when you sign out", "sync later"):
        assert phrase not in app, phrase
        assert not any(phrase in k or phrase in str(v) for k, v in TRANSLATIONS.items()), phrase
    kept_alive = {
        "desktop/single_instance.py": ("upload still in flight", "keep it alive"),
        "genizah_app.py": ("auto-sync and logout-sync workers", "auto-sync or logout-sync worker",
                           "upload still in flight", "_disable_lists_cloud_sync"),
        "tests/test_single_instance_lock.py": ("upload in flight", "in-flight upload"),
    }
    for rel, phrases in kept_alive.items():
        text = (ROOT / rel).read_text(encoding="utf-8")
        for phrase in phrases:
            assert phrase not in text, f"{rel} still says {phrase!r}"
    tree = ast.parse((ROOT / "tests/test_personal_state_atomic_writes.py").read_text(encoding="utf-8"))
    docs = [ast.get_docstring(n) or "" for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module))]
    assert not any("the auto-sync worker" in d for d in docs)


# ---------------------------------------------------------------------------
# Sync lists now, on the Lists tab
# ---------------------------------------------------------------------------

def _lists_tab(host):
    host.lists_tab = host.create_lists_tab()
    return host.btn_lists_sync_now


def test_sync_lists_now_is_enabled_only_when_signed_in_and_offers_the_choice_even_when_in_sync(gui, lang):
    host = gui()
    button = _lists_tab(host)
    assert button.text() == tr("Sync lists now")

    host.corrections_client.signed_in = False
    host._update_corner_login_state()
    assert not button.isEnabled()
    assert button.toolTip() == tr("Sign in to sync your lists with your account")

    host.corrections_client.sign_in()
    host._update_corner_login_state()
    assert button.isEnabled()
    assert button.toolTip() == tr("Download, upload or merge your lists with your account on "
                                  "genizahsearch.com")

    host.lists_mgr.data["lists"][host.list_ids[0]]["cloud_id"] = "cl-0"
    host.lists_mgr.data["lists"]["default"]["cloud_id"] = "cl-1"
    button.click()
    host._lists_sync.finish(host._lists_sync.last("preview"),
                            {"preview": _preview(2, names=["General", "Local list"])})
    assert len(host.dialogs) == 1, "Sync lists now did not offer the choice for lists in sync"

    _signed_in(host)
    host._do_logout()
    assert not button.isEnabled(), "Sync lists now stayed enabled during the sign-out"
    host._lists_sync.finish(host._lists_sync.last("logout"), {"upload": {"success": True}})
    assert not button.isEnabled() and button.toolTip() == tr("Sign in to sync your lists with your account")
    if lang == "he":
        assert all(_is_hebrew(_first_letter(t)) for t in (button.text(), button.toolTip()))


# ---------------------------------------------------------------------------
# The website-removal prompt
# ---------------------------------------------------------------------------

@pytest.fixture
def removals():
    from desktop import lists_web_removals_dialog
    return lists_web_removals_dialog


ENTRIES = [("990000001", "L1", "T-S 1.1", "List one"),
           ("990000002::img::3", "L1", "T-S 2.2, Page 3", "List one"),
           ("990000003", "L2", "ENA 3", "List two")]


def test_the_removal_dialog_returns_each_choice(removals, lang):
    dialog = removals.WebRemovalsDialog(None, ENTRIES)
    try:
        assert dialog.windowTitle() == tr("Entries removed on the website")
        assert [dialog.table.horizontalHeaderItem(i).text() for i in range(3)] == [
            tr("Shelfmark"), tr("List"), tr("Choice")]
        box = dialog.choice_boxes[0]
        assert [box.itemText(i) for i in range(box.count())] == [
            tr("Decide later"), tr("Remove from this computer too"),
            tr("Keep it (and add it back on the website)")]
        assert [b.currentData() for b in dialog.choice_boxes] == ["later"] * 3
        assert (dialog.layoutDirection() == Qt.LayoutDirection.RightToLeft) == (lang == "he")
        if lang == "he":
            texts = [w.text() for w in dialog.findChildren(QLabel) + dialog.findChildren(QPushButton)]
            assert all(_is_hebrew(_first_letter(t)) for t in texts), texts
        dialog.choice_boxes[0].setCurrentIndex(1)
        dialog.choice_boxes[2].setCurrentIndex(2)
        dialog.apply_btn.click()
        assert dialog.choices() == {("990000001", "L1"): "remove", ("990000003", "L2"): "keep"}
    finally:
        dialog.deleteLater()

    for button, choice in (("remove_all_btn", "remove"), ("keep_all_btn", "keep")):
        dialog = removals.WebRemovalsDialog(None, ENTRIES)
        getattr(dialog, button).click()
        assert dialog.choices() == {(e[0], e[1]): choice for e in ENTRIES}
        dialog.deleteLater()

    dialog = removals.WebRemovalsDialog(None, ENTRIES)
    dialog.choice_boxes[0].setCurrentIndex(1)
    dialog.close_btn.click()
    assert dialog.choices() == {}, "Close decided something"
    dialog.deleteLater()


def test_ask_about_web_removals_shows_the_prompt_and_returns_its_answer(removals, monkeypatch):
    def answer(self):
        self.choice_boxes[1].setCurrentIndex(2)
        self._apply()
        return 1

    monkeypatch.setattr(removals.WebRemovalsDialog, "exec", answer)
    assert removals.ask_about_web_removals(None, ENTRIES) == {("990000002::img::3", "L1"): "keep"}


def _prompting(removals, host, monkeypatch, pending, decide=None):
    """pending_web_removals() returns `pending`; the prompt answers `decide`."""
    asked, resolved = [], []

    def ask(parent, entries):
        asked.append(list(entries))
        return dict(decide or {})

    def resolve(choices):
        resolved.append(dict(choices))
        return (sum(v == "remove" for v in choices.values()), sum(v == "keep" for v in choices.values()))

    monkeypatch.setattr(removals, "ask_about_web_removals", ask)
    monkeypatch.setattr(host.lists_mgr, "pending_web_removals", lambda: list(pending))
    monkeypatch.setattr(host.lists_mgr, "resolve_web_removals", resolve)
    host.meta_mgr = types.SimpleNamespace(
        get_meta_for_id=lambda sid: ("T-S 1.1" if sid == "990000001" else "", "a title"))
    return asked, resolved


def _auto_done(host):
    host._on_lists_auto_done({"upload": {"success": True}})


def test_the_removal_prompt_lists_what_the_website_removed_and_applies_each_choice(gui, removals, monkeypatch, lang):
    host = gui()
    _signed_in(host)
    list_id = host.list_ids[0]
    host.lists_mgr.add_item("990000002", list_id, img="3")
    pending = [("990000001", list_id), ("990000002::img::3", list_id)]
    asked, resolved = _prompting(removals, host, monkeypatch, pending,
                                 decide={pending[0]: "remove", pending[1]: "keep"})

    _auto_done(host)

    assert asked == [[("990000001", list_id, "T-S 1.1", "Local list"),
                      ("990000002::img::3", list_id, f"990000002, {tr('Page')} 3", "Local list")]]
    assert resolved == [{pending[0]: "remove", pending[1]: "keep"}]
    assert host.refreshed and host._lists_sync.names().count("request_auto") == 1
    assert status(host) == tr("Removed from this computer: {}. To be added back on the website: {}.").format(1, 1)
    if lang == "he":
        assert _is_hebrew(_first_letter(status(host)))


def test_closing_the_removal_prompt_decides_nothing(gui, removals, monkeypatch):
    host = gui()
    _signed_in(host)
    asked, resolved = _prompting(removals, host, monkeypatch, [("990000001", host.list_ids[0])], decide={})
    _auto_done(host)
    assert len(asked) == 1 and resolved == []
    assert "request_auto" not in host._lists_sync.names()


def test_a_remove_only_answer_asks_for_an_upload(gui, removals, monkeypatch):
    host = gui()
    _signed_in(host)
    entry = ("990000001", host.list_ids[0])
    _prompting(removals, host, monkeypatch, [entry], decide={entry: "remove"})
    _auto_done(host)
    assert host._lists_sync.names().count("request_auto") == 1


def test_the_removal_prompt_is_not_repeated_after_auto_syncs_but_is_after_sync_lists_now(gui, removals, monkeypatch):
    host = gui()
    _signed_in(host)
    entry = ("990000001", host.list_ids[0])
    asked, resolved = _prompting(removals, host, monkeypatch, [entry], decide={})   # decide later
    _auto_done(host)
    _auto_done(host)
    assert len(asked) == 1, "an automatic sync offered the same entry again"
    host._do_sync_action(DIALOG, "upload")
    host._lists_sync.finish(host._lists_sync.jobs[-1], {"upload": {"success": True}})
    assert len(asked) == 2, "a manual sync did not offer the entry still pending"


@pytest.mark.parametrize("when", ["sign-out", "shutting-down", "close-pending", "close-waiting-for-prompt"])
def test_the_removal_prompt_is_not_shown_during_sign_out_or_close(gui, removals, monkeypatch, when):
    host = gui()
    _signed_in(host)
    entry = ("990000001", host.list_ids[0])
    asked, _ = _prompting(removals, host, monkeypatch, [entry], decide={})
    if when == "sign-out":
        host._do_logout()
    elif when == "shutting-down":
        host._app_shutting_down = True
    elif when == "close-pending":
        host._close_pending = True
    else:
        host._close_waiting_for_prompt = True
    _auto_done(host)
    pump(0.3)
    assert asked == [], f"the removal prompt opened during {when}"
    if when.startswith("close"):
        setattr(host, "_close_pending" if when == "close-pending" else "_close_waiting_for_prompt", False)
        pump(0.3)                           # the close was cancelled: the held offer opens
        assert len(asked) == 1
    elif when == "sign-out":
        host._lists_sync.finish(host._lists_sync.last("logout"), {"upload": {"success": True}})
        pump(0.3)
        assert asked == [], "the offer held from before the sign-out opened after it"


@pytest.mark.parametrize("sync", ["auto", "manual", "startup"])
@pytest.mark.parametrize("blocked", ["hidden", "restoring", "modal-open", "close-pending",
                                     "close-waiting-for-prompt"])
def test_nothing_sync_related_appears_before_the_window_is_shown_or_over_the_restore_question(
        gui, removals, monkeypatch, sync, blocked):
    host = gui()
    entry = ("990000001", host.list_ids[0])
    asked, _ = _prompting(removals, host, monkeypatch, [entry], decide={})
    set_blocked = {
        "hidden": lambda on: host.screen.__setitem__("visible", not on),
        "restoring": lambda on: setattr(host, "_restoring_session", on),
        "modal-open": lambda on: host.screen.__setitem__("modal", object() if on else None),
        "close-pending": lambda on: setattr(host, "_close_pending", on),
        "close-waiting-for-prompt": lambda on: setattr(host, "_close_waiting_for_prompt", on),
    }[blocked]
    set_blocked(True)
    if sync == "startup":
        host._restore_session = lambda: None
        host._restore_session_then_lists_sync()
        host._lists_sync.finish(host._lists_sync.last("preview"), {"preview": _preview(2)})
        _auto_done(host)
    elif sync == "manual":
        _signed_in(host)
        host._do_sync_action(DIALOG, "upload")
        host._lists_sync.finish(host._lists_sync.jobs[-1], {"upload": {"success": True}})
    else:
        _signed_in(host)
        _auto_done(host)
    pump(0.3)
    assert asked == [] and host.dialogs == [], f"a sync question opened while {blocked}"

    set_blocked(False)
    pump(0.3)
    assert len(asked) == 1, "the held removal prompt did not open once the screen was free"
    if sync == "startup":
        assert len(host.dialogs) == 1, "the held sync choice did not open once the screen was free"
    pump(0.2)
    assert len(asked) == 1 and len(host.dialogs) == (1 if sync == "startup" else 0)


def test_no_sync_text_says_saved_while_lists_cannot_be_saved_removal_prompt_held(gui, removals, monkeypatch):
    host = gui()
    _signed_in(host)
    entry = ("990000001", host.list_ids[0])
    asked, _ = _prompting(removals, host, monkeypatch, [entry], decide={})
    failing = {"on": True}
    monkeypatch.setattr(host.lists_mgr, "saves_failing", lambda: failing["on"])
    _auto_done(host)
    pump(0.3)
    assert asked == [], "the prompt (\"until you choose they stay here\") opened while nothing is saved"
    assert host._web_removal_timer is None, "a held offer keeps looking while saves fail"
    failing["on"] = False                           # a later edit's save landed
    _auto_done(host)
    pump(0.2)
    assert len(asked) == 1
    _auto_done(host)
    assert len(asked) == 1, "offered more than once"


# ---------------------------------------------------------------------------
# The window's own ListsSyncRunner (desktop/lists_sync_runner.py), not a recording one
# ---------------------------------------------------------------------------

NOTES_DIFFER_HINT = ("Some notes differ from your account and were not uploaded. To keep both "
                     "versions, use Sync lists now, then Merge Both.")
UPLOAD_STOPPED = ("Upload stopped. The rest of your changes are saved on this computer but have not "
                  "reached your account yet.")


def _inline_runner(host):
    """The real runner, every stage on this thread (the window's handlers run as they return)."""
    from desktop.lists_sync_runner import ListsSyncRunner
    host._lists_sync = ListsSyncRunner(host.lists_mgr, parent=host, on_auto_done=host._on_lists_auto_done,
                                       inline=True)
    return host._lists_sync


def test_the_window_builds_its_runner_with_the_automatic_upload_handler(gui, monkeypatch):
    """_lists_sync_runner() as the app calls it: a real runner, its drain timer owned by the window,
    whose automatic upload ends in _on_lists_auto_done (the removal prompt and the notes hints)."""
    from desktop.lists_sync_runner import ListsSyncRunner
    host = gui()
    host._lists_sync = None                          # nothing injected: the window makes its own
    _signed_in(host)
    uploads = []

    def sync_to_cloud(data=None, **kw):              # the network half, on the runner's worker
        uploads.append((data is not None and data is not host.lists_mgr.data,
                        threading.current_thread() is not threading.main_thread()))
        return {"success": True, "lists_pushed": 1, "items_pushed": 1, "notes_differing": 1,
                "notes_too_long": 0}
    monkeypatch.setattr(host.lists_mgr, "sync_to_cloud", sync_to_cloud)

    host._lists_auto_sync()                          # a list change: the first use builds the runner
    runner = host._lists_sync
    try:
        assert type(runner) is ListsSyncRunner
        assert runner.on_auto_done == host._on_lists_auto_done
        assert runner._timer is not None and runner._timer.parent() is host
        end = time.monotonic() + 5
        while status(host) != tr(NOTES_DIFFER_HINT) and time.monotonic() < end:
            pump(0.05)
        assert uploads == [(True, True)], "the upload did not run once, on a copy, off the UI thread"
        assert status(host) == tr(NOTES_DIFFER_HINT), "the automatic upload's end never reached the window"
    finally:
        runner.shutdown()


def test_a_manual_upload_the_sign_out_deadline_stops_says_upload_stopped(gui, monkeypatch):
    """A sign-out while a manual upload runs holds it to the sign-out's deadline: it stops, not
    cancelled ({'stopped', 'deadline'}), and the status bar says so -- not a sync error."""
    host = gui()
    _signed_in(host)
    runner = _inline_runner(host)
    host.LOGOUT_SYNC_BUDGET_S = 0.01
    checks = []

    def sync_to_cloud(data=None, should_stop=None, **kw):
        host._do_logout()                            # the user signs out while it runs
        time.sleep(0.05)                             # past the sign-out's deadline
        checks.append(should_stop())                 # the engine asks before its next request
        return {"success": False, "stopped": True, "error": "Sync stopped", "lists_pushed": 0,
                "items_pushed": 0, "items_failed": 0, "lists_not_uploaded": ["Local list"]}
    monkeypatch.setattr(host.lists_mgr, "sync_to_cloud", sync_to_cloud)

    host._do_sync_action(DIALOG, "upload")

    assert checks == [True]
    assert status(host) == tr(UPLOAD_STOPPED)
    assert _notice(host) == ("warning", [tr(P17)])   # the one notice is the sign-out's
    assert not host._logout_pending and not runner.busy


@pytest.mark.parametrize("action", ["download", "upload", "merge"])
def test_a_sync_that_fails_before_its_first_half_says_why(gui, monkeypatch, action):
    """A job whose start raises ends with an 'error' and no half: the notice gives that error."""
    host = gui()
    _signed_in(host)
    _inline_runner(host)

    def broken(*a, **k):
        raise RuntimeError("the lists could not be read")
    monkeypatch.setattr(host.lists_mgr, "begin_upload" if action == "upload" else "remembered_row_ids", broken)

    host._do_sync_action(DIALOG, action)

    assert host.notices == [("warning", tr("Sync Error"), "the lists could not be read")]


def test_a_sign_out_whose_upload_could_not_start_says_the_changes_did_not_upload(gui, monkeypatch):
    """The sign-out's own upload (nothing synced in the last minute) fails to start: the job
    ends in an error while nothing is marked unsent, and the notice says string 17."""
    host = gui()
    _signed_in(host)
    runner = _inline_runner(host)

    def broken(*a, **k):
        raise RuntimeError("no copy of the lists")
    monkeypatch.setattr(host.lists_mgr, "begin_upload", broken)
    assert not runner.unsent and runner._logout_needs_upload()

    host._do_logout()

    assert not host._logout_pending
    assert _notice(host) == ("warning", [tr(P17)])
