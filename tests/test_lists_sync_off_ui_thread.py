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
import sys
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
        host.corrections_client = types.SimpleNamespace(
            current_user=types.SimpleNamespace(_uuid=USER, username="reader"), _client=object())
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
    for host in hosts:  # no timer of one test fires in the next
        getattr(host, "_drop_held_lists_dialog", lambda: None)()
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
