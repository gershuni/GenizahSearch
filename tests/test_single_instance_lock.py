# -*- coding: utf-8 -*-
"""Only one desktop copy runs per data folder, and the language-change restart
hands over to the new copy only after the old one has saved and exited.

Two copies each keep a full in-memory lists/settings state and every save
writes one copy's whole state over the shared files, so the last save wins.
The lock is a QLockFile at INDEX_DIR/app.lock, taken in genizah_app's
__main__ after the headless self-test branches and before GenizahGUI().
"""
import ast
import logging
import os
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Holds the lock for argv[3] seconds after printing its own pid (not Popen's:
# a venv launcher would make those differ).
HOLDER = r'''
import os, sys, time
sys.path.insert(0, sys.argv[1])
from desktop import single_instance as si
lock, other = si.acquire_instance_lock(sys.argv[2], wait_ms=0)
print("HELD" if lock is not None else "NOT HELD", os.getpid(), flush=True)
time.sleep(float(sys.argv[3]))
'''


@pytest.fixture(autouse=True)
def personal_state(tmp_path, monkeypatch):
    from genizah_core import Config
    from shared import lists_manager as lm
    monkeypatch.setattr(Config, "INDEX_DIR", str(tmp_path))
    monkeypatch.setattr(Config, "SESSION_FILE", str(tmp_path / "session.json"))
    monkeypatch.setattr(Config, "CONFIG_FILE", str(tmp_path / "config.pkl"))
    monkeypatch.setattr(Config, "LANGUAGE_FILE", str(tmp_path / "lang.pkl"))
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))


def _start_holder(index_dir, seconds):
    proc = subprocess.Popen([sys.executable, "-c", HOLDER, str(REPO), str(index_dir), str(seconds)],
                            stdout=subprocess.PIPE, text=True)
    state, pid = proc.stdout.readline().split()
    assert state == "HELD"
    return proc, int(pid)


def _stop(proc):
    proc.kill()
    proc.wait(10)


def test_a_second_copy_is_refused_until_the_first_is_gone(tmp_path):
    from desktop import single_instance as si

    first, _ = _start_holder(tmp_path, 60)
    try:
        lock, other_running = si.acquire_instance_lock(str(tmp_path), wait_ms=500)
        assert lock is None and other_running is True

        _stop(first)  # a crash: nothing unlocks, the lock file stays behind
        assert (tmp_path / si.LOCK_FILE_NAME).exists()
        lock, other_running = si.acquire_instance_lock(str(tmp_path), wait_ms=500)
        assert lock is not None and other_running is False
        lock.unlock()
    finally:
        _stop(first)


def test_a_relaunched_copy_waits_for_its_parent_and_for_no_one_else(tmp_path):
    """The old copy can outlive its window by more than any fixed wait (an
    upload in flight is joined at interpreter exit). The new copy names it on
    its command line and waits for exactly that process; any other holder is
    refused after the normal wait."""
    from desktop import single_instance as si

    holder, holder_pid = _start_holder(tmp_path, 5)
    try:
        started = time.monotonic()
        lock, other_running = si.acquire_instance_lock(
            str(tmp_path), wait_ms=1000,
            parent_pid=si.restarted_from(["genizah_app.py", f"--restarted-from={os.getpid()}"]))
        assert lock is None and other_running is True, "a copy that is not our parent was waited for"
        assert time.monotonic() - started < 4, "the normal wait ran long"

        lock, other_running = si.acquire_instance_lock(
            str(tmp_path), wait_ms=1000, parent_wait_ms=30000,  # bounded, so a regression cannot hang CI
            parent_pid=si.restarted_from(["genizah_app.py", f"--restarted-from={holder_pid}"]))
        assert other_running is False and lock is not None, \
            "the relaunched copy gave up while its parent was still exiting"
        assert holder.poll() is not None, "the lock was taken while the parent still held it"
        lock.unlock()
    finally:
        _stop(holder)


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


def _seconds_until_refused(monkeypatch, lock_info, parent_pid):
    """Run acquire_instance_lock with its default waits against a copy that
    never lets go, on a fake clock; return how long it waited before refusing."""
    from PyQt6.QtCore import QLockFile
    from desktop import single_instance as si

    clock = _FakeClock()

    class _NeverFreed:
        LockError = QLockFile.LockError

        def __init__(self, path):
            pass

        def tryLock(self, timeout_ms):
            clock.now += max(timeout_ms, 1) / 1000
            return False

        def error(self):
            return QLockFile.LockError.LockFailedError

        def getLockInfo(self):
            return lock_info

    monkeypatch.setattr(si, "QLockFile", _NeverFreed)
    monkeypatch.setattr(si, "time", clock)
    lock, other_running = si.acquire_instance_lock("unused", parent_pid=parent_pid)
    assert lock is None and other_running is True
    return clock.now


def test_the_default_waits_are_three_seconds_and_several_minutes_for_the_parent(monkeypatch):
    """__main__ passes neither wait, so these defaults are what users get: a
    launch waits 3 s for a copy that is closing; a relaunched copy waits for
    the copy that relaunched it for minutes (an upload in flight can keep it
    alive long after its window closed), but for no one else."""
    parent = 4242
    launch = _seconds_until_refused(monkeypatch, (True, parent, "host", "app"), parent_pid=None)
    assert launch == pytest.approx(3.0, abs=0.01)
    for_parent = _seconds_until_refused(monkeypatch, (True, parent, "host", "app"), parent_pid=parent)
    assert 5 * 60 <= for_parent <= 15 * 60, for_parent
    for_other = _seconds_until_refused(monkeypatch, (True, parent + 1, "host", "app"), parent_pid=parent)
    assert for_other == pytest.approx(3.0, abs=0.01)


def test_a_lock_holder_that_cannot_be_read_is_not_taken_for_the_parent(monkeypatch):
    """getLockInfo() returns (ok, pid, hostname, appname), and the pid means
    nothing when ok is False."""
    parent = 4242
    waited = _seconds_until_refused(monkeypatch, (False, parent, "", ""), parent_pid=parent)
    assert waited == pytest.approx(3.0, abs=0.01), "an unreadable lock file was waited on as the parent"


def test_a_lock_that_cannot_be_created_lets_the_app_run_unguarded(tmp_path):
    """No lock file possible (a missing or read-only folder): the app starts
    without the guard rather than refusing to start at all."""
    from desktop import single_instance as si

    records = []
    handler = logging.Handler(logging.WARNING)
    handler.emit = records.append
    si.LOGGER.addHandler(handler)
    try:
        started = time.monotonic()
        lock, other_running = si.acquire_instance_lock(str(tmp_path / "missing" / "folder"))
    finally:
        si.LOGGER.removeHandler(handler)

    assert (lock, other_running) == (None, False)
    assert time.monotonic() - started < 2
    assert any("unguarded" in r.getMessage() for r in records)


def test_the_restart_command_line_names_this_process_once():
    from desktop import single_instance as si

    argv = si.restart_argv("python.exe", ["genizah_app.py", "--restarted-from=111", "-x"], 222)
    assert argv == ["python.exe", "genizah_app.py", "-x", "--restarted-from=222"]
    assert si.restarted_from(argv) == 222
    assert si.restarted_from(["genizah_app.py"]) is None
    assert si.restarted_from(["genizah_app.py", "--restarted-from=junk"]) is None


def test_relaunch_happens_only_when_requested_and_names_this_process(monkeypatch):
    from desktop import single_instance as si

    monkeypatch.setattr(si, "_restart_requested", False)
    monkeypatch.setattr(sys, "argv", ["genizah_app.py"])
    launched = []

    def fake_popen(argv, **kwargs):
        launched.append((argv, kwargs))

    assert si.relaunch_if_requested(popen=fake_popen) is False
    si.request_restart()
    assert si.relaunch_if_requested(popen=fake_popen) is True
    assert si.relaunch_if_requested(popen=fake_popen) is False  # once per request

    assert launched == [([sys.executable, "genizah_app.py", f"--restarted-from={os.getpid()}"],
                         {"cwd": os.getcwd()})]


def _answering_yes():
    """genizah_app's QMessageBox, answering the restart question Yes."""
    from PyQt6.QtWidgets import QMessageBox as RealBox

    class FakeBox:
        Icon, StandardButton = RealBox.Icon, RealBox.StandardButton

        def __init__(self, *args):
            pass

        def __getattr__(self, name):  # setIcon, setText, ... are no-ops
            return lambda *args: types.SimpleNamespace(setText=lambda *a: None)

        def exec(self):
            return RealBox.StandardButton.Yes

    return FakeBox


def test_language_restart_leaves_the_relaunch_to_main(monkeypatch):
    """toggle_language used to Popen the new copy BEFORE this one's closeEvent
    saved the session, so the new copy could restore a stale session (and would
    now find the lock still held)."""
    import genizah_app

    launched, quits, closes, saved = [], [], [], []
    monkeypatch.setattr(genizah_app, "QMessageBox", _answering_yes())
    monkeypatch.setattr(genizah_app, "save_language", saved.append)
    # Whether pytest's own command line could be relaunched is not the question here.
    monkeypatch.setattr(genizah_app, "relaunch_looks_possible", lambda: True)
    monkeypatch.setattr(genizah_app, "QApplication", types.SimpleNamespace(
        instance=lambda: types.SimpleNamespace(quit=lambda: quits.append(True))))
    monkeypatch.setattr(subprocess, "Popen", lambda argv, **kwargs: launched.append(argv))
    from desktop import single_instance as si
    # Recorded before toggle_language sets it, so teardown puts back False and
    # no later test in the process relaunches anything.
    monkeypatch.setattr(si, "_restart_requested", False)
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    gui.close = lambda: closes.append(True)

    gui.toggle_language("he")

    assert launched == [], "the new copy was started before this one had closed"
    assert saved == ["he"] and closes == [True]
    assert quits == [], "quit() closes the windows in no set order (see the next test)"
    assert si._restart_requested is True


@pytest.mark.gui
def test_the_language_restart_closes_the_main_window_first_and_the_loop_still_ends(monkeypatch):
    """quit() closed the top-level windows in no set order (Qt walks a hash of
    them), and when the Joins Lab went first the main window's closeEvent saved
    it as closed, so it did not reopen after the restart. Closing the main
    window runs its closeEvent while every child window is still open, and the
    event loop still ends, so __main__ goes on to relaunch. Several child
    windows, so that quit() gets the order right by chance only rarely."""
    import genizah_app
    from PyQt6.QtCore import QTimer
    from PyQt6.QtWidgets import QApplication, QDialog, QMainWindow
    from desktop import single_instance as si

    monkeypatch.setattr(si, "_restart_requested", False)
    monkeypatch.setattr(genizah_app, "QMessageBox", _answering_yes())
    monkeypatch.setattr(genizah_app, "save_language", lambda lang: None)
    monkeypatch.setattr(genizah_app, "relaunch_looks_possible", lambda: True)
    seen = []

    class Main(QMainWindow):
        toggle_language = genizah_app.GenizahGUI.toggle_language

        def closeEvent(self, event):
            # GenizahGUI's saves join_lab.open from the Lab's visibility here.
            seen.append(("the main window closes, children open:",
                         sum(child.isVisible() for child in children)))
            super().closeEvent(event)

    app = QApplication.instance()
    main = Main()
    main.show()
    children = [QDialog(main) for _ in range(4)]    # the Joins Lab is a non-modal child dialog
    for child in children:
        child.setModal(False)
        child.show()
    watchdog = QTimer()
    watchdog.setSingleShot(True)
    watchdog.timeout.connect(lambda: (seen.append("the event loop did not end"), app.exit(1)))
    watchdog.start(5000)
    QTimer.singleShot(0, lambda: main.toggle_language("he"))
    code = app.exec()
    watchdog.stop()

    assert seen == [("the main window closes, children open:", 4)]
    assert code == 0 and si._restart_requested is True


def _recording_box(shown):
    """genizah_app's QMessageBox: each box it runs appends (title, text, OK
    label) to `shown` and answers Yes."""
    from PyQt6.QtWidgets import QMessageBox as RealBox

    class Box:
        Icon, StandardButton = RealBox.Icon, RealBox.StandardButton

        def __init__(self, *args):
            self.title = self.text = self.ok_label = None

        def setWindowTitle(self, title):
            self.title = title

        def setText(self, text):
            self.text = text

        def button(self, which):
            def set_label(label):
                if which == RealBox.StandardButton.Ok:
                    self.ok_label = label
            return types.SimpleNamespace(setText=set_label)

        def __getattr__(self, name):  # setIcon, setStandardButtons
            return lambda *args: None

        def exec(self):
            shown.append((self.title, self.text, self.ok_label))
            return RealBox.StandardButton.Yes

    return Box


def _is_hebrew(ch):
    return "֐" <= ch <= "׿"


def test_relaunch_looks_possible_only_when_what_it_would_run_is_there(tmp_path, monkeypatch):
    from desktop import single_instance as si

    exe = tmp_path / "python.exe"
    exe.write_bytes(b"")
    script = tmp_path / "genizah_app.py"
    script.write_text("", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert si.relaunch_looks_possible(str(exe), ["genizah_app.py"], frozen=False) is True
    assert si.relaunch_looks_possible(str(exe), [str(script)], frozen=False) is True
    assert si.relaunch_looks_possible(str(tmp_path / "gone.exe"), ["genizah_app.py"], frozen=False) is False
    assert si.relaunch_looks_possible("", ["genizah_app.py"], frozen=False) is False
    assert si.relaunch_looks_possible(str(exe), ["moved_away.py"], frozen=False) is False
    assert si.relaunch_looks_possible(str(exe), [], frozen=False) is False
    # A frozen EXE is the program itself: there is no script to find.
    assert si.relaunch_looks_possible(str(exe), [str(exe)], frozen=True) is True

    def no_working_folder():
        raise FileNotFoundError(2, "The system cannot find the file specified")

    monkeypatch.setattr(os, "getcwd", no_working_folder)
    assert si.relaunch_looks_possible(str(exe), ["genizah_app.py"], frozen=False) is False


def test_a_restart_that_cannot_start_keeps_the_window_open(monkeypatch):
    """The relaunch runs after the window has closed and the event loop has
    ended, so a command that cannot start then leaves nothing running. Asked
    first, the answer keeps this window open, and the saved choice applies at
    the next start."""
    import genizah_app
    import genizah_core
    from desktop import single_instance as si

    monkeypatch.setattr(si, "_restart_requested", False)
    shown, saved, closes, launched, labels = [], [], [], [], []
    monkeypatch.setattr(genizah_app, "QMessageBox", _recording_box(shown))
    monkeypatch.setattr(genizah_app, "save_language", saved.append)
    monkeypatch.setattr(genizah_app, "relaunch_looks_possible", lambda: False)
    monkeypatch.setattr(subprocess, "Popen", lambda argv, **kwargs: launched.append(argv))
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    gui.close = lambda: closes.append(True)
    gui.lang_btn = types.SimpleNamespace(setText=labels.append)

    gui.toggle_language("he")

    assert si._restart_requested is False, "a restart that cannot start was requested"
    assert closes == [] and launched == [], "the window closed with nothing to follow it"
    assert saved == ["he"]
    tr = genizah_core.tr
    assert len(shown) == 2  # the question, then the notice
    assert shown[1] == (
        tr("Could not restart"),
        tr("The application cannot restart itself right now. The language will change "
           "to {} the next time you start it.").format("עברית"),
        tr("OK"))
    assert labels == ["English"], "the button still offers the language just chosen"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_a_relaunch_that_fails_tells_the_user_to_start_the_app_again(monkeypatch, lang):
    """The old window has closed and its event loop has ended: if the new copy
    cannot start, this one exits and nothing is running. Only a log line said
    so. The QApplication still exists, so a modal box can run."""
    import genizah_app
    import genizah_core
    from desktop import single_instance as si

    monkeypatch.setattr(si, "_restart_requested", False)
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    shown = []
    monkeypatch.setattr(genizah_app, "QMessageBox", _recording_box(shown))

    def popen(argv, **kwargs):
        raise FileNotFoundError(2, "The system cannot find the file specified", argv[0])

    si.request_restart()
    assert si.relaunch_if_requested(popen=popen, on_failure=genizah_app._report_failed_relaunch) is False

    tr = genizah_core.tr
    assert shown == [(tr("Could not restart"),
                      tr("The application could not restart itself. Start it again to use "
                         "the new language."),
                      tr("OK"))]
    if lang == "he":
        assert all(_is_hebrew(part[0]) for part in shown[0])


# ---------------------------------------------------------------------------
# genizah_app's __main__
# ---------------------------------------------------------------------------

def _main_block():
    tree = ast.parse((REPO / "genizah_app.py").read_text(encoding="utf-8"))
    return next(n for n in tree.body
                if isinstance(n, ast.If) and ast.unparse(n.test) == "__name__ == '__main__'").body


def _first(body, pred, what):
    hits = [i for i, node in enumerate(body) if pred(node)]
    assert hits, f"__main__ has no {what}"
    return hits[0]


def _calls(node, func):
    value = getattr(node, "value", None)
    return isinstance(value, ast.Call) and ast.unparse(value.func) == func


def _lock_statement(body):
    return _first(body, lambda n: isinstance(n, ast.Assign) and _calls(n, "acquire_instance_lock"),
                  "acquire_instance_lock(...) bound to names")


def test_main_takes_the_lock_after_the_self_tests_and_before_the_window():
    body = _main_block()
    self_tests = [
        _first(body, lambda n, f=flag: isinstance(n, ast.If) and f in ast.unparse(n.test), flag)
        for flag in ("--telemetry-selftest", "--self-test-pymupdf", "--self-test-imports")]
    qapp = _first(body, lambda n: isinstance(n, ast.Assign) and _calls(n, "QApplication"), "QApplication(...)")
    lock = _lock_statement(body)
    gui = _first(body, lambda n: isinstance(n, ast.Assign) and _calls(n, "GenizahGUI"), "GenizahGUI()")

    assert max(self_tests) < lock, "the lock would block the headless self-test runs"
    assert qapp < lock < gui, "the refusal message needs a QApplication and no window yet"
    call = body[lock].value
    assert [ast.unparse(a) for a in call.args] == ["Config.INDEX_DIR"]
    assert {k.arg: ast.unparse(k.value) for k in call.keywords} == {"parent_pid": "restarted_from(sys.argv)"}
    target = body[lock].targets[0]
    assert isinstance(target, ast.Tuple) and all(isinstance(e, ast.Name) for e in target.elts)
    other_running = target.elts[1].id
    refusals = [n for n in body[lock + 1:gui]
                if isinstance(n, ast.If) and ast.unparse(n.test) == other_running]
    assert refusals, "a second copy must be refused before the window is built"
    refusal = ast.unparse(refusals[0])
    # A box whose OK button reads tr("OK") (QMessageBox.information's is English).
    assert "sys.exit(0)" in refusal and "_show_ok_notice(None, 'information'" in refusal
    assert "setLayoutDirection(Qt.LayoutDirection.RightToLeft)" in refusal


def test_the_refusal_message_is_translated_hebrew_first():
    from shared.genizah_translations import TRANSLATIONS

    body = _main_block()
    lock = _lock_statement(body)
    messages = [n.args[0].value for n in ast.walk(ast.Module(body=body[lock:], type_ignores=[]))
                if isinstance(n, ast.Call) and ast.unparse(n.func) == "tr"
                and n.args and isinstance(n.args[0], ast.Constant)]
    assert "Already running" in messages
    long_texts = [m for m in messages if "already open" in m]
    assert long_texts, "no refusal message after the lock"
    for text in long_texts:
        hebrew = TRANSLATIONS.get(text, "")
        # A Hebrew label that starts with the Latin product name lays out LTR.
        assert hebrew and "֐" <= hebrew[0] <= "׿", text


def test_main_relaunches_after_the_event_loop_and_before_exiting():
    body = _main_block()
    exec_at = _first(body, lambda n: isinstance(n, ast.Assign) and _calls(n, "app.exec"), "rc = app.exec()")
    relaunch = _first(body, lambda n: isinstance(n, ast.Expr) and _calls(n, "relaunch_if_requested"),
                      "relaunch_if_requested()")
    assert exec_at < relaunch
    assert _calls(body[-1], "sys.exit") and ast.unparse(body[-1].value.args[0]) == body[exec_at].targets[0].id
    assert relaunch == len(body) - 2, "nothing may run between the relaunch and the exit"


def test_main_tells_the_user_when_the_relaunch_fails():
    body = _main_block()
    relaunch = _first(body, lambda n: isinstance(n, ast.Expr) and _calls(n, "relaunch_if_requested"),
                      "relaunch_if_requested()")
    call = body[relaunch].value
    assert {k.arg: ast.unparse(k.value) for k in call.keywords} == {"on_failure": "_report_failed_relaunch"}


def test_the_instance_lock_is_held_until_the_process_exits():
    """Bound at module level in __main__ (so it lives until interpreter exit,
    after the executor threads of an in-flight upload are joined) and never
    released, rebound or deleted there."""
    body = _main_block()
    lock = _lock_statement(body)
    lock_name = body[lock].targets[0].elts[0].id
    exec_at = _first(body, lambda n: isinstance(n, ast.Assign) and _calls(n, "app.exec"), "rc = app.exec()")
    assert lock < exec_at
    main = ast.Module(body=body, type_ignores=[])
    for node in ast.walk(main):
        if isinstance(node, ast.Delete):
            assert lock_name not in ast.unparse(node), "the instance lock is deleted in __main__"
        if isinstance(node, ast.Call) and ast.unparse(node.func) in (f"{lock_name}.unlock",):
            pytest.fail("the instance lock is unlocked in __main__")
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)) and node is not body[lock]:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = {n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)}
            assert lock_name not in names, "the instance lock is rebound in __main__"
