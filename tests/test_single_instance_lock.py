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


def test_language_restart_leaves_the_relaunch_to_main(monkeypatch):
    """toggle_language used to Popen the new copy BEFORE this one's closeEvent
    saved the session, so the new copy could restore a stale session (and would
    now find the lock still held)."""
    import genizah_app
    from PyQt6.QtWidgets import QMessageBox as RealBox

    class FakeBox:
        Icon, StandardButton = RealBox.Icon, RealBox.StandardButton

        def __init__(self, *args):
            pass

        def __getattr__(self, name):  # setIcon, setText, ... are no-ops
            return lambda *args: types.SimpleNamespace(setText=lambda *a: None)

        def exec(self):
            return RealBox.StandardButton.Yes

    launched, quits, saved = [], [], []
    monkeypatch.setattr(genizah_app, "QMessageBox", FakeBox)
    monkeypatch.setattr(genizah_app, "save_language", saved.append)
    monkeypatch.setattr(genizah_app, "QApplication", types.SimpleNamespace(
        instance=lambda: types.SimpleNamespace(quit=lambda: quits.append(True))))
    monkeypatch.setattr(subprocess, "Popen", lambda argv, **kwargs: launched.append(argv))
    from desktop import single_instance as si
    # Recorded before toggle_language sets it, so teardown puts back False and
    # no later test in the process relaunches anything.
    monkeypatch.setattr(si, "_restart_requested", False)
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)

    gui.toggle_language("he")

    assert launched == [], "the new copy was started before this one had closed"
    assert saved == ["he"] and quits == [True]
    assert si._restart_requested is True


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
    assert "sys.exit(0)" in refusal and "QMessageBox.information" in refusal
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
