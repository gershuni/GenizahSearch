# -*- coding: utf-8 -*-
"""No running QThread is destroyed on the way out of the desktop app.

Destroying a QThread whose thread still runs aborts the process ("QThread: Destroyed
while thread '' is still running"; Windows 0xC0000409). desktop/qthread_exit.py
explains the two waves in which the interpreter drops objects at exit: in the first,
PyQt's exit handler destroys the windows, and with them every QThread held only by a
hidden child window's attribute or parented to it; in the second, a QThread still held
by a module-level list is left running. desktop.qthread_exit.settle_running_threads,
called by genizah_app after app.exec(), moves every thread still running into the
second wave.

Until 2026-09-29 this file ran one child whose threads had Python run() methods, and
the frame's ``self`` held each thread through the first wave unless the thread had not
yet entered run() -- a race that failed about one run in twenty (GitHub Actions run
36608081072; 5 of 100 locally). The child here is deterministic: its stuck threads are
event-loop workers blocked in a slot, so no Python frame refers to the QThread, and one
is parented to the hidden window. Without the settle step it aborts every time, which
the first test asserts: that is the check that this harness can fail.

The child is a self-contained PyQt6 script that imports only desktop.qthread_exit (no
app data folder is touched). Deliberately not gui-marked: it starts its own process, so
the main CI job runs it on every OS.
"""
import ast
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
WAIT_MS = 400

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PyQt6") is None,
                                reason="PyQt6 not installed")

_CHILD = textwrap.dedent("""
    import json
    import sys
    import time

    MODE, ROOT, WAIT_MS = sys.argv[1], sys.argv[2], int(sys.argv[3])
    sys.path.insert(0, ROOT)

    from PyQt6.QtCore import QObject, QThread, QTimer, pyqtSlot
    from PyQt6.QtWidgets import QApplication, QDialog, QMainWindow

    from desktop.qthread_exit import settle_running_threads  # at startup, as the app does

    KEEP = []


    class Blocker(QObject):
        # Keeps its thread's event loop busy: quit() and requestInterruption() cannot
        # stop it, and no Python frame refers to the QThread while it runs.
        @pyqtSlot()
        def block(self):
            end = time.monotonic() + 25
            while time.monotonic() < end:
                time.sleep(0.02)


    class Stubborn(QThread):
        pass


    class Cooperative(QThread):
        def run(self):
            while not self.isInterruptionRequested():
                self.msleep(10)


    def stubborn(parent=None):
        t = Stubborn(parent)
        worker = Blocker()
        worker.moveToThread(t)
        t.started.connect(worker.block)
        t.start()
        return t, worker


    app = QApplication(sys.argv)
    window = QMainWindow()          # module-level, as in genizah_app's __main__
    window.show()
    lab = QDialog(window)           # a closed (hidden) Joins Lab
    lab.show()
    lab.hide()
    lab._img_threads = []
    lab._workers = []
    for _ in range(3):
        t, worker = stubborn()
        lab._workers.append(worker)
        lab._img_threads.append(t)
        plain = QThread()           # runs an event loop; quit() ends it
        plain.start()
        lab._img_threads.append(plain)
        coop = Cooperative()        # ends when interruption is requested
        coop.start()
        lab._img_threads.append(coop)
    if MODE == "module":
        KEEP.extend(lab._img_threads)
        lab._img_threads.clear()
    else:
        t, worker = stubborn(parent=lab)   # held by its Qt parent only
        lab._workers.append(worker)
    del t, worker, plain, coop, lab



    def settle():
        # Inside a function: the returned list must not outlive this call, or this
        # script -- not desktop.qthread_exit -- would be what keeps the threads.
        start = time.monotonic()
        kept = settle_running_threads(WAIT_MS)
        return {
            "kept": sorted(type(k).__name__ for k in kept),
            "parented": [k.parent() is not None for k in kept],
            "elapsed_ms": (time.monotonic() - start) * 1000,
        }


    QTimer.singleShot(200, window.close)
    rc = app.exec()
    if MODE == "settle":
        print(json.dumps(settle()), flush=True)
    sys.exit(rc)
""")


def _run_child(tmp_path, mode):
    script = tmp_path / "kept_qthreads_child.py"
    script.write_text(_CHILD, encoding="utf-8")
    # QT_FORCE_STDERR_LOGGING: on Windows the fatal message otherwise goes to the
    # debugger output whenever stderr is not a console, and the abort looks silent.
    env = dict(os.environ, QT_QPA_PLATFORM="offscreen", QT_FORCE_STDERR_LOGGING="1")
    return subprocess.run(
        [sys.executable, str(script), mode, str(ROOT), str(WAIT_MS)],
        capture_output=True, text=True, timeout=60, env=env, cwd=str(tmp_path),
    )


def test_without_the_settle_step_a_running_qthread_of_a_hidden_window_aborts_the_exit(tmp_path):
    proc = _run_child(tmp_path, "none")
    assert "Destroyed while thread" in proc.stderr, (proc.returncode, proc.stderr)
    assert proc.returncode != 0, proc.stderr


def test_the_settle_step_leaves_no_running_qthread_to_be_destroyed(tmp_path):
    proc = _run_child(tmp_path, "settle")
    assert "Destroyed while thread" not in proc.stderr, proc.stderr
    assert proc.returncode == 0, (proc.returncode, proc.stderr)
    report = json.loads(proc.stdout.strip().splitlines()[-1])
    # The three plain event-loop threads stopped on quit() and the three cooperative
    # ones on requestInterruption() within the shared wait -- so the wait released the
    # GIL. Only the four blocked ones are left running, none of them parented any more.
    assert report["kept"] == ["Stubborn"] * 4, report
    assert report["parented"] == [False] * 4, report
    # They never stop, so the whole wait is spent -- and not much more.
    assert WAIT_MS * 0.9 <= report["elapsed_ms"] < WAIT_MS + 2000, report


def test_running_qthreads_held_by_a_module_level_list_are_left_running_at_exit(tmp_path):
    """The second wave, which desktop.gui_threads._ORPHANED_WORKERS relies on too.

    The same unparented threads as above, held by a module-level list instead of the
    hidden window, and no settle step: none is destroyed."""
    proc = _run_child(tmp_path, "module")
    assert "Destroyed while thread" not in proc.stderr, proc.stderr
    assert proc.returncode == 0, (proc.returncode, proc.stderr)


def test_genizah_app_settles_running_threads_after_the_event_loop():
    """The call site: unconditional, right after app.exec() and before the relaunch.

    tests/test_single_instance_lock.py pins the relaunch as the last step before
    sys.exit(), so a relaunched copy never waits on anything but this one's exit."""
    tree = ast.parse((ROOT / "genizah_app.py").read_text(encoding="utf-8"))
    main_block = next(
        n for n in tree.body
        if isinstance(n, ast.If) and ast.unparse(n.test) == "__name__ == '__main__'")
    body = [ast.unparse(s) for s in main_block.body]
    exec_at = body.index("_exit_code = app.exec()")
    assert body[exec_at + 1] == "settle_running_threads()", body[exec_at:exec_at + 3]
    assert body[exec_at + 2].startswith("relaunch_if_requested("), body[exec_at:exec_at + 3]
