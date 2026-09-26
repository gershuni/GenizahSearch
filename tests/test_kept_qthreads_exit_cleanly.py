# -*- coding: utf-8 -*-
"""Running QThreads that are still referenced when the app exits do not abort the exit.

desktop.gui_threads._keep_until_finished deliberately has no quit step: no wait and no
terminate() at aboutToQuit. That rests on this behaviour of the pinned PyQt6/sip: a
QThread still referenced at exit -- from a module-level list like the keeper, or from a
Python attribute of a hidden child window like a closed Joins Lab's loader lists -- is
not destroyed, and the process exits with code 0. If a PyQt6/sip upgrade starts
destroying such threads ("QThread: Destroyed while thread is still running", an abort),
this test fails and the keeper needs a quit step again.

The child is a self-contained PyQt6 script (no repository import, so no personal state
is touched). Deliberately not gui-marked: it starts its own process, so the main CI job
runs it on every OS.
"""
import importlib.util
import os
import subprocess
import sys
import textwrap

import pytest

_CHILD = textwrap.dedent("""
    import sys
    import time

    from PyQt6.QtCore import QThread, QTimer
    from PyQt6.QtWidgets import QApplication, QDialog, QMainWindow

    KEEP = []


    class Stuck(QThread):
        def __init__(self, sleeping):
            super().__init__()
            self._sleeping = sleeping

        def run(self):
            end = time.monotonic() + 25
            x = 0
            while time.monotonic() < end:
                if self._sleeping:
                    self.msleep(20)
                else:
                    x += 1          # busy in Python, taking the GIL


    app = QApplication(sys.argv)
    window = QMainWindow()
    window.show()
    lab = QDialog(window)           # a closed (hidden) Lab, parented to the main window
    lab.show()
    lab.hide()
    lab._img_threads = []
    for i in range(10):
        w = Stuck(i % 2 == 0)
        w.start()
        KEEP.append(w)
    for i in range(10):
        w = Stuck(i % 2 == 0)
        w.start()
        lab._img_threads.append(w)
    del w, lab
    QTimer.singleShot(200, window.close)
    sys.exit(app.exec())
""")


@pytest.mark.skipif(importlib.util.find_spec("PyQt6") is None, reason="PyQt6 not installed")
def test_running_qthreads_still_referenced_at_exit_do_not_abort_it(tmp_path):
    script = tmp_path / "kept_qthreads_child.py"
    script.write_text(_CHILD, encoding="utf-8")
    env = dict(os.environ, QT_QPA_PLATFORM="offscreen")
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True,
        timeout=20, env=env, cwd=str(tmp_path),
    )
    assert "Destroyed while thread" not in proc.stderr, proc.stderr
    assert proc.returncode == 0, (proc.returncode, proc.stderr)
