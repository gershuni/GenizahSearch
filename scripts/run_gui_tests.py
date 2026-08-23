# -*- coding: utf-8 -*-
"""Run the gui-marked tests one FILE PER PROCESS.

Why. Qt state is process-global and this suite's desktop tests leak it across
test boundaries: a widget closed with `deleteLater()` is only queued for
destruction, and a worker QThread or QTimer can post a queued call into a
widget a previous test already closed. Nothing fires until something runs an
event loop -- and then everything fires at once, on objects whose Python
wrappers are still referenced. The result is an access violation (exit 139)
whose position moves with the run: PR #324 CI died in
`test_pause_integration_qt.py` on Linux and `test_my_library_tab_prior_status_
cache.py` on Windows, and every one of those files passes when run alone.

`tests/conftest.py::_drain_qt_deferred_deletions` quiesces timers and threads
at each gui test's own boundary, which fixes the small clusters. It does not
fix the whole bucket, because the leak is not confined to one mechanism and
chasing each one is unbounded work in desktop code this change does not touch.

So: bound the blast radius instead. One process per file means a leak can only
reach the tests in its own file, which is the isolation the repo already buys
at coarser grain -- `tests`, `gui-tests`, `render-smoke-tests` and
`atlas-bake-tests` are separate jobs for exactly this reason. This is that
pattern at one finer level, not a new idea.

The real defect is a desktop widget/worker lifetime bug and belongs to the
desktop phase, with `desktop/my_library_tab.py`'s worker-finished lambda
(`lambda result: self._on_worker_finished(...)`) as the first place to look.

Exit code is non-zero if ANY file fails, and the summary lists every failing
file -- a partial run must never look like a pass.
"""
from __future__ import annotations

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def gui_files() -> list:
    """Ask pytest which files `-m gui` actually selects.

    Deliberately NOT read from `tests/conftest.py::_GUI_TEST_FILES`. That list
    only covers files conftest auto-marks; other files carry their own
    `pytest.mark.gui`, and a first version of this script read the conftest
    list and reported a confident green over 7 files out of ~34. Collecting
    through pytest is the only source of truth for what `-m gui` means, and it
    stays correct however a future file acquires the marker.
    """
    proc = subprocess.run(
        [sys.executable, '-m', 'pytest', 'tests/', '-m', 'gui',
         '--collect-only', '-q', '-p', 'no:randomly', '--no-header'],
        cwd=ROOT, capture_output=True, text=True)
    if proc.returncode not in (0, 5):
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f'gui collection failed (exit {proc.returncode})')

    names = []
    for line in proc.stdout.splitlines():
        line = line.strip().replace('\\', '/')
        if '::' not in line or not line.startswith('tests/'):
            continue
        rel = line.split('::', 1)[0]
        if rel not in names:
            names.append(rel)
    if not names:
        raise SystemExit('`-m gui` collected no test files -- refusing to '
                         'report a vacuous pass')
    return sorted(names)


def main() -> int:
    files = gui_files()
    print(f'running {len(files)} gui test file(s), one process each',
          flush=True)
    failed, crashed, passed = [], [], 0
    for name in files:
        rel = name
        proc = subprocess.run(
            [sys.executable, '-m', 'pytest', rel, '-m', 'gui', '-q',
             '-p', 'no:randomly', '--tb=short'],
            cwd=ROOT)
        code = proc.returncode
        if code == 0:
            passed += 1
        elif code == 5:
            # "no tests collected" -- the file is listed but its tests are all
            # deselected or skipped. Not a failure, but say so out loud.
            print(f'  NOTE {rel}: no gui tests collected', flush=True)
            passed += 1
        elif code in (1, 2):
            failed.append(rel)
        else:
            # 139/-11 (SIGSEGV), 0xC0000005 on Windows, etc.
            crashed.append((rel, code))

    print('\n' + '=' * 66)
    print(f'gui files: {len(files)}   ok: {passed}   '
          f'failed: {len(failed)}   crashed: {len(crashed)}')
    for rel in failed:
        print(f'  FAILED  {rel}')
    for rel, code in crashed:
        print(f'  CRASHED {rel} (exit {code})')
    print('=' * 66)
    return 1 if (failed or crashed) else 0


if __name__ == '__main__':
    raise SystemExit(main())
