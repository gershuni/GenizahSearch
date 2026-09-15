"""Parent death must stop native matching even when the worker holds the GIL."""
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil
import pytest

from shared.research_worker import protect_parent_death


def test_unsupported_platform_disables_native_matching(monkeypatch):
    monkeypatch.setattr(sys, 'platform', 'win32')
    assert protect_parent_death(os.getpid()) is False


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux kernel parent-death signal')
def test_parent_exit_kills_worker_in_native_matching(tmp_path):
    root = str(Path(__file__).resolve().parents[1])
    child = '''
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from shared.research_worker import protect_parent_death
from shared.search_regex import compile, isolated_matching
assert protect_parent_death(os.getppid())
pattern = compile('(a|aa)+$')
with isolated_matching(native=True):
    Path(sys.argv[2]).write_text(str(os.getpid()))
    pattern.search('a' * 10000 + '!')
'''
    parent = '''
import subprocess, sys, time
from pathlib import Path
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]],
                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
while not Path(sys.argv[3]).exists():
    time.sleep(0.01)
print('ready', flush=True)
sys.stdin.read(1)
'''
    ready = tmp_path / 'ready'
    proc = subprocess.Popen([sys.executable, '-c', parent, child, root, str(ready)],
                            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
    worker = None
    try:
        deadline = time.monotonic() + 10
        while not ready.exists():
            assert proc.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        worker = psutil.Process(int(ready.read_text()))
        time.sleep(0.1)
        assert worker.is_running() and worker.status() != psutil.STATUS_ZOMBIE
        proc.kill()
        proc.wait(timeout=5)
        deadline = time.monotonic() + 5
        while worker.is_running() and worker.status() != psutil.STATUS_ZOMBIE:
            assert time.monotonic() < deadline, 'native matcher survived parent death'
            time.sleep(0.02)
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        if worker is not None and worker.is_running():
            try:
                worker.kill()
            except psutil.NoSuchProcess:
                pass


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux kernel parent-death signal')
def test_parent_changed_before_protection_exits():
    root = str(Path(__file__).resolve().parents[1])
    code = ('import os, sys; sys.path.insert(0, sys.argv[1]); '
            'from shared.research_worker import protect_parent_death; '
            'protect_parent_death(-1)')
    assert subprocess.run([sys.executable, '-c', code, root], timeout=5).returncode == 1
