"""Real subprocess readers cannot overlap index swaps, including after a crash."""
from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
import time

import pytest

from shared.local_index_leases import index_leases, index_swap


def wait_for(predicate):
    deadline = time.monotonic() + 8
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.02)


@pytest.fixture
def reader_process(tmp_path):
    script = tmp_path / 'reader.py'
    script.write_text(f'''
import sys, time
sys.path[:] = {sys.path!r}
from pathlib import Path
from shared.local_index_leases import index_leases
with index_leases([sys.argv[1]]):
    with (Path(sys.argv[1]) / 'data.txt').open('r'):
        Path(sys.argv[2]).write_text('ready')
        while True:
            time.sleep(0.02)
''', encoding='utf-8')
    processes = []

    def start(index):
        ready = tmp_path / f'ready-{len(processes)}'
        process = subprocess.Popen([sys.executable, str(script), str(index), str(ready)])
        processes.append(process)
        return process, ready
    yield start
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


def test_swap_waits_for_all_process_readers_and_closes_handles(tmp_path, reader_process):
    index = tmp_path / 'LocalIndex'
    index.mkdir()
    (index / 'data.txt').write_text('content')
    first, first_ready = reader_process(index)
    wait_for(first_ready.exists)
    second, second_ready = reader_process(index)
    wait_for(second_ready.exists)  # shared readers really coexist

    class Indexer:
        _index_dir = str(index)
        _lab_index_dir = None

        @index_swap
        def swap(self):
            # Nested callbacks are reentrant; no self-deadlock on reload.
            with index_leases([index], exclusive=True):
                index.rename(tmp_path / 'old-index')
                index.mkdir()

    with ThreadPoolExecutor(1) as executor:
        future = executor.submit(Indexer().swap)
        try:
            time.sleep(0.15)
            assert not future.done()
            first.kill()
            first.wait(timeout=5)
            time.sleep(0.15)
            assert not future.done()
        finally:
            second.kill()
            second.wait(timeout=5)
        future.result(timeout=5)
    assert (tmp_path / 'old-index' / 'data.txt').read_text() == 'content'


def test_reader_waits_until_swap_releases_lease(tmp_path, reader_process):
    index = tmp_path / 'LocalIndex'
    index.mkdir()
    (index / 'data.txt').write_text('content')
    with index_leases([index], exclusive=True):
        process, ready = reader_process(index)
        time.sleep(0.2)
        assert process.poll() is None
        assert not ready.exists()
    wait_for(ready.exists)
