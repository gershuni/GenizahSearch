"""Real subprocess lifecycle tests, without loading the research corpus."""
import asyncio
import os
from pathlib import Path
import sys
import time
import subprocess

import psutil
import pytest

from web.research_jobs import ResearchJobError, ResearchQueue, run_research_call


@pytest.fixture
def queue(tmp_path, monkeypatch):
    monkeypatch.setenv('GENIZAH_RESEARCH_WORKERS', '1')
    monkeypatch.setenv('GENIZAH_RESEARCH_QUEUE_SIZE', '2')
    monkeypatch.setenv('GENIZAH_WEB_RESERVE_MB', '128')
    script = tmp_path / 'worker.py'
    script.write_text('''
import gzip, json, os, pickle, sys, time
from pathlib import Path
root = Path(sys.argv[1])
request = pickle.loads((root / 'input.pkl').read_bytes())
(root / 'progress.json').write_text(json.dumps({'status': 'Searching', 'progress': [1, 2]}))
if request.get('crash'):
    os._exit(9)
if request.get('block'):
    while True:
        time.sleep(0.01)
encoded = pickle.dumps({'value': request, 'pid': os.getpid()})
if request.get('compressed'):
    with gzip.open(root / 'output.pkl', 'wb') as stream:
        stream.write(encoded)
    (root / 'output-size.json').write_text(json.dumps({'expanded_bytes': len(encoded)}))
else:
    (root / 'output.pkl').write_bytes(encoded)
''', encoding='utf-8')
    manager = ResearchQueue(command=[sys.executable, str(script)])
    yield manager
    manager.close()


def wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'worker did not reach expected state'
        time.sleep(0.02)


def test_executes_in_another_process_and_preserves_results(queue):
    data = {'query': 'ראובן AND שמעון', 'ids': {'a', 'b'}}
    result = queue.submit(data).future.result(timeout=5)
    assert result['pid'] != os.getpid()
    assert result['value'] == data


def test_streamed_compressed_results_preserve_transcriptions(queue):
    data = {'compressed': True, 'transcription': 'ראובן שמעון ' * 10000}
    assert queue.submit(data).future.result(timeout=5)['value'] == data


@pytest.mark.parametrize('kind', ['search', 'lab', 'passage'])
def test_worker_receives_selected_settings_snapshot(queue, monkeypatch, kind):
    from types import SimpleNamespace
    from shared.lab_settings import LabSettings
    from shared.variants import VariantManager
    from shared.research_worker import restore_settings
    from web.research_jobs import IsolatedEngine
    monkeypatch.setattr(LabSettings, 'load', lambda self: None)
    settings = LabSettings()
    settings.variant_pairs_count = 123
    settings.variant_max_changes = 1
    settings.custom_variants = {'א=ת': True}
    settings.gap_penalty = 7
    variants = VariantManager(settings)
    engine = SimpleNamespace(execute_search=lambda query_str, mode, gap: None)
    if kind == 'lab':
        engine.settings = settings
    elif kind == 'passage':
        engine.text_fetcher = SimpleNamespace(var_mgr=variants)
    else:
        engine.var_mgr = variants
    monkeypatch.setattr('web.research_jobs.get_queue', lambda: queue)
    submit = queue.submit

    def submit_then_change_live_settings(payload):
        job = submit(payload)
        settings.variant_pairs_count = 2
        settings.variant_max_changes = 4
        settings.custom_variants.clear()
        return job
    monkeypatch.setattr(queue, 'submit', submit_then_change_live_settings)
    received = IsolatedEngine(engine, kind).execute_search('אבג', 'variants', 0)
    restored = restore_settings(received['settings'])
    worker_variants = VariantManager(restored)
    assert worker_variants._get_pairs_count() == 123
    assert worker_variants._get_max_changes_for_length(8, 4) == 1
    assert restored.custom_variants == {'א=ת': True}
    assert restored.gap_penalty == 7


def test_stream_writer_bounds_expanded_size():
    import io
    from shared.research_worker import MeasuredWriter
    writer = MeasuredWriter(io.BytesIO(), 10)
    writer.write(b'12345')
    with pytest.raises(ValueError, match='Expanded search output'):
        writer.write(b'123456')


def test_fifo_queue_and_queued_cancellation(queue):
    first = queue.submit({'block': True})
    wait_for(lambda: first.status == 'Searching')
    second = queue.submit({'query': 'second'})
    third = queue.submit({'query': 'third'})
    assert queue.position(second) == 1
    assert queue.position(third) == 2
    with pytest.raises(ResearchJobError, match='queue is full'):
        queue.submit({})
    queue.cancel(second)
    with pytest.raises(InterruptedError):
        second.future.result(timeout=2)
    assert second.process is None
    assert queue.position(third) == 1
    queue.cancel(first)
    with pytest.raises(InterruptedError):
        first.future.result(timeout=5)
    assert third.future.result(timeout=5)['value']['query'] == 'third'


def test_stop_kills_busy_process_before_reusing_slot(queue):
    job = queue.submit({'block': True})
    wait_for(lambda: job.status == 'Searching')
    pid = job.process.pid
    queue.cancel(job)
    with pytest.raises(InterruptedError):
        job.future.result(timeout=5)
    assert not psutil.pid_exists(pid)
    assert queue.submit({'next': True}).future.result(timeout=5)['value']['next']


def test_memory_watchdog_kills_worker_and_queue_recovers(queue):
    job = queue.submit({'block': True})
    wait_for(lambda: job.status == 'Searching')
    pid = job.process.pid
    # Lower the allowance below this real worker's resident memory; avoid
    # allocating hundreds of megabytes just to exercise the same watchdog.
    queue.memory_mb = 1
    with pytest.raises(ResearchJobError, match='memory allowance'):
        job.future.result(timeout=5)
    assert not psutil.pid_exists(pid)
    queue.memory_mb = 2048
    assert queue.submit({}).future.result(timeout=5)['value'] == {}


def test_crashed_worker_does_not_poison_next_job(queue):
    with pytest.raises(ResearchJobError, match='unexpectedly'):
        queue.submit({'crash': True}).future.result(timeout=5)
    assert queue.submit({'ok': True}).future.result(timeout=5)['value']['ok']


def test_waiting_for_memory_can_be_cancelled_without_starting_process(queue, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(psutil, 'virtual_memory', lambda: SimpleNamespace(available=0))
    job = queue.submit({})
    wait_for(lambda: job.status == 'Waiting for memory')
    queue.cancel(job)
    with pytest.raises(InterruptedError):
        job.future.result(timeout=5)
    assert job.process is None


def test_async_wait_keeps_event_loop_responsive_and_cancels_worker(queue, monkeypatch):
    from web import research_jobs
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)

    class FakeEngine:
        def execute_search(self, query_str, progress_callback=None):
            raise AssertionError('must execute in subprocess')

    engine = research_jobs.IsolatedEngine(FakeEngine(), 'search')

    # This fixture worker reads block at the top level, while the engine
    # adapter submits arguments. Adapt only the test transport payload.
    submit = queue.submit
    monkeypatch.setattr(queue, 'submit', lambda payload: submit({'block': True}))

    async def exercise():
        task = asyncio.create_task(run_research_call(lambda: engine.execute_search('query')))
        ticks = 0
        for _ in range(30):
            await asyncio.sleep(0.02)
            ticks += 1
        assert not task.done()
        with queue.condition:
            job = next(iter(queue.active))
        pid = job.process.pid
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        while not job.future.done():
            await asyncio.sleep(0.02)
        assert not psutil.pid_exists(pid)
        assert ticks == 30
    asyncio.run(exercise())


def test_website_routes_heavy_engines_through_workers():
    source = Path('web/main.py').read_text(encoding='utf-8')
    assert "IsolatedEngine(state.searcher, 'search')" in source
    assert "IsolatedEngine(state.lab_engine, 'lab')" in source


def test_progress_file_sharing_violation_does_not_abort_search(tmp_path, monkeypatch):
    from shared.research_worker import write_progress

    def locked(*args):
        raise PermissionError('progress reader holds the file')

    monkeypatch.setattr(Path, 'replace', locked)
    write_progress(tmp_path, {'status': 'Searching', 'progress': (1, 10)})


def test_result_loading_waits_for_memory_and_remains_cancellable(queue, monkeypatch):
    holder = {}

    def available():
        job = holder.get('job')
        if job and job.status == 'Waiting for memory to load results':
            return 0
        return 16 * 1024**3

    monkeypatch.setattr('web.research_jobs.available_memory', available)
    job = holder['job'] = queue.submit({'query': 'result'})
    wait_for(lambda: job.status == 'Waiting for memory to load results')
    queue.cancel(job)
    with pytest.raises(InterruptedError):
        job.future.result(timeout=5)


def test_os_allocation_limit_rejects_large_allocation_in_child(tmp_path):
    script = tmp_path / 'limit.py'
    script.write_text(
        'import sys\n'
        f'sys.path[:] = {sys.path!r}\n'
        'from shared.research_limits import limit_memory\n'
        'limit_memory(128 * 1024**2)\n'
        'try:\n'
        '    allocation = bytearray(256 * 1024**2)\n'
        'except MemoryError:\n'
        '    print("allocation refused")\n'
        'else:\n'
        '    raise AssertionError("allocation limit was not enforced")\n', encoding='utf-8')
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert 'allocation refused' in result.stdout
