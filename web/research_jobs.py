"""One FIFO queue for isolated research computations (no NiceGUI imports).

The web process owns admission and cancellation. Each computation gets a fresh
subprocess, so killing a pathological query also releases its native threads,
indexes and memory. Nothing in the child imports web.main.
"""
from __future__ import annotations

import atexit
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import inspect
import gzip
import json
import logging
import os
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import threading
import time

import psutil
from shared.research_limits import available_memory

log = logging.getLogger(__name__)
_cancel_context = ContextVar('research_cancel', default=None)
_status_context = ContextVar('research_status', default=None)


class ResearchJobError(RuntimeError):
    """A computation stopped without returning misleading partial results."""


def _integer(name, default, minimum=1, maximum=1024):
    try:
        return max(minimum, min(maximum, int(os.environ.get(name, default))))
    except (TypeError, ValueError):
        return default


@contextmanager
def job_context(cancel=None, status=None):
    first = _cancel_context.set(cancel)
    second = _status_context.set(status)
    try:
        yield
    finally:
        _status_context.reset(second)
        _cancel_context.reset(first)


@dataclass(eq=False)
class Job:
    payload: dict
    future: Future = field(default_factory=Future)
    cancel: threading.Event = field(default_factory=threading.Event)
    status: str = 'Queued'
    progress: tuple = (0, 0)
    process: subprocess.Popen | None = None


def _kill(process):
    """Kill descendants too; wait before a slot is reusable."""
    if process is None:
        return
    if process.poll() is not None:
        process.wait()
        return
    try:
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        try:
            parent.kill()
        except psutil.NoSuchProcess:
            pass
        psutil.wait_procs(children, timeout=2)
    except psutil.NoSuchProcess:
        pass
    process.wait(timeout=5)


class ResearchQueue:
    def __init__(self, *, command=None):
        self.capacity = _integer('GENIZAH_RESEARCH_WORKERS', 1, maximum=4)
        self.queue_limit = _integer('GENIZAH_RESEARCH_QUEUE_SIZE', 16, maximum=64)
        self.memory_mb = _integer('GENIZAH_RESEARCH_MEMORY_MB', 4096, minimum=128, maximum=65536)
        self.reserve_mb = _integer('GENIZAH_WEB_RESERVE_MB', 1024, minimum=128, maximum=65536)
        self.result_mb = _integer('GENIZAH_RESEARCH_RESULT_MB', 512, maximum=512)
        self.command = command or [sys.executable, '-m', 'shared.research_worker']
        self.condition = threading.Condition()
        self.pending = deque()
        self.active = set()
        self.closed = False
        self.threads = []
        for number in range(self.capacity):
            thread = threading.Thread(target=self._consume, args=(number,), name=f'research-{number}', daemon=True)
            thread.start()
            self.threads.append(thread)

    def submit(self, payload):
        # Serialize before admission to reject unserializable closures, not in
        # the consumer where they could strand a future.
        if len(pickle.dumps(payload)) > 16 * 1024 * 1024:
            raise ResearchJobError('Search input is too large for the worker queue.')
        job = Job(payload)
        with self.condition:
            if self.closed:
                raise ResearchJobError('Search service is shutting down. Please retry shortly.')
            if len(self.pending) >= self.queue_limit:
                raise ResearchJobError('The search queue is full. Please try again shortly.')
            self.pending.append(job)
            self.condition.notify_all()
        return job

    def position(self, job):
        with self.condition:
            try:
                return list(self.pending).index(job) + 1
            except ValueError:
                return 0

    def cancel(self, job):
        job.cancel.set()
        with self.condition:
            if job in self.pending:
                self.pending.remove(job)
                job.future.set_exception(InterruptedError('Search cancelled'))
            self.condition.notify_all()

    def _consume(self, slot):
        while True:
            with self.condition:
                while not self.closed and not self.pending:
                    self.condition.wait()
                if self.closed:
                    return
                job = self.pending.popleft()
                self.active.add(job)
            try:
                value = self._execute(job, slot)
                job.future.set_result(value)
            except BaseException as exc:
                job.future.set_exception(exc)
            finally:
                with self.condition:
                    self.active.discard(job)
                    self.condition.notify_all()

    def _execute(self, job, slot):
        if job.cancel.is_set():
            raise InterruptedError('Search cancelled')
        # Don't start another worker while the machine lacks even the website's
        # reserve. This wait has no time cutoff and is immediately cancellable.
        job.status = 'Waiting for memory'
        while available_memory() < (self.reserve_mb + 128 * self.capacity) * 1024**2:
            if job.cancel.wait(0.2):
                raise InterruptedError('Search cancelled')
        available_mb = available_memory() // 1024**2
        allocation_mb = min(self.memory_mb, max(128, (available_mb - self.reserve_mb) // self.capacity))
        with tempfile.TemporaryDirectory(prefix='genizah-research-') as directory:
            root = Path(directory)
            (root / 'input.pkl').write_bytes(pickle.dumps(job.payload))
            env = dict(os.environ)
            env.update(RAYON_NUM_THREADS='1', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
                       MKL_NUM_THREADS='1', GENIZAH_SEARCH_BUDGET_SECONDS='0',
                       GENIZAH_RESEARCH_PARENT=str(os.getpid()),
                       GENIZAH_RESEARCH_SLOT=str(slot),
                       GENIZAH_RESEARCH_MEMORY_MB=str(allocation_mb))
            job.status = 'Starting search worker'
            # Logs stay in the server log, never in a response containing paths
            # or corpus text. No shell, no web module entry point, no stdout pipe
            # which could fill and deadlock a verbose native library.
            job.process = subprocess.Popen(
                [*self.command, str(root)], env=env,
                cwd=str(Path(__file__).resolve().parents[1]),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                start_new_session=os.name != 'nt',
            )
            proc = psutil.Process(job.process.pid)
            try:
                while job.process.poll() is None:
                    if job.cancel.wait(0.1):
                        raise InterruptedError('Search cancelled')
                    try:
                        rss = proc.memory_info().rss + sum(
                            child.memory_info().rss for child in proc.children(recursive=True))
                        if rss > self.memory_mb * 1024**2:
                            raise ResearchJobError('Search exceeded its worker memory allowance; no complete results were returned.')
                        if available_memory() < self.reserve_mb * 1024**2:
                            raise ResearchJobError('Search stopped to preserve memory for the website. Please retry when it is less busy.')
                    except psutil.NoSuchProcess:
                        pass
                    progress = root / 'progress.json'
                    try:
                        event = json.loads(progress.read_text(encoding='utf-8'))
                        job.status = event['status']
                        job.progress = tuple(event.get('progress', (0, 0)))
                    except (OSError, ValueError, KeyError):
                        pass
                    output = root / 'output.pkl'
                    if output.exists() and output.stat().st_size > self.result_mb * 1024**2:
                        raise ResearchJobError('Search output exceeded its transfer allowance; no complete results were returned.')
                if job.cancel.is_set():
                    raise InterruptedError('Search cancelled')
                output = root / 'output.pkl'
                if job.process.returncode != 0 or not output.exists():
                    raise ResearchJobError('The search worker stopped unexpectedly. No complete results were returned.')
                output_bytes = output.stat().st_size
                if output_bytes > self.result_mb * 1024**2:
                    raise ResearchJobError('Search output exceeded its transfer allowance; no complete results were returned.')
                size_file = root / 'output-size.json'
                expanded_bytes = json.loads(size_file.read_text(encoding='utf-8'))['expanded_bytes'] if size_file.exists() else output_bytes
                if expanded_bytes > self.memory_mb * 1024**2:
                    raise ResearchJobError('Expanded search output exceeded its memory allowance; no complete results were returned.')
                # The child has exited and released its memory. Reserve room
                # for deserializing the result before allocating in the server.
                job.status = 'Waiting for memory to load results'
                while available_memory() < self.reserve_mb * 1024**2 + 2 * expanded_bytes:
                    if job.cancel.wait(0.2):
                        raise InterruptedError('Search cancelled')
                # Only our own subprocess writes this private temporary file.
                with (gzip.open(output, 'rb') if size_file.exists() else output.open('rb')) as stream:
                    result = pickle.load(stream)
                log.info('Research result transferred: %d compressed bytes, %d expanded bytes', output_bytes, expanded_bytes)
                if result.get('error'):
                    if result.get('exception') == 'NoWitnessesResolved':
                        from shared.passage_parallels import NoWitnessesResolved
                        raise NoWitnessesResolved(result['report'])
                    if result.get('validation'):
                        raise ValueError(result['error'])
                    raise ResearchJobError(result['error'])
                return result
            finally:
                _kill(job.process)
                job.process = None

    def close(self):
        with self.condition:
            self.closed = True
            for job in list(self.pending):
                self.cancel(job)
            for job in self.active:
                job.cancel.set()
            self.condition.notify_all()
        for thread in self.threads:
            thread.join(timeout=7)


_queue = None
_queue_lock = threading.Lock()
_wait_executor = None


def shutdown_research():
    """Stop child processes before the web server exits its lifespan."""
    if _queue is not None:
        _queue.close()
    if _wait_executor is not None:
        _wait_executor.shutdown(wait=False, cancel_futures=True)


def wait_executor():
    """Waiting for a subprocess must not occupy NiceGUI/browse pool threads."""
    global _wait_executor
    with _queue_lock:
        if _wait_executor is None:
            capacity = _integer('GENIZAH_RESEARCH_QUEUE_SIZE', 16, 1, 64)
            workers = _integer('GENIZAH_RESEARCH_WORKERS', 1, 1, 4)
            _wait_executor = ThreadPoolExecutor(max_workers=capacity + workers + 8, thread_name_prefix='research-wait')
    return _wait_executor


async def run_research_call(function):
    import asyncio
    from contextvars import copy_context
    cancel = threading.Event()
    with job_context(cancel=cancel):
        context = copy_context()
    future = asyncio.get_running_loop().run_in_executor(wait_executor(), context.run, function)
    try:
        return await asyncio.shield(future)
    finally:
        cancel.set()
        future.add_done_callback(lambda f: None if f.cancelled() else f.exception())


def get_queue():
    global _queue
    with _queue_lock:
        if _queue is None:
            _queue = ResearchQueue()
            atexit.register(_queue.close)
    return _queue


class IsolatedEngine:
    """Preserve the engine interface; isolate only expensive entry points."""
    def __init__(self, engine, kind, *, options=None, cancel=None, status=None, deadline=None):
        self._engine = engine
        self._kind = kind
        self._options = options or {}
        self._cancel = cancel
        self._status = status
        self._deadline = deadline

    def controlled(self, *, cancel=None, status=None, seconds=None):
        return IsolatedEngine(self._engine, self._kind, options=self._options,
                              cancel=cancel, status=status,
                              deadline=None if seconds is None else time.monotonic() + seconds)

    def __getattr__(self, name):
        original = getattr(self._engine, name)
        if name not in {'execute_search', 'search_composition_logic', 'lab_search', 'lab_composition_search'}:
            return original

        def call(*args, **kwargs):
            from shared.search_regex import _deadline, SearchBudgetExceeded
            bound = inspect.signature(original).bind(*args, **kwargs)
            arguments = dict(bound.arguments)
            progress = arguments.pop('progress_callback', None)
            arguments.pop('phase_callback', None)
            queue = get_queue()
            job = queue.submit({'kind': self._kind, 'method': name, 'arguments': arguments,
                                'options': self._options})
            cancelled = self._cancel or _cancel_context.get()
            update = self._status or _status_context.get()
            try:
                while True:
                    if cancelled is not None and cancelled.is_set():
                        raise InterruptedError('Search cancelled')
                    deadline = _deadline.get()
                    if self._deadline is not None:
                        deadline = min(deadline or float('inf'), self._deadline)
                    if deadline is not None and time.monotonic() >= deadline:
                        raise SearchBudgetExceeded()
                    position = queue.position(job)
                    status = f'Queued: {position}' if position else job.status
                    if update:
                        update(status, job.progress)
                    if progress:
                        # Poll even when the engine is stuck in native code, so
                        # the UI Stop callback can terminate the subprocess.
                        progress(*((-position, 0) if position else job.progress))
                    try:
                        result = job.future.result(timeout=0.1)
                        break
                    except FutureTimeout:
                        continue
            except BaseException:
                queue.cancel(job)
                raise
            from shared.search_engine import _set_last_responsa_downgrade, _set_last_responsa_downgrade_meta
            if result.get('downgrade'):
                _set_last_responsa_downgrade(result['downgrade'])
            if result.get('cascade'):
                _set_last_responsa_downgrade_meta(result['cascade'])
            return result['value']
        return call


def api_engine(engine, request, seconds):
    """Bind API job cancellation/progress without changing fake test engines."""
    if not isinstance(engine, IsolatedEngine):
        return engine
    background = request.scope.get('research_job')
    return engine.controlled(
        cancel=background.cancel if background else None,
        status=background.update if background else None,
        seconds=None if background else seconds,
    )
