"""One FIFO queue for isolated research computations (no NiceGUI imports).

The web process owns admission and cancellation. Each computation gets a fresh
subprocess, so killing a pathological query also releases its native threads,
indexes and memory. Nothing in the child imports web.main.

Each slot starts its next subprocess before a search arrives (a "warm" worker,
GENIZAH_RESEARCH_PRESTART=0 turns this off): it loads the catalogue, which costs
seconds, and waits for its input. It still serves exactly one computation.

A text search can ask for early rows (execute_search's preview_callback): the
child writes them to preview.pkl and IsolatedEngine hands them on while the
search is still running.
"""
from __future__ import annotations

import atexit
from copy import deepcopy
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
from shared.research_worker import PREVIEW_MAX_BYTES

log = logging.getLogger(__name__)
_cancel_context = ContextVar('research_cancel', default=None)
_status_context = ContextVar('research_status', default=None)
# A worker that has not stopped this long after its time limit is stopped by force.
TIME_LIMIT_GRACE_SECONDS = 60
_time_limit_stop = threading.local()


def consume_time_limit_stop() -> bool:
    """Whether the last research call on this thread was stopped by the website's
    time limit (Config.WEB_SEARCH_TIME_LIMIT), then forget it."""
    stopped = bool(getattr(_time_limit_stop, 'value', False))
    _time_limit_stop.value = False
    return stopped


_shared_limit = ContextVar('research_shared_time_limit', default=None)


@contextmanager
def shared_time_limit():
    """The searches run inside share one time limit (Config.WEB_SEARCH_TIME_LIMIT):
    their running times add up, so a refinement chain run again step by step stops
    after the limit as one search does. A step started after the limit is spent
    stops at once and comes back cut off, like any search the limit stopped."""
    token = _shared_limit.set({'left': None})
    try:
        yield
    finally:
        _shared_limit.reset(token)


class ResearchJobError(RuntimeError):
    """A computation stopped without returning misleading partial results."""


def snapshot_settings(engine, overrides=None):
    """The engine's settings as a worker rebuilds them, with *overrides* on top."""
    settings = getattr(engine, 'settings', None)
    if settings is None:
        variants = getattr(engine, 'var_mgr', None)
        if variants is None:
            variants = getattr(getattr(engine, 'text_fetcher', None), 'var_mgr', None)
        settings = getattr(variants, '_settings', None)
    snapshot = deepcopy(vars(settings)) if settings is not None else None
    if overrides:
        snapshot = {**(snapshot or {}), **deepcopy(overrides)}
    return snapshot


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
    # The latest early rows from the child (payload 'preview'), numbered so the
    # waiting caller hands each one on once.
    preview: list | None = None
    preview_sequence: int = 0


@dataclass(eq=False)
class Worker:
    """A started child process and the private directory it reads and writes."""
    process: subprocess.Popen
    directory: tempfile.TemporaryDirectory

    @property
    def root(self):
        return Path(self.directory.name)

    def discard(self):
        """Kill the process (and its children), then remove its directory."""
        try:
            _kill(self.process)
        finally:
            self.directory.cleanup()


def _write_input(root, payload):
    # A warm child polls for input.pkl: rename a complete file into place.
    temporary = root / 'input.tmp'
    temporary.write_bytes(pickle.dumps(payload))
    temporary.replace(root / 'input.pkl')


def _read_preview(root, job, seen):
    """Load a new preview.pkl into *job*; return what was read (its stat key)."""
    try:
        stat = (root / 'preview.pkl').stat()
    except OSError:
        return seen
    key = (stat.st_mtime_ns, stat.st_size)
    if key == seen or stat.st_size > PREVIEW_MAX_BYTES:
        return seen
    try:
        # Only our own subprocess writes this private temporary file.
        event = pickle.loads((root / 'preview.pkl').read_bytes())
        sequence, rows = int(event['sequence']), event['rows']
    except (OSError, EOFError, pickle.UnpicklingError, KeyError, TypeError, ValueError):
        return seen  # Read again on the next tick.
    if sequence > job.preview_sequence and isinstance(rows, list):
        job.preview, job.preview_sequence = rows, sequence
    return key


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
    def __init__(self, *, command=None, warm=None):
        self.capacity = _integer('GENIZAH_RESEARCH_WORKERS', 1, maximum=4)
        self.queue_limit = _integer('GENIZAH_RESEARCH_QUEUE_SIZE', 16, maximum=64)
        self.memory_mb = _integer('GENIZAH_RESEARCH_MEMORY_MB', 4096, minimum=128, maximum=65536)
        self.reserve_mb = _integer('GENIZAH_WEB_RESERVE_MB', 1024, minimum=128, maximum=65536)
        self.result_mb = _integer('GENIZAH_RESEARCH_RESULT_MB', 512, maximum=512)
        self.command = command or [sys.executable, '-m', 'shared.research_worker']
        # Start each slot's next worker before its search (the real worker
        # only: a test command need not know the warm protocol).
        if warm is None:
            warm = command is None and os.environ.get('GENIZAH_RESEARCH_PRESTART', '1') != '0'
        self.warm = bool(warm)
        self.spares = {}  # slot -> Worker started ahead of its search
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
            self._start_spare(slot)
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

    def _memory_for_new_worker(self):
        return available_memory() >= (self.reserve_mb + 128 * self.capacity) * 1024**2

    def _start_worker(self, slot, payload=None):
        """Start a child; without *payload* it loads first and waits for one."""
        available_mb = available_memory() // 1024**2
        allocation_mb = min(self.memory_mb, max(128, (available_mb - self.reserve_mb) // self.capacity))
        directory = tempfile.TemporaryDirectory(prefix='genizah-research-', ignore_cleanup_errors=True)
        try:
            root = Path(directory.name)
            if payload is not None:
                _write_input(root, payload)
            env = dict(os.environ)
            env.update(RAYON_NUM_THREADS='1', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
                       MKL_NUM_THREADS='1', GENIZAH_SEARCH_BUDGET_SECONDS='0',
                       GENIZAH_RESEARCH_PARENT=str(os.getpid()),
                       GENIZAH_RESEARCH_SLOT=str(slot),
                       GENIZAH_RESEARCH_MEMORY_MB=str(allocation_mb),
                       GENIZAH_RESEARCH_WARM='0' if payload is not None else '1')
            # Logs stay in the server log, never in a response containing paths
            # or corpus text. No shell, no web module entry point, no stdout pipe
            # which could fill and deadlock a verbose native library.
            process = subprocess.Popen(
                [*self.command, str(root)], env=env,
                cwd=str(Path(__file__).resolve().parents[1]),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                start_new_session=os.name != 'nt',
            )
        except BaseException:
            directory.cleanup()
            raise
        return Worker(process, directory)

    def _start_spare(self, slot):
        """Start this slot's next worker now, so its search skips the loading.

        Only when the machine has the memory a new worker is admitted with;
        otherwise the search starts its worker itself, as before.
        """
        with self.condition:
            if not self.warm or self.closed or slot in self.spares:
                return
        if not self._memory_for_new_worker():
            return
        try:
            worker = self._start_worker(slot)
        except Exception:
            log.exception('Could not start a search worker ahead of its search')
            return
        with self.condition:
            if not self.closed:
                self.spares[slot] = worker
                return
        worker.discard()  # close() ran while it started

    def _take_spare(self, slot):
        with self.condition:
            worker = self.spares.pop(slot, None)
        if worker is not None and worker.process.poll() is not None:
            worker.discard()  # It died while waiting; start a fresh one.
            return None
        return worker

    def _execute(self, job, slot):
        if job.cancel.is_set():
            raise InterruptedError('Search cancelled')
        if isinstance(job.payload, dict) and job.payload.get('time_limit'):
            # The job has left the queue: its time limit counts from now, the
            # worker's loading and its wait for the index leases included.
            job.payload['started_at'] = time.time()
        worker = self._take_spare(slot)
        if worker is None:
            # Don't start another worker while the machine lacks even the website's
            # reserve. This wait has no time cutoff and is immediately cancellable.
            job.status = 'Waiting for memory'
            while not self._memory_for_new_worker():
                if job.cancel.wait(0.2):
                    raise InterruptedError('Search cancelled')
            job.status = 'Starting search worker'
            worker = self._start_worker(slot, job.payload)
        else:
            job.status = 'Starting search worker'
            try:
                _write_input(worker.root, job.payload)
            except BaseException:
                worker.discard()
                raise
        job.process = worker.process
        root = worker.root
        wants_preview = isinstance(job.payload, dict) and bool(job.payload.get('preview'))
        preview_seen = None
        try:
            try:
                proc = psutil.Process(job.process.pid)
            except psutil.NoSuchProcess:
                proc = None  # Already exited: reported below.
            while job.process.poll() is None:
                if job.cancel.wait(0.1):
                    raise InterruptedError('Search cancelled')
                try:
                    if proc is not None:
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
                if wants_preview:
                    preview_seen = _read_preview(root, job, preview_seen)
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
                if result.get('time_limit'):
                    from shared.search_regex import SearchBudgetExceeded
                    raise SearchBudgetExceeded()
                if result.get('exception') == 'NoWitnessesResolved':
                    from shared.passage_parallels import NoWitnessesResolved
                    raise NoWitnessesResolved(result['report'])
                if result.get('validation'):
                    raise ValueError(result['error'])
                raise ResearchJobError(result['error'])
            return result
        finally:
            job.process = None
            worker.discard()

    def close(self):
        with self.condition:
            self.closed = True
            for job in list(self.pending):
                self.cancel(job)
            for job in self.active:
                job.cancel.set()
            spares, self.spares = list(self.spares.values()), {}
            self.condition.notify_all()
        for worker in spares:
            worker.discard()
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
    """Preserve the engine interface; isolate only expensive entry points.

    Every job starts from the website's variant defaults
    (``web.variant_preferences.WEBSITE_DEFAULTS``) with this wrapper's per-search
    settings on top, never from the variant values in the server's LabSettings:
    that object is shared by every visitor, and nothing a visitor does may change
    another visitor's search.
    """
    def __init__(self, engine, kind, *, options=None, cancel=None, status=None, deadline=None,
                 settings=None):
        self._engine = engine
        self._kind = kind
        self._options = options or {}
        self._cancel = cancel
        self._status = status
        self._deadline = deadline
        self._settings = dict(settings or {})

    def controlled(self, *, cancel=None, status=None, seconds=None):
        return IsolatedEngine(self._engine, self._kind, options=self._options,
                              cancel=cancel, status=status,
                              deadline=None if seconds is None else time.monotonic() + seconds,
                              settings=self._settings)

    def with_settings(self, **values):
        """A view of this engine whose jobs carry these settings (checked).

        The shared engine and its settings object are not modified, so two
        visitors searching at the same time cannot change each other's search.
        """
        from web.variant_preferences import request_settings
        return IsolatedEngine(self._engine, self._kind, options=self._options,
                              cancel=self._cancel, status=self._status,
                              deadline=self._deadline,
                              settings={**self._settings, **request_settings(**values)})

    def job_settings(self):
        """The settings snapshot a job of this wrapper hands its worker."""
        from web.variant_preferences import website_defaults
        return snapshot_settings(self._engine, {**website_defaults(), **self._settings})

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
            # Early rows come back through the job (preview.pkl), not by pickling
            # the caller's callback into the child.
            preview = arguments.pop('preview_callback', None)
            # The cut-off signal is per thread: this call's answer replaces any older one,
            # and a call that fails or is stopped leaves none.
            from shared.search_engine import consume_last_search_cutoff
            consume_last_search_cutoff()
            consume_time_limit_stop()
            queue = get_queue()
            from shared.config import Config
            time_limit = Config.WEB_SEARCH_TIME_LIMIT
            shared = _shared_limit.get()
            if shared is not None and time_limit:
                if shared['left'] is None:
                    shared['left'] = float(time_limit)
                # What is left of the shared limit; 0 would mean no limit to the worker.
                time_limit = max(shared['left'], 0.001)
            payload = {'kind': self._kind, 'method': name, 'arguments': arguments,
                       'options': self._options, 'settings': self.job_settings(),
                       'time_limit': time_limit}
            if preview is not None:
                payload['preview'] = True
            job = queue.submit(payload)
            cancelled = self._cancel or _cancel_context.get()
            update = self._status or _status_context.get()
            previewed = 0
            running_since = None
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
                    # The worker stops itself at its time limit; one that cannot
                    # (stuck outside the engine's checks) is stopped here.
                    if not position and running_since is None:
                        running_since = time.monotonic()
                    if (time_limit and running_since is not None
                            and time.monotonic() - running_since >= time_limit + TIME_LIMIT_GRACE_SECONDS):
                        raise SearchBudgetExceeded()
                    status = f'Queued: {position}' if position else job.status
                    if update:
                        update(status, job.progress)
                    if progress:
                        # Poll even when the engine is stuck in native code, so
                        # the UI Stop callback can terminate the subprocess.
                        progress(*((-position, 0) if position else job.progress))
                    if preview is not None and job.preview_sequence > previewed:
                        previewed, rows = job.preview_sequence, job.preview
                        try:
                            preview(rows)
                        except Exception:
                            # Early rows are a convenience; the result still comes.
                            log.exception('Search preview callback failed')
                    try:
                        result = job.future.result(timeout=0.1)
                        break
                    except FutureTimeout:
                        continue
            except BaseException:
                queue.cancel(job)
                raise
            finally:
                if shared is not None and shared['left'] is not None and running_since is not None:
                    shared['left'] -= time.monotonic() - running_since
            from shared.search_engine import (_note_search_cutoff, _set_last_responsa_downgrade,
                                              _set_last_responsa_downgrade_meta)
            cutoff = result.get('cutoff') or {}
            _note_search_cutoff(capped=bool(cutoff.get('capped')),
                                interrupted=bool(cutoff.get('interrupted')))
            _time_limit_stop.value = bool(result.get('time_limit'))
            if result.get('downgrade'):
                _set_last_responsa_downgrade(result['downgrade'])
            if result.get('cascade'):
                _set_last_responsa_downgrade_meta(result['cascade'])
            return result['value']
        return call


def with_request_settings(engine, **values):
    """Bind one search's settings to an engine that runs its searches in workers.

    Engines that do not run in workers (fakes in tests) are returned unchanged,
    but the names are still checked.
    """
    if isinstance(engine, IsolatedEngine):
        return engine.with_settings(**values)
    from web.variant_preferences import request_settings
    request_settings(**values)
    return engine


def api_engine(engine, request, seconds):
    """Bind API job cancellation/progress without changing fake test engines.

    API jobs run with the website defaults only (``controlled`` starts a wrapper
    with no per-search settings), never with anything a website visitor chose.
    """
    if not isinstance(engine, IsolatedEngine):
        return engine
    background = request.scope.get('research_job')
    return IsolatedEngine(engine._engine, engine._kind, options=engine._options).controlled(
        cancel=background.cancel if background else None,
        status=background.update if background else None,
        seconds=None if background else seconds,
    )
