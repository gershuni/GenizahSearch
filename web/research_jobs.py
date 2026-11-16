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
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait as futures_wait
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
# The engine's catalogue-only modes (SearchEngine._execute_metadata_search): no regex,
# no matching, a row per manuscript. The manuscripts are resolved HERE, in the server
# process, whose catalogue is loaded and indexed (MetadataManager.search_by_meta), and
# handed to execute_search: a lookup of up to INLINE_METADATA_ROWS manuscripts then runs
# in this thread, with no worker to start or queue to wait in -- a shelfmark lookup used
# to take 10 s or more on the API, behind the worker's catalogue load and whoever was
# searching -- and a broader search still goes to a worker, which skips its catalogue.
METADATA_SEARCH_MODES = frozenset({'Title', 'Shelfmark'})
INLINE_METADATA_ROWS = 100
_TO_WORKER = object()   # IsolatedEngine._metadata_search: the search goes to a worker
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


def raise_worker_error(result):
    """Raise the exception a research worker's error *result* stands for, as the
    search would have raised it in this process: the time limit, an unresolved
    witness list, a validation error (a refused Responsa query keeps its type and
    code -- shared/responsa_match.py -- so the API answers it with that code), or
    else ResearchJobError."""
    if result.get('time_limit'):
        from shared.search_regex import SearchBudgetExceeded
        raise SearchBudgetExceeded()
    if result.get('exception') == 'NoWitnessesResolved':
        from shared.passage_parallels import NoWitnessesResolved
        raise NoWitnessesResolved(result['report'])
    if result.get('validation'):
        if result.get('code'):
            from shared.responsa_match import ResponsaQueryRefused
            raise ResponsaQueryRefused(result['error'], code=result['code'])
        raise ValueError(result['error'])
    raise ResearchJobError(result['error'])


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
    def __init__(self, *, command=None, warm=None, capacity_env='GENIZAH_RESEARCH_WORKERS', slot_offset=0,
                 name='research'):
        # *slot_offset*: the editions queue's slots come after the main queue's, so its
        # workers are pinned to other CPUs (research_worker.main, GENIZAH_RESEARCH_SLOT).
        self.capacity = _integer(capacity_env, 1, maximum=4)
        self.slot_offset = slot_offset
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
            thread = threading.Thread(target=self._consume, args=(number,), name=f'{name}-{number}', daemon=True)
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
                       GENIZAH_RESEARCH_SLOT=str(slot + self.slot_offset),
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
                raise_worker_error(result)
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
_editions_queue = None
_queue_lock = threading.Lock()
_wait_executor = None


def shutdown_research():
    """Stop child processes before the web server exits its lifespan. The globals are reset, so
    a later lifespan in the same process (the tests run several) builds a fresh queue and pool
    instead of scheduling on a closed one ("cannot schedule new futures after shutdown")."""
    global _queue, _editions_queue, _wait_executor
    with _queue_lock:
        queue, editions, executor = _queue, _editions_queue, _wait_executor
        _queue = _editions_queue = _wait_executor = None
    for q in (queue, editions):
        if q is not None:
            q.close()
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)


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


def get_editions_queue():
    """The editions leg's own worker slots (``GENIZAH_EDITIONS_WORKERS``, default 1, at most 4),
    beside the main queue's: a search with editions runs its two legs at once (editions plan
    §3.10, the owner's parallel worker), and the editions leg never waits behind another
    visitor's search in the main queue, nor takes a slot from one. Its slots are numbered
    after the main queue's, so their workers run on other CPUs."""
    global _editions_queue
    with _queue_lock:
        if _editions_queue is None:
            _editions_queue = ResearchQueue(capacity_env='GENIZAH_EDITIONS_WORKERS',
                                            slot_offset=_integer('GENIZAH_RESEARCH_WORKERS', 1, maximum=4),
                                            name='editions')
            atexit.register(_editions_queue.close)
    return _editions_queue


_editions_skipped = threading.local()


def consume_editions_skipped() -> bool:
    """Whether the last search on this thread was asked to read the editions and could not (the
    editions worker failed, or its queue was full): its rows are the automatic transcriptions'
    alone, and the results say so once (editions plan §4, PR 3). Then forget it."""
    skipped = bool(getattr(_editions_skipped, 'value', False))
    _editions_skipped.value = False
    return skipped


def _leg_arguments(arguments, leg):
    """``execute_search``'s arguments as ``execute_search_leg`` takes them for *leg*."""
    out = {k: v for k, v in arguments.items() if k not in ('sources', 'metadata_sys_ids', 'preview_callback')}
    out['leg'] = leg
    return out


def _reads_editions_in_parallel(name, arguments):
    """A search whose source choice is "automatic and editions" (``sources='all'``) in a text
    mode: its legs run in two workers at once. The caller decided the editions are served
    (the switch, readiness, who is an editor); "editions only" is one leg, one worker, as the
    coordinator runs it."""
    if name != 'execute_search' or arguments.get('mode') in METADATA_SEARCH_MODES:
        return False
    if arguments.get('corpus_scope', 'all') == 'local':
        return False
    from shared.dictionary_switch import OFF
    from shared.editions_search import SOURCES_ALL, editions_switch, normalize_sources
    try:
        wanted = normalize_sources(arguments.get('sources')) == SOURCES_ALL
    except ValueError:
        return False          # the worker refuses it with execute_search's own message
    # Switched off: one worker, which opens no editions (today's search).
    return wanted and editions_switch() != OFF


@dataclass(eq=False)
class _Waiting:
    """A submitted job and what its caller waits for: *required* (its failure is the search's)
    or not (the editions leg: its failure leaves the main rows, with a note)."""
    queue: ResearchQueue
    job: Job
    required: bool = True
    running_since: float | None = None
    result: dict | None = None
    error: BaseException | None = None


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

    def _resolve_metadata_search(self, arguments):
        """The manuscripts a Title/Shelfmark ``execute_search`` is for, resolved by this
        process's engine (``SearchEngine.metadata_search_ids``); None when it cannot:
        an engine without the method (a test fake), or a catalogue still loading
        (the web app reads libraries.csv in the background; a worker loads its own,
        whole, so until then the worker resolves them as before)."""
        resolve = getattr(self._engine, 'metadata_search_ids', None)
        if resolve is None:
            return None
        if not getattr(getattr(self._engine, 'meta_mgr', None), '_csv_bank_loaded', False):
            return None
        return list(resolve(arguments.get('query_str', ''), arguments['mode'],
                            arguments.get('restrict_sys_ids')))

    def _metadata_search(self, original, arguments, progress, time_limit, shared):
        """A Title/Shelfmark search's manuscripts, resolved in this process, and a
        lookup (INLINE_METADATA_ROWS or fewer) run in this thread; ``_TO_WORKER``
        when the search goes to a worker (the engine cannot resolve them here, or
        there are more), with its manuscripts in *arguments* when they were resolved.

        Under what the worker-wait loop enforces on a worker: the cancel event (a
        background API job's, or the page's `run_research_call`), the deadlines
        (``search_budget`` and ``controlled(seconds=)``: SearchBudgetExceeded, as
        the loop raises it), and the website's time limit (Config.WEB_SEARCH_TIME_LIMIT,
        or what is left of a shared one: the engine returns the rows it had, marked
        as a stopped search, and ``consume_time_limit_stop`` says so, as for a worker).
        Checked before the manuscripts are resolved (an already-cancelled job does
        no catalogue work), at every progress tick the engine asks for (every few
        manuscripts), and, cancel and deadlines, once the engine has returned: its
        last tick may be manuscripts ago, and the loop checks them before it takes a
        worker's result. The time limit is a tick's check, there as here: the worker's
        ``_time_limited`` is its progress callback, and a lookup that ended is complete.
        The time this takes, resolution included, comes off a shared limit; the
        engine's cut-off signal lands on this thread, as a worker's is copied onto it.
        """
        from shared.search_regex import _deadline, SearchBudgetExceeded
        cancelled = self._cancel or _cancel_context.get()
        started = time.monotonic()
        stopped = {'time_limit': False}

        def check(at_tick=True):
            now = time.monotonic()
            if cancelled is not None and cancelled.is_set():
                raise InterruptedError('Search cancelled')
            deadline = _deadline.get()
            if self._deadline is not None:
                deadline = min(deadline or float('inf'), self._deadline)
            if deadline is not None and now >= deadline:
                raise SearchBudgetExceeded()
            if at_tick and time_limit and now - started >= time_limit:
                stopped['time_limit'] = True
                raise InterruptedError('Search time limit reached')

        def inline_progress(*values):
            check()
            if progress:
                progress(*values)

        try:
            check()
            sys_ids = self._resolve_metadata_search(arguments)
            if sys_ids is None:
                return _TO_WORKER
            arguments['metadata_sys_ids'] = sys_ids
            if len(sys_ids) > INLINE_METADATA_ROWS:
                return _TO_WORKER
            result = original(**arguments, progress_callback=inline_progress)
        finally:
            if shared is not None and shared['left'] is not None:
                shared['left'] -= time.monotonic() - started
        # The engine answers a cancel with the rows it had; the caller of a cancelled
        # search gets what the worker path gives it.
        check(at_tick=False)
        _time_limit_stop.value = stopped['time_limit']
        return result

    def __getattr__(self, name):
        original = getattr(self._engine, name)
        if name not in {'execute_search', 'search_composition_logic', 'lab_search', 'lab_composition_search'}:
            return original

        def call(*args, **kwargs):
            bound = inspect.signature(original).bind(*args, **kwargs)
            arguments = dict(bound.arguments)
            progress = arguments.pop('progress_callback', None)
            arguments.pop('phase_callback', None)
            # Early rows come back through the job (preview.pkl), not by pickling
            # the caller's callback into the child.
            preview = arguments.pop('preview_callback', None)
            # The cut-off and Responsa downgrade signals are per thread: this call's
            # answer replaces any older one, and a call that fails or is stopped leaves
            # none. Not every caller reads the downgrade signal (a search with rows has
            # it on results[0]), so without this the next caller on this wait thread
            # would report it.
            from shared.search_engine import (consume_last_search_cutoff,
                                              _consume_last_responsa_downgrade,
                                              _consume_last_responsa_downgrade_meta)
            consume_last_search_cutoff()
            consume_time_limit_stop()
            _consume_last_responsa_downgrade()
            _consume_last_responsa_downgrade_meta()
            from shared.config import Config
            time_limit = Config.WEB_SEARCH_TIME_LIMIT
            shared = _shared_limit.get()
            if shared is not None and time_limit:
                if shared['left'] is None:
                    shared['left'] = float(time_limit)
                # What is left of the shared limit; 0 would mean no limit to the worker.
                time_limit = max(shared['left'], 0.001)
            if name == 'execute_search' and arguments.get('mode') in METADATA_SEARCH_MODES:
                result = self._metadata_search(original, arguments, progress, time_limit, shared)
                if result is not _TO_WORKER:
                    return result
                if shared is not None and time_limit:
                    # Resolving the manuscripts here took from the shared limit.
                    time_limit = max(shared['left'], 0.001)
            queue = get_queue()
            payload = {'kind': self._kind, 'method': name, 'arguments': arguments,
                       'options': self._options, 'settings': self.job_settings(),
                       'time_limit': time_limit}
            parallel = _reads_editions_in_parallel(name, arguments)
            if preview is not None and not parallel:
                # With editions there are no early rows: the grouped order is known only
                # at the end (editions plan §5).
                payload['preview'] = True
            _editions_skipped.value = False
            if parallel:
                # The two legs of a search with editions, each in a worker of its own, at
                # once (editions plan §3.10): the main index in the main queue, the editions
                # index in its own. Both share the time limit and a Stop.
                waiting = [_Waiting(queue, queue.submit(
                    {**payload, 'method': 'execute_search_leg', 'arguments': _leg_arguments(arguments, 'main')}))]
                editions_queue = get_editions_queue()
                try:
                    waiting.append(_Waiting(editions_queue, editions_queue.submit(
                        {**payload, 'method': 'execute_search_leg',
                         'arguments': _leg_arguments(arguments, 'editions')}), required=False))
                except ResearchJobError as exc:
                    log.warning('Editions leg not started: %s', exc)
                    _editions_skipped.value = True
            else:
                waiting = [_Waiting(queue, queue.submit(payload))]
            self._wait_for(waiting, time_limit=time_limit, shared=shared, progress=progress,
                           preview=None if parallel else preview)
            from shared.search_engine import (_note_search_cutoff, _set_last_responsa_downgrade,
                                              _set_last_responsa_downgrade_meta)
            results = [w.result for w in waiting if w.result is not None]
            for w in waiting:
                if w.error is not None:
                    # Failed, refused, or still queued or running when the main leg's answer was
                    # due (_wait_for): the main rows are complete and stand alone, with the note.
                    log.warning('Editions leg not used; the main rows stand alone: %r', w.error)
                    _editions_skipped.value = True
            if any(r.get('editions_unavailable') for r in results):
                # A worker asked to read the editions had none open: an editions-only search
                # searched nothing, which the page says rather than "0 results" (Codex review
                # of PR #18).
                log.warning('Editions asked for, but the worker had no editions index open')
                _editions_skipped.value = True
            for result in results:
                cutoff = result.get('cutoff') or {}
                partial = bool(cutoff.get('partial'))
                _note_search_cutoff(capped=bool(cutoff.get('limited') if partial else cutoff.get('capped')),
                                    interrupted=bool(cutoff.get('interrupted')), partial=partial)
            _time_limit_stop.value = any(bool(r.get('time_limit')) for r in results)
            if parallel:
                from shared.search_engine import SearchEngine, _joined_warnings
                legs = [w.result['value'] if w.result is not None else None for w in waiting]
                value, _ = SearchEngine.merge_search_legs(
                    legs[0], legs[1] if len(legs) > 1 else None, ids_only=bool(arguments.get('ids_only')))
                downgrade = _joined_warnings(*(r.get('downgrade') for r in results))
                cascade = next((r['cascade'] for r in results if r.get('cascade')), None)
            else:
                value, downgrade, cascade = results[0]['value'], results[0].get('downgrade'), results[0].get('cascade')
            if downgrade:
                _set_last_responsa_downgrade(downgrade)
            if cascade:
                _set_last_responsa_downgrade_meta(cascade)
            return value
        return call

    @staticmethod
    def _give_up(waiting, error):
        """Stop the jobs of *waiting* that are not required and have no answer yet: each is
        cancelled (its worker discarded) and keeps *error* as its failure."""
        for w in waiting:
            if not w.required and w.result is None and w.error is None:
                w.queue.cancel(w.job)
                w.error = error()

    def _wait_for(self, waiting, *, time_limit, shared, progress, preview):
        """Wait for the submitted jobs (_Waiting): report the first one's queue position and
        progress, hand on its early rows, and stop them all on a cancel, a deadline, a worker
        stuck past its time limit, or a required job's failure (raised). A job that is not
        required keeps its failure in ``error``; the others' results are in ``result``.

        A job that is not required never costs the required ones their answer (Codex review of
        PR #17): at the deadline, once every required job has its result, the others still
        queued or running are stopped and given SearchBudgetExceeded as their ``error``; and a
        worker stuck past its time limit is stopped the same way when it is not required."""
        from shared.search_regex import _deadline, SearchBudgetExceeded
        cancelled = self._cancel or _cancel_context.get()
        update = self._status or _status_context.get()
        previewed = 0
        lead = waiting[0]
        try:
            while True:
                if cancelled is not None and cancelled.is_set():
                    raise InterruptedError('Search cancelled')
                deadline = _deadline.get()
                if self._deadline is not None:
                    deadline = min(deadline or float('inf'), self._deadline)
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    if all(w.result is not None for w in waiting if w.required):
                        self._give_up(waiting, SearchBudgetExceeded)
                        return
                    raise SearchBudgetExceeded()
                for w in waiting:
                    # The worker stops itself at its time limit; one that cannot
                    # (stuck outside the engine's checks) is stopped here.
                    if w.running_since is None and not w.queue.position(w.job):
                        w.running_since = now
                    if (time_limit and w.running_since is not None and not w.job.future.done()
                            and w.result is None and w.error is None
                            and now - w.running_since >= time_limit + TIME_LIMIT_GRACE_SECONDS):
                        if w.required:
                            raise SearchBudgetExceeded()
                        self._give_up([w], SearchBudgetExceeded)
                position = lead.queue.position(lead.job)
                status = f'Queued: {position}' if position else lead.job.status
                if update:
                    update(status, lead.job.progress)
                if progress:
                    # Poll even when the engine is stuck in native code, so
                    # the UI Stop callback can terminate the subprocess.
                    progress(*((-position, 0) if position else lead.job.progress))
                if preview is not None and lead.job.preview_sequence > previewed:
                    previewed, rows = lead.job.preview_sequence, lead.job.preview
                    try:
                        preview(rows)
                    except Exception:
                        # Early rows are a convenience; the result still comes.
                        log.exception('Search preview callback failed')
                pending = [w for w in waiting if w.result is None and w.error is None]
                done, _ = futures_wait([w.job.future for w in pending], timeout=0.1, return_when=FIRST_COMPLETED)
                for w in pending:
                    if w.job.future not in done:
                        continue
                    try:
                        w.result = w.job.future.result()
                    except BaseException as exc:
                        if w.required:
                            raise
                        w.error = exc
                if all(w.result is not None or w.error is not None for w in waiting):
                    return
        except BaseException:
            for w in waiting:
                w.queue.cancel(w.job)
            raise
        finally:
            started = [w.running_since for w in waiting if w.running_since is not None]
            if shared is not None and shared['left'] is not None and started:
                shared['left'] -= time.monotonic() - min(started)


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
