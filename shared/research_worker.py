"""Private subprocess entry point for the web research queue."""
import json
import gzip
import os
from pathlib import Path
import pickle
import sys
import time


class MeasuredWriter:
    def __init__(self, stream, limit):
        self.stream, self.limit, self.size = stream, limit, 0

    def write(self, data):
        self.size += len(data)
        if self.size > self.limit:
            raise ValueError('Expanded search output exceeds its memory allowance.')
        return self.stream.write(data)


def write_progress(root, event):
    """A progress reader briefly holding the file must not abort research."""
    temporary = root / 'progress.tmp'
    try:
        temporary.write_text(json.dumps(event), encoding='utf-8')
        temporary.replace(root / 'progress.json')
    except OSError:
        # Windows can reject replacement while the parent reads. A later
        # progress update replaces this one; completion uses a separate file.
        pass


# Early rows (execute_search's preview_callback) are a few dozen rows; a bigger
# file is not written, so the parent never loads more than this before the result.
PREVIEW_MAX_BYTES = 16 * 1024**2


def write_preview(root, sequence, rows):
    """Hand the parent the rows found so far, replacing the previous preview.

    Each preview extends the one before (the engine's contract), so a preview
    lost to a reader holding the file is covered by the next one or the result.
    """
    data = pickle.dumps({'sequence': sequence, 'rows': rows}, protocol=pickle.HIGHEST_PROTOCOL)
    if len(data) > PREVIEW_MAX_BYTES:
        return
    temporary = root / 'preview.tmp'
    for attempt in range(3):
        try:
            temporary.write_bytes(data)
            temporary.replace(root / 'preview.pkl')
            return
        except OSError:
            time.sleep(0.02 * (attempt + 1))


def wait_for_input(root, poll=0.02):
    """A warm worker waits here, everything loaded, until the parent hands it a
    search. The parent writes input.pkl under another name and renames it, so
    the file is complete once it exists."""
    path = root / 'input.pkl'
    while not path.exists():
        time.sleep(poll)
    return pickle.loads(path.read_bytes())


def preload():
    """What every search needs before it can start, loaded before it arrives.

    The catalogue (libraries.csv and what is built from it) takes seconds to
    load; the index itself opens in milliseconds and is opened per search, under
    the index lease. A failure here is not fatal: run_query loads again and
    reports the error the normal way.
    """
    try:
        from shared.metadata_manager import MetadataManager
        import shared.lab_engine  # noqa: F401 -- imported for run_query
        import shared.search_engine  # noqa: F401
        import shared.variants  # noqa: F401

        # Started only by the web app (web/research_jobs.py): its library fetches
        # check every redirect hop, as the web's own MetadataManager does.
        meta = MetadataManager(checked_library_fetches=True)
        meta._load_heavy_caches_bg()
        return meta
    except MemoryError:
        raise
    except Exception:
        import traceback
        traceback.print_exc()
        return None


def restore_settings(snapshot):
    from shared.lab_settings import LabSettings
    settings = LabSettings()
    if snapshot is not None:
        for name, value in snapshot.items():
            if name in vars(settings):
                setattr(settings, name, value)
    return settings


def protect_parent_death(parent_pid):
    """Permit GIL-holding matching only with kernel-enforced parent death."""
    if sys.platform != 'linux':
        return False
    import ctypes
    import signal
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        # PR_SET_PDEATHSIG: SIGKILL requires neither Python nor a running thread.
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            return False
    except (AttributeError, OSError):
        return False
    # Close the race where the parent exited before prctl was armed.
    if os.getppid() != parent_pid:
        os._exit(1)
    return True


def main(directory):
    parent_pid = int(os.environ['GENIZAH_RESEARCH_PARENT'])
    native_matching = protect_parent_death(parent_pid)
    from shared.research_limits import limit_memory
    limit_memory(int(os.environ['GENIZAH_RESEARCH_MEMORY_MB']) * 1024**2)
    import psutil
    import threading

    # A crashed/restarted web process must not leave unlimited orphan searches.
    parent = psutil.Process(parent_pid)

    def watch_parent():
        while parent.is_running():
            time.sleep(1)
        os._exit(1)

    threading.Thread(target=watch_parent, daemon=True).start()

    proc = psutil.Process()
    if os.name == 'nt':
        proc.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        proc.nice(10)
    if hasattr(proc, 'cpu_affinity'):
        allowed = proc.cpu_affinity()
        # Leave the first allowed CPU available exclusively to non-search work.
        if len(allowed) > 1:
            search_cpus = allowed[1:]
            slot = int(os.environ.get('GENIZAH_RESEARCH_SLOT', '0'))
            proc.cpu_affinity([search_cpus[slot % len(search_cpus)]])

    root = Path(directory)
    meta = None
    if os.environ.get('GENIZAH_RESEARCH_WARM') == '1':
        # Started ahead of its search (web/research_jobs.py): load first, then
        # wait. Still one search per process, so killing it frees everything.
        meta = preload()
        payload = wait_for_input(root)
    else:
        payload = pickle.loads((root / 'input.pkl').read_bytes())
    last = [0.0]

    def report(*values):
        now = time.monotonic()
        if now - last[0] < 0.2:
            return
        last[0] = now
        progress = values if len(values) == 2 and all(isinstance(v, (int, float)) for v in values) else (0, 0)
        status = ('Checking candidate texts'
                  if payload['kind'] == 'search' and payload['method'] == 'execute_search'
                  else 'Searching')
        write_progress(root, {'status': status, 'progress': progress})

    from shared.config import Config
    from shared.local_index_leases import index_leases
    write_progress(root, {'status': 'Waiting for local index maintenance', 'progress': (0, 0)})
    with index_leases([Config.LOCAL_INDEX_DIR, Config.LOCAL_LAB_INDEX_DIR]):
        write_progress(root, {'status': 'Starting search worker', 'progress': (0, 0)})
        try:
            run_query(root, payload, report, native_matching=native_matching, meta=meta)
        finally:
            # Drop engine cycles/native handles before releasing the read lease.
            import gc
            gc.collect()


def _time_limited(report, seconds, stopped):
    """*report*, stopping the search once *seconds* have passed: the engine treats
    the InterruptedError as a Stop and returns what it had checked, with its cut-off
    signal 'interrupted'. *stopped* records that the time limit stopped it."""
    if not seconds:
        return report
    deadline = time.monotonic() + seconds

    def progress(*args, **kwargs):
        if time.monotonic() >= deadline:
            stopped['time_limit'] = True
            raise InterruptedError('Search time limit reached')
        return report(*args, **kwargs)
    return progress


def run_query(root, payload, report, *, native_matching=False, meta=None):
    stopped = {'time_limit': False}
    try:
        from shared.metadata_manager import MetadataManager
        from shared.variants import VariantManager
        from shared.lab_engine import LabEngine
        from shared.search_engine import (SearchEngine, _consume_last_responsa_downgrade,
                                          _consume_last_responsa_downgrade_meta, consume_last_search_cutoff)
        from shared.search_regex import isolated_matching

        if meta is None:
            # Started only by the web app (web/research_jobs.py): its library fetches
            # check every redirect hop, as the web's own MetadataManager does.
            meta = MetadataManager(checked_library_fetches=True)
            # The web initializer loads these asynchronously. A short-lived worker
            # must finish loading before it searches or serializes display metadata.
            meta._load_heavy_caches_bg()
        lab = LabEngine(meta, None, settings=restore_settings(payload.get('settings')))
        variants = VariantManager(settings=lab.settings)
        lab.var_mgr = variants
        scope = payload['arguments'].get('corpus_scope',
                                         'all' if payload['method'] == 'execute_search' else 'genizah')
        searcher = SearchEngine(meta, variants, worker_mode=True, open_local=scope != 'genizah')
        kind = payload['kind']
        if kind == 'search' and searcher.searcher is None and payload['arguments'].get('mode') not in ('Title', 'Shelfmark'):
            raise ValueError('The transcription search index is unavailable.')
        if kind == 'lab' and lab.lab_searcher is None:
            raise ValueError('The lab search index is unavailable.')
        engine = searcher if kind == 'search' else lab
        if kind == 'passage':
            from shared.passage_index import open_index
            from shared.passage_parallels import PassageSearcher
            from shared.passage_policy import compose
            options = payload['options']
            index = open_index(options['path'])
            if index is None:
                raise ValueError('The passage index is unavailable.')
            engine = PassageSearcher(index=index, text_fetcher=searcher,
                                     policy=compose(options['preset'], options['length'], options['depth']),
                                     **({} if options['render_cap'] is None else {'render_cap': options['render_cap']}))
        allowed = {'search': {'execute_search', 'search_composition_logic'},
                   'lab': {'lab_search', 'lab_composition_search'},
                   'passage': {'search_composition_logic'}}
        if payload['method'] not in allowed[kind]:
            raise ValueError('Unsupported research operation.')
        arguments = payload['arguments']
        if arguments.get('restrict_sys_ids') is not None:
            arguments['restrict_sys_ids'] = set(arguments['restrict_sys_ids'])
        arguments['progress_callback'] = _time_limited(report, payload.get('time_limit'), stopped)
        if kind == 'passage':
            # The letter-level searcher leaves progress_callback alone (the desktop's
            # drives a chunk progress bar); it is stopped through `checkpoint`.
            arguments['checkpoint'] = arguments['progress_callback']
        if payload.get('preview') and kind == 'search' and payload['method'] == 'execute_search':
            sent = [0]

            def preview(rows):
                sent[0] += 1
                write_preview(root, sent[0], rows)
            arguments['preview_callback'] = preview
        write_progress(root, {'status': 'Preparing search', 'progress': (0, 0)})
        with isolated_matching(native=native_matching):
            value = getattr(engine, payload['method'])(**arguments)
        # 'cutoff': the search reached its candidate limit, or was stopped (D8);
        # the web shows such a count as "N+" and completes the step before a
        # combination relies on it.
        result = {'value': value, 'downgrade': _consume_last_responsa_downgrade(),
                  'cascade': _consume_last_responsa_downgrade_meta(),
                  'cutoff': consume_last_search_cutoff(),
                  'time_limit': stopped['time_limit']}
    except InterruptedError:
        # The limit stopped a part of the search that cannot return what it had.
        result = {'error': 'The search reached its time limit.', 'time_limit': True}
    except ValueError as exc:
        result = {'error': str(exc), 'validation': True}
        if type(exc).__name__ == 'NoWitnessesResolved':
            result.update(exception='NoWitnessesResolved', report=exc.report)
    except Exception:
        import traceback
        traceback.print_exc()
        result = {'error': 'The search worker could not complete this query. No complete results were returned.'}
    # Transcriptions and HTML contain considerable repetition. Stream the
    # transfer compressed without allocating another full serialized copy.
    write_progress(root, {'status': 'Saving search results', 'progress': (0, 0)})
    with gzip.open(root / 'output.pkl', 'wb', compresslevel=1) as stream:
        measured = MeasuredWriter(stream, int(os.environ['GENIZAH_RESEARCH_MEMORY_MB']) * 1024**2)
        pickle.dump(result, measured, protocol=pickle.HIGHEST_PROTOCOL)
    (root / 'output-size.json').write_text(json.dumps({'expanded_bytes': measured.size}), encoding='utf-8')


if __name__ == '__main__':
    main(sys.argv[1])
