"""Web search speed and early rows (2026-10-05).

A web search ran ~6 s slower than the same desktop search, the time its fresh
worker spent loading the catalogue, and showed nothing until it finished. Now
each queue slot starts its next worker ahead of the search ("warm": loaded,
waiting for its input; still one search per process), and a search can ask the
worker for its early rows (execute_search's preview_callback), which the page
shows while the search runs.
"""
import ast
import os
from pathlib import Path
import pickle
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from web.research_jobs import IsolatedEngine, Job, ResearchQueue, _read_preview

ROOT = Path(__file__).resolve().parents[1]

# A stand-in for shared/research_worker.py that speaks the same protocol: a warm
# worker waits for input.pkl; a preview request gets two previews, the search
# then waits for a 'release' file so a test can see the previews first.
WORKER = 'import sys\nsys.path.insert(0, ' + repr(str(ROOT)) + ')\n' + '''
import json, os, pickle, sys, time
from pathlib import Path
from shared.research_worker import wait_for_input, write_preview
root = Path(sys.argv[1])
warm = os.environ.get('GENIZAH_RESEARCH_WARM')
(root / 'started').write_text(str(time.monotonic()))
if warm == '1':
    request = wait_for_input(root)
else:
    request = pickle.loads((root / 'input.pkl').read_bytes())
(root / 'progress.json').write_text(json.dumps({'status': 'Searching', 'progress': [1, 2]}))
if request.get('preview'):
    write_preview(root, 1, ['row 1'])
    time.sleep(0.3)
    write_preview(root, 2, ['row 1', 'row 2'])
    while not Path(request['arguments']['release']).exists():
        time.sleep(0.01)
if request.get('block'):
    while True:
        time.sleep(0.01)
(root / 'output.pkl').write_bytes(pickle.dumps(
    {'value': {'request': request, 'pid': os.getpid(), 'warm': warm}}))
'''


@pytest.fixture
def make_queue(tmp_path, monkeypatch):
    monkeypatch.setenv('GENIZAH_RESEARCH_WORKERS', '1')
    monkeypatch.setenv('GENIZAH_RESEARCH_QUEUE_SIZE', '2')
    monkeypatch.setenv('GENIZAH_WEB_RESERVE_MB', '128')
    # These tests are about the warm protocol, not admission (one test sets
    # memory to 0 itself). The host's reading can be low for reasons of its own:
    # a container's cgroup counts page cache as used (1.3 GB "available" with
    # 15 GB free), so four test lanes left a search waiting for memory.
    monkeypatch.setattr('web.research_jobs.available_memory', lambda: 64 * 1024**3)
    script = tmp_path / 'worker.py'
    script.write_text(WORKER, encoding='utf-8')
    queues = []

    def make(warm):
        queue = ResearchQueue(command=[sys.executable, str(script)], warm=warm)
        queues.append(queue)
        return queue
    yield make
    for queue in queues:
        queue.close()


def wait_for(predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'expected state not reached'
        time.sleep(0.02)


def spare_of(queue, slot=0):
    with queue.condition:
        return queue.spares.get(slot)


# --- The warm worker -------------------------------------------------------------

def test_slot_starts_its_worker_before_the_search_and_the_search_uses_it(make_queue):
    queue = make_queue(warm=True)
    wait_for(lambda: spare_of(queue) is not None)
    spare = spare_of(queue)
    wait_for(lambda: (spare.root / 'started').exists())
    assert not (spare.root / 'input.pkl').exists()  # waiting for a search
    result = queue.submit({'query': 'שלום'}).future.result(timeout=30)['value']
    assert result['pid'] == spare.process.pid
    assert result['warm'] == '1'
    assert result['request'] == {'query': 'שלום'}
    # One search per process: it is gone, its directory removed, and the slot
    # has already started the next one.
    assert spare.process.poll() is not None  # exited and reaped (not pid_exists: pids wrap)
    assert not spare.root.exists()
    wait_for(lambda: spare_of(queue) is not None)
    assert spare_of(queue).process.pid != spare.process.pid


def test_cold_queue_starts_its_worker_with_the_search(make_queue):
    queue = make_queue(warm=False)
    time.sleep(0.2)
    assert spare_of(queue) is None
    result = queue.submit({'query': 'x'}).future.result(timeout=30)['value']
    assert result['warm'] == '0'
    assert spare_of(queue) is None


def test_stop_kills_the_warm_worker_and_the_next_search_works(make_queue):
    queue = make_queue(warm=True)
    wait_for(lambda: spare_of(queue) is not None)
    job = queue.submit({'block': True})
    wait_for(lambda: job.status == 'Searching')
    process = job.process
    queue.cancel(job)
    with pytest.raises(InterruptedError):
        job.future.result(timeout=30)
    assert process.poll() is not None
    assert queue.submit({'next': True}).future.result(timeout=30)['value']['request']['next']


def test_a_warm_worker_that_died_is_replaced_by_a_fresh_one(make_queue):
    queue = make_queue(warm=True)
    wait_for(lambda: spare_of(queue) is not None)
    dead = spare_of(queue)
    dead.process.kill()
    dead.process.wait(timeout=5)
    result = queue.submit({'query': 'x'}).future.result(timeout=30)['value']
    assert result['pid'] != dead.process.pid
    assert result['warm'] == '0'  # started with its search
    assert not dead.root.exists()


def test_close_kills_the_idle_warm_worker(make_queue):
    queue = make_queue(warm=True)
    wait_for(lambda: spare_of(queue) is not None)
    spare = spare_of(queue)
    queue.close()
    assert spare.process.poll() is not None
    assert not spare.root.exists()
    assert queue.spares == {}


def test_no_warm_worker_without_the_memory_to_admit_one(make_queue, monkeypatch):
    monkeypatch.setattr('web.research_jobs.available_memory', lambda: 0)
    queue = make_queue(warm=True)
    time.sleep(0.3)
    assert spare_of(queue) is None


def test_only_the_real_worker_is_started_ahead_by_default(monkeypatch):
    monkeypatch.setenv('GENIZAH_RESEARCH_PRESTART', '0')
    queue = ResearchQueue()
    try:
        assert queue.warm is False
    finally:
        queue.close()
    custom = ResearchQueue(command=[sys.executable, '-c', 'pass'])
    try:
        assert custom.warm is False  # a test command need not know the protocol
    finally:
        custom.close()


def test_real_worker_preloads_with_checked_fetches_and_waits_for_input(tmp_path, monkeypatch):
    from shared import research_worker
    built = []

    class FakeMeta:
        def __init__(self, **kwargs):
            built.append(kwargs)

        def _load_heavy_caches_bg(self):
            built.append('loaded')
    monkeypatch.setattr('shared.metadata_manager.MetadataManager', FakeMeta)
    meta = research_worker.preload()
    assert isinstance(meta, FakeMeta)
    assert built == [{'checked_library_fetches': True}, 'loaded']

    def hand_over():
        time.sleep(0.1)
        (tmp_path / 'input.tmp').write_bytes(pickle.dumps({'q': 1}))
        (tmp_path / 'input.tmp').replace(tmp_path / 'input.pkl')
    threading.Thread(target=hand_over).start()
    assert research_worker.wait_for_input(tmp_path, poll=0.01) == {'q': 1}


def test_real_worker_main_loads_then_waits_for_its_search(tmp_path):
    import subprocess
    from web.research_jobs import _write_input
    script = tmp_path / 'main.py'
    script.write_text('import sys\nsys.path.insert(0, ' + repr(str(ROOT)) + ')\n' + '''
import contextlib, pickle
import shared.research_worker as worker
import shared.local_index_leases as leases
leases.index_leases = lambda paths: contextlib.nullcontext()
worker.preload = lambda: 'loaded catalogue'

def run_query(root, payload, report, native_matching=False, meta=None):
    (root / 'output.pkl').write_bytes(pickle.dumps({'payload': payload, 'meta': meta}))
worker.run_query = run_query
worker.main(sys.argv[1])
''', encoding='utf-8')
    root = tmp_path / 'job'
    root.mkdir()
    env = dict(os.environ, GENIZAH_RESEARCH_PARENT=str(os.getpid()),
               GENIZAH_RESEARCH_MEMORY_MB='1024', GENIZAH_RESEARCH_WARM='1')
    process = subprocess.Popen([sys.executable, str(script), str(root)], env=env,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    try:
        time.sleep(0.8)
        assert process.poll() is None, 'a warm worker must wait for its search'
        _write_input(root, {'query': 'שלום'})
        assert process.wait(timeout=30) == 0
        output = pickle.loads((root / 'output.pkl').read_bytes())
        assert output == {'payload': {'query': 'שלום'}, 'meta': 'loaded catalogue'}
    finally:
        if process.poll() is None:
            process.kill()


def test_a_failed_preload_leaves_loading_to_the_search(monkeypatch):
    from shared import research_worker

    def broken(**kwargs):
        raise RuntimeError('catalogue unreadable')
    monkeypatch.setattr('shared.metadata_manager.MetadataManager', broken)
    assert research_worker.preload() is None


def test_website_starts_the_worker_slots_at_startup():
    source = (ROOT / 'web' / 'main.py').read_text(encoding='utf-8')
    wrap = source.index("IsolatedEngine(state.searcher, 'search')")
    assert 'get_queue()' in source[wrap:wrap + 600]


# --- Early rows ------------------------------------------------------------------

def test_preview_rows_reach_the_caller_before_the_result(make_queue, tmp_path, monkeypatch):
    queue = make_queue(warm=True)
    monkeypatch.setattr('web.research_jobs.get_queue', lambda: queue)
    release = tmp_path / 'release'
    seen = []

    def preview(rows):
        seen.append(list(rows))
        if len(rows) == 2:
            release.write_text('go')

    class Engine:
        def execute_search(self, query_str, mode, gap, progress_callback=None,
                           preview_callback=None, release=None):
            raise AssertionError('must run in the worker')

    value = IsolatedEngine(Engine(), 'search').execute_search(
        'שלום', 'exact', 0, preview_callback=preview, release=str(release))
    request = value['request']
    assert request['preview'] is True
    # The callback stays in this process; it is never pickled into the child.
    assert 'preview_callback' not in request['arguments']
    assert seen[-1] == ['row 1', 'row 2']
    assert all(len(a) <= len(b) for a, b in zip(seen, seen[1:]))


def test_no_preview_requested_without_a_callback(make_queue, monkeypatch):
    queue = make_queue(warm=False)
    monkeypatch.setattr('web.research_jobs.get_queue', lambda: queue)

    class Engine:
        def execute_search(self, query_str, mode, gap, progress_callback=None, preview_callback=None):
            raise AssertionError('must run in the worker')

    value = IsolatedEngine(Engine(), 'search').execute_search('x', 'exact', 0)
    assert 'preview' not in value['request']


def test_read_preview_keeps_the_newest_and_skips_what_it_cannot_use(tmp_path, monkeypatch):
    from shared.research_worker import write_preview
    job = Job({})
    seen = _read_preview(tmp_path, job, None)
    assert seen is None and job.preview is None  # nothing written yet
    write_preview(tmp_path, 2, ['a', 'b'])
    seen = _read_preview(tmp_path, job, seen)
    assert job.preview == ['a', 'b'] and job.preview_sequence == 2
    # An older preview never replaces a newer one.
    write_preview(tmp_path, 1, ['a'])
    os.utime(tmp_path / 'preview.pkl', ns=(1, 1))
    _read_preview(tmp_path, job, seen)
    assert job.preview == ['a', 'b']
    # An oversized file is not loaded.
    monkeypatch.setattr('web.research_jobs.PREVIEW_MAX_BYTES', 4)
    write_preview(tmp_path, 3, ['a', 'b', 'c'])
    _read_preview(tmp_path, job, None)
    assert job.preview_sequence == 2


def test_worker_writes_preview_only_for_a_text_search_that_asked(tmp_path, monkeypatch):
    from shared import research_worker
    calls = []

    class FakeSearchEngine:
        def __init__(self, meta, variants, *, worker_mode=False, open_local=True):
            calls.append(('open_local', open_local))
            self.searcher = object()

        def execute_search(self, query_str, mode, gap, progress_callback=None,
                           corpus_scope='all', preview_callback=None):
            calls.append(('preview_callback', preview_callback is not None))
            if preview_callback is not None:
                preview_callback([{'uid': 'p1'}])
                preview_callback([{'uid': 'p1'}, {'uid': 'p2'}])
            return [{'uid': 'p1'}, {'uid': 'p2'}, {'uid': 'p3'}]
    monkeypatch.setattr('shared.search_engine.SearchEngine', FakeSearchEngine)
    monkeypatch.setenv('GENIZAH_RESEARCH_MEMORY_MB', '512')

    def payload(preview):
        return {'kind': 'search', 'method': 'execute_search', 'preview': preview, 'settings': None,
                'arguments': {'query_str': 'שלום', 'mode': 'exact', 'gap': 0, 'corpus_scope': 'genizah'}}
    research_worker.run_query(tmp_path, payload(True), lambda *a: None, meta=SimpleNamespace())
    assert ('open_local', False) in calls and ('preview_callback', True) in calls
    preview = pickle.loads((tmp_path / 'preview.pkl').read_bytes())
    assert preview == {'sequence': 2, 'rows': [{'uid': 'p1'}, {'uid': 'p2'}]}

    other = tmp_path / 'plain'
    other.mkdir()
    calls.clear()
    research_worker.run_query(other, payload(False), lambda *a: None, meta=SimpleNamespace())
    assert ('preview_callback', False) in calls
    assert not (other / 'preview.pkl').exists()


# --- The search page -------------------------------------------------------------

def _state(**fields):
    from web.pages.search_state import SearchUIState
    state = SearchUIState()
    for name, value in fields.items():
        setattr(state, name, value)
    return state


def test_early_rows_shown_only_when_no_filter_could_hide_one():
    from web.pages.search_state import preview_shows_final_rows
    assert preview_shows_final_rows(_state())
    hidden = [
        {'exclusion_sources': [object()]},
        {'word_search_excluded_ids': {'990001'}},
        {'domain_exclusions': {'Bible'}},
        {'printed_filter': 'hide_printed'},
        {'pgp_filter': 'only_pgp'},
        {'library_filter': ['CUL']},
        {'_all_terms_filter': True, 'refinement_chain': [object()]},
        {'post_filter_width_min': 10},
        {'post_filter_measurement_material': ['paper']},
    ]
    for fields in hidden:
        assert not preview_shows_final_rows(_state(**fields)), fields
    # The all-terms checkbox without a chain filters nothing.
    assert preview_shows_final_rows(_state(_all_terms_filter=True))


def _execute_search_source():
    tree = ast.parse((ROOT / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_search':
            return node
    raise AssertionError('execute_search not found')


def test_page_asks_for_early_rows_in_the_genizah_scope():
    function = _execute_search_source()
    calls = [n for n in ast.walk(function) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == 'execute_search']
    assert len(calls) == 1
    keywords = {k.arg: ast.unparse(k.value) for k in calls[0].keywords}
    assert keywords['corpus_scope'] == "'genizah'"
    assert keywords['mode'] == 'engine_mode(mode)'
    assert keywords['preview_callback'] == 'preview_cb if _preview_wanted else None'
    source = ast.unparse(function)
    assert 'preview_shows_final_rows(search_state)' in source
    # Stop keeps the rows already shown.
    assert "results = list(_preview_box['rows'])" in source
    # The painter is stopped before the result is rendered.
    assert '_painter.cancel()' in source


def test_preview_translation_exists():
    from shared.genizah_translations import TRANSLATIONS
    assert TRANSLATIONS['Still searching. The first results:']


# --- Exact runs as the desktop's Exact ---------------------------------------------

def test_page_exact_is_the_engines_literal():
    """The engine's whole-word rule and Exact fast paths key on 'literal'; the
    page's 'exact' had none of them (e8decfca deferred it)."""
    from web.pages.search_state import engine_mode
    from shared.search_engine import _WHOLE_WORD_MODES
    assert engine_mode('exact') == 'literal' and 'literal' in _WHOLE_WORD_MODES
    for mode in ('variants', 'variants_extended', 'variants_maximum', 'fuzzy', 'Regex',
                 'Title', 'Shelfmark', 'responsa', 'literal'):
        assert engine_mode(mode) == mode


def test_search_within_steps_record_the_mode_they_ran_with():
    """A step replays with its recorded mode: it must be the one the search ran."""
    tree = ast.parse((ROOT / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8'))
    steps = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == 'RefinementStep']
    assert len(steps) == 2
    for call in steps:
        modes = [ast.unparse(k.value) for k in call.keywords if k.arg == 'mode']
        if modes:
            assert modes[0].startswith('engine_mode('), modes[0]
        else:
            # The first step of a chain is built from the run recorded at dispatch
            # (search_state.last_run) or, for restored results, from the controls.
            assert any(k.arg is None for k in call.keywords), ast.unparse(call)
    # Both sources of a step's run record the engine's mode.
    sources = [n.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Attribute) and t.attr == 'last_run' for t in n.targets)]
    sources += [r.value for f in ast.walk(tree) if isinstance(f, ast.FunctionDef)
                and f.name == '_run_params_from_controls'
                for r in ast.walk(f) if isinstance(r, ast.Return)]
    recorded = [ast.unparse(v) for d in sources if isinstance(d, ast.Dict)
                for k, v in zip(d.keys, d.values) if isinstance(k, ast.Constant) and k.value == 'mode']
    assert len(recorded) == 2 and all(v.startswith('engine_mode(') for v in recorded), recorded
    # Undo puts a 'literal' step back in the selector as Exact.
    source = ast.unparse(tree)
    assert "mode_select.value = 'exact' if last_step.mode == 'literal'" in source
