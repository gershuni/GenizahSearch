"""A search with editions on the website: its two legs in two research workers at once (editions
plan §3.10, the owner's parallel worker), merged in the web process. Real subprocesses with a
stand-in worker script, without the corpus (as tests/test_research_jobs.py)."""
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import psutil
import pytest

from shared.search_engine import consume_last_search_cutoff
from web import research_jobs
from web.research_jobs import IsolatedEngine, ResearchJobError, ResearchQueue, consume_editions_skipped

WORKER = '''
import json, os, pickle, sys, time
from pathlib import Path
root = Path(sys.argv[1])
request = pickle.loads((root / 'input.pkl').read_bytes())
(root / 'progress.json').write_text(json.dumps({'status': 'Searching', 'progress': [1, 2]}))
args = request['arguments']
leg = args.get('leg')
query = args.get('query_str', '')
(Path(os.environ['GENIZAH_TEST_PID_DIR']) / (str(leg) + '.pid')).write_text(str(os.getpid()))
if 'block' in query:
    while True:
        time.sleep(0.01)
if 'edstuck' in query and leg == 'editions':
    while True:
        time.sleep(0.01)
if 'edslow' in query and leg == 'editions':
    time.sleep(5.0)
if 'slow' in query and 'edslow' not in query:
    time.sleep(1.0)
if ('edfail' in query and leg == 'editions') or ('mainfail' in query and leg == 'main'):
    os._exit(9)
if leg == 'main':
    value = {'rows': [{'uid': 'P1', 'display': {'shelfmark': 'A'}}, {'uid': 'P2', 'display': {'shelfmark': 'B'}}],
             'leg': {'warning': None}}
elif leg == 'editions':
    value = {'rows': [{'uid': 'ED:fgp:1', 'group_key': 'P2', 'display': {'shelfmark': 'B'},
                       'edition': {'kind': 'fgp', 'uid': 'ED:fgp:1'}}], 'leg': {'warning': None}}
else:
    value = {'method': request['method'], 'arguments': args}
if 'early' in query:
    # The editions worker leaves its rows; the main one shows what it makes of them, and only
    # then goes on.
    from shared.research_worker import read_editions_result, write_editions_result, write_preview
    if leg == 'editions':
        write_editions_result(request['editions_file'], {'value': value, 'cutoff': {'capped': True}})
        if 'lost' in query:
            os._exit(9)         # it failed in saving what it found
    else:
        while read_editions_result(request['editions_file']) is None:
            time.sleep(0.01)
        if 'unshown' in query:
            time.sleep(0.5)
        else:
            first = 'P1' if 'wrongly' in query else 'P2'
            found = read_editions_result(request['editions_file'])['value']['rows'][0]['uid']
            write_preview(root, 1, [{'uid': first, 'from': found}])
        (Path(os.environ['GENIZAH_TEST_PID_DIR']) / 'shown').touch()
        time.sleep(0.5)
cutoff = {'capped': 'capped' in query and leg == 'editions', 'interrupted': False}
out = {'value': value, 'downgrade': None, 'cascade': None, 'cutoff': cutoff, 'time_limit': False,
       'payload': request, 'editions_unavailable': 'edunavailable' in query}
(root / 'output.pkl').write_bytes(pickle.dumps(out))
'''


def search(query_str, mode, gap, progress_callback=None, exclude_words=None, responsa_options=None,
           restrict_sys_ids=None, text_position=None, corpus_scope="all", phase_callback=None,
           preview_callback=None, ids_only=False, metadata_sys_ids=None, sources=None, max_rows=None):
    raise AssertionError("runs in a worker")


@pytest.fixture
def queues(tmp_path, monkeypatch):
    monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', '1')
    monkeypatch.setenv('GENIZAH_RESEARCH_WORKERS', '1')
    monkeypatch.setenv('GENIZAH_EDITIONS_WORKERS', '1')
    monkeypatch.setenv('GENIZAH_RESEARCH_QUEUE_SIZE', '2')
    monkeypatch.setenv('GENIZAH_WEB_RESERVE_MB', '128')
    (tmp_path / 'pids').mkdir()
    monkeypatch.setenv('GENIZAH_TEST_PID_DIR', str(tmp_path / 'pids'))
    script = tmp_path / 'worker.py'
    script.write_text('import sys\nsys.path.insert(0, ' + repr(str(Path(__file__).resolve().parents[1]))
                      + ')\n' + WORKER, encoding='utf-8')
    main = ResearchQueue(command=[sys.executable, str(script)])
    editions = ResearchQueue(command=[sys.executable, str(script)], capacity_env='GENIZAH_EDITIONS_WORKERS',
                             slot_offset=1, name='editions')
    submitted = []
    for q in (main, editions):
        submit = q.submit

        def spy(payload, _submit=submit, _q=q):
            submitted.append((_q, payload))
            return _submit(payload)
        q.submit = spy
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: main)
    monkeypatch.setattr(research_jobs, 'get_editions_queue', lambda: editions)
    consume_editions_skipped()
    consume_last_search_cutoff()
    yield {'main': main, 'editions': editions, 'submitted': submitted, 'tmp': tmp_path}
    main.close()
    editions.close()


def _engine():
    from types import SimpleNamespace
    return IsolatedEngine(SimpleNamespace(execute_search=search), 'search')


def test_the_two_legs_run_at_once_and_come_back_as_one_result_per_page(queues):
    started = time.monotonic()
    rows = _engine().execute_search('slow', 'literal', 0, corpus_scope='genizah', sources='all',
                                    preview_callback=lambda rows: None)
    elapsed = time.monotonic() - started
    # Each leg sleeps a second: one after the other would take two.
    assert elapsed < 1.9, elapsed
    assert [r['uid'] for r in rows] == ['P2', 'P1']      # P2 is found by both legs
    # The edition ranked first in its leg, the page second in its own: the edition leads.
    assert [m['uid'] for m in rows[0]['members']] == ['ED:fgp:1', 'P2']
    assert rows[0]['sources'] == ['fgp_edition', 'automatic']
    legs = {p['arguments']['leg']: (q, p) for q, p in queues['submitted']}
    assert legs['main'][0] is queues['main'] and legs['editions'][0] is queues['editions']
    for leg, (_q, payload) in legs.items():
        assert payload['method'] == 'execute_search_leg'
        assert 'sources' not in payload['arguments']
        # Early rows were asked for: the editions leg leaves its rows where the main leg looks.
        assert payload['preview'] and payload['editions_file'] == legs['main'][1]['editions_file']
        assert payload['time_limit']
    assert consume_editions_skipped() is False


@pytest.mark.parametrize('query', ['early', 'early wrongly'])
def test_the_main_worker_shows_early_rows_once_the_editions_worker_left_its_rows(queues, query):
    from shared import editions_preview
    early = []
    with patch.object(editions_preview.LOGGER, 'error') as said:
        rows = _engine().execute_search(query, 'literal', 0, corpus_scope='genizah', sources='all',
                                        preview_callback=early.append)
    assert [r['uid'] for r in rows] == ['P2', 'P1']
    assert [[r['from'] for r in shown] for shown in early] == [['ED:fgp:1']], 'handed on while the search ran'
    assert said.called is ('wrongly' in query)      # early rows that were not its first: said
    # The file the two workers shared is gone with the search.
    assert not Path(queues['submitted'][0][1]['editions_file']).parent.exists()


def test_an_editions_worker_lost_after_its_early_rows_were_shown_is_not_lost_to_the_list(queues):
    # Codex review of dev PR #128: the rows shown are the start of the list also when that worker
    # fails in saving what it found. With none shown, the main rows stand alone, as before.
    early = []
    rows = _engine().execute_search('early lost', 'literal', 0, corpus_scope='genizah', sources='all',
                                    preview_callback=early.append)
    assert early and [r['uid'] for r in rows] == ['P2', 'P1'] and len(rows[0]['members']) == 2
    assert consume_editions_skipped() is False and consume_last_search_cutoff()['capped'] is True
    rows = _engine().execute_search('early lost unshown', 'literal', 0, corpus_scope='genizah', sources='all',
                                    preview_callback=early.append)
    assert [r['uid'] for r in rows] == ['P1', 'P2'] and consume_editions_skipped() is True


def test_without_early_rows_the_workers_share_no_file(queues):
    _engine().execute_search('q', 'literal', 0, corpus_scope='genizah', sources='all')
    assert all('editions_file' not in p and 'preview' not in p for _q, p in queues['submitted'])


def test_a_search_that_shows_its_first_rows_runs_in_one_worker(queues):
    # The API passes its limit as max_rows (2026-11-25): the coordinator runs the editions leg
    # first and builds only the main rows that can be among the first results
    # (SearchEngine._first_grouped_rows), which two legs in two workers cannot know.
    value = _engine().execute_search('q', 'literal', 0, corpus_scope='genizah', sources='all', max_rows=5)
    assert [(q is queues['main'], p['method']) for q, p in queues['submitted']] == [(True, 'execute_search')]
    assert value['arguments']['max_rows'] == 5 and value['arguments']['sources'] == 'all'
    # Its legs, were they ever run apart, take no max_rows (execute_search_leg has none).
    from web.research_jobs import _leg_arguments
    assert 'max_rows' not in _leg_arguments({'query_str': 'q', 'max_rows': 5}, 'main')


def test_other_source_choices_are_one_worker(queues):
    eng = _engine()
    for sources in (None, 'automatic', 'editions'):
        queues['submitted'].clear()
        value = eng.execute_search('x', 'literal', 0, sources=sources)
        assert len(queues['submitted']) == 1 and queues['submitted'][0][0] is queues['main']
        assert value['method'] == 'execute_search'
    # Switched off: one worker, which opens no editions.
    queues['submitted'].clear()
    import os
    os.environ['EDITIONS_SEARCH_ENABLED'] = '0'
    try:
        eng.execute_search('x', 'literal', 0, sources='all')
    finally:
        os.environ['EDITIONS_SEARCH_ENABLED'] = '1'
    assert [p['method'] for _q, p in queues['submitted']] == ['execute_search']
    # Title / Shelfmark and My Library only never read the editions.
    queues['submitted'].clear()
    eng.execute_search('x', 'literal', 0, sources='all', corpus_scope='local')
    assert [p['method'] for _q, p in queues['submitted']] == ['execute_search']


def test_a_failed_editions_worker_leaves_the_main_rows_with_a_note(queues):
    rows = _engine().execute_search('edfail', 'literal', 0, sources='all')
    assert [r['uid'] for r in rows] == ['P1', 'P2']
    assert all(r['sources'] == ['automatic'] for r in rows)
    assert consume_editions_skipped() is True
    assert consume_editions_skipped() is False           # said once


def test_a_worker_with_no_editions_open_is_said_not_passed_off_as_no_match(queues):
    """A Scholarly search whose worker could not open the editions: the note, not a plain
    "0 results" (Codex review of PR #18)."""
    _engine().execute_search('edunavailable', 'literal', 0, sources='editions')
    assert consume_editions_skipped() is True
    _engine().execute_search('x', 'literal', 0, sources='editions')
    assert consume_editions_skipped() is False


def test_a_full_editions_queue_leaves_the_main_rows_with_a_note(queues, monkeypatch):
    def full(payload):
        raise ResearchJobError('The search queue is full. Please try again shortly.')
    monkeypatch.setattr(queues['editions'], 'submit', full)
    rows = _engine().execute_search('x', 'literal', 0, sources='all')
    assert [r['uid'] for r in rows] == ['P1', 'P2']
    assert consume_editions_skipped() is True


def test_a_failed_main_worker_fails_the_search_and_stops_the_editions(queues):
    with pytest.raises(ResearchJobError):
        _engine().execute_search('mainfail slow', 'literal', 0, sources='all')
    ed = [p for _q, p in queues['submitted'] if p['arguments']['leg'] == 'editions']
    assert ed


def test_the_cut_off_of_either_leg_is_the_searchs(queues):
    _engine().execute_search('capped', 'literal', 0, sources='all')
    assert consume_last_search_cutoff()['capped'] is True


def test_stop_ends_both_workers(queues):
    cancel = threading.Event()
    engine = IsolatedEngine(_engine()._engine, 'search', cancel=cancel)
    errors = []

    def run():
        try:
            engine.execute_search('block', 'literal', 0, sources='all')
        except BaseException as exc:          # noqa: BLE001 -- the test reads it
            errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    pid_files = []
    deadline = time.monotonic() + 20
    while len(pid_files) < 2:
        assert time.monotonic() < deadline, 'both workers did not start'
        pid_files = list((queues['tmp'] / 'pids').glob('*.pid'))
        time.sleep(0.05)
    pids = [int(p.read_text()) for p in pid_files[:2]]
    cancel.set()
    thread.join(timeout=20)
    assert errors and isinstance(errors[0], InterruptedError)
    deadline = time.monotonic() + 10
    def alive(pid):
        try:
            return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
    while any(alive(pid) for pid in pids):
        assert time.monotonic() < deadline, 'a leg kept running after Stop'
        time.sleep(0.05)
    assert {p.stem for p in pid_files} == {'main', 'editions'}


def test_a_spare_is_warm_only_when_its_catalogue_loaded(tmp_path):
    """The web timing gate waits for warm workers; a failed preload is not warm (Codex, #17)."""
    import sys
    import threading
    from types import SimpleNamespace
    from shared.research_worker import mark_warm
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
    from verify_editions_search import _wait_warm
    ok, failed = tmp_path / 'ok', tmp_path / 'failed'
    ok.mkdir()
    failed.mkdir()
    mark_warm(ok, object())
    mark_warm(failed, None)
    assert (ok / 'warm.ready').exists() and not (ok / 'warm.failed').exists()
    assert (failed / 'warm.failed').exists() and not (failed / 'warm.ready').exists()

    def queue(root):
        return SimpleNamespace(condition=threading.Condition(), capacity=1, spares={0: SimpleNamespace(root=root)})
    _wait_warm(queue(ok), timeout=1)
    with pytest.raises(RuntimeError, match='catalogue'):
        _wait_warm(queue(failed), timeout=1)


def test_the_deadline_after_the_main_leg_leaves_its_rows(queues):
    """A deadline (the API's core timeout) that passes while only the editions leg is still
    running gives the main rows with the note, not a 504 (Codex review of PR #17)."""
    engine = IsolatedEngine(_engine()._engine, 'search').controlled(seconds=2)
    started = time.monotonic()
    rows = engine.execute_search('edslow', 'literal', 0, sources='all')
    assert time.monotonic() - started < 4.5
    assert [r['uid'] for r in rows] == ['P1', 'P2'] and all(r['sources'] == ['automatic'] for r in rows)
    assert consume_editions_skipped() is True
    assert consume_last_search_cutoff()['interrupted'] is False      # the main rows are complete


def test_the_deadline_before_the_main_leg_still_fails_the_search(queues):
    from shared.search_regex import SearchBudgetExceeded
    engine = IsolatedEngine(_engine()._engine, 'search').controlled(seconds=0.3)
    with pytest.raises(SearchBudgetExceeded):
        engine.execute_search('slow', 'literal', 0, sources='all')


def test_a_stuck_editions_worker_is_dropped_not_the_search(queues, monkeypatch):
    """The editions worker stuck past its time limit and grace is stopped; the main rows stand."""
    from shared.config import Config
    monkeypatch.setattr(research_jobs, 'TIME_LIMIT_GRACE_SECONDS', 0)
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 3)   # the main stand-in starts in about a second
    rows = _engine().execute_search('edstuck', 'literal', 0, sources='all')
    assert [r['uid'] for r in rows] == ['P1', 'P2']
    assert consume_editions_skipped() is True
