# -*- coding: utf-8 -*-
"""The desktop's startup builds, one at a time, and builds that give way to the user's searches
(desktop/startup_work.py, shared/background_work.py; 2026-11-23).

Measured before the change: about 33 s of Python started in five or six threads within the first
6 s of the desktop app, and the window froze for 13-21 s. These tests pin the parts the change
rests on: the queue's order and its gate, the builds giving way (and not giving way where they
must not), the GIL switch interval coming back, the cheaper FL-id lookup giving the same index,
and the CUDL alias index built on first need instead of silently missing.
"""
from __future__ import annotations

import inspect
import re
import sys
import threading
import time
import types

import pytest
from PyQt6.QtCore import Qt

from shared import background_work


@pytest.fixture(autouse=True)
def _clean_background_work():
    interval = sys.getswitchinterval()
    yield
    background_work._user_threads.clear()
    background_work._yielding_threads.clear()
    background_work._stopped_yielding.clear()
    sys.setswitchinterval(interval)


def _thread(fn):
    out = {}

    def body():
        out['value'] = fn()
    t = threading.Thread(target=body, daemon=True)
    t.start()
    return t, out


def _timed_checkpoint(wanted=None):
    def run():
        with background_work.yielding():
            t0 = time.monotonic()
            background_work.checkpoint(wanted)
            return time.monotonic() - t0
    return run


# --- shared/background_work.py ---------------------------------------------------------------

def test_a_yielding_build_waits_while_a_search_runs():
    background_work.user_work_started()
    t, out = _thread(_timed_checkpoint())
    time.sleep(0.3)
    assert t.is_alive()                     # still waiting for the search
    background_work.user_work_ended()
    t.join(2)
    assert not t.is_alive() and out['value'] >= 0.25


def test_no_other_thread_ever_waits():
    background_work.user_work_started()
    t0 = time.monotonic()
    background_work.checkpoint()            # the UI thread, a search worker, the web
    assert time.monotonic() - t0 < 0.05
    t, out = _thread(lambda: background_work.checkpoint())
    t.join(1)
    assert not t.is_alive()


def test_a_build_someone_waits_for_does_not_give_way():
    background_work.user_work_started()
    t, out = _thread(_timed_checkpoint(wanted=lambda: True))
    t.join(1)
    assert not t.is_alive() and out['value'] < 0.05


def test_a_build_stops_giving_way_after_max_wait(monkeypatch):
    monkeypatch.setattr(background_work, 'MAX_WAIT', 0.2)
    background_work.user_work_started()     # never ends: a worker that did not say so

    def run():
        with background_work.yielding():
            t0 = time.monotonic()
            background_work.checkpoint()
            first = time.monotonic() - t0
            t1 = time.monotonic()
            background_work.checkpoint()
            return first, time.monotonic() - t1
    t, out = _thread(run)
    t.join(2)
    first, second = out['value']
    assert first >= 0.15 and second < 0.05


def test_user_work_is_kept_per_thread():
    background_work.user_work_started()
    background_work.user_work_started()     # resumed from Pause: the same thread again
    assert background_work.user_work_running()
    background_work.user_work_ended()
    assert not background_work.user_work_running()


def test_fast_switching_gives_the_interval_back():
    before = sys.getswitchinterval()
    with background_work.fast_switching():
        assert sys.getswitchinterval() == pytest.approx(background_work.FAST_SWITCH_INTERVAL)
        background_work.user_work_started()   # two busy threads: the old interval
        assert sys.getswitchinterval() == pytest.approx(before)
        background_work.user_work_ended()
        assert sys.getswitchinterval() == pytest.approx(background_work.FAST_SWITCH_INTERVAL)
    assert sys.getswitchinterval() == pytest.approx(before)


def test_an_on_demand_build_is_wanted_while_a_caller_waits():
    build = background_work.OnDemandBuild()

    def waiter():
        with build.held():
            pass
    with build.held():
        t, _ = _thread(waiter)
        deadline = time.monotonic() + 2
        while not build.wanted() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert build.wanted()
    t.join(2)
    assert not t.is_alive() and not build.wanted()


def test_search_workers_say_when_they_run():
    from desktop.gui_threads import _allow_sleep, _prevent_sleep
    _prevent_sleep()
    assert background_work.user_work_running()
    _allow_sleep()
    assert not background_work.user_work_running()


# --- the FL-id lookup (shared/search_engine.py) ------------------------------------------------

def _engine(browse_map):
    from shared.search_engine import SearchEngine
    eng = SearchEngine.__new__(SearchEngine)
    eng._fl_id_index = None
    eng._fl_id_index_building = False
    eng._fl_id_index_lock = threading.Lock()
    eng._fl_id_index_generation = 0
    eng._load_browse_map = lambda: browse_map
    return eng


HEADERS = [
    '###990000000000000001_IE1234_P0001_FL7654321 xml - IE1234/FL7654321.xml',
    'IE12_P3_FL000123',
    'a FL12 b FL34',              # the first FL id
    'no id here', 'FL', 'FLx12', '',
    '9700000000000001_F0001_P0002',   # a My Library page: no FL id
]


def test_the_fl_lookup_is_what_parse_full_id_components_gives():
    from shared.metadata_manager import MetadataManager
    browse_map = {'990000000000000001': [{'full_header': h} for h in HEADERS],
                  '990000000000000002': [{'full_header': 'IE9_P1_FL5'}, {}, {'full_header': 'FL5'}]}
    eng = _engine(browse_map)
    assert eng._build_fl_id_index() is True
    expected = {}
    for sid, pages in browse_map.items():
        for idx, page in enumerate(pages):
            fl = MetadataManager.parse_full_id_components(MetadataManager, page.get('full_header', ''))['fl_id']
            digits = re.sub(r"\D", "", str(fl)) if fl else ''
            if digits:
                expected.setdefault(digits, []).append((sid, idx))
    assert eng._fl_id_index == expected
    assert eng._fl_id_index['5'] == [('990000000000000002', 0), ('990000000000000002', 2)]


def test_build_fl_id_index_now_builds_once():
    eng = _engine({'990000000000000001': [{'full_header': 'IE1_P1_FL42'}]})
    eng._fl_id_index_building = True           # a build is running
    eng.build_fl_id_index_now()
    assert eng._fl_id_index is None
    eng._fl_id_index_building = False
    eng.build_fl_id_index_now()
    assert eng._fl_id_index == {'42': [('990000000000000001', 0)]}
    assert eng._fl_id_index_building is False


def test_the_desktop_engine_leaves_the_fl_lookup_to_the_queue():
    from desktop.gui_threads import StartupThread
    from shared.search_engine import SearchEngine
    src = inspect.getsource(StartupThread.run)
    assert 'SearchEngine(meta_mgr, var_mgr, build_fl_index=False)' in src
    assert 'start_background_loading' not in src
    assert 'and build_fl_index:' in inspect.getsource(SearchEngine.__init__)


# --- the CUDL alias index on first need (shared/shelfmark_bridge.py) ---------------------------

BANK = {
    '990000000000000001': {'shelfmark': 'T-S 12.123', 'call_numbers_raw': ['T-S 12.123'],
                           'library_code': 'CUL'},
    '990000000000000002': {'shelfmark': 'T-S NS 5.6', 'call_numbers_raw': ['Ms. T-S NS 5/6'],
                           'library_code': 'CUL'},
}


@pytest.fixture
def bridge(tmp_path, monkeypatch):
    from shared import shelfmark_bridge as b
    monkeypatch.setattr(b, '_CUDL_ALIAS_INDEX', None)
    monkeypatch.setattr(b, '_ALIAS_SOURCE', None)
    real = b.build_alias_index
    builds = []

    def build(bank, report_path=None, wanted=None):
        builds.append(len(bank))
        return real(bank, report_path=tmp_path / 'collisions.csv', wanted=wanted)
    monkeypatch.setattr(b, 'build_alias_index', build)
    b.builds = builds
    return b


def test_without_deferral_an_unbuilt_alias_index_answers_nothing(bridge):
    assert bridge.lookup_cudl('T-S NS 5.6') is None
    assert bridge.builds == []


def _off_ui(fn):
    t, out = _thread(fn)
    t.join(10)
    return out['value']


def test_a_deferred_alias_index_is_built_by_the_first_lookup_off_the_ui_thread(bridge):
    bridge.defer_alias_index(lambda: BANK)
    assert _off_ui(lambda: bridge.lookup_cudl('T-S NS 5.6')) == {
        'sys_id': '990000000000000002', 'shelfmark': 'T-S NS 5.6'}
    assert bridge.lookup_cudl('ts12.123')['sys_id'] == '990000000000000001'
    assert bridge.builds == [2]                 # once
    assert bridge.ensure_alias_index() is True and bridge.builds == [2]


def test_a_lookup_on_the_ui_thread_never_builds_it(bridge):
    # Browse resolves a typed shelfmark on the UI thread: seconds of CPU there would freeze
    # the window. It answers as before the index existed, until the queue has built it.
    bridge.defer_alias_index(lambda: BANK)
    assert bridge.lookup_cudl('T-S NS 5.6') is None
    assert bridge.builds == [] and bridge._CUDL_ALIAS_INDEX is None


def test_a_deferred_alias_index_waits_for_the_catalogue(bridge):
    ready = []
    bridge.defer_alias_index(lambda: BANK if ready else None)
    assert _off_ui(lambda: bridge.lookup_cudl('T-S NS 5.6')) is None
    assert bridge._CUDL_ALIAS_INDEX is None and bridge.builds == []
    ready.append(True)
    assert _off_ui(lambda: bridge.lookup_cudl('T-S NS 5.6'))['sys_id'] == '990000000000000002'


# --- libraries.csv and the Oxford parts (shared/metadata_manager.py) ---------------------------

def _manager():
    from shared.metadata_manager import MetadataManager
    mgr = MetadataManager.__new__(MetadataManager)
    mgr.csv_bank = {}
    mgr._csv_bank_loaded = False
    mgr._csv_bank_done = threading.Event()
    return mgr


def test_load_catalogue_now_leaves_the_alias_index_for_later(bridge, tmp_path, monkeypatch):
    from shared.config import Config
    csv_path = tmp_path / 'libraries.csv'
    csv_path.write_text('system_number,oxford_part_id,call_numbers,library_code,a,b,c,titles\n'
                        '990000000000000002,,Ms. T-S NS 5/6|T-S NS 5.6,CUL,,,,A title\n',
                        encoding='utf-8')
    monkeypatch.setattr(Config, 'LIBRARIES_CSV', str(csv_path))
    mgr = _manager()
    mgr.load_catalogue_now()
    assert mgr._csv_bank_loaded and mgr._csv_bank_done.is_set()
    assert mgr.csv_bank['990000000000000002']['shelfmark'] == 'T-S NS 5.6'
    assert bridge.builds == [] and bridge._ALIAS_SOURCE() is mgr.csv_bank
    assert _off_ui(lambda: bridge.lookup_cudl('T-S NS 5.6'))['sys_id'] == '990000000000000002'


def test_wait_for_oxford_parts_waits_only_for_an_expected_load():
    mgr = _manager()
    codico = types.SimpleNamespace(_loaded=False)
    codico.load = lambda csv_bank=None: setattr(codico, '_loaded', True)
    mgr.codico_mgr = codico
    t0 = time.monotonic()
    assert mgr.wait_for_oxford_parts(timeout=5) is False      # none expected: at once
    assert time.monotonic() - t0 < 0.1
    mgr.expect_oxford_parts()

    def later():
        time.sleep(0.2)
        mgr.load_oxford_parts_now()
    t, _ = _thread(later)
    t0 = time.monotonic()
    assert mgr.wait_for_oxford_parts(timeout=5) is True
    assert time.monotonic() - t0 >= 0.15
    t.join(1)


# --- the queue (desktop/startup_work.py) -------------------------------------------------------

class _Meta:
    def __init__(self, log):
        self.log = log
        self.csv_bank = {'990000000000000001': {'shelfmark': 'T-S 12.123'},
                         '990000000000000002': {'shelfmark': 'MS. Heb. a.1'}}

    def _record(self, name):
        self.log.append((name, sys.getswitchinterval(),
                         threading.get_ident() in background_work._yielding_threads))

    def expect_oxford_parts(self):
        self.log.append('expected')

    def load_catalogue_now(self):
        self._record('catalogue')

    def load_oxford_parts_now(self):
        self._record('oxford parts')

    def get_part_autocomplete_list(self):
        return [{'display': 'heb. d. 29 part 2', 'normalized': 'hebd29part2',
                 'part_id': 'MS. Heb. d. 29/2'}]

    def prewarm_shelfmark_search(self, timeout):
        self._record('shelfmark search index')
        return True

    def prewarm_browse_resolver(self):
        self._record('browse resolver index')
        return True

    def _build_file_map_background(self):
        self._record('file map')


class _Searcher:
    def __init__(self, meta, fail=False):
        self.meta = meta
        self.fail = fail

    def _load_browse_map(self):
        self.meta._record('browse map')
        if self.fail:
            raise OSError("no browse map")

    def build_fl_id_index_now(self):
        self.meta._record('fl-id lookup')


@pytest.fixture
def queue(monkeypatch):
    from desktop.startup_work import StartupWork
    from shared import shelfmark_bridge
    made = []

    def make(fail=False, gate_timeout=5.0):
        log = []
        meta = _Meta(log)
        monkeypatch.setattr(shelfmark_bridge, 'ensure_alias_index', lambda: meta._record('alias index'))
        work = StartupWork(meta, _Searcher(meta, fail=fail), gate_timeout=gate_timeout,
                           gc_freeze=lambda: log.append('gc freeze'))
        work.rows = []
        work.phase1_signals = []
        # Direct: delivered in the queue's thread, no event loop needed.
        work.completer_rows_ready.connect(work.rows.append, Qt.ConnectionType.DirectConnection)
        work.phase1_ready.connect(lambda: work.phase1_signals.append(list(work.steps_done)),
                                  Qt.ConnectionType.DirectConnection)
        made.append(work)
        return work, log
    yield make
    for work in made:
        work.open_gate()
        if work._thread is not None:
            work._thread.join(5)


def _wait(cond, seconds=5):
    deadline = time.monotonic() + seconds
    while not cond() and time.monotonic() < deadline:
        time.sleep(0.01)
    return cond()


def test_phase_two_waits_for_the_restored_manuscript(queue):
    work, log = queue()
    work.start()
    assert _wait(lambda: len(work.steps_done) == 3)
    time.sleep(0.3)
    assert work.steps_done == ['browse map', 'catalogue', 'oxford parts']   # the gate is shut
    assert work.browse_map_loaded and work.phase1_done and not work.done
    # A Browse state without a sys_id is restored on this: the catalogue and the parts are in.
    assert work.phase1_signals == [['browse map', 'catalogue', 'oxford parts']]
    work.open_gate()
    assert _wait(lambda: work.done)
    assert work.steps_done == ['browse map', 'catalogue', 'oxford parts', 'completer rows',
                               'browse resolver index', 'alias index', 'shelfmark search index',
                               'fl-id lookup', 'file map']
    assert work.browse_resolver_built
    assert log[0] == 'expected'
    # gc.freeze() after every build: the next full collection does not walk what it made.
    assert log.count('gc freeze') == 9
    assert [e for e in log[1:] if e != 'gc freeze'][0][0] == 'browse map' and log[2] == 'gc freeze'
    builds = [e for e in log[1:] if e != 'gc freeze']
    phase1 = builds[:3]
    phase2 = builds[3:]
    fast = background_work.FAST_SWITCH_INTERVAL
    assert all(interval == pytest.approx(fast) and not yielding for _n, interval, yielding in phase1)
    assert all(interval == pytest.approx(fast) and yielding for _n, interval, yielding in phase2)
    assert work.rows == [[('MS. Heb. a.1', 'heb.a.1', None), ('T-S 12.123', 'ts12.123', None),
                          ('heb. d. 29 part 2', 'hebd29part2', 'MS. Heb. d. 29/2')]]


def test_the_gate_opens_by_itself_after_its_timeout(queue):
    work, _log = queue(gate_timeout=0.2)
    work.start()
    assert _wait(lambda: work.done, seconds=5)
    assert len(work.steps_done) == 9


def test_a_failed_build_does_not_stop_the_others(queue):
    work, _log = queue(fail=True)
    work.start()
    assert _wait(lambda: len(work.steps_done) == 3)
    assert work.browse_map_loaded                 # the restore goes on: it loads the map itself
    work.open_gate()
    assert _wait(lambda: work.done) and len(work.steps_done) == 9


def test_the_interval_is_given_back_when_the_queue_ends(queue):
    before = sys.getswitchinterval()
    work, _log = queue(gate_timeout=0)
    work.start()
    assert _wait(lambda: work.done)
    work._thread.join(5)
    assert sys.getswitchinterval() == pytest.approx(before)


def test_the_fl_fallback_scan_finds_what_the_lookup_finds():
    # Before the lookup is built (phase 2), Browse by FL id scans the map with the same search.
    pages = {'990000000000000001': [{'uid': 'u1', 'p_num': '1', 'full_header': 'x_P1_FL7'},
                                    {'uid': 'u2', 'p_num': '2', 'full_header': 'a FL12 b FL34'}],
             '990000000000000002': [{'uid': 'u3', 'p_num': '1', 'full_header': 'y_P1_FL34'}]}
    eng = _engine(pages)
    eng.get_full_text_by_id = lambda uid: ''

    def found():
        return {fl: (eng.get_browse_page_by_fl(fl) or {}).get('uid') for fl in ('7', 'FL12', '34', '99')}
    scanned = found()
    eng._build_fl_id_index()
    assert eng._fl_id_index is not None
    assert scanned == found() == {'7': 'u1', 'FL12': 'u2', '34': 'u3', '99': None}


def test_browse_resolver_index_is_built_once_when_two_threads_ask():
    # The queue builds it while the UI thread's Go to asks: the second waits, not builds again.
    mgr = _manager()
    mgr.csv_bank = {'990000000000000001': {'shelfmark': 'T-S 12.123'}}
    mgr._csv_bank_loaded = True
    mgr._shelfmark_index = None
    mgr._cached_shelfmarks = lambda: {}
    builds = []

    def sources(_cached):
        builds.append(1)
        time.sleep(0.3)
        yield '990000000000000001', 'T-S 12.123', ''
    mgr._iter_shelfmark_sources = sources
    t, out = _thread(mgr.prewarm_browse_resolver)
    time.sleep(0.1)
    index = mgr._get_shelfmark_index()
    t.join(5)
    assert builds == [1] and out['value'] is True
    assert list(index) == [('990000000000000001', 'T-S 12.123', '', 'ts12.123', 'ts12123')]


def test_browse_resolver_waits_for_the_whole_catalogue():
    mgr = _manager()
    mgr.csv_bank = {'990000000000000001': {'shelfmark': 'T-S 12.123'}}   # still being read
    assert mgr.prewarm_browse_resolver() is False
    assert getattr(mgr, '_shelfmark_index', None) is None


def _resolver_manager(rows=5):
    mgr = _manager()
    mgr.csv_bank = {f'99000000000000000{i}': {'shelfmark': f'T-S {i}.1'} for i in range(rows)}
    mgr._csv_bank_loaded = True
    mgr._shelfmark_index = None
    mgr._cached_shelfmarks = lambda: {}
    mgr._iter_shelfmark_sources = lambda _cached: (
        (sid, data['shelfmark'], '') for sid, data in mgr.csv_bank.items())
    return mgr


def test_the_queues_resolver_build_gives_way_to_a_search():
    mgr = _resolver_manager()
    background_work.user_work_started()

    def build():
        with background_work.yielding():
            return mgr.prewarm_browse_resolver()
    t, out = _thread(build)
    time.sleep(0.3)
    assert t.is_alive() and mgr._shelfmark_index is None     # paused at its first checkpoint
    background_work.user_work_ended()
    t.join(2)
    assert out['value'] is True and len(mgr._shelfmark_index) == 5


def test_a_go_to_waiting_for_the_resolver_lets_the_queues_build_go_on():
    mgr = _resolver_manager()
    background_work.user_work_started()          # a search runs

    def build():
        with background_work.yielding():
            return mgr.prewarm_browse_resolver()
    t, out = _thread(build)
    time.sleep(0.2)
    t0 = time.monotonic()
    index = mgr._get_shelfmark_index()           # the UI thread's Go to: waits, wanted
    assert time.monotonic() - t0 < 2 and len(index) == 5
    t.join(2)
    assert out['value'] is True


def test_a_refinement_replay_is_user_work(monkeypatch):
    # The session restore replays a saved refinement chain: a search per step, so the startup
    # builds give way to it as to any search.
    import shared.refinement as refinement
    from desktop.gui_threads import RefinementReplayThread
    seen = []

    def replay(chain, searcher, restrict, searcher_for_step=None):
        seen.append(background_work.user_work_running())
        return {'restrict': None}
    monkeypatch.setattr(refinement, 'replay_chain', replay)
    RefinementReplayThread([], object(), None).run()        # in this thread, as a QThread would
    assert seen == [True] and not background_work.user_work_running()


def test_the_legacy_file_map_scan_gives_way_to_a_search(tmp_path, monkeypatch):
    from shared.config import Config
    corpus = tmp_path / 'AllGenizah_OLD.txt'
    corpus.write_text('### IE1_P1_FL5 xml - a/b.xml\ntext\n', encoding='utf-8')
    monkeypatch.setattr(Config, 'FILE_V7', str(corpus))
    monkeypatch.setattr(Config, 'CACHE_META', str(tmp_path / 'metadata_cache.pkl'))
    mgr = _manager()
    mgr.meta_map = {}
    background_work.user_work_started()

    def build():
        with background_work.yielding():
            mgr._build_file_map_background()
    t, _ = _thread(build)
    time.sleep(0.3)
    assert t.is_alive() and mgr.meta_map == {}             # paused at its first checkpoint
    background_work.user_work_ended()
    t.join(2)
    assert mgr.meta_map == {'IE1_P1_FL5': 'a/b.xml'}


def test_a_worker_stopped_without_saying_so_no_longer_holds_the_builds():
    # QThread.terminate() skips the worker's finally: its mark goes once alive() says it is gone,
    # not after MAX_WAIT.
    running = [True]
    t, _ = _thread(lambda: background_work.user_work_started(alive=lambda: running[0]))
    t.join(1)                                  # the "worker" left without user_work_ended
    assert background_work.user_work_running()
    w, out = _thread(_timed_checkpoint())
    time.sleep(0.3)
    assert w.is_alive()                        # still waiting: the worker looks alive
    running[0] = False                         # terminated
    w.join(2)
    assert not w.is_alive() and out['value'] < 5 and not background_work.user_work_running()


def test_the_join_lab_cross_side_search_is_user_work(monkeypatch):
    from desktop.join_workbench import _CrossSideWorker as worker_cls
    seen = []
    monkeypatch.setattr(worker_cls, '_search', lambda self: seen.append(background_work.user_work_running()))
    worker_cls(None, [], None, {}, 'AND', None).run()
    assert seen == [True] and not background_work.user_work_running()
