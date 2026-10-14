# -*- coding: utf-8 -*-
"""Composition Search conditions on the desktop: the worker, the adapter, the
dispatch gate and Stop (gates G4 and G5 of
docs/plans/COMPOSITION_CONDITIONS_PLAN.md).

Qt-free: the worker's REAL `run` is borrowed onto a stub, as in
tests/test_desktop_multi_witness.py, and window methods are borrowed onto a
stub window, as in tests/test_desktop_passage_gate.py. The adapter test runs
the real adapter -> PassageSearcher -> masked search on a small built index.
"""
from __future__ import annotations

import pytest

import desktop.passage_lifecycle as pl
from desktop import gui_threads
from desktop import passage_witnesses as pw
from shared import compound_query as cq
from shared.compound_query import Condition
from shared.passage_parallels import ConditionSearch

SEED = pw.WITNESS_SEED_ID
S1, S2, S3 = '990000000000000001', '990000000000000002', '990000000000000003'


def rid(sys_id, ie, p):
    return f'{sys_id}_IE{ie}_P{p:06d}_FL1'


class _Sig:
    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args if len(args) != 1 else args[0])


class _Searcher:
    """Main-text rows by text; condition answers by text; records calls."""

    def __init__(self, main=None, cond=None, raises=None, on_call=None):
        self.main = main or {}
        self.cond = cond or {}
        self.raises = raises or {}
        self.on_call = on_call
        self.calls = []

    def search_composition_logic(self, text, **kw):
        self.calls.append(('main', text, kw))
        if self.on_call:
            self.on_call(len(self.calls))
        if text in self.raises:
            raise self.raises[text]
        return self.main.get(text, {'main': [], 'filtered': []})

    def search_condition(self, text, pages=(), sys_ids=(), should_stop=None):
        self.calls.append(('cond', text, {'pages': pages, 'sys_ids': sys_ids,
                                          'should_stop': should_stop}))
        if self.on_call:
            self.on_call(len(self.calls))
        if text in self.raises:
            raise self.raises[text]
        answer = self.cond.get(text, ())
        if isinstance(answer, ConditionSearch):
            return answer
        return ConditionSearch(tuple(answer), (), {}, False)


mixin = gui_threads.PausableSearchMixin
CT = gui_threads.CompoundCompositionThread


def _worker(main_list, conditions, searcher, pinned=None):
    class _W:
        run = CT.run
        _abort_if_generation_moved = CT._abort_if_generation_moved
        _raise_if_stopped = CT._raise_if_stopped
        _between_steps = CT._between_steps
        _checkpoint = mixin._checkpoint
        _should_abort = mixin._should_abort
        _emit_pause_ack = mixin._emit_pause_ack
        _init_pause_support = mixin._init_pause_support

    w = _W()
    w.searcher = searcher
    w.main_witnesses = list(main_list)
    w.conditions = tuple(conditions)
    w.filter_text = None
    w.restrict_sys_ids = None
    w.corpus_scope = 'genizah'
    w.pinned_generation = pinned
    w.group_cap = None
    w.fingerprint = 'fp'
    for name in ('progress_signal', 'status_signal', 'scan_finished_signal',
                 'error_signal', 'perf_signal', 'pause_ack_signal', 'step_signal'):
        setattr(w, name, _Sig())
    w._init_pause_support(7)
    return w


def _rows(*headers):
    return {'main': [{'raw_header': h, 'score': 100.0 - i, 'uid': h}
                     for i, h in enumerate(headers)], 'filtered': []}


MAIN = 'main text'
P1, P2, P3 = rid(S1, 1, 5), rid(S2, 1, 1), rid(S3, 1, 1)


def _result(w):
    assert w.error_signal.calls == [], w.error_signal.calls
    assert len(w.scan_finished_signal.calls) == 1
    return w.scan_finished_signal.calls[0]


# -- G4: the worker ----------------------------------------------------------------

def test_every_step_runs_main_first_then_each_version():
    c1 = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('hg one', 'hg two'))
    c2 = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('rashi',))
    s = _Searcher(main={MAIN: _rows(P1, P2)})
    w = _worker([(SEED, 'Your text', MAIN), ('w1', 'B', 'witness')], [c1, c2], s)
    w.run()
    assert [(k, t) for k, t, _ in s.calls] == [
        ('main', MAIN), ('main', 'witness'),
        ('cond', 'hg one'), ('cond', 'hg two'), ('cond', 'rashi')]
    # Each condition is searched over the scope its place defines.
    assert set(s.calls[2][2]['sys_ids']) == {S1, S2}
    assert set(s.calls[4][2]['pages']) == {(S1, 'IE1', 4), (S1, 'IE1', 6),
                                           (S2, 'IE1', 0), (S2, 'IE1', 2)}
    # Main-text searches get a checkpoint; conditions get should_stop.
    assert callable(s.calls[0][2]['checkpoint'])
    assert callable(s.calls[2][2]['should_stop'])


def test_judgement_end_to_end_removed_kept_and_dropped():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('hg',))
    s = _Searcher(main={MAIN: _rows(P1, P2, P3)},
                  cond={'hg': (rid(S2, 4, 30),)})      # another page of S2
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert [r['raw_header'] for r in out['main']] == [P1, P3]
    removed = out['compound']['removed']
    assert [r['raw_header'] for r in removed] == [P2]
    assert removed[0]['compound']['evidence'] == [[rid(S2, 4, 30)]]
    assert out['compound']['run_id'] == 7 and out['partial'] is False


def test_a_failed_condition_version_is_unresolved_and_the_run_goes_on():
    c1 = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('boom',))
    c2 = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('ok',))
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'ok': (P1,)},
                  raises={'boom': RuntimeError('engine')})
    w = _worker([(SEED, '', MAIN)], [c1, c2], s)
    w.run()
    out = _result(w)
    assert out['main'] == []
    [row] = out['compound']['unresolved']
    assert row['compound']['states'] == [cq.UNRESOLVED, cq.SATISFIED]
    assert row['compound']['reasons'] == [cq.CUT_FAILED, '']


def test_a_stop_inside_a_condition_leaves_the_rest_not_run():
    c1 = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('first',))
    c2 = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('second',))
    stopped = ConditionSearch((), (), {}, True)
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'first': stopped})
    w = _worker([(SEED, '', MAIN)], [c1, c2], s)
    w.run()
    out = _result(w)
    assert [t for k, t, _ in s.calls if k == 'cond'] == ['first']
    [row] = out['compound']['unresolved']
    assert row['compound']['reasons'] == [cq.CUT_STOPPED, cq.CUT_NOT_RUN]
    assert out['partial'] is True and out['main'] == []


def test_stop_between_steps_runs_nothing_more():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('a', 'b'))
    s = _Searcher(main={MAIN: _rows(P1)})
    w = _worker([(SEED, '', MAIN)], [c], s)
    s.on_call = lambda n: setattr(w, 'cancel_flag', n >= 2)
    w.run()
    out = _result(w)
    assert [t for k, t, _ in s.calls] == [MAIN, 'a']
    [row] = out['compound']['unresolved']
    assert row['compound']['reasons'] == [cq.CUT_NOT_RUN]


def test_the_main_text_checkpoint_raises_once_stopped():
    w = _worker([(SEED, '', MAIN)], [], _Searcher())
    w._raise_if_stopped(0, 1)                 # not stopped: returns
    w.cancel_flag = True
    with pytest.raises(InterruptedError):
        w._raise_if_stopped(0, 1)


@pytest.mark.parametrize('fail', [False, True])
def test_an_index_swap_after_a_step_voids_the_run(monkeypatch, fail):
    """Checked after EVERY step -- including one that failed."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('cond',))
    pinned = pl.current_state_generation()
    raises = {'cond': RuntimeError('x')} if fail else {}
    s = _Searcher(main={MAIN: _rows(P1)}, raises=raises)

    def _swap(n):
        if n == 2:
            monkeypatch.setattr(pl, '_state_generation', pinned + 1)
    s.on_call = _swap
    w = _worker([(SEED, '', MAIN)], [c], s, pinned=pinned)
    w.run()
    assert w.scan_finished_signal.calls == []
    assert w.error_signal.calls


def test_a_failed_seed_fails_the_run():
    s = _Searcher(raises={MAIN: RuntimeError('x')})
    w = _worker([(SEED, '', MAIN)],
                [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('c',))], s)
    w.run()
    assert w.scan_finished_signal.calls == [] and w.error_signal.calls


def test_the_thread_declares_every_signal_dispatch_reaches_for():
    for name in ('progress_signal', 'status_signal', 'scan_finished_signal',
                 'error_signal', 'perf_signal', 'pause_ack_signal', 'step_signal'):
        assert hasattr(CT, name), name


# -- G4: worker -> adapter -> searcher -> index ---------------------------------------

def _aperiodic(n, salt=0):
    out, x = [], 987_654_321 + salt
    for _ in range(n):
        x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        out.append(chr(0x05D0 + (x >> 7) % 22))
    return ''.join(out)


TEXT_B = _aperiodic(90, salt=77)


def _build(tmp_path, name, order):
    from shared.passage_builder import build_index
    from shared.passage_index import open_index
    recs = {
        'a': (rid(S1, 1, 1), _aperiodic(200, 1)),
        'b': (rid(S1, 1, 2), _aperiodic(60, 2) + TEXT_B + _aperiodic(60, 3)),
        'c': (rid(S2, 1, 2), _aperiodic(60, 4) + TEXT_B + _aperiodic(60, 5)),
    }
    d = str(tmp_path / name)
    build_index([recs[k] for k in order], d, partitions=1, apply_hygiene=False)
    return open_index(d)


def _lease(monkeypatch, index_box):
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda: index_box[0])
    monkeypatch.setattr(pl, '_release_lease', lambda: None)


class _NoText:
    def get_full_text_by_header(self, header):
        raise AssertionError('a condition must never fetch page text')


def test_the_adapter_runs_the_real_masked_search(monkeypatch, tmp_path):
    from shared.passage_pages import clear_cache
    clear_cache()
    box = [_build(tmp_path, 'a', 'abc')]
    _lease(monkeypatch, box)
    adapter = pl.get_passage_searcher(_NoText(), render_cap=0)
    found = adapter.search_condition(TEXT_B, pages=((S1, 'IE1', 2),))
    assert found.hit_record_ids == (rid(S1, 1, 2),)       # S2's copy is outside
    found = adapter.search_condition(TEXT_B, sys_ids=(S1, S2))
    assert set(found.hit_record_ids) == {rid(S1, 1, 2), rid(S2, 1, 2)}


def test_a_swap_between_the_check_and_the_lease_resolves_keys_in_the_new_build(
        monkeypatch, tmp_path):
    """The race Codex probed: the scope is computed while build A is current,
    the index swaps to B (same pages, different record ORDER) before the
    lease is taken. Keys are resolved inside the lease, against B."""
    from shared.passage_pages import clear_cache, page_index_for
    clear_cache()
    a = _build(tmp_path, 'a', 'abc')
    b = _build(tmp_path, 'b', 'bca')       # A's record 1 is S2's copy in B
    page_index_for(a)                       # A's map is cached first
    box = [b]
    _lease(monkeypatch, box)
    found = pl.get_passage_searcher(_NoText(), render_cap=0).search_condition(
        TEXT_B, pages=((S1, 'IE1', 2),))
    assert found.hit_record_ids == (rid(S1, 1, 2),)


def test_an_adapter_failure_crosses_the_boundary_as_a_string(monkeypatch):
    class _Boom:
        def __init__(self, **kw):
            pass

        def search_condition(self, *a, **k):
            raise ValueError('inside')
    import shared.passage_parallels as pp
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda: object())
    monkeypatch.setattr(pl, '_release_lease', lambda: None)
    monkeypatch.setattr(pp, 'PassageSearcher', _Boom)
    monkeypatch.setattr(pl, 'compose', lambda *a, **k: 'POLICY')
    with pytest.raises(pl.PassageSearchError) as exc:
        pl.get_passage_searcher(_NoText()).search_condition('x')
    assert exc.value.__context__ is None and 'inside' in str(exc.value)


# -- G5 and Stop: the window --------------------------------------------------------

import genizah_app  # noqa: E402

APP = genizah_app.GenizahGUI


class _Label:
    def __init__(self):
        self.text = ''

    def setText(self, t):
        self.text = t

    def setStyleSheet(self, *_a):
        pass


class _Win:
    _comp_compound_gate = APP._comp_compound_gate
    _comp_compound_main_list = APP._comp_compound_main_list
    _compound_problem_text = APP._compound_problem_text
    _passage_scan_in_flight = APP._passage_scan_in_flight
    _passage_batch_in_flight = APP._passage_batch_in_flight
    _refuse_stop_during_passage_scan = APP._refuse_stop_during_passage_scan
    _comp_witness_state = APP._comp_witness_state

    def __init__(self, conditions):
        self.comp_conditions = list(conditions)
        self.pre_search_restrict_sys_ids = None

    def _get_filter_text(self):
        return ''

    def _rehydrate_witness_texts(self):
        pass


@pytest.fixture
def warnings(monkeypatch):
    shown = []
    monkeypatch.setattr(genizah_app.QMessageBox, 'warning',
                        lambda *a, **k: shown.append(a[2] if len(a) > 2 else ''))
    return shown


GOOD = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('א' * 60,))


@pytest.mark.parametrize('method, lab', [('chunk', False), ('passage', True),
                                         ('chunk', True)])
def test_conditions_never_run_on_another_method(warnings, method, lab):
    w = _Win([GOOD])
    assert w._comp_compound_gate('main ' * 20, method, lab, 'genizah',
                                 'standard-40', 'normal', 'normal') is None
    assert len(warnings) == 1


def test_no_conditions_pass_through_on_any_method(warnings):
    w = _Win([])
    assert w._comp_compound_gate('t', 'chunk', True, 'genizah',
                                 'standard-40', 'normal', 'normal') == ([], [], '')
    assert warnings == []


def test_an_under_floor_or_fifth_condition_is_refused(warnings):
    short = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('א' * 10,))
    for conds in ([short], [GOOD] * 5):
        w = _Win(conds)
        assert w._comp_compound_gate('main', 'passage', False, 'genizah',
                                     'standard-40', 'normal', 'normal') is None
    assert len(warnings) == 2


def test_a_valid_query_gets_its_main_list_and_fingerprint(warnings):
    w = _Win([GOOD])
    conds, main_list, fp = w._comp_compound_gate(
        'the main text', 'passage', False, 'genizah', 'standard-40',
        'normal', 'normal')
    assert conds == [GOOD] and main_list[0][0] == SEED and len(fp) == 64
    assert warnings == []


def test_run_composition_checks_conditions_after_the_demotion():
    """The gate must see the method that will actually RUN: the dispatch-time
    re-validation can demote letter-level to chunk, and a gate placed before it
    would let a chunk search run under unexecuted conditions."""
    import inspect
    src = inspect.getsource(APP.run_composition)
    demote = src.index('self._revert_comp_method_to_chunk(_passage_reason)')
    gate = src.index('self._comp_compound_gate(')
    lab = src.index('if self.btn_lab_mode_toggle_comp.isChecked():')
    assert demote < gate < lab


class _FakeCompound:
    pass


def test_stop_is_allowed_on_a_compound_search(monkeypatch):
    monkeypatch.setattr(genizah_app, 'CompoundCompositionThread', _FakeCompound)
    w = _Win([GOOD])
    w.is_comp_running = True
    w._comp_last_result_method = 'passage'
    w._comp_grouping_active = False
    w.comp_thread = _FakeCompound()
    w.lbl_comp_status = _Label()
    assert w._refuse_stop_during_passage_scan() is False
    from genizah_core import tr
    assert w.lbl_comp_status.text == tr(
        "Stopping. Results found so far are kept; conditions not yet "
        "searched are marked as not fully checked.")


def test_a_stale_runs_result_is_dropped(monkeypatch):
    """A finished run's queued result must not land after a newer run."""
    w = _Win([])
    w._comp_active_run_id = 9
    w._comp_compound = 'untouched'
    w.reset_comp_ui = lambda: (_ for _ in ()).throw(AssertionError('rendered'))
    APP.on_comp_scan_finished(w, {'main': [], 'compound': {'run_id': 8}})
    assert w._comp_compound == 'untouched'
