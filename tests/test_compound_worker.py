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

    def __init__(self, main=None, cond=None, raises=None, on_call=None,
                 cond_raises=None):
        self.main = main or {}
        self.cond = cond or {}
        self.raises = raises or {}
        # Raised ONLY from search_condition -- distinct from `raises`, which
        # both search_composition_logic and search_condition honour. The
        # protection step searches `search_condition` with the SAME text the
        # main-text search already used (the seed), so a test that wants the
        # PROTECTION search to fail, without also failing the main-text
        # search that ran moments before with that identical string, needs a
        # knob that fires only on the search_condition call.
        self.cond_raises = cond_raises or {}
        self.on_call = on_call
        self.calls = []

    def search_composition_logic(self, text, **kw):
        self.calls.append(('main', text, kw))
        if self.on_call:
            self.on_call(len(self.calls))
        if text in self.raises:
            raise self.raises[text]
        return self.main.get(text, {'main': [], 'filtered': []})

    def search_condition(self, text, pages=(), sys_ids=(), should_stop=None,
                         render_limit=0):
        self.calls.append(('cond', text, {'pages': pages, 'sys_ids': sys_ids,
                                          'should_stop': should_stop,
                                          'render_limit': render_limit}))
        if self.on_call:
            self.on_call(len(self.calls))
        if text in self.cond_raises:
            raise self.cond_raises[text]
        if text in self.raises:
            raise self.raises[text]
        answer = self.cond.get(text, ())
        if isinstance(answer, ConditionSearch):
            return answer
        rows = tuple({'raw_header': r, 'score': 50.0, 'text': f'ev {r}',
                      'highlight_pattern': 'p'} for r in answer)
        # A big, obviously-"own text" window for every plain-tuple answer
        # (none of this file's protection stubs overlap it) -- the own-text
        # rule needs ConditionSearch.hit_windows, not just hit_record_ids, so
        # a stub that answered without one would now remove nothing at all.
        windows = {r: ((0, 100),) for r in answer}
        return ConditionSearch(tuple(answer), (), {}, False, rows, windows)


mixin = gui_threads.PausableSearchMixin
CT = gui_threads.CompoundCompositionThread


def _worker(main_list, conditions, searcher, pinned=None):
    class _W:
        run = CT.run
        EVIDENCE_RENDER_LIMIT = CT.EVIDENCE_RENDER_LIMIT
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
        ('cond', MAIN),          # protection: the seed, over c1's (must_not) scope
        ('cond', 'hg one'), ('cond', 'hg two'), ('cond', 'rashi')]
    # Protection and c1 share one scope (same manuscript, must_not only).
    assert set(s.calls[2][2]['sys_ids']) == {S1, S2}
    assert set(s.calls[3][2]['sys_ids']) == {S1, S2}
    assert set(s.calls[5][2]['pages']) == {(S1, 'IE1', 4), (S1, 'IE1', 6),
                                           (S2, 'IE1', 0), (S2, 'IE1', 2)}
    # Main-text searches get a checkpoint; conditions AND protection get should_stop.
    assert callable(s.calls[0][2]['checkpoint'])
    assert callable(s.calls[2][2]['should_stop'])
    assert callable(s.calls[3][2]['should_stop'])


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
    # The protection search (shared scope, both conditions are must_not) runs
    # first; then 'first' stops and 'second' is never reached.
    assert [t for k, t, _ in s.calls if k == 'cond'] == [MAIN, 'first']
    [row] = out['compound']['unresolved']
    assert row['compound']['reasons'] == [cq.CUT_STOPPED, cq.CUT_NOT_RUN]
    assert out['partial'] is True and out['main'] == []


def test_stop_between_steps_runs_nothing_more():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('a', 'b'))
    s = _Searcher(main={MAIN: _rows(P1)})
    w = _worker([(SEED, '', MAIN)], [c], s)
    # Step 1 is the main search, step 2 the protection search (the must_not
    # condition's own scope), step 3 version 'a' -- stop lands right after
    # that, so 'b' is the one left not run.
    s.on_call = lambda n: setattr(w, 'cancel_flag', n >= 3)
    w.run()
    out = _result(w)
    assert [t for k, t, _ in s.calls] == [MAIN, MAIN, 'a']
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
            monkeypatch.setattr(pl._SLOTS[pl.SLOT_GENIZAH], 'generation', pinned + 1)
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
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda *_a: index_box[0])
    monkeypatch.setattr(pl, '_release_lease', lambda *_a: None)


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
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda *_a: object())
    monkeypatch.setattr(pl, '_release_lease', lambda *_a: None)
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


def test_evidence_rows_travel_tagged_and_only_when_referenced():
    c = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('rashi',), 'Rashi')
    other = rid(S3, 9, 9)                         # a hit no judged page uses
    s = _Searcher(main={MAIN: _rows(P1)},
                  cond={'rashi': (rid(S1, 1, 6), other)})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert s.calls[-1][2]['render_limit'] == CT.EVIDENCE_RENDER_LIMIT
    ev = out['compound']['evidence']
    assert set(ev) == {rid(S1, 1, 6)}
    assert ev[rid(S1, 1, 6)]['compound_evidence'] == [
        {'condition': 0, 'label': 'Rashi', 'requirement': cq.REQ_MUST,
         'where': cq.WHERE_ADJACENT}]


# -- own text (owner, 2026-10-14): must_not removes only a quoting work's own
# letters, never a page where it only matched because it quotes the main text.

def test_the_protection_step_gets_its_own_status_line():
    """`kind='protection'` carries the scope (e.g. 'same_image') as `label`,
    not a condition name -- it must not be shown as one."""
    from genizah_core import tr
    w = _Win([GOOD])
    w.lbl_comp_status = _Label()
    APP._on_compound_step(w, 2, 5, 'protection', cq.WHERE_SAME_IMAGE)
    assert w.lbl_comp_status.text == tr("Checking your own text ({}/{})").format(2, 5)


def test_must_not_covered_by_the_main_text_keeps_the_page():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((50, 90),)})
    # The main text (protection, searched as MAIN) covers the quoting text's
    # window on P1 entirely (widened by EDGE_ALLOWANCE besides) -- the Rif
    # quoting the Talmud, nothing of the Rif's OWN.
    protection = ConditionSearch((), (), {}, False, (), {P1: ((40, 100),)})
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert [r['raw_header'] for r in out['main']] == [P1]
    assert out['compound']['removed'] == []


def test_must_not_with_enough_own_letters_removes_the_page():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((0, 200),)})
    # Protection covers only the middle; widened by EDGE_ALLOWANCE=12 that is
    # (68, 132) -- 68 UNCOVERED letters remain on each side, well past
    # OWN_TEXT_MIN=40.
    protection = ConditionSearch((), (), {}, False, (), {P1: ((80, 120),)})
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert out['main'] == []
    removed = out['compound']['removed']
    assert [r['raw_header'] for r in removed] == [P1]


def test_a_cut_protection_search_removes_nothing():
    """The protection search completed -- it was never Stopped -- but its own
    verify cap fired, so it cannot prove full coverage of the main text.
    Nothing is removed; the page is "not fully checked" instead."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((0, 200),)})
    protection = ConditionSearch((), (), {'verify_truncated': True}, False, (), {})
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert out['main'] == []
    assert out['compound']['removed'] == []
    [row] = out['compound']['unresolved']
    assert row['raw_header'] == P1
    assert row['compound']['reasons'] == [cq.CUT_VERIFY]


def test_a_failed_protection_search_removes_nothing():
    """The protection search itself RAISES -- distinct from a condition
    VERSION raising (test_a_failed_condition_version_is_unresolved_and_the_
    run_goes_on, above). Nothing can be called "own text" without it, so the
    must_not version removes nothing and the page is "not fully checked"."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((0, 200),)})
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting},
                 cond_raises={MAIN: RuntimeError('protection engine error')})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert out['main'] == []
    assert out['compound']['removed'] == []
    [row] = out['compound']['unresolved']
    assert row['raw_header'] == P1
    assert row['compound']['reasons'] == [cq.CUT_FAILED]
    [pstep] = [st for st in out['compound']['steps'] if st['kind'] == 'protection']
    assert pstep['status'] == 'failed'


def test_a_stop_just_before_protection_removes_nothing():
    """Cancelled right after the main-text search -- before the protection
    step ever runs. Nothing is removed, and the whole run is partial."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((0, 200),)})
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting})
    w = _worker([(SEED, '', MAIN)], [c], s)
    s.on_call = lambda n: setattr(w, 'cancel_flag', n >= 1)
    w.run()
    out = _result(w)
    assert [t for k, t, _ in s.calls if k == 'cond'] == []   # protection never ran
    assert out['compound']['removed'] == []
    assert out['partial'] is True
    [row] = out['compound']['unresolved']
    assert row['compound']['reasons'] == [cq.CUT_NOT_RUN]


def test_a_stopped_protection_marks_the_run_partial():
    """The protection search RAN -- it was never cut off before it started,
    and it did not raise -- but a Stop landed inside it. Nothing is removed,
    the run is partial, and -- because a stopped step marks the WHOLE run
    partial immediately -- the condition version is never even attempted
    (CUT_NOT_RUN, not CUT_STOPPED): no step runs after a stopped one."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((0, 200),)})
    protection = ConditionSearch((), (), {}, True, (), {})       # stopped=True
    s = _Searcher(main={MAIN: _rows(P1)}, cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert [t for k, t, _ in s.calls if k == 'cond'] == [MAIN]   # 'hg' never ran
    assert out['main'] == []
    assert out['compound']['removed'] == []
    [row] = out['compound']['unresolved']
    assert row['raw_header'] == P1
    assert row['compound']['reasons'] == [cq.CUT_NOT_RUN]
    assert out['partial'] is True


def test_protection_is_searched_over_each_conditions_own_scope():
    """Two must_not conditions, two DIFFERENT places -- protection is
    searched once per scope, with exactly that scope's pages/sys_ids, never
    the other condition's."""
    c1 = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    c2 = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('rashi',))
    s = _Searcher(main={MAIN: _rows(P1, P2)})
    w = _worker([(SEED, '', MAIN)], [c1, c2], s)
    w.run()
    _result(w)
    protection_calls = [c for c in s.calls if c[0] == 'cond' and c[1] == MAIN]
    assert len(protection_calls) == 2
    same_image = next(c for c in protection_calls if c[2]['pages'])
    manuscript = next(c for c in protection_calls if c[2]['sys_ids'])
    assert set(same_image[2]['pages']) == {(S1, 'IE1', 5), (S2, 'IE1', 1)}
    assert not same_image[2]['sys_ids']
    assert set(manuscript[2]['sys_ids']) == {S1, S2}
    assert not manuscript[2]['pages']


def test_main_row_windows_protect_even_when_protection_finds_nothing_there():
    """The main search's OWN matched-core windows on a page (`page_windows`,
    carried on its result rows) protect that page even when a SECOND search
    of the main text (the protection step) re-finds LESS there than the main
    search itself already established -- the experiment's remaining
    weakness (docs/plans/COMPOSITION_CONDITIONS_PLAN.md section 12): without
    this union the page would be wrongly removed below."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    main_result = {'main': [{'raw_header': P1, 'score': 100.0, 'uid': P1,
                             'page_windows': ((40, 100),)}], 'filtered': []}
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((50, 90),)})
    protection = ConditionSearch((), (), {}, False, (), {})   # found NOTHING on P1
    s = _Searcher(main={MAIN: main_result}, cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c], s)
    w.run()
    out = _result(w)
    assert [r['raw_header'] for r in out['main']] == [P1]
    assert out['compound']['removed'] == []


def test_every_witness_windows_protect_not_only_the_fused_rows():
    """A fused row keeps only its top-ranked witness's fields. Here a second
    witness ranks P1 higher but matched less of it; the main text's own wider
    match must still protect the page."""
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('hg',))
    seed_rows = {'main': [{'raw_header': P1, 'score': 100.0, 'uid': P1,
                           'page_windows': ((40, 100),)}], 'filtered': []}
    other_rows = {'main': [{'raw_header': P1, 'score': 200.0, 'uid': P1,
                            'page_windows': ((80, 100),)}], 'filtered': []}
    # Against the second witness's window alone, 20..68 is 48 own letters
    # (removed); against the main text's own window, only 8 (kept).
    quoting = ConditionSearch((P1,), (), {}, False, (), {P1: ((20, 90),)})
    protection = ConditionSearch((), (), {}, False, (), {})
    s = _Searcher(main={MAIN: seed_rows, 'other witness': other_rows},
                  cond={'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN), ('w2', 'W2', 'other witness')], [c], s)
    w.run()
    out = _result(w)
    assert [r['raw_header'] for r in out['main']] == [P1]
    assert out['main'][0]['page_windows'] == ((80, 100),)     # the fused row is W2's
    assert out['compound']['removed'] == []


def test_kept_pages_evidence_not_polluted_by_a_non_decisive_must_not_match():
    """A page that is genuinely "evidence" for one condition must not ALSO
    carry a DIFFERENT must_not condition's tag when that condition's match
    there was not decisive (own text) -- it was correctly judged satisfied,
    not removed, and must not look like evidence for removing anything."""
    c1 = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('rashi',), 'Rashi')
    c2 = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('hg',), 'HG')
    e = rid(S1, 1, 6)                     # adjacent to P1 (p=5): P+1=6
    quoting = ConditionSearch(
        (e,), (), {}, False,
        ({'raw_header': e, 'score': 50.0, 'text': 'ev', 'highlight_pattern': 'p'},),
        {e: ((50, 90),)})
    protection = ConditionSearch((), (), {}, False, (), {e: ((40, 100),)})
    s = _Searcher(main={MAIN: _rows(P1)},
                 cond={'rashi': (e,), 'hg': quoting, MAIN: protection})
    w = _worker([(SEED, '', MAIN)], [c1, c2], s)
    w.run()
    out = _result(w)
    assert [r['raw_header'] for r in out['main']] == [P1]
    assert out['compound']['removed'] == []
    ev = out['compound']['evidence']
    assert set(ev) == {e}
    assert ev[e]['compound_evidence'] == [
        {'condition': 0, 'label': 'Rashi', 'requirement': cq.REQ_MUST,
         'where': cq.WHERE_ADJACENT}]


# -- step 4: evidence pages, reasons, flat markers, the score mark -------------------

class _ResultsWin:
    _attach_compound_evidence = APP._attach_compound_evidence
    _compound_evidence_labels = APP._compound_evidence_labels
    _compound_item_reason = APP._compound_item_reason
    _condition_label = APP._condition_label
    _comp_flat_marker_map = APP._comp_flat_marker_map
    _comp_flat_root_label = APP._comp_flat_root_label
    _format_score_with_boundary = APP._format_score_with_boundary
    _compound_summary_text = APP._compound_summary_text

    def __init__(self, conditions):
        self._comp_compound = {'conditions': [c.to_dict() for c in conditions]}

    def _get_filter_reason(self, item):
        return ''


def _judged(header, states, evidence, reasons=None):
    n = len(states)
    oc = cq.PageOutcome(cq._category(states), tuple(states),
                        tuple(tuple(e) for e in evidence),
                        tuple(reasons or [''] * n))
    return {'raw_header': header, 'score': 90.0, 'compound': oc.to_dict()}


def test_evidence_pages_join_their_manuscript_scoreless_and_once():
    c = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('r',), 'Rashi')
    w = _ResultsWin([c])
    ev_rid = rid(S1, 1, 6)
    evidence = {ev_rid: {'raw_header': ev_rid, 'score': 77.0, 'text': 't',
                         'compound_evidence': [{'condition': 0}]}}
    rows = [_judged(P1, [cq.SATISFIED], [[ev_rid]])]
    out = w._attach_compound_evidence(rows, evidence)
    assert [r['raw_header'] for r in out] == [P1, ev_rid]
    added = out[1]
    assert added['is_condition_evidence'] and added['score'] == 0.0
    assert added['evidence_score'] == 77.0
    assert w._format_score_with_boundary(added) == '◆ Rashi'
    assert w._format_score_with_boundary(out[0]) == '90'
    # An evidence page that is itself a main-text page is tagged, not added.
    rows = [_judged(P1, [cq.SATISFIED], [[P1]])]
    out = w._attach_compound_evidence(rows, {P1: dict(evidence[ev_rid], raw_header=P1)})
    assert len(out) == 1 and out[0]['compound_evidence'] == [{'condition': 0}]


def test_reasons_name_the_condition_and_what_cut_it():
    from genizah_core import tr
    c1 = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('h',), 'HG')
    c2 = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('r',))
    w = _ResultsWin([c1, c2])
    ms = {'pages': [_judged(P1, [cq.REMOVED, cq.UNRESOLVED], [[P1], []],
                            ['', cq.CUT_VERIFY])]}
    assert w._compound_item_reason(ms, 'removed') == 'HG'
    assert w._compound_item_reason(ms, 'unresolved') == \
        f"{tr('Condition {}').format(2)} ({tr('search limit reached')})"


def test_flat_view_marks_and_counts_the_two_new_groups():
    from genizah_core import tr
    w = _ResultsWin([Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('h',))])
    removed = [{'pages': [_judged(P1, [cq.REMOVED], [[P1]])]}]
    unresolved = [{'pages': [_judged(P2, [cq.UNRESOLVED], [[]], [cq.CUT_STOPPED])]}]
    markers = w._comp_flat_marker_map([], [], removed=removed, unresolved=unresolved)
    kinds = sorted(k for _i, k, _r in markers.values())
    assert kinds == ['removed', 'unresolved']
    label = w._comp_flat_root_label(2, markers)
    assert tr("{} removed by a condition").format(1) in label
    assert tr("{} not fully checked").format(1) in label


def test_summary_says_what_each_condition_did():
    from genizah_core import tr
    w = _ResultsWin([Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('h',), 'HG')])
    w._comp_compound.update({'failed_counts': [3], 'removed': [1, 2],
                             'unresolved': []})
    text = w._compound_summary_text()
    assert tr("{} pages without {}").format(3, 'HG') in text
    assert tr("{} pages removed").format(2) in text
