# -*- coding: utf-8 -*-
"""Gate 7 of docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md (§3.4): Stop between native calls.

An engine-generated pattern matches with stdlib `re` on the desktop, and a `re`
call cannot be interrupted. So the three desktop search threads put their own
cancel predicate (PausableSearchMixin._should_abort) in shared.search_regex's
ContextVar around the engine call, and a generated pattern's NEXT call raises
InterruptedError once Stop is pressed. That error must arrive through the same
paths a progress checkpoint's InterruptedError takes:

- SearchThread: the cut-off says interrupted (and, when it escapes the engine,
  no rows -- the thread's own `except InterruptedError`);
- ChainCompletionThread: {'restrict': None, 'interrupted': True}, the chain as it was;
- CompositionThread (the real engine loop, Genizah and LOCAL): partial=True, and
  no row for a page cancelled at its second search.

The predicate is the thread's, it is set only while the engine runs, and the
ContextVar is back to None after run() whatever happened.

Routing is never tested with a catastrophic subject here: Stop is pressed from a
stand-in around `Pattern._stdlib`, so a wrong route only changes a count.

Mutation that reddens every thread test below: remove a `with cancel_check(
self._should_abort):` wrapper from desktop/gui_threads.py (the generated pattern
then never sees Stop, and the run completes).

Thread objects are built without a QApplication and run() is called on this
thread, so signals are delivered directly (as tests/test_search_ids_only.py does).
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from unittest.mock import MagicMock  # noqa: E402

import pytest  # noqa: E402

import shared.search_engine as se  # noqa: E402
import shared.search_regex as sr  # noqa: E402
from desktop.gui_threads import ChainCompletionThread, CompositionThread, SearchThread  # noqa: E402
from shared.refinement import RefinementStep  # noqa: E402

PHRASE = r'שלום\W+עולם'
# Brackets make the engine search a page twice (stripped, then the original).
BRACKETED = 'פתיחה שלום [עולם] סוף'
PLAIN = 'פתיחה שלום עולם סוף'


@pytest.fixture(autouse=True)
def _clean_signals():
    """No cut-off signal or cancel predicate from elsewhere on this thread."""
    se.consume_last_search_cutoff()
    assert sr._cancel_check.get() is None
    yield
    se.consume_last_search_cutoff()


class _StopAfter:
    """Stands in for a generated pattern's `_stdlib`: matches with the real
    stdlib object, counts the native calls and presses Stop on *thread* right
    after call number *after* -- between two native calls."""

    def __init__(self, real, after):
        self.real, self.after, self.calls, self.thread = real, after, 0, None

    def search(self, *a, **kw):
        result = self.real.search(*a, **kw)
        self.calls += 1
        if self.calls == self.after:
            self.thread.request_cancel()      # the one Stop entry point
        return result

    def __getattr__(self, name):              # groups, groupindex, flags, pattern
        return getattr(self.real, name)


def _generated(after):
    """A real engine-generated pattern on the native route, with Stop pressed
    after native call *after*."""
    pattern = sr.compile_generated(PHRASE, 0, cost_per_start_ns=100)
    assert pattern._generated, "a non-generated pattern never checks the predicate"
    pattern._stdlib = _StopAfter(pattern._stdlib, after)
    return pattern


def _signals(thread, *names):
    got = {name: [] for name in names}
    for name in names:
        getattr(thread, name).connect(lambda *a, n=name: got[n].append(a[0] if len(a) == 1 else a))
    return got


# --- SearchThread -------------------------------------------------------------------

class _VerifyingEngine:
    """execute_search reduced to its verifier: the generated pattern over each
    page, as the per-hit handler runs it. *catches*: the engine turns the Stop into
    partial results and an interrupted cut-off, as execute_search's outer handlers
    do; otherwise the InterruptedError leaves the engine."""

    def __init__(self, pattern, pages=4, catches=False):
        self.pattern, self.pages, self.catches = pattern, pages, catches
        self.rows, self.predicates = [], []

    def _verify(self):
        self.predicates.append(sr._cancel_check.get())
        for page in range(self.pages):
            if self.pattern.search(PLAIN):
                self.rows.append({'uid': f'99_{page}', 'display': {'id': '99'}})
        return list(self.rows)

    def execute_search(self, *a, **kw):
        if not self.catches:
            return self._verify()
        try:
            return self._verify()
        except InterruptedError:
            se._note_search_cutoff(interrupted=True)
            return list(self.rows)


def _search_thread(engine):
    t = SearchThread(engine, 'שלום עולם', 'variants', 0, corpus_scope='genizah')
    engine.pattern._stdlib.thread = t
    return t, _signals(t, 'cutoff_signal', 'results_signal', 'perf_signal', 'error_signal')


def test_a_stop_between_native_calls_ends_a_search_as_a_checkpoint_does():
    engine = _VerifyingEngine(_generated(after=1))
    t, got = _search_thread(engine)
    t.run()
    assert engine.pattern._stdlib.calls == 1, "the call after the Stop ran"
    assert got['cutoff_signal'] == [{'capped': False, 'interrupted': True}]
    assert got['results_signal'] == [[]]
    assert got['perf_signal'] == [] and got['error_signal'] == []
    assert engine.predicates == [t._should_abort], "the predicate is not the thread's own"
    assert sr._cancel_check.get() is None, "the predicate outlived the run"


def test_a_stop_the_engine_catches_keeps_the_rows_found_so_far():
    engine = _VerifyingEngine(_generated(after=2), catches=True)
    t, got = _search_thread(engine)
    t.run()
    assert engine.pattern._stdlib.calls == 2
    assert got['cutoff_signal'] == [{'capped': False, 'interrupted': True}]
    assert got['results_signal'] == [engine.rows] and len(engine.rows) == 2
    assert got['perf_signal'] == [], "a stopped run is not a completed run"
    assert sr._cancel_check.get() is None


def test_the_predicate_is_reset_when_the_engine_fails():
    class _Failing:
        def execute_search(self, *a, **kw):
            assert sr._cancel_check.get() is not None
            raise RuntimeError('index gone')

    t = SearchThread(_Failing(), 'שלום', 'literal', 0)
    got = _signals(t, 'error_signal', 'results_signal')
    t.run()
    assert got == {'error_signal': ['index gone'], 'results_signal': []}
    assert sr._cancel_check.get() is None


def test_without_a_stop_the_search_runs_every_native_call():
    """The control: the stand-in counts what a run that is not stopped does."""
    engine = _VerifyingEngine(_generated(after=99))
    t, got = _search_thread(engine)
    t.run()
    assert engine.pattern._stdlib.calls == 4
    assert got['cutoff_signal'] == [{'capped': False, 'interrupted': False}]
    assert len(got['results_signal'][0]) == 4 and len(got['perf_signal']) == 1


# --- ChainCompletionThread ------------------------------------------------------------

def _cut_off_chain():
    first = RefinementStep('שלום עולם', 'variants', corpus_scope='genizah', result_count=3,
                           result_count_capped=True)
    first._result_sys_ids = {'990001'}
    return [first]


@pytest.mark.parametrize('catches', [False, True], ids=['escapes the engine', 'engine catches'])
def test_a_stop_between_native_calls_ends_a_chain_completion(catches):
    engine = _VerifyingEngine(_generated(after=1), catches=catches)
    chain = _cut_off_chain()
    t = ChainCompletionThread(chain, engine, None)
    engine.pattern._stdlib.thread = t
    got = _signals(t, 'finished_signal', 'error_signal')
    t.run()
    assert engine.pattern._stdlib.calls == 1
    assert got == {'finished_signal': [{'restrict': None, 'interrupted': True}], 'error_signal': []}
    assert (chain[0]._result_sys_ids, chain[0].result_count, chain[0].result_count_capped) == (
        {'990001'}, 3, True), "a stopped completion changed the step"
    assert engine.predicates == [t._should_abort]
    assert sr._cancel_check.get() is None


# --- CompositionThread, through the real engine loop ------------------------------------

def _composition_engine(pattern, scope, contents):
    """A SearchEngine with __init__ bypassed (as tests/test_comp_corpus_scope.py
    builds it): one chunk, whose query finds *contents*, verified with *pattern*."""
    engine = se.SearchEngine.__new__(se.SearchEngine)
    hits = MagicMock()
    hits.hits = [(1.0, n) for n in range(len(contents))]
    docs = {n: {'unique_id': [f'99000{n}_1'], 'content': [c], 'full_header': [f'99000{n}_P1'],
                'source': ['V0.8'], 'shelfmark': [f'T-S {n}']}
            for n, c in enumerate(contents)}
    index, searcher = MagicMock(), MagicMock()
    searcher.search.return_value = hits
    searcher.doc.side_effect = docs.__getitem__
    if scope == 'local':
        engine.local_index, engine.local_searcher = index, searcher
        engine.index = engine.searcher = None
    else:
        engine.index, engine.searcher = index, searcher
        engine.local_index = engine.local_searcher = None
    engine._my_library_tab_ref = None
    engine.build_tantivy_query = MagicMock(return_value='content:x')
    engine.build_regex_pattern = MagicMock(return_value=pattern)
    engine._load_browse_map = MagicMock(return_value={})
    return engine


def _run_composition(scope, after, contents):
    pattern = _generated(after)
    engine = _composition_engine(pattern, scope, contents)
    t = CompositionThread(engine, 'שלום עולם', 2, 50, 'variants', corpus_scope=scope)
    pattern._stdlib.thread = t
    got = _signals(t, 'scan_finished_signal', 'error_signal', 'perf_signal')
    t.run()
    assert got['error_signal'] == [], got['error_signal']
    (result,) = got['scan_finished_signal']
    return pattern._stdlib.calls, result, got


@pytest.mark.parametrize('scope', ['genizah', 'local'])
def test_a_composition_stopped_at_a_pages_second_search_is_partial_without_its_row(scope):
    # Page 0 is verified (two native calls); Stop lands after page 1's first call,
    # so its second search -- the original, bracketed content -- raises.
    calls, result, got = _run_composition(scope, after=3, contents=[BRACKETED, BRACKETED])
    assert calls == 3, "the call after the Stop ran"
    assert result['partial'] is True and result['cancelled'] is True
    assert [r['uid'] for r in result['main'] + result['filtered']] == ['990000_1'], (
        "a page cancelled between its two searches left a row")
    assert got['perf_signal'] == []
    assert sr._cancel_check.get() is None


@pytest.mark.parametrize('scope', ['genizah', 'local'])
def test_a_composition_that_is_not_stopped_verifies_every_page(scope):
    """The control for the test above: the same pages, no Stop."""
    calls, result, got = _run_composition(scope, after=99, contents=[BRACKETED, BRACKETED])
    assert calls == 4
    assert result['partial'] is False
    assert sorted(r['uid'] for r in result['main']) == ['990000_1', '990001_1']
    assert len(got['perf_signal']) == 1
