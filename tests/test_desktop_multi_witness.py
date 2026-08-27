# -*- coding: utf-8 -*-
"""Desktop multi-witness: the render cap, and the index-generation pin.

Both are about a BATCH being one answer assembled from N separate engine
calls. Each call is independently correct; what these guard is the two ways
the assembly can be wrong without any single call failing.

Qt-free: nothing here constructs a QApplication, so it runs in the default
(non-gui) lane. The engine is stubbed -- these test the desktop's plumbing,
not the matcher, and a real index costs minutes.
"""
import pytest

from desktop import passage_lifecycle as pl


class _Sentinel:
    """Stands in for a live PassageIndex. Never dereferenced."""


class _RecordingSearcher:
    """Captures the kwargs `PassageSearchAdapter` builds it with."""

    seen = []

    def __init__(self, **kwargs):
        _RecordingSearcher.seen.append(kwargs)

    def search_composition_logic(self, *a, **kw):
        return {'main': [], 'filtered': []}


@pytest.fixture
def leased(monkeypatch):
    """A lease that always succeeds, and a stubbed engine.

    `search_composition_logic` refuses before it takes a lease when no index
    is installed, so a test of what it does AFTER the lease has to grant one.
    """
    import shared.passage_parallels as pp

    _RecordingSearcher.seen = []
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda: _Sentinel())
    monkeypatch.setattr(pl, '_release_lease', lambda: None)
    monkeypatch.setattr(pp, 'PassageSearcher', _RecordingSearcher)
    monkeypatch.setattr(pl, 'compose', lambda *a, **k: 'POLICY')
    return _RecordingSearcher


class _NullFetcher:
    def get_full_text_by_header(self, header):
        return None


# ---------------------------------------------------------------------------
# The render cap. Three-valued, and the distinction decides what a
# multi-witness search is able to find.
# ---------------------------------------------------------------------------

def test_the_single_witness_path_says_nothing_about_the_cap(leased):
    """v9.0.0 behaviour, byte for byte: no `render_cap` kwarg at all, so
    PassageSearcher applies its own PARALLELS_GROUP_CAP default. Passing
    `render_cap=None` explicitly would override that default WITH None and
    break the `> 0` comparison inside the searcher."""
    pl.get_passage_searcher(_NullFetcher()).search_composition_logic('text')
    assert 'render_cap' not in leased.seen[0]


def test_the_multi_witness_path_asks_for_no_cap_at_all(leased):
    """Each witness must come back WHOLE, because the cap has to be applied
    once to the FUSED list. Capping each witness first fuses N already-
    truncated lists and silently drops every contributor past rank 200 in its
    own witness -- which is exactly where a rare witness of a widely-copied
    work sits."""
    pl.get_passage_searcher(
        _NullFetcher(), render_cap=0).search_composition_logic('text')
    assert leased.seen[0]['render_cap'] == 0


def test_zero_is_a_real_value_and_not_a_falsy_stand_in_for_unset(leased):
    """The caps are applied as `if self.render_cap and self.render_cap > 0`,
    so 0 means NO CAP while None means NO OPINION. Any truthiness test on
    this parameter collapses the two, and the collapse is silent: the
    multi-witness path would quietly get the 200-group cap back."""
    pl.get_passage_searcher(_NullFetcher(), render_cap=0).search_composition_logic('t')
    pl.get_passage_searcher(_NullFetcher()).search_composition_logic('t')
    with_zero, with_none = leased.seen
    assert with_zero.get('render_cap', 'ABSENT') == 0
    assert 'render_cap' not in with_none


def test_an_explicit_cap_is_passed_through(leased):
    pl.get_passage_searcher(
        _NullFetcher(), render_cap=50).search_composition_logic('text')
    assert leased.seen[0]['render_cap'] == 50


def test_the_cap_does_not_disturb_the_other_axes(leased):
    """The three policy axes are still validated independently and still
    reach `compose`; adding a fourth parameter must not shift them."""
    pl.get_passage_searcher(
        _NullFetcher(), 'max-40', 'short', 'deep',
        render_cap=0).search_composition_logic('text')
    assert leased.seen[0]['policy'] == 'POLICY'
    assert leased.seen[0]['render_cap'] == 0


def test_the_adapter_still_exposes_no_index_with_a_cap_set():
    """`__slots__` rules out a stray attribute holding a live index. Adding
    `_render_cap` must not have opened an instance dict."""
    adapter = pl.get_passage_searcher(_NullFetcher(), render_cap=0)
    with pytest.raises(AttributeError):
        adapter.__dict__


# ---------------------------------------------------------------------------
# The generation pin. A batch that straddles an index swap fuses ranks that
# were never comparable.
# ---------------------------------------------------------------------------

def test_an_unchanged_generation_is_not_a_change():
    assert pl.generation_changed(pl.current_state_generation()) is False


def test_a_bumped_generation_is_a_change(monkeypatch):
    pinned = pl.current_state_generation()
    monkeypatch.setattr(pl, '_state_generation', pinned + 1)
    assert pl.generation_changed(pinned) is True


def test_no_pin_is_not_a_change(monkeypatch):
    """A batch that never captured a generation started before anything was
    installed. Its first lease fails with PassageSearchUnavailableError,
    which is the honest error for that case -- not this one."""
    monkeypatch.setattr(pl, '_state_generation', 99)
    assert pl.generation_changed(None) is False


def test_the_replacement_error_is_catchable_and_not_a_witness_failure():
    """A swap voids the whole BATCH. Reporting it as a per-witness failure
    would tell the reader a witness found nothing, when in fact none of the
    fused numbers mean anything."""
    assert issubclass(pl.PassageIndexReplaced, RuntimeError)
    assert not issubclass(pl.PassageIndexReplaced, pl.PassageSearchError)


# ---------------------------------------------------------------------------
# The worker. Qt-free: `run()` is borrowed onto a plain stub that supplies
# recording signals, the same trick tests/test_desktop_passage_gate.py uses
# for the window's methods. Borrowing the REAL method matters -- a
# re-implementation would only prove the test agrees with itself.
# ---------------------------------------------------------------------------

class _Sig:
    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args if len(args) != 1 else args[0])


class _StubSearcher:
    """Records the texts it was asked to search and returns canned rows."""

    def __init__(self, by_text=None, raises=None):
        self.by_text = by_text or {}
        self.raises = raises or {}
        self.searched = []
        self.kwargs = []

    def search_composition_logic(self, text, **kw):
        self.searched.append(text)
        self.kwargs.append(kw)
        if text in self.raises:
            raise self.raises[text]
        return self.by_text.get(text, {'main': [], 'filtered': []})


def _worker(witnesses, searcher, pinned=None, abort_after=None, cap=None,
            roster=None, prior_rows=None, prior_filtered=None):
    """A stub carrying the real `run` and the real cancel plumbing."""
    from gui_threads import MultiWitnessCompositionThread as MW

    class _W:
        # The REAL methods, borrowed. A re-implementation would only prove
        # the test agrees with itself -- and the pause gate in particular is
        # the machinery this thread's whole cancel story rests on, so it is
        # the real `PauseGate`, not a stand-in that cannot park.
        run = MW.run
        _checkpoint = pl_mixin._checkpoint
        _should_abort = pl_mixin._should_abort
        _emit_pause_ack = pl_mixin._emit_pause_ack
        _init_pause_support = pl_mixin._init_pause_support

    w = _W()
    w.searcher = searcher
    w.witnesses = list(witnesses)
    w.filter_text = None
    w.threshold = 5
    w.restrict_sys_ids = None
    w.corpus_scope = 'genizah'
    w.pinned_generation = pinned
    w.group_cap = cap
    w.roster = (list(roster) if roster is not None
                else [(wid, label) for wid, label, _t in w.witnesses])
    w.prior_rows = dict(prior_rows or {})
    w.prior_filtered = dict(prior_filtered or {})
    w.progress_signal = _Sig()
    w.status_signal = _Sig()
    w.scan_finished_signal = _Sig()
    w.error_signal = _Sig()
    w.perf_signal = _Sig()
    w.witness_progress_signal = _Sig()
    w.pause_ack_signal = _Sig()
    w._init_pause_support(0)
    if abort_after is not None:
        # Cancel once N witnesses have been dispatched, the way a user
        # pressing Stop mid-batch does.
        original = searcher.search_composition_logic

        def _counting(text, **kw):
            out = original(text, **kw)
            if len(searcher.searched) >= abort_after:
                w.cancel_flag = True
            return out
        searcher.search_composition_logic = _counting
    return w


def _rows(*pairs):
    return {'main': [{'raw_header': h, 'score': s} for h, s in pairs],
            'filtered': []}


import gui_threads  # noqa: E402
pl_mixin = gui_threads.PausableSearchMixin


def test_every_witness_is_searched_separately_never_concatenated():
    """Joining witnesses into one query starves the engine's per-query
    posting budget: the 17 Birkat Hamazon witnesses joined into one 33,180
    character query admit 2.4% of their own postings and reach 48.2%, WORSE
    than the best single witness's 56.7%, against 74.1% fused."""
    s = _StubSearcher()
    w = _worker([('w1', 'A', 'alpha text'), ('w2', 'B', 'beta text')], s)
    w.run()
    assert s.searched == ['alpha text', 'beta text']
    for text in s.searched:
        assert 'alpha text\nbeta text' not in text


def test_results_are_fused_and_contributors_counted():
    s = _StubSearcher({
        'alpha text': _rows(('990000000001_1r', 100), ('990000000002_1r', 50)),
        'beta text': _rows(('990000000001_1r', 80), ('990000000003_1r', 40)),
    })
    w = _worker([('w1', 'A', 'alpha text'), ('w2', 'B', 'beta text')], s)
    w.run()
    payload = w.scan_finished_signal.calls[0]
    by_header = {r['raw_header']: r for r in payload['main']}
    assert by_header['990000000001_1r']['witness_count'] == 2
    assert by_header['990000000002_1r']['witness_count'] == 1
    assert payload['witnesses_searched'] == 2


def test_a_cancel_between_witnesses_stops_the_batch_and_keeps_what_it_found():
    """The boundary is the ONLY place a cancel can be honoured -- the passage
    engine emits no progress and cannot be interrupted mid-search -- so the
    in-flight witness always completes and its rows are kept."""
    s = _StubSearcher({
        'one': _rows(('990000000001_1r', 100)),
        'two': _rows(('990000000002_1r', 90)),
        'three': _rows(('990000000003_1r', 80)),
    })
    w = _worker([('w1', 'A', 'one'), ('w2', 'B', 'two'), ('w3', 'C', 'three')],
                s, abort_after=2)
    w.run()
    payload = w.scan_finished_signal.calls[0]
    assert s.searched == ['one', 'two'], 'the third witness still ran'
    assert payload['partial'] is True
    assert payload['witnesses_searched'] == 2
    assert payload['main'], 'a cancelled batch threw away what it had found'


def test_a_cancelled_run_emits_no_perf_sample():
    """Phase 115 D-08: telemetry describes COMPLETED runs. A partial batch
    would report a fast search that did a fraction of the work."""
    s = _StubSearcher({'one': _rows(('990000000001_1r', 100)),
                       'two': _rows(('990000000002_1r', 90))})
    w = _worker([('w1', 'A', 'one'), ('w2', 'B', 'two')], s, abort_after=1)
    w.run()
    assert w.perf_signal.calls == []


def test_a_completed_run_emits_one_perf_sample():
    s = _StubSearcher({'one': _rows(('990000000001_1r', 100))})
    w = _worker([('w1', 'A', 'one')], s)
    w.run()
    assert len(w.perf_signal.calls) == 1


def test_one_failing_witness_does_not_fail_the_batch():
    """The user asked for seventeen searches and can still have sixteen."""
    s = _StubSearcher({'good': _rows(('990000000001_1r', 100))},
                      raises={'bad': RuntimeError('boom')})
    w = _worker([('w1', 'A', 'bad'), ('w2', 'B', 'good')], s)
    w.run()
    payload = w.scan_finished_signal.calls[0]
    assert payload['main'], 'the good witness was lost with the bad one'
    statuses = {r['witness_id']: r['status'] for r in payload['witness_report']}
    assert statuses == {'w1': 'failed', 'w2': 'searched'}
    assert w.error_signal.calls == [], 'a witness failure aborted the batch'


def test_an_empty_witness_text_is_never_dispatched():
    """The engine answers an empty query with nothing, and the panel would
    then report a perfectly honest-looking "0 matches" for a search that
    never ran -- the false negative the web shipped and had to fix."""
    s = _StubSearcher({'good': _rows(('990000000001_1r', 100))})
    w = _worker([('w1', 'A', '   '), ('w2', 'B', 'good')], s)
    w.run()
    assert s.searched == ['good']
    report = {r['witness_id']: r for r in w.scan_finished_signal.calls[0]['witness_report']}
    assert report['w1']['status'] == 'failed'
    assert report['w1']['reason'] == 'empty_text'


def test_a_missing_index_stops_the_batch_rather_than_failing_each_witness():
    """Every remaining witness would fail identically; N identical errors
    tell the reader less than one."""
    s = _StubSearcher(raises={'one': pl.PassageSearchUnavailableError('none')})
    w = _worker([('w1', 'A', 'one'), ('w2', 'B', 'two')], s)
    w.run()
    assert s.searched == ['one']
    assert w.error_signal.calls, 'the failure was swallowed'
    assert w.scan_finished_signal.calls == []


def test_an_index_swap_mid_batch_voids_the_run(monkeypatch):
    """A batch is ONE answer assembled from N calls. An install between two
    of them fuses ranks that were never comparable -- so the batch is
    abandoned, not published with a per-witness 'failed'."""
    s = _StubSearcher({'one': _rows(('990000000001_1r', 100)),
                       'two': _rows(('990000000002_1r', 90))})
    pinned = pl.current_state_generation()
    w = _worker([('w1', 'A', 'one'), ('w2', 'B', 'two')], s, pinned=pinned)
    original = s.search_composition_logic

    def _swap(text, **kw):
        out = original(text, **kw)
        monkeypatch.setattr(pl, '_state_generation', pinned + 1)
        return out
    s.search_composition_logic = _swap
    w.run()
    assert s.searched == ['one'], 'kept searching against a replaced index'
    assert w.scan_finished_signal.calls == [], 'published a mixed-index fusion'
    assert w.error_signal.calls, 'the swap was silent'


def test_no_swap_no_complaint():
    s = _StubSearcher({'one': _rows(('990000000001_1r', 100))})
    w = _worker([('w1', 'A', 'one')], s, pinned=pl.current_state_generation())
    w.run()
    assert w.error_signal.calls == []
    assert w.scan_finished_signal.calls


def test_progress_names_the_witness_being_searched():
    s = _StubSearcher()
    w = _worker([('w1', 'Rite A', 'one'), ('w2', 'Rite B', 'two')], s)
    w.run()
    assert w.witness_progress_signal.calls == [
        (1, 2, 'Rite A'), (2, 2, 'Rite B')]


def test_the_engine_is_called_with_the_passage_neutral_arguments():
    """`boundary_mode` must be 'full': PassageSearcher RAISES on anything
    else rather than silently degrading, and the caller has to respect that
    before the worker thread is reached."""
    s = _StubSearcher()
    w = _worker([('w1', 'A', 'one')], s)
    w.run()
    assert s.kwargs[0]['boundary_mode'] == 'full'
    assert s.kwargs[0]['corpus_scope'] == 'genizah'


def test_the_group_cap_is_applied_once_to_the_fused_list():
    """Not per witness. Capping each witness first fuses N already-truncated
    lists and drops every contributor past the cap in its OWN witness."""
    s = _StubSearcher({
        'one': _rows(('990000000001_1r', 10), ('990000000002_1r', 500)),
        'two': _rows(('990000000001_1r', 10), ('990000000003_1r', 400)),
    })
    w = _worker([('w1', 'A', 'one'), ('w2', 'B', 'two')], s, cap=1)
    w.run()
    payload = w.scan_finished_signal.calls[0]
    surviving = {r['raw_header'] for r in payload['main']}
    # 990000000001 has the LOWEST raw score and the HIGHEST fusion score
    # (two witnesses). Rank fusion is what the cap must respect.
    assert surviving == {'990000000001_1r'}, surviving
    assert payload['truncated_to_200'] is True


def test_the_thread_declares_every_signal_dispatch_reaches_for():
    """Dispatch in genizah_app.py accesses progress, error, status, pause_ack
    and perf unconditionally. A partial interface either raises before the
    thread starts or silently drops Pause, status and telemetry."""
    from gui_threads import CompositionThread, MultiWitnessCompositionThread
    required = {n for n in dir(CompositionThread)
                if n.endswith('_signal')}
    have = {n for n in dir(MultiWitnessCompositionThread)
            if n.endswith('_signal')}
    assert required <= have, f'missing: {sorted(required - have)}'
    assert 'witness_progress_signal' in have


def test_the_single_witness_thread_is_untouched():
    """Phase 146 kept `CompositionThread` byte-unchanged and 97 tests pin the
    fact; the multi-witness path is a SIBLING, not a mode of it."""
    import inspect
    from gui_threads import CompositionThread
    src = inspect.getsource(CompositionThread)
    assert 'witness' not in src.lower()
    assert 'fuse' not in src.lower()


# ---------------------------------------------------------------------------
# The incremental roster. This is what keeps auto-expand linear instead of
# quadratic, and what stops each round overwriting the last.
# ---------------------------------------------------------------------------

def test_a_later_round_searches_only_the_new_witness():
    """Round three must not re-run rounds one and two. The web's whole
    "cost is 1 + rounds x K" premise rests on this; re-running everything
    every round makes an R-round expansion quadratic."""
    s = _StubSearcher({'gamma': _rows(('990000000003_1r', 70))})
    w = _worker(
        [('w3', 'C', 'gamma')], s,
        roster=[('seed', 'Your text'), ('w2', 'B'), ('w3', 'C')],
        prior_rows={'seed': [{'raw_header': '990000000001_1r', 'score': 100}],
                    'w2': [{'raw_header': '990000000001_1r', 'score': 90}]})
    w.run()
    assert s.searched == ['gamma'], 'earlier rounds were re-searched'


def test_a_later_round_fuses_the_earlier_rounds_from_cache():
    """The rows of earlier rounds are every bit as much part of the answer.
    Fusing only what this run searched would make each round overwrite the
    last instead of widening it."""
    s = _StubSearcher({'gamma': _rows(('990000000001_1r', 70),
                                      ('990000000003_1r', 60))})
    w = _worker(
        [('w3', 'C', 'gamma')], s,
        roster=[('seed', 'Your text'), ('w2', 'B'), ('w3', 'C')],
        prior_rows={'seed': [{'raw_header': '990000000001_1r', 'score': 100}],
                    'w2': [{'raw_header': '990000000002_1r', 'score': 90}]})
    w.run()
    payload = w.scan_finished_signal.calls[0]
    found = {r['raw_header']: r for r in payload['main']}
    # All three manuscripts survive: two from cache, one newly searched.
    assert set(found) == {'990000000001_1r', '990000000002_1r',
                          '990000000003_1r'}
    # And the one both the seed and the new witness reached counts two.
    assert found['990000000001_1r']['witness_count'] == 2
    assert payload['witnesses_searched'] == 3


def test_the_roster_defaults_to_this_runs_own_witnesses():
    """A first search has nothing prior to fuse with, and must not need the
    caller to say so."""
    s = _StubSearcher({'one': _rows(('990000000001_1r', 100))})
    w = _worker([('w1', 'A', 'one')], s)
    w.run()
    assert w.scan_finished_signal.calls[0]['main']


def test_fusion_set_does_not_duplicate_the_seed():
    """`order()` always puts the seed first, so a roster that names it must
    not also add it as an entry -- a doubled witness inflates `witness_count`
    and the RRF sum for every record it reached."""
    from desktop import passage_witnesses as pw
    from shared.passage_witness_source import WITNESS_SEED_ID

    st = pw.fusion_set(
        [(WITNESS_SEED_ID, 'Your text'), ('w1', 'A')],
        {WITNESS_SEED_ID: [{'raw_header': 'h', 'score': 10}],
         'w1': [{'raw_header': 'h', 'score': 8}]})
    assert pw.order(st) == [WITNESS_SEED_ID, 'w1']
    assert pw.order(st).count(WITNESS_SEED_ID) == 1
    main, _f, _t = pw.fuse_and_cap(st)
    assert main[0]['witness_count'] == 2


def test_fusion_set_reproduces_the_roster_order_exactly():
    """`fuse()` breaks rank ties by witness POSITION, so two rosters holding
    the same witnesses in different orders can rank the same records
    differently. The caller's order is authoritative."""
    from desktop import passage_witnesses as pw
    from shared.passage_witness_source import WITNESS_SEED_ID

    st = pw.fusion_set([(WITNESS_SEED_ID, ''), ('wB', 'B'), ('wA', 'A')], {})
    assert pw.order(st) == [WITNESS_SEED_ID, 'wB', 'wA']


def test_fusion_set_copies_the_row_lists():
    """The caller's caches must not be mutated by a fusion -- `tag_rows`
    writes onto the row dicts, and the LISTS must not be shared either or a
    later append would silently join a completed fusion."""
    from desktop import passage_witnesses as pw

    rows = {'w1': [{'raw_header': 'h', 'score': 1}]}
    st = pw.fusion_set([('w1', 'A')], rows)
    st.rows['w1'].append({'raw_header': 'h2', 'score': 2})
    assert len(rows['w1']) == 1
