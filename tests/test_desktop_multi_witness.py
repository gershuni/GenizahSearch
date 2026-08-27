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


# ---------------------------------------------------------------------------
# The surface. Qt-free throughout: the window's real methods are borrowed onto
# a stub, so these run in the default lane with no QApplication.
# ---------------------------------------------------------------------------

import ast  # noqa: E402
import io as _io  # noqa: E402
import os as _os  # noqa: E402

import genizah_app  # noqa: E402
from desktop import passage_witnesses as pw  # noqa: E402
from shared.passage_witness_source import WITNESS_SEED_ID  # noqa: E402

GAPP = genizah_app.GenizahGUI
_APP_PATH = _os.path.join(_os.path.dirname(_os.path.dirname(
    _os.path.abspath(__file__))), 'genizah_app.py')


def _app_src():
    return _io.open(_APP_PATH, encoding='utf-8').read()


def _fn_src(name):
    src = _app_src()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node) or ''
    raise AssertionError('%s not found' % name)


class _Win2:
    """Borrows the real methods. A re-implementation would only prove the
    test agrees with itself."""

    _comp_witness_state = GAPP._comp_witness_state
    _comp_seed_text = GAPP._comp_seed_text
    _comp_has_witnesses = GAPP._comp_has_witnesses
    _comp_witness_roster = GAPP._comp_witness_roster
    _comp_witness_dispatch_list = GAPP._comp_witness_dispatch_list
    _comp_witness_prior_rows = GAPP._comp_witness_prior_rows
    _absorb_witness_result = GAPP._absorb_witness_result
    _comp_witness_total = GAPP._comp_witness_total
    _comp_sort_key = GAPP._comp_sort_key
    _sort_comp_items = GAPP._sort_comp_items
    _current_comp_sort_mode = GAPP._current_comp_sort_mode
    _comp_export_settings_lines = GAPP._comp_export_settings_lines
    _comp_chunk_preference = GAPP._comp_chunk_preference
    _restore_comp_passage_preferences = GAPP._restore_comp_passage_preferences
    _add_comp_search_to_history = GAPP._add_comp_search_to_history
    _PASSAGE_FORCED_CONTROLS = GAPP._PASSAGE_FORCED_CONTROLS
    COMP_SORT_MODES = GAPP.COMP_SORT_MODES


def _win(seed='the seed text of this work'):
    w = _Win2()
    w._comp_witnesses = pw.WitnessSet()
    w.comp_text_area = _TextArea(seed)
    w.comp_sort_mode = 'score'
    w.comp_sort_reverse = True
    return w


class _TextArea:
    def __init__(self, text=''):
        self._t = text

    def toPlainText(self):
        return self._t


def _grp(header, score, **kw):
    """A manuscript-level item as `group_pages_by_manuscript` builds it."""
    page = {'raw_header': header, 'score': score}
    page.update(kw)
    return {'type': 'manuscript', 'score': score, 'pages': [page]}


# --- dispatch --------------------------------------------------------------

def test_the_dispatch_list_includes_the_seed_when_it_has_no_rows():
    """The seed IS a witness and the fusion is wrong without it."""
    w = _win()
    pw.add_texts(w._comp_witness_state(), ['alpha beta gamma'],
                 w._comp_seed_text(), 'P')
    ids = [wid for wid, _l, _t in GAPP._comp_witness_dispatch_list(w)]
    assert ids[0] == WITNESS_SEED_ID


def test_the_dispatch_list_skips_the_seed_once_it_has_rows():
    """What keeps a later round linear: a witness is searched at most once."""
    w = _win()
    st = w._comp_witness_state()
    st.rows[WITNESS_SEED_ID] = [{'raw_header': 'h', 'score': 1}]
    pw.add_texts(st, ['alpha beta gamma'], 'the seed text of this work', 'P')
    ids = [wid for wid, _l, _t in GAPP._comp_witness_dispatch_list(w)]
    assert WITNESS_SEED_ID not in ids
    assert len(ids) == 1


def test_a_textless_witness_is_left_out_of_the_dispatch_list():
    """Never dispatch an empty query: the engine answers it with nothing and
    the panel would report an honest-looking "0 matches" for a search that
    never ran."""
    w = _win()
    st = w._comp_witness_state()
    st.entries.append(pw.WitnessEntry(id='w1', label='M', kind='manuscript',
                                      sys_id='990000000001', text=''))
    ids = [wid for wid, _l, _t in GAPP._comp_witness_dispatch_list(w)]
    assert 'w1' not in ids


def test_the_roster_is_the_full_picture_with_the_seed_first():
    w = _win()
    pw.add_texts(w._comp_witness_state(), ['alpha beta gamma'],
                 'the seed text of this work', 'P')
    roster = GAPP._comp_witness_roster(w)
    assert roster[0][0] == WITNESS_SEED_ID
    assert len(roster) == 2


def test_dispatch_asks_for_an_uncapped_searcher_only_when_fusing():
    """render_cap=0 for a batch, nothing for a single search. Capping each
    witness before the fusion drops every contributor past rank 200 in its
    own witness."""
    src = _fn_src('run_composition')
    assert 'render_cap=0 if _comp_multi else None' in src, (
        'the multi-witness path no longer asks for an uncapped searcher')


def test_dispatch_pins_the_index_generation_on_the_ui_thread():
    """Pinned before the worker can take its first lease; a swap between the
    two would otherwise go unnoticed."""
    src = _fn_src('run_composition')
    assert 'pinned_generation=' in src
    assert 'current_state_generation()' in src


def test_a_chunk_run_never_takes_the_multi_witness_path():
    src = _fn_src('run_composition')
    assert "_comp_multi = (_comp_dispatch_method == 'passage'" in src, (
        'the multi-witness branch is no longer gated on the method')


# --- absorbing a finished batch --------------------------------------------

def test_absorbing_a_batch_moves_each_witness_to_its_new_status():
    w = _win()
    st = w._comp_witness_state()
    pw.add_texts(st, ['alpha beta gamma', 'delta epsilon zeta'],
                 'the seed text of this work', 'P')
    a, b = st.entries[0].id, st.entries[1].id
    GAPP._absorb_witness_result(w, {
        'witness_rows': {a: [{'raw_header': 'h', 'score': 5}]},
        'witness_filtered': {},
        'witness_report': [
            {'witness_id': a, 'status': 'searched', 'hits': 1},
            {'witness_id': b, 'status': 'failed', 'reason': 'search_failed'},
        ],
    })
    assert st.entries[0].status == pw.STATUS_SEARCHED
    assert st.entries[0].hits == 1
    assert st.entries[1].status == pw.STATUS_FAILED
    assert st.entries[1].error, 'a failed witness carries no reason'


def test_absorbing_keeps_the_reason_the_worker_knew():
    """Rehydration empties an over-long witness and records WHY; a generic
    "could not load" over that replaces a true reason with a false one and
    the user retries forever."""
    w = _win()
    st = w._comp_witness_state()
    pw.add_texts(st, ['alpha beta gamma'], 'the seed text of this work', 'P')
    wid = st.entries[0].id
    GAPP._absorb_witness_result(w, {
        'witness_rows': {}, 'witness_filtered': {},
        'witness_report': [{'witness_id': wid, 'status': 'failed',
                            'reason': 'empty_text'}]})
    # Compared against tr(), never an English literal: tr() reads the
    # OWNER's configured language, so an assert-English test passes on a
    # CI box and fails on a Hebrew desktop.
    from genizah_core import tr as _tr
    assert st.entries[0].error == _tr(
        "Could not load text for this manuscript.")
    assert st.entries[0].error != _tr(
        "The letter-level search could not be completed. Details have been "
        "written to the log."), 'the specific reason was overwritten'


# --- the witness column and sorting ----------------------------------------

def test_fused_sorting_reads_the_group_not_a_dropped_field():
    """`group_pages_by_manuscript` keeps only summed raw `score` and drops
    `fusion_score` on the way to manuscript level, so a sort that read the
    field would put every manuscript into one tie at zero."""
    w = _win()
    w.comp_sort_mode = 'fused'
    strong = _grp('990000000001_1r', 10, fusion_score=0.5, witness_count=3,
                  witness_ids='seed,w1,w2')
    weak = _grp('990000000002_1r', 900, fusion_score=0.01, witness_count=1,
                witness_ids='seed')
    out = GAPP._sort_comp_items(w, [weak, strong])
    assert out[0] is strong, 'raw score beat the fusion'


def test_witness_sorting_orders_by_distinct_contributors():
    w = _win()
    w.comp_sort_mode = 'witnesses'
    many = _grp('990000000001_1r', 5, witness_count=4, witness_ids='seed,w1,w2,w3')
    few = _grp('990000000002_1r', 800, witness_count=1, witness_ids='seed')
    out = GAPP._sort_comp_items(w, [few, many])
    assert out[0] is many


def test_score_sorting_is_unchanged():
    w = _win()
    w.comp_sort_mode = 'score'
    a, b = _grp('990000000001_1r', 10), _grp('990000000002_1r', 900)
    assert GAPP._sort_comp_items(w, [a, b])[0] is b


def test_an_unknown_sort_mode_falls_back_to_score_not_shelfmark():
    """It used to fall THROUGH to shelfmark ordering, so a hand-edited
    session file silently re-sorted the page."""
    w = _win()
    w.comp_sort_mode = 'nonsense-from-a-session-file'
    assert GAPP._current_comp_sort_mode(w) == 'score'


def test_a_fused_mode_is_a_known_mode():
    w = _win()
    w.comp_sort_mode = 'fused'
    assert GAPP._current_comp_sort_mode(w) == 'fused'


def test_the_witness_column_is_appended_so_no_index_moves():
    """Every existing column index is load-bearing across the tree, the
    exports and the header filters."""
    src = _app_src()
    assert 'self.comp_col_src = 8' in src
    assert 'self.comp_col_witnesses = 9' in src


def test_the_witness_column_visibility_is_set_on_every_render():
    """There is no composition header visibility persistence, so a column
    shown for a fused run would survive onto the next chunk result."""
    src = _fn_src('display_comp_results')
    assert 'setColumnHidden' in src and 'comp_col_witnesses' in src


def test_the_witness_total_is_provenance_not_the_live_panel():
    """Adding a witness after a search must not change the denominator
    printed beside rows that search produced."""
    w = _win()
    w._comp_last_result_witness_total = 3
    pw.add_texts(w._comp_witness_state(), ['alpha beta gamma'],
                 'the seed text of this work', 'P')
    assert GAPP._comp_witness_total(w) == 3


# --- provenance, history, export -------------------------------------------

def test_the_export_settings_name_the_witness_count():
    w = _win()
    w._comp_last_result_method = 'passage'
    w._comp_last_result_width = 'widest-40'
    w._comp_last_result_length = 'normal'
    w._comp_last_result_depth = 'normal'
    w._comp_last_result_witnesses = [{'id': 'w1'}, {'id': 'w2'}]
    w._comp_axis_label = lambda axis, value: str(value)
    lines = GAPP._comp_export_settings_lines(w)
    # Two witnesses plus the seed.
    assert any('3' in ln for ln in lines), lines


def test_a_single_witness_export_gains_no_witness_line():
    w = _win()
    w._comp_last_result_method = 'passage'
    w._comp_last_result_width = 'widest-40'
    w._comp_last_result_length = 'normal'
    w._comp_last_result_depth = 'normal'
    w._comp_last_result_witnesses = []
    w._comp_axis_label = lambda axis, value: str(value)
    assert len(GAPP._comp_export_settings_lines(w)) == 4


def test_history_records_the_witnesses_it_ran_with():
    """Without it, re-running a saved multi-witness entry ran the SEED ALONE
    under the same name."""
    src = _fn_src('_add_comp_search_to_history')
    assert "'comp_witnesses'" in src


def test_history_restore_rebuilds_the_witnesses():
    """The restore seam is `_restore_comp_passage_preferences`, which the
    history path already calls -- so the key has to be the one it reads."""
    assert "'comp_witnesses'" in _fn_src('_restore_comp_passage_preferences')
    assert '_restore_comp_passage_preferences' in _fn_src(
        '_restore_comp_search_from_state')


def test_the_session_stores_the_witness_list_as_a_preference():
    assert "'comp_witnesses'" in _fn_src('_comp_passage_preference_fields')


def test_the_export_witness_column_is_conditional():
    """A single-witness or chunk export stays byte-identical to v9.0.0; a
    blank column would mean nothing.

    Checked by AST, not substring: `assert '_comp_wit_col' in src` passes
    just as happily against `_comp_wit_col = True`, which turns the column on
    for every export. A mutation proved exactly that.
    """
    tree = ast.parse(_fn_src('export_comp_report'))
    found = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == '_comp_wit_col'):
            found = node.value
    assert found is not None, '_comp_wit_col is no longer assigned'
    assert isinstance(found, ast.Compare), (
        '_comp_wit_col is a constant, so the column is no longer conditional')
    assert isinstance(found.ops[0], ast.Gt)
    assert isinstance(found.comparators[0], ast.Constant)
    assert found.comparators[0].value == 1, (
        'the column now appears for a single-witness run too')


# --- the recursive button ---------------------------------------------------

def test_recursive_runs_fusion_on_passage_and_concatenation_on_chunk():
    """Chunk keeps concatenating -- measured correct for its own engine, 392
    manuscripts both ways with an empty difference in both directions. The
    passage branch runs rank-fused expansion instead, and must RETURN: the
    concatenation below it starves the posting budget to 48.2%, below the
    56.7% of the best single witness.

    The return is checked by AST. Asserting only that `'passage'` appears
    before `combined_text` passes with the return deleted, which is precisely
    the fall-through this guards -- a mutation proved it.
    """
    src = _fn_src('run_recursive_composition')
    assert 'combined_text' in src, (
        'the chunk branch no longer concatenates -- that is correct for it')

    tree = ast.parse(src)
    branch = None
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and any(
                isinstance(c, ast.Constant) and c.value == 'passage'
                for c in ast.walk(node.test)):
            branch = node
            break
    assert branch is not None, 'no passage branch in run_recursive_composition'
    calls = [n.func.attr for n in ast.walk(branch)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    assert '_run_auto_expand' in calls, (
        'the passage branch no longer runs rank-fused expansion')
    assert isinstance(branch.body[-1], ast.Return), (
        'the passage branch falls through into the concatenating path')


def test_the_recursive_button_is_no_longer_disabled_in_passage_mode():
    """It was, and `_update_recursive_button_state` re-enabled it on the next
    render anyway -- so the disable never held and only the programmatic
    guard caught the click."""
    src = _fn_src('_apply_passage_mode_ui')
    assert 'rec.setEnabled' not in src


def test_auto_expand_refuses_a_round_it_cannot_complete():
    """Refused rather than silently shrunk: a control that quietly does less
    than it says is a lie."""
    src = _fn_src('_advance_auto_expand')
    assert 'DESKTOP_WITNESS_CAP' in src
    assert '_stop_auto_expand' in src


def test_auto_expand_is_driven_by_completed_searches_not_a_loop():
    """Each round IS a search, and the search is a worker thread. A loop
    would either block the UI thread or start round two against round one's
    unfinished results."""
    src = _fn_src('on_comp_scan_finished')
    assert '_advance_auto_expand' in src
    assert '_auto_expand_left' in src


# ---------------------------------------------------------------------------
# Owner hand-test follow-ups, 2026-08-27.
# ---------------------------------------------------------------------------

def test_new_clears_the_witness_list():
    """"New" clears the search, and the witness list IS part of the search.
    Left behind, it silently carried the previous work's witnesses into the
    next one -- and a witness of one work is noise in another."""
    src = _fn_src('_reset_composition')
    assert 'passage_witnesses.WitnessSet()' in src, (
        'New no longer clears the witnesses')
    assert '_comp_last_result_witnesses = []' in src
    assert '_auto_expand_left = 0' in src, (
        'a running auto-expand survives New')


def test_restored_manuscript_witnesses_are_rehydrated_before_dispatch():
    """THE gap this follow-up closed. `snapshot()` stores a manuscript
    witness WITHOUT its text, and the dispatch list skips a textless witness
    -- so after a restart those witnesses were silently left out and the run
    reported success with fewer witnesses than the dialog listed."""
    src = _fn_src('run_composition')
    assert '_rehydrate_witness_texts()' in src
    tree = ast.parse(src)
    order = [n.lineno for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ('_rehydrate_witness_texts',
                                 '_comp_witness_dispatch_list')]
    calls = [n.func.attr for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ('_rehydrate_witness_texts',
                                 '_comp_witness_dispatch_list')]
    pairs = sorted(zip(order, calls))
    assert pairs[0][1] == '_rehydrate_witness_texts', (
        'the dispatch list is built before the texts are put back')


def test_rehydration_prefers_the_headers_the_promotion_recorded():
    """A promoted witness is the concatenation of the pages that MATCHED, so
    re-deriving headers from whatever rows are on screen now rebuilds a
    different witness under the same label."""
    src = _fn_src('_rehydrate_witness_texts')
    assert 'headers_by_sid' in src
    assert 'e.headers' in src or 'entry.headers' in src


def test_an_unresolvable_witness_is_failed_with_a_reason_not_dropped():
    src = _fn_src('_rehydrate_witness_texts')
    assert 'STATUS_FAILED' in src
    assert 'Could not load text for this manuscript.' in src


def test_files_can_be_loaded_one_or_many():
    src = _fn_src('_load_witness_files')
    assert 'getOpenFileNames' in src, 'only one file can be chosen'
    assert 'for path in paths' in src


def test_loading_a_file_falls_back_from_utf8_to_the_hebrew_codepage():
    """The owner's witness files come out of Word and Notepad; a hard utf-8
    read turns a cp1255 file into an unreadable one."""
    src = _fn_src('_load_witness_files')
    assert 'utf-8-sig' in src
    assert 'cp1255' in src


def test_loading_files_reports_every_rejection():
    """Same rule as the paste path: a load that quietly loses part of a
    selection is the failure this repo treats as a defect."""
    src = _fn_src('_load_witness_files')
    assert '_report_witness_additions' in src
    assert 'unreadable' in src


def test_the_recursive_button_and_the_dialog_button_share_one_name():
    """Two labels for one behaviour read as two features. In letter-level
    mode the toolbar button runs exactly what the dialog's auto-expand button
    runs, so it says exactly what that one says; in chunk mode it keeps its
    own name, because there it does something else."""
    applied = _fn_src('_apply_passage_mode_ui')
    assert 'Run auto-expand now' in applied
    assert 'Full Recursive Search' in applied, (
        'the chunk name is no longer restored when leaving passage mode')


def test_the_render_hook_does_not_put_the_chunk_name_back():
    """`_update_recursive_button_state` rewrites the label on EVERY render,
    so without a method branch it would undo the rename the moment any
    result appeared."""
    src = _fn_src('_update_recursive_button_state')
    assert "_comp_method() == 'passage'" in src
    assert 'Run auto-expand now' in src


# ---------------------------------------------------------------------------
# Two rules that a source-text assertion cannot check, and a mutation proved
# it: `assert 'split_by_length' in src` passes with the CALL deleted, because
# the name survives on the import line; `assert '{n} witnesses' in src` passes
# with the branch disabled, because the literal is still there.
# ---------------------------------------------------------------------------

class _Fetcher:
    """Stands in for the SearchEngine's two text fetchers."""

    def __init__(self, by_header=None, whole=None):
        self._by_header = by_header or {}
        self._whole = whole or {}

    def get_full_text_by_header(self, header):
        return self._by_header.get(header)

    def get_full_manuscript(self, sys_id):
        text = self._whole.get(sys_id)
        return [{'text': text}] if text else []


def _rehydrate_win(entry, fetcher):
    w = _win()
    w._comp_witness_state().entries.append(entry)
    w.searcher = fetcher
    w._comp_last_fused_rows = []
    w._witness_notify = lambda text: None
    w._refresh_witness_panel = lambda: None
    return w


def _manuscript_entry(text=''):
    return pw.WitnessEntry(
        id='w1', label='T-S 1.1', kind='manuscript', sys_id='990000000001',
        headers=['990000000001_1r'], text=text,
        status=pw.STATUS_PENDING)


def test_rehydration_puts_a_restored_manuscript_text_back():
    """The gap this closed: a restored manuscript witness carries no text,
    and the dispatch list skips a textless witness -- so the run completed
    with fewer witnesses than the dialog listed and said nothing."""
    entry = _manuscript_entry()
    w = _rehydrate_win(entry, _Fetcher({'990000000001_1r': 'aleph bet gimel'}))
    GAPP._rehydrate_witness_texts(w)
    assert entry.text == 'aleph bet gimel'
    assert entry.status == pw.STATUS_PENDING
    ids = [wid for wid, _l, _t in GAPP._comp_witness_dispatch_list(w)]
    assert 'w1' in ids, 'still left out of the search after rehydration'


def test_rehydration_empties_an_over_long_refetch_rather_than_truncating():
    """What comes back is NOT what was promoted -- the refetch falls back to
    the whole manuscript -- so a reload could turn a capped witness into an
    uncapped one that spends its entire run and fails. Emptied, not
    truncated: half a manuscript searched as if it were the whole one is a
    worse answer than none, and an invisible one."""
    from shared.passage_fusion import MAX_WITNESS_CHARS

    huge = 'a' * (MAX_WITNESS_CHARS + 1)
    entry = _manuscript_entry()
    w = _rehydrate_win(entry, _Fetcher({'990000000001_1r': huge}))
    GAPP._rehydrate_witness_texts(w)
    assert entry.text == '', 'an over-long refetch was accepted'
    assert entry.status == pw.STATUS_FAILED
    assert entry.error, 'failed with no reason'
    ids = [wid for wid, _l, _t in GAPP._comp_witness_dispatch_list(w)]
    assert 'w1' not in ids


def test_rehydration_keeps_a_text_that_fits():
    """The cap must not reject the ordinary case."""
    entry = _manuscript_entry()
    w = _rehydrate_win(entry, _Fetcher({'990000000001_1r': 'a' * 100}))
    GAPP._rehydrate_witness_texts(w)
    assert len(entry.text) == 100
    assert entry.status == pw.STATUS_PENDING


def test_rehydration_fails_a_manuscript_whose_text_cannot_be_found():
    entry = _manuscript_entry()
    w = _rehydrate_win(entry, _Fetcher({}))
    GAPP._rehydrate_witness_texts(w)
    assert entry.status == pw.STATUS_FAILED
    assert entry.error


def test_rehydration_leaves_a_pasted_witness_alone():
    """Its text existed nowhere but the snapshot, so there is nothing to
    re-fetch -- and it already has the text."""
    entry = pw.WitnessEntry(id='w1', label='P', kind='pasted',
                            text='aleph bet gimel', status=pw.STATUS_PENDING)
    w = _rehydrate_win(entry, _Fetcher({}))
    GAPP._rehydrate_witness_texts(w)
    assert entry.text == 'aleph bet gimel'
    assert entry.status == pw.STATUS_PENDING


def test_history_names_the_witness_count_in_the_entry(monkeypatch):
    """A history list showing only the seed offers two visibly identical
    entries for two searches that asked different questions. Checked by
    capturing the entry, not by grepping the source: the literal survives a
    disabled branch."""
    import genizah_app
    import shared.session_persistence as sp

    captured = {}
    monkeypatch.setattr(sp, 'add_history_entry',
                        lambda kind, entry, limit=None: captured.update(entry))
    monkeypatch.setattr(genizah_app, 'load_app_config', lambda: {})

    w = _win()

    class _Line:
        def text(self):
            return 'Birkat Hamazon'
    w.comp_title_input = _Line()
    w.comp_raw_items = []
    w.comp_raw_filtered = []
    w._comp_last_result_witnesses = [{'id': 'w1'}, {'id': 'w2'}]
    w.pre_search_filters = {}
    # UI refresh is not what this test is about.
    w._refresh_comp_history = lambda: None
    w.excluded_raw_entries = []
    for name in ('spin_chunk', 'spin_freq', 'spin_filter',
                 'comp_mode_combo', 'comp_corpus_scope_combo'):
        setattr(w, name, _Combo0())
    w.btn_lab_mode_toggle_comp = _Combo0()
    w.chk_lab_deep_comp = _Combo0()
    w.chk_comp_flat = _Combo0()

    GAPP._add_comp_search_to_history(w)

    assert captured, 'nothing was written to history'
    # The COUNT is stored, not a translated sentence: a translated string in
    # a persisted record freezes the language at save time, so entries saved
    # in English would keep reading English in a Hebrew history list.
    # Two witnesses plus the seed.
    assert captured['witness_count'] == 3
    # Compared against tr(), not the English literal: tr() reads the OWNER's
    # configured language, so asserting the English form passes on a Hebrew
    # desktop no matter what was stored. A mutation proved exactly that.
    from genizah_core import tr as _tr
    _rendered = _tr("{n} witnesses").format(n=3)
    assert _rendered not in captured.get('query', ''), (
        'a rendered sentence was baked into the stored entry')
    assert captured['search_params']['comp_witnesses'] == [
        {'id': 'w1'}, {'id': 'w2'}]


class _Combo0:
    def value(self):
        return 0

    def currentIndex(self):
        return 0

    def currentData(self):
        return 'genizah'

    def isChecked(self):
        return False

    def text(self):
        return ''


# ---------------------------------------------------------------------------
# A restore describes a WHOLE state, not a patch to the current one.
# Owner-reported 2026-08-27: loading an old chunk search and switching to
# letter-level showed twenty-two witnesses belonging to a different work.
# ---------------------------------------------------------------------------

def _restorable_win():
    w = _win()
    w._refresh_witness_panel = lambda: None
    w._witness_notify = lambda text: None
    return w


def test_restoring_an_entry_with_no_witnesses_clears_the_current_ones():
    """THE bug. The restore ran only `if 'comp_witnesses' in comp`, so every
    chunk search -- and every search saved before this feature existed --
    left whatever happened to be in memory untouched."""
    w = _restorable_win()
    pw.add_texts(w._comp_witness_state(),
                 ['alpha beta gamma', 'delta epsilon zeta'], w._comp_seed_text(), 'P')
    assert len(w._comp_witness_state().entries) == 2
    GAPP._restore_comp_passage_preferences(w, {'chunk_size': 5})
    assert w._comp_witness_state().entries == [], (
        'a chunk entry left the previous search\'s witnesses behind')


def test_restoring_an_empty_witness_list_clears_them_too():
    """A saved multi-witness search whose witnesses were all removed before
    saving is still a statement: none."""
    w = _restorable_win()
    pw.add_texts(w._comp_witness_state(), ['alpha beta gamma'], w._comp_seed_text(), 'P')
    GAPP._restore_comp_passage_preferences(w, {'comp_witnesses': []})
    assert w._comp_witness_state().entries == []


def test_restoring_an_entry_with_witnesses_brings_exactly_those():
    w = _restorable_win()
    pw.add_texts(w._comp_witness_state(), ['stale one here'], w._comp_seed_text(), 'P')
    GAPP._restore_comp_passage_preferences(w, {'comp_witnesses': [
        {'kind': 'pasted', 'label': 'A', 'text': 'aleph bet gimel'},
        {'kind': 'pasted', 'label': 'B', 'text': 'dalet he vav'},
    ]})
    got = [e.text for e in w._comp_witness_state().entries]
    assert got == ['aleph bet gimel', 'dalet he vav'], got


def test_a_restore_also_drops_the_previous_runs_provenance():
    """`_comp_last_result_witnesses` feeds the export settings block and the
    next history entry. Left behind, a restored chunk search would report
    the previous search's witness count."""
    w = _restorable_win()
    w._comp_last_result_witnesses = [{'id': 'w1'}, {'id': 'w2'}]
    GAPP._restore_comp_passage_preferences(w, {'chunk_size': 5})
    assert w._comp_last_result_witnesses == []


def test_a_restore_stops_a_running_auto_expand():
    """Rounds queued against the old result set would promote from rows the
    restore has just replaced."""
    w = _restorable_win()
    w._auto_expand_left = 2
    GAPP._restore_comp_passage_preferences(w, {'chunk_size': 5})
    assert w._auto_expand_left == 0


def test_a_restore_clears_the_leftover_progress_line():
    """"Witness 23/23: T-S 8H11.3" left on screen described a search the
    restored one is not."""
    w = _win()
    w._refresh_witness_panel = lambda: None
    seen = []
    w._witness_notify = lambda text: seen.append(text)
    GAPP._restore_comp_passage_preferences(w, {'chunk_size': 5})
    assert '' in seen, 'the progress line was left as it was'


def test_new_clears_the_progress_line_too():
    src = _fn_src('_reset_composition')
    assert "_witness_notify('')" in src


def test_a_single_witness_search_stores_no_witness_count():
    """`witness_count` is what the history list keys its suffix on, so a
    plain search must not carry one."""
    src = _fn_src('_add_comp_search_to_history')
    assert "'witness_count': (len(_hist_wits) + 1) if _hist_wits else 0" in src


def test_the_history_list_renders_the_count_through_tr():
    """Rendered at DISPLAY time, in whatever language the list is being
    drawn in -- which is the half that was frozen before."""
    src = _fn_src('_add_history_menu_item')
    assert 'witness_count' in src
    assert 'tr("{n} witnesses")' in src


def test_the_history_suffix_is_hidden_for_a_single_witness():
    """"1 witnesses" beside every ordinary search is noise."""
    src = _fn_src('_add_history_menu_item')
    assert '_wits > 1' in src
