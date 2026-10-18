# -*- coding: utf-8 -*-
"""Composition Search conditions: the fixes from Codex's first code review of
feat/composition-conditions (docs/plans/COMPOSITION_CONDITIONS_PLAN.md §12).
One test (or a few) per finding; each is shown red by a named mutation.
Qt-free: window methods are borrowed onto stubs.
"""
from __future__ import annotations

import inspect
import threading

import pytest

import genizah_app
from desktop import passage_witnesses as pw
from shared import compound_query as cq
from shared.compound_query import Condition

APP = genizah_app.GenizahGUI
S1, S2 = '990000000000000001', '990000000000000002'


def hdr(sys_id, ie, p):
    return f'{sys_id}_IE{ie}_P{p:06d}_FL1'


def oc(states, evidence, reasons=None):
    return cq.PageOutcome(cq._category(states), tuple(states),
                          tuple(tuple(e) for e in evidence),
                          tuple(reasons or [''] * len(states))).to_dict()


# -- H1: the identity finalizer never waits on the lock --------------------------------

def test_the_identity_finalizer_does_not_take_the_lock():
    from shared import passage_pages as pp
    done = threading.Event()
    with pp._IDENTITY_LOCK:                         # held, as during registration
        t = threading.Thread(target=lambda: (pp._forget_identity(12345), done.set()))
        t.start()
        t.join(timeout=2)
    assert done.is_set(), 'the finalizer blocked on _IDENTITY_LOCK'


# -- H2: a witness gathered for another main text never joins the run -----------------

class _MainListWin:
    _comp_compound_main_list = APP._comp_compound_main_list

    def __init__(self, state):
        self._state = state

    def _comp_witness_state(self):
        return self._state

    def _rehydrate_witness_texts(self):
        pass

    def _refresh_witness_panel(self):
        pass


def test_the_main_list_has_every_witness_of_this_text_and_none_of_another():
    state = pw.WitnessSet()
    mine = pw.WitnessEntry(id='w1', label='mine', text='a witness of B',
                           seed_digest=pw.witness_text_key('text B'))
    other = pw.WitnessEntry(id='w2', label='other', text='a witness of A',
                            seed_digest=pw.witness_text_key('text A'),
                            status=pw.STATUS_SEARCHED)   # searched for A earlier
    state.entries = [mine, other]
    out = _MainListWin(state)._comp_compound_main_list('text B')
    assert [w for w, _l, _t in out] == [pw.WITNESS_SEED_ID, 'w1']


# -- H3: no unjudged copy of a judged page; the clicked row opens itself ---------------

class _AttachWin:
    _attach_compound_evidence = APP._attach_compound_evidence


def test_evidence_never_duplicates_a_page_judged_in_another_group():
    ev = {hdr(S1, 1, 2): {'raw_header': hdr(S1, 1, 2), 'highlight_pattern': 'B',
                          'compound_evidence': [{'condition': 0}]}}
    main = [{'raw_header': hdr(S1, 1, 1), 'highlight_pattern': 'A',
             'compound': oc([cq.SATISFIED], [[hdr(S1, 1, 2)]])}]
    out = _AttachWin()._attach_compound_evidence(
        main, ev, judged={hdr(S1, 1, 1), hdr(S1, 1, 2)})   # P2 is removed elsewhere
    assert [r['raw_header'] for r in out] == [hdr(S1, 1, 1)]
    out = _AttachWin()._attach_compound_evidence(main, ev, judged={hdr(S1, 1, 1)})
    assert [r['raw_header'] for r in out] == [hdr(S1, 1, 1), hdr(S1, 1, 2)]


def test_a_shared_page_carries_both_highlights():
    ev = {hdr(S1, 1, 1): {'raw_header': hdr(S1, 1, 1), 'highlight_pattern': 'COND',
                          'compound_evidence': [{'condition': 0}]}}
    main = [{'raw_header': hdr(S1, 1, 1), 'highlight_pattern': 'MAIN',
             'compound': oc([cq.SATISFIED], [[hdr(S1, 1, 1)]])}]
    [row] = _AttachWin()._attach_compound_evidence(main, ev, judged={hdr(S1, 1, 1)})
    assert row['highlight_pattern'] == '(?:MAIN)|(?:COND)'
    assert cq.merge_patterns('', 'B') == 'B' and cq.merge_patterns('A', 'A') == 'A'


def test_the_viewer_opens_the_clicked_row_and_shares_one_page_map():
    src = inspect.getsource(APP.on_comp_item_double_clicked)
    assert 'ids.index(id(data))' in src
    assert "_entry['compound_page_patterns'] = _all_patterns" in src


# -- M2: a rendering failure never costs the verified hits ------------------------------

def test_a_render_failure_keeps_every_verified_hit(tmp_path):
    from shared.passage_builder import build_index
    from shared.passage_index import open_index
    from shared.passage_pages import clear_cache
    from shared.passage_parallels import PassageSearcher
    from shared.passage_policy import get_preset
    from tests.test_compound_query import _aperiodic
    text = _aperiodic(90, salt=31)
    build_index([(hdr(S1, 1, 1), _aperiodic(60, 1) + text + _aperiodic(60, 2))], str(tmp_path / 'i'),
                partitions=1, apply_hygiene=False)
    idx = open_index(str(tmp_path / 'i'))
    clear_cache()

    class _Boom:
        def get_full_text_by_header(self, header):
            raise MemoryError('fetch')
    s = PassageSearcher(index=idx, text_fetcher=_Boom(), policy=get_preset('standard-40'),
                        render_cap=0)
    found = s.search_condition(text, sys_ids=(S1,), render_limit=5)
    assert found.hit_record_ids == (hdr(S1, 1, 1),) and found.rows == ()
    assert found.as_version_search().cut == cq.CUT_NONE


# -- M3: a stored outcome cannot claim an unproven absence ---------------------------------

@pytest.mark.parametrize('bad', [
    {'category': 'main', 'states': ['satisfied'], 'evidence': [[]], 'reasons': ['stopped']},
    {'category': 'unresolved', 'states': ['unresolved'], 'evidence': [[]], 'reasons': ['']},
    {'category': 'removed', 'states': ['removed'], 'evidence': [[]], 'reasons': ['']},
    {'category': 'dropped', 'states': ['failed'], 'evidence': [['x']], 'reasons': ['']},
])
def test_contradictory_outcomes_are_refused(bad):
    with pytest.raises(ValueError):
        cq.PageOutcome.from_dict(bad, 1)


# -- M4: no republish while conditions are SET ---------------------------------------------

def test_removing_a_witness_with_conditions_set_never_republishes(monkeypatch):
    class _W:
        _after_witness_removal = APP._after_witness_removal
        _comp_compound = None                         # the screen shows a plain run
        comp_conditions = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('x' * 50,))]
        notes = []

        def _witness_notify(self, m):
            self.notes.append(m)

        def _refresh_witness_panel(self):
            pass

        def _has_comp_results(self):
            return True

        def _schedule_session_save(self):
            pass

        def _comp_witness_state(self):
            st = pw.WitnessSet()
            st.rows = {pw.WITNESS_SEED_ID: [{'raw_header': hdr(S1, 1, 1)}]}
            return st
    monkeypatch.setattr(pw, 'fuse_and_cap', lambda *a, **k: pytest.fail('republished'))
    w = _W()
    w._after_witness_removal()
    assert w.notes


# -- M5: a plain result shown after a compound one carries none of its groups -------------

def test_showing_a_plain_result_clears_the_last_compound_one():
    class _W:
        _clear_compound_results = APP._clear_compound_results
    w = _W()
    w._comp_compound = {'removed': [1]}
    w.comp_cond_removed = w.comp_cond_unresolved = [1]
    w._comp_shown_cond_removed = w._comp_shown_cond_unresolved = [1]
    w._clear_compound_results()
    assert w._comp_compound is None and not (w.comp_cond_removed or w.comp_cond_unresolved
                                             or w._comp_shown_cond_removed)
    for fn in (APP._display_restored_comp_snapshot, APP._restore_comp_search_from_state):
        assert 'self._clear_compound_results()' in inspect.getsource(fn), fn.__name__


# -- M6: Exclude Manuscripts keeps every page of a manuscript ------------------------------

def test_excluding_a_manuscript_keeps_its_pages_from_every_group():
    class _W:
        _merge_comp_known = APP._merge_comp_known
    w = _W()
    w.comp_known = []
    p1 = {'raw_header': hdr(S1, 1, 1), 'compound': oc([cq.SATISFIED], [[]])}
    p2 = {'raw_header': hdr(S1, 1, 2), 'compound': oc([cq.REMOVED], [[hdr(S1, 1, 9)]])}
    w._merge_comp_known([{'type': 'manuscript', 'sys_id': S1, 'pages': [p1]},
                         {'type': 'manuscript', 'sys_id': S1, 'pages': [p2]}])
    [item] = w.comp_known
    assert [p['raw_header'] for p in item['pages']] == [hdr(S1, 1, 1), hdr(S1, 1, 2)]
    assert item['pages'][1]['compound']['category'] == cq.CAT_REMOVED


# -- M8: Add selected to List takes the two condition groups too -----------------------------

def test_add_selected_to_list_includes_the_condition_groups():
    class _W:
        _collect_selected_comp_pages = APP._collect_selected_comp_pages

        def _collect_checked_comp_items_struct(self):
            return [], {}, [], {}, []

        def _compound_export_buckets(self, has_selection):
            assert has_selection
            return ([{'type': 'manuscript', 'pages': [{'raw_header': 'r'}]}],
                    [{'type': 'manuscript', 'pages': [{'raw_header': 'u'}]}])
    assert [p['raw_header'] for p in _W()._collect_selected_comp_pages()] == ['r', 'u']


# -- M9: grouping lands only for its own run; no deferred restore over a new search ----------

def test_grouping_results_land_only_for_the_run_they_grouped():
    class _W:
        _on_grouping_for_run = APP._on_grouping_for_run
        _comp_active_run_id = 5
        landed = []

        def on_comp_finished(self, *a):
            self.landed.append(a)
    w = _W()
    w._on_grouping_for_run(4, 'stale')
    w._on_grouping_for_run(5, 'fresh')
    assert w.landed == [('fresh',)]


def test_no_deferred_restore_over_a_search_started_since_launch():
    """Both index loads (Genizah and My Library) judge a held-back snapshot
    through ONE function, and it never displays over a run started since
    launch -- whichever load answers last."""
    class _J:
        _judge_deferred_comp_snapshot = APP._judge_deferred_comp_snapshot

        def __init__(self, run_id, wait=False):
            self._comp_active_run_id = run_id
            self._deferred_comp_snapshot = {'results': ['r']}
            self._wait = wait
            self.shown = []

        def _passage_snapshot_must_wait(self, comp):
            return self._wait

        def _display_restored_comp_snapshot(self, comp):
            self.shown.append(comp)

    busy, idle, waiting = _J(5), _J(None), _J(None, wait=True)
    for w in (busy, idle, waiting):
        w._judge_deferred_comp_snapshot()
    assert busy.shown == [] and '_deferred_comp_snapshot' not in busy.__dict__
    assert idle.shown == [{'results': ['r']}]
    assert waiting.shown == [] and '_deferred_comp_snapshot' in waiting.__dict__
    for load_slot in (APP._on_passage_loaded, APP._on_local_passage_loaded):
        assert 'self._judge_deferred_comp_snapshot()' in inspect.getsource(
            load_slot)


# -- M10: a failed main text is an error, stopped or not -------------------------------------

def test_a_failed_seed_is_an_error_even_when_the_run_was_stopped():
    import tests.test_compound_worker as tw
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('c',))
    s = tw._Searcher(main={'witness': {'main': [{'raw_header': hdr(S2, 1, 1), 'score': 9.0}],
                                       'filtered': [], 'partial': True}},
                     raises={tw.MAIN: RuntimeError('x')})
    w = tw._worker([(tw.SEED, '', tw.MAIN), ('w1', '', 'witness')], [c], s)
    w.run()
    assert w.scan_finished_signal.calls == [] and w.error_signal.calls


# -- M11/M16: left-out pages come from the judged build, adjacency included ------------------

def test_the_worker_records_the_judged_builds_left_out_pages(monkeypatch):
    import tests.test_compound_worker as tw
    import desktop.passage_lifecycle as pl
    monkeypatch.setattr(pl, 'excluded_pages', lambda: (
        {tw.S1: 4}, {(tw.S1, 'IE1', 6)}))
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_ADJACENT, ('n',))
    s = tw._Searcher(main={tw.MAIN: tw._rows(tw.P1)})
    w = tw._worker([(tw.SEED, '', tw.MAIN)], [c], s)
    w.run()
    out = tw._result(w)
    assert out['compound']['unsearchable'] == {tw.S1: 4}
    assert out['compound']['neighbours_left_out'] == {f'{tw.S1}|IE1|5': [6]}


def test_the_viewer_line_names_a_neighbour_that_could_not_be_searched():
    from genizah_core import tr

    class _W:
        _compound_page_note = APP._compound_page_note
        _condition_label = APP._condition_label
        _unsearchable_pages = APP._unsearchable_pages
    w = _W()
    w._comp_compound = {
        'conditions': [Condition(cq.REQ_MUST_NOT, cq.WHERE_ADJACENT, ('n',), 'N').to_dict()],
        'unsearchable': {S1: 3},
        'neighbours_left_out': {f'{S1}|IE1|5': [6]}}
    note = w._compound_page_note({'raw_header': hdr(S1, 1, 5),
                                  'compound': oc([cq.SATISFIED], [[]])})
    assert tr("(image {} could not be searched)").format('6') in note
    assert w._unsearchable_pages(S1) == 3 and w._unsearchable_pages(S2) == 0


# -- M12: two conditions on one page highlight both --------------------------------------------

def test_the_worker_merges_two_conditions_on_one_page():
    import tests.test_compound_worker as tw
    from shared.passage_parallels import ConditionSearch
    ev = hdr(tw.S1, 1, 6)

    def found(pattern):
        return ConditionSearch((ev,), (), {}, False,
                               ({'raw_header': ev, 'score': 5.0, 'highlight_pattern': pattern},))
    c1 = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('one',))
    c2 = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('two',))
    s = tw._Searcher(main={tw.MAIN: tw._rows(tw.P1)},
                     cond={'one': found('ONE'), 'two': found('TWO')})
    w = tw._worker([(tw.SEED, '', tw.MAIN)], [c1, c2], s)
    w.run()
    row = tw._result(w)['compound']['evidence'][ev]
    assert row['highlight_pattern'] == '(?:ONE)|(?:TWO)'
    assert [t['condition'] for t in row['compound_evidence']] == [0, 1]


# -- M14: the two groups are results ---------------------------------------------------------------

def test_a_result_of_only_removed_pages_is_a_result():
    class _W:
        _has_comp_results = APP._has_comp_results
        _comp_compound_has_rows = APP._comp_compound_has_rows
        comp_main = comp_appendix = comp_known = []
        comp_filtered_main = comp_filtered_appendix = []
        _comp_compound = {'removed': [{'raw_header': 'x'}]}
    assert _W()._has_comp_results() is True


# -- M15: exports record the dispatched conditions in full ------------------------------------

def test_the_report_names_each_dispatched_condition_and_its_versions():
    from genizah_core import tr

    class _W:
        _comp_export_condition_lines = APP._comp_export_condition_lines
    w = _W()
    w._comp_last_result_conditions = [
        Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('first text', 'second text'),
                  'HG').to_dict()]
    lines = w._comp_export_condition_lines()
    assert lines[0].startswith(tr("Condition {}").format(1) + ": ")
    assert tr("Must not appear") in lines[0] and 'HG' in lines[0]
    assert lines[1].endswith('first text') and lines[2].endswith('second text')
    src = inspect.getsource(APP._comp_export_settings_lines)
    assert 'self._comp_export_condition_lines()' in src


# -- review §5: gaps in the existing tests -------------------------------------------------------

def test_a_failure_inside_the_lease_keeps_no_frame_holding_the_index(monkeypatch):
    """The fresh exception must carry no traceback frame whose locals reach
    the leased index (an access violation once the index is closed)."""
    import desktop.passage_lifecycle as pl
    import shared.passage_parallels as pp
    sentinel = type('Index', (), {})()

    class _Boom:
        def __init__(self, **kw):
            self.index = kw.get('index')

        def search_condition(self, *a, **k):
            raise ValueError('inside')
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda *_a: sentinel)
    monkeypatch.setattr(pl, '_release_lease', lambda *_a: None)
    monkeypatch.setattr(pp, 'PassageSearcher', _Boom)
    monkeypatch.setattr(pl, 'compose', lambda *a, **k: 'POLICY')
    with pytest.raises(pl.PassageSearchError) as exc:
        pl.get_passage_searcher(None).search_condition('x')
    tb = exc.value.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code.co_name == 'test_a_failure_inside_the_lease_keeps_no_frame_holding_the_index':
            tb = tb.tb_next          # this test's own frame holds the sentinel
            continue
        for value in tb.tb_frame.f_locals.values():
            assert value is not sentinel and getattr(value, 'index', None) is not sentinel
        tb = tb.tb_next


def test_grouping_keeps_each_pages_stored_outcome():
    from shared.metadata_manager import MetadataManager
    from shared.search_engine import SearchEngine

    class _Meta(MetadataManager):
        def __init__(self):
            pass

        def get_part_for_folio(self, sid):
            return None
    engine = SearchEngine.__new__(SearchEngine)
    engine.meta_mgr = _Meta()
    page = {'raw_header': hdr(S1, 1, 1), 'score': 5.0, 'uid': 'u', 'text': '',
            'compound': oc([cq.REMOVED], [[hdr(S1, 1, 3)]])}
    [item] = engine.group_pages_by_manuscript([page])
    assert item['pages'][0]['compound']['category'] == cq.CAT_REMOVED


# -- Codex code review round 2 -----------------------------------------------------------

def test_a_grouping_error_lands_only_for_its_run():
    class _W:
        _on_grouping_for_run = APP._on_grouping_for_run
        _comp_active_run_id = 5
    w = _W()
    seen = []
    w._on_grouping_for_run(4, 'stale error', handler=seen.append)
    w._on_grouping_for_run(5, 'current error', handler=seen.append)
    assert seen == ['current error']
    src = inspect.getsource(APP.start_grouping)
    assert 'handler=self.on_grouping_error' in src


def test_evidence_highlight_reaches_a_page_judged_in_another_group():
    """P1 removed because condition B matched its neighbour P2; P2 itself is a
    Main result. P2's row must still show B's match."""
    ev = {hdr(S1, 1, 2): {'raw_header': hdr(S1, 1, 2), 'highlight_pattern': 'COND_B',
                          'compound_evidence': [{'condition': 0}]}}
    main_p2 = [{'raw_header': hdr(S1, 1, 2), 'highlight_pattern': 'MAIN',
                'compound': oc([cq.SATISFIED], [[]])}]
    judged = {hdr(S1, 1, 1), hdr(S1, 1, 2)}
    [row] = _AttachWin()._attach_compound_evidence(main_p2, ev, judged)
    assert row['highlight_pattern'] == '(?:MAIN)|(?:COND_B)'
    assert row['compound_evidence'] == [{'condition': 0}]


def test_restore_refuses_non_text_evidence():
    with pytest.raises(ValueError):
        cq.PageOutcome.from_dict({'category': 'removed', 'states': ['removed'],
                                  'evidence': [[5]], 'reasons': ['']}, 1)


@pytest.mark.parametrize('method, lab, ok', [('passage', False, True), ('chunk', False, False),
                                             ('passage', True, False)])
def test_the_strip_is_frozen_outside_letter_level(method, lab, ok):
    class _Strip:
        calls = []

        def set_available(self, available, reason=''):
            self.calls.append((available, bool(reason)))

    class _Btn:
        def isChecked(self):
            return lab

    class _W:
        _refresh_comp_conditions_available = APP._refresh_comp_conditions_available
        comp_conditions_strip = _Strip()
        btn_lab_mode_toggle_comp = _Btn()

        def _comp_method(self):
            return method
    _Strip.calls = []
    _W()._refresh_comp_conditions_available()
    assert _Strip.calls == [(ok, not ok)]


@pytest.mark.parametrize('scope, ok', [('genizah', True), ('local', False),
                                       ('all', False)])
def test_the_strip_is_frozen_outside_the_genizah_corpus(scope, ok):
    """Conditions are Genizah-only, so the strip follows the corpus scope
    too, not only the method."""
    class _Strip:
        calls = []

        def set_available(self, available, reason=''):
            self.calls.append((available, bool(reason)))

    class _Btn:
        def isChecked(self):
            return False

    class _Combo:
        def currentData(self):
            return scope

    class _W:
        _refresh_comp_conditions_available = APP._refresh_comp_conditions_available
        comp_conditions_strip = _Strip()
        btn_lab_mode_toggle_comp = _Btn()
        comp_corpus_scope_combo = _Combo()

        def _comp_method(self):
            return 'passage'
    _Strip.calls = []
    _W()._refresh_comp_conditions_available()
    assert _Strip.calls == [(ok, not ok)]


def _export_host_with_conditions():
    import tests.test_comp_flat_markers_and_xlsx_columns as tx
    from types import MethodType
    host = tx._compound_host()
    host._comp_last_result_conditions = [
        Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('the forbidden text',), 'HG').to_dict()]
    host._comp_export_condition_lines = MethodType(APP._comp_export_condition_lines, host)
    return tx, host


@pytest.mark.parametrize('fmt', ['csv', 'txt'])
def test_csv_and_txt_record_the_dispatched_text_and_conditions(tmp_path, monkeypatch, fmt):
    from genizah_core import tr
    tx, host = _export_host_with_conditions()
    out = tmp_path / f'r.{fmt}'
    tx._patch_dialogs(monkeypatch, out)
    host._default_report_path = lambda title, fallback, f=fmt: title + '.' + f
    host.export_comp_report(fmt)
    text = out.read_text(encoding='utf-8-sig')
    assert 'the dispatched text' in text and 'edited after the search' not in text
    assert tr("Condition {}").format(1) in text and 'the forbidden text' in text
# -- PR #399 review: a main witness that failed is said, not hidden ---------------------

def test_a_failed_witness_is_marked_failed_and_left_out_of_the_provenance():
    """Codex, PR #399 (P2): a non-seed main witness whose search raised was
    recorded only in the run's steps. The run looked complete, the pages
    only it would have found were silently missing, and exports and history
    still named it. Now the window marks it Failed in the Witnesses panel
    (Retry) and drops it from the provenance."""
    import tests.test_compound_worker as tw
    from genizah_app import tr
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('c',))
    s = tw._Searcher(main={tw.MAIN: tw._rows(tw.P1), 'ok text': tw._rows(tw.P2)},
                     raises={'bad text': RuntimeError('x')})
    w = tw._worker([(tw.SEED, '', tw.MAIN), ('w1', 'W1', 'bad text'),
                    ('w2', 'W2', 'ok text')], [c], s)
    w.run()
    steps = tw._result(w)['compound']['steps']

    state = pw.WitnessSet(entries=[
        pw.WitnessEntry(id='w1', label='W1', text='bad text'),
        pw.WitnessEntry(id='w2', label='W2', text='ok text'),
        pw.WitnessEntry(id='w3', label='W3', text=''),        # never dispatched
    ])

    class _Win:
        _absorb_compound_main_steps = APP._absorb_compound_main_steps
        refreshed = 0

        def _comp_witness_state(self):
            return state

        def _refresh_witness_panel(self):
            _Win.refreshed += 1

    win = _Win()
    win._comp_last_result_witnesses = [{'id': 'w1'}, {'id': 'w2'}, {'id': 'w3'}]
    win._absorb_compound_main_steps(steps)
    assert [r['id'] for r in win._comp_last_result_witnesses] == ['w2']
    by_id = {e.id: e for e in state.entries}
    assert by_id['w1'].status == pw.STATUS_FAILED and by_id['w1'].error == tr(
        "The letter-level search could not be completed. "
        "Details have been written to the log.")
    assert by_id['w2'].status == pw.STATUS_PENDING
    assert _Win.refreshed == 1
    assert 'self._absorb_compound_main_steps(' in inspect.getsource(
        APP.on_comp_scan_finished)
