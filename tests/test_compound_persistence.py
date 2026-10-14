# -*- coding: utf-8 -*-
"""Composition Search conditions: persistence and the paths that must not
bypass them (gates G6 and G9 of docs/plans/COMPOSITION_CONDITIONS_PLAN.md).

Qt-free: window methods are borrowed onto stubs, as in
tests/test_desktop_passage_gate.py.
"""
from __future__ import annotations

import inspect

import pytest

import genizah_app
from shared import compound_query as cq
from shared.compound_query import Condition

APP = genizah_app.GenizahGUI
GOOD = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('א' * 60,), 'HG')


class _Label:
    def __init__(self):
        self.text = ''

    def setText(self, t):
        self.text = t

    def setStyleSheet(self, *_a):
        pass


class _Strip:
    def __init__(self):
        self.set_to = None

    def set_conditions(self, conds):
        self.set_to = list(conds)


class _Win:
    _restored_provenance_is_valid = APP._restored_provenance_is_valid
    _restore_comp_conditions = APP._restore_comp_conditions
    _on_comp_conditions_changed = APP._on_comp_conditions_changed
    _run_auto_expand = APP._run_auto_expand
    _advance_auto_expand = APP._advance_auto_expand
    _stop_auto_expand = APP._stop_auto_expand
    _after_witness_removal = APP._after_witness_removal
    _add_comp_search_to_history = APP._add_comp_search_to_history

    def __init__(self):
        self.lbl_comp_status = _Label()
        self.comp_conditions_strip = _Strip()
        self.comp_conditions = []
        self._comp_compound = None
        self.notes = []
        self.saves = 0
        self.is_comp_running = False

    def _witness_notify(self, msg):
        self.notes.append(msg)

    def _schedule_session_save(self):
        self.saves += 1

    def _passage_disabled_reason_now(self, scope=None):
        return None


# -- G6: restore never shows rows judged by conditions ---------------------------------

def _page(**extra):
    return dict({'raw_header': '990000000000000001_IE1_P000001_FL1'}, **extra)


@pytest.mark.parametrize('comp', [
    {'compound_results_withheld': True, 'results': []},
    {'results': [{'pages': [_page(compound={'category': 'main'})]}]},
    {'results': [_page(compound={'category': 'main'})]},
    {'filtered_results': [{'pages': [_page(is_condition_evidence=True)]}]},
    {'last_result_method': 'passage',
     'results': [{'pages': [_page(compound={'category': 'main'})]}]},
])
def test_a_snapshot_judged_by_conditions_is_never_displayed(comp):
    assert _Win()._restored_provenance_is_valid(comp) is False


def test_a_plain_snapshot_restores_as_before():
    w = _Win()
    assert w._restored_provenance_is_valid({'results': [{'pages': [_page()]}]}) is True
    assert w._restored_provenance_is_valid(
        {'last_result_method': 'passage', 'results': [_page()]}) is True


def test_restoring_conditions_is_strict_and_replaces():
    w = _Win()
    w._restore_comp_conditions([GOOD.to_dict()])
    assert w.comp_conditions == [GOOD] and w.comp_conditions_strip.set_to == [GOOD]
    w._restore_comp_conditions(None)                      # an entry without any
    assert w.comp_conditions == [] and w.comp_conditions_strip.set_to == []
    w._restore_comp_conditions([GOOD.to_dict(), {'requirement': 'maybe'}])
    assert w.comp_conditions == []                        # never half-applied
    from genizah_core import tr
    assert w.lbl_comp_status.text == tr(
        "The saved conditions could not be read and were cleared.")


def test_history_entry_carries_the_dispatched_conditions(monkeypatch):
    import shared.session_persistence as sp
    saved = []
    monkeypatch.setattr(sp, 'add_history_entry',
                        lambda kind, entry, limit=20: saved.append(entry))
    monkeypatch.setattr(genizah_app, 'load_app_config', lambda: {})

    class _Text:
        def __init__(self, t):
            self.t = t

        def text(self):
            return self.t

        def toPlainText(self):
            return self.t

    w = _Win()
    w.comp_title_input = _Text('')
    w.comp_text_area = _Text('the main text')
    w._comp_last_result_conditions = [GOOD.to_dict()]
    w.comp_conditions = []                 # edited after the run: not recorded
    w._refresh_comp_history = lambda: None
    w._add_comp_search_to_history()
    assert saved[0]['search_params']['comp_conditions'] == [GOOD.to_dict()]


def test_history_restore_and_legacy_snapshot_branch_apply_the_rules():
    """Call-site pins for the two long restore paths: the history re-run
    replaces the conditions, and its legacy snapshot branch drops rows judged
    by conditions before anything is displayed."""
    src = inspect.getsource(APP._restore_comp_search_from_state)
    assert "self._restore_comp_conditions((params or {}).get('comp_conditions'))" in src
    guard = src.index('_snapshot_has_compound_rows(results, filtered)')
    display = src.index('self.display_comp_results(results')
    assert guard < display


def test_session_save_withholds_rows_judged_by_conditions_and_keeps_the_conditions():
    src = inspect.getsource(APP._save_session)
    assert "'compound_results_withheld': getattr(self, '_comp_compound', None) is not None" in src
    assert "'conditions': [c.to_dict() for c in getattr(self, 'comp_conditions', None) or []]" in src
    assert ("'results': ([] if getattr(self, '_comp_compound', None) is not None"
            in src)


def test_snapshot_helper():
    f = genizah_app._snapshot_has_compound_rows
    assert f([{'pages': [_page(compound={})]}]) is True
    assert f([], [_page(is_condition_evidence=True)]) is True
    assert f([{'pages': [_page()]}], None, ['not a dict']) is False


# -- G9: paths that would bypass the conditions ---------------------------------------

def test_auto_expand_refuses_while_conditions_are_set():
    w = _Win()
    w.comp_conditions = [GOOD]
    w._has_comp_results = lambda: True
    w._run_auto_expand()
    assert not getattr(w, '_auto_expand_left', 0)
    assert w.notes


def test_an_armed_round_stops_once_conditions_exist():
    w = _Win()
    w.comp_conditions = [GOOD]
    w._auto_expand_left = 2
    w._auto_expand_armed = True
    w._comp_method = lambda: 'passage'
    w._advance_auto_expand()
    assert w._auto_expand_left == 0 and w._auto_expand_armed is False


def test_adding_a_condition_disarms_auto_expand_and_saves():
    w = _Win()
    w._auto_expand_left = 3
    w._auto_expand_armed = True
    w._on_comp_conditions_changed([GOOD])
    assert w.comp_conditions == [GOOD]
    assert w._auto_expand_left == 0 and w._auto_expand_armed is False
    assert w.saves == 1


def test_removing_a_witness_never_republishes_a_judged_result(monkeypatch):
    from desktop import passage_witnesses as pw
    w = _Win()
    w._comp_compound = {'run_id': 1}
    state = pw.WitnessSet()
    state.rows = {pw.WITNESS_SEED_ID: [_page()]}
    w._comp_witness_state = lambda: state
    w._refresh_witness_panel = lambda: None
    w._has_comp_results = lambda: True
    monkeypatch.setattr(pw, 'fuse_and_cap',
                        lambda *a, **k: pytest.fail('re-fused a judged result'))
    w.on_comp_scan_finished = lambda *_a: pytest.fail('re-published a judged result')
    w._after_witness_removal()
    assert w.notes


def test_a_compound_result_writes_no_witness_row_cache():
    """The window absorbs per-witness rows into its cache only when the
    payload carries them; a compound result must not, so a later run can never
    reuse rows whose completeness was never recorded."""
    import tests.test_compound_worker as tw
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('x',))
    s = tw._Searcher(main={tw.MAIN: tw._rows(tw.P1)}, cond={'x': (tw.P1,)})
    w = tw._worker([(tw.SEED, '', tw.MAIN)], [c], s)
    w.run()
    out = tw._result(w)
    assert not {'witness_rows', 'witness_filtered', 'witness_report'} & set(out)


# -- the Manuscript Viewer: per-image evidence (step 5) -------------------------------

S1 = '990000000000000001'


def _hdr(ie, p, sys_id=S1):
    return f'{sys_id}_IE{ie}_P{p:06d}_FL1'


class _ViewWin:
    _compound_viewer_fields = APP._compound_viewer_fields
    _compound_page_note = APP._compound_page_note
    _compound_key_str = staticmethod(APP._compound_key_str)
    _condition_label = APP._condition_label
    _compound_evidence_labels = APP._compound_evidence_labels

    def __init__(self, conditions, unsearchable=0):
        self._comp_compound = {'conditions': [c.to_dict() for c in conditions]}
        self._n = unsearchable

    def _unsearchable_pages(self, sys_id):
        return self._n


def _oc(states, evidence):
    return cq.PageOutcome(cq._category(states), tuple(states),
                          tuple(tuple(e) for e in evidence), ('',) * len(states)).to_dict()


def test_viewer_fields_map_each_image_to_what_matched_there():
    from genizah_core import tr
    rashi = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('r',), 'Rashi')
    hg = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('h',), 'HG')
    w = _ViewWin([rashi, hg], unsearchable=2)
    main = {'raw_header': _hdr(1, 5), 'highlight_pattern': 'A',
            'compound': _oc([cq.SATISFIED, cq.SATISFIED], [[_hdr(1, 6)], []])}
    ev = {'raw_header': _hdr(1, 6), 'highlight_pattern': 'B',
          'is_condition_evidence': True, 'compound_evidence': [{'condition': 0}]}
    fields = w._compound_viewer_fields({'pages': [main, ev]})
    k5, k6 = f'{S1}|IE1|5', f'{S1}|IE1|6'
    assert fields['compound_page_patterns'] == {k5: 'A', k6: 'B'}
    note = fields['compound_page_notes'][k5]
    assert tr("Main text: image {}").format(5) in note
    assert tr("{}: found on image {}").format('Rashi', '6') in note
    assert tr("({} pages of this manuscript could not be searched)").format(2) in note
    assert fields['compound_page_notes'][k6] == tr("{} matched on this image.").format('Rashi')
    # A result without conditions gets nothing, so the viewer behaves as before.
    assert w._compound_viewer_fields({'pages': [{'raw_header': _hdr(1, 5)}]}) == {}


from desktop.result_dialog import ResultDialog  # noqa: E402


class _Dlg:
    _compound_page_key = ResultDialog._compound_page_key
    _pattern_for_current_page = ResultDialog._pattern_for_current_page
    _refresh_compound_strip = ResultDialog._refresh_compound_strip

    def __init__(self, data, header):
        self.data = data
        self.current_full_header = header

        class _L:
            text_value, visible = '', None

            def setText(self, t):
                self.text_value = t

            def setVisible(self, v):
                self.visible = v
        self.lbl_rd_compound = _L()


def test_viewer_highlights_by_the_image_shown_not_by_the_entry():
    from genizah_core import tr
    data = {'highlight_pattern': 'A',
            'compound_page_patterns': {f'{S1}|IE1|5': 'A', f'{S1}|IE1|6': 'B'},
            'compound_page_notes': {f'{S1}|IE1|5': 'n5', f'{S1}|IE1|6': 'n6'}}
    d = _Dlg(data, _hdr(1, 6))                     # paged from 5 to 6
    assert d._pattern_for_current_page() == 'B'
    d._refresh_compound_strip()
    assert d.lbl_rd_compound.text_value == 'n6' and d.lbl_rd_compound.visible
    d.current_full_header = _hdr(2, 6)             # same P, another image set
    assert d._pattern_for_current_page() == ''
    d._refresh_compound_strip()
    assert d.lbl_rd_compound.text_value == tr(
        "This image is not part of the result with conditions.")
    # An ordinary entry keeps its one pattern and shows no strip.
    plain = _Dlg({'highlight_pattern': 'A'}, _hdr(2, 6))
    assert plain._pattern_for_current_page() == 'A'
    plain._refresh_compound_strip()
    assert plain.lbl_rd_compound.visible is False


def test_load_page_takes_its_pattern_from_the_page_map():
    src = inspect.getsource(ResultDialog.load_page)
    assert 'pattern_str = self._pattern_for_current_page()' in src
    assert "self.data.get('highlight_pattern')" not in src
    assert 'self._refresh_compound_strip()' in src


def test_excluded_page_counts_reads_the_installed_list(tmp_path, monkeypatch):
    import desktop.passage_lifecycle as pl
    live = tmp_path / pl.LIVE_DIRNAME
    live.mkdir()
    (live / 'excluded_records.tsv').write_text(
        'record_id\treason\n'
        f'{_hdr(1, 3)}\tshort\n{_hdr(2, 9)}\ttarget_sheet\n'
        f'{_hdr(1, 1, "990000000000000002")}\tshort\n', encoding='utf-8')
    monkeypatch.setattr(pl, 'passage_root', lambda: str(tmp_path))
    pl._EXCLUDED_COUNTS.clear()
    assert pl.excluded_page_counts() == {S1: 2, '990000000000000002': 1}
    monkeypatch.setattr(pl, 'passage_root', lambda: str(tmp_path / 'none'))
    assert pl.excluded_page_counts() == {}
